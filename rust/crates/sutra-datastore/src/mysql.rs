//! MySQL / MariaDB user-datastore dialect.
//!
//! The same store operations re-stated on the sqlx MySQL driver, on the same
//! `(store_name, store_key)` cutover predicate the [`crate::postgres`] reference uses.
//! The store SQL is deliberately dialect-neutral (the portability proof shows the
//! identical four statements run unchanged on PostgreSQL, MySQL, MariaDB and SQL Server),
//! so this module differs from the reference only in the driver, placeholder syntax (`?`
//! vs `$n`), and the migration advisory lock (`GET_LOCK` vs `pg_advisory_lock`). MariaDB
//! 10.6+/11 rides this same module — the mariadb container suite re-runs the identical
//! test sources to prove it.
//!
//! Dialect notes (each pinned by the container suites):
//! - **READ COMMITTED per connection.** MySQL/MariaDB default to REPEATABLE READ, under
//!   which a plain `SELECT` after the lock-acquiring `UPDATE` would return the transaction
//!   snapshot rather than the just-locked latest value — breaking read-modify-write. The
//!   pool sets `SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED` on every connection,
//!   exactly as the store contract pins it. PostgreSQL/SQL Server already default
//!   to READ COMMITTED.
//! - **Every write bumps `rev`,** so an `UPDATE` that matches a row always CHANGES it —
//!   `rows_affected()` reports the match regardless of `CLIENT_FOUND_ROWS`, and the
//!   portable upsert / CAS need no matched-rows flag.
//! - **Duplicate-key detection** reads the driver-mapped `ErrorKind::UniqueViolation`, NOT the
//!   SQLSTATE class — MySQL buckets duplicate key, `NOT NULL`, foreign-key and `CHECK` failures
//!   all into `23000`, so the class cannot discriminate. See [`is_duplicate_key`].

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value as Json;
use sqlx::error::ErrorKind;
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection, MySqlPool, MySqlPoolOptions};
use sqlx::pool::PoolConnection;
use sqlx::{Acquire, ConnectOptions, Connection, Executor, MySql, Row, Transaction};
use tokio::sync::Mutex;

use crate::config::StoreDefinition;
use crate::coverage::{
    self, check_deployment_id, CoverageCounts, CoverageFragmentRow, CoverageMetrics,
};
use crate::error::DataStoreError;
use crate::projected::{ActualColumn, ProjectedStore, SqlDialect};

/// This module's dialect, for the shared projected-store SQL builder.
const DIALECT: SqlDialect = SqlDialect::Mysql;

/// A boxed, provably-`Send` future — see [`crate::postgres`] for why the public API boxes.
pub type SendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const WHERE_KEY: &str = " WHERE store_name = ? AND store_key = ?";

/// A `sql`-provider data store on MySQL/MariaDB over the module's OWN connection. Cheap to
/// clone; all clones share one pool and one run-once migration gate.
#[derive(Clone)]
pub struct MysqlDataStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    name: String,
    pool: MySqlPool,
    options: MySqlConnectOptions,
    migrations: Vec<String>,
    migrated: Mutex<bool>,
    /// Pre-cutover compatibility: the legacy `(tenant, module, version)` triple stamped
    /// into the 5-column table on INSERT (see [`crate::postgres`]). `None` = cutover shape.
    legacy_namespace: Option<(String, String, String)>,
    /// The store's declared row structure (see [`crate::postgres`]): `Some` serves the store from
    /// the author's typed-column table; `None` is exactly today's opaque key→JSON behaviour.
    projected: Option<Arc<ProjectedStore>>,
}

impl MysqlDataStore {
    /// Build a store from its declaration on the cutover table shape.
    pub fn from_definition(
        def: &StoreDefinition,
        migrations: Vec<String>,
    ) -> Result<MysqlDataStore, DataStoreError> {
        Self::from_definition_with_namespace(def, migrations, None)
    }

    /// [`Self::from_definition`] with the pre-cutover namespace triple (5-column tables).
    pub fn from_definition_with_namespace(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
    ) -> Result<MysqlDataStore, DataStoreError> {
        Self::from_definition_projected(def, migrations, legacy_namespace, None)
    }

    /// [`Self::from_definition_with_namespace`] plus the store's resolved row PROJECTION — see
    /// [`crate::postgres::PostgresDataStore::from_definition_projected`] for the contract.
    pub fn from_definition_projected(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
        projected: Option<ProjectedStore>,
    ) -> Result<MysqlDataStore, DataStoreError> {
        Ok(MysqlDataStore {
            inner: Arc::new(store_inner(def, migrations, legacy_namespace, projected)?),
        })
    }

    /// The store's declared name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Autocommit read; `None` when the key is absent.
    pub fn get(&self, key: &str) -> SendFuture<'static, Result<Option<Json>, DataStoreError>> {
        Box::pin(store_get(Arc::clone(&self.inner), key.to_string()))
    }

    /// Autocommit write (insert-or-replace).
    pub fn put(&self, key: &str, value: &Json) -> SendFuture<'static, Result<(), DataStoreError>> {
        Box::pin(store_put(
            Arc::clone(&self.inner),
            key.to_string(),
            value.clone(),
        ))
    }

    /// Autocommit delete; a no-op when the key is absent.
    pub fn delete(&self, key: &str) -> SendFuture<'static, Result<(), DataStoreError>> {
        Box::pin(store_delete(Arc::clone(&self.inner), key.to_string()))
    }

    /// The revision at `key` — `0` when absent, bumped on every write.
    pub fn revision(&self, key: &str) -> SendFuture<'static, Result<i64, DataStoreError>> {
        Box::pin(store_revision(Arc::clone(&self.inner), key.to_string()))
    }

    /// Autocommit compare-and-set (`<q:store expect="unchanged">`); `false` = conflict.
    pub fn put_if_revision(
        &self,
        key: &str,
        value: &Json,
        expected_rev: i64,
    ) -> SendFuture<'static, Result<bool, DataStoreError>> {
        Box::pin(store_put_if_revision(
            Arc::clone(&self.inner),
            key.to_string(),
            value.clone(),
            expected_rev,
        ))
    }

    /// Opens a caller-managed transaction (the atomicity boundary). Dropping without
    /// `commit` rolls back.
    pub fn begin(&self) -> SendFuture<'static, Result<MysqlDataStoreTx, DataStoreError>> {
        Box::pin(store_begin(Arc::clone(&self.inner)))
    }
}

/// Resolve a store declaration into its live connection state — see
/// [`crate::postgres`] for the contract. Shared by the KV store above and the
/// [`MysqlCoverageStore`] below, which differ only in WHOSE migration scripts they carry.
fn store_inner(
    def: &StoreDefinition,
    migrations: Vec<String>,
    legacy_namespace: Option<(String, String, String)>,
    projected: Option<ProjectedStore>,
) -> Result<StoreInner, DataStoreError> {
    let url = def
        .resolved("sql.url")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| no_connection(&def.name))?;
    let mut options = MySqlConnectOptions::from_str(&normalize_url(&url)).map_err(|e| {
        DataStoreError::with_source(
            format!(
                "mysql data store '{}' has an invalid connection URL",
                def.name
            ),
            e,
        )
    })?;
    if let Some(user) = def.resolved("sql.username")? {
        options = options.username(&user);
    }
    if let Some(password) = def.resolved("sql.password")? {
        options = options.password(&password);
    }
    // Config-driven pool sizing (datastores.yaml `maxConnections` / `acquireTimeout`),
    // defaulting to the shared ceiling when unset — no hardcoded pool size.
    let pool_config = def.pool_config()?;
    let mut pool_options = MySqlPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // READ COMMITTED so a read after the lock-acquiring UPDATE sees the
                // latest committed value, not the REPEATABLE READ snapshot.
                conn.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    .await?;
                Ok(())
            })
        });
    if let Some(acquire_timeout) = pool_config.acquire_timeout {
        pool_options = pool_options.acquire_timeout(acquire_timeout);
    }
    let pool = pool_options.connect_lazy_with(options.clone());
    Ok(StoreInner {
        name: def.name.clone(),
        pool,
        options,
        migrations,
        migrated: Mutex::new(false),
        legacy_namespace,
        projected: projected.map(Arc::new),
    })
}

// ---- coverage store (engine-owned schema, author-chosen connection) --------

/// The coverage store on a MySQL/MariaDB connection the AUTHOR declared — the MySQL half of
/// [`crate::postgres::PostgresCoverageStore`]; read that type for the ownership contract. Same
/// declaration, same lazy pool, same run-once first-use gate, ENGINE-shipped DDL
/// ([`crate::coverage::shipped_ddl`]).
///
/// Dialect notes: the flag is a `BOOLEAN` (`TINYINT`) compared `= 1` / `= 0`, and the mark's
/// insert half relies on duplicate-key REJECTION (SQLSTATE `23`) rather than `INSERT IGNORE`
/// (which would downgrade unrelated errors) or `ON DUPLICATE KEY UPDATE` (whose affected-rows
/// cannot distinguish "inserted" from "already covered" under `CLIENT_FOUND_ROWS`).
#[derive(Clone)]
pub struct MysqlCoverageStore {
    inner: Arc<StoreInner>,
}

impl MysqlCoverageStore {
    /// Build the coverage store over the declared `coverage` store's own connection.
    pub fn from_definition(def: &StoreDefinition) -> Result<MysqlCoverageStore, DataStoreError> {
        Ok(MysqlCoverageStore {
            inner: Arc::new(store_inner(
                def,
                crate::coverage::shipped_ddl(DIALECT),
                None,
                None,
            )?),
        })
    }

    /// The declared store name this coverage store was built from.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Open a transaction, applying the engine's coverage DDL on first use.
    async fn begin(&self) -> Result<Transaction<'static, MySql>, DataStoreError> {
        ensure_migrated(&self.inner).await?;
        self.inner
            .pool
            .begin()
            .await
            .map_err(|e| cov_err(&self.inner.name, "connect", e))
    }

    /// A connection primed for a REPEATABLE READ snapshot. The pool pins connections to READ
    /// COMMITTED for the KV read-modify-write contract, and MySQL refuses to change transaction
    /// characteristics once a transaction is open — so the level is set BEFORE `begin`, with no
    /// scope keyword, which applies it to the NEXT transaction on this connection only and leaves
    /// the session default (and every other borrower of the pool) untouched. The caller opens the
    /// transaction off the returned connection.
    async fn snapshot_conn(&self) -> Result<PoolConnection<MySql>, DataStoreError> {
        ensure_migrated(&self.inner).await?;
        let mut conn = self
            .inner
            .pool
            .acquire()
            .await
            .map_err(|e| cov_err(&self.inner.name, "connect", e))?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *conn)
            .await
            .map_err(|e| cov_err(&self.inner.name, "snapshot isolation", e))?;
        Ok(conn)
    }

    /// Idempotently seed every declared path as uncovered; returns how many rows were newly
    /// inserted. A duplicate key means the path was already seeded — not an error.
    pub async fn seed_declared(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, DataStoreError> {
        check_deployment_id(deployment_id)?;
        if path_urns.is_empty() {
            return Ok(0);
        }
        let sql = coverage::seed_sql(DIALECT);
        let mut tx = self.begin().await?;
        let mut inserted = 0u64;
        for urn in path_urns {
            match sqlx::query(&sql)
                .bind(deployment_id)
                .bind(urn)
                .execute(&mut *tx)
                .await
            {
                Ok(done) => inserted += done.rows_affected(),
                Err(e) if is_duplicate_key(&e) => {}
                Err(e) => return Err(cov_err(&self.inner.name, "seed", e)),
            }
        }
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "seed commit", e))?;
        Ok(inserted)
    }

    /// Flip a declared path to covered; `true` when THIS call newly covered it. The guarded
    /// `UPDATE … AND covered = 0` matches only rows it actually flips, so its row count is the
    /// durable first-covers-wins answer even under `CLIENT_FOUND_ROWS`.
    pub async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin().await?;
        let flipped = sqlx::query(&coverage::mark_update_sql(DIALECT))
            .bind(deployment_id)
            .bind(path_urn)
            .execute(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "mark", e))?
            .rows_affected();
        let newly = if flipped > 0 {
            true
        } else {
            match sqlx::query(&coverage::mark_insert_sql(DIALECT))
                .bind(deployment_id)
                .bind(path_urn)
                .execute(&mut *tx)
                .await
            {
                Ok(done) => done.rows_affected() > 0,
                // A concurrent marker (or an already-covered seeded row) got there first.
                Err(e) if is_duplicate_key(&e) => false,
                Err(e) => return Err(cov_err(&self.inner.name, "mark insert", e)),
            }
        };
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "mark commit", e))?;
        Ok(newly)
    }

    /// The subset of `path_urns` currently covered — one round trip per chunk.
    pub async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        if path_urns.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut conn = self.snapshot_conn().await?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| cov_err(&self.inner.name, "connect", e))?;
        let mut covered = BTreeSet::new();
        for chunk in path_urns.chunks(coverage::IN_CHUNK) {
            let sql = coverage::covered_among_sql(DIALECT, chunk.len());
            let mut query = sqlx::query(&sql).bind(deployment_id);
            for urn in chunk {
                query = query.bind(urn);
            }
            let rows = query
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| cov_err(&self.inner.name, "covered_among", e))?;
            for row in &rows {
                covered.insert(text_of(row, 0)?);
            }
        }
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "covered_among commit", e))?;
        Ok(covered)
    }

    /// Clear the covered flag on the named paths, returning how many were actually flipped.
    pub async fn clear_paths(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, DataStoreError> {
        check_deployment_id(deployment_id)?;
        if path_urns.is_empty() {
            return Ok(0);
        }
        let mut tx = self.begin().await?;
        let mut cleared = 0u64;
        for chunk in path_urns.chunks(coverage::IN_CHUNK) {
            let sql = coverage::clear_paths_sql(DIALECT, chunk.len());
            let mut query = sqlx::query(&sql).bind(deployment_id);
            for urn in chunk {
                query = query.bind(urn);
            }
            cleared += query
                .execute(&mut *tx)
                .await
                .map_err(|e| cov_err(&self.inner.name, "clear", e))?
                .rows_affected();
        }
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "clear commit", e))?;
        Ok(cleared)
    }

    /// The counts alone, as one aggregate.
    pub async fn count_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageCounts, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin().await?;
        let row = sqlx::query(&coverage::counts_sql(DIALECT))
            .bind(deployment_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "count", e))?;
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "count commit", e))?;
        Ok(CoverageCounts {
            total: row.get::<i64, _>(0) as u64,
            covered: row.get::<i64, _>(1) as u64,
        })
    }

    /// Counts + the uncovered set, from ONE REPEATABLE READ snapshot.
    pub async fn read_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageMetrics, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut conn = self.snapshot_conn().await?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| cov_err(&self.inner.name, "connect", e))?;
        let counts = sqlx::query(&coverage::counts_sql(DIALECT))
            .bind(deployment_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "count", e))?;
        let uncovered = sqlx::query(&coverage::uncovered_sql(DIALECT))
            .bind(deployment_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "uncovered", e))?;
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "read commit", e))?;
        Ok(CoverageMetrics {
            total: counts.get::<i64, _>(0) as u64,
            covered: counts.get::<i64, _>(1) as u64,
            uncovered: uncovered
                .iter()
                .map(|r| text_of(r, 0))
                .collect::<Result<Vec<String>, _>>()?,
        })
    }

    /// Append a reconstruction fragment.
    pub async fn write_fragment(
        &self,
        deployment_id: &str,
        row: &CoverageFragmentRow,
    ) -> Result<(), DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin().await?;
        sqlx::query(&coverage::write_fragment_sql(DIALECT))
            .bind(deployment_id)
            .bind(&row.route_urn)
            .bind(&row.segment_process)
            .bind(&row.instance_id)
            .bind(&row.business_key)
            .bind(&row.trace_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment insert", e))?;
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment commit", e))?;
        Ok(())
    }

    /// All reconstruction fragments for the deployment, in insertion order.
    pub async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragmentRow>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin().await?;
        let rows = sqlx::query(&coverage::read_fragments_sql(DIALECT))
            .bind(deployment_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment read", e))?;
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment read commit", e))?;
        // The text columns are `utf8mb4_bin`, which the server flags as binary on the wire, so
        // they decode as bytes — the same `str_col` handling every MySQL store here does.
        rows.iter()
            .map(|r| {
                Ok(CoverageFragmentRow {
                    route_urn: text_of(r, "route_urn")?,
                    segment_process: text_of(r, "segment_process")?,
                    instance_id: text_of(r, "instance_id")?,
                    business_key: opt_text_of(r, "business_key")?,
                    trace_id: opt_text_of(r, "trace_id")?,
                })
            })
            .collect()
    }

    /// Re-seed every declared path to uncovered and drop the deployment's fragments.
    pub async fn reset(&self, deployment_id: &str) -> Result<(), DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin().await?;
        sqlx::query(&coverage::reset_metrics_sql(DIALECT))
            .bind(deployment_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "reset", e))?;
        sqlx::query(&coverage::delete_fragments_sql(DIALECT))
            .bind(deployment_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment reset", e))?;
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "reset commit", e))?;
        Ok(())
    }
}

/// Reads a text column that may arrive as BYTES.
///
/// The coverage tables are `utf8mb4_bin`, and a binary-collated column is flagged binary on the
/// wire — but not identically by both engines this module serves (MariaDB hands back VARBINARY
/// where MySQL may hand back VARCHAR). So decode as text first and fall back to bytes rather than
/// pinning one engine's answer; the stored content is UTF-8 the store layer itself wrote.
fn text_of<I>(row: &sqlx::mysql::MySqlRow, col: I) -> Result<String, DataStoreError>
where
    I: sqlx::ColumnIndex<sqlx::mysql::MySqlRow> + Clone + std::fmt::Debug,
{
    if let Ok(text) = row.try_get::<String, _>(col.clone()) {
        return Ok(text);
    }
    let bytes: Vec<u8> = row
        .try_get(col.clone())
        .map_err(|e| DataStoreError::with_source(format!("coverage: read column {col:?}"), e))?;
    String::from_utf8(bytes).map_err(|e| {
        DataStoreError::with_source(format!("coverage: column {col:?} is not UTF-8"), e)
    })
}

/// Nullable [`text_of`].
fn opt_text_of<I>(row: &sqlx::mysql::MySqlRow, col: I) -> Result<Option<String>, DataStoreError>
where
    I: sqlx::ColumnIndex<sqlx::mysql::MySqlRow> + Clone + std::fmt::Debug,
{
    if let Ok(text) = row.try_get::<Option<String>, _>(col.clone()) {
        return Ok(text);
    }
    let bytes: Option<Vec<u8>> = row
        .try_get(col.clone())
        .map_err(|e| DataStoreError::with_source(format!("coverage: read column {col:?}"), e))?;
    match bytes {
        None => Ok(None),
        Some(bytes) => String::from_utf8(bytes).map(Some).map_err(|e| {
            DataStoreError::with_source(format!("coverage: column {col:?} is not UTF-8"), e)
        }),
    }
}

/// A coverage-store failure, named by store and operation.
fn cov_err(store: &str, op: &str, e: sqlx::Error) -> DataStoreError {
    DataStoreError::with_source(format!("coverage store '{store}': {op} failed"), e)
}

// ---- store-level bodies (owned state — `'static`, freely spawnable) --------

async fn store_begin(inner: Arc<StoreInner>) -> Result<MysqlDataStoreTx, DataStoreError> {
    ensure_migrated(&inner).await?;
    let tx = inner.pool.begin().await.map_err(|e| {
        DataStoreError::with_source(
            format!("failed to open connection for store '{}'", inner.name),
            e,
        )
    })?;
    Ok(MysqlDataStoreTx {
        tx: Some(tx),
        store: inner.name.clone(),
        legacy_namespace: inner.legacy_namespace.clone(),
        projected: inner.projected.clone(),
    })
}

async fn store_get(inner: Arc<StoreInner>, key: String) -> Result<Option<Json>, DataStoreError> {
    let mut tx = store_begin(inner).await?;
    let value = tx_select(&mut tx, &key).await?;
    tx.commit().await?;
    Ok(value)
}

async fn store_put(inner: Arc<StoreInner>, key: String, value: Json) -> Result<(), DataStoreError> {
    let mut tx = store_begin(inner).await?;
    tx_put(&mut tx, &key, &value).await?;
    tx.commit().await
}

async fn store_delete(inner: Arc<StoreInner>, key: String) -> Result<(), DataStoreError> {
    let mut tx = store_begin(inner).await?;
    tx_delete(&mut tx, &key).await?;
    tx.commit().await
}

async fn store_revision(inner: Arc<StoreInner>, key: String) -> Result<i64, DataStoreError> {
    let mut tx = store_begin(inner).await?;
    let rev = tx_revision(&mut tx, &key).await?;
    tx.commit().await?;
    Ok(rev)
}

async fn store_put_if_revision(
    inner: Arc<StoreInner>,
    key: String,
    value: Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    let mut tx = store_begin(inner).await?;
    let applied = tx_put_if_revision(&mut tx, &key, &value, expected_rev).await?;
    tx.commit().await?;
    Ok(applied)
}

/// Run the module-resident idempotent migrations once per store instance, serialised
/// across replicas by a `GET_LOCK` named lock. As in the reference dialect the scripts run
/// through `sqlx::raw_sql` (multi-statement DDL), whose future is not provably `Send` under
/// the sqlx `Executor` HRTB generalisation bug — so the run happens on a dedicated
/// connection inside `spawn_blocking` + a current-thread runtime, where `Send` is not
/// required. Fail-closed: the flag stays `false` on error so a later use retries.
/// (Also the FIRST-USE gate for a projected store's table verification — same once-per-instance
/// flag, so a projected store costs one extra catalog round-trip on first use and none after.)
async fn ensure_migrated(inner: &StoreInner) -> Result<(), DataStoreError> {
    let mut migrated = inner.migrated.lock().await;
    if *migrated {
        return Ok(());
    }
    if !inner.migrations.is_empty() {
        let options = inner.options.clone();
        let name = inner.name.clone();
        let scripts = inner.migrations.clone();
        tokio::task::spawn_blocking(move || run_migration_scripts(options, &name, &scripts))
            .await
            .map_err(|e| DataStoreError::with_source("migration task failed", e))??;
    }
    if let Some(projected) = &inner.projected {
        verify_projection(&inner.pool, projected).await?;
    }
    *migrated = true;
    Ok(())
}

/// First-use verification (design §4.5, RULED fail-closed) — see
/// [`crate::postgres`]. The catalog probe is scoped to `DATABASE()` on this dialect.
async fn verify_projection(
    pool: &MySqlPool,
    projected: &ProjectedStore,
) -> Result<(), DataStoreError> {
    let sql = projected.columns_probe_sql(DIALECT);
    let rows = sqlx::query(&sql)
        .bind(projected.table())
        .fetch_all(pool)
        .await
        .map_err(|e| {
            DataStoreError::with_source(
                format!(
                    "data store '{}' could not read the columns of table '{}' to verify its \
                     declared structure",
                    projected.store(),
                    projected.table()
                ),
                e,
            )
        })?;
    let actual: Vec<ActualColumn> = rows
        .iter()
        .map(|row| ActualColumn {
            name: row.get::<String, _>(0),
            nullable: row.get::<String, _>(1).eq_ignore_ascii_case("YES"),
            has_default: row.get::<String, _>(2) == "Y",
        })
        .collect();
    projected.verify(&actual)
}

fn run_migration_scripts(
    options: MySqlConnectOptions,
    name: &str,
    scripts: &[String],
) -> Result<(), DataStoreError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| DataStoreError::with_source("failed to build migration runtime", e))?;
    // MySQL named locks are capped at 64 bytes; the store name is short (VARCHAR(128) key
    // column) but truncate defensively.
    let lock_key: String = format!("sutra_ds_{name}").chars().take(64).collect();
    runtime.block_on(async move {
        let mut conn: MySqlConnection = options
            .connect()
            .await
            .map_err(|e| migration_err(name, e))?;
        sqlx::query("SELECT GET_LOCK(?, 30)")
            .bind(&lock_key)
            .execute(&mut conn)
            .await
            .map_err(|e| migration_err(name, e))?;
        let mut result = Ok(());
        for script in scripts {
            if let Err(e) = sqlx::raw_sql(script).execute(&mut conn).await {
                result = Err(migration_err(name, e));
                break;
            }
        }
        let _ = sqlx::query("SELECT RELEASE_LOCK(?)")
            .bind(&lock_key)
            .execute(&mut conn)
            .await;
        let _ = conn.close().await;
        result
    })
}

fn migration_err(store: &str, e: sqlx::Error) -> DataStoreError {
    DataStoreError::with_source(format!("migration failed for data store '{store}'"), e)
}

fn no_connection(name: &str) -> DataStoreError {
    DataStoreError::new(format!(
        "sql data store '{name}' declares no connection. A store must own its connection \
         in datastores.yaml (sql.url + credentials, or *-ref secret-refs). The engine's \
         datasource is reserved for engine-internal tables and is never a module's store — \
         point the store at its own database."
    ))
}

/// Normalise the store's declared `sql.url` for sqlx: its MySQL driver serves MariaDB too but
/// only recognises the `mysql://` scheme, so `mariadb://` is rewritten. Any other URL is passed
/// through untouched.
fn normalize_url(url: &str) -> String {
    match url.strip_prefix("mariadb://") {
        Some(rest) => format!("mysql://{rest}"),
        None => url.to_string(),
    }
}

/// A caller-managed transaction over one store — one connection for its whole life, READ
/// COMMITTED (pinned on the pool), commit/rollback as the atomicity boundary,
/// drop-without-commit = rollback.
pub struct MysqlDataStoreTx {
    tx: Option<Transaction<'static, MySql>>,
    store: String,
    legacy_namespace: Option<(String, String, String)>,
    /// `Some` for a store that declares a `structure`: the same operations run against the
    /// author's typed-column table instead of the generic blob table.
    projected: Option<Arc<ProjectedStore>>,
}

impl MysqlDataStoreTx {
    /// Read within the transaction; `None` when the key is absent.
    pub fn get<'a>(
        &'a mut self,
        key: &str,
    ) -> SendFuture<'a, Result<Option<Json>, DataStoreError>> {
        Box::pin(tx_select_owned(self, key.to_string()))
    }

    /// Pessimistic-locking read: takes the row lock with a rev-bumping `UPDATE` (the
    /// portable `SELECT … FOR UPDATE` substitute). On an absent key it touches no row.
    pub fn get_for_update<'a>(
        &'a mut self,
        key: &str,
    ) -> SendFuture<'a, Result<Option<Json>, DataStoreError>> {
        Box::pin(tx_get_for_update_owned(self, key.to_string()))
    }

    /// Write (insert-or-replace) within the transaction — the portable upsert.
    pub fn put<'a>(
        &'a mut self,
        key: &str,
        value: &Json,
    ) -> SendFuture<'a, Result<(), DataStoreError>> {
        Box::pin(tx_put_owned(self, key.to_string(), value.clone()))
    }

    /// Delete within the transaction; a no-op when the key is absent.
    pub fn delete<'a>(&'a mut self, key: &str) -> SendFuture<'a, Result<(), DataStoreError>> {
        Box::pin(tx_delete_owned(self, key.to_string()))
    }

    /// The revision at `key` within this transaction — `0` when absent.
    pub fn revision<'a>(&'a mut self, key: &str) -> SendFuture<'a, Result<i64, DataStoreError>> {
        Box::pin(tx_revision_owned(self, key.to_string()))
    }

    /// Compare-and-set (`<q:store expect="unchanged">`).
    pub fn put_if_revision<'a>(
        &'a mut self,
        key: &str,
        value: &Json,
        expected_rev: i64,
    ) -> SendFuture<'a, Result<bool, DataStoreError>> {
        Box::pin(tx_put_if_revision_owned(
            self,
            key.to_string(),
            value.clone(),
            expected_rev,
        ))
    }

    /// Commits and releases the transaction.
    pub async fn commit(mut self) -> Result<(), DataStoreError> {
        let store = self.store.clone();
        let tx = self.tx.take().ok_or_else(|| released(&store))?;
        tx.commit()
            .await
            .map_err(|e| DataStoreError::with_source(format!("commit failed for '{store}'"), e))
    }

    /// Rolls back and releases the transaction. (Dropping without either call also rolls
    /// back.)
    pub async fn rollback(mut self) -> Result<(), DataStoreError> {
        let store = self.store.clone();
        let tx = self.tx.take().ok_or_else(|| released(&store))?;
        tx.rollback()
            .await
            .map_err(|e| DataStoreError::with_source(format!("rollback failed for '{store}'"), e))
    }

    fn conn(&mut self) -> Result<&mut Transaction<'static, MySql>, DataStoreError> {
        let store = &self.store;
        self.tx.as_mut().ok_or_else(|| {
            DataStoreError::new(format!("transaction on '{store}' already released"))
        })
    }
}

// ---- transaction-level bodies (single `&mut` borrow + owned params) --------

async fn tx_select_owned(
    tx: &mut MysqlDataStoreTx,
    key: String,
) -> Result<Option<Json>, DataStoreError> {
    tx_select(tx, &key).await
}

async fn tx_get_for_update_owned(
    tx: &mut MysqlDataStoreTx,
    key: String,
) -> Result<Option<Json>, DataStoreError> {
    let store = tx.store.clone();
    match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.lock_sql(DIALECT);
            sqlx::query(&sql)
                .bind(&key)
                .execute(&mut **tx.conn()?)
                .await
                .map_err(|e| op_err(&store, &key, "lock-for-update", e))?;
        }
        None => {
            let sql = format!("UPDATE data_store SET rev = rev + 1{WHERE_KEY}");
            sqlx::query(&sql)
                .bind(&store)
                .bind(&key)
                .execute(&mut **tx.conn()?)
                .await
                .map_err(|e| op_err(&store, &key, "lock-for-update", e))?;
        }
    }
    tx_select(tx, &key).await
}

async fn tx_put_owned(
    tx: &mut MysqlDataStoreTx,
    key: String,
    value: Json,
) -> Result<(), DataStoreError> {
    tx_put(tx, &key, &value).await
}

async fn tx_delete_owned(tx: &mut MysqlDataStoreTx, key: String) -> Result<(), DataStoreError> {
    tx_delete(tx, &key).await
}

async fn tx_revision_owned(tx: &mut MysqlDataStoreTx, key: String) -> Result<i64, DataStoreError> {
    tx_revision(tx, &key).await
}

async fn tx_put_if_revision_owned(
    tx: &mut MysqlDataStoreTx,
    key: String,
    value: Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    tx_put_if_revision(tx, &key, &value, expected_rev).await
}

async fn tx_select(tx: &mut MysqlDataStoreTx, key: &str) -> Result<Option<Json>, DataStoreError> {
    let store = tx.store.clone();
    if let Some(projected) = tx.projected.clone() {
        let sql = projected.select_sql(DIALECT);
        let row = sqlx::query(&sql)
            .bind(key)
            .fetch_optional(&mut **tx.conn()?)
            .await
            .map_err(|e| op_err(&store, key, "get", e))?;
        return Ok(row.map(|r| {
            let cells: Vec<Option<String>> = (0..projected.projection().fields.len())
                .map(|i| r.get::<Option<String>, _>(i))
                .collect();
            projected.row_to_json(&cells)
        }));
    }
    let sql = format!("SELECT store_value FROM data_store{WHERE_KEY}");
    let row = sqlx::query(&sql)
        .bind(&store)
        .bind(key)
        .fetch_optional(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "get", e))?;
    match row {
        None => Ok(None),
        Some(r) => {
            let text: String = r.get(0);
            serde_json::from_str(&text).map(Some).map_err(|e| {
                DataStoreError::with_source(
                    format!("stored value at '{store}'[{key}] is not valid JSON"),
                    e,
                )
            })
        }
    }
}

async fn tx_put(tx: &mut MysqlDataStoreTx, key: &str, value: &Json) -> Result<(), DataStoreError> {
    // The projected path marshals FIRST, so an undeclared field is refused before any statement
    // runs — a rejected write touches nothing (design §4.2).
    if let Some(projected) = tx.projected.clone() {
        let cells = projected.bind_values(key, value)?;
        if tx_update_projected(tx, &projected, key, &cells).await? > 0 {
            return Ok(());
        }
        return match tx_insert_projected(tx, &projected, key, &cells).await {
            Ok(()) => Ok(()),
            Err(e) if is_duplicate_key(&e) => {
                // Another writer inserted the key first — the retried UPDATE IS the write, so
                // its row count is load-bearing: zero means the row went away again. (Every
                // update also bumps `rev`, so a matched row always counts as changed, which is
                // what makes the count trustworthy under MySQL's changed-rows semantics.)
                let store = tx.store.clone();
                match tx_update_projected(tx, &projected, key, &cells).await? {
                    0 => Err(vanished_row_err(&store, key)),
                    _ => Ok(()),
                }
            }
            Err(e) => {
                let store = tx.store.clone();
                Err(op_err(&store, key, "put", e))
            }
        };
    }
    let json = value.to_string();
    if tx_update(tx, key, &json).await? > 0 {
        return Ok(());
    }
    match tx_insert(tx, key, &json).await {
        Ok(()) => Ok(()),
        Err(e) if is_duplicate_key(&e) => {
            let store = tx.store.clone();
            match tx_update(tx, key, &json).await? {
                0 => Err(vanished_row_err(&store, key)),
                _ => Ok(()),
            }
        }
        Err(e) => {
            let store = tx.store.clone();
            Err(op_err(&store, key, "put", e))
        }
    }
}

async fn tx_delete(tx: &mut MysqlDataStoreTx, key: &str) -> Result<(), DataStoreError> {
    let store = tx.store.clone();
    match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.delete_sql(DIALECT);
            sqlx::query(&sql)
                .bind(key)
                .execute(&mut **tx.conn()?)
                .await
                .map_err(|e| op_err(&store, key, "delete", e))?;
        }
        None => {
            let sql = format!("DELETE FROM data_store{WHERE_KEY}");
            sqlx::query(&sql)
                .bind(&store)
                .bind(key)
                .execute(&mut **tx.conn()?)
                .await
                .map_err(|e| op_err(&store, key, "delete", e))?;
        }
    }
    Ok(())
}

async fn tx_revision(tx: &mut MysqlDataStoreTx, key: &str) -> Result<i64, DataStoreError> {
    let store = tx.store.clone();
    let row = match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.revision_sql(DIALECT);
            sqlx::query(&sql)
                .bind(key)
                .fetch_optional(&mut **tx.conn()?)
                .await
        }
        None => {
            let sql = format!("SELECT rev FROM data_store{WHERE_KEY}");
            sqlx::query(&sql)
                .bind(&store)
                .bind(key)
                .fetch_optional(&mut **tx.conn()?)
                .await
        }
    }
    .map_err(|e| op_err(&store, key, "revision", e))?;
    Ok(row.map(|r| r.get::<i64, _>(0)).unwrap_or(0))
}

/// The projected upsert's `UPDATE` half — binds `[fields…, key]`, in statement-text order (which
/// on this dialect IS the positional `?` order).
async fn tx_update_projected(
    tx: &mut MysqlDataStoreTx,
    projected: &ProjectedStore,
    key: &str,
    cells: &[Option<String>],
) -> Result<u64, DataStoreError> {
    let sql = projected.update_sql(DIALECT);
    let store = tx.store.clone();
    let mut query = sqlx::query(&sql);
    for cell in cells {
        query = query.bind(cell.as_deref());
    }
    let result = query
        .bind(key)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "put", e))?;
    Ok(result.rows_affected())
}

/// The projected upsert's `INSERT` half — binds `[key, fields…]`, in statement-text order.
async fn tx_insert_projected(
    tx: &mut MysqlDataStoreTx,
    projected: &ProjectedStore,
    key: &str,
    cells: &[Option<String>],
) -> Result<(), sqlx::Error> {
    let sql = projected.insert_sql(DIALECT);
    let conn = tx.conn().map_err(|_| sqlx::Error::PoolClosed)?;
    let mut query = sqlx::query(&sql).bind(key);
    for cell in cells {
        query = query.bind(cell.as_deref());
    }
    query.execute(&mut **conn).await?;
    Ok(())
}

async fn tx_put_if_revision(
    tx: &mut MysqlDataStoreTx,
    key: &str,
    value: &Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    if let Some(projected) = tx.projected.clone() {
        let cells = projected.bind_values(key, value)?;
        if expected_rev <= 0 {
            return match tx_insert_projected(tx, &projected, key, &cells).await {
                Ok(()) => Ok(true),
                Err(e) if is_duplicate_key(&e) => Ok(false),
                Err(e) => {
                    let store = tx.store.clone();
                    Err(op_err(&store, key, "compare-and-set", e))
                }
            };
        }
        let sql = projected.update_if_revision_sql(DIALECT);
        let store = tx.store.clone();
        let mut query = sqlx::query(&sql);
        for cell in &cells {
            query = query.bind(cell.as_deref());
        }
        let result = query
            .bind(key)
            .bind(expected_rev)
            .execute(&mut **tx.conn()?)
            .await
            .map_err(|e| op_err(&store, key, "compare-and-set", e))?;
        return Ok(result.rows_affected() > 0);
    }
    let json = value.to_string();
    if expected_rev <= 0 {
        return match tx_insert(tx, key, &json).await {
            Ok(()) => Ok(true),
            Err(e) if is_duplicate_key(&e) => Ok(false),
            Err(e) => {
                let store = tx.store.clone();
                Err(op_err(&store, key, "compare-and-set", e))
            }
        };
    }
    let sql = format!(
        "UPDATE data_store SET store_value = ?, rev = rev + 1, \
         updated_at = CURRENT_TIMESTAMP{WHERE_KEY} AND rev = ?"
    );
    let store = tx.store.clone();
    let result = sqlx::query(&sql)
        .bind(&json)
        .bind(&store)
        .bind(key)
        .bind(expected_rev)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "compare-and-set", e))?;
    Ok(result.rows_affected() > 0)
}

async fn tx_update(
    tx: &mut MysqlDataStoreTx,
    key: &str,
    json: &str,
) -> Result<u64, DataStoreError> {
    let sql = format!(
        "UPDATE data_store SET store_value = ?, rev = rev + 1, \
         updated_at = CURRENT_TIMESTAMP{WHERE_KEY}"
    );
    let store = tx.store.clone();
    let result = sqlx::query(&sql)
        .bind(json)
        .bind(&store)
        .bind(key)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "put", e))?;
    Ok(result.rows_affected())
}

/// Cutover insert (`(store_name, store_key, …)`), or the legacy 5-column stamp when a
/// pre-cutover namespace triple is configured.
async fn tx_insert(tx: &mut MysqlDataStoreTx, key: &str, json: &str) -> Result<(), sqlx::Error> {
    let store = tx.store.clone();
    let legacy = tx.legacy_namespace.clone();
    let conn = tx.conn().map_err(|_| sqlx::Error::PoolClosed)?;
    match legacy {
        Some((tenant, module, version)) => {
            sqlx::query(
                "INSERT INTO data_store (tenant_id, module_id, module_version, store_name, \
                 store_key, store_value, rev, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)",
            )
            .bind(&tenant)
            .bind(&module)
            .bind(&version)
            .bind(&store)
            .bind(key)
            .bind(json)
            .execute(&mut **conn)
            .await?;
        }
        None => {
            sqlx::query(
                "INSERT INTO data_store (store_name, store_key, store_value, rev, updated_at) \
                 VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)",
            )
            .bind(&store)
            .bind(key)
            .bind(json)
            .execute(&mut **conn)
            .await?;
        }
    }
    Ok(())
}

fn released(store: &str) -> DataStoreError {
    DataStoreError::new(format!("transaction on '{store}' already released"))
}

fn op_err(store: &str, key: &str, op: &str, e: sqlx::Error) -> DataStoreError {
    DataStoreError::with_source(format!("{op} failed for '{store}'[{key}]"), e)
}

/// Duplicate key ONLY — the insert lost a race and the row is already there.
///
/// MySQL is why the class-`23` shortcut cannot be used at all here: SQLSTATE `23000` is a
/// *single* bucket covering duplicate key (1062), `NOT NULL` (1048), foreign key (1451/1452)
/// and `CHECK` (3819) alike, so the class carries no information. That matters on the
/// PROJECTED path, where the table is the author's own and a `field=`-narrowed create of an
/// absent key binds every other declared column `NULL`: treated as a duplicate, it retries an
/// UPDATE that matches nothing, and — MySQL having no aborted-transaction state to trip over —
/// the write would be reported as successful having touched nothing. `sqlx`'s driver-mapped
/// `ErrorKind::UniqueViolation` reads the native error number, which does discriminate.
fn is_duplicate_key(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if matches!(db.kind(), ErrorKind::UniqueViolation))
}

/// The retried `UPDATE` after a duplicate-key insert matched nothing: the row the insert
/// collided with was deleted in between. Reporting `Ok` there would claim a write that never
/// landed, so it is a loud failure — see the call sites in [`tx_put`].
fn vanished_row_err(store: &str, key: &str) -> DataStoreError {
    DataStoreError::new(format!(
        "put failed for '{store}'[{key}]: the insert collided with an existing row, but the \
         retried update matched none — the row was deleted concurrently. Nothing was written."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_the_mariadb_url_scheme() {
        assert_eq!(normalize_url("mysql://h:3306/db"), "mysql://h:3306/db");
        assert_eq!(normalize_url("mariadb://h:3306/db"), "mysql://h:3306/db");
    }

    #[test]
    fn missing_connection_fails_closed() {
        let def = StoreDefinition {
            name: "accounts".into(),
            store_type: "sql".into(),
            properties: std::collections::BTreeMap::new(),
            structure: None,
        };
        assert!(MysqlDataStore::from_definition(&def, Vec::new()).is_err());
    }
}
