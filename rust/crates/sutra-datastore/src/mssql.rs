//! Microsoft SQL Server user-datastore dialect.
//!
//! sqlx has no SQL Server driver, so this dialect runs on its own TDS client stack
//! (`tiberius` + a small crate-local checkout/checkin pool, the same shape
//! `sutra-persistence` uses). The same store operations are re-stated on the same
//! `(store_name, store_key)` cutover predicate the [`crate::postgres`] reference uses;
//! the store SQL is dialect-neutral (proven portable across dialects), so this module differs
//! only in the driver, the `@Pn` placeholder syntax, and the migration lock.
//!
//! Dialect notes (each pinned by the container suite):
//! - **Explicit transactions.** tiberius is autocommit by default, so every store operation
//!   runs inside `SET XACT_ABORT OFF; BEGIN TRANSACTION`. The lock-acquiring `UPDATE` in
//!   [`MssqlDataStoreTx::get_for_update`] takes a row-exclusive lock held to commit — that
//!   is what serialises concurrent read-modify-write. SQL Server already defaults to READ
//!   COMMITTED, so no isolation pin is needed (unlike MySQL).
//! - **`SET XACT_ABORT OFF`** keeps a duplicate-key violation (errors 2627/2601) from
//!   aborting the whole transaction, so the portable upsert's INSERT→UPDATE fallback runs
//!   in the same transaction — the reference dialect's conflict-ignoring insert.
//! - **Uncommitted-drop rolls back by connection teardown:** a dropped
//!   [`MssqlDataStoreTx`] discards its connection instead of returning it to the pool, so
//!   the server aborts the open transaction (drop is a rollback).

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::Value as Json;
use tiberius::AuthMethod;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::StoreDefinition;
use crate::coverage::{
    self, check_deployment_id, CoverageCounts, CoverageFragmentRow, CoverageMetrics,
};
use crate::error::DataStoreError;
use crate::projected::{ActualColumn, ProjectedStore, SqlDialect};

/// This module's dialect, for the shared projected-store SQL builder.
const DIALECT: SqlDialect = SqlDialect::Mssql;

/// A boxed, provably-`Send` future — the public API boxes for uniformity with the other
/// dialects (tiberius futures are `Send`, so no HRTB workaround is needed here).
pub type SendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The concrete TDS client type (TDS over tokio TCP).
pub type MssqlClient = tiberius::Client<Compat<tokio::net::TcpStream>>;

const WHERE_KEY: &str = " WHERE store_name = @P1 AND store_key = @P2";

// ---- connection config (parsed from the store's declared URL) --------------

/// Connection settings for one SQL Server database, parsed from the store's declared
/// `sql.url` (`sqlserver://host:port;databaseName=db;…`), with credentials overlaid
/// from the separate `sql.username` / `sql.password` secret-refs when present.
#[derive(Debug, Clone)]
struct MssqlConfig {
    host: String,
    port: u16,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    trust_cert: bool,
}

impl MssqlConfig {
    fn parse(url: &str, name: &str) -> Result<MssqlConfig, DataStoreError> {
        let body = url
            .strip_prefix("sqlserver://")
            .or_else(|| url.strip_prefix("mssql://"))
            .ok_or_else(|| {
                DataStoreError::new(format!(
                    "mssql data store '{name}' URL must start with sqlserver:// or mssql:// \
                     (got '{url}')"
                ))
            })?;
        let (authority, tail) = body.split_once(';').unwrap_or((body, ""));
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse::<u16>().map_err(|_| {
                    DataStoreError::new(format!(
                        "mssql data store '{name}' URL has an invalid port in '{authority}'"
                    ))
                })?,
            ),
            None => (authority.to_string(), 1433),
        };
        let mut config = MssqlConfig {
            host,
            port,
            database: None,
            user: None,
            password: None,
            trust_cert: false,
        };
        for pair in tail.split(';').filter(|p| !p.trim().is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match k.trim().to_ascii_lowercase().as_str() {
                "databasename" | "database" => config.database = Some(v.trim().to_string()),
                "user" | "username" | "uid" => config.user = Some(v.trim().to_string()),
                "password" | "pwd" => config.password = Some(v.trim().to_string()),
                "trustservercertificate" => {
                    config.trust_cert =
                        matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "yes")
                }
                _ => {}
            }
        }
        Ok(config)
    }

    fn to_tiberius(&self) -> tiberius::Config {
        let mut config = tiberius::Config::new();
        config.host(&self.host);
        config.port(self.port);
        if let Some(db) = &self.database {
            config.database(db);
        }
        config.authentication(AuthMethod::sql_server(
            self.user.clone().unwrap_or_default(),
            self.password.clone().unwrap_or_default(),
        ));
        if self.trust_cert {
            config.trust_cert();
        }
        config
    }
}

// ---- crate-local checkout/checkin pool (mirrors sutra-persistence) ----------

#[derive(Clone)]
struct MssqlPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: MssqlConfig,
    idle: Mutex<Vec<MssqlClient>>,
}

impl MssqlPool {
    fn new(config: MssqlConfig) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                idle: Mutex::new(Vec::new()),
            }),
        }
    }

    async fn acquire(&self) -> Result<PooledMssql, DataStoreError> {
        let idle = {
            let mut guard = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.pop()
        };
        let client = match idle {
            Some(client) => client,
            None => {
                let config = self.inner.config.to_tiberius();
                let tcp = tokio::net::TcpStream::connect((
                    self.inner.config.host.as_str(),
                    self.inner.config.port,
                ))
                .await
                .map_err(|e| DataStoreError::with_source("mssql tcp connect", e))?;
                tcp.set_nodelay(true)
                    .map_err(|e| DataStoreError::with_source("mssql tcp nodelay", e))?;
                tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|e| DataStoreError::with_source("mssql connect", e))?
            }
        };
        Ok(PooledMssql {
            client: Some(client),
            pool: Arc::clone(&self.inner),
            discard: false,
        })
    }
}

struct PooledMssql {
    client: Option<MssqlClient>,
    pool: Arc<PoolInner>,
    discard: bool,
}

impl PooledMssql {
    fn client(&mut self) -> &mut MssqlClient {
        self.client.as_mut().expect("client present until drop")
    }
}

impl Drop for PooledMssql {
    fn drop(&mut self) {
        if self.discard {
            return; // dropping the client closes the socket; the server rolls back
        }
        if let (Some(client), Ok(mut idle)) = (self.client.take(), self.pool.idle.lock()) {
            idle.push(client);
        }
    }
}

/// Runs a parameterless statement batch to completion.
async fn run_batch(client: &mut MssqlClient, sql: &str) -> Result<(), DataStoreError> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| DataStoreError::with_source("mssql batch", e))?
        .into_results()
        .await
        .map_err(|e| DataStoreError::with_source("mssql batch drain", e))?;
    Ok(())
}

// ---- store -----------------------------------------------------------------

/// A `sql`-provider data store on SQL Server over the module's OWN connection. Cheap to
/// clone; all clones share one pool and one run-once migration gate.
#[derive(Clone)]
pub struct MssqlDataStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    name: String,
    pool: MssqlPool,
    migrations: Vec<String>,
    migrated: tokio::sync::Mutex<bool>,
    legacy_namespace: Option<(String, String, String)>,
    /// The store's declared row structure (see [`crate::postgres`]): `Some` serves the store from
    /// the author's typed-column table; `None` is exactly today's opaque key→JSON behaviour.
    projected: Option<Arc<ProjectedStore>>,
}

impl MssqlDataStore {
    /// Build a store from its declaration on the cutover table shape.
    pub fn from_definition(
        def: &StoreDefinition,
        migrations: Vec<String>,
    ) -> Result<MssqlDataStore, DataStoreError> {
        Self::from_definition_with_namespace(def, migrations, None)
    }

    /// [`Self::from_definition`] with the pre-cutover namespace triple (5-column tables).
    pub fn from_definition_with_namespace(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
    ) -> Result<MssqlDataStore, DataStoreError> {
        Self::from_definition_projected(def, migrations, legacy_namespace, None)
    }

    /// [`Self::from_definition_with_namespace`] plus the store's resolved row PROJECTION — see
    /// [`crate::postgres::PostgresDataStore::from_definition_projected`] for the contract.
    pub fn from_definition_projected(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
        projected: Option<ProjectedStore>,
    ) -> Result<MssqlDataStore, DataStoreError> {
        Ok(MssqlDataStore {
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
    /// `commit` discards the connection, which rolls back server-side.
    pub fn begin(&self) -> SendFuture<'static, Result<MssqlDataStoreTx, DataStoreError>> {
        Box::pin(store_begin(Arc::clone(&self.inner)))
    }
}

/// Resolve a store declaration into its live connection state — see [`crate::postgres`] for the
/// contract. Shared by the KV store above and the [`MssqlCoverageStore`] below, which differ only
/// in WHOSE migration scripts they carry.
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
    let mut config = MssqlConfig::parse(&url, &def.name)?;
    // The yaml pattern keeps credentials in their own secret-refs; when present they
    // win over anything inline in the URL.
    if let Some(user) = def.resolved("sql.username")? {
        config.user = Some(user);
    }
    if let Some(password) = def.resolved("sql.password")? {
        config.password = Some(password);
    }
    Ok(StoreInner {
        name: def.name.clone(),
        pool: MssqlPool::new(config),
        migrations,
        migrated: tokio::sync::Mutex::new(false),
        legacy_namespace,
        projected: projected.map(Arc::new),
    })
}

// ---- coverage store (engine-owned schema, author-chosen connection) --------

/// The coverage store on a SQL Server connection the AUTHOR declared — the T-SQL half of
/// [`crate::postgres::PostgresCoverageStore`]; read that type for the ownership contract. Same
/// declaration, same pool, same run-once first-use gate, ENGINE-shipped DDL
/// ([`crate::coverage::shipped_ddl`]).
///
/// Dialect notes: `covered` is a `BIT`, so it is compared `= 1` / `= 0` (a BIT is a value, not a
/// predicate); `COUNT` returns `INT` here, decoded as `i32`; the mark's insert half relies on
/// duplicate-key rejection (2627/2601) under `SET XACT_ABORT OFF`, so the violation ends the
/// statement and not the transaction; and an operation that fails mid-transaction DISCARDS its
/// connection, which is how the server learns to roll back.
#[derive(Clone)]
pub struct MssqlCoverageStore {
    inner: Arc<StoreInner>,
}

/// One open coverage transaction. Committing marks it released; dropping it un-committed
/// discards the connection, aborting the transaction server-side (the KV transaction's posture).
struct CoverageTx {
    conn: PooledMssql,
    released: bool,
}

impl CoverageTx {
    fn client(&mut self) -> &mut MssqlClient {
        self.conn.client()
    }

    /// Commit, and restore the connection's isolation level before it returns to the pool (a
    /// snapshot read raises it to REPEATABLE READ, and the level is session state).
    async fn commit(mut self, store: &str) -> Result<(), DataStoreError> {
        run_batch(
            self.conn.client(),
            "COMMIT TRANSACTION; SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .await
        .map_err(|e| DataStoreError::with_source(format!("coverage store '{store}'"), e))?;
        self.released = true;
        Ok(())
    }
}

impl Drop for CoverageTx {
    fn drop(&mut self) {
        if !self.released {
            self.conn.discard = true;
        }
    }
}

impl MssqlCoverageStore {
    /// Build the coverage store over the declared `coverage` store's own connection.
    pub fn from_definition(def: &StoreDefinition) -> Result<MssqlCoverageStore, DataStoreError> {
        Ok(MssqlCoverageStore {
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

    /// Open a transaction, applying the engine's coverage DDL on first use. `snapshot` raises the
    /// isolation to REPEATABLE READ, which is what lets the counts and the uncovered list describe
    /// ONE state of the table (see [`crate::coverage`]); [`CoverageTx::commit`] restores it.
    async fn begin(&self, snapshot: bool) -> Result<CoverageTx, DataStoreError> {
        ensure_migrated(&self.inner).await?;
        let mut conn = self.inner.pool.acquire().await?;
        let isolation = if snapshot {
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
        } else {
            ""
        };
        run_batch(
            conn.client(),
            &format!("{isolation}SET XACT_ABORT OFF; BEGIN TRANSACTION"),
        )
        .await?;
        Ok(CoverageTx {
            conn,
            released: false,
        })
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
        let mut tx = self.begin(false).await?;
        let mut inserted = 0u64;
        for urn in path_urns {
            match tx
                .client()
                .execute(sql.as_str(), &[&deployment_id, &urn.as_str()])
                .await
            {
                Ok(done) => inserted += done.total(),
                Err(e) if is_duplicate_key(&e) => {}
                Err(e) => return Err(cov_err(&self.inner.name, "seed", e)),
            }
        }
        tx.commit(&self.inner.name).await?;
        Ok(inserted)
    }

    /// Flip a declared path to covered; `true` when THIS call newly covered it — the guarded
    /// `UPDATE … AND covered = 0` row count, or (for a never-seeded path) the insert's
    /// duplicate-key branch. Never a read-then-write.
    pub async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(false).await?;
        let flipped = tx
            .client()
            .execute(
                coverage::mark_update_sql(DIALECT).as_str(),
                &[&deployment_id, &path_urn],
            )
            .await
            .map_err(|e| cov_err(&self.inner.name, "mark", e))?
            .total();
        let newly = if flipped > 0 {
            true
        } else {
            match tx
                .client()
                .execute(
                    coverage::mark_insert_sql(DIALECT).as_str(),
                    &[&deployment_id, &path_urn],
                )
                .await
            {
                Ok(done) => done.total() > 0,
                Err(e) if is_duplicate_key(&e) => false,
                Err(e) => return Err(cov_err(&self.inner.name, "mark insert", e)),
            }
        };
        tx.commit(&self.inner.name).await?;
        Ok(newly)
    }

    /// The subset of `path_urns` currently covered — one round trip per chunk (SQL Server caps a
    /// statement at 2100 parameters), all inside one snapshot.
    pub async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        if path_urns.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut tx = self.begin(true).await?;
        let mut covered = BTreeSet::new();
        for chunk in path_urns.chunks(coverage::IN_CHUNK) {
            let sql = coverage::covered_among_sql(DIALECT, chunk.len());
            let mut params: Vec<&dyn tiberius::ToSql> = vec![&deployment_id];
            params.extend(chunk.iter().map(|u| u as &dyn tiberius::ToSql));
            let rows = tx
                .client()
                .query(sql.as_str(), &params)
                .await
                .map_err(|e| cov_err(&self.inner.name, "covered_among", e))?
                .into_first_result()
                .await
                .map_err(|e| cov_err(&self.inner.name, "covered_among", e))?;
            for row in rows {
                covered.insert(row.get::<&str, _>(0).unwrap_or_default().to_string());
            }
        }
        tx.commit(&self.inner.name).await?;
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
        let mut tx = self.begin(false).await?;
        let mut cleared = 0u64;
        for chunk in path_urns.chunks(coverage::IN_CHUNK) {
            let sql = coverage::clear_paths_sql(DIALECT, chunk.len());
            let mut params: Vec<&dyn tiberius::ToSql> = vec![&deployment_id];
            params.extend(chunk.iter().map(|u| u as &dyn tiberius::ToSql));
            cleared += tx
                .client()
                .execute(sql.as_str(), &params)
                .await
                .map_err(|e| cov_err(&self.inner.name, "clear", e))?
                .total();
        }
        tx.commit(&self.inner.name).await?;
        Ok(cleared)
    }

    /// The counts alone, as one aggregate.
    pub async fn count_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageCounts, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(false).await?;
        let counts = read_counts(&mut tx, &self.inner.name, deployment_id).await?;
        tx.commit(&self.inner.name).await?;
        Ok(counts)
    }

    /// Counts + the uncovered set, from ONE REPEATABLE READ snapshot.
    pub async fn read_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageMetrics, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(true).await?;
        let counts = read_counts(&mut tx, &self.inner.name, deployment_id).await?;
        let rows = tx
            .client()
            .query(coverage::uncovered_sql(DIALECT).as_str(), &[&deployment_id])
            .await
            .map_err(|e| cov_err(&self.inner.name, "uncovered", e))?
            .into_first_result()
            .await
            .map_err(|e| cov_err(&self.inner.name, "uncovered", e))?;
        tx.commit(&self.inner.name).await?;
        Ok(CoverageMetrics {
            total: counts.total,
            covered: counts.covered,
            uncovered: rows
                .iter()
                .map(|r| r.get::<&str, _>(0).unwrap_or_default().to_string())
                .collect(),
        })
    }

    /// Append a reconstruction fragment.
    pub async fn write_fragment(
        &self,
        deployment_id: &str,
        row: &CoverageFragmentRow,
    ) -> Result<(), DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(false).await?;
        let business_key = row.business_key.as_deref();
        let trace_id = row.trace_id.as_deref();
        tx.client()
            .execute(
                coverage::write_fragment_sql(DIALECT).as_str(),
                &[
                    &deployment_id,
                    &row.route_urn.as_str(),
                    &row.segment_process.as_str(),
                    &row.instance_id.as_str(),
                    &business_key,
                    &trace_id,
                ],
            )
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment insert", e))?;
        tx.commit(&self.inner.name).await?;
        Ok(())
    }

    /// All reconstruction fragments for the deployment, in insertion order.
    pub async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragmentRow>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(false).await?;
        let rows = tx
            .client()
            .query(
                coverage::read_fragments_sql(DIALECT).as_str(),
                &[&deployment_id],
            )
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment read", e))?
            .into_first_result()
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment read", e))?;
        tx.commit(&self.inner.name).await?;
        Ok(rows
            .iter()
            .map(|r| CoverageFragmentRow {
                route_urn: r
                    .get::<&str, _>("route_urn")
                    .unwrap_or_default()
                    .to_string(),
                segment_process: r
                    .get::<&str, _>("segment_process")
                    .unwrap_or_default()
                    .to_string(),
                instance_id: r
                    .get::<&str, _>("instance_id")
                    .unwrap_or_default()
                    .to_string(),
                business_key: r.get::<&str, _>("business_key").map(str::to_string),
                trace_id: r.get::<&str, _>("trace_id").map(str::to_string),
            })
            .collect())
    }

    /// Re-seed every declared path to uncovered and drop the deployment's fragments.
    pub async fn reset(&self, deployment_id: &str) -> Result<(), DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin(false).await?;
        tx.client()
            .execute(
                coverage::reset_metrics_sql(DIALECT).as_str(),
                &[&deployment_id],
            )
            .await
            .map_err(|e| cov_err(&self.inner.name, "reset", e))?;
        tx.client()
            .execute(
                coverage::delete_fragments_sql(DIALECT).as_str(),
                &[&deployment_id],
            )
            .await
            .map_err(|e| cov_err(&self.inner.name, "fragment reset", e))?;
        tx.commit(&self.inner.name).await?;
        Ok(())
    }
}

/// The `total` / `covered` aggregate. SQL Server's `COUNT` is `INT`, so the pair decodes as `i32`.
async fn read_counts(
    tx: &mut CoverageTx,
    store: &str,
    deployment_id: &str,
) -> Result<CoverageCounts, DataStoreError> {
    let rows = tx
        .client()
        .query(coverage::counts_sql(DIALECT).as_str(), &[&deployment_id])
        .await
        .map_err(|e| cov_err(store, "count", e))?
        .into_first_result()
        .await
        .map_err(|e| cov_err(store, "count", e))?;
    let row = rows.first().ok_or_else(|| {
        DataStoreError::new(format!("coverage store '{store}': count returned no row"))
    })?;
    Ok(CoverageCounts {
        total: row.get::<i32, _>(0).unwrap_or_default().max(0) as u64,
        covered: row.get::<i32, _>(1).unwrap_or_default().max(0) as u64,
    })
}

/// A coverage-store failure, named by store and operation.
fn cov_err(store: &str, op: &str, e: tiberius::error::Error) -> DataStoreError {
    DataStoreError::with_source(format!("coverage store '{store}': {op} failed"), e)
}

// ---- store-level bodies ----------------------------------------------------

async fn store_begin(inner: Arc<StoreInner>) -> Result<MssqlDataStoreTx, DataStoreError> {
    ensure_migrated(&inner).await?;
    let mut conn = inner.pool.acquire().await?;
    run_batch(conn.client(), "SET XACT_ABORT OFF; BEGIN TRANSACTION").await?;
    Ok(MssqlDataStoreTx {
        conn,
        released: false,
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

/// Run the module-resident idempotent migrations once per store instance, serialised across
/// replicas by an `sp_getapplock` session lock. tiberius batches (`simple_query`) run the
/// scripts; SQL Server DDL is transactional but the scripts here are idempotent
/// (`IF OBJECT_ID … CREATE`), so no ledger is kept — exactly as in the reference dialect.
///
/// (Also the FIRST-USE gate for a projected store's table verification — same once-per-instance
/// flag, so a projected store costs one extra catalog round-trip on first use and none after.)
async fn ensure_migrated(inner: &StoreInner) -> Result<(), DataStoreError> {
    let mut migrated = inner.migrated.lock().await;
    if *migrated {
        return Ok(());
    }
    if !inner.migrations.is_empty() {
        let mut conn = inner.pool.acquire().await?;
        let lock_key = format!("sutra_ds_{}", inner.name);
        run_batch(
            conn.client(),
            &format!(
                "EXEC sp_getapplock @Resource = N'{lock_key}', @LockMode = 'Exclusive', \
                 @LockOwner = 'Session', @LockTimeout = 30000"
            ),
        )
        .await?;
        let mut result = Ok(());
        for script in &inner.migrations {
            if let Err(e) = run_batch(conn.client(), script).await {
                result = Err(e);
                break;
            }
        }
        let _ = run_batch(
            conn.client(),
            &format!("EXEC sp_releaseapplock @Resource = N'{lock_key}', @LockOwner = 'Session'"),
        )
        .await;
        result?;
    }
    if let Some(projected) = &inner.projected {
        verify_projection(&inner.pool, projected).await?;
    }
    *migrated = true;
    Ok(())
}

/// First-use verification (design §4.5, RULED fail-closed) — see [`crate::postgres`]. SQL
/// Server's `INFORMATION_SCHEMA.COLUMNS` view is already database-local, so the probe needs no
/// schema predicate.
async fn verify_projection(
    pool: &MssqlPool,
    projected: &ProjectedStore,
) -> Result<(), DataStoreError> {
    let mut conn = pool.acquire().await?;
    let sql = projected.columns_probe_sql(DIALECT);
    let table = projected.table().to_string();
    let rows = conn
        .client()
        .query(sql.as_str(), &[&table])
        .await
        .map_err(|e| probe_err(projected, e))?
        .into_first_result()
        .await
        .map_err(|e| probe_err(projected, e))?;
    let actual: Vec<ActualColumn> = rows
        .iter()
        .map(|row| ActualColumn {
            name: row.get::<&str, _>(0).unwrap_or_default().to_string(),
            nullable: row
                .get::<&str, _>(1)
                .is_some_and(|v| v.eq_ignore_ascii_case("YES")),
            has_default: row.get::<&str, _>(2) == Some("Y"),
        })
        .collect();
    projected.verify(&actual)
}

fn probe_err(projected: &ProjectedStore, e: tiberius::error::Error) -> DataStoreError {
    DataStoreError::with_source(
        format!(
            "data store '{}' could not read the columns of table '{}' to verify its declared \
             structure",
            projected.store(),
            projected.table()
        ),
        e,
    )
}

fn no_connection(name: &str) -> DataStoreError {
    DataStoreError::new(format!(
        "sql data store '{name}' declares no connection. A store must own its connection \
         in datastores.yaml (sql.url + credentials, or *-ref secret-refs). The engine's \
         datasource is reserved for engine-internal tables and is never a module's store — \
         point the store at its own database."
    ))
}

// ---- transaction -----------------------------------------------------------

/// A caller-managed transaction over one store. One pooled connection for its whole life;
/// commit/rollback return it to the pool, drop-without-commit discards it (server rollback).
pub struct MssqlDataStoreTx {
    conn: PooledMssql,
    released: bool,
    store: String,
    legacy_namespace: Option<(String, String, String)>,
    /// `Some` for a store that declares a `structure`: the same operations run against the
    /// author's typed-column table instead of the generic blob table.
    projected: Option<Arc<ProjectedStore>>,
}

impl MssqlDataStoreTx {
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

    /// Commits and returns the connection to the pool.
    pub async fn commit(mut self) -> Result<(), DataStoreError> {
        run_batch(self.conn.client(), "COMMIT TRANSACTION")
            .await
            .map_err(|e| {
                DataStoreError::with_source(format!("commit failed for '{}'", self.store), e)
            })?;
        self.released = true;
        Ok(())
    }

    /// Rolls back and returns the connection to the pool.
    pub async fn rollback(mut self) -> Result<(), DataStoreError> {
        run_batch(self.conn.client(), "ROLLBACK TRANSACTION")
            .await
            .map_err(|e| {
                DataStoreError::with_source(format!("rollback failed for '{}'", self.store), e)
            })?;
        self.released = true;
        Ok(())
    }
}

impl Drop for MssqlDataStoreTx {
    fn drop(&mut self) {
        if !self.released {
            // discard the connection: the socket close aborts the open transaction
            self.conn.discard = true;
        }
    }
}

// ---- transaction-level bodies ----------------------------------------------

async fn tx_select_owned(
    tx: &mut MssqlDataStoreTx,
    key: String,
) -> Result<Option<Json>, DataStoreError> {
    tx_select(tx, &key).await
}

async fn tx_get_for_update_owned(
    tx: &mut MssqlDataStoreTx,
    key: String,
) -> Result<Option<Json>, DataStoreError> {
    let store = tx.store.clone();
    match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.lock_sql(DIALECT);
            tx.conn
                .client()
                .execute(sql.as_str(), &[&key])
                .await
                .map_err(|e| op_err(&store, &key, "lock-for-update", e))?;
        }
        None => {
            let sql = format!("UPDATE data_store SET rev = rev + 1{WHERE_KEY}");
            tx.conn
                .client()
                .execute(sql.as_str(), &[&store, &key])
                .await
                .map_err(|e| op_err(&store, &key, "lock-for-update", e))?;
        }
    }
    tx_select(tx, &key).await
}

async fn tx_put_owned(
    tx: &mut MssqlDataStoreTx,
    key: String,
    value: Json,
) -> Result<(), DataStoreError> {
    tx_put(tx, &key, &value).await
}

async fn tx_delete_owned(tx: &mut MssqlDataStoreTx, key: String) -> Result<(), DataStoreError> {
    tx_delete(tx, &key).await
}

async fn tx_revision_owned(tx: &mut MssqlDataStoreTx, key: String) -> Result<i64, DataStoreError> {
    tx_revision(tx, &key).await
}

async fn tx_put_if_revision_owned(
    tx: &mut MssqlDataStoreTx,
    key: String,
    value: Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    tx_put_if_revision(tx, &key, &value, expected_rev).await
}

async fn tx_select(tx: &mut MssqlDataStoreTx, key: &str) -> Result<Option<Json>, DataStoreError> {
    let store = tx.store.clone();
    if let Some(projected) = tx.projected.clone() {
        let sql = projected.select_sql(DIALECT);
        let row = tx
            .conn
            .client()
            .query(sql.as_str(), &[&key])
            .await
            .map_err(|e| op_err(&store, key, "get", e))?
            .into_row()
            .await
            .map_err(|e| op_err(&store, key, "get", e))?;
        return Ok(row.map(|r| {
            let cells: Vec<Option<String>> = (0..projected.projection().fields.len())
                .map(|i| r.get::<&str, _>(i).map(|s| s.to_string()))
                .collect();
            projected.row_to_json(&cells)
        }));
    }
    let sql = format!("SELECT store_value FROM data_store{WHERE_KEY}");
    let row = tx
        .conn
        .client()
        .query(sql.as_str(), &[&store, &key])
        .await
        .map_err(|e| op_err(&store, key, "get", e))?
        .into_row()
        .await
        .map_err(|e| op_err(&store, key, "get", e))?;
    match row {
        None => Ok(None),
        Some(r) => {
            let text: &str = r.get(0).ok_or_else(|| {
                DataStoreError::new(format!("stored value at '{store}'[{key}] is NULL"))
            })?;
            serde_json::from_str(text).map(Some).map_err(|e| {
                DataStoreError::with_source(
                    format!("stored value at '{store}'[{key}] is not valid JSON"),
                    e,
                )
            })
        }
    }
}

async fn tx_put(tx: &mut MssqlDataStoreTx, key: &str, value: &Json) -> Result<(), DataStoreError> {
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
                // its row count is load-bearing: zero means the row went away again.
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

async fn tx_delete(tx: &mut MssqlDataStoreTx, key: &str) -> Result<(), DataStoreError> {
    let store = tx.store.clone();
    match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.delete_sql(DIALECT);
            tx.conn
                .client()
                .execute(sql.as_str(), &[&key])
                .await
                .map_err(|e| op_err(&store, key, "delete", e))?;
        }
        None => {
            let sql = format!("DELETE FROM data_store{WHERE_KEY}");
            tx.conn
                .client()
                .execute(sql.as_str(), &[&store, &key])
                .await
                .map_err(|e| op_err(&store, key, "delete", e))?;
        }
    }
    Ok(())
}

async fn tx_revision(tx: &mut MssqlDataStoreTx, key: &str) -> Result<i64, DataStoreError> {
    let store = tx.store.clone();
    let row = match tx.projected.clone() {
        Some(projected) => {
            let sql = projected.revision_sql(DIALECT);
            tx.conn.client().query(sql.as_str(), &[&key]).await
        }
        None => {
            let sql = format!("SELECT rev FROM data_store{WHERE_KEY}");
            tx.conn.client().query(sql.as_str(), &[&store, &key]).await
        }
    }
    .map_err(|e| op_err(&store, key, "revision", e))?
    .into_row()
    .await
    .map_err(|e| op_err(&store, key, "revision", e))?;
    Ok(row.and_then(|r| r.get::<i64, _>(0)).unwrap_or(0))
}

/// The projected upsert's `UPDATE` half — binds `[fields…, key]`, in statement-text order (which
/// on this dialect IS the `@Pn` order).
async fn tx_update_projected(
    tx: &mut MssqlDataStoreTx,
    projected: &ProjectedStore,
    key: &str,
    cells: &[Option<String>],
) -> Result<u64, DataStoreError> {
    let store = tx.store.clone();
    let sql = projected.update_sql(DIALECT);
    let mut params: Vec<&dyn tiberius::ToSql> = cells
        .iter()
        .map(|cell| cell as &dyn tiberius::ToSql)
        .collect();
    params.push(&key);
    let affected = tx
        .conn
        .client()
        .execute(sql.as_str(), &params)
        .await
        .map_err(|e| op_err(&store, key, "put", e))?
        .total();
    Ok(affected)
}

/// The projected upsert's `INSERT` half — binds `[key, fields…]`, in statement-text order.
async fn tx_insert_projected(
    tx: &mut MssqlDataStoreTx,
    projected: &ProjectedStore,
    key: &str,
    cells: &[Option<String>],
) -> Result<(), tiberius::error::Error> {
    let sql = projected.insert_sql(DIALECT);
    let mut params: Vec<&dyn tiberius::ToSql> = vec![&key];
    params.extend(cells.iter().map(|cell| cell as &dyn tiberius::ToSql));
    tx.conn.client().execute(sql.as_str(), &params).await?;
    Ok(())
}

async fn tx_put_if_revision(
    tx: &mut MssqlDataStoreTx,
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
        let store = tx.store.clone();
        let sql = projected.update_if_revision_sql(DIALECT);
        let mut params: Vec<&dyn tiberius::ToSql> = cells
            .iter()
            .map(|cell| cell as &dyn tiberius::ToSql)
            .collect();
        params.push(&key);
        params.push(&expected_rev);
        let affected = tx
            .conn
            .client()
            .execute(sql.as_str(), &params)
            .await
            .map_err(|e| op_err(&store, key, "compare-and-set", e))?
            .total();
        return Ok(affected > 0);
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
    let store = tx.store.clone();
    let sql = format!(
        "UPDATE data_store SET store_value = @P3, rev = rev + 1, \
         updated_at = SYSUTCDATETIME(){WHERE_KEY} AND rev = @P4"
    );
    let affected = tx
        .conn
        .client()
        .execute(sql.as_str(), &[&store, &key, &json, &expected_rev])
        .await
        .map_err(|e| op_err(&store, key, "compare-and-set", e))?
        .total();
    Ok(affected > 0)
}

async fn tx_update(
    tx: &mut MssqlDataStoreTx,
    key: &str,
    json: &str,
) -> Result<u64, DataStoreError> {
    let store = tx.store.clone();
    let sql = format!(
        "UPDATE data_store SET store_value = @P3, rev = rev + 1, \
         updated_at = SYSUTCDATETIME(){WHERE_KEY}"
    );
    let affected = tx
        .conn
        .client()
        .execute(sql.as_str(), &[&store, &key, &json])
        .await
        .map_err(|e| op_err(&store, key, "put", e))?
        .total();
    Ok(affected)
}

/// Cutover insert (`(store_name, store_key, …)`), or the legacy 5-column stamp when a
/// pre-cutover namespace triple is configured.
async fn tx_insert(
    tx: &mut MssqlDataStoreTx,
    key: &str,
    json: &str,
) -> Result<(), tiberius::error::Error> {
    let store = tx.store.clone();
    match tx.legacy_namespace.clone() {
        Some((tenant, module, version)) => {
            tx.conn
                .client()
                .execute(
                    "INSERT INTO data_store (tenant_id, module_id, module_version, store_name, \
                     store_key, store_value, rev, updated_at) \
                     VALUES (@P1, @P2, @P3, @P4, @P5, @P6, 1, SYSUTCDATETIME())",
                    &[&tenant, &module, &version, &store, &key, &json],
                )
                .await?;
        }
        None => {
            tx.conn
                .client()
                .execute(
                    "INSERT INTO data_store (store_name, store_key, store_value, rev, updated_at) \
                     VALUES (@P1, @P2, @P3, 1, SYSUTCDATETIME())",
                    &[&store, &key, &json],
                )
                .await?;
        }
    }
    Ok(())
}

fn op_err(store: &str, key: &str, op: &str, e: tiberius::error::Error) -> DataStoreError {
    DataStoreError::with_source(format!("{op} failed for '{store}'[{key}]"), e)
}

/// SQL Server duplicate-key server errors: 2627 (PRIMARY KEY / UNIQUE constraint) and 2601
/// (duplicate key in a unique index). Deliberately narrow — a `NOT NULL` (515) or `CHECK`
/// (547) failure on a projected write is a real fault, not a lost race, and must not be sent
/// down the retry path (the sqlx dialects narrow the same way via `ErrorKind::UniqueViolation`).
fn is_duplicate_key(e: &tiberius::error::Error) -> bool {
    matches!(e.code(), Some(2627) | Some(2601))
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
    fn parses_a_sqlserver_url() {
        let c = MssqlConfig::parse(
            "sqlserver://db.host:1433;databaseName=sutra;trustServerCertificate=true",
            "accounts",
        )
        .unwrap();
        assert_eq!(c.host, "db.host");
        assert_eq!(c.port, 1433);
        assert_eq!(c.database.as_deref(), Some("sutra"));
        assert!(c.trust_cert);
    }

    #[test]
    fn defaults_port_and_rejects_foreign_scheme() {
        let c = MssqlConfig::parse("sqlserver://onlyhost;databaseName=d", "s").unwrap();
        assert_eq!(c.port, 1433);
        assert!(MssqlConfig::parse("postgres://h/db", "s").is_err());
    }

    #[test]
    fn missing_connection_fails_closed() {
        let def = StoreDefinition {
            name: "accounts".into(),
            store_type: "sql".into(),
            properties: std::collections::BTreeMap::new(),
            structure: None,
        };
        assert!(MssqlDataStore::from_definition(&def, Vec::new()).is_err());
    }
}
