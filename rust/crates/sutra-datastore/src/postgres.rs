//! The sqlx/PostgreSQL store — [`PostgresDataStore`] / [`PostgresDataStoreTx`] on the
//! `(store_name, store_key)` predicate. Every statement is pinned to the contract's SQL surface
//! (lock-via-UPDATE, portable upsert, the CAS shapes); only the namespace
//! columns are gone — within the store's declared connection, the two-column predicate IS
//! the isolation.
//!
//! API shape: the public operations return boxed, provably-`Send` futures ([`SendFuture`])
//! built from named `async fn`s over owned state (store level) or a single `&mut` borrow
//! (transaction level). This deliberately avoids `async` blocks/closures capturing borrows
//! across sqlx's generic executors, which trip rustc's auto-trait generalization
//! (rust-lang/rust#96865 / #100013) at `tokio::spawn` sites.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value as Json;
use sqlx::error::ErrorKind;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, Postgres, Row, Transaction};
use tokio::sync::Mutex;

use crate::config::StoreDefinition;
use crate::coverage::{
    self, check_deployment_id, CoverageCounts, CoverageFragmentRow, CoverageMetrics,
};
use crate::error::DataStoreError;
use crate::projected::{ActualColumn, ProjectedStore, SqlDialect};

/// This module's dialect, for the shared projected-store SQL builder.
const DIALECT: SqlDialect = SqlDialect::Postgres;

/// A boxed, provably-`Send` future — see the module docs for why the public API boxes.
pub type SendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const WHERE_KEY: &str = " WHERE store_name = $1 AND store_key = $2";

/// Namespace half of the PG advisory-lock key guarding first-use migrations (the second
/// half is `hashtext(store_name)`), so concurrent engine replicas serialise the idempotent
/// migration run instead of racing it.
const MIGRATION_LOCK_SPACE: i32 = 0x5D5A; // "SDS" — sutra data store

/// A `sql`-provider data store over the module's OWN connection (never the engine
/// datasource — each store owns its connection). Cheap to clone; all clones share one pool
/// and one run-once migration gate.
#[derive(Clone)]
pub struct PostgresDataStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    name: String,
    pool: PgPool,
    /// The store's resolved connect options — kept so the migration run can open its own
    /// dedicated connection (see [`run_migration_scripts`]).
    options: PgConnectOptions,
    migrations: Vec<String>,
    /// `true` once the module-resident migrations ran (the run-once-per-instance gate;
    /// the PG advisory lock serialises the run across replicas).
    migrated: Mutex<bool>,
    /// Pre-cutover compatibility: the `(tenant, module, version)` triple stamped into the
    /// LEGACY namespace columns on INSERT. The cutover shape drops those columns,
    /// but a module whose own migrations still create the 5-column pre-cutover table
    /// (`NOT NULL` namespace columns — e.g. the shipped examples) needs them supplied or
    /// every new-key INSERT fails. `None` = cutover table shape (the Rust-era default).
    legacy_namespace: Option<(String, String, String)>,
    /// The store's declared row structure, resolved at plan time. `Some` switches every
    /// operation onto the TYPED-COLUMN path — the author's own table, one column per declared
    /// field — and arms the first-use table verification. `None` is exactly today's opaque
    /// key→JSON behaviour, which is the compatibility guarantee.
    projected: Option<Arc<ProjectedStore>>,
}

impl PostgresDataStore {
    /// Build a store from its declaration: resolve the env-indirected connection
    /// (`sql.url[-ref]` + credentials — fails closed when no connection is declared) and
    /// hold the given idempotent migration scripts to run once on first use. The pool is
    /// lazy — nothing connects until the store is first used (a registered
    /// definition costs nothing).
    pub fn from_definition(
        def: &StoreDefinition,
        migrations: Vec<String>,
    ) -> Result<PostgresDataStore, DataStoreError> {
        Self::from_definition_with_namespace(def, migrations, None)
    }

    /// [`Self::from_definition`] with the pre-cutover namespace triple: INSERTs stamp the
    /// legacy `(tenant_id, module_id, module_version)` columns so modules whose migrations
    /// still create the 5-column table keep working until the cutover collapses
    /// them. Reads/updates are unaffected — `(store_name, store_key)` stays the predicate.
    pub fn from_definition_with_namespace(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
    ) -> Result<PostgresDataStore, DataStoreError> {
        Self::from_definition_projected(def, migrations, legacy_namespace, None)
    }

    /// [`Self::from_definition_with_namespace`] plus the store's resolved row PROJECTION.
    ///
    /// `Some` serves the store from the author's own typed-column table (design §4.5): every
    /// operation binds/reads one column per declared field instead of the `store_value` blob, and
    /// first use verifies the live table can satisfy the projection before anything is written.
    /// The projection is derived at PLAN time (it needs the module's compiled schemas, which this
    /// crate deliberately cannot reach), so a structure that cannot be projected has already
    /// failed the deploy by the time this is called.
    pub fn from_definition_projected(
        def: &StoreDefinition,
        migrations: Vec<String>,
        legacy_namespace: Option<(String, String, String)>,
        projected: Option<ProjectedStore>,
    ) -> Result<PostgresDataStore, DataStoreError> {
        Ok(PostgresDataStore {
            inner: Arc::new(store_inner(def, migrations, legacy_namespace, projected)?),
        })
    }

    /// The store's declared name (from `datastores.yaml`).
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

    /// The revision of the value at `key` — `0` when absent, bumped on every write.
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

    /// Opens a caller-managed transaction — the atomicity boundary the engine threads
    /// across the data tasks enclosed in a `<bpmn:transaction>` scope. Dropping the handle
    /// without `commit` rolls back (drop-without-commit is a rollback).
    pub fn begin(&self) -> SendFuture<'static, Result<PostgresDataStoreTx, DataStoreError>> {
        Box::pin(store_begin(Arc::clone(&self.inner)))
    }
}

/// Resolve a store declaration into its live connection state — the env-indirected URL +
/// credentials, the config-driven pool sizing, and the scripts its first use applies. Shared by
/// the KV store above and the [`PostgresCoverageStore`] below, whose only difference is WHOSE
/// scripts those are: the package's for a business store, the engine's for coverage.
fn store_inner(
    def: &StoreDefinition,
    migrations: Vec<String>,
    legacy_namespace: Option<(String, String, String)>,
    projected: Option<ProjectedStore>,
) -> Result<StoreInner, DataStoreError> {
    let url = def
        .resolved("sql.url")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            DataStoreError::new(format!(
                "sql data store '{}' declares no connection. A store must own its \
                 connection in datastores.yaml (sql.url + credentials, or *-ref \
                 secret-refs). The engine's datasource is reserved for engine-internal \
                 tables and is never a module's store — point the store at its own database.",
                def.name
            ))
        })?;
    let mut options = PgConnectOptions::from_str(&url).map_err(|e| {
        DataStoreError::with_source(
            format!(
                "sql data store '{}' has an invalid connection URL",
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
    let mut pool_options = PgPoolOptions::new().max_connections(pool_config.max_connections);
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

/// The coverage store on a PostgreSQL connection the AUTHOR declared (`datastore-schema-
/// projection.md` §7). Same connection machinery as the KV store above — same
/// `datastores.yaml` declaration, same lazy pool, same run-once first-use migration gate — with
/// ONE difference: the scripts it applies are the ENGINE's ([`crate::coverage::shipped_ddl`]),
/// because the coverage schema is an in-built feature and the author writes no coverage SQL.
///
/// Statements come from [`crate::coverage`] (dialect-parameterised, the `projected` module's
/// precedent); this type is the PostgreSQL driver half.
#[derive(Clone)]
pub struct PostgresCoverageStore {
    inner: Arc<StoreInner>,
}

impl PostgresCoverageStore {
    /// Build the coverage store over the declared `coverage` store's own connection. Nothing
    /// connects here (the pool is lazy) and nothing migrates until first use.
    pub fn from_definition(def: &StoreDefinition) -> Result<PostgresCoverageStore, DataStoreError> {
        Ok(PostgresCoverageStore {
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
    async fn begin(&self) -> Result<Transaction<'static, Postgres>, DataStoreError> {
        ensure_migrated(&self.inner).await?;
        self.inner
            .pool
            .begin()
            .await
            .map_err(|e| cov_err(&self.inner.name, "connect", e))
    }

    /// A read transaction at REPEATABLE READ — the portable replacement for the single-statement
    /// snapshot the PG-only aggregate used to get for free (see [`crate::coverage`]).
    async fn begin_snapshot(&self) -> Result<Transaction<'static, Postgres>, DataStoreError> {
        let mut tx = self.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await
            .map_err(|e| cov_err(&self.inner.name, "snapshot isolation", e))?;
        Ok(tx)
    }

    /// Idempotently seed every declared path as uncovered (the "total to cover"). Returns how many
    /// rows were newly inserted; an already-seeded or already-covered flag is left untouched.
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
            inserted += sqlx::query(&sql)
                .bind(deployment_id)
                .bind(urn)
                .execute(&mut *tx)
                .await
                .map_err(|e| cov_err(&self.inner.name, "seed", e))?
                .rows_affected();
        }
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "seed commit", e))?;
        Ok(inserted)
    }

    /// Flip a declared path to covered. `true` when THIS call newly covered it — the durable
    /// first-covers-wins signal, carried by the guarded flip's row count (and, for a path that was
    /// never seeded, by the insert's conflict branch). Never a read-then-write.
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
            sqlx::query(&coverage::mark_insert_sql(DIALECT))
                .bind(deployment_id)
                .bind(path_urn)
                .execute(&mut *tx)
                .await
                .map_err(|e| cov_err(&self.inner.name, "mark insert", e))?
                .rows_affected()
                > 0
        };
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "mark commit", e))?;
        Ok(newly)
    }

    /// The subset of `path_urns` currently covered — one round trip per chunk, in place of a `get`
    /// per declared path. A path with no row reads as uncovered.
    pub async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        if path_urns.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut tx = self.begin_snapshot().await?;
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
            covered.extend(rows.into_iter().map(|r| r.get::<String, _>(0)));
        }
        tx.commit()
            .await
            .map_err(|e| cov_err(&self.inner.name, "covered_among commit", e))?;
        Ok(covered)
    }

    /// Clear the covered flag on the named paths (rows stay seeded), returning how many were
    /// actually flipped `true → false` — the `coverage:reset` `cleared` count.
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

    /// The counts alone, as one aggregate — for callers that never list the uncovered set.
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

    /// Counts + the uncovered set. Both statements run in ONE REPEATABLE READ transaction, so the
    /// list can never disagree with the counts.
    pub async fn read_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageMetrics, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let mut tx = self.begin_snapshot().await?;
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
                .into_iter()
                .map(|r| r.get::<String, _>(0))
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
        Ok(rows
            .into_iter()
            .map(|r| CoverageFragmentRow {
                route_urn: r.get("route_urn"),
                segment_process: r.get("segment_process"),
                instance_id: r.get("instance_id"),
                business_key: r.get("business_key"),
                trace_id: r.get("trace_id"),
            })
            .collect())
    }

    /// Re-seed every declared path to uncovered and drop the deployment's fragments — atomic, one
    /// transaction.
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

/// A coverage-store failure, named by store and operation (the store name is the author's, so the
/// message points at the declaration that chose the database).
fn cov_err(store: &str, op: &str, e: sqlx::Error) -> DataStoreError {
    DataStoreError::with_source(format!("coverage store '{store}': {op} failed"), e)
}

// ---- store-level bodies (owned state — `'static`, freely spawnable) --------

async fn store_begin(inner: Arc<StoreInner>) -> Result<PostgresDataStoreTx, DataStoreError> {
    ensure_migrated(&inner).await?;
    let tx = inner.pool.begin().await.map_err(|e| {
        DataStoreError::with_source(
            format!("failed to open connection for store '{}'", inner.name),
            e,
        )
    })?;
    Ok(PostgresDataStoreTx {
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

/// Run the module-resident idempotent migrations once per store instance (and, via the PG
/// advisory lock, once at a time across concurrent replicas). Idempotent SQL means a re-run
/// on the next boot is safe — there is no ledger. Fail-closed: the flag
/// stays `false` on error, so a later use retries.
///
/// The scripts run through `sqlx::raw_sql` (simple-query protocol — multi-statement DDL,
/// dollar-quoted function bodies). Under rustc 1.96 a `raw_sql` future is not provably
/// `Send` in any formulation (the `Executor` HRTB generalization bug the module docs cite),
/// so the run happens on a dedicated connection inside `spawn_blocking` + a current-thread
/// runtime, where `Send` is not required. Migrations are a one-shot boot cost — blocking a
/// worker thread briefly is fine.
/// (Since the projection work this is also the FIRST-USE gate for a projected store's table
/// verification — the two run under the same once-per-instance flag, so a projected store costs
/// one extra catalog round-trip on first use and none thereafter.)
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

/// First-use verification (design §4.5, RULED fail-closed): read the live table's columns from
/// `information_schema` and refuse to serve the store if the projection is not satisfiable.
///
/// Lint proves the package-time case from the package's own migrations; this is the defence
/// against a DEPLOYED table that drifted from them — a hand-applied `ALTER`. It runs on the same
/// once-per-instance path as the migrations above (which the advisory lock already serialises
/// across replicas), never per operation.
async fn verify_projection(
    pool: &PgPool,
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

/// Blocking-thread body of the migration run: dedicated connection, advisory lock, each
/// script via the simple-query protocol, unlock, close.
fn run_migration_scripts(
    options: PgConnectOptions,
    name: &str,
    scripts: &[String],
) -> Result<(), DataStoreError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| DataStoreError::with_source("failed to build migration runtime", e))?;
    runtime.block_on(async move {
        let mut conn: PgConnection = options
            .connect()
            .await
            .map_err(|e| migration_err(name, e))?;
        sqlx::query("SELECT pg_advisory_lock($1, hashtext($2))")
            .bind(MIGRATION_LOCK_SPACE)
            .bind(name)
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
        // Release the advisory lock even when a script failed (session-scoped, but be
        // explicit before the close below).
        let _ = sqlx::query("SELECT pg_advisory_unlock($1, hashtext($2))")
            .bind(MIGRATION_LOCK_SPACE)
            .bind(name)
            .execute(&mut conn)
            .await;
        let _ = conn.close().await;
        result
    })
}

fn migration_err(store: &str, e: sqlx::Error) -> DataStoreError {
    DataStoreError::with_source(format!("migration failed for data store '{store}'"), e)
}

/// A caller-managed transaction over one store — [`PostgresDataStoreTx`]: one connection
/// for its whole life, READ COMMITTED (the PostgreSQL default), commit/rollback as the
/// atomicity boundary, drop-without-commit = rollback.
pub struct PostgresDataStoreTx {
    tx: Option<Transaction<'static, Postgres>>,
    store: String,
    legacy_namespace: Option<(String, String, String)>,
    /// `Some` for a store that declares a `structure`: the same operations run against the
    /// author's typed-column table instead of the generic blob table.
    projected: Option<Arc<ProjectedStore>>,
}

impl PostgresDataStoreTx {
    /// Read within the transaction; `None` when the key is absent.
    pub fn get<'a>(
        &'a mut self,
        key: &str,
    ) -> SendFuture<'a, Result<Option<Json>, DataStoreError>> {
        let key = key.to_string();
        Box::pin(tx_select_owned(self, key))
    }

    /// Pessimistic-locking read: takes the row lock with a rev-bumping `UPDATE` (the
    /// portable `SELECT … FOR UPDATE` substitute — a guaranteed non-no-op write, so the
    /// row-exclusive lock is held to commit). On an absent key it touches no row (no lock);
    /// the subsequent read returns `None` and [`Self::put`] will INSERT.
    pub fn get_for_update<'a>(
        &'a mut self,
        key: &str,
    ) -> SendFuture<'a, Result<Option<Json>, DataStoreError>> {
        let key = key.to_string();
        Box::pin(tx_get_for_update_owned(self, key))
    }

    /// Write (insert-or-replace) within the transaction — the portable upsert: `UPDATE`,
    /// and only if it touches no row `INSERT` (falling back to `UPDATE` on a lost insert
    /// race, detected by the ANSI integrity-violation SQLSTATE class 23).
    pub fn put<'a>(
        &'a mut self,
        key: &str,
        value: &Json,
    ) -> SendFuture<'a, Result<(), DataStoreError>> {
        let key = key.to_string();
        let value = value.clone();
        Box::pin(tx_put_owned(self, key, value))
    }

    /// Delete within the transaction; a no-op when the key is absent.
    pub fn delete<'a>(&'a mut self, key: &str) -> SendFuture<'a, Result<(), DataStoreError>> {
        let key = key.to_string();
        Box::pin(tx_delete_owned(self, key))
    }

    /// The revision at `key` within this transaction — `0` when absent.
    pub fn revision<'a>(&'a mut self, key: &str) -> SendFuture<'a, Result<i64, DataStoreError>> {
        let key = key.to_string();
        Box::pin(tx_revision_owned(self, key))
    }

    /// Compare-and-set (`<q:store expect="unchanged">`): `expected_rev <= 0` expects
    /// ABSENT — insert-only, a unique/PK violation means a concurrent writer created the
    /// key first (conflict, `false`); otherwise the conditional `UPDATE … AND rev = $n`
    /// touches 0 rows when a concurrent commit bumped `rev` (or the row is gone) — the
    /// detected conflict.
    pub fn put_if_revision<'a>(
        &'a mut self,
        key: &str,
        value: &Json,
        expected_rev: i64,
    ) -> SendFuture<'a, Result<bool, DataStoreError>> {
        let key = key.to_string();
        let value = value.clone();
        Box::pin(tx_put_if_revision_owned(self, key, value, expected_rev))
    }

    /// Commits and releases the transaction.
    pub async fn commit(mut self) -> Result<(), DataStoreError> {
        let store = self.store.clone();
        let tx = self.tx.take().ok_or_else(|| released(&store))?;
        tx.commit()
            .await
            .map_err(|e| DataStoreError::with_source(format!("commit failed for '{store}'"), e))
    }

    /// Rolls back and releases the transaction. (Dropping the handle without either call
    /// also rolls back — drop is a rollback.)
    pub async fn rollback(mut self) -> Result<(), DataStoreError> {
        let store = self.store.clone();
        let tx = self.tx.take().ok_or_else(|| released(&store))?;
        tx.rollback()
            .await
            .map_err(|e| DataStoreError::with_source(format!("rollback failed for '{store}'"), e))
    }

    fn conn(&mut self) -> Result<&mut Transaction<'static, Postgres>, DataStoreError> {
        let store = &self.store;
        self.tx.as_mut().ok_or_else(|| {
            DataStoreError::new(format!("transaction on '{store}' already released"))
        })
    }
}

// ---- transaction-level bodies (single `&mut` borrow + owned params) --------

async fn tx_select_owned(
    tx: &mut PostgresDataStoreTx,
    key: String,
) -> Result<Option<Json>, DataStoreError> {
    tx_select(tx, &key).await
}

async fn tx_get_for_update_owned(
    tx: &mut PostgresDataStoreTx,
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
    tx: &mut PostgresDataStoreTx,
    key: String,
    value: Json,
) -> Result<(), DataStoreError> {
    tx_put(tx, &key, &value).await
}

async fn tx_delete_owned(tx: &mut PostgresDataStoreTx, key: String) -> Result<(), DataStoreError> {
    tx_delete(tx, &key).await
}

async fn tx_revision_owned(
    tx: &mut PostgresDataStoreTx,
    key: String,
) -> Result<i64, DataStoreError> {
    tx_revision(tx, &key).await
}

async fn tx_put_if_revision_owned(
    tx: &mut PostgresDataStoreTx,
    key: String,
    value: Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    tx_put_if_revision(tx, &key, &value, expected_rev).await
}

async fn tx_select(
    tx: &mut PostgresDataStoreTx,
    key: &str,
) -> Result<Option<Json>, DataStoreError> {
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

async fn tx_put(
    tx: &mut PostgresDataStoreTx,
    key: &str,
    value: &Json,
) -> Result<(), DataStoreError> {
    // The projected path marshals FIRST, so an undeclared field is refused before any statement
    // runs — a rejected write touches nothing (design §4.2).
    if let Some(projected) = tx.projected.clone() {
        let cells = projected.bind_values(key, value)?;
        if tx_update_projected(tx, &projected, key, &cells).await? > 0 {
            return Ok(());
        }
        savepoint_stmt(tx, PUT_SAVEPOINT, "put", key).await?;
        return match tx_insert_projected(tx, &projected, key, &cells).await {
            Ok(()) => savepoint_stmt(tx, PUT_SAVEPOINT_RELEASE, "put", key).await,
            Err(e) => {
                undo_insert(tx, "put", key).await?;
                let store = tx.store.clone();
                if !is_duplicate_key(&e) {
                    return Err(op_err(&store, key, "put", e));
                }
                // Another writer inserted the key first — the retried UPDATE IS the write, so
                // its row count is load-bearing: zero means the row went away again.
                match tx_update_projected(tx, &projected, key, &cells).await? {
                    0 => Err(vanished_row_err(&store, key)),
                    _ => Ok(()),
                }
            }
        };
    }
    let json = value.to_string();
    if tx_update(tx, key, &json).await? > 0 {
        return Ok(()); // existing row replaced
    }
    savepoint_stmt(tx, PUT_SAVEPOINT, "put", key).await?;
    match tx_insert(tx, key, &json).await {
        Ok(()) => savepoint_stmt(tx, PUT_SAVEPOINT_RELEASE, "put", key).await,
        Err(e) => {
            undo_insert(tx, "put", key).await?;
            let store = tx.store.clone();
            if !is_duplicate_key(&e) {
                return Err(op_err(&store, key, "put", e));
            }
            // another writer inserted the key first — replace its value
            match tx_update(tx, key, &json).await? {
                0 => Err(vanished_row_err(&store, key)),
                _ => Ok(()),
            }
        }
    }
}

async fn tx_delete(tx: &mut PostgresDataStoreTx, key: &str) -> Result<(), DataStoreError> {
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

async fn tx_revision(tx: &mut PostgresDataStoreTx, key: &str) -> Result<i64, DataStoreError> {
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

async fn tx_put_if_revision(
    tx: &mut PostgresDataStoreTx,
    key: &str,
    value: &Json,
    expected_rev: i64,
) -> Result<bool, DataStoreError> {
    if let Some(projected) = tx.projected.clone() {
        let cells = projected.bind_values(key, value)?;
        if expected_rev <= 0 {
            // Expect-absent. The savepoint matters here too: `Ok(false)` means "conflict, no
            // write", and the caller may well carry on inside the same transaction — which it
            // could not do if the failed INSERT had aborted it.
            savepoint_stmt(tx, PUT_SAVEPOINT, "compare-and-set", key).await?;
            return match tx_insert_projected(tx, &projected, key, &cells).await {
                Ok(()) => {
                    savepoint_stmt(tx, PUT_SAVEPOINT_RELEASE, "compare-and-set", key).await?;
                    Ok(true)
                }
                Err(e) => {
                    undo_insert(tx, "compare-and-set", key).await?;
                    if is_duplicate_key(&e) {
                        return Ok(false);
                    }
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
        savepoint_stmt(tx, PUT_SAVEPOINT, "compare-and-set", key).await?;
        return match tx_insert(tx, key, &json).await {
            Ok(()) => {
                savepoint_stmt(tx, PUT_SAVEPOINT_RELEASE, "compare-and-set", key).await?;
                Ok(true)
            }
            Err(e) => {
                undo_insert(tx, "compare-and-set", key).await?;
                if is_duplicate_key(&e) {
                    return Ok(false);
                }
                let store = tx.store.clone();
                Err(op_err(&store, key, "compare-and-set", e))
            }
        };
    }
    let sql = format!(
        "UPDATE data_store SET store_value = $3, rev = rev + 1, \
         updated_at = CURRENT_TIMESTAMP{WHERE_KEY} AND rev = $4"
    );
    let store = tx.store.clone();
    let result = sqlx::query(&sql)
        .bind(&store)
        .bind(key)
        .bind(&json)
        .bind(expected_rev)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "compare-and-set", e))?;
    Ok(result.rows_affected() > 0)
}

/// The projected upsert's `UPDATE` half — binds `[fields…, key]`, in statement-text order.
async fn tx_update_projected(
    tx: &mut PostgresDataStoreTx,
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
    tx: &mut PostgresDataStoreTx,
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

async fn tx_update(
    tx: &mut PostgresDataStoreTx,
    key: &str,
    json: &str,
) -> Result<u64, DataStoreError> {
    let sql = format!(
        "UPDATE data_store SET store_value = $3, rev = rev + 1, \
         updated_at = CURRENT_TIMESTAMP{WHERE_KEY}"
    );
    let store = tx.store.clone();
    let result = sqlx::query(&sql)
        .bind(&store)
        .bind(key)
        .bind(json)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, "put", e))?;
    Ok(result.rows_affected())
}

/// Cutover insert: only `(store_name, store_key, store_value, rev, updated_at)` —
/// the collapsed-away namespace columns do not exist in the Rust-era table shape. With a
/// configured pre-cutover namespace triple, the INSERT stamps the legacy columns instead
/// (decided up-front — a failed statement would abort the enclosing PG transaction, so
/// there is deliberately no try-then-fallback here).
async fn tx_insert(tx: &mut PostgresDataStoreTx, key: &str, json: &str) -> Result<(), sqlx::Error> {
    let store = tx.store.clone();
    let legacy = tx.legacy_namespace.clone();
    let conn = tx.conn().map_err(|_| sqlx::Error::PoolClosed)?;
    match legacy {
        Some((tenant, module, version)) => {
            sqlx::query(
                "INSERT INTO data_store (tenant_id, module_id, module_version, store_name, \
                 store_key, store_value, rev, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, 1, CURRENT_TIMESTAMP)",
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
                 VALUES ($1, $2, $3, 1, CURRENT_TIMESTAMP)",
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
/// Deliberately **not** the whole SQLSTATE class `23`: that family also carries `NOT NULL`
/// (23502), `CHECK` (23514) and foreign-key (23503) violations. The KV table is the engine's
/// own fixed shape and never raises those, but a PROJECTED table is the author's, with the
/// author's constraints — and a `field=`-narrowed create of an absent key binds every other
/// declared column `NULL`, which is precisely a 23502. Matching the whole class sends those
/// down the duplicate-key retry path, where they surface as a *conflict* (or, on a dialect
/// that does not poison the transaction, as silent success). `sqlx`'s driver-mapped
/// `ErrorKind::UniqueViolation` is the portable narrow predicate; SQL Server narrows the same
/// way on 2627/2601 (`mssql.rs`).
fn is_duplicate_key(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if matches!(db.kind(), ErrorKind::UniqueViolation))
}

/// The savepoint every INSERT-with-a-fallback runs inside.
///
/// PostgreSQL aborts the WHOLE transaction on ANY failed statement, and every store write is
/// transactional — so without this the duplicate-key fallbacks below could never actually run:
/// the retried `UPDATE` would itself fail with `25P02` (*current transaction is aborted*), and
/// a legitimate lost race would surface as an error rather than as the write it is. Rolling
/// back to a savepoint undoes only the failed INSERT and leaves the transaction usable, which
/// is what makes a concurrent create converge here the way it already does on the other two
/// dialects. This is the ONE place the reference dialect needs machinery its siblings do not:
/// MySQL and SQL Server leave the transaction usable after a failed statement.
///
/// One name is reused throughout: every path releases or rolls back before it returns, and a
/// store operation is never re-entered mid-flight.
const PUT_SAVEPOINT: &str = "SAVEPOINT sutra_put";
const PUT_SAVEPOINT_RELEASE: &str = "RELEASE SAVEPOINT sutra_put";
const PUT_SAVEPOINT_UNDO: &str = "ROLLBACK TO SAVEPOINT sutra_put";

/// Execute one savepoint control statement (see [`PUT_SAVEPOINT`]).
async fn savepoint_stmt(
    tx: &mut PostgresDataStoreTx,
    stmt: &'static str,
    op: &str,
    key: &str,
) -> Result<(), DataStoreError> {
    let store = tx.store.clone();
    sqlx::query(stmt)
        .execute(&mut **tx.conn()?)
        .await
        .map_err(|e| op_err(&store, key, op, e))?;
    Ok(())
}

/// Undo a failed INSERT: roll back to the savepoint and release it, leaving the enclosing
/// transaction exactly as it was before the attempt.
async fn undo_insert(
    tx: &mut PostgresDataStoreTx,
    op: &str,
    key: &str,
) -> Result<(), DataStoreError> {
    savepoint_stmt(tx, PUT_SAVEPOINT_UNDO, op, key).await?;
    savepoint_stmt(tx, PUT_SAVEPOINT_RELEASE, op, key).await
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
