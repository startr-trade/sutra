//! Async store adapters — the ASSEMBLY glue between the executor's async
//! `DataStore`/`DataStoreTx`/`CoverageMetricStore` SPI and the async
//! `sutra_datastore::{Postgres,Mysql,Mssql}DataStore` (sqlx). The SPI is itself async
//! and `Result`-returning, so these adapters just `.await` the inner store and convert
//! `Json`↔`FeelValue` + `DataStoreError`→`StoreError`.
//!
//! There is no `Handle::block_on` bridge, no poison slot, and no `last_error()`. A backend
//! failure is a typed `Err(StoreError)` that the executor fails CLOSED on at the transactional
//! step boundaries — the same "never reply success on a lost write" guarantee that makes the
//! money-transfer atomicity fault-injection observable, now typed instead of out-of-band. The
//! adapter futures are `!Send` (`?Send` async-trait); they are driven on the channel-engine
//! actor thread via the runtime handle threaded through the dispatcher.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use async_trait::async_trait;
use sutra_datastore::{
    CoverageFragmentRow, CoverageStore, DataStoreError, MssqlDataStore, MssqlDataStoreTx,
    MysqlDataStore, MysqlDataStoreTx, PostgresDataStore, PostgresDataStoreTx,
};
use sutra_executor::variables::{feel_to_json, json_to_feel};
use sutra_executor::{
    CoverageFragment, CoverageMetricStore, CoverageMetrics, DataStore, DataStoreTx,
    StoreAggregateCell, StoreAggregateValue, StoreError, StoreKeyQuery, StoreQueryResult,
};
use sutra_feel::FeelValue;

/// Map a durable-store backend failure to the executor SPI's typed error (fail-closed).
fn to_store_err(e: DataStoreError) -> StoreError {
    StoreError::new(e.to_string())
}

/// The executor's key query in the dialect stores' terms.
fn to_range_query(query: &StoreKeyQuery) -> sutra_datastore::KeyRangeQuery {
    sutra_datastore::KeyRangeQuery {
        lower: query.lower.clone(),
        upper: query.upper.clone(),
        after: query.after.clone(),
        row_limit: query.row_limit,
        aggregates: query
            .aggregates
            .iter()
            .map(|a| sutra_datastore::KeyQueryAggregate {
                sum_field: a.sum_field.clone(),
                group_by: a.group_by.clone(),
            })
            .collect(),
    }
}

/// A dialect store's key-query page in the executor's terms (values as FEEL, sums as exact
/// decimals).
fn from_query_page(page: sutra_datastore::KeyQueryPage) -> Result<StoreQueryResult, StoreError> {
    fn cell(c: sutra_datastore::KeyAggregateCell) -> Result<StoreAggregateCell, StoreError> {
        let sum = match c.sum {
            None => None,
            Some(text) => Some(
                <bigdecimal::BigDecimal as std::str::FromStr>::from_str(&text).map_err(|e| {
                    StoreError::new(format!("key query sum '{text}' is not a decimal: {e}"))
                })?,
            ),
        };
        Ok(StoreAggregateCell {
            count: c.count,
            sum,
            non_numeric: c.non_numeric,
        })
    }
    let mut aggregates = Vec::with_capacity(page.aggregates.len());
    for a in page.aggregates {
        aggregates.push(match a {
            sutra_datastore::KeyAggregateResult::Total(c) => StoreAggregateValue::Total(cell(c)?),
            sutra_datastore::KeyAggregateResult::Grouped(groups) => {
                let mut out = std::collections::BTreeMap::new();
                for (k, c) in groups {
                    out.insert(k, cell(c)?);
                }
                StoreAggregateValue::Grouped(out)
            }
        });
    }
    Ok(StoreQueryResult {
        rows: page
            .rows
            .into_iter()
            .map(|(k, v)| (k, json_to_feel(&v)))
            .collect(),
        aggregates,
        truncated: page.truncated,
        next: page.next,
    })
}

/// Generates the async `DataStore`/`DataStoreTx` adapters over one async dialect
/// store whose surface matches [`PostgresDataStore`] one-for-one (every supported dialect —
/// PostgreSQL / MySQL-MariaDB / SQL Server — rides the SAME executor SPI). The adapter is a thin
/// `.await` + `Json`↔`FeelValue` + error-map shim — no runtime bridge, no poison slot.
macro_rules! async_dialect_store {
    ($store:ident, $store_ty:ty, $tx:ident, $tx_ty:ty) => {
        /// Async [`DataStore`] adapter over one dialect store (autocommit surface + `begin`).
        pub struct $store {
            inner: $store_ty,
        }

        impl $store {
            pub fn new(inner: $store_ty) -> $store {
                $store { inner }
            }
        }

        #[async_trait(?Send)]
        impl DataStore for $store {
            fn name(&self) -> &str {
                self.inner.name()
            }

            async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
                Ok(self
                    .inner
                    .get(key)
                    .await
                    .map_err(to_store_err)?
                    .map(|j| json_to_feel(&j)))
            }

            async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
                let json = feel_to_json(&value);
                self.inner.put(key, &json).await.map_err(to_store_err)
            }

            async fn delete(&self, key: &str) -> Result<(), StoreError> {
                self.inner.delete(key).await.map_err(to_store_err)
            }

            async fn revision(&self, key: &str) -> Result<i64, StoreError> {
                self.inner.revision(key).await.map_err(to_store_err)
            }

            async fn get_with_revision(
                &self,
                key: &str,
            ) -> Result<(Option<FeelValue>, i64), StoreError> {
                let (value, rev) = self
                    .inner
                    .get_with_revision(key)
                    .await
                    .map_err(to_store_err)?;
                Ok((value.map(|j| json_to_feel(&j)), rev))
            }

            async fn put_if_revision(
                &self,
                key: &str,
                value: FeelValue,
                expected_rev: i64,
            ) -> Result<bool, StoreError> {
                let json = feel_to_json(&value);
                self.inner
                    .put_if_revision(key, &json, expected_rev)
                    .await
                    .map_err(to_store_err)
            }

            async fn query(&self, query: &StoreKeyQuery) -> Result<StoreQueryResult, StoreError> {
                let page = self
                    .inner
                    .query(&to_range_query(query))
                    .await
                    .map_err(to_store_err)?;
                from_query_page(page)
            }

            async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError> {
                let tx = self.inner.begin().await.map_err(to_store_err)?;
                Ok(Some(Rc::new($tx {
                    store: self.inner.name().to_string(),
                    tx: RefCell::new(Some(tx)),
                })))
            }
        }

        /// Async [`DataStoreTx`] adapter over one open dialect transaction — the
        /// `<bpmn:transaction>` scope. Dropping without `commit` rolls back (sqlx Drop).
        pub struct $tx {
            store: String,
            tx: RefCell<Option<$tx_ty>>,
        }

        impl $tx {
            fn released(&self) -> StoreError {
                StoreError::new(format!(
                    "transaction on store '{}' already released",
                    self.store
                ))
            }
        }

        #[async_trait(?Send)]
        impl DataStoreTx for $tx {
            async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
                // Take the inner tx out for the await (its ops borrow `&mut self`), then
                // restore it — the RefCell is never borrowed across the await point.
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.get(key).await;
                *self.tx.borrow_mut() = Some(tx);
                Ok(r.map_err(to_store_err)?.map(|j| json_to_feel(&j)))
            }

            async fn get_for_update(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.get_for_update(key).await;
                *self.tx.borrow_mut() = Some(tx);
                Ok(r.map_err(to_store_err)?.map(|j| json_to_feel(&j)))
            }

            async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
                let json = feel_to_json(&value);
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.put(key, &json).await;
                *self.tx.borrow_mut() = Some(tx);
                r.map_err(to_store_err)
            }

            async fn delete(&self, key: &str) -> Result<(), StoreError> {
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.delete(key).await;
                *self.tx.borrow_mut() = Some(tx);
                r.map_err(to_store_err)
            }

            async fn revision(&self, key: &str) -> Result<i64, StoreError> {
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.revision(key).await;
                *self.tx.borrow_mut() = Some(tx);
                r.map_err(to_store_err)
            }

            async fn put_if_revision(
                &self,
                key: &str,
                value: FeelValue,
                expected_rev: i64,
            ) -> Result<bool, StoreError> {
                let json = feel_to_json(&value);
                let mut tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                let r = tx.put_if_revision(key, &json, expected_rev).await;
                *self.tx.borrow_mut() = Some(tx);
                r.map_err(to_store_err)
            }

            async fn commit(&self) -> Result<(), StoreError> {
                let tx = self.tx.borrow_mut().take().ok_or_else(|| self.released())?;
                tx.commit().await.map_err(to_store_err)
            }

            async fn rollback(&self) -> Result<(), StoreError> {
                // `let`-bind so the `RefMut` is dropped BEFORE the await (a `match` scrutinee
                // would extend it across the await point — clippy `await_holding_refcell_ref`).
                let taken = self.tx.borrow_mut().take();
                match taken {
                    Some(tx) => tx.rollback().await.map_err(to_store_err),
                    None => Ok(()), // rollback after a failed commit / double-release is a no-op
                }
            }
        }
    };
}

async_dialect_store!(PgStore, PostgresDataStore, PgTx, PostgresDataStoreTx);
async_dialect_store!(MysqlStore, MysqlDataStore, MysqlTx, MysqlDataStoreTx);
async_dialect_store!(MssqlStore, MssqlDataStore, MssqlTx, MssqlDataStoreTx);

/// Map a durable coverage-store failure to the executor SPI's typed error. The runtime marking
/// swallows it (coverage is a metric side-effect, never a reason to fail an instance); the
/// reserved `coverage:report` / `coverage:reset` ops surface it as a diagnostic.
fn to_metric_err(e: DataStoreError) -> StoreError {
    StoreError::new(e.to_string())
}

/// Adapter + ROUTER: the executor's TYPED [`CoverageMetricStore`] SPI over the per-deployment
/// coverage stores each deployment DECLARED.
///
/// Coverage is no longer an engine-database feature, so there is no single coverage connection to
/// hold: every deployment's marks go to the store named in ITS `datastores.yaml`, on whatever
/// database (and therefore dialect) that store's data source picks. The SPI is already
/// deployment-scoped — every method takes the deployment id — so routing is a lookup, and a
/// deployment with no usable coverage store gets a typed error naming exactly that rather than a
/// silent 0%.
pub struct DeclaredCoverageStores {
    /// deployment id → its declared coverage store.
    by_deployment: HashMap<String, CoverageStore>,
    /// deployment id → why it has none, when a declared store could not be built.
    faults: HashMap<String, String>,
}

impl DeclaredCoverageStores {
    /// An empty router — every deployment reports "no coverage store declared".
    pub fn new() -> DeclaredCoverageStores {
        DeclaredCoverageStores {
            by_deployment: HashMap::new(),
            faults: HashMap::new(),
        }
    }

    /// Register a deployment's coverage store.
    pub fn register(&mut self, deployment_id: impl Into<String>, store: CoverageStore) {
        self.by_deployment.insert(deployment_id.into(), store);
    }

    /// Record why a deployment that DECLARED a coverage store has none — surfaced verbatim by the
    /// reserved ops.
    pub fn register_fault(&mut self, deployment_id: impl Into<String>, fault: impl Into<String>) {
        self.faults.insert(deployment_id.into(), fault.into());
    }

    /// The store for `deployment_id`, or the typed error that says why there is none.
    fn resolve(&self, deployment_id: &str) -> Result<&CoverageStore, StoreError> {
        if let Some(store) = self.by_deployment.get(deployment_id) {
            return Ok(store);
        }
        match self.faults.get(deployment_id) {
            Some(fault) => Err(StoreError::new(format!(
                "deployment {deployment_id} declares a 'coverage' data store, but it could not be \
                 opened: {fault}"
            ))),
            None => Err(StoreError::new(format!(
                "deployment {deployment_id} declares no 'coverage' data store — that store is \
                 where coverage marks are persisted. Declare it in datastores.yaml (the engine \
                 owns its schema and applies it on first use; you supply no coverage SQL)."
            ))),
        }
    }
}

impl Default for DeclaredCoverageStores {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl CoverageMetricStore for DeclaredCoverageStores {
    async fn seed_declared(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<(), StoreError> {
        self.resolve(deployment_id)?
            .seed_declared(deployment_id, path_urns)
            .await
            .map(|_| ())
            .map_err(to_metric_err)
    }

    async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, StoreError> {
        self.resolve(deployment_id)?
            .mark_path_covered(deployment_id, path_urn)
            .await
            .map_err(to_metric_err)
    }

    async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<std::collections::BTreeSet<String>, StoreError> {
        self.resolve(deployment_id)?
            .covered_among(deployment_id, path_urns)
            .await
            .map_err(to_metric_err)
    }

    async fn clear_paths(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, StoreError> {
        self.resolve(deployment_id)?
            .clear_paths(deployment_id, path_urns)
            .await
            .map_err(to_metric_err)
    }

    async fn write_fragment(
        &self,
        deployment_id: &str,
        fragment: &CoverageFragment,
    ) -> Result<(), StoreError> {
        let row = CoverageFragmentRow {
            route_urn: fragment.route_urn.clone(),
            segment_process: fragment.segment_process.clone(),
            instance_id: fragment.instance_id.clone(),
            business_key: fragment.business_key.clone(),
            trace_id: fragment.trace_id.clone(),
        };
        self.resolve(deployment_id)?
            .write_fragment(deployment_id, &row)
            .await
            .map_err(to_metric_err)
    }

    async fn read_metrics(&self, deployment_id: &str) -> Result<CoverageMetrics, StoreError> {
        let m = self
            .resolve(deployment_id)?
            .read_metrics(deployment_id)
            .await
            .map_err(to_metric_err)?;
        Ok(CoverageMetrics {
            total: m.total,
            covered: m.covered,
            uncovered: m.uncovered,
        })
    }

    async fn reset(&self, deployment_id: &str) -> Result<(), StoreError> {
        self.resolve(deployment_id)?
            .reset(deployment_id)
            .await
            .map_err(to_metric_err)
    }

    async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragment>, StoreError> {
        let rows = self
            .resolve(deployment_id)?
            .read_fragments(deployment_id)
            .await
            .map_err(to_metric_err)?;
        Ok(rows
            .into_iter()
            .map(|r| CoverageFragment {
                route_urn: r.route_urn,
                segment_process: r.segment_process,
                instance_id: r.instance_id,
                business_key: r.business_key,
                trace_id: r.trace_id,
            })
            .collect())
    }
}

// ---- the opt-in read cache (`datastores.yaml` `cache:`, 0.6.5) ------------------------------

/// One store's cache, shared by every lane of the process (keyed by deployment + store name in
/// [`shared_cache`]), so a write through any lane invalidates the key for all of them.
pub struct StoreCache {
    config: sutra_datastore::CacheConfig,
    /// key → (revision, value as JSON, when it was last read or revalidated).
    entries: std::sync::Mutex<HashMap<String, CachedEntry>>,
}

#[derive(Clone)]
struct CachedEntry {
    rev: i64,
    value: Option<serde_json::Value>,
    checked: std::time::Instant,
}

impl StoreCache {
    fn new(config: sutra_datastore::CacheConfig) -> StoreCache {
        StoreCache {
            config,
            entries: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn lookup(&self, key: &str) -> Option<CachedEntry> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    fn store(&self, key: &str, rev: i64, value: Option<serde_json::Value>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= self.config.max_entries && !entries.contains_key(key) {
            // Evict the entry checked longest ago (O(n) over a small, bounded map).
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, e)| e.checked)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key.to_string(),
            CachedEntry {
                rev,
                value,
                checked: std::time::Instant::now(),
            },
        );
    }

    fn touch(&self, key: &str) {
        if let Some(entry) = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(key)
        {
            entry.checked = std::time::Instant::now();
        }
    }

    fn invalidate(&self, key: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }

    /// Whether an entry may be served without asking the database at all.
    fn fresh(&self, entry: &CachedEntry) -> bool {
        !self.config.ttl.is_zero() && entry.checked.elapsed() < self.config.ttl
    }

    /// How many keys are cached (tests and diagnostics).
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether nothing is cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The process-wide cache of one deployment's store — every lane's [`CachedStore`] for the
/// same `(deployment, store)` shares it, so an invalidation is seen by all of them.
pub fn shared_cache(
    deployment: &str,
    store: &str,
    config: sutra_datastore::CacheConfig,
) -> std::sync::Arc<StoreCache> {
    type Registry = std::sync::Mutex<HashMap<String, std::sync::Arc<StoreCache>>>;
    static CACHES: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    let mut caches = CACHES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let entry = caches
        .entry(format!("{deployment}/{store}"))
        .or_insert_with(|| std::sync::Arc::new(StoreCache::new(config)));
    if entry.config != config {
        *entry = std::sync::Arc::new(StoreCache::new(config));
    }
    std::sync::Arc::clone(entry)
}

/// A [`DataStore`] with the opt-in read cache in front of it.
///
/// * A read of a cached key asks the database for the key's revision only, and serves the
///   cached value when the revision is unchanged — one indexed lookup instead of a full read.
///   Every write the engine makes bumps the revision, so the next read on any replica sees it.
/// * With a positive `ttl` a key checked within the last `ttl` is served with no query at all.
/// * A write through this store — autocommit or inside a transaction — invalidates the key at
///   once, before and after the write.
pub struct CachedStore {
    inner: Rc<dyn DataStore>,
    cache: std::sync::Arc<StoreCache>,
}

impl CachedStore {
    pub fn new(inner: Rc<dyn DataStore>, cache: std::sync::Arc<StoreCache>) -> CachedStore {
        CachedStore { inner, cache }
    }
}

#[async_trait(?Send)]
impl DataStore for CachedStore {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        Ok(self.get_with_revision(key).await?.0)
    }

    async fn get_with_revision(&self, key: &str) -> Result<(Option<FeelValue>, i64), StoreError> {
        if let Some(entry) = self.cache.lookup(key) {
            if self.cache.fresh(&entry) {
                return Ok((entry.value.as_ref().map(json_to_feel), entry.rev));
            }
            let rev = self.inner.revision(key).await?;
            if rev == entry.rev && rev >= 0 {
                self.cache.touch(key);
                return Ok((entry.value.as_ref().map(json_to_feel), rev));
            }
        }
        let (value, rev) = self.inner.get_with_revision(key).await?;
        if rev >= 0 {
            self.cache.store(key, rev, value.as_ref().map(feel_to_json));
        }
        Ok((value, rev))
    }

    async fn revision(&self, key: &str) -> Result<i64, StoreError> {
        if let Some(entry) = self.cache.lookup(key) {
            if self.cache.fresh(&entry) {
                return Ok(entry.rev);
            }
        }
        self.inner.revision(key).await
    }

    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
        self.cache.invalidate(key);
        let result = self.inner.put(key, value).await;
        self.cache.invalidate(key);
        result
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.cache.invalidate(key);
        let result = self.inner.delete(key).await;
        self.cache.invalidate(key);
        result
    }

    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        expected_rev: i64,
    ) -> Result<bool, StoreError> {
        self.cache.invalidate(key);
        let result = self.inner.put_if_revision(key, value, expected_rev).await;
        self.cache.invalidate(key);
        result
    }

    /// A key query is never served from (or stored in) the cache: it reads the store itself.
    async fn query(&self, query: &StoreKeyQuery) -> Result<StoreQueryResult, StoreError> {
        self.inner.query(query).await
    }

    async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError> {
        Ok(self.inner.begin().await?.map(|tx| {
            Rc::new(CachedTx {
                inner: tx,
                cache: std::sync::Arc::clone(&self.cache),
                written: RefCell::new(Vec::new()),
            }) as Rc<dyn DataStoreTx>
        }))
    }
}

/// A transaction of a [`CachedStore`]: reads go to the database (a transaction must see its
/// own snapshot and locks), writes invalidate their keys when made and again when the
/// transaction ends.
struct CachedTx {
    inner: Rc<dyn DataStoreTx>,
    cache: std::sync::Arc<StoreCache>,
    written: RefCell<Vec<String>>,
}

impl CachedTx {
    fn wrote(&self, key: &str) {
        self.cache.invalidate(key);
        self.written.borrow_mut().push(key.to_string());
    }

    fn end(&self) {
        for key in self.written.borrow_mut().drain(..) {
            self.cache.invalidate(&key);
        }
    }
}

#[async_trait(?Send)]
impl DataStoreTx for CachedTx {
    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn get_for_update(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        self.inner.get_for_update(key).await
    }

    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
        self.wrote(key);
        self.inner.put(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.wrote(key);
        self.inner.delete(key).await
    }

    async fn revision(&self, key: &str) -> Result<i64, StoreError> {
        self.inner.revision(key).await
    }

    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        expected_rev: i64,
    ) -> Result<bool, StoreError> {
        self.wrote(key);
        self.inner.put_if_revision(key, value, expected_rev).await
    }

    async fn commit(&self) -> Result<(), StoreError> {
        let result = self.inner.commit().await;
        self.end();
        result
    }

    async fn rollback(&self) -> Result<(), StoreError> {
        let result = self.inner.rollback().await;
        self.end();
        result
    }
}

#[cfg(test)]
mod cache_tests {
    use std::cell::Cell;

    use super::*;
    use sutra_executor::InMemoryDataStore;

    /// Counts the reads that reach the store behind the cache.
    struct Counting {
        inner: InMemoryDataStore,
        full_reads: Cell<u32>,
        revision_reads: Cell<u32>,
    }

    #[async_trait(?Send)]
    impl DataStore for Counting {
        fn name(&self) -> &str {
            self.inner.name()
        }
        async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
            self.full_reads.set(self.full_reads.get() + 1);
            self.inner.get(key).await
        }
        async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
            self.inner.put(key, value).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        async fn revision(&self, key: &str) -> Result<i64, StoreError> {
            self.revision_reads.set(self.revision_reads.get() + 1);
            self.inner.revision(key).await
        }
        async fn put_if_revision(
            &self,
            key: &str,
            value: FeelValue,
            expected_rev: i64,
        ) -> Result<bool, StoreError> {
            self.inner.put_if_revision(key, value, expected_rev).await
        }
        async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError> {
            self.inner.begin().await
        }
    }

    fn cached(
        name: &str,
        ttl: std::time::Duration,
    ) -> (Rc<Counting>, CachedStore, InMemoryDataStore) {
        let backing = InMemoryDataStore::new("ref");
        let counting = Rc::new(Counting {
            inner: backing.clone(),
            full_reads: Cell::new(0),
            revision_reads: Cell::new(0),
        });
        let config = sutra_datastore::CacheConfig {
            enabled: true,
            max_entries: 2,
            ttl,
        };
        let store = CachedStore::new(
            Rc::clone(&counting) as Rc<dyn DataStore>,
            shared_cache("dep-cache-test", name, config),
        );
        (counting, store, backing)
    }

    fn s(v: &str) -> FeelValue {
        FeelValue::String(v.to_string())
    }

    /// Default ttl 0: a cached key costs one revision check, and a write made behind the
    /// cache's back that bumps the revision is seen on the very next read.
    #[tokio::test]
    async fn a_cached_key_is_revalidated_by_revision_and_sees_an_outside_write() {
        let (counting, store, backing) = cached("revalidate", std::time::Duration::ZERO);
        backing.put("k", s("v1")).await.unwrap();
        assert_eq!(store.get_with_revision("k").await.unwrap().0, Some(s("v1")));
        let full = counting.full_reads.get();
        assert_eq!(store.get_with_revision("k").await.unwrap().0, Some(s("v1")));
        assert_eq!(counting.full_reads.get(), full, "served from the cache");
        // Another writer (another replica, or SQL that bumps rev): the next read sees it.
        backing.put("k", s("v2")).await.unwrap();
        assert_eq!(store.get_with_revision("k").await.unwrap().0, Some(s("v2")));
    }

    /// A write through the engine invalidates at once, even with a long ttl — autocommit and
    /// inside a transaction.
    #[tokio::test]
    async fn a_write_through_the_engine_applies_to_the_next_read_even_with_a_ttl() {
        let (_, store, _) = cached("invalidate", std::time::Duration::from_secs(3600));
        store.put("k", s("v1")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(s("v1")));
        store.put("k", s("v2")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(s("v2")));
        let tx = store.begin().await.unwrap().expect("transactions");
        tx.put("k", s("v3")).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(s("v3")));
    }

    /// A positive ttl serves a fresh key with no query at all; an outside write shows once the
    /// ttl has passed (the documented trade).
    #[tokio::test]
    async fn a_ttl_serves_without_a_query_until_it_passes() {
        let (counting, store, backing) = cached("ttl", std::time::Duration::from_millis(50));
        backing.put("k", s("v1")).await.unwrap();
        store.get("k").await.unwrap();
        let (full, revs) = (counting.full_reads.get(), counting.revision_reads.get());
        backing.put("k", s("outside")).await.unwrap();
        assert_eq!(
            store.get("k").await.unwrap(),
            Some(s("v1")),
            "within the ttl"
        );
        assert_eq!(
            (counting.full_reads.get(), counting.revision_reads.get()),
            (full, revs),
            "no query within the ttl"
        );
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(store.get("k").await.unwrap(), Some(s("outside")));
    }

    /// The cache never holds more than maxEntries keys.
    #[tokio::test]
    async fn the_cache_is_bounded_by_max_entries() {
        let (_, store, backing) = cached("bounded", std::time::Duration::ZERO);
        for key in ["a", "b", "c", "d"] {
            backing.put(key, s(key)).await.unwrap();
            store.get(key).await.unwrap();
        }
        assert!(store.cache.len() <= 2);
    }
}
