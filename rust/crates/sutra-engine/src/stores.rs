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
    CoverageFragment, CoverageMetricStore, CoverageMetrics, DataStore, DataStoreTx, StoreError,
};
use sutra_feel::FeelValue;

/// Map a durable-store backend failure to the executor SPI's typed error (fail-closed).
fn to_store_err(e: DataStoreError) -> StoreError {
    StoreError::new(e.to_string())
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
