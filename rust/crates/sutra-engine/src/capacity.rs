//! Capacity pools — the production backing for [`sutra_channels::CapacityPoolStore`] and the
//! process-wide set of declared pools.
//!
//! The pools are engine configuration (`sutra.capacity-pools.<name>.*`), installed once at boot
//! with [`install_configured_pools`] so the deployment planner can refuse a channel that names an
//! undeclared pool (or one its tenant may not use) without threading the configuration through
//! every archive-preparation path.
//!
//! [`PersistedCapacityPools`] wraps [`PgCapacitySlotStore`] (the V703 `capacity_pool` /
//! `capacity_slot` tables): acquisition counts under a row lock on the pool's own row, so the
//! count holds across replicas and survives a crash. A persistence-less engine falls back to
//! [`sutra_channels::InMemoryCapacityPools`] (per process only), with a WARN.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use sqlx::PgPool;
use tracing::warn;
use uuid::Uuid;

use sutra_channels::{
    CallSelector, CapacityPool, CapacityPoolStore, ChannelDefinition, Diagnostic,
    InMemoryCapacityPools, SlotAcquisition, SlotDirection, SlotRequest,
};
use sutra_executor::DeploymentId;
use sutra_persistence::stores::{PgCapacitySlotStore, SlotClaim, SlotGrant, SlotSelection};
use sutra_persistence::DeploymentId as PersistDeploymentId;

fn configured() -> &'static RwLock<Arc<BTreeMap<String, CapacityPool>>> {
    static POOLS: std::sync::OnceLock<RwLock<Arc<BTreeMap<String, CapacityPool>>>> =
        std::sync::OnceLock::new();
    POOLS.get_or_init(|| RwLock::new(Arc::new(BTreeMap::new())))
}

/// Install the engine's declared pools (boot, from [`crate::EngineConfig::capacity_pools`]).
/// Replaces any earlier set.
pub fn install_configured_pools(pools: &[CapacityPool]) {
    let map = pools.iter().map(|p| (p.name.clone(), p.clone())).collect();
    *configured().write().unwrap_or_else(|e| e.into_inner()) = Arc::new(map);
}

/// The declared pools.
pub fn configured_pools() -> Arc<BTreeMap<String, CapacityPool>> {
    Arc::clone(&configured().read().unwrap_or_else(|e| e.into_inner()))
}

/// The load-time check of a deployment's `pool:` references: every named pool is declared,
/// and the channel's tenant is on the pool's allow-list.
pub fn validate_pool_references(
    definitions: &[ChannelDefinition],
    deployment: &DeploymentId,
) -> Result<(), String> {
    let pools = configured_pools();
    for def in definitions {
        let Some(name) = def.binding.pool.as_deref() else {
            continue;
        };
        let channel = &def.binding.channel_name;
        let Some(pool) = pools.get(name) else {
            return Err(format!(
                "[{}] channel '{channel}' of deployment {} names capacity pool '{name}', which \
                 the engine configuration does not declare (declared: {:?})",
                sutra_channels::codes::CONFIG_CHANNEL_POOL_UNDECLARED,
                deployment.value(),
                pools.keys().collect::<Vec<_>>()
            ));
        };
        let tenant = def.binding.tenant();
        if !pool.allows_tenant(tenant) {
            return Err(format!(
                "[{}] channel '{channel}' of deployment {} (tenant '{tenant}') names capacity \
                 pool '{name}', whose tenants allow-list does not include '{tenant}'",
                sutra_channels::codes::CONFIG_CHANNEL_POOL_TENANT_NOT_ALLOWED,
                deployment.value()
            ));
        }
    }
    Ok(())
}

type SharedStore = RwLock<Option<Arc<dyn CapacityPoolStore>>>;

fn shared() -> &'static SharedStore {
    static STORE: std::sync::OnceLock<SharedStore> = std::sync::OnceLock::new();
    STORE.get_or_init(|| RwLock::new(None))
}

/// Install the store the runtime's lanes share (built once per runtime by
/// [`build_capacity_store`]); every lane built afterwards — including an activation flip's
/// rebuild — draws from it, so one count governs them all.
pub fn install_shared_store(store: Option<Arc<dyn CapacityPoolStore>>) {
    *shared().write().unwrap_or_else(|e| e.into_inner()) = store;
}

/// The installed shared store, if any pool is declared.
pub fn shared_store() -> Option<Arc<dyn CapacityPoolStore>> {
    shared().read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// The store every lane and the outbox dispatcher share — `None` when no pool is declared.
pub fn build_capacity_store(pool: Option<PgPool>) -> Option<Arc<dyn CapacityPoolStore>> {
    let pools = configured_pools();
    if pools.is_empty() {
        return None;
    }
    let declared: Vec<CapacityPool> = pools.values().cloned().collect();
    Some(match pool {
        Some(pool) => Arc::new(PersistedCapacityPools::new(
            PgCapacitySlotStore::new(pool),
            declared,
            sutra_channels::replica_id().to_string(),
        )),
        None => {
            warn!(
                pools = declared.len(),
                "no engine datasource — capacity pools are counted per process only (not \
                 shared across replicas, not crash-safe)"
            );
            Arc::new(InMemoryCapacityPools::new(declared))
        }
    })
}

/// [`CapacityPoolStore`] backed by the persisted `capacity_pool` / `capacity_slot` tables.
pub struct PersistedCapacityPools {
    store: PgCapacitySlotStore,
    pools: BTreeMap<String, CapacityPool>,
    /// This engine replica's id, recorded on every slot it takes (the sweep's lease).
    holder: String,
}

impl PersistedCapacityPools {
    pub fn new(
        store: PgCapacitySlotStore,
        pools: Vec<CapacityPool>,
        holder: String,
    ) -> PersistedCapacityPools {
        PersistedCapacityPools {
            store,
            pools: pools.into_iter().map(|p| (p.name.clone(), p)).collect(),
            holder,
        }
    }
}

fn persist_dep(deployment: &DeploymentId) -> Result<PersistDeploymentId, Diagnostic> {
    PersistDeploymentId::new(deployment.value()).map_err(|e| {
        Diagnostic::error(
            sutra_channels::codes::RUNTIME_UNEXPECTED,
            format!("capacity pool: deployment id rejected: {e}"),
        )
    })
}

fn holder_uuid(holder: &str) -> Option<Uuid> {
    Uuid::parse_str(holder)
        .map_err(|e| warn!(holder, error = %e, "capacity pool: holder is not a UUID"))
        .ok()
}

#[async_trait::async_trait]
impl CapacityPoolStore for PersistedCapacityPools {
    fn pool(&self, name: &str) -> Option<CapacityPool> {
        self.pools.get(name).cloned()
    }

    async fn acquire(&self, request: &SlotRequest<'_>) -> Result<SlotAcquisition, Diagnostic> {
        let Some(pool) = self.pools.get(request.pool).cloned() else {
            return Ok(SlotAcquisition::UnknownPool);
        };
        let dep = persist_dep(request.deployment)?;
        let instance = Uuid::parse_str(request.holder).map_err(|e| {
            Diagnostic::error(
                sutra_channels::codes::RUNTIME_UNEXPECTED,
                format!(
                    "capacity pool: holder '{}' is not a UUID: {e}",
                    request.holder
                ),
            )
        })?;
        let direction = request.direction;
        let admits = move |inbound: i32, outbound: i32| {
            pool.admits(direction, inbound.max(0) as u32, outbound.max(0) as u32)
        };
        let claim = SlotClaim {
            deployment: &dep,
            pool: request.pool,
            tenant: request.tenant,
            instance,
            call_key: request.call,
            direction: direction.as_str(),
            holder: &self.holder,
            require_live: request.require_live,
        };
        let grant = self.store.acquire(&claim, &admits).await.map_err(|e| {
            Diagnostic::error(
                sutra_channels::codes::RUNTIME_UNEXPECTED,
                format!("capacity pool '{}' acquisition failed: {e}", request.pool),
            )
        })?;
        Ok(match grant {
            SlotGrant::Acquired => SlotAcquisition::Acquired,
            SlotGrant::AlreadyHeld => SlotAcquisition::AlreadyHeld,
            SlotGrant::Exhausted {
                inbound_used,
                outbound_used,
            } => SlotAcquisition::Exhausted {
                inbound_used: inbound_used.max(0) as u32,
                outbound_used: outbound_used.max(0) as u32,
            },
            SlotGrant::InstanceEnded => SlotAcquisition::InstanceEnded,
            SlotGrant::SessionEnded => SlotAcquisition::SessionEnded,
        })
    }

    async fn end_session(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: Option<&str>,
    ) -> Vec<SlotDirection> {
        let (Ok(dep), Some(instance)) = (persist_dep(deployment), holder_uuid(holder)) else {
            return Vec::new();
        };
        match self.store.end_session(&dep, instance, call).await {
            Ok(released) => released
                .iter()
                .filter_map(|d| SlotDirection::parse(d))
                .collect(),
            Err(e) => {
                // The slots go back at the latest when the instance ends; until the session end
                // is recorded, a queued request of it may still take a slot.
                warn!(holder, error = %e, "capacity pool session end failed");
                Vec::new()
            }
        }
    }

    async fn session_ended(&self, deployment: &DeploymentId, holder: &str, call: &str) -> bool {
        let (Ok(dep), Some(instance)) = (persist_dep(deployment), holder_uuid(holder)) else {
            return false;
        };
        self.store
            .session_ended(&dep, instance, call)
            .await
            .unwrap_or_else(|e| {
                warn!(holder, error = %e, "capacity pool session-ended read failed");
                false
            })
    }

    async fn rebind(&self, deployment: &DeploymentId, from: &str, to: &str) {
        let (Ok(dep), Some(from_id), Some(to_id)) =
            (persist_dep(deployment), holder_uuid(from), holder_uuid(to))
        else {
            return;
        };
        if let Err(e) = self.store.rebind(&dep, from_id, to_id).await {
            // The slot stays under its admission key: the sweep returns it once the bound
            // passes (the key names no stored instance), so the pool cannot leak it for good.
            warn!(from, to, error = %e, "capacity pool rebind failed");
        }
    }

    async fn release(&self, deployment: &DeploymentId, holder: &str) -> u32 {
        let (Ok(dep), Some(instance)) = (persist_dep(deployment), holder_uuid(holder)) else {
            return 0;
        };
        match self.store.release(&dep, instance).await {
            Ok(released) => released as u32,
            Err(e) => {
                // The sweep returns the slot once the instance is seen finished.
                warn!(holder, error = %e, "capacity pool release failed");
                0
            }
        }
    }

    async fn release_calls(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        selector: CallSelector<'_>,
    ) -> Vec<SlotDirection> {
        let (Ok(dep), Some(instance)) = (persist_dep(deployment), holder_uuid(holder)) else {
            return Vec::new();
        };
        let selection = match selector {
            CallSelector::Call(call) => SlotSelection::Call(call),
            CallSelector::Direction(direction) => SlotSelection::Direction(direction.as_str()),
        };
        match self.store.release_calls(&dep, instance, selection).await {
            Ok(released) => released
                .iter()
                .filter_map(|d| SlotDirection::parse(d))
                .collect(),
            Err(e) => {
                // The call's slot is returned at the latest when the instance ends.
                warn!(holder, error = %e, "capacity pool call release failed");
                Vec::new()
            }
        }
    }

    async fn release_call_if_ended(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: &str,
    ) -> u32 {
        let (Ok(dep), Some(instance)) = (persist_dep(deployment), holder_uuid(holder)) else {
            return 0;
        };
        match self.store.release_call_if_over(&dep, instance, call).await {
            Ok(released) => released as u32,
            Err(e) => {
                // The sweep returns the slot of a finished instance on its next tick.
                warn!(holder, error = %e, "capacity pool release of a delivered call failed");
                0
            }
        }
    }

    async fn usage(&self, pool: &str) -> (u32, u32) {
        match self.store.usage(pool).await {
            Ok((inbound, outbound)) => (inbound.max(0) as u32, outbound.max(0) as u32),
            Err(e) => {
                warn!(pool, error = %e, "capacity pool usage read failed");
                (0, 0)
            }
        }
    }
}
