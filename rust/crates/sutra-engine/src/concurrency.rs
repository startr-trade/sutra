//! Persisted per-channel concurrency gauge — the production backing for the dispatcher's
//! [`sutra_channels::ConcurrencyStore`] seam.
//!
//! Wraps [`sutra_persistence::stores::PgChannelConcurrencyStore`] (the `channel_instance`
//! table, V701/V702 — the durable channel-concurrency store). The seam is async end to end
//! (execution scale-out §3(a), Phase 3): the dispatcher awaits these calls on its lane's
//! single actor task, exactly like [`crate::bridge::PersistenceBridge`] — no captured
//! runtime handle, no `block_on`.
//!
//! Why persisted, not in-memory: the count is the replica-coherent source of truth for the
//! per-channel cap. A parked instance leaves a `WAITING` row that every replica's `COUNT(*)`
//! sees, and that survives a pod crash (an in-memory counter would reset and, across N
//! replicas, multiply a cap of N into N×replicas). A persistence-less boot falls back to the
//! in-memory store with a WARN (per-process only) — wait states already fail closed there, so
//! nothing durably parks anyway.

use tracing::warn;
use uuid::Uuid;

use sutra_channels::{ActiveInstanceCount, ConcurrencyStore};
use sutra_executor::DeploymentId;
use sutra_persistence::stores::{
    ChannelConcurrencyStore, InstanceStore, PgChannelConcurrencyStore, PgInstanceStore,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;

fn persist_dep(deployment: &DeploymentId) -> Option<PersistDeploymentId> {
    PersistDeploymentId::new(deployment.value())
        .map_err(|e| warn!(deployment = deployment.value(), error = %e, "channel-concurrency: deployment id rejected persistence-form validation"))
        .ok()
}

/// [`ConcurrencyStore`] backed by the persisted `channel_instance` table.
pub struct PersistedChannelConcurrency {
    store: PgChannelConcurrencyStore,
}

impl PersistedChannelConcurrency {
    pub fn new(store: PgChannelConcurrencyStore) -> PersistedChannelConcurrency {
        PersistedChannelConcurrency { store }
    }

    fn instance(instance_id: &str) -> Option<Uuid> {
        Uuid::parse_str(instance_id)
            .map_err(|e| warn!(instance_id, error = %e, "channel-concurrency: instance id is not a UUID"))
            .ok()
    }
}

#[async_trait::async_trait(?Send)]
impl ConcurrencyStore for PersistedChannelConcurrency {
    async fn count_active(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        include_waiting: bool,
    ) -> u64 {
        let Some(dep) = persist_dep(deployment) else {
            return 0;
        };
        match self
            .store
            .count_active_by_channel(&dep, channel, include_waiting)
            .await
        {
            Ok(count) => count.max(0) as u64,
            Err(e) => {
                // Fail OPEN on a count error (admit), like the inbox-dedup hook — a transient
                // DB blip must not reject legitimate traffic; the cap re-asserts on recovery.
                warn!(channel, error = %e, "channel-concurrency count failed — admitting (fail-open)");
                0
            }
        }
    }

    async fn record_started(&self, deployment: &DeploymentId, instance_id: &str, channel: &str) {
        let (Some(dep), Some(instance)) = (persist_dep(deployment), Self::instance(instance_id))
        else {
            return;
        };
        if let Err(e) = self.store.record_started(&dep, instance, channel).await {
            warn!(instance_id, channel, error = %e, "channel-concurrency recordStarted failed");
        }
    }

    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: &str) {
        let (Some(dep), Some(instance)) = (persist_dep(deployment), Self::instance(instance_id))
        else {
            return;
        };
        if let Err(e) = self.store.record_suspended(&dep, instance).await {
            warn!(instance_id, error = %e, "channel-concurrency recordSuspended failed");
        }
    }

    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: &str) {
        let (Some(dep), Some(instance)) = (persist_dep(deployment), Self::instance(instance_id))
        else {
            return;
        };
        if let Err(e) = self.store.record_resumed(&dep, instance).await {
            warn!(instance_id, error = %e, "channel-concurrency recordResumed failed");
        }
    }

    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: &str) {
        let (Some(dep), Some(instance)) = (persist_dep(deployment), Self::instance(instance_id))
        else {
            return;
        };
        if let Err(e) = self.store.record_terminal(&dep, instance).await {
            warn!(instance_id, error = %e, "channel-concurrency recordTerminal failed");
        }
    }
}

/// [`ActiveInstanceCount`] backed by the persisted `instance_state` table — the
/// replica-coherent per-deployment live-instance count the tenant-quota enforcer's
/// concurrent dimension reads (the active-instance count:
/// `SELECT COUNT(*) FROM instance_state WHERE deployment_id = ?`). Same rationale as
/// [`PersistedChannelConcurrency`]: an in-memory counter resets on crash and multiplies a
/// cluster-wide quota by the replica count.
pub struct PersistedActiveInstanceCount {
    store: PgInstanceStore,
}

impl PersistedActiveInstanceCount {
    pub fn new(store: PgInstanceStore) -> PersistedActiveInstanceCount {
        PersistedActiveInstanceCount { store }
    }
}

#[async_trait::async_trait(?Send)]
impl ActiveInstanceCount for PersistedActiveInstanceCount {
    async fn count_active(&self, deployment: &DeploymentId) -> u64 {
        let Some(dep) = persist_dep(deployment) else {
            return 0;
        };
        match self.store.count_active(&dep).await {
            Ok(count) => count.max(0) as u64,
            Err(e) => {
                // Fail OPEN on a count error (admit), matching the concurrency gauge.
                warn!(deployment = deployment.value(), error = %e, "tenant-quota active-instance count failed — admitting (fail-open)");
                0
            }
        }
    }
}
