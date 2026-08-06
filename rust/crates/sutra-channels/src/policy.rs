//! Channel-layer dispatcher policy — the *limit / feature-gate* seams the intake consults
//! before it admits an inbound delivery. These are the runtime policy objects that were
//! parsed but not enforced here until this layer landed:
//!
//! - [`PayloadCapPolicy`] — global default byte cap + per-channel overrides + the `0`
//!   "disabled" sentinel;
//! - [`FeatureProvider`] — the `${feature.X}` channel feature-gate;
//! - [`ConcurrencyStore`] — the per-channel `max-concurrent-instances` admission gauge;
//! - [`TenantQuotaEnforcer`] / [`DefaultTenantQuotaEnforcer`] — the per-tenant rate
//!   (sliding 60 s window) + concurrent-instance quotas.
//!
//! Every seam is OPTIONAL on the [`crate::ChannelEngine`]: with none wired the dispatcher
//! behaves exactly as it did before they existed. The diagnostic codes raised on denial
//! reuse the shared code constants (`crate::codes`), so no new wire-visible code is
//! minted here.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use sutra_executor::DeploymentId;

use crate::codes;
use crate::diag::Diagnostic;

// =====================================================================================
// PayloadCapPolicy — global default + per-channel override + 0-disables sentinel
// =====================================================================================

/// Two-tier inbound byte-cap policy. A `0` cap (global or
/// per-channel) is the documented "disabled / unlimited" sentinel; the cap is INCLUSIVE
/// (`payloadBytes == cap` passes); a NEGATIVE cap is a construction-time configuration
/// error (`SUTRA.CONFIG.PROPERTY.INVALID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadCapPolicy {
    global_cap_bytes: u64,
    per_channel: HashMap<String, u64>,
}

impl PayloadCapPolicy {
    /// The shipped default global cap: 10 MiB (mirrors the recommended inbound body-size
    /// ceiling so HTTP and broker channels enforce the same limit).
    pub const DEFAULT_MAX_PAYLOAD_BYTES: u64 = 10 * 1024 * 1024;

    /// Enforcement disabled everywhere (global cap `0`, no overrides).
    pub fn disabled() -> PayloadCapPolicy {
        PayloadCapPolicy {
            global_cap_bytes: 0,
            per_channel: HashMap::new(),
        }
    }

    /// A global-only cap (no per-channel overrides). Rejects a negative cap.
    pub fn of_global(global_cap_bytes: i64) -> Result<PayloadCapPolicy, Diagnostic> {
        PayloadCapPolicy::try_new(global_cap_bytes, Vec::new())
    }

    /// Construct from a global cap and per-channel overrides, rejecting any negative value
    /// at construction time with `SUTRA.CONFIG.PROPERTY.INVALID` — a negative cap is a
    /// config typo (use `0` to disable explicitly).
    pub fn try_new(
        global_cap_bytes: i64,
        per_channel: Vec<(String, i64)>,
    ) -> Result<PayloadCapPolicy, Diagnostic> {
        if global_cap_bytes < 0 {
            return Err(negative_cap_error(
                "sutra.codec.max-payload-bytes",
                global_cap_bytes,
            ));
        }
        let mut map = HashMap::with_capacity(per_channel.len());
        for (channel, cap) in per_channel {
            if cap < 0 {
                return Err(negative_cap_error(
                    &format!("sutra.channel.{channel}.max-payload-bytes"),
                    cap,
                ));
            }
            map.insert(channel, cap as u64);
        }
        Ok(PayloadCapPolicy {
            global_cap_bytes: global_cap_bytes as u64,
            per_channel: map,
        })
    }

    /// Set (or replace) a per-channel override — used by the builder to fold each channel's
    /// `payload-cap-bytes` YAML value into the policy.
    pub fn set_channel_override(&mut self, channel: &str, cap_bytes: u64) {
        self.per_channel.insert(channel.to_string(), cap_bytes);
    }

    /// The global default cap (`0` = disabled).
    pub fn global_cap_bytes(&self) -> u64 {
        self.global_cap_bytes
    }

    /// The effective cap for a channel: the per-channel override if present (absolute — it
    /// may RAISE the global default), else the global default. `0` means "no cap".
    pub fn effective_cap_bytes(&self, channel: &str) -> u64 {
        self.per_channel
            .get(channel)
            .copied()
            .unwrap_or(self.global_cap_bytes)
    }
}

fn negative_cap_error(property: &str, value: i64) -> Diagnostic {
    Diagnostic::error(
        codes::CONFIG_PROPERTY_INVALID,
        format!(
            "payload cap '{property}' = {value} is negative; a payload cap must be >= 0 \
             (use 0 to disable enforcement)"
        ),
    )
    .with_attribute("property", property)
    .with_attribute("value", value.to_string())
}

// =====================================================================================
// FeatureProvider — the ${feature.X} channel gate
// =====================================================================================

/// Resolves a channel's `enabled` feature-gate expression to a boolean — the
/// feature-provider boolean surface. A channel whose gate resolves to `false` is rejected
/// with `SUTRA.INBOUND.FEATURE_DISABLED` before any executor work.
pub trait FeatureProvider {
    /// `true` when the feature named by `expression` (e.g. `${feature.newPipeline}`) is
    /// enabled. The default when a provider cannot resolve the key is caller-defined.
    fn is_enabled(&self, expression: &str) -> bool;
}

/// A provider that enables every gate (the "no feature system configured" default).
pub struct AllowAllFeatureProvider;

impl FeatureProvider for AllowAllFeatureProvider {
    fn is_enabled(&self, _expression: &str) -> bool {
        true
    }
}

// =====================================================================================
// ConcurrencyStore — per-channel max-concurrent-instances admission gauge
// =====================================================================================

/// The replica-coherent active-instance gauge a channel's `max-concurrent-instances` cap is
/// enforced against — the channel-concurrency-store contract. One entry per
/// non-terminal instance, keyed `(deployment, instance_id)`, carrying the admitting channel
/// and a `RUNNING`/`WAITING` status. The dispatcher READS [`Self::count_active`] at admission
/// and records the START (it knows the channel); the state transitions after that are keyed by
/// instance id (the entry already carries the channel).
///
/// Production wiring backs this with the persisted `channel_instance` table (V701/V702) so the
/// count is the SAME on every replica and survives a crash — a persistence-less host falls back
/// to an in-memory store (per-process only). The dispatcher maintains the entries at its own
/// lifecycle commit points (park → WAITING, terminal → removed): the dispatcher owns
/// those transition points and has the channel, so it produces the same persisted end-state
/// the concurrency-tracker listener does.
///
/// ASYNC seam (execution scale-out §3(a), Phase 3): awaited on the shard lane's single
/// actor task — see [`crate::bridge::InstanceBridge`] for the ordering argument. `?Send`
/// because the consumer is the `Rc`-based dispatcher.
#[async_trait::async_trait(?Send)]
pub trait ConcurrencyStore {
    /// Admission count for the channel: `RUNNING` only, or `RUNNING` + `WAITING` when
    /// `include_waiting` (the `use-only-in-flight-for-concurrency-cap: false` case — a
    /// held/parked instance still holds its slot).
    async fn count_active(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        include_waiting: bool,
    ) -> u64;
    /// Instance admitted → `RUNNING` entry (the dispatcher supplies the channel).
    async fn record_started(&self, deployment: &DeploymentId, instance_id: &str, channel: &str);
    /// Instance parked at a wait node → `WAITING`. Unknown instance is a silent no-op.
    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: &str);
    /// Instance resumed → `RUNNING`. Unknown instance is a silent no-op.
    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: &str);
    /// Instance reached a terminal state → entry removed. Unknown instance is a silent no-op.
    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: &str);
}

/// Whether an in-flight instance is executing or parked at a wait node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceStatus {
    Running,
    Waiting,
}

/// In-memory [`ConcurrencyStore`] mirroring `InMemoryChannelConcurrencyStore` (tests / a
/// single-replica, persistence-less host). NOT replica-coherent or crash-safe — the persisted
/// store is the production path.
#[derive(Debug, Default)]
pub struct InMemoryConcurrencyStore {
    /// `(deployment, instance_id)` → `(channel, status)`.
    entries: RefCell<HashMap<(String, String), (String, InstanceStatus)>>,
}

impl InMemoryConcurrencyStore {
    pub fn new() -> InMemoryConcurrencyStore {
        InMemoryConcurrencyStore::default()
    }

    fn key(deployment: &DeploymentId, instance_id: &str) -> (String, String) {
        (deployment.value().to_string(), instance_id.to_string())
    }

    fn set_status(&self, deployment: &DeploymentId, instance_id: &str, status: InstanceStatus) {
        if let Some(entry) = self
            .entries
            .borrow_mut()
            .get_mut(&Self::key(deployment, instance_id))
        {
            entry.1 = status;
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ConcurrencyStore for InMemoryConcurrencyStore {
    async fn count_active(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        include_waiting: bool,
    ) -> u64 {
        let prefix = deployment.value();
        self.entries
            .borrow()
            .iter()
            .filter(|((dep, _), (chan, status))| {
                dep == prefix
                    && chan == channel
                    && (*status == InstanceStatus::Running
                        || (include_waiting && *status == InstanceStatus::Waiting))
            })
            .count() as u64
    }

    async fn record_started(&self, deployment: &DeploymentId, instance_id: &str, channel: &str) {
        self.entries.borrow_mut().insert(
            Self::key(deployment, instance_id),
            (channel.to_string(), InstanceStatus::Running),
        );
    }

    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: &str) {
        self.set_status(deployment, instance_id, InstanceStatus::Waiting);
    }

    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: &str) {
        self.set_status(deployment, instance_id, InstanceStatus::Running);
    }

    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: &str) {
        self.entries
            .borrow_mut()
            .remove(&Self::key(deployment, instance_id));
    }
}

// =====================================================================================
// TenantQuotaEnforcer — per-tenant rate + concurrent-instance quotas
// =====================================================================================

/// Per-tenant quotas (both dimensions optional — an omitted dimension is unlimited).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantQuotas {
    /// Ceiling on simultaneously in-flight instances for the tenant's deployment.
    pub max_concurrent_instances: Option<u64>,
    /// Ceiling on admitted inbounds within a sliding 60-second window.
    pub max_inbound_rate_per_minute: Option<u64>,
}

/// One tenant's configuration slice the quota enforcer reads (`quotas` absent ⇒ unlimited).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantConfig {
    pub tenant: String,
    pub quotas: Option<TenantQuotas>,
}

impl TenantConfig {
    /// A tenant with no quotas (unlimited).
    pub fn unlimited(tenant: &str) -> TenantConfig {
        TenantConfig {
            tenant: tenant.to_string(),
            quotas: None,
        }
    }
}

/// Resolves a tenant's [`TenantConfig`] — the tenant-config source.
pub trait TenantConfigSource {
    fn get(&self, tenant: &str) -> Option<TenantConfig>;
}

/// A fixed in-memory [`TenantConfigSource`] (tests / a static tenant set).
pub struct StaticTenantConfigSource {
    configs: HashMap<String, TenantConfig>,
}

impl StaticTenantConfigSource {
    pub fn new(configs: Vec<TenantConfig>) -> StaticTenantConfigSource {
        StaticTenantConfigSource {
            configs: configs.into_iter().map(|c| (c.tenant.clone(), c)).collect(),
        }
    }
}

impl TenantConfigSource for StaticTenantConfigSource {
    fn get(&self, tenant: &str) -> Option<TenantConfig> {
        self.configs.get(tenant).cloned()
    }
}

/// The active in-flight instance count per deployment — the quota enforcer's concurrency
/// substrate (the active-instance-count slice the enforcer consumes).
///
/// ASYNC seam (Phase 3), `?Send` — same posture as [`ConcurrencyStore`]: the production
/// impl is a persisted COUNT(*) awaited on the lane's actor task.
#[async_trait::async_trait(?Send)]
pub trait ActiveInstanceCount {
    async fn count_active(&self, deployment: &DeploymentId) -> u64;
}

/// In-memory [`ActiveInstanceCount`] (tests): a mutable per-deployment counter.
#[derive(Debug, Default)]
pub struct InMemoryActiveInstanceCount {
    counts: RefCell<HashMap<String, u64>>,
}

impl InMemoryActiveInstanceCount {
    pub fn new() -> InMemoryActiveInstanceCount {
        InMemoryActiveInstanceCount::default()
    }

    pub fn add(&self, deployment: &DeploymentId) {
        *self
            .counts
            .borrow_mut()
            .entry(deployment.value().to_string())
            .or_insert(0) += 1;
    }

    pub fn remove_one(&self, deployment: &DeploymentId) {
        if let Some(count) = self.counts.borrow_mut().get_mut(deployment.value()) {
            *count = count.saturating_sub(1);
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ActiveInstanceCount for InMemoryActiveInstanceCount {
    async fn count_active(&self, deployment: &DeploymentId) -> u64 {
        self.counts
            .borrow()
            .get(deployment.value())
            .copied()
            .unwrap_or(0)
    }
}

/// Monotonic wall clock the sliding rate window reads — a seam so tests can advance time.
pub trait Clock {
    /// Seconds since the Unix epoch.
    fn now_epoch_secs(&self) -> i64;
}

/// The system clock (`SystemTime::now`).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

/// The outcome of a quota check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaCheckResult {
    /// The inbound is within quota.
    Allowed,
    /// The inbound is denied; `reason` is the `SUTRA.*` code, `detail` the operator message.
    Denied { reason: String, detail: String },
}

impl QuotaCheckResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, QuotaCheckResult::Allowed)
    }
}

/// The tenant-quota admission gate — the tenant-quota-enforcer contract.
///
/// ASYNC seam (Phase 3), `?Send`: the concurrent-instance dimension awaits the
/// [`ActiveInstanceCount`] read on the lane's actor task.
#[async_trait::async_trait(?Send)]
pub trait TenantQuotaEnforcer {
    async fn check_inbound(
        &self,
        tenant: &str,
        deployment: &DeploymentId,
        channel: &str,
    ) -> QuotaCheckResult;
}

/// Rate + concurrent-instance quota enforcer — the default tenant-quota enforcer.
/// The rate dimension is a per-tenant sliding 60-second window of admitted-timestamp
/// counts; the concurrent dimension reads the live in-flight count. An omitted dimension is
/// unlimited; a tenant with no quota block is never throttled.
pub struct DefaultTenantQuotaEnforcer {
    configs: Box<dyn TenantConfigSource>,
    instances: Rc<dyn ActiveInstanceCount>,
    clock: Box<dyn Clock>,
    /// tenant → admitted-timestamp (epoch-seconds) window.
    rate_windows: RefCell<HashMap<String, VecDeque<i64>>>,
}

/// The sliding rate window length — 60 seconds (per-minute quota).
const RATE_WINDOW_SECS: i64 = 60;

impl DefaultTenantQuotaEnforcer {
    /// Construct with the system clock.
    pub fn new(
        configs: Box<dyn TenantConfigSource>,
        instances: Rc<dyn ActiveInstanceCount>,
    ) -> DefaultTenantQuotaEnforcer {
        DefaultTenantQuotaEnforcer::with_clock(configs, instances, Box::new(SystemClock))
    }

    /// Construct with an explicit clock (the sliding-window tests inject a mutable one).
    pub fn with_clock(
        configs: Box<dyn TenantConfigSource>,
        instances: Rc<dyn ActiveInstanceCount>,
        clock: Box<dyn Clock>,
    ) -> DefaultTenantQuotaEnforcer {
        DefaultTenantQuotaEnforcer {
            configs,
            instances,
            clock,
            rate_windows: RefCell::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl TenantQuotaEnforcer for DefaultTenantQuotaEnforcer {
    async fn check_inbound(
        &self,
        tenant: &str,
        deployment: &DeploymentId,
        channel: &str,
    ) -> QuotaCheckResult {
        let Some(quotas) = self.configs.get(tenant).and_then(|c| c.quotas) else {
            // No config / no quota block → unlimited.
            return QuotaCheckResult::Allowed;
        };

        // Concurrent-instance dimension. Awaited BEFORE the rate window is touched, so no
        // `RefCell` borrow is ever held across an await point.
        if let Some(max_concurrent) = quotas.max_concurrent_instances {
            let in_flight = self.instances.count_active(deployment).await;
            if in_flight >= max_concurrent {
                return QuotaCheckResult::Denied {
                    reason: codes::INBOUND_QUOTA_EXCEEDED_CONCURRENT.to_string(),
                    detail: format!(
                        "Channel '{channel}': tenant '{tenant}' has {in_flight} in-flight \
                         instances at maxConcurrentInstances={max_concurrent}"
                    ),
                };
            }
        }

        // Rate dimension — sliding 60-second window; a DENY does not record a timestamp.
        if let Some(max_rate) = quotas.max_inbound_rate_per_minute {
            let now = self.clock.now_epoch_secs();
            let mut windows = self.rate_windows.borrow_mut();
            let window = windows.entry(tenant.to_string()).or_default();
            while let Some(front) = window.front() {
                if now - *front >= RATE_WINDOW_SECS {
                    window.pop_front();
                } else {
                    break;
                }
            }
            if window.len() as u64 >= max_rate {
                return QuotaCheckResult::Denied {
                    reason: codes::INBOUND_QUOTA_EXCEEDED_RATE.to_string(),
                    detail: format!(
                        "Channel '{channel}': tenant '{tenant}' exceeded \
                         maxInboundRatePerMinute={max_rate}"
                    ),
                };
            }
            window.push_back(now);
        }

        QuotaCheckResult::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive one seam future to completion on the test thread — the async seams
    /// (Phase 3) never suspend over these in-memory impls, but they still return futures.
    #[cfg(feature = "transport")]
    fn drive<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(fut)
    }

    fn dep(tenant: &str) -> DeploymentId {
        let id = match tenant {
            "acme" => "dep-0000000000000000000000b1",
            "globex" => "dep-0000000000000000000000b2",
            _ => "dep-0000000000000000000000b9",
        };
        DeploymentId::of(id).expect("valid deployment id")
    }

    // ===== payload byte cap =====

    #[test]
    fn default_global_cap_is_ten_mebibytes() {
        assert_eq!(
            PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES,
            10 * 1024 * 1024
        );
    }

    #[test]
    fn global_default_is_returned_when_no_override_matches() {
        let policy = PayloadCapPolicy::try_new(2048, vec![("special".to_string(), 4096)]).unwrap();
        assert_eq!(policy.effective_cap_bytes("unconfigured-channel"), 2048);
    }

    #[test]
    fn per_channel_override_takes_precedence() {
        let policy =
            PayloadCapPolicy::try_new(2048, vec![("large-file".to_string(), 50 * 1024 * 1024)])
                .unwrap();
        assert_eq!(policy.effective_cap_bytes("large-file"), 50 * 1024 * 1024);
    }

    #[test]
    fn per_channel_override_of_zero_disables_cap_for_that_channel_only() {
        let policy = PayloadCapPolicy::try_new(1024, vec![("unlimited".to_string(), 0)]).unwrap();
        assert_eq!(policy.effective_cap_bytes("unlimited"), 0);
        assert_eq!(policy.effective_cap_bytes("strict"), 1024);
    }

    #[test]
    fn disabled_policy_reports_zero() {
        assert_eq!(PayloadCapPolicy::disabled().global_cap_bytes(), 0);
        assert_eq!(PayloadCapPolicy::disabled().effective_cap_bytes("any"), 0);
    }

    #[test]
    fn negative_global_cap_is_rejected_at_construction_time() {
        let err = PayloadCapPolicy::of_global(-1).expect_err("negative rejected");
        assert_eq!(err.code, codes::CONFIG_PROPERTY_INVALID);
        assert_eq!(
            err.attributes.get("property").map(String::as_str),
            Some("sutra.codec.max-payload-bytes")
        );
        assert_eq!(err.attributes.get("value").map(String::as_str), Some("-1"));
    }

    #[test]
    fn negative_per_channel_override_is_rejected_at_construction_time() {
        let err = PayloadCapPolicy::try_new(1024, vec![("rogue".to_string(), -42)])
            .expect_err("negative override rejected");
        assert_eq!(err.code, codes::CONFIG_PROPERTY_INVALID);
        assert_eq!(
            err.attributes.get("property").map(String::as_str),
            Some("sutra.channel.rogue.max-payload-bytes")
        );
        assert_eq!(err.attributes.get("value").map(String::as_str), Some("-42"));
    }

    #[test]
    fn zero_global_cap_remains_the_documented_disabled_sentinel() {
        let policy = PayloadCapPolicy::of_global(0).unwrap();
        assert_eq!(policy.global_cap_bytes(), 0);
        assert_eq!(policy.effective_cap_bytes("any"), 0);
    }

    // ===== ConcurrencyStore =====

    #[test]
    fn concurrency_store_counts_running_and_waiting_by_instance() {
        let store = InMemoryConcurrencyStore::new();
        let d = dep("acme");
        assert_eq!(drive(store.count_active(&d, "c", false)), 0);

        drive(store.record_started(&d, "i1", "c"));
        drive(store.record_started(&d, "i2", "c"));
        assert_eq!(drive(store.count_active(&d, "c", false)), 2);

        // A parked (WAITING) instance drops out of the RUNNING-only count but stays in the
        // include-waiting count (the held-line semantics).
        drive(store.record_suspended(&d, "i2"));
        assert_eq!(drive(store.count_active(&d, "c", false)), 1);
        assert_eq!(drive(store.count_active(&d, "c", true)), 2);

        // Resume restores it to RUNNING.
        drive(store.record_resumed(&d, "i2"));
        assert_eq!(drive(store.count_active(&d, "c", false)), 2);

        // Terminal removes the entry; a second terminal / unknown id is a silent no-op.
        drive(store.record_terminal(&d, "i1"));
        drive(store.record_terminal(&d, "i1"));
        drive(store.record_terminal(&d, "unknown"));
        assert_eq!(drive(store.count_active(&d, "c", false)), 1);

        // Channel isolation: a different channel is counted separately.
        drive(store.record_started(&d, "i3", "other"));
        assert_eq!(drive(store.count_active(&d, "c", true)), 1);
        assert_eq!(drive(store.count_active(&d, "other", true)), 1);
    }

    // ===== tenant quota enforcement =====

    struct MutableClock {
        now: RefCell<i64>,
    }
    impl MutableClock {
        fn new(start: i64) -> MutableClock {
            MutableClock {
                now: RefCell::new(start),
            }
        }
        fn advance_seconds(&self, secs: i64) {
            *self.now.borrow_mut() += secs;
        }
    }
    impl Clock for MutableClock {
        fn now_epoch_secs(&self) -> i64 {
            *self.now.borrow()
        }
    }

    fn single_tenant(tenant: &str, quotas: TenantQuotas) -> Box<dyn TenantConfigSource> {
        Box::new(StaticTenantConfigSource::new(vec![TenantConfig {
            tenant: tenant.to_string(),
            quotas: Some(quotas),
        }]))
    }

    const ACME: &str = "acme";
    const GLOBEX: &str = "globex";
    const CHANNEL: &str = "orders-in";

    #[test]
    fn under_limit_inbound_is_allowed() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: Some(100),
                max_inbound_rate_per_minute: Some(600),
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        let enforcer = DefaultTenantQuotaEnforcer::new(configs, store);
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
    }

    #[test]
    fn concurrent_instance_quota_at_limit_is_denied() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: Some(5),
                max_inbound_rate_per_minute: None,
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        for _ in 0..5 {
            store.add(&dep(ACME));
        }
        let enforcer = DefaultTenantQuotaEnforcer::new(configs, store);
        let result = drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL));
        match result {
            QuotaCheckResult::Denied { reason, detail } => {
                assert_eq!(reason, codes::INBOUND_QUOTA_EXCEEDED_CONCURRENT);
                assert!(detail.contains("5 in-flight instances"), "{detail}");
                assert!(detail.contains("maxConcurrentInstances=5"), "{detail}");
            }
            QuotaCheckResult::Allowed => panic!("expected denial"),
        }
    }

    #[test]
    fn rate_quota_at_limit_is_denied() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: None,
                max_inbound_rate_per_minute: Some(3),
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        let clock = Box::new(MutableClock::new(1_700_000_000));
        let enforcer = DefaultTenantQuotaEnforcer::with_clock(configs, store, clock);
        for _ in 0..3 {
            assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        }
        match drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)) {
            QuotaCheckResult::Denied { reason, detail } => {
                assert_eq!(reason, codes::INBOUND_QUOTA_EXCEEDED_RATE);
                assert!(detail.contains("maxInboundRatePerMinute=3"), "{detail}");
            }
            QuotaCheckResult::Allowed => panic!("expected denial"),
        }
    }

    #[test]
    fn rate_window_slides_after_60_seconds() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: None,
                max_inbound_rate_per_minute: Some(2),
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        let clock = MutableClock::new(1_700_000_000);
        // Reach into the enforcer with the shared clock — reconstruct via with_clock using a
        // clock we can advance through a raw pointer-free handle.
        let clock_handle = Rc::new(clock);
        let enforcer = DefaultTenantQuotaEnforcer::with_clock(
            configs,
            store,
            Box::new(SharedClock(Rc::clone(&clock_handle))),
        );
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        assert!(!drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        clock_handle.advance_seconds(61);
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        assert!(!drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
    }

    struct SharedClock(Rc<MutableClock>);
    impl Clock for SharedClock {
        fn now_epoch_secs(&self) -> i64 {
            self.0.now_epoch_secs()
        }
    }

    #[test]
    fn concurrent_count_decreases_on_instance_completion() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: Some(2),
                max_inbound_rate_per_minute: None,
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        store.add(&dep(ACME));
        store.add(&dep(ACME));
        let enforcer = DefaultTenantQuotaEnforcer::new(
            configs,
            Rc::clone(&store) as Rc<dyn ActiveInstanceCount>,
        );
        assert!(!drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        store.remove_one(&dep(ACME));
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
    }

    #[test]
    fn omitted_quotas_means_unlimited() {
        let configs = Box::new(StaticTenantConfigSource::new(vec![
            TenantConfig::unlimited(ACME),
        ]));
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        for _ in 0..10_000 {
            store.add(&dep(ACME));
        }
        let enforcer = DefaultTenantQuotaEnforcer::new(configs, store);
        for _ in 0..100 {
            assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        }
    }

    #[test]
    fn tenants_are_isolated() {
        let configs = Box::new(StaticTenantConfigSource::new(vec![
            TenantConfig {
                tenant: ACME.to_string(),
                quotas: Some(TenantQuotas {
                    max_concurrent_instances: None,
                    max_inbound_rate_per_minute: Some(1),
                }),
            },
            TenantConfig::unlimited(GLOBEX),
        ]));
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        let enforcer =
            DefaultTenantQuotaEnforcer::with_clock(configs, store, Box::new(MutableClock::new(0)));
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        assert!(!drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        for _ in 0..50 {
            assert!(drive(enforcer.check_inbound(GLOBEX, &dep(GLOBEX), CHANNEL)).is_allowed());
        }
    }

    #[test]
    fn denied_rate_names_channel_tenant_and_cap() {
        let configs = single_tenant(
            ACME,
            TenantQuotas {
                max_concurrent_instances: None,
                max_inbound_rate_per_minute: Some(1),
            },
        );
        let store = Rc::new(InMemoryActiveInstanceCount::new());
        let enforcer = DefaultTenantQuotaEnforcer::new(configs, store);
        assert!(drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)).is_allowed());
        match drive(enforcer.check_inbound(ACME, &dep(ACME), CHANNEL)) {
            QuotaCheckResult::Denied { reason, detail } => {
                assert_eq!(reason, codes::INBOUND_QUOTA_EXCEEDED_RATE);
                assert!(detail.contains(CHANNEL), "{detail}");
                assert!(detail.contains(ACME), "{detail}");
                assert!(detail.contains("maxInboundRatePerMinute=1"), "{detail}");
            }
            QuotaCheckResult::Allowed => panic!("expected denial"),
        }
    }
}
