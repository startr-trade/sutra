//! DB-lease leader election on top of the sutra-persistence
//! [`PgLeaseStore`]: one poll task per role calls `try_acquire(role, identity, ttl)` at
//! `poll_interval` cadence; a successful acquire (fresh, expired takeover, or
//! renewal-by-acquire) flips the role to leader, a contended lease flips it to follower.
//!
//! The election exposes each role as a [`LeaderGate`] — the seam a singleton
//! broker consumer polls per (re)connect and delivery-loop turn. Roles register
//! DYNAMICALLY on first [`DbLeaderElection::gate`] call (leadership-change
//! auto-registration), so a singleton channel's role
//! (`sutra-channel:<tenant>:<channel>`, [`channel_role`]) starts being contended
//! without pre-listing in config.
//!
//! Fixed constants: `ttl = 30s`, `poll = 10s`, poll strictly less than ttl,
//! first poll one full interval after registration. There is no listener callback
//! surface — gate polling is the notification mechanism (the consumer
//! re-checks `is_leading()` on every loop turn; singleton semantics preserved).
//!
//! When no engine datasource is configured the posture falls back to
//! [`AlwaysLeading`] (a no-op election): every replica leads, gating is a
//! no-op — see [`ChannelLeadership`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sutra_channels::{AlwaysLeading, BoxFuture, LeaderGate};
use sutra_persistence::stores::{LeaseStore as _, PgLeaseStore};
use tracing::{info, warn};

/// Default lease TTL.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);
/// Default polling cadence — strictly less than TTL.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The dynamic lease-role name gating one singleton channel consumer.
pub fn channel_role(tenant: &str, channel: &str) -> String {
    format!("sutra-channel:{tenant}:{channel}")
}

/// The minimal async lease surface the election polls — dyn-compatible (BoxFuture)
/// mirror of the sutra-persistence `LeaseStore`, so tests can stub it in memory.
pub trait LeaseHandle: Send + Sync {
    /// Acquire-or-renew; `Ok(true)` when this holder now owns the lease.
    fn try_acquire<'a>(
        &'a self,
        name: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, String>>;
    /// The current holder, if any (diagnostics after a contended poll).
    fn current_holder<'a>(&'a self, name: &'a str)
        -> BoxFuture<'a, Result<Option<String>, String>>;
    /// Release if held by `holder` (not holding is a no-op).
    fn release<'a>(&'a self, name: &'a str, holder: &'a str) -> BoxFuture<'a, Result<(), String>>;
}

/// [`LeaseHandle`] over the durable PostgreSQL lease store.
pub struct PgLeaseHandle(pub PgLeaseStore);

impl LeaseHandle for PgLeaseHandle {
    fn try_acquire<'a>(
        &'a self,
        name: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, String>> {
        Box::pin(async move {
            self.0
                .try_acquire(name, holder, ttl)
                .await
                .map(|lease| lease.is_some())
                .map_err(|e| e.to_string())
        })
    }

    fn current_holder<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        Box::pin(async move {
            self.0
                .current(name)
                .await
                .map(|lease| lease.map(|l| l.holder))
                .map_err(|e| e.to_string())
        })
    }

    fn release<'a>(&'a self, name: &'a str, holder: &'a str) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.0
                .release(name, holder)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

/// Per-role live state — the gate view reads `is_leader` lock-free.
struct RoleState {
    is_leader: AtomicBool,
    current_holder: Mutex<Option<String>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RoleState {
    fn new() -> Arc<RoleState> {
        Arc::new(RoleState {
            is_leader: AtomicBool::new(false),
            current_holder: Mutex::new(None),
            task: Mutex::new(None),
        })
    }
}

struct Inner {
    store: Arc<dyn LeaseHandle>,
    identity: String,
    ttl: Duration,
    poll_interval: Duration,
    closed: AtomicBool,
    states: Mutex<HashMap<String, Arc<RoleState>>>,
    handle: tokio::runtime::Handle,
}

/// The election daemon. Cheap to clone-share via [`Arc`]; every registered role polls
/// on its own tokio task until [`DbLeaderElection::release_all`].
pub struct DbLeaderElection {
    inner: Arc<Inner>,
}

/// One role's [`LeaderGate`] view onto the election.
pub struct RoleGate {
    state: Arc<RoleState>,
}

impl LeaderGate for RoleGate {
    fn is_leading(&self) -> bool {
        self.state.is_leader.load(Ordering::SeqCst)
    }
}

impl DbLeaderElection {
    /// Election with the default timings (ttl 30s / poll 10s).
    pub fn with_defaults(
        store: Arc<dyn LeaseHandle>,
        identity: Option<String>,
        handle: tokio::runtime::Handle,
    ) -> DbLeaderElection {
        DbLeaderElection::new(store, identity, DEFAULT_TTL, DEFAULT_POLL_INTERVAL, handle)
            .expect("default timings are valid")
    }

    /// Election with explicit timings. Fails when `ttl` or `poll_interval` is zero or
    /// `poll_interval >= ttl` (constructor validation).
    pub fn new(
        store: Arc<dyn LeaseHandle>,
        identity: Option<String>,
        ttl: Duration,
        poll_interval: Duration,
        handle: tokio::runtime::Handle,
    ) -> Result<DbLeaderElection, String> {
        if ttl.is_zero() {
            return Err("ttl must be strictly positive".to_string());
        }
        if poll_interval.is_zero() {
            return Err("pollInterval must be strictly positive".to_string());
        }
        if poll_interval >= ttl {
            return Err(format!(
                "pollInterval must be strictly less than ttl (got pollInterval={poll_interval:?}, \
                 ttl={ttl:?})"
            ));
        }
        Ok(DbLeaderElection {
            inner: Arc::new(Inner {
                store,
                identity: resolve_identity(identity),
                ttl,
                poll_interval,
                closed: AtomicBool::new(false),
                states: Mutex::new(HashMap::new()),
                handle,
            }),
        })
    }

    /// The identity this elector acquires leases under.
    pub fn identity(&self) -> &str {
        &self.inner.identity
    }

    /// Register `role` (idempotent, dynamic — leadership-change
    /// auto-registration) and return its gate. The role's poll loop starts on the
    /// election's runtime handle; the first poll fires one full interval later.
    pub fn gate(&self, role: &str) -> Arc<RoleGate> {
        let state = self.register_role(role);
        Arc::new(RoleGate { state })
    }

    fn register_role(&self, role: &str) -> Arc<RoleState> {
        let mut states = self.inner.states.lock().expect("states lock");
        if let Some(existing) = states.get(role) {
            return Arc::clone(existing);
        }
        let state = RoleState::new();
        states.insert(role.to_string(), Arc::clone(&state));
        if !self.inner.closed.load(Ordering::SeqCst) {
            let inner = Arc::clone(&self.inner);
            let task_state = Arc::clone(&state);
            let role_name = role.to_string();
            info!(
                role = %role_name,
                identity = %inner.identity,
                ttl = ?inner.ttl,
                poll_interval = ?inner.poll_interval,
                "SUTRA.LEADERSHIP.DB.STARTING"
            );
            let task = self.inner.handle.spawn(async move {
                loop {
                    // First poll one full interval after registration, so
                    // callers can install gates before the scheduler races.
                    tokio::time::sleep(inner.poll_interval).await;
                    if inner.closed.load(Ordering::SeqCst) {
                        return;
                    }
                    poll_once(&inner, &role_name, &task_state).await;
                }
            });
            *state.task.lock().expect("task lock") = Some(task);
        }
        state
    }

    /// True while this replica holds the lease for `role` (unknown roles are followers).
    pub fn is_leader(&self, role: &str) -> bool {
        if self.inner.closed.load(Ordering::SeqCst) {
            return false;
        }
        let states = self.inner.states.lock().expect("states lock");
        states
            .get(role)
            .is_some_and(|s| s.is_leader.load(Ordering::SeqCst))
    }

    /// The last observed holder of `role`, if any.
    pub fn current_holder(&self, role: &str) -> Option<String> {
        let states = self.inner.states.lock().expect("states lock");
        states
            .get(role)
            .and_then(|s| s.current_holder.lock().expect("holder lock").clone())
    }

    /// Force one immediate poll for `role` (deterministic tests / diagnostics).
    /// Panics on an unknown role.
    pub async fn poll_now(&self, role: &str) {
        let state = {
            let states = self.inner.states.lock().expect("states lock");
            states
                .get(role)
                .cloned()
                .unwrap_or_else(|| panic!("unknown role: {role}"))
        };
        poll_once(&self.inner, role, &state).await;
    }

    /// Shut down: stop every poll task, release every held lease, report follower
    /// everywhere. Idempotent.
    pub async fn release_all(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return; // second release is a no-op
        }
        // Cancel the scheduled polls first so no poll races the release.
        let states: Vec<(String, Arc<RoleState>)> = {
            let states = self.inner.states.lock().expect("states lock");
            states
                .iter()
                .map(|(role, state)| (role.clone(), Arc::clone(state)))
                .collect()
        };
        for (_, state) in &states {
            if let Some(task) = state.task.lock().expect("task lock").take() {
                task.abort();
            }
        }
        for (role, state) in &states {
            // Release even when not currently flagged leader — an in-flight poll may
            // have just acquired without flipping the flag yet.
            let was_leader = state.is_leader.swap(false, Ordering::SeqCst);
            match self.inner.store.release(role, &self.inner.identity).await {
                Ok(()) => {
                    if was_leader {
                        info!(
                            role = %role,
                            identity = %self.inner.identity,
                            "SUTRA.LEADERSHIP.DB.RELEASED"
                        );
                    }
                }
                Err(e) => {
                    warn!(role = %role, error = %e, "lease release failed");
                }
            }
        }
    }
}

/// One poll turn — the poll state machine (acquire ⇒ leader, contended ⇒
/// follower + holder lookup, store failure ⇒ follower-for-safety, never panics the
/// scheduler).
async fn poll_once(inner: &Inner, role: &str, state: &RoleState) {
    if inner.closed.load(Ordering::SeqCst) {
        return;
    }
    match inner
        .store
        .try_acquire(role, &inner.identity, inner.ttl)
        .await
    {
        Ok(true) => {
            *state.current_holder.lock().expect("holder lock") = Some(inner.identity.clone());
            let was_leader = state.is_leader.swap(true, Ordering::SeqCst);
            if !was_leader {
                info!(
                    role = %role,
                    identity = %inner.identity,
                    "SUTRA.LEADERSHIP.DB.ACQUIRED"
                );
            }
        }
        Ok(false) => {
            let holder: Option<String> = inner.store.current_holder(role).await.unwrap_or_default();
            if let Some(h) = &holder {
                *state.current_holder.lock().expect("holder lock") = Some(h.clone());
            }
            let was_leader = state.is_leader.swap(false, Ordering::SeqCst);
            if was_leader {
                warn!(
                    role = %role,
                    identity = %inner.identity,
                    new_holder = %holder.as_deref().unwrap_or("<unknown>"),
                    "SUTRA.LEADERSHIP.DB.LOST"
                );
            }
        }
        Err(e) => {
            // Transient store failure — treat as non-leader for safety; next poll retries.
            state.is_leader.store(false, Ordering::SeqCst);
            warn!(
                role = %role,
                identity = %inner.identity,
                error = %e,
                "SUTRA.LEADERSHIP.DB.POLL_FAILED"
            );
        }
    }
}

/// Identity precedence: explicit override → `HOSTNAME` env →
/// random fallback.
fn resolve_identity(overridden: Option<String>) -> String {
    if let Some(id) = overridden {
        let trimmed = id.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    if let Ok(host) = std::env::var("HOSTNAME") {
        if !host.trim().is_empty() {
            return host;
        }
    }
    format!("db-{}", uuid::Uuid::new_v4())
}

/// The channel-consumer gating posture the wiring hands to broker sources: DB-lease
/// election when an engine datasource exists, [`AlwaysLeading`] otherwise
/// (no election — every replica leads).
pub enum ChannelLeadership {
    /// Singleton roles contend through the durable lease table.
    Elected(Arc<DbLeaderElection>),
    /// No datasource — no election, everyone leads.
    AlwaysLeading,
}

impl ChannelLeadership {
    /// The gate for one lease role.
    pub fn gate_for(&self, role: &str) -> Arc<dyn LeaderGate> {
        match self {
            ChannelLeadership::Elected(election) => election.gate(role),
            ChannelLeadership::AlwaysLeading => Arc::new(AlwaysLeading),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::time::Instant;

    use super::*;

    /// In-memory [`LeaseHandle`] with the durable lease store's semantics: acquire
    /// succeeds when the lease is unowned, expired, or renewal-by-self.
    #[derive(Default)]
    struct InMemoryLeases {
        leases: Mutex<HashMap<String, (String, Instant)>>,
        acquire_count: AtomicU32,
        fail_next_acquire: AtomicBool,
    }

    impl InMemoryLeases {
        fn seed(&self, name: &str, holder: &str, ttl: Duration) {
            self.leases
                .lock()
                .unwrap()
                .insert(name.to_string(), (holder.to_string(), Instant::now() + ttl));
        }

        fn evict(&self, name: &str) {
            self.leases.lock().unwrap().remove(name);
        }
    }

    impl LeaseHandle for InMemoryLeases {
        fn try_acquire<'a>(
            &'a self,
            name: &'a str,
            holder: &'a str,
            ttl: Duration,
        ) -> BoxFuture<'a, Result<bool, String>> {
            Box::pin(async move {
                self.acquire_count.fetch_add(1, Ordering::SeqCst);
                if self.fail_next_acquire.swap(false, Ordering::SeqCst) {
                    return Err("simulated DB failure".to_string());
                }
                let mut leases = self.leases.lock().unwrap();
                if let Some((existing_holder, expires_at)) = leases.get(name) {
                    if *expires_at > Instant::now() && existing_holder != holder {
                        return Ok(false);
                    }
                }
                leases.insert(name.to_string(), (holder.to_string(), Instant::now() + ttl));
                Ok(true)
            })
        }

        fn current_holder<'a>(
            &'a self,
            name: &'a str,
        ) -> BoxFuture<'a, Result<Option<String>, String>> {
            Box::pin(async move {
                Ok(self
                    .leases
                    .lock()
                    .unwrap()
                    .get(name)
                    .map(|(holder, _)| holder.clone()))
            })
        }

        fn release<'a>(
            &'a self,
            name: &'a str,
            holder: &'a str,
        ) -> BoxFuture<'a, Result<(), String>> {
            Box::pin(async move {
                let mut leases = self.leases.lock().unwrap();
                if leases.get(name).is_some_and(|(h, _)| h == holder) {
                    leases.remove(name);
                }
                Ok(())
            })
        }
    }

    /// Poll-interval long enough that the background scheduler never fires during a
    /// test — every test drives timing via `poll_now`.
    fn elector(store: Arc<InMemoryLeases>, identity: &str) -> DbLeaderElection {
        DbLeaderElection::new(
            store,
            Some(identity.to_string()),
            Duration::from_secs(600),
            Duration::from_secs(300),
            tokio::runtime::Handle::current(),
        )
        .expect("valid timings")
    }

    #[tokio::test]
    async fn acquires_leadership_on_first_poll() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");
        let gate = election.gate("timer-leader");
        assert!(!gate.is_leading(), "follower until the first poll");

        election.poll_now("timer-leader").await;

        assert!(election.is_leader("timer-leader"));
        assert!(gate.is_leading());
        assert_eq!(
            election.current_holder("timer-leader").as_deref(),
            Some("replica-A")
        );
        assert!(store.acquire_count.load(Ordering::SeqCst) >= 1);
        election.release_all().await;
    }

    #[tokio::test]
    async fn does_not_acquire_when_another_holder_owns_the_lease() {
        let store = Arc::new(InMemoryLeases::default());
        store.seed("timer-leader", "replica-B", Duration::from_secs(300));
        let election = elector(Arc::clone(&store), "replica-A");
        let gate = election.gate("timer-leader");

        election.poll_now("timer-leader").await;

        assert!(!election.is_leader("timer-leader"));
        assert!(!gate.is_leading());
        assert_eq!(
            election.current_holder("timer-leader").as_deref(),
            Some("replica-B")
        );
        election.release_all().await;
    }

    #[tokio::test]
    async fn renewal_by_acquire_keeps_leadership() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");
        election.gate("timer-leader");

        election.poll_now("timer-leader").await;
        election.poll_now("timer-leader").await;
        election.poll_now("timer-leader").await;

        assert!(election.is_leader("timer-leader"));
        assert!(store.acquire_count.load(Ordering::SeqCst) >= 3);
        election.release_all().await;
    }

    #[tokio::test]
    async fn loses_leadership_when_another_holder_takes_over() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");
        let gate = election.gate("timer-leader");

        election.poll_now("timer-leader").await;
        assert!(gate.is_leading());

        // Expire + takeover by another holder.
        store.evict("timer-leader");
        store.seed("timer-leader", "replica-B", Duration::from_secs(300));

        election.poll_now("timer-leader").await;
        assert!(!gate.is_leading(), "the gate must flip on leadership loss");
        assert!(!election.is_leader("timer-leader"));
        assert_eq!(
            election.current_holder("timer-leader").as_deref(),
            Some("replica-B")
        );
        election.release_all().await;
    }

    #[tokio::test]
    async fn release_all_removes_the_lease_and_reports_follower() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");
        let gate = election.gate("timer-leader");
        election.poll_now("timer-leader").await;
        assert!(gate.is_leading());
        assert!(store.leases.lock().unwrap().contains_key("timer-leader"));

        election.release_all().await;

        assert!(!election.is_leader("timer-leader"));
        assert!(!gate.is_leading());
        assert!(!store.leases.lock().unwrap().contains_key("timer-leader"));

        // Second release is a no-op and must complete without panicking.
        election.release_all().await;
    }

    #[tokio::test]
    async fn multiple_roles_are_elected_independently() {
        let store = Arc::new(InMemoryLeases::default());
        store.seed("stuck-scanner", "replica-B", Duration::from_secs(300));
        let election = elector(Arc::clone(&store), "replica-A");
        election.gate("timer-leader");
        election.gate("stuck-scanner");

        election.poll_now("timer-leader").await;
        election.poll_now("stuck-scanner").await;

        assert!(election.is_leader("timer-leader"));
        assert!(!election.is_leader("stuck-scanner"));
        assert_eq!(
            election.current_holder("stuck-scanner").as_deref(),
            Some("replica-B")
        );
        election.release_all().await;
    }

    #[tokio::test]
    async fn gate_registers_unknown_singleton_channel_roles_dynamically() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");

        // A singleton channel's lease role is NOT pre-listed anywhere; requesting its
        // gate must REGISTER it (start contending), not discard it.
        let role = channel_role("acme", "pay");
        assert_eq!(role, "sutra-channel:acme:pay");
        let gate = election.gate(&role);
        assert!(!gate.is_leading());
        assert!(!election.is_leader(&role));

        election.poll_now(&role).await;
        assert!(gate.is_leading());
        assert_eq!(election.current_holder(&role).as_deref(), Some("replica-A"));
        election.release_all().await;
    }

    #[tokio::test]
    async fn poll_failure_is_treated_as_follower_and_next_poll_recovers() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(Arc::clone(&store), "replica-A");
        let gate = election.gate("timer-leader");

        election.poll_now("timer-leader").await;
        assert!(gate.is_leading());

        store.fail_next_acquire.store(true, Ordering::SeqCst);
        election.poll_now("timer-leader").await;
        assert!(!gate.is_leading(), "store failure demotes for safety");

        election.poll_now("timer-leader").await;
        assert!(gate.is_leading(), "next poll recovers");
        election.release_all().await;
    }

    #[tokio::test]
    async fn unknown_role_always_reports_follower() {
        let store = Arc::new(InMemoryLeases::default());
        let election = elector(store, "replica-A");
        assert!(!election.is_leader("never-registered"));
        assert_eq!(election.current_holder("never-registered"), None);
        election.release_all().await;
    }

    #[tokio::test]
    async fn scheduled_task_actually_fires_polls_without_manual_help() {
        let store = Arc::new(InMemoryLeases::default());
        let election = DbLeaderElection::new(
            Arc::clone(&store) as Arc<dyn LeaseHandle>,
            Some("replica-fast".to_string()),
            Duration::from_secs(1),
            Duration::from_millis(20),
            tokio::runtime::Handle::current(),
        )
        .expect("valid timings");
        let gate = election.gate("timer-leader");

        // No poll_now — wait for the background scheduler.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !gate.is_leading() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(gate.is_leading(), "background poll must acquire");
        election.release_all().await;
    }

    #[tokio::test]
    async fn constructor_rejects_invalid_timings() {
        let store: Arc<dyn LeaseHandle> = Arc::new(InMemoryLeases::default());
        let handle = tokio::runtime::Handle::current();
        assert!(DbLeaderElection::new(
            Arc::clone(&store),
            Some("id".to_string()),
            Duration::ZERO,
            Duration::from_millis(1),
            handle.clone(),
        )
        .is_err());
        assert!(DbLeaderElection::new(
            Arc::clone(&store),
            Some("id".to_string()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            handle.clone(),
        )
        .is_err());
        assert!(DbLeaderElection::new(
            store,
            Some("id".to_string()),
            Duration::from_secs(1),
            Duration::ZERO,
            handle,
        )
        .is_err());
    }

    #[tokio::test]
    async fn always_leading_posture_gates_nothing() {
        let leadership = ChannelLeadership::AlwaysLeading;
        assert!(leadership.gate_for("sutra-channel:acme:pay").is_leading());
    }
}
