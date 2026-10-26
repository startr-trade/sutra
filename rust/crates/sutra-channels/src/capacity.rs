//! Capacity pools — a named, cluster-wide count of slots shared by every channel that names it
//! (`pool: <name>`), across deployments and replicas.
//!
//! A pool is engine infrastructure, declared in the engine configuration
//! (`sutra.capacity-pools.<name>.*`) and referenced by name from channel files. It bounds how many
//! calls may be active at once, whichever channel carries them:
//!
//! - an **inbound** start on a pooled channel takes a slot or is rejected busy
//!   ([`crate::codes::INBOUND_POOL_EXHAUSTED`]);
//! - an **outbound** request on a pooled channel takes a slot for its call before it is sent;
//!   while the pool is full the outbox entry waits (no delivery attempt is counted).
//!
//! **The holding rule: one slot per active call.** A slot is keyed by `(instance, call key)`.
//! The call key of an inbound admission is the delivery's identity; that of an outbound request
//! is the outbox entry's identity — in both cases the value of a declared header instead when the
//! channel names one (`pool-call-key: header.<name>`). An instance bridging two calls therefore
//! holds two slots, while several requests carrying the same call key share one. A call's slot
//! is released when that call ends — a `session-end: on-message` naming its key, or the
//! admitting channel's `on-reply` for the inbound call — and every slot the instance still holds
//! is released when it ends (End, a failure, or a cancel). A park releases nothing.
//!
//! **Reserves.** `reserve.inbound = R` keeps `R` slots that outbound acquisitions may not take
//! while inbound holds fewer than `R` (and symmetrically for `reserve.outbound`). Formally, a
//! direction may take a slot when `used + 1 + max(0, reserve(other) − used(other)) ≤ slots`.
//!
//! The store seam ([`CapacityPoolStore`]) is `Send + Sync` because both the dispatcher lanes and
//! the outbox dispatcher task acquire through ONE shared instance; production backs it with a
//! persisted table whose acquisition counts under a row lock on the pool's own row, so replicas
//! cannot oversubscribe. [`InMemoryCapacityPools`] serves tests and a persistence-less host.

use std::collections::BTreeMap;
use std::sync::Mutex;

use sutra_executor::DeploymentId;

use crate::diag::Diagnostic;

/// The reserved outbox-entry header naming the capacity pool an outbound request draws from.
/// Stamped by the dispatcher when the request's channel declares `pool:`, read and STRIPPED by
/// the outbox dispatcher before the request reaches any transport.
pub const CAPACITY_POOL_HEADER: &str = "sutra-capacity-pool";
/// The reserved outbox-entry header marking a pooled request as a channel-call (a request whose
/// answer the instance waits for). A call still waiting for a slot when its instance has finished
/// is abandoned rather than sent. Stripped with [`CAPACITY_POOL_HEADER`].
pub const CAPACITY_CALL_HEADER: &str = "sutra-capacity-call";
/// The reserved outbox-entry header carrying a pooled request's call key, when its channel
/// declares `pool-call-key:` and the request carries that header. Absent, the outbox entry's own
/// id is the call key. Stripped with [`CAPACITY_POOL_HEADER`].
pub const CAPACITY_CALL_KEY_HEADER: &str = "sutra-capacity-call-key";

/// Which calls a [`CapacityPoolStore::release_calls`] releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallSelector<'a> {
    /// The slots held for this call key (in every pool).
    Call(&'a str),
    /// The slots taken in this direction (the inbound admission is the instance's one inbound
    /// call).
    Direction(SlotDirection),
}

impl CallSelector<'_> {
    /// Whether a slot held for `call` in `direction` is one this selector names.
    pub fn matches(&self, call: &str, direction: SlotDirection) -> bool {
        match self {
            CallSelector::Call(key) => *key == call,
            CallSelector::Direction(d) => *d == direction,
        }
    }
}

/// Which side of a pool a slot was taken for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SlotDirection {
    /// An instance admitted by a pooled inbound channel.
    Inbound,
    /// An instance whose outbound request on a pooled channel took the slot.
    Outbound,
}

impl SlotDirection {
    /// The persisted spelling (`INBOUND` / `OUTBOUND`).
    pub fn as_str(self) -> &'static str {
        match self {
            SlotDirection::Inbound => "INBOUND",
            SlotDirection::Outbound => "OUTBOUND",
        }
    }

    /// Parse the persisted spelling.
    pub fn parse(raw: &str) -> Option<SlotDirection> {
        match raw {
            "INBOUND" => Some(SlotDirection::Inbound),
            "OUTBOUND" => Some(SlotDirection::Outbound),
            _ => None,
        }
    }
}

/// One declared pool (`sutra.capacity-pools.<name>.*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityPool {
    pub name: String,
    /// `slots` — the total number of calls that may hold a slot at once.
    pub slots: u32,
    /// `reserve.inbound` — slots outbound acquisitions may never take from inbound.
    pub reserve_inbound: u32,
    /// `reserve.outbound` — slots inbound acquisitions may never take from outbound.
    pub reserve_outbound: u32,
    /// `tenants` — the optional allow-list of tenants whose channels may name the pool. `None`
    /// = every tenant.
    pub tenants: Option<Vec<String>>,
}

impl CapacityPool {
    /// A pool of `slots` with no reserves and no tenant restriction.
    pub fn new(name: &str, slots: u32) -> CapacityPool {
        CapacityPool {
            name: name.to_string(),
            slots,
            reserve_inbound: 0,
            reserve_outbound: 0,
            tenants: None,
        }
    }

    /// Builder: set the per-direction reserves.
    #[must_use]
    pub fn with_reserves(mut self, inbound: u32, outbound: u32) -> CapacityPool {
        self.reserve_inbound = inbound;
        self.reserve_outbound = outbound;
        self
    }

    /// Builder: restrict the pool to these tenants.
    #[must_use]
    pub fn with_tenants(mut self, tenants: Vec<String>) -> CapacityPool {
        self.tenants = Some(tenants);
        self
    }

    /// The declaration's own consistency: at least one slot, a name of lowercase letters,
    /// digits and dashes, and reserves that fit inside the pool.
    pub fn validate(&self) -> Result<(), String> {
        let valid_name = !self.name.is_empty()
            && self
                .name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !valid_name {
            return Err(format!(
                "capacity pool name '{}' must be lowercase letters, digits and dashes",
                self.name
            ));
        }
        if self.slots == 0 {
            return Err(format!(
                "capacity pool '{}' declares 0 slots; a pool needs at least one",
                self.name
            ));
        }
        if u64::from(self.reserve_inbound) + u64::from(self.reserve_outbound)
            > u64::from(self.slots)
        {
            return Err(format!(
                "capacity pool '{}': reserve.inbound ({}) + reserve.outbound ({}) exceeds its {} \
                 slots",
                self.name, self.reserve_inbound, self.reserve_outbound, self.slots
            ));
        }
        Ok(())
    }

    /// Whether a channel of `tenant` may name this pool.
    pub fn allows_tenant(&self, tenant: &str) -> bool {
        self.tenants
            .as_ref()
            .is_none_or(|allowed| allowed.iter().any(|t| t == tenant))
    }

    /// The reserve rule: may `direction` take one more slot when inbound holds `inbound_used`
    /// and outbound holds `outbound_used`?
    pub fn admits(&self, direction: SlotDirection, inbound_used: u32, outbound_used: u32) -> bool {
        let (other_used, other_reserve) = match direction {
            SlotDirection::Inbound => (outbound_used, self.reserve_outbound),
            SlotDirection::Outbound => (inbound_used, self.reserve_inbound),
        };
        let held_back = other_reserve.saturating_sub(other_used);
        u64::from(inbound_used) + u64::from(outbound_used) + 1 + u64::from(held_back)
            <= u64::from(self.slots)
    }
}

/// One acquisition request.
#[derive(Debug, Clone)]
pub struct SlotRequest<'a> {
    pub pool: &'a str,
    pub deployment: &'a DeploymentId,
    pub tenant: &'a str,
    /// The slot's holder key: the instance id (a UUID string). An inbound start acquires under
    /// a fresh admission key and re-keys it to the instance id once the instance exists
    /// ([`CapacityPoolStore::rebind`]).
    pub holder: &'a str,
    /// The call the slot is held for (see the module doc). The slot is `(holder, call, pool)`.
    pub call: &'a str,
    pub direction: SlotDirection,
    /// Answer [`SlotAcquisition::InstanceEnded`] instead of acquiring when the store knows the
    /// holder instance has already finished (a channel-call request that outlived its flow).
    pub require_live: bool,
}

/// The outcome of [`CapacityPoolStore::acquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAcquisition {
    /// A slot was taken for the holder.
    Acquired,
    /// The holder already holds a slot of this pool for this call.
    AlreadyHeld,
    /// No slot is free for this direction.
    Exhausted {
        inbound_used: u32,
        outbound_used: u32,
    },
    /// `require_live` and the holder instance has finished.
    InstanceEnded,
    /// An OUTBOUND request of a holder whose session has ended
    /// ([`CapacityPoolStore::end_session`]) — for the whole session, or for this request's call.
    /// The request is sent without a slot: an ended session holds none and takes none.
    SessionEnded,
    /// The pool is not declared (the configuration changed under a running deployment).
    UnknownPool,
}

/// The replica-coherent pool gauge. Every method is idempotent where the name says so; an
/// unknown holder is a silent no-op on release and rebind.
#[async_trait::async_trait]
pub trait CapacityPoolStore: Send + Sync {
    /// The declared pool of this name.
    fn pool(&self, name: &str) -> Option<CapacityPool>;

    /// Take a slot for `request.holder`, respecting the pool's size and the other direction's
    /// reserve. `Err` is a store failure; the caller decides between fail-open and waiting.
    async fn acquire(&self, request: &SlotRequest<'_>) -> Result<SlotAcquisition, Diagnostic>;

    /// Re-key every slot `from` holds in `deployment` to `to` (an admission key becoming the
    /// instance id). A no-op when `from` holds nothing.
    async fn rebind(&self, deployment: &DeploymentId, from: &str, to: &str);

    /// Release every slot `holder` holds in `deployment`, in any pool. Idempotent; returns how
    /// many were released.
    async fn release(&self, deployment: &DeploymentId, holder: &str) -> u32;

    /// The holder instance has FINISHED (End, a failure, a cancel): release every slot it holds,
    /// as [`Self::release`] does, and remember the instance ended so a request it emitted before
    /// finishing releases its slot as soon as it is delivered
    /// ([`Self::release_call_if_ended`]). A persisted store knows that from the instance's own
    /// row and needs nothing more.
    async fn release_ended(&self, deployment: &DeploymentId, holder: &str) -> u32 {
        self.release(deployment, holder).await
    }

    /// End `holder`'s session — the whole of it (`call = None`) or the one call named — while the
    /// instance runs on: release the slots it holds for that session or call, and remember that
    /// it ended, so a later OUTBOUND acquisition for it (a request still queued in the outbox
    /// when the session ended, or one the instance sends afterwards) answers
    /// [`SlotAcquisition::SessionEnded`] and is sent without a slot instead of re-taking one.
    /// Returns the directions of the slots released.
    ///
    /// The default only releases, for a store that keeps no session record.
    async fn end_session(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: Option<&str>,
    ) -> Vec<SlotDirection> {
        match call {
            Some(call) => {
                self.release_calls(deployment, holder, CallSelector::Call(call))
                    .await
            }
            None => {
                self.release(deployment, holder).await;
                Vec::new()
            }
        }
    }

    /// Whether [`Self::end_session`] ended `holder`'s whole session or its `call` — the check an
    /// outbound request makes without asking for a slot (when its pool is already known to be
    /// full this tick), so an ended session's request never waits for one. The default keeps no
    /// session record and answers `false`.
    async fn session_ended(&self, _deployment: &DeploymentId, _holder: &str, _call: &str) -> bool {
        false
    }

    /// Release the slots `holder` holds for the calls `selector` names, in any pool. Idempotent;
    /// returns the directions of the slots released (empty when none).
    async fn release_calls(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        selector: CallSelector<'_>,
    ) -> Vec<SlotDirection>;

    /// Release `holder`'s slots for `call` when the holder instance has already finished — a
    /// request it emitted before finishing has now been delivered (or given up on), so nothing
    /// is left to hold the slot for. A no-op while the instance runs. Returns how many were
    /// released.
    async fn release_call_if_ended(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: &str,
    ) -> u32;

    /// `(inbound, outbound)` slots currently held in the pool (observability and tests).
    async fn usage(&self, pool: &str) -> (u32, u32);
}

/// In-memory [`CapacityPoolStore`] — tests and a single-replica, persistence-less host. NOT
/// replica-coherent or crash-safe.
#[derive(Debug, Default)]
pub struct InMemoryCapacityPools {
    pools: BTreeMap<String, CapacityPool>,
    /// `(deployment, holder, pool, call)` → direction.
    slots: Mutex<BTreeMap<SlotKey, SlotDirection>>,
    /// `(deployment, holder)` of the instances [`CapacityPoolStore::release_ended`] saw finish,
    /// oldest first, bounded by [`ENDED_MEMORY`].
    ended: Mutex<std::collections::VecDeque<(String, String)>>,
    /// `(deployment, holder, call)` of the sessions [`CapacityPoolStore::end_session`] ended
    /// (`call` empty = the whole session), oldest first, bounded by [`ENDED_MEMORY`].
    sessions_ended: Mutex<std::collections::VecDeque<(String, String, String)>>,
}

/// `(deployment, holder, pool, call)`.
type SlotKey = (String, String, String, String);

/// How many finished instances the in-memory store remembers for
/// [`CapacityPoolStore::release_call_if_ended`]. A request delivered after its instance fell out
/// of this window keeps its slot until the process restarts — the in-memory store is per process
/// and not crash-safe by contract.
const ENDED_MEMORY: usize = 4096;

impl InMemoryCapacityPools {
    pub fn new(pools: impl IntoIterator<Item = CapacityPool>) -> InMemoryCapacityPools {
        InMemoryCapacityPools {
            pools: pools.into_iter().map(|p| (p.name.clone(), p)).collect(),
            slots: Mutex::new(BTreeMap::new()),
            ended: Mutex::new(std::collections::VecDeque::new()),
            sessions_ended: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Whether `holder`'s whole session, or its `call`, was ended by
    /// [`CapacityPoolStore::end_session`].
    fn ended_session(&self, deployment: &DeploymentId, holder: &str, call: &str) -> bool {
        self.sessions_ended
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|(d, h, c)| d == deployment.value() && h == holder && (c.is_empty() || c == call))
    }

    fn slots(&self) -> std::sync::MutexGuard<'_, BTreeMap<SlotKey, SlotDirection>> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn usage_of(slots: &BTreeMap<SlotKey, SlotDirection>, pool: &str) -> (u32, u32) {
        let mut inbound = 0;
        let mut outbound = 0;
        for ((_, _, p, _), direction) in slots {
            if p == pool {
                match direction {
                    SlotDirection::Inbound => inbound += 1,
                    SlotDirection::Outbound => outbound += 1,
                }
            }
        }
        (inbound, outbound)
    }

    /// Whether `holder` holds a slot of `pool` in `deployment`, for any call (tests).
    pub fn holds(&self, deployment: &DeploymentId, holder: &str, pool: &str) -> bool {
        self.held_calls(deployment, holder, pool) > 0
    }

    /// How many calls' slots `holder` holds of `pool` in `deployment` (tests).
    pub fn held_calls(&self, deployment: &DeploymentId, holder: &str, pool: &str) -> usize {
        self.slots()
            .keys()
            .filter(|(d, h, p, _)| d == deployment.value() && h == holder && p == pool)
            .count()
    }

    /// Release `holder`'s slots for which `release` answers true; returns their directions.
    fn release_where(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        release: impl Fn(&str, SlotDirection) -> bool,
    ) -> Vec<SlotDirection> {
        let mut slots = self.slots();
        let mut released = Vec::new();
        slots.retain(|(d, h, _, call), direction| {
            if d == deployment.value() && h == holder && release(call, *direction) {
                released.push(*direction);
                false
            } else {
                true
            }
        });
        released
    }
}

#[async_trait::async_trait]
impl CapacityPoolStore for InMemoryCapacityPools {
    fn pool(&self, name: &str) -> Option<CapacityPool> {
        self.pools.get(name).cloned()
    }

    async fn acquire(&self, request: &SlotRequest<'_>) -> Result<SlotAcquisition, Diagnostic> {
        let Some(pool) = self.pools.get(request.pool) else {
            return Ok(SlotAcquisition::UnknownPool);
        };
        let mut slots = self.slots();
        let key = (
            request.deployment.value().to_string(),
            request.holder.to_string(),
            request.pool.to_string(),
            request.call.to_string(),
        );
        if request.direction == SlotDirection::Outbound
            && self.ended_session(request.deployment, request.holder, request.call)
        {
            return Ok(SlotAcquisition::SessionEnded);
        }
        if slots.contains_key(&key) {
            return Ok(SlotAcquisition::AlreadyHeld);
        }
        let (inbound_used, outbound_used) = Self::usage_of(&slots, request.pool);
        if !pool.admits(request.direction, inbound_used, outbound_used) {
            return Ok(SlotAcquisition::Exhausted {
                inbound_used,
                outbound_used,
            });
        }
        slots.insert(key, request.direction);
        Ok(SlotAcquisition::Acquired)
    }

    async fn rebind(&self, deployment: &DeploymentId, from: &str, to: &str) {
        let mut slots = self.slots();
        let moved: Vec<(SlotKey, SlotDirection)> = slots
            .iter()
            .filter(|((d, h, _, _), _)| d == deployment.value() && h == from)
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        for ((d, _, pool, call), direction) in moved {
            slots.remove(&(d.clone(), from.to_string(), pool.clone(), call.clone()));
            slots
                .entry((d, to.to_string(), pool, call))
                .or_insert(direction);
        }
    }

    async fn release(&self, deployment: &DeploymentId, holder: &str) -> u32 {
        self.release_where(deployment, holder, |_, _| true).len() as u32
    }

    async fn release_ended(&self, deployment: &DeploymentId, holder: &str) -> u32 {
        {
            let mut ended = self.ended.lock().unwrap_or_else(|e| e.into_inner());
            let key = (deployment.value().to_string(), holder.to_string());
            if !ended.contains(&key) {
                if ended.len() >= ENDED_MEMORY {
                    ended.pop_front();
                }
                ended.push_back(key);
            }
        }
        self.release(deployment, holder).await
    }

    async fn end_session(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: Option<&str>,
    ) -> Vec<SlotDirection> {
        {
            let mut ended = self
                .sessions_ended
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let key = (
                deployment.value().to_string(),
                holder.to_string(),
                call.unwrap_or_default().to_string(),
            );
            if !ended.contains(&key) {
                if ended.len() >= ENDED_MEMORY {
                    ended.pop_front();
                }
                ended.push_back(key);
            }
        }
        match call {
            Some(call) => self.release_where(deployment, holder, |c, _| c == call),
            None => self.release_where(deployment, holder, |_, _| true),
        }
    }

    async fn session_ended(&self, deployment: &DeploymentId, holder: &str, call: &str) -> bool {
        self.ended_session(deployment, holder, call)
    }

    async fn release_calls(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        selector: CallSelector<'_>,
    ) -> Vec<SlotDirection> {
        self.release_where(deployment, holder, |call, direction| {
            selector.matches(call, direction)
        })
    }

    async fn release_call_if_ended(
        &self,
        deployment: &DeploymentId,
        holder: &str,
        call: &str,
    ) -> u32 {
        let ended = self
            .ended
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&(deployment.value().to_string(), holder.to_string()));
        if !ended {
            return 0;
        }
        self.release_calls(deployment, holder, CallSelector::Call(call))
            .await
            .len() as u32
    }

    async fn usage(&self, pool: &str) -> (u32, u32) {
        Self::usage_of(&self.slots(), pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_rule_holds_back_the_other_directions_unused_reserve() {
        let pool = CapacityPool::new("p", 5).with_reserves(2, 0);
        // Outbound may take 3 of 5 while inbound holds nothing (2 are kept for inbound).
        assert!(pool.admits(SlotDirection::Outbound, 0, 2));
        assert!(!pool.admits(SlotDirection::Outbound, 0, 3));
        // Inbound may still take the last two.
        assert!(pool.admits(SlotDirection::Inbound, 0, 3));
        assert!(pool.admits(SlotDirection::Inbound, 1, 3));
        assert!(!pool.admits(SlotDirection::Inbound, 2, 3));
        // Once inbound uses its reserve, nothing is held back from outbound.
        assert!(pool.admits(SlotDirection::Outbound, 2, 2));
    }

    #[test]
    fn validation_rejects_empty_pools_bad_names_and_oversized_reserves() {
        assert!(CapacityPool::new("p", 0).validate().is_err());
        assert!(CapacityPool::new("Bad_Name", 1).validate().is_err());
        assert!(CapacityPool::new("p", 3)
            .with_reserves(2, 2)
            .validate()
            .is_err());
        assert!(CapacityPool::new("main-1", 3)
            .with_reserves(1, 2)
            .validate()
            .is_ok());
    }

    #[test]
    fn tenant_allow_list_is_optional() {
        assert!(CapacityPool::new("p", 1).allows_tenant("anyone"));
        let pool = CapacityPool::new("p", 1).with_tenants(vec!["acme".into()]);
        assert!(pool.allows_tenant("acme"));
        assert!(!pool.allows_tenant("globex"));
    }
}
