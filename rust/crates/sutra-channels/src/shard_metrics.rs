//! Per-shard observability for the engine's shard router (execution scale-out §6.1) —
//! shipped WITH the N-lane feature, not after. Plain atomics, no exporter dependency:
//! this crate records; the engine's telemetry module reads the registry into OTel
//! observable instruments (`sutra.engine.shard.*`, one point per `shard` label).
//!
//! What each figure means (and where it is recorded):
//!
//! - **queue depth** — requests enqueued on the lane's mailbox and not yet dequeued.
//!   Incremented by the router side of `EngineHandle` at a successful send, decremented
//!   by the lane's actor loop at dequeue. The hot-key skew alarm: one lane's depth
//!   growing while its siblings idle is the §7 `correlation-heavy` signature.
//! - **dispatches** — work requests the lane's actor DRAINED (inbound deliveries,
//!   deferred deliveries, timer fires, scheduled starts, resolved-resume handoffs —
//!   everything except the activation `Update`). Counted in the actor loop.
//! - **parks** — initial park commits (`commit_park` succeeded): a spawn reached a wait
//!   state on this lane. Re-parks ride `resumes` (a resume pass that re-parked is still
//!   one resume pass).
//! - **resumes** — resume passes this lane COMMITTED (relay, timer fire, or handoff
//!   execution; terminal or re-park alike).
//! - **handoffs** — relays this lane RESOLVED to an instance owned by another lane
//!   (`DispatchOutcome::Handoff` answered; the router re-enqueues on the owner). The
//!   expected rate is `(S-1)/S` of relay deliveries; spawns and timer fires never hop.
//! - **claim bounces** — `CLAIM_HELD` refusals, split by path: `relay` (a correlated or
//!   handed-off resume found the instance claimed) and `timer` (a timer fire did). The
//!   mis-route alarm (§4): routing is an affinity optimization, never the correctness
//!   mechanism, so at a correct N>1 rollout this reads near zero outside genuine
//!   cross-replica contention.

use std::sync::atomic::{AtomicI64, AtomicU64};
// Only the recorders below order their stores, and they exist with the transport spine.
#[cfg(feature = "transport")]
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// One lane's counters. Every engine holds a handle (a default, unobserved one when
/// built outside the router — bare builders, unit tests), so the dispatch pipeline
/// records unconditionally and only router-built engines are exported.
#[derive(Debug, Default)]
pub struct ShardLaneMetrics {
    /// Mailbox depth: enqueued and not yet dequeued (send +1, dequeue −1). Signed so a
    /// racy read can never underflow-wrap a gauge.
    pub queue_depth: AtomicI64,
    /// Work requests drained by the lane's actor (everything except `Update`).
    pub dispatches: AtomicU64,
    /// Initial park commits on this lane.
    pub parks: AtomicU64,
    /// Resume passes committed on this lane (relay / timer / handoff; terminal or re-park).
    pub resumes: AtomicU64,
    /// Relays resolved here but owned elsewhere (`Handoff` answered to the router).
    pub handoffs: AtomicU64,
    /// `CLAIM_HELD` bounces on the relay/handoff resume path.
    pub claim_bounce_relay: AtomicU64,
    /// `CLAIM_HELD` bounces on the timer-fire path.
    pub claim_bounce_timer: AtomicU64,
}

/// The recording half — every call site is in the transport spine (the router in `http`, the
/// pipeline in `dispatch`), both of which are `feature = "transport"`. Gated to match, so a
/// `--no-default-features` build (the deploy-time lint's pure channel MODEL) does not carry
/// seven recorders nothing can call.
#[cfg(feature = "transport")]
impl ShardLaneMetrics {
    pub(crate) fn enqueued(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dequeued(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn dispatched(&self) {
        self.dispatches.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn parked(&self) {
        self.parks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn resumed(&self) {
        self.resumes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn handed_off(&self) {
        self.handoffs.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn claim_bounced_relay(&self) {
        self.claim_bounce_relay.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn claim_bounced_timer(&self) {
        self.claim_bounce_timer.fetch_add(1, Ordering::Relaxed);
    }
}

/// The whole router's lanes, index-aligned with the shard indexes. Created once per
/// router spawn and shared three ways: each `ShardSender` (queue depth), each lane's
/// actor loop (dispatch counts), and each lane's engine build (the semantic counters in
/// the dispatch pipeline). It outlives activation flips — a rebuilt engine keeps its
/// lane's handle, so the counters are process-lifetime like every other engine meter.
#[derive(Debug)]
pub struct ShardRouterMetrics {
    lanes: Vec<Arc<ShardLaneMetrics>>,
}

impl ShardRouterMetrics {
    /// A registry with one lane per shard (`count >= 1`).
    pub fn new(count: u32) -> ShardRouterMetrics {
        ShardRouterMetrics {
            lanes: (0..count.max(1))
                .map(|_| Arc::new(ShardLaneMetrics::default()))
                .collect(),
        }
    }

    /// The lane handle for `index` (panics on an out-of-range index — the router and the
    /// assembly agree on the count by construction).
    pub fn lane(&self, index: u32) -> Arc<ShardLaneMetrics> {
        Arc::clone(&self.lanes[index as usize])
    }

    /// Every lane, index-aligned with the shard indexes (the exporter's iteration seam).
    pub fn lanes(&self) -> &[Arc<ShardLaneMetrics>] {
        &self.lanes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "transport")]
    fn lanes_are_index_aligned_and_independent() {
        let metrics = ShardRouterMetrics::new(4);
        assert_eq!(metrics.lanes().len(), 4);
        metrics.lane(2).dispatched();
        metrics.lane(2).dispatched();
        metrics.lane(3).parked();
        assert_eq!(metrics.lanes()[2].dispatches.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.lanes()[3].parks.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.lanes()[0].dispatches.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_zero_count_still_yields_one_lane() {
        // Defensive floor only — config validation refuses 0 long before here.
        assert_eq!(ShardRouterMetrics::new(0).lanes().len(), 1);
    }
}
