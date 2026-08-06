//! Deferred-ack registry — the message-ack registry contract
//! for `ack-mode=on-complete` transports:
//!
//! - `INSTANCE_COMPLETED` → the ack callback fires exactly once; the entry is removed.
//! - `INSTANCE_FAILED` → the nack callback fires exactly once; the entry is removed.
//! - Time-out (configurable) → nack; the entry is settled so the broker slot frees; inbox
//!   dedup catches any redelivery.
//! - LRU eviction at the bounded size cap → nack on the evicted (oldest) entry.
//! - Duplicate registration for the same instance is a no-op — the first wins.
//!
//! Callbacks must be idempotent and must not block. HTTP `on-complete` holds the
//! connection instead (the sync reply IS the ack) — this registry serves broker-style
//! transports, per the ack-mode contract.
//!
//! Threading: the registry is `Send + Sync` (`Mutex` interior, `Send` callbacks) because
//! it is shared across three seams — REGISTRATION happens on the engine actor thread
//! (inside `ChannelEngine::dispatch_deferred`'s park arm, BEFORE `commit_park` and
//! withdrawn on a failed commit, so a terminal event for the instance — from this lane
//! or, under the shard router, any other — always finds the registration), terminal
//! events fire via the executor listener bus, and `sweep_timeouts()` runs on a tokio
//! interval task.
//! Callbacks are `Send` closures over the transport's native ack/nack (they cross from
//! the transport task into the actor thread with the dispatch request).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::codes;

type AckCallback = Box<dyn FnMut() + Send>;

/// Fire an ack/nack callback, SWALLOWING any panic it raises — a misbehaving broker
/// callback must never corrupt the registry or unwind through the executor listener bus
/// (the "swallow callback exception" contract). The callback is
/// invoked outside any registry lock, so catching the unwind here is re-entrancy-safe.
fn fire_swallowing(callback: &mut AckCallback) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
}

struct Pending {
    instance_id: String,
    /// The delivering channel (diagnostics — every `SUTRA.ACK.*` event carries it).
    channel: String,
    registered_at: Instant,
    ack: AckCallback,
    nack: AckCallback,
}

/// Bounded registry of deferred acks (`Send + Sync` — see the module docs for the
/// three-seam threading contract; callbacks always fire OUTSIDE the internal lock).
pub struct DeferredAckRegistry {
    entries: Mutex<VecDeque<Pending>>,
    capacity: usize,
    timeout: Duration,
}

impl DeferredAckRegistry {
    /// Defaults ride the engine config (`sutra.ack.deferred.*`): capacity 10 000,
    /// timeout 1 h. A zero capacity is clamped to one.
    pub fn new(capacity: usize, timeout: Duration) -> DeferredAckRegistry {
        DeferredAckRegistry {
            entries: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            timeout,
        }
    }

    /// Register a deferred ack for a freshly-started instance on `channel`. `false` on a
    /// duplicate registration (the new callbacks are dropped). At the size cap the OLDEST
    /// entry is evicted with a nack (`SUTRA.ACK.DEFERRED_OVERFLOW`), then the new one is
    /// accepted.
    pub fn register(
        &self,
        instance_id: &str,
        channel: &str,
        ack: impl FnMut() + Send + 'static,
        nack: impl FnMut() + Send + 'static,
    ) -> bool {
        // Mutate under one lock, but fire the evicted nack only AFTER releasing it —
        // the "invoked outside any registry lock" re-entrancy contract (a nack may
        // call back into the registry, e.g. to re-register the redelivery).
        let evicted = {
            let mut entries = self.entries.lock().expect("deferred-ack registry");
            if entries.iter().any(|p| p.instance_id == instance_id) {
                return false;
            }
            let evicted = if entries.len() >= self.capacity {
                entries.pop_front()
            } else {
                None
            };
            entries.push_back(Pending {
                instance_id: instance_id.to_string(),
                channel: channel.to_string(),
                registered_at: Instant::now(),
                ack: Box::new(ack),
                nack: Box::new(nack),
            });
            evicted
        };
        tracing::debug!(
            code = codes::ACK_DEFERRED_REGISTERED,
            channel = %channel,
            instance = %instance_id,
            "deferred ack registered — broker settle held until the instance's terminal event"
        );
        if let Some(mut evicted) = evicted {
            tracing::warn!(
                code = codes::ACK_DEFERRED_OVERFLOW,
                channel = %evicted.channel,
                instance = %evicted.instance_id,
                capacity = self.capacity,
                "deferred-ack registry at capacity — oldest entry evicted with a nack \
                 (raise sutra.ack.deferred.capacity or investigate never-terminating instances)"
            );
            fire_swallowing(&mut evicted.nack);
        }
        true
    }

    /// `INSTANCE_COMPLETED` — fires the ack exactly once and removes the entry. A missing
    /// registration (orphan terminal event) is a no-op.
    pub fn on_instance_completed(&self, instance_id: &str) {
        if let Some(mut entry) = self.remove(instance_id) {
            tracing::debug!(
                code = codes::ACK_DEFERRED_ACKED,
                channel = %entry.channel,
                instance = %instance_id,
                "deferred ack fired — instance completed"
            );
            fire_swallowing(&mut entry.ack);
        }
    }

    /// `INSTANCE_FAILED` — fires the nack exactly once and removes the entry. A missing
    /// registration (orphan terminal event) is a no-op.
    pub fn on_instance_failed(&self, instance_id: &str) {
        if let Some(mut entry) = self.remove(instance_id) {
            tracing::info!(
                code = codes::ACK_DEFERRED_NACKED,
                channel = %entry.channel,
                instance = %instance_id,
                "deferred nack fired — instance failed (permanent reject, DLQ posture)"
            );
            fire_swallowing(&mut entry.nack);
        }
    }

    /// Sweep timed-out entries — each gets a nack (`SUTRA.ACK.DEFERRED_TIMEOUT`; the
    /// broker slot frees and inbox dedup absorbs any redelivery). Returns the number of
    /// entries nacked.
    pub fn sweep_timeouts(&self) -> usize {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        {
            let mut entries = self.entries.lock().expect("deferred-ack registry");
            let mut kept = VecDeque::with_capacity(entries.len());
            while let Some(p) = entries.pop_front() {
                if now.duration_since(p.registered_at) >= self.timeout {
                    timed_out.push(p);
                } else {
                    kept.push_back(p);
                }
            }
            *entries = kept;
        }
        let count = timed_out.len();
        for mut p in timed_out {
            tracing::warn!(
                code = codes::ACK_DEFERRED_TIMEOUT,
                channel = %p.channel,
                instance = %p.instance_id,
                timeout = ?self.timeout,
                "deferred ack timed out — nacked (instance still runs; raise \
                 sutra.ack.deferred.timeout for long-running processes)"
            );
            fire_swallowing(&mut p.nack);
        }
        count
    }

    /// Withdraw a registration WITHOUT firing either callback — the inverse of
    /// [`Self::register`] for a park whose commit FAILED. Registration now happens
    /// before `commit_park` (the park/terminal ordering fix: were it registered after,
    /// another shard could claim, resume and complete the instance inside the
    /// commit-to-registration window and the terminal event would find nothing). A failed
    /// commit therefore takes the registration back out; the dispatch surfaces `Err` and
    /// the transport applies its own redelivery disposition, exactly as before. Returns
    /// whether an entry was removed.
    pub fn withdraw(&self, instance_id: &str) -> bool {
        self.remove(instance_id).is_some()
    }

    /// Snapshot count of pending deferred acks (diagnostic-only).
    pub fn pending_count(&self) -> usize {
        self.entries.lock().expect("deferred-ack registry").len()
    }

    fn remove(&self, instance_id: &str) -> Option<Pending> {
        let mut entries = self.entries.lock().expect("deferred-ack registry");
        let idx = entries.iter().position(|p| p.instance_id == instance_id)?;
        entries.remove(idx)
    }
}

/// The wiring: the registry observes the engine's execution-listener bus and fires
/// the matching callback exactly once when the instance reaches a terminal state.
/// Register via `TokenExecutor::builder(...).with_listener(registry)` (the engine
/// assembly wraps its shared `Arc` in [`DeferredAckListener`] for the `Rc` bus).
impl sutra_executor::listener::ExecutionListener for DeferredAckRegistry {
    fn on_instance_completed(&self, event: &sutra_executor::listener::InstanceEvent) {
        DeferredAckRegistry::on_instance_completed(self, &event.instance_id);
    }

    fn on_instance_failed(
        &self,
        event: &sutra_executor::listener::InstanceEvent,
        _diagnostic: &sutra_bpmn::SutraError,
    ) {
        DeferredAckRegistry::on_instance_failed(self, &event.instance_id);
    }
}

/// `Rc`-bus adapter: the engine shares ONE registry (`Arc` — actor thread + sweep task +
/// activation flips) while the executor's listener fan-out is `Rc<dyn ExecutionListener>`;
/// this newtype lets the same instance ride both.
pub struct DeferredAckListener(std::sync::Arc<DeferredAckRegistry>);

impl DeferredAckListener {
    pub fn new(registry: std::sync::Arc<DeferredAckRegistry>) -> DeferredAckListener {
        DeferredAckListener(registry)
    }
}

impl sutra_executor::listener::ExecutionListener for DeferredAckListener {
    fn on_instance_completed(&self, event: &sutra_executor::listener::InstanceEvent) {
        DeferredAckRegistry::on_instance_completed(&self.0, &event.instance_id);
    }

    fn on_instance_failed(
        &self,
        event: &sutra_executor::listener::InstanceEvent,
        _diagnostic: &sutra_bpmn::SutraError,
    ) {
        DeferredAckRegistry::on_instance_failed(&self.0, &event.instance_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::source::AckDecision;

    use super::DeferredAckRegistry;

    const HOUR: Duration = Duration::from_secs(3600);

    /// Shared fire-count + a callback bumping it. The registry owns `FnMut() + Send`
    /// callbacks (Mutex interior), so the recorder is an atomic.
    fn counter() -> (Arc<AtomicU32>, impl FnMut() + Send + 'static) {
        let count: Arc<AtomicU32> = Arc::default();
        let inner = Arc::clone(&count);
        (count, move || {
            inner.fetch_add(1, Ordering::SeqCst);
        })
    }

    /// A callback appending `label` to the shared log — for firing-order assertions.
    fn labeler(
        log: &Arc<Mutex<Vec<&'static str>>>,
        label: &'static str,
    ) -> impl FnMut() + Send + 'static {
        let log = Arc::clone(log);
        move || log.lock().expect("label log").push(label)
    }

    #[test]
    fn completed_fires_the_ack_exactly_once_and_removes_the_entry() {
        let registry = DeferredAckRegistry::new(8, HOUR);
        let (acks, ack) = counter();
        let (nacks, nack) = counter();
        assert!(registry.register("i-1", "ch", ack, nack));
        assert_eq!(registry.pending_count(), 1);

        registry.on_instance_completed("i-1");
        registry.on_instance_completed("i-1"); // entry already consumed — no double ack

        assert_eq!(
            (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
            (1, 0)
        );
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn failed_fires_the_nack_exactly_once_mapping_to_nack_drop() {
        // The transport-side seam contract (`source.rs`): the ack callback executes
        // `AckDecision::Ack`; an instance FAILED under `on-complete` is a permanent
        // reject, so the nack callback executes `AckDecision::NackDrop` (DLQ posture).
        // Record the decision a broker source would execute.
        let registry = DeferredAckRegistry::new(8, HOUR);
        let executed: Arc<Mutex<Vec<AckDecision>>> = Arc::default();
        let ack_log = Arc::clone(&executed);
        let nack_log = Arc::clone(&executed);
        assert!(registry.register(
            "i-fail",
            "ch",
            move || ack_log.lock().expect("log").push(AckDecision::Ack),
            move || nack_log.lock().expect("log").push(AckDecision::NackDrop),
        ));

        registry.on_instance_failed("i-fail");
        registry.on_instance_failed("i-fail"); // second terminal event: entry gone, no-op

        assert_eq!(*executed.lock().expect("log"), vec![AckDecision::NackDrop]);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn terminal_events_only_consume_their_own_instance() {
        let registry = DeferredAckRegistry::new(8, HOUR);
        let (a_acks, a_ack) = counter();
        let (b_acks, b_ack) = counter();
        registry.register("a", "ch", a_ack, || {});
        registry.register("b", "ch", b_ack, || {});

        registry.on_instance_completed("b");
        assert_eq!(
            (a_acks.load(Ordering::SeqCst), b_acks.load(Ordering::SeqCst)),
            (0, 1)
        );
        assert_eq!(registry.pending_count(), 1);

        registry.on_instance_completed("a");
        assert_eq!(a_acks.load(Ordering::SeqCst), 1);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn a_terminal_event_for_an_unknown_instance_is_a_no_op() {
        let registry = DeferredAckRegistry::new(8, HOUR);
        registry.on_instance_completed("never-registered"); // empty registry: no panic
        registry.on_instance_failed("never-registered");
        assert_eq!(registry.pending_count(), 0);

        // With an unrelated entry present: nothing fires, nothing is removed.
        let (acks, ack) = counter();
        let (nacks, nack) = counter();
        registry.register("present", "ch", ack, nack);
        registry.on_instance_completed("absent");
        registry.on_instance_failed("absent");
        assert_eq!(
            (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
            (0, 0)
        );
        assert_eq!(registry.pending_count(), 1);
    }

    #[test]
    fn duplicate_registration_is_rejected_and_the_first_callbacks_win() {
        let registry = DeferredAckRegistry::new(8, HOUR);
        let (first_acks, first_ack) = counter();
        let (second_acks, second_ack) = counter();
        let (second_nacks, second_nack) = counter();
        assert!(registry.register("dup", "ch", first_ack, || {}));
        assert!(!registry.register("dup", "ch", second_ack, second_nack));
        assert_eq!(registry.pending_count(), 1); // no second entry

        registry.on_instance_completed("dup");
        assert_eq!(first_acks.load(Ordering::SeqCst), 1); // the first registration's callback fires
        assert_eq!(
            (
                second_acks.load(Ordering::SeqCst),
                second_nacks.load(Ordering::SeqCst)
            ),
            (0, 0)
        ); // dropped

        // Once the entry is consumed the id is registrable again (broker redelivery).
        assert!(registry.register("dup", "ch", || {}, || {}));
        assert_eq!(registry.pending_count(), 1);
    }

    #[test]
    fn a_duplicate_registration_at_capacity_does_not_evict() {
        // The duplicate check runs BEFORE the eviction check — a rejected duplicate
        // must not nack an innocent oldest entry.
        let registry = DeferredAckRegistry::new(2, HOUR);
        let (oldest_nacks, oldest_nack) = counter();
        registry.register("oldest", "ch", || {}, oldest_nack);
        registry.register("newest", "ch", || {}, || {});
        assert!(!registry.register("newest", "ch", || {}, || {}));
        assert_eq!(oldest_nacks.load(Ordering::SeqCst), 0);
        assert_eq!(registry.pending_count(), 2);
    }

    #[test]
    fn the_size_cap_evicts_oldest_registrations_first_with_redelivery_nacks() {
        // Eviction order is oldest-registration-first (queue front) — insertion order,
        // NOT access recency; each eviction fires the evicted entry's nack (the broker
        // redelivers) and the registry never exceeds its capacity.
        let registry = DeferredAckRegistry::new(2, HOUR);
        let evicted: Arc<Mutex<Vec<&'static str>>> = Arc::default();
        for id in ["a", "b", "c", "d"] {
            assert!(registry.register(id, "ch", || {}, labeler(&evicted, id)));
            assert!(registry.pending_count() <= 2); // the bound holds throughout
        }
        assert_eq!(*evicted.lock().expect("log"), vec!["a", "b"]); // oldest first, in order
        assert_eq!(registry.pending_count(), 2);

        // The survivors are the two youngest and still settle normally.
        registry.on_instance_completed("c");
        registry.on_instance_completed("d");
        assert_eq!(registry.pending_count(), 0);
        assert_eq!(*evicted.lock().expect("log"), vec!["a", "b"]); // no further nacks
    }

    #[test]
    fn zero_capacity_is_clamped_to_a_bound_of_one() {
        // `new(0, ..)` must still hold one entry (`capacity.max(1)`), not nack on sight.
        let registry = DeferredAckRegistry::new(0, HOUR);
        let (first_nacks, first_nack) = counter();
        assert!(registry.register("first", "ch", || {}, first_nack));
        assert_eq!(registry.pending_count(), 1);
        assert_eq!(first_nacks.load(Ordering::SeqCst), 0); // admitted, not evicted

        assert!(registry.register("second", "ch", || {}, || {}));
        assert_eq!(first_nacks.load(Ordering::SeqCst), 1); // evicted at the bound of one
        assert_eq!(registry.pending_count(), 1);
    }

    #[test]
    fn sweep_nacks_only_entries_past_the_timeout() {
        let registry = DeferredAckRegistry::new(8, Duration::from_millis(150));
        let (old_nacks, old_nack) = counter();
        let (young_acks, young_ack) = counter();
        let (young_nacks, young_nack) = counter();
        registry.register("old", "ch", || {}, old_nack);
        std::thread::sleep(Duration::from_millis(200)); // age "old" past the timeout
        registry.register("young", "ch", young_ack, young_nack);

        assert_eq!(registry.sweep_timeouts(), 1); // only "old" has aged out
        assert_eq!(old_nacks.load(Ordering::SeqCst), 1);
        assert_eq!(
            (
                young_acks.load(Ordering::SeqCst),
                young_nacks.load(Ordering::SeqCst)
            ),
            (0, 0)
        );
        assert_eq!(registry.pending_count(), 1);

        // A terminal event for the swept entry is now a no-op; the survivor still acks.
        registry.on_instance_failed("old");
        assert_eq!(old_nacks.load(Ordering::SeqCst), 1);
        registry.on_instance_completed("young");
        assert_eq!(young_acks.load(Ordering::SeqCst), 1);
        assert_eq!(registry.sweep_timeouts(), 0);
    }

    #[test]
    fn a_zero_timeout_expires_entries_immediately() {
        // Boundary pin: expiry is `elapsed >= timeout`, so `Duration::ZERO` sweeps all.
        let registry = DeferredAckRegistry::new(8, Duration::ZERO);
        let (nacks, nack) = counter();
        registry.register("i", "ch", || {}, nack);
        assert_eq!(registry.sweep_timeouts(), 1);
        assert_eq!(nacks.load(Ordering::SeqCst), 1);
        assert_eq!(registry.sweep_timeouts(), 0); // nothing left to sweep
    }

    #[test]
    fn a_panicking_ack_callback_is_swallowed_and_the_registry_stays_usable() {
        // The "swallow callback exception" contract: a misbehaving broker callback must
        // never unwind through the executor listener bus. (The "thread panicked" line
        // this prints on stderr is expected and harmless.)
        let registry = DeferredAckRegistry::new(8, HOUR);
        registry.register("boom", "ch", || panic!("ack blew up"), || {});
        registry.on_instance_completed("boom"); // must not propagate
        assert_eq!(registry.pending_count(), 0); // the entry is still consumed

        // The registry remains fully usable afterwards.
        let (acks, ack) = counter();
        assert!(registry.register("after", "ch", ack, || {}));
        registry.on_instance_completed("after");
        assert_eq!(acks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_panicking_nack_during_sweep_does_not_stop_the_sweep() {
        let registry = DeferredAckRegistry::new(8, Duration::ZERO);
        let (nacks, nack) = counter();
        registry.register("boom", "ch", || {}, || panic!("nack blew up"));
        registry.register("fine", "ch", || {}, nack);
        assert_eq!(registry.sweep_timeouts(), 2); // both counted, the panic swallowed
        assert_eq!(nacks.load(Ordering::SeqCst), 1); // the second nack still fired
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn an_evicted_nack_may_reenter_the_registry() {
        // Regression: the eviction nack used to fire while the registry's internal
        // borrow was still held, so a nack that re-entered the registry (pending_count,
        // re-register for redelivery) hit a re-entrancy panic that was silently
        // swallowed — violating the documented "invoked outside any registry borrow"
        // re-entrancy contract. With the Mutex interior the same defect would DEADLOCK
        // instead, so this test still pins the contract: the nack must be able to
        // re-enter and its work must actually happen.
        let registry = Arc::new(DeferredAckRegistry::new(1, HOUR));
        let seen_pending: Arc<Mutex<Option<usize>>> = Arc::default();
        let redelivered_acks: Arc<AtomicU32> = Arc::default();
        let (second_nacks, second_nack) = counter();

        let first_nack = {
            let registry = Arc::clone(&registry);
            let seen_pending = Arc::clone(&seen_pending);
            let redelivered_acks = Arc::clone(&redelivered_acks);
            move || {
                // Both calls re-enter the registry from inside an eviction nack.
                *seen_pending.lock().expect("seen") = Some(registry.pending_count());
                let acks = Arc::clone(&redelivered_acks);
                assert!(registry.register(
                    "first-redelivery",
                    "ch",
                    move || {
                        acks.fetch_add(1, Ordering::SeqCst);
                    },
                    || {},
                ));
            }
        };

        assert!(registry.register("first", "ch", || {}, first_nack));
        assert!(registry.register("second", "ch", || {}, second_nack)); // evicts "first"

        assert_eq!(*seen_pending.lock().expect("seen"), Some(1)); // re-entrant view: "second" is in
        assert_eq!(second_nacks.load(Ordering::SeqCst), 1); // the re-entrant register evicted it
        assert_eq!(registry.pending_count(), 1);

        registry.on_instance_completed("first-redelivery");
        assert_eq!(redelivered_acks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn an_interleaved_register_settle_sweep_smoke_loses_no_callbacks() {
        // Dense single-threaded interleaving: 300 instances registered while earlier
        // ones complete/fail, with periodic no-op sweeps. Every callback fires exactly
        // once; none are lost or doubled. (Cross-thread settlement is pinned separately
        // below — the registry is `Send + Sync` by construction now.)
        let registry = DeferredAckRegistry::new(512, HOUR);
        let acks: Arc<AtomicU32> = Arc::default();
        let nacks: Arc<AtomicU32> = Arc::default();
        for i in 0..300u32 {
            let a = Arc::clone(&acks);
            let n = Arc::clone(&nacks);
            assert!(registry.register(
                &format!("i-{i}"),
                "ch",
                move || {
                    a.fetch_add(1, Ordering::SeqCst);
                },
                move || {
                    n.fetch_add(1, Ordering::SeqCst);
                },
            ));
            // Interleave: settle the instance registered 50 iterations back.
            if i >= 50 {
                let settled = i - 50;
                if settled % 3 == 0 {
                    registry.on_instance_failed(&format!("i-{settled}"));
                } else {
                    registry.on_instance_completed(&format!("i-{settled}"));
                }
            }
            if i % 64 == 0 {
                assert_eq!(registry.sweep_timeouts(), 0); // nothing has aged out
            }
        }
        // Drain the tail (the last 50 are still pending).
        for i in 250..300u32 {
            registry.on_instance_completed(&format!("i-{i}"));
        }
        // 0..250 settled in-loop, every 3rd failed; the rest and the tail completed.
        let failed = (0..250u32).filter(|i| i % 3 == 0).count() as u32;
        assert_eq!(nacks.load(Ordering::SeqCst), failed);
        assert_eq!(acks.load(Ordering::SeqCst), 300 - failed);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn cross_thread_register_and_settle_loses_no_callbacks() {
        // The Send+Sync interior's own smoke: 4 registering threads × 100 instances race
        // a settling thread; every callback fires exactly once (the engine shape —
        // register on the actor thread, sweep on a tokio task — is a 2-thread subset).
        let registry = Arc::new(DeferredAckRegistry::new(4096, HOUR));
        let acks: Arc<AtomicU32> = Arc::default();
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let registry = Arc::clone(&registry);
                let acks = Arc::clone(&acks);
                std::thread::spawn(move || {
                    for i in 0..100u32 {
                        let a = Arc::clone(&acks);
                        assert!(registry.register(
                            &format!("t{t}-i{i}"),
                            "ch",
                            move || {
                                a.fetch_add(1, Ordering::SeqCst);
                            },
                            || {},
                        ));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("register thread");
        }
        let settler = {
            let registry = Arc::clone(&registry);
            std::thread::spawn(move || {
                for t in 0..4 {
                    for i in 0..100u32 {
                        registry.on_instance_completed(&format!("t{t}-i{i}"));
                    }
                }
            })
        };
        settler.join().expect("settle thread");
        assert_eq!(acks.load(Ordering::SeqCst), 400);
        assert_eq!(registry.pending_count(), 0);
    }
}
