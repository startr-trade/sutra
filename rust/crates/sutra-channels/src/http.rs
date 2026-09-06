//! The HTTP transport — the trigger source, request processor, route registrar, and
//! API-key auth handler on axum.
//!
//! - Per-channel `bind: "<METHOD> <path>"` routes plus the catch-all
//!   `POST /channels/{channel}` (the default path scheme).
//! - Every HTTP channel MUST declare an auth scheme (`SUTRA.CHANNEL.AUTH.MISSING_SCHEME`);
//!   the engine supports `apikey` (constant-time compare; `X-API-Key` or the configured header,
//!   with the `Authorization: ApiKey <key>` fallback).
//! - `ack-mode: on-complete` (the HTTP default) keeps the synchronous request/reply
//!   contract — the flow's `<q:reply mode="native">` rides the connection back with the
//!   reply's content type; `on-persist` answers `202 Accepted` carrying the flow's receipt if it
//!   produced one (respond-and-continue), and an empty body if it did not.
//! - Failures render RFC 7807 `application/problem+json` with the status mapping
//!   (auth → 401, `SUTRA.INBOUND.REJECTED.*` → 400, `SUTRA.RESOLVE.*` → 404, else 500).
//!
//! The engine (`Rc`-based, deliberately single-threaded) runs on a dedicated actor
//! thread as ONE async task on that thread's own current-thread runtime (execution
//! scale-out §3(a)); axum handlers talk to it through a channel — one delivery awaited to
//! completion at a time, which is also what the examples' singleton/serial channel
//! contracts declare.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::shard_metrics::{ShardLaneMetrics, ShardRouterMetrics};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use sha2::{Digest, Sha256};

use crate::codes;
use crate::config::ChannelDefinition;
use crate::diag::Diagnostic;
use crate::dispatch::{ChannelEngine, DispatchOutcome, InboundMessage, TimerFireOutcome};

// ---- engine actor -------------------------------------------------------------------------

enum EngineRequest {
    Dispatch {
        message: InboundMessage,
        respond: tokio::sync::oneshot::Sender<Result<DispatchOutcome, Diagnostic>>,
    },
    /// An `ack-mode: on-complete` broker delivery: the settle callbacks ride
    /// the request onto the actor thread, where the park arm registers them on the
    /// deferred-ack registry BEFORE the park commits (withdrawn on a failed commit —
    /// "transport told Deferred ⇒ registered ∧ committed").
    DispatchDeferred {
        message: InboundMessage,
        settle: crate::source::DeferredSettle,
        respond:
            tokio::sync::oneshot::Sender<Result<crate::dispatch::DeferredDispatch, Diagnostic>>,
    },
    /// A claimed due timer driven through the resume path.
    FireTimer {
        fire: sutra_executor::TimerFire,
        respond: tokio::sync::oneshot::Sender<Result<TimerFireOutcome, Diagnostic>>,
    },
    /// The outbox dispatcher terminally POISONED a channel-call request delivery: the wake
    /// prompt that offers the failure to the parked task's `<q:retry>` policy. Best-effort —
    /// the engine re-derives everything from durable facts under the instance claim (a wake
    /// without the poisoned row is a no-op), and a LOST wake is recovered by the call's
    /// `<q:timeout>` boundary, which the loader guarantees exists.
    FailChannelCall {
        fire: crate::dispatch::ChannelCallPoisonFire,
        respond: tokio::sync::oneshot::Sender<
            Result<crate::dispatch::ChannelCallPoisonOutcome, Diagnostic>,
        >,
    },
    /// A claimed due timer-START schedule, minting a fresh instance. Rides the SAME actor queue
    /// as inbound dispatches and activation swaps, so a schedule can never fire against a
    /// half-flipped registry.
    StartScheduled {
        fire: crate::dispatch::ScheduledStartFire,
        respond: tokio::sync::oneshot::Sender<
            Result<crate::dispatch::ScheduledStartOutcome, Diagnostic>,
        >,
    },
    /// Apply a prepared mutation ON the actor thread (the two-phase activation swap). The
    /// actor serialises requests, so the swap is atomic with respect to dispatches: every
    /// dispatch that entered before this request completes on the pre-swap engine; every
    /// later one sees the post-swap engine. Nothing is ever observed half-flipped.
    Update {
        apply: Box<dyn FnOnce(&mut ChannelEngine) + Send>,
        respond: tokio::sync::oneshot::Sender<Result<(), Diagnostic>>,
    },
    /// A relay handoff's already-resolved resume, enqueued by the ROUTER side of
    /// [`EngineHandle`] on the instance's OWNER shard after the arrival shard answered
    /// [`DispatchOutcome::Handoff`]. Only router-side caller tasks send this — shard loops
    /// never send into other shards' queues. Never taken at `shard-count = 1` (the
    /// default): the resolution shard is always the owner shard, so no dispatch ever
    /// answers `Handoff`.
    ResumeResolved {
        resolved: Box<crate::dispatch::ResolvedResume>,
        respond: tokio::sync::oneshot::Sender<Result<DispatchOutcome, Diagnostic>>,
    },
}

/// A shard's identity inside the engine's shard router, handed to the per-shard engine
/// build so shard-scoped salts (the claim owner, intake ids) and the relay handoff
/// decision know their lane. At the default `shard-count = 1` this is always
/// `{ index: 0, count: 1 }` and every id is owned by the one lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineShard {
    /// This lane's index, `0..count`.
    pub index: u32,
    /// The router's total lane count (`sutra.engine.shards`).
    pub count: u32,
}

impl EngineShard {
    /// The single-lane identity (`shard-count = 1`, the default).
    pub fn single() -> EngineShard {
        EngineShard { index: 0, count: 1 }
    }

    /// True when `instance_id` hashes onto this shard's lane.
    pub fn owns(&self, instance_id: &str) -> bool {
        shard_index_of(instance_id, self.count) == self.index
    }
}

/// The stable instance→shard routing hash: FNV-1a 64 over the id's bytes, mod `count`.
/// Routing is an AFFINITY optimization, never the correctness mechanism — the shard-scoped
/// claim owner makes a mis-route degrade to a visible `CLAIM_HELD` bounce, not to
/// interleaved execution. At `count = 1` every id maps to shard 0.
pub fn shard_index_of(instance_id: &str, count: u32) -> u32 {
    if count <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in instance_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(count)) as u32
}

/// One shard's mailbox sender. Unbounded is the default (parity with the historic
/// unbounded queue); `sutra.engine.shard-queue-capacity` opts into a bound, in which case
/// a full mailbox makes `send` AWAIT on the caller's async task — backpressure propagates
/// to the transport (HTTP request in flight, broker prefetch window, poller tick), never
/// to another shard loop.
#[derive(Clone)]
struct ShardSender {
    queue: ShardQueueSender,
    /// The lane's counters: `send` moves the queue-depth gauge (+1 at a successful
    /// enqueue; the lane's actor loop takes the −1 at dequeue).
    metrics: Arc<ShardLaneMetrics>,
}

#[derive(Clone)]
enum ShardQueueSender {
    Unbounded(tokio::sync::mpsc::UnboundedSender<EngineRequest>),
    Bounded(tokio::sync::mpsc::Sender<EngineRequest>),
}

impl ShardSender {
    /// TRUE once the lane's actor task has dropped its receiver — the lane is dead and
    /// every send into it answers [`engine_gone`].
    fn is_dead(&self) -> bool {
        match &self.queue {
            ShardQueueSender::Unbounded(tx) => tx.is_closed(),
            ShardQueueSender::Bounded(tx) => tx.is_closed(),
        }
    }

    async fn send(&self, request: EngineRequest) -> Result<(), Diagnostic> {
        // Depth moves BEFORE the enqueue (rolled back on failure), so the actor's
        // dequeue-side decrement can never observe the gauge below zero.
        self.metrics.enqueued();
        let sent = match &self.queue {
            ShardQueueSender::Unbounded(tx) => tx.send(request).map_err(|_| engine_gone()),
            ShardQueueSender::Bounded(tx) => tx.send(request).await.map_err(|_| engine_gone()),
        };
        if sent.is_err() {
            self.metrics.dequeued();
        }
        sent
    }
}

/// The receiving half of a shard mailbox, drained by that shard's single actor task.
enum ShardReceiver {
    Unbounded(tokio::sync::mpsc::UnboundedReceiver<EngineRequest>),
    Bounded(tokio::sync::mpsc::Receiver<EngineRequest>),
}

impl ShardReceiver {
    /// Async dequeue, awaited by the lane's actor loop between requests (execution
    /// scale-out §3(a): the loop is `while let Some(req) = rx.recv().await { handle(req).await }`);
    /// `None` once every handle is dropped — the actor's exit condition.
    async fn recv(&mut self) -> Option<EngineRequest> {
        match self {
            ShardReceiver::Unbounded(rx) => rx.recv().await,
            ShardReceiver::Bounded(rx) => rx.recv().await,
        }
    }
}

fn shard_queue(
    capacity: Option<usize>,
    metrics: Arc<ShardLaneMetrics>,
) -> (ShardSender, ShardReceiver) {
    let (queue, rx) = match capacity {
        None => {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (
                ShardQueueSender::Unbounded(tx),
                ShardReceiver::Unbounded(rx),
            )
        }
        Some(capacity) => {
            let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
            (ShardQueueSender::Bounded(tx), ShardReceiver::Bounded(rx))
        }
    };
    (ShardSender { queue, metrics }, rx)
}

/// Cloneable, Send+Sync handle to the engine's shard router: one mailbox per shard
/// (exactly one at the default `shard-count = 1`), each drained by its own actor thread.
/// Instance-addressed work (timer fires, resolved-resume handoffs) routes by the stable
/// instance-id hash; keyless work (inbound deliveries whose instance is not yet known,
/// scheduled starts) takes the round-robin arrival shard.
#[derive(Clone)]
pub struct EngineHandle {
    /// One mailbox per shard; the vector index IS the shard index.
    shards: Arc<[ShardSender]>,
    /// Round-robin arrival-shard counter for keyless work. Always shard 0 at
    /// `shard-count = 1`.
    arrival: Arc<AtomicUsize>,
    /// The router's per-shard counters (queue depth, dispatch/park/resume, handoffs,
    /// claim bounces — execution scale-out §6.1). Router-lifetime: it survives
    /// activation flips, so the exporter reads one stable registry.
    metrics: Arc<ShardRouterMetrics>,
}

impl EngineHandle {
    fn arrival_shard(&self) -> &ShardSender {
        let count = self.shards.len();
        let index = if count == 1 {
            0
        } else {
            self.arrival.fetch_add(1, Ordering::Relaxed) % count
        };
        &self.shards[index]
    }

    fn shard_for_instance(&self, instance_id: &str) -> &ShardSender {
        &self.shards[shard_index_of(instance_id, self.shards.len() as u32) as usize]
    }

    /// The router's lane count (`sutra.engine.shards`) — the bound the timer poller's
    /// per-tick fire concurrency uses (S lanes can absorb S concurrent fires).
    pub fn shard_count(&self) -> u32 {
        self.shards.len() as u32
    }

    /// The router's per-shard observability registry (execution scale-out §6.1) —
    /// index-aligned with the shard indexes; the engine's telemetry module exports it as
    /// the `sutra.engine.shard.*` instruments.
    pub fn shard_metrics(&self) -> Arc<ShardRouterMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The indexes of DEAD lanes — lanes whose actor task is gone (receiver dropped, e.g.
    /// a panic outside the per-dispatch containment). Instance-addressed work hashed to a
    /// dead lane can never run again in this process, so the health surface folds this
    /// into LIVENESS: a replica with a dead lane must be restarted, not routed around —
    /// the lane's key space has no other home here. Empty = healthy.
    pub fn dead_lanes(&self) -> Vec<u32> {
        self.shards
            .iter()
            .enumerate()
            .filter(|(_, shard)| shard.is_dead())
            .map(|(index, _)| index as u32)
            .collect()
    }

    /// Dispatch one delivery through the engine: resolution runs on the arrival shard; a
    /// relay whose correlated instance lives on another shard's lane comes back as
    /// [`DispatchOutcome::Handoff`] and is re-enqueued HERE, on the caller's async task,
    /// onto the owner shard (shard loops never send into other shards' queues). Transports
    /// therefore never observe `Handoff`.
    pub async fn dispatch(&self, message: InboundMessage) -> Result<DispatchOutcome, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.arrival_shard()
            .send(EngineRequest::Dispatch { message, respond })
            .await?;
        match rx.await.map_err(|_| engine_gone())?? {
            DispatchOutcome::Handoff { resolved, .. } => self.resume_resolved(*resolved).await,
            outcome => Ok(outcome),
        }
    }

    /// Dispatch one `ack-mode: on-complete` broker delivery, handing the engine
    /// the per-delivery settle callbacks. Answers [`crate::dispatch::DeferredDispatch`]:
    /// `Deferred` = the instance parked and the callbacks are registered (do NOT settle);
    /// `Settled(outcome)` = settle now, exactly like [`Self::dispatch`]. A relay handoff
    /// (never a park — only a spawn's park arm consumes the callbacks) is resolved on the
    /// owner shard before this returns, exactly like [`Self::dispatch`].
    pub async fn dispatch_deferred(
        &self,
        message: InboundMessage,
        settle: crate::source::DeferredSettle,
    ) -> Result<crate::dispatch::DeferredDispatch, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.arrival_shard()
            .send(EngineRequest::DispatchDeferred {
                message,
                settle,
                respond,
            })
            .await?;
        match rx.await.map_err(|_| engine_gone())?? {
            crate::dispatch::DeferredDispatch::Settled(DispatchOutcome::Handoff {
                resolved,
                ..
            }) => Ok(crate::dispatch::DeferredDispatch::Settled(
                self.resume_resolved(*resolved).await?,
            )),
            outcome => Ok(outcome),
        }
    }

    /// Drive one claimed due timer through the engine (the poller's entry point) —
    /// routed by `fire.instance_id` onto the owner shard (arrival shape 1: the key is
    /// already known, no hop ever).
    pub async fn fire_timer(
        &self,
        fire: sutra_executor::TimerFire,
    ) -> Result<TimerFireOutcome, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.shard_for_instance(&fire.instance_id)
            .send(EngineRequest::FireTimer { fire, respond })
            .await?;
        rx.await.map_err(|_| engine_gone())?
    }

    /// Offer a terminally-poisoned channel-call delivery to the parked task's `<q:retry>`
    /// policy (the outbox dispatcher's wake prompt) — routed by `fire.instance_id` onto the
    /// owner shard exactly like [`Self::fire_timer`].
    pub async fn fail_channel_call(
        &self,
        fire: crate::dispatch::ChannelCallPoisonFire,
    ) -> Result<crate::dispatch::ChannelCallPoisonOutcome, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.shard_for_instance(&fire.instance_id)
            .send(EngineRequest::FailChannelCall { fire, respond })
            .await?;
        rx.await.map_err(|_| engine_gone())?
    }

    /// Mint an instance from one claimed due timer-start schedule (the schedule poller's
    /// entry point — the spawn counterpart of [`Self::fire_timer`]'s resume). The instance
    /// id does not exist yet, so any shard may run it: round-robin arrival, never a hop.
    pub async fn start_scheduled(
        &self,
        fire: crate::dispatch::ScheduledStartFire,
    ) -> Result<crate::dispatch::ScheduledStartOutcome, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.arrival_shard()
            .send(EngineRequest::StartScheduled { fire, respond })
            .await?;
        rx.await.map_err(|_| engine_gone())?
    }

    /// Execute a relay handoff's resolved resume on the instance's owner shard and await
    /// that shard's oneshot. The owner shard re-runs only the race-safe tail (claim → load
    /// → guards → pin → resume) and can never answer `Handoff` again — the at-most-one-hop
    /// rule holds by construction. DEAD at `shard-count = 1` (nothing produces a handoff).
    pub async fn resume_resolved(
        &self,
        resolved: crate::dispatch::ResolvedResume,
    ) -> Result<DispatchOutcome, Diagnostic> {
        let (respond, rx) = tokio::sync::oneshot::channel();
        self.shard_for_instance(&resolved.instance_id)
            .send(EngineRequest::ResumeResolved {
                resolved: Box::new(resolved),
                respond,
            })
            .await?;
        rx.await.map_err(|_| engine_gone())?
    }

    /// Apply a prepared mutation on EVERY shard's actor thread — the control-plane
    /// fan-out of the activation swap (execution scale-out §5.1). `lane_apply` is called
    /// once per lane, ON THIS caller task, to mint that lane's closure (a whole-engine
    /// rebuild clones the shared plan set per lane); each closure then runs BETWEEN that
    /// lane's dispatches — never during one — so per-lane the swap stays atomic: traffic
    /// in flight finishes on the old engine, later intake sees the new one, and nothing
    /// is ever observed half-flipped. An instance's steps all run on its one shard, whose
    /// flip is a single point in its queue — so cross-shard skew during the window is
    /// indistinguishable from two dispatches ordered around a single swap point.
    ///
    /// AWAIT-ALL BARRIER: this returns only after every lane has applied (or failed) its
    /// swap — the caller replaces the live-deployment set and rewires transports strictly
    /// after that, so those later stages never race a lane that still serves the old
    /// engine. A panic inside a lane's closure is caught (the actor survives) and
    /// surfaces here as the first error; every other lane is still awaited first.
    ///
    /// At `shard-count = 1` this is exactly the historic single-swap: one closure, one
    /// `Update`, one await.
    pub async fn update(
        &self,
        mut lane_apply: impl FnMut() -> Box<dyn FnOnce(&mut ChannelEngine) + Send>,
    ) -> Result<(), Diagnostic> {
        // Enqueue on ALL lanes first (lanes flip concurrently), then await all. A send
        // failure (engine gone — shutdown) is recorded, never an early return, so no
        // lane's swap is left in flight unobserved behind an error path.
        let mut pending = Vec::with_capacity(self.shards.len());
        let mut first_error: Option<Diagnostic> = None;
        for shard in self.shards.iter() {
            let (respond, rx) = tokio::sync::oneshot::channel();
            match shard
                .send(EngineRequest::Update {
                    apply: lane_apply(),
                    respond,
                })
                .await
            {
                Ok(()) => pending.push(rx),
                Err(d) => {
                    first_error.get_or_insert(d);
                }
            }
        }
        for rx in pending {
            match rx.await.map_err(|_| engine_gone()) {
                Ok(Ok(())) => {}
                Ok(Err(d)) | Err(d) => {
                    first_error.get_or_insert(d);
                }
            }
        }
        match first_error {
            None => Ok(()),
            Some(d) => Err(d),
        }
    }
}

fn engine_gone() -> Diagnostic {
    Diagnostic::error(codes::RUNTIME_UNEXPECTED, "engine actor is not running")
}

/// Drive a request future to completion, converting a panic in any poll into a structured
/// diagnostic — a panic anywhere in a dispatch must not kill the actor (every later
/// request would see "engine actor is not running"). The async form of the old
/// closure-based wrapper: same guarantee (the request is contained, the lane survives),
/// same diagnostic shape.
async fn catch_actor_panic<T>(
    f: impl std::future::Future<Output = Result<T, Diagnostic>>,
) -> Result<T, Diagnostic> {
    crate::dispatch::catch_unwind_completion(f)
        .await
        .unwrap_or_else(|panic| {
            let detail = panic
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("(non-string panic payload)");
            Err(Diagnostic::error(
                codes::RUNTIME_UNEXPECTED,
                format!("dispatch panicked: {detail}"),
            ))
        })
}

/// Spawn the engine actor: `build` runs ON the actor thread (the engine is `Rc`-based
/// and never crosses threads). The actor drains requests until every handle is dropped.
/// `runtime` is the process runtime whose I/O driver the lane's awaits register with
/// (see [`spawn_engine_sharded`]). The single-lane form of [`spawn_engine_sharded`] —
/// same one actor thread, same unbounded queue, same drain order.
pub fn spawn_engine<F>(runtime: tokio::runtime::Handle, build: F) -> EngineHandle
where
    F: FnOnce() -> ChannelEngine + Send + 'static,
{
    // One lane ⇒ the build runs exactly once; the `Mutex<Option<…>>` adapter turns the
    // caller's `FnOnce` into the router's per-lane-cloneable `Fn` bound without asking
    // callers for `Clone` state.
    let build = Arc::new(std::sync::Mutex::new(Some(build)));
    spawn_engine_sharded(1, None, runtime, move |_shard, _metrics| {
        let build = build
            .lock()
            .expect("single-lane build slot")
            .take()
            .expect("the single-lane build runs exactly once");
        build()
    })
}

/// Spawn the engine's shard router: `shard_count` actor lanes (each an [`EngineShard`]-
/// identified dedicated thread driving ONE async task), `queue_capacity` the opt-in
/// PER-LANE mailbox bound (`None` = unbounded, the parity default). `runtime` is the
/// PROCESS runtime (the one serving transports): each lane's loop is driven on its own
/// thread via `Handle::block_on`, so every I/O resource a lane's awaits create — pool
/// connections above all — registers with that always-alive shared driver, exactly
/// where the pre-Phase-3 `block_on` seams put them. Lanes deliberately do NOT own
/// reactors: a per-lane runtime dies with its lane, and a pooled connection bound to a
/// dead lane reactor hangs whoever draws it from the shared pool next (observed as the
/// engine-restart conformance flake: the shutdown lease release drew one and the
/// successor engine waited out the full lease TTL). The caller must keep `runtime`
/// alive for as long as any handle can dispatch — the same lifetime the old
/// `block_on`-driving handle already required.
///
/// `build` runs ON each shard's actor thread — outside any runtime context, so
/// build-time seams may still `block_on` the process runtime's handle — and receives
/// the shard identity plus that lane's [`ShardLaneMetrics`] handle; it is `Fn + Clone`
/// because every lane builds its own engine from the same captured raw parts (the
/// `Send` plan set is cloned per lane — the engine itself is `Rc`-based and never
/// crosses threads).
///
/// Correctness never rides on the lane count: instance-addressed work routes by the
/// stable id hash, a mis-route degrades to a `CLAIM_HELD` bounce via the shard-scoped
/// claim owner, and shard loops never send into other shards' queues (only router-side
/// caller tasks do — the §1.1 no-inter-shard-deadlock rule).
pub fn spawn_engine_sharded<F>(
    shard_count: u32,
    queue_capacity: Option<usize>,
    runtime: tokio::runtime::Handle,
    build: F,
) -> EngineHandle
where
    F: Fn(EngineShard, Arc<ShardLaneMetrics>) -> ChannelEngine + Clone + Send + 'static,
{
    assert!(
        shard_count >= 1,
        "shard-count {shard_count} is invalid: the router needs at least one lane \
         (config validation refuses 0 before this)"
    );
    let metrics = Arc::new(ShardRouterMetrics::new(shard_count));
    let mut lanes: Vec<ShardSender> = Vec::with_capacity(shard_count as usize);
    for index in 0..shard_count {
        let shard = EngineShard {
            index,
            count: shard_count,
        };
        let lane_metrics = metrics.lane(index);
        let (tx, mut rx) = shard_queue(queue_capacity, Arc::clone(&lane_metrics));
        let build = build.clone();
        let runtime = runtime.clone();
        std::thread::Builder::new()
            .name(format!("sutra-channel-engine-{index}"))
            .spawn(move || {
                // The engine BUILD runs on the plain thread, BEFORE the runtime context is
                // entered: build-time seams may drive the process runtime's `Handle` with
                // `block_on`, which panics from inside a runtime context. (The activation
                // flip's rebuild runs inside the loop below and is therefore `block_on`-free
                // by construction — the coverage read it used to block on is hoisted to the
                // controller.)
                let mut engine = build(shard, Arc::clone(&lane_metrics));
                // Phase 3 lane shape (execution scale-out §3(a)): ONE task — this loop —
                // driven to completion on THIS thread via `Handle::block_on` over a
                // `LocalSet` (the engine state stays `Rc`/`RefCell`, `!Send`; the future
                // never leaves this thread). Every request is awaited to completion before
                // the next `recv`, so per lane the §0 ordering properties hold verbatim:
                // commit happens-before the oneshot reply happens-before the next dequeue.
                // Nothing here (or below it) may `spawn_local`/`spawn` request handling —
                // that would re-introduce intra-lane interleaving, the design's rejected
                // option (b). Lanes never run on the process runtime's worker threads (the
                // loop is polled HERE, so a lane's long execution cannot starve transport
                // I/O) — only the lane's I/O/timer REGISTRATIONS live on that shared,
                // always-alive driver; see `spawn_engine_sharded`'s doc for why lanes must
                // not own reactors.
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    while let Some(request) = rx.recv().await {
                        lane_metrics.dequeued();
                        match request {
                            EngineRequest::Dispatch { message, respond } => {
                                lane_metrics.dispatched();
                                let outcome = catch_actor_panic(engine.dispatch(&message)).await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::DispatchDeferred {
                                message,
                                settle,
                                respond,
                            } => {
                                lane_metrics.dispatched();
                                let outcome =
                                    catch_actor_panic(engine.dispatch_deferred(&message, settle))
                                        .await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::FireTimer { fire, respond } => {
                                lane_metrics.dispatched();
                                let outcome = catch_actor_panic(engine.fire_timer(&fire)).await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::StartScheduled { fire, respond } => {
                                lane_metrics.dispatched();
                                let outcome =
                                    catch_actor_panic(engine.fire_scheduled_start(&fire)).await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::FailChannelCall { fire, respond } => {
                                lane_metrics.dispatched();
                                let outcome =
                                    catch_actor_panic(engine.fail_channel_call(&fire)).await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::Update { apply, respond } => {
                                let outcome = catch_actor_panic(async {
                                    apply(&mut engine);
                                    Ok(())
                                })
                                .await;
                                let _ = respond.send(outcome);
                            }
                            EngineRequest::ResumeResolved { resolved, respond } => {
                                lane_metrics.dispatched();
                                let outcome =
                                    catch_actor_panic(engine.resume_resolved(&resolved)).await;
                                let _ = respond.send(outcome);
                            }
                        }
                    }
                }));
            })
            .expect("spawn engine actor thread");
        lanes.push(tx);
    }
    EngineHandle {
        shards: Arc::from(lanes),
        arrival: Arc::new(AtomicUsize::new(0)),
        metrics,
    }
}

// ---- channel routing ------------------------------------------------------------------------

/// Auth configuration resolved at server build (the `defaultAuthHandlerFor` moment).
#[derive(Debug, Clone)]
enum AuthConfig {
    ApiKey {
        expected: String,
        header: String,
    },
    /// Inbound bearer: one or more ref-resolved static tokens, presented as
    /// `Authorization: Bearer <token>` and constant-time-compared.
    Bearer {
        tokens: Vec<String>,
    },
}

/// One servable HTTP channel — plain Send data derived from its [`ChannelDefinition`].
#[derive(Debug, Clone)]
struct HttpChannel {
    channel_name: String,
    module_key: String,
    tenant: String,
    ack_mode: String,
    auth: AuthConfig,
    idempotency_key_header: Option<String>,
    /// Inbound CloudEvents mode (`cloudevents.mode`, default `auto`).
    ce_mode: crate::cloudevents::CeMode,
    /// Wrap-mode `source` default (`ce.source` property).
    ce_source_default: Option<String>,
    /// Wrap-mode `type` default (`ce.type` property).
    ce_type_default: Option<String>,
}

/// The swappable `(METHOD, path)` → channel table the HTTP transport serves from — the
/// route side of the binding pointer flip. Handlers read the CURRENT snapshot per
/// request; [`ChannelRouteTable::swap`] replaces the whole snapshot atomically (a request
/// resolves entirely against one snapshot, never a mix).
/// A published route snapshot: `(METHOD, path)` → channel.
type RouteSnapshot = Arc<HashMap<(String, String), HttpChannel>>;

#[derive(Clone, Default)]
pub struct ChannelRouteTable {
    routes: Arc<RwLock<RouteSnapshot>>,
}

impl ChannelRouteTable {
    pub fn new() -> ChannelRouteTable {
        ChannelRouteTable::default()
    }

    /// Atomically replace the served route set (built by [`http_routes_of`]).
    pub fn swap(&self, routes: HttpRouteSet) {
        *self.routes.write().expect("route table lock") = Arc::new(routes.0);
    }

    fn current(&self) -> RouteSnapshot {
        Arc::clone(&self.routes.read().expect("route table lock"))
    }
}

/// A validated, servable HTTP route set (opaque — build via [`http_routes_of`]).
pub struct HttpRouteSet(HashMap<(String, String), HttpChannel>);

/// Resolve every `transport: http` channel of `definitions` to its served
/// `(METHOD, path)` route with a literal auth-value resolver (no ref indirection) — the
/// pure form used by the channel-side [`channel_router`] and unit tests.
pub fn http_routes_of(definitions: &[ChannelDefinition]) -> Result<HttpRouteSet, Diagnostic> {
    http_routes_of_resolved(definitions, &|value| Ok(value.to_string()))
}

/// [`http_routes_of`] with an injected auth-value resolver — the engine passes the envref
/// registry so `apikey.value` / `bearer.token` may be `env:`/`secret:`/`vault:` refs
/// (inbound bearer tokens are static-token-via-ref). Fail-closed on missing/unsupported auth
/// schemes, unresolvable refs, and route collisions (the checks the boot path has always run).
pub fn http_routes_of_resolved(
    definitions: &[ChannelDefinition],
    resolve: &dyn Fn(&str) -> Result<String, Diagnostic>,
) -> Result<HttpRouteSet, Diagnostic> {
    let mut by_route: HashMap<(String, String), HttpChannel> = HashMap::new();
    for def in definitions {
        if def.transport.as_deref() != Some("http") {
            continue;
        }
        if def.is_outbound() {
            // `direction: outbound` — a <q:send> target, not a served route; the outbox
            // dispatcher resolves it by destination scheme.
            continue;
        }
        let scheme = def.auth_scheme.as_deref().ok_or_else(|| {
            Diagnostic::error(
                codes::CHANNEL_AUTH_MISSING_SCHEME,
                format!(
                    "Channel '{}' has no auth scheme declared. HTTP channels MUST declare \
                     one of apikey, bearer.",
                    def.binding.channel_name
                ),
            )
        })?;
        let auth = match scheme {
            "apikey" => AuthConfig::ApiKey {
                expected: match def.properties.get("apikey.value") {
                    Some(raw) => resolve(raw)?,
                    None => String::new(),
                },
                header: def
                    .properties
                    .get("apikey.header")
                    .cloned()
                    .unwrap_or_else(|| "X-API-Key".to_string()),
            },
            "bearer" => AuthConfig::Bearer {
                tokens: resolve_bearer_tokens(def, resolve)?,
            },
            "jwt" => {
                // jwt REMAINS fail-closed at boot (a deliberate decision, with no timeline).
                return Err(Diagnostic::error(
                    codes::CHANNEL_AUTH_SCHEME_INVALID,
                    format!(
                        "Channel '{}' declares auth scheme 'jwt'; jwt is unsupported; use \
                         bearer or apikey.",
                        def.binding.channel_name
                    ),
                ));
            }
            other => {
                return Err(Diagnostic::error(
                    codes::CHANNEL_AUTH_SCHEME_INVALID,
                    format!(
                        "Channel '{}' declares auth scheme '{other}'; the HTTP transport \
                         supports: apikey, bearer.",
                        def.binding.channel_name
                    ),
                ));
            }
        };
        let (method, path) = def.bind_method_and_path();
        let channel = HttpChannel {
            channel_name: def.binding.channel_name.clone(),
            module_key: def.binding.namespace.module_key(),
            tenant: def.binding.namespace.tenant.clone(),
            ack_mode: def.effective_ack_mode().to_string(),
            auth,
            idempotency_key_header: def.idempotency_key_header.clone(),
            ce_mode: crate::cloudevents::CeMode::parse(def.cloud_events_mode.as_deref()),
            ce_source_default: def.properties.get("ce.source").cloned(),
            ce_type_default: def.properties.get("ce.type").cloned(),
        };
        if by_route
            .insert((method.clone(), path.clone()), channel)
            .is_some()
        {
            return Err(Diagnostic::error(
                codes::CHANNEL_NAME_COLLISION,
                format!(
                    "HTTP route '{method} {path}' is already bound; refusing to bind a \
                     second channel ('{}') to it.",
                    def.binding.channel_name
                ),
            ));
        }
    }
    Ok(HttpRouteSet(by_route))
}

struct AppState {
    handle: EngineHandle,
    /// The live route table — swapped whole on a deployment activation flip.
    routes: ChannelRouteTable,
}

/// Build the axum [`Router`] serving every `transport: http` channel of `definitions` —
/// the static (boot-once) form of [`channel_router_dynamic`].
pub fn channel_router(
    definitions: &[ChannelDefinition],
    handle: EngineHandle,
) -> Result<Router, Diagnostic> {
    let table = ChannelRouteTable::new();
    table.swap(http_routes_of(definitions)?);
    Ok(channel_router_dynamic(&table, handle))
}

/// Build the axum [`Router`] serving whatever `table` currently holds. Routes resolve by
/// exact `(METHOD, path)` lookup per request (channel binds are literal paths — including
/// the `/channels/<name>` default scheme), so activating/retiring deployments is a table
/// swap with no router rebuild; unknown routes render the 404 problem shape.
pub fn channel_router_dynamic(table: &ChannelRouteTable, handle: EngineHandle) -> Router {
    let state = Arc::new(AppState {
        handle,
        routes: table.clone(),
    });
    Router::new()
        .fallback(
            |state: State<Arc<AppState>>,
             method: axum::http::Method,
             uri: axum::http::Uri,
             headers: HeaderMap,
             body: Bytes| async move {
                let route_key = (method.as_str().to_uppercase(), uri.path().to_string());
                serve_route(state, route_key, headers, body).await
            },
        )
        .with_state(state)
}

async fn serve_route(
    State(state): State<Arc<AppState>>,
    route_key: (String, String),
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let routes = state.routes.current();
    // Read once, up front: every failure below renders its RFC 7807 problem document in the
    // format the caller speaks (design R4), so the content-type must be in hand before the first
    // possible rejection — including the 404 and the 401.
    let request_content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let problem_ct = request_content_type.as_deref();
    let Some(channel) = routes.get(&route_key) else {
        return problem_response(
            404,
            &Diagnostic::error(
                codes::RESOLVE_CHANNEL_UNKNOWN,
                format!("No channel bound at '{} {}'", route_key.0, route_key.1),
            ),
            problem_ct,
        );
    };

    // 1. Authenticate (constant-time compare; never before reading config).
    if let Err(diagnostic) = authenticate(channel, &headers) {
        return problem_response(401, &diagnostic, problem_ct);
    }

    // 2. Extract the inbound CloudEvent per `cloudevents.mode`. A failed extraction
    //    (missing required attribute, malformed envelope) is a 400 reject.
    let copied_headers = copy_headers(&headers);
    let extraction = match crate::cloudevents::extract(
        channel.ce_mode,
        &channel.channel_name,
        &copied_headers,
        request_content_type.as_deref(),
        &body,
        crate::cloudevents::WrapDefaults {
            source: channel.ce_source_default.as_deref(),
            event_type: channel.ce_type_default.as_deref(),
        },
    ) {
        Ok(x) => x,
        Err(diagnostic) => {
            return problem_response(status_for_code(&diagnostic.code), &diagnostic, problem_ct)
        }
    };
    // The effective body/content-type ride from the extraction (binary/native keep the
    // request body; structured lifts the envelope's data + inner content type).
    let content_type = extraction.content_type.clone();

    // 3. Resolve the idempotency key: explicit header → CE id (explicit) → sha256(body).
    let (idempotency_key, explicit) =
        resolve_idempotency_key(channel, &headers, extraction.explicit_id.as_deref(), &body);

    // 4. Build the InboundMessage — tenant comes from the channel binding, never the client.
    let message = InboundMessage {
        tenant: channel.tenant.clone(),
        module_key: channel.module_key.clone(),
        channel: channel.channel_name.clone(),
        headers: copied_headers,
        body: extraction.body,
        content_type: content_type.clone(),
        idempotency_key,
        explicit_event_id: explicit,
        received_at: now_rfc3339(),
        cloud_event: extraction.cloud_event.map(Box::new),
    };

    // 5. Dispatch + 6. ack per ack-mode.
    let async_ack = channel.ack_mode.eq_ignore_ascii_case("on-persist");
    match state.handle.dispatch(message).await {
        Ok(DispatchOutcome::Duplicate) => StatusCode::ACCEPTED.into_response(),
        Ok(DispatchOutcome::Completed { reply, outputs, .. }) => {
            if async_ack {
                // on-persist ⇒ asynchronous channel: the answer does not wait for the work to
                // finish. That settles WHEN we respond, not WHETHER the flow had something to
                // say. A flow that ran `<q:reply continue="true">` produced its receipt
                // deliberately, before parking, precisely so a long-running load could hand the
                // caller a batch id and then detach — so that receipt IS the 202's body. A 202
                // carrying a representation of the accepted work is exactly what the status is
                // for. Only a flow that replied nothing gets the bare 202.
                //
                // It used to be dropped unconditionally, which made the two declarations
                // silently exclusive: the asynchronous ack mode a long load wants and the
                // receipt it wants to send could not coexist, and nothing said so — the caller
                // just got an empty body.
                return match reply {
                    Some(r) => {
                        response_with(StatusCode::ACCEPTED, &r.content_type, r.body.into_inner())
                    }
                    None => StatusCode::ACCEPTED.into_response(),
                };
            }
            render_ok(reply, outputs, content_type.as_deref())
        }
        // A non-idempotent process failed and was dead-lettered (consumed + recorded as a
        // durable incident). HTTP is synchronous with no engine-managed redelivery, so surface the
        // failure to the caller as a problem+json carrying the at-most-once incident code.
        Ok(DispatchOutcome::DeadLettered { code, detail, .. }) => {
            let diagnostic = Diagnostic::error(&code, detail);
            problem_response(status_for_code(&diagnostic.code), &diagnostic, problem_ct)
        }
        // Unreachable: `EngineHandle::dispatch` consumes every handoff on the router side.
        Ok(DispatchOutcome::Handoff { .. }) => problem_response(
            500,
            &Diagnostic::error(
                codes::RUNTIME_UNEXPECTED,
                "internal: a shard handoff escaped the engine router",
            ),
            problem_ct,
        ),
        Err(diagnostic) => {
            problem_response(status_for_code(&diagnostic.code), &diagnostic, problem_ct)
        }
    }
}

/// Render the sync response — the `HttpRequestProcessor.renderOk` contract: the flow's
/// reply verbatim; else the outputs map as JSON; empty outputs → 200 empty with the
/// symmetric (inbound) content type.
fn render_ok(
    reply: Option<crate::dispatch::SyncReply>,
    outputs: serde_json::Value,
    inbound_content_type: Option<&str>,
) -> Response {
    let symmetric = match inbound_content_type {
        Some(ct) if !ct.trim().is_empty() => ct.to_string(),
        _ => "application/octet-stream".to_string(),
    };
    if let Some(reply) = reply {
        return response_with(StatusCode::OK, &reply.content_type, reply.body.into_inner());
    }
    match &outputs {
        serde_json::Value::Object(map) if !map.is_empty() => {
            let body = serde_json::to_vec(&outputs).unwrap_or_default();
            response_with(StatusCode::OK, "application/json", body)
        }
        _ => response_with(StatusCode::OK, &symmetric, Vec::new()),
    }
}

fn response_with(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response {
    (
        status,
        [(axum::http::header::CONTENT_TYPE, content_type.to_string())],
        body,
    )
        .into_response()
}

// ---- auth ------------------------------------------------------------------------------------

/// Resolve the `bearer.token` property into one or more expected tokens. The property is a
/// comma-separated list of refs (`env:`/`secret:`/`vault:` or a literal) — "static token(s)
/// via ref". Fail-closed when no non-empty token is configured.
fn resolve_bearer_tokens(
    def: &ChannelDefinition,
    resolve: &dyn Fn(&str) -> Result<String, Diagnostic>,
) -> Result<Vec<String>, Diagnostic> {
    let raw = def.properties.get("bearer.token").map(String::as_str);
    let mut tokens = Vec::new();
    if let Some(raw) = raw {
        for reference in raw.split(',').map(str::trim).filter(|r| !r.is_empty()) {
            let resolved = resolve(reference)?;
            if !resolved.trim().is_empty() {
                tokens.push(resolved.trim().to_string());
            }
        }
    }
    if tokens.is_empty() {
        return Err(Diagnostic::error(
            codes::CHANNEL_AUTH_SCHEME_INVALID,
            format!(
                "Channel '{}' declares auth scheme 'bearer' but no non-empty \
                 'bearer.token' is configured.",
                def.binding.channel_name
            ),
        ));
    }
    Ok(tokens)
}

/// The inbound auth check: `apikey` (the configured header, default `X-API-Key`, or
/// `Authorization: ApiKey <key>`) or `bearer` (`Authorization: Bearer <token>`), both
/// constant-time byte-wise (the shared [`crate::auth::constant_time_equals`]).
fn authenticate(channel: &HttpChannel, headers: &HeaderMap) -> Result<(), Diagnostic> {
    match &channel.auth {
        AuthConfig::ApiKey { expected, header } => {
            if expected.trim().is_empty() {
                return Err(Diagnostic::error(
                    codes::INBOUND_REJECTED_AUTH,
                    format!(
                        "Channel '{}' has authScheme=apikey but no 'apikey.value' configured.",
                        channel.channel_name
                    ),
                ));
            }
            let Some(presented) = extract_presented_key(headers, header) else {
                return Err(Diagnostic::error(
                    codes::INBOUND_REJECTED_AUTH,
                    format!(
                        "Channel '{}' requires an API key in '{header}' or 'Authorization: \
                         ApiKey <key>'.",
                        channel.channel_name
                    ),
                ));
            };
            if !crate::auth::constant_time_equals(expected.as_bytes(), presented.as_bytes()) {
                return Err(Diagnostic::error(
                    codes::INBOUND_REJECTED_AUTH,
                    format!("Channel '{}' rejected API key.", channel.channel_name),
                ));
            }
            Ok(())
        }
        AuthConfig::Bearer { tokens } => {
            let Some(presented) = extract_bearer_token(headers) else {
                return Err(Diagnostic::error(
                    codes::INBOUND_REJECTED_AUTH,
                    format!(
                        "Channel '{}' requires a token in 'Authorization: Bearer <token>'.",
                        channel.channel_name
                    ),
                ));
            };
            // Constant-time compare against EVERY configured token (accept on any match) —
            // the loop never short-circuits on a mismatch.
            let mut matched = false;
            for token in tokens {
                matched |=
                    crate::auth::constant_time_equals(token.as_bytes(), presented.as_bytes());
            }
            if !matched {
                return Err(Diagnostic::error(
                    codes::INBOUND_REJECTED_AUTH,
                    format!("Channel '{}' rejected bearer token.", channel.channel_name),
                ));
            }
            Ok(())
        }
    }
}

fn extract_presented_key(headers: &HeaderMap, header_name: &str) -> Option<String> {
    if let Some(custom) = headers.get(header_name).and_then(|v| v.to_str().ok()) {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let authz = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?
        .trim();
    let prefix = "ApiKey ";
    if authz.len() > prefix.len() && authz[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return Some(authz[prefix.len()..].trim().to_string());
    }
    None
}

/// Extract the token from `Authorization: Bearer <token>` (case-insensitive scheme).
fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let authz = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?
        .trim();
    let prefix = "Bearer ";
    if authz.len() > prefix.len() && authz[..prefix.len()].eq_ignore_ascii_case(prefix) {
        let token = authz[prefix.len()..].trim();
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    None
}

// ---- idempotency key ---------------------------------------------------------------------------

/// `IdempotencyKeyExtractor` port: the channel's configured header (explicit) → the
/// extracted CloudEvent `id` (explicit) → SHA-256(body) truncated to 32 hex chars
/// (the transport fallback; NOT an explicit event id, so it never drives inbox dedup).
fn resolve_idempotency_key(
    channel: &HttpChannel,
    headers: &HeaderMap,
    ce_id: Option<&str>,
    body: &[u8],
) -> (String, bool) {
    if let Some(header) = &channel.idempotency_key_header {
        if let Some(v) = headers.get(header.as_str()).and_then(|v| v.to_str().ok()) {
            if !v.trim().is_empty() {
                return (v.trim().to_string(), true);
            }
        }
    }
    if let Some(id) = ce_id {
        if !id.trim().is_empty() {
            return (id.trim().to_string(), true);
        }
    }
    (sha256_truncated(body), false)
}

/// SHA-256 of the body, truncated to 32 hex characters (the hashing rule).
pub fn sha256_truncated(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    hex[..32.min(hex.len())].to_string()
}

// ---- problem rendering ----------------------------------------------------------------------

/// The diagnostic-code → HTTP status mapping.
/// Map a diagnostic code to its HTTP status.
///
/// The `SUTRA.INBOUND.*` prefix is NOT one status class — it mixes the caller's fault (a document
/// that fails its schema), load shedding (quota, capacity) and the deployment's own fault (a codec
/// that is not there, a handler that is ambiguous). Only the last of those is a 5xx, so the codes
/// are mapped deliberately and anything unlisted keeps the fail-safe 500: a wrong 4xx tells a
/// caller not to retry something that would have succeeded, which is worse than an honest 500.
fn status_for_code(code: &str) -> u16 {
    match code {
        codes::INBOUND_REJECTED_AUTH => return 401,
        // The caller's document failed its contract — `reject` at intake, or an unhandled
        // `error` posture. Their file, their fix; a retry of the same bytes cannot succeed.
        codes::INBOUND_VALIDATION_REJECT | codes::INBOUND_VALIDATION_ERROR => return 400,
        codes::INBOUND_PAYLOAD_TOO_LARGE => return 413,
        // A correlation alias this message would bind is already held by another instance.
        codes::INBOUND_ALIAS_CONFLICT_REJECT => return 409,
        // Load shedding, not failure: the request was well formed and may succeed later.
        codes::INBOUND_QUOTA_EXCEEDED_RATE | codes::INBOUND_QUOTA_EXCEEDED_CONCURRENT => {
            return 429
        }
        codes::INBOUND_CHANNEL_AT_CAPACITY => return 503,
        _ => {}
    }
    if code.starts_with("SUTRA.INBOUND.REJECTED.") {
        return 400;
    }
    if code.starts_with("SUTRA.RESOLVE.") {
        return 404;
    }
    500
}

/// The RFC 7807 problem document, rendered in the format the CALLER speaks.
///
/// The document's *model* is unchanged and remains the contract — `type`, `title`, `status`,
/// `detail`, `code`, `attributes`. What varies is the syntax it is serialised in, chosen from the
/// inbound content-type exactly as a reply is: a client
/// that posted `text/csv` should not get JSON back mid-conversation, and for a batch of per-cell
/// violations a table is the more usable answer — the sender can diff it against the file they
/// posted. `application/problem+json` stays the default and the fallback for any inbound whose
/// format cannot render one.
fn problem_response(status: u16, diagnostic: &Diagnostic, inbound: Option<&str>) -> Response {
    let (content_type, bytes) = render_problem(status, diagnostic, inbound);
    response_with(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &content_type,
        bytes,
    )
}

/// The problem document's fields, as an ordered name→text map — the one model every rendering
/// serialises. `attributes` are flattened in (a csv row and an xml element have nowhere to nest a
/// sub-object, and the flattened form loses nothing: the keys are already unique).
fn problem_fields(status: u16, diagnostic: &Diagnostic) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "type".to_string(),
            format!("urn:bpm:diag:{}", diagnostic.code),
        ),
        ("title".to_string(), diagnostic.code.clone()),
        ("status".to_string(), status.to_string()),
        ("detail".to_string(), diagnostic.message.clone()),
        ("code".to_string(), diagnostic.code.clone()),
    ];
    for (k, v) in &diagnostic.attributes {
        fields.push((k.clone(), v.clone()));
    }
    fields
}

/// Serialise the problem document in the caller's format, returning `(content-type, bytes)`.
fn render_problem(
    status: u16,
    diagnostic: &Diagnostic,
    inbound: Option<&str>,
) -> (String, Vec<u8>) {
    let fields = problem_fields(status, diagnostic);
    let is = |patterns: &[&str]| {
        let patterns: Vec<String> = patterns.iter().map(|s| s.to_string()).collect();
        inbound
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .is_some_and(|ct| crate::content_type::accepts(&patterns, Some(ct)))
    };

    if is(&["text/csv", "application/csv"]) {
        // One row per field: a `name,value` table. For a batch rejection the caller gets the
        // issue list in the same shape as the file they sent.
        let rows: Vec<serde_json::Value> = fields
            .iter()
            .map(|(k, v)| serde_json::json!({ "field": k.clone(), "value": v.clone() }))
            .collect();
        // Reached through the SPI's built-in format registry, not by depending on the concrete
        // format crate: `sutra-channels` resolves formats by name through the pull registry, and
        // that layering is deliberate. A binary that did not link the csv format simply falls
        // through to the JSON rendering below — the fail-safe, not a failure.
        if let Some(csv) = sutra_codec_spi::builtin_formats()
            .into_iter()
            .find(|f| f.name == "csv")
        {
            if let Ok(bytes) = csv.codec.encode(
                &sutra_codec_spi::CodecValue::Json(serde_json::Value::Array(rows)),
                Some("text/csv"),
            ) {
                return ("text/csv".to_string(), bytes);
            }
        }
    } else if is(&["application/xml", "text/xml", "application/*+xml"]) {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><problem>");
        for (k, v) in &fields {
            xml.push_str(&format!("<{k}>{}</{k}>", xml_escape(v)));
        }
        xml.push_str("</problem>");
        return ("application/problem+xml".to_string(), xml.into_bytes());
    } else if is(&["application/yaml", "application/x-yaml", "text/yaml"]) {
        let mut yaml = String::new();
        for (k, v) in &fields {
            yaml.push_str(&format!("{k}: {}\n", yaml_scalar(v)));
        }
        return ("application/problem+yaml".to_string(), yaml.into_bytes());
    }
    (
        "application/problem+json".to_string(),
        problem_json(status, diagnostic),
    )
}

/// Minimal XML text escaping for the problem rendering (the values are diagnostic text, never
/// markup, so the five predefined entities are the whole job).
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// A YAML scalar: always double-quoted, so a value carrying `:`/`#`/a newline stays one scalar.
fn yaml_scalar(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// RFC 7807 `application/problem+json` — the small, stable shape, and the default rendering.
fn problem_json(status: u16, diagnostic: &Diagnostic) -> Vec<u8> {
    let mut body = serde_json::Map::new();
    body.insert(
        "type".to_string(),
        serde_json::Value::String(format!("urn:bpm:diag:{}", diagnostic.code)),
    );
    body.insert(
        "title".to_string(),
        serde_json::Value::String(diagnostic.code.clone()),
    );
    body.insert("status".to_string(), serde_json::Value::from(status));
    body.insert(
        "detail".to_string(),
        serde_json::Value::String(diagnostic.message.clone()),
    );
    body.insert(
        "code".to_string(),
        serde_json::Value::String(diagnostic.code.clone()),
    );
    if !diagnostic.attributes.is_empty() {
        body.insert(
            "attributes".to_string(),
            serde_json::Value::Object(
                diagnostic
                    .attributes
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect(),
            ),
        );
    }
    serde_json::to_vec(&serde_json::Value::Object(body)).unwrap_or_default()
}

fn copy_headers(headers: &HeaderMap) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for (name, value) in headers {
        if let Ok(v) = value.to_str() {
            out.entry(name.as_str().to_string())
                .and_modify(|existing: &mut String| {
                    existing.push(',');
                    existing.push_str(v);
                })
                .or_insert_with(|| v.to_string());
        }
    }
    out
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod local_channel_tests {

    // ---- R4: the problem document is rendered in the caller's format ------------------------

    fn diag() -> Diagnostic {
        Diagnostic::error(codes::INBOUND_VALIDATION_REJECT, "row 3 is malformed")
            .with_attribute("issueCount", "1")
    }

    #[test]
    fn problem_defaults_to_json_and_stays_rfc_7807() {
        for inbound in [
            None,
            Some("application/json"),
            Some("application/vnd.x+json"),
        ] {
            let (ct, bytes) = render_problem(400, &diag(), inbound);
            assert_eq!(ct, "application/problem+json", "inbound {inbound:?}");
            let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            assert_eq!(v["status"], 400);
            assert_eq!(v["code"], codes::INBOUND_VALIDATION_REJECT);
            assert_eq!(v["detail"], "row 3 is malformed");
            // The MODEL is unchanged: attributes stay nested on the JSON rendering.
            assert_eq!(v["attributes"]["issueCount"], "1");
        }
    }

    /// Regression: every intake reject fell through to 500 unless its code carried the literal
    /// `REJECTED.` segment. `SUTRA.INBOUND.VALIDATION_REJECT` does not, so a CSV batch refused for
    /// three bad cells answered `500 Internal Server Error` — the engine claiming its own fault
    /// for the caller's malformed file, and telling every well-behaved client that the identical
    /// bytes were worth retrying.
    #[test]
    fn a_caller_fault_is_a_4xx_and_only_an_engine_fault_is_a_500() {
        assert_eq!(status_for_code(codes::INBOUND_VALIDATION_REJECT), 400);
        assert_eq!(status_for_code(codes::INBOUND_VALIDATION_ERROR), 400);
        assert_eq!(status_for_code(codes::INBOUND_REJECTED_AUTH), 401);
        assert_eq!(status_for_code(codes::INBOUND_ALIAS_CONFLICT_REJECT), 409);
        assert_eq!(status_for_code(codes::INBOUND_PAYLOAD_TOO_LARGE), 413);
        assert_eq!(status_for_code(codes::INBOUND_QUOTA_EXCEEDED_RATE), 429);
        assert_eq!(status_for_code(codes::INBOUND_CHANNEL_AT_CAPACITY), 503);
        // The deployment's own fault stays a 500 — the caller can do nothing about either.
        assert_eq!(status_for_code(codes::INBOUND_CODEC_NOT_FOUND), 500);
        assert_eq!(status_for_code(codes::INBOUND_AMBIGUOUS_HANDLER), 500);
        assert_eq!(status_for_code("SUTRA.SOMETHING.UNMAPPED"), 500);
    }

    #[test]
    fn a_csv_caller_gets_the_problem_as_a_table() {
        let (ct, bytes) = render_problem(400, &diag(), Some("text/csv; charset=utf-8"));
        assert_eq!(ct, "text/csv");
        let text = String::from_utf8(bytes).expect("utf-8");
        // A `field,value` table: one row per problem field, diffable against what was posted.
        assert!(text.starts_with("field,value\n"), "got: {text}");
        assert!(text.contains("detail,row 3 is malformed"), "got: {text}");
        assert!(text.contains(&format!("code,{}", codes::INBOUND_VALIDATION_REJECT)));
        // Attributes flatten in — a csv row has nowhere to nest a sub-object.
        assert!(text.contains("issueCount,1"), "got: {text}");
    }

    #[test]
    fn an_xml_caller_gets_problem_xml_with_escaped_text() {
        let diagnostic = Diagnostic::error(codes::INBOUND_VALIDATION_REJECT, "a < b & c");
        let (ct, bytes) = render_problem(400, &diagnostic, Some("application/xml"));
        assert_eq!(ct, "application/problem+xml");
        let text = String::from_utf8(bytes).expect("utf-8");
        assert!(text.contains("<problem>"), "got: {text}");
        assert!(text.contains("<status>400</status>"), "got: {text}");
        assert!(
            text.contains("<detail>a &lt; b &amp; c</detail>"),
            "diagnostic text must be escaped, not injected as markup: {text}"
        );
    }

    #[test]
    fn a_yaml_caller_gets_problem_yaml_with_quoted_scalars() {
        let (ct, bytes) = render_problem(400, &diag(), Some("application/yaml"));
        assert_eq!(ct, "application/problem+yaml");
        let text = String::from_utf8(bytes).expect("utf-8");
        assert!(text.contains("status: \"400\""), "got: {text}");
        // Quoted, so a value carrying ':' or '#' cannot break the document.
        assert!(
            text.contains("detail: \"row 3 is malformed\""),
            "got: {text}"
        );
    }

    #[test]
    fn an_unrenderable_inbound_format_falls_back_to_problem_json() {
        // raw bytes / an unknown media type: there is no sensible rendering, so the default holds.
        let (ct, _) = render_problem(500, &diag(), Some("application/octet-stream"));
        assert_eq!(ct, "application/problem+json");
    }
    use super::*;
    use crate::config::{ChannelBinding, ChannelDefinition, Namespace};
    use sutra_executor::DeploymentId;

    fn channel(name: &str, transport: &str, auth_scheme: Option<&str>) -> ChannelDefinition {
        ChannelDefinition {
            binding: ChannelBinding::new(
                name,
                Namespace::new("acme", "pay", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some(transport.to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: auth_scheme.map(str::to_string),
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn a_local_inbound_channel_mounts_no_http_route_and_bypasses_the_auth_mandate() {
        // A `transport: local` inbound channel declares NO auth scheme; the HTTP transport
        // skips it entirely (it is not `transport: http`), so no route is mounted (no
        // external listener) AND the HTTP auth mandate never fires.
        let local = channel("demoflow-in", "local", None);
        let routes =
            http_routes_of(&[local]).expect("transport: local bypasses the HTTP auth mandate");
        assert!(
            routes.0.is_empty(),
            "no HTTP route is mounted for a transport: local channel"
        );

        // Contrast: an HTTP inbound channel with no auth scheme fails the auth mandate closed.
        let http_no_auth = channel("sender-in", "http", None);
        assert!(http_routes_of(&[http_no_auth]).is_err());
    }
}
