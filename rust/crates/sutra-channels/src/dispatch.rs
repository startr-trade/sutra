//! The engine dispatcher (every runtime lookup keys on the opaque [`DeploymentId`]),
//! running the intake pipeline in its NORMATIVE order for each transport delivery.
//!
//! Sync-eligible activations run to completion on the connection. With an
//! [`InstanceBridge`] wired, a wait-state process takes the inbound→stateful path
//! (`execute_stateful` → park → step-commit) and an inbound on a wait node's
//! relay channel resumes the correlated waiting instance (the relay-resolution,
//! relay-correlation, and instance-resume semantics). Without a bridge, a
//! wait-state process fails fast with `SUTRA.INBOUND.PERSISTENCE_REQUIRED` (the
//! posture when no `InstanceStore` is wired).

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::model::CHANNEL_CALL_PREFIX;
use sutra_bpmn::qbindings::{AliasBinding, AliasConflict};
use sutra_bpmn::{DeclaredVariable, Node, ProcessDefinition, ProcessModule};
use sutra_codec_spi::{CodecValue, DecodeResult};
use sutra_executor::telemetry;
use sutra_executor::variables::feel_to_json;
use sutra_executor::{
    CoverageCorrelation, DeploymentId, ExecResult, StatefulExecResult, TimerFire, TimerWait,
    TokenExecutor, Variables,
};
use sutra_feel::FeelValue;

use crate::ack::DeferredAckRegistry;
use crate::bridge::{
    auth_ref_to_json, cloud_event_to_json, new_uuid, AliasRecord, InstanceBridge,
    InstanceClaimOutcome, OutboxEmission, SuspendedInstance, TimerWaitRecord,
};
use crate::capacity::{
    CallSelector, CapacityPoolStore, SlotAcquisition, SlotDirection, SlotRequest,
    CAPACITY_CALL_HEADER, CAPACITY_CALL_KEY_HEADER, CAPACITY_POOL_HEADER,
};
use crate::codes;
use crate::config::{ChannelBinding, ChannelDefinition, Namespace};
use crate::diag::Diagnostic;
use crate::inline::{InlineCaller, InlineCallerRegistry, InlineFailure, InlineResponse};
use crate::inline_chain::{
    InlineCall, InlineCallResult, InlineContinuation, InlineHandoff, InlineRetry, InlineStep,
};
use crate::intake::{InboundChain, IntakeOutcome};
use crate::policy::{
    ConcurrencyStore, FeatureProvider, PayloadCapPolicy, QuotaCheckResult, TenantQuotaEnforcer,
};
use crate::registry::{ChannelRegistry, DrainingSink, HandlerMatch, ProcessModuleRegistry};
use crate::source::DeferredSettle;
use crate::stores::{
    AliasStore, CollectingOutbox, InboundIncident, InboxStore, IncidentSink, OutboxSink,
};

/// A [`Diagnostic`] attribute the dispatcher stamps on an execution-failure diagnostic to
/// tell the transport ack-decision mapper (`sutra-transport-spi::EngineIntake`) the failure is
/// RETRY-SAFE (the target process asserted `<q:process idempotent="true">`) and should map to
/// `NackRequeue` rather than `NackDrop`. Absent ⇒ the failure is a permanent reject (NackDrop).
pub const ACK_DISPOSITION_ATTR: &str = "sutra.ackDisposition";
/// The [`ACK_DISPOSITION_ATTR`] value requesting a broker requeue (redelivery) — the retry-safe
/// posture of an idempotent process.
pub const ACK_DISPOSITION_REQUEUE: &str = "requeue";

/// A [`Diagnostic`] attribute the relay path stamps on a refusal that means "the target exists,
/// but is not parked at the wait yet": no live instance carries the wait's correlation alias
/// (the instance has not reached the wait, so its alias is not written), the instance is parked
/// at a different node, or it is not suspended at all. Nothing was executed and no inbox record
/// was written, so the same delivery can be offered again. A transport that cannot tell a race
/// from a stale message keeps its own disposition; the in-process `local` sink, whose sender is
/// a co-deployed process that may still be between steps, holds and retries it (bounded) instead
/// of poisoning it.
pub const WAIT_PENDING_ATTR: &str = "sutra.waitPending";

/// One transport delivery, as handed to the dispatcher. The transport stamps the
/// channel's namespace (`module_key`) and tenant — never the client.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub tenant: String,
    /// The `"<tenant>/<module>/<version>"` namespace key of the serving channel.
    pub module_key: String,
    pub channel: String,
    pub headers: BTreeMap<String, String>,
    /// The raw inbound payload — wrapped [`Sensitive`] so a stray `{:?}` on an
    /// [`InboundMessage`] masks it (a compile-time backstop; `Deref`-transparent for reads,
    /// `into_inner()` at a boundary that legitimately needs the owned bytes).
    pub body: sutra_executor::Sensitive<Vec<u8>>,
    pub content_type: Option<String>,
    /// Transport-resolved dedup key (header → ce.id → sha256(body) precedence).
    pub idempotency_key: String,
    /// True when the key was EXPLICITLY supplied (header / broker message id) — only
    /// explicit ids drive inbox dedup; the sha256 fallback never suppresses a re-post.
    pub explicit_event_id: bool,
    /// RFC 3339 receive stamp.
    pub received_at: String,
    /// The extracted inbound CloudEvent view (HTTP `cloudevents.mode`), projected
    /// into the intake variable `event.cloudEvent`; `None` for non-CE deliveries. Boxed so a
    /// non-CE delivery (the common case) keeps [`InboundMessage`] — and every enum that
    /// carries it — small.
    pub cloud_event: Option<Box<crate::cloudevents::CloudEvent>>,
}

/// The synchronous reply riding the inbound connection (`<q:reply mode="native">` with no
/// destination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReply {
    /// The synchronous reply payload — wrapped [`Sensitive`] (see [`InboundMessage::body`]).
    pub body: sutra_executor::Sensitive<Vec<u8>>,
    pub content_type: String,
}

/// Outcome of a dispatch — plain data (Send) so transports can cross threads with it.
#[derive(Debug, Clone)]
pub enum DispatchOutcome {
    Completed {
        instance_id: String,
        /// End-event variable snapshot as a JSON tree (the reply-rendering view).
        outputs: serde_json::Value,
        reply: Option<SyncReply>,
    },
    /// Inbox dedup hit — the delivery was already observed (first-observer-wins).
    Duplicate,
    /// A NON-idempotent process (`<q:process idempotent="false">`, the default) failed
    /// during execution. Blind redelivery-and-reprocess would duplicate side effects, so the
    /// delivery is CONSUMED (the transport acks — at-most-once, no requeue) and a durable incident
    /// is recorded. The transport maps this to `AckDecision::Ack`.
    DeadLettered {
        /// The at-most-once incident code (`SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE`).
        code: String,
        /// The failing process id.
        process_id: String,
        /// The originating failure's diagnostic code (the cause).
        cause_code: String,
        /// The originating failure's message.
        detail: String,
    },
    /// A relay whose correlated instance lives on ANOTHER shard's lane (execution
    /// scale-out §1.1). Never observed by a transport: the router side of
    /// [`crate::http::EngineHandle`] consumes it and re-enqueues the resolved resume on
    /// the owner shard before answering the caller. DEAD at the default
    /// `shard-count = 1` — the resolution shard is always the owner shard.
    Handoff {
        /// The owner shard's index (`crate::http::shard_index_of(instance_id, count)`).
        shard: u32,
        resolved: Box<ResolvedResume>,
    },
    /// A start parked ONLY at calls on synchronous outbound channels (`ack-mode:
    /// on-complete`): nothing was committed, and the in-memory instance waits on the calls the
    /// handoff carries. Like [`Self::Handoff`], never observed by a transport — the router side
    /// of [`crate::http::EngineHandle`] makes the calls, resumes the instance with each answer
    /// and returns the chain's final outcome instead. Only an engine with inline callers wired
    /// and a synchronous outbound channel declared ever answers this.
    InlineCalls(InlineHandoff),
}

/// The already-resolved resume a relay HANDOFF carries between shards (execution
/// scale-out §1.1). Channel resolution, decode, intake validation and the
/// correlation-alias FEEL evaluation ran on the ARRIVAL shard — they are deterministic
/// over the delivery and are never repeated; the owner shard re-runs only the race-safe
/// tail (claim → load → guards → pin resolution → resume). `Send` data only — enforced by
/// the compile-time assertion below, because the payload crosses shard queues.
#[derive(Debug, Clone)]
pub struct ResolvedResume {
    /// The deployment scope the alias resolved in (the channel's LIVE scope, or a
    /// DRAINING prior scope) — the claim/load key.
    pub deployment: DeploymentId,
    /// The correlated instance (fixed — a re-resolution is impossible, so at most one hop).
    pub instance_id: String,
    /// The satisfied wait node the relay targets.
    pub wait_node_id: String,
    /// The relay's merged, already-validated variables (post intake validation, pre
    /// channel-call output mapping — the mapping is applied against the PINNED process).
    pub variables: Variables,
    /// The correlation that named the instance (diagnostics; `alias_value` doubles as the
    /// coverage business key).
    pub alias_name: String,
    pub alias_value: String,
    /// The delivering channel (diagnostics on the claim-bounce path).
    pub channel: String,
    /// The arrival binding's labels (tenant/module/version), stamped on execution events.
    pub labels: BTreeMap<String, String>,
    /// The inbound `traceparent`, when one rode the delivery (coverage correlation +
    /// emission stamping).
    pub traceparent: Option<String>,
    /// The relay delivery itself, so the turn's `<q:reply>` can answer it (rendered against
    /// the relay's own binding and message, exactly like a start's reply).
    pub reply_to: Option<Box<RelayReplyContext>>,
    /// Inbox dedup keys the arrival lane recorded for this delivery, written by the owner
    /// lane's resume step in its own transaction (0.6.5).
    pub pending_dedup: Vec<crate::stores::PendingDedup>,
}

/// The delivery a relayed resume answers: its channel binding and message, against which the
/// turn's `<q:reply>` is rendered (codec, content type, contract) like a start's reply.
#[derive(Debug, Clone)]
pub struct RelayReplyContext {
    pub binding: ChannelBinding,
    pub message: InboundMessage,
}

// The handoff payload crosses shard queues: `Send` data only. `FeelValue` (inside
// `Variables`) holds `Arc`, never `Rc` — this assertion turns any regression of that
// property into a compile error rather than a runtime surprise.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<ResolvedResume>();
};

/// Outcome of [`ChannelEngine::dispatch_deferred`] — the `ack-mode: on-complete` broker
/// path. Plain data (Send) like [`DispatchOutcome`].
#[derive(Debug, Clone)]
pub enum DeferredDispatch {
    /// The instance PARKED at a wait state and the delivery's [`DeferredSettle`]
    /// callbacks were registered on the [`DeferredAckRegistry`] — the transport must not
    /// settle; the ack/nack fires at the instance's terminal event (or registry
    /// timeout/overflow). `instance_id` is the parked instance (diagnostics).
    Deferred { instance_id: String },
    /// The dispatch finished without parking (ran to a terminal state, duplicate, or
    /// dead-lettered) — the terminal listener events already fired ON this dispatch, so
    /// the transport settles NOW from the outcome, exactly like the plain path.
    Settled(DispatchOutcome),
}

/// Drive a future to completion, converting a PANIC in any of its polls into
/// `Err(payload)` — the async replacement for wrapping a synchronous body in
/// `std::panic::catch_unwind`. The future is boxed so the pin projection stays safe
/// (`#![forbid(unsafe_code)]`); the per-request allocation matches what the async-trait
/// seams already pay per call.
pub(crate) async fn catch_unwind_completion<T>(
    fut: impl std::future::Future<Output = T>,
) -> Result<T, Box<dyn std::any::Any + Send>> {
    let mut fut = Box::pin(fut);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    })
    .await
}

/// Hand a per-instance ownership claim back after a resume pass that did NOT release it
/// inside its step transaction — the async replacement for the old drop-guard's release
/// arm (the happy path still releases in-transaction via `commit_repark` /
/// `commit_complete`; this covers a rehydrate rejection, an executor failure, a rolled-back
/// commit, a caught panic, and the DRAINING-scope success whose step committed under a
/// different deployment key). Owner-scoped in the store, so firing after an in-transaction
/// release is a no-op that cannot disturb a successor that has already re-claimed.
async fn release_claim_best_effort(
    bridge: &Rc<dyn InstanceBridge>,
    deployment: &DeploymentId,
    instance_id: &str,
) {
    if let Err(d) = bridge.release_instance(deployment, instance_id).await {
        // Never escalate: the sweeper is the backstop for exactly this case.
        tracing::warn!(
            instance_id,
            deployment = deployment.value(),
            code = d.code,
            message = d.message,
            "instance claim release failed — the StuckInstanceScanner will reclaim it \
             after sutra.instance.claim-timeout"
        );
    }
}

/// Builder-assembled channel engine: registries + the inbound chain + the executor.
pub struct ChannelEngine {
    channels: ChannelRegistry,
    /// The deployed process graphs — `Arc`, built ONCE per activation and shared by every
    /// actor lane (execution scale-out §2 row 10), never one deep copy per lane.
    processes: Arc<ProcessModuleRegistry>,
    chain: InboundChain,
    executor: TokenExecutor,
    emissions: Rc<DrainingSink>,
    outbox: Rc<dyn OutboxSink>,
    inbox: Option<Rc<dyn InboxStore>>,
    aliases: Option<Rc<dyn AliasStore>>,
    /// Durable suspend→resume persistence. `None` = no wait-state support.
    bridge: Option<Rc<dyn InstanceBridge>>,
    /// The durable dead-letter / incident sink. On a NON-idempotent process's execution
    /// failure the dispatcher records the incident here (then acks — at-most-once). `None` = no
    /// durable incident store wired (the failure is still logged at error level).
    incidents: Option<Rc<dyn IncidentSink>>,
    /// A typed handle to the audit listener registered on the executor (`None` when no
    /// audit sink is wired). Used on the actor thread to READ the per-instance seq high-water
    /// at suspend (persisted into the snapshot's `audit_seq`) and to SEED it back before a
    /// resume, so the per-instance audit seq stays monotonic across suspend/resume + restart.
    audit_listener: Option<Rc<crate::audit::AuditListener>>,
    /// Two-tier payload byte cap (global default + per-channel override + the `0`
    /// disabled sentinel). Seeded from each channel's `payload-cap-bytes`; global default
    /// wired via [`ChannelEngineBuilder::with_payload_cap_policy`] (default: disabled).
    payload_cap_policy: PayloadCapPolicy,
    /// The `${feature.X}` channel feature-gate; `None` = gates always pass (an `enabled`
    /// expression is then parsed but not consulted).
    feature_provider: Option<Rc<dyn FeatureProvider>>,
    /// Per-tenant rate + concurrent-instance quotas, consulted BEFORE the
    /// tenant-binding check; `None` = unlimited.
    quota_enforcer: Option<Rc<dyn TenantQuotaEnforcer>>,
    /// Per-channel `max-concurrent-instances` admission gauge (read-only at
    /// admission; the count is maintained by the persistence/tracker layer). `None` = caps
    /// declared but not enforced.
    concurrency: Option<Rc<dyn ConcurrencyStore>>,
    /// The engine-declared capacity pools (`pool:` on a channel). Shared (`Arc`) by every lane
    /// and the outbox dispatcher, so one count governs every acquirer. `None` = no pool is
    /// declared; a `pool:` key then has nothing to draw from (load validation refuses it).
    capacity: Option<Arc<dyn CapacityPoolStore>>,
    /// The pool slot THIS start took under an admission key, not yet bound to its instance.
    /// Set by `drive_resolved`, consumed (re-keyed to the instance id) where the instance
    /// outlives the pass — its first durable park or a synchronous chain — and released by
    /// `drive_resolved` when nothing consumed it. Actor-local scratch, like `pending_settle`.
    pool_admission: std::cell::RefCell<Option<PoolAdmission>>,
    /// Instances whose session a reply ended during the current entry point (`session-end:
    /// on-reply`): released when the entry point returns, after any admission slot has been
    /// bound to the instance.
    session_releases: std::cell::RefCell<Vec<(DeploymentId, String)>>,
    /// `(module_key, channel)` of every inbound channel named by another channel's
    /// `session-end: on-message` → the session channels it ends.
    session_end_targets: BTreeMap<(String, String), Vec<String>>,
    /// DRAINING deployments (the deployment lifecycle), most recently drained first: relay
    /// correlation falls back to these scopes so an instance pinned to a flipped-away
    /// deployment keeps resuming until it retires. New intake never starts here.
    prior_deployments: Vec<DeploymentId>,
    /// The engine-wide deferred-ack registry (`ack-mode: on-complete` broker
    /// transports). Shared (`Arc`) with the executor's listener bus, the sweep task and
    /// across activation flips; `None` = no deferral capability (tests / bare builders):
    /// `dispatch_deferred` then settles every delivery immediately.
    deferred_acks: Option<Arc<DeferredAckRegistry>>,
    /// The CURRENT dispatch's deferred settle callbacks (set by `dispatch_deferred`,
    /// consumed by `drive_stateful`'s park arm after `commit_park` succeeds). Actor-local
    /// scratch — cleared at every dispatch entry so a panicked dispatch can never leak
    /// its callbacks into a later delivery.
    pending_settle: std::cell::RefCell<Option<DeferredSettle>>,
    intake_counter: std::cell::RefCell<u64>,
    /// This engine's lane in the shard router (execution scale-out): decides the relay
    /// handoff (`ResolvedResume` to the owner shard when correlation names an id off this
    /// lane) and salts the intake ids. The default is [`crate::http::EngineShard::single`]
    /// (`sutra.engine.shards = 1`), under which every id is owned here and the handoff
    /// arm is never taken.
    shard: crate::http::EngineShard,
    /// This lane's observability counters (execution scale-out §6.1): parks, resume
    /// passes, handoffs and claim bounces are recorded at their semantic sites in this
    /// pipeline. A bare builder gets a fresh, unobserved handle; the router-built engine
    /// receives its lane's shared handle so the exporter sees one registry per router.
    shard_metrics: std::sync::Arc<crate::shard_metrics::ShardLaneMetrics>,
    /// Scheme → inline caller for SYNCHRONOUS outbound channels. Empty (the default) means no
    /// channel-call is ever made inline.
    inline_callers: InlineCallerRegistry,
    /// Secret resolution for an inline call's outbound auth reference — the same resolver the
    /// outbox dispatcher uses for the async path (default: `env:` only).
    inline_secret_env: crate::outbox_dispatch::SecretEnv,
    /// Whether THIS pass has a driver waiting on it (a transport caller, the timer or schedule
    /// poller), so its synchronous calls may be made inline. Set by every entry point (false
    /// for a poison wake and a broadcast fan-out); actor-local scratch, like `pending_settle`.
    inline_capable: std::cell::Cell<bool>,
    /// The inbox dedup keys THIS dispatch recorded (`(channel, key)`), handed to a synchronous
    /// chain so a chain that fails can release them. Cleared at every dispatch entry.
    dispatch_dedup: std::cell::RefCell<Vec<(String, String)>>,
    /// Whether THIS dispatch recorded an inbox dedup key, so it must be settled
    /// ([`InboxStore::settle`]) before the dispatch returns.
    dedup_touched: std::cell::Cell<bool>,
    /// Stored instances a synchronous chain on THIS lane is driving. The chain holds the
    /// instance's claim, and claims are re-entrant per owner (one owner per lane), so a second
    /// relay or timer for the same instance would be granted the claim and resume a stale
    /// frontier — every resume path refuses an instance listed here instead.
    inline_held: std::cell::RefCell<std::collections::HashSet<String>>,
    /// `(module_key, channel)` of every inbound channel declaring `reply-stream: sse`: a chain
    /// answering a delivery on one streams its `q:stream` calls to the caller.
    sse_channels: BTreeSet<(String, String)>,
}

pub struct ChannelEngineBuilder {
    engine: ChannelEngine,
}

impl ChannelEngine {
    /// `executor` must have been built with `.with_emission_sink(emissions.clone())` so
    /// the dispatcher can drain `<q:reply destination>` / `<q:send>` emissions into the
    /// outbox hook after each execution. The whole dispatch surface is async (execution
    /// scale-out §3(a), Phase 3): the executor and every store seam are awaited directly on
    /// the caller's task — the shard lane's actor loop, which drives one request to
    /// completion at a time — so no runtime handle is captured here any more.
    pub fn builder(
        executor: TokenExecutor,
        emissions: Rc<DrainingSink>,
        chain: InboundChain,
    ) -> ChannelEngineBuilder {
        ChannelEngineBuilder {
            engine: ChannelEngine {
                channels: ChannelRegistry::new(),
                processes: Arc::new(ProcessModuleRegistry::new()),
                chain,
                executor,
                emissions,
                outbox: Rc::new(CollectingOutbox::new()),
                inbox: None,
                aliases: None,
                bridge: None,
                incidents: None,
                audit_listener: None,
                payload_cap_policy: PayloadCapPolicy::disabled(),
                feature_provider: None,
                quota_enforcer: None,
                concurrency: None,
                capacity: None,
                pool_admission: std::cell::RefCell::new(None),
                session_releases: std::cell::RefCell::new(Vec::new()),
                session_end_targets: BTreeMap::new(),
                prior_deployments: Vec::new(),
                deferred_acks: None,
                pending_settle: std::cell::RefCell::new(None),
                intake_counter: std::cell::RefCell::new(0),
                shard: crate::http::EngineShard::single(),
                shard_metrics: std::sync::Arc::new(
                    crate::shard_metrics::ShardLaneMetrics::default(),
                ),
                inline_callers: InlineCallerRegistry::new(),
                inline_secret_env: Arc::new(|full_ref| {
                    full_ref
                        .strip_prefix("env:")
                        .and_then(|name| std::env::var(name).ok())
                }),
                inline_capable: std::cell::Cell::new(false),
                dispatch_dedup: std::cell::RefCell::new(Vec::new()),
                dedup_touched: std::cell::Cell::new(false),
                inline_held: std::cell::RefCell::new(std::collections::HashSet::new()),
                sse_channels: BTreeSet::new(),
            },
        }
    }

    /// This engine's lane in the shard router (see the `shard` field).
    pub fn shard(&self) -> crate::http::EngineShard {
        self.shard
    }

    /// This lane's observability counters (see the `shard_metrics` field) — read by the
    /// activation flip to keep the rebuilt engine on the SAME lane handle, and by tests.
    pub fn shard_metrics(&self) -> std::sync::Arc<crate::shard_metrics::ShardLaneMetrics> {
        std::sync::Arc::clone(&self.shard_metrics)
    }

    /// The audit listener's current per-instance seq high-water (0 when no listener is
    /// wired, or the instance is unseen). Read at suspend to persist into the snapshot.
    fn audit_seq_for(&self, instance_id: &str) -> u32 {
        self.audit_listener
            .as_ref()
            .map(|l| l.seq_for(instance_id))
            .unwrap_or(0)
    }

    /// Seed the audit listener's per-instance seq from a resumed snapshot's `audit_seq`
    /// (no-op when no listener is wired). Must run before the resume path emits its next event.
    fn seed_audit(&self, instance_id: &str, seq: u32) {
        if let Some(l) = &self.audit_listener {
            l.seed(instance_id, seq);
        }
    }

    /// Dispatch one delivery through the intake pipeline. Async (Phase 3): the caller —
    /// the shard lane's actor loop — awaits it to completion before the next dequeue, so
    /// every §0 ordering property (reply-implies-committed; commit happens-before the
    /// next request) holds exactly as under the synchronous form.
    pub async fn dispatch(&self, message: &InboundMessage) -> Result<DispatchOutcome, Diagnostic> {
        // Panic hygiene: clear any settle callbacks a PANICKED earlier deferred dispatch
        // left behind (the actor catches the unwind) — a plain dispatch must never adopt
        // another delivery's callbacks.
        self.pending_settle.borrow_mut().take();
        self.begin_dispatch(true);
        let result = self.dispatch_inner(message).await;
        let result = self.hand_over_dedup(result);
        self.settle_dedup(result.is_ok()).await;
        result
    }

    /// A relay resolved to another lane carries the dispatch's unwritten dedup keys with it:
    /// the owner lane's resume step writes them in its own transaction.
    fn hand_over_dedup(
        &self,
        result: Result<DispatchOutcome, Diagnostic>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        match result {
            Ok(DispatchOutcome::Handoff {
                shard,
                mut resolved,
            }) => {
                if let Some(inbox) = &self.inbox {
                    resolved.pending_dedup = inbox.take_pending();
                }
                Ok(DispatchOutcome::Handoff { shard, resolved })
            }
            other => other,
        }
    }

    /// Before a dispatch that recorded a dedup key returns: write what no step consumed when the
    /// dispatch completed, drop it when it failed (see [`InboxStore::settle`]).
    async fn settle_dedup(&self, processed: bool) {
        if self.dedup_touched.replace(false) {
            if let Some(inbox) = &self.inbox {
                inbox.settle(processed).await;
            }
        }
    }

    /// Reset the per-dispatch scratch at an entry point; `inline` says whether a caller waits on
    /// this pass (see `inline_capable`).
    fn begin_dispatch(&self, inline: bool) {
        self.inline_capable.set(inline);
        self.dispatch_dedup.borrow_mut().clear();
        self.dedup_touched.set(false);
    }

    /// `ack-mode: on-complete` (broker) dispatch: the SAME pipeline, but when the
    /// instance PARKS at a wait state the delivery's settle callbacks are registered on
    /// the [`DeferredAckRegistry`] (inside the park arm, BEFORE `commit_park` — withdrawn
    /// on a failed commit) and the answer is [`DeferredDispatch::Deferred`] — the
    /// transport must not settle. The arm's invariant: "transport told Deferred ⇒
    /// registered ∧ committed"; a terminal event for the instance — from this lane or,
    /// under the shard router, any other — always finds the registration (see the park
    /// arm's ordering comment). When the dispatch does not park (ran to a terminal state
    /// / duplicate / dead-lettered / reject) — or no registry is wired — the callbacks
    /// drop unfired and the outcome settles immediately, exactly like [`Self::dispatch`].
    pub async fn dispatch_deferred(
        &self,
        message: &InboundMessage,
        settle: DeferredSettle,
    ) -> Result<DeferredDispatch, Diagnostic> {
        *self.pending_settle.borrow_mut() = Some(settle);
        self.begin_dispatch(true);
        let result = self.dispatch_inner(message).await;
        let result = self.hand_over_dedup(result);
        self.settle_dedup(result.is_ok()).await;
        let leftover = self.pending_settle.borrow_mut().take();
        let outcome = result?;
        // A synchronous chain carries the callbacks itself (when it took them) and settles at
        // its end; the router side maps its final outcome.
        if matches!(outcome, DispatchOutcome::InlineCalls(_)) {
            return Ok(DeferredDispatch::Settled(outcome));
        }
        Ok(match leftover {
            // The park arm consumed the callbacks — the ack is now deferred.
            None => DeferredDispatch::Deferred {
                instance_id: match &outcome {
                    DispatchOutcome::Completed { instance_id, .. } => instance_id.clone(),
                    // Unreachable: only the park arm consumes, and it yields `Completed`.
                    _ => String::new(),
                },
            },
            Some(_) => DeferredDispatch::Settled(outcome),
        })
    }

    async fn dispatch_inner(
        &self,
        message: &InboundMessage,
    ) -> Result<DispatchOutcome, Diagnostic> {
        // The `sutra.dispatch` waterfall span (telemetry facade; the OTLP exporter
        // subscribes to these spans without touching this call site). Holding the entered
        // guard across the awaits below is sound HERE (and at every span in this module):
        // the dispatch future is `!Send`, polled only by its lane's single-threaded actor
        // loop, which drives one request to completion before the next dequeue — the
        // thread never runs another dispatch while this span is entered.
        let span = tracing::info_span!(
            telemetry::SPAN_DISPATCH,
            channel = %message.channel,
            tenant = %message.tenant,
            deployment.id = tracing::field::Empty,
        );
        let _guard = span.enter();

        // ---- channel resolution --------------------------------------------------------
        let binding = self
            .channels
            .find(&message.module_key, &message.channel)
            .ok_or_else(|| {
                Diagnostic::error(
                    codes::RESOLVE_CHANNEL_UNKNOWN,
                    format!("No binding registered for channel '{}'", message.channel),
                )
            })?
            .clone();
        let deployment = binding.deployment_id();
        span.record("deployment.id", tracing::field::display(deployment.value()));

        // ---- tenant quota (BEFORE the tenant-binding check) -----------------------------
        // The dispatcher consults the quota enforcer before the binding's
        // tenant agreement (a throttled tenant sees the quota code, not a tenant-mismatch
        // code) — `quotaDeniedRejectsBeforeTenantBindingCheck`.
        if let Some(enforcer) = &self.quota_enforcer {
            if let QuotaCheckResult::Denied { reason, detail } = enforcer
                .check_inbound(&message.tenant, &deployment, &message.channel)
                .await
            {
                return Err(Diagnostic::error(&reason, detail));
            }
        }

        // ---- tenant agreement ----------------------------------------------------------
        if binding.tenant() != message.tenant {
            return Err(Diagnostic::error(
                codes::INBOUND_REJECTED_TENANT_CHANNEL_NOT_ALLOWED,
                format!(
                    "Channel '{}' is bound to tenant '{}' but the inbound message declares \
                     tenant '{}'",
                    message.channel,
                    binding.tenant(),
                    message.tenant
                ),
            ));
        }

        // ---- channel feature-gate ------------------------------------------------------
        // A channel's `${feature.X}` `enabled` expression, resolved through the wired
        // provider; a disabled gate rejects BEFORE any executor work
        // (`disabledChannelGateRejectsInboundWithFeatureDisabledCode`).
        if let (Some(expr), Some(provider)) = (&binding.enabled_expression, &self.feature_provider)
        {
            if !provider.is_enabled(expr) {
                return Err(Diagnostic::error(
                    codes::INBOUND_FEATURE_DISABLED,
                    format!(
                        "Channel '{}' is gated by feature expression '{expr}', which resolved \
                         to disabled.",
                        message.channel
                    ),
                )
                .with_attribute("channel", &message.channel));
            }
        }

        // ---- payload byte cap (cheap, BEFORE any decode) -------------------------------
        // Two-tier policy: per-channel override else the global default; `0` disables; the
        // cap is inclusive (`==` passes).
        let cap = self
            .payload_cap_policy
            .effective_cap_bytes(&message.channel);
        if cap > 0 {
            let payload_bytes = message.body.len() as u64;
            if payload_bytes > cap {
                return Err(Diagnostic::error(
                    codes::INBOUND_PAYLOAD_TOO_LARGE,
                    format!(
                        "Inbound on channel '{}' rejected: payload size {payload_bytes} B \
                         exceeds the effective cap of {cap} B.",
                        message.channel
                    ),
                )
                .with_attribute("channelId", &message.channel)
                .with_attribute("payloadBytes", payload_bytes.to_string())
                .with_attribute("effectiveCapBytes", cap.to_string()));
            }
        }

        // ---- per-channel concurrency cap (admission gauge) -----------------------------
        // A declared `max-concurrent-instances` admits at most N active instances; the
        // (N+1)th is a busy-signal reject. The count comes from the concurrency store —
        // the persisted `channel_instance` table in production (replica-coherent + crash-safe).
        // `use-only-in-flight-for-concurrency-cap: false` counts parked (WAITING) instances
        // too — a held call still holds its line.
        if let (Some(store), Some(channel_cap)) =
            (&self.concurrency, binding.max_concurrent_instances)
        {
            let include_waiting = !binding.use_only_in_flight_for_concurrency_cap;
            if store
                .count_active(&deployment, &message.channel, include_waiting)
                .await
                >= channel_cap as u64
            {
                return Err(Diagnostic::error(
                    codes::INBOUND_CHANNEL_AT_CAPACITY,
                    format!(
                        "Channel '{}' is at capacity ({channel_cap} concurrent instances); the \
                         inbound is rejected (busy).",
                        message.channel
                    ),
                )
                .with_attribute("channel", &message.channel)
                .with_attribute("cap", channel_cap.to_string()));
            }
        }

        // ---- inbox dedup (first-observer-wins) -------------------------------------------
        // Only an EXPLICITLY-supplied event id dedups; the transport's sha256(body)
        // fallback stays a variables-level key (a re-POST of the same business payload is
        // a new delivery — the MoneyTransfer isolation semantics).
        if message.explicit_event_id && !message.idempotency_key.is_empty() {
            if let Some(inbox) = &self.inbox {
                let first = inbox
                    .record_seen(&deployment, &message.channel, &message.idempotency_key)
                    .await;
                self.dedup_touched.set(true);
                if !first {
                    return Ok(DispatchOutcome::Duplicate);
                }
                self.dispatch_dedup
                    .borrow_mut()
                    .push((message.channel.clone(), message.idempotency_key.clone()));
            }
        }

        // ---- decode ONCE, up front (channel-scoped codec) --------------------------------
        let decoded =
            self.chain
                .decode(&binding, &message.body, message.content_type.as_deref())?;
        let message_type = decoded.as_ref().and_then(|d| d.message_type.clone());

        // The context a `<q:source filterExpression>` is evaluated against: the codec's reported
        // attributes under `attr`, plus `messageType`. Headers and the payload are deliberately
        // absent — see `selector_context`.
        //
        // A FATAL decode gets an EMPTY context, which keeps selection on message type alone. A
        // codec that could not parse cannot classify, so consulting predicates would make every
        // predicated subscriber answer false, resolve to zero handlers, and turn a clean
        // validation rejection into "no start event matched" — discarding the decode issues and
        // skipping `<q:onValidation>` entirely.
        let fatal_decode = decoded
            .as_ref()
            .map(|d| d.outcome == sutra_codec_spi::DecodeOutcome::Fatal)
            .unwrap_or(false);
        let selector_ctx = if fatal_decode {
            sutra_feel::FeelContext::new()
        } else {
            crate::intake::selector_context(decoded.as_ref(), message_type.as_deref())
        };

        // ---- start-event routing (multi-start <q:source> match) --------------------------
        let resolution = {
            // `sutra.resolve` (handler resolution).
            let _resolve = tracing::info_span!(
                telemetry::SPAN_RESOLVE,
                channel = %message.channel,
                deployment.id = %deployment.value(),
            )
            .entered();
            let resolution = self.processes.resolve_handlers(
                &deployment,
                &message.channel,
                message_type.as_deref(),
                &selector_ctx,
            );
            // A predicate that could not be decided is recorded and the dispatch continues: the
            // start event is simply not eligible. Aborting here would let one broken process deny
            // a channel every other subscriber shares.
            for (process_id, node_id, code, detail) in &resolution.predicate_faults {
                tracing::warn!(
                    code = %code,
                    process.id = %process_id,
                    node.id = %node_id,
                    channel = %message.channel,
                    "{detail}"
                );
            }
            resolution
        };
        let predicate_faults = resolution.predicate_faults;
        let handlers = resolution.handlers;
        if handlers.is_empty() {
            // `session-end: on-message`: this channel ends sessions another channel admitted. A
            // correlated message releases its instance's slot whether or not a wait in the flow
            // also subscribes; with none, it is accepted for the release alone.
            if self
                .session_end_targets
                .contains_key(&(binding.namespace.module_key(), message.channel.clone()))
            {
                return self
                    .end_session_on_message(&binding, &deployment, &decoded, message)
                    .await;
            }
            // Before failing "no start event", check whether this is a
            // channel-delivered RELAY: a wait node (userTask / message catch / receiveTask) subscribes to
            // this channel, so the inbound resumes a WAITING instance rather than minting
            // one. Same two-tier intake (validators run against the wait node's
            // `<q:source>`), then correlate by the process's `<q:alias>` and resume.
            if let Some(outcome) = self
                .resolve_relay(&binding, &deployment, &decoded, message)
                .await?
            {
                return Ok(outcome);
            }
            if !self.processes.has_module(&deployment) {
                return Err(Diagnostic::error(
                    codes::RESOLVE_MODULE_NOT_FOUND,
                    format!(
                        "No BPMN module registered under id '{}'",
                        binding.namespace.module_key()
                    ),
                ));
            }
            // No wait node subscribes either (or no bridge is wired) — fail closed.
            return Err(Diagnostic::error(
                codes::INBOUND_NO_START_EVENT_FOR_MESSAGE_TYPE,
                format!(
                    "Inbound on channel '{}' decoded to message type '{}' but no subscribing \
                     Start Event in module '{}' matched it.",
                    message.channel,
                    message_type.as_deref().unwrap_or("(none)"),
                    binding.namespace.module_key()
                ),
            )
            .with_attribute("channel", &message.channel)
            .with_attribute("messageType", message_type.as_deref().unwrap_or(""))
            // Without this, a message dropped because every predicate answered false looks
            // exactly like one whose type nothing subscribes to.
            .with_attribute(
                "selectPredicateFaults",
                predicate_faults
                    .iter()
                    .map(|(p, n, _, detail)| format!("{p}/{n}: {detail}"))
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        if !binding.broadcast && handlers.len() > 1 {
            // Start events, not processes: two matching start events in ONE process is a
            // configuration predicates make ordinary, and naming only the process would print
            // the same id twice.
            let starts: Vec<String> = handlers
                .iter()
                .map(|h| format!("{}/{}", h.process.id, h.start_event_id))
                .collect();
            return Err(Diagnostic::error(
                codes::INBOUND_AMBIGUOUS_HANDLER,
                format!(
                    "Inbound on non-broadcast channel '{}' (message type '{}') resolved to {} \
                     start events {starts:?}; a non-broadcast channel must resolve to exactly \
                     one. Narrow them with <q:source filterExpression>, split the channel, or \
                     set broadcast on the channel if fan-out is intended.",
                    message.channel,
                    message_type.as_deref().unwrap_or("(none)"),
                    handlers.len()
                ),
            ));
        }
        let selected: Vec<&HandlerMatch> = if binding.broadcast {
            handlers.iter().collect()
        } else {
            vec![&handlers[0]]
        };
        // A fan-out answers with its LAST handler's outcome, so an earlier handler's in-memory
        // chain would have nowhere to go: broadcast deliveries take the async path.
        if selected.len() > 1 {
            self.inline_capable.set(false);
        }

        // ---- per-handler: validate → aliases → execute → ack ------------------------------
        let mut last = None;
        for handler in selected {
            last = Some(
                self.drive_one(handler, &binding, &deployment, message, &decoded)
                    .await?,
            );
        }
        Ok(last.expect("at least one handler"))
    }

    async fn drive_one(
        &self,
        handler: &HandlerMatch,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
        decoded: &Option<DecodeResult>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        let process = &handler.process;
        let start_id = handler.start_event_id.as_str();
        let mut variables = project_variables(message);

        // ---- tier-1/tier-2 validation, frozen summary, policy ----------------------------
        let mut routed_start: Option<String> = Some(start_id.to_string());
        if let Some(decoded) = decoded {
            match self
                .chain
                .apply_decoded(process, start_id, binding, decoded, &mut variables)?
            {
                IntakeOutcome::Proceed { start_node_id } => {
                    if start_node_id.is_some() {
                        routed_start = start_node_id;
                    }
                }
                IntakeOutcome::Reject(d) => return Err(d),
                IntakeOutcome::Error { diagnostic, .. } => return Err(diagnostic),
            }
        }

        // A `<q:source dedupKey="body.<path>">` spec re-derives the dedup key from
        // the DECODED payload (header/ce.id precedence otherwise untouched). Unlike the pre-decode
        // transport-side dedup, this key is only knowable AFTER decode + handler resolution, so the
        // dedup runs HERE — a body-path dedup key now actually suppresses a redelivery (previously
        // it only re-projected `event.idempotencyKey`, never driving inbox dedup).
        if let Some(body_key) = apply_body_path_dedup_key(process, start_id, &mut variables) {
            if let Some(inbox) = &self.inbox {
                let first = inbox
                    .record_seen(deployment, &message.channel, &body_key)
                    .await;
                self.dedup_touched.set(true);
                if !first {
                    return Ok(DispatchOutcome::Duplicate);
                }
                self.dispatch_dedup
                    .borrow_mut()
                    .push((message.channel.clone(), body_key.clone()));
            }
        }

        self.drive_resolved(
            process,
            start_id,
            binding,
            deployment,
            message,
            variables,
            routed_start,
        )
        .await
    }

    /// Drive a RESOLVED start: the process, its start node and its seed variables are already
    /// settled, and what is left is the execution itself — park-or-run-to-end, alias
    /// materialisation, emissions commit, reply, failure gating.
    ///
    /// Split out of [`Self::drive_one`] so the SCHEDULED start path
    /// ([`Self::fire_scheduled_start`]) reaches the identical machinery. A timer start differs
    /// from an inbound only in what comes BEFORE this point (no channel resolution, no decode, no
    /// intake validation, no payload) — everything from here on, tenancy and quota and audit and
    /// concurrency included, must be one implementation or the two paths will drift.
    #[allow(clippy::too_many_arguments)]
    async fn drive_resolved(
        &self,
        process: &ProcessDefinition,
        start_id: &str,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
        variables: Variables,
        routed_start: Option<String>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        // ---- capacity pool (a start only — a relay or a timer resumes an instance that
        // already holds its slot) --------------------------------------------------------------
        // The slot is taken BEFORE the first pass runs, under a fresh admission key, so a full
        // pool rejects without executing anything. The pass re-keys it to the instance where
        // the instance outlives the pass (a park, a synchronous chain); otherwise it is
        // released here.
        let admission = self.admit_to_pool(binding, deployment, message).await?;
        *self.pool_admission.borrow_mut() = admission;
        let result = self
            .drive_resolved_pass(
                process,
                start_id,
                binding,
                deployment,
                message,
                variables,
                routed_start,
            )
            .await;
        let unbound = self.pool_admission.borrow_mut().take();
        if let (Some(admission), Some(store)) = (unbound, &self.capacity) {
            store.release(&admission.deployment, &admission.key).await;
        }
        self.flush_session_releases().await;
        result
    }

    /// The body of [`Self::drive_resolved`], run with any admission slot already taken.
    #[allow(clippy::too_many_arguments)]
    async fn drive_resolved_pass(
        &self,
        process: &ProcessDefinition,
        start_id: &str,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
        mut variables: Variables,
        routed_start: Option<String>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        // ---- inbound→stateful: a wait-state process parks rather than running to end ------
        if !process.is_sync_eligible() {
            return self
                .drive_stateful(
                    process,
                    start_id,
                    binding,
                    deployment,
                    message,
                    variables,
                    routed_start,
                )
                .await;
        }

        // ---- alias materialisation + alias rows (onConflict semantics) -------------------
        let intake_id = self.next_intake_id();
        let spawn_business_key = self.materialise_and_index_aliases(
            process,
            start_id,
            deployment,
            &intake_id,
            &mut variables,
        )?;
        // C6 phase 4 — thread the spawn correlation onto the SYNC path too (parity with
        // drive_stateful): a sync-eligible participant's cross-process coverage fragment then
        // carries a joinable business_key + trace_id rather than a NULL key that never unions.
        let correlation = CoverageCorrelation {
            trace_id: inbound_traceparent(message),
            business_key: spawn_business_key,
        };
        // Drive the async executor to completion HERE (Phase 3): its store ops await on
        // this same task, and the persistence commit below awaits after it — strictly
        // sequential, exactly the order the old `block_on` pair imposed.
        let result = self
            .executor
            .execute_sync_from_correlated(
                process,
                variables,
                deployment.clone(),
                binding.labels(),
                routed_start.as_deref(),
                correlation,
            )
            .await;
        self.retire_aliases(deployment, &intake_id);

        // Destination-bearing emissions commit at the quiescent point — whatever was
        // collected before a failure is NOT enqueued (the strict transactional outbox). With a
        // bridge wired the terminal step enqueues them durably in ONE transaction; the
        // in-memory hook serves persistence-less hosts and tests.
        match result {
            Ok(exec) => {
                self.commit_sync_emissions(binding, deployment, message, process)
                    .await?;
                self.mark_instance_ended(deployment, &exec.instance_id)
                    .await;
                let reply = self.build_reply(binding, process, message, &exec)?;
                Ok(DispatchOutcome::Completed {
                    instance_id: exec.instance_id,
                    outputs: exec.outputs.to_json(),
                    reply,
                })
            }
            Err(e) => {
                let _ = self.emissions.drain(); // failed step: nothing reaches the outbox
                let d = e.to_diagnostic();
                // Gate the ack disposition on the process's idempotency assertion: an
                // idempotent process is retried (NackRequeue), a non-idempotent one is consumed +
                // dead-lettered (at-most-once) rather than blind-redelivered-and-reprocessed.
                self.gate_execution_failure(
                    process,
                    deployment,
                    message,
                    Diagnostic::error(&d.code, d.message.clone()),
                )
                .await
            }
        }
    }

    /// Map an EXECUTION failure (a selected process's executor `Err`) to an ack disposition
    /// gated by the process's `<q:process idempotent>` assertion. This is the correctness fix:
    /// inbound redelivery is transport-level at-least-once and was previously ungated by
    /// idempotency, so a non-idempotent flow could be redelivered AND reprocessed on failure →
    /// duplicate side effects.
    ///
    /// - **Idempotent process** (`idempotent="true"`) → re-execution converges to one end state, so
    ///   the failure is RETRY-SAFE. The diagnostic is stamped with [`ACK_DISPOSITION_ATTR`] =
    ///   [`ACK_DISPOSITION_REQUEUE`] and returned as `Err`; the transport ack-mapper maps it to
    ///   `NackRequeue` (the broker redelivers and inbox dedup absorbs a duplicate).
    /// - **Non-idempotent process** (`idempotent="false"`, the fail-closed default) → blind
    ///   reprocess is unsafe. Record a durable incident (or, absent a sink, log at error level) and
    ///   return a [`DispatchOutcome::DeadLettered`] the transport ACKs — the message is CONSUMED
    ///   (at-most-once, no requeue), never blind-redelivered.
    async fn gate_execution_failure(
        &self,
        process: &ProcessDefinition,
        deployment: &DeploymentId,
        message: &InboundMessage,
        cause: Diagnostic,
    ) -> Result<DispatchOutcome, Diagnostic> {
        if process.idempotent {
            return Err(cause.with_attribute(ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE));
        }
        // The incident's `deployment` is the RESOLVED `dep-<hex>` pin, not the `module_key`
        // namespace string: it is the isolation column the durable sink binds and RLS-scopes on,
        // and a `module_key` there fails the sink's id validation outright (the row is dropped and
        // only the log floor below survives). `module_key`/`tenant` ride the capture instead, where
        // the replay path wants them.
        let incident = InboundIncident::of_failure(
            deployment.value(),
            &message.channel,
            &process.id,
            &message.idempotency_key,
            &cause.code,
            &cause.message,
            &message.received_at,
        )
        .with_capture(
            &message.tenant,
            &message.module_key,
            message.content_type.clone(),
            self.capture_payload(message),
            message.headers.clone(),
        );
        // Always observable — the record floor even when no durable sink is wired.
        tracing::error!(
            code = codes::INBOUND_NON_IDEMPOTENT_FAILURE,
            channel = %incident.channel,
            process = %incident.process_id,
            dedup_key = %incident.dedup_key,
            cause_code = %incident.failure_code,
            "inbound dead-lettered: non-idempotent process failed; consumed at-most-once (no requeue)"
        );
        if let Some(sink) = &self.incidents {
            sink.record(incident).await;
        }
        Ok(DispatchOutcome::DeadLettered {
            code: codes::INBOUND_NON_IDEMPOTENT_FAILURE.to_string(),
            process_id: process.id.clone(),
            cause_code: cause.code,
            detail: cause.message,
        })
    }

    /// The dead-letter payload capture: the consumed body, TRUNCATED at the channel's effective
    /// payload cap so one incident row can never hold more than the engine was willing to accept.
    /// When the cap is the documented `0`-disabled sentinel the shipped default ceiling applies
    /// anyway — "cap disabled" is a statement about what intake ADMITS, never a licence for the
    /// incident table to grow without bound.
    fn capture_payload(&self, message: &InboundMessage) -> Vec<u8> {
        let cap = self
            .payload_cap_policy
            .effective_cap_bytes(&message.channel);
        let ceiling = if cap == 0 {
            PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES
        } else {
            cap
        } as usize;
        let body: &[u8] = message.body.get();
        body[..body.len().min(ceiling)].to_vec()
    }

    /// Record a RESUMED instance's fatal step as durable FAILED state (the wait→dead commit),
    /// then hand back the diagnostic to surface. Called from both resume paths — the relay
    /// correlation and the timer fire — so they agree on the verdict.
    ///
    /// Only fatal signals reach here: the executor returns `Err` for `Uncaught`/`Diag` only, while
    /// a BPMN error routes to its boundary or error event sub-process inside the executor and never
    /// surfaces as a failed step. A failure to WRITE the marker is logged, not raised: the
    /// original cause is what the caller must see, and the always-on log floor already recorded it.
    async fn mark_instance_failed(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        pinned: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        cause: &Diagnostic,
    ) {
        // The failing step's emissions never reached a quiescent point — nothing may be delivered.
        let _ = self.emissions.drain();
        tracing::error!(
            code = codes::DISPATCH_INSTANCE_FAILED,
            instance = %instance_id,
            deployment = %pinned.value(),
            process = %snapshot.process_id,
            cause_code = %cause.code,
            "resumed instance FAILED fatally — persisting durable FAILED state; it is no longer \
             resumable by relay or timer"
        );
        if let Err(e) = bridge
            .commit_failed(pinned, instance_id, &cause.code, &cause.message)
            .await
        {
            tracing::error!(
                instance = %instance_id,
                error = %e.message,
                "durable FAILED state could not be committed — the instance stays at its previous \
                 frontier (the failure is still on the log floor above)"
            );
        }
        // A dead instance holds no admission slot: free it exactly as a terminal step does.
        if let Some(store) = &self.concurrency {
            store.record_terminal(pinned, instance_id).await;
        }
        self.release_pool_slots(pinned, instance_id).await;
    }

    /// Settle a resumed step that could not be committed, and hand back the diagnostic to
    /// surface.
    ///
    /// A [`ResumeCommitError::Refused`] step will be refused identically on every retry (its
    /// emissions break their contract, its reply cannot render, its aliases cannot be
    /// evaluated), so the instance fails exactly as it would had the executor itself raised the
    /// error: listeners hear `on_instance_failed` (the audit journal's `INSTANCE_FAILED`, an
    /// `on-complete` ack's settlement), then [`Self::mark_instance_failed`] persists the durable
    /// FAILED step, which resolves the instance's wait and timer rows. Without this a timer fire
    /// re-ran the whole tail on every backoff tick, forever, with the instance stuck SUSPENDED.
    ///
    /// A [`ResumeCommitError::Transient`] error is returned untouched: nothing was committed, the
    /// instance is where it was, and the caller's retry (the timer poller's backoff, a broker's
    /// redelivery) is the right response to a store that was briefly unavailable.
    #[allow(clippy::too_many_arguments)]
    async fn fail_refused_resume(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        process: &ProcessDefinition,
        pinned: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        labels: &BTreeMap<String, String>,
        error: ResumeCommitError,
    ) -> Diagnostic {
        let cause = match error {
            ResumeCommitError::Transient(d) => return d,
            ResumeCommitError::Refused(d) => d,
        };
        self.executor.notify_instance_failed(
            process,
            instance_id,
            pinned,
            labels,
            &cause.code,
            &cause.message,
        );
        self.mark_instance_failed(bridge, pinned, instance_id, snapshot, &cause)
            .await;
        cause
    }

    /// Take a start's capacity-pool slot when its channel draws from a pool (`pool:`): under a
    /// fresh admission key, since the instance id does not exist yet. A full pool is a
    /// busy-signal reject. A store failure ADMITS (fail-open, like the channel cap's count): a
    /// transient database blip must not turn every inbound away.
    ///
    /// The slot is held for the delivery's call: the declared `pool-call-key` header's value,
    /// else the delivery's own identity (its idempotency key, else the admission key).
    async fn admit_to_pool(
        &self,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
    ) -> Result<Option<PoolAdmission>, Diagnostic> {
        let (Some(store), Some(pool)) = (&self.capacity, binding.pool.as_deref()) else {
            return Ok(None);
        };
        let key = new_uuid();
        let call = binding
            .call_key_in(&message.headers)
            .or_else(|| {
                (!message.idempotency_key.is_empty()).then(|| message.idempotency_key.clone())
            })
            .unwrap_or_else(|| key.clone());
        let request = SlotRequest {
            pool,
            deployment,
            tenant: binding.tenant(),
            holder: &key,
            call: &call,
            direction: SlotDirection::Inbound,
            require_live: false,
        };
        match store.acquire(&request).await {
            Ok(SlotAcquisition::Acquired | SlotAcquisition::AlreadyHeld) => {
                Ok(Some(PoolAdmission {
                    deployment: deployment.clone(),
                    key,
                }))
            }
            Ok(SlotAcquisition::Exhausted {
                inbound_used,
                outbound_used,
            }) => {
                let slots = store.pool(pool).map(|p| p.slots).unwrap_or(0);
                Err(Diagnostic::error(
                    codes::INBOUND_POOL_EXHAUSTED,
                    format!(
                        "Channel '{}' draws from capacity pool '{pool}', which has no free slot \
                         ({inbound_used} inbound + {outbound_used} outbound of {slots}); the \
                         inbound is rejected (busy).",
                        binding.channel_name
                    ),
                )
                .with_attribute("channel", &binding.channel_name)
                .with_attribute("pool", pool)
                .with_attribute("slots", slots.to_string()))
            }
            Ok(SlotAcquisition::UnknownPool) => {
                tracing::warn!(
                    channel = %binding.channel_name,
                    pool,
                    "channel names a capacity pool the engine no longer declares — admitted \
                     without a slot"
                );
                Ok(None)
            }
            Ok(SlotAcquisition::InstanceEnded | SlotAcquisition::SessionEnded) => Ok(None),
            Err(d) => {
                tracing::warn!(
                    channel = %binding.channel_name,
                    pool,
                    code = %d.code,
                    error = %d.message,
                    "capacity-pool acquisition failed — admitting without a slot (fail-open)"
                );
                Ok(None)
            }
        }
    }

    /// Bind this pass's admission slot to the instance that now outlives the pass (its first
    /// durable park, or a synchronous chain). Returns whether a slot was bound.
    async fn bind_admission(&self, instance_id: &str) -> bool {
        let admission = self.pool_admission.borrow_mut().take();
        let Some(admission) = admission else {
            return false;
        };
        if let Some(store) = &self.capacity {
            store
                .rebind(&admission.deployment, &admission.key, instance_id)
                .await;
        }
        true
    }

    /// The instance has FINISHED (End, a failure, a cancel): release every capacity-pool slot
    /// it still holds, whatever call took it, and let the store know it ended — a request it
    /// emitted before finishing then releases its slot as soon as it is delivered. Idempotent.
    async fn release_pool_slots(&self, deployment: &DeploymentId, instance_id: &str) {
        if let Some(store) = &self.capacity {
            store.release_ended(deployment, instance_id).await;
        }
    }

    /// A pass took an instance straight through to End: it may hold no slot, but requests it
    /// emitted on a pooled channel will take one when the outbox sends them, and the store must
    /// know their instance is over to return those slots at delivery.
    async fn mark_instance_ended(&self, deployment: &DeploymentId, instance_id: &str) {
        if !instance_id.is_empty() {
            self.release_pool_slots(deployment, instance_id).await;
        }
    }

    /// End an instance's WHOLE session (`session-end:` with no call key to go by): free its
    /// channel admission slot and every pool slot while the instance runs on. Every later
    /// suspend/resume record for the instance is a no-op, and nothing re-acquires at a park, so
    /// the release sticks.
    async fn end_session(&self, deployment: &DeploymentId, instance_id: &str) {
        if let Some(store) = &self.concurrency {
            store.record_terminal(deployment, instance_id).await;
        }
        if let Some(store) = &self.capacity {
            store.end_session(deployment, instance_id, None).await;
        }
    }

    /// End ONE call of an instance (`session-end:` naming it): release the pool slots held for
    /// the calls `selector` names. When the call released is the instance's inbound admission,
    /// its channel admission slot goes too — the session it opened is over. The instance's
    /// other calls keep their slots.
    async fn end_call(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        selector: CallSelector<'_>,
    ) {
        let released = match (&self.capacity, selector) {
            // A named call's session is over: a request on it still waiting in the outbox is
            // sent without a slot, rather than re-taking the one released here.
            (Some(store), CallSelector::Call(call)) => {
                store.end_session(deployment, instance_id, Some(call)).await
            }
            (Some(store), selector) => store.release_calls(deployment, instance_id, selector).await,
            (None, _) => Vec::new(),
        };
        let admission_ended = matches!(selector, CallSelector::Direction(SlotDirection::Inbound))
            || released.contains(&SlotDirection::Inbound);
        if admission_ended {
            if let Some(store) = &self.concurrency {
                store.record_terminal(deployment, instance_id).await;
            }
        }
    }

    /// End what a session-ending message names: the one call its key identifies when the
    /// session channel declares `pool-call-key:` and the message carries it, the whole session
    /// otherwise.
    async fn end_named(&self, deployment: &DeploymentId, instance_id: &str, call: Option<&str>) {
        match call {
            Some(call) => {
                self.end_call(deployment, instance_id, CallSelector::Call(call))
                    .await
            }
            None => self.end_session(deployment, instance_id).await,
        }
    }

    /// The call key a session-ending message on `binding` names: the value of the first session
    /// channel's `pool-call-key` header that it carries.
    fn session_call_key(
        &self,
        binding: &ChannelBinding,
        message: &InboundMessage,
    ) -> Option<String> {
        let module_key = binding.namespace.module_key();
        self.session_end_targets
            .get(&(module_key.clone(), message.channel.clone()))?
            .iter()
            .filter_map(|session| self.channels.find(&module_key, session))
            .find_map(|session| session.call_key_in(&message.headers))
    }

    /// The deployment scopes an instance correlated through `deployment` can live in: the live
    /// one first, then the DRAINING ones.
    fn session_scopes(&self, deployment: &DeploymentId) -> Vec<DeploymentId> {
        let mut scopes = vec![deployment.clone()];
        for prior in &self.prior_deployments {
            if !scopes.contains(prior) {
                scopes.push(prior.clone());
            }
        }
        scopes
    }

    /// Release the sessions a reply ended (`session-end: on-reply`) since the last flush: the
    /// replying instance's inbound call (its admission slot and channel slot) — any outbound
    /// call it holds stays. Runs after the pass that rendered the reply bound any admission slot
    /// to its instance.
    async fn flush_session_releases(&self) {
        let pending: Vec<(DeploymentId, String)> =
            self.session_releases.borrow_mut().drain(..).collect();
        for (deployment, instance_id) in pending {
            for scope in self.session_scopes(&deployment) {
                self.end_call(
                    &scope,
                    &instance_id,
                    CallSelector::Direction(SlotDirection::Inbound),
                )
                .await;
            }
        }
    }

    /// The fail-closed guard both resume paths run BEFORE re-driving a loaded instance: a durably
    /// FAILED instance is never resumed. Returns the [`codes::DISPATCH_INSTANCE_FAILED`]
    /// diagnostic to surface, or `None` when the instance is resumable as far as its status goes.
    fn instance_failed_guard(
        snapshot: &SuspendedInstance,
        instance_id: &str,
    ) -> Option<Diagnostic> {
        (snapshot.status == crate::bridge::INSTANCE_STATUS_FAILED).then(|| {
            Diagnostic::error(
                codes::DISPATCH_INSTANCE_FAILED,
                format!(
                    "Instance {instance_id} is FAILED (a fatal step killed it); it is not \
                     resumable — inspect it on the admin surface and cancel it once handled."
                ),
            )
        })
    }

    /// The fail-closed guard for an instance that has already FINISHED: since terminal retention
    /// (P1-2) a completed or operator-terminated instance keeps its row for the configured
    /// retention window instead of vanishing, so a resume path can now genuinely load one.
    ///
    /// It reuses [`codes::RUNTIME_RESUME_NOT_SUSPENDED`] rather than minting a code, because that
    /// is exactly what this has always been and what a relay to a finished instance would have
    /// fallen through to anyway ("in state X, not SUSPENDED") — retention changes how OFTEN the
    /// condition is reachable, not what it means. What it does add is an honest message: a finished
    /// instance is not "not parked yet", it is over, and the row an operator can see is history
    /// rather than something to wait on. Distinct from `INSTANCE_FAILED`, which means "needs a
    /// human" — nothing needs doing here.
    fn instance_terminal_guard(
        snapshot: &SuspendedInstance,
        instance_id: &str,
    ) -> Option<Diagnostic> {
        (snapshot.status == crate::bridge::INSTANCE_STATUS_COMPLETED
            || snapshot.status == crate::bridge::INSTANCE_STATUS_TERMINATED)
            .then(|| {
                Diagnostic::error(
                    codes::RUNTIME_RESUME_NOT_SUSPENDED,
                    format!(
                        "Instance {instance_id} already reached {}; it is retained as execution \
                     history (sutra.instance.retention) and is not resumable.",
                        snapshot.status
                    ),
                )
                .with_attribute("instanceId", instance_id)
                .with_attribute("instanceStatus", &snapshot.status)
            })
    }

    /// Drain the collected emissions at a COMPLETED quiescent point: durable one-tx
    /// enqueue through the bridge when wired (the sync-path terminal step), else the
    /// in-memory [`crate::stores::OutboxSink`] hook.
    async fn commit_sync_emissions(
        &self,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
        process: &ProcessDefinition,
    ) -> Result<(), Diagnostic> {
        let mut drained = self.emissions.drain();
        // Held to the destination's contract BEFORE the transactional commit: a rejection here
        // fails the step, so nothing is enqueued and nothing reaches the wire. Validating at the
        // sink instead would be after the commit, which turns a contract breach into a delivery
        // incident and leaves the flow believing it sent a valid message.
        self.validate_emissions(
            &binding.namespace.module_key(),
            deployment,
            &binding.channel_name,
            process,
            &mut drained,
        )?;
        if let Some(bridge) = &self.bridge {
            let rows = to_outbox_emissions(drained, binding.labels(), inbound_traceparent(message));
            if !rows.is_empty() {
                bridge.commit_emissions(deployment, &rows).await?;
            }
            return Ok(());
        }
        for emission in drained {
            self.outbox.enqueue(emission);
        }
        Ok(())
    }

    /// Resolve the contract one emission must satisfy and validate its body against it.
    ///
    /// See [`NO_INFERRED_REPLY_CODEC`] for why a reply infers nothing.
    ///
    /// `<q:send>` and `<q:reply>` share the validation model — the same
    /// [`InboundChain::validate_outbound`], the same fail-closed posture, the same precedence of
    /// a stated codec over an inferred one. What differs is only WHERE the inferred one comes
    /// from, and this is the awkward case: a `<q:reply destination=…>` is delivered out of band
    /// and so arrives here as an emission rather than as the synchronous reply, while only a
    /// DESTINATION-LESS reply rides the inbound connection and is checked in `build_reply`.
    ///
    /// Reading only the `send` binding therefore skipped every destination-bearing reply,
    /// including one that stated its codec — which is the precise failure this check exists to
    /// prevent, the single declaration asking to be validated being the one exempt from it. So
    /// the binding consulted follows the emission's own kind.
    ///
    /// `host_channel` names the inbound channel for the diagnostic only — a reply does not
    /// inherit that channel's contract.
    #[allow(clippy::too_many_arguments)]
    fn validate_emission(
        &self,
        module_key: &str,
        deployment: &DeploymentId,
        host_channel: &str,
        process: &ProcessDefinition,
        emission: &sutra_executor::Emission,
    ) -> Result<(), Diagnostic> {
        let bindings = process.bindings_for(&emission.node_id);
        let (declared, channel, fallback) = match emission.kind {
            // A reply names no channel — it answers over the one the request arrived on.
            sutra_executor::EmissionKind::Reply => (
                bindings.reply.as_ref().and_then(|r| r.codec.clone()),
                None,
                NO_INFERRED_REPLY_CODEC.to_string(),
            ),
            // A send names its own: that channel's binding supplies the codec the author did
            // not state, and names the channel the diagnostic reports either way.
            _ => {
                let send = bindings.send.as_ref();
                let channel = send.and_then(|s| s.channel.clone());
                let fallback = channel
                    .as_deref()
                    .and_then(|c| self.channels.find(module_key, c.trim()))
                    .map(|b| b.codec.clone())
                    .unwrap_or_default();
                (send.and_then(|s| s.codec.clone()), channel, fallback)
            }
        };
        // A stated codec governs even where no channel of this deployment does — which is the
        // point of stating it, since a `<q:send destination>` addresses an endpoint and carries
        // nothing to infer from. With neither, there is no contract to hold the message to.
        if declared.is_none() && fallback.trim().is_empty() {
            return Ok(());
        }
        let label = channel
            .as_deref()
            .or(Some(host_channel).filter(|h| !h.is_empty()))
            .unwrap_or(&emission.destination);
        self.chain.validate_outbound(
            deployment,
            label,
            declared.as_deref(),
            &fallback,
            &emission.body,
            emission.content_type.as_deref(),
            &format!("Node '{}' of process '{}'", emission.node_id, process.id),
        )
    }

    /// Hold every emission of one quiescent step to its destination's contract, BEFORE the
    /// commit that would make it real.
    ///
    /// Called from each of the four places a step's emissions drain — the sync terminal and park
    /// steps and the resumed terminal and re-park steps. All four needed it: a parked flow is
    /// the normal shape for broker ingress, so wiring only the synchronous path would have left
    /// the common case silently unvalidated, which is the same class of gap this check exists
    /// to close.
    #[allow(clippy::too_many_arguments)]
    ///
    /// The same pass stamps every request bound for a pooled outbound channel with the pool it
    /// draws from ([`CAPACITY_POOL_HEADER`], plus [`CAPACITY_CALL_HEADER`] for a channel-call),
    /// so the outbox dispatcher can hold the request until its call holds a slot.
    fn validate_emissions(
        &self,
        module_key: &str,
        deployment: &DeploymentId,
        host_channel: &str,
        process: &ProcessDefinition,
        emissions: &mut [sutra_executor::Emission],
    ) -> Result<(), Diagnostic> {
        for emission in emissions.iter_mut() {
            self.validate_emission(module_key, deployment, host_channel, process, emission)?;
            self.stamp_capacity_pool(module_key, process, emission);
            stamp_media_capture(process, emission);
            if self.audit_listener.is_some() {
                stamp_audit_journal(process, emission);
            }
        }
        Ok(())
    }

    /// Mark a request whose outbound channel declares `pool:` (see [`Self::validate_emissions`]).
    fn stamp_capacity_pool(
        &self,
        module_key: &str,
        process: &ProcessDefinition,
        emission: &mut sutra_executor::Emission,
    ) {
        if emission.kind != sutra_executor::EmissionKind::Send {
            return;
        }
        let call = channel_call_name(process, &emission.node_id);
        let channel = call.clone().or_else(|| {
            process
                .bindings_for(&emission.node_id)
                .send
                .as_ref()
                .and_then(|send| send.channel.clone())
        });
        let Some(binding) = channel
            .as_deref()
            .and_then(|c| self.channels.find(module_key, c.trim()))
        else {
            return;
        };
        let Some(pool) = binding.pool.clone() else {
            return;
        };
        if let Some(call_key) = binding.call_key_in(&emission.headers) {
            emission
                .headers
                .insert(CAPACITY_CALL_KEY_HEADER.to_string(), call_key);
        }
        emission
            .headers
            .insert(CAPACITY_POOL_HEADER.to_string(), pool);
        if call.is_some() {
            emission
                .headers
                .insert(CAPACITY_CALL_HEADER.to_string(), "true".to_string());
        }
    }

    /// Evaluate every `<q:alias>` on the routed start event: bind the value into
    /// `variables` under the alias's short name (raw list for `multi=true`; nulls skipped)
    /// and record the alias rows, applying the unique-conflict policy —
    /// `reject` (default) raises `SUTRA.INBOUND.ALIAS_CONFLICT_REJECT`; `correlate` is
    /// recorded as a redirect but v1 rejects the second arrival.
    /// Materialise + index the start node's `<q:alias>` rows, returning the spawn's primary
    /// correlation value (the C6 coverage `business_key`): the first unique alias's value,
    /// else the first single-valued alias's — mirroring the stateful path's `spawn_business_key`
    /// so a sync-eligible participant's cross-process fragment carries a joinable key.
    fn materialise_and_index_aliases(
        &self,
        process: &ProcessDefinition,
        start_id: &str,
        deployment: &DeploymentId,
        intake_id: &str,
        variables: &mut Variables,
    ) -> Result<Option<String>, Diagnostic> {
        let aliases = &process.bindings_for(start_id).aliases;
        if aliases.is_empty() {
            return Ok(None);
        }
        let mut first_key: Option<String> = None;
        let mut unique_key: Option<String> = None;
        for alias in aliases.clone() {
            let value =
                sutra_feel::expressions::eval(&alias.expression, &variables.to_feel_context())
                    .map_err(|e| {
                        Diagnostic::error(
                            codes::INBOUND_ALIAS_FEEL_EVAL_FAILED,
                            format!(
                                "<q:alias {}> expression '{}' on node {start_id} threw at \
                         evaluation: {}",
                                alias.name, alias.expression, e.message
                            ),
                        )
                    })?;
            if alias.multi {
                match &value {
                    FeelValue::Null => continue, // empty alias-list — a valid no-op
                    FeelValue::List(items) => {
                        for item in items {
                            if item.is_null() {
                                continue;
                            }
                            self.record_alias_row(
                                deployment,
                                intake_id,
                                &alias,
                                &sutra_feel::value::canonical_string_of(item),
                            )?;
                        }
                        variables.insert(alias.name.clone(), value.clone());
                    }
                    other => {
                        return Err(Diagnostic::error(
                            codes::INBOUND_ALIAS_MULTI_NOT_LIST,
                            format!(
                                "<q:alias {} multi=true> expression '{}' on node {start_id} \
                                 returned {}; multi=true requires a list-valued result",
                                alias.name,
                                alias.expression,
                                other.type_name()
                            ),
                        ));
                    }
                }
            } else {
                if value.is_null() {
                    continue; // producers may legitimately omit a key
                }
                let sval = sutra_feel::value::canonical_string_of(&value);
                if alias.unique && unique_key.is_none() {
                    unique_key = Some(sval.clone());
                }
                if first_key.is_none() {
                    first_key = Some(sval.clone());
                }
                self.record_alias_row(deployment, intake_id, &alias, &sval)?;
                variables.insert(alias.name.clone(), value);
            }
        }
        // Prefer the unique alias (the correlation key) else the first — parity with the
        // stateful path's `spawn_business_key`.
        Ok(unique_key.or(first_key))
    }

    fn record_alias_row(
        &self,
        deployment: &DeploymentId,
        intake_id: &str,
        alias: &sutra_bpmn::qbindings::AliasBinding,
        value: &str,
    ) -> Result<(), Diagnostic> {
        let Some(store) = &self.aliases else {
            return Ok(()); // no alias store wired — materialisation only
        };
        let inserted = store.record(deployment, intake_id, &alias.name, value, alias.unique);
        if inserted || !alias.unique {
            return Ok(());
        }
        let Some(owner) = store.find_live(deployment, &alias.name, value) else {
            return Ok(()); // race / idempotent retry — treat as recorded
        };
        if owner == intake_id {
            return Ok(());
        }
        match alias.on_conflict.unwrap_or(AliasConflict::Reject) {
            AliasConflict::Reject => Err(Diagnostic::error(
                codes::INBOUND_ALIAS_CONFLICT_REJECT,
                format!(
                    "<q:alias {}> = '{value}' already bound to live instance {owner} for \
                     deployment {} (unique=true, onConflict=reject)",
                    alias.name,
                    deployment.value()
                ),
            )),
            AliasConflict::Correlate => Err(Diagnostic::error(
                codes::INBOUND_ALIAS_CONFLICT_REJECT,
                format!(
                    "<q:alias {}> = '{value}' already bound to live instance {owner} \
                     (onConflict=correlate); signal-routing to the existing instance is a \
                     follow-up — for now the second arrival is rejected",
                    alias.name
                ),
            )),
        }
    }

    fn retire_aliases(&self, deployment: &DeploymentId, intake_id: &str) {
        if let Some(store) = &self.aliases {
            store.retire(deployment, intake_id);
        }
    }

    // ---- the stateful (wait-state) surface -----------------------------------------------

    /// Drive a wait-state activation: `execute_stateful` and, on SUSPENDED, persist the
    /// snapshot + wait rows + alias rows in ONE step via the [`InstanceBridge`].
    /// Fails fast with `SUTRA.INBOUND.PERSISTENCE_REQUIRED` when no bridge is wired (the
    /// no-InstanceStore posture) — no instance is ever orphaned.
    #[allow(clippy::too_many_arguments)]
    async fn drive_stateful(
        &self,
        process: &ProcessDefinition,
        start_id: &str,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        message: &InboundMessage,
        mut variables: Variables,
        routed_start: Option<String>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        let Some(bridge) = self.bridge.clone() else {
            return Err(Diagnostic::error(
                codes::INBOUND_PERSISTENCE_REQUIRED,
                format!(
                    "Inbound on channel '{}' resolved to wait-state process '{}', but no \
                     InstanceStore is configured; a process that parks at a wait node cannot \
                     be durably suspended without persistence.",
                    message.channel, process.id
                ),
            ));
        };

        // Materialise alias VALUES into variables and collect the rows; the
        // rows ride the park step instead of a pre-commit AliasStore write.
        let alias_rows = self.evaluate_alias_rows(process, start_id, &mut variables)?;
        // Alias-indexer posture: a unique alias already LIVE on another instance aborts
        // the start BEFORE any token is driven (both `reject` and `correlate` reject the
        // duplicate start). The step primitive re-checks atomically at commit — this
        // pre-check is the deterministic reject, the step is the race-safe guard.
        for row in &alias_rows {
            if !row.record.unique {
                continue;
            }
            if let Some(owner) = bridge
                .find_live_alias(deployment, &row.record.name, &row.record.value)
                .await?
            {
                return Err(Diagnostic::error(
                    codes::INBOUND_ALIAS_CONFLICT_REJECT,
                    format!(
                        "<q:alias {}> = '{}' already bound to live instance {owner} \
                         (onConflict={}); for now the second arrival is rejected",
                        row.record.name,
                        row.record.value,
                        match row.on_conflict {
                            AliasConflict::Reject => "reject",
                            AliasConflict::Correlate => "correlate",
                        }
                    ),
                ));
            }
            // A synchronous chain in flight holds its unique aliases in memory only (nothing is
            // committed until the chain ends), so the durable index cannot see it.
            if let Some(owner) = self
                .aliases
                .as_ref()
                .and_then(|store| store.find_live(deployment, &row.record.name, &row.record.value))
            {
                return Err(Diagnostic::error(
                    codes::INBOUND_ALIAS_CONFLICT_REJECT,
                    format!(
                        "<q:alias {}> = '{}' already bound to in-flight instance {owner} \
                         (a synchronous call chain); the second arrival is rejected",
                        row.record.name, row.record.value
                    ),
                ));
            }
        }

        // C6 phase 4 — the spawn's coverage correlation dims for any cross-process fragment this
        // instance writes: the inbound trace-id + this leg's primary `<q:alias>` value (prefer a
        // unique alias — the correlation key — else the first). Best-effort (`None`-tolerant).
        let spawn_business_key = alias_rows
            .iter()
            .find(|r| r.record.unique)
            .or_else(|| alias_rows.first())
            .map(|r| r.record.value.clone());
        let correlation = CoverageCorrelation {
            trace_id: inbound_traceparent(message),
            business_key: spawn_business_key,
        };
        let result = match self
            .executor
            .execute_stateful_from_correlated(
                process,
                variables,
                deployment.clone(),
                binding.labels(),
                routed_start.as_deref(),
                correlation,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                // The initial stateful pass FAILED (before any quiescent commit, so nothing
                // persisted). Gate the ack disposition on the process's idempotency assertion:
                // idempotent ⇒ retry (NackRequeue); non-idempotent ⇒ consume + dead-letter.
                let d = e.to_diagnostic();
                return self
                    .gate_execution_failure(
                        process,
                        deployment,
                        message,
                        Diagnostic::error(&d.code, d.message.clone()),
                    )
                    .await;
            }
        };

        match result {
            StatefulExecResult::Completed {
                instance_id,
                outputs,
                visited_nodes,
            } => {
                // Ran straight through to COMPLETED (no wait node was actually reached) —
                // behave exactly like a sync-eligible flow, reply included.
                self.commit_sync_emissions(binding, deployment, message, process)
                    .await?;
                self.mark_instance_ended(deployment, &instance_id).await;
                let exec = ExecResult {
                    instance_id: instance_id.clone(),
                    outputs,
                    visited_nodes,
                };
                let reply = self.build_reply(binding, process, message, &exec)?;
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: exec.outputs.to_json(),
                    reply,
                })
            }
            StatefulExecResult::Suspended {
                instance_id,
                waiting_nodes,
                completed_nodes,
                variables,
                start_node,
                timer_waits,
                detached_reply,
                coverage_progress,
                retry_attempts,
                retry_backoff,
                fresh_waits,
            } => {
                // Respond-and-continue: when the pass parked at a `<q:reply continue>`
                // service task, flush the produced reply to the caller NOW. Built BEFORE the park
                // commit so "reply returned ⇒ park committed" holds — a failed `commit_park`
                // propagates via `?` and the reply is never returned. A due-now timer wait (armed by
                // the executor on the continue-reply node) self-resumes the remaining nodes at once.
                // A plain park answers with the last destination-less `<q:reply>` the pass
                // rendered, if it reached one.
                let reply = {
                    let exec = ExecResult {
                        instance_id: instance_id.clone(),
                        outputs: variables.clone(),
                        visited_nodes: completed_nodes.iter().cloned().collect(),
                    };
                    self.reply_at(binding, process, message, &exec, !detached_reply)?
                };
                // Quiescent-point rule: the destination-bearing emissions this
                // step collected (channel-call requests, `<q:send>`s before the park)
                // ride the park step and commit ATOMICALLY with the snapshot — or not at
                // all (the strict transactional outbox).
                let mut drained = self.emissions.drain();
                self.validate_emissions(
                    &binding.namespace.module_key(),
                    deployment,
                    &binding.channel_name,
                    process,
                    &mut drained,
                )?;
                // Synchronous outbound calls: when a caller waits on this pass, nothing parks —
                // the in-memory instance goes to the router side with its requests and is
                // persisted only if (and when) the chain reaches a durable park. A respond-and-
                // continue detach keeps its own early-reply path.
                let sync_sites = if detached_reply {
                    Vec::new()
                } else {
                    self.sync_call_sites(process, deployment, &fresh_waits, &variables)
                };
                if !sync_sites.is_empty() {
                    let pinned = self.processes.find_in_module(deployment, &process.id);
                    match pinned {
                        Some(pinned)
                            if self.inline_capable.get()
                                && sync_sites.iter().all(|site| site.caller.is_some()) =>
                        {
                            let mut chain = Box::new(InlineChain {
                                lane: self.shard.index,
                                instance_id: instance_id.clone(),
                                process: pinned,
                                deployment: deployment.clone(),
                                binding: binding.clone(),
                                message: message.clone(),
                                completed_nodes: completed_nodes.clone(),
                                waiting_nodes: waiting_nodes.clone(),
                                variables: variables.clone(),
                                start_node: start_node.clone(),
                                coverage: coverage_progress.clone(),
                                retry_attempts: retry_attempts.clone(),
                                retry_backoff: retry_backoff.clone(),
                                pending_timers: timer_waits.clone(),
                                emissions: Vec::new(),
                                alias_rows: alias_rows.iter().map(|r| r.record.clone()).collect(),
                                held_aliases: false,
                                counted: false,
                                pooled: false,
                                marked: false,
                                stored: None,
                                settle: if self.deferred_acks.is_some() {
                                    self.pending_settle.borrow_mut().take()
                                } else {
                                    None
                                },
                                dedup_keys: self.dispatch_dedup.borrow_mut().drain(..).collect(),
                                in_flight: BTreeSet::new(),
                                visited: completed_nodes.iter().cloned().collect(),
                                audit_seq: self.audit_seq_for(&instance_id),
                                early_reply: None,
                                fresh_waits: fresh_waits.clone(),
                            });
                            // The chain carries the instance on: it holds the start's pool slot.
                            chain.pooled = self.bind_admission(&instance_id).await;
                            return self.launch_inline_chain(chain, drained, sync_sites).await;
                        }
                        _ => self.warn_sync_degraded(
                            process,
                            &sync_sites,
                            if self.inline_capable.get() {
                                "no inline caller serves the channel's scheme"
                            } else {
                                "this pass cannot carry a chain (a broadcast fan-out or a poison wake)"
                            },
                        ),
                    }
                }
                let emissions =
                    to_outbox_emissions(drained, binding.labels(), inbound_traceparent(message));
                let pv = persisted_variables(
                    &process.declared_variables,
                    persisted_variable_values(&variables),
                );
                let snapshot = SuspendedInstance {
                    process_id: process.id.clone(),
                    deployment_id: deployment.value().to_string(),
                    status: "SUSPENDED".to_string(),
                    suspended: true,
                    completed_nodes,
                    variables: pv.kept,
                    sensitive: pv.sensitive,
                    waiting_nodes: waiting_nodes.clone(),
                    start_node: start_node.unwrap_or_default(),
                    coverage: coverage_progress,
                    // A first pass CAN already carry failed retry attempts: a <q:retry> task that
                    // failed on this very activation parks here, and its attempt count must land
                    // with the snapshot or the re-drive would start the budget over.
                    retry_attempts,
                    // Channel-call backoff markers cannot arise on an INITIAL pass (a call's
                    // first failure needs a later timeout fire / poison), but the field is
                    // authoritative-from-executor like `retry_attempts`, so it is carried
                    // rather than assumed empty.
                    retry_backoff,
                    // `on_instance_suspended` has fired synchronously on this thread; the
                    // listener's per-instance seq is now the high-water to persist.
                    audit_seq: self.audit_seq_for(&instance_id),
                    // The migration-stable crypto anchor is the channel's tenant.
                    key_id: binding.tenant().to_string(),
                    encrypt_names: pv.encrypt_names,
                    subjects: pv.subjects,
                };
                let mut records: Vec<AliasRecord> =
                    alias_rows.into_iter().map(|r| r.record).collect();
                // The wait nodes' own park keys: each parked node's declared <q:alias>,
                // evaluated over the suspended context, recorded in the SAME step.
                records.extend(wait_alias_rows(process, &fresh_waits, &variables)?);
                // `ack-mode: on-complete`: the delivering transport handed settle
                // callbacks (`dispatch_deferred`). Registered BEFORE `commit_park` — the
                // park/terminal ordering fix (execution scale-out §2.1): under the shard
                // router, the instance's first relay can land on ANOTHER lane, which can
                // claim, resume and complete it inside a commit-to-registration window —
                // the terminal event would find no registration and the delivery would
                // dangle until the timeout sweep. Registering first closes that window
                // cleanly: before this commit no alias row exists (alias rows ride the
                // step transaction), so nothing can correlate to the instance and no
                // terminal event for it can fire from anywhere but this same dispatch. A
                // failed commit WITHDRAWS the registration below and surfaces as `Err`
                // exactly as before. The arm's invariant is therefore "transport told
                // Deferred ⇒ registered ∧ committed" — the direction the transport
                // actually depends on. Unobservable at `shard-count = 1` (single thread).
                let registered_settle = match &self.deferred_acks {
                    Some(registry) => {
                        self.pending_settle.borrow_mut().take().map(|settle| {
                            if !registry.register(
                                &instance_id,
                                &message.channel,
                                settle.ack,
                                settle.nack,
                            ) {
                                // Unreachable for freshly-minted instance ids; loud if ever.
                                tracing::error!(
                                    channel = %message.channel,
                                    instance = %instance_id,
                                    "deferred-ack registration rejected as duplicate — the \
                                     delivery's settle callbacks were dropped (broker \
                                     redelivery recovers via inbox dedup)"
                                );
                            }
                            instance_id.clone()
                        })
                    }
                    None => None,
                };
                if let Err(commit_error) = bridge
                    .commit_park(
                        deployment,
                        &instance_id,
                        &snapshot,
                        &records,
                        &timer_records(&timer_waits),
                        &emissions,
                    )
                    .await
                {
                    // The park did NOT commit: take the registration back out (callbacks
                    // drop unfired) so a terminal event can never fire for an instance
                    // that was never durably parked. The transport sees `Err` and applies
                    // its own redelivery disposition, exactly as before the inversion.
                    if let (Some(registry), Some(id)) = (&self.deferred_acks, &registered_settle) {
                        registry.withdraw(id);
                    }
                    return Err(commit_error);
                }
                // §6.1: an initial park committed on this lane.
                self.shard_metrics.parked();
                // Concurrency substrate: the parked instance holds a slot. Record it
                // (RUNNING → WAITING) so the per-channel count is replica-coherent and survives
                // a crash (production backs this with the persisted channel_instance table). The
                // dispatcher owns the write because it knows the admitting channel — the executor
                // lifecycle event does not. Freed at the terminal step (see drive_relay /
                // fire_timer).
                //
                // Only a CAPPED channel is counted (as the synchronous-chain path below already
                // does): the count is read solely by the `max-concurrent-instances` admission
                // check, so the rows of an uncapped channel are never read, and writing them
                // cost three durable commits per instance. The later status updates and the
                // terminal delete then match no row and commit nothing.
                if let (Some(store), Some(_)) =
                    (&self.concurrency, binding.max_concurrent_instances)
                {
                    store
                        .record_started(deployment, &instance_id, &message.channel)
                        .await;
                    store.record_suspended(deployment, &instance_id).await;
                }
                // The parked instance keeps the pool slot its start took.
                self.bind_admission(&instance_id).await;
                // A plain suspended inbound has no synchronous reply body — the transport gets an
                // accept; any reply flows later on resume+complete. A respond-and-continue park
                // carries the flushed reply (`reply`, built above).
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: serde_json::json!({}),
                    reply,
                })
            }
        }
    }

    /// A message on a channel named by a `session-end: on-message`: release the session of the
    /// instance it correlates to, then deliver it to a subscribing wait as usual. A message no
    /// wait takes — or one that correlates to no live instance — is ACCEPTED (the release, if
    /// any, was its whole purpose) instead of being rejected as uncorrelatable.
    async fn end_session_on_message(
        &self,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        decoded: &Option<DecodeResult>,
        message: &InboundMessage,
    ) -> Result<DispatchOutcome, Diagnostic> {
        let call = self.session_call_key(binding, message);
        let released = self
            .release_correlated_session(binding, deployment, decoded, message, call.as_deref())
            .await;
        let accepted = |instance_id: Option<String>| DispatchOutcome::Completed {
            instance_id: instance_id.unwrap_or_default(),
            outputs: serde_json::json!({}),
            reply: None,
        };
        match self
            .resolve_relay(binding, deployment, decoded, message)
            .await
        {
            Ok(Some(outcome)) => {
                // The wait's own correlation may name the instance where the session process's
                // alias did not: release it too (idempotent).
                let resumed = match &outcome {
                    DispatchOutcome::Completed { instance_id, .. } if !instance_id.is_empty() => {
                        Some(instance_id.clone())
                    }
                    DispatchOutcome::Handoff { resolved, .. } => Some(resolved.instance_id.clone()),
                    DispatchOutcome::InlineCalls(handoff) => handoff.instance_id(),
                    _ => None,
                };
                if let Some(instance_id) = resumed {
                    for scope in self.session_scopes(deployment) {
                        self.end_named(&scope, &instance_id, call.as_deref()).await;
                    }
                }
                Ok(outcome)
            }
            Ok(None) => Ok(accepted(released)),
            Err(d) if d.code == codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND => {
                Ok(accepted(released))
            }
            Err(d) => Err(d),
        }
    }

    /// Correlate a session-ending message to the instance it ends, through the `<q:alias
    /// onConflict="correlate">` of the processes that START on the session channel, evaluated
    /// against the message (its `event.*` view, and its decoded payload under the start event's
    /// `<q:source name>`). Looks at stored instances and at in-flight synchronous chains, in the
    /// live scope and then the DRAINING ones. Releases — the one `call` when the message names
    /// one, the whole session otherwise — and returns the first instance found.
    async fn release_correlated_session(
        &self,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        decoded: &Option<DecodeResult>,
        message: &InboundMessage,
        call: Option<&str>,
    ) -> Option<String> {
        let sessions = self
            .session_end_targets
            .get(&(binding.namespace.module_key(), message.channel.clone()))?;
        let payload = decoded
            .as_ref()
            .filter(|d| d.outcome != sutra_codec_spi::DecodeOutcome::Fatal)
            .map(|d| {
                d.payload
                    .as_ref()
                    .map(crate::intake::payload_view)
                    .unwrap_or(FeelValue::Null)
            });
        for scope in self.session_scopes(deployment) {
            for session_channel in sessions {
                for (process, start_id) in self.processes.starts_on_channel(&scope, session_channel)
                {
                    let mut variables = project_variables(message);
                    if let Some(payload) = &payload {
                        let name = process
                            .bindings_for(&start_id)
                            .source()
                            .map(|source| source.name.clone())
                            .unwrap_or_else(|| "payload".to_string());
                        variables.insert(name, payload.clone());
                    }
                    let Some((alias_name, alias_value)) =
                        correlation_for(&process, &variables, None)
                    else {
                        continue;
                    };
                    let stored = match &self.bridge {
                        Some(bridge) => bridge
                            .find_live_alias(&scope, &alias_name, &alias_value)
                            .await
                            .ok()
                            .flatten(),
                        None => None,
                    };
                    let instance_id = stored.or_else(|| {
                        self.aliases
                            .as_ref()
                            .and_then(|store| store.find_live(&scope, &alias_name, &alias_value))
                    });
                    if let Some(instance_id) = instance_id {
                        self.end_named(&scope, &instance_id, call).await;
                        return Some(instance_id);
                    }
                }
            }
        }
        None
    }

    /// Resolve + drive a channel-delivered relay, or `Ok(None)` when the inbound is
    /// not a relay (no wait node subscribes to the channel / no bridge wired). Runs the
    /// relay through the SAME two-tier intake as an activation (validators against the wait
    /// node's `<q:source>`; a FATAL relay rejects here and the parked instance stays parked
    /// — the wait is the safe state), correlates via the process's
    /// `<q:alias onConflict="correlate">` through the alias index, and resumes:
    /// replay-skipping-completed to the next quiescent point. COMPLETED → terminal step
    /// (delete + resolve waits + retire aliases); SUSPENDED again → re-park step.
    async fn resolve_relay(
        &self,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        decoded: &Option<DecodeResult>,
        message: &InboundMessage,
    ) -> Result<Option<DispatchOutcome>, Diagnostic> {
        let Some(bridge) = self.bridge.clone() else {
            return Ok(None);
        };
        // Scope order: the channel's LIVE deployment first, then the DRAINING
        // prior deployments (most recently drained first). An instance parked under a
        // deployment that has since been flipped away stays resumable — its alias rows
        // and snapshot are deployment-scoped, so correlation must look where it parked.
        let mut scopes: Vec<DeploymentId> = vec![deployment.clone()];
        for prior in &self.prior_deployments {
            if !scopes.contains(prior) {
                scopes.push(prior.clone());
            }
        }
        let mut first_failure: Option<Diagnostic> = None;
        for scope in &scopes {
            match self
                .resolve_relay_in(&bridge, binding, scope, decoded, message)
                .await?
            {
                RelayResolution::NoTargets => {}
                RelayResolution::NotCorrelated(diagnostic) => {
                    // Remember the primary scope's diagnostic — it names the alias the
                    // operator should look for — and keep trying older scopes.
                    first_failure.get_or_insert(diagnostic);
                }
                RelayResolution::Resumed(outcome) => return Ok(Some(outcome)),
                // Surface the handoff to the router side of the handle, which re-enqueues
                // it on the owner shard — never executed here. Dead at `shard-count = 1`.
                RelayResolution::Handoff(resolved) => {
                    return Ok(Some(DispatchOutcome::Handoff {
                        shard: crate::http::shard_index_of(&resolved.instance_id, self.shard.count),
                        resolved,
                    }))
                }
            }
        }
        match first_failure {
            // Some scope had a subscribing wait node but nothing correlated — the
            // fail-closed posture this path has always had.
            Some(diagnostic) => Err(diagnostic),
            // No wait node subscribes to this channel anywhere — not a relay.
            None => Ok(None),
        }
    }

    /// One correlation attempt inside a single deployment scope — the body of the
    /// relay path, with "no live instance" reported as data (so the caller can fall back
    /// to a DRAINING scope) instead of an error.
    async fn resolve_relay_in(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        binding: &ChannelBinding,
        deployment: &DeploymentId,
        decoded: &Option<DecodeResult>,
        message: &InboundMessage,
    ) -> Result<RelayResolution, Diagnostic> {
        // Same selector context the start-event path builds, and the same fatal-decode posture:
        // a codec that could not parse cannot classify, so an unparseable message selects on
        // channel alone rather than having every predicated wait node answer false.
        let relay_selector_ctx = if decoded
            .as_ref()
            .map(|d| d.outcome == sutra_codec_spi::DecodeOutcome::Fatal)
            .unwrap_or(false)
        {
            sutra_feel::FeelContext::new()
        } else {
            crate::intake::selector_context(
                decoded.as_ref(),
                decoded.as_ref().and_then(|d| d.message_type.as_deref()),
            )
        };
        let targets =
            self.processes
                .resolve_relay_targets(deployment, &message.channel, &relay_selector_ctx);
        let Some(target) = targets.first() else {
            return Ok(RelayResolution::NoTargets);
        };
        let process = &target.process;

        // Two-tier intake validation of the relayed payload against the wait node's
        // <q:source> (uniform with a start event). FATAL ⇒ reject; nothing advances.
        let relay_vars =
            self.relay_intake_variables(process, &target.wait_node_id, binding, decoded, message)?;

        // The correlation the relay carries — the WAIT NODE's own declared <q:alias>
        // first (the channel-call park key), then the process-level
        // start-event `onConflict=correlate` alias (the approval-hold pattern).
        let Some((alias_name, alias_value)) =
            correlation_for_wait_node(process, &target.wait_node_id, &relay_vars).or_else(|| {
                let relay_payload = process
                    .bindings_for(&target.wait_node_id)
                    .sources
                    .iter()
                    .find(|source| source.channel == message.channel)
                    .map(|source| source.name.as_str());
                correlation_for(process, &relay_vars, relay_payload)
            })
        else {
            return Ok(RelayResolution::NotCorrelated(Diagnostic::error(
                codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND,
                format!(
                    "Relay on channel '{}' cannot correlate: process '{}' declares no \
                     <q:alias onConflict=\"correlate\"> whose expression resolves against \
                     the relayed payload — nothing identifies which waiting instance to \
                     resume.",
                    message.channel, process.id
                ),
            )));
        };
        let Some(instance_id) = bridge
            .find_live_alias(deployment, &alias_name, &alias_value)
            .await?
        else {
            return Ok(RelayResolution::NotCorrelated(
                Diagnostic::error(
                    codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND,
                    format!(
                        "No live instance carries alias {alias_name}={alias_value} for \
                     deployment {}; the relay cannot be correlated to a waiting instance.",
                        deployment.value()
                    ),
                )
                .with_attribute(WAIT_PENDING_ATTR, "true"),
            ));
        };

        // Shard routing seam (execution scale-out §1.1): correlation just named the
        // instance. When its lane is another shard's, answer the router with the resolved
        // resume instead of executing here — the owner shard re-runs the race-safe tail
        // only; decode, validation and correlation FEEL are deterministic over the
        // delivery and are not repeated. DEAD CODE at the default `shard-count = 1`: the
        // single lane owns every id.
        if !self.shard.owns(&instance_id) {
            // §6.1: resolved here, owned elsewhere — the router re-enqueues on the owner.
            self.shard_metrics.handed_off();
            return Ok(RelayResolution::Handoff(Box::new(ResolvedResume {
                deployment: deployment.clone(),
                instance_id,
                wait_node_id: target.wait_node_id.clone(),
                variables: relay_vars,
                alias_name,
                alias_value,
                channel: message.channel.clone(),
                labels: binding.labels(),
                traceparent: inbound_traceparent(message),
                reply_to: Some(Box::new(RelayReplyContext {
                    binding: binding.clone(),
                    message: message.clone(),
                })),
                // Filled by `hand_over_dedup` once the dispatch returns the handoff.
                pending_dedup: Vec::new(),
            })));
        }
        let outcome = self
            .resume_correlated_claimed(
                bridge,
                deployment,
                &instance_id,
                &target.wait_node_id,
                relay_vars,
                &alias_name,
                &alias_value,
                &message.channel,
                binding.labels(),
                inbound_traceparent(message),
                Some(RelayReplyContext {
                    binding: binding.clone(),
                    message: message.clone(),
                }),
            )
            .await?;
        Ok(RelayResolution::Resumed(outcome))
    }

    /// The RACE-SAFE tail of a correlated relay resume: claim → load → failed/terminal/
    /// suspended guards → pin resolution → resume → step commit. Shared VERBATIM by the
    /// arrival-shard path ([`Self::resolve_relay_in`]) and the handoff path
    /// ([`Self::resume_resolved`]) — everything before this point (decode, intake
    /// validation, correlation FEEL, the alias lookup) is deterministic over the delivery
    /// and never re-run; everything from the claim on MUST re-run wherever the resume
    /// finally executes.
    #[allow(clippy::too_many_arguments)]
    async fn resume_correlated_claimed(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        deployment: &DeploymentId,
        instance_id: &str,
        wait_node_id: &str,
        relay_vars: Variables,
        alias_name: &str,
        alias_value: &str,
        channel: &str,
        labels: BTreeMap<String, String>,
        traceparent: Option<String>,
        reply_to: Option<RelayReplyContext>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        // ---- instance ownership: claim BEFORE rehydrating ------------------------------
        // Correlation named a live instance; take ownership of it before a single byte of
        // its snapshot is read, so a winner reads a frontier no loser can still be moving.
        // Claiming after the load would leave the classic read-then-act window: two
        // replicas both load the same frontier, both execute, both commit a step.
        //
        // Heartbeat: NOT wired, deliberately. A step here runs between two quiescent points
        // — synchronous executor work plus one local transaction, milliseconds to low
        // seconds — while `sutra.instance.claim-timeout` defaults to PT5M. The claim (and
        // its re-entrant refresh) stamps `last_heartbeat_at` at every claim, so the sweeper
        // can only reclaim an instance whose owner has been silent for two orders of
        // magnitude longer than a step takes. A long-running step (a future async/external
        // task that holds the instance across a network call) is what would make a
        // mid-step heartbeat load-bearing; until such a step exists, the timeout margin IS
        // the liveness signal.
        if let Some(held) = self.inline_held_guard(instance_id, channel) {
            return Err(held);
        }
        match bridge.claim_instance(deployment, instance_id).await? {
            InstanceClaimOutcome::Granted => {}
            InstanceClaimOutcome::HeldByOther => {
                // The CAS matched no row: either a live claim stands, or the instance
                // completed between the alias hit and the claim. Only the first is
                // retryable — re-read to tell them apart, and let a vanished instance fall
                // through to the not-found posture below.
                if bridge.load(deployment, instance_id).await?.is_some() {
                    // §6.1 / §4: the relay-path claim bounce — the mis-route alarm reads
                    // near zero at a correct N>1 rollout outside genuine replica contention.
                    self.shard_metrics.claim_bounced_relay();
                    return Err(Diagnostic::error(
                        codes::RUNTIME_RESUME_CLAIM_HELD,
                        format!(
                            "Instance {instance_id} is claimed by another replica; this relay \
                             on channel '{channel}' refuses to resume it concurrently. Nothing \
                             was executed — the delivery is retry-safe and requeued.",
                        ),
                    )
                    .with_attribute("instanceId", instance_id)
                    .with_attribute("channel", channel)
                    // Retry-safe by construction (no load, no execution, no commit), so the
                    // transport requeues for redelivery under the broker's own backoff —
                    // the same disposition an at-capacity/idempotent-failure bounce uses.
                    .with_attribute(ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE));
                }
            }
        }
        // The claim is now held. Everything below runs inside a panic-catching wrapper so
        // EVERY exit that did not release in-transaction — an `Err`, a DRAINING-scope
        // success (step committed under a different key), or a panic the actor loop will
        // catch — hands the claim back before this function returns (the async replacement
        // for the old drop-guard; the release itself must be awaited, which `Drop` cannot).
        let released_in_tx = std::cell::Cell::new(false);
        let body = async {
            // Rehydrate: load + peek. The instance's OWN pinned deployment is the resolution
            // key; the channel's deployment is authoritative when no readable pin exists.
            let Some(snapshot) = bridge.load(deployment, instance_id).await? else {
                return Err(Diagnostic::error(
                    codes::RUNTIME_RESUME_INSTANCE_NOT_FOUND,
                    format!(
                        "No persisted instance {instance_id} exists for deployment {}; nothing \
                     to resume.",
                        deployment.value()
                    ),
                ));
            };
            // Fail closed on a durably FAILED instance BEFORE the generic not-suspended answer, so the
            // operator gets the specific "needs a human" verdict rather than "not parked".
            if let Some(failed) = Self::instance_failed_guard(&snapshot, instance_id) {
                return Err(failed);
            }
            // …and on an instance that already FINISHED, whose row terminal retention now keeps
            // around. Same code as the generic answer below (this IS "not suspended"), better words.
            if let Some(terminal) = Self::instance_terminal_guard(&snapshot, instance_id) {
                return Err(terminal);
            }
            if !snapshot.suspended {
                return Err(Diagnostic::error(
                    codes::RUNTIME_RESUME_NOT_SUSPENDED,
                    format!(
                        "Instance {instance_id} is in state {}, not SUSPENDED; resume requires \
                     a parked instance.",
                        snapshot.status
                    ),
                )
                .with_attribute(WAIT_PENDING_ATTR, "true"));
            }
            // A relay for a channel-call node sitting in a RETRY BACKOFF window: the response
            // belongs to a DEAD attempt (its timeout fired, or its delivery poisoned, and the
            // failure already consumed a slot of the task's <q:retry> budget). Fail CLOSED —
            // resuming here would swallow the payload and strand the instance (the replay
            // would re-park the node while this commit resolved its pending backoff timer,
            // leaving nothing to ever wake it). The alias row deliberately stays live
            // (instance-scoped, exactly like the FAILED posture), so a late response gets
            // THIS honest verdict instead of a "no live instance" miss; once the backoff
            // re-drive re-emits the request, the marker is gone and the counterpart's answer
            // to the retry resumes the instance normally.
            if let Some(parked_code) = snapshot.retry_backoff.get(wait_node_id) {
                return Err(Diagnostic::error(
                    codes::DISPATCH_CHANNEL_CALL_RETRY_PENDING,
                    format!(
                        "Instance {instance_id} is in a <q:retry> backoff window on \
                         channel-call task '{wait_node_id}' (the attempt failed with \
                         {parked_code}); this response arrived for the DEAD attempt and is \
                         refused — the re-drive will re-issue the request.",
                    ),
                )
                .with_attribute("instanceId", instance_id)
                .with_attribute("nodeId", wait_node_id));
            }
            // The instance's OWN pin decides which GRAPH the resume replays against, and the
            // pin is resolved FAIL-CLOSED — exactly the posture the timer path has always had
            // (`fire_timer`, `RESOLVE_MODULE_NOT_FOUND`). A resume is replay-skipping-completed
            // against node ids, so silently falling back to the currently-active definition
            // would let a hot-deploy migrate an in-flight instance onto a graph it never ran
            // on (renamed/reordered nodes replay as fresh work). Both failures below leave the
            // parked instance completely untouched, so the inbound is safe to redeliver once
            // the pinned deployment is registered again (rollback, or a restart that
            // re-planned the DRAINING tail from the deployment archive).
            let pinned = if snapshot.deployment_id.trim().is_empty() {
                deployment.clone()
            } else {
                DeploymentId::of(&snapshot.deployment_id).map_err(|e| {
                    Diagnostic::error(
                        codes::RUNTIME_RESUME_PIN_UNRESOLVABLE,
                        format!(
                            "Instance {instance_id} carries an unreadable pinned deployment \
                         '{}' ({e}); the relay refuses to resume it against deployment {} \
                         rather than run it on a different definition.",
                            snapshot.deployment_id,
                            deployment.value()
                        ),
                    )
                })?
            };
            let Some(resume_process) = self.processes.find_in_module(&pinned, &snapshot.process_id)
            else {
                return Err(Diagnostic::error(
                    codes::RESOLVE_MODULE_NOT_FOUND,
                    format!(
                        "relay resume of instance {instance_id} names process '{}', which is not \
                     deployed under its pinned deployment {} — that definition is no longer \
                     registered (retired, or not re-planned into the DRAINING tail); the relay \
                     fails closed instead of resuming on the active definition",
                        snapshot.process_id,
                        pinned.value()
                    ),
                ));
            };

            // Several wait nodes of one process may take messages from the same channel (a
            // hang-up awaited while listening, and again after asking for one). Target
            // resolution names the FIRST subscriber in model order; the relay belongs to the
            // one this instance is actually parked at.
            let (rebound_node, relay_vars) = rebind_relay_to_waiting(
                Some(&resume_process),
                &snapshot.waiting_nodes,
                wait_node_id,
                channel,
                relay_vars,
            );
            let wait_node_id: &str = &rebound_node;
            // (After the rebind above, so a channel shared by several wait nodes is judged against
            // the node this instance is actually parked at.)
            // The relay must name a wait the instance is parked at NOW. A relay for a node the
            // token already left — a late answer to an earlier loop iteration, a duplicate of a
            // relay that already resumed it — would otherwise be taken as satisfying a wait that
            // no longer exists and run that node's continuation a second time. Refused before
            // anything executes; the parked instance is untouched.
            if !snapshot.waiting_nodes.iter().any(|n| n == wait_node_id) {
                return Err(Diagnostic::error(
                    codes::RUNTIME_RESUME_NOT_WAITING,
                    format!(
                        "Instance {instance_id} is not waiting at '{wait_node_id}' (it is parked \
                         at {:?}); this relay on channel '{channel}' arrived for a wait the \
                         instance already left and is refused — nothing was executed.",
                        snapshot.waiting_nodes
                    ),
                )
                .with_attribute("instanceId", instance_id)
                .with_attribute("nodeId", wait_node_id)
                .with_attribute(WAIT_PENDING_ATTR, "true"));
            }
            // Restored variables come back TYPED (the snapshot value model); the relay's variables
            // are merged in by resume() — relay wins on collision. Nothing is re-parsed here: a
            // string-to-number coercion on this path would double-convert a value the store already
            // handed back as a number, and would guess at one it deliberately handed back as text.
            let mut prior = Variables::new();
            for (name, value) in &snapshot.variables {
                prior.insert(name.clone(), value.clone());
            }
            let start_node = if snapshot.start_node.trim().is_empty() {
                None
            } else {
                Some(snapshot.start_node.as_str())
            };
            // A channel-call task with a DECLARED output mapping: the
            // response payload lands per the mapping and ONLY per the mapping (un-mapped
            // response variables drop). No declared mapping ⇒ the full relay context merges
            // (today's behavior, backward-compatible).
            let relay_vars =
                match channel_call_output_mapping(&resume_process, wait_node_id, &relay_vars)? {
                    Some(mapped) => mapped,
                    None => relay_vars,
                };
            // Seed the audit listener's per-instance seq from the persisted high-water BEFORE
            // resume fires `on_instance_resumed` (which emits the next audit event), so the seq
            // continues at `audit_seq + 1` instead of restarting at 1 and colliding with prior rows.
            self.seed_audit(instance_id, snapshot.audit_seq);
            // The relay leg's cross-process coverage correlation dims for any fragment
            // this resume completes: the inbound trace-id + the `<q:alias>` value that correlated this
            // relay to the waiting instance (the per-hop business key). Best-effort.
            let correlation = CoverageCorrelation {
                trace_id: traceparent.clone(),
                business_key: Some(alias_value.to_string()),
            };
            let result = match self
                .executor
                .resume_correlated(
                    &resume_process,
                    instance_id,
                    &snapshot.completed_nodes,
                    prior,
                    wait_node_id,
                    &relay_vars,
                    pinned.clone(),
                    labels.clone(),
                    start_node,
                    &snapshot.waiting_nodes,
                    &snapshot.coverage,
                    &snapshot.retry_attempts,
                    &snapshot.retry_backoff,
                    correlation,
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    // The resumed step failed FATALLY. Persist the durable FAILED marker (the instance
                    // used to be left silently sitting at its old frontier), then surface the cause.
                    let d = e.to_diagnostic();
                    let cause = Diagnostic::error(&d.code, d.message.clone());
                    self.mark_instance_failed(bridge, &pinned, instance_id, &snapshot, &cause)
                        .await;
                    return Err(cause);
                }
            };

            // The labels the executor pass ran under, for a failure step's listener event.
            let failure_labels = labels.clone();
            let outcome = self
                .commit_resume_outcome(
                    bridge,
                    labels,
                    traceparent,
                    &resume_process,
                    &snapshot,
                    &pinned,
                    deployment,
                    wait_node_id,
                    result,
                    reply_to,
                    Some((alias_name.to_string(), alias_value.to_string())),
                )
                .await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(e) => {
                    return Err(self
                        .fail_refused_resume(
                            bridge,
                            &resume_process,
                            &pinned,
                            instance_id,
                            &snapshot,
                            &failure_labels,
                            e,
                        )
                        .await)
                }
            };
            // The step committed under `pinned`; when that is the key we claimed under (the
            // ordinary case — a DRAINING-scope resume is the exception), the release already rode
            // that transaction and there is nothing left to hand back. A synchronous chain took
            // the claim with it: the chain's final commit (or its failure) hands it back.
            if pinned == *deployment || matches!(outcome, DispatchOutcome::InlineCalls(_)) {
                released_in_tx.set(true);
            }
            Ok(outcome)
        };
        match catch_unwind_completion(body).await {
            Ok(outcome) => {
                if !released_in_tx.get() {
                    release_claim_best_effort(bridge, deployment, instance_id).await;
                }
                outcome
            }
            Err(panic) => {
                // Same release the old guard performed during the unwind, then let the
                // panic continue to the actor loop's catch (identical payload).
                release_claim_best_effort(bridge, deployment, instance_id).await;
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Execute a relay HANDOFF's resolved resume — the owner-shard entry the router calls
    /// via `EngineRequest::ResumeResolved`. Exactly the race-safe tail
    /// ([`Self::resume_correlated_claimed`]); a re-resolution is impossible (the instance
    /// id is fixed), so this can never answer `Handoff` again — the at-most-one-hop rule
    /// holds by construction. DEAD at the default `shard-count = 1` (no dispatch ever
    /// answers `Handoff`, so nothing enqueues this).
    pub async fn resume_resolved(
        &self,
        resolved: &ResolvedResume,
    ) -> Result<DispatchOutcome, Diagnostic> {
        let Some(bridge) = self.bridge.clone() else {
            return Err(Diagnostic::error(
                codes::INBOUND_PERSISTENCE_REQUIRED,
                "a resolved-resume handoff requires the persistence bridge — none is configured",
            ));
        };
        // The router side awaits the handoff, so the resume may run a chain.
        self.begin_dispatch(true);
        if !resolved.pending_dedup.is_empty() {
            if let Some(inbox) = &self.inbox {
                inbox.restore_pending(resolved.pending_dedup.clone());
                self.dedup_touched.set(true);
            }
        }
        let result = self
            .resume_correlated_claimed(
                &bridge,
                &resolved.deployment,
                &resolved.instance_id,
                &resolved.wait_node_id,
                resolved.variables.clone(),
                &resolved.alias_name,
                &resolved.alias_value,
                &resolved.channel,
                resolved.labels.clone(),
                resolved.traceparent.clone(),
                resolved.reply_to.as_deref().cloned(),
            )
            .await;
        self.settle_dedup(result.is_ok()).await;
        result
    }

    /// Persist a resume pass's quiescent point (shared by the relay path and the timer
    /// fire): COMPLETED ⇒ terminal step (delete + resolve every wait + retire aliases +
    /// enqueue the step's emissions); SUSPENDED ⇒ re-park step (snapshot + resolved waits
    /// incl. cancelled timer rows + fresh wait/timer/alias rows + emissions) — each ONE
    /// transaction.
    ///
    /// The error says whether the step can EVER commit (see [`ResumeCommitError`]): a step the
    /// engine refuses to commit is the caller's cue to fail the instance, while a store error
    /// leaves the instance untouched for the caller's retry.
    #[allow(clippy::too_many_arguments)]
    async fn commit_resume_outcome(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        labels: BTreeMap<String, String>,
        traceparent: Option<String>,
        process: &ProcessDefinition,
        prior_snapshot: &SuspendedInstance,
        pinned: &DeploymentId,
        claim_scope: &DeploymentId,
        satisfied_node: &str,
        result: StatefulExecResult,
        reply_to: Option<RelayReplyContext>,
        relay_alias: Option<(String, String)>,
    ) -> Result<DispatchOutcome, ResumeCommitError> {
        use ResumeCommitError::{Refused, Transient};
        match result {
            StatefulExecResult::Completed {
                instance_id,
                outputs,
                visited_nodes,
            } => {
                let mut drained = self.emissions.drain();
                self.validate_emissions(
                    &module_key_of_labels(&labels),
                    pinned,
                    "",
                    process,
                    &mut drained,
                )
                .map_err(Refused)?;
                let emissions = to_outbox_emissions(drained, labels, traceparent);
                // Terminal step: no longer in-flight — drop the row, resolve every wait
                // point, retire the aliases, enqueue the step's emissions (a completed
                // instance never lingers; its sends never miss the commit).
                bridge
                    .commit_complete(pinned, &instance_id, &emissions)
                    .await
                    .map_err(Transient)?;
                // §6.1: a resume pass committed (terminal) on this lane.
                self.shard_metrics.resumed();
                // The instance is terminal — free its channel_instance slot and its pool slots.
                if let Some(store) = &self.concurrency {
                    store.record_terminal(pinned, &instance_id).await;
                }
                self.release_pool_slots(pinned, &instance_id).await;
                // A relay answers with the `<q:reply>` THIS pass reached, rendered against its
                // own delivery; outputs stay empty, never the resumed instance's raw variable
                // map (the drive-relay contract).
                let reply = match &reply_to {
                    Some(to) => self
                        .relay_reply(
                            to,
                            process,
                            prior_snapshot,
                            &instance_id,
                            outputs,
                            visited_nodes,
                            false,
                        )
                        // The terminal step has committed: the instance is complete, so a
                        // reply that fails to render is the caller's error alone, never a
                        // reason to fail it.
                        .map_err(Transient)?,
                    None => None,
                };
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: serde_json::json!({}),
                    reply,
                })
            }
            StatefulExecResult::Suspended {
                instance_id,
                waiting_nodes,
                completed_nodes,
                variables,
                start_node,
                timer_waits,
                // A continue-reply reached during a resumed tail: a relay's caller gets it as the
                // response (below); a timer fire has no caller, so only the reply body is lost.
                detached_reply,
                coverage_progress,
                retry_attempts,
                retry_backoff,
                fresh_waits,
            } => {
                // The re-park step carries the emissions collected up to this
                // quiescent point.
                let mut drained = self.emissions.drain();
                self.validate_emissions(
                    &module_key_of_labels(&labels),
                    pinned,
                    "",
                    process,
                    &mut drained,
                )
                .map_err(Refused)?;
                // Synchronous calls reached by a resume a caller drives (a relay, a timer fire):
                // the chain keeps the claim, makes the calls inline and ends with the one
                // commit that fits the stored row.
                let sync_sites = if detached_reply {
                    Vec::new()
                } else {
                    self.sync_call_sites(process, pinned, &fresh_waits, &variables)
                };
                if !sync_sites.is_empty() {
                    let pinned_process = self.processes.find_in_module(pinned, &process.id);
                    match pinned_process {
                        Some(pinned_process)
                            if self.inline_capable.get()
                                && sync_sites.iter().all(|site| site.caller.is_some()) =>
                        {
                            let chain = self.stored_chain(
                                pinned_process,
                                prior_snapshot,
                                pinned,
                                claim_scope,
                                satisfied_node,
                                labels,
                                traceparent,
                                reply_to,
                                StoredPass {
                                    instance_id,
                                    waiting_nodes,
                                    completed_nodes,
                                    variables,
                                    start_node,
                                    timer_waits,
                                    coverage_progress,
                                    retry_attempts,
                                    retry_backoff,
                                    fresh_waits,
                                    relay_alias,
                                },
                            );
                            // The chain owns its failure handling (`abort_chain`).
                            return self
                                .launch_inline_chain(chain, drained, sync_sites)
                                .await
                                .map_err(Transient);
                        }
                        _ => self.warn_sync_degraded(
                            process,
                            &sync_sites,
                            if self.inline_capable.get() {
                                "no inline caller serves the channel's scheme"
                            } else {
                                "this pass cannot carry a chain (a broadcast fan-out or a poison wake)"
                            },
                        ),
                    }
                }
                // A relayed turn that parks answers with the reply it reached: a
                // respond-and-continue receipt (the tail then runs on its due-now timer), or the
                // last destination-less `<q:reply>` the turn rendered.
                let early_reply = match &reply_to {
                    Some(to) => self
                        .relay_reply(
                            to,
                            process,
                            prior_snapshot,
                            &instance_id,
                            variables.clone(),
                            completed_nodes.iter().cloned().collect(),
                            !detached_reply,
                        )
                        .map_err(Refused)?,
                    None => None,
                };
                let emissions = to_outbox_emissions(drained, labels, traceparent);
                let pv = persisted_variables(
                    &process.declared_variables,
                    persisted_variable_values(&variables),
                );
                let new_snapshot = SuspendedInstance {
                    process_id: prior_snapshot.process_id.clone(),
                    deployment_id: pinned.value().to_string(),
                    status: "SUSPENDED".to_string(),
                    suspended: true,
                    completed_nodes,
                    variables: pv.kept,
                    sensitive: pv.sensitive,
                    waiting_nodes: waiting_nodes.clone(),
                    start_node: start_node.unwrap_or_default(),
                    coverage: coverage_progress,
                    // AUTHORITATIVE, not merged with the prior snapshot: the executor was seeded
                    // from `prior_snapshot.retry_attempts` at the top of this pass, so what it
                    // hands back already carries every branch's count — including the increment
                    // this pass made, and including the REMOVAL of a node whose retried task
                    // finally succeeded.
                    retry_attempts,
                    // Same authority rule: seeded from the prior snapshot, minus the marker a
                    // re-drive consumed, plus the marker a fresh backoff park set. The bridge
                    // additionally keys on the marked nodes to WITHDRAW the dead attempt's
                    // outbox rows inside this same step transaction.
                    retry_backoff,
                    // The resume pass re-parked; carry the fresh seq high-water forward.
                    audit_seq: self.audit_seq_for(&instance_id),
                    // Preserve the ORIGINAL keyId (the migration-stable tenant anchor) the
                    // instance was first parked under; the resume path has no channel binding, so
                    // it flows from the loaded prior snapshot (stable across re-parks).
                    key_id: prior_snapshot.key_id.clone(),
                    encrypt_names: pv.encrypt_names,
                    subjects: pv.subjects,
                };
                // Waits THIS pass parked fresh record their alias rows (a loop's new iteration
                // included); waits carried over from the prior pass keep the rows their own park
                // step indexed.
                let aliases =
                    wait_alias_rows(process, &fresh_waits, &variables).map_err(Refused)?;
                let resolved = resolved_with_fresh(process, satisfied_node, &fresh_waits);
                let retired = retired_wait_aliases(
                    process,
                    &resolved,
                    prior_snapshot,
                    &waiting_nodes,
                    &fresh_waits,
                    &aliases,
                    relay_alias.as_ref(),
                );
                bridge
                    .commit_repark_retiring(
                        pinned,
                        &instance_id,
                        &new_snapshot,
                        &resolved,
                        &aliases,
                        &timer_records(&timer_waits),
                        &emissions,
                        &retired,
                    )
                    .await
                    .map_err(Transient)?;
                // §6.1: a resume pass committed (re-park) on this lane.
                self.shard_metrics.resumed();
                self.flush_session_releases().await;
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: serde_json::json!({}),
                    reply: early_reply,
                })
            }
        }
    }

    /// Fire one claimed DUE timer-start schedule: MINT an instance of `fire.process_id` at its
    /// timer `<startEvent>`.
    ///
    /// This is the schedule counterpart of an inbound spawn, and it deliberately joins the
    /// inbound pipeline at [`Self::drive_resolved`] — the point where a delivery has finished
    /// being a delivery and has become "this process, this start node, these variables".
    /// Everything downstream of that point is therefore literally the same code: the tenant
    /// quota check, `<q:alias>` materialisation and its uniqueness reject, the stateful park with
    /// its transactional outbox commit, the concurrency gauge, the audit listener chain, the
    /// OTel spans, and the idempotency-gated failure disposition.
    ///
    /// What a timer start does NOT have is an inbound: no channel binding was matched, no bytes
    /// were decoded, no intake validators ran, and — the semantic that matters to an author —
    /// **the instance starts with EMPTY variables**. A schedule carries no payload, so anything
    /// the flow needs it must read for itself (a data task, a store read) rather than expect on
    /// `event.*`. The synthesized [`InboundMessage`] exists only to satisfy the shared
    /// signature's observability/incident fields; its body is empty and its dedup key is not
    /// explicit, so the inbox never suppresses an occurrence.
    ///
    /// [`ScheduledStartOutcome::Stale`] means the row outlived its model (process gone, node
    /// gone, or the start event is no longer timer-triggered) — the poller resolves it.
    pub async fn fire_scheduled_start(
        &self,
        fire: &ScheduledStartFire,
    ) -> Result<ScheduledStartOutcome, Diagnostic> {
        // The schedule poller's task awaits the fire, so it drives a synchronous chain the way
        // an inbound caller does.
        self.begin_dispatch(true);
        let span = tracing::info_span!(
            telemetry::SPAN_DISPATCH,
            channel = SCHEDULE_CHANNEL,
            tenant = %fire.tenant,
            deployment.id = %fire.deployment.value(),
        );
        let _guard = span.enter();

        // ---- resolve the model, fail STALE (never loud) on a model that moved on -----------
        let Some(process) = self
            .processes
            .find_in_module(&fire.deployment, &fire.process_id)
        else {
            return Ok(ScheduledStartOutcome::Stale);
        };
        let still_a_timer_start = matches!(
            process.node(&fire.node_id),
            Ok(Node::StartEvent { timer: Some(_), .. })
        );
        if !still_a_timer_start {
            return Ok(ScheduledStartOutcome::Stale);
        }

        // ---- tenant quota, exactly as an inbound spawn consults it -------------------------
        if let Some(enforcer) = &self.quota_enforcer {
            if let QuotaCheckResult::Denied { reason, detail } = enforcer
                .check_inbound(&fire.tenant, &fire.deployment, SCHEDULE_CHANNEL)
                .await
            {
                return Err(Diagnostic::error(&reason, detail));
            }
        }

        // The synthesized binding: the deployment's own namespace under the reserved schedule
        // channel, schema-less (nothing is decoded). `labels()` off this binding is what stamps
        // tenant/module/version onto every execution event the fire produces.
        let namespace = namespace_of_module_key(&fire.module_key, &fire.tenant);
        let binding = ChannelBinding::new(
            SCHEDULE_CHANNEL,
            namespace,
            fire.deployment.clone(),
            "", // schema-less: a schedule decodes nothing
        );
        let message = InboundMessage {
            tenant: fire.tenant.clone(),
            module_key: fire.module_key.clone(),
            channel: SCHEDULE_CHANNEL.to_string(),
            headers: BTreeMap::new(),
            body: sutra_executor::Sensitive::new(Vec::new()),
            content_type: None,
            // Stable, human-readable occurrence identity for an incident row / log line. NOT an
            // explicit event id: inbox dedup must never suppress an occurrence (the poller's
            // SKIP LOCKED claim plus the row advance is the only dedup a schedule gets).
            idempotency_key: format!("{}:{}@{}", fire.process_id, fire.node_id, fire.due_at),
            explicit_event_id: false,
            received_at: fire.fired_at.clone(),
            cloud_event: None,
        };

        // EMPTY variables — the defining semantic of a schedule-started instance.
        let outcome = self
            .drive_resolved(
                &process,
                &fire.node_id,
                &binding,
                &fire.deployment,
                &message,
                Variables::new(),
                Some(fire.node_id.clone()),
            )
            .await?;
        if let DispatchOutcome::InlineCalls(handoff) = outcome {
            return Ok(ScheduledStartOutcome::InlineCalls(handoff));
        }
        Self::scheduled_outcome_of(outcome, &fire.node_id)
    }

    /// A scheduled start's dispatch outcome → the poller's answer (also applied to the final
    /// outcome of a synchronous chain the start began).
    pub(crate) fn scheduled_outcome_of(
        outcome: DispatchOutcome,
        node_id: &str,
    ) -> Result<ScheduledStartOutcome, Diagnostic> {
        Ok(match outcome {
            DispatchOutcome::Completed { instance_id, .. } => ScheduledStartOutcome::Started {
                // A parked instance reports through the same `Completed` shape as a finished
                // one (the park arm yields it); "completed" here means the dispatch returned a
                // terminal-or-parked instance, which is a successful fire either way.
                completed: true,
                instance_id,
            },
            // A schedule cannot be a duplicate delivery (no explicit event id was offered) and
            // cannot be dead-lettered as an inbound (nothing was consumed) — but both shapes are
            // reachable through the shared path, and neither is a reason to keep re-firing.
            DispatchOutcome::Duplicate => ScheduledStartOutcome::Started {
                instance_id: String::new(),
                completed: true,
            },
            DispatchOutcome::DeadLettered {
                code,
                process_id,
                cause_code,
                detail,
            } => {
                tracing::error!(
                    code,
                    process = %process_id,
                    cause_code,
                    node_id = %node_id,
                    "scheduled start dead-lettered: the non-idempotent process failed"
                );
                return Err(Diagnostic::error(&cause_code, detail));
            }
            // Unreachable: a schedule MINTS an instance (the spawn path) — no relay
            // correlation runs, so a shard handoff cannot be produced; a synchronous chain is
            // taken out before this mapping and its final outcome is never another chain.
            DispatchOutcome::Handoff { .. } | DispatchOutcome::InlineCalls(_) => {
                return Err(Diagnostic::error(
                    codes::RUNTIME_UNEXPECTED,
                    "internal: a scheduled start answered a shard handoff",
                ))
            }
        })
    }

    /// Drive a claimed DUE timer through the resume path. Returns
    /// [`TimerFireOutcome::Stale`] when the row no longer matches a live parked instance
    /// (the poller resolves the row); an executor failure on an UNCAUGHT timeout error
    /// terminates the instance closed (terminal step) and surfaces the diagnostic.
    ///
    /// Ownership: the instance is CLAIMED before it is rehydrated. A claim held by another
    /// replica surfaces as `SUTRA.RUNTIME.RESUME.CLAIM_HELD`, which the poller's failure arm
    /// treats like any other transient failure — the due row is deferred by the poller's
    /// retry backoff and re-fires once the owner has finished its step.
    pub async fn fire_timer(&self, fire: &TimerFire) -> Result<TimerFireOutcome, Diagnostic> {
        let Some(bridge) = self.bridge.clone() else {
            return Err(Diagnostic::error(
                codes::INBOUND_PERSISTENCE_REQUIRED,
                "a timer fire requires the persistence bridge — none is configured",
            ));
        };
        // A timer fire has a driver waiting on it (the poller's task), so it may run a chain.
        self.begin_dispatch(true);
        // A synchronous chain on this lane is driving the instance: defer, like a held claim.
        if let Some(held) = self.inline_held_guard(&fire.instance_id, &fire.node_id) {
            return Err(held);
        }
        // Ownership first, exactly as the relay path does (see the claim/heartbeat rationale
        // there): a fired timer and a correlated relay are the two ways one instance gets
        // resumed, and they can land on different replicas at the same instant.
        match bridge
            .claim_instance(&fire.deployment, &fire.instance_id)
            .await?
        {
            InstanceClaimOutcome::Granted => {}
            InstanceClaimOutcome::HeldByOther => {
                // Row gone ⇒ the timer outlived its instance: STALE, and the poller resolves
                // the row. Row present ⇒ genuine contention: fail with the retry-safe code so
                // the poller DEFERS the due row by its retry backoff and re-fires later
                // (nothing was loaded or executed, so re-firing is a no-op replay).
                return if bridge
                    .load(&fire.deployment, &fire.instance_id)
                    .await?
                    .is_some()
                {
                    // §6.1: the timer-path claim bounce (split from relay in the counter).
                    self.shard_metrics.claim_bounced_timer();
                    Err(Diagnostic::error(
                        codes::RUNTIME_RESUME_CLAIM_HELD,
                        format!(
                            "Instance {} is claimed by another replica; timer node '{}' defers \
                             rather than resuming it concurrently.",
                            fire.instance_id, fire.node_id
                        ),
                    )
                    .with_attribute("instanceId", &fire.instance_id)
                    .with_attribute("nodeId", &fire.node_id))
                } else {
                    Ok(TimerFireOutcome::Stale)
                };
            }
        }
        // The claim is now held. Same async release posture as the relay path (see
        // `resume_correlated_claimed`): every exit below that did not release
        // in-transaction — incl. the uncaught-timeout terminal arm, whose `commit_failed`
        // makes the hand-back a no-op — releases after the wrapped body returns.
        let released_in_tx = std::cell::Cell::new(false);
        let body = async {
            let Some(snapshot) = bridge.load(&fire.deployment, &fire.instance_id).await? else {
                return Ok(TimerFireOutcome::Stale);
            };
            // A durably FAILED instance is never re-driven: fail CLOSED with the specific code rather
            // than the generic Stale (which reads as "nothing to do here"). The failure commit already
            // resolved this instance's wait rows, so reaching this is a race — and the poller keys on
            // the code to resolve the row instead of re-firing it.
            if let Some(failed) = Self::instance_failed_guard(&snapshot, &fire.instance_id) {
                return Err(failed);
            }
            // A FINISHED instance (terminal retention keeps its row now) is STALE, not an error: the
            // terminal step already resolved every one of its wait rows, so reaching here at all is a
            // race, and `Stale` is precisely what tells the poller to resolve the row it is holding
            // and stop. Deliberately NOT the relay path's diagnostic — there is no delivery to reject
            // and nobody to tell, only a timer row to retire. It falls through the `!suspended` arm
            // below; this comment is here because the arm's meaning quietly widened with retention.
            if !snapshot.suspended {
                return Ok(TimerFireOutcome::Stale);
            }
            // Same fail-closed pin resolution as the relay path: an unreadable pin is a
            // structured failure, never a silent fallback onto the timer row's deployment.
            let pinned = if snapshot.deployment_id.trim().is_empty() {
                fire.deployment.clone()
            } else {
                DeploymentId::of(&snapshot.deployment_id).map_err(|e| {
                    Diagnostic::error(
                        codes::RUNTIME_RESUME_PIN_UNRESOLVABLE,
                        format!(
                            "timer fire for instance {} carries an unreadable pinned deployment \
                         '{}' ({e}); the fire refuses to resume it against deployment {}.",
                            fire.instance_id,
                            snapshot.deployment_id,
                            fire.deployment.value()
                        ),
                    )
                })?
            };
            let Some(process) = self.processes.find_in_module(&pinned, &snapshot.process_id) else {
                return Err(Diagnostic::error(
                    codes::RESOLVE_MODULE_NOT_FOUND,
                    format!(
                        "timer fire for instance {} names process '{}', which is not deployed \
                     under {}",
                        fire.instance_id,
                        snapshot.process_id,
                        pinned.value()
                    ),
                ));
            };
            // Frontier check: the timer's HOST (a boundary's attached activity, or the timer
            // catch node itself) must still be parked — otherwise the row is stale.
            let (host, fired_boundary) = match process.node(&fire.node_id) {
                Ok(Node::BoundaryEvent {
                    attached_to_ref, ..
                }) => (attached_to_ref.clone(), true),
                Ok(_) => (fire.node_id.clone(), false),
                Err(_) => return Ok(TimerFireOutcome::Stale),
            };
            if !snapshot.waiting_nodes.iter().any(|n| n == &host) {
                return Ok(TimerFireOutcome::Stale);
            }
            // A timer BOUNDARY whose host sits in a `<q:retry>` BACKOFF window is STALE: the
            // host's attempt already failed (a poison beat this timeout to it), the failure
            // consumed its budget slot, and the pending backoff timer owns the re-drive.
            // Racing a second failure in would double-count one attempt. The park that set
            // the marker resolves the boundary's row in the same transaction, so this arm is
            // only reachable through the claim/poll race — Stale tells the poller to retire
            // the row it is still holding.
            if fired_boundary && snapshot.retry_backoff.contains_key(&host) {
                return Ok(TimerFireOutcome::Stale);
            }

            // Typed restore, same contract as the relay path above.
            let mut prior = Variables::new();
            for (name, value) in &snapshot.variables {
                prior.insert(name.clone(), value.clone());
            }
            let start_node = if snapshot.start_node.trim().is_empty() {
                None
            } else {
                Some(snapshot.start_node.as_str())
            };
            let pinned_fire = TimerFire {
                deployment: pinned.clone(),
                ..fire.clone()
            };
            // Seed the audit listener's per-instance seq from the persisted high-water BEFORE
            // the resume path fires its next audit event (same rationale as the relay resume).
            self.seed_audit(&fire.instance_id, snapshot.audit_seq);
            // Discriminate what this due TIMER row keyed to a non-boundary node MEANS — from
            // durable evidence only, under the instance claim:
            //
            // - a CHANNEL-CALL task with its `sutra.retryWait.<nodeId>` marker set: the
            //   `<q:retry>` BACKOFF came due — the explicit re-drive entry re-runs the park
            //   side-effects (a FRESH request emission, fresh timeout boundary). Explicit
            //   rather than inferred because for a call node the attempt count alone cannot
            //   distinguish this from a mid-retry relay (see `resume_retry_redrive`).
            // - a CHANNEL-CALL task WITHOUT the marker: stale residue — the only writer of a
            //   TIMER row keyed to a call node is a backoff park, and its marker commits in
            //   the same transaction. Resolve the row.
            // - a REGISTERED `<q:retry>` task: the row's node carries a durable attempt count
            //   (a success drops it) and is parked on the frontier (checked above). Whether it
            //   is ALSO in `completed_nodes` says nothing — inside a loop it completed in an
            //   earlier iteration. Rides the relay resume path (`resume_timer` would reject a
            //   non-timer node); the inference is sound because no relay can ever target a
            //   registered task.
            // - a `<q:reply continue>` marker: the respond-and-continue self-resume, node in
            //   `completed_nodes`, replay skips it and runs the tail.
            let call_node = matches!(
                process.node(&fire.node_id),
                Ok(Node::ServiceTask { implementation, .. })
                    if implementation.starts_with(CHANNEL_CALL_PREFIX)
            );
            let retry_redrive = if call_node {
                snapshot.retry_backoff.contains_key(&fire.node_id)
            } else {
                snapshot.retry_attempts.contains_key(&fire.node_id)
            };
            if call_node && !retry_redrive {
                return Ok(TimerFireOutcome::Stale);
            }
            let result = if call_node {
                self.executor
                    .resume_retry_redrive(
                        &process,
                        &fire.instance_id,
                        &snapshot.completed_nodes,
                        prior,
                        &fire.node_id,
                        pinned.clone(),
                        BTreeMap::new(),
                        start_node,
                        &snapshot.waiting_nodes,
                        &snapshot.coverage,
                        &snapshot.retry_attempts,
                        &snapshot.retry_backoff,
                    )
                    .await
            } else if retry_redrive || process.has_continue_reply(&fire.node_id) {
                self.executor
                    .resume(
                        &process,
                        &fire.instance_id,
                        &snapshot.completed_nodes,
                        prior,
                        &fire.node_id,
                        &Variables::new(),
                        pinned.clone(),
                        BTreeMap::new(),
                        start_node,
                        &snapshot.waiting_nodes,
                        &snapshot.coverage,
                        &snapshot.retry_attempts,
                        &snapshot.retry_backoff,
                    )
                    .await
            } else {
                self.executor
                    .resume_timer(
                        &process,
                        &pinned_fire,
                        &snapshot.completed_nodes,
                        prior,
                        BTreeMap::new(),
                        start_node,
                        &snapshot.waiting_nodes,
                        &snapshot.coverage,
                        &snapshot.retry_attempts,
                        &snapshot.retry_backoff,
                    )
                    .await
            };
            let result = match result {
                Ok(r) => r,
                Err(e) => {
                    // An uncaught timeout error kills the instance CLOSED. It used to be DELETED here
                    // (a terminal step) — leaving no trace that a flow died rather than finished.
                    // It now persists as durable FAILED state instead: the same wait-resolution that
                    // stops the due row refiring forever, plus a marker an operator can find. The
                    // diagnostic then surfaces to the poller as before.
                    let d = e.to_diagnostic();
                    let cause = Diagnostic::error(&d.code, d.message.clone());
                    self.mark_instance_failed(
                        &bridge,
                        &pinned,
                        &fire.instance_id,
                        &snapshot,
                        &cause,
                    )
                    .await;
                    return Err(cause);
                }
            };
            let completed = result.is_completed();
            // Timer fires run outside any channel binding; labels ride empty on the step's
            // emissions (authoring labels are observability payload, never identity), and no
            // inbound traceparent exists to bridge.
            let outcome = match self
                .commit_resume_outcome(
                    &bridge,
                    BTreeMap::new(),
                    None,
                    &process,
                    &snapshot,
                    &pinned,
                    &fire.deployment,
                    &fire.node_id,
                    result,
                    None,
                    None,
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(e) => {
                    return Err(self
                        .fail_refused_resume(
                            &bridge,
                            &process,
                            &pinned,
                            &fire.instance_id,
                            &snapshot,
                            &BTreeMap::new(),
                            e,
                        )
                        .await)
                }
            };
            // A synchronous chain carries the claim on; the poller's task drives it to its end.
            if let DispatchOutcome::InlineCalls(handoff) = outcome {
                released_in_tx.set(true);
                return Ok(TimerFireOutcome::InlineCalls(handoff));
            }
            // Released inside the step transaction when the step's key is the one we claimed
            // under (see the relay path's note on the pinned-vs-scope exception).
            if pinned == fire.deployment {
                released_in_tx.set(true);
            }
            Ok(TimerFireOutcome::Resumed {
                instance_id: fire.instance_id.clone(),
                completed,
            })
        };
        match catch_unwind_completion(body).await {
            Ok(outcome) => {
                if !released_in_tx.get() {
                    release_claim_best_effort(&bridge, &fire.deployment, &fire.instance_id).await;
                }
                outcome
            }
            Err(panic) => {
                // Same release the old guard performed during the unwind, then let the
                // panic continue to the actor loop's catch (identical payload).
                release_claim_best_effort(&bridge, &fire.deployment, &fire.instance_id).await;
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Offer a terminally-POISONED channel-call request delivery to the parked task's
    /// `<q:retry>` policy — the outbox dispatcher's wake prompt (F1, failure mode (b)).
    ///
    /// Everything is re-derived from DURABLE FACTS under the instance claim; the prompt
    /// itself is never trusted:
    /// - the instance must be SUSPENDED (not FAILED/terminal) and parked ON the named node,
    /// - the node must be a channel-call task DECLARING a `<q:retry>` policy (a policy-less
    ///   call keeps its pre-F1 posture: parked until its timeout boundary fires — the
    ///   incident already recorded the poison),
    /// - the node must NOT already sit in a backoff window (the timeout beat the poison to
    ///   the failure; driving it again would double-count one attempt),
    /// - and a poisoned outbox row must EXIST for this exact `(instance, node)`.
    ///
    /// Any unmet condition is [`ChannelCallPoisonOutcome::NotApplicable`] — a no-op, never an
    /// error. The prompt is best-effort by design: a lost or refused wake is recovered by the
    /// call's `<q:timeout>` boundary (the loader guarantees one exists), which reaches the
    /// same retry policy through [`TokenExecutor::resume_timer`]'s path. On a genuine
    /// failure delivery: budget left ⇒ the backoff park commits (resolving the attempt's
    /// wait/timeout rows and WITHDRAWING its outbox rows — the poisoned row among them);
    /// budget spent / non-retryable ⇒ the durable FAILED snapshot commits, exactly as an
    /// exhausted registered-task retry.
    pub async fn fail_channel_call(
        &self,
        fire: &ChannelCallPoisonFire,
    ) -> Result<ChannelCallPoisonOutcome, Diagnostic> {
        let Some(bridge) = self.bridge.clone() else {
            // No persistence ⇒ no outbox dispatcher ⇒ no poison can exist. Unreachable, but
            // a no-op is the only honest answer.
            return Ok(ChannelCallPoisonOutcome::NotApplicable);
        };
        // The wake's outcome type has no room for a chain: its synchronous calls degrade.
        self.begin_dispatch(false);
        if self.inline_held.borrow().contains(&fire.instance_id) {
            return Ok(ChannelCallPoisonOutcome::NotApplicable);
        }
        // Ownership first, exactly as the relay and timer paths do: a poison wake and a
        // correlated relay can land on different replicas at the same instant, and claims
        // are what arbitrate every §2.1-style window.
        match bridge
            .claim_instance(&fire.deployment, &fire.instance_id)
            .await?
        {
            InstanceClaimOutcome::Granted => {}
            InstanceClaimOutcome::HeldByOther => {
                // Contended or vanished — either way the wake is DROPPED, not retried: the
                // prompt is best-effort and the timeout boundary is the guaranteed detector.
                // (The winning owner's step will see the poisoned row itself if relevant.)
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }
        }
        let released_in_tx = std::cell::Cell::new(false);
        let body = async {
            let Some(snapshot) = bridge.load(&fire.deployment, &fire.instance_id).await? else {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            };
            if !snapshot.suspended
                || Self::instance_failed_guard(&snapshot, &fire.instance_id).is_some()
                || Self::instance_terminal_guard(&snapshot, &fire.instance_id).is_some()
            {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }
            if !snapshot.waiting_nodes.iter().any(|n| n == &fire.node_id) {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }
            if snapshot.retry_backoff.contains_key(&fire.node_id) {
                // Already in a backoff window — the failure was already accounted.
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }
            // Fail-closed pin resolution, same as the other resume paths.
            let pinned = if snapshot.deployment_id.trim().is_empty() {
                fire.deployment.clone()
            } else {
                DeploymentId::of(&snapshot.deployment_id).map_err(|e| {
                    Diagnostic::error(
                        codes::RUNTIME_RESUME_PIN_UNRESOLVABLE,
                        format!(
                            "poison wake for instance {} carries an unreadable pinned \
                             deployment '{}' ({e}); the wake refuses to act.",
                            fire.instance_id, snapshot.deployment_id
                        ),
                    )
                })?
            };
            let Some(process) = self.processes.find_in_module(&pinned, &snapshot.process_id) else {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            };
            let is_call = matches!(
                process.node(&fire.node_id),
                Ok(Node::ServiceTask { implementation, .. })
                    if implementation.starts_with(CHANNEL_CALL_PREFIX)
            );
            if !is_call || process.retry_policy(&fire.node_id).is_none() {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }
            // The durable evidence gate: no poisoned row for (instance, node) ⇒ no failure.
            if !bridge
                .poisoned_call_emission_exists(&pinned, &fire.instance_id, &fire.node_id)
                .await?
            {
                return Ok(ChannelCallPoisonOutcome::NotApplicable);
            }

            let mut prior = Variables::new();
            for (name, value) in &snapshot.variables {
                prior.insert(name.clone(), value.clone());
            }
            let start_node = if snapshot.start_node.trim().is_empty() {
                None
            } else {
                Some(snapshot.start_node.as_str())
            };
            self.seed_audit(&fire.instance_id, snapshot.audit_seq);
            let result = self
                .executor
                .resume_channel_call_failure(
                    &process,
                    &fire.instance_id,
                    &fire.node_id,
                    codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED,
                    &format!(
                        "the outbox terminally poisoned the request delivery of channel-call \
                         task '{}' (sutra.outbox.retry.max-attempts exhausted); the in-flight \
                         attempt can never be answered",
                        fire.node_id
                    ),
                    &snapshot.completed_nodes,
                    prior,
                    pinned.clone(),
                    BTreeMap::new(),
                    start_node,
                    &snapshot.waiting_nodes,
                    &snapshot.coverage,
                    &snapshot.retry_attempts,
                    &snapshot.retry_backoff,
                )
                .await;
            let result = match result {
                Ok(r) => r,
                Err(e) => {
                    // Exhausted budget / non-retryable classification: the durable FAILED
                    // snapshot, exactly as an exhausted registered-task retry.
                    let d = e.to_diagnostic();
                    let cause = Diagnostic::error(&d.code, d.message.clone());
                    self.mark_instance_failed(
                        &bridge,
                        &pinned,
                        &fire.instance_id,
                        &snapshot,
                        &cause,
                    )
                    .await;
                    return Ok(ChannelCallPoisonOutcome::Failed {
                        instance_id: fire.instance_id.clone(),
                    });
                }
            };
            match self
                .commit_resume_outcome(
                    &bridge,
                    BTreeMap::new(),
                    None,
                    &process,
                    &snapshot,
                    &pinned,
                    &fire.deployment,
                    &fire.node_id,
                    result,
                    None,
                    None,
                )
                .await
            {
                Ok(_) => {}
                Err(ResumeCommitError::Transient(d)) => return Err(d),
                Err(refused @ ResumeCommitError::Refused(_)) => {
                    // The same durable FAILED step as the executor-error arm above.
                    self.fail_refused_resume(
                        &bridge,
                        &process,
                        &pinned,
                        &fire.instance_id,
                        &snapshot,
                        &BTreeMap::new(),
                        refused,
                    )
                    .await;
                    return Ok(ChannelCallPoisonOutcome::Failed {
                        instance_id: fire.instance_id.clone(),
                    });
                }
            }
            if pinned == fire.deployment {
                released_in_tx.set(true);
            }
            Ok(ChannelCallPoisonOutcome::Parked {
                instance_id: fire.instance_id.clone(),
            })
        };
        match catch_unwind_completion(body).await {
            Ok(outcome) => {
                if !released_in_tx.get() {
                    release_claim_best_effort(&bridge, &fire.deployment, &fire.instance_id).await;
                }
                outcome
            }
            Err(panic) => {
                release_claim_best_effort(&bridge, &fire.deployment, &fire.instance_id).await;
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Evaluate every `<q:alias>` on the routed start event into `variables` (the same
    /// binding semantics as [`Self::materialise_and_index_aliases`]) but COLLECT the rows
    /// instead of writing them — the park step records them atomically.
    fn evaluate_alias_rows(
        &self,
        process: &ProcessDefinition,
        start_id: &str,
        variables: &mut Variables,
    ) -> Result<Vec<EvaluatedAlias>, Diagnostic> {
        let aliases = &process.bindings_for(start_id).aliases;
        let mut out = Vec::new();
        if aliases.is_empty() {
            return Ok(out);
        }
        for alias in aliases.clone() {
            let value =
                sutra_feel::expressions::eval(&alias.expression, &variables.to_feel_context())
                    .map_err(|e| {
                        Diagnostic::error(
                            codes::INBOUND_ALIAS_FEEL_EVAL_FAILED,
                            format!(
                                "<q:alias {}> expression '{}' on node {start_id} threw at \
                                 evaluation: {}",
                                alias.name, alias.expression, e.message
                            ),
                        )
                    })?;
            if alias.multi {
                match &value {
                    FeelValue::Null => continue,
                    FeelValue::List(items) => {
                        for item in items {
                            if item.is_null() {
                                continue;
                            }
                            out.push(evaluated_alias(&alias, item));
                        }
                        variables.insert(alias.name.clone(), value.clone());
                    }
                    other => {
                        return Err(Diagnostic::error(
                            codes::INBOUND_ALIAS_MULTI_NOT_LIST,
                            format!(
                                "<q:alias {} multi=true> expression '{}' on node {start_id} \
                                 returned {}; multi=true requires a list-valued result",
                                alias.name,
                                alias.expression,
                                other.type_name()
                            ),
                        ));
                    }
                }
            } else {
                if value.is_null() {
                    continue;
                }
                out.push(evaluated_alias(&alias, &value));
                variables.insert(alias.name.clone(), value);
            }
        }
        Ok(out)
    }

    /// Build the synchronous reply: a flow-produced `responseBody` (a template render)
    /// wins; else a structured `responseObject` is codec-encoded to the INBOUND content
    /// type (native-reply continuity — the reply-encode step). The reply
    /// content type prefers `responseContentType` → the visited `<q:reply>` node's
    /// declared `contentType` → the inbound content type (symmetric reply).
    fn build_reply(
        &self,
        binding: &ChannelBinding,
        process: &ProcessDefinition,
        message: &InboundMessage,
        exec: &sutra_executor::ExecResult,
    ) -> Result<Option<SyncReply>, Diagnostic> {
        let reply = self.render_reply(binding, process, message, exec)?;
        // `session-end: on-reply`: the reply on the session's channel ends the replying
        // instance's session. Recorded here (the one place every reply is rendered) and
        // released by the next flush, once the pass has bound any admission slot.
        if reply.is_some() && binding.session_end.as_ref().is_some_and(|end| end.on_reply) {
            self.session_releases
                .borrow_mut()
                .push((binding.deployment_id(), exec.instance_id.clone()));
        }
        Ok(reply)
    }

    /// Render the reply [`Self::build_reply`] answers with.
    fn render_reply(
        &self,
        binding: &ChannelBinding,
        process: &ProcessDefinition,
        message: &InboundMessage,
        exec: &sutra_executor::ExecResult,
    ) -> Result<Option<SyncReply>, Diagnostic> {
        // A reply is something the flow DECLARED, not a by-product of having rendered
        // something. `responseBody` is the only slot the executor has for a template / script /
        // decision render, so a flow whose terminal task renders bytes for a `<q:send>` fills it
        // without ever asking to answer the caller — and reading intent into that mechanism made
        // an asynchronous channel deliver the same message twice, once out of band through its
        // send and once back in the ack's body.
        //
        // So: no visited `<q:reply>`, no synchronous reply. `<q:reply continue="true">`, whose
        // receipt an on-persist ack deliberately carries, is unaffected — it is a declaration,
        // and this only requires that the declaration exist.
        if !visited_reply_declared(process, exec) {
            return Ok(None);
        }
        let declared_ct = visited_reply_content_type(process, exec);
        let symmetric = message
            .content_type
            .clone()
            .filter(|c| !c.trim().is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let content_type = match exec.output("responseContentType") {
            Some(FeelValue::String(s)) => s.clone(),
            _ => declared_ct.unwrap_or(symmetric),
        };

        if let Some(FeelValue::String(body)) = exec.output("responseBody") {
            // A template render reaches the wire without passing through a codec, so this is
            // the ONLY point at which the channel's contract can still see it.
            let bytes = body.clone().into_bytes();
            self.chain.validate_outbound(
                &binding.deployment_id(),
                &binding.channel_name,
                declared_reply_codec(process, exec).as_deref(),
                NO_INFERRED_REPLY_CODEC,
                &bytes,
                Some(content_type.as_str()),
                &format!("The reply of process '{}'", process.id),
            )?;
            return Ok(Some(SyncReply {
                body: bytes.into(),
                content_type,
            }));
        }
        if let Some(reply_object) = exec.output("responseObject") {
            if !binding.codec.is_empty() {
                let value = CodecValue::Json(feel_to_json(reply_object));
                let bytes = self
                    .chain
                    .encode(binding, &value, message.content_type.as_deref())?;
                // Encoding projects; it does not validate (`project_to_tree` is the
                // non-validating half of the schema pair), so the contract check is this call,
                // not the one above.
                self.chain.validate_outbound(
                    &binding.deployment_id(),
                    &binding.channel_name,
                    declared_reply_codec(process, exec).as_deref(),
                    NO_INFERRED_REPLY_CODEC,
                    &bytes,
                    message.content_type.as_deref(),
                    &format!("The reply of process '{}'", process.id),
                )?;
                // Native reply: the wire content-type mirrors the inbound.
                return Ok(Some(SyncReply {
                    body: bytes.into(),
                    content_type: message
                        .content_type
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".to_string()),
                }));
            }
        }
        Ok(None)
    }

    /// Mint the per-dispatch intake id — the owner key in the (per-lane) in-memory alias
    /// store, salted with the shard index so ids stay process-unique (and log-readable)
    /// once Phase 2 runs N lanes, each with its own counter.
    fn next_intake_id(&self) -> String {
        let mut counter = self.intake_counter.borrow_mut();
        *counter += 1;
        format!("intake-s{}-{:016x}", self.shard.index, *counter)
    }

    // ---- synchronous outbound calls (the in-memory chain) ---------------------------------

    /// The channel-call nodes among the pass's `fresh_waits` (the calls it just issued — a first
    /// arrival or a loop coming round again) whose outbound channel is synchronous, each with the inline caller its destination resolves to (`None`
    /// when this engine has none for the scheme).
    ///
    /// A call whose `<q:retry>` delay expression cannot be armed against `variables` gets no
    /// caller either, so it degrades to the durable path, where the executor evaluates the delay
    /// again only if a retry is ever armed (and fails the instance once if it still cannot).
    fn sync_call_sites(
        &self,
        process: &ProcessDefinition,
        deployment: &DeploymentId,
        fresh_waits: &[String],
        variables: &sutra_executor::Variables,
    ) -> Vec<SyncCallSite> {
        let mut sites = Vec::new();
        for node_id in fresh_waits {
            let Some(channel) = channel_call_name(process, node_id) else {
                continue;
            };
            let Some(resolved) = self.executor.outbound_channels().find(deployment, &channel)
            else {
                continue;
            };
            if !resolved.sync {
                continue;
            }
            let mut caller = self.inline_callers.resolve(&resolved.destination);
            if let Some(policy) = process.retry_policy(node_id) {
                if let Err(reason) = self
                    .executor
                    .resolve_retry_delays(deployment, policy, variables)
                {
                    tracing::warn!(
                        code = codes::CHANNEL_SYNC_CALL_DEGRADED,
                        process = %process.id,
                        node = %node_id,
                        "synchronous channel-call takes the async path: its <q:retry> delay \
                         expression could not be armed ({reason})"
                    );
                    caller = None;
                }
            }
            sites.push(SyncCallSite {
                node_id: node_id.clone(),
                caller,
            });
        }
        sites
    }

    /// The runtime half of `SUTRA.CHANNEL.SYNC_CALL_DEGRADED`: these synchronous calls are going
    /// out through the outbox, and their answers must come back by correlation.
    fn warn_sync_degraded(&self, process: &ProcessDefinition, sites: &[SyncCallSite], why: &str) {
        for site in sites {
            tracing::warn!(
                code = codes::CHANNEL_SYNC_CALL_DEGRADED,
                process = %process.id,
                node = %site.node_id,
                "synchronous channel-call takes the async path ({why}): the request goes \
                 through the outbox and the instance parks until a correlated response or \
                 its timeout"
            );
        }
    }

    /// Start a chain: hold the start's unique aliases in memory, split this pass's emissions
    /// into the inline requests and the carried rest, and hand the instance to the router.
    async fn launch_inline_chain(
        &self,
        mut chain: Box<InlineChain>,
        drained: Vec<sutra_executor::Emission>,
        sites: Vec<SyncCallSite>,
    ) -> Result<DispatchOutcome, Diagnostic> {
        if let Some(store) = &self.aliases {
            for row in chain.alias_rows.iter().filter(|r| r.unique) {
                chain.held_aliases = true;
                if !store.record(
                    &chain.deployment,
                    &chain.instance_id,
                    &row.name,
                    &row.value,
                    true,
                ) {
                    let owner = store
                        .find_live(&chain.deployment, &row.name, &row.value)
                        .unwrap_or_default();
                    let d = Diagnostic::error(
                        codes::INBOUND_ALIAS_CONFLICT_REJECT,
                        format!(
                            "<q:alias {}> = '{}' already bound to in-flight instance {owner} \
                             (a synchronous call chain); the second arrival is rejected",
                            row.name, row.value
                        ),
                    );
                    self.release_chain(&chain, ChainEnd::Aborted).await;
                    return Err(d);
                }
            }
        }
        // An instance inside a chain holds its channel's admission slot like a RUNNING one:
        // counted now, freed when the chain completes or fails (a durable park keeps it). Only
        // a capped channel is counted, so an uncapped fully synchronous chain still writes
        // nothing durable.
        if let (Some(store), Some(_)) = (&self.concurrency, chain.binding.max_concurrent_instances)
        {
            if chain.stored.is_some() {
                // A stored instance already has its row (WAITING) — unless its session ended,
                // in which case it must not take the slot back.
                store
                    .record_resumed(&chain.deployment, &chain.instance_id)
                    .await;
            } else {
                store
                    .record_started(
                        &chain.deployment,
                        &chain.instance_id,
                        &chain.message.channel,
                    )
                    .await;
            }
            chain.counted = true;
        }
        if chain.stored.is_some() {
            self.inline_held
                .borrow_mut()
                .insert(chain.instance_id.clone());
        }
        // The durable in-flight marker: what a crash mid-chain leaves behind for the sweep to
        // release (the delivery's dedup rows, an admission slot). Only written when there is
        // something to release.
        if let Some(bridge) = &self.bridge {
            if !chain.dedup_keys.is_empty() || chain.counted || chain.pooled {
                if let Err(d) = bridge
                    .begin_inline_chain(&chain.deployment, &chain.instance_id, &chain.dedup_keys)
                    .await
                {
                    self.release_chain(&chain, ChainEnd::Aborted).await;
                    return Err(d);
                }
                chain.marked = true;
            }
        }
        let calls = self.issue_inline_calls(&mut chain, drained, &sites);
        if calls.is_empty() {
            // Unreachable: a fresh synchronous call always emits its request.
            self.release_chain(&chain, ChainEnd::Aborted).await;
            return Err(Diagnostic::error(
                codes::RUNTIME_UNEXPECTED,
                "internal: a synchronous call chain started with no request to make",
            ));
        }
        let heartbeat = chain
            .stored
            .as_ref()
            .map(|stored| (stored.claim_scope.clone(), chain.instance_id.clone()));
        Ok(DispatchOutcome::InlineCalls(InlineHandoff::new(
            InlineContinuation {
                chain,
                calls,
                heartbeat,
            },
        )))
    }

    /// Turn the requests of `sites` among `drained` into calls (marking their nodes in flight);
    /// every other emission is carried to the chain's final commit.
    fn issue_inline_calls(
        &self,
        chain: &mut InlineChain,
        drained: Vec<sutra_executor::Emission>,
        sites: &[SyncCallSite],
    ) -> Vec<InlineCall> {
        let mut calls = Vec::new();
        for emission in drained {
            let caller = (emission.kind == sutra_executor::EmissionKind::Send)
                .then(|| {
                    sites
                        .iter()
                        .find(|s| s.node_id == emission.node_id)
                        .and_then(|s| s.caller.clone())
                })
                .flatten();
            let Some(caller) = caller else {
                chain.emissions.push(emission);
                continue;
            };
            let node_id = emission.node_id.clone();
            // A retry delay expression that cannot be armed here sends the call down the durable
            // path instead: there the executor evaluates it again when (and only when) a retry is
            // armed, and fails the instance once if it still cannot.
            let retry = match inline_retry(&self.executor, chain, &node_id) {
                Ok(retry) => retry,
                Err(reason) => {
                    self.warn_sync_degraded(
                        &chain.process,
                        &[SyncCallSite {
                            node_id: node_id.clone(),
                            caller: None,
                        }],
                        &format!("its <q:retry> delay expression could not be armed: {reason}"),
                    );
                    chain.emissions.push(emission);
                    continue;
                }
            };
            let mut request = self.inline_request(chain, emission);
            // The instance's audit routing and identity, for a caller that writes audit events
            // only (the same rule the outbox dispatcher applies).
            if self.audit_listener.is_some() && caller.writes_audit_events() {
                request.headers.insert(
                    crate::transport_audit::AUDIT_JOURNAL_HEADER.to_string(),
                    audit_journal_policy(&chain.process),
                );
            }
            crate::transport_audit::stamp_journal_identity(
                &mut request.headers,
                caller.writes_audit_events(),
                chain.deployment.value(),
                &chain.instance_id,
            );
            // Streamed only when the task asks AND the delivery the chain answers asked for a
            // streamed reply; anywhere else `q:stream` changes nothing.
            let stream = matches!(
                chain.process.node(&node_id),
                Ok(Node::ServiceTask { stream: true, .. })
            ) && self.sse_channels.contains(&(
                chain.binding.namespace.module_key(),
                chain.message.channel.clone(),
            ));
            calls.push(InlineCall {
                node_id: node_id.clone(),
                request,
                caller,
                budget: inline_call_budget(&chain.process, &node_id, &chain.pending_timers),
                retry,
                stream,
            });
            chain.in_flight.insert(node_id);
        }
        calls
    }

    /// Render one request exactly as the outbox would put it on the wire (reply mode,
    /// CloudEvents rendering, auth headers), so a synchronous call carries the same bytes and
    /// headers as its async form.
    fn inline_request(
        &self,
        chain: &InlineChain,
        emission: sutra_executor::Emission,
    ) -> crate::sink::OutboundMessage {
        let row = to_outbox_emissions(
            vec![emission],
            chain.labels(),
            inbound_traceparent(&chain.message),
        )
        .pop()
        .expect("one emission in, one row out");
        let claimed = crate::outbox_dispatch::ClaimedOutboxRow {
            entry_id: String::new(),
            attempt_count: 0,
            destination: row.destination,
            headers: row.headers,
            body: row.body.into_inner(),
            content_type: row.content_type,
            outbox_key: row.outbox_key,
            mode: row.mode,
            cloud_event_json: row.cloud_event_json,
            auth_ref_json: row.auth_ref_json,
            labels: row.labels,
            traceparent: row.traceparent,
            instance_id: row.instance_id,
            node_id: Some(row.node_id),
            required: row.required,
            last_diagnostic_json: None,
        };
        crate::outbox_dispatch::encode_wire_message(
            &claimed,
            &now_rfc3339(),
            &*self.inline_secret_env,
        )
    }

    /// Apply one call's answer to the in-memory instance — the actor half of the chain. Runs
    /// one resume pass and ends the chain, or hands it back with any calls the pass issued.
    pub(crate) async fn resume_in_memory(
        &self,
        mut chain: Box<InlineChain>,
        node_id: String,
        result: InlineCallResult,
    ) -> InlineStep {
        chain.in_flight.remove(&node_id);
        let process = Arc::clone(&chain.process);
        let policy = process.retry_policy(&node_id).cloned();
        let failure = match result {
            InlineCallResult::Answered(response) => {
                match self.inline_response_variables(&chain, &node_id, response) {
                    Ok(answer) => {
                        return self
                            .resume_chain_pass(chain, &node_id, ChainPass::Answer(answer))
                            .await
                    }
                    // An answer the call's own contract rejects is a failed call.
                    Err(d) => d,
                }
            }
            InlineCallResult::TimedOut => {
                let boundary = inline_timeout_boundary(&process, &node_id, &chain.pending_timers);
                match boundary {
                    // No policy: the timeout boundary fires, exactly as a durable timer would
                    // — its modelled route, or the catchable timeout error.
                    Some(boundary) if policy.is_none() => {
                        return self
                            .resume_chain_pass(chain, &node_id, ChainPass::Timeout(boundary))
                            .await
                    }
                    _ => Diagnostic::error(
                        sutra_executor::codes::DISPATCH_CHANNEL_CALL_TIMEOUT,
                        format!(
                            "synchronous channel-call task '{node_id}' got no answer within \
                             its <q:timeout>"
                        ),
                    ),
                }
            }
            InlineCallResult::Failed(InlineFailure::Permanent(d))
            | InlineCallResult::Failed(InlineFailure::Retryable(d)) => d,
        };
        if policy.is_some() {
            // The policy already ran inline; the failure path of the task rules on what is left.
            return self
                .resume_chain_pass(chain, &node_id, ChainPass::Failure(failure))
                .await;
        }
        // No policy: an uncaught task failure fails the flow, as a registered task's does.
        self.fail_chain(chain, failure).await
    }

    /// One in-memory resume pass and its quiescent point.
    async fn resume_chain_pass(
        &self,
        mut chain: Box<InlineChain>,
        node_id: &str,
        pass: ChainPass,
    ) -> InlineStep {
        let process = Arc::clone(&chain.process);
        let labels = chain.labels();
        self.seed_audit(&chain.instance_id, chain.audit_seq);
        let (satisfied, result) = match pass {
            ChainPass::Answer(answer) => {
                let correlation = CoverageCorrelation {
                    trace_id: inbound_traceparent(&chain.message),
                    business_key: chain
                        .alias_rows
                        .iter()
                        .find(|r| r.unique)
                        .or_else(|| chain.alias_rows.first())
                        .map(|r| r.value.clone()),
                };
                let result = self
                    .executor
                    .resume_correlated(
                        &process,
                        &chain.instance_id,
                        &chain.completed_nodes,
                        chain.variables.clone(),
                        node_id,
                        &answer,
                        chain.deployment.clone(),
                        labels,
                        chain.start_node.as_deref(),
                        &chain.waiting_nodes,
                        &chain.coverage,
                        &chain.retry_attempts,
                        &chain.retry_backoff,
                        correlation,
                    )
                    .await;
                (node_id.to_string(), result)
            }
            ChainPass::Timeout(boundary) => {
                let due_at = chain
                    .pending_timers
                    .iter()
                    .find(|t| t.node_id == boundary)
                    .map(|t| t.due_at.clone())
                    .unwrap_or_else(now_rfc3339);
                let fire = TimerFire {
                    deployment: chain.deployment.clone(),
                    instance_id: chain.instance_id.clone(),
                    node_id: boundary.clone(),
                    due_at,
                    fired_at: now_rfc3339(),
                };
                let result = self
                    .executor
                    .resume_timer(
                        &process,
                        &fire,
                        &chain.completed_nodes,
                        chain.variables.clone(),
                        labels,
                        chain.start_node.as_deref(),
                        &chain.waiting_nodes,
                        &chain.coverage,
                        &chain.retry_attempts,
                        &chain.retry_backoff,
                    )
                    .await;
                (boundary, result)
            }
            ChainPass::Failure(failure) => {
                // The inline attempts spent the budget, so the policy must not park a durable
                // backoff: offer the failure as the LAST attempt.
                let mut attempts = chain.retry_attempts.clone();
                if let Some(policy) = process.retry_policy(node_id) {
                    attempts.insert(node_id.to_string(), policy.max_attempts.saturating_sub(1));
                }
                let result = self
                    .executor
                    .resume_channel_call_failure(
                        &process,
                        &chain.instance_id,
                        node_id,
                        &failure.code,
                        &failure.message,
                        &chain.completed_nodes,
                        chain.variables.clone(),
                        chain.deployment.clone(),
                        labels,
                        chain.start_node.as_deref(),
                        &chain.waiting_nodes,
                        &chain.coverage,
                        &attempts,
                        &chain.retry_backoff,
                    )
                    .await;
                (node_id.to_string(), result)
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                let d = e.to_diagnostic();
                return self
                    .fail_chain(chain, Diagnostic::error(&d.code, d.message.clone()))
                    .await;
            }
        };
        // The satisfied wait consumed its own timer rows (and, for a fired boundary, its host's).
        let resolved = resolved_waits_for(&process, &satisfied);
        chain
            .pending_timers
            .retain(|t| !resolved.contains(&t.node_id));
        if let Some(stored) = &mut chain.stored {
            for node in resolved {
                if !stored.satisfied.contains(&node) {
                    stored.satisfied.push(node);
                }
            }
        }
        let mut drained = self.emissions.drain();
        if let Err(d) = self.validate_emissions(
            &chain.binding.namespace.module_key(),
            &chain.deployment,
            &chain.binding.channel_name,
            &process,
            &mut drained,
        ) {
            return self.abort_chain(chain, d).await;
        }
        match result {
            StatefulExecResult::Completed {
                outputs,
                visited_nodes,
                ..
            } => {
                self.complete_chain(chain, drained, outputs, visited_nodes)
                    .await
            }
            StatefulExecResult::Suspended {
                waiting_nodes,
                completed_nodes,
                variables,
                timer_waits,
                detached_reply,
                coverage_progress,
                retry_attempts,
                retry_backoff,
                fresh_waits,
                ..
            } => {
                chain.waiting_nodes = waiting_nodes;
                // The waits still needing fresh rows at the chain's durable park: every new wait
                // of every pass so far that is still waiting.
                for node in &fresh_waits {
                    if !chain.fresh_waits.contains(node) {
                        chain.fresh_waits.push(node.clone());
                    }
                }
                let still_waiting = chain.waiting_nodes.clone();
                chain.fresh_waits.retain(|n| still_waiting.contains(n));
                chain.visited.extend(completed_nodes.iter().cloned());
                chain.completed_nodes = completed_nodes;
                chain.variables = variables;
                chain.coverage = coverage_progress;
                chain.retry_attempts = retry_attempts;
                chain.retry_backoff = retry_backoff;
                chain.pending_timers.extend(timer_waits);
                chain.audit_seq = self.audit_seq_for(&chain.instance_id);
                // Respond-and-continue reached mid-chain: the caller is still waiting on the
                // chain, so the reply is held and returned when the chain ends.
                if detached_reply && chain.early_reply.is_none() {
                    let reply = match &chain.stored {
                        Some(_) => self.stored_chain_reply(
                            &chain,
                            chain.variables.clone(),
                            chain.visited.clone(),
                            false,
                        ),
                        None => {
                            let exec = ExecResult {
                                instance_id: chain.instance_id.clone(),
                                outputs: chain.variables.clone(),
                                visited_nodes: chain.visited.clone(),
                            };
                            self.build_reply(&chain.binding, &process, &chain.message, &exec)
                        }
                    };
                    match reply {
                        Ok(reply) => chain.early_reply = reply,
                        Err(d) => return self.abort_chain(chain, d).await,
                    }
                }
                let sites = self.sync_call_sites(
                    &process,
                    &chain.deployment,
                    &fresh_waits,
                    &chain.variables,
                );
                let (inline, degraded): (Vec<SyncCallSite>, Vec<SyncCallSite>) =
                    sites.into_iter().partition(|s| s.caller.is_some());
                if !degraded.is_empty() {
                    self.warn_sync_degraded(
                        &process,
                        &degraded,
                        "no inline caller serves the channel's scheme",
                    );
                }
                let calls = self.issue_inline_calls(&mut chain, drained, &inline);
                if chain.in_flight.is_empty() {
                    return self.park_chain(chain).await;
                }
                self.flush_session_releases().await;
                InlineStep::More { chain, calls }
            }
        }
    }

    /// The chain completed: commit every carried emission in one step. A start answers the
    /// caller with the flow's reply, built against the ORIGINAL binding and message; a stored
    /// instance takes its terminal step (and, like any resume, has no reply of its own).
    async fn complete_chain(
        &self,
        mut chain: Box<InlineChain>,
        mut drained: Vec<sutra_executor::Emission>,
        outputs: Variables,
        visited_nodes: std::collections::HashSet<String>,
    ) -> InlineStep {
        let mut emissions = std::mem::take(&mut chain.emissions);
        emissions.append(&mut drained);
        if self.bridge.is_none() {
            // Persistence-less host: the collect-only hook, as on the run-to-end path.
            for emission in std::mem::take(&mut emissions) {
                self.outbox.enqueue(emission);
            }
        }
        let rows = to_outbox_emissions(
            emissions,
            chain.labels(),
            inbound_traceparent(&chain.message),
        );
        if chain.stored.is_some() {
            let Some(bridge) = self.bridge.clone() else {
                return self.abort_chain(chain, no_bridge()).await;
            };
            if let Err(d) = bridge
                .commit_complete(&chain.deployment, &chain.instance_id, &rows)
                .await
            {
                return self.abort_chain(chain, d).await;
            }
            self.shard_metrics.resumed();
            if let Some(store) = &self.concurrency {
                store
                    .record_terminal(&chain.deployment, &chain.instance_id)
                    .await;
            }
            self.release_chain(&chain, ChainEnd::Completed).await;
            chain.visited.extend(visited_nodes);
            let reply = self.stored_chain_reply(&chain, outputs, chain.visited.clone(), false);
            let early = chain.early_reply.take();
            return InlineStep::Done {
                outcome: reply.map(|reply| DispatchOutcome::Completed {
                    instance_id: chain.instance_id.clone(),
                    outputs: serde_json::json!({}),
                    reply: reply.or(early),
                }),
                settle_consumed: false,
                completed: true,
            };
        }
        let committed = match &self.bridge {
            Some(bridge) if !rows.is_empty() => {
                bridge.commit_emissions(&chain.deployment, &rows).await
            }
            _ => Ok(()),
        };
        if let Err(d) = committed {
            return self.abort_chain(chain, d).await;
        }
        chain.visited.extend(visited_nodes);
        let exec = ExecResult {
            instance_id: chain.instance_id.clone(),
            outputs,
            visited_nodes: chain.visited.clone(),
        };
        let reply = self.build_reply(&chain.binding, &chain.process, &chain.message, &exec);
        // The step is committed either way: the dedup row stays, as on the run-to-end path.
        self.release_chain(&chain, ChainEnd::Completed).await;
        self.flush_session_releases().await;
        let outcome = reply.map(|reply| DispatchOutcome::Completed {
            instance_id: exec.instance_id.clone(),
            outputs: exec.outputs.to_json(),
            reply: reply.or_else(|| chain.early_reply.take()),
        });
        InlineStep::Done {
            outcome,
            settle_consumed: false,
            completed: true,
        }
    }

    /// The chain reached a wait no synchronous call answers: the one durable commit of the
    /// whole chain, carrying every alias, emission and timer row it accumulated — the first
    /// park of a start, or the re-park (claim handed back in the same transaction) of a stored
    /// instance.
    async fn park_chain(&self, mut chain: Box<InlineChain>) -> InlineStep {
        let Some(bridge) = self.bridge.clone() else {
            return self.abort_chain(chain, no_bridge()).await;
        };
        let process = Arc::clone(&chain.process);
        // The request the chain answers gets the last destination-less `<q:reply>` its passes
        // rendered — else a respond-and-continue receipt produced on the way.
        let park_reply = match &chain.stored {
            Some(_) => self.stored_chain_reply(
                &chain,
                chain.variables.clone(),
                chain.visited.clone(),
                true,
            ),
            None => self.reply_at(
                &chain.binding,
                &process,
                &chain.message,
                &ExecResult {
                    instance_id: chain.instance_id.clone(),
                    outputs: chain.variables.clone(),
                    visited_nodes: chain.visited.clone(),
                },
                true,
            ),
        };
        let park_reply = match park_reply {
            Ok(reply) => reply.or_else(|| chain.early_reply.take()),
            Err(d) => return self.abort_chain(chain, d).await,
        };
        let mut records = chain.alias_rows.clone();
        match wait_alias_rows(&process, &chain.fresh_waits, &chain.variables) {
            Ok(rows) => records.extend(rows),
            Err(d) => return self.abort_chain(chain, d).await,
        }
        let emissions = to_outbox_emissions(
            std::mem::take(&mut chain.emissions),
            chain.labels(),
            inbound_traceparent(&chain.message),
        );
        let pv = persisted_variables(
            &process.declared_variables,
            persisted_variable_values(&chain.variables),
        );
        let snapshot = SuspendedInstance {
            process_id: process.id.clone(),
            deployment_id: chain.deployment.value().to_string(),
            status: "SUSPENDED".to_string(),
            suspended: true,
            completed_nodes: chain.completed_nodes.clone(),
            variables: pv.kept,
            sensitive: pv.sensitive,
            waiting_nodes: chain.waiting_nodes.clone(),
            start_node: chain.start_node.clone().unwrap_or_default(),
            coverage: chain.coverage.clone(),
            retry_attempts: chain.retry_attempts.clone(),
            retry_backoff: chain.retry_backoff.clone(),
            audit_seq: self.audit_seq_for(&chain.instance_id),
            // A stored instance keeps the key id it was first parked under.
            key_id: match &chain.stored {
                Some(stored) => stored.prior.key_id.clone(),
                None => chain.binding.tenant().to_string(),
            },
            encrypt_names: pv.encrypt_names,
            subjects: pv.subjects,
        };
        if let Some(stored) = &chain.stored {
            // A wait the chain parked fresh replaces whatever row its node had; the satisfied
            // waits' own aliases retire with them.
            let mut resolved = stored.satisfied.clone();
            for node in &chain.fresh_waits {
                if !resolved.contains(node) {
                    resolved.push(node.clone());
                }
            }
            let retired = retired_wait_aliases(
                &process,
                &resolved,
                &stored.prior,
                &chain.waiting_nodes,
                &chain.fresh_waits,
                &records,
                stored.relay_alias.as_ref(),
            );
            if let Err(d) = bridge
                .commit_repark_retiring(
                    &chain.deployment,
                    &chain.instance_id,
                    &snapshot,
                    &resolved,
                    &records,
                    &timer_records(&chain.pending_timers),
                    &emissions,
                    &retired,
                )
                .await
            {
                return self.abort_chain(chain, d).await;
            }
            self.shard_metrics.resumed();
            self.release_chain(&chain, ChainEnd::Parked).await;
            self.flush_session_releases().await;
            return InlineStep::Done {
                outcome: Ok(DispatchOutcome::Completed {
                    instance_id: chain.instance_id.clone(),
                    outputs: serde_json::json!({}),
                    reply: park_reply,
                }),
                settle_consumed: false,
                completed: false,
            };
        }
        // Registered BEFORE the commit and withdrawn if it fails — the park arm's ordering.
        let registered = match (&self.deferred_acks, chain.settle.take()) {
            (Some(registry), Some(settle)) => {
                if !registry.register(
                    &chain.instance_id,
                    &chain.message.channel,
                    settle.ack,
                    settle.nack,
                ) {
                    tracing::error!(
                        channel = %chain.message.channel,
                        instance = %chain.instance_id,
                        "deferred-ack registration rejected as duplicate — the delivery's \
                         settle callbacks were dropped (broker redelivery recovers via inbox \
                         dedup)"
                    );
                }
                true
            }
            _ => false,
        };
        if let Err(commit_error) = bridge
            .commit_park(
                &chain.deployment,
                &chain.instance_id,
                &snapshot,
                &records,
                &timer_records(&chain.pending_timers),
                &emissions,
            )
            .await
        {
            if let (true, Some(registry)) = (registered, &self.deferred_acks) {
                registry.withdraw(&chain.instance_id);
            }
            return self.abort_chain(chain, commit_error).await;
        }
        self.shard_metrics.parked();
        // The slot was taken when the chain began; the parked instance keeps it (WAITING).
        // Only a capped channel is counted (see the initial-park arm).
        if let (Some(store), Some(_)) = (&self.concurrency, chain.binding.max_concurrent_instances)
        {
            if !chain.counted {
                store
                    .record_started(
                        &chain.deployment,
                        &chain.instance_id,
                        &chain.message.channel,
                    )
                    .await;
            }
            store
                .record_suspended(&chain.deployment, &chain.instance_id)
                .await;
        }
        self.release_chain(&chain, ChainEnd::Parked).await;
        self.flush_session_releases().await;
        InlineStep::Done {
            outcome: Ok(DispatchOutcome::Completed {
                instance_id: chain.instance_id.clone(),
                outputs: serde_json::json!({}),
                reply: park_reply,
            }),
            settle_consumed: registered,
            completed: false,
        }
    }

    /// The flow failed inside the chain. A start has no row to stamp, so the ack disposition
    /// is gated on the process's idempotency exactly as on the run-to-end path; a stored
    /// instance is persisted FAILED, exactly as a failed resume.
    async fn fail_chain(&self, chain: Box<InlineChain>, cause: Diagnostic) -> InlineStep {
        let _ = self.emissions.drain();
        if let (Some(stored), Some(bridge)) = (&chain.stored, self.bridge.clone()) {
            self.mark_instance_failed(
                &bridge,
                &chain.deployment,
                &chain.instance_id,
                &stored.prior,
                &cause,
            )
            .await;
            self.release_chain(&chain, ChainEnd::Failed).await;
            return InlineStep::Done {
                outcome: Err(cause),
                settle_consumed: false,
                completed: false,
            };
        }
        self.release_chain(&chain, ChainEnd::Failed).await;
        let outcome = self
            .gate_execution_failure(&chain.process, &chain.deployment, &chain.message, cause)
            .await;
        InlineStep::Done {
            outcome,
            settle_consumed: false,
            completed: false,
        }
    }

    /// The chain could not reach a quiescent commit (a rejected emission, a failed commit):
    /// surfaced as-is, like the same failure on the park path. A stored instance stays at the
    /// frontier it had before the chain.
    async fn abort_chain(&self, chain: Box<InlineChain>, d: Diagnostic) -> InlineStep {
        let _ = self.emissions.drain();
        self.release_chain(&chain, ChainEnd::Aborted).await;
        InlineStep::Done {
            outcome: Err(d),
            settle_consumed: false,
            completed: false,
        }
    }

    /// Release what the chain held while it ran: its unique aliases; its admission slot unless
    /// it parked; a stored instance's claim (unless the final step released it in its own
    /// transaction); its in-flight marker; and — when nothing of it was persisted — the inbox
    /// dedup rows of its delivery, so the client's retry runs again instead of being swallowed
    /// as a duplicate.
    async fn release_chain(&self, chain: &InlineChain, end: ChainEnd) {
        if chain.held_aliases {
            self.retire_aliases(&chain.deployment, &chain.instance_id);
        }
        // The instance is over (or, for a start, was never persisted): it holds no slot. A
        // STORED instance whose chain aborted stays parked at its frontier, slots and all.
        let ended = match end {
            ChainEnd::Completed | ChainEnd::Failed => true,
            ChainEnd::Aborted => chain.stored.is_none(),
            ChainEnd::Parked => false,
        };
        if ended {
            if chain.counted {
                if let Some(store) = &self.concurrency {
                    store
                        .record_terminal(&chain.deployment, &chain.instance_id)
                        .await;
                }
            }
            if chain.pooled || chain.stored.is_some() {
                self.release_pool_slots(&chain.deployment, &chain.instance_id)
                    .await;
            }
        }
        // A FAILED stored instance was persisted; everything else that did not commit leaves
        // its delivery free to be redelivered.
        let unpersisted = match end {
            ChainEnd::Aborted => true,
            ChainEnd::Failed => chain.stored.is_none(),
            ChainEnd::Parked | ChainEnd::Completed => false,
        };
        if unpersisted {
            if let Some(inbox) = &self.inbox {
                for (channel, key) in &chain.dedup_keys {
                    inbox.forget(&chain.deployment, channel, key).await;
                }
            }
        }
        if let Some(bridge) = &self.bridge {
            if chain.marked {
                // Idempotent: the final step already cleared it inside its own transaction.
                bridge
                    .end_inline_chain(&chain.deployment, &chain.instance_id)
                    .await;
            }
            if let Some(stored) = &chain.stored {
                let released_in_tx = stored.claim_scope == chain.deployment
                    && matches!(end, ChainEnd::Parked | ChainEnd::Completed);
                if !released_in_tx {
                    release_claim_best_effort(bridge, &stored.claim_scope, &chain.instance_id)
                        .await;
                }
            }
        }
        if chain.stored.is_some() {
            self.inline_held.borrow_mut().remove(&chain.instance_id);
        }
    }

    /// A stored chain's reply: only a relay's chain answers (a timer fire has no caller), with
    /// the `<q:reply>` its turn reached.
    fn stored_chain_reply(
        &self,
        chain: &InlineChain,
        outputs: Variables,
        visited: std::collections::HashSet<String>,
        at_park: bool,
    ) -> Result<Option<SyncReply>, Diagnostic> {
        match &chain.stored {
            Some(stored) if stored.replies => self.relay_reply(
                &RelayReplyContext {
                    binding: chain.binding.clone(),
                    message: chain.message.clone(),
                },
                &chain.process,
                &stored.prior,
                &chain.instance_id,
                outputs,
                visited,
                at_park,
            ),
            _ => Ok(None),
        }
    }

    /// The resume paths' refusal of an instance a synchronous chain on this lane is driving:
    /// retry-safe, like a claim held by another replica (nothing was loaded or executed).
    fn inline_held_guard(&self, instance_id: &str, origin: &str) -> Option<Diagnostic> {
        self.inline_held.borrow().contains(instance_id).then(|| {
            Diagnostic::error(
                codes::RUNTIME_RESUME_CLAIM_HELD,
                format!(
                    "Instance {instance_id} is inside a synchronous call chain on this replica; \
                     '{origin}' refuses to resume it concurrently. Nothing was executed — retry \
                     once the chain has ended."
                ),
            )
            .with_attribute("instanceId", instance_id)
            .with_attribute(ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE)
        })
    }

    /// Keep a stored chain's claim alive while its calls run: a re-claim by the same owner
    /// restamps the heartbeat the stuck-instance sweep reads.
    pub(crate) async fn refresh_inline_claim(&self, deployment: &DeploymentId, instance_id: &str) {
        let Some(bridge) = &self.bridge else {
            return;
        };
        match bridge.claim_instance(deployment, instance_id).await {
            Ok(InstanceClaimOutcome::Granted) => {}
            Ok(InstanceClaimOutcome::HeldByOther) => tracing::error!(
                instance = instance_id,
                deployment = deployment.value(),
                "a synchronous call chain lost its instance claim — another replica may resume \
                 the instance; the chain's final commit will not apply to a moved frontier"
            ),
            Err(d) => tracing::warn!(
                instance = instance_id,
                code = %d.code,
                "synchronous call chain claim refresh failed: {}",
                d.message
            ),
        }
    }

    /// A relayed turn's reply: the `<q:reply>` THIS turn reached, rendered against the relay's
    /// own delivery exactly like a start's reply. Only nodes the turn actually ran count — the
    /// replay walks past every node of earlier turns, and a reply declared on one of those must
    /// not answer again.
    #[allow(clippy::too_many_arguments)]
    fn relay_reply(
        &self,
        to: &RelayReplyContext,
        process: &ProcessDefinition,
        prior: &SuspendedInstance,
        instance_id: &str,
        outputs: Variables,
        mut visited_nodes: std::collections::HashSet<String>,
        at_park: bool,
    ) -> Result<Option<SyncReply>, Diagnostic> {
        for done in &prior.completed_nodes {
            visited_nodes.remove(done);
        }
        let exec = ExecResult {
            instance_id: instance_id.to_string(),
            outputs,
            visited_nodes,
        };
        self.reply_at(&to.binding, process, &to.message, &exec, at_park)
    }

    /// The reply a pass answers its request with. At a PARK, only a destination-less
    /// `<q:reply>` the pass reached answers (a reply with a destination is delivered out of
    /// band); its body is the last one rendered, since each render overwrites the reply
    /// variables. At the end of a flow, or a respond-and-continue detach, the reply rules of
    /// [`Self::build_reply`] apply unchanged.
    fn reply_at(
        &self,
        binding: &ChannelBinding,
        process: &ProcessDefinition,
        message: &InboundMessage,
        exec: &ExecResult,
        at_park: bool,
    ) -> Result<Option<SyncReply>, Diagnostic> {
        if at_park && !visited_sync_reply(process, exec) {
            return Ok(None);
        }
        self.build_reply(binding, process, message, exec)
    }

    /// The in-memory chain of a RESUMED instance: its stored snapshot, the claim it keeps, and
    /// the resume pass that reached the synchronous calls.
    #[allow(clippy::too_many_arguments)]
    fn stored_chain(
        &self,
        process: Arc<ProcessDefinition>,
        prior: &SuspendedInstance,
        pinned: &DeploymentId,
        claim_scope: &DeploymentId,
        satisfied_node: &str,
        labels: BTreeMap<String, String>,
        traceparent: Option<String>,
        reply_to: Option<RelayReplyContext>,
        pass: StoredPass,
    ) -> Box<InlineChain> {
        // A resume carries labels, not a binding: rebuild the namespace the labels came from
        // (the tenant falls back to the instance's key id, which is its tenant anchor).
        let module_key = module_key_of_labels(&labels);
        let tenant = labels
            .get("tenant")
            .filter(|t| !t.is_empty())
            .cloned()
            .unwrap_or_else(|| prior.key_id.clone());
        let binding = ChannelBinding::new(
            "",
            namespace_of_module_key(&module_key, &tenant),
            pinned.clone(),
            "",
        );
        let mut headers = BTreeMap::new();
        if let Some(traceparent) = traceparent {
            headers.insert(
                sutra_executor::telemetry::TRACEPARENT_HEADER.to_string(),
                traceparent,
            );
        }
        let message = InboundMessage {
            tenant,
            module_key,
            channel: String::new(),
            headers,
            body: sutra_executor::Sensitive::new(Vec::new()),
            content_type: None,
            idempotency_key: String::new(),
            explicit_event_id: false,
            received_at: now_rfc3339(),
            cloud_event: None,
        };
        // A relay's chain renders its reply against the relay's own delivery.
        let replies = reply_to.is_some();
        let (binding, message) = match reply_to {
            Some(to) => (to.binding, to.message),
            None => (binding, message),
        };
        Box::new(InlineChain {
            lane: self.shard.index,
            instance_id: pass.instance_id.clone(),
            process: Arc::clone(&process),
            deployment: pinned.clone(),
            binding,
            message,
            visited: pass.completed_nodes.iter().cloned().collect(),
            completed_nodes: pass.completed_nodes,
            waiting_nodes: pass.waiting_nodes,
            variables: pass.variables,
            start_node: pass.start_node,
            coverage: pass.coverage_progress,
            retry_attempts: pass.retry_attempts,
            retry_backoff: pass.retry_backoff,
            pending_timers: pass.timer_waits,
            emissions: Vec::new(),
            alias_rows: Vec::new(),
            held_aliases: false,
            counted: false,
            pooled: false,
            marked: false,
            settle: None,
            dedup_keys: self.dispatch_dedup.borrow_mut().drain(..).collect(),
            in_flight: BTreeSet::new(),
            audit_seq: self.audit_seq_for(&pass.instance_id),
            early_reply: None,
            stored: Some(StoredChain {
                claim_scope: claim_scope.clone(),
                prior: prior.clone(),
                satisfied: resolved_waits_for(&process, satisfied_node),
                labels,
                replies,
                relay_alias: pass.relay_alias,
            }),
            fresh_waits: pass.fresh_waits,
        })
    }

    /// One call's answer → the variables it resumes with: the same decode, `<q:source>` intake
    /// and channel-call output mapping a correlated reply gets, decoded with the OUTBOUND
    /// channel's codec (the answer arrives on the call's own connection, not on a reply
    /// channel).
    fn inline_response_variables(
        &self,
        chain: &InlineChain,
        node_id: &str,
        response: InlineResponse,
    ) -> Result<Variables, Diagnostic> {
        let channel = channel_call_name(&chain.process, node_id).unwrap_or_default();
        let module_key = chain.binding.namespace.module_key();
        let binding = self
            .channels
            .find(&module_key, &channel)
            .cloned()
            .unwrap_or_else(|| {
                ChannelBinding::new(
                    &channel,
                    chain.binding.namespace.clone(),
                    chain.deployment.clone(),
                    "",
                )
            });
        let message = InboundMessage {
            tenant: chain.message.tenant.clone(),
            module_key,
            channel: channel.clone(),
            headers: response.headers,
            body: sutra_executor::Sensitive::new(response.body),
            content_type: response.content_type,
            idempotency_key: String::new(),
            explicit_event_id: false,
            received_at: now_rfc3339(),
            cloud_event: None,
        };
        let decoded =
            self.chain
                .decode(&binding, &message.body, message.content_type.as_deref())?;
        let answer =
            self.relay_intake_variables(&chain.process, node_id, &binding, &decoded, &message)?;
        // A sync call's task rarely declares a `<q:source>` (the answer is bound to it by
        // construction), so its output mapping sees the decoded answer under `payload`, as the
        // `<q:source>` default name would. Only the mapping sees it: an unmapped answer merges as
        // before and never shadows the instance's own `payload`.
        let mut mapping_view = answer.clone();
        if mapping_view.get("payload").is_none() {
            if let Some(view) = decoded
                .as_ref()
                .and_then(|d| d.payload.as_ref())
                .map(crate::intake::payload_view)
            {
                mapping_view.insert("payload".to_string(), view);
            }
        }
        Ok(channel_call_output_mapping(&chain.process, node_id, &mapping_view)?.unwrap_or(answer))
    }

    /// A relayed message's variables: the projected `event` view plus the two-tier intake of
    /// the payload against the wait node's `<q:source>`. A rejection is the error.
    fn relay_intake_variables(
        &self,
        process: &ProcessDefinition,
        wait_node_id: &str,
        binding: &ChannelBinding,
        decoded: &Option<DecodeResult>,
        message: &InboundMessage,
    ) -> Result<Variables, Diagnostic> {
        let mut variables = project_variables(message);
        if let Some(decoded) = decoded {
            match self.chain.apply_decoded(
                process,
                wait_node_id,
                binding,
                decoded,
                &mut variables,
            )? {
                IntakeOutcome::Proceed { .. } => {}
                IntakeOutcome::Reject(d) => return Err(d),
                IntakeOutcome::Error { diagnostic, .. } => return Err(diagnostic),
            }
        }
        Ok(variables)
    }
}

impl ChannelEngineBuilder {
    /// Register the bindings (and per-channel payload caps) of every loaded channel
    /// definition. A duplicate channel URN with a differing binding panics — channel
    /// uniqueness is a load/lint-time invariant (`ChannelUniquenessValidator`), so a
    /// collision here is a genuine boot-time fail-closed condition.
    pub fn with_channel_definitions(mut self, definitions: &[ChannelDefinition]) -> Self {
        for def in definitions {
            if def.wants_reply_stream_sse() {
                self.engine.sse_channels.insert((
                    def.binding.namespace.module_key(),
                    def.binding.channel_name.clone(),
                ));
            }
            if let Some(cap) = def.payload_cap_bytes {
                self.engine
                    .payload_cap_policy
                    .set_channel_override(&def.binding.channel_name, cap);
            }
            self.register_session_end(&def.binding);
            self.engine
                .channels
                .register(def.binding.clone())
                .expect("channel registration must not collide (validated at load/lint time)");
        }
        self
    }

    /// Register a bare binding (tests / synthetic dispatch).
    pub fn with_binding(mut self, binding: ChannelBinding) -> Self {
        self.register_session_end(&binding);
        self.engine
            .channels
            .register(binding)
            .expect("channel registration must not collide (validated at load/lint time)");
        self
    }

    /// Index a `session-end: on-message` so a message on the named channel finds the session
    /// channels it ends.
    fn register_session_end(&mut self, binding: &ChannelBinding) {
        if let Some(target) = binding
            .session_end
            .as_ref()
            .and_then(|end| end.on_message.clone())
        {
            let sessions = self
                .engine
                .session_end_targets
                .entry((binding.namespace.module_key(), target))
                .or_default();
            if !sessions.contains(&binding.channel_name) {
                sessions.push(binding.channel_name.clone());
            }
        }
    }

    /// Wire the two-tier payload-cap policy (global default + per-channel overrides).
    /// Overrides already folded in from channel YAML are preserved unless the supplied
    /// policy carries its own for the same channel.
    pub fn with_payload_cap_policy(mut self, policy: PayloadCapPolicy) -> Self {
        self.engine.payload_cap_policy = policy;
        self
    }

    /// Wire the `${feature.X}` channel feature-gate provider.
    pub fn with_feature_provider(mut self, provider: Rc<dyn FeatureProvider>) -> Self {
        self.engine.feature_provider = Some(provider);
        self
    }

    /// Wire the per-tenant quota enforcer (consulted before the tenant-binding check).
    pub fn with_quota_enforcer(mut self, enforcer: Rc<dyn TenantQuotaEnforcer>) -> Self {
        self.engine.quota_enforcer = Some(enforcer);
        self
    }

    /// Wire the per-channel concurrency admission gauge.
    pub fn with_concurrency_store(mut self, store: Rc<dyn ConcurrencyStore>) -> Self {
        self.engine.concurrency = Some(store);
        self
    }

    /// Wire the capacity-pool store (`pool:` on a channel). The SAME `Arc` must be handed to
    /// every lane and to the outbox dispatcher, so all of them count one pool.
    pub fn with_capacity_pools(mut self, store: Arc<dyn CapacityPoolStore>) -> Self {
        self.engine.capacity = Some(store);
        self
    }

    /// Set this engine's lane in the shard router (default:
    /// [`crate::http::EngineShard::single`] — one lane, owns every id).
    pub fn with_shard(mut self, shard: crate::http::EngineShard) -> Self {
        self.engine.shard = shard;
        self
    }

    /// Adopt the router's per-lane counters (execution scale-out §6.1) so the pipeline's
    /// park/resume/handoff/claim-bounce increments land on the registry the exporter
    /// reads (default: a fresh, unobserved handle — the bare-builder posture).
    pub fn with_shard_metrics(
        mut self,
        metrics: std::sync::Arc<crate::shard_metrics::ShardLaneMetrics>,
    ) -> Self {
        self.engine.shard_metrics = metrics;
        self
    }

    /// Register every process of a loaded BPMN file under its deployment. The test/bare-builder
    /// path: it mutates through the shared handle, which is uncontended here because the registry
    /// has not been handed to any lane yet.
    pub fn with_module(mut self, deployment: &DeploymentId, module: &ProcessModule) -> Self {
        Arc::make_mut(&mut self.engine.processes).register(deployment, module);
        self
    }

    /// Adopt a pre-built process registry (shared with the executor's module resolver so
    /// coverage ops / call activities resolve against the same deployed set).
    /// The deployed process graphs. Takes anything convertible into the shared `Arc` — a plain
    /// owned registry (tests) or the one the activation built and every lane shares (the engine
    /// assembly; execution scale-out §2 row 10).
    pub fn with_process_registry(
        mut self,
        processes: impl Into<Arc<ProcessModuleRegistry>>,
    ) -> Self {
        self.engine.processes = processes.into();
        self
    }

    pub fn with_inbox(mut self, inbox: Rc<dyn InboxStore>) -> Self {
        self.engine.inbox = Some(inbox);
        self
    }

    pub fn with_alias_store(mut self, aliases: Rc<dyn AliasStore>) -> Self {
        self.engine.aliases = Some(aliases);
        self
    }

    pub fn with_outbox(mut self, outbox: Rc<dyn OutboxSink>) -> Self {
        self.engine.outbox = outbox;
        self
    }

    /// Wire the durable suspend→resume persistence bridge. Without it a
    /// wait-state inbound fails fast (`SUTRA.INBOUND.PERSISTENCE_REQUIRED`) and a relay
    /// channel falls through to the no-start-event error, exactly as when no
    /// `InstanceStore`/`RelayCorrelator` is wired.
    pub fn with_instance_bridge(mut self, bridge: Rc<dyn InstanceBridge>) -> Self {
        self.engine.bridge = Some(bridge);
        self
    }

    /// Wire the durable dead-letter / incident sink. Without it a NON-idempotent process's
    /// execution failure is still consumed (at-most-once) and logged at error level, but no durable
    /// incident row lands (the persistence-less posture, mirroring an absent inbox/alias store).
    pub fn with_incident_sink(mut self, incidents: Rc<dyn IncidentSink>) -> Self {
        self.engine.incidents = Some(incidents);
        self
    }

    /// A typed handle to the [`crate::audit::AuditListener`] that was ALSO registered on
    /// the executor as an `ExecutionListener`. The dispatcher uses it on the actor thread to
    /// persist the per-instance audit-seq high-water into the snapshot at suspend and to seed it
    /// back at resume. `None` (the default) when no audit sink is configured.
    pub fn with_audit_listener(mut self, listener: Rc<crate::audit::AuditListener>) -> Self {
        self.engine.audit_listener = Some(listener);
        self
    }

    /// Wire the engine-wide deferred-ack registry (`ack-mode: on-complete`
    /// broker transports). The SAME `Arc` must also observe the executor's listener bus
    /// (via [`crate::ack::DeferredAckListener`]) so terminal events settle the entries,
    /// and ride any sweep schedule. Without it (the default) `dispatch_deferred` settles
    /// every delivery immediately — the on-persist-equivalent posture.
    pub fn with_deferred_acks(mut self, registry: Arc<DeferredAckRegistry>) -> Self {
        self.engine.deferred_acks = Some(registry);
        self
    }

    /// Declare the DRAINING deployments (most recently drained first):
    /// relay correlation falls back to these scopes so instances pinned to a
    /// flipped-away deployment keep resuming until it retires. Their processes /
    /// artifacts must still be registered under their own deployment ids.
    pub fn with_prior_deployments(mut self, prior: Vec<DeploymentId>) -> Self {
        self.engine.prior_deployments = prior;
        self
    }

    /// Wire the inline callers that make SYNCHRONOUS outbound calls (`ack-mode: on-complete`
    /// outbound channels). Without them (the default) every channel-call takes the async path.
    pub fn with_inline_callers(mut self, callers: InlineCallerRegistry) -> Self {
        self.engine.inline_callers = callers;
        self
    }

    /// Wire the secret resolver an inline call's outbound auth reference resolves through —
    /// the same one the outbox dispatcher is given (default: `env:` only).
    pub fn with_inline_secret_env(
        mut self,
        secret_env: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.engine.inline_secret_env = Arc::new(secret_env);
        self
    }

    pub fn build(self) -> ChannelEngine {
        self.engine
    }
}

/// Rebuild the `"<tenant>/<module>/<version>"` module key from a step's authoring labels.
///
/// The resume path carries labels rather than the originating [`ChannelBinding`] — it is
/// answering a relay, not an intake — but the channel registry is keyed by module. The labels
/// are built from the namespace by `ChannelBinding::labels`, so this is that function read
/// backwards; a label the binding omitted (an empty module or version) comes back empty, which
/// is exactly what the key held in the first place.
/// Why a resumed step could not be committed — the one fact a resume path needs to decide
/// whether the instance is dead.
///
/// The executor's pass succeeded, but committing it runs checks of its own first, and those
/// checks are deterministic: a step they refuse is refused the same way on every retry. A store
/// error is the opposite case, since the same step may well commit a moment later. Folding both
/// into one `Diagnostic` left the callers unable to tell them apart, and they treated a refusal
/// as a store error: the timer poller re-fired the step every backoff tick, forever.
#[derive(Debug)]
enum ResumeCommitError {
    /// The engine refused to commit the step, before anything was written: an emission broke its
    /// contract (`SUTRA.OUTBOUND.VALIDATION_REJECT`), the relay reply could not render, or a fresh
    /// wait's alias could not be evaluated. The instance must fail.
    Refused(Diagnostic),
    /// Anything else: the store failed to commit, or the failure came after the step committed.
    /// The instance is not failed; the caller's own retry policy applies.
    Transient(Diagnostic),
}

fn module_key_of_labels(labels: &BTreeMap<String, String>) -> String {
    let get = |k: &str| labels.get(k).cloned().unwrap_or_default();
    format!("{}/{}/{}", get("tenant"), get("module"), get("version"))
}

/// Split a `"<tenant>/<module>/<version>"` module key back into its [`Namespace`].
///
/// The schedule row stores the key as one string (it is what every registry is keyed by); the
/// synthesized binding needs the three parts. A key that does not have three segments falls back
/// to the row's own tenant with empty module/version — the binding is only ever used for labels
/// and the tenant check, so a degraded label is strictly better than refusing to fire.
fn namespace_of_module_key(module_key: &str, tenant: &str) -> Namespace {
    let parts: Vec<&str> = module_key.splitn(3, '/').collect();
    match parts.as_slice() {
        [t, m, v] => Namespace::new(t, m, v),
        _ => Namespace::new(tenant, "", ""),
    }
}

/// The reserved channel name a SCHEDULED start dispatch runs under.
///
/// A timer start has no channel — nothing was delivered — but every downstream surface
/// (telemetry span, incident row, concurrency gauge, quota check) is keyed by one, so the
/// scheduler binds this reserved name instead of an empty string. It is not routable: a `:`
/// cannot appear in an authored channel name, so no YAML binding can collide with it and no
/// transport can ever serve it.
pub const SCHEDULE_CHANNEL: &str = "sutra:schedule";

/// One due timer-start occurrence, as handed to the engine by the schedule poller.
///
/// The deployment/process/node triple is the schedule row's identity; `tenant` and `module_key`
/// ride the row (rather than being re-derived here) so the synthesized dispatch binds exactly
/// the namespace the deployment was activated under, even mid-flip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledStartFire {
    pub deployment: DeploymentId,
    /// The tenant the started instance belongs to.
    pub tenant: String,
    /// `"<tenant>/<module>/<version>"`.
    pub module_key: String,
    /// Archive-local process id to start.
    pub process_id: String,
    /// The timer `<startEvent>` the schedule fires.
    pub node_id: String,
    /// RFC 3339 due timestamp the claimed row carried.
    pub due_at: String,
    /// RFC 3339 observation stamp of the actual fire (poller clock).
    pub fired_at: String,
}

/// Outcome of a [`ChannelEngine::fire_scheduled_start`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledStartOutcome {
    /// An instance was minted. `completed` distinguishes a flow that ran straight through from
    /// one that parked at a wait state — both are successful fires.
    Started {
        instance_id: String,
        completed: bool,
    },
    /// The row no longer names a live timer start in this deployment (the module changed, the
    /// process was removed, the start event stopped being timer-triggered) — the poller RESOLVES
    /// the row so it stops firing.
    Stale,
    /// The minted instance waits on synchronous calls — see [`DispatchOutcome::InlineCalls`].
    /// Consumed by the router side of [`crate::http::EngineHandle`], which answers the chain's
    /// final outcome instead; the poller never sees it.
    InlineCalls(InlineHandoff),
}

/// Outcome of a [`ChannelEngine::fire_timer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerFireOutcome {
    /// The fire resumed the instance (which then completed or re-parked); the resume step
    /// resolved the TIMER row.
    Resumed {
        instance_id: String,
        completed: bool,
    },
    /// The row no longer matches a live parked instance (already resumed / completed /
    /// model changed) — the poller resolves the row so it stops firing.
    Stale,
    /// The resumed instance waits on synchronous calls — consumed by the router side exactly
    /// like [`ScheduledStartOutcome::InlineCalls`].
    InlineCalls(InlineHandoff),
}

/// One evaluated `<q:alias>` row awaiting its step-commit, with the conflict policy kept
/// for the reject message.
struct EvaluatedAlias {
    record: AliasRecord,
    on_conflict: AliasConflict,
}

/// One poison wake — the outbox dispatcher terminally poisoned this `(instance, node)`'s
/// request delivery. A best-effort PROMPT: the engine re-derives the verdict from durable
/// facts under the instance claim ([`ChannelEngine::fail_channel_call`]).
#[derive(Debug, Clone)]
pub struct ChannelCallPoisonFire {
    /// The deployment the poisoned row belongs to (the emitting step's pin).
    pub deployment: DeploymentId,
    pub instance_id: String,
    /// The channel-call task whose request the row carried.
    pub node_id: String,
}

/// Outcome of one poison wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelCallPoisonOutcome {
    /// The failure consumed a `<q:retry>` budget slot and the backoff park committed — the
    /// re-drive will re-emit the request.
    Parked { instance_id: String },
    /// The budget was spent (or the poison code declared non-retryable): the durable FAILED
    /// snapshot committed.
    Failed { instance_id: String },
    /// Nothing to do — no policy, node not parked, already in backoff, instance
    /// gone/terminal/contended, or no durable poison evidence. Deliberately never an error:
    /// the wake is a prompt, and the `<q:timeout>` boundary is the guaranteed detector.
    NotApplicable,
}

/// Outcome of one relay-correlation attempt inside a single deployment scope.
enum RelayResolution {
    /// No wait node in this scope subscribes to the channel.
    NoTargets,
    /// A wait node subscribes but nothing correlated — the diagnostic to surface if no
    /// other scope resolves either.
    NotCorrelated(Diagnostic),
    /// Correlated, resumed and committed.
    Resumed(DispatchOutcome),
    /// Correlated to an instance ANOTHER shard's lane owns — nothing was claimed or
    /// executed; the resolved resume surfaces as [`DispatchOutcome::Handoff`] for the
    /// router to re-enqueue. Dead at the default `shard-count = 1`.
    Handoff(Box<ResolvedResume>),
}

// ---- the synchronous-call chain ------------------------------------------------------------

/// An instance in flight inside a synchronous-call chain — never persisted, so this IS the
/// instance until the chain parks or completes. It carries everything the chain's single
/// final commit needs (the emissions, alias rows, timer rows and settle callbacks accumulated
/// across passes) and the original delivery, whose binding and message the final reply is
/// built against. `Send`: it travels between the actor lane and the router-side task.
pub struct InlineChain {
    /// The lane that started the chain: every resume returns there, because the lane's
    /// in-memory alias index holds the chain's unique aliases.
    pub(crate) lane: u32,
    pub(crate) instance_id: String,
    process: Arc<ProcessDefinition>,
    deployment: DeploymentId,
    binding: ChannelBinding,
    message: InboundMessage,
    completed_nodes: Vec<String>,
    waiting_nodes: Vec<String>,
    variables: Variables,
    start_node: Option<String>,
    coverage: BTreeMap<String, u64>,
    retry_attempts: BTreeMap<String, u32>,
    retry_backoff: BTreeMap<String, String>,
    /// Every timer row still pending (fresh rows of each pass, minus those a resume resolved).
    pending_timers: Vec<TimerWait>,
    /// Emissions that are not synchronous requests — committed with the chain's last step.
    emissions: Vec<sutra_executor::Emission>,
    /// The start event's alias rows, recorded with the durable park.
    alias_rows: Vec<AliasRecord>,
    /// Whether the start's unique aliases are held in the lane's in-memory index.
    held_aliases: bool,
    /// Whether the chain holds an admission slot in the concurrency store.
    counted: bool,
    /// Whether the chain's start took a capacity-pool slot (bound to the instance at launch).
    pooled: bool,
    /// Whether the durable in-flight marker was written.
    marked: bool,
    settle: Option<DeferredSettle>,
    /// The delivery's inbox dedup keys, released if the chain fails.
    dedup_keys: Vec<(String, String)>,
    /// Synchronous calls issued and not yet answered.
    in_flight: BTreeSet<String>,
    /// Every node visited so far — the reply is gated on a visited `<q:reply>`, which a later
    /// pass replays rather than visits.
    visited: std::collections::HashSet<String>,
    audit_seq: u32,
    /// A respond-and-continue reply produced mid-chain, returned when the chain ends.
    early_reply: Option<SyncReply>,
    /// Set when the chain drives a STORED instance (a relay or timer resume): the chain ends
    /// with the commits that fit a stored row instead of a first park.
    stored: Option<StoredChain>,
    /// The waits the chain's passes parked as NEW waits and that are still waiting — the ones
    /// the chain's durable park records fresh wait and alias rows for. A wait the stored
    /// snapshot already had and no pass re-entered is not here: its rows already exist.
    fresh_waits: Vec<String>,
}

impl InlineChain {
    /// The labels the chain's emissions carry: a resume's own, else the start binding's.
    fn labels(&self) -> BTreeMap<String, String> {
        match &self.stored {
            Some(stored) => stored.labels.clone(),
            None => self.binding.labels(),
        }
    }
}

/// What a chain over a stored instance keeps besides the in-memory state.
struct StoredChain {
    /// The deployment the instance's claim was taken under (a DRAINING-scope resume claims
    /// under the scope, not the pin).
    claim_scope: DeploymentId,
    /// The snapshot as loaded: the frontier the instance returns to if the chain aborts, and
    /// the key id and indexed waits the final re-park keeps.
    prior: SuspendedInstance,
    /// Every wait the chain's passes satisfied — resolved by the final re-park.
    satisfied: Vec<String>,
    labels: BTreeMap<String, String>,
    /// The chain answers a relay delivery (its binding and message are the chain's).
    replies: bool,
    /// The `(name, value)` alias the relay that resumed the instance correlated on, if any —
    /// retired with the satisfied wait it belonged to.
    relay_alias: Option<(String, String)>,
}

/// The fields of the resume pass that reached a chain's first synchronous calls.
struct StoredPass {
    instance_id: String,
    waiting_nodes: Vec<String>,
    completed_nodes: Vec<String>,
    variables: Variables,
    start_node: Option<String>,
    timer_waits: Vec<TimerWait>,
    coverage_progress: BTreeMap<String, u64>,
    retry_attempts: BTreeMap<String, u32>,
    retry_backoff: BTreeMap<String, String>,
    fresh_waits: Vec<String>,
    relay_alias: Option<(String, String)>,
}

fn no_bridge() -> Diagnostic {
    Diagnostic::error(
        codes::INBOUND_PERSISTENCE_REQUIRED,
        "a synchronous call chain reached a durable commit with no bridge wired",
    )
}

const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<InlineChain>();
};

/// A freshly waiting channel-call on a synchronous outbound channel.
struct SyncCallSite {
    node_id: String,
    caller: Option<Arc<dyn InlineCaller>>,
}

/// What one in-memory resume pass applies.
enum ChainPass {
    /// The call's answer, already through intake and output mapping.
    Answer(Variables),
    /// The call's timeout: fire this timer boundary.
    Timeout(String),
    /// The call failed (its inline retries, if any, spent): the task's failure path.
    Failure(Diagnostic),
}

/// A capacity-pool slot a start took, still keyed by its admission key.
#[derive(Debug, Clone)]
struct PoolAdmission {
    deployment: DeploymentId,
    key: String,
}

/// How a chain ended, for what it releases.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ChainEnd {
    Parked,
    Completed,
    /// The flow failed (a start is dead-lettered or requeued; a stored instance is FAILED).
    Failed,
    /// No quiescent commit was reached — nothing changed durably.
    Aborted,
}

/// The outbound channel a `channel:<name>` service task calls.
fn channel_call_name(process: &ProcessDefinition, node_id: &str) -> Option<String> {
    match process.node(node_id) {
        Ok(Node::ServiceTask { implementation, .. }) => implementation
            .strip_prefix(CHANNEL_CALL_PREFIX)
            .map(|c| c.trim().to_string()),
        _ => None,
    }
}

/// The interrupting timer boundaries on a call, with how long each allows from now.
///
/// A FEEL-expression boundary (an expression `<q:timeout>` among them) was evaluated when the
/// executor armed it, in the same pass that issued the call, so its budget is read from that
/// armed row in `pending` rather than evaluated a second time.
fn call_timeouts(
    process: &ProcessDefinition,
    node_id: &str,
    pending: &[TimerWait],
) -> Vec<(String, std::time::Duration)> {
    let mut out = Vec::new();
    for n in process.nodes() {
        let Node::BoundaryEvent {
            id,
            kind: sutra_bpmn::model::BoundaryKind::Timer,
            attached_to_ref,
            interrupting: true,
            timer: Some(timer),
            ..
        } = n
        else {
            continue;
        };
        if attached_to_ref != node_id {
            continue;
        }
        let allowed = match timer {
            sutra_bpmn::timer::TimerDefinition::Duration(spec) => {
                sutra_bpmn::duration::parse_iso8601_duration(spec).ok()
            }
            sutra_bpmn::timer::TimerDefinition::Date(at) => {
                time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339)
                    .ok()
                    .map(|due| {
                        let left = due - time::OffsetDateTime::now_utc();
                        std::time::Duration::try_from(left).unwrap_or_default()
                    })
            }
            sutra_bpmn::timer::TimerDefinition::Cycle(_) => None,
            sutra_bpmn::timer::TimerDefinition::Expression { .. } => pending
                .iter()
                .find(|t| &t.node_id == id)
                .and_then(|t| {
                    time::OffsetDateTime::parse(
                        &t.due_at,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .ok()
                })
                .map(|due| {
                    let left = due - time::OffsetDateTime::now_utc();
                    std::time::Duration::try_from(left).unwrap_or_default()
                }),
        };
        if let Some(allowed) = allowed {
            out.push((id.clone(), allowed));
        }
    }
    out
}

/// A synchronous call's whole budget: its earliest interrupting timer boundary (the loader
/// guarantees every channel-call has one).
fn inline_call_budget(
    process: &ProcessDefinition,
    node_id: &str,
    pending: &[TimerWait],
) -> std::time::Duration {
    call_timeouts(process, node_id, pending)
        .into_iter()
        .map(|(_, d)| d)
        .min()
        .unwrap_or(std::time::Duration::from_secs(30))
}

/// The boundary a synchronous call's timeout fires — the one that set its budget.
fn inline_timeout_boundary(
    process: &ProcessDefinition,
    node_id: &str,
    pending: &[TimerWait],
) -> Option<String> {
    call_timeouts(process, node_id, pending)
        .into_iter()
        .min_by_key(|(_, d)| *d)
        .map(|(id, _)| id)
}

/// A call task's `<q:retry>` resolved to durations: literals were validated at load; a FEEL
/// delay is evaluated now, as the synchronous call arms its policy, against the chain's
/// variables plus `config`. `Err` carries why an expression could not be armed.
fn inline_retry(
    executor: &sutra_executor::TokenExecutor,
    chain: &InlineChain,
    node_id: &str,
) -> Result<Option<InlineRetry>, String> {
    let Some(policy) = chain.process.retry_policy(node_id) else {
        return Ok(None);
    };
    let (initial_delay, max_delay) =
        executor.resolve_retry_delays(&chain.deployment, policy, &chain.variables)?;
    Ok(Some(InlineRetry {
        max_attempts: policy.max_attempts,
        initial_delay,
        backoff_coefficient: policy.backoff_coefficient,
        max_delay,
        non_retryable_codes: policy.non_retryable_codes.clone(),
    }))
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

// ---- channel-call / timer helpers ----------------------------------------------------------

/// [`TimerWait`]s (executor shape, RFC 3339 due-at) → bridge records.
fn timer_records(timer_waits: &[TimerWait]) -> Vec<TimerWaitRecord> {
    timer_waits
        .iter()
        .map(|t| TimerWaitRecord {
            node_id: t.node_id.clone(),
            due_at: t.due_at.clone(),
        })
        .collect()
}

/// The wait node a correlated relay on `channel` satisfies: `named` (the first subscriber in
/// model order) when the instance waits there, else the waiting node of the same process that
/// subscribes `channel` — with the decoded payload moved from `named`'s `<q:source name>` to
/// that node's. Unchanged when no other waiting node subscribes (the existing posture).
fn rebind_relay_to_waiting(
    process: Option<&ProcessDefinition>,
    waiting: &[String],
    named: &str,
    channel: &str,
    mut relay_vars: Variables,
) -> (String, Variables) {
    let Some(process) = process else {
        return (named.to_string(), relay_vars);
    };
    if waiting.iter().any(|w| w == named) {
        return (named.to_string(), relay_vars);
    }
    let subscribes = |node_id: &str| -> bool {
        match process.node(node_id) {
            Ok(Node::MessageCatchEvent { channels, .. })
            | Ok(Node::ReceiveTask { channels, .. })
            | Ok(Node::UserTask { channels, .. }) => channels.iter().any(|c| c == channel),
            Ok(Node::ServiceTask { implementation, .. })
                if implementation.starts_with(CHANNEL_CALL_PREFIX) =>
            {
                process
                    .bindings_for(node_id)
                    .sources
                    .iter()
                    .any(|s| s.channel == channel)
            }
            _ => process
                .bindings_for(node_id)
                .sources
                .iter()
                .any(|s| s.channel == channel),
        }
    };
    let Some(waiting_node) = waiting.iter().find(|w| subscribes(w)) else {
        return (named.to_string(), relay_vars);
    };
    let name_of = |node_id: &str| {
        process
            .bindings_for(node_id)
            .sources
            .iter()
            .find(|s| s.channel == channel)
            .map(|s| s.name.clone())
    };
    if let (Some(from), Some(to)) = (name_of(named), name_of(waiting_node)) {
        if from != to {
            if let Some(value) = relay_vars.get(&from).cloned() {
                relay_vars.remove(&from);
                relay_vars.insert(to, value);
            }
        }
    }
    (waiting_node.clone(), relay_vars)
}

/// The wait rows a resume step resolves: the satisfied node itself, the HOST's row when
/// the satisfied node is a fired timer boundary, and every (other) timer boundary armed on
/// that host — a satisfied host consumes its pending timers (the response cancels the
/// timeout; the timeout cancels the wait).
fn resolved_waits_for(process: &ProcessDefinition, satisfied: &str) -> Vec<String> {
    let mut out = vec![satisfied.to_string()];
    // An event-based gateway is a RACE: the arm that fired wins and every sibling arm must stop
    // waiting. Retiring them here puts it in the same transaction as the winner's advance, which
    // is what makes first-wins atomic — a losing timer row left pending would fire later and send
    // a second token through a gateway that already resolved.
    //
    // The group is static model structure (the gateway's outgoing targets), so nothing has to be
    // remembered across the park: no snapshot field, no counter, nothing to get out of step.
    for gw in process.nodes() {
        if !matches!(gw, Node::EventBasedGateway { .. }) {
            continue;
        }
        let arms: Vec<&str> = process
            .outgoing(gw.id())
            .iter()
            .map(|f| f.target_ref.as_str())
            .collect();
        if arms.contains(&satisfied) {
            for arm in arms {
                if arm != satisfied {
                    out.push(arm.to_string());
                }
            }
        }
    }
    let host = match process.node(satisfied) {
        Ok(Node::BoundaryEvent {
            attached_to_ref, ..
        }) => {
            out.push(attached_to_ref.clone());
            attached_to_ref.clone()
        }
        _ => satisfied.to_string(),
    };
    for n in process.nodes() {
        if let Node::BoundaryEvent {
            id,
            kind: sutra_bpmn::model::BoundaryKind::Timer,
            attached_to_ref,
            ..
        } = n
        {
            if attached_to_ref == &host && id != satisfied {
                out.push(id.clone());
            }
        }
    }
    out
}

/// Evaluate the declared `<q:alias>` rows of every wait in `fresh_waits` — the waits this pass
/// parked as NEW waits (a first arrival, or a loop that brought the token back). Waits carried
/// over unchanged from a prior pass were indexed by their own park step and are not here.
///
/// A CHANNEL-CALL park with ZERO resolvable alias rows is refused — its response could never
/// resume it. Any other wait node (a message catch, a receive task, a user task) indexes the
/// aliases that resolve and skips the rest: it can still be correlated through the start
/// event's `onConflict="correlate"` alias, so an unresolvable expression there is not fatal.
fn wait_alias_rows(
    process: &ProcessDefinition,
    fresh_waits: &[String],
    variables: &Variables,
) -> Result<Vec<AliasRecord>, Diagnostic> {
    let mut out = Vec::new();
    let ctx = variables.to_feel_context();
    for node_id in fresh_waits {
        let channel_call = match process.node(node_id) {
            Ok(Node::ServiceTask { implementation, .. }) => {
                if !implementation.starts_with(CHANNEL_CALL_PREFIX) {
                    continue;
                }
                true
            }
            Ok(
                Node::MessageCatchEvent { .. } | Node::ReceiveTask { .. } | Node::UserTask { .. },
            ) => false,
            _ => continue,
        };
        let aliases = &process.bindings_for(node_id).aliases;
        if !channel_call {
            for alias in aliases {
                let Ok(value) = sutra_feel::expressions::eval(&alias.expression, &ctx) else {
                    continue;
                };
                push_alias_values(&mut out, alias, &value);
            }
            continue;
        }
        let mut resolved_any = false;
        for alias in aliases {
            let value = sutra_feel::expressions::eval(&alias.expression, &ctx).map_err(|e| {
                Diagnostic::error(
                    codes::INBOUND_ALIAS_FEEL_EVAL_FAILED,
                    format!(
                        "<q:alias {}> expression '{}' on channel-call task {node_id} threw \
                         at park-time evaluation: {}",
                        alias.name, alias.expression, e.message
                    ),
                )
            })?;
            resolved_any |= push_alias_values(&mut out, alias, &value);
        }
        if !resolved_any {
            return Err(Diagnostic::error(
                codes::DISPATCH_CHANNEL_CALL_ALIAS_UNRESOLVED,
                format!(
                    "Channel-call task '{node_id}' of process '{}' parked but none of its \
                     declared <q:alias> expressions resolved to a value — the correlated \
                     response could never resume it; the step is refused (fail closed).",
                    process.id
                ),
            ));
        }
    }
    Ok(out)
}

/// Append the alias row(s) one evaluated `<q:alias>` value yields — one per non-null list item
/// for a `multi` alias, else one for a non-null value. Returns whether any row was added.
fn push_alias_values(out: &mut Vec<AliasRecord>, alias: &AliasBinding, value: &FeelValue) -> bool {
    let mut added = false;
    match value {
        FeelValue::Null => {}
        FeelValue::List(items) if alias.multi => {
            for item in items.iter().filter(|i| !i.is_null()) {
                out.push(AliasRecord {
                    name: alias.name.clone(),
                    value: sutra_feel::value::canonical_string_of(item),
                    unique: alias.unique,
                });
                added = true;
            }
        }
        other => {
            out.push(AliasRecord {
                name: alias.name.clone(),
                value: sutra_feel::value::canonical_string_of(other),
                unique: alias.unique,
            });
            added = true;
        }
    }
    added
}

/// The wait rows a resume step resolves ([`resolved_waits_for`]) plus every wait the pass
/// parked fresh: a node that parks a NEW wait replaces whatever row it had (a loop's previous
/// iteration), so the step resolves the old row and records the new one in the same commit.
fn resolved_with_fresh(
    process: &ProcessDefinition,
    satisfied: &str,
    fresh_waits: &[String],
) -> Vec<String> {
    let mut out = resolved_waits_for(process, satisfied);
    for node in fresh_waits {
        if !out.contains(node) {
            out.push(node.clone());
        }
    }
    out
}

/// The alias rows a resume step RETIRES: the correlation keys of the waits it resolved, so a
/// late relay for a wait the instance has left (an earlier loop iteration) no longer finds the
/// instance at all.
///
/// The keys are re-derived the way the park step derived them — each resolved wait node's
/// declared `<q:alias>` evaluated over the snapshot variables the park step saw — plus the
/// `(name, value)` the resuming relay actually correlated on. Never retired:
/// - a start-event alias name (the instance's business key lives for the whole instance);
/// - a key this same step records again (a loop whose iteration reuses the value);
/// - a key a wait that is still pending also answers to.
fn retired_wait_aliases(
    process: &ProcessDefinition,
    resolved: &[String],
    prior: &SuspendedInstance,
    waiting_now: &[String],
    fresh_waits: &[String],
    recorded: &[AliasRecord],
    relay_alias: Option<&(String, String)>,
) -> Vec<AliasRecord> {
    let mut prior_vars = Variables::new();
    for (name, value) in &prior.variables {
        prior_vars.insert(name.clone(), value.clone());
    }
    let ctx = prior_vars.to_feel_context();
    let keys_of = |node: &str| -> Vec<AliasRecord> {
        let mut rows = Vec::new();
        for alias in &process.bindings_for(node).aliases {
            if let Ok(value) = sutra_feel::expressions::eval(&alias.expression, &ctx) {
                push_alias_values(&mut rows, alias, &value);
            }
        }
        rows
    };
    let start_names: BTreeSet<&str> = process
        .nodes()
        .iter()
        .filter(|n| matches!(n, Node::StartEvent { .. }))
        .flat_map(|n| process.bindings_for(n.id()).aliases.iter())
        .map(|a| a.name.as_str())
        .collect();
    let mut keep: BTreeSet<(String, String)> = recorded
        .iter()
        .map(|r| (r.name.clone(), r.value.clone()))
        .collect();
    for node in waiting_now.iter().filter(|n| !fresh_waits.contains(n)) {
        keep.extend(keys_of(node).into_iter().map(|r| (r.name, r.value)));
    }
    let mut candidates: Vec<AliasRecord> = resolved
        .iter()
        .filter(|n| prior.waiting_nodes.contains(n))
        .flat_map(|n| keys_of(n))
        .collect();
    if let Some((name, value)) = relay_alias {
        candidates.push(AliasRecord {
            name: name.clone(),
            value: value.clone(),
            unique: false,
        });
    }
    let mut out: Vec<AliasRecord> = Vec::new();
    for row in candidates {
        let key = (row.name.clone(), row.value.clone());
        if start_names.contains(row.name.as_str())
            || keep.contains(&key)
            || out
                .iter()
                .any(|o| o.name == row.name && o.value == row.value)
        {
            continue;
        }
        out.push(row);
    }
    out
}

/// The wait node's OWN declared `<q:alias>` correlation (the channel-call park key):
/// the first alias on `wait_node_id` whose expression resolves
/// non-blank against the relayed context, regardless of `onConflict` (the alias on a task
/// exists solely to correlate).
fn correlation_for_wait_node(
    process: &ProcessDefinition,
    wait_node_id: &str,
    ctx: &Variables,
) -> Option<(String, String)> {
    let feel_ctx = ctx.to_feel_context();
    for alias in &process.bindings_for(wait_node_id).aliases {
        let Ok(value) = sutra_feel::expressions::eval(&alias.expression, &feel_ctx) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let s = sutra_feel::value::canonical_string_of(&value);
        if !s.trim().is_empty() {
            return Some((alias.name.clone(), s));
        }
    }
    None
}

/// A channel-call task's DECLARED output mapping, applied to the
/// correlated response context: `<assignment><from>FEEL</from><to>var</to></assignment>`
/// rows evaluate against the response; plain `outputs` names copy through. `Ok(None)`
/// when the target declares no output mapping (full-merge compatibility).
fn channel_call_output_mapping(
    process: &ProcessDefinition,
    wait_node_id: &str,
    relay_vars: &Variables,
) -> Result<Option<Variables>, Diagnostic> {
    let Ok(Node::ServiceTask {
        implementation,
        data_mapping,
        ..
    }) = process.node(wait_node_id)
    else {
        return Ok(None);
    };
    if !implementation.starts_with(CHANNEL_CALL_PREFIX)
        || (data_mapping.outputs.is_empty() && data_mapping.assignments.is_empty())
    {
        return Ok(None);
    }
    let ctx = relay_vars.to_feel_context();
    let mut mapped = Variables::new();
    for a in &data_mapping.assignments {
        let value = sutra_feel::expressions::eval(&a.expression, &ctx).map_err(|e| {
            Diagnostic::error(
                codes::DISPATCH_CHANNEL_CALL_OUTPUT_MAPPING_FAILED,
                format!(
                    "Channel-call task '{wait_node_id}' output mapping '{}' → '{}' failed \
                     against the response payload: {}",
                    a.expression, a.target_var, e.message
                ),
            )
        })?;
        if !value.is_null() {
            mapped.insert(a.target_var.clone(), value);
        }
    }
    for name in &data_mapping.outputs {
        if let Some(v) = relay_vars.get(name) {
            mapped.insert(name.clone(), v.clone());
        }
    }
    Ok(Some(mapped))
}

fn evaluated_alias(alias: &AliasBinding, value: &FeelValue) -> EvaluatedAlias {
    EvaluatedAlias {
        record: AliasRecord {
            name: alias.name.clone(),
            value: sutra_feel::value::canonical_string_of(value),
            unique: alias.unique,
        },
        on_conflict: alias.on_conflict.unwrap_or(AliasConflict::Reject),
    }
}

/// Convert the drained executor emissions to the step-commit outbox payloads: every
/// emission is destination-bearing by construction; each gets a freshly
/// minted `outbox_key`; the emitting binding's authoring labels ride as payload data;
/// the inbound `traceparent` (when the client sent one — [`inbound_traceparent`])
/// persists for the delivery-span bridge. Timer-fire steps run outside any
/// channel binding: labels ride empty, no traceparent.
fn to_outbox_emissions(
    emissions: Vec<sutra_executor::Emission>,
    labels: BTreeMap<String, String>,
    traceparent: Option<String>,
) -> Vec<OutboxEmission> {
    if emissions.is_empty() {
        return Vec::new();
    }
    emissions
        .into_iter()
        .map(|emission| OutboxEmission {
            instance_id: emission.instance_id,
            node_id: emission.node_id,
            destination: emission.destination,
            // Both sides are `Sensitive<Vec<u8>>` — a plain field carry stays masked all the
            // way to the outbox row (encrypted separately at rest if the channel is sensitive).
            body: emission.body,
            content_type: emission.content_type,
            required: emission.required,
            mode: emission.mode,
            outbox_key: new_uuid(),
            cloud_event_json: emission.cloud_event.as_ref().map(cloud_event_to_json),
            auth_ref_json: emission.auth_ref.as_ref().map(auth_ref_to_json),
            labels: labels.clone(),
            traceparent: traceparent.clone(),
            headers: emission.headers,
        })
        .collect()
}

/// The inbound `traceparent` header (case-insensitive), trimmed, when the client sent a
/// non-empty one — the value the step's outbox rows persist for the trace-context bridge.
fn inbound_traceparent(message: &InboundMessage) -> Option<String> {
    message
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(sutra_executor::telemetry::TRACEPARENT_HEADER))
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The variable map the snapshot persists, TYPED — each value handed to the store as the value it
/// is, so a wait state stops being the point where `amount` turns into the string `"42"`.
///
/// Nothing is flattened here any more. The store decides what it can carry (see the persistence
/// crate's value model); this side only names the values.
fn persisted_variable_values(variables: &Variables) -> Vec<(String, FeelValue)> {
    variables
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

/// Apply the transient/sensitive discipline to a park's variables. `@transient`
/// variables are DROPPED (never persisted — held in memory only, gone on resume; a fail-closed
/// package lint rejects reading one after a wait state). The surviving `@sensitive` variable
/// names are returned for the snapshot's `sutra.sensitive` marking so audit/log/diagnostics
/// layers redact their values. The common case (no transient/sensitive declared) is untouched.
/// Partitions the variables for persistence: drops `@transient`, returns the kept
/// `(name, value)` pairs, the `@sensitive` marker names (observability redaction), the at-rest
/// **encrypt-set** = `@sensitive` ∪ redactor-controlled (a var with a `<name>.redacted` companion is
/// redaction-controlled, so its raw value must be ciphertext at rest — the companion itself stays
/// plaintext), and the **subjects** = `(name, raw value)` of each `@subjectKey` variable present
/// (for GDPR blind-indexing).
///
/// The kept values are typed; the SUBJECT values deliberately are not. A subject value is the
/// input to an HMAC blind index that is already persisted for live instances, so it must keep
/// hashing the exact string it hashed before variables carried types — anything else silently
/// orphans every index row written to date.
fn persisted_variables(
    declared: &[DeclaredVariable],
    values: Vec<(String, FeelValue)>,
) -> PersistedVariables {
    let transient: BTreeSet<&str> = declared
        .iter()
        .filter(|d| d.transient)
        .map(|d| d.name.as_str())
        .collect();
    let sensitive: BTreeSet<&str> = declared
        .iter()
        .filter(|d| d.sensitive)
        .map(|d| d.name.as_str())
        .collect();
    let subject_keys: BTreeSet<&str> = declared
        .iter()
        .filter(|d| d.subject_key)
        .map(|d| d.name.as_str())
        .collect();
    let mut kept = Vec::with_capacity(values.len());
    let mut marked = Vec::new();
    let mut subjects = Vec::new();
    for (name, value) in values {
        if transient.contains(name.as_str()) {
            continue; // @transient never persisted
        }
        if sensitive.contains(name.as_str()) {
            marked.push(name.clone());
        }
        if subject_keys.contains(name.as_str()) {
            subjects.push((name.clone(), blind_index_input(&value)));
        }
        kept.push((name, value));
    }

    // Encrypt-set: @sensitive names plus every base var that has a persisted `<base>.redacted`
    // companion (redactor-controlled). Only names actually kept can be encrypted.
    let mut encrypt: BTreeSet<String> = marked.iter().cloned().collect();
    for (name, _) in &kept {
        if let Some(base) = name.strip_suffix(sutra_bpmn::REDACTION_COMPANION_SUFFIX) {
            if kept.iter().any(|(n, _)| n.as_str() == base) {
                encrypt.insert(base.to_string());
            }
        }
    }
    PersistedVariables {
        kept,
        sensitive: marked,
        encrypt_names: encrypt.into_iter().collect(),
        subjects,
    }
}

/// The exact string a `@subjectKey` value used to be persisted as, and therefore the only string
/// its blind index may ever be computed over. Frozen deliberately — see [`persisted_variables`].
fn blind_index_input(value: &FeelValue) -> String {
    if value.is_null() {
        String::new()
    } else {
        sutra_feel::value::canonical_string_of(value)
    }
}

/// The result of [`persisted_variables`] — the persist-ready variable partition + the DLP metadata.
struct PersistedVariables {
    kept: Vec<(String, FeelValue)>,
    sensitive: Vec<String>,
    encrypt_names: Vec<String>,
    subjects: Vec<(String, String)>,
}

/// The correlation (alias name + value) a relay carries — the first
/// `onConflict=correlate` `<q:alias>` on a start event of `process` whose expression
/// resolves non-blank against the relayed context (the correlation lookup). An alias
/// whose expression fails to evaluate is skipped (uncorrelatable ⇒ the wait is the safe
/// state).
///
/// A start-event alias is written in the start event's own names: `payload.draftId` reads the
/// payload under the start event's `<q:source name>`. The wait that receives the relay may bind
/// its payload under a different name (`<q:source name="revision">`), which is a local choice of
/// that wait and must not change which instance the message correlates to. So the relayed
/// payload (`relay_payload`, the wait's source name) is also made visible under the start
/// event's payload name for the evaluation, unless the relayed context already defines that name.
fn correlation_for(
    process: &ProcessDefinition,
    ctx: &Variables,
    relay_payload: Option<&str>,
) -> Option<(String, String)> {
    for start in process.start_events() {
        let bindings = process.bindings_for(start.id());
        let start_payload = bindings.source().map(|source| source.name.as_str());
        let feel_ctx = match (relay_payload, start_payload) {
            (Some(relayed), Some(start_name))
                if relayed != start_name && !ctx.contains_key(start_name) =>
            {
                let mut aliased = ctx.clone();
                if let Some(value) = ctx.get(relayed) {
                    aliased.insert(start_name, value.clone());
                }
                aliased.to_feel_context()
            }
            _ => ctx.to_feel_context(),
        };
        for alias in &bindings.aliases {
            if alias.on_conflict != Some(AliasConflict::Correlate) {
                continue;
            }
            let Ok(value) = sutra_feel::expressions::eval(&alias.expression, &feel_ctx) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let s = sutra_feel::value::canonical_string_of(&value);
            if !s.trim().is_empty() {
                return Some((alias.name.clone(), s));
            }
        }
    }
    None
}

/// The initial-variable map — the projected `event` sub-map. `event.body`
/// is a (lossy) UTF-8 string: `FeelValue` has no bytes variant, and every text-wire flow
/// reads it verbatim.
fn project_variables(message: &InboundMessage) -> Variables {
    let mut event = BTreeMap::new();
    event.insert(
        "channel".to_string(),
        FeelValue::String(message.channel.clone()),
    );
    event.insert(
        "contentType".to_string(),
        FeelValue::String(message.content_type.clone().unwrap_or_default()),
    );
    event.insert(
        "body".to_string(),
        FeelValue::String(String::from_utf8_lossy(&message.body).into_owned()),
    );
    event.insert(
        "headers".to_string(),
        FeelValue::Map(
            message
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), FeelValue::String(v.clone())))
                .collect(),
        ),
    );
    // The resolved dedup key value. Exposed under the canonical `event.dedupKey` (matching
    // the renamed `<q:source dedupKey>` attribute) AND the retained `event.idempotencyKey` alias
    // (backward-compat for existing author FEEL expressions). Both carry the same value.
    event.insert(
        "dedupKey".to_string(),
        FeelValue::String(message.idempotency_key.clone()),
    );
    event.insert(
        "idempotencyKey".to_string(),
        FeelValue::String(message.idempotency_key.clone()),
    );
    event.insert(
        "receivedAt".to_string(),
        FeelValue::String(message.received_at.clone()),
    );
    if let Some(ce) = message.cloud_event.as_deref() {
        event.insert("cloudEvent".to_string(), cloud_event_to_feel(ce));
    }
    // Expose the inbound headers as a top-level `header` map so a
    // `<q:alias expression="header.<field>">` (and any FEEL over the inbound) can correlate on a
    // header attribute the sender set, symmetric with `<q:source>` reading `header.<field>` for its
    // dedupKey. Additive: `event.headers` stays as-is. Present at BOTH alias-resolution sites
    // (start-event spawn + imec relay-wait) because both build their FEEL context from here.
    let header_map: sutra_feel::FeelContext = message
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), FeelValue::String(v.clone())))
        .collect();
    let mut variables = Variables::new();
    variables.insert("event", FeelValue::Map(event));
    variables.insert("header", FeelValue::Map(header_map));
    variables
}

/// Project a [`crate::cloudevents::CloudEvent`] into the `event.cloudEvent` FEEL map under the
/// camel-cased attribute names (`id`, `source`, `specVersion`, `type`, `subject`, `time`,
/// `dataContentType`, `dataSchema`, `extensions`). Absent attributes are omitted.
fn cloud_event_to_feel(ce: &crate::cloudevents::CloudEvent) -> FeelValue {
    let mut map = BTreeMap::new();
    let mut put = |key: &str, value: &Option<String>| {
        if let Some(v) = value {
            map.insert(key.to_string(), FeelValue::String(v.clone()));
        }
    };
    put("id", &ce.id);
    put("source", &ce.source);
    put("specVersion", &ce.spec_version);
    put("type", &ce.event_type);
    put("subject", &ce.subject);
    put("time", &ce.time);
    put("dataContentType", &ce.data_content_type);
    put("dataSchema", &ce.data_schema);
    if !ce.extensions.is_empty() {
        map.insert(
            "extensions".to_string(),
            FeelValue::Map(
                ce.extensions
                    .iter()
                    .map(|(k, v)| (k.clone(), FeelValue::String(v.clone())))
                    .collect(),
            ),
        );
    }
    FeelValue::Map(map)
}

/// Apply a `<q:source dedupKey="body.<path>">` spec against the DECODED payload:
/// the path binds the decoded body under `body`, and a non-blank result (a) overrides the
/// `event.idempotencyKey` FEEL variable and (b) is RETURNED so the caller can drive inbox dedup on
/// it (a body-path dedup key now actually deduplicates — previously it only re-projected the FEEL
/// variable, after the transport-side dedup check, so it never suppressed a redelivery). A blank /
/// non-`body.` spec returns `None` (header.* / ce.id forms are resolved transport-side, upstream).
fn apply_body_path_dedup_key(
    process: &ProcessDefinition,
    start_id: &str,
    variables: &mut Variables,
) -> Option<String> {
    let bindings = process.bindings_for(start_id);
    let source = bindings.source()?;
    let spec = source.dedup_key.as_deref()?;
    let spec = spec.trim();
    if !spec.starts_with("body.") {
        return None; // header.* / ce.id specs are transport-side
    }
    // The projected payload view — unwrap an envelope's `body` key so `body.GrpHdr.MsgId`
    // navigates the structured message in both shapes.
    let payload = variables.get(&source.name).cloned()?;
    let decoded_body = match &payload {
        FeelValue::Map(m) if m.contains_key("body") => m.get("body").cloned().expect("checked"),
        other => other.clone(),
    };
    let mut ctx = sutra_feel::FeelContext::new();
    ctx.insert("body".to_string(), decoded_body);
    let resolved = match sutra_feel::expressions::eval(spec, &ctx) {
        Ok(v) if !v.is_null() => sutra_feel::value::canonical_string_of(&v),
        _ => return None,
    };
    if resolved.trim().is_empty() {
        return None;
    }
    if let Some(FeelValue::Map(event)) = variables.get("event") {
        let mut event = event.clone();
        event.insert("dedupKey".to_string(), FeelValue::String(resolved.clone()));
        event.insert(
            "idempotencyKey".to_string(),
            FeelValue::String(resolved.clone()),
        );
        variables.insert("event", FeelValue::Map(event));
    }
    Some(resolved)
}

/// The `contentType` declared on a VISITED `<q:reply>` node with no destination (the
/// synchronous reply) — the reply content type the native wire mode serves.
fn visited_reply_content_type(
    process: &ProcessDefinition,
    exec: &sutra_executor::ExecResult,
) -> Option<String> {
    for node in process.nodes() {
        if !exec.visited_nodes.contains(node.id()) {
            continue;
        }
        if let Some(reply) = &process.bindings_for(node.id()).reply {
            if reply.destination.is_none() {
                if let Some(ct) = &reply.content_type {
                    return Some(ct.clone());
                }
            }
        }
    }
    None
}

/// A reply infers no codec: it is validated only against one stated on the `<q:reply>` itself.
///
/// The tempting fallback is the inbound channel's codec, since a destination-less reply rides
/// that channel back out. It is wrong, and wrong in the common case rather than an edge: a
/// channel's codec is the contract its REQUESTS satisfy, and a reply is by definition a
/// different message. A schema codec bound to validate a transfer request has no declaration
/// for the acceptance that answers it, so inferring it rejects correct replies — which is how
/// this was found, with three conformance examples refusing to answer at all.
///
/// For a bare format (json in, json out — the native-reply continuity) the inference would have
/// been a no-op anyway, because a format asserts no schema and so carries no contract to fail.
/// So inference here was never useful and sometimes harmful, and the honest surface is the
/// declaration: state `<q:reply codec>` when the answer has a contract to keep.
///
/// A `<q:send>` is the asymmetric case and keeps its inference, because it is not guessing: the
/// bytes are addressed to a named channel, and that channel's codec is exactly what its
/// consumers will decode them with.
const NO_INFERRED_REPLY_CODEC: &str = "";

/// Did the pass visit a node carrying a DESTINATION-LESS `<q:reply>` — one that answers the
/// request on its own connection?
fn visited_sync_reply(process: &ProcessDefinition, exec: &sutra_executor::ExecResult) -> bool {
    process.nodes().iter().any(|node| {
        exec.visited_nodes.contains(node.id())
            && process
                .bindings_for(node.id())
                .reply
                .as_ref()
                .is_some_and(|reply| reply.destination.is_none())
    })
}

/// Did the flow actually visit a node carrying a `<q:reply>`?
///
/// The gate on producing a synchronous reply at all. Kept separate from
/// [`visited_reply_content_type`], which answers a narrower question (the declared content type
/// of a reply with no explicit destination) and so cannot stand in for this one.
fn visited_reply_declared(process: &ProcessDefinition, exec: &sutra_executor::ExecResult) -> bool {
    process.nodes().iter().any(|node| {
        exec.visited_nodes.contains(node.id()) && process.bindings_for(node.id()).reply.is_some()
    })
}

/// The `<q:reply codec>` of the visited reply node — the codec the author declared this reply
/// must satisfy, which need not be the one that decoded the request. Resolved the same way as
/// the reply content type, off the same visited node, so the two never disagree about which
/// `<q:reply>` is speaking.
fn declared_reply_codec(
    process: &ProcessDefinition,
    exec: &sutra_executor::ExecResult,
) -> Option<String> {
    for node in process.nodes() {
        if !exec.visited_nodes.contains(node.id()) {
            continue;
        }
        if let Some(reply) = &process.bindings_for(node.id()).reply {
            if let Some(codec) = &reply.codec {
                return Some(codec.clone());
            }
        }
    }
    None
}

/// Stamp a request with its instance's audit routing ([`crate::transport_audit::AUDIT_JOURNAL_HEADER`]):
/// the process's `<q:audit>` level and sink, or metadata level into the engine's default sink
/// for a process that declares none. Only stamped while the engine has an audit sink; the
/// outbox dispatcher and the inline caller keep it (and add the instance identity) only for a
/// transport that writes audit events.
fn stamp_audit_journal(process: &ProcessDefinition, emission: &mut sutra_executor::Emission) {
    if emission.kind != sutra_executor::EmissionKind::Send {
        return;
    }
    let value = audit_journal_policy(process);
    emission.headers.insert(
        crate::transport_audit::AUDIT_JOURNAL_HEADER.to_string(),
        value,
    );
}

/// The [`crate::transport_audit::AUDIT_JOURNAL_HEADER`] value of `process`'s requests.
fn audit_journal_policy(process: &ProcessDefinition) -> String {
    match process.audit.as_ref() {
        Some(audit) => {
            crate::transport_audit::journal_policy_value(audit.capture, Some(&audit.sink))
        }
        None => crate::transport_audit::journal_policy_value(
            sutra_bpmn::qbindings::AuditCapture::Metadata,
            None,
        ),
    }
}

/// Mark every request of a `<q:audit capture="media">` process with
/// [`crate::audit::AUDIT_CAPTURE_HEADER`], so a media transport records the session it opens.
fn stamp_media_capture(process: &ProcessDefinition, emission: &mut sutra_executor::Emission) {
    use sutra_bpmn::qbindings::AuditCapture;
    if emission.kind != sutra_executor::EmissionKind::Send {
        return;
    }
    if process.audit.as_ref().map(|a| a.capture) == Some(AuditCapture::Media) {
        emission.headers.insert(
            crate::audit::AUDIT_CAPTURE_HEADER.to_string(),
            crate::audit::AUDIT_CAPTURE_MEDIA.to_string(),
        );
    }
}

#[cfg(test)]
mod transient_sensitive_tests {
    use super::*;
    use sutra_bpmn::FieldType;

    fn var(name: &str, transient: bool, sensitive: bool) -> DeclaredVariable {
        DeclaredVariable {
            name: name.to_string(),
            ty: FieldType::Any,
            schema: None,
            source: None,
            transient,
            sensitive,
            subject_key: false,
        }
    }

    #[test]
    fn drops_transient_and_marks_sensitive() {
        // @transient is never persisted; @sensitive persists but is marked for redaction.
        let declared = vec![
            var("temp", true, false),
            var("card", false, true),
            var("plain", false, false),
        ];
        let values = vec![
            ("temp".to_string(), FeelValue::from("x")),
            ("card".to_string(), FeelValue::from("4111")),
            ("plain".to_string(), FeelValue::from("ok")),
            ("undeclared".to_string(), FeelValue::from("y")),
        ];
        let pv = persisted_variables(&declared, values);
        // The transient "temp" is dropped; declared non-transient + undeclared vars survive.
        let names: Vec<&str> = pv.kept.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["card", "plain", "undeclared"]);
        // Only the sensitive var that actually survived is marked.
        assert_eq!(pv.sensitive, vec!["card".to_string()]);
        // The at-rest encrypt-set includes the @sensitive var (no redactor companions here).
        assert_eq!(pv.encrypt_names, vec!["card".to_string()]);
        assert!(pv.subjects.is_empty()); // no @subjectKey declared
    }

    #[test]
    fn common_case_untouched_when_no_flags() {
        let declared = vec![var("a", false, false)];
        // Typing is end to end: a number handed in is a number handed on, not the text "1".
        let values = vec![("a".to_string(), FeelValue::num("1"))];
        let pv = persisted_variables(&declared, values);
        assert_eq!(pv.kept, vec![("a".to_string(), FeelValue::num("1"))]);
        assert!(pv.sensitive.is_empty());
        assert!(pv.encrypt_names.is_empty());
        assert!(pv.subjects.is_empty());
    }

    #[test]
    fn transient_that_is_also_sensitive_is_dropped_not_marked() {
        // Transient wins: a var marked both is never persisted, so it cannot be in the sensitive list.
        let declared = vec![var("both", true, true)];
        let values = vec![("both".to_string(), FeelValue::from("v"))];
        let pv = persisted_variables(&declared, values);
        assert!(pv.kept.is_empty());
        assert!(pv.sensitive.is_empty());
        assert!(pv.encrypt_names.is_empty());
    }

    #[test]
    fn subject_key_variable_is_collected_with_its_raw_value() {
        // A @subjectKey var is surfaced as (name, raw value) for blind-indexing; it also persists.
        let mut cust = var("customerId", false, false);
        cust.subject_key = true;
        let declared = vec![cust, var("amount", false, false)];
        let values = vec![
            ("customerId".to_string(), FeelValue::from("cust-42")),
            ("amount".to_string(), FeelValue::num("10")),
        ];
        let pv = persisted_variables(&declared, values);
        // The blind-index input is the frozen STRING form — a typed subject key must keep
        // hashing what it always hashed, or every index row already written is orphaned.
        assert_eq!(
            pv.subjects,
            vec![("customerId".to_string(), "cust-42".to_string())]
        );
        // The subject var still persists (it is not transient).
        assert!(pv.kept.iter().any(|(n, _)| n == "customerId"));
    }
}

/// `resolved_waits_for` is the first-wins guard of an event-based gateway, so it is unit-tested
/// directly rather than only through a dispatch: if a losing arm is left out of the retirement
/// list, its wait row stays pending and fires later, sending a SECOND token through a gateway
/// that already resolved. That is instance corruption, not a glitch, and it would not show up in
/// a happy-path integration test — only in one that waits for the loser's timer.
#[cfg(test)]
mod event_gateway_race_tests {
    use super::*;
    use sutra_bpmn::BpmnModelLoader;

    /// Two message arms and a timer arm on one gateway, plus an UNRELATED catch event that must
    /// never be dragged into the retirement list.
    const RACE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f0</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:eventBasedGateway id="G"><bpmn:incoming>f0</bpmn:incoming></bpmn:eventBasedGateway>
    <bpmn:intermediateCatchEvent id="Accept">
      <bpmn:extensionElements><q:source channel="accepts"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="Reject">
      <bpmn:extensionElements><q:source channel="rejects"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="Expire">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT30M</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="Unrelated">
      <bpmn:extensionElements><q:source channel="other"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="G"/>
    <bpmn:sequenceFlow id="f1" sourceRef="G" targetRef="Accept"/>
    <bpmn:sequenceFlow id="f2" sourceRef="G" targetRef="Reject"/>
    <bpmn:sequenceFlow id="f3" sourceRef="G" targetRef="Expire"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Accept" targetRef="Unrelated"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Reject" targetRef="E"/>
    <bpmn:sequenceFlow id="f6" sourceRef="Expire" targetRef="E"/>
    <bpmn:sequenceFlow id="f7" sourceRef="Unrelated" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

    fn race_process() -> sutra_bpmn::model::ProcessDefinition {
        BpmnModelLoader::new()
            .load(RACE.as_bytes())
            .expect("the race fixture loads")
            .processes()[0]
            .clone()
    }

    #[test]
    fn the_winning_arm_retires_every_sibling() {
        let p = race_process();
        for winner in ["Accept", "Reject", "Expire"] {
            let mut got = resolved_waits_for(&p, winner);
            got.sort();
            assert_eq!(
                got,
                vec![
                    "Accept".to_string(),
                    "Expire".to_string(),
                    "Reject".to_string()
                ],
                "whichever arm wins, all three must be resolved in the SAME transaction — a \
                 losing row left pending fires later and double-advances the gateway (winner: \
                 {winner})"
            );
        }
    }

    #[test]
    fn a_catch_event_outside_the_race_retires_only_itself() {
        let p = race_process();
        assert_eq!(
            resolved_waits_for(&p, "Unrelated"),
            vec!["Unrelated".to_string()],
            "the group is the gateway's own arms; an unrelated wait must not be swept up"
        );
    }

    /// The grouping is derived from the gateway's outgoing flows, so it holds with no snapshot
    /// field, no counter and nothing to survive the park — which is why there is no state here
    /// to get out of step with the model.
    #[test]
    fn the_group_is_the_gateways_outgoing_arms() {
        let p = race_process();
        let arms: Vec<&str> = p
            .outgoing("G")
            .iter()
            .map(|f| f.target_ref.as_str())
            .collect();
        let mut arms_sorted = arms.clone();
        arms_sorted.sort();
        assert_eq!(arms_sorted, vec!["Accept", "Expire", "Reject"]);
        let mut resolved = resolved_waits_for(&p, "Accept");
        resolved.sort();
        assert_eq!(
            resolved.len(),
            arms.len(),
            "the retirement list is exactly the arm set"
        );
    }
}
