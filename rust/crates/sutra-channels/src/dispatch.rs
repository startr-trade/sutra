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
use crate::codes;
use crate::config::{ChannelBinding, ChannelDefinition, Namespace};
use crate::diag::Diagnostic;
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
                prior_deployments: Vec::new(),
                deferred_acks: None,
                pending_settle: std::cell::RefCell::new(None),
                intake_counter: std::cell::RefCell::new(0),
                shard: crate::http::EngineShard::single(),
                shard_metrics: std::sync::Arc::new(
                    crate::shard_metrics::ShardLaneMetrics::default(),
                ),
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
        self.dispatch_inner(message).await
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
        let result = self.dispatch_inner(message).await;
        let leftover = self.pending_settle.borrow_mut().take();
        let outcome = result?;
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
                if !first {
                    return Ok(DispatchOutcome::Duplicate);
                }
            }
        }

        // ---- decode ONCE, up front (channel-scoped codec) --------------------------------
        let decoded =
            self.chain
                .decode(&binding, &message.body, message.content_type.as_deref())?;
        let message_type = decoded.as_ref().and_then(|d| d.message_type.clone());

        // ---- start-event routing (multi-start <q:source> match) --------------------------
        let handlers = {
            // `sutra.resolve` (handler resolution).
            let _resolve = tracing::info_span!(
                telemetry::SPAN_RESOLVE,
                channel = %message.channel,
                deployment.id = %deployment.value(),
            )
            .entered();
            self.processes
                .resolve_handlers(&deployment, &message.channel, message_type.as_deref())
        };
        if handlers.is_empty() {
            // Before failing "no start event", check whether this is a
            // channel-delivered RELAY: a wait node (userTask / message catch) subscribes to
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
            .with_attribute("messageType", message_type.as_deref().unwrap_or("")));
        }
        if !binding.broadcast && handlers.len() > 1 {
            let processes: Vec<&str> = handlers.iter().map(|h| h.process.id.as_str()).collect();
            return Err(Diagnostic::error(
                codes::INBOUND_AMBIGUOUS_HANDLER,
                format!(
                    "Inbound on non-broadcast channel '{}' (message type '{}') resolved to {} \
                     processes {processes:?}; a non-broadcast channel must resolve to exactly \
                     one.",
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
                if !inbox
                    .record_seen(deployment, &message.channel, &body_key)
                    .await
                {
                    return Ok(DispatchOutcome::Duplicate);
                }
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
                self.commit_sync_emissions(binding, deployment, message)
                    .await?;
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
    ) -> Result<(), Diagnostic> {
        let drained = self.emissions.drain();
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
                self.commit_sync_emissions(binding, deployment, message)
                    .await?;
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
            } => {
                // Respond-and-continue: when the pass parked at a `<q:reply continue>`
                // service task, flush the produced reply to the caller NOW. Built BEFORE the park
                // commit so "reply returned ⇒ park committed" holds — a failed `commit_park`
                // propagates via `?` and the reply is never returned. A due-now timer wait (armed by
                // the executor on the continue-reply node) self-resumes the remaining nodes at once.
                let reply = if detached_reply {
                    let exec = ExecResult {
                        instance_id: instance_id.clone(),
                        outputs: variables.clone(),
                        visited_nodes: completed_nodes.iter().cloned().collect(),
                    };
                    self.build_reply(binding, process, message, &exec)?
                } else {
                    None
                };
                // Quiescent-point rule: the destination-bearing emissions this
                // step collected (channel-call requests, `<q:send>`s before the park)
                // ride the park step and commit ATOMICALLY with the snapshot — or not at
                // all (the strict transactional outbox).
                let emissions = to_outbox_emissions(
                    self.emissions.drain(),
                    binding.labels(),
                    inbound_traceparent(message),
                );
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
                // The channel-call park key: the task's declared <q:alias>,
                // evaluated over the suspended context, recorded in the SAME step.
                records.extend(channel_call_alias_rows(
                    process,
                    &waiting_nodes,
                    &[],
                    &variables,
                )?);
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
                if let Some(store) = &self.concurrency {
                    store
                        .record_started(deployment, &instance_id, &message.channel)
                        .await;
                    store.record_suspended(deployment, &instance_id).await;
                }
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
        let targets = self
            .processes
            .resolve_relay_targets(deployment, &message.channel);
        let Some(target) = targets.first() else {
            return Ok(RelayResolution::NoTargets);
        };
        let process = &target.process;

        // Two-tier intake validation of the relayed payload against the wait node's
        // <q:source> (uniform with a start event). FATAL ⇒ reject; nothing advances.
        let mut relay_vars = project_variables(message);
        if let Some(decoded) = decoded {
            match self.chain.apply_decoded(
                process,
                &target.wait_node_id,
                binding,
                decoded,
                &mut relay_vars,
            )? {
                IntakeOutcome::Proceed { .. } => {}
                IntakeOutcome::Reject(d) => return Err(d),
                IntakeOutcome::Error { diagnostic, .. } => return Err(diagnostic),
            }
        }

        // The correlation the relay carries — the WAIT NODE's own declared <q:alias>
        // first (the channel-call park key), then the process-level
        // start-event `onConflict=correlate` alias (the approval-hold pattern).
        let Some((alias_name, alias_value)) =
            correlation_for_wait_node(process, &target.wait_node_id, &relay_vars)
                .or_else(|| correlation_for(process, &relay_vars))
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
            return Ok(RelayResolution::NotCorrelated(Diagnostic::error(
                codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND,
                format!(
                    "No live instance carries alias {alias_name}={alias_value} for \
                     deployment {}; the relay cannot be correlated to a waiting instance.",
                    deployment.value()
                ),
            )));
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
            })));
        }
        let outcome = self
            .resume_correlated_claimed(
                bridge,
                deployment,
                &instance_id,
                &target.wait_node_id,
                relay_vars,
                &alias_value,
                &message.channel,
                binding.labels(),
                inbound_traceparent(message),
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
        alias_value: &str,
        channel: &str,
        labels: BTreeMap<String, String>,
        traceparent: Option<String>,
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
                ));
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

            let outcome = self
                .commit_resume_outcome(
                    bridge,
                    labels,
                    traceparent,
                    &resume_process,
                    &snapshot,
                    &pinned,
                    wait_node_id,
                    result,
                )
                .await?;
            // The step committed under `pinned`; when that is the key we claimed under (the
            // ordinary case — a DRAINING-scope resume is the exception), the release already rode
            // that transaction and there is nothing left to hand back.
            if pinned == *deployment {
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
        self.resume_correlated_claimed(
            &bridge,
            &resolved.deployment,
            &resolved.instance_id,
            &resolved.wait_node_id,
            resolved.variables.clone(),
            &resolved.alias_value,
            &resolved.channel,
            resolved.labels.clone(),
            resolved.traceparent.clone(),
        )
        .await
    }

    /// Persist a resume pass's quiescent point (shared by the relay path and the timer
    /// fire): COMPLETED ⇒ terminal step (delete + resolve every wait + retire aliases +
    /// enqueue the step's emissions); SUSPENDED ⇒ re-park step (snapshot + resolved waits
    /// incl. cancelled timer rows + fresh wait/timer/alias rows + emissions) — each ONE
    /// transaction.
    #[allow(clippy::too_many_arguments)]
    async fn commit_resume_outcome(
        &self,
        bridge: &Rc<dyn InstanceBridge>,
        labels: BTreeMap<String, String>,
        traceparent: Option<String>,
        process: &ProcessDefinition,
        prior_snapshot: &SuspendedInstance,
        pinned: &DeploymentId,
        satisfied_node: &str,
        result: StatefulExecResult,
    ) -> Result<DispatchOutcome, Diagnostic> {
        match result {
            StatefulExecResult::Completed { instance_id, .. } => {
                let emissions = to_outbox_emissions(self.emissions.drain(), labels, traceparent);
                // Terminal step: no longer in-flight — drop the row, resolve every wait
                // point, retire the aliases, enqueue the step's emissions (a completed
                // instance never lingers; its sends never miss the commit).
                bridge
                    .commit_complete(pinned, &instance_id, &emissions)
                    .await?;
                // §6.1: a resume pass committed (terminal) on this lane.
                self.shard_metrics.resumed();
                // The instance is terminal — free its channel_instance slot.
                if let Some(store) = &self.concurrency {
                    store.record_terminal(pinned, &instance_id).await;
                }
                // A relay has no synchronous reply body of its own — empty outputs, never
                // the resumed instance's raw variable map (the drive-relay contract).
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: serde_json::json!({}),
                    reply: None,
                })
            }
            StatefulExecResult::Suspended {
                instance_id,
                waiting_nodes,
                completed_nodes,
                variables,
                start_node,
                timer_waits,
                // A continue-reply reached during a RESUMED tail has no sync caller to flush to; it
                // still parks + self-resumes (the executor armed the due-now timer), so the tail
                // runs to completion — only the (callerless) reply body is not sent. v1 limitation.
                detached_reply: _,
                coverage_progress,
                retry_attempts,
                retry_backoff,
            } => {
                // The re-park step carries the emissions collected up to this
                // quiescent point.
                let emissions = to_outbox_emissions(self.emissions.drain(), labels, traceparent);
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
                // Fresh channel-call parks reached by THIS pass record their alias rows;
                // nodes still waiting from the prior pass were indexed by the original
                // park step and are skipped.
                let aliases = channel_call_alias_rows(
                    process,
                    &waiting_nodes,
                    &prior_snapshot.waiting_nodes,
                    &variables,
                )?;
                bridge
                    .commit_repark(
                        pinned,
                        &instance_id,
                        &new_snapshot,
                        &resolved_waits_for(process, satisfied_node),
                        &aliases,
                        &timer_records(&timer_waits),
                        &emissions,
                    )
                    .await?;
                // §6.1: a resume pass committed (re-park) on this lane.
                self.shard_metrics.resumed();
                Ok(DispatchOutcome::Completed {
                    instance_id,
                    outputs: serde_json::json!({}),
                    reply: None,
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
                    node_id = %fire.node_id,
                    "scheduled start dead-lettered: the non-idempotent process failed"
                );
                return Err(Diagnostic::error(&cause_code, detail));
            }
            // Unreachable: a schedule MINTS an instance (the spawn path) — no relay
            // correlation runs, so a shard handoff cannot be produced.
            DispatchOutcome::Handoff { .. } => {
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
            //   and is absent from `completed_nodes` (a retry park never records the task as
            //   done — that omission IS the re-drive mechanism). Rides the relay resume path
            //   (`resume_timer` would reject a non-timer node); the inference is sound there
            //   because no relay can ever target a registered task.
            // - a `<q:reply continue>` marker: the respond-and-continue self-resume, node in
            //   `completed_nodes`, replay skips it and runs the tail.
            let call_node = matches!(
                process.node(&fire.node_id),
                Ok(Node::ServiceTask { implementation, .. })
                    if implementation.starts_with(CHANNEL_CALL_PREFIX)
            );
            let retry_redrive = !snapshot.completed_nodes.iter().any(|n| n == &fire.node_id)
                && if call_node {
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
            self.commit_resume_outcome(
                &bridge,
                BTreeMap::new(),
                None,
                &process,
                &snapshot,
                &pinned,
                &fire.node_id,
                result,
            )
            .await?;
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
            self.commit_resume_outcome(
                &bridge,
                BTreeMap::new(),
                None,
                &process,
                &snapshot,
                &pinned,
                &fire.node_id,
                result,
            )
            .await?;
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
            return Ok(Some(SyncReply {
                body: body.clone().into_bytes().into(),
                content_type,
            }));
        }
        if let Some(reply_object) = exec.output("responseObject") {
            if !binding.codec.is_empty() {
                let value = CodecValue::Json(feel_to_json(reply_object));
                let bytes = self
                    .chain
                    .encode(binding, &value, message.content_type.as_deref())?;
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
}

impl ChannelEngineBuilder {
    /// Register the bindings (and per-channel payload caps) of every loaded channel
    /// definition. A duplicate channel URN with a differing binding panics — channel
    /// uniqueness is a load/lint-time invariant (`ChannelUniquenessValidator`), so a
    /// collision here is a genuine boot-time fail-closed condition.
    pub fn with_channel_definitions(mut self, definitions: &[ChannelDefinition]) -> Self {
        for def in definitions {
            if let Some(cap) = def.payload_cap_bytes {
                self.engine
                    .payload_cap_policy
                    .set_channel_override(&def.binding.channel_name, cap);
            }
            self.engine
                .channels
                .register(def.binding.clone())
                .expect("channel registration must not collide (validated at load/lint time)");
        }
        self
    }

    /// Register a bare binding (tests / synthetic dispatch).
    pub fn with_binding(mut self, binding: ChannelBinding) -> Self {
        self.engine
            .channels
            .register(binding)
            .expect("channel registration must not collide (validated at load/lint time)");
        self
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

    pub fn build(self) -> ChannelEngine {
        self.engine
    }
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

/// The wait rows a resume step resolves: the satisfied node itself, the HOST's row when
/// the satisfied node is a fired timer boundary, and every (other) timer boundary armed on
/// that host — a satisfied host consumes its pending timers (the response cancels the
/// timeout; the timeout cancels the wait).
fn resolved_waits_for(process: &ProcessDefinition, satisfied: &str) -> Vec<String> {
    let mut out = vec![satisfied.to_string()];
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

/// Evaluate the declared `<q:alias>` rows of every FRESHLY-parked
/// channel-call task in `waiting_nodes` (nodes still waiting from the prior pass were
/// indexed by their original park step and are skipped). A channel-call park with ZERO
/// resolvable alias rows is refused — it could never be resumed.
fn channel_call_alias_rows(
    process: &ProcessDefinition,
    waiting_nodes: &[String],
    prior_waiting: &[String],
    variables: &Variables,
) -> Result<Vec<AliasRecord>, Diagnostic> {
    let mut out = Vec::new();
    let ctx = variables.to_feel_context();
    for node_id in waiting_nodes {
        if prior_waiting.iter().any(|n| n == node_id) {
            continue;
        }
        let Ok(Node::ServiceTask { implementation, .. }) = process.node(node_id) else {
            continue;
        };
        if !implementation.starts_with(CHANNEL_CALL_PREFIX) {
            continue;
        }
        let aliases = &process.bindings_for(node_id).aliases;
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
            match &value {
                FeelValue::Null => continue,
                FeelValue::List(items) if alias.multi => {
                    for item in items {
                        if item.is_null() {
                            continue;
                        }
                        out.push(AliasRecord {
                            name: alias.name.clone(),
                            value: sutra_feel::value::canonical_string_of(item),
                            unique: alias.unique,
                        });
                        resolved_any = true;
                    }
                }
                other => {
                    out.push(AliasRecord {
                        name: alias.name.clone(),
                        value: sutra_feel::value::canonical_string_of(other),
                        unique: alias.unique,
                    });
                    resolved_any = true;
                }
            }
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
fn correlation_for(process: &ProcessDefinition, ctx: &Variables) -> Option<(String, String)> {
    let feel_ctx = ctx.to_feel_context();
    for start in process.start_events() {
        for alias in &process.bindings_for(start.id()).aliases {
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
