//! Channels + the intake pipeline — the engine's inbound surface
//! (the engine dispatcher, the inbound chain, and the channel registry, the
//! channel-config loader, and the HTTP trigger transport). The intake ordering, the frozen
//! `validation.*` summary names, the validator payload-shape, and the emission kinds are
//! all contract-frozen — a change here is a wire-contract change.
//!
//! The NORMATIVE pipeline, per transport delivery:
//! 1. inbox dedup (first-observer-wins — [`stores::InboxStore`] hook; sutra-persistence
//!    provides the durable impl),
//! 2. decode (channel codec; FATAL short-circuits to the reject posture),
//! 3. tier-1 structural (intrinsic to the codec's [`sutra_codec_spi::DecodeResult`]),
//! 4. tier-2 content validators (`<q:validators>` chain; a validator crash becomes a
//!    synthetic `SUTRA.RUNTIME.VALIDATOR.UNCAUGHT` ERROR issue — never a dropped message),
//! 5. the FROZEN `validation.*` summary variables,
//! 6. alias materialisation ([`stores::AliasStore`] hook; `onConflict` reject/correlate),
//! 7. `<q:onValidation mode=…>` route / reject,
//! 8. dispatch (start-event routing — multi-start `<q:source>` message-type match),
//! 9. ack per ack-mode (`on-persist` → 202 + any respond-and-continue receipt; `on-complete` → the sync reply
//!    rides the connection; broker-style deferred acks via [`ack::DeferredAckRegistry`]).
//!
//! Under the sync executor the scope is activation dispatch only: channel-delivered RELAYS
//! to waiting instances, durable outbox delivery, and broker transports belong to the
//! stateful path — `<q:send>` emissions land in an [`stores::OutboxSink`] (collected +
//! persist hook, not delivered).
//!
//! Stateful-path seams: [`sink`] (scheme-keyed [`MessageSink`] + [`SinkRegistry`] the
//! outbox dispatcher delivers through), [`source`] ([`TriggerSource`] broker-consumer
//! lifecycle, [`LeaderGate`] singleton token, [`InboundIntake`] delivery hook), and the
//! re-exported [`telemetry`] name facade.
#![forbid(unsafe_code)]

// --- pure channel MODEL (always compiled; wasm-clean — the deploy-time lint's surface) ---
/// Media-type matching, shared with the codec layer: it lives in the SPI so a schema codec can
/// negotiate its own format by content-type exactly as a channel does. Re-exported here because
/// every existing `crate::content_type::accepts` call site reads better unqualified.
pub use sutra_codec_spi::content_type;

pub mod ack;
pub mod auth;
pub mod bridge;
pub mod cloudevents;
pub mod codes;
pub mod config;
pub mod diag;
pub mod intake;
pub mod policy;
pub mod redaction;
pub mod registry;
pub mod shard_metrics;
pub mod sink;
pub mod stores;
pub mod validators;

// --- async TRANSPORT spine (feature `transport`, on by default; tokio/axum/hyper) ---
#[cfg(feature = "transport")]
pub mod audit;
#[cfg(feature = "transport")]
pub mod dispatch;
#[cfg(feature = "transport")]
pub mod external_task;
#[cfg(feature = "transport")]
pub mod http;
#[cfg(feature = "transport")]
pub mod http_sink;
#[cfg(feature = "transport")]
pub mod outbox_dispatch;
#[cfg(feature = "transport")]
pub mod source;

/// Contract-frozen telemetry names (spans / metrics / attribute keys) — defined once in
/// `sutra_executor::telemetry`, re-exported here so channel-side call sites and the OTLP
/// exporter share the same constants.
pub use sutra_executor::telemetry;
// `ChannelBinding::new` takes a `DeploymentId` in its public signature; re-export it so
// downstream crates (the vendor transports) that only depend on `sutra-channels` can name it.
pub use sutra_executor::DeploymentId;

pub use ack::{DeferredAckListener, DeferredAckRegistry};
#[cfg(feature = "transport")]
pub use audit::{
    audit_channel, spawn_audit_dispatcher, AuditEvent, AuditListener, AuditSink, AuditSinkRegistry,
    JsonlAuditSink, AUDIT_SINK_WRITE_FAILED,
};
#[cfg(feature = "transport")]
pub use bridge::replica_id;
pub use bridge::{
    AliasRecord, InstanceBridge, InstanceClaimOutcome, OutboxEmission, SuspendedInstance,
    TimerWaitRecord,
};
pub use config::{load_channel_definitions, ChannelBinding, ChannelDefinition, Namespace};
pub use diag::Diagnostic;
#[cfg(feature = "transport")]
pub use dispatch::{
    ChannelCallPoisonFire, ChannelCallPoisonOutcome, ChannelEngine, DeferredDispatch,
    DispatchOutcome, InboundMessage, ResolvedResume, ScheduledStartFire, ScheduledStartOutcome,
    SyncReply, TimerFireOutcome, ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE, SCHEDULE_CHANNEL,
};
#[cfg(feature = "transport")]
pub use external_task::{
    parse_pull_destination, CompletedTask, ExternalTask, ExternalTaskLimits, ExternalTaskNotifier,
    ExternalTaskRows, ExternalTaskService, FailedTask, FetchRequest, ParkRequest, PullDeliverySink,
    TaskLockView,
};
#[cfg(feature = "transport")]
pub use http::{
    channel_router, channel_router_dynamic, http_routes_of, http_routes_of_resolved,
    shard_index_of, spawn_engine, spawn_engine_sharded, ChannelRouteTable, EngineHandle,
    EngineShard, HttpRouteSet,
};
#[cfg(feature = "transport")]
pub use http_sink::HttpSink;
pub use intake::{InboundChain, IntakeOutcome};
#[cfg(feature = "transport")]
pub use outbox_dispatch::{
    encode_wire_message, spawn_dispatch_loop, ClaimedOutboxRow, DispatchStats, LiveDeploymentSet,
    OutboxDispatcher, OutboxDispatcherHandle, OutboxRowStore, PoisonedDelivery, RetryPolicy,
};
pub use policy::{
    ActiveInstanceCount, AllowAllFeatureProvider, Clock, ConcurrencyStore,
    DefaultTenantQuotaEnforcer, FeatureProvider, InMemoryActiveInstanceCount,
    InMemoryConcurrencyStore, PayloadCapPolicy, QuotaCheckResult, StaticTenantConfigSource,
    SystemClock, TenantConfig, TenantConfigSource, TenantQuotaEnforcer, TenantQuotas,
};
pub use registry::{
    ChannelRegistry, CodecRegistry, DrainingSink, FormatRegistry, ProcessModuleRegistry,
    RelayTarget, CODEC_BUILTIN_URN_PREFIX,
};
pub use shard_metrics::{ShardLaneMetrics, ShardRouterMetrics};
pub use sink::{
    scheme_of, BoxFuture, MessageSink, OutboundMessage, SendOutcome, SinkRegistry, PULL_SCHEME,
};
#[cfg(feature = "transport")]
pub use source::{
    AckDecision, AlwaysLeading, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate,
    TriggerSource,
};
pub use stores::{
    AliasStore, CollectingOutbox, InMemoryAliasStore, InMemoryInboxStore, InMemoryIncidentSink,
    InboundIncident, InboxStore, IncidentSink, OutboxSink,
};
pub use validators::{
    ContentValidator, DmnContentValidator, SrlContentValidator, ValidatorRegistry, ValidatorTier,
};
// The redactor SPI lives in its own thin crate (`sutra-redactor-spi`) — the per-standard
// `sutra-redactor-<std>` crates depend only on it, not on all of `sutra-channels`. Re-exported
// here so the intake call site can name the types without a direct dependency line everywhere.
pub use redaction::{mask_projection, REDACTED_MARKER};
pub use sutra_redactor_spi::{
    run_redactor, ContentRedactor, RedactionLocator, RedactionOutcome, RedactorRegistry,
};
