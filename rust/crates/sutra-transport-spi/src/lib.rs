//! The transport SPI + shared cluster substrate — the neutral seam vendor transports
//! self-register through, so `sutra-engine` composes broker consumers GENERICALLY and
//! names no vendor (domain-neutrality refactor).
//!
//! It hosts three moved-down pieces the per-vendor `sutra-transport-<vendor>` crates need
//! but that must not live in `sutra-engine` (which depends on the transport crates to
//! bundle them — the reverse edge would be a cycle):
//! - [`leadership`] — the DB-lease leader election ([`leadership::DbLeaderElection`],
//!   [`leadership::PgLeaseHandle`], [`leadership::ChannelLeadership`],
//!   [`leadership::channel_role`]). Shared: the timer poller leads through it too.
//! - [`EngineIntake`] — the `EngineHandle` → `InboundIntake` adapter the sources deliver
//!   through.
//! - the [`TransportChannels`] lifecycle trait + the inventory-collected
//!   [`TransportFactory`] each vendor crate `inventory::submit!`s, iterated by the engine
//!   assembly via [`transport_factories`].
#![forbid(unsafe_code)]

use std::sync::Arc;

use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, SinkRegistry};
use tokio::runtime::Handle;

pub mod intake;
pub mod leadership;

pub use intake::EngineIntake;

/// A resolver for `env:` / `secret:` / `vault:` references + `${…}` placeholders — the
/// engine's `envref` registry, passed into a transport's [`TransportFactory::spawn`] as a
/// plain `fn` pointer so the transport crate resolves inbound-auth + broker credentials
/// without depending on `sutra-engine` (where the registry lives).
pub type EnvRefResolver = fn(&str) -> Result<String, String>;

/// The uniform inbound-consumer spawner signature every vendor's [`TransportFactory`]
/// exposes: `(defs, engine, pool, envref-resolver, runtime) -> the live manager`.
pub type TransportSpawn = fn(
    &[ChannelDefinition],
    EngineHandle,
    Option<PgPool>,
    EnvRefResolver,
    Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic>;

/// One vendor transport's live consumer set + lifecycle. The engine holds each as an
/// `Arc<dyn TransportChannels>` and drives them GENERICALLY: `rewire` on an activation
/// flip, `drain` on graceful shutdown, `stop_all_detached` on the sync stop path.
#[async_trait::async_trait]
pub trait TransportChannels: Send + Sync {
    /// The `transport:` discriminator this manager owns (e.g. `kafka`, `aws-sqs`).
    fn transport(&self) -> &str;
    /// Number of consumers currently wired.
    fn consumer_count(&self) -> usize;
    /// Reconcile the running consumers to the new ACTIVE definition set (the activation flip):
    /// stop changed/removed, start added, keep unchanged. Idempotent + non-fatal.
    async fn rewire(&self, active: &[ChannelDefinition]);
    /// Await-drain every consumer + release the channel-lease election (graceful shutdown).
    async fn drain(&self);
    /// Detached stop of every consumer + lease release (the sync stop path).
    fn stop_all_detached(&self, runtime: &Handle);
    /// The inbound routes this transport serves over the process's shared HTTP listener, if
    /// any. This is a protocol-NEUTRAL capability, not an HTTP special case: a transport that
    /// receives *inbound over the engine's own listener* (HTTP today; a future ws/grpc-on-the-
    /// same-port transport tomorrow) contributes its axum [`Router`], which the server merges
    /// under its `/sutra/health/*` API. Transports that dial *out* to their own broker
    /// (kafka, rabbitmq, …) own their connection and take the default `None`.
    fn inbound_router(&self) -> Option<axum::Router> {
        None
    }
}

/// A vendor transport's self-registration: its `transport:` discriminator + the inbound
/// consumer spawner + the outbound sink registrar. Each `sutra-transport-<vendor>` crate
/// `inventory::submit!`s exactly one; [`transport_factories`] collects them.
pub struct TransportFactory {
    /// The `transport:` value this factory wires (channel definitions are filtered by it).
    pub transport: &'static str,
    /// Construct + start the inbound consumers for this transport's definitions. Signature
    /// is UNIFORM across vendors: `(defs, engine, pool, envref-resolver, runtime)`.
    pub spawn: TransportSpawn,
    /// Register this transport's outbound sink into the dispatcher's [`SinkRegistry`]
    /// (reads its own engine-wide `SUTRA_SINK_<VENDOR>_*` env config internally).
    pub register_sink: fn(&mut SinkRegistry),
    /// Whether this transport REALISES `ack-mode: on-complete`: a broker source
    /// by deferring its settle through the engine's deferred-ack registry
    /// (`InboundIntake::deliver_deferred` — rabbitmq), an on-listener transport by
    /// holding the connection to completion (http). `false` ⇒ the engine assembly emits
    /// the `SUTRA.ACK.ON_COMPLETE_UNSUPPORTED` startup diagnostic for any inbound
    /// definition declaring `on-complete` on this transport (loud degrade to
    /// `on-persist`, never silent) — the assembly stays vendor-neutral by reading THIS
    /// flag instead of naming transports.
    pub handles_on_complete: bool,
}

inventory::collect!(TransportFactory);

/// Every registered [`TransportFactory`], sorted by `transport` for deterministic wiring.
pub fn transport_factories() -> Vec<&'static TransportFactory> {
    let mut factories: Vec<&'static TransportFactory> =
        inventory::iter::<TransportFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}

/// A transport whose outbound sink ANSWERS by re-entering the engine — a request/reply
/// exchange whose reply is delivered back as an inbound message, the way the in-process
/// `local` sink does. The bare [`TransportFactory::register_sink`] fn-ptr has no engine
/// access, so such a transport additionally `inventory::submit!`s one of these; the engine
/// assembly registers every collected sink with the engine handle, naming none.
///
/// The `transport` value doubles as the sink's URI scheme: a bare `<transport>://<channel>`
/// outbound bind is qualified to `<transport>://<module_key>/<channel>` at load, exactly
/// as `local://` is, so the sink can address the reply to the owning deployment.
pub struct ReplySinkFactory {
    /// The `transport:` value (and URI scheme) this sink serves.
    pub transport: &'static str,
    /// Build the sink around the engine handle it re-enters through.
    pub build: fn(EngineHandle) -> Arc<dyn sutra_channels::MessageSink>,
}

inventory::collect!(ReplySinkFactory);

/// Every registered [`ReplySinkFactory`], sorted by `transport` for deterministic wiring.
pub fn reply_sink_factories() -> Vec<&'static ReplySinkFactory> {
    let mut factories: Vec<&'static ReplySinkFactory> =
        inventory::iter::<ReplySinkFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}

/// A transport that can answer an outbound call SYNCHRONOUSLY (an outbound channel declaring
/// `ack-mode: on-complete`): the response is merged into the flow without a durable park. The
/// engine assembly registers every collected caller into the dispatcher's
/// [`sutra_channels::InlineCallerRegistry`], naming none; a sync outbound channel whose scheme
/// has no caller is a load-time error.
pub struct InlineCallerFactory {
    /// The `transport:` value this caller serves.
    pub transport: &'static str,
    pub build: fn() -> Arc<dyn sutra_channels::InlineCaller>,
}

inventory::collect!(InlineCallerFactory);

/// Every registered [`InlineCallerFactory`], sorted by `transport`.
pub fn inline_caller_factories() -> Vec<&'static InlineCallerFactory> {
    let mut factories: Vec<&'static InlineCallerFactory> =
        inventory::iter::<InlineCallerFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}
