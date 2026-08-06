//! The Dapr pub/sub vendor transport — behind the SAME neutral
//! [`sutra_transport_spi::TransportChannels`] SPI every vendor transport self-registers
//! through (domain-neutrality refactor): the engine iterates `transport_factories()` and
//! never names `dapr` — this crate's `inventory::submit!` is the only place that does.
//!
//! Like `sutra-transport-http` (and UNLIKE the broker transports — kafka/rabbitmq/sqs/…),
//! Dapr is *inbound over the engine's own shared HTTP listener*: the sidecar's pub/sub
//! building block PUSHES subscribed topics as CloudEvents-binary HTTP POSTs, so there is no
//! long-lived consumer to leader-elect — the sidecar's own at-least-once push IS the
//! delivery guarantee. That single asymmetry is expressed through
//! [`sutra_transport_spi::TransportChannels::inbound_router`] (this transport returns
//! `Some(router)`; the brokers take the default `None`).
//!
//! `ack-mode`: `on-persist` only. Dapr's push has no detached settle — the app
//! response IS the ack — and the bound on that response is per-COMPONENT config the engine
//! cannot see (Redis Streams `processingTimeout`/`redeliverInterval`, Service Bus
//! `handlerTimeoutInSec`, Kafka poll timeouts), so holding a response across a wait state
//! would multiply broker deliveries rather than defer one ack. The transport therefore
//! declares `handles_on_complete: false` and an `on-complete` channel degrades LOUDLY at
//! startup; the full rationale sits at the `inventory::submit!` below.
//!
//! Outbound (`dapr://<pubsub>/<topic>`) rewrites the destination to the local sidecar's
//! `http://localhost:<port>/v1.0/publish/<pubsub>/<topic>` and delegates to
//! [`sutra_channels::HttpSink`] — see [`dapr::sink`] for why this needs no second HTTP
//! client.
//!
//! Two things are deliberately NOT per-channel here (documented at their call sites too): the
//! sidecar port is process-wide config (`SUTRA_SINK_DAPR_SIDECAR_PORT`), so a channel's
//! `sidecar.port` property is validated but never read back; and CE extraction is the shared
//! [`sutra_channels::cloudevents`] `auto`/`binary`/`structured`/`wrap`/`none` machinery rather
//! than a Dapr-specific parser.
#![forbid(unsafe_code)]

pub mod dapr;

pub use dapr::{
    codes, dapr_router_dynamic, dapr_routes_of, DaprChannelProperties, DaprMessageSink,
    DaprRouteSet, DaprRouteTable, TRANSPORT,
};

use std::sync::Arc;

use axum::Router;
use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;
use sutra_channels::http::EngineHandle;
use sutra_channels::sink::SinkRegistry;
use sutra_transport_spi::{EnvRefResolver, TransportChannels, TransportFactory};

/// The Dapr transport: binds `transport: dapr` inbound channels to the topic-keyed route
/// table and serves them over the engine's shared listener — the Dapr half of the
/// binding flip is a route-table swap, exactly like the HTTP transport.
pub struct DaprTransport {
    routes: DaprRouteTable,
    router: Router,
}

impl DaprTransport {
    fn build(
        definitions: &[ChannelDefinition],
        engine: EngineHandle,
    ) -> Result<DaprTransport, Diagnostic> {
        let routes = DaprRouteTable::new();
        routes.swap(dapr_routes_of(definitions)?);
        let router = dapr_router_dynamic(&routes, engine);
        Ok(DaprTransport { routes, router })
    }
}

#[async_trait::async_trait]
impl TransportChannels for DaprTransport {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    /// Push-based over the shared listener (like HTTP) — no long-lived consumer.
    fn consumer_count(&self) -> usize {
        0
    }

    /// Activation flip: rebuild the topic route set from the new active definitions and
    /// swap the table. Non-fatal + idempotent — a rebuild error WARNs and keeps the current
    /// routes (the engine keeps running), mirroring the HTTP transport's `rewire`.
    async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        match dapr_routes_of(active_definitions) {
            Ok(route_set) => self.routes.swap(route_set),
            Err(d) => tracing::warn!(
                code = %d.code,
                "dapr route rebuild failed on activation flip: {}", d.message
            ),
        }
    }

    /// No-op: the axum serve loop is owned by the server; Dapr holds no broker connection or
    /// lease to release (see [`Self::consumer_count`]).
    async fn drain(&self) {}

    /// No-op (see [`Self::drain`]).
    fn stop_all_detached(&self, _runtime: &tokio::runtime::Handle) {}

    fn inbound_router(&self) -> Option<Router> {
        Some(self.router.clone())
    }
}

/// Factory `spawn` adapter — binds the Dapr channels + builds the transport (ignores `pool`
/// and the runtime `handle`: like HTTP, Dapr needs neither a datasource nor a spawn runtime —
/// it is push-based over the shared listener, not a polled/leader-elected consumer).
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    _pool: Option<sqlx::PgPool>,
    _resolver: EnvRefResolver,
    _handle: tokio::runtime::Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(DaprTransport::build(definitions, engine)?))
}

/// Factory `register_sink` adapter — reads [`dapr::sink::SINK_SIDECAR_PORT_ENV`] itself (a
/// Dapr sidecar is one-per-process, so the port is engine-wide config, never a per-channel
/// property — same posture as the Kafka/SQS/GCP sinks' `SUTRA_SINK_<VENDOR>_*` env config).
fn register_sink(registry: &mut SinkRegistry) {
    let port = std::env::var(dapr::sink::SINK_SIDECAR_PORT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u16>().ok())
        .unwrap_or(DaprChannelProperties::DEFAULT_SIDECAR_PORT);
    registry.register(Arc::new(DaprMessageSink::new(port)));
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // `ack-mode: on-complete` is DELIBERATELY GATED (decision, not a gap). Dapr pub/sub
        // has no detached
        // settle: the app's HTTP response IS the ack, so the only mechanism available is the
        // response-hold the HTTP/Knative transports use (the deferred-ack registry's
        // register-and-return would ack the sidecar BEFORE the instance terminates — a lie).
        // A hold cannot be made honest here, because the bound on a Dapr app-callback is
        // owned by the COMPONENT, not by the subscription, and is invisible to both the
        // engine and the channel author:
        //   * Redis Streams (the default local component) redelivers a message whose handler
        //     has been pending longer than `processingTimeout` (default 15s), scanning every
        //     `redeliverInterval` (default 60s) — the redelivery lands on ANOTHER consumer
        //     WHILE this response is still held, i.e. concurrent duplicate delivery, not a
        //     post-failure retry;
        //   * Azure Service Bus aborts the app handler at `handlerTimeoutInSec` (default 60s)
        //     and the lock/maxDeliveryCount machinery redelivers;
        //   * Kafka-backed components block the consumer for the hold, risking a
        //     max-poll/session-timeout rebalance that stalls the whole subscription.
        // Every one of those is per-component config in the operator's Dapr Component YAML —
        // nothing the engine can read and size a hold against (contrast Knative, whose
        // `DeliverySpec.timeout` is an explicit, per-subscription, operator-authored bound on
        // the very resource that pushes to us). A hold that outlives the component's timer
        // does not degrade quietly: it multiplies deliveries at the broker. Sidecar resend
        // timers make long holds a redelivery storm — so an `ack-mode: on-complete` inbound
        // definition on this transport keeps the loud SUTRA.ACK.ON_COMPLETE_UNSUPPORTED
        // startup diagnostic and runs on-persist. NOTE that on-persist here already answers
        // AFTER the dispatch runs to its quiescent point (Dapr's push is synchronous over the
        // shared listener); what stays unsupported is holding across a WAIT STATE.
        handles_on_complete: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_dapr_destinations_after_registration() {
        // The gate-defect guard: a registered dapr sink MUST resolve dapr:// destinations
        // (an unregistered sink poisons every outbound row for this transport).
        let mut registry = SinkRegistry::new();
        register_sink(&mut registry);
        assert!(registry
            .resolve("dapr://messagebus/orders.created")
            .is_some());
        assert!(registry.resolve("https://host/cb").is_none());
        assert!(registry.resolve("kafka://topic").is_none());
    }

    #[test]
    fn this_crate_self_registers_exactly_one_dapr_transport_factory() {
        let factories: Vec<&'static TransportFactory> = sutra_transport_spi::transport_factories()
            .into_iter()
            .filter(|f| f.transport == TRANSPORT)
            .collect();
        assert_eq!(
            factories.len(),
            1,
            "exactly one TransportFactory named 'dapr' must be registered via inventory"
        );
        // The gate is a DECISION (see the rationale at the factory): Dapr's
        // component-owned redelivery timers make a response-hold a duplicate-delivery
        // multiplier, so `on-complete` stays loudly unsupported rather than silently
        // mis-promised. Pin it so a future flip has to argue with this test.
        assert!(
            !factories[0].handles_on_complete,
            "dapr stays gated for ack-mode: on-complete — flipping this needs a contract-level \
             answer for the component-owned redelivery timers (processingTimeout / \
             handlerTimeoutInSec / max-poll), not just a hold implementation"
        );
    }
}
