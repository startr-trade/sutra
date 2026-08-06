//! The Knative Eventing vendor transport — behind the SAME neutral
//! [`sutra_transport_spi::TransportChannels`] SPI every vendor transport self-registers
//! through (domain-neutrality refactor): the engine iterates `transport_factories()` and
//! never names `knative` — this crate's `inventory::submit!` is the only place that does.
//!
//! Like `sutra-transport-http`/`sutra-transport-dapr` (and UNLIKE the broker transports),
//! Knative is *inbound over the engine's own shared HTTP listener*: a Knative
//! Subscription/Trigger PUSHES events as CloudEvents-binary HTTP POSTs, so there is no
//! long-lived consumer to leader-elect — the Broker's own push IS the delivery guarantee.
//! That single asymmetry is expressed through
//! [`sutra_transport_spi::TransportChannels::inbound_router`] (this transport returns
//! `Some(router)`; the brokers take the default `None`).
//!
//! Outbound (`knative://<namespace>/<broker>`) rewrites the destination to the Broker
//! ingress URL (or `K_SINK` — see [`knative::sink`]) and delegates to
//! [`sutra_channels::HttpSink`].
//!
//! `ack-mode`: `on-persist` (the transport default) answers `202` at dispatch
//! return; `on-complete` HOLDS the push response until the instance's terminal event —
//! Knative's data-plane contract makes the subscriber's response the settle signal, so the
//! hold, not the broker-style deferred settle, is the honest realisation. See
//! [`knative::router`] for the status mapping, the bounded hold, and the operator rule
//! (`on-complete.hold-timeout` must sit below the sender's `DeliverySpec.timeout`).
//!
//! Two things are deliberately NOT per-channel here (documented at their call sites too): the
//! Broker ingress base is process-wide config (`SUTRA_SINK_KNATIVE_BROKER_INGRESS`, else
//! `K_SINK`), so a channel's `broker.url` property is parsed but never read back; and CE
//! extraction is the shared [`sutra_channels::cloudevents`]
//! `auto`/`binary`/`structured`/`wrap`/`none` machinery rather than a Knative-specific
//! parser. The one Knative-specific inbound strictness is the partial-CE-headers hard reject
//! ([`knative::codes::INBOUND_MISSING_HEADERS`]).
#![forbid(unsafe_code)]

pub mod knative;

pub use knative::{
    codes, knative_router_dynamic, knative_routes_of, parse_hold_timeout, KnativeChannelProperties,
    KnativeMessageSink, KnativeRouteSet, KnativeRouteTable, DEFAULT_BROKER_INGRESS,
    DEFAULT_HOLD_TIMEOUT, HOLD_TIMEOUT_PROPERTY, TRANSPORT,
};

use std::sync::Arc;

use axum::Router;
use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;
use sutra_channels::http::EngineHandle;
use sutra_channels::sink::SinkRegistry;
use sutra_transport_spi::{EnvRefResolver, TransportChannels, TransportFactory};

/// The Knative transport: binds `transport: knative` inbound channels to the
/// subscription-keyed route table and serves them over the engine's shared listener — the
/// Knative half of the binding flip is a route-table swap, exactly like HTTP/Dapr.
pub struct KnativeTransport {
    routes: KnativeRouteTable,
    router: Router,
}

impl KnativeTransport {
    fn build(
        definitions: &[ChannelDefinition],
        engine: EngineHandle,
    ) -> Result<KnativeTransport, Diagnostic> {
        let routes = KnativeRouteTable::new();
        routes.swap(knative_routes_of(definitions)?);
        let router = knative_router_dynamic(&routes, engine);
        Ok(KnativeTransport { routes, router })
    }
}

#[async_trait::async_trait]
impl TransportChannels for KnativeTransport {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    /// Push-based over the shared listener (like HTTP/Dapr) — no long-lived consumer.
    fn consumer_count(&self) -> usize {
        0
    }

    /// Activation flip: rebuild the subscription route set from the new active
    /// definitions and swap the table. Non-fatal + idempotent.
    async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        match knative_routes_of(active_definitions) {
            Ok(route_set) => self.routes.swap(route_set),
            Err(d) => tracing::warn!(
                code = %d.code,
                "knative route rebuild failed on activation flip: {}", d.message
            ),
        }
    }

    /// No-op: the axum serve loop is owned by the server; Knative holds no broker
    /// connection or lease to release (see [`Self::consumer_count`]).
    async fn drain(&self) {}

    /// No-op (see [`Self::drain`]).
    fn stop_all_detached(&self, _runtime: &tokio::runtime::Handle) {}

    fn inbound_router(&self) -> Option<Router> {
        Some(self.router.clone())
    }
}

/// Factory `spawn` adapter — binds the Knative channels + builds the transport (ignores
/// `pool` and the runtime `handle`: like HTTP/Dapr, Knative needs neither a datasource nor a
/// spawn runtime).
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    _pool: Option<sqlx::PgPool>,
    _resolver: EnvRefResolver,
    _handle: tokio::runtime::Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(KnativeTransport::build(definitions, engine)?))
}

/// Factory `register_sink` adapter — reads [`knative::sink::SINK_BROKER_INGRESS_ENV`] (then
/// the Knative-standard `K_SINK`, then [`DEFAULT_BROKER_INGRESS`]) itself; the Broker ingress is
/// engine-wide config, never a per-channel property (same posture as the other vendor
/// sinks' `SUTRA_SINK_<VENDOR>_*` env config).
fn register_sink(registry: &mut SinkRegistry) {
    let ingress = std::env::var(knative::sink::SINK_BROKER_INGRESS_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var(knative::sink::K_SINK_ENV)
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or_else(|| DEFAULT_BROKER_INGRESS.to_string());
    registry.register(Arc::new(KnativeMessageSink::new(ingress)));
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // Knative REALISES `on-complete` the way the HTTP transport does — by
        // holding the push response until the instance's terminal event ([`knative::router`])
        // — because the data-plane contract makes the subscriber's RESPONSE the settle
        // signal (no detached ack exists). The deferred-ack registry still supplies the
        // timing; the settle callbacks resolve the held response instead of calling a broker
        // ack. So the assembly's ON_COMPLETE_UNSUPPORTED scan never flags a knative channel.
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_knative_destinations_after_registration() {
        // The gate-defect guard: a registered knative sink MUST resolve knative://
        // destinations (an unregistered sink poisons every outbound row for this transport).
        let mut registry = SinkRegistry::new();
        register_sink(&mut registry);
        assert!(registry.resolve("knative://acme-ns/default").is_some());
        assert!(registry.resolve("https://host/cb").is_none());
        assert!(registry.resolve("dapr://bus/topic").is_none());
    }

    #[test]
    fn this_crate_self_registers_exactly_one_knative_transport_factory() {
        let factories: Vec<&'static TransportFactory> = sutra_transport_spi::transport_factories()
            .into_iter()
            .filter(|f| f.transport == TRANSPORT)
            .collect();
        assert_eq!(
            factories.len(),
            1,
            "exactly one TransportFactory named 'knative' must be registered via inventory"
        );
        assert!(
            factories[0].handles_on_complete,
            "knative REALISES on-complete (response-hold) — the assembly's \
             ON_COMPLETE_UNSUPPORTED scan must not flag a knative channel"
        );
    }
}
