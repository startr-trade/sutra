//! The HTTP channel transport — the engine's native, always-present inbound transport, now
//! behind the SAME neutral [`sutra_transport_spi::TransportChannels`] SPI as the vendor
//! brokers (domain-neutrality refactor). Channel bind + activate is thereby protocol-neutral:
//! the engine assembly iterates `transport_factories()` and never branches on `transport ==
//! "http"`; HTTP is just the transport whose `transport:` discriminator is `"http"`.
//!
//! It differs from a broker transport only in DIRECTION — HTTP is *inbound over the engine's
//! own listener* (shared with `/sutra/health/*`), where brokers dial *out* to their own
//! broker. That single asymmetry is expressed through the neutral
//! [`sutra_transport_spi::TransportChannels::inbound_router`] capability (this transport
//! returns `Some(router)`; brokers take the default `None`) — the server merges the returned
//! routes under its health API. `rewire` (the activation flip) is a route-table swap;
//! `drain`/`stop_all_detached` are no-ops (the axum serve loop is owned by the server).
//!
//! Unlike the vendor transports HTTP is the universal baseline protocol — no vendor lock-in —
//! so it stays neutral and is always force-linked into the binary (never an optional bundle).
#![forbid(unsafe_code)]

use std::sync::Arc;

use axum::Router;
use sutra_channels::http::{channel_router_dynamic, EngineHandle};
use sutra_channels::{
    http_routes_of_resolved, ChannelDefinition, ChannelRouteTable, Diagnostic, HttpSink,
    SinkRegistry,
};
use sutra_transport_spi::{EnvRefResolver, TransportChannels, TransportFactory};
use tracing::warn;

/// The `transport:` discriminator this transport binds (the HTTP channel default scheme).
pub const TRANSPORT: &str = "http";

/// The HTTP channel transport: it binds every `transport: http` channel to an axum route set
/// and serves them from a swappable [`ChannelRouteTable`] the mounted [`Router`] reads
/// dynamically. On an activation flip [`HttpTransport::rewire`] rebuilds the route set from
/// the new active definitions and swaps the table (the request path resolves the new routes
/// atomically) — the exact HTTP half of the binding flip, now inside the SPI.
pub struct HttpTransport {
    /// Shared with the mounted router (Arc-backed); `swap` on a flip is what the router sees.
    routes: ChannelRouteTable,
    /// The injected `env:`/`secret:`/`vault:` resolver — inbound-auth values (`apikey.value`
    /// / `bearer.token`) may be secret refs; a literal passes through unchanged.
    resolver: EnvRefResolver,
    /// The channel router, built ONCE over `routes`; handed to the server via
    /// [`TransportChannels::inbound_router`] and mounted under the health API.
    router: Router,
}

impl HttpTransport {
    /// Bind the `transport: http` channels of `definitions` (fail-closed on an authored auth
    /// error, exactly like a broker's boot) and build the router over the route table.
    fn build(
        definitions: &[ChannelDefinition],
        engine: EngineHandle,
        resolver: EnvRefResolver,
    ) -> Result<HttpTransport, Diagnostic> {
        let routes = ChannelRouteTable::new();
        routes.swap(http_routes_of_resolved(
            definitions,
            &auth_resolver(resolver),
        )?);
        let router = channel_router_dynamic(&routes, engine);
        Ok(HttpTransport {
            routes,
            resolver,
            router,
        })
    }
}

/// Adapt the neutral [`EnvRefResolver`] (`Result<_, String>`) into the `Result<_, Diagnostic>`
/// shape `http_routes_of_resolved` wants, tagging an unresolvable ref as the frozen
/// `CHANNEL_AUTH_SCHEME_INVALID` diagnostic (the value the engine's inline resolver used).
fn auth_resolver(resolver: EnvRefResolver) -> impl Fn(&str) -> Result<String, Diagnostic> {
    move |value: &str| {
        resolver(value).map_err(|e| {
            Diagnostic::error(
                sutra_channels::codes::CHANNEL_AUTH_SCHEME_INVALID,
                format!("HTTP channel auth value could not be resolved: {e}"),
            )
        })
    }
}

#[async_trait::async_trait]
impl TransportChannels for HttpTransport {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    /// HTTP has no long-lived consumers (request-driven), so the consumer count is 0 — the
    /// engine's boot log line is broker-oriented; HTTP's routes ride the mounted router.
    fn consumer_count(&self) -> usize {
        0
    }

    /// Activation flip (HTTP half): rebuild the route set from the new active
    /// definitions and swap the table. Non-fatal + idempotent, mirroring the broker rewire —
    /// a rebuild error WARNs and keeps the current routes (the engine keeps running).
    async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        match http_routes_of_resolved(active_definitions, &auth_resolver(self.resolver)) {
            Ok(route_set) => self.routes.swap(route_set),
            Err(d) => warn!(
                code = %d.code,
                "HTTP route rebuild failed on activation flip: {}", d.message
            ),
        }
    }

    /// No-op: the axum serve loop is owned by the server (aborted on shutdown), and HTTP
    /// holds no broker connection or lease to release.
    async fn drain(&self) {}

    /// No-op (see [`Self::drain`]).
    fn stop_all_detached(&self, _runtime: &tokio::runtime::Handle) {}

    fn inbound_router(&self) -> Option<Router> {
        Some(self.router.clone())
    }
}

/// Factory `spawn` adapter — binds the HTTP channels + builds the transport (ignores `pool`
/// and the runtime `handle`: HTTP needs neither a datasource nor a spawn runtime).
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    _pool: Option<sqlx::PgPool>,
    resolver: EnvRefResolver,
    _handle: tokio::runtime::Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(HttpTransport::build(
        definitions,
        engine,
        resolver,
    )?))
}

/// Factory `register_sink` adapter — the outbound HTTP(S) sink (no engine-wide env config;
/// the destination host rides the `http(s)://` URI).
fn register_sink(registry: &mut SinkRegistry) {
    registry.register(Arc::new(HttpSink::new()));
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // HTTP REALISES `on-complete` natively — connection-hold, the sync reply
        // IS the ack (its per-transport DEFAULT, no deferred-ack registry involved) — so
        // the neutral ON_COMPLETE_UNSUPPORTED assembly scan never flags an HTTP channel.
        handles_on_complete: true,
    }
}
