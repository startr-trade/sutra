//! The Dapr inbound route table + axum router — the topic-keyed analogue of
//! `sutra_channels::http`'s `(METHOD, path)` route table. Dapr's sidecar delivers every
//! subscribed topic to ONE catch-all route (`POST /dapr/{topic}`); the topic path segment
//! resolves the served channel.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;

use sutra_channels::cloudevents::{self, CeMode, WrapDefaults};
use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;
use sutra_channels::http::sha256_truncated;
use sutra_channels::{codes as shared_codes, DispatchOutcome, EngineHandle, InboundMessage};

use super::{codes, DaprChannelProperties, TRANSPORT};

/// The Dapr-specific header naming the topic — validated against the URL path segment by the
/// topic-mismatch guard in [`handle_post`]. There is deliberately no `dapr-pubsubname`
/// counterpart: the `x-dapr-pubsubname` header lifted below comes from the CHANNEL's
/// configured `pubsub.name` property, never from an incoming header.
const HEADER_TOPIC: &str = "dapr-topic";

/// One servable Dapr channel — plain Send data derived from its [`ChannelDefinition`].
#[derive(Debug, Clone)]
struct DaprChannel {
    channel_name: String,
    module_key: String,
    tenant: String,
    pubsub_name: Option<String>,
    ce_mode: CeMode,
    ce_source_default: Option<String>,
    idempotency_key_header: Option<String>,
}

type RouteSnapshot = Arc<HashMap<String, DaprChannel>>;

/// The swappable topic → channel table the mounted [`Router`] reads dynamically — the Dapr
/// half of the binding-flip route swap, exactly like
/// [`sutra_channels::http::ChannelRouteTable`].
#[derive(Clone, Default)]
pub struct DaprRouteTable {
    routes: Arc<RwLock<RouteSnapshot>>,
}

impl DaprRouteTable {
    pub fn new() -> DaprRouteTable {
        DaprRouteTable::default()
    }

    pub fn swap(&self, routes: DaprRouteSet) {
        *self.routes.write().expect("dapr route table lock") = Arc::new(routes.0);
    }

    fn current(&self) -> RouteSnapshot {
        Arc::clone(&self.routes.read().expect("dapr route table lock"))
    }
}

/// A validated, servable Dapr route set (opaque — build via [`dapr_routes_of`]).
#[derive(Debug)]
pub struct DaprRouteSet(HashMap<String, DaprChannel>);

/// Resolve every `transport: dapr` inbound channel of `definitions` to its topic binding.
/// Fail-closed on a missing topic ([`codes::INBOUND_TOPIC_NOT_BOUND`]) and on two channels
/// claiming the same topic ([`shared_codes::CHANNEL_NAME_COLLISION`]).
/// `direction: outbound` definitions are skipped (a `<q:send>` target, not a served
/// route — resolved by the outbox dispatcher via [`super::sink::DaprMessageSink`] instead).
pub fn dapr_routes_of(definitions: &[ChannelDefinition]) -> Result<DaprRouteSet, Diagnostic> {
    let mut by_topic: HashMap<String, DaprChannel> = HashMap::new();
    for def in definitions {
        if def.transport.as_deref() != Some(TRANSPORT) {
            continue;
        }
        if def.is_outbound() {
            continue;
        }
        let props = DaprChannelProperties::from_definition(def)?;
        if !props.has_topic() {
            return Err(Diagnostic::error(
                codes::INBOUND_TOPIC_NOT_BOUND,
                format!(
                    "dapr channel '{}' requires property 'topic'",
                    def.binding.channel_name
                ),
            ));
        }
        let channel = DaprChannel {
            channel_name: def.binding.channel_name.clone(),
            module_key: def.binding.namespace.module_key(),
            tenant: def.binding.namespace.tenant.clone(),
            pubsub_name: props.pubsub_name.clone(),
            ce_mode: CeMode::parse(def.cloud_events_mode.as_deref()),
            ce_source_default: props.source.clone(),
            idempotency_key_header: def.idempotency_key_header.clone(),
        };
        if by_topic.insert(props.topic.clone(), channel).is_some() {
            return Err(Diagnostic::error(
                shared_codes::CHANNEL_NAME_COLLISION,
                format!(
                    "dapr topic '{}' is already bound; refusing to bind a second channel \
                     ('{}') to it.",
                    props.topic, def.binding.channel_name
                ),
            ));
        }
    }
    Ok(DaprRouteSet(by_topic))
}

struct AppState {
    handle: EngineHandle,
    routes: DaprRouteTable,
}

/// Build the axum [`Router`] serving whatever `table` currently holds — one route,
/// `POST /dapr/{topic}`, dispatched by the topic path segment.
pub fn dapr_router_dynamic(table: &DaprRouteTable, handle: EngineHandle) -> Router {
    let state = Arc::new(AppState {
        handle,
        routes: table.clone(),
    });
    Router::new()
        .route("/dapr/{topic}", post(handle_post))
        .with_state(state)
}

async fn handle_post(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let routes = state.routes.current();
    let Some(channel) = routes.get(&topic) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let copied_headers = copy_headers(&headers);

    // Topic-mismatch guard: a `dapr-topic` header disagreeing with the URL
    // path segment is a sidecar-side misconfiguration — reject rather than silently trust
    // the path.
    if let Some(reported) = lookup_ci(&copied_headers, HEADER_TOPIC) {
        if reported != topic {
            tracing::warn!(
                code = %codes::INBOUND_TOPIC_MISMATCH,
                path_topic = %topic,
                header_topic = %reported,
                "dapr delivery URL topic disagrees with header dapr-topic"
            );
            return StatusCode::BAD_REQUEST.into_response();
        }
    }

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let extraction = match cloudevents::extract(
        channel.ce_mode,
        &channel.channel_name,
        &copied_headers,
        content_type.as_deref(),
        &body,
        WrapDefaults {
            source: channel.ce_source_default.as_deref(),
            event_type: None,
        },
    ) {
        Ok(x) => x,
        Err(diagnostic) => return problem_status(&diagnostic).into_response(),
    };

    let (idempotency_key, explicit) = resolve_idempotency_key(
        channel,
        &copied_headers,
        extraction.explicit_id.as_deref(),
        &body,
    );

    // Lift Dapr-specific routing metadata onto the InboundMessage headers (`x-dapr-topic` /
    // `x-dapr-pubsubname`) so downstream FEEL expressions can reference it.
    let mut lifted_headers = copied_headers;
    lifted_headers.insert("x-dapr-topic".to_string(), topic.clone());
    if let Some(pubsub) = &channel.pubsub_name {
        lifted_headers
            .entry("x-dapr-pubsubname".to_string())
            .or_insert_with(|| pubsub.clone());
    }

    let message = InboundMessage {
        tenant: channel.tenant.clone(),
        module_key: channel.module_key.clone(),
        channel: channel.channel_name.clone(),
        headers: lifted_headers,
        body: extraction.body,
        content_type: extraction.content_type.clone(),
        idempotency_key,
        explicit_event_id: explicit,
        received_at: now_rfc3339(),
        cloud_event: extraction.cloud_event.map(Box::new),
    };

    match state.handle.dispatch(message).await {
        // Per Dapr's at-least-once contract 2xx acks the delivery; unlike the synchronous
        // HTTP transport there is no `<q:reply>` to render back — Dapr pub/sub is
        // fire-and-forget push, so the handler answers with a bare 200.
        // A dead-lettered (non-idempotent, failed) delivery is CONSUMED at-most-once with a
        // durable incident recorded; a 2xx suppresses the sidecar's at-least-once retry (a retry
        // would only re-fail and re-dead-letter).
        Ok(DispatchOutcome::Duplicate)
        | Ok(DispatchOutcome::Completed { .. })
        | Ok(DispatchOutcome::DeadLettered { .. }) => StatusCode::OK.into_response(),
        // Unreachable: `EngineHandle::dispatch` consumes every shard handoff on the router
        // side. Non-2xx so the sidecar retries per its at-least-once contract.
        Ok(DispatchOutcome::Handoff { .. }) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        // A reject/runtime diagnostic maps to a non-2xx so the sidecar retries per its
        // at-least-once contract (a rejected CloudEvent is a 400, anything else a 500).
        Err(diagnostic) => problem_status(&diagnostic).into_response(),
    }
}

fn problem_status(diagnostic: &Diagnostic) -> StatusCode {
    if diagnostic.code == shared_codes::INBOUND_REJECTED_CLOUDEVENT {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn copy_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
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

fn lookup_ci(headers: &BTreeMap<String, String>, name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

/// `IdempotencyKeyExtractor`: the channel's configured header (explicit) → the extracted
/// CloudEvent `id` (explicit) → SHA-256(body) (the transport fallback; NOT explicit, so it
/// never drives inbox dedup) — the same 3-tier resolution as the HTTP transport. The last
/// tier is deliberately deterministic: a freshly minted id would never dedup anything.
fn resolve_idempotency_key(
    channel: &DaprChannel,
    headers: &BTreeMap<String, String>,
    ce_id: Option<&str>,
    body: &[u8],
) -> (String, bool) {
    if let Some(header_name) = &channel.idempotency_key_header {
        if let Some(v) = lookup_ci(headers, header_name) {
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

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(name: &str, transport: &str, props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "orders", "v1");
        let binding = ChannelBinding::new(name, namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some(transport.to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn binds_one_channel_per_topic() {
        let defs = vec![definition(
            "orders-in",
            "dapr",
            &[("topic", "orders.created")],
        )];
        let set = dapr_routes_of(&defs).expect("routes");
        assert_eq!(set.0.len(), 1);
        assert!(set.0.contains_key("orders.created"));
    }

    #[test]
    fn non_dapr_and_outbound_definitions_are_skipped() {
        let mut other = definition("http-in", "http", &[]);
        let mut outbound = definition("orders-out", "dapr", &[("topic", "orders.out")]);
        outbound
            .properties
            .insert("direction".to_string(), "outbound".to_string());
        other.transport = Some("http".to_string());
        let set = dapr_routes_of(&[other, outbound]).expect("routes");
        assert!(set.0.is_empty());
    }

    #[test]
    fn missing_topic_is_rejected_at_route_build() {
        let defs = vec![definition("orders-in", "dapr", &[])];
        let err = dapr_routes_of(&defs).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_TOPIC_NOT_BOUND);
    }

    #[test]
    fn duplicate_topic_across_channels_collides() {
        let defs = vec![
            definition("orders-in-1", "dapr", &[("topic", "orders.created")]),
            definition("orders-in-2", "dapr", &[("topic", "orders.created")]),
        ];
        let err = dapr_routes_of(&defs).unwrap_err();
        assert_eq!(err.code, shared_codes::CHANNEL_NAME_COLLISION);
    }
}
