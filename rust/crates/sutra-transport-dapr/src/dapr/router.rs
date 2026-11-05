//! The Dapr inbound route table + axum router — the topic-keyed analogue of
//! `sutra_channels::http`'s `(METHOD, path)` route table. Dapr's sidecar delivers every
//! subscribed topic to ONE catch-all route (`POST /dapr/{topic}`); the topic path segment
//! resolves the served channels. A topic is broadcast: every channel bound to it, in any
//! deployment, receives its own copy of each delivery.

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

type RouteSnapshot = Arc<HashMap<String, Vec<DaprChannel>>>;

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
pub struct DaprRouteSet(HashMap<String, Vec<DaprChannel>>);

/// Resolve every `transport: dapr` inbound channel of `definitions` to its topic binding.
/// Fail-closed on a missing topic ([`codes::INBOUND_TOPIC_NOT_BOUND`]). Several channels may bind
/// one topic.
/// `direction: outbound` definitions are skipped (a `<q:send>` target, not a served
/// route — resolved by the outbox dispatcher via [`super::sink::DaprMessageSink`] instead).
pub fn dapr_routes_of(definitions: &[ChannelDefinition]) -> Result<DaprRouteSet, Diagnostic> {
    let mut by_topic: HashMap<String, Vec<DaprChannel>> = HashMap::new();
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
        by_topic
            .entry(props.topic.clone())
            .or_default()
            .push(channel);
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
    let Some(channels) = routes.get(&topic) else {
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

    // One catch-all route serves every component, so a topic name on two components needs telling
    // apart here: an event must never reach a subscriber of another component.
    let channels = match subscribers_of_delivery(channels, &body) {
        Ok(channels) => channels,
        Err(status) => {
            tracing::warn!(
                code = %codes::INBOUND_TOPIC_MISMATCH,
                topic = %topic,
                "dapr delivery on a topic several pub/sub components use names no subscribed component"
            );
            return status.into_response();
        }
    };

    if let [only] = channels.as_slice() {
        return deliver(
            state.handle.clone(),
            only.clone(),
            topic,
            copied_headers,
            content_type,
            body,
        )
        .await
        .into_response();
    }

    // Broadcast: each subscriber gets its own copy, concurrently, so a slow one does not delay
    // the rest. Anything short of all-accepted answers non-2xx so the sidecar redelivers, to every
    // subscriber. What stops one that already accepted from running again is its inbox: an event id
    // or a declared key dedupes per subscriber. With neither, a redelivery runs again, and the lint
    // warns the author of such a flow.
    let mut deliveries = tokio::task::JoinSet::new();
    for channel in channels {
        deliveries.spawn(deliver(
            state.handle.clone(),
            channel,
            topic.clone(),
            copied_headers.clone(),
            content_type.clone(),
            body.clone(),
        ));
    }
    let mut worst = StatusCode::OK;
    while let Some(result) = deliveries.join_next().await {
        let status = result.unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        if delivery_severity(status) > delivery_severity(worst) {
            worst = status;
        }
    }
    worst.into_response()
}

/// The channels of a topic this delivery belongs to. Dapr's envelope names the component, and a
/// channel that names another component does not get it. Without an envelope (a raw payload) a topic
/// several components use cannot attribute the delivery, so nobody gets it rather than a stranger.
/// Either way the answer is a 404: the sidecar drops the message on a 404 and retries anything else
/// that is not a 2xx, and neither case can ever come out differently.
fn subscribers_of_delivery(
    channels: &[DaprChannel],
    body: &[u8],
) -> Result<Vec<DaprChannel>, StatusCode> {
    let mut components: Vec<&str> = channels
        .iter()
        .filter_map(|c| c.pubsub_name.as_deref())
        .collect();
    components.sort_unstable();
    components.dedup();
    if components.is_empty() {
        return Ok(channels.to_vec());
    }
    let envelope: Option<serde_json::Value> = serde_json::from_slice(body).ok();
    let Some(component) = envelope
        .as_ref()
        .and_then(|e| e.get("pubsubname"))
        .and_then(serde_json::Value::as_str)
    else {
        if components.len() == 1 {
            return Ok(channels.to_vec());
        }
        return Err(StatusCode::NOT_FOUND);
    };
    let matching: Vec<DaprChannel> = channels
        .iter()
        .filter(|c| {
            c.pubsub_name
                .as_deref()
                .is_none_or(|name| name == component)
        })
        .cloned()
        .collect();
    if matching.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(matching)
}

/// How badly a subscriber's answer should weigh on the one answer the sidecar gets: a retryable
/// failure outranks a rejection, which outranks success.
fn delivery_severity(status: StatusCode) -> u8 {
    if status.is_success() {
        0
    } else if status.is_client_error() {
        1
    } else {
        2
    }
}

/// Deliver one topic message to one subscribing channel.
async fn deliver(
    handle: EngineHandle,
    channel: DaprChannel,
    topic: String,
    copied_headers: BTreeMap<String, String>,
    content_type: Option<String>,
    body: Bytes,
) -> StatusCode {
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
        Err(diagnostic) => return problem_status(&diagnostic),
    };

    let (idempotency_key, explicit) = resolve_idempotency_key(
        &channel,
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

    match handle.dispatch(message).await {
        // Per Dapr's at-least-once contract 2xx acks the delivery; unlike the synchronous
        // HTTP transport there is no `<q:reply>` to render back — Dapr pub/sub is
        // fire-and-forget push, so the handler answers with a bare 200.
        // A dead-lettered (non-idempotent, failed) delivery is CONSUMED at-most-once with a
        // durable incident recorded; a 2xx suppresses the sidecar's at-least-once retry (a retry
        // would only re-fail and re-dead-letter).
        Ok(DispatchOutcome::Duplicate { .. })
        | Ok(DispatchOutcome::Completed { .. })
        | Ok(DispatchOutcome::DeadLettered { .. }) => StatusCode::OK,
        // Unreachable: `EngineHandle::dispatch` consumes every shard handoff on the router
        // side. Non-2xx so the sidecar retries per its at-least-once contract.
        Ok(DispatchOutcome::Handoff { .. } | DispatchOutcome::InlineCalls(_)) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        // A reject/runtime diagnostic maps to a non-2xx so the sidecar retries per its
        // at-least-once contract (a rejected CloudEvent is a 400, anything else a 500).
        Err(diagnostic) => problem_status(&diagnostic),
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

    fn channel_on(component: Option<&str>) -> DaprChannel {
        DaprChannel {
            channel_name: format!("in-{}", component.unwrap_or("any")),
            module_key: "m".to_string(),
            tenant: "t".to_string(),
            pubsub_name: component.map(str::to_string),
            ce_mode: CeMode::parse(None),
            ce_source_default: None,
            idempotency_key_header: None,
        }
    }

    #[test]
    fn a_delivery_reaches_only_the_subscribers_of_its_component() {
        let channels = [channel_on(Some("bus-a")), channel_on(Some("bus-b"))];
        let found =
            subscribers_of_delivery(&channels, br#"{"id":"1","pubsubname":"bus-b"}"#).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pubsub_name.as_deref(), Some("bus-b"));
    }

    #[test]
    fn a_delivery_that_names_no_component_is_refused_when_several_share_the_topic() {
        let channels = [channel_on(Some("bus-a")), channel_on(Some("bus-b"))];
        assert_eq!(
            subscribers_of_delivery(&channels, br#"{"pubsubname":"bus-c"}"#).unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn one_component_needs_no_envelope() {
        let channels = [channel_on(Some("bus-a")), channel_on(Some("bus-a"))];
        assert_eq!(subscribers_of_delivery(&channels, b"raw").unwrap().len(), 2);
    }

    /// One channel names a component and one names none: the delivery still says which it came from.
    #[test]
    fn a_delivery_from_another_component_skips_a_subscriber_that_named_its_own() {
        let channels = [channel_on(Some("bus-a")), channel_on(None)];
        let found =
            subscribers_of_delivery(&channels, br#"{"id":"1","pubsubname":"bus-z"}"#).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pubsub_name, None);
        let only = [channel_on(Some("bus-a"))];
        assert_eq!(
            subscribers_of_delivery(&only, br#"{"pubsubname":"bus-z"}"#).unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    /// The sidecar retries anything but a 2xx or a 404, and this delivery can never be attributed.
    #[test]
    fn a_delivery_that_can_never_be_attributed_is_not_retried() {
        let channels = [channel_on(Some("bus-a")), channel_on(Some("bus-b"))];
        assert_eq!(
            subscribers_of_delivery(&channels, b"raw").unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn a_topic_broadcasts_to_every_channel_bound_to_it() {
        let defs = vec![
            definition("orders-in-1", "dapr", &[("topic", "orders.created")]),
            definition("orders-in-2", "dapr", &[("topic", "orders.created")]),
        ];
        let set = dapr_routes_of(&defs).unwrap();
        let bound: Vec<&str> = set.0["orders.created"]
            .iter()
            .map(|c| c.channel_name.as_str())
            .collect();
        assert_eq!(bound, ["orders-in-1", "orders-in-2"]);
    }

    #[test]
    fn the_worst_subscriber_answer_wins() {
        assert!(
            delivery_severity(StatusCode::INTERNAL_SERVER_ERROR)
                > delivery_severity(StatusCode::BAD_REQUEST)
        );
        assert!(delivery_severity(StatusCode::BAD_REQUEST) > delivery_severity(StatusCode::OK));
    }
}
