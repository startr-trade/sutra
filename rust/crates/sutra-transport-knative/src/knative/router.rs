//! The Knative inbound route table + axum router — the subscription-keyed analogue of
//! `sutra_channels::http`'s `(METHOD, path)` route table. A Knative Subscription/Trigger
//! delivers to ONE catch-all route (`POST /knative/{subscription}`, mirroring
//! `KnativeTriggerSource.handleKnativePost`); the subscription segment resolves the served
//! channel.
//!
//! ## `ack-mode` — the response IS the ack
//!
//! Knative Eventing's data-plane contract makes the SUBSCRIBER'S HTTP RESPONSE the settle
//! signal — there is no detached ack, so the deferred-ack registry's broker pattern
//! (register-and-return) cannot be the mechanism here. The registry is still the timing
//! source; what differs is what the transport does with it: instead of calling
//! `basic.ack` from the settle callback, this router **holds the push response** until the
//! callback fires, and then answers with the status the decision maps to. That is the HTTP
//! transport's own `on-complete` semantics (connection-hold), applied to a push:
//!
//! | engine outcome | response | Knative data-plane meaning |
//! |---|---|---|
//! | instance COMPLETED / duplicate | `202` | accepted, no retry |
//! | instance FAILED / dead-lettered | `422` | non-retryable failure → `deadLetterSink` |
//! | held past [`super::HOLD_TIMEOUT_PROPERTY`] | `202` + [`codes::INBOUND_HOLD_TIMEOUT`] | accepted (this delivery degrades to `on-persist`) |
//! | settle dropped unfired | `500` | retryable → the sender redelivers, inbox dedup absorbs it |
//!
//! The retryable/non-retryable split is the spec's, not ours: `5xx` / `404` / `408` / `409`
//! / `429` are the retryable codes, and "other `4xx`" must NOT be retried — which is exactly
//! the `AckDecision::NackDrop` (permanent reject, DLQ posture) the registry hands us.
//!
//! **The hold is bounded, and the bound is the operator's.** A held response occupies a
//! delivery slot on the sender (and, when the engine runs as a Knative Service, a request
//! against the revision's `timeoutSeconds`). The channel's `on-complete.hold-timeout` must
//! therefore be authored BELOW the pushing Subscription/Trigger's `DeliverySpec.timeout`;
//! otherwise the sender times out first, retries, and every held delivery becomes a
//! redelivery (absorbed by inbox dedup, but the on-complete guarantee is gone). `on-persist`
//! (the transport default) is unchanged: the response goes out at dispatch return.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

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
use sutra_channels::{
    codes as shared_codes, AckDecision, DeferredDispatch, DeferredSettle, DispatchOutcome,
    EngineHandle, InboundMessage,
};

use super::{codes, KnativeChannelProperties, HOLD_TIMEOUT_WARN_ABOVE, TRANSPORT};

/// One servable Knative channel — plain Send data derived from its [`ChannelDefinition`].
#[derive(Debug, Clone)]
struct KnativeChannel {
    channel_name: String,
    module_key: String,
    tenant: String,
    ce_mode: CeMode,
    ce_source_default: Option<String>,
    idempotency_key_header: Option<String>,
    /// `ack-mode: on-complete` (the transport default is `on-persist`): hold the
    /// push response until the instance's terminal event. See the module docs.
    hold_until_complete: bool,
    /// The hold bound (`on-complete.hold-timeout`); unused under `on-persist`.
    hold_timeout: Duration,
}

type RouteSnapshot = Arc<HashMap<String, KnativeChannel>>;

/// The swappable subscription → channel table the mounted [`Router`] reads dynamically —
/// the Knative half of the binding-flip route swap.
#[derive(Clone, Default)]
pub struct KnativeRouteTable {
    routes: Arc<RwLock<RouteSnapshot>>,
}

impl KnativeRouteTable {
    pub fn new() -> KnativeRouteTable {
        KnativeRouteTable::default()
    }

    pub fn swap(&self, routes: KnativeRouteSet) {
        *self.routes.write().expect("knative route table lock") = Arc::new(routes.0);
    }

    fn current(&self) -> RouteSnapshot {
        Arc::clone(&self.routes.read().expect("knative route table lock"))
    }
}

/// A validated, servable Knative route set (opaque — build via [`knative_routes_of`]).
#[derive(Debug)]
pub struct KnativeRouteSet(HashMap<String, KnativeChannel>);

/// Resolve every `transport: knative` inbound channel of `definitions` to its subscription
/// binding. Fail-closed on a missing subscription
/// ([`codes::INBOUND_SUBSCRIPTION_NOT_BOUND`], mirroring
/// `KnativeTriggerSource.start`'s `hasSubscription()` guard) and on a subscription collision
/// ([`shared_codes::CHANNEL_NAME_COLLISION`]). `direction: outbound` definitions are skipped
/// (a `<q:send>` target, resolved by the outbox dispatcher via
/// [`super::sink::KnativeMessageSink`] instead).
pub fn knative_routes_of(definitions: &[ChannelDefinition]) -> Result<KnativeRouteSet, Diagnostic> {
    let mut by_subscription: HashMap<String, KnativeChannel> = HashMap::new();
    for def in definitions {
        if def.transport.as_deref() != Some(TRANSPORT) {
            continue;
        }
        if def.is_outbound() {
            continue;
        }
        let props = KnativeChannelProperties::from_definition(def)?;
        if !props.has_subscription() {
            return Err(Diagnostic::error(
                codes::INBOUND_SUBSCRIPTION_NOT_BOUND,
                format!(
                    "knative channel '{}' requires property 'subscription'",
                    def.binding.channel_name
                ),
            ));
        }
        let hold_until_complete = def.wants_on_complete_ack();
        if hold_until_complete && props.hold_timeout > HOLD_TIMEOUT_WARN_ABOVE {
            // Not fatal (the engine may not be running as a Knative Service), but it cannot
            // be honoured under Serving's default ceiling — say so once, at route build.
            tracing::warn!(
                channel = %def.binding.channel_name,
                hold_timeout_secs = props.hold_timeout.as_secs(),
                "knative channel holds its on-complete push response longer than Knative \
                 Serving's default max-revision-timeout-seconds (600s) — a request that \
                 outlives the revision timeout is terminated by the queue-proxy and retried"
            );
        }
        let channel = KnativeChannel {
            channel_name: def.binding.channel_name.clone(),
            module_key: def.binding.namespace.module_key(),
            tenant: def.binding.namespace.tenant.clone(),
            ce_mode: CeMode::parse(def.cloud_events_mode.as_deref()),
            ce_source_default: props.source.clone(),
            idempotency_key_header: def.idempotency_key_header.clone(),
            hold_until_complete,
            hold_timeout: props.hold_timeout,
        };
        if by_subscription
            .insert(props.subscription.clone(), channel)
            .is_some()
        {
            return Err(Diagnostic::error(
                shared_codes::CHANNEL_NAME_COLLISION,
                format!(
                    "knative subscription '{}' is already bound; refusing to bind a second \
                     channel ('{}') to it.",
                    props.subscription, def.binding.channel_name
                ),
            ));
        }
    }
    Ok(KnativeRouteSet(by_subscription))
}

struct AppState {
    handle: EngineHandle,
    routes: KnativeRouteTable,
}

/// Build the axum [`Router`] serving whatever `table` currently holds — one route,
/// `POST /knative/{subscription}`, dispatched by the subscription path segment.
pub fn knative_router_dynamic(table: &KnativeRouteTable, handle: EngineHandle) -> Router {
    let state = Arc::new(AppState {
        handle,
        routes: table.clone(),
    });
    Router::new()
        .route("/knative/{subscription}", post(handle_post))
        .with_state(state)
}

async fn handle_post(
    State(state): State<Arc<AppState>>,
    Path(subscription): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let routes = state.routes.current();
    let Some(channel) = routes.get(&subscription) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let copied_headers = copy_headers(&headers);

    // A partial CloudEvent (some `ce-*` headers present but not all of id/source/type) is a
    // hard reject here, ahead of CE extraction — unlike Dapr, which tolerates it and falls
    // back to the body-hash idempotency key.
    if has_any_ce_header(&copied_headers) && !has_all_required_ce_headers(&copied_headers) {
        tracing::warn!(
            code = %codes::INBOUND_MISSING_HEADERS,
            channel = %channel.channel_name,
            "knative delivery presented a partial CloudEvent (missing one or more of \
             ce-id, ce-source, ce-type)"
        );
        return StatusCode::BAD_REQUEST.into_response();
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

    let message = InboundMessage {
        tenant: channel.tenant.clone(),
        module_key: channel.module_key.clone(),
        channel: channel.channel_name.clone(),
        headers: copied_headers,
        body: extraction.body,
        content_type: extraction.content_type.clone(),
        idempotency_key,
        explicit_event_id: explicit,
        received_at: now_rfc3339(),
        cloud_event: extraction.cloud_event.map(Box::new),
    };

    if !channel.hold_until_complete {
        // `ack-mode: on-persist` (the transport default) — settle at dispatch return.
        return match state.handle.dispatch(message).await {
            // Knative subscribers ack with a 2xx-empty; this handler answers 202 — non-2xx
            // triggers retry per the subscription's deliverySpec policy.
            // A dead-lettered (non-idempotent, failed) delivery is CONSUMED at-most-once: a
            // durable incident was recorded, so a 2xx suppresses the subscription's retry policy (a
            // retry would only re-fail and re-dead-letter).
            Ok(DispatchOutcome::Duplicate)
            | Ok(DispatchOutcome::Completed { .. })
            | Ok(DispatchOutcome::DeadLettered { .. }) => StatusCode::ACCEPTED.into_response(),
            // Unreachable: `EngineHandle::dispatch` consumes every shard handoff on the
            // router side. Non-2xx so the subscription's retry policy re-delivers.
            Ok(DispatchOutcome::Handoff { .. }) => {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            Err(diagnostic) => problem_status(&diagnostic).into_response(),
        };
    }

    // `ack-mode: on-complete` — response-hold (see the module docs). The settle callbacks
    // resolve THIS request instead of calling a broker ack: they ride the dispatch onto the
    // engine actor, the park arm registers them on the deferred-ack registry, and whichever
    // one the instance's terminal event fires releases the held response.
    let (decided, settle) = response_settle();
    match state.handle.dispatch_deferred(message, settle).await {
        // No park — the terminal events fired inside the dispatch, exactly like on-persist;
        // the settle callbacks dropped unfired and the outcome decides the status now.
        Ok(DeferredDispatch::Settled(outcome)) => settled_response(&outcome),
        Ok(DeferredDispatch::Deferred { instance_id }) => {
            match tokio::time::timeout(channel.hold_timeout, decided).await {
                Ok(Ok(decision)) => decision_response(decision),
                // The registry dropped the entry without firing either callback (engine actor
                // gone, or a duplicate registration was refused) — retryable, and the
                // redelivery is absorbed by inbox dedup.
                Ok(Err(_recv)) => {
                    tracing::warn!(
                        code = %codes::INBOUND_HOLD_ABANDONED,
                        channel = %channel.channel_name,
                        instance = %instance_id,
                        "knative on-complete hold ended without a settle decision — answering \
                         retryable (the sender redelivers; inbox dedup absorbs it)"
                    );
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
                // The bound expired with the instance still running. The intake IS durable
                // (the park committed) and a redelivery would only be deduped, so the honest
                // answer is ACCEPT — this ONE delivery degrades to on-persist, loudly.
                Err(_elapsed) => {
                    tracing::warn!(
                        code = %codes::INBOUND_HOLD_TIMEOUT,
                        channel = %channel.channel_name,
                        instance = %instance_id,
                        hold_timeout_secs = channel.hold_timeout.as_secs_f64(),
                        "knative on-complete hold expired before the instance reached a \
                         terminal state — releasing the push response as 202 (this delivery \
                         runs on-persist; raise on-complete.hold-timeout, and the sender's \
                         DeliverySpec.timeout with it, or use ack-mode: on-persist)"
                    );
                    StatusCode::ACCEPTED.into_response()
                }
            }
        }
        Err(diagnostic) => problem_status(&diagnostic).into_response(),
    }
}

/// Build the per-delivery [`DeferredSettle`] whose callbacks resolve the held response.
/// Both callbacks share ONE `oneshot::Sender` (the registry fires exactly one of them, and
/// `Option::take` makes that structural); they run on the engine actor thread or the sweep
/// task, so they only hand over a value — never block, never touch the network.
fn response_settle() -> (tokio::sync::oneshot::Receiver<AckDecision>, DeferredSettle) {
    let (tx, rx) = tokio::sync::oneshot::channel::<AckDecision>();
    let shared = Arc::new(Mutex::new(Some(tx)));
    fn once(
        shared: &Arc<Mutex<Option<tokio::sync::oneshot::Sender<AckDecision>>>>,
        decision: AckDecision,
    ) -> Box<dyn FnMut() + Send> {
        let shared = Arc::clone(shared);
        Box::new(move || {
            // A closed receiver means the push connection went away first (client
            // disconnect, or our own hold timeout already answered) — the instance is
            // unaffected, so this is a no-op, not a failure.
            if let Some(tx) = shared.lock().expect("knative hold settle").take() {
                let _ = tx.send(decision);
            }
        })
    }
    let settle = DeferredSettle {
        ack: once(&shared, AckDecision::Ack),
        nack: once(&shared, AckDecision::NackDrop),
    };
    (rx, settle)
}

/// The settle decision → Knative data-plane status mapping (see the module docs).
fn decision_response(decision: AckDecision) -> Response {
    match decision {
        AckDecision::Ack => StatusCode::ACCEPTED.into_response(),
        // Permanent reject: a non-retryable 4xx, which the sender routes to its
        // `deadLetterSink` instead of re-driving a flow that already failed.
        AckDecision::NackDrop => StatusCode::UNPROCESSABLE_ENTITY.into_response(),
        // The registry only ever fires ack/nack(drop); mapped for completeness — a retryable
        // 5xx is the requeue analogue on this transport.
        AckDecision::NackRequeue => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// An `on-complete` dispatch that never parked: the same outcome→status mapping as
/// `on-persist`, EXCEPT that a dead-letter is an instance failure and therefore carries the
/// permanent-reject status (the `NackDrop` posture the registry would have fired).
fn settled_response(outcome: &DispatchOutcome) -> Response {
    match outcome {
        DispatchOutcome::Duplicate | DispatchOutcome::Completed { .. } => {
            StatusCode::ACCEPTED.into_response()
        }
        DispatchOutcome::DeadLettered { .. } => decision_response(AckDecision::NackDrop),
        // Unreachable: `EngineHandle::dispatch_deferred` consumes every shard handoff on
        // the router side. Non-2xx so the subscription's retry policy re-delivers.
        DispatchOutcome::Handoff { .. } => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn problem_status(diagnostic: &Diagnostic) -> StatusCode {
    if diagnostic.code == shared_codes::INBOUND_REJECTED_CLOUDEVENT {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn has_any_ce_header(headers: &BTreeMap<String, String>) -> bool {
    headers
        .keys()
        .any(|k| k.to_ascii_lowercase().starts_with("ce-"))
}

fn has_all_required_ce_headers(headers: &BTreeMap<String, String>) -> bool {
    ["ce-id", "ce-source", "ce-type"]
        .iter()
        .all(|h| lookup_ci(headers, h).is_some())
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
/// never drives inbox dedup) — the same 3-tier resolution as the HTTP/Dapr transports. The
/// last tier is deliberately deterministic: a freshly minted id would never dedup anything.
fn resolve_idempotency_key(
    channel: &KnativeChannel,
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
    fn binds_one_channel_per_subscription() {
        let defs = vec![definition(
            "orders-in",
            "knative",
            &[("subscription", "orders-sub")],
        )];
        let set = knative_routes_of(&defs).expect("routes");
        assert_eq!(set.0.len(), 1);
        assert!(set.0.contains_key("orders-sub"));
    }

    #[test]
    fn non_knative_and_outbound_definitions_are_skipped() {
        let mut other = definition("http-in", "http", &[]);
        other.transport = Some("http".to_string());
        let mut outbound = definition(
            "orders-out",
            "knative",
            &[("subscription", "orders-out-sub")],
        );
        outbound
            .properties
            .insert("direction".to_string(), "outbound".to_string());
        let set = knative_routes_of(&[other, outbound]).expect("routes");
        assert!(set.0.is_empty());
    }

    #[test]
    fn missing_subscription_is_rejected_at_route_build() {
        let defs = vec![definition("orders-in", "knative", &[])];
        let err = knative_routes_of(&defs).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_SUBSCRIPTION_NOT_BOUND);
    }

    #[test]
    fn duplicate_subscription_across_channels_collides() {
        let defs = vec![
            definition("orders-in-1", "knative", &[("subscription", "orders-sub")]),
            definition("orders-in-2", "knative", &[("subscription", "orders-sub")]),
        ];
        let err = knative_routes_of(&defs).unwrap_err();
        assert_eq!(err.code, shared_codes::CHANNEL_NAME_COLLISION);
    }

    #[test]
    fn partial_ce_headers_are_detected() {
        let mut h = BTreeMap::new();
        h.insert("ce-id".to_string(), "e1".to_string());
        h.insert("ce-source".to_string(), "/s".to_string());
        assert!(has_any_ce_header(&h));
        assert!(!has_all_required_ce_headers(&h));

        h.insert("ce-type".to_string(), "t".to_string());
        assert!(has_all_required_ce_headers(&h));
    }
}
