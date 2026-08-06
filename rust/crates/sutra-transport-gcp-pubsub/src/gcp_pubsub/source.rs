//! The GCP Pub/Sub inbound trigger — [`GcpPubSubTriggerSource`] implements [`TriggerSource`]:
//! one Subscriber (pull consumer) per channel binding, messages projected into
//! [`InboundMessage`]s and pushed through the [`InboundIntake`] seam, the returned
//! [`AckDecision`] executed as a native settle (Pub/Sub `ack` / `nack`).
//!
//! The trigger-source lifecycle:
//!
//! - **Broker absence is NON-FATAL**: `start` spawns a supervisor task and resolves;
//!   a missing/unreachable Pub/Sub endpoint WARNs
//!   (`SUTRA.INBOUND.GCP_PUBSUB.CONNECTION_FAILED`) and the supervisor retries with
//!   exponential backoff in the background — readiness is unaffected.
//! - **Ack timing rides the intake seam**: the source `await`s
//!   [`InboundIntake::deliver`] and maps the decision onto Pub/Sub's settle model — `Ack` →
//!   `message.ack()` (the delivery is owned), `NackDrop` → `message.ack()` (Pub/Sub has no
//!   DLQ by default, so acking IS the drop — the poison never redelivers), `NackRequeue` →
//!   `message.nack()` so Pub/Sub redelivers after the ack deadline (inbox dedup absorbs the
//!   duplicate).
//! - **`ack-mode: on-complete`**: the source instead calls
//!   [`InboundIntake::deliver_deferred`], handing the engine per-delivery settle callbacks
//!   closed over an `Arc<ReceivedMessage>` (the ack id + its subscriber client — `Send +
//!   Sync`, so the handle simply moves into the closures). A PARKED instance answers
//!   [`DeliveryDisposition::Deferred`] — the source does NOT settle; the deferred-ack
//!   registry fires the held `ack()` at the instance's terminal event. A dispatch that ran
//!   to completion answers [`DeliveryDisposition::Settle`] and settles at return exactly
//!   like `on-persist`.
//! - **Singleton gating**: the subscriber pulls ONLY while `gate.is_leading()`; on
//!   leadership loss it drops the session (in-flight messages redeliver under the ack
//!   deadline) and re-opens when the gate returns. The engine per-channel lease — not
//!   Pub/Sub — is what makes a `singleton: true` channel consume on exactly one replica.
//!
//! Idempotency key: the `sutra-outbox-key` attribute when set (`explicit_event_id = true`);
//! otherwise the broker-assigned `message_id` (NON-explicit, so it never suppresses a
//! re-post through inbox dedup).
//!
//! ## Ack-deadline caveat for `on-complete` (OPERATOR GUIDANCE)
//!
//! Pub/Sub leases a pulled message for the subscription's **ack deadline**
//! (`ackDeadlineSeconds`, 10 s…600 s), not for the life of the consumer. A deferred settle
//! is therefore only valid *while that lease holds*, and this source uses the unary `pull`
//! API — which, unlike the streaming `subscribe`/`receive` paths, does **no automatic
//! lease extension**: there is no `ModifyAckDeadline` heartbeat while an instance is
//! parked. Consequences, honestly stated:
//!
//! 1. **Redelivery while parked.** An instance still parked when the ack deadline lapses
//!    sees Pub/Sub redeliver the message to this same subscriber — a second, concurrent
//!    delivery alongside the parked original. The idempotency key is stable across
//!    redeliveries (`sutra-outbox-key`, else the broker `message_id`), so **inbox dedup is
//!    what absorbs it**; the duplicate resolves and settles immediately. `on-complete` on
//!    Pub/Sub does NOT hold a message indefinitely.
//! 2. **Late settle is a best-effort no-op.** A held `ack()` fired after the lease lapsed
//!    (or after the session died) targets a stale ack id: Pub/Sub answers or ignores it,
//!    the error is swallowed as a WARN, and redelivery + inbox dedup are the recovery.
//! 3. **Sizing rule.** Raise the subscription's `ackDeadlineSeconds` toward its 600 s
//!    ceiling for parked workloads, and keep `sutra.ack.deferred.timeout` (the registry
//!    sweep) ≤ that deadline so the registry drops a stuck instance before Pub/Sub starts
//!    redelivering it. Beyond 600 s of park, redelivery-plus-dedup is the only contract —
//!    prefer `on-persist` there.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscriber::ReceivedMessage;
use google_cloud_pubsub::subscription::Subscription;
use tracing::{info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{
    attributes_to_headers, build_client_config, codes, AckMode, GcpPubSubChannelProperties,
    ATTR_CONTENT_TYPE, ATTR_OUTBOX_KEY, HEADER_MESSAGE_ID, HEADER_ORDERING_KEY, TRANSPORT,
};

/// Everything one subscriber needs, prepared by the wiring.
#[derive(Debug, Clone)]
pub struct GcpPubSubSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Project/subscription/ack-mode properties.
    pub properties: GcpPubSubChannelProperties,
    /// Leadership re-check cadence while idle / the max time one pull blocks before the
    /// gate is re-checked.
    pub gate_poll: Duration,
    /// Reconnect backoff floor (doubles per failure up to [`Self::reconnect_max`]).
    pub reconnect_min: Duration,
    /// Reconnect backoff ceiling.
    pub reconnect_max: Duration,
    /// Max messages a single pull requests (batch size; each is settled in order).
    pub pull_max: i32,
    /// Per-message inbound auth (`inbound-auth.*`), resolved once at wiring. `None` =
    /// no inbound-auth declared. A rejected credential drops the message (Pub/Sub `ack`).
    pub inbound_auth: Option<BrokerInboundAuth>,
}

impl GcpPubSubSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff, pull batch of 16.
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: GcpPubSubChannelProperties,
    ) -> GcpPubSubSourceConfig {
        GcpPubSubSourceConfig {
            tenant: tenant.to_string(),
            module_key: module_key.to_string(),
            channel: channel.to_string(),
            properties,
            gate_poll: Duration::from_secs(1),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            pull_max: 16,
            inbound_auth: None,
        }
    }
}

/// One Pub/Sub subscriber serving one channel binding (the singleton unit).
pub struct GcpPubSubTriggerSource {
    config: GcpPubSubSourceConfig,
    running: tokio::sync::Mutex<Option<Running>>,
}

struct Running {
    task: tokio::task::JoinHandle<()>,
    stop: Arc<StopToken>,
}

/// Cooperative stop signal shared with the supervisor task.
struct StopToken {
    requested: AtomicBool,
    notify: tokio::sync::Notify,
}

impl StopToken {
    fn new() -> StopToken {
        StopToken {
            requested: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Sleep that wakes early on stop; returns true when stop was requested.
    async fn sleep(&self, duration: Duration) -> bool {
        if self.is_requested() {
            return true;
        }
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = tokio::time::sleep(duration) => {}
        }
        self.is_requested()
    }
}

/// Why one broker session ended (connect → consume → teardown).
enum SessionEnd {
    Stopped,
    LeadershipLost,
    ConnectionLost,
}

impl GcpPubSubTriggerSource {
    /// A source for one channel binding. `config.properties` must carry a subscription
    /// ([`codes::INBOUND_SUBSCRIPTION_MISSING`]) — fail-closed at wiring time. `project-id`
    /// is already validated by [`GcpPubSubChannelProperties::from_definition`].
    pub fn new(config: GcpPubSubSourceConfig) -> Result<GcpPubSubTriggerSource, Diagnostic> {
        if !config.properties.has_subscription() {
            return Err(Diagnostic::error(
                codes::INBOUND_SUBSCRIPTION_MISSING,
                format!(
                    "gcp-pubsub inbound channel '{}' requires property 'subscription'",
                    config.channel
                ),
            ));
        }
        Ok(GcpPubSubTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured subscription (diagnostics / tests).
    pub fn subscription(&self) -> &str {
        &self.config.properties.subscription
    }
}

impl TriggerSource for GcpPubSubTriggerSource {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    fn channel(&self) -> &str {
        &self.config.channel
    }

    fn start(
        &self,
        intake: Arc<dyn InboundIntake>,
        gate: Arc<dyn LeaderGate>,
    ) -> BoxFuture<'_, Result<(), Diagnostic>> {
        Box::pin(async move {
            let mut running = self.running.lock().await;
            if running.is_some() {
                return Ok(()); // idempotent — the supervisor is already up
            }
            // Per-message mTLS is UNSUPPORTED: allow-through with a one-time boot WARN
            // (broker/transport-level TLS still applies).
            if let Some(auth) = &self.config.inbound_auth {
                if auth.scheme() == InboundScheme::Mtls {
                    warn!(
                        channel = %self.config.channel,
                        code = codes::INBOUND_MTLS_UNSUPPORTED,
                        "gcp-pubsub channel declared inbound-auth.scheme=mtls but per-channel \
                         mTLS is not supported — falling back to transport-level TLS"
                    );
                }
            }
            let stop = Arc::new(StopToken::new());
            let task = tokio::spawn(supervise(
                self.config.clone(),
                intake,
                gate,
                Arc::clone(&stop),
            ));
            *running = Some(Running { task, stop });
            Ok(())
        })
    }

    fn stop(&self) -> BoxFuture<'_, Result<(), Diagnostic>> {
        Box::pin(async move {
            let taken = { self.running.lock().await.take() };
            let Some(Running { task, stop }) = taken else {
                return Ok(()); // idempotent
            };
            stop.request();
            if let Err(e) = task.await {
                warn!(
                    channel = %self.config.channel,
                    error = %e,
                    "gcp-pubsub source supervisor did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The subscriber supervisor: leadership-gated open → pull → settle loop with exponential
/// reconnect backoff. Broker absence never escapes as an error.
async fn supervise(
    config: GcpPubSubSourceConfig,
    intake: Arc<dyn InboundIntake>,
    gate: Arc<dyn LeaderGate>,
    stop: Arc<StopToken>,
) {
    let mut backoff = config.reconnect_min;
    loop {
        if stop.is_requested() {
            return;
        }
        if !gate.is_leading() {
            // Not (or no longer) the leader — pull nothing and re-check.
            if stop.sleep(config.gate_poll).await {
                return;
            }
            continue;
        }
        match open_session(&config).await {
            Err(diagnostic) => {
                // NON-FATAL: WARN + retry in the background; readiness unaffected.
                warn!(
                    channel = %config.channel,
                    code = %diagnostic.code,
                    "gcp-pubsub subscriber unavailable — retrying in {:?}: {}",
                    backoff,
                    diagnostic.message
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
            Ok(subscription) => {
                backoff = config.reconnect_min;
                info!(
                    channel = %config.channel,
                    subscription = %config.properties.subscription,
                    project = %config.properties.project_id,
                    "gcp-pubsub subscriber up"
                );
                let end = run_session(&config, &intake, &gate, &stop, &subscription).await;
                match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::LeadershipLost => info!(
                        channel = %config.channel,
                        "gcp-pubsub subscriber paused — leadership lost"
                    ),
                    SessionEnd::ConnectionLost => warn!(
                        channel = %config.channel,
                        "gcp-pubsub subscriber pull failed — reconnecting"
                    ),
                }
            }
        }
    }
}

/// Build the client and resolve the subscription handle. Creating the client CONNECTS
/// (gRPC channel pool), so an unreachable endpoint fails closed here (non-fatal
/// upstream).
async fn open_session(config: &GcpPubSubSourceConfig) -> Result<Subscription, Diagnostic> {
    let props = &config.properties;
    let client_config = build_client_config(&props.project_id, props.endpoint_override.as_deref());
    let client = Client::new(client_config).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONNECTION_FAILED,
            format!(
                "gcp-pubsub channel '{}' could not build a client for project '{}': {e}",
                config.channel, props.project_id
            ),
        )
    })?;
    Ok(client.subscription(&props.subscription))
}

/// The pull loop: gate re-check per turn (and on an idle tick), pull batch → per-message
/// projection → intake → native settle.
async fn run_session(
    config: &GcpPubSubSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopToken>,
    subscription: &Subscription,
) -> SessionEnd {
    loop {
        if stop.is_requested() {
            return SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            return SessionEnd::LeadershipLost;
        }
        // `pull` blocks until at least one message is available; race it against the gate
        // poll and the stop signal so leadership loss / shutdown are honoured promptly
        // (an abandoned pull redelivers — the messages were never acked).
        let pulled = tokio::select! {
            _ = stop.notify.notified() => return SessionEnd::Stopped,
            _ = tokio::time::sleep(config.gate_poll) => continue, // idle gate re-check
            result = subscription.pull(config.pull_max, None) => result,
        };
        let messages = match pulled {
            Ok(m) => m,
            Err(e) => {
                warn!(channel = %config.channel, error = %e, "gcp-pubsub pull failed");
                return SessionEnd::ConnectionLost;
            }
        };
        for message in messages {
            if stop.is_requested() {
                return SessionEnd::Stopped; // un-settled messages redeliver
            }
            // `Arc` so the `on-complete` settle callbacks can outlive this loop turn (the
            // deferred half); the on-persist path just derefs it.
            let message = Arc::new(message);
            let inbound = to_inbound_message(config, &message);
            // Per-message inbound auth: a rejected credential drops the message
            // natively (Pub/Sub `ack` — no redelivery, matching the poison-drop path) and
            // NEVER dispatches.
            if let Some(auth) = &config.inbound_auth {
                if auth.verify(&inbound.headers) == InboundVerdict::Reject {
                    warn!(
                        channel = %config.channel,
                        code = codes::INBOUND_AUTH_REJECTED,
                        "gcp-pubsub channel rejected a message — credential did not match expected"
                    );
                    settle(config, &message, AckDecision::NackDrop).await;
                    continue;
                }
            }
            // The intake owns ack-mode TIMING: `on-persist` awaits the decision and
            // settles at dispatch-return; `on-complete` hands the engine per-delivery
            // settle callbacks — a PARKED instance defers the Pub/Sub ack to its terminal
            // event (the deferred-ack registry), a run-to-completion dispatch settles now
            // exactly like on-persist.
            let decision = if config.properties.ack_mode == AckMode::OnComplete {
                let settle_callbacks =
                    deferred_settle(&config.channel, &message, tokio::runtime::Handle::current());
                match intake.deliver_deferred(inbound, settle_callbacks).await {
                    DeliveryDisposition::Deferred => {
                        tracing::debug!(
                            channel = %config.channel,
                            "delivery deferred — the Pub/Sub ack is held until the instance's \
                             terminal event (valid only while the ack deadline holds)"
                        );
                        continue;
                    }
                    DeliveryDisposition::Settle(decision) => decision,
                }
            } else {
                intake.deliver(inbound).await
            };
            settle(config, &message, decision).await;
        }
    }
}

/// Execute an [`AckDecision`] on the message — `Ack`/`NackDrop` ack (own / drop);
/// `NackRequeue` nacks so Pub/Sub redelivers after the ack deadline. Settle failures are
/// non-fatal (the message redelivers under the ack deadline regardless).
async fn settle(config: &GcpPubSubSourceConfig, message: &ReceivedMessage, decision: AckDecision) {
    let outcome = match decision {
        // NackDrop acks too: Pub/Sub has no default DLQ, so acking IS the drop (the poison
        // never redelivers).
        AckDecision::Ack | AckDecision::NackDrop => message.ack().await,
        AckDecision::NackRequeue => message.nack().await,
    };
    if let Err(e) = outcome {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_RECEIVE_FAILED,
            error = %e,
            "gcp-pubsub settle failed — message will redeliver under the ack deadline"
        );
    }
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete` — the deferred half
/// of the ack mapping. BOTH callbacks are `message.ack()`, because that is exactly what the
/// settle-at-return path already does for `Ack` and `NackDrop` alike: Pub/Sub has no
/// default dead-letter hand-off (a dead-letter topic is delivery-attempt driven, not reject
/// driven), so acking IS the drop posture — the poison never redelivers. Registry
/// timeout/overflow nacks share it, freeing the lease.
///
/// The callbacks fire on the engine actor thread or the sweep task — non-async contexts —
/// so each spawns the Pub/Sub op onto the runtime captured here. [`ReceivedMessage`] is
/// `Send + Sync` (an ack id + subscription name + a cheap-clone gRPC subscriber client), so
/// an `Arc` of it simply moves into the closures — pinned by
/// `received_message_is_send_and_sync_so_the_deferred_settle_compiles`.
///
/// LIFETIME CAVEAT (see the module docs): the held ack is only meaningful while the
/// subscription's ack deadline still leases this delivery. There is NO lease-extension
/// heartbeat on the unary `pull` path, so past that window Pub/Sub has already redelivered
/// and the held ack on the stale ack id is a best-effort WARN no-op.
fn deferred_settle(
    channel: &str,
    message: &Arc<ReceivedMessage>,
    runtime: tokio::runtime::Handle,
) -> DeferredSettle {
    fn settle_callback(
        channel: &str,
        message: &Arc<ReceivedMessage>,
        runtime: &tokio::runtime::Handle,
        label: &'static str,
    ) -> Box<dyn FnMut() + Send> {
        let channel = channel.to_string();
        let message = Arc::clone(message);
        let runtime = runtime.clone();
        Box::new(move || {
            let channel = channel.clone();
            let message = Arc::clone(&message);
            runtime.spawn(async move {
                // A lapsed lease / torn-down session fails here: Pub/Sub has already
                // returned the message for redelivery, and inbox dedup absorbs it.
                if let Err(e) = message.ack().await {
                    warn!(
                        channel = %channel,
                        code = codes::INBOUND_RECEIVE_FAILED,
                        error = %e,
                        "deferred {label} failed — the message redelivers under the ack \
                         deadline (inbox dedup absorbs the duplicate)"
                    );
                }
            });
        })
    }
    DeferredSettle {
        ack: settle_callback(channel, message, &runtime, "ack (instance completed)"),
        nack: settle_callback(
            channel,
            message,
            &runtime,
            "ack (instance failed — drop posture; Pub/Sub has no default DLQ)",
        ),
    }
}

/// Project one Pub/Sub message into the engine's [`InboundMessage`].
fn to_inbound_message(config: &GcpPubSubSourceConfig, message: &ReceivedMessage) -> InboundMessage {
    let m = &message.message;
    project_inbound(
        config,
        attributes_to_headers(&m.attributes),
        &m.message_id,
        &m.ordering_key,
        m.data.clone(),
    )
}

/// The pure message→[`InboundMessage`] projection (the broker-free core, unit-tested): the
/// `sutra-outbox-key` attribute is the EXPLICIT idempotency key when set; otherwise the
/// broker-assigned `message_id` (NON-explicit); `content-type` attribute → content type
/// (default `application/octet-stream`); the message id + ordering key ride as informational
/// `x-gcp-pubsub-*` headers.
fn project_inbound(
    config: &GcpPubSubSourceConfig,
    attributes: BTreeMap<String, String>,
    message_id: &str,
    ordering_key: &str,
    body: Vec<u8>,
) -> InboundMessage {
    let mut headers = attributes;
    let outbox_key = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(ATTR_OUTBOX_KEY))
        .map(|(_, v)| v.trim().to_string())
        .filter(|s| !s.is_empty());
    let explicit_event_id = outbox_key.is_some();
    let idempotency_key = outbox_key.unwrap_or_else(|| message_id.to_string());
    let content_type = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(ATTR_CONTENT_TYPE))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    if !message_id.is_empty() {
        headers.insert(HEADER_MESSAGE_ID.to_string(), message_id.to_string());
    }
    if !ordering_key.is_empty() {
        headers.insert(HEADER_ORDERING_KEY.to_string(), ordering_key.to_string());
    }
    InboundMessage {
        tenant: config.tenant.clone(),
        module_key: config.module_key.clone(),
        channel: config.channel.clone(),
        headers,
        body: body.into(),
        content_type: Some(content_type),
        idempotency_key,
        explicit_event_id,
        received_at: now_rfc3339(),
        cloud_event: None,
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::sink::BoxFuture;

    fn base_properties() -> GcpPubSubChannelProperties {
        GcpPubSubChannelProperties {
            project_id: "acme-payments".to_string(),
            subscription: String::new(),
            topic: String::new(),
            max_outstanding_messages: GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_MESSAGES,
            max_outstanding_request_bytes:
                GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES,
            endpoint_override: None,
            ack_mode: super::super::AckMode::OnPersist,
            singleton: false,
        }
    }

    fn config() -> GcpPubSubSourceConfig {
        let mut props = base_properties();
        props.subscription = "transfer-sub".to_string();
        GcpPubSubSourceConfig::new("acme", "acme/payments/1.0.0", "transfer-sub", props)
    }

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn outbox_key_attribute_becomes_the_explicit_idempotency_key() {
        let m = project_inbound(
            &config(),
            attrs(&[
                ("sutra-outbox-key", "ob-7"),
                ("content-type", "application/xml"),
            ]),
            "srv-msg-1",
            "",
            b"<Document/>".to_vec(),
        );
        assert_eq!(m.idempotency_key, "ob-7");
        assert!(m.explicit_event_id);
        assert_eq!(m.body.into_inner(), b"<Document/>");
        assert_eq!(m.content_type.as_deref(), Some("application/xml"));
        assert_eq!(
            m.headers.get("sutra-outbox-key").map(String::as_str),
            Some("ob-7")
        );
        assert_eq!(
            m.headers.get("x-gcp-pubsub-message-id").map(String::as_str),
            Some("srv-msg-1")
        );
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.channel, "transfer-sub");
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn idempotency_key_falls_back_to_message_id() {
        let m = project_inbound(&config(), attrs(&[]), "srv-msg-42", "", b"x".to_vec());
        assert_eq!(m.idempotency_key, "srv-msg-42");
        assert!(!m.explicit_event_id, "the fallback key is non-explicit");
    }

    #[test]
    fn blank_outbox_key_attribute_falls_back() {
        let m = project_inbound(
            &config(),
            attrs(&[("sutra-outbox-key", "   ")]),
            "srv-msg-9",
            "",
            Vec::new(),
        );
        assert_eq!(m.idempotency_key, "srv-msg-9");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn content_type_defaults_to_octet_stream_and_ordering_key_rides() {
        let m = project_inbound(&config(), attrs(&[]), "id", "order-3", Vec::new());
        assert_eq!(m.content_type.as_deref(), Some("application/octet-stream"));
        assert_eq!(
            m.headers
                .get("x-gcp-pubsub-ordering-key")
                .map(String::as_str),
            Some("order-3")
        );
    }

    #[test]
    fn source_without_subscription_fails_closed() {
        let cfg = GcpPubSubSourceConfig::new("acme", "acme/m/1", "ch", base_properties());
        let err = match GcpPubSubTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a subscription must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_SUBSCRIPTION_MISSING);
    }

    /// The threading precondition of the `on-complete` port: the deferred settle callbacks
    /// must be `Box<dyn FnMut() + Send>`, which only holds if an `Arc<ReceivedMessage>` can
    /// move into them — i.e. if the client library's message type is `Send + Sync`. A
    /// future client bump that adds an `Rc`/`Cell` to `ReceivedMessage` breaks HERE, with a
    /// named test, instead of deep inside `deferred_settle`.
    #[test]
    fn received_message_is_send_and_sync_so_the_deferred_settle_compiles() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReceivedMessage>();
        assert_send_sync::<Arc<ReceivedMessage>>();
    }

    #[tokio::test]
    async fn stop_before_start_is_idempotent_and_start_resolves_without_a_broker() {
        // Points at a port nobody listens on — start MUST still resolve Ok (broker
        // absence is non-fatal, the supervisor retries in the background).
        let mut cfg = config();
        cfg.properties.endpoint_override = Some("127.0.0.1:1".to_string()); // reserved, never a broker
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        cfg.gate_poll = Duration::from_millis(20);
        let source = GcpPubSubTriggerSource::new(cfg).expect("source");

        struct NoIntake;
        impl InboundIntake for NoIntake {
            fn deliver(&self, _m: InboundMessage) -> BoxFuture<'_, AckDecision> {
                Box::pin(async { AckDecision::Ack })
            }
        }

        source.stop().await.expect("stop before start");
        source
            .start(
                Arc::new(NoIntake),
                Arc::new(sutra_channels::source::AlwaysLeading),
            )
            .await
            .expect("start resolves despite broker absence");
        tokio::time::sleep(Duration::from_millis(60)).await;
        source.stop().await.expect("stop");
        source.stop().await.expect("second stop is a no-op");
    }
}
