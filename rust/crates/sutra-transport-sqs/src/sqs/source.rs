//! The AWS SQS inbound trigger — [`SqsTriggerSource`] implements [`TriggerSource`]: one
//! long-poll loop per channel binding, each received message projected into an
//! [`InboundMessage`] and pushed through the [`InboundIntake`] seam, the returned
//! [`AckDecision`] executed as an SQS-native settle (delete / leave-for-redelivery).
//!
//! The AWS SQS trigger-source lifecycle:
//!
//! - **Broker absence is NON-FATAL**: `start` spawns a supervisor task and resolves;
//!   a failing ReceiveMessage WARNs (`SUTRA.INBOUND.AWS_SQS.RECEIVE_FAILED`) and the
//!   supervisor retries with exponential backoff in the background — readiness is unaffected.
//! - **Ack timing rides the intake seam**: the source `await`s
//!   [`InboundIntake::deliver`] and maps the decision onto SQS's settle model — `Ack` →
//!   `DeleteMessage` (the delivery is durable), `NackDrop` → `DeleteMessage` (SQS has no
//!   default DLQ, so deleting IS the drop), `NackRequeue` → leave the message in flight so
//!   the visibility timeout redelivers it (inbox dedup absorbs the duplicate).
//! - **`ack-mode: on-complete`**: the source instead calls
//!   [`InboundIntake::deliver_deferred`], handing the engine per-delivery settle callbacks
//!   closed over the receipt handle + a clone of the SQS [`Client`]. A PARKED instance
//!   answers [`DeliveryDisposition::Deferred`] — the source does NOT settle; the
//!   deferred-ack registry fires the held `DeleteMessage` at the instance's terminal event.
//!   A dispatch that ran to completion answers [`DeliveryDisposition::Settle`] and settles
//!   at return exactly like `on-persist`.
//! - **Singleton gating**: the loop long-polls ONLY while `gate.is_leading()`; on
//!   leadership loss it stops receiving, then resumes when the gate returns. The engine
//!   per-channel lease — not SQS — is what makes a `singleton: true` channel consume on
//!   exactly one replica.
//!
//! Idempotency key: the FIFO `MessageDeduplicationId` when present, else the
//! `sutra-outbox-key` message attribute (`explicit_event_id = true`), else SQS's own
//! `MessageId` (NON-explicit, so it never suppresses a re-post through inbox dedup).
//!
//! ## Visibility-timeout caveat for `on-complete` (OPERATOR GUIDANCE)
//!
//! Unlike RabbitMQ — where an unacked delivery is held for as long as the consumer's
//! channel lives — an SQS message is only "in flight" for the queue's **visibility
//! timeout** (`visibility-timeout-seconds`, max 12 h). A deferred settle is therefore only
//! valid *while that window holds*. Three consequences the operator must size for:
//!
//! 1. **Redelivery while parked.** If the instance is still parked when the visibility
//!    timeout lapses, SQS makes the message visible again and this very source receives it
//!    a second time — a *concurrent* delivery alongside the parked original. The
//!    idempotency key is stable across redeliveries (FIFO dedup id / `sutra-outbox-key` /
//!    the SQS `MessageId`), so **inbox dedup is what absorbs it** — the second delivery
//!    resolves as a DUPLICATE and settles immediately. This is the at-least-once contract,
//!    not a defect, but it means `on-complete` on SQS does NOT hold a message indefinitely.
//! 2. **Late settle is a best-effort no-op.** Once the message has been re-received (new
//!    receipt handle) or deleted by the duplicate's settle, the held `DeleteMessage` on the
//!    ORIGINAL receipt handle no longer identifies a live delivery: it either succeeds
//!    vacuously or fails, and either way it is a WARN, never fatal.
//! 3. **Sizing rule.** `visibility-timeout-seconds` ≥ the expected worst-case park
//!    duration, and `sutra.ack.deferred.timeout` (the registry sweep) ≤ that visibility
//!    timeout — so the registry nacks (drops) a stuck instance *before* SQS starts
//!    redelivering it. There is no lease-extension heartbeat: this source does NOT call
//!    `ChangeMessageVisibility` while an instance is parked.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName};
use aws_sdk_sqs::Client;
use tracing::{info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{
    codes, AckMode, SqsChannelProperties, HEADER_CONTENT_TYPE, HEADER_OUTBOX_KEY, TRANSPORT,
};

/// Build an SQS client for `region` with the static-credentials provider (from
/// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, LocalStack-friendly
/// `test`/`test` defaults) and the ring rustls TLS provider. `endpoint` overrides the SDK
/// endpoint URL (LocalStack). Constructing the client does not open a connection.
pub fn build_client(region: &str, endpoint: Option<&str>) -> Client {
    use aws_config::BehaviorVersion;
    use aws_sdk_sqs::config::Region;

    let access = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "test".to_string());
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "test".to_string());
    let session = std::env::var("AWS_SESSION_TOKEN").ok();
    let credentials = Credentials::new(access, secret, session, None, "sutra-static-credentials");

    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https();

    let mut builder = aws_sdk_sqs::config::Builder::default()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .credentials_provider(credentials)
        .http_client(http);
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    Client::from_conf(builder.build())
}

/// Everything one long-poll loop needs, prepared by the wiring.
#[derive(Debug, Clone)]
pub struct SqsSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Region/queue/wait/ack-mode properties.
    pub properties: SqsChannelProperties,
    /// Leadership re-check cadence while gated out.
    pub gate_poll: Duration,
    /// Reconnect backoff floor (doubles per failure up to [`Self::reconnect_max`]).
    pub reconnect_min: Duration,
    /// Reconnect backoff ceiling.
    pub reconnect_max: Duration,
    /// Per-message inbound auth (`inbound-auth.*`), resolved once at wiring. `None` =
    /// no inbound-auth declared. A rejected credential drops the message (SQS DeleteMessage).
    pub inbound_auth: Option<BrokerInboundAuth>,
}

impl SqsSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff.
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: SqsChannelProperties,
    ) -> SqsSourceConfig {
        SqsSourceConfig {
            tenant: tenant.to_string(),
            module_key: module_key.to_string(),
            channel: channel.to_string(),
            properties,
            gate_poll: Duration::from_secs(1),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            inbound_auth: None,
        }
    }
}

/// One SQS long-poll loop serving one channel binding (the singleton unit).
pub struct SqsTriggerSource {
    config: SqsSourceConfig,
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

/// Why one poll session ended.
enum SessionEnd {
    Stopped,
    LeadershipLost,
    ReceiveFailed,
    QueueGone,
}

impl SqsTriggerSource {
    /// A source for one channel binding. `config.properties` must carry a queue URL
    /// ([`codes::INBOUND_QUEUE_MISSING`]) and a region ([`codes::INBOUND_CONFIG_INVALID`])
    /// — fail-closed at wiring time.
    pub fn new(config: SqsSourceConfig) -> Result<SqsTriggerSource, Diagnostic> {
        if !config.properties.has_queue_url() {
            return Err(Diagnostic::error(
                codes::INBOUND_QUEUE_MISSING,
                format!(
                    "aws-sqs channel '{}' requires property 'queue.url'",
                    config.channel
                ),
            ));
        }
        if config.properties.region.trim().is_empty() {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "aws-sqs channel '{}' requires property 'region'",
                    config.channel
                ),
            ));
        }
        Ok(SqsTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured queue URL (diagnostics / tests).
    pub fn queue_url(&self) -> &str {
        &self.config.properties.queue_url
    }
}

impl TriggerSource for SqsTriggerSource {
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
                        "aws-sqs channel declared inbound-auth.scheme=mtls but per-channel mTLS \
                         is not supported — falling back to transport-level TLS"
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
                    "aws-sqs source supervisor did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The poll supervisor: leadership-gated receive → settle loop with exponential reconnect
/// backoff. Broker/queue absence never escapes as an error.
async fn supervise(
    config: SqsSourceConfig,
    intake: Arc<dyn InboundIntake>,
    gate: Arc<dyn LeaderGate>,
    stop: Arc<StopToken>,
) {
    let client = build_client(
        &config.properties.region,
        config.properties.endpoint_override.as_deref(),
    );
    let mut backoff = config.reconnect_min;
    loop {
        if stop.is_requested() {
            return;
        }
        if !gate.is_leading() {
            if stop.sleep(config.gate_poll).await {
                return;
            }
            continue;
        }
        info!(
            channel = %config.channel,
            queue = %config.properties.queue_url,
            "aws-sqs consumer polling"
        );
        match run_session(&config, &intake, &gate, &stop, &client).await {
            SessionEnd::Stopped => return,
            SessionEnd::LeadershipLost => {
                info!(channel = %config.channel, "aws-sqs consumer paused — leadership lost");
                backoff = config.reconnect_min;
            }
            SessionEnd::QueueGone => {
                // The queue does not exist — a NON-FATAL background WARN + retry (the queue
                // may be provisioned out-of-band); readiness is unaffected.
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_QUEUE_MISSING,
                    "aws-sqs queue not found — retrying in {:?}",
                    backoff
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
            SessionEnd::ReceiveFailed => {
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_RECEIVE_FAILED,
                    "aws-sqs receiveMessage failed — retrying in {:?}",
                    backoff
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
        }
    }
}

/// The receive loop: gate re-check per turn, long-poll receive → projection → intake →
/// SQS-native settle. Returns the reason the session ended.
async fn run_session(
    config: &SqsSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopToken>,
    client: &Client,
) -> SessionEnd {
    let props = &config.properties;
    loop {
        if stop.is_requested() {
            return SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            return SessionEnd::LeadershipLost;
        }
        let receive = client
            .receive_message()
            .queue_url(&props.queue_url)
            .wait_time_seconds(props.wait_time_seconds)
            .max_number_of_messages(props.max_messages)
            .visibility_timeout(props.visibility_timeout_seconds)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .send();
        let response = tokio::select! {
            _ = stop.notify.notified() => return SessionEnd::Stopped,
            response = receive => response,
        };
        let response = match response {
            Ok(r) => r,
            Err(error) => {
                if is_queue_gone(&error) {
                    return SessionEnd::QueueGone;
                }
                warn!(
                    channel = %config.channel,
                    error = %error,
                    "aws-sqs receiveMessage failed"
                );
                return SessionEnd::ReceiveFailed;
            }
        };
        for message in response.messages() {
            let projected = project_message(config, message);
            // Per-message inbound auth: a rejected credential drops the message
            // natively (SQS DeleteMessage — no visibility-timeout re-appear) and NEVER
            // dispatches.
            if let Some(auth) = &config.inbound_auth {
                if auth.verify(&projected.headers) == InboundVerdict::Reject {
                    warn!(
                        channel = %config.channel,
                        code = codes::INBOUND_AUTH_REJECTED,
                        "aws-sqs channel rejected a message — credential did not match expected"
                    );
                    settle(config, client, message, AckDecision::NackDrop).await;
                    continue;
                }
            }
            // The intake owns ack-mode TIMING: `on-persist` awaits the decision and
            // settles at dispatch-return; `on-complete` hands the engine per-delivery
            // settle callbacks — a PARKED instance defers the DeleteMessage to its terminal
            // event (the deferred-ack registry), a run-to-completion dispatch settles now
            // exactly like on-persist.
            let decision = if props.ack_mode == AckMode::OnComplete {
                let settle_callbacks = deferred_settle(
                    &config.channel,
                    client,
                    &props.queue_url,
                    message.receipt_handle().unwrap_or_default(),
                    tokio::runtime::Handle::current(),
                );
                match intake.deliver_deferred(projected, settle_callbacks).await {
                    DeliveryDisposition::Deferred => {
                        tracing::debug!(
                            channel = %config.channel,
                            "delivery deferred — the SQS delete is held until the instance's \
                             terminal event (valid only while the visibility timeout holds)"
                        );
                        continue;
                    }
                    DeliveryDisposition::Settle(decision) => decision,
                }
            } else {
                intake.deliver(projected).await
            };
            settle(config, client, message, decision).await;
        }
    }
}

/// Execute an [`AckDecision`] as an SQS settle. `Ack`/`NackDrop` delete the message
/// (deleting IS the drop — SQS has no default DLQ); `NackRequeue` leaves it in flight so
/// the visibility timeout redelivers it.
async fn settle(
    config: &SqsSourceConfig,
    client: &Client,
    message: &Message,
    decision: AckDecision,
) {
    match decision {
        AckDecision::Ack | AckDecision::NackDrop => {
            let Some(receipt) = message.receipt_handle() else {
                return;
            };
            delete_message(
                &config.channel,
                client,
                &config.properties.queue_url,
                receipt,
                "deleteMessage",
            )
            .await;
        }
        AckDecision::NackRequeue => {
            // Leave in flight — visibility-timeout redelivery is the SQS-native
            // at-least-once contract (inbox dedup absorbs the duplicate).
        }
    }
}

/// `DeleteMessage` by receipt handle — the one SQS settle verb this transport issues (both
/// `Ack` and `NackDrop` map onto it). Failure is a WARN, never fatal: the message simply
/// stays in flight until the visibility timeout returns it, and inbox dedup absorbs the
/// redelivery. `label` names the caller for the log line.
async fn delete_message(
    channel: &str,
    client: &Client,
    queue_url: &str,
    receipt: &str,
    label: &str,
) {
    if receipt.is_empty() {
        warn!(
            channel = %channel,
            "aws-sqs {label} skipped — the delivery carried no receipt handle"
        );
        return;
    }
    if let Err(error) = client
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt)
        .send()
        .await
    {
        warn!(
            channel = %channel,
            error = %error,
            "aws-sqs {label} failed — leaving in flight for redelivery"
        );
    }
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete` — the deferred half
/// of the ack mapping. BOTH callbacks are a `DeleteMessage` on the captured receipt handle,
/// because that is exactly what the settle-at-return path already does for `Ack` and
/// `NackDrop` alike: SQS has no immediate dead-letter hand-off (its redrive policy is
/// receive-count driven, not reject driven), so deleting IS the drop posture. Registry
/// timeout/overflow nacks share it — the delivery is consumed rather than left to churn.
///
/// The callbacks fire on the engine actor thread or the sweep task — non-async contexts —
/// so each spawns the SQS op onto the runtime captured here. The SDK [`Client`] is a
/// cheap-clone `Send + Sync` handle, so it (and the receipt handle string) simply move into
/// the closures; nothing borrows the [`Message`], which is long gone by then.
///
/// LIFETIME CAVEAT (see the module docs): a deferred delete is only meaningful while the
/// queue's visibility timeout still holds this delivery in flight. Past that window SQS has
/// already redelivered the message under a NEW receipt handle, and the held delete on the
/// old one is a best-effort WARN no-op.
fn deferred_settle(
    channel: &str,
    client: &Client,
    queue_url: &str,
    receipt: &str,
    runtime: tokio::runtime::Handle,
) -> DeferredSettle {
    fn settle_callback(
        channel: &str,
        client: &Client,
        queue_url: &str,
        receipt: &str,
        runtime: &tokio::runtime::Handle,
        label: &'static str,
    ) -> Box<dyn FnMut() + Send> {
        let channel = channel.to_string();
        let client = client.clone();
        let queue_url = queue_url.to_string();
        let receipt = receipt.to_string();
        let runtime = runtime.clone();
        Box::new(move || {
            let channel = channel.clone();
            let client = client.clone();
            let queue_url = queue_url.clone();
            let receipt = receipt.clone();
            runtime.spawn(async move {
                delete_message(&channel, &client, &queue_url, &receipt, label).await;
            });
        })
    }
    DeferredSettle {
        ack: settle_callback(
            channel,
            client,
            queue_url,
            receipt,
            &runtime,
            "deferred deleteMessage (instance completed)",
        ),
        nack: settle_callback(
            channel,
            client,
            queue_url,
            receipt,
            &runtime,
            "deferred deleteMessage (instance failed — drop posture)",
        ),
    }
}

/// A ReceiveMessage error is a "queue gone" only when its underlying cause is
/// `QueueDoesNotExist` (matched on the error string — the SDK's typed variant is not
/// surfaced on the operation error).
fn is_queue_gone<E: std::fmt::Debug>(error: &aws_sdk_sqs::error::SdkError<E>) -> bool {
    format!("{error:?}").contains("QueueDoesNotExist")
}

/// Project one SQS [`Message`] into the engine's [`InboundMessage`].
fn project_message(config: &SqsSourceConfig, message: &Message) -> InboundMessage {
    let mut attributes = BTreeMap::new();
    if let Some(attrs) = message.message_attributes() {
        for (name, value) in attrs {
            attributes.insert(name.clone(), attribute_string(value));
        }
    }
    let dedup_id = message
        .attributes()
        .and_then(|a| a.get(&MessageSystemAttributeName::MessageDeduplicationId))
        .cloned();
    let message_id = message.message_id().map(str::to_string);
    let body = message
        .body()
        .map(|b| b.as_bytes().to_vec())
        .unwrap_or_default();
    project_inbound(config, attributes, dedup_id, message_id, body)
}

/// The pure attributes→[`InboundMessage`] projection (the broker-free core, unit-tested):
/// the idempotency key is the FIFO dedup id, else the `sutra-outbox-key` attribute
/// (EXPLICIT), else SQS's `MessageId` (NON-explicit); `content-type` attribute → content
/// type (default `application/octet-stream`).
fn project_inbound(
    config: &SqsSourceConfig,
    headers: BTreeMap<String, String>,
    dedup_id: Option<String>,
    message_id: Option<String>,
    body: Vec<u8>,
) -> InboundMessage {
    let dedup = dedup_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let outbox_key = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(HEADER_OUTBOX_KEY))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let explicit_event_id = dedup.is_some() || outbox_key.is_some();
    let idempotency_key = dedup.or(outbox_key).or(message_id).unwrap_or_default();
    let content_type = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(HEADER_CONTENT_TYPE))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
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

/// The string view of an SQS message attribute (string value, else UTF-8 of the binary
/// value, else empty).
fn attribute_string(value: &aws_sdk_sqs::types::MessageAttributeValue) -> String {
    if let Some(s) = value.string_value() {
        return s.to_string();
    }
    if let Some(blob) = value.binary_value() {
        return String::from_utf8_lossy(blob.as_ref()).into_owned();
    }
    String::new()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqs::AckMode;

    fn base_properties() -> SqsChannelProperties {
        SqsChannelProperties {
            region: "us-east-1".to_string(),
            queue_url: "https://sqs.us-east-1.amazonaws.com/000000000000/transfer".to_string(),
            wait_time_seconds: 10,
            max_messages: 10,
            visibility_timeout_seconds: 30,
            endpoint_override: None,
            ack_mode: AckMode::OnPersist,
            singleton: false,
        }
    }

    fn config() -> SqsSourceConfig {
        SqsSourceConfig::new(
            "acme",
            "acme/payments/1.0.0",
            "transfer-queue",
            base_properties(),
        )
    }

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn outbox_key_attribute_becomes_the_explicit_idempotency_key() {
        let m = project_inbound(
            &config(),
            headers(&[
                ("sutra-outbox-key", "order-1"),
                ("content-type", "application/xml"),
                ("x-tenant", "acme"),
            ]),
            None,
            Some("sqs-msg-id".to_string()),
            b"<Document/>".to_vec(),
        );
        assert_eq!(m.idempotency_key, "order-1");
        assert!(m.explicit_event_id);
        assert_eq!(m.body.into_inner(), b"<Document/>");
        assert_eq!(m.content_type.as_deref(), Some("application/xml"));
        assert_eq!(m.headers.get("x-tenant").map(String::as_str), Some("acme"));
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.channel, "transfer-queue");
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn fifo_dedup_id_wins_over_outbox_key() {
        let m = project_inbound(
            &config(),
            headers(&[("sutra-outbox-key", "ob-7")]),
            Some("dedup-42".to_string()),
            Some("sqs-id".to_string()),
            b"x".to_vec(),
        );
        assert_eq!(m.idempotency_key, "dedup-42");
        assert!(m.explicit_event_id);
    }

    #[test]
    fn idempotency_key_falls_back_to_message_id_non_explicit() {
        let m = project_inbound(
            &config(),
            headers(&[("content-type", "text/plain")]),
            None,
            Some("sqs-message-id-1".to_string()),
            b"no-key".to_vec(),
        );
        assert_eq!(m.idempotency_key, "sqs-message-id-1");
        assert!(
            !m.explicit_event_id,
            "the MessageId fallback is non-explicit"
        );
    }

    #[test]
    fn blank_outbox_key_attribute_falls_back() {
        let m = project_inbound(
            &config(),
            headers(&[("sutra-outbox-key", "   ")]),
            None,
            Some("mid".to_string()),
            Vec::new(),
        );
        assert_eq!(m.idempotency_key, "mid");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn content_type_defaults_to_octet_stream() {
        let m = project_inbound(
            &config(),
            headers(&[]),
            None,
            Some("mid".to_string()),
            Vec::new(),
        );
        assert_eq!(m.content_type.as_deref(), Some("application/octet-stream"));
    }

    #[test]
    fn source_without_queue_url_fails_closed() {
        let mut props = base_properties();
        props.queue_url = String::new();
        let cfg = SqsSourceConfig::new("acme", "acme/m/1", "ch", props);
        let err = match SqsTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a queue URL must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_QUEUE_MISSING);
    }

    #[test]
    fn source_without_region_fails_closed() {
        let mut props = base_properties();
        props.region = String::new();
        let cfg = SqsSourceConfig::new("acme", "acme/m/1", "ch", props);
        let err = match SqsTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a region must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[tokio::test]
    async fn deferred_settle_callbacks_are_send_and_survive_a_dead_endpoint() {
        // The settle-after-session-death posture: both held callbacks spawn a DeleteMessage
        // onto the captured runtime; against an endpoint nobody serves the SDK error is
        // swallowed as a WARN. Firing them must never panic or block the caller (they run
        // on the engine actor thread / the registry sweep task).
        let client = build_client("us-east-1", Some("http://127.0.0.1:1"));
        let mut settle = deferred_settle(
            "payments-inbound",
            &client,
            "https://sqs.us-east-1.amazonaws.com/000000000000/transfer",
            "receipt-handle-1",
            tokio::runtime::Handle::current(),
        );
        // `DeferredSettle` demands `Box<dyn FnMut() + Send>` — this only compiles because
        // the SDK client + receipt handle are Send and MOVE into the closures.
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&settle.ack);
        assert_send(&settle.nack);
        (settle.ack)();
        (settle.nack)();
        // Idempotent re-fire (the registry never double-fires, but the contract allows it).
        (settle.ack)();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    #[tokio::test]
    async fn deferred_settle_without_a_receipt_handle_is_a_warn_no_op() {
        let client = build_client("us-east-1", Some("http://127.0.0.1:1"));
        let mut settle = deferred_settle(
            "payments-inbound",
            &client,
            "https://sqs.us-east-1.amazonaws.com/000000000000/transfer",
            "", // Message::receipt_handle() was None
            tokio::runtime::Handle::current(),
        );
        (settle.ack)();
        (settle.nack)();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn stop_before_start_is_idempotent_and_start_resolves_without_a_broker() {
        // Points at a LocalStack-style endpoint nobody listens on — start MUST still
        // resolve Ok (broker absence is non-fatal, the supervisor retries in the
        // background).
        let mut props = base_properties();
        props.endpoint_override = Some("http://127.0.0.1:1".to_string()); // reserved, never a broker
        let mut cfg = SqsSourceConfig::new("acme", "acme/payments/1.0.0", "transfer-queue", props);
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        cfg.gate_poll = Duration::from_millis(20);
        let source = SqsTriggerSource::new(cfg).expect("source");

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
