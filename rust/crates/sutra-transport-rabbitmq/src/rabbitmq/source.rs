//! The RabbitMQ inbound trigger — [`RabbitMqTriggerSource`] implements
//! [`TriggerSource`]: one AMQP consumer per channel binding, deliveries projected into
//! [`InboundMessage`]s and pushed through the [`InboundIntake`] seam, the returned
//! [`AckDecision`] executed on the broker (`basic.ack` / `basic.nack`).
//!
//! The RabbitMQ trigger-source lifecycle:
//!
//! - **Broker absence is NON-FATAL**: `start` spawns a supervisor task and
//!   resolves; a missing broker logs a WARN (`SUTRA.INBOUND.RABBITMQ.CONNECTION_FAILED`)
//!   and the supervisor keeps retrying with exponential backoff in the background —
//!   readiness is unaffected.
//! - **Ack timing rides the intake seam**: the source `await`s
//!   [`InboundIntake::deliver`] and maps the decision 1:1 — `Ack` → `basic.ack`,
//!   `NackRequeue` → `basic.nack(requeue=true)`, `NackDrop` → `basic.nack(requeue=false)`
//!   (the broker's DLX posture, exactly the consumer's `requeue=false` reject). The
//!   intake adapter owns WHEN the future resolves (`on-persist` after durable intake,
//!   `on-complete` at the instance's terminal event).
//! - **Singleton gating**: the consumer subscribes ONLY while `gate.is_leading()` and
//!   re-checks the gate every delivery-loop turn plus on an idle tick; on leadership
//!   loss it cancels the consumer and closes the connection (so the queue's
//!   `consumerCount` stays 1 across replicas), then re-subscribes when the gate returns.
//!
//! The queue is declared PASSIVELY (queues pre-created by the examples); a missing queue
//! is retried like broker absence (`SUTRA.INBOUND.RABBITMQ.QUEUE_MISSING` WARN).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use tracing::{info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{codes, stringify_field, AckMode, RabbitMqChannelProperties, TRANSPORT};

/// Everything one consumer needs, prepared by the wiring (credentials RESOLVED).
#[derive(Debug, Clone)]
pub struct RabbitMqSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Broker/queue/ack-mode properties (credentials already resolved).
    pub properties: RabbitMqChannelProperties,
    /// Per-message inbound auth (`inbound-auth.*`); `None` when unconfigured. The
    /// engine wiring resolves the expected-key ref and stamps this before `start`.
    pub inbound_auth: Option<BrokerInboundAuth>,
    /// Leadership re-check cadence while idle / between reconnect checks.
    pub gate_poll: Duration,
    /// Reconnect backoff floor (doubles per failure up to [`Self::reconnect_max`]).
    pub reconnect_min: Duration,
    /// Reconnect backoff ceiling.
    pub reconnect_max: Duration,
}

impl RabbitMqSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff (the client's
    /// fixed 5s recovery interval sits inside this envelope).
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: RabbitMqChannelProperties,
    ) -> RabbitMqSourceConfig {
        RabbitMqSourceConfig {
            tenant: tenant.to_string(),
            module_key: module_key.to_string(),
            channel: channel.to_string(),
            properties,
            inbound_auth: None,
            gate_poll: Duration::from_secs(1),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
        }
    }
}

/// One RabbitMQ consumer serving one channel binding (the singleton unit).
pub struct RabbitMqTriggerSource {
    config: RabbitMqSourceConfig,
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

impl RabbitMqTriggerSource {
    /// A source for one channel binding. `config.properties` must carry a queue
    /// ([`codes::INBOUND_QUEUE_MISSING`] otherwise — fail-closed at wiring time, the
    /// fail-closed start posture).
    pub fn new(config: RabbitMqSourceConfig) -> Result<RabbitMqTriggerSource, Diagnostic> {
        if !config.properties.has_queue() {
            return Err(Diagnostic::error(
                codes::INBOUND_QUEUE_MISSING,
                format!(
                    "rabbitmq channel '{}' requires property 'queue'",
                    config.channel
                ),
            ));
        }
        Ok(RabbitMqTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured queue (diagnostics / tests).
    pub fn queue(&self) -> &str {
        &self.config.properties.queue
    }
}

impl TriggerSource for RabbitMqTriggerSource {
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
            // (broker-level TLS still applies).
            if let Some(auth) = &self.config.inbound_auth {
                if auth.scheme() == InboundScheme::Mtls {
                    warn!(
                        channel = %self.config.channel,
                        code = codes::INBOUND_MTLS_UNSUPPORTED,
                        "rabbitmq channel declared inbound-auth.scheme=mtls but per-channel mTLS \
                         is not supported — falling back to broker-level TLS"
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
                    "rabbitmq source supervisor did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The consumer supervisor: leadership-gated connect → consume → teardown loop with
/// exponential reconnect backoff. Broker absence never escapes as an error.
async fn supervise(
    config: RabbitMqSourceConfig,
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
            // Not (or no longer) the leader — stay subscribed to nothing and re-check.
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
                    "rabbitmq consumer unavailable — retrying in {:?}: {}",
                    backoff,
                    diagnostic.message
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
            Ok((connection, channel, consumer)) => {
                backoff = config.reconnect_min;
                info!(
                    channel = %config.channel,
                    queue = %config.properties.queue,
                    "rabbitmq consumer up"
                );
                let end = run_session(&config, &intake, &gate, &stop, &channel, consumer).await;
                teardown(&config, &connection, &channel).await;
                match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::LeadershipLost => {
                        info!(
                            channel = %config.channel,
                            "rabbitmq consumer cancelled — leadership lost"
                        );
                    }
                    SessionEnd::ConnectionLost => {
                        warn!(
                            channel = %config.channel,
                            "rabbitmq connection lost — reconnecting"
                        );
                    }
                }
            }
        }
    }
}

/// Connect + qos + passive queue declare + subscribe.
async fn open_session(
    config: &RabbitMqSourceConfig,
) -> Result<(Connection, lapin::Channel, lapin::Consumer), Diagnostic> {
    let props = &config.properties;
    let uri = props.connection_uri();
    let options = ConnectionProperties::default()
        .with_connection_name(format!("sutra-trigger-rabbitmq-{}", config.channel).into());
    let connection = Connection::connect(&uri, options).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONNECTION_FAILED,
            format!(
                "rabbitmq channel '{}' could not connect to {}:{}: {e}",
                config.channel, props.host, props.port
            ),
        )
    })?;
    let channel = connection.create_channel().await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONNECTION_FAILED,
            format!(
                "rabbitmq channel '{}' could not open an AMQP channel: {e}",
                config.channel
            ),
        )
    })?;
    channel
        .basic_qos(props.prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| {
            Diagnostic::error(
                codes::INBOUND_CONNECTION_FAILED,
                format!(
                    "rabbitmq channel '{}' basic.qos failed: {e}",
                    config.channel
                ),
            )
        })?;
    // PASSIVE declare — queues are pre-created; a missing one is a retried WARN, not a
    // boot failure (the non-fatal posture extends the fail-closed start).
    channel
        .queue_declare(
            &props.queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            Diagnostic::error(
                codes::INBOUND_QUEUE_MISSING,
                format!(
                    "rabbitmq channel '{}' queue '{}' was not declared on the broker: {e}",
                    config.channel, props.queue
                ),
            )
        })?;
    let consumer = channel
        .basic_consume(
            &props.queue,
            &format!("sutra-{}", config.channel),
            BasicConsumeOptions::default(), // no_ack=false — manual acks
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            Diagnostic::error(
                codes::INBOUND_DELIVER_FAILED,
                format!(
                    "rabbitmq channel '{}' could not start consumer on queue '{}': {e}",
                    config.channel, props.queue
                ),
            )
        })?;
    Ok((connection, channel, consumer))
}

/// The delivery loop: gate re-check per turn (and on an idle tick), delivery →
/// projection → intake → ack decision execution.
async fn run_session(
    config: &RabbitMqSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopToken>,
    channel: &lapin::Channel,
    mut consumer: lapin::Consumer,
) -> SessionEnd {
    loop {
        if stop.is_requested() {
            return SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            return SessionEnd::LeadershipLost;
        }
        let next = tokio::select! {
            _ = stop.notify.notified() => return SessionEnd::Stopped,
            _ = tokio::time::sleep(config.gate_poll) => continue, // idle gate re-check
            next = next_delivery(&mut consumer) => next,
        };
        match next {
            None => return SessionEnd::ConnectionLost,
            Some(Err(e)) => {
                warn!(
                    channel = %config.channel,
                    error = %e,
                    "rabbitmq delivery stream failed"
                );
                return SessionEnd::ConnectionLost;
            }
            Some(Ok(delivery)) => {
                let message = to_inbound_message(config, &delivery);
                // Per-message inbound auth: a rejected credential drops the delivery
                // (basic.nack(requeue=false) — the NackDrop posture) and NEVER dispatches.
                if let Some(auth) = &config.inbound_auth {
                    if auth.verify(&message.headers) == InboundVerdict::Reject {
                        warn!(
                            channel = %config.channel,
                            code = codes::INBOUND_AUTH_REJECTED,
                            delivery_tag = delivery.delivery_tag,
                            "rabbitmq channel rejected delivery — credential did not match expected"
                        );
                        if let Err(e) =
                            execute_decision(channel, &delivery, AckDecision::NackDrop).await
                        {
                            warn!(
                                channel = %config.channel,
                                code = codes::INBOUND_ACK_FAILED,
                                delivery_tag = delivery.delivery_tag,
                                error = %e,
                                "rabbitmq nack(drop) after auth reject failed — reconnecting"
                            );
                            return SessionEnd::ConnectionLost;
                        }
                        continue;
                    }
                }
                // The intake owns ack-mode TIMING: `on-persist` awaits the
                // decision and settles at dispatch-return; `on-complete` hands the
                // engine per-delivery settle callbacks — a PARKED instance defers the
                // broker settle to its terminal event (the deferred-ack registry), a
                // run-to-completion dispatch settles now exactly like on-persist.
                let decision = if config.properties.ack_mode == AckMode::OnComplete {
                    let settle = deferred_settle(
                        &config.channel,
                        channel,
                        delivery.delivery_tag,
                        tokio::runtime::Handle::current(),
                    );
                    match intake.deliver_deferred(message, settle).await {
                        DeliveryDisposition::Deferred => {
                            tracing::debug!(
                                channel = %config.channel,
                                delivery_tag = delivery.delivery_tag,
                                "delivery deferred — broker settle held until the \
                                 instance's terminal event"
                            );
                            continue;
                        }
                        DeliveryDisposition::Settle(decision) => decision,
                    }
                } else {
                    intake.deliver(message).await
                };
                if let Err(e) = execute_decision(channel, &delivery, decision).await {
                    warn!(
                        channel = %config.channel,
                        code = codes::INBOUND_ACK_FAILED,
                        delivery_tag = delivery.delivery_tag,
                        error = %e,
                        "rabbitmq ack/nack failed — reconnecting (broker will redeliver)"
                    );
                    return SessionEnd::ConnectionLost;
                }
            }
        }
    }
}

/// Pull the next delivery off lapin's `Stream` without a futures-util dependency.
async fn next_delivery(consumer: &mut lapin::Consumer) -> Option<Result<Delivery, lapin::Error>> {
    std::future::poll_fn(|cx| futures_core::Stream::poll_next(std::pin::Pin::new(consumer), cx))
        .await
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete` — the deferred
/// half of the ack mapping: ack → `basic.ack` (instance COMPLETED), nack →
/// `basic.nack(requeue=false)` (the NackDrop/DLX posture — instance FAILED is a
/// permanent reject; registry timeout/overflow nacks share it, freeing the broker slot,
/// and inbox dedup absorbs any DLX-side re-route). The callbacks fire on the engine
/// actor thread or the sweep task — non-async contexts — so each spawns the broker op
/// onto the runtime captured here. A failed settle is a WARN, never fatal: an unacked
/// delivery redelivers on reconnect and inbox dedup absorbs the duplicate.
fn deferred_settle(
    channel_name: &str,
    amqp: &lapin::Channel,
    delivery_tag: u64,
    runtime: tokio::runtime::Handle,
) -> DeferredSettle {
    fn settle_callback(
        channel_name: &str,
        amqp: &lapin::Channel,
        delivery_tag: u64,
        runtime: &tokio::runtime::Handle,
        decision: AckDecision,
        label: &'static str,
    ) -> Box<dyn FnMut() + Send> {
        let channel_name = channel_name.to_string();
        let amqp = amqp.clone();
        let runtime = runtime.clone();
        Box::new(move || {
            let channel_name = channel_name.clone();
            let amqp = amqp.clone();
            runtime.spawn(async move {
                // A dead AMQP channel (session torn down since the delivery) fails here:
                // the broker has already returned the unacked delivery to the queue.
                if let Err(e) = execute_decision_on(&amqp, delivery_tag, decision).await {
                    warn!(
                        channel = %channel_name,
                        code = codes::INBOUND_ACK_FAILED,
                        delivery_tag,
                        error = %e,
                        "deferred {label} failed — broker redelivers on reconnect \
                         (inbox dedup absorbs the duplicate)"
                    );
                }
            });
        })
    }
    DeferredSettle {
        ack: settle_callback(
            channel_name,
            amqp,
            delivery_tag,
            &runtime,
            AckDecision::Ack,
            "basic.ack",
        ),
        nack: settle_callback(
            channel_name,
            amqp,
            delivery_tag,
            &runtime,
            AckDecision::NackDrop,
            "basic.nack(requeue=false)",
        ),
    }
}

/// Execute an [`AckDecision`] on the broker — the ack mapping (`Ack` → `basic.ack`,
/// `NackRequeue` → `basic.nack(requeue)`, `NackDrop` → `basic.nack` without requeue, the
/// DLX posture).
async fn execute_decision(
    channel: &lapin::Channel,
    delivery: &Delivery,
    decision: AckDecision,
) -> Result<(), lapin::Error> {
    execute_decision_on(channel, delivery.delivery_tag, decision).await
}

/// [`execute_decision`] by bare delivery tag — the deferred settle callbacks outlive the
/// [`Delivery`] they answer for, so they carry only its tag.
async fn execute_decision_on(
    channel: &lapin::Channel,
    delivery_tag: u64,
    decision: AckDecision,
) -> Result<(), lapin::Error> {
    match decision {
        AckDecision::Ack => {
            channel
                .basic_ack(delivery_tag, BasicAckOptions::default())
                .await
        }
        AckDecision::NackRequeue => {
            channel
                .basic_nack(
                    delivery_tag,
                    BasicNackOptions {
                        requeue: true,
                        ..BasicNackOptions::default()
                    },
                )
                .await
        }
        AckDecision::NackDrop => {
            channel
                .basic_nack(
                    delivery_tag,
                    BasicNackOptions {
                        requeue: false,
                        ..BasicNackOptions::default()
                    },
                )
                .await
        }
    }
}

/// Drain posture: cancel the consumer first (in-flight deliveries settle their acks),
/// then close channel + connection. Every step best-effort.
async fn teardown(
    config: &RabbitMqSourceConfig,
    connection: &Connection,
    channel: &lapin::Channel,
) {
    let tag = format!("sutra-{}", config.channel);
    if let Err(e) = channel
        .basic_cancel(&tag, BasicCancelOptions::default())
        .await
    {
        tracing::debug!(channel = %config.channel, error = %e, "rabbitmq basic.cancel failed");
    }
    if let Err(e) = channel.close(200, "sutra source teardown").await {
        tracing::debug!(channel = %config.channel, error = %e, "rabbitmq channel close failed");
    }
    if let Err(e) = connection.close(200, "sutra source teardown").await {
        tracing::debug!(channel = %config.channel, error = %e, "rabbitmq connection close failed");
    }
}

/// Project one AMQP delivery into the engine's [`InboundMessage`] — the inbound-message
/// projection + header flattening (FROZEN `x-amqp-*` names).
///
/// Idempotency key: the AMQP `message-id` property when set (`explicit_event_id =
/// true` — broker-side dedup identifier); otherwise the channel-scoped delivery tag as a
/// string. The fallback is intentionally weak and marked NON-explicit so
/// it never suppresses a re-post through inbox dedup.
fn to_inbound_message(config: &RabbitMqSourceConfig, delivery: &Delivery) -> InboundMessage {
    let properties = &delivery.properties;
    let message_id = properties
        .message_id()
        .as_ref()
        .map(|s| s.as_str().to_string())
        .filter(|s| !s.trim().is_empty());
    let explicit_event_id = message_id.is_some();
    let idempotency_key = message_id.unwrap_or_else(|| delivery.delivery_tag.to_string());
    let content_type = properties
        .content_type()
        .as_ref()
        .map(|s| s.as_str().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    InboundMessage {
        tenant: config.tenant.clone(),
        module_key: config.module_key.clone(),
        channel: config.channel.clone(),
        headers: flatten_headers(delivery),
        body: delivery.data.clone().into(),
        content_type: Some(content_type),
        idempotency_key,
        explicit_event_id,
        received_at: now_rfc3339(),
        cloud_event: None,
    }
}

/// Header flattening — envelope + standard properties under the FROZEN
/// `x-amqp-*` names, then the custom application headers verbatim.
fn flatten_headers(delivery: &Delivery) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let exchange = delivery.exchange.as_str();
    if !exchange.is_empty() {
        out.insert("x-amqp-exchange".to_string(), exchange.to_string());
    }
    let routing_key = delivery.routing_key.as_str();
    if !routing_key.is_empty() {
        out.insert("x-amqp-routing-key".to_string(), routing_key.to_string());
    }
    let p = &delivery.properties;
    let mut put = |key: &str, value: Option<String>| {
        if let Some(v) = value {
            out.insert(key.to_string(), v);
        }
    };
    put(
        "x-amqp-message-id",
        p.message_id().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-correlation-id",
        p.correlation_id().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-type",
        p.kind().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-app-id",
        p.app_id().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-user-id",
        p.user_id().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-reply-to",
        p.reply_to().as_ref().map(|s| s.as_str().to_string()),
    );
    put(
        "x-amqp-content-encoding",
        p.content_encoding()
            .as_ref()
            .map(|s| s.as_str().to_string()),
    );
    if let Some(headers) = p.headers().as_ref() {
        for (key, value) in headers.inner() {
            out.insert(key.as_str().to_string(), stringify_field(value));
        }
    }
    out
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use lapin::types::{AMQPValue, FieldTable, ShortString};
    use lapin::BasicProperties;

    use super::*;

    fn config() -> RabbitMqSourceConfig {
        let mut props = base_properties();
        props.queue = "q".to_string();
        RabbitMqSourceConfig::new("acme", "acme/payments/1.0.0", "transfer-queue", props)
    }

    fn base_properties() -> RabbitMqChannelProperties {
        RabbitMqChannelProperties {
            host: "localhost".to_string(),
            port: 5672,
            virtual_host: "/".to_string(),
            username: None,
            password: None,
            queue: String::new(),
            exchange: String::new(),
            prefetch_count: 10,
            ack_mode: super::super::AckMode::OnPersist,
            singleton: false,
        }
    }

    fn delivery(properties: BasicProperties, body: &[u8], tag: u64) -> Delivery {
        Delivery {
            delivery_tag: tag,
            exchange: "test-ex".into(),
            routing_key: "test-key".into(),
            redelivered: false,
            properties,
            data: body.to_vec(),
            acker: lapin::acker::Acker::default(),
        }
    }

    #[test]
    fn message_id_becomes_the_explicit_idempotency_key() {
        let d = delivery(
            BasicProperties::default().with_message_id("evt-42".into()),
            b"hello",
            17,
        );
        let m = to_inbound_message(&config(), &d);
        assert_eq!(m.idempotency_key, "evt-42");
        assert!(m.explicit_event_id);
        assert_eq!(m.body.into_inner(), b"hello");
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.module_key, "acme/payments/1.0.0");
        assert_eq!(m.channel, "transfer-queue");
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn delivery_tag_fallback_is_weak_and_non_explicit() {
        let d = delivery(BasicProperties::default(), b"x", 99);
        let m = to_inbound_message(&config(), &d);
        assert_eq!(m.idempotency_key, "99");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn amqp_standard_properties_project_under_the_frozen_x_amqp_names() {
        let properties = BasicProperties::default()
            .with_message_id("msg-1".into())
            .with_correlation_id("corr-1".into())
            .with_content_type("application/json".into())
            .with_content_encoding("utf-8".into())
            .with_reply_to("amqp://reply-exchange/route-key".into())
            .with_type("order.created".into())
            .with_app_id("payment-service".into());
        let m = to_inbound_message(&config(), &delivery(properties, b"{}", 1));

        // FROZEN header names — the AMQP standard properties project under `x-amqp-*`.
        assert_eq!(m.headers.get("x-amqp-message-id").unwrap(), "msg-1");
        assert_eq!(m.headers.get("x-amqp-correlation-id").unwrap(), "corr-1");
        assert_eq!(m.headers.get("x-amqp-content-encoding").unwrap(), "utf-8");
        assert_eq!(
            m.headers.get("x-amqp-reply-to").unwrap(),
            "amqp://reply-exchange/route-key"
        );
        assert_eq!(m.headers.get("x-amqp-type").unwrap(), "order.created");
        assert_eq!(m.headers.get("x-amqp-app-id").unwrap(), "payment-service");
        assert_eq!(m.headers.get("x-amqp-exchange").unwrap(), "test-ex");
        assert_eq!(m.headers.get("x-amqp-routing-key").unwrap(), "test-key");
        assert_eq!(m.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn custom_headers_flatten_including_long_string_and_byte_values() {
        let mut table = FieldTable::default();
        table.insert(
            ShortString::from("plain"),
            AMQPValue::LongString("plain-value".into()),
        );
        table.insert(
            ShortString::from("bytes"),
            AMQPValue::ByteArray(b"byte-value".to_vec().into()),
        );
        table.insert(ShortString::from("count"), AMQPValue::LongInt(7));
        let properties = BasicProperties::default().with_headers(table);
        let m = to_inbound_message(&config(), &delivery(properties, b"", 1));
        assert_eq!(m.headers.get("plain").unwrap(), "plain-value");
        assert_eq!(m.headers.get("bytes").unwrap(), "byte-value");
        assert_eq!(m.headers.get("count").unwrap(), "7");
    }

    #[test]
    fn content_type_defaults_to_octet_stream() {
        let m = to_inbound_message(&config(), &delivery(BasicProperties::default(), b"", 1));
        assert_eq!(m.content_type.as_deref(), Some("application/octet-stream"));
    }

    #[test]
    fn source_without_queue_fails_closed() {
        let cfg = RabbitMqSourceConfig::new("acme", "acme/m/1", "ch", base_properties());
        let err = match RabbitMqTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a queue must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_QUEUE_MISSING);
    }

    #[tokio::test]
    async fn stop_before_start_is_idempotent_and_start_resolves_without_a_broker() {
        // Points at a port nobody listens on — start MUST still resolve Ok (
        // broker absence is non-fatal, the supervisor retries in the background).
        let mut cfg = config();
        cfg.properties.host = "127.0.0.1".to_string();
        cfg.properties.port = 1; // reserved, never a broker
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        let source = RabbitMqTriggerSource::new(cfg).expect("source");

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
        // Give the supervisor one failed connect cycle, then stop cleanly.
        tokio::time::sleep(Duration::from_millis(50)).await;
        source.stop().await.expect("stop");
        source.stop().await.expect("second stop is a no-op");
    }
}
