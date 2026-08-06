//! The AMQP 1.0 inbound trigger — [`AmqpTriggerSource`] implements [`TriggerSource`]: one
//! `fe2o3-amqp` receiver per channel binding, deliveries projected into [`InboundMessage`]s
//! and pushed through the [`InboundIntake`] seam, the returned [`AckDecision`] executed as an
//! AMQP 1.0 disposition (accept / release / reject).
//!
//! The AMQP 1.0 trigger-source lifecycle (mirrors the kafka
//! transport structurally; the settle model is AMQP dispositions, not offset commits):
//!
//! - **Broker absence is NON-FATAL**: `start` spawns a supervisor task and resolves;
//!   a missing broker WARNs (`SUTRA.INBOUND.AMQP.CONNECTION_FAILED`) and the supervisor
//!   retries with exponential backoff in the background — readiness is unaffected.
//! - **Ack timing rides the intake seam**: the source `await`s
//!   [`InboundIntake::deliver`] and maps the decision onto AMQP 1.0 dispositions — `Ack` →
//!   **accept** (settle Accepted), `NackRequeue` → **release** (settle Released; the broker
//!   redelivers), `NackDrop` → **reject** (settle Rejected; the broker DLQs/discards the
//!   poison — advancing IS the drop). The inbox dedup absorbs any release-redelivery.
//! - **`ack-mode: on-complete` defers the disposition**: the source hands the engine
//!   per-delivery settle callbacks through [`InboundIntake::deliver_deferred`]; a PARKED
//!   instance answers `Deferred` and the delivery stays UNSETTLED on the link until the
//!   instance's terminal event fires the registered accept (COMPLETED) or reject (FAILED /
//!   registry timeout/overflow). Because a `fe2o3-amqp` [`Receiver`] is owned by the session
//!   task (`recv` needs `&mut`) and a delivery is only disposable on the link that carried it,
//!   the callbacks do not touch the receiver: they post a [`SettleCommand`] over an unbounded
//!   channel and the consume loop executes the disposition on its next turn.
//! - **Singleton gating**: the receiver attaches ONLY while `gate.is_leading()`; on
//!   leadership loss it detaches (closing the link), then re-attaches when the gate returns.
//!   The engine per-channel lease — not the broker — makes a `singleton: true` channel
//!   consume on exactly one replica.
//!
//! Idempotency key: the `sutra-outbox-key` application property when set
//! (`explicit_event_id = true`); otherwise the AMQP `message-id` (NON-explicit) and, absent
//! that, the synthetic `address-<delivery-id>` coordinate — so a fallback key never
//! suppresses a re-post through inbox dedup (the Kafka `topic-partition-offset` posture).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::types::messaging::Body;
use fe2o3_amqp::types::primitives::{SimpleValue, Value};
use fe2o3_amqp::{Connection, Delivery, Receiver, Session};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{
    codes, AckMode, AmqpChannelProperties, PROPERTY_CONTENT_TYPE, PROPERTY_OUTBOX_KEY, TRANSPORT,
};

/// The body type the receiver decodes into — `Body<Value>` accepts any body section (a
/// `Data` binary from the sink, or an amqp-value String / Binary from a foreign producer).
type WireBody = Body<Value>;

/// Everything one receiver needs, prepared by the wiring (credentials already RESOLVED).
#[derive(Debug, Clone)]
pub struct AmqpSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Host/destination/credentials/ack-mode properties (credentials resolved).
    pub properties: AmqpChannelProperties,
    /// Leadership re-check cadence while idle / between reconnect checks.
    pub gate_poll: Duration,
    /// Reconnect backoff floor (doubles per failure up to [`Self::reconnect_max`]).
    pub reconnect_min: Duration,
    /// Reconnect backoff ceiling.
    pub reconnect_max: Duration,
    /// Per-message inbound auth (`inbound-auth.*`), resolved once at wiring. `None` =
    /// no inbound-auth declared. A rejected credential drops the delivery (AMQP 1.0
    /// settle-accepted — no redelivery, no dead-letter).
    pub inbound_auth: Option<BrokerInboundAuth>,
}

impl AmqpSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff.
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: AmqpChannelProperties,
    ) -> AmqpSourceConfig {
        AmqpSourceConfig {
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

    /// The effective per-poll receive timeout (the idle gate/stop re-check cadence).
    fn recv_timeout(&self) -> Duration {
        Duration::from_millis(self.properties.receive_timeout_ms.max(1))
    }
}

/// One AMQP receiver serving one channel binding (the singleton unit).
/// How long [`TriggerSource::start`] waits for the first consumer attach before returning and
/// leaving the supervisor to keep retrying. Long enough to cover a cold broker on a loaded CI
/// runner; not a correctness bound (the supervisor reconnects regardless).
const ATTACH_WAIT: Duration = Duration::from_secs(30);

pub struct AmqpTriggerSource {
    config: AmqpSourceConfig,
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

/// A DEFERRED disposition (`ack-mode: on-complete`) queued by a settle callback for the
/// session task to execute. The callbacks fire on the engine actor thread (or the registry
/// sweep) — non-async contexts that must never block — and a delivery can only be disposed
/// on the link that carried it, whose [`Receiver`] the consume loop owns. So the callback
/// only POSTS this command (unbounded, non-blocking) and the loop performs the disposition.
struct SettleCommand {
    /// The delivery coordinates (id / tag / receiver-settle-mode) a disposition frame needs —
    /// it outlives the [`Delivery`] it answers for, which is dropped at dispatch time.
    info: DeliveryInfo,
    /// The [`AckDecision`] to map onto a disposition — `Ack` for the instance's COMPLETED
    /// callback, `NackDrop` for its FAILED / timeout / overflow callback.
    decision: AckDecision,
    /// Disposition name, for the settled/dropped diagnostics.
    label: &'static str,
}

/// What one consume-loop turn produced. Modelled as owned DATA so no `tokio::select!` branch
/// holds a borrow of the [`BrokerSession`] while another branch borrows its receiver mutably
/// for `recv` — the handler then disposes/dispatches with the borrow free.
enum Turn {
    /// Stop was signalled mid-turn.
    Stopped,
    /// The receive timed out — the idle gate/stop re-check.
    Idle,
    /// A deferred settle callback posted a disposition to execute.
    Settle(SettleCommand),
    /// A delivery arrived (boxed: `Delivery` dwarfs the other variants).
    Delivery(Box<Delivery<WireBody>>),
    /// `recv` failed — the link/connection is unhealthy (message rendered for the WARN).
    RecvFailed(String),
}

/// A live connection + session + receiver, torn down together.
struct BrokerSession {
    connection: fe2o3_amqp::connection::ConnectionHandle<()>,
    session: fe2o3_amqp::session::SessionHandle<()>,
    receiver: Receiver,
}

impl BrokerSession {
    /// Best-effort teardown: detach the receiver (in-flight settle already ran), end the
    /// session, close the connection.
    async fn teardown(mut self) {
        let _ = self.receiver.close().await;
        let _ = self.session.end().await;
        let _ = self.connection.close().await;
    }
}

impl AmqpTriggerSource {
    /// A source for one channel binding. `config.properties` must carry a `host`
    /// ([`codes::INBOUND_CONFIG_INVALID`]) and an inbound destination — a `queue` OR a
    /// `topic` ([`codes::INBOUND_QUEUE_MISSING`]) — fail-closed at wiring time.
    pub fn new(config: AmqpSourceConfig) -> Result<AmqpTriggerSource, Diagnostic> {
        if config.properties.host.trim().is_empty() {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("amqp channel '{}' requires property 'host'", config.channel),
            ));
        }
        if config.properties.source_address().is_none() {
            return Err(Diagnostic::error(
                codes::INBOUND_QUEUE_MISSING,
                format!(
                    "amqp inbound channel '{}' requires property 'queue' or property 'topic'",
                    config.channel
                ),
            ));
        }
        Ok(AmqpTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured source address (queue/topic; diagnostics / tests).
    pub fn address(&self) -> Option<&str> {
        self.config.properties.source_address()
    }
}

impl TriggerSource for AmqpTriggerSource {
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
                        "amqp channel declared inbound-auth.scheme=mtls but per-channel mTLS is \
                         not supported — falling back to transport-level TLS"
                    );
                }
            }
            let stop = Arc::new(StopToken::new());
            // `start()` resolves once the consumer is ATTACHED, not merely spawned. Without this
            // a caller that publishes immediately after start races the attach — and on a broker
            // whose address is auto-created and non-durable (Artemis MULTICAST, as the ITs use),
            // a message published before any consumer exists is DISCARDED, not queued. The wait
            // is bounded: if the broker is slow or down the supervisor keeps retrying in the
            // background exactly as before, so startup never blocks indefinitely.
            let (tx, rx) = tokio::sync::oneshot::channel();
            let attached = Arc::new(tokio::sync::Mutex::new(Some(tx)));
            let task = tokio::spawn(supervise(
                self.config.clone(),
                intake,
                gate,
                Arc::clone(&stop),
                Arc::clone(&attached),
            ));
            *running = Some(Running { task, stop });
            let _ = tokio::time::timeout(ATTACH_WAIT, rx).await;
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
                    "amqp source supervisor did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The receiver supervisor: leadership-gated attach → consume → teardown loop with
/// exponential reconnect backoff. Broker absence never escapes as an error.
async fn supervise(
    config: AmqpSourceConfig,
    intake: Arc<dyn InboundIntake>,
    gate: Arc<dyn LeaderGate>,
    stop: Arc<StopToken>,
    attached: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
) {
    let mut backoff = config.reconnect_min;
    loop {
        if stop.is_requested() {
            return;
        }
        if !gate.is_leading() {
            // Not (or no longer) the leader — attach nothing and re-check.
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
                    "amqp consumer unavailable — retrying in {:?}: {}",
                    backoff,
                    diagnostic.message
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
            Ok(mut broker) => {
                backoff = config.reconnect_min;
                info!(
                    channel = %config.channel,
                    address = config.properties.source_address().unwrap_or_default(),
                    "amqp consumer up"
                );
                // First successful attach: release anyone blocked in `start()`. Fired once; later
                // reconnects find the receiver already taken and simply skip it.
                if let Some(tx) = attached.lock().await.take() {
                    let _ = tx.send(());
                }
                let end = run_session(&config, &intake, &gate, &stop, &mut broker).await;
                broker.teardown().await;
                match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::LeadershipLost => info!(
                        channel = %config.channel,
                        "amqp consumer detached — leadership lost"
                    ),
                    SessionEnd::ConnectionLost => warn!(
                        channel = %config.channel,
                        "amqp consumer stream failed — reconnecting"
                    ),
                }
            }
        }
    }
}

/// Open the connection, begin a session, attach a receiver on the queue/topic. A broker
/// that is down fails here (NON-FATAL — the supervisor retries).
async fn open_session(config: &AmqpSourceConfig) -> Result<BrokerSession, Diagnostic> {
    let props = &config.properties;
    if props.tls {
        return Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!(
                "amqp channel '{}' requested tls but TLS is not compiled into this build \
                 (PLAINTEXT only); use broker-level TLS via a sidecar/mesh",
                config.channel
            ),
        ));
    }
    let address = props.source_address().ok_or_else(|| {
        Diagnostic::error(
            codes::INBOUND_QUEUE_MISSING,
            format!(
                "amqp channel '{}' has no queue/topic address",
                config.channel
            ),
        )
    })?;
    let container = format!("sutra-amqp-source-{}", config.channel);
    let uri = props.connection_uri();
    let mut connection = Connection::open(container.clone(), uri.as_str())
        .await
        .map_err(|e| {
            Diagnostic::error(
                codes::INBOUND_CONNECTION_FAILED,
                format!(
                    "amqp channel '{}' could not connect to {}:{}: {e}",
                    config.channel, props.host, props.port
                ),
            )
        })?;
    let mut session = Session::begin(&mut connection).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONNECTION_FAILED,
            format!(
                "amqp channel '{}' could not begin a session: {e}",
                config.channel
            ),
        )
    })?;
    let receiver = Receiver::attach(&mut session, format!("{container}-recv"), address)
        .await
        .map_err(|e| {
            Diagnostic::error(
                codes::INBOUND_CONNECTION_FAILED,
                format!(
                    "amqp channel '{}' could not attach a receiver on '{}': {e}",
                    config.channel, address
                ),
            )
        })?;
    Ok(BrokerSession {
        connection,
        session,
        receiver,
    })
}

/// The consume loop: gate re-check per turn (and on an idle receive timeout), delivery →
/// projection → intake → disposition, interleaved with any DEFERRED dispositions the
/// `ack-mode: on-complete` settle callbacks posted since the last turn.
async fn run_session(
    config: &AmqpSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopToken>,
    broker: &mut BrokerSession,
) -> SessionEnd {
    let recv_timeout = config.recv_timeout();
    // The deferred-settle bridge is per SESSION: a `DeliveryInfo` is only disposable on the
    // link that delivered it, and the receiving half dies with this loop — so a LATE settle
    // (terminal event after the session/link went down) fails its send and WARNs as a no-op,
    // which is the correct posture: the broker never saw a disposition, so it redelivers the
    // unsettled delivery and inbox dedup absorbs the duplicate.
    let (settle_tx, mut settle_rx) = tokio::sync::mpsc::unbounded_channel::<SettleCommand>();
    loop {
        if stop.is_requested() {
            return SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            return SessionEnd::LeadershipLost;
        }
        // `Receiver::recv` is documented cancel-safe, so losing the race to a stop signal or a
        // queued deferred disposition never drops an in-flight transfer.
        let turn = tokio::select! {
            _ = stop.notify.notified() => Turn::Stopped,
            Some(command) = settle_rx.recv() => Turn::Settle(command),
            r = tokio::time::timeout(recv_timeout, broker.receiver.recv::<WireBody>()) => match r {
                Err(_elapsed) => Turn::Idle,
                Ok(Ok(delivery)) => Turn::Delivery(Box::new(delivery)),
                Ok(Err(e)) => Turn::RecvFailed(e.to_string()),
            },
        };
        let delivery = match turn {
            Turn::Stopped => return SessionEnd::Stopped,
            Turn::Idle => continue, // idle gate/stop re-check
            Turn::Settle(command) => {
                if let Err(end) = settle_deferred(config, &broker.receiver, command).await {
                    return end;
                }
                continue;
            }
            Turn::RecvFailed(e) => {
                warn!(channel = %config.channel, error = %e, "amqp recv failed");
                return SessionEnd::ConnectionLost;
            }
            Turn::Delivery(delivery) => *delivery,
        };
        let inbound = to_inbound_message(config, &delivery);
        // Per-message inbound auth: a rejected credential drops the delivery natively
        // (AMQP 1.0 settle-accepted — no redelivery, no dead-letter) and NEVER dispatches.
        if let Some(auth) = &config.inbound_auth {
            if auth.verify(&inbound.headers) == InboundVerdict::Reject {
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_AUTH_REJECTED,
                    "amqp channel rejected a delivery — credential did not match expected"
                );
                if let Err(end) =
                    settle(config, &broker.receiver, &delivery, AckDecision::Ack).await
                {
                    return end;
                }
                continue;
            }
        }
        // The intake owns ack-mode TIMING; the disposition mirrors the decision.
        // `on-persist` awaits the decision and disposes at dispatch-return; `on-complete`
        // hands the engine per-delivery settle callbacks — a PARKED instance defers the
        // disposition to its terminal event (the deferred-ack registry), a run-to-completion
        // dispatch disposes now exactly like on-persist.
        let decision = if config.properties.ack_mode == AckMode::OnComplete {
            let settle = deferred_settle(&config.channel, &delivery, &settle_tx);
            match intake.deliver_deferred(inbound, settle).await {
                DeliveryDisposition::Deferred => {
                    debug!(
                        channel = %config.channel,
                        delivery_id = delivery.delivery_id(),
                        "delivery deferred — the amqp disposition is held (delivery stays \
                         UNSETTLED on the link) until the instance's terminal event"
                    );
                    continue;
                }
                DeliveryDisposition::Settle(decision) => decision,
            }
        } else {
            intake.deliver(inbound).await
        };
        if let Err(end) = settle(config, &broker.receiver, &delivery, decision).await {
            return end;
        }
    }
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete` — the deferred half of
/// the disposition mapping: ack → **accept** (instance COMPLETED), nack → **reject**
/// (instance FAILED is a permanent reject; the broker DLQs/discards the poison — the AMQP 1.0
/// drop posture. Registry timeout/overflow nacks share it, freeing the link's unsettled slot,
/// and inbox dedup absorbs any DLQ-side re-route). Each callback carries its own
/// [`DeliveryInfo`] and posts it to the session task; `Option::take` makes them IDEMPOTENT by
/// construction, so a repeat call can never emit a second disposition.
fn deferred_settle(
    channel: &str,
    delivery: &Delivery<WireBody>,
    settle_tx: &UnboundedSender<SettleCommand>,
) -> DeferredSettle {
    DeferredSettle {
        ack: settle_callback(
            channel,
            DeliveryInfo::from(delivery),
            AckDecision::Ack,
            settle_tx.clone(),
            "accept",
        ),
        nack: settle_callback(
            channel,
            DeliveryInfo::from(delivery),
            AckDecision::NackDrop,
            settle_tx.clone(),
            "reject",
        ),
    }
}

/// One deferred settle callback: post the disposition to the session task, or WARN when the
/// session is already gone (a late settle is a no-op — the broker redelivers).
fn settle_callback(
    channel: &str,
    info: DeliveryInfo,
    decision: AckDecision,
    settle_tx: UnboundedSender<SettleCommand>,
    label: &'static str,
) -> Box<dyn FnMut() + Send> {
    let channel = channel.to_string();
    let mut info = Some(info);
    Box::new(move || {
        let Some(info) = info.take() else {
            return; // already settled — idempotent no-op
        };
        let delivery_id = info.delivery_id();
        if settle_tx
            .send(SettleCommand {
                info,
                decision,
                label,
            })
            .is_err()
        {
            warn!(
                channel = %channel,
                code = codes::INBOUND_RECEIVE_FAILED,
                delivery_id,
                "deferred {label} dropped — the amqp session ended before the instance's \
                 terminal event; the delivery was never settled, so the broker redelivers it \
                 (inbox dedup absorbs the duplicate)"
            );
        }
    })
}

/// Execute a DEFERRED disposition posted by a settle callback. A disposition for a delivery
/// the link no longer tracks is a silent no-op in `fe2o3-amqp` (it is simply not in the
/// unsettled map); a real failure means the link is unhealthy → reconnect, exactly like the
/// inline path.
async fn settle_deferred(
    config: &AmqpSourceConfig,
    receiver: &Receiver,
    command: SettleCommand,
) -> Result<(), SessionEnd> {
    let SettleCommand {
        info,
        decision,
        label,
    } = command;
    let delivery_id = info.delivery_id();
    settle(config, receiver, info, decision).await?;
    debug!(
        channel = %config.channel,
        delivery_id,
        "deferred {label} executed on the broker"
    );
    Ok(())
}

/// Execute an [`AckDecision`] as an AMQP 1.0 disposition — `Ack` → accept, `NackRequeue`
/// → release (redeliver), `NackDrop` → reject (poison drop). A disposition failure means
/// the link/connection is unhealthy → reconnect (the broker redelivers un-settled work).
async fn settle(
    config: &AmqpSourceConfig,
    receiver: &Receiver,
    delivery: impl Into<DeliveryInfo>,
    decision: AckDecision,
) -> Result<(), SessionEnd> {
    // One `DeliveryInfo` up front: the inline path passes `&Delivery`, the deferred path passes
    // the info a settle callback carried across the dispatch.
    let delivery = delivery.into();
    let result = match decision {
        AckDecision::Ack => receiver.accept(delivery).await,
        AckDecision::NackRequeue => receiver.release(delivery).await,
        AckDecision::NackDrop => receiver.reject(delivery, None).await,
    };
    if let Err(e) = result {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_RECEIVE_FAILED,
            error = %e,
            "amqp disposition failed — reconnecting (broker will redeliver un-settled work)"
        );
        return Err(SessionEnd::ConnectionLost);
    }
    Ok(())
}

/// Project one AMQP delivery into the engine's [`InboundMessage`].
fn to_inbound_message(config: &AmqpSourceConfig, delivery: &Delivery<WireBody>) -> InboundMessage {
    let message = delivery.message();
    let mut headers = BTreeMap::new();
    if let Some(ap) = &message.application_properties {
        for (key, value) in ap.0.iter() {
            headers.insert(key.clone(), simple_value_to_string(value));
        }
    }
    let message_id = message
        .properties
        .as_ref()
        .and_then(|p| p.message_id.as_ref())
        .and_then(message_id_to_string);
    let body = body_bytes(&message.body);
    let coordinate = format!(
        "{}-{}",
        config.properties.source_address().unwrap_or_default(),
        delivery.delivery_id()
    );
    project_inbound(config, headers, message_id, coordinate, body)
}

/// The pure application-property → [`InboundMessage`] projection (the broker-free core,
/// unit-tested): the `sutra-outbox-key` property is the EXPLICIT idempotency key when set;
/// otherwise the AMQP `message-id` (NON-explicit), else the synthetic `coordinate`;
/// `content-type` property → content type (default `application/octet-stream`).
fn project_inbound(
    config: &AmqpSourceConfig,
    headers: BTreeMap<String, String>,
    message_id: Option<String>,
    coordinate: String,
    body: Vec<u8>,
) -> InboundMessage {
    let outbox_key = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(PROPERTY_OUTBOX_KEY))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let explicit_event_id = outbox_key.is_some();
    let idempotency_key = outbox_key
        .or_else(|| {
            // The AMQP 1.0 / JMS message-id carries the `ID:` prefix (Artemis stamps it);
            // strip it so the NON-explicit fallback key is the raw provider id (stable
            // across the wire for inbox dedup).
            message_id
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.strip_prefix("ID:").map(str::to_string).unwrap_or(s))
        })
        .unwrap_or(coordinate);
    let content_type = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(PROPERTY_CONTENT_TYPE))
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

/// Extract the raw payload bytes from any AMQP body section.
fn body_bytes(body: &WireBody) -> Vec<u8> {
    match body {
        Body::Data(batch) => {
            let mut out = Vec::new();
            for data in batch.iter() {
                out.extend_from_slice(&data.0);
            }
            out
        }
        Body::Value(value) => value_to_bytes(&value.0),
        Body::Sequence(_) | Body::Empty => Vec::new(),
    }
}

fn value_to_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::String(s) => s.clone().into_bytes(),
        Value::Binary(b) => b.to_vec(),
        _ => Vec::new(),
    }
}

/// Render a message-id (`ID:`-prefix stripped, matching the Qpid provider form) as a string.
fn message_id_to_string(id: &fe2o3_amqp::types::messaging::MessageId) -> Option<String> {
    use fe2o3_amqp::types::messaging::MessageId;
    let raw = match id {
        MessageId::String(s) => s.clone(),
        MessageId::Ulong(u) => u.to_string(),
        MessageId::Uuid(u) => format!("{u:?}"),
        MessageId::Binary(b) => String::from_utf8_lossy(b).into_owned(),
    };
    let stripped = raw.strip_prefix("ID:").unwrap_or(&raw).to_string();
    Some(stripped).filter(|s| !s.is_empty())
}

/// Render an application-property value as a string (strings verbatim; other scalars debug).
fn simple_value_to_string(value: &SimpleValue) -> String {
    match value {
        SimpleValue::String(s) => s.clone(),
        SimpleValue::Bool(b) => b.to_string(),
        SimpleValue::Int(i) => i.to_string(),
        SimpleValue::Long(l) => l.to_string(),
        SimpleValue::Uint(u) => u.to_string(),
        SimpleValue::Ulong(u) => u.to_string(),
        other => format!("{other:?}"),
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

    fn base_properties() -> AmqpChannelProperties {
        AmqpChannelProperties {
            host: "localhost".to_string(),
            port: 5672,
            tls: false,
            username: None,
            password: None,
            queue: Some("in-q".to_string()),
            topic: None,
            prefetch_count: 10,
            receive_timeout_ms: 1000,
            ack_mode: super::super::AckMode::OnPersist,
            singleton: false,
        }
    }

    fn config() -> AmqpSourceConfig {
        AmqpSourceConfig::new(
            "acme",
            "acme/payments/1.0.0",
            "payments-in",
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
    fn outbox_key_property_becomes_the_explicit_idempotency_key() {
        let m = project_inbound(
            &config(),
            headers(&[
                ("sutra-outbox-key", "ob-in-1"),
                ("content-type", "application/json"),
            ]),
            Some("provider-msg-99".to_string()),
            "in-q-3".to_string(),
            b"{\"hello\":\"world\"}".to_vec(),
        );
        assert_eq!(m.idempotency_key, "ob-in-1");
        assert!(m.explicit_event_id);
        assert_eq!(m.body.into_inner(), b"{\"hello\":\"world\"}");
        assert_eq!(m.content_type.as_deref(), Some("application/json"));
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.channel, "payments-in");
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn idempotency_key_falls_back_to_message_id_then_coordinate() {
        // message-id present, no outbox key ⇒ non-explicit message-id.
        let m = project_inbound(
            &config(),
            headers(&[]),
            Some("ID:provider-77".to_string()),
            "in-q-4".to_string(),
            b"x".to_vec(),
        );
        assert_eq!(m.idempotency_key, "provider-77", "ID: prefix stripped");
        assert!(!m.explicit_event_id);

        // no outbox key and no message-id ⇒ synthetic coordinate (non-explicit).
        let m = project_inbound(
            &config(),
            headers(&[]),
            None,
            "in-q-9".to_string(),
            b"x".to_vec(),
        );
        assert_eq!(m.idempotency_key, "in-q-9");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn blank_outbox_key_property_falls_back() {
        let m = project_inbound(
            &config(),
            headers(&[("sutra-outbox-key", "   ")]),
            None,
            "in-q-0".to_string(),
            Vec::new(),
        );
        assert_eq!(m.idempotency_key, "in-q-0");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn content_type_defaults_to_octet_stream() {
        let m = project_inbound(&config(), headers(&[]), None, "c".to_string(), Vec::new());
        assert_eq!(m.content_type.as_deref(), Some("application/octet-stream"));
    }

    #[test]
    fn source_without_destination_fails_closed() {
        let mut props = base_properties();
        props.queue = None;
        props.topic = None;
        let err =
            match AmqpTriggerSource::new(AmqpSourceConfig::new("acme", "acme/m/1", "ch", props)) {
                Err(diagnostic) => diagnostic,
                Ok(_) => panic!("a source without a destination must be refused"),
            };
        assert_eq!(err.code, codes::INBOUND_QUEUE_MISSING);
    }

    #[test]
    fn source_without_host_fails_closed() {
        let mut props = base_properties();
        props.host = String::new();
        let err =
            match AmqpTriggerSource::new(AmqpSourceConfig::new("acme", "acme/m/1", "ch", props)) {
                Err(diagnostic) => diagnostic,
                Ok(_) => panic!("a source without a host must be refused"),
            };
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[tokio::test]
    async fn stop_before_start_is_idempotent_and_start_resolves_without_a_broker() {
        // Points at a port nobody listens on — start MUST still resolve Ok (broker
        // absence is non-fatal, the supervisor retries in the background).
        let mut props = base_properties();
        props.host = "127.0.0.1".to_string();
        props.port = 1; // reserved, never a broker
        let mut cfg = AmqpSourceConfig::new("acme", "acme/payments/1.0.0", "payments-in", props);
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        cfg.gate_poll = Duration::from_millis(20);
        let source = AmqpTriggerSource::new(cfg).expect("source");

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
