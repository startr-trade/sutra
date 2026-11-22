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
//! - **Concurrent intake per binding**: up to `max-in-flight` (default 16) deliveries run
//!   through the intake at once, in an [`IntakeWindow`]; each is settled when its own call
//!   returns, in completion order, so two deliveries for one instance may reach the engine in
//!   either order. `max-in-flight: 1` is the serial, arrival-order consumer. The receiver is
//!   attached with link credit `max(prefetch-count, 2 × max-in-flight)`, auto-replenished.
//! - **Session end drains**: a stop, a lost connection or lost leadership first waits (up to
//!   30 s) for the calls in flight. Only a stop settles them, on the still-open link; otherwise
//!   the broker redelivers and inbox dedup absorbs the copies.
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
//! - **Outage window**: the first failed connect, or a lost connection, logs
//!   `SUTRA.INBOUND.AMQP.OUTAGE_STARTED` with its time; the next attach logs
//!   `SUTRA.INBOUND.AMQP.OUTAGE_ENDED` with the start, the end and the length in milliseconds.
//!   Every step of an attach (connect and open, session begin, receiver attach) and the
//!   teardown of a lost session are bounded by the operation timeout (30 s), so a broker that
//!   takes the connection and never answers — as a restarting queue manager can — costs one
//!   attempt, not the supervisor.
//! - **Address type**: `address-type: queue | topic` sends the matching source capability, which
//!   brokers that read an address without one as a topic need for a queue.
//!
//! Message properties ride as headers: `message-id`, `correlation-id`, `reply-to` and `subject`
//! as `x-amqp-message-id`, `x-amqp-correlation-id`, `x-amqp-reply-to`, `x-amqp-subject` (a binary
//! id that is not printable text as lower-case hex).
//!
//! Idempotency key: the `sutra-outbox-key` application property when set (or its identifier
//! spelling `sutra_outbox_key`)
//! (`explicit_event_id = true`); otherwise the AMQP `message-id` (NON-explicit) and, absent
//! that, the synthetic `address-<delivery-id>` coordinate — so a fallback key never
//! suppresses a re-post through inbox dedup (the Kafka `topic-partition-offset` posture).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::link::CreditMode;
use fe2o3_amqp::types::messaging::{Body, Source};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Value};
use fe2o3_amqp::{Connection, Delivery, Receiver, Session};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::connectivity::{ConnectivityDirection, ConnectivityEvent};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, DeliveryDropRecord, InboundIntake,
    IntakeWindow, LeaderGate, TriggerSource,
};

use super::outage::OutageWindow;
use super::tls::{classify_open_error, open_tls_connection, AmqpBrokerTarget, AmqpConnectFailure};
use super::{
    codes, identifier_name, unique_container_id, AckMode, AmqpChannelProperties,
    HEADER_CORRELATION_ID, HEADER_MESSAGE_ID, HEADER_REPLY_TO, HEADER_SUBJECT,
    PROPERTY_CONTENT_TYPE, PROPERTY_OUTBOX_KEY, TRANSPORT,
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
    /// The bound on each broker round trip of an attach (connect with SASL and open, session
    /// begin, receiver attach) and on the whole teardown. A broker that accepts the TCP
    /// connection and then never answers — a queue manager going down or coming up, a proxy
    /// in front of a dead backend — fails the attempt at this bound instead of holding the
    /// supervisor forever, so the reconnect loop keeps going and the outage end is logged.
    pub operation_timeout: Duration,
    /// Per-message inbound auth (`inbound-auth.*`), resolved once at wiring. `None` =
    /// no inbound-auth declared. A rejected credential drops the delivery (AMQP 1.0
    /// settle-accepted — no redelivery, no dead-letter).
    pub inbound_auth: Option<BrokerInboundAuth>,
}

impl AmqpSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff, and the transport's 30 s
    /// operation bound on every attach and teardown step.
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
            operation_timeout: super::sink::DEFAULT_OPERATION_TIMEOUT,
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
    /// The link or connection failed; the outage window starts at the failure, not after the
    /// drain and teardown that follow it.
    ConnectionLost(OutageWindow),
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
    /// An intake call in the window returned: the delivery it answers for and its disposition.
    Finished(DeliveryInfo, DeliveryDisposition),
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
    /// session, close the connection — all of it within `bound`. Each step waits for the
    /// broker's answer, and a peer that is gone without closing the socket never sends one; past
    /// the bound the handles are dropped, which closes the connection without waiting.
    async fn teardown(mut self, bound: Duration) -> bool {
        let steps = async {
            let _ = self.receiver.close().await;
            let _ = self.session.end().await;
            let _ = self.connection.close().await;
        };
        tokio::time::timeout(bound, steps).await.is_ok()
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
    // The outage in progress, from the first failed connect or the lost connection until the
    // next attach. Both ends are logged, so the window can be read from the log.
    let mut outage: Option<OutageWindow> = None;
    // The same two ends, reported to the engine for the owning deployment (a no-op unless the
    // engine has connectivity events configured).
    let connectivity = intake.connectivity();
    let report = |code: &str, started: String, ended: Option<(String, u64)>| {
        if connectivity.is_enabled() {
            connectivity.report(inbound_connectivity_event(&config, code, started, ended));
        }
    };
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
        // Every step of the attach is bounded by the operation timeout, and a stop does not
        // wait for an attempt in progress.
        let opened = tokio::select! {
            _ = stop.notify.notified() => return,
            opened = open_session(&config) => opened,
        };
        match opened {
            Err(diagnostic) => {
                if outage.is_none() {
                    let window = OutageWindow::begin();
                    warn!(
                        channel = %config.channel,
                        code = codes::INBOUND_BROKER_OUTAGE_STARTED,
                        outage_started = %window.started(),
                        "amqp consumer has no broker connection; retrying"
                    );
                    report(codes::INBOUND_BROKER_OUTAGE_STARTED, window.started(), None);
                    outage = Some(window);
                }
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
                if let Some(window) = outage.take() {
                    let ended = window.end();
                    info!(
                        channel = %config.channel,
                        code = codes::INBOUND_BROKER_OUTAGE_ENDED,
                        outage_started = %ended.started,
                        outage_ended = %ended.ended,
                        outage_ms = ended.millis as u64,
                        "amqp consumer reconnected after an outage"
                    );
                    report(
                        codes::INBOUND_BROKER_OUTAGE_ENDED,
                        ended.started,
                        Some((ended.ended, ended.millis as u64)),
                    );
                }
                // First successful attach: release anyone blocked in `start()`. Fired once; later
                // reconnects find the receiver already taken and simply skip it.
                if let Some(tx) = attached.lock().await.take() {
                    let _ = tx.send(());
                }
                let end = run_session(&config, &intake, &gate, &stop, &mut broker).await;
                if !broker.teardown(config.operation_timeout).await {
                    debug!(
                        channel = %config.channel,
                        "amqp broker did not answer the detach within {:?}; the connection is \
                         dropped",
                        config.operation_timeout
                    );
                }
                match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::LeadershipLost => info!(
                        channel = %config.channel,
                        "amqp consumer detached — leadership lost"
                    ),
                    SessionEnd::ConnectionLost(window) => {
                        warn!(
                            channel = %config.channel,
                            code = codes::INBOUND_BROKER_OUTAGE_STARTED,
                            outage_started = %window.started(),
                            "amqp consumer stream failed — reconnecting"
                        );
                        report(codes::INBOUND_BROKER_OUTAGE_STARTED, window.started(), None);
                        outage = Some(window);
                    }
                }
            }
        }
    }
}

/// The connectivity event for one end of an inbound outage: `ended` carries the end time and
/// the length once the consumer is attached again.
fn inbound_connectivity_event(
    config: &AmqpSourceConfig,
    code: &str,
    started_at: String,
    ended: Option<(String, u64)>,
) -> ConnectivityEvent {
    let (ended_at, millis) = match ended {
        Some((at, ms)) => (Some(at), Some(ms)),
        None => (None, None),
    };
    ConnectivityEvent {
        tenant: config.tenant.clone(),
        module_key: config.module_key.clone(),
        code: code.to_string(),
        channel: config.channel.clone(),
        direction: ConnectivityDirection::Inbound,
        started_at,
        ended_at,
        millis,
        detail: format!(
            "amqp broker {}:{}",
            config.properties.host, config.properties.port
        ),
    }
}

/// Open the connection, begin a session, attach a receiver on the queue/topic. A broker
/// that is down, or that does not answer a step within the operation timeout, fails here
/// (NON-FATAL — the supervisor retries).
async fn open_session(config: &AmqpSourceConfig) -> Result<BrokerSession, Diagnostic> {
    let props = &config.properties;
    let address = props.source_address().ok_or_else(|| {
        Diagnostic::error(
            codes::INBOUND_QUEUE_MISSING,
            format!(
                "amqp channel '{}' has no queue/topic address",
                config.channel
            ),
        )
    })?;
    let container = unique_container_id(&format!("sutra-amqp-source-{}", config.channel));
    let connect = async {
        if props.tls {
            match props.tls_settings.client_config() {
                Ok(tls_config) => {
                    let target = AmqpBrokerTarget {
                        host: &props.host,
                        port: props.port,
                        username: props
                            .username
                            .as_deref()
                            .map(str::trim)
                            .filter(|user| !user.is_empty()),
                        password: props.password.as_deref().map(str::trim),
                    };
                    open_tls_connection(&container, &target, std::sync::Arc::new(tls_config)).await
                }
                Err(detail) => Err(AmqpConnectFailure::Tls(detail)),
            }
        } else {
            Connection::open(container.clone(), props.connection_uri().as_str())
                .await
                .map_err(classify_open_error)
        }
    };
    let opened = bounded_step(config, "opened the connection", connect).await?;
    let mut connection = opened.map_err(|failure| {
        let (code, detail) = match failure {
            AmqpConnectFailure::Tls(detail) => (codes::INBOUND_BROKER_TLS_FAILED, detail),
            AmqpConnectFailure::Auth(detail) => (codes::INBOUND_BROKER_AUTH_FAILED, detail),
            AmqpConnectFailure::Connection(detail) => (codes::INBOUND_CONNECTION_FAILED, detail),
        };
        Diagnostic::error(
            code,
            format!(
                "amqp channel '{}' could not connect to {}:{}: {detail}",
                config.channel, props.host, props.port
            ),
        )
    })?;
    let begun = bounded_step(config, "began a session", Session::begin(&mut connection)).await?;
    let mut session = begun.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONNECTION_FAILED,
            format!(
                "amqp channel '{}' could not begin a session: {e}",
                config.channel
            ),
        )
    })?;
    let source = {
        let builder = Source::builder().address(address.to_string());
        match props.address_type {
            Some(kind) => builder
                .capabilities(vec![Symbol::from(kind.capability())])
                .build(),
            None => builder.build(),
        }
    };
    let attach = Receiver::builder()
        .name(format!("{container}-recv"))
        .source(source)
        .credit_mode(CreditMode::Auto(link_credit(props)))
        .attach(&mut session);
    let receiver = bounded_step(config, "attached the receiver", attach)
        .await?
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

/// Run one broker round trip of an attach under the configured operation timeout. A broker that
/// accepts the connection and never answers fails the attempt here, so the supervisor retries
/// instead of waiting forever. `step` names what the broker did not answer, for the WARN.
async fn bounded_step<T>(
    config: &AmqpSourceConfig,
    step: &str,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, Diagnostic> {
    tokio::time::timeout(config.operation_timeout, fut)
        .await
        .map_err(|_| {
            Diagnostic::error(
                codes::INBOUND_CONNECTION_FAILED,
                format!(
                    "amqp channel '{}' got no answer from {}:{} within {} ms while it {step}",
                    config.channel,
                    config.properties.host,
                    config.properties.port,
                    config.operation_timeout.as_millis()
                ),
            )
        })
}

/// How long a session end waits for the intake calls still in flight — the operation timeout
/// the rest of the transport uses. A call still running at the bound is never settled: the
/// broker redelivers it and inbox dedup absorbs the copy.
const DRAIN_BOUND: Duration = Duration::from_secs(30);

/// The receiver link credit: `max(prefetch-count, 2 × max-in-flight)`, so a full window of
/// intake calls never waits for the broker to grant more credit.
fn link_credit(props: &AmqpChannelProperties) -> u32 {
    props
        .prefetch_count
        .max(props.max_in_flight.saturating_mul(2))
}

/// The consume loop: gate re-check per turn (and on an idle receive timeout), delivery →
/// projection → intake → disposition, interleaved with any DEFERRED dispositions the
/// `ack-mode: on-complete` settle callbacks posted since the last turn.
///
/// Up to `max-in-flight` intake calls run at once in an [`IntakeWindow`]; a delivery is settled
/// when its own call returns, in completion order. The loop stops receiving while the window is
/// full. Every session end drains the window first; only a stop settles what the drain
/// finished, because after a lost connection or lost leadership the link is gone and the broker
/// redelivers.
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
    let mut window: IntakeWindow<'_, DeliveryInfo> =
        IntakeWindow::new(config.properties.max_in_flight as usize);
    let end = loop {
        if stop.is_requested() {
            break SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            break SessionEnd::LeadershipLost;
        }
        let room = window.has_room();
        let busy = !window.is_empty();
        // `Receiver::recv` is documented cancel-safe, so losing the race to a stop signal, a
        // finished intake call or a queued deferred disposition never drops an in-flight
        // transfer. `IntakeWindow::next` is cancel-safe too: an unfinished call stays in the
        // window.
        let turn = tokio::select! {
            _ = stop.notify.notified() => Turn::Stopped,
            Some((info, disposition)) = window.next(), if busy => Turn::Finished(info, disposition),
            Some(command) = settle_rx.recv() => Turn::Settle(command),
            r = tokio::time::timeout(recv_timeout, broker.receiver.recv::<WireBody>()), if room => match r {
                Err(_elapsed) => Turn::Idle,
                Ok(Ok(delivery)) => Turn::Delivery(Box::new(delivery)),
                Ok(Err(e)) => Turn::RecvFailed(e.to_string()),
            },
            // A full window still re-checks the gate and stop while its calls run.
            _ = tokio::time::sleep(recv_timeout), if !room => Turn::Idle,
        };
        let delivery = match turn {
            Turn::Stopped => break SessionEnd::Stopped,
            Turn::Idle => continue, // idle gate/stop re-check
            Turn::Finished(info, disposition) => {
                if let Err(end) = settle_finished(config, &broker.receiver, info, disposition).await
                {
                    break end;
                }
                continue;
            }
            Turn::Settle(command) => {
                if let Err(end) = settle_deferred(config, &broker.receiver, command).await {
                    break end;
                }
                continue;
            }
            Turn::RecvFailed(e) => {
                warn!(channel = %config.channel, error = %e, "amqp recv failed");
                break SessionEnd::ConnectionLost(OutageWindow::begin());
            }
            Turn::Delivery(delivery) => *delivery,
        };
        let inbound = to_inbound_message(config, &delivery);
        // Per-message inbound auth: a rejected credential drops the delivery natively
        // (AMQP 1.0 settle-accepted — no redelivery, no dead-letter) and NEVER dispatches.
        // It settles inline and never enters the window.
        if let Some(auth) = &config.inbound_auth {
            if auth.verify(&inbound.headers) == InboundVerdict::Reject {
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_AUTH_REJECTED,
                    "amqp channel rejected a delivery — credential did not match expected"
                );
                DeliveryDropRecord::of(&inbound).audit(&Diagnostic::error(
                    codes::INBOUND_AUTH_REJECTED,
                    "the credential did not match the expected key",
                ));
                if let Err(end) =
                    settle(config, &broker.receiver, &delivery, AckDecision::Ack).await
                {
                    break end;
                }
                continue;
            }
        }
        // The intake owns ack-mode TIMING; the disposition mirrors the decision.
        // `on-persist` disposes when the call returns its decision; `on-complete` hands the
        // engine per-delivery settle callbacks — a PARKED instance defers the disposition to
        // its terminal event (the deferred-ack registry) and frees its window slot, a
        // run-to-completion dispatch disposes on return exactly like on-persist.
        let info = DeliveryInfo::from(&delivery);
        let call: BoxFuture<'_, DeliveryDisposition> =
            if config.properties.ack_mode == AckMode::OnComplete {
                let settle = deferred_settle(&config.channel, &delivery, &settle_tx);
                intake.deliver_deferred(inbound, settle)
            } else {
                let decision = intake.deliver(inbound);
                Box::pin(async move { DeliveryDisposition::Settle(decision.await) })
            };
        window.push(info, call);
    };
    drain_window(config, &broker.receiver, &mut window, end).await
}

/// End a session: wait (up to [`DRAIN_BOUND`]) for the intake calls still in flight, then
/// settle what finished on the still-open link when the session is stopping. After a lost
/// connection or lost leadership nothing is settled: the broker redelivers.
async fn drain_window(
    config: &AmqpSourceConfig,
    receiver: &Receiver,
    window: &mut IntakeWindow<'_, DeliveryInfo>,
    end: SessionEnd,
) -> SessionEnd {
    if window.is_empty() {
        return end;
    }
    let finished = window.drain(DRAIN_BOUND).await;
    if matches!(end, SessionEnd::Stopped) {
        for (info, disposition) in finished {
            if settle_finished(config, receiver, info, disposition)
                .await
                .is_err()
            {
                break; // the link is gone: the broker redelivers the rest
            }
        }
    } else if !finished.is_empty() {
        debug!(
            channel = %config.channel,
            finished = finished.len(),
            "amqp session ended with finished deliveries it can no longer settle; the broker \
             redelivers them (inbox dedup absorbs the copies)"
        );
    }
    if !window.is_empty() {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_RECEIVE_FAILED,
            unfinished = window.len(),
            "amqp session ended with intake calls still running after {:?}; their deliveries \
             stay unsettled and the broker redelivers them",
            DRAIN_BOUND
        );
    }
    end
}

/// Act on one finished intake call: settle its delivery now, or — when the engine deferred the
/// disposition to the instance's terminal event — only note it.
async fn settle_finished(
    config: &AmqpSourceConfig,
    receiver: &Receiver,
    info: DeliveryInfo,
    disposition: DeliveryDisposition,
) -> Result<(), SessionEnd> {
    match disposition {
        DeliveryDisposition::Settle(decision) => settle(config, receiver, info, decision).await,
        DeliveryDisposition::Deferred => {
            debug!(
                channel = %config.channel,
                delivery_id = info.delivery_id(),
                "delivery deferred — the amqp disposition is held (delivery stays \
                 UNSETTLED on the link) until the instance's terminal event"
            );
            Ok(())
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
    let delivery_id = delivery.delivery_id();
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
        return Err(SessionEnd::ConnectionLost(OutageWindow::begin()));
    }
    debug!(
        channel = %config.channel,
        delivery_id,
        decision = ?decision,
        "amqp disposition executed"
    );
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
    if let Some(properties) = &message.properties {
        // The message properties a reply or a correlating flow reads, under the names the sink
        // takes them back from. Written after the application properties, so they win a clash.
        // Trailing blanks are dropped: brokers with fixed-width names (IBM MQ's 48-character
        // ReplyToQ) pad them.
        let mut put = |name: &str, value: Option<String>| {
            if let Some(value) = value
                .map(|v| v.trim_end().to_string())
                .filter(|v| !v.is_empty())
            {
                headers.insert(name.to_string(), value);
            }
        };
        put(
            HEADER_MESSAGE_ID,
            properties.message_id.as_ref().and_then(present_id),
        );
        put(
            HEADER_CORRELATION_ID,
            properties.correlation_id.as_ref().and_then(present_id),
        );
        put(HEADER_REPLY_TO, properties.reply_to.clone());
        put(HEADER_SUBJECT, properties.subject.clone());
    }
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
/// `content-type` property → content type, absent when the sender stated none.
fn project_inbound(
    config: &AmqpSourceConfig,
    mut headers: BTreeMap<String, String>,
    message_id: Option<String>,
    coordinate: String,
    body: Vec<u8>,
) -> InboundMessage {
    // The broker's id, for a flow that declares it as its dedup key. Set here whatever a sender put
    // in its properties under this name.
    let native_id = message_id
        .as_deref()
        .map(|id| id.strip_prefix("ID:").unwrap_or(id).trim().to_string())
        .filter(|id| !id.is_empty());
    headers.retain(|name, _| !name.eq_ignore_ascii_case(sutra_channels::NATIVE_MESSAGE_ID_HEADER));
    if let Some(id) = native_id {
        headers.insert(sutra_channels::NATIVE_MESSAGE_ID_HEADER.to_string(), id);
    }
    // A sender that writes identifier property names (`property-names=identifier`) sends
    // `sutra_outbox_key` and `content_type`; both spellings are read.
    let named = |k: &str, name: &str| k.eq_ignore_ascii_case(name) || k == identifier_name(name);
    let outbox_key = headers
        .iter()
        .find(|(k, _)| named(k, PROPERTY_OUTBOX_KEY))
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
        .find(|(k, _)| named(k, PROPERTY_CONTENT_TYPE))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty());
    InboundMessage {
        tenant: config.tenant.clone(),
        module_key: config.module_key.clone(),
        channel: config.channel.clone(),
        headers,
        body: body.into(),
        content_type,
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
    let raw = render_message_id(id);
    let stripped = raw.strip_prefix("ID:").unwrap_or(&raw).to_string();
    Some(stripped).filter(|s| !s.is_empty())
}

/// An id worth passing on: rendered, unless it is a binary id of zero bytes only, which brokers
/// with fixed-width binary ids send for "none" (IBM MQ's `MQCI_NONE`).
fn present_id(id: &fe2o3_amqp::types::messaging::MessageId) -> Option<String> {
    use fe2o3_amqp::types::messaging::MessageId;
    match id {
        MessageId::Binary(b) if b.iter().all(|c| *c == 0) => None,
        other => Some(render_message_id(other)),
    }
}

/// Render a message-id or correlation-id as text. A binary id that is printable ASCII is read as
/// that text, as before; any other binary id is written as lower-case hex. Brokers that keep
/// their own 24-byte binary ids (and hand them to AMQP clients as binary) put arbitrary bytes
/// there, and decoding those as UTF-8 would map different ids onto the same replacement
/// characters, so two messages could share an idempotency key.
fn render_message_id(id: &fe2o3_amqp::types::messaging::MessageId) -> String {
    use fe2o3_amqp::types::messaging::MessageId;
    match id {
        MessageId::String(s) => s.clone(),
        MessageId::Ulong(u) => u.to_string(),
        MessageId::Uuid(u) => format!("{u:?}"),
        MessageId::Binary(b) => {
            if !b.is_empty() && b.iter().all(|c| (0x20..0x7f).contains(c)) {
                String::from_utf8_lossy(b).into_owned()
            } else {
                b.iter().map(|c| format!("{c:02x}")).collect()
            }
        }
    }
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
            tls_settings: Default::default(),
            username: None,
            password: None,
            queue: Some("in-q".to_string()),
            topic: None,
            prefetch_count: 10,
            max_in_flight: crate::amqp::DEFAULT_MAX_IN_FLIGHT,
            receive_timeout_ms: 1000,
            ack_mode: super::super::AckMode::OnPersist,
            singleton: false,
            address_type: None,
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

    /// The link credit covers twice the window, and never drops below `prefetch-count`.
    #[test]
    fn link_credit_covers_the_window() {
        let mut props = base_properties();
        assert_eq!(link_credit(&props), 32, "defaults: max(10, 2 x 16)");
        props.max_in_flight = 1;
        assert_eq!(
            link_credit(&props),
            10,
            "prefetch wins over a narrow window"
        );
        props.prefetch_count = 500;
        props.max_in_flight = 64;
        assert_eq!(link_credit(&props), 500);
        props.prefetch_count = 0;
        props.max_in_flight = 1024;
        assert_eq!(link_credit(&props), 2048);
    }

    #[test]
    fn the_message_id_is_handed_over_as_the_native_id_with_its_prefix_gone() {
        let m = project_inbound(
            &config(),
            headers(&[("sutra-outbox-key", "ob-in-1")]),
            Some("ID:provider-msg-99".to_string()),
            "in-q-3".to_string(),
            b"x".to_vec(),
        );
        assert_eq!(
            m.headers.get(sutra_channels::NATIVE_MESSAGE_ID_HEADER),
            Some(&"provider-msg-99".to_string())
        );
    }

    /// A sender can't supply it as a property and have it taken for the broker's.
    #[test]
    fn a_property_cannot_pose_as_the_native_id() {
        let m = project_inbound(
            &config(),
            headers(&[(sutra_channels::NATIVE_MESSAGE_ID_HEADER, "forged")]),
            None,
            "in-q-3".to_string(),
            b"x".to_vec(),
        );
        assert_eq!(
            m.headers.get(sutra_channels::NATIVE_MESSAGE_ID_HEADER),
            None
        );
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
    fn identifier_spellings_of_the_outbox_key_and_content_type_are_read() {
        let m = project_inbound(
            &config(),
            headers(&[
                ("sutra_outbox_key", "ob-in-2"),
                ("content_type", "application/xml"),
            ]),
            None,
            "in-q-4".to_string(),
            b"<a/>".to_vec(),
        );
        assert_eq!(m.idempotency_key, "ob-in-2");
        assert!(m.explicit_event_id);
        assert_eq!(m.content_type.as_deref(), Some("application/xml"));
    }

    #[test]
    fn binary_ids_render_as_text_when_printable_and_as_hex_otherwise() {
        use fe2o3_amqp::types::messaging::MessageId;
        use fe2o3_amqp::types::primitives::Binary;
        let printable = MessageId::Binary(Binary::from(b"ID:abc-1".to_vec()));
        assert_eq!(message_id_to_string(&printable).as_deref(), Some("abc-1"));
        // A 24-byte id of the kind brokers stamp themselves: text, then counter bytes.
        let mut raw = b"AMQ QM1     ".to_vec();
        raw.extend_from_slice(&[0x00, 0xff, 0x10, 0x80, 1, 2, 3, 4, 5, 6, 7, 8]);
        let opaque = MessageId::Binary(Binary::from(raw.clone()));
        let rendered = render_message_id(&opaque);
        assert_eq!(rendered.len(), 48);
        assert!(rendered.starts_with("414d5120514d3120"), "{rendered}");
        // Two ids differing only in non-text bytes stay distinct.
        raw[13] = 0xfe;
        let other = MessageId::Binary(Binary::from(raw));
        assert_ne!(render_message_id(&other), rendered);
        assert_eq!(render_message_id(&MessageId::String("s-1".into())), "s-1");
        assert_eq!(
            present_id(&MessageId::Binary(Binary::from(vec![0u8; 24]))),
            None
        );
        assert_eq!(present_id(&opaque), Some(rendered));
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
    fn an_unstated_content_type_stays_absent() {
        let m = project_inbound(&config(), headers(&[]), None, "c".to_string(), Vec::new());
        assert_eq!(m.content_type, None);
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

    /// A listener that takes every TCP connection and never says anything: the AMQP open waits
    /// for a header that never comes. Counts the connections it took.
    async fn silent_listener() -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = accepted.clone();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(socket);
            }
        });
        (port, accepted, task)
    }

    fn silent_config(port: u16, operation_timeout: Duration) -> AmqpSourceConfig {
        let mut props = base_properties();
        props.host = "127.0.0.1".to_string();
        props.port = port;
        props.password = Some("hunter2".to_string());
        props.username = Some("svc".to_string());
        let mut cfg = AmqpSourceConfig::new("acme", "acme/m/1.0.0", "silent-in", props);
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        cfg.gate_poll = Duration::from_millis(20);
        cfg.operation_timeout = operation_timeout;
        cfg
    }

    struct NoIntake;
    impl InboundIntake for NoIntake {
        fn deliver(&self, _m: InboundMessage) -> BoxFuture<'_, AckDecision> {
            Box::pin(async { AckDecision::Ack })
        }
    }

    fn spawn_supervisor(cfg: AmqpSourceConfig) -> (Arc<StopToken>, tokio::task::JoinHandle<()>) {
        let stop = Arc::new(StopToken::new());
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(supervise(
            cfg,
            Arc::new(NoIntake),
            Arc::new(sutra_channels::source::AlwaysLeading),
            Arc::clone(&stop),
            Arc::new(tokio::sync::Mutex::new(Some(tx))),
        ));
        (stop, task)
    }

    async fn wait_until(what: &str, check: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !check() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// An attach against a broker that takes the connection and never answers fails at the
    /// operation timeout with a coded, credential-free diagnostic, instead of waiting forever.
    #[tokio::test]
    async fn an_attach_to_a_broker_that_never_answers_fails_at_the_operation_timeout() {
        let (port, _accepted, listener) = silent_listener().await;
        let cfg = silent_config(port, Duration::from_millis(300));
        let outcome = tokio::time::timeout(Duration::from_secs(5), open_session(&cfg)).await;
        listener.abort();
        match outcome {
            Err(_) => panic!("the attach waited on a broker that never answers"),
            Ok(Ok(_)) => panic!("a silent broker cannot attach"),
            Ok(Err(d)) => {
                assert_eq!(d.code, codes::INBOUND_CONNECTION_FAILED, "{}", d.message);
                assert!(d.message.contains("got no answer"), "{}", d.message);
                assert!(d.message.contains("opened the connection"), "{}", d.message);
                assert!(d.message.contains("300 ms"), "{}", d.message);
                assert!(!d.message.contains("hunter2"), "{}", d.message);
            }
        }
    }

    /// The supervisor gives a silent attempt up and tries again, so a broker that comes back
    /// after a silent spell is attached to (and the outage end logged).
    #[tokio::test]
    async fn the_supervisor_keeps_retrying_a_broker_that_never_answers() {
        let (port, accepted, listener) = silent_listener().await;
        let (stop, task) = spawn_supervisor(silent_config(port, Duration::from_millis(100)));
        wait_until("three attach attempts", || {
            accepted.load(Ordering::SeqCst) >= 3
        })
        .await;
        stop.request();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the supervisor stops")
            .unwrap();
        listener.abort();
    }

    /// A stop does not wait for an attach attempt in progress to reach its bound.
    #[tokio::test]
    async fn a_stop_does_not_wait_for_an_attach_in_progress() {
        let (port, accepted, listener) = silent_listener().await;
        let (stop, task) = spawn_supervisor(silent_config(port, Duration::from_secs(60)));
        wait_until("the attach attempt", || {
            accepted.load(Ordering::SeqCst) >= 1
        })
        .await;
        // The attempt is waiting on the silent broker now.
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop.request();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the stop ends the supervisor without waiting out the attach bound")
            .unwrap();
        listener.abort();
    }

    /// The production default bounds an attach step by the transport's operation timeout.
    #[test]
    fn the_attach_bound_defaults_to_the_operation_timeout() {
        assert_eq!(
            config().operation_timeout,
            crate::amqp::sink::DEFAULT_OPERATION_TIMEOUT
        );
    }
}
