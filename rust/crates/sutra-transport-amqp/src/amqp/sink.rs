//! The AMQP 1.0 outbound sink — [`AmqpMessageSink`] implements [`MessageSink`] for the
//! `amqp10://[user:pass@]host[:port]/<destination>[?type=topic]` destination scheme.
//!
//! The wire projection is FROZEN — the `sutra-*` carriers below are a cross-broker contract,
//! and the native client imposes no identifier restriction on application-property keys:
//!
//! - the row's `outbox_key` rides the **`sutra-outbox-key`** application property (the
//!   shared cross-broker dedup token);
//! - the reply `content-type` lands on the **`content-type`** application property (declared
//!   → caller header → `application/octet-stream`);
//! - every other outbound header rides the message as an application property verbatim (the
//!   CloudEvents `ce-*` binary projection happens upstream, [`sutra_channels::outbox_dispatch`]
//!   `CeBinding::Amqp10` — the `ce-` DASH prefix); a present `traceparent` rides as a
//!   `traceparent` application property (trace-context bridge);
//! - the body is a single AMQP `Data` section (the raw reply bytes);
//! - the `message-id` property is the outbox key, unless an `x-amqp-message-id` header names
//!   another; the `x-amqp-correlation-id`, `x-amqp-reply-to` and `x-amqp-subject` headers fill
//!   the `correlation-id`, `reply-to` and `subject` properties instead of riding as application
//!   properties (the source writes those fields back under the same names).
//!
//! The destination query adds per-destination options, all off by default (see
//! [`super::AmqpSendOptions`]): `type=queue|topic` (the target capability), `durable`, `ttl-ms`,
//! `body=binary|text` (one `amqp-value` holding binary or a string instead of a `data` section) and
//! `property-names=identifier`.
//!
//! A send that cannot reach its broker starts an outage window for that broker, logged once as
//! `SUTRA.OUTBOUND.AMQP.OUTAGE_STARTED`; the next send that reaches it logs
//! `SUTRA.OUTBOUND.AMQP.OUTAGE_ENDED` with the start, the end and the length.
//!
//! Unlike Kafka the broker host rides the URI (per-authority). **The sink keeps its connections.**
//! Per broker (scheme, host, port and credentials) it holds a pool of at most
//! [`DEFAULT_MAX_CONNECTIONS`] connections (`SUTRA_SINK_AMQP_MAX_CONNECTIONS`), each with one
//! session (IBM MQ's AMQP channel allows one per connection) and a sender link per target address
//! (at most 32; the least recently used is detached past that). A send takes an idle connection,
//! reuses its link to the target or attaches one, and puts the connection back once the broker has
//! settled the message; past the bound it waits for one, for at most the operation timeout. A
//! connection that fails a send is dropped, and a send that failed on a reused connection is tried
//! once more on a fresh one at once (a broker restart leaves every pooled connection dead, which
//! is not an outage of the broker). One left idle for [`IDLE_CLOSE`] is closed. The transport's
//! drain closes them all ([`drain_all_sinks`]). Opening a connection, a session and a link per
//! message cost several broker round trips and, on IBM MQ, a channel instance per send.
//! Engine-wide default credentials (from `SUTRA_SINK_AMQP_{USERNAME,
//! PASSWORD}`) apply when the URI carries no userinfo (anonymous when both are empty).
//!
//! An `amqp10s://` destination is secured natively (see [`super::tls`]) under the engine-wide
//! trust settings (`SUTRA_SINK_AMQP_TLS_CA_FILE`, `SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY`). A
//! destination URI can hold a password, so every diagnostic here names it redacted.
//!
//! **Every broker round trip is bounded.** Opening the connection (TCP, TLS, AMQP open and SASL),
//! beginning the session, attaching the sender and waiting for the disposition each get at most
//! the sink's operation timeout ([`DEFAULT_OPERATION_TIMEOUT`], 30 s, or
//! `SUTRA_SINK_AMQP_OPERATION_TIMEOUT_MS`). A broker that stops answering fails the send with the
//! retryable `SUTRA.OUTBOUND.AMQP.SEND_TIMEOUT`, so the outbox sends again later instead of one
//! delivery worker waiting forever. A send that timed out waiting for its disposition may already
//! have been accepted, so the retry can deliver it twice; the `sutra-outbox-key` property is what a
//! receiver dedupes that on. Closing after the outcome is bounded the same way and never changes it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use sutra_channels::connectivity::{ConnectivityDirection, ConnectivityEvent, SendContext};

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{ApplicationProperties, Header, Message, Properties, Target};
use fe2o3_amqp::types::primitives::{Binary, Symbol};
use fe2o3_amqp::{Connection, Sender, Session};
use tokio::sync::Semaphore;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use tracing::{info, warn};

use super::outage::OutageWindow;
use super::tls::{
    classify_open_error, open_tls_connection, AmqpBrokerTarget, AmqpConnectFailure,
    AmqpTlsProperties,
};
use super::{
    codes, parse_destination, redact_destination, unique_container_id, AmqpDestination,
    AmqpSendOptions, BodySection, HEADER_CORRELATION_ID, HEADER_MESSAGE_ID, HEADER_REPLY_TO,
    HEADER_SUBJECT, PROPERTY_CONTENT_TYPE, PROPERTY_FIELD_HEADERS, PROPERTY_OUTBOX_KEY,
};
use super::{SCHEME, SCHEME_TLS};

/// The container-id fe2o3 announces on the AMQP `open` performative.
const CONTAINER_ID: &str = "sutra-amqp-sink";

/// How long one broker round trip of a send may take by default: the same bound the TLS connect
/// and handshake have.
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// The outbound AMQP 1.0 transport. `default_username`/`default_password` are the engine-wide
/// SASL credentials applied when a destination URI carries no userinfo.
pub struct AmqpMessageSink {
    default_username: Option<String>,
    default_password: Option<String>,
    /// The trust settings for `amqp10s://` destinations, or why they could not be read (then
    /// those sends fail closed rather than fall back to a weaker trust decision).
    tls_settings: Result<AmqpTlsProperties, String>,
    /// The loaded client configuration, so the CA bundle is read once rather than per send. A
    /// failed load is not cached.
    tls_config: Mutex<Option<Arc<rustls::ClientConfig>>>,
    /// The bound on each broker round trip of a send.
    operation_timeout: Duration,
    /// Brokers (`host:port`) that the last send could not reach, with when that started. The
    /// first send that reaches one again logs the window and removes it.
    outages: Mutex<HashMap<String, OutageWindow>>,
    /// The same windows per sending channel, `(broker, module key, channel)`, kept only while
    /// the engine collects connectivity events: each channel that sends during an outage
    /// reports its own down, and its own up on its first send that reaches the broker again.
    channel_outages: Mutex<HashMap<(String, String, String), OutageWindow>>,
    /// The pooled connections, per broker.
    pools: Arc<LanePools>,
}

impl AmqpMessageSink {
    /// A sink with the engine-wide default credentials (either may be empty/`None` ⇒
    /// anonymous / unset, in which case a destination URI's own userinfo — when present — is
    /// used).
    pub fn new(
        default_username: Option<String>,
        default_password: Option<String>,
    ) -> AmqpMessageSink {
        AmqpMessageSink {
            default_username: default_username.filter(|s| !s.is_empty()),
            default_password: default_password.filter(|s| !s.is_empty()),
            tls_settings: Ok(AmqpTlsProperties::default()),
            tls_config: Mutex::new(None),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            outages: Mutex::new(HashMap::new()),
            channel_outages: Mutex::new(HashMap::new()),
            pools: Self::new_pools(DEFAULT_MAX_CONNECTIONS),
        }
    }

    fn new_pools(max_connections: usize) -> Arc<LanePools> {
        let pools = Arc::new(LanePools::new(max_connections));
        let mut live = lock(live_pools());
        live.retain(|weak| weak.strong_count() > 0);
        live.push(Arc::downgrade(&pools));
        pools
    }

    /// Keep at most `max` connections to each broker instead of [`DEFAULT_MAX_CONNECTIONS`]
    /// (at least one).
    pub fn with_max_connections(mut self, max: usize) -> AmqpMessageSink {
        self.pools = Self::new_pools(max);
        self
    }

    /// How many broker connections this sink has opened so far, pooled or not. A send on a pooled
    /// connection opens none.
    pub fn connections_opened(&self) -> u64 {
        self.pools.opened.load(Ordering::Relaxed)
    }

    /// How many connections sit idle in the pool, over every broker.
    pub fn idle_connections(&self) -> usize {
        self.pools.idle_count()
    }

    /// Bound each broker round trip of a send by `timeout` instead of the default 30 s.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> AmqpMessageSink {
        self.operation_timeout = timeout;
        self
    }

    /// Set the trust settings for `amqp10s://` destinations. `Err` holds why the engine-wide
    /// settings are unusable; every TLS send then reports it.
    pub fn with_tls_settings(
        mut self,
        tls_settings: Result<AmqpTlsProperties, String>,
    ) -> AmqpMessageSink {
        self.tls_settings = tls_settings;
        self
    }

    /// The cached-or-fresh TLS client configuration.
    fn tls_client_config(&self) -> Result<Arc<rustls::ClientConfig>, Diagnostic> {
        let settings = self.tls_settings.as_ref().map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_CONFIG_INVALID,
                format!("amqp sink TLS settings are invalid: {e}"),
            )
        })?;
        let mut cache = self
            .tls_config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(config) = cache.as_ref() {
            return Ok(Arc::clone(config));
        }
        let config = settings.client_config().map(Arc::new).map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_BROKER_TLS_FAILED,
                format!("amqp sink could not load its TLS trust settings: {e}"),
            )
        })?;
        *cache = Some(Arc::clone(&config));
        Ok(config)
    }

    /// Close every pooled connection (each step bounded by the operation timeout). A send still
    /// running finishes and then closes its connection, and any later send uses a connection of
    /// its own, closed after the send: the drain never fails a send.
    pub async fn drain(&self) {
        close_pool(&self.pools, self.operation_timeout).await;
    }

    /// Fold the engine-wide default credentials into a destination that carries none.
    fn apply_default_credentials(&self, mut dest: AmqpDestination) -> AmqpDestination {
        if dest.username.is_none() {
            dest.username = self.default_username.clone();
            dest.password = self.default_password.clone();
        }
        dest
    }
}

impl MessageSink for AmqpMessageSink {
    fn schemes(&self) -> Vec<String> {
        // Distinct from rabbitmq's {"amqp","rabbitmq"} — AMQP 1.0 lives under `amqp10`.
        vec![SCHEME.to_string(), SCHEME_TLS.to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => self.apply_default_credentials(d),
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            if destination.options.body == BodySection::Text
                && std::str::from_utf8(&message.body).is_err()
            {
                return SendOutcome::PermanentFailure(not_text(&message.destination));
            }
            let tls_config = if destination.tls {
                match self.tls_client_config() {
                    Ok(config) => Some(config),
                    Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
                }
            } else {
                None
            };
            let (outcome, reach) = deliver(
                &self.pools,
                &destination,
                message,
                tls_config,
                self.operation_timeout,
            )
            .await;
            self.record_outage(&destination, &message.destination, reach);
            outcome
        })
    }
}

impl AmqpMessageSink {
    /// Track the outage window per broker: a send that could not open a connection starts one
    /// (logged once), the next send that opens one ends it (logged with start, end and length),
    /// whatever the broker then does with the message. A refused handshake or login leaves the
    /// window as it is.
    fn record_outage(&self, destination: &AmqpDestination, uri: &str, reach: Reach) {
        let broker = format!("{}:{}", destination.host, destination.port);
        let unreachable = reach == Reach::Unreachable;
        if reach == Reach::Unknown {
            return;
        }
        if let Some(context) = sutra_channels::connectivity::current_send() {
            self.report_channel_outage(&broker, unreachable, context);
        }
        let mut outages = self
            .outages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if unreachable {
            if let std::collections::hash_map::Entry::Vacant(slot) = outages.entry(broker) {
                let window = OutageWindow::begin();
                warn!(
                    code = codes::OUTBOUND_BROKER_OUTAGE_STARTED,
                    broker = %slot.key(),
                    destination = %redact_destination(uri),
                    outage_started = %window.started(),
                    "amqp sink cannot reach the broker; the outbox keeps sending"
                );
                slot.insert(window);
            }
        } else if let Some(window) = outages.remove(&broker) {
            let ended = window.end();
            info!(
                code = codes::OUTBOUND_BROKER_OUTAGE_ENDED,
                broker = %broker,
                destination = %redact_destination(uri),
                outage_started = %ended.started,
                outage_ended = %ended.ended,
                outage_ms = ended.millis as u64,
                "amqp sink reached the broker again"
            );
        }
    }
}

impl AmqpMessageSink {
    /// The per-channel half of [`Self::record_outage`]: report the sending channel's outage
    /// start and end to the engine for the deployment that sent.
    fn report_channel_outage(&self, broker: &str, unreachable: bool, context: SendContext) {
        let key = (
            broker.to_string(),
            context.module_key.clone(),
            context.channel.clone(),
        );
        let mut outages = self
            .channel_outages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let event = |code: &str, started_at: String, ended: Option<(String, u64)>| {
            let (ended_at, millis) = match ended {
                Some((at, ms)) => (Some(at), Some(ms)),
                None => (None, None),
            };
            ConnectivityEvent {
                tenant: context.tenant.clone(),
                module_key: context.module_key.clone(),
                code: code.to_string(),
                channel: context.channel.clone(),
                direction: ConnectivityDirection::Outbound,
                started_at,
                ended_at,
                millis,
                detail: format!("amqp broker {broker}"),
            }
        };
        if unreachable {
            if let std::collections::hash_map::Entry::Vacant(slot) = outages.entry(key) {
                let window = OutageWindow::begin();
                context.reporter.report(event(
                    codes::OUTBOUND_BROKER_OUTAGE_STARTED,
                    window.started(),
                    None,
                ));
                slot.insert(window);
            }
        } else if let Some(window) = outages.remove(&key) {
            let ended = window.end();
            context.reporter.report(event(
                codes::OUTBOUND_BROKER_OUTAGE_ENDED,
                ended.started,
                Some((ended.ended, ended.millis as u64)),
            ));
        }
    }
}

/// Run one broker round trip of a send under `timeout`. `Err` names the step for the diagnostic.
async fn bounded<T>(
    timeout: Duration,
    step: &str,
    redacted: &str,
    fut: impl Future<Output = T>,
) -> Result<T, SendOutcome> {
    tokio::time::timeout(timeout, fut).await.map_err(|_| {
        SendOutcome::RetryableFailure(Diagnostic::error(
            codes::OUTBOUND_SEND_TIMEOUT,
            format!(
                "amqp sink got no answer from {redacted} within {} ms while it {step}; the outbox \
                 sends again",
                timeout.as_millis()
            ),
        ))
    })
}

/// How many connections the sink keeps to one broker by default: the engine's default number of
/// outbox delivery workers, so a send never waits for a connection in normal operation.
pub const DEFAULT_MAX_CONNECTIONS: usize = 8;

/// A pooled connection left unused this long is closed instead of reused.
pub const IDLE_CLOSE: Duration = Duration::from_secs(120);

/// The most sender links one pooled connection keeps attached. Past it, the link used least
/// recently is detached.
const MAX_LINKS_PER_CONNECTION: usize = 32;

/// How often a send also closes the pooled connections that went idle on every broker.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// One broker as the pool tells them apart: the connection's scheme, authority and SASL
/// credentials. It holds a resolved password, so it has no `Debug`.
#[derive(Clone, PartialEq, Eq, Hash)]
struct BrokerKey {
    tls: bool,
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

impl BrokerKey {
    fn of(destination: &AmqpDestination) -> BrokerKey {
        BrokerKey {
            tls: destination.tls,
            host: destination.host.clone(),
            port: destination.port,
            username: destination.username.clone(),
            password: destination.password.clone(),
        }
    }
}

/// One pooled connection with its one session (IBM MQ's AMQP channel allows one session per
/// connection) and the sender links attached on it, one per target. A lane is used by one send at
/// a time: a send takes it out of the pool and puts it back when it is done with it.
struct Lane {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    senders: HashMap<String, (Sender, Instant)>,
    last_used: Instant,
}

impl Lane {
    /// Whether the lane can carry another send: its connection and session are still running and
    /// it has not sat idle past [`IDLE_CLOSE`].
    fn usable(&self, now: Instant) -> bool {
        !self.connection.is_closed()
            && !self.session.is_ended()
            && now.duration_since(self.last_used) < IDLE_CLOSE
    }

    /// Detach every link, end the session and close the connection, each step bounded.
    async fn close(mut self, timeout: Duration) {
        for (_, (sender, _)) in self.senders.drain() {
            let _ = tokio::time::timeout(timeout, sender.close()).await;
        }
        let _ = tokio::time::timeout(timeout, self.session.end()).await;
        let _ = tokio::time::timeout(timeout, self.connection.close()).await;
    }
}

/// The idle lanes to one broker, and the permits that bound how many lanes exist at once.
struct BrokerLanes {
    idle: Mutex<Vec<Lane>>,
    permits: Arc<Semaphore>,
}

/// Every broker's pooled connections for one sink.
struct LanePools {
    brokers: Mutex<HashMap<BrokerKey, Arc<BrokerLanes>>>,
    max_connections: usize,
    /// Set by [`AmqpMessageSink::drain`]: from then on a send closes its connection when it is
    /// done, as before pooling, instead of returning it to the pool.
    closed: AtomicBool,
    /// Connections opened so far (pooled or not).
    opened: AtomicU64,
    last_sweep: Mutex<Instant>,
}

impl LanePools {
    fn new(max_connections: usize) -> LanePools {
        LanePools {
            brokers: Mutex::new(HashMap::new()),
            max_connections: max_connections.max(1),
            closed: AtomicBool::new(false),
            opened: AtomicU64::new(0),
            last_sweep: Mutex::new(Instant::now()),
        }
    }

    fn broker(&self, key: &BrokerKey) -> Arc<BrokerLanes> {
        let mut brokers = lock(&self.brokers);
        Arc::clone(brokers.entry(key.clone()).or_insert_with(|| {
            Arc::new(BrokerLanes {
                idle: Mutex::new(Vec::new()),
                permits: Arc::new(Semaphore::new(self.max_connections)),
            })
        }))
    }

    /// The most recently used idle lane that can still carry a send; dead or expired ones met on
    /// the way are dropped (which closes them).
    fn take_idle(&self, broker: &BrokerLanes) -> Option<Lane> {
        let now = Instant::now();
        let mut idle = lock(&broker.idle);
        while let Some(lane) = idle.pop() {
            if lane.usable(now) {
                return Some(lane);
            }
        }
        None
    }

    /// Put a lane back for the next send, unless the sink is draining.
    fn give_back(&self, broker: &BrokerLanes, mut lane: Lane) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        lane.last_used = Instant::now();
        lock(&broker.idle).push(lane);
    }

    /// At most every [`SWEEP_INTERVAL`], drop the idle lanes on every broker that are dead or
    /// expired, so a broker nobody sends to any more does not keep its connections open.
    fn sweep(&self) {
        let now = Instant::now();
        {
            let mut last = lock(&self.last_sweep);
            if now.duration_since(*last) < SWEEP_INTERVAL {
                return;
            }
            *last = now;
        }
        let brokers: Vec<Arc<BrokerLanes>> = lock(&self.brokers).values().cloned().collect();
        for broker in brokers {
            lock(&broker.idle).retain(|lane| lane.usable(now));
        }
    }

    /// Every idle lane on every broker, taken out of the pool.
    fn take_all_idle(&self) -> Vec<Lane> {
        let brokers: Vec<Arc<BrokerLanes>> = lock(&self.brokers).values().cloned().collect();
        brokers
            .iter()
            .flat_map(|broker| std::mem::take(&mut *lock(&broker.idle)))
            .collect()
    }

    fn idle_count(&self) -> usize {
        let brokers: Vec<Arc<BrokerLanes>> = lock(&self.brokers).values().cloned().collect();
        brokers.iter().map(|b| lock(&b.idle).len()).sum()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Every live sink's pools, so the transport's drain can close their connections without the
/// sink registry handing the sinks back.
fn live_pools() -> &'static Mutex<Vec<Weak<LanePools>>> {
    static LIVE: OnceLock<Mutex<Vec<Weak<LanePools>>>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(Vec::new()))
}

/// Close the pooled connections of every AMQP 1.0 sink in the process (the transport's drain at
/// shutdown). A send still running finishes on its own connection and then closes it.
pub async fn drain_all_sinks(timeout: Duration) {
    let pools: Vec<Arc<LanePools>> = {
        let mut live = lock(live_pools());
        live.retain(|weak| weak.strong_count() > 0);
        live.iter().filter_map(Weak::upgrade).collect()
    };
    for pool in pools {
        close_pool(&pool, timeout).await;
    }
}

async fn close_pool(pool: &LanePools, timeout: Duration) {
    pool.closed.store(true, Ordering::Release);
    for lane in pool.take_all_idle() {
        lane.close(timeout).await;
    }
}

/// Deliver one message on a pooled connection to its broker.
///
/// - A send takes an idle connection to the broker, or opens one when none is idle and fewer than
///   the pool's bound exist; past the bound it waits, at most the operation timeout, for one to
///   come back.
/// - The sender link to the message's target is reused when the connection has one, attached
///   otherwise.
/// - A connection that fails a send is dropped. When it had been reused, the send is tried once
///   more on a fresh connection at once: a broker restart or an idle close leaves pooled
///   connections dead, and that is not an outage. A timeout is not retried at once (a fresh
///   connection would most likely wait as long); the outbox sends again later.
///
/// The delivery guarantee is the one the sink always had: [`SendOutcome::Delivered`] only on the
/// broker's `accepted`, everything else retryable or permanent. A send retried after a lost
/// connection may already have been accepted, and the `sutra-outbox-key` property is what a
/// receiver dedupes that on, as for any outbox retry.
async fn deliver(
    pools: &LanePools,
    destination: &AmqpDestination,
    message: &OutboundMessage,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
) -> (SendOutcome, Reach) {
    let redacted = redact_destination(&message.destination);
    pools.sweep();
    let broker = pools.broker(&BrokerKey::of(destination));
    let waited = bounded(
        timeout,
        "waited for a free connection to the broker",
        &redacted,
        Arc::clone(&broker.permits).acquire_owned(),
    )
    .await;
    let _permit = match waited {
        Ok(Ok(permit)) => permit,
        // Every connection was busy: that says nothing about whether the broker is there.
        Err(timed_out) => return (timed_out, Reach::Unknown),
        Ok(Err(_)) => unreachable!("the pool never closes its semaphore"),
    };

    if let Some(mut lane) = pools.take_idle(&broker) {
        let (outcome, healthy) =
            send_on_lane(&mut lane, destination, message, &redacted, timeout).await;
        if healthy {
            pools.give_back(&broker, lane);
            return (outcome, Reach::Reached);
        }
        drop(lane);
        let retry_at_once = matches!(
            &outcome,
            SendOutcome::RetryableFailure(d) if d.code != codes::OUTBOUND_SEND_TIMEOUT
        );
        if !retry_at_once {
            return (outcome, Reach::Unknown);
        }
        tracing::debug!(
            destination = %redacted,
            "amqp sink: a pooled connection failed a send; sending once more on a fresh one"
        );
    }

    let mut lane = match open_lane(pools, destination, tls_config, &redacted, timeout).await {
        Ok(lane) => lane,
        Err(failed) => return failed,
    };
    let (outcome, healthy) =
        send_on_lane(&mut lane, destination, message, &redacted, timeout).await;
    if healthy {
        pools.give_back(&broker, lane);
    }
    (outcome, Reach::Reached)
}

/// Open a connection and begin its session. `Err` carries the send's outcome and what it says
/// about the broker's reachability.
async fn open_lane(
    pools: &LanePools,
    destination: &AmqpDestination,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    redacted: &str,
    timeout: Duration,
) -> Result<Lane, (SendOutcome, Reach)> {
    let container_id = unique_container_id(CONTAINER_ID);
    let open = async {
        match tls_config {
            Some(config) => {
                let target = AmqpBrokerTarget {
                    host: &destination.host,
                    port: destination.port,
                    username: destination.username.as_deref(),
                    password: destination.password.as_deref(),
                };
                open_tls_connection(&container_id, &target, config).await
            }
            None => Connection::open(container_id.as_str(), destination.connection_uri().as_str())
                .await
                .map_err(classify_open_error),
        }
    };
    let opened = match bounded(timeout, "opened the connection", redacted, open).await {
        Ok(opened) => opened,
        Err(timed_out) => return Err((timed_out, Reach::Unreachable)),
    };
    let mut connection = match opened {
        Ok(c) => c,
        Err(failure) => {
            let (code, detail, reach) = match failure {
                AmqpConnectFailure::Tls(detail) => {
                    (codes::OUTBOUND_BROKER_TLS_FAILED, detail, Reach::Unknown)
                }
                AmqpConnectFailure::Auth(detail) => {
                    (codes::OUTBOUND_BROKER_AUTH_FAILED, detail, Reach::Unknown)
                }
                AmqpConnectFailure::Connection(detail) => {
                    (codes::OUTBOUND_SEND_FAILED, detail, Reach::Unreachable)
                }
            };
            let outcome = SendOutcome::RetryableFailure(Diagnostic::error(
                code,
                format!("amqp sink could not connect to {redacted}: {detail}"),
            ));
            return Err((outcome, reach));
        }
    };
    pools.opened.fetch_add(1, Ordering::Relaxed);
    let begun = bounded(
        timeout,
        "began a session",
        redacted,
        Session::begin(&mut connection),
    )
    .await;
    let failed = match begun {
        Ok(Ok(session)) => {
            return Ok(Lane {
                connection,
                session,
                senders: HashMap::new(),
                last_used: Instant::now(),
            })
        }
        Err(timed_out) => timed_out,
        Ok(Err(e)) => SendOutcome::RetryableFailure(Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("amqp sink could not begin a session to {redacted}: {e}"),
        )),
    };
    let _ = bounded(
        timeout,
        "closed the connection",
        redacted,
        connection.close(),
    )
    .await;
    Err((failed, Reach::Reached))
}

/// The key a lane keeps a sender link under: the target address and its capability.
fn link_key(destination: &AmqpDestination) -> String {
    match destination.options.address_type {
        Some(kind) => format!("{}?type={}", destination.address, kind.capability()),
        None => destination.address.clone(),
    }
}

/// The link name: unique on its connection, which holds at most one link per [`link_key`].
fn link_name(destination: &AmqpDestination) -> String {
    match destination.options.address_type {
        Some(kind) => format!(
            "{CONTAINER_ID}-{}-{}",
            destination.address,
            kind.capability()
        ),
        None => format!("{CONTAINER_ID}-{}", destination.address),
    }
}

/// Send the projected message on `lane`, attaching a sender for its target when the lane has
/// none. Returns the outcome and whether the lane is fit to carry the next send: it is after a
/// settled send (accepted or not) and after a refusal before anything went on the wire; it is not
/// after a link, session, connection or timeout failure.
async fn send_on_lane(
    lane: &mut Lane,
    destination: &AmqpDestination,
    message: &OutboundMessage,
    redacted: &str,
    timeout: Duration,
) -> (SendOutcome, bool) {
    let key = link_key(destination);
    let mut sender = match lane.senders.remove(&key) {
        Some((sender, _)) => sender,
        None => {
            if lane.senders.len() >= MAX_LINKS_PER_CONNECTION {
                let oldest = lane
                    .senders
                    .iter()
                    .min_by_key(|(_, (_, used))| *used)
                    .map(|(k, _)| k.clone());
                if let Some(oldest) = oldest {
                    // Dropping a sender detaches its link.
                    lane.senders.remove(&oldest);
                }
            }
            let attached = bounded(
                timeout,
                "attached a sender",
                redacted,
                Sender::builder()
                    .name(link_name(destination))
                    .target(target_for(destination))
                    .attach(&mut lane.session),
            )
            .await;
            match attached {
                Ok(Ok(sender)) => sender,
                Err(timed_out) => return (timed_out, false),
                Ok(Err(e)) => {
                    return (
                        SendOutcome::RetryableFailure(Diagnostic::error(
                            codes::OUTBOUND_SEND_FAILED,
                            format!("amqp sink could not attach a sender to {redacted}: {e}"),
                        )),
                        false,
                    )
                }
            }
        }
    };

    let sent = bounded(
        timeout,
        "waited for the broker to settle the message",
        redacted,
        send_projected(&mut sender, destination, message),
    )
    .await;
    let (outcome, healthy) = match sent {
        Err(timed_out) => (timed_out, false),
        Ok(Err(SendError::Permanent(diagnostic))) => {
            (SendOutcome::PermanentFailure(diagnostic), true)
        }
        Ok(Err(SendError::Broker(e))) => (
            SendOutcome::RetryableFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                format!("amqp publish to {redacted} failed: {e}"),
            )),
            false,
        ),
        Ok(Ok(outcome)) => {
            if outcome.is_accepted() {
                (SendOutcome::Delivered, true)
            } else {
                (
                    SendOutcome::RetryableFailure(Diagnostic::error(
                        codes::OUTBOUND_SEND_FAILED,
                        format!(
                            "amqp publish to {redacted} was not accepted (disposition: \
                             {outcome:?})"
                        ),
                    )),
                    true,
                )
            }
        }
    };
    if healthy {
        lane.senders.insert(key, (sender, Instant::now()));
    }
    (outcome, healthy)
}

/// Whether a send reached its broker, for the outage window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    /// A connection was open: the broker is there, whatever it then did with the message.
    Reached,
    /// The connection could not be opened, or opening it timed out.
    Unreachable,
    /// The broker answered but refused the TLS handshake or the login, every pooled connection
    /// was busy, or a pooled connection timed out: says nothing either way.
    Unknown,
}

/// The target terminus for a destination: its address, plus the `queue` / `topic` capability
/// when the destination names its type (`?type=`).
fn target_for(destination: &AmqpDestination) -> Target {
    let builder = Target::builder().address(destination.address.clone());
    match destination.options.address_type {
        Some(kind) => builder
            .capabilities(vec![Symbol::from(kind.capability())])
            .build(),
        None => builder.build(),
    }
}

/// Why a projected send did not get a disposition.
enum SendError {
    /// The message cannot be built under the destination's options; a retry cannot fix it.
    Permanent(Diagnostic),
    /// The broker or the link failed the send.
    Broker(fe2o3_amqp::link::SendError),
}

/// Build the message under the destination's options and send it.
async fn send_projected(
    sender: &mut Sender,
    destination: &AmqpDestination,
    message: &OutboundMessage,
) -> Result<fe2o3_amqp::types::messaging::Outcome, SendError> {
    let options = &destination.options;
    let application_properties = (!options.omit_application_properties)
        .then(|| build_application_properties(message, options));
    let builder = Message::builder()
        .header(build_header(options))
        .properties(build_properties(message, &destination.address))
        .application_properties(application_properties);
    match options.body {
        BodySection::Data => sender
            .send(builder.data(message.body.clone()).build())
            .await
            .map_err(SendError::Broker),
        BodySection::Binary => sender
            .send(builder.value(Binary::from(message.body.clone())).build())
            .await
            .map_err(SendError::Broker),
        BodySection::Text => {
            let text = String::from_utf8(message.body.clone())
                .map_err(|_| SendError::Permanent(not_text(&message.destination)))?;
            sender
                .send(builder.value(text).build())
                .await
                .map_err(SendError::Broker)
        }
    }
}

/// The refusal of a `body=text` send whose payload is not UTF-8. A retry cannot fix it.
fn not_text(destination: &str) -> Diagnostic {
    Diagnostic::error(
        codes::OUTBOUND_CONFIG_INVALID,
        format!(
            "amqp destination '{}' sends its body as text (body=text), but the payload is not \
             UTF-8",
            redact_destination(destination)
        ),
    )
}

/// The message header, present only when an option sets one of its fields.
fn build_header(options: &AmqpSendOptions) -> Option<Header> {
    if options.durable.is_none() && options.ttl_ms.is_none() {
        return None;
    }
    Some(
        Header::builder()
            .durable(options.durable.unwrap_or(false))
            .ttl(options.ttl_ms)
            .build(),
    )
}

/// The message properties: `to` is the destination address, `message-id` is the outbox key unless
/// the message names its own with the `x-amqp-message-id` header; `x-amqp-correlation-id`,
/// `x-amqp-reply-to` and
/// `x-amqp-subject` fill the fields of the same name. The same names are what the source writes
/// those fields into, so a received message's ids can be passed on as they are.
fn build_properties(message: &OutboundMessage, address: &str) -> Properties {
    let header = |name: &str| {
        message
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let message_id = header(HEADER_MESSAGE_ID).unwrap_or_else(|| message.outbox_key.clone());
    // `to` repeats the link's target address: some brokers take the destination from the message
    // rather than the link (IBM MQ refuses a message without it, AMQXR0032E).
    let mut builder = Properties::builder()
        .message_id(message_id)
        .to(address.to_string());
    if let Some(correlation_id) = header(HEADER_CORRELATION_ID) {
        builder = builder.correlation_id(correlation_id);
    }
    if let Some(reply_to) = header(HEADER_REPLY_TO) {
        builder = builder.reply_to(reply_to);
    }
    if let Some(subject) = header(HEADER_SUBJECT) {
        builder = builder.subject(subject);
    }
    builder.build()
}

/// The FROZEN application-property table (unit-tested independently of a broker):
/// `sutra-outbox-key` = the outbox key, `content-type` = the resolved content type, every other
/// header verbatim (the two owned properties de-duplicated case-insensitively, the
/// [`PROPERTY_FIELD_HEADERS`] left to the message properties), plus `traceparent` when present.
/// Under `property-names=identifier` every name is written as an identifier.
fn build_application_properties(
    message: &OutboundMessage,
    options: &AmqpSendOptions,
) -> ApplicationProperties {
    let content_type = resolve_content_type(message);
    let name = |n: &str| options.property_name(n);
    let mut builder = ApplicationProperties::builder()
        .insert(name(PROPERTY_OUTBOX_KEY), message.outbox_key.as_str())
        .insert(name(PROPERTY_CONTENT_TYPE), content_type.as_str());
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(PROPERTY_OUTBOX_KEY)
            || key.eq_ignore_ascii_case(PROPERTY_CONTENT_TYPE)
            || PROPERTY_FIELD_HEADERS
                .iter()
                .any(|h| key.eq_ignore_ascii_case(h))
        {
            continue;
        }
        builder = builder.insert(name(key), value.as_str());
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            builder = builder.insert(name(TRACEPARENT_HEADER), traceparent.as_str());
        }
    }
    builder.build()
}

/// Content-type resolution: declared content type wins, then a caller-supplied
/// `content-type` header (ASCII case-insensitive), then `application/octet-stream`.
fn resolve_content_type(message: &OutboundMessage) -> String {
    if let Some(declared) = &message.content_type {
        if !declared.trim().is_empty() {
            return declared.clone();
        }
    }
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(PROPERTY_CONTENT_TYPE) {
            return value.clone();
        }
    }
    "application/octet-stream".to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fe2o3_amqp::types::primitives::SimpleValue;

    use super::*;

    fn message(destination: &str) -> OutboundMessage {
        OutboundMessage {
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: b"{\"ok\":true}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "outbox-abc-123".to_string(),
            traceparent: None,
        }
    }

    /// Read one application-property value as a string (test helper).
    fn prop(ap: &ApplicationProperties, key: &str) -> Option<String> {
        ap.0.iter().find_map(|(k, v)| {
            if k == key {
                match v {
                    SimpleValue::String(s) => Some(s.clone()),
                    other => Some(format!("{other:?}")),
                }
            } else {
                None
            }
        })
    }

    fn count(ap: &ApplicationProperties, key: &str) -> usize {
        ap.0.iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(key))
            .count()
    }

    #[test]
    fn outbox_key_rides_the_sutra_outbox_key_property_and_content_type_is_set() {
        // The m9 outbox-key sharing invariant — the AMQP 1.0 sink carries `sutra-outbox-key`.
        let ap = build_application_properties(
            &message("amqp10://broker:5672/payment-replies"),
            &AmqpSendOptions::default(),
        );
        assert_eq!(
            prop(&ap, PROPERTY_OUTBOX_KEY).as_deref(),
            Some("outbox-abc-123")
        );
        assert_eq!(
            prop(&ap, PROPERTY_CONTENT_TYPE).as_deref(),
            Some("application/json")
        );
    }

    #[test]
    fn identifier_property_names_rewrite_every_application_property() {
        let mut m = message("amqp10://broker/replies?property-names=identifier");
        m.headers.insert("ce-type".to_string(), "t".to_string());
        m.traceparent = Some("tp".to_string());
        let options = parse_destination(&m.destination).unwrap().options;
        let ap = build_application_properties(&m, &options);
        let names: Vec<&str> = ap.0.keys().map(String::as_str).collect();
        for name in ["sutra_outbox_key", "content_type", "ce_type", "traceparent"] {
            assert!(names.contains(&name), "{name} in {names:?}");
        }
        assert!(names.iter().all(|n| !n.contains('-')), "{names:?}");
    }

    #[test]
    fn message_properties_carry_the_outbox_key_and_the_lifted_headers() {
        use fe2o3_amqp::types::messaging::MessageId;
        let mut m = message("amqp10://broker/replies");
        let p = build_properties(&m, "replies");
        assert_eq!(p.to.as_deref(), Some("replies"));
        assert_eq!(
            p.message_id,
            Some(MessageId::String("outbox-abc-123".to_string()))
        );
        assert_eq!(p.correlation_id, None);
        m.headers
            .insert("X-AMQP-Correlation-Id".to_string(), "corr-7".to_string());
        m.headers
            .insert(HEADER_REPLY_TO.to_string(), "REPLY.Q".to_string());
        m.headers
            .insert(HEADER_SUBJECT.to_string(), "subj".to_string());
        m.headers
            .insert(HEADER_MESSAGE_ID.to_string(), "own-id".to_string());
        let p = build_properties(&m, "replies");
        assert_eq!(p.message_id, Some(MessageId::String("own-id".to_string())));
        assert_eq!(
            p.correlation_id,
            Some(MessageId::String("corr-7".to_string()))
        );
        assert_eq!(p.reply_to.as_deref(), Some("REPLY.Q"));
        assert_eq!(p.subject.as_deref(), Some("subj"));
        // The lifted headers do not also ride as application properties.
        let ap = build_application_properties(&m, &AmqpSendOptions::default());
        for name in PROPERTY_FIELD_HEADERS {
            assert_eq!(count(&ap, name), 0, "{name}");
        }
    }

    #[test]
    fn the_header_is_sent_only_when_an_option_sets_it() {
        assert!(build_header(&AmqpSendOptions::default()).is_none());
        let options = AmqpSendOptions::from_query(Some("durable=true&ttl-ms=1500")).unwrap();
        let header = build_header(&options).expect("header");
        assert!(header.durable);
        assert_eq!(header.ttl, Some(1500));
        let options = AmqpSendOptions::from_query(Some("ttl-ms=10")).unwrap();
        assert!(!build_header(&options).expect("header").durable);
    }

    #[test]
    fn the_target_names_its_capability_only_when_the_type_is_given() {
        let plain = target_for(&parse_destination("amqp10://b/APP.Q").unwrap());
        assert_eq!(plain.capabilities, None);
        let queue = target_for(&parse_destination("amqp10://b/APP.Q?type=queue").unwrap());
        let caps: Vec<String> = queue
            .capabilities
            .expect("capabilities")
            .0
            .into_iter()
            .map(|s| s.0)
            .collect();
        assert_eq!(caps, vec!["queue".to_string()]);
    }

    #[tokio::test]
    async fn a_text_body_that_is_not_utf8_is_a_permanent_failure() {
        // Refused before any connection: the port refuses at once, so a connection attempt would
        // show as a retryable failure instead.
        let sink = AmqpMessageSink::new(None, None);
        let mut m = message("amqp10://127.0.0.1:1/q?body=text");
        m.body = vec![0xff, 0xfe];
        match sink.send(&m).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID),
            other => panic!("expected a permanent failure, got {other:?}"),
        }
        let bad = message("amqp10://127.0.0.1:1/q?body=xml");
        match sink.send(&bad).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID),
            other => panic!("expected a permanent failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unreachable_broker_opens_one_outage_window() {
        let sink = AmqpMessageSink::new(None, None);
        let m = message("amqp10://127.0.0.1:1/q");
        for _ in 0..2 {
            let _ = sink.send(&m).await;
        }
        let outages = sink.outages.lock().unwrap();
        assert_eq!(outages.len(), 1);
        assert!(outages.contains_key("127.0.0.1:1"));
    }

    #[test]
    fn ce_dash_attributes_and_headers_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("amqp10://broker/replies");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        // The AMQP 1.0 CE binding prefix is `ce-` (projected upstream, carried verbatim here).
        m.headers
            .insert("ce-type".to_string(), "payment.reply".to_string());
        m.headers.insert("ce-id".to_string(), "evt-9".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let ap = build_application_properties(&m, &AmqpSendOptions::default());
        assert_eq!(prop(&ap, "x-tenant").as_deref(), Some("acme"));
        assert_eq!(prop(&ap, "ce-type").as_deref(), Some("payment.reply"));
        assert_eq!(prop(&ap, "ce-id").as_deref(), Some("evt-9"));
        assert_eq!(
            prop(&ap, "traceparent").as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("amqp10://broker/replies");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let ap = build_application_properties(&m, &AmqpSendOptions::default());
        assert_eq!(prop(&ap, "traceparent").as_deref(), Some("explicit"));
        assert_eq!(count(&ap, "traceparent"), 1);
    }

    #[test]
    fn caller_supplied_owned_properties_do_not_duplicate() {
        let mut m = message("amqp10://broker/replies");
        m.headers
            .insert("Sutra-Outbox-Key".to_string(), "stale".to_string());
        m.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let ap = build_application_properties(&m, &AmqpSendOptions::default());
        assert_eq!(
            count(&ap, PROPERTY_OUTBOX_KEY),
            1,
            "exactly one outbox-key property"
        );
        assert_eq!(
            prop(&ap, PROPERTY_OUTBOX_KEY).as_deref(),
            Some("outbox-abc-123"),
            "the owned outbox key wins over a stale caller header"
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("amqp10://broker/replies");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");

        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[test]
    fn sink_claims_only_the_amqp10_schemes() {
        let sink = AmqpMessageSink::new(None, None);
        assert_eq!(
            sink.schemes(),
            vec!["amqp10".to_string(), "amqp10s".to_string()]
        );
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = AmqpMessageSink::new(None, None);
        for bad in ["amqp10://broker:5672/", "amqp://broker/q", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(_) => {}
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unusable_tls_settings_fail_tls_sends_closed_and_name_the_setting() {
        let sink = AmqpMessageSink::new(None, None)
            .with_tls_settings(Err("SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY".to_string()));
        match sink.send(&message("amqp10s://broker:5671/q")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID);
                assert!(d
                    .message
                    .contains("SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY"));
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unloadable_ca_bundle_is_a_retryable_tls_failure() {
        let sink = AmqpMessageSink::new(None, None).with_tls_settings(Ok(AmqpTlsProperties {
            ca_file: Some("/nonexistent/ca.pem".to_string()),
            ..AmqpTlsProperties::default()
        }));
        match sink.send(&message("amqp10s://broker:5671/q")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_BROKER_TLS_FAILED)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_broker_that_never_answers_is_a_retryable_coded_timeout() {
        // A listener that accepts the TCP connection and then says nothing: the AMQP open waits for
        // a header that never comes.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let sink =
            AmqpMessageSink::new(None, None).with_operation_timeout(Duration::from_millis(300));
        let started = std::time::Instant::now();
        let outcome = sink
            .send(&message(&format!(
                "amqp10://svc:hunter2@127.0.0.1:{port}/q"
            )))
            .await;
        silent.abort();
        match outcome {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_SEND_TIMEOUT, "{}", d.message);
                assert!(d.message.contains("opened the connection"), "{}", d.message);
                assert!(!d.message.contains("hunter2"), "{}", d.message);
            }
            other => panic!("expected a retryable timeout, got {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(5), "bounded");
    }

    #[tokio::test]
    async fn the_operation_timeout_defaults_to_the_connect_bound() {
        assert_eq!(
            AmqpMessageSink::new(None, None).operation_timeout,
            DEFAULT_OPERATION_TIMEOUT
        );
    }

    #[tokio::test]
    async fn a_connect_failure_never_names_the_uri_password() {
        // Port 1 on loopback refuses at once, and the send reports it with the userinfo removed.
        let sink = AmqpMessageSink::new(None, None);
        match sink
            .send(&message("amqp10://svc-user:hunter2@127.0.0.1:1/q"))
            .await
        {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED);
                assert!(!d.message.contains("hunter2"), "{}", d.message);
                assert!(!d.message.contains("svc-user"), "{}", d.message);
                assert!(d.message.contains("127.0.0.1:1"), "{}", d.message);
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }
}
