//! The Kafka inbound trigger — [`KafkaTriggerSource`] implements [`TriggerSource`]: one
//! `group.id` consumer per channel binding, records projected into [`InboundMessage`]s and
//! pushed through the [`InboundIntake`] seam, the returned [`AckDecision`] executed as an
//! offset action (commit / seek-back).
//!
//! The Kafka trigger-source lifecycle:
//!
//! - **Broker absence is NON-FATAL**: `start` spawns a supervisor task and resolves;
//!   a missing broker WARNs (`SUTRA.INBOUND.KAFKA.CONSUMER_FAILED`) and the supervisor
//!   retries with exponential backoff in the background — readiness is unaffected.
//! - **Ack timing rides the intake seam**: the source `await`s
//!   [`InboundIntake::deliver`] and maps the decision onto Kafka's offset model — `Ack` →
//!   commit the record offset (advance), `NackDrop` → commit (skip the poison; Kafka has
//!   no DLQ, so advancing IS the drop), `NackRequeue` → seek back to the record offset so
//!   the next poll re-reads it (redelivery; inbox dedup absorbs the duplicate). Offsets
//!   are committed manually (`enable.auto.commit=false`) so a nack never advances.
//! - **`ack-mode: on-complete` is WIRED**: see "Deferred acking" below.
//! - **Singleton gating**: the consumer subscribes ONLY while `gate.is_leading()`; on
//!   leadership loss it drops the consumer (leaving the group so the partitions rebalance),
//!   then re-subscribes when the gate returns. The engine per-channel lease — not the Kafka
//!   consumer group — is what makes a `singleton: true` channel consume on exactly one
//!   replica.
//!
//! Idempotency key: the `sutra-outbox-key` record header when set
//! (`explicit_event_id = true`); otherwise the synthetic `topic-partition-offset`
//! coordinate (NON-explicit, so it never suppresses a re-post through inbox dedup).
//!
//! # Deferred acking (`ack-mode: on-complete`)
//!
//! Under `on-complete` the source calls [`InboundIntake::deliver_deferred`] with a
//! [`DeferredSettle`] pair; a PARKED instance answers [`DeliveryDisposition::Deferred`] and
//! the engine's deferred-ack registry fires one of the callbacks at the instance's terminal
//! event. Kafka's offset model forces two decisions the other brokers do not face:
//!
//! **(1) The settle is bridged back to the consumer task, never executed on the callback's
//! thread.** The callbacks fire on the engine actor thread or the registry sweep task; they
//! only enqueue a `DeferredSettleRequest` on an unbounded channel that the session loop
//! drains. `rdkafka`'s consumer handles ARE `Send + Sync` and `commit` takes `&self`, so
//! sharing an `Arc<StreamConsumer>` into the closures would compile — but it would keep the
//! consumer (and its group membership) ALIVE past `drop(consumer)` on leadership loss, and
//! `CommitMode::Sync` would block the single engine actor thread on a broker round-trip.
//! Bridging keeps both invariants: the callback is a non-blocking enqueue, and a settle
//! arriving after the session died finds a closed channel and WARNs (the dead-channel
//! posture — never a panic, never a commit on a partition this consumer no longer owns).
//!
//! **(2) Offsets commit at the per-partition LOW WATERMARK, not per record.** A Kafka commit
//! of offset `N+1` implicitly commits everything below it, and under `on-complete` a parked
//! instance holds its record while the loop keeps consuming — so per-record commits would
//! routinely commit PAST a still-parked record (making the `on-complete` guarantee a lie)
//! and, when a low offset settles after a high one, would move the committed offset
//! BACKWARDS (Kafka accepts any value), replaying a window of already-terminal records after
//! a restart. `OffsetTracker` therefore tracks per partition which offsets are in flight
//! and which have settled, and commits `1 + the highest settled offset below the lowest
//! in-flight offset` — the only faithful encoding of "everything up to here is terminal" in
//! Kafka's single-scalar commit. HONEST CONSEQUENCES:
//!
//! - Records that terminate AFTER a still-parked earlier record stay uncommitted until the
//!   parked one settles. On a crash/rebalance in that window they REDELIVER; inbox dedup
//!   absorbs the duplicates (at-least-once, the correct trade).
//! - One never-terminating instance pins its partition's commit point until the registry
//!   timeout (`sutra.ack.deferred.timeout`, default 1 h) nacks it — head-of-line blocking of
//!   the COMMIT POINT only, never of consumption.
//! - Ack and nack perform the SAME offset action (commit/advance): Kafka has no per-record
//!   reject, so `NackDrop` IS "commit past the poison" (the ack mapping the immediate path
//!   already uses). A deferred nack therefore does NOT redeliver — the FAILED instance is
//!   the record of the failure, and a dead-letter topic is an explicit BPMN wiring choice.
//! - `NackRequeue` never reaches the deferred path ([`DeferredSettle`] has only ack/nack);
//!   the immediate path's seek-back keeps the offset IN FLIGHT so the watermark cannot step
//!   over a record Kafka has been asked to re-read.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use sutra_channels::auth::{BrokerInboundAuth, InboundScheme, InboundVerdict};
use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{
    codes, AckMode, KafkaChannelProperties, HEADER_CONTENT_TYPE, HEADER_OUTBOX_KEY, TRANSPORT,
};

/// Everything one consumer needs, prepared by the wiring.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Broker/topic/group/ack-mode properties.
    pub properties: KafkaChannelProperties,
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

impl KafkaSourceConfig {
    /// Production defaults: 1s gate poll, 1s→30s reconnect backoff.
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: KafkaChannelProperties,
    ) -> KafkaSourceConfig {
        KafkaSourceConfig {
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

/// One Kafka consumer serving one channel binding (the singleton unit).
pub struct KafkaTriggerSource {
    config: KafkaSourceConfig,
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

impl KafkaTriggerSource {
    /// A source for one channel binding. `config.properties` must carry a topic
    /// ([`codes::INBOUND_TOPIC_MISSING`]) and bootstrap servers
    /// ([`codes::INBOUND_CONFIG_INVALID`]) — fail-closed at wiring time.
    pub fn new(config: KafkaSourceConfig) -> Result<KafkaTriggerSource, Diagnostic> {
        if !config.properties.has_topic() {
            return Err(Diagnostic::error(
                codes::INBOUND_TOPIC_MISSING,
                format!(
                    "kafka channel '{}' requires property 'topic'",
                    config.channel
                ),
            ));
        }
        if !config.properties.has_bootstrap() {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "kafka channel '{}' requires property 'bootstrap.servers'",
                    config.channel
                ),
            ));
        }
        Ok(KafkaTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured topic (diagnostics / tests).
    pub fn topic(&self) -> &str {
        &self.config.properties.topic
    }
}

impl TriggerSource for KafkaTriggerSource {
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
            // (broker-level SASL/SSL still applies).
            if let Some(auth) = &self.config.inbound_auth {
                if auth.scheme() == InboundScheme::Mtls {
                    warn!(
                        channel = %self.config.channel,
                        code = codes::INBOUND_MTLS_UNSUPPORTED,
                        "kafka channel declared inbound-auth.scheme=mtls but per-channel mTLS is \
                         not supported — falling back to broker-level SASL/SSL"
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
                    "kafka source supervisor did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The consumer supervisor: leadership-gated subscribe → consume → teardown loop with
/// exponential reconnect backoff. Broker absence never escapes as an error.
async fn supervise(
    config: KafkaSourceConfig,
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
            // Not (or no longer) the leader — subscribe to nothing and re-check.
            if stop.sleep(config.gate_poll).await {
                return;
            }
            continue;
        }
        match open_session(&config) {
            Err(diagnostic) => {
                // NON-FATAL: WARN + retry in the background; readiness unaffected.
                warn!(
                    channel = %config.channel,
                    code = %diagnostic.code,
                    "kafka consumer unavailable — retrying in {:?}: {}",
                    backoff,
                    diagnostic.message
                );
                if stop.sleep(backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(config.reconnect_max);
            }
            Ok(consumer) => {
                backoff = config.reconnect_min;
                info!(
                    channel = %config.channel,
                    topic = %config.properties.topic,
                    group = %config.properties.group_id,
                    "kafka consumer up"
                );
                let end = run_session(&config, &intake, &gate, &stop, &consumer).await;
                // Dropping the consumer leaves the group (partitions rebalance).
                drop(consumer);
                match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::LeadershipLost => info!(
                        channel = %config.channel,
                        "kafka consumer left the group — leadership lost"
                    ),
                    SessionEnd::ConnectionLost => warn!(
                        channel = %config.channel,
                        "kafka consumer stream failed — reconnecting"
                    ),
                }
            }
        }
    }
}

/// Build the consumer client and subscribe to the topic. Creating the consumer does not
/// connect (librdkafka connects lazily on first poll); a bad config fails closed here.
fn open_session(config: &KafkaSourceConfig) -> Result<StreamConsumer, Diagnostic> {
    let props = &config.properties;
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &props.bootstrap_servers)
        .set("group.id", &props.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", &props.auto_offset_reset)
        .set("security.protocol", &props.security_protocol);
    for (key, value) in &props.client_config {
        client.set(key, value);
    }
    let consumer: StreamConsumer = client.create().map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!(
                "kafka channel '{}' could not build a consumer for '{}': {e}",
                config.channel, props.bootstrap_servers
            ),
        )
    })?;
    consumer.subscribe(&[props.topic.as_str()]).map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_CONSUMER_FAILED,
            format!(
                "kafka channel '{}' could not subscribe to topic '{}': {e}",
                config.channel, props.topic
            ),
        )
    })?;
    Ok(consumer)
}

/// The consume loop: gate re-check per turn (and on an idle tick), record → projection →
/// intake → offset action. Under `ack-mode: on-complete` the loop additionally drains the
/// deferred-settle channel (the registry callbacks' only entry point into this task) and
/// routes EVERY settle through the per-partition low-watermark tracker.
async fn run_session(
    config: &KafkaSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopToken>,
    consumer: &StreamConsumer,
) -> SessionEnd {
    // `on-complete`: settle callbacks bridge back here (see the module docs — the
    // consumer handle is deliberately NOT shared into them). Both live for exactly one
    // session: when it ends, `settle_rx` drops and any late settle WARNs instead of
    // committing on a partition this consumer may no longer own.
    let on_complete = config.properties.ack_mode == AckMode::OnComplete;
    let (settle_tx, mut settle_rx) =
        tokio::sync::mpsc::unbounded_channel::<DeferredSettleRequest>();
    let mut offsets = OffsetTracker::default();
    loop {
        if stop.is_requested() {
            return SessionEnd::Stopped;
        }
        if !gate.is_leading() {
            return SessionEnd::LeadershipLost;
        }
        let received = tokio::select! {
            _ = stop.notify.notified() => return SessionEnd::Stopped,
            // A deferred settle fired — advance the partition's commit watermark. `settle_tx`
            // is held by this frame, so `recv()` never resolves `None` (no busy loop).
            Some(request) = settle_rx.recv() => {
                apply_deferred_settle(config, consumer, &mut offsets, request);
                continue;
            }
            _ = tokio::time::sleep(config.gate_poll) => continue, // idle gate re-check
            received = consumer.recv() => received,
        };
        let message = match received {
            Ok(m) => m,
            Err(e) => {
                warn!(channel = %config.channel, error = %e, "kafka recv failed");
                return SessionEnd::ConnectionLost;
            }
        };
        let inbound = to_inbound_message(config, &message);
        // Per-message inbound auth: a rejected credential drops the record (commit/
        // advance — the Kafka NackDrop posture) and NEVER dispatches.
        if let Some(auth) = &config.inbound_auth {
            if auth.verify(&inbound.headers) == InboundVerdict::Reject {
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_AUTH_REJECTED,
                    "kafka channel rejected record {}-{}-{} — credential did not match expected",
                    message.topic(),
                    message.partition(),
                    message.offset()
                );
                if let Err(end) = settle_record(
                    config,
                    consumer,
                    &mut offsets,
                    on_complete,
                    &message,
                    AckDecision::NackDrop,
                ) {
                    return end;
                }
                continue;
            }
        }
        // The intake owns ack-mode TIMING: `on-persist` awaits the decision and settles
        // at dispatch-return; `on-complete` hands the engine per-delivery settle callbacks —
        // a PARKED instance holds this record's offset out of the commit watermark until its
        // terminal event, a run-to-completion dispatch settles now exactly like on-persist.
        let decision = if on_complete {
            offsets.in_flight(message.topic(), message.partition(), message.offset());
            let settle = deferred_settle(
                &config.channel,
                &settle_tx,
                message.topic(),
                message.partition(),
                message.offset(),
            );
            match intake.deliver_deferred(inbound, settle).await {
                DeliveryDisposition::Deferred => {
                    tracing::debug!(
                        channel = %config.channel,
                        topic = message.topic(),
                        partition = message.partition(),
                        offset = message.offset(),
                        "delivery deferred — the offset stays below the commit watermark until \
                         the instance's terminal event"
                    );
                    continue;
                }
                DeliveryDisposition::Settle(decision) => decision,
            }
        } else {
            intake.deliver(inbound).await
        };
        if let Err(end) = settle_record(
            config,
            consumer,
            &mut offsets,
            on_complete,
            &message,
            decision,
        ) {
            return end;
        }
    }
}

/// Settle one record NOW, in the mode the channel declared: `on-persist` commits/seeks per
/// record ([`execute_decision`], untouched); `on-complete` routes through the low-watermark
/// tracker so an immediate settle can never commit past a still-parked earlier offset.
fn settle_record(
    config: &KafkaSourceConfig,
    consumer: &StreamConsumer,
    offsets: &mut OffsetTracker,
    on_complete: bool,
    message: &BorrowedMessage<'_>,
    decision: AckDecision,
) -> Result<(), SessionEnd> {
    if !on_complete {
        return execute_decision(config, consumer, message, decision);
    }
    let (topic, partition, offset) = (message.topic(), message.partition(), message.offset());
    match decision {
        AckDecision::Ack | AckDecision::NackDrop => {
            // Both advance on Kafka — `NackDrop` IS "commit past the poison" (no per-record
            // reject exists). The watermark decides WHEN that advance becomes visible.
            if let Some(next) = offsets.settle(topic, partition, offset) {
                commit_watermark(config, consumer, topic, partition, next);
            }
            Ok(())
        }
        AckDecision::NackRequeue => {
            // The record stays IN FLIGHT: the watermark must never step over an offset we
            // are asking Kafka to re-read. The re-read re-registers it (idempotent insert).
            offsets.in_flight(topic, partition, offset);
            seek_back(config, consumer, message)
        }
    }
}

/// A fired deferred settle, bridged from the registry callback (engine actor thread / sweep
/// task) to the session task that owns the consumer. Carries the record COORDINATE, not the
/// record: the callbacks outlive the [`BorrowedMessage`] they answer for.
#[derive(Debug, Clone)]
struct DeferredSettleRequest {
    topic: String,
    partition: i32,
    offset: i64,
    /// Which callback fired. The offset action is the SAME for both (commit/advance — see
    /// the module docs); this only shapes the diagnostic.
    label: &'static str,
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete`. Both callbacks are
/// non-blocking enqueues onto the session's settle channel — they may run on the engine
/// actor thread, where a `CommitMode::Sync` round-trip would stall the whole engine. A
/// closed channel (the session died since the delivery: leadership loss, rebalance,
/// reconnect) is a WARN no-op: the offset was never committed, so the record redelivers to
/// the next session and inbox dedup absorbs the duplicate.
fn deferred_settle(
    channel_name: &str,
    settle_tx: &UnboundedSender<DeferredSettleRequest>,
    topic: &str,
    partition: i32,
    offset: i64,
) -> DeferredSettle {
    fn settle_callback(
        channel_name: &str,
        settle_tx: &UnboundedSender<DeferredSettleRequest>,
        topic: &str,
        partition: i32,
        offset: i64,
        label: &'static str,
    ) -> Box<dyn FnMut() + Send> {
        let channel_name = channel_name.to_string();
        let settle_tx = settle_tx.clone();
        let request = DeferredSettleRequest {
            topic: topic.to_string(),
            partition,
            offset,
            label,
        };
        Box::new(move || {
            if settle_tx.send(request.clone()).is_err() {
                warn!(
                    channel = %channel_name,
                    code = codes::INBOUND_CONSUMER_FAILED,
                    topic = %request.topic,
                    partition = request.partition,
                    offset = request.offset,
                    "deferred {label} arrived after the consumer session ended — offset NOT \
                     committed (the record redelivers; inbox dedup absorbs the duplicate)"
                );
            }
        })
    }
    DeferredSettle {
        ack: settle_callback(
            channel_name,
            settle_tx,
            topic,
            partition,
            offset,
            "ack (commit)",
        ),
        nack: settle_callback(
            channel_name,
            settle_tx,
            topic,
            partition,
            offset,
            "nack (commit — the Kafka drop posture)",
        ),
    }
}

/// Apply one bridged deferred settle: mark the offset settled and commit the partition's new
/// low watermark, if it moved.
fn apply_deferred_settle(
    config: &KafkaSourceConfig,
    consumer: &StreamConsumer,
    offsets: &mut OffsetTracker,
    request: DeferredSettleRequest,
) {
    tracing::debug!(
        channel = %config.channel,
        topic = %request.topic,
        partition = request.partition,
        offset = request.offset,
        "deferred {} fired — settling the record",
        request.label
    );
    if let Some(next) = offsets.settle(&request.topic, request.partition, request.offset) {
        commit_watermark(config, consumer, &request.topic, request.partition, next);
    }
}

/// Commit one partition's watermark (`next` = the next offset to consume, i.e. the highest
/// terminal offset + 1). Revocation-safe by construction: a partition this consumer no
/// longer owns is a WARN no-op — never a wrong-partition commit that would roll back the new
/// owner's progress. A failed commit is also a WARN and does NOT end the session (unlike the
/// `on-persist` path): tearing the session down would strand every other parked instance's
/// settle, and the next settle commits a HIGHER watermark that subsumes this one.
fn commit_watermark(
    config: &KafkaSourceConfig,
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    next: i64,
) {
    if !owns_partition(consumer, topic, partition) {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_CONSUMER_FAILED,
            topic = %topic,
            partition,
            offset = next,
            "deferred settle after partition revocation — offset NOT committed (the new owner \
             redelivers the records; inbox dedup absorbs the duplicates)"
        );
        return;
    }
    let mut tpl = TopicPartitionList::new();
    if let Err(e) = tpl.add_partition_offset(topic, partition, Offset::Offset(next)) {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_CONSUMER_FAILED,
            error = %e,
            "kafka deferred commit could not be built for {topic}-{partition}@{next}"
        );
        return;
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_CONSUMER_FAILED,
            topic = %topic,
            partition,
            offset = next,
            error = %e,
            "kafka deferred offset commit failed — the watermark advances on the next settle \
             (uncommitted records redeliver; inbox dedup absorbs the duplicates)"
        );
    }
}

/// Whether this consumer still holds the partition — the rebalance guard on every deferred
/// commit (local state, no broker round-trip).
fn owns_partition(consumer: &StreamConsumer, topic: &str, partition: i32) -> bool {
    if consumer.assignment_lost() {
        return false;
    }
    match consumer.assignment() {
        Ok(assignment) => assignment.find_partition(topic, partition).is_some(),
        Err(_) => false,
    }
}

/// Per-partition offset bookkeeping for `ack-mode: on-complete` — session-scoped and owned
/// by the consume loop (single-threaded: the settle channel serialises every mutation).
#[derive(Debug, Default)]
struct OffsetTracker {
    partitions: BTreeMap<(String, i32), PartitionOffsets>,
}

impl OffsetTracker {
    /// Record an offset handed to the engine and not yet terminal — the watermark may not
    /// pass it. Idempotent (a seek-back re-reads records already tracked).
    fn in_flight(&mut self, topic: &str, partition: i32, offset: i64) {
        self.entry(topic, partition).in_flight.insert(offset);
    }

    /// Settle one offset; answers the offset to COMMIT when the low watermark moved.
    fn settle(&mut self, topic: &str, partition: i32, offset: i64) -> Option<i64> {
        let state = self.entry(topic, partition);
        state.in_flight.remove(&offset);
        state.settled.insert(offset);
        state.take_commit_point()
    }

    fn entry(&mut self, topic: &str, partition: i32) -> &mut PartitionOffsets {
        self.partitions
            .entry((topic.to_string(), partition))
            .or_default()
    }
}

/// One partition's in-flight/settled offset sets plus the last watermark committed in this
/// session (the monotonic guard — a commit must never move the group backwards).
#[derive(Debug, Default)]
struct PartitionOffsets {
    in_flight: BTreeSet<i64>,
    settled: BTreeSet<i64>,
    committed: Option<i64>,
}

impl PartitionOffsets {
    /// The low-watermark rule: commit `highest settled offset BELOW the lowest in-flight
    /// offset, + 1`. Offset gaps (control records, compaction) are fine — the rule never
    /// assumes contiguity, only that every offset this session saw is in exactly one of the
    /// two sets. Settled offsets below the new commit point are pruned (they are subsumed by
    /// the commit); the remainder is bounded by how far consumption runs ahead of the oldest
    /// parked instance, itself bounded by `sutra.ack.deferred.timeout`.
    fn take_commit_point(&mut self) -> Option<i64> {
        let barrier = self.in_flight.iter().next().copied().unwrap_or(i64::MAX);
        let highest_terminal = *self.settled.range(..barrier).next_back()?;
        let next = highest_terminal.checked_add(1)?;
        if self.committed.is_some_and(|committed| committed >= next) {
            return None; // already covered by an earlier commit
        }
        self.settled = self.settled.split_off(&next);
        self.committed = Some(next);
        Some(next)
    }
}

/// Execute an [`AckDecision`] on the consumer — `Ack`/`NackDrop` commit the offset
/// (advance); `NackRequeue` seeks back to the record so the next poll re-reads it.
fn execute_decision(
    config: &KafkaSourceConfig,
    consumer: &StreamConsumer,
    message: &BorrowedMessage<'_>,
    decision: AckDecision,
) -> Result<(), SessionEnd> {
    match decision {
        AckDecision::Ack | AckDecision::NackDrop => {
            if let Err(e) = consumer.commit_message(message, CommitMode::Sync) {
                warn!(
                    channel = %config.channel,
                    code = codes::INBOUND_CONSUMER_FAILED,
                    error = %e,
                    "kafka offset commit failed — reconnecting (records will redeliver)"
                );
                return Err(SessionEnd::ConnectionLost);
            }
            Ok(())
        }
        AckDecision::NackRequeue => seek_back(config, consumer, message),
    }
}

/// Seek back to THIS record so the next poll re-reads it (redelivery; inbox dedup absorbs
/// the duplicate). Do NOT commit — the offset never advances.
fn seek_back(
    config: &KafkaSourceConfig,
    consumer: &StreamConsumer,
    message: &BorrowedMessage<'_>,
) -> Result<(), SessionEnd> {
    if let Err(e) = consumer.seek(
        message.topic(),
        message.partition(),
        Offset::Offset(message.offset()),
        Timeout::After(Duration::from_secs(5)),
    ) {
        warn!(
            channel = %config.channel,
            code = codes::INBOUND_CONSUMER_FAILED,
            error = %e,
            "kafka seek-for-requeue failed — reconnecting (records will redeliver)"
        );
        return Err(SessionEnd::ConnectionLost);
    }
    Ok(())
}

/// Project one Kafka record into the engine's [`InboundMessage`] — the inbound-message
/// projection (FROZEN `sutra-outbox-key` / `content-type` lift).
fn to_inbound_message(config: &KafkaSourceConfig, message: &BorrowedMessage<'_>) -> InboundMessage {
    project_inbound(
        config,
        flatten_headers(message),
        message.topic(),
        message.partition(),
        message.offset(),
        message.payload().map(|b| b.to_vec()).unwrap_or_default(),
    )
}

/// The pure record→[`InboundMessage`] projection (the broker-free core, unit-tested): the
/// `sutra-outbox-key` header is the EXPLICIT idempotency key when set; otherwise the
/// synthetic `topic-partition-offset` coordinate (NON-explicit); `content-type` header →
/// content type (default `application/octet-stream`).
fn project_inbound(
    config: &KafkaSourceConfig,
    headers: BTreeMap<String, String>,
    topic: &str,
    partition: i32,
    offset: i64,
    body: Vec<u8>,
) -> InboundMessage {
    let outbox_key = headers
        .get(HEADER_OUTBOX_KEY)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let explicit_event_id = outbox_key.is_some();
    let idempotency_key = outbox_key.unwrap_or_else(|| format!("{topic}-{partition}-{offset}"));
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

/// Every record header, values decoded as UTF-8 (lossy). Deterministic order.
fn flatten_headers(message: &BorrowedMessage<'_>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(headers) = message.headers() {
        for header in headers.iter() {
            let value = header
                .value
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            out.insert(header.key.to_string(), value);
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
    use super::*;
    use sutra_channels::sink::BoxFuture;

    fn base_properties() -> KafkaChannelProperties {
        KafkaChannelProperties {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: String::new(),
            group_id: "g".to_string(),
            auto_offset_reset: "earliest".to_string(),
            security_protocol: "PLAINTEXT".to_string(),
            client_config: BTreeMap::new(),
            ack_mode: super::super::AckMode::OnPersist,
            singleton: false,
        }
    }

    fn config() -> KafkaSourceConfig {
        let mut props = base_properties();
        props.topic = "transfer".to_string();
        KafkaSourceConfig::new("acme", "acme/payments/1.0.0", "transfer-topic", props)
    }

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn outbox_key_header_becomes_the_explicit_idempotency_key() {
        // FROZEN — a present `sutra-outbox-key` header IS the idempotency key.
        let m = project_inbound(
            &config(),
            headers(&[
                ("sutra-outbox-key", "ob-7"),
                ("content-type", "application/xml"),
            ]),
            "transfer",
            2,
            41,
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
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.channel, "transfer-topic");
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn idempotency_key_falls_back_to_topic_partition_offset() {
        // FROZEN — with no outbox-key header the key is the `topic-partition-offset`
        // coordinate.
        let m = project_inbound(&config(), headers(&[]), "transfer", 3, 99, b"x".to_vec());
        assert_eq!(m.idempotency_key, "transfer-3-99");
        assert!(!m.explicit_event_id, "the fallback key is non-explicit");
    }

    #[test]
    fn blank_outbox_key_header_falls_back() {
        let m = project_inbound(
            &config(),
            headers(&[("sutra-outbox-key", "   ")]),
            "transfer",
            0,
            0,
            Vec::new(),
        );
        assert_eq!(m.idempotency_key, "transfer-0-0");
        assert!(!m.explicit_event_id);
    }

    #[test]
    fn content_type_defaults_to_octet_stream() {
        let m = project_inbound(&config(), headers(&[]), "t", 0, 0, Vec::new());
        assert_eq!(m.content_type.as_deref(), Some("application/octet-stream"));
    }

    #[test]
    fn source_without_topic_fails_closed() {
        let cfg = KafkaSourceConfig::new("acme", "acme/m/1", "ch", base_properties());
        let err = match KafkaTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a topic must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_TOPIC_MISSING);
    }

    #[test]
    fn source_without_bootstrap_fails_closed() {
        let mut props = base_properties();
        props.topic = "t".to_string();
        props.bootstrap_servers = String::new();
        let cfg = KafkaSourceConfig::new("acme", "acme/m/1", "ch", props);
        let err = match KafkaTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without bootstrap servers must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    // ---- the `on-complete` low-watermark rule (broker-free core) ----------------------

    #[test]
    fn watermark_holds_while_an_earlier_offset_is_still_parked() {
        // The on-complete guarantee: a settled LATER record must not commit past a record
        // whose instance is still parked (committing N+1 commits everything below it).
        let mut offsets = OffsetTracker::default();
        for offset in 0..3 {
            offsets.in_flight("t", 0, offset);
        }
        assert_eq!(
            offsets.settle("t", 0, 2),
            None,
            "offset 0 and 1 still parked"
        );
        assert_eq!(offsets.settle("t", 0, 1), None, "offset 0 still parked");
        // The barrier settles last — the whole prefix becomes committable at once.
        assert_eq!(offsets.settle("t", 0, 0), Some(3));
    }

    #[test]
    fn watermark_advances_per_record_when_nothing_is_parked() {
        let mut offsets = OffsetTracker::default();
        offsets.in_flight("t", 0, 0);
        assert_eq!(offsets.settle("t", 0, 0), Some(1));
        offsets.in_flight("t", 0, 1);
        assert_eq!(offsets.settle("t", 0, 1), Some(2));
    }

    #[test]
    fn watermark_never_moves_backwards_and_never_recommits() {
        // A late settle of an already-subsumed offset is a no-op — the group's committed
        // offset must never roll back (that would replay terminal records on restart).
        let mut offsets = OffsetTracker::default();
        offsets.in_flight("t", 0, 0);
        offsets.in_flight("t", 0, 1);
        assert_eq!(offsets.settle("t", 0, 1), None);
        assert_eq!(offsets.settle("t", 0, 0), Some(2));
        assert_eq!(offsets.settle("t", 0, 0), None, "no re-commit of offset 0");
        assert_eq!(offsets.settle("t", 0, 1), None, "no re-commit of offset 1");
    }

    #[test]
    fn watermark_tolerates_offset_gaps() {
        // Control records / compaction leave holes in the offset sequence; the rule only
        // needs "highest settled below the lowest in-flight", never contiguity.
        let mut offsets = OffsetTracker::default();
        offsets.in_flight("t", 0, 5);
        offsets.in_flight("t", 0, 9);
        assert_eq!(offsets.settle("t", 0, 9), None);
        assert_eq!(offsets.settle("t", 0, 5), Some(10));
    }

    #[test]
    fn watermark_is_tracked_per_partition() {
        let mut offsets = OffsetTracker::default();
        offsets.in_flight("t", 0, 7);
        offsets.in_flight("t", 1, 0);
        // A parked record on partition 0 does not hold partition 1 back.
        assert_eq!(offsets.settle("t", 1, 0), Some(1));
        assert_eq!(offsets.settle("t", 0, 7), Some(8));
    }

    #[test]
    fn requeued_offset_stays_in_flight_and_blocks_the_watermark() {
        // NackRequeue seeks back: the watermark must not step over a record Kafka has been
        // asked to re-read, even after later records settle.
        let mut offsets = OffsetTracker::default();
        offsets.in_flight("t", 0, 0);
        offsets.in_flight("t", 0, 1);
        assert_eq!(offsets.settle("t", 0, 1), None);
        offsets.in_flight("t", 0, 0); // the seek-back re-registration (idempotent)
        assert_eq!(
            offsets.settle("t", 0, 1),
            None,
            "offset 0 is still re-reading"
        );
        assert_eq!(offsets.settle("t", 0, 0), Some(2), "the redelivery settled");
    }

    #[tokio::test]
    async fn stop_before_start_is_idempotent_and_start_resolves_without_a_broker() {
        // Points at a port nobody listens on — start MUST still resolve Ok (broker
        // absence is non-fatal, the supervisor retries in the background).
        let mut cfg = config();
        cfg.properties.bootstrap_servers = "127.0.0.1:1".to_string(); // reserved, never a broker
        cfg.reconnect_min = Duration::from_millis(10);
        cfg.reconnect_max = Duration::from_millis(20);
        cfg.gate_poll = Duration::from_millis(20);
        let source = KafkaTriggerSource::new(cfg).expect("source");

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
