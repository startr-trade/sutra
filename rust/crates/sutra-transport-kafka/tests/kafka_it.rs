//! Kafka transport integration suite — Testcontainers-backed
//! (`apache/kafka-native:3.8.0`). Exercises the transport seams end to end: the source delivers
//! into a scripted [`InboundIntake`] and each [`AckDecision`] is proven on the broker
//! (Ack/NackDrop commit the offset, NackRequeue seeks back and redelivers), and the sink
//! publishes with the FROZEN wire projection
//! (outbox key on the `sutra-outbox-key` header, record key from the URI path, `content-type`
//! header, CE `ce_*` headers carried verbatim).
//!
//! Requires a Docker daemon (same posture as the rabbitmq / sutra-persistence pg suites).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use sutra_channels::auth::BrokerInboundAuth;
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};
use sutra_channels::{BoxFuture, DeferredAckRegistry, InboundMessage};
use sutra_transport_kafka::{
    AckMode, KafkaChannelProperties, KafkaMessageSink, KafkaSourceConfig, KafkaTriggerSource,
};
use testcontainers::runners::SyncRunner;
use testcontainers::Container;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

// ---- shared broker fixture -----------------------------------------------------------------

static BROKER: OnceLock<(Container<Kafka>, u16)> = OnceLock::new();

/// One shared broker per test binary; each test creates its own fresh topic.
fn broker_port() -> u16 {
    let (_, port) = BROKER.get_or_init(|| {
        // Blocking runner on a dedicated thread — must not run inside a tokio worker.
        std::thread::spawn(|| {
            let container = Kafka::default()
                .start()
                .expect("start apache/kafka-native (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container
                .get_host_port_ipv4(KAFKA_PORT)
                .expect("mapped kafka port");
            (container, port)
        })
        .join()
        .expect("broker bootstrap thread")
    });
    *port
}

fn bootstrap() -> String {
    format!("127.0.0.1:{}", broker_port())
}

/// Unique-enough topic name per test (no uuid dependency).
fn fresh_topic() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "t-{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// Pre-create the topic (1 partition) so the consumer sees it immediately — rdkafka's
/// metadata refresh for an auto-created topic is minutes, too slow for a test.
async fn create_topic(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");
}

async fn produce(topic: &str, key: &str, headers: &[(&str, &str)], body: &[u8]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    let mut owned = OwnedHeaders::new();
    for (k, v) in headers {
        owned = owned.insert(Header {
            key: k,
            value: Some(*v),
        });
    }
    producer
        .send(
            FutureRecord::to(topic)
                .payload(body)
                .key(key)
                .headers(owned),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .expect("produce");
}

/// Read one record off a topic from the start (fresh group) — the raw wire view.
async fn consume_one_raw(topic: &str) -> OwnedMessage {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("group.id", format!("verify-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("verify consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let message = tokio::time::timeout(Duration::from_secs(20), consumer.recv())
        .await
        .expect("record within timeout")
        .expect("record");
    message.detach()
}

fn header_value(message: &OwnedMessage, key: &str) -> Option<String> {
    let headers = message.headers()?;
    (0..headers.count()).find_map(|i| {
        let h = headers.get(i);
        if h.key == key {
            Some(String::from_utf8_lossy(h.value.unwrap_or_default()).into_owned())
        } else {
            None
        }
    })
}

/// A NON-subscribing probe client for one consumer group — it reads committed offsets
/// without ever joining the group (no rebalance, so the source under test keeps its
/// partitions).
fn probe_consumer(group: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .expect("probe consumer")
}

/// The group's committed offset for one partition — `None` when nothing is committed yet.
fn read_committed(probe: &BaseConsumer, topic: &str, partition: i32) -> Option<i64> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Invalid)
        .expect("probe tpl");
    let committed = probe
        .committed_offsets(tpl, Timeout::After(Duration::from_secs(10)))
        .expect("committed offsets");
    match committed
        .find_partition(topic, partition)
        .map(|e| e.offset())
    {
        Some(Offset::Offset(value)) => Some(value),
        _ => None,
    }
}

/// One-shot committed-offset read (the negative assertions: "still nothing committed").
async fn committed_now(group: &str, topic: &str, partition: i32) -> Option<i64> {
    let (group, topic) = (group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || read_committed(&probe_consumer(&group), &topic, partition))
        .await
        .expect("probe task")
}

/// Block (off the reactor) until the group's committed offset reaches `expected`.
async fn wait_for_committed(group: &str, topic: &str, partition: i32, expected: i64) {
    let (group, topic) = (group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let probe = probe_consumer(&group);
        let deadline = Instant::now() + Duration::from_secs(25);
        let mut last = None;
        while Instant::now() < deadline {
            last = read_committed(&probe, &topic, partition);
            if last == Some(expected) {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!(
            "timed out waiting for committed offset {expected} on {topic}-{partition} \
             (last seen: {last:?})"
        );
    })
    .await
    .expect("probe task");
}

async fn wait_until<F: Fn() -> bool>(what: &str, timeout: Duration, check: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

// ---- test doubles ---------------------------------------------------------------------------

/// Scripted intake: answers queued decisions in order (front first), then the default.
struct ScriptedIntake {
    decisions: Mutex<std::collections::VecDeque<AckDecision>>,
    default: AckDecision,
    delivered: Mutex<Vec<InboundMessage>>,
}

impl ScriptedIntake {
    fn always(default: AckDecision) -> Arc<ScriptedIntake> {
        ScriptedIntake::scripted(vec![], default)
    }

    fn scripted(decisions: Vec<AckDecision>, default: AckDecision) -> Arc<ScriptedIntake> {
        Arc::new(ScriptedIntake {
            decisions: Mutex::new(decisions.into()),
            default,
            delivered: Mutex::new(Vec::new()),
        })
    }

    fn delivered_count(&self) -> usize {
        self.delivered.lock().unwrap().len()
    }

    fn delivered_at(&self, index: usize) -> InboundMessage {
        self.delivered.lock().unwrap()[index].clone()
    }
}

impl InboundIntake for ScriptedIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            self.delivered.lock().unwrap().push(message);
            self.decisions
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(self.default)
        })
    }
}

struct FlippableGate(AtomicBool);

impl FlippableGate {
    fn leading(initial: bool) -> Arc<FlippableGate> {
        Arc::new(FlippableGate(AtomicBool::new(initial)))
    }

    fn set(&self, leading: bool) {
        self.0.store(leading, Ordering::SeqCst);
    }
}

impl LeaderGate for FlippableGate {
    fn is_leading(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

fn source_for(topic: &str, channel: &str, group: &str) -> KafkaTriggerSource {
    source_with_mode(topic, channel, group, AckMode::OnPersist)
}

fn source_with_mode(
    topic: &str,
    channel: &str,
    group: &str,
    ack_mode: AckMode,
) -> KafkaTriggerSource {
    let properties = KafkaChannelProperties {
        bootstrap_servers: bootstrap(),
        topic: topic.to_string(),
        group_id: group.to_string(),
        auto_offset_reset: "earliest".to_string(),
        security_protocol: "PLAINTEXT".to_string(),
        client_config: BTreeMap::new(),
        ack_mode,
        singleton: false,
    };
    let mut config = KafkaSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    KafkaTriggerSource::new(config).expect("source")
}

// ---- inbound ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_delivers_body_headers_and_outbox_key_as_idempotency_key() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    // Produce first (topic pre-created), then read from earliest.
    produce(
        &topic,
        "customer-1",
        &[
            ("sutra-outbox-key", "order-1"),
            ("content-type", "application/xml"),
            ("x-tenant", "acme"),
        ],
        b"<Document/>",
    )
    .await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&topic, "payments-inbound", "g-inbound-1");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("first delivery", Duration::from_secs(25), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    assert_eq!(m.body.into_inner(), b"<Document/>");
    // FROZEN — the sutra-outbox-key header is the explicit idempotency key.
    assert_eq!(m.idempotency_key, "order-1");
    assert!(m.explicit_event_id);
    assert_eq!(m.content_type.as_deref(), Some("application/xml"));
    assert_eq!(m.headers.get("x-tenant").map(String::as_str), Some("acme"));
    assert_eq!(m.tenant, "acme");
    assert_eq!(m.channel, "payments-inbound");
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_key_falls_back_to_topic_partition_offset() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    produce(&topic, "k", &[("content-type", "text/plain")], b"no-key").await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&topic, "payments-inbound", "g-fallback-1");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    wait_until("delivery", Duration::from_secs(25), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    // FROZEN — synthetic topic-partition-offset coordinate, non-explicit.
    assert_eq!(m.idempotency_key, format!("{topic}-0-0"));
    assert!(!m.explicit_event_id, "the fallback key is non-explicit");
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_seeks_back_and_redelivers_until_acked() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    produce(
        &topic,
        "k",
        &[("sutra-outbox-key", "evt-requeue")],
        b"retry-me",
    )
    .await;

    // First decision: transient failure (persistence down) — the source seeks back and the
    // record is re-read; the redelivery is then acked (committed).
    let intake = ScriptedIntake::scripted(vec![AckDecision::NackRequeue], AckDecision::Ack);
    let source = source_for(&topic, "payments-inbound", "g-requeue-1");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until(
        "redelivery after nack(requeue)",
        Duration::from_secs(25),
        || intake.delivered_count() >= 2,
    )
    .await;
    assert_eq!(intake.delivered_at(0).idempotency_key, "evt-requeue");
    assert_eq!(
        intake.delivered_at(1).idempotency_key,
        "evt-requeue",
        "the SAME record rides the redelivery — inbox dedup absorbs it"
    );
    source.stop().await.expect("stop");
}

// ---- singleton gating (transport mechanics; the lease-backed proof lives in
// sutra-engine's kafka rewire IT) -----------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leadership_gate_keeps_exactly_one_consumer_and_hands_over() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    // DISTINCT groups so Kafka's own group coordinator never gates them — the ENGINE lease
    // (LeaderGate) is what must keep exactly one consuming.
    let intake_a = ScriptedIntake::always(AckDecision::Ack);
    let intake_b = ScriptedIntake::always(AckDecision::Ack);
    let gate_a = FlippableGate::leading(true);
    let gate_b = FlippableGate::leading(false);
    let source_a = source_for(&topic, "transfer-topic", "g-a");
    let source_b = source_for(&topic, "transfer-topic", "g-b");

    source_a
        .start(intake_a.clone(), gate_a.clone())
        .await
        .expect("start A");
    source_b
        .start(intake_b.clone(), gate_b.clone())
        .await
        .expect("start B");

    produce(&topic, "k", &[("sutra-outbox-key", "m-A")], b"to-A").await;
    wait_until("A consumed", Duration::from_secs(25), || {
        intake_a.delivered_count() >= 1
    })
    .await;
    // The follower (gate false) consumes nothing.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        intake_b.delivered_count(),
        0,
        "the follower consumes nothing"
    );

    // Handover: A's gate revokes, B's grants. B reads from earliest (its own group), so it
    // sees the message; assert B now consumes and A stopped advancing.
    gate_a.set(false);
    gate_b.set(true);
    wait_until("B consumed after handover", Duration::from_secs(25), || {
        intake_b.delivered_count() >= 1
    })
    .await;

    let a_before = intake_a.delivered_count();
    produce(&topic, "k", &[("sutra-outbox-key", "m-after")], b"after").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        intake_a.delivered_count(),
        a_before,
        "A no longer consumes after losing leadership"
    );

    source_a.stop().await.expect("stop A");
    source_b.stop().await.expect("stop B");
}

// ---- outbound (the m9 wire projection) ------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_publishes_with_the_frozen_wire_projection() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    let sink = KafkaMessageSink::new(bootstrap());

    let mut headers = BTreeMap::new();
    headers.insert("x-tenant".to_string(), "acme".to_string());
    // A `ce_*` binary-binding header (projected upstream by the dispatcher) rides verbatim.
    headers.insert("ce_type".to_string(), "io.sutra.reply.v1".to_string());
    let message = OutboundMessage {
        // kafka://<topic>/<key> — the path segment is the record (partition) key.
        destination: format!("kafka://{topic}/customer-7"),
        headers,
        body: b"{\"ok\":true}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: "outbox-abc-123".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };

    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let record = consume_one_raw(&topic).await;
    assert_eq!(record.payload(), Some(b"{\"ok\":true}".as_slice()));
    // FROZEN — record key comes from the URI path (partitioning), NOT the outbox key.
    assert_eq!(
        record
            .key()
            .map(|k| String::from_utf8_lossy(k).into_owned()),
        Some("customer-7".to_string())
    );
    // FROZEN — the outbox key rides the sutra-outbox-key header (dedup token).
    assert_eq!(
        header_value(&record, "sutra-outbox-key").as_deref(),
        Some("outbox-abc-123")
    );
    assert_eq!(
        header_value(&record, "content-type").as_deref(),
        Some("application/json")
    );
    assert_eq!(header_value(&record, "x-tenant").as_deref(), Some("acme"));
    // The Kafka CE binding prefix is `ce_` (underscore) — carried verbatim by the sink.
    assert_eq!(
        header_value(&record, "ce_type").as_deref(),
        Some("io.sutra.reply.v1")
    );
    assert_eq!(
        header_value(&record, "traceparent").as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    sink.drain().await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_omits_the_record_key_when_the_uri_has_no_path() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    let sink = KafkaMessageSink::new(bootstrap());
    let message = OutboundMessage {
        destination: format!("kafka://{topic}"), // no path ⇒ NULL record key
        headers: BTreeMap::new(),
        body: b"payload".to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: "ob-nokey".to_string(),
        traceparent: None,
    };
    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let record = consume_one_raw(&topic).await;
    assert_eq!(record.key(), None, "no URI path ⇒ NULL record key");
    assert_eq!(
        header_value(&record, "sutra-outbox-key").as_deref(),
        Some("ob-nokey")
    );
    sink.drain().await;
}

// ---- message-level inbound auth (round trip) ------------------------------------------------

/// A parsed apikey inbound-auth config expecting `expected` in the `X-API-Key` header.
fn apikey_auth(expected: &str) -> BrokerInboundAuth {
    let props: BTreeMap<String, String> = [
        ("inbound-auth.scheme".to_string(), "apikey".to_string()),
        (
            "inbound-auth.expected-key-ref".to_string(),
            format!("literal:{expected}"),
        ),
    ]
    .into();
    BrokerInboundAuth::from_properties(&props, "SUTRA.INBOUND.KAFKA.CONFIG_INVALID", |r| {
        r.strip_prefix("literal:")
            .map(str::to_string)
            .ok_or_else(|| "unresolved".to_string())
    })
    .expect("auth parses")
    .expect("auth configured")
}

fn source_with_auth(
    topic: &str,
    channel: &str,
    group: &str,
    auth: BrokerInboundAuth,
) -> KafkaTriggerSource {
    let properties = KafkaChannelProperties {
        bootstrap_servers: bootstrap(),
        topic: topic.to_string(),
        group_id: group.to_string(),
        auto_offset_reset: "earliest".to_string(),
        security_protocol: "PLAINTEXT".to_string(),
        client_config: BTreeMap::new(),
        ack_mode: AckMode::OnPersist,
        singleton: false,
    };
    let mut config = KafkaSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    config.inbound_auth = Some(auth);
    KafkaTriggerSource::new(config).expect("source")
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_apikey_drops_unauthorised_and_delivers_authorised() {
    let topic = fresh_topic();
    create_topic(&topic).await;
    // A wrong key is dropped (offset advances); the correct key is delivered. Ordering on a
    // single partition proves the first was dropped when only the second reaches intake.
    produce(&topic, "k1", &[("X-API-Key", "wrong")], b"unauthorised").await;
    produce(&topic, "k2", &[("X-API-Key", "correct-key")], b"authorised").await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_with_auth(
        &topic,
        "payments-inbound",
        "g-auth-1",
        apikey_auth("correct-key"),
    );
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("authorised delivery", Duration::from_secs(25), || {
        intake.delivered_count() >= 1
    })
    .await;
    // Settle: give any (wrongly) surviving record a chance to arrive — it must not.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        intake.delivered_count(),
        1,
        "only the authorised record reaches intake (the wrong key was dropped)"
    );
    assert_eq!(intake.delivered_at(0).body.into_inner(), b"authorised");
    source.stop().await.expect("stop");
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The broker-side half of the on-complete contract against a REAL Kafka: the source hands
// its per-delivery settle callbacks through `InboundIntake::deliver_deferred` and the
// OFFSET COMMIT only happens when the instance's terminal event settles the
// `DeferredAckRegistry` entry — at the partition's LOW WATERMARK, so a settled later record
// never commits past a still-parked earlier one (Kafka's commit of N+1 commits everything
// below it). The engine-side half (dispatch parks → registry → listener bus) is
// `sutra-channels/tests/all/deferred_ack_test.rs`.
//
// Records are produced WITHOUT `sutra-outbox-key`, so each delivery's idempotency key IS its
// `topic-partition-offset` coordinate — the handle these tests use to fire the terminal
// event of a chosen OFFSET.

/// The engine-actor stand-in for the on-complete seam: registers each delivery's settle
/// callbacks on a REAL [`DeferredAckRegistry`] under a synthetic instance id (exactly what
/// the dispatcher's park arm does) and answers `Deferred`; the test then fires the
/// instance's terminal event by hand.
struct DeferringIntake {
    registry: Arc<DeferredAckRegistry>,
    /// `(instance_id, idempotency_key)` in delivery order.
    instances: Mutex<Vec<(String, String)>>,
    plain_deliveries: Mutex<usize>,
}

impl DeferringIntake {
    fn new(registry: Arc<DeferredAckRegistry>) -> Arc<DeferringIntake> {
        Arc::new(DeferringIntake {
            registry,
            instances: Mutex::new(Vec::new()),
            plain_deliveries: Mutex::new(0),
        })
    }

    fn instance_count(&self) -> usize {
        self.instances.lock().unwrap().len()
    }

    fn plain_count(&self) -> usize {
        *self.plain_deliveries.lock().unwrap()
    }

    /// The instance id of the delivery carrying `topic-0-<offset>` — the parked instance
    /// holding that record's offset.
    fn instance_for_offset(&self, topic: &str, offset: i64) -> String {
        let key = format!("{topic}-0-{offset}");
        self.instances
            .lock()
            .unwrap()
            .iter()
            .find(|(_, delivered)| *delivered == key)
            .unwrap_or_else(|| panic!("no deferred delivery for {key}"))
            .0
            .clone()
    }
}

impl InboundIntake for DeferringIntake {
    fn deliver(&self, _message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            *self.plain_deliveries.lock().unwrap() += 1;
            AckDecision::Ack
        })
    }

    fn deliver_deferred(
        &self,
        message: InboundMessage,
        settle: DeferredSettle,
    ) -> BoxFuture<'_, DeliveryDisposition> {
        Box::pin(async move {
            let instance_id = format!("inst-{}", fresh_topic());
            assert!(self.registry.register(
                &instance_id,
                &message.channel,
                settle.ack,
                settle.nack
            ));
            self.instances
                .lock()
                .unwrap()
                .push((instance_id, message.idempotency_key.clone()));
            DeliveryDisposition::Deferred
        })
    }
}

fn registry() -> Arc<DeferredAckRegistry> {
    Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)))
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_withholds_the_offset_commit_until_the_instance_completes() {
    // record in → commit DEFERRED → instance completes → the offset commits.
    let topic = fresh_topic();
    let group = "g-on-complete-1";
    create_topic(&topic).await;
    produce(&topic, "k", &[], b"deferred-1").await;

    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("deferred delivery", Duration::from_secs(25), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(
        registry.pending_count(),
        1,
        "the settle is REGISTERED, not fired"
    );
    assert_eq!(
        intake.plain_count(),
        0,
        "an on-complete source routes through deliver_deferred, never plain deliver"
    );
    // The discriminator: nothing is committed while the instance runs (an on-persist source
    // would have committed offset 1 at dispatch-return).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        committed_now(group, &topic, 0).await,
        None,
        "no offset commits while the instance is parked"
    );

    registry.on_instance_completed(&intake.instance_for_offset(&topic, 0));
    assert_eq!(registry.pending_count(), 0);
    wait_for_committed(group, &topic, 0, 1).await; // "next offset to consume" = 1
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_commits_the_partition_low_watermark_not_the_settled_record() {
    // THE ordering pin. Three records park; offsets 2 and 1 complete FIRST. Committing
    // either would implicitly commit offset 0 — whose instance is still running — so the
    // commit point must not move until offset 0 itself settles.
    let topic = fresh_topic();
    let group = "g-on-complete-watermark";
    create_topic(&topic).await;
    for n in 0..3 {
        produce(&topic, "k", &[], format!("record-{n}").as_bytes()).await;
    }

    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("three deferred deliveries", Duration::from_secs(25), || {
        intake.instance_count() >= 3
    })
    .await;
    assert_eq!(registry.pending_count(), 3, "all three settles are held");

    // Out-of-order completion: the two LATER records terminate while offset 0 parks on.
    registry.on_instance_completed(&intake.instance_for_offset(&topic, 2));
    registry.on_instance_completed(&intake.instance_for_offset(&topic, 1));
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        committed_now(group, &topic, 0).await,
        None,
        "committing 2 or 3 here would silently commit the still-parked offset 0"
    );
    assert_eq!(registry.pending_count(), 1);

    // The barrier settles — the whole terminal prefix commits at once.
    registry.on_instance_completed(&intake.instance_for_offset(&topic, 0));
    wait_for_committed(group, &topic, 0, 3).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_failed_instance_commits_past_the_record_the_kafka_drop_posture() {
    // failure path: record in → commit deferred → instance FAILS → the offset commits
    // ANYWAY. Kafka has no per-record reject, so NackDrop IS "commit past the poison" (the
    // ack mapping the immediate path already uses) — the record must NEVER redeliver.
    let topic = fresh_topic();
    let group = "g-on-complete-failed";
    create_topic(&topic).await;
    produce(&topic, "k", &[], b"will-fail").await;

    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("deferred delivery", Duration::from_secs(25), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    registry.on_instance_failed(&intake.instance_for_offset(&topic, 0));
    assert_eq!(registry.pending_count(), 0);
    wait_for_committed(group, &topic, 0, 1).await;
    source.stop().await.expect("stop");

    // The drop proof: a new session in the SAME group starts past the poison record.
    let next_intake = ScriptedIntake::always(AckDecision::Ack);
    let next_source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnComplete);
    next_source
        .start(next_intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("restart");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        next_intake.delivered_count(),
        0,
        "the failed record was dropped (offset committed past it) — never redelivered"
    );
    next_source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_redelivers_when_the_session_dies_with_the_settle_still_held() {
    // The at-least-once proof for the chosen posture, and the dead-session settle: with no
    // terminal event the offset is never committed, so the record replays to the next
    // session; a settle that fires AFTER the session died finds a closed bridge, WARNs, and
    // commits nothing (never a commit on a partition this consumer may no longer own).
    let topic = fresh_topic();
    let group = "g-on-complete-redeliver";
    create_topic(&topic).await;
    produce(&topic, "k", &[], b"still-running").await;

    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("deferred delivery", Duration::from_secs(25), || {
        intake.instance_count() >= 1
    })
    .await;
    source.stop().await.expect("stop"); // session dies with the settle still held

    // A LATE terminal event: the bridge to the (dead) consumer task is closed — WARN no-op.
    registry.on_instance_completed(&intake.instance_for_offset(&topic, 0));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        committed_now(group, &topic, 0).await,
        None,
        "a settle after the session died must not commit"
    );

    // Uncommitted ⇒ the next session in the same group re-reads the record (inbox dedup
    // absorbs the duplicate downstream).
    let next_intake = ScriptedIntake::always(AckDecision::Ack);
    let next_source = source_with_mode(&topic, "payments-inbound", group, AckMode::OnPersist);
    next_source
        .start(next_intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("restart");
    wait_until(
        "redelivery to the new session",
        Duration::from_secs(25),
        || next_intake.delivered_count() >= 1,
    )
    .await;
    assert_eq!(
        next_intake.delivered_at(0).idempotency_key,
        format!("{topic}-0-0"),
        "the SAME record rides the redelivery"
    );
    next_source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_persist_source_still_commits_at_dispatch_return() {
    // Regression pin for the untouched path: an on-persist source keeps calling plain
    // `deliver` and commits at dispatch-return — the deferred seam is never involved.
    let topic = fresh_topic();
    let group = "g-on-persist-pin";
    create_topic(&topic).await;
    produce(&topic, "k", &[], b"classic").await;

    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_for(&topic, "payments-inbound", group); // OnPersist
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("plain delivery", Duration::from_secs(25), || {
        intake.plain_count() >= 1
    })
    .await;
    assert_eq!(intake.instance_count(), 0, "deliver_deferred never called");
    assert_eq!(registry.pending_count(), 0);
    wait_for_committed(group, &topic, 0, 1).await; // committed immediately
    source.stop().await.expect("stop");
}
