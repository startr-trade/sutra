//! AMQP 1.0 transport integration suite — Testcontainers-backed
//! (`apache/activemq-artemis:latest-alpine`, AMQP 1.0 on 5672). Exercises the transport seams
//! against a real broker via the native `fe2o3-amqp` client:
//! the sink→source round trip carries the `sutra-outbox-key` idempotency key + body +
//! content-type; the sink's outbound wire projection carries the key + `ce-` DASH CE
//! attributes as AMQP 1.0 application properties; each [`AckDecision`] is proven on the
//! broker (release redelivers, reject drops the poison).
//!
//! Requires a Docker daemon (same posture as the rabbitmq / kafka suites).

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Message};
use fe2o3_amqp::types::primitives::{SimpleValue, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, TriggerSource,
};
use sutra_channels::{BoxFuture, DeferredAckRegistry, InboundMessage};
use sutra_transport_amqp::{
    AckMode, AmqpChannelProperties, AmqpMessageSink, AmqpSourceConfig, AmqpTriggerSource,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt};

// ---- shared broker fixture -----------------------------------------------------------------

static BROKER: OnceLock<(Container<GenericImage>, u16)> = OnceLock::new();

fn artemis_image() -> ContainerRequest<GenericImage> {
    GenericImage::new("apache/activemq-artemis", "latest-alpine")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server is now active"))
        .with_env_var("ARTEMIS_USER", "artemis")
        .with_env_var("ARTEMIS_PASSWORD", "artemis")
        .with_env_var("ANONYMOUS_LOGIN", "true")
}

/// One shared broker per test binary; each test declares its own fresh queue name.
fn broker_port() -> u16 {
    let (_, port) = BROKER.get_or_init(|| {
        std::thread::spawn(|| {
            let container = artemis_image()
                .start()
                .expect("start apache/activemq-artemis:latest-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5672).expect("mapped 5672");
            (container, port)
        })
        .join()
        .expect("broker bootstrap thread")
    });
    *port
}

fn fresh_queue() -> String {
    format!("q-{}", uuid_ish())
}

/// Unique-enough suffix without pulling a uuid dependency into this crate.
fn uuid_ish() -> String {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

// ---- raw fe2o3 producer / consumer (drive/observe the broker) ------------------------------

async fn raw_send(port: u16, address: &str, outbox_key: &str, content_type: &str, body: &[u8]) {
    let mut connection = Connection::open(
        "it-raw-producer",
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open producer connection");
    let mut session = Session::begin(&mut connection)
        .await
        .expect("begin session");
    let mut sender = Sender::attach(&mut session, format!("it-raw-producer-{address}"), address)
        .await
        .expect("attach sender");
    let ap = ApplicationProperties::builder()
        .insert("sutra-outbox-key", outbox_key)
        .insert("content-type", content_type)
        .build();
    let message = Message::builder()
        .application_properties(ap)
        .data(body.to_vec())
        .build();
    sender
        .send(message)
        .await
        .expect("send")
        .accepted_or_else(|s| panic!("send not accepted: {s:?}"))
        .unwrap();
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
}

fn simple_to_string(v: &SimpleValue) -> String {
    match v {
        SimpleValue::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

// ---- test doubles --------------------------------------------------------------------------

/// Scripted intake: answers queued decisions in order (front first), then the default.
struct ScriptedIntake {
    decisions: Mutex<VecDeque<AckDecision>>,
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

fn source_for(port: u16, queue: &str, channel: &str) -> AmqpTriggerSource {
    source_with_mode(port, queue, channel, AckMode::OnPersist)
}

fn source_with_mode(port: u16, queue: &str, channel: &str, ack_mode: AckMode) -> AmqpTriggerSource {
    let properties = AmqpChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        tls: false,
        username: None, // ANONYMOUS_LOGIN — the image allows anonymous sessions
        password: None,
        queue: Some(queue.to_string()),
        topic: None,
        prefetch_count: 10,
        receive_timeout_ms: 250,
        ack_mode,
        singleton: false,
    };
    let mut config = AmqpSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    AmqpTriggerSource::new(config).expect("source")
}

fn sink_for() -> AmqpMessageSink {
    AmqpMessageSink::new(None, None)
}

async fn wait_until<F: Fn() -> bool>(what: &str, timeout: Duration, check: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

// ---- round trip ----------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_from_sink_to_source_carries_outbox_key_body_and_content_type() {
    let port = broker_port();
    let queue = fresh_queue();
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(port, &queue, "payments-in");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    let sink = sink_for();
    let message = OutboundMessage {
        destination: format!("amqp10://127.0.0.1:{port}/{queue}"),
        headers: BTreeMap::new(),
        body: b"round-trip-payload".to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: "rt-ob-key".to_string(),
        traceparent: None,
    };
    match sink.send(&message).await {
        SendOutcome::Delivered => {}
        other => panic!("sink send failed: {other:?}"),
    }

    wait_until("first delivery", Duration::from_secs(45), || {
        intake.delivered_count() >= 1
    })
    .await;
    let m = intake.delivered_at(0);
    assert_eq!(m.idempotency_key, "rt-ob-key");
    assert!(m.explicit_event_id);
    assert_eq!(m.body.into_inner(), b"round-trip-payload");
    assert_eq!(m.content_type.as_deref(), Some("text/plain"));
    assert_eq!(m.tenant, "acme");
    assert_eq!(m.channel, "payments-in");

    source.stop().await.expect("stop");
    sink.drain().await;
}

// ---- outbound wire projection --------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_sink_wire_carries_outbox_key_and_ce_dash_attributes() {
    // The outbox→sink wire assertion: the AMQP 1.0 sink carries the shared `sutra-outbox-key`
    // and the CloudEvents binary `ce-<attr>` (DASH) attributes as application properties.
    let port = broker_port();
    let queue = fresh_queue();
    let sink = sink_for();

    let mut headers = BTreeMap::new();
    headers.insert("ce-id".to_string(), "evt-9".to_string());
    headers.insert("ce-type".to_string(), "payment.reply".to_string());
    headers.insert("ce-source".to_string(), "/acme".to_string());
    headers.insert("x-tenant".to_string(), "acme".to_string());
    let message = OutboundMessage {
        destination: format!("amqp10://127.0.0.1:{port}/{queue}"),
        headers,
        body: b"{\"ok\":true}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: "ob-wire-1".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };
    // Consumer-first: Artemis routes this auto-created address to consumers attached at
    // publish time (the round-trip IT starts its source consumer before sending for the same
    // reason). Attach the raw receiver BEFORE the sink publishes, else the message is dropped.
    let mut connection = Connection::open(
        "it-wire-consumer",
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open consumer connection");
    let mut session = Session::begin(&mut connection)
        .await
        .expect("begin session");
    let mut receiver = Receiver::attach(
        &mut session,
        format!("it-wire-consumer-{queue}"),
        queue.as_str(),
    )
    .await
    .expect("attach receiver");

    match sink.send(&message).await {
        SendOutcome::Delivered => {}
        other => panic!("sink send failed: {other:?}"),
    }

    let delivery = tokio::time::timeout(Duration::from_secs(45), receiver.recv::<Body<Value>>())
        .await
        .expect("a message on the queue")
        .expect("recv delivery");
    receiver.accept(&delivery).await.expect("accept");
    let wire = delivery.message();
    let mut props = BTreeMap::new();
    if let Some(ap) = &wire.application_properties {
        for (k, v) in ap.0.iter() {
            props.insert(k.clone(), simple_to_string(v));
        }
    }
    let body = match &wire.body {
        Body::Data(batch) => {
            let mut b = Vec::new();
            for d in batch.iter() {
                b.extend_from_slice(&d.0);
            }
            b
        }
        Body::Value(v) => match &v.0 {
            Value::String(s) => s.clone().into_bytes(),
            Value::Binary(b) => b.to_vec(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    assert_eq!(body, b"{\"ok\":true}");
    assert_eq!(
        props.get("sutra-outbox-key").map(String::as_str),
        Some("ob-wire-1")
    );
    assert_eq!(
        props.get("content-type").map(String::as_str),
        Some("application/json")
    );
    // The AMQP 1.0 CE binding keeps the `ce-` DASH prefix on the wire (no JMS sanitisation).
    assert_eq!(props.get("ce-id").map(String::as_str), Some("evt-9"));
    assert_eq!(
        props.get("ce-type").map(String::as_str),
        Some("payment.reply")
    );
    assert_eq!(props.get("ce-source").map(String::as_str), Some("/acme"));
    assert_eq!(props.get("x-tenant").map(String::as_str), Some("acme"));
    assert_eq!(
        props.get("traceparent").map(String::as_str),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    sink.drain().await;
}

// ---- disposition semantics -----------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers_until_acked() {
    // NackRequeue → AMQP `release` → the broker redelivers; the redelivery is then acked.
    let port = broker_port();
    let queue = fresh_queue();
    let intake = ScriptedIntake::scripted(vec![AckDecision::NackRequeue], AckDecision::Ack);
    let source = source_for(port, &queue, "payments-in");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(port, &queue, "evt-release", "text/plain", b"retry-me").await;

    wait_until("redelivery after release", Duration::from_secs(20), || {
        intake.delivered_count() >= 2
    })
    .await;
    assert_eq!(intake.delivered_at(0).idempotency_key, "evt-release");
    assert_eq!(
        intake.delivered_at(1).idempotency_key,
        "evt-release",
        "the SAME outbox key rides the redelivery — inbox dedup absorbs it"
    );
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_drops_the_poison() {
    // NackDrop → AMQP `reject` → the broker DLQs/discards the poison; our consumer never
    // sees it again.
    let port = broker_port();
    let queue = fresh_queue();
    let intake = ScriptedIntake::always(AckDecision::NackDrop);
    let source = source_for(port, &queue, "payments-in");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(port, &queue, "evt-reject", "text/plain", b"poison").await;

    wait_until("first delivery", Duration::from_secs(45), || {
        intake.delivered_count() >= 1
    })
    .await;
    let before = intake.delivered_count();
    // Give the broker time to (not) redeliver — a rejected message is not returned to us.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        intake.delivered_count(),
        before,
        "a rejected message is not redelivered to this consumer"
    );
    assert_eq!(intake.delivered_at(0).idempotency_key, "evt-reject");
    source.stop().await.expect("stop");
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The broker-side half of the on-complete contract against a REAL AMQP 1.0 broker: the source
// hands its per-delivery accept / reject callbacks through `InboundIntake::deliver_deferred`,
// and the disposition is only written to the link when the instance's terminal event settles
// the `DeferredAckRegistry` entry (the engine-side half — dispatch parks → registry → listener
// bus — is `sutra-channels/tests/all/deferred_ack_test.rs`).
//
// Discriminator note (verified against the image, not assumed): this suite's queues are
// AUTO-CREATED Artemis addresses, which default to MULTICAST with a non-durable subscription —
// a message published with no consumer attached is dropped, and an UNSETTLED delivery is
// discarded (not re-queued) when the consumer's link detaches. So "is the message back on the
// queue?" cannot tell an accepted delivery apart from a never-settled one here. What these ITs
// pin is therefore the DEFERRAL itself, observed in-process against a real broker delivery:
// the source routes through `deliver_deferred`, never settles at dispatch-return, holds the
// entry across many receive-timeout turns, and fires exactly one callback at the terminal
// event. The disposition MAPPING those callbacks execute (`accept`/`release`/`reject`) is the
// same `settle()` the inline path uses, already proven on the broker by
// `release_redelivers_until_acked` + `reject_drops_the_poison` above.

/// The engine-actor stand-in for the on-complete seam: registers each delivery's settle
/// callbacks on a REAL [`DeferredAckRegistry`] under a synthetic instance id (exactly what the
/// dispatcher's park arm does) and answers `Deferred`; the test then fires the instance's
/// terminal event by hand.
struct DeferringIntake {
    registry: Arc<DeferredAckRegistry>,
    instances: Mutex<Vec<String>>,
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

    fn instance_at(&self, index: usize) -> String {
        self.instances.lock().unwrap()[index].clone()
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
            let instance_id = format!("inst-{}", uuid_ish());
            assert!(self.registry.register(
                &instance_id,
                &message.channel,
                settle.ack,
                settle.nack
            ));
            self.instances.lock().unwrap().push(instance_id);
            DeliveryDisposition::Deferred
        })
    }
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_withholds_the_disposition_until_the_instance_completes() {
    // message in → disposition DEFERRED and HELD across many receive-timeout turns → instance
    // completes → exactly one callback fires and the session task writes the `accept`.
    let port = broker_port();
    let queue = fresh_queue();
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-in", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(
        port,
        &queue,
        "evt-deferred-ack",
        "text/plain",
        b"deferred-1",
    )
    .await;
    wait_until("deferred delivery", Duration::from_secs(45), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(
        registry.pending_count(),
        1,
        "the settle is REGISTERED, not fired"
    );
    assert_eq!(
        *intake.plain_deliveries.lock().unwrap(),
        0,
        "an on-complete source routes through deliver_deferred, never plain deliver"
    );

    // HELD: eight receive-timeout turns (250ms each) later the entry is still pending — the
    // source never settles a deferred delivery on its own, whatever the consume loop does.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        registry.pending_count(),
        1,
        "the delivery stays unsettled while the instance runs"
    );
    assert_eq!(
        intake.instance_count(),
        1,
        "and it is not redelivered meanwhile"
    );

    // The instance's terminal event fires the held accept; the session task writes the
    // disposition on its next loop turn (≤ the 250ms receive timeout), and a healthy consume
    // loop shuts down cleanly afterwards (a failed disposition would have torn the session down
    // as ConnectionLost).
    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(
        registry.pending_count(),
        0,
        "exactly one callback consumed the entry"
    );
    tokio::time::sleep(Duration::from_millis(750)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_late_settle_after_the_session_died_is_a_no_op() {
    // The instance is still parked when the consumer goes away (drain / flip / crash): the
    // per-session settle bridge dies with the session, so a LATE terminal event finds it closed,
    // WARNs and no-ops instead of touching a detached link. The delivery was never settled, so
    // broker redelivery + inbox dedup are the documented recovery.
    let port = broker_port();
    let queue = fresh_queue();
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-in", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(
        port,
        &queue,
        "evt-still-running",
        "text/plain",
        b"still-running",
    )
    .await;
    wait_until("deferred delivery", Duration::from_secs(45), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    // Stop with the instance STILL parked — the delivery is unsettled at detach.
    source.stop().await.expect("stop");

    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(
        registry.pending_count(),
        0,
        "the entry is settled registry-side"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Nothing else may happen: no panic from the orphaned callback, no re-delivery.
    assert_eq!(intake.instance_count(), 1);
    source.stop().await.expect("second stop is a no-op");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_rejects_the_delivery_when_the_instance_fails() {
    // failure path: message in → disposition deferred → instance FAILS → the nack callback,
    // which maps to `reject` — the AMQP 1.0 DROP posture (the broker DLQs/discards the poison),
    // never `release`. The reject/release split itself is pinned on the broker by
    // `reject_drops_the_poison` / `release_redelivers_until_acked` on the same `settle()`.
    let port = broker_port();
    let queue = fresh_queue();
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-in", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(port, &queue, "evt-will-fail", "text/plain", b"will-fail").await;
    wait_until("deferred delivery", Duration::from_secs(45), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    registry.on_instance_failed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(750)).await;

    // A rejected delivery is NOT released back to this consumer (the poison never loops).
    assert_eq!(
        intake.instance_count(),
        1,
        "the drop posture never redelivers the failed message to us"
    );
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_persist_source_still_settles_at_dispatch_return() {
    // Regression pin for the untouched path: an on-persist source keeps calling plain
    // `deliver` and accepts at dispatch-return — the deferred seam is never involved.
    let port = broker_port();
    let queue = fresh_queue();
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_for(port, &queue, "payments-in"); // OnPersist
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    raw_send(port, &queue, "evt-classic", "text/plain", b"classic").await;
    wait_until("plain delivery", Duration::from_secs(45), || {
        *intake.plain_deliveries.lock().unwrap() >= 1
    })
    .await;
    assert_eq!(intake.instance_count(), 0, "deliver_deferred never called");
    assert_eq!(registry.pending_count(), 0, "nothing was ever deferred");
    // Accepted at dispatch-return ⇒ the broker never redelivers it to us.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(*intake.plain_deliveries.lock().unwrap(), 1);
    source.stop().await.expect("stop");
}
