//! RabbitMQ transport integration suite — Testcontainers-backed
//! (`rabbitmq:3.13-management-alpine`, the only broker image the integration gate uses).
//! Exercises the transport seams end to end: the source delivers into a scripted
//! [`InboundIntake`] and each [`AckDecision`] is proven on the
//! broker (ack consumes, NackRequeue redelivers, NackDrop lands in DLX posture), the
//! sink publishes with the FROZEN wire projection (outbox key on `message-id`,
//! persistent delivery), and the consumer survives a broker restart (reconnect/backoff).
//!
//! Requires a Docker daemon (same posture as the sutra-persistence `pg` suite).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lapin::options::{BasicGetOptions, BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use sutra_channels::auth::BrokerInboundAuth;
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};
use sutra_channels::{BoxFuture, DeferredAckRegistry, InboundMessage};
use sutra_transport_rabbitmq::{
    AckMode, RabbitMqChannelProperties, RabbitMqMessageSink, RabbitMqSourceConfig,
    RabbitMqTriggerSource,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

// ---- shared broker fixture -----------------------------------------------------------------

static BROKER: OnceLock<(Container<GenericImage>, u16)> = OnceLock::new();

fn rabbit_image() -> GenericImage {
    GenericImage::new("rabbitmq", "3.13-management-alpine")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
}

/// One shared broker per test binary; each test declares its own fresh queue.
fn broker_port() -> u16 {
    let (_, port) = BROKER.get_or_init(|| {
        // Blocking runner on a dedicated thread — must not run inside a tokio worker.
        std::thread::spawn(|| {
            let container = rabbit_image()
                .start()
                .expect("start rabbitmq:3.13-management-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5672).expect("mapped 5672");
            (container, port)
        })
        .join()
        .expect("broker bootstrap thread")
    });
    *port
}

async fn raw_connection(port: u16) -> Connection {
    Connection::connect(
        &format!("amqp://127.0.0.1:{port}"),
        ConnectionProperties::default(),
    )
    .await
    .expect("raw AMQP connection")
}

async fn declare_fresh_queue(port: u16) -> String {
    let name = format!("q-{}", uuid_ish());
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .queue_declare(
            &name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
    connection.close(200, "declared").await.ok();
    name
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

async fn publish(port: u16, queue: &str, properties: BasicProperties, body: &[u8]) {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .basic_publish("", queue, BasicPublishOptions::default(), body, properties)
        .await
        .expect("publish")
        .await
        .expect("confirm");
    connection.close(200, "published").await.ok();
}

async fn consumer_count(port: u16, queue: &str) -> u32 {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let q = channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let count = q.consumer_count();
    connection.close(200, "counted").await.ok();
    count
}

async fn message_count(port: u16, queue: &str) -> u32 {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let q = channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let count = q.message_count();
    connection.close(200, "counted").await.ok();
    count
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

async fn wait_for_message_count(port: u16, queue: &str, expected: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if message_count(port, queue).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for message count {expected} on {queue}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_consumer_count(port: u16, queue: &str, expected: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if consumer_count(port, queue).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for consumer count {expected} on {queue}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---- test doubles ---------------------------------------------------------------------------

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

fn source_for(port: u16, queue: &str, channel: &str) -> RabbitMqTriggerSource {
    source_with_mode(port, queue, channel, AckMode::OnPersist)
}

fn source_with_mode(
    port: u16,
    queue: &str,
    channel: &str,
    ack_mode: AckMode,
) -> RabbitMqTriggerSource {
    let properties = RabbitMqChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        virtual_host: "/".to_string(),
        username: None, // anonymous session — the image allows guest defaults
        password: None,
        queue: queue.to_string(),
        exchange: String::new(),
        prefetch_count: 10,
        ack_mode,
        singleton: false,
    };
    let mut config = RabbitMqSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    RabbitMqTriggerSource::new(config).expect("source")
}

// ---- inbound ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_delivers_body_headers_and_message_id_as_idempotency_key() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(port, &queue, "payments-inbound");

    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    let props = BasicProperties::default()
        .with_message_id("order-1".into())
        .with_content_type("application/xml".into())
        .with_reply_to("amqp://reply-exchange/rk".into())
        .with_headers({
            let mut t = FieldTable::default();
            t.insert(
                "x-tenant".into(),
                lapin::types::AMQPValue::LongString("acme".into()),
            );
            t
        });
    publish(port, &queue, props, b"<Document/>").await;

    wait_until("first delivery", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    assert_eq!(m.body.into_inner(), b"<Document/>");
    assert_eq!(m.idempotency_key, "order-1");
    assert!(m.explicit_event_id);
    assert_eq!(m.content_type.as_deref(), Some("application/xml"));
    assert_eq!(m.headers.get("x-tenant").map(String::as_str), Some("acme"));
    assert_eq!(
        m.headers.get("x-amqp-reply-to").map(String::as_str),
        Some("amqp://reply-exchange/rk")
    );
    assert_eq!(m.tenant, "acme");
    assert_eq!(m.channel, "payments-inbound");

    // Ack consumed the message — the queue drains.
    wait_for_message_count(port, &queue, 0, Duration::from_secs(5)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_key_falls_back_to_delivery_tag_when_message_id_absent() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(port, &queue, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(port, &queue, BasicProperties::default(), b"no-message-id").await;
    wait_until("delivery", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    assert!(
        m.idempotency_key.chars().all(|c| c.is_ascii_digit()),
        "delivery-tag fallback must be numeric, got '{}'",
        m.idempotency_key
    );
    assert!(!m.explicit_event_id, "the fallback key is non-explicit");
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_until_acked() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    // First decision: transient failure (persistence down) — broker must redeliver;
    // the redelivery is then acked.
    let intake = ScriptedIntake::scripted(vec![AckDecision::NackRequeue], AckDecision::Ack);
    let source = source_for(port, &queue, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("evt-requeue".into()),
        b"retry-me",
    )
    .await;

    wait_until(
        "redelivery after nack(requeue)",
        Duration::from_secs(10),
        || intake.delivered_count() >= 2,
    )
    .await;
    assert_eq!(intake.delivered_at(0).idempotency_key, "evt-requeue");
    assert_eq!(
        intake.delivered_at(1).idempotency_key,
        "evt-requeue",
        "the SAME message id rides the redelivery — inbox dedup absorbs it"
    );
    // The second (acked) pass settles the queue.
    wait_for_message_count(port, &queue, 0, Duration::from_secs(5)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_drop_rejects_without_redelivery() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::NackDrop);
    let source = source_for(port, &queue, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("poison".into()),
        b"poison",
    )
    .await;

    wait_until("poison delivery", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;
    // requeue=false: no DLX bound here, so the message is DROPPED — exactly one call
    // even after waiting.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(intake.delivered_count(), 1);
    wait_for_message_count(port, &queue, 0, Duration::from_secs(5)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_cancels_the_consumer_and_later_publishes_stay_queued() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(port, &queue, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("first".into()),
        b"first",
    )
    .await;
    wait_until("first delivery", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;

    source.stop().await.expect("drain");
    wait_for_consumer_count(port, &queue, 0, Duration::from_secs(5)).await;

    let before = intake.delivered_count();
    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("second".into()),
        b"second",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(intake.delivered_count(), before, "no delivery after drain");
    assert_eq!(message_count(port, &queue).await, 1, "second stays queued");
}

// ---- singleton gating (transport mechanics; the lease-backed proof lives in
// sutra-engine's IT) ---------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leadership_gate_keeps_exactly_one_consumer_and_hands_over() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake_a = ScriptedIntake::always(AckDecision::Ack);
    let intake_b = ScriptedIntake::always(AckDecision::Ack);
    let gate_a = FlippableGate::leading(true);
    let gate_b = FlippableGate::leading(false);
    let source_a = source_for(port, &queue, "transfer-queue");
    let source_b = source_for(port, &queue, "transfer-queue");

    source_a
        .start(intake_a.clone(), gate_a.clone())
        .await
        .expect("start A");
    source_b
        .start(intake_b.clone(), gate_b.clone())
        .await
        .expect("start B");

    // Exactly ONE active consumer while A leads (B stays subscribed to nothing).
    wait_for_consumer_count(port, &queue, 1, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(consumer_count(port, &queue).await, 1, "count stays 1");

    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("m-A".into()),
        b"to-A",
    )
    .await;
    wait_until("A consumed", Duration::from_secs(10), || {
        intake_a.delivered_count() >= 1
    })
    .await;
    assert_eq!(
        intake_b.delivered_count(),
        0,
        "the follower consumes nothing"
    );

    // Handover: A's gate revokes, B's grants — A cancels within a gate poll, B
    // subscribes; the consumer count returns to exactly 1 and B consumes. A may still
    // drain one probe during the transition window, so probe until B receives.
    gate_a.set(false);
    gate_b.set(true);
    let handover_deadline = Instant::now() + Duration::from_secs(15);
    let mut probe = 0;
    while intake_b.delivered_count() == 0 {
        assert!(
            Instant::now() < handover_deadline,
            "handover never completed — B consumed nothing"
        );
        probe += 1;
        publish(
            port,
            &queue,
            BasicProperties::default().with_message_id(format!("m-B-{probe}").into()),
            b"to-B",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let a_before = intake_a.delivered_count();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(consumer_count(port, &queue).await, 1, "still exactly one");
    assert_eq!(intake_a.delivered_count(), a_before, "A no longer consumes");

    source_a.stop().await.expect("stop A");
    source_b.stop().await.expect("stop B");
}

// ---- reconnect --------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_reconnects_after_a_broker_restart() {
    // Dedicated container — restarting the shared broker would disturb parallel tests.
    // `docker restart` RE-ALLOCATES ephemeral host-port mappings, so the broker must be
    // bound to an EXPLICIT host port (dynamically picked, never hardcoded) for the
    // reconnect target to stay stable across the restart.
    let (container_id, port) = std::thread::spawn(|| {
        let free_port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("probe listener")
            .local_addr()
            .expect("probe addr")
            .port();
        let container = rabbit_image()
            .with_mapped_port(free_port, 5672.tcp())
            .start()
            .expect("start dedicated rabbitmq (docker required)");
        let id = container.id().to_string();
        // Keep the container alive for the whole test binary. testcontainers-rs 0.25 has no
        // ryuk reaper, so a forgotten handle would leak; register the id for the process-exit
        // reaper (sutra-testkit) which force-removes it when the test process ends.
        sutra_testkit::reap_on_exit(&id);
        std::mem::forget(container);
        (id, free_port)
    })
    .join()
    .expect("dedicated broker thread");

    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(port, &queue, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(
        port,
        &queue,
        BasicProperties::default().with_message_id("before-restart".into()),
        b"one",
    )
    .await;
    wait_until("delivery before restart", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;

    // `docker restart` keeps the mapped port; the supervisor must reconnect+resubscribe.
    let status = std::process::Command::new("docker")
        .args(["restart", &container_id])
        .status()
        .expect("docker restart");
    assert!(status.success(), "docker restart failed");

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // The broker may briefly refuse connections while booting.
        let publish_attempt = async {
            let connection = Connection::connect(
                &format!("amqp://127.0.0.1:{port}"),
                ConnectionProperties::default(),
            )
            .await
            .ok()?;
            let channel = connection.create_channel().await.ok()?;
            channel
                .basic_publish(
                    "",
                    &queue,
                    BasicPublishOptions::default(),
                    b"two",
                    BasicProperties::default().with_message_id("after-restart".into()),
                )
                .await
                .ok()?
                .await
                .ok()?;
            connection.close(200, "published").await.ok();
            Some(())
        }
        .await;
        if publish_attempt.is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "broker never came back");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    wait_until("delivery after restart", Duration::from_secs(30), || {
        intake
            .delivered
            .lock()
            .unwrap()
            .iter()
            .any(|m| m.idempotency_key == "after-restart")
    })
    .await;
    source.stop().await.expect("stop");
}

// ---- outbound ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_publishes_with_the_frozen_wire_projection() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let sink = RabbitMqMessageSink::new();

    let mut headers = std::collections::BTreeMap::new();
    headers.insert("x-tenant".to_string(), "acme".to_string());
    headers.insert("x-trace-id".to_string(), "trace-9".to_string());
    let message = OutboundMessage {
        destination: format!("rabbitmq://127.0.0.1:{port}/{queue}"),
        headers,
        body: b"{\"ok\":true}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: "outbox-abc-123".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };

    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let mut got = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(delivery) = channel
            .basic_get(&queue, BasicGetOptions { no_ack: true })
            .await
            .expect("basic.get")
        {
            got = Some(delivery);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let delivery = got.expect("published message");
    assert_eq!(delivery.data, b"{\"ok\":true}");
    // FROZEN: the outbox key rides AMQP message-id.
    assert_eq!(
        delivery
            .properties
            .message_id()
            .as_ref()
            .map(|s| s.as_str()),
        Some("outbox-abc-123")
    );
    assert_eq!(*delivery.properties.delivery_mode(), Some(2));
    assert_eq!(
        delivery
            .properties
            .content_type()
            .as_ref()
            .map(|s| s.as_str()),
        Some("application/json")
    );
    let amqp_headers = delivery.properties.headers().as_ref().expect("headers");
    let header = |k: &str| {
        amqp_headers
            .inner()
            .get(&lapin::types::ShortString::from(k))
            .map(|v| match v {
                lapin::types::AMQPValue::LongString(s) => {
                    String::from_utf8_lossy(s.as_bytes()).into_owned()
                }
                other => format!("{other:?}"),
            })
    };
    assert_eq!(header("x-tenant").as_deref(), Some("acme"));
    assert_eq!(header("x-trace-id").as_deref(), Some("trace-9"));
    assert_eq!(
        header("traceparent").as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    connection.close(200, "asserted").await.ok();
    sink.drain().await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_retry_delivers_twice_with_the_same_message_id() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let sink = RabbitMqMessageSink::new();
    let message = OutboundMessage {
        destination: format!("rabbitmq://127.0.0.1:{port}/{queue}"),
        headers: std::collections::BTreeMap::new(),
        body: b"payload".to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: "shared-outbox-key".to_string(),
        traceparent: None,
    };

    // Simulate a crash-during-send replay from a sibling replica: RabbitMQ has no
    // native id dedup — BOTH land, with the SAME message-id so a consumer-side dedup
    // table can drop the duplicate (the outbox<->sink contract).
    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);
    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let mut ids = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while ids.len() < 2 && Instant::now() < deadline {
        if let Some(delivery) = channel
            .basic_get(&queue, BasicGetOptions { no_ack: true })
            .await
            .expect("basic.get")
        {
            ids.push(
                delivery
                    .properties
                    .message_id()
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
            );
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert_eq!(ids, vec!["shared-outbox-key", "shared-outbox-key"]);
    connection.close(200, "asserted").await.ok();
    sink.drain().await;
}

// ---- message-level inbound auth (round trip) ------------------------------------------------

/// A parsed apikey inbound-auth config expecting `expected` in the `X-API-Key` header.
fn apikey_auth(expected: &str) -> BrokerInboundAuth {
    let props: std::collections::BTreeMap<String, String> = [
        ("inbound-auth.scheme".to_string(), "apikey".to_string()),
        (
            "inbound-auth.expected-key-ref".to_string(),
            format!("literal:{expected}"),
        ),
    ]
    .into();
    BrokerInboundAuth::from_properties(&props, "SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID", |r| {
        r.strip_prefix("literal:")
            .map(str::to_string)
            .ok_or_else(|| "unresolved".to_string())
    })
    .expect("auth parses")
    .expect("auth configured")
}

fn source_with_auth(
    port: u16,
    queue: &str,
    channel: &str,
    auth: BrokerInboundAuth,
) -> RabbitMqTriggerSource {
    let properties = RabbitMqChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        virtual_host: "/".to_string(),
        username: None,
        password: None,
        queue: queue.to_string(),
        exchange: String::new(),
        prefetch_count: 10,
        ack_mode: AckMode::OnPersist,
        singleton: false,
    };
    let mut config = RabbitMqSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    config.inbound_auth = Some(auth);
    RabbitMqTriggerSource::new(config).expect("source")
}

fn apikey_props(key: &str) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(
        "X-API-Key".into(),
        lapin::types::AMQPValue::LongString(key.into()),
    );
    BasicProperties::default().with_headers(headers)
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_apikey_drops_unauthorised_and_delivers_authorised() {
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_with_auth(port, &queue, "payments-inbound", apikey_auth("correct-key"));
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    // A wrong key is NackDrop-dropped; the correct key is delivered. FIFO on one queue means
    // only the authorised message reaching intake proves the first was dropped.
    publish(port, &queue, apikey_props("wrong"), b"unauthorised").await;
    publish(port, &queue, apikey_props("correct-key"), b"authorised").await;

    wait_until("authorised delivery", Duration::from_secs(10), || {
        intake.delivered_count() >= 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(
        intake.delivered_count(),
        1,
        "only the authorised delivery reaches intake (the wrong key was dropped)"
    );
    assert_eq!(intake.delivered_at(0).body.into_inner(), b"authorised");
    source.stop().await.expect("stop");
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The broker-side half of the on-complete contract against a REAL RabbitMQ: the source
// hands its per-delivery basic.ack / basic.nack(requeue=false) callbacks through
// `InboundIntake::deliver_deferred`, and the settle fires only when the instance's
// terminal event settles the `DeferredAckRegistry` entry (the engine-side half —
// dispatch parks → registry → listener bus — is `sutra-channels/tests/all/deferred_ack_test.rs`).

/// The engine-actor stand-in for the on-complete seam: registers each delivery's settle
/// callbacks on a REAL [`DeferredAckRegistry`] under a synthetic instance id (exactly
/// what the dispatcher's park arm does) and answers `Deferred`; the test then fires the
/// instance's terminal event by hand.
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
async fn on_complete_withholds_the_broker_ack_until_the_instance_completes() {
    // message in → ack DEFERRED → instance completes → basic.ack fires on the broker.
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(port, &queue, BasicProperties::default(), b"deferred-1").await;
    wait_until("deferred delivery", Duration::from_secs(10), || {
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

    // The instance's terminal event fires the held basic.ack (spawned onto the runtime);
    // give it a moment to land on the broker BEFORE the consumer is cancelled.
    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The discriminator: cancelling the consumer REQUEUES any still-unacked delivery.
    // The queue staying empty after stop proves the ack really landed.
    source.stop().await.expect("stop");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        message_count(port, &queue).await,
        0,
        "the deferred ack consumed the message — nothing requeued at consumer cancel"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_holds_the_delivery_unacked_while_the_instance_runs() {
    // The withheld-ack proof (the converse discriminator): with NO terminal event, the
    // delivery is still unacked when the consumer cancels — RabbitMQ requeues it.
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(port, &queue, BasicProperties::default(), b"still-running").await;
    wait_until("deferred delivery", Duration::from_secs(10), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    source.stop().await.expect("stop");
    wait_for_message_count(port, &queue, 1, Duration::from_secs(5)).await; // requeued — never acked

    // A LATE terminal event (instance completed after the session died) fires the ack on
    // the dead AMQP channel: logged WARN, swallowed, and the broker keeps the message —
    // redelivery + inbox dedup are the documented recovery.
    registry.on_instance_completed(&intake.instance_at(0));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        message_count(port, &queue).await,
        1,
        "the late ack on the dead channel cannot consume the requeued message"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_nacks_without_requeue_when_the_instance_fails() {
    // failure path: message in → ack deferred → instance FAILS → basic.nack(requeue=false)
    // — the DLQ posture: no DLX is configured on the fresh queue, so the delivery drops.
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(port, &queue, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(port, &queue, BasicProperties::default(), b"will-fail").await;
    wait_until("deferred delivery", Duration::from_secs(10), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    registry.on_instance_failed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // requeue=false: the message must NOT come back when the consumer cancels.
    source.stop().await.expect("stop");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        message_count(port, &queue).await,
        0,
        "the deferred nack dropped the delivery (DLX posture) — nothing requeued"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_persist_source_still_settles_at_dispatch_return() {
    // Regression pin for the untouched path: an on-persist source keeps calling plain
    // `deliver` and acks at dispatch-return — the deferred seam is never involved.
    let port = broker_port();
    let queue = declare_fresh_queue(port).await;
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_for(port, &queue, "payments-inbound"); // OnPersist
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(port, &queue, BasicProperties::default(), b"classic").await;
    wait_until("plain delivery", Duration::from_secs(10), || {
        *intake.plain_deliveries.lock().unwrap() >= 1
    })
    .await;
    assert_eq!(intake.instance_count(), 0, "deliver_deferred never called");
    assert_eq!(registry.pending_count(), 0);
    wait_for_message_count(port, &queue, 0, Duration::from_secs(5)).await; // acked immediately
    source.stop().await.expect("stop");
}
