//! AWS SQS transport integration suite — Testcontainers-backed
//! (`localstack/localstack:3`, SQS service only). Exercises the transport seams end to end: the
//! source delivers into a scripted [`InboundIntake`] and each [`AckDecision`] is proven on the
//! broker (Ack/NackDrop delete, NackRequeue leaves the message for the visibility-timeout to
//! redeliver), and the sink publishes with the FROZEN wire projection (outbox key on the
//! `sutra-outbox-key` message attribute, `content-type` attribute, CE `ce-*` dash attributes
//! carried verbatim).
//!
//! Requires a Docker daemon (same posture as the kafka / rabbitmq / pg suites).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client;
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};
use sutra_channels::{BoxFuture, DeferredAckRegistry, InboundMessage};
use sutra_transport_sqs::{
    build_client, AckMode, SqsChannelProperties, SqsMessageSink, SqsSinkSettings, SqsSourceConfig,
    SqsTriggerSource,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

// ---- shared LocalStack fixture -------------------------------------------------------------

static LOCALSTACK: OnceLock<(Container<GenericImage>, u16)> = OnceLock::new();

const REGION: &str = "us-east-1";
const ACCOUNT_ID: &str = "000000000000";

/// One shared LocalStack (SQS only) per test binary; each test creates its own fresh queue.
fn localstack_port() -> u16 {
    let (_, port) = LOCALSTACK.get_or_init(|| {
        // Blocking runner on a dedicated thread — must not run inside a tokio worker.
        std::thread::spawn(|| {
            let container = GenericImage::new("localstack/localstack", "3")
                .with_exposed_port(4566.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Ready."))
                .with_env_var("SERVICES", "sqs")
                .start()
                .expect("start localstack/localstack:3 (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container
                .get_host_port_ipv4(4566.tcp())
                .expect("mapped 4566");
            (container, port)
        })
        .join()
        .expect("localstack bootstrap thread")
    });
    *port
}

fn endpoint() -> String {
    format!("http://127.0.0.1:{}", localstack_port())
}

fn client() -> Client {
    build_client(REGION, Some(&endpoint()))
}

/// Unique-enough queue name per test (no uuid dependency).
fn fresh_queue_name() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "q-{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// Create the queue and return its URL.
async fn create_queue(name: &str) -> String {
    client()
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .expect("create queue")
        .queue_url()
        .expect("queue url")
        .to_string()
}

/// Create the queue with a custom visibility timeout (seconds) and return its URL.
async fn create_queue_with_visibility(name: &str, visibility: i32) -> String {
    use aws_sdk_sqs::types::QueueAttributeName;
    client()
        .create_queue()
        .queue_name(name)
        .attributes(
            QueueAttributeName::VisibilityTimeout,
            visibility.to_string(),
        )
        .send()
        .await
        .expect("create queue")
        .queue_url()
        .expect("queue url")
        .to_string()
}

async fn send_raw(queue_url: &str, attrs: &[(&str, &str)], body: &[u8]) {
    let mut request = client()
        .send_message()
        .queue_url(queue_url)
        .message_body(String::from_utf8_lossy(body).into_owned());
    for (name, value) in attrs {
        let attr = MessageAttributeValue::builder()
            .data_type("String")
            .string_value(*value)
            .build()
            .expect("attr");
        request = request.message_attributes(*name, attr);
    }
    request.send().await.expect("send");
}

/// Read one message off a queue (long poll), with all message attributes.
async fn receive_one_raw(queue_url: &str) -> aws_sdk_sqs::types::Message {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = client()
            .receive_message()
            .queue_url(queue_url)
            .wait_time_seconds(2)
            .max_number_of_messages(1)
            .message_attribute_names("All")
            .send()
            .await
            .expect("receive");
        if let Some(first) = response.messages().first() {
            return first.clone();
        }
        if Instant::now() >= deadline {
            panic!("no message within timeout");
        }
    }
}

/// `(visible, in-flight)` message counts — the SQS-native discriminator between "deleted"
/// `(0, 0)`, "held un-deleted in flight" `(0, 1)` and "back on the queue" `(1, 0)`.
async fn queue_depth(queue_url: &str) -> (u32, u32) {
    use aws_sdk_sqs::types::QueueAttributeName;
    let response = client()
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .expect("get queue attributes");
    let read = |name: &QueueAttributeName| {
        response
            .attributes()
            .and_then(|a| a.get(name))
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0)
    };
    (
        read(&QueueAttributeName::ApproximateNumberOfMessages),
        read(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
    )
}

async fn wait_for_depth(queue_url: &str, expected: (u32, u32), timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let depth = queue_depth(queue_url).await;
        if depth == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for queue depth {expected:?} (visible, in-flight) — last saw {depth:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn attr_value(message: &aws_sdk_sqs::types::Message, key: &str) -> Option<String> {
    message
        .message_attributes()?
        .get(key)
        .and_then(|v| v.string_value())
        .map(str::to_string)
}

async fn wait_until<F: Fn() -> bool>(what: &str, timeout: Duration, check: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
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

fn source_for(queue_url: &str, channel: &str, visibility: i32) -> SqsTriggerSource {
    source_with_mode(queue_url, channel, visibility, AckMode::OnPersist)
}

fn source_with_mode(
    queue_url: &str,
    channel: &str,
    visibility: i32,
    ack_mode: AckMode,
) -> SqsTriggerSource {
    let properties = SqsChannelProperties {
        region: REGION.to_string(),
        queue_url: queue_url.to_string(),
        wait_time_seconds: 1,
        max_messages: 10,
        visibility_timeout_seconds: visibility,
        endpoint_override: Some(endpoint()),
        ack_mode,
        singleton: false,
    };
    let mut config = SqsSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    SqsTriggerSource::new(config).expect("source")
}

fn sink_settings() -> SqsSinkSettings {
    SqsSinkSettings {
        region: REGION.to_string(),
        account_id: Some(ACCOUNT_ID.to_string()),
        endpoint_override: Some(endpoint()),
    }
}

// ---- inbound ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_delivers_body_attrs_and_outbox_key_as_idempotency_key() {
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    send_raw(
        &queue_url,
        &[
            ("sutra-outbox-key", "order-1"),
            ("content-type", "application/xml"),
            ("x-tenant", "acme"),
        ],
        b"<Document/>",
    )
    .await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&queue_url, "payments-inbound", 30);
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
    // FROZEN — the sutra-outbox-key attribute is the explicit idempotency key.
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
async fn idempotency_key_falls_back_to_message_id_non_explicit() {
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    send_raw(&queue_url, &[("content-type", "text/plain")], b"no-key").await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&queue_url, "payments-inbound", 30);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    wait_until("delivery", Duration::from_secs(25), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    // FROZEN — SQS MessageId fallback, non-explicit (never suppresses a re-post).
    assert!(!m.idempotency_key.is_empty());
    assert!(
        !m.explicit_event_id,
        "the MessageId fallback is non-explicit"
    );
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_leaves_in_flight_and_redelivers_until_acked() {
    let name = fresh_queue_name();
    // Short visibility timeout so the NackRequeue redelivery lands quickly.
    let queue_url = create_queue_with_visibility(&name, 2).await;
    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-requeue")],
        b"retry-me",
    )
    .await;

    // First decision: transient failure — the source leaves the message in flight and the
    // visibility timeout redelivers it; the redelivery is then acked (deleted).
    let intake = ScriptedIntake::scripted(vec![AckDecision::NackRequeue], AckDecision::Ack);
    let source = source_for(&queue_url, "payments-inbound", 2);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until(
        "redelivery after nack(requeue)",
        Duration::from_secs(30),
        || intake.delivered_count() >= 2,
    )
    .await;
    assert_eq!(intake.delivered_at(0).idempotency_key, "evt-requeue");
    assert_eq!(
        intake.delivered_at(1).idempotency_key,
        "evt-requeue",
        "the SAME message rides the redelivery — inbox dedup absorbs it"
    );
    source.stop().await.expect("stop");
}

// ---- singleton gating (transport mechanics; the lease-backed proof lives in
// sutra-engine's aws-sqs rewire IT) ---------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leadership_gate_keeps_exactly_one_consumer_and_hands_over() {
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let intake_a = ScriptedIntake::always(AckDecision::Ack);
    let intake_b = ScriptedIntake::always(AckDecision::Ack);
    let gate_a = FlippableGate::leading(true);
    let gate_b = FlippableGate::leading(false);
    let source_a = source_for(&queue_url, "transfer-queue", 5);
    let source_b = source_for(&queue_url, "transfer-queue", 5);

    source_a
        .start(intake_a.clone(), gate_a.clone())
        .await
        .expect("start A");
    source_b
        .start(intake_b.clone(), gate_b.clone())
        .await
        .expect("start B");

    send_raw(&queue_url, &[("sutra-outbox-key", "m-A")], b"to-A").await;
    wait_until("A consumed", Duration::from_secs(25), || {
        intake_a.delivered_count() >= 1
    })
    .await;
    // The follower (gate false) consumes nothing.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        intake_b.delivered_count(),
        0,
        "the follower consumes nothing"
    );

    // Handover: A's gate revokes, B's grants — A pauses within a gate poll, B starts
    // polling; the queue returns to exactly one consumer and B consumes. A may still grab
    // one probe during the in-flight long-poll transition window (a benign at-least-once
    // overlap), so probe until B receives (mirrors the rabbitmq handover IT).
    gate_a.set(false);
    gate_b.set(true);
    let handover_deadline = Instant::now() + Duration::from_secs(25);
    let mut probe = 0;
    while intake_b.delivered_count() == 0 {
        assert!(
            Instant::now() < handover_deadline,
            "handover never completed — B consumed nothing"
        );
        probe += 1;
        let key = format!("m-after-{probe}");
        send_raw(&queue_url, &[("sutra-outbox-key", key.as_str())], b"after").await;
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    source_a.stop().await.expect("stop A");
    source_b.stop().await.expect("stop B");
}

// ---- outbound (the m9 wire projection) ------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_publishes_with_the_frozen_wire_projection() {
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let sink = SqsMessageSink::new(sink_settings());

    let mut headers = BTreeMap::new();
    headers.insert("x-tenant".to_string(), "acme".to_string());
    // A `ce-*` binary-binding attribute (projected upstream by the dispatcher) rides verbatim.
    headers.insert("ce-type".to_string(), "io.sutra.reply.v1".to_string());
    headers.insert("ce-id".to_string(), "ce-7".to_string());
    let message = OutboundMessage {
        // aws-sqs://<queueName> — resolved via the sink settings (region/account/endpoint).
        destination: format!("aws-sqs://{name}"),
        headers,
        body: b"{\"ok\":true}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: "outbox-abc-123".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };

    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let received = receive_one_raw(&queue_url).await;
    assert_eq!(received.body(), Some("{\"ok\":true}"));
    // FROZEN — the outbox key rides the sutra-outbox-key attribute (shared dedup token).
    assert_eq!(
        attr_value(&received, "sutra-outbox-key").as_deref(),
        Some("outbox-abc-123")
    );
    assert_eq!(
        attr_value(&received, "content-type").as_deref(),
        Some("application/json")
    );
    assert_eq!(attr_value(&received, "x-tenant").as_deref(), Some("acme"));
    // The SQS CE binding prefix is `ce-` (dash) — carried verbatim by the sink.
    assert_eq!(
        attr_value(&received, "ce-type").as_deref(),
        Some("io.sutra.reply.v1")
    );
    assert_eq!(attr_value(&received, "ce-id").as_deref(), Some("ce-7"));
    assert_eq!(
        attr_value(&received, "traceparent").as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    sink.drain().await;
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The broker-side half of the on-complete contract against a REAL SQS (LocalStack): the
// source hands its per-delivery DeleteMessage callbacks through
// `InboundIntake::deliver_deferred`, and the settle fires only when the instance's terminal
// event settles the `DeferredAckRegistry` entry (the engine-side half — dispatch parks →
// registry → listener bus — is `sutra-channels/tests/all/deferred_ack_test.rs`).
//
// SQS-SPECIFIC: both held callbacks are a DeleteMessage. That is not a shortcut — it is
// exactly what the settle-at-return path already does for `Ack` and `NackDrop` alike (SQS
// has no immediate dead-letter hand-off; its redrive policy is receive-count driven), so
// "the instance failed" means the delivery is DROPPED by deleting it. The discriminator
// between the two paths is therefore the registry event that fires, not the SQS verb.

/// The engine-actor stand-in for the on-complete seam: registers each delivery's settle
/// callbacks on a REAL [`DeferredAckRegistry`] under a synthetic instance id (exactly what
/// the dispatcher's park arm does) and answers `Deferred`; the test then fires the
/// instance's terminal event by hand.
struct DeferringIntake {
    registry: Arc<DeferredAckRegistry>,
    instances: Mutex<Vec<String>>,
    deferred: Mutex<Vec<InboundMessage>>,
    plain_deliveries: Mutex<usize>,
}

impl DeferringIntake {
    fn new(registry: Arc<DeferredAckRegistry>) -> Arc<DeferringIntake> {
        Arc::new(DeferringIntake {
            registry,
            instances: Mutex::new(Vec::new()),
            deferred: Mutex::new(Vec::new()),
            plain_deliveries: Mutex::new(0),
        })
    }

    fn instance_count(&self) -> usize {
        self.instances.lock().unwrap().len()
    }

    fn instance_at(&self, index: usize) -> String {
        self.instances.lock().unwrap()[index].clone()
    }

    fn deferred_at(&self, index: usize) -> InboundMessage {
        self.deferred.lock().unwrap()[index].clone()
    }

    fn plain_count(&self) -> usize {
        *self.plain_deliveries.lock().unwrap()
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
            let instance_id = format!("inst-{}", fresh_queue_name());
            assert!(self.registry.register(
                &instance_id,
                &message.channel,
                settle.ack,
                settle.nack
            ));
            self.deferred.lock().unwrap().push(message);
            self.instances.lock().unwrap().push(instance_id);
            DeliveryDisposition::Deferred
        })
    }
}

fn registry() -> Arc<DeferredAckRegistry> {
    Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)))
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_withholds_the_delete_until_the_instance_completes() {
    // message in → delete DEFERRED (message stays in flight) → instance completes →
    // DeleteMessage fires and the queue drains.
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    // A long visibility window so the held delete is unambiguously the thing that consumes
    // the message (no expiry-driven redelivery inside the test).
    let source = source_with_mode(&queue_url, "payments-inbound", 60, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-deferred-1")],
        b"deferred-1",
    )
    .await;
    wait_until("deferred delivery", Duration::from_secs(30), || {
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
    // Held: in flight (received, invisible) but NOT deleted.
    wait_for_depth(&queue_url, (0, 1), Duration::from_secs(15)).await;

    // The instance's terminal event fires the held DeleteMessage (spawned onto the runtime).
    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    // (0, 0) is the discriminator: not merely invisible — GONE.
    wait_for_depth(&queue_url, (0, 0), Duration::from_secs(15)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_holds_the_delivery_undeleted_while_the_instance_runs() {
    // The withheld-delete proof (the converse discriminator): with NO terminal event the
    // message is never deleted — it stays in flight and returns to the queue when the
    // visibility timeout lapses.
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    // Short visibility so "still on the queue" is observable inside the test budget.
    let source = source_with_mode(&queue_url, "payments-inbound", 3, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-running")],
        b"still-running",
    )
    .await;
    wait_until("deferred delivery", Duration::from_secs(30), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);
    // Stop BEFORE the visibility timeout lapses so the source cannot re-receive it.
    source.stop().await.expect("stop");

    // Never deleted: the visibility timeout returns it to the queue.
    wait_for_depth(&queue_url, (1, 0), Duration::from_secs(20)).await;

    // A LATE terminal event (instance completed after the session died) fires the held
    // DeleteMessage on a receipt handle whose visibility window has lapsed. It is
    // best-effort: swallowed as a WARN, never fatal, and the process keeps running. Whether
    // the stale handle still deletes is BROKER-DEFINED (AWS keeps a receipt handle usable
    // until the message is re-received; LocalStack is its own implementation), so this test
    // deliberately pins survivability + registry drain, not the queue outcome.
    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let (visible, in_flight) = queue_depth(&queue_url).await;
    assert!(
        visible + in_flight <= 1,
        "the late settle must not duplicate or resurrect the delivery"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_visibility_expiry_redelivers_while_the_instance_is_still_parked() {
    // THE SQS CAVEAT, pinned: a deferred delete is only valid while the visibility timeout
    // holds. With a park longer than the window, SQS redelivers the SAME message to the
    // SAME live source while the original is still parked — a second, concurrent delivery.
    // The idempotency key is stable across redeliveries, so INBOX DEDUP is the absorber;
    // `on-complete` on SQS does not hold a message indefinitely. Operator rule: size
    // visibility-timeout-seconds >= the worst-case park, and keep the registry timeout
    // under it (see the sqs::source module docs).
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&queue_url, "payments-inbound", 2, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-expiry")],
        b"long-park",
    )
    .await;
    wait_until(
        "redelivery while the first instance is parked",
        Duration::from_secs(40),
        || intake.instance_count() >= 2,
    )
    .await;

    assert_eq!(
        intake.deferred_at(0).idempotency_key,
        "evt-expiry",
        "the first (parked) delivery"
    );
    assert_eq!(
        intake.deferred_at(1).idempotency_key,
        "evt-expiry",
        "the SAME message rides the visibility-expiry redelivery — inbox dedup absorbs it"
    );
    assert!(
        registry.pending_count() >= 2,
        "both the parked original and its redelivery hold deferred settles"
    );

    // Drain: complete every registered instance and let the deletes land.
    for index in 0..intake.instance_count() {
        registry.on_instance_completed(&intake.instance_at(index));
    }
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_drops_the_delivery_when_the_instance_fails() {
    // failure path: message in → delete deferred → instance FAILS → the held nack fires the
    // DROP posture. On SQS the drop verb IS DeleteMessage (no immediate DLQ hand-off), so
    // the proof is the same (0, 0) depth — the delivery is consumed, never redelivered.
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&queue_url, "payments-inbound", 60, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-fail")],
        b"will-fail",
    )
    .await;
    wait_until("deferred delivery", Duration::from_secs(30), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);
    wait_for_depth(&queue_url, (0, 1), Duration::from_secs(15)).await;

    registry.on_instance_failed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    wait_for_depth(&queue_url, (0, 0), Duration::from_secs(15)).await;

    source.stop().await.expect("stop");
    // Nothing comes back: the drop consumed it (contrast NackRequeue, which leaves it).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(queue_depth(&queue_url).await, (0, 0));
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_persist_source_still_settles_at_dispatch_return() {
    // Regression pin for the untouched path: an on-persist source keeps calling plain
    // `deliver` and deletes at dispatch-return — the deferred seam is never involved.
    let name = fresh_queue_name();
    let queue_url = create_queue(&name).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_for(&queue_url, "payments-inbound", 60); // OnPersist
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    send_raw(
        &queue_url,
        &[("sutra-outbox-key", "evt-classic")],
        b"classic",
    )
    .await;
    wait_until("plain delivery", Duration::from_secs(30), || {
        intake.plain_count() >= 1
    })
    .await;
    assert_eq!(intake.instance_count(), 0, "deliver_deferred never called");
    assert_eq!(registry.pending_count(), 0);
    wait_for_depth(&queue_url, (0, 0), Duration::from_secs(15)).await; // deleted immediately
    source.stop().await.expect("stop");
}
