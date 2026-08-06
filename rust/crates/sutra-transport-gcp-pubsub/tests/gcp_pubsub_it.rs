//! GCP Pub/Sub transport integration suite — Testcontainers-backed against the official
//! emulator (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`, started with
//! `gcloud beta emulators pubsub start`). Exercises the transport seams end to end: the source
//! delivers into a scripted [`InboundIntake`] and each [`AckDecision`] is proven on the broker
//! (Ack/NackDrop ack, NackRequeue nacks and redelivers), and the sink publishes with the FROZEN
//! wire projection (outbox key on the `sutra-outbox-key` attribute, `content-type` attribute,
//! CE `ce-*` attributes carried verbatim).
//!
//! Requires a Docker daemon (same posture as the rabbitmq / kafka / pg suites). The emulator
//! is plaintext — the client reaches it via the per-channel `endpoint-override`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscriber::ReceivedMessage;
use google_cloud_pubsub::subscription::{Subscription, SubscriptionConfig};
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, TriggerSource,
};
use sutra_channels::{BoxFuture, DeferredAckRegistry, InboundMessage};
use sutra_transport_gcp_pubsub::{
    build_client_config, AckMode, GcpPubSubChannelProperties, GcpPubSubMessageSink,
    GcpPubSubSourceConfig, GcpPubSubTriggerSource,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const PROJECT: &str = "sutra-it";

// ---- shared emulator fixture ---------------------------------------------------------------

static EMULATOR: OnceLock<(Container<GenericImage>, u16)> = OnceLock::new();

/// One shared emulator per test binary; each test creates its own fresh topic + subscription.
fn emulator_port() -> u16 {
    let (_, port) = EMULATOR.get_or_init(|| {
        // Blocking runner on a dedicated thread — must not run inside a tokio worker.
        std::thread::spawn(|| {
            let container = GenericImage::new(
                "gcr.io/google.com/cloudsdktool/google-cloud-cli",
                "emulators",
            )
            .with_exposed_port(8085.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server started, listening on"))
            .with_cmd([
                "gcloud",
                "beta",
                "emulators",
                "pubsub",
                "start",
                "--host-port=0.0.0.0:8085",
                "--project=sutra-it",
            ])
            .start()
            .expect("start pubsub emulator (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container
                .get_host_port_ipv4(8085)
                .expect("mapped pubsub port");
            (container, port)
        })
        .join()
        .expect("emulator bootstrap thread")
    });
    *port
}

fn endpoint() -> String {
    format!("127.0.0.1:{}", emulator_port())
}

async fn client() -> Client {
    Client::new(build_client_config(PROJECT, Some(&endpoint())))
        .await
        .expect("emulator client")
}

/// Unique-enough topic/subscription id per test (no uuid dependency).
fn fresh_id(prefix: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{prefix}-{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// Create a topic + subscription pair (subscription must exist before publish so messages
/// are retained). Returns `(topic, subscription)`.
async fn create_topic_and_subscription() -> (String, String) {
    create_topic_and_subscription_with_deadline(10).await
}

/// As [`create_topic_and_subscription`] with an explicit `ackDeadlineSeconds` — the lease
/// window a deferred settle must fit inside (Pub/Sub's legal range is 10 s…600 s).
async fn create_topic_and_subscription_with_deadline(deadline: i32) -> (String, String) {
    let topic = fresh_id("t");
    let subscription = fresh_id("s");
    let client = client().await;
    client
        .create_topic(&topic, None, None)
        .await
        .expect("create topic");
    client
        .create_subscription(
            &subscription,
            &topic,
            SubscriptionConfig {
                ack_deadline_seconds: deadline,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create subscription");
    (topic, subscription)
}

async fn publish(topic: &str, attributes: &[(&str, &str)], body: &[u8]) {
    let client = client().await;
    let publisher = client.topic(topic).new_publisher(None);
    let attrs: HashMap<String, String> = attributes
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let awaiter = publisher
        .publish(PubsubMessage {
            data: body.to_vec(),
            attributes: attrs,
            ..Default::default()
        })
        .await;
    awaiter.get().await.expect("publish");
}

/// Pull one message off a subscription (the raw wire view for the sink assertions).
async fn pull_one(subscription: &str) -> ReceivedMessage {
    let client = client().await;
    let sub: Subscription = client.subscription(subscription);
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let mut messages = tokio::time::timeout(Duration::from_secs(20), sub.pull(1, None))
            .await
            .expect("pull within timeout")
            .expect("pull");
        if let Some(m) = messages.pop() {
            return m;
        }
        if Instant::now() >= deadline {
            panic!("no message pulled within timeout");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn attr(message: &PubsubMessage, key: &str) -> Option<String> {
    message.attributes.get(key).cloned()
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

fn source_for(subscription: &str, channel: &str) -> GcpPubSubTriggerSource {
    source_with_mode(subscription, channel, AckMode::OnPersist)
}

fn source_with_mode(
    subscription: &str,
    channel: &str,
    ack_mode: AckMode,
) -> GcpPubSubTriggerSource {
    let properties = GcpPubSubChannelProperties {
        project_id: PROJECT.to_string(),
        subscription: subscription.to_string(),
        topic: String::new(),
        max_outstanding_messages: GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_MESSAGES,
        max_outstanding_request_bytes:
            GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES,
        endpoint_override: Some(endpoint()),
        ack_mode,
        singleton: false,
    };
    let mut config = GcpPubSubSourceConfig::new("acme", "acme/payments/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    GcpPubSubTriggerSource::new(config).expect("source")
}

// ---- inbound ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_delivers_body_attributes_and_outbox_key_as_idempotency_key() {
    let (topic, subscription) = create_topic_and_subscription().await;
    publish(
        &topic,
        &[
            ("sutra-outbox-key", "order-1"),
            ("content-type", "application/xml"),
            ("x-tenant", "acme"),
        ],
        b"<Document/>",
    )
    .await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&subscription, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    wait_until("first delivery", Duration::from_secs(30), || {
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
async fn idempotency_key_falls_back_to_message_id() {
    let (topic, subscription) = create_topic_and_subscription().await;
    publish(&topic, &[("content-type", "text/plain")], b"no-key").await;

    let intake = ScriptedIntake::always(AckDecision::Ack);
    let source = source_for(&subscription, "payments-inbound");
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    wait_until("delivery", Duration::from_secs(30), || {
        intake.delivered_count() >= 1
    })
    .await;

    let m = intake.delivered_at(0);
    // FROZEN — the broker-assigned message id is the fallback key, non-explicit.
    assert!(
        !m.idempotency_key.is_empty(),
        "the broker message id is the fallback key"
    );
    assert!(!m.explicit_event_id, "the fallback key is non-explicit");
    assert_eq!(
        m.headers.get("x-gcp-pubsub-message-id").map(String::as_str),
        Some(m.idempotency_key.as_str()),
        "the message id also rides as the informational header"
    );
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_until_acked() {
    let (topic, subscription) = create_topic_and_subscription().await;
    publish(&topic, &[("sutra-outbox-key", "evt-requeue")], b"retry-me").await;

    // First decision: transient failure (persistence down) — the source nacks and Pub/Sub
    // redelivers; the redelivery is then acked.
    let intake = ScriptedIntake::scripted(vec![AckDecision::NackRequeue], AckDecision::Ack);
    let source = source_for(&subscription, "payments-inbound");
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

// ---- outbound (the m9 wire projection) ------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_publishes_with_the_frozen_wire_projection() {
    let (topic, subscription) = create_topic_and_subscription().await;
    let sink = GcpPubSubMessageSink::new(PROJECT, Some(endpoint()));

    let mut headers = std::collections::BTreeMap::new();
    headers.insert("x-tenant".to_string(), "acme".to_string());
    // A `ce-*` binary-binding attribute (projected upstream by the dispatcher) rides verbatim.
    headers.insert("ce-type".to_string(), "io.sutra.reply.v1".to_string());
    let message = OutboundMessage {
        destination: format!("gcp-pubsub://{topic}"),
        headers,
        body: b"{\"ok\":true}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: "outbox-abc-123".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };

    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let received = pull_one(&subscription).await;
    let m = &received.message;
    assert_eq!(m.data, b"{\"ok\":true}");
    // FROZEN — the outbox key rides the sutra-outbox-key attribute (dedup token).
    assert_eq!(
        attr(m, "sutra-outbox-key").as_deref(),
        Some("outbox-abc-123")
    );
    assert_eq!(attr(m, "content-type").as_deref(), Some("application/json"));
    assert_eq!(attr(m, "x-tenant").as_deref(), Some("acme"));
    // The GCP CE binding prefix is `ce-` (dash) — carried verbatim by the sink.
    assert_eq!(attr(m, "ce-type").as_deref(), Some("io.sutra.reply.v1"));
    assert_eq!(
        attr(m, "traceparent").as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    received.ack().await.expect("ack");
    sink.drain().await;
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The broker-side half of the on-complete contract against the REAL Pub/Sub emulator: the
// source hands its per-delivery ack callbacks through `InboundIntake::deliver_deferred`,
// and the settle fires only when the instance's terminal event settles the
// `DeferredAckRegistry` entry (the engine-side half — dispatch parks → registry → listener
// bus — is `sutra-channels/tests/all/deferred_ack_test.rs`).
//
// PUB/SUB-SPECIFIC: both held callbacks are `message.ack()`. That is not a shortcut — it is
// exactly what the settle-at-return path already does for `Ack` and `NackDrop` alike
// (Pub/Sub has no default dead-letter hand-off; a dead-letter topic is delivery-attempt
// driven, not reject driven), so "the instance failed" means the delivery is DROPPED by
// acking it — the poison never redelivers. The discriminator between the two paths is
// therefore the registry event that fires, not the Pub/Sub verb.
//
// The observable proof of "the ack landed" is the ABSENCE of a redelivery once the
// subscription's ack deadline has lapsed — so these tests deliberately run a wall-clock
// window slightly longer than the configured deadline.

/// A wall-clock window comfortably past the 10 s ack deadline used by these tests — long
/// enough that an UN-acked message would have been redelivered inside it.
const PAST_ACK_DEADLINE: Duration = Duration::from_secs(16);

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
            let instance_id = fresh_id("inst");
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

/// Assert nothing new arrived across `window` — the settle-landed proof (an unacked Pub/Sub
/// message would have redelivered inside a window longer than the ack deadline).
async fn assert_no_further_deliveries<F: Fn() -> usize>(what: &str, window: Duration, count: F) {
    let baseline = count();
    tokio::time::sleep(window).await;
    assert_eq!(
        count(),
        baseline,
        "{what}: a redelivery arrived — the settle did not land"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_withholds_the_ack_until_the_instance_completes() {
    // message in → ack DEFERRED → instance completes → message.ack() fires on the broker,
    // proven by the absence of a redelivery past the ack deadline.
    let (topic, subscription) = create_topic_and_subscription_with_deadline(10).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&subscription, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(
        &topic,
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

    // The instance's terminal event fires the held ack (spawned onto the runtime).
    registry.on_instance_completed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The discriminator: the source keeps pulling. A message whose ack never landed would
    // redeliver once the 10 s deadline lapsed; staying at one delivery proves it landed.
    assert_no_further_deliveries("completed instance", PAST_ACK_DEADLINE, || {
        intake.instance_count()
    })
    .await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_holds_the_ack_so_the_deadline_redelivers_while_the_instance_is_parked() {
    // TWO proofs in one, and THE PUB/SUB CAVEAT pinned:
    //   (a) withheld-ack: with NO terminal event the message is never acked;
    //   (b) the lease is the bound: this source pulls with the UNARY `pull` API, which does
    //       no ModifyAckDeadline heartbeat, so an instance parked past ackDeadlineSeconds
    //       sees Pub/Sub redeliver the SAME message alongside the parked original. The
    //       idempotency key is stable, so INBOX DEDUP is the absorber. Operator rule: raise
    //       ackDeadlineSeconds (max 600 s) toward the worst-case park and keep the registry
    //       timeout under it (see the gcp_pubsub::source module docs).
    let (topic, subscription) = create_topic_and_subscription_with_deadline(10).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&subscription, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(&topic, &[("sutra-outbox-key", "evt-parked")], b"long-park").await;
    wait_until(
        "redelivery while the first instance is parked",
        Duration::from_secs(60),
        || intake.instance_count() >= 2,
    )
    .await;

    assert_eq!(
        intake.deferred_at(0).idempotency_key,
        "evt-parked",
        "the first (parked) delivery"
    );
    assert_eq!(
        intake.deferred_at(1).idempotency_key,
        "evt-parked",
        "the SAME message rides the ack-deadline redelivery — inbox dedup absorbs it"
    );
    assert!(
        registry.pending_count() >= 2,
        "both the parked original and its redelivery hold deferred settles"
    );

    // Drain: complete every registered instance. The FIRST one's held ack targets a stale
    // ack id (its lease lapsed) — best-effort, swallowed as a WARN, never fatal.
    for index in 0..intake.instance_count() {
        registry.on_instance_completed(&intake.instance_at(index));
    }
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(500)).await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_complete_drops_the_delivery_when_the_instance_fails() {
    // failure path: message in → ack deferred → instance FAILS → the held nack fires the
    // DROP posture. On Pub/Sub the drop verb IS `ack()` (no default DLQ), so the proof is
    // the same as the completed path: the poison never redelivers.
    let (topic, subscription) = create_topic_and_subscription_with_deadline(10).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_with_mode(&subscription, "payments-inbound", AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(&topic, &[("sutra-outbox-key", "evt-fail")], b"will-fail").await;
    wait_until("deferred delivery", Duration::from_secs(30), || {
        intake.instance_count() >= 1
    })
    .await;
    assert_eq!(registry.pending_count(), 1);

    registry.on_instance_failed(&intake.instance_at(0));
    assert_eq!(registry.pending_count(), 0);
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_no_further_deliveries("failed instance (drop posture)", PAST_ACK_DEADLINE, || {
        intake.instance_count()
    })
    .await;
    source.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_persist_source_still_settles_at_dispatch_return() {
    // Regression pin for the untouched path: an on-persist source keeps calling plain
    // `deliver` and acks at dispatch-return — the deferred seam is never involved.
    let (topic, subscription) = create_topic_and_subscription_with_deadline(10).await;
    let registry = registry();
    let intake = DeferringIntake::new(Arc::clone(&registry));
    let source = source_for(&subscription, "payments-inbound"); // OnPersist
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    publish(&topic, &[("sutra-outbox-key", "evt-classic")], b"classic").await;
    wait_until("plain delivery", Duration::from_secs(30), || {
        intake.plain_count() >= 1
    })
    .await;
    assert_eq!(intake.instance_count(), 0, "deliver_deferred never called");
    assert_eq!(registry.pending_count(), 0);

    // Acked at dispatch-return: no redelivery once the deadline lapses.
    assert_no_further_deliveries("on-persist delivery", PAST_ACK_DEADLINE, || {
        intake.plain_count()
    })
    .await;
    source.stop().await.expect("stop");
}
