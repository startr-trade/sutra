//! Native TLS and SASL against a real Artemis that requires a login: a round trip through the sink
//! and the source over AMQPS, refusal of an untrusted CA and of a host name the certificate does
//! not cover, refusal of wrong credentials with a diagnostic that carries no secret, the explicit
//! verification opt-out, and plaintext unchanged. Certificates are generated at test time.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{AckDecision, InboundIntake, TriggerSource};
use sutra_channels::{BoxFuture, InboundMessage};
use sutra_testkit::broker_tls::{
    start_tls_artemis, unique_token, BrokerPlacement, TestBrokerPki, TlsArtemisBroker,
};
use sutra_transport_amqp::codes;
use sutra_transport_amqp::{
    AckMode, AmqpChannelProperties, AmqpMessageSink, AmqpSourceConfig, AmqpTlsProperties,
    AmqpTriggerSource,
};

struct Fixture {
    pki: TestBrokerPki,
    broker: TlsArtemisBroker,
}

static FIXTURE: OnceLock<Fixture> = OnceLock::new();

/// One broker per test binary. The certificate covers `localhost` only, so `127.0.0.1` is the
/// host-name-mismatch case.
fn fixture() -> &'static Fixture {
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pki = TestBrokerPki::generate(&["localhost"]);
            let broker = start_tls_artemis(&pki, &BrokerPlacement::default());
            Fixture { pki, broker }
        })
        .join()
        .expect("broker bootstrap thread")
    })
}

fn trust_test_ca() -> AmqpTlsProperties {
    AmqpTlsProperties {
        ca_file: Some(fixture().pki.ca_file.display().to_string()),
        ..AmqpTlsProperties::default()
    }
}

fn verified_sink() -> AmqpMessageSink {
    let f = fixture();
    AmqpMessageSink::new(
        Some(f.broker.username.clone()),
        Some(f.broker.password.clone()),
    )
    .with_tls_settings(Ok(trust_test_ca()))
}

fn message(destination: String) -> OutboundMessage {
    OutboundMessage {
        destination,
        headers: BTreeMap::new(),
        body: b"over-tls".to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: unique_token("key"),
        traceparent: None,
    }
}

#[derive(Default)]
struct CollectingIntake {
    delivered: Mutex<Vec<InboundMessage>>,
}

impl InboundIntake for CollectingIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            self.delivered.lock().unwrap().push(message);
            AckDecision::Ack
        })
    }
}

fn source(tls: bool, port: u16, tls_settings: AmqpTlsProperties, queue: &str) -> AmqpTriggerSource {
    let f = fixture();
    let properties = AmqpChannelProperties {
        host: "localhost".to_string(),
        port,
        tls,
        tls_settings,
        username: Some(f.broker.username.clone()),
        password: Some(f.broker.password.clone()),
        queue: Some(queue.to_string()),
        topic: None,
        prefetch_count: 10,
        receive_timeout_ms: 250,
        ack_mode: AckMode::OnPersist,
        singleton: false,
    };
    let mut config = AmqpSourceConfig::new("acme", "acme/orders/1.0.0", "orders-in", properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    AmqpTriggerSource::new(config).expect("source")
}

async fn assert_delivered(intake: &CollectingIntake, outbox_key: &str) {
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        if let Some(found) = intake
            .delivered
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.idempotency_key == outbox_key)
        {
            assert_eq!(found.body.clone().into_inner(), b"over-tls");
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no delivery with key {outbox_key}");
}

fn expect_refused(outcome: SendOutcome, code: &str, secret: &str) -> String {
    match outcome {
        SendOutcome::RetryableFailure(d) => {
            assert_eq!(d.code, code, "{}", d.message);
            assert!(
                !d.message.contains(secret),
                "the diagnostic leaked the password: {}",
                d.message
            );
            d.message
        }
        other => panic!("expected a retryable {code}, got {other:?}"),
    }
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_and_sasl_round_trip_through_sink_and_source() {
    let f = fixture();
    let queue = format!("tls-{}", unique_token("q"));
    let intake = Arc::new(CollectingIntake::default());
    let consumer = source(true, f.broker.tls_port, trust_test_ca(), &queue);
    consumer
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    let outbound = message(format!("amqp10s://localhost:{}/{queue}", f.broker.tls_port));
    let sent = verified_sink().send(&outbound).await;
    assert!(matches!(sent, SendOutcome::Delivered), "{sent:?}");

    assert_delivered(&intake, &outbound.outbox_key).await;
    consumer.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ca_can_come_from_pem_text_as_a_resolved_secret_reference() {
    let f = fixture();
    let queue = format!("tls-{}", unique_token("q"));
    let intake = Arc::new(CollectingIntake::default());
    let consumer = source(
        true,
        f.broker.tls_port,
        AmqpTlsProperties {
            ca_pem: Some(f.pki.ca_pem.clone()),
            ..AmqpTlsProperties::default()
        },
        &queue,
    );
    consumer
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    let outbound = message(format!("amqp10s://localhost:{}/{queue}", f.broker.tls_port));
    assert!(matches!(
        verified_sink().send(&outbound).await,
        SendOutcome::Delivered
    ));
    assert_delivered(&intake, &outbound.outbox_key).await;
    consumer.stop().await.expect("stop");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrelated_ca_is_refused_with_a_tls_diagnostic() {
    let f = fixture();
    let sink = AmqpMessageSink::new(
        Some(f.broker.username.clone()),
        Some(f.broker.password.clone()),
    )
    .with_tls_settings(Ok(AmqpTlsProperties {
        ca_file: Some(f.pki.foreign_ca_file.display().to_string()),
        ..AmqpTlsProperties::default()
    }));
    let outcome = sink
        .send(&message(format!(
            "amqp10s://localhost:{}/refused",
            f.broker.tls_port
        )))
        .await;
    let text = expect_refused(
        outcome,
        codes::OUTBOUND_BROKER_TLS_FAILED,
        &f.broker.password,
    );
    assert!(text.contains("TLS handshake failed"), "{text}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_native_roots_do_not_trust_a_private_ca() {
    let f = fixture();
    let sink = AmqpMessageSink::new(
        Some(f.broker.username.clone()),
        Some(f.broker.password.clone()),
    );
    let outcome = sink
        .send(&message(format!(
            "amqp10s://localhost:{}/refused",
            f.broker.tls_port
        )))
        .await;
    expect_refused(
        outcome,
        codes::OUTBOUND_BROKER_TLS_FAILED,
        &f.broker.password,
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_name_the_certificate_does_not_cover_is_refused() {
    let f = fixture();
    let outcome = verified_sink()
        .send(&message(format!(
            "amqp10s://127.0.0.1:{}/refused",
            f.broker.tls_port
        )))
        .await;
    expect_refused(
        outcome,
        codes::OUTBOUND_BROKER_TLS_FAILED,
        &f.broker.password,
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_credentials_are_refused_with_an_auth_diagnostic_that_carries_no_secret() {
    let f = fixture();
    let wrong = unique_token("wrongpw");
    let sink = AmqpMessageSink::new(Some(f.broker.username.clone()), Some(wrong.clone()))
        .with_tls_settings(Ok(trust_test_ca()));
    let outcome = sink
        .send(&message(format!(
            "amqp10s://localhost:{}/refused",
            f.broker.tls_port
        )))
        .await;
    let text = expect_refused(outcome, codes::OUTBOUND_BROKER_AUTH_FAILED, &wrong);
    assert!(text.contains("refused the SASL credentials"), "{text}");

    // The same, with the credentials in the destination URI, which the diagnostic names.
    let sink = AmqpMessageSink::new(None, None).with_tls_settings(Ok(trust_test_ca()));
    let outcome = sink
        .send(&message(format!(
            "amqp10s://{}:{wrong}@localhost:{}/refused",
            f.broker.username, f.broker.tls_port
        )))
        .await;
    expect_refused(outcome, codes::OUTBOUND_BROKER_AUTH_FAILED, &wrong);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_can_be_turned_off_only_by_the_explicit_setting() {
    let f = fixture();
    let sink = AmqpMessageSink::new(
        Some(f.broker.username.clone()),
        Some(f.broker.password.clone()),
    )
    .with_tls_settings(Ok(AmqpTlsProperties {
        insecure_skip_verify: true,
        ..AmqpTlsProperties::default()
    }));
    let outcome = sink
        .send(&message(format!(
            "amqp10s://127.0.0.1:{}/{}",
            f.broker.tls_port,
            unique_token("q")
        )))
        .await;
    assert!(matches!(outcome, SendOutcome::Delivered), "{outcome:?}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_with_sasl_is_unchanged_and_refuses_wrong_credentials() {
    let f = fixture();
    let queue = format!("plain-{}", unique_token("q"));
    let intake = Arc::new(CollectingIntake::default());
    let consumer = source(
        false,
        f.broker.plain_port,
        AmqpTlsProperties::default(),
        &queue,
    );
    consumer
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    let sink = AmqpMessageSink::new(
        Some(f.broker.username.clone()),
        Some(f.broker.password.clone()),
    );
    let outbound = message(format!(
        "amqp10://localhost:{}/{queue}",
        f.broker.plain_port
    ));
    let sent = sink.send(&outbound).await;
    assert!(matches!(sent, SendOutcome::Delivered), "{sent:?}");
    assert_delivered(&intake, &outbound.outbox_key).await;
    consumer.stop().await.expect("stop");

    let wrong = unique_token("wrongpw");
    let sink = AmqpMessageSink::new(Some(f.broker.username.clone()), Some(wrong.clone()));
    let outcome = sink
        .send(&message(format!(
            "amqp10://localhost:{}/refused",
            f.broker.plain_port
        )))
        .await;
    expect_refused(outcome, codes::OUTBOUND_BROKER_AUTH_FAILED, &wrong);
}
