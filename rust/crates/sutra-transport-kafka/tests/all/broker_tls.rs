//! Native TLS and SASL against a real Kafka broker with one listener per protocol: a round trip
//! through the sink and the source on SSL, on SASL_SSL with each of PLAIN, SCRAM-SHA-256 and
//! SCRAM-SHA-512, and on SASL_PLAINTEXT; refusal of an unrelated CA and of a host name the
//! certificate does not cover; refusal of wrong credentials with a diagnostic that carries no
//! secret; the explicit verification opt-out. The plaintext path stays covered by `kafka_it`.
//! Certificates are generated at test time.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{AckDecision, InboundIntake, TriggerSource};
use sutra_channels::{BoxFuture, InboundMessage};
use sutra_testkit::broker_tls::{
    start_tls_kafka, unique_token, BrokerPlacement, TestBrokerPki, TlsKafkaBroker,
};
use sutra_transport_kafka::codes;
use sutra_transport_kafka::{
    AckMode, KafkaBrokerSecurity, KafkaChannelProperties, KafkaMessageSink, KafkaSinkSecurity,
    KafkaSourceConfig, KafkaTriggerSource,
};

struct Fixture {
    pki: TestBrokerPki,
    broker: TlsKafkaBroker,
}

static FIXTURE: OnceLock<Fixture> = OnceLock::new();

/// One broker per test binary. The certificate covers `localhost` only, so `127.0.0.1` is the
/// host-name-mismatch case.
fn fixture() -> &'static Fixture {
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pki = TestBrokerPki::generate(&["localhost"]);
            let broker = start_tls_kafka(&pki, &BrokerPlacement::default());
            Fixture { pki, broker }
        })
        .join()
        .expect("broker bootstrap thread")
    })
}

fn trust_test_ca() -> KafkaBrokerSecurity {
    KafkaBrokerSecurity {
        tls_ca_file: Some(fixture().pki.ca_file.display().to_string()),
        ..KafkaBrokerSecurity::default()
    }
}

fn sasl(mechanism: &str, tls: bool) -> KafkaBrokerSecurity {
    let f = fixture();
    let base = if tls {
        trust_test_ca()
    } else {
        KafkaBrokerSecurity::default()
    };
    KafkaBrokerSecurity {
        sasl_mechanism: Some(mechanism.to_string()),
        sasl_username: Some(f.broker.username.clone()),
        sasl_password: Some(f.broker.password.clone()),
        ..base
    }
}

async fn create_topic(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &fixture().broker.plain_bootstrap)
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");
    for result in results {
        result.expect("topic created");
    }
}

fn message(topic: &str) -> OutboundMessage {
    OutboundMessage {
        destination: format!("kafka://{topic}"),
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

/// Publish through the sink and consume through the source, both on one protocol and settings.
async fn round_trip(protocol: &str, bootstrap: &str, security: KafkaBrokerSecurity) {
    let topic = format!("t-{}", unique_token("rt"));
    create_topic(&topic).await;

    let sink = KafkaMessageSink::new(bootstrap).with_security(Ok(KafkaSinkSecurity {
        protocol: protocol.to_string(),
        security: security.clone(),
    }));
    let outbound = message(&topic);
    let sent = sink.send(&outbound).await;
    assert!(
        matches!(sent, SendOutcome::Delivered),
        "{protocol}: {sent:?}"
    );

    let properties = KafkaChannelProperties {
        bootstrap_servers: bootstrap.to_string(),
        topic: topic.clone(),
        group_id: unique_token("g"),
        auto_offset_reset: "earliest".to_string(),
        security_protocol: protocol.to_string(),
        security,
        client_config: BTreeMap::new(),
        ack_mode: AckMode::OnPersist,
        singleton: false,
    };
    let mut config = KafkaSourceConfig::new("acme", "acme/orders/1.0.0", "orders-in", properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    let consumer = KafkaTriggerSource::new(config).expect("source");
    let intake = Arc::new(CollectingIntake::default());
    consumer
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(found) = intake
            .delivered
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.idempotency_key == outbound.outbox_key)
        {
            assert_eq!(found.body.clone().into_inner(), b"over-tls");
            break;
        }
        assert!(Instant::now() < deadline, "{protocol}: no delivery");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    consumer.stop().await.expect("stop");
    sink.drain().await;
}

/// Send to a topic that needs no creation, because the record is never meant to arrive.
async fn refused(
    protocol: &str,
    bootstrap: &str,
    security: KafkaBrokerSecurity,
) -> (String, String) {
    let sink = KafkaMessageSink::new(bootstrap).with_security(Ok(KafkaSinkSecurity {
        protocol: protocol.to_string(),
        security,
    }));
    let outcome = sink.send(&message("never-delivered")).await;
    sink.drain().await;
    match outcome {
        SendOutcome::RetryableFailure(d) => (d.code.to_string(), d.message),
        other => panic!("expected a retryable failure, got {other:?}"),
    }
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_round_trip_with_the_configured_ca() {
    let bootstrap = fixture().broker.ssl_bootstrap.clone();
    round_trip("SSL", &bootstrap, trust_test_ca()).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ca_can_come_from_pem_text_as_a_resolved_secret_reference() {
    let f = fixture();
    let security = KafkaBrokerSecurity {
        tls_ca_pem: Some(f.pki.ca_pem.clone()),
        ..KafkaBrokerSecurity::default()
    };
    round_trip("SSL", &f.broker.ssl_bootstrap, security).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_ssl_round_trip_with_plain() {
    let bootstrap = fixture().broker.sasl_ssl_bootstrap.clone();
    round_trip("SASL_SSL", &bootstrap, sasl("PLAIN", true)).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_ssl_round_trip_with_scram_sha_256() {
    let bootstrap = fixture().broker.sasl_ssl_bootstrap.clone();
    round_trip("SASL_SSL", &bootstrap, sasl("SCRAM-SHA-256", true)).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_ssl_round_trip_with_scram_sha_512() {
    let bootstrap = fixture().broker.sasl_ssl_bootstrap.clone();
    round_trip("SASL_SSL", &bootstrap, sasl("SCRAM-SHA-512", true)).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plaintext_round_trip_with_scram_sha_256() {
    let bootstrap = fixture().broker.sasl_plain_bootstrap.clone();
    round_trip("SASL_PLAINTEXT", &bootstrap, sasl("SCRAM-SHA-256", false)).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrelated_ca_is_refused_with_a_tls_diagnostic() {
    let f = fixture();
    let security = KafkaBrokerSecurity {
        tls_ca_file: Some(f.pki.foreign_ca_file.display().to_string()),
        ..KafkaBrokerSecurity::default()
    };
    let (code, text) = refused("SSL", &f.broker.ssl_bootstrap, security).await;
    assert_eq!(code, codes::OUTBOUND_BROKER_TLS_FAILED, "{text}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_name_the_certificate_does_not_cover_is_refused() {
    let f = fixture();
    let bootstrap = format!("127.0.0.1:{}", f.broker.sasl_ssl_port);
    let (code, text) = refused("SASL_SSL", &bootstrap, sasl("PLAIN", true)).await;
    assert_eq!(code, codes::OUTBOUND_BROKER_TLS_FAILED, "{text}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_credentials_are_refused_with_an_auth_diagnostic_that_carries_no_secret() {
    let f = fixture();
    for mechanism in ["PLAIN", "SCRAM-SHA-256"] {
        let wrong = unique_token("wrongpw");
        let security = KafkaBrokerSecurity {
            sasl_password: Some(wrong.clone()),
            ..sasl(mechanism, true)
        };
        let (code, text) = refused("SASL_SSL", &f.broker.sasl_ssl_bootstrap, security).await;
        assert_eq!(
            code,
            codes::OUTBOUND_BROKER_AUTH_FAILED,
            "{mechanism}: {text}"
        );
        assert!(!text.contains(&wrong), "leaked the password: {text}");
    }
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_can_be_turned_off_only_by_the_explicit_setting() {
    // No CA is named and the platform bundle does not hold the test CA, so the opt-out is the only
    // reason this connects.
    let bootstrap = fixture().broker.ssl_bootstrap.clone();
    let security = KafkaBrokerSecurity {
        tls_insecure_skip_verify: true,
        ..KafkaBrokerSecurity::default()
    };
    round_trip("SSL", &bootstrap, security).await;
}
