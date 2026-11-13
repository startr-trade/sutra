//! Conformance: native broker TLS and SASL through the shipped engine image. A sealed package with
//! an inbound and an outbound channel on a TLS + SASL broker; a real message goes in over the
//! broker, a process runs, and its result is read back off the broker. The engine's credentials and
//! its CA come from a mounted secret and an env reference, as a deployment would supply them.
//!
//! The refusals run the same package: an engine holding the wrong password, or trusting the wrong
//! CA, logs the matching diagnostic code, never the secret, and processes nothing.
//!
//! The brokers are local containers: Artemis and Apache Kafka. A managed service is not exercised.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{Body, Message};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message as _};
use sutra_testkit::broker_tls::{
    start_tls_artemis, start_tls_kafka, unique_token, BrokerPlacement, TestBrokerPki,
    TlsArtemisBroker, TlsKafkaBroker, KAFKA_NETWORK_SASL_SSL_PORT,
};

use crate::support::engine::{self, EngineBuilder, EngineHandle, PgFixture};
use crate::support::util;

const CA_MOUNT: &str = "/etc/sutra/broker-ca";
const RELAY_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_tls_relay"
                  targetNamespace="urn:sutra:module:tls-relay:1.0.0">
  <bpmn:process id="tls-relay" name="Answer over the broker" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables><q:variable name="responseBody" type="object"/></q:variables>
    </bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="orders-in"/></bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Build"/>
    <bpmn:serviceTask id="Build">
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
      <bpmn:dataInputAssociation>
        <bpmn:assignment>
          <bpmn:from>{ref: payload.ref, status: "ANSWERED"}</bpmn:from>
          <bpmn:to>responseBody</bpmn:to>
        </bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="F2" sourceRef="Build" targetRef="Answer"/>
    <bpmn:sendTask id="Answer">
      <bpmn:extensionElements><q:send channel="orders-out"/></bpmn:extensionElements>
      <bpmn:incoming>F2</bpmn:incoming><bpmn:outgoing>F3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="F3" sourceRef="Answer" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#;

/// Seal a one-process package around the given channels file.
fn seal_package(channels_yaml: &str) -> PathBuf {
    let package = util::world_readable_temp_dir("sutra-broker-tls-package");
    std::fs::create_dir_all(package.join("bpmn")).expect("package dir");
    std::fs::write(
        package.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"tls-relay\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    )
    .expect("package.yaml");
    std::fs::write(package.join("channels.yaml"), channels_yaml).expect("channels.yaml");
    std::fs::write(package.join("bpmn/tls-relay.bpmn"), RELAY_BPMN).expect("bpmn");
    let archives = util::world_readable_temp_dir("sutra-archives");
    sutra_loader::package::assemble_dir(
        &package,
        &archives,
        &sutra_loader::PackageOptions::default(),
    )
    .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
    util::make_world_readable(&archives);
    archives
}

fn ca_dir(pki: &TestBrokerPki) -> PathBuf {
    pki.ca_file
        .parent()
        .expect("the PKI files sit in a directory")
        .to_path_buf()
}

fn container_logs(id: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("docker logs");
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Wait for a diagnostic code in the engine's log, and hold the log to the no-secret rule.
async fn assert_logs_code_without_secret(engine: &EngineHandle, code: &str, secret: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let logs = container_logs(engine.container.id());
        assert!(!logs.contains(secret), "the engine log holds the secret");
        if logs.contains(code) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no {code} in the engine log:\n{}",
            &logs[logs.len().saturating_sub(4000)..]
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Each engine gets its own database: two on one would be two replicas, which no test here is
/// about. Both are built on a plain thread and leaked, so no runtime worker ever drops them.
fn start_engine(
    hint: &'static str,
    configure: impl FnOnce(EngineBuilder) -> EngineBuilder + Send + 'static,
    archives: &Path,
) -> &'static EngineHandle {
    let archives = archives.to_path_buf();
    let handle = std::thread::spawn(move || {
        let pg: &'static PgFixture = Box::leak(Box::new(engine::start_postgres(hint)));
        configure(EngineBuilder::new(hint, pg))
            .expected_deployments(1)
            .start(&archives)
    })
    .join()
    .expect("engine start");
    Box::leak(Box::new(handle))
}

// ---- Kafka --------------------------------------------------------------------------------------

struct KafkaFixture {
    pki: TestBrokerPki,
    broker: TlsKafkaBroker,
    broker_name: String,
}

static KAFKA: OnceLock<KafkaFixture> = OnceLock::new();

fn kafka_fixture() -> &'static KafkaFixture {
    KAFKA.get_or_init(|| {
        std::thread::spawn(|| {
            let hint = "brokertlskafka";
            let broker_name = format!("{hint}-kafka-{}", util::short_id());
            let pki = TestBrokerPki::generate(&[broker_name.as_str(), "localhost"]);
            let broker = start_tls_kafka(
                &pki,
                &BrokerPlacement {
                    network: Some(util::network_for(hint).to_string()),
                    container_name: Some(broker_name.clone()),
                },
            );
            KafkaFixture {
                pki,
                broker,
                broker_name,
            }
        })
        .join()
        .expect("kafka topology")
    })
}

fn kafka_channels(f: &KafkaFixture, in_topic: &str, out_topic: &str) -> String {
    format!(
        r#"channels:
  - name: orders-in
    transport: kafka
    codec: json
    cloudevents-mode: none
    bootstrap.servers: {}:{KAFKA_NETWORK_SASL_SSL_PORT}
    topic: {in_topic}
    group.id: {}
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-256
    sasl.username: env:BROKER_USER
    sasl.password: secret:BROKER_PASSWORD
    tls-ca-pem: secret:BROKER_CA
  - name: orders-out
    direction: outbound
    transport: kafka
    bind: kafka://{out_topic}
    cloudevents-mode: none
"#,
        f.broker_name,
        unique_token("g")
    )
}

/// An engine whose inbound channel and engine-wide sink both go to the SASL_SSL listener. The sink
/// trusts the CA by file, and the channel by a secret reference to PEM text.
fn kafka_engine(
    f: &'static KafkaFixture,
    archives: &Path,
    password: &str,
    ca_pem: &str,
) -> &'static EngineHandle {
    let password = password.to_string();
    let ca_pem = ca_pem.to_string();
    let configure = move |builder: EngineBuilder| {
        builder
            .env("BROKER_USER", f.broker.username.clone())
            .secret("BROKER_PASSWORD", password.clone())
            .secret("BROKER_CA", ca_pem)
            .mount_read_only(&ca_dir(&f.pki), CA_MOUNT)
            .env(
                "SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS",
                format!("{}:{KAFKA_NETWORK_SASL_SSL_PORT}", f.broker_name),
            )
            .env("SUTRA_SINK_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
            .env("SUTRA_SINK_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
            .env("SUTRA_SINK_KAFKA_SASL_USERNAME", f.broker.username.clone())
            .env("SUTRA_SINK_KAFKA_SASL_PASSWORD", password)
            .env("SUTRA_SINK_KAFKA_TLS_CA_FILE", format!("{CA_MOUNT}/ca.pem"))
    };
    start_engine("brokertlskafka", configure, archives)
}

async fn create_topics(f: &KafkaFixture, topics: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &f.broker.plain_bootstrap)
        .create()
        .expect("admin client");
    let new: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();
    for result in admin
        .create_topics(new.iter(), &AdminOptions::new())
        .await
        .expect("create topics")
    {
        result.expect("topic created");
    }
}

async fn kafka_produce(f: &KafkaFixture, topic: &str, body: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &f.broker.plain_bootstrap)
        .create()
        .expect("producer");
    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(body).headers(
                rdkafka::message::OwnedHeaders::new().insert(rdkafka::message::Header {
                    key: "content-type",
                    value: Some("application/json"),
                }),
            ),
            Timeout::After(Duration::from_secs(20)),
        )
        .await
        .expect("produce");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_kafka_sasl_ssl_message_in_result_out_through_the_engine() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = kafka_fixture();
    let in_topic = format!("in-{}", unique_token("t"));
    let out_topic = format!("out-{}", unique_token("t"));
    create_topics(f, &[&in_topic, &out_topic]).await;
    let archives = seal_package(&kafka_channels(f, &in_topic, &out_topic));
    let engine = kafka_engine(f, &archives, &f.broker.password, &f.pki.ca_pem);

    let reference = unique_token("ref");
    kafka_produce(f, &in_topic, &format!(r#"{{"ref":"{reference}"}}"#)).await;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &f.broker.plain_bootstrap)
        .set("group.id", unique_token("readback"))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer");
    consumer
        .subscribe(&[out_topic.as_str()])
        .expect("subscribe");
    let record = tokio::time::timeout(Duration::from_secs(120), consumer.recv())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "no result on the output topic; engine log:\n{}",
                container_logs(engine.container.id())
            )
        })
        .expect("read the result");
    let body = String::from_utf8_lossy(record.payload().unwrap_or_default()).into_owned();
    assert!(
        body.contains(&reference) && body.contains("ANSWERED"),
        "{body}"
    );
    let logs = container_logs(engine.container.id());
    assert!(
        !logs.contains(&f.broker.password),
        "the engine log holds the password"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_kafka_wrong_password_is_refused_with_a_clear_log_and_no_secret() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = kafka_fixture();
    let in_topic = format!("in-{}", unique_token("t"));
    create_topics(f, &[&in_topic]).await;
    let archives = seal_package(&kafka_channels(f, &in_topic, "unused-out"));
    let wrong = unique_token("wrongpw");
    let engine = kafka_engine(f, &archives, &wrong, &f.pki.ca_pem);
    assert_logs_code_without_secret(engine, "SUTRA.INBOUND.KAFKA.BROKER_AUTH_FAILED", &wrong).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_kafka_wrong_ca_is_refused_with_a_clear_log() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = kafka_fixture();
    let in_topic = format!("in-{}", unique_token("t"));
    create_topics(f, &[&in_topic]).await;
    let archives = seal_package(&kafka_channels(f, &in_topic, "unused-out"));
    let engine = kafka_engine(f, &archives, &f.broker.password, &f.pki.foreign_ca_pem);
    assert_logs_code_without_secret(
        engine,
        "SUTRA.INBOUND.KAFKA.BROKER_TLS_FAILED",
        &f.broker.password,
    )
    .await;
}

// ---- AMQP 1.0 -----------------------------------------------------------------------------------

struct ArtemisFixture {
    pki: TestBrokerPki,
    broker: TlsArtemisBroker,
    broker_name: String,
}

static ARTEMIS: OnceLock<ArtemisFixture> = OnceLock::new();

fn artemis_fixture() -> &'static ArtemisFixture {
    ARTEMIS.get_or_init(|| {
        std::thread::spawn(|| {
            let hint = "brokertlsamqp";
            let broker_name = format!("{hint}-artemis-{}", util::short_id());
            let pki = TestBrokerPki::generate(&[broker_name.as_str()]);
            let broker = start_tls_artemis(
                &pki,
                &BrokerPlacement {
                    network: Some(util::network_for(hint).to_string()),
                    container_name: Some(broker_name.clone()),
                },
            );
            ArtemisFixture {
                pki,
                broker,
                broker_name,
            }
        })
        .join()
        .expect("artemis topology")
    })
}

fn amqp_channels(f: &ArtemisFixture, in_queue: &str, out_queue: &str) -> String {
    format!(
        r#"channels:
  - name: orders-in
    transport: amqp
    codec: json
    cloudevents-mode: none
    host: {name}
    tls: true
    tls-ca-file: {CA_MOUNT}/ca.pem
    username: env:BROKER_USER
    password: secret:BROKER_PASSWORD
    queue: {in_queue}
  - name: orders-out
    direction: outbound
    transport: amqp
    bind: amqp10s://{name}/{out_queue}
    cloudevents-mode: none
"#,
        name = f.broker_name
    )
}

/// An engine whose inbound channel and sink both use AMQPS.
fn amqp_engine(
    f: &'static ArtemisFixture,
    archives: &Path,
    password: &str,
) -> &'static EngineHandle {
    let password = password.to_string();
    let configure = move |builder: EngineBuilder| {
        builder
            .env("BROKER_USER", f.broker.username.clone())
            .secret("BROKER_PASSWORD", password.clone())
            .mount_read_only(&ca_dir(&f.pki), CA_MOUNT)
            .env("SUTRA_SINK_AMQP_USERNAME", f.broker.username.clone())
            .env("SUTRA_SINK_AMQP_PASSWORD", password)
            .env("SUTRA_SINK_AMQP_TLS_CA_FILE", format!("{CA_MOUNT}/ca.pem"))
    };
    start_engine("brokertlsamqp", configure, archives)
}

fn driver_uri(f: &ArtemisFixture) -> String {
    format!(
        "amqp://{}:{}@127.0.0.1:{}",
        f.broker.username, f.broker.password, f.broker.plain_port
    )
}

async fn amqp_send(f: &ArtemisFixture, queue: &str, body: &str) {
    let mut connection = Connection::open("tc-driver-out", driver_uri(f).as_str())
        .await
        .expect("open the driver's connection");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let mut sender = Sender::attach(&mut session, format!("tc-driver-{queue}"), queue)
        .await
        .expect("attach");
    let properties = fe2o3_amqp::types::messaging::ApplicationProperties::builder()
        .insert("content-type", "application/json")
        .build();
    let message = Message::builder()
        .application_properties(properties)
        .data(body.as_bytes().to_vec())
        .build();
    sender
        .send(message)
        .await
        .expect("send")
        .accepted_or_else(|s| panic!("not accepted: {s:?}"))
        .expect("accepted");
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
}

async fn amqp_read(f: &ArtemisFixture, queue: &str, engine: &EngineHandle) -> String {
    let mut connection = Connection::open("tc-driver-in", driver_uri(f).as_str())
        .await
        .expect("open the driver's connection");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let mut receiver = Receiver::attach(&mut session, format!("tc-reader-{queue}"), queue)
        .await
        .expect("attach");
    let delivery = tokio::time::timeout(Duration::from_secs(120), receiver.recv::<Body<Value>>())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "no result on the output queue; engine log:\n{}",
                container_logs(engine.container.id())
            )
        })
        .expect("recv");
    receiver.accept(&delivery).await.expect("accept");
    let body = match &delivery.message().body {
        Body::Data(batch) => batch
            .iter()
            .flat_map(|d| d.0.iter().copied())
            .collect::<Vec<u8>>(),
        other => panic!("unexpected body {other:?}"),
    };
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    String::from_utf8_lossy(&body).into_owned()
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_amqp_tls_message_in_result_out_through_the_engine() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = artemis_fixture();
    let in_queue = format!("in-{}", unique_token("q"));
    let out_queue = format!("out-{}", unique_token("q"));
    let archives = seal_package(&amqp_channels(f, &in_queue, &out_queue));
    let engine = amqp_engine(f, &archives, &f.broker.password);

    let reference = unique_token("ref");
    amqp_send(f, &in_queue, &format!(r#"{{"ref":"{reference}"}}"#)).await;

    let body = amqp_read(f, &out_queue, engine).await;
    assert!(
        body.contains(&reference) && body.contains("ANSWERED"),
        "{body}"
    );
    let logs = container_logs(engine.container.id());
    assert!(
        !logs.contains(&f.broker.password),
        "the engine log holds the password"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_amqp_wrong_password_is_refused_with_a_clear_log_and_no_secret() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = artemis_fixture();
    let archives = seal_package(&amqp_channels(
        f,
        &format!("in-{}", unique_token("q")),
        "unused-out",
    ));
    let wrong = unique_token("wrongpw");
    let engine = amqp_engine(f, &archives, &wrong);
    assert_logs_code_without_secret(engine, "SUTRA.INBOUND.AMQP.BROKER_AUTH_FAILED", &wrong).await;
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tc_amqp_wrong_ca_is_refused_with_a_clear_log() {
    let _suite = sutra_testkit::hold_suite!("brokertlskafka", "brokertlsamqp");
    let f = artemis_fixture();
    let channels = amqp_channels(f, &format!("in-{}", unique_token("q")), "unused-out")
        .replace("ca.pem", "foreign-ca.pem");
    let archives = seal_package(&channels);
    let engine = amqp_engine(f, &archives, &f.broker.password);
    assert_logs_code_without_secret(
        engine,
        "SUTRA.INBOUND.AMQP.BROKER_TLS_FAILED",
        &f.broker.password,
    )
    .await;
}
