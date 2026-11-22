//! Concurrent AMQP 1.0 intake with the real engine and a database: two copies of one message key
//! in flight at once start one instance. Inbox dedup serializes them in the database, the second
//! copy sees a duplicate, and both deliveries are accepted.
//!
//! The fixture (an Artemis broker, a package whose AMQP intake parks each instance on a long
//! timer, a producer) is shared with the intake benchmark in `amqp_intake_bench`.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Header, Message, Source, Target};
use fe2o3_amqp::types::primitives::{Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use sqlx::PgPool;

use crate::archive_activation_conformance::{
    archive_engine_config, fresh_db, package, place_archive, temp_root, wait_until, write,
};

const ARTEMIS: &str = "apache/activemq-artemis:latest-alpine";

fn docker(args: &[&str]) -> (bool, String) {
    let out = Command::new("docker").args(args).output().expect("docker");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn unique(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!(
        "{prefix}{}x{}x{nanos}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// An Artemis container of the test's own, removed when dropped.
pub(crate) struct Broker {
    name: String,
    pub(crate) port: u16,
}

impl Broker {
    pub(crate) fn start() -> Broker {
        let name = unique("sutra-amqp-intake-");
        let port = free_port();
        let (ok, text) = docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            &format!("127.0.0.1:{port}:5672"),
            "-e",
            "ARTEMIS_USER=artemis",
            "-e",
            "ARTEMIS_PASSWORD=artemis",
            "-e",
            "ANONYMOUS_LOGIN=true",
            ARTEMIS,
        ]);
        assert!(ok, "docker run: {text}");
        sutra_testkit::reap_on_exit(text.trim().to_string());
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            let (_, logs) = docker(&["logs", &name]);
            if logs.contains("Server is now active")
                && std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
            {
                return Broker { name, port };
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        let _ = docker(&["rm", "-f", &name]);
        panic!("broker {name} not active");
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

pub(crate) async fn start_broker() -> Broker {
    tokio::task::spawn_blocking(Broker::start)
        .await
        .expect("broker")
}

/// A fresh queue name.
pub(crate) fn fresh_queue() -> String {
    unique("intake-q-")
}

/// A producer on one queue: durable messages, each carrying its key as the outbox key.
pub(crate) struct Producer {
    connection: fe2o3_amqp::connection::ConnectionHandle<()>,
    session: fe2o3_amqp::session::SessionHandle<()>,
    sender: Sender,
}

impl Producer {
    pub(crate) async fn open(port: u16, queue: &str) -> Producer {
        let mut connection = Connection::open(
            unique("intake-producer").as_str(),
            format!("amqp://127.0.0.1:{port}").as_str(),
        )
        .await
        .expect("open producer connection");
        let mut session = Session::begin(&mut connection).await.expect("session");
        let sender = Sender::builder()
            .name(unique("intake-producer"))
            .target(
                Target::builder()
                    .address(queue.to_string())
                    .capabilities(vec![Symbol::from("queue")])
                    .build(),
            )
            .attach(&mut session)
            .await
            .expect("attach sender");
        Producer {
            connection,
            session,
            sender,
        }
    }

    /// Send one message and wait for the broker to accept it.
    pub(crate) async fn send(&mut self, key: &str) {
        let ap = ApplicationProperties::builder()
            .insert("sutra-outbox-key", key)
            .insert("content-type", "application/json")
            .build();
        let message = Message::builder()
            .header(Header {
                durable: true,
                ..Default::default()
            })
            .application_properties(ap)
            .data(format!("{{\"key\":\"{key}\"}}").into_bytes())
            .build();
        self.sender
            .send(message)
            .await
            .expect("send")
            .accepted_or_else(|s| panic!("send not accepted: {s:?}"))
            .unwrap();
    }

    pub(crate) async fn close(mut self) {
        let _ = self.sender.close().await;
        let _ = self.session.end().await;
        let _ = self.connection.close().await;
    }
}

/// How many messages are still on `queue` (read and accepted until it stays empty for 2 s).
pub(crate) async fn remaining(port: u16, queue: &str) -> usize {
    let mut connection = Connection::open(
        unique("intake-reader").as_str(),
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open reader");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let mut receiver = Receiver::builder()
        .name(unique("intake-reader"))
        .source(
            Source::builder()
                .address(queue.to_string())
                .capabilities(vec![Symbol::from("queue")])
                .build(),
        )
        .attach(&mut session)
        .await
        .expect("attach receiver");
    let mut n = 0;
    while let Ok(delivery) =
        tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await
    {
        let delivery = delivery.expect("recv");
        receiver.accept(&delivery).await.expect("accept");
        n += 1;
    }
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    n
}

/// A package with one AMQP intake channel per queue (`in-0`, `in-1`, …), each starting a process
/// that parks on a one-hour timer: the dispatch persists the instance, then the intake answers
/// and the delivery is accepted.
pub(crate) fn intake_package(broker_port: u16, queues: &[String], max_in_flight: u32) -> PathBuf {
    let mut channels = String::from("channels:\n");
    let mut processes = String::new();
    for (i, queue) in queues.iter().enumerate() {
        channels.push_str(&format!(
            "  - name: in-{i}\n    transport: amqp\n    codec: json\n    cloudevents-mode: none\n    \
             host: 127.0.0.1\n    port: {broker_port}\n    queue: {queue}\n    address-type: queue\n    \
             max-in-flight: {max_in_flight}\n"
        ));
        processes.push_str(&format!(
            r#"  <bpmn:process id="park{i}" isExecutable="true">
    <bpmn:startEvent id="S{i}">
      <bpmn:extensionElements><q:source channel="in-{i}" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Park{i}">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E{i}"/>
    <bpmn:sequenceFlow id="a{i}" sourceRef="S{i}" targetRef="Park{i}"/>
    <bpmn:sequenceFlow id="b{i}" sourceRef="Park{i}" targetRef="E{i}"/>
  </bpmn:process>
"#
        ));
    }
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_intake"
                  targetNamespace="urn:sutra:module:intake:1.0.0">
{processes}</bpmn:definitions>
"#
    );
    let root = temp_root("amqp-intake-pkg");
    write(&root, "bpmn/intake.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"intake\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let dir = temp_root("amqp-intake-dir");
    place_archive(&dir, "intake.sutra", &package(&root));
    dir
}

/// Boot an engine on a fresh database with the intake package deployed.
pub(crate) async fn boot(
    broker_port: u16,
    queues: &[String],
    max_in_flight: u32,
) -> (sutra_engine::RunningEngine, PgPool) {
    let (pool, url) = fresh_db().await;
    let dir = intake_package(broker_port, queues, max_in_flight);
    let engine = sutra_engine::serve(archive_engine_config(dir, Some(url)))
        .await
        .expect("engine boots");
    (engine, pool)
}

/// Instances parked on the timer.
pub(crate) async fn parked(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM waiting_event WHERE status = 'WAITING'")
        .fetch_one(pool)
        .await
        .expect("count parked")
}

/// Two copies of one key, in flight together, start one instance; both are accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn two_copies_of_one_key_in_flight_start_one_instance() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    // Both copies wait on the queue before the consumer attaches, so the window takes them
    // together.
    let mut producer = Producer::open(broker.port, &queue).await;
    producer.send("dup-1").await;
    producer.send("dup-1").await;
    producer.send("other-1").await;
    producer.close().await;

    let (engine, pool) = boot(broker.port, std::slice::from_ref(&queue), 8).await;
    assert!(
        wait_until(60, || async { parked(&pool).await >= 2 }).await,
        "the instances park"
    );
    // Give the second copy of `dup-1` time to finish as a duplicate.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(parked(&pool).await, 2, "one instance per key");
    let seen: i64 =
        sqlx::query_scalar("SELECT count(*) FROM inbox_seen WHERE event_id LIKE '%dup-1%'")
            .fetch_one(&pool)
            .await
            .expect("count inbox");
    assert_eq!(seen, 1, "the key is recorded once");
    engine.shutdown().await;
    assert_eq!(
        remaining(broker.port, &queue).await,
        0,
        "both copies were accepted"
    );
}
