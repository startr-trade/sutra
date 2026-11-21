//! The AMQP 1.0 sink's connection pool against a real broker (Apache ActiveMQ Artemis, AMQP 1.0 on
//! 5672, in a container started with the docker CLI so a test can restart it on a fixed port).
//!
//! - sequential sends reuse one connection, and every message arrives once;
//! - concurrent sends stay within the per-broker connection bound, and every message arrives once;
//! - a broker restart leaves the pooled connections dead: the next send reconnects by itself and
//!   nothing accepted is lost;
//! - the drain closes the pooled connections, and a send after it still delivers;
//! - the benchmark: the same messages sent one connection per send (what the sink did before it
//!   pooled) and on the pool, sequentially and eight at a time.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Message, Source, Target};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_testkit::broker_tls::unique_token;
use sutra_transport_amqp::AmqpMessageSink;

// ---- the broker ----------------------------------------------------------------------------

fn docker(args: &[&str]) -> (bool, String) {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("run docker (docker required)");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .unwrap()
        .port()
}

/// A broker container on a fixed loopback port, so a restart keeps the address.
struct Broker {
    name: String,
    port: u16,
}

impl Broker {
    fn start() -> Broker {
        let name = format!("sutra-amqp-pool-{}", unique_token("b"));
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
            "apache/activemq-artemis:latest-alpine",
        ]);
        assert!(ok, "docker run: {text}");
        sutra_testkit::reap_on_exit(text.trim().to_string());
        let broker = Broker { name, port };
        broker.wait_active(1);
        broker
    }

    /// Wait for the `n`th "Server is now active" in the log and for the port to answer.
    fn wait_active(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            let (_, logs) = docker(&["logs", &self.name]);
            if logs.matches("Server is now active").count() >= n
                && std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("broker {} not active ({n})", self.name);
    }

    fn destination(&self, queue: &str) -> String {
        format!(
            "amqp10://127.0.0.1:{}/{queue}?type=queue&durable=true",
            self.port
        )
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

/// One broker shared by the tests that do not restart it.
fn shared() -> &'static Broker {
    static SHARED: OnceLock<Broker> = OnceLock::new();
    SHARED.get_or_init(|| std::thread::spawn(Broker::start).join().expect("broker"))
}

// ---- helpers -------------------------------------------------------------------------------

fn message(destination: &str, key: &str) -> OutboundMessage {
    OutboundMessage {
        destination: destination.to_string(),
        headers: BTreeMap::new(),
        body: key.as_bytes().to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: key.to_string(),
        traceparent: None,
    }
}

fn expect_delivered(outcome: SendOutcome) {
    assert!(
        matches!(outcome, SendOutcome::Delivered),
        "expected Delivered, got {outcome:?}"
    );
}

/// Send until the sink reports delivered, as the outbox does with a retryable failure.
async fn send_until_delivered(sink: &AmqpMessageSink, m: &OutboundMessage) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut attempts = 0;
    loop {
        attempts += 1;
        match sink.send(m).await {
            SendOutcome::Delivered => return attempts,
            SendOutcome::PermanentFailure(d) => panic!("permanent failure: {d:?}"),
            SendOutcome::RetryableFailure(d) => {
                assert!(Instant::now() < deadline, "never delivered: {d:?}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Every outbox key on `queue`, in arrival order, read until the queue stays empty for a second.
async fn drain_queue(port: u16, queue: &str) -> Vec<String> {
    let mut connection = Connection::open(
        unique_token("pool-reader").as_str(),
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open reader");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let source = Source::builder()
        .address(queue.to_string())
        .capabilities(vec![Symbol::from("queue")])
        .build();
    let mut receiver = Receiver::builder()
        .name(unique_token("pool-reader"))
        .source(source)
        .attach(&mut session)
        .await
        .expect("attach receiver");
    let mut keys = Vec::new();
    loop {
        let next =
            tokio::time::timeout(Duration::from_secs(1), receiver.recv::<Body<Value>>()).await;
        let Ok(delivery) = next else { break };
        let delivery = delivery.expect("recv");
        let key = delivery
            .message()
            .application_properties
            .as_ref()
            .and_then(|ap: &ApplicationProperties| ap.get("sutra-outbox-key"))
            .map(|v| match v {
                SimpleValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
            .expect("outbox key");
        receiver.accept(&delivery).await.expect("accept");
        keys.push(key);
    }
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    keys
}

fn assert_each_once(received: &[String], sent: &[String]) {
    let unique: BTreeSet<&String> = received.iter().collect();
    assert_eq!(unique.len(), received.len(), "a message arrived twice");
    let expected: BTreeSet<&String> = sent.iter().collect();
    assert_eq!(unique, expected, "every message arrived");
}

// ---- reuse ---------------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_sends_reuse_one_connection_and_each_arrives_once() {
    let broker = shared();
    let queue = unique_token("seq");
    let sink = AmqpMessageSink::new(None, None);
    let keys: Vec<String> = (0..50).map(|i| unique_token(&format!("s{i}-"))).collect();
    for key in &keys {
        expect_delivered(sink.send(&message(&broker.destination(&queue), key)).await);
    }
    assert_eq!(sink.connections_opened(), 1, "one connection for 50 sends");
    assert_eq!(sink.idle_connections(), 1);
    assert_each_once(&drain_queue(broker.port, &queue).await, &keys);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sends_stay_within_the_connection_bound_and_each_arrives_once() {
    let broker = shared();
    let queues = [unique_token("cq-a"), unique_token("cq-b")];
    let sink = Arc::new(AmqpMessageSink::new(None, None).with_max_connections(3));
    let mut sent = [Vec::new(), Vec::new()];
    let mut tasks = Vec::new();
    for i in 0..120 {
        let which = i % 2;
        let key = unique_token(&format!("c{i}-"));
        sent[which].push(key.clone());
        let m = message(&broker.destination(&queues[which]), &key);
        let sink = Arc::clone(&sink);
        tasks.push(tokio::spawn(async move { sink.send(&m).await }));
    }
    for task in tasks {
        expect_delivered(task.await.unwrap());
    }
    assert!(
        sink.connections_opened() <= 3,
        "at most the bound: {}",
        sink.connections_opened()
    );
    for (queue, keys) in queues.iter().zip(&sent) {
        assert_each_once(&drain_queue(broker.port, queue).await, keys);
    }
}

// ---- failure and shutdown ------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_restart_is_ridden_out_on_a_fresh_connection_and_nothing_is_lost() {
    let broker = Broker::start();
    let queue = unique_token("restart");
    let destination = broker.destination(&queue);
    let sink =
        Arc::new(AmqpMessageSink::new(None, None).with_operation_timeout(Duration::from_secs(5)));
    let before: Vec<String> = (0..10).map(|i| unique_token(&format!("b{i}-"))).collect();
    for key in &before {
        expect_delivered(sink.send(&message(&destination, key)).await);
    }
    assert_eq!(sink.connections_opened(), 1);

    // The broker goes away while the pooled connection sits idle, then comes back.
    let (ok, text) = docker(&["restart", "-t", "10", &broker.name]);
    assert!(ok, "docker restart: {text}");
    broker.wait_active(2);

    // The first send after the restart finds its pooled connection dead and reconnects at once.
    let first = unique_token("after-first");
    expect_delivered(sink.send(&message(&destination, &first)).await);
    assert_eq!(sink.connections_opened(), 2, "one reconnect");

    // Sends during a second outage fail retryably and are sent again until accepted.
    let (ok, text) = docker(&["stop", "-t", "10", &broker.name]);
    assert!(ok, "docker stop: {text}");
    let during: Vec<String> = (0..5).map(|i| unique_token(&format!("d{i}-"))).collect();
    let retries: Vec<_> = during
        .iter()
        .map(|key| {
            let sink = Arc::clone(&sink);
            let m = message(&destination, key);
            tokio::spawn(async move { send_until_delivered(&sink, &m).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (ok, text) = docker(&["start", &broker.name]);
    assert!(ok, "docker start: {text}");
    broker.wait_active(3);
    for retry in retries {
        assert!(
            retry.await.unwrap() > 1,
            "a send during the outage was refused first"
        );
    }

    let mut sent = before.clone();
    sent.push(first);
    sent.extend(during);
    assert_each_once(&drain_queue(broker.port, &queue).await, &sent);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_closes_the_pooled_connections_and_a_later_send_still_delivers() {
    let broker = shared();
    let queue = unique_token("drain");
    let sink = AmqpMessageSink::new(None, None);
    let keys = [unique_token("d1-"), unique_token("d2-")];
    expect_delivered(
        sink.send(&message(&broker.destination(&queue), &keys[0]))
            .await,
    );
    assert_eq!(sink.idle_connections(), 1);
    sink.drain().await;
    assert_eq!(
        sink.idle_connections(),
        0,
        "the drain closed the pooled connection"
    );
    expect_delivered(
        sink.send(&message(&broker.destination(&queue), &keys[1]))
            .await,
    );
    assert_eq!(
        sink.idle_connections(),
        0,
        "after the drain nothing is pooled"
    );
    assert_each_once(&drain_queue(broker.port, &queue).await, &keys);
}

// ---- benchmark -----------------------------------------------------------------------------

/// One message on a connection of its own: open, session, attach, send, settle, close. This is
/// what the sink did for every send before it pooled its connections.
async fn send_on_own_connection(port: u16, queue: &str, key: &str) {
    let mut connection = Connection::open(
        unique_token("bench-own").as_str(),
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let target = Target::builder()
        .address(queue.to_string())
        .capabilities(vec![Symbol::from("queue")])
        .build();
    let mut sender = Sender::builder()
        .name(format!("bench-{queue}"))
        .target(target)
        .attach(&mut session)
        .await
        .expect("attach");
    let ap = ApplicationProperties::builder()
        .insert("sutra-outbox-key", key)
        .build();
    let m = Message::builder()
        .application_properties(ap)
        .data(key.as_bytes().to_vec())
        .build();
    let outcome = sender.send(m).await.expect("send");
    assert!(outcome.is_accepted(), "{outcome:?}");
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

struct Run {
    label: &'static str,
    messages: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

impl Run {
    fn rate(&self) -> f64 {
        self.messages as f64 / self.elapsed.as_secs_f64()
    }

    fn report(&mut self) -> String {
        self.latencies.sort();
        format!(
            "{:<34} {:>5} msgs {:>8.1} msg/s  p50 {:>7.2} ms  p99 {:>7.2} ms",
            self.label,
            self.messages,
            self.rate(),
            percentile(&self.latencies, 0.50).as_secs_f64() * 1000.0,
            percentile(&self.latencies, 0.99).as_secs_f64() * 1000.0,
        )
    }
}

async fn timed<F, Fut>(label: &'static str, messages: usize, concurrency: usize, send: F) -> Run
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let send = Arc::new(send);
    let started = Instant::now();
    let per_task = messages / concurrency;
    let tasks: Vec<_> = (0..concurrency)
        .map(|t| {
            let send = Arc::clone(&send);
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_task);
                for i in 0..per_task {
                    let at = Instant::now();
                    send(t * per_task + i).await;
                    latencies.push(at.elapsed());
                }
                latencies
            })
        })
        .collect();
    let mut latencies = Vec::with_capacity(messages);
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    Run {
        label,
        messages: per_task * concurrency,
        elapsed: started.elapsed(),
        latencies,
    }
}

/// The pool's gain, measured: the same broker, the same messages, one connection per send against
/// the pool, one at a time and eight at a time. Prints the table (`--nocapture`) and asserts the
/// pool is faster each way.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn benchmark_pooled_sends_against_a_connection_per_send() {
    let broker = shared();
    let port = broker.port;
    let n: usize = std::env::var("SUTRA_AMQP_BENCH_MESSAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    // Warm the broker's queues and the JIT on both paths first.
    let warm_queue = unique_token("bench-warm");
    for i in 0..20 {
        send_on_own_connection(port, &warm_queue, &format!("w{i}")).await;
    }
    let mut runs = Vec::new();
    for (concurrency, own_label, pool_label) in [
        (1, "connection per send, 1 at a time", "pooled, 1 at a time"),
        (8, "connection per send, 8 at a time", "pooled, 8 at a time"),
    ] {
        let queue = Arc::new(unique_token("bench-own"));
        let q = Arc::clone(&queue);
        let own = timed(own_label, n, concurrency, move |i| {
            let q = Arc::clone(&q);
            async move { send_on_own_connection(port, &q, &format!("o{i}")).await }
        })
        .await;
        let sink = Arc::new(AmqpMessageSink::new(None, None));
        let destination = Arc::new(broker.destination(&unique_token("bench-pool")));
        let s = Arc::clone(&sink);
        let d = Arc::clone(&destination);
        let pooled = timed(pool_label, n, concurrency, move |i| {
            let s = Arc::clone(&s);
            let d = Arc::clone(&d);
            async move { expect_delivered(s.send(&message(&d, &format!("p{i}"))).await) }
        })
        .await;
        assert!(sink.connections_opened() <= concurrency as u64);
        runs.push((own, pooled));
    }
    let mut table = String::from("AMQP 1.0 sink, Artemis on loopback:\n");
    for (own, pooled) in &mut runs {
        table.push_str(&own.report());
        table.push('\n');
        table.push_str(&pooled.report());
        table.push_str(&format!("   ({:.1}x)\n", pooled.rate() / own.rate()));
    }
    eprintln!("{table}");
    for (own, pooled) in &runs {
        assert!(pooled.rate() > own.rate(), "the pool is faster: {table}");
    }
}
