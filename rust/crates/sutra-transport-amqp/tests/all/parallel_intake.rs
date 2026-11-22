//! Concurrent AMQP 1.0 intake per binding, against a real broker (Apache ActiveMQ Artemis in a
//! container started with the docker CLI, so a test can restart it on a fixed port).
//!
//! - up to `max-in-flight` deliveries are in the intake at once, never more;
//! - a delivery is settled only after its own intake call returns;
//! - a stop drains the window and settles every call that finished;
//! - a lost connection drains without settling, and the broker redelivers each delivery once;
//! - `on-complete` deliveries that the engine defers free their window slot;
//! - `max-in-flight: 1` processes deliveries one at a time, in arrival order.
//!
//! The intake here is a gated double that counts the calls running at once. The duplicate
//! absorbed by inbox dedup, with the engine and a database, is `sutra-engine`'s
//! `amqp_parallel_intake_it`.

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Header, Message, Source, Target};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use sutra_channels::source::{
    AckDecision, AlwaysLeading, DeferredSettle, DeliveryDisposition, InboundIntake, TriggerSource,
};
use sutra_channels::{BoxFuture, InboundMessage};
use sutra_testkit::broker_tls::unique_token;
use sutra_transport_amqp::{
    AckMode, AddressType, AmqpChannelProperties, AmqpSourceConfig, AmqpTriggerSource,
};
use tokio::sync::Semaphore;

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

/// A broker container of the test's own on a fixed loopback port, so a restart keeps the
/// address. Removed when dropped.
struct Broker {
    name: String,
    port: u16,
}

impl Broker {
    fn start() -> Broker {
        let name = format!("sutra-amqp-par-{}", unique_token("b"));
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

    /// Stop the container: every open connection drops, durable messages stay stored.
    fn stop(&self) {
        let (ok, text) = docker(&["stop", "-t", "5", &self.name]);
        assert!(ok, "docker stop: {text}");
    }

    /// Start the stopped container again on the same port.
    fn start_again(&self) {
        let (ok, text) = docker(&["start", &self.name]);
        assert!(ok, "docker start: {text}");
        self.wait_active(2);
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

/// Run a blocking broker step off the runtime.
async fn on_broker(broker: &Broker, step: fn(&Broker)) {
    let copy = Broker {
        name: broker.name.clone(),
        port: broker.port,
    };
    tokio::task::spawn_blocking(move || {
        step(&copy);
        std::mem::forget(copy); // the caller's guard removes the container
    })
    .await
    .expect("broker step");
}

async fn start_broker() -> Broker {
    tokio::task::spawn_blocking(Broker::start)
        .await
        .expect("broker")
}

// ---- producing and reading -----------------------------------------------------------------

fn fresh_queue() -> String {
    unique_token("par-q-")
}

/// Send one durable message per key, in order, each carrying its key as the outbox key.
async fn send_keys(port: u16, queue: &str, keys: &[String]) {
    let mut connection = Connection::open(
        unique_token("par-producer").as_str(),
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open producer connection");
    let mut session = Session::begin(&mut connection)
        .await
        .expect("begin session");
    let mut sender = Sender::builder()
        .name(unique_token("par-producer"))
        .target(
            Target::builder()
                .address(queue.to_string())
                .capabilities(vec![Symbol::from("queue")])
                .build(),
        )
        .attach(&mut session)
        .await
        .expect("attach sender");
    for key in keys {
        let ap = ApplicationProperties::builder()
            .insert("sutra-outbox-key", key.as_str())
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
        sender
            .send(message)
            .await
            .expect("send")
            .accepted_or_else(|s| panic!("send not accepted: {s:?}"))
            .unwrap();
    }
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
}

/// The keys still on `queue`, read (and accepted) until it stays empty for two seconds.
async fn remaining(port: u16, queue: &str) -> Vec<String> {
    let mut connection = Connection::open(
        unique_token("par-reader").as_str(),
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open reader");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let mut receiver = Receiver::builder()
        .name(unique_token("par-reader"))
        .source(
            Source::builder()
                .address(queue.to_string())
                .capabilities(vec![Symbol::from("queue")])
                .build(),
        )
        .attach(&mut session)
        .await
        .expect("attach receiver");
    let mut keys = Vec::new();
    loop {
        let next =
            tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await;
        let Ok(delivery) = next else { break };
        let delivery = delivery.expect("recv");
        let key = delivery
            .message()
            .application_properties
            .as_ref()
            .and_then(|ap| ap.get("sutra-outbox-key"))
            .map(|v| match v {
                SimpleValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
            .unwrap_or_default();
        receiver.accept(&delivery).await.expect("accept");
        keys.push(key);
    }
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    keys
}

fn keys(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}-{i:03}")).collect()
}

// ---- the intake double ---------------------------------------------------------------------

/// An intake whose calls wait for a permit before they return, and which counts how many run
/// at once. Under `defer`, `deliver_deferred` keeps the settle callbacks and answers `Deferred`.
struct GatedIntake {
    permits: Semaphore,
    running: AtomicUsize,
    peak: AtomicUsize,
    finished: AtomicUsize,
    started: Mutex<Vec<String>>,
    /// Per-key sleep before returning, to scramble completion order.
    jitter: bool,
    defer: bool,
    deferred: Mutex<Vec<DeferredSettle>>,
}

impl Default for GatedIntake {
    fn default() -> Self {
        GatedIntake {
            permits: Semaphore::new(0),
            running: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            started: Mutex::new(Vec::new()),
            jitter: false,
            defer: false,
            deferred: Mutex::new(Vec::new()),
        }
    }
}

impl GatedIntake {
    fn gated() -> Arc<GatedIntake> {
        Arc::new(GatedIntake::default())
    }

    fn release(&self, n: usize) {
        self.permits.add_permits(n);
    }

    fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }

    fn count_of(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for key in self.started() {
            *counts.entry(key).or_insert(0) += 1;
        }
        counts
    }
}

impl InboundIntake for GatedIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            let key = message.idempotency_key.clone();
            let index = self.started.lock().unwrap().len();
            self.started.lock().unwrap().push(key);
            self.permits.acquire().await.expect("open").forget();
            if self.jitter {
                tokio::time::sleep(Duration::from_millis(((index * 7) % 13) as u64)).await;
            }
            self.running.fetch_sub(1, Ordering::SeqCst);
            self.finished.fetch_add(1, Ordering::SeqCst);
            AckDecision::Ack
        })
    }

    fn deliver_deferred(
        &self,
        message: InboundMessage,
        settle: DeferredSettle,
    ) -> BoxFuture<'_, DeliveryDisposition> {
        Box::pin(async move {
            if self.defer {
                self.started
                    .lock()
                    .unwrap()
                    .push(message.idempotency_key.clone());
                self.deferred.lock().unwrap().push(settle);
                DeliveryDisposition::Deferred
            } else {
                drop(settle);
                DeliveryDisposition::Settle(self.deliver(message).await)
            }
        })
    }
}

fn source(port: u16, queue: &str, max_in_flight: u32, ack_mode: AckMode) -> AmqpTriggerSource {
    let properties = AmqpChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        tls: false,
        tls_settings: Default::default(),
        username: None,
        password: None,
        queue: Some(queue.to_string()),
        topic: None,
        prefetch_count: 10,
        max_in_flight,
        receive_timeout_ms: 250,
        ack_mode,
        singleton: false,
        // The queue exists before the consumer attaches: name it a queue, not an address.
        address_type: Some(AddressType::Queue),
    };
    let mut config = AmqpSourceConfig::new("acme", "acme/orders/1.0.0", "orders-in", properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    AmqpTriggerSource::new(config).expect("source")
}

async fn wait_until<F: Fn() -> bool>(what: &str, timeout: Duration, check: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

// ---- the tests -----------------------------------------------------------------------------

/// With `max-in-flight: 4` and twelve messages queued, exactly four calls run at once
/// while the intake holds them, and all twelve are settled once released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn deliveries_are_processed_concurrently_up_to_max_in_flight() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    let sent = keys("conc", 12);
    send_keys(broker.port, &queue, &sent).await;
    let intake = GatedIntake::gated();
    let source = source(broker.port, &queue, 4, AckMode::OnPersist);
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");

    wait_until("four calls in flight", Duration::from_secs(30), || {
        intake.running() == 4
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(intake.running(), 4, "a full window takes no fifth delivery");
    assert_eq!(intake.started().len(), 4);

    intake.release(sent.len());
    wait_until("all twelve finished", Duration::from_secs(30), || {
        intake.finished.load(Ordering::SeqCst) == 12
    })
    .await;
    assert_eq!(intake.peak.load(Ordering::SeqCst), 4);
    source.stop().await.expect("stop");
    let mut got = intake.started();
    got.sort();
    assert_eq!(got, sent, "each delivery reached the intake once");
    assert!(
        remaining(broker.port, &queue).await.is_empty(),
        "all accepted"
    );
}

/// A delivery whose intake call has not returned is not settled: when the connection
/// drops under it, the broker delivers it again; once the call returns `Ack`, it is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn a_delivery_is_settled_only_after_its_intake_returns() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    send_keys(broker.port, &queue, &["held-1".to_string()]).await;
    let intake = GatedIntake::gated();
    let source = source(broker.port, &queue, 4, AckMode::OnPersist);
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");
    wait_until("the call is in flight", Duration::from_secs(30), || {
        intake.running() == 1
    })
    .await;

    // The call is held: nothing was settled yet. Drop the connection under it.
    on_broker(&broker, Broker::stop).await;
    intake.release(1); // the first call returns into a dead link
    on_broker(&broker, Broker::start_again).await;
    intake.release(1); // the redelivered copy
    wait_until("the delivery came back", Duration::from_secs(60), || {
        intake.started().len() == 2
    })
    .await;
    wait_until("the copy finished", Duration::from_secs(30), || {
        intake.finished.load(Ordering::SeqCst) == 2
    })
    .await;
    source.stop().await.expect("stop");
    assert_eq!(intake.started(), vec!["held-1", "held-1"]);
    assert!(
        remaining(broker.port, &queue).await.is_empty(),
        "accepted after return"
    );
}

/// On stop: a stop waits for the calls in flight and settles each one that finished on the
/// still-open link, so nothing is left on the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn stop_settles_every_finished_delivery() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    let sent = keys("stop", 4);
    send_keys(broker.port, &queue, &sent).await;
    let intake = GatedIntake::gated();
    let source = Arc::new(source(broker.port, &queue, 4, AckMode::OnPersist));
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");
    wait_until("four calls in flight", Duration::from_secs(30), || {
        intake.running() == 4
    })
    .await;

    let stopping = {
        let source = source.clone();
        tokio::spawn(async move { source.stop().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !stopping.is_finished(),
        "the stop waits for the calls in flight"
    );
    intake.release(4);
    stopping.await.expect("join").expect("stop");
    assert_eq!(intake.finished.load(Ordering::SeqCst), 4);
    assert!(
        remaining(broker.port, &queue).await.is_empty(),
        "every finished call was settled before the link closed"
    );
}

/// On connection loss: calls that finish after the connection dropped are not settled; the
/// broker redelivers each of them once, and the copies are settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn a_lost_connection_redelivers_unsettled_work_once() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    let sent = keys("lost", 3);
    send_keys(broker.port, &queue, &sent).await;
    let intake = GatedIntake::gated();
    let source = source(broker.port, &queue, 8, AckMode::OnPersist);
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");
    wait_until("three calls in flight", Duration::from_secs(30), || {
        intake.running() == 3
    })
    .await;

    on_broker(&broker, Broker::stop).await;
    intake.release(3); // they finish during the drain, on a link that is gone
    on_broker(&broker, Broker::start_again).await;
    intake.release(Semaphore::MAX_PERMITS / 2);
    wait_until("each delivery came back", Duration::from_secs(60), || {
        intake.started().len() == 6
    })
    .await;
    wait_until("the copies finished", Duration::from_secs(30), || {
        intake.finished.load(Ordering::SeqCst) == 6
    })
    .await;
    // No third copy turns up.
    tokio::time::sleep(Duration::from_secs(2)).await;
    source.stop().await.expect("stop");
    let counts = intake.count_of();
    assert_eq!(counts.len(), 3);
    assert!(
        counts.values().all(|n| *n == 2),
        "redelivered once: {counts:?}"
    );
    assert!(remaining(broker.port, &queue).await.is_empty());
}

/// Under `on-complete`, deliveries the engine defers leave the window at once, so a
/// window of two still takes six parked deliveries; firing their accepts empties the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn on_complete_deferrals_do_not_consume_the_window() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    let sent = keys("defer", 6);
    send_keys(broker.port, &queue, &sent).await;
    let intake = Arc::new(GatedIntake {
        defer: true,
        ..Default::default()
    });
    let source = source(broker.port, &queue, 2, AckMode::OnComplete);
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");
    wait_until("all six deferred", Duration::from_secs(30), || {
        intake.deferred.lock().unwrap().len() == 6
    })
    .await;

    for settle in intake.deferred.lock().unwrap().iter_mut() {
        (settle.ack)();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    source.stop().await.expect("stop");
    assert!(
        remaining(broker.port, &queue).await.is_empty(),
        "the deferred accepts settled every delivery"
    );
}

/// `max-in-flight: 1` is the serial consumer: one call at a time, in arrival order,
/// however long each takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn max_in_flight_one_processes_in_arrival_order() {
    let broker = start_broker().await;
    let queue = fresh_queue();
    let sent = keys("serial", 20);
    send_keys(broker.port, &queue, &sent).await;
    let intake = Arc::new(GatedIntake {
        jitter: true,
        ..Default::default()
    });
    intake.release(Semaphore::MAX_PERMITS / 2);
    let source = source(broker.port, &queue, 1, AckMode::OnPersist);
    source
        .start(intake.clone(), Arc::new(AlwaysLeading))
        .await
        .expect("start");
    wait_until("all twenty finished", Duration::from_secs(60), || {
        intake.finished.load(Ordering::SeqCst) == 20
    })
    .await;
    source.stop().await.expect("stop");
    assert_eq!(intake.peak.load(Ordering::SeqCst), 1);
    assert_eq!(intake.started(), sent, "arrival order");
}
