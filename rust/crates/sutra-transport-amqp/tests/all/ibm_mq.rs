//! The AMQP 1.0 transport against IBM MQ: two queue managers in IBM MQ Advanced for Developers
//! containers, the first with AMQP channels (plain on 5672, TLS on 5671, both SASL PLAIN), joined
//! to the second by a sender/receiver channel pair over mutual TLS.
//!
//! The image is the published developer image with its AMQP component added back
//! (`tests/ibm-mq/build-image.sh`, tag `$SUTRA_IBM_MQ_IMAGE`, default
//! `sutra-test/ibm-mq-amqp:10.0.1.0`). A missing image is built on first use, which downloads the
//! developer archive from IBM (about 550 MB). Starting the containers accepts the IBM MQ Advanced
//! for Developers licence (`LICENSE=accept`).
//!
//! What a message looks like to an MQ application is read with the queue manager's own sample
//! programs inside the container (`amqsbcg` browses a queue and prints the message descriptor and
//! properties), so the assertions are about what the far side sees, not about what the transport
//! believes it sent.
//!
//! The restart tests stop and start their own queue manager, so each has a container of its own.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use sutra_channels::sink::{MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::source::{AckDecision, InboundIntake, TriggerSource};
use sutra_channels::{BoxFuture, InboundMessage};
use sutra_testkit::broker_tls::{unique_token, TestBrokerPki};
use sutra_transport_amqp::{
    codes, AckMode, AddressType, AmqpChannelProperties, AmqpMessageSink, AmqpSourceConfig,
    AmqpTlsProperties, AmqpTriggerSource, HEADER_CORRELATION_ID, HEADER_MESSAGE_ID,
    HEADER_REPLY_TO,
};

const QM1: &str = "QM1";
const QM2: &str = "QM2";
const USER: &str = "app";

// ---- docker helpers ------------------------------------------------------------------------

fn image() -> String {
    std::env::var("SUTRA_IBM_MQ_IMAGE").unwrap_or_else(|_| "sutra-test/ibm-mq-amqp:10.0.1.0".into())
}

fn docker(args: &[&str]) -> (bool, String) {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("run docker (docker required)");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

fn docker_ok(args: &[&str]) -> String {
    let (ok, text) = docker(args);
    assert!(ok, "docker {args:?} failed: {text}");
    text
}

/// Build the image when it is not there yet (once per test binary).
fn ensure_image() {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let tag = image();
        if docker(&["image", "inspect", &tag]).0 {
            return;
        }
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/ibm-mq/build-image.sh");
        let status = Command::new("bash")
            .arg(&script)
            .env("SUTRA_IBM_MQ_IMAGE", &tag)
            .status()
            .expect("run build-image.sh");
        assert!(
            status.success(),
            "building {tag} with {} failed",
            script.display()
        );
    });
}

/// A loopback port nothing listens on right now, for a fixed host port mapping that survives a
/// container restart.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .unwrap()
        .port()
}

fn make_public(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

/// The queue manager's key repository: a PKCS#12 file holding the server certificate (label `qm`)
/// and the CA that signed it, written with `openssl` to `<dir>/tls/qm.p12` under `password`. The
/// MQSC files point SSLKEYR at it (see qm1.mqsc for why not the image's own /etc/mqm/pki layout).
fn write_key_repository(dir: &Path, pki: &TestBrokerPki, password: &str) {
    let tls = dir.join("tls");
    std::fs::create_dir_all(&tls).unwrap();
    let (cert, key) = (dir.join("server.crt"), dir.join("server.key"));
    std::fs::write(&cert, &pki.server_cert_pem).unwrap();
    std::fs::write(&key, &pki.server_key_pem).unwrap();
    let p12 = tls.join("qm.p12");
    let status = Command::new("openssl")
        .args(["pkcs12", "-export", "-name", "qm", "-in"])
        .arg(&cert)
        .arg("-inkey")
        .arg(&key)
        .arg("-certfile")
        .arg(&pki.ca_file)
        .arg("-out")
        .arg(&p12)
        .arg("-passout")
        .arg(format!("pass:{password}"))
        .status()
        .expect("run openssl (needed to write the queue manager's PKCS#12 key repository)");
    assert!(status.success(), "openssl pkcs12 -export failed");
    make_public(&tls, 0o755);
    // Throwaway key: readable by the container's unprivileged user.
    make_public(&p12, 0o644);
}

/// One queue manager container.
struct QueueManager {
    name: String,
    qm: &'static str,
}

impl QueueManager {
    /// Start `qm` from `mqsc` (applied on every start) with the PKI under `dir`.
    fn start(
        name: &str,
        qm: &'static str,
        network: &str,
        dir: &Path,
        mqsc: &Path,
        password: &str,
        ports: &[(u16, u16)],
    ) -> QueueManager {
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.into(),
            "--network".into(),
            network.into(),
            "-e".into(),
            "LICENSE=accept".into(),
            "-e".into(),
            format!("MQ_QMGR_NAME={qm}"),
            "-e".into(),
            format!("MQ_APP_PASSWORD={password}"),
            "-e".into(),
            format!("MQ_ADMIN_PASSWORD={password}"),
            "-v".into(),
            format!("{}:/etc/mqm/30-sutra.mqsc:ro", mqsc.display()),
            "-v".into(),
            format!("{}:/sutra-tls:ro", dir.join("tls").display()),
        ];
        for (host, container) in ports {
            args.push("-p".into());
            args.push(format!("127.0.0.1:{host}:{container}"));
        }
        args.push(image());
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let id = docker_ok(&refs).trim().to_string();
        sutra_testkit::reap_on_exit(id);
        let manager = QueueManager {
            name: name.to_string(),
            qm,
        };
        manager.wait_started();
        manager
    }

    /// Wait for the queue manager to report itself started (after a `docker start` too: the log
    /// line is counted from `since`).
    fn wait_started(&self) {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let (_, logs) = docker(&["logs", &self.name]);
            let starts = logs.matches("Started queue manager").count();
            let restarts = logs.matches("Starting queue manager").count();
            if starts >= restarts.max(1) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{} did not start: {logs}",
                self.name
            );
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn runmqsc(&self, commands: &str) -> String {
        let script = format!("echo \"{commands}\" | runmqsc {}", self.qm);
        docker(&["exec", &self.name, "bash", "-c", &script]).1
    }

    /// Start the named AMQP channels, retrying while the AMQP service is still coming up.
    fn start_amqp_channels(&self, channels: &[&str]) {
        for channel in channels {
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                let out = self.runmqsc(&format!(
                    "START CHANNEL('{channel}')\nDISPLAY CHSTATUS('{channel}') CHLTYPE(AMQP)"
                ));
                // Started, and listening: the status says RUNNING once the port is open.
                if out.contains("STATUS(RUNNING)") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "AMQP channel {channel} did not start: {out}"
                );
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    /// Browse a queue with the queue manager's sample program: descriptor, properties and body
    /// of every message, without removing them.
    fn browse(&self, queue: &str) -> String {
        docker(&[
            "exec",
            &self.name,
            "/opt/mqm/samp/bin/amqsbcg",
            queue,
            self.qm,
        ])
        .1
    }

    fn depth(&self, queue: &str) -> u32 {
        let out = self.runmqsc(&format!("DISPLAY QLOCAL('{queue}') CURDEPTH"));
        out.split("CURDEPTH(")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or_else(|| panic!("no CURDEPTH for {queue}: {out}"))
    }

    fn clear(&self, queue: &str) {
        self.runmqsc(&format!("CLEAR QLOCAL('{queue}')"));
    }

    /// Wait until `queue` holds `n` messages.
    fn wait_depth(&self, queue: &str, n: u32) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let depth = self.depth(queue);
            if depth >= n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{queue} on {} holds {depth} messages, expected {n}",
                self.qm
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

struct Pair {
    qm1: QueueManager,
    qm2: QueueManager,
    plain_port: u16,
    tls_port: u16,
    password: String,
    pki: TestBrokerPki,
}

fn template(dir: &Path, source: &str, replacements: &[(&str, &str)]) -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/ibm-mq")
        .join(source);
    let mut text = std::fs::read_to_string(&path).expect("read mqsc");
    for (from, to) in replacements {
        text = text.replace(from, to);
    }
    let out = dir.join(source);
    std::fs::write(&out, text).unwrap();
    make_public(&out, 0o644);
    out
}

/// The PKI's directory, with the key repository written into it under `keyrpwd`.
fn fixture_dir(pki: &TestBrokerPki, keyrpwd: &str) -> PathBuf {
    let dir = pki.ca_file.parent().expect("pki dir").to_path_buf();
    write_key_repository(&dir, pki, keyrpwd);
    dir
}

/// The two queue managers, shared by every test but the restart one.
fn pair() -> &'static Pair {
    static PAIR: OnceLock<Pair> = OnceLock::new();
    PAIR.get_or_init(|| {
        ensure_image();
        let token = unique_token("mq");
        let network = format!("sutra-mq-{token}");
        docker_ok(&["network", "create", &network]);
        sutra_testkit::reap_network_on_exit(network.clone());
        let qm1_name = format!("sutra-mq-qm1-{token}");
        let qm2_name = format!("sutra-mq-qm2-{token}");
        let pki = TestBrokerPki::generate(&["localhost", "127.0.0.1", &qm1_name, &qm2_name]);
        let keyrpwd = unique_token("kr");
        let dir = fixture_dir(&pki, &keyrpwd);
        let password = unique_token("pw");
        let qm2_mqsc = template(&dir, "qm2.mqsc", &[("@KEYRPWD@", &keyrpwd)]);
        let qm2 = QueueManager::start(&qm2_name, QM2, &network, &dir, &qm2_mqsc, &password, &[]);
        let qm1_mqsc = template(
            &dir,
            "qm1.mqsc",
            &[
                ("qm2(1414)", &format!("{qm2_name}(1414)")),
                ("@KEYRPWD@", &keyrpwd),
            ],
        );
        let (plain_port, tls_port) = (free_port(), free_port());
        let qm1 = QueueManager::start(
            &qm1_name,
            QM1,
            &network,
            &dir,
            &qm1_mqsc,
            &password,
            &[(plain_port, 5672), (tls_port, 5671)],
        );
        qm1.start_amqp_channels(&["SUTRA.AMQP", "SUTRA.AMQPS"]);
        Pair {
            qm1,
            qm2,
            plain_port,
            tls_port,
            password,
            pki,
        }
    })
}

// ---- engine-side doubles -------------------------------------------------------------------

#[derive(Default)]
struct CollectingIntake {
    delivered: Mutex<Vec<InboundMessage>>,
}

impl CollectingIntake {
    fn keys(&self) -> BTreeSet<String> {
        self.delivered
            .lock()
            .unwrap()
            .iter()
            .map(|m| m.idempotency_key.clone())
            .collect()
    }

    fn find(&self, key: &str) -> Option<InboundMessage> {
        self.delivered
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.idempotency_key == key)
            .cloned()
    }

    fn len(&self) -> usize {
        self.delivered.lock().unwrap().len()
    }
}

impl InboundIntake for CollectingIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            self.delivered.lock().unwrap().push(message);
            AckDecision::Ack
        })
    }
}

fn source_properties(port: u16, password: &str, queue: &str) -> AmqpChannelProperties {
    AmqpChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        tls: false,
        tls_settings: AmqpTlsProperties::default(),
        username: Some(USER.to_string()),
        password: Some(password.to_string()),
        queue: Some(queue.to_string()),
        topic: None,
        prefetch_count: 10,
        max_in_flight: sutra_transport_amqp::DEFAULT_MAX_IN_FLIGHT,
        receive_timeout_ms: 250,
        ack_mode: AckMode::OnPersist,
        singleton: false,
        address_type: Some(AddressType::Queue),
    }
}

async fn start_source(
    channel: &str,
    properties: AmqpChannelProperties,
) -> (AmqpTriggerSource, Arc<CollectingIntake>) {
    let mut config = AmqpSourceConfig::new("acme", "acme/mq/1.0.0", channel, properties);
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(250);
    config.reconnect_max = Duration::from_secs(2);
    config.operation_timeout = Duration::from_secs(10);
    let source = AmqpTriggerSource::new(config).expect("source");
    let intake = Arc::new(CollectingIntake::default());
    source
        .start(intake.clone(), Arc::new(sutra_channels::AlwaysLeading))
        .await
        .expect("start");
    (source, intake)
}

fn sink(password: &str) -> AmqpMessageSink {
    AmqpMessageSink::new(Some(USER.to_string()), Some(password.to_string()))
        .with_operation_timeout(Duration::from_secs(10))
}

fn message(destination: String, key: &str, body: &[u8]) -> OutboundMessage {
    OutboundMessage {
        destination,
        headers: BTreeMap::new(),
        body: body.to_vec(),
        content_type: Some("application/xml".to_string()),
        outbox_key: key.to_string(),
        traceparent: None,
    }
}

async fn wait_for<F: Fn() -> bool>(what: &str, timeout: Duration, check: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn expect_delivered(outcome: SendOutcome) {
    match outcome {
        SendOutcome::Delivered => {}
        other => panic!("expected Delivered, got {other:?}"),
    }
}

/// The query a queue destination on MQ needs: the queue capability. (Property names are left
/// verbatim here; the descriptor and remote-queue tests send identifier names.)
const MQ_QUEUE: &str = "type=queue";

/// One field of an `amqsbcg` message descriptor: the quoted value without its quotes and
/// padding, or the bare word.
fn md_field(descriptor: &str, name: &str) -> String {
    let at = descriptor
        .find(&format!("{name} "))
        .unwrap_or_else(|| panic!("no {name} in {descriptor}"));
    let rest = descriptor[at + name.len()..].trim_start();
    let rest = rest
        .strip_prefix(':')
        .expect("':' after the field name")
        .trim_start();
    match rest.strip_prefix('\'') {
        Some(quoted) => quoted.split('\'').next().unwrap().trim_end().to_string(),
        None => rest
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string(),
    }
}

/// The printable text of the message bodies `amqsbcg` dumped (its right-hand column, 16 bytes a
/// line, other bytes as `.`), joined up again.
fn dumped_text(browsed: &str) -> String {
    browsed
        .lines()
        .filter(|l| l.len() > 10 && l.as_bytes()[8] == b':')
        .filter_map(|l| {
            let open = l.find('\'')?;
            let close = l.rfind('\'')?;
            (close > open).then(|| l[open + 1..close].to_string())
        })
        .collect()
}

/// The descriptors `amqsbcg` printed, one per message.
fn descriptors(browsed: &str) -> Vec<&str> {
    browsed
        .split("****Message descriptor****")
        .skip(1)
        .collect()
}

// ---- point to point ------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_put_on_a_local_queue_is_got_by_the_source() {
    let p = pair();
    let (source, intake) = start_source(
        "p2p-in",
        source_properties(p.plain_port, &p.password, "APP.P2P"),
    )
    .await;
    let sink = sink(&p.password);
    let key = unique_token("p2p");
    expect_delivered(
        sink.send(&message(
            format!("amqp10://127.0.0.1:{}/APP.P2P?{MQ_QUEUE}", p.plain_port),
            &key,
            b"<Document>p2p</Document>",
        ))
        .await,
    );
    wait_for("the p2p delivery", Duration::from_secs(30), || {
        intake.find(&key).is_some()
    })
    .await;
    let got = intake.find(&key).unwrap();
    assert!(got.explicit_event_id);
    assert_eq!(got.body.into_inner(), b"<Document>p2p</Document>");
    assert_eq!(got.content_type.as_deref(), Some("application/xml"));
    assert_eq!(
        got.headers.get(HEADER_MESSAGE_ID).map(String::as_str),
        Some(key.as_str()),
        "the AMQP message id survives MQ"
    );
    source.stop().await.unwrap();
    assert_eq!(
        p.qm1.depth("APP.P2P"),
        0,
        "the source took the message off the queue"
    );
}

/// Without the queue capability MQ reads the address as a topic string, not as the queue of that
/// name.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_queue_type_mq_reads_the_address_as_a_topic() {
    let p = pair();
    let sink = sink(&p.password);
    p.qm1.clear("APP.UNTYPED");
    // The `app` user may publish only under the APP.NOTIFY topic object, so MQ refuses the
    // publication, and says what it took the address for.
    let outcome = sink
        .send(&message(
            format!("amqp10://127.0.0.1:{}/APP.UNTYPED", p.plain_port),
            &unique_token("topic"),
            b"x",
        ))
        .await;
    match outcome {
        SendOutcome::RetryableFailure(d) => {
            assert!(
                d.message.contains("publish to topic 'APP.UNTYPED'"),
                "{}",
                d.message
            );
            record("a send to a queue name without type=queue", &d.message);
        }
        other => panic!("expected MQ to take the address for a topic, got {other:?}"),
    }
    assert_eq!(p.qm1.depth("APP.UNTYPED"), 0);
}

// ---- what MQ applications see --------------------------------------------------------------

/// APP.MAPPING has PROPCTL(NONE), as a queue read by an application that wants no MQRFH2 would:
/// the descriptor is what such a reader sees.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_message_descriptor_carries_the_send_options() {
    let p = pair();
    let sink = sink(&p.password);
    p.qm1.clear("APP.MAPPING");
    p.qm1.clear("APP.P2P.PROPS");
    let key = unique_token("md");
    let text = "<Nm>Ünïcødé ஆ ع</Nm>";
    let mut m = message(
        format!(
            "amqp10://127.0.0.1:{}/APP.MAPPING?type=queue&property-names=identifier&durable=true&ttl-ms=600000&body=text",
            p.plain_port
        ),
        &key,
        text.as_bytes(),
    );
    m.headers
        .insert(HEADER_CORRELATION_ID.to_string(), "CORR-0001".to_string());
    m.headers
        .insert(HEADER_REPLY_TO.to_string(), "APP.REPLIES".to_string());
    expect_delivered(sink.send(&m).await);
    // The default projection (a `data` section, no header), then the payload as AMQP binary.
    for (extra, key) in [("", "md-default"), ("&body=binary", "md-binary")] {
        expect_delivered(
            sink.send(&message(
                format!(
                    "amqp10://127.0.0.1:{}/APP.MAPPING?{MQ_QUEUE}{extra}",
                    p.plain_port
                ),
                &unique_token(key),
                b"<a/>",
            ))
            .await,
        );
    }
    p.qm1.wait_depth("APP.MAPPING", 3);
    let browsed = p.qm1.browse("APP.MAPPING");
    record(
        "APP.MAPPING (PROPCTL(NONE)): a text, durable message with ttl and ids; the default projection; body=binary",
        &browsed,
    );
    let all = descriptors(&browsed);
    assert_eq!(all.len(), 3, "{browsed}");
    let first = all[0];
    assert_eq!(md_field(first, "Format"), "MQSTR", "{first}");
    assert_eq!(md_field(first, "CodedCharSetId"), "1208");
    assert_eq!(md_field(first, "Persistence"), "1");
    assert_eq!(md_field(first, "MsgType"), "8");
    assert_eq!(md_field(first, "ReplyToQ"), "APP.REPLIES");
    assert_eq!(md_field(first, "ReplyToQMgr"), "QM1");
    // CORR-0001, padded with zero bytes to 24.
    assert!(
        first.contains("CorrelId : X'434F52522D3030303100000000000000"),
        "{first}"
    );
    assert!(
        !first.contains("RFH "),
        "no MQRFH2 under PROPCTL(NONE): {first}"
    );
    let expiry: i64 = md_field(first, "Expiry").parse().expect("Expiry");
    assert!(
        (5900..=6000).contains(&expiry),
        "expiry in tenths of a second: {expiry}"
    );
    // The UTF-8 text is in the message as sent.
    let hex: String = text.bytes().take(8).map(|b| format!("{b:02X}")).collect();
    let dump: String = first
        .lines()
        .filter_map(|l| l.split_once(":  ").map(|(_, r)| r))
        .flat_map(|r| r.split("  ").next().unwrap_or_default().split(' '))
        .collect();
    assert!(dump.contains(&hex), "{hex} in {dump}");
    // A `data` section is kept as an embedded AMQP message; AMQP binary is a plain MQ message.
    let second = all[1];
    assert_eq!(md_field(second, "Format"), "MQAMQP", "{second}");
    assert_eq!(md_field(second, "Persistence"), "0");
    assert_eq!(md_field(second, "Expiry"), "-1");
    let third = all[2];
    assert_eq!(md_field(third, "Format"), "", "{third}");
    assert!(dumped_text(third).contains("<a/>"), "{third}");

    // On a queue with the default PROPCTL the properties are there, as an MQRFH2 to a reader
    // that does not ask for them as properties; verbatim names are kept as sent.
    expect_delivered(
        sink.send(&message(
            format!(
                "amqp10://127.0.0.1:{}/APP.P2P.PROPS?{MQ_QUEUE}&body=text",
                p.plain_port
            ),
            &unique_token("props"),
            b"<a/>",
        ))
        .await,
    );
    p.qm1.wait_depth("APP.P2P.PROPS", 1);
    let browsed = p.qm1.browse("APP.P2P.PROPS");
    record(
        "APP.P2P.PROPS (PROPCTL(COMPAT)): the same with verbatim property names",
        &browsed,
    );
    let d = descriptors(&browsed)[0];
    assert_eq!(md_field(d, "Format"), "MQHRF2", "{d}");
    assert!(dumped_text(d).contains("<usr>"), "{d}");
}

/// A message an MQ application put (as the far side would) reaches the engine as text, with its
/// MQ ids and reply queue as headers.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_natively_put_message_reaches_the_source_with_its_ids() {
    let p = pair();
    let (source, intake) = start_source(
        "native-in",
        source_properties(p.plain_port, &p.password, "APP.INBOUND"),
    )
    .await;
    // amqsreq puts each input line as a request (Format MQSTR) whose ReplyToQ is a dynamic queue
    // made from the model queue, then waits for replies; the timeout ends that wait.
    let script = "printf '<Document>native</Document>\\n\\n' | timeout 5 /opt/mqm/samp/bin/amqsreq APP.INBOUND QM1 SYSTEM.DEFAULT.MODEL.QUEUE || true";
    docker(&["exec", &p.qm1.name, "bash", "-c", script]);
    wait_for("the native delivery", Duration::from_secs(30), || {
        intake.len() >= 1
    })
    .await;
    let got = intake.delivered.lock().unwrap()[0].clone();
    record(
        "headers of a natively put request message",
        &format!("{:#?}", got.headers),
    );
    assert_eq!(
        got.body.clone().into_inner(),
        b"<Document>native</Document>"
    );
    let reply_to = got.headers.get(HEADER_REPLY_TO).expect("reply-to");
    assert!(
        reply_to.starts_with("AMQ.") && !reply_to.ends_with(' '),
        "{reply_to:?}"
    );
    let id = got.headers.get(HEADER_MESSAGE_ID).expect("message id");
    assert_eq!(id.len(), 48, "a 24-byte MQ MsgId as hex: {id}");
    assert!(
        id.starts_with("414d5120514d3120"),
        "starts with 'AMQ QM1 ': {id}"
    );
    assert_eq!(&got.idempotency_key, id);
    assert!(
        !got.headers.contains_key(HEADER_CORRELATION_ID),
        "MQCI_NONE is no correlation id"
    );
    source.stop().await.unwrap();
}

// ---- remote queue, topic -------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_to_a_remote_queue_definition_crosses_the_mtls_channel() {
    let p = pair();
    let sink = sink(&p.password);
    p.qm2.clear("QM2.IN");
    // First with application properties, then with none. QM2.IN keeps the default PROPCTL(COMPAT),
    // as a queue on someone else's queue manager would.
    for (extra, body) in [
        ("&property-names=identifier", "<Document>with</Document>"),
        (
            "&application-properties=none",
            "<Document>without</Document>",
        ),
    ] {
        expect_delivered(
            sink.send(&message(
                format!(
                    "amqp10://127.0.0.1:{}/APP.TO.QM2?{MQ_QUEUE}&body=text{extra}",
                    p.plain_port
                ),
                &unique_token("remote"),
                body.as_bytes(),
            ))
            .await,
        );
    }
    p.qm2.wait_depth("QM2.IN", 2);
    let browsed = p.qm2.browse("QM2.IN");
    record(
        "on the far queue manager, read from a PROPCTL(COMPAT) queue: with and without application properties",
        &browsed,
    );
    let all = descriptors(&browsed);
    assert_eq!(all.len(), 2, "{browsed}");
    // Application properties (the usr folder) reach a reader that does not ask for properties as
    // an MQRFH2 in front of the body.
    assert_eq!(md_field(all[0], "Format"), "MQHRF2", "{}", all[0]);
    assert!(
        dumped_text(all[0]).contains("sutra_outbox_key"),
        "{}",
        all[0]
    );
    assert!(dumped_text(all[0]).contains("<Document>with</Document>"));
    // Without them the AMQP channel's own properties are not handed to such a reader: a plain
    // MQSTR message in CCSID 1208.
    let d = all[1];
    assert_eq!(md_field(d, "Format"), "MQSTR", "{d}");
    assert_eq!(md_field(d, "CodedCharSetId"), "1208");
    assert!(!dumped_text(d).contains("RFH "), "{d}");
    assert!(
        dumped_text(d).contains("<Document>without</Document>"),
        "{d}"
    );
    let status = p
        .qm1
        .runmqsc("DISPLAY CHSTATUS('QM1.TO.QM2') STATUS SSLCIPH");
    record("the sender channel", &status);
    assert!(status.contains("STATUS(RUNNING)"), "{status}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publication_reaches_both_administrative_subscriptions() {
    let p = pair();
    let (source, intake) = start_source(
        "notify-in",
        source_properties(p.plain_port, &p.password, "APP.NOTIFY.LOCAL"),
    )
    .await;
    p.qm2.clear("QM2.NOTIFY");
    let sink = sink(&p.password);
    let key = unique_token("pub");
    expect_delivered(
        sink.send(&message(
            format!(
                "amqp10://127.0.0.1:{}/app/notify?type=topic&property-names=identifier&body=text",
                p.plain_port
            ),
            &key,
            b"<Notification/>",
        ))
        .await,
    );
    wait_for(
        "the local subscription's copy",
        Duration::from_secs(30),
        || intake.find(&key).is_some(),
    )
    .await;
    assert_eq!(
        intake.find(&key).unwrap().body.into_inner(),
        b"<Notification/>"
    );
    p.qm2.wait_depth("QM2.NOTIFY", 1);
    let browsed = p.qm2.browse("QM2.NOTIFY");
    record(
        "the remote subscription's copy on the far queue manager",
        &browsed,
    );
    assert!(
        dumped_text(&browsed).contains("<Notification/>"),
        "{browsed}"
    );
    source.stop().await.unwrap();
}

// ---- TLS, SASL -----------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_and_sasl_plain_on_the_amqp_channel() {
    let p = pair();
    let trust = AmqpTlsProperties {
        ca_file: Some(p.pki.ca_file.display().to_string()),
        ..AmqpTlsProperties::default()
    };
    let mut properties = source_properties(p.tls_port, &p.password, "APP.P2P");
    properties.host = "localhost".to_string();
    properties.tls = true;
    properties.tls_settings = trust.clone();
    let (source, intake) = start_source("tls-in", properties).await;
    let tls_sink = sink(&p.password).with_tls_settings(Ok(trust.clone()));
    let key = unique_token("tls");
    expect_delivered(
        tls_sink
            .send(&message(
                format!("amqp10s://localhost:{}/APP.P2P?{MQ_QUEUE}", p.tls_port),
                &key,
                b"<Document>tls</Document>",
            ))
            .await,
    );
    wait_for("the TLS delivery", Duration::from_secs(30), || {
        intake.find(&key).is_some()
    })
    .await;
    source.stop().await.unwrap();

    // A wrong password is refused by the queue manager's connection authentication.
    let wrong = AmqpMessageSink::new(Some(USER.to_string()), Some("not-the-password".to_string()))
        .with_tls_settings(Ok(trust));
    match wrong
        .send(&message(
            format!("amqp10s://localhost:{}/APP.P2P?{MQ_QUEUE}", p.tls_port),
            "k",
            b"x",
        ))
        .await
    {
        SendOutcome::RetryableFailure(d) => {
            assert!(!d.message.contains("not-the-password"), "{}", d.message);
            record(
                "a wrong password on the AMQP channel",
                &format!("{}: {}", d.code, d.message),
            );
        }
        other => panic!("expected the login to be refused, got {other:?}"),
    }
    // A CA that did not sign the queue manager's certificate is refused by the client.
    let foreign = sink(&p.password).with_tls_settings(Ok(AmqpTlsProperties {
        ca_file: Some(p.pki.foreign_ca_file.display().to_string()),
        ..AmqpTlsProperties::default()
    }));
    match foreign
        .send(&message(
            format!("amqp10s://localhost:{}/APP.P2P?{MQ_QUEUE}", p.tls_port),
            "k",
            b"x",
        ))
        .await
    {
        SendOutcome::RetryableFailure(d) => {
            assert_eq!(d.code, codes::OUTBOUND_BROKER_TLS_FAILED, "{}", d.message)
        }
        other => panic!("expected an untrusted certificate to be refused, got {other:?}"),
    }
}

// ---- concurrency ---------------------------------------------------------------------------

/// The AMQP channel allows one session per connection; the sink pools its connections, each with
/// its one session and a sender link per queue, so 40 concurrent sends share at most the pool's
/// bound of connections, and every message arrives.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sends_share_the_pooled_connections_each_with_one_session() {
    let p = pair();
    let (source, intake) = start_source(
        "concurrent-in",
        source_properties(p.plain_port, &p.password, "APP.CONCURRENT"),
    )
    .await;
    let sink = Arc::new(sink(&p.password));
    let keys: Vec<String> = (0..40).map(|i| unique_token(&format!("c{i}-"))).collect();
    let sends: Vec<_> = keys
        .iter()
        .map(|key| {
            let sink = sink.clone();
            let m = message(
                format!(
                    "amqp10://127.0.0.1:{}/APP.CONCURRENT?{MQ_QUEUE}",
                    p.plain_port
                ),
                key,
                key.as_bytes(),
            );
            tokio::spawn(async move { sink.send(&m).await })
        })
        .collect();
    for send in sends {
        expect_delivered(send.await.unwrap());
    }
    assert!(
        sink.connections_opened() <= sutra_transport_amqp::sink::DEFAULT_MAX_CONNECTIONS as u64,
        "{} connections for 40 sends",
        sink.connections_opened()
    );
    wait_for("all concurrent deliveries", Duration::from_secs(60), || {
        keys.iter().all(|k| intake.keys().contains(k))
    })
    .await;
    source.stop().await.unwrap();
}

// ---- restart -------------------------------------------------------------------------------

/// Records what the queue managers showed, for the findings write-up (`SUTRA_MQ_RECORD=<file>`).
fn record(what: &str, text: &str) {
    if let Ok(path) = std::env::var("SUTRA_MQ_RECORD") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("record file");
        let _ = writeln!(file, "\n===== {what}\n{text}");
    }
}

/// Log lines, captured for the outage-window assertions.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured_log() -> &'static CapturedLog {
    static LOG: OnceLock<CapturedLog> = OnceLock::new();
    LOG.get_or_init(|| {
        let log = CapturedLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        log
    })
}

/// TX-02: the queue manager stops and starts again. The source reconnects by itself and logs the
/// outage window; sends during the outage fail and are sent again (as the outbox does) until the
/// queue manager is back; nothing is lost. Persistent messages waiting on a queue survive the
/// restart, and so do non-persistent ones on a queue with `NPMCLASS(HIGH)`; non-persistent ones
/// on an ordinary queue do not, which is MQ's own rule.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_source_and_sink_ride_out_a_queue_manager_restart_and_log_the_outage() {
    let log = captured_log();
    ensure_image();
    let token = unique_token("mqr");
    let network = format!("sutra-mq-{token}");
    docker_ok(&["network", "create", &network]);
    let _network = sutra_testkit::NetworkGuard::new(network.clone());
    let name = format!("sutra-mq-restart-{token}");
    let pki = TestBrokerPki::generate(&["localhost", "127.0.0.1"]);
    let keyrpwd = unique_token("kr");
    let dir = fixture_dir(&pki, &keyrpwd);
    let password = unique_token("pw");
    let mqsc = template(&dir, "qm1.mqsc", &[("@KEYRPWD@", &keyrpwd)]);
    let port = free_port();
    let qm = QueueManager::start(
        &name,
        QM1,
        &network,
        &dir,
        &mqsc,
        &password,
        &[(port, 5672)],
    );
    qm.runmqsc(
        "DEFINE QLOCAL('APP.NP') REPLACE\nDEFINE QLOCAL('APP.NP.HIGH') NPMCLASS(HIGH) REPLACE",
    );
    qm.start_amqp_channels(&["SUTRA.AMQP"]);

    let channel = format!("restart-in-{token}");
    let (source, intake) =
        start_source(&channel, source_properties(port, &password, "APP.P2P")).await;
    let sink = Arc::new(sink(&password));
    let to =
        |queue: &str, extra: &str| format!("amqp10://127.0.0.1:{port}/{queue}?{MQ_QUEUE}{extra}");

    // Before: delivered and consumed.
    let before: Vec<String> = (0..5)
        .map(|i| unique_token(&format!("before{i}-")))
        .collect();
    for key in &before {
        expect_delivered(sink.send(&message(to("APP.P2P", ""), key, b"before")).await);
    }
    wait_for(
        "the messages sent before the outage",
        Duration::from_secs(30),
        || before.iter().all(|k| intake.keys().contains(k)),
    )
    .await;
    // Waiting on queues nobody reads: persistent, non-persistent, non-persistent with NPMCLASS(HIGH).
    for (queue, extra) in [
        ("APP.DURABLE", "&durable=true"),
        ("APP.NP", ""),
        ("APP.NP.HIGH", ""),
    ] {
        for i in 0..3 {
            expect_delivered(
                sink.send(&message(
                    to(queue, extra),
                    &unique_token(&format!("wait{i}-")),
                    b"w",
                ))
                .await,
            );
        }
        qm.wait_depth(queue, 3);
    }

    // The outage: stop the queue manager; send while it is down, retrying as the outbox does.
    let stopped_at = time::OffsetDateTime::now_utc();
    docker_ok(&["stop", "-t", "60", &name]);
    let during: Vec<String> = (0..5)
        .map(|i| unique_token(&format!("during{i}-")))
        .collect();
    let retries: Vec<_> = during
        .iter()
        .map(|key| {
            let sink = sink.clone();
            let m = message(to("APP.P2P", "&durable=true"), key, b"during");
            tokio::spawn(async move {
                let mut attempts = 0u32;
                loop {
                    attempts += 1;
                    match sink.send(&m).await {
                        SendOutcome::Delivered => return attempts,
                        SendOutcome::PermanentFailure(d) => panic!("permanent failure: {d:?}"),
                        SendOutcome::RetryableFailure(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await
                        }
                    }
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(5)).await;
    docker_ok(&["start", &name]);
    qm.wait_started();
    // No START CHANNEL here: an AMQP channel that was running when the queue manager stopped is
    // started again with it.
    for retry in retries {
        let attempts = retry.await.unwrap();
        assert!(
            attempts > 1,
            "a send during the outage failed at least once"
        );
    }
    wait_for(
        "the messages sent during the outage",
        Duration::from_secs(60),
        || during.iter().all(|k| intake.keys().contains(k)),
    )
    .await;
    assert!(
        before
            .iter()
            .chain(&during)
            .all(|k| intake.keys().contains(k)),
        "nothing lost"
    );

    // After the restart: persistent and NPMCLASS(HIGH) messages kept, plain non-persistent lost.
    let depths = (
        qm.depth("APP.DURABLE"),
        qm.depth("APP.NP"),
        qm.depth("APP.NP.HIGH"),
    );
    record(
        "queue depths after the restart (durable, non-persistent, NPMCLASS(HIGH))",
        &format!("{depths:?}"),
    );
    assert_eq!(depths, (3, 0, 3));

    // The outage window in the log, both ways.
    let text = String::from_utf8_lossy(&log.0.lock().unwrap()).into_owned();
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(&channel) || l.contains("OUTBOUND.AMQP.OUTAGE"))
        .collect();
    record("outage log lines", &lines.join("\n"));
    let started = lines
        .iter()
        .rev()
        .find(|l| l.contains(codes::INBOUND_BROKER_OUTAGE_STARTED))
        .expect("the source logged the outage start");
    // The last window: the consumer may also have retried once while the channel came up.
    let ended = lines
        .iter()
        .rev()
        .find(|l| l.contains(codes::INBOUND_BROKER_OUTAGE_ENDED))
        .expect("the source logged the outage end");
    assert!(
        ended.contains("outage_started=")
            && ended.contains("outage_ended=")
            && ended.contains("outage_ms="),
        "{ended}"
    );
    assert!(started.contains("outage_started="), "{started}");
    let outage_started = field(ended, "outage_started");
    let at = time::OffsetDateTime::parse(
        &outage_started,
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert!(
        at >= stopped_at - time::Duration::seconds(1),
        "the outage starts at the stop: {at} vs {stopped_at}"
    );
    let ms: u64 = field(ended, "outage_ms").parse().unwrap();
    assert!(ms >= 5_000, "the queue manager was away at least 5 s: {ms}");
    assert!(
        lines
            .iter()
            .any(|l| l.contains(codes::OUTBOUND_BROKER_OUTAGE_ENDED)),
        "the sink logged its window"
    );

    source.stop().await.unwrap();
    docker(&["rm", "-f", &name]);
}

/// A TCP relay in front of the queue manager's AMQP port that can cut the connections it carries
/// and, while holding, accept new ones without ever answering them. That is how a restarting
/// queue manager can look from the client side: the address takes the connection, then nothing
/// comes back.
struct HoldingRelay {
    port: u16,
    holding: Arc<std::sync::atomic::AtomicBool>,
    cut: tokio::sync::watch::Sender<u64>,
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl HoldingRelay {
    async fn start(target: u16) -> HoldingRelay {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let holding = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicUsize::new(0));
        let (cut, _) = tokio::sync::watch::channel(0u64);
        let task = {
            let holding = holding.clone();
            let accepted = accepted.clone();
            let cut = cut.clone();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((mut client, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    if holding.load(Ordering::SeqCst) {
                        held.push(client); // accepted, never answered, never closed
                        continue;
                    }
                    let mut cut_rx = cut.subscribe();
                    tokio::spawn(async move {
                        let Ok(mut upstream) =
                            tokio::net::TcpStream::connect(("127.0.0.1", target)).await
                        else {
                            return; // the queue manager is down: the client sees the close
                        };
                        tokio::select! {
                            _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {}
                            _ = cut_rx.changed() => {}
                        }
                    });
                }
            })
        };
        HoldingRelay {
            port,
            holding,
            cut,
            accepted,
            task,
        }
    }

    /// Drop every relayed connection and hold every new one.
    fn cut_and_hold(&self) {
        self.holding
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.cut.send_modify(|generation| *generation += 1);
    }

    /// Relay new connections again (the held ones stay held).
    fn relay(&self) {
        self.holding
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn accepted(&self) -> usize {
        self.accepted.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for HoldingRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The queue manager restarts, and while it is away the consumer's reconnect lands on an address
/// that takes the TCP connection and never answers the AMQP open. The consumer gives that attempt
/// up at its operation timeout, keeps retrying, attaches once the queue manager is back, logs the
/// outage end with its start, end and length, and receives again.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_source_reconnects_after_a_restart_whose_listener_does_not_answer() {
    let log = captured_log();
    ensure_image();
    let token = unique_token("mqh");
    let network = format!("sutra-mq-{token}");
    docker_ok(&["network", "create", &network]);
    let _network = sutra_testkit::NetworkGuard::new(network.clone());
    let name = format!("sutra-mq-hold-{token}");
    let pki = TestBrokerPki::generate(&["localhost", "127.0.0.1"]);
    let keyrpwd = unique_token("kr");
    let dir = fixture_dir(&pki, &keyrpwd);
    let password = unique_token("pw");
    let mqsc = template(&dir, "qm1.mqsc", &[("@KEYRPWD@", &keyrpwd)]);
    let port = free_port();
    let qm = QueueManager::start(
        &name,
        QM1,
        &network,
        &dir,
        &mqsc,
        &password,
        &[(port, 5672)],
    );
    qm.start_amqp_channels(&["SUTRA.AMQP"]);
    let relay = HoldingRelay::start(port).await;

    let channel = format!("hold-in-{token}");
    let (source, intake) = start_source(
        &channel,
        source_properties(relay.port, &password, "APP.P2P"),
    )
    .await;
    let sink = sink(&password);
    let to = format!("amqp10://127.0.0.1:{port}/APP.P2P?{MQ_QUEUE}&durable=true");
    let before = unique_token("before-");
    expect_delivered(sink.send(&message(to.clone(), &before, b"before")).await);
    wait_for(
        "the message sent before the outage",
        Duration::from_secs(30),
        || intake.keys().contains(&before),
    )
    .await;

    // The restart: the consumer's connection drops, its reconnects are taken and never answered.
    let cut_at = time::OffsetDateTime::now_utc();
    relay.cut_and_hold();
    let held_from = relay.accepted();
    docker_ok(&["stop", "-t", "60", &name]);
    // The consumer's backoff can miss a fast restart, so keep the queue manager away until it has
    // attempted a reconnect into the held address.
    wait_for(
        "the consumer to reconnect into the held address while the queue manager was away",
        Duration::from_secs(60),
        || relay.accepted() > held_from,
    )
    .await;
    docker_ok(&["start", &name]);
    qm.wait_started();
    // The queue manager is back and reachable again.
    let back_at = Instant::now();
    relay.relay();
    let after = unique_token("after-");
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match sink.send(&message(to.clone(), &after, b"after")).await {
            SendOutcome::Delivered => break,
            SendOutcome::PermanentFailure(d) => panic!("permanent failure: {d:?}"),
            SendOutcome::RetryableFailure(_) => {
                assert!(attempts < 120, "the queue manager accepts sends again");
                tokio::time::sleep(Duration::from_millis(500)).await
            }
        }
    }
    let ended_line = || {
        let text = String::from_utf8_lossy(&log.0.lock().unwrap()).into_owned();
        text.lines()
            .filter(|l| l.contains(&channel))
            .find(|l| l.contains(codes::INBOUND_BROKER_OUTAGE_ENDED))
            .map(str::to_string)
    };
    wait_for(
        "the outage end, within 60 s of the queue manager being back",
        Duration::from_secs(60),
        || ended_line().is_some(),
    )
    .await;
    record(
        "held-listener restart: seconds from back to OUTAGE_ENDED",
        &format!("{:?}", back_at.elapsed()),
    );
    wait_for(
        "the message sent after the restart",
        Duration::from_secs(30),
        || intake.keys().contains(&after),
    )
    .await;

    let text = String::from_utf8_lossy(&log.0.lock().unwrap()).into_owned();
    let lines: Vec<&str> = text.lines().filter(|l| l.contains(&channel)).collect();
    record("held-listener restart log lines", &lines.join("\n"));
    let started = lines
        .iter()
        .find(|l| l.contains(codes::INBOUND_BROKER_OUTAGE_STARTED))
        .expect("the source logged the outage start");
    assert!(started.contains("outage_started="), "{started}");
    assert!(
        lines.iter().any(|l| l.contains("got no answer")),
        "the held attempt failed at the operation timeout: {lines:#?}"
    );
    let ended = ended_line().unwrap();
    let outage_started = time::OffsetDateTime::parse(
        &field(&ended, "outage_started"),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert!(
        outage_started >= cut_at - time::Duration::seconds(1),
        "the outage starts at the cut: {outage_started} vs {cut_at}"
    );
    assert!(field(&ended, "outage_ended").len() > 10, "{ended}");
    let ms: u64 = field(&ended, "outage_ms").parse().unwrap();
    assert!(ms > 0, "{ended}");

    source.stop().await.unwrap();
    docker(&["rm", "-f", &name]);
}

fn field(line: &str, name: &str) -> String {
    line.split(&format!("{name}="))
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        .to_string()
}
