//! What an inbound delivery and a synchronous caller get from the real engine binary, on real
//! LocalStack SQS and PostgreSQL.
//!
//! - A channel's `region`, `queue.url` and `endpoint-override` written as environment references
//!   connect and consume, and an unresolved one stops the channel at wiring with a coded error.
//! - A message with no `content-type` attribute on a channel with a codec is decoded by that codec.
//! - A message the codec refuses, or one stating a type it does not accept, leaves a WARN with a
//!   stable code and an audit record, and is deleted from the queue.
//! - A message with no type on a channel with no codec is read as raw bytes.
//! - A synchronous flow with no `<q:reply>` answers an empty body, and one with a reply answers it.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use aws_sdk_sqs::types::{MessageAttributeValue, QueueAttributeName};
use aws_sdk_sqs::Client;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

const REGION: &str = "us-east-1";
const API_KEY: &str = "sqs-intake-conformance-key";
const CONFIG_INVALID: &str = "SUTRA.INBOUND.AWS_SQS.CONFIG_INVALID";

/// Parked in a `static`: a sync container dropped on a tokio worker panics, so these are never
/// dropped and the reaper removes them when the process exits.
struct Fixtures {
    _localstack: Container<GenericImage>,
    postgres: Container<Postgres>,
    localstack_port: u16,
    postgres_port: u16,
}

/// Blocking runners, on their own thread: they must not run on a tokio worker.
fn fixtures() -> &'static Fixtures {
    static FIXTURES: OnceLock<Fixtures> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        std::thread::spawn(|| {
            let localstack = GenericImage::new("localstack/localstack", "3")
                .with_exposed_port(4566.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Ready."))
                .with_env_var("SERVICES", "sqs")
                .start()
                .expect("start localstack/localstack:3 (docker required)");
            sutra_testkit::reap_on_exit(localstack.id());
            let postgres = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("start postgres:16-alpine (docker required)");
            sutra_testkit::reap_on_exit(postgres.id());
            let localstack_port = localstack.get_host_port_ipv4(4566.tcp()).expect("4566");
            let postgres_port = postgres.get_host_port_ipv4(5432).expect("5432");
            Fixtures {
                _localstack: localstack,
                postgres,
                localstack_port,
                postgres_port,
            }
        })
        .join()
        .expect("fixture thread")
    })
}

impl Fixtures {
    fn sql(&self, database: &str, query: &str) -> String {
        let out = Command::new("docker")
            .args(["exec", self.postgres.id()])
            .args(["psql", "-U", "postgres", "-d", database, "-tAc", query])
            .output()
            .expect("docker exec psql");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn sqs(&self) -> Client {
        sutra_transport_sqs::build_client(
            REGION,
            Some(&format!("http://127.0.0.1:{}", self.localstack_port)),
        )
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("its address")
        .port()
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf()
}

/// Every migration, as the engine image is given them.
fn migration_roots() -> String {
    repo_root()
        .join("rust/crates/sutra-persistence/migrations")
        .display()
        .to_string()
}

/// The engine binary this checkout built. Killed on drop, so a failed test leaves no process.
struct Engine {
    child: Child,
    log: PathBuf,
    port: u16,
    _work: PathBuf,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Engine {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn url(&self, channel: &str) -> String {
        format!("http://127.0.0.1:{}/channels/{channel}", self.port)
    }
}

fn start_engine(fixtures: &Fixtures, database: &str, hint: &str, env: &[(&str, &str)]) -> Engine {
    let work = std::env::temp_dir().join(format!("sqs-intake-{}-{hint}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    let archives = work.join("archives");
    std::fs::create_dir_all(&archives).expect("archives dir");
    sutra_loader::assemble_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sqs-intake/default--sqsintake--1.0.0"),
        &archives,
        &sutra_loader::PackageOptions::default(),
    )
    .expect("the package seals");
    let log = work.join("engine.log");
    let out = File::create(&log).expect("log file");
    let err = out.try_clone().expect("log file");
    let port = free_port();
    let pg = fixtures.postgres_port;
    let mut command = Command::new(env!("CARGO_BIN_EXE_sutra-engine"));
    command
        .env("SUTRA_DEPLOYMENTS_DIR", &archives)
        .env(
            "SUTRA_DATASOURCE_URL",
            format!("postgres://postgres:postgres@127.0.0.1:{pg}/{database}"),
        )
        .env("SUTRA_DB_MIGRATIONS", migration_roots())
        .env("SUTRA_HTTP_PORT", port.to_string())
        .env("SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED", "false")
        .env("SUTRA_AUDIT_SQL", "true")
        .env("SQSI_API_KEY", API_KEY)
        .env(
            "SQSI_STORE_URL",
            format!("postgresql://127.0.0.1:{pg}/{database}"),
        )
        .env("SQSI_STORE_USER", "postgres")
        .env("SQSI_STORE_PASSWORD", "postgres")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test");
    for (key, value) in env {
        command.env(key, value);
    }
    let child = command
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("start the engine binary");
    Engine {
        child,
        log,
        port,
        _work: work,
    }
}

/// Polls `check` until it answers, or panics with the engine's log.
async fn wait_for<T>(engine: &Engine, what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}. Engine log:\n{}",
            engine.log()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn await_ready(engine: &Engine) {
    let url = format!("http://127.0.0.1:{}/sutra/health/ready", engine.port);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(response) = reqwest::get(&url).await {
            if response.status().is_success() {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the engine never got ready. Log:\n{}",
            engine.log()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn create_queue(sqs: &Client, name: &str) -> String {
    sqs.create_queue()
        .queue_name(name)
        .send()
        .await
        .expect("create queue")
        .queue_url()
        .expect("queue url")
        .to_string()
}

/// Send `body`, with the `content-type` attribute when one is given.
async fn send(sqs: &Client, queue: &str, body: &str, content_type: Option<&str>) {
    let mut request = sqs.send_message().queue_url(queue).message_body(body);
    if let Some(content_type) = content_type {
        request = request.message_attributes(
            "content-type",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(content_type)
                .build()
                .expect("attribute"),
        );
    }
    request.send().await.expect("send message");
}

/// Waits until the queue holds nothing, visible or in flight.
async fn await_empty(engine: &Engine, sqs: &Client, queue: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let attributes = sqs
            .get_queue_attributes()
            .queue_url(queue)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .send()
            .await
            .expect("queue attributes");
        let count = |name: QueueAttributeName| {
            attributes
                .attributes()
                .and_then(|a| a.get(&name))
                .map(String::as_str)
                == Some("0")
        };
        if count(QueueAttributeName::ApproximateNumberOfMessages)
            && count(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "queue {queue} never emptied. Engine log:\n{}",
            engine.log()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The audit record of a dropped delivery with this cause, on `channel`.
fn dropped_audit(fixtures: &Fixtures, cause: &str, channel: &str) -> usize {
    fixtures
        .sql(
            "postgres",
            &format!(
                "SELECT COUNT(*) FROM audit_event WHERE event_type = 'inbound.DELIVERY_DROPPED' \
                 AND diagnostic_code = '{cause}' AND diagnostic_json LIKE '%\"{channel}\"%'"
            ),
        )
        .parse()
        .unwrap_or(0)
}

/// The engine's WARN for a dropped delivery with this cause.
fn dropped_warning(engine: &Engine, cause: &str) -> bool {
    engine.log().lines().any(|line| {
        line.contains("SUTRA.INBOUND.DELIVERY_DROPPED")
            && line.contains(cause)
            && line.contains("WARN")
    })
}

async fn post(engine: &Engine, channel: &str, body: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(engine.url(channel))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .header("X-Marker", "HEADER-MARKER")
        .body(body.to_string())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("post");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

fn assert_nothing_of_the_request_came_back(answer: &str) {
    for leak in [API_KEY, "HEADER-MARKER", "BODY-MARKER", "event", "payload"] {
        assert!(
            !answer.contains(leak),
            "{leak} reached the caller: {answer}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_sqs_intake_and_synchronous_answers_on_the_built_binary() {
    let _suite = sutra_testkit::hold_suite!();
    let fixtures = fixtures();
    let sqs = fixtures.sqs();
    let orders = create_queue(&sqs, "sqsi-orders").await;
    let raw = create_queue(&sqs, "sqsi-raw").await;
    let endpoint = format!("http://127.0.0.1:{}", fixtures.localstack_port);
    let base = format!("{endpoint}/000000000000");
    let engine = start_engine(
        fixtures,
        "postgres",
        "main",
        &[
            ("SQSI_REGION", REGION),
            ("SQSI_BASE", &base),
            ("SQSI_ENDPOINT", &endpoint),
        ],
    );
    await_ready(&engine).await;

    // References connect and consume; a body with no type is decoded by the channel's codec.
    send(
        &sqs,
        &orders,
        r#"<Order xmlns="urn:sutra:order"><Ref>NO-TYPE-1</Ref></Order>"#,
        None,
    )
    .await;
    let row = "SELECT store_value FROM data_store WHERE store_key = 'order|NO-TYPE-1'";
    let stored = wait_for(&engine, "the row of the untyped message", || {
        Some(fixtures.sql("postgres", row)).filter(|v| !v.is_empty())
    })
    .await;
    assert!(stored.contains("received"), "{stored}");
    await_empty(&engine, &sqs, &orders).await;
    assert!(
        !engine.log().contains("receiveMessage failed"),
        "{}",
        engine.log()
    );

    // A stated type still works.
    send(
        &sqs,
        &orders,
        r#"<Order xmlns="urn:sutra:order"><Ref>TYPED-1</Ref></Order>"#,
        Some("application/xml"),
    )
    .await;
    let row = "SELECT COUNT(*) FROM data_store WHERE store_key = 'order|TYPED-1'";
    wait_for(&engine, "the row of the typed message", || {
        Some(()).filter(|()| fixtures.sql("postgres", row) == "1")
    })
    .await;

    // A body the codec refuses: the WARN, the audit record, and the message deleted.
    send(&sqs, &orders, "<<< this is not a document", None).await;
    wait_for(&engine, "the codec refusal's audit record", || {
        Some(()).filter(|()| {
            dropped_audit(fixtures, "SUTRA.INBOUND.VALIDATION_REJECT", "orders-in") == 1
        })
    })
    .await;
    assert!(
        dropped_warning(&engine, "SUTRA.INBOUND.VALIDATION_REJECT"),
        "{}",
        engine.log()
    );
    assert!(
        !engine.log().contains("this is not a document"),
        "the body reached the log: {}",
        engine.log()
    );
    await_empty(&engine, &sqs, &orders).await;

    // A type the codec does not accept is refused the same way, not read as if unstated.
    send(
        &sqs,
        &orders,
        r#"<Order xmlns="urn:sutra:order"><Ref>WRONG-TYPE-1</Ref></Order>"#,
        Some("application/octet-stream"),
    )
    .await;
    wait_for(&engine, "the type refusal's audit record", || {
        Some(()).filter(|()| {
            dropped_audit(fixtures, "SUTRA.INBOUND.CAPABILITY_MISMATCH", "orders-in") == 1
        })
    })
    .await;
    assert!(
        dropped_warning(&engine, "SUTRA.INBOUND.CAPABILITY_MISMATCH"),
        "{}",
        engine.log()
    );
    await_empty(&engine, &sqs, &orders).await;
    let row = "SELECT COUNT(*) FROM data_store WHERE store_key = 'order|WRONG-TYPE-1'";
    assert_eq!(fixtures.sql("postgres", row), "0");

    // No codec and no type: read as raw bytes, as before.
    send(&sqs, &raw, "opaque text with no stated type", None).await;
    let row = "SELECT COUNT(*) FROM data_store WHERE store_key = 'raw|raw-in'";
    wait_for(&engine, "the row of the raw message", || {
        Some(()).filter(|()| fixtures.sql("postgres", row) == "1")
    })
    .await;

    // A synchronous flow with no reply that runs to its end answers an empty 200.
    let (status, answer) = post(&engine, "quiet", r#"{"ref":"Q-1","note":"BODY-MARKER"}"#).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer, "", "no <q:reply>, no body");
    let row = "SELECT COUNT(*) FROM data_store WHERE store_key = 'quiet|Q-1'";
    assert_eq!(fixtures.sql("postgres", row), "1", "the flow ran");

    // So does one that parks.
    let (status, answer) = post(&engine, "parked", r#"{"wait":"PT1H","note":"BODY-MARKER"}"#).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer, "", "no <q:reply>, no body");

    // A declared reply answers with its body, and only that.
    let (status, answer) = post(&engine, "answered", r#"{"note":"BODY-MARKER"}"#).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer, r#"{"answer":"from-the-reply"}"#);
    assert_nothing_of_the_request_came_back(&answer);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_sqs_intake_an_unresolved_reference_fails_at_wiring() {
    let _suite = sutra_testkit::hold_suite!();
    let fixtures = fixtures();
    fixtures.sql("postgres", "CREATE DATABASE unresolved");
    let endpoint = format!("http://127.0.0.1:{}", fixtures.localstack_port);
    let base = format!("{endpoint}/000000000000");
    // SQSI_REGION is not set anywhere.
    let engine = start_engine(
        fixtures,
        "unresolved",
        "unresolved",
        &[("SQSI_BASE", &base), ("SQSI_ENDPOINT", &endpoint)],
    );
    let line = wait_for(&engine, "the coded wiring error", || {
        engine
            .log()
            .lines()
            .find(|line| line.contains(CONFIG_INVALID) && line.contains("'region'"))
            .map(str::to_string)
    })
    .await;
    assert!(line.contains("orders-in"), "{line}");
    assert!(
        !engine.log().contains("aws-sqs consumer polling"),
        "the channel must not start: {}",
        engine.log()
    );
}
