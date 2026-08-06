//! Channel-call `<q:retry>` through a REAL engine (F1 — retry reachability), end to end:
//! pure-BPMN authoring through the shipped `serve()` boot path, a real PostgreSQL, the real
//! outbox dispatcher delivering to a local counterpart, and the P1-7 `TestClock` collapsing
//! the timeout/backoff waits to real milliseconds.
//!
//! What only this file can prove — the full loop the unit layers assert in pieces:
//!
//! * (a) the counterpart NEVER answers: `<q:timeout PT2S>` fires → backoff park (the dead
//!   attempt's outbox rows WITHDRAWN) → the re-drive RE-EMITS (a genuinely second/third
//!   outbound request observed on the wire) → the budget exhausts → the durable FAILED
//!   snapshot names `SUTRA.RUNTIME.RETRY.EXHAUSTED`, no wait row survives, and no fourth
//!   request ever leaves;
//! * (b) the counterpart answers ATTEMPT 2: a response during the backoff window is REFUSED
//!   (`CHANNEL_CALL.RETRY_PENDING` — the dead attempt is not resurrectable), the re-driven
//!   attempt's response completes the instance normally, and a post-completion replay of the
//!   same response finds nothing to resume (aliases retired at the terminal step).
//!
//! Docker-gated (tier-2), same fixture shape as `time_skipping_it.rs`: virtual-clock jumps
//! are EXPLICIT (`clock.advance`) rather than `fast_forward_until`, because the outbox
//! delivers in REAL time — each jump lands exactly one timer event, then real-time polling
//! lets the delivery catch up, keeping every wire count deterministic.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine, TestClock};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::snapshot::InstanceSnapshot;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static SEQ: AtomicU32 = AtomicU32::new(0);
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

// ---- container / database fixture (mirrors time_skipping_it.rs) ----------------------------

fn container_port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    let port = PORT.get_or_init(|| {
        // Snappy REAL poller tick — the virtual clock only moves when a test advances it;
        // the tick is how fast the poller NOTICES the jump.
        std::env::set_var("SUTRA_TIMER_TICK_MS", "20");
        std::thread::spawn(|| {
            let container: Container<Postgres> = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("postgres container starts");
            let port = container
                .get_host_port_ipv4(5432)
                .expect("mapped postgres port");
            sutra_testkit::reap_on_exit(container.id());
            std::mem::forget(container);
            port
        })
        .join()
        .expect("container bootstrap thread")
    });
    *port
}

fn db_url(db: &str) -> String {
    format!(
        "postgres://postgres:postgres@127.0.0.1:{}/{db}",
        container_port()
    )
}

fn migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    vec![
        repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
        repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
        manifest
            .ancestors()
            .nth(1)
            .expect("crates dir")
            .join("sutra-persistence/migrations/native"),
    ]
}

async fn fresh_db() -> (PgPool, String) {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url("postgres"))
        .await
        .expect("admin connect");
    let db = format!(
        "ccretry_{}_{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create db");
    admin.close().await;

    let url = db_url(&db);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("db connect");
    let roots = migration_roots();
    let refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);
    (pool, url)
}

// ---- capture sink ---------------------------------------------------------------------------

type CapturedDelivery = (String, String);

#[derive(Clone, Default)]
struct Capture {
    requests: Arc<Mutex<Vec<CapturedDelivery>>>,
}

impl Capture {
    fn delivered(&self, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == path)
            .count()
    }
}

async fn capture_handler(
    axum::extract::State(state): axum::extract::State<Capture>,
    uri: axum::http::Uri,
    body: String,
) -> &'static str {
    state
        .requests
        .lock()
        .unwrap()
        .push((uri.path().to_string(), body));
    "ok"
}

async fn capture_server() -> (SocketAddr, Capture) {
    let capture = Capture::default();
    let app = axum::Router::new()
        .fallback(axum::routing::post(capture_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capture listener");
    let addr = listener.local_addr().expect("capture addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("capture serve");
    });
    (addr, capture)
}

// ---- package fixture ------------------------------------------------------------------------

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "ccretry-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Start (`{marker}-start`) → channel-call `Call` (outbound channel `callout` → the capture
/// sink; response relay on `{marker}-resp`; `<q:timeout PT2S>`; retried 3× at PT30S/2.0) →
/// `<q:send>` a done marker → end. The raw body is the correlation key on BOTH legs (the
/// start payload and the response payload bind the same `payload` variable the alias reads).
fn call_retry_package(sink: SocketAddr, marker: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{marker}"
                  targetNamespace="urn:sutra:module:{marker}:1.0.0">
  <bpmn:process id="{marker}" name="Retried channel call" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="{marker}-start" name="payload"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Call"/>
    <bpmn:serviceTask id="Call" name="Call the counterpart" implementation="channel:callout">
      <bpmn:extensionElements>
        <q:source channel="{marker}-resp" name="payload"/>
        <q:alias name="ccKey" expression="payload"/>
        <q:timeout duration="PT2S"/>
        <q:retry maxAttempts="3" initialDelay="PT30S" backoffCoefficient="2.0"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="Done"/>
    <bpmn:sendTask id="Done" name="Announce completion">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-done"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Done" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: {marker}-start
    transport: http
    bind: "POST /channels/{marker}-start"
    auth:
      scheme: apikey
      apikey:
        value: cc-retry-key
        header: X-Api-Key
  - name: {marker}-resp
    transport: http
    bind: "POST /channels/{marker}-resp"
    auth:
      scheme: apikey
      apikey:
        value: cc-retry-key
        header: X-Api-Key
  - name: callout
    direction: outbound
    transport: http
    bind: "http://{sink}/{marker}-req"
"#
    );
    write(&root, "bpmn/flow.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        &format!("labels:\n  \"tenant\": \"t1\"\n  \"module\": \"{marker}\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n"),
    );
    root
}

fn package(package_dir: &Path) -> Vec<u8> {
    let out = temp_root("pkg");
    let outcome =
        sutra_loader::assemble_dir(package_dir, &out, &sutra_loader::PackageOptions::default())
            .expect("fixture package seals");
    assert_eq!(outcome.archives.len(), 1, "one package = one archive");
    std::fs::read(&outcome.archives[0].file_path).expect("archive bytes")
}

fn place_archive(dir: &Path, name: &str, bytes: &[u8]) {
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).expect("write temp archive");
    std::fs::rename(&tmp, dir.join(name)).expect("rename into place");
}

// ---- engine boot ----------------------------------------------------------------------------

async fn boot(deployments_dir: PathBuf, datasource_url: String, clock: TestClock) -> RunningEngine {
    serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_millis(200),
        http_port: 0,
        datasource_url: Some(datasource_url),
        datasource_username: None,
        datasource_password: None,
        outbox_tick_interval: std::time::Duration::from_millis(100),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: Some(clock),
    })
    .await
    .expect("engine boots")
}

// ---- probes ---------------------------------------------------------------------------------

async fn live_instance_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NULL")
        .fetch_one(pool)
        .await
        .expect("count instance_state")
}

/// The one persisted instance's snapshot status (`SUSPENDED`/`FAILED`/`COMPLETED`…).
async fn instance_status(pool: &PgPool) -> Option<String> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT serialised FROM instance_state LIMIT 1")
        .fetch_optional(pool)
        .await
        .expect("read instance_state");
    row.map(|(bytes,)| {
        InstanceSnapshot::peek(&bytes)
            .expect("snapshot peeks")
            .status
    })
}

async fn waiting_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM waiting_event WHERE status = 'WAITING'")
        .fetch_one(pool)
        .await
        .expect("count waiting_event")
}

/// The instance's own outstanding outbox rows (the withdrawal probe).
async fn outbox_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM outbox_entry")
        .fetch_one(pool)
        .await
        .expect("count outbox_entry")
}

/// The durable backoff-park signal: `Call`'s single wait row flips MESSAGE → TIMER when a
/// failed attempt parks the backoff (and back on the re-drive). This — not the outbox count,
/// which also empties on ordinary delivery — is what sequences the virtual-clock jumps: each
/// jump lands exactly one timer event, and the next jump waits for its durable effect.
async fn call_wait_kind(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar(
        "SELECT kind FROM waiting_event WHERE node_id = 'Call' AND status = 'WAITING'",
    )
    .fetch_optional(pool)
    .await
    .expect("read Call wait row")
}

async fn call_in_backoff(pool: &PgPool) -> bool {
    call_wait_kind(pool).await.as_deref() == Some("TIMER")
}

/// Real-time poll (~10/s) up to `secs` — the virtual clock does NOT move here; this is how
/// the outbox/poller catch up with the last explicit jump.
async fn wait_until<F, Fut>(secs: u64, mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        if probe().await {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ---- tiny blocking HTTP client --------------------------------------------------------------

fn http_post(addr: SocketAddr, path: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: cc-retry-key\r\n\
         Content-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("request head");
    stream.write_all(body).expect("request body");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    (status, response)
}

async fn post(addr: SocketAddr, path: &str, body: Vec<u8>) -> (u16, String) {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || http_post(addr, &path, &body))
        .await
        .expect("post task")
}

// ---- (a) the counterpart never answers: re-emit → re-emit → exhaust → FAILED ----------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_silent_counterpart_burns_the_budget_re_emitting_and_lands_durable_failed_state() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let clock = TestClock::starting_now();
    let archive = package(&call_retry_package(sink, "silent"));
    let dir = temp_root("dir");
    place_archive(&dir, "silent.sutra", &archive);
    let engine = boot(dir, url, clock.clone()).await;
    let req = "/silent-req";
    let started = std::time::Instant::now();

    // Attempt 1 parks and its request reaches the wire in REAL time.
    let (status, body) = post(engine.local_addr, "/channels/silent-start", b"K-1".to_vec()).await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert!(
        wait_until(10, || async { capture.delivered(req) == 1 }).await,
        "attempt 1's request must ride the park step onto the wire"
    );

    // Timeout 1 (+PT2S virtual): the failure parks the backoff (Call's wait row flips to
    // TIMER) and WITHDRAWS the dead attempt's outbox rows — nothing outstanding remains to
    // deliver or poison, and nothing further reaches the wire.
    clock.advance(time::Duration::seconds(3));
    assert!(
        wait_until(10, || async { call_in_backoff(&pool).await }).await,
        "timeout 1 must park the backoff (kind={:?})",
        call_wait_kind(&pool).await
    );
    assert_eq!(
        outbox_rows(&pool).await,
        0,
        "the dead attempt's rows are gone"
    );
    assert_eq!(
        capture.delivered(req),
        1,
        "nothing delivered after the park"
    );

    // Backoff 1 elapses (+PT30S): the RE-DRIVE RE-EMITS — a genuinely second request.
    clock.advance(time::Duration::seconds(31));
    assert!(
        wait_until(10, || async { capture.delivered(req) == 2 }).await,
        "the re-drive must RE-EMIT the request (second delivery observed)"
    );

    // Timeout 2, backoff 2 (doubled: PT60S), attempt 3's re-emission.
    clock.advance(time::Duration::seconds(3));
    assert!(
        wait_until(10, || async { call_in_backoff(&pool).await }).await,
        "timeout 2 must park the second backoff"
    );
    clock.advance(time::Duration::seconds(61));
    assert!(
        wait_until(10, || async { capture.delivered(req) == 3 }).await,
        "the third (last-budget) attempt must re-emit"
    );

    // Timeout 3 exhausts maxAttempts=3: the durable FAILED snapshot, waits resolved.
    clock.advance(time::Duration::seconds(3));
    assert!(
        wait_until(10, || async {
            instance_status(&pool).await.as_deref() == Some("FAILED")
        })
        .await,
        "exhaustion must land the durable FAILED snapshot (status={:?})",
        instance_status(&pool).await
    );
    assert_eq!(waiting_rows(&pool).await, 0, "no wait row survives FAILED");

    // However far time goes, the corpse never twitches: no fourth request, ever.
    clock.advance(time::Duration::minutes(10));
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(
        capture.delivered(req),
        3,
        "a FAILED instance never re-emits"
    );
    assert_eq!(
        capture.delivered("/silent-done"),
        0,
        "the flow never finished"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(45),
        "94 modelled seconds of backoff must run in real seconds: {:?}",
        started.elapsed()
    );

    engine.shutdown().await;
}

// ---- (b) the counterpart answers attempt 2 --------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_answer_to_the_re_driven_attempt_completes_and_the_dead_attempt_stays_dead() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let clock = TestClock::starting_now();
    let archive = package(&call_retry_package(sink, "answered"));
    let dir = temp_root("dir");
    place_archive(&dir, "answered.sutra", &archive);
    let engine = boot(dir, url, clock.clone()).await;
    let req = "/answered-req";

    let (status, body) = post(
        engine.local_addr,
        "/channels/answered-start",
        b"K-2".to_vec(),
    )
    .await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert!(
        wait_until(10, || async { capture.delivered(req) == 1 }).await,
        "attempt 1's request reaches the wire"
    );

    // Timeout 1: attempt 1 dies into the backoff window.
    clock.advance(time::Duration::seconds(3));
    assert!(
        wait_until(10, || async { call_in_backoff(&pool).await }).await,
        "timeout 1 must park the backoff (kind={:?})",
        call_wait_kind(&pool).await
    );
    assert_eq!(
        outbox_rows(&pool).await,
        0,
        "the dead attempt's rows are gone"
    );

    // A LATE answer to the DEAD attempt is refused fail-closed — the instance is untouched.
    let (status, body) = post(
        engine.local_addr,
        "/channels/answered-resp",
        b"K-2".to_vec(),
    )
    .await;
    assert_ne!(status, 200, "a dead attempt's response must not resume");
    assert!(
        body.contains("SUTRA.DISPATCH.CHANNEL_CALL.RETRY_PENDING"),
        "the refusal names the backoff window: {body}"
    );
    assert_eq!(
        instance_status(&pool).await.as_deref(),
        Some("SUSPENDED"),
        "the parked instance is untouched by the refused relay"
    );

    // The re-drive re-emits; THIS attempt's answer completes the flow normally.
    clock.advance(time::Duration::seconds(31));
    assert!(
        wait_until(10, || async { capture.delivered(req) == 2 }).await,
        "the re-drive re-emits"
    );
    let (status, body) = post(
        engine.local_addr,
        "/channels/answered-resp",
        b"K-2".to_vec(),
    )
    .await;
    assert_eq!(status, 200, "the retry's response correlates: {body}");
    assert!(
        wait_until(10, || async {
            live_instance_count(&pool).await == 0 && capture.delivered("/answered-done") == 1
        })
        .await,
        "the answered retry completes the instance (done={}, live={})",
        capture.delivered("/answered-done"),
        live_instance_count(&pool).await
    );

    // NOT resurrectable: replaying the response finds nothing (aliases retired at terminal),
    // and the completed instance stays completed.
    let (status, _body) = post(
        engine.local_addr,
        "/channels/answered-resp",
        b"K-2".to_vec(),
    )
    .await;
    assert_ne!(
        status, 200,
        "a completed call's response replay must not land"
    );
    assert_eq!(
        instance_status(&pool).await.as_deref(),
        Some("COMPLETED"),
        "terminal retention keeps the COMPLETED record"
    );
    assert_eq!(
        capture.delivered("/answered-done"),
        1,
        "no double completion"
    );

    engine.shutdown().await;
}
