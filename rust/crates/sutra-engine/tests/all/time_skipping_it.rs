//! Time-skipping test runtime, end to end through a real engine (P1-7).
//!
//! The claim this file defends: a process with a wall-clock-infeasible timer (`PT24H`) or a
//! repeating `<timeCycle>` schedule (`R3/PT12H` — 36 MODELLED hours) completes in real
//! milliseconds/seconds under `sutra_engine::{TestClock, fast_forward_until}`, with NOTHING
//! sleeping for the modelled duration — only a virtual clock moving forward and a real, snappy
//! timer-poller tick noticing it. Exactly Temporal's most-praised DX, and exactly what the
//! comparison against other durable-execution engines promised was cheap here.
//!
//! Docker-gated (tier-2), same shape as `timer_channel_call_conformance.rs` and
//! `timer_start_conformance.rs`: a real PostgreSQL (testcontainers) and the real boot path
//! (`serve`), with `EngineConfig::now_override` installed instead of left `None`.
//!
//! Coverage:
//! - (a) an intermediate catch timer, `PT24H` — parks, fast-forwards, fires, completes;
//! - (b) a timer `<startEvent>` cyclic schedule, `R3/PT12H` — mints three instances under
//!   fast-forward and then stops (the `R3` budget), proving the SAME poller claim path the P1-5b
//!   suite pins also drives correctly off a virtual clock;
//! - (c) `<q:retry>` backoff parks ride the identical `waiting_event` TIMER-row/claim path proven
//!   in (a) — the due-at ARITHMETIC proof for retry lives in
//!   `sutra-executor/tests/all/time_skipping_retry_test.rs`, and the full serve()-reachable
//!   retry loop (a CHANNEL-CALL task's timeout → backoff → re-emission → exhaustion, F1) is
//!   pinned end to end in `channel_call_retry_it.rs`, so neither is duplicated here.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::{
    fast_forward_until, serve, DeploymentSourceKind, EngineConfig, RunningEngine, TestClock,
};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static SEQ: AtomicU32 = AtomicU32::new(0);
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

// ---- container / database fixture (mirrors timer_start_conformance.rs) ----------------------

fn container_port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    let port = PORT.get_or_init(|| {
        // A snappy REAL tick: fast-forward jumps the virtual clock straight to the next due
        // instant, so the only real waiting left is for the poller's next tick to notice it —
        // this keeps that noticing-latency small. It does not change WHAT the poller does, only
        // how often it looks; every claim predicate still reads `now` off `TestClock`.
        std::env::set_var("SUTRA_TIMER_TICK_MS", "20");
        std::thread::spawn(|| {
            let container: Container<Postgres> = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("postgres container starts");
            let port = container
                .get_host_port_ipv4(5432)
                .expect("mapped postgres port");
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
        "tskip_{}_{}",
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

// ---- capture sink (mirrors timer_start_conformance.rs) ---------------------------------------

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

// ---- package fixtures -------------------------------------------------------------------------

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "tskip-{name}-{}-{}",
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

/// Start (HTTP, `POST /channels/{marker}-start`) -> intermediate catch timer (`timer_xml`) ->
/// `<q:send>` a fired marker -> end. The (a) PT24H proof's fixture.
fn long_timer_package(sink: SocketAddr, marker: &str, timer_xml: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{marker}"
                  targetNamespace="urn:sutra:module:{marker}:1.0.0">
  <bpmn:process id="{marker}" name="Long timer" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="{marker}-start"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Wait"/>
    <bpmn:intermediateCatchEvent id="Wait" name="Hold until due">
      <bpmn:timerEventDefinition>{timer_xml}</bpmn:timerEventDefinition>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="Notify"/>
    <bpmn:sendTask id="Notify" name="Emit fired marker">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-done"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Notify" targetRef="End"/>
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
        value: time-skip-key
        header: X-Api-Key
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

/// A deployment whose ONLY entry point is a timer START — no inbound channel at all, so any
/// `<q:send>` it fires is proof the schedule fired on its own (mirrors
/// `timer_start_conformance.rs`'s fixture exactly; duplicated here rather than shared across
/// crate test binaries, matching this crate's existing per-file fixture convention).
fn cyclic_start_package(sink: SocketAddr, marker: &str, timer_xml: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{marker}"
                  targetNamespace="urn:sutra:module:{marker}:1.0.0">
  <bpmn:process id="{marker}" name="Cyclic schedule" isExecutable="true">
    <bpmn:startEvent id="Tick">
      <bpmn:timerEventDefinition>{timer_xml}</bpmn:timerEventDefinition>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Tick" targetRef="Notify"/>
    <bpmn:sendTask id="Notify" name="Announce the run">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-fired"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: notify
    direction: outbound
    transport: http
    bind: "http://{sink}/{marker}-fired"
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

// ---- engine boot --------------------------------------------------------------------------

/// Boots with `clock` installed on `EngineConfig::now_override` — the ONLY difference from
/// every other engine-level conformance suite's `boot()` in this crate.
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
        outbox_tick_interval: std::time::Duration::from_millis(200),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        // The fixture role (testcontainers postgres superuser) has BYPASSRLS; relax the boot
        // check in tests only. rls_bypass_it proves the enforcement itself.
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: Some(clock),
    })
    .await
    .expect("engine boots")
}

async fn live_instance_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NULL")
        .fetch_one(pool)
        .await
        .expect("count instance_state")
}

// ---- tiny blocking HTTP client -----------------------------------------------------------

fn http_post(addr: SocketAddr, path: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: time-skip-key\r\n\
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

// ---- (a) PT24H intermediate catch timer ------------------------------------------------------

/// The headline proof: a `PT24H` timer — one full modelled DAY — parks, fast-forwards, fires,
/// and the instance completes, in real wall-clock milliseconds.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_pt24h_catch_timer_fast_forwards_to_completion_in_real_milliseconds() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let clock = TestClock::starting_now();
    let archive = package(&long_timer_package(
        sink,
        "long",
        "<bpmn:timeDuration>PT24H</bpmn:timeDuration>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "long.sutra", &archive);
    let engine = boot(dir, url, clock.clone()).await;

    let (status, body) = post(engine.local_addr, "/channels/long-start", b"go".to_vec()).await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert_eq!(
        live_instance_count(&pool).await,
        1,
        "parked at the PT24H catch"
    );

    let started = std::time::Instant::now();
    let completed = fast_forward_until(
        &pool,
        &clock,
        std::time::Duration::from_secs(15),
        || async { live_instance_count(&pool).await == 0 && capture.delivered("/long-done") >= 1 },
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        completed,
        "the PT24H timer must fire under fast-forward and the instance must complete \
         (delivered={}, live={})",
        capture.delivered("/long-done"),
        live_instance_count(&pool).await
    );
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "a PT24H timer must complete in real wall-clock SECONDS under fast-forward, not real \
         hours: took {elapsed:?}"
    );
    assert!(
        clock.now() >= time::OffsetDateTime::now_utc() + time::Duration::hours(23),
        "the virtual clock itself must actually have advanced ~24h, not just the assertions: {}",
        clock.now()
    );

    engine.shutdown().await;
}

// ---- (b) R3/PT12H cyclic timer-start -----------------------------------------------------

/// `R3/PT12H`: three fires, 12 modelled hours apart (36 modelled hours total), then the cycle
/// exhausts its `R3` budget — all observed inside real wall-clock seconds.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_r3_pt12h_cyclic_start_fires_three_times_under_fast_forward() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let clock = TestClock::starting_now();
    let archive = package(&cyclic_start_package(
        sink,
        "cyc",
        "<bpmn:timeCycle>R3/PT12H</bpmn:timeCycle>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "cyc.sutra", &archive);

    let started = std::time::Instant::now();
    let engine = boot(dir, url, clock.clone()).await;

    let fired_three = fast_forward_until(
        &pool,
        &clock,
        std::time::Duration::from_secs(15),
        || async { capture.delivered("/cyc-fired") >= 3 },
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        fired_three,
        "an R3 cycle must fire exactly 3 times under fast-forward: delivered={}",
        capture.delivered("/cyc-fired")
    );
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "three fires spanning R3/PT12H = 36 modelled hours must complete in real wall-clock \
         SECONDS: took {elapsed:?}"
    );

    // Budget spent: fast-forwarding further must not produce a fourth fire.
    let delivered = capture.delivered("/cyc-fired");
    fast_forward_until(
        &pool,
        &clock,
        std::time::Duration::from_millis(500),
        || async { false },
    )
    .await;
    assert_eq!(
        capture.delivered("/cyc-fired"),
        delivered,
        "R3 means exactly three fires, budget-exhausted, however far the clock is pushed"
    );

    engine.shutdown().await;
}
