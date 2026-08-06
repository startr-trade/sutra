//! Timer START events, end to end through a real engine (P1-5b).
//!
//! The claim this file defends is the whole point of the feature: **deploy → the schedule fires
//! → an instance actually runs**, with nothing pushing it. No inbound arrives; the only thing
//! that happens is that a deployment becomes ACTIVE and the clock moves.
//!
//! Docker-gated (tier-2): timer schedules are durable rows on a real PostgreSQL, and the poller
//! that claims them is leader-gated over the same database — there is no persistence-less path
//! through this feature to test.
//!
//! Coverage:
//! - (a) a past-dated `<timeDate>` start is armed already-due, fires on the first tick, runs its
//!   flow to a `<q:send>` the capture server observes, and RESOLVES its row (a single-shot
//!   schedule never fires twice);
//! - (b) a started instance has EMPTY variables — the rendered payload proves no `event.*` was
//!   projected;
//! - (c) an `R2/PT1S` cycle fires repeatedly and exhausts its budget;
//! - (d) retirement: dropping the archive resolves the deployment's schedule rows, so a
//!   flipped-away deployment stops minting work;
//! - (e) hot-deploy handoff: the replacement's schedules arm and the replaced deployment's
//!   resolve, so exactly one deployment mints for the slot.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static SEQ: AtomicU32 = AtomicU32::new(0);
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

// ---- container / database fixture -----------------------------------------------------------

fn container_port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    let port = PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: Container<Postgres> = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("postgres container starts");
            let port = container
                .get_host_port_ipv4(5432)
                .expect("mapped postgres port");
            // Leak the handle: the container must outlive every test in the binary.
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

/// The shipped migration roots + the Rust-only `native` addendum — V803 timer wait states AND
/// V804 timer-start schedules, which is what this suite exercises.
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
        "tstart_{}_{}",
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

    /// The body of the first delivery to `path` (the rendered payload).
    fn body(&self, path: &str) -> Option<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, b)| b.clone())
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

// ---- package fixtures -------------------------------------------------------------------

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "tstart-{name}-{}-{}",
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

/// A deployment whose ONLY entry point is a timer start. It has no inbound channel at all —
/// nothing can push it — so anything it does is proof the schedule fired.
///
/// `timer_xml` is the `<timerEventDefinition>` body; `marker` distinguishes deployment versions
/// on the capture sink.
fn timer_start_package(sink: SocketAddr, marker: &str, timer_xml: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_nightly"
                  targetNamespace="urn:sutra:module:nightly:1.0.0">
  <bpmn:process id="nightly" name="Scheduled sweep" isExecutable="true">
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
    // Outbound only: a schedule-driven module legitimately declares NO inbound channel, which is
    // also why the deployment plan must carry its namespace in its own right.
    let channels = format!(
        r#"channels:
  - name: notify
    direction: outbound
    transport: http
    bind: "http://{sink}/{marker}-fired"
"#
    );
    write(&root, "bpmn/nightly.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"nightly\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
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

// ---- engine boot ------------------------------------------------------------------------

async fn boot(deployments_dir: PathBuf, datasource_url: String) -> RunningEngine {
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
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    })
    .await
    .expect("engine boots")
}

async fn wait_until<F, Fut>(secs: u64, mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if probe().await {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

/// Schedule rows in the database, as `(node_id, status, remaining_fires)`. Read as the fixture
/// superuser, which bypasses RLS — the policy itself is proven by `rls_bypass_it`.
async fn schedule_rows(pool: &PgPool) -> Vec<(String, String, Option<i32>)> {
    sqlx::query(
        "SELECT node_id, status, remaining_fires FROM timer_schedule ORDER BY deployment_id, node_id",
    )
    .fetch_all(pool)
    .await
    .expect("read timer_schedule")
    .iter()
    .map(|r| {
        (
            r.get::<String, _>("node_id"),
            r.get::<String, _>("status"),
            r.get::<Option<i32>, _>("remaining_fires"),
        )
    })
    .collect()
}

async fn scheduled_count(pool: &PgPool) -> usize {
    schedule_rows(pool)
        .await
        .iter()
        .filter(|(_, status, _)| status == "SCHEDULED")
        .count()
}

// ---- (a)(b) deploy → fire → run --------------------------------------------------------------

/// The headline: a deployment whose only trigger is a past-dated `<timeDate>` start mints an
/// instance with no inbound whatsoever, runs its flow, and then STOPS (a single-shot schedule
/// resolves after its one fire).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_timer_start_deploys_fires_once_and_runs_its_flow() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    // A date in the past ⇒ armed already-due ⇒ fires on the first poller tick. Deterministic,
    // unlike waiting out a relative duration.
    let archive = package(&timer_start_package(
        sink,
        "v1",
        "<bpmn:timeDate>2020-01-01T00:00:00Z</bpmn:timeDate>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "nightly.sutra", &archive);

    let engine = boot(dir, url).await;

    // The schedule was armed by the ACTIVATION flip, before any tick.
    assert!(
        wait_until(10, || async { !schedule_rows(&pool).await.is_empty() }).await,
        "activation arms a schedule row for the timer start"
    );

    // ...and the poller fires it, minting an instance that runs to its <q:send>.
    assert!(
        wait_until(20, || async { capture.delivered("/v1-fired") >= 1 }).await,
        "the timer start must fire and its instance must run"
    );

    // A started instance carries NO inbound payload: the flow saw empty variables.
    let body = capture.body("/v1-fired").unwrap_or_default();
    assert!(
        !body.contains("idempotencyKey") && !body.contains("receivedAt"),
        "a schedule-started instance projects no event.* intake variables, got: {body}"
    );

    // Single-shot: the row resolves, so it never fires again.
    assert!(
        wait_until(15, || async { scheduled_count(&pool).await == 0 }).await,
        "a fired single-shot schedule RESOLVES: {:?}",
        schedule_rows(&pool).await
    );
    let delivered = capture.delivered("/v1-fired");
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(
        capture.delivered("/v1-fired"),
        delivered,
        "a resolved single-shot schedule must never fire a second time"
    );

    engine.shutdown().await;
}

// ---- (c) repeating cycles ---------------------------------------------------------------------

/// `R2/PT1S`: fires twice, one second apart, then exhausts its budget and resolves.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_repeating_cycle_fires_its_budget_then_exhausts() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let archive = package(&timer_start_package(
        sink,
        "cyc",
        "<bpmn:timeCycle>R2/PT1S</bpmn:timeCycle>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "nightly.sutra", &archive);

    let engine = boot(dir, url).await;

    assert!(
        wait_until(30, || async { capture.delivered("/cyc-fired") >= 2 }).await,
        "an R2 cycle fires twice: {:?}",
        schedule_rows(&pool).await
    );
    assert!(
        wait_until(20, || async { scheduled_count(&pool).await == 0 }).await,
        "and then exhausts its budget: {:?}",
        schedule_rows(&pool).await
    );

    // Budget spent ⇒ no third fire, however long we wait.
    let delivered = capture.delivered("/cyc-fired");
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(
        capture.delivered("/cyc-fired"),
        delivered,
        "R2 means exactly two fires"
    );

    engine.shutdown().await;
}

// ---- (d) retirement ---------------------------------------------------------------------------

/// Dropping the archive flips the deployment away; its schedules resolve in the same flip, so a
/// deployment that is no longer ACTIVE stops minting work even while it drains.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn retiring_a_deployment_stops_its_schedule() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    // An unbounded 1-second cycle: it would fire forever if retirement did not stop it.
    let archive = package(&timer_start_package(
        sink,
        "ret",
        "<bpmn:timeCycle>R/PT1S</bpmn:timeCycle>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "nightly.sutra", &archive);

    let engine = boot(dir.clone(), url).await;
    assert!(
        wait_until(30, || async { capture.delivered("/ret-fired") >= 1 }).await,
        "the cycle is firing before we retire it"
    );

    // Remove the archive — the deployment flips away.
    std::fs::remove_file(dir.join("nightly.sutra")).expect("remove archive");
    assert!(
        wait_until(20, || async { scheduled_count(&pool).await == 0 }).await,
        "a flipped-away deployment's schedules RESOLVE: {:?}",
        schedule_rows(&pool).await
    );

    let delivered = capture.delivered("/ret-fired");
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(
        capture.delivered("/ret-fired"),
        delivered,
        "a retired deployment mints no further work"
    );

    engine.shutdown().await;
}

// ---- (e) hot-deploy handoff --------------------------------------------------------------------

/// Replacing the slot's archive hands the schedule over: the NEW deployment mints, the replaced
/// one stops. Schedules follow the ACTIVE deployment, never the draining tail.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_hot_deploy_hands_the_schedule_to_the_replacement() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let v1 = package(&timer_start_package(
        sink,
        "h1",
        "<bpmn:timeCycle>R/PT1S</bpmn:timeCycle>",
    ));
    let v2 = package(&timer_start_package(
        sink,
        "h2",
        "<bpmn:timeCycle>R/PT1S</bpmn:timeCycle>",
    ));
    let dir = temp_root("dir");
    place_archive(&dir, "nightly.sutra", &v1);

    let engine = boot(dir.clone(), url).await;
    assert!(
        wait_until(30, || async { capture.delivered("/h1-fired") >= 1 }).await,
        "v1's schedule is minting"
    );

    // HOT-DEPLOY: v2 replaces v1 in the slot.
    place_archive(&dir, "nightly.sutra", &v2);
    assert!(
        wait_until(30, || async { capture.delivered("/h2-fired") >= 1 }).await,
        "the replacement's schedule takes over: {:?}",
        schedule_rows(&pool).await
    );

    // Exactly one deployment mints for the slot: v1's rows resolved when it flipped away.
    assert!(
        wait_until(20, || async { scheduled_count(&pool).await == 1 }).await,
        "exactly one ACTIVE deployment holds an armed schedule: {:?}",
        schedule_rows(&pool).await
    );
    let v1_fires = capture.delivered("/h1-fired");
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(
        capture.delivered("/h1-fired"),
        v1_fires,
        "the replaced deployment stopped minting"
    );
    assert!(
        capture.delivered("/h2-fired") > 1,
        "while the replacement keeps going"
    );

    engine.shutdown().await;
}
