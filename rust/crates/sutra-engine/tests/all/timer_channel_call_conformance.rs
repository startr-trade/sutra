//! Rust-side conformance suite: timers + channel-call +
//! strict-outbox step semantics at ENGINE level, against a real PostgreSQL (testcontainers)
//! and the real boot path (`serve`). The channel-semantics contract is normative and
//! THESE TESTS ARE THE PIN:
//!   (a) an intermediate timer parks a durable TIMER row, fires, and the instance completes;
//!   (b) a timer boundary on a channel-call fires → the timeout path is taken (and the
//!       request enqueue committed atomically with the park);
//!   (c) a channel-call parks, the correlated response resumes it with the declared output
//!       mapping applied, cancelling the pending timeout;
//!   (d) a channel-call without a timer boundary / `<q:timeout>` refuses to PACKAGE;
//!   (e) a step failure between send-collect and step-commit persists NOTHING (the strict
//!       transactional step — the crash surrogate is a forced unique-alias collision inside
//!       the park step);
//!   (f) a due TIMER row survives an engine restart and fires on the next engine.
//!
//! The booted engine runs the outbox DISPATCHER, so committed rows are
//! claimed and DELIVERED (then deleted). The static package fixture's `*.example`
//! destinations are staged per test with the hosts patched to a local capture listener —
//! the outbox pins are now the STRONGER end-to-end form: the emission must actually
//! DELIVER (delivery ⇒ the row was enqueued by exactly the step under test, since no
//! other step in these flows targets that destination), and delivered rows drain to zero.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::stores::{AliasStore, PgAliasStore};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

const API_KEY: &str = "conformance-key";

// ---- fixture --------------------------------------------------------------------------

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Fast poller ticks for every engine this binary boots (read once at serve()).
        std::env::set_var("SUTRA_TIMER_TICK_MS", "100");
        std::thread::spawn(|| {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("start postgres:16-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5432).expect("mapped 5432");
            (container, port)
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

/// The shipped migration roots + the Rust V803 timer addendum — everything the RUST
/// engine applies.
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

/// Fresh fully-migrated database; returns the pool and the datasource URL for `serve`.
async fn fresh_db() -> (PgPool, String) {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url("postgres"))
        .await
        .expect("admin connect");
    let db = format!(
        "conf_{}_{}",
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
    let refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);
    (pool, url)
}

/// A committed conformance deployment-package directory (`bpmn/` + `channels.yaml` +
/// `package.yaml`) — sealed to a `.sutra` per test.
fn conformance_package(root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/resources/conformance/{root}"))
}

/// The `*.example` sink hosts the static resources name — patched per test onto the
/// local capture listener (`http://<name>.example/<path>` → `http://<sink>/<name>/<path>`).
const SINK_HOSTS: [&str; 7] = [
    "timer-done-slow",
    "timer-done",
    "continue-tail",
    "callout2",
    "callout",
    "timeout",
    "done",
];

/// Stage the static `main` package directory into a fresh temp root with every `*.example`
/// destination rewritten to the test's capture listener.
fn stage_main(sink: SocketAddr) -> PathBuf {
    let src = conformance_package("main");
    let root = std::env::temp_dir().join(format!(
        "wsc-conf-{}-{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    copy_patched(&src, &root, &format!("http://{sink}"));
    root
}

fn copy_patched(src: &Path, dst: &Path, sink_base: &str) {
    std::fs::create_dir_all(dst).expect("stage dir");
    for entry in std::fs::read_dir(src).expect("read resources") {
        let entry = entry.expect("dir entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_patched(&from, &to, sink_base);
        } else {
            let mut content = std::fs::read_to_string(&from).expect("resource is text");
            for host in SINK_HOSTS {
                content = content.replace(
                    &format!("http://{host}.example/"),
                    &format!("{sink_base}/{host}/"),
                );
            }
            std::fs::write(&to, content).expect("stage resource");
        }
    }
}

/// Seal one package directory into a fresh temp archives dir (one `.sutra`) and return it.
fn seal_to_archives_dir(package_dir: &Path) -> PathBuf {
    let out = std::env::temp_dir().join(format!(
        "wsc-arch-{}-{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(package_dir, &out, &sutra_loader::PackageOptions::default())
        .expect("conformance package seals into one .sutra archive");
    out
}

/// Seal the (already staged + sink-patched) package directory into a `.sutra` archive and
/// boot the engine against that deployments directory — the only deployment model.
async fn boot(package_dir: PathBuf, datasource_url: &str) -> RunningEngine {
    let deployments_dir = seal_to_archives_dir(&package_dir);
    serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_secs(2),
        http_port: 0,
        datasource_url: Some(datasource_url.to_string()),
        datasource_username: None,
        datasource_password: None,
        // Fast dispatcher ticks: the tests observe actual delivery to the capture sink.
        outbox_tick_interval: std::time::Duration::from_millis(200),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        // The fixture role (testcontainers postgres superuser) has BYPASSRLS; relax the
        // boot check in tests only. rls_bypass_it proves the enforcement itself.
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    })
    .await
    .expect("engine boots")
}

// ---- capture sink (the patched `*.example` destinations deliver here) -------------------

/// One captured delivery: `(path, body)`.
type CapturedDelivery = (String, Vec<u8>);

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

    fn body_of(&self, path: &str) -> Option<Vec<u8>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, body)| body.clone())
    }

    fn total(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

async fn capture_handler(
    axum::extract::State(state): axum::extract::State<Capture>,
    uri: axum::http::Uri,
    body: axum::body::Bytes,
) -> axum::http::StatusCode {
    state
        .requests
        .lock()
        .unwrap()
        .push((uri.path().to_string(), body.to_vec()));
    axum::http::StatusCode::ACCEPTED
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

/// The deployment id the engine actually runs the sealed package under — the manifest-hash
/// identity (`deployment_id_of_manifest`), obtained by sealing the package and reading it back
/// through the PRODUCTION loader (`read_archive_file`). Sealing is deterministic, so this is the
/// exact id the booted engine assigns. Deliberately NOT the legacy `DeploymentId::derive` triple
/// shim: the archive path stamps the manifest-hash id, never the shim, so seeding engine state
/// under the shim silently mismatches and no-ops (the fault this replaces).
fn archive_deployment_id(package_dir: &Path) -> PersistDeploymentId {
    let archives_dir = seal_to_archives_dir(package_dir);
    let archive = std::fs::read_dir(&archives_dir)
        .expect("read archives dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|p| p.extension().is_some_and(|ext| ext == "sutra"))
        .expect("sealed .sutra archive present");
    let loaded = sutra_loader::read_archive_file(&archive).expect("read sealed archive");
    PersistDeploymentId::new(loaded.id.value()).expect("valid deployment id")
}

// ---- tiny blocking HTTP client ---------------------------------------------------------

fn http_post(addr: SocketAddr, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: {API_KEY}\r\n\
         Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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

async fn post(addr: SocketAddr, path: &str, content_type: &str, body: Vec<u8>) -> (u16, String) {
    let (path, content_type) = (path.to_string(), content_type.to_string());
    tokio::task::spawn_blocking(move || http_post(addr, &path, &content_type, &body))
        .await
        .expect("post task")
}

// ---- DB probes -------------------------------------------------------------------------

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
}

/// LIVE instances: `terminal_at IS NULL`.
///
/// This suite asks "is the instance still running?" at every park and every completion, and until
/// terminal retention (P1-2) the honest way to ask was `COUNT(*)` — a finished instance's row was
/// DELETED in the terminal transaction, so row-absence and completion were the same fact. They are
/// no longer: the terminal step now re-stamps the snapshot COMPLETED and stamps `terminal_at`, and
/// the row survives for `sutra.instance.retention` (default `P7D`) so an operator can still look
/// the instance up. `terminal_at IS NULL` is the definition of LIVE that P1-2 settled on
/// store-wide — `InstanceStore::count_active` and the default `list` both key off it — so this
/// probe now asks the same question the same way the engine does.
///
/// Deliberately NOT a raw row count and deliberately NOT retention-disabled boot: the suite runs
/// the engine on its DEFAULT posture, which means these pins now also prove that retaining
/// finished instances does not disturb the timer/channel-call lifecycle.
async fn live_instance_count(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NULL",
    )
    .await
}

/// RETAINED instances: finished, re-stamped and awaiting the retention purge
/// (`terminal_at IS NOT NULL`) — the other half of the same table.
async fn retained_instance_count(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NOT NULL",
    )
    .await
}

/// Poll `probe` (≈10×/s) until it returns true or `secs` elapse.
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

// ---- (a) intermediate timer parks then fires and completes ------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn timer_catch_parks_durably_then_fires_and_completes() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot(stage_main(sink), &url).await;

    let (status, body) = post(
        engine.local_addr,
        "/channels/timer-start",
        "text/plain",
        b"go".to_vec(),
    )
    .await;
    assert_eq!(status, 200, "park accepted: {body}");

    // Parked: one instance row + a WAITING TIMER row with a real due-at.
    assert_eq!(live_instance_count(&pool).await, 1);
    let timer_rows: i64 = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE kind = 'TIMER' AND status = 'WAITING' \
         AND timer_due_at IS NOT NULL AND node_id = 'Wait'",
    )
    .await;
    assert_eq!(timer_rows, 1, "the TIMER waiting_event row is durable");

    // The poller fires it; the instance completes: no LIVE instance left, waits resolved, the
    // post-timer send enqueued by the terminal step — and DELIVERED by the dispatcher
    // (only the terminal step targets this destination).
    let completed = wait_until(10, || {
        let pool = pool.clone();
        let capture = capture.clone();
        async move {
            live_instance_count(&pool).await == 0 && capture.delivered("/timer-done/notify") == 1
        }
    })
    .await;
    assert!(completed, "timer fired and the instance completed");
    // …and completing RETAINED it rather than deleting it (terminal retention, default `P7D`).
    // Pinned positively so the difference between "no longer running" and "no longer there" is
    // stated in the suite that cares most about the distinction.
    assert_eq!(
        retained_instance_count(&pool).await,
        1,
        "the finished instance is retained as history, not deleted"
    );
    let drained = wait_until(10, || {
        let pool = pool.clone();
        async move { count(&pool, "SELECT COUNT(*) FROM outbox_entry").await == 0 }
    })
    .await;
    assert!(drained, "delivered outbox rows are deleted");
    let live_waits = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE status = 'WAITING'",
    )
    .await;
    assert_eq!(live_waits, 0, "no wait row outlives the instance");
    engine.shutdown().await;
}

// ---- respond-and-continue: flush the reply, then the poller runs the detached tail ------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn respond_and_continue_flushes_the_reply_then_the_poller_runs_the_tail() {
    // A `<q:reply continue="true">` serviceTask flushes its reply to the SYNC caller
    // immediately, then parks (a due-now self-resume marker on the reply node) so the timer poller
    // runs the tail detached. End-to-end: the POST returns the rendered reply body right away, and
    // the tail's send lands on the capture sink exactly once (proving the poller self-resumed it).
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot(stage_main(sink), &url).await;

    // The POST returns the FLUSHED reply body synchronously — before the tail runs.
    let (status, body) = post(
        engine.local_addr,
        "/channels/continue-start",
        "application/json",
        br#"{"key":"K-1"}"#.to_vec(),
    )
    .await;
    assert_eq!(
        status, 200,
        "the continue-reply flushes a real 200 reply: {body}"
    );
    assert!(
        body.contains("ACCEPTED"),
        "the flushed body is the rendered reply, returned before the tail: {body}"
    );

    // The tail is detached: the poller claims the due-now marker and runs `TailNotify`, whose send
    // lands on the capture sink. Nothing else targets this destination, so a delivery proves the
    // parked tail self-resumed — exactly once — and the instance then completes.
    let tail_ran = wait_until(15, || {
        let pool = pool.clone();
        let capture = capture.clone();
        async move {
            capture.delivered("/continue-tail/notify") == 1 && live_instance_count(&pool).await == 0
        }
    })
    .await;
    let delivered = capture.delivered("/continue-tail/notify");
    let instances = live_instance_count(&pool).await;
    assert!(
        tail_ran,
        "the poller self-resumed the parked tail exactly once and the instance completed \
         (delivered={delivered}, instances={instances})"
    );
    let live_waits = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE status = 'WAITING'",
    )
    .await;
    assert_eq!(
        live_waits, 0,
        "no wait row outlives the completed continue-reply instance"
    );
    engine.shutdown().await;
}

// ---- (b) timer boundary on a channel-call fires → timeout path --------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn channel_call_timeout_boundary_takes_the_timeout_path() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot(stage_main(sink), &url).await;

    let (status, body) = post(
        engine.local_addr,
        "/channels/call-timeout-start",
        "text/plain",
        b"call-1".to_vec(),
    )
    .await;
    assert_eq!(status, 200, "park accepted: {body}");

    // Parked the moment the dispatch returned; the park recorded the host wait + the
    // boundary TIMER row.
    assert_eq!(live_instance_count(&pool).await, 1);
    let host_wait = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE node_id = 'CallOut' AND status = 'WAITING'",
    )
    .await;
    let timer_wait = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE node_id = 'CallTimeout' AND kind = 'TIMER' \
         AND status = 'WAITING'",
    )
    .await;
    assert_eq!((host_wait, timer_wait), (1, 1));

    // The REQUEST enqueue is committed atomically with the park step, and the
    // dispatcher DELIVERS it: no response ever arrives on this flow and the timeout path
    // sends elsewhere, so a delivered request can ONLY have ridden the park step.
    let requested = wait_until(10, || {
        let capture = capture.clone();
        async move { capture.delivered("/callout/req") == 1 }
    })
    .await;
    assert!(requested, "the channel-call request rode the park step");

    // No response ever arrives → the boundary fires → the timeout path emits its marker
    // and the instance completes.
    let timed_out =
        wait_until(10, || {
            let pool = pool.clone();
            let capture = capture.clone();
            async move {
                live_instance_count(&pool).await == 0 && capture.delivered("/timeout/notify") == 1
            }
        })
        .await;
    assert!(timed_out, "the timeout path ran to completion");
    let live_waits = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE status = 'WAITING'",
    )
    .await;
    assert_eq!(live_waits, 0);
    engine.shutdown().await;
}

// ---- (c) channel-call parks; correlated response resumes with output mapping ------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn channel_call_response_resumes_with_output_mapping_applied() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot(stage_main(sink), &url).await;

    let (status, body) = post(
        engine.local_addr,
        "/channels/call-resp-start",
        "application/json",
        br#"{"key":"K-100","note":"request"}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 200, "park accepted: {body}");

    // Parked: host wait + the <q:timeout>-synthesized TIMER row + the request enqueue +
    // the declared alias, all from ONE step. The request DELIVERS while the instance is
    // still parked (the PT30S timeout is far off; only the park step targets callout2) —
    // the strict transactional enqueue observed end-to-end.
    assert_eq!(live_instance_count(&pool).await, 1);
    let requested = wait_until(10, || {
        let capture = capture.clone();
        async move { capture.delivered("/callout2/req") == 1 }
    })
    .await;
    assert!(requested, "the channel-call request rode the park step");
    assert_eq!(
        live_instance_count(&pool).await,
        1,
        "still parked after delivery"
    );
    let timeout_row = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE node_id = 'CallOut#timeout' AND \
         kind = 'TIMER' AND status = 'WAITING'",
    )
    .await;
    assert_eq!(timeout_row, 1, "the <q:timeout> boundary armed durably");
    let alias_live: i64 = count(
        &pool,
        "SELECT COUNT(*) FROM alias_index WHERE alias_name = 'ccKey' AND alias_value = 'K-100' \
         AND live = TRUE",
    )
    .await;
    assert_eq!(alias_live, 1, "the park is keyed by the DECLARED alias");

    // The correlated response arrives → resume → output mapping binds payload.status to
    // responseBody → the done-marker send carries it → terminal step.
    let (status, body) = post(
        engine.local_addr,
        "/channels/call-response",
        "application/json",
        br#"{"key":"K-100","status":"APPROVED","noise":"MUST-NOT-LAND"}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 200, "response correlated: {body}");

    assert_eq!(live_instance_count(&pool).await, 0, "resumed to completion");
    // The done-marker send was enqueued by the terminal step and DELIVERS.
    let done = wait_until(10, || {
        let capture = capture.clone();
        async move { capture.delivered("/done/notify") == 1 }
    })
    .await;
    assert!(done, "the done marker delivered");
    let done_body = capture.body_of("/done/notify").expect("done marker body");
    assert_eq!(
        String::from_utf8_lossy(&done_body),
        "APPROVED",
        "the OUTPUT MAPPING landed payload.status as responseBody"
    );
    // The pending timeout was cancelled (resolved), the alias retired.
    let live_timer = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE kind = 'TIMER' AND status = 'WAITING'",
    )
    .await;
    assert_eq!(live_timer, 0, "the response cancels the timeout");
    let alias_live: i64 = count(&pool, "SELECT COUNT(*) FROM alias_index WHERE live = TRUE").await;
    assert_eq!(alias_live, 0, "aliases retired at completion");
    engine.shutdown().await;
}

// ---- (d) load-time error: channel-call without timeout ----------------------------------

#[test]
fn channel_call_without_timeout_refuses_to_package() {
    // In the archive model the fail-closed guarantee is EARLIER: a channel-call task with
    // no timeout can't even be sealed — `assemble_dir` refuses, so no such
    // archive ever reaches the engine.
    let out = std::env::temp_dir().join(format!(
        "wsc-badpkg-{}-{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&out);
    let error = sutra_loader::assemble_dir(
        &conformance_package("bad-missing-timeout"),
        &out,
        &sutra_loader::PackageOptions::default(),
    )
    .expect_err("packaging must fail closed (package-time validation)");
    let message = match error {
        sutra_loader::PackageError::Validation(report) => report
            .diagnostics
            .iter()
            .map(|d| d.message.clone())
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected a package-time validation refusal, got: {other}"),
    };
    assert!(
        message.contains("SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT_REQUIRED"),
        "got: {message}"
    );
    let _ = std::fs::remove_dir_all(&out);
}

// ---- (e) strict transactional step at engine level: failed park step persists NOTHING ---

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn failed_park_step_after_send_collect_persists_nothing() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let staged = stage_main(sink);
    // Seed the fault under the id the engine will actually run under (the archive's manifest-hash
    // identity), read from the sealed package via the production loader.
    let deployment = archive_deployment_id(&staged);
    let engine = boot(staged, &url).await;

    // Fault injection (the crash surrogate): another live instance already owns the
    // unique park alias, so the park step's alias write fails AFTER the request emission
    // was collected — the whole step must roll back.
    let alias_store = PgAliasStore::new(pool.clone());
    assert!(alias_store
        .record(&deployment, Uuid::new_v4(), "ccKey", "DUP", true)
        .await
        .unwrap());

    let (status, body) = post(
        engine.local_addr,
        "/channels/call-resp-start",
        "application/json",
        br#"{"key":"DUP","note":"colliding request"}"#.to_vec(),
    )
    .await;
    assert_ne!(status, 200, "the arrival must be rejected: {body}");
    assert!(
        body.contains("SUTRA.INBOUND.ALIAS_CONFLICT_REJECT"),
        "got: {body}"
    );

    // NOTHING persisted: no instance, no wait rows, no TIMER rows, and — the strict
    // transactional outbox — NO request enqueue, though the send was already collected.
    // The RAW row count, not the live one: this pin is "the step wrote nothing at all", and a
    // retained terminal row would be a write. (Nothing here ever reaches a terminal state, so the
    // two counts agree — asking the strict question keeps them honest if that ever changes.)
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM instance_state").await, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM waiting_event").await, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM outbox_entry").await, 0);
    assert_eq!(
        capture.total(),
        0,
        "nothing enqueued ⇒ the dispatcher had nothing to deliver"
    );
    let alias_rows: i64 = count(&pool, "SELECT COUNT(*) FROM alias_index").await;
    assert_eq!(
        alias_rows, 1,
        "only the pre-seeded owner's alias row remains"
    );
    engine.shutdown().await;
}

// ---- (f) a due TIMER row survives an engine restart --------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn timer_survives_engine_restart_and_fires_on_the_next_engine() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let staged = stage_main(sink);

    // Engine A parks the slow timer, then dies before due-at.
    let engine_a = boot(staged.clone(), &url).await;
    let (status, body) = post(
        engine_a.local_addr,
        "/channels/timer-start-slow",
        "text/plain",
        b"go".to_vec(),
    )
    .await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert_eq!(live_instance_count(&pool).await, 1);
    engine_a.shutdown().await;

    // The row is durable — still WAITING after the shutdown.
    let waiting: i64 = count(
        &pool,
        "SELECT COUNT(*) FROM waiting_event WHERE kind = 'TIMER' AND status = 'WAITING'",
    )
    .await;
    assert_eq!(waiting, 1, "the due row survives the engine");

    // Engine B (same database) picks it up, completes the instance and delivers the
    // terminal step's send. The window covers the timer-leader lease handover (engine
    // A's release is asynchronous; B re-polls on its election cadence).
    let engine_b = boot(staged, &url).await;
    let completed = wait_until(30, || {
        let pool = pool.clone();
        let capture = capture.clone();
        async move {
            live_instance_count(&pool).await == 0
                && capture.delivered("/timer-done-slow/notify") == 1
        }
    })
    .await;
    assert!(completed, "the restarted engine fired the durable timer");
    engine_b.shutdown().await;
}
