//! Conformance suite — archive loading + two-phase activation. The archive contract is
//! normative and THESE TESTS ARE THE PIN for prepare-fully-then-swap.
//!
//!   (a) reject-mutated-archive — a digest-tampered `.sutra` registers NOTHING, and the
//!       engine still boots and serves the rest of the source (fail-closed per archive);
//!   (b) reject-stowaway — an unlisted entry rejects the whole archive the same way;
//!   (d) flip-under-load — HTTP traffic driven through an archive swap: zero dropped /
//!       zero mixed responses, in-flight completes on the old deployment, new intake
//!       lands on the new one;
//!   (e) rollback-flip — swapping the old file back flips traffic back (same mechanism);
//!   (f) pinned-resume-across-flip — an instance parked on v1 resumes (relay) AFTER the
//!       flip to v2, executes v1's model (its pinned deployment), a post-flip park lands
//!       on v2, and the drained v1 RETIRES (deregisters) once quiescent.
//!
//! (a)-(e) run persistence-less; (f) runs against a real PostgreSQL (testcontainers) +
//! the real boot path (`serve`) with the outbox dispatcher delivering to a local
//! capture sink.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

const API_KEY: &str = "flip-key";

static SEQ: AtomicU32 = AtomicU32::new(0);

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "wsl-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

// ---- fixture package directories --------------------------------------------------------
// Each helper builds a STANDALONE deployment-package directory (bpmn/ + templates/ +
// channels.yaml + package.yaml) — the authoring unit `assemble_dir` seals into one
// `.sutra`. The `.sutra` archive is the only deployment model.

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A `package.yaml` for the given authoring triple (labels are opaque selectors).
fn package_yaml(tenant: &str, module: &str) -> String {
    format!(
        "labels:\n  \"tenant\": \"{tenant}\"\n  \"module\": \"{module}\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n"
    )
}

/// The sync app: `POST /channels/<channel>` runs a template reply flow whose body is the
/// VERSION MARKER (`v1` / `v2`) — the observable of the binding flip.
fn sync_app_package(module: &str, channel: &str, marker: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_ping"
                  targetNamespace="urn:sutra:module:{module}:1.0.0">
  <bpmn:process id="ping" name="Ping" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="{channel}"/></bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Reply"/>
    <bpmn:serviceTask id="Reply" name="Render the marker" implementation="version.hbs">
      <bpmn:extensionElements><q:reply mode="native" contentType="text/plain"/></bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="F2" sourceRef="Reply" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: {channel}
    transport: http
    bind: "POST /channels/{channel}"
    auth:
      scheme: apikey
      apikey:
        value: {API_KEY}
        header: X-Api-Key
"#
    );
    write(&root, "bpmn/ping.bpmn", &bpmn);
    write(&root, "templates/version.hbs", marker);
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &package_yaml("t1", module));
    root
}

/// The stateful app: `hold-start` parks a channel-call keyed by `payload.key`; the
/// correlated response on `hold-response` resumes it; the done-marker send names the
/// VERSION (`http://<sink>/<marker>-done`) — the observable of pinned resume.
fn stateful_app_package(sink: SocketAddr, marker: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_hold"
                  targetNamespace="urn:sutra:module:hold:1.0.0">
  <bpmn:process id="hold" name="Hold for a correlated response" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="hold-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="CallOut"/>
    <bpmn:serviceTask id="CallOut" name="Call the partner" implementation="channel:callout">
      <bpmn:extensionElements>
        <q:source channel="hold-response"/>
        <q:alias name="holdKey" expression="payload.key" unique="true"/>
        <q:timeout duration="PT120S"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="CallOut" targetRef="DoneNotify"/>
    <bpmn:sendTask id="DoneNotify" name="Version-marked done">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-done"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="DoneNotify" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: hold-start
    transport: http
    bind: "POST /channels/hold-start"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: {API_KEY}
        header: X-Api-Key
  - name: hold-response
    transport: http
    bind: "POST /channels/hold-response"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: {API_KEY}
        header: X-Api-Key
  - name: callout
    direction: outbound
    transport: http
    bind: "http://{sink}/callout"
"#
    );
    write(&root, "bpmn/hold.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &package_yaml("t1", "hold"));
    root
}

/// Seal one package directory → the single archive's bytes (the `sutra package` code path).
fn package(package_dir: &Path) -> Vec<u8> {
    let out = temp_root("pkg");
    let outcome =
        sutra_loader::assemble_dir(package_dir, &out, &sutra_loader::PackageOptions::default())
            .expect("fixture package seals");
    assert_eq!(outcome.archives.len(), 1, "one package = one archive");
    std::fs::read(&outcome.archives[0].file_path).expect("archive bytes")
}

// ---- archive tampering ------------------------------------------------------------------

fn entries_of(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let mut out = BTreeMap::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        out.insert(file.name().to_string(), buf);
    }
    out
}

/// Flip the template's bytes WITHOUT re-hashing the manifest — a digest mismatch.
fn mutate_template(bytes: &[u8]) -> Vec<u8> {
    let mut entries = entries_of(bytes);
    let key = entries
        .keys()
        .find(|k| k.starts_with("templates/"))
        .expect("a template entry")
        .clone();
    entries.insert(key, b"tampered".to_vec());
    sutra_loader::write_archive(&entries).expect("re-zip")
}

/// Add an entry the manifest does not list — a stowaway.
fn add_stowaway(bytes: &[u8]) -> Vec<u8> {
    let mut entries = entries_of(bytes);
    entries.insert("templates/rogue.hbs".to_string(), b"stowaway".to_vec());
    sutra_loader::write_archive(&entries).expect("re-zip")
}

// ---- deployments-dir helpers --------------------------------------------------------------

/// Write/replace an archive ATOMICALLY (temp file + rename) — a poll tick never observes
/// a half-written or missing file. The operator contract for live swaps.
fn place_archive(dir: &Path, name: &str, bytes: &[u8]) {
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).expect("write temp archive");
    std::fs::rename(&tmp, dir.join(name)).expect("rename into place");
}

// ---- engine boot ---------------------------------------------------------------------------

async fn boot_archive_engine(
    deployments_dir: PathBuf,
    datasource_url: Option<String>,
) -> RunningEngine {
    serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_millis(200),
        http_port: 0,
        datasource_url,
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

// ---- tiny blocking HTTP client -------------------------------------------------------------

fn http_request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: {API_KEY}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

async fn post(addr: SocketAddr, path: &str, body: &[u8]) -> (u16, String) {
    let (path, body) = (path.to_string(), body.to_vec());
    tokio::task::spawn_blocking(move || http_request(addr, "POST", &path, &body))
        .await
        .expect("post task")
}

async fn ready_deployments(addr: SocketAddr) -> Option<u64> {
    let (status, body) =
        tokio::task::spawn_blocking(move || http_request(addr, "GET", "/sutra/health/ready", b""))
            .await
            .expect("ready task");
    if status != 200 {
        return None;
    }
    let json: serde_json::Value = serde_json::from_str(body.trim()).ok()?;
    json["checks"][0]["data"]["deployments"].as_u64()
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

// ---- (a) reject-mutated-archive ------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn mutated_archive_registers_nothing_and_engine_serves_the_rest() {
    let bad = mutate_template(&package(&sync_app_package("app", "ping", "v1")));
    let good = package(&sync_app_package("other", "pong", "ok"));

    let dir = temp_root("dir");
    place_archive(&dir, "bad.sutra", &bad);
    place_archive(&dir, "good.sutra", &good);

    let engine = boot_archive_engine(dir, None).await;
    let addr = engine.local_addr;

    // The tampered archive registered NOTHING; the good one serves — fail-closed is
    // per-archive, never engine-wide.
    assert_eq!(ready_deployments(addr).await, Some(1));
    let (status, _) = post(addr, "/channels/ping", b"{}").await;
    assert_eq!(status, 404, "the mutated archive's channel must not exist");
    let (status, body) = post(addr, "/channels/pong", b"{}").await;
    assert_eq!(status, 200, "the good archive serves: {body}");
    assert_eq!(body, "ok");
    engine.shutdown().await;
}

// ---- (b) reject-stowaway --------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stowaway_archive_registers_nothing() {
    let bad = add_stowaway(&package(&sync_app_package("app", "ping", "v1")));
    let dir = temp_root("dir");
    place_archive(&dir, "app.sutra", &bad);

    let engine = boot_archive_engine(dir, None).await;
    let addr = engine.local_addr;
    assert_eq!(ready_deployments(addr).await, Some(0));
    let (status, _) = post(addr, "/channels/ping", b"{}").await;
    assert_eq!(status, 404);
    engine.shutdown().await;
}

// ---- (g10a) deployment-status endpoint ------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn deployment_status_endpoint_reports_active_slot_and_unknown() {
    let good = package(&sync_app_package("statusapp", "sping", "v1"));
    let id = sutra_loader::read_archive(&good)
        .expect("read archive")
        .id
        .value()
        .to_string();

    let dir = temp_root("dir");
    place_archive(&dir, "statusapp.sutra", &good);
    let engine = boot_archive_engine(dir, None).await;
    let addr = engine.local_addr;

    // The watcher publishes the snapshot asynchronously after boot — poll until the deployed
    // archive shows up Active, keyed by its slot (the archive filename = the ConfigMap key).
    let idc = id.clone();
    let ok = wait_until(5, || {
        let idc = idc.clone();
        async move {
            let (status, body) = tokio::task::spawn_blocking(move || {
                http_request(addr, "GET", "/sutra/deployments", b"")
            })
            .await
            .expect("list task");
            if status != 200 {
                return false;
            }
            let json: serde_json::Value = match serde_json::from_str(body.trim()) {
                Ok(j) => j,
                Err(_) => return false,
            };
            json["active"].as_array().is_some_and(|a| {
                a.len() == 1
                    && a[0]["deploymentId"] == idc
                    && a[0]["slot"] == "statusapp.sutra"
                    && a[0]["phase"] == "Active"
                    && a[0]["ready"] == true
            })
        }
    })
    .await;
    assert!(
        ok,
        "deployed archive must appear Active in /sutra/deployments"
    );

    // By-id: the deployed (content-hash) id is Active; a well-formed but unknown id is 404.
    let idc = id.clone();
    let (s_ok, b_ok) = tokio::task::spawn_blocking(move || {
        http_request(addr, "GET", &format!("/sutra/deployments/{idc}"), b"")
    })
    .await
    .expect("by-id task");
    assert_eq!(s_ok, 200, "by-id Active: {b_ok}");
    let by_id: serde_json::Value = serde_json::from_str(b_ok.trim()).expect("by-id json");
    assert_eq!(by_id["phase"], "Active");
    assert_eq!(by_id["deploymentId"], id);

    let (s_unknown, _) = tokio::task::spawn_blocking(move || {
        http_request(
            addr,
            "GET",
            "/sutra/deployments/dep-000000000000000000000000",
            b"",
        )
    })
    .await
    .expect("unknown task");
    assert_eq!(s_unknown, 404, "an unactivated id must be 404 Unknown");

    engine.shutdown().await;
}

// ---- (d)+(e) flip under load, then rollback ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn flip_under_load_drops_nothing_and_rollback_flips_back() {
    let v1 = package(&sync_app_package("app", "ping", "v1"));
    let v2 = package(&sync_app_package("app", "ping", "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "app.sutra", &v1);

    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/channels/ping", b"{}").await;
    assert_eq!((status, body.as_str()), (200, "v1"));

    // Drive continuous traffic from 4 client threads while the file is swapped.
    let stop = Arc::new(AtomicBool::new(false));
    let observed: Arc<Mutex<Vec<(u16, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let clients: Vec<_> = (0..4)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let observed = Arc::clone(&observed);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let result = http_request(addr, "POST", "/channels/ping", b"{}");
                    observed.lock().unwrap().push(result);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            })
        })
        .collect();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    place_archive(&dir, "app.sutra", &v2); // atomic replace — the flip
    assert!(
        wait_until(10, || async {
            post(addr, "/channels/ping", b"{}").await.1 == "v2"
        })
        .await,
        "the flip must reach v2"
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    for c in clients {
        c.join().expect("client thread");
    }

    let observed = Arc::try_unwrap(observed).unwrap().into_inner().unwrap();
    let v1_count = observed.iter().filter(|(_, b)| b == "v1").count();
    let v2_count = observed.iter().filter(|(_, b)| b == "v2").count();
    let bad: Vec<_> = observed
        .iter()
        .filter(|(s, b)| *s != 200 || (b != "v1" && b != "v2"))
        .collect();
    assert!(
        bad.is_empty(),
        "zero dropped/mixed responses across the flip; got {} bad of {}: {:?}",
        bad.len(),
        observed.len(),
        &bad[..bad.len().min(5)]
    );
    assert!(v1_count > 0, "traffic before the flip completed on v1");
    assert!(v2_count > 0, "traffic after the flip landed on v2");

    // (e) rollback = the operator swaps the old file back — the same mechanism.
    place_archive(&dir, "app.sutra", &v1);
    assert!(
        wait_until(10, || async {
            post(addr, "/channels/ping", b"{}").await.1 == "v1"
        })
        .await,
        "rollback must flip traffic back to v1"
    );
    // The rolled-back deployment serves steadily (both archives stayed REGISTERED).
    let (status, body) = post(addr, "/channels/ping", b"{}").await;
    assert_eq!((status, body.as_str()), (200, "v1"));
    engine.shutdown().await;
}

// ---- (f) pinned-resume-across-flip (real PostgreSQL) -----------------------------------------

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
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
    let base = format!(
        "postgres://postgres:postgres@127.0.0.1:{}",
        container_port()
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("admin connect");
    let db = format!(
        "wsl_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create db");
    admin.close().await;

    let url = format!("{base}/{db}");
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn pinned_instance_resumes_across_flip_then_old_deployment_retires() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let v1 = package(&stateful_app_package(sink, "v1"));
    let v2 = package(&stateful_app_package(sink, "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "hold.sutra", &v1);

    let engine = boot_archive_engine(dir.clone(), Some(url)).await;
    let addr = engine.local_addr;
    assert_eq!(ready_deployments(addr).await, Some(1));

    // Park K1 on v1: the channel-call request commits with the park step and delivers.
    let (status, body) = post(addr, "/channels/hold-start", br#"{"key":"K1"}"#).await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert!(
        wait_until(10, || async { capture.delivered("/callout") >= 1 }).await,
        "the park step's channel-call request must deliver (outbox)"
    );

    // FLIP: v2 replaces the archive; v1 drains (K1 is pinned to it).
    place_archive(&dir, "hold.sutra", &v2);
    assert!(
        wait_until(10, || async { ready_deployments(addr).await == Some(2) }).await,
        "after the flip: v2 active + v1 DRAINING (K1 still parked)"
    );

    // New intake lands on v2: park K2 post-flip.
    let (status, _) = post(addr, "/channels/hold-start", br#"{"key":"K2"}"#).await;
    assert_eq!(status, 200);
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 2 }).await);

    // The correlated response for K1 arrives on the FLIPPED channel — the relay must
    // fall back to the DRAINING scope, resume the v1-pinned instance, and run V1's
    // model: the done marker is /v1-done, never /v2-done.
    let (status, body) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200, "relay resumes the pinned instance: {body}");
    assert!(
        wait_until(10, || async { capture.delivered("/v1-done") >= 1 }).await,
        "the resumed instance completes on ITS pinned deployment (v1 model)"
    );
    assert_eq!(
        capture.delivered("/v2-done"),
        0,
        "the v1-pinned instance must never execute v2's model"
    );

    // K2 resumes on v2 — new instances are pinned to the new deployment.
    let (status, _) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K2","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        wait_until(10, || async { capture.delivered("/v2-done") >= 1 }).await,
        "the post-flip instance completes on v2"
    );

    // v1 is now quiescent (no instances, outbox drained) — it RETIRES and deregisters.
    assert!(
        wait_until(15, || async { ready_deployments(addr).await == Some(1) }).await,
        "the drained v1 deployment retires once quiescent"
    );
    engine.shutdown().await;
}

// ---- (h) db source: the DRAINING tail, pinned resume, and fail-closed pinning ----------------

/// The `db` deployment source: no folder anywhere — the archive bytes live in
/// `deployment_archive` and deploys arrive through `POST /admin/deployments`. The poll interval
/// still matters here: it is the cadence of the retire-when-quiescent sweep on this source.
async fn boot_db_engine(datasource_url: String) -> RunningEngine {
    // The db source reads `deployment_archive` at boot — that table is the `deploy` migration
    // family, so the engine must see the FULL shipped root list (same value every IT writes).
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Db,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: None,
        deployments_poll_interval: std::time::Duration::from_millis(200),
        http_port: 0,
        datasource_url: Some(datasource_url),
        datasource_username: None,
        datasource_password: None,
        outbox_tick_interval: std::time::Duration::from_millis(200),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        // The db source's ONLY deploy path is `POST /admin/deployments`, so the admin surface
        // must be reachable — the explicit dev escape hatch, exactly like a compose dev stack.
        admin_auth: sutra_engine::config::AdminAuthConfig {
            dev_disabled: true,
            ..Default::default()
        },
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        now_override: None,
    })
    .await
    .expect("engine boots on the db deployment source")
}

/// One string field out of a JSON response body.
fn json_field(body: &str, field: &str) -> String {
    let json: serde_json::Value = serde_json::from_str(body.trim()).expect("json body");
    json[field]
        .as_str()
        .unwrap_or_else(|| panic!("field '{field}' in {body}"))
        .to_string()
}

/// The db-source counterpart of `pinned_instance_resumes_across_flip_then_old_deployment_retires`
/// — the source where the DRAINING tail did not exist at all: a hot-deploy through the deploy API
/// re-activated only the store's `active` rows, so the demoted revision was deregistered the
/// instant the new one landed and every instance pinned to it lost its definition.
///
/// Three properties, in order: (1) after a hot-deploy the pinned instance still resumes on ITS
/// OWN graph (v1's model, never v2's); (2) once it is quiescent the drained revision RETIRES on
/// this source too (the sweep the dir watcher always had); (3) when the pinned definition really
/// is gone, the relay FAILS CLOSED — it must never migrate the instance onto the active graph.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn db_source_keeps_pinned_instances_resumable_across_a_hot_deploy() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&stateful_app_package(sink, "v1"));
    let v2 = package(&stateful_app_package(sink, "v2"));
    let v3 = package(&stateful_app_package(sink, "v3"));

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;

    // Deploy v1 through the sync deploy API — the whole deployment source is the table.
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1 deploy: {body}");
    assert!(wait_until(10, || async { ready_deployments(addr).await == Some(1) }).await);

    // Park K1 on v1: the channel-call request commits with the park step and delivers.
    let (status, body) = post(addr, "/channels/hold-start", br#"{"key":"K1"}"#).await;
    assert_eq!(status, 200, "park accepted: {body}");
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 1 }).await);

    // HOT-DEPLOY v2 into the same slot. `upsert_active` demotes v1 to `draining`; the flip must
    // re-plan it from its stored bytes and keep it registered — K1 is pinned to it.
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 hot-deploy: {body}");
    assert!(
        wait_until(10, || async { ready_deployments(addr).await == Some(2) }).await,
        "after the hot-deploy: v2 active + v1 DRAINING (K1 still parked)"
    );

    // The correlated response for K1 arrives on the re-bound channel — the relay falls back to
    // the DRAINING scope and runs V1's model: the done marker is /v1-done, never /v2-done.
    let (status, body) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200, "relay resumes the pinned instance: {body}");
    assert!(
        wait_until(10, || async { capture.delivered("/v1-done") >= 1 }).await,
        "the resumed instance completes on ITS pinned deployment (v1 model)"
    );
    assert_eq!(
        capture.delivered("/v2-done"),
        0,
        "the v1-pinned instance must never execute v2's model"
    );

    // v1 is now quiescent (no instances, outbox drained) — the db source's own sweep retires it.
    assert!(
        wait_until(20, || async { ready_deployments(addr).await == Some(1) }).await,
        "the drained v1 deployment retires once quiescent on the db source too"
    );

    // ---- fail closed when the pin is genuinely gone -----------------------------------------
    // Park K3 on v2, hot-deploy v3 (so K3's pin drains), then retire v2's row BEHIND the
    // engine's back — something the quiescence gate would never do while an instance is pinned,
    // which is exactly why the relay cannot assume the pin resolves. The relay must refuse the
    // resume; the invariant under test is that NEITHER model runs, not which diagnostic fires
    // (a lost pin surfaces either as an unresolvable pin or as an uncorrelatable relay).
    let (status, body) = post(addr, "/channels/hold-start", br#"{"key":"K3"}"#).await;
    assert_eq!(status, 200, "K3 parks on v2: {body}");
    // Second callout in THIS capture: K1's park was the first, K3's park is the second (the
    // model calls out exactly once per instance, and K1's resume replay-skips its send).
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 2 }).await);

    let (status, body) = post(addr, "/admin/deployments", &v3).await;
    assert_eq!(status, 200, "v3 hot-deploy: {body}");
    let v3_id = json_field(&body, "deploymentId");
    assert!(wait_until(10, || async { ready_deployments(addr).await == Some(2) }).await);

    let v2_id: String = sqlx::query_scalar(
        "SELECT deployment_id FROM deployment_archive \
         WHERE status = 'draining' AND deployment_id <> $1",
    )
    .bind(&v3_id)
    .fetch_one(&pool)
    .await
    .expect("the drained v2 row");
    sqlx::query("UPDATE deployment_archive SET status = 'retired' WHERE deployment_id = $1")
        .bind(&v2_id)
        .execute(&pool)
        .await
        .expect("force-retire the pinned row");
    sqlx::query("SELECT pg_notify('sutra_deployments', 'force-retire')")
        .execute(&pool)
        .await
        .expect("converge");
    assert!(
        wait_until(10, || async { ready_deployments(addr).await == Some(1) }).await,
        "the force-retired v2 definition deregisters (only v3 is left)"
    );

    let (status, _) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K3","status":"done"}"#,
    )
    .await;
    assert_ne!(
        status, 200,
        "a relay whose pinned definition is gone must fail closed, not resume on v3"
    );
    // Give any (wrongly) resumed instance time to reach a done marker before asserting silence.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        capture.delivered("/v3-done"),
        0,
        "the v2-pinned instance must never execute v3's model"
    );
    assert_eq!(
        capture.delivered("/v2-done"),
        0,
        "and it must not have run its own retired model either"
    );
    engine.shutdown().await;
}

// ---- (i) boot survives the accumulated-drain store ------------------------------------------

/// Several DRAINING revisions of ONE slot are a legal store state — only `status='active'`
/// is unique per slot, and interrupted drains accumulate across runs (a k8s IT store built
/// exactly this shape: four draining rows of one slot, no active row). Every revision's
/// bindings differ (a binding carries its manifest-hash deployment id), so the boot-time
/// draining-tail replay used to collide on the channel URN and PANIC the engine actor —
/// after which every operation answered `SUTRA.RUNTIME.UNEXPECTED — engine actor is not
/// running`. Boot must instead register each channel key once (newest draining revision
/// wins — the relay walk's order) and keep serving: fresh deploys land, and once a new
/// ACTIVE revision re-binds the slot's routes (inbound routes always follow the active
/// set — a draining-only slot serves none), the instances pinned to BOTH draining
/// revisions resume, each on its own model.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn boot_survives_multiple_draining_revisions_of_one_slot() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&stateful_app_package(sink, "v1"));
    let v2 = package(&stateful_app_package(sink, "v2"));

    // Build the accumulated shape through the real flows: deploy v1, park K1 on it,
    // hot-deploy v2 (v1 → draining, K1 pins it), park K2 on v2.
    let engine = boot_db_engine(url.clone()).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1 deploy: {body}");
    let (status, _) = post(addr, "/channels/hold-start", br#"{"key":"K1"}"#).await;
    assert_eq!(status, 200);
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 1 }).await);
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 hot-deploy: {body}");
    assert!(wait_until(10, || async { ready_deployments(addr).await == Some(2) }).await);
    let (status, _) = post(addr, "/channels/hold-start", br#"{"key":"K2"}"#).await;
    assert_eq!(status, 200);
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 2 }).await);
    engine.shutdown().await;

    // Demote the active row too: the slot now holds TWO draining revisions and no active —
    // the store an interrupted teardown leaves behind.
    let flipped = sqlx::query("UPDATE deployment_archive SET status = 'draining'")
        .execute(&pool)
        .await
        .expect("flip the active row to draining")
        .rows_affected();
    assert_eq!(flipped, 2, "the slot's two rows, both draining");

    // Reboot on that store — the replay that used to panic the actor.
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    assert!(
        wait_until(10, || async { ready_deployments(addr).await == Some(2) }).await,
        "both draining revisions register (the tail) and the engine reports ready"
    );

    // The k8s symptom, regressed directly: a deploy must land on a LIVE actor. Deploying a
    // NEW revision into the accumulated slot is also the operator's recovery move — it
    // re-binds the slot's routes (inbound routes follow the ACTIVE set; a draining-only
    // slot serves none).
    let v3 = package(&stateful_app_package(sink, "v3"));
    let (status, body) = post(addr, "/admin/deployments", &v3).await;
    assert_eq!(
        status, 200,
        "a fresh deploy lands on the rebooted engine: {body}"
    );
    assert!(
        wait_until(10, || async { ready_deployments(addr).await == Some(3) }).await,
        "v3 active + the two draining revisions"
    );

    // And the tail is FUNCTIONAL, not just skipped: both parked instances resume through
    // the relay's DRAINING-scope walk, each on ITS OWN pinned model.
    let (status, body) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K2","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200, "K2 resumes on the v2 pin: {body}");
    assert!(wait_until(10, || async { capture.delivered("/v2-done") >= 1 }).await);
    let (status, body) = post(
        addr,
        "/channels/hold-response",
        br#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "K1 resumes through the draining-scope walk: {body}"
    );
    assert!(
        wait_until(10, || async { capture.delivered("/v1-done") >= 1 }).await,
        "K1 completes on v1's model"
    );
    engine.shutdown().await;
}
