//! Instance migration (P1-8) end to end, against real PostgreSQL + the real boot path.
//!
//! The unit tests in `sutra_engine::migrate` pin the compatibility MATRIX (they are pure functions
//! over node shapes and need no database), and `sutra_persistence` pins the snapshot key-patch. What
//! only a real engine can prove is the thing the feature actually claims:
//!
//!   an instance parked on v1, after a hot-deploy to a v2 that RENAMED its wait node, can be
//!   migrated onto v2 with an explicit mapping and then COMPLETES ON V2'S MODEL.
//!
//! That last clause is the whole test. `archive_activation_conformance`'s pinned-resume case proves
//! the opposite default — a v1-pinned instance runs v1's model across a flip, never v2's — so this
//! suite is what distinguishes "migration happened" from "the pin quietly held".
//!
//! Also covered here, because they are posture and not arithmetic: the dry run mutates nothing, an
//! identity mapping over a renamed node is REFUSED (never silently accepted), and each refusal class
//! answers with its own structured code.
//!
//! The v2 (F2) section at the foot of this file adds the three follow-ons — the batch's
//! partial-failure contract, a cross-PROCESS re-home that finishes on the target process's model,
//! and a FAILED instance that is migrated onto a repaired model with `resume: true` and then runs to
//! completion through the ordinary timer path.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::config::AdminAuthConfig;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

const API_KEY: &str = "migrate-key";

static SEQ: AtomicU32 = AtomicU32::new(0);

// ---- fixture packages -----------------------------------------------------------------------

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "mig-{name}-{}-{}",
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

/// The stateful app, parameterised by the WAIT NODE's id and the done-marker version.
///
/// `wait_node` is what makes this suite work: v2 uses a different id for the same channel-call
/// task, so an identity migration onto v2 has nowhere to put the parked token and an explicitly
/// mapped one does. The `<q:timeout>` on that task synthesizes a `<waitNode>#timeout` timer
/// boundary, so the parked instance holds BOTH a message locus and a timer locus — the mapping has
/// to carry both, which is exactly the mistake an unvalidated migration would make.
fn hold_package(sink: SocketAddr, wait_node: &str, marker: &str) -> PathBuf {
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
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="{wait_node}"/>
    <bpmn:serviceTask id="{wait_node}" name="Call the partner" implementation="channel:callout">
      <bpmn:extensionElements>
        <q:source channel="hold-response"/>
        <q:alias name="holdKey" expression="payload.key" unique="true"/>
        <q:timeout duration="PT600S"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="{wait_node}" targetRef="DoneNotify"/>
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
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"hold\"\n  \"version\": \"1.0.0\"\n\
         engine:\n  minContract: 1\n",
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

/// Write/replace an archive ATOMICALLY — a poll tick never sees a half-written file.
fn place_archive(dir: &Path, name: &str, bytes: &[u8]) {
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).expect("write temp archive");
    std::fs::rename(&tmp, dir.join(name)).expect("rename into place");
}

// ---- database + engine ------------------------------------------------------------------------

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
        "mig_{}_{}",
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

/// Boot with the admin surface DEV-OPEN — the gate itself is proved in `admin.rs`'s unit tests;
/// this suite is about what the migrate endpoint DOES once a caller is through it.
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
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        external_task: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        // The testcontainers superuser has BYPASSRLS; `rls_bypass_it` proves the enforcement.
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: AdminAuthConfig {
            dev_disabled: true,
            ..AdminAuthConfig::default()
        },
        now_override: None,
    })
    .await
    .expect("engine boots")
}

// ---- tiny blocking HTTP client ------------------------------------------------------------------

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

async fn request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let (method, path, body) = (method.to_string(), path.to_string(), body.to_vec());
    tokio::task::spawn_blocking(move || http_request(addr, &method, &path, &body))
        .await
        .expect("http task")
}

async fn post_json(addr: SocketAddr, path: &str, body: &str) -> (u16, serde_json::Value) {
    let (status, raw) = request(addr, "POST", path, body.as_bytes()).await;
    let json = serde_json::from_str(raw.trim()).unwrap_or(serde_json::Value::Null);
    (status, json)
}

async fn get_json(addr: SocketAddr, path: &str) -> serde_json::Value {
    let (_, raw) = request(addr, "GET", path, b"").await;
    serde_json::from_str(raw.trim()).unwrap_or(serde_json::Value::Null)
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

/// The single SUSPENDED instance, as `(instanceId, deploymentId)`.
async fn only_suspended(addr: SocketAddr) -> (String, String) {
    let list = get_json(addr, "/sutra/instances?status=SUSPENDED").await;
    let rows = list["instances"].as_array().cloned().unwrap_or_default();
    assert_eq!(rows.len(), 1, "exactly one parked instance: {list}");
    (
        rows[0]["instanceId"]
            .as_str()
            .expect("instanceId")
            .to_owned(),
        rows[0]["deploymentId"]
            .as_str()
            .expect("deploymentId")
            .to_owned(),
    )
}

/// The ACTIVE deployment id (there is exactly one archive slot in these fixtures).
async fn active_deployment(addr: SocketAddr) -> String {
    let status = get_json(addr, "/sutra/deployments").await;
    status["active"][0]["deploymentId"]
        .as_str()
        .expect("an active deployment")
        .to_owned()
}

/// Every violation code in a migrate report.
fn codes(report: &serde_json::Value) -> Vec<String> {
    report["violations"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|v| v["code"].as_str().map(str::to_owned))
        .collect()
}

// ---- the capture sink -------------------------------------------------------------------------

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

// ---- the end-to-end -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_migrated_instance_resumes_on_the_target_model_after_a_hot_deploy() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    // v1 parks at `CallOut`; v2 is the SAME flow with that node renamed to `CallOutV2`.
    let v1 = package(&hold_package(sink, "CallOut", "v1"));
    let v2 = package(&hold_package(sink, "CallOutV2", "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "hold.sutra", &v1);

    let engine = boot(dir.clone(), url).await;
    let addr = engine.local_addr;
    let v1_id = active_deployment(addr).await;

    // Park K1 on v1.
    let (status, _) = post_json(addr, "/channels/hold-start", r#"{"key":"K1"}"#).await;
    assert_eq!(status, 200);
    assert!(
        wait_until(15, || async { capture.delivered("/callout") >= 1 }).await,
        "the park step's channel-call request delivers"
    );
    let (instance, pinned) = only_suspended(addr).await;
    assert_eq!(pinned, v1_id, "the instance is pinned to v1");

    // Hot-deploy v2. v1 keeps the instance and goes DRAINING (Wave A P0-3).
    place_archive(&dir, "hold.sutra", &v2);
    assert!(
        wait_until(15, || async {
            let s = get_json(addr, "/sutra/deployments").await;
            s["draining"].as_array().is_some_and(|d| !d.is_empty())
        })
        .await,
        "v1 drains once v2 is active"
    );
    let v2_id = active_deployment(addr).await;
    assert_ne!(v2_id, v1_id, "a renamed node is new content, so a new pin");

    // --- 1. an IDENTITY migration is REFUSED: v2 has no `CallOut` ---------------------------
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","dryRun":true}}"#),
    )
    .await;
    assert_eq!(
        status, 422,
        "identity over a renamed node must refuse: {report}"
    );
    assert_eq!(report["valid"], false);
    assert!(
        codes(&report)
            .iter()
            .any(|c| c == "SUTRA.ADMIN.MIGRATE.NODE_UNMAPPED"),
        "the parked node has nowhere to land: {report}"
    );
    // And the instance is untouched — still parked, still pinned to v1.
    let (_, still_pinned) = only_suspended(addr).await;
    assert_eq!(still_pinned, v1_id, "a refused migration changes nothing");

    // --- 2. the mapped DRY RUN validates and mutates nothing ---------------------------------
    let mapping = r#""nodeMapping":{"CallOut":"CallOutV2","CallOut#timeout":"CallOutV2#timeout"}"#;
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}",{mapping},"dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 200, "the mapped migration validates: {report}");
    assert_eq!(report["valid"], true);
    assert_eq!(report["migrated"], false, "a dry run commits nothing");
    assert_eq!(report["mappingSource"], "explicit");
    // Both loci are reported, each with the construct it landed on.
    let kinds: Vec<String> = report["loci"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|l| l["kind"].as_str().map(str::to_owned))
        .collect();
    assert!(kinds.contains(&"MESSAGE_WAIT".to_owned()), "{report}");
    assert!(kinds.contains(&"TIMER_WAIT".to_owned()), "{report}");
    let (_, still_pinned) = only_suspended(addr).await;
    assert_eq!(still_pinned, v1_id, "a dry run mutates nothing");

    // --- 3. the real migration moves it -----------------------------------------------------
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}",{mapping}}}"#),
    )
    .await;
    assert_eq!(status, 200, "the migration commits: {report}");
    assert_eq!(report["migrated"], true);
    assert_eq!(
        report["resumed"], false,
        "migration re-pins and rewrites; it never advances the instance"
    );
    assert!(
        report["rewrites"]["waitRows"].as_u64().unwrap_or(0) >= 2,
        "both the message park and the synthesized timer park moved: {report}"
    );
    let (_, now_pinned) = only_suspended(addr).await;
    assert_eq!(now_pinned, v2_id, "the instance is re-pinned to v2");

    // The inspect projection agrees, and the frontier carries the NEW node id.
    let inspect = get_json(addr, &format!("/sutra/instances/{instance}")).await;
    assert_eq!(inspect["deploymentId"], v2_id.as_str());
    assert_eq!(inspect["waitingNodes"][0], "CallOutV2");

    // --- 4. and it completes on V2'S MODEL ---------------------------------------------------
    // This is the assertion the whole feature exists for. Without migration the pin holds and the
    // instance would emit /v1-done (that is exactly what the pinned-resume conformance proves).
    let (status, _) = post_json(
        addr,
        "/channels/hold-response",
        r#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        wait_until(15, || async { capture.delivered("/v2-done") >= 1 }).await,
        "the migrated instance completes on the TARGET model"
    );
    assert_eq!(
        capture.delivered("/v1-done"),
        0,
        "it must never execute the model it was migrated off"
    );

    engine.shutdown().await;
}

// ---- the refusal classes ------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn every_refusal_class_answers_with_its_own_structured_code() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let v1 = package(&hold_package(sink, "CallOut", "v1"));
    let v2 = package(&hold_package(sink, "CallOutV2", "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "hold.sutra", &v1);

    let engine = boot(dir.clone(), url).await;
    let addr = engine.local_addr;
    let v1_id = active_deployment(addr).await;

    let (status, _) = post_json(addr, "/channels/hold-start", r#"{"key":"K1"}"#).await;
    assert_eq!(status, 200);
    assert!(wait_until(15, || async { capture.delivered("/callout") >= 1 }).await);
    let (instance, _) = only_suspended(addr).await;

    // (a) an unknown instance id is a 404, not a validation report.
    let (status, _) = post_json(
        addr,
        "/admin/instances/00000000-0000-4000-8000-000000000000/migrate",
        &format!(r#"{{"targetDeploymentId":"{v1_id}"}}"#),
    )
    .await;
    assert_eq!(status, 404);

    // (b) a malformed id is a 400 before anything is resolved.
    let (status, _) = post_json(
        addr,
        "/admin/instances/not-a-uuid/migrate",
        &format!(r#"{{"targetDeploymentId":"{v1_id}"}}"#),
    )
    .await;
    assert_eq!(status, 400);

    // (c) a missing target is a 400.
    let (status, _) = post_json(addr, &format!("/admin/instances/{instance}/migrate"), "{}").await;
    assert_eq!(status, 400);

    // (d) an unknown deployment id is TARGET_NOT_ACTIVE.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        r#"{"targetDeploymentId":"dep-000000000000000000000099"}"#,
    )
    .await;
    assert_eq!(status, 422);
    assert_eq!(codes(&report), ["SUTRA.ADMIN.MIGRATE.TARGET_NOT_ACTIVE"]);

    // (e) migrating onto the pin it already has is refused rather than silently rewritten.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v1_id}"}}"#),
    )
    .await;
    assert_eq!(status, 422);
    assert_eq!(
        codes(&report),
        ["SUTRA.ADMIN.MIGRATE.TARGET_SAME_AS_SOURCE"]
    );

    // Flip to v2 so v1 becomes DRAINING.
    place_archive(&dir, "hold.sutra", &v2);
    assert!(
        wait_until(15, || async {
            let s = get_json(addr, "/sutra/deployments").await;
            s["draining"].as_array().is_some_and(|d| !d.is_empty())
        })
        .await
    );
    let v2_id = active_deployment(addr).await;

    // (f) a DRAINING target is refused — it retires the moment it is quiescent, so migrating onto
    //     it would strand the instance again.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v1_id}"}}"#),
    )
    .await;
    assert_eq!(status, 422, "{report}");
    assert_eq!(codes(&report), ["SUTRA.ADMIN.MIGRATE.TARGET_NOT_ACTIVE"]);

    // (g) a mapping entry naming a node the instance does not pin is refused, not ignored: a typo
    //     must never read as "identity mapping, then".
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(
            r#"{{"targetDeploymentId":"{v2_id}","nodeMapping":{{"CallOutTypo":"CallOutV2"}},"dryRun":true}}"#
        ),
    )
    .await;
    assert_eq!(status, 422);
    assert!(codes(&report)
        .iter()
        .any(|c| c == "SUTRA.ADMIN.MIGRATE.MAPPING_INVALID"));

    // (h) a locus mapped onto an INCOMPATIBLE construct is refused — `Start` exists in v2, but a
    //     parked message wait cannot resume at a start event.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(
            r#"{{"targetDeploymentId":"{v2_id}","nodeMapping":{{"CallOut":"Start","CallOut#timeout":"CallOutV2#timeout"}},"dryRun":true}}"#
        ),
    )
    .await;
    assert_eq!(status, 422);
    assert!(codes(&report)
        .iter()
        .any(|c| c == "SUTRA.ADMIN.MIGRATE.NODE_INCOMPATIBLE"));

    // (i) a TERMINAL instance is history, not live state. Cancel it, then try.
    let (status, _) = request(
        addr,
        "POST",
        &format!("/sutra/instances/{instance}/cancel"),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 422, "{report}");
    assert!(codes(&report)
        .iter()
        .any(|c| c == "SUTRA.ADMIN.MIGRATE.INSTANCE_TERMINAL"));

    engine.shutdown().await;
}

// ================================ v2 (F2) =====================================================
//
// Three follow-ons, each with exactly one thing only a real engine can settle:
//
// * **batch** — the PARTIAL-FAILURE contract. A population where one instance moves, one is
//   claim-held and one is terminal must produce one report with three different verdicts, and the
//   two that did not move must be untouched down to their rows (per-instance transactions, not one
//   big one).
// * **cross-process** — an instance re-homed into a DIFFERENT process id completes on THAT
//   process's model, which is only observable end to end.
// * **migrate-then-resume** — a FAILED instance, moved onto a repaired model with `resume: true`,
//   comes back through the ORDINARY timer path and runs to completion with no further input.

/// The re-homing target: the same flow under a DIFFERENT process id, with every node id changed
/// too — so a cross-process migration cannot pass by accident and must name each locus by hand.
/// The channel names are unchanged (they are the deployment's intake surface, not the process's).
fn rehomed_package(sink: SocketAddr, marker: &str) -> PathBuf {
    let root = temp_root("pkg-rehome");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_hold"
                  targetNamespace="urn:sutra:module:hold:1.0.0">
  <bpmn:process id="holdtwo" name="Hold, re-homed" isExecutable="true">
    <bpmn:startEvent id="StartX">
      <bpmn:extensionElements><q:source channel="hold-start"/></bpmn:extensionElements>
      <bpmn:outgoing>g1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="g1" sourceRef="StartX" targetRef="CallOutX"/>
    <bpmn:serviceTask id="CallOutX" name="Call the partner" implementation="channel:callout">
      <bpmn:extensionElements>
        <q:source channel="hold-response"/>
        <q:alias name="holdKey" expression="payload.key" unique="true"/>
        <q:timeout duration="PT600S"/>
      </bpmn:extensionElements>
      <bpmn:incoming>g1</bpmn:incoming>
      <bpmn:outgoing>g2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="g2" sourceRef="CallOutX" targetRef="DoneNotifyX"/>
    <bpmn:sendTask id="DoneNotifyX" name="Version-marked done">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-done"/>
      </bpmn:extensionElements>
      <bpmn:incoming>g2</bpmn:incoming>
      <bpmn:outgoing>g3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="g3" sourceRef="DoneNotifyX" targetRef="EndX"/>
    <bpmn:endEvent id="EndX"><bpmn:incoming>g3</bpmn:incoming></bpmn:endEvent>
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
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"hold\"\n  \"version\": \"1.0.0\"\n\
         engine:\n  minContract: 1\n",
    );
    root
}

/// A flow that FAILS FATALLY after a timer fires, and its repair.
///
/// `Start → Wait (timer PT2S) → Decide (exclusive gateway) → DoneNotify → End`. The broken build
/// gives `Decide` one never-satisfied condition and NO default flow, which is an uncaught fatal at
/// the moment the timer resumes it — the instance dies durably FAILED, parked at `Wait`, with the
/// timer row torn down by the failure commit. The repaired build differs in exactly one place: the
/// gateway gets its default. Node ids are IDENTICAL, so the migration off the broken model onto the
/// repaired one is an identity mapping — which is what a model REPAIR actually looks like.
fn repair_package(sink: SocketAddr, marker: &str, fixed: bool) -> PathBuf {
    let root = temp_root("pkg-repair");
    let gateway = if fixed {
        r#"<bpmn:exclusiveGateway id="Decide" name="Ready?" default="f3">
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="f3" sourceRef="Decide" targetRef="DoneNotify"/>"#
    } else {
        r#"<bpmn:exclusiveGateway id="Decide" name="Ready?">
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="f3" sourceRef="Decide" targetRef="DoneNotify">
      <bpmn:conditionExpression>1 = 2</bpmn:conditionExpression>
    </bpmn:sequenceFlow>"#
    };
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_repair"
                  targetNamespace="urn:sutra:module:repair:1.0.0">
  <bpmn:process id="repair" name="Fails after a timer" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="repair-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Wait"/>
    <bpmn:intermediateCatchEvent id="Wait" name="Settle">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT2S</bpmn:timeDuration></bpmn:timerEventDefinition>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="Decide"/>
    {gateway}
    <bpmn:sendTask id="DoneNotify" name="Version-marked done">
      <bpmn:extensionElements>
        <q:send destination="http://{sink}/{marker}-done"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming>
      <bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f4" sourceRef="DoneNotify" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: repair-start
    transport: http
    bind: "POST /channels/repair-start"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: {API_KEY}
        header: X-Api-Key
"#
    );
    write(&root, "bpmn/repair.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"repair\"\n  \"version\": \"1.0.0\"\n\
         engine:\n  minContract: 1\n",
    );
    root
}

/// Every instance id in a given snapshot status.
async fn instances_in(addr: SocketAddr, status: &str) -> Vec<String> {
    let list = get_json(addr, &format!("/sutra/instances?status={status}")).await;
    list["instances"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|row| row["instanceId"].as_str().map(str::to_owned))
        .collect()
}

/// Start one instance on `hold-start` and return the id it parked under (the one that was not
/// already parked). Deliveries are counted so the assertion never races the park step.
async fn park_one(
    addr: SocketAddr,
    capture: &Capture,
    key: &str,
    expect_callouts: usize,
) -> String {
    let before: std::collections::BTreeSet<String> =
        instances_in(addr, "SUSPENDED").await.into_iter().collect();
    let (status, _) = post_json(
        addr,
        "/channels/hold-start",
        &format!(r#"{{"key":"{key}"}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        wait_until(15, || async {
            capture.delivered("/callout") >= expect_callouts
        })
        .await,
        "the park step's channel-call request delivers for {key}"
    );
    let expected = before.len() + 1;
    assert!(
        wait_until(15, || async {
            instances_in(addr, "SUSPENDED").await.len() == expected
        })
        .await,
        "{key} parks"
    );
    instances_in(addr, "SUSPENDED")
        .await
        .into_iter()
        .find(|id| !before.contains(id))
        .expect("the newly parked instance")
}

/// The batch report's entry for one instance.
fn entry_for<'a>(report: &'a serde_json::Value, instance: &str) -> &'a serde_json::Value {
    report["instances"]
        .as_array()
        .expect("instances[]")
        .iter()
        .find(|e| e["instanceId"] == instance)
        .unwrap_or_else(|| panic!("no entry for {instance} in {report}"))
}

/// How many `waiting_event` rows the instance holds under `deployment` — the row-level half of
/// "this instance did not move".
async fn wait_rows(pool: &PgPool, deployment: &str, instance: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM waiting_event WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(deployment)
    .bind(uuid::Uuid::parse_str(instance).expect("uuid"))
    .fetch_one(pool)
    .await
    .expect("count wait rows")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_batch_migrates_what_it_can_and_reports_per_instance_what_it_could_not() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let v1 = package(&hold_package(sink, "CallOut", "v1"));
    let v2 = package(&hold_package(sink, "CallOut", "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "hold.sutra", &v1);

    let engine = boot(dir.clone(), url).await;
    let addr = engine.local_addr;
    let v1_id = active_deployment(addr).await;

    // A mixed population on v1: one that will migrate, one whose claim is held, one terminal.
    let movable = park_one(addr, &capture, "K1", 1).await;
    let contended = park_one(addr, &capture, "K2", 2).await;
    let terminal = park_one(addr, &capture, "K3", 3).await;

    // Hot-deploy v2 (same node ids, different `<q:send>` marker) — v1 goes DRAINING with all three.
    place_archive(&dir, "hold.sutra", &v2);
    assert!(
        wait_until(15, || async {
            let s = get_json(addr, "/sutra/deployments").await;
            s["draining"].as_array().is_some_and(|d| !d.is_empty())
        })
        .await,
        "v1 drains once v2 is active"
    );
    let v2_id = active_deployment(addr).await;

    // K3 becomes retained history; K2's ownership claim is taken by "another replica".
    let (status, _) = request(
        addr,
        "POST",
        &format!("/sutra/instances/{terminal}/cancel"),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    sqlx::query(
        "UPDATE instance_state SET claim_owner = $1, claimed_at = now(), \
         last_heartbeat_at = now() WHERE instance_id = $2",
    )
    .bind("replica-elsewhere")
    .bind(uuid::Uuid::parse_str(&contended).unwrap())
    .execute(&pool)
    .await
    .expect("steal the claim");

    // --- 1. a DRY RUN over the batch validates every instance and moves nothing ---------------
    let body = format!(
        r#"{{"targetDeploymentId":"{v2_id}","dryRun":true,
             "filter":{{"sourceDeploymentId":"{v1_id}","includeTerminal":true}}}}"#
    );
    let (status, report) = post_json(addr, "/admin/instances/migrate", &body).await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["selected"], 3, "{report}");
    assert_eq!(report["totals"]["valid"], 2, "the two live ones validate");
    assert_eq!(report["totals"]["migrated"], 0, "a dry run commits nothing");
    assert_eq!(
        entry_for(&report, &contended)["outcome"],
        "VALID",
        "a dry run takes no claim, so contention cannot even arise"
    );
    assert_eq!(entry_for(&report, &terminal)["outcome"], "REFUSED");

    // --- 2. the real batch: three instances, three different verdicts, one report -------------
    let body = format!(
        r#"{{"targetDeploymentId":"{v2_id}",
             "filter":{{"sourceDeploymentId":"{v1_id}","includeTerminal":true}}}}"#
    );
    let (status, report) = post_json(addr, "/admin/instances/migrate", &body).await;
    assert_eq!(
        status, 200,
        "the batch RAN — a per-instance refusal is data, not an HTTP error: {report}"
    );
    assert_eq!(report["selected"], 3);
    assert_eq!(report["totals"]["migrated"], 1, "{report}");
    assert_eq!(report["totals"]["bounced"], 1, "{report}");
    assert_eq!(report["totals"]["refused"], 1, "{report}");
    assert_eq!(report["totals"]["resumed"], 0);

    let moved = entry_for(&report, &movable);
    assert_eq!(moved["outcome"], "MIGRATED");
    assert_eq!(moved["migrated"], true);
    assert_eq!(moved["toDeploymentId"], v2_id.as_str());

    let bounced = entry_for(&report, &contended);
    assert_eq!(bounced["outcome"], "BOUNCED");
    assert_eq!(
        bounced["retrySafe"], true,
        "the caller re-runs; nothing was written"
    );
    assert_eq!(
        bounced["violations"][0]["code"],
        "SUTRA.ADMIN.MIGRATE.CLAIM_HELD"
    );
    assert!(
        report["note"]
            .as_str()
            .unwrap_or_default()
            .contains("BOUNCED"),
        "and the batch note says what to do about it: {report}"
    );

    let refused = entry_for(&report, &terminal);
    assert_eq!(refused["outcome"], "REFUSED");
    assert_eq!(refused["retrySafe"], false);
    assert!(
        codes(refused)
            .iter()
            .any(|c| c == "SUTRA.ADMIN.MIGRATE.INSTANCE_TERMINAL"),
        "an explicit refusal, not a silent omission: {refused}"
    );

    // --- 3. per-instance atomicity: each one is FULLY moved or COMPLETELY untouched -----------
    assert_eq!(
        wait_rows(&pool, &v1_id, &movable).await,
        0,
        "the migrated instance left no rows behind"
    );
    assert!(
        wait_rows(&pool, &v2_id, &movable).await >= 2,
        "…and its parks landed whole under the target"
    );
    assert_eq!(
        wait_rows(&pool, &v2_id, &contended).await,
        0,
        "a bounced instance wrote NOTHING under the target"
    );
    assert!(
        wait_rows(&pool, &v1_id, &contended).await >= 2,
        "…and kept every row under its own pin"
    );
    assert_eq!(wait_rows(&pool, &v2_id, &terminal).await, 0);
    let still_on_v1 = get_json(addr, &format!("/sutra/instances/{contended}")).await;
    assert_eq!(still_on_v1["deploymentId"], v1_id.as_str());

    // --- 4. `resume` on a live instance is a validation error, not a no-op --------------------
    let (status, single) = post_json(
        addr,
        &format!("/admin/instances/{contended}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","resume":true,"dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 422, "{single}");
    assert!(codes(&single)
        .iter()
        .any(|c| c == "SUTRA.ADMIN.MIGRATE.RESUME_NOT_FAILED"));

    // --- 5. and the one that moved completes on V2'S model ------------------------------------
    let (status, _) = post_json(
        addr,
        "/channels/hold-response",
        r#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        wait_until(15, || async { capture.delivered("/v2-done") >= 1 }).await,
        "the migrated instance completes on the TARGET model"
    );
    assert_eq!(capture.delivered("/v1-done"), 0);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_cross_process_migration_completes_on_the_target_processs_model() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    // v1 runs process `hold`; v2 runs `holdtwo` — a different process id AND different node ids.
    let v1 = package(&hold_package(sink, "CallOut", "v1"));
    let v2 = package(&rehomed_package(sink, "v2"));
    let dir = temp_root("dir");
    place_archive(&dir, "hold.sutra", &v1);

    let engine = boot(dir.clone(), url).await;
    let addr = engine.local_addr;
    let v1_id = active_deployment(addr).await;

    let (status, _) = post_json(addr, "/channels/hold-start", r#"{"key":"K1"}"#).await;
    assert_eq!(status, 200);
    assert!(wait_until(15, || async { capture.delivered("/callout") >= 1 }).await);
    let (instance, pinned) = only_suspended(addr).await;
    assert_eq!(pinned, v1_id);

    place_archive(&dir, "hold.sutra", &v2);
    assert!(
        wait_until(15, || async {
            let s = get_json(addr, "/sutra/deployments").await;
            s["draining"].as_array().is_some_and(|d| !d.is_empty())
        })
        .await
    );
    let v2_id = active_deployment(addr).await;

    // (a) without a targetProcessId the re-home is not even attempted: `hold` is absent from v2.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 422, "{report}");
    assert_eq!(codes(&report), ["SUTRA.ADMIN.MIGRATE.PROCESS_ABSENT"]);
    assert!(
        report["violations"][0]["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("holdtwo"),
        "the refusal names what the target DOES declare: {report}"
    );

    // (b) naming the target process is not enough — identity is never implicit across processes.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","targetProcessId":"holdtwo","dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 422, "{report}");
    assert!(
        codes(&report)
            .iter()
            .any(|c| c == "SUTRA.ADMIN.MIGRATE.CROSS_PROCESS_UNMAPPED"),
        "{report}"
    );

    // (c) the fully-mapped re-home commits, and the report names both processes.
    let mapping = r#""nodeMapping":{"CallOut":"CallOutX","CallOut#timeout":"CallOutX#timeout","Start":"StartX"}"#;
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{v2_id}","targetProcessId":"holdtwo",{mapping}}}"#),
    )
    .await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["migrated"], true);
    assert_eq!(report["crossProcess"], true);
    assert_eq!(report["fromProcessId"], "hold");
    assert_eq!(report["toProcessId"], "holdtwo");
    assert_eq!(report["resumed"], false, "a re-home is not a resume");

    let inspect = get_json(addr, &format!("/sutra/instances/{instance}")).await;
    assert_eq!(inspect["deploymentId"], v2_id.as_str());
    assert_eq!(inspect["waitingNodes"][0], "CallOutX");

    // (d) the whole point: it resumes on the TARGET PROCESS's graph and finishes there.
    let (status, _) = post_json(
        addr,
        "/channels/hold-response",
        r#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        wait_until(20, || async { capture.delivered("/v2-done") >= 1 }).await,
        "the re-homed instance completes on the target PROCESS's model"
    );
    assert_eq!(capture.delivered("/v1-done"), 0);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_failed_instance_migrated_with_resume_runs_to_completion_on_the_repaired_model() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;

    let broken = package(&repair_package(sink, "v1", false));
    let fixed = package(&repair_package(sink, "v2", true));
    let dir = temp_root("dir");
    place_archive(&dir, "repair.sutra", &broken);

    let engine = boot(dir.clone(), url).await;
    let addr = engine.local_addr;
    let broken_id = active_deployment(addr).await;

    // Start it, let the timer fire, and let the broken gateway kill it.
    let (status, _) = post_json(addr, "/channels/repair-start", r#"{"ref":"R-1"}"#).await;
    assert_eq!(status, 200);
    assert!(
        wait_until(30, || async {
            !instances_in(addr, "FAILED").await.is_empty()
        })
        .await,
        "the timer fires into a gateway with no satisfiable flow — the instance dies FAILED"
    );
    let failed = instances_in(addr, "FAILED").await;
    assert_eq!(failed.len(), 1);
    let instance = failed[0].clone();
    let dead = get_json(addr, &format!("/sutra/instances/{instance}")).await;
    assert_eq!(dead["status"], "FAILED");
    assert_eq!(
        dead["waitingNodes"][0], "Wait",
        "it kept the frontier it died at: {dead}"
    );
    assert_eq!(
        capture.delivered("/v1-done"),
        0,
        "nothing ran past the gateway"
    );

    // Repair the model. Same node ids — a repair, not a redesign — so the move is identity-mapped.
    place_archive(&dir, "repair.sutra", &fixed);
    assert!(
        wait_until(20, || async {
            let s = get_json(addr, "/sutra/deployments").await;
            s["draining"].as_array().is_some_and(|d| !d.is_empty())
        })
        .await,
        "the broken deployment drains once the repair is active"
    );
    let fixed_id = active_deployment(addr).await;
    assert_ne!(fixed_id, broken_id);

    // A migration WITHOUT resume leaves it dead — re-pinning never advances an instance.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{fixed_id}","dryRun":true}}"#),
    )
    .await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["resumed"], false);
    assert_eq!(
        report["loci"][0]["kind"], "TIMER_WAIT",
        "a dead instance's park is still classified from its ROWS, not guessed: {report}"
    );

    // Migrate-then-resume.
    let (status, report) = post_json(
        addr,
        &format!("/admin/instances/{instance}/migrate"),
        &format!(r#"{{"targetDeploymentId":"{fixed_id}","resume":true}}"#),
    )
    .await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["migrated"], true);
    assert_eq!(report["resumed"], true, "{report}");
    assert!(
        report["rewrites"]["rearmedParks"].as_u64().unwrap_or(0) >= 1,
        "the park the failure tore down came back: {report}"
    );

    // And now the ORDINARY timer path takes it from there — no further input of any kind.
    assert!(
        wait_until(30, || async { capture.delivered("/v2-done") >= 1 }).await,
        "the revived instance is re-driven by the ordinary timer poller and completes on the \
         repaired model"
    );
    assert_eq!(capture.delivered("/v1-done"), 0);
    assert!(
        wait_until(15, || async {
            instances_in(addr, "FAILED").await.is_empty()
        })
        .await,
        "and it is no longer a FAILED instance"
    );

    engine.shutdown().await;
}
