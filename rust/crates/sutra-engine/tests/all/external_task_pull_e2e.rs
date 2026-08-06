//! External-task (PULL) end-to-end slice: the FULL engine (`serve`) boots a two-process
//! deployment whose first process emits `<q:send channel="to-worker">` onto a
//! `transport: pull` channel. Instead of dialing anything, the relay PARKS the delivery as a
//! fetchable task; a worker drives the real `/sutra/external-tasks/*` routes to fetch-and-lock
//! it and complete it with a result; the completion re-enters the engine through the ORDINARY
//! inbound path and starts the second process, whose own HTTP send proves the worker's payload
//! actually flowed through execution.
//!
//! What this pins, beyond "it works": the worker's result reaches the next process (no new
//! resume entry point — the completion is an ordinary delivery), a locked task is invisible to
//! a second worker, the long poll returns an empty list on timeout rather than hanging, the
//! bounds are rejected loudly rather than clamped, a foreign worker fails closed with a
//! structured code, and the completed row is gone.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;

use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};

// ---- PG container (the sutra-persistence pg-suite pattern) ---------------------------------

static CONTAINER: OnceLock<(
    testcontainers::Container<testcontainers_modules::postgres::Postgres>,
    u16,
)> = OnceLock::new();

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        std::thread::spawn(|| {
            use testcontainers::runners::SyncRunner;
            use testcontainers::ImageExt;
            let container = testcontainers_modules::postgres::Postgres::default()
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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf()
}

fn shipped_migration_roots_env() -> String {
    let shipped = repo_root().join("rust/crates/sutra-persistence/migrations/shipped");
    ["core", "audit", "deploy"]
        .iter()
        .map(|family| shipped.join(family).display().to_string())
        .collect::<Vec<_>>()
        .join(":")
}

// ---- the synthesized two-process deployment ------------------------------------------------

const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:pull-demo:1.0.0">
  <bpmn:process id="ask">
    <bpmn:startEvent id="S1">
      <bpmn:extensionElements><q:source channel="pull-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Ask" implementation="ask.hbs">
      <bpmn:extensionElements>
        <q:send channel="to-worker" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E1"/>
    <bpmn:sequenceFlow id="a1" sourceRef="S1" targetRef="Ask"/>
    <bpmn:sequenceFlow id="a2" sourceRef="Ask" targetRef="E1"/>
  </bpmn:process>
  <bpmn:process id="consume">
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements><q:source channel="score-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Cb" implementation="cb.hbs">
      <bpmn:extensionElements>
        <q:send channel="cb-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="c1" sourceRef="S2" targetRef="Cb"/>
    <bpmn:sequenceFlow id="c2" sourceRef="Cb" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const CHANNELS_YAML: &str = r#"channels:
  # Async intake — 202 Accepted; the work leaves the box as a parked external task.
  - name: pull-in
    transport: http
    bind: "POST /channels/pull-in"
    ack-mode: on-persist
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: pull-demo-key
        header: X-Api-Key

  # The internal channel the worker's COMPLETION lands on — the same shape a local:// hop
  # targets, because that is exactly what a completed pull task is.
  - name: score-in
    transport: local

  # DECLARED OUTBOUND, transport: pull — <q:send channel="to-worker"> parks here for a worker
  # instead of dialing anything, and the worker's result is delivered to score-in.
  - name: to-worker
    direction: outbound
    transport: pull
    bind: pull://score-in

  - name: cb-out
    direction: outbound
    transport: http
    bind: "http://${PULL_CALLBACK_HOST}/cb"
"#;

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn synthesize_deployments_dir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("pull-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let pkg = base.join("default--pull-demo--1.0.0");
    write(pkg.join("bpmn/pull-demo.bpmn"), BPMN);
    // The request handed to the worker carries the intake's own reference…
    write(
        pkg.join("templates/ask.hbs"),
        "{\"ask\":\"{{payload.ref}}\"}",
    );
    // …and the second process's callback carries the WORKER'S result, which is what proves the
    // completion payload travelled the ordinary inbound path into a real execution.
    write(
        pkg.join("templates/cb.hbs"),
        "{\"scored\":\"{{payload.score}}\"}",
    );
    write(pkg.join("channels.yaml"), CHANNELS_YAML);
    write(
        pkg.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"pull-demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let out = base.join("archives");
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(&pkg, &out, &sutra_loader::PackageOptions::default())
        .expect("synthetic package seals into one .sutra archive");
    out
}

// ---- callback capture ----------------------------------------------------------------------

type CapturedRequest = (BTreeMap<String, String>, Vec<u8>);

#[derive(Clone, Default)]
struct Capture {
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
}

async fn capture_handler(
    State(state): State<Capture>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let mut map = BTreeMap::new();
    for (name, value) in &headers {
        if let Ok(v) = value.to_str() {
            map.insert(name.as_str().to_string(), v.to_string());
        }
    }
    state.requests.lock().unwrap().push((map, body.to_vec()));
    StatusCode::ACCEPTED
}

async fn callback_server() -> (SocketAddr, Capture) {
    let capture = Capture::default();
    let app = Router::new()
        .route("/cb", post(capture_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, capture)
}

// ---- minimal blocking HTTP POST (dependency-free, the smoke.rs style) ----------------------

/// POST `body` as JSON and answer `(status, response body)`. `api_key` is the channel intake's
/// header; the worker routes are operate-posture and take none.
fn http_post(addr: SocketAddr, path: &str, body: &str, api_key: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let key_header = api_key
        .map(|k| format!("X-Api-Key: {k}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{key_header}\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    // Chunked or not, the JSON object we care about starts at the first '{' after the headers.
    let payload = response
        .split_once("\r\n\r\n")
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_default();
    let json = match (payload.find('{'), payload.rfind('}')) {
        (Some(start), Some(end)) if end >= start => payload[start..=end].to_string(),
        _ => payload,
    };
    (status, json)
}

async fn post_json(addr: SocketAddr, path: String, body: String) -> (u16, serde_json::Value) {
    let (status, text) = tokio::task::spawn_blocking(move || http_post(addr, &path, &body, None))
        .await
        .unwrap();
    let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, value)
}

// ---- the slice ------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_parked_task_is_fetched_completed_and_resumes_through_the_inbound_path() {
    let pg_port = container_port();
    {
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
            ))
            .await
            .expect("admin pool");
        sqlx::query("CREATE DATABASE pull_e2e")
            .execute(&admin)
            .await
            .expect("create database");
    }
    let datasource_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/pull_e2e");

    let (callback_addr, capture) = callback_server().await;
    std::env::set_var("PULL_CALLBACK_HOST", callback_addr.to_string());
    std::env::set_var("SUTRA_DB_MIGRATIONS", shipped_migration_roots_env());

    let engine = serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(synthesize_deployments_dir()),
        deployments_poll_interval: std::time::Duration::from_secs(2),
        http_port: 0,
        datasource_url: Some(datasource_url.clone()),
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
        now_override: None,
    })
    .await
    .expect("engine boots");
    let addr = engine.local_addr;

    // 1. Async intake starts the first process, whose send parks as an external task.
    let (status, _) = tokio::task::spawn_blocking(move || {
        http_post(
            addr,
            "/channels/pull-in",
            "{\"ref\":\"R-1\"}",
            Some("pull-demo-key"),
        )
    })
    .await
    .unwrap();
    assert_eq!(status, 202, "ack-mode on-persist answers 202");

    // 2. Bounds are rejected LOUDLY, not clamped — a worker that thinks it holds a longer lock
    //    than it does is a duplicate-execution bug.
    let (status, body) = post_json(
        addr,
        "/sutra/external-tasks/fetch-and-lock".to_string(),
        r#"{"workerId":"worker-1","channels":["score-in"],"lockDuration":"30"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 400, "a non-ISO-8601 lockDuration is a 400: {body}");
    assert_eq!(body["code"], "SUTRA.EXTERNAL_TASK.REQUEST_INVALID");

    let (status, body) = post_json(
        addr,
        "/sutra/external-tasks/fetch-and-lock".to_string(),
        r#"{"workerId":"worker-1","channels":["score-in"],"lockDuration":"PT99H"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 400, "a lockDuration above the ceiling is a 400");
    assert_eq!(body["code"], "SUTRA.EXTERNAL_TASK.REQUEST_INVALID");

    // 3. The long poll: the fetch may arrive before the outbox tick has parked anything, and
    //    must wake on the park rather than answering empty.
    let (status, body) = post_json(
        addr,
        "/sutra/external-tasks/fetch-and-lock".to_string(),
        r#"{"workerId":"worker-1","channels":["score-in"],"lockDuration":"PT60S",
            "maxTasks":10,"asyncResponseTimeout":"PT20S"}"#
            .to_string(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let tasks = body["tasks"].as_array().cloned().unwrap_or_default();
    assert_eq!(tasks.len(), 1, "exactly one task was parked: {body}");
    let task = &tasks[0];
    let task_id = task["id"].as_str().expect("task id").to_string();
    assert_eq!(task["channel"], "score-in");
    assert_eq!(
        task["payload"], "{\"ask\":\"R-1\"}",
        "the worker receives the rendered request payload"
    );
    assert_eq!(task["contentType"], "application/json");
    assert_eq!(task["retries"], 3, "the default failure budget");
    assert_eq!(task["attempts"], 1);
    assert!(
        task["idempotencyKey"]
            .as_str()
            .is_some_and(|k| !k.is_empty()),
        "the delivery's idempotency key reaches the worker"
    );

    // 4. A locked task is invisible to every other worker, and the long poll ANSWERS on timeout
    //    rather than hanging.
    let started = std::time::Instant::now();
    let (status, body) = post_json(
        addr,
        "/sutra/external-tasks/fetch-and-lock".to_string(),
        r#"{"workerId":"worker-2","channels":["score-in"],"lockDuration":"PT30S",
            "asyncResponseTimeout":"PT2S"}"#
            .to_string(),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["tasks"].as_array().is_some_and(|t| t.is_empty()),
        "a held lock hides the task: {body}"
    );
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the long poll actually waited"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(15),
        "and it is BOUNDED by asyncResponseTimeout, never an open-ended hang"
    );

    // 5. A foreign worker fails CLOSED — never a 200 it could read as success.
    let complete_path = format!("/sutra/external-tasks/{task_id}/complete");
    let (status, body) = post_json(
        addr,
        complete_path.clone(),
        r#"{"workerId":"worker-2","result":"{}"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 409, "a foreign worker cannot complete: {body}");
    assert_eq!(body["code"], "SUTRA.EXTERNAL_TASK.LOCK_HELD");

    // Nor fail it — the same guard covers both terminal operations.
    let (status, body) = post_json(
        addr,
        format!("/sutra/external-tasks/{task_id}/failure"),
        r#"{"workerId":"worker-2","errorMessage":"not mine"}"#.to_string(),
    )
    .await;
    assert_eq!(
        status, 409,
        "a foreign worker cannot fail it either: {body}"
    );
    assert_eq!(body["code"], "SUTRA.EXTERNAL_TASK.LOCK_HELD");

    // 6. The owner completes with a result — which must re-enter the ORDINARY inbound path.
    let (status, body) = post_json(
        addr,
        complete_path.clone(),
        r#"{"workerId":"worker-1","result":"{\"score\":\"700\"}","contentType":"application/json"}"#
            .to_string(),
    )
    .await;
    assert_eq!(status, 200, "the completion is accepted: {body}");
    assert_eq!(body["status"], "COMPLETED");
    assert_eq!(body["channel"], "score-in");

    // 7. The second process ran on that completion and its own send carried the WORKER'S result.
    for _ in 0..150 {
        if !capture.requests.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let requests = capture.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "the completion started the consuming process exactly once"
    );
    assert_eq!(
        String::from_utf8(requests[0].1.clone()).unwrap(),
        "{\"scored\":\"700\"}",
        "the worker's result travelled the ordinary inbound path into real execution"
    );

    // 8. The completed row is gone, and the same completion no longer resolves.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&datasource_url)
        .await
        .expect("verify pool");
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM external_task")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "a completed task is deleted");

    let (status, body) = post_json(
        addr,
        complete_path,
        r#"{"workerId":"worker-1","result":"{}"}"#.to_string(),
    )
    .await;
    assert_eq!(status, 404, "a completed task is no longer completable");
    assert_eq!(body["code"], "SUTRA.EXTERNAL_TASK.NOT_FOUND");
}
