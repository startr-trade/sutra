//! Outbox end-to-end slice: the FULL engine (`serve`) boots a money-transfer-style
//! resource tree whose flow does THREE `<q:send channel="wsa-out">` emissions (the
//! multi-emission posture — no last-wins), against real PG persistence. An
//! inbound POST completes the flow; the transactional terminal step commits all three outbox
//! rows atomically; the outbox dispatcher tick claims them and the HTTP sink delivers
//! each to a local axum listener with the frozen `Idempotency-Key` wire header and the
//! propagated `traceparent`.

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

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

// ---- PG container ----------------------------------------------------------------------------

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

/// The FULL shipped migration set as a ':'-joined `SUTRA_DB_MIGRATIONS` value
/// (core:audit:deploy). Every IT that boots an engine writes this SAME value, so the
/// process-global env var is race-benign across threaded tests — and the `db` deployment
/// source gets its `deployment_archive` table (the `deploy` family) like the image does.
pub fn shipped_migration_roots_env() -> String {
    let shipped = repo_root().join("rust/crates/sutra-persistence/migrations/shipped");
    ["core", "audit", "deploy"]
        .iter()
        .map(|family| shipped.join(family).display().to_string())
        .collect::<Vec<_>>()
        .join(":")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf()
}

// ---- the synthesized resource tree (money-transfer layout) ------------------------------------

const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:wsa-demo:1.0.0">
  <bpmn:process id="emit-three">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="wsa-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T1" implementation="cb-one.hbs">
      <bpmn:extensionElements>
        <q:send channel="wsa-out" mode="native" contentType="application/xml"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="T2" implementation="cb-two.hbs">
      <bpmn:extensionElements>
        <q:send channel="wsa-out" mode="native" contentType="application/xml"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="T3" implementation="cb-three.hbs">
      <bpmn:extensionElements>
        <q:send channel="wsa-out" mode="native" contentType="application/xml"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T1" targetRef="T2"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T2" targetRef="T3"/>
    <bpmn:sequenceFlow id="f4" sourceRef="T3" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const CHANNELS_YAML: &str = r#"channels:
  # Async intake — 202 Accepted; every response is emitted out-of-band via wsa-out.
  - name: wsa-in
    transport: http
    bind: "POST /channels/wsa-in"
    ack-mode: on-persist
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: wsa-demo-key
        header: X-Api-Key

  # DECLARED OUTBOUND — <q:send channel="wsa-out"> emits here; the callback host is a
  # 15-factor ${ENV} reference resolved at startup (the outbound-http posture).
  - name: wsa-out
    direction: outbound
    transport: http
    bind: "http://${WSA_CALLBACK_HOST}/wsa-callback"
"#;

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build a synthetic standalone deployment-package directory, seal it into one `.sutra`,
/// and return the archives directory the engine watches (`SUTRA_DEPLOYMENTS_DIR`).
fn synthesize_deployments_dir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("wsa-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let pkg = base.join("default--wsa-demo--1.0.0");
    write(pkg.join("bpmn/emit-three.bpmn"), BPMN);
    write(pkg.join("templates/cb-one.hbs"), "<cb><seq>one</seq></cb>");
    write(pkg.join("templates/cb-two.hbs"), "<cb><seq>two</seq></cb>");
    write(
        pkg.join("templates/cb-three.hbs"),
        "<cb><seq>three</seq></cb>",
    );
    write(pkg.join("channels.yaml"), CHANNELS_YAML);
    write(
        pkg.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"wsa-demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let out = base.join("archives");
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(&pkg, &out, &sutra_loader::PackageOptions::default())
        .expect("synthetic package seals into one .sutra archive");
    out
}

// ---- callback capture server -------------------------------------------------------------------

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
        .route("/wsa-callback", post(capture_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, capture)
}

// ---- minimal blocking HTTP POST (keeps the test dependency-free, smoke.rs style) ---------------

fn http_post(addr: SocketAddr, path: &str, body: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: wsa-demo-key\r\n\
         Content-Type: application/json\r\ntraceparent: {TRACEPARENT}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code")
}

// ---- the slice ----------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn three_emissions_from_one_instance_all_deliver() {
    // PG database for the engine-internal tables.
    let pg_port = container_port();
    {
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
            ))
            .await
            .expect("admin pool");
        sqlx::query("CREATE DATABASE wsa_e2e")
            .execute(&admin)
            .await
            .expect("create database");
    }
    let datasource_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/wsa_e2e");

    // Callback listener + the ${ENV} indirections the resource tree references.
    let (callback_addr, capture) = callback_server().await;
    std::env::set_var("WSA_CALLBACK_HOST", callback_addr.to_string());
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
        // The fixture role (testcontainers postgres superuser) has BYPASSRLS; relax the
        // boot check in tests only. rls_bypass_it proves the enforcement itself.
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    })
    .await
    .expect("engine boots");
    let addr = engine.local_addr;

    // Async intake: 202 Accepted, no business body.
    let status =
        tokio::task::spawn_blocking(move || http_post(addr, "/channels/wsa-in", "{\"amount\":42}"))
            .await
            .unwrap();
    assert_eq!(status, 202, "ack-mode on-persist answers 202");

    // The dispatcher tick (200 ms) drains the three rows to the callback listener.
    for _ in 0..150 {
        if capture.requests.lock().unwrap().len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let requests = capture.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        3,
        "all three emissions of the instance must deliver (no last-wins)"
    );

    // Delivered bodies = the three template renders, in emission order.
    let bodies: Vec<String> = requests
        .iter()
        .map(|(_, body)| String::from_utf8(body.clone()).unwrap())
        .collect();
    assert_eq!(
        bodies,
        vec![
            "<cb><seq>one</seq></cb>",
            "<cb><seq>two</seq></cb>",
            "<cb><seq>three</seq></cb>",
        ]
    );

    // Frozen wire contract per delivery: Idempotency-Key + Content-Type + traceparent.
    let mut keys = std::collections::BTreeSet::new();
    for (headers, _) in &requests {
        let key = headers
            .get("idempotency-key")
            .expect("Idempotency-Key on the wire");
        assert!(!key.trim().is_empty());
        keys.insert(key.clone());
        assert_eq!(headers.get("content-type").unwrap(), "application/xml");
        assert_eq!(
            headers.get("traceparent").unwrap(),
            TRACEPARENT,
            "the enqueuing request's traceparent rides the delivery"
        );
    }
    assert_eq!(
        keys.len(),
        3,
        "each emission carries its own idempotency key"
    );

    // Delivered rows are deleted — the outbox drains to empty.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&datasource_url)
        .await
        .expect("verify pool");
    let mut remaining: i64 = -1;
    for _ in 0..100 {
        remaining = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_entry")
            .fetch_one(&pool)
            .await
            .expect("count");
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(remaining, 0, "delivered outbox rows must be deleted");

    // Drain hook: refusing further ticks is a clean no-op at shutdown.
    engine.drain_outbox();
}
