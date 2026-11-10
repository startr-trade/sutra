//! The first outbound delivery after the engine starts is as prompt as the ones after it.
//!
//! The full engine (`serve`) boots a package whose flow keeps the inbound message in a SQL data
//! store and then sends it to an outbound HTTP channel; a local listener stands in for the
//! receiver. Each POST is timed from the request to the delivery arriving, the first one included,
//! with the outbox tick at one second as a deployment would set it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;

use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};

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

const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:first-send:1.0.0">
  <bpmn:process id="keep-and-send">
    <bpmn:dataStore id="notes" name="notes"/>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="note-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:dataObject id="doPayload" name="payload"/>
    <bpmn:dataStoreReference id="refWrite" dataStoreRef="notes">
      <bpmn:extensionElements><q:store key="payload.id"/></bpmn:extensionElements>
    </bpmn:dataStoreReference>
    <bpmn:serviceTask id="Keep">
      <bpmn:dataOutputAssociation>
        <bpmn:sourceRef>doPayload</bpmn:sourceRef><bpmn:targetRef>refWrite</bpmn:targetRef>
      </bpmn:dataOutputAssociation>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Notify" implementation="notify.hbs">
      <bpmn:extensionElements>
        <q:send channel="note-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Keep"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Keep" targetRef="Notify"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Notify" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const CHANNELS_YAML: &str = r#"channels:
  - name: note-in
    transport: http
    bind: "POST /channels/note-in"
    ack-mode: on-persist
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: first-send-key
        header: X-Api-Key
  - name: note-out
    direction: outbound
    transport: http
    bind: "http://${FIRST_SEND_CALLBACK_HOST}/notify"
"#;

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn synthesize_deployments_dir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("first-send-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let pkg = base.join("default--first-send--1.0.0");
    write(pkg.join("bpmn/keep-and-send.bpmn"), BPMN);
    write(
        pkg.join("templates/notify.hbs"),
        "{\"id\":\"{{payload.id}}\"}",
    );
    write(pkg.join("channels.yaml"), CHANNELS_YAML);
    write(
        pkg.join("datastores.yaml"),
        "datastores:\n  - name: notes\n    type: sql\n    sql:\n      url-ref: env:FIRST_SEND_DB_URL\n      migrations: migrations/notes\n",
    );
    write(
        pkg.join("migrations/notes/V001__notes.sql"),
        "CREATE TABLE IF NOT EXISTS data_store (\n\
           tenant_id VARCHAR(64) NOT NULL, module_id VARCHAR(256) NOT NULL,\n\
           module_version VARCHAR(64) NOT NULL, store_name VARCHAR(128) NOT NULL,\n\
           store_key VARCHAR(512) NOT NULL, store_value TEXT NOT NULL,\n\
           rev BIGINT NOT NULL DEFAULT 1,\n\
           updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n\
           PRIMARY KEY (tenant_id, module_id, module_version, store_name, store_key));\n",
    );
    write(
        pkg.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"first-send\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let out = base.join("archives");
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(&pkg, &out, &sutra_loader::PackageOptions::default())
        .expect("synthetic package seals into one .sutra archive");
    out
}

#[derive(Clone, Default)]
struct Arrivals {
    at: Arc<Mutex<Vec<Instant>>>,
}

async fn arrive(State(state): State<Arrivals>) -> StatusCode {
    state.at.lock().unwrap().push(Instant::now());
    StatusCode::ACCEPTED
}

async fn receiver() -> (SocketAddr, Arrivals) {
    let arrivals = Arrivals::default();
    let app = Router::new()
        .route("/notify", post(arrive))
        .with_state(arrivals.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, arrivals)
}

fn http_post(addr: SocketAddr, body: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST /channels/note-in HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: first-send-key\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
    (status, response)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn the_first_delivery_after_start_is_as_prompt_as_the_next() {
    let pg_port = container_port();
    {
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
            ))
            .await
            .expect("admin pool");
        sqlx::query("CREATE DATABASE first_send")
            .execute(&admin)
            .await
            .expect("create database");
    }
    let datasource_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/first_send");

    let (receiver_addr, arrivals) = receiver().await;
    std::env::set_var("FIRST_SEND_CALLBACK_HOST", receiver_addr.to_string());
    std::env::set_var("FIRST_SEND_DB_URL", &datasource_url);
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );

    let engine = serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(synthesize_deployments_dir()),
        deployments_poll_interval: Duration::from_secs(2),
        http_port: 0,
        datasource_url: Some(datasource_url.clone()),
        datasource_username: None,
        datasource_password: None,
        outbox_tick_interval: Duration::from_secs(1),
        outbox_tuning: Default::default(),
        datasource_pools: Default::default(),
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
        capacity_pools: Vec::new(),
        object_stores: Vec::new(),
        now_override: None,
    })
    .await
    .expect("engine boots");
    let addr = engine.local_addr;

    let mut latencies = Vec::new();
    for n in 0..3 {
        let sent = Instant::now();
        let body = format!("{{\"id\":\"note-{n}\"}}");
        let (status, response) = tokio::task::spawn_blocking(move || http_post(addr, &body))
            .await
            .unwrap();
        let answered = sent.elapsed();
        assert_eq!(status, 202, "on-persist answers 202: {response}");
        let deadline = sent + Duration::from_secs(20);
        loop {
            if arrivals.at.lock().unwrap().len() > n {
                break;
            }
            if Instant::now() >= deadline {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&datasource_url)
                    .await
                    .expect("diagnostic pool");
                let dead: Vec<String> =
                    sqlx::query_scalar("SELECT row_to_json(d)::text FROM dead_letter d")
                        .fetch_all(&pool)
                        .await
                        .unwrap_or_default();
                panic!("delivery {n} never arrived; dead letters: {dead:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let arrived = arrivals.at.lock().unwrap()[n] - sent;
        eprintln!("FIRST-DELIVERY-LATENCY n={n} answered={answered:?} delivered={arrived:?}");
        latencies.push(arrived);
    }
    // A delivery is handed to the dispatcher at the commit; it never waits for a tick.
    for (n, latency) in latencies.iter().enumerate() {
        assert!(
            *latency < Duration::from_millis(900),
            "delivery {n} took {latency:?} from the POST (all: {latencies:?})"
        );
    }
    engine.drain_outbox();
}
