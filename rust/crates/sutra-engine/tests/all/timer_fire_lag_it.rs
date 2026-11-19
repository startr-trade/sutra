//! Timer fire lag under a steady arrival rate: a parked timer fires on time while new
//! instances keep arriving.
//!
//! The full engine (`serve`) boots a package whose flow parks every new instance on a short
//! intermediate timer and, when it fires, sends a follow-up to an outbound HTTP channel; a local
//! listener stands in for the receiver. The test opens `TIMER_LAG_RATE` new instances per second
//! for `TIMER_LAG_SECS` seconds and measures, per instance, how late the follow-up arrived
//! relative to the timer's due time (POST + timer duration). New-instance answer latency is
//! measured alongside, so a run shows whether follow-ups queue behind new work.
//!
//! The bar: the p99 follow-up lag stays under a few seconds at a rate the engine's own
//! new-instance path absorbs easily. A poller that fires only a fixed batch per tick caps fired
//! timers far below the arrival rate, and the lag then grows for as long as the load lasts.
//!
//! ```text
//! TIMER_LAG_RATE=400 TIMER_LAG_SECS=20 \
//!   cargo test -p sutra-engine --test all timer_fire_lag -- --ignored --nocapture
//! ```

use std::collections::HashMap;
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

/// The timer every instance parks on.
const TIMER: Duration = Duration::from_secs(1);

const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:timer-lag:1.0.0">
  <bpmn:process id="park-then-follow-up">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="work-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="FollowUp" implementation="follow-up.hbs">
      <bpmn:extensionElements>
        <q:send channel="follow-up-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="FollowUp"/>
    <bpmn:sequenceFlow id="f3" sourceRef="FollowUp" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const CHANNELS_YAML: &str = r#"channels:
  - name: work-in
    transport: http
    bind: "POST /channels/work-in"
    ack-mode: on-persist
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: timer-lag-key
        header: X-Api-Key
  - name: follow-up-out
    direction: outbound
    transport: http
    bind: "http://${TIMER_LAG_CALLBACK_HOST}/follow-up"
"#;

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn synthesize_deployments_dir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("timer-lag-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let pkg = base.join("default--timer-lag--1.0.0");
    write(pkg.join("bpmn/park-then-follow-up.bpmn"), BPMN);
    write(
        pkg.join("templates/follow-up.hbs"),
        "{\"id\":\"{{payload.id}}\"}",
    );
    write(pkg.join("channels.yaml"), CHANNELS_YAML);
    write(
        pkg.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"timer-lag\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let out = base.join("archives");
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(&pkg, &out, &sutra_loader::PackageOptions::default())
        .expect("synthetic package seals into one .sutra archive");
    out
}

/// Follow-up arrival time per instance id.
#[derive(Clone, Default)]
struct Arrivals {
    at: Arc<Mutex<HashMap<String, Instant>>>,
}

async fn arrive(State(state): State<Arrivals>, body: String) -> StatusCode {
    let now = Instant::now();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
        if let Some(id) = v.get("id").and_then(|i| i.as_str()) {
            state.at.lock().unwrap().entry(id.to_owned()).or_insert(now);
        }
    }
    StatusCode::ACCEPTED
}

async fn receiver() -> (SocketAddr, Arrivals) {
    let arrivals = Arrivals::default();
    let app = Router::new()
        .route("/follow-up", post(arrive))
        .with_state(arrivals.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, arrivals)
}

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summary(label: &str, mut v: Vec<Duration>) -> Duration {
    v.sort();
    let p99 = percentile(&v, 0.99);
    eprintln!(
        "TIMER-FIRE-LAG {label}: n={} p50={:?} p90={:?} p99={:?} max={:?}",
        v.len(),
        percentile(&v, 0.50),
        percentile(&v, 0.90),
        p99,
        v.last().copied().unwrap_or_default(),
    );
    p99
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn timer_fire_lag_stays_bounded_under_a_steady_arrival_rate() {
    let rate = env_or("TIMER_LAG_RATE", 200).max(1);
    let secs = env_or("TIMER_LAG_SECS", 15).max(1);
    let p99_bar = Duration::from_millis(env_or("TIMER_LAG_P99_BAR_MS", 3000));

    let pg_port = container_port();
    {
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
            ))
            .await
            .expect("admin pool");
        sqlx::query("CREATE DATABASE timer_lag")
            .execute(&admin)
            .await
            .expect("create database");
    }
    let datasource_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/timer_lag");

    let (receiver_addr, arrivals) = receiver().await;
    std::env::set_var("TIMER_LAG_CALLBACK_HOST", receiver_addr.to_string());
    // The shipped families plus the native tree, which carries the timer columns.
    let native =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sutra-persistence/migrations/native");
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        format!(
            "{}:{}",
            crate::outbox_e2e::shipped_migration_roots_env(),
            native.display()
        ),
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
        connectivity_events_channel: None,
    })
    .await
    .expect("engine boots");
    let url = format!("http://{}/channels/work-in", engine.local_addr);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(256)
        .build()
        .expect("client");

    // Open `rate` instances per second for `secs` seconds, each POST on its own task so a slow
    // answer never slows the arrival clock.
    let total = rate * secs;
    let sent: Arc<Mutex<HashMap<String, Instant>>> = Arc::default();
    let answers: Arc<Mutex<Vec<Duration>>> = Arc::default();
    let failures: Arc<Mutex<Vec<String>>> = Arc::default();
    let start = Instant::now();
    let mut posts = tokio::task::JoinSet::new();
    for n in 0..total {
        let due = start + Duration::from_secs_f64(n as f64 / rate as f64);
        tokio::time::sleep_until(due.into()).await;
        let (client, url) = (client.clone(), url.clone());
        let (sent, answers, failures) = (sent.clone(), answers.clone(), failures.clone());
        posts.spawn(async move {
            let id = format!("w-{n}");
            let at = Instant::now();
            sent.lock().unwrap().insert(id.clone(), at);
            let res = client
                .post(&url)
                .header("X-Api-Key", "timer-lag-key")
                .header("Content-Type", "application/json")
                .body(format!("{{\"id\":\"{id}\"}}"))
                .send()
                .await;
            match res {
                Ok(r) if r.status().as_u16() == 202 => answers.lock().unwrap().push(at.elapsed()),
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("{id}: {status} {body}"));
                }
                Err(e) => failures.lock().unwrap().push(format!("{id}: {e}")),
            }
        });
    }
    while posts.join_next().await.is_some() {}
    let offered = start.elapsed();
    let failures = failures.lock().unwrap().clone();
    assert!(
        failures.is_empty(),
        "{} POSTs failed, first: {:?}",
        failures.len(),
        failures.first()
    );

    // Wait for every follow-up (bounded: a lagging poller still drains eventually).
    let deadline = Instant::now() + Duration::from_secs(secs * 20 + 60);
    loop {
        let got = arrivals.at.lock().unwrap().len() as u64;
        if got >= total || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let drained = start.elapsed();

    let sent = sent.lock().unwrap().clone();
    let got = arrivals.at.lock().unwrap().clone();
    let lags: Vec<Duration> = sent
        .iter()
        .filter_map(|(id, at)| {
            got.get(id)
                .map(|arrived| arrived.saturating_duration_since(*at + TIMER))
        })
        .collect();
    eprintln!(
        "TIMER-FIRE-LAG run: rate={rate}/s secs={secs} total={total} offered_in={offered:?} \
         follow_ups={} drained_in={drained:?} shards={}",
        got.len(),
        crate::shard_support::engine_shards_from_env().shards,
    );
    summary("answer", answers.lock().unwrap().clone());
    let p99 = summary("follow-up lag past due", lags);
    engine.drain_outbox();
    assert_eq!(got.len() as u64, total, "every follow-up arrives");
    assert!(
        p99 < p99_bar,
        "p99 follow-up lag {p99:?} exceeds {p99_bar:?} at {rate} new instances/s"
    );
}
