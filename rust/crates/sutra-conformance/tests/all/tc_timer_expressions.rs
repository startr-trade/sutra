//! Conformance: expression-valued timer intervals, end to end against the engine image on real
//! PostgreSQL. Each timer kind takes its interval from a FEEL expression — a flow variable, or the
//! deployment configuration (`package.yaml` `properties:`, read as `config.<key>`) — and the
//! instant it resolves to is what the durable rows carry.
//!
//! The package lives in `fixtures/timer-expressions/`. Its two properties default to long
//! intervals (`PT30S`, `PT1H`); the engine's environment sets them short, so the tests that use
//! them only pass when the configuration was resolved from the environment and applied.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "timer-expressions-test-key";
const TIMER_EXPRESSION_INVALID: &str = "SUTRA.RUNTIME.TIMER.EXPRESSION_INVALID";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/timer-expressions/default--timers--1.0.0");
    let out = util::world_readable_temp_dir("sutra-archives");
    sutra_loader::package::assemble_dir(&package, &out, &sutra_loader::PackageOptions::default())
        .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
    util::make_world_readable(&out);
    out
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("timers");
            let engine = EngineBuilder::new("timers", &pg)
                .env("TE_API_KEY", API_KEY)
                // The deployment configuration, from the engine's environment.
                .env("TE_RETRY_DELAY", "PT4S")
                .env("TE_START_AFTER", "PT3S")
                .env("SUTRA_AUDIT_SQL", "true")
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { pg, engine }
        })
        .join()
        .expect("timer-expressions topology")
    })
}

/// (1) A timer catch whose duration is `payload.wait`: the durable row is due exactly that long
/// after it was armed, and the instance completes when it fires — not before.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_timer_expressions_a_catch_waits_the_duration_a_flow_variable_names() {
    let client = reqwest::Client::new();
    let id = "TE-CATCH-1";
    let (status, body) = post(
        &client,
        "/channels/te/catch",
        &format!(r#"{{"id":"{id}","wait":"PT3S"}}"#),
    )
    .await;
    assert_eq!(status, 200, "the start parks at the timer: {body}");
    let instance = instance_for(id).await;

    let armed = seconds_armed(&instance, "CatchWait").await;
    assert!(
        (2.5..=3.5).contains(&armed),
        "the row is due payload.wait (3 s) after it was armed: {armed}"
    );
    assert_eq!(inspect(&client, &instance).await["status"], "SUSPENDED");
    finished(&client, &instance, "COMPLETED").await;
}

/// (2) The listen-window pattern with the window taken from the request: no status arrives, so
/// the boundary fires after `payload.window` and the boundary path completes the instance.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_timer_expressions_a_receive_task_boundary_closes_the_window_a_flow_variable_names() {
    let client = reqwest::Client::new();
    let id = "TE-LISTEN-1";
    let (status, body) = post(
        &client,
        "/channels/te/listen",
        &format!(r#"{{"id":"{id}","window":"PT2S"}}"#),
    )
    .await;
    assert_eq!(status, 200, "the start parks at the receive task: {body}");
    let instance = instance_for(id).await;
    let armed = seconds_armed(&instance, "ListenTimeout").await;
    assert!(
        (1.5..=2.5).contains(&armed),
        "the boundary is due payload.window (2 s) after the host parked: {armed}"
    );

    finished(&client, &instance, "COMPLETED").await;
    let nodes = audited_nodes(&instance).await;
    assert!(
        nodes.iter().any(|n| n == "ListenTimedOut"),
        "the boundary path ran: {nodes:?}"
    );
    assert!(
        !nodes.iter().any(|n| n == "ListenDone"),
        "the normal path did not: {nodes:?}"
    );

    // Inside a longer window a status resumes the receive task instead.
    let id = "TE-LISTEN-2";
    let (status, body) = post(
        &client,
        "/channels/te/listen",
        &format!(r#"{{"id":"{id}","window":"PT60S"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let instance = instance_for(id).await;
    let (status, body) = post(
        &client,
        "/channels/te/status",
        &format!(r#"{{"id":"{id}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "the status resumes the receive task: {body}");
    finished(&client, &instance, "COMPLETED").await;
    let nodes = audited_nodes(&instance).await;
    assert!(nodes.iter().any(|n| n == "ListenDone"), "{nodes:?}");
}

/// (3) A `<q:retry>` delay from the deployment configuration: the call times out after 1 s, the
/// backoff is `config.retryDelay` (PT4S from the environment, PT30S if the configuration were
/// not applied), the second attempt times out, and the budget is spent.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_timer_expressions_a_retry_backoff_is_the_configured_delay() {
    let client = reqwest::Client::new();
    let id = "TE-RETRY-1";
    let began = Instant::now();
    let (status, body) = post(
        &client,
        "/channels/te/retry",
        &format!(r#"{{"id":"{id}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "the start parks at the call: {body}");
    let instance = instance_for(id).await;

    let detail = finished(&client, &instance, "FAILED").await;
    let elapsed = began.elapsed();
    assert_eq!(
        detail["failureCode"],
        serde_json::json!("SUTRA.RUNTIME.RETRY.EXHAUSTED"),
        "{detail}"
    );
    // 1 s timeout + 4 s configured backoff + 1 s timeout, plus poller ticks.
    assert!(
        elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(25),
        "the backoff was config.retryDelay (PT4S): {elapsed:?}"
    );
}

/// (4) A timer start event armed from the deployment configuration: the schedule row carries
/// the RESOLVED literal (`PT3S`, from the environment), fires once, and the process runs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_timer_expressions_a_start_timer_is_armed_from_the_configuration() {
    let _ = fixture();
    let row = sql("SELECT kind || ' ' || spec FROM timer_schedule WHERE node_id = 'Tick'");
    assert_eq!(row, "DURATION PT3S", "the resolved spec is what persists");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ran = sql(
            "SELECT count(*) FROM audit_event WHERE event_type = 'NODE_LEFT' \
             AND node_id = 'TickDone'",
        );
        let status = sql("SELECT status FROM timer_schedule WHERE node_id = 'Tick'");
        if ran == "1" && status == "RESOLVED" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the start timer never fired (ran={ran}, schedule={status})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// (5) A timer whose expression is null when it is armed: the instance goes FAILED once with
/// the timer code — one INSTANCE_FAILED, no wait row left, and it stays that way.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_timer_expressions_a_null_expression_fails_the_instance_once() {
    let client = reqwest::Client::new();
    let id = "TE-NULL-1";
    let (status, body) = post(&client, "/channels/te/null", &format!(r#"{{"id":"{id}"}}"#)).await;
    assert_eq!(
        status, 200,
        "the start parks at the first, literal wait: {body}"
    );
    let instance = instance_for(id).await;

    let detail = finished(&client, &instance, "FAILED").await;
    assert_eq!(
        detail["failureCode"],
        serde_json::json!(TIMER_EXPRESSION_INVALID),
        "{detail}"
    );
    // Wait out more than two of the timer poller's backoff ticks: nothing refires.
    tokio::time::sleep(Duration::from_secs(12)).await;
    let failed_events = sql(&format!(
        "SELECT count(*) FROM audit_event WHERE instance_id = '{instance}' \
         AND event_type = 'INSTANCE_FAILED'"
    ));
    assert_eq!(
        failed_events, "1",
        "one INSTANCE_FAILED in the audit journal"
    );
    let waiting = sql(&format!(
        "SELECT count(*) FROM waiting_event WHERE instance_id = '{instance}' \
         AND status = 'WAITING'"
    ));
    assert_eq!(waiting, "0", "no wait row is left to fire");
    assert_eq!(
        inspect(&client, &instance).await["status"],
        serde_json::json!("FAILED")
    );
}

// ---- helpers ----------------------------------------------------------------------------

async fn post(client: &reqwest::Client, path: &str, body: &str) -> (u16, String) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}{path}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.to_string())
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    (status, resp.text().await.unwrap_or_default())
}

async fn inspect(client: &reqwest::Client, instance: &str) -> Value {
    client
        .get(format!(
            "http://127.0.0.1:{}/sutra/instances/{instance}",
            fixture().engine.http_port
        ))
        .send()
        .await
        .expect("GET instance")
        .json()
        .await
        .expect("instance JSON")
}

/// Poll until the instance reports `status` (allow 40 s).
async fn finished(client: &reqwest::Client, instance: &str, status: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let detail = inspect(client, instance).await;
        if detail["status"] == status {
            return detail;
        }
        assert!(
            Instant::now() < deadline,
            "instance {instance} never reached {status}: {detail}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// How long after it was armed the TIMER row of `node` is due, in seconds.
async fn seconds_armed(instance: &str, node: &str) -> f64 {
    let query = format!(
        "SELECT EXTRACT(EPOCH FROM timer_due_at - created_at) FROM waiting_event \
         WHERE instance_id = '{instance}' AND node_id = '{node}' AND kind = 'TIMER'"
    );
    let row = tokio::task::spawn_blocking(move || sql(&query))
        .await
        .expect("psql task");
    row.parse()
        .unwrap_or_else(|_| panic!("no TIMER row for {node}: '{row}'"))
}

/// The node ids of every `NODE_LEFT` audit event of `instance`, in journal order.
async fn audited_nodes(instance: &str) -> Vec<String> {
    let query = format!(
        "SELECT node_id FROM audit_event WHERE instance_id = '{instance}' \
         AND event_type = 'NODE_LEFT' AND node_id IS NOT NULL ORDER BY seq"
    );
    let rows = tokio::task::spawn_blocking(move || sql(&query))
        .await
        .expect("psql task");
    rows.lines().map(str::to_string).collect()
}

/// The one persisted instance whose state mentions `id`.
async fn instance_for(id: &str) -> String {
    let query = format!(
        "SELECT instance_id FROM instance_state \
         WHERE position(convert_to('{id}', 'UTF8') in serialised) > 0"
    );
    let rows = tokio::task::spawn_blocking(move || sql(&query))
        .await
        .expect("psql task");
    let ids: Vec<&str> = rows.lines().collect();
    assert_eq!(ids.len(), 1, "one instance for {id}: {ids:?}");
    ids[0].to_string()
}

fn sql(query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            fixture().pg.container.id(),
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAc",
            query,
        ])
        .output()
        .expect("docker exec psql");
    assert!(
        out.status.success(),
        "psql {query}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}
