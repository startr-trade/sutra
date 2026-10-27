//! Conformance: a RECEIVE TASK in a loop with an interrupting timer boundary — the listen-window
//! pattern ("wait for a correlated status, or give up after a few seconds; loop back for the next
//! status") — end to end against the engine image on real PostgreSQL.
//!
//! A receive task parks exactly like an intermediate message catch, so a status posted on its
//! `<q:source>` channel is a channel-delivered RELAY: correlated through the start event's
//! `onConflict="correlate"` alias and resumed. Unlike a catch event it is an activity, so it can
//! host the timer boundary that closes the window.
//!
//! Each test drives its own correlation id, so the three share one engine fixture in any order.
//! The package lives in `fixtures/listen-window/`; its timer window is 4 s.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "listen-window-test-key";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

/// Seal the fixture package into a fresh world-readable archives directory.
fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/listen-window/default--listen--1.0.0");
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
            let pg = engine::start_postgres("listen");
            let engine = EngineBuilder::new("listen", &pg)
                .env("LISTEN_API_KEY", API_KEY)
                // The audit journal is how the suite sees which path an instance took.
                .env("SUTRA_AUDIT_SQL", "true")
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { pg, engine }
        })
        .join()
        .expect("listen-window topology")
    })
}

/// (a) Statuses that arrive inside the window resume the receive task; a non-final one loops
/// back to a NEW wait that a second status resumes again; the final one completes the instance
/// through the normal path, never the boundary.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_listen_window_statuses_before_the_timer_resume_the_receive_task_each_round() {
    let client = reqwest::Client::new();
    let id = "LW-ROUNDS-1";

    let (status, body) = start(&client, id).await;
    assert_eq!(status, 200, "the start parks at the receive task: {body}");
    let instance = instance_for(id).await;
    assert_eq!(
        inspect(&client, &instance).await["waitingNodes"],
        serde_json::json!(["Listen"])
    );

    let (status, body) = post_status(&client, id, "running").await;
    assert_eq!(
        status, 200,
        "the first status resumes the receive task: {body}"
    );
    let detail = inspect(&client, &instance).await;
    assert_eq!(
        detail["waitingNodes"],
        serde_json::json!(["Listen"]),
        "the loop waits again"
    );
    assert_eq!(
        detail["variables"]["rounds"],
        serde_json::json!("1"),
        "{detail}"
    );

    let (status, body) = post_status(&client, id, "running").await;
    assert_eq!(
        status, 200,
        "the second status resumes the new wait: {body}"
    );
    let detail = inspect(&client, &instance).await;
    assert_eq!(detail["waitingNodes"], serde_json::json!(["Listen"]));
    assert_eq!(
        detail["variables"]["rounds"],
        serde_json::json!("2"),
        "{detail}"
    );

    let (status, body) = post_status(&client, id, "final").await;
    assert_eq!(status, 200, "the final status leaves the loop: {body}");
    completed(&client, &instance).await;
    let nodes = audited_nodes_until(&instance, "Done").await;
    assert_eq!(
        nodes.iter().filter(|n| *n == "Count").count(),
        3,
        "three statuses, three rounds: {nodes:?}"
    );
    assert!(
        !nodes.iter().any(|n| n == "MarkTimeout" || n == "TimedOut"),
        "the boundary path never ran: {nodes:?}"
    );
}

/// (b) No status arrives: the timer boundary fires, interrupts the receive task, takes the
/// boundary path and completes the instance there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_listen_window_no_status_fires_the_timer_and_takes_the_boundary_path() {
    let client = reqwest::Client::new();
    let id = "LW-TIMEOUT-1";

    let (status, body) = start(&client, id).await;
    assert_eq!(status, 200, "the start parks at the receive task: {body}");
    let instance = instance_for(id).await;

    completed(&client, &instance).await;
    let nodes = audited_nodes_until(&instance, "TimedOut").await;
    assert!(
        nodes.iter().any(|n| n == "MarkTimeout"),
        "the boundary path ran: {nodes:?}"
    );
    assert!(
        !nodes.iter().any(|n| n == "Count" || n == "Done"),
        "the normal path did not: {nodes:?}"
    );

    let (status, body) = post_status(&client, id, "running").await;
    assert_eq!(
        status, 500,
        "a status after the window closed finds nothing: {body}"
    );
    assert!(
        body.contains("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND"),
        "correlation-not-found code: {body}"
    );
}

/// (c) A status that correlates to no instance is refused, and the parked instance is untouched.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_listen_window_an_uncorrelated_status_is_refused_and_moves_nothing() {
    let client = reqwest::Client::new();
    let id = "LW-UNCORRELATED-1";

    let (status, body) = start(&client, id).await;
    assert_eq!(status, 200, "the start parks at the receive task: {body}");
    let instance = instance_for(id).await;

    let (status, body) = post_status(&client, "LW-NOBODY", "running").await;
    assert_eq!(status, 500, "an uncorrelated status is refused: {body}");
    assert!(
        body.contains("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND"),
        "correlation-not-found code: {body}"
    );
    let detail = inspect(&client, &instance).await;
    assert_eq!(detail["status"], serde_json::json!("SUSPENDED"), "{detail}");
    assert_eq!(
        detail["waitingNodes"],
        serde_json::json!(["Listen"]),
        "{detail}"
    );
    assert_eq!(
        detail["variables"]["rounds"],
        serde_json::json!("0"),
        "{detail}"
    );
}

// ---- drive helpers ----------------------------------------------------------------------

async fn start(client: &reqwest::Client, id: &str) -> (u16, String) {
    post(client, "/channels/listen", &format!("{{\"id\":\"{id}\"}}")).await
}

async fn post_status(client: &reqwest::Client, id: &str, state: &str) -> (u16, String) {
    post(
        client,
        "/channels/listen/status",
        &format!("{{\"id\":\"{id}\",\"state\":\"{state}\"}}"),
    )
    .await
}

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

/// Poll until the instance reports COMPLETED (the timer window is 4 s; allow 30).
async fn completed(client: &reqwest::Client, instance: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let detail = inspect(client, instance).await;
        if detail["status"] == "COMPLETED" {
            return detail;
        }
        assert!(
            Instant::now() < deadline,
            "instance {instance} never completed: {detail}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The node ids of every `NODE_LEFT` audit event of `instance`, in journal order, once one of
/// them is `last` (allow 30 s).
///
/// Read from the journal, not the inspect projection: a finished instance is retained as the
/// snapshot of its last park, so its variables do not show what ran after that park.
async fn audited_nodes_until(instance: &str, last: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let query = format!(
            "SELECT node_id FROM audit_event WHERE instance_id = '{instance}' \
             AND event_type = 'NODE_LEFT' AND node_id IS NOT NULL ORDER BY seq"
        );
        let rows = tokio::task::spawn_blocking(move || sql(&query))
            .await
            .expect("psql task");
        let nodes: Vec<String> = rows.lines().map(str::to_string).collect();
        if nodes.iter().any(|n| n == last) {
            return nodes;
        }
        assert!(
            Instant::now() < deadline,
            "instance {instance} never audited {last}: {nodes:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
