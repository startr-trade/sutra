//! Conformance: an answer a wait subscribes to that correlates to no live instance, end to end
//! against the engine image on real PostgreSQL.
//!
//! - Refused (no `uncorrelated:` on the channel): HTTP 500 with
//!   `SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`, a WARN line naming
//!   `SUTRA.INBOUND.RELAY_UNCORRELATED`, and an `inbound.RELAY_UNCORRELATED` row in the audit
//!   journal. `sutra.waitPending` is set only while an instance may still reach the wait: absent
//!   for an answer whose order has ended.
//! - Routed (`uncorrelated: { start: late-answer }`): the answer starts `late-answer`, which sees
//!   `event.uncorrelated`; an answer that correlates still resumes its order and starts nothing.
//!
//! The package lives in `fixtures/uncorrelated-relay/`. Each test drives its own ids, so the
//! tests share one engine fixture in any order.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "uncorrelated-test-key";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/uncorrelated-relay/default--uncorrelated--1.0.0");
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
            let pg = engine::start_postgres("uncorrelated");
            let engine = EngineBuilder::new("uncorrelated", &pg)
                .env("UNCORRELATED_API_KEY", API_KEY)
                .env("SUTRA_AUDIT_SQL", "true")
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { pg, engine }
        })
        .join()
        .expect("uncorrelated topology")
    })
}

/// (a) No `uncorrelated:` on the channel. An answer for an id nothing ever carried is refused
/// wait-pending (an order may be a step short of its wait); an answer for an order that has ended
/// is refused NOT wait-pending. Both leave a WARN line and an audit row with the keys tried.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_uncorrelated_relay_a_refused_answer_is_recorded_and_wait_pending_only_while_it_may_park(
) {
    let _suite = sutra_testkit::hold_suite!("uncorrelated");
    let client = reqwest::Client::new();

    let (status, body) = post(&client, "/channels/plain/answer", "UC-NOBODY").await;
    assert_eq!(status, 500, "an answer for nobody is refused: {body}");
    let problem: Value = serde_json::from_str(&body).expect("problem JSON");
    assert!(
        body.contains("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND"),
        "{body}"
    );
    assert_eq!(
        problem["attributes"]["sutra.waitPending"], "true",
        "nothing says the key's instance ended: {body}"
    );

    let (status, body) = post(&client, "/channels/plain", "UC-ENDED").await;
    assert_eq!(status, 200, "the order parks at its wait: {body}");
    let (status, body) = post(&client, "/channels/plain/answer", "UC-ENDED").await;
    assert_eq!(
        status, 200,
        "the answer resumes the order to its end: {body}"
    );

    let (status, body) = post(&client, "/channels/plain/answer", "UC-ENDED").await;
    assert_eq!(status, 500, "a second answer finds no live order: {body}");
    let problem: Value = serde_json::from_str(&body).expect("problem JSON");
    assert!(
        body.contains("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND"),
        "{body}"
    );
    assert!(
        problem["attributes"].get("sutra.waitPending").is_none(),
        "the order ended, so no instance waits for this answer: {body}"
    );

    let rows = audit_rows_for("UC-ENDED").await;
    assert_eq!(rows.len(), 1, "one record for the late answer: {rows:?}");
    let (instance, code, detail) = &rows[0];
    assert!(instance.is_empty(), "a deployment-level row: {rows:?}");
    assert_eq!(code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");
    assert_eq!(detail["channel"], "plain-answer", "{detail}");
    assert_eq!(detail["outcome"], "refused", "{detail}");
    assert_eq!(detail["ended"], true, "{detail}");
    assert_eq!(detail["keys"][0]["alias"], "plainId", "{detail}");
    assert_eq!(detail["keys"][0]["value"], "UC-ENDED", "{detail}");
    let nobody = audit_rows_for("UC-NOBODY").await;
    assert_eq!(nobody.len(), 1, "{nobody:?}");
    assert_eq!(nobody[0].2["ended"], false, "{:?}", nobody[0]);

    let logs = engine_logs();
    let line = logs
        .lines()
        .find(|l| l.contains("SUTRA.INBOUND.RELAY_UNCORRELATED") && l.contains("UC-ENDED"))
        .unwrap_or_else(|| panic!("a WARN line for the late answer:\n{logs}"));
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains("plain-answer"), "{line}");
}

/// (b) `uncorrelated: { start: late-answer }` on the channel. An answer that correlates resumes
/// its order and starts nothing; the same answer once the order has ended starts `late-answer`,
/// which sees the keys tried in `event.uncorrelated`; the audit row names the started instance.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_uncorrelated_relay_a_routed_answer_starts_the_named_process_and_correlation_is_unaffected(
) {
    let _suite = sutra_testkit::hold_suite!("uncorrelated");
    let client = reqwest::Client::new();

    let (status, body) = post(&client, "/channels/routed", "UC-ROUTED").await;
    assert_eq!(status, 200, "the order parks at its wait: {body}");
    let order = instance_of("routed-order", "UC-ROUTED").await;

    let (status, body) = post(&client, "/channels/routed/answer", "UC-ROUTED").await;
    assert_eq!(status, 200, "the answer resumes the order: {body}");
    assert_eq!(inspect(&client, &order).await["status"], "COMPLETED");
    assert!(
        instances_of("late-answer", "UC-ROUTED").await.is_empty(),
        "a correlated answer starts nothing"
    );

    let (status, body) = post(&client, "/channels/routed/answer", "UC-ROUTED").await;
    assert_eq!(
        status, 200,
        "an answer for the ended order starts late-answer: {body}"
    );
    let late = instance_of("late-answer", "UC-ROUTED").await;
    let detail = inspect(&client, &late).await;
    assert_eq!(detail["status"], "SUSPENDED", "{detail}");
    assert_eq!(detail["variables"]["lateKey"], "UC-ROUTED", "{detail}");
    assert_eq!(detail["variables"]["lateEnded"], "true", "{detail}");
    assert_eq!(
        detail["variables"]["lateCode"], "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND",
        "{detail}"
    );

    let rows = audit_rows_for("UC-ROUTED").await;
    assert_eq!(rows.len(), 1, "one record for the routed answer: {rows:?}");
    let detail = &rows[0].2;
    assert_eq!(detail["outcome"], "started", "{detail}");
    assert_eq!(detail["startedProcess"], "late-answer", "{detail}");
    assert_eq!(detail["instanceId"], late.as_str(), "{detail}");
}

// ---- drive helpers ----------------------------------------------------------------------

async fn post(client: &reqwest::Client, path: &str, id: &str) -> (u16, String) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}{path}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(format!("{{\"id\":\"{id}\"}}"))
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

/// The persisted instances of `process` whose state mentions `id`.
async fn instances_of(process: &str, id: &str) -> Vec<String> {
    let query = format!(
        "SELECT instance_id FROM instance_state \
         WHERE position(convert_to('{id}', 'UTF8') in serialised) > 0 \
           AND position(convert_to('{process}', 'UTF8') in serialised) > 0"
    );
    let rows = tokio::task::spawn_blocking(move || sql(&query))
        .await
        .expect("psql task");
    rows.lines().map(str::to_string).collect()
}

async fn instance_of(process: &str, id: &str) -> String {
    let ids = instances_of(process, id).await;
    assert_eq!(ids.len(), 1, "one {process} instance for {id}: {ids:?}");
    ids[0].clone()
}

/// `(instance_id, diagnostic_code, diagnostic_json)` of every `inbound.RELAY_UNCORRELATED` row
/// whose detail names `id`. The journal writes in the background, so poll briefly.
async fn audit_rows_for(id: &str) -> Vec<(String, String, Value)> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let query = format!(
            "SELECT coalesce(instance_id::text, '') || '|' || coalesce(diagnostic_code, '') \
             || '|' || diagnostic_json FROM audit_event \
             WHERE event_type = 'inbound.RELAY_UNCORRELATED' \
               AND position('{id}' in diagnostic_json) > 0"
        );
        let out = tokio::task::spawn_blocking(move || sql(&query))
            .await
            .expect("psql task");
        let rows: Vec<(String, String, Value)> = out
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                let mut parts = l.splitn(3, '|');
                let instance = parts.next().unwrap_or_default().to_string();
                let code = parts.next().unwrap_or_default().to_string();
                let detail =
                    serde_json::from_str(parts.next().unwrap_or("null")).unwrap_or(Value::Null);
                (instance, code, detail)
            })
            .collect();
        if !rows.is_empty() || Instant::now() >= deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn engine_logs() -> String {
    let out = Command::new("docker")
        .args(["logs", fixture().engine.container.id()])
        .output()
        .expect("docker logs");
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
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
