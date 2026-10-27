//! Conformance: a respond-and-continue flow whose detached tail renders a message its declared
//! codec refuses ends FAILED, once — end to end against the engine image on real PostgreSQL.
//!
//! The caller is answered at the `<q:reply continue="true">` node (a 202 with the receipt, since
//! the channel is `ack-mode: on-persist`); the rest of the flow runs on the instance's due-now
//! self-resume timer. Its `<q:send codec="urn:notice">` renders a body without the `<Ref>` the
//! codec's XSD requires, so the read-back refuses the step before it commits.
//!
//! That refusal used to be treated like a store error: the timer poller deferred the row and
//! re-ran the tail every 5 s, forever, with the instance stuck SUSPENDED. The suite pins the fix:
//! the instance is FAILED with the refusal's code on the operate surface, the audit journal has
//! one `INSTANCE_FAILED` and the tail ran once, nothing reached the outbox, and no wait row is
//! left to fire. The package lives in `fixtures/early-reply-refused/`.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "early-reply-refused-test-key";
const REJECT: &str = "SUTRA.OUTBOUND.VALIDATION_REJECT";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

/// Seal the fixture package into a fresh world-readable archives directory.
fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/early-reply-refused/default--notice--1.0.0");
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
            let pg = engine::start_postgres("notice");
            let engine = EngineBuilder::new("notice", &pg)
                .env("NOTICE_API_KEY", API_KEY)
                // The audit journal is how the suite counts what ran.
                .env("SUTRA_AUDIT_SQL", "true")
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { pg, engine }
        })
        .join()
        .expect("early-reply-refused topology")
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_early_reply_refused_send_fails_the_instance_once() {
    let client = reqwest::Client::new();
    let id = "ER-REFUSED-1";

    // The caller gets its receipt at the continue-reply node, before the tail runs.
    let (status, body) = post(&client, &format!("{{\"id\":\"{id}\"}}")).await;
    assert_eq!(
        status, 202,
        "on-persist answers 202 with the receipt: {body}"
    );
    assert!(body.contains("accepted"), "the receipt is the body: {body}");
    let instance = instance_for(id).await;

    // The tail runs on the self-resume timer, and its send is refused: FAILED, with the code.
    let detail = failed(&client, &instance).await;
    assert_eq!(detail["failureCode"], serde_json::json!(REJECT), "{detail}");

    // Wait out more than two of the timer poller's 5 s backoff ticks. Before the fix, each tick
    // re-ran the tail; now nothing may run again.
    tokio::time::sleep(Duration::from_secs(12)).await;
    let detail = inspect(&client, &instance).await;
    assert_eq!(detail["status"], serde_json::json!("FAILED"), "{detail}");

    assert_eq!(
        count(&format!(
            "SELECT count(*) FROM audit_event WHERE instance_id = '{instance}' \
             AND event_type = 'INSTANCE_FAILED'"
        ))
        .await,
        1,
        "one INSTANCE_FAILED in the audit journal"
    );
    assert_eq!(
        count(&format!(
            "SELECT count(*) FROM audit_event WHERE instance_id = '{instance}' \
             AND event_type = 'NODE_LEFT' AND node_id = 'Render'"
        ))
        .await,
        1,
        "the tail before the refused send ran once, not once per backoff tick"
    );
    assert_eq!(
        count(&format!(
            "SELECT count(*) FROM outbox_entry WHERE instance_id = '{instance}'"
        ))
        .await,
        0,
        "the refused notice never reached the outbox"
    );
    assert_eq!(
        count(&format!(
            "SELECT count(*) FROM waiting_event WHERE instance_id = '{instance}' \
             AND status = 'WAITING'"
        ))
        .await,
        0,
        "no wait or timer row is left to fire"
    );
}

// ---- drive helpers ----------------------------------------------------------------------

async fn post(client: &reqwest::Client, body: &str) -> (u16, String) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/channels/notice",
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

/// Poll until the instance reports FAILED (the self-resume is due at once; allow 30 s).
async fn failed(client: &reqwest::Client, instance: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let detail = inspect(client, instance).await;
        if detail["status"] == "FAILED" {
            return detail;
        }
        assert!(
            Instant::now() < deadline,
            "instance {instance} never reached FAILED: {detail}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The one persisted instance whose state mentions `id` (allow 10 s for the start to commit).
async fn instance_for(id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query = format!(
            "SELECT instance_id FROM instance_state \
             WHERE position(convert_to('{id}', 'UTF8') in serialised) > 0"
        );
        let rows = tokio::task::spawn_blocking(move || sql(&query))
            .await
            .expect("psql task");
        let ids: Vec<String> = rows.lines().map(str::to_string).collect();
        if ids.len() == 1 {
            return ids[0].clone();
        }
        assert!(
            ids.len() < 2 && Instant::now() < deadline,
            "one instance for {id}: {ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn count(query: &str) -> u64 {
    let query = query.to_string();
    let out = tokio::task::spawn_blocking(move || sql(&query))
        .await
        .expect("psql task");
    out.parse().unwrap_or_else(|e| panic!("count {out:?}: {e}"))
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
