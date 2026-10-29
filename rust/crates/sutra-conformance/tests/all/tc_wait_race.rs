//! Conformance: a `local://` answer that reaches its target instance while the instance is still
//! between steps, end to end on real PostgreSQL with the package in `fixtures/wait-race/`.
//!
//! `race-intake` sends a request to `race-worker` and only parks at the wait that takes the answer
//! one step later (a two-second timer sits in between). The worker answers at once, so the answer
//! arrives while the intake instance is parked at the timer: the wait's correlation alias does not
//! exist yet. The answer must be held and retried until the instance parks at the wait, and then
//! delivered exactly once. Nothing may be lost, and nothing may wait for the outbox's five-minute
//! poison horizon.

use std::collections::BTreeSet;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::callback::{start_recorder, HostRecorder};
use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "wait-race-conformance-key";
const ORDERS: usize = 20;

struct Fixture {
    pg: PgFixture,
    recorder: HostRecorder,
    engine: engine::EngineHandle,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/wait-race/default--waitrace--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("waitrace");
            let recorder = start_recorder();
            let engine = EngineBuilder::new("waitrace", &pg)
                .env("WAIT_RACE_API_KEY", API_KEY)
                .env(
                    "WAIT_RACE_CALLBACK_URL",
                    format!("http://host.docker.internal:{}/callback", recorder.port),
                )
                .env("SUTRA_ENGINE_SHARDS", "4")
                .host_gateway()
                .expected_deployments(1)
                .start(&archives);
            Fixture {
                pg,
                recorder,
                engine,
            }
        })
        .join()
        .expect("wait-race topology")
    })
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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn called_back(prefix: &str) -> (BTreeSet<String>, usize) {
    let bodies = fixture()
        .recorder
        .recorder
        .matching(&format!("\"{prefix}-"));
    let refs = bodies
        .iter()
        .filter_map(|b| serde_json::from_str::<serde_json::Value>(b).ok())
        .filter_map(|v| v["ref"].as_str().map(str::to_string))
        .collect();
    (refs, bodies.len())
}

/// Every order whose answer raced its instance to the wait still settles, well inside the
/// outbox's poison horizon, exactly once; the outbox drains and no answer is left poisoned.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_wait_race_an_answer_that_beats_the_wait_is_held_until_the_wait_parks() {
    let f = fixture();
    let client = reqwest::Client::new();
    let mut accepted = BTreeSet::new();
    for i in 0..ORDERS {
        let r = format!("race-{i:03}");
        let status = client
            .post(format!(
                "http://127.0.0.1:{}/channels/order-in",
                f.engine.http_port
            ))
            .header("Content-Type", "application/json")
            .header("X-Api-Key", API_KEY)
            .body(format!(r#"{{"ref":"{r}"}}"#))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .expect("post order")
            .status();
        assert!(status.is_success(), "{r}: intake answered {status}");
        accepted.insert(r);
    }

    // The timer between the send and the wait is 2 s; the poison horizon is 5 minutes. Anything
    // not settled after 90 s was dropped or poisoned rather than held.
    let deadline = Instant::now() + Duration::from_secs(90);
    let (refs, total) = loop {
        let (refs, total) = called_back("race");
        if accepted.is_subset(&refs) || Instant::now() > deadline {
            break (refs, total);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let lost: Vec<&String> = accepted.difference(&refs).collect();
    assert!(
        lost.is_empty(),
        "answers that raced the wait were lost: {lost:?}\noutbox: {}",
        sql(
            "SELECT string_agg(destination||' att='||attempt_count||' diag='||\
             coalesce(left(last_diagnostic_json,240),'-'), E'\\n') FROM outbox_entry"
        )
    );
    assert_eq!(total, ORDERS, "each order called back exactly once");

    // Every intake instance completed, and the outbox holds nothing (no poisoned answer).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let pending: i64 = sql("SELECT count(*) FROM outbox_entry")
            .parse()
            .unwrap_or(-1);
        if pending == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "outbox still holds {pending} rows"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let unfinished: i64 = sql(
        "SELECT count(*) FROM instance_state s JOIN alias_index a USING (deployment_id, instance_id) \
         WHERE a.alias_name = 'orderRef' AND a.alias_value LIKE 'race-%' AND s.terminal_at IS NULL",
    )
    .parse()
    .unwrap_or(-1);
    assert_eq!(unfinished, 0, "every intake instance completed");
}
