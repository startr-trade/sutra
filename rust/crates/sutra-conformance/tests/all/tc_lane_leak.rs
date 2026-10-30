//! Conformance: a start that sends and then fails leaves nothing behind on its engine lane —
//! end to end against the engine image on real PostgreSQL, with the package in
//! `fixtures/lane-leak/`.
//!
//! `send-then-fail` performs a `<q:send>` and then ends in an uncaught BPMN error. Its graph has
//! a timer wait on another branch, so it starts on the wait-state path. The send never reached a
//! quiescent point, so it must be dropped with the failed step.
//!
//! It used to stay in the lane's emission buffer. The next request on the lane then took it as
//! its own: a request whose process parks committed it with its park step, which the store
//! refuses (`outbox entry … does not belong to step instance`), so an unrelated caller got an
//! HTTP 500. A request whose process runs to its end committed it with its own sends, so the
//! failed process's message went out. The suite drives both and checks the outbox table.

use std::process::Command;
use std::sync::OnceLock;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "lane-leak-test-key";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/lane-leak/default--laneleak--1.0.0");
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
            let pg = engine::start_postgres("laneleak");
            let engine = EngineBuilder::new("laneleak", &pg)
                .env("LANELEAK_API_KEY", API_KEY)
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { pg, engine }
        })
        .join()
        .expect("lane-leak topology")
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_lane_leak_a_failed_send_never_reaches_the_next_request() {
    let client = reqwest::Client::new();

    // A sends and fails; the next request on the lane parks. It must succeed on its own terms.
    let (status, body) = post(&client, "fail", "LL-A-1").await;
    assert!(
        !status.is_success(),
        "send-then-fail must fail: {status} {body}"
    );
    let (status, body) = post(&client, "park", "LL-B-1").await;
    assert!(
        status.is_success(),
        "an unrelated parking request after a failed send answered {status}: {body}"
    );

    // Again, and this time the next request runs to its end with a send of its own.
    let (status, body) = post(&client, "fail", "LL-A-2").await;
    assert!(
        !status.is_success(),
        "send-then-fail must fail: {status} {body}"
    );
    let (status, body) = post(&client, "straight", "LL-C-1").await;
    assert!(
        status.is_success(),
        "an unrelated run-to-end request after a failed send answered {status}: {body}"
    );

    // Several failures in a row, then a parking request: nothing accumulates.
    for n in 3..6 {
        post(&client, "fail", &format!("LL-A-{n}")).await;
    }
    let (status, body) = post(&client, "park", "LL-B-2").await;
    assert!(
        status.is_success(),
        "parking request answered {status}: {body}"
    );

    // The outbox holds the successful requests' sends and none of the failed process's.
    assert_eq!(
        count("SELECT count(*) FROM outbox_entry WHERE destination LIKE '%/from-fail'"),
        0,
        "a send of the failed process was committed"
    );
    assert_eq!(
        count("SELECT count(*) FROM outbox_entry WHERE destination LIKE '%/from-park'"),
        2,
        "each parking request committed exactly its own send"
    );
    assert_eq!(
        count("SELECT count(*) FROM outbox_entry WHERE destination LIKE '%/from-straight'"),
        1,
        "the run-to-end request committed exactly its own send"
    );
    // No committed outbox row belongs to anything but a parked or completed request: every row's
    // instance carries one of the successful sends.
    assert_eq!(
        count(
            "SELECT count(DISTINCT destination) FROM outbox_entry o WHERE EXISTS \
             (SELECT 1 FROM outbox_entry p WHERE p.instance_id = o.instance_id \
              AND p.destination <> o.destination)"
        ),
        0,
        "no instance's step carried another process's send"
    );
}

// ---- helpers ------------------------------------------------------------------------------

async fn post(client: &reqwest::Client, channel: &str, id: &str) -> (reqwest::StatusCode, String) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(format!("{{\"id\":\"{id}\"}}"))
        .send()
        .await
        .expect("engine request");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    eprintln!("POST /channels/{channel} {id} -> {status} {body}");
    (status, body)
}

fn count(query: &str) -> u64 {
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
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    text.parse()
        .unwrap_or_else(|e| panic!("count {text:?}: {e}"))
}
