//! Conformance: a delivery with an explicit event id is refused while it cannot be checked for a
//! duplicate, end to end on real PostgreSQL with the package in `fixtures/dedup-refusal/`.
//!
//! The database is stopped, so the check cannot answer. A delivery then gets a 503 carrying the
//! refusal's code, whether or not its flow would have touched the database. Once the database is
//! back, the sender's retry of the same id runs exactly once, and a further retry is a duplicate.
//! A delivery with no key to protect never reaches the check.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::callback::{start_recorder, HostRecorder};
use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "dedup-refusal-conformance-key";
const REFUSAL: &str = "SUTRA.INBOUND.DEDUP_CHECK_UNAVAILABLE";

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
                .join("fixtures/dedup-refusal/default--dedupref--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("dedupref");
            let recorder = start_recorder();
            let engine = EngineBuilder::new("dedupref", &pg)
                .env("DEDUP_REFUSAL_API_KEY", API_KEY)
                .env(
                    "DEDUP_REFUSAL_CALLBACK_URL",
                    format!("http://host.docker.internal:{}/callback", recorder.port),
                )
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
        .expect("dedup-refusal topology")
    })
}

fn docker_pg(action: &str) {
    let status = Command::new("docker")
        .args([action, fixture().pg.container.id()])
        .status()
        .expect("docker");
    assert!(status.success(), "docker {action} postgres");
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

/// POST one event; answers the status and the body. A missing `event_id` sends no id at all.
async fn post(channel: &str, event_id: Option<&str>, reference: &str) -> (u16, String) {
    let mut request = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(format!(r#"{{"ref":"{reference}"}}"#))
        .timeout(Duration::from_secs(90));
    if let Some(id) = event_id {
        // The header a channel or a flow names, and the CloudEvents id a flow can declare instead.
        request = request.header("X-Event-Id", id).header("ce-id", id);
    }
    let response = request.send().await.expect("post");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

fn callbacks(reference: &str) -> usize {
    fixture()
        .recorder
        .recorder
        .matching(&format!("\"{reference}\""))
        .len()
}

/// Repeat the sender's retry until the engine accepts it, as a sender would.
async fn retry_until_accepted(channel: &str, event_id: &str, reference: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let (status, body) = post(channel, Some(event_id), reference).await;
        if (200..300).contains(&status) {
            return;
        }
        assert_eq!(
            status, 503,
            "only a retryable refusal is acceptable: {body}"
        );
        assert!(Instant::now() < deadline, "never accepted: {body}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_dedup_refusal_a_delivery_that_cannot_be_checked_is_retried_and_processed_once() {
    let _suite = sutra_testkit::hold_suite!("dedupref");
    fixture();
    // The same id twice while healthy: the suppression this refusal protects.
    let (status, _) = post("event-in", Some("healthy-1"), "healthy").await;
    assert!((200..300).contains(&status), "{status}");
    let (status, _) = post("event-in", Some("healthy-1"), "healthy").await;
    assert!(
        (200..300).contains(&status),
        "a duplicate answers 2xx: {status}"
    );

    // The same, from a key only the flow declares.
    for (channel, id, reference) in [
        ("declared-header-in", "declared-h-1", "declared-header"),
        ("declared-ce-in", "declared-c-1", "declared-ce"),
    ] {
        for _ in 0..2 {
            let (status, _) = post(channel, Some(id), reference).await;
            assert!((200..300).contains(&status), "{channel}: {status}");
        }
    }

    docker_pg("stop");
    let (send_status, send_body) = post("event-in", Some("outage-1"), "outage").await;
    let (quiet_status, quiet_body) = post("quiet-in", Some("outage-2"), "outage-quiet").await;
    let (header_status, header_body) =
        post("declared-header-in", Some("outage-3"), "outage-header").await;
    let (ce_status, ce_body) = post("declared-ce-in", Some("outage-4"), "outage-ce").await;
    docker_pg("start");

    for (status, body) in [(header_status, &header_body), (ce_status, &ce_body)] {
        assert_eq!(status, 503, "a declared key is refused too: {body}");
        assert!(body.contains(REFUSAL), "{body}");
    }

    println!("refusal: {send_status} {send_body}");
    assert_eq!(send_status, 503, "{send_body}");
    assert!(send_body.contains(REFUSAL), "{send_body}");
    // A flow that persists nothing would have answered normally, unchecked.
    assert_eq!(quiet_status, 503, "{quiet_body}");
    assert!(quiet_body.contains(REFUSAL), "{quiet_body}");

    // The sender retries the same ids.
    retry_until_accepted("event-in", "outage-1", "outage").await;
    retry_until_accepted("quiet-in", "outage-2", "outage-quiet").await;
    retry_until_accepted("declared-header-in", "outage-3", "outage-header").await;
    retry_until_accepted("declared-ce-in", "outage-4", "outage-ce").await;
    // A further retry after success is a duplicate, not a second run.
    let (status, _) = post("event-in", Some("outage-1"), "outage").await;
    assert!((200..300).contains(&status), "{status}");

    let deadline = Instant::now() + Duration::from_secs(60);
    while callbacks("outage") == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        callbacks("healthy"),
        1,
        "the healthy duplicate was suppressed"
    );
    assert_eq!(callbacks("outage"), 1, "processed exactly once");
    assert_eq!(
        callbacks("declared-header"),
        1,
        "the declared header dedupes"
    );
    assert_eq!(callbacks("declared-ce"), 1, "the declared id dedupes");
    for id in [
        "outage-1",
        "outage-2",
        "healthy-1",
        "declared-h-1",
        "declared-c-1",
        "outage-3",
        "outage-4",
    ] {
        assert_eq!(
            sql(&format!(
                "SELECT count(*) FROM inbox_seen WHERE event_id = '{id}'"
            )),
            "1",
            "{id} is recorded"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_dedup_refusal_a_delivery_with_no_explicit_id_never_consults_the_check() {
    let _suite = sutra_testkit::hold_suite!("dedupref");
    fixture();
    let (first, _) = post("quiet-in", None, "no-id").await;
    let (second, _) = post("quiet-in", None, "no-id").await;
    assert!((200..300).contains(&first), "{first}");
    assert!((200..300).contains(&second), "{second}");
}
