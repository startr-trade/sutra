//! Conformance: `event.body` is the raw message, so in a flow that declares masking it must be
//! masked wherever the declared payload is, end to end on the engine image with the package in
//! `fixtures/masking/`.
//!
//! `masked` declares its payload sensitive and audits with `capture="payload"`, and the engine has a
//! master key, so a parked instance must not show the raw body on any surface: the inspect API, the
//! JSONL audit file, the audit table, or the stored snapshot. `plain` declares nothing and shows it,
//! which is what proves the same surfaces are being looked at.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "masking-conformance-key";
const MASTER_KEY: &str = "masking-conformance-master-key-0123456789abcdef";
const JSONL_PATH: &str = "/tmp/sutra-audit/audit.jsonl";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/masking/default--masking--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("masking");
            let engine = EngineBuilder::new("masking", &pg)
                .env("MASKING_API_KEY", API_KEY)
                .env("SUTRA_CRYPTO_MASTER_KEY", MASTER_KEY)
                .env("SUTRA_AUDIT_JSONL", JSONL_PATH)
                .expected_deployments(1)
                .start(&archives);
            Fixture { pg, engine }
        })
        .join()
        .expect("masking topology")
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

fn jsonl_file() -> String {
    let dir = util::world_readable_temp_dir("sutra-masking-jsonl");
    let target = dir.join("audit.jsonl");
    let out = Command::new("docker")
        .args([
            "cp",
            &format!("{}:{JSONL_PATH}", fixture().engine.container.id()),
            &target.to_string_lossy(),
        ])
        .output()
        .expect("docker cp");
    let body = if out.status.success() {
        std::fs::read_to_string(&target).unwrap_or_default()
    } else {
        String::new()
    };
    let _ = std::fs::remove_dir_all(&dir);
    body
}

const AUTHORIZATION: &str = "Bearer RAW-BEARER-TOKEN";
const CUSTOM: &str = "RAW-CUSTOM-HEADER";
/// A traceparent whose trace id can be looked for in the outbox.
const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

async fn post_with(channel: &str, api_key: &str, extra: &[(&str, &str)], body: String) -> u16 {
    let mut request = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", api_key)
        .header("Authorization", AUTHORIZATION)
        .header("X-Custom", CUSTOM)
        .header("traceparent", format!("00-{TRACE_ID}-00f067aa0ba902b7-01"))
        .body(body)
        .timeout(Duration::from_secs(30));
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    request.send().await.expect("post").status().as_u16()
}

async fn post(channel: &str, body: String) {
    let status = post_with(channel, API_KEY, &[], body).await;
    assert!((200..300).contains(&status), "{channel}: {status}");
}

/// The parked instance holding `secret` under the alias value `reference`.
async fn parked_instance(reference: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let instance = sql(&format!(
            "SELECT s.instance_id FROM instance_state s JOIN alias_index a \
             USING (deployment_id, instance_id) WHERE a.alias_value = '{reference}'"
        ));
        if !instance.is_empty() {
            return instance;
        }
        assert!(Instant::now() < deadline, "{reference} never parked");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The audit file's lines for `instance`: the file is shared with every flow of this engine, and a
/// flow that declares no masking writes its headers to it.
fn audit_lines_of(instance: &str) -> String {
    let file = jsonl_file();
    let lines: Vec<&str> = file.lines().filter(|l| l.contains(instance)).collect();
    lines.join("\n")
}

/// Which surfaces show `secret` for `instance`.
async fn surfaces_showing(secret: &str, instance: &str) -> Vec<&'static str> {
    let inspect = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/sutra/instances/{instance}",
            fixture().engine.http_port
        ))
        .send()
        .await
        .expect("inspect")
        .text()
        .await
        .expect("inspect body");
    let snapshot = sql(&format!(
        "SELECT position(convert_to('{secret}','UTF8') in serialised) > 0 \
         FROM instance_state WHERE instance_id = '{instance}'"
    ));
    let audit_table = sql(&format!(
        "SELECT count(*) FROM audit_event a \
         WHERE a.instance_id = '{instance}' AND a::text LIKE '%{secret}%'"
    ));
    let mut showing = Vec::new();
    if inspect.contains(secret) {
        showing.push("inspect");
    }
    if audit_lines_of(instance).contains(secret) {
        showing.push("audit jsonl");
    }
    if audit_table != "0" {
        showing.push("audit table");
    }
    if snapshot == "t" {
        showing.push("snapshot");
    }
    showing
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_masking_a_declared_flow_shows_the_raw_body_on_no_surface_and_still_resumes() {
    fixture();
    let secret = "RAW-SECRET-MASKED-1";
    post(
        "masked-in",
        format!(r#"{{"ref":"masked-1","card":"{secret}"}}"#),
    )
    .await;
    let instance = parked_instance("masked-1").await;
    let showing = surfaces_showing(secret, &instance).await;
    println!("masked flow, surfaces showing the raw body: {showing:?}");
    assert!(showing.is_empty(), "the raw body leaked to {showing:?}");
    for header in [API_KEY, AUTHORIZATION, CUSTOM] {
        let showing = surfaces_showing(header, &instance).await;
        println!("masked flow, surfaces showing header {header}: {showing:?}");
        assert!(showing.is_empty(), "{header} leaked to {showing:?}");
    }

    // The encrypted snapshot still resumes.
    post("status-in", r#"{"ref":"masked-1"}"#.to_string()).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let finished = sql(&format!(
            "SELECT terminal_at IS NOT NULL FROM instance_state WHERE instance_id = '{instance}'"
        ));
        if finished == "t" {
            break;
        }
        assert!(Instant::now() < deadline, "the instance never resumed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let showing = surfaces_showing(secret, &instance).await;
    assert!(showing.is_empty(), "after resuming: {showing:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_masking_a_flow_that_declares_nothing_shows_it_as_before() {
    fixture();
    let secret = "RAW-SECRET-PLAIN-1";
    post(
        "plain-in",
        format!(r#"{{"ref":"plain-1","card":"{secret}"}}"#),
    )
    .await;
    let instance = parked_instance("plain-1").await;
    let showing = surfaces_showing(secret, &instance).await;
    println!("plain flow, surfaces showing the raw body: {showing:?}");
    assert!(showing.contains(&"inspect"), "{showing:?}");
    assert!(showing.contains(&"snapshot"), "{showing:?}");
    // Unchanged for a flow that declares nothing: its headers still show. The release notes say so.
    for header in [API_KEY, AUTHORIZATION, CUSTOM] {
        let showing = surfaces_showing(header, &instance).await;
        assert!(showing.contains(&"inspect"), "{header}: {showing:?}");
        assert!(showing.contains(&"snapshot"), "{header}: {showing:?}");
    }
}

/// Masking changes what the surfaces show, not what the engine does with a header.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_masking_auth_idempotency_correlation_and_tracing_still_work() {
    fixture();
    // Inbound auth reads the real header.
    let refused = post_with(
        "masked-in",
        "wrong-key",
        &[],
        r#"{"ref":"auth-1"}"#.to_string(),
    )
    .await;
    assert_eq!(refused, 401, "a wrong key is refused");

    // The idempotency key reads the real header: the repeat is suppressed.
    for _ in 0..2 {
        let status = post_with(
            "masked-in",
            API_KEY,
            &[("X-Event-Id", "idem-1")],
            r#"{"ref":"idem-ref"}"#.to_string(),
        )
        .await;
        assert!((200..300).contains(&status), "{status}");
    }
    let instance = parked_instance("idem-ref").await;
    assert_eq!(
        sql("SELECT count(*) FROM inbox_seen WHERE event_id = 'idem-1'"),
        "1"
    );
    assert_eq!(
        sql("SELECT count(DISTINCT instance_id) FROM alias_index WHERE alias_value = 'idem-ref'"),
        "1",
        "the repeat started nothing: {instance}"
    );

    // Correlation reads the real header, a flow can copy one, and the copy resumes.
    let status = post_with(
        "header-in",
        API_KEY,
        &[("xcase", "HEADER-CASE-1")],
        r#"{"ref":"ignored"}"#.to_string(),
    )
    .await;
    assert!((200..300).contains(&status), "{status}");
    let by_header = parked_instance("HEADER-CASE-1").await;
    let inspected = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{}/sutra/instances/{by_header}",
            fixture().engine.http_port
        ))
        .send()
        .await
        .expect("inspect")
        .text()
        .await
        .expect("inspect body");
    println!("by-header inspect: {inspected}");
    post("header-status-in", r#"{"ref":"HEADER-CASE-1"}"#.to_string()).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while sql(&format!(
        "SELECT terminal_at IS NOT NULL FROM instance_state WHERE instance_id = '{by_header}'"
    )) != "t"
    {
        assert!(Instant::now() < deadline, "correlating on a header failed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Tracing reads the real traceparent: the flow's send carries its trace id onward.
    assert_eq!(
        sql(&format!(
            "SELECT count(*) FROM outbox_entry \
             WHERE instance_id = '{by_header}' AND traceparent LIKE '%{TRACE_ID}%'"
        )),
        "1",
        "the send lost the inbound trace context"
    );
}
