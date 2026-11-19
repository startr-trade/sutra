//! Conformance: a synchronous HTTP answer sets its own status and response headers with
//! `<q:reply status>` and `<q:header>` — end to end against the engine image on real PostgreSQL.
//!
//! The package (`fixtures/reply-status/`) covers a 201 with `Location`, a status chosen by FEEL
//! (200 or 404), an auth check that answers 200 with identity headers or 401 with a challenge and
//! no body, an expression whose value is not a status (a coded 500), and respond-and-continue,
//! where the 201 and its `Location` reach the caller before the detached tail runs.

use std::sync::OnceLock;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "reply-status-test-key";

struct Fixture {
    _pg: PgFixture,
    engine: engine::EngineHandle,
}

/// Seal the fixture package into a fresh world-readable archives directory.
fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/reply-status/default--replies--1.0.0");
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
            let pg = engine::start_postgres("replies");
            let engine = EngineBuilder::new("replies", &pg)
                .env("REPLIES_API_KEY", API_KEY)
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("reply-status topology")
    })
}

struct Answer {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: String,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

async fn post(client: &reqwest::Client, channel: &str, body: &str) -> Answer {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.to_string())
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    Answer {
        status,
        headers,
        body: resp.text().await.unwrap_or_default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_reply_status_and_headers_reach_the_caller() {
    let _suite = sutra_testkit::hold_suite!("replies");
    let client = reqwest::Client::new();

    // A literal status with headers and a rendered body.
    let a = post(&client, "items", r#"{"id":"7"}"#).await;
    assert_eq!(a.status, 201, "{}", a.body);
    assert_eq!(a.header("location"), Some("/items/7"));
    assert_eq!(a.header("x-request-id"), Some("7"));
    assert_eq!(a.header("content-type"), Some("application/json"));
    assert_eq!(a.body.trim(), r#"{"id":"7"}"#);

    // A status chosen by FEEL.
    assert_eq!(post(&client, "lookup", r#"{"id":"42"}"#).await.status, 200);
    let a = post(&client, "lookup", r#"{"id":"41"}"#).await;
    assert_eq!(a.status, 404, "{}", a.body);
    assert_eq!(a.body.trim(), r#"{"id":"41"}"#, "the flow's own body");

    // An auth check: identity headers, or a challenge, and no body either way.
    let a = post(&client, "check", r#"{"token":"good","user":"u-1"}"#).await;
    assert_eq!(a.status, 200, "{}", a.body);
    assert_eq!(a.header("x-user-id"), Some("u-1"));
    assert!(a.header("www-authenticate").is_none());
    assert!(a.body.is_empty(), "{}", a.body);
    let a = post(&client, "check", r#"{"token":"bad"}"#).await;
    assert_eq!(a.status, 401, "{}", a.body);
    assert_eq!(a.header("www-authenticate"), Some(r#"Bearer realm="sim""#));
    assert!(a.header("x-user-id").is_none());
    assert!(a.body.is_empty(), "{}", a.body);

    // A value that is not a status: refused with the code, nothing half-sent.
    for body in [
        r#"{"code":700}"#,
        r#"{"code":"Created"}"#,
        r#"{"code":301}"#,
    ] {
        let a = post(&client, "bad", body).await;
        assert_eq!(a.status, 500, "{body}: {}", a.body);
        let problem: serde_json::Value = serde_json::from_str(&a.body).expect("problem json");
        assert_eq!(
            problem["code"],
            serde_json::json!("SUTRA.OUTBOUND.REPLY_STATUS_INVALID"),
            "{body}: {problem}"
        );
    }
    assert_eq!(post(&client, "bad", r#"{"code":409}"#).await.status, 409);

    // Respond-and-continue: the caller gets the 201 and its Location at the reply node.
    let a = post(&client, "later", r#"{"id":"9"}"#).await;
    assert_eq!(a.status, 201, "{}", a.body);
    assert_eq!(a.header("location"), Some("/items/9"));
    assert_eq!(a.body.trim(), r#"{"id":"9"}"#);
}
