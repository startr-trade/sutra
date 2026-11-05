//! Conformance: a message kept as raw text in a typed store column, and read back decoded, end to
//! end on the engine image with the package in `fixtures/raw-message/`.
//!
//! `capture` writes `event.body` to a TEXT column and `retrieve` reads it, runs `<q:decode>` and
//! answers from the decoded fields. `held` masks: it captures, parks, resumes and only then decodes,
//! with `event` and the capture variable ciphertext in the parked row.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "raw-message-conformance-key";
const MASTER_KEY: &str = "raw-message-conformance-master-key-0123456789";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/raw-message/default--rawmsg--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("rawmsg");
            let engine = EngineBuilder::new("rawmsg", &pg)
                .env("RAW_MESSAGE_API_KEY", API_KEY)
                .env("SUTRA_CRYPTO_MASTER_KEY", MASTER_KEY)
                .env("SUTRA_AUDIT_JSONL", "/tmp/sutra-audit/audit.jsonl")
                .env(
                    "STORE_DB_URL",
                    format!("postgresql://{}:5432/postgres", pg.container_name),
                )
                .env("STORE_DB_USER", "postgres")
                .env("STORE_DB_PASSWORD", "postgres")
                .expected_deployments(1)
                .start(&archives);
            Fixture { pg, engine }
        })
        .join()
        .expect("raw-message topology")
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
    String::from_utf8_lossy(&out.stdout)
        .trim_end_matches('\n')
        .to_string()
}

async fn send(
    channel: &str,
    content_type: &str,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> (u16, String) {
    let mut request = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", content_type)
        .header("X-Api-Key", API_KEY)
        .body(body)
        .timeout(Duration::from_secs(30));
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("post");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// Keep `body` under `id`, and answer the stored row's text once it exists.
async fn capture(id: &str, content_type: &str, body: &[u8]) -> String {
    let (status, text) = send(
        "message-in",
        content_type,
        &[("X-Message-Id", id)],
        body.to_vec(),
    )
    .await;
    assert!(
        (200..300).contains(&status),
        "capture {id}: {status} {text}"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stored = sql(&format!(
            "SELECT body FROM messages WHERE store_key = '{id}'"
        ));
        if !stored.is_empty() {
            return stored;
        }
        assert!(Instant::now() < deadline, "{id} was never stored");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn read_back(id: &str) -> serde_json::Value {
    let (status, text) = send(
        "message-read",
        "application/json",
        &[],
        format!(r#"{{"id":"{id}"}}"#).into_bytes(),
    )
    .await;
    assert!((200..300).contains(&status), "read {id}: {status} {text}");
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_raw_message_text_column_round_trip() {
    let _suite = sutra_testkit::hold_suite!("raw_message");
    fixture();
    // Odd spacing, key order, a number's written scale and non-ASCII: a parse and re-serialise would
    // change all of them, so the exact text in the column proves nothing parsed it.
    let text = "{ \"count\" : 3.50,\n  \"subject\":\"caf\u{e9} \u{1f600}\" }";
    let stored = capture("json-1", "application/json", text.as_bytes()).await;
    assert_eq!(stored, text, "the column holds the exact text");
    let answer = read_back("json-1").await;
    assert_eq!(answer["decoded"], true);
    assert_eq!(answer["subject"], "caf\u{e9} \u{1f600}");
    assert_eq!(answer["count"].to_string(), "3.50");

    // The row's own content type picks the parser.
    let xml = "<note><subject>hello</subject><count>2</count></note>";
    assert_eq!(
        capture("xml-1", "application/xml", xml.as_bytes()).await,
        xml
    );
    let answer = read_back("xml-1").await;
    assert_eq!(answer["subject"], "hello");

    assert_eq!(
        sql("SELECT content_type FROM messages WHERE store_key = 'xml-1'"),
        "application/xml",
        "the typed column beside it"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_raw_message_undecodable_text_takes_the_error_boundary() {
    let _suite = sutra_testkit::hold_suite!("raw_message");
    fixture();
    let text = "{ this is not json";
    assert_eq!(
        capture("bad-1", "application/json", text.as_bytes()).await,
        text
    );
    assert_eq!(read_back("bad-1").await["decoded"], false);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_raw_message_flow_routes_a_lossy_body_away() {
    let _suite = sutra_testkit::hold_suite!("raw_message");
    fixture();
    let (status, text) = send(
        "message-in",
        "application/octet-stream",
        &[("X-Message-Id", "lossy-1")],
        vec![b'o', b'k', 0xFF, 0xFE],
    )
    .await;
    assert!((200..300).contains(&status), "{status} {text}");
    // The clean message after it is kept, so the lossy one has had its turn.
    capture("after-lossy", "application/json", b"{}").await;
    assert_eq!(
        sql("SELECT count(*) FROM messages WHERE store_key = 'lossy-1'"),
        "0"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_raw_message_survives_park_with_redactor() {
    let _suite = sutra_testkit::hold_suite!("raw_message");
    fixture();
    let secret = "RAW-CARD-TEXT-1";
    let reference = "held-1";
    let body = format!(r#"{{"ref":"{reference}","card":"{secret}"}}"#);
    let (status, text) = send("held-in", "application/json", &[], body.into_bytes()).await;
    assert!((200..300).contains(&status), "{status} {text}");

    let deadline = Instant::now() + Duration::from_secs(30);
    let instance = loop {
        let instance = sql(&format!(
            "SELECT s.instance_id FROM instance_state s JOIN alias_index a \
             USING (deployment_id, instance_id) WHERE a.alias_value = '{reference}'"
        ));
        if !instance.is_empty() {
            break instance;
        }
        assert!(Instant::now() < deadline, "never parked");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    // Parked: neither `event` nor the capture variable is readable in the row or through inspect.
    assert_eq!(
        sql(&format!(
            "SELECT position(convert_to('{secret}','UTF8') in serialised) > 0 \
             FROM instance_state WHERE instance_id = '{instance}'"
        )),
        "f",
        "the parked row holds ciphertext"
    );
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
    assert!(!inspect.contains(secret), "inspect shows it: {inspect}");

    // Resume: the capture decodes after the wait, and the relay's `event` didn't replace it.
    let (status, text) = send(
        "held-go",
        "application/json",
        &[],
        format!(r#"{{"ref":"{reference}"}}"#).into_bytes(),
    )
    .await;
    assert!((200..300).contains(&status), "{status} {text}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stored = sql(&format!(
            "SELECT store_value FROM data_store WHERE store_name = 'outcomes' \
             AND store_key = '{reference}'"
        ));
        if !stored.is_empty() {
            assert!(stored.contains(reference), "{stored}");
            break;
        }
        assert!(Instant::now() < deadline, "never decoded after the wait");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
