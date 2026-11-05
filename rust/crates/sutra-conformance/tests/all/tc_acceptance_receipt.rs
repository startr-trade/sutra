//! Conformance: the engine's acceptance receipt, end to end against the engine image on real
//! PostgreSQL. A 202 carries `Sutra-Instance-Id` and `Sutra-Receipt`; a flow that replied nothing
//! gets the receipt as its body, and a flow-authored body stays byte for byte; a duplicate names
//! the instance the first delivery started; attributes are the flow's choice and are masked; and
//! `GET /sutra/instances?alias=<name>:<value>` finds what a lost 202 would have named. The package
//! lives in `fixtures/acceptance-receipt/`.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "acceptance-receipt-test-key";
const MASK: &str = "***REDACTED***";

struct Fixture {
    _pg: PgFixture,
    engine: engine::EngineHandle,
}

fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/acceptance-receipt/default--receipt--1.0.0");
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
            let pg = engine::start_postgres("receipt");
            let engine = EngineBuilder::new("receipt", &pg)
                .env("RECEIPT_API_KEY", API_KEY)
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("acceptance-receipt topology")
    })
}

const ADMIN_KEY: &str = "acceptance-receipt-admin-key";

/// The same package on an engine with admin auth on.
fn gated_fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("receipt-gated");
            let engine = EngineBuilder::new("receipt-gated", &pg)
                .env("RECEIPT_API_KEY", API_KEY)
                .env("SUTRA_ADMIN_AUTH_SCHEME", "apikey")
                .env("SUTRA_ADMIN_AUTH_KEY_REF", "env:RECEIPT_ADMIN_KEY")
                .env("RECEIPT_ADMIN_KEY", ADMIN_KEY)
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("gated acceptance-receipt topology")
    })
}

struct Answer {
    status: u16,
    instance_header: Option<String>,
    receipt_header: Value,
    content_type: String,
    body: String,
}

async fn post(channel: &str, key: Option<&str>, body: Value) -> Answer {
    let mut request = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.to_string());
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let resp = request.send().await.expect("engine request");
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .map(|v| v.to_str().expect("an ASCII header").to_string())
    };
    let instance_header = header("Sutra-Instance-Id");
    let receipt_header = header("Sutra-Receipt")
        .map(|v| serde_json::from_str(&v).expect("the receipt header is JSON"))
        .unwrap_or(Value::Null);
    let content_type = header("Content-Type").unwrap_or_default();
    let status = resp.status().as_u16();
    Answer {
        status,
        instance_header,
        receipt_header,
        content_type,
        body: resp.text().await.unwrap_or_default(),
    }
}

async fn get_json(path: &str) -> (u16, Value) {
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}{path}",
        fixture().engine.http_port
    ))
    .await
    .expect("engine GET");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

fn listed_ids(list: &Value) -> Vec<String> {
    list["instances"]
        .as_array()
        .expect("an instance list")
        .iter()
        .map(|i| i["instanceId"].as_str().expect("instanceId").to_string())
        .collect()
}

/// A flow that replies nothing: the receipt is the body, the headers repeat it, the deployment
/// answers to its labels, and the instance it names is the one the operate surface shows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_of_a_silent_flow_is_the_json_body_and_the_headers() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let a = post(
        "case-open",
        Some("open-1"),
        serde_json::json!({"orderId": "O-1", "caseId": "C-1", "holderId": "H-SECRET-1"}),
    )
    .await;
    assert_eq!(a.status, 202, "{}", a.body);
    assert!(a.content_type.starts_with("application/json"));
    let body: Value = serde_json::from_str(&a.body).expect("the receipt is the body");

    let id = a.instance_header.clone().expect("Sutra-Instance-Id");
    assert_eq!(body["instanceId"], Value::String(id.clone()));
    assert_eq!(a.receipt_header, body, "the header and the body agree");
    assert!(body["deploymentId"].as_str().unwrap().starts_with("dep-"));
    assert_eq!(body["labels"]["module"], "receipt");
    assert_eq!(body["labels"]["tenant"], "default");
    assert_eq!(body["labels"]["version"], "1.0.0");
    assert_eq!(body["duplicate"], false);
    assert_eq!(body["idempotencyKey"], "open-1");
    assert_eq!(body["aliases"]["orderId"], "O-1");

    // Read back: the instance the receipt names exists, parked at the wait.
    let (status, detail) = get_json(&format!("/sutra/instances/{id}")).await;
    assert_eq!(status, 200, "{detail}");
    assert_eq!(detail["status"], "SUSPENDED", "{detail}");
    assert_eq!(
        detail["deploymentId"], body["deploymentId"],
        "the deployment named is the instance's"
    );
}

/// Attributes are the flow's choice: a payload value, a value computed before the wait (which an
/// evaluation at intake could not see), a sensitive variable masked, a missing field left out. An
/// alias over a sensitive variable is masked too, and the raw value is nowhere in the answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_attributes_are_declared_and_redacted() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let a = post(
        "case-open",
        None,
        serde_json::json!({"orderId": "O-2", "caseId": "C-2", "holderId": "H-SECRET-2"}),
    )
    .await;
    assert_eq!(a.status, 202, "{}", a.body);
    let body: Value = serde_json::from_str(&a.body).unwrap();
    assert_eq!(body["attributes"]["caseId"], "C-2", "{body}");
    assert_eq!(body["attributes"]["computed"], "run-C-2", "{body}");
    assert_eq!(body["attributes"]["holder"], MASK, "{body}");
    assert!(body["attributes"].get("absent").is_none(), "{body}");
    assert_eq!(body["aliases"]["holderId"], MASK, "{body}");
    assert!(!a.body.contains("H-SECRET-2"), "{}", a.body);
    assert!(!a.receipt_header.to_string().contains("H-SECRET-2"));
    // No key was supplied, so none is echoed: the body hash recognises no retry.
    assert!(body.get("idempotencyKey").is_none(), "{body}");
}

/// A flow-authored 202 body stays byte for byte; the receipt rides only in the headers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_leaves_a_flow_authored_body_unchanged() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let a = post(
        "case-authored",
        Some("authored-1"),
        serde_json::json!({"caseId": "C-3"}),
    )
    .await;
    assert_eq!(a.status, 202, "{}", a.body);
    assert_eq!(a.body.trim(), r#"{"status":"accepted by the flow"}"#);
    let id = a.instance_header.expect("Sutra-Instance-Id");
    assert_eq!(a.receipt_header["instanceId"], Value::String(id));
    assert_eq!(a.receipt_header["attributes"]["caseId"], "C-3");
    assert_eq!(a.receipt_header["idempotencyKey"], "authored-1");

    // A duplicate of an authored delivery gets the receipt: nothing ran to author a body.
    let again = post(
        "case-authored",
        Some("authored-1"),
        serde_json::json!({"caseId": "C-3"}),
    )
    .await;
    let dup: Value = serde_json::from_str(&again.body).expect("a duplicate answers a receipt");
    assert_eq!(dup["duplicate"], true);
    assert_eq!(dup["originalInstanceId"], a.receipt_header["instanceId"]);
}

/// A parked instance's duplicate names the instance the first delivery started and starts nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_of_a_duplicate_names_its_original() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let payload = serde_json::json!({"orderId": "O-4", "caseId": "C-4", "holderId": "H-4"});
    let first = post("case-open", Some("dup-1"), payload.clone()).await;
    assert_eq!(first.status, 202, "{}", first.body);
    let original = first.instance_header.expect("Sutra-Instance-Id");

    let second = post("case-open", Some("dup-1"), payload).await;
    assert_eq!(second.status, 202, "{}", second.body);
    assert_eq!(second.instance_header.as_deref(), Some(original.as_str()));
    let body: Value = serde_json::from_str(&second.body).expect("the receipt is the body");
    assert_eq!(body["duplicate"], true, "{body}");
    assert_eq!(body["originalInstanceId"], Value::String(original.clone()));
    assert_eq!(body["instanceId"], Value::String(original.clone()));
    assert_eq!(body["idempotencyKey"], "dup-1");

    let (_, list) = get_json("/sutra/instances?alias=orderId:O-4").await;
    assert_eq!(listed_ids(&list), vec![original], "no second instance");
}

/// A flow that runs to its end persists nothing on its way, so the end of the dispatch writes the
/// dedup record: it keeps the instance as well. The key comes from the body, by `dedupKey`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_of_a_run_to_end_duplicate_names_its_original() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let payload = serde_json::json!({"ref": "R-5"});
    let first = post("case-run", None, payload.clone()).await;
    assert_eq!(first.status, 202, "{}", first.body);
    let original = first.instance_header.expect("Sutra-Instance-Id");
    let body: Value = serde_json::from_str(&first.body).unwrap();
    assert_eq!(body["duplicate"], false);
    assert_eq!(body["idempotencyKey"], "R-5", "the derived key is echoed");

    let second = post("case-run", None, payload).await;
    let body: Value = serde_json::from_str(&second.body).expect("the receipt is the body");
    assert_eq!(body["duplicate"], true, "{body}");
    assert_eq!(body["originalInstanceId"], Value::String(original));
    assert_eq!(body["idempotencyKey"], "R-5");
}

/// `?alias=` finds a live instance; a malformed one is the caller's error; a resume answers with a
/// receipt of its own; and a finished instance is found only when history is asked for, because
/// its aliases retired with it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_alias_lookup_finds_the_instance_a_lost_202_named() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let a = post(
        "case-open",
        None,
        serde_json::json!({"orderId": "O:6", "caseId": "C-6", "holderId": "H-6"}),
    )
    .await;
    assert_eq!(a.status, 202, "{}", a.body);
    let id = a.instance_header.expect("Sutra-Instance-Id");

    // The value holds a colon: the split is at the first one.
    let (status, list) = get_json("/sutra/instances?alias=orderId:O%3A6").await;
    assert_eq!(status, 200, "{list}");
    assert_eq!(listed_ids(&list), vec![id.clone()], "{list}");

    let (status, list) = get_json("/sutra/instances?alias=orderId:no-such-order").await;
    assert_eq!(status, 200, "{list}");
    assert!(listed_ids(&list).is_empty(), "{list}");

    let (status, _) = get_json("/sutra/instances?alias=orderId").await;
    assert_eq!(status, 400, "no colon is a malformed alias");

    let release = post("case-release", None, serde_json::json!({"orderId": "O:6"})).await;
    assert_eq!(release.status, 202, "{}", release.body);
    assert_eq!(
        release.instance_header.as_deref(),
        Some(id.as_str()),
        "the resume names the instance it resumed: {}",
        release.body
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, detail) = get_json(&format!("/sutra/instances/{id}")).await;
        if detail["status"] == "COMPLETED" {
            break;
        }
        assert!(Instant::now() < deadline, "never completed: {detail}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (_, list) = get_json("/sutra/instances?alias=orderId:O%3A6").await;
    assert!(listed_ids(&list).is_empty(), "retired with the instance");
    let (_, list) = get_json("/sutra/instances?alias=orderId:O%3A6&includeTerminal=true").await;
    assert_eq!(listed_ids(&list), vec![id], "{list}");
}

/// With admin auth on, the list, the alias search and one instance answer only to the admin key,
/// and hold real data behind it. `tc_receipt_of_a_silent_flow_is_the_json_body_and_the_headers`
/// and the alias tests above are the same reads answered with admin auth off.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_receipt_instance_reads_answer_only_to_the_admin_key_when_admin_auth_is_on() {
    let _suite = sutra_testkit::hold_suite!("receipt");
    let base = format!("http://127.0.0.1:{}", gated_fixture().engine.http_port);
    let client = reqwest::Client::new();
    let accepted = client
        .post(format!("{base}/channels/case-open"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(r#"{"orderId":"O-GATE","caseId":"C-GATE","holderId":"H-GATE"}"#)
        .send()
        .await
        .expect("the start");
    assert_eq!(accepted.status(), 202);
    let id = accepted.headers()["Sutra-Instance-Id"]
        .to_str()
        .unwrap()
        .to_string();

    for path in [
        "/sutra/instances".to_string(),
        "/sutra/instances?alias=orderId:O-GATE".to_string(),
        format!("/sutra/instances/{id}"),
    ] {
        let url = format!("{base}{path}");
        let none = client.get(&url).send().await.expect("no key");
        assert_eq!(none.status(), 401, "{path} with no key");
        let wrong = client
            .get(&url)
            .header("X-API-Key", "not-the-key")
            .send()
            .await
            .expect("wrong key");
        assert_eq!(wrong.status(), 401, "{path} with the wrong key");
        let right = client
            .get(&url)
            .header("X-API-Key", ADMIN_KEY)
            .send()
            .await
            .expect("right key");
        assert_eq!(right.status(), 200, "{path} with the right key");
        let body = right.text().await.expect("a body");
        assert!(body.contains(&id), "{path} names the instance: {body}");
    }
}
