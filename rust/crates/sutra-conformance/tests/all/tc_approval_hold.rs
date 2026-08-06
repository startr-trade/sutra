//! Conformance: approval-hold — the S-X3 human-in-the-loop wait-state relay + the declarative
//! showcase flows, end to end on real PostgreSQL.
//!
//! Six independent `tc_*` functions share one engine fixture — each drives a distinct
//! correlation id on a stateless flow, so order does not matter. The first walks the full
//! park / duplicate-rejected / relay-resume / retire / uncorrelated-relay lifecycle; the other
//! five each drive one declarative showcase flow (templates, scripts, throw flavours, inline
//! sub-process scope, data mapping) and assert its rendered output.

use std::sync::OnceLock;

use crate::support::engine::{self, EngineBuilder, PgFixture};

const API_KEY: &str = "approval-demo-key";

struct Fixture {
    _pg: PgFixture,
    engine: engine::EngineHandle,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("approval");
            let engine = EngineBuilder::new("approval", &pg)
                .expected_deployments(1)
                .start(&engine::assemble_example("approval-hold"));
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("approval-hold topology")
    })
}

fn port() -> u16 {
    fixture().engine.http_port
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_park_relay_resume_lifecycle() {
    let client = reqwest::Client::new();
    let port = port();
    let e2e = "E2E-APPROVAL-1";

    // 1) park
    let (status, _) = post_request(&client, port, e2e, "1500.00").await;
    assert_eq!(
        status, 200,
        "ApprovalRequest parks at the userTask (accept)"
    );

    // 2) duplicate while parked is rejected — the durable unique-alias correlate guard
    let (status, body) = post_request(&client, port, e2e, "1500.00").await;
    assert_eq!(status, 500, "duplicate while parked is rejected");
    assert!(
        body.contains("SUTRA.INBOUND.ALIAS_CONFLICT_REJECT"),
        "alias-conflict code: {body}"
    );

    // 3) channel relay resumes the parked instance to completion
    let (status, _) = post_decision(&client, port, e2e, "APPROVE").await;
    assert_eq!(status, 200, "ApprovalDecision relay resumes + completes");

    // 4) retire — same E2EId is accepted again (the alias was retired at completion)
    let (status, _) = post_request(&client, port, e2e, "1500.00").await;
    assert_eq!(
        status, 200,
        "post-completion re-request is accepted (alias retired)"
    );

    // 5) an uncorrelated relay is rejected and disturbs nothing (the wait is the safe state)
    let (status, body) = post_decision(&client, port, "E2E-UNKNOWN", "APPROVE").await;
    assert_eq!(status, 500);
    assert!(
        body.contains("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND"),
        "correlation-not-found code: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_template_showcase() {
    let client = reqwest::Client::new();
    let port = port();

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>SHOW-XSLT-1</E2EId><Amount>99.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "XSLT branch renders + replies");
    assert!(
        body.contains("<Xslt prepped=\"ok\" e2e=\"SHOW-XSLT-1\" amount=\"99.00\"/>"),
        "XSLT render: {body}"
    );

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>SHOW-HBS-1</E2EId><Amount>98.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "Handlebars (default) branch renders + replies");
    assert!(
        body.contains("<Hbs prepped=\"ok\" e2e=\"SHOW-HBS-1\" amount=\"98.00\"/>"),
        "Handlebars render: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_script_showcase() {
    let client = reqwest::Client::new();
    let port = port();

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/script-showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>SCRIPT-1</E2EId><Amount>42.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "script-showcase runs + replies");
    assert!(
        body.contains("<ScriptShowcase autoApprove=\"true\""),
        "boolean merged from the Handlebars script: {body}"
    );
    assert!(
        body.contains("engines=\"handlebars+handlebars\""),
        "both scriptTasks ran, in order: {body}"
    );
    // Script-on-script state: the second script re-read the first's {{uuid}}-derived
    // correlationId and echoed it — the two attribute values must match, and the id is a uuid.
    let correlation_id = extract_attr(&body, "correlationId")
        .unwrap_or_else(|| panic!("reply carries correlationId: {body}"));
    let correlation_echo = extract_attr(&body, "correlationEcho")
        .unwrap_or_else(|| panic!("reply carries correlationEcho: {body}"));
    assert!(
        is_uuid(&correlation_id),
        "correlationId is a uuid: {correlation_id}"
    );
    assert_eq!(
        correlation_id, correlation_echo,
        "the second script echoed the first script's merged state"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_throw_showcase() {
    let client = reqwest::Client::new();
    let port = port();

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/throw-showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>THROW-1</E2EId><Amount>42.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "throw-showcase runs + replies");
    for needle in ["linkJump=\"ok\"", "escalated=\"true\"", "stage=\"prepped\""] {
        assert!(
            body.contains(needle),
            "throw evidence missing {needle}: {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_subprocess_showcase() {
    let client = reqwest::Client::new();
    let port = port();

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/subprocess-showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>SUBPROC-1</E2EId><Amount>42.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "subprocess-showcase runs + replies");
    assert!(
        body.contains("<SubProcessShowcase riskScore=\"27\" decision=\"approve\"/>"),
        "inner sub-process variables echoed: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_approval_hold_datamapping_showcase() {
    let client = reqwest::Client::new();
    let port = port();

    let (status, body) = post_xml(
        &client,
        port,
        "/channels/datamapping-showcase-request",
        "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>DATAMAP-1</E2EId><Amount>42.00</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(status, 200, "datamapping-showcase runs + replies");
    assert!(
        body.contains("<DataMapShowcase amount=\"1500\" riskBand=\"high\" note=\"high-band\"/>"),
        "FEEL-assigned variables + scoped param: {body}"
    );
}

// ---- drive helpers ----------------------------------------------------------------------

async fn post_request(
    client: &reqwest::Client,
    port: u16,
    e2e: &str,
    amount: &str,
) -> (u16, String) {
    let body = format!("{{\"ApprovalRequest\":{{\"E2EId\":\"{e2e}\",\"Amount\":\"{amount}\"}}}}");
    post(
        client,
        port,
        "/channels/approval-request",
        "application/json",
        &body,
    )
    .await
}

async fn post_decision(
    client: &reqwest::Client,
    port: u16,
    e2e: &str,
    decision: &str,
) -> (u16, String) {
    let body =
        format!("{{\"ApprovalDecision\":{{\"E2EId\":\"{e2e}\",\"Decision\":\"{decision}\"}}}}");
    post(
        client,
        port,
        "/channels/approval-decision",
        "application/json",
        &body,
    )
    .await
}

async fn post_xml(client: &reqwest::Client, port: u16, path: &str, xml: &str) -> (u16, String) {
    post(client, port, path, "application/xml", xml).await
}

async fn post(
    client: &reqwest::Client,
    port: u16,
    path: &str,
    content_type: &str,
    body: &str,
) -> (u16, String) {
    let resp = client
        .post(format!("http://127.0.0.1:{port}{path}"))
        .header("Content-Type", content_type)
        .header("X-Api-Key", API_KEY)
        .body(body.to_string())
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    (status, text)
}

fn extract_attr(body: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// A canonical `8-4-4-4-12` lowercase-hex uuid.
fn is_uuid(s: &str) -> bool {
    let groups: Vec<&str> = s.split('-').collect();
    groups.len() == 5
        && [8usize, 4, 4, 4, 12]
            .iter()
            .zip(&groups)
            .all(|(len, g)| g.len() == *len && g.chars().all(|c| c.is_ascii_hexdigit()))
}
