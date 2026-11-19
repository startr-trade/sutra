//! `<q:reply status>` and the response headers of a synchronous reply, at deploy time: a literal
//! status must be 200–599 and a literal redirect needs a `Location`; a status expression must
//! parse; a header name must be an HTTP token the flow may set; and a status on a process with
//! no synchronous HTTP intake is pointed out, because nothing would send it.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const STATUS_INVALID: &str = "SUTRA.OUTBOUND.REPLY_STATUS_INVALID";
const HEADER_INVALID: &str = "SUTRA.OUTBOUND.REPLY_HEADER_INVALID";
const STATUS_IGNORED: &str = "SUTRA.CHANNEL.REPLY_STATUS_IGNORED";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const HTTP_SYNC: &str = "channels:\n  - name: in-ch\n    transport: http\n    bind: \"POST /channels/in\"\n    codec: json\n    auth:\n      scheme: apikey\n      apikey:\n        value: env:IN_KEY\n";
const HTTP_ASYNC: &str = "channels:\n  - name: in-ch\n    transport: http\n    bind: \"POST /channels/in\"\n    codec: json\n    ack-mode: on-persist\n    auth:\n      scheme: apikey\n      apikey:\n        value: env:IN_KEY\n";
const LOCAL: &str = "channels:\n  - name: in-ch\n    transport: local\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn report_on(channels: &str, reply: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "channels.yaml", channels);
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in-ch" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E">
      <bpmn:extensionElements>{reply}</bpmn:extensionElements>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
        ),
    );
    lint_dir(root)
}

fn report(reply: &str) -> LintReport {
    report_on(HTTP_SYNC, reply)
}

fn error_codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

fn warning_codes(report: &LintReport) -> Vec<String> {
    report.warnings().map(|d| d.code.clone()).collect()
}

fn reply_codes(report: &LintReport) -> Vec<String> {
    error_codes(report)
        .into_iter()
        .chain(warning_codes(report))
        .filter(|c| c.contains("REPLY_"))
        .collect()
}

#[test]
fn a_status_and_headers_on_a_synchronous_http_intake_are_clean() {
    let r = report(
        r#"<q:reply status="201">
             <q:header name="Location" value="'/items/' + payload.id"/>
             <q:header name="WWW-Authenticate" value="'Bearer'"/>
             <q:header name="X-Request-Id" value="payload.id"/>
           </q:reply>"#,
    );
    assert!(reply_codes(&r).is_empty(), "{:?}", reply_codes(&r));
    let r = report(r#"<q:reply status="if payload.found then 200 else 404"/>"#);
    assert!(reply_codes(&r).is_empty(), "{:?}", reply_codes(&r));
}

#[test]
fn a_literal_status_outside_200_to_599_is_refused() {
    for status in ["101", "199", "600", "99999999999"] {
        let r = report(&format!(r#"<q:reply status="{status}"/>"#));
        assert!(
            error_codes(&r).contains(&STATUS_INVALID.to_string()),
            "{status}: {:?}",
            error_codes(&r)
        );
    }
}

#[test]
fn a_literal_redirect_needs_a_location_header() {
    let r = report(r#"<q:reply status="302"/>"#);
    assert!(error_codes(&r).contains(&STATUS_INVALID.to_string()));
    let r =
        report(r#"<q:reply status="303"><q:header name="location" value="'/next'"/></q:reply>"#);
    assert!(reply_codes(&r).is_empty(), "{:?}", reply_codes(&r));
}

#[test]
fn a_status_expression_that_does_not_parse_is_refused() {
    let r = report(r#"<q:reply status="if payload.found then"/>"#);
    assert!(
        r.errors().any(|d| d.message.contains("status=")),
        "{:?}",
        error_codes(&r)
    );
}

#[test]
fn a_header_the_flow_may_not_set_is_refused() {
    for name in [
        "Content-Length",
        "Transfer-Encoding",
        "Connection",
        "Content-Type",
        "Sutra-Receipt",
        "Bad Name",
    ] {
        let r = report(&format!(
            r#"<q:reply><q:header name="{name}" value="'x'"/></q:reply>"#
        ));
        assert!(
            error_codes(&r).contains(&HEADER_INVALID.to_string()),
            "{name}: {:?}",
            error_codes(&r)
        );
    }
}

#[test]
fn a_reply_with_a_destination_keeps_its_headers_unchecked() {
    // Out-of-band replies carry headers to another endpoint or broker, as before.
    let r = report(
        r#"<q:reply destination="http://example.invalid/cb">
             <q:header name="Content-Type" value="'x'"/>
           </q:reply>"#,
    );
    assert!(
        !error_codes(&r).contains(&HEADER_INVALID.to_string()),
        "{:?}",
        error_codes(&r)
    );
}

#[test]
fn a_status_nothing_would_send_is_a_warning() {
    for channels in [HTTP_ASYNC, LOCAL] {
        let r = report_on(channels, r#"<q:reply status="201"/>"#);
        assert!(
            warning_codes(&r).contains(&STATUS_IGNORED.to_string()),
            "{:?}",
            warning_codes(&r)
        );
        assert!(!error_codes(&r).contains(&STATUS_IGNORED.to_string()));
    }
    let r = report(r#"<q:reply status="201"/>"#);
    assert!(!warning_codes(&r).contains(&STATUS_IGNORED.to_string()));
}
