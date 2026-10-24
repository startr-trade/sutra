//! A channel-call's `<q:alias>` requirement depends on its channel: an ASYNC call's response
//! resumes the parked instance by correlation, so its park needs a declared alias; a
//! SYNCHRONOUS call (`ack-mode: on-complete`) is answered on its own connection and needs none.
//! The BPMN loader cannot see channels, so the deploy-time lint is where this is enforced.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const ALIAS_REQUIRED: &str = "SUTRA.DISPATCH.CHANNEL_CALL.ALIAS_REQUIRED";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// One process calling the outbound channel `svc` (declared with `ack_mode`, if any) from a
/// task carrying `alias`.
fn report(ack_mode: Option<&str>, alias: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    let ack = ack_mode
        .map(|m| format!("    ack-mode: {m}\n"))
        .unwrap_or_default();
    write(
        root,
        "channels.yaml",
        &format!(
            "channels:\n\
             \x20 - name: in-ch\n    transport: local\n\
             \x20 - name: svc\n    direction: outbound\n    transport: local\n    bind: local://in-ch\n{ack}"
        ),
    );
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in-ch"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Call" implementation="channel:svc">
      <bpmn:extensionElements><q:timeout duration="PT5S"/>{alias}</bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
    lint_dir(root)
}

fn has(report: &LintReport, code: &str) -> bool {
    report.diagnostics.iter().any(|d| d.code == code)
}

#[test]
fn an_async_call_without_an_alias_is_an_error() {
    for ack_mode in [None, Some("on-persist")] {
        let report = report(ack_mode, "");
        assert!(
            has(&report, ALIAS_REQUIRED),
            "{ack_mode:?}: {:#?}",
            report.diagnostics
        );
    }
}

#[test]
fn an_async_call_with_an_alias_passes() {
    let report = report(None, r#"<q:alias name="k" expression="'x'"/>"#);
    assert!(!has(&report, ALIAS_REQUIRED), "{:#?}", report.diagnostics);
}

#[test]
fn a_sync_call_needs_no_alias() {
    let report = report(Some("on-complete"), "");
    assert!(!has(&report, ALIAS_REQUIRED), "{:#?}", report.diagnostics);
}
