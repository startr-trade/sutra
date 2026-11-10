//! A literal `local://` send must stay inside its package, and a call must name a process the
//! package has. Both are refused at lint, so a package learns of them before it deploys.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const CROSSES: &str = "SUTRA.CONFIG.SEND.LOCAL_DESTINATION_CROSSES_PACKAGE";
const UNKNOWN_CALLED: &str = "SUTRA.CONFIG.CALL_ACTIVITY.UNKNOWN_CALLED_ELEMENT";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const CHANNELS: &str = "channels:\n  - name: go\n    transport: local\n    codec: json\n    cloudevents-mode: none\n  - name: in-local\n    transport: local\n    codec: json\n    cloudevents-mode: none\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// `sender` starts on `go`, runs `middle`, and ends. `target` starts on `in-local`.
fn report(middle: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "channels.yaml", CHANNELS);
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="sender" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="responseBody" type="object"/>
      <q:variable name="kind" type="string"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="go"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Middle"/>
{middle}
    <bpmn:sequenceFlow id="f2" sourceRef="Middle" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
  <bpmn:process id="target" isExecutable="true">
    <bpmn:startEvent id="TS">
      <bpmn:extensionElements><q:source channel="in-local"/></bpmn:extensionElements>
      <bpmn:outgoing>t1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="t1" sourceRef="TS" targetRef="TE"/>
    <bpmn:endEvent id="TE"><bpmn:incoming>t1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#
        ),
    );
    lint_dir(root)
}

fn send_to(destination: &str) -> LintReport {
    report(&format!(
        r#"    <bpmn:sendTask id="Middle" name="Send">
      <bpmn:extensionElements><q:send destination="{destination}" contentType="application/json"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:sendTask>"#
    ))
}

fn call(called: &str, extension: &str) -> LintReport {
    report(&format!(
        r#"    <bpmn:callActivity id="Middle" calledElement="{called}">
      <bpmn:extensionElements>{extension}</bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:callActivity>"#
    ))
}

fn errors_of(report: &LintReport, code: &str) -> Vec<String> {
    report
        .errors()
        .filter(|d| d.code == code)
        .map(|d| d.message.clone())
        .collect()
}

#[test]
fn a_literal_local_destination_to_another_package_is_refused() {
    let found = errors_of(&send_to("local://t1/other/1.0.0/in-local"), CROSSES);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].contains("local://t1/other/1.0.0/in-local"),
        "{}",
        found[0]
    );
    assert!(found[0].contains("'Middle'"), "{}", found[0]);
    // A bare address names no package, and the runtime cannot read it.
    assert_eq!(errors_of(&send_to("local://in-local"), CROSSES).len(), 1);
}

#[test]
fn one_to_its_own_channel_passes() {
    let report = send_to("local://t1/demo/1.0.0/in-local");
    assert!(errors_of(&report, CROSSES).is_empty());
    // Its own name, but not a local inbound channel it declares.
    assert_eq!(
        errors_of(&send_to("local://t1/demo/1.0.0/missing"), CROSSES).len(),
        1
    );
}

#[test]
fn another_version_is_refused() {
    let found = errors_of(&send_to("local://t1/demo/2.0.0/in-local"), CROSSES);
    assert_eq!(found.len(), 1, "{found:?}");
}

#[test]
fn a_dynamic_destination_is_not_judged() {
    let report = send_to("local://t1/${TARGET_MODULE}/1.0.0/in-local");
    assert!(errors_of(&report, CROSSES).is_empty());
    // Another scheme is not a local address.
    assert!(errors_of(&send_to("http://127.0.0.1:9/x"), CROSSES).is_empty());
}

#[test]
fn an_unknown_called_element_is_refused() {
    let found = errors_of(&call("no-such-process", ""), UNKNOWN_CALLED);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("no-such-process"), "{}", found[0]);
    assert!(errors_of(&call("target", ""), UNKNOWN_CALLED).is_empty());
}

#[test]
fn an_unknown_dispatch_case_is_refused() {
    let unknown_case = call(
        "ignored",
        r#"<q:dispatch default="target"><q:case when="kind = &quot;a&quot;" calledElement="nowhere"/></q:dispatch>"#,
    );
    let found = errors_of(&unknown_case, UNKNOWN_CALLED);
    assert_eq!(
        found.len(),
        1,
        "only the case, not the unused calledElement: {found:?}"
    );
    assert!(found[0].contains("nowhere"), "{}", found[0]);
    let unknown_default = call(
        "ignored",
        r#"<q:dispatch default="nowhere"><q:case when="kind = &quot;a&quot;" calledElement="target"/></q:dispatch>"#,
    );
    assert_eq!(errors_of(&unknown_default, UNKNOWN_CALLED).len(), 1);
}

#[test]
fn a_prefixed_called_element_resolves() {
    let report = call("urn:sutra:module:demo:1.0.0:target", "");
    assert!(
        errors_of(&report, UNKNOWN_CALLED).is_empty(),
        "{:?}",
        errors_of(&report, UNKNOWN_CALLED)
    );
}
