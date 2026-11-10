//! A start event and a wait on one channel with overlapping message types warn: start events are
//! tried first, so a message meant for the parked instance starts a new one.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const START_AND_WAIT: &str = "SUTRA.CONFIG.CHANNEL.START_AND_WAIT";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A start on `c` typed `start_type`, and a receive task on `c` typed `wait_type`.
fn report(start_type: &str, wait_type: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: c\n    transport: local\n    codec: json\n    cloudevents-mode: none\n",
    );
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="flow" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="c" {start_type}/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="c" name="answer" {wait_type}/>
        <q:alias name="k" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Await" targetRef="E"/>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#
        ),
    );
    lint_dir(root)
}

fn warnings_of(report: &LintReport) -> Vec<String> {
    report
        .warnings()
        .filter(|d| d.code == START_AND_WAIT)
        .map(|d| d.message.clone())
        .collect()
}

#[test]
fn start_and_wait_lint_warns() {
    let found = warnings_of(&report("", ""));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].contains("'S'") && found[0].contains("flow/Await"),
        "{}",
        found[0]
    );
    assert!(
        !report("", "").errors().any(|d| d.code == START_AND_WAIT),
        "a warning, not a refusal"
    );
    // One typed, the other not: they still overlap.
    assert_eq!(
        warnings_of(&report(r#"messageTypeValue="request""#, "")).len(),
        1
    );
}

#[test]
fn start_and_wait_with_disjoint_types_is_clean() {
    assert!(warnings_of(&report(
        r#"messageTypeValue="request""#,
        r#"messageTypeValue="reply""#
    ))
    .is_empty());
}
