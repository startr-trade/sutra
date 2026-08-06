//! The fail-closed `transient`-read-after-wait gate (`check_transient_reads`). A
//! `<q:variable transient="true">` is never persisted, so reading it in a node reachable *after* a
//! wait state would silently yield null — a package-time ERROR. Reading it before the wait, or a
//! non-transient variable after the wait, is clean.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const TRANSIENT_CODE: &str = "SUTRA.CONFIG.TRANSIENT.READ_AFTER_WAIT";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build a package whose process declares one variable `temp` (optionally `@transient`) and reads
/// it in a `serviceTask` template. `read_after_wait` places that task after a `userTask` wait
/// (`S → W → T → E`) or before it (`S → T → W → E`).
fn build(root: &Path, transient: bool, read_after_wait: bool) {
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "templates/x.hbs", "<v>{{temp}}</v>");
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"urn:sutra:codec:json\"\n",
    );
    let transient_attr = if transient { " transient=\"true\"" } else { "" };
    let (nodes, flows) = if read_after_wait {
        (
            r#"<bpmn:userTask id="W"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:userTask>
    <bpmn:serviceTask id="T" implementation="x.hbs"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing></bpmn:serviceTask>"#,
            r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="W"/>
    <bpmn:sequenceFlow id="f2" sourceRef="W" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>"#,
        )
    } else {
        (
            r#"<bpmn:serviceTask id="T" implementation="x.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:userTask id="W"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing></bpmn:userTask>"#,
            r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="E"/>"#,
        )
    };
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables><q:variable name="temp"{transient_attr}/></q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    {nodes}
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
    {flows}
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
}

fn has_transient_error(report: &LintReport) -> bool {
    report.diagnostics.iter().any(|d| d.code == TRANSIENT_CODE)
}

#[test]
fn transient_read_after_wait_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), true, true);
    assert!(
        has_transient_error(&lint_dir(dir.path())),
        "reading a @transient variable after a wait state must be a deploy ERROR"
    );
}

#[test]
fn transient_read_before_wait_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), true, false);
    assert!(
        !has_transient_error(&lint_dir(dir.path())),
        "reading a @transient variable BEFORE the wait is fine — it survives in the segment"
    );
}

#[test]
fn non_transient_read_after_wait_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), false, true);
    assert!(
        !has_transient_error(&lint_dir(dir.path())),
        "a non-transient variable read after a wait is fine — it is persisted across the park"
    );
}
