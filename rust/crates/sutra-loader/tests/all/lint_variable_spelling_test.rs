//! The two-spellings advisory (`check_variable_spelling_conflicts`). BPMN's `<property>` and
//! `<q:variable>` are both accepted so a model can round-trip through a modelling tool; when both
//! declare one name the `<q:variable>` wins, and the author — the only one who knows which they
//! meant — is told.

use std::path::Path;

use sutra_loader::{lint_dir, LintDiagnostic, LintReport, LintSeverity};

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const CONFLICT_CODE: &str = "SUTRA.CONFIG.BPMN.VARIABLE_SPELLING_CONFLICT";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A one-task process declaring `amount` as a `<bpmn:property>`, plus — when `also_q_variable` —
/// the same name as a `<q:variable>`.
fn build(root: &Path, also_q_variable: bool) {
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "templates/x.hbs", "<v>1</v>");
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"urn:sutra:codec:json\"\n",
    );
    let q_variables = if also_q_variable {
        r#"<q:variables><q:variable name="amount" sensitive="true"/></q:variables>"#
    } else {
        ""
    };
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                  targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:itemDefinition id="amountItem" structureRef="xsd:decimal"/>
  <bpmn:process id="p1">
    <bpmn:extensionElements>{q_variables}</bpmn:extensionElements>
    <bpmn:property id="prop1" name="amount" itemSubjectRef="amountItem"/>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="x.hbs">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
}

fn conflicts(report: &LintReport) -> Vec<&LintDiagnostic> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.code == CONFLICT_CODE)
        .collect()
}

#[test]
fn two_spellings_of_one_name_warn_and_say_which_won() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), true);
    let report = lint_dir(dir.path());
    let found = conflicts(&report);
    assert_eq!(found.len(), 1, "{:?}", report.diagnostics);
    let d = found[0];
    assert_eq!(
        d.severity,
        LintSeverity::Warning,
        "both spellings are accepted on purpose; the ambiguity is advice, not a deploy block"
    );
    assert!(
        d.message.contains("amount") && d.message.contains("<q:variable> wins"),
        "the advisory has to name the variable and the winner to be actionable: {}",
        d.message
    );
}

#[test]
fn a_bpmn_property_alone_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), false);
    assert!(
        conflicts(&lint_dir(dir.path())).is_empty(),
        "one spelling is unambiguous — the front door is meant to be used"
    );
}
