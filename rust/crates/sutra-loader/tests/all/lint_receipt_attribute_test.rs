//! `<q:receipt>` at deploy time: the expressions must parse, each name appears once, and an
//! attribute reading a sensitive variable is pointed out (a warning, because the engine masks it).

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const DUPLICATE: &str = "SUTRA.CONFIG.RECEIPT.ATTRIBUTE_DUPLICATE";
const SENSITIVE: &str = "SUTRA.CONFIG.RECEIPT.ATTRIBUTE_SENSITIVE";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn report(receipt: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: in-ch\n    transport: local\n",
    );
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements>
      <q:variables><q:variable name="secret" sensitive="true"/></q:variables>
      <q:receipt>{receipt}</q:receipt>
    </bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in-ch"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
        ),
    );
    lint_dir(root)
}

fn error_codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

fn warning_codes(report: &LintReport) -> Vec<String> {
    report.warnings().map(|d| d.code.clone()).collect()
}

#[test]
fn a_plain_receipt_is_clean() {
    let r = report(r#"<q:attribute name="channel" expression="event.channel"/>"#);
    assert!(
        !error_codes(&r).iter().any(|c| c.contains("RECEIPT")),
        "{:?}",
        error_codes(&r)
    );
    assert!(!warning_codes(&r).contains(&SENSITIVE.to_string()));
}

#[test]
fn a_repeated_attribute_name_is_refused() {
    let r = report(
        r#"<q:attribute name="a" expression="event.channel"/>
           <q:attribute name="a" expression="event.receivedAt"/>"#,
    );
    assert!(error_codes(&r).contains(&DUPLICATE.to_string()));
}

#[test]
fn an_expression_that_does_not_parse_is_refused() {
    let r = report(r#"<q:attribute name="a" expression="1 +"/>"#);
    assert!(
        r.errors().any(|d| d.message.contains("<q:receipt>")),
        "{:?}",
        error_codes(&r)
    );
}

#[test]
fn reading_a_sensitive_variable_is_a_warning_not_a_refusal() {
    let r = report(r#"<q:attribute name="a" expression="secret"/>"#);
    assert!(warning_codes(&r).contains(&SENSITIVE.to_string()));
    assert!(!error_codes(&r).contains(&SENSITIVE.to_string()));
}
