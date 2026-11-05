//! The raw-body masking warnings (`check_raw_body_masking`). In a process that declares masking, a
//! plain variable holding raw `event.body` text, or a store write of it, is a WARNING: the engine
//! masks `event.body` itself but cannot see through a copy. Neither is an error yet.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport, LintSeverity};

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const UNMASKED: &str = "SUTRA.CONFIG.BPMN.RAW_BODY_UNMASKED";
const STORED: &str = "SUTRA.CONFIG.BPMN.RAW_BODY_STORED";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A process with `variables`, whose task assigns `expression` to `copy`, then `copy` to `second`.
fn lint(variables: &str, expression: &str) -> LintReport {
    lint_with(variables, expression, "", "")
}

/// [`lint`], with `reference` beside the task and `output` inside it.
fn lint_with(variables: &str, expression: &str, reference: &str, output: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: json\n",
    );
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables>{variables}</q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
    </bpmn:startEvent>
    {reference}
    <bpmn:serviceTask id="T">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>{expression}</bpmn:from><bpmn:to>copy</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>copy</bpmn:from><bpmn:to>second</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
      {output}
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
    lint_dir(root)
}

const MASKING: &str = r#"<q:variable name="payload" type="object" sensitive="true"/>"#;

fn warnings_with(report: &LintReport, code: &str) -> Vec<String> {
    let found: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|d| d.code == code)
        .collect();
    for d in &found {
        assert_eq!(d.severity, LintSeverity::Warning, "{}", d.message);
    }
    found.iter().map(|d| d.message.clone()).collect()
}

#[test]
fn a_plain_copy_of_the_raw_body_in_a_masking_flow_warns_through_every_assignment() {
    let found = warnings_with(&lint(MASKING, "event.body"), UNMASKED);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found.iter().any(|m| m.contains("'copy'")), "{found:?}");
    assert!(found.iter().any(|m| m.contains("'second'")), "{found:?}");
}

#[test]
fn reading_event_whole_counts_as_the_raw_body() {
    let found = warnings_with(&lint(MASKING, "string(event)"), UNMASKED);
    assert_eq!(found.len(), 2, "{found:?}");
}

#[test]
fn a_sensitive_copy_does_not_warn() {
    let variables = format!(
        r#"{MASKING}<q:variable name="copy" sensitive="true"/><q:variable name="second" sensitive="true"/>"#
    );
    assert!(warnings_with(&lint(&variables, "event.body"), UNMASKED).is_empty());
}

#[test]
fn a_flow_that_declares_no_masking_does_not_warn() {
    assert!(warnings_with(&lint("", "event.body"), UNMASKED).is_empty());
}

#[test]
fn another_part_of_the_event_does_not_warn() {
    assert!(warnings_with(&lint(MASKING, "event.channel"), UNMASKED).is_empty());
}

const STORE_REFERENCE: &str = r#"<bpmn:dataStoreReference id="rows" dataStoreRef="messages">
      <bpmn:extensionElements><q:store key="'k'"/></bpmn:extensionElements>
    </bpmn:dataStoreReference>"#;

#[test]
fn writing_the_raw_body_to_a_store_warns() {
    let output = r#"<bpmn:dataOutputAssociation>
        <bpmn:sourceRef>second</bpmn:sourceRef><bpmn:targetRef>rows</bpmn:targetRef>
      </bpmn:dataOutputAssociation>"#;
    let report = lint_with(MASKING, "event.body", STORE_REFERENCE, output);
    let found = warnings_with(&report, STORED);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("'messages'"), "{found:?}");
}

#[test]
fn writing_something_else_to_a_store_does_not_warn() {
    let output = r#"<bpmn:dataOutputAssociation>
        <bpmn:sourceRef>second</bpmn:sourceRef><bpmn:targetRef>rows</bpmn:targetRef>
      </bpmn:dataOutputAssociation>"#;
    let report = lint_with(MASKING, "event.channel", STORE_REFERENCE, output);
    assert!(warnings_with(&report, STORED).is_empty());
}

// ---- <q:decode> and stored raw text ------------------------------------------------------------

const REDACTORS_MISSING: &str = "SUTRA.CONFIG.DECODE.REDACTORS_MISSING";
const CODEC_UNKNOWN: &str = "SUTRA.CONFIG.DECODE.CODEC_UNKNOWN";
const REDACTOR_UNKNOWN: &str = "SUTRA.CONFIG.DECODE.REDACTOR_UNKNOWN";

/// A package with a capture flow writing `event.body` to store `notes`, and a retrieval flow reading
/// it into `row` and decoding `row.body` with `decode`.
fn lint_store_package(
    capture_variables: &str,
    retrieve_variables: &str,
    decode: &str,
) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: json\n  - name: rd\n    transport: http\n    bind: \"POST /channels/rd\"\n    codec: json\n",
    );
    write(
        root,
        "datastores.yaml",
        "datastores:\n  - name: notes\n    type: sql\n    sql:\n      url-ref: env:DB_URL\n      username-ref: env:DB_USER\n      password-ref: env:DB_PASSWORD\n",
    );
    write(
        root,
        "bpmn/flows.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:dataStore id="notes" name="notes"/>
  <bpmn:process id="capture">
    <bpmn:extensionElements><q:variables>{capture_variables}</q:variables></bpmn:extensionElements>
    <bpmn:dataStoreReference id="w" dataStoreRef="notes">
      <bpmn:extensionElements><q:store key="payload.id"/></bpmn:extensionElements>
    </bpmn:dataStoreReference>
    <bpmn:startEvent id="S1"><bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:serviceTask id="Keep">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>event.body</bpmn:from><bpmn:to>raw</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
      <bpmn:dataOutputAssociation><bpmn:sourceRef>raw</bpmn:sourceRef><bpmn:targetRef>w</bpmn:targetRef></bpmn:dataOutputAssociation>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E1"/>
    <bpmn:sequenceFlow id="c1" sourceRef="S1" targetRef="Keep"/>
    <bpmn:sequenceFlow id="c2" sourceRef="Keep" targetRef="E1"/>
  </bpmn:process>
  <bpmn:process id="retrieve">
    <bpmn:extensionElements><q:variables>{retrieve_variables}</q:variables></bpmn:extensionElements>
    <bpmn:dataStoreReference id="r" dataStoreRef="notes">
      <bpmn:extensionElements><q:store key="req.id"/></bpmn:extensionElements>
    </bpmn:dataStoreReference>
    <bpmn:startEvent id="S2"><bpmn:extensionElements><q:source channel="rd" name="req"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:serviceTask id="Load">
      <bpmn:extensionElements>{decode}</bpmn:extensionElements>
      <bpmn:dataInputAssociation><bpmn:sourceRef>r</bpmn:sourceRef><bpmn:targetRef>row</bpmn:targetRef></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="r1" sourceRef="S2" targetRef="Load"/>
    <bpmn:sequenceFlow id="r2" sourceRef="Load" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
    lint_dir(root)
}

fn codes_of(report: &LintReport, code: &str) -> Vec<(LintSeverity, String)> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.code == code)
        .map(|d| (d.severity, d.message.clone()))
        .collect()
}

const PLAIN_DECODE: &str = r#"<q:decode name="message" expression="row" codec="json"/>"#;

#[test]
fn lint_raw_body_taint_through_assignment_and_store() {
    let report = lint_store_package(MASKING, "", PLAIN_DECODE);
    // The capture variable and its store write, in the masking flow.
    assert!(warnings_with(&report, UNMASKED)
        .iter()
        .any(|m| m.contains("'raw'")));
    assert_eq!(warnings_with(&report, STORED).len(), 1);
    // The store carries it to the retrieval flow, which has no masking of its own.
    assert!(
        warnings_with(&report, UNMASKED)
            .iter()
            .any(|m| m.contains("'row'") && m.contains("reads text a masking process stored")),
        "{:?}",
        report.diagnostics
    );
    // An unmasked decode of it is an error: the element is new, so no package relies on it.
    let missing = codes_of(&report, REDACTORS_MISSING);
    assert_eq!(missing.len(), 1, "{missing:?}");
    assert_eq!(missing[0].0, LintSeverity::Error);
}

#[test]
fn a_masked_decode_target_satisfies_the_rule() {
    let sensitive = lint_store_package(
        MASKING,
        r#"<q:variable name="message" type="object" sensitive="true"/>"#,
        PLAIN_DECODE,
    );
    assert!(codes_of(&sensitive, REDACTORS_MISSING).is_empty());

    let redacted = lint_store_package(
        MASKING,
        "",
        r#"<q:decode name="message" expression="row" codec="json">
             <q:redactors><q:redactor ref="urn:sutra:redactor:nothing-here"/></q:redactors>
           </q:decode>"#,
    );
    assert!(codes_of(&redacted, REDACTORS_MISSING).is_empty());
}

#[test]
fn lint_raw_body_ignored_without_redaction() {
    let report = lint_store_package("", "", PLAIN_DECODE);
    for code in [UNMASKED, STORED, REDACTORS_MISSING] {
        assert!(codes_of(&report, code).is_empty(), "{code}");
    }
}

#[test]
fn lint_decode_codec_and_redactor_unknown() {
    let unknown = lint_store_package(
        "",
        "",
        r#"<q:decode name="message" expression="row" codec="urn:nothing-here">
             <q:redactors><q:redactor ref="nothing-here"/></q:redactors>
           </q:decode>"#,
    );
    let codec = codes_of(&unknown, CODEC_UNKNOWN);
    assert_eq!(codec.len(), 1, "{:?}", unknown.diagnostics);
    assert_eq!(codec[0].0, LintSeverity::Error);
    let redactor = codes_of(&unknown, REDACTOR_UNKNOWN);
    assert_eq!(redactor.len(), 1, "{:?}", unknown.diagnostics);
    assert_eq!(redactor[0].0, LintSeverity::Error);

    let builtin = lint_store_package("", "", PLAIN_DECODE);
    assert!(codes_of(&builtin, CODEC_UNKNOWN).is_empty());
    assert!(codes_of(&builtin, REDACTOR_UNKNOWN).is_empty());
}
