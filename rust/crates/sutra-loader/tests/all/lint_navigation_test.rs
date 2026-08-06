//! The navigation⇒schema suite. Each process's typed intake schema is resolved and every
//! FEEL data path it dereferences (`<q:alias>`, `<q:dispatch>` case `when`, `<q:source
//! dedupKey>`, `<q:simpleValidator path>`, sequence-flow conditions) is checked: a path
//! absent from a closed intake schema is a deploy-blocking ERROR (provable typo); a numeric
//! operator on a declared string is an ERROR; an opaque codec yields a WARN
//! (advise-don't-gatekeep). Fixtures are synthesised authoring trees run through `lint`.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

/// The `package.yaml` every synthetic package directory carries (labels are opaque).
const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

// ---- fixtures --------------------------------------------------------------------------

/// Closed XSD intake: `Payment { Id: string, Amount: decimal }`.
const PAYMENT_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

/// A two-root XSD (for the message-type-unpinned case): `Alpha` and `Beta`.
const MULTI_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Alpha"><xs:complexType><xs:sequence>
    <xs:element name="A" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
  <xs:element name="Beta"><xs:complexType><xs:sequence>
    <xs:element name="B" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

const CODEC_MANIFEST: &str = "schemaKind: xsd\nformats: [xml, json, yaml]\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build the standard authoring tree: library module `demo/1.0.0` with the given codec folder
/// (an XSD under `schemas/<codec_folder>/`, unless `schema` is empty for a builtin/opaque
/// codec), one BPMN, and a tenant binding whose `pay-in` channel binds `codec`.
fn build(root: &Path, bpmn: &str, channels: &str, schema: &[(&str, &str)]) {
    write(root, "package.yaml", PACKAGE_YAML);
    for (folder, xsd) in schema {
        write(root, &format!("schemas/{folder}/{folder}.xsd"), xsd);
        write(
            root,
            &format!("schemas/{folder}/codec-manifest.yaml"),
            CODEC_MANIFEST,
        );
    }
    write(root, "bpmn/flow.bpmn", bpmn);
    write(root, "channels.yaml", channels);
}

fn channels(codec: &str) -> String {
    format!(
        "channels:\n  - name: pay-in\n    transport: http\n    bind: \"POST /channels/pay-in\"\n    codec: \"{codec}\"\n"
    )
}

/// A linear `start → end` BPMN with a `<q:source>` (+ optional `<q:variables>`) and the given
/// start-event `<q:*>` bindings (aliases / dedupKey live inline on the source).
fn bpmn(vars: &str, source_attrs: &str, start_bindings: &str) -> String {
    let vars_block = if vars.is_empty() {
        String::new()
    } else {
        format!(
            "<bpmn:extensionElements><q:variables>{vars}</q:variables></bpmn:extensionElements>"
        )
    };
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="payments">
    {vars_block}
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="pay-in"{source_attrs}/>
        {start_bindings}
      </bpmn:extensionElements>
      <bpmn:outgoing>f</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"><bpmn:incoming>f</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

/// A `start → exclusiveGateway → end` BPMN whose gateway-branch flow carries `condition`.
fn bpmn_gateway(condition: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="payments">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="pay-in" messageTypeValue="Payment"/></bpmn:extensionElements>
      <bpmn:outgoing>f</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:exclusiveGateway id="GW"><bpmn:incoming>f</bpmn:incoming><bpmn:outgoing>g</bpmn:outgoing></bpmn:exclusiveGateway>
    <bpmn:endEvent id="E"><bpmn:incoming>g</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="GW"/>
    <bpmn:sequenceFlow id="g" sourceRef="GW" targetRef="E"><bpmn:conditionExpression>{condition}</bpmn:conditionExpression></bpmn:sequenceFlow>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

const PAY_CODEC: &str = "urn:pay";

fn error_codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

fn path_family_codes(report: &LintReport) -> Vec<String> {
    report
        .diagnostics
        .iter()
        .filter(|d| {
            d.code.starts_with("SUTRA.CONFIG.BPMN.PATH_")
                || d.code.contains("MESSAGE_TYPE_UNPINNED")
        })
        .map(|d| d.code.clone())
        .collect()
}

fn lint_module(bpmn: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        bpmn,
        &channels(PAY_CODEC),
        &[("pay", PAYMENT_XSD)],
    );
    lint_dir(dir.path())
}

fn lint_opaque(bpmn: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), bpmn, &channels("raw-text"), &[]);
    lint_dir(dir.path())
}

// ---- payload-path cases (closed module XSD intake) -------------------------------------

#[test]
fn a_valid_path_against_the_xsd_intake_is_clean() {
    let report = lint_module(&bpmn(
        "",
        r#" messageTypeValue="Payment""#,
        r#"<q:alias name="k" expression="payload.Amount"/>"#,
    ));
    assert!(
        path_family_codes(&report).is_empty(),
        "got: {:?}",
        report.diagnostics
    );
    assert!(!report.has_errors(), "got: {:?}", error_codes(&report));
}

#[test]
fn a_typo_against_the_closed_xsd_is_a_deploy_blocking_error() {
    let report = lint_module(&bpmn(
        "",
        r#" messageTypeValue="Payment""#,
        r#"<q:alias name="k" expression="payload.Typo"/>"#,
    ));
    assert_eq!(
        error_codes(&report),
        vec!["SUTRA.CONFIG.BPMN.PATH_UNKNOWN_FIELD".to_string()],
        "exactly one ERROR — the typo; got: {:?}",
        report.diagnostics
    );
    assert!(
        report.errors().any(|d| d.message.contains("payload.Typo")),
        "message names the offending path"
    );
}

#[test]
fn a_numeric_operator_on_a_declared_string_is_an_error() {
    // payload.Id is xs:string; using it under '>' forces a numeric reading → PATH_TYPE_MISMATCH.
    let report = lint_module(&bpmn(
        "",
        r#" messageTypeValue="Payment""#,
        r#"<q:alias name="k" expression="payload.Id &gt; 5"/>"#,
    ));
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_TYPE_MISMATCH".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn an_opaque_codec_yields_a_warning_not_an_error() {
    let report = lint_opaque(&bpmn(
        "",
        "",
        r#"<q:alias name="k" expression="payload.anything"/>"#,
    ));
    assert!(!report.has_errors(), "got: {:?}", error_codes(&report));
    assert!(
        report
            .warnings()
            .any(|d| d.code == "SUTRA.CONFIG.BPMN.PATH_UNVERIFIABLE"),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn non_payload_roots_are_skipped() {
    // header.MessageId is not the typed payload root — not checked, no navigation diagnostic.
    let report = lint_module(&bpmn(
        "",
        r#" messageTypeValue="Payment""#,
        r#"<q:alias name="k" expression="header.MessageId"/>"#,
    ));
    assert!(
        path_family_codes(&report).is_empty(),
        "no navigation diagnostic for a non-payload root; got: {:?}",
        report.diagnostics
    );
}

#[test]
fn idempotency_key_path_is_navigation_checked() {
    // A payload-rooted dedupKey typo is caught at the source-binding navigation site.
    let report = lint_module(&bpmn(
        "",
        r#" messageTypeValue="Payment" dedupKey="payload.Nope""#,
        "",
    ));
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_UNKNOWN_FIELD".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

// ---- variable-rooted cases -------------------------------------------------------------

#[test]
fn a_numeric_operator_on_a_declared_string_variable_is_an_error() {
    let report = lint_opaque(&bpmn(
        r#"<q:variable name="status" type="string"/>"#,
        "",
        r#"<q:alias name="k" expression="status &gt; 5"/>"#,
    ));
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_TYPE_MISMATCH".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_numeric_operator_on_a_declared_number_variable_is_clean() {
    let report = lint_opaque(&bpmn(
        r#"<q:variable name="riskScore" type="number"/>"#,
        "",
        r#"<q:alias name="k" expression="riskScore &gt; 5"/>"#,
    ));
    assert!(
        !error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_TYPE_MISMATCH".to_string()),
        "a number variable in a numeric operator is clean; got: {:?}",
        report.diagnostics
    );
}

#[test]
fn an_undeclared_variable_root_is_skipped() {
    // 'ratio' has no <q:variable> declaration → not checkable, no diagnostic.
    let report = lint_opaque(&bpmn(
        r#"<q:variable name="other" type="string"/>"#,
        "",
        r#"<q:alias name="k" expression="ratio &gt; 5"/>"#,
    ));
    assert!(
        path_family_codes(&report).is_empty(),
        "got: {:?}",
        report.diagnostics
    );
}

// ---- sequence-flow condition site ------------------------------------------------------

#[test]
fn a_valid_flow_condition_against_the_xsd_is_clean() {
    // payload.Amount is xs:decimal → numeric '>' is fine.
    let report = lint_module(&bpmn_gateway("payload.Amount &gt; 1000"));
    assert!(
        path_family_codes(&report).is_empty(),
        "got: {:?}",
        report.diagnostics
    );
    assert!(!report.has_errors(), "got: {:?}", error_codes(&report));
}

#[test]
fn a_numeric_flow_condition_on_a_string_field_is_an_error() {
    // payload.Id is xs:string → used under '>' on a gateway branch → PATH_TYPE_MISMATCH.
    let report = lint_module(&bpmn_gateway("payload.Id &gt; 1000"));
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_TYPE_MISMATCH".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

// ---- message-type unpinned over a multi-type codec -------------------------------------

#[test]
fn a_multi_type_codec_with_no_pin_is_message_type_unpinned() {
    let dir = tempfile::tempdir().unwrap();
    // 'multi' declares Alpha + Beta; the <q:source> pins neither → no concrete schema.
    build(
        dir.path(),
        &bpmn("", "", r#"<q:alias name="k" expression="payload.A"/>"#),
        &channels("urn:multi"),
        &[("multi", MULTI_XSD)],
    );
    let report = lint_dir(dir.path());
    assert!(
        report
            .warnings()
            .any(|d| d.code == "SUTRA.CONFIG.BPMN.MESSAGE_TYPE_UNPINNED"),
        "got: {:?}",
        report.diagnostics
    );
    // Advisory only — the unpinned intake never hard-errors on its paths.
    assert!(
        !error_codes(&report).contains(&"SUTRA.CONFIG.BPMN.PATH_UNKNOWN_FIELD".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}
