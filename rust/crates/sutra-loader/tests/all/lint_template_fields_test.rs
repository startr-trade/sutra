//! Template field-path validation: the per-type intersection over a `messageTypePattern`
//! plus the single-pinned module-codec field check now that `sutra-xsd` emits a navigation
//! shape for module structural codecs. A payload path invalid in EVERY pattern-matching type is a
//! deploy-blocking ERROR (`FIELD_UNKNOWN`); valid in only some is a WARN (`FIELD_PARTIAL`);
//! valid in all is clean. Fixtures are synthesised authoring trees run through `lint`.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

/// The `package.yaml` every synthetic package directory carries (labels are opaque).
const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

/// A two-root XSD: `Alpha`/`Beta` share `Common`; `OnlyAlpha`/`OnlyBeta` are type-specific.
const MULTI_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Alpha"><xs:complexType><xs:sequence>
    <xs:element name="Common" type="xs:string"/>
    <xs:element name="OnlyAlpha" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
  <xs:element name="Beta"><xs:complexType><xs:sequence>
    <xs:element name="Common" type="xs:string"/>
    <xs:element name="OnlyBeta" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

/// A single-root closed XSD: `Payment { Id: string, Amount: decimal }`.
const PAYMENT_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

const CODEC_MANIFEST: &str = "schemaKind: xsd\nformats: [xml, json, yaml]\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build a library module (`demo/1.0.0`) with one codec folder, one BPMN, one template, and a
/// tenant binding whose `ch` channel binds `codec`. `source_attrs` is spliced onto the
/// `<q:source>` (the messageTypePattern / messageTypeValue under test).
fn build(
    root: &Path,
    codec_folder: &str,
    xsd: &str,
    source_attrs: &str,
    template: &str,
    codec_urn: &str,
) {
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        &format!("schemas/{codec_folder}/{codec_folder}.xsd"),
        xsd,
    );
    write(
        root,
        &format!("schemas/{codec_folder}/codec-manifest.yaml"),
        CODEC_MANIFEST,
    );
    write(root, "templates/x.hbs", template);
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"{source_attrs}/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="x.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
    write(
        root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"{codec_urn}\"\n"
        ),
    );
}

fn error_codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

fn field_codes(report: &LintReport) -> Vec<String> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.code.starts_with("SUTRA.CONFIG.TEMPLATE.FIELD_"))
        .map(|d| d.code.clone())
        .collect()
}

/// Run the C5 fixture: a messageTypePattern ".*" over the 2-type `multi` codec, with a
/// template reading `payload.<field>`.
fn lint_c5(field: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        "multi",
        MULTI_XSD,
        r#" messageTypePattern=".*""#,
        &format!("<x>{{{{payload.{field}}}}}</x>"),
        "urn:multi",
    );
    lint_dir(dir.path())
}

/// Run the single-pinned fixture: messageTypeValue "Payment" over the closed `pay` codec.
fn lint_single(field: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        "pay",
        PAYMENT_XSD,
        r#" messageTypeValue="Payment""#,
        &format!("<x>{{{{payload.{field}}}}}</x>"),
        "urn:pay",
    );
    lint_dir(dir.path())
}

// ---- C5 — messageTypePattern intersection ----------------------------------------------

#[test]
fn c5_path_declared_in_every_matching_type_is_silent() {
    // Common is in both Alpha and Beta → declared everywhere → no field diagnostic.
    let report = lint_c5("Common");
    assert!(
        field_codes(&report).is_empty(),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn c5_path_declared_in_only_some_matching_types_is_partial_warning() {
    // OnlyAlpha is declared in Alpha but not Beta → partial (ambiguous) → WARN, not ERROR.
    let report = lint_c5("OnlyAlpha");
    assert!(
        !report.has_errors(),
        "must not error; got: {:?}",
        error_codes(&report)
    );
    assert!(
        report
            .warnings()
            .any(|d| d.code == "SUTRA.CONFIG.TEMPLATE.FIELD_PARTIAL"),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn c5_path_declared_in_no_matching_type_is_error() {
    // Nope is in neither Alpha nor Beta → dead in every matching type → ERROR (module-evicting).
    let report = lint_c5("Nope");
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.TEMPLATE.FIELD_UNKNOWN".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

/// Run a single-pinned (`messageTypeValue="Payment"`) fixture with a caller-supplied template.
fn lint_single_tpl(template: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        "pay",
        PAYMENT_XSD,
        r#" messageTypeValue="Payment""#,
        template,
        "urn:pay",
    );
    lint_dir(dir.path())
}

// ---- NOT_VALIDATABLE — unresolvable (dynamic-lookup) construct -------------------------

#[test]
fn a_dynamic_lookup_key_is_not_statically_validatable() {
    // {{lookup item @index}} — the key is a runtime data var, so the field cannot be tied to a
    // concrete schema path → NOT_VALIDATABLE (deploy-blocking).
    let report =
        lint_single_tpl("<x>{{#each payload.Id as |item|}}{{lookup item @index}}{{/each}}</x>");
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.TEMPLATE.NOT_VALIDATABLE".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_literal_lookup_key_is_statically_validatable() {
    // {{lookup payload.Id "Fixed"}} — a literal key resolves statically → no NOT_VALIDATABLE, and
    // the template lints clean (payload.Id is a declared field).
    let report = lint_single_tpl(r#"<x>{{lookup payload.Id "Fixed"}}</x>"#);
    assert!(
        !error_codes(&report).contains(&"SUTRA.CONFIG.TEMPLATE.NOT_VALIDATABLE".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

// ---- single-pinned module-codec field check (sutra-xsd shape) --------------------------

#[test]
fn a_declared_field_on_a_pinned_module_codec_is_clean() {
    // payload.Id is declared in Payment → resolves against the sutra-xsd shape → clean.
    let report = lint_single("Id");
    assert!(
        field_codes(&report).is_empty(),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn an_unknown_field_on_a_pinned_module_codec_is_deploy_blocking() {
    // payload.Ghost is absent from the closed Payment schema → FIELD_UNKNOWN (was a mere
    // FIELD_UNVERIFIABLE warning before the sutra-xsd shape increment).
    let report = lint_single("Ghost");
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.TEMPLATE.FIELD_UNKNOWN".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}
