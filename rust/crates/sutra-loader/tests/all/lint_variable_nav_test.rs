//! B2 (T4-2) — the navigation⇒schema field-check extended from the `payload` root to
//! `<q:variable>`-rooted `var.field` reads at two FEEL navigation sites the earlier increments
//! left unchecked: sequence-flow conditions and `<q:param expression>` on a serviceTask. A
//! `var.field` absent from the closed container of the variable's bound shape (its `@schema`
//! codec, or the `@source` channel's codec) is a deploy-blocking `PATH_UNKNOWN_FIELD` ERROR; an
//! open / opaque shape yields a `PATH_UNVERIFIABLE` WARN; a declared field is clean. Fixtures are
//! synthesised authoring trees run through `lint`, mirroring `lint_navigation_test` /
//! `lint_variable_schema_test`.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

/// A single-root closed XSD: `Order { Id: string, Amount: decimal }`.
const ORDER_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Order"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

const CODEC_MANIFEST: &str = "schemaKind: xsd\nformats: [xml, json, yaml]\n";

const PATH_UNKNOWN: &str = "SUTRA.CONFIG.BPMN.PATH_UNKNOWN_FIELD";
const PATH_UNVERIFIABLE: &str = "SUTRA.CONFIG.BPMN.PATH_UNVERIFIABLE";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Assemble a package: `package.yaml`, a trivial serviceTask template, one channel `ch` bound to
/// `codec`, the given BPMN, and (when `with_order_xsd`) the closed `Order` codec under
/// `schemas/orders/`. `codec` is the URN the intake channel — and any `@schema`/`@source`
/// variable — resolves against.
fn build(root: &Path, bpmn: &str, codec: &str, with_order_xsd: bool) {
    write(root, "package.yaml", PACKAGE_YAML);
    if with_order_xsd {
        write(root, "schemas/orders/orders.xsd", ORDER_XSD);
        write(root, "schemas/orders/codec-manifest.yaml", CODEC_MANIFEST);
    }
    write(root, "templates/x.hbs", "<v>ok</v>");
    write(
        root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"{codec}\"\n"
        ),
    );
    write(root, "bpmn/flow.bpmn", bpmn);
}

/// A `start → exclusiveGateway → end` BPMN. `vars` is the `<q:variables>` block body; the
/// gateway-branch flow carries `condition`. The start event sources channel `ch`.
fn bpmn_flow_cond(vars: &str, condition: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables>{vars}</q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
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

/// A `start → serviceTask → end` BPMN whose serviceTask carries `<q:param name="k"
/// expression=param_expr/>`. The start event sources channel `ch`.
fn bpmn_param(vars: &str, param_expr: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables>{vars}</q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="x.hbs">
      <bpmn:extensionElements><q:param name="k" expression="{param_expr}"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

fn lint_closed(bpmn: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), bpmn, "urn:orders", true);
    lint_dir(dir.path())
}

fn lint_opaque(bpmn: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), bpmn, "raw-text", false);
    lint_dir(dir.path())
}

fn path_family_codes(report: &LintReport) -> Vec<String> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.code.starts_with("SUTRA.CONFIG.BPMN.PATH_"))
        .map(|d| d.code.clone())
        .collect()
}

const ORDER_SCHEMA_VAR: &str = r#"<q:variable name="order" schema="urn:orders"/>"#;
const BLOB_SOURCE_VAR: &str = r#"<q:variable name="blob" source="ch"/>"#;

// ---- FEEL flow-condition site (var.field) ----------------------------------------------

#[test]
fn a_flow_condition_unknown_field_on_a_closed_schema_var_is_an_error() {
    // `order.Nope` — no such field under the closed `Order` container → deploy-blocking ERROR at
    // the PATH_UNKNOWN_FIELD (nav-family) code, exactly as a payload-rooted typo.
    let report = lint_closed(&bpmn_flow_cond(ORDER_SCHEMA_VAR, "order.Nope &gt; 1000"));
    assert!(
        report.errors().any(|d| d.code == PATH_UNKNOWN),
        "an unknown field on a closed <q:variable schema> read in a flow condition must be \
         PATH_UNKNOWN_FIELD (ERROR); got: {:?}",
        report.diagnostics
    );
    assert!(
        report.errors().any(|d| d.message.contains("order.Nope")),
        "the diagnostic names the offending path; got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_flow_condition_valid_field_on_a_closed_schema_var_is_clean() {
    // `order.Amount` is a declared decimal field → numeric `>` is fine → no nav diagnostic.
    let report = lint_closed(&bpmn_flow_cond(ORDER_SCHEMA_VAR, "order.Amount &gt; 1000"));
    assert!(
        path_family_codes(&report).is_empty(),
        "a declared variable field must not be flagged; got: {:?}",
        report.diagnostics
    );
    assert!(!report.has_errors(), "got: {:?}", report.diagnostics);
}

#[test]
fn a_flow_condition_field_on_an_open_var_is_an_advisory_warning() {
    // `blob` is sourced from the opaque `raw-text` codec → no single message shape → its field
    // reads are UNVERIFIABLE (WARN), never a false ERROR (advise-don't-gatekeep).
    let report = lint_opaque(&bpmn_flow_cond(BLOB_SOURCE_VAR, r#"blob.anything = "x""#));
    assert!(
        !report.errors().any(|d| d.code == PATH_UNKNOWN),
        "an open/opaque variable field must not hard-error; got: {:?}",
        report.diagnostics
    );
    assert!(
        report
            .warnings()
            .any(|d| d.code == PATH_UNVERIFIABLE && d.message.contains("blob.anything")),
        "an open/opaque variable field must be PATH_UNVERIFIABLE (WARN); got: {:?}",
        report.diagnostics
    );
}

// ---- <q:param expression> site (var.field) ---------------------------------------------

#[test]
fn a_param_unknown_field_on_a_closed_schema_var_is_an_error() {
    // `<q:param expression="order.Nope">` — a serviceTask param FEEL read of an unknown field on
    // the closed `Order` shape → deploy-blocking PATH_UNKNOWN_FIELD.
    let report = lint_closed(&bpmn_param(ORDER_SCHEMA_VAR, "order.Nope"));
    assert!(
        report.errors().any(|d| d.code == PATH_UNKNOWN),
        "an unknown field read by <q:param> on a closed <q:variable schema> must be \
         PATH_UNKNOWN_FIELD (ERROR); got: {:?}",
        report.diagnostics
    );
    assert!(
        report.errors().any(|d| d.message.contains("param 'k'")),
        "the diagnostic names the <q:param> site; got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_param_valid_field_on_a_closed_schema_var_is_clean() {
    // `<q:param expression="order.Amount">` — a declared field → no nav diagnostic, no error.
    let report = lint_closed(&bpmn_param(ORDER_SCHEMA_VAR, "order.Amount"));
    assert!(
        path_family_codes(&report).is_empty(),
        "a declared variable field read by <q:param> must not be flagged; got: {:?}",
        report.diagnostics
    );
    assert!(!report.has_errors(), "got: {:?}", report.diagnostics);
}
