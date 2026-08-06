//! Schema-shape service-task input contracts. A `<q:variable name=… schema="urn:codec">`
//! binds the variable's value to a codec's message shape; template reads of `{{var.field}}` are
//! field-checked against it at deploy with the same closed→ERROR / open→WARN discipline the
//! `payload` root gets: an unknown field on a closed schema is `FIELD_UNKNOWN` (ERROR); a declared
//! field is clean.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

/// A single-root closed XSD: `Order { Id: string, Amount: decimal }`.
const ORDER_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Order"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>
"#;

const CODEC_MANIFEST: &str = "schemaKind: xsd\nformats: [xml, json, yaml]\n";

const FIELD_UNKNOWN: &str = "SUTRA.CONFIG.TEMPLATE.FIELD_UNKNOWN";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build a package whose process declares `<q:variable name="order" schema="urn:orders">` and reads
/// `{{order.<field>}}` in a serviceTask template. The `orders` codec (single-root `Order` XSD) also
/// backs the intake channel so the deployment resolves cleanly.
fn lint_order_field(field: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "schemas/orders/orders.xsd", ORDER_XSD);
    write(root, "schemas/orders/codec-manifest.yaml", CODEC_MANIFEST);
    write(
        root,
        "templates/x.hbs",
        &format!("<v>{{{{order.{field}}}}}</v>"),
    );
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"urn:orders\"\n",
    );
    write(
        root,
        "bpmn/flow.bpmn",
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables><q:variable name="order" schema="urn:orders"/></q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="x.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#,
    );
    lint_dir(root)
}

fn has_field_unknown(report: &LintReport) -> bool {
    report.diagnostics.iter().any(|d| d.code == FIELD_UNKNOWN)
}

#[test]
fn declared_variable_field_is_clean() {
    // `{{order.Id}}` — `Id` is a declared field on the closed `Order` schema.
    assert!(
        !has_field_unknown(&lint_order_field("Id")),
        "a declared variable field must not be flagged"
    );
}

#[test]
fn unknown_variable_field_is_an_error() {
    // `{{order.Nope}}` — no such field under the closed `Order` container → deploy-blocking ERROR.
    assert!(
        has_field_unknown(&lint_order_field("Nope")),
        "an unknown field on a closed <q:variable schema> must be FIELD_UNKNOWN (ERROR)"
    );
}

// ---- @source-derived variable shapes --------------------------------------------------------

/// Build a package whose process declares `<q:variable name="order" source="ch">` (the
/// shape is derived from the intake channel `ch`'s codec, NO `@schema`) and reads
/// `{{order.<field>}}`. The `ch` channel binds the single-root `Order` codec.
fn lint_sourced_field(field: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "schemas/orders/orders.xsd", ORDER_XSD);
    write(root, "schemas/orders/codec-manifest.yaml", CODEC_MANIFEST);
    write(
        root,
        "templates/x.hbs",
        &format!("<v>{{{{order.{field}}}}}</v>"),
    );
    write(
        root,
        "channels.yaml",
        "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"urn:orders\"\n",
    );
    write(
        root,
        "bpmn/flow.bpmn",
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables><q:variable name="order" source="ch"/></q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="x.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#,
    );
    lint_dir(root)
}

#[test]
fn source_derived_variable_field_is_clean() {
    // `{{order.Id}}`, `order` sourced from channel `ch` (Order codec): `Id` is declared.
    assert!(
        !has_field_unknown(&lint_sourced_field("Id")),
        "a declared @source-derived variable field must not be flagged"
    );
}

#[test]
fn unknown_source_derived_variable_field_is_an_error() {
    // `{{order.Nope}}`: no such field on the closed `Order` shape the source channel's
    // codec exposes → deploy-blocking FIELD_UNKNOWN, exactly as the `@schema` half.
    assert!(
        has_field_unknown(&lint_sourced_field("Nope")),
        "an unknown field on a @source-derived variable shape must be FIELD_UNKNOWN (ERROR)"
    );
}
