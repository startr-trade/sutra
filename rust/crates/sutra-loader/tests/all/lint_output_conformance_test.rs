//! `check_output_conformance` — a template's declared `outputMessageType` is checked against the
//! codec of the channel the render is actually DESTINED FOR.
//!
//! The distinction the fixture below pins: a `<q:reply>` rides the inbound channel back to the
//! caller, so the intake codec is its contract; a `<q:send channel="X">` is encoded for X's
//! consumer, so X's target codec is. Checking a send against the intake codec compares the render
//! to the wrong contract and errors on a correct transform.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const UNKNOWN: &str = "SUTRA.CONFIG.TEMPLATE.OUTPUT_TYPE_UNKNOWN";
const UNVERIFIABLE: &str = "SUTRA.CONFIG.TEMPLATE.OUTPUT_UNVERIFIABLE";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

/// Two codecs: `in` declares Inbound, `out` declares Outbound. They share no type, so a render
/// checked against the wrong one is unambiguous.
const IN_XSD: &str = r#"<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:t:in"
           xmlns="urn:t:in" elementFormDefault="qualified">
  <xs:element name="Inbound"><xs:complexType><xs:sequence>
    <xs:element name="id" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

const OUT_XSD: &str = r#"<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:t:out"
           xmlns="urn:t:out" elementFormDefault="qualified">
  <xs:element name="Outbound"><xs:complexType><xs:sequence>
    <xs:element name="id" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

const CHANNELS: &str = "channels:\n\
     \x20 - name: in-ch\n    transport: http\n    bind: \"POST /channels/in-ch\"\n    codec: urn:in\n\
     \x20 - name: out-ch\n    transport: local\n    codec: urn:out\n\
     \x20 - name: to-out\n    direction: outbound\n    transport: local\n    bind: local://out-ch\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A package whose single process renders `t.hbs` on a node carrying `node_ext`, with the
/// template declaring `outputMessageType: <declared>`.
fn build(root: &Path, node_ext: &str, declared: &str) {
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "channels.yaml", CHANNELS);
    write(
        root,
        "schemas/in/codec-manifest.yaml",
        "schemaKind: xsd\nformats: [xml]\n",
    );
    write(root, "schemas/in/in.xsd", IN_XSD);
    write(
        root,
        "schemas/out/codec-manifest.yaml",
        "schemaKind: xsd\nformats: [xml]\n",
    );
    write(root, "schemas/out/out.xsd", OUT_XSD);
    write(root, "templates/t.hbs", "<v>{{payload.id}}</v>");
    write(
        root,
        "templates/template-manifest.yaml",
        &format!("templates:\n  - file: t.hbs\n    outputMessageType: {declared}\n"),
    );
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="in-ch" messageTypeValue="Inbound"/>
        <q:onValidation mode="route"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="t.hbs">
      <bpmn:extensionElements>{node_ext}</bpmn:extensionElements>
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

fn has(report: &LintReport, code: &str) -> bool {
    report.diagnostics.iter().any(|d| d.code == code)
}

fn report_for(node_ext: &str, declared: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), node_ext, declared);
    lint_dir(dir.path())
}

/// A `<q:send channel="to-out">` (binding `local://out-ch`) is destined for `out-ch`'s codec, so
/// declaring that codec's type is CORRECT — and used to error, because the check resolved the
/// INTAKE codec (`urn:in`, which cannot produce `Outbound`) instead.
#[test]
fn a_send_is_checked_against_its_target_channels_codec() {
    let report = report_for(
        r#"<q:send channel="to-out" messageType="Outbound"/>"#,
        "Outbound",
    );
    assert!(
        !has(&report, UNKNOWN),
        "a send's output type must be checked against the TARGET codec; diagnostics: {:#?}",
        report.diagnostics
    );
}

/// The same send declaring the INTAKE codec's type is genuinely wrong — `out-ch` cannot produce
/// `Inbound` — so the error must still fire. (Proves the fix did not simply disable the check.)
#[test]
fn a_send_declaring_a_type_its_target_cannot_produce_still_errors() {
    let report = report_for(
        r#"<q:send channel="to-out" messageType="Inbound"/>"#,
        "Inbound",
    );
    assert!(
        has(&report, UNKNOWN),
        "a type the target codec cannot produce is still a dead binding; diagnostics: {:#?}",
        report.diagnostics
    );
}

/// A `<q:reply>` DOES ride the inbound channel, so the intake codec stays its contract.
#[test]
fn a_reply_is_still_checked_against_the_intake_codec() {
    let clean = report_for(r#"<q:reply mode="native"/>"#, "Inbound");
    assert!(
        !has(&clean, UNKNOWN),
        "diagnostics: {:#?}",
        clean.diagnostics
    );

    let wrong = report_for(r#"<q:reply mode="native"/>"#, "Outbound");
    assert!(
        has(&wrong, UNKNOWN),
        "a reply cannot emit a type the intake codec does not declare; diagnostics: {:#?}",
        wrong.diagnostics
    );
}

/// `<q:send destination="…">` names an endpoint, not a declared channel: there is no codec in
/// this deployment to check against, so it is honestly unverifiable rather than falsely checked.
#[test]
fn a_send_to_a_bare_destination_is_unverifiable_not_wrong() {
    let report = report_for(
        r#"<q:send destination="https://example.test/hook" messageType="Outbound"/>"#,
        "Outbound",
    );
    assert!(
        !has(&report, UNKNOWN),
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(
        has(&report, UNVERIFIABLE),
        "an explicit destination has no codec to check; diagnostics: {:#?}",
        report.diagnostics
    );
}

// ---- the render itself, classified by the codec that must carry it -------------------------
//
// Everything above compares two DECLARATIONS: the manifest's `outputMessageType` against the
// destination codec's type set. That cannot see the failure that actually matters — a template
// that renders the wrong message while declaring the right one. The checks below ask the codec
// what the template's own text IS.
//
// They work because a template is text with holes, and for a text-shaped wire format the holes
// are leaf content: the element structure that decides classification is already present
// unrendered.

const MISMATCH: &str = "SUTRA.CONFIG.TEMPLATE.OUTPUT_MISMATCH";
const CODEC_UNKNOWN: &str = "SUTRA.CONFIG.OUTPUT.CODEC_UNKNOWN";

/// The fixture above, with the template body and the manifest entry under the author's control.
fn report_for_render(node_ext: &str, template: &str, manifest_entry: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path(), node_ext, "Outbound");
    write(dir.path(), "templates/t.hbs", template);
    write(
        dir.path(),
        "templates/template-manifest.yaml",
        &format!("templates:\n  - file: t.hbs\n{manifest_entry}"),
    );
    lint_dir(dir.path())
}

fn codes_of(report: &LintReport) -> Vec<&str> {
    report.diagnostics.iter().map(|d| d.code.as_str()).collect()
}

#[test]
fn a_template_rendering_the_declared_message_passes() {
    let report = report_for_render(
        r#"<q:send channel="to-out" messageType="Outbound"/>"#,
        r#"<Outbound xmlns="urn:t:out"><id>{{payload.id}}</id></Outbound>"#,
        "    outputMessageType: Outbound\n",
    );
    assert!(
        !has(&report, MISMATCH),
        "a template that renders what it declares must pass: {:?}",
        codes_of(&report)
    );
}

/// The failure the declaration check cannot see: the manifest says `Outbound` and the codec
/// agrees `Outbound` is producible, but the template renders something else entirely.
#[test]
fn a_template_rendering_a_different_message_than_it_declares_is_caught() {
    let report = report_for_render(
        r#"<q:send channel="to-out" messageType="Outbound"/>"#,
        r#"<Inbound xmlns="urn:t:in"><id>{{payload.id}}</id></Inbound>"#,
        "    outputMessageType: Outbound\n",
    );
    assert!(
        has(&report, MISMATCH),
        "a wrong-envelope template must be caught: {:?}",
        codes_of(&report)
    );
    let reported = report
        .diagnostics
        .iter()
        .find(|d| d.code == MISMATCH)
        .expect("checked above");
    // Both halves of the comparison must be in the message, or the author cannot tell which of
    // the two to change — the manifest or the template.
    assert!(
        reported.message.contains("'Outbound'") && reported.message.contains("'Inbound'"),
        "the declared and the actual message type must both be named: {}",
        reported.message
    );
}

/// A codec named on the output but absent from the deployment is fail-closed, not a warning:
/// stating it is how an author ASKS for the contract check, so a typo must not silently exempt
/// the one output that requested one.
#[test]
fn a_stated_codec_the_deployment_does_not_carry_is_an_error() {
    let report = report_for_render(
        r#"<q:send channel="to-out" codec="urn:nobody" messageType="Outbound"/>"#,
        r#"<Outbound xmlns="urn:t:out"><id>{{payload.id}}</id></Outbound>"#,
        "    outputMessageType: Outbound\n",
    );
    assert!(
        has(&report, CODEC_UNKNOWN),
        "an unresolvable stated codec must error: {:?}",
        codes_of(&report)
    );
}

/// A send to a bare `@destination` has no channel to infer a codec from — but STATING one holds
/// the render to a contract anyway, which is the case inference can never reach.
#[test]
fn a_stated_codec_governs_a_destination_send_that_inference_cannot_reach() {
    let inferred = report_for_render(
        r#"<q:send destination="https://example.test/out" messageType="Outbound"/>"#,
        r#"<Outbound xmlns="urn:t:out"><id>{{payload.id}}</id></Outbound>"#,
        "    outputMessageType: Outbound\n",
    );
    assert!(
        has(&inferred, UNVERIFIABLE),
        "an endpoint send carries no codec to infer: {:?}",
        codes_of(&inferred)
    );
    let stated = report_for_render(
        r#"<q:send destination="https://example.test/out" codec="urn:out" messageType="Outbound"/>"#,
        r#"<Outbound xmlns="urn:t:out"><id>{{payload.id}}</id></Outbound>"#,
        "    outputMessageType: Outbound\n",
    );
    assert!(
        !has(&stated, UNVERIFIABLE) && !has(&stated, MISMATCH),
        "stating the codec makes the same send verifiable: {:?}",
        codes_of(&stated)
    );
}
