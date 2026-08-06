//! Behavioural conformance of the streaming validator against recorded expectations: every
//! case's `(line, column) × count` below is the expected violation set for that schema/instance
//! pair — presence, severity and location parity, message prose free. Every schema and instance
//! here is written inline, so the file is self-contained. The corpus-scale comparison runs in
//! whichever repository owns a message corpus; this file locks the conventions at unit
//! granularity so a regression pinpoints the exact behaviour.

use sutra_xsd::{Schema, Severity};

const PROBE_XSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns="urn:probe" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="urn:probe">
    <xs:element name="Doc" type="Doc"/>
    <xs:simpleType name="Amt_Simple">
        <xs:restriction base="xs:decimal">
            <xs:fractionDigits value="5"/>
            <xs:totalDigits value="18"/>
            <xs:minInclusive value="0"/>
        </xs:restriction>
    </xs:simpleType>
    <xs:simpleType name="Ccy">
        <xs:restriction base="xs:string">
            <xs:pattern value="[A-Z]{3,3}"/>
        </xs:restriction>
    </xs:simpleType>
    <xs:complexType name="Amt">
        <xs:simpleContent>
            <xs:extension base="Amt_Simple">
                <xs:attribute name="Ccy" type="Ccy" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>
    <xs:simpleType name="Max35Text">
        <xs:restriction base="xs:string">
            <xs:minLength value="1"/>
            <xs:maxLength value="35"/>
        </xs:restriction>
    </xs:simpleType>
    <xs:simpleType name="Mtd">
        <xs:restriction base="xs:string">
            <xs:enumeration value="INDA"/>
            <xs:enumeration value="INGA"/>
        </xs:restriction>
    </xs:simpleType>
    <xs:complexType name="Doc">
        <xs:sequence>
            <xs:element name="Id" type="Max35Text"/>
            <xs:element name="When" type="xs:dateTime"/>
            <xs:element name="Mtd" type="Mtd"/>
            <xs:element name="Amount" type="Amt"/>
            <xs:element name="Note" type="Max35Text" minOccurs="0" maxOccurs="2"/>
            <xs:element name="Grp" type="Grp" minOccurs="0"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="Grp">
        <xs:sequence>
            <xs:element name="A" type="Max35Text"/>
            <xs:element name="B" type="Max35Text"/>
            <xs:element name="C" type="Max35Text" minOccurs="0"/>
        </xs:sequence>
    </xs:complexType>
</xs:schema>
"#;

const OK: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Doc xmlns="urn:probe">
  <Id>ABC-1</Id>
  <When>2026-05-22T10:00:00</When>
  <Mtd>INDA</Mtd>
  <Amount Ccy="USD">100.00</Amount>
  <Note>hello</Note>
  <Grp><A>a</A><B>b</B></Grp>
</Doc>
"#;

fn schema() -> Schema {
    Schema::compile(PROBE_XSD.as_bytes()).expect("probe schema compiles")
}

/// Drop the whole line containing `needle` — the `sed '/x/d'` a case uses to delete an element.
fn drop_line(doc: &str, needle: &str) -> String {
    doc.lines()
        .filter(|l| !l.contains(needle))
        .map(|l| format!("{l}\n"))
        .collect()
}

#[track_caller]
fn assert_positions(doc: &str, expected: &[(u32, u32)]) {
    let violations = schema().validate(doc.as_bytes()).expect("well-formed");
    let got: Vec<(u32, u32)> = violations
        .iter()
        .map(|v| (v.pos.line, v.pos.column))
        .collect();
    assert_eq!(got, expected, "violations: {violations:#?}");
    assert!(violations.iter().all(|v| v.severity == Severity::Error));
}

#[test]
fn clean_document_has_no_violations() {
    assert_positions(OK, &[]);
}

#[test]
fn dropped_required_element_flags_the_next_sibling_start() {
    // Expected: ERROR 4:8 (invalid content starting with 'Mtd', 'When' expected).
    assert_positions(&drop_line(OK, "<When>"), &[(4, 8)]);
}

#[test]
fn dropped_trailing_required_element_flags_the_parent_end() {
    // Expected: ERROR 8:22 (content of 'Grp' not complete).
    let doc = OK.replace("<Grp><A>a</A><B>b</B></Grp>", "<Grp><A>a</A></Grp>");
    assert_positions(&doc, &[(8, 22)]);
}

#[test]
fn reordered_sequence_flags_the_first_out_of_place_element() {
    // Expected: ERROR 3:9 (one error only — recovery then validates both elements).
    let doc = OK
        .replace("<Id>ABC-1</Id>", "@@")
        .replace("<When>2026-05-22T10:00:00</When>", "<Id>ABC-1</Id>")
        .replace("@@", "<When>2026-05-22T10:00:00</When>");
    assert_positions(&doc, &[(3, 9)]);
}

#[test]
fn unknown_element_flags_its_own_start() {
    // Expected: ERROR 5:25.
    let doc = OK.replace("<Mtd>INDA</Mtd>", "<Mtd>INDA</Mtd><Bogus>x</Bogus>");
    assert_positions(&doc, &[(5, 25)]);
}

#[test]
fn attribute_pattern_violation_is_two_errors_at_the_start_tag() {
    // Expected: ERROR 6:21 ×2 (facet + attribute-value companion).
    let doc = OK.replace("Ccy=\"USD\"", "Ccy=\"US1\"");
    assert_positions(&doc, &[(6, 21), (6, 21)]);
}

#[test]
fn max_length_violation_is_two_errors_at_the_end_tag() {
    // Expected: ERROR 3:48 ×2.
    let doc = OK.replace(
        "<Id>ABC-1</Id>",
        "<Id>AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDD</Id>",
    );
    assert_positions(&doc, &[(3, 48), (3, 48)]);
}

#[test]
fn min_length_violation_on_empty_value() {
    // Expected: ERROR 3:12 ×2.
    let doc = OK.replace("<Id>ABC-1</Id>", "<Id></Id>");
    assert_positions(&doc, &[(3, 12), (3, 12)]);
}

#[test]
fn enumeration_violation_is_two_errors_at_the_end_tag() {
    // Expected: ERROR 5:18 ×2.
    let doc = OK.replace("<Mtd>INDA</Mtd>", "<Mtd>ZZZZ</Mtd>");
    assert_positions(&doc, &[(5, 18), (5, 18)]);
}

#[test]
fn fraction_digits_violation_on_simple_content() {
    // Expected: ERROR 6:40 ×2 (facet + simpleContent companion).
    let doc = OK.replace(">100.00<", ">100.123456<");
    assert_positions(&doc, &[(6, 40), (6, 40)]);
}

#[test]
fn min_inclusive_violation_on_simple_content() {
    // Expected: ERROR 6:35 ×2.
    let doc = OK.replace(">100.00<", ">-5.00<");
    assert_positions(&doc, &[(6, 35), (6, 35)]);
}

#[test]
fn datatype_violation_is_two_errors_at_the_end_tag() {
    // Expected: ERROR 4:26 ×2.
    let doc = OK.replace(
        "<When>2026-05-22T10:00:00</When>",
        "<When>not-a-date</When>",
    );
    assert_positions(&doc, &[(4, 26), (4, 26)]);
}

#[test]
fn missing_required_attribute_is_one_error_at_the_start_tag() {
    // Expected: ERROR 6:11.
    let doc = OK.replace(" Ccy=\"USD\"", "");
    assert_positions(&doc, &[(6, 11)]);
}

#[test]
fn unknown_attribute_is_one_error_at_the_start_tag() {
    // Expected: ERROR 6:31.
    let doc = OK.replace("Ccy=\"USD\"", "Ccy=\"USD\" Extra=\"1\"");
    assert_positions(&doc, &[(6, 31)]);
}

#[test]
fn bounded_max_occurs_overflow_flags_the_overflowing_start() {
    // Expected: ERROR 7:37 (2.4.e), one error only.
    let doc = OK.replace(
        "<Note>hello</Note>",
        "<Note>a</Note><Note>b</Note><Note>c</Note>",
    );
    assert_positions(&doc, &[(7, 37)]);
}

#[test]
fn undeclared_root_is_one_error_and_the_subtree_is_skipped() {
    // Expected: ERROR 2:24.
    let doc = OK.replace("urn:probe", "urn:other");
    assert_positions(&doc, &[(2, 24)]);
}

#[test]
fn stray_text_in_element_only_content_flags_the_parent_end() {
    // Expected: ERROR 8:40.
    let doc = OK.replace("<Grp><A>a</A>", "<Grp>stray text<A>a</A>");
    assert_positions(&doc, &[(8, 40)]);
}

#[test]
fn element_child_inside_simple_type_is_three_errors_at_the_end_tag() {
    // Expected: ERROR 3:20 ×3 (no-children + minLength on the empty text + companion).
    let doc = OK.replace("<Id>ABC-1</Id>", "<Id><X>1</X></Id>");
    assert_positions(&doc, &[(3, 20), (3, 20), (3, 20)]);
}

#[test]
fn violations_in_two_parents_both_surface() {
    // Expected: ERROR 4:8 and ERROR 7:22.
    let doc = drop_line(OK, "<When>").replace("<Grp><A>a</A><B>b</B></Grp>", "<Grp><A>a</A></Grp>");
    assert_positions(&doc, &[(4, 8), (7, 22)]);
}

#[test]
fn poisoned_parent_still_value_checks_the_offending_element() {
    // Expected: ERROR 4:8, then ERROR 4:18 ×2 (the enum violation inside 'Mtd').
    let doc = drop_line(OK, "<When>").replace("<Mtd>INDA</Mtd>", "<Mtd>ZZZZ</Mtd>");
    assert_positions(&doc, &[(4, 8), (4, 18), (4, 18)]);
}

#[test]
fn poisoned_parent_still_validates_following_siblings() {
    // Expected: ERROR 4:8, then ERROR 5:21 ×2 (the Ccy pattern violation on 'Amount').
    let doc = drop_line(OK, "<When>").replace("Ccy=\"USD\"", "Ccy=\"US1\"");
    assert_positions(&doc, &[(4, 8), (5, 21), (5, 21)]);
}

#[test]
fn poisoned_parent_reports_no_second_content_model_error() {
    // Expected: ERROR 4:8 only (a later unknown sibling is silently skipped).
    let doc =
        drop_line(OK, "<When>").replace("<Note>hello</Note>", "<Note>hello</Note><Bogus>x</Bogus>");
    assert_positions(&doc, &[(4, 8)]);
}

#[test]
fn poisoned_parent_reports_no_completeness_error_at_its_end() {
    // Expected: ERROR 4:8 only (the also-missing 'Amount' is not reported).
    let doc = drop_line(&drop_line(OK, "<When>"), "<Amount");
    assert_positions(&doc, &[(4, 8)]);
}

#[test]
fn out_of_place_complex_element_is_still_validated_inside() {
    // Expected: ERROR 6:8 (Grp out of place) + ERROR 6:22 (Grp incomplete).
    let doc = OK
        .replace("<Amount Ccy=\"USD\">100.00</Amount>", "@@")
        .replace(
            "<Grp><A>a</A><B>b</B></Grp>",
            "<Amount Ccy=\"USD\">100.00</Amount>",
        )
        .replace("@@", "<Grp><A>a</A></Grp>");
    assert_positions(&doc, &[(6, 8), (6, 22)]);
}

#[test]
fn recovery_lookup_is_scoped_to_the_parent_content_model() {
    // 'A' is declared only inside 'Grp'; inserted under 'Doc' it is flagged once and
    // its (facet-violating) value is NOT checked. Expected: ERROR 7:24 only.
    let doc = OK.replace(
        "<Note>hello</Note>",
        "<Note>hello</Note><A>AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDD</A>",
    );
    assert_positions(&doc, &[(7, 24)]);
}

#[test]
fn empty_element_form_reports_at_the_empty_tag() {
    // Expected: ERROR 3:8 ×2 (minLength on '' + companion).
    let doc = OK.replace("<Id>ABC-1</Id>", "<Id/>");
    assert_positions(&doc, &[(3, 8), (3, 8)]);
}

#[test]
fn datatype_edge_lexicals_match_the_expected_lexical_space() {
    // Accepted: 24:00:00, +05:30 timezone, '+.5', '1.'.
    for (from, to) in [
        (">2026-05-22T10:00:00<", ">2026-05-22T24:00:00<"),
        (">2026-05-22T10:00:00<", ">2026-05-22T10:00:00+05:30<"),
        (">100.00<", ">+.5<"),
        (">100.00<", ">1.<"),
    ] {
        assert_positions(&OK.replace(from, to), &[]);
    }
    // Rejected: Feb 30 (2 errors at 4:35), '12,34' (2 at 6:35), '1e3' (2 at 6:33).
    assert_positions(
        &OK.replace(">2026-05-22T10:00:00<", ">2026-02-30T10:00:00<"),
        &[(4, 35), (4, 35)],
    );
    assert_positions(&OK.replace(">100.00<", ">12,34<"), &[(6, 35), (6, 35)]);
    assert_positions(&OK.replace(">100.00<", ">1e3<"), &[(6, 33), (6, 33)]);
}

#[test]
fn malformed_xml_is_a_document_error_not_violations() {
    let doc = OK.replace("</Doc>", "");
    assert!(schema().validate(doc.as_bytes()).is_err());
    let doc = "<?xml version=\"1.0\"?><!DOCTYPE Doc []><Doc xmlns=\"urn:probe\"/>";
    assert!(schema().validate(doc.as_bytes()).is_err());
}

/// An extension codec's profile: proves the crate emits whatever codes the CALLER
/// publishes and knows no message standard of its own. Deliberately not a real product
/// code — a real one would only be a copy of the private codec's constants.
const EXTENSION: sutra_xsd::DiagnosticProfile = sutra_xsd::DiagnosticProfile {
    violation: "EXAMPLE.VALIDATE.EXT.SCHEMA_VIOLATION",
    not_found: "EXAMPLE.VALIDATE.EXT.SCHEMA_NOT_FOUND",
};

#[test]
fn diagnostic_profiles_carry_the_caller_s_codes() {
    let doc = OK.replace("<Mtd>INDA</Mtd>", "<Mtd>ZZZZ</Mtd>");
    let violations = schema().validate(doc.as_bytes()).unwrap();

    let module = violations[0].diagnostic(sutra_xsd::DiagnosticProfile::MODULE_CODEC);
    assert_eq!(module.code, "SUTRA.PARSE.XSD.SCHEMA_VIOLATION");
    assert_eq!(module.path, "line 5:18");
    assert_eq!(module.value, None);

    // Same violation, same slots — only the code changes with the profile.
    let ext = violations[0].diagnostic(EXTENSION);
    assert_eq!(ext.code, "EXAMPLE.VALIDATE.EXT.SCHEMA_VIOLATION");
    assert_eq!(ext.path, module.path);
    assert_eq!(ext.message, module.message);

    let not_found = sutra_xsd::schema_not_found(
        sutra_xsd::DiagnosticProfile::MODULE_CODEC,
        "urn:example:unbundled",
        "Document",
    );
    assert_eq!(not_found.code, "SUTRA.PARSE.XSD.SCHEMA_NOT_FOUND");
    assert_eq!(not_found.path, "/Document");
    assert_eq!(not_found.value.as_deref(), Some("urn:example:unbundled"));
    assert_eq!(
        sutra_xsd::schema_not_found(EXTENSION, "urn:example:unbundled", "Document").code,
        "EXAMPLE.VALIDATE.EXT.SCHEMA_NOT_FOUND"
    );
}
