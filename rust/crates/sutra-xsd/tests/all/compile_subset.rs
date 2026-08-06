//! The compile gate: every schema this crate's suite validates against compiles under the
//! Tier-1 subset, and every OUT construct is rejected with a collected finding — the
//! module-codec authoring contract.
//!
//! Two fixture families feed it. `tests/data/schemas/` holds AUTHORED schemas written in the
//! Standards-Editor idiom the subset targets (see its `provenance.md`) — nothing published is
//! vendored, so this crate carries no third-party licensing surface. `examples/**` holds the
//! module schemas the public example deployments ship, compiled in place.
//!
//! Breadth over a large registered corpus is a different gate with a different fixture: it runs
//! in the repository that owns such a corpus, against this same compiler, as a conformance
//! suite.

use sutra_xsd::{Schema, SchemaSet};

/// The authored fixture set — a fixed, committed directory, so its size is asserted exactly: a
/// fixture silently dropped (or an untracked one appearing) is a change in sweep breadth that
/// should have to touch this line.
const FIXTURES: usize = 2;

#[test]
fn the_authored_fixtures_compile() {
    let entries = crate::support::fixtures();
    assert_eq!(
        entries.len(),
        FIXTURES,
        "the authored schema fixture set in {}",
        crate::support::fixtures_dir().display()
    );
    let mut compiled = 0usize;
    for path in entries {
        let bytes = std::fs::read(&path).unwrap();
        let schema = Schema::compile(&bytes)
            .unwrap_or_else(|e| panic!("{} must compile:\n{e}", path.display()));
        assert!(
            schema.target_namespace().starts_with("urn:sutra:test:"),
            "{}",
            path.display()
        );
        assert!(schema.root_names().count() >= 1, "{}", path.display());
        compiled += 1;
    }
    assert_eq!(compiled, FIXTURES);
}

/// The construct families the authored fixtures must keep exercising. Asserted structurally
/// rather than trusted: this is what the fixtures REPLACED a vendored corpus for, so a fixture
/// edit that quietly drops a family has to fail here.
#[test]
fn the_authored_fixtures_cover_every_construct_family() {
    let mut seen: Vec<&str> = Vec::new();
    for path in crate::support::fixtures() {
        let text = std::fs::read_to_string(&path).unwrap();
        for (family, needle) in [
            ("nested-sequence", "<xs:sequence>"),
            ("choice", "<xs:choice>"),
            ("repeated-unbounded", r#"maxOccurs="unbounded""#),
            ("repeated-bounded", r#"maxOccurs="9""#),
            ("simple-content", "<xs:simpleContent>"),
            ("required-attribute", r#"use="required""#),
            ("facet-fraction-digits", "<xs:fractionDigits"),
            ("facet-total-digits", "<xs:totalDigits"),
            ("facet-min-inclusive", "<xs:minInclusive"),
            ("facet-max-inclusive", "<xs:maxInclusive"),
            ("facet-enumeration", "<xs:enumeration"),
            ("facet-pattern", "<xs:pattern"),
            ("facet-length", "<xs:length"),
            ("facet-min-length", "<xs:minLength"),
            ("facet-max-length", "<xs:maxLength"),
            ("builtin-datetime", r#"type="xs:dateTime""#),
            ("builtin-date", r#"type="xs:date""#),
            ("builtin-boolean", r#"type="xs:boolean""#),
            ("builtin-integer", r#"type="xs:integer""#),
            ("builtin-base64", r#"type="xs:base64Binary""#),
        ] {
            if text.contains(needle) && !seen.contains(&family) {
                seen.push(family);
            }
        }
    }
    seen.sort_unstable();
    let mut expected = [
        "builtin-base64",
        "builtin-boolean",
        "builtin-date",
        "builtin-datetime",
        "builtin-integer",
        "choice",
        "facet-enumeration",
        "facet-fraction-digits",
        "facet-length",
        "facet-max-inclusive",
        "facet-max-length",
        "facet-min-inclusive",
        "facet-min-length",
        "facet-pattern",
        "facet-total-digits",
        "nested-sequence",
        "repeated-bounded",
        "repeated-unbounded",
        "required-attribute",
        "simple-content",
    ];
    expected.sort_unstable();
    assert_eq!(seen, expected, "construct-family coverage of the fixtures");
}

#[test]
fn the_example_module_schemas_compile() {
    let transfer = crate::support::repo_schema(
        "examples/money-transfer/deployments-src/default--money-transfer--1.0.0/schemas/transfer/transfer.xsd",
    );
    let approval = crate::support::repo_schema(
        "examples/approval-hold/deployments-src/default--approval--1.0.0/schemas/approval/approval.xsd",
    );
    let set = SchemaSet::compile(&[&transfer, &approval]).expect("module schemas compile");
    let mut roots: Vec<&str> = set.root_names().collect();
    roots.sort_unstable();
    assert_eq!(
        roots,
        [
            "ApprovalDecision",
            "ApprovalRequest",
            "BalanceQuery",
            "CoverageQuery",
            "CoverageReset",
            "TransferRequest",
        ]
    );
    assert_eq!(
        set.schema_for_root("TransferRequest")
            .unwrap()
            .target_namespace(),
        "urn:sutra:transfer"
    );
}

// ---------------------------------------------------------------------------
// OUT constructs — reject at compile with a "not in the supported subset" finding
// ---------------------------------------------------------------------------

fn wrap(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<xs:schema xmlns="urn:t" xmlns:xs="http://www.w3.org/2001/XMLSchema"
           elementFormDefault="qualified" targetNamespace="urn:t">
{body}
</xs:schema>"#
    )
}

#[track_caller]
fn assert_rejected(body: &str, expect_in_message: &str) {
    let xsd = wrap(body);
    let err = Schema::compile(xsd.as_bytes()).expect_err("must be rejected");
    assert!(
        err.findings
            .iter()
            .any(|f| f.message.contains(expect_in_message)),
        "expected a finding mentioning '{expect_in_message}', got: {err}"
    );
}

#[test]
fn import_and_include_are_rejected() {
    assert_rejected(
        r#"<xs:import namespace="urn:x" schemaLocation="x.xsd"/>"#,
        "xs:import",
    );
    assert_rejected(r#"<xs:include schemaLocation="x.xsd"/>"#, "xs:include");
    assert_rejected(r#"<xs:redefine schemaLocation="x.xsd"/>"#, "xs:redefine");
    assert_rejected(r#"<xs:override schemaLocation="x.xsd"/>"#, "xs:override");
}

#[test]
fn groups_and_all_are_rejected() {
    assert_rejected(
        r#"<xs:group name="g"><xs:sequence/></xs:group>"#,
        "xs:group",
    );
    assert_rejected(r#"<xs:attributeGroup name="g"/>"#, "xs:attributeGroup");
    assert_rejected(
        r#"<xs:complexType name="T"><xs:all><xs:element name="a" type="xs:string"/></xs:all></xs:complexType>"#,
        "xs:all",
    );
}

#[test]
fn list_union_and_identity_constraints_are_rejected() {
    assert_rejected(
        r#"<xs:simpleType name="S"><xs:list itemType="xs:string"/></xs:simpleType>"#,
        "xs:list",
    );
    assert_rejected(
        r#"<xs:simpleType name="S"><xs:union memberTypes="xs:string xs:decimal"/></xs:simpleType>"#,
        "xs:union",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string"><xs:unique name="u"><xs:selector xpath="x"/><xs:field xpath="y"/></xs:unique></xs:element>"#,
        "xs:unique",
    );
    assert_rejected(r#"<xs:notation name="n" public="p"/>"#, "xs:notation");
}

#[test]
fn element_modifiers_are_rejected() {
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" nillable="true"/>"#,
        "nillable",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" fixed="x"/>"#,
        "fixed",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" default="x"/>"#,
        "default",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" block="extension"/>"#,
        "block",
    );
    assert_rejected(
        "<xs:element name=\"E\" type=\"xs:string\" final=\"#all\"/>",
        "final",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" abstract="true"/>"#,
        "abstract",
    );
    assert_rejected(
        r#"<xs:element name="E" type="xs:string" substitutionGroup="F"/>"#,
        "substitutionGroup",
    );
    assert_rejected(
        r#"<xs:complexType name="T"><xs:sequence><xs:element ref="E"/></xs:sequence></xs:complexType>"#,
        "ref",
    );
    assert_rejected(
        r#"<xs:complexType name="T"><xs:sequence><xs:element name="e" type="xs:string" form="unqualified"/></xs:sequence></xs:complexType>"#,
        "form",
    );
}

#[test]
fn complex_content_derivation_and_mixed_are_rejected() {
    assert_rejected(
        r#"<xs:complexType name="T"><xs:complexContent><xs:extension base="B"/></xs:complexContent></xs:complexType>"#,
        "xs:complexContent",
    );
    assert_rejected(
        r#"<xs:complexType name="T" mixed="true"><xs:sequence/></xs:complexType>"#,
        "mixed",
    );
    assert_rejected(
        r#"<xs:complexType name="T" abstract="true"><xs:sequence/></xs:complexType>"#,
        "abstract",
    );
}

#[test]
fn length_facet_compiles_and_enforces_exact_length() {
    // xs:length (exact Unicode scalar count) is in the subset: real message definitions use it
    // for fixed-width code fields where others express the same constraint as a min/max pair.
    let xsd = wrap(
        r#"<xs:element name="E" type="S"/>
<xs:simpleType name="S"><xs:restriction base="xs:string"><xs:length value="3"/></xs:restriction></xs:simpleType>"#,
    );
    let schema = Schema::compile(xsd.as_bytes()).expect("xs:length is in the subset");
    let ok = schema
        .validate(br#"<E xmlns="urn:t">abc</E>"#)
        .expect("well-formed");
    assert!(ok.is_empty(), "exact-length value passes: {ok:#?}");
    for bad in [
        &br#"<E xmlns="urn:t">ab</E>"#[..],
        br#"<E xmlns="urn:t">abcd</E>"#,
    ] {
        let violations = schema.validate(bad).expect("well-formed");
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("required length 3")),
            "off-length value violates xs:length: {violations:#?}"
        );
    }
    // Still rejected on a numeric base, like the other length facets.
    assert_rejected(
        r#"<xs:simpleType name="S"><xs:restriction base="xs:decimal"><xs:length value="3"/></xs:restriction></xs:simpleType>"#,
        "length facets on a numeric base",
    );
}

#[test]
fn excluded_facets_are_rejected() {
    // `xs:whiteSpace` is the only string-base facet outside the subset (`xs:length` is in).
    assert_rejected(
        r#"<xs:simpleType name="S"><xs:restriction base="xs:string"><xs:whiteSpace value="collapse"/></xs:restriction></xs:simpleType>"#,
        "whiteSpace",
    );
    for (facet, snippet) in [
        ("minExclusive", r#"<xs:minExclusive value="0"/>"#),
        ("maxExclusive", r#"<xs:maxExclusive value="9"/>"#),
    ] {
        assert_rejected(
            &format!(
                r#"<xs:simpleType name="S"><xs:restriction base="xs:decimal">{snippet}</xs:restriction></xs:simpleType>"#
            ),
            facet,
        );
    }
}

#[test]
fn wildcard_and_attribute_restrictions() {
    assert_rejected(
        "<xs:complexType name=\"T\"><xs:sequence><xs:any namespace=\"##any\" processContents=\"strict\"/></xs:sequence></xs:complexType>",
        "processContents",
    );
    assert_rejected(
        "<xs:complexType name=\"T\"><xs:sequence><xs:any namespace=\"##any\"/></xs:sequence></xs:complexType>",
        "processContents",
    );
    assert_rejected(
        r#"<xs:complexType name="T"><xs:anyAttribute/></xs:complexType>"#,
        "xs:anyAttribute",
    );
    assert_rejected(
        r#"<xs:complexType name="T"><xs:sequence/><xs:attribute name="a" type="xs:string" default="x"/></xs:complexType>"#,
        "default",
    );
    assert_rejected(
        r#"<xs:complexType name="T"><xs:sequence/><xs:attribute name="a" type="xs:string" fixed="x"/></xs:complexType>"#,
        "fixed",
    );
}

#[test]
fn unsupported_builtins_and_untyped_declarations_are_rejected() {
    assert_rejected(r#"<xs:element name="E" type="xs:QName"/>"#, "xs:QName");
    assert_rejected(r#"<xs:element name="E" type="xs:ID"/>"#, "xs:ID");
    assert_rejected(r#"<xs:element name="E" type="xs:anyURI"/>"#, "xs:anyURI");
    assert_rejected(r#"<xs:element name="E" type="xs:double"/>"#, "xs:double");
    assert_rejected(r#"<xs:element name="E"/>"#, "untyped element");
    assert_rejected(
        r#"<xs:element name="E" type="Missing"/>"#,
        "undeclared type",
    );
}

#[test]
fn schema_level_requirements() {
    let no_tns = r#"<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
<xs:element name="E" type="xs:string"/>
</xs:schema>"#;
    let err = Schema::compile(no_tns.as_bytes()).expect_err("no targetNamespace");
    assert!(err
        .findings
        .iter()
        .any(|f| f.message.contains("targetNamespace")));

    let unqualified = r#"<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:t" xmlns="urn:t">
<xs:element name="E" type="xs:string"/>
</xs:schema>"#;
    let err = Schema::compile(unqualified.as_bytes()).expect_err("must require qualified");
    assert!(err
        .findings
        .iter()
        .any(|f| f.message.contains("elementFormDefault")));
}

#[test]
fn findings_are_collected_not_first_failure_only() {
    let body = r#"
<xs:element name="E" type="xs:string" nillable="true"/>
<xs:simpleType name="S"><xs:list itemType="xs:string"/></xs:simpleType>
<xs:complexType name="T"><xs:all/></xs:complexType>"#;
    let err = Schema::compile(wrap(body).as_bytes()).expect_err("rejected");
    assert!(err.findings.len() >= 3, "collect-all findings: {err}");
    // Findings carry line:col positions.
    assert!(err
        .findings
        .iter()
        .all(|f| f.pos.line > 0 && f.pos.column > 0));
}
