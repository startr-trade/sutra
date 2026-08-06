//! Declared-field enumeration ([`Schema::fields_of`]) — the precise, one-type-deep back-end
//! column typing is derived from.
//!
//! Driven by inline schemas rather than the shared fixtures: each case pins ONE declaration
//! shape (an occurrence bound, a restriction chain, a group nesting) so a failure names the
//! rule it broke. The `tests/data/schemas/` fixtures exercise breadth; this file exercises
//! precision.

use sutra_xsd::{Builtin, FieldDecl, FieldShape, Schema, TEXT_CONTENT_FIELD, WILDCARD_FIELD};

/// Wrap a schema body in the Tier-1 preamble.
fn compile(body: &str) -> Schema {
    let source = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns="urn:sutra:test:fields"
           targetNamespace="urn:sutra:test:fields"
           elementFormDefault="qualified">
{body}
</xs:schema>
"#
    );
    Schema::compile(source.as_bytes()).unwrap_or_else(|e| panic!("must compile:\n{e}\n{source}"))
}

fn field<'f>(fields: &'f [FieldDecl], name: &str) -> &'f FieldDecl {
    fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field '{name}' in {:?}", names(fields)))
}

fn names(fields: &[FieldDecl]) -> Vec<&str> {
    fields.iter().map(|f| f.name.as_str()).collect()
}

/// One expected declaration, asserted field by field.
struct Row {
    name: &'static str,
    is_attribute: bool,
    occurs_min: u32,
    occurs_max: Option<u32>,
    in_choice: bool,
}

impl Row {
    fn new(
        name: &'static str,
        is_attribute: bool,
        occurs_min: u32,
        occurs_max: Option<u32>,
        in_choice: bool,
    ) -> Row {
        Row {
            name,
            is_attribute,
            occurs_min,
            occurs_max,
            in_choice,
        }
    }
}

/// The two symbol spaces a declaration may name — a global element root and a named type —
/// both resolve, roots first; an unknown name is `None`, and a name with nothing to enumerate
/// is an empty list rather than `None`.
#[test]
fn name_resolution_covers_roots_types_and_the_two_empty_cases() {
    let schema = compile(
        r#"
  <xs:element name="Record" type="RecordType"/>
  <xs:element name="Scalar" type="xs:string"/>
  <xs:complexType name="RecordType">
    <xs:sequence>
      <xs:element name="id" type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="EmptyType"/>
  <xs:simpleType name="Code">
    <xs:restriction base="xs:string"/>
  </xs:simpleType>
"#,
    );

    // A root resolves through its type reference.
    assert_eq!(names(&schema.fields_of("Record").unwrap()), ["id"]);
    // A named complex type resolves directly.
    assert_eq!(names(&schema.fields_of("RecordType").unwrap()), ["id"]);
    // Nothing to enumerate — distinct from "unknown".
    assert_eq!(schema.fields_of("Scalar"), Some(Vec::new()));
    assert_eq!(schema.fields_of("EmptyType"), Some(Vec::new()));
    assert_eq!(schema.fields_of("Code"), Some(Vec::new()));
    // Unknown in both symbol spaces.
    assert_eq!(schema.fields_of("NoSuchThing"), None);
}

/// Declared order is the contract: attributes first (declaration order), then the content
/// model's particles (declaration order, inner groups flattened in place).
#[test]
fn order_is_attributes_then_declared_particles_with_groups_flattened() {
    let schema = compile(
        r#"
  <xs:complexType name="Ordered">
    <xs:sequence>
      <xs:element name="first" type="xs:string"/>
      <xs:sequence>
        <xs:element name="nestedA" type="xs:string"/>
        <xs:element name="nestedB" type="xs:string"/>
      </xs:sequence>
      <xs:element name="last" type="xs:string"/>
    </xs:sequence>
    <xs:attribute name="attrOne" type="xs:string"/>
    <xs:attribute name="attrTwo" type="xs:string"/>
  </xs:complexType>
"#,
    );
    let fields = schema.fields_of("Ordered").unwrap();
    assert_eq!(
        names(&fields),
        ["attrOne", "attrTwo", "first", "nestedA", "nestedB", "last"]
    );
    assert!(fields[0].is_attribute);
    assert!(!fields[2].is_attribute);
    // Re-derivation is stable.
    assert_eq!(schema.fields_of("Ordered").unwrap(), fields);
}

/// Every shape/occurrence/choice combination the flatness rule discriminates on, in one type,
/// asserted field by field.
#[test]
fn shapes_occurrences_and_choice_membership() {
    let schema = compile(
        r#"
  <xs:complexType name="Mixed">
    <xs:sequence>
      <xs:element name="scalarRequired" type="xs:string"/>
      <xs:element name="scalarOptional" type="xs:string" minOccurs="0"/>
      <xs:element name="scalarBounded" type="xs:string" maxOccurs="4"/>
      <xs:element name="scalarUnbounded" type="xs:string" maxOccurs="unbounded"/>
      <xs:element name="nested" type="Inner"/>
      <xs:choice>
        <xs:element name="branchA" type="xs:string"/>
        <xs:element name="branchB" type="xs:decimal"/>
      </xs:choice>
      <xs:any processContents="lax" minOccurs="0"/>
    </xs:sequence>
    <xs:attribute name="attrRequired" type="xs:string" use="required"/>
    <xs:attribute name="attrOptional" type="xs:string"/>
  </xs:complexType>
  <xs:complexType name="Inner">
    <xs:sequence>
      <xs:element name="deep" type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
"#,
    );
    let fields = schema.fields_of("Mixed").unwrap();

    let expected = [
        Row::new("attrRequired", true, 1, Some(1), false),
        Row::new("attrOptional", true, 0, Some(1), false),
        Row::new("scalarRequired", false, 1, Some(1), false),
        Row::new("scalarOptional", false, 0, Some(1), false),
        Row::new("scalarBounded", false, 1, Some(4), false),
        Row::new("scalarUnbounded", false, 1, None, false),
        Row::new("nested", false, 1, Some(1), false),
        Row::new("branchA", false, 1, Some(1), true),
        Row::new("branchB", false, 1, Some(1), true),
        Row::new(WILDCARD_FIELD, false, 0, Some(1), false),
    ];
    assert_eq!(
        names(&fields),
        expected.iter().map(|e| e.name).collect::<Vec<_>>()
    );
    for row in expected {
        let f = field(&fields, row.name);
        let name = row.name;
        assert_eq!(f.is_attribute, row.is_attribute, "{name}.is_attribute");
        assert_eq!(f.occurs_min, row.occurs_min, "{name}.occurs_min");
        assert_eq!(f.occurs_max, row.occurs_max, "{name}.occurs_max");
        assert_eq!(f.in_choice, row.in_choice, "{name}.in_choice");
        // The two derived predicates the flatness rule actually consults.
        assert_eq!(
            f.is_repeated(),
            !matches!(row.occurs_max, Some(0 | 1)),
            "{name}.is_repeated"
        );
        assert_eq!(
            f.is_optional(),
            row.occurs_min == 0 || row.in_choice,
            "{name}.is_optional"
        );
    }

    // Shapes: scalars carry their builtin, a complex child is never descended, a wildcard is
    // open content.
    assert_eq!(
        field(&fields, "scalarRequired").scalar().map(|(b, _)| b),
        Some(Builtin::String)
    );
    assert_eq!(
        field(&fields, "branchB").scalar().map(|(b, _)| b),
        Some(Builtin::Decimal)
    );
    assert_eq!(field(&fields, "nested").shape, FieldShape::Complex);
    assert!(field(&fields, "nested").scalar().is_none());
    assert_eq!(field(&fields, WILDCARD_FIELD).shape, FieldShape::Any);
    // One level deep: the nested type's own child is NOT in this list.
    assert!(!names(&fields).contains(&"deep"));
}

/// A repeatable inner group multiplies through to its members: the flatness rule must see the
/// EFFECTIVE bound, not the element's own `maxOccurs="1"`.
#[test]
fn enclosing_group_occurrences_multiply_through() {
    let schema = compile(
        r#"
  <xs:complexType name="Repeating">
    <xs:sequence>
      <xs:sequence maxOccurs="3">
        <xs:element name="boundedByGroup" type="xs:string"/>
      </xs:sequence>
      <xs:sequence maxOccurs="unbounded">
        <xs:element name="unboundedByGroup" type="xs:string"/>
      </xs:sequence>
      <xs:sequence minOccurs="0">
        <xs:element name="optionalByGroup" type="xs:string"/>
      </xs:sequence>
      <xs:choice minOccurs="0">
        <xs:sequence>
          <xs:element name="deepInChoice" type="xs:string"/>
        </xs:sequence>
      </xs:choice>
    </xs:sequence>
  </xs:complexType>
"#,
    );
    let fields = schema.fields_of("Repeating").unwrap();

    let bounded = field(&fields, "boundedByGroup");
    assert_eq!(bounded.occurs_max, Some(3));
    assert!(bounded.is_repeated());

    let unbounded = field(&fields, "unboundedByGroup");
    assert_eq!(unbounded.occurs_max, None);
    assert!(unbounded.is_repeated());

    let optional = field(&fields, "optionalByGroup");
    assert_eq!(optional.occurs_min, 0);
    assert!(optional.is_optional() && !optional.is_repeated());

    // Choice membership propagates to every depth inside it.
    let deep = field(&fields, "deepInChoice");
    assert!(deep.in_choice && deep.is_optional());
}

/// `simpleContent` is text PLUS attributes: the attributes enumerate, and the unnamed
/// character content reports as a named, non-scalar child rather than vanishing.
#[test]
fn simple_content_reports_its_text_as_a_named_non_scalar_child() {
    let schema = compile(
        r#"
  <xs:complexType name="Amount">
    <xs:simpleContent>
      <xs:extension base="xs:decimal">
        <xs:attribute name="currency" type="xs:string" use="required"/>
      </xs:extension>
    </xs:simpleContent>
  </xs:complexType>
"#,
    );
    let fields = schema.fields_of("Amount").unwrap();
    assert_eq!(names(&fields), ["currency", TEXT_CONTENT_FIELD]);
    assert!(field(&fields, "currency").scalar().is_some());
    assert_eq!(
        field(&fields, TEXT_CONTENT_FIELD).shape,
        FieldShape::Complex
    );
}

/// Facets accumulate down the WHOLE restriction chain, narrowest per kind — the property a
/// column type has to hold. Asserted over a three-level chain plus a contradictory one.
#[test]
fn facets_are_effective_down_a_restriction_chain() {
    let schema = compile(
        r#"
  <xs:simpleType name="Text70">
    <xs:restriction base="xs:string">
      <xs:minLength value="1"/>
      <xs:maxLength value="70"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Text35">
    <xs:restriction base="Text70">
      <xs:maxLength value="35"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Text16">
    <xs:restriction base="Text35">
      <xs:minLength value="4"/>
      <xs:maxLength value="16"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Amount18">
    <xs:restriction base="xs:decimal">
      <xs:totalDigits value="18"/>
      <xs:fractionDigits value="5"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Amount13">
    <xs:restriction base="Amount18">
      <xs:totalDigits value="13"/>
      <xs:fractionDigits value="2"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Status">
    <xs:restriction base="xs:string">
      <xs:enumeration value="NEW"/>
      <xs:enumeration value="HELD"/>
      <xs:enumeration value="DONE"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="OpenStatus">
    <xs:restriction base="Status">
      <xs:enumeration value="HELD"/>
      <xs:enumeration value="NEW"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Widening">
    <xs:restriction base="Text16">
      <xs:maxLength value="140"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Fixed8">
    <xs:restriction base="xs:string">
      <xs:length value="8"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:complexType name="Chained">
    <xs:sequence>
      <xs:element name="text" type="Text16"/>
      <xs:element name="amount" type="Amount13"/>
      <xs:element name="status" type="OpenStatus"/>
      <xs:element name="widened" type="Widening"/>
      <xs:element name="fixed" type="Fixed8"/>
      <xs:element name="plain" type="xs:string"/>
    </xs:sequence>
    <xs:attribute name="attrText" type="Text16"/>
  </xs:complexType>
"#,
    );
    let fields = schema.fields_of("Chained").unwrap();

    // Three levels: the narrowest ceiling and the LARGEST floor both survive.
    let (builtin, text) = field(&fields, "text").scalar().unwrap();
    assert_eq!(builtin, Builtin::String);
    assert_eq!(text.max_length, Some(16));
    assert_eq!(text.min_length, Some(4));
    assert_eq!(text.length, None);
    // An attribute's value type is chained identically.
    let (_, attr_text) = field(&fields, "attrText").scalar().unwrap();
    assert_eq!(attr_text.max_length, Some(16));
    assert_eq!(attr_text.min_length, Some(4));

    // Digit facets narrow the same way.
    let (builtin, amount) = field(&fields, "amount").scalar().unwrap();
    assert_eq!(builtin, Builtin::Decimal);
    assert_eq!(amount.total_digits, Some(13));
    assert_eq!(amount.fraction_digits, Some(2));

    // Enumerations intersect; a well-formed chain only narrows, and the reported order is the
    // narrowest step's own.
    let (_, status) = field(&fields, "status").scalar().unwrap();
    assert_eq!(
        status.enumeration.as_deref(),
        Some(["HELD".to_string(), "NEW".to_string()].as_slice())
    );

    // A chain that tries to WIDEN cannot: the base's ceiling still binds.
    let (_, widened) = field(&fields, "widened").scalar().unwrap();
    assert_eq!(widened.max_length, Some(16));

    // `length` is carried, and an unrestricted builtin carries nothing.
    let (_, fixed) = field(&fields, "fixed").scalar().unwrap();
    assert_eq!(fixed.length, Some(8));
    let (_, plain) = field(&fields, "plain").scalar().unwrap();
    assert_eq!(plain, &sutra_xsd::FieldFacets::default());
}

/// An inline (anonymous) type is enumerated exactly like a named one — the projection must not
/// depend on the author having named their types.
#[test]
fn inline_types_enumerate_like_named_ones() {
    let schema = compile(
        r#"
  <xs:element name="Inline">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="code">
          <xs:simpleType>
            <xs:restriction base="xs:string">
              <xs:maxLength value="3"/>
            </xs:restriction>
          </xs:simpleType>
        </xs:element>
        <xs:element name="nested">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="deep" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
"#,
    );
    let fields = schema.fields_of("Inline").unwrap();
    assert_eq!(names(&fields), ["code", "nested"]);
    let (_, code) = field(&fields, "code").scalar().unwrap();
    assert_eq!(code.max_length, Some(3));
    assert_eq!(field(&fields, "nested").shape, FieldShape::Complex);
}

/// The public accessors read the same declarations the module example schemas ship, so the
/// surface is exercised against real authored content, not only inline cases.
#[test]
fn a_module_schema_enumerates_its_declared_fields() {
    let schema = Schema::compile(&crate::support::repo_schema(
        "examples/money-transfer/deployments-src/default--money-transfer--1.0.0/schemas/transfer/transfer.xsd",
    ))
    .unwrap();
    let fields = schema.fields_of("TransferRequest").unwrap();
    assert_eq!(names(&fields), ["fromId", "toId", "amount"]);
    assert!(fields.iter().all(|f| f.scalar().is_some()));
    assert!(fields.iter().all(|f| !f.is_repeated()));
    assert_eq!(
        field(&fields, "amount").scalar().map(|(b, _)| b),
        Some(Builtin::Decimal)
    );
}
