//! The module XSD codec wired through the `sutra-xsd` subset validator
//! (`compile_with_formats`), including the distinctive cases: repeated element → list, and
//! invalid-XSD-rejected-at-compile.
//!
//! Fixture note: the `sutra-xsd` Tier-1 subset requires an explicit `targetNamespace` +
//! `elementFormDefault="qualified"` (the authoring contract). The fixtures are therefore
//! namespaced — behaviourally identical, just in a real ISO-20022-style namespace.
//!
//! Posture notes:
//! - the unified module codec resolves `messageType` from the document root (e.g.
//!   `Payment`) rather than stamping a separate schema-assigned type (`payment.v1`);
//! - `xs:decimal` leaves are coerced to numbers (the schema-type coercion step) rather than
//!   left as string leaves.

use sutra_codec_schema::StructuralCodec;
use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};

const ALL_FORMATS: &[&str] = &["xml", "json", "yaml"];

const XSD: &[u8] = br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Id" type="xs:string"/>
        <xs:element name="Amount" type="xs:decimal"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"#;

fn codec() -> StructuralCodec {
    StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:payments",
        &[XSD],
        ALL_FORMATS,
    )
    .expect("XSD compiles")
}

fn reply() -> CodecValue {
    CodecValue::Json(serde_json::json!({"Payment": {"Id": "INB-7", "Amount": "100"}}))
}

fn amount_is_100(m: &serde_json::Map<String, serde_json::Value>) {
    let amount = m.get("Amount").unwrap();
    assert!(
        amount.is_number(),
        "Amount coerced to number, got {amount:?}"
    );
    assert_eq!(amount.as_f64().unwrap(), 100.0);
}

#[test]
fn encode_round_trips_through_xml_json_yaml() {
    let codec = codec();
    for ct in ["application/xml", "application/json", "application/yaml"] {
        let bytes = codec
            .encode(&reply(), Some(ct))
            .unwrap_or_else(|e| panic!("encode {ct}: {e}"));
        let redecoded = codec.decode(&bytes, Some(ct));
        assert_eq!(redecoded.outcome, DecodeOutcome::Ok, "round-trip via {ct}");
        assert_eq!(redecoded.message_type.as_deref(), Some("Payment"));
        let CodecValue::Json(serde_json::Value::Object(p)) = redecoded.payload.unwrap() else {
            panic!("map payload");
        };
        assert_eq!(p.get("Id").unwrap(), "INB-7");
        amount_is_100(&p);
    }
}

#[test]
fn encode_non_conformant_reply_fails_with_outbound_encode_failed() {
    let codec = codec();
    let bad =
        CodecValue::Json(serde_json::json!({"Payment": {"Id": "INB-7", "Amount": "not-a-number"}}));
    let err = codec
        .encode(&bad, Some("application/xml"))
        .expect_err("non-conformant reply");
    assert!(err.contains("SUTRA.OUTBOUND.ENCODE_FAILED"), "got: {err}");
}

#[test]
fn xml_json_and_yaml_all_validate_against_the_same_xsd_and_resolve_the_same_type() {
    let codec = codec();
    let xml = codec.decode(
        br#"<Payment xmlns="urn:ex"><Id>INB-7</Id><Amount>100</Amount></Payment>"#,
        Some("application/xml"),
    );
    let json = codec.decode(
        br#"{"Payment":{"Id":"INB-7","Amount":100}}"#,
        Some("application/json"),
    );
    let yaml = codec.decode(
        b"Payment:\n  Id: INB-7\n  Amount: 100\n",
        Some("application/yaml"),
    );

    for r in [xml, json, yaml] {
        assert_eq!(r.outcome, DecodeOutcome::Ok);
        assert_eq!(r.message_type.as_deref(), Some("Payment"));
        let CodecValue::Json(serde_json::Value::Object(m)) = r.payload.unwrap() else {
            panic!("map payload");
        };
        assert_eq!(m.get("Id").unwrap(), "INB-7");
        amount_is_100(&m);
    }
}

#[test]
fn an_xsd_invalid_payload_is_soft_error_regardless_of_format() {
    // Missing the required <Amount> — XSD-invalid but structurally parsed; routable.
    let json = codec().decode(br#"{"Payment":{"Id":"INB-7"}}"#, Some("application/json"));

    assert_eq!(json.outcome, DecodeOutcome::SoftErrors);
    assert_eq!(json.message_type.as_deref(), Some("Payment"));
    let CodecValue::Json(serde_json::Value::Object(m)) = json.payload.clone().unwrap() else {
        panic!("map payload");
    };
    assert_eq!(m.get("Id").unwrap(), "INB-7");
    assert!(json
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.PARSE.XSD.SCHEMA_VIOLATION"));
}

#[test]
fn malformed_input_is_fatal() {
    let result = codec().decode(br#"{"Payment": "#, Some("application/json"));
    assert_eq!(result.outcome, DecodeOutcome::Fatal);
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.RUNTIME.CODEC.DECODE_FAILED"));
}

#[test]
fn formats_restrict_accepted_content_types() {
    let xml_only = StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:xmlonly",
        &[XSD],
        &["xml"],
    )
    .expect("compiles");
    let cts = xml_only.accepted_content_types();
    assert!(cts.iter().any(|c| c == "application/xml"));
    assert!(!cts.iter().any(|c| c == "application/json"));
}

const MULTI_XSD: &[u8] = br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
  <xs:element name="Reversal"><xs:complexType><xs:sequence>
    <xs:element name="OrigId" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

#[test]
fn declared_message_types_are_the_xsd_global_elements() {
    assert_eq!(codec().declared_message_types(), vec!["Payment"]);

    let multi = StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:multi",
        &[MULTI_XSD],
        ALL_FORMATS,
    )
    .expect("compiles");
    let mut types = multi.declared_message_types();
    types.sort();
    assert_eq!(types, vec!["Payment", "Reversal"]);

    let pay = multi.decode(br#"{"Payment":{"Id":"A"}}"#, Some("application/json"));
    let rev = multi.decode(br#"{"Reversal":{"OrigId":"A"}}"#, Some("application/json"));
    assert_eq!(pay.message_type.as_deref(), Some("Payment"));
    assert_eq!(rev.message_type.as_deref(), Some("Reversal"));
}

#[test]
fn multiple_xsd_files_compose_one_codec_across_namespaces() {
    let xsd_a: &[u8] = br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
               targetNamespace="urn:a" xmlns="urn:a" elementFormDefault="qualified">
      <xs:element name="Alpha"><xs:complexType><xs:sequence>
        <xs:element name="Id" type="xs:string"/>
      </xs:sequence></xs:complexType></xs:element>
    </xs:schema>"#;
    let xsd_b: &[u8] = br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
               targetNamespace="urn:b" xmlns="urn:b" elementFormDefault="qualified">
      <xs:element name="Beta"><xs:complexType><xs:sequence>
        <xs:element name="Ref" type="xs:string"/>
      </xs:sequence></xs:complexType></xs:element>
    </xs:schema>"#;
    let codec = StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:multi-file",
        &[xsd_a, xsd_b],
        ALL_FORMATS,
    )
    .expect("compiles");

    let mut types = codec.declared_message_types();
    types.sort();
    assert_eq!(types, vec!["Alpha", "Beta"]);

    let a = codec.decode(br#"{"Alpha":{"Id":"X"}}"#, Some("application/json"));
    let b = codec.decode(br#"{"Beta":{"Ref":"Y"}}"#, Some("application/json"));
    assert_eq!(a.outcome, DecodeOutcome::Ok);
    assert_eq!(a.message_type.as_deref(), Some("Alpha"));
    assert_eq!(b.outcome, DecodeOutcome::Ok);
    assert_eq!(b.message_type.as_deref(), Some("Beta"));
}

// ---- distinctive XSD schema coverage ----------------------------------------------------------

const REPEATING_XSD: &[u8] = br#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Id" type="xs:string"/>
        <xs:element name="Amount" type="xs:decimal"/>
        <xs:element name="Item" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence><xs:element name="Sku" type="xs:string"/></xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"#;

#[test]
fn valid_xml_decodes_to_typed_navigable_map_with_repeated_elements_as_a_list() {
    let codec = StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:payments",
        &[REPEATING_XSD],
        ALL_FORMATS,
    )
    .expect("compiles");
    let xml = br#"<Payment xmlns="urn:ex"><Id>INB-7</Id><Amount>1234.56</Amount><Item><Sku>A</Sku></Item><Item><Sku>B</Sku></Item></Payment>"#;
    let result = codec.decode(xml, Some("application/xml"));

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    assert_eq!(result.message_type.as_deref(), Some("Payment"));
    let CodecValue::Json(serde_json::Value::Object(m)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert_eq!(m.get("Id").unwrap(), "INB-7");
    // Repeated elements → list of nested maps (the N→List projection branch).
    let items = m.get("Item").unwrap().as_array().expect("Item is a list");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].get("Sku").unwrap(), "A");
    assert_eq!(items[1].get("Sku").unwrap(), "B");
}

#[test]
fn invalid_xsd_itself_fails_to_compile() {
    let err = StructuralCodec::compile_with_formats(
        "urn:sutra:module:demo:1.0.0:broken",
        &[b"<xs:schema><not-valid"],
        ALL_FORMATS,
    );
    assert!(err.is_err(), "an invalid XSD must not compile");
}
