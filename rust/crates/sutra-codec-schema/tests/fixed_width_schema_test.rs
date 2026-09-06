//! A FIXED-WIDTH batch validated row-wise against an XSD — the same contract the csv batch gets
//! (`csv_xsd_batch_test`), reached through a manifest-declared column layout instead of a header
//! line. Plus the check csv cannot have: because a fixed-width layout is CONFIGURATION, it can be
//! verified against the bound schema at package time.

use std::sync::Arc;

use sutra_codec_schema::StructuralCodec;
use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};
use sutra_formats::{FixedWidthCodec, FixedWidthField};

const CDR_XSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:sutra:cdr" xmlns="urn:sutra:cdr" elementFormDefault="qualified">
  <xs:simpleType name="Msisdn">
    <xs:restriction base="xs:string"><xs:pattern value="\+[0-9]{8,15}"/></xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Direction">
    <xs:restriction base="xs:string">
      <xs:enumeration value="originated"/><xs:enumeration value="received"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="DurationSeconds">
    <xs:restriction base="xs:int">
      <xs:minInclusive value="0"/><xs:maxInclusive value="86400"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="CallDetailRecord">
    <xs:complexType><xs:sequence>
      <xs:element name="recordId"    type="xs:string"/>
      <xs:element name="msisdn"      type="Msisdn"/>
      <xs:element name="durationSec" type="DurationSeconds"/>
      <xs:element name="direction"   type="Direction"/>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>
"#;

fn field(name: &str, width: usize) -> FixedWidthField {
    FixedWidthField::new(name, width).expect("valid field")
}

fn layout() -> Vec<FixedWidthField> {
    vec![
        field("recordId", 10),
        field("msisdn", 15),
        field("durationSec", 6),
        field("direction", 11),
    ]
}

fn codec_with(layout: Vec<FixedWidthField>) -> Result<StructuralCodec, String> {
    let columns: Vec<String> = layout.iter().map(|f| f.name().to_string()).collect();
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    let parser: Arc<dyn PayloadCodec> = Arc::new(FixedWidthCodec::new(layout)?);
    StructuralCodec::compile_with_layout(
        "urn:cdr",
        &[CDR_XSD.as_bytes()],
        &["fixed-width"],
        vec![parser],
        &columns,
    )
    // The two refusal kinds are distinguished by type now; these tests only assert the message.
    .map_err(|e| e.to_string())
}

fn codec() -> StructuralCodec {
    codec_with(layout()).expect("compiles")
}

fn line(record_id: &str, msisdn: &str, duration: &str, direction: &str) -> String {
    format!("{record_id:<10}{msisdn:<15}{duration:<6}{direction:<11}")
}

fn rows(result: &sutra_codec_spi::DecodeResult) -> &Vec<serde_json::Value> {
    match result.payload.as_ref().expect("payload") {
        CodecValue::Json(serde_json::Value::Object(m)) => match &m["value"] {
            serde_json::Value::Array(rows) => rows,
            other => panic!("value should be an array, got {other:?}"),
        },
        other => panic!("expected the batch object, got {other:?}"),
    }
}

#[test]
fn every_record_and_column_is_validated_and_typed_in_one_decode() {
    let body = format!(
        "{}\n{}\n",
        line("CDR-1", "+14155550101", "182", "originated"),
        line("CDR-2", "+14155550102", "45", "received")
    );
    let result = codec().decode(body.as_bytes(), Some("text/plain"));

    assert_eq!(
        result.outcome,
        DecodeOutcome::Ok,
        "issues: {:?}",
        result.issues
    );
    assert_eq!(result.message_type.as_deref(), Some("CallDetailRecord"));
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["recordId"], "CDR-1");
    // The XSD's leaf types are applied to the untyped columns: xs:int is a NUMBER, not "182".
    assert_eq!(rows[0]["durationSec"], 182);
    assert!(rows[0]["durationSec"].is_number());
}

#[test]
fn a_violation_in_any_column_names_its_record_and_the_batch_stays_routable() {
    let body = format!(
        "{}\n{}\n{}\n{}\n",
        line("CDR-1", "+14155550101", "182", "originated"),
        line("CDR-2", "4155550999", "45", "originated"), // pattern
        line("CDR-3", "+14155550103", "99999", "received"), // maxInclusive
        line("CDR-4", "+14155550104", "45", "sideways")  // enumeration
    );
    let result = codec().decode(body.as_bytes(), Some("text/plain"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert_eq!(rows(&result).len(), 4, "every record still projects");

    let paths: Vec<&str> = result.issues.iter().map(|i| i.path.as_str()).collect();
    for bad in ["value[1]", "value[2]", "value[3]"] {
        assert!(
            paths.iter().any(|p| p.starts_with(bad)),
            "{bad} must be reported; paths: {paths:?}"
        );
    }
    assert!(
        !paths.iter().any(|p| p.starts_with("value[0]")),
        "the good record must not be reported; paths: {paths:?}"
    );
}

#[test]
fn a_fixed_width_codec_accepts_its_own_content_types() {
    let accepted = codec().accepted_content_types();
    assert!(accepted.iter().any(|c| c == "text/plain"), "{accepted:?}");
    assert!(
        accepted.iter().any(|c| c == "application/x-fixed-width"),
        "{accepted:?}"
    );
}

// ---- the check csv cannot have: the layout IS configuration, so verify it against the schema --

#[test]
fn a_column_the_type_does_not_declare_is_refused_at_compile() {
    let mut bad = layout();
    bad.push(field("cellId", 8)); // not in CallDetailRecord
    let Err(err) = codec_with(bad) else {
        panic!("an undeclared column must not compile");
    };
    assert!(err.contains("cellId"), "{err}");
    assert!(
        err.contains("does not declare"),
        "the error should say why: {err}"
    );
}

#[test]
fn a_required_element_with_no_column_is_refused_at_compile() {
    // Drop `direction`, which the type requires — no record could ever validate.
    let bad: Vec<FixedWidthField> = layout().into_iter().take(3).collect();
    let Err(err) = codec_with(bad) else {
        panic!("a required element with no column must not compile");
    };
    assert!(err.contains("direction"), "{err}");
    assert!(err.contains("no column"), "{err}");
}

#[test]
fn a_layout_matching_the_type_compiles() {
    assert!(
        codec_with(layout()).is_ok(),
        "the declared columns are exactly the type's elements"
    );
}

#[test]
fn declaring_fixed_width_without_its_layout_fails_closed() {
    // The layout-less constructor cannot serve fixed-width: there is no default to fall back on.
    let Err(err) =
        StructuralCodec::compile_with_formats("urn:cdr", &[CDR_XSD.as_bytes()], &["fixed-width"])
    else {
        panic!("fixed-width has no zero-config form");
    };
    assert!(err.contains("no default layout"), "{err}");
}
