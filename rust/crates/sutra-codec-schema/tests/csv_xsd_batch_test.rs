//! R2 — a CSV batch validated ROW-WISE against an XSD, every row and every cell, in one decode
//! before any process runs. The facets do real work on cells (pattern, enumeration, xs:dateTime,
//! numeric range), each violation names its row, and a bad row does not stop the rest being
//! reported. See design `schema-format-binding.md`.

use sutra_codec_schema::StructuralCodec;
use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};

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
  <xs:simpleType name="RateCode">
    <xs:restriction base="xs:string">
      <xs:enumeration value="PEAK"/><xs:enumeration value="OFFPEAK"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="CallDetailRecord">
    <xs:complexType><xs:sequence>
      <xs:element name="recordId"    type="xs:string"/>
      <xs:element name="msisdn"      type="Msisdn"/>
      <xs:element name="startTime"   type="xs:dateTime"/>
      <xs:element name="durationSec" type="DurationSeconds"/>
      <xs:element name="direction"   type="Direction"/>
      <xs:element name="rateCode"    type="RateCode" minOccurs="0"/>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>
"#;

fn codec() -> StructuralCodec {
    StructuralCodec::compile_with_formats("urn:cdr", &[CDR_XSD.as_bytes()], &["csv"])
        .expect("compiles")
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

const HEADER: &str = "recordId,msisdn,startTime,durationSec,direction,rateCode\n";

#[test]
fn every_row_and_cell_is_validated_and_typed_in_one_decode() {
    let body = format!(
        "{HEADER}\
         CDR-1,+14155550101,2026-09-06T09:14:02Z,182,originated,PEAK\n\
         CDR-2,+14155550102,2026-09-06T09:31:47Z,45,received,OFFPEAK\n"
    );
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

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
    // The XSD's leaf types are applied to the untyped CSV cells: an xs:int is a NUMBER, not "182".
    assert_eq!(rows[0]["durationSec"], 182);
    assert!(rows[0]["durationSec"].is_number());
}

#[test]
fn a_violation_in_any_cell_names_its_row_and_the_batch_stays_routable() {
    let body = format!(
        "{HEADER}\
         CDR-1,+14155550101,2026-09-06T09:14:02Z,182,originated,PEAK\n\
         CDR-2,4155550999,2026-09-06T09:31:47Z,45,originated,PEAK\n\
         CDR-3,+14155550103,not-a-timestamp,45,received,PEAK\n\
         CDR-4,+14155550104,2026-09-06T10:02:19Z,930,sideways,PEAK\n\
         CDR-5,+14155550105,2026-09-06T11:00:00Z,930,received,PEAK\n"
    );
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

    // SOFT_ERRORS, not FATAL: the payload still projects so <q:onValidation> can decide.
    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert_eq!(rows(&result).len(), 5, "every row still projects");

    let paths: Vec<&str> = result.issues.iter().map(|i| i.path.as_str()).collect();
    // Rows are 0-based; rows 1, 2 and 3 are the bad ones and EACH is reported.
    assert!(
        paths.iter().any(|p| p.starts_with("value[1]")),
        "the pattern violation must name its row; paths: {paths:?}"
    );
    assert!(
        paths.iter().any(|p| p.starts_with("value[2]")),
        "the xs:dateTime violation must name its row; paths: {paths:?}"
    );
    assert!(
        paths.iter().any(|p| p.starts_with("value[3]")),
        "the enumeration violation must name its row; paths: {paths:?}"
    );
    assert!(
        !paths
            .iter()
            .any(|p| p.starts_with("value[0]") || p.starts_with("value[4]")),
        "the good rows must not be reported; paths: {paths:?}"
    );
}

#[test]
fn a_numeric_range_facet_is_enforced_on_a_cell() {
    let body = format!("{HEADER}CDR-1,+14155550101,2026-09-06T09:14:02Z,99999,originated,PEAK\n");
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert!(
        result.issues.iter().any(|i| i.path.starts_with("value[0]")),
        "maxInclusive must reject 99999; issues: {:?}",
        result.issues
    );
}

#[test]
fn an_unparseable_file_is_fatal_but_a_bad_row_is_not() {
    let result = codec().decode(b"", Some("text/csv"));
    assert_eq!(
        result.outcome,
        DecodeOutcome::Fatal,
        "an empty file has no rows"
    );
}

#[test]
fn a_csv_codec_accepts_text_csv() {
    let accepted = codec().accepted_content_types();
    assert!(
        accepted.iter().any(|c| c == "text/csv"),
        "a csv-bound schema codec must admit text/csv; got {accepted:?}"
    );
}

#[test]
fn declaring_csv_alongside_xml_still_reads_an_xml_body_as_one_document() {
    let codec =
        StructuralCodec::compile_with_formats("urn:cdr", &[CDR_XSD.as_bytes()], &["xml", "csv"])
            .expect("compiles");
    let xml = r#"<CallDetailRecord xmlns="urn:sutra:cdr"><recordId>CDR-1</recordId>
        <msisdn>+14155550101</msisdn><startTime>2026-09-06T09:14:02Z</startTime>
        <durationSec>182</durationSec><direction>originated</direction>
        <rateCode>PEAK</rateCode></CallDetailRecord>"#;

    let result = codec.decode(xml.as_bytes(), Some("application/xml"));
    assert_eq!(
        result.outcome,
        DecodeOutcome::Ok,
        "issues: {:?}",
        result.issues
    );
    let CodecValue::Json(serde_json::Value::Object(m)) = result.payload.expect("payload") else {
        panic!("map payload");
    };
    assert!(
        !m.contains_key("value"),
        "a document must not be wrapped as a batch"
    );
    assert_eq!(m["recordId"], "CDR-1");
}

#[test]
fn an_empty_cell_reads_as_absent_for_an_optional_element() {
    // A tabular row has a cell for EVERY column whether or not it carries a value. If an empty
    // cell became `<rateCode></rateCode>`, the enumeration would reject `''` and minOccurs="0"
    // would be unusable with any tabular format — every optional column would have to be
    // populated in every row. Found by decoding the call-log-load example's own sample.
    let body = format!("{HEADER}CDR-1,+14155550101,2026-09-06T09:14:02Z,182,originated,\n");
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

    assert_eq!(
        result.outcome,
        DecodeOutcome::Ok,
        "an empty OPTIONAL cell is absence, not a facet violation; issues: {:?}",
        result.issues
    );
}

#[test]
fn an_empty_cell_for_a_required_element_is_still_a_violation() {
    // The counterpart: absence is only tolerated where the type permits it. An empty required
    // cell is a genuine data error and must still be reported.
    let body = format!("{HEADER}CDR-1,,2026-09-06T09:14:02Z,182,originated,PEAK\n");
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert!(
        result.issues.iter().any(|i| i.path == "value[0]"),
        "issues: {:?}",
        result.issues
    );
}

#[test]
fn an_issue_path_names_the_row_and_never_an_xml_offset() {
    // The XSD validator reports a position into the one-line fragment this codec synthesised;
    // `line 1:362` means nothing to whoever sent the file, so the row index stands alone.
    let body = format!("{HEADER}CDR-1,4155550999,2026-09-06T09:14:02Z,182,originated,PEAK\n");
    let result = codec().decode(body.as_bytes(), Some("text/csv"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    for issue in &result.issues {
        assert!(
            !issue.path.contains("line "),
            "a synthesised-XML offset must not leak into the path: {:?}",
            issue
        );
        assert!(issue.path.starts_with("value[0]"), "{:?}", issue);
    }
}
