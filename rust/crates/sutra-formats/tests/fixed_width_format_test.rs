//! Behavioural fixtures for the builtin `fixed-width` format: column slicing, short-line
//! tolerance, the encode inverse, and the configuration errors it refuses.

use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};
use sutra_formats::{FixedWidthCodec, FixedWidthField};

fn field(name: &str, width: usize) -> FixedWidthField {
    FixedWidthField::new(name, width).expect("valid field")
}

/// `recordId`(10) `msisdn`(15) `durationSec`(6)
fn codec() -> FixedWidthCodec {
    FixedWidthCodec::new(vec![
        field("recordId", 10),
        field("msisdn", 15),
        field("durationSec", 6),
    ])
    .expect("valid layout")
}

fn rows(result: &sutra_codec_spi::DecodeResult) -> &Vec<serde_json::Value> {
    match result.payload.as_ref().expect("payload") {
        CodecValue::Json(serde_json::Value::Array(rows)) => rows,
        other => panic!("expected an array payload, got {other:?}"),
    }
}

/// Build one wire line from the three column widths, so a hand-miscounted space in a test
/// fixture can never masquerade as a parser bug.
fn line(record_id: &str, msisdn: &str, duration: &str) -> String {
    format!("{record_id:<10}{msisdn:<15}{duration:<6}")
}

#[test]
fn each_line_becomes_one_record_keyed_by_the_layout() {
    let body = format!(
        "{}\n{}\n",
        line("CDR-1", "+14155550101", "182"),
        line("CDR-2", "+442071838750", "45")
    );
    let result = codec().decode(body.as_bytes(), Some("text/plain"));

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["recordId"], "CDR-1");
    assert_eq!(rows[0]["msisdn"], "+14155550101");
    assert_eq!(rows[0]["durationSec"], "182");
    assert_eq!(rows[1]["msisdn"], "+442071838750");
    // Values are STRINGS — faithful to the untyped wire form; typing is the schema's job.
    assert!(rows[0]["durationSec"].is_string());
}

#[test]
fn a_short_line_yields_empty_trailing_fields_rather_than_failing() {
    let result = codec().decode(b"CDR-9     +1415555", Some("text/plain"));
    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let rows = rows(&result);
    assert_eq!(rows[0]["recordId"], "CDR-9");
    assert_eq!(rows[0]["msisdn"], "+1415555");
    assert_eq!(rows[0]["durationSec"], "");
}

#[test]
fn blank_lines_and_a_trailing_newline_are_skipped() {
    let body = format!(
        "{}\n\n{}\n",
        line("CDR-1", "+14155550101", "182"),
        line("CDR-2", "+14155550102", "45")
    );
    let result = codec().decode(body.as_bytes(), Some("text/plain"));
    assert_eq!(
        rows(&result).len(),
        2,
        "no phantom record for the blank line"
    );
}

#[test]
fn an_empty_document_is_fatal() {
    let result = codec().decode(b"", Some("text/plain"));
    assert_eq!(result.outcome, DecodeOutcome::Fatal);
}

#[test]
fn encode_pads_every_column_and_decode_reads_it_back() {
    let codec = codec();
    let tree = serde_json::json!([
        {"recordId": "CDR-1", "msisdn": "+14155550101", "durationSec": "182"},
        {"recordId": "CDR-2", "msisdn": "+442071838750", "durationSec": "45"},
    ]);
    let bytes = codec
        .encode(&CodecValue::Json(tree.clone()), Some("text/plain"))
        .expect("encodes");
    let text = String::from_utf8(bytes).expect("utf-8");

    // Every line is exactly one record wide, so the columns line up.
    for line in text.lines() {
        assert_eq!(line.chars().count(), codec.record_width(), "line: {line:?}");
    }
    // The real assertion: this format's own parser reads back what it wrote.
    let CodecValue::Json(back) = codec
        .decode(text.as_bytes(), Some("text/plain"))
        .payload
        .expect("payload")
    else {
        panic!("json payload")
    };
    assert_eq!(back, tree);
}

#[test]
fn encode_accepts_the_array_root_value_wrapper() {
    let tree = serde_json::json!({"value": [{"recordId": "A", "msisdn": "B", "durationSec": "1"}]});
    let bytes = codec()
        .encode(&CodecValue::Json(tree), Some("text/plain"))
        .expect("encodes");
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        format!("{}\n", line("A", "B", "1"))
    );
}

#[test]
fn encode_refuses_an_overlong_value_rather_than_truncating_it() {
    // Truncating a fixed-width field would shift every column after it on that line — silent
    // corruption of the whole record, so it is an error.
    let err = codec()
        .encode(
            &CodecValue::Json(serde_json::json!([
                {"recordId": "THIS-ID-IS-FAR-TOO-LONG", "msisdn": "x", "durationSec": "1"}
            ])),
            Some("text/plain"),
        )
        .expect_err("an overlong value is not encodable");
    assert!(err.contains("recordId"), "{err}");
    assert!(err.contains("shift"), "the error should say why: {err}");
}

#[test]
fn a_layout_must_be_non_empty_named_and_positive_width() {
    assert!(FixedWidthCodec::new(vec![]).is_err(), "empty layout");
    assert!(FixedWidthField::new("", 4).is_err(), "blank name");
    assert!(FixedWidthField::new("id", 0).is_err(), "zero width");
}

#[test]
fn a_duplicate_column_name_is_refused() {
    let err = FixedWidthCodec::new(vec![field("id", 4), field("id", 6)])
        .expect_err("a duplicate column would be unreachable");
    assert!(err.contains("twice"), "{err}");
}
