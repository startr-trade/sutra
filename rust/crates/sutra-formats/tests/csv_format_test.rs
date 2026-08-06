//! Behavioural fixtures for the builtin `csv` format: RFC 4180 quoting, header vs
//! no-header layout, and the empty-input FATAL posture.

use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};
use sutra_formats::CsvCodec;

fn parse(codec: &CsvCodec, csv: &str) -> sutra_codec_spi::DecodeResult {
    codec.decode(csv.as_bytes(), Some("text/csv"))
}

fn rows(result: &sutra_codec_spi::DecodeResult) -> &Vec<serde_json::Value> {
    match result.payload.as_ref().expect("payload present") {
        CodecValue::Json(serde_json::Value::Array(rows)) => rows,
        other => panic!("expected a JSON array payload, got {other:?}"),
    }
}

#[test]
fn header_csv_parses_to_array_of_row_objects() {
    let result = parse(&CsvCodec::default(), "Id,Amt\nINB-7,100\nINB-8,200\n");

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let arr = rows(&result);
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["Id"], "INB-7");
    assert_eq!(arr[0]["Amt"], "100");
    assert_eq!(arr[1]["Id"], "INB-8");
}

#[test]
fn rfc4180_quoting_honours_embedded_delimiter_newline_and_escaped_quote() {
    // Row 1's "name" field carries a comma, a newline, and a doubled "" escape.
    let csv = "id,name\n1,\"Doe, John\nJr \"\"the boss\"\"\"\n";
    let result = parse(&CsvCodec::default(), csv);

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let arr = rows(&result);
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Doe, John\nJr \"the boss\"");
}

#[test]
fn no_header_mode_parses_to_array_of_row_arrays() {
    let result = parse(&CsvCodec::new(';', false), "a;b;c\nd;e;f\n");

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let arr = rows(&result);
    assert_eq!(arr.len(), 2);
    assert!(arr[0].is_array());
    assert_eq!(arr[0][1], "b");
    assert_eq!(arr[1][2], "f");
}

#[test]
fn empty_input_is_fatal_under_the_csv_parse_code() {
    // The code names the FORMAT that failed — a csv failure must not be reported as a
    // json one (its own family, alongside SUTRA.PARSE.{XML,JSON,YAML}.PARSE_ERROR).
    let result = parse(&CsvCodec::default(), "");
    assert_eq!(result.outcome, DecodeOutcome::Fatal);
    assert_eq!(
        result.issues[0].code,
        sutra_codec_spi::codes::PARSE_CSV_PARSE_ERROR
    );
    assert_eq!(result.issues[0].code, "SUTRA.PARSE.CSV.PARSE_ERROR");
}

#[test]
fn csv_rows_tree_is_schema_ready() {
    // The schema-composition half (binding the parsed tree to a JSON schema for typing and
    // message-type stamping) lives in the schema-codec layer, not this crate; this pins
    // the tree shape that layer consumes: one string-valued object per data row.
    let codec = CsvCodec::default();
    assert_eq!(codec.name(), "csv");
    assert_eq!(
        codec.accepted_content_types(),
        vec!["text/csv".to_string(), "application/csv".to_string()]
    );
    let result = parse(&codec, "Id,Amt\nINB-7,100\n");
    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let arr = rows(&result);
    assert_eq!(arr.len(), 1);
    assert!(arr[0].is_object());
    assert!(arr[0]["Id"].is_string());
    // Decode-only: replies are template renders, not row re-serialization.
    let err = codec
        .encode(result.payload.as_ref().unwrap(), Some("text/csv"))
        .unwrap_err();
    assert!(err.contains("does not support encode"), "{err}");
}
