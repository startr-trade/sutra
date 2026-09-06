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
    // The tree is also re-encodable, so a csv channel can answer in csv (see the encode block).
    let written = codec
        .encode(result.payload.as_ref().unwrap(), Some("text/csv"))
        .expect("rows re-encode");
    assert_eq!(String::from_utf8(written).unwrap(), "Amt,Id\n100,INB-7\n");
}

// ---- encode (the symmetric-reply / problem-table inverse) -----------------------------------

fn encode(codec: &CsvCodec, tree: serde_json::Value) -> String {
    String::from_utf8(
        codec
            .encode(&CodecValue::Json(tree), Some("text/csv"))
            .expect("encodes"),
    )
    .expect("utf-8")
}

#[test]
fn encode_round_trips_values_and_normalises_column_order() {
    let codec = CsvCodec::default();
    // Columns come back SORTED: a decoded row is a BTreeMap, so the source header's order is
    // already gone by the time encode sees it. Values are faithful; order is normalised.
    let decoded = parse(&codec, "Id,Amt\nINB-7,100\nINB-8,200\n");
    let CodecValue::Json(tree) = decoded.payload.expect("payload") else {
        panic!("json payload");
    };
    let written = encode(&codec, tree);
    assert_eq!(written, "Amt,Id\n100,INB-7\n200,INB-8\n");

    // The contract that DOES hold as an identity: decode(encode(rows)) == rows.
    let CodecValue::Json(reparsed) = parse(&codec, &written).payload.expect("payload") else {
        panic!("json payload");
    };
    let CodecValue::Json(original) = parse(&codec, "Id,Amt\nINB-7,100\nINB-8,200\n")
        .payload
        .expect("payload")
    else {
        panic!("json payload");
    };
    assert_eq!(reparsed, original);
}

#[test]
fn encode_accepts_the_array_root_value_wrapper() {
    // An array root projects under `value`, so a batch re-encodes without unwrapping first.
    let tree = serde_json::json!({"value": [{"Id": "INB-7"}, {"Id": "INB-8"}]});
    assert_eq!(encode(&CsvCodec::default(), tree), "Id\nINB-7\nINB-8\n");
}

#[test]
fn encode_writes_the_union_of_every_rows_keys_and_blanks_the_missing() {
    // Row 2 omits `Amt` and introduces `Note`. Every row must still line up under one header.
    let tree = serde_json::json!([
        {"Id": "A", "Amt": 1},
        {"Id": "B", "Note": "late"},
    ]);
    assert_eq!(
        encode(&CsvCodec::default(), tree),
        "Amt,Id,Note\n1,A,\n,B,late\n"
    );
}

#[test]
fn encode_quotes_per_rfc_4180_and_decode_reads_it_back() {
    let codec = CsvCodec::default();
    let tree = serde_json::json!([{"Id": "a,b", "Note": "he said \"hi\"", "Multi": "one\ntwo"}]);

    let written = encode(&codec, tree);
    assert_eq!(
        written,
        "Id,Multi,Note\n\"a,b\",\"one\ntwo\",\"he said \"\"hi\"\"\"\n"
    );
    // The real assertion: the writer's output is readable by this format's own parser.
    let back = parse(&codec, &written);
    assert_eq!(back.outcome, DecodeOutcome::Ok);
    let arr = rows(&back);
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["Id"], "a,b");
    assert_eq!(arr[0]["Note"], "he said \"hi\"");
    assert_eq!(arr[0]["Multi"], "one\ntwo");
}

#[test]
fn encode_preserves_the_exact_decimal_form_and_blanks_null() {
    // `arbitrary_precision` is on workspace-wide, so a decimal's WRITTEN scale survives — never
    // an f64 round trip that would turn "0.0250" into "0.025". Parsed from text, not the json!
    // macro, because the macro's float literal goes through f64 before serde_json ever sees it.
    let tree: serde_json::Value =
        serde_json::from_str(r#"[{"Rate": 0.0250, "Cell": null}]"#).expect("parses");
    assert_eq!(encode(&CsvCodec::default(), tree), "Cell,Rate\n,0.0250\n");
}

#[test]
fn headerless_encode_emits_cell_arrays_positionally() {
    let codec = CsvCodec::new(';', false);
    let tree = serde_json::json!([["a", "b"], ["c", "d"]]);
    assert_eq!(encode(&codec, tree), "a;b\nc;d\n");
}

#[test]
fn encode_refuses_a_nested_cell_rather_than_stringifying_it() {
    let err = CsvCodec::default()
        .encode(
            &CodecValue::Json(serde_json::json!([{"Id": "A", "Nested": {"x": 1}}])),
            Some("text/csv"),
        )
        .expect_err("a nested cell is not encodable");
    assert!(err.contains("flat"), "error should say why: {err}");
}
