//! The `call-log-load` example decoded through its OWN codec, against its OWN sample files.
//!
//! The package lints clean, but lint never decodes anything — so this is what proves the example
//! actually works: both committed samples are accepted by the committed XSD, they project the
//! same records, and the committed bad-row file is rejected exactly where its README says.

use std::path::PathBuf;
use std::sync::Arc;

use sutra_codec_schema::StructuralCodec;
use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec};
use sutra_formats::{CsvCodec, FixedWidthCodec, FixedWidthField};

fn example(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/call-log-load")
        .join(rel)
}

fn read(rel: &str) -> Vec<u8> {
    std::fs::read(example(rel)).unwrap_or_else(|e| panic!("reading {rel}: {e}"))
}

/// The layout the example's `codec-manifest.yaml` declares.
fn fixed_width_layout() -> Vec<FixedWidthField> {
    [
        ("recordId", 12),
        ("msisdn", 16),
        ("peerMsisdn", 16),
        ("startTime", 22),
        ("durationSec", 6),
        ("direction", 11),
        ("cellId", 10),
        ("chargeAmount", 10),
        ("rateCode", 8),
    ]
    .into_iter()
    .map(|(n, w)| FixedWidthField::new(n, w).expect("valid field"))
    .collect()
}

/// The example's inbound codec, built from its COMMITTED schema and its COMMITTED manifest
/// layout — both wire forms, exactly as the package declares them.
fn cdr_codec() -> StructuralCodec {
    let xsd = read("deployments-src/default--call-log--1.0.0/schemas/cdr/cdr.xsd");
    let layout = fixed_width_layout();
    let columns: Vec<&str> = layout.iter().map(|f| f.name()).collect();
    let fixed = FixedWidthCodec::new(layout.clone()).expect("valid layout");
    StructuralCodec::compile_with_layout(
        "urn:cdr",
        &[xsd.as_slice()],
        &["csv", "fixed-width"],
        vec![Arc::new(CsvCodec::default()), Arc::new(fixed)],
        &columns,
    )
    .expect("the example's inbound schema compiles with both wire forms")
}

fn rows(result: &sutra_codec_spi::DecodeResult) -> Vec<serde_json::Value> {
    match result.payload.as_ref().expect("payload") {
        CodecValue::Json(serde_json::Value::Object(m)) => match &m["value"] {
            serde_json::Value::Array(rows) => rows.clone(),
            other => panic!("value should be an array, got {other:?}"),
        },
        other => panic!("expected the batch object, got {other:?}"),
    }
}

#[test]
fn the_committed_sample_csv_is_accepted_by_the_committed_schema() {
    let result = cdr_codec().decode(&read("sample/call-logs.csv"), Some("text/csv"));

    assert_eq!(
        result.outcome,
        DecodeOutcome::Ok,
        "the example's own sample must pass its own schema; issues: {:?}",
        result.issues
    );
    let rows = rows(&result);
    assert_eq!(rows.len(), 4, "four records");
    assert_eq!(rows[0]["recordId"], "CDR-100001");
    // The XSD's leaf types reach the cells: these are numbers, which is what lets the transform
    // render them unquoted into JSON and the store write them into numeric columns.
    assert!(rows[0]["durationSec"].is_number(), "{}", rows[0]);
    assert!(rows[0]["chargeAmount"].is_number(), "{}", rows[0]);
    // Row 3 leaves the optional rateCode column EMPTY, which must read as ABSENT rather than as
    // an empty string that would fail the RateCode enumeration.
    assert!(
        rows[2].get("rateCode").is_none(),
        "an empty optional cell is absence: {}",
        rows[2]
    );
}

/// The SAME codec, the same schema, the other wire form — selected purely by content-type. This
/// is what the example's two samples are for: a CSV feed and a fixed-width feed of the same
/// records, over one channel, indistinguishable to everything downstream.
#[test]
fn the_same_codec_accepts_the_committed_fixed_width_sample() {
    let codec = cdr_codec();
    let result = codec.decode(
        &read("sample/call-logs.fixed-width.txt"),
        Some("text/plain"),
    );

    assert_eq!(
        result.outcome,
        DecodeOutcome::Ok,
        "the fixed-width sample must pass the SAME schema; issues: {:?}",
        result.issues
    );
    // The decisive assertion: both wire forms project the SAME records.
    let csv = codec.decode(&read("sample/call-logs.csv"), Some("text/csv"));
    assert_eq!(
        rows(&result),
        rows(&csv),
        "the CSV and fixed-width samples are the same four records"
    );
}

#[test]
fn the_committed_bad_row_sample_is_rejected_exactly_where_the_readme_says() {
    let result = cdr_codec().decode(
        &read("sample/call-logs-with-a-bad-row.csv"),
        Some("text/csv"),
    );

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert_eq!(rows(&result).len(), 5, "every row still projects");
    let paths: Vec<&str> = result.issues.iter().map(|i| i.path.as_str()).collect();
    // README: rows CDR-100011 / -100012 / -100013 are bad (0-based indices 1, 2, 3).
    for bad in ["value[1]", "value[2]", "value[3]"] {
        assert!(
            paths.iter().any(|p| p.starts_with(bad)),
            "{bad} must be reported; paths: {paths:?}"
        );
    }
    for good in ["value[0]", "value[4]"] {
        assert!(
            !paths.iter().any(|p| p.starts_with(good)),
            "{good} is a good row and must not be reported; paths: {paths:?}"
        );
    }
}
