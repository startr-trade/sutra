//! Port of `JsonSchemaTest` (4) — the codec = json format × JSON-schema pipeline end to end:
//! parse to a tree, validate + project to a typed map, compose via `SchemaBoundCodec`. Schema
//! violations are `SOFT_ERRORS` (routable); malformed JSON is FATAL from the format.

use sutra_codec_schema::{json_schema_bound_codec, JsonNodeFormat, JsonSchema};
use sutra_codec_spi::{CodecValue, DecodeOutcome, MessageFormat, PayloadCodec};

const SCHEMA: &str = r#"
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["Id", "Amount"],
  "properties": {
    "Id": {"type": "string"},
    "Amount": {"type": "number"},
    "Item": {
      "type": "array",
      "items": {"type": "object", "required": ["Sku"], "properties": {"Sku": {"type": "string"}}}
    }
  }
}
"#;

fn codec() -> sutra_codec_spi::SchemaBoundCodec {
    let schema = JsonSchema::compile("payment.json", SCHEMA.as_bytes(), Some("payment.v1"))
        .expect("schema compiles");
    json_schema_bound_codec("payment-json", schema)
}

#[test]
fn valid_json_decodes_to_typed_navigable_map() {
    let json = br#"{"Id":"INB-7","Amount":1234.56,"Item":[{"Sku":"A"},{"Sku":"B"}]}"#;
    let result = codec().decode(json, Some("application/json"));

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    assert_eq!(result.message_type.as_deref(), Some("payment.v1"));
    let CodecValue::Json(serde_json::Value::Object(m)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert_eq!(m.get("Id").unwrap(), "INB-7");
    // Scalars keep their JSON types — Amount is a number, not a string.
    assert_eq!(m.get("Amount").unwrap().as_f64().unwrap(), 1234.56);
    // Arrays → list of nested maps.
    let items = m.get("Item").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].get("Sku").unwrap(), "A");
    assert_eq!(items[1].get("Sku").unwrap(), "B");
}

#[test]
fn schema_violation_is_soft_error_with_payload_still_projected() {
    // Missing the required "Amount" — schema-invalid but well-formed JSON.
    let result = codec().decode(br#"{"Id":"INB-7"}"#, Some("application/json"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    let CodecValue::Json(serde_json::Value::Object(m)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert_eq!(m.get("Id").unwrap(), "INB-7"); // routable
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION"));
}

#[test]
fn wrong_scalar_type_is_a_schema_violation() {
    // Amount as a string, not a number — a type violation the schema catches.
    let result = codec().decode(
        br#"{"Id":"INB-7","Amount":"not-a-number"}"#,
        Some("application/json"),
    );

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION"));
}

#[test]
fn malformed_json_is_fatal_from_the_format() {
    let result = JsonNodeFormat.parse(br#"{"Id":"#, Some("application/json"));

    assert_eq!(result.outcome, DecodeOutcome::Fatal);
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.PARSE.JSON.PARSE_ERROR"));
}
