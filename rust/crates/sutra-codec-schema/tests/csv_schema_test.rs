//! The CSV format composing with a JSON-schema through `SchemaBoundCodec`. The end-to-end
//! schema composition is pinned (outcome, message type, array-root-under-`value` projection).
//! (The former fixed-width companion case was dropped with the fixed-width codec — it had no
//! xsd/json way to express its column layout.)

use std::sync::Arc;

use sutra_codec_schema::JsonSchema;
use sutra_codec_spi::{CodecValue, DecodeOutcome, PayloadCodec, SchemaBoundCodec};
use sutra_formats::CsvCodec;

const ROWS_SCHEMA: &str = r#"
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "array",
  "items": {"type": "object", "required": ["Id"], "properties": {"Id": {"type": "string"}}}
}
"#;

#[test]
fn csv_composes_with_a_json_schema_for_typing() {
    let schema = JsonSchema::compile("rows.json", ROWS_SCHEMA.as_bytes(), Some("rows.v1"))
        .expect("compiles");
    let codec = SchemaBoundCodec::new("rows-csv", Arc::new(CsvCodec::default()), Arc::new(schema));

    let result = codec.decode(b"Id,Amt\nINB-7,100\n", Some("text/csv"));

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    assert_eq!(result.message_type.as_deref(), Some("rows.v1"));
    // Array root projects under "value" (FEEL: payload.value[0].Id).
    let CodecValue::Json(serde_json::Value::Object(m)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert!(m.contains_key("value"));
}
