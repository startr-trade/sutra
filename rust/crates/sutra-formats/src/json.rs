//! The `json` codec. Arbitrary-precision parse (the workspace
//! `serde_json` enables `arbitrary_precision`, preserving arbitrary-precision decimals);
//! malformed / empty input is FATAL, never thrown.

use sutra_codec_spi::codec::{sanitize, PayloadCodec};
use sutra_codec_spi::codes;
use sutra_codec_spi::issue::ValidationIssue;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

pub struct JsonCodec;

// Self-registers as a zero-config global built-in (inventory pull model).
// json is a nested-map format — interchangeable with xml/yaml by content-type. It is a FORMAT,
// not a codec: it self-registers as a BuiltinFormat only (no schema ⇒ no static type validation).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "json",
        shape_class: sutra_codec_spi::ShapeClass::NestedMap,
        make: || std::sync::Arc::new(JsonCodec),
    }
}

impl PayloadCodec for JsonCodec {
    fn name(&self) -> &str {
        "json"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "application/json".to_string(),
            "application/*+json".to_string(),
        ]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let ct = content_type.unwrap_or("application/json");
        if body.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_JSON_PARSE_ERROR,
                    "",
                    "JSON body is empty",
                )],
                ct,
            );
        }
        match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(value) => DecodeResult::ok(CodecValue::Json(value), ct),
            Err(e) => DecodeResult::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_JSON_PARSE_ERROR,
                    "",
                    format!("JSON parse failed: {}", sanitize(&e.to_string())),
                )],
                ct,
            ),
        }
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let value = match payload {
            CodecValue::Json(v) => v.clone(),
            CodecValue::Text(s) => serde_json::Value::String(s.clone()),
            CodecValue::Bytes(_) => {
                return Err("json cannot encode raw bytes".to_string());
            }
        };
        serde_json::to_vec(&value).map_err(|e| sanitize(&e.to_string()))
    }
}
