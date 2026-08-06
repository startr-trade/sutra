//! The `yaml` codec. YAML 1.2 is a JSON superset, so decode
//! yields the SAME JSON tree envelope as the `json` codec (uniform FEEL paths).
//!
//! Safety posture is data-only: `serde_yaml_ng` only ever builds
//! plain data (scalars / sequences / mappings — no object instantiation), rejects duplicate
//! mapping keys, and bounds alias expansion (billion-laughs protection). Malformed input is
//! FATAL, never thrown.

use sutra_codec_spi::codec::{sanitize, PayloadCodec};
use sutra_codec_spi::codes;
use sutra_codec_spi::issue::ValidationIssue;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

pub struct YamlCodec;

// Self-registers as a zero-config global built-in (inventory pull model).
// yaml is a nested-map format — interchangeable with json/xml by content-type (BuiltinFormat only).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "yaml",
        shape_class: sutra_codec_spi::ShapeClass::NestedMap,
        make: || std::sync::Arc::new(YamlCodec),
    }
}

impl PayloadCodec for YamlCodec {
    fn name(&self) -> &str {
        "yaml"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "application/yaml".to_string(),
            "application/x-yaml".to_string(),
            "text/yaml".to_string(),
        ]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let ct = content_type.unwrap_or("application/yaml");
        if body.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_YAML_PARSE_ERROR,
                    "",
                    "YAML body is empty",
                )],
                ct,
            );
        }
        match serde_yaml_ng::from_slice::<serde_yaml_ng::Value>(body) {
            Ok(value) => DecodeResult::ok(CodecValue::Json(yaml_to_json(&value)), ct),
            Err(e) => DecodeResult::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_YAML_PARSE_ERROR,
                    "",
                    format!("YAML parse failed: {}", sanitize(&e.to_string())),
                )],
                ct,
            ),
        }
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let value = match payload {
            CodecValue::Json(v) => json_to_yaml(v)?,
            CodecValue::Text(s) => serde_yaml_ng::Value::String(s.clone()),
            CodecValue::Bytes(_) => return Err("yaml cannot encode raw bytes".to_string()),
        };
        serde_yaml_ng::to_string(&value)
            .map(String::into_bytes)
            .map_err(|e| sanitize(&e.to_string()))
    }
}

/// YAML tree → JSON tree, per the number/key mapping: integral numbers stay
/// integral, other numbers become doubles, non-string mapping keys are stringified,
/// tagged values collapse to their inner value.
fn yaml_to_json(v: &serde_yaml_ng::Value) -> serde_json::Value {
    use serde_yaml_ng::Value as Y;
    match v {
        Y::Null => serde_json::Value::Null,
        Y::Bool(b) => serde_json::Value::Bool(*b),
        Y::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::from(i)
            } else if let Some(u) = n.as_u64() {
                serde_json::Value::from(u)
            } else {
                serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0))
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
        }
        Y::String(s) => serde_json::Value::String(s.clone()),
        Y::Sequence(items) => serde_json::Value::Array(items.iter().map(yaml_to_json).collect()),
        Y::Mapping(m) => {
            let mut out = serde_json::Map::new();
            for (k, val) in m {
                out.insert(yaml_key_string(k), yaml_to_json(val));
            }
            serde_json::Value::Object(out)
        }
        Y::Tagged(t) => yaml_to_json(&t.value),
    }
}

/// Mapping key → string (any scalar key rendered as its string form).
fn yaml_key_string(k: &serde_yaml_ng::Value) -> String {
    use serde_yaml_ng::Value as Y;
    match k {
        Y::String(s) => s.clone(),
        Y::Bool(b) => b.to_string(),
        Y::Number(n) => n.to_string(),
        Y::Null => "null".to_string(),
        other => serde_yaml_ng::to_string(other)
            .map(|s| s.trim_end().to_string())
            .unwrap_or_default(),
    }
}

/// JSON tree → YAML tree (the encode inverse). Explicit — the `arbitrary_precision`
/// `serde_json::Number` does not serialize portably through foreign serializers.
fn json_to_yaml(v: &serde_json::Value) -> Result<serde_yaml_ng::Value, String> {
    use serde_json::Value as J;
    Ok(match v {
        J::Null => serde_yaml_ng::Value::Null,
        J::Bool(b) => serde_yaml_ng::Value::Bool(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_yaml_ng::Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                serde_yaml_ng::Value::Number(u.into())
            } else if let Some(f) = n.as_f64() {
                serde_yaml_ng::Value::Number(f.into())
            } else {
                // Arbitrary-precision literal outside f64 — keep the exact digits as text.
                serde_yaml_ng::Value::String(n.to_string())
            }
        }
        J::String(s) => serde_yaml_ng::Value::String(s.clone()),
        J::Array(items) => serde_yaml_ng::Value::Sequence(
            items.iter().map(json_to_yaml).collect::<Result<_, _>>()?,
        ),
        J::Object(m) => {
            let mut out = serde_yaml_ng::Mapping::new();
            for (k, val) in m {
                out.insert(serde_yaml_ng::Value::String(k.clone()), json_to_yaml(val)?);
            }
            serde_yaml_ng::Value::Mapping(out)
        }
    })
}
