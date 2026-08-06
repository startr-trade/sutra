//! The format × schema composition SPI, exercised with tiny test-double format/schema
//! pairs (`LINE_MAP` / `RequiredKeysSchema`).
//!
//! Note: `serde_json` maps are `BTreeMap` (no `preserve_order` feature), so
//! `encode_delegates_to_schema_projection_then_format_encode` asserts decode-equality +
//! field presence rather than an exact insertion-order byte string.

use std::sync::Arc;

use sutra_codec_spi::{
    CodecValue, DecodeOutcome, FormatOnlyCodec, FormatParse, MessageFormat, MessageSchema,
    PayloadCodec, SchemaBoundCodec, SchemaKind, ValidationIssue,
};

// ---- test doubles ----------------------------------------------------------------------------

/// Parses `key=value` lines into an object; FATAL on a line without `=`. No encode.
struct LineMap;

impl LineMap {
    fn parse_lines(body: &[u8], content_type: &str) -> FormatParse {
        let text = String::from_utf8_lossy(body);
        let mut map = serde_json::Map::new();
        for line in text.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            match line.find('=') {
                None => {
                    return FormatParse::fatal(
                        vec![ValidationIssue::error(
                            "SUTRA.TEST.PARSE_FAILED",
                            "",
                            format!("no '=' in line: {line}"),
                        )],
                        content_type,
                    );
                }
                Some(eq) => {
                    map.insert(
                        line[..eq].trim().to_string(),
                        serde_json::Value::String(line[eq + 1..].trim().to_string()),
                    );
                }
            }
        }
        FormatParse::ok(serde_json::Value::Object(map), content_type)
    }
}

impl MessageFormat for LineMap {
    fn name(&self) -> &str {
        "line-map"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/x-line-map".to_string()]
    }
    fn parse(&self, body: &[u8], content_type: Option<&str>) -> FormatParse {
        LineMap::parse_lines(body, content_type.unwrap_or("text/x-line-map"))
    }
}

/// Round-trippable variant: also encodes an object back to `key=value` lines.
struct LineMapRt;

impl MessageFormat for LineMapRt {
    fn name(&self) -> &str {
        "line-map-rt"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/x-line-map".to_string()]
    }
    fn parse(&self, body: &[u8], content_type: Option<&str>) -> FormatParse {
        LineMap::parse_lines(body, content_type.unwrap_or("text/x-line-map"))
    }
    fn encode_tree(
        &self,
        tree: &serde_json::Value,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        let mut out = String::new();
        if let serde_json::Value::Object(map) = tree {
            for (k, v) in map {
                let value = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                out.push_str(&format!("{k}={value}\n"));
            }
        }
        Ok(out.into_bytes())
    }
}

/// Requires a set of keys present; projects the map through; tags a message type.
struct RequiredKeysSchema {
    required_keys: Vec<&'static str>,
    message_type: Option<String>,
}

impl MessageSchema for RequiredKeysSchema {
    fn kind(&self) -> SchemaKind {
        SchemaKind::JsonSchema
    }
    fn message_type(&self) -> Option<String> {
        self.message_type.clone()
    }
    fn shape_of(&self, _message_type: &str) -> Option<sutra_codec_spi::SchemaShape> {
        None
    }
    fn validate_and_project(
        &self,
        tree: &serde_json::Value,
        content_type: &str,
    ) -> sutra_codec_spi::DecodeResult {
        let mut issues = Vec::new();
        let obj = tree.as_object();
        for k in &self.required_keys {
            let present = obj.map(|m| m.contains_key(*k)).unwrap_or(false);
            if !present {
                issues.push(ValidationIssue::error(
                    "SUTRA.TEST.REQUIRED_MISSING",
                    &format!("/{k}"),
                    format!("missing '{k}'"),
                ));
            }
        }
        let payload = CodecValue::Json(tree.clone());
        let result = if issues.is_empty() {
            sutra_codec_spi::DecodeResult::ok(payload, content_type)
        } else {
            sutra_codec_spi::DecodeResult::soft_errors(payload, issues, content_type)
        };
        match &self.message_type {
            Some(mt) => result.with_message_type(mt),
            None => result,
        }
    }
    fn project_to_tree(
        &self,
        payload: &serde_json::Value,
        _content_type: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(payload.clone()) // identity — here the tree IS a map
    }
}

fn settlement() -> RequiredKeysSchema {
    RequiredKeysSchema {
        required_keys: vec!["id", "amount"],
        message_type: Some("settlement.v1".to_string()),
    }
}

fn bound(format: Arc<dyn MessageFormat>) -> SchemaBoundCodec {
    SchemaBoundCodec::new("settlement-csv", format, Arc::new(settlement()))
}

// ---- tests -----------------------------------------------------------------------------------

#[test]
fn schema_bound_codec_decodes_to_typed_validated_map() {
    let codec = bound(Arc::new(LineMap));
    let result = codec.decode(b"id=INB-7\namount=1234.56", Some("text/x-line-map"));

    assert_eq!(result.outcome, DecodeOutcome::Ok);
    assert_eq!(result.message_type.as_deref(), Some("settlement.v1"));
    let CodecValue::Json(serde_json::Value::Object(typed)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert_eq!(typed.get("id").unwrap(), "INB-7");
    assert_eq!(typed.get("amount").unwrap(), "1234.56");
    assert_eq!(codec.accepted_content_types(), vec!["text/x-line-map"]);
}

#[test]
fn schema_violation_is_soft_error_with_payload_still_present() {
    let codec = bound(Arc::new(LineMap));
    let result = codec.decode(b"id=INB-7", Some("text/x-line-map"));

    assert_eq!(result.outcome, DecodeOutcome::SoftErrors);
    assert!(result.payload.is_some());
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.TEST.REQUIRED_MISSING"));
    assert_eq!(result.message_type.as_deref(), Some("settlement.v1"));
}

#[test]
fn format_parse_failure_short_circuits_as_fatal() {
    let codec = bound(Arc::new(LineMap));
    let result = codec.decode(b"this-line-has-no-equals", Some("text/x-line-map"));

    assert_eq!(result.outcome, DecodeOutcome::Fatal);
    assert!(result.payload.is_none());
    assert!(result
        .issues
        .iter()
        .any(|i| i.code == "SUTRA.TEST.PARSE_FAILED"));
}

#[test]
fn declared_message_types_reports_the_single_bound_type_when_present() {
    let codec = bound(Arc::new(LineMap));
    assert_eq!(codec.declared_message_types(), vec!["settlement.v1"]);
}

#[test]
fn declared_message_types_is_empty_when_the_schema_has_no_message_type() {
    let untyped = RequiredKeysSchema {
        required_keys: vec!["id"],
        message_type: None,
    };
    let codec = SchemaBoundCodec::new("opaque-csv", Arc::new(LineMap), Arc::new(untyped));
    assert!(codec.declared_message_types().is_empty());
}

#[test]
fn format_only_codec_is_the_degenerate_opaque_mode() {
    let codec = FormatOnlyCodec::new(Arc::new(LineMap));
    let result = codec.decode(b"id=INB-7\namount=1", Some("text/x-line-map"));

    assert_eq!(codec.name(), "line-map"); // adopts the format's name
    assert_eq!(result.outcome, DecodeOutcome::Ok);
    let CodecValue::Json(serde_json::Value::Object(map)) = result.payload.unwrap() else {
        panic!("map payload");
    };
    assert_eq!(map.get("id").unwrap(), "INB-7");
    assert!(result.message_type.is_none()); // opaque: no typed message type
}

#[test]
fn encode_errs_when_the_underlying_format_or_schema_lacks_it() {
    // LINE_MAP implements no encode; the schema's projectToTree is identity, so the format's
    // missing encode is what fails.
    let bound = bound(Arc::new(LineMap));
    let opaque = FormatOnlyCodec::new(Arc::new(LineMap));
    let empty = CodecValue::Json(serde_json::json!({}));
    assert!(bound.encode(&empty, Some("text/x-line-map")).is_err());
    assert!(opaque.encode(&empty, Some("text/x-line-map")).is_err());
}

#[test]
fn encode_delegates_to_schema_projection_then_format_encode() {
    let codec = SchemaBoundCodec::new("settlement", Arc::new(LineMapRt), Arc::new(settlement()));
    let reply = CodecValue::Json(serde_json::json!({"id": "INB-7", "amount": "1234.56"}));

    let bytes = codec
        .encode(&reply, Some("text/x-line-map"))
        .expect("encodes");
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(text.contains("id=INB-7"));
    assert!(text.contains("amount=1234.56"));
    // Round-trip: the encoded bytes decode back to the same typed map.
    let CodecValue::Json(serde_json::Value::Object(map)) = codec
        .decode(&bytes, Some("text/x-line-map"))
        .payload
        .unwrap()
    else {
        panic!("map payload");
    };
    assert_eq!(map.get("id").unwrap(), "INB-7");
    assert_eq!(map.get("amount").unwrap(), "1234.56");
}
