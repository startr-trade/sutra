//! The `raw-text` / `raw-bytes` codecs — ports of `RawTextFormat` / `RawBytesFormat`.
//! Both are total: always [`DecodeOutcome::Ok`].

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

/// `raw-text`: bytes → UTF-8 string, no structure (opaque text building block).
pub struct RawTextCodec;

// Self-registers as a zero-config global built-in (inventory pull model).
// raw-text is opaque (text/plain) — no map projection, not interchangeable (BuiltinFormat only).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "raw-text",
        shape_class: sutra_codec_spi::ShapeClass::Opaque,
        make: || std::sync::Arc::new(RawTextCodec),
    }
}

impl PayloadCodec for RawTextCodec {
    fn name(&self) -> &str {
        "raw-text"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string(), "*/*".to_string()]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let text = String::from_utf8_lossy(body).into_owned();
        DecodeResult::ok(
            CodecValue::Text(text),
            content_type.unwrap_or("text/plain; charset=utf-8"),
        )
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        match payload {
            CodecValue::Text(s) => Ok(s.as_bytes().to_vec()),
            CodecValue::Bytes(b) => Ok(b.clone()),
            CodecValue::Json(v) => Ok(match v {
                serde_json::Value::String(s) => s.as_bytes().to_vec(),
                other => other.to_string().into_bytes(),
            }),
        }
    }
}

/// `raw-bytes`: passthrough, no structure (binary blobs, encrypted envelopes).
pub struct RawBytesCodec;

// Self-registers as a zero-config global built-in (inventory pull model).
// raw-bytes is opaque (application/octet-stream) — no map projection (BuiltinFormat only).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "raw-bytes",
        shape_class: sutra_codec_spi::ShapeClass::Opaque,
        make: || std::sync::Arc::new(RawBytesCodec),
    }
}

impl PayloadCodec for RawBytesCodec {
    fn name(&self) -> &str {
        "raw-bytes"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["application/octet-stream".to_string(), "*/*".to_string()]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        DecodeResult::ok(
            CodecValue::Bytes(body.to_vec()),
            content_type.unwrap_or("application/octet-stream"),
        )
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        match payload {
            CodecValue::Bytes(b) => Ok(b.clone()),
            CodecValue::Text(s) => Ok(s.as_bytes().to_vec()),
            CodecValue::Json(v) => Err(format!(
                "raw-bytes cannot encode a JSON tree ({})",
                match v {
                    serde_json::Value::Object(_) => "object",
                    serde_json::Value::Array(_) => "array",
                    _ => "scalar",
                }
            )),
        }
    }
}
