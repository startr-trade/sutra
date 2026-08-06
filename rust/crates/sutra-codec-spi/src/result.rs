//! The outcome of a codec decode.

use crate::issue::ValidationIssue;

/// Decode outcome per the three-way contract: `OK` (payload, no issues),
/// `SOFT_ERRORS` (usable payload + routable structural issues), `FATAL` (no payload;
/// issues carry the cause — the engine rejects per the configured posture).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeOutcome {
    Ok,
    SoftErrors,
    Fatal,
}

/// The typed payload a built-in codec produces. Where the contract is generically typed
/// (string / bytes / JSON value / DOM projection), this closed set makes the variants
/// explicit. `Json` doubles as the FEEL-walkable tree for `json`, `yaml`, and the `xml`
/// map projection (numbers keep arbitrary precision via `serde_json`).
#[derive(Debug, Clone, PartialEq)]
pub enum CodecValue {
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

/// Decode result — outcome + optional typed payload + accumulated tier-1 issues +
/// effective content type + the recognised message type (multi-type codecs).
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeResult {
    pub outcome: DecodeOutcome,
    pub payload: Option<CodecValue>,
    pub issues: Vec<ValidationIssue>,
    pub content_type: String,
    pub message_type: Option<String>,
}

impl DecodeResult {
    pub fn ok(payload: CodecValue, content_type: &str) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::Ok,
            payload: Some(payload),
            issues: Vec::new(),
            content_type: content_type.to_string(),
            message_type: None,
        }
    }

    pub fn soft_errors(
        payload: CodecValue,
        issues: Vec<ValidationIssue>,
        content_type: &str,
    ) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::SoftErrors,
            payload: Some(payload),
            issues,
            content_type: content_type.to_string(),
            message_type: None,
        }
    }

    pub fn fatal(issues: Vec<ValidationIssue>, content_type: &str) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::Fatal,
            payload: None,
            issues,
            content_type: content_type.to_string(),
            message_type: None,
        }
    }

    /// Copy carrying the recognised `message_type` (blank clears it) — the
    /// message-type builder.
    pub fn with_message_type(mut self, message_type: &str) -> DecodeResult {
        let t = message_type.trim();
        self.message_type = if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        };
        self
    }
}
