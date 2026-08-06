//! A single validation issue. Codecs emit
//! `SUTRA.PARSE.*` (structural) issues; content validators emit `SUTRA.VALIDATE.*` (or
//! domain codes). The `value` slot carries the vendor reason code the frozen validation
//! summary surfaces as `validation.firstReasonCode`.

/// ERROR / WARNING / INFO — mirror of `DiagnosticSeverity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueSeverity {
    Error,
    Warning,
    Info,
}

impl IssueSeverity {
    /// The canonical enum-constant name (`ERROR` / `WARNING` / `INFO`) — the wire/FEEL string.
    pub fn as_str(&self) -> &'static str {
        match self {
            IssueSeverity::Error => "ERROR",
            IssueSeverity::Warning => "WARNING",
            IssueSeverity::Info => "INFO",
        }
    }
}

/// One issue emitted by a codec (structural) or a content validator (semantic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    /// Stable `SUTRA.*` (or extension) code string.
    pub code: String,
    pub severity: IssueSeverity,
    /// Path into the payload (JSON-Pointer-shaped; empty when not applicable).
    pub path: String,
    pub message: String,
    /// Optional offending value / vendor reason code (`validation.firstReasonCode`).
    pub value: Option<String>,
}

impl ValidationIssue {
    pub fn error(code: &str, path: &str, message: impl Into<String>) -> ValidationIssue {
        ValidationIssue {
            code: code.to_string(),
            severity: IssueSeverity::Error,
            path: path.to_string(),
            message: message.into(),
            value: None,
        }
    }
}
