//! Minimal mirror of the SPI `ValidationIssue` record — a single issue emitted by a content
//! validator, carrying a stable diagnostic code, severity, payload path, message, and the
//! optional offending value.

use sutra_feel::FeelValue;

/// ERROR / WARNING / INFO — mirror of `DiagnosticSeverity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationIssue {
    /// Stable issue code (e.g. `SUTRA.VALIDATE.DMN.RULESET_FAILED` or a `bpm:code` override).
    pub code: String,
    pub severity: Severity,
    /// JSON-Pointer-shaped path into the payload (unused by the DMN validator — always "").
    pub path: String,
    pub message: String,
    /// Optional offending value (the evaluated output).
    pub value: Option<FeelValue>,
}
