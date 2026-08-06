//! Minimal coded-error / diagnostic pair for the FEEL subsystem.
//!
//! The full structured diagnostic is reduced to the load-bearing fields only: the stable
//! `SUTRA.FEEL.*` code string, the human message, the character offset of the offending
//! token/sub-expression, an optional line/column [`SourceLocation`] (present when source
//! positions were in scope), and the optional remediation hint.

use std::fmt;

/// 1-based line/column source location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    /// Source URI; `"feel:inline"` when the expression was not lifted from a file.
    pub uri: String,
    pub line: u32,
    pub column: u32,
    pub end_line: Option<u32>,
    pub end_column: Option<u32>,
}

/// FEEL error — carries a `SUTRA.FEEL.*` diagnostic code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeelError {
    /// Stable diagnostic code string (see [`crate::codes`]).
    pub code: String,
    pub message: String,
    /// Character offset of the offending token / sub-expression, when known.
    pub offset: Option<usize>,
    /// Line/column location — only populated when source positions were in scope
    /// (facade entry points); `None` for direct AST evaluation, which yields
    /// position-less diagnostics. Boxed to keep the error small on the `Result` hot path.
    pub location: Option<Box<SourceLocation>>,
    /// Remediation hint.
    pub hint: Option<String>,
}

impl FeelError {
    /// Position-less error (mirror of `Diagnostic.error(code, message)`).
    pub fn plain(code: &str, message: impl Into<String>) -> Self {
        FeelError {
            code: code.to_string(),
            message: message.into(),
            offset: None,
            location: None,
            hint: None,
        }
    }

    pub fn at(
        code: &str,
        message: impl Into<String>,
        offset: usize,
        location: Option<SourceLocation>,
    ) -> Self {
        FeelError {
            code: code.to_string(),
            message: message.into(),
            offset: Some(offset),
            location: location.map(Box::new),
            hint: None,
        }
    }
}

impl fmt::Display for FeelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for FeelError {}
