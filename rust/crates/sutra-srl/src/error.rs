//! Coded diagnostic for the `.srl` subsystem — modelled on `sutra_dmn::error::DmnError`, but
//! carrying a 1-based `line`/`column` so it can `Display` as `line:col: message` (the offending
//! construct's source position).
//!
//! FEEL sub-diagnostics (parse or eval) are wrapped: the underlying `SUTRA.FEEL.*` code and
//! message are folded into [`SrlError::message`], and the FEEL character offset is composed with
//! the embedded expression's `.srl` origin so the reported `line`/`column` point into the `.srl`
//! source, not the lifted sub-expression.

use std::fmt;

use sutra_feel::positions::FeelSourcePositions;

/// A coded `.srl` diagnostic with a source position and the offending construct's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SrlError {
    /// Stable diagnostic code string (see [`crate::codes`]).
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// 1-based line of the offending construct.
    pub line: u32,
    /// 1-based column of the offending construct.
    pub column: u32,
    /// The offending construct's source text (a token's text, a verb name, …), when known.
    pub construct: Option<String>,
}

impl SrlError {
    /// Build an error whose position is derived from a `.srl` character `offset`.
    pub fn at(
        code: &str,
        message: impl Into<String>,
        offset: usize,
        positions: &FeelSourcePositions,
    ) -> Self {
        let loc = positions.location_for(offset);
        SrlError {
            code: code.to_string(),
            message: message.into(),
            line: loc.line,
            column: loc.column,
            construct: None,
        }
    }

    /// Attach the offending construct's source text (builder style).
    pub fn with_construct(mut self, construct: impl Into<String>) -> Self {
        self.construct = Some(construct.into());
        self
    }
}

impl fmt::Display for SrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.column, self.message)
    }
}

impl std::error::Error for SrlError {}
