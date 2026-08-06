//! Minimal coded-error type — loader and model raise coded errors carrying a stable `SUTRA.*`
//! diagnostic code string (see [`crate::codes`]).

use std::fmt;

/// A coded engine error carrying a stable diagnostic code string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SutraError {
    /// Stable diagnostic code string (see [`crate::codes`]).
    pub code: String,
    pub message: String,
}

impl SutraError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        SutraError {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SutraError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for SutraError {}
