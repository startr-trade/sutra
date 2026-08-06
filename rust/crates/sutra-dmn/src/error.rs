//! Minimal coded-error type for the DMN extension — the loader and
//! validator raise coded errors under `SUTRA.VALIDATE.DMN.*` (see [`crate::codes`]).

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmnError {
    /// Stable diagnostic code string (see [`crate::codes`]).
    pub code: String,
    pub message: String,
}

impl DmnError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        DmnError {
            code: code.to_string(),
            message: message.into(),
        }
    }

    /// `SUTRA.VALIDATE.DMN.FILE_PARSE_ERROR` convenience constructor.
    pub fn parse(message: impl Into<String>) -> Self {
        DmnError::new(crate::codes::DMN_FILE_PARSE_ERROR, message)
    }
}

impl fmt::Display for DmnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for DmnError {}
