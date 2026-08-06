//! Structured diagnostic — the load-bearing subset of the diagnostic contract
//! (code + message + string attributes) that the intake pipeline and the HTTP problem
//! rendering need.

use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable `SUTRA.*` code string.
    pub code: String,
    pub message: String,
    /// Structured attributes (`errorCode`, `channel`, …) — stringly, deterministic order.
    pub attributes: BTreeMap<String, String>,
}

impl Diagnostic {
    pub fn error(code: &str, message: impl Into<String>) -> Diagnostic {
        Diagnostic {
            code: code.to_string(),
            message: message.into(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn with_attribute(mut self, key: &str, value: impl Into<String>) -> Diagnostic {
        self.attributes.insert(key.to_string(), value.into());
        self
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for Diagnostic {}
