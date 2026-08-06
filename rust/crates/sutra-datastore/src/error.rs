//! Data-store errors — the `DataStoreException` analog, fail-closed everywhere.

use std::fmt;

/// A data-store failure: bad declaration, unresolvable secret-ref, connection/SQL failure,
/// or a malformed stored value.
///
/// Most failures are plain prose (a driver error the operator reads). Some are CONTRACT
/// failures the engine names with a stable code — the projected-store rejections of
/// [`crate::projected::codes`]; those carry it in [`DataStoreError::code`] and render it as a
/// `[CODE] ` prefix, so the code survives the executor's `StoreError::new(e.to_string())`
/// stringification onto the instance diagnostic.
#[derive(Debug)]
pub struct DataStoreError {
    code: Option<&'static str>,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DataStoreError {
    pub fn new(message: impl Into<String>) -> DataStoreError {
        DataStoreError {
            code: None,
            message: message.into(),
            source: None,
        }
    }

    /// A failure carrying a stable `SUTRA.*` diagnostic code (see the type docs).
    pub fn with_code(code: &'static str, message: impl Into<String>) -> DataStoreError {
        DataStoreError {
            code: Some(code),
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> DataStoreError {
        DataStoreError {
            code: None,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// The stable diagnostic code, when this failure has one.
    pub fn code(&self) -> Option<&'static str> {
        self.code
    }
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            write!(f, "[{code}] ")?;
        }
        match &self.source {
            Some(s) => write!(f, "{}: {s}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for DataStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}
