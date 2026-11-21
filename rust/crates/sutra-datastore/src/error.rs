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

    /// The message alone — no `[CODE] ` prefix and no source.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The stable diagnostic code, when this failure has one.
    pub fn code(&self) -> Option<&'static str> {
        self.code
    }

    /// Whether the failure is the store's database being unreachable or going away — a dropped
    /// or reset connection, a pool that could not hand one out in time, the server shutting down
    /// or not yet accepting connections — rather than anything about the operation. Nothing the
    /// operation asked for can have been applied by a statement that never reached the server, so
    /// the step that ran it can be offered again once the database is back.
    pub fn is_transient(&self) -> bool {
        let mut next: Option<&(dyn std::error::Error + 'static)> = self
            .source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static));
        while let Some(error) = next {
            if is_unreachable(error) {
                return true;
            }
            next = error.source();
        }
        false
    }
}

/// One link of a source chain that says the database could not be reached.
fn is_unreachable(error: &(dyn std::error::Error + 'static)) -> bool {
    if error.downcast_ref::<std::io::Error>().is_some() {
        return true;
    }
    #[cfg(feature = "providers")]
    if let Some(e) = error.downcast_ref::<sqlx::Error>() {
        return match e {
            sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed => true,
            // SQLSTATE class 08 (connection exception) and 57P01/57P02/57P03 (admin shutdown,
            // crash shutdown, cannot connect now).
            sqlx::Error::Database(db) => db.code().is_some_and(|code| {
                code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
            }),
            _ => false,
        };
    }
    #[cfg(feature = "mssql")]
    if let Some(e) = error.downcast_ref::<tiberius::error::Error>() {
        return matches!(e, tiberius::error::Error::Io { .. });
    }
    false
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

#[cfg(test)]
mod transient_tests {
    use super::DataStoreError;

    #[test]
    fn only_an_unreachable_database_is_transient() {
        let reset = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        assert!(DataStoreError::with_source("put failed", reset).is_transient());
        assert!(!DataStoreError::new("no connection declared").is_transient());
        assert!(!DataStoreError::with_code("SUTRA.X", "contract").is_transient());
    }

    #[cfg(feature = "providers")]
    #[test]
    fn a_pool_timeout_or_a_broken_connection_is_transient_and_a_missing_row_is_not() {
        assert!(DataStoreError::with_source("acquire", sqlx::Error::PoolTimedOut).is_transient());
        let broken = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "Broken pipe",
        ));
        assert!(DataStoreError::with_source("put", broken).is_transient());
        assert!(!DataStoreError::with_source("get", sqlx::Error::RowNotFound).is_transient());
    }
}
