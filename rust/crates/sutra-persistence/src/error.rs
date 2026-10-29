use crate::DeploymentId;

/// Error type for every persistence operation in this crate.
///
/// A single wrapper over all store failures, but keeps distinct variants for the conditions
/// callers branch on: the unique-live alias
/// collision (the engine resolves it via `onConflict=reject`/`correlate`) and
/// migration-runner failures (which are configuration errors, not runtime store errors).
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// Underlying database failure, tagged with the operation that issued it.
    #[error("{operation} failed: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },

    /// A unique-live alias is already owned by a different live instance. Surfaced
    /// by the strict transactional step so the whole step rolls back.
    #[error(
        "unique alias collision: ({deployment}, {alias_name}, {alias_value}) is owned by a \
         different live instance"
    )]
    AliasCollision {
        deployment: DeploymentId,
        alias_name: String,
        alias_value: String,
    },

    /// The step carried an inbox dedup record that another delivery of the same event committed
    /// first (two deliveries of one event ran concurrently). The whole step rolls back: the
    /// event was processed exactly once, by the other delivery.
    #[error("inbox duplicate: ({deployment}, {channel}, {event_id}) was already recorded")]
    InboxDuplicate {
        deployment: DeploymentId,
        channel: String,
        event_id: String,
    },

    /// Underlying SQL Server failure, tagged with the operation that issued it (the
    /// `mssql` dialect runs on its own TDS client stack, not sqlx).
    #[cfg(feature = "mssql")]
    #[error("{operation} failed: {source}")]
    Mssql {
        operation: &'static str,
        #[source]
        source: tiberius::error::Error,
    },

    /// Invalid input that must be rejected before touching the database (bad deployment-id
    /// form, non-positive TTL, negative age, ...).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Migration discovery / ordering / application failure.
    #[error("migration error: {0}")]
    Migration(String),
}

impl PersistenceError {
    /// Whether the failure is the database being unreachable or going away — a dropped or reset
    /// connection, a pool that could not hand one out in time, the server shutting down or not
    /// yet accepting connections — rather than anything about the statement. Such a step did not
    /// commit, or (a COMMIT whose answer was lost) committed together with its inbox dedup record;
    /// either way the same delivery can be offered again and is processed at most once.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Database { source, .. } => match source {
                sqlx::Error::Io(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed => true,
                sqlx::Error::Database(db) => db.code().is_some_and(|code| {
                    // SQLSTATE class 08 (connection exception) and 57P01/57P02/57P03
                    // (admin shutdown, crash shutdown, cannot connect now).
                    code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
                }),
                _ => false,
            },
            _ => false,
        }
    }

    /// Tags a sqlx error with the store operation that produced it.
    pub fn db(operation: &'static str) -> impl FnOnce(sqlx::Error) -> Self {
        move |source| Self::Database { operation, source }
    }

    /// Tags a SQL Server client error with the store operation that produced it.
    #[cfg(feature = "mssql")]
    pub fn mssql(operation: &'static str) -> impl FnOnce(tiberius::error::Error) -> Self {
        move |source| Self::Mssql { operation, source }
    }
}

#[cfg(test)]
mod transient_tests {
    use super::*;

    #[test]
    fn only_an_unreachable_database_is_transient() {
        let io = PersistenceError::db("step commit")(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "Connection reset by peer",
        )));
        assert!(io.is_transient());
        assert!(PersistenceError::db("acquire")(sqlx::Error::PoolTimedOut).is_transient());
        assert!(!PersistenceError::db("decode")(sqlx::Error::RowNotFound).is_transient());
        assert!(!PersistenceError::InvalidArgument("bad".into()).is_transient());
    }
}
