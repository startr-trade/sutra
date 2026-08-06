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
