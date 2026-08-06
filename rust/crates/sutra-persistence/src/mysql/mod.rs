//! MySQL / MariaDB dialect (PostgreSQL is the reference dialect — semantics are normative,
//! syntax is not).
//!
//! One module serves BOTH MySQL 8 and MariaDB 10.6+/11: every statement here restricts
//! itself to the SQL surface the two share (`FOR UPDATE SKIP LOCKED`,
//! `INSERT ... ON DUPLICATE KEY UPDATE` with `VALUES()` transfer, `NOW(6)` /
//! `CURRENT_TIMESTAMP(6)`, `TIMESTAMPADD`). The container suites run the identical test
//! sources against both engines to prove the shared dialect genuinely holds.
//!
//! Deliberate duplication: this module restates the reference stores rather than
//! abstracting over them — the PostgreSQL code is frozen as the reference implementation,
//! and boring per-dialect SQL beats a clever cross-dialect query builder.
//!
//! Dialect-wide notes (each pinned by the container suites):
//!
//! - **Isolation posture**: enforced-bind-only. MySQL/MariaDB have no row-security
//!   policies, so the database layer of the reference dialect's two-layer enforcement does
//!   not exist here; the explicit `deployment_id` bind on every statement is the isolation.
//!   See [`scope`].
//! - **Matched-rows semantics**: connections run with `CLIENT_FOUND_ROWS`, so
//!   `rows_affected` counts MATCHED rows like the reference dialect (an `UPDATE` that
//!   rewrites identical values still reports 1).
//! - **First-observer dedup** is a plain `INSERT` + duplicate-key rejection (error 1062),
//!   not `INSERT IGNORE` (which downgrades unrelated errors to warnings) and not
//!   `ON DUPLICATE KEY UPDATE` (whose affected-rows cannot distinguish insert from
//!   duplicate under `CLIENT_FOUND_ROWS`).
//! - **Timestamps** are `DATETIME(6)` bound/read as UTC `PrimitiveDateTime` (sessions run
//!   at `time_zone = '+00:00'`); the public API stays `OffsetDateTime` in UTC.
//! - **UUIDs** are `BINARY(16)` columns bound directly from [`uuid::Uuid`].

pub mod migrate;
pub mod scope;
pub mod step;
pub mod stores;

use time::{OffsetDateTime, PrimitiveDateTime};

/// MySQL duplicate-key error number (`ER_DUP_ENTRY`) — the first-observer-wins signal.
pub(crate) const ER_DUP_ENTRY: &str = "23000";

/// Whether a sqlx error is the duplicate-key rejection (unique/primary key violation).
///
/// Checks the SQLSTATE (`23000`) rather than the vendor number so MariaDB's aliases stay
/// covered.
pub(crate) fn is_duplicate_key(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == ER_DUP_ENTRY)
}

/// UTC [`OffsetDateTime`] → the naive `DATETIME(6)` bind value (sessions run at +00:00).
pub(crate) fn to_db(t: OffsetDateTime) -> PrimitiveDateTime {
    let utc = t.to_offset(time::UtcOffset::UTC);
    PrimitiveDateTime::new(utc.date(), utc.time())
}

/// Reads a `*_bin`-collated text column. The server flags binary-collated columns as
/// binary on the wire, so they decode as bytes; the stored content is UTF-8 the store
/// layer itself wrote.
pub(crate) fn str_col(row: &sqlx::mysql::MySqlRow, col: &str) -> crate::Result<String> {
    use sqlx::Row as _;
    let bytes: Vec<u8> = row
        .try_get(col)
        .map_err(crate::PersistenceError::db("read column"))?;
    String::from_utf8(bytes).map_err(|e| {
        crate::PersistenceError::InvalidArgument(format!("column {col} is not UTF-8: {e}"))
    })
}

/// Nullable variant of [`str_col`].
pub(crate) fn opt_str_col(row: &sqlx::mysql::MySqlRow, col: &str) -> crate::Result<Option<String>> {
    use sqlx::Row as _;
    let bytes: Option<Vec<u8>> = row
        .try_get(col)
        .map_err(crate::PersistenceError::db("read column"))?;
    bytes
        .map(|bytes| {
            String::from_utf8(bytes).map_err(|e| {
                crate::PersistenceError::InvalidArgument(format!("column {col} is not UTF-8: {e}"))
            })
        })
        .transpose()
}

/// Naive `DATETIME(6)` column value → UTC [`OffsetDateTime`].
pub(crate) fn from_db(t: PrimitiveDateTime) -> OffsetDateTime {
    t.assume_utc()
}
