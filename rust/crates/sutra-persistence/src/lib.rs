//! Sutra engine persistence layer.
//!
//! The deployment-scoped persistence and instance-snapshot layer, implemented against
//! PostgreSQL — the reference dialect — via sqlx, plus the strict transactional step
//! primitive: outbox enqueue is strictly atomic with the snapshot.
//!
//! Contents:
//!
//! - [`DeploymentId`] — the opaque single-column runtime identity (`dep-<24 hex>`).
//! - [`snapshot`] — the byte-deterministic instance-snapshot codec (Properties-line format).
//! - [`value`] — the TYPED snapshot value model (v4): what one persisted user variable is, and how
//!   its type survives a wait state instead of being flattened to a display string.
//! - [`scope`] — deployment-scoped transactions: per-transaction
//!   `SELECT set_config('sutra.deployment_id', $1, true)` so PostgreSQL Row-Level Security
//!   engages, layered under explicit `deployment_id` binds on every statement (two-layer
//!   enforcement).
//! - [`stores`] — the store traits and their Postgres implementations for the persistence
//!   subsystems (instance, outbox, wait-state, alias, channel-concurrency, inbox, lease).
//! - [`step`] — the commit-at-wait-state transactional step (strict transactional outbox):
//!   snapshot + wait-state rows + alias rows + outbox enqueues commit atomically or not at all.
//! - [`migrate`] — a small ordered migration runner applying the shipped migration SQL
//!   (`V<number>__<description>.sql`, globally-unique version numbers sorted
//!   ascending across subsystem subfolders, mirroring `tools/sutra-migrate`). The ledger
//!   (`sutra_schema_history`) carries a sha256 checksum per applied script for drift
//!   detection; `sutra migrate` consumes this module as a library.

#![forbid(unsafe_code)]
#![allow(async_fn_in_trait)]

mod deployment;
mod error;
pub mod migrate;
mod props;
pub mod scope;
pub mod snapshot;
pub mod step;
pub mod stores;
pub mod value;

// The non-reference SQL dialects (PostgreSQL above is the reference dialect, and is frozen).
// Additive per-dialect modules; semantics are normative, syntax is not. MariaDB rides the
// `mysql` module (shared SQL surface, separately container-proven).
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;

pub use deployment::DeploymentId;
pub use error::PersistenceError;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, PersistenceError>;
