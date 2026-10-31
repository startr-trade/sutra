//! The `sql`-type data-store provider — `PostgresDataStore` / `PostgresDataStoreTx`
//! (plus the MySQL and SQL Server dialects) over each module's OWN connection, with the
//! `datastores.yaml` loader, on the **two-column cutover model**: a business data-store is an
//! EXTERNAL resource — the module's `datastores.yaml` declares each store's OWN connection
//! (env-indirected, never the engine datasource), and rows key on `(store_name, store_key)`
//! **within the declared connection** — the declaration IS the namespace. There is no
//! `deployment_id` column (a content-addressed id changing on every redeploy would sever the
//! implicit carry-over the declaration model provides across deployment supersession).
//!
//! Semantics follow the data-store SPI contract exactly:
//! - values are opaque JSON text (`serde_json` with `arbitrary_precision`, the `BigDecimal`
//!   analog — a balance never becomes a lossy double);
//! - `get_for_update` takes its row lock via a rev-bumping `UPDATE` (the portable
//!   `SELECT … FOR UPDATE` substitute; a no-op on an absent key);
//! - `revision` is `0` for an absent key, bumped on every write;
//! - `put_if_revision` is the compare-and-set behind `<q:store expect="unchanged">`:
//!   `expected_rev <= 0` expects ABSENT (insert-only; a unique violation = conflict), else a
//!   conditional `UPDATE … AND rev = ?` (0 rows = conflict);
//! - module-resident migrations (`migrations/<store>/*.sql`, see [`migration`] for order and
//!   placeholders) run on first use against the store's own connection, under a cross-replica
//!   lock, and are recorded in the per-database `sutra_store_migrations` ledger so each script
//!   runs once.
#![forbid(unsafe_code)]

pub mod config;
/// The COVERAGE store — the engine-owned coverage schema, served over the connection the author
/// declares. The statement set + the engine-shipped DDL are
/// pure (strings), so they ride the `--no-default-features` build with `config`/`projection`; the
/// three dialect modules execute them.
pub mod coverage;
pub mod error;
/// Key queries (`<q:store query=…>`) on a key/value or typed store — the statement text, bounds and
/// collation rules, pure like `projected`; the three dialect modules execute them.
pub mod keyquery;
/// Module-resident migrations — versions, placeholders, checksums and the migration ledger. Pure,
/// so it rides the `--no-default-features` build; the dialect modules execute it.
pub mod migration;
/// PostgreSQL reference dialect (feature `providers`, on by default). The `config` loader above is
/// pure and always compiles — the deploy-time lint needs only that, so a `--no-default-features`
/// build carries no sqlx/tokio and targets `wasm32-*`.
#[cfg(feature = "providers")]
pub mod postgres;
/// The projected-store RUNTIME — the SQL shapes, value marshalling and first-use table
/// verification a store that declares a `structure` is served through. Pure like `projection`
/// below (the statements are strings, the marshalling is `serde_json`), so it too rides the
/// `--no-default-features` build; the three dialect modules execute what it builds.
pub mod projected;
/// Schema projection — the flat-structure classification and column naming a typed-column store
/// is derived from. Pure (no connection, no dialect, no SQL), so it compiles alongside `config`
/// in a `--no-default-features` lint/wasm build.
pub mod projection;

/// Microsoft SQL Server dialect on the tiberius TDS stack (feature `mssql`, on by default).
#[cfg(feature = "mssql")]
pub mod mssql;
/// MySQL / MariaDB dialect (feature `mysql`, on by default).
#[cfg(feature = "mysql")]
pub mod mysql;

pub use config::{
    load_migrations, parse_datastores, CacheConfig, PoolConfig, StoreDefinition, StructureRef,
    DEFAULT_MAX_CONNECTIONS,
};
#[cfg(feature = "providers")]
pub use coverage::CoverageStore;
pub use coverage::{
    dialect_for_url, shipped_ddl, CoverageCounts, CoverageFragmentRow, CoverageMetrics,
    COVERAGE_STORE_NAME,
};
pub use error::DataStoreError;
pub use keyquery::{
    KeyAggregateCell, KeyAggregateResult, KeyQueryAggregate, KeyQueryPage, KeyRangeQuery,
};
pub use migration::{
    build_migration_scripts, load_migration_scripts, substitute_placeholders,
    MigrationPlaceholders, MigrationScript, StoreMigrations,
};
#[cfg(feature = "mssql")]
pub use mssql::{MssqlDataStore, MssqlDataStoreTx};
#[cfg(feature = "mysql")]
pub use mysql::{MysqlDataStore, MysqlDataStoreTx};
#[cfg(feature = "providers")]
pub use postgres::{PostgresDataStore, PostgresDataStoreTx};
pub use projected::{table_for, ActualColumn, ProjectedStore, SqlDialect, ValueClass};
pub use projection::{
    default_column_name, NamingFault, NamingRules, NotFlatFault, NotFlatReason, ProjectedField,
    Projection, ProjectionError,
};
