//! The coverage store — engine-owned SCHEMA on the connection the AUTHOR declares.
//!
//! **SUPERSEDING RULING 2026-08-04**. Coverage marks are
//! typed rows with SQL-aggregate counts, and they live in the user-declared `coverage` data store:
//! the author names the store in `datastores.yaml`, its data source decides the database and
//! therefore the dialect, and the engine hosts nothing of coverage in its own database. What the
//! author does NOT supply is coverage SQL — coverage tables are an in-built feature, so the
//! engine ships their DDL ([`shipped_ddl`], embedded from `migrations/coverage/<dialect>/`) and
//! applies it to that connection on first use, under the same cross-replica lock a module's own
//! `migrations/<store>/` scripts take — but on the idempotent, LEDGER-LESS path: the DDL is
//! engine-owned and re-applied on every first use, so it is never recorded in the
//! `sutra_store_migrations` ledger the package scripts are.
//!
//! Two tables, both keyed by `deployment_id` (a column AND a bound predicate on every statement —
//! the isolation posture; no RLS on a user-owned connection, see the shipped DDL):
//!
//! 1. `coverage_metric(deployment_id, path_urn, covered)` — every declared coverage path
//!    (intra- and cross-process, by fully-qualified URN) seeded `covered=false` at deploy /
//!    `coverage init` / reset (the "total to cover"), flipped `true` when exercised. `total` /
//!    `covered` / `coverage_percentage` + the uncovered set derive straight off these flags as SQL
//!    aggregates, never a select-every-row-and-fold-in-Rust.
//! 2. `coverage_fragment(deployment_id, route_urn, segment_process, instance_id, business_key,
//!    trace_id, …)` — one cross-process reconstruction fragment per injected-segment completion,
//!    the union-find reconstruction input. Deliberately decoupled from the audit stream.
//!
//! Those rows are what "no cycle" means. Compliance cycles keep theirs beside them, in
//! `coverage_cycle_metric` and `coverage_cycle_fragment`, with `coverage_cycle` as the registry and
//! `coverage_active_cycle` holding the one active label per deployment (see [`CoverageRowScope`]).
//!
//! ## Portability, and the two properties it must not cost
//!
//! The first cut of this store was PostgreSQL-only (`count(*) FILTER (WHERE covered)`,
//! `array_agg(… ORDER BY …) FILTER`) because it rode the engine's own pool. A user-declared store
//! can be any of the three shipped dialects, so the statements are built here from
//! [`SqlDialect`] — the same parameterised-builder seam [`crate::projected`] uses — and both
//! correctness properties of the PG-only version are preserved deliberately:
//!
//! - **Counts and the uncovered list come from a consistent snapshot.** PostgreSQL gave that for
//!   free (one statement = one snapshot); `COUNT(CASE WHEN …)` + a separate ordered list query
//!   cannot. So the read path runs both statements inside ONE `REPEATABLE READ` transaction,
//!   which every shipped dialect supports and which is strictly stronger than the single-statement
//!   guarantee it replaces. `COUNT(CASE WHEN … THEN 1 END)` is the portable pivot (it returns a
//!   count type on all three; `SUM(CASE …)` would return DECIMAL on MySQL and NULL on an empty
//!   table).
//! - **First-covers-wins stays durable, and stays in the write.** [`mark_update_sql`] flips the
//!   flag with `… AND NOT covered` in the predicate, so the row count IS the answer to "did THIS
//!   call newly cover it" — one statement, no read-then-write race. Only when it matches nothing
//!   does [`mark_insert_sql`] run (an unseeded path), and its conflict branch answers the same
//!   question: inserted ⇒ newly covered, duplicate ⇒ someone else got there first.
//!
//! Long path sets are chunked ([`IN_CHUNK`]) because SQL Server caps a statement at 2100
//! parameters; the chunks run inside one transaction, so the set they answer for is still one
//! snapshot.

use crate::error::DataStoreError;
use crate::projected::SqlDialect;

/// The reserved store name coverage is persisted in. `sutra lint` errors when a deployment
/// declares `<q:coverage>` paths without it.
pub const COVERAGE_STORE_NAME: &str = "coverage";

/// How many path URNs go into one `IN (…)` list. SQL Server's hard cap is 2100 parameters per
/// statement; 500 leaves ample headroom on every dialect and keeps a large declaration's read to a
/// handful of round trips.
pub const IN_CHUNK: usize = 500;

// ---- the engine-shipped DDL ---------------------------------------------------------------

/// PostgreSQL coverage DDL, embedded at build time (the reference dialect).
const DDL_POSTGRES: [&str; 3] = [
    include_str!("../migrations/coverage/postgres/V901__coverage_metric.sql"),
    include_str!("../migrations/coverage/postgres/V902__coverage_fragment.sql"),
    include_str!("../migrations/coverage/postgres/V904__coverage_cycle.sql"),
];

/// MySQL / MariaDB coverage DDL.
const DDL_MYSQL: [&str; 3] = [
    include_str!("../migrations/coverage/mysql/V901__coverage_metric.sql"),
    include_str!("../migrations/coverage/mysql/V902__coverage_fragment.sql"),
    include_str!("../migrations/coverage/mysql/V904__coverage_cycle.sql"),
];

/// SQL Server coverage DDL.
const DDL_MSSQL: [&str; 3] = [
    include_str!("../migrations/coverage/mssql/V901__coverage_metric.sql"),
    include_str!("../migrations/coverage/mssql/V902__coverage_fragment.sql"),
    include_str!("../migrations/coverage/mssql/V904__coverage_cycle.sql"),
];

/// The engine-shipped coverage DDL for `dialect`, in apply order.
///
/// These are the ENGINE's scripts, not the package's: they are handed to the store's ordinary
/// first-use migration path (advisory/named/app-lock serialised across replicas) as
/// [`crate::migration::StoreMigrations::Idempotent`] — never recorded in the ledger — which
/// is why every statement in them is idempotent. A deployment package ships no coverage SQL at
/// all — `migrations/coverage/` does not exist any more.
pub fn shipped_ddl(dialect: SqlDialect) -> Vec<String> {
    let scripts = match dialect {
        SqlDialect::Postgres => DDL_POSTGRES,
        SqlDialect::Mysql => DDL_MYSQL,
        SqlDialect::Mssql => DDL_MSSQL,
    };
    scripts.iter().map(|s| (*s).to_string()).collect()
}

/// The connection dialect a store's `sql.url` names, taken from the URL scheme — the same
/// classification every business store gets (`postgres(ql)`, `mysql`/`mariadb`,
/// `sqlserver`/`mssql`). An unrecognised scheme is a fail-closed config error.
pub fn dialect_for_url(url: &str) -> Result<SqlDialect, DataStoreError> {
    let scheme = url
        .split([':', '/'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match scheme.as_str() {
        "postgres" | "postgresql" => Ok(SqlDialect::Postgres),
        "mysql" | "mariadb" => Ok(SqlDialect::Mysql),
        "sqlserver" | "mssql" => Ok(SqlDialect::Mssql),
        other => Err(DataStoreError::new(format!(
            "unsupported data-store connection scheme '{other}' (expected one of \
             postgres/postgresql, mysql/mariadb, sqlserver/mssql)"
        ))),
    }
}

// ---- row / metric shapes -------------------------------------------------------------------

/// One reconstruction-fragment row for `coverage_fragment`. Domain-neutral: `business_key` is an
/// author-declared correlation value, `trace_id` the W3C traceparent — no domain semantics enter
/// the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageFragmentRow {
    /// Fully-qualified coverage-route URN (`urn:sutra:coverage:<file>:<path>`).
    pub route_urn: String,
    /// The process whose injected segment completed.
    pub segment_process: String,
    /// The completing instance's id (string form — not necessarily a UUID; kept TEXT for
    /// portability and to avoid coupling to the engine's instance-id shape).
    pub instance_id: String,
    /// Per-hop business key observed at this segment (`None` when absent).
    pub business_key: Option<String>,
    /// W3C trace-id observed at this segment (`None` when absent).
    pub trace_id: Option<String>,
}

/// The counts alone — `total` / `covered` — for callers that never list the uncovered set.
/// One aggregate; no rows cross the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CoverageCounts {
    /// Total declared paths (every seeded URN).
    pub total: u64,
    /// How many are `covered = true`.
    pub covered: u64,
}

impl CoverageCounts {
    /// Coverage percentage (two-decimal, `0.0` for an empty declaration) — matches the executor's
    /// `CoverageMetrics::coverage_percentage` and the existing `coverage:report` math.
    pub fn coverage_percentage(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.covered as f64 * 10000.0 / self.total as f64).round() / 100.0
        }
    }
}

/// Derived coverage metrics for a deployment — read straight off the `coverage_metric` flags.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoverageMetrics {
    /// Total declared paths (every seeded URN).
    pub total: u64,
    /// How many are `covered = true`.
    pub covered: u64,
    /// The still-uncovered path URNs (`covered = false`), ascending for determinism.
    pub uncovered: Vec<String>,
}

impl CoverageMetrics {
    /// Coverage percentage (two-decimal, `0.0` for an empty declaration).
    pub fn coverage_percentage(&self) -> f64 {
        self.counts().coverage_percentage()
    }

    /// The count pair, so the two shapes agree by construction.
    pub fn counts(&self) -> CoverageCounts {
        CoverageCounts {
            total: self.total,
            covered: self.covered,
        }
    }
}

// ---- the statement set, per dialect ----------------------------------------------------------

/// The `covered` flag's literal value in this dialect: PostgreSQL has a real boolean type;
/// MySQL's `BOOLEAN` is `TINYINT` and SQL Server's `BIT` is not a predicate, so both spell it `1`
/// / `0`.
fn flag(dialect: SqlDialect, value: bool) -> &'static str {
    match (dialect, value) {
        (SqlDialect::Postgres, true) => "true",
        (SqlDialect::Postgres, false) => "false",
        (_, true) => "1",
        (_, false) => "0",
    }
}

/// The predicate form of the flag — `covered` / `NOT covered` on PostgreSQL, an equality test
/// elsewhere (a `BIT` column is a value, not a condition, in T-SQL).
fn flag_predicate(dialect: SqlDialect, value: bool) -> String {
    match dialect {
        SqlDialect::Postgres => {
            if value {
                "covered".to_string()
            } else {
                "NOT covered".to_string()
            }
        }
        _ => format!("covered = {}", flag(dialect, value)),
    }
}

/// Which rows a statement addresses: the deployment's plain rows, which are what "no cycle" means,
/// or one compliance cycle's. The two live in separate tables, so an in-cycle statement differs in
/// its table and in carrying `cycle_label` as a second key, always bound straight after the
/// deployment id. A no-cycle statement is exactly what it was before cycles existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageRowScope {
    /// `coverage_metric` / `coverage_fragment`.
    NoCycle,
    /// `coverage_cycle_metric` / `coverage_cycle_fragment`.
    InCycle,
}

impl CoverageRowScope {
    fn metric_table(self) -> &'static str {
        match self {
            CoverageRowScope::NoCycle => "coverage_metric",
            CoverageRowScope::InCycle => "coverage_cycle_metric",
        }
    }

    fn fragment_table(self) -> &'static str {
        match self {
            CoverageRowScope::NoCycle => "coverage_fragment",
            CoverageRowScope::InCycle => "coverage_cycle_fragment",
        }
    }

    fn key_columns(self) -> &'static str {
        match self {
            CoverageRowScope::NoCycle => "deployment_id",
            CoverageRowScope::InCycle => "deployment_id, cycle_label",
        }
    }

    /// How many key binds lead the statement.
    fn key_count(self) -> usize {
        match self {
            CoverageRowScope::NoCycle => 1,
            CoverageRowScope::InCycle => 2,
        }
    }

    /// The placeholders for [`Self::key_columns`], comma-separated.
    fn key_binds(self, dialect: SqlDialect) -> String {
        (1..=self.key_count())
            .map(|n| dialect.placeholder(n))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The `WHERE` predicate that isolates this scope's rows.
    fn key_predicate(self, dialect: SqlDialect) -> String {
        match self {
            CoverageRowScope::NoCycle => format!("deployment_id = {}", dialect.placeholder(1)),
            CoverageRowScope::InCycle => format!(
                "deployment_id = {} AND cycle_label = {}",
                dialect.placeholder(1),
                dialect.placeholder(2)
            ),
        }
    }
}

/// Insert one flag row with the given value. PostgreSQL states the conflict branch inline; the
/// other two dialects let the duplicate-key rejection say it (their drivers surface it as a
/// catchable error instead of poisoning the transaction).
fn insert_flag_sql(dialect: SqlDialect, scope: CoverageRowScope, value: bool) -> String {
    let columns = scope.key_columns();
    let path = dialect.placeholder(scope.key_count() + 1);
    let base = format!(
        "INSERT INTO {} ({columns}, path_urn, covered) VALUES ({}, {path}, {})",
        scope.metric_table(),
        scope.key_binds(dialect),
        flag(dialect, value)
    );
    match dialect {
        SqlDialect::Postgres => format!("{base} ON CONFLICT ({columns}, path_urn) DO NOTHING"),
        _ => base,
    }
}

/// Seed one declared path as uncovered.
pub fn seed_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    insert_flag_sql(dialect, scope, false)
}

/// Flip a seeded path to covered. The `AND NOT covered` guard makes the affected-row count the
/// durable first-covers-wins answer: 1 = THIS call covered it, 0 = it was already covered (or the
/// row does not exist yet, which [`mark_insert_sql`] then settles).
pub fn mark_update_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "UPDATE {} SET covered = {} WHERE {} AND path_urn = {} AND {}",
        scope.metric_table(),
        flag(dialect, true),
        scope.key_predicate(dialect),
        dialect.placeholder(scope.key_count() + 1),
        flag_predicate(dialect, false)
    )
}

/// Insert an already-covered path (a mark on a path that was never seeded). A conflict means a
/// concurrent marker won the race, i.e. this call did NOT newly cover it.
pub fn mark_insert_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    insert_flag_sql(dialect, scope, true)
}

/// `total` + `covered` as one aggregate. `COUNT(CASE WHEN … THEN 1 END)` is the portable pivot for
/// the PostgreSQL-only `count(*) FILTER (WHERE covered)`: NULLs are not counted, so the CASE with
/// no ELSE counts exactly the covered rows, and the result stays a count type on every dialect.
/// Column order is positional: 0 = total, 1 = covered.
pub fn counts_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "SELECT COUNT(*) AS total, COUNT(CASE WHEN {} THEN 1 END) AS covered FROM {} WHERE {}",
        flag_predicate(dialect, true),
        scope.metric_table(),
        scope.key_predicate(dialect)
    )
}

/// The still-uncovered path URNs, ascending. Its own ordered query rather than an `array_agg(…
/// ORDER BY …) FILTER` — run in the same `REPEATABLE READ` transaction as [`counts_sql`], so the
/// pair still describes one snapshot.
pub fn uncovered_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "SELECT path_urn FROM {} WHERE {} AND {} ORDER BY path_urn",
        scope.metric_table(),
        scope.key_predicate(dialect),
        flag_predicate(dialect, false)
    )
}

/// Which of `count` caller-supplied paths are covered — the `coverage:report` substrate, one round
/// trip per chunk instead of one `get` per declared path. A path with no row is simply absent from
/// the result, i.e. reads as uncovered.
pub fn covered_among_sql(dialect: SqlDialect, scope: CoverageRowScope, count: usize) -> String {
    format!(
        "SELECT path_urn FROM {} WHERE {} AND {} AND path_urn IN {}",
        scope.metric_table(),
        scope.key_predicate(dialect),
        flag_predicate(dialect, true),
        in_list(dialect, count, scope.key_count() + 1)
    )
}

/// Clear the covered flag on the named paths (the rows stay seeded). Only rows actually covered
/// are touched, so the affected-row count IS the `cleared` count.
pub fn clear_paths_sql(dialect: SqlDialect, scope: CoverageRowScope, count: usize) -> String {
    format!(
        "UPDATE {} SET covered = {} WHERE {} AND {} AND path_urn IN {}",
        scope.metric_table(),
        flag(dialect, false),
        scope.key_predicate(dialect),
        flag_predicate(dialect, true),
        in_list(dialect, count, scope.key_count() + 1)
    )
}

/// Re-seed every declared path of the scope to uncovered.
pub fn reset_metrics_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "UPDATE {} SET covered = {} WHERE {}",
        scope.metric_table(),
        flag(dialect, false),
        scope.key_predicate(dialect)
    )
}

/// Drop the scope's reconstruction fragments (the other half of a reset).
pub fn delete_fragments_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        scope.fragment_table(),
        scope.key_predicate(dialect)
    )
}

/// Append one reconstruction fragment.
pub fn write_fragment_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    let binds: Vec<String> = (1..=scope.key_count() + 5)
        .map(|n| dialect.placeholder(n))
        .collect();
    format!(
        "INSERT INTO {} ({}, route_urn, segment_process, instance_id, business_key, trace_id) \
         VALUES ({})",
        scope.fragment_table(),
        scope.key_columns(),
        binds.join(", ")
    )
}

/// Every fragment of the scope in insertion order — the union-find input.
pub fn read_fragments_sql(dialect: SqlDialect, scope: CoverageRowScope) -> String {
    format!(
        "SELECT route_urn, segment_process, instance_id, business_key, trace_id FROM {} WHERE {} \
         ORDER BY id",
        scope.fragment_table(),
        scope.key_predicate(dialect)
    )
}

// ---- the cycle registry statements. Two binds at most, all of them labels or ids. --------------

/// Declare a cycle: `[deployment_id, cycle_label]`. A repeat declaration inserts nothing.
pub fn declare_cycle_sql(dialect: SqlDialect) -> String {
    let base = format!(
        "INSERT INTO coverage_cycle (deployment_id, cycle_label) VALUES ({}, {})",
        dialect.placeholder(1),
        dialect.placeholder(2)
    );
    match dialect {
        SqlDialect::Postgres => {
            format!("{base} ON CONFLICT (deployment_id, cycle_label) DO NOTHING")
        }
        _ => base,
    }
}

/// Whether a cycle is declared: `[deployment_id, cycle_label]`, one row or none.
pub fn cycle_declared_sql(dialect: SqlDialect) -> String {
    format!(
        "SELECT cycle_label FROM coverage_cycle WHERE deployment_id = {} AND cycle_label = {}",
        dialect.placeholder(1),
        dialect.placeholder(2)
    )
}

/// Every declared cycle of a deployment, oldest first: `[deployment_id]`.
pub fn list_cycles_sql(dialect: SqlDialect) -> String {
    format!(
        "SELECT cycle_label FROM coverage_cycle WHERE deployment_id = {} \
         ORDER BY declared_at, cycle_label",
        dialect.placeholder(1)
    )
}

/// Claim the deployment's single active slot: `[deployment_id, cycle_label]`. The primary key
/// answers the race, so one affected row means this call holds the slot.
pub fn claim_active_cycle_sql(dialect: SqlDialect) -> String {
    let base = format!(
        "INSERT INTO coverage_active_cycle (deployment_id, cycle_label) VALUES ({}, {})",
        dialect.placeholder(1),
        dialect.placeholder(2)
    );
    match dialect {
        SqlDialect::Postgres => format!("{base} ON CONFLICT (deployment_id) DO NOTHING"),
        _ => base,
    }
}

/// The deployment's active cycle: `[deployment_id]`, one row or none.
pub fn active_cycle_sql(dialect: SqlDialect) -> String {
    format!(
        "SELECT cycle_label FROM coverage_active_cycle WHERE deployment_id = {}",
        dialect.placeholder(1)
    )
}

/// Release the active slot if this cycle holds it: `[deployment_id, cycle_label]`.
pub fn release_active_cycle_sql(dialect: SqlDialect) -> String {
    format!(
        "DELETE FROM coverage_active_cycle WHERE deployment_id = {} AND cycle_label = {}",
        dialect.placeholder(1),
        dialect.placeholder(2)
    )
}

/// A parenthesised placeholder list of `count` items starting at parameter `from`.
fn in_list(dialect: SqlDialect, count: usize, from: usize) -> String {
    let binds: Vec<String> = (0..count)
        .map(|i| dialect.placeholder(from + i))
        .collect::<Vec<_>>();
    format!("({})", binds.join(", "))
}

/// Fail-closed guard on the deployment id every statement binds. The column is `VARCHAR(64)`, so
/// an over-long or empty id is a typed error here rather than a driver-level truncation surprise.
pub fn check_deployment_id(deployment_id: &str) -> Result<(), DataStoreError> {
    if deployment_id.is_empty() || deployment_id.len() > 64 {
        return Err(DataStoreError::new(format!(
            "coverage store: invalid deployment id '{deployment_id}' (1..=64 characters)"
        )));
    }
    Ok(())
}

/// The longest cycle label, matching the `cycle_label` columns.
pub const CYCLE_LABEL_MAX_CHARS: usize = 128;

/// Fail-closed guard on a cycle label. Labels are free-form, so this refuses only what could not
/// be told apart in a report or stored whole: a blank label, one with control characters, and one
/// longer than the column.
pub fn check_cycle_label(label: &str) -> Result<(), DataStoreError> {
    if label.trim().is_empty()
        || label.chars().any(char::is_control)
        || label.chars().count() > CYCLE_LABEL_MAX_CHARS
    {
        return Err(DataStoreError::new(format!(
            "coverage store: invalid cycle label '{label}' (non-blank, no control characters, \
             1..={CYCLE_LABEL_MAX_CHARS} characters)"
        )));
    }
    Ok(())
}

/// What asking to activate a cycle came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageCycleActivation {
    /// The cycle now holds the deployment's active slot. Reactivating an earlier cycle lands here
    /// too, and continues it: covered flags are set-only, so its marks are still there.
    Activated,
    /// The cycle already held the slot, so nothing changed.
    AlreadyActive,
    /// Another cycle holds the slot, and it was left active. Deactivate it first.
    Refused {
        /// The cycle that holds the slot.
        active_cycle: String,
    },
    /// No such cycle has been declared on this deployment.
    Undeclared,
}

// ---- the dialect-dispatching store ----------------------------------------------------------

/// The coverage store, bound to whatever dialect the declared `coverage` store's connection names
/// — the coverage counterpart of the business stores' `StoreBackend`. Cheap to clone (every
/// dialect store shares one pool), so the engine can hold one per deployment.
///
/// Built from the deployment's OWN `datastores.yaml` declaration, so the author chooses the
/// database; the schema and the statements are the engine's.
#[cfg(feature = "providers")]
#[derive(Clone)]
pub enum CoverageStore {
    /// PostgreSQL (the reference dialect).
    Pg(crate::postgres::PostgresCoverageStore),
    /// MySQL / MariaDB.
    #[cfg(feature = "mysql")]
    Mysql(crate::mysql::MysqlCoverageStore),
    /// Microsoft SQL Server.
    #[cfg(feature = "mssql")]
    Mssql(crate::mssql::MssqlCoverageStore),
}

#[cfg(feature = "providers")]
impl CoverageStore {
    /// Build the coverage store from the deployment's `coverage` store declaration, dispatching on
    /// the resolved connection URL's scheme exactly as a business store does. Fails closed: an
    /// unresolvable connection (env-ref unset), an unsupported scheme, or a dialect this build
    /// excluded is an `Err`, never a silently coverage-less deployment.
    pub fn from_definition(def: &crate::config::StoreDefinition) -> Result<Self, DataStoreError> {
        let url = def
            .resolved("sql.url")?
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                DataStoreError::new(format!(
                    "coverage store '{}' declares no usable connection in datastores.yaml \
                     (sql.url + credentials, or *-ref pointing at a set env var). Coverage marks \
                     are persisted in this store — the engine owns its schema, but not its \
                     location.",
                    def.name
                ))
            })?;
        match dialect_for_url(&url)? {
            SqlDialect::Postgres => Ok(CoverageStore::Pg(
                crate::postgres::PostgresCoverageStore::from_definition(def)?,
            )),
            #[cfg(feature = "mysql")]
            SqlDialect::Mysql => Ok(CoverageStore::Mysql(
                crate::mysql::MysqlCoverageStore::from_definition(def)?,
            )),
            #[cfg(not(feature = "mysql"))]
            SqlDialect::Mysql => Err(unsupported_dialect(&def.name, "mysql/mariadb")),
            #[cfg(feature = "mssql")]
            SqlDialect::Mssql => Ok(CoverageStore::Mssql(
                crate::mssql::MssqlCoverageStore::from_definition(def)?,
            )),
            #[cfg(not(feature = "mssql"))]
            SqlDialect::Mssql => Err(unsupported_dialect(&def.name, "sqlserver/mssql")),
        }
    }

    /// The declared store name (the author's — always `coverage` in practice).
    pub fn name(&self) -> &str {
        dispatch!(self, s => s.name())
    }

    /// The same store, its metric and fragment methods addressing `cycle`'s rows, or the plain
    /// "no cycle" rows for `None`. Cheap: the handle shares its pool.
    pub fn in_cycle(&self, cycle: Option<&str>) -> CoverageStore {
        match self {
            CoverageStore::Pg(s) => CoverageStore::Pg(s.in_cycle(cycle)),
            #[cfg(feature = "mysql")]
            CoverageStore::Mysql(s) => CoverageStore::Mysql(s.in_cycle(cycle)),
            #[cfg(feature = "mssql")]
            CoverageStore::Mssql(s) => CoverageStore::Mssql(s.in_cycle(cycle)),
        }
    }

    /// The dialect of this store's connection.
    fn dialect(&self) -> SqlDialect {
        match self {
            CoverageStore::Pg(_) => SqlDialect::Postgres,
            #[cfg(feature = "mysql")]
            CoverageStore::Mysql(_) => SqlDialect::Mysql,
            #[cfg(feature = "mssql")]
            CoverageStore::Mssql(_) => SqlDialect::Mssql,
        }
    }

    /// Run a cycle-registry write; the affected-row count, with a duplicate key counting as zero.
    async fn cycle_write(
        &self,
        sql: &str,
        binds: &[&str],
        op: &'static str,
    ) -> Result<u64, DataStoreError> {
        dispatch!(self, s => s.cycle_write(sql, binds, op).await)
    }

    /// Run a cycle-registry read; the labels of its single text column.
    async fn cycle_labels(
        &self,
        sql: &str,
        binds: &[&str],
        op: &'static str,
    ) -> Result<Vec<String>, DataStoreError> {
        dispatch!(self, s => s.cycle_labels(sql, binds, op).await)
    }

    /// Declare a cycle on a deployment. `true` when this call declared it, `false` when it already
    /// was. Declaring records nothing by itself: only activation routes coverage to the cycle.
    pub async fn declare_cycle(
        &self,
        deployment_id: &str,
        label: &str,
    ) -> Result<bool, DataStoreError> {
        check_deployment_id(deployment_id)?;
        check_cycle_label(label)?;
        let sql = declare_cycle_sql(self.dialect());
        Ok(self
            .cycle_write(&sql, &[deployment_id, label], "cycle declare")
            .await?
            > 0)
    }

    /// Whether a cycle is declared on a deployment.
    pub async fn cycle_is_declared(
        &self,
        deployment_id: &str,
        label: &str,
    ) -> Result<bool, DataStoreError> {
        check_deployment_id(deployment_id)?;
        check_cycle_label(label)?;
        let sql = cycle_declared_sql(self.dialect());
        Ok(!self
            .cycle_labels(&sql, &[deployment_id, label], "cycle lookup")
            .await?
            .is_empty())
    }

    /// Every cycle declared on a deployment, oldest first.
    pub async fn list_cycles(&self, deployment_id: &str) -> Result<Vec<String>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let sql = list_cycles_sql(self.dialect());
        self.cycle_labels(&sql, &[deployment_id], "cycle list")
            .await
    }

    /// The deployment's active cycle, if any. Read on every mark rather than cached, so an
    /// operator's activation takes effect at once on every replica with nothing to signal.
    pub async fn active_cycle(
        &self,
        deployment_id: &str,
    ) -> Result<Option<String>, DataStoreError> {
        check_deployment_id(deployment_id)?;
        let sql = active_cycle_sql(self.dialect());
        Ok(self
            .cycle_labels(&sql, &[deployment_id], "active cycle")
            .await?
            .into_iter()
            .next())
    }

    /// Activate a declared cycle. Refuses while another cycle is active: switching would move the
    /// marks of whatever is still running under that cycle, with nothing to tell its owner.
    pub async fn activate_cycle(
        &self,
        deployment_id: &str,
        label: &str,
    ) -> Result<CoverageCycleActivation, DataStoreError> {
        if !self.cycle_is_declared(deployment_id, label).await? {
            return Ok(CoverageCycleActivation::Undeclared);
        }
        let claim = claim_active_cycle_sql(self.dialect());
        // A holder can release between a lost claim and the read of its label; claim again then.
        // Only a sustained stream of activations could use all three tries.
        for _ in 0..3 {
            if self
                .cycle_write(&claim, &[deployment_id, label], "cycle activate")
                .await?
                > 0
            {
                return Ok(CoverageCycleActivation::Activated);
            }
            match self.active_cycle(deployment_id).await? {
                Some(active) if active == label => {
                    return Ok(CoverageCycleActivation::AlreadyActive)
                }
                Some(active_cycle) => return Ok(CoverageCycleActivation::Refused { active_cycle }),
                None => {}
            }
        }
        Err(DataStoreError::new(format!(
            "coverage store: could not activate cycle '{label}': the active slot of deployment \
             {deployment_id} kept changing"
        )))
    }

    /// Deactivate a cycle. `true` when it held the active slot; `false` when it did not, which
    /// leaves any other active cycle alone.
    pub async fn deactivate_cycle(
        &self,
        deployment_id: &str,
        label: &str,
    ) -> Result<bool, DataStoreError> {
        check_deployment_id(deployment_id)?;
        check_cycle_label(label)?;
        let sql = release_active_cycle_sql(self.dialect());
        Ok(self
            .cycle_write(&sql, &[deployment_id, label], "cycle deactivate")
            .await?
            > 0)
    }

    /// Idempotently seed every declared path as uncovered — the "total to cover".
    pub async fn seed_declared(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, DataStoreError> {
        dispatch!(self, s => s.seed_declared(deployment_id, path_urns).await)
    }

    /// Flip a declared path to covered; `true` when THIS call newly covered it.
    pub async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, DataStoreError> {
        dispatch!(self, s => s.mark_path_covered(deployment_id, path_urn).await)
    }

    /// The subset of `path_urns` currently covered.
    pub async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<std::collections::BTreeSet<String>, DataStoreError> {
        dispatch!(self, s => s.covered_among(deployment_id, path_urns).await)
    }

    /// Clear the covered flag on the named paths; returns how many were actually flipped.
    pub async fn clear_paths(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, DataStoreError> {
        dispatch!(self, s => s.clear_paths(deployment_id, path_urns).await)
    }

    /// The counts alone (`total` / `covered`).
    pub async fn count_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageCounts, DataStoreError> {
        dispatch!(self, s => s.count_metrics(deployment_id).await)
    }

    /// Counts + the uncovered set, from one consistent snapshot.
    pub async fn read_metrics(
        &self,
        deployment_id: &str,
    ) -> Result<CoverageMetrics, DataStoreError> {
        dispatch!(self, s => s.read_metrics(deployment_id).await)
    }

    /// Append a reconstruction fragment.
    pub async fn write_fragment(
        &self,
        deployment_id: &str,
        row: &CoverageFragmentRow,
    ) -> Result<(), DataStoreError> {
        dispatch!(self, s => s.write_fragment(deployment_id, row).await)
    }

    /// All reconstruction fragments for the deployment, in insertion order.
    pub async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragmentRow>, DataStoreError> {
        dispatch!(self, s => s.read_fragments(deployment_id).await)
    }

    /// Re-seed every declared path to uncovered and drop the deployment's fragments.
    pub async fn reset(&self, deployment_id: &str) -> Result<(), DataStoreError> {
        dispatch!(self, s => s.reset(deployment_id).await)
    }
}

/// Dispatch one expression across every compiled-in dialect variant.
#[cfg(feature = "providers")]
macro_rules! dispatch {
    ($self:expr, $store:ident => $body:expr) => {
        match $self {
            CoverageStore::Pg($store) => $body,
            #[cfg(feature = "mysql")]
            CoverageStore::Mysql($store) => $body,
            #[cfg(feature = "mssql")]
            CoverageStore::Mssql($store) => $body,
        }
    };
}
#[cfg(feature = "providers")]
use dispatch;

/// A store whose dialect this build excluded — a lean build must say so, not go silently
/// coverage-less.
#[cfg(all(feature = "providers", not(all(feature = "mysql", feature = "mssql"))))]
fn unsupported_dialect(store: &str, dialect: &str) -> DataStoreError {
    DataStoreError::new(format!(
        "coverage store '{store}' names a {dialect} connection, but this engine build excludes \
         that dialect"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_ddl_is_embedded_and_idempotent_per_dialect() {
        for dialect in [SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Mssql] {
            let scripts = shipped_ddl(dialect);
            assert_eq!(
                scripts.len(),
                3,
                "metric + fragment + cycle DDL for {dialect:?}"
            );
            assert!(scripts.iter().all(|s| s.contains("coverage_metric")
                || s.contains("coverage_fragment")
                || s.contains("coverage_cycle")));
            // Re-runnable on every dialect: the engine applies these ledger-lessly on first use.
            let guarded = match dialect {
                SqlDialect::Mssql => "IF OBJECT_ID",
                _ => "CREATE TABLE IF NOT EXISTS",
            };
            assert!(
                scripts.iter().all(|s| s.contains(guarded)),
                "{dialect:?} DDL must be idempotent"
            );
            // No RLS on a user-owned connection (§7).
            assert!(!scripts.iter().any(|s| s.contains("ROW LEVEL SECURITY")));
        }
    }

    #[test]
    fn dialect_comes_from_the_url_scheme() {
        assert_eq!(
            dialect_for_url("postgresql://h/db").unwrap(),
            SqlDialect::Postgres
        );
        assert_eq!(
            dialect_for_url("mariadb://h/db").unwrap(),
            SqlDialect::Mysql
        );
        assert_eq!(
            dialect_for_url("sqlserver://h;databaseName=db").unwrap(),
            SqlDialect::Mssql
        );
        assert!(dialect_for_url("oracle://h/db").is_err());
    }

    #[test]
    fn the_mark_is_a_guarded_flip_not_a_read_then_write() {
        // The guard is what makes the affected-row count the first-covers-wins answer.
        assert!(
            mark_update_sql(SqlDialect::Postgres, CoverageRowScope::NoCycle)
                .ends_with("AND NOT covered")
        );
        assert!(
            mark_update_sql(SqlDialect::Mysql, CoverageRowScope::NoCycle)
                .ends_with("AND covered = 0")
        );
        assert!(
            mark_update_sql(SqlDialect::Mssql, CoverageRowScope::NoCycle)
                .ends_with("AND covered = 0")
        );
        // …and the insert half never overwrites a concurrent winner.
        assert!(
            mark_insert_sql(SqlDialect::Postgres, CoverageRowScope::NoCycle)
                .contains("ON CONFLICT")
        );
        assert!(!mark_insert_sql(SqlDialect::Mysql, CoverageRowScope::NoCycle).contains("IGNORE"));
    }

    #[test]
    fn the_counts_are_a_portable_aggregate() {
        assert_eq!(
            counts_sql(SqlDialect::Postgres, CoverageRowScope::NoCycle),
            "SELECT COUNT(*) AS total, COUNT(CASE WHEN covered THEN 1 END) AS covered FROM \
             coverage_metric WHERE deployment_id = $1"
        );
        assert_eq!(
            counts_sql(SqlDialect::Mysql, CoverageRowScope::NoCycle),
            "SELECT COUNT(*) AS total, COUNT(CASE WHEN covered = 1 THEN 1 END) AS covered FROM \
             coverage_metric WHERE deployment_id = ?"
        );
        assert_eq!(
            counts_sql(SqlDialect::Mssql, CoverageRowScope::NoCycle),
            "SELECT COUNT(*) AS total, COUNT(CASE WHEN covered = 1 THEN 1 END) AS covered FROM \
             coverage_metric WHERE deployment_id = @P1"
        );
        // No PostgreSQL-only spelling survives anywhere in the statement set.
        for dialect in [SqlDialect::Postgres, SqlDialect::Mysql, SqlDialect::Mssql] {
            for sql in [
                counts_sql(dialect, CoverageRowScope::NoCycle),
                uncovered_sql(dialect, CoverageRowScope::NoCycle),
                covered_among_sql(dialect, CoverageRowScope::NoCycle, 3),
                clear_paths_sql(dialect, CoverageRowScope::NoCycle, 2),
            ] {
                assert!(!sql.contains("FILTER (WHERE"), "{sql}");
                assert!(!sql.contains("array_agg"), "{sql}");
                assert!(!sql.contains("= ANY("), "{sql}");
            }
        }
    }

    #[test]
    fn in_lists_are_numbered_per_dialect() {
        assert!(
            covered_among_sql(SqlDialect::Postgres, CoverageRowScope::NoCycle, 3)
                .ends_with("IN ($2, $3, $4)")
        );
        assert!(
            covered_among_sql(SqlDialect::Mysql, CoverageRowScope::NoCycle, 3)
                .ends_with("IN (?, ?, ?)")
        );
        assert!(
            covered_among_sql(SqlDialect::Mssql, CoverageRowScope::NoCycle, 3)
                .ends_with("IN (@P2, @P3, @P4)")
        );
        assert!(
            clear_paths_sql(SqlDialect::Mssql, CoverageRowScope::NoCycle, 2)
                .ends_with("IN (@P2, @P3)")
        );
    }

    #[test]
    fn the_uncovered_list_is_ordered_and_the_percentage_math_is_frozen() {
        assert!(uncovered_sql(SqlDialect::Mysql, CoverageRowScope::NoCycle)
            .ends_with("ORDER BY path_urn"));
        let m = CoverageMetrics {
            total: 3,
            covered: 1,
            uncovered: vec!["y".into(), "z".into()],
        };
        assert_eq!(m.coverage_percentage(), 33.33);
        assert_eq!(m.counts().coverage_percentage(), 33.33);
        assert_eq!(CoverageMetrics::default().coverage_percentage(), 0.0);
        assert_eq!(
            CoverageCounts {
                total: 4,
                covered: 2
            }
            .coverage_percentage(),
            50.0
        );
    }

    #[test]
    fn deployment_ids_are_bounded_by_the_column() {
        assert!(check_deployment_id("dep-1").is_ok());
        assert!(check_deployment_id("").is_err());
        assert!(check_deployment_id(&"x".repeat(65)).is_err());
    }

    #[test]
    fn no_cycle_statements_are_what_they_were_before_cycles() {
        let d = SqlDialect::Postgres;
        let s = CoverageRowScope::NoCycle;
        assert_eq!(
            seed_sql(d, s),
            "INSERT INTO coverage_metric (deployment_id, path_urn, covered) VALUES ($1, $2, \
             false) ON CONFLICT (deployment_id, path_urn) DO NOTHING"
        );
        assert_eq!(
            mark_update_sql(d, s),
            "UPDATE coverage_metric SET covered = true WHERE deployment_id = $1 AND path_urn = \
             $2 AND NOT covered"
        );
        assert_eq!(
            clear_paths_sql(SqlDialect::Mssql, s, 1),
            "UPDATE coverage_metric SET covered = 0 WHERE deployment_id = @P1 AND covered = 1 \
             AND path_urn IN (@P2)"
        );
        assert_eq!(
            write_fragment_sql(SqlDialect::Mysql, s),
            "INSERT INTO coverage_fragment (deployment_id, route_urn, segment_process, \
             instance_id, business_key, trace_id) VALUES (?, ?, ?, ?, ?, ?)"
        );
        assert_eq!(
            read_fragments_sql(d, s),
            "SELECT route_urn, segment_process, instance_id, business_key, trace_id FROM \
             coverage_fragment WHERE deployment_id = $1 ORDER BY id"
        );
    }

    #[test]
    fn in_cycle_statements_address_the_cycle_tables_with_the_label_second() {
        let d = SqlDialect::Postgres;
        let s = CoverageRowScope::InCycle;
        assert_eq!(
            seed_sql(d, s),
            "INSERT INTO coverage_cycle_metric (deployment_id, cycle_label, path_urn, covered) \
             VALUES ($1, $2, $3, false) ON CONFLICT (deployment_id, cycle_label, path_urn) DO \
             NOTHING"
        );
        assert!(mark_update_sql(d, s).contains("deployment_id = $1 AND cycle_label = $2"));
        assert!(mark_update_sql(d, s).contains("path_urn = $3"));
        assert!(covered_among_sql(d, s, 2).ends_with("IN ($3, $4)"));
        assert!(clear_paths_sql(SqlDialect::Mssql, s, 1).ends_with("IN (@P3)"));
        assert!(write_fragment_sql(d, s).contains("coverage_cycle_fragment"));
        assert!(write_fragment_sql(d, s).ends_with("VALUES ($1, $2, $3, $4, $5, $6, $7)"));
        assert!(delete_fragments_sql(d, s).contains("coverage_cycle_fragment"));
        assert!(reset_metrics_sql(d, s).contains("coverage_cycle_metric"));
    }

    #[test]
    fn cycle_labels_are_free_form_but_bounded() {
        assert!(check_cycle_label("Release 4.2 / QA").is_ok());
        assert!(check_cycle_label(&"x".repeat(CYCLE_LABEL_MAX_CHARS)).is_ok());
        assert!(check_cycle_label("").is_err());
        assert!(check_cycle_label("   ").is_err());
        assert!(check_cycle_label("a\nb").is_err());
        assert!(check_cycle_label(&"x".repeat(CYCLE_LABEL_MAX_CHARS + 1)).is_err());
    }
}
