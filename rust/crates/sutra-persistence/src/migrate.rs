//! Ordered SQL migration runner.
//!
//! The engine applies **the shipped migration SQL** (Rust-owned under
//! `sutra-persistence/migrations/shipped/{core,audit,deploy}` — the `core`/`audit` trees are
//! byte-identical copies of the frozen reference engine migration trees, so the checksummed ledger
//! stays interoperable). COVERAGE is deliberately absent: since the 2026-08-04 superseding ruling
//! coverage lives in the deployment's OWN declared
//! `coverage` data store, so its engine-owned DDL ships with `sutra-datastore` and is applied to
//! that connection on first use — never to the engine database, and never through this ledger.
//! Files are named
//! `V<number>__<description>.sql` with globally-unique version numbers namespaced per
//! subsystem (alias=V1xx, audit=V2xx, inbox=V3xx, instance=V4xx, lease=V5xx, outbox=V6xx,
//! channel=V7xx, waitstate=V8xx; V9xx is retired — it was the coverage store, now
//! `sutra-datastore`'s), so a single ascending V-number sort across all
//! subfolders is deterministic without per-subsystem collisions — the same ordering rule
//! the reference `sutra-migrate` (Flyway) uses.
//!
//! Applied versions are recorded in `sutra_schema_history` (the same table name as
//! sutra-migrate) and skipped on re-run; each script runs in its own transaction together
//! with its history INSERT, mirroring Flyway's per-migration transactionality.
//!
//! Ledger shape: the ledger is deliberately minimal, and its `sutra_schema_history`
//! table is normative — `version, description, script, checksum,
//! installed_on`. The `checksum` column (sha256 hex of the script bytes, [`script_checksum`])
//! enables drift detection by `sutra migrate verify`; it was added pre-GA with no installed
//! data to migrate.
//!
//! **Scripts that cannot run in a transaction.** A script whose first line is
//! [`NO_TRANSACTION_DIRECTIVE`] (`-- sutra:no-transaction`) runs outside one: a statement such as
//! `CREATE INDEX CONCURRENTLY` is refused inside a transaction block, and it is what lets an index
//! be built on a large live table without blocking its writes. Such a script runs under a
//! session-level advisory lock, so two engine replicas booting together apply it once: the runner
//! takes the lock (polling for it: a session blocked on it would deadlock a concurrent index
//! build), re-reads the ledger, skips the script if a peer already applied it, otherwise
//! runs each `;`-terminated statement in autocommit and records the ledger row, then unlocks. A
//! crash part-way leaves the script unrecorded, so it runs again in full on the next boot, and its
//! statements must therefore be safe to repeat (`DROP INDEX CONCURRENTLY IF EXISTS` first clears an
//! index a crashed concurrent build left invalid).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};

use crate::{PersistenceError, Result};

/// The first line that marks a script as one to run outside a transaction (see the module docs).
pub const NO_TRANSACTION_DIRECTIVE: &str = "-- sutra:no-transaction";

/// The advisory-lock key the runner holds while it applies a no-transaction script. A constant
/// shared by every engine on one database: `'sutra_mig'` as eight ASCII bytes.
const NO_TRANSACTION_LOCK_KEY: i64 = 0x7375_7472_615f_6d69;

/// How often a runner waiting for that lock tries it again.
const NO_TRANSACTION_LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// One discovered migration script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationScript {
    /// Numeric version parsed from the `V<number>__` prefix.
    pub version: i64,
    /// The `<description>` part of the file name (underscores preserved).
    pub description: String,
    /// Absolute path the script was read from.
    pub path: PathBuf,
    /// File contents (UTF-8 SQL, possibly multi-statement).
    pub sql: String,
}

impl MigrationScript {
    /// Whether this script must run outside a transaction: its first line is
    /// [`NO_TRANSACTION_DIRECTIVE`].
    pub fn runs_without_transaction(&self) -> bool {
        self.sql
            .lines()
            .next()
            .is_some_and(|first| first.trim() == NO_TRANSACTION_DIRECTIVE)
    }
}

/// The statements of a no-transaction script, in order: its text split at each `;` that ends a
/// line, with `--` comment lines dropped and empty statements skipped. Such a script holds plain
/// DDL; a `;` inside a string literal or a function body is not supported there.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
        if trimmed.ends_with(';') {
            let statement = current.trim().trim_end_matches(';').trim().to_owned();
            if !statement.is_empty() {
                statements.push(statement);
            }
            current.clear();
        }
    }
    let rest = current.trim();
    if !rest.is_empty() {
        statements.push(rest.to_owned());
    }
    statements
}

/// Recursively collects `V<number>__<description>.sql` scripts under each root, sorted
/// ascending by version. Duplicate version numbers across roots/subfolders are an error —
/// the global-uniqueness invariant is what makes the ordering deterministic.
pub fn collect_migrations(roots: &[&Path]) -> Result<Vec<MigrationScript>> {
    let mut scripts: Vec<MigrationScript> = Vec::new();
    for root in roots {
        walk(root, &mut scripts)?;
    }
    if scripts.is_empty() {
        return Err(PersistenceError::Migration(format!(
            "no V<number>__<description>.sql scripts found under {}",
            roots
                .iter()
                .map(|r| r.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    order_scripts(scripts)
}

/// Applies the runner's ordering invariants to an arbitrarily-sourced script set: ascending
/// V-number sort, duplicate-version rejection, non-empty. Callers that discover scripts
/// outside the filesystem walk (e.g. a binary-embedded tree) route through this so the
/// invariants stay single-source.
pub fn order_scripts(mut scripts: Vec<MigrationScript>) -> Result<Vec<MigrationScript>> {
    scripts.sort_by_key(|s| s.version);
    for pair in scripts.windows(2) {
        if pair[0].version == pair[1].version {
            return Err(PersistenceError::Migration(format!(
                "duplicate migration version V{}: {} and {}",
                pair[0].version,
                pair[0].path.display(),
                pair[1].path.display()
            )));
        }
    }
    if scripts.is_empty() {
        return Err(PersistenceError::Migration(
            "no migration scripts provided".to_owned(),
        ));
    }
    Ok(scripts)
}

/// Parses a `V<number>__<description>.sql` file name into its version + description parts.
/// Returns `None` for names that are not migration scripts (the walk ignores those).
pub fn parse_script_name(file_name: &str) -> Option<(i64, String)> {
    let stem = file_name.strip_suffix(".sql")?;
    let rest = stem.strip_prefix('V')?;
    let (digits, description) = rest.split_once("__")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let version: i64 = digits.parse().ok()?;
    Some((version, description.to_owned()))
}

/// Sha256 checksum (lowercase hex) of a script's SQL text — the value recorded in the
/// ledger's `checksum` column and compared by `sutra migrate verify` for drift detection.
pub fn script_checksum(sql: &str) -> String {
    let digest = Sha256::digest(sql.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn walk(dir: &Path, out: &mut Vec<MigrationScript>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        PersistenceError::Migration(format!("cannot read migration dir {}: {e}", dir.display()))
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            PersistenceError::Migration(format!("cannot read dir entry in {}: {e}", dir.display()))
        })?;
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out)?;
        } else if let Some(script) = parse_script(&path)? {
            out.push(script);
        }
    }
    Ok(())
}

fn parse_script(path: &Path) -> Result<Option<MigrationScript>> {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return Ok(None);
    };
    let Some((version, description)) = parse_script_name(name) else {
        return Ok(None);
    };
    let sql = std::fs::read_to_string(path)
        .map_err(|e| PersistenceError::Migration(format!("cannot read {}: {e}", path.display())))?;
    Ok(Some(MigrationScript {
        version,
        description,
        path: path.to_owned(),
        sql,
    }))
}

/// Applies every not-yet-applied script in ascending V-number order and returns how many ran.
///
/// Creates `sutra_schema_history` on first use. Each script + its history row commit in one
/// transaction; a failing script aborts the run with everything before it already applied
/// (Flyway semantics).
pub async fn apply_migrations(conn: &mut PgConnection, scripts: &[MigrationScript]) -> Result<u32> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS sutra_schema_history (
           version      BIGINT PRIMARY KEY,
           description  TEXT NOT NULL,
           script       TEXT NOT NULL,
           checksum     TEXT NOT NULL,
           installed_on TIMESTAMPTZ NOT NULL DEFAULT now()
         )",
    )
    .execute(&mut *conn)
    .await
    .map_err(PersistenceError::db("create sutra_schema_history"))?;

    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM sutra_schema_history ORDER BY version")
            .fetch_all(&mut *conn)
            .await
            .map_err(PersistenceError::db("read sutra_schema_history"))?;

    let mut ran = 0u32;
    for script in scripts {
        if applied.contains(&script.version) {
            continue;
        }
        if script.runs_without_transaction() {
            if apply_without_transaction(conn, script).await? {
                ran += 1;
            }
            continue;
        }
        let mut tx = conn
            .begin()
            .await
            .map_err(PersistenceError::db("begin migration transaction"))?;
        sqlx::raw_sql(&script.sql)
            .execute(&mut *tx)
            .await
            .map_err(|e| script_failed(script, &e))?;
        record_applied(&mut tx, script).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("commit migration"))?;
        ran += 1;
    }
    Ok(ran)
}

fn script_failed(script: &MigrationScript, e: &sqlx::Error) -> PersistenceError {
    let mut msg = String::new();
    let _ = write!(
        msg,
        "migration V{} ({}) failed: {e}",
        script.version, script.description
    );
    PersistenceError::Migration(msg)
}

async fn record_applied(conn: &mut PgConnection, script: &MigrationScript) -> Result<()> {
    sqlx::query(
        "INSERT INTO sutra_schema_history (version, description, script, checksum) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(script.version)
    .bind(&script.description)
    .bind(
        script
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default(),
    )
    .bind(script_checksum(&script.sql))
    .execute(&mut *conn)
    .await
    .map_err(PersistenceError::db("record sutra_schema_history row"))?;
    Ok(())
}

/// Apply one no-transaction script under the runner's advisory lock. `Ok(false)` when a peer
/// applied it while this runner waited for the lock.
async fn apply_without_transaction(
    conn: &mut PgConnection,
    script: &MigrationScript,
) -> Result<bool> {
    // Polled with the non-blocking form, not waited for with `pg_advisory_lock`: a concurrent index
    // build waits for every transaction that is running when it starts, and a session blocked in
    // `pg_advisory_lock` is one, waiting in turn on the builder's lock, which PostgreSQL resolves
    // by aborting one of the two as a deadlock. Each try is a statement that ends at once.
    loop {
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(NO_TRANSACTION_LOCK_KEY)
            .fetch_one(&mut *conn)
            .await
            .map_err(PersistenceError::db("take the migration lock"))?;
        if taken {
            break;
        }
        // A sleep in the server, so this crate needs no timer of its own. It waits on no lock.
        sqlx::query("SELECT pg_sleep($1)")
            .bind(NO_TRANSACTION_LOCK_POLL.as_secs_f64())
            .execute(&mut *conn)
            .await
            .map_err(PersistenceError::db("wait for the migration lock"))?;
    }
    let outcome = apply_locked(conn, script).await;
    let unlocked = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(NO_TRANSACTION_LOCK_KEY)
        .execute(&mut *conn)
        .await
        .map_err(PersistenceError::db("release the migration lock"));
    let ran = outcome?;
    unlocked?;
    Ok(ran)
}

async fn apply_locked(conn: &mut PgConnection, script: &MigrationScript) -> Result<bool> {
    let already: Option<i64> =
        sqlx::query_scalar("SELECT version FROM sutra_schema_history WHERE version = $1")
            .bind(script.version)
            .fetch_optional(&mut *conn)
            .await
            .map_err(PersistenceError::db("re-read sutra_schema_history"))?;
    if already.is_some() {
        return Ok(false);
    }
    for statement in split_statements(&script.sql) {
        sqlx::raw_sql(&statement)
            .execute(&mut *conn)
            .await
            .map_err(|e| script_failed(script, &e))?;
    }
    record_applied(conn, script).await?;
    Ok(true)
}

/// One applied-migration row from the `sutra_schema_history` ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// The applied V-number.
    pub version: i64,
    /// Description recorded at apply time.
    pub description: String,
    /// Script file name recorded at apply time.
    pub script: String,
    /// Sha256 hex of the script text at apply time (see [`script_checksum`]).
    pub checksum: String,
    /// When the script was applied.
    pub installed_on: time::OffsetDateTime,
}

/// Reads the full ledger ordered by version. A database whose ledger table does not exist
/// yet reads as an empty ledger — the state of a never-migrated database.
pub async fn read_ledger(conn: &mut PgConnection) -> Result<Vec<LedgerEntry>> {
    let exists: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('sutra_schema_history')::text")
            .fetch_one(&mut *conn)
            .await
            .map_err(PersistenceError::db("probe sutra_schema_history"))?;
    if exists.is_none() {
        return Ok(Vec::new());
    }
    let rows: Vec<(i64, String, String, String, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT version, description, script, checksum, installed_on \
         FROM sutra_schema_history ORDER BY version",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(PersistenceError::db("read sutra_schema_history ledger"))?;
    Ok(rows
        .into_iter()
        .map(
            |(version, description, script, checksum, installed_on)| LedgerEntry {
                version,
                description,
                script,
                checksum,
                installed_on,
            },
        )
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine-shipped migration roots, resolved relative to this crate at test time
    /// (read-only access to the reference tree). Production packaging note: the standalone image
    /// copies these SQL trees verbatim.
    pub(crate) fn shipped_migration_roots() -> Vec<PathBuf> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo = manifest
            .ancestors()
            .nth(3)
            .expect("repo root")
            .to_path_buf();
        vec![
            repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
            repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
            repo.join("rust/crates/sutra-persistence/migrations/shipped/deploy"),
        ]
    }

    #[test]
    fn collects_shipped_migrations_in_global_v_number_order() {
        let roots = shipped_migration_roots();
        let refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
        let scripts = collect_migrations(&refs).unwrap();

        let versions: Vec<i64> = scripts.iter().map(|s| s.version).collect();
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        assert_eq!(versions, sorted, "ascending V-number order");

        // The full shipped set: alias V1xx, audit V2xx, inbox V3xx, instance V4xx,
        // lease V5xx, outbox V6xx, channel V7xx, waitstate V8xx, deploy V10xx, subject V11xx,
        // incident/dead-letter V12xx, crypto/data-key V13xx. No V9xx: coverage is not an
        // engine-database subsystem any more (§7) — its DDL ships with `sutra-datastore` and is
        // applied to the store the deployment declares.
        assert_eq!(
            versions,
            vec![
                101, 201, 202, 203, 301, 304, 305, 306, 401, 402, 403, 404, 501, 601, 602, 603,
                604, 605, 606, 701, 702, 801, 802, 1001, 1002, 1003, 1101, 1201, 1202, 1301
            ]
        );
        assert_eq!(scripts[0].description, "alias_index");
    }

    #[test]
    fn the_directive_is_read_from_the_first_line_only() {
        let script = |sql: &str| MigrationScript {
            version: 1,
            description: "x".to_owned(),
            path: PathBuf::from("V1__x.sql"),
            sql: sql.to_owned(),
        };
        assert!(
            script("-- sutra:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (c);")
                .runs_without_transaction()
        );
        assert!(
            !script("-- a comment\n-- sutra:no-transaction\nSELECT 1;").runs_without_transaction()
        );
        assert!(!script("SELECT 1;").runs_without_transaction());
    }

    #[test]
    fn statements_split_at_line_ending_semicolons_without_comments() {
        let sql = "-- sutra:no-transaction\n-- why\nDROP INDEX CONCURRENTLY IF EXISTS i;\n\n\
                   CREATE UNIQUE INDEX CONCURRENTLY i\n  ON t (a, b) WHERE a IS NOT NULL;\n";
        assert_eq!(
            split_statements(sql),
            vec![
                "DROP INDEX CONCURRENTLY IF EXISTS i".to_owned(),
                "CREATE UNIQUE INDEX CONCURRENTLY i\n  ON t (a, b) WHERE a IS NOT NULL".to_owned(),
            ]
        );
    }

    #[test]
    fn duplicate_versions_are_rejected() {
        let dir = std::env::temp_dir().join(format!("sutra-mig-dup-{}", std::process::id()));
        let sub_a = dir.join("a");
        let sub_b = dir.join("b");
        std::fs::create_dir_all(&sub_a).unwrap();
        std::fs::create_dir_all(&sub_b).unwrap();
        std::fs::write(sub_a.join("V1__one.sql"), "SELECT 1;").unwrap();
        std::fs::write(sub_b.join("V1__other.sql"), "SELECT 1;").unwrap();
        let err = collect_migrations(&[dir.as_path()]).unwrap_err();
        assert!(err.to_string().contains("duplicate migration version V1"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn script_name_parsing_extracts_version_and_description() {
        assert_eq!(
            parse_script_name("V803__waiting_event_timer.sql"),
            Some((803, "waiting_event_timer".to_owned()))
        );
        assert_eq!(parse_script_name("Vx__bad.sql"), None);
        assert_eq!(parse_script_name("V3_missing_sep.sql"), None);
        assert_eq!(parse_script_name("readme.md"), None);
    }

    #[test]
    fn checksum_is_the_sha256_hex_of_the_script_text() {
        // Pinned vector: sha256("SELECT 1;").
        assert_eq!(
            script_checksum("SELECT 1;"),
            "17db4fd369edb9244b9f91d9aeed145c3d04ad8ba6e95d06247f07a63527d11a"
        );
        assert_ne!(script_checksum("SELECT 1;"), script_checksum("SELECT 2;"));
    }

    #[test]
    fn order_scripts_sorts_and_rejects_duplicates() {
        let make = |version: i64, name: &str| MigrationScript {
            version,
            description: name.to_owned(),
            path: PathBuf::from(format!("V{version}__{name}.sql")),
            sql: String::new(),
        };
        let ordered = order_scripts(vec![make(301, "b"), make(101, "a")]).unwrap();
        assert_eq!(
            ordered.iter().map(|s| s.version).collect::<Vec<_>>(),
            vec![101, 301]
        );
        let err = order_scripts(vec![make(101, "a"), make(101, "b")]).unwrap_err();
        assert!(err.to_string().contains("duplicate migration version V101"));
        assert!(order_scripts(Vec::new()).is_err());
    }

    #[test]
    fn non_matching_files_are_ignored() {
        let dir = std::env::temp_dir().join(format!("sutra-mig-ign-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("V2__real.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.join("readme.md"), "not sql").unwrap();
        std::fs::write(dir.join("Vx__bad.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.join("V3_missing_sep.sql"), "SELECT 1;").unwrap();
        let scripts = collect_migrations(&[dir.as_path()]).unwrap();
        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].version, 2);
        std::fs::remove_dir_all(&dir).ok();
    }
}
