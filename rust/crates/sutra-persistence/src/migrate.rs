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

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};

use crate::{PersistenceError, Result};

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
        let mut tx = conn
            .begin()
            .await
            .map_err(PersistenceError::db("begin migration transaction"))?;
        sqlx::raw_sql(&script.sql)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                let mut msg = String::new();
                let _ = write!(
                    msg,
                    "migration V{} ({}) failed: {e}",
                    script.version, script.description
                );
                PersistenceError::Migration(msg)
            })?;
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
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("record sutra_schema_history row"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("commit migration"))?;
        ran += 1;
    }
    Ok(ran)
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
                101, 201, 301, 401, 402, 403, 404, 501, 601, 602, 603, 604, 605, 606, 701, 702,
                801, 802, 1001, 1101, 1201, 1202, 1301
            ]
        );
        assert_eq!(scripts[0].description, "alias_index");
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
