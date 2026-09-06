//! Ordered SQL migration runner — MySQL/MariaDB dialect.
//!
//! Applies the `migrations_mysql/**` scripts (same `V<number>__<description>.sql` naming
//! and global-V-number ordering rule as the reference runner in [`crate::migrate`];
//! discovery is shared via [`crate::migrate::collect_migrations`]). Applied versions are
//! recorded in the same `sutra_schema_history` ledger table.
//!
//! Dialect note: MySQL/MariaDB DDL is implicitly committing, so scripts do NOT get the
//! reference dialect's script+history one-transaction guarantee — a script that fails
//! midway leaves its earlier DDL applied with no history row (the standard migration-tool
//! posture on this dialect). Scripts are split on `;` statement terminators and executed
//! one statement at a time.

use std::fmt::Write as _;

use sqlx::MySqlConnection;

use crate::migrate::MigrationScript;
use crate::{PersistenceError, Result};

/// Splits a script into `;`-terminated statements. Full-line `--` comments are stripped
/// FIRST (they may legitimately contain `;`); in what remains, the dialect migration
/// files keep `;` strictly as a statement terminator (no string literals contain one).
fn split_statements(sql: &str) -> Vec<String> {
    let body: String = sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    body.split(';')
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Applies every not-yet-applied script in ascending V-number order; returns how many ran.
/// Creates `sutra_schema_history` on first use; re-running is a no-op for applied versions.
pub async fn apply_migrations(
    conn: &mut MySqlConnection,
    scripts: &[MigrationScript],
) -> Result<u32> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS sutra_schema_history (
           version      BIGINT PRIMARY KEY,
           description  VARCHAR(512) NOT NULL,
           script       VARCHAR(512) NOT NULL,
           installed_on DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
         ) CHARACTER SET utf8mb4",
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
        for statement in split_statements(&script.sql) {
            if let Err(e) = sqlx::raw_sql(&statement).execute(&mut *conn).await {
                let mut msg = String::new();
                let _ = write!(
                    msg,
                    "migration V{} ({}) failed: {e}",
                    script.version, script.description
                );
                return Err(PersistenceError::Migration(msg));
            }
        }
        sqlx::query(
            "INSERT INTO sutra_schema_history (version, description, script) VALUES (?, ?, ?)",
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
        .execute(&mut *conn)
        .await
        .map_err(PersistenceError::db("record sutra_schema_history row"))?;
        ran += 1;
    }
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_terminators_and_drops_comment_lines() {
        let sql = "-- header comment; with a semicolon\nCREATE TABLE t (a INT);\n\n\
                   -- tail comment\nCREATE INDEX i ON t (a);\n-- trailing prose\n";
        let statements = split_statements(sql);
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("CREATE TABLE"));
        assert!(statements[1].contains("CREATE INDEX"));
        assert!(
            !statements[0].contains("semicolon"),
            "comment lines are stripped before splitting"
        );
    }

    #[test]
    fn dialect_tree_collects_expected_versions() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations_mysql");
        let scripts = crate::migrate::collect_migrations(&[root.as_path()]).unwrap();
        let versions: Vec<i64> = scripts.iter().map(|s| s.version).collect();
        // The reference set minus the four row-security scripts (V403/V602/V702/V802 —
        // enforced-bind posture) plus the V605 external-task (pull) table, the V803 timer
        // addendum and the deployment-archive store (V1001 — DB-backed deployment source). No
        // V9xx: coverage is not an engine-database
        // subsystem — its DDL ships with `sutra-datastore`
        // and is applied to the `coverage` store the deployment declares.
        assert_eq!(
            versions,
            vec![101, 201, 301, 401, 402, 501, 601, 603, 604, 605, 606, 701, 801, 803, 804, 1001]
        );
    }
}
