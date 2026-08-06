//! Ordered SQL migration runner — SQL Server dialect.
//!
//! Applies the `migrations_mssql/**` scripts (same `V<number>__<description>.sql` naming
//! and global-V-number ordering rule as the reference runner in [`crate::migrate`];
//! discovery is shared via [`crate::migrate::collect_migrations`]). Applied versions are
//! recorded in the same `sutra_schema_history` ledger table.
//!
//! T-SQL scripts may contain `GO` batch separators (a client-side convention): a batch
//! referencing columns added earlier in the same script must compile separately. Each
//! script's batches and its history INSERT run inside one transaction — SQL Server DDL is
//! transactional, so the reference dialect's script+history atomicity carries over.

use std::fmt::Write as _;

use crate::migrate::MigrationScript;
use crate::mssql::{req, run_batch, MssqlClient};
use crate::{PersistenceError, Result};

/// Splits a script on lines containing only `GO` (case-insensitive).
fn split_batches(sql: &str) -> Vec<String> {
    let mut batches = Vec::new();
    let mut current = String::new();
    for line in sql.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            batches.push(std::mem::take(&mut current));
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    batches.push(current);
    batches
        .into_iter()
        .filter(|batch| {
            batch
                .lines()
                .map(str::trim)
                .any(|line| !line.is_empty() && !line.starts_with("--"))
        })
        .collect()
}

/// Applies every not-yet-applied script in ascending V-number order; returns how many ran.
/// Creates `sutra_schema_history` on first use; re-running is a no-op for applied versions.
pub async fn apply_migrations(
    client: &mut MssqlClient,
    scripts: &[MigrationScript],
) -> Result<u32> {
    run_batch(
        client,
        "IF OBJECT_ID('sutra_schema_history', 'U') IS NULL \
         CREATE TABLE sutra_schema_history (\
           version      BIGINT NOT NULL CONSTRAINT pk_sutra_schema_history PRIMARY KEY,\
           description  NVARCHAR(400) NOT NULL,\
           script       NVARCHAR(400) NOT NULL,\
           installed_on DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()\
         )",
    )
    .await?;

    let rows = client
        .query(
            "SELECT version FROM sutra_schema_history ORDER BY version",
            &[],
        )
        .await
        .map_err(PersistenceError::mssql("read sutra_schema_history"))?
        .into_first_result()
        .await
        .map_err(PersistenceError::mssql("read sutra_schema_history rows"))?;
    let applied: Vec<i64> = rows
        .iter()
        .map(|row| req::<i64>(row, "version"))
        .collect::<Result<_>>()?;

    let mut ran = 0u32;
    for script in scripts {
        if applied.contains(&script.version) {
            continue;
        }
        run_batch(client, "SET XACT_ABORT ON; BEGIN TRANSACTION").await?;
        for batch in split_batches(&script.sql) {
            if let Err(e) = run_batch(client, &batch).await {
                // Best-effort rollback keeps the connection reusable for error reporting.
                let _ = run_batch(client, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await;
                let mut msg = String::new();
                let _ = write!(
                    msg,
                    "migration V{} ({}) failed: {e}",
                    script.version, script.description
                );
                return Err(PersistenceError::Migration(msg));
            }
        }
        let file = script
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        client
            .execute(
                "INSERT INTO sutra_schema_history (version, description, script) \
                 VALUES (@P1, @P2, @P3)",
                &[&script.version, &script.description.as_str(), &file],
            )
            .await
            .map_err(PersistenceError::mssql("record sutra_schema_history row"))?;
        run_batch(client, "COMMIT TRANSACTION").await?;
        ran += 1;
    }
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_go_lines_only() {
        let sql = "ALTER TABLE t ADD c INT;\nGO\nCREATE INDEX i ON t (c) WHERE c IS NOT NULL;\n";
        let batches = split_batches(sql);
        assert_eq!(batches.len(), 2);
        assert!(batches[0].contains("ALTER TABLE"));
        assert!(batches[1].contains("CREATE INDEX"));
        // No GO: a single batch.
        assert_eq!(split_batches("SELECT 1;\nSELECT 2;\n").len(), 1);
    }

    #[test]
    fn dialect_tree_collects_expected_versions() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations_mssql");
        let scripts = crate::migrate::collect_migrations(&[root.as_path()]).unwrap();
        let versions: Vec<i64> = scripts.iter().map(|s| s.version).collect();
        // The reference set minus the four row-security scripts (V403/V602/V702/V802 —
        // enforced-bind posture) plus the V605 external-task (pull) table, the V803 timer
        // addendum and the deployment-archive store (V1001 — DB-backed deployment source). No
        // V9xx: coverage is not an engine-database
        // subsystem (`datastore-schema-projection.md` §7) — its DDL ships with `sutra-datastore`
        // and is applied to the `coverage` store the deployment declares.
        assert_eq!(
            versions,
            vec![101, 201, 301, 401, 402, 501, 601, 603, 604, 605, 606, 701, 801, 803, 804, 1001]
        );
    }
}
