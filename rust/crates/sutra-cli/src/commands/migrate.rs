//! `sutra migrate` — apply the engine schema migrations to a PostgreSQL database, plus
//! the read-only `status` and `verify` inspections and a plan-only `--dry-run`.
//!
//! The migration runner itself is `sutra_persistence::migrate` (library reuse); this
//! command adds argument/environment wiring, the embedded SQL source
//! ([`crate::embedded`], overridable via `--migrations`), and the ledger checks: the
//! checksummed minimal ledger is the normative format, and `verify` = expected-head +
//! continuity + drift.
//!
//! Connection settings fall back to the deploy contract's environment variables
//! (`SUTRA_DB_URL` / `SUTRA_DB_USERNAME` / `SUTRA_DB_PASSWORD` / `SUTRA_DB_SCHEMA`), so
//! the binary drops into the pre-deploy migration Job unchanged.
//!
//! PostgreSQL only for now — `--dialect` arrives once further dialects land.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection};
use sutra_persistence::migrate::{
    apply_migrations, collect_migrations, read_ledger, script_checksum, LedgerEntry,
    MigrationScript,
};

use crate::embedded;
use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra migrate` — the `SUTRA.MIGRATE.*` family the
/// persistence contract reserves for it.
pub mod codes {
    pub const APPLY_FAILED: &str = "SUTRA.MIGRATE.APPLY_FAILED";
    pub const LEDGER_EMPTY: &str = "SUTRA.MIGRATE.LEDGER_EMPTY";
    pub const HEAD_MISMATCH: &str = "SUTRA.MIGRATE.HEAD_MISMATCH";
    pub const VERSION_GAP: &str = "SUTRA.MIGRATE.VERSION_GAP";
    pub const UNKNOWN_VERSION: &str = "SUTRA.MIGRATE.UNKNOWN_VERSION";
    pub const CHECKSUM_DRIFT: &str = "SUTRA.MIGRATE.CHECKSUM_DRIFT";
}

#[derive(Debug, clap::Args)]
pub struct MigrateArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Plan only: report what would be applied without touching the schema.
    #[arg(long)]
    pub dry_run: bool,

    #[command(subcommand)]
    pub action: Option<MigrateAction>,
}

#[derive(Debug, clap::Subcommand)]
pub enum MigrateAction {
    /// List applied vs pending migrations.
    Status(StatusArgs),
    /// Check ledger integrity: expected head, version continuity, checksum drift.
    Verify(VerifyArgs),
}

#[derive(Debug, clap::Args)]
pub struct StatusArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,
}

#[derive(Debug, clap::Args)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Expected head version (defaults to the highest available script version).
    #[arg(long, value_name = "VERSION")]
    pub expected_head: Option<i64>,
}

/// Database connection + script source settings, shared by every migrate action.
/// Environment fallbacks match the deploy contract's migration-Job variables.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct ConnectionArgs {
    /// Database URL (postgres://…).
    #[arg(long, env = "SUTRA_DB_URL", value_name = "URL")]
    pub url: Option<String>,

    /// Database user (overrides any user embedded in the URL).
    #[arg(long, env = "SUTRA_DB_USERNAME", value_name = "USER")]
    pub user: Option<String>,

    /// Database password (overrides any password embedded in the URL).
    #[arg(
        long,
        env = "SUTRA_DB_PASSWORD",
        hide_env_values = true,
        value_name = "PASSWORD"
    )]
    pub password: Option<String>,

    /// Schema to migrate into (created if absent; default: public).
    #[arg(long, env = "SUTRA_DB_SCHEMA", value_name = "SCHEMA")]
    pub schema: Option<String>,

    /// Directory of V<number>__<description>.sql scripts overriding the embedded engine set.
    #[arg(long, value_name = "DIR")]
    pub migrations: Option<PathBuf>,
}

pub fn execute(args: MigrateArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "migrate: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        None => run_apply(&args.conn, args.dry_run, format, io),
        Some(MigrateAction::Status(status)) => run_status(&status.conn, format, io),
        Some(MigrateAction::Verify(verify)) => {
            run_verify(&verify.conn, verify.expected_head, format, io)
        }
    }
}

// ----- apply (+ --dry-run) -----

fn run_apply(conn: &ConnectionArgs, dry_run: bool, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let scripts = match load_scripts(conn, io) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let head = scripts.last().map(|s| s.version).unwrap_or_default();

    block_on(async {
        let mut db = match connect(conn).await {
            Ok(db) => db,
            Err(msg) => {
                let _ = writeln!(io.err, "migrate: {msg}");
                return exit::USAGE;
            }
        };

        if dry_run {
            let ledger = match read_ledger(&mut db).await {
                Ok(l) => l,
                Err(e) => {
                    let _ = writeln!(io.err, "migrate: {e}");
                    return exit::USAGE;
                }
            };
            let applied: BTreeSet<i64> = ledger.iter().map(|e| e.version).collect();
            let pending: Vec<&MigrationScript> = scripts
                .iter()
                .filter(|s| !applied.contains(&s.version))
                .collect();
            match format {
                ReportFormat::Text => {
                    let _ = writeln!(
                        io.out,
                        "Pending migrations ({} of {} available):",
                        pending.len(),
                        scripts.len()
                    );
                    for script in &pending {
                        let _ = writeln!(io.out, "  V{:<6}{}", script.version, script.description);
                    }
                    let _ = writeln!(io.out, "--dry-run: no changes applied");
                }
                ReportFormat::Json => {
                    let payload = serde_json::json!({
                        "dryRun": true,
                        "available": scripts.len(),
                        "pending": pending.iter().map(|s| serde_json::json!({
                            "version": s.version,
                            "description": s.description,
                        })).collect::<Vec<_>>(),
                    });
                    let _ = writeln!(io.out, "{payload}");
                }
            }
            return exit::OK;
        }

        match apply_migrations(&mut db, &scripts).await {
            Ok(applied) => {
                match format {
                    ReportFormat::Text => {
                        if applied == 0 {
                            let _ = writeln!(
                                io.out,
                                "Nothing to apply — schema is up to date (head V{head})"
                            );
                        } else {
                            let _ = writeln!(
                                io.out,
                                "Applied {applied} migration(s); head is now V{head}"
                            );
                        }
                    }
                    ReportFormat::Json => {
                        let payload = serde_json::json!({ "applied": applied, "head": head });
                        let _ = writeln!(io.out, "{payload}");
                    }
                }
                exit::OK
            }
            Err(e) => {
                let d = Diagnostic::error(codes::APPLY_FAILED, e.to_string());
                let _ = writeln!(io.err, "{}", d.render_text());
                exit::FINDINGS
            }
        }
    })
}

// ----- status -----

fn run_status(conn: &ConnectionArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let scripts = match load_scripts(conn, io) {
        Ok(s) => s,
        Err(code) => return code,
    };

    block_on(async {
        let mut db = match connect(conn).await {
            Ok(db) => db,
            Err(msg) => {
                let _ = writeln!(io.err, "migrate status: {msg}");
                return exit::USAGE;
            }
        };
        let ledger = match read_ledger(&mut db).await {
            Ok(l) => l,
            Err(e) => {
                let _ = writeln!(io.err, "migrate status: {e}");
                return exit::USAGE;
            }
        };

        let applied: BTreeSet<i64> = ledger.iter().map(|e| e.version).collect();
        let pending = scripts
            .iter()
            .filter(|s| !applied.contains(&s.version))
            .count();
        let script_versions: BTreeSet<i64> = scripts.iter().map(|s| s.version).collect();

        match format {
            ReportFormat::Text => {
                let _ = writeln!(
                    io.out,
                    "Schema migration status: {} applied, {} pending, {} available",
                    ledger.len(),
                    pending,
                    scripts.len()
                );
                for script in &scripts {
                    let state = ledger
                        .iter()
                        .find(|e| e.version == script.version)
                        .map(|e| format!("applied {}", format_time(e)))
                        .unwrap_or_else(|| "pending".to_owned());
                    let _ = writeln!(
                        io.out,
                        "  V{:<6}{:<32}{state}",
                        script.version, script.description
                    );
                }
                for entry in &ledger {
                    if !script_versions.contains(&entry.version) {
                        let _ = writeln!(
                            io.out,
                            "  V{:<6}{:<32}applied {} (not in migration source)",
                            entry.version,
                            entry.description,
                            format_time(entry)
                        );
                    }
                }
            }
            ReportFormat::Json => {
                let mut entries: Vec<serde_json::Value> = scripts
                    .iter()
                    .map(|script| {
                        let row = ledger.iter().find(|e| e.version == script.version);
                        serde_json::json!({
                            "version": script.version,
                            "description": script.description,
                            "state": if row.is_some() { "applied" } else { "pending" },
                            "installedOn": row.map(format_time),
                        })
                    })
                    .collect();
                for entry in &ledger {
                    if !script_versions.contains(&entry.version) {
                        entries.push(serde_json::json!({
                            "version": entry.version,
                            "description": entry.description,
                            "state": "applied-unknown",
                            "installedOn": format_time(entry),
                        }));
                    }
                }
                let payload = serde_json::json!({
                    "applied": ledger.len(),
                    "pending": pending,
                    "available": scripts.len(),
                    "entries": entries,
                });
                let _ = writeln!(io.out, "{payload}");
            }
        }
        exit::OK
    })
}

// ----- verify -----

fn run_verify(
    conn: &ConnectionArgs,
    expected_head: Option<i64>,
    format: ReportFormat,
    io: &mut Io<'_>,
) -> i32 {
    let scripts = match load_scripts(conn, io) {
        Ok(s) => s,
        Err(code) => return code,
    };

    block_on(async {
        let mut db = match connect(conn).await {
            Ok(db) => db,
            Err(msg) => {
                let _ = writeln!(io.err, "migrate verify: {msg}");
                return exit::USAGE;
            }
        };
        let ledger = match read_ledger(&mut db).await {
            Ok(l) => l,
            Err(e) => {
                let _ = writeln!(io.err, "migrate verify: {e}");
                return exit::USAGE;
            }
        };

        let expected = expected_head
            .or_else(|| scripts.iter().map(|s| s.version).max())
            .unwrap_or_default();
        let findings = verify_findings(&scripts, &ledger, expected);

        match format {
            ReportFormat::Text => {
                if findings.is_empty() {
                    let _ = writeln!(
                        io.out,
                        "Verification OK: head V{expected}, {} applied, checksums match",
                        ledger.len()
                    );
                } else {
                    for finding in &findings {
                        let _ = writeln!(io.out, "{}", finding.render_text());
                    }
                    let _ = writeln!(io.out, "Verification FAILED: {} finding(s)", findings.len());
                }
            }
            ReportFormat::Json => {
                let payload = serde_json::json!({
                    "ok": findings.is_empty(),
                    "expectedHead": expected,
                    "applied": ledger.len(),
                    "findings": findings.iter().map(Diagnostic::to_json).collect::<Vec<_>>(),
                });
                let _ = writeln!(io.out, "{payload}");
            }
        }
        if findings.is_empty() {
            exit::OK
        } else {
            exit::FINDINGS
        }
    })
}

/// The ledger checks behind `sutra migrate verify` — the expected-head gate semantics of
/// the earlier schema-version check, re-based onto the checksummed ledger shape:
///
/// 1. an empty (or absent) ledger fails closed;
/// 2. the ledger head must equal the expected head;
/// 3. continuity — every available script at or below the ledger head must be applied;
/// 4. every ledger row must correspond to an available script (else it is unknown);
/// 5. each matched row's recorded checksum must equal the script's current checksum.
pub fn verify_findings(
    scripts: &[MigrationScript],
    ledger: &[LedgerEntry],
    expected_head: i64,
) -> Vec<Diagnostic> {
    let mut findings = Vec::new();

    if ledger.is_empty() {
        findings.push(
            Diagnostic::error(
                codes::LEDGER_EMPTY,
                format!("no migrations have been applied; expected head V{expected_head}"),
            )
            .at("sutra_schema_history"),
        );
        return findings;
    }

    let actual_head = ledger.iter().map(|e| e.version).max().unwrap_or_default();
    if actual_head != expected_head {
        findings.push(
            Diagnostic::error(
                codes::HEAD_MISMATCH,
                format!(
                    "schema head mismatch: expected V{expected_head}, ledger has V{actual_head}"
                ),
            )
            .at("sutra_schema_history"),
        );
    }

    let applied: BTreeSet<i64> = ledger.iter().map(|e| e.version).collect();
    for script in scripts {
        if script.version <= actual_head && !applied.contains(&script.version) {
            findings.push(
                Diagnostic::error(
                    codes::VERSION_GAP,
                    format!(
                        "script V{} ({}) is below the ledger head V{actual_head} but was never applied",
                        script.version, script.description
                    ),
                )
                .at(script.path.display().to_string()),
            );
        }
    }

    for entry in ledger {
        match scripts.iter().find(|s| s.version == entry.version) {
            None => findings.push(
                Diagnostic::error(
                    codes::UNKNOWN_VERSION,
                    format!(
                        "ledger records V{} ({}) but no such script exists in the migration source",
                        entry.version, entry.description
                    ),
                )
                .at(format!("sutra_schema_history:V{}", entry.version)),
            ),
            Some(script) => {
                let current = script_checksum(&script.sql);
                if current != entry.checksum {
                    findings.push(
                        Diagnostic::error(
                            codes::CHECKSUM_DRIFT,
                            format!(
                                "checksum drift for V{} ({}): ledger has {}, script is {}",
                                entry.version,
                                entry.description,
                                short(&entry.checksum),
                                short(&current)
                            ),
                        )
                        .at(script.path.display().to_string()),
                    );
                }
            }
        }
    }

    findings
}

fn short(checksum: &str) -> &str {
    &checksum[..checksum.len().min(12)]
}

// ----- shared plumbing -----

fn load_scripts(conn: &ConnectionArgs, io: &mut Io<'_>) -> Result<Vec<MigrationScript>, i32> {
    let loaded = match &conn.migrations {
        Some(dir) => collect_migrations(&[dir.as_path()]),
        None => embedded::engine_scripts(),
    };
    loaded.map_err(|e| {
        let _ = writeln!(io.err, "migrate: {e}");
        exit::USAGE
    })
}

async fn connect(conn: &ConnectionArgs) -> Result<PgConnection, String> {
    let Some(url) = conn.url.as_deref() else {
        return Err("--url (or SUTRA_DB_URL) is required".to_owned());
    };
    let mut options =
        PgConnectOptions::from_str(url).map_err(|e| format!("invalid database URL: {e}"))?;
    if let Some(user) = &conn.user {
        options = options.username(user);
    }
    if let Some(password) = &conn.password {
        options = options.password(password);
    }
    let mut db = PgConnection::connect_with(&options)
        .await
        .map_err(|e| format!("cannot connect to the database: {e}"))?;

    if let Some(schema) = conn.schema.as_deref() {
        if schema != "public" {
            if !is_safe_identifier(schema) {
                return Err(format!("unsafe schema identifier: {schema}"));
            }
            sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .execute(&mut db)
                .await
                .map_err(|e| format!("cannot create schema {schema}: {e}"))?;
            sqlx::raw_sql(&format!("SET search_path TO {schema}"))
                .execute(&mut db)
                .await
                .map_err(|e| format!("cannot set search_path to {schema}: {e}"))?;
        }
    }
    Ok(db)
}

/// Identifier whitelist for schema names: lowercase alphanumerics + underscore, max 63
/// bytes — the same conservative rule the earlier version gate applied to table names.
fn is_safe_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn format_time(entry: &LedgerEntry) -> String {
    entry
        .installed_on
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| entry.installed_on.to_string())
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(version: i64, description: &str, sql: &str) -> MigrationScript {
        MigrationScript {
            version,
            description: description.to_owned(),
            path: PathBuf::from(format!("embedded:V{version}__{description}.sql")),
            sql: sql.to_owned(),
        }
    }

    fn entry(version: i64, description: &str, sql: &str) -> LedgerEntry {
        LedgerEntry {
            version,
            description: description.to_owned(),
            script: format!("V{version}__{description}.sql"),
            checksum: script_checksum(sql),
            installed_on: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn clean_ledger_verifies() {
        let scripts = vec![
            script(101, "a", "CREATE TABLE a();"),
            script(201, "b", "CREATE TABLE b();"),
        ];
        let ledger = vec![
            entry(101, "a", "CREATE TABLE a();"),
            entry(201, "b", "CREATE TABLE b();"),
        ];
        assert!(verify_findings(&scripts, &ledger, 201).is_empty());
    }

    #[test]
    fn empty_ledger_fails_closed() {
        let scripts = vec![script(101, "a", "x")];
        let findings = verify_findings(&scripts, &[], 101);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, codes::LEDGER_EMPTY);
    }

    #[test]
    fn head_mismatch_is_reported() {
        let scripts = vec![script(101, "a", "x"), script(201, "b", "y")];
        let ledger = vec![entry(101, "a", "x")];
        let findings = verify_findings(&scripts, &ledger, 201);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, codes::HEAD_MISMATCH);
        assert!(
            findings[0].message.contains("expected V201"),
            "{}",
            findings[0].message
        );
        assert!(
            findings[0].message.contains("ledger has V101"),
            "{}",
            findings[0].message
        );
    }

    #[test]
    fn continuity_gap_is_reported() {
        let scripts = vec![script(101, "a", "x"), script(201, "b", "y")];
        let ledger = vec![entry(201, "b", "y")]; // V101 skipped
        let findings = verify_findings(&scripts, &ledger, 201);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, codes::VERSION_GAP);
    }

    #[test]
    fn unknown_ledger_version_is_reported() {
        let scripts = vec![script(101, "a", "x")];
        let ledger = vec![entry(101, "a", "x"), entry(999, "mystery", "z")];
        let findings = verify_findings(&scripts, &ledger, 999);
        // V999 is the ledger head; expected head 999 avoids a head-mismatch double-report.
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, codes::UNKNOWN_VERSION);
    }

    #[test]
    fn checksum_drift_is_reported() {
        let scripts = vec![script(101, "a", "CREATE TABLE a(id BIGINT);")];
        let ledger = vec![entry(101, "a", "CREATE TABLE a();")];
        let findings = verify_findings(&scripts, &ledger, 101);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, codes::CHECKSUM_DRIFT);
        assert!(findings[0].message.contains("checksum drift for V101"));
    }

    #[test]
    fn safe_identifier_whitelist() {
        assert!(is_safe_identifier("public_2"));
        assert!(!is_safe_identifier("Public"));
        assert!(!is_safe_identifier("x; DROP TABLE y"));
        assert!(!is_safe_identifier(""));
    }
}
