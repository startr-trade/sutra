//! Migration-runner proofs against a real database: full shipped set applies, re-run
//! is a no-op, history is recorded under `sutra_schema_history` (same table name as
//! `tools/sutra-migrate`).

use std::path::{Path, PathBuf};

use sqlx::postgres::PgPoolOptions;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};

use crate::fixture::shipped_migration_roots;

#[ignore = "docker"]
#[tokio::test]
async fn applies_java_migrations_once_and_is_idempotent() {
    // fresh_pool() already migrates; here we drive the runner explicitly on a virgin DB.
    let (pool, _) = crate::fixture::fresh_pool_named().await; // migrated once by the fixture
    drop(pool);

    // Build a second, un-migrated database by connecting to a new one.
    let (migrated_pool, db) = fresh_unmigrated().await;

    let roots = shipped_migration_roots();
    let refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).unwrap();
    assert_eq!(
        scripts.len(),
        23,
        "V101..V802 subsystems + V404 terminal-retention marker + V604 outbox poison flag + V606 outbox emitting node + \
         V605 external-task pull parking + V1001 deployment-archive (deploy family) + \
         V1101 subject-index + V1201 dead-letter + V1202 dead-letter payload capture + \
         V1301 data-key"
    );

    let mut conn = migrated_pool.acquire().await.unwrap();
    let first_run = apply_migrations(&mut conn, &scripts).await.unwrap();
    assert_eq!(first_run, 23);
    let second_run = apply_migrations(&mut conn, &scripts).await.unwrap();
    assert_eq!(second_run, 0, "already-applied versions are skipped");
    drop(conn);

    let history: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM sutra_schema_history ORDER BY version")
            .fetch_all(&migrated_pool)
            .await
            .unwrap();
    assert_eq!(
        history,
        vec![
            101, 201, 301, 401, 402, 403, 404, 501, 601, 602, 603, 604, 605, 606, 701, 702, 801,
            802, 1001, 1101, 1201, 1202, 1301
        ]
    );

    // Every table family exists (the audit family included).
    for table in [
        "alias_index",
        "audit_event",
        "inbox_seen",
        "instance_state",
        "lease",
        "outbox_entry",
        "external_task",
        "channel_instance",
        "waiting_event",
        "dead_letter",
        "data_key",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&migrated_pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{table} exists and is empty in {db}");
    }

    // V1202's replay capture: nullable columns on the existing dead_letter table (an ALTER, so a
    // deployment that already carries V1201 rows migrates without a rewrite or a default).
    let capture_columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, is_nullable FROM information_schema.columns \
         WHERE table_name = 'dead_letter' AND column_name IN \
         ('payload', 'headers_json', 'content_type', 'tenant', 'module_key') \
         ORDER BY column_name",
    )
    .fetch_all(&migrated_pool)
    .await
    .unwrap();
    assert_eq!(
        capture_columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "content_type",
            "headers_json",
            "module_key",
            "payload",
            "tenant"
        ],
        "V1202 added the five replay-capture columns in {db}"
    );
    assert!(
        capture_columns
            .iter()
            .all(|(_, nullable)| nullable == "YES"),
        "every capture column is NULLABLE — pre-capture rows stay valid"
    );

    // V404's terminal-retention marker: an ADDED nullable column, so an existing deployment's
    // in-flight instances migrate without a table rewrite and read back as live (NULL = live).
    let terminal_at: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, is_nullable FROM information_schema.columns \
         WHERE table_name = 'instance_state' AND column_name = 'terminal_at'",
    )
    .fetch_all(&migrated_pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_at,
        vec![("terminal_at".to_owned(), "YES".to_owned())],
        "V404 added a nullable terminal_at to instance_state in {db}"
    );
    let terminal_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'instance_state' \
         AND indexname = 'instance_state_terminal_at'",
    )
    .fetch_one(&migrated_pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_index, 1,
        "the partial purge index ships with V404 — the sweep must not seq-scan a live table"
    );
}

/// Creates a database WITHOUT running the fixture's migrations.
async fn fresh_unmigrated() -> (sqlx::PgPool, String) {
    // Reuse the fixture's container by asking it for a migrated db first (starts the
    // container if needed), then create a sibling raw database by hand.
    let (bootstrap, migrated_db) = crate::fixture::fresh_pool_named().await;
    let raw_db = format!("{migrated_db}_raw");
    sqlx::query(&format!("CREATE DATABASE {raw_db}"))
        .execute(&bootstrap)
        .await
        .unwrap();
    bootstrap.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&crate::fixture::db_url(&raw_db))
        .await
        .unwrap();
    (pool, raw_db)
}
