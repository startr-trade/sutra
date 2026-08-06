//! Mirrors `tests/pg/migrations.rs`: the dialect migration tree applies once on a real
//! database, re-running is a no-op, history lands in `sutra_schema_history` (the shared
//! ledger name), and every table family exists.

use std::path::Path;

use sqlx::mysql::MySqlPoolOptions;
use sutra_persistence::migrate::collect_migrations;
use sutra_persistence::mysql::migrate::apply_migrations;

use crate::fixture::{db_url, fresh_pool_named, migration_root};

#[ignore = "docker"]
#[tokio::test]
async fn applies_dialect_migrations_once_and_is_idempotent() {
    // Build an un-migrated sibling database on the suite container.
    let (bootstrap, migrated_db) = fresh_pool_named().await;
    let raw_db = format!("{migrated_db}_raw");
    sqlx::query(&format!("CREATE DATABASE {raw_db}"))
        .execute(&bootstrap)
        .await
        .unwrap();
    bootstrap.close().await;
    let pool = MySqlPoolOptions::new()
        .max_connections(3)
        .connect(&db_url(&raw_db))
        .await
        .unwrap();

    let root = migration_root();
    let scripts = collect_migrations(&[root.as_path() as &Path]).unwrap();
    assert_eq!(
        scripts.len(),
        16,
        "reference set minus the four row-security scripts, plus V604/V605/V606/V803/V804 and the \
         V1001 deployment-archive store (coverage is no longer an engine-database subsystem — §7)"
    );

    let mut conn = pool.acquire().await.unwrap();
    let first_run = apply_migrations(&mut conn, &scripts).await.unwrap();
    assert_eq!(first_run, 16);
    let second_run = apply_migrations(&mut conn, &scripts).await.unwrap();
    assert_eq!(second_run, 0, "already-applied versions are skipped");
    drop(conn);

    let history: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM sutra_schema_history ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        history,
        vec![101, 201, 301, 401, 402, 501, 601, 603, 604, 605, 606, 701, 801, 803, 804, 1001]
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
        "timer_schedule",
        "deployment_archive",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "{table} exists and is empty in {raw_db}");
    }
}
