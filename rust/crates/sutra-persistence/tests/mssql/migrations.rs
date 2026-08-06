//! Mirrors `tests/pg/migrations.rs`: the dialect migration tree applies once on a real
//! database, re-running is a no-op, history lands in `sutra_schema_history` (the shared
//! ledger name), and every table family exists.

use sutra_persistence::migrate::collect_migrations;
use sutra_persistence::mssql::migrate::apply_migrations;
use sutra_persistence::mssql::MssqlPool;

use crate::fixture::{config_for, count_all, create_database, migration_root};

#[ignore = "docker"]
#[tokio::test]
async fn applies_dialect_migrations_once_and_is_idempotent() {
    let db = create_database().await; // virgin database, no migrations yet
    let pool = MssqlPool::new(config_for(&db));

    let root = migration_root();
    let scripts = collect_migrations(&[root.as_path()]).unwrap();
    assert_eq!(
        scripts.len(),
        16,
        "reference set minus the four row-security scripts, plus V604/V605/V606/V803/V804 and the \
         V1001 deployment-archive store (coverage is no longer an engine-database subsystem — §7)"
    );

    let mut conn = pool.acquire().await.unwrap();
    let first_run = apply_migrations(conn.client(), &scripts).await.unwrap();
    assert_eq!(first_run, 16);
    let second_run = apply_migrations(conn.client(), &scripts).await.unwrap();
    assert_eq!(second_run, 0, "already-applied versions are skipped");

    let rows = conn
        .client()
        .simple_query("SELECT version FROM sutra_schema_history ORDER BY version")
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let history: Vec<i64> = rows
        .iter()
        .map(|row| row.get::<i64, _>("version").expect("version"))
        .collect();
    assert_eq!(
        history,
        vec![101, 201, 301, 401, 402, 501, 601, 603, 604, 605, 606, 701, 801, 803, 804, 1001]
    );
    drop(conn);

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
        assert_eq!(
            count_all(&pool, table).await,
            0,
            "{table} exists and is empty in {db}"
        );
    }
}
