//! Migration-runner proofs against a real database: full shipped set applies, re-run
//! is a no-op, history is recorded under `sutra_schema_history` (same table name as
//! `tools/sutra-migrate`).

use std::path::{Path, PathBuf};

use sqlx::postgres::PgPoolOptions;
use sutra_persistence::migrate::{apply_migrations, collect_migrations, MigrationScript};

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
        33,
        "V101..V802 subsystems + V304 inbox instance + V305/V306 inbox owner + V404 terminal-retention marker + V604 outbox poison flag + V606 outbox emitting node + \
         V605 external-task pull parking + V1001 deployment-archive + V1002 endpoint holder + V1003 retained channels (deploy family) + \
         V1101 subject-index + V1201 dead-letter + V1202 dead-letter payload capture + \
         V1301 data-key + V202 audit transport events + V203 audit journal ordinal + V204 audit media-pending index + V205 audit spool reference + V206 audit attempt"
    );

    let mut conn = migrated_pool.acquire().await.unwrap();
    let first_run = apply_migrations(&mut conn, &scripts).await.unwrap();
    assert_eq!(first_run, 33);
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
            101, 201, 202, 203, 204, 205, 206, 301, 304, 305, 306, 401, 402, 403, 404, 501, 601,
            602, 603, 604, 605, 606, 701, 702, 801, 802, 1001, 1002, 1003, 1101, 1201, 1202, 1301
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

/// The high-churn tables carry their own autovacuum storage parameters after the engine's full
/// migration set (V102/V303/V405/V607/V805): the outbox queue vacuums after a fixed 5,000 dead
/// tuples, unthrottled; the larger tables at 1% with a 5,000 floor, at the server's throttle.
#[ignore = "docker"]
#[tokio::test]
async fn the_high_churn_tables_carry_their_autovacuum_settings() {
    let pool = crate::fixture::fresh_pool().await;
    let options = |table: &'static str| {
        let pool = pool.clone();
        async move {
            let mut opts: Vec<String> = sqlx::query_scalar(
                "SELECT unnest(reloptions) FROM pg_class \
                 WHERE relname = $1 AND relkind = 'r'",
            )
            .bind(table)
            .fetch_all(&pool)
            .await
            .unwrap();
            opts.sort();
            opts
        }
    };
    assert_eq!(
        options("outbox_entry").await,
        vec![
            "autovacuum_analyze_scale_factor=0",
            "autovacuum_analyze_threshold=5000",
            "autovacuum_vacuum_cost_delay=0",
            "autovacuum_vacuum_scale_factor=0",
            "autovacuum_vacuum_threshold=5000",
        ]
    );
    for table in [
        "alias_index",
        "inbox_seen",
        "instance_state",
        "waiting_event",
    ] {
        assert_eq!(
            options(table).await,
            vec![
                "autovacuum_analyze_scale_factor=0.02",
                "autovacuum_analyze_threshold=5000",
                "autovacuum_vacuum_scale_factor=0.01",
                "autovacuum_vacuum_threshold=5000",
            ],
            "{table}"
        );
    }
}

fn no_transaction_script(version: i64) -> MigrationScript {
    MigrationScript {
        version,
        description: "rebuild_t_key".to_owned(),
        path: PathBuf::from(format!("V{version}__rebuild_t_key.sql")),
        sql: "-- sutra:no-transaction\n\
              -- Rebuilt concurrently, so writes to t are never blocked.\n\
              DROP INDEX CONCURRENTLY IF EXISTS t_key;\n\
              CREATE UNIQUE INDEX CONCURRENTLY t_key ON t (a) WHERE a IS NOT NULL;\n"
            .to_owned(),
    }
}

async fn index_is_valid(pool: &sqlx::PgPool, name: &str) -> Option<bool> {
    sqlx::query_scalar(
        "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE c.relname = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// A script marked `-- sutra:no-transaction` runs its statements in autocommit (a concurrent
/// index build is refused inside a transaction), and its DROP first clears an index a crashed
/// build left invalid.
#[ignore = "docker"]
#[tokio::test]
async fn a_no_transaction_script_rebuilds_an_invalid_index() {
    let (pool, _) = fresh_unmigrated().await;
    sqlx::raw_sql("CREATE TABLE t (a TEXT); CREATE UNIQUE INDEX t_key ON t (a);")
        .execute(&pool)
        .await
        .unwrap();
    // What a crash inside CREATE INDEX CONCURRENTLY leaves behind.
    sqlx::query(
        "UPDATE pg_index SET indisvalid = false \
         WHERE indexrelid = 't_key'::regclass",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(index_is_valid(&pool, "t_key").await, Some(false));

    let mut conn = pool.acquire().await.unwrap();
    let ran = apply_migrations(&mut conn, &[no_transaction_script(9001)])
        .await
        .unwrap();
    assert_eq!(ran, 1);
    drop(conn);
    assert_eq!(index_is_valid(&pool, "t_key").await, Some(true));
    let partial: String =
        sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE indexname = 't_key'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(partial.contains("WHERE (a IS NOT NULL)"), "{partial}");
    let recorded: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sutra_schema_history WHERE version = 9001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recorded, 1);
}

/// Two engines booting together on one database apply a no-transaction script once: the second
/// waits on the runner's advisory lock, re-reads the ledger and skips it.
#[ignore = "docker"]
#[tokio::test]
async fn two_runners_apply_it_once() {
    let (pool, _) = fresh_unmigrated().await;
    sqlx::raw_sql("CREATE TABLE t (a TEXT);")
        .execute(&pool)
        .await
        .unwrap();
    // The ledger table first: both runners creating it at once is not what this proves.
    let mut setup = pool.acquire().await.unwrap();
    apply_migrations(&mut setup, &[]).await.unwrap();
    drop(setup);

    let scripts = [no_transaction_script(9002)];
    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();
    let (a, b) = tokio::join!(
        apply_migrations(&mut first, &scripts),
        apply_migrations(&mut second, &scripts)
    );
    assert_eq!(a.unwrap() + b.unwrap(), 1, "exactly one runner applied it");
    drop((first, second));
    let recorded: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sutra_schema_history WHERE version = 9002")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recorded, 1);
    assert_eq!(index_is_valid(&pool, "t_key").await, Some(true));
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
