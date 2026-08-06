//! Shared MySQL-dialect fixture: one container per test binary (the enclosing suite's
//! `start_db` decides the engine — mysql:8.0 or mariadb:11), a uniquely-named database
//! per test, migrations applied through the dialect runner from `migrations_mysql/**`.

use std::any::Any;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use sutra_persistence::migrate::collect_migrations;
use sutra_persistence::mysql::migrate::apply_migrations;
use sutra_persistence::DeploymentId;

static CONTAINER: OnceLock<(Box<dyn Any + Send + Sync>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Start on a dedicated OS thread: the blocking testcontainers runner drives its
        // own runtime and must not be entered from inside a tokio worker.
        std::thread::spawn(crate::start_db)
            .join()
            .expect("container bootstrap thread")
    });
    *port
}

fn admin_url() -> String {
    // The container images run root with an empty password and ship a `mysql` system db.
    format!("mysql://root@127.0.0.1:{}/mysql", container_port())
}

/// Root URL for `db` on the suite container.
pub fn db_url(db: &str) -> String {
    format!("mysql://root@127.0.0.1:{}/{db}", container_port())
}

/// The crate-shipped MySQL/MariaDB migration tree.
pub fn migration_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations_mysql")
}

/// Creates a fresh, fully-migrated database and returns a pool on it.
pub async fn fresh_pool() -> MySqlPool {
    let (pool, _) = fresh_pool_named().await;
    pool
}

/// Like [`fresh_pool`] but also returns the database name (for tests that open
/// additional connections onto the same database).
pub async fn fresh_pool_named() -> (MySqlPool, String) {
    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url())
        .await
        .expect("admin connect");
    let db = format!("sutra_test_{}", DB_SEQ.fetch_add(1, Ordering::Relaxed));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create test database");
    admin.close().await;

    let pool = MySqlPoolOptions::new()
        .max_connections(10)
        .connect(&db_url(&db))
        .await
        .expect("test db connect");

    let root = migration_root();
    let scripts = collect_migrations(&[root.as_path()]).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire for migration");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);

    (pool, db)
}

/// Two distinct well-formed deployment ids used across the suite.
pub fn dep_a() -> DeploymentId {
    DeploymentId::new("dep-aaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

/// See [`dep_a`].
pub fn dep_b() -> DeploymentId {
    DeploymentId::new("dep-bbbbbbbbbbbbbbbbbbbbbbbb").unwrap()
}

/// `now()` truncated to whole microseconds — `DATETIME(6)` resolution — so values
/// round-trip equal through the database.
pub fn now_micros() -> time::OffsetDateTime {
    let now = time::OffsetDateTime::now_utc();
    now - time::Duration::nanoseconds(i64::from(now.nanosecond() % 1_000))
}
