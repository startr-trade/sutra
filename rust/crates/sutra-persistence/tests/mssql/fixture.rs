//! Shared SQL Server fixture: one `mcr.microsoft.com/mssql/server:2022-latest` container
//! per test binary, a uniquely-named database per test, migrations applied through the
//! dialect runner from `migrations_mssql/**`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sutra_persistence::migrate::collect_migrations;
use sutra_persistence::mssql::migrate::apply_migrations;
use sutra_persistence::mssql::{MssqlConfig, MssqlPool};
use sutra_persistence::DeploymentId;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;

static CONTAINER: OnceLock<(Container<MssqlServer>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Start on a dedicated OS thread: the blocking testcontainers runner drives its
        // own runtime and must not be entered from inside a tokio worker.
        std::thread::spawn(|| {
            let container = MssqlServer::default()
                .with_accept_eula()
                .with_tag("2022-latest")
                .start()
                .expect("start mssql/server:2022-latest (docker required, ~2 GB)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(1433).expect("mapped 1433");
            (container, port)
        })
        .join()
        .expect("container bootstrap thread")
    });
    *port
}

/// Connection settings for `db` on the suite container (SA + module default password;
/// the container's self-signed certificate is trusted).
pub fn config_for(db: &str) -> MssqlConfig {
    MssqlConfig {
        host: "127.0.0.1".to_owned(),
        port: container_port(),
        database: db.to_owned(),
        user: "sa".to_owned(),
        password: MssqlServer::DEFAULT_SA_PASSWORD.to_owned(),
        trust_cert: true,
    }
}

/// The crate-shipped SQL Server migration tree.
pub fn migration_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations_mssql")
}

/// Creates a fresh, fully-migrated database and returns a pool on it.
pub async fn fresh_pool() -> MssqlPool {
    let (pool, _) = fresh_pool_named().await;
    pool
}

/// Like [`fresh_pool`] but also returns the database name.
pub async fn fresh_pool_named() -> (MssqlPool, String) {
    let db = create_database().await;
    let pool = MssqlPool::new(config_for(&db));

    let root = migration_root();
    let scripts = collect_migrations(&[root.as_path()]).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire for migration");
    apply_migrations(conn.client(), &scripts)
        .await
        .expect("apply migrations");
    drop(conn);

    (pool, db)
}

/// Creates an empty database on the suite container and returns its name.
pub async fn create_database() -> String {
    let db = format!("sutra_test_{}", DB_SEQ.fetch_add(1, Ordering::Relaxed));
    let master = MssqlPool::new(config_for("master"));
    let mut conn = master.acquire().await.expect("master connect");
    conn.client()
        .simple_query(format!("CREATE DATABASE [{db}]"))
        .await
        .expect("create test database")
        .into_results()
        .await
        .expect("create test database result");
    db
}

/// `COUNT_BIG(*)` over `table` without any deployment bind (fixture-level bookkeeping).
pub async fn count_all(pool: &MssqlPool, table: &str) -> i64 {
    let mut conn = pool.acquire().await.unwrap();
    let row = conn
        .client()
        .simple_query(format!("SELECT COUNT_BIG(*) AS n FROM {table}"))
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .expect("count row");
    row.get::<i64, _>("n").expect("count value")
}

/// Two distinct well-formed deployment ids used across the suite.
pub fn dep_a() -> DeploymentId {
    DeploymentId::new("dep-aaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

/// See [`dep_a`].
pub fn dep_b() -> DeploymentId {
    DeploymentId::new("dep-bbbbbbbbbbbbbbbbbbbbbbbb").unwrap()
}

/// `now()` truncated to whole microseconds — `DATETIME2(6)` resolution — so values
/// round-trip equal through the database.
pub fn now_micros() -> time::OffsetDateTime {
    let now = time::OffsetDateTime::now_utc();
    now - time::Duration::nanoseconds(i64::from(now.nanosecond() % 1_000))
}
