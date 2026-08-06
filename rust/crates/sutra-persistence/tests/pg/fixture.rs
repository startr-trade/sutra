//! Shared PostgreSQL fixture: one `postgres:16-alpine` container per test binary (a
//! per-binary singleton), a uniquely-named database per test, migrations
//! applied through the crate's ordered runner from the shipped SQL trees (read-only).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::DeploymentId;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);
static ROLE_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Start on a dedicated OS thread: the blocking testcontainers runner drives its own
        // runtime and must not be entered from inside a tokio worker.
        std::thread::spawn(|| {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("start postgres:16-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5432).expect("mapped 5432");
            (container, port)
        })
        .join()
        .expect("container bootstrap thread")
    });
    *port
}

fn admin_url() -> String {
    format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        container_port()
    )
}

/// Superuser URL for `db` on the suite container.
pub fn db_url(db: &str) -> String {
    format!(
        "postgres://postgres:postgres@127.0.0.1:{}/{db}",
        container_port()
    )
}

/// The engine-shipped migration roots (read-only reference into the reference tree).
pub fn shipped_migration_roots() -> Vec<PathBuf> {
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

/// The Rust-only migration addendum (the V803 `waiting_event` TIMER marker; timers do not
/// exist in the reference baseline, so this SQL lives in the Rust tree).
pub fn rust_migration_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations/native")
}

/// Every migration root the engine applies (shipped set + Rust addenda). Coverage is NOT among
/// them: since `datastore-schema-projection.md` §7 its tables live in the deployment's own
/// declared `coverage` store, migrated there by `sutra-datastore` with engine-shipped DDL.
pub fn all_migration_roots() -> Vec<PathBuf> {
    let mut roots = shipped_migration_roots();
    roots.push(rust_migration_root());
    roots
}

/// Creates a fresh, fully-migrated database and returns a pool on it (as the superuser /
/// table owner — RLS-bypassing, like the fixture's default connection).
pub async fn fresh_pool() -> PgPool {
    let (pool, _) = fresh_pool_named().await;
    pool
}

/// Like [`fresh_pool`] but also returns the database name (for tests that open additional
/// role-scoped connections onto the same database).
pub async fn fresh_pool_named() -> (PgPool, String) {
    let admin = PgPoolOptions::new()
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

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url(&db))
        .await
        .expect("test db connect");

    let roots = all_migration_roots();
    let refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire for migration");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);

    (pool, db)
}

/// Creates a per-test unique `NOBYPASSRLS` application role (roles are cluster-scoped, so
/// the name must be unique across parallel tests) with CRUD grants on `tables`, plus
/// membership for the current user so `SET LOCAL ROLE` works. Returns the role name.
///
/// **Sequence grants ride the table grants.** `audit_event.id` is a `BIGSERIAL`, so an
/// unprivileged writer needs `USAGE, SELECT` on `audit_event_id_seq` as well — a table grant alone
/// leaves every INSERT failing on the sequence rather than on anything the test is about. Any
/// app-role test that writes `audit_event` needs it, so it is granted HERE alongside the table
/// instead of being re-discovered (and worked around locally) by each new one.
pub async fn create_app_role(pool: &PgPool, tables: &[&str]) -> String {
    let role = format!("sutra_app_{}", ROLE_SEQ.fetch_add(1, Ordering::Relaxed));
    let n = std::process::id();
    let role = format!("{role}_{n}");
    sqlx::query(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'app' NOBYPASSRLS"
    ))
    .execute(pool)
    .await
    .expect("create app role");
    sqlx::query(&format!("GRANT {role} TO CURRENT_USER"))
        .execute(pool)
        .await
        .expect("grant role");
    for table in tables {
        sqlx::query(&format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {role}"
        ))
        .execute(pool)
        .await
        .expect("grant table");
        if *table == "audit_event" {
            sqlx::query(&format!(
                "GRANT USAGE, SELECT ON SEQUENCE audit_event_id_seq TO {role}"
            ))
            .execute(pool)
            .await
            .expect("grant the audit_event BIGSERIAL sequence");
        }
    }
    role
}

/// Opens a pool on `db` authenticated AS `role` (LOGIN role from [`create_app_role`]) — a
/// genuinely unprivileged, non-owner session where RLS policies engage.
pub async fn role_pool(db: &str, role: &str) -> PgPool {
    let url = format!("postgres://{role}:app@127.0.0.1:{}/{db}", container_port());
    PgPoolOptions::new()
        .max_connections(3)
        .connect(&url)
        .await
        .expect("role connect")
}

/// Two distinct well-formed deployment ids used across the suite.
pub fn dep_a() -> DeploymentId {
    DeploymentId::new("dep-aaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

/// See [`dep_a`].
pub fn dep_b() -> DeploymentId {
    DeploymentId::new("dep-bbbbbbbbbbbbbbbbbbbbbbbb").unwrap()
}

/// `now()` truncated to whole microseconds — PostgreSQL `timestamptz` resolution — so values
/// round-trip equal through the database.
pub fn now_micros() -> time::OffsetDateTime {
    let now = time::OffsetDateTime::now_utc();
    now - time::Duration::nanoseconds(i64::from(now.nanosecond() % 1_000))
}
