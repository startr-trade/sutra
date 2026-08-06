//! Boot-time RLS posture checks, proven on real PostgreSQL.
//!
//! Attribute half: a safe role (NOBYPASSRLS, not superuser) clears; a superuser or a
//! BYPASSRLS role is a `SUTRA.STARTUP.RLS_BYPASS_RISK` ERROR that refuses boot; the opt-out
//! (`enabled=false`) downgrades the same role to a `SUTRA.STARTUP.RLS_BYPASS_ACKNOWLEDGED`
//! WARNING that lets boot continue.
//!
//! Enforcement half: `NOBYPASSRLS` alone does not make the policies bind. The second test
//! migrates a database AS a non-superuser role — reproducing the shipped posture where the
//! engine role owns its own tables — and proves the owner-without-FORCE case raises
//! `SUTRA.STARTUP.RLS_INERT_POSTURE` (a WARNING that must NOT refuse boot), while either
//! remediation (a non-owning role, or `FORCE ROW LEVEL SECURITY`) clears it.
//!
//! Hermetic postgres:16-alpine via testcontainers.

use std::path::PathBuf;
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::rls_check::{
    check_rls_bypass_posture, check_rls_enforcement_posture, enforce_rls_bypass_posture, Severity,
    STARTUP_RLS_BYPASS_ACKNOWLEDGED, STARTUP_RLS_BYPASS_RISK, STARTUP_RLS_INERT_POSTURE,
};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};

static CONTAINER: OnceLock<(
    testcontainers::Container<testcontainers_modules::postgres::Postgres>,
    u16,
)> = OnceLock::new();

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        std::thread::spawn(|| {
            use testcontainers::runners::SyncRunner;
            use testcontainers::ImageExt;
            let container = testcontainers_modules::postgres::Postgres::default()
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

#[tokio::test]
#[ignore = "docker"]
async fn rls_bypass_posture_refuses_risky_roles_and_clears_safe_ones() {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    for stmt in [
        "DROP ROLE IF EXISTS sutra_rls_safe",
        "DROP ROLE IF EXISTS sutra_rls_bypass",
        "CREATE ROLE sutra_rls_safe LOGIN PASSWORD 'pw' NOSUPERUSER NOBYPASSRLS",
        "CREATE ROLE sutra_rls_bypass LOGIN PASSWORD 'pw' NOSUPERUSER BYPASSRLS",
    ] {
        sqlx::query(stmt)
            .execute(&admin)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    drop(admin);

    // Arm 1 — safe role (NOBYPASSRLS, not superuser): clean, boot proceeds.
    let safe = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://sutra_rls_safe:pw@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("safe pool");
    let report = check_rls_bypass_posture(&safe, true).await;
    assert!(
        report.diagnostics.is_empty(),
        "a safe role must emit no diagnostics: {report:?}"
    );
    assert!(!report.has_errors());
    assert!(enforce_rls_bypass_posture(&safe, true).await.is_ok());
    drop(safe);

    // Arm 2 — superuser role, check ENABLED: RLS_BYPASS_RISK ERROR, boot refused.
    let superuser = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("superuser pool");
    let report = check_rls_bypass_posture(&superuser, true).await;
    assert_eq!(report.diagnostics.len(), 1);
    let diag = &report.diagnostics[0];
    assert_eq!(diag.code, STARTUP_RLS_BYPASS_RISK);
    assert_eq!(diag.severity, Severity::Error);
    assert!(diag.super_user, "the postgres role is a superuser");
    assert!(report.has_errors());
    let refusal = enforce_rls_bypass_posture(&superuser, true).await;
    assert!(
        refusal.is_err(),
        "a risky role must refuse boot when the check is enabled"
    );
    assert!(refusal.unwrap_err().contains("RLS_BYPASS_RISK"));

    // Arm 3 — same superuser role, check DISABLED (opt-out): RLS_BYPASS_ACKNOWLEDGED
    // WARNING, boot continues.
    let report = check_rls_bypass_posture(&superuser, false).await;
    assert_eq!(report.diagnostics.len(), 1);
    assert_eq!(report.diagnostics[0].code, STARTUP_RLS_BYPASS_ACKNOWLEDGED);
    assert_eq!(report.diagnostics[0].severity, Severity::Warning);
    assert!(!report.has_errors());
    assert!(
        enforce_rls_bypass_posture(&superuser, false).await.is_ok(),
        "the acknowledged opt-out must let boot continue"
    );
    drop(superuser);

    // Arm 4 — NOSUPERUSER but BYPASSRLS: risky via the bypass attribute alone.
    let bypass = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://sutra_rls_bypass:pw@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("bypass pool");
    let report = check_rls_bypass_posture(&bypass, true).await;
    assert_eq!(report.diagnostics.len(), 1);
    assert_eq!(report.diagnostics[0].code, STARTUP_RLS_BYPASS_RISK);
    assert!(report.diagnostics[0].bypass_rls);
    assert!(!report.diagnostics[0].super_user);
    assert!(report.has_errors());
    drop(bypass);
}

/// The shipped migration roots (read-only reference into the repo tree).
fn shipped_migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    vec![repo.join("rust/crates/sutra-persistence/migrations/shipped/core")]
}

/// Creates `db` owned by a fresh non-superuser `NOBYPASSRLS` role and applies the shipped
/// migrations OVER A CONNECTION AS THAT ROLE — so the role owns every engine table, which is
/// precisely the posture a single-role deployment ends up in. Returns a pool as the owner.
async fn migrated_db_owned_by_app_role(port: u16, db: &str, owner: &str) -> PgPool {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("DROP ROLE IF EXISTS {owner}"),
        format!("CREATE ROLE {owner} LOGIN PASSWORD 'pw' NOSUPERUSER NOBYPASSRLS"),
        // Database ownership carries CREATE on the `public` schema through
        // `pg_database_owner` — PostgreSQL 15+ no longer grants it to PUBLIC.
        format!("CREATE DATABASE {db} OWNER {owner}"),
    ] {
        sqlx::query(&stmt)
            .execute(&admin)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    drop(admin);

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://{owner}:pw@127.0.0.1:{port}/{db}"))
        .await
        .expect("owner pool");
    let roots = shipped_migration_roots();
    let refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire for migration");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);
    pool
}

#[tokio::test]
#[ignore = "docker"]
async fn rls_inert_posture_is_flagged_for_owners_and_cleared_by_either_remediation() {
    let port = container_port();
    let db = "sutra_rls_posture";
    let owner = "sutra_rls_owner";
    let owner_pool = migrated_db_owned_by_app_role(port, db, owner).await;

    // Arm 1 — the SHIPPED posture: NOBYPASSRLS (so the attribute gate is clean) but the role
    // owns the tables and none are FORCEd. Every policy is declared and none of them binds.
    assert!(
        check_rls_bypass_posture(&owner_pool, true)
            .await
            .diagnostics
            .is_empty(),
        "the owner role is NOBYPASSRLS and not a superuser — the attribute gate must be clean"
    );
    let report = check_rls_enforcement_posture(&owner_pool).await;
    assert_eq!(report.diagnostics.len(), 1, "one aggregate diagnostic");
    let diag = &report.diagnostics[0];
    assert_eq!(diag.code, STARTUP_RLS_INERT_POSTURE);
    assert_eq!(diag.severity, Severity::Warning);
    assert!(
        !report.has_errors(),
        "the inert posture is NON-FATAL — dev/test must keep booting"
    );
    assert_eq!(diag.role, owner);
    for table in [
        "public.instance_state",
        "public.outbox_entry",
        "public.waiting_event",
        "public.channel_instance",
    ] {
        assert!(
            diag.inert_tables.iter().any(|t| t == table),
            "{table} carries RLS with no FORCE and is owned by {owner}: {:?}",
            diag.inert_tables
        );
    }
    assert!(
        diag.message.contains("FORCE ROW LEVEL SECURITY")
            && diag.message.contains("NOBYPASSRLS")
            && diag.message.contains("INERT"),
        "the message must name the remediation: {}",
        diag.message
    );
    // Boot proceeds regardless — only the bypass-attribute gate refuses.
    assert!(
        enforce_rls_bypass_posture(&owner_pool, true).await.is_ok(),
        "an inert posture must never refuse startup"
    );

    // Arm 2 — remediation A: a dedicated NON-OWNING NOBYPASSRLS role. Nothing changes about
    // the tables; the policies bind because PostgreSQL only exempts the owner.
    let app = "sutra_rls_nonowner";
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    for stmt in [
        format!("DROP ROLE IF EXISTS {app}"),
        format!("CREATE ROLE {app} LOGIN PASSWORD 'pw' NOSUPERUSER NOBYPASSRLS"),
    ] {
        sqlx::query(&stmt)
            .execute(&admin)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    drop(admin);
    let app_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("postgres://{app}:pw@127.0.0.1:{port}/{db}"))
        .await
        .expect("non-owning app pool");
    assert!(
        check_rls_enforcement_posture(&app_pool)
            .await
            .diagnostics
            .is_empty(),
        "a non-owning role is subject to every policy — nothing is inert"
    );
    drop(app_pool);

    // Arm 3 — remediation B: keep the owning role, FORCE every flagged table. Applying the
    // diagnostic's own table list is the remediation the message prescribes.
    for table in &diag.inert_tables {
        sqlx::query(&format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"))
            .execute(&owner_pool)
            .await
            .unwrap_or_else(|e| panic!("FORCE {table}: {e}"));
    }
    assert!(
        check_rls_enforcement_posture(&owner_pool)
            .await
            .diagnostics
            .is_empty(),
        "FORCE ROW LEVEL SECURITY subjects the owner to its own policies"
    );
    drop(owner_pool);
}
