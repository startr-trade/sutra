//! `sutra migrate` proof against a real PostgreSQL: fresh database →
//! embedded set applies and every table family exists (channel V7xx included) → second
//! run is an idempotent no-op → `status` reports clean → a mutated ledger checksum makes
//! `verify` fail with a drift diagnostic. Requires docker (like the persistence suite).

use std::sync::OnceLock;

use sutra_cli::commands::migrate::{
    execute, ConnectionArgs, MigrateAction, MigrateArgs, StatusArgs, VerifyArgs,
};
use sutra_cli::output::Io;
use sutra_cli::{exit, GlobalArgs};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
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

fn db_url(db: &str) -> String {
    format!(
        "postgres://postgres:postgres@127.0.0.1:{}/{db}",
        container_port()
    )
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn create_database(name: &str) {
    block_on(async {
        use sqlx::Connection;
        let mut admin = sqlx::PgConnection::connect(&db_url("postgres"))
            .await
            .expect("admin connect");
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .expect("create database");
    });
}

fn query_scalar_i64(db: &str, sql: &str) -> i64 {
    block_on(async {
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(&db_url(db))
            .await
            .expect("connect");
        sqlx::query_scalar(sql).fetch_one(&mut conn).await.unwrap()
    })
}

fn exec_sql(db: &str, sql: &str) {
    block_on(async {
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(&db_url(db))
            .await
            .expect("connect");
        sqlx::raw_sql(sql).execute(&mut conn).await.unwrap();
    });
}

/// Runs a migrate invocation with captured streams; returns (code, stdout, stderr).
fn run(args: MigrateArgs) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        execute(args, &GlobalArgs::default(), &mut io)
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn conn_args(db: &str, creds_via_flags: bool) -> ConnectionArgs {
    let url = if creds_via_flags {
        format!("postgresql://127.0.0.1:{}/{db}", container_port())
    } else {
        db_url(db)
    };
    ConnectionArgs {
        url: Some(url),
        // A credential-less URL — exercise the user/password flags instead.
        user: creds_via_flags.then(|| "postgres".to_owned()),
        password: creds_via_flags.then(|| "postgres".to_owned()),
        schema: None,
        migrations: None,
    }
}

fn apply_args(db: &str, dry_run: bool) -> MigrateArgs {
    MigrateArgs {
        conn: conn_args(db, false),
        dry_run,
        action: None,
    }
}

const EXPECTED_TABLES: [&str; 10] = [
    "alias_index",
    "audit_event",
    "inbox_seen",
    "instance_state",
    "lease",
    "outbox_entry",
    "external_task",
    "channel_instance",
    "waiting_event",
    "deployment_archive",
];

#[ignore = "docker"]
#[test]
fn migrate_apply_status_verify_and_drift_detection() {
    let db = "sutra_cli_it";
    create_database(db);

    // --dry-run against the virgin database plans everything and applies nothing. Derive the
    // embedded-set size and head from the plan rows themselves so these assertions track
    // migration growth (a hardcoded "18 of 18"/V1001 rotted silently when V1101/V1201/V1301
    // landed); the floor guard below keeps it a real tripwire.
    //
    // The floor is NOT "the set only ever grows" — that was the original claim and it is no
    // longer true. It dropped 21 -> 19 when coverage moved OUT of the engine database into the
    // deployment's own declared `coverage` store (design §7): `shipped/coverage/V901,V902` were
    // deleted, because the engine owning that SCHEMA is not the same as the engine's DATABASE
    // holding those tables. So a shrink is legal, but only as a deliberate act — if this
    // assertion fires, confirm a migration was intentionally removed and lower the floor in the
    // same commit. It firing on its own is the accident it exists to catch.
    let (code, out, _) = run(apply_args(db, true));
    assert_eq!(code, exit::OK);
    let plan: Vec<&str> = out.lines().filter(|l| l.starts_with("  V")).collect();
    let n = plan.len();
    let head = plan
        .last()
        .and_then(|l| l.split_whitespace().next())
        .expect("dry-run plan has a head migration")
        .to_string();
    assert!(n >= 20, "embedded migration set unexpectedly shrank: {n}");
    assert!(
        out.contains(&format!("Pending migrations ({n} of {n} available):")),
        "{out}"
    );
    assert!(out.contains("--dry-run: no changes applied"), "{out}");
    let ledger_absent: i64 = query_scalar_i64(
        db,
        "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'sutra_schema_history'",
    );
    assert_eq!(ledger_absent, 0, "dry-run must not create the ledger");

    // First real run applies the full embedded set.
    let (code, out, _) = run(apply_args(db, false));
    assert_eq!(code, exit::OK);
    assert!(
        out.contains(&format!("Applied {n} migration(s); head is now {head}")),
        "{out}"
    );

    // Every table family exists — including the channel V7xx family.
    for table in EXPECTED_TABLES {
        let count = query_scalar_i64(db, &format!("SELECT COUNT(*) FROM {table}"));
        assert_eq!(count, 0, "{table} exists and is empty");
    }

    // Second run is an idempotent no-op (exercised through the credential-flag form).
    let (code, out, _) = run(MigrateArgs {
        conn: conn_args(db, true),
        dry_run: false,
        action: None,
    });
    assert_eq!(code, exit::OK);
    assert!(out.contains("Nothing to apply"), "{out}");

    // status is clean: everything applied, nothing pending.
    let (code, out, _) = run(MigrateArgs {
        conn: conn_args(db, false),
        dry_run: false,
        action: Some(MigrateAction::Status(StatusArgs {
            conn: conn_args(db, false),
        })),
    });
    assert_eq!(code, exit::OK);
    assert!(
        out.contains(&format!(
            "Schema migration status: {n} applied, 0 pending, {n} available"
        )),
        "{out}"
    );

    // verify is clean.
    let (code, out, _) = run(MigrateArgs {
        conn: conn_args(db, false),
        dry_run: false,
        action: Some(MigrateAction::Verify(VerifyArgs {
            conn: conn_args(db, false),
            expected_head: None,
        })),
    });
    assert_eq!(code, exit::OK);
    assert!(
        out.contains(&format!("Verification OK: head {head}, {n} applied")),
        "{out}"
    );

    // Mutate one ledger checksum → verify fails with a drift diagnostic.
    exec_sql(
        db,
        "UPDATE sutra_schema_history SET checksum = 'deadbeef' WHERE version = 401",
    );
    let (code, out, _) = run(MigrateArgs {
        conn: conn_args(db, false),
        dry_run: false,
        action: Some(MigrateAction::Verify(VerifyArgs {
            conn: conn_args(db, false),
            expected_head: None,
        })),
    });
    assert_eq!(code, exit::FINDINGS);
    assert!(
        out.contains("[ERROR] SUTRA.MIGRATE.CHECKSUM_DRIFT — checksum drift for V401"),
        "{out}"
    );
    assert!(out.contains("Verification FAILED: 1 finding(s)"), "{out}");
}

#[ignore = "docker"]
#[test]
fn verify_on_a_virgin_database_fails_closed_and_migrations_dir_overrides() {
    let db = "sutra_cli_it_virgin";
    create_database(db);

    // verify against an un-migrated database is a finding (fail-closed gate).
    let (code, out, _) = run(MigrateArgs {
        conn: conn_args(db, false),
        dry_run: false,
        action: Some(MigrateAction::Verify(VerifyArgs {
            conn: conn_args(db, false),
            expected_head: None,
        })),
    });
    assert_eq!(code, exit::FINDINGS);
    assert!(out.contains("SUTRA.MIGRATE.LEDGER_EMPTY"), "{out}");

    // --migrations <dir> replaces the embedded set.
    let dir = std::env::temp_dir().join(format!("sutra-cli-mig-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("V1__probe_table.sql"),
        "CREATE TABLE probe_table (id BIGINT PRIMARY KEY);",
    )
    .unwrap();
    let mut conn = conn_args(db, false);
    conn.migrations = Some(dir.clone());
    let (code, out, _) = run(MigrateArgs {
        conn,
        dry_run: false,
        action: None,
    });
    assert_eq!(code, exit::OK);
    assert!(
        out.contains("Applied 1 migration(s); head is now V1"),
        "{out}"
    );
    let count = query_scalar_i64(db, "SELECT COUNT(*) FROM probe_table");
    assert_eq!(count, 0, "probe_table exists");
    std::fs::remove_dir_all(&dir).ok();
}

#[ignore = "docker"]
#[test]
fn missing_url_is_a_usage_error() {
    let (code, _, err) = run(MigrateArgs {
        conn: ConnectionArgs::default(),
        dry_run: false,
        action: None,
    });
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("--url (or SUTRA_DB_URL) is required"), "{err}");
}
