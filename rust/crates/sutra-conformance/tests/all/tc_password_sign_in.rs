//! Conformance: a flow signs a user in against a typed store with `password verify`, end to end on
//! the engine image with the package in `fixtures/password-sign-in/`.
//!
//! `enrol` hashes a password with `password hash` and writes the `users` row; `sign-in` reads the
//! row and answers whether the password matched. One user is seeded by the store's migration with a
//! fixed argon2id test vector. Both flows bind their credentials to a source variable declared
//! `@transient` and `@sensitive`, and audit with the payload, so the plaintext must reach no stored
//! row (every table, data only), no audit line and no log line.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "sign-in-conformance-key";
const JSONL_PATH: &str = "/tmp/sutra-audit/audit.jsonl";

/// The seeded user's password: the fixed test vector the migration stores the hash of.
const SEEDED_USER: &str = "ada";
const SEEDED_PASSWORD: &str = "correct horse";
/// Markers chosen so a leak is unambiguous wherever it lands.
const ENROLLED_USER: &str = "grace";
const ENROLLED_PASSWORD: &str = "PLAINTEXT-ENROL-PW-7f3a";
const WRONG_PASSWORD: &str = "PLAINTEXT-WRONG-PW-91c2";

struct Fixture {
    pg: PgFixture,
    engine: engine::EngineHandle,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/password-sign-in/default--signin--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("signin");
            let engine = EngineBuilder::new("signin", &pg)
                .env("SIGN_IN_API_KEY", API_KEY)
                .env("SUTRA_AUDIT_JSONL", JSONL_PATH)
                .env(
                    "STORE_DB_URL",
                    format!("postgresql://{}:5432/postgres", pg.container_name),
                )
                .env("STORE_DB_USER", "postgres")
                .env("STORE_DB_PASSWORD", "postgres")
                .expected_deployments(1)
                .start(&archives);
            Fixture { pg, engine }
        })
        .join()
        .expect("sign-in topology")
    })
}

fn sql(query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            fixture().pg.container.id(),
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAc",
            query,
        ])
        .output()
        .expect("docker exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Every row of every table, as text.
fn database_dump() -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            fixture().pg.container.id(),
            "pg_dump",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "--data-only",
        ])
        .output()
        .expect("docker exec pg_dump");
    assert!(out.status.success(), "pg_dump failed");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn jsonl_file() -> String {
    let dir = util::world_readable_temp_dir("sutra-signin-jsonl");
    let target = dir.join("audit.jsonl");
    let out = Command::new("docker")
        .args([
            "cp",
            &format!("{}:{JSONL_PATH}", fixture().engine.container.id()),
            &target.to_string_lossy(),
        ])
        .output()
        .expect("docker cp");
    let body = if out.status.success() {
        std::fs::read_to_string(&target).unwrap_or_default()
    } else {
        String::new()
    };
    let _ = std::fs::remove_dir_all(&dir);
    body
}

fn engine_logs() -> String {
    let out = Command::new("docker")
        .args(["logs", fixture().engine.container.id()])
        .output()
        .expect("docker logs");
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

async fn post(channel: &str, user: &str, password: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(serde_json::json!({"user": user, "password": password}).to_string())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .unwrap_or_else(|e| panic!("post {channel}: {e}"));
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

async fn sign_in(user: &str, password: &str) -> serde_json::Value {
    let (status, text) = post("sign-in", user, password).await;
    assert!((200..300).contains(&status), "sign-in: {status} {text}");
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_password_sign_in_verifies_and_never_stores_the_password() {
    let _suite = sutra_testkit::hold_suite!("password_sign_in");
    fixture();

    // The seeded user: the migration's row, checked against the fixed vector.
    assert_eq!(
        sign_in(SEEDED_USER, SEEDED_PASSWORD).await["signedIn"],
        true
    );
    assert_eq!(
        sign_in(SEEDED_USER, WRONG_PASSWORD).await["signedIn"],
        false
    );

    // Enrol a user through the flow: the row holds an argon2id hash, never the password.
    let (status, text) = post("enrol", ENROLLED_USER, ENROLLED_PASSWORD).await;
    assert!((200..300).contains(&status), "enrol: {status} {text}");
    let deadline = Instant::now() + Duration::from_secs(30);
    let stored = loop {
        let stored = sql(&format!(
            "SELECT password_hash FROM users WHERE store_key = '{ENROLLED_USER}'"
        ));
        if !stored.is_empty() {
            break stored;
        }
        assert!(
            Instant::now() < deadline,
            "{ENROLLED_USER} was never stored"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        stored.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"),
        "{stored}"
    );
    assert_eq!(
        sign_in(ENROLLED_USER, ENROLLED_PASSWORD).await["signedIn"],
        true
    );
    assert_eq!(
        sign_in(ENROLLED_USER, WRONG_PASSWORD).await["signedIn"],
        false
    );

    // The surfaces are really being looked at: the dump has the stored hash, the audit file has
    // the flows' events.
    let dump = database_dump();
    assert!(dump.contains(&stored), "the dump holds the users rows");
    let audit = jsonl_file();
    assert!(
        audit.lines().any(|l| l.contains("sign-in")),
        "the audit file has the sign-in events"
    );
    let logs = engine_logs();

    let mut leaks = Vec::new();
    for password in [SEEDED_PASSWORD, ENROLLED_PASSWORD, WRONG_PASSWORD] {
        if dump.contains(password) {
            leaks.push(format!("{password} in the database"));
        }
        if audit.contains(password) {
            leaks.push(format!("{password} in the audit file"));
        }
        if logs.contains(password) {
            leaks.push(format!("{password} in the engine log"));
        }
    }
    assert!(leaks.is_empty(), "the plaintext leaked: {leaks:?}");
}
