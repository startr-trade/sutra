//! Tier 2 — the `call-log-load` example against a REAL database: upload a CSV batch, and check
//! the rows it produces.
//!
//! Its sibling [`super::call_log_csv_e2e`] boots the same archive with no datasource, so every
//! valid upload stops at the persistence check and nothing downstream of intake ever runs. That
//! covers the codec half of the example, and it covered it well — but it made the load half
//! structurally untestable, and two defects lived there undisturbed until the example was run by
//! hand:
//!
//!   * a `<dataObject>` declared on the process was invisible to the store write inside the
//!     multi-instance sub-process, so every row was written as `null`; and
//!   * `ack-mode: on-persist` discarded the respond-and-continue receipt, so the caller got an
//!     empty `202` where the flow had rendered a batch id.
//!
//! Neither is reachable without a store, so this test exists to reach them. What it asserts is
//! the example's actual promise: the caller is answered immediately WITH a receipt, the rows land
//! transformed into the storage schema, and re-uploading the same batch converges.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};

const API_KEY: &str = "call-log-it-key";

// ---- fixture ---------------------------------------------------------------------------------

static CONTAINER: OnceLock<(
    testcontainers::Container<testcontainers_modules::postgres::Postgres>,
    u16,
)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

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

async fn fresh_database() -> (String, PgPool) {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    let db = format!("call_log_it_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    drop(admin);
    // The engine's OWN schema. A containerised engine migrates itself from the image's
    // `/opt/sutra/db/migration`; an in-process `serve()` has no such root, so the shipped core
    // scripts are applied here exactly as the other tier-2 suites do.
    {
        use sqlx::ConnectOptions;
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root")
            .to_path_buf();
        let roots = [
            repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
            repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
            repo.join("rust/crates/sutra-persistence/migrations/native"),
        ];
        let refs: Vec<&std::path::Path> = roots.iter().map(|p| p.as_path()).collect();
        let scripts = collect_migrations(&refs).expect("collect migrations");
        let mut conn = sqlx::postgres::PgConnectOptions::new()
            .host("127.0.0.1")
            .port(port)
            .username("postgres")
            .password("postgres")
            .database(&db)
            .connect()
            .await
            .expect("migration connection");
        apply_migrations(&mut conn, &scripts)
            .await
            .expect("apply the engine's shipped core migrations");
    }

    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/{db}");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("pool");
    (format!("postgres://127.0.0.1:{port}/{db}"), pool)
}

fn call_log_deployments_dir() -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/call-log-load/deployments-src/default--call-log--1.0.0");
    let dir = std::env::temp_dir().join(format!(
        "call-log-it-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("deployments dir");
    sutra_loader::assemble_dir(&src, &dir, &sutra_loader::PackageOptions::default())
        .expect("the call-log package seals into one .sutra archive");
    dir
}

fn sample(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/call-log-load/sample")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn config_for(deployments_dir: PathBuf, url: &str) -> EngineConfig {
    EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_secs(1),
        http_port: 0,
        datasource_url: Some(url.to_string()),
        datasource_username: Some("postgres".into()),
        datasource_password: Some("postgres".into()),
        outbox_tick_interval: std::time::Duration::from_secs(5),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    }
}

/// A blocking HTTP/1.1 POST returning `(status, content-type, body)`. `request_id` drives the
/// example's inbox dedup (`dedupKey="header.X-Request-Id"`), so a convergence check has to vary
/// it — otherwise the second upload is absorbed at the door and proves nothing about the write.
fn http_post(
    addr: SocketAddr,
    content_type: &str,
    request_id: &str,
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST /channels/cdr-upload HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: {content_type}\r\nX-Api-Key: {API_KEY}\r\n\
         X-Request-Id: {request_id}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .expect("request head");
    stream.write_all(body).expect("request body");
    stream.flush().expect("flush");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in: {response}"));
    let ct = response
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-type:"))
        .map(|l| l.split_once(':').unwrap().1.trim().to_string())
        .unwrap_or_default();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, ct, body)
}

/// The load is DETACHED — the reply is flushed at the park and the rows arrive afterwards — so
/// the assertion has to wait for them rather than read once.
async fn await_rows(pool: &PgPool, expected: i64) -> i64 {
    for _ in 0..80 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM call_log")
            .fetch_one(pool)
            .await
            .unwrap_or(-1);
        if n == expected {
            return n;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    sqlx::query_scalar("SELECT count(*) FROM call_log")
        .fetch_one(pool)
        .await
        .unwrap_or(-1)
}

// ---- the test --------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_csv_batch_is_receipted_immediately_and_loads_as_typed_rows() {
    let (url, pool) = fresh_database().await;
    std::env::set_var("CDR_UPLOAD_API_KEY", API_KEY);
    std::env::set_var("CALL_LOG_DB_URL", &url);
    std::env::set_var("CALL_LOG_DB_USER", "postgres");
    std::env::set_var("CALL_LOG_DB_PASSWORD", "postgres");

    let engine = serve(config_for(call_log_deployments_dir(), &url))
        .await
        .expect("engine boots on the call-log archive with a datasource");
    let addr = engine.local_addr;

    // ---- the receipt ------------------------------------------------------------------------
    // `ack-mode: on-persist` settles WHEN the caller is answered (now, not when the load ends).
    // `<q:reply continue="true">` settles WHAT with. Both, together — the empty body this used to
    // return is the whole reason a caller could not tell an accepted batch from a lost one.
    let csv = sample("call-logs.csv");
    let (status, ct, body) =
        tokio::task::spawn_blocking(move || http_post(addr, "text/csv", "it-csv-1", &csv))
            .await
            .unwrap();
    assert_eq!(
        status, 202,
        "the load is asynchronous — 202, not 200: {body}"
    );
    assert!(ct.starts_with("application/json"), "ct {ct:?}: {body}");
    let receipt: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("the 202 must carry the rendered receipt ({e}): {body:?}"));
    assert_eq!(receipt["rowsAccepted"], 4, "{body}");
    assert_eq!(receipt["status"], "loading", "{body}");
    assert!(
        receipt["batchId"].as_str().is_some_and(|s| !s.is_empty()),
        "a receipt without a batch id is not a receipt: {body}"
    );

    // ---- the rows ---------------------------------------------------------------------------
    assert_eq!(await_rows(&pool, 4).await, 4, "four rows load");
    let rows = sqlx::query(
        "SELECT entry_id, subscriber, counterparty, bearing, duration_seconds, \
                rated_amount, billable, cell_site FROM call_log ORDER BY entry_id",
    )
    .fetch_all(&pool)
    .await
    .expect("select");

    // Row 1 pins the whole transform: the renames, the vocabulary map
    // (originated -> outgoing), the derived `billable`, and the numeric types the codec applied
    // to untyped CSV cells surviving into typed columns.
    let r = &rows[0];
    assert_eq!(r.get::<String, _>("entry_id"), "CDR-100001");
    assert_eq!(r.get::<String, _>("subscriber"), "+14155550101");
    assert_eq!(r.get::<String, _>("counterparty"), "+14155550187");
    assert_eq!(r.get::<String, _>("bearing"), "outgoing");
    assert_eq!(r.get::<i32, _>("duration_seconds"), 182);
    assert!(r.get::<bool, _>("billable"));
    assert_eq!(
        r.get::<Option<String>, _>("cell_site").as_deref(),
        Some("CELL-0042")
    );

    // Row 2 is the OTHER branch of both conditionals — `received` maps to incoming, and an
    // incoming call is not billable. A transform that ignored `direction` would still pass on
    // row 1 alone.
    let r = &rows[1];
    assert_eq!(r.get::<String, _>("bearing"), "incoming");
    assert!(!r.get::<bool, _>("billable"));

    // ---- convergence ------------------------------------------------------------------------
    // `<q:process idempotent="true">` asserts that re-running the batch converges. It is only
    // true because every row is written by key and the store write is an upsert — so re-uploading
    // must leave four rows, not eight. A fresh request id bypasses inbox dedup on purpose: the
    // claim under test is the WRITE's convergence, not the door's.
    let again = sample("call-logs.csv");
    let (status, _, body) =
        tokio::task::spawn_blocking(move || http_post(addr, "text/csv", "it-csv-2", &again))
            .await
            .unwrap();
    assert_eq!(status, 202, "{body}");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM call_log")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        n, 4,
        "a re-uploaded batch upserts by entryId — it does not append"
    );

    // ---- the other wire form, the same rows ---------------------------------------------------
    let fixed = sample("call-logs.fixed-width.txt");
    let (status, _, body) =
        tokio::task::spawn_blocking(move || http_post(addr, "text/plain", "it-fw-1", &fixed))
            .await
            .unwrap();
    assert_eq!(
        status, 202,
        "the fixed-width form is receipted alike: {body}"
    );
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM call_log")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        n, 4,
        "the fixed-width file carries the SAME four records — same schema, same keys, so the \
         same four rows"
    );

    // ---- and a bad batch still never reaches the store ------------------------------------------
    let bad = sample("call-logs-with-a-bad-row.csv");
    let (status, ct, body) =
        tokio::task::spawn_blocking(move || http_post(addr, "text/csv", "it-bad-1", &bad))
            .await
            .unwrap();
    assert_eq!(
        status, 400,
        "a malformed batch is the CALLER's fault: a 500 would invite a pointless retry: {body}"
    );
    assert!(ct.starts_with("text/csv"), "answered in kind: {ct:?}");
    assert_eq!(
        await_rows(&pool, 4).await,
        4,
        "validation runs at intake on the whole file, so a bad batch writes NOTHING"
    );
}
