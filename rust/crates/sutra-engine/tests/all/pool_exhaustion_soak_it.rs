//! Phase-3 pool-exhaustion soak (execution scale-out §8 Phase 3 + its risk register):
//! S=4 async lanes + the timer poller + the outbox dispatcher, all live and all hitting
//! the ONE engine datasource pool concurrently, with the offered demand deliberately
//! above what the pool can serve at once — verify FAIR PROGRESS and NO DEADLOCK, bounded
//! runtime.
//!
//! What "exhaustion" means here: the engine pool is the shipped fixed size
//! (`sutra_datastore::DEFAULT_MAX_CONNECTIONS` = 8; there is deliberately no config knob
//! to shrink it, and Phase 3 adds no config surface). The soak therefore drives PEAK
//! CONCURRENT ACQUIRERS past it instead: four lanes mid-commit + the poller's bounded
//! concurrent fires (up to S) + the outbox tick + the sweeps exceed the pool, so
//! acquires genuinely QUEUE. The two Phase-3 failure classes this flushes out:
//!
//! - a lingering `block_on` on a now-async path — under the async lanes that is an
//!   immediate panic the very first time the path runs under load;
//! - unfair progress / deadlock while lanes PARK ON THE POOL instead of blocking a
//!   thread: every instance must still reach terminal within the deadline, every outbox
//!   row must drain, and no wait row may survive.
//!
//! Bounded: one wave of 48 concurrent stateful spawns (24 timer parks that self-fire at
//! PT3S through the poller + 24 channel-call parks resumed by 24 concurrent relays),
//! then a completion deadline. No expectation on ordering — only on total progress.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, RunningEngine};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

const API_KEY: &str = "conformance-key";

// ---- fixture (the shard_scale_out_it harness, verbatim) ---------------------------------

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

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

fn migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    vec![
        repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
        repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
        manifest
            .ancestors()
            .nth(1)
            .expect("crates dir")
            .join("sutra-persistence/migrations/native"),
    ]
}

async fn fresh_db() -> (PgPool, String) {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url("postgres"))
        .await
        .expect("admin connect");
    let db = format!(
        "soak_{}_{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create db");
    admin.close().await;

    let url = db_url(&db);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("db connect");
    let roots = migration_roots();
    let refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);
    (pool, url)
}

fn seal_to_archives_dir(package_dir: &Path) -> PathBuf {
    let out = std::env::temp_dir().join(format!(
        "soak-arch-{}-{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(package_dir, &out, &sutra_loader::PackageOptions::default())
        .expect("conformance package seals into one .sutra archive");
    out
}

/// Stage the conformance `main` package with every `*.example` outbox destination
/// pointed at a throwaway local listener (deliveries must not error the flows; the
/// outbox dispatcher DRAINING them under pool pressure is part of the soak).
fn stage_main(sink: SocketAddr) -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/resources/conformance/main");
    let root = std::env::temp_dir().join(format!(
        "soak-conf-{}-{}",
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    copy_patched(&src, &root, &format!("http://{sink}"));
    root
}

fn copy_patched(src: &Path, dst: &Path, sink_base: &str) {
    const SINK_HOSTS: [&str; 7] = [
        "timer-done-slow",
        "timer-done",
        "continue-tail",
        "callout2",
        "callout",
        "timeout",
        "done",
    ];
    std::fs::create_dir_all(dst).expect("stage dir");
    for entry in std::fs::read_dir(src).expect("read resources") {
        let entry = entry.expect("dir entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_patched(&from, &to, sink_base);
        } else {
            let mut content = std::fs::read_to_string(&from).expect("resource is text");
            for host in SINK_HOSTS {
                content = content.replace(
                    &format!("http://{host}.example/"),
                    &format!("{sink_base}/{host}/"),
                );
            }
            std::fs::write(&to, content).expect("stage resource");
        }
    }
}

async fn accept_all_server() -> SocketAddr {
    async fn ok() -> axum::http::StatusCode {
        axum::http::StatusCode::ACCEPTED
    }
    let app = axum::Router::new().fallback(axum::routing::post(ok));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("sink listener");
    let addr = listener.local_addr().expect("sink addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("sink serve");
    });
    addr
}

/// Boot at FOUR lanes with a BOUNDED per-lane queue (backpressure lands on callers), a
/// fast outbox tick and a fast sweep — every pool consumer live and eager.
async fn boot_soak(package_dir: PathBuf, datasource_url: &str) -> RunningEngine {
    let deployments_dir = seal_to_archives_dir(&package_dir);
    serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_secs(2),
        http_port: 0,
        datasource_url: Some(datasource_url.to_string()),
        datasource_username: None,
        datasource_password: None,
        outbox_tick_interval: std::time::Duration::from_millis(100),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: sutra_engine::StuckInstanceScannerConfig {
            interval: std::time::Duration::from_secs(1),
            claim_timeout: std::time::Duration::from_secs(5),
        },
        engine_shards: sutra_engine::EngineShardConfig {
            shards: 4,
            queue_capacity: Some(8),
        },
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        rls_bypass_check_enabled: false,
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    })
    .await
    .expect("the engine boots at sutra.engine.shards = 4")
}

// ---- tiny blocking HTTP client ---------------------------------------------------------

fn http_post(addr: SocketAddr, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: {API_KEY}\r\n\
         Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("request head");
    stream.write_all(body).expect("request body");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    (status, response)
}

async fn post(
    addr: SocketAddr,
    path: &str,
    content_type: &'static str,
    body: Vec<u8>,
) -> (u16, String) {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || http_post(addr, &path, content_type, &body))
        .await
        .expect("post task")
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
}

async fn live_instance_count(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NULL",
    )
    .await
}

async fn wait_until<F, Fut>(secs: u64, mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if probe().await {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    false
}

// ---- the soak ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "docker"]
async fn s4_lanes_poller_and_outbox_make_fair_progress_against_a_saturated_pool() {
    let (pool, url) = fresh_db().await;
    let sink = accept_all_server().await;
    let engine = boot_soak(stage_main(sink), &url).await;
    let addr = engine.local_addr;

    // Wave: 24 timer parks (PT3S self-fire through the poller) + 24 channel-call parks,
    // all in flight at once. 48 concurrent commits against 4 lanes means every lane's
    // mailbox is full and backpressure is live; the parks' step transactions, the
    // poller's fires, the outbox's deliveries and the sweeps all contend for the one
    // fixed-size pool from here on.
    let mut waves = Vec::new();
    for i in 0..24 {
        waves.push(tokio::spawn(async move {
            post(addr, "/channels/timer-start", "text/plain", b"go".to_vec()).await
        }));
        waves.push(tokio::spawn(async move {
            post(
                addr,
                "/channels/call-resp-start",
                "application/json",
                format!(r#"{{"key":"soak{i}"}}"#).into_bytes(),
            )
            .await
        }));
    }
    for wave in waves {
        let (status, body) = wave.await.expect("spawn task");
        assert_eq!(status, 200, "every spawn parks under pool pressure: {body}");
    }
    assert_eq!(live_instance_count(&pool).await, 48, "48 instances parked");

    // The 24 relays land WHILE the 24 timers come due: resumes, fires and outbox drains
    // interleave across the four lanes.
    let mut relays = Vec::new();
    for i in 0..24 {
        relays.push(tokio::spawn(async move {
            post(
                addr,
                "/channels/call-response",
                "application/json",
                format!(r#"{{"key":"soak{i}","status":"done"}}"#).into_bytes(),
            )
            .await
        }));
    }
    for relay in relays {
        let (status, body) = relay.await.expect("relay task");
        assert_eq!(
            status, 200,
            "every relay resumes under pool pressure: {body}"
        );
    }

    // FAIR PROGRESS, bounded: every instance reaches terminal (a FAILED instance keeps
    // terminal_at NULL, so 0 here also proves no flow died to a pool-starved store
    // error), every wait row resolves (no timer refires forever), and the outbox drains
    // to empty (the dispatcher kept claiming under the same pool pressure). A deadlock —
    // a lane parked on the pool holding work no one can finish — shows up here as a
    // stuck count and a timeout, never as a hang past the deadline.
    assert!(
        wait_until(60, || async { live_instance_count(&pool).await == 0 }).await,
        "every instance reached terminal under pool saturation (live = {})",
        live_instance_count(&pool).await
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM waiting_event WHERE status = 'WAITING'",
        )
        .await,
        0,
        "no wait row survives the soak"
    );
    assert!(
        wait_until(30, || async {
            count(&pool, "SELECT COUNT(*) FROM outbox_entry").await == 0
        })
        .await,
        "the outbox drained to empty under the same pool pressure"
    );

    engine.shutdown().await;
}
