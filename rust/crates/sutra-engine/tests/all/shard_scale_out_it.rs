//! Phase-2 shard scale-out at ENGINE level (execution scale-out §8): the REAL boot path
//! (`serve`) on a FOUR-lane router with a bounded per-lane queue, against a real
//! PostgreSQL (testcontainers) — the production claim CAS, shard-suffixed claim owners
//! (`…-s<i>`, §4) and the lease-gated background loops all live:
//!
//! - a fleet of park/relay instances spawns and resumes to completion through four lanes
//!   with concurrent relays (per-instance serialization under the DB-backed claim);
//! - CHAOS: an instance whose claim is held by a DEAD shard-suffixed owner (the killed
//!   mid-step surrogate — stale heartbeat) bounces its relay `CLAIM_HELD` while the
//!   claim stands, is reclaimed by the `StuckInstanceScanner` (owner-blind sweep, §4),
//!   and then resumes normally.
//!
//! The FULL engine suite's N=4 lane is the env seam (`shard_support.rs`):
//! `SUTRA_ENGINE_SHARDS=4 cargo test -p sutra-engine --test all -- --ignored --skip k8s_`.

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

// ---- fixture (the timer_channel_call_conformance harness, minimally) --------------------

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
        "shard_{}_{}",
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
        .max_connections(5)
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
        "shard-arch-{}-{}",
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
/// pointed at a throwaway local listener (deliveries are not this suite's subject; they
/// must simply not error the flows).
fn stage_main(sink: SocketAddr) -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/resources/conformance/main");
    let root = std::env::temp_dir().join(format!(
        "shard-conf-{}-{}",
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

/// Boot the engine at FOUR lanes with a bounded per-lane queue (the Phase-2 posture
/// under test) and a fast stuck-instance sweep for the chaos case.
async fn boot_n4(package_dir: PathBuf, datasource_url: &str) -> RunningEngine {
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
        outbox_tick_interval: std::time::Duration::from_millis(200),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        // Fast reclaim for the chaos case: sweep every second, claims lapse after 2 s
        // of owner silence (config-load's `claim_timeout > interval` rule holds).
        instance_sweep: sutra_engine::StuckInstanceScannerConfig {
            interval: std::time::Duration::from_secs(1),
            claim_timeout: std::time::Duration::from_secs(2),
        },
        engine_shards: sutra_engine::EngineShardConfig {
            shards: 4,
            queue_capacity: Some(16),
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

async fn post(addr: SocketAddr, path: &str, body: Vec<u8>) -> (u16, String) {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || http_post(addr, &path, "application/json", &body))
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

// ---- 1. four lanes, real claims, concurrent park/relay traffic --------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_n4_engine_runs_a_concurrent_park_relay_fleet_to_completion() {
    let (pool, url) = fresh_db().await;
    let sink = accept_all_server().await;
    let engine = boot_n4(stage_main(sink), &url).await;
    let addr = engine.local_addr;

    // 12 stateful spawns (channel-call parks keyed k0..k11), fired CONCURRENTLY: the
    // arrival lanes fan round-robin, the parks land on whichever lane admitted them.
    let mut spawns = Vec::new();
    for i in 0..12 {
        spawns.push(tokio::spawn(async move {
            post(
                addr,
                "/channels/call-resp-start",
                format!(r#"{{"key":"k{i}"}}"#).into_bytes(),
            )
            .await
        }));
    }
    for spawn in spawns {
        let (status, body) = spawn.await.expect("spawn task");
        assert_eq!(status, 200, "spawn parks: {body}");
    }
    assert_eq!(live_instance_count(&pool).await, 12, "12 instances parked");

    // 12 correlated responses, again CONCURRENT: each relay resolves on its arrival
    // lane, hops to its instance's owner lane where needed, claims under that lane's
    // `…-s<i>` owner, and resumes. Every one must complete — a double-resume or a
    // wrongly-granted re-entrant claim would surface as an error or a stuck LIVE row.
    let mut relays = Vec::new();
    for i in 0..12 {
        relays.push(tokio::spawn(async move {
            post(
                addr,
                "/channels/call-response",
                format!(r#"{{"key":"k{i}","status":"done"}}"#).into_bytes(),
            )
            .await
        }));
    }
    for relay in relays {
        let (status, body) = relay.await.expect("relay task");
        assert_eq!(status, 200, "relay resumes: {body}");
    }
    assert!(
        wait_until(10, || async { live_instance_count(&pool).await == 0 }).await,
        "every instance reached terminal (live = {})",
        live_instance_count(&pool).await
    );

    engine.shutdown().await;
}

// ---- 2. chaos: a dead lane's stale claim bounces, is swept, then resumes ----------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_stale_shard_suffixed_claim_bounces_then_is_reclaimed_and_the_relay_resumes() {
    let (pool, url) = fresh_db().await;
    let sink = accept_all_server().await;
    let engine = boot_n4(stage_main(sink), &url).await;
    let addr = engine.local_addr;

    let (status, body) = post(
        addr,
        "/channels/call-resp-start",
        br#"{"key":"chaos-1"}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 200, "spawn parks: {body}");
    assert_eq!(live_instance_count(&pool).await, 1);

    // The killed-mid-step surrogate (§4): a SHARD-SUFFIXED owner from a dead process
    // holds the claim, heartbeat long lapsed. The owner string is opaque to the store
    // and the sweeper — `-s3` proves the sweep is owner-format-blind.
    let stamped = sqlx::query(
        "UPDATE instance_state SET claim_owner = 'deadhost-99-deadbeef-s3', \
         claimed_at = now() - interval '10 minutes', \
         last_heartbeat_at = now() - interval '10 minutes' \
         WHERE terminal_at IS NULL",
    )
    .execute(&pool)
    .await
    .expect("stamp stale claim")
    .rows_affected();
    assert_eq!(stamped, 1, "the parked instance carries the stale claim");

    // While the claim stands, the relay REFUSES to resume concurrently: CLAIM_HELD,
    // nothing executed — the visible, retry-safe bounce (never interleaving). Raced
    // against the 1 s sweep, so a fast reclaim can legally answer 200 already; what is
    // ILLEGAL is any other failure shape.
    let (status, body) = post(
        addr,
        "/channels/call-response",
        br#"{"key":"chaos-1","status":"done"}"#.to_vec(),
    )
    .await;
    if status != 200 {
        assert_eq!(
            status, 500,
            "the bounce surfaces as the problem shape: {body}"
        );
        assert!(
            body.contains("SUTRA.RUNTIME.RESUME.CLAIM_HELD"),
            "the bounce names CLAIM_HELD: {body}"
        );
        assert_eq!(
            live_instance_count(&pool).await,
            1,
            "the bounced relay executed nothing"
        );
    }

    // The StuckInstanceScanner clears the lapsed claim (owner-blind, heartbeat-aged).
    assert!(
        wait_until(15, || async {
            count(
                &pool,
                "SELECT COUNT(*) FROM instance_state WHERE claim_owner IS NOT NULL",
            )
            .await
                == 0
                || live_instance_count(&pool).await == 0
        })
        .await,
        "the scanner reclaims the stale shard-suffixed claim"
    );

    // And the relay now resumes the instance to completion.
    if live_instance_count(&pool).await != 0 {
        let (status, body) = post(
            addr,
            "/channels/call-response",
            br#"{"key":"chaos-1","status":"done"}"#.to_vec(),
        )
        .await;
        assert_eq!(status, 200, "the reclaimed instance resumes: {body}");
    }
    assert!(
        wait_until(10, || async { live_instance_count(&pool).await == 0 }).await,
        "the chaos instance reached terminal"
    );

    engine.shutdown().await;
}
