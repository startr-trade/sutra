//! Conformance: the concurrent outbox dispatcher and the local hand-off (0.6.5), end to end on
//! real PostgreSQL with the relay workload of `tools/sutra-load-test/fixtures/relay`: an HTTP
//! intake, a `local://` forward to a second process, its answer relayed back over a second
//! `local://` channel, and an HTTP callback to a host recorder.
//!
//! What must hold however the deliveries interleave: every order calls back; every intake
//! instance completes exactly once (its two local hops are each consumed once — inbox dedup); the
//! outbox drains to empty; and an engine killed mid-flight (SIGKILL — no drain, no deletes) is
//! recovered by the next engine from the durable rows alone.

use std::collections::BTreeSet;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::callback::{start_recorder, HostRecorder};
use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "relay-conformance-key";

struct Fixture {
    pg: PgFixture,
    recorder: HostRecorder,
    archives: std::path::PathBuf,
}

/// Each test gets its own database and recorder: two engines on one database would be two
/// replicas, which is not what either test is about.
static CONCURRENT: OnceLock<Fixture> = OnceLock::new();
static CRASH: OnceLock<Fixture> = OnceLock::new();
static PG_CRASH: OnceLock<Fixture> = OnceLock::new();

fn fixture(slot: &'static OnceLock<Fixture>) -> &'static Fixture {
    slot.get_or_init(|| {
        std::thread::spawn(|| {
            let package = util::repo_root().join("tools/sutra-load-test/fixtures/relay");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            Fixture {
                pg: engine::start_postgres("relay"),
                recorder: start_recorder(),
                archives,
            }
        })
        .join()
        .expect("relay topology")
    })
}

/// One engine on the shared database. Built on a plain thread (see `EngineHandle`'s drop notes)
/// and leaked into a `'static` so no runtime worker ever drops it.
fn start_engine(slot: &'static OnceLock<Fixture>, lanes: u32) -> &'static engine::EngineHandle {
    let f = fixture(slot);
    let pg_url = format!("postgres://{}:5432/postgres", f.pg.container_name);
    let callback = format!("http://host.docker.internal:{}/callback", f.recorder.port);
    let archives = f.archives.clone();
    let handle = std::thread::spawn(move || {
        EngineBuilder::new("relay", &fixture(slot).pg)
            .env("RELAY_BENCH_API_KEY", API_KEY)
            .env("RELAY_BENCH_CALLBACK_URL", callback)
            .env("RELAY_DB_URL", pg_url)
            .env("RELAY_DB_USER", "postgres")
            .env("RELAY_DB_PASSWORD", "postgres")
            .env("SUTRA_ENGINE_SHARDS", lanes.to_string())
            .env("SUTRA_OUTBOX_WORKERS", "8")
            // A killed engine's ownership claims are released by the stuck-instance sweeper
            // after the claim timeout — shortened here so recovery is seconds, not 5 minutes.
            .env("SUTRA_INSTANCE_CLAIM_TIMEOUT", "PT10S")
            .env("SUTRA_INSTANCE_SWEEP_INTERVAL", "PT2S")
            .host_gateway()
            .expected_deployments(1)
            .start(&archives)
    })
    .join()
    .expect("engine start");
    Box::leak(Box::new(handle))
}

fn sql(slot: &'static OnceLock<Fixture>, query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            fixture(slot).pg.container.id(),
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

fn count(slot: &'static OnceLock<Fixture>, query: &str) -> i64 {
    sql(slot, query).parse().unwrap_or(-1)
}

/// POST `n` orders with refs `<prefix>-<i>`, 16 at a time; returns the refs the intake ACCEPTED
/// (answered 2xx — sent only after the order's step committed synchronously).
async fn post_orders(port: u16, prefix: String, n: usize) -> BTreeSet<String> {
    let client = reqwest::Client::new();
    let mut accepted = BTreeSet::new();
    let refs: Vec<String> = (0..n).map(|i| format!("{prefix}-{i:04}")).collect();
    for chunk in refs.chunks(16) {
        let mut posts = Vec::new();
        for r in chunk {
            let client = client.clone();
            let r = r.clone();
            let body = format!(r#"{{"ref":"{r}","amount":10,"party":"party-a","sentAt":0}}"#);
            posts.push(tokio::spawn(async move {
                let ok = client
                    .post(format!("http://127.0.0.1:{port}/channels/order-in"))
                    .header("Content-Type", "application/json")
                    .header("X-Api-Key", API_KEY)
                    .body(body)
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false);
                (r, ok)
            }));
        }
        for post in posts {
            if let Ok((r, true)) = post.await {
                accepted.insert(r);
            }
        }
    }
    accepted
}

/// The distinct refs with `prefix` the recorder has seen, and how many callbacks in total.
fn called_back(slot: &'static OnceLock<Fixture>, prefix: &str) -> (BTreeSet<String>, usize) {
    let bodies = fixture(slot)
        .recorder
        .recorder
        .matching(&format!("\"{prefix}-"));
    let refs = bodies
        .iter()
        .filter_map(|b| serde_json::from_str::<serde_json::Value>(b).ok())
        .filter_map(|v| v["ref"].as_str().map(str::to_string))
        .collect();
    (refs, bodies.len())
}

async fn await_callbacks(
    slot: &'static OnceLock<Fixture>,
    prefix: &str,
    n: usize,
    within: Duration,
) -> (BTreeSet<String>, usize) {
    let deadline = Instant::now() + within;
    loop {
        let (refs, total) = called_back(slot, prefix);
        if refs.len() >= n || Instant::now() > deadline {
            return (refs, total);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn await_outbox_empty(slot: &'static OnceLock<Fixture>, within: Duration) -> i64 {
    let deadline = Instant::now() + within;
    loop {
        let pending = count(slot, "SELECT count(*) FROM outbox_entry");
        if pending == 0 || Instant::now() > deadline {
            return pending;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The instance-level exactly-once checks for the orders with `prefix`: one intake instance per
/// order that called back, each COMPLETED once, none left parked, every local hop consumed once.
fn assert_exactly_once(slot: &'static OnceLock<Fixture>, prefix: &str, settled: usize) {
    let n = settled as i64;
    let intakes = count(
        slot,
        &format!(
            "SELECT count(DISTINCT instance_id) FROM alias_index \
         WHERE alias_name = 'orderRef' AND alias_value LIKE '{prefix}-%'"
        ),
    );
    assert_eq!(
        intakes, n,
        "one intake instance per settled order, and no other"
    );
    let unfinished = count(slot, &format!(
        "SELECT count(*) FROM instance_state s JOIN alias_index a USING (deployment_id, instance_id) \
         WHERE a.alias_name = 'orderRef' AND a.alias_value LIKE '{prefix}-%' \
           AND s.terminal_at IS NULL"
    ));
    assert_eq!(unfinished, 0, "every intake instance completed");
    for channel in ["worker-in", "answer-in"] {
        let seen = count(
            slot,
            &format!("SELECT count(*) FROM inbox_seen WHERE channel = '{channel}'"),
        );
        assert!(seen >= n, "{channel}: every hop recorded ({seen} < {n})");
    }
}

/// Load `n` orders while `kill` runs once 30 orders have called back; returns the accepted refs.
async fn load_and_kill(
    slot: &'static OnceLock<Fixture>,
    port: u16,
    prefix: &str,
    n: usize,
    kill: impl FnOnce(),
) -> BTreeSet<String> {
    let posting = tokio::spawn(post_orders(port, prefix.to_string(), n));
    let _ = await_callbacks(slot, prefix, 30, Duration::from_secs(60)).await;
    kill();
    let done = called_back(slot, prefix).0.len();
    let accepted = posting.await.expect("posting task");
    assert!(
        done < accepted.len(),
        "the kill landed mid-flight ({done} called back of {} accepted)",
        accepted.len()
    );
    accepted
}

/// Every accepted order called back; the instance checks hold for everything that settled.
async fn assert_recovered(
    slot: &'static OnceLock<Fixture>,
    prefix: &str,
    accepted: &BTreeSet<String>,
) {
    let deadline = Instant::now() + Duration::from_secs(240);
    let (refs, total) = loop {
        let (refs, total) = called_back(slot, prefix);
        if accepted.is_subset(&refs) || Instant::now() > deadline {
            break (refs, total);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let lost: Vec<&String> = accepted.difference(&refs).collect();
    if !lost.is_empty() {
        let mut dump = String::new();
        for r in &lost {
            dump.push_str(&format!(
                "\n{r}: aliases={} | instance={} | waits={} | outbox={} | inbox={}",
                sql(slot, &format!("SELECT string_agg(alias_name||'='||alias_value||' live='||live||' inst='||instance_id, '; ') FROM alias_index WHERE instance_id IN (SELECT instance_id FROM alias_index WHERE alias_value = '{r}')")),
                sql(slot, &format!("SELECT string_agg(instance_id||' terminal='||coalesce(terminal_at::text,'-')||' claim='||coalesce(claim_owner,'-'), '; ') FROM instance_state WHERE instance_id IN (SELECT instance_id FROM alias_index WHERE alias_value = '{r}')")),
                sql(slot, &format!("SELECT string_agg(node_id||' '||status, '; ') FROM waiting_event WHERE instance_id IN (SELECT instance_id FROM alias_index WHERE alias_value = '{r}')")),
                sql(slot, &format!("SELECT string_agg(destination||' due='||next_attempt_at||' att='||attempt_count||' diag='||coalesce(left(last_diagnostic_json,200),'-'), '; ') FROM outbox_entry WHERE instance_id IN (SELECT instance_id FROM alias_index WHERE alias_value = '{r}')")),
                sql(slot, "SELECT count(*) FROM inbox_seen"),
            ));
        }
        panic!("accepted orders never called back: {lost:?}{dump}");
    }
    assert!(total >= refs.len());
    assert_eq!(await_outbox_empty(slot, Duration::from_secs(60)).await, 0);
    // Let in-flight completions of orders whose 202 was lost in the crash settle too.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let settled = called_back(slot, prefix).0.len();
    assert_exactly_once(slot, prefix, settled);
}

/// 300 orders through 4 lanes and 8 workers: every order calls back, nothing is consumed twice,
/// and the outbox drains.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_outbox_relay_settles_every_order_exactly_once_under_concurrency() {
    let slot = &CONCURRENT;
    let engine = start_engine(slot, 4);
    let accepted = post_orders(engine.http_port, "conc".to_string(), 300).await;
    assert_eq!(accepted.len(), 300, "every order accepted");
    let (refs, total) = await_callbacks(slot, "conc", 300, Duration::from_secs(120)).await;
    assert_eq!(refs.len(), 300, "every order called back");
    assert_eq!(total, 300, "no duplicate callback without a crash");
    assert_eq!(await_outbox_empty(slot, Duration::from_secs(30)).await, 0);
    assert_exactly_once(slot, "conc", 300);
}

/// An engine SIGKILLed while orders are in flight (rows committed, hand-offs queued, deletes not
/// yet flushed) is recovered by a fresh engine from the durable rows: every order still calls back
/// (a callback may repeat — at-least-once, idempotency key on the wire), every intake instance
/// completes exactly once, and the outbox drains.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_outbox_relay_recovers_from_a_crash_mid_flight() {
    let slot = &CRASH;
    let first = start_engine(slot, 4);
    let accepted = load_and_kill(slot, first.http_port, "crash", 1500, || {
        let status = Command::new("docker")
            .args(["kill", "--signal", "KILL", first.container.id()])
            .status()
            .expect("docker kill");
        assert!(status.success(), "the engine was killed");
    })
    .await;
    // A new engine on the same database picks the work up from the rows alone (local rows
    // handed off in-process are due again after the hand-off grace).
    let _second = start_engine(slot, 4);
    assert_recovered(slot, "crash", &accepted).await;
}

/// PostgreSQL itself is killed (SIGKILL — no shutdown checkpoint, the WAL replayed on start)
/// while orders are in flight, then started again; the engine keeps running and reconnects. Every
/// order the intake accepted (its `202` was sent only after its step's synchronous commit) still
/// calls back, and every intake instance completes exactly once — whatever bookkeeping commits
/// had not reached the disk are lost and redone, never an acknowledged state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_outbox_relay_recovers_from_a_postgres_crash_mid_flight() {
    let slot = &PG_CRASH;
    let engine = start_engine(slot, 4);
    let pg = fixture(slot).pg.container.id().to_string();
    let accepted = load_and_kill(slot, engine.http_port, "pgcrash", 1500, || {
        let killed = Command::new("docker")
            .args(["kill", "--signal", "KILL", &pg])
            .status()
            .expect("docker kill postgres");
        assert!(killed.success(), "postgres was killed");
        std::thread::sleep(Duration::from_secs(2));
        let started = Command::new("docker")
            .args(["start", &pg])
            .status()
            .expect("docker start postgres");
        assert!(started.success(), "postgres restarted");
    })
    .await;
    assert_recovered(slot, "pgcrash", &accepted).await;
}
