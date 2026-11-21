//! Conformance: the engine's database restarted, or killed, under load, with the package in
//! `fixtures/db-restart/`: an HTTP intake that logs each order (`in|<ref>`) and forwards it over a
//! local channel to a worker that counts its runs for the order (`out|<ref>`). Both processes are
//! non-idempotent (the default), and both data stores live on the same PostgreSQL as the engine's
//! own tables, so the outage reaches the outbox, the inbox dedup, the instance claims and the data
//! tasks at once.
//!
//! What must hold: every order the intake accepted (answered 2xx, sent only after its step
//! committed) reaches the worker once the database is back — a forward that fails because the
//! database is unreachable is retried, never consumed and dead-lettered — and no forward is
//! processed twice (every `out|<ref>` count is 1). Nothing is dead-lettered and the outbox drains.
//! The four bookkeeping writes that commit without waiting for their WAL flush (the instance
//! claim, the outbox claim and its lease renewal, the delete of delivered rows) may be lost by the
//! crash; that only redelivers, and the counts show it never reprocesses.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::support::engine::{self, EngineBuilder, EngineHandle, PgFixture};
use crate::support::util;

const API_KEY: &str = "db-restart-conformance-key";

struct Stack {
    pg: PgFixture,
    engine: EngineHandle,
}

static RESTART: OnceLock<Stack> = OnceLock::new();
static CRASH: OnceLock<Stack> = OnceLock::new();

fn stack(slot: &'static OnceLock<Stack>, name: &'static str) -> &'static Stack {
    slot.get_or_init(|| {
        std::thread::spawn(move || {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/db-restart/default--dbrestart--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres(name);
            let engine = EngineBuilder::new(name, &pg)
                .env("DB_RESTART_API_KEY", API_KEY)
                .env(
                    "LOG_DB_URL",
                    format!("postgresql://{}:5432/postgres", pg.container_name),
                )
                .env("LOG_DB_USER", "postgres")
                .env("LOG_DB_PASSWORD", "postgres")
                .env("SUTRA_ENGINE_SHARDS", "4")
                .env("SUTRA_OUTBOX_WORKERS", "8")
                .expected_deployments(1)
                .start(&archives);
            Stack { pg, engine }
        })
        .join()
        .expect("db-restart stack")
    })
}

fn sql(stack: &Stack, query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            stack.pg.container.id(),
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

/// The `out|<ref>` counts for `prefix`, by ref (an unreachable database reads as empty).
fn worker_counts(stack: &Stack, prefix: &str) -> BTreeMap<String, String> {
    sql(
        stack,
        &format!(
            "SELECT substr(store_key, 5) || ' ' || store_value FROM data_store \
             WHERE store_name = 'log' AND store_key LIKE 'out|{prefix}-%'"
        ),
    )
    .lines()
    .filter_map(|l| l.split_once(' '))
    .map(|(r, v)| (r.to_string(), v.trim().to_string()))
    .collect()
}

/// Post `n` orders with refs `<prefix>-<i>`, 16 in flight, adding each ref the intake accepted to
/// `accepted` and counting the refusals.
async fn post_orders(
    port: u16,
    prefix: String,
    n: usize,
    accepted: Arc<Mutex<BTreeSet<String>>>,
    refused: Arc<Mutex<BTreeMap<u16, usize>>>,
) {
    const IN_FLIGHT: usize = 16;
    let client = reqwest::Client::new();
    let mut posts = tokio::task::JoinSet::new();
    for i in 0..n {
        if posts.len() >= IN_FLIGHT {
            let _ = posts.join_next().await;
        }
        let client = client.clone();
        let accepted = Arc::clone(&accepted);
        let refused = Arc::clone(&refused);
        let r = format!("{prefix}-{i:05}");
        posts.spawn(async move {
            let status = client
                .post(format!("http://127.0.0.1:{port}/channels/order-in"))
                .header("Content-Type", "application/json")
                .header("X-Api-Key", API_KEY)
                .body(format!(r#"{{"ref":"{r}"}}"#))
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .map(|r| r.status().as_u16())
                .unwrap_or(0);
            if (200..300).contains(&status) {
                accepted.lock().expect("accepted").insert(r);
            } else {
                *refused.lock().expect("refused").entry(status).or_default() += 1;
            }
        });
    }
    while posts.join_next().await.is_some() {}
}

/// Load the stack, run `outage` once 100 orders are accepted, and check that every accepted order
/// reached the worker exactly once.
async fn survives(slot: &'static OnceLock<Stack>, name: &'static str, outage: impl FnOnce(&str)) {
    const ORDERS: usize = 2000;
    let s = stack(slot, name);
    let accepted = Arc::new(Mutex::new(BTreeSet::new()));
    let refused = Arc::new(Mutex::new(BTreeMap::new()));
    let posting = tokio::spawn(post_orders(
        s.engine.http_port,
        name.to_string(),
        ORDERS,
        Arc::clone(&accepted),
        Arc::clone(&refused),
    ));
    let deadline = Instant::now() + Duration::from_secs(120);
    while accepted.lock().expect("accepted").len() < 100 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = accepted.lock().expect("accepted").len();
    outage(s.pg.container.id());
    posting.await.expect("posting");
    let accepted = accepted.lock().expect("accepted").clone();
    let refused = refused.lock().expect("refused").clone();
    eprintln!(
        "{name}: {} accepted ({before} before the outage), refused by status {refused:?}",
        accepted.len()
    );
    assert!(
        accepted.len() > before,
        "the load went on after the outage: {} accepted, {before} before it",
        accepted.len()
    );

    // Every accepted order reaches the worker once the database is back.
    let deadline = Instant::now() + Duration::from_secs(300);
    let counts = loop {
        let counts = worker_counts(s, name);
        let done = accepted.iter().all(|r| counts.contains_key(r));
        if done || Instant::now() > deadline {
            break counts;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let lost: Vec<&String> = accepted
        .iter()
        .filter(|r| !counts.contains_key(*r))
        .collect();
    assert!(
        lost.is_empty(),
        "{} accepted orders never reached the worker: {:?}; dead letters: {}",
        lost.len(),
        lost.iter().take(10).collect::<Vec<_>>(),
        sql(
            s,
            "SELECT string_agg(process_id || ' ' || failure_code, '; ') FROM dead_letter"
        )
    );
    // Nothing reached the worker twice.
    let twice: Vec<(&String, &String)> = counts.iter().filter(|(_, n)| *n != "1").collect();
    assert!(
        twice.is_empty(),
        "orders processed more than once: {twice:?}"
    );
    // The outbox drains and nothing was dead-lettered.
    let deadline = Instant::now() + Duration::from_secs(60);
    while sql(s, "SELECT count(*) FROM outbox_entry") != "0" && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(sql(s, "SELECT count(*) FROM outbox_entry"), "0");
    assert_eq!(
        sql(s, "SELECT count(*) FROM dead_letter"),
        "0",
        "nothing dead-lettered"
    );
    // Every logged intake that reached the worker is an accepted order or one whose answer the
    // outage swallowed; no worker run without its intake.
    let intakes: BTreeSet<String> = sql(
        s,
        &format!(
            "SELECT substr(store_key, 4) FROM data_store \
             WHERE store_name = 'log' AND store_key LIKE 'in|{name}-%'"
        ),
    )
    .lines()
    .map(str::to_string)
    .collect();
    assert!(
        counts.keys().all(|r| intakes.contains(r)),
        "a worker run has no logged intake"
    );
}

/// PostgreSQL restarted (`docker restart`, a fast shutdown, up again in seconds) while orders are in
/// flight: the pooled connections of the engine and of the data stores all go dead at once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_db_restart_every_accepted_order_is_processed_once_across_a_postgres_restart() {
    let _suite = sutra_testkit::hold_suite!("dbrestart");
    survives(&RESTART, "dbrestart-r", |pg| {
        let restarted = Command::new("docker")
            .args(["restart", "-t", "5", pg])
            .status()
            .expect("docker restart postgres");
        assert!(restarted.success(), "postgres restarted");
    })
    .await;
}

/// PostgreSQL killed (SIGKILL: no shutdown checkpoint, the WAL replayed on start) while orders are
/// in flight, then started again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_db_restart_every_accepted_order_is_processed_once_across_a_postgres_crash() {
    let _suite = sutra_testkit::hold_suite!("dbrestart");
    survives(&CRASH, "dbrestart-c", |pg| {
        let killed = Command::new("docker")
            .args(["kill", "--signal", "KILL", pg])
            .status()
            .expect("docker kill postgres");
        assert!(killed.success(), "postgres was killed");
        std::thread::sleep(Duration::from_secs(2));
        let started = Command::new("docker")
            .args(["start", pg])
            .status()
            .expect("docker start postgres");
        assert!(started.success(), "postgres started again");
    })
    .await;
}
