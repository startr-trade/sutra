//! Conformance: money-transfer per-channel singleton across replicas.
//!
//! Three engine replicas share one internal Postgres (the `lease` table the leader election
//! contends on), one `accounts` Postgres (the shared ledger), and one RabbitMQ broker. The
//! `transfer-queue` channel declares `singleton: true` on the rabbitmq transport, so its
//! consumer is leader-gated: exactly ONE replica subscribes. The broker's consumer count is the
//! deterministic proof (1 = singleton OK; 3 = lease wiring missing; 0 = role never registered),
//! and all N published transfers apply to the shared ledger with no lost update.

use std::sync::OnceLock;
use std::time::Duration;

use crate::support::broker;
use crate::support::engine::{self, EngineBuilder, EngineHandle, PgFixture};
use crate::support::util::wait_until;

const API_KEY: &str = "transfer-demo-key";
const REPLICAS: usize = 3;
const TRANSFERS: i64 = 12;
const QUEUE: &str = "transfer-queue-q";
const RABBIT_USER: &str = "mtransfer";
const RABBIT_PASS: &str = "mtransfer-broker-pw";

struct Topology {
    _pg: PgFixture,
    _accounts: PgFixture,
    broker: broker::BrokerFixture,
    replicas: Vec<EngineHandle>,
}

/// Held in a `static` so the container handles are never dropped on a tokio worker (their
/// testcontainers `Drop` would `block_on` docker removal); the atexit reaper removes them.
fn topology() -> &'static Topology {
    static TOPO: OnceLock<Topology> = OnceLock::new();
    TOPO.get_or_init(|| {
        std::thread::spawn(build_topology)
            .join()
            .expect("multi-replica topology")
    })
}

fn build_topology() -> Topology {
    let pg = engine::start_postgres("mr");
    let accounts = engine::start_postgres("mr-accounts");
    let broker = broker::start_broker("mr", "rabbit", RABBIT_USER, RABBIT_PASS);
    let archives = engine::assemble_example("money-transfer");
    // Start replica 0 alone first (runs the engine migrations uncontended); each `start()`
    // blocks until Ready, so the rest come up against a migrated schema and contend for the
    // channel lease.
    let mut replicas = Vec::new();
    for i in 0..REPLICAS {
        let engine = EngineBuilder::new(&format!("mr-{i}"), &pg)
            .env(
                "ACCOUNTS_DB_URL",
                format!("postgresql://{}:5432/postgres", accounts.container_name),
            )
            .env("ACCOUNTS_DB_USER", "postgres")
            .env("ACCOUNTS_DB_PASSWORD", "postgres")
            .env("RABBITMQ_USERNAME", RABBIT_USER)
            .env("RABBITMQ_PASSWORD", RABBIT_PASS)
            .expected_deployments(1)
            .start(&archives);
        replicas.push(engine);
    }
    Topology {
        _pg: pg,
        _accounts: accounts,
        broker,
        replicas,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn tc_multi_replica_singleton_serializes_channel() {
    let topo = topology();
    let broker_port = topo.broker.host_port;
    let engine_port = topo.replicas[0].http_port;
    let client = reqwest::Client::new();

    // The trigger declares the queue passively, so it must pre-exist; the leader-gated consumer
    // subscribes on the next retry once it exists.
    let publish_conn = broker::connect(broker_port, RABBIT_USER, RABBIT_PASS).await;
    broker::declare_durable_queue(&publish_conn, QUEUE).await;

    let carol_before = balance_of(&client, engine_port, "carol").await;
    let bob_before = balance_of(&client, engine_port, "bob").await;

    // 1) The singleton must settle to exactly ONE active broker consumer across the replicas.
    wait_until(
        "consumer count settles to 1",
        // 120s, not 60s: leader election across replicas is lease-driven, and both replicas
        // must boot first — the slowest step on a loaded runner (same reasoning as below).
        Duration::from_secs(120),
        || async {
            broker::consumer_count(broker_port, RABBIT_USER, RABBIT_PASS, QUEUE).await == 1
        },
    )
    .await;

    // 2) Publish N transfers (distinct messageId per message so inbox dedup keeps them distinct).
    let body = "{\"TransferRequest\":{\"fromId\":\"carol\",\"toId\":\"bob\",\"amount\":1}}";
    for i in 0..TRANSFERS {
        broker::publish(
            &publish_conn,
            QUEUE,
            &format!("mr-{}-{i}", std::process::id()),
            "application/json",
            body.as_bytes(),
        )
        .await;
    }

    // 3) The single active consumer drains them; wait until the ledger reflects all N.
    let expected_carol = carol_before - TRANSFERS as f64;
    // Wait on the DEFINITE end-state, and fail with the broker's own view of why — never a bare
    // "timed out". Three distinguishable outcomes instead of one opaque one:
    //   ready > 0            the drain stalled (singleton consumer died / never took over)
    //   ready == 0, short    deliveries were consumed but their effect was lost (a real bug)
    //   ledger == expected   done, returns immediately
    // The budget only bounds the failure report; a healthy run settles in seconds.
    let deadline = std::time::Instant::now() + Duration::from_secs(240);
    let settled = loop {
        let carol = balance_of(&client, engine_port, "carol").await;
        if (carol - expected_carol).abs() < f64::EPSILON {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    if !settled {
        let carol = balance_of(&client, engine_port, "carol").await;
        let bob = balance_of(&client, engine_port, "bob").await;
        let (ready, consumers) =
            broker::queue_stats(broker_port, RABBIT_USER, RABBIT_PASS, QUEUE).await;
        panic!(
            "ledger never reflected all {TRANSFERS} transfers.\n  \
             carol: expected {expected_carol}, got {carol} (short by {})\n  \
             bob:   {bob}\n  \
             queue {QUEUE}: {ready} ready, {consumers} active consumer(s)\n  \
             {}",
            expected_carol - carol,
            if ready > 0 {
                "DRAIN STALLED - messages still queued; the singleton consumer is not draining."
            } else {
                "MESSAGES CONSUMED BUT LOST - queue is empty yet the ledger is short."
            }
        );
    }

    // No lost update across replicas: exactly N debited from carol, N credited to bob.
    assert_eq!(
        balance_of(&client, engine_port, "carol").await,
        carol_before - TRANSFERS as f64,
        "carol debited exactly {TRANSFERS} across replicas"
    );
    assert_eq!(
        balance_of(&client, engine_port, "bob").await,
        bob_before + TRANSFERS as f64,
        "bob credited exactly {TRANSFERS} across replicas"
    );

    // Still exactly one consumer after draining — the singleton didn't multiply under load.
    assert_eq!(
        broker::consumer_count(broker_port, RABBIT_USER, RABBIT_PASS, QUEUE).await,
        1,
        "still exactly one active consumer after draining"
    );
    publish_conn.close(200, "done").await.ok();
}

async fn balance_of(client: &reqwest::Client, port: u16, account: &str) -> f64 {
    let body = format!("{{\"BalanceQuery\":{{\"accountId\":\"{account}\"}}}}");
    let resp = client
        .post(format!("http://127.0.0.1:{port}/channels/balance"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body)
        .send()
        .await
        .expect("balance request");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "balance query replies 200 for {account}"
    );
    let text = resp.text().await.unwrap_or_default();
    let needle = "balance=\"";
    let start = text
        .find(needle)
        .unwrap_or_else(|| panic!("reply carries a balance attribute: {text}"))
        + needle.len();
    let rest = &text[start..];
    let end = rest.find('"').expect("closing quote");
    rest[..end].parse().expect("numeric balance")
}
