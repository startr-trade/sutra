//! Conformance: money-transfer — ACID ledger on real PostgreSQL.
//!
//! Seven sequential steps of ONE shared-engine test, and the order is load-bearing: they share
//! seeded ledger state. Step 1 must observe the pristine seed (alice=bob=100) before anything
//! else touches the accounts, and the coverage steps must run last, reading the covered-set the
//! ACID steps stamped. Steps 1-5 cover durability + cross-instance read, insufficient funds,
//! a frozen account, atomicity (credit fails, debit rolls back) and isolation under concurrent
//! transfers; steps 6-7 assert the coverage report reads 100% and that reset clears it.
//!
//! Topology: engine + its internal Postgres + a SEPARATE `accounts` Postgres (the module-owned
//! ledger store, wired through `ACCOUNTS_DB_*`). The rabbitmq/kafka channels the module also
//! declares stay down (no broker here) — fail-closed per channel, the engine still serves HTTP.

use std::sync::OnceLock;

use crate::support::engine::{self, EngineBuilder, PgFixture};

const API_KEY: &str = "transfer-demo-key";

struct Topology {
    _pg: PgFixture,
    _accounts: PgFixture,
    engine: engine::EngineHandle,
}

/// Held in a `static` so the container handles are never dropped on a tokio worker (their
/// testcontainers `Drop` would `block_on` docker removal → "runtime within a runtime"); the
/// atexit reaper removes them at process exit.
fn topology() -> &'static Topology {
    static TOPO: OnceLock<Topology> = OnceLock::new();
    TOPO.get_or_init(|| {
        std::thread::spawn(build_topology)
            .join()
            .expect("money-transfer topology")
    })
}

fn build_topology() -> Topology {
    let pg = engine::start_postgres("mt");
    let accounts = engine::start_postgres("mt-accounts");
    let engine = EngineBuilder::new("mt", &pg)
        // The `accounts` store's OWN connection (datastores.yaml env refs) — the SEPARATE
        // accounts Postgres, never the engine datasource.
        .env(
            "ACCOUNTS_DB_URL",
            format!("postgresql://{}:5432/postgres", accounts.container_name),
        )
        .env("ACCOUNTS_DB_USER", "postgres")
        .env("ACCOUNTS_DB_PASSWORD", "postgres")
        // The transfer-queue rabbitmq channel resolves these refs at startup, then simply
        // can't reach a broker here (expected) — supply them so startup doesn't error.
        .env("RABBITMQ_USERNAME", "mtransfer")
        .env("RABBITMQ_PASSWORD", "mtransfer-broker-pw")
        .expected_deployments(1)
        .start(&engine::assemble_example("money-transfer"));
    Topology {
        _pg: pg,
        _accounts: accounts,
        engine,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn tc_money_transfer_acid_ledger() {
    let port = topology().engine.http_port;
    let client = reqwest::Client::new();

    // ---- step 1: durability + cross-instance --------------------------------------------
    let (status, body) = post_transfer(&client, port, "alice", "bob", "50").await;
    assert_eq!(status, 200, "accepted transfer replies 200: {body}");
    for needle in [
        "<TransferAccepted",
        "from=\"alice\"",
        "to=\"bob\"",
        "newFromBalance=\"50\"",
        "newToBalance=\"150\"",
    ] {
        assert!(
            body.contains(needle),
            "accepted reply missing {needle}: {body}"
        );
    }
    // A separate balance-query instance reads the durable persisted balances.
    assert_eq!(
        balance_of(&client, port, "alice").await,
        50.0,
        "alice debited + persisted"
    );
    assert_eq!(
        balance_of(&client, port, "bob").await,
        150.0,
        "bob credited + persisted"
    );

    // ---- step 2: consistency — insufficient funds ---------------------------------------
    let alice_before = balance_of(&client, port, "alice").await;
    let bob_before = balance_of(&client, port, "bob").await;
    let (_, body) = post_transfer(&client, port, "alice", "bob", "1000").await;
    assert!(body.contains("<TransferRejected"), "rejected reply: {body}");
    assert!(
        body.contains("reason=\"insufficient-funds\""),
        "reason: {body}"
    );
    assert_eq!(
        balance_of(&client, port, "alice").await,
        alice_before,
        "alice unchanged"
    );
    assert_eq!(
        balance_of(&client, port, "bob").await,
        bob_before,
        "bob unchanged"
    );

    // ---- step 3: consistency — frozen account -------------------------------------------
    let alice_before = balance_of(&client, port, "alice").await;
    let fred_before = balance_of(&client, port, "frozen-fred").await;
    let (_, body) = post_transfer(&client, port, "alice", "frozen-fred", "10").await;
    assert!(body.contains("<TransferRejected"), "rejected reply: {body}");
    assert!(body.contains("reason=\"frozen-account\""), "reason: {body}");
    assert_eq!(
        balance_of(&client, port, "alice").await,
        alice_before,
        "alice unchanged"
    );
    assert_eq!(
        balance_of(&client, port, "frozen-fred").await,
        fred_before,
        "frozen-fred unchanged"
    );

    // ---- step 4: atomicity — credit fails, debit rolls back -----------------------------
    let alice_before = balance_of(&client, port, "alice").await;
    let (_, body) = post_transfer(&client, port, "alice", "explode-on-credit", "10").await;
    assert!(
        !body.contains("<TransferAccepted"),
        "not accepted — the credit step failed: {body}"
    );
    assert_eq!(
        balance_of(&client, port, "alice").await,
        alice_before,
        "debit rolled back — alice unchanged"
    );

    // ---- step 5: isolation — concurrent transfers, no lost update -----------------------
    let n = 8;
    let carol_before = balance_of(&client, port, "carol").await;
    let bob_before = balance_of(&client, port, "bob").await;
    let mut handles = Vec::new();
    for _ in 0..n {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            post_transfer(&client, port, "carol", "bob", "1").await
        }));
    }
    let mut accepted = 0;
    for handle in handles {
        let (status, body) = handle.await.expect("concurrent transfer task");
        if status == 200 && body.contains("<TransferAccepted") {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, n,
        "all concurrent transfers serialized + accepted (FOR UPDATE)"
    );
    assert_eq!(
        balance_of(&client, port, "carol").await,
        carol_before - n as f64,
        "carol debited exactly {n}"
    );
    assert_eq!(
        balance_of(&client, port, "bob").await,
        bob_before + n as f64,
        "bob credited exactly {n}"
    );

    // ---- step 6: coverage — the ACID suite drove both branches, report reads 100% -------
    let (status, body) = post_json(
        &client,
        port,
        "/channels/coverage-query",
        "{\"CoverageQuery\":{\"process\":\"transfer\"}}",
    )
    .await;
    assert_eq!(status, 200, "coverage report replies 200: {body}");
    for needle in [
        "<CoverageReport",
        "process=\"transfer\"",
        "percentage=\"100.0\"",
        "covered=\"2\"",
        "total=\"2\"",
        "<covered>accept</covered>",
        "<covered>reject</covered>",
    ] {
        assert!(
            body.contains(needle),
            "full coverage missing {needle}: {body}"
        );
    }
    assert!(
        !body.contains("<uncovered>"),
        "nothing left uncovered: {body}"
    );

    // ---- step 7: coverage — reset clears the covered-set, following report reads 0% ------
    let (status, body) = post_json(
        &client,
        port,
        "/channels/coverage-reset",
        "{\"CoverageReset\":{\"process\":\"transfer\"}}",
    )
    .await;
    assert_eq!(status, 200, "coverage reset replies 200: {body}");
    for needle in [
        "<CoverageReset",
        "process=\"transfer\"",
        "cleared=\"2\"",
        "total=\"2\"",
    ] {
        assert!(
            body.contains(needle),
            "reset reply missing {needle}: {body}"
        );
    }
    let (_, body) = post_json(
        &client,
        port,
        "/channels/coverage-query",
        "{\"CoverageQuery\":{\"process\":\"transfer\"}}",
    )
    .await;
    for needle in [
        "percentage=\"0.0\"",
        "covered=\"0\"",
        "<uncovered>accept</uncovered>",
        "<uncovered>reject</uncovered>",
    ] {
        assert!(
            body.contains(needle),
            "0% after reset missing {needle}: {body}"
        );
    }
    assert!(!body.contains("<covered>"), "no path still covered: {body}");
}

// ---- drive helpers ----------------------------------------------------------------------

async fn post_transfer(
    client: &reqwest::Client,
    port: u16,
    from: &str,
    to: &str,
    amount: &str,
) -> (u16, String) {
    let body = format!(
        "{{\"TransferRequest\":{{\"fromId\":\"{from}\",\"toId\":\"{to}\",\"amount\":{amount}}}}}"
    );
    post_json(client, port, "/channels/transfer-request", &body).await
}

async fn balance_of(client: &reqwest::Client, port: u16, account: &str) -> f64 {
    let body = format!("{{\"BalanceQuery\":{{\"accountId\":\"{account}\"}}}}");
    let (status, reply) = post_json(client, port, "/channels/balance", &body).await;
    assert_eq!(
        status, 200,
        "balance query replies 200 for {account}: {reply}"
    );
    extract_attr(&reply, "balance")
        .unwrap_or_else(|| panic!("reply carries a balance attribute: {reply}"))
        .parse()
        .expect("numeric balance")
}

async fn post_json(client: &reqwest::Client, port: u16, path: &str, body: &str) -> (u16, String) {
    let resp = client
        .post(format!("http://127.0.0.1:{port}{path}"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.to_string())
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    (status, text)
}

/// Extract the value of `attr="..."` from a reply body.
fn extract_attr(body: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}
