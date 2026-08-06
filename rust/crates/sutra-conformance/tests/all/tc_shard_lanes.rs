//! Conformance: the container really does boot — and use — N actor lanes.
//!
//! The black-box half of the execution scale-out §8 acceptance bar. The other half is the
//! whole suite re-run at `SUTRA_CONFORMANCE_SHARDS=4` (same tests, same expectations, an
//! N-lane container underneath — the seam is `sutra_testkit::conformance::engine`); that lane
//! is only worth anything if the container it boots is genuinely multi-lane, which is what
//! this suite pins down. It pins its own `.shards(4)`, so it asserts the same thing whether
//! or not the run-wide knob is set.
//!
//! # The observable, and why this one
//!
//! `/sutra/health/ready` reports `checks[0].data.shards` — the LIVE lane count, read by the
//! engine off its running shard-router handle (one entry per spawned actor lane), never
//! echoed back from config. It is the only honest black-box evidence available: the
//! `sutra.engine.shard.*` meters are OTLP-push, so there is nothing to scrape in tier-2, and
//! lane thread names never cross the container boundary. A config echo would prove only that
//! the env arrived; this proves the router built the lanes.
//!
//! Two containers, two claims. One is pinned to four lanes with `.shards(4)` and reports 4
//! whatever the run-wide knob says; the other is UNPINNED — built exactly like every other
//! suite's — and must report the knob's own reading, which makes it the default-identity
//! assertion on a default run and the env-threading assertion on an N-lane run. The default
//! is additionally gated where it costs nothing: `container_env`'s unit tests in the testkit
//! (an unset knob injects NO `SUTRA_ENGINE_SHARDS` at all) and the engine's own tier-1
//! `smoke` (an in-process default boot reports `"shards":1`).

use std::sync::OnceLock;

use crate::support::engine::{self, EngineBuilder, PgFixture};

/// The approval-hold example's channel API key (its `channels.yaml`).
const API_KEY: &str = "approval-demo-key";
/// The lane count this suite pins, independent of the run-wide knob.
const LANES: u32 = 4;
/// Distinct correlation ids, so the id hash spreads owners across all four lanes.
const CORRELATIONS: usize = 12;

struct Fixture {
    _pg: PgFixture,
    engine: engine::EngineHandle,
}

/// A four-lane engine on the approval-hold example — the smallest package with a DURABLE
/// wait state, which is what makes the traffic below cross lanes at all.
///
/// Held in a `static` so the container handles are never dropped on a tokio worker (their
/// testcontainers `Drop` would `block_on` docker removal); the atexit reaper removes them.
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("shardlanes");
            let engine = EngineBuilder::new("shardlanes", &pg)
                .shards(LANES)
                .expected_deployments(1)
                .start(&engine::assemble_example("approval-hold"));
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("four-lane approval-hold topology")
    })
}

/// An UNPINNED engine — exactly what every other suite builds, so its lane count is whatever
/// the run-wide knob says.
fn unpinned() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("unpinnedlanes");
            let engine = EngineBuilder::new("unpinnedlanes", &pg)
                .expected_deployments(1)
                .start(&engine::assemble_example("approval-hold"));
            Fixture { _pg: pg, engine }
        })
        .join()
        .expect("unpinned approval-hold topology")
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_shard_lanes_container_boots_the_requested_lane_count() {
    let port = fixture().engine.http_port;
    let live = tokio::task::spawn_blocking(move || engine::ready_shards(port))
        .await
        .unwrap();
    assert_eq!(
        live,
        Some(u64::from(LANES)),
        "the ready payload reports the router's live lane count"
    );
}

/// The run-wide knob, end to end — the property the whole `SUTRA_CONFORMANCE_SHARDS=4` rerun
/// rests on, and the one thing a suite-pinned `.shards(n)` can never demonstrate.
///
/// An unpinned container (what every other suite builds) must come up at exactly the knob's
/// reading: 4 under the N-lane rerun, 1 on a default run. So this test asserts the DEFAULT
/// identity on a default run and the threading on an N-lane run — the same assertion,
/// carrying whichever claim the run is making.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_shard_lanes_unpinned_container_follows_the_run_wide_knob() {
    let expected = u64::from(engine::conformance_shards().unwrap_or(1));
    let port = unpinned().engine.http_port;
    let live = tokio::task::spawn_blocking(move || engine::ready_shards(port))
        .await
        .unwrap();
    assert_eq!(
        live,
        Some(expected),
        "an unpinned container boots at the run-wide lane count (SUTRA_CONFORMANCE_SHARDS)"
    );
}

/// The lanes carry instance-addressed work, and the expectations do not move.
///
/// Each correlation id parks a spawn on its arrival lane and is then resumed by a relay that
/// arrives on a round-robin lane — at four lanes the resolving lane is the owner only ~1/4 of
/// the time, so most of these resumes are genuine cross-lane handoffs. Every one of them must
/// still produce exactly what the single-lane suite expects (per-instance serialization is
/// preserved at every N; only the incidental cross-instance serialization goes away), and the
/// duplicate-while-parked guard must still bite — a lane count that quietly broke correlation
/// would show up here as an accepted duplicate or a lost relay.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_shard_lanes_park_and_relay_across_lanes() {
    let client = reqwest::Client::new();
    let port = fixture().engine.http_port;

    let ids: Vec<String> = (0..CORRELATIONS).map(|n| format!("E2E-LANE-{n}")).collect();

    // Park one instance per correlation id — the owners spread over the four lanes by hash.
    for e2e in &ids {
        let (status, body) = post_request(&client, port, e2e, "1500.00").await;
        assert_eq!(status, 200, "{e2e} parks at the userTask: {body}");
    }

    // The durable unique-alias guard still rejects a duplicate while parked, on whatever lane
    // the duplicate happens to arrive on.
    let (status, body) = post_request(&client, port, &ids[0], "1500.00").await;
    assert_eq!(status, 500, "duplicate while parked is rejected: {body}");
    assert!(
        body.contains("SUTRA.INBOUND.ALIAS_CONFLICT_REJECT"),
        "alias-conflict code: {body}"
    );

    // Relay each decision — routed to the owner lane, mostly via a handoff.
    for e2e in &ids {
        let (status, body) = post_decision(&client, port, e2e, "APPROVE").await;
        assert_eq!(status, 200, "{e2e} resumes + completes: {body}");
    }

    // Aliases retired at completion — the same ids are accepted again, on every lane.
    for e2e in &ids {
        let (status, body) = post_request(&client, port, e2e, "1500.00").await;
        assert_eq!(
            status, 200,
            "{e2e} re-accepted after completion (alias retired): {body}"
        );
    }
}

// ---- drive helpers ----------------------------------------------------------------------

async fn post_request(
    client: &reqwest::Client,
    port: u16,
    e2e: &str,
    amount: &str,
) -> (u16, String) {
    let body = format!("{{\"ApprovalRequest\":{{\"E2EId\":\"{e2e}\",\"Amount\":\"{amount}\"}}}}");
    post(client, port, "/channels/approval-request", &body).await
}

async fn post_decision(
    client: &reqwest::Client,
    port: u16,
    e2e: &str,
    decision: &str,
) -> (u16, String) {
    let body =
        format!("{{\"ApprovalDecision\":{{\"E2EId\":\"{e2e}\",\"Decision\":\"{decision}\"}}}}");
    post(client, port, "/channels/approval-decision", &body).await
}

async fn post(client: &reqwest::Client, port: u16, path: &str, body: &str) -> (u16, String) {
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
