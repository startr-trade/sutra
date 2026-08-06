//! Tier-3 conformance: the deploy smoke. Package → deploy through the admin API onto the shared
//! k8s engine → one real request/response round trip through the Ingress → undeploy.
//!
//! # What this suite is for
//!
//! It is the LEAN end-to-end proof that the deployment lifecycle works on a cluster, as opposed
//! to on Testcontainers: `sutra package` seals a package dir, `sutra deploy --api` hot-deploys it
//! into a running engine that reads its deployments from the database, the channel it declares
//! goes live behind the Ingress, and `undeploy` takes it back down. Everything richer — ACID
//! ledger semantics, multi-replica singletons, coverage — is proved at tier-2 by
//! `tc_money_transfer` / `tc_multi_replica` against a container topology that can be torn down
//! and reseeded. Repeating those assertions here would only add cluster-state coupling for
//! coverage the engine already has, so the round trip deliberately asserts reachability and
//! status, not ledger arithmetic.
//!
//! # Why money-transfer
//!
//! It is the richest public example that needs nothing the shared scenario does not already
//! provide. Its accounts ledger is MODULE-OWNED: `datastores.yaml` declares the store and its
//! schema+seed live in `migrations/accounts/` INSIDE the sealed archive, so the sql store
//! provider creates and seeds the table on first use against the `ACCOUNTS_DB_*` env the shared
//! scenario already injects for exactly this purpose. No tofu change, no out-of-band psql, no
//! fixture SQL.
//!
//! The module's `transfer-queue` (rabbitmq) and `transfer-topic` (kafka) channels are expected to
//! stay DOWN: the scenario provisions no kafka at all, and `transfer-queue-q` is not pre-declared
//! (the trigger declares passively). Both fail closed per channel — the same shape tier-2 relies
//! on — and the HTTP channels serve regardless, which is what this suite drives.
//!
//! Because the ledger lives in the shared instance's Postgres and outlives the run, the round
//! trip uses the READ-ONLY `balance` channel. That keeps the suite idempotent and re-runnable:
//! nothing it does depends on, or changes, the balance any previous run left behind.
//!
//! COMPILE-ONLY in this track: executed by the coordinator serially at wave close.

#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use crate::support::k8s;
use crate::support::util::{self, wait_until};

/// The package's read-only query channel + its api key (see its `channels.yaml`).
const DRIVE_CHANNEL: &str = "/channels/balance";
const API_KEY: &str = "transfer-demo-key";
/// Single-variant example: `deployments-src/<slot>` is the package dir as-is (no `shared/` +
/// `variants/` composition step).
const PACKAGE: &str = "money-transfer/deployments-src/default--money-transfer--1.0.0";
/// An account the module's own seed migration creates.
const ACCOUNT: &str = "alice";

struct Fixture {
    _rt: tokio::runtime::Runtime,
    engine_base: String,
    admin_key: String,
    archive_key: String,
}

fn provision() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(build)
            .join()
            .expect("money-transfer k8s provision")
    })
}

fn build() -> Fixture {
    let kubeconfig = k8s::kubeconfig_path().to_string_lossy().to_string();
    let scenario = k8s::shared_scenario_dir();

    k8s::tofu(&scenario, &["init", "-input=false", "-no-color"]);
    k8s::tofu(
        &scenario,
        &[
            "apply",
            "-auto-approve",
            "-input=false",
            "-no-color",
            "-var",
            &format!("kubeconfig_path={kubeconfig}"),
            "-var",
            &format!("engine_image={}", k8s::engine_image()),
        ],
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("k8s runtime");
    let (engine_base, admin_key) = rt.block_on(async {
        let client = k8s::kube_client().await;
        // Gate on rollout convergence — an apply that changed the engine image rolls the pod;
        // without this the swap lands mid-suite (see k8s::await_rollout).
        k8s::await_rollout(&client, "default", "sutra-engine").await;
        let ingress =
            k8s::ingress_endpoint(&client, "ingress-nginx", "ingress-nginx-controller").await;
        let engine_base = format!("http://{ingress}");
        let admin_key = k8s::admin_api_key(&client, "default").await;
        wait_engine_ready(&engine_base).await;
        (engine_base, admin_key)
    });

    let archives_dir = util::world_readable_temp_dir("mt-k8s-archives");
    let pkg = util::examples_dir().join(PACKAGE);
    let archive_key = format!("{}.sutra", pkg.file_name().unwrap().to_string_lossy());
    k8s::run_cli(
        &[],
        &[
            "package",
            &pkg.to_string_lossy(),
            "--out",
            &archives_dir.to_string_lossy(),
        ],
    );
    // db deployment source: deploy through the sync API (deterministic activation).
    k8s::deploy_api(&engine_base, &archives_dir.join(&archive_key), &admin_key);
    rt.block_on(async {
        wait_until("balance route active", Duration::from_secs(240), || async {
            let client = reqwest::Client::new();
            balance_query(&client, &engine_base).await.0 == 200
        })
        .await;
    });

    Fixture {
        _rt: rt,
        engine_base,
        admin_key,
        archive_key,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "k8s"]
async fn k8s_money_transfer_deployed_channel_round_trips() {
    let fx = provision();
    let client = reqwest::Client::new();
    let (status, body) = balance_query(&client, &fx.engine_base).await;

    assert_eq!(
        status, 200,
        "the deployed balance channel answers through the Ingress: {body}"
    );
    // The reply is the package's `balance.hbs` render — a `<Balance …/>` element carrying the
    // queried account and its balance. Asserting its SHAPE proves the request reached the flow,
    // the module-owned store was created+seeded from the archive's own migrations, and the reply
    // template rendered; asserting the VALUE would couple this suite to whatever the shared
    // instance's ledger happens to hold.
    assert!(
        body.contains("<Balance") && body.contains(&format!("accountId=\"{ACCOUNT}\"")),
        "reply is a Balance for {ACCOUNT}: {body}"
    );
    assert!(
        body.contains("balance=\""),
        "reply carries a balance attribute (the module-owned store resolved): {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "k8s"]
async fn k8s_money_transfer_zz_teardown() {
    let fx = provision();
    let slot = fx
        .archive_key
        .strip_suffix(".sutra")
        .unwrap_or(&fx.archive_key);
    k8s::undeploy_api_quiet(&fx.engine_base, slot, &fx.admin_key);
}

// ---- drive helpers ----------------------------------------------------------------------

async fn balance_query(client: &reqwest::Client, engine_base: &str) -> (u16, String) {
    let resp = client
        .post(format!("{engine_base}{DRIVE_CHANNEL}"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(format!(
            "{{\"BalanceQuery\":{{\"accountId\":\"{ACCOUNT}\"}}}}"
        ))
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    (status, text)
}

async fn wait_engine_ready(engine_base: &str) {
    let client = reqwest::Client::new();
    wait_until(
        "engine /sutra/health/ready via the Ingress",
        Duration::from_secs(180),
        || async {
            client
                .get(format!("{engine_base}/sutra/health/ready"))
                .send()
                .await
                .map(|r| r.status().as_u16() == 200)
                .unwrap_or(false)
        },
    )
    .await;
}
