//! Tier-3 conformance: OTLP observability on the shared k8s engine. One POST through the
//! Ingress must produce all three OTLP signals in the in-cluster Elasticsearch — logs
//! (`service.name=sutra-engine`), a complete engine trace tree (`sutra.dispatch` covering
//! resolve/validate, plus a `sutra.execute` for the flow), and metrics.
//!
//! # Why money-transfer drives it
//!
//! The signals asserted here are emitted by the NEUTRAL engine — `sutra.dispatch` /
//! `sutra.resolve` (channel dispatch + handler resolution), `sutra.validate` (post-decode, for
//! any start node carrying a `<q:source>`) and `sutra.execute` (flow execution) — so any
//! deployed package exercises them identically. The suite drives the money-transfer example,
//! which keeps engine tier-3 self-contained: it binds only the package's own path-derived XSD
//! codec (`urn:transfer`, compiled from `schemas/transfer/transfer.xsd`), so it needs no
//! extension codec and no example beyond the ones the public engine ships. The assertions stay
//! SIGNAL-focused (which spans, which indices) and never inspect a payload — the payload is
//! only a way to get a message through the pipeline.
//!
//! `POST /channels/balance` is the minimal drive: `balance` is a read-only, non-singleton HTTP
//! channel whose `balance-query.bpmn` start event carries a `<q:source>` (so `sutra.validate`
//! fires) and which replies synchronously with 200. Execution is therefore in-line rather than
//! rehydrated out of band, which is why the `sutra.execute` check below is a time-bounded
//! GLOBAL query rather than a lookup within the dispatch trace — it holds for both shapes and
//! makes no claim about which trace carries the span.
//!
//! Nothing external is required: the accounts ledger the query reads is provisioned by the
//! package ITSELF (`datastores.yaml` + `migrations/accounts/`, sealed into the archive and run
//! by the sql store provider on first use) against the `ACCOUNTS_DB_*` env the shared scenario
//! already injects. The module's rabbitmq/kafka channels stay down here — fail-closed per
//! channel, exactly as in tier-2 — and the HTTP channels serve regardless.
//!
//! COMPILE-ONLY in this track: executed by the coordinator serially at wave close. Requires
//! the `infra/` observability stack (es-lb + the elastic credential) to be up.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use crate::support::k8s;
use crate::support::util::{self, wait_until};

const ES_LB_SVC: &str = "es-lb";
const ES_SECRET: &str = "sutra-es-es-elastic-user";
const SERVICE_NAME: &str = "sutra-engine";
/// The money-transfer package's read-only query channel + its api key (see its `channels.yaml`).
const DRIVE_CHANNEL: &str = "/channels/balance";
const API_KEY: &str = "transfer-demo-key";
/// The package deployed for the drive. Single-variant example: `deployments-src/<slot>` is the
/// package dir as-is (no `shared/` + `variants/` composition step).
const PACKAGE: &str = "money-transfer/deployments-src/default--money-transfer--1.0.0";
/// An account the module's own seed migration creates — the query needs a real key to return 200.
const DRIVE_ACCOUNT: &str = "alice";

struct Fixture {
    _rt: tokio::runtime::Runtime,
    engine_base: String,
    admin_key: String,
    es_base_url: String,
    es_password: String,
    es_client: reqwest::Client,
    archives_dir: PathBuf,
    archive_key: String,
}

fn provision() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(build)
            .join()
            .expect("observability k8s provision")
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
    let (engine_base, admin_key, es_base_url, es_password) = rt.block_on(async {
        let client = k8s::kube_client().await;
        // Gate on rollout convergence — an apply that changed the engine image rolls the pod;
        // without this the swap lands mid-suite (see k8s::await_rollout).
        k8s::await_rollout(&client, "default", "sutra-engine").await;
        let ingress =
            k8s::ingress_endpoint(&client, "ingress-nginx", "ingress-nginx-controller").await;
        let engine_base = format!("http://{ingress}");
        let admin_key = k8s::admin_api_key(&client, "default").await;
        wait_engine_ready(&engine_base).await;
        // Observability infra: es-lb LoadBalancer IP + the elastic credential.
        let es_ip = k8s::await_lb_ip(&client, "default", ES_LB_SVC).await;
        let es_password = k8s::secret_value(&client, "default", ES_SECRET, "elastic").await;
        (
            engine_base,
            admin_key,
            format!("https://{es_ip}:9200"),
            es_password,
        )
    });

    let es_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // throwaway in-cluster CA over a local LB
        .build()
        .expect("es client");

    // Hot-deploy the money-transfer package.
    let archives_dir = util::world_readable_temp_dir("obs-archives");
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
            post_drive(&client, &engine_base).await.0 == 200
        })
        .await;
    });

    Fixture {
        _rt: rt,
        engine_base,
        admin_key,
        es_base_url,
        es_password,
        es_client,
        archives_dir,
        archive_key,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "k8s"]
async fn k8s_observability_post_yields_all_three_signals() {
    let fx = provision();
    // Fail fast and legibly on the one environment fault that otherwise looks exactly like a
    // broken engine: a flood-stage-blocked Elasticsearch silently accepting no new documents.
    assert_es_accepts_writes(fx).await;
    let since = util::iso_utc_now_minus_secs(5);

    let client = reqwest::Client::new();
    let (status, body) = post_drive(&client, &fx.engine_base).await;
    assert_eq!(status, 200, "engine serves the query channel: {body}");

    // OTLP export + collector batch + ES refresh take a few seconds — poll all three signals.
    // EVERY signal is bounded by `since`: the indices are long-lived and shared, so an
    // unbounded count is satisfied by data from an earlier run (or an earlier week) and would
    // report green for a pipeline that has been dead for days.
    wait_until("all three OTLP signals in Elasticsearch", Duration::from_secs(90), || async {
        // (1) LOGS — a structured ECS doc from the shared engine service, in this run's window.
        let logs = es_count(
            fx,
            "sutra-app-logs",
            &format!(
                "{{\"bool\":{{\"filter\":[{{\"match\":{{\"service.name\":\"{SERVICE_NAME}\"}}}},{{\"range\":{{\"@timestamp\":{{\"gte\":\"{since}\"}}}}}}]}}}}"
            ),
        )
        .await;
        if logs == 0 {
            return false;
        }
        // (2) TRACES — the dispatch trace covers the intake pipeline (dispatch/resolve/validate);
        // the flow's `sutra.execute` is asserted as a time-bounded GLOBAL query, since whether it
        // shares the dispatch trace (synchronous reply) or gets its own (rehydrated out of band)
        // is a property of the channel's ack-mode, not of the engine's instrumentation.
        let Some(trace_id) = first_trace_id_for_dispatch_since(fx, &since).await else {
            return false;
        };
        let spans = es_search_source(
            fx,
            "sutra-traces",
            &format!("{{\"term\":{{\"TraceId\":\"{trace_id}\"}}}}"),
            "\"Name\"",
            50,
        )
        .await;
        if !(spans.contains("sutra.dispatch")
            && spans.contains("sutra.resolve")
            && spans.contains("sutra.validate")
            && span_recorded_since(fx, "sutra.execute", &since).await)
        {
            return false;
        }
        // (3) METRICS — the OTLP metrics data streams took documents in this run's window.
        // Not filtered by service: the collector's own periodic export is the signal that the
        // metrics pipe is live, and the engine's meter rides the same exporter.
        es_count(
            fx,
            "metrics-*",
            &format!("{{\"range\":{{\"@timestamp\":{{\"gte\":\"{since}\"}}}}}}"),
        )
        .await
            > 0
    })
    .await;
}

/// Elasticsearch is a SHARED, long-lived instance on a dev box, and its flood-stage watermark
/// (disk ≥ 95%) sets `index.blocks.read_only_allow_delete` on every index it has — a block it
/// does NOT release until usage falls back under the *high* watermark. The collector then
/// fails every bulk index while the engine, the ingress and the flow all keep working, so the
/// symptom reaching this suite is "no signals arrived", indistinguishable from a regression in
/// engine instrumentation. It cost a false lead once (P5); assert it up front instead.
async fn assert_es_accepts_writes(fx: &Fixture) {
    // Ask for the ONE setting by name. The obvious-looking `filter_path=**.blocks*` does NOT
    // work here: with `flat_settings=true` the setting is a single key that CONTAINS dots
    // (`index.blocks.read_only_allow_delete`), while filter_path treats dots as path
    // separators — so the pattern matches nothing, the body comes back empty, and the
    // assertion below passes on a cluster that is refusing every write. This guard silently
    // no-opped until 2026-08-04, when a flood-staged run reached the signal assertions and
    // timed out exactly as the guard was written to prevent. A clean cluster answers `{}`.
    let settings = es_get(
        fx,
        "/_all/_settings/index.blocks.read_only_allow_delete?flat_settings=true",
    )
    .await;
    assert!(
        !settings.contains("read_only_allow_delete"),
        "Elasticsearch is refusing writes — indices carry \
         index.blocks.read_only_allow_delete (flood-stage watermark, disk >= 95%). \
         The engine is not at fault. Reclaim disk (`df -h /`), then clear the block:\n  \
         curl -k -u elastic:<pw> -XPUT https://<es>:9200/_all/_settings \\\n    \
         -H 'Content-Type: application/json' \\\n    \
         -d '{{\"index.blocks.read_only_allow_delete\":null}}'\n\
         Blocked settings: {settings}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "k8s"]
async fn k8s_observability_zz_teardown() {
    let fx = provision();
    let slot = fx
        .archive_key
        .strip_suffix(".sutra")
        .unwrap_or(&fx.archive_key);
    k8s::undeploy_api_quiet(&fx.engine_base, slot, &fx.admin_key);
}

// ---- engine drive -----------------------------------------------------------------------

/// POST one `BalanceQuery` through the Ingress onto the deployed package's read-only channel.
///
/// `balance` replies synchronously (200) with the account's `<Balance/>`; the CONTENT is
/// incidental here — the request exists to get one message through decode → validate → execute
/// so the engine emits its span waterfall.
async fn post_drive(client: &reqwest::Client, engine_base: &str) -> (u16, String) {
    let resp = client
        .post(format!("{engine_base}{DRIVE_CHANNEL}"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(format!(
            "{{\"BalanceQuery\":{{\"accountId\":\"{DRIVE_ACCOUNT}\"}}}}"
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

// ---- Elasticsearch query helpers --------------------------------------------------------

async fn es_count(fx: &Fixture, index: &str, query_json: &str) -> u64 {
    let body = es_post(
        fx,
        &format!("/{index}/_count"),
        &format!("{{\"query\":{query_json}}}"),
    )
    .await;
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["count"].as_u64())
        .unwrap_or(0)
}

async fn first_trace_id_for_dispatch_since(fx: &Fixture, since: &str) -> Option<String> {
    let query = format!(
        "{{\"bool\":{{\"filter\":[{{\"term\":{{\"Name.keyword\":\"sutra.dispatch\"}}}},{{\"range\":{{\"@timestamp\":{{\"gte\":\"{since}\"}}}}}}]}}}}"
    );
    let body = es_search_source(fx, "sutra-traces", &query, "\"TraceId\"", 1).await;
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v["hits"]["hits"]
                .as_array()
                .and_then(|hits| hits.first())
                .and_then(|hit| hit["_source"]["TraceId"].as_str())
                .map(|s| s.to_string())
        })
}

async fn span_recorded_since(fx: &Fixture, name: &str, since: &str) -> bool {
    es_count(
        fx,
        "sutra-traces",
        &format!(
            "{{\"bool\":{{\"filter\":[{{\"term\":{{\"Name.keyword\":\"{name}\"}}}},{{\"range\":{{\"@timestamp\":{{\"gte\":\"{since}\"}}}}}}]}}}}"
        ),
    )
    .await
        > 0
}

async fn es_search_source(
    fx: &Fixture,
    index: &str,
    query_json: &str,
    source_field: &str,
    size: u32,
) -> String {
    es_post(
        fx,
        &format!("/{index}/_search"),
        &format!("{{\"size\":{size},\"_source\":[{source_field}],\"query\":{query_json}}}"),
    )
    .await
}

async fn es_get(fx: &Fixture, path: &str) -> String {
    match fx
        .es_client
        .get(format!("{}{path}", fx.es_base_url))
        .basic_auth("elastic", Some(&fx.es_password))
        .send()
        .await
    {
        Ok(resp) => resp.text().await.unwrap_or_default(),
        Err(e) => panic!("Elasticsearch GET {path} failed: {e}"),
    }
}

async fn es_post(fx: &Fixture, path: &str, json: &str) -> String {
    match fx
        .es_client
        .post(format!("{}{path}", fx.es_base_url))
        .header("Content-Type", "application/json")
        .basic_auth("elastic", Some(&fx.es_password))
        .body(json.to_string())
        .send()
        .await
    {
        Ok(resp) => resp.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}
