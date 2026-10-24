//! Conformance: the two `llm`-transport examples, `examples/ai-summarize` and
//! `examples/ai-advanced`, end to end on real PostgreSQL against the examples' own stub model
//! server — no provider key, no network.
//!
//! Each example keeps its flows once under `shared/` and one small layer per provider under
//! `variants/<name>/`; every package here is composed exactly as `sutra package <variant>
//! --overlay <shared>` composes it (`sutra_loader::overlay`, via the harness's
//! `compose_variant`), then sealed. Three engines, one per variant under test, share one stub:
//!
//! | variant | wire | flows driven |
//! |---|---|---|
//! | ai-summarize `stub-openai` | OpenAI Chat Completions | sync, SSE-streamed, async |
//! | ai-summarize `stub-anthropic` | Anthropic Messages | sync, SSE-streamed, async |
//! | ai-advanced `stub` | both | cross-provider fallback, JSON mode, tool call, OpenAI background job (polled), OpenAI background job (signed webhook), Anthropic Message Batch |
//!
//! THE STUB. `examples/ai-summarize/stub/model-stub.py` runs in `python:3.12-alpine` with the
//! script bind-mounted. The packages address it as `http://model-stub:8000`, so it must answer to
//! that name on the suite's network — but a container NAME is global to the docker host, so it
//! starts under a unique name and joins the suite network with `model-stub` as a network ALIAS
//! (`docker network connect --alias`), which is scoped to that network. Host-side, the tests read
//! its `/sink`, `/stats` and flip its outage switch through the mapped port.
//!
//! WHAT IS READ BACK. Replies and SSE frames on the caller's connection; the stub's `/sink` for
//! the async and job hand-offs; and the engine's own rows: a sync call leaves no
//! `instance_state` row, an async or job instance's row is found by its ticket id and reported
//! `COMPLETED` by `/sutra/instances/{id}`, and `llm_job` holds the job while the provider works
//! and is empty for that channel once the result is delivered.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use testcontainers::core::{AccessMode, IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::support::compose;
use crate::support::engine::{self, EngineBuilder, EngineHandle, PgFixture};
use crate::support::util;

/// Fixture names start `ai-` so every container lands on the one `ai` suite network.
const SUITE: &str = "ai";
const SUMMARIZE_KEY: &str = "ai-summarize-e2e-key";
const ADVANCED_KEY: &str = "ai-advanced-e2e-key";
/// The stub's webhook signing secret; the engine verifies with the same value.
const WEBHOOK_SECRET: &str = "whsec_ZGV2LW9ubHktd2ViaG9vay1zZWNyZXQ=";
/// The stub's fixed answer.
const SUMMARY: &str =
    "STUB SUMMARY: the customer reports a problem and asks for it to be looked at.";

// ---- fixtures -------------------------------------------------------------------------------

struct Stub {
    _container: Container<GenericImage>,
    port: u16,
}

/// The shared stub model server, reachable from the engines as `model-stub:8000`.
fn stub() -> &'static Stub {
    static STUB: OnceLock<Stub> = OnceLock::new();
    STUB.get_or_init(|| {
        std::thread::spawn(|| {
            let script_dir = util::examples_dir().join("ai-summarize/stub");
            let container = GenericImage::new("python", "3.12-alpine")
                .with_exposed_port(8000.tcp())
                .with_wait_for(WaitFor::message_on_stdout("listening on 8000"))
                .with_mount(
                    Mount::bind_mount(script_dir.to_string_lossy().to_string(), "/stub")
                        .with_access_mode(AccessMode::ReadOnly),
                )
                .with_cmd(["python3", "/stub/model-stub.py", "8000"])
                .with_env_var("STUB_WEBHOOK_SECRET", WEBHOOK_SECRET)
                .with_container_name(format!("ai-model-stub-{}", util::short_id()))
                .start()
                .expect("start python:3.12-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let network = util::network_for(SUITE);
            // The network may not exist yet (the first postgres fixture creates it otherwise);
            // "already exists" is the expected failure of a second create.
            let _ = Command::new("docker")
                .args(["network", "create", network])
                .output();
            let connected = Command::new("docker")
                .args([
                    "network",
                    "connect",
                    "--alias",
                    "model-stub",
                    network,
                    container.id(),
                ])
                .output()
                .expect("docker network connect");
            assert!(
                connected.status.success(),
                "attach the stub to {network} as model-stub: {}",
                String::from_utf8_lossy(&connected.stderr)
            );
            let port = container.get_host_port_ipv4(8000).expect("mapped 8000");
            Stub {
                _container: container,
                port,
            }
        })
        .join()
        .expect("model stub")
    })
}

struct Fixture {
    pg: PgFixture,
    engine: EngineHandle,
}

/// Compose `<example>/variants/<variant>` over `<example>/shared` (the `--overlay` path), seal
/// it, and boot an engine on it against its own PostgreSQL.
fn boot(example: &str, variant: &str, hint: &str) -> Fixture {
    stub();
    let composed = compose::compose_variant(&util::examples_dir().join(example), variant);
    let archives = util::world_readable_temp_dir("sutra-archives");
    sutra_loader::package::assemble_dir(
        &composed,
        &archives,
        &sutra_loader::PackageOptions::default(),
    )
    .unwrap_or_else(|e| panic!("package {example} variant {variant}: {e}"));
    util::make_world_readable(&archives);
    let pg = engine::start_postgres(hint);
    let engine = EngineBuilder::new(hint, &pg)
        .env("SUMMARIZE_API_KEY", SUMMARIZE_KEY)
        .env("AI_ADVANCED_API_KEY", ADVANCED_KEY)
        .env("LLM_API_KEY", "stub-not-a-real-key")
        .env("STUB_WEBHOOK_SECRET", WEBHOOK_SECRET)
        .expected_deployments(1)
        .start(&archives);
    Fixture { pg, engine }
}

fn summarize_openai() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        std::thread::spawn(|| boot("ai-summarize", "stub-openai", "ai-sumoa"))
            .join()
            .expect("ai-summarize stub-openai topology")
    })
}

fn summarize_anthropic() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        std::thread::spawn(|| boot("ai-summarize", "stub-anthropic", "ai-sumant"))
            .join()
            .expect("ai-summarize stub-anthropic topology")
    })
}

fn advanced() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        std::thread::spawn(|| boot("ai-advanced", "stub", "ai-adv"))
            .join()
            .expect("ai-advanced stub topology")
    })
}

// ---- helpers --------------------------------------------------------------------------------

/// POST JSON to an inbound channel; returns (status, content-type, body).
async fn post(fixture: &Fixture, key: &str, channel: &str, body: &Value) -> (u16, String, String) {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            fixture.engine.http_port
        ))
        .header("X-Api-Key", key)
        .json(body)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /channels/{channel}: {e}"));
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    (
        status,
        content_type,
        response.text().await.unwrap_or_default(),
    )
}

async fn stub_get(path: &str) -> Value {
    reqwest::get(format!("http://127.0.0.1:{}{path}", stub().port))
        .await
        .and_then(|r| r.error_for_status())
        .unwrap_or_else(|e| panic!("stub GET {path}: {e}"))
        .json()
        .await
        .expect("stub JSON")
}

async fn set_outage(on: bool) {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/control/outage", stub().port))
        .json(&json!({ "on": on }))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .expect("flip the stub's outage switch");
}

async fn stub_count(route: &str) -> u64 {
    stub_get("/stats").await[route].as_u64().unwrap_or(0)
}

/// The sink delivery for `ticket_id`, parsed, once it arrives.
async fn sink_delivery(ticket_id: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let bodies = stub_get("/sink").await;
        let found = bodies.as_array().into_iter().flatten().find_map(|b| {
            let v: Value = serde_json::from_str(b.as_str()?).ok()?;
            (v["ticketId"] == ticket_id).then_some(v)
        });
        if let Some(v) = found {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "no sink delivery for {ticket_id} within {timeout:?}; sink: {bodies}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One scalar from the fixture's PostgreSQL, through `psql` inside its container.
fn sql(fixture: &Fixture, query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            fixture.pg.container.id(),
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
    assert!(
        out.status.success(),
        "psql {query}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The `instance_state` rows whose serialised state mentions `ticket_id`.
fn instance_ids_for(fixture: &Fixture, ticket_id: &str) -> Vec<String> {
    let rows = sql(
        fixture,
        &format!(
            "SELECT instance_id FROM instance_state \
             WHERE position(convert_to('{ticket_id}', 'UTF8') in serialised) > 0"
        ),
    );
    rows.lines().map(str::to_string).collect()
}

fn llm_jobs(fixture: &Fixture, channel: &str) -> u64 {
    sql(
        fixture,
        &format!("SELECT count(*) FROM llm_job WHERE channel = '{channel}'"),
    )
    .parse()
    .expect("count")
}

/// The instance that handled `ticket_id` is persisted and reported COMPLETED.
async fn assert_completed_instance(fixture: &Fixture, ticket_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ids = instance_ids_for(fixture, ticket_id);
        if ids.len() == 1 {
            let detail: Value = reqwest::get(format!(
                "http://127.0.0.1:{}/sutra/instances/{}",
                fixture.engine.http_port, ids[0]
            ))
            .await
            .expect("GET instance")
            .json()
            .await
            .expect("instance JSON");
            if detail["status"] == "COMPLETED" {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{ticket_id}: expected one COMPLETED instance row, have {ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The `data:` payloads of every `event: <name>` frame in an SSE body.
fn sse_data(body: &str, name: &str) -> Vec<String> {
    body.split("\n\n")
        .filter_map(|frame| {
            let mut event = None;
            let mut data = Vec::new();
            for line in frame.lines() {
                if let Some(e) = line.strip_prefix("event:") {
                    event = Some(e.trim().to_string());
                } else if let Some(d) = line.strip_prefix("data:") {
                    data.push(d.strip_prefix(' ').unwrap_or(d).to_string());
                }
            }
            (event.as_deref() == Some(name)).then(|| data.join("\n"))
        })
        .collect()
}

fn ticket(id: &str) -> Value {
    json!({ "ticketId": id, "subject": "Cannot log in",
            "description": "The password reset email never arrives." })
}

// ---- ai-summarize: the same three flows over each wire ---------------------------------------

async fn summarize_sync(fixture: &Fixture, id: &str) {
    let (status, _, body) = post(fixture, SUMMARIZE_KEY, "ticket", &ticket(id)).await;
    assert_eq!(status, 200, "{body}");
    let reply: Value = serde_json::from_str(&body).expect("JSON reply");
    assert_eq!(reply, json!({ "ticketId": id, "summary": SUMMARY }));
    assert!(
        instance_ids_for(fixture, id).is_empty(),
        "a sync call runs in memory and leaves no instance_state row"
    );
}

async fn summarize_stream(fixture: &Fixture, id: &str) {
    let (status, content_type, body) =
        post(fixture, SUMMARIZE_KEY, "ticket-stream", &ticket(id)).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );
    let chunks = sse_data(&body, "chunk");
    assert_eq!(
        chunks.len(),
        SUMMARY.split(' ').count(),
        "one chunk per streamed piece: {body}"
    );
    assert_eq!(chunks.concat(), SUMMARY, "the chunks add up to the answer");
    let replies = sse_data(&body, "reply");
    assert_eq!(replies.len(), 1, "{body}");
    let reply: Value = serde_json::from_str(&replies[0]).expect("reply frame is JSON");
    assert_eq!(reply, json!({ "ticketId": id, "summary": SUMMARY }));
    assert!(sse_data(&body, "error").is_empty(), "{body}");
    assert!(instance_ids_for(fixture, id).is_empty());
}

async fn summarize_async(fixture: &Fixture, id: &str) {
    let (status, _, body) = post(fixture, SUMMARIZE_KEY, "ticket-async", &ticket(id)).await;
    assert_eq!(status, 202, "{body}");
    let note = sink_delivery(id, Duration::from_secs(60)).await;
    assert_eq!(note, json!({ "ticketId": id, "summary": SUMMARY }));
    assert_completed_instance(fixture, id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_openai_wire_sync() {
    let before = stub_count("chat").await;
    summarize_sync(summarize_openai(), "AI-OA-SYNC-1").await;
    assert!(
        stub_count("chat").await > before,
        "answered over Chat Completions"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_openai_wire_streamed() {
    summarize_stream(summarize_openai(), "AI-OA-SSE-1").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_openai_wire_async() {
    summarize_async(summarize_openai(), "AI-OA-ASYNC-1").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_anthropic_wire_sync() {
    let before = stub_count("messages").await;
    summarize_sync(summarize_anthropic(), "AI-AN-SYNC-1").await;
    assert!(
        stub_count("messages").await > before,
        "answered over Messages"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_anthropic_wire_streamed() {
    summarize_stream(summarize_anthropic(), "AI-AN-SSE-1").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_summarize_anthropic_wire_async() {
    summarize_async(summarize_anthropic(), "AI-AN-ASYNC-1").await;
}

// ---- ai-advanced -----------------------------------------------------------------------------

async fn advanced_sync(channel: &str, body: Value) -> Value {
    let (status, _, text) = post(advanced(), ADVANCED_KEY, channel, &body).await;
    assert_eq!(status, 200, "{channel}: {text}");
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{channel} reply {text}: {e}"))
}

/// Anthropic first, OpenAI second: with the Anthropic candidate down the call falls back inside
/// the same request; with it up, Anthropic answers. The only test that touches the switch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_cross_provider_fallback() {
    advanced();
    set_outage(true).await;
    let gated = stub_count("flaky").await;
    let reply = advanced_sync("fallback", json!({})).await;
    assert_eq!(reply["provider"], "openai", "Anthropic is down: {reply}");
    assert_eq!(reply["text"], SUMMARY);
    assert!(
        stub_count("flaky").await > gated,
        "the Anthropic candidate was tried first"
    );

    set_outage(false).await;
    let reply = advanced_sync("fallback", json!({})).await;
    set_outage(true).await;
    assert_eq!(reply["provider"], "anthropic", "Anthropic is up: {reply}");
    assert_eq!(reply["text"], SUMMARY);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_json_mode() {
    let reply = advanced_sync(
        "classify",
        json!({ "ticketId": "AI-JSON-1", "description": "Checkout crashes for everyone." }),
    )
    .await;
    assert_eq!(reply["ticketId"], "AI-JSON-1");
    assert_eq!(reply["finish"], "stop");
    let object: Value = serde_json::from_str(reply["text"].as_str().expect("text"))
        .expect("the answer is a JSON object");
    assert_eq!(object, json!({ "category": "outage", "urgency": "high" }));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_tool_call() {
    let reply = advanced_sync("weather", json!({})).await;
    assert_eq!(reply["finish"], "tool-calls", "{reply}");
    let calls = reply["toolCalls"].as_array().expect("toolCalls");
    assert_eq!(calls.len(), 1, "{reply}");
    assert_eq!(calls[0]["name"], "get_weather");
    assert_eq!(calls[0]["arguments"], json!({ "city": "Paris" }));
}

/// A job flow: 202, the job row while the provider works, the sink delivery, the job row gone,
/// the instance COMPLETED.
async fn advanced_job(channel: &str, model_channel: &str, id: &str, via: &str) {
    let fixture = advanced();
    let (status, _, body) = post(
        fixture,
        ADVANCED_KEY,
        channel,
        &json!({ "ticketId": id, "description": "My parcel never arrived." }),
    )
    .await;
    assert_eq!(status, 202, "{body}");
    let note = sink_delivery(id, Duration::from_secs(60)).await;
    assert_eq!(
        note,
        json!({ "ticketId": id, "summary": SUMMARY, "via": via })
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while llm_jobs(fixture, model_channel) != 0 {
        assert!(
            Instant::now() < deadline,
            "{model_channel}: the delivered job's llm_job row is never deleted"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_completed_instance(fixture, id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_openai_job_polled() {
    let polls = stub_count("responses-poll").await;
    // The submit and the first poll are seconds apart, so the job row is observable in between.
    let watcher = tokio::spawn(async {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if llm_jobs(advanced(), "job-model") > 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    });
    advanced_job("summarize-job", "job-model", "AI-JOB-1", "job-poll").await;
    assert!(
        watcher.await.unwrap(),
        "the job was held as an llm_job row while the provider worked"
    );
    assert!(
        stub_count("responses-poll").await >= polls + 2,
        "the result was found by polling (in_progress, then completed)"
    );
}

/// The candidate's safety-net poll is 600 s, so a delivery within the test's minute can only be
/// the signed webhook: the engine verified it against the callback secret and fetched the result.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_openai_job_webhook() {
    advanced_job(
        "summarize-webhook",
        "webhook-model",
        "AI-HOOK-1",
        "job-webhook",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_ai_advanced_anthropic_batch() {
    let results = stub_count("batches-results").await;
    advanced_job("summarize-batch", "batch-model", "AI-BATCH-1", "batch").await;
    assert!(
        stub_count("batches-results").await > results,
        "the answer came from the ended batch's JSONL results"
    );
}

// ---- tier 1: every variant composes and lints clean ---------------------------------------

/// Every committed variant of both examples — the real-provider ones CI cannot call included —
/// composes over its `shared/` layer and passes package validation, as `sutra lint <variant>
/// --overlay <shared>` would report.
#[test]
fn ai_example_variants_compose_and_lint_clean() {
    for example in ["ai-summarize", "ai-advanced"] {
        let dir = util::examples_dir().join(example);
        let mut variants: Vec<_> = std::fs::read_dir(dir.join("variants"))
            .unwrap_or_else(|e| panic!("{example}/variants: {e}"))
            .flatten()
            .map(|e| e.path())
            .collect();
        variants.sort();
        assert!(!variants.is_empty(), "{example} has no variants");
        for variant in variants {
            let report = sutra_loader::lint_dir_with_overlays(&variant, &[dir.join("shared")]);
            let errors: Vec<_> = report.errors().map(|d| d.to_string()).collect();
            assert!(errors.is_empty(), "{}: {errors:#?}", variant.display());
        }
    }
}
