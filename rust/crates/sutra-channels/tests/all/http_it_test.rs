//! HTTP-level tests driving the REAL shipped resources read-only —
//! `examples/money-transfer` + `examples/approval-hold` channels.yaml + BPMN + templates
//! — over the axum transport, asserting the money-transfer wire behaviour end to end
//! (accept / reject / balance flows; coverage report/reset flows), plus auth, ack-mode,
//! problem rendering, and the catch-all route.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::support::StructuralStandInCodec;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bigdecimal::BigDecimal;
use http_body_util::BodyExt;
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::http::{channel_router, spawn_engine, EngineHandle};
use sutra_channels::{
    load_channel_definitions, ChannelDefinition, ChannelEngine, CodecRegistry, CollectingOutbox,
    DrainingSink, InMemoryAliasStore, InMemoryInboxStore, InboundChain, ProcessModuleRegistry,
    ValidatorRegistry,
};
use sutra_executor::{
    ArtifactType, CoverageMetricStore, DataStore, DeploymentId, HbsTemplateEngine,
    InMemoryCoverageStore, InMemoryDataStore, TaskRegistry, TemplateEngineRegistry,
    TemplateRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;
use tower::util::ServiceExt;

const MT: &str = "examples/money-transfer/deployments-src/default--money-transfer--1.0.0";
const MT_TENANT: &str = "examples/money-transfer/deployments-src/default--money-transfer--1.0.0";
const AH: &str = "examples/approval-hold/deployments-src/default--approval--1.0.0";
const AH_TENANT: &str = "examples/approval-hold/deployments-src/default--approval--1.0.0";

const TRANSFER_KEY: &str = "transfer-demo-key";
const APPROVAL_KEY: &str = "approval-demo-key";

fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn read(path: &str) -> Vec<u8> {
    let full = repo_root().join(path);
    std::fs::read(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()))
}

fn account(balance: i64, frozen: bool) -> FeelValue {
    let mut m = BTreeMap::new();
    m.insert(
        "balance".to_string(),
        FeelValue::Number(BigDecimal::from(balance)),
    );
    m.insert("frozen".to_string(), FeelValue::Boolean(frozen));
    FeelValue::Map(m)
}

/// The two example deployments' opaque ids — this test's stand-in for the archive manifest-hash
/// identity. Production stamps that id onto every channel binding at archive load; here we stamp
/// the hand-parsed bindings the same way (see `stamp_bindings`) so `binding.deployment_id()`
/// matches the module-registry key. Shared so `build_engine` (dispatch) and `app` (router) agree.
fn mt_dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000011").expect("valid deployment id")
}
fn ah_dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000012").expect("valid deployment id")
}

/// Stamp every parsed binding with `dep` — mirrors the archive-load stamp (`assembly.rs`): a
/// freshly parsed binding is `unresolved()` until its deployment identity is applied.
fn stamp_bindings(defs: &mut [ChannelDefinition], dep: &DeploymentId) {
    for d in defs {
        d.binding.deployment = dep.clone();
    }
}

/// Build the two-module engine — runs ON the actor thread (`Rc`-based).
fn build_engine() -> ChannelEngine {
    let mt_dep = mt_dep();
    let ah_dep = ah_dep();

    // BPMN modules (read-only, the shipped files).
    let mut processes = ProcessModuleRegistry::new();
    for file in [
        "bpmn/transfer.bpmn",
        "bpmn/balance-query.bpmn",
        "bpmn/coverage-report.bpmn",
        "bpmn/coverage-reset.bpmn",
    ] {
        let module = BpmnModelLoader::new()
            .load(&read(&format!("{MT}/{file}")))
            .unwrap_or_else(|e| panic!("load {file}: {e}"));
        processes.register(&mt_dep, &module);
    }
    for file in [
        "bpmn/approval-hold.bpmn",
        "bpmn/template-showcase.bpmn",
        "bpmn/datamapping-showcase.bpmn",
    ] {
        let module = BpmnModelLoader::new()
            .load(&read(&format!("{AH}/{file}")))
            .unwrap_or_else(|e| panic!("load {file}: {e}"));
        processes.register(&ah_dep, &module);
    }

    // Templates, keyed by their deployment-scoped artifact ids.
    let mut templates = TemplateRegistry::new();
    for file in [
        "transfer-result.hbs",
        "transfer-rejected.hbs",
        "balance.hbs",
        "coverage-report.hbs",
        "coverage-reset.hbs",
    ] {
        templates.register(
            &mt_dep.artifact(ArtifactType::Template, file),
            read(&format!("{MT}/templates/{file}")),
        );
    }
    for file in ["greeting.hbs", "datamap-reply.hbs"] {
        templates.register(
            &ah_dep.artifact(ArtifactType::Template, file),
            read(&format!("{AH}/templates/{file}")),
        );
    }

    // The module-owned durable stores: the `accounts` ledger (seeded like the IT's
    // migrations: alice/bob/carol at 100, frozen-fred frozen) + the coverage set.
    let accounts = Rc::new(InMemoryDataStore::new("accounts"));
    // The store SPI is async — seed via block_on (InMemory puts never error).
    crate::support::test_runtime().block_on(async {
        accounts.put("alice", account(100, false)).await.unwrap();
        accounts.put("bob", account(100, false)).await.unwrap();
        accounts.put("carol", account(100, false)).await.unwrap();
        accounts
            .put("frozen-fred", account(100, true))
            .await
            .unwrap();
    });
    let coverage = Rc::new(InMemoryCoverageStore::new());
    // Seed-at-deploy, as the assembly does: the transfer flow's two declared <q:coverage>
    // paths start seeded `covered = false` in the typed metric store (the single coverage
    // surface — the module KV covered-set was retired).
    crate::support::test_runtime().block_on(async {
        coverage
            .seed_declared(
                mt_dep.value(),
                &["accept".to_string(), "reject".to_string()],
            )
            .await
            .unwrap();
    });

    let sink = Rc::new(DrainingSink::new());
    let module_resolver_view = processes.clone();
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(HbsTemplateEngine::new()),
            templates,
        )
        .with_data_stores(move |_, name| {
            (name == "accounts").then(|| Rc::clone(&accounts) as Rc<dyn DataStore>)
        })
        .with_coverage_metric_store(coverage as Rc<dyn CoverageMetricStore>)
        .with_module_resolver(move |deployment, id| {
            module_resolver_view.find_in_module(deployment, id)
        })
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();

    // The package user structural codecs the channels bind (stand-ins for the real
    // schema-bound family). A user codec is referenced by its path-derived URN
    // `urn:<path from schemas/>`, so schemas/transfer/ → urn:transfer, schemas/approval/ →
    // urn:approval — the codec registers under (and channels bind) exactly that name.
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register(StructuralStandInCodec::compile(
        "urn:transfer",
        &read(&format!("{MT}/schemas/transfer/transfer.xsd")),
    ));
    codecs.register(StructuralStandInCodec::compile(
        "urn:approval",
        &read(&format!("{AH}/schemas/approval/approval.xsd")),
    ));

    let mut mt_defs = load_channel_definitions(
        &read(&format!("{MT_TENANT}/channels.yaml")),
        "default",
        "money-transfer",
        "1.0.0",
        "channels.yaml",
    )
    .expect("money-transfer channels load");
    stamp_bindings(&mut mt_defs, &mt_dep);
    let mut ah_defs = load_channel_definitions(
        &read(&format!("{AH_TENANT}/channels.yaml")),
        "default",
        "approval",
        "1.0.0",
        "channels.yaml",
    )
    .expect("approval channels load");
    stamp_bindings(&mut ah_defs, &ah_dep);

    ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_channel_definitions(&mt_defs)
    .with_channel_definitions(&ah_defs)
    .with_process_registry(processes)
    .with_alias_store(Rc::new(InMemoryAliasStore::new()))
    .with_inbox(Rc::new(InMemoryInboxStore::new()))
    .with_outbox(Rc::new(CollectingOutbox::new()))
    .build()
}

/// One router over BOTH example modules' channels (fresh engine per test).
fn app() -> Router {
    let handle: EngineHandle = spawn_engine(tokio::runtime::Handle::current(), build_engine);
    let mut defs = load_channel_definitions(
        &read(&format!("{MT_TENANT}/channels.yaml")),
        "default",
        "money-transfer",
        "1.0.0",
        "channels.yaml",
    )
    .expect("loads");
    stamp_bindings(&mut defs, &mt_dep());
    let mut ah_defs = load_channel_definitions(
        &read(&format!("{AH_TENANT}/channels.yaml")),
        "default",
        "approval",
        "1.0.0",
        "channels.yaml",
    )
    .expect("loads");
    stamp_bindings(&mut ah_defs, &ah_dep());
    defs.extend(ah_defs);
    channel_router(&defs, handle).expect("router builds")
}

struct Reply {
    status: StatusCode,
    content_type: String,
    body: String,
}

async fn post(app: &Router, path: &str, api_key: &str, content_type: &str, body: &str) -> Reply {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", content_type)
        .header("X-Api-Key", api_key)
        .body(Body::from(body.to_string()))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    Reply {
        status,
        content_type,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

async fn post_transfer(app: &Router, from: &str, to: &str, amount: &str) -> Reply {
    post(
        app,
        "/channels/transfer-request",
        TRANSFER_KEY,
        "application/json",
        &format!(r#"{{"TransferRequest":{{"fromId":"{from}","toId":"{to}","amount":{amount}}}}}"#),
    )
    .await
}

async fn balance_of(app: &Router, account_id: &str) -> BigDecimal {
    let r = post(
        app,
        "/channels/balance",
        TRANSFER_KEY,
        "application/json",
        &format!(r#"{{"BalanceQuery":{{"accountId":"{account_id}"}}}}"#),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let marker = "balance=\"";
    let start = r
        .body
        .find(marker)
        .map(|i| i + marker.len())
        .unwrap_or_else(|| panic!("reply carries a balance attribute: {}", r.body));
    let end = r.body[start..].find('"').expect("closing quote") + start;
    r.body[start..end].parse().expect("numeric balance")
}

// ---- Durability + cross-instance -----------------------------------------------------------

#[tokio::test]
async fn durability_transfer_persists_and_a_later_balance_query_reads_it() {
    let app = app();
    let r = post_transfer(&app, "alice", "bob", "50").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    // The <q:reply mode="native" contentType="application/xml"> rides the connection.
    assert_eq!(r.content_type, "application/xml");
    assert!(r.body.contains("<TransferAccepted"), "{}", r.body);
    assert!(r.body.contains(r#"from="alice""#), "{}", r.body);
    assert!(r.body.contains(r#"to="bob""#), "{}", r.body);
    assert!(r.body.contains(r#"newFromBalance="50""#), "{}", r.body);
    assert!(r.body.contains(r#"newToBalance="150""#), "{}", r.body);

    // Cross-instance: a SEPARATE balance-query instance reads the persisted balances.
    assert_eq!(balance_of(&app, "alice").await, BigDecimal::from(50));
    assert_eq!(balance_of(&app, "bob").await, BigDecimal::from(150));
}

// ---- Consistency (Order(2)/(3)) --------------------------------------------------------------

#[tokio::test]
async fn consistency_insufficient_funds_is_rejected_and_balances_unchanged() {
    let app = app();
    let alice_before = balance_of(&app, "alice").await;
    let bob_before = balance_of(&app, "bob").await;

    let r = post_transfer(&app, "alice", "bob", "1000").await;
    assert!(r.body.contains("<TransferRejected"), "{}", r.body);
    assert!(
        r.body.contains(r#"reason="insufficient-funds""#),
        "{}",
        r.body
    );

    assert_eq!(balance_of(&app, "alice").await, alice_before);
    assert_eq!(balance_of(&app, "bob").await, bob_before);
}

#[tokio::test]
async fn consistency_frozen_account_is_rejected_and_balances_unchanged() {
    let app = app();
    let alice_before = balance_of(&app, "alice").await;
    let fred_before = balance_of(&app, "frozen-fred").await;

    let r = post_transfer(&app, "alice", "frozen-fred", "10").await;
    assert!(r.body.contains("<TransferRejected"), "{}", r.body);
    assert!(r.body.contains(r#"reason="frozen-account""#), "{}", r.body);

    assert_eq!(balance_of(&app, "alice").await, alice_before);
    assert_eq!(balance_of(&app, "frozen-fred").await, fred_before);
}

// ---- Isolation (Order(5)) — N concurrent transfers, no lost update ---------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolation_concurrent_transfers_leave_the_exact_balance() {
    let app = app();
    let n = 8;
    let carol_before = balance_of(&app, "carol").await;
    let bob_before = balance_of(&app, "bob").await;

    let mut handles = Vec::new();
    for _ in 0..n {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            post_transfer(&app, "carol", "bob", "1").await
        }));
    }
    let mut accepted = 0;
    for h in handles {
        let r = h.await.expect("join");
        if r.status == StatusCode::OK && r.body.contains("<TransferAccepted") {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, n,
        "all concurrent transfers serialized + accepted"
    );

    assert_eq!(
        balance_of(&app, "carol").await,
        carol_before - BigDecimal::from(n)
    );
    assert_eq!(
        balance_of(&app, "bob").await,
        bob_before + BigDecimal::from(n)
    );
}

// ---- Path coverage (Order(6)/(7)) --------------------------------------------------------------

#[tokio::test]
async fn coverage_report_reads_full_after_both_branches_then_reset_clears_it() {
    let app = app();
    // Drive BOTH declared <q:coverage> routes: a committed transfer (accept) and a
    // rejected one (reject) — the ACID suite's Order(1)+Order(2) equivalent.
    let accept = post_transfer(&app, "alice", "bob", "50").await;
    assert!(accept.body.contains("<TransferAccepted"), "{}", accept.body);
    let reject = post_transfer(&app, "alice", "bob", "100000").await;
    assert!(reject.body.contains("<TransferRejected"), "{}", reject.body);

    // coverage:report:transfer over coverage-report.bpmn — 100% (2/2).
    let report = post(
        &app,
        "/channels/coverage-query",
        TRANSFER_KEY,
        "application/json",
        r#"{"CoverageQuery":{"process":"transfer"}}"#,
    )
    .await;
    assert_eq!(report.status, StatusCode::OK, "{}", report.body);
    assert!(report.body.contains("<CoverageReport"), "{}", report.body);
    assert!(
        report.body.contains(r#"process="transfer""#),
        "{}",
        report.body
    );
    assert!(
        report.body.contains(r#"percentage="100.0""#),
        "{}",
        report.body
    );
    assert!(report.body.contains(r#"covered="2""#), "{}", report.body);
    assert!(report.body.contains(r#"total="2""#), "{}", report.body);
    assert!(
        report.body.contains("<covered>accept</covered>"),
        "{}",
        report.body
    );
    assert!(
        report.body.contains("<covered>reject</covered>"),
        "{}",
        report.body
    );
    assert!(!report.body.contains("<uncovered>"), "{}", report.body);

    // coverage:reset:transfer clears the covered-set.
    let reset = post(
        &app,
        "/channels/coverage-reset",
        TRANSFER_KEY,
        "application/json",
        r#"{"CoverageReset":{"process":"transfer"}}"#,
    )
    .await;
    assert_eq!(reset.status, StatusCode::OK, "{}", reset.body);
    assert!(reset.body.contains("<CoverageReset"), "{}", reset.body);
    assert!(
        reset.body.contains(r#"process="transfer""#),
        "{}",
        reset.body
    );
    assert!(reset.body.contains(r#"cleared="2""#), "{}", reset.body);
    assert!(reset.body.contains(r#"total="2""#), "{}", reset.body);

    // A fresh report reads 0%, both paths uncovered.
    let after = post(
        &app,
        "/channels/coverage-query",
        TRANSFER_KEY,
        "application/json",
        r#"{"CoverageQuery":{"process":"transfer"}}"#,
    )
    .await;
    assert!(after.body.contains(r#"percentage="0.0""#), "{}", after.body);
    assert!(after.body.contains(r#"covered="0""#), "{}", after.body);
    assert!(
        after.body.contains("<uncovered>accept</uncovered>"),
        "{}",
        after.body
    );
    assert!(
        after.body.contains("<uncovered>reject</uncovered>"),
        "{}",
        after.body
    );
    assert!(!after.body.contains("<covered>"), "{}", after.body);
}

// ---- XML wire (the same codec serves xml/json/yaml) --------------------------------------------

#[tokio::test]
async fn transfer_request_accepts_the_xml_wire_form_too() {
    let app = app();
    let r = post(
        &app,
        "/channels/transfer-request",
        TRANSFER_KEY,
        "application/xml",
        "<TransferRequest><fromId>alice</fromId><toId>bob</toId><amount>25</amount></TransferRequest>",
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert!(r.body.contains("<TransferAccepted"), "{}", r.body);
    assert!(r.body.contains(r#"newFromBalance="75""#), "{}", r.body);
}

// ---- Auth (ApiKeyAuthHandler semantics over the wire) -------------------------------------------

#[tokio::test]
async fn missing_api_key_is_401_problem_json() {
    let app = app();
    let request = Request::builder()
        .method("POST")
        .uri("/channels/balance")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"BalanceQuery":{"accountId":"alice"}}"#))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let problem: serde_json::Value = serde_json::from_slice(&body).expect("problem json");
    assert_eq!(problem["code"], "SUTRA.INBOUND.REJECTED.AUTH");
    assert_eq!(problem["status"], 401);
}

#[tokio::test]
async fn wrong_api_key_is_401() {
    let app = app();
    let r = post(
        &app,
        "/channels/balance",
        "not-the-key",
        "application/json",
        r#"{"BalanceQuery":{"accountId":"alice"}}"#,
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert!(r.body.contains("SUTRA.INBOUND.REJECTED.AUTH"), "{}", r.body);
}

#[tokio::test]
async fn authorization_apikey_scheme_is_accepted() {
    let app = app();
    let request = Request::builder()
        .method("POST")
        .uri("/channels/balance")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("apikey {TRANSFER_KEY}"))
        .body(Body::from(r#"{"BalanceQuery":{"accountId":"alice"}}"#))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

// ---- Routing edges --------------------------------------------------------------------------------

#[tokio::test]
async fn unknown_channel_path_renders_a_404_problem() {
    let app = app();
    let r = post(
        &app,
        "/channels/nope",
        TRANSFER_KEY,
        "application/json",
        "{}",
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(
        r.body.contains("SUTRA.RESOLVE.CHANNEL.UNKNOWN"),
        "{}",
        r.body
    );
}

#[tokio::test]
async fn unhandled_message_type_is_a_500_problem_with_the_no_start_event_code() {
    // CoverageQuery on the transfer-request channel: the codec decodes it, but no start
    // event of the module subscribes (transfer-request, CoverageQuery).
    let app = app();
    let r = post(
        &app,
        "/channels/transfer-request",
        TRANSFER_KEY,
        "application/json",
        r#"{"CoverageQuery":{"process":"transfer"}}"#,
    )
    .await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        r.body
            .contains("SUTRA.INBOUND.NO_START_EVENT_FOR_MESSAGE_TYPE"),
        "{}",
        r.body
    );
}

// ---- approval-hold module over the same wire ------------------------------------------------------

#[tokio::test]
async fn template_showcase_renders_the_hbs_reply_from_an_xml_approval_request() {
    let app = app();
    let r = post(
        &app,
        "/channels/showcase-request",
        APPROVAL_KEY,
        "application/xml",
        "<ApprovalRequest><E2EId>E2E-7</E2EId><Amount>750</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.content_type, "application/xml");
    assert_eq!(r.body, r#"<Hbs prepped="ok" e2e="E2E-7" amount="750"/>"#);
}

#[tokio::test]
async fn datamapping_showcase_replies_the_derived_data() {
    let app = app();
    let r = post(
        &app,
        "/channels/datamapping-showcase-request",
        APPROVAL_KEY,
        "application/xml",
        "<ApprovalRequest><E2EId>E2E-9</E2EId><Amount>1</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert!(
        r.body
            .contains(r#"<DataMapShowcase amount="1500" riskBand="high" note="high-band"/>"#),
        "{}",
        r.body
    );
}

#[tokio::test]
async fn wait_state_approval_hold_fails_fast_without_persistence() {
    // approval-hold.bpmn parks at a wait node — this harness wires no InstanceStore, so the inbound
    // fails BEFORE executing (the INBOUND_PERSISTENCE_REQUIRED posture).
    let app = app();
    let r = post(
        &app,
        "/channels/approval-request",
        APPROVAL_KEY,
        "application/xml",
        "<ApprovalRequest><E2EId>E2E-1</E2EId><Amount>10</Amount></ApprovalRequest>",
    )
    .await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        r.body.contains("SUTRA.INBOUND.PERSISTENCE_REQUIRED"),
        "{}",
        r.body
    );
}

// ---- ack-mode: on-persist → 202 empty --------------------------------------------------------------

#[tokio::test]
async fn on_persist_channel_answers_202_with_an_empty_body() {
    // A synthetic http channel declaring ack-mode: on-persist over a tiny echo flow —
    // the flow still runs; the reply does NOT ride the inbound connection.
    let yaml = b"channels:\n  - name: fire-and-forget\n    transport: http\n    bind: \"POST /channels/fire-and-forget\"\n    codec: raw-text\n    ack-mode: on-persist\n    auth:\n      scheme: apikey\n      apikey:\n        value: fnf-key\n";
    let mut defs =
        load_channel_definitions(yaml, "t", "echo-mod", "1.0.0", "test.yaml").expect("loads");
    stamp_bindings(
        &mut defs,
        &DeploymentId::of("dep-000000000000000000000013").expect("valid deployment id"),
    );
    let defs_for_router = defs.clone();
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        let bpmn = r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:q="urn:sutra:q:1.0">
              <bpmn:process id="echo">
                <bpmn:startEvent id="S">
                  <bpmn:extensionElements><q:source channel="fire-and-forget" name="payload"/></bpmn:extensionElements>
                </bpmn:startEvent>
                <bpmn:endEvent id="E"/>
                <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
              </bpmn:process>
            </bpmn:definitions>"#;
        let module = BpmnModelLoader::new()
            .load(bpmn.as_bytes())
            .expect("BPMN loads");
        let sink = Rc::new(DrainingSink::new());
        let executor = TokenExecutor::builder(TaskRegistry::new())
            .with_feel()
            .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
            .build();
        ChannelEngine::builder(
            executor,
            sink,
            InboundChain::new(
                CodecRegistry::with_builtins(),
                sutra_channels::FormatRegistry::with_builtins(),
                ValidatorRegistry::new(),
            ),
        )
        .with_channel_definitions(&defs)
        .with_module(
            &DeploymentId::of("dep-000000000000000000000013").expect("valid deployment id"),
            &module,
        )
        .build()
    });
    let app = channel_router(&defs_for_router, handle).expect("router builds");
    let r = post(
        &app,
        "/channels/fire-and-forget",
        "fnf-key",
        "text/plain",
        "hello",
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    assert!(
        r.body.is_empty(),
        "202 carries no business body: {}",
        r.body
    );
}

// ---- a REAL bound listener (port 0 — never a fixed port) --------------------------------------------

#[tokio::test]
async fn bound_listener_on_port_zero_serves_a_real_socket_round_trip() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let app = app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind port 0");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let body = r#"{"TransferRequest":{"fromId":"alice","toId":"bob","amount":50}}"#;
    let request = format!(
        "POST /channels/transfer-request HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nX-Api-Key: {TRANSFER_KEY}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).await.expect("read");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("<TransferAccepted"), "{response}");
    assert!(response.contains(r#"newFromBalance="50""#), "{response}");
}
