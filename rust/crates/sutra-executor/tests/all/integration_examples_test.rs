//! Integration proof for the sync executor — loads REAL shipped BPMN read-only from `examples/` and
//! executes the sync paths with stub emissions, asserting routing / template-render /
//! coverage outcomes consistent with what those examples' integration tests assert over HTTP:
//!
//! - `examples/money-transfer/.../transfer.bpmn` (+ its `<q:coverage>` accept/reject routes,
//!   the `<bpmn:transaction>` ACID scope, and the Handlebars reply templates), plus the
//!   `coverage-report.bpmn` admin flow (`coverage:report:transfer` reserved op).
//! - `examples/approval-hold/.../template-showcase.bpmn`, `datamapping-showcase.bpmn` and
//!   `script-showcase.bpmn` (declarative showcases: FEEL data-assignment nodes, exclusive
//!   -gateway routing, scoped `<q:param>`, Handlebars script tasks with injected uuid/now).
//!
//! The `<q:reply mode="native">` on these flows is a SYNCHRONOUS reply (no destination →
//! rides the inbound connection), so the assertions read the rendered `responseBody`
//! variable — the same body the ITs assert over HTTP.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;

use crate::common::*;
use bigdecimal::BigDecimal;
use sutra_bpmn::{BpmnModelLoader, ProcessDefinition};
use sutra_executor::{
    CoverageMetricStore, DataStore, DeploymentId, HbsTemplateEngine, InMemoryCoverageStore,
    InMemoryDataStore, ScriptRegistry, TaskRegistry, TemplateEngineRegistry, TemplateRegistry,
    TokenExecutor, Variables,
};
use sutra_feel::FeelValue;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn read(path: &str) -> Vec<u8> {
    let full = repo_root().join(path);
    std::fs::read(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()))
}

fn load_process(path: &str, id: &str) -> ProcessDefinition {
    BpmnModelLoader::new()
        .load(&read(path))
        .unwrap_or_else(|e| panic!("load {path}: {e}"))
        .process(id)
        .unwrap()
        .clone()
}

fn response_body(result: &sutra_executor::ExecResult) -> String {
    match result.output("responseBody") {
        Some(FeelValue::String(s)) => s.clone(),
        other => panic!("expected responseBody string, got {other:?}"),
    }
}

// ---- money-transfer: transfer.bpmn (ACID ledger) + coverage admin ------------------------

const MT: &str = "examples/money-transfer/deployments-src/default--money-transfer--1.0.0";

fn account(balance: i64, frozen: bool) -> FeelValue {
    fmap(&[
        ("balance", FeelValue::Number(BigDecimal::from(balance))),
        ("frozen", boolean(frozen)),
    ])
}

fn transfer_payload(from: &str, to: &str, amount: i64) -> Variables {
    vars(&[(
        "payload",
        fmap(&[
            ("fromId", string(from)),
            ("toId", string(to)),
            ("amount", num(amount)),
        ]),
    )])
}

fn money_transfer_executor(
    accounts: Rc<InMemoryDataStore>,
    coverage: Rc<InMemoryCoverageStore>,
    transfer: ProcessDefinition,
) -> TokenExecutor {
    let mut templates = TemplateRegistry::new();
    for file in [
        "transfer-result.hbs",
        "transfer-rejected.hbs",
        "coverage-report.hbs",
    ] {
        templates.register(file, read(&format!("{MT}/templates/{file}")));
    }
    let transfer = std::sync::Arc::new(transfer);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(HbsTemplateEngine::new()),
            templates,
        )
        .with_data_stores(move |_, name| {
            (name == "accounts").then(|| Rc::clone(&accounts) as Rc<dyn DataStore>)
        })
        .with_coverage_metric_store(coverage as Rc<dyn CoverageMetricStore>)
        .with_process_resolver(move |id| {
            Ok((id == "transfer").then(|| std::sync::Arc::clone(&transfer)))
        })
        .build()
}

async fn balance_of(store: &InMemoryDataStore, key: &str) -> BigDecimal {
    match store.get(key).await.unwrap() {
        Some(FeelValue::Map(m)) => match m.get("balance") {
            Some(FeelValue::Number(n)) => n.clone(),
            other => panic!("expected number balance, got {other:?}"),
        },
        other => panic!("expected map account, got {other:?}"),
    }
}

#[tokio::test]
async fn money_transfer_accept_reject_and_coverage_report_match_the_it_outcomes() {
    let transfer = load_process(&format!("{MT}/bpmn/transfer.bpmn"), "transfer");
    // The transfer flow declares the two business-outcome coverage routes.
    let path_ids: Vec<&str> = transfer
        .coverage_paths
        .iter()
        .map(|p| p.id.as_str())
        .collect();
    assert_eq!(path_ids, vec!["accept", "reject"]);
    // Multi-start (http / queue / kafka intakes) — driven from the http start event.
    assert_eq!(transfer.start_events().len(), 3);
    assert!(transfer.is_sync_eligible());

    let accounts = Rc::new(InMemoryDataStore::new("accounts"));
    accounts.put("alice", account(100, false)).await.unwrap();
    accounts.put("bob", account(100, false)).await.unwrap();
    let coverage = Rc::new(InMemoryCoverageStore::new());
    // Mirror seed-at-deploy: the declared paths start seeded `covered = false`.
    coverage
        .seed_declared(
            DeploymentId::unresolved().value(),
            &["accept".to_string(), "reject".to_string()],
        )
        .await
        .unwrap();
    let executor =
        money_transfer_executor(Rc::clone(&accounts), Rc::clone(&coverage), transfer.clone());

    // ---- accept: alice → bob 30 commits atomically and replies TransferAccepted ----
    let accepted = executor
        .execute_sync_from(
            &transfer,
            transfer_payload("alice", "bob", 30),
            DeploymentId::unresolved(),
            BTreeMap::new(),
            Some("Start"),
        )
        .await
        .unwrap();
    assert!(accepted.visited_nodes.contains("OkReply"));
    assert!(!accepted.visited_nodes.contains("RejectReply"));
    let body = response_body(&accepted);
    assert!(body.contains("<TransferAccepted"), "{body}");
    assert!(body.contains(r#"from="alice""#), "{body}");
    assert!(body.contains(r#"to="bob""#), "{body}");
    assert!(body.contains(r#"amount="30""#), "{body}");
    assert!(body.contains(r#"newFromBalance="70""#), "{body}");
    assert!(body.contains(r#"newToBalance="130""#), "{body}");
    assert_eq!(balance_of(&accounts, "alice").await, BigDecimal::from(70));
    assert_eq!(balance_of(&accounts, "bob").await, BigDecimal::from(130));
    let dep = DeploymentId::unresolved();
    assert!(coverage.is_covered(dep.value(), "accept"));
    assert!(!coverage.is_covered(dep.value(), "reject"));

    // ---- reject: insufficient funds rolls the transaction back, replies TransferRejected ----
    let rejected = executor
        .execute_sync_from(
            &transfer,
            transfer_payload("alice", "bob", 1000),
            DeploymentId::unresolved(),
            BTreeMap::new(),
            Some("Start"),
        )
        .await
        .unwrap();
    assert!(rejected.visited_nodes.contains("RejectReply"));
    assert!(!rejected.visited_nodes.contains("OkReply"));
    let body = response_body(&rejected);
    assert!(body.contains("<TransferRejected"), "{body}");
    assert!(body.contains(r#"reason="insufficient-funds""#), "{body}");
    // Atomicity: the failed transfer left both balances untouched.
    assert_eq!(balance_of(&accounts, "alice").await, BigDecimal::from(70));
    assert_eq!(balance_of(&accounts, "bob").await, BigDecimal::from(130));
    assert!(coverage.is_covered(dep.value(), "reject"));

    // ---- frozen account: the other reject reason ----
    accounts.put("carol", account(500, true)).await.unwrap();
    let frozen = executor
        .execute_sync_from(
            &transfer,
            transfer_payload("carol", "bob", 10),
            DeploymentId::unresolved(),
            BTreeMap::new(),
            Some("Start"),
        )
        .await
        .unwrap();
    let body = response_body(&frozen);
    assert!(body.contains(r#"reason="frozen-account""#), "{body}");

    // ---- coverage-report.bpmn: the reserved coverage:report:transfer op → 100% ----
    let report_process = load_process(
        &format!("{MT}/bpmn/coverage-report.bpmn"),
        "coverage-report",
    );
    let report_run = executor
        .execute_sync_from(
            &report_process,
            vars(&[]),
            DeploymentId::unresolved(),
            BTreeMap::new(),
            Some("Start"),
        )
        .await
        .unwrap();
    let FeelValue::Map(report) = report_run.output("coverageReport").expect("report") else {
        panic!("coverageReport is not a map");
    };
    assert_eq!(report.get("covered"), Some(&num(2)));
    assert_eq!(report.get("total"), Some(&num(2)));
    assert_eq!(report.get("percentage"), Some(&FeelValue::from(100.0)));
    let body = response_body(&report_run);
    assert!(body.contains("<CoverageReport"), "{body}");
    assert!(body.contains(r#"percentage="100"#), "{body}");
    assert!(body.contains("<covered>accept</covered>"), "{body}");
    assert!(body.contains("<covered>reject</covered>"), "{body}");
}

// ---- approval-hold declarative showcases ---------------------------------------------------

const AH: &str = "examples/approval-hold/deployments-src/default--approval--1.0.0";

fn approval_payload(e2e: &str, amount: i64) -> Variables {
    vars(&[(
        "payload",
        fmap(&[("E2EId", string(e2e)), ("Amount", num(amount))]),
    )])
}

#[tokio::test]
async fn template_showcase_routes_the_hbs_branch_and_renders_the_reply() {
    let process = load_process(
        &format!("{AH}/bpmn/template-showcase.bpmn"),
        "template-showcase",
    );

    let mut templates = TemplateRegistry::new();
    templates.register(
        "greeting.hbs",
        read(&format!("{AH}/templates/greeting.hbs")),
    );
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(HbsTemplateEngine::new()),
            templates,
        )
        .build();

    let result = executor
        .execute_sync(&process, approval_payload("E2E-7", 750))
        .await
        .unwrap();

    // The FEEL data-assignment node fed the gateway: no "XSLT" marker → the Handlebars branch.
    assert_eq!(result.output("prepped"), Some(&string("ok")));
    assert_eq!(result.output("engineChoice"), Some(&string("hbs")));
    assert!(result.visited_nodes.contains("RenderHbs"));
    assert!(!result.visited_nodes.contains("RenderXslt"));
    assert_eq!(
        response_body(&result),
        r#"<Hbs prepped="ok" e2e="E2E-7" amount="750"/>"#
    );
}

#[tokio::test]
async fn datamapping_showcase_renders_the_scoped_param_without_persisting_it() {
    let process = load_process(
        &format!("{AH}/bpmn/datamapping-showcase.bpmn"),
        "datamapping-showcase",
    );

    let mut templates = TemplateRegistry::new();
    templates.register(
        "datamap-reply.hbs",
        read(&format!("{AH}/templates/datamap-reply.hbs")),
    );
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(HbsTemplateEngine::new()),
            templates,
        )
        .build();

    let result = executor
        .execute_sync(&process, approval_payload("E2E-9", 1))
        .await
        .unwrap();

    // Seed / Derive are FEEL data-assignment nodes (amount := 1500, riskBand := "high").
    assert_eq!(result.output("amount"), Some(&num(1500)));
    assert_eq!(result.output("riskBand"), Some(&string("high")));
    // The reply rendered the scoped <q:param riskNote> — which never persisted.
    assert_eq!(result.output("riskNote"), None);
    let body = response_body(&result);
    assert!(
        body.contains(r#"<DataMapShowcase amount="1500" riskBand="high" note="high-band"/>"#),
        "{body}"
    );
}

#[tokio::test]
async fn script_showcase_merges_typed_script_state_and_replies() {
    let process = load_process(
        &format!("{AH}/bpmn/script-showcase.bpmn"),
        "script-showcase",
    );

    let mut templates = TemplateRegistry::new();
    templates.register(
        "script-reply.hbs",
        read(&format!("{AH}/templates/script-reply.hbs")),
    );
    let mut scripts = ScriptRegistry::new();
    for file in ["derive-metadata.hbs", "derive-decision.hbs"] {
        scripts.register(file, read(&format!("{AH}/scripts/{file}")));
    }
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(HbsTemplateEngine::new()),
            templates,
        )
        .with_scripts(scripts)
        // Deterministic render-context suppliers (injected by the caller — never wall-clock
        // in the template engine).
        .with_uuid_supplier(|| "fixed-uuid-1234".to_string())
        .with_now_supplier(|| "2026-07-11T00:00:00Z".to_string())
        .build();

    let result = executor
        .execute_sync(&process, approval_payload("E2E-11", 42))
        .await
        .unwrap();

    // The first script's {{uuid}} render merged typed state; the second re-read it.
    assert_eq!(
        result.output("correlationId"),
        Some(&string("fixed-uuid-1234"))
    );
    assert_eq!(
        result.output("correlationEcho"),
        Some(&string("fixed-uuid-1234"))
    );
    assert_eq!(result.output("autoApprove"), Some(&boolean(true)));
    assert_eq!(result.output("scriptEngine"), Some(&string("handlebars")));
    let body = response_body(&result);
    assert!(
        body.contains(
            r#"<ScriptShowcase autoApprove="true" correlationId="fixed-uuid-1234" correlationEcho="fixed-uuid-1234" engines="handlebars+handlebars"/>"#
        ),
        "{body}"
    );
}
