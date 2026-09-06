//! Ports of `CoverageRuntimeTest` (fired-flow trace + ordered-subsequence path marking)
//! and `CoverageAdminTest` (the `coverage:report` / `coverage:reset` reserved ops).
//!
//! Both run against the TYPED metric store — the single coverage surface since the module KV
//! covered-set was retired. `execute_sync` runs under
//! `DeploymentId::unresolved()`, hence [`DEP`].

use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::executor::feel_condition_evaluator;
use sutra_executor::listener::{ExecutionListener, InstanceEvent};
use sutra_executor::{
    CoverageCorrelation, CoverageMetricStore, InMemoryCoverageStore, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

/// The deployment id `execute_sync` runs under (`DeploymentId::unresolved()`).
const DEP: &str = "dep-ffffffffffffffffffffffff";

fn urns(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|s| s.to_string()).collect()
}

const RUNTIME_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:extensionElements>
          <q:coverage path="accept" flows="f1 f2"/>
          <q:coverage path="reject" flows="f1 f3"/>
        </bpmn:extensionElements>
        <bpmn:startEvent id="S"/>
        <bpmn:exclusiveGateway id="G" default="f2"/>
        <bpmn:endEvent id="EOk"/>
        <bpmn:endEvent id="ERej"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="G"/>
        <bpmn:sequenceFlow id="f2" sourceRef="G" targetRef="EOk"/>
        <bpmn:sequenceFlow id="f3" sourceRef="G" targetRef="ERej">
          <bpmn:conditionExpression>reject</bpmn:conditionExpression>
        </bpmn:sequenceFlow>
      </bpmn:process>
    </bpmn:definitions>"#;

fn coverage_executor(store: Rc<InMemoryCoverageStore>) -> TokenExecutor {
    TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(feel_condition_evaluator())
        .with_coverage_metric_store(store as Rc<dyn CoverageMetricStore>)
        .build()
}

#[tokio::test]
async fn each_branch_covers_its_path_and_marks_are_idempotent() {
    let store = Rc::new(InMemoryCoverageStore::new());
    store
        .seed_declared(DEP, &urns(&["accept", "reject"]))
        .await
        .unwrap(); // mirror seed-at-deploy
    let process = proc(RUNTIME_BPMN, "p");
    let exec = coverage_executor(Rc::clone(&store));

    // Walk the accept branch → only "accept" flips covered.
    exec.execute_sync(&process, vars(&[("reject", boolean(false))]))
        .await
        .unwrap();
    assert!(store.is_covered(DEP, "accept"));
    assert!(!store.is_covered(DEP, "reject"));

    // Walk the reject branch → "reject" now covered too; "accept" untouched.
    exec.execute_sync(&process, vars(&[("reject", boolean(true))]))
        .await
        .unwrap();
    assert!(store.is_covered(DEP, "accept"));
    assert!(store.is_covered(DEP, "reject"));

    // Re-walk accept → idempotent: the flag stays true, the total never grows.
    exec.execute_sync(&process, vars(&[("reject", boolean(false))]))
        .await
        .unwrap();
    let m = store.read_metrics(DEP).await.unwrap();
    assert_eq!((m.total, m.covered), (2, 2));
}

struct CoveredRecorder {
    covered: RefCell<Vec<String>>,
}

impl ExecutionListener for CoveredRecorder {
    fn on_path_covered(&self, _event: &InstanceEvent, path_id: &str) {
        self.covered.borrow_mut().push(path_id.to_string());
    }
}

#[tokio::test]
async fn fires_path_covered_event_once_per_new_mark() {
    let store = Rc::new(InMemoryCoverageStore::new());
    let process = proc(RUNTIME_BPMN, "p");
    let recorder = Rc::new(CoveredRecorder {
        covered: RefCell::new(Vec::new()),
    });
    let exec = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(feel_condition_evaluator())
        .with_coverage_metric_store(Rc::clone(&store) as Rc<dyn CoverageMetricStore>)
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    exec.execute_sync(&process, vars(&[("reject", boolean(false))]))
        .await
        .unwrap(); // accept newly covered → 1 event
    exec.execute_sync(&process, vars(&[("reject", boolean(false))]))
        .await
        .unwrap(); // re-cover accept → no new event (idempotent)
    exec.execute_sync(&process, vars(&[("reject", boolean(true))]))
        .await
        .unwrap(); // reject newly covered → 1 event

    assert_eq!(recorder.covered.borrow().as_slice(), &["accept", "reject"]);
}

#[tokio::test]
async fn no_coverage_store_lookup_when_process_has_no_paths() {
    // A process without <q:coverage> never touches the store (opt-in) — no store wired.
    let plain = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="q">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(plain, "q");
    let result = TokenExecutor::builder(TaskRegistry::new())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert!(result.visited_nodes.contains("E"));
}

// ---- typed metric store (intra flag vs cross-process fragment) -------------------------

#[tokio::test]
async fn intra_process_completion_flips_its_metric_flag() {
    // An intra-process `<q:coverage>` path (author mnemonic, no injected `#p` suffix) → its
    // completion flips the seeded metric flag, NOT a fragment. The metric store is the ONLY
    // coverage surface wired (independent of the KV covered-set).
    let store = Rc::new(InMemoryCoverageStore::new());
    // `execute_sync` runs under `DeploymentId::unresolved()`.
    let dep = "dep-ffffffffffffffffffffffff";
    store
        .seed_declared(dep, &["accept".to_string(), "reject".to_string()])
        .await
        .unwrap(); // mirror seed-at-deploy

    let process = proc(RUNTIME_BPMN, "p");
    let exec = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(feel_condition_evaluator())
        .with_coverage_metric_store(Rc::clone(&store) as Rc<dyn CoverageMetricStore>)
        .build();

    // Walk the accept branch → only "accept" flips true.
    exec.execute_sync(&process, vars(&[("reject", boolean(false))]))
        .await
        .unwrap();

    let m = store.read_metrics(dep).await.unwrap();
    assert_eq!((m.total, m.covered), (2, 1));
    assert_eq!(m.uncovered, vec!["reject".to_string()]);
    // No fragment for an intra path.
    assert!(store.read_fragments(dep).await.unwrap().is_empty());
}

const XPROC_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p1">
        <bpmn:extensionElements>
          <q:coverage path="urn:sutra:coverage:orders:e2e:reply1#p1" flows="f1"/>
        </bpmn:extensionElements>
        <bpmn:startEvent id="S"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

#[tokio::test]
async fn cross_process_injected_subpath_writes_a_fragment_with_correlation() {
    use std::collections::BTreeMap;
    use sutra_executor::{DeploymentId, StatefulExecResult};

    // A desugar-injected cross-process sub-path (`…:reply1#p1`) → its completion writes a
    // reconstruction fragment carrying the parsed route_urn + segment_process + instance_id and
    // the pass's correlation dims (trace_id + business_key). It does NOT flip any metric flag
    // (the ROUTE flag is phase-5 `coverage check`'s union-find verdict).
    let store = Rc::new(InMemoryCoverageStore::new());
    let dep = DeploymentId::of("dep-0000000000000000000000c6").unwrap();
    let process = proc(XPROC_BPMN, "p1");
    let exec = TokenExecutor::builder(TaskRegistry::new())
        .with_coverage_metric_store(Rc::clone(&store) as Rc<dyn CoverageMetricStore>)
        .build();

    let result = exec
        .execute_stateful_from_correlated(
            &process,
            vars(&[]),
            dep.clone(),
            BTreeMap::new(),
            None,
            CoverageCorrelation {
                trace_id: Some("tA".to_string()),
                business_key: Some("txn-1".to_string()),
            },
        )
        .await
        .unwrap();
    let StatefulExecResult::Completed { instance_id, .. } = result else {
        panic!("straight-through process must complete");
    };

    let frags = store.read_fragments(dep.value()).await.unwrap();
    assert_eq!(frags.len(), 1);
    let f = &frags[0];
    assert_eq!(f.route_urn, "urn:sutra:coverage:orders:e2e:reply1");
    assert_eq!(f.segment_process, "p1");
    assert_eq!(f.instance_id, instance_id);
    assert_eq!(f.business_key.as_deref(), Some("txn-1"));
    assert_eq!(f.trace_id.as_deref(), Some("tA"));

    // The cross-process fragment path leaves the metric-flag table untouched (no intra mark).
    assert_eq!(store.read_metrics(dep.value()).await.unwrap().total, 0);
}

// ---- coverage across suspend/resume (persisted per-path cursor) ------------------------

// A route (`viaB` = f1 f2 f3 f5 f7) that spans TWO wait states (U1, then U2). An exclusive
// gateway sits between the waits: the U1-relay keeps `takeB` true so the instance routes via B
// (walking f3, f5) and parks at U2; the U2-relay flips `takeB` false so the FINAL replay diverges
// through C (f4, f6) yet re-converges at U2 and completes. The route was therefore walked in full
// only ACROSS passes — never in the single final pass — so the per-pass `flow_trace` alone misses
// it. Only the persisted per-path contiguous-prefix cursor (seeded across resumes) marks it.
const WAIT_COVERAGE_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="wcov">
        <bpmn:extensionElements>
          <q:coverage path="viaB" flows="f1 f2 f3 f5 f7"/>
        </bpmn:extensionElements>
        <bpmn:startEvent id="S"/>
        <bpmn:userTask id="U1"/>
        <bpmn:exclusiveGateway id="G" default="f4"/>
        <bpmn:manualTask id="B"/>
        <bpmn:manualTask id="C"/>
        <bpmn:userTask id="U2"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U1"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U1" targetRef="G"/>
        <bpmn:sequenceFlow id="f3" sourceRef="G" targetRef="B">
          <bpmn:conditionExpression>takeB</bpmn:conditionExpression>
        </bpmn:sequenceFlow>
        <bpmn:sequenceFlow id="f4" sourceRef="G" targetRef="C"/>
        <bpmn:sequenceFlow id="f5" sourceRef="B" targetRef="U2"/>
        <bpmn:sequenceFlow id="f6" sourceRef="C" targetRef="U2"/>
        <bpmn:sequenceFlow id="f7" sourceRef="U2" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn wait_coverage_executor(store: Rc<InMemoryCoverageStore>) -> TokenExecutor {
    TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(var_truthy_evaluator())
        .with_coverage_metric_store(store as Rc<dyn CoverageMetricStore>)
        .build()
}

/// Drive the two-wait divergent-replay instance to completion, optionally threading each
/// suspend's `coverage_progress` into the next resume as `prior_coverage`. Returns whether the
/// `viaB` route was marked in the store at completion.
async fn run_two_wait_instance(store: &Rc<InMemoryCoverageStore>, thread_coverage: bool) -> bool {
    use std::collections::BTreeMap;
    use sutra_executor::{DeploymentId, StatefulExecResult, Variables};

    let process = proc(WAIT_COVERAGE_BPMN, "wcov");
    let exec = wait_coverage_executor(Rc::clone(store));
    let dep = DeploymentId::of("dep-000000000000000000000071").unwrap();
    let iid = "11111111-2222-4333-8444-555555555555";
    let empty: BTreeMap<String, u64> = BTreeMap::new();

    // Pass 1 — parks at U1. No coverage marked (suspended).
    let first = exec
        .execute_stateful_from(
            &process,
            vars(&[("takeB", boolean(true))]),
            dep.clone(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes: w1,
        completed_nodes: c1,
        coverage_progress: cov1,
        ..
    } = first
    else {
        panic!("pass 1 must suspend at U1");
    };

    // Pass 2 — relay satisfies U1 with takeB still true → routes via B (walks f3, f5), parks at U2.
    let second = exec
        .resume(
            &process,
            iid,
            &c1,
            vars(&[("takeB", boolean(true))]),
            "U1",
            &Variables::new(),
            dep.clone(),
            BTreeMap::new(),
            None,
            &w1,
            if thread_coverage { &cov1 } else { &empty },
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes: w2,
        completed_nodes: c2,
        coverage_progress: cov2,
        ..
    } = second
    else {
        panic!("pass 2 must suspend at U2");
    };

    // Pass 3 — relay satisfies U2 and FLIPS takeB false → final replay diverges through C, but
    // re-converges at U2 and completes.
    let third = exec
        .resume(
            &process,
            iid,
            &c2,
            vars(&[("takeB", boolean(true))]),
            "U2",
            &vars(&[("takeB", boolean(false))]),
            dep.clone(),
            BTreeMap::new(),
            None,
            &w2,
            if thread_coverage { &cov2 } else { &empty },
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(third.is_completed(), "pass 3 must complete");

    store.is_covered(dep.value(), "viaB")
}

#[tokio::test]
async fn coverage_marked_across_suspend_resume_via_persisted_cursor() {
    // Threading the persisted per-path cursor across resumes marks the route whose flows span two
    // wait states — even though the final replay never walked the whole route in one pass. This
    // FAILS without the persisted cursor (mark_coverage used the per-pass ordered-subsequence trace, which
    // the divergent final replay never satisfies) and PASSES after.
    let store = Rc::new(InMemoryCoverageStore::new());
    let marked = run_two_wait_instance(&store, true).await;
    assert!(
        marked,
        "route viaB spans two wait states; the persisted cursor must mark it at completion"
    );
}

#[tokio::test]
async fn coverage_not_marked_across_suspend_resume_without_threaded_cursor() {
    // NEGATIVE control — with the SAME divergent-replay instance but WITHOUT threading the cursor
    // (today's behavior: each resume starts from a zero cursor), the final pass alone routes via C
    // and never completes the viaB prefix, so the route stays unmarked. Proves the threaded cursor
    // is load-bearing (the fix is not a blanket "always mark").
    let store = Rc::new(InMemoryCoverageStore::new());
    let marked = run_two_wait_instance(&store, false).await;
    assert!(
        !marked,
        "without the threaded cursor the divergent final replay must leave viaB unmarked"
    );
}

// ---- CoverageAdminTest ----------------------------------------------------------------

const ADMIN_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="transfer">
        <bpmn:extensionElements>
          <q:coverage path="accept" flows="tf1 tf2"/>
          <q:coverage path="reject" flows="tf1 tf3"/>
        </bpmn:extensionElements>
        <bpmn:startEvent id="TS"/>
        <bpmn:exclusiveGateway id="TG" default="tf2"/>
        <bpmn:endEvent id="TEok"/>
        <bpmn:endEvent id="TErej"/>
        <bpmn:sequenceFlow id="tf1" sourceRef="TS" targetRef="TG"/>
        <bpmn:sequenceFlow id="tf2" sourceRef="TG" targetRef="TEok"/>
        <bpmn:sequenceFlow id="tf3" sourceRef="TG" targetRef="TErej">
          <bpmn:conditionExpression>reject</bpmn:conditionExpression>
        </bpmn:sequenceFlow>
      </bpmn:process>
      <bpmn:process id="coverage">
        <bpmn:startEvent id="RS"/>
        <bpmn:serviceTask id="Report" implementation="coverage:report:transfer"/>
        <bpmn:endEvent id="RE"/>
        <bpmn:sequenceFlow id="rf1" sourceRef="RS" targetRef="Report"/>
        <bpmn:sequenceFlow id="rf2" sourceRef="Report" targetRef="RE"/>
      </bpmn:process>
      <bpmn:process id="coverage-reset">
        <bpmn:startEvent id="XS"/>
        <bpmn:serviceTask id="Reset" implementation="coverage:reset:transfer"/>
        <bpmn:endEvent id="XE"/>
        <bpmn:sequenceFlow id="xf1" sourceRef="XS" targetRef="Reset"/>
        <bpmn:sequenceFlow id="xf2" sourceRef="Reset" targetRef="XE"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn admin_executor(
    store: Rc<InMemoryCoverageStore>,
    transfer: sutra_bpmn::ProcessDefinition,
) -> TokenExecutor {
    let transfer = std::sync::Arc::new(transfer);
    TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(feel_condition_evaluator())
        .with_coverage_metric_store(store as Rc<dyn CoverageMetricStore>)
        .with_process_resolver(move |id| {
            Ok((id == "transfer").then(|| std::sync::Arc::clone(&transfer)))
        })
        .build()
}

/// Seed `transfer`'s two declared paths and flip the named ones covered.
async fn seeded_admin_store(covered: &[&str]) -> Rc<InMemoryCoverageStore> {
    let store = Rc::new(InMemoryCoverageStore::new());
    store
        .seed_declared(DEP, &urns(&["accept", "reject"]))
        .await
        .unwrap();
    for path in covered {
        store.mark_path_covered(DEP, path).await.unwrap();
    }
    store
}

#[tokio::test]
async fn report_computes_covered_uncovered_and_percentage() {
    let module = load(ADMIN_BPMN);
    let store = seeded_admin_store(&["accept"]).await; // 1 of 2 covered
    let exec = admin_executor(
        Rc::clone(&store),
        module.process("transfer").unwrap().clone(),
    );

    let result = exec
        .execute_sync(&module.process("coverage").unwrap().clone(), vars(&[]))
        .await
        .unwrap();
    let FeelValue::Map(report) = result.output("coverageReport").expect("report present") else {
        panic!("coverageReport is not a map");
    };

    assert_eq!(report.get("covered"), Some(&num(1)));
    assert_eq!(report.get("total"), Some(&num(2)));
    assert_eq!(report.get("percentage"), Some(&FeelValue::from(50.0)));
    assert_eq!(
        report.get("coveredPaths"),
        Some(&FeelValue::List(vec![string("accept")]))
    );
    assert_eq!(
        report.get("uncoveredPaths"),
        Some(&FeelValue::List(vec![string("reject")]))
    );
}

#[tokio::test]
async fn reset_clears_the_covered_set() {
    let module = load(ADMIN_BPMN);
    let store = seeded_admin_store(&["accept", "reject"]).await;
    let exec = admin_executor(
        Rc::clone(&store),
        module.process("transfer").unwrap().clone(),
    );

    let result = exec
        .execute_sync(
            &module.process("coverage-reset").unwrap().clone(),
            vars(&[]),
        )
        .await
        .unwrap();

    // The flags are cleared but the rows stay seeded — the total to cover never shrinks.
    assert!(!store.is_covered(DEP, "accept"));
    assert!(!store.is_covered(DEP, "reject"));
    let m = store.read_metrics(DEP).await.unwrap();
    assert_eq!((m.total, m.covered), (2, 0));

    let FeelValue::Map(reset) = result.output("coverageReset").expect("reset present") else {
        panic!("coverageReset is not a map");
    };
    assert_eq!(reset.get("cleared"), Some(&num(2)));
    assert_eq!(reset.get("total"), Some(&num(2)));
    assert_eq!(reset.get("process"), Some(&string("transfer")));

    // Re-running the reset clears nothing (nothing was covered) but still reports the total.
    let again = exec
        .execute_sync(
            &module.process("coverage-reset").unwrap().clone(),
            vars(&[]),
        )
        .await
        .unwrap();
    let FeelValue::Map(reset) = again.output("coverageReset").expect("reset present") else {
        panic!("coverageReset is not a map");
    };
    assert_eq!(reset.get("cleared"), Some(&num(0)));
    assert_eq!(reset.get("total"), Some(&num(2)));
}

// ---- frozen report shapes + the no-engine-database diagnostic --------------------------

#[tokio::test]
async fn report_and_reset_serialize_in_the_frozen_shape() {
    // The field NAMES (and the percentage's rounding) are the contract every downstream
    // template/channel binds to — asserted on the serialized form, not just the numbers.
    let module = load(ADMIN_BPMN);
    let store = seeded_admin_store(&["accept"]).await;
    let exec = admin_executor(
        Rc::clone(&store),
        module.process("transfer").unwrap().clone(),
    );

    let run = exec
        .execute_sync(&module.process("coverage").unwrap().clone(), vars(&[]))
        .await
        .unwrap();
    let report = sutra_executor::variables::feel_to_json(run.output("coverageReport").unwrap());
    assert_eq!(
        report.to_string(),
        r#"{"covered":1,"coveredPaths":["accept"],"percentage":50.0,"process":"transfer","total":2,"uncoveredPaths":["reject"]}"#
    );

    let run = exec
        .execute_sync(
            &module.process("coverage-reset").unwrap().clone(),
            vars(&[]),
        )
        .await
        .unwrap();
    let reset = sutra_executor::variables::feel_to_json(run.output("coverageReset").unwrap());
    assert_eq!(
        reset.to_string(),
        r#"{"cleared":1,"process":"transfer","total":2}"#
    );
}

#[tokio::test]
async fn report_without_a_coverage_store_fails_loudly_instead_of_reporting_zero() {
    // RULED 2026-08-04 (§7 superseding): coverage marks are persisted in the deployment's OWN
    // declared `coverage` store, so no store wired ⇒ no coverage. A silent 0% would read as a
    // real measurement, so the op fails with a diagnostic naming the cause.
    let module = load(ADMIN_BPMN);
    let transfer = std::sync::Arc::new(module.process("transfer").unwrap().clone());
    let exec = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(feel_condition_evaluator())
        .with_process_resolver(move |id| {
            Ok((id == "transfer").then(|| std::sync::Arc::clone(&transfer)))
        })
        .build();

    let err = exec
        .execute_sync(&module.process("coverage").unwrap().clone(), vars(&[]))
        .await
        .expect_err("no metric store must fail the coverage op");
    let rendered = format!("{err:?}");
    assert!(
        rendered.contains("SUTRA.CONFIG.COVERAGE.STORE_MISSING"),
        "diagnostic code names the missing coverage store: {rendered}"
    );
    assert!(
        rendered.contains("no coverage store wired")
            && rendered.contains("'coverage' data store the deployment declares"),
        "diagnostic names the CAUSE (no declared coverage store): {rendered}"
    );
}
