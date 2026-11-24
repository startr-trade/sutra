//! The audit plan is what runs: each fixture in `sutra-bpmn/tests/fixtures/audit` is loaded, its
//! [`sutra_bpmn::AuditPlan`] compared with the committed `*.audit.json` (the shape
//! `sutra lint --audit --format json` prints), and the process executed. The nodes that emit an
//! audited `NODE_*` event are exactly the plan's audited nodes among those the run reached, and a
//! node emits `NODE_ENTERED` exactly when the plan says so.
//!
//! Re-bless the JSON after an intentional change with
//! `SUTRA_BLESS=1 cargo test -p sutra-executor --test all audit_plan_runtime`.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::ElementAuditDisposition;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
use sutra_executor::{TaskError, TaskRegistry, TokenExecutor};

const FIXTURES: [&str; 7] = [
    "no-audit",
    "bare",
    "all",
    "points",
    "instance",
    "opt-out",
    "boundary-fired",
];

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sutra-bpmn/tests/fixtures/audit")
}

/// One reported event: its type, its node (none for an instance event) and its sink (none when
/// it is not audited).
type ReportedEvent = (String, Option<String>, Option<String>);

/// Every event, with whether it was audited (routed to a sink).
#[derive(Default)]
struct Trail {
    events: RefCell<Vec<ReportedEvent>>,
}

impl ExecutionListener for Trail {
    fn on_instance_started(&self, e: &InstanceEvent) {
        self.events
            .borrow_mut()
            .push(("INSTANCE_STARTED".into(), None, e.audit_sink.clone()));
    }
    fn on_instance_completed(&self, e: &InstanceEvent) {
        self.events
            .borrow_mut()
            .push(("INSTANCE_COMPLETED".into(), None, e.audit_sink.clone()));
    }
    fn on_token_entered(&self, e: &TokenEvent) {
        self.events.borrow_mut().push((
            "NODE_ENTERED".into(),
            Some(e.node_id.clone()),
            e.audit_sink.clone(),
        ));
    }
    fn on_token_left(&self, e: &TokenEvent) {
        self.events.borrow_mut().push((
            "NODE_LEFT".into(),
            Some(e.node_id.clone()),
            e.audit_sink.clone(),
        ));
    }
}

struct Run {
    plan: sutra_bpmn::AuditPlan,
    /// `(event type, node id)` of every AUDITED event, in order.
    audited: Vec<(String, Option<String>)>,
    /// Every node the run reported, audited or not.
    seen: BTreeSet<String>,
}

async fn run(fixture: &str) -> Run {
    let xml = std::fs::read_to_string(fixture_dir().join(format!("{fixture}.bpmn")))
        .expect("fixture exists");
    let mut process = proc(&xml, "flow");
    process.set_audit_sink(Some("sql".to_string()));
    let plan = process.audit_plan().clone();
    let registry = TaskRegistry::new()
        .register("noop", |_, _| ok_map(&[]))
        .register("raiser", |_, _| Err(TaskError::BpmnError("E_ANY".into())));
    let trail = Rc::new(Trail::default());
    TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&trail) as Rc<dyn ExecutionListener>)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_or_else(|e| panic!("{fixture} runs: {e:?}"));
    let events = trail.events.borrow().clone();
    for (_, _, sink) in &events {
        if let Some(sink) = sink {
            assert_eq!(
                sink, "sql",
                "{fixture}: the resolved sink rides every event"
            );
        }
    }
    Run {
        plan,
        seen: events.iter().filter_map(|(_, n, _)| n.clone()).collect(),
        audited: events
            .into_iter()
            .filter(|(_, _, sink)| sink.is_some())
            .map(|(t, n, _)| (t, n))
            .collect(),
    }
}

fn audited_nodes(run: &Run, event_type: &str) -> BTreeSet<String> {
    run.audited
        .iter()
        .filter(|(t, _)| t == event_type)
        .filter_map(|(_, n)| n.clone())
        .collect()
}

#[tokio::test]
async fn audit_plan_matches_runtime() {
    for fixture in FIXTURES {
        let run = run(fixture).await;

        // The plan is the committed JSON.
        let json_path = fixture_dir().join(format!("{fixture}.audit.json"));
        let rendered = format!(
            "{}\n",
            serde_json::to_string_pretty(&run.plan.to_json()).unwrap()
        );
        if std::env::var_os("SUTRA_BLESS").is_some() {
            std::fs::write(&json_path, &rendered).unwrap();
        } else {
            let on_disk = std::fs::read_to_string(&json_path).unwrap_or_default();
            assert_eq!(on_disk, rendered, "{} is stale", json_path.display());
        }

        // The run's audited nodes are exactly the plan's, among the nodes it reached.
        let mut planned_left = BTreeSet::new();
        let mut planned_entered = BTreeSet::new();
        for (id, disposition) in &run.plan.elements {
            if !run.seen.contains(id) {
                continue;
            }
            if let ElementAuditDisposition::Desugared { entered, .. } = disposition {
                planned_left.insert(id.clone());
                if *entered {
                    planned_entered.insert(id.clone());
                }
            }
        }
        assert_eq!(
            audited_nodes(&run, "NODE_LEFT"),
            planned_left,
            "{fixture}: NODE_LEFT"
        );
        assert_eq!(
            audited_nodes(&run, "NODE_ENTERED"),
            planned_entered,
            "{fixture}: NODE_ENTERED"
        );
        let instance_audited = run.audited.iter().any(|(t, _)| t.starts_with("INSTANCE_"));
        assert_eq!(
            instance_audited,
            run.plan.audits_instance(),
            "{fixture}: instance events"
        );
    }
}

/// No tag, no audit — not one event is routed.
#[tokio::test]
async fn no_audit_declared_no_events() {
    let run = run("no-audit").await;
    assert!(run.audited.is_empty(), "{:?}", run.audited);
    assert!(!run.seen.is_empty(), "the process still runs");
    assert!(!run.plan.audits_instance());
}

#[tokio::test]
async fn points_scope_records_only_points() {
    for fixture in ["points", "bare"] {
        let run = run(fixture).await;
        let expected: BTreeSet<String> = ["S", "X", "Sub", "SS", "SE", "E"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(audited_nodes(&run, "NODE_LEFT"), expected, "{fixture}");
        // None of them waits, so none records its entry.
        assert!(audited_nodes(&run, "NODE_ENTERED").is_empty(), "{fixture}");
        for gateway_or_task in ["G", "T", "F", "A", "M", "J"] {
            assert!(
                run.seen.contains(gateway_or_task),
                "{fixture}: {gateway_or_task} ran"
            );
        }
    }
}

#[tokio::test]
async fn instance_scope_records_lifecycle_only() {
    let run = run("instance").await;
    let types: Vec<&str> = run.audited.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(types, vec!["INSTANCE_STARTED", "INSTANCE_COMPLETED"]);
}

#[tokio::test]
async fn node_opt_out_beats_scope() {
    let run = run("opt-out").await;
    let left = audited_nodes(&run, "NODE_LEFT");
    assert!(!left.contains("T"), "{left:?}");
    assert!(run.seen.contains("T"));
    for node in ["S", "G", "X", "F", "A", "M", "J", "E"] {
        assert!(left.contains(node), "events=all records {node}: {left:?}");
    }
    assert_eq!(
        run.plan.disposition("T"),
        Some(&ElementAuditDisposition::OptedOut)
    );
}

#[tokio::test]
async fn fired_boundary_is_a_point() {
    let run = run("boundary-fired").await;
    let left = audited_nodes(&run, "NODE_LEFT");
    assert!(
        left.contains("B"),
        "the fired boundary is recorded: {left:?}"
    );
    assert!(left.contains("EErr"), "{left:?}");
    assert!(!left.contains("T") && !left.contains("R"), "{left:?}");
}
