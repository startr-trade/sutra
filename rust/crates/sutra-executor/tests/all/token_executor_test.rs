//! The token executor's walking skeleton (sync-executor scope) plus the channel-call
//! typed-NotYetImplemented check and the suspend/resume lifecycle pins.
//!
//! serviceTask implementations are registered task FUNCTIONS (there is no bean SPI), and a
//! failing task returns `TaskError::Failed(..)`, which the executor wraps into a diagnostic
//! whose message text the assertions check.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TaskEvent, TokenEvent};
use sutra_executor::{DeploymentId, ExecError, TaskContextView, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

#[tokio::test]
async fn runs_linear_process_and_captures_output() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${stamp}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("stamp", |_, _| {
        ok_map(&[("stampedBy", string("test-task"))])
    });
    let executor = TokenExecutor::builder(registry).build();
    let result = executor
        .execute_sync(&process, vars(&[("inboundId", string("INB-42"))]))
        .await
        .unwrap();

    assert_eq!(result.output("stampedBy"), Some(&string("test-task")));
    assert_eq!(result.output("inboundId"), Some(&string("INB-42")));
    let mut visited: Vec<&str> = result.visited_nodes.iter().map(|s| s.as_str()).collect();
    visited.sort_unstable();
    assert_eq!(visited, vec!["E", "S", "T"]);
}

#[tokio::test]
async fn exclusive_gateway_takes_first_satisfied_flow() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:exclusiveGateway id="G" default="fLow"/>
            <bpmn:endEvent id="EHigh"/>
            <bpmn:endEvent id="ELow"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="G"/>
            <bpmn:sequenceFlow id="fHigh" sourceRef="G" targetRef="EHigh">
              <bpmn:conditionExpression>HIGH</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fLow" sourceRef="G" targetRef="ELow"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(|expr, vars| {
            Ok(expr == "HIGH" && vars.get("isHigh") == Some(&FeelValue::Boolean(true)))
        })
        .build();

    let high = executor
        .execute_sync(&process, vars(&[("isHigh", boolean(true))]))
        .await
        .unwrap();
    assert!(high.visited_nodes.contains("EHigh"));
    assert!(!high.visited_nodes.contains("ELow"));

    let low = executor
        .execute_sync(&process, vars(&[("isHigh", boolean(false))]))
        .await
        .unwrap();
    assert!(low.visited_nodes.contains("ELow"));
    assert!(!low.visited_nodes.contains("EHigh"));
}

#[tokio::test]
async fn parallel_gateway_forks_and_joins() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:parallelGateway id="Fork"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TB" implementation="${b}"/>
            <bpmn:parallelGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="TA"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="TB"/>
            <bpmn:sequenceFlow id="f4" sourceRef="TA" targetRef="Join"/>
            <bpmn:sequenceFlow id="f5" sourceRef="TB" targetRef="Join"/>
            <bpmn:sequenceFlow id="f6" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let calls = Rc::new(RefCell::new(0));
    let (ca, cb) = (Rc::clone(&calls), Rc::clone(&calls));
    let registry = TaskRegistry::new()
        .register("a", move |_, _| {
            *ca.borrow_mut() += 1;
            ok_map(&[("ranA", boolean(true))])
        })
        .register("b", move |_, _| {
            *cb.borrow_mut() += 1;
            ok_map(&[("ranB", boolean(true))])
        });
    let executor = TokenExecutor::builder(registry).build();
    let result = executor.execute_sync(&process, vars(&[])).await.unwrap();

    assert_eq!(*calls.borrow(), 2);
    assert_eq!(result.output("ranA"), Some(&boolean(true)));
    assert_eq!(result.output("ranB"), Some(&boolean(true)));
    let mut visited: Vec<&str> = result.visited_nodes.iter().map(|s| s.as_str()).collect();
    visited.sort_unstable();
    assert_eq!(visited, vec!["E", "Fork", "Join", "S", "TA", "TB"]);
}

#[tokio::test]
async fn task_exception_wraps_as_sutra_exception() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${boom}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("boom", |_, _| {
        Err(sutra_executor::TaskError::Failed("kaboom".to_string()))
    });
    let executor = TokenExecutor::builder(registry).build();
    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.TASK.UNCAUGHT");
    assert!(e.message().contains("boom"), "{e}");
}

#[tokio::test]
async fn task_context_carries_tenant_and_instance_id() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${inspect}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let capture: Rc<RefCell<Option<TaskContextView>>> = Rc::new(RefCell::new(None));
    let cap = Rc::clone(&capture);
    let registry = TaskRegistry::new().register("inspect", move |_, ctx| {
        *cap.borrow_mut() = Some(ctx.clone());
        ok_map(&[])
    });
    let executor = TokenExecutor::builder(registry).build();
    executor
        .execute_sync_with(
            &process,
            vars(&[("foo", string("bar"))]),
            DeploymentId::of("dep-000000000000000000000091").expect("valid deployment id"),
            [("tenant".to_string(), "acme".to_string())].into(),
        )
        .await
        .unwrap();

    let ctx = capture.borrow().clone().expect("ctx captured");
    assert_eq!(
        ctx.deployment.value(),
        DeploymentId::of("dep-000000000000000000000091")
            .expect("valid deployment id")
            .value()
    );
    assert_eq!(ctx.labels.get("tenant").map(|s| s.as_str()), Some("acme"));
    assert_eq!(ctx.module_id, "p1");
    assert_eq!(ctx.variable("foo"), Some(&string("bar")));
    assert!(!ctx.simulation);
}

struct Recorder {
    events: RefCell<Vec<String>>,
}

impl ExecutionListener for Recorder {
    fn on_instance_started(&self, _e: &InstanceEvent) {
        self.events.borrow_mut().push("instance.started".into());
    }
    fn on_instance_completed(&self, _e: &InstanceEvent) {
        self.events.borrow_mut().push("instance.completed".into());
    }
    fn on_token_entered(&self, e: &TokenEvent) {
        self.events
            .borrow_mut()
            .push(format!("token.entered:{}", e.node_id));
    }
    fn on_token_left(&self, e: &TokenEvent) {
        self.events
            .borrow_mut()
            .push(format!("token.left:{}", e.node_id));
    }
    fn on_task_invoked(&self, e: &TaskEvent) {
        self.events
            .borrow_mut()
            .push(format!("task.invoked:{}", e.task_name));
    }
    fn on_task_completed(&self, e: &TaskEvent) {
        self.events.borrow_mut().push(format!(
            "task.completed:{}:duration>0={}",
            e.task_name,
            e.duration_nanos > 0
        ));
    }
}

#[tokio::test]
async fn registered_listener_receives_lifecycle_events_in_order() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${noop}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("noop", |_, _| ok_map(&[]));
    let recorder = Rc::new(Recorder {
        events: RefCell::new(Vec::new()),
    });
    let executor = TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();
    executor.execute_sync(&process, vars(&[])).await.unwrap();

    assert_eq!(
        recorder.events.borrow().as_slice(),
        &[
            "instance.started",
            "token.entered:S",
            "token.left:S",
            "token.entered:T",
            "task.invoked:noop",
            "task.completed:noop:duration>0=true",
            "token.left:T",
            "token.entered:E",
            "token.left:E",
            "instance.completed",
        ]
    );
}

/// One recorded audit directive: `(node_id, audit_sink, payload_json)`.
type PayloadEvent = (String, Option<String>, Option<String>);

/// Records each node's resolved audit directive (`audit_sink`, `payload_json`) from
/// `on_token_entered` (B1 single-sink routing + process-level capture + node suppression).
struct PayloadRecorder {
    events: RefCell<Vec<PayloadEvent>>,
}

impl ExecutionListener for PayloadRecorder {
    fn on_token_entered(&self, e: &TokenEvent) {
        self.events.borrow_mut().push((
            e.node_id.clone(),
            e.audit_sink.clone(),
            e.payload_json.clone(),
        ));
    }
}

/// B1 — process-level `<q:audit sink capture>` drives single-sink routing + payload capture; the
/// ONLY node-level override is `capture="none"` (suppression). A `@sensitive` variable is masked.
#[tokio::test]
async fn process_audit_routes_to_one_sink_captures_payload_and_node_none_suppresses() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>
              <q:audit sink="jsonl" capture="payload"/>
              <q:variable name="amount"/>
              <q:variable name="ssn" sensitive="true"/>
            </bpmn:extensionElements>
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${noop}"/>
            <bpmn:serviceTask id="U" implementation="${noop}">
              <bpmn:extensionElements><q:audit capture="none"/></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="U"/>
            <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    // The process-level policy is parsed and pinned.
    let audit = process.audit.as_ref().expect("process declares <q:audit>");
    assert_eq!(audit.sink, "jsonl");

    let registry = TaskRegistry::new().register("noop", |_, _| ok_map(&[]));
    let recorder = Rc::new(PayloadRecorder {
        events: RefCell::new(Vec::new()),
    });
    let executor = TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();
    executor
        .execute_sync(
            &process,
            vars(&[("amount", string("100")), ("ssn", string("000-00-0000"))]),
        )
        .await
        .unwrap();

    let events = recorder.events.borrow();
    let of = |node: &str| {
        events
            .iter()
            .find(|(n, _, _)| n == node)
            .map(|(_, sink, payload)| (sink.clone(), payload.clone()))
            .unwrap()
    };

    // A normal node routes to the process's single sink AND carries the redacted payload.
    let (sink, payload) = of("T");
    assert_eq!(
        sink.as_deref(),
        Some("jsonl"),
        "routes to the process's single sink"
    );
    let captured = payload.expect("process captures at payload level");
    let v: serde_json::Value = serde_json::from_str(&captured).unwrap();
    assert_eq!(
        v["amount"], "100",
        "non-sensitive variable captured verbatim"
    );
    assert_eq!(v["ssn"], "***REDACTED***", "@sensitive value masked");
    assert!(
        !captured.contains("000-00-0000"),
        "the raw @sensitive value must never appear in the audit payload"
    );

    // Process-level capture applies process-wide — the start event is captured + routed too.
    assert_eq!(of("S").0.as_deref(), Some("jsonl"));

    // The ONLY node-level override: capture="none" SUPPRESSES that node (no sink → no event).
    assert_eq!(of("U").0, None, "capture=\"none\" suppresses the node");
}

struct Thrower;

impl ExecutionListener for Thrower {
    fn on_instance_started(&self, _e: &InstanceEvent) {
        panic!("boom");
    }
}

#[tokio::test]
async fn listener_panic_does_not_break_execution() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_listener(Rc::new(Thrower) as Rc<dyn ExecutionListener>)
        .build();

    // Execution completes despite the listener panicking — listeners are observation-only.
    let result = executor.execute_sync(&process, vars(&[])).await.unwrap();
    assert!(!result.instance_id.is_empty());
    assert!(result.visited_nodes.contains("S"));
    assert!(result.visited_nodes.contains("E"));
}

struct FailureRecorder {
    failed: RefCell<Vec<String>>,
}

impl ExecutionListener for FailureRecorder {
    fn on_task_failed(&self, e: &TaskEvent, d: &sutra_bpmn::SutraError) {
        self.failed
            .borrow_mut()
            .push(format!("{}:{}", e.task_name, d.code));
    }
}

#[tokio::test]
async fn task_failure_emits_on_task_failed_with_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${kaboom}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("kaboom", |_, _| {
        Err(sutra_executor::TaskError::Failed(
            "planned failure".to_string(),
        ))
    });
    let recorder = Rc::new(FailureRecorder {
        failed: RefCell::new(Vec::new()),
    });
    let executor = TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();
    executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert_eq!(
        recorder.failed.borrow().as_slice(),
        &["kaboom:SUTRA.RUNTIME.TASK.UNCAUGHT"]
    );
}

// ---- a channel-call task is a WAIT STATE (Rust-only) ----------------------------------

/// A channel-call process is not sync-eligible: it PARKS (the stateful surface), so
/// `execute_sync` refuses it up front — replacing the earlier typed-NotYetImplemented posture.
#[tokio::test]
async fn channel_call_task_makes_the_process_stateful() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:responses-out">
              <bpmn:extensionElements>
                <q:timeout duration="PT30S"/>
                <q:alias name="callKey" expression="event.idempotencyKey"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(!process.is_sync_eligible());

    let executor = TokenExecutor::builder(TaskRegistry::new()).build();
    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    match e {
        ExecError::Diagnostic(d) => {
            assert_eq!(d.code, "SUTRA.RUNTIME.UNEXPECTED");
            assert!(d.message.contains("wait states"), "got: {}", d.message);
        }
        other => panic!("expected a wait-state rejection, got {other}"),
    }
}

// ---- suspend/resume + wait-free listener pins ----------------------------------------------

/// Records the instance-lifecycle callbacks the suspend/resume span-management contract
/// pins: `started` / `suspended` / `resumed` labels plus the `instance_id` each carried.
#[derive(Default)]
struct LifecycleRecorder {
    started: RefCell<Vec<String>>,
    suspended: RefCell<Vec<String>>,
    resumed: RefCell<Vec<String>>,
}

impl ExecutionListener for LifecycleRecorder {
    fn on_instance_started(&self, e: &InstanceEvent) {
        self.started.borrow_mut().push(e.instance_id.clone());
    }
    fn on_instance_suspended(&self, e: &InstanceEvent) {
        self.suspended.borrow_mut().push(e.instance_id.clone());
    }
    fn on_instance_resumed(&self, e: &InstanceEvent) {
        self.resumed.borrow_mut().push(e.instance_id.clone());
    }
}

/// A single userTask wait node — the simplest stateful (parking) process.
const WAIT_FLOW: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
      <bpmn:process id="p1">
        <bpmn:startEvent id="S"/>
        <bpmn:userTask id="U"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

/// The suspend park fires `on_instance_suspended` exactly once, carrying the instance id
/// (so a tracing layer can close the open span at the quiescent point).
#[tokio::test]
async fn suspend_notifies_listeners_with_the_instance_id() {
    let process = proc(WAIT_FLOW, "p1");
    let recorder = Rc::new(LifecycleRecorder::default());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    let result = executor
        .execute_stateful_from(
            &process,
            vars(&[]),
            DeploymentId::unresolved(),
            std::collections::BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let suspended_id = match &result {
        sutra_executor::StatefulExecResult::Suspended { instance_id, .. } => instance_id.clone(),
        other => panic!("expected Suspended, got {other:?}"),
    };

    assert_eq!(recorder.started.borrow().len(), 1, "started fires once");
    assert_eq!(
        recorder.suspended.borrow().as_slice(),
        &[suspended_id],
        "suspended fires exactly once with the instance id"
    );
    assert!(recorder.resumed.borrow().is_empty(), "no resume yet");
}

/// `resume` fires `on_instance_resumed` (the continuation marker) — NOT a second
/// `on_instance_started` — and the resumed id matches the suspended id.
#[tokio::test]
async fn resume_notifies_on_instance_resumed_not_a_second_started() {
    let process = proc(WAIT_FLOW, "p1");
    let recorder = Rc::new(LifecycleRecorder::default());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    let suspended = executor
        .execute_stateful_from(
            &process,
            vars(&[]),
            DeploymentId::unresolved(),
            std::collections::BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let instance_id = suspended.instance_id().to_string();
    assert_eq!(recorder.started.borrow().len(), 1);
    assert!(recorder.resumed.borrow().is_empty());

    let resumed = executor
        .resume(
            &process,
            &instance_id,
            &["S".to_string()],
            vars(&[]),
            "U",
            &vars(&[]),
            DeploymentId::unresolved(),
            std::collections::BTreeMap::new(),
            None,
            &["U".to_string()],
            &std::collections::BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(
        resumed.is_completed(),
        "resume runs the instance to completion"
    );

    assert_eq!(
        recorder.started.borrow().len(),
        1,
        "resume does NOT re-fire on_instance_started"
    );
    assert_eq!(
        recorder.resumed.borrow().as_slice(),
        &[instance_id],
        "resumed fires once with the same instance id"
    );
}

/// A wait-free process driven through the STATEFUL surface completes without ever firing
/// `on_instance_suspended`.
#[tokio::test]
async fn wait_free_stateful_completion_never_notifies_suspended() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${noop}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let recorder = Rc::new(LifecycleRecorder::default());
    let registry = TaskRegistry::new().register("noop", |_, _| ok_map(&[]));
    let executor = TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    let result = executor
        .execute_stateful_from(
            &process,
            vars(&[]),
            DeploymentId::unresolved(),
            std::collections::BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(result.is_completed(), "a wait-free stateful pass completes");
    assert!(
        recorder.suspended.borrow().is_empty(),
        "a wait-free completion never notifies suspended"
    );
    assert_eq!(recorder.started.borrow().len(), 1);
}

/// A resume with a blank satisfied-wait-node id is rejected fail-closed
/// (SUTRA.RUNTIME.UNEXPECTED) BEFORE any `on_instance_resumed` fires.
#[tokio::test]
async fn resume_rejects_a_blank_wait_node() {
    let process = proc(WAIT_FLOW, "p1");
    let recorder = Rc::new(LifecycleRecorder::default());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    let e = executor
        .resume(
            &process,
            "11111111-2222-4333-8444-555555555555",
            &["S".to_string()],
            vars(&[]),
            "  ",
            &vars(&[]),
            DeploymentId::unresolved(),
            std::collections::BTreeMap::new(),
            None,
            &["U".to_string()],
            &std::collections::BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.UNEXPECTED");
    assert!(d.message.contains("wait node"), "got: {}", d.message);
    assert!(
        recorder.resumed.borrow().is_empty(),
        "the blank-node check runs before on_instance_resumed"
    );
}
