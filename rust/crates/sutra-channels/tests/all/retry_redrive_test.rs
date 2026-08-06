//! The dispatcher half of `<q:retry>` (P1-1): a due backoff timer must RE-DRIVE the failed task.
//!
//! The executor tests prove the retry park's shape; what can only be proven here is the routing
//! decision `fire_timer` makes. A retry park's timer row names a `serviceTask`, not a timer node,
//! so the ordinary `resume_timer` call would reject it outright ("not a timer boundary or an
//! intermediate timer catch"). The dispatcher therefore has to recognise a retry backoff from the
//! snapshot alone — a durable attempt count for the node, and that node absent from
//! `completed_nodes` — and route it down the relay resume path instead.
//!
//! Driven through the same controllable in-memory [`InstanceBridge`] double the relay negatives
//! use, so the assertions are about the dispatcher's decisions and the step it commits, not about
//! any database.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DrainingSink,
    InboundChain, InstanceBridge, Namespace, OutboxEmission, SuspendedInstance, TimerWaitRecord,
    ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskError, TaskRegistry, TimerFire, TokenExecutor};

use crate::support::drive;

const TENANT: &str = "acme";
const INSTANCE: &str = "11111111-1111-4111-8111-111111111111";
const NOW: &str = "2026-08-05T10:00:00Z";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "billing", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000092").expect("valid deployment id")
}

/// start(`start-in`) → serviceTask `T` (`flaky`, retried 3×) → serviceTask `T2` (`tail`) → end.
const RETRY_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="charge">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="flaky">
      <bpmn:extensionElements>
        <q:retry maxAttempts="3" initialDelay="PT10S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="T2" implementation="tail"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="T2"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T2" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// One observed re-park step: the snapshot the dispatcher would have persisted, the wait nodes
/// it resolved on the way through, and the fresh timer rows it armed.
struct ObservedRepark {
    snapshot: SuspendedInstance,
    satisfied_wait_nodes: Vec<String>,
    timer_waits: Vec<TimerWaitRecord>,
}

/// The in-memory bridge double: records the re-park step's snapshot + timer rows, so a test can
/// read back exactly what the dispatcher would have persisted.
#[derive(Default)]
struct FakeBridge {
    loaded: Option<SuspendedInstance>,
    reparks: RefCell<Vec<ObservedRepark>>,
    /// Set when the instance reached its terminal step.
    completed: RefCell<bool>,
    /// `(instance_id, failure_code, detail)` per `commit_failed`.
    failed_commits: RefCell<Vec<(String, String, String)>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for FakeBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self.loaded.clone())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        _name: &str,
        _value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        Ok(None)
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.completed.borrow_mut() = true;
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.reparks.borrow_mut().push(ObservedRepark {
            snapshot: snapshot.clone(),
            satisfied_wait_nodes: satisfied_wait_nodes.to_vec(),
            timer_waits: timer_waits.to_vec(),
        });
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic> {
        self.failed_commits.borrow_mut().push((
            instance_id.to_string(),
            failure_code.to_string(),
            detail.to_string(),
        ));
        Ok(())
    }
}

/// An engine whose `flaky` task fails for the first `fail_times` invocations, then succeeds;
/// `calls` counts real invocations.
fn engine_with(bridge: Rc<FakeBridge>, fail_times: usize) -> (ChannelEngine, Rc<RefCell<usize>>) {
    let module = BpmnModelLoader::new()
        .load(RETRY_BPMN.as_bytes())
        .expect("BPMN loads");
    let calls = Rc::new(RefCell::new(0usize));
    let counter = Rc::clone(&calls);
    let registry = TaskRegistry::new()
        .register("flaky", move |_, _| {
            let mut n = counter.borrow_mut();
            *n += 1;
            if *n <= fail_times {
                Err(TaskError::Failed("gateway timeout".to_string()))
            } else {
                Ok(sutra_feel::FeelValue::Map(BTreeMap::from([(
                    "charged".to_string(),
                    sutra_feel::FeelValue::Boolean(true),
                )])))
            }
        })
        .register("tail", |_, _| {
            Ok(sutra_feel::FeelValue::Map(BTreeMap::from([(
                "tailRan".to_string(),
                sutra_feel::FeelValue::Boolean(true),
            )])))
        });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(registry)
        .with_feel()
        .with_now_supplier(|| NOW.to_string())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "start-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
    .build();
    (engine, calls)
}

/// A snapshot parked on `T`'s retry backoff after `attempts` failed attempts — exactly what the
/// executor's retry park produces and the bridge persists. Note `T` is deliberately NOT in
/// `completed_nodes`: that omission is the re-drive discriminator.
fn parked_on_retry(attempts: u32) -> SuspendedInstance {
    SuspendedInstance {
        process_id: "charge".to_string(),
        deployment_id: deployment().value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        waiting_nodes: vec!["T".to_string()],
        retry_attempts: BTreeMap::from([("T".to_string(), attempts)]),
        ..Default::default()
    }
}

fn fire() -> TimerFire {
    TimerFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: "T".to_string(),
        due_at: "2026-08-05T09:59:50Z".to_string(),
        fired_at: NOW.to_string(),
    }
}

// ============================ the re-drive routing ======================================

#[test]
fn a_due_retry_backoff_re_executes_the_task_rather_than_replaying_past_it() {
    // The task succeeds on this second invocation, so the instance runs the tail and completes.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_retry(1)),
        ..Default::default()
    });
    let (engine, calls) = engine_with(Rc::clone(&bridge), 0);

    let outcome = drive(engine.fire_timer(&fire()))
        .expect("a retry backoff is a valid timer fire, not a rejected non-timer node");

    match outcome {
        sutra_channels::TimerFireOutcome::Resumed { completed, .. } => {
            assert!(
                completed,
                "the retried task succeeded and the flow finished"
            );
        }
        other => panic!("expected the retry to resume the instance, got {other:?}"),
    }
    assert_eq!(
        *calls.borrow(),
        1,
        "the parked task ran again on the re-drive"
    );
    assert!(*bridge.completed.borrow(), "the terminal step committed");
}

#[test]
fn a_re_drive_that_fails_again_re_parks_with_the_advanced_attempt_count() {
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_retry(1)),
        ..Default::default()
    });
    let (engine, calls) = engine_with(Rc::clone(&bridge), usize::MAX);

    drive(engine.fire_timer(&fire())).expect("the re-drive parks again");

    assert_eq!(*calls.borrow(), 1);
    let reparks = bridge.reparks.borrow();
    assert_eq!(reparks.len(), 1, "exactly one re-park step");
    let observed = &reparks[0];

    // The advanced count is what the next re-drive is seeded from.
    assert_eq!(observed.snapshot.retry_attempts.get("T"), Some(&2));
    // Still parked on the same node, still not completed.
    assert_eq!(observed.snapshot.waiting_nodes, vec!["T".to_string()]);
    assert!(!observed.snapshot.completed_nodes.contains(&"T".to_string()));
    // The fired row is resolved and a fresh one armed — initialDelay PT10S doubled for attempt 2.
    assert_eq!(observed.satisfied_wait_nodes, vec!["T".to_string()]);
    assert_eq!(observed.timer_waits.len(), 1);
    assert_eq!(observed.timer_waits[0].node_id, "T");
    assert_eq!(observed.timer_waits[0].due_at, "2026-08-05T10:00:20Z");
}

#[test]
fn exhausting_the_budget_on_a_re_drive_persists_durable_failed_state() {
    // Attempt 3 of 3 fails: no fourth park, and the Wave A FAILED marker records the retry
    // verdict — `RETRY.EXHAUSTED`, not the bare task error — so an operator can tell that adding
    // attempts, not fixing this one error, was the lever.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_retry(2)),
        ..Default::default()
    });
    let (engine, calls) = engine_with(Rc::clone(&bridge), usize::MAX);

    let d = drive(engine.fire_timer(&fire()))
        .expect_err("the budget is spent — the fire surfaces the failure");

    assert_eq!(*calls.borrow(), 1);
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(
        bridge.reparks.borrow().is_empty(),
        "an exhausted retry must not park a fourth timer"
    );
    let failed = bridge.failed_commits.borrow();
    assert_eq!(failed.len(), 1, "exactly one durable FAILED commit");
    assert_eq!(failed[0].0, INSTANCE);
    assert_eq!(failed[0].1, "SUTRA.RUNTIME.RETRY.EXHAUSTED");
    assert!(
        failed[0].2.contains("gateway timeout"),
        "the underlying cause is carried into the durable record: {}",
        failed[0].2
    );
}

#[test]
fn a_timer_fire_for_a_node_no_longer_on_the_frontier_is_stale() {
    // The instance moved on (the task succeeded on an earlier re-drive and the frontier advanced),
    // so a straggling backoff row must be resolved as stale, never re-executed.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(SuspendedInstance {
            completed_nodes: vec!["S".to_string(), "T".to_string()],
            waiting_nodes: vec!["T2".to_string()],
            retry_attempts: BTreeMap::new(),
            ..parked_on_retry(0)
        }),
        ..Default::default()
    });
    let (engine, calls) = engine_with(Rc::clone(&bridge), usize::MAX);

    let outcome = drive(engine.fire_timer(&fire())).expect("a stale row is not an error");

    assert!(
        matches!(outcome, sutra_channels::TimerFireOutcome::Stale),
        "expected Stale, got {outcome:?}"
    );
    assert_eq!(*calls.borrow(), 0, "nothing was re-executed");
}
