//! A failed step's emissions never outlive the step.
//!
//! The emission sink is per lane and outlives every step on it. A process that performs a
//! `<q:send>` and then fails must drop that send: it never reached a quiescent point, so it is
//! never committed. It used to stay in the sink when a wait-state start failed. The NEXT request
//! on the lane then drained it as its own and committed it with its own step: the durable store
//! refuses an outbox row stamped with another instance, so an unrelated caller got a failure
//! (and a store without that check would have delivered the failed step's send).
//!
//! Each test runs process A (send, then a failing task) and then an unrelated request on the
//! same engine. The second request must succeed, and nothing of A's may ever be committed.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, Namespace, OutboxEmission,
    SuspendedInstance, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskError, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";
/// What the durable store answers when a step commits another instance's outbox row.
const FOREIGN_ROW: &str = "TEST.OUTBOX.FOREIGN_ROW";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "lanes", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000f1").expect("valid deployment id")
}

/// `failing` sends and then fails; `parking` sends and parks; `straight` sends and ends.
/// `failing` is a wait-state process (its receive task is never reached) unless `sync` asks
/// for the run-to-end shape, so both start paths are covered.
fn definitions(sync: bool) -> String {
    let failing_tail = if sync {
        r#"<bpmn:endEvent id="AE"/>
    <bpmn:sequenceFlow id="a3" sourceRef="ABoom" targetRef="AE"/>"#
    } else {
        r#"<bpmn:receiveTask id="AWait">
      <bpmn:extensionElements><q:source channel="a-turn" name="turn"/></bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="AE"/>
    <bpmn:sequenceFlow id="a3" sourceRef="ABoom" targetRef="AWait"/>
    <bpmn:sequenceFlow id="a4" sourceRef="AWait" targetRef="AE"/>"#
    };
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="failing">
    <bpmn:startEvent id="AS">
      <bpmn:extensionElements><q:source channel="a-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sendTask id="ASend">
      <bpmn:extensionElements><q:send destination="notify://ops/from-a"/></bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:serviceTask id="ABoom" implementation="boom"/>
    {failing_tail}
    <bpmn:sequenceFlow id="a1" sourceRef="AS" targetRef="ASend"/>
    <bpmn:sequenceFlow id="a2" sourceRef="ASend" targetRef="ABoom"/>
  </bpmn:process>
  <bpmn:process id="parking">
    <bpmn:startEvent id="BS">
      <bpmn:extensionElements><q:source channel="b-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sendTask id="BSend">
      <bpmn:extensionElements><q:send destination="notify://ops/from-b"/></bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:receiveTask id="BWait">
      <bpmn:extensionElements><q:source channel="b-turn" name="turn"/></bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="BE"/>
    <bpmn:sequenceFlow id="b1" sourceRef="BS" targetRef="BSend"/>
    <bpmn:sequenceFlow id="b2" sourceRef="BSend" targetRef="BWait"/>
    <bpmn:sequenceFlow id="b3" sourceRef="BWait" targetRef="BE"/>
  </bpmn:process>
  <bpmn:process id="straight">
    <bpmn:startEvent id="CS">
      <bpmn:extensionElements><q:source channel="c-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sendTask id="CSend">
      <bpmn:extensionElements><q:send destination="notify://ops/from-c"/></bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:endEvent id="CE"/>
    <bpmn:sequenceFlow id="c1" sourceRef="CS" targetRef="CSend"/>
    <bpmn:sequenceFlow id="c2" sourceRef="CSend" targetRef="CE"/>
  </bpmn:process>
</bpmn:definitions>"#
    )
}

/// Keeps every committed outbox row, and refuses a step whose rows belong to another instance —
/// the check the durable store makes.
#[derive(Default)]
struct Bridge {
    committed: RefCell<Vec<OutboxEmission>>,
    parked: RefCell<Vec<String>>,
}

impl Bridge {
    fn accept(&self, instance_id: Option<&str>, rows: &[OutboxEmission]) -> Result<(), Diagnostic> {
        if let Some(owner) = instance_id {
            if let Some(foreign) = rows.iter().find(|r| r.instance_id != owner) {
                return Err(Diagnostic::error(
                    FOREIGN_ROW,
                    format!(
                        "outbox entry for node '{}' belongs to instance {}, not to step \
                         instance {owner}",
                        foreign.node_id, foreign.instance_id
                    ),
                ));
            }
        }
        self.committed.borrow_mut().extend(rows.iter().cloned());
        Ok(())
    }

    fn committed_nodes(&self) -> Vec<String> {
        self.committed
            .borrow()
            .iter()
            .map(|r| r.node_id.clone())
            .collect()
    }
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for Bridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.accept(Some(instance_id), emissions)?;
        self.parked.borrow_mut().push(instance_id.to_string());
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(None)
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
        instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.accept(Some(instance_id), emissions)
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.accept(Some(instance_id), emissions)
    }

    async fn commit_repark_retiring(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
        _retired_aliases: &[AliasRecord],
    ) -> Result<(), Diagnostic> {
        self.accept(Some(instance_id), emissions)
    }

    /// The run-to-end terminal step: every row must belong to ONE instance.
    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let owner = emissions.first().map(|r| r.instance_id.clone());
        self.accept(owner.as_deref(), emissions)
    }

    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
}

fn rig(sync: bool) -> (ChannelEngine, Rc<Bridge>) {
    let module = BpmnModelLoader::new()
        .load(definitions(sync).as_bytes())
        .expect("BPMN loads");
    let tasks = TaskRegistry::new().register("boom", |_, _| -> Result<FeelValue, TaskError> {
        Err(TaskError::Failed("downstream refused".to_string()))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let bridge = Rc::new(Bridge::default());
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in ["a-in", "a-turn", "b-in", "b-turn", "c-in"] {
        builder = builder.with_binding(ChannelBinding::new(
            channel,
            namespace(),
            deployment(),
            "raw-text",
        ));
    }
    let engine = builder
        .with_module(&deployment(), &module)
        .with_instance_bridge(Rc::clone(&bridge) as Rc<dyn InstanceBridge>)
        .build();
    (engine, bridge)
}

fn inbound(channel: &str, key: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: b"hello".to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: key.to_string(),
        explicit_event_id: true,
        received_at: "2026-10-30T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

/// A's failure, whether surfaced as an error or as the at-most-once dead letter a
/// non-idempotent process's failure becomes.
fn assert_failed(result: Result<DispatchOutcome, Diagnostic>) {
    match result {
        Err(_) | Ok(DispatchOutcome::DeadLettered { .. }) => {}
        Ok(other) => panic!("process A must fail, got {other:?}"),
    }
}

#[test]
fn a_failed_wait_state_start_leaves_no_send_for_the_next_parking_request() {
    let (engine, bridge) = rig(false);
    assert_failed(drive(engine.dispatch(&inbound("a-in", "a-1"))));
    assert!(
        bridge.committed.borrow().is_empty(),
        "A's send is never committed"
    );

    let next = drive(engine.dispatch(&inbound("b-in", "b-1")));
    assert!(
        matches!(next, Ok(DispatchOutcome::Completed { .. })),
        "the unrelated request parks cleanly: {next:?}"
    );
    assert_eq!(bridge.parked.borrow().len(), 1);
    assert_eq!(
        bridge.committed_nodes(),
        ["BSend"],
        "only B's own send is committed"
    );
}

#[test]
fn a_failed_wait_state_start_leaves_no_send_for_the_next_run_to_end_request() {
    let (engine, bridge) = rig(false);
    assert_failed(drive(engine.dispatch(&inbound("a-in", "a-1"))));

    let next = drive(engine.dispatch(&inbound("c-in", "c-1")));
    assert!(
        matches!(next, Ok(DispatchOutcome::Completed { .. })),
        "the unrelated request completes: {next:?}"
    );
    assert_eq!(
        bridge.committed_nodes(),
        ["CSend"],
        "only C's own send is committed"
    );
}

#[test]
fn a_failed_run_to_end_start_leaves_no_send_for_the_next_request() {
    let (engine, bridge) = rig(true);
    assert_failed(drive(engine.dispatch(&inbound("a-in", "a-1"))));

    let next = drive(engine.dispatch(&inbound("b-in", "b-1")));
    assert!(
        matches!(next, Ok(DispatchOutcome::Completed { .. })),
        "the unrelated request parks cleanly: {next:?}"
    );
    assert_eq!(
        bridge.committed_nodes(),
        ["BSend"],
        "only B's own send is committed"
    );
}

#[test]
fn repeated_failures_never_accumulate_sends_on_the_lane() {
    let (engine, bridge) = rig(false);
    for n in 0..3 {
        assert_failed(drive(engine.dispatch(&inbound("a-in", &format!("a-{n}")))));
    }
    let next = drive(engine.dispatch(&inbound("b-in", "b-1")));
    assert!(
        matches!(next, Ok(DispatchOutcome::Completed { .. })),
        "{next:?}"
    );
    assert_eq!(bridge.committed_nodes(), ["BSend"]);
}
