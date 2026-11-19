//! A message a wait subscribes to that correlates to no live instance: refused as before (with
//! `sutra.waitPending` only while an instance may still reach the wait), or, on a channel with
//! `uncorrelated: { start: <processId> }`, the start of that process with `event.uncorrelated`
//! set. A message that does correlate still resumes its instance and starts nothing.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, InstanceClaimOutcome, Namespace,
    OutboxEmission, SuspendedInstance, TimerWaitRecord, UncorrelatedRoute, ValidatorRegistry,
    WAIT_PENDING_ATTR,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000077").expect("valid deployment id")
}

/// `hold` starts on `start-in` (correlate alias e2eId = the body) and waits on `relay-in`;
/// `late` starts on `relay-in` and parks on `late-park`.
const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="payload"/>
        <q:alias name="e2eId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U" name="Approve">
      <bpmn:extensionElements>
        <q:source channel="relay-in" name="decision"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
  </bpmn:process>
  <bpmn:process id="late">
    <bpmn:startEvent id="LS">
      <bpmn:extensionElements>
        <q:source channel="relay-in" name="reply"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="LU" name="Look at it">
      <bpmn:extensionElements>
        <q:source channel="late-park" name="seen"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="LE"/>
    <bpmn:sequenceFlow id="l1" sourceRef="LS" targetRef="LU"/>
    <bpmn:sequenceFlow id="l2" sourceRef="LU" targetRef="LE"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// Only `hold`: no process starts on `relay-in`.
const HOLD_ONLY: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="payload"/>
        <q:alias name="e2eId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U" name="Approve">
      <bpmn:extensionElements>
        <q:source channel="relay-in" name="decision"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[derive(Default)]
struct Bridge {
    alias_owner: Option<String>,
    /// `alias_ended` answer.
    ended: bool,
    loaded: Option<SuspendedInstance>,
    parked: RefCell<Vec<SuspendedInstance>>,
    resumed: RefCell<bool>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for Bridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.parked.borrow_mut().push(snapshot.clone());
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
        Ok(self.alias_owner.clone())
    }

    async fn alias_ended(
        &self,
        _deployment: &DeploymentId,
        _name: &str,
        _value: &str,
    ) -> Result<bool, Diagnostic> {
        Ok(self.ended)
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.resumed.borrow_mut() = true;
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.resumed.borrow_mut() = true;
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    async fn claim_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        Ok(InstanceClaimOutcome::Granted)
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
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

fn engine(bpmn: &str, uncorrelated: Option<&str>, bridge: Rc<Bridge>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut relay = ChannelBinding::new("relay-in", namespace(), deployment(), "raw-text");
    relay.uncorrelated = uncorrelated.map(|start| UncorrelatedRoute {
        start: start.to_string(),
    });
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in ["start-in", "late-park"] {
        builder = builder.with_binding(ChannelBinding::new(
            channel,
            namespace(),
            deployment(),
            "raw-text",
        ));
    }
    builder
        .with_binding(relay)
        .with_module(&deployment(), &module)
        .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
        .build()
}

fn inbound(channel: &str, body: &[u8]) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: "msg-1".to_string(),
        explicit_event_id: false,
        received_at: "2026-11-19T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn parked_at_u() -> SuspendedInstance {
    SuspendedInstance {
        process_id: "hold".to_string(),
        deployment_id: deployment().value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        waiting_nodes: vec!["U".to_string()],
        start_node: "S".to_string(),
        ..Default::default()
    }
}

fn refusal(result: Result<DispatchOutcome, Diagnostic>) -> Diagnostic {
    match result {
        Err(d) => d,
        Ok(_) => panic!("expected a refusal"),
    }
}

#[test]
fn without_the_key_an_uncorrelated_relay_is_refused_and_may_still_be_wait_pending() {
    let bridge = Rc::new(Bridge::default());
    let engine = engine(HOLD_ONLY, None, Rc::clone(&bridge));
    let d = refusal(drive(engine.dispatch(&inbound("relay-in", b"K-1"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");
    assert_eq!(
        d.attributes.get(WAIT_PENDING_ATTR).map(String::as_str),
        Some("true"),
        "no record says the key's instance ended: it may not have parked yet: {d:?}"
    );
    assert!(bridge.parked.borrow().is_empty(), "nothing started");
}

#[test]
fn a_relay_for_an_instance_that_ended_is_not_wait_pending() {
    let bridge = Rc::new(Bridge {
        ended: true,
        ..Default::default()
    });
    let engine = engine(HOLD_ONLY, None, Rc::clone(&bridge));
    let d = refusal(drive(engine.dispatch(&inbound("relay-in", b"K-1"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");
    assert!(
        !d.attributes.contains_key(WAIT_PENDING_ATTR),
        "no instance waits for it: {d:?}"
    );
    assert!(d.message.contains("has ended"), "{d:?}");
}

#[test]
fn with_the_key_an_uncorrelated_relay_starts_the_named_process_marked_uncorrelated() {
    let bridge = Rc::new(Bridge {
        ended: true,
        ..Default::default()
    });
    let engine = engine(BPMN, Some("late"), Rc::clone(&bridge));
    let outcome = drive(engine.dispatch(&inbound("relay-in", b"K-7"))).expect("routed start");
    assert!(
        matches!(outcome, DispatchOutcome::Completed { ref instance_id, .. } if !instance_id.is_empty()),
        "{outcome:?}"
    );
    let parked = bridge.parked.borrow();
    assert_eq!(parked.len(), 1, "one instance of `late` parked");
    assert_eq!(parked[0].process_id, "late");
    let event = parked[0]
        .variables
        .iter()
        .find(|(name, _)| name == "event")
        .map(|(_, v)| v.clone())
        .expect("event persisted");
    let FeelValue::Map(event) = event else {
        panic!("event is a context: {event:?}")
    };
    let Some(FeelValue::Map(marker)) = event.get("uncorrelated") else {
        panic!("event.uncorrelated is set: {event:?}")
    };
    assert_eq!(
        marker.get("code"),
        Some(&FeelValue::String(
            "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND".to_string()
        ))
    );
    let Some(FeelValue::Map(keys)) = marker.get("keys") else {
        panic!("keys: {marker:?}")
    };
    assert_eq!(
        keys.get("e2eId"),
        Some(&FeelValue::String("K-7".to_string()))
    );
    assert_eq!(marker.get("ended"), Some(&FeelValue::Boolean(true)));
}

#[test]
fn with_the_key_a_correlated_relay_resumes_and_starts_nothing() {
    let bridge = Rc::new(Bridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_at_u()),
        ..Default::default()
    });
    let engine = engine(BPMN, Some("late"), Rc::clone(&bridge));
    drive(engine.dispatch(&inbound("relay-in", b"K-1"))).expect("resumes");
    assert!(*bridge.resumed.borrow(), "the waiting instance resumed");
    assert!(
        bridge.parked.borrow().is_empty(),
        "the reserved start did not run"
    );
}

#[test]
fn without_the_key_a_start_on_the_channel_is_an_ordinary_start() {
    // Control: the same package without `uncorrelated:` — `late`'s start is an ordinary
    // subscriber, so an uncorrelated message starts it unmarked (waits first, then starts).
    let bridge = Rc::new(Bridge::default());
    let engine = engine(BPMN, None, Rc::clone(&bridge));
    drive(engine.dispatch(&inbound("relay-in", b"K-2"))).expect("ordinary start");
    let parked = bridge.parked.borrow();
    assert_eq!(parked.len(), 1);
    let event = parked[0]
        .variables
        .iter()
        .find(|(name, _)| name == "event")
        .map(|(_, v)| v.clone());
    let Some(FeelValue::Map(event)) = event else {
        panic!("event persisted: {event:?}")
    };
    assert!(!event.contains_key("uncorrelated"), "{event:?}");
}
