//! Loops through wait states, end to end through the dispatcher: every iteration's wait is a
//! NEW wait with its own correlation key, a resolved iteration's key stops correlating, and a
//! relay for a wait the instance already left is refused before anything runs.
//!
//! Driven over [`LoopBridge`], an in-memory double that keeps one instance's snapshot and a
//! live/retired alias table the way the durable bridge does, so correlation across turns is
//! observed rather than stubbed.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, Namespace, OutboxEmission,
    SuspendedInstance, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::{
    DeploymentId, OutboundChannelRegistry, ResolvedOutboundChannel, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "loops", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000d1").expect("valid deployment id")
}

/// S → Prompt (sets the next turn's key) → W (message catch keyed by that turn) → G: `turn < 3`
/// back to Prompt, else E. The wait's alias resolves from the instance's `turnKey` at park
/// time and from the relay's body at correlation time.
const PER_TURN_KEY: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Prompt" implementation="${prompt}"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements>
        <q:source channel="turn-in" name="answer"/>
        <q:alias name="turnKey" expression="if turnKey != null then turnKey else event.body"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prompt"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prompt" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Prompt">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// The same loop keyed by ONE value for the whole conversation (the start payload): every
/// turn's wait answers to the same key.
const SAME_KEY: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Prompt" implementation="${prompt}"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements>
        <q:source channel="turn-in" name="answer"/>
        <q:alias name="conv" expression="event.body"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prompt"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prompt" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Prompt">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// S → Prompt → Call (async channel-call keyed by the iteration's `callKey`) → G: `turn < 3`
/// back to Prompt, else E.
const CALL_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Prompt" implementation="${prompt}"/>
    <bpmn:serviceTask id="Call" implementation="channel:out">
      <bpmn:extensionElements>
        <q:source channel="turn-in" name="answer"/>
        <q:timeout duration="PT30S"/>
        <q:alias name="turnKey" expression="if turnKey != null then turnKey else event.body"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prompt"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prompt" targetRef="Call"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Call" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Prompt">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// S (business key) → A (wait) → B (wait) → E: two different waits on one key, so a relay can
/// arrive for the one the instance already left.
const TWO_WAITS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="two">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="A">
      <bpmn:extensionElements><q:source channel="turn-in" name="a"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="B">
      <bpmn:extensionElements><q:source channel="other-in" name="b"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="B"/>
    <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

// ---- an in-memory bridge with a real alias table ---------------------------------------------

#[derive(Default)]
struct Store {
    instance: Option<String>,
    snapshot: Option<SuspendedInstance>,
    /// `(name, value, live)` — the alias index of the one instance.
    aliases: Vec<(String, String, bool)>,
    /// The outbox rows every step enqueued.
    emissions: Vec<OutboxEmission>,
    /// The waits each re-park step resolved.
    resolved: Vec<Vec<String>>,
    completed: bool,
}

#[derive(Default)]
struct LoopBridge(RefCell<Store>);

impl LoopBridge {
    fn record(&self, aliases: &[AliasRecord]) {
        let mut store = self.0.borrow_mut();
        for a in aliases {
            match store
                .aliases
                .iter_mut()
                .find(|(n, v, _)| n == &a.name && v == &a.value)
            {
                Some(row) => row.2 = true,
                None => store.aliases.push((a.name.clone(), a.value.clone(), true)),
            }
        }
    }

    fn live(&self) -> Vec<(String, String)> {
        let mut live: Vec<(String, String)> = self
            .0
            .borrow()
            .aliases
            .iter()
            .filter(|(_, _, l)| *l)
            .map(|(n, v, _)| (n.clone(), v.clone()))
            .collect();
        live.sort();
        live
    }

    fn waiting(&self) -> Vec<String> {
        self.0
            .borrow()
            .snapshot
            .as_ref()
            .map(|s| s.waiting_nodes.clone())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for LoopBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        {
            let mut store = self.0.borrow_mut();
            store.instance = Some(instance_id.to_string());
            store.snapshot = Some(snapshot.clone());
            store.emissions.extend(emissions.iter().cloned());
        }
        self.record(aliases);
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self.0.borrow().snapshot.clone())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let store = self.0.borrow();
        let live = store
            .aliases
            .iter()
            .any(|(n, v, l)| *l && n == name && v == value);
        Ok(if live { store.instance.clone() } else { None })
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut store = self.0.borrow_mut();
        store.completed = true;
        store.snapshot = None;
        store.emissions.extend(emissions.iter().cloned());
        for row in &mut store.aliases {
            row.2 = false;
        }
        Ok(())
    }

    async fn commit_repark(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.commit_repark_retiring(
            deployment,
            instance_id,
            snapshot,
            satisfied_wait_nodes,
            aliases,
            timer_waits,
            emissions,
            &[],
        )
        .await
    }

    async fn commit_repark_retiring(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
        retired_aliases: &[AliasRecord],
    ) -> Result<(), Diagnostic> {
        {
            let mut store = self.0.borrow_mut();
            for r in retired_aliases {
                for row in &mut store.aliases {
                    if row.0 == r.name && row.1 == r.value {
                        row.2 = false;
                    }
                }
            }
            store.snapshot = Some(snapshot.clone());
            store.emissions.extend(emissions.iter().cloned());
            store.resolved.push(satisfied_wait_nodes.to_vec());
        }
        self.record(aliases);
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
        _instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic> {
        panic!("no step in these tests may fail: {failure_code} {detail}");
    }
}

// ---- the engine ------------------------------------------------------------------------------

struct Rig {
    engine: ChannelEngine,
    bridge: Rc<LoopBridge>,
    prompts: Rc<Cell<i64>>,
}

fn rig(bpmn: &str) -> Rig {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let prompts = Rc::new(Cell::new(0));
    let count = Rc::clone(&prompts);
    let tasks = TaskRegistry::new().register("prompt", move |_, _| {
        count.set(count.get() + 1);
        let turn = count.get();
        Ok(FeelValue::Map(BTreeMap::from([
            ("turn".to_string(), FeelValue::from(turn)),
            (
                "turnKey".to_string(),
                FeelValue::String(format!("T-{turn}")),
            ),
        ])))
    });
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
        &deployment(),
        ResolvedOutboundChannel::resolve(
            "out",
            "http",
            "http://svc.example/op",
            None,
            None,
            None,
            "none",
        ),
    );
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_outbound_channels(outbound)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let bridge = Rc::new(LoopBridge::default());
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in ["start-in", "turn-in", "other-in"] {
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
    Rig {
        engine,
        bridge,
        prompts,
    }
}

fn inbound(channel: &str, body: &str, key: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: key.to_string(),
        explicit_event_id: true,
        received_at: "2026-10-25T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn send(rig: &Rig, channel: &str, body: &str, key: &str) -> Result<DispatchOutcome, Diagnostic> {
    drive(rig.engine.dispatch(&inbound(channel, body, key)))
}

fn pair(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

// ---- the tests -------------------------------------------------------------------------------

#[test]
fn a_message_catch_loop_with_a_new_key_each_turn_correlates_every_turn() {
    let rig = rig(PER_TURN_KEY);
    send(&rig, "start-in", "hello", "s-1").expect("the instance parks");
    assert_eq!(rig.bridge.waiting(), ["W"]);
    assert_eq!(rig.bridge.live(), [pair("turnKey", "T-1")]);

    send(&rig, "turn-in", "T-1", "r-1").expect("turn 1 resumes");
    assert_eq!(rig.bridge.waiting(), ["W"], "the loop parks at W again");
    assert_eq!(rig.prompts.get(), 2);
    assert_eq!(
        rig.bridge.live(),
        [pair("turnKey", "T-2")],
        "turn 2's wait is keyed by its own value; turn 1's key is retired"
    );
    assert!(
        rig.bridge.0.borrow().resolved[0].contains(&"W".to_string()),
        "the answered wait row is resolved in the same step"
    );

    send(&rig, "turn-in", "T-2", "r-2").expect("turn 2 resumes");
    assert_eq!(rig.bridge.live(), [pair("turnKey", "T-3")]);
    send(&rig, "turn-in", "T-3", "r-3").expect("turn 3 resumes and leaves the loop");
    assert!(rig.bridge.0.borrow().completed);
    assert_eq!(rig.prompts.get(), 3);
}

#[test]
fn a_late_relay_for_a_past_turn_is_refused_and_moves_nothing() {
    let rig = rig(PER_TURN_KEY);
    send(&rig, "start-in", "hello", "s-1").expect("the instance parks");
    send(&rig, "turn-in", "T-1", "r-1").expect("turn 1 resumes");

    // A second answer to turn 1 — a retry, a duplicate, a slow sender — after turn 2 parked.
    let d = send(&rig, "turn-in", "T-1", "r-1-late").expect_err("a past turn is refused");
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND", "{d:?}");
    assert_eq!(rig.prompts.get(), 2, "nothing ran");
    assert_eq!(rig.bridge.waiting(), ["W"]);
    assert_eq!(rig.bridge.live(), [pair("turnKey", "T-2")]);

    // The current turn still resumes normally.
    send(&rig, "turn-in", "T-2", "r-2").expect("turn 2 resumes");
    assert_eq!(rig.prompts.get(), 3);
}

#[test]
fn a_message_catch_loop_keyed_by_one_value_correlates_every_turn() {
    let rig = rig(SAME_KEY);
    send(&rig, "start-in", "C-9", "s-1").expect("the instance parks");
    assert_eq!(rig.bridge.live(), [pair("conv", "C-9")]);
    for turn in 2..=3 {
        send(&rig, "turn-in", "C-9", &format!("r-{turn}")).expect("the turn resumes");
        assert_eq!(rig.bridge.waiting(), ["W"]);
        assert_eq!(rig.prompts.get(), turn);
        assert_eq!(
            rig.bridge.live(),
            [pair("conv", "C-9")],
            "the key the new turn parks on stays live"
        );
    }
    send(&rig, "turn-in", "C-9", "r-4").expect("the last turn completes");
    assert!(rig.bridge.0.borrow().completed);
}

#[test]
fn an_async_channel_call_loop_sends_and_keys_each_iteration_separately() {
    let rig = rig(CALL_LOOP);
    send(&rig, "start-in", "go", "s-1").expect("the instance parks on its first call");
    assert_eq!(rig.bridge.0.borrow().emissions.len(), 1);
    assert_eq!(rig.bridge.live(), [pair("turnKey", "T-1")]);

    send(&rig, "turn-in", "T-1", "r-1").expect("the first answer resumes");
    assert_eq!(rig.bridge.waiting(), ["Call"]);
    assert_eq!(
        rig.bridge.0.borrow().emissions.len(),
        2,
        "the second iteration sent its own request"
    );
    assert_eq!(rig.bridge.live(), [pair("turnKey", "T-2")]);

    let d = send(&rig, "turn-in", "T-1", "r-1-late").expect_err("the first answer again");
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");

    send(&rig, "turn-in", "T-2", "r-2").expect("the second answer resumes");
    send(&rig, "turn-in", "T-3", "r-3").expect("the third answer completes");
    assert!(rig.bridge.0.borrow().completed);
    assert_eq!(rig.bridge.0.borrow().emissions.len(), 3);
}

#[test]
fn a_relay_for_a_wait_the_instance_already_left_is_refused() {
    let rig = rig(TWO_WAITS);
    send(&rig, "start-in", "K-1", "s-1").expect("parks at A");
    send(&rig, "turn-in", "K-1", "r-1").expect("A resumes; the instance parks at B");
    assert_eq!(rig.bridge.waiting(), ["B"]);

    let d = send(&rig, "turn-in", "K-1", "r-1-dup").expect_err("A is no longer waiting");
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.NOT_WAITING", "{d:?}");
    assert_eq!(rig.bridge.waiting(), ["B"], "the instance did not move");

    send(&rig, "other-in", "K-1", "r-2").expect("B resumes");
    assert!(rig.bridge.0.borrow().completed);
}
