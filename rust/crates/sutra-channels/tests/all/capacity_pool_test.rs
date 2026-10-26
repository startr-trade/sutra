//! Session slots and capacity pools, driven through the dispatcher with in-memory stores.
//!
//! A channel's `pool:` draws slots from an engine-declared capacity pool, one per active call: an
//! inbound start takes a slot for its call or is rejected busy, a park keeps it, and End, a
//! failure or the admitting channel's `session-end:` (a correlated message on its `on-message`
//! channel — naming one call when the channel declares `pool-call-key:` — or the flow's reply
//! with `on-reply: true`) gives it back. The same `session-end:` frees the channel's own
//! `max-concurrent-instances` slot. Requests bound for a pooled outbound channel are stamped with
//! the pool so the outbox can hold them until their call holds a slot.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, CapacityPool, CapacityPoolStore, ChannelBinding, ChannelEngine, CodecRegistry,
    Diagnostic, DispatchOutcome, DrainingSink, InMemoryAliasStore, InMemoryCapacityPools,
    InMemoryConcurrencyStore, InboundChain, InboundMessage, InstanceBridge, InstanceClaimOutcome,
    Namespace, OutboxEmission, SessionEnd, SlotAcquisition, SlotDirection, SlotRequest,
    SuspendedInstance, TimerWaitRecord, ValidatorRegistry, CAPACITY_CALL_HEADER,
    CAPACITY_POOL_HEADER,
};
use sutra_executor::{
    DeploymentId, OutboundChannelRegistry, ResolvedOutboundChannel, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";
const POOL: &str = "lines";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "sessions", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000d1").expect("valid deployment id")
}

/// A session: start on `call-in` (correlated by its body) → wait on `agent-in` → end.
const SESSION: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="session">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="call-in" name="payload"/>
        <q:alias name="callId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="agent-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// As [`SESSION`], but the flow itself also waits for the session-ending message.
const SESSION_AWAITS_END: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="session">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="call-in" name="payload"/>
        <q:alias name="callId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="H">
      <bpmn:extensionElements><q:source channel="hangup-in" name="bye"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="agent-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="H"/>
    <bpmn:sequenceFlow id="f2" sourceRef="H" targetRef="U"/>
    <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A session that answers its caller first (`<q:reply>`), then waits.
const SESSION_REPLIES: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="session">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="call-in" name="payload"/>
        <q:alias name="callId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="R" implementation="answer">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="agent-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="R"/>
    <bpmn:sequenceFlow id="f2" sourceRef="R" targetRef="U"/>
    <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A session whose resumed leg throws (an uncaught task error — fatal).
const SESSION_FAILS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="session">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="call-in" name="payload"/>
        <q:alias name="callId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="agent-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="T" implementation="boom"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A start whose first pass throws before any park.
const FAILS_AT_ONCE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="session">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="call-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="boom"/>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="agent-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="U"/>
    <bpmn:sequenceFlow id="f3" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A flow (started on an UNPOOLED channel) that sends on the pooled `dial-out`, then calls it.
const DIALS_OUT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="dialer">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="plain-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sendTask id="Notify">
      <bpmn:extensionElements><q:send channel="dial-out"/></bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:serviceTask id="Dial" implementation="channel:dial-out">
      <bpmn:extensionElements>
        <q:source channel="dial-answers" name="answer"/>
        <q:alias name="dialId" expression="event.dedupKey"/>
        <q:timeout duration="PT1M"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="Dial"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Dial" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

// ---- a working in-memory instance store ------------------------------------------------------

#[derive(Default)]
struct Stored {
    parked: HashMap<String, SuspendedInstance>,
    /// `(alias name, value, instance)`.
    aliases: Vec<(String, String, String)>,
    /// Every outbox row a park or commit wrote.
    emissions: Vec<OutboxEmission>,
    completed: Vec<String>,
    failed: Vec<String>,
}

#[derive(Default)]
struct MemBridge(RefCell<Stored>);

impl MemBridge {
    fn parked(&self) -> usize {
        self.0.borrow().parked.len()
    }
    fn completed(&self) -> Vec<String> {
        self.0.borrow().completed.clone()
    }
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for MemBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut stored = self.0.borrow_mut();
        stored
            .parked
            .insert(instance_id.to_string(), snapshot.clone());
        for alias in aliases {
            stored.aliases.push((
                alias.name.clone(),
                alias.value.clone(),
                instance_id.to_string(),
            ));
        }
        stored.emissions.extend(emissions.iter().cloned());
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self.0.borrow().parked.get(instance_id).cloned())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let stored = self.0.borrow();
        Ok(stored
            .aliases
            .iter()
            .find(|(n, v, id)| n == name && v == value && stored.parked.contains_key(id))
            .map(|(_, _, id)| id.clone()))
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut stored = self.0.borrow_mut();
        stored.parked.remove(instance_id);
        stored.completed.push(instance_id.to_string());
        stored.emissions.extend(emissions.iter().cloned());
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut stored = self.0.borrow_mut();
        stored
            .parked
            .insert(instance_id.to_string(), snapshot.clone());
        for alias in aliases {
            stored.aliases.push((
                alias.name.clone(),
                alias.value.clone(),
                instance_id.to_string(),
            ));
        }
        stored.emissions.extend(emissions.iter().cloned());
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.0
            .borrow_mut()
            .emissions
            .extend(emissions.iter().cloned());
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
        instance_id: &str,
        _failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        let mut stored = self.0.borrow_mut();
        if let Some(snapshot) = stored.parked.get_mut(instance_id) {
            snapshot.status = "FAILED".to_string();
        }
        stored.failed.push(instance_id.to_string());
        Ok(())
    }
}

// ---- the rig ---------------------------------------------------------------------------------

struct Rig {
    engine: ChannelEngine,
    bridge: Rc<MemBridge>,
    pools: Arc<InMemoryCapacityPools>,
    concurrency: Rc<InMemoryConcurrencyStore>,
}

/// `call-in` as the test configures it (pooled, capped, session-ending — per test).
fn call_in() -> ChannelBinding {
    ChannelBinding::new("call-in", namespace(), deployment(), "raw-text")
}

fn pooled(mut binding: ChannelBinding) -> ChannelBinding {
    binding.pool = Some(POOL.to_string());
    binding
}

fn ends_on_hangup(mut binding: ChannelBinding) -> ChannelBinding {
    binding.session_end = Some(SessionEnd {
        on_message: Some("hangup-in".to_string()),
        on_reply: false,
    });
    binding
}

fn rig(bpmn: &str, call_in: ChannelBinding, pools: Arc<InMemoryCapacityPools>) -> Rig {
    rig_with_bridge(bpmn, call_in, pools, Rc::new(MemBridge::default()))
}

fn rig_with_bridge(
    bpmn: &str,
    call_in: ChannelBinding,
    pools: Arc<InMemoryCapacityPools>,
    bridge: Rc<MemBridge>,
) -> Rig {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let tasks = TaskRegistry::new()
        .register("answer", |_input, _ctx| {
            Ok(FeelValue::Map(BTreeMap::from([(
                "responseBody".to_string(),
                FeelValue::String("hello".to_string()),
            )])))
        })
        .register("boom", |_input, _ctx| {
            Err(sutra_executor::TaskError::Failed(
                "synthetic failure".to_string(),
            ))
        });
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
        &deployment(),
        ResolvedOutboundChannel::resolve(
            "dial-out",
            "fake",
            "fake://dial/out",
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
    let binding = |name: &str| ChannelBinding::new(name, namespace(), deployment(), "raw-text");
    let concurrency = Rc::new(InMemoryConcurrencyStore::new());
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(call_in)
    .with_binding(binding("agent-in"))
    .with_binding(binding("hangup-in"))
    .with_binding(binding("plain-in"))
    .with_binding(binding("dial-answers"))
    .with_binding(pooled(binding("dial-out")))
    .with_module(&deployment(), &module)
    .with_instance_bridge(Rc::clone(&bridge) as Rc<dyn InstanceBridge>)
    .with_alias_store(Rc::new(InMemoryAliasStore::new()))
    .with_concurrency_store(Rc::clone(&concurrency) as Rc<dyn sutra_channels::ConcurrencyStore>)
    .with_capacity_pools(Arc::clone(&pools) as Arc<dyn CapacityPoolStore>)
    .build();
    Rig {
        engine,
        bridge,
        pools,
        concurrency,
    }
}

fn pool_of(slots: u32) -> Arc<InMemoryCapacityPools> {
    Arc::new(InMemoryCapacityPools::new([CapacityPool::new(POOL, slots)]))
}

fn message(channel: &str, body: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: format!("{channel}-{body}"),
        explicit_event_id: false,
        received_at: "2026-10-24T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn instance_of(outcome: &DispatchOutcome) -> String {
    match outcome {
        DispatchOutcome::Completed { instance_id, .. } => instance_id.clone(),
        other => panic!("expected a completed dispatch, got {other:?}"),
    }
}

fn usage(rig: &Rig) -> (u32, u32) {
    drive(rig.pools.usage(POOL))
}

fn channel_count(rig: &Rig) -> u64 {
    drive(sutra_channels::ConcurrencyStore::count_active(
        rig.concurrency.as_ref(),
        &deployment(),
        "call-in",
        true,
    ))
}

// ---- inbound admission -------------------------------------------------------------------------

#[test]
fn a_pooled_start_is_rejected_busy_at_n_and_admitted_again_once_a_holder_ends() {
    let rig = rig(SESSION, pooled(call_in()), pool_of(1));

    let first = drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("admitted");
    let first = instance_of(&first);
    // A park keeps the slot: the parked instance is still a live session.
    assert_eq!(usage(&rig), (1, 0));
    assert!(rig.pools.holds(&deployment(), &first, POOL));

    let busy = drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect_err("pool full");
    assert_eq!(busy.code, "SUTRA.INBOUND.POOL.EXHAUSTED", "{busy:?}");
    assert_eq!(
        busy.attributes.get("pool").map(String::as_str),
        Some(POOL),
        "{busy:?}"
    );
    assert_eq!(
        rig.bridge.parked(),
        1,
        "a rejected start runs nothing and parks nothing"
    );

    // The first session ends at End: its slot is free again.
    drive(rig.engine.dispatch(&message("agent-in", "c-1"))).expect("resumed to End");
    assert_eq!(rig.bridge.completed(), vec![first]);
    assert_eq!(usage(&rig), (0, 0));
    drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect("admitted again");
    assert_eq!(usage(&rig), (1, 0));
}

#[test]
fn the_outbound_reserve_keeps_slots_inbound_may_not_take() {
    let pools = Arc::new(InMemoryCapacityPools::new([
        CapacityPool::new(POOL, 3).with_reserves(0, 1)
    ]));
    let rig = rig(SESSION, pooled(call_in()), pools);
    drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("1 of 2 inbound-usable");
    drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect("2 of 2 inbound-usable");
    let busy = drive(rig.engine.dispatch(&message("call-in", "c-3"))).expect_err("reserve");
    assert_eq!(busy.code, "SUTRA.INBOUND.POOL.EXHAUSTED");
    // The reserved slot is still there for outbound.
    let outbound = drive(rig.pools.acquire(&SlotRequest {
        pool: POOL,
        deployment: &deployment(),
        tenant: TENANT,
        holder: "a1b2c3d4-0000-4000-8000-000000000001",
        call: "dial-1",
        direction: SlotDirection::Outbound,
        require_live: false,
    }))
    .unwrap();
    assert_eq!(outbound, SlotAcquisition::Acquired);
}

#[test]
fn two_engines_sharing_one_pool_cannot_exceed_it() {
    let pools = pool_of(1);
    let a = rig(SESSION, pooled(call_in()), Arc::clone(&pools));
    let b = rig(SESSION, pooled(call_in()), Arc::clone(&pools));
    drive(a.engine.dispatch(&message("call-in", "c-1"))).expect("admitted on a");
    let busy = drive(b.engine.dispatch(&message("call-in", "c-2"))).expect_err("full on b");
    assert_eq!(busy.code, "SUTRA.INBOUND.POOL.EXHAUSTED");
}

#[test]
fn a_failed_instance_gives_its_slot_back() {
    let rig = rig(SESSION_FAILS, pooled(call_in()), pool_of(1));
    drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("parked");
    assert_eq!(usage(&rig), (1, 0));
    let failed = drive(rig.engine.dispatch(&message("agent-in", "c-1")));
    assert!(
        failed.is_err() || matches!(failed, Ok(DispatchOutcome::DeadLettered { .. })),
        "{failed:?}"
    );
    assert_eq!(rig.bridge.0.borrow().failed.len(), 1);
    assert_eq!(usage(&rig), (0, 0));
}

#[test]
fn a_start_that_fails_before_parking_holds_no_slot() {
    let rig = rig(FAILS_AT_ONCE, pooled(call_in()), pool_of(1));
    let _ = drive(rig.engine.dispatch(&message("call-in", "c-1")));
    assert_eq!(usage(&rig), (0, 0));
    assert_eq!(rig.bridge.parked(), 0);
}

// ---- session-end -------------------------------------------------------------------------------

#[test]
fn a_channel_cap_counting_waiting_instances_rejects_a_second_session() {
    let mut capped = call_in();
    capped.max_concurrent_instances = Some(1);
    capped.use_only_in_flight_for_concurrency_cap = false;
    let rig = rig(SESSION, ends_on_hangup(capped), pool_of(1));
    drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("admitted");
    let busy = drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect_err("capped");
    assert_eq!(busy.code, "SUTRA.INBOUND.CHANNEL_AT_CAPACITY");

    // The session-ending message frees the channel slot while the instance runs on.
    drive(rig.engine.dispatch(&message("hangup-in", "c-1"))).expect("accepted");
    assert_eq!(channel_count(&rig), 0);
    drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect("admitted after the end");
}

#[test]
fn a_session_ending_message_releases_the_slot_while_the_instance_continues_to_end() {
    let rig = rig(SESSION, ends_on_hangup(pooled(call_in())), pool_of(1));
    let first =
        instance_of(&drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("admitted"));

    // No wait in the flow takes `hangup-in`: the message is ACCEPTED for the release alone.
    let ended = drive(rig.engine.dispatch(&message("hangup-in", "c-1"))).expect("accepted");
    assert_eq!(instance_of(&ended), first, "it names the released instance");
    assert_eq!(usage(&rig), (0, 0));
    assert_eq!(rig.bridge.parked(), 1, "the instance itself runs on");

    // The freed slot admits a new session while the first one is still alive...
    drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect("admitted");
    assert_eq!(usage(&rig), (1, 0));
    // ...and the first one still reaches End — without touching the second one's slot.
    drive(rig.engine.dispatch(&message("agent-in", "c-1"))).expect("resumed");
    assert_eq!(rig.bridge.completed(), vec![first]);
    assert_eq!(usage(&rig), (1, 0));

    // A release is idempotent: ending the finished session again changes nothing.
    drive(rig.engine.dispatch(&message("hangup-in", "c-1"))).expect("accepted");
    assert_eq!(usage(&rig), (1, 0));
}

#[test]
fn a_session_ending_message_for_an_unknown_instance_is_accepted_and_does_nothing() {
    let rig = rig(SESSION, ends_on_hangup(pooled(call_in())), pool_of(2));
    drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("admitted");
    let outcome =
        drive(rig.engine.dispatch(&message("hangup-in", "nobody"))).expect("accepted anyway");
    assert_eq!(instance_of(&outcome), "", "nothing was released");
    assert_eq!(usage(&rig), (1, 0));
}

#[test]
fn a_session_ending_message_a_wait_subscribes_to_is_delivered_and_still_releases() {
    let rig = rig(
        SESSION_AWAITS_END,
        ends_on_hangup(pooled(call_in())),
        pool_of(1),
    );
    let first =
        instance_of(&drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("admitted"));
    let outcome = drive(rig.engine.dispatch(&message("hangup-in", "c-1"))).expect("delivered");
    assert_eq!(instance_of(&outcome), first);
    // Delivered: the flow moved on from its hang-up wait to the next one.
    let snapshot = rig.bridge.0.borrow().parked.get(&first).cloned().unwrap();
    assert_eq!(snapshot.waiting_nodes, vec!["U".to_string()]);
    // Released: the instance runs on without its slot.
    assert_eq!(usage(&rig), (0, 0));
}

#[test]
fn on_reply_releases_the_slot_at_the_flows_reply() {
    let mut binding = pooled(call_in());
    binding.max_concurrent_instances = Some(1);
    binding.use_only_in_flight_for_concurrency_cap = false;
    binding.session_end = Some(SessionEnd {
        on_message: None,
        on_reply: true,
    });
    let rig = rig(SESSION_REPLIES, binding, pool_of(1));
    let outcome = drive(rig.engine.dispatch(&message("call-in", "c-1"))).expect("answered");
    let DispatchOutcome::Completed { reply, .. } = &outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(
        reply.as_ref().map(|r| r.body.get().as_slice()),
        Some(&b"hello"[..]),
        "the reply rode the request"
    );
    assert_eq!(rig.bridge.parked(), 1, "the instance parked after replying");
    assert_eq!(usage(&rig), (0, 0), "the reply ended the session");
    assert_eq!(channel_count(&rig), 0, "and freed the channel slot");
    drive(rig.engine.dispatch(&message("call-in", "c-2"))).expect("admitted");
}

// ---- one slot per call --------------------------------------------------------------------------

const CALL_KEY: &str = "x-call-id";

/// `message` carrying the call key header.
fn on_call(channel: &str, body: &str, call: &str) -> InboundMessage {
    let mut message = message(channel, body);
    message
        .headers
        .insert("X-Call-Id".to_string(), call.to_string());
    message
}

/// The outbound leg's slot, taken as the outbox takes it when it dials.
fn dial_leg(rig: &Rig, instance: &str, call: &str) {
    let acquired = drive(rig.pools.acquire(&SlotRequest {
        pool: POOL,
        deployment: &deployment(),
        tenant: TENANT,
        holder: instance,
        call,
        direction: SlotDirection::Outbound,
        require_live: true,
    }))
    .unwrap();
    assert_eq!(acquired, SlotAcquisition::Acquired);
}

fn keyed_session(mut binding: ChannelBinding) -> ChannelBinding {
    binding.pool_call_key = Some(CALL_KEY.to_string());
    binding.max_concurrent_instances = Some(5);
    binding.use_only_in_flight_for_concurrency_cap = false;
    ends_on_hangup(pooled(binding))
}

#[test]
fn a_bridged_instance_holds_one_slot_per_leg_and_a_hang_up_frees_only_that_leg() {
    let rig = rig(SESSION, keyed_session(call_in()), pool_of(3));
    let first = instance_of(
        &drive(rig.engine.dispatch(&on_call("call-in", "c-1", "leg-a"))).expect("admitted"),
    );
    dial_leg(&rig, &first, "leg-b");
    assert_eq!(
        rig.pools.held_calls(&deployment(), &first, POOL),
        2,
        "two legs, two slots"
    );
    assert_eq!(usage(&rig), (1, 1));

    // The far leg hangs up: its slot alone is freed; the caller's leg and channel slot stay.
    drive(rig.engine.dispatch(&on_call("hangup-in", "c-1", "leg-b"))).expect("accepted");
    assert_eq!(usage(&rig), (1, 0));
    assert_eq!(channel_count(&rig), 1, "the inbound session is still open");

    // End frees everything the instance still holds.
    dial_leg(&rig, &first, "leg-c");
    drive(rig.engine.dispatch(&message("agent-in", "c-1"))).expect("resumed to End");
    assert_eq!(rig.bridge.completed(), vec![first.clone()]);
    assert_eq!(usage(&rig), (0, 0));
    assert_eq!(rig.pools.held_calls(&deployment(), &first, POOL), 0);
}

#[test]
fn hanging_up_the_inbound_leg_frees_its_slot_and_the_channel_slot_but_not_the_far_leg() {
    let rig = rig(SESSION, keyed_session(call_in()), pool_of(3));
    let first = instance_of(
        &drive(rig.engine.dispatch(&on_call("call-in", "c-1", "leg-a"))).expect("admitted"),
    );
    dial_leg(&rig, &first, "leg-b");
    drive(rig.engine.dispatch(&on_call("hangup-in", "c-1", "leg-a"))).expect("accepted");
    assert_eq!(usage(&rig), (0, 1), "the far leg still holds its slot");
    assert_eq!(
        channel_count(&rig),
        0,
        "the session the caller opened is over"
    );
    assert_eq!(rig.bridge.parked(), 1, "the instance runs on");
}

#[test]
fn a_hang_up_without_the_call_key_ends_the_whole_session() {
    let rig = rig(SESSION, keyed_session(call_in()), pool_of(3));
    let first = instance_of(
        &drive(rig.engine.dispatch(&on_call("call-in", "c-1", "leg-a"))).expect("admitted"),
    );
    dial_leg(&rig, &first, "leg-b");
    drive(rig.engine.dispatch(&message("hangup-in", "c-1"))).expect("accepted");
    assert_eq!(usage(&rig), (0, 0));
    assert_eq!(channel_count(&rig), 0);
}

#[test]
fn on_reply_ends_the_inbound_call_only() {
    let mut binding = keyed_session(call_in());
    binding.session_end = Some(SessionEnd {
        on_message: None,
        on_reply: true,
    });
    let rig = rig(SESSION_REPLIES, binding, pool_of(3));
    let first = instance_of(
        &drive(rig.engine.dispatch(&on_call("call-in", "c-1", "leg-a"))).expect("answered"),
    );
    assert_eq!(usage(&rig), (0, 0), "the reply ended the inbound call");
    dial_leg(&rig, &first, "leg-b");
    assert_eq!(usage(&rig), (0, 1));
}

// ---- outbound stamping ---------------------------------------------------------------------------

#[test]
fn requests_for_a_pooled_outbound_channel_carry_the_pool() {
    let rig = rig(DIALS_OUT, call_in(), pool_of(1));
    drive(rig.engine.dispatch(&message("plain-in", "go"))).expect("parked at the call");
    let emissions = rig.bridge.0.borrow().emissions.clone();
    assert_eq!(emissions.len(), 2, "{emissions:?}");
    for emission in &emissions {
        assert_eq!(
            emission
                .headers
                .get(CAPACITY_POOL_HEADER)
                .map(String::as_str),
            Some(POOL),
            "{emission:?}"
        );
    }
    let calls: Vec<bool> = emissions
        .iter()
        .map(|e| e.headers.contains_key(CAPACITY_CALL_HEADER))
        .collect();
    assert_eq!(
        calls,
        vec![false, true],
        "only the channel-call is marked as one"
    );
    // Nothing is acquired at enqueue: the outbox takes the slot when it sends.
    assert_eq!(usage(&rig), (0, 0));
}
