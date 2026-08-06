//! The dispatcher half of channel-call `<q:retry>` (F1): the timer-fire DISCRIMINATION, the
//! relay guard, and the poison wake — everything only this layer can prove.
//!
//! A channel-call node's snapshot facts are ambiguous in a way a registered task's never are:
//! `waiting + attempts>0` describes BOTH "backoff pending (attempt dead)" and "attempt in
//! flight (waiting on its response)". The durable `sutra.retryWait.<nodeId>` marker is the
//! discriminator, and every decision pinned here keys on it:
//!
//! * a due TIMER row on a MARKED call node is the backoff RE-DRIVE — it must RE-EMIT;
//! * a due TIMER row on an UNMARKED call node is stale residue — resolved, never driven;
//! * a timeout-boundary fire whose host is MARKED is stale (the poison beat it; driving it
//!   would double-count one attempt);
//! * a relay correlating to a MARKED node is REFUSED (`CHANNEL_CALL.RETRY_PENDING`) — the
//!   response belongs to the dead attempt;
//! * a poison wake acts ONLY on durable evidence (a poisoned row for the exact
//!   `(instance, node)`), and never on a node already marked.
//!
//! Driven through the same in-memory [`InstanceBridge`] double the other dispatcher tests
//! use; the observed re-park steps carry the emissions, so re-emission is asserted at the
//! COMMIT seam (what would have become outbox rows), not at a mock transport.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelCallPoisonFire, ChannelCallPoisonOutcome, ChannelEngine,
    CodecRegistry, Diagnostic, DrainingSink, InboundChain, InboundMessage, InstanceBridge,
    Namespace, OutboxEmission, SuspendedInstance, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::{
    DeploymentId, OutboundChannelRegistry, ResolvedOutboundChannel, TaskRegistry, TimerFire,
    TokenExecutor,
};

use crate::support::drive;

const TENANT: &str = "acme";
const INSTANCE: &str = "22222222-1111-4111-8111-111111111111";
const NOW: &str = "2026-08-06T10:00:00Z";
const TIMEOUT_CODE: &str = "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT";
const POISON_CODE: &str = "SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "billing", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000f2").expect("valid deployment id")
}

/// start(`start-in`) → channel-call `Call` (out; response on `call-resp`; `<q:timeout PT2S>`;
/// retried 3× at PT10S/2.0) → end.
const CALL_RETRY_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="callflow">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Call" implementation="channel:out">
      <bpmn:extensionElements>
        <q:source channel="call-resp" name="payload"/>
        <q:alias name="ccKey" expression="payload"/>
        <q:timeout duration="PT2S"/>
        <q:retry maxAttempts="3" initialDelay="PT10S" backoffCoefficient="2.0"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// One observed re-park step, EMISSIONS included — the re-emission contract is asserted at
/// this commit seam (these are what would have become fresh outbox rows).
struct ObservedRepark {
    snapshot: SuspendedInstance,
    satisfied_wait_nodes: Vec<String>,
    timer_waits: Vec<TimerWaitRecord>,
    emissions: Vec<OutboxEmission>,
}

#[derive(Default)]
struct FakeBridge {
    loaded: Option<SuspendedInstance>,
    /// The relay correlation answer (`find_live_alias`).
    alias_owner: Option<String>,
    /// The poison wake's durable-evidence answer.
    poisoned_exists: bool,
    reparks: RefCell<Vec<ObservedRepark>>,
    completed: RefCell<bool>,
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
        Ok(self.alias_owner.clone())
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
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.reparks.borrow_mut().push(ObservedRepark {
            snapshot: snapshot.clone(),
            satisfied_wait_nodes: satisfied_wait_nodes.to_vec(),
            timer_waits: timer_waits.to_vec(),
            emissions: emissions.to_vec(),
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

    async fn poisoned_call_emission_exists(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _node_id: &str,
    ) -> Result<bool, Diagnostic> {
        Ok(self.poisoned_exists)
    }
}

fn engine_with(bridge: Rc<FakeBridge>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(CALL_RETRY_BPMN.as_bytes())
        .expect("BPMN loads");
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
        &deployment(),
        ResolvedOutboundChannel::resolve(
            "out",
            "http",
            "http://sink.example/req",
            None,
            None,
            None,
            "none",
        ),
    );
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_outbound_channels(outbound)
        .with_now_supplier(|| NOW.to_string())
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
    .with_binding(ChannelBinding::new(
        "start-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_binding(ChannelBinding::new(
        "call-resp",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
    .build()
}

/// The snapshot of an instance parked ON `Call`. `marker` = the backoff state: `Some(code)`
/// is a DEAD attempt awaiting its re-drive; `None` is an attempt IN FLIGHT.
fn parked_on_call(attempts: u32, marker: Option<&str>) -> SuspendedInstance {
    SuspendedInstance {
        process_id: "callflow".to_string(),
        deployment_id: deployment().value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        waiting_nodes: vec!["Call".to_string()],
        variables: vec![(
            "payload".to_string(),
            sutra_feel::FeelValue::String("K-1".to_string()),
        )],
        retry_attempts: if attempts > 0 {
            BTreeMap::from([("Call".to_string(), attempts)])
        } else {
            BTreeMap::new()
        },
        retry_backoff: marker
            .map(|code| BTreeMap::from([("Call".to_string(), code.to_string())]))
            .unwrap_or_default(),
        ..Default::default()
    }
}

fn fire(node: &str) -> TimerFire {
    TimerFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: node.to_string(),
        due_at: "2026-08-06T09:59:50Z".to_string(),
        fired_at: NOW.to_string(),
    }
}

fn response(body: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: "call-resp".to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: "resp-1".to_string(),
        explicit_event_id: false,
        received_at: NOW.to_string(),
        cloud_event: None,
    }
}

// ============================ the timer-fire discrimination ==============================

#[test]
fn a_due_backoff_on_a_marked_call_re_drives_with_a_fresh_emission() {
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, Some(TIMEOUT_CODE))),
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fire_timer(&fire("Call"))).expect("the re-drive parks");
    assert!(
        matches!(
            outcome,
            sutra_channels::TimerFireOutcome::Resumed {
                completed: false,
                ..
            }
        ),
        "expected a re-park, got {outcome:?}"
    );

    let reparks = bridge.reparks.borrow();
    assert_eq!(reparks.len(), 1);
    let observed = &reparks[0];
    // THE RE-EMISSION: exactly one fresh request rode the step commit, with a fresh outbox
    // key (idempotency posture) minted for it.
    assert_eq!(observed.emissions.len(), 1, "the re-drive RE-EMITS");
    assert_eq!(observed.emissions[0].node_id, "Call");
    assert_eq!(observed.emissions[0].destination, "http://sink.example/req");
    assert!(!observed.emissions[0].outbox_key.is_empty());
    // The backoff row resolves; the fresh attempt waits with a FRESH timeout boundary.
    assert_eq!(observed.satisfied_wait_nodes[0], "Call");
    assert_eq!(observed.timer_waits.len(), 1);
    assert_eq!(observed.timer_waits[0].node_id, "Call#timeout");
    assert_eq!(observed.timer_waits[0].due_at, "2026-08-06T10:00:02Z");
    // Marker consumed, burned budget kept.
    assert!(observed.snapshot.retry_backoff.is_empty());
    assert_eq!(observed.snapshot.retry_attempts.get("Call"), Some(&1));
}

#[test]
fn a_due_timer_row_on_an_unmarked_call_is_stale_residue() {
    // The only writer of a TIMER row keyed to a call node is a backoff park, whose marker
    // commits in the same transaction — no marker means the row outlived its meaning.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, None)),
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fire_timer(&fire("Call"))).expect("stale is not an error");
    assert!(
        matches!(outcome, sutra_channels::TimerFireOutcome::Stale),
        "expected Stale, got {outcome:?}"
    );
    assert!(bridge.reparks.borrow().is_empty(), "nothing was driven");
}

#[test]
fn a_timeout_boundary_fire_on_a_marked_host_is_stale() {
    // The poison beat this timeout to the failure: the marker's park already resolved the
    // boundary row and consumed a budget slot. Racing the fire in would double-count it.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, Some(POISON_CODE))),
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fire_timer(&fire("Call#timeout"))).expect("stale, not an error");
    assert!(
        matches!(outcome, sutra_channels::TimerFireOutcome::Stale),
        "expected Stale, got {outcome:?}"
    );
    assert!(bridge.reparks.borrow().is_empty());
    assert!(bridge.failed_commits.borrow().is_empty());
}

// ============================ the relay guard ============================================

#[test]
fn a_relay_for_a_marked_node_is_refused_with_retry_pending() {
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, Some(TIMEOUT_CODE))),
        alias_owner: Some(INSTANCE.to_string()),
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let d = drive(engine.dispatch(&response("K-1")))
        .expect_err("the response belongs to the DEAD attempt and is refused");
    assert_eq!(d.code, "SUTRA.DISPATCH.CHANNEL_CALL.RETRY_PENDING", "{d:?}");
    assert!(
        bridge.reparks.borrow().is_empty() && !*bridge.completed.borrow(),
        "the parked instance must be untouched"
    );
}

#[test]
fn a_relay_for_an_in_flight_attempt_resumes_normally() {
    // Mid-retry (one burned attempt) but NO marker: the attempt is live, its answer wins.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, None)),
        alias_owner: Some(INSTANCE.to_string()),
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    drive(engine.dispatch(&response("K-1"))).expect("the response resumes the instance");
    assert!(
        *bridge.completed.borrow(),
        "the answered call completes the flow"
    );
}

// ============================ the poison wake ============================================

#[test]
fn a_poison_wake_with_durable_evidence_parks_the_backoff() {
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(0, None)),
        poisoned_exists: true,
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fail_channel_call(&ChannelCallPoisonFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: "Call".to_string(),
    }))
    .expect("the wake parks the backoff");
    assert!(
        matches!(outcome, ChannelCallPoisonOutcome::Parked { .. }),
        "expected Parked, got {outcome:?}"
    );

    let reparks = bridge.reparks.borrow();
    assert_eq!(reparks.len(), 1);
    let observed = &reparks[0];
    assert!(observed.emissions.is_empty(), "a failing pass never emits");
    assert_eq!(
        observed
            .snapshot
            .retry_backoff
            .get("Call")
            .map(String::as_str),
        Some(POISON_CODE)
    );
    assert_eq!(observed.snapshot.retry_attempts.get("Call"), Some(&1));
    assert_eq!(observed.timer_waits.len(), 1);
    assert_eq!(observed.timer_waits[0].node_id, "Call");
    assert_eq!(observed.timer_waits[0].due_at, "2026-08-06T10:00:10Z");
}

#[test]
fn a_poison_wake_without_durable_evidence_is_a_no_op() {
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(0, None)),
        poisoned_exists: false,
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fail_channel_call(&ChannelCallPoisonFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: "Call".to_string(),
    }))
    .expect("a prompt without evidence is a no-op, never an error");
    assert_eq!(outcome, ChannelCallPoisonOutcome::NotApplicable);
    assert!(bridge.reparks.borrow().is_empty());
    assert!(bridge.failed_commits.borrow().is_empty());
}

#[test]
fn a_poison_wake_on_a_marked_node_is_a_no_op() {
    // The timeout already accounted this attempt's failure — a second failure would
    // double-count one budget slot.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(1, Some(TIMEOUT_CODE))),
        poisoned_exists: true,
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fail_channel_call(&ChannelCallPoisonFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: "Call".to_string(),
    }))
    .expect("no-op");
    assert_eq!(outcome, ChannelCallPoisonOutcome::NotApplicable);
    assert!(bridge.reparks.borrow().is_empty());
}

#[test]
fn a_poison_wake_on_a_spent_budget_commits_durable_failed_state() {
    // Two attempts already burned (in flight, unmarked); the poison is attempt 3 of 3.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(parked_on_call(2, None)),
        poisoned_exists: true,
        ..Default::default()
    });
    let engine = engine_with(Rc::clone(&bridge));

    let outcome = drive(engine.fail_channel_call(&ChannelCallPoisonFire {
        deployment: deployment(),
        instance_id: INSTANCE.to_string(),
        node_id: "Call".to_string(),
    }))
    .expect("exhaustion is a handled outcome, not a wake error");
    assert!(
        matches!(outcome, ChannelCallPoisonOutcome::Failed { .. }),
        "expected Failed, got {outcome:?}"
    );
    let failed = bridge.failed_commits.borrow();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].1, "SUTRA.RUNTIME.RETRY.EXHAUSTED");
    assert!(
        failed[0].2.contains(POISON_CODE),
        "the poison classification is carried into the durable record: {}",
        failed[0].2
    );
    assert!(bridge.reparks.borrow().is_empty());
}
