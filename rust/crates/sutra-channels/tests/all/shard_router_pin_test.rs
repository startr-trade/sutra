//! Phase-1 shard-router pins (execution scale-out §8, N=1 identity): the router behind
//! `spawn_engine` must be byte-identical to the pre-router single mailbox — one actor
//! thread, FIFO drain, and the EXACT bridge-call/outcome sequence for an interleaved
//! park/relay/timer run — while the seams that are DEAD at `shard-count = 1` (the
//! `Handoff` outcome, `resume_resolved`) answer exactly as specified when driven
//! directly.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    shard_index_of, spawn_engine, AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry,
    Diagnostic, DispatchOutcome, DrainingSink, EngineShard, InboundChain, InboundMessage,
    InstanceBridge, InstanceClaimOutcome, Namespace, OutboxEmission, ResolvedResume,
    SuspendedInstance, TimerFireOutcome, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TimerFire, TokenExecutor, Variables};

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000051").expect("valid deployment id")
}

/// hold: start(`start-in`, correlate alias e2eId=event.body) → userTask(`relay-in`) → end.
/// The relay_resume_test / deferred_ack_test shape — a relay completes the instance.
const HOLD_BPMN: &str = r#"<?xml version="1.0"?>
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

/// The bridge's observable state, `Send` behind an `Arc<Mutex<…>>` so the TEST thread can
/// pin the exact call sequence the actor thread produced (the bridge itself is `Rc`-held
/// on the actor thread; only this state crosses).
#[derive(Default)]
struct BridgeState {
    /// Every bridge call, in arrival order — the drain-order pin.
    trace: Vec<String>,
    /// The one parked instance (id + snapshot); `commit_complete` clears it.
    parked: Option<(String, SuspendedInstance)>,
}

struct SharedBridge {
    state: Arc<Mutex<BridgeState>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for SharedBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("park:{instance_id}"));
        state.parked = Some((instance_id.to_string(), snapshot.clone()));
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        let hit = state
            .parked
            .as_ref()
            .filter(|(id, _)| id == instance_id)
            .map(|(_, snapshot)| snapshot.clone());
        state.trace.push(format!(
            "load:{instance_id}:{}",
            if hit.is_some() { "hit" } else { "miss" }
        ));
        Ok(hit)
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("alias:{name}={value}"));
        Ok(state.parked.as_ref().map(|(id, _)| id.clone()))
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("complete:{instance_id}"));
        state.parked = None;
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("repark:{instance_id}"));
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
        instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("claim:{instance_id}"));
        Ok(InstanceClaimOutcome::Granted)
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("release:{instance_id}"));
        Ok(())
    }

    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state
            .trace
            .push(format!("failed:{instance_id}:{failure_code}"));
        Ok(())
    }
}

fn engine_with_state(state: Arc<Mutex<BridgeState>>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(HOLD_BPMN.as_bytes())
        .expect("BPMN loads");
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
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
        "relay-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_instance_bridge(Rc::new(SharedBridge { state }) as Rc<dyn InstanceBridge>)
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
        received_at: "2026-08-04T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn completed_instance(outcome: DispatchOutcome) -> String {
    match outcome {
        DispatchOutcome::Completed { instance_id, .. } => instance_id,
        other => panic!("expected Completed, got {other:?}"),
    }
}

// ================================ the N=1 identity pin ====================================

/// The Phase-1 acceptance bar: an interleaved park → relay → timer sequence, driven
/// through `spawn_engine`'s router (one lane), produces EXACTLY the pre-router bridge
/// call sequence and outcome shapes. Any drift the router extraction introduced — an
/// extra hop, a reordered claim, a handoff wrongly taken at N=1 — breaks this trace.
#[tokio::test]
async fn n1_router_preserves_the_park_relay_timer_sequence_exactly() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let actor_state = Arc::clone(&state);
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        engine_with_state(actor_state)
    });

    // 1) SPAWN: the wait-state start parks instance A (one park commit, nothing else).
    // (The spawn's own `onConflict=correlate` alias consults the live-alias index before
    // parking — today's pipeline order, so the trace pin below starts with that lookup.)
    let a = completed_instance(
        handle
            .dispatch(inbound("start-in", b"K-100"))
            .await
            .expect("spawn parks"),
    );
    assert!(!a.is_empty(), "the parked instance id rides back");

    // 2) RELAY: correlates via the alias index, claims, rehydrates, completes A.
    let resumed = completed_instance(
        handle
            .dispatch(inbound("relay-in", b"K-100"))
            .await
            .expect("relay resumes to completion"),
    );
    assert_eq!(resumed, a, "the relay resumed the parked instance");

    // 3) TIMER: a fire for the now-terminal A finds no row — Stale, exactly today's
    //    answer (claim, miss, guard release). Routed by instance id through the router.
    let outcome = handle
        .fire_timer(TimerFire {
            deployment: deployment(),
            instance_id: a.clone(),
            node_id: "U".to_string(),
            due_at: "2026-08-04T10:05:00Z".to_string(),
            fired_at: "2026-08-04T10:05:01Z".to_string(),
        })
        .await
        .expect("timer fire answers");
    assert!(matches!(outcome, TimerFireOutcome::Stale));

    // The pin: the exact bridge-call sequence the single actor produced pre-router.
    let trace = state.lock().expect("bridge state").trace.clone();
    assert_eq!(
        trace,
        vec![
            "alias:e2eId=K-100".to_string(),
            format!("park:{a}"),
            "alias:e2eId=K-100".to_string(),
            format!("claim:{a}"),
            format!("load:{a}:hit"),
            format!("complete:{a}"),
            format!("claim:{a}"),
            format!("load:{a}:miss"),
            format!("release:{a}"),
        ],
        "the N=1 router must reproduce the pre-router dispatch sequence byte-for-byte"
    );
}

// ============================ the dead-at-N=1 handoff seams ===============================

#[test]
fn the_single_lane_owns_every_instance_id() {
    for id in [
        "5f0c9f5e-0000-4000-8000-000000000001",
        "5f0c9f5e-0000-4000-8000-00000000ffff",
        "anything-at-all",
        "",
    ] {
        assert_eq!(shard_index_of(id, 1), 0, "count=1 maps '{id}' to shard 0");
        assert!(EngineShard::single().owns(id));
    }
    // The hash is a stable function of the id, always inside the lane count.
    for id in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        let lane = shard_index_of(id, 4);
        assert!(lane < 4);
        assert_eq!(lane, shard_index_of(id, 4), "stable per id");
    }
}

#[test]
fn resume_resolved_without_a_bridge_fails_closed() {
    // The handoff-execution arm shares the relay tail's posture: no persistence bridge,
    // no resume — never a silent drop.
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
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
    .build();
    let error = drive(engine.resume_resolved(&resolved_resume("inst-1")))
        .expect_err("no bridge ⇒ fail closed");
    assert_eq!(error.code, "SUTRA.INBOUND.PERSISTENCE_REQUIRED");
}

#[test]
fn resume_resolved_executes_the_race_safe_tail_exactly_like_a_relay() {
    // DEAD at N=1 in production (nothing produces a handoff), so prove the arm directly:
    // a pre-parked instance resumed via `resume_resolved` runs claim → load → resume →
    // terminal commit — the same tail the relay path runs, byte-for-byte.
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let engine = engine_with_state(Arc::clone(&state));
    {
        let mut s = state.lock().expect("bridge state");
        s.parked = Some((
            "11111111-2222-4333-8444-555555555555".to_string(),
            SuspendedInstance {
                process_id: "hold".to_string(),
                deployment_id: deployment().value().to_string(),
                status: "SUSPENDED".to_string(),
                suspended: true,
                waiting_nodes: vec!["U".to_string()],
                completed_nodes: vec!["S".to_string()],
                ..Default::default()
            },
        ));
    }
    let outcome =
        drive(engine.resume_resolved(&resolved_resume("11111111-2222-4333-8444-555555555555")))
            .expect("the resolved resume completes the instance");
    assert_eq!(
        completed_instance(outcome),
        "11111111-2222-4333-8444-555555555555"
    );
    let trace = state.lock().expect("bridge state").trace.clone();
    assert_eq!(
        trace,
        vec![
            "claim:11111111-2222-4333-8444-555555555555".to_string(),
            "load:11111111-2222-4333-8444-555555555555:hit".to_string(),
            "complete:11111111-2222-4333-8444-555555555555".to_string(),
        ],
        "the handoff arm re-runs exactly the race-safe tail"
    );
}

fn resolved_resume(instance_id: &str) -> ResolvedResume {
    ResolvedResume {
        deployment: deployment(),
        instance_id: instance_id.to_string(),
        wait_node_id: "U".to_string(),
        variables: Variables::new(),
        alias_name: "e2eId".to_string(),
        alias_value: "K-100".to_string(),
        channel: "relay-in".to_string(),
        labels: BTreeMap::new(),
        traceparent: None,
    }
}
