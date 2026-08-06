//! Phase-3 loop-shape pin (execution scale-out §8 Phase 3 + its risk register): a lane
//! drives ONE request to completion — through its AWAIT points — before the next dequeue.
//!
//! The risk this exists to catch: an accidental `spawn_local`/`spawn` of request handling
//! inside the lane loop (the design's rejected option (b) sneaking back in unreviewed).
//! Under the correct loop shape, a request whose store commit is parked on a LONG AWAIT
//! (not a thread block — the runtime is free, the reactor turns, nothing else may run
//! only because the loop refuses to dequeue) keeps every queued request waiting. Under a
//! spawned/interleaved loop the second request would run inside the first one's await
//! window — which is exactly what the trace + pending assertions below reject.
//!
//! This is also the §8 per-key ordering pin in its strongest form: the second request IS
//! the first instance's own relay, so "park commit happens-before the instance's next
//! work" is asserted directly (claim/load never appear in the trace before the park
//! commit finishes).

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    spawn_engine, AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic,
    DispatchOutcome, DrainingSink, InboundChain, InboundMessage, InstanceBridge,
    InstanceClaimOutcome, Namespace, OutboxEmission, SuspendedInstance, TimerWaitRecord,
    ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000061").expect("valid deployment id")
}

/// hold: start(`start-in`, correlate alias e2eId=event.body) → userTask(`relay-in`) → end.
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

/// The async pause: `commit_park` records its state (alias visible), signals entry, then
/// AWAITS release — an await, never a thread block, so the lane's runtime and thread are
/// both free to run anything the loop (wrongly) let in.
#[derive(Default)]
struct AwaitGate {
    entered: AtomicBool,
    entered_notify: tokio::sync::Notify,
    released: AtomicBool,
    release_notify: tokio::sync::Notify,
}

impl AwaitGate {
    async fn wait_entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let notified = self.entered_notify.notified();
                if self.entered.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("the park never reached its commit await within 10s")
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }

    async fn held_until_released(&self) {
        self.entered.store(true, Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let notified = self.release_notify.notified();
                if self.released.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("the park gate was never released within 10s")
    }
}

#[derive(Default)]
struct BridgeState {
    trace: Vec<String>,
    parked: std::collections::HashMap<String, SuspendedInstance>,
    aliases: Vec<(String, String, String)>,
}

struct GatedBridge {
    state: Arc<Mutex<BridgeState>>,
    gate: Arc<AwaitGate>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for GatedBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        {
            let mut state = self.state.lock().expect("bridge state");
            state.trace.push(format!("park-enter:{instance_id}"));
            state
                .parked
                .insert(instance_id.to_string(), snapshot.clone());
            for alias in aliases {
                state.aliases.push((
                    alias.name.clone(),
                    alias.value.clone(),
                    instance_id.to_string(),
                ));
            }
        }
        // The long await: state committed and visible, the commit future parked. Held
        // with the state mutex released — anything wrongly interleaved could proceed.
        self.gate.held_until_released().await;
        self.state
            .lock()
            .expect("bridge state")
            .trace
            .push(format!("park-done:{instance_id}"));
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("load:{instance_id}"));
        Ok(state.parked.get(instance_id).cloned())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let state = self.state.lock().expect("bridge state");
        Ok(state
            .aliases
            .iter()
            .find(|(n, v, id)| n == name && v == value && state.parked.contains_key(id))
            .map(|(_, _, id)| id.clone()))
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("complete:{instance_id}"));
        state.parked.remove(instance_id);
        state.aliases.retain(|(_, _, id)| id != instance_id);
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state.trace.push(format!("repark:{instance_id}"));
        state
            .parked
            .insert(instance_id.to_string(), snapshot.clone());
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
        self.state
            .lock()
            .expect("bridge state")
            .trace
            .push(format!("claim:{instance_id}"));
        Ok(InstanceClaimOutcome::Granted)
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        self.state
            .lock()
            .expect("bridge state")
            .trace
            .push(format!("release:{instance_id}"));
        Ok(())
    }

    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        self.state
            .lock()
            .expect("bridge state")
            .trace
            .push(format!("failed:{instance_id}:{failure_code}"));
        Ok(())
    }
}

fn build_engine(state: Arc<Mutex<BridgeState>>, gate: Arc<AwaitGate>) -> ChannelEngine {
    let hold = BpmnModelLoader::new()
        .load(HOLD_BPMN.as_bytes())
        .expect("hold BPMN loads");
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
    .with_module(&deployment(), &hold)
    .with_instance_bridge(Rc::new(GatedBridge { state, gate }) as Rc<dyn InstanceBridge>)
    .build()
}

fn inbound(channel: &str, body: &[u8], key: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: key.to_string(),
        explicit_event_id: false,
        received_at: "2026-08-06T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn completed_instance(outcome: DispatchOutcome) -> String {
    match outcome {
        DispatchOutcome::Completed { instance_id, .. } => instance_id,
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// The pin. One lane; request 1 = a spawn whose park COMMIT is held open on an await;
/// request 2 = that instance's own relay, enqueued while the commit is held. The relay
/// must not start — not even its claim — until the park commit completes, and its own
/// dequeue must observe the committed park (per-key: commit happens-before the
/// instance's next work; per-lane: one request awaited to completion before the next
/// dequeue).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_await_request_fully_completes_before_a_queued_second_request_starts() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let gate = Arc::new(AwaitGate::default());
    let handle = {
        let state = Arc::clone(&state);
        let gate = Arc::clone(&gate);
        spawn_engine(tokio::runtime::Handle::current(), move || {
            build_engine(state, gate)
        })
    };

    // Request 1: the spawn. Its dispatch future is now held INSIDE commit_park's await.
    let spawn = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.dispatch(inbound("start-in", b"K-1", "m-s")).await })
    };
    gate.wait_entered().await;

    // Request 2: the SAME instance's relay, queued behind the held request.
    let relay = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.dispatch(inbound("relay-in", b"K-1", "m-r")).await })
    };

    // The lane's thread and runtime are both idle at an await point — yet the relay must
    // NOT begin: the loop owes request 1 completion first. Give a wrongly-interleaved
    // loop ample time to betray itself, then assert nothing after the park-enter ran.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !relay.is_finished(),
        "the queued relay finished while the first request was still mid-commit — \
         the lane interleaved requests"
    );
    {
        let held_id = parked_id(&state);
        let trace = state.lock().expect("state").trace.clone();
        assert_eq!(
            trace,
            vec![format!("park-enter:{held_id}")],
            "nothing may run on the lane while request 1 is held at its commit await"
        );
    }

    // Release: request 1 completes its commit, THEN request 2 claims/loads/completes.
    gate.release();
    let spawned = completed_instance(spawn.await.expect("spawn task").expect("spawn parks"));
    let resumed = completed_instance(relay.await.expect("relay task").expect("relay resumes"));
    assert_eq!(resumed, spawned, "the relay resumed its own instance");

    let trace = state.lock().expect("state").trace.clone();
    assert_eq!(
        trace,
        vec![
            format!("park-enter:{spawned}"),
            format!("park-done:{spawned}"),
            format!("claim:{spawned}"),
            format!("load:{spawned}"),
            format!("complete:{spawned}"),
        ],
        "the per-key order is total: park commit strictly before the relay's first op"
    );
}

/// The one parked instance's id (the spawn minted it mid-flight; the trace carries it).
fn parked_id(state: &Arc<Mutex<BridgeState>>) -> String {
    let state = state.lock().expect("state");
    state
        .parked
        .keys()
        .next()
        .cloned()
        .expect("exactly one parked instance")
}
