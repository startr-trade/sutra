//! Phase-2 shard scale-out ITs (execution scale-out §8, N=4 live): REAL multi-lane
//! routers — four `sutra-channel-engine-<i>` actor threads over one shared (Send)
//! bridge — proving the cross-shard windows the N=1 suites structurally cannot reach:
//!
//! - concurrent relays to ONE instance stay strictly serial (exactly one terminal
//!   commit; the instance-op trace is a serial pattern; the cross-lane audit seq is
//!   strictly monotonic — the §8 witness);
//! - a relay resolved on a non-owner lane HANDS OFF and resumes on the owner lane;
//! - the activation flip fans out with an await-all barrier (update returns ⇒ every
//!   lane serves the new build; a pre-flip park resumes after the flip);
//! - the REAL §2.1 deferred-ack race: park blocked in its commit window on lane A, the
//!   instance completed via lane B — the ack fires exactly once, never dangles;
//! - mis-route injection: a wrong-lane resume against a held claim bounces `CLAIM_HELD`
//!   (retry-safe, nothing loaded past the guard, never interleaving) and lands on the
//!   claim-bounce counters split relay/timer;
//! - a bounded-capacity N=4 router under a 100-task burst makes progress without
//!   deadlock (backpressure awaits on the caller, never on another lane).

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    codes, shard_index_of, spawn_engine_sharded, AliasRecord, AuditEvent, AuditListener,
    ChannelBinding, ChannelEngine, CodecRegistry, DeferredAckListener, DeferredAckRegistry,
    DeferredDispatch, DeferredSettle, Diagnostic, DispatchOutcome, DrainingSink, EngineHandle,
    EngineShard, InboundChain, InboundMessage, InstanceBridge, InstanceClaimOutcome, Namespace,
    OutboxEmission, ResolvedResume, ShardLaneMetrics, SuspendedInstance, TimerWaitRecord,
    ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor, Variables};

use crate::support::drive;

const TENANT: &str = "acme";
const SHARDS: u32 = 4;

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000052").expect("valid deployment id")
}

/// hold: start(`start-in`, correlate alias e2eId=event.body) → userTask(`relay-in`) → end,
/// AUDITED (`<q:audit sink="jsonl">`) so the per-instance audit seq — persisted into the
/// snapshot at park, seeded back on the owner lane at resume — is observable.
const HOLD_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold">
    <bpmn:extensionElements>
      <q:audit sink="jsonl"/>
    </bpmn:extensionElements>
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

/// flow: start(`flow-in`) → end — runs to completion inside the dispatch (the load shape).
const RUN_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo-run">
    <bpmn:startEvent id="S3">
      <bpmn:extensionElements>
        <q:source channel="flow-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E3"/>
    <bpmn:sequenceFlow id="f" sourceRef="S3" targetRef="E3"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// flow2: the V2-ONLY run-to-end process (`flow2-in`) — the flip-generation marker: a
/// lane that answers on `flow2-in` is provably serving the post-flip build.
const RUN2_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo-run-v2">
    <bpmn:startEvent id="S4">
      <bpmn:extensionElements>
        <q:source channel="flow2-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E4"/>
    <bpmn:sequenceFlow id="f" sourceRef="S4" targetRef="E4"/>
  </bpmn:process>
</bpmn:definitions>"#;

// ---------------------------------------------------------------------------------------
// The shared (Send) bridge: one state behind an Arc<Mutex<…>>, wrapped per lane in an
// `Rc` bridge — the shard_router_pin_test pattern, extended to hold MANY instances and
// their alias rows, plus an optional park gate for the §2.1 race window.
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct BridgeState {
    /// Instance-op trace entries (`park`/`claim`/`load`/`complete`/`repark`/`release`),
    /// each tagged with the acting lane (parsed from the actor thread's name).
    trace: Vec<String>,
    parked: HashMap<String, SuspendedInstance>,
    /// `(alias name, alias value, instance id)` — rows live from park to terminal.
    aliases: Vec<(String, String, String)>,
}

/// The lane index parsed from the current actor thread's name
/// (`sutra-channel-engine-<i>`) — pins the §8 thread-name contract as a side effect.
fn current_lane() -> Option<u32> {
    std::thread::current()
        .name()
        .and_then(|n| n.strip_prefix("sutra-channel-engine-").map(str::to_string))
        .and_then(|i| i.parse().ok())
}

fn lane_tag() -> String {
    current_lane().map_or_else(|| "?".to_string(), |i| i.to_string())
}

/// The §2.1 race window: `commit_park` commits state (alias visible), reports the
/// parked instance + parking lane, then HOLDS the lane's park future until the test
/// releases it — the exact "park committed, dispatch arm not yet returned" window another
/// lane can complete the instance inside.
///
/// Phase 3 MECHANISM change (assertions untouched): the hold is an ASYNC gate (`Notify` +
/// flags), not a std `Condvar` blocking the lane's OS thread — `commit_park` is awaited on
/// the lane's single actor task now, and an async-aware pause is the faithful way to hold
/// that task open. The race being pinned is identical: lane A parked-but-not-returned,
/// lane B (a different lane = a different thread) completes the instance inside the
/// window, the deferred ack fires exactly once. If this test DEADLOCKS, a lane stopped
/// driving one request to completion at a time — the exact regression the design's risk
/// register names.
#[derive(Default)]
struct ParkGate {
    entered: Mutex<Option<(String, u32)>>,
    entered_notify: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
    release_notify: tokio::sync::Notify,
}

impl ParkGate {
    async fn wait_entered(&self) -> (String, u32) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                // Register interest BEFORE checking, so a notify between check and wait
                // is never lost.
                let notified = self.entered_notify.notified();
                if let Some(hit) = self.entered.lock().expect("gate entered").take() {
                    return hit;
                }
                notified.await;
            }
        })
        .await
        .expect("no park entered the gate within 10s")
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }

    fn rearm(&self) {
        self.released.store(false, Ordering::SeqCst);
    }

    async fn held_until_released(&self) {
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

struct SharedBridge {
    state: Arc<Mutex<BridgeState>>,
    park_gate: Option<Arc<ParkGate>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for SharedBridge {
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
            state
                .trace
                .push(format!("park:{instance_id}@{}", lane_tag()));
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
        // The window is held with the state mutex RELEASED — the commit is visible to
        // every other lane while this lane's dispatch arm has not yet returned. The hold
        // is an await (the Phase 3 mechanism): this lane's single actor task stays inside
        // `commit_park` while OTHER lanes' tasks — other threads — keep executing.
        if let Some(gate) = &self.park_gate {
            {
                let mut entered = gate.entered.lock().expect("gate entered");
                *entered = Some((
                    instance_id.to_string(),
                    current_lane().expect("park runs on a named engine lane"),
                ));
                gate.entered_notify.notify_waiters();
            }
            gate.held_until_released().await;
        }
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        let hit = state.parked.get(instance_id).cloned();
        state.trace.push(format!(
            "load:{instance_id}:{}@{}",
            if hit.is_some() { "hit" } else { "miss" },
            lane_tag()
        ));
        Ok(hit)
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
        state
            .trace
            .push(format!("complete:{instance_id}@{}", lane_tag()));
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
        state
            .trace
            .push(format!("repark:{instance_id}@{}", lane_tag()));
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
        let mut state = self.state.lock().expect("bridge state");
        state
            .trace
            .push(format!("claim:{instance_id}@{}", lane_tag()));
        Ok(InstanceClaimOutcome::Granted)
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        let mut state = self.state.lock().expect("bridge state");
        state
            .trace
            .push(format!("release:{instance_id}@{}", lane_tag()));
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
        state.trace.push(format!(
            "failed:{instance_id}:{failure_code}@{}",
            lane_tag()
        ));
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------
// Lane builds — everything the per-lane closure captures is Arc/Sender (Clone + Send).
// ---------------------------------------------------------------------------------------

#[derive(Clone)]
struct LaneParts {
    state: Arc<Mutex<BridgeState>>,
    /// The engine-process-scoped registry (production shape: ONE Arc, every lane's
    /// executor listener and park arm share it). `None` = no deferral capability.
    deferred: Option<Arc<DeferredAckRegistry>>,
    /// One audit channel across all lanes — each lane's listener sends into it.
    audit_tx: Option<tokio::sync::mpsc::UnboundedSender<AuditEvent>>,
    park_gate: Option<Arc<ParkGate>>,
    /// V2 flip marker: also register the `flow2-in` process + binding.
    with_v2_flow: bool,
}

fn build_lane(
    parts: &LaneParts,
    shard: EngineShard,
    metrics: Arc<ShardLaneMetrics>,
) -> ChannelEngine {
    let hold = BpmnModelLoader::new()
        .load(HOLD_BPMN.as_bytes())
        .expect("hold BPMN loads");
    let run = BpmnModelLoader::new()
        .load(RUN_BPMN.as_bytes())
        .expect("run BPMN loads");
    let sink = Rc::new(DrainingSink::new());
    let mut executor_builder = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>);
    if let Some(registry) = &parts.deferred {
        executor_builder =
            executor_builder.with_listener(Rc::new(DeferredAckListener::new(Arc::clone(registry))));
    }
    let audit_listener = parts.audit_tx.as_ref().map(|tx| {
        Rc::new(AuditListener::new(tx.clone(), || {
            "2026-08-06T00:00:00Z".to_string()
        }))
    });
    if let Some(listener) = &audit_listener {
        executor_builder = executor_builder.with_listener(
            Rc::clone(listener) as Rc<dyn sutra_executor::listener::ExecutionListener>
        );
    }
    let executor = executor_builder.build();
    let mut builder = ChannelEngine::builder(
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
    .with_binding(ChannelBinding::new(
        "flow-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &hold)
    .with_module(&deployment(), &run)
    .with_instance_bridge(Rc::new(SharedBridge {
        state: Arc::clone(&parts.state),
        park_gate: parts.park_gate.clone(),
    }) as Rc<dyn InstanceBridge>)
    .with_shard(shard)
    .with_shard_metrics(metrics);
    if parts.with_v2_flow {
        let run2 = BpmnModelLoader::new()
            .load(RUN2_BPMN.as_bytes())
            .expect("run2 BPMN loads");
        builder = builder
            .with_binding(ChannelBinding::new(
                "flow2-in",
                namespace(),
                deployment(),
                "raw-text",
            ))
            .with_module(&deployment(), &run2);
    }
    if let Some(registry) = &parts.deferred {
        builder = builder.with_deferred_acks(Arc::clone(registry));
    }
    if let Some(listener) = audit_listener {
        builder = builder.with_audit_listener(listener);
    }
    builder.build()
}

fn spawn_router(parts: LaneParts, capacity: Option<usize>) -> EngineHandle {
    spawn_engine_sharded(
        SHARDS,
        capacity,
        tokio::runtime::Handle::current(),
        move |shard, metrics| build_lane(&parts, shard, metrics),
    )
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

/// Sum one lane counter across the router.
fn sum_metric(handle: &EngineHandle, read: impl Fn(&ShardLaneMetrics) -> u64) -> u64 {
    handle.shard_metrics().lanes().iter().map(|l| read(l)).sum()
}

fn resolved_resume(instance_id: &str) -> ResolvedResume {
    ResolvedResume {
        deployment: deployment(),
        instance_id: instance_id.to_string(),
        wait_node_id: "U".to_string(),
        variables: Variables::new(),
        alias_name: "e2eId".to_string(),
        alias_value: "K".to_string(),
        channel: "relay-in".to_string(),
        labels: BTreeMap::new(),
        traceparent: None,
    }
}

// =======================================================================================
// 1. Concurrent relays to ONE instance stay strictly serial (§8: audit-seq witness)
// =======================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_relays_to_one_instance_stay_strictly_serial_at_n4() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let (audit_tx, mut audit_rx) = sutra_channels::audit_channel();
    let handle = spawn_router(
        LaneParts {
            state: Arc::clone(&state),
            deferred: None,
            audit_tx: Some(audit_tx),
            park_gate: None,
            with_v2_flow: false,
        },
        None,
    );

    // Park one instance; its owner lane is fixed by the id hash from here on.
    let id = completed_instance(
        handle
            .dispatch(inbound("start-in", b"K-100", "m-0"))
            .await
            .expect("spawn parks"),
    );

    // 8 CONCURRENT relays carrying the same correlation. Arrival lanes fan round-robin;
    // every resume must serialize on the one owner lane.
    let mut relays = Vec::new();
    for i in 0..8 {
        let handle = handle.clone();
        relays.push(tokio::spawn(async move {
            handle
                .dispatch(inbound("relay-in", b"K-100", &format!("m-r{i}")))
                .await
        }));
    }
    let mut completed = 0u32;
    for relay in relays {
        match relay.await.expect("relay task") {
            Ok(outcome) => {
                assert_eq!(completed_instance(outcome), id);
                completed += 1;
            }
            Err(d) => assert!(
                d.code == codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND
                    || d.code == codes::RUNTIME_RESUME_INSTANCE_NOT_FOUND,
                "a losing relay answers a serial-outcome code, got [{}] {}",
                d.code,
                d.message
            ),
        }
    }
    assert_eq!(completed, 1, "exactly ONE relay completes the instance");

    // The instance-op trace is the serial pattern: park, the winning resume's
    // claim→load(hit)→complete, then only post-terminal claim→load(miss)→release tails.
    // Every op names the ONE owner lane (`shard_index_of(id, 4)`) except the park, which
    // runs on its arrival lane.
    let owner = shard_index_of(&id, SHARDS);
    let ops: Vec<String> = state
        .lock()
        .expect("state")
        .trace
        .iter()
        .filter(|t| !t.starts_with("park:"))
        .cloned()
        .collect();
    let expected_head = [
        format!("claim:{id}@{owner}"),
        format!("load:{id}:hit@{owner}"),
        format!("complete:{id}@{owner}"),
    ];
    assert_eq!(
        &ops[..3],
        &expected_head[..],
        "the winning resume runs first and alone on the owner lane"
    );
    let tail = &ops[3..];
    assert_eq!(
        tail.len() % 3,
        0,
        "tails are whole claim/load/release triples"
    );
    for triple in tail.chunks(3) {
        assert_eq!(
            triple,
            &[
                format!("claim:{id}@{owner}"),
                format!("load:{id}:miss@{owner}"),
                format!("release:{id}@{owner}"),
            ],
            "a losing resume observes only the post-terminal miss — never interleaving"
        );
    }
    {
        let trace = &state.lock().expect("state").trace;
        let park = trace
            .iter()
            .find(|t| t.starts_with("park:"))
            .expect("the spawn parked")
            .clone();
        let lane: u32 = park
            .rsplit('@')
            .next()
            .and_then(|l| l.parse().ok())
            .expect("the park names its actor lane (sutra-channel-engine-<i>)");
        assert!(lane < SHARDS);
    }

    // The audit-seq witness: park events (arrival lane's listener) and resume events
    // (owner lane's listener, seeded from the snapshot) form ONE strictly-increasing
    // per-instance sequence — a duplicate or a reset would betray an interleaved resume
    // or a broken seed.
    let mut seqs = Vec::new();
    while let Ok(event) = audit_rx.try_recv() {
        if event.instance_id == id {
            seqs.push(event.seq);
        }
    }
    assert!(
        seqs.len() >= 3,
        "started/suspended at park + resume events arrived: {seqs:?}"
    );
    assert!(
        seqs.windows(2).all(|w| w[1] > w[0]),
        "the cross-lane audit seq is strictly monotonic: {seqs:?}"
    );
    assert_eq!(seqs[0], 1, "the sequence starts at 1");
    assert_eq!(*seqs.last().expect("nonempty") as usize, seqs.len());

    // §6.1 counters: one park, one committed resume, zero claim bounces; queues drained.
    assert_eq!(sum_metric(&handle, |l| l.parks.load(Ordering::Relaxed)), 1);
    assert_eq!(
        sum_metric(&handle, |l| l.resumes.load(Ordering::Relaxed)),
        1
    );
    assert_eq!(
        sum_metric(&handle, |l| l.claim_bounce_relay.load(Ordering::Relaxed))
            + sum_metric(&handle, |l| l.claim_bounce_timer.load(Ordering::Relaxed)),
        0
    );
    for lane in handle.shard_metrics().lanes() {
        assert_eq!(lane.queue_depth.load(Ordering::Relaxed), 0);
    }
}

// =======================================================================================
// 2. The live handoff: resolved on a non-owner lane, executed on the owner lane
// =======================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_lane_relay_hands_off_and_resumes_on_the_owner_lane() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let handle = spawn_router(
        LaneParts {
            state: Arc::clone(&state),
            deferred: None,
            audit_tx: None,
            park_gate: None,
            with_v2_flow: false,
        },
        None,
    );

    // Park + relay, sequentially, until an iteration's relay ARRIVES on a lane that does
    // not own the instance — the handoff counter moving is the proof the §1.1 path ran.
    // Arrival lanes rotate round-robin and instance ids hash uniformly, so a cross-lane
    // hit is a 3/4-per-iteration event; 20 misses in a row is a 4^-20 impossibility.
    let mut observed_handoff = false;
    for i in 0..20 {
        let key = format!("K-{i}");
        let id = completed_instance(
            handle
                .dispatch(inbound("start-in", key.as_bytes(), &format!("h-s{i}")))
                .await
                .expect("spawn parks"),
        );
        let before = sum_metric(&handle, |l| l.handoffs.load(Ordering::Relaxed));
        let resumed = completed_instance(
            handle
                .dispatch(inbound("relay-in", key.as_bytes(), &format!("h-r{i}")))
                .await
                .expect("relay resumes to completion"),
        );
        assert_eq!(resumed, id, "the relay resumed its own instance");
        // Whether it hopped or resolved on the owner lane, the resume COMMITTED on the
        // owner lane — the trace's complete entry names it.
        let owner = shard_index_of(&id, SHARDS);
        assert!(
            state
                .lock()
                .expect("state")
                .trace
                .contains(&format!("complete:{id}@{owner}")),
            "the terminal commit ran on the owner lane"
        );
        if sum_metric(&handle, |l| l.handoffs.load(Ordering::Relaxed)) > before {
            observed_handoff = true;
            break;
        }
    }
    assert!(
        observed_handoff,
        "20 park/relay rounds never crossed lanes — the handoff path did not run"
    );
}

// =======================================================================================
// 3. Flip under load at N=4: the await-all barrier + pinned resumes survive
// =======================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flip_under_load_at_n4_leaves_no_lane_half_flipped_and_pinned_resumes_survive() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let v1 = LaneParts {
        state: Arc::clone(&state),
        deferred: None,
        audit_tx: None,
        park_gate: None,
        with_v2_flow: false,
    };
    let v2 = LaneParts {
        with_v2_flow: true,
        ..v1.clone()
    };
    let handle = spawn_router(v1, None);

    // A pre-flip park — must still resume AFTER the flip (the bridge state, like the
    // production store, outlives the engine rebuild).
    let pinned = completed_instance(
        handle
            .dispatch(inbound("start-in", b"K-PIN", "f-s"))
            .await
            .expect("pre-flip spawn parks"),
    );

    // Background load on a channel BOTH builds serve, hammering all lanes through the
    // flip window. Every dispatch must succeed — a lane observed half-flipped would
    // answer RESOLVE_CHANNEL_UNKNOWN.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Signalled once the load task has a dispatch BEHIND it. Without this the test is a race
    // it usually wins rather than an assertion: `tokio::spawn` only queues the task, so on a
    // busy runtime the flip can complete and set `stop` before the task is ever polled, and
    // the load loop then exits on its first condition check having exercised nothing. The
    // window would go untested and the test would still pass — except for the emptiness check
    // below, which is why CI saw it fail rather than pass vacuously.
    let (running_tx, running_rx) = tokio::sync::oneshot::channel();
    let load = {
        let handle = handle.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut running_tx = Some(running_tx);
            let mut results = Vec::new();
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                results.push(
                    handle
                        .dispatch(inbound("flow-in", b"x", &format!("f-l{i}")))
                        .await,
                );
                // First completed dispatch: the load is genuinely in flight, so the flip that
                // follows lands ON it. Send failure means the test task is gone — nothing to
                // report to.
                if let Some(tx) = running_tx.take() {
                    let _ = tx.send(());
                }
                i += 1;
            }
            results
        })
    };
    running_rx
        .await
        .expect("the load task completed a dispatch before the flip");

    // The fan-out flip: one rebuild closure per lane, each preserving the lane identity
    // and its §6.1 counter handle — the deploy.rs shape.
    handle
        .update(|| {
            let parts = v2.clone();
            Box::new(move |engine: &mut ChannelEngine| {
                let shard = engine.shard();
                let metrics = engine.shard_metrics();
                *engine = build_lane(&parts, shard, metrics);
            })
        })
        .await
        .expect("the fan-out flip applies on every lane");

    stop.store(true, Ordering::Relaxed);
    let load_results = load.await.expect("load task");
    assert!(
        !load_results.is_empty(),
        "the load task ran through the flip window"
    );
    for result in load_results {
        completed_instance(result.expect("no dispatch fails across the flip window"));
    }

    // The await-all barrier: update returned ⇒ EVERY lane serves the v2 build. Eight
    // sequential probes rotate the round-robin arrival counter across all four lanes —
    // each must answer on the v2-only channel.
    for i in 0..8 {
        completed_instance(
            handle
                .dispatch(inbound("flow2-in", b"probe", &format!("f-p{i}")))
                .await
                .unwrap_or_else(|d| {
                    panic!(
                        "a lane still serves the pre-flip build after update returned \
                         (probe {i}): [{}] {}",
                        d.code, d.message
                    )
                }),
        );
    }

    // The pinned instance resumes to completion on the post-flip engines.
    let resumed = completed_instance(
        handle
            .dispatch(inbound("relay-in", b"K-PIN", "f-r"))
            .await
            .expect("the pre-flip park resumes after the flip"),
    );
    assert_eq!(resumed, pinned);
}

// =======================================================================================
// 4. The REAL §2.1 deferred-ack race: park on lane A, complete via lane B, ack once
// =======================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_lane_terminal_inside_the_park_window_fires_the_deferred_ack_exactly_once() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let gate = Arc::new(ParkGate::default());
    let handle = spawn_router(
        LaneParts {
            state: Arc::clone(&state),
            deferred: Some(Arc::clone(&registry)),
            audit_tx: None,
            park_gate: Some(Arc::clone(&gate)),
            with_v2_flow: false,
        },
        None,
    );

    // Retry until the instance's OWNER lane differs from its PARKING lane (uniform hash:
    // pass probability 3/4 per round; 25 misses is a 4^-25 impossibility). When they
    // differ, the completion genuinely runs on another OS thread inside A's window.
    let mut raced = false;
    for i in 0..25 {
        let key = format!("R-{i}");
        gate.rearm();
        let (acks, nacks, settle) = settle_probe();
        let in_flight = {
            let handle = handle.clone();
            let key = key.clone();
            tokio::spawn(async move {
                handle
                    .dispatch_deferred(inbound("start-in", key.as_bytes(), &key), settle)
                    .await
            })
        };
        // The parking lane is now HELD inside commit_park's await: registration done
        // (§2.1 inversion), commit visible, dispatch arm not yet returned.
        let (id, parking_lane) = gate.wait_entered().await;
        let owner = shard_index_of(&id, SHARDS);

        if owner == parking_lane {
            // Same lane — the completion would just queue behind the blocked park.
            // Release, settle this instance cleanly, try again.
            gate.release();
            match in_flight.await.expect("dispatch task").expect("parks") {
                DeferredDispatch::Deferred { .. } => {}
                other => panic!("expected Deferred, got {other:?}"),
            }
            handle
                .resume_resolved(resolved_resume(&id))
                .await
                .expect("cleanup resume completes");
            assert_eq!(acks.load(Ordering::SeqCst), 1, "cleanup ack fired");
            continue;
        }

        // THE RACE: complete the instance via the OWNER lane while the parking lane is
        // still inside its commit window. The terminal event must find the registration
        // (registered BEFORE the commit) and fire the broker ack exactly once, NOW.
        let outcome = handle
            .resume_resolved(resolved_resume(&id))
            .await
            .expect("the owner lane resumes inside the window");
        assert_eq!(completed_instance(outcome), id);
        assert_eq!(
            (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
            (1, 0),
            "the in-window terminal fired the deferred ack exactly once"
        );
        assert_eq!(registry.pending_count(), 0, "no dangling registration");

        // Release the parking lane; its arm still answers Deferred — "transport told
        // Deferred ⇒ registered ∧ committed" (the ack has ALREADY settled it).
        gate.release();
        match in_flight.await.expect("dispatch task").expect("parks") {
            DeferredDispatch::Deferred { instance_id } => assert_eq!(instance_id, id),
            other => panic!("expected Deferred, got {other:?}"),
        }
        assert_eq!(
            (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
            (1, 0),
            "nothing double-fires after the parking lane returns"
        );
        raced = true;
        break;
    }
    assert!(
        raced,
        "25 rounds never yielded owner ≠ parking lane — the race was never exercised"
    );
}

/// Atomic-recorder settle callbacks (the broker-source shape).
fn settle_probe() -> (
    Arc<std::sync::atomic::AtomicU32>,
    Arc<std::sync::atomic::AtomicU32>,
    DeferredSettle,
) {
    let acks: Arc<std::sync::atomic::AtomicU32> = Arc::default();
    let nacks: Arc<std::sync::atomic::AtomicU32> = Arc::default();
    let (a, n) = (Arc::clone(&acks), Arc::clone(&nacks));
    (
        acks,
        nacks,
        DeferredSettle {
            ack: Box::new(move || {
                a.fetch_add(1, Ordering::SeqCst);
            }),
            nack: Box::new(move || {
                n.fetch_add(1, Ordering::SeqCst);
            }),
        },
    )
}

// =======================================================================================
// 5. Mis-route injection: a held claim bounces CLAIM_HELD — never interleaving
// =======================================================================================

/// Claim always HELD by someone else; load serves a parked snapshot (the "live claim
/// stands" branch). Trace-recording only.
struct ClaimHeldBridge {
    trace: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for ClaimHeldBridge {
    async fn commit_park(
        &self,
        _d: &DeploymentId,
        _i: &str,
        _s: &SuspendedInstance,
        _a: &[AliasRecord],
        _t: &[TimerWaitRecord],
        _e: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.trace.lock().expect("trace").push("park".into());
        Ok(())
    }
    async fn load(
        &self,
        _d: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        self.trace.lock().expect("trace").push("load".into());
        Ok(Some(SuspendedInstance {
            process_id: "hold".to_string(),
            deployment_id: deployment().value().to_string(),
            status: "SUSPENDED".to_string(),
            suspended: true,
            waiting_nodes: vec!["U".to_string()],
            completed_nodes: vec!["S".to_string()],
            ..Default::default()
        }))
    }
    async fn find_live_alias(
        &self,
        _d: &DeploymentId,
        _n: &str,
        _v: &str,
    ) -> Result<Option<String>, Diagnostic> {
        Ok(None)
    }
    async fn commit_complete(
        &self,
        _d: &DeploymentId,
        _i: &str,
        _e: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.trace.lock().expect("trace").push("complete".into());
        Ok(())
    }
    async fn commit_repark(
        &self,
        _d: &DeploymentId,
        _i: &str,
        _s: &SuspendedInstance,
        _w: &[String],
        _a: &[AliasRecord],
        _t: &[TimerWaitRecord],
        _e: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.trace.lock().expect("trace").push("repark".into());
        Ok(())
    }
    async fn commit_emissions(
        &self,
        _d: &DeploymentId,
        _e: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
    async fn claim_instance(
        &self,
        _d: &DeploymentId,
        _i: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        self.trace.lock().expect("trace").push("claim".into());
        Ok(InstanceClaimOutcome::HeldByOther)
    }
    async fn release_instance(&self, _d: &DeploymentId, _i: &str) -> Result<(), Diagnostic> {
        self.trace.lock().expect("trace").push("release".into());
        Ok(())
    }
    async fn commit_failed(
        &self,
        _d: &DeploymentId,
        _i: &str,
        _c: &str,
        _x: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
}

#[test]
fn a_mis_routed_resume_bounces_claim_held_and_counts_on_the_split_bounce_meters() {
    // The injection: an engine claiming to be lane 1-of-4 handed a resume for an
    // instance whose claim another owner holds — the shard-scoped claim owner (§4) makes
    // exactly this the shape of any real mis-route. The bounce must be visible, the
    // execution zero.
    let trace = Arc::new(Mutex::new(Vec::new()));
    let hold = BpmnModelLoader::new()
        .load(HOLD_BPMN.as_bytes())
        .expect("hold BPMN loads");
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
    .with_binding(ChannelBinding::new(
        "relay-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &hold)
    .with_instance_bridge(Rc::new(ClaimHeldBridge {
        trace: Arc::clone(&trace),
    }) as Rc<dyn InstanceBridge>)
    .with_shard(EngineShard {
        index: 1,
        count: SHARDS,
    })
    .build();
    let metrics = engine.shard_metrics();

    // Relay-path bounce: CLAIM_HELD, retry-safe requeue disposition, nothing executed.
    let error =
        drive(engine.resume_resolved(&resolved_resume("11111111-2222-4333-8444-555555555555")))
            .expect_err("a held claim refuses the resume");
    assert_eq!(error.code, codes::RUNTIME_RESUME_CLAIM_HELD);
    assert_eq!(
        error
            .attributes
            .get("sutra.ackDisposition")
            .map(String::as_str),
        Some("requeue"),
        "the bounce is retry-safe — requeue, never drop"
    );
    assert_eq!(
        *trace.lock().expect("trace"),
        vec!["claim".to_string(), "load".to_string()],
        "claim + the held-vs-vanished re-read only — no resume, no commit, no interleaving"
    );
    assert_eq!(metrics.claim_bounce_relay.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.claim_bounce_timer.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.resumes.load(Ordering::Relaxed), 0);

    // Timer-path bounce: same refusal, the split counter's OTHER leg.
    let error = drive(engine.fire_timer(&sutra_executor::TimerFire {
        deployment: deployment(),
        instance_id: "11111111-2222-4333-8444-555555555555".to_string(),
        node_id: "U".to_string(),
        due_at: "2026-08-06T10:05:00Z".to_string(),
        fired_at: "2026-08-06T10:05:01Z".to_string(),
    }))
    .expect_err("a held claim defers the timer fire");
    assert_eq!(error.code, codes::RUNTIME_RESUME_CLAIM_HELD);
    assert_eq!(metrics.claim_bounce_relay.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.claim_bounce_timer.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.resumes.load(Ordering::Relaxed), 0);
}

// =======================================================================================
// 6. Backpressure: a bounded-capacity N=4 router under burst makes progress
// =======================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bounded_capacity_n4_router_absorbs_a_burst_without_deadlock() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let handle = spawn_router(
        LaneParts {
            state,
            deferred: None,
            audit_tx: None,
            park_gate: None,
            with_v2_flow: false,
        },
        // Capacity 1 per lane: every burst sender beyond the 4 in-flight slots AWAITS on
        // its own task — backpressure lands on callers, never on another lane's loop.
        Some(1),
    );

    let mut tasks = Vec::new();
    for i in 0..100 {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            handle
                .dispatch(inbound("flow-in", b"x", &format!("b-{i}")))
                .await
        }));
    }
    let all = async {
        for task in tasks {
            completed_instance(task.await.expect("burst task").expect("dispatch succeeds"));
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(60), all)
        .await
        .expect("the burst drains without deadlock");

    assert_eq!(
        sum_metric(&handle, |l| l.dispatches.load(Ordering::Relaxed)),
        100,
        "every burst dispatch drained through a lane"
    );
    for lane in handle.shard_metrics().lanes() {
        assert_eq!(
            lane.queue_depth.load(Ordering::Relaxed),
            0,
            "queues drained"
        );
    }
}

// =======================================================================================
// 7. A lane killed by a BUILD panic is reported dead; the rest keep serving
// =======================================================================================

/// The initial engine build runs uncontained on the lane's own thread (unlike dispatches
/// and flip closures, which are panic-contained) — a panic there kills the lane before its
/// loop ever starts. From then on, work hashed to the dead lane answers
/// `SUTRA.RUNTIME.UNEXPECTED — engine actor is not running` while the process otherwise
/// looks healthy: the zombie replica a k8s IT once ran against for 14 minutes.
/// [`EngineHandle::dead_lanes`] is the health surface's probe for exactly this state —
/// the engine folds it into `/sutra/health/live` (restart) and `/ready` (stop routing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lane_killed_by_a_build_panic_is_reported_dead_and_the_rest_keep_serving() {
    let state = Arc::new(Mutex::new(BridgeState::default()));
    let parts = LaneParts {
        state: Arc::clone(&state),
        deferred: None,
        audit_tx: None,
        park_gate: None,
        with_v2_flow: false,
    };
    let handle = spawn_engine_sharded(
        SHARDS,
        None,
        tokio::runtime::Handle::current(),
        move |shard, metrics| {
            if shard.index == 1 {
                panic!("injected lane-1 build failure (the boot-replay death mode)");
            }
            build_lane(&parts, shard, metrics)
        },
    );

    // The build panic lands asynchronously on the lane's own thread — wait for the death.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while handle.dead_lanes().is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(handle.dead_lanes(), vec![1], "exactly lane 1 is dead");

    // Work hashed to the dead lane answers the actor-gone diagnostic...
    let dead_key = (0..)
        .map(|i| format!("inst-{i}"))
        .find(|id| shard_index_of(id, SHARDS) == 1)
        .expect("an id hashing to lane 1");
    let error = handle
        .resume_resolved(resolved_resume(&dead_key))
        .await
        .expect_err("the dead lane cannot serve");
    assert_eq!(error.code, codes::RUNTIME_UNEXPECTED);

    // ...while a LIVE lane still serves engine logic (any engine-level answer — here the
    // unknown-instance refusal — proves the lane's loop is running, not gone).
    let live_key = (0..)
        .map(|i| format!("inst-{i}"))
        .find(|id| shard_index_of(id, SHARDS) == 0)
        .expect("an id hashing to lane 0");
    let error = handle
        .resume_resolved(resolved_resume(&live_key))
        .await
        .expect_err("no such instance exists on the live lane");
    assert_ne!(
        error.code,
        codes::RUNTIME_UNEXPECTED,
        "lane 0 answered from a running engine: {}",
        error.code
    );
}
