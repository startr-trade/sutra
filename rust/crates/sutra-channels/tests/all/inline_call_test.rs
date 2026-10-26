//! Synchronous outbound calls (`ack-mode: on-complete` outbound channels), driven through the
//! router exactly as a transport drives them: `spawn_engine` + `EngineHandle::dispatch`, with a
//! fake [`InlineCaller`] standing in for the transport. The synchronous calls declare no
//! `<q:alias>`: nothing correlates their answers.
//!
//! The properties pinned here are the ones only the dispatcher can break: a fully synchronous
//! chain writes NOTHING durable; parallel calls are in flight together while their answers are
//! applied one at a time; a chain that ends at a durable park commits everything it carried in
//! ONE step; the reply answers the ORIGINAL request; timeouts, retries and failures take the
//! task's paths without ever parking a durable backoff; a failed chain releases its inbox
//! dedup row; an in-flight chain holds its channel's admission slot; and a broadcast fan-out
//! degrades to the async path; and relay- and timer-driven resumes run their chains over the
//! stored instance under its claim.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    spawn_engine, AliasRecord, BoxFuture, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic,
    DispatchOutcome, DrainingSink, EngineHandle, InMemoryConcurrencyStore, InMemoryInboxStore,
    InboundChain, InboundMessage, InlineCaller, InlineCallerRegistry, InlineFailure,
    InlineResponse, InstanceBridge, InstanceClaimOutcome, Namespace, OutboundMessage,
    OutboxEmission, ScheduledStartFire, ScheduledStartOutcome, SuspendedInstance, TimerWaitRecord,
    ValidatorRegistry,
};
use sutra_executor::{
    DeploymentId, OutboundChannelRegistry, ResolvedOutboundChannel, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

const TENANT: &str = "acme";
const TIMEOUT_CODE: &str = "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT";
const INSTANCE: &str = "33333333-1111-4111-8111-111111111111";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "calls", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000c1").expect("valid deployment id")
}

/// start(`start-in`) → sync call `A` on `svc` → reply task → end.
const SINGLE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="single">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT0.5S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="R"/>
    <bpmn:sequenceFlow id="f3" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start → fork → sync calls `A` and `B` in parallel → join → reply → end.
const PARALLEL: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="parallel">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:parallelGateway id="Fork"/>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="B" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="b"/>
        <q:alias name="bKey" expression="event.dedupKey"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:parallelGateway id="Join"/>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="B"/>
    <bpmn:sequenceFlow id="f4" sourceRef="A" targetRef="Join"/>
    <bpmn:sequenceFlow id="f5" sourceRef="B" targetRef="Join"/>
    <bpmn:sequenceFlow id="f6" sourceRef="Join" targetRef="R"/>
    <bpmn:sequenceFlow id="f7" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start(unique alias) → send (async, carried) → sync call `A` → user task (durable wait) → end.
const THEN_PARK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="thenpark">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="e2eId" expression="event.dedupKey" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sendTask id="Notify">
      <bpmn:extensionElements>
        <q:send destination="notify://ops/started"/>
      </bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="decision"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="U"/>
    <bpmn:sequenceFlow id="f4" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// The single-call shape with an inline `<q:retry>` policy.
const RETRIED: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="retried">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
        <q:retry maxAttempts="3" initialDelay="PT0.01S" backoffCoefficient="1.0"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="R"/>
    <bpmn:sequenceFlow id="f3" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A timer-started process whose first step is a sync call.
const SCHEDULED: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="scheduled">
    <bpmn:startEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="T" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A conversation turn: start → wait for a turn (relay) → sync call → end.
const RELAY_THEN_CALL: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="header.conv" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// The same turn, followed by the NEXT turn's wait: the chain ends at a durable re-park.
const RELAY_THEN_CALL_THEN_WAIT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="header.conv" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="Next">
      <bpmn:extensionElements><q:source channel="relay-next" name="turn2"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="Next"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Next" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start → timer catch → sync call → end: the timer poller's fire drives the chain.
const TIMER_THEN_CALL: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="later">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// Two processes subscribed to one broadcast channel, each starting with a sync call.
const BROADCAST: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="fan1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:alias name="aKey" expression="'one'"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="E"/>
  </bpmn:process>
  <bpmn:process id="fan2">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:alias name="aKey" expression="'two'"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

// ---- the fake caller ----------------------------------------------------------------------

type Script =
    dyn Fn(usize) -> BoxFuture<'static, Result<InlineResponse, InlineFailure>> + Send + Sync;

/// An inline caller for the `fake` scheme whose answer to the n-th call (0-based) is
/// `script(n)`; it records every request and the peak number of calls in flight at once.
struct FakeCaller {
    script: Box<Script>,
    calls: AtomicUsize,
    in_flight: Arc<AtomicUsize>,
    peak: AtomicUsize,
    requests: Mutex<Vec<OutboundMessage>>,
    /// Pieces a STREAMED call emits before answering.
    chunks: Mutex<Vec<String>>,
    /// How many calls were made through `call_streaming`.
    streamed: AtomicUsize,
}

impl FakeCaller {
    fn new(
        script: impl Fn(usize) -> BoxFuture<'static, Result<InlineResponse, InlineFailure>>
            + Send
            + Sync
            + 'static,
    ) -> Arc<FakeCaller> {
        Arc::new(FakeCaller {
            script: Box::new(script),
            calls: AtomicUsize::new(0),
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            chunks: Mutex::new(Vec::new()),
            streamed: AtomicUsize::new(0),
        })
    }

    /// A caller that streams `chunks` before each streamed answer (`"<reset>"` streams a reset).
    fn streaming(self: Arc<Self>, chunks: &[&str]) -> Arc<FakeCaller> {
        *self.chunks.lock().unwrap() = chunks.iter().map(|c| c.to_string()).collect();
        self
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl InlineCaller for FakeCaller {
    fn schemes(&self) -> Vec<String> {
        vec!["fake".to_string()]
    }

    fn call<'a>(
        &'a self,
        request: &'a OutboundMessage,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let answer = (self.script)(n);
        let in_flight = Arc::clone(&self.in_flight);
        Box::pin(async move {
            let result = answer.await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        })
    }

    fn call_streaming<'a>(
        &'a self,
        request: &'a OutboundMessage,
        on_chunk: sutra_channels::InlineChunkSink,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        self.streamed.fetch_add(1, Ordering::SeqCst);
        for chunk in self.chunks.lock().unwrap().iter() {
            on_chunk(if chunk == "<reset>" {
                sutra_channels::InlineChunk::Reset
            } else {
                sutra_channels::InlineChunk::Text(chunk)
            });
        }
        self.call(request)
    }
}

fn json(body: &str) -> Result<InlineResponse, InlineFailure> {
    Ok(InlineResponse {
        body: body.as_bytes().to_vec(),
        content_type: Some("application/json".to_string()),
        headers: BTreeMap::new(),
    })
}

fn retryable(code: &str) -> Result<InlineResponse, InlineFailure> {
    Err(InlineFailure::Retryable(Diagnostic::error(
        code,
        "upstream unavailable",
    )))
}

// ---- the bridge: every call is a durable write (or read) the test can see -----------------

/// One `commit_park`: snapshot, alias rows, timer rows, outbox rows.
type ObservedPark = (
    SuspendedInstance,
    Vec<AliasRecord>,
    Vec<TimerWaitRecord>,
    Vec<OutboxEmission>,
);

#[derive(Default)]
struct Observed {
    /// Every bridge call by name, in order.
    trace: Vec<String>,
    parks: Vec<ObservedPark>,
    committed_emissions: Vec<OutboxEmission>,
    /// The one stored instance (relay/timer tests); every lookup and claim answers for it.
    stored: Option<SuspendedInstance>,
    /// Each `commit_repark`: the new snapshot and the waits it resolved.
    reparks: Vec<(SuspendedInstance, Vec<String>)>,
    /// In-flight marker writes, apart from `trace`: `begin <keys>` / `end`.
    markers: Vec<String>,
}

struct RecordingBridge(Arc<Mutex<Observed>>);

impl RecordingBridge {
    fn note(&self, call: &str) {
        self.0.lock().unwrap().trace.push(call.to_string());
    }
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for RecordingBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.note("commit_park");
        self.0.lock().unwrap().stored = Some(snapshot.clone());
        self.0.lock().unwrap().parks.push((
            snapshot.clone(),
            aliases.to_vec(),
            timer_waits.to_vec(),
            emissions.to_vec(),
        ));
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        self.note("load");
        Ok(self.0.lock().unwrap().stored.clone())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        _name: &str,
        _value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        // A read, not a write — but recorded so "nothing touched the store" can be asserted.
        self.note("find_live_alias");
        Ok(self
            .0
            .lock()
            .unwrap()
            .stored
            .as_ref()
            .map(|_| INSTANCE.to_string()))
    }

    async fn claim_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        self.note("claim_instance");
        Ok(InstanceClaimOutcome::Granted)
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<(), Diagnostic> {
        self.note("release_instance");
        Ok(())
    }

    async fn begin_inline_chain(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        dedup_keys: &[(String, String)],
    ) -> Result<(), Diagnostic> {
        self.0
            .lock()
            .unwrap()
            .markers
            .push(format!("begin {dedup_keys:?}"));
        Ok(())
    }

    async fn end_inline_chain(&self, _deployment: &DeploymentId, _instance_id: &str) {
        self.0.lock().unwrap().markers.push("end".to_string());
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.note("commit_complete");
        self.0.lock().unwrap().stored = None;
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.note("commit_repark");
        let mut observed = self.0.lock().unwrap();
        observed
            .reparks
            .push((snapshot.clone(), satisfied_wait_nodes.to_vec()));
        observed.stored = Some(snapshot.clone());
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.note("commit_emissions");
        self.0
            .lock()
            .unwrap()
            .committed_emissions
            .extend(emissions.iter().cloned());
        Ok(())
    }

    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        self.note("commit_failed");
        Ok(())
    }
}

/// A reply task's output: `responseObject = {"said": <what>}`.
fn said(what: &str) -> FeelValue {
    FeelValue::Map(BTreeMap::from([(
        "responseObject".to_string(),
        FeelValue::Map(BTreeMap::from([(
            "said".to_string(),
            FeelValue::String(what.to_string()),
        )])),
    )]))
}

// ---- the engine ---------------------------------------------------------------------------

struct Rig {
    handle: EngineHandle,
    observed: Arc<Mutex<Observed>>,
}

/// One engine lane serving `bpmn`, with `svc` a SYNCHRONOUS outbound channel served by
/// `caller`, a json start channel, and (when `cap` is set) a concurrency cap on it.
fn rig(bpmn: &'static str, caller: Arc<FakeCaller>, cap: Option<u32>) -> Rig {
    rig_over(bpmn, caller, cap, None)
}

/// [`rig`] over an instance already STORED (parked) — the relay and timer tests.
fn rig_over(
    bpmn: &'static str,
    caller: Arc<FakeCaller>,
    cap: Option<u32>,
    stored: Option<SuspendedInstance>,
) -> Rig {
    let observed = Arc::new(Mutex::new(Observed {
        stored,
        ..Observed::default()
    }));
    let bridge_state = Arc::clone(&observed);
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        build_engine(bpmn, caller, cap, bridge_state, None, None)
    });
    Rig { handle, observed }
}

/// [`rig`] with `start-in` drawing from the capacity pool `lines` held in `pools`.
fn pooled_rig(
    bpmn: &'static str,
    caller: Arc<FakeCaller>,
    pools: Arc<sutra_channels::InMemoryCapacityPools>,
) -> Rig {
    let observed = Arc::new(Mutex::new(Observed::default()));
    let bridge_state = Arc::clone(&observed);
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        build_engine(bpmn, caller, None, bridge_state, None, Some(pools))
    });
    Rig { handle, observed }
}

/// An HTTP front over the engine: `yaml` declares the inbound channels (bound to the test
/// deployment); the engine serves them, and `svc` stays the synchronous outbound channel.
struct HttpRig {
    app: axum::Router,
    handle: EngineHandle,
    observed: Arc<Mutex<Observed>>,
}

fn http_rig(
    bpmn: &'static str,
    caller: Arc<FakeCaller>,
    yaml: &'static str,
    stored: Option<SuspendedInstance>,
) -> HttpRig {
    let mut defs = sutra_channels::load_channel_definitions(
        yaml.as_bytes(),
        TENANT,
        "calls",
        "1.0.0",
        "channels.yaml",
    )
    .expect("channels load");
    for def in &mut defs {
        def.binding.deployment = deployment();
    }
    let observed = Arc::new(Mutex::new(Observed {
        stored,
        ..Observed::default()
    }));
    let bridge_state = Arc::clone(&observed);
    let engine_defs = defs.clone();
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        build_engine(bpmn, caller, None, bridge_state, Some(engine_defs), None)
    });
    let app = sutra_channels::channel_router(&defs, handle.clone()).expect("router builds");
    HttpRig {
        app,
        handle,
        observed,
    }
}

struct HttpReply {
    status: u16,
    content_type: String,
    body: String,
}

async fn post(rig: &HttpRig, path: &str, conv: &str, body: &str) -> HttpReply {
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json")
        .header("X-Api-Key", "k")
        .header("conv", conv)
        .body(axum::body::Body::from(body.to_string()))
        .expect("request builds");
    let response = rig.app.clone().oneshot(request).await.expect("response");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    HttpReply {
        status,
        content_type,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn build_engine(
    bpmn: &str,
    caller: Arc<FakeCaller>,
    cap: Option<u32>,
    bridge_state: Arc<Mutex<Observed>>,
    defs: Option<Vec<sutra_channels::ChannelDefinition>>,
    pools: Option<Arc<sutra_channels::InMemoryCapacityPools>>,
) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
        &deployment(),
        ResolvedOutboundChannel::resolve("svc", "fake", "fake://svc/op", None, None, None, "none")
            .with_sync(true),
    );
    // The reply task answers with whatever the calls brought back.
    let tasks = TaskRegistry::new()
        .register("reply", |input, _ctx| {
            let FeelValue::Map(scope) = input else {
                return Ok(FeelValue::Null);
            };
            let mut answer = BTreeMap::new();
            for key in ["a", "b", "c"] {
                if let Some(v) = scope.get(key) {
                    answer.insert(key.to_string(), v.clone());
                }
            }
            Ok(FeelValue::Map(BTreeMap::from([(
                "responseObject".to_string(),
                FeelValue::Map(answer),
            )])))
        })
        .register("say-first", |_, _| Ok(said("first")))
        .register("say-second", |_, _| Ok(said("second")));
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_outbound_channels(outbound)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut callers = InlineCallerRegistry::new();
    callers.register(caller as Arc<dyn InlineCaller>);
    let mut start = ChannelBinding::new("start-in", namespace(), deployment(), "json");
    start.max_concurrent_instances = cap;
    start.broadcast = bpmn == BROADCAST;
    if pools.is_some() {
        start.pool = Some("lines".to_string());
    }
    let builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    let builder = match defs {
        Some(defs) => builder.with_channel_definitions(&defs),
        None => builder
            .with_binding(start)
            .with_binding(ChannelBinding::new(
                "relay-in",
                namespace(),
                deployment(),
                "json",
            )),
    };
    let builder = match pools {
        Some(pools) => {
            builder.with_capacity_pools(pools as Arc<dyn sutra_channels::CapacityPoolStore>)
        }
        None => builder,
    };
    builder
        .with_module(&deployment(), &module)
        .with_instance_bridge(Rc::new(RecordingBridge(bridge_state)) as Rc<dyn InstanceBridge>)
        .with_inbox(Rc::new(InMemoryInboxStore::new()))
        .with_alias_store(Rc::new(sutra_channels::InMemoryAliasStore::new()))
        .with_concurrency_store(Rc::new(InMemoryConcurrencyStore::new()))
        .with_inline_callers(callers)
        .build()
}

fn start(key: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: "start-in".to_string(),
        headers: BTreeMap::new(),
        body: br#"{"order":"o-1"}"#.to_vec().into(),
        content_type: Some("application/json".to_string()),
        idempotency_key: key.to_string(),
        explicit_event_id: true,
        received_at: "2026-10-24T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn reply_json(outcome: &DispatchOutcome) -> serde_json::Value {
    let DispatchOutcome::Completed {
        reply: Some(reply), ..
    } = outcome
    else {
        panic!("expected a completed dispatch with a reply, got {outcome:?}");
    };
    serde_json::from_slice(reply.body.get()).expect("the reply is json")
}

fn trace(rig: &Rig) -> Vec<String> {
    rig.observed.lock().unwrap().trace.clone()
}

// ---- the chain ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_call_completes_without_any_store_write_and_answers_the_original_request() {
    let caller = FakeCaller::new(|_| Box::pin(async { json(r#"{"v":7}"#) }));
    let rig = rig(SINGLE, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the chain completes");

    // The flow's reply rides the ORIGINAL request, carrying what the call answered.
    assert_eq!(reply_json(&outcome), serde_json::json!({"a": {"v": 7}}));
    assert_eq!(caller.calls(), 1);
    // The request went out as the outbox would have sent it: to the channel's destination.
    assert_eq!(
        caller.requests.lock().unwrap()[0].destination,
        "fake://svc/op"
    );
    // Not one bridge call — no park, no outbox row, no completion step.
    assert!(
        trace(&rig).is_empty(),
        "the chain touched the store: {:?}",
        trace(&rig)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_sync_calls_are_in_flight_together_and_both_answers_land() {
    // Each call waits for the other to start: serial calls would never get past the barrier
    // and would time out instead.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let caller = FakeCaller::new(move |n| {
        let barrier = Arc::clone(&barrier);
        Box::pin(async move {
            barrier.wait().await;
            json(&format!(r#"{{"call":{n}}}"#))
        })
    });
    let rig = rig(PARALLEL, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the chain completes");

    assert_eq!(
        caller.peak.load(Ordering::SeqCst),
        2,
        "both calls were in flight at once"
    );
    let reply = reply_json(&outcome);
    let mut answers = vec![reply["a"]["call"].clone(), reply["b"]["call"].clone()];
    answers.sort_by_key(|v| v.as_u64());
    assert_eq!(answers, vec![serde_json::json!(0), serde_json::json!(1)]);
    assert!(trace(&rig).is_empty(), "{:?}", trace(&rig));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_call_then_a_durable_wait_commits_everything_in_one_park() {
    let caller = FakeCaller::new(|_| Box::pin(async { json(r#"{"v":1}"#) }));
    let rig = rig(THEN_PARK, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the chain parks");
    assert!(
        matches!(outcome, DispatchOutcome::Completed { reply: None, .. }),
        "{outcome:?}"
    );

    let observed = rig.observed.lock().unwrap();
    // One durable write for the whole chain (the unique-alias pre-check is a read).
    assert_eq!(observed.trace, vec!["find_live_alias", "commit_park"]);
    let (snapshot, aliases, timers, emissions) = &observed.parks[0];
    assert_eq!(snapshot.waiting_nodes, vec!["U".to_string()]);
    assert!(snapshot.completed_nodes.contains(&"A".to_string()));
    assert!(
        snapshot.variables.iter().any(|(n, _)| n == "a"),
        "the call's answer is in the parked state"
    );
    // The start's alias rides the park; the sync call needed none.
    assert_eq!(
        aliases
            .iter()
            .map(|a| (a.name.as_str(), a.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("e2eId", "k-1")]
    );
    // The carried async send commits with the park; the sync request never becomes a row.
    assert_eq!(emissions.len(), 1, "{emissions:?}");
    assert_eq!(emissions[0].destination, "notify://ops/started");
    // The answered call's timeout row is gone with it.
    assert!(
        timers.iter().all(|t| !t.node_id.starts_with("A")),
        "{timers:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_call_that_never_answers_fires_its_timeout_boundary() {
    let caller = FakeCaller::new(|_| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            json("{}")
        })
    });
    let rig = rig(SINGLE, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("handled failure");
    // The route-less `<q:timeout>` boundary raises the timeout error at the task; uncaught, a
    // non-idempotent flow is dead-lettered (nothing to stamp FAILED — nothing was persisted).
    let DispatchOutcome::DeadLettered {
        cause_code, detail, ..
    } = &outcome
    else {
        panic!("expected the timeout to fail the flow, got {outcome:?}");
    };
    assert_eq!(cause_code, "SUTRA.RUNTIME.ERROR.UNCAUGHT");
    assert!(
        detail.contains(TIMEOUT_CODE),
        "the boundary's timeout error: {detail}"
    );
    assert!(trace(&rig).is_empty(), "{:?}", trace(&rig));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retryable_failure_is_retried_inline_and_succeeds() {
    let caller = FakeCaller::new(|n| {
        Box::pin(async move {
            if n < 2 {
                retryable("UPSTREAM_BUSY")
            } else {
                json(r#"{"v":3}"#)
            }
        })
    });
    let rig = rig(RETRIED, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the third attempt answers");
    assert_eq!(caller.calls(), 3);
    assert_eq!(reply_json(&outcome), serde_json::json!({"a": {"v": 3}}));
    // Retried inside the chain: no durable backoff park.
    assert!(trace(&rig).is_empty(), "{:?}", trace(&rig));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exhausted_inline_retry_takes_the_failure_path_without_a_backoff_park() {
    let caller = FakeCaller::new(|_| Box::pin(async { retryable("UPSTREAM_BUSY") }));
    let rig = rig(RETRIED, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("handled failure");
    assert_eq!(caller.calls(), 3, "maxAttempts bounds the inline attempts");
    let DispatchOutcome::DeadLettered {
        cause_code, detail, ..
    } = &outcome
    else {
        panic!("expected the failure path, got {outcome:?}");
    };
    assert_eq!(cause_code, "SUTRA.RUNTIME.RETRY.EXHAUSTED");
    assert!(detail.contains("UPSTREAM_BUSY"), "{detail}");
    assert!(
        trace(&rig).is_empty(),
        "never a durable backoff: {:?}",
        trace(&rig)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_permanent_failure_fails_the_flow_and_releases_the_dedup_row() {
    let caller = FakeCaller::new(|n| {
        Box::pin(async move {
            if n == 0 {
                Err(InlineFailure::Permanent(Diagnostic::error(
                    "UPSTREAM_REJECTED",
                    "422 from the counterpart",
                )))
            } else {
                json(r#"{"v":2}"#)
            }
        })
    });
    let rig = rig(SINGLE, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("handled failure");
    let DispatchOutcome::DeadLettered { cause_code, .. } = &outcome else {
        panic!("expected the failure path, got {outcome:?}");
    };
    assert_eq!(cause_code, "UPSTREAM_REJECTED");
    assert_eq!(caller.calls(), 1, "a permanent failure is never retried");

    // Nothing of the failed chain was persisted, so its dedup row was released: the client's
    // retry of the SAME event id runs again rather than being swallowed as a duplicate.
    let retry = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the retry runs");
    assert_eq!(reply_json(&retry), serde_json::json!({"a": {"v": 2}}));
    // …while a completed chain keeps its row.
    let duplicate = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("dedup answers");
    assert!(
        matches!(duplicate, DispatchOutcome::Duplicate),
        "{duplicate:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_in_flight_chain_holds_its_admission_slot_until_it_ends() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::clone(&gate);
    let caller = FakeCaller::new(move |n| {
        let gate = Arc::clone(&opened);
        Box::pin(async move {
            if n == 0 {
                gate.notified().await;
            }
            json(r#"{"v":1}"#)
        })
    });
    let rig = rig(SINGLE, Arc::clone(&caller), Some(1));

    let first = {
        let handle = rig.handle.clone();
        tokio::spawn(async move { handle.dispatch(start("k-1")).await })
    };
    // Wait until the first chain's call is in flight.
    for _ in 0..200 {
        if caller.calls() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(caller.calls(), 1);

    let refused = rig
        .handle
        .dispatch(start("k-2"))
        .await
        .expect_err("the cap of 1 is held by the in-flight chain");
    assert_eq!(
        refused.code, "SUTRA.INBOUND.CHANNEL_AT_CAPACITY",
        "{refused:?}"
    );

    gate.notify_one();
    first.await.unwrap().expect("the first chain completes");
    // The completed chain freed its slot.
    let third = rig
        .handle
        .dispatch(start("k-3"))
        .await
        .expect("admitted again");
    assert_eq!(reply_json(&third), serde_json::json!({"a": {"v": 1}}));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_in_flight_chain_holds_its_pool_slot_until_it_ends() {
    use sutra_channels::CapacityPoolStore;
    let gate = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::clone(&gate);
    let caller = FakeCaller::new(move |n| {
        let gate = Arc::clone(&opened);
        Box::pin(async move {
            if n == 0 {
                gate.notified().await;
            }
            json(r#"{"v":1}"#)
        })
    });
    let pools = Arc::new(sutra_channels::InMemoryCapacityPools::new([
        sutra_channels::CapacityPool::new("lines", 1),
    ]));
    let rig = pooled_rig(SINGLE, Arc::clone(&caller), Arc::clone(&pools));

    let first = {
        let handle = rig.handle.clone();
        tokio::spawn(async move { handle.dispatch(start("k-1")).await })
    };
    for _ in 0..200 {
        if caller.calls() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(caller.calls(), 1);
    // The chain took the slot at its start, before anything was stored.
    assert_eq!(pools.usage("lines").await, (1, 0));

    let refused = rig
        .handle
        .dispatch(start("k-2"))
        .await
        .expect_err("the pool of 1 is held by the in-flight chain");
    assert_eq!(refused.code, "SUTRA.INBOUND.POOL.EXHAUSTED", "{refused:?}");

    gate.notify_one();
    first.await.unwrap().expect("the first chain completes");
    assert_eq!(
        pools.usage("lines").await,
        (0, 0),
        "the completed chain freed it"
    );
    rig.handle
        .dispatch(start("k-3"))
        .await
        .expect("admitted again");
    assert_eq!(pools.usage("lines").await, (0, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_start_drives_its_sync_call_inline() {
    let caller = FakeCaller::new(|_| Box::pin(async { json("{}") }));
    let rig = rig(SCHEDULED, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .start_scheduled(ScheduledStartFire {
            deployment: deployment(),
            tenant: TENANT.to_string(),
            module_key: namespace().module_key(),
            process_id: "scheduled".to_string(),
            node_id: "T".to_string(),
            due_at: "2026-10-24T10:00:00Z".to_string(),
            fired_at: "2026-10-24T10:00:00Z".to_string(),
        })
        .await
        .expect("the schedule fires");
    assert!(
        matches!(outcome, ScheduledStartOutcome::Started { .. }),
        "{outcome:?}"
    );
    // The poller's task is the chain's driver, as an inbound caller is for a start.
    assert_eq!(caller.calls(), 1);
    assert!(trace(&rig).is_empty(), "{:?}", trace(&rig));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broadcast_fan_out_degrades_its_sync_calls_to_the_async_path() {
    let caller = FakeCaller::new(|_| Box::pin(async { json("{}") }));
    let rig = rig(BROADCAST, Arc::clone(&caller), None);

    rig.handle
        .dispatch(start("k-1"))
        .await
        .expect("both handlers park");
    // A fan-out answers with its last handler's outcome, so no handler may hold a chain: each
    // call goes out through the outbox with its handler's durable park.
    assert_eq!(caller.calls(), 0);
    let observed = rig.observed.lock().unwrap();
    assert_eq!(observed.parks.len(), 2);
    for (snapshot, _, timers, emissions) in &observed.parks {
        assert_eq!(snapshot.waiting_nodes, vec!["A".to_string()]);
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].destination, "fake://svc/op");
        assert!(
            timers.iter().any(|t| t.node_id == "A#timeout"),
            "{timers:?}"
        );
    }
}

// ---- chains over a STORED instance (relay and timer driven) -------------------------------

fn parked(process_id: &str, waiting: &str) -> SuspendedInstance {
    SuspendedInstance {
        process_id: process_id.to_string(),
        deployment_id: deployment().value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        waiting_nodes: vec![waiting.to_string()],
        key_id: TENANT.to_string(),
        ..Default::default()
    }
}

fn turn(key: &str) -> InboundMessage {
    InboundMessage {
        channel: "relay-in".to_string(),
        headers: BTreeMap::from([("conv".to_string(), "c-1".to_string())]),
        body: br#"{"say":"hello"}"#.to_vec().into(),
        ..start(key)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_turn_makes_its_sync_call_inline_and_completes_the_stored_instance() {
    let caller = FakeCaller::new(|_| Box::pin(async { json(r#"{"answer":"hi"}"#) }));
    let rig = rig_over(
        RELAY_THEN_CALL,
        Arc::clone(&caller),
        None,
        Some(parked("turns", "U")),
    );

    let outcome = rig
        .handle
        .dispatch(turn("t-1"))
        .await
        .expect("the turn completes");
    assert!(
        matches!(outcome, DispatchOutcome::Completed { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        caller.calls(),
        1,
        "the call was made inline, not through the outbox"
    );
    // Claimed once for the whole chain; the terminal step is the only write (it releases the
    // claim in its own transaction) — no intermediate re-park for the call.
    assert_eq!(
        trace(&rig),
        vec![
            "find_live_alias",
            "claim_instance",
            "load",
            "commit_complete"
        ]
    );
    let observed = rig.observed.lock().unwrap();
    assert_eq!(
        observed.markers,
        vec![
            r#"begin [("relay-in", "t-1")]"#.to_string(),
            "end".to_string()
        ],
        "the in-flight marker brackets the chain"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_turn_that_reaches_the_next_wait_re_parks_once() {
    let caller = FakeCaller::new(|_| Box::pin(async { json(r#"{"answer":"hi"}"#) }));
    let rig = rig_over(
        RELAY_THEN_CALL_THEN_WAIT,
        Arc::clone(&caller),
        None,
        Some(parked("turns", "U")),
    );

    rig.handle
        .dispatch(turn("t-1"))
        .await
        .expect("the turn re-parks");
    let observed = rig.observed.lock().unwrap();
    assert_eq!(
        observed.trace,
        vec!["find_live_alias", "claim_instance", "load", "commit_repark"]
    );
    let (snapshot, satisfied) = &observed.reparks[0];
    assert_eq!(snapshot.waiting_nodes, vec!["Next".to_string()]);
    assert!(
        snapshot.variables.iter().any(|(n, _)| n == "a"),
        "the answer is parked"
    );
    assert!(
        satisfied.contains(&"U".to_string()),
        "the relayed wait is resolved: {satisfied:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_call_in_a_relay_turn_persists_the_instance_failed() {
    let caller = FakeCaller::new(|_| {
        Box::pin(async {
            Err(InlineFailure::Permanent(Diagnostic::error(
                "UPSTREAM_REJECTED",
                "422",
            )))
        })
    });
    let rig = rig_over(
        RELAY_THEN_CALL,
        Arc::clone(&caller),
        None,
        Some(parked("turns", "U")),
    );

    let d = rig
        .handle
        .dispatch(turn("t-1"))
        .await
        .expect_err("the resumed flow failed");
    assert_eq!(d.code, "UPSTREAM_REJECTED");
    let trace = trace(&rig);
    assert!(trace.contains(&"commit_failed".to_string()), "{trace:?}");
    assert!(
        trace.last() == Some(&"release_instance".to_string()),
        "the claim is handed back after the failure step: {trace:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_turn_during_a_chain_bounces_retry_safe() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::clone(&gate);
    let caller = FakeCaller::new(move |_| {
        let gate = Arc::clone(&opened);
        Box::pin(async move {
            gate.notified().await;
            json(r#"{"answer":"hi"}"#)
        })
    });
    let rig = rig_over(
        RELAY_THEN_CALL,
        Arc::clone(&caller),
        None,
        Some(parked("turns", "U")),
    );
    let first = {
        let handle = rig.handle.clone();
        tokio::spawn(async move { handle.dispatch(turn("t-1")).await })
    };
    for _ in 0..200 {
        if caller.calls() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The chain's claim is re-entrant for this lane, so the second turn would be GRANTED and
    // resume a stale frontier; the lane refuses it instead.
    let d = rig
        .handle
        .dispatch(turn("t-2"))
        .await
        .expect_err("the instance is inside a chain");
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.CLAIM_HELD", "{d:?}");
    gate.notify_one();
    first.await.unwrap().expect("the first turn completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timer_fire_drives_its_sync_call_inline() {
    let caller = FakeCaller::new(|_| Box::pin(async { json(r#"{"v":1}"#) }));
    let rig = rig_over(
        TIMER_THEN_CALL,
        Arc::clone(&caller),
        None,
        Some(parked("later", "T")),
    );

    let outcome = rig
        .handle
        .fire_timer(sutra_executor::TimerFire {
            deployment: deployment(),
            instance_id: INSTANCE.to_string(),
            node_id: "T".to_string(),
            due_at: "2026-10-24T10:00:00Z".to_string(),
            fired_at: "2026-10-24T10:00:00Z".to_string(),
        })
        .await
        .expect("the fire resumes");
    assert_eq!(
        outcome,
        sutra_channels::TimerFireOutcome::Resumed {
            instance_id: INSTANCE.to_string(),
            completed: true,
        }
    );
    assert_eq!(caller.calls(), 1);
    assert_eq!(
        trace(&rig),
        vec!["claim_instance", "load", "commit_complete"]
    );
}

// ---- over HTTP: replies on relayed turns, and streamed replies ------------------------------

const HTTP_CHANNELS: &str = "channels:
  - name: start-in
    transport: http
    bind: \"POST /channels/start-in\"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: k
  - name: relay-in
    transport: http
    bind: \"POST /channels/relay-in\"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: k
  - name: relay-2
    transport: http
    bind: \"POST /channels/relay-2\"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: k
";

const SSE_CHANNELS: &str = "channels:
  - name: start-in
    transport: http
    bind: \"POST /channels/start-in\"
    codec: json
    reply-stream: sse
    auth:
      scheme: apikey
      apikey:
        value: k
";

/// A conversation: the opening request and two relayed turns, each making a sync model call
/// and replying with it — respond-and-continue mid-conversation, a plain reply at the end.
const MULTI_TURN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="chat">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="header.conv" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="RA" implementation="reply">
      <bpmn:extensionElements><q:reply continue="true"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="U1">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn1"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="B" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="b"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="RB" implementation="reply">
      <bpmn:extensionElements><q:reply continue="true"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="U2">
      <bpmn:extensionElements><q:source channel="relay-2" name="turn2"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="C" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="c"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="RC" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="RA"/>
    <bpmn:sequenceFlow id="f3" sourceRef="RA" targetRef="U1"/>
    <bpmn:sequenceFlow id="f4" sourceRef="U1" targetRef="B"/>
    <bpmn:sequenceFlow id="f5" sourceRef="B" targetRef="RB"/>
    <bpmn:sequenceFlow id="f6" sourceRef="RB" targetRef="U2"/>
    <bpmn:sequenceFlow id="f7" sourceRef="U2" targetRef="C"/>
    <bpmn:sequenceFlow id="f8" sourceRef="C" targetRef="RC"/>
    <bpmn:sequenceFlow id="f9" sourceRef="RC" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// One streamed model call, then the reply.
const STREAMED: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="streamed">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc" q:stream="true">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="R"/>
    <bpmn:sequenceFlow id="f3" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn numbered() -> Arc<FakeCaller> {
    FakeCaller::new(|n| Box::pin(async move { json(&format!(r#"{{"n":{n}}}"#)) }))
}

#[tokio::test(flavor = "multi_thread")]
async fn each_relayed_turn_answers_with_its_own_reply() {
    let rig = http_rig(MULTI_TURN, numbered(), HTTP_CHANNELS, None);

    let opening = post(&rig, "/channels/start-in", "c-1", r#"{"hi":1}"#).await;
    assert_eq!(opening.status, 200, "{}", opening.body);
    let opening: serde_json::Value = serde_json::from_str(&opening.body).expect("json reply");
    assert_eq!(opening, serde_json::json!({"a": {"n": 0}}));
    // Respond-and-continue: the tail after the reply runs on its self-resume timer.
    continue_after(&rig, "RA").await;

    // Turn 1: the relay resumes the instance, makes its sync call inline and answers with the
    // reply THIS turn reached (RB) — not the opening's RA, which the replay walks past.
    let turn1 = post(&rig, "/channels/relay-in", "c-1", r#"{"say":"one"}"#).await;
    assert_eq!(turn1.status, 200, "{}", turn1.body);
    let turn1: serde_json::Value = serde_json::from_str(&turn1.body).expect("json reply");
    assert_eq!(turn1["b"], serde_json::json!({"n": 1}), "{turn1}");
    continue_after(&rig, "RB").await;

    // Turn 2 completes the instance and answers with RC.
    let turn2 = post(&rig, "/channels/relay-2", "c-1", r#"{"say":"two"}"#).await;
    assert_eq!(turn2.status, 200, "{}", turn2.body);
    let turn2: serde_json::Value = serde_json::from_str(&turn2.body).expect("json reply");
    assert_eq!(turn2["c"], serde_json::json!({"n": 2}), "{turn2}");

    let observed = rig.observed.lock().unwrap();
    assert_eq!(
        observed
            .trace
            .iter()
            .filter(|c| c.starts_with("commit_"))
            .collect::<Vec<_>>(),
        vec![
            "commit_park",
            "commit_repark",
            "commit_repark",
            "commit_repark",
            "commit_complete"
        ],
        "one durable step per turn, plus each respond-and-continue tail"
    );
}

/// Fire a `<q:reply continue>` node's due-now self-resume timer, as the poller would.
async fn continue_after(rig: &HttpRig, node: &str) {
    let fired = rig
        .handle
        .fire_timer(sutra_executor::TimerFire {
            deployment: deployment(),
            instance_id: INSTANCE.to_string(),
            node_id: node.to_string(),
            due_at: "2026-10-24T10:00:00Z".to_string(),
            fired_at: "2026-10-24T10:00:00Z".to_string(),
        })
        .await
        .expect("the tail runs");
    assert!(
        matches!(
            fired,
            sutra_channels::TimerFireOutcome::Resumed {
                completed: false,
                ..
            }
        ),
        "{fired:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relayed_turn_that_reaches_no_reply_answers_200_empty() {
    let rig = http_rig(
        RELAY_THEN_CALL,
        numbered(),
        HTTP_CHANNELS,
        Some(parked("turns", "U")),
    );
    let turn = post(&rig, "/channels/relay-in", "c-1", r#"{"say":"hello"}"#).await;
    assert_eq!(turn.status, 200);
    assert!(
        turn.body.is_empty(),
        "no <q:reply> in this turn: {}",
        turn.body
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_call_answers_as_server_sent_events() {
    let caller = numbered().streaming(&["Hel", "lo\nthere"]);
    let rig = http_rig(STREAMED, Arc::clone(&caller), SSE_CHANNELS, None);

    let response = post(&rig, "/channels/start-in", "c-1", r#"{"hi":1}"#).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.content_type, "text/event-stream");
    // Chunks in order, then the flow's reply, then the stream closes.
    assert_eq!(
        response.body,
        "event: chunk\ndata: Hel\n\n\
         event: chunk\ndata: lo\ndata: there\n\n\
         event: reply\ndata: {\"a\":{\"n\":0}}\n\n"
    );
    assert_eq!(caller.streamed.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_channel_without_reply_stream_ignores_q_stream() {
    let caller = numbered().streaming(&["never", "seen"]);
    let rig = http_rig(STREAMED, Arc::clone(&caller), HTTP_CHANNELS, None);

    let response = post(&rig, "/channels/start-in", "c-1", r#"{"hi":1}"#).await;
    assert_eq!(response.status, 200);
    assert_ne!(response.content_type, "text/event-stream");
    let reply: serde_json::Value = serde_json::from_str(&response.body).expect("json reply");
    assert_eq!(reply, serde_json::json!({"a": {"n": 0}}));
    assert_eq!(
        caller.streamed.load(Ordering::SeqCst),
        0,
        "the call was not streamed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_streamed_call_ends_the_stream_with_its_code() {
    let caller = FakeCaller::new(|_| {
        Box::pin(async {
            Err(InlineFailure::Permanent(Diagnostic::error(
                "UPSTREAM_REJECTED",
                "the counterpart said something private",
            )))
        })
    })
    .streaming(&["partial"]);
    let rig = http_rig(STREAMED, Arc::clone(&caller), SSE_CHANNELS, None);

    let response = post(&rig, "/channels/start-in", "c-1", r#"{"hi":1}"#).await;
    assert_eq!(response.content_type, "text/event-stream");
    // The code only — never the failure's message.
    assert_eq!(
        response.body,
        "event: chunk\ndata: partial\n\nevent: error\ndata: UPSTREAM_REJECTED\n\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_fallback_tells_the_client_to_reset() {
    let caller = numbered().streaming(&["part", "<reset>", "whole"]);
    let rig = http_rig(STREAMED, Arc::clone(&caller), SSE_CHANNELS, None);

    let response = post(&rig, "/channels/start-in", "c-1", r#"{"hi":1}"#).await;
    assert_eq!(response.content_type, "text/event-stream");
    assert_eq!(
        response.body,
        "event: chunk\ndata: part\n\n\
         event: reset\ndata: \n\n\
         event: chunk\ndata: whole\n\n\
         event: reply\ndata: {\"a\":{\"n\":0}}\n\n"
    );
}

// ---- a reply reached before a park answers the request ------------------------------------

/// start → two replies → a durable wait (no synchronous call at all).
const REPLY_THEN_PARK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="greets">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="R1" implementation="say-first">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="R2" implementation="say-second">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="R1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="R1" targetRef="R2"/>
    <bpmn:sequenceFlow id="f3" sourceRef="R2" targetRef="U"/>
    <bpmn:sequenceFlow id="f4" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A relayed turn: wait → sync call → reply → the next wait.
const TURN_REPLY_THEN_PARK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="header.conv" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:userTask id="Next">
      <bpmn:extensionElements><q:source channel="relay-next" name="turn2"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="R"/>
    <bpmn:sequenceFlow id="f4" sourceRef="R" targetRef="Next"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Next" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_start_that_replies_then_parks_answers_with_the_last_reply() {
    let rig = rig(REPLY_THEN_PARK, numbered(), None);

    let outcome = rig
        .handle
        .dispatch(start("k-1"))
        .await
        .expect("the start parks");
    // Two replies in one pass: the LAST one rendered answers.
    assert_eq!(reply_json(&outcome), serde_json::json!({"said": "second"}));
    assert_eq!(trace(&rig), vec!["commit_park"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relayed_turn_that_replies_then_parks_answers_with_its_reply() {
    let rig = rig_over(
        TURN_REPLY_THEN_PARK,
        numbered(),
        None,
        Some(parked("turns", "U")),
    );

    let outcome = rig
        .handle
        .dispatch(turn("t-1"))
        .await
        .expect("the turn re-parks");
    assert_eq!(reply_json(&outcome), serde_json::json!({"a": {"n": 0}}));
    assert_eq!(
        trace(&rig),
        vec!["find_live_alias", "claim_instance", "load", "commit_repark"]
    );
}

// ---- a loop whose wait is a synchronous call ----------------------------------------------

/// start → sync call `A` → G: `a.n < 3` calls `A` again, else reply → end.
const SYNC_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="syncloop">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="G" default="out"/>
    <bpmn:serviceTask id="R" implementation="reply">
      <bpmn:extensionElements><q:reply/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="A">
      <bpmn:conditionExpression>a.n &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="out" sourceRef="G" targetRef="R"/>
    <bpmn:sequenceFlow id="f3" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start → turn wait `U` → sync call `A` → G: `a.n < 2` waits for the next turn, else end.
const TURN_CALL_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turnloop">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="header.conv" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="relay-in" name="turn"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="A" implementation="channel:svc">
      <bpmn:extensionElements>
        <q:source channel="svc-answers" name="a"/>
        <q:timeout duration="PT2S"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="G" default="done"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="A"/>
    <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="G"/>
    <bpmn:sequenceFlow id="next" sourceRef="G" targetRef="U">
      <bpmn:conditionExpression>a.n &lt; 2</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="done" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// The n-th call (0-based) answers `{"n": n + 1}`.
fn counting() -> Arc<FakeCaller> {
    FakeCaller::new(|n| Box::pin(async move { json(&format!(r#"{{"n":{}}}"#, n + 1)) }))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_call_in_a_loop_is_made_again_inline_on_every_iteration() {
    let caller = counting();
    let rig = rig(SYNC_LOOP, Arc::clone(&caller), None);

    let outcome = rig
        .handle
        .dispatch(start("k-loop"))
        .await
        .expect("the chain loops and completes");

    assert_eq!(caller.calls(), 3, "each iteration makes its own call");
    assert_eq!(reply_json(&outcome), serde_json::json!({"a": {"n": 3}}));
    assert!(
        trace(&rig).is_empty(),
        "a loop of sync calls is still one in-memory chain: {:?}",
        trace(&rig)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_turn_loop_with_a_sync_call_parks_the_next_turn_as_a_new_wait() {
    let caller = counting();
    let rig = rig_over(
        TURN_CALL_LOOP,
        Arc::clone(&caller),
        None,
        Some(parked("turnloop", "U")),
    );

    rig.handle
        .dispatch(turn("t-1"))
        .await
        .expect("the first turn loops back to the turn wait");
    {
        let observed = rig.observed.lock().unwrap();
        assert_eq!(caller.calls(), 1);
        let (snapshot, resolved) = observed.reparks.last().expect("the turn re-parked");
        assert_eq!(snapshot.waiting_nodes, vec!["U".to_string()]);
        assert!(
            resolved.contains(&"U".to_string()),
            "the answered turn's wait row is resolved and the next turn's recorded fresh: \
             {resolved:?}"
        );
    }

    rig.handle
        .dispatch(turn("t-2"))
        .await
        .expect("the second turn leaves the loop");
    assert_eq!(caller.calls(), 2, "the second turn made its own call");
    assert_eq!(
        trace(&rig).last().map(String::as_str),
        Some("commit_complete")
    );
}
