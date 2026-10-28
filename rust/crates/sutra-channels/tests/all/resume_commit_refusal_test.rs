//! A resumed step the engine refuses to COMMIT fails the instance, exactly once.
//!
//! The executor can finish a resumed pass without error and the step can still be uncommittable:
//! committing first holds every emission to its contract, and a `<q:send>` whose rendered body
//! breaks its codec's contract is refused (`SUTRA.OUTBOUND.VALIDATION_REJECT`). That refusal is
//! deterministic — the same pass is refused the same way on every retry — so it must end the
//! instance the way an executor failure does: one durable FAILED step, one `on_instance_failed`.
//!
//! It used to be reported like a store error instead. The timer poller then deferred the row and
//! fired it again every backoff tick, forever: the instance stayed SUSPENDED, and the tail before
//! the refused send (here, the `work` task) re-ran on each fire. A `<q:reply continue="true">`
//! tail is the common victim, because the whole tail runs on its due-now timer.
//!
//! Covered per resume path: the continue-reply self-resume, an ordinary timer catch, a retry
//! backoff re-drive, and a relay. The last test pins the other side of the line: a store that
//! fails to commit is NOT a refusal, and the instance must survive it for the retry.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::{BpmnModelLoader, SutraError};
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, Namespace, OutboxEmission,
    SuspendedInstance, TimerFireOutcome, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::listener::{ExecutionListener, InstanceEvent};
use sutra_executor::{DeploymentId, TaskError, TaskRegistry, TimerFire, TokenExecutor};
use sutra_feel::FeelValue;

use crate::outbound_validation_test::ContractCodec;
use crate::support::drive;

const TENANT: &str = "acme";
const REJECT: &str = "SUTRA.OUTBOUND.VALIDATION_REJECT";
const INSTANCE_FAILED: &str = "SUTRA.DISPATCH.INSTANCE_FAILED";
const STORE_DOWN: &str = "TEST.STORE.UNAVAILABLE";
const GOOD: &str = "<Packet><Document>ok</Document></Packet>";
const BAD: &str = "<Packet></Packet>";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "notify", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000e7").expect("valid deployment id")
}

/// The send every fixture ends its tail with: a codec STATED on the send, so its read-back
/// holds the rendered body to `contract-codec`'s contract (it must contain `<Document>`).
const SEND: &str = r#"<bpmn:sendTask id="Send">
      <bpmn:extensionElements>
        <q:send destination="notify://ops/out" codec="contract-codec"/>
      </bpmn:extensionElements>
    </bpmn:sendTask>"#;

fn definitions(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    {body}
  </bpmn:process>
</bpmn:definitions>"#
    )
}

/// S → Ack (`<q:reply continue="true">`) → Work → Send → Listen (receive task, PT60S boundary)
/// → E. Everything after Ack runs on Ack's due-now self-resume timer.
fn continue_reply() -> String {
    definitions(&format!(
        r#"<bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Ack" implementation="${{ack}}">
      <bpmn:extensionElements><q:reply continue="true"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Work" implementation="${{work}}"/>
    {SEND}
    <bpmn:receiveTask id="Listen">
      <bpmn:extensionElements><q:source channel="turn-in" name="status"/></bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:boundaryEvent id="Timeout" attachedToRef="Listen">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT60S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:endEvent id="ET"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Ack"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Ack" targetRef="Work"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Work" targetRef="Send"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Send" targetRef="Listen"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Listen" targetRef="E"/>
    <bpmn:sequenceFlow id="t1" sourceRef="Timeout" targetRef="ET"/>"#
    ))
}

/// S → Wait (intermediate timer catch) → Work → Send → E.
fn timer_catch() -> String {
    definitions(&format!(
        r#"<bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT10S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="Work" implementation="${{work}}"/>
    {SEND}
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="Work"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Work" targetRef="Send"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Send" targetRef="E"/>"#
    ))
}

/// S → Flaky (`<q:retry>`, fails its first attempt) → Work → Send → E. The second attempt runs
/// on the retry backoff's timer.
fn retry() -> String {
    definitions(&format!(
        r#"<bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Flaky" implementation="flaky">
      <bpmn:extensionElements><q:retry maxAttempts="3" initialDelay="PT10S"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Work" implementation="${{work}}"/>
    {SEND}
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Flaky"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Flaky" targetRef="Work"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Work" targetRef="Send"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Send" targetRef="E"/>"#
    ))
}

/// S (correlating business key) → Listen (receive task on `turn-in`) → Work → Send → E.
fn relay() -> String {
    definitions(&format!(
        r#"<bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="start-in" name="req"/>
        <q:alias name="conv" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:receiveTask id="Listen">
      <bpmn:extensionElements><q:source channel="turn-in" name="status"/></bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:serviceTask id="Work" implementation="${{work}}"/>
    {SEND}
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Listen"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Listen" targetRef="Work"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Work" targetRef="Send"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Send" targetRef="E"/>"#
    ))
}

// ---- an in-memory bridge that keeps one instance the way the durable one does ----------------

#[derive(Default)]
struct Store {
    instance: Option<String>,
    snapshot: Option<SuspendedInstance>,
    aliases: Vec<(String, String)>,
    /// Every outbox row a committed step enqueued.
    emissions: Vec<OutboxEmission>,
    /// The timer rows the latest park/re-park armed.
    timers: Vec<String>,
    reparks: usize,
    completed: bool,
    /// `(failure_code, detail)` per durable FAILED step.
    failed: Vec<(String, String)>,
}

#[derive(Default)]
struct Bridge {
    store: RefCell<Store>,
    /// Make every step commit fail as an unavailable store would.
    store_down: Cell<bool>,
}

impl Bridge {
    fn instance(&self) -> String {
        self.store.borrow().instance.clone().expect("an instance")
    }

    fn down(&self) -> Result<(), Diagnostic> {
        if self.store_down.get() {
            return Err(Diagnostic::error(
                STORE_DOWN,
                "the instance store is unavailable",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for Bridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.down()?;
        let mut store = self.store.borrow_mut();
        store.instance = Some(instance_id.to_string());
        store.snapshot = Some(snapshot.clone());
        store.timers = timer_waits.iter().map(|t| t.node_id.clone()).collect();
        store.emissions.extend(emissions.iter().cloned());
        store
            .aliases
            .extend(aliases.iter().map(|a| (a.name.clone(), a.value.clone())));
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self.store.borrow().snapshot.clone())
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let store = self.store.borrow();
        let live = store.aliases.iter().any(|(n, v)| n == name && v == value);
        Ok(if live { store.instance.clone() } else { None })
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.down()?;
        let mut store = self.store.borrow_mut();
        store.completed = true;
        store.snapshot = None;
        store.timers.clear();
        store.emissions.extend(emissions.iter().cloned());
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
        _satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
        _retired_aliases: &[AliasRecord],
    ) -> Result<(), Diagnostic> {
        self.down()?;
        let mut store = self.store.borrow_mut();
        store.reparks += 1;
        store.snapshot = Some(snapshot.clone());
        store.timers = timer_waits.iter().map(|t| t.node_id.clone()).collect();
        store.emissions.extend(emissions.iter().cloned());
        store
            .aliases
            .extend(aliases.iter().map(|a| (a.name.clone(), a.value.clone())));
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.store
            .borrow_mut()
            .emissions
            .extend(emissions.iter().cloned());
        Ok(())
    }

    /// The durable failure step: the snapshot turns FAILED and every wait/timer row resolves,
    /// so nothing refires. Aliases stay live, so a relay still finds the instance and is told
    /// it is FAILED.
    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic> {
        let mut store = self.store.borrow_mut();
        store
            .failed
            .push((failure_code.to_string(), detail.to_string()));
        store.timers.clear();
        if let Some(snapshot) = store.snapshot.as_mut() {
            snapshot.status = "FAILED".to_string();
        }
        Ok(())
    }
}

/// Counts the terminal listener events — the hooks the audit journal (`INSTANCE_FAILED`) and an
/// `on-complete` acknowledgement settle on.
#[derive(Default)]
struct Terminal {
    failed: RefCell<Vec<String>>,
}

impl ExecutionListener for Terminal {
    fn on_instance_failed(&self, _event: &InstanceEvent, diagnostic: &SutraError) {
        self.failed.borrow_mut().push(diagnostic.code.clone());
    }
}

// ---- the rig --------------------------------------------------------------------------------

struct Rig {
    engine: ChannelEngine,
    bridge: Rc<Bridge>,
    terminal: Rc<Terminal>,
    /// How many times `work` ran — the side effect a refire would repeat.
    work: Rc<Cell<u32>>,
    /// The body `work` renders for the send.
    body: Rc<RefCell<&'static str>>,
}

fn rig(bpmn: &str) -> Rig {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let work = Rc::new(Cell::new(0));
    let body = Rc::new(RefCell::new(BAD));
    let (count, rendered) = (Rc::clone(&work), Rc::clone(&body));
    let flaky_calls = Rc::new(Cell::new(0));
    let tasks = TaskRegistry::new()
        .register("ack", |_, _| {
            Ok(FeelValue::Map(BTreeMap::from([(
                "responseBody".to_string(),
                FeelValue::String("accepted".to_string()),
            )])))
        })
        .register("flaky", move |_, _| {
            flaky_calls.set(flaky_calls.get() + 1);
            if flaky_calls.get() == 1 {
                Err(TaskError::Failed("gateway timeout".to_string()))
            } else {
                Ok(FeelValue::Null)
            }
        })
        .register("work", move |_, _| {
            count.set(count.get() + 1);
            Ok(FeelValue::Map(BTreeMap::from([(
                "responseBody".to_string(),
                FeelValue::String(rendered.borrow().to_string()),
            )])))
        });
    let terminal = Rc::new(Terminal::default());
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_listener(Rc::clone(&terminal) as Rc<dyn ExecutionListener>)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register_shared(Arc::new(ContractCodec));
    let bridge = Rc::new(Bridge::default());
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in ["start-in", "turn-in"] {
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
        terminal,
        work,
        body,
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
        received_at: "2026-10-27T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn start(rig: &Rig) {
    drive(rig.engine.dispatch(&inbound("start-in", "C-1", "s-1"))).expect("the start parks");
}

fn fire(rig: &Rig, node: &str) -> Result<TimerFireOutcome, Diagnostic> {
    drive(rig.engine.fire_timer(&TimerFire {
        deployment: deployment(),
        instance_id: rig.bridge.instance(),
        node_id: node.to_string(),
        due_at: "2026-10-27T10:00:00Z".to_string(),
        fired_at: "2026-10-27T10:00:10Z".to_string(),
    }))
}

/// The failure code a dispatch reports, whether it surfaced as an error or as the at-most-once
/// dead-letter a non-idempotent process's failure is turned into.
fn dispatch_failure(result: Result<DispatchOutcome, Diagnostic>) -> String {
    match result {
        Err(d) => d.code,
        Ok(DispatchOutcome::DeadLettered { cause_code, .. }) => cause_code,
        Ok(other) => panic!("expected the resume to fail, got {other:?}"),
    }
}

/// The verdict every refused resume must reach: one durable FAILED step carrying the refusal,
/// one `on_instance_failed`, no committed step, no outbox row and no timer left to fire.
fn assert_failed_once(rig: &Rig, emissions_before: usize) {
    let store = rig.bridge.store.borrow();
    assert_eq!(
        store.failed.len(),
        1,
        "exactly one FAILED step: {:?}",
        store.failed
    );
    assert_eq!(store.failed[0].0, REJECT);
    assert!(
        store.failed[0].1.contains("Node 'Send'"),
        "the cause names the refused node: {}",
        store.failed[0].1
    );
    assert_eq!(
        *rig.terminal.failed.borrow(),
        [REJECT],
        "one INSTANCE_FAILED event"
    );
    assert_eq!(store.reparks, 0, "the refused step was never committed");
    assert!(!store.completed);
    assert_eq!(
        store.emissions.len(),
        emissions_before,
        "the refused send never reached the outbox"
    );
    assert!(store.timers.is_empty(), "no timer row is left to refire");
    assert_eq!(
        store.snapshot.as_ref().map(|s| s.status.as_str()),
        Some("FAILED")
    );
}

// ---- the timer resume paths -----------------------------------------------------------------

#[test]
fn a_refused_send_after_an_early_reply_fails_the_instance_once() {
    let rig = rig(&continue_reply());
    start(&rig);
    assert_eq!(
        rig.bridge.store.borrow().timers,
        ["Ack"],
        "the due-now self-resume"
    );
    let before = rig.bridge.store.borrow().emissions.len();

    let err = fire(&rig, "Ack").expect_err("the tail's send is refused");
    assert_eq!(err.code, REJECT, "{err:?}");
    assert_failed_once(&rig, before);
    assert_eq!(rig.work.get(), 1);

    // The poller fires the row again (it was claimed in the same window): the instance is dead,
    // so the fire is refused before anything runs, and `work` does not run a second time.
    let again = fire(&rig, "Ack").expect_err("a FAILED instance is never resumed");
    assert_eq!(again.code, INSTANCE_FAILED, "{again:?}");
    assert_eq!(rig.work.get(), 1, "the tail's side effects do not repeat");
    assert_eq!(
        rig.bridge.store.borrow().failed.len(),
        1,
        "still one FAILED step"
    );
    assert_eq!(
        rig.terminal.failed.borrow().len(),
        1,
        "still one INSTANCE_FAILED"
    );
}

#[test]
fn a_conforming_send_after_an_early_reply_parks_at_the_receive_task() {
    // The control for the test above: the same flow with a body the contract accepts runs its
    // tail, sends, and arms the receive task's boundary.
    let rig = rig(&continue_reply());
    *rig.body.borrow_mut() = GOOD;
    start(&rig);
    let before = rig.bridge.store.borrow().emissions.len();

    let outcome = fire(&rig, "Ack").expect("the tail runs");
    assert!(
        matches!(
            outcome,
            TimerFireOutcome::Resumed {
                completed: false,
                ..
            }
        ),
        "{outcome:?}"
    );
    let store = rig.bridge.store.borrow();
    assert!(store.failed.is_empty());
    assert_eq!(store.reparks, 1);
    assert_eq!(
        store.emissions.len(),
        before + 1,
        "the send reached the outbox"
    );
    assert_eq!(store.snapshot.as_ref().unwrap().waiting_nodes, ["Listen"]);
}

#[test]
fn a_refused_send_after_a_timer_catch_fails_the_instance_once() {
    let rig = rig(&timer_catch());
    start(&rig);
    let before = rig.bridge.store.borrow().emissions.len();

    let err = fire(&rig, "Wait").expect_err("the send is refused");
    assert_eq!(err.code, REJECT, "{err:?}");
    assert_failed_once(&rig, before);

    let again = fire(&rig, "Wait").expect_err("a FAILED instance is never resumed");
    assert_eq!(again.code, INSTANCE_FAILED);
    assert_eq!(rig.work.get(), 1, "the tail's side effects do not repeat");
}

#[test]
fn a_refused_send_after_a_retry_re_drive_fails_the_instance_once() {
    let rig = rig(&retry());
    start(&rig);
    assert_eq!(
        rig.bridge.store.borrow().timers,
        ["Flaky"],
        "the retry backoff"
    );
    let before = rig.bridge.store.borrow().emissions.len();

    let err = fire(&rig, "Flaky").expect_err("the re-driven tail's send is refused");
    assert_eq!(err.code, REJECT, "{err:?}");
    assert_failed_once(&rig, before);

    let again = fire(&rig, "Flaky").expect_err("a FAILED instance is never resumed");
    assert_eq!(again.code, INSTANCE_FAILED);
    assert_eq!(rig.work.get(), 1, "the tail's side effects do not repeat");
}

// ---- the relay resume path ------------------------------------------------------------------

#[test]
fn a_refused_send_after_a_relay_fails_the_instance_once() {
    let rig = rig(&relay());
    start(&rig);
    let before = rig.bridge.store.borrow().emissions.len();

    let code = dispatch_failure(drive(
        rig.engine.dispatch(&inbound("turn-in", "C-1", "r-1")),
    ));
    assert_eq!(code, REJECT);
    assert_failed_once(&rig, before);

    // A broker's redelivery (a fresh delivery id) finds the instance FAILED and runs nothing.
    let code = dispatch_failure(drive(
        rig.engine.dispatch(&inbound("turn-in", "C-1", "r-2")),
    ));
    assert_eq!(code, INSTANCE_FAILED);
    assert_eq!(rig.work.get(), 1, "the tail's side effects do not repeat");
    assert_eq!(rig.bridge.store.borrow().failed.len(), 1);
}

// ---- a store error is not a refusal ---------------------------------------------------------

#[test]
fn a_store_error_on_a_timer_resume_leaves_the_instance_for_the_retry() {
    let rig = rig(&continue_reply());
    *rig.body.borrow_mut() = GOOD;
    start(&rig);

    rig.bridge.store_down.set(true);
    let err = fire(&rig, "Ack").expect_err("the step cannot be committed");
    assert_eq!(err.code, STORE_DOWN, "the store's error surfaces as it was");
    {
        let store = rig.bridge.store.borrow();
        assert!(
            store.failed.is_empty(),
            "a store error never fails the instance"
        );
        assert!(rig.terminal.failed.borrow().is_empty());
        assert_eq!(
            store.timers,
            ["Ack"],
            "the row stays for the poller's deferred retry"
        );
        assert_eq!(
            store.snapshot.as_ref().map(|s| s.status.as_str()),
            Some("SUSPENDED")
        );
    }

    // The store is back: the deferred fire commits the step.
    rig.bridge.store_down.set(false);
    let outcome = fire(&rig, "Ack").expect("the retried fire commits");
    assert!(
        matches!(
            outcome,
            TimerFireOutcome::Resumed {
                completed: false,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(rig.bridge.store.borrow().reparks, 1);
}

// ---- an expression timer that cannot be armed on a resume (expression-valued timers) ---------

const TIMER_EXPRESSION_INVALID: &str = "SUTRA.RUNTIME.TIMER.EXPRESSION_INVALID";

/// S → Wait (PT10S) → Work → Again (timer catch whose duration is the unset `missing`) → E.
fn null_timer_after_a_resume() -> String {
    definitions(
        r#"<bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT10S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="Work" implementation="${work}"/>
    <bpmn:intermediateCatchEvent id="Again">
      <bpmn:timerEventDefinition><bpmn:timeDuration>missing</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="Work"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Work" targetRef="Again"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Again" targetRef="E"/>"#,
    )
}

#[test]
fn a_timer_expression_that_cannot_be_armed_on_a_resume_fails_the_instance_once() {
    let rig = rig(&null_timer_after_a_resume());
    start(&rig);
    assert_eq!(rig.bridge.store.borrow().timers, ["Wait"]);

    let err = fire(&rig, "Wait").expect_err("the next timer cannot be armed");
    assert_eq!(err.code, TIMER_EXPRESSION_INVALID, "{err:?}");
    {
        let store = rig.bridge.store.borrow();
        assert_eq!(store.failed.len(), 1, "one FAILED step: {:?}", store.failed);
        assert_eq!(store.failed[0].0, TIMER_EXPRESSION_INVALID);
        assert!(
            store.failed[0].1.contains("Again") && store.failed[0].1.contains("null"),
            "the cause names the timer and the result: {}",
            store.failed[0].1
        );
        assert_eq!(store.reparks, 0, "nothing was re-parked");
        assert!(store.timers.is_empty(), "no timer row is left to refire");
        assert_eq!(
            store.snapshot.as_ref().map(|s| s.status.as_str()),
            Some("FAILED")
        );
    }
    assert_eq!(
        *rig.terminal.failed.borrow(),
        [TIMER_EXPRESSION_INVALID],
        "one INSTANCE_FAILED event"
    );
    assert_eq!(rig.work.get(), 1);

    // A second fire of the same row runs nothing and fails nothing again: no silent default,
    // no retry loop.
    let again = fire(&rig, "Wait").expect_err("a FAILED instance is never resumed");
    assert_eq!(again.code, INSTANCE_FAILED, "{again:?}");
    assert_eq!(rig.work.get(), 1);
    assert_eq!(rig.bridge.store.borrow().failed.len(), 1);
    assert_eq!(rig.terminal.failed.borrow().len(), 1);
}
