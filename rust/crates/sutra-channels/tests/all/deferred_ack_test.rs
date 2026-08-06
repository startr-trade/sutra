//! The deferred-ack DISPATCH contract (`ack-mode: on-complete`, broker seam):
//! `ChannelEngine::dispatch_deferred` + `DeferredAckRegistry` + the executor listener
//! bus, end-to-end at the engine level (the broker-side half — real basic.ack/basic.nack
//! against a live RabbitMQ — is `sutra-transport-rabbitmq/tests/rabbitmq_it.rs`):
//!
//! - a wait-state start PARKS → the delivery's settle callbacks are registered (answer
//!   `Deferred`), and the broker ack fires only when the relay-resumed instance reaches
//!   `INSTANCE_COMPLETED` (via the listener bus — no dispatcher involvement);
//! - a resume that FAILS fires the nack (the permanent-reject / DLQ posture);
//! - a run-to-completion dispatch answers `Settled` and never touches the registry (the
//!   terminal events fired inside the dispatch itself);
//! - without a wired registry the callbacks drop unfired and everything settles — the
//!   bare-builder/test posture.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, DeferredAckListener,
    DeferredAckRegistry, DeferredDispatch, DeferredSettle, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, Namespace, OutboxEmission,
    SuspendedInstance, TimerWaitRecord, ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskError, TaskRegistry, TokenExecutor};

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000041").expect("valid deployment id")
}

/// hold: start(`start-in`, correlate alias e2eId=event.body) → userTask(`relay-in`) → end.
/// The same shape as `relay_resume_test`'s HOLD_BPMN — a relay completes the instance.
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

/// hold-fail: like `hold`, but the resume runs a `boom` service task that always fails —
/// the resumed instance reaches `INSTANCE_FAILED`.
const HOLD_FAIL_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold-fail">
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
    <bpmn:serviceTask id="T" implementation="boom"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// sync: start(`sync-in`) → end — runs to completion inside the dispatch.
const SYNC_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo-sync">
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements>
        <q:source channel="sync-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="f" sourceRef="S2" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A STATEFUL in-memory [`InstanceBridge`]: serves the park it captured back to the
/// relay-resume path (alias correlation + snapshot load), so one engine can park an
/// instance and then genuinely resume it to a terminal state.
#[derive(Default)]
struct ParkingBridge {
    parked: RefCell<Option<(String, SuspendedInstance)>>,
    aliases: RefCell<Vec<AliasRecord>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for ParkingBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parked.borrow_mut() = Some((instance_id.to_string(), snapshot.clone()));
        *self.aliases.borrow_mut() = aliases.to_vec();
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self
            .parked
            .borrow()
            .as_ref()
            .filter(|(id, _)| id == instance_id)
            .map(|(_, snapshot)| snapshot.clone()))
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let matches = self
            .aliases
            .borrow()
            .iter()
            .any(|a| a.name == name && a.value == value);
        Ok(self
            .parked
            .borrow()
            .as_ref()
            .filter(|_| matches)
            .map(|(id, _)| id.clone()))
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parked.borrow_mut() = None;
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
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    /// A fatally-failed resume marks the instance FAILED. This double keeps the parked row (the
    /// real one re-stamps it in place) — no deferred-ack case here drives a failing resume.
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

/// The production wiring shape in miniature: ONE `Arc` registry, registered BOTH on the
/// executor's listener bus (terminal events settle entries) and on the `ChannelEngine`
/// (the park arm registers `dispatch_deferred` callbacks). `registry = None` builds the
/// bare-posture engine (no deferral capability).
fn engine_with(
    bpmn: &str,
    bridge: Rc<dyn InstanceBridge>,
    registry: Option<Arc<DeferredAckRegistry>>,
) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let sync_module = BpmnModelLoader::new()
        .load(SYNC_BPMN.as_bytes())
        .expect("sync BPMN loads");
    let tasks = TaskRegistry::new().register("boom", |_input, _ctx| {
        Err(TaskError::Failed("synthetic task failure".to_string()))
    });
    let sink = Rc::new(DrainingSink::new());
    let mut executor_builder = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>);
    if let Some(registry) = &registry {
        executor_builder =
            executor_builder.with_listener(Rc::new(DeferredAckListener::new(Arc::clone(registry))));
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
        "sync-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_module(&deployment(), &sync_module)
    .with_instance_bridge(bridge);
    if let Some(registry) = registry {
        builder = builder.with_deferred_acks(registry);
    }
    builder.build()
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
        received_at: "2026-07-28T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

/// Atomic-recorder settle callbacks (the shape a broker source hands over).
fn settle() -> (Arc<AtomicU32>, Arc<AtomicU32>, DeferredSettle) {
    let acks: Arc<AtomicU32> = Arc::default();
    let nacks: Arc<AtomicU32> = Arc::default();
    let (a, n) = (Arc::clone(&acks), Arc::clone(&nacks));
    let settle = DeferredSettle {
        ack: Box::new(move || {
            a.fetch_add(1, Ordering::SeqCst);
        }),
        nack: Box::new(move || {
            n.fetch_add(1, Ordering::SeqCst);
        }),
    };
    (acks, nacks, settle)
}

#[test]
fn a_parked_deferred_dispatch_acks_only_when_the_relayed_instance_completes() {
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let bridge = Rc::new(ParkingBridge::default());
    let engine = engine_with(
        HOLD_BPMN,
        Rc::clone(&bridge) as Rc<dyn InstanceBridge>,
        Some(Arc::clone(&registry)),
    );

    // 1. The on-complete delivery parks: the answer is Deferred, the callbacks are
    //    REGISTERED (not fired), and the broker slot stays open.
    let (acks, nacks, settle) = settle();
    let disposition = drive(engine.dispatch_deferred(&inbound("start-in", b"K-100"), settle))
        .expect("deferred dispatch parks");
    let instance_id = match disposition {
        DeferredDispatch::Deferred { instance_id } => instance_id,
        other => panic!("expected Deferred, got {other:?}"),
    };
    assert!(!instance_id.is_empty(), "the parked instance id rides back");
    assert_eq!(registry.pending_count(), 1);
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (0, 0),
        "nothing settles at park"
    );

    // 2. The relay resumes the instance to COMPLETED — the executor's listener bus fires
    //    the registry's ack exactly once (no dispatcher involvement).
    let outcome = drive(engine.dispatch(&inbound("relay-in", b"K-100")))
        .expect("relay resumes to completion");
    assert!(matches!(outcome, DispatchOutcome::Completed { .. }));
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (1, 0),
        "the deferred broker ack fired at INSTANCE_COMPLETED"
    );
    assert_eq!(registry.pending_count(), 0, "the entry is consumed");
}

#[test]
fn a_failed_resume_fires_the_deferred_nack() {
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let bridge = Rc::new(ParkingBridge::default());
    let engine = engine_with(
        HOLD_FAIL_BPMN,
        Rc::clone(&bridge) as Rc<dyn InstanceBridge>,
        Some(Arc::clone(&registry)),
    );

    let (acks, nacks, settle) = settle();
    let disposition = drive(engine.dispatch_deferred(&inbound("start-in", b"K-200"), settle))
        .expect("deferred dispatch parks");
    assert!(matches!(disposition, DeferredDispatch::Deferred { .. }));
    assert_eq!(registry.pending_count(), 1);

    // The relay resume runs the failing `boom` task → INSTANCE_FAILED → the nack (the
    // permanent-reject / NackDrop posture) fires exactly once; the relay's own dispatch
    // error is the relay DELIVERY's problem, settled by its own transport.
    let result = drive(engine.dispatch(&inbound("relay-in", b"K-200")));
    assert!(
        result.is_err(),
        "the failing resume surfaces its diagnostic"
    );
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (0, 1),
        "the deferred nack fired at INSTANCE_FAILED"
    );
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn a_run_to_completion_deferred_dispatch_settles_immediately() {
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let bridge = Rc::new(ParkingBridge::default());
    let engine = engine_with(HOLD_BPMN, bridge, Some(Arc::clone(&registry)));

    // `sync-in` starts a no-wait process: the dispatch completes inside the call, the
    // terminal events already fired, so the transport settles NOW (Ack) and the registry
    // is never touched — run-to-completion on-complete === on-persist timing.
    let (acks, nacks, settle) = settle();
    let disposition = drive(engine.dispatch_deferred(&inbound("sync-in", b"payload"), settle))
        .expect("sync dispatch completes");
    match disposition {
        DeferredDispatch::Settled(DispatchOutcome::Completed { .. }) => {}
        other => panic!("expected Settled(Completed), got {other:?}"),
    }
    assert_eq!(registry.pending_count(), 0, "the registry is never touched");
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (0, 0),
        "the callbacks drop unfired — the returned decision IS the settle"
    );
}

#[test]
fn without_a_wired_registry_a_parking_deferred_dispatch_still_settles() {
    // The bare-builder posture (`with_deferred_acks` never called — tests, minimal
    // hosts): dispatch_deferred must not defer, or the delivery would never settle.
    let bridge = Rc::new(ParkingBridge::default());
    let engine = engine_with(HOLD_BPMN, bridge, None);

    let (acks, nacks, settle) = settle();
    let disposition = drive(engine.dispatch_deferred(&inbound("start-in", b"K-300"), settle))
        .expect("dispatch parks");
    assert!(
        matches!(
            disposition,
            DeferredDispatch::Settled(DispatchOutcome::Completed { .. })
        ),
        "no registry ⇒ the park settles immediately (the accept outcome)"
    );
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (0, 0)
    );
}

// ============================ §2.1 park/terminal ordering =================================

/// A bridge double whose `commit_park` reproduces the cross-lane park/terminal window
/// DETERMINISTICALLY (no sleeps, no threads): after the park is durably captured but
/// BEFORE the dispatch returns to its arm, the instance's terminal event fires — exactly
/// what another shard lane can do the moment the commit is visible (claim, resume,
/// complete). The registry is `Send + Sync`, so firing it inline is the same call the
/// listener bus would make.
struct TerminalDuringParkBridge {
    inner: ParkingBridge,
    registry: Arc<DeferredAckRegistry>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for TerminalDuringParkBridge {
    async fn commit_park(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.inner
            .commit_park(
                deployment,
                instance_id,
                snapshot,
                aliases,
                timer_waits,
                emissions,
            )
            .await?;
        // The window: the park is committed and visible; a sibling lane completes the
        // instance before this lane's dispatch arm runs another statement.
        self.registry.on_instance_completed(instance_id);
        Ok(())
    }

    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        self.inner.load(deployment, instance_id).await
    }

    async fn find_live_alias(
        &self,
        deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        self.inner.find_live_alias(deployment, name, value).await
    }

    async fn commit_complete(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.inner
            .commit_complete(deployment, instance_id, emissions)
            .await
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
        self.inner
            .commit_repark(
                deployment,
                instance_id,
                snapshot,
                satisfied_wait_nodes,
                aliases,
                timer_waits,
                emissions,
            )
            .await
    }

    async fn commit_emissions(
        &self,
        deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.inner.commit_emissions(deployment, emissions).await
    }

    async fn commit_failed(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic> {
        self.inner
            .commit_failed(deployment, instance_id, failure_code, detail)
            .await
    }
}

/// A bridge double whose `commit_park` always FAILS — the withdraw leg of the inverted
/// registration order.
struct FailingParkBridge;

#[async_trait::async_trait(?Send)]
impl InstanceBridge for FailingParkBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Err(Diagnostic::error(
            "TEST.PARK.COMMIT_FAILED",
            "synthetic park-commit failure",
        ))
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
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
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
        _failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
}

#[test]
fn a_terminal_event_inside_the_park_commit_window_still_finds_the_registration() {
    // The §2.1 race, made deterministic: registration now precedes `commit_park`, so a
    // terminal event that fires the instant the commit is visible (another lane's claim →
    // resume → complete) finds the entry and the broker settle fires EXACTLY once. Under
    // the pre-inversion order (register after commit) this test fails: the terminal event
    // finds nothing, the ack never fires, and the entry dangles until the timeout sweep.
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let bridge = Rc::new(TerminalDuringParkBridge {
        inner: ParkingBridge::default(),
        registry: Arc::clone(&registry),
    });
    let engine = engine_with(HOLD_BPMN, bridge, Some(Arc::clone(&registry)));

    let (acks, nacks, settle) = settle();
    let disposition = drive(engine.dispatch_deferred(&inbound("start-in", b"K-400"), settle))
        .expect("deferred dispatch parks");
    assert!(
        matches!(disposition, DeferredDispatch::Deferred { .. }),
        "the transport is told Deferred — registered ∧ committed"
    );
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (1, 0),
        "the in-window terminal event fired the ack exactly once"
    );
    assert_eq!(registry.pending_count(), 0, "the entry settled — no dangle");
}

#[test]
fn a_failed_park_commit_withdraws_the_registration_and_surfaces_err() {
    // The inversion's failure leg: register → commit fails → withdraw. The transport sees
    // `Err` (its own redelivery disposition applies, exactly as before the inversion), no
    // callback fires, and no registration is left for a terminal event to trip over.
    let registry = Arc::new(DeferredAckRegistry::new(
        16,
        std::time::Duration::from_secs(3600),
    ));
    let engine = engine_with(
        HOLD_BPMN,
        Rc::new(FailingParkBridge),
        Some(Arc::clone(&registry)),
    );

    let (acks, nacks, settle) = settle();
    let result = drive(engine.dispatch_deferred(&inbound("start-in", b"K-500"), settle));
    let diagnostic = match result {
        Err(d) => d,
        Ok(other) => panic!("expected the park-commit failure, got {other:?}"),
    };
    assert_eq!(diagnostic.code, "TEST.PARK.COMMIT_FAILED");
    assert_eq!(
        registry.pending_count(),
        0,
        "the registration was withdrawn with the failed commit"
    );
    assert_eq!(
        (acks.load(Ordering::SeqCst), nacks.load(Ordering::SeqCst)),
        (0, 0),
        "neither callback fired — the transport's Err disposition is the settle path"
    );
}
