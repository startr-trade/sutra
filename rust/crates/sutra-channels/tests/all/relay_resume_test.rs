//! Relay/resume negative paths + alias-materialisation edges: correlation misses, resume
//! against a missing or non-suspended instance, and the multi-row / two-alias
//! alias-indexing cases.
//!
//! The relay/resume machinery is implemented in `dispatch.rs`; these tests drive its
//! negative paths through a controllable in-memory [`InstanceBridge`] double (the durable
//! bridge lives in `sutra-persistence` and is exercised end-to-end by
//! `timer_channel_call_conformance.rs`).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DispatchOutcome,
    DrainingSink, InboundChain, InboundMessage, InstanceBridge, InstanceClaimOutcome, Namespace,
    OutboxEmission, SuspendedInstance, TimerWaitRecord, ValidatorRegistry, ACK_DISPOSITION_ATTR,
    ACK_DISPOSITION_REQUEUE,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000031").expect("valid deployment id")
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

// ---- controllable in-memory InstanceBridge double --------------------------------------

#[derive(Default)]
struct FakeBridge {
    /// `find_live_alias` result.
    alias_owner: Option<String>,
    /// `load` result.
    loaded: Option<SuspendedInstance>,
    /// Alias records captured from `commit_park` (the "one row per element" observation).
    parked_aliases: RefCell<Vec<AliasRecord>>,
    /// Set when a resume step actually commits — must stay false on every negative path.
    resume_committed: RefCell<bool>,
    /// `(name, value)` pairs `find_live_alias` was asked to correlate — lets a test observe the
    /// value the relay derived (e.g. from an inbound header) at the correlate site.
    alias_lookups: RefCell<Vec<(String, String)>>,
    /// When true, `claim_instance` answers `HeldByOther` — the "another replica owns this
    /// instance" posture the ownership wiring must bounce on.
    claim_held_by_other: bool,
    /// Instance ids `claim_instance` was called for, in order (the claim must precede the
    /// rehydrate).
    claims: RefCell<Vec<String>>,
    /// Instance ids handed back via `release_instance` (the drop-guard's trace).
    releases: RefCell<Vec<String>>,
    /// `(instance_id, failure_code, detail)` per `commit_failed` — the durable FAILED marker a
    /// fatally-failed resume step must write.
    failed_commits: RefCell<Vec<(String, String, String)>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for FakeBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parked_aliases.borrow_mut() = aliases.to_vec();
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
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        self.alias_lookups
            .borrow_mut()
            .push((name.to_string(), value.to_string()));
        Ok(self.alias_owner.clone())
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.resume_committed.borrow_mut() = true;
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
        *self.resume_committed.borrow_mut() = true;
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
        self.claims.borrow_mut().push(instance_id.to_string());
        Ok(if self.claim_held_by_other {
            InstanceClaimOutcome::HeldByOther
        } else {
            InstanceClaimOutcome::Granted
        })
    }

    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        self.releases.borrow_mut().push(instance_id.to_string());
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
}

fn engine_with_bridge(bridge: Rc<FakeBridge>) -> ChannelEngine {
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
        received_at: "2026-06-25T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn expect_err(result: Result<DispatchOutcome, Diagnostic>) -> Diagnostic {
    match result {
        Err(d) => d,
        Ok(_) => panic!("expected a dispatch failure"),
    }
}

// ============================ relay/resume negatives ====================================

#[test]
fn an_uncorrelated_relay_is_rejected_and_leaves_the_instance_parked() {
    // The relay correlates to alias e2eId="K-100" but NO live instance carries it →
    // RUNTIME.RELAY.CORRELATION_NOT_FOUND; the wait is the safe state (nothing resumed).
    let bridge = Rc::new(FakeBridge {
        alias_owner: None,
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");
    assert!(!*bridge.resume_committed.borrow(), "no resume ran");
}

#[test]
fn a_relay_to_an_unpersisted_instance_is_rejected() {
    // Correlation resolves to instance "inst-1" but the snapshot cannot be loaded →
    // RUNTIME.RESUME.INSTANCE_NOT_FOUND (the 404-equivalent).
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: None,
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.INSTANCE_NOT_FOUND");
    assert!(!*bridge.resume_committed.borrow());
}

#[test]
fn a_second_relay_to_an_already_resumed_instance_is_rejected() {
    // The instance exists but is no longer SUSPENDED (already resumed to completion) →
    // RUNTIME.RESUME.NOT_SUSPENDED (the 409-equivalent); no side effects re-run.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(SuspendedInstance {
            status: "COMPLETED".to_string(),
            suspended: false,
            ..Default::default()
        }),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.NOT_SUSPENDED");
    assert!(!*bridge.resume_committed.borrow());
}

// ==================== pinned-deployment integrity (hot-deploy migration) ======================

/// A parked snapshot of the `hold` process pinned to `deployment_id`, correlated by the
/// fake bridge to instance `inst-1` — the shape a relay resume rehydrates.
fn parked_snapshot(deployment_id: &str) -> SuspendedInstance {
    SuspendedInstance {
        process_id: "hold".to_string(),
        deployment_id: deployment_id.to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        waiting_nodes: vec!["U".to_string()],
        completed_nodes: vec!["S".to_string()],
        ..Default::default()
    }
}

#[test]
fn a_relay_to_an_instance_pinned_to_a_gone_deployment_fails_closed() {
    // The hot-deploy hazard: the instance parked under an OLDER deployment whose graph is no
    // longer registered (retired, or never re-planned after a restart). The relay must NOT
    // migrate it onto the currently-active definition — resume is replay-skipping-completed
    // against node ids, so a renamed/reordered node would replay as fresh work. Fail closed
    // with the same code the timer path raises on this condition, and leave the park intact
    // so the delivery is safe to redeliver once the pin is registered again.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_snapshot("dep-000000000000000000000099")),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RESOLVE.MODULE.NOT_FOUND");
    assert!(
        d.message.contains("dep-000000000000000000000099"),
        "the diagnostic names the unresolvable pin: {}",
        d.message
    );
    assert!(!*bridge.resume_committed.borrow(), "no resume ran");
}

#[test]
fn a_relay_to_an_instance_with_an_unreadable_pin_fails_closed() {
    // A corrupt/legacy pin column is a structured failure too (PIN_UNRESOLVABLE), never a
    // silent fallback to the deployment the delivery arrived under.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_snapshot("not-a-deployment-id")),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.PIN_UNRESOLVABLE");
    assert!(!*bridge.resume_committed.borrow(), "no resume ran");
}

#[test]
fn a_relay_to_an_instance_pinned_to_a_registered_deployment_still_resumes() {
    // The other half of fail-closed: while the pinned definition IS registered (it is the
    // live one here; the DRAINING tail registers flipped-away ones the same way), the resume
    // runs exactly as before — the fail-closed guard is not a blanket refusal.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_snapshot(deployment().value())),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    drive(engine.dispatch(&inbound("relay-in", b"K-100")))
        .expect("the pinned definition is registered — the relay resumes");
    assert!(*bridge.resume_committed.borrow(), "the resume committed");
}

// ============================ instance ownership (P0-1) =================================

/// A parked instance the relay can legitimately resume (frontier at the userTask `U`).
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

#[test]
fn a_relay_bounces_retry_safe_when_another_replica_holds_the_claim() {
    // Two replicas race the same correlated relay: the loser must not rehydrate. The bounce
    // is RETRY-SAFE — nothing loaded, nothing executed, nothing committed — so it carries the
    // requeue disposition and the broker redelivers under its own backoff.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_at_u()),
        claim_held_by_other: true,
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.CLAIM_HELD");
    assert_eq!(
        d.attributes.get(ACK_DISPOSITION_ATTR).map(String::as_str),
        Some(ACK_DISPOSITION_REQUEUE),
        "a contended claim is retry-safe: the broker must redeliver, not drop"
    );
    assert_eq!(*bridge.claims.borrow(), ["inst-1".to_string()]);
    assert!(
        !*bridge.resume_committed.borrow(),
        "the loser committed nothing"
    );
}

#[test]
fn a_failed_claim_on_a_vanished_instance_keeps_the_permanent_not_found_posture() {
    // The CAS also matches nothing when the ROW is gone (the instance completed between the
    // alias hit and the claim). That is permanent, not contention — it must not requeue
    // forever behind a claim that will never be released.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: None,
        claim_held_by_other: true,
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.INSTANCE_NOT_FOUND");
    assert!(!d.attributes.contains_key(ACK_DISPOSITION_ATTR));
}

#[test]
fn a_resumed_relay_claims_before_rehydrating_and_hands_the_claim_to_the_step() {
    // The happy path: claim → rehydrate → resume → commit. The hand-back rides the step's own
    // transaction (the durable bridge releases in `commit_repark`/by deleting the row in
    // `commit_complete`), so the drop guard stands down rather than spending a second round
    // trip proving the claim is already gone.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(parked_at_u()),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let outcome = drive(engine.dispatch(&inbound("relay-in", b"K-100")))
        .expect("the relay resumes the parked instance");
    match outcome {
        DispatchOutcome::Completed { instance_id, .. } => assert_eq!(instance_id, "inst-1"),
        other => panic!("expected a completed resume, got {other:?}"),
    }
    assert_eq!(*bridge.claims.borrow(), ["inst-1".to_string()]);
    assert!(*bridge.resume_committed.borrow(), "the step committed");
    assert!(
        bridge.releases.borrow().is_empty(),
        "a committed step releases in-transaction — the guard must not double-release"
    );
}

#[test]
fn a_rejected_rehydrate_still_releases_the_claim() {
    // Not-suspended is one of several exits that commit no step; each one must still hand the
    // claim back, or the instance would be unavailable until the sweeper's claim-timeout.
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(SuspendedInstance {
            status: "COMPLETED".to_string(),
            suspended: false,
            ..Default::default()
        }),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));
    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));
    assert_eq!(d.code, "SUTRA.RUNTIME.RESUME.NOT_SUSPENDED");
    assert_eq!(*bridge.releases.borrow(), ["inst-1".to_string()]);
}

/// Terminal retention (P1-2) made "the relay loaded a FINISHED instance" a routinely reachable
/// state rather than a race: the row now lingers for the retention window instead of being deleted
/// at completion. The verdict keeps the SAME code — this genuinely is "not suspended", and minting
/// a new one would split an existing contract — but the diagnostic must say the instance is OVER
/// rather than merely not-parked, and must name the status so an operator can tell a completed
/// instance from a cancelled one without a second call.
#[test]
fn a_relay_to_a_retained_terminal_instance_is_told_it_is_finished_not_merely_unparked() {
    for status in ["COMPLETED", "TERMINATED"] {
        let bridge = Rc::new(FakeBridge {
            alias_owner: Some("inst-1".to_string()),
            loaded: Some(SuspendedInstance {
                status: status.to_string(),
                suspended: false,
                ..Default::default()
            }),
            ..Default::default()
        });
        let engine = engine_with_bridge(Rc::clone(&bridge));
        let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));

        assert_eq!(
            d.code, "SUTRA.RUNTIME.RESUME.NOT_SUSPENDED",
            "reuse the existing code — the semantics match exactly"
        );
        assert_eq!(
            d.attributes.get("instanceStatus").map(String::as_str),
            Some(status),
            "the terminal guard fired (not the generic not-suspended fallthrough)"
        );
        assert_eq!(
            d.attributes.get("instanceId").map(String::as_str),
            Some("inst-1")
        );
        assert!(
            d.message.contains("already reached") && d.message.contains("not resumable"),
            "the message must read as finished, not as try-later: {}",
            d.message
        );
        // Nothing was executed and no step committed, so the claim still has to go back.
        assert_eq!(*bridge.releases.borrow(), ["inst-1".to_string()]);
        assert!(!*bridge.resume_committed.borrow());
    }
}

// ============================ durable FAILED state (P0-4) ===============================
//
// A fatally-failed RESUME used to persist NOTHING: the instance stayed at its previous wait
// frontier looking healthy, with only a log line to say it had died. These pin the replacement —
// the failure commit, and both resume paths refusing a FAILED instance afterwards.

/// hold-then-fail: start(`start-in`, correlate alias) → userTask(`relay-in`) → serviceTask that
/// throws → end. Resuming the parked user task therefore fails FATALLY (an uncaught task error,
/// never a BPMN error — nothing catches it).
const HOLD_THEN_FAIL_BPMN: &str = r#"<?xml version="1.0"?>
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
    <bpmn:serviceTask id="T" implementation="boom"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// As [`engine_with_bridge`] but over the given BPMN, with a `boom` task that always throws.
fn failing_engine_with_bridge(bpmn: &str, bridge: Rc<FakeBridge>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let tasks = TaskRegistry::new().register("boom", |_input, _ctx| {
        Err(sutra_executor::TaskError::Failed(
            "synthetic resume-leg failure".to_string(),
        ))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
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
    .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
    .build()
}

/// The parked snapshot the relay resumes: token at the user task, start event replayed as done.
fn parked_at_user_task() -> SuspendedInstance {
    SuspendedInstance {
        process_id: "hold".to_string(),
        deployment_id: deployment().value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        waiting_nodes: vec!["U".to_string()],
        ..Default::default()
    }
}

#[test]
fn a_fatally_failed_resume_persists_durable_failed_state() {
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("11111111-1111-4111-8111-111111111111".to_string()),
        loaded: Some(parked_at_user_task()),
        ..Default::default()
    });
    let engine = failing_engine_with_bridge(HOLD_THEN_FAIL_BPMN, Rc::clone(&bridge));

    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));

    // The cause still surfaces to the transport unchanged (fail closed, nothing swallowed) …
    assert_eq!(d.code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
    // … and the instance is now durably marked, naming the cause.
    let failed = bridge.failed_commits.borrow();
    assert_eq!(failed.len(), 1, "exactly one FAILED commit");
    assert_eq!(failed[0].0, "11111111-1111-4111-8111-111111111111");
    assert_eq!(failed[0].1, "SUTRA.RUNTIME.TASK.UNCAUGHT");
    assert!(
        failed[0].2.contains("synthetic resume-leg failure"),
        "the failure detail is recorded: {}",
        failed[0].2
    );
    assert!(
        !*bridge.resume_committed.borrow(),
        "a failed step commits no re-park and no terminal step"
    );
}

#[test]
fn a_relay_to_a_failed_instance_fails_closed_with_instance_failed() {
    // The durable marker's whole point: the NEXT relay must not re-drive a dead instance. The
    // alias is still bound (the failure step deliberately leaves it live), so correlation
    // succeeds and the load answers FAILED — the specific verdict, not the generic "not parked".
    let bridge = Rc::new(FakeBridge {
        alias_owner: Some("inst-1".to_string()),
        loaded: Some(SuspendedInstance {
            status: "FAILED".to_string(),
            suspended: false,
            ..parked_at_user_task()
        }),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));

    let d = expect_err(drive(engine.dispatch(&inbound("relay-in", b"K-100"))));

    assert_eq!(d.code, "SUTRA.DISPATCH.INSTANCE_FAILED");
    assert!(
        d.message.contains("inst-1") && d.message.contains("not"),
        "the diagnostic names the instance and says it is not resumable: {}",
        d.message
    );
    assert!(!*bridge.resume_committed.borrow(), "nothing was resumed");
    assert!(
        bridge.failed_commits.borrow().is_empty(),
        "refusing a resume must not re-mark the instance"
    );
}

#[test]
fn a_timer_fire_on_a_failed_instance_fails_closed_instead_of_reporting_stale() {
    // The timer path fails closed on the SAME code. Stale would read as "nothing to do"; the
    // poller keys on INSTANCE_FAILED to resolve the row instead of re-firing it forever.
    let bridge = Rc::new(FakeBridge {
        loaded: Some(SuspendedInstance {
            status: "FAILED".to_string(),
            suspended: false,
            ..parked_at_user_task()
        }),
        ..Default::default()
    });
    let engine = engine_with_bridge(Rc::clone(&bridge));

    let d = drive(engine.fire_timer(&sutra_executor::TimerFire {
        deployment: deployment(),
        instance_id: "inst-1".to_string(),
        node_id: "U".to_string(),
        due_at: "2026-06-25T10:00:00Z".to_string(),
        fired_at: "2026-06-25T10:00:01Z".to_string(),
    }))
    .expect_err("a FAILED instance is never re-driven by a timer");

    assert_eq!(d.code, "SUTRA.DISPATCH.INSTANCE_FAILED");
}

// ============================ alias-materialisation edges ===============================

#[test]
fn bad_feel_alias_expression_raises_alias_feel_eval_failed() {
    // A start-event <q:alias> whose expression fails to evaluate aborts the start with
    // INBOUND.ALIAS_FEEL_EVAL_FAILED. Sync process so no bridge/park is needed — the failure
    // is at alias materialisation.
    const BAD_ALIAS_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S">
          <bpmn:extensionElements>
            <q:source channel="start-in" name="payload"/>
            <q:alias name="bad" expression="noSuchBuiltin(payload)"/>
          </bpmn:extensionElements>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;
    let module = BpmnModelLoader::new()
        .load(BAD_ALIAS_BPMN.as_bytes())
        .expect("BPMN loads");
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
        "start-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .build();
    let d = expect_err(drive(engine.dispatch(&inbound("start-in", b"anything"))));
    assert_eq!(d.code, "SUTRA.INBOUND.ALIAS_FEEL_EVAL_FAILED");
}

/// Park a wait-state process whose start event declares `aliases`, returning the alias rows
/// the park step recorded (captured by the fake bridge). The codec + body let a test feed a
/// list-shaped payload for the multi-value case.
fn parked_alias_rows(
    start_extensions: &str,
    codec: &str,
    body: &[u8],
    ct: &str,
) -> Vec<AliasRecord> {
    let bpmn = format!(
        r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S">
          <bpmn:extensionElements>
            <q:source channel="start-in" name="payload"/>
            {start_extensions}
          </bpmn:extensionElements>
        </bpmn:startEvent>
        <bpmn:userTask id="U" name="Wait"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#
    );
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let bridge = Rc::new(FakeBridge::default());
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
        "start-in",
        namespace(),
        deployment(),
        codec,
    ))
    .with_module(&deployment(), &module)
    .with_instance_bridge(Rc::clone(&bridge) as Rc<dyn InstanceBridge>)
    .build();
    let mut msg = inbound("start-in", body);
    msg.content_type = Some(ct.to_string());
    drive(engine.dispatch(&msg)).expect("parks");
    let rows = bridge.parked_aliases.borrow().clone();
    rows
}

#[test]
fn multi_value_alias_produces_one_row_per_list_element() {
    // multi=true over a list-valued payload → one alias row per element at the intake park
    // path.
    let rows = parked_alias_rows(
        r#"<q:alias name="itemIds" expression="payload" multi="true"/>"#,
        "json",
        br#"["ID-1","ID-2","ID-3"]"#,
        "application/json",
    );
    let values: Vec<&str> = rows.iter().map(|r| r.value.as_str()).collect();
    assert_eq!(values, vec!["ID-1", "ID-2", "ID-3"]);
}

#[test]
fn two_aliases_on_one_start_event_produce_two_rows() {
    // Two scalar aliases on one start event → two alias rows.
    let rows = parked_alias_rows(
        r#"<q:alias name="uetr" expression="&quot;U-1&quot;"/>
           <q:alias name="endToEnd" expression="&quot;E-9&quot;"/>"#,
        "raw-text",
        b"body-value",
        "text/plain",
    );
    let pairs: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r.name.as_str(), r.value.as_str()))
        .collect();
    assert_eq!(pairs, vec![("uetr", "U-1"), ("endToEnd", "E-9")]);
}

// ==================== <q:alias> from an author-declared inbound header =========================

#[test]
fn start_event_alias_resolves_from_inbound_header() {
    // A start-event <q:alias expression="header.<field>"> indexes the spawned instance on a value
    // read from the inbound HEADER (not the payload). Proves `header` is in the alias FEEL context
    // at the start-event spawn site (the park-path alias-index).
    let bpmn = format!(
        r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S">
          <bpmn:extensionElements>
            <q:source channel="start-in" name="payload"/>
            {alias}
          </bpmn:extensionElements>
        </bpmn:startEvent>
        <bpmn:userTask id="U" name="Wait"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#,
        alias = r#"<q:alias name="txnAlias" expression="header.txnId" unique="true"/>"#
    );
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let bridge = Rc::new(FakeBridge::default());
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
        "start-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_instance_bridge(Rc::clone(&bridge) as Rc<dyn InstanceBridge>)
    .build();

    let mut msg = inbound("start-in", b"payload-body-ignored");
    msg.headers.insert("txnId".to_string(), "TX-77".to_string());
    drive(engine.dispatch(&msg)).expect("parks");

    // The alias row indexed the spawned instance under the HEADER-derived value.
    let rows = bridge.parked_aliases.borrow().clone();
    let pairs: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r.name.as_str(), r.value.as_str()))
        .collect();
    assert_eq!(pairs, vec![("txnAlias", "TX-77")]);
}

#[test]
fn imec_relay_wait_alias_correlates_from_inbound_header() {
    // A wait-node (imec relay-wait) <q:alias expression="header.<field>"> derives the correlation
    // value from the inbound HEADER, then correlates via find_live_alias. Proves `header` is in the
    // alias FEEL context at the relay-correlate site. (alias_owner=None ⇒ CORRELATION_NOT_FOUND, so
    // no fragile full-resume snapshot is needed — the header-derived lookup value is the assertion.)
    let bpmn = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="hold">
        <bpmn:startEvent id="S">
          <bpmn:extensionElements>
            <q:source channel="start-in" name="payload"/>
          </bpmn:extensionElements>
        </bpmn:startEvent>
        <bpmn:userTask id="U" name="Approve">
          <bpmn:extensionElements>
            <q:source channel="relay-in" name="decision"/>
            <q:alias name="corrAlias" expression="header.corrId"/>
          </bpmn:extensionElements>
        </bpmn:userTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let bridge = Rc::new(FakeBridge {
        alias_owner: None,
        ..Default::default()
    });
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
    .with_instance_bridge(Rc::clone(&bridge) as Rc<dyn InstanceBridge>)
    .build();

    let mut msg = inbound("relay-in", b"decision-body-ignored");
    msg.headers
        .insert("corrId".to_string(), "CORR-55".to_string());
    let d = expect_err(drive(engine.dispatch(&msg)));
    assert_eq!(d.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND");

    // The relay derived its correlation value from the inbound HEADER and asked find_live_alias for
    // exactly that (name, value).
    let lookups = bridge.alias_lookups.borrow().clone();
    assert_eq!(
        lookups,
        vec![("corrAlias".to_string(), "CORR-55".to_string())]
    );
}
