//! Channel-call `<q:retry>` execution semantics (F1 — retry reachability): the failure-set
//! routing and the re-emission contract, at the executor seam.
//!
//! The HONEST failure set of a channel-call task, and nothing else:
//!
//! * the ROUTE-LESS `<q:timeout>` boundary firing (classification
//!   `SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT`) — delivered through `resume_timer`, ruled on by
//!   the policy INSTEAD of raising the timeout BPMN error;
//! * a terminally-POISONED request delivery (classification
//!   `SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED`) — delivered through
//!   `resume_channel_call_failure` by the dispatcher, which has already verified the durable
//!   poisoned-row evidence.
//!
//! A correlated BUSINESS response is a NORMAL COMPLETION, never a retry trigger — the
//! counterpart answered, and re-sending would double-submit. That non-membership is pinned
//! here too.
//!
//! The re-drive contract: `resume_retry_redrive` re-runs the park side-effects — a FRESH
//! request emission (fresh outbox row, fresh idempotency key downstream) and a FRESH
//! `<q:timeout>` boundary — and consumes the `retry_backoff` marker. An ordinary resume
//! replay of a still-waiting call re-parks silently (never re-sends); only the explicit
//! re-drive emits.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, OutboundChannelRegistry, ResolvedOutboundChannel,
    StatefulExecResult, TaskRegistry, TimerFire, TokenExecutor, Variables,
};

const NOW: &str = "2026-01-01T00:00:00Z";
const IID: &str = "11111111-2222-4333-8444-555555555555";
const TIMEOUT_CODE: &str = "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT";
const POISON_CODE: &str = "SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000f1").expect("valid deployment id")
}

fn outbound() -> OutboundChannelRegistry {
    let mut registry = OutboundChannelRegistry::new();
    registry.register(
        &dep(),
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
    registry
}

fn executor(sink: Rc<CollectingSink>) -> TokenExecutor {
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_outbound_channels(outbound())
        .with_emission_sink(sink as Rc<dyn EmissionSink>)
        .with_now_supplier(|| NOW.to_string())
        .build()
}

/// start → channel-call `Call` (`<q:timeout PT2S>` + `retry`) → end. Explicit
/// `initialDelay`/`backoffCoefficient` make every backoff due-at exact under the frozen now.
fn flow(retry_attrs: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:alias name="callKey" expression="e2eId"/>
                <q:timeout duration="PT2S"/>
                <q:retry maxAttempts="3" initialDelay="PT10S" backoffCoefficient="2.0" {retry_attrs}/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn timeout_fire() -> TimerFire {
    TimerFire {
        deployment: dep(),
        instance_id: IID.to_string(),
        node_id: "Call#timeout".to_string(),
        due_at: "2026-01-01T00:00:02Z".to_string(),
        fired_at: "2026-01-01T00:00:02.1Z".to_string(),
    }
}

struct Parked {
    waiting_nodes: Vec<String>,
    completed_nodes: Vec<String>,
    timer_waits: Vec<sutra_executor::TimerWait>,
    retry_attempts: BTreeMap<String, u32>,
    retry_backoff: BTreeMap<String, String>,
}

fn expect_parked(result: StatefulExecResult) -> Parked {
    match result {
        StatefulExecResult::Suspended {
            waiting_nodes,
            completed_nodes,
            timer_waits,
            retry_attempts,
            retry_backoff,
            ..
        } => Parked {
            waiting_nodes,
            completed_nodes,
            timer_waits,
            retry_attempts,
            retry_backoff,
        },
        StatefulExecResult::Completed { .. } => panic!("expected a park, got COMPLETED"),
    }
}

// ============================ failure mode (a): the route-less timeout ====================

#[tokio::test]
async fn a_routeless_timeout_on_a_retry_call_parks_a_backoff_instead_of_erroring() {
    let process = proc(&flow(""), "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = expect_parked(
        executor
            .resume_timer(
                &process,
                &timeout_fire(),
                &["S".to_string()],
                vars(&[("e2eId", string("E2E-1"))]),
                BTreeMap::new(),
                None,
                &["Call".to_string()],
                &BTreeMap::new(),
                &BTreeMap::new(),
                &BTreeMap::new(),
            )
            .await
            .expect("the timeout is a RETRYABLE failure here, not the uncaught BPMN error"),
    );

    // The failed call is the frontier, parked on its BACKOFF timer only.
    assert_eq!(parked.waiting_nodes, vec!["Call".to_string()]);
    assert_eq!(parked.timer_waits.len(), 1, "{:?}", parked.timer_waits);
    assert_eq!(parked.timer_waits[0].node_id, "Call");
    assert_eq!(parked.timer_waits[0].due_at, "2026-01-01T00:00:10Z");
    // NOT completed — the omission that makes the re-drive re-execute it.
    assert!(!parked.completed_nodes.contains(&"Call".to_string()));
    // The durable budget + the backoff marker (the dead-attempt discriminator).
    assert_eq!(parked.retry_attempts.get("Call"), Some(&1));
    assert_eq!(
        parked.retry_backoff.get("Call").map(String::as_str),
        Some(TIMEOUT_CODE)
    );
    // A failure pass NEVER emits: the re-send belongs to the re-drive, not the park.
    assert!(sink.is_empty(), "the failing pass must not re-send");
}

#[tokio::test]
async fn the_timeout_classification_honours_non_retryable_codes() {
    // The timeout DOES carry a structured classification — the engine's own stable code —
    // so an author can declare "never retry timeouts, only delivery poisons".
    let process = proc(
        &flow(r#"nonRetryableCodes="SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT""#),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let e = executor
        .resume_timer(
            &process,
            &timeout_fire(),
            &["S".to_string()],
            vars(&[("e2eId", string("E2E-1"))]),
            BTreeMap::new(),
            None,
            &["Call".to_string()],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("a non-retryable classification fails the pass immediately");
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(
        d.message.contains("NON-RETRYABLE") && d.message.contains(TIMEOUT_CODE),
        "got: {}",
        d.message
    );
}

// ============================ the re-drive: RE-EMISSION ===================================

#[tokio::test]
async fn the_redrive_re_emits_the_request_and_re_arms_the_timeout() {
    let process = proc(&flow(""), "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = expect_parked(
        executor
            .resume_retry_redrive(
                &process,
                IID,
                &["S".to_string()],
                vars(&[
                    ("e2eId", string("E2E-1")),
                    ("requestBody", string("<Req>1</Req>")),
                ]),
                "Call",
                dep(),
                BTreeMap::new(),
                None,
                &["Call".to_string()],
                &BTreeMap::new(),
                &BTreeMap::from([("Call".to_string(), 1u32)]),
                &BTreeMap::from([("Call".to_string(), TIMEOUT_CODE.to_string())]),
            )
            .await
            .expect("the re-drive re-parks the fresh attempt"),
    );

    // THE re-emission contract: a fresh outbound request was collected (the dispatcher mints
    // it a fresh outbox row + idempotency key), body rebuilt from the same durable variables.
    assert_eq!(sink.len(), 1, "the re-drive RE-EMITS exactly one request");
    let emissions = sink.emissions();
    assert_eq!(emissions[0].node_id, "Call");
    assert_eq!(emissions[0].destination, "http://sink.example/req");
    assert_eq!(emissions[0].body.get(), &b"<Req>1</Req>".to_vec());
    // Fresh park shape: the call waits again, its `<q:timeout>` boundary re-armed FRESH.
    assert_eq!(parked.waiting_nodes, vec!["Call".to_string()]);
    assert_eq!(parked.timer_waits.len(), 1, "{:?}", parked.timer_waits);
    assert_eq!(parked.timer_waits[0].node_id, "Call#timeout");
    assert_eq!(parked.timer_waits[0].due_at, "2026-01-01T00:00:02Z");
    // The marker is CONSUMED (the attempt is live again); the burned budget survives.
    assert!(
        parked.retry_backoff.is_empty(),
        "{:?}",
        parked.retry_backoff
    );
    assert_eq!(parked.retry_attempts.get("Call"), Some(&1));
}

#[tokio::test]
async fn a_second_timeout_grows_the_backoff_and_exhaustion_fails_with_retry_exhausted() {
    let process = proc(&flow(""), "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    // Attempt 2 (re-driven, in flight, marker consumed) times out: backoff doubles.
    let parked = expect_parked(
        executor
            .resume_timer(
                &process,
                &timeout_fire(),
                &["S".to_string()],
                vars(&[("e2eId", string("E2E-1"))]),
                BTreeMap::new(),
                None,
                &["Call".to_string()],
                &BTreeMap::new(),
                &BTreeMap::from([("Call".to_string(), 1u32)]),
                &BTreeMap::new(),
            )
            .await
            .expect("attempt 2 still has budget"),
    );
    assert_eq!(parked.retry_attempts.get("Call"), Some(&2));
    assert_eq!(parked.timer_waits[0].due_at, "2026-01-01T00:00:20Z");
    assert_eq!(
        parked.retry_backoff.get("Call").map(String::as_str),
        Some(TIMEOUT_CODE)
    );

    // Attempt 3 is the last (maxAttempts=3): its timeout goes FATAL, not another park —
    // the dispatcher stamps the durable FAILED snapshot from this error.
    let e = executor
        .resume_timer(
            &process,
            &timeout_fire(),
            &["S".to_string()],
            vars(&[("e2eId", string("E2E-1"))]),
            BTreeMap::new(),
            None,
            &["Call".to_string()],
            &BTreeMap::new(),
            &BTreeMap::from([("Call".to_string(), 2u32)]),
            &BTreeMap::new(),
        )
        .await
        .expect_err("the budget is spent");
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(d.message.contains(TIMEOUT_CODE), "got: {}", d.message);
    assert!(sink.is_empty(), "no failure pass ever emits");
}

// ============================ failure mode (b): the poisoned delivery =====================

#[tokio::test]
async fn a_poison_failure_parks_the_backoff_with_the_delivery_exhausted_classification() {
    let process = proc(&flow(""), "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = expect_parked(
        executor
            .resume_channel_call_failure(
                &process,
                IID,
                "Call",
                POISON_CODE,
                "the outbox terminally poisoned the request delivery",
                &["S".to_string()],
                vars(&[("e2eId", string("E2E-1"))]),
                dep(),
                BTreeMap::new(),
                None,
                &["Call".to_string()],
                &BTreeMap::new(),
                &BTreeMap::new(),
                &BTreeMap::new(),
            )
            .await
            .expect("a poisoned delivery is a retryable task failure"),
    );

    assert_eq!(parked.waiting_nodes, vec!["Call".to_string()]);
    assert_eq!(parked.retry_attempts.get("Call"), Some(&1));
    assert_eq!(
        parked.retry_backoff.get("Call").map(String::as_str),
        Some(POISON_CODE)
    );
    assert_eq!(parked.timer_waits.len(), 1);
    assert_eq!(parked.timer_waits[0].node_id, "Call");
    assert_eq!(parked.timer_waits[0].due_at, "2026-01-01T00:00:10Z");
    assert!(sink.is_empty(), "the failing pass must not re-send");
}

#[tokio::test]
async fn a_poison_classification_can_be_declared_non_retryable() {
    let process = proc(
        &flow(r#"nonRetryableCodes="SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED""#),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let e = executor
        .resume_channel_call_failure(
            &process,
            IID,
            "Call",
            POISON_CODE,
            "the outbox terminally poisoned the request delivery",
            &["S".to_string()],
            vars(&[("e2eId", string("E2E-1"))]),
            dep(),
            BTreeMap::new(),
            None,
            &["Call".to_string()],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect_err("the declared non-retryable poison fails immediately");
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(d.message.contains(POISON_CODE), "got: {}", d.message);
}

// ============================ what is NOT a failure =======================================

#[tokio::test]
async fn a_correlated_response_mid_retry_completes_normally_and_clears_the_budget() {
    // Attempt 2 is in flight (one burned attempt, NO backoff marker) and the counterpart
    // finally answers: a BUSINESS response is a NORMAL COMPLETION, never a retry trigger.
    // The flow continues past the call to a further wait, whose park must show the call's
    // burned-attempt counter DROPPED (the attempt succeeded) and nothing re-sent.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:alias name="callKey" expression="e2eId"/>
                <q:timeout duration="PT2S"/>
                <q:retry maxAttempts="3"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:userTask id="Review"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="Review"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Review" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = expect_parked(
        executor
            .resume(
                &process,
                IID,
                &["S".to_string()],
                vars(&[("e2eId", string("E2E-1"))]),
                "Call",
                &vars(&[("status", string("APPROVED"))]),
                dep(),
                BTreeMap::new(),
                None,
                &["Call".to_string()],
                &BTreeMap::new(),
                &BTreeMap::from([("Call".to_string(), 1u32)]),
                &BTreeMap::new(),
            )
            .await
            .expect("the response resumes normally"),
    );

    assert_eq!(parked.waiting_nodes, vec!["Review".to_string()]);
    assert!(
        parked.completed_nodes.contains(&"Call".to_string()),
        "the answered call is DONE: {:?}",
        parked.completed_nodes
    );
    assert!(
        parked.retry_attempts.is_empty(),
        "the succeeded attempt drops its burned-budget counter: {:?}",
        parked.retry_attempts
    );
    assert!(sink.is_empty(), "a response resume never re-sends");
}

#[tokio::test]
async fn an_unrelated_resume_carries_the_backoff_marker_and_budget_forward() {
    // A parallel sibling's relay re-parks the instance while `Call` sits in its backoff
    // window: the re-park must carry BOTH the marker and the burned budget forward
    // untouched, and must not re-send (the still-waiting call re-parks silently).
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:parallelGateway id="Fork"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:alias name="callKey" expression="e2eId"/>
                <q:timeout duration="PT2S"/>
                <q:retry maxAttempts="3"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:userTask id="Approve"/>
            <bpmn:userTask id="Second"/>
            <bpmn:parallelGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="Call"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="Approve"/>
            <bpmn:sequenceFlow id="f4" sourceRef="Call" targetRef="Join"/>
            <bpmn:sequenceFlow id="f5" sourceRef="Approve" targetRef="Second"/>
            <bpmn:sequenceFlow id="f6" sourceRef="Second" targetRef="Join"/>
            <bpmn:sequenceFlow id="f7" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = expect_parked(
        executor
            .resume(
                &process,
                IID,
                &["S".to_string()],
                vars(&[("e2eId", string("E2E-1"))]),
                "Approve",
                &Variables::new(),
                dep(),
                BTreeMap::new(),
                None,
                &["Call".to_string(), "Approve".to_string()],
                &BTreeMap::new(),
                &BTreeMap::from([("Call".to_string(), 1u32)]),
                &BTreeMap::from([("Call".to_string(), TIMEOUT_CODE.to_string())]),
            )
            .await
            .expect("the sibling resume re-parks"),
    );

    assert!(parked.waiting_nodes.contains(&"Call".to_string()));
    assert!(parked.waiting_nodes.contains(&"Second".to_string()));
    assert_eq!(
        parked.retry_attempts.get("Call"),
        Some(&1),
        "an unrelated resume must not reset the budget"
    );
    assert_eq!(
        parked.retry_backoff.get("Call").map(String::as_str),
        Some(TIMEOUT_CODE),
        "an unrelated resume must not clear the backoff marker"
    );
    assert!(sink.is_empty(), "the still-parked call must not re-send");
}
