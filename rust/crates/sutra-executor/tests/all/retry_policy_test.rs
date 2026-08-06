//! `<q:retry>` execution semantics (P1-1) — the runtime half of the per-task retry policy.
//!
//! The load-time attribute/placement contract is pinned in
//! `sutra-bpmn/tests/all/retry_policy_test.rs`; what matters here is the EXECUTION shape, and
//! specifically that it is a durable TIMER PARK rather than a sleep. The engine's actor is
//! single-threaded and `block_on`-ed, so a backoff that blocked would freeze every other instance
//! on the replica. So each failed attempt must leave the executor at a quiescent point with:
//!
//! * the failed task NOT in `completed_nodes` (that omission is what makes the re-drive
//!   re-execute it rather than replay past it),
//! * the task in `waiting_nodes` (the frontier the dispatcher persists),
//! * a fresh timer row at `now + backoff`, and
//! * a durable attempt count in `retry_attempts` (what stops the budget restarting).
//!
//! A frozen `now_supplier` makes every due-at assertion exact.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    DeploymentId, StatefulExecResult, TaskError, TaskRegistry, TokenExecutor, Variables,
};

const NOW: &str = "2026-01-01T00:00:00Z";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000091").expect("valid deployment id")
}

/// start → serviceTask(`T`, implementation `flaky`) → serviceTask(`T2`, `tail`) → end.
/// `retry` is spliced into `T`'s extension elements.
fn flow(retry: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="flaky">
              <bpmn:extensionElements>{retry}</bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="T2" implementation="tail"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="T2"/>
            <bpmn:sequenceFlow id="f3" sourceRef="T2" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

/// A `flaky` task that fails with `message` for its first `fail_times` invocations and then
/// succeeds, plus a counter recording how many times it actually ran (the proof that a re-drive
/// RE-EXECUTES rather than replays past).
fn executor_failing(fail_times: usize, message: &str) -> (TokenExecutor, Rc<RefCell<usize>>) {
    let calls = Rc::new(RefCell::new(0usize));
    let counter = Rc::clone(&calls);
    let message = message.to_string();
    let registry = TaskRegistry::new()
        .register("flaky", move |_, _| {
            let mut n = counter.borrow_mut();
            *n += 1;
            if *n <= fail_times {
                Err(TaskError::Failed(message.clone()))
            } else {
                ok_map(&[("charged", boolean(true))])
            }
        })
        .register("tail", |_, _| ok_map(&[("tailRan", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_now_supplier(|| NOW.to_string())
        .build();
    (executor, calls)
}

/// Always-failing `flaky`.
fn executor_always_failing(message: &str) -> (TokenExecutor, Rc<RefCell<usize>>) {
    executor_failing(usize::MAX, message)
}

struct Parked {
    waiting_nodes: Vec<String>,
    completed_nodes: Vec<String>,
    variables: Variables,
    timer_waits: Vec<sutra_executor::TimerWait>,
    retry_attempts: BTreeMap<String, u32>,
}

fn expect_parked(result: StatefulExecResult) -> Parked {
    match result {
        StatefulExecResult::Suspended {
            waiting_nodes,
            completed_nodes,
            variables,
            timer_waits,
            retry_attempts,
            ..
        } => Parked {
            waiting_nodes,
            completed_nodes,
            variables,
            timer_waits,
            retry_attempts,
        },
        StatefulExecResult::Completed { .. } => panic!("expected a retry park, got COMPLETED"),
    }
}

// ============================ the park ==================================================

#[tokio::test]
async fn a_failed_attempt_parks_on_a_timer_instead_of_failing_the_instance() {
    let process = proc(&flow(r#"<q:retry maxAttempts="3"/>"#), "p1");
    let (executor, calls) = executor_always_failing("upstream unavailable");

    let parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .expect("a retryable failure parks; it does not fail the pass"),
    );

    assert_eq!(*calls.borrow(), 1, "the task ran exactly once");
    // The frontier is the failed task itself — that is the wait row the dispatcher persists.
    assert_eq!(parked.waiting_nodes, vec!["T".to_string()]);
    // NOT completed: this omission is the entire re-drive mechanism.
    assert!(
        !parked.completed_nodes.contains(&"T".to_string()),
        "a retry-parked task must not be recorded completed: {:?}",
        parked.completed_nodes
    );
    // The tail did not run.
    assert!(parked.variables.get("tailRan").is_none());
    // One fresh timer row, due at now + the default initialDelay (PT1S).
    assert_eq!(parked.timer_waits.len(), 1);
    assert_eq!(parked.timer_waits[0].node_id, "T");
    assert_eq!(parked.timer_waits[0].due_at, "2026-01-01T00:00:01Z");
    // The durable attempt count: one attempt has failed.
    assert_eq!(parked.retry_attempts.get("T"), Some(&1));
}

#[tokio::test]
async fn the_backoff_grows_geometrically_and_clamps_at_max_delay() {
    // initialDelay PT10S, coefficient 3.0, ceiling PT1M:
    //   attempt 1 -> 10s, attempt 2 -> 30s, attempt 3 -> 90s clamped to 60s.
    let process = proc(
        &flow(
            r#"<q:retry maxAttempts="9" initialDelay="PT10S" backoffCoefficient="3.0"
                        maxDelay="PT1M"/>"#,
        ),
        "p1",
    );
    let (executor, _) = executor_always_failing("still down");

    let expected = [
        ("2026-01-01T00:00:10Z", 1u32),
        ("2026-01-01T00:00:30Z", 2),
        ("2026-01-01T00:01:00Z", 3),
        ("2026-01-01T00:01:00Z", 4),
    ];
    // Attempt 1 is the fresh activation; each later attempt is a timer re-drive seeded from the
    // previous park's durable count.
    let mut parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .unwrap(),
    );
    assert_eq!(parked.timer_waits[0].due_at, expected[0].0);
    assert_eq!(parked.retry_attempts.get("T"), Some(&expected[0].1));

    for (due_at, attempts) in &expected[1..] {
        parked = expect_parked(
            executor
                .resume(
                    &process,
                    "11111111-2222-4333-8444-555555555555",
                    &parked.completed_nodes,
                    parked.variables.clone(),
                    "T",
                    &Variables::new(),
                    dep(),
                    BTreeMap::new(),
                    None,
                    &parked.waiting_nodes,
                    &BTreeMap::new(),
                    &parked.retry_attempts,
                    &BTreeMap::new(),
                )
                .await
                .expect("the re-drive parks again"),
        );
        assert_eq!(
            parked.timer_waits[0].due_at, *due_at,
            "attempt {attempts} due-at"
        );
        assert_eq!(parked.retry_attempts.get("T"), Some(attempts));
    }
}

// ============================ the re-drive ==============================================

#[tokio::test]
async fn the_timer_re_drive_re_executes_the_task_and_a_success_clears_the_counter() {
    // Fails once, then succeeds: the re-drive must RE-RUN the task (not replay past it), and the
    // durable counter must be dropped so it does not linger on every later snapshot.
    let process = proc(&flow(r#"<q:retry maxAttempts="3"/>"#), "p1");
    let (executor, calls) = executor_failing(1, "transient");

    let parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .unwrap(),
    );
    assert_eq!(*calls.borrow(), 1);
    assert_eq!(parked.retry_attempts.get("T"), Some(&1));

    let result = executor
        .resume(
            &process,
            "11111111-2222-4333-8444-555555555555",
            &parked.completed_nodes,
            parked.variables.clone(),
            "T",
            &Variables::new(),
            dep(),
            BTreeMap::new(),
            None,
            &parked.waiting_nodes,
            &BTreeMap::new(),
            &parked.retry_attempts,
            &BTreeMap::new(),
        )
        .await
        .expect("the re-drive succeeds this time");

    assert_eq!(
        *calls.borrow(),
        2,
        "the task ran a SECOND time on the re-drive"
    );
    match result {
        StatefulExecResult::Completed { outputs, .. } => {
            assert_eq!(outputs.get("charged"), Some(&boolean(true)));
            assert_eq!(
                outputs.get("tailRan"),
                Some(&boolean(true)),
                "the tail runs once the retried task finally succeeds"
            );
        }
        StatefulExecResult::Suspended { .. } => panic!("expected COMPLETED after the retry"),
    }
}

#[tokio::test]
async fn a_re_drive_that_fails_again_carries_the_attempt_count_forward() {
    let process = proc(&flow(r#"<q:retry maxAttempts="4"/>"#), "p1");
    let (executor, calls) = executor_always_failing("still broken");

    let first = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .unwrap(),
    );
    let second = expect_parked(
        executor
            .resume(
                &process,
                "11111111-2222-4333-8444-555555555555",
                &first.completed_nodes,
                first.variables.clone(),
                "T",
                &Variables::new(),
                dep(),
                BTreeMap::new(),
                None,
                &first.waiting_nodes,
                &BTreeMap::new(),
                &first.retry_attempts,
                &BTreeMap::new(),
            )
            .await
            .unwrap(),
    );

    assert_eq!(*calls.borrow(), 2);
    // 1 -> 2, not 1 -> 1: the seeded count is what makes the budget finite across re-drives.
    assert_eq!(second.retry_attempts.get("T"), Some(&2));
    assert_eq!(second.waiting_nodes, vec!["T".to_string()]);
    assert!(!second.completed_nodes.contains(&"T".to_string()));
    // Second attempt waits the doubled default delay.
    assert_eq!(second.timer_waits[0].due_at, "2026-01-01T00:00:02Z");
}

// ============================ terminal outcomes =========================================

#[tokio::test]
async fn the_last_attempt_fails_the_instance_under_retry_exhausted() {
    // maxAttempts=2: attempt 1 parks, attempt 2 is the last and must go FATAL rather than park a
    // third time. `SUTRA.RUNTIME.RETRY.EXHAUSTED` (not the bare TASK.UNCAUGHT) is what tells an
    // operator the budget — not the first error — is what ended the instance.
    let process = proc(&flow(r#"<q:retry maxAttempts="2"/>"#), "p1");
    let (executor, calls) = executor_always_failing("permanently down");

    let parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .unwrap(),
    );
    assert_eq!(parked.retry_attempts.get("T"), Some(&1));

    let err = executor
        .resume(
            &process,
            "11111111-2222-4333-8444-555555555555",
            &parked.completed_nodes,
            parked.variables.clone(),
            "T",
            &Variables::new(),
            dep(),
            BTreeMap::new(),
            None,
            &parked.waiting_nodes,
            &BTreeMap::new(),
            &parked.retry_attempts,
            &BTreeMap::new(),
        )
        .await
        .expect_err("the budget is spent — the pass fails");

    assert_eq!(*calls.borrow(), 2, "exactly maxAttempts invocations");
    let d = err.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(
        d.message.contains("permanently down"),
        "the underlying failure is quoted so it is not lost: {}",
        d.message
    );
}

#[tokio::test]
async fn max_attempts_of_one_fails_on_the_first_attempt_without_parking() {
    let process = proc(&flow(r#"<q:retry maxAttempts="1"/>"#), "p1");
    let (executor, calls) = executor_always_failing("nope");

    let err = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect_err("a one-attempt budget is spent immediately");

    assert_eq!(*calls.borrow(), 1);
    assert_eq!(err.to_diagnostic().code, "SUTRA.RUNTIME.RETRY.EXHAUSTED");
}

#[tokio::test]
async fn a_non_retryable_code_short_circuits_the_remaining_attempts() {
    // The budget allows 5, but the failure classifies as a declared non-retryable code, so the
    // instance fails on attempt 1 with no timer parked at all.
    let process = proc(
        &flow(r#"<q:retry maxAttempts="5" nonRetryableCodes="ACCOUNT_CLOSED,CARD_EXPIRED"/>"#),
        "p1",
    );
    let (executor, calls) = executor_always_failing("ACCOUNT_CLOSED: account 42 is closed");

    let err = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect_err("a non-retryable classification fails immediately");

    assert_eq!(*calls.borrow(), 1, "no further attempt is made");
    let d = err.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.RETRY.EXHAUSTED", "{d:?}");
    assert!(
        d.message.contains("ACCOUNT_CLOSED") && d.message.contains("NON-RETRYABLE"),
        "the diagnostic names the classification that stopped it: {}",
        d.message
    );
}

#[tokio::test]
async fn an_unclassified_failure_still_retries_when_other_codes_are_non_retryable() {
    // Ordinary prose must NOT be mistaken for a classification — "connection refused: no route"
    // has a colon but no code-shaped token, so it classifies as SUTRA.RUNTIME.TASK.UNCAUGHT and
    // keeps retrying.
    let process = proc(
        &flow(r#"<q:retry maxAttempts="3" nonRetryableCodes="ACCOUNT_CLOSED"/>"#),
        "p1",
    );
    let (executor, _) = executor_always_failing("connection refused: no route to host");

    let parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .expect("an unclassified failure is retryable"),
    );
    assert_eq!(parked.retry_attempts.get("T"), Some(&1));
}

#[tokio::test]
async fn listing_the_uncaught_code_opts_out_of_retrying_unclassified_failures() {
    // The documented use of the stable code in the list: "never retry a failure the task did not
    // classify".
    let process = proc(
        &flow(r#"<q:retry maxAttempts="5" nonRetryableCodes="SUTRA.RUNTIME.TASK.UNCAUGHT"/>"#),
        "p1",
    );
    let (executor, calls) = executor_always_failing("something went wrong");

    let err = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect_err("unclassified failures are opted out of retry");
    assert_eq!(*calls.borrow(), 1);
    assert_eq!(err.to_diagnostic().code, "SUTRA.RUNTIME.RETRY.EXHAUSTED");
}

// ============================ what retry must NOT touch =================================

#[tokio::test]
async fn a_bpmn_error_routes_to_its_boundary_and_never_retries() {
    // A BPMN error is a MODELLED outcome. Retrying it would re-run the branch the author drew.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="flaky">
              <bpmn:extensionElements><q:retry maxAttempts="5"/></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:boundaryEvent id="B" attachedToRef="T">
              <bpmn:errorEventDefinition errorRef="DECLINED"/>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="H" implementation="tail"/>
            <bpmn:endEvent id="E"/>
            <bpmn:endEvent id="EH"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="H"/>
            <bpmn:sequenceFlow id="f4" sourceRef="H" targetRef="EH"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let calls = Rc::new(RefCell::new(0usize));
    let counter = Rc::clone(&calls);
    let registry = TaskRegistry::new()
        .register("flaky", move |_, _| {
            *counter.borrow_mut() += 1;
            Err(TaskError::BpmnError("DECLINED".to_string()))
        })
        .register("tail", |_, _| ok_map(&[("handled", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_now_supplier(|| NOW.to_string())
        .build();

    let result = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect("the error routes to its boundary");

    assert_eq!(*calls.borrow(), 1, "a BPMN error is never re-attempted");
    match result {
        StatefulExecResult::Completed { outputs, .. } => {
            assert_eq!(outputs.get("handled"), Some(&boolean(true)));
        }
        StatefulExecResult::Suspended { .. } => {
            panic!("a BPMN error must route, not park on a retry timer")
        }
    }
}

#[tokio::test]
async fn a_task_without_a_retry_policy_keeps_its_single_attempt_and_uncaught_code() {
    // The pre-P1-1 behaviour, pinned: no policy means one attempt, then the historical code.
    let process = proc(&flow(""), "p1");
    let (executor, calls) = executor_always_failing("boom");

    let err = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect_err("no policy — the first failure is fatal");

    assert_eq!(*calls.borrow(), 1);
    assert_eq!(err.to_diagnostic().code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
}
