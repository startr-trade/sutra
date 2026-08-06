//! Time-skipping proof for `<q:retry>` backoff (P1-7 time-skipping test runtime) — the Wave B
//! machinery `retry_policy_test.rs` pins, driven here by an ADVANCING
//! [`sutra_executor::TestClock`] `now_supplier` instead of a fixed string.
//!
//! `<q:retry>` is reachable ONLY from a `TaskRegistry`-registered task (see
//! `executor.rs`'s "the ONLY retryable failure: a registered task function that threw"), and the
//! Rust engine's own `serve()` assembly registers NONE — service tasks there route to
//! channel/template/decision dispatch, never a Rust closure (verified while surveying this
//! feature; `docs/plan/now/temporal-gap-implementation.md` P1-1 scopes retry to
//! "registered-task serviceTasks" for exactly this reason). So `<q:retry>` cannot be exercised
//! by pure-BPMN authoring through the shipped engine binary at all, today — not something this
//! change should paper over with a fault-injection hook wired into production assembly. This
//! crate is where the retry PARK/re-drive machinery actually lives (and is already unit-tested),
//! so it is the right level for the time-skipping proof: what changes here versus
//! `retry_policy_test.rs` is that the clock the executor reads for each attempt's due-at is the
//! SAME moving instant a real timer-poller `fire -> resume` cycle would hand it (`now` at the
//! moment of the re-drive), not a constant.
//!
//! The `<= 2s` wall-clock assertion below is the entire point: three parked attempts spanning
//! 1h + 2h + 4h = 7 MODELLED hours run in real milliseconds, because nothing ever slept — only
//! the virtual clock moved, exactly like the engine-level `tests/all/time_skipping_it.rs` proof
//! (PT24H catch timer, R3/PT12H cyclic start) in `sutra-engine`, and exactly the DX the P1-7
//! mission is chasing (Temporal's time-skipping test environment).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    DeploymentId, StatefulExecResult, TaskError, TaskRegistry, TestClock, TokenExecutor, Variables,
};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn dep() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000a7").expect("valid deployment id")
}

/// start -> serviceTask(`T`, implementation `flaky`) -> end. `retry` is spliced into `T`'s
/// extension elements — the same shape `retry_policy_test.rs` uses.
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
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

/// A `flaky` task that always fails, wired to `clock` via `now_supplier` — the executor computes
/// every park's due-at from `clock.now()` at the instant of failure, exactly as a real boot's
/// executor would from the timer poller's virtual `now`.
fn executor_on(clock: &TestClock) -> (TokenExecutor, Rc<RefCell<usize>>) {
    let calls = Rc::new(RefCell::new(0usize));
    let counter = Rc::clone(&calls);
    let registry = TaskRegistry::new().register("flaky", move |_, _| {
        *counter.borrow_mut() += 1;
        Err(TaskError::Failed("upstream unavailable".to_string()))
    });
    let now_clock = clock.clone();
    let executor = TokenExecutor::builder(registry)
        .with_now_supplier(move || now_clock.rfc3339())
        .build();
    (executor, calls)
}

struct Parked {
    waiting_nodes: Vec<String>,
    completed_nodes: Vec<String>,
    variables: Variables,
    retry_attempts: BTreeMap<String, u32>,
    due_at: String,
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
            retry_attempts,
            due_at: timer_waits[0].due_at.clone(),
        },
        StatefulExecResult::Completed { .. } => panic!("expected a retry park, got COMPLETED"),
    }
}

#[tokio::test]
async fn retry_backoff_chain_fast_forwards_through_hours_of_modelled_time_in_real_milliseconds() {
    // initialDelay PT1H, coefficient 2, ceiling PT6H: attempts due at +1h, +2h, +4h.
    let clock = TestClock::new(time::macros::datetime!(2026-01-01 00:00:00 UTC));
    let (executor, calls) = executor_on(&clock);
    let process = proc(
        &flow(
            r#"<q:retry maxAttempts="4" initialDelay="PT1H" backoffCoefficient="2"
                        maxDelay="PT6H"/>"#,
        ),
        "p1",
    );

    let started = std::time::Instant::now();

    let mut parked = expect_parked(
        executor
            .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
            .await
            .expect("a retryable failure parks; it does not fail the pass"),
    );
    let expected = [
        ("2026-01-01T01:00:00Z", 1u32),
        ("2026-01-01T03:00:00Z", 2),
        ("2026-01-01T07:00:00Z", 3),
    ];
    assert_eq!(parked.due_at, expected[0].0, "attempt 1 due-at (+1h)");
    assert_eq!(parked.retry_attempts.get("T"), Some(&expected[0].1));

    for (want_due, want_attempt) in &expected[1..] {
        // FAST-FORWARD: jump the virtual clock straight to the parked due-at — the instant a
        // real timer poller would fire this row and re-drive it. No sleep, real or virtual-step.
        clock.set(
            OffsetDateTime::parse(&parked.due_at, &Rfc3339).expect("due-at is valid RFC 3339"),
        );
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
        assert_eq!(parked.due_at, *want_due, "attempt {want_attempt} due-at");
        assert_eq!(parked.retry_attempts.get("T"), Some(want_attempt));
    }

    assert_eq!(
        *calls.borrow(),
        3,
        "each fast-forwarded park actually RE-RAN the task (a re-drive, not a replay)"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "three parks spanning 1h + 2h + 4h = 7 modelled hours must run in real milliseconds \
         (only the virtual clock moved): took {:?}",
        started.elapsed()
    );
}
