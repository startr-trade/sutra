//! Loader conformance for `<q:retry>` — the per-task retry policy (P1-1).
//!
//! Two things are pinned here and nothing else belongs in this file: the ATTRIBUTE contract
//! (what parses, what defaults, what fails closed) and the PLACEMENT contract (which nodes may
//! carry the element at all). Both are load-time gates, so every negative case asserts a stable
//! `SUTRA.CONFIG.BPMN.RETRY_*` code rather than a message — the message is free to improve.
//!
//! The runtime half — attempt persistence, backoff scheduling, non-retryable short-circuit,
//! exhaustion — lives in `sutra-executor/tests/all/retry_policy_test.rs`.

use sutra_bpmn::{BpmnModelLoader, ProcessModule};

fn load(bpmn: &str) -> Result<ProcessModule, sutra_bpmn::SutraError> {
    BpmnModelLoader::new().load(bpmn.as_bytes())
}

fn defs(inner: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">{inner}</bpmn:process>
        </bpmn:definitions>"#
    )
}

/// start → serviceTask(`T`, a registered task) → end, with `retry` spliced into `T`'s
/// extension elements.
fn task_process(retry: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="chargeCard">
          <bpmn:extensionElements>{retry}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#
    ))
}

fn expect_err(bpmn: &str) -> sutra_bpmn::SutraError {
    match load(bpmn) {
        Err(e) => e,
        Ok(_) => panic!("expected the load to fail closed"),
    }
}

// ============================ attribute contract ========================================

#[test]
fn a_minimal_retry_takes_every_documented_default() {
    let module = load(&task_process(r#"<q:retry maxAttempts="3"/>"#)).expect("loads");
    let policy = module
        .process("p1")
        .unwrap()
        .retry_policy("T")
        .expect("T carries a retry policy");
    assert_eq!(policy.max_attempts, 3);
    assert_eq!(policy.initial_delay, "PT1S");
    assert_eq!(policy.max_delay, "PT5M");
    assert_eq!(policy.backoff_coefficient, 2.0);
    assert!(policy.non_retryable_codes.is_empty());
}

#[test]
fn every_attribute_round_trips_verbatim() {
    let module = load(&task_process(
        r#"<q:retry maxAttempts="5" initialDelay="PT10S" backoffCoefficient="1.5"
                    maxDelay="PT2M" nonRetryableCodes="ACCOUNT_CLOSED, CARD_EXPIRED"/>"#,
    ))
    .expect("loads");
    let policy = module.process("p1").unwrap().retry_policy("T").unwrap();
    assert_eq!(policy.max_attempts, 5);
    assert_eq!(policy.initial_delay, "PT10S");
    assert_eq!(policy.max_delay, "PT2M");
    assert_eq!(policy.backoff_coefficient, 1.5);
    // Comma list is split and trimmed; the codes keep their authored spelling.
    assert_eq!(
        policy.non_retryable_codes,
        vec!["ACCOUNT_CLOSED".to_string(), "CARD_EXPIRED".to_string()]
    );
}

#[test]
fn max_attempts_of_one_is_a_legal_never_retry_declaration() {
    // Not a no-op worth rejecting: it documents in the model that the author considered retries
    // and decided against them, which a reviewer can see and a missing element cannot convey.
    let module = load(&task_process(r#"<q:retry maxAttempts="1"/>"#)).expect("loads");
    assert_eq!(
        module
            .process("p1")
            .unwrap()
            .retry_policy("T")
            .unwrap()
            .max_attempts,
        1
    );
}

#[test]
fn a_missing_max_attempts_is_a_load_error() {
    let e = expect_err(&task_process(r#"<q:retry initialDelay="PT5S"/>"#));
    assert_eq!(
        e.code, "SUTRA.CONFIG.BPMN.RETRY_MAX_ATTEMPTS_INVALID",
        "{e}"
    );
}

#[test]
fn a_zero_or_negative_or_non_numeric_max_attempts_is_a_load_error() {
    for raw in ["0", "-2", "many", "3.5", ""] {
        let e = expect_err(&task_process(&format!(r#"<q:retry maxAttempts="{raw}"/>"#)));
        assert_eq!(
            e.code, "SUTRA.CONFIG.BPMN.RETRY_MAX_ATTEMPTS_INVALID",
            "maxAttempts='{raw}' must fail closed: {e}"
        );
    }
}

#[test]
fn an_unparseable_delay_is_a_load_error() {
    let e = expect_err(&task_process(
        r#"<q:retry maxAttempts="3" initialDelay="30 seconds"/>"#,
    ));
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID", "{e}");

    let e = expect_err(&task_process(
        r#"<q:retry maxAttempts="3" maxDelay="soon"/>"#,
    ));
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID", "{e}");
}

#[test]
fn a_max_delay_below_the_initial_delay_is_a_load_error() {
    // The ceiling clamps the growing delay, so it can never be the smaller of the two — an
    // author who wrote this meant something the engine cannot honour.
    let e = expect_err(&task_process(
        r#"<q:retry maxAttempts="3" initialDelay="PT1M" maxDelay="PT10S"/>"#,
    ));
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID", "{e}");
}

#[test]
fn a_backoff_coefficient_below_one_is_a_load_error() {
    // Below 1 SHRINKS the wait on each failure — the opposite of backoff, and a fast way to
    // hammer the dependency that is already failing.
    for raw in ["0.5", "0", "-1", "fast"] {
        let e = expect_err(&task_process(&format!(
            r#"<q:retry maxAttempts="3" backoffCoefficient="{raw}"/>"#
        )));
        assert_eq!(
            e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID",
            "backoffCoefficient='{raw}' must fail closed: {e}"
        );
    }
}

#[test]
fn a_coefficient_of_exactly_one_is_a_legal_fixed_delay() {
    let module = load(&task_process(
        r#"<q:retry maxAttempts="4" backoffCoefficient="1.0"/>"#,
    ))
    .expect("loads");
    assert_eq!(
        module
            .process("p1")
            .unwrap()
            .retry_policy("T")
            .unwrap()
            .backoff_coefficient,
        1.0
    );
}

#[test]
fn a_non_retryable_codes_list_that_names_nothing_is_a_load_error() {
    // Present-but-empty is rejected rather than read as "retry everything": the author wrote the
    // attribute, so they meant something by it, and silently ignoring it would be the worst
    // possible answer.
    for raw in [" ", ",", " , ,"] {
        let e = expect_err(&task_process(&format!(
            r#"<q:retry maxAttempts="3" nonRetryableCodes="{raw}"/>"#
        )));
        assert_eq!(
            e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID",
            "nonRetryableCodes='{raw}' must fail closed: {e}"
        );
    }
}

// ============================ placement contract ========================================

#[test]
fn a_retry_on_a_channel_call_task_with_a_routeless_timeout_is_valid() {
    // PREMISE OVERTURNED (F1 — retry reachability): this placement used to be a load error on
    // the theory that the outbox retry curve plus the timeout boundary covered the failure
    // space. It does not — the outbox curve retries the DELIVERY only (and by default forever),
    // and the timeout without a policy simply kills the instance. `<q:retry>` on a channel-call
    // now declares the task-level budget over the honest failure set (the route-less timeout
    // firing; a terminally-poisoned request delivery), with the re-drive RE-EMITTING the
    // request. The route-less `<q:timeout>` is the failure detector the policy rides on.
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>
            <q:alias name="k" expression="event.idempotencyKey"/>
            <q:timeout duration="PT30S"/>
            <q:retry maxAttempts="3"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>"#,
    );
    let module = load(&bpmn).expect("a channel-call <q:retry> with <q:timeout> loads");
    let p = module.process("p1").unwrap();
    let policy = p.retry_policy("Call").expect("Call carries the policy");
    assert_eq!(policy.max_attempts, 3);
    assert!(
        !p.is_sync_eligible(),
        "a channel-call process is stateful regardless"
    );
}

#[test]
fn a_retry_on_a_channel_call_with_a_routed_timeout_boundary_is_a_load_error() {
    // A timer boundary WITH outgoing flows is an AUTHORED timeout outcome, and a modelled
    // outcome always wins over a retry policy (exactly as a BPMN error never retries on a
    // registered task). Combining the two would load a policy that can never fire on a
    // timeout — a silent near-no-op, refused fail-closed.
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>
            <q:alias name="k" expression="event.idempotencyKey"/>
            <q:retry maxAttempts="3"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:boundaryEvent id="B" attachedToRef="Call">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT30S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:serviceTask id="H" implementation="notifyTimeout"/>
        <bpmn:endEvent id="E"/>
        <bpmn:endEvent id="EH"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="H"/>
        <bpmn:sequenceFlow id="f4" sourceRef="H" targetRef="EH"/>"#,
    );
    let e = expect_err(&bpmn);
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_NOT_APPLICABLE", "{e}");
}

#[test]
fn a_retry_on_a_non_service_task_is_a_load_error() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:userTask id="U">
          <bpmn:extensionElements><q:retry maxAttempts="3"/></bpmn:extensionElements>
        </bpmn:userTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>"#,
    );
    let e = expect_err(&bpmn);
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_NOT_APPLICABLE", "{e}");
}

#[test]
fn a_retry_on_a_looped_service_task_is_a_load_error() {
    // A retry parks a durable timer; the engine cannot re-enter a single loop iteration.
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="chargeCard">
          <bpmn:extensionElements><q:retry maxAttempts="3"/></bpmn:extensionElements>
          <bpmn:multiInstanceLoopCharacteristics>
            <bpmn:loopCardinality>3</bpmn:loopCardinality>
          </bpmn:multiInstanceLoopCharacteristics>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#,
    );
    let e = expect_err(&bpmn);
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_NOT_APPLICABLE", "{e}");
}

#[test]
fn a_retry_inside_an_embedded_sub_process_is_a_load_error() {
    // The inline sub-process runner discards its sub-state's wait frontier, so a backoff timer
    // recorded there would be silently dropped.
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:subProcess id="SP">
          <bpmn:startEvent id="IS"/>
          <bpmn:serviceTask id="IT" implementation="chargeCard">
            <bpmn:extensionElements><q:retry maxAttempts="3"/></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="IE"/>
          <bpmn:sequenceFlow id="if1" sourceRef="IS" targetRef="IT"/>
          <bpmn:sequenceFlow id="if2" sourceRef="IT" targetRef="IE"/>
        </bpmn:subProcess>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="SP"/>
        <bpmn:sequenceFlow id="f2" sourceRef="SP" targetRef="E"/>"#,
    );
    let e = expect_err(&bpmn);
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_NOT_APPLICABLE", "{e}");
}

// ============================ structural classification =================================

#[test]
fn a_retry_task_makes_an_otherwise_synchronous_process_stateful() {
    // The retry wait is a durable TIMER park, and the synchronous executor has no snapshot, no
    // timer rows and no resume. Classifying the process stateful UP FRONT is what gives a first
    // failed attempt somewhere to land; the alternative is a policy that silently never fires.
    let plain = load(&task_process("")).expect("loads");
    assert!(
        plain.process("p1").unwrap().is_sync_eligible(),
        "the same process without <q:retry> is sync-eligible"
    );

    let retried = load(&task_process(r#"<q:retry maxAttempts="3"/>"#)).expect("loads");
    let p = retried.process("p1").unwrap();
    assert!(
        !p.is_sync_eligible(),
        "a <q:retry> task forces the stateful path"
    );
    assert!(p.has_retry_policy("T"));
    assert!(
        !p.has_retry_policy("S"),
        "the start event carries no policy"
    );
}
