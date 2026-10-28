//! Expression-valued timers at the executor seam: each timer kind armed from a flow variable and
//! from the deployment configuration (`config.<key>`), each failure mode raising one terminal
//! `SUTRA.RUNTIME.TIMER.EXPRESSION_INVALID`, and literal timers scheduling exactly as before.
//!
//! A frozen `now_supplier` makes every due-at exact.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::common::*;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, ExecError, OutboundChannelRegistry,
    ResolvedOutboundChannel, StatefulExecResult, TaskError, TaskRegistry, TimerWait, TokenExecutor,
    Variables,
};

const NOW: &str = "2026-01-01T00:00:00Z";
const CODE: &str = "SUTRA.RUNTIME.TIMER.EXPRESSION_INVALID";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000e7").expect("valid deployment id")
}

fn config() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("window".to_string(), "PT2M".to_string()),
        ("retryDelay".to_string(), "PT15S".to_string()),
        ("seconds".to_string(), "9".to_string()),
    ])
}

fn executor(tasks: TaskRegistry) -> TokenExecutor {
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
    let config = Arc::new(config());
    TokenExecutor::builder(tasks)
        .with_feel()
        .with_outbound_channels(registry)
        .with_emission_sink(Rc::new(CollectingSink::new()) as Rc<dyn EmissionSink>)
        .with_deployment_config(move |d| (d == &dep()).then(|| Arc::clone(&config)))
        .with_now_supplier(|| NOW.to_string())
        .build()
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

fn catch(child: &str, text: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:intermediateCatchEvent id="Wait">
          <bpmn:timerEventDefinition><bpmn:{child}>{text}</bpmn:{child}></bpmn:timerEventDefinition>
        </bpmn:intermediateCatchEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#
    ))
}

async fn run(
    executor: &TokenExecutor,
    bpmn: &str,
    variables: Variables,
) -> Result<StatefulExecResult, ExecError> {
    executor
        .execute_stateful_from(&proc(bpmn, "p1"), variables, dep(), BTreeMap::new(), None)
        .await
}

fn timers(result: Result<StatefulExecResult, ExecError>) -> Vec<TimerWait> {
    match result.expect("the pass parks") {
        StatefulExecResult::Suspended { timer_waits, .. } => timer_waits,
        StatefulExecResult::Completed { .. } => panic!("expected a park, got COMPLETED"),
    }
}

fn due_of(waits: &[TimerWait], node: &str) -> String {
    waits
        .iter()
        .find(|t| t.node_id == node)
        .unwrap_or_else(|| panic!("no timer for {node}: {waits:?}"))
        .due_at
        .clone()
}

fn expect_invalid(result: Result<StatefulExecResult, ExecError>) -> String {
    match result {
        Err(e) => {
            assert_eq!(e.code(), CODE, "{e:?}");
            e.message()
        }
        Ok(_) => panic!("expected {CODE}"),
    }
}

// ---- intermediate catch ----------------------------------------------------------------------

#[tokio::test]
async fn a_literal_catch_schedules_exactly_as_before() {
    let ex = executor(TaskRegistry::new());
    let waits = timers(run(&ex, &catch("timeDuration", "PT45S"), Variables::new()).await);
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:00:45Z");
}

#[tokio::test]
async fn a_catch_duration_from_a_flow_variable() {
    let ex = executor(TaskRegistry::new());
    // An ISO string variable.
    let waits = timers(
        run(
            &ex,
            &catch("timeDuration", "payload.wait"),
            vars(&[("payload", fmap(&[("wait", string("PT45S"))]))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:00:45Z");
    // A FEEL days-and-time duration computed from a number.
    let waits = timers(
        run(
            &ex,
            &catch("timeDuration", r#"duration("PT" + string(secs) + "S")"#),
            vars(&[("secs", num(7))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:00:07Z");
}

#[tokio::test]
async fn a_catch_duration_from_config() {
    let ex = executor(TaskRegistry::new());
    let waits = timers(
        run(
            &ex,
            &catch("timeDuration", "config.window"),
            Variables::new(),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:02:00Z");
    // Config values are strings: number() converts one for arithmetic.
    let waits = timers(
        run(
            &ex,
            &catch(
                "timeDuration",
                r#"duration("PT" + string(number(config.seconds) * 2) + "S")"#,
            ),
            Variables::new(),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:00:18Z");
    // `config` in a timer expression is the deployment configuration, even when an instance
    // variable has the same name.
    let waits = timers(
        run(
            &ex,
            &catch("timeDuration", "config.window"),
            vars(&[("config", fmap(&[("window", string("PT1S"))]))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-01-01T00:02:00Z");
}

#[tokio::test]
async fn a_catch_date_from_a_flow_variable() {
    let ex = executor(TaskRegistry::new());
    let waits = timers(
        run(
            &ex,
            &catch("timeDate", "deadline"),
            vars(&[("deadline", string("2026-03-01T15:00:00+05:30"))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-03-01T09:30:00Z");
    let waits = timers(
        run(
            &ex,
            &catch("timeDate", r#"date and time(deadline)"#),
            vars(&[("deadline", string("2026-03-02T00:00:00Z"))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Wait"), "2026-03-02T00:00:00Z");
}

#[tokio::test]
async fn every_bad_result_fails_once_with_the_timer_code() {
    let ex = executor(TaskRegistry::new());
    for (child, expr, variables, needle) in [
        ("timeDuration", "missing", Variables::new(), "null"),
        (
            "timeDuration",
            "w",
            vars(&[("w", string("soon"))]),
            "not a usable duration",
        ),
        ("timeDuration", "w", vars(&[("w", num(5))]), "number"),
        (
            "timeDuration",
            r#"duration("P1M")"#,
            Variables::new(),
            "years-and-months",
        ),
        (
            "timeDuration",
            r#"duration("-PT5S")"#,
            Variables::new(),
            "negative",
        ),
        (
            "timeDuration",
            "config.undeclared",
            Variables::new(),
            "null",
        ),
        (
            "timeDate",
            "d",
            vars(&[("d", string("2026-03-01T09:30:00"))]),
            "not a usable instant",
        ),
        (
            "timeDate",
            r#"date and time("2026-03-01T09:30:00")"#,
            Variables::new(),
            "no offset or zone",
        ),
    ] {
        let message = expect_invalid(run(&ex, &catch(child, expr), variables).await);
        assert!(message.contains(needle), "'{expr}': {message}");
        assert!(message.contains("Wait"), "names the node: {message}");
    }
}

// ---- boundaries and q:timeout ----------------------------------------------------------------

fn listen_window(window: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:receiveTask id="Listen">
          <bpmn:extensionElements><q:source channel="status" name="status"/></bpmn:extensionElements>
        </bpmn:receiveTask>
        <bpmn:boundaryEvent id="Timeout" attachedToRef="Listen">
          <bpmn:timerEventDefinition><bpmn:timeDuration>{window}</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:endEvent id="TimedOut"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Listen"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Listen" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="Timeout" targetRef="TimedOut"/>"#
    ))
}

#[tokio::test]
async fn a_receive_task_boundary_from_a_flow_variable() {
    let ex = executor(TaskRegistry::new());
    let waits = timers(
        run(
            &ex,
            &listen_window("payload.window"),
            vars(&[("payload", fmap(&[("window", string("PT30S"))]))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Timeout"), "2026-01-01T00:00:30Z");
    let message =
        expect_invalid(run(&ex, &listen_window("payload.window"), Variables::new()).await);
    assert!(message.contains("Timeout"), "{message}");
}

fn call(timeout: &str, retry: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>
            <q:alias name="callKey" expression="key"/>
            <q:timeout duration="{timeout}"/>{retry}
          </bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>"#
    ))
}

#[tokio::test]
async fn a_q_timeout_from_a_flow_variable_and_from_config() {
    let ex = executor(TaskRegistry::new());
    let waits = timers(
        run(
            &ex,
            &call("budget", ""),
            vars(&[("key", string("K1")), ("budget", string("PT3S"))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Call#timeout"), "2026-01-01T00:00:03Z");
    let waits = timers(
        run(
            &ex,
            &call("config.window", ""),
            vars(&[("key", string("K2"))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "Call#timeout"), "2026-01-01T00:02:00Z");
    // The literal form is unchanged.
    let waits = timers(run(&ex, &call("PT30S", ""), vars(&[("key", string("K3"))])).await);
    assert_eq!(due_of(&waits, "Call#timeout"), "2026-01-01T00:00:30Z");
}

// ---- q:retry ---------------------------------------------------------------------------------

fn retry_flow(retry: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="flaky">
          <bpmn:extensionElements>{retry}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#
    ))
}

fn failing() -> TaskRegistry {
    TaskRegistry::new().register("flaky", |_, _| Err(TaskError::Failed("down".into())))
}

#[tokio::test]
async fn a_retry_delay_from_config_and_from_a_variable() {
    let ex = executor(failing());
    let waits = timers(
        run(
            &ex,
            &retry_flow(
                r#"<q:retry maxAttempts="3" initialDelay="config.retryDelay" maxDelay="PT1H"/>"#,
            ),
            Variables::new(),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "T"), "2026-01-01T00:00:15Z");
    let waits = timers(
        run(
            &ex,
            &retry_flow(
                r#"<q:retry maxAttempts="3" initialDelay="PT1S" maxDelay="cap" backoffCoefficient="1.0"/>"#,
            ),
            vars(&[("cap", string("PT5S"))]),
        )
        .await,
    );
    assert_eq!(due_of(&waits, "T"), "2026-01-01T00:00:01Z");
}

#[tokio::test]
async fn a_bad_retry_delay_fails_once_and_is_never_itself_retried() {
    let ex = executor(failing());
    let message = expect_invalid(
        run(
            &ex,
            &retry_flow(r#"<q:retry maxAttempts="5" initialDelay="missing"/>"#),
            Variables::new(),
        )
        .await,
    );
    assert!(message.contains("initialDelay"), "{message}");
    // A resolved ceiling below the initial delay is refused when the backoff is armed.
    let message = expect_invalid(
        run(
            &ex,
            &retry_flow(r#"<q:retry maxAttempts="5" initialDelay="PT1M" maxDelay="cap"/>"#),
            vars(&[("cap", string("PT10S"))]),
        )
        .await,
    );
    assert!(message.contains("maxDelay"), "{message}");
}
