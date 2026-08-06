//! Stateful executor conformance (contract-driven):
//! channel-call park + collected request emission, replay-safe re-park, timer
//! boundary fire (routed + `<q:timeout>` error form), intermediate timer catch, and the
//! task input/output scoping + render-capture rules.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, OutboundChannelRegistry, ResolvedOutboundChannel,
    StatefulExecResult, TaskRegistry, TemplateEngineRegistry, TemplateRegistry, TimerFire,
    TokenExecutor, Variables,
};

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000071").expect("valid deployment id")
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
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build()
}

const CALL_FLOW: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p1">
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>
            <q:timeout duration="PT30S"/>
            <q:alias name="callKey" expression="e2eId"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

#[tokio::test]
async fn channel_call_parks_with_collected_request_and_timer_wait() {
    let process = proc(CALL_FLOW, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let result = executor
        .execute_stateful_from(
            &process,
            vars(&[
                ("e2eId", string("E2E-1")),
                ("requestBody", string("<Req>1</Req>")),
            ]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let StatefulExecResult::Suspended {
        waiting_nodes,
        timer_waits,
        ..
    } = result
    else {
        panic!("expected Suspended");
    };
    // The HOST is the wait frontier; the timer boundary rides timer_waits only.
    assert_eq!(waiting_nodes, vec!["Call".to_string()]);
    assert_eq!(timer_waits.len(), 1);
    assert_eq!(timer_waits[0].node_id, "Call#timeout");
    assert_eq!(timer_waits[0].due_at, "2026-01-01T00:00:30Z");

    // The request emission was COLLECTED (commits with the park step).
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "http://sink.example/req");
    assert_eq!(emissions[0].body.get(), &b"<Req>1</Req>".to_vec());
    assert_eq!(emissions[0].node_id, "Call");
}

#[tokio::test]
async fn unknown_outbound_channel_fails_closed() {
    let process = proc(CALL_FLOW, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_emission_sink(sink as Rc<dyn EmissionSink>)
        .build(); // no outbound channels registered

    let e = executor
        .execute_stateful_from(
            &process,
            vars(&[("e2eId", string("E2E-1"))]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.CONFIG.CHANNEL.OUTBOUND_UNKNOWN");
}

#[tokio::test]
async fn correlated_resume_walks_past_the_satisfied_call_without_resending() {
    let process = proc(CALL_FLOW, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let result = executor
        .resume(
            &process,
            "11111111-2222-4333-8444-555555555555",
            &["S".to_string()],
            vars(&[("e2eId", string("E2E-1"))]),
            "Call",
            &vars(&[("callStatus", string("APPROVED"))]),
            dep(),
            BTreeMap::new(),
            None,
            &["Call".to_string()],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();

    let StatefulExecResult::Completed { outputs, .. } = result else {
        panic!("expected Completed");
    };
    assert_eq!(outputs.get("callStatus"), Some(&string("APPROVED")));
    assert!(sink.is_empty(), "the satisfied call must NOT re-send");
}

#[tokio::test]
async fn resume_of_a_sibling_branch_reparks_the_call_without_resending_or_rearming() {
    // Parallel: one branch parks a userTask, the other a channel-call. Satisfying the
    // userTask re-parks the call — no second request, no timer reset.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:parallelGateway id="Fork"/>
            <bpmn:userTask id="U"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:timeout duration="PT30S"/>
                <q:alias name="callKey" expression="e2eId"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:parallelGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="U"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="Call"/>
            <bpmn:sequenceFlow id="f4" sourceRef="U" targetRef="Join"/>
            <bpmn:sequenceFlow id="f5" sourceRef="Call" targetRef="Join"/>
            <bpmn:sequenceFlow id="f6" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    // Initial pass: both branches park; ONE request emission; one fresh timer.
    let first = executor
        .execute_stateful_from(
            &process,
            vars(&[("e2eId", string("E2E-1"))]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes,
        timer_waits,
        completed_nodes,
        ..
    } = first
    else {
        panic!("expected Suspended");
    };
    assert!(waiting_nodes.contains(&"U".to_string()));
    assert!(waiting_nodes.contains(&"Call".to_string()));
    assert_eq!(sink.len(), 1);
    assert_eq!(timer_waits.len(), 1);

    // Relay satisfies the userTask; the still-pending call RE-PARKS quietly.
    let resumed = executor
        .resume(
            &process,
            "11111111-2222-4333-8444-555555555555",
            &completed_nodes,
            vars(&[("e2eId", string("E2E-1"))]),
            "U",
            &Variables::new(),
            dep(),
            BTreeMap::new(),
            None,
            &waiting_nodes,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes: still_waiting,
        timer_waits: fresh_timers,
        ..
    } = resumed
    else {
        panic!("expected Suspended (the call still waits)");
    };
    assert_eq!(still_waiting, vec!["Call".to_string()]);
    assert_eq!(sink.len(), 1, "no second request emission");
    assert!(fresh_timers.is_empty(), "the pending timer must not re-arm");
}

#[tokio::test]
async fn fired_timer_boundary_routes_the_timeout_path() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:alias name="callKey" expression="e2eId"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:boundaryEvent id="B" attachedToRef="Call">
              <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="MarkTimeout" implementation="${mark}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:endEvent id="ETimeout"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="MarkTimeout"/>
            <bpmn:sequenceFlow id="f4" sourceRef="MarkTimeout" targetRef="ETimeout"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn);
    let process = module.process("p1").unwrap().clone();
    let sink = Rc::new(CollectingSink::new());
    let registry =
        TaskRegistry::new().register("mark", |_, _| ok_map(&[("path", string("timeout"))]));
    let executor = TokenExecutor::builder(registry)
        .with_feel()
        .with_outbound_channels(outbound())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();

    let fire = TimerFire {
        deployment: dep(),
        instance_id: "11111111-2222-4333-8444-555555555555".to_string(),
        node_id: "B".to_string(),
        due_at: "2026-01-01T00:00:01Z".to_string(),
        fired_at: "2026-01-01T00:00:01.2Z".to_string(),
    };
    let result = executor
        .resume_timer(
            &process,
            &fire,
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
        .unwrap();

    let StatefulExecResult::Completed {
        outputs,
        visited_nodes,
        ..
    } = result
    else {
        panic!("expected Completed via the timeout path");
    };
    assert_eq!(outputs.get("path"), Some(&string("timeout")));
    assert!(visited_nodes.contains("ETimeout"));
    assert!(!visited_nodes.contains("E"), "the happy path must not run");
    assert!(sink.is_empty(), "the fired call must NOT re-send");
}

#[tokio::test]
async fn fired_q_timeout_without_route_raises_the_timeout_error() {
    let process = proc(CALL_FLOW, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let fire = TimerFire {
        deployment: dep(),
        instance_id: "11111111-2222-4333-8444-555555555555".to_string(),
        node_id: "Call#timeout".to_string(),
        due_at: "2026-01-01T00:00:30Z".to_string(),
        fired_at: "2026-01-01T00:00:30.5Z".to_string(),
    };
    let e = executor
        .resume_timer(
            &process,
            &fire,
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
        .unwrap_err();
    let d = e.to_diagnostic();
    assert_eq!(d.code, "SUTRA.RUNTIME.ERROR.UNCAUGHT");
    assert!(
        d.message.contains("SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT"),
        "got: {}",
        d.message
    );
}

#[tokio::test]
async fn fired_q_timeout_is_catchable_by_an_error_boundary() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:timeout duration="PT1S"/>
                <q:alias name="callKey" expression="e2eId"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:boundaryEvent id="Catch" attachedToRef="Call">
              <bpmn:errorEventDefinition/>
            </bpmn:boundaryEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:endEvent id="ECaught"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Catch" targetRef="ECaught"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let fire = TimerFire {
        deployment: dep(),
        instance_id: "11111111-2222-4333-8444-555555555555".to_string(),
        node_id: "Call#timeout".to_string(),
        due_at: "2026-01-01T00:00:01Z".to_string(),
        fired_at: "2026-01-01T00:00:01.5Z".to_string(),
    };
    let result = executor
        .resume_timer(
            &process,
            &fire,
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
        .unwrap();
    let StatefulExecResult::Completed { visited_nodes, .. } = result else {
        panic!("expected Completed via the error boundary");
    };
    assert!(visited_nodes.contains("ECaught"));
}

#[tokio::test]
async fn intermediate_timer_catch_parks_then_fire_completes() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition><bpmn:timeDuration>PT0.25S</bpmn:timeDuration></bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let parked = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes,
        timer_waits,
        completed_nodes,
        ..
    } = parked
    else {
        panic!("expected Suspended");
    };
    // A timer catch is BOTH the token position and a timer row.
    assert_eq!(waiting_nodes, vec!["Wait".to_string()]);
    assert_eq!(timer_waits.len(), 1);
    assert_eq!(timer_waits[0].node_id, "Wait");
    assert_eq!(timer_waits[0].due_at, "2026-01-01T00:00:00.25Z");

    let fire = TimerFire {
        deployment: dep(),
        instance_id: "11111111-2222-4333-8444-555555555555".to_string(),
        node_id: "Wait".to_string(),
        due_at: timer_waits[0].due_at.clone(),
        fired_at: "2026-01-01T00:00:00.3Z".to_string(),
    };
    let result = executor
        .resume_timer(
            &process,
            &fire,
            &completed_nodes,
            Variables::new(),
            BTreeMap::new(),
            None,
            &waiting_nodes,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(matches!(result, StatefulExecResult::Completed { .. }));
}

// ---- task I/O scoping + render capture ----------------------------------------------------

fn template_executor(template: &str) -> TokenExecutor {
    let mut templates = TemplateRegistry::new();
    templates.register("render.hbs", template.as_bytes().to_vec());
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new().register(sutra_executor::HbsTemplateEngine::new()),
            templates,
        )
        .build()
}

#[tokio::test]
async fn output_variable_captures_the_render_alongside_response_body() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="render.hbs">
              <bpmn:extensionElements><q:output variable="renderedRequest"/></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let executor = template_executor("Hello {{name}}");

    let result = executor
        .execute_sync(&process, vars(&[("name", string("world"))]))
        .await
        .unwrap();
    // The render is re-readable downstream AND still lands as responseBody.
    assert_eq!(
        result.output("renderedRequest"),
        Some(&string("Hello world"))
    );
    assert_eq!(result.output("responseBody"), Some(&string("Hello world")));
}

#[tokio::test]
async fn template_scoped_inputs_and_outputs() {
    // Inputs: only `name` is visible (secret is NOT); outputs: only `renderedRequest`
    // writes back — responseBody DROPS (un-mapped task-local writes drop).
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:dataObject id="doName" name="name"/>
            <bpmn:dataObject id="doOut" name="renderedRequest"/>
            <bpmn:serviceTask id="T" implementation="render.hbs">
              <bpmn:extensionElements><q:output variable="renderedRequest"/></bpmn:extensionElements>
              <bpmn:dataInputAssociation><bpmn:sourceRef>doName</bpmn:sourceRef></bpmn:dataInputAssociation>
              <bpmn:dataOutputAssociation><bpmn:targetRef>doOut</bpmn:targetRef></bpmn:dataOutputAssociation>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    // Strict rendering: the un-mapped `secret` must be ABSENT from the scoped view,
    // so it needs the coalesce fallback — proving the input scoping.
    let executor = template_executor(r#"name={{name}} secret={{coalesce secret "-"}}"#);

    let result = executor
        .execute_sync(
            &process,
            vars(&[("name", string("bob")), ("secret", string("s3cr3t"))]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.output("renderedRequest"),
        Some(&string("name=bob secret=-"))
    );
    assert_eq!(
        result.output("responseBody"),
        None,
        "un-mapped task-local writes drop under declared outputs"
    );
}

// ---- timeDate due-at computation (P1-5b) ----------------------------------------------------

/// An intermediate timer catch declaring `<bpmn:timeDate>` parks on the ABSOLUTE instant, not on
/// `now + something`. The clock is pinned (`with_now_supplier`) so the assertion is exact.
#[tokio::test]
async fn a_time_date_catch_parks_on_the_absolute_instant() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition>
                <bpmn:timeDate>2026-06-01T12:00:00Z</bpmn:timeDate>
              </bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let result = executor
        .execute_stateful_from(&process, Variables::new(), dep(), BTreeMap::new(), None)
        .await
        .unwrap();
    let StatefulExecResult::Suspended { timer_waits, .. } = result else {
        panic!("expected Suspended");
    };
    assert_eq!(timer_waits.len(), 1);
    assert_eq!(timer_waits[0].node_id, "Wait");
    // The pinned clock is 2026-01-01T00:00:00Z; the due-at ignores it entirely.
    assert_eq!(timer_waits[0].due_at, "2026-06-01T12:00:00Z");
}

/// A `<bpmn:timeDate>` already in the PAST is legal and parks ALREADY DUE — the poller fires it
/// on its next tick. This is the documented past-date semantics, asserted rather than assumed.
#[tokio::test]
async fn a_past_time_date_parks_already_due() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition>
                <bpmn:timeDate>2020-01-01T00:00:00Z</bpmn:timeDate>
              </bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let result = executor
        .execute_stateful_from(&process, Variables::new(), dep(), BTreeMap::new(), None)
        .await
        .unwrap();
    let StatefulExecResult::Suspended { timer_waits, .. } = result else {
        panic!("expected Suspended");
    };
    assert_eq!(timer_waits[0].due_at, "2020-01-01T00:00:00Z");
    assert!(
        timer_waits[0].due_at.as_str() < "2026-01-01T00:00:00Z",
        "a past date is due BEFORE the pinned now — it fires on the first tick"
    );
}

/// An offset-bearing `<bpmn:timeDate>` on a timer BOUNDARY resolves to the instant it names —
/// the deadline is absolute, so the host's park moment does not shift it.
#[tokio::test]
async fn a_time_date_boundary_uses_the_absolute_deadline() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Call" implementation="channel:out">
              <bpmn:extensionElements>
                <q:alias name="k" expression="e2eId"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:boundaryEvent id="B" attachedToRef="Call">
              <bpmn:timerEventDefinition>
                <bpmn:timeDate>2026-03-01T15:00:00+05:30</bpmn:timeDate>
              </bpmn:timerEventDefinition>
            </bpmn:boundaryEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:endEvent id="TO"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="TO"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = executor(Rc::clone(&sink));

    let result = executor
        .execute_stateful_from(
            &process,
            vars(&[("e2eId", string("E2E-DATE"))]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let StatefulExecResult::Suspended { timer_waits, .. } = result else {
        panic!("expected Suspended");
    };
    assert_eq!(timer_waits.len(), 1);
    assert_eq!(timer_waits[0].node_id, "B");
    // 15:00+05:30 is 09:30Z — the same instant, rendered in UTC.
    assert_eq!(timer_waits[0].due_at, "2026-03-01T09:30:00Z");
}
