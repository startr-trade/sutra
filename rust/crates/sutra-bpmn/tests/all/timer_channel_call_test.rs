//! Loader conformance: timer catch / timer boundary / `<q:timeout>` parsing, and the
//! channel-call package-time validations.

use sutra_bpmn::model::{BoundaryKind, Node};
use sutra_bpmn::timer::TimerDefinition;
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

/// A well-formed channel-call task (q:timeout + alias) plus start/end plumbing.
fn call_process(call_extensions: &str, extra_nodes: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>{call_extensions}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
        {extra_nodes}"#
    ))
}

const ALIAS: &str = r#"<q:alias name="k" expression="event.idempotencyKey"/>"#;

#[test]
fn intermediate_timer_catch_parses_with_duration() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:intermediateCatchEvent id="Wait">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT0.5S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:intermediateCatchEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#,
    );
    let module = load(&bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let node = process.node("Wait").unwrap();
    assert!(
        matches!(node, Node::TimerCatchEvent { timer, .. }
            if *timer == TimerDefinition::Duration("PT0.5S".to_owned())),
        "got: {node:?}"
    );
    assert!(!process.is_sync_eligible(), "a timer catch is a wait state");
}

/// A `<bpmn:timeDate>` on an intermediate catch — the P1-5b addition. The absolute instant is
/// carried verbatim; due-at computation (and the past-date semantics) is unit-tested in
/// `sutra_bpmn::timer`.
#[test]
fn intermediate_timer_catch_parses_with_a_date() {
    for instant in [
        "2026-03-01T09:30:00Z",
        "2026-03-01T15:00:00+05:30",
        // A date in the PAST is deliberately legal: it is already due.
        "2020-01-01T00:00:00Z",
    ] {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition><bpmn:timeDate>{instant}</bpmn:timeDate></bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#
        ));
        let module = load(&bpmn).unwrap_or_else(|e| panic!("'{instant}' should load: {e}"));
        let node = module.process("p1").unwrap().node("Wait").unwrap();
        assert!(
            matches!(node, Node::TimerCatchEvent { timer, .. }
                if *timer == TimerDefinition::Date(instant.to_owned())),
            "'{instant}' got: {node:?}"
        );
    }
}

/// A timer BOUNDARY takes a date too — same extraction path, same contract.
#[test]
fn timer_boundary_parses_with_a_date() {
    let bpmn = defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>{ALIAS}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:boundaryEvent id="B" attachedToRef="Call">
          <bpmn:timerEventDefinition><bpmn:timeDate>2026-03-01T09:30:00Z</bpmn:timeDate></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="E"/>"#
    ));
    let module = load(&bpmn).unwrap();
    let node = module.process("p1").unwrap().node("B").unwrap();
    assert!(
        matches!(node, Node::BoundaryEvent { timer: Some(t), .. }
            if *t == TimerDefinition::Date("2026-03-01T09:30:00Z".to_owned())),
        "got: {node:?}"
    );
}

/// A garbage `<timeDate>` is a typo, and gets the typo code — never the deliberately-unsupported
/// one. Keeping the two apart is the whole point of the three-way diagnostic split.
#[test]
fn an_unparseable_time_date_is_date_invalid() {
    for bad in ["x", "", "2026-03-01", "2026-03-01T09:30:00", "tomorrow"] {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition><bpmn:timeDate>{bad}</bpmn:timeDate></bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#
        ));
        let e = load(&bpmn).unwrap_err();
        assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.DATE_INVALID", "'{bad}': {e}");
    }
}

/// `timeCycle` remains unsupported on a catch/boundary: a token parks there ONCE, so a repeating
/// trigger has nowhere to go. The diagnostic now says exactly that, and points at start events.
#[test]
fn time_cycle_is_still_rejected_on_a_catch_event() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:intermediateCatchEvent id="Wait">
          <bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle></bpmn:timerEventDefinition>
        </bpmn:intermediateCatchEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#,
    );
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
    assert!(
        e.message.contains("START event"),
        "the diagnostic must point at where a cycle IS supported: {e}"
    );
}

/// One `<timerEventDefinition>` carries exactly one time specification.
#[test]
fn two_time_specifications_on_one_definition_are_rejected() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:intermediateCatchEvent id="Wait">
          <bpmn:timerEventDefinition>
            <bpmn:timeDuration>PT1H</bpmn:timeDuration>
            <bpmn:timeDate>2026-03-01T09:30:00Z</bpmn:timeDate>
          </bpmn:timerEventDefinition>
        </bpmn:intermediateCatchEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#,
    );
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
    assert!(e.message.contains("timeDuration + timeDate"), "{e}");
}

// ---- timer START events (P1-5b) --------------------------------------------------------------

/// The headline of P1-5b: all three schedulable forms now LOAD on a start event, where P1-5a
/// rejected every one of them.
#[test]
fn timer_start_events_load_in_all_three_schedulable_forms() {
    let cases = [
        (
            "PT1H",
            "timeDuration",
            TimerDefinition::Duration("PT1H".to_owned()),
        ),
        (
            "2026-03-01T09:30:00Z",
            "timeDate",
            TimerDefinition::Date("2026-03-01T09:30:00Z".to_owned()),
        ),
    ];
    for (spec, element, expected) in cases {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S">
              <bpmn:timerEventDefinition><bpmn:{element}>{spec}</bpmn:{element}></bpmn:timerEventDefinition>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#
        ));
        let module = load(&bpmn).unwrap_or_else(|e| panic!("'{spec}' should load: {e}"));
        let process = module.process("p1").unwrap();
        assert!(
            matches!(process.node("S").unwrap(), Node::StartEvent { timer: Some(t), .. }
                if *t == expected),
            "'{spec}' got: {:?}",
            process.node("S").unwrap()
        );
        // And it is discoverable as a schedule the activation flip must arm.
        let starts = process.timer_start_events();
        assert_eq!(starts.len(), 1, "'{spec}'");
        assert_eq!(starts[0].0, "S");
    }
}

/// Every ISO-8601 repeating-interval spelling a `timeCycle` start may use.
#[test]
fn timer_start_events_accept_every_iso_cycle_form() {
    for cycle in [
        "R/PT1H",
        "R5/PT30S",
        "R/2026-03-01T00:00:00Z/PT1H",
        "R3/2026-03-01T00:00:00Z/P1D",
    ] {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S">
              <bpmn:timerEventDefinition><bpmn:timeCycle>{cycle}</bpmn:timeCycle></bpmn:timerEventDefinition>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#
        ));
        let module = load(&bpmn).unwrap_or_else(|e| panic!("'{cycle}' should load: {e}"));
        let node = module.process("p1").unwrap().node("S").unwrap();
        assert!(
            matches!(
                node,
                Node::StartEvent {
                    timer: Some(TimerDefinition::Cycle(_)),
                    ..
                }
            ),
            "'{cycle}' got: {node:?}"
        );
    }
}

/// P1-5a's code, NARROWED: it now fires only for the forms that remain deliberately out of
/// contract — cron syntax and calendar-length durations — and its message names them.
#[test]
fn only_cron_and_calendar_start_timers_keep_the_unsupported_code() {
    let cases = [
        ("timeCycle", "0 0 * * *"),
        ("timeCycle", "*/5 * * * *"),
        ("timeCycle", "R/P1M"),
        ("timeDuration", "P1Y"),
        ("timeDuration", "P1M"),
    ];
    for (element, spec) in cases {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S">
              <bpmn:timerEventDefinition><bpmn:{element}>{spec}</bpmn:{element}></bpmn:timerEventDefinition>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#
        ));
        let e = load(&bpmn).unwrap_err();
        assert_eq!(
            e.code, "SUTRA.CONFIG.BPMN.TIMER_START_UNSUPPORTED",
            "'{spec}': {e}"
        );
        assert!(
            e.message.contains("cron") || e.message.contains("calendar"),
            "the narrowed diagnostic must name what is unsupported: {e}"
        );
        assert!(e.message.contains('S'), "and point at the start event: {e}");
    }
}

/// A cycle that is ISO-shaped but written wrong is a TYPO, and reports as one — the narrowed
/// unsupported code must not absorb it.
#[test]
fn a_malformed_start_cycle_is_cycle_invalid_not_unsupported() {
    for bad in ["R", "Rx/PT1H", "R0/PT1H", "R/", "R/PT0S"] {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S">
              <bpmn:timerEventDefinition><bpmn:timeCycle>{bad}</bpmn:timeCycle></bpmn:timerEventDefinition>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#
        ));
        let e = load(&bpmn).unwrap_err();
        assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.CYCLE_INVALID", "'{bad}': {e}");
    }
}

/// A start event has ONE trigger contract. Declaring a channel source AND a timer is a load
/// error naming both.
#[test]
fn a_start_event_cannot_be_both_channel_and_timer_triggered() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S">
          <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#,
    );
    let e = load(&bpmn).unwrap_err();
    assert_eq!(
        e.code, "SUTRA.CONFIG.BPMN.TIMER_START_SOURCE_CONFLICT",
        "{e}"
    );
    assert!(e.message.contains("in"), "names the channel: {e}");
    assert!(e.message.contains('S'), "names the start event: {e}");
}

/// A timer start needs no `<q:source>` — that is the point. (A plain manual start stays legal
/// too; both are start events with no channel.)
#[test]
fn a_source_less_start_event_is_legal_with_and_without_a_timer() {
    let plain = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#,
    );
    let module = load(&plain).unwrap();
    let node = module.process("p1").unwrap().node("S").unwrap();
    assert!(
        matches!(node, Node::StartEvent { timer: None, channels, .. } if channels.is_empty()),
        "got: {node:?}"
    );

    let timed = defs(
        r#"
        <bpmn:startEvent id="S">
          <bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle></bpmn:timerEventDefinition>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#,
    );
    let module = load(&timed).unwrap();
    let process = module.process("p1").unwrap();
    assert!(
        matches!(process.node("S").unwrap(),
            Node::StartEvent { timer: Some(_), channels, .. } if channels.is_empty()),
        "a timer start carries no channels"
    );
    assert_eq!(process.timer_start_events().len(), 1);
}

/// A start event with no timer contributes no schedule — the activation flip must not arm rows
/// for ordinary processes.
#[test]
fn a_process_without_timer_starts_declares_no_schedules() {
    let module = load(&defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#,
    ))
    .unwrap();
    assert!(module
        .process("p1")
        .unwrap()
        .timer_start_events()
        .is_empty());
}

#[test]
fn unparseable_or_calendar_durations_are_rejected() {
    for bad in ["30 seconds", "P1Y", "P1M", ""] {
        let bpmn = defs(&format!(
            r#"
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="Wait">
              <bpmn:timerEventDefinition><bpmn:timeDuration>{bad}</bpmn:timeDuration></bpmn:timerEventDefinition>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Wait"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>"#
        ));
        let e = load(&bpmn).unwrap_err();
        assert_eq!(
            e.code, "SUTRA.DISPATCH.TIMER.DURATION_INVALID",
            "'{bad}': {e}"
        );
    }
}

#[test]
fn channel_call_without_timer_or_timeout_is_a_load_time_error() {
    // REQUIRED on every channel-call task; package-time error if absent.
    let e = load(&call_process(ALIAS, "")).unwrap_err();
    assert_eq!(
        e.code, "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT_REQUIRED",
        "{e}"
    );
}

#[test]
fn channel_call_without_declared_alias_is_a_load_time_error() {
    // The park key is a DECLARED <q:alias>.
    let e = load(&call_process(r#"<q:timeout duration="PT30S"/>"#, "")).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.CHANNEL_CALL.ALIAS_REQUIRED", "{e}");
}

#[test]
fn q_timeout_synthesizes_an_interrupting_timer_boundary() {
    let module = load(&call_process(
        &format!(r#"<q:timeout duration="PT30S"/>{ALIAS}"#),
        "",
    ))
    .unwrap();
    let process = module.process("p1").unwrap();
    let boundary = process.node("Call#timeout").unwrap();
    match boundary {
        Node::BoundaryEvent {
            kind,
            attached_to_ref,
            interrupting,
            timer,
            ..
        } => {
            assert_eq!(*kind, BoundaryKind::Timer);
            assert_eq!(attached_to_ref, "Call");
            assert!(*interrupting);
            assert_eq!(
                timer.as_ref(),
                Some(&TimerDefinition::Duration("PT30S".to_owned()))
            );
        }
        other => panic!("expected the synthesized timer boundary, got {other:?}"),
    }
    // The synthetic boundary has no outgoing route — its fire raises the timeout error.
    assert!(process.outgoing("Call#timeout").is_empty());
}

#[test]
fn bpmn_timer_boundary_on_channel_call_satisfies_the_requirement() {
    let bpmn = defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>{ALIAS}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:boundaryEvent id="B" attachedToRef="Call">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:endEvent id="ETimeout"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="ETimeout"/>"#
    ));
    let module = load(&bpmn).unwrap();
    let process = module.process("p1").unwrap();
    assert!(matches!(
        process.node("B").unwrap(),
        Node::BoundaryEvent {
            kind: BoundaryKind::Timer,
            ..
        }
    ));
}

#[test]
fn non_interrupting_timer_boundary_is_rejected() {
    let bpmn = defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements>{ALIAS}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:boundaryEvent id="B" attachedToRef="Call" cancelActivity="false">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>"#
    ));
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
}

#[test]
fn timer_boundary_on_a_synchronous_task_is_rejected() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="${stamp}"/>
        <bpmn:boundaryEvent id="B" attachedToRef="T">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#,
    );
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
}

#[test]
fn q_timeout_on_a_non_channel_call_node_is_rejected() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="${stamp}">
          <bpmn:extensionElements><q:timeout duration="PT1S"/></bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#,
    );
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
}

#[test]
fn timer_boundary_on_user_task_is_supported() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:userTask id="U"/>
        <bpmn:boundaryEvent id="B" attachedToRef="U">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:endEvent id="ETimeout"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
        <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="ETimeout"/>"#,
    );
    assert!(load(&bpmn).is_ok());
}

#[test]
fn channel_call_inside_a_sub_process_is_rejected() {
    let bpmn = defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:subProcess id="SP">
          <bpmn:startEvent id="S2"/>
          <bpmn:serviceTask id="Call" implementation="channel:out">
            <bpmn:extensionElements><q:timeout duration="PT1S"/>{ALIAS}</bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="E2"/>
          <bpmn:sequenceFlow id="g1" sourceRef="S2" targetRef="Call"/>
          <bpmn:sequenceFlow id="g2" sourceRef="Call" targetRef="E2"/>
        </bpmn:subProcess>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="SP"/>
        <bpmn:sequenceFlow id="f2" sourceRef="SP" targetRef="E"/>"#
    ));
    let e = load(&bpmn).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
}

#[test]
fn q_output_binding_parses_and_requires_variable() {
    let bpmn = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="render.hbs">
          <bpmn:extensionElements><q:output variable="renderedRequest"/></bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#,
    );
    let module = load(&bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let output = process.bindings_for("T").output.as_ref().unwrap();
    assert_eq!(output.variable, "renderedRequest");

    let missing = defs(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="render.hbs">
          <bpmn:extensionElements><q:output/></bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#,
    );
    assert!(load(&missing).is_err());
}
