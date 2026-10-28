//! Expression-valued timers at load: the "literal first, else FEEL" rule for every timer text,
//! the start-timer scope rule, and the FEEL-result conversions the executor and the deployment
//! planner share.

use sutra_bpmn::model::Node;
use sutra_bpmn::timer::{
    classify_timer_text, duration_from_feel_value, instant_from_feel_value,
    resolve_timer_expression_value, TimerDefinition, TimerExpressionForm, TimerTextClass,
};
use sutra_bpmn::{BpmnModelLoader, ProcessModule};
use sutra_feel::{FeelDuration, FeelValue};

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

fn catch_with(child: &str, text: &str) -> String {
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

fn start_with(child: &str, text: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S">
          <bpmn:timerEventDefinition><bpmn:{child}>{text}</bpmn:{child}></bpmn:timerEventDefinition>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#
    ))
}

fn catch_timer(bpmn: &str) -> TimerDefinition {
    let module = load(bpmn).unwrap();
    match module.process("p1").unwrap().node("Wait").unwrap() {
        Node::TimerCatchEvent { timer, .. } => timer.clone(),
        other => panic!("not a timer catch: {other:?}"),
    }
}

// ---- the classification rule ----------------------------------------------------------------

#[test]
fn every_valid_literal_stays_a_literal() {
    for d in ["PT5S", "PT0.5S", "P1DT2H", "P2W", "PT0S"] {
        assert_eq!(
            classify_timer_text(TimerExpressionForm::Duration, d),
            Ok(TimerTextClass::Literal),
            "{d}"
        );
        assert_eq!(
            catch_timer(&catch_with("timeDuration", d)),
            TimerDefinition::Duration(d.to_owned())
        );
    }
    for at in ["2026-03-01T09:30:00Z", "2026-03-01T15:00:00+05:30"] {
        assert_eq!(
            catch_timer(&catch_with("timeDate", at)),
            TimerDefinition::Date(at.to_owned())
        );
    }
    for c in ["R/PT1H", "R5/PT1H", "R/2026-03-01T00:00:00Z/PT1H"] {
        assert_eq!(
            classify_timer_text(TimerExpressionForm::Cycle, c),
            Ok(TimerTextClass::Literal),
            "{c}"
        );
    }
}

#[test]
fn a_literal_shaped_typo_keeps_its_load_error() {
    for (child, bad, code) in [
        (
            "timeDuration",
            "PT5X",
            "SUTRA.DISPATCH.TIMER.DURATION_INVALID",
        ),
        (
            "timeDuration",
            "P1M",
            "SUTRA.DISPATCH.TIMER.DURATION_INVALID",
        ),
        (
            "timeDate",
            "2026-03-01",
            "SUTRA.DISPATCH.TIMER.DATE_INVALID",
        ),
        (
            "timeDate",
            "2026-03-01T09:30:00",
            "SUTRA.DISPATCH.TIMER.DATE_INVALID",
        ),
    ] {
        let e = load(&catch_with(child, bad)).unwrap_err();
        assert_eq!(e.code, code, "'{bad}': {e}");
    }
    for bad in ["R", "R0/PT1H", "Rx/PT1H", "R/PT0S"] {
        let e = load(&start_with("timeCycle", bad)).unwrap_err();
        assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.CYCLE_INVALID", "'{bad}': {e}");
    }
}

#[test]
fn text_that_is_neither_literal_nor_feel_keeps_the_literal_code_and_names_both() {
    let e = load(&catch_with("timeDuration", "wait ( for")).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.DURATION_INVALID", "{e}");
    assert!(e.message.contains("FEEL"), "{e}");
    // A cron string on a start event keeps its out-of-contract code.
    let e = load(&start_with("timeCycle", "0 0 * * *")).unwrap_err();
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.TIMER_START_UNSUPPORTED", "{e}");
}

#[test]
fn valid_feel_loads_as_an_expression_of_the_elements_form() {
    assert_eq!(
        catch_timer(&catch_with("timeDuration", "payload.wait")),
        TimerDefinition::Expression {
            form: TimerExpressionForm::Duration,
            expression: "payload.wait".to_owned(),
        }
    );
    assert_eq!(
        catch_timer(&catch_with(
            "timeDuration",
            r#"duration("PT" + string(n) + "S")"#
        )),
        TimerDefinition::Expression {
            form: TimerExpressionForm::Duration,
            expression: r#"duration("PT" + string(n) + "S")"#.to_owned(),
        }
    );
    // Spaced builtins parse: the name table is applied.
    assert_eq!(
        catch_timer(&catch_with("timeDate", r#"date and time(order.deadline)"#)),
        TimerDefinition::Expression {
            form: TimerExpressionForm::Date,
            expression: "date and time(order.deadline)".to_owned(),
        }
    );
    // A text that used to be a date typo, and is now a variable name.
    assert!(catch_timer(&catch_with("timeDate", "tomorrow")).is_expression());
}

// ---- boundaries, q:timeout, q:retry ----------------------------------------------------------

fn receive_with_boundary(cancel: &str, child: &str, text: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements><q:alias name="k" expression="event.key"/></bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:boundaryEvent id="B" attachedToRef="Call" cancelActivity="{cancel}">
          <bpmn:timerEventDefinition><bpmn:{child}>{text}</bpmn:{child}></bpmn:timerEventDefinition>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>"#
    ))
}

#[test]
fn a_boundary_takes_a_duration_or_date_expression() {
    for (child, form) in [
        ("timeDuration", TimerExpressionForm::Duration),
        ("timeDate", TimerExpressionForm::Date),
    ] {
        let module = load(&receive_with_boundary("true", child, "payload.window")).unwrap();
        match module.process("p1").unwrap().node("B").unwrap() {
            Node::BoundaryEvent { timer: Some(t), .. } => assert_eq!(
                *t,
                TimerDefinition::Expression {
                    form,
                    expression: "payload.window".to_owned()
                }
            ),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn a_recurring_boundary_refuses_an_expression_cycle() {
    let e = load(&receive_with_boundary(
        "false",
        "timeCycle",
        "config.interval",
    ))
    .unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.UNSUPPORTED", "{e}");
    // The literal recurring boundary still loads.
    assert!(load(&receive_with_boundary("false", "timeCycle", "R/PT1M")).is_ok());
}

fn call_with(extensions: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Call" implementation="channel:out">
          <bpmn:extensionElements><q:alias name="k" expression="event.key"/>{extensions}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="E"/>"#
    ))
}

#[test]
fn a_q_timeout_expression_reaches_its_synthesized_boundary() {
    let module = load(&call_with(r#"<q:timeout duration="request.budget"/>"#)).unwrap();
    let process = module.process("p1").unwrap();
    let binding = process.bindings_for("Call").timeout.clone().unwrap();
    assert!(binding.duration_expression);
    match process.node("Call#timeout").unwrap() {
        Node::BoundaryEvent { timer: Some(t), .. } => assert_eq!(
            *t,
            TimerDefinition::Expression {
                form: TimerExpressionForm::Duration,
                expression: "request.budget".to_owned()
            }
        ),
        other => panic!("{other:?}"),
    }
    // The literal form is untouched.
    let module = load(&call_with(r#"<q:timeout duration="PT30S"/>"#)).unwrap();
    let process = module.process("p1").unwrap();
    assert!(
        !process
            .bindings_for("Call")
            .timeout
            .clone()
            .unwrap()
            .duration_expression
    );
    assert!(matches!(
        process.node("Call#timeout").unwrap(),
        Node::BoundaryEvent { timer: Some(TimerDefinition::Duration(d)), .. } if d == "PT30S"
    ));
    // A literal-shaped typo is still a load error.
    let e = load(&call_with(r#"<q:timeout duration="PT3OS"/>"#)).unwrap_err();
    assert_eq!(e.code, "SUTRA.DISPATCH.TIMER.DURATION_INVALID", "{e}");
}

fn retry_task(retry: &str) -> String {
    defs(&format!(
        r#"
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="doWork">
          <bpmn:extensionElements>{retry}</bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>"#
    ))
}

#[test]
fn retry_delays_take_expressions_and_keep_the_literal_checks() {
    let module = load(&retry_task(
        r#"<q:retry maxAttempts="3" initialDelay="config.retryDelay" maxDelay="PT1M"/>"#,
    ))
    .unwrap();
    let process = module.process("p1").unwrap();
    let policy = process.retry_policy("T").unwrap();
    assert!(policy.initial_delay_expression);
    assert!(!policy.max_delay_expression);
    let sites = process.node_timer_expressions(process.node("T").unwrap());
    assert_eq!(sites.len(), 1);
    assert_eq!(sites[0].site, "q:retry@initialDelay");
    assert_eq!(sites[0].expression, "config.retryDelay");

    // Both literal: the ordering check still runs at load.
    let e = load(&retry_task(
        r#"<q:retry maxAttempts="3" initialDelay="PT1M" maxDelay="PT10S"/>"#,
    ))
    .unwrap_err();
    assert_eq!(e.code, "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID", "{e}");
    // Defaults stay literal.
    let module = load(&retry_task(r#"<q:retry maxAttempts="2"/>"#)).unwrap();
    let policy = module
        .process("p1")
        .unwrap()
        .retry_policy("T")
        .unwrap()
        .clone();
    assert!(!policy.initial_delay_expression && !policy.max_delay_expression);
    assert_eq!(policy.initial_delay, "PT1S");
}

// ---- timer start events: config only ---------------------------------------------------------

#[test]
fn a_start_timer_expression_reads_config_only() {
    let module = load(&start_with("timeDuration", "config.startAfter")).unwrap();
    let process = module.process("p1").unwrap();
    let starts = process.timer_start_events();
    assert_eq!(
        *starts[0].1,
        TimerDefinition::Expression {
            form: TimerExpressionForm::Duration,
            expression: "config.startAfter".to_owned()
        }
    );
    let sites = process.node_timer_expressions(process.node("S").unwrap());
    assert!(sites[0].on_start_event);
    assert!(load(&start_with("timeCycle", r#"config.cycle"#)).is_ok());
    assert!(load(&start_with("timeDate", r#"date and time(config.runAt)"#)).is_ok());

    for bad in ["payload.wait", "config.base + extra", "x"] {
        let e = load(&start_with("timeDuration", bad)).unwrap_err();
        assert_eq!(
            e.code, "SUTRA.CONFIG.BPMN.TIMER_START_EXPRESSION_SCOPE",
            "'{bad}': {e}"
        );
    }
}

// ---- the FEEL-result conversions -------------------------------------------------------------

fn days_time(secs: i64) -> FeelValue {
    FeelValue::Duration(FeelDuration::DaysTime(time::Duration::seconds(secs)))
}

#[test]
fn duration_results() {
    assert_eq!(
        duration_from_feel_value(&FeelValue::String("PT90S".into())).unwrap(),
        std::time::Duration::from_secs(90)
    );
    assert_eq!(
        duration_from_feel_value(&days_time(30)).unwrap(),
        std::time::Duration::from_secs(30)
    );
    for bad in [
        FeelValue::Null,
        FeelValue::String("soon".into()),
        FeelValue::String("P1M".into()),
        FeelValue::Number(5.into()),
        FeelValue::Boolean(true),
        FeelValue::Duration(FeelDuration::YearsMonths(1)),
        days_time(-5),
    ] {
        assert!(duration_from_feel_value(&bad).is_err(), "{bad:?}");
    }
    assert_eq!(
        resolve_timer_expression_value(TimerExpressionForm::Duration, &days_time(4)).unwrap(),
        TimerDefinition::Duration("PT4S".into())
    );
}

#[test]
fn instant_and_cycle_results() {
    let at =
        instant_from_feel_value(&FeelValue::String("2026-03-01T15:00:00+05:30".into())).unwrap();
    assert_eq!(
        resolve_timer_expression_value(
            TimerExpressionForm::Date,
            &FeelValue::String("2026-03-01T15:00:00+05:30".into())
        )
        .unwrap(),
        TimerDefinition::Date("2026-03-01T09:30:00Z".into())
    );
    assert_eq!(at.unix_timestamp(), 1_772_357_400);
    let zoned = sutra_feel::expressions::eval(
        r#"date and time("2026-03-01T09:30:00Z")"#,
        &Default::default(),
    )
    .unwrap();
    assert!(instant_from_feel_value(&zoned).is_ok());
    let local = sutra_feel::expressions::eval(
        r#"date and time("2026-03-01T09:30:00")"#,
        &Default::default(),
    )
    .unwrap();
    assert!(
        instant_from_feel_value(&local).is_err(),
        "zone-less is refused"
    );
    assert!(instant_from_feel_value(&FeelValue::Null).is_err());
    assert!(instant_from_feel_value(&days_time(3)).is_err());

    assert!(matches!(
        resolve_timer_expression_value(
            TimerExpressionForm::Cycle,
            &FeelValue::String("R3/PT1H".into())
        ),
        Ok(TimerDefinition::Cycle(_))
    ));
    assert!(resolve_timer_expression_value(
        TimerExpressionForm::Cycle,
        &FeelValue::String("0 0 * * *".into())
    )
    .is_err());
    assert!(resolve_timer_expression_value(TimerExpressionForm::Cycle, &FeelValue::Null).is_err());
}
