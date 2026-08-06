//! Error handling and scoping: error boundary events and compensation, embedded / nested /
//! transaction sub-processes, event sub-processes, ad-hoc sub-processes, and throw events.

use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::model::{BoundaryKind, ThrowKind};
use sutra_bpmn::Node;
use sutra_executor::executor::feel_condition_evaluator;
use sutra_executor::{CollectingSink, EmissionSink, TaskError, TaskRegistry, TokenExecutor};

// ---- ErrorBoundaryEventTest --------------------------------------------------------

#[tokio::test]
async fn boundary_event_catches_matching_error_code() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:error id="errBoom" name="Boom" errorCode="E_BOOM"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${raiser}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="T">
              <bpmn:errorEventDefinition errorRef="errBoom"/>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="Recover" implementation="${recover}"/>
            <bpmn:endEvent id="ENormal"/>
            <bpmn:endEvent id="EErr"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="ENormal"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="Recover"/>
            <bpmn:sequenceFlow id="f4" sourceRef="Recover" targetRef="EErr"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new()
        .register("raiser", |_, _| Err(TaskError::BpmnError("E_BOOM".into())))
        .register("recover", |_, _| ok_map(&[("recovered", boolean(true))]));
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(result.output("recovered"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("Recover"));
    assert!(result.visited_nodes.contains("EErr"));
    assert!(!result.visited_nodes.contains("ENormal"));
}

#[tokio::test]
async fn boundary_event_without_error_ref_catches_any_error() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${raiser}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="T">
              <bpmn:errorEventDefinition/>
            </bpmn:boundaryEvent>
            <bpmn:endEvent id="ENormal"/>
            <bpmn:endEvent id="EErr"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="ENormal"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="EErr"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("raiser", |_, _| {
        Err(TaskError::BpmnError("ANY_CODE".into()))
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert!(result.visited_nodes.contains("EErr"));
    assert!(!result.visited_nodes.contains("ENormal"));
}

#[tokio::test]
async fn boundary_event_does_not_catch_non_matching_error_code() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:error id="errA" name="A" errorCode="E_A"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${raiser}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="T">
              <bpmn:errorEventDefinition errorRef="errA"/>
            </bpmn:boundaryEvent>
            <bpmn:endEvent id="ENormal"/>
            <bpmn:endEvent id="EErr"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="ENormal"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="EErr"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry =
        TaskRegistry::new().register("raiser", |_, _| Err(TaskError::BpmnError("E_B".into())));
    let e = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert!(e.message().contains("E_B"), "{e}");
    assert!(
        e.message()
            .contains("no boundary event or event sub-process in scope caught"),
        "{e}"
    );
}

#[tokio::test]
async fn error_end_event_uncaught_when_no_host_boundary_exists() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="ErrEnd">
              <bpmn:errorEventDefinition/>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="ErrEnd"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let e = TokenExecutor::builder(TaskRegistry::new())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert!(
        e.message()
            .contains("no boundary event or event sub-process in scope caught"),
        "{e}"
    );
}

#[tokio::test]
async fn compensation_handler_runs_lifo_for_completed_activities() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="A" implementation="${a}"/>
            <bpmn:boundaryEvent id="BA" attachedToRef="A">
              <bpmn:compensateEventDefinition/>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="UndoA" implementation="${undoA}"/>
            <bpmn:serviceTask id="B" implementation="${b}"/>
            <bpmn:boundaryEvent id="BB" attachedToRef="B">
              <bpmn:compensateEventDefinition/>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="UndoB" implementation="${undoB}"/>
            <bpmn:intermediateThrowEvent id="Throw">
              <bpmn:compensateEventDefinition/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
            <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="B"/>
            <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="Throw"/>
            <bpmn:sequenceFlow id="f4" sourceRef="Throw" targetRef="E"/>
            <bpmn:sequenceFlow id="f5" sourceRef="BA" targetRef="UndoA"/>
            <bpmn:sequenceFlow id="f6" sourceRef="BB" targetRef="UndoB"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let (oa, ob) = (Rc::clone(&order), Rc::clone(&order));
    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("aDone", boolean(true))]))
        .register("b", |_, _| ok_map(&[("bDone", boolean(true))]))
        .register("undoA", move |_, _| {
            oa.borrow_mut().push("undoA");
            ok_map(&[])
        })
        .register("undoB", move |_, _| {
            ob.borrow_mut().push("undoB");
            ok_map(&[])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    // LIFO: B compensated before A.
    assert_eq!(order.borrow().as_slice(), &["undoB", "undoA"]);
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn compensation_without_matching_handler_is_noop() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="A" implementation="${a}"/>
            <bpmn:intermediateThrowEvent id="Throw">
              <bpmn:compensateEventDefinition/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="A"/>
            <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="Throw"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Throw" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("a", |_, _| ok_map(&[("aDone", boolean(true))]));
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(result.output("aDone"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("E"));
}

#[test]
fn parser_rejects_boundary_event_with_unknown_attached_to_ref() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${noop}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="nosuch">
              <bpmn:errorEventDefinition/>
            </bpmn:boundaryEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert!(e.message.contains("no matching activity"), "{e}");
}

// ---- SubProcessTest -----------------------------------------------------------------

#[tokio::test]
async fn embedded_sub_process_expands_inline_and_shares_variable_scope() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Sub" name="review">
              <bpmn:startEvent id="SubStart"/>
              <bpmn:serviceTask id="Inner" implementation="${inner}"/>
              <bpmn:endEvent id="SubEnd"/>
              <bpmn:sequenceFlow id="s1" sourceRef="SubStart" targetRef="Inner"/>
              <bpmn:sequenceFlow id="s2" sourceRef="Inner" targetRef="SubEnd"/>
            </bpmn:subProcess>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sub"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="After"/>
            <bpmn:sequenceFlow id="f3" sourceRef="After" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(matches!(
        process.node("Sub").unwrap(),
        Node::SubProcess { .. }
    ));

    let registry = TaskRegistry::new()
        .register("inner", |_, _| ok_map(&[("fromInner", string("set"))]))
        .register("after", |_, ctx| {
            let saw = ctx
                .variable("fromInner")
                .map(sutra_feel::value::canonical_string_of)
                .unwrap_or_else(|| "MISSING".to_string());
            ok_map(&[("sawInner", string(&saw))])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(result.output("sawInner"), Some(&string("set")));
    for node in ["Sub", "Inner", "After", "E"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
}

#[tokio::test]
async fn nested_sub_processes_expand_to_any_depth() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Outer">
              <bpmn:startEvent id="OStart"/>
              <bpmn:subProcess id="InnerSub">
                <bpmn:startEvent id="IStart"/>
                <bpmn:serviceTask id="Deep" implementation="${deep}"/>
                <bpmn:endEvent id="IEnd"/>
                <bpmn:sequenceFlow id="i1" sourceRef="IStart" targetRef="Deep"/>
                <bpmn:sequenceFlow id="i2" sourceRef="Deep" targetRef="IEnd"/>
              </bpmn:subProcess>
              <bpmn:endEvent id="OEnd"/>
              <bpmn:sequenceFlow id="o1" sourceRef="OStart" targetRef="InnerSub"/>
              <bpmn:sequenceFlow id="o2" sourceRef="InnerSub" targetRef="OEnd"/>
            </bpmn:subProcess>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Outer"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Outer" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let deep_runs = Rc::new(RefCell::new(0));
    let dr = Rc::clone(&deep_runs);
    let registry = TaskRegistry::new().register("deep", move |_, _| {
        *dr.borrow_mut() += 1;
        ok_map(&[])
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*deep_runs.borrow(), 1);
    for node in ["Outer", "InnerSub", "Deep", "E"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
}

#[tokio::test]
async fn error_inside_sub_process_propagates_to_boundary_on_sub_process() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:error id="err" errorCode="BOOM"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Sub">
              <bpmn:startEvent id="SubStart"/>
              <bpmn:serviceTask id="Fail" implementation="${fail}"/>
              <bpmn:endEvent id="SubEnd"/>
              <bpmn:sequenceFlow id="s1" sourceRef="SubStart" targetRef="Fail"/>
              <bpmn:sequenceFlow id="s2" sourceRef="Fail" targetRef="SubEnd"/>
            </bpmn:subProcess>
            <bpmn:boundaryEvent id="B" attachedToRef="Sub">
              <bpmn:errorEventDefinition errorRef="err"/>
            </bpmn:boundaryEvent>
            <bpmn:serviceTask id="Handler" implementation="${handler}"/>
            <bpmn:endEvent id="HandlerEnd"/>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="MainEnd"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sub"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="After"/>
            <bpmn:sequenceFlow id="f3" sourceRef="After" targetRef="MainEnd"/>
            <bpmn:sequenceFlow id="fB" sourceRef="B" targetRef="Handler"/>
            <bpmn:sequenceFlow id="fH" sourceRef="Handler" targetRef="HandlerEnd"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let after_runs = Rc::new(RefCell::new(0));
    let handler_runs = Rc::new(RefCell::new(0));
    let (ar, hr) = (Rc::clone(&after_runs), Rc::clone(&handler_runs));
    let registry = TaskRegistry::new()
        .register("fail", |_, _| Err(TaskError::BpmnError("BOOM".into())))
        .register("handler", move |_, _| {
            *hr.borrow_mut() += 1;
            ok_map(&[])
        })
        .register("after", move |_, _| {
            *ar.borrow_mut() += 1;
            ok_map(&[])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*handler_runs.borrow(), 1);
    assert_eq!(*after_runs.borrow(), 0);
    assert!(result.visited_nodes.contains("Handler"));
    assert!(result.visited_nodes.contains("HandlerEnd"));
    assert!(!result.visited_nodes.contains("After"));
    assert!(!result.visited_nodes.contains("MainEnd"));
}

#[tokio::test]
async fn error_inside_sub_process_caught_by_inner_boundary_stays_in_scope() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:error id="err" errorCode="BOOM"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Sub">
              <bpmn:startEvent id="SubStart"/>
              <bpmn:serviceTask id="Fail" implementation="${fail}"/>
              <bpmn:boundaryEvent id="InnerB" attachedToRef="Fail">
                <bpmn:errorEventDefinition errorRef="err"/>
              </bpmn:boundaryEvent>
              <bpmn:serviceTask id="InnerHandler" implementation="${innerHandler}"/>
              <bpmn:endEvent id="SubEnd"/>
              <bpmn:endEvent id="SubHandlerEnd"/>
              <bpmn:sequenceFlow id="s1" sourceRef="SubStart" targetRef="Fail"/>
              <bpmn:sequenceFlow id="s2" sourceRef="Fail" targetRef="SubEnd"/>
              <bpmn:sequenceFlow id="s3" sourceRef="InnerB" targetRef="InnerHandler"/>
              <bpmn:sequenceFlow id="s4" sourceRef="InnerHandler" targetRef="SubHandlerEnd"/>
            </bpmn:subProcess>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="MainEnd"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sub"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="After"/>
            <bpmn:sequenceFlow id="f3" sourceRef="After" targetRef="MainEnd"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let after_runs = Rc::new(RefCell::new(0));
    let inner_handler_runs = Rc::new(RefCell::new(0));
    let (ar, ihr) = (Rc::clone(&after_runs), Rc::clone(&inner_handler_runs));
    let registry = TaskRegistry::new()
        .register("fail", |_, _| Err(TaskError::BpmnError("BOOM".into())))
        .register("innerHandler", move |_, _| {
            *ihr.borrow_mut() += 1;
            ok_map(&[])
        })
        .register("after", move |_, _| {
            *ar.borrow_mut() += 1;
            ok_map(&[])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*inner_handler_runs.borrow(), 1);
    assert_eq!(*after_runs.borrow(), 1);
    for node in ["InnerHandler", "After", "MainEnd"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
}

#[test]
fn sub_process_with_a_wait_state_is_not_sync_eligible() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Sub">
              <bpmn:startEvent id="SubStart"/>
              <bpmn:userTask id="Wait"/>
              <bpmn:endEvent id="SubEnd"/>
              <bpmn:sequenceFlow id="s1" sourceRef="SubStart" targetRef="Wait"/>
              <bpmn:sequenceFlow id="s2" sourceRef="Wait" targetRef="SubEnd"/>
            </bpmn:subProcess>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sub"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(!process.is_sync_eligible());
}

#[tokio::test]
async fn transaction_sub_process_parses_and_runs() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:transaction id="Tx">
              <bpmn:startEvent id="TxStart"/>
              <bpmn:endEvent id="TxEnd"/>
              <bpmn:sequenceFlow id="t1" sourceRef="TxStart" targetRef="TxEnd"/>
            </bpmn:transaction>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Tx"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Tx" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("Tx").unwrap() {
        Node::TransactionSubProcess { inner, .. } => {
            assert_eq!(inner.start_event().unwrap().id(), "TxStart");
        }
        other => panic!("expected TransactionSubProcess, got {other:?}"),
    }

    let result = TokenExecutor::builder(TaskRegistry::new())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert!(result.visited_nodes.contains("E"));
}

#[test]
fn event_sub_process_fails_closed_at_load() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:subProcess id="Ev" triggeredByEvent="true">
              <bpmn:startEvent id="EvStart"/>
              <bpmn:endEvent id="EvEnd"/>
              <bpmn:sequenceFlow id="e1" sourceRef="EvStart" targetRef="EvEnd"/>
            </bpmn:subProcess>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_SUBPROCESS_UNSUPPORTED);
}

// ---- EventSubProcessTest --------------------------------------------------------------

fn esp_bpmn(error_def: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:error id="errBoom" errorCode="E_BOOM"/>
          <bpmn:process id="p">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Risky" implementation="${{risky}}"/>
            <bpmn:endEvent id="Done"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Risky"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Risky" targetRef="Done"/>
            <bpmn:subProcess id="Handler" triggeredByEvent="true">
              <bpmn:startEvent id="HStart">{error_def}</bpmn:startEvent>
              <bpmn:serviceTask id="Recover" implementation="${{recover}}"/>
              <bpmn:endEvent id="HEnd"/>
              <bpmn:sequenceFlow id="hf1" sourceRef="HStart" targetRef="Recover"/>
              <bpmn:sequenceFlow id="hf2" sourceRef="Recover" targetRef="HEnd"/>
            </bpmn:subProcess>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

async fn run_raising(error_def: &str, thrown_code: &str) -> sutra_executor::ExecResult {
    let process = proc(&esp_bpmn(error_def), "p");
    let code = thrown_code.to_string();
    let registry = TaskRegistry::new()
        .register("risky", move |_, _| Err(TaskError::BpmnError(code.clone())))
        .register("recover", |_, _| ok_map(&[("recovered", boolean(true))]));
    TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap()
}

#[tokio::test]
async fn matching_error_triggers_the_event_sub_process() {
    let result = run_raising(
        r#"<bpmn:errorEventDefinition errorRef="errBoom"/>"#,
        "E_BOOM",
    )
    .await;
    assert_eq!(result.output("recovered"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("Recover"));
    assert!(result.visited_nodes.contains("HEnd"));
    assert!(!result.visited_nodes.contains("Done"));
}

#[tokio::test]
async fn catch_all_event_sub_process_catches_any_error() {
    let result = run_raising("<bpmn:errorEventDefinition/>", "SOME_OTHER_CODE").await;
    assert_eq!(result.output("recovered"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("Recover"));
    assert!(!result.visited_nodes.contains("Done"));
}

#[test]
fn non_error_event_sub_process_is_rejected_at_load() {
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(esp_bpmn("<bpmn:messageEventDefinition/>").as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_SUBPROCESS_UNSUPPORTED);
}

// ---- AdHocSubProcessTest --------------------------------------------------------------

const ADHOC_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
      <bpmn:process id="adhoc">
        <bpmn:startEvent id="S"/>
        <bpmn:adHocSubProcess id="Ah" ordering="Sequential">
          <bpmn:completionCondition>done</bpmn:completionCondition>
          <bpmn:serviceTask id="A" implementation="${a}"/>
          <bpmn:serviceTask id="B" implementation="${b}"/>
          <bpmn:serviceTask id="C" implementation="${c}"/>
        </bpmn:adHocSubProcess>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Ah"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Ah" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

async fn run_adhoc(
    effects: Rc<RefCell<Vec<&'static str>>>,
    b_signals_done: bool,
) -> sutra_executor::ExecResult {
    let process = proc(ADHOC_BPMN, "adhoc");
    let (ea, eb, ec) = (
        Rc::clone(&effects),
        Rc::clone(&effects),
        Rc::clone(&effects),
    );
    let registry = TaskRegistry::new()
        .register("a", move |_, _| {
            ea.borrow_mut().push("a");
            ok_map(&[])
        })
        .register("b", move |_, _| {
            eb.borrow_mut().push("b");
            ok_map(&[("done", boolean(b_signals_done))])
        })
        .register("c", move |_, _| {
            ec.borrow_mut().push("c");
            ok_map(&[])
        });
    TokenExecutor::builder(registry)
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap()
}

#[tokio::test]
async fn stops_early_when_completion_condition_holds() {
    let effects = Rc::new(RefCell::new(Vec::new()));
    let result = run_adhoc(Rc::clone(&effects), true).await; // B sets done=true → C skipped

    assert_eq!(effects.borrow().as_slice(), &["a", "b"]);
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn runs_every_activity_when_condition_stays_false() {
    let effects = Rc::new(RefCell::new(Vec::new()));
    let result = run_adhoc(Rc::clone(&effects), false).await;

    assert_eq!(effects.borrow().as_slice(), &["a", "b", "c"]);
    assert!(result.visited_nodes.contains("E"));
}

// ---- ThrowEventTest ----------------------------------------------------------------------

#[tokio::test]
async fn message_throw_emits_send_then_continues() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Notify">
              <bpmn:extensionElements>
                <q:send destination="https://ops.example/notify" contentType="text/plain"/>
              </bpmn:extensionElements>
              <bpmn:messageEventDefinition/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("Notify").unwrap() {
        Node::IntermediateThrowEvent { kind, .. } => assert_eq!(*kind, ThrowKind::Message),
        other => panic!("expected IntermediateThrowEvent, got {other:?}"),
    }

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    let result = executor
        .execute_sync(&process, vars(&[("payload.body", string("flagged"))]))
        .await
        .unwrap();

    assert!(result.visited_nodes.contains("Notify"));
    assert!(result.visited_nodes.contains("E"));
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "https://ops.example/notify");
    assert_eq!(emissions[0].body_utf8(), "flagged");
}

#[test]
fn message_throw_without_send_fails_closed_at_load() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Notify"><bpmn:messageEventDefinition/></bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_THROW_SEND_REQUIRED);
}

#[tokio::test]
async fn signal_throw_without_send_is_pure_continue() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:signal id="sig1" name="PaymentFlagged"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Raise">
              <bpmn:signalEventDefinition signalRef="sig1"/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Raise"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Raise" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("Raise").unwrap() {
        Node::IntermediateThrowEvent {
            kind, reference, ..
        } => {
            assert_eq!(*kind, ThrowKind::Signal);
            assert_eq!(reference.as_deref(), Some("PaymentFlagged"));
        }
        other => panic!("expected IntermediateThrowEvent, got {other:?}"),
    }

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    let result = executor.execute_sync(&process, vars(&[])).await.unwrap();

    assert!(result.visited_nodes.contains("Raise"));
    assert!(result.visited_nodes.contains("E"));
    assert!(sink.is_empty());
}

#[tokio::test]
async fn escalation_throw_routes_to_boundary_handler_and_main_flow_continues() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:escalation id="esc1" escalationCode="LATE"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Guarded" implementation="${guard}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="Guarded">
              <bpmn:escalationEventDefinition escalationRef="esc1"/>
            </bpmn:boundaryEvent>
            <bpmn:intermediateThrowEvent id="Esc">
              <bpmn:escalationEventDefinition escalationRef="esc1"/>
            </bpmn:intermediateThrowEvent>
            <bpmn:serviceTask id="Handler" implementation="${handler}"/>
            <bpmn:endEvent id="HandlerEnd"/>
            <bpmn:endEvent id="MainEnd"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Guarded"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Guarded" targetRef="Esc"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Esc" targetRef="MainEnd"/>
            <bpmn:sequenceFlow id="fB" sourceRef="B" targetRef="Handler"/>
            <bpmn:sequenceFlow id="fH" sourceRef="Handler" targetRef="HandlerEnd"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("B").unwrap() {
        Node::BoundaryEvent {
            kind,
            escalation_code,
            interrupting,
            ..
        } => {
            assert_eq!(*kind, BoundaryKind::Escalation);
            assert_eq!(escalation_code.as_deref(), Some("LATE"));
            assert!(
                !interrupting,
                "escalation boundary defaults non-interrupting"
            );
        }
        other => panic!("expected BoundaryEvent, got {other:?}"),
    }

    let handler_runs = Rc::new(RefCell::new(0));
    let hr = Rc::clone(&handler_runs);
    let registry = TaskRegistry::new()
        .register("guard", |_, _| ok_map(&[]))
        .register("handler", move |_, _| {
            *hr.borrow_mut() += 1;
            ok_map(&[("handled", boolean(true))])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*handler_runs.borrow(), 1);
    for node in ["Handler", "HandlerEnd", "MainEnd"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
}

#[tokio::test]
async fn interrupting_escalation_cancels_the_raising_flow() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:escalation id="esc1" escalationCode="FATAL"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Guarded" implementation="${guard}"/>
            <bpmn:boundaryEvent id="B" attachedToRef="Guarded" cancelActivity="true">
              <bpmn:escalationEventDefinition escalationRef="esc1"/>
            </bpmn:boundaryEvent>
            <bpmn:intermediateThrowEvent id="Esc">
              <bpmn:escalationEventDefinition escalationRef="esc1"/>
            </bpmn:intermediateThrowEvent>
            <bpmn:serviceTask id="AfterEsc" implementation="${after}"/>
            <bpmn:endEvent id="MainEnd"/>
            <bpmn:endEvent id="HandlerEnd"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Guarded"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Guarded" targetRef="Esc"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Esc" targetRef="AfterEsc"/>
            <bpmn:sequenceFlow id="f4" sourceRef="AfterEsc" targetRef="MainEnd"/>
            <bpmn:sequenceFlow id="fB" sourceRef="B" targetRef="HandlerEnd"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("B").unwrap() {
        Node::BoundaryEvent { interrupting, .. } => assert!(interrupting),
        other => panic!("expected BoundaryEvent, got {other:?}"),
    }

    let after_runs = Rc::new(RefCell::new(0));
    let ar = Rc::clone(&after_runs);
    let registry = TaskRegistry::new()
        .register("guard", |_, _| ok_map(&[]))
        .register("after", move |_, _| {
            *ar.borrow_mut() += 1;
            ok_map(&[])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(
        *after_runs.borrow(),
        0,
        "interrupting escalation cancels the raising continuation"
    );
    assert!(result.visited_nodes.contains("HandlerEnd"));
    assert!(!result.visited_nodes.contains("AfterEsc"));
    assert!(!result.visited_nodes.contains("MainEnd"));
}

#[tokio::test]
async fn link_throw_jumps_to_matching_link_catch() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Goto">
              <bpmn:linkEventDefinition name="Resume"/>
            </bpmn:intermediateThrowEvent>
            <bpmn:intermediateCatchEvent id="Here">
              <bpmn:linkEventDefinition name="Resume"/>
            </bpmn:intermediateCatchEvent>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Goto"/>
            <bpmn:sequenceFlow id="fC" sourceRef="Here" targetRef="After"/>
            <bpmn:sequenceFlow id="fA" sourceRef="After" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(matches!(
        process.node("Here").unwrap(),
        Node::LinkCatchEvent { .. }
    ));
    match process.node("Goto").unwrap() {
        Node::IntermediateThrowEvent { kind, .. } => assert_eq!(*kind, ThrowKind::Link),
        other => panic!("expected IntermediateThrowEvent, got {other:?}"),
    }

    let after_runs = Rc::new(RefCell::new(0));
    let ar = Rc::clone(&after_runs);
    let registry = TaskRegistry::new().register("after", move |_, _| {
        *ar.borrow_mut() += 1;
        ok_map(&[("resumed", boolean(true))])
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*after_runs.borrow(), 1);
    for node in ["Goto", "Here", "After", "E"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
}

#[test]
fn link_throw_with_no_catch_fails_closed_at_load() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Goto">
              <bpmn:linkEventDefinition name="Nowhere"/>
            </bpmn:intermediateThrowEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Goto"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_LINK_CATCH_NOT_FOUND);
}

#[test]
fn duplicate_link_catch_fails_closed_at_load() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="C1"><bpmn:linkEventDefinition name="Dup"/></bpmn:intermediateCatchEvent>
            <bpmn:intermediateCatchEvent id="C2"><bpmn:linkEventDefinition name="Dup"/></bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_LINK_CATCH_DUPLICATE);
}
