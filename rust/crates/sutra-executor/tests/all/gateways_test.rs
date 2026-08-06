//! Gateway and loop semantics: complex-gateway n-of-m joins and diverging behaviour, inclusive
//! gateways, multi-instance (sequential / parallel / collection) and standard loops.

use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::Node;
use sutra_executor::executor::feel_condition_evaluator;
use sutra_executor::{TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

// ---- ComplexGatewayTest ---------------------------------------------------------

const THREE_INTO_JOIN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
      <bpmn:process id="p1">
        <bpmn:startEvent id="S"/>
        <bpmn:parallelGateway id="Fork"/>
        <bpmn:serviceTask id="TA" implementation="${a}"/>
        <bpmn:serviceTask id="TB" implementation="${b}"/>
        <bpmn:serviceTask id="TC" implementation="${c}"/>
        <bpmn:complexGateway id="Join">%ACTIVATION%</bpmn:complexGateway>
        <bpmn:serviceTask id="After" implementation="${after}"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
        <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA"/>
        <bpmn:sequenceFlow id="fB" sourceRef="Fork" targetRef="TB"/>
        <bpmn:sequenceFlow id="fC" sourceRef="Fork" targetRef="TC"/>
        <bpmn:sequenceFlow id="fAJ" sourceRef="TA" targetRef="Join"/>
        <bpmn:sequenceFlow id="fBJ" sourceRef="TB" targetRef="Join"/>
        <bpmn:sequenceFlow id="fCJ" sourceRef="TC" targetRef="Join"/>
        <bpmn:sequenceFlow id="fJA" sourceRef="Join" targetRef="After"/>
        <bpmn:sequenceFlow id="fAE" sourceRef="After" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn abc_after(after_runs: Rc<RefCell<i32>>) -> TaskRegistry {
    TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("b", |_, _| ok_map(&[("ranB", boolean(true))]))
        .register("c", |_, _| ok_map(&[("ranC", boolean(true))]))
        .register("after", move |_, _| {
            *after_runs.borrow_mut() += 1;
            ok_map(&[("ranAfter", boolean(true))])
        })
}

fn feel_executor(registry: TaskRegistry) -> TokenExecutor {
    TokenExecutor::builder(registry)
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
}

#[tokio::test]
async fn n_of_m_join_fires_once_when_activation_condition_met() {
    let bpmn = THREE_INTO_JOIN.replace(
        "%ACTIVATION%",
        "<bpmn:activationCondition>arrivedCount >= 2</bpmn:activationCondition>",
    );
    let process = proc(&bpmn, "p1");
    match process.node("Join").unwrap() {
        Node::ComplexGateway {
            activation_condition,
            ..
        } => assert_eq!(activation_condition.as_deref(), Some("arrivedCount >= 2")),
        other => panic!("expected ComplexGateway, got {other:?}"),
    }

    let after_runs = Rc::new(RefCell::new(0));
    let result = feel_executor(abc_after(Rc::clone(&after_runs)))
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    // Fires at the 2nd of 3 arrivals and absorbs the 3rd — After runs exactly once.
    assert_eq!(*after_runs.borrow(), 1);
    assert_eq!(result.output("ranAfter"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn join_without_activation_condition_waits_for_all_arrivals() {
    let bpmn = THREE_INTO_JOIN.replace("%ACTIVATION%", "");
    let process = proc(&bpmn, "p1");
    match process.node("Join").unwrap() {
        Node::ComplexGateway {
            activation_condition,
            ..
        } => assert!(activation_condition.is_none()),
        other => panic!("expected ComplexGateway, got {other:?}"),
    }

    let after_runs = Rc::new(RefCell::new(0));
    let result = feel_executor(abc_after(Rc::clone(&after_runs)))
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*after_runs.borrow(), 1);
    assert_eq!(result.output("ranAfter"), Some(&boolean(true)));
}

#[tokio::test]
async fn diverging_complex_gateway_takes_every_satisfied_flow_like_inclusive() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:complexGateway id="Fork" default="fD"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TB" implementation="${b}"/>
            <bpmn:serviceTask id="TDef" implementation="${def}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>doA</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fB" sourceRef="Fork" targetRef="TB">
              <bpmn:conditionExpression>doB</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fD" sourceRef="Fork" targetRef="TDef"/>
            <bpmn:sequenceFlow id="fAE" sourceRef="TA" targetRef="E"/>
            <bpmn:sequenceFlow id="fBE" sourceRef="TB" targetRef="E"/>
            <bpmn:sequenceFlow id="fDE" sourceRef="TDef" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("b", |_, _| ok_map(&[("ranB", boolean(true))]))
        .register("def", |_, _| ok_map(&[("ranDef", boolean(true))]));
    let result = feel_executor(registry)
        .execute_sync(
            &process,
            vars(&[("doA", boolean(true)), ("doB", boolean(true))]),
        )
        .await
        .unwrap();

    assert_eq!(result.output("ranA"), Some(&boolean(true)));
    assert_eq!(result.output("ranB"), Some(&boolean(true)));
    assert_eq!(result.output("ranDef"), None);
}

#[tokio::test]
async fn diverging_complex_gateway_falls_back_to_default_when_no_condition_matches() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:complexGateway id="Fork" default="fD"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TDef" implementation="${def}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>doA</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fD" sourceRef="Fork" targetRef="TDef"/>
            <bpmn:sequenceFlow id="fAE" sourceRef="TA" targetRef="E"/>
            <bpmn:sequenceFlow id="fDE" sourceRef="TDef" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("def", |_, _| ok_map(&[("ranDef", boolean(true))]));
    let result = feel_executor(registry)
        .execute_sync(&process, vars(&[("doA", boolean(false))]))
        .await
        .unwrap();

    assert_eq!(result.output("ranDef"), Some(&boolean(true)));
    assert_eq!(result.output("ranA"), None);
}

// ---- InclusiveGatewayTest --------------------------------------------------------

#[tokio::test]
async fn fork_takes_all_satisfied_flows() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:inclusiveGateway id="Fork" default="fDefault"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TB" implementation="${b}"/>
            <bpmn:serviceTask id="TC" implementation="${c}"/>
            <bpmn:inclusiveGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>doA</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fB" sourceRef="Fork" targetRef="TB">
              <bpmn:conditionExpression>doB</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fC" sourceRef="Fork" targetRef="TC">
              <bpmn:conditionExpression>doC</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fDefault" sourceRef="Fork" targetRef="Join"/>
            <bpmn:sequenceFlow id="fAJ" sourceRef="TA" targetRef="Join"/>
            <bpmn:sequenceFlow id="fBJ" sourceRef="TB" targetRef="Join"/>
            <bpmn:sequenceFlow id="fCJ" sourceRef="TC" targetRef="Join"/>
            <bpmn:sequenceFlow id="fJE" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let calls = Rc::new(RefCell::new(0));
    let (c1, c2, c3) = (Rc::clone(&calls), Rc::clone(&calls), Rc::clone(&calls));
    let registry = TaskRegistry::new()
        .register("a", move |_, _| {
            *c1.borrow_mut() += 1;
            ok_map(&[("ranA", boolean(true))])
        })
        .register("b", move |_, _| {
            *c2.borrow_mut() += 1;
            ok_map(&[("ranB", boolean(true))])
        })
        .register("c", move |_, _| {
            *c3.borrow_mut() += 1;
            ok_map(&[("ranC", boolean(true))])
        });
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(var_truthy_evaluator())
        .build();
    let result = executor
        .execute_sync(
            &process,
            vars(&[
                ("doA", boolean(true)),
                ("doB", boolean(false)),
                ("doC", boolean(true)),
            ]),
        )
        .await
        .unwrap();

    // A and C run; B skipped.
    assert_eq!(*calls.borrow(), 2);
    assert_eq!(result.output("ranA"), Some(&boolean(true)));
    assert_eq!(result.output("ranC"), Some(&boolean(true)));
    assert_eq!(result.output("ranB"), None);
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn fork_falls_back_to_default_when_no_condition_matches() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:inclusiveGateway id="Fork" default="fDefault"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TDef" implementation="${def}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>never</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fDefault" sourceRef="Fork" targetRef="TDef"/>
            <bpmn:sequenceFlow id="fEnd" sourceRef="TDef" targetRef="E"/>
            <bpmn:sequenceFlow id="fEnd2" sourceRef="TA" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("def", |_, _| ok_map(&[("ranDef", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(var_truthy_evaluator())
        .build();
    let result = executor
        .execute_sync(&process, vars(&[("never", boolean(false))]))
        .await
        .unwrap();

    assert_eq!(result.output("ranDef"), Some(&boolean(true)));
    assert_eq!(result.output("ranA"), None);
}

#[tokio::test]
async fn join_waits_for_all_expected_tokens() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:inclusiveGateway id="Fork"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:serviceTask id="TB" implementation="${b}"/>
            <bpmn:inclusiveGateway id="Join"/>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>doA</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fB" sourceRef="Fork" targetRef="TB">
              <bpmn:conditionExpression>doB</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fAJ" sourceRef="TA" targetRef="Join"/>
            <bpmn:sequenceFlow id="fBJ" sourceRef="TB" targetRef="Join"/>
            <bpmn:sequenceFlow id="fJAfter" sourceRef="Join" targetRef="After"/>
            <bpmn:sequenceFlow id="fAfterEnd" sourceRef="After" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let after_runs = Rc::new(RefCell::new(0));
    let ar = Rc::clone(&after_runs);
    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("b", |_, _| ok_map(&[("ranB", boolean(true))]))
        .register("after", move |_, _| {
            *ar.borrow_mut() += 1;
            ok_map(&[("ranAfter", boolean(true))])
        });
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(var_truthy_evaluator())
        .build();
    let result = executor
        .execute_sync(
            &process,
            vars(&[("doA", boolean(true)), ("doB", boolean(true))]),
        )
        .await
        .unwrap();

    // After runs exactly once even though two tokens arrive at Join.
    assert_eq!(*after_runs.borrow(), 1);
    assert_eq!(result.output("ranAfter"), Some(&boolean(true)));
}

#[tokio::test]
async fn single_branch_inclusive_join_fires_immediately() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:inclusiveGateway id="Fork"/>
            <bpmn:serviceTask id="TA" implementation="${a}"/>
            <bpmn:inclusiveGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fA" sourceRef="Fork" targetRef="TA">
              <bpmn:conditionExpression>doA</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fAJ" sourceRef="TA" targetRef="Join"/>
            <bpmn:sequenceFlow id="fJE" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("a", |_, _| ok_map(&[("ranA", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(var_truthy_evaluator())
        .build();
    let result = executor
        .execute_sync(&process, vars(&[("doA", boolean(true))]))
        .await
        .unwrap();
    assert_eq!(result.output("ranA"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn inclusive_gateway_uses_feel_evaluator_for_conditions() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:inclusiveGateway id="Fork" default="fD"/>
            <bpmn:serviceTask id="TBig" implementation="${big}"/>
            <bpmn:serviceTask id="TDef" implementation="${def}"/>
            <bpmn:inclusiveGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fBig" sourceRef="Fork" targetRef="TBig">
              <bpmn:conditionExpression>amount &gt; 100</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fD" sourceRef="Fork" targetRef="TDef"/>
            <bpmn:sequenceFlow id="fBJ" sourceRef="TBig" targetRef="Join"/>
            <bpmn:sequenceFlow id="fDJ" sourceRef="TDef" targetRef="Join"/>
            <bpmn:sequenceFlow id="fJE" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new()
        .register("big", |_, _| ok_map(&[("ranBig", boolean(true))]))
        .register("def", |_, _| ok_map(&[("ranDef", boolean(true))]));
    let result = feel_executor(registry)
        .execute_sync(&process, vars(&[("amount", num(250))]))
        .await
        .unwrap();
    assert_eq!(result.output("ranBig"), Some(&boolean(true)));
    assert_eq!(result.output("ranDef"), None);
}

// ---- MultiInstanceTest -----------------------------------------------------------

fn mi_process(characteristics: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${{tick}}">
              {characteristics}
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[tokio::test]
async fn sequential_cardinality_runs_inner_task_n_times() {
    let process = proc(
        &mi_process(
            r#"<bpmn:multiInstanceLoopCharacteristics isSequential="true">
                 <bpmn:loopCardinality>3</bpmn:loopCardinality>
               </bpmn:multiInstanceLoopCharacteristics>"#,
        ),
        "p1",
    );
    let counter = Rc::new(RefCell::new(0));
    let c = Rc::clone(&counter);
    let registry = TaskRegistry::new().register("tick", move |_, _| {
        *c.borrow_mut() += 1;
        ok_map(&[])
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 3);
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn parallel_cardinality_runs_inner_task_n_times() {
    let process = proc(
        &mi_process(
            r#"<bpmn:multiInstanceLoopCharacteristics isSequential="false">
                 <bpmn:loopCardinality>3</bpmn:loopCardinality>
               </bpmn:multiInstanceLoopCharacteristics>"#,
        ),
        "p1",
    );
    let counter = Rc::new(RefCell::new(0));
    let c = Rc::clone(&counter);
    let registry = TaskRegistry::new().register("tick", move |_, _| {
        *c.borrow_mut() += 1;
        ok_map(&[])
    });
    TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 3);
}

#[tokio::test]
async fn collection_iteration_binds_input_data_item() {
    // This case needs a recording task, so swap "capture" into the shared template.
    let bpmn = mi_process(
        r#"<bpmn:multiInstanceLoopCharacteristics isSequential="true">
             <bpmn:loopDataInputRef>items</bpmn:loopDataInputRef>
             <bpmn:inputDataItem name="entry"/>
           </bpmn:multiInstanceLoopCharacteristics>"#,
    )
    .replace("${tick}", "${capture}");
    let process = proc(&bpmn, "p1");

    let seen: Rc<RefCell<Vec<FeelValue>>> = Rc::new(RefCell::new(Vec::new()));
    let s = Rc::clone(&seen);
    let registry = TaskRegistry::new().register("capture", move |_, ctx| {
        s.borrow_mut()
            .push(ctx.variable("entry").cloned().unwrap_or(FeelValue::Null));
        ok_map(&[])
    });
    TokenExecutor::builder(registry)
        .build()
        .execute_sync(
            &process,
            vars(&[(
                "items",
                FeelValue::List(vec![string("alpha"), string("beta"), string("gamma")]),
            )]),
        )
        .await
        .unwrap();
    assert_eq!(
        seen.borrow().as_slice(),
        &[string("alpha"), string("beta"), string("gamma")]
    );
}

#[tokio::test]
async fn completion_condition_stops_iteration_early() {
    let process = proc(
        &mi_process(
            r#"<bpmn:multiInstanceLoopCharacteristics isSequential="true">
                 <bpmn:loopCardinality>10</bpmn:loopCardinality>
                 <bpmn:completionCondition>loopCounter &gt;= 2</bpmn:completionCondition>
               </bpmn:multiInstanceLoopCharacteristics>"#,
        ),
        "p1",
    );
    let counter = Rc::new(RefCell::new(0));
    let c = Rc::clone(&counter);
    let registry = TaskRegistry::new().register("tick", move |_, _| {
        *c.borrow_mut() += 1;
        ok_map(&[])
    });
    TokenExecutor::builder(registry)
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    // After the third iteration (loopCounter=2), the completion condition fires.
    assert_eq!(*counter.borrow(), 3);
}

#[tokio::test]
async fn empty_collection_runs_zero_iterations() {
    let process = proc(
        &mi_process(
            r#"<bpmn:multiInstanceLoopCharacteristics isSequential="true">
                 <bpmn:loopDataInputRef>items</bpmn:loopDataInputRef>
               </bpmn:multiInstanceLoopCharacteristics>"#,
        ),
        "p1",
    );
    let counter = Rc::new(RefCell::new(0));
    let c = Rc::clone(&counter);
    let registry = TaskRegistry::new().register("tick", move |_, _| {
        *c.borrow_mut() += 1;
        ok_map(&[])
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[("items", FeelValue::List(vec![]))]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 0);
    assert!(result.visited_nodes.contains("E"));
}

// ---- StandardLoopTest --------------------------------------------------------------

fn looped_task(characteristics: &str) -> String {
    mi_process(characteristics)
}

fn tick_registry(counter: Rc<RefCell<i64>>) -> TaskRegistry {
    TaskRegistry::new().register("tick", move |_, _| {
        *counter.borrow_mut() += 1;
        let n = *counter.borrow();
        ok_map(&[("count", num(n))])
    })
}

#[test]
fn standard_loop_characteristics_parse_to_standard_loop_node() {
    let bpmn = looped_task(
        r#"<bpmn:standardLoopCharacteristics testBefore="true" loopMaximum="5">
             <bpmn:loopCondition>count &lt; 3</bpmn:loopCondition>
           </bpmn:standardLoopCharacteristics>"#,
    );
    let process = proc(&bpmn, "p1");
    match process.node("T").unwrap() {
        Node::StandardLoop {
            test_before,
            loop_maximum,
            loop_condition,
            inner,
            ..
        } => {
            assert!(test_before);
            assert_eq!(*loop_maximum, Some(5));
            assert_eq!(loop_condition.as_deref(), Some("count < 3"));
            assert!(matches!(**inner, Node::ServiceTask { .. }));
        }
        other => panic!("expected StandardLoop, got {other:?}"),
    }
}

#[tokio::test]
async fn do_while_runs_until_condition_fails() {
    // testBefore=false ⇒ run once, then check. count reaches 3, then 3 < 3 is false.
    let bpmn = looped_task(
        r#"<bpmn:standardLoopCharacteristics testBefore="false">
             <bpmn:loopCondition>count &lt; 3</bpmn:loopCondition>
           </bpmn:standardLoopCharacteristics>"#,
    );
    let process = proc(&bpmn, "p1");
    let counter = Rc::new(RefCell::new(0i64));
    let result = TokenExecutor::builder(tick_registry(Rc::clone(&counter)))
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 3);
    assert_eq!(result.output("count"), Some(&num(3)));
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn while_test_before_runs_zero_times_when_condition_starts_false() {
    let bpmn = looped_task(
        r#"<bpmn:standardLoopCharacteristics testBefore="true">
             <bpmn:loopCondition>count &lt; 3</bpmn:loopCondition>
           </bpmn:standardLoopCharacteristics>"#,
    );
    let process = proc(&bpmn, "p1");
    let counter = Rc::new(RefCell::new(0i64));
    let result = TokenExecutor::builder(tick_registry(Rc::clone(&counter)))
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[("count", num(5))]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 0);
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn loop_maximum_caps_iterations() {
    let bpmn = looped_task(
        r#"<bpmn:standardLoopCharacteristics testBefore="false" loopMaximum="2">
             <bpmn:loopCondition>count &lt; 100</bpmn:loopCondition>
           </bpmn:standardLoopCharacteristics>"#,
    );
    let process = proc(&bpmn, "p1");
    let counter = Rc::new(RefCell::new(0i64));
    let result = TokenExecutor::builder(tick_registry(Rc::clone(&counter)))
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(*counter.borrow(), 2);
    assert_eq!(result.output("count"), Some(&num(2)));
}
