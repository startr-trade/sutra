//! Event-based gateway across a park: the first pass arms every arm (and a timer arm's durable
//! row); the resume after one arm fires advances the token THROUGH that arm and arms nothing
//! again — the losers were retired in the same step that recorded the win.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{DeploymentId, StatefulExecResult, TaskRegistry, TokenExecutor, Variables};

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000081").expect("valid deployment id")
}

/// S -> G (event-based) -> { A: message on `answers` -> T (side effect) -> E,
///                           B: timer PT1H -> E2 }
const RACE: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="race">
        <bpmn:startEvent id="S"><bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements></bpmn:startEvent>
        <bpmn:eventBasedGateway id="G"/>
        <bpmn:intermediateCatchEvent id="A">
          <bpmn:extensionElements><q:source channel="answers"/></bpmn:extensionElements>
          <bpmn:messageEventDefinition/>
        </bpmn:intermediateCatchEvent>
        <bpmn:intermediateCatchEvent id="B">
          <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
        </bpmn:intermediateCatchEvent>
        <bpmn:serviceTask id="T" implementation="${after}"/>
        <bpmn:endEvent id="E"/>
        <bpmn:endEvent id="E2"/>
        <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="G"/>
        <bpmn:sequenceFlow id="f1" sourceRef="G" targetRef="A"/>
        <bpmn:sequenceFlow id="f2" sourceRef="G" targetRef="B"/>
        <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="T"/>
        <bpmn:sequenceFlow id="f4" sourceRef="T" targetRef="E"/>
        <bpmn:sequenceFlow id="f5" sourceRef="B" targetRef="E2"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn executor(after_runs: Rc<RefCell<u32>>) -> TokenExecutor {
    let registry = TaskRegistry::new().register("after", move |_, _| {
        *after_runs.borrow_mut() += 1;
        ok_map(&[("after", boolean(true))])
    });
    TokenExecutor::builder(registry)
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build()
}

async fn park(executor: &TokenExecutor) -> (Vec<String>, Vec<String>, Variables) {
    let process = proc(RACE, "race");
    let StatefulExecResult::Suspended {
        waiting_nodes,
        completed_nodes,
        variables,
        timer_waits,
        ..
    } = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .unwrap()
    else {
        panic!("the race parks");
    };
    assert_eq!(waiting_nodes, vec!["A".to_string(), "B".to_string()]);
    assert_eq!(timer_waits.len(), 1, "the timer arm has its durable row");
    assert_eq!(timer_waits[0].node_id, "B");
    (waiting_nodes, completed_nodes, variables)
}

async fn resume_with(
    executor: &TokenExecutor,
    satisfied: &str,
    parked: (Vec<String>, Vec<String>, Variables),
) -> StatefulExecResult {
    let (waiting, completed, variables) = parked;
    executor
        .resume(
            &proc(RACE, "race"),
            "inst-1",
            &completed,
            variables,
            satisfied,
            &Variables::new(),
            dep(),
            BTreeMap::new(),
            None,
            &waiting,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn the_message_arm_wins_and_the_token_goes_on_through_it() {
    let runs = Rc::new(RefCell::new(0));
    let executor = executor(Rc::clone(&runs));
    let parked = park(&executor).await;
    let StatefulExecResult::Completed { visited_nodes, .. } =
        resume_with(&executor, "A", parked).await
    else {
        panic!("the instance completes through the winning arm, nothing is re-armed");
    };
    assert!(visited_nodes.contains("T") && visited_nodes.contains("E"));
    assert!(!visited_nodes.contains("E2"), "the losing arm never runs");
    assert_eq!(*runs.borrow(), 1, "the winner's path ran once");
}

#[tokio::test]
async fn the_timer_arm_wins_and_the_message_arm_is_not_armed_again() {
    let runs = Rc::new(RefCell::new(0));
    let executor = executor(Rc::clone(&runs));
    let parked = park(&executor).await;
    let StatefulExecResult::Completed { visited_nodes, .. } =
        resume_with(&executor, "B", parked).await
    else {
        panic!("the instance completes through the timer arm");
    };
    assert!(visited_nodes.contains("E2"));
    assert!(!visited_nodes.contains("T"));
    assert_eq!(*runs.borrow(), 0);
}
