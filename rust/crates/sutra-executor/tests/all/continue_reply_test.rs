//! Respond-and-continue — a `<q:reply continue="true">` service task flushes its reply and
//! PARKS, then a due-now timer self-resumes the remaining nodes. Executor-level: the first pass
//! produces the reply and parks (surfacing `detached_reply` + a due-now marker, tail NOT run); the
//! resume runs the tail exactly once (the reply task is `completed`, so the replay skips it).

use std::collections::BTreeMap;

use crate::common::*;
use sutra_executor::{DeploymentId, StatefulExecResult, TaskRegistry, TokenExecutor, Variables};

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000071").expect("valid deployment id")
}

const CONTINUE_FLOW: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p1">
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="${reply}">
          <bpmn:extensionElements><q:reply continue="true"/></bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:serviceTask id="T2" implementation="${tail}"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="T2"/>
        <bpmn:sequenceFlow id="f3" sourceRef="T2" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn executor() -> TokenExecutor {
    let registry = TaskRegistry::new()
        .register("reply", |_, _| ok_map(&[("responseBody", string("OK"))]))
        .register("tail", |_, _| ok_map(&[("tailRan", boolean(true))]));
    TokenExecutor::builder(registry)
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build()
}

#[test]
fn continue_reply_process_is_not_sync_eligible() {
    // A `<q:reply continue>` forces the stateful path even with no other wait node.
    assert!(!proc(CONTINUE_FLOW, "p1").is_sync_eligible());
}

#[tokio::test]
async fn continue_reply_parks_with_reply_then_tail_resumes_once() {
    let process = proc(CONTINUE_FLOW, "p1");
    let executor = executor();

    // Pass 1: run up to the continue-reply task, produce the reply body, and PARK (tail NOT run).
    let result = executor
        .execute_stateful_from(&process, vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .unwrap();
    let StatefulExecResult::Suspended {
        waiting_nodes,
        completed_nodes,
        variables,
        timer_waits,
        detached_reply,
        ..
    } = result
    else {
        panic!("expected a continue-reply park, not completion");
    };
    assert!(
        detached_reply,
        "the continue-reply park must set detached_reply (flush signal)"
    );
    assert!(waiting_nodes.contains(&"T".to_string()));
    assert!(
        completed_nodes.contains(&"T".to_string()),
        "the reply task ran"
    );
    assert!(
        !completed_nodes.contains(&"T2".to_string()),
        "the tail must NOT have run before the reply is flushed"
    );
    assert_eq!(
        variables.get("responseBody"),
        Some(&string("OK")),
        "the reply body is produced at the park"
    );
    assert_eq!(
        variables.get("tailRan"),
        None,
        "the tail side-effect must not have fired yet"
    );
    assert_eq!(timer_waits.len(), 1, "one due-now self-resume marker");
    assert_eq!(
        timer_waits[0].node_id, "T",
        "the marker is on the continue-reply node"
    );
    assert_eq!(timer_waits[0].due_at, "2026-01-01T00:00:00Z", "due now");

    // Pass 2 — the self-resume (fire_timer routes a continue-reply node through the relay `resume`
    // path with no payload). The tail runs to completion; the reply task is not re-run.
    let done = executor
        .resume(
            &process,
            "inst-1",
            &completed_nodes,
            variables,
            "T",
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
    let StatefulExecResult::Completed {
        visited_nodes,
        outputs,
        ..
    } = done
    else {
        panic!("expected completion after the continue-resume");
    };
    assert!(visited_nodes.contains("T2"), "the tail node runs on resume");
    assert!(visited_nodes.contains("E"), "the process completes");
    assert_eq!(
        outputs.get("tailRan"),
        Some(&boolean(true)),
        "the tail side-effect fired exactly once, on resume"
    );
}
