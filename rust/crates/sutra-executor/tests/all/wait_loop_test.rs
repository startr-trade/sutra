//! Re-entering a wait state: a loop that brings the token back to a node it already waited at
//! parks a NEW wait there, any number of times within one instance — an intermediate message
//! catch, a userTask sent back for rework, a timer catch, a channel-call, an event-based
//! gateway's race. Also pins the resume contract those loops rest on: the token continues from
//! the satisfied node (nothing upstream runs again), the other parked waits carry over
//! untouched, and a converging gateway regains the arrivals of tokens that already reached it.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, OutboundChannelRegistry, ResolvedOutboundChannel,
    StatefulExecResult, TaskRegistry, TimerFire, TokenExecutor, Variables,
};

const INSTANCE: &str = "11111111-2222-4333-8444-555555555555";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000072").expect("valid deployment id")
}

/// A suspended pass, flattened for the next resume.
#[derive(Debug, Clone)]
struct Parked {
    waiting: Vec<String>,
    completed: Vec<String>,
    variables: Variables,
    fresh: Vec<String>,
    timers: Vec<String>,
}

fn parked(result: StatefulExecResult) -> Parked {
    match result {
        StatefulExecResult::Suspended {
            waiting_nodes,
            completed_nodes,
            variables,
            fresh_waits,
            timer_waits,
            ..
        } => Parked {
            waiting: waiting_nodes,
            completed: completed_nodes,
            variables,
            fresh: fresh_waits,
            timers: timer_waits.into_iter().map(|t| t.node_id).collect(),
        },
        StatefulExecResult::Completed { .. } => panic!("expected a park, got completion"),
    }
}

fn counter_task(
    count: &Rc<Cell<i64>>,
    var: &'static str,
) -> impl Fn(
    &sutra_feel::FeelValue,
    &sutra_executor::TaskContextView,
) -> Result<sutra_feel::FeelValue, sutra_executor::TaskError> {
    let count = Rc::clone(count);
    move |_, _| {
        count.set(count.get() + 1);
        ok_map(&[(var, num(count.get()))])
    }
}

async fn start(executor: &TokenExecutor, bpmn: &str) -> StatefulExecResult {
    executor
        .execute_stateful_from(&proc(bpmn, "p1"), vars(&[]), dep(), BTreeMap::new(), None)
        .await
        .expect("first pass runs")
}

async fn relay(
    executor: &TokenExecutor,
    bpmn: &str,
    at: &Parked,
    node: &str,
    payload: Variables,
) -> StatefulExecResult {
    executor
        .resume(
            &proc(bpmn, "p1"),
            INSTANCE,
            &at.completed,
            at.variables.clone(),
            node,
            &payload,
            dep(),
            BTreeMap::new(),
            None,
            &at.waiting,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("resume runs")
}

async fn fire(executor: &TokenExecutor, bpmn: &str, at: &Parked, node: &str) -> StatefulExecResult {
    let fire = TimerFire {
        deployment: dep(),
        instance_id: INSTANCE.to_string(),
        node_id: node.to_string(),
        due_at: "2026-01-01T00:00:10Z".to_string(),
        fired_at: "2026-01-01T00:00:10Z".to_string(),
    };
    executor
        .resume_timer(
            &proc(bpmn, "p1"),
            &fire,
            &at.completed,
            at.variables.clone(),
            BTreeMap::new(),
            None,
            &at.waiting,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("timer resume runs")
}

fn feel_executor(registry: TaskRegistry) -> TokenExecutor {
    TokenExecutor::builder(registry)
        .with_feel()
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build()
}

// ---- an intermediate message catch in a loop -------------------------------------------------

/// S → Prompt → W (message catch) → G: `turn < 3` loops back to Prompt, else E.
const TURN_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Prompt" implementation="${prompt}"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements><q:source channel="turn-in" name="turn"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prompt"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prompt" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Prompt">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn a_message_catch_in_a_loop_parks_a_new_wait_on_every_iteration() {
    let prompts = Rc::new(Cell::new(0));
    let executor =
        feel_executor(TaskRegistry::new().register("prompt", counter_task(&prompts, "turn")));

    let mut at = parked(start(&executor, TURN_LOOP).await);
    assert_eq!(at.waiting, ["W"]);
    assert_eq!(at.fresh, ["W"], "a first park is a fresh wait");
    assert_eq!(prompts.get(), 1);

    for turn in 2..=3 {
        at = parked(relay(&executor, TURN_LOOP, &at, "W", vars(&[])).await);
        assert_eq!(at.waiting, ["W"], "turn {turn}: the loop parks at W again");
        assert_eq!(
            at.fresh,
            ["W"],
            "turn {turn}: and it is a NEW wait, not the old one"
        );
        assert_eq!(
            prompts.get(),
            turn,
            "the loop body runs once per turn — never replayed"
        );
    }

    let done = relay(&executor, TURN_LOOP, &at, "W", vars(&[])).await;
    let StatefulExecResult::Completed { outputs, .. } = done else {
        panic!("the third turn leaves the loop");
    };
    assert_eq!(outputs.get("turn"), Some(&num(3)));
    assert_eq!(prompts.get(), 3, "nothing ran again on the way out");
}

/// A snapshot written before waits were re-enterable has exactly the shape a first park writes
/// now (the completed set and the frontier, nothing else) — so an instance an earlier engine
/// parked, even with the start event listed as completed, resumes into the loop correctly.
#[tokio::test]
async fn an_instance_parked_by_an_earlier_engine_resumes_into_the_loop() {
    let prompts = Rc::new(Cell::new(1));
    let executor =
        feel_executor(TaskRegistry::new().register("prompt", counter_task(&prompts, "turn")));
    let legacy = Parked {
        waiting: vec!["W".to_string()],
        completed: vec!["S".to_string(), "Prompt".to_string()],
        variables: vars(&[("turn", num(1))]),
        fresh: Vec::new(),
        timers: Vec::new(),
    };

    let at = parked(relay(&executor, TURN_LOOP, &legacy, "W", vars(&[])).await);
    assert_eq!(at.waiting, ["W"]);
    assert_eq!(at.fresh, ["W"]);
    assert_eq!(prompts.get(), 2, "the second turn's prompt ran");
    let at = parked(relay(&executor, TURN_LOOP, &at, "W", vars(&[])).await);
    assert_eq!(prompts.get(), 3);
    assert!(relay(&executor, TURN_LOOP, &at, "W", vars(&[]))
        .await
        .is_completed());
}

// ---- a userTask sent back for rework ---------------------------------------------------------

/// S → Draft → Review (userTask) → G: `approved` ends, anything else goes back to Draft.
const REWORK: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Draft" implementation="${draft}"/>
    <bpmn:userTask id="Review">
      <bpmn:extensionElements><q:source channel="review-in" name="review"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:exclusiveGateway id="G" default="rework"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Draft"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Draft" targetRef="Review"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Review" targetRef="G"/>
    <bpmn:sequenceFlow id="accept" sourceRef="G" targetRef="E">
      <bpmn:conditionExpression>approved = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="rework" sourceRef="G" targetRef="Draft"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn a_user_task_sent_back_for_rework_waits_again_after_each_redraft() {
    let drafts = Rc::new(Cell::new(0));
    let executor =
        feel_executor(TaskRegistry::new().register("draft", counter_task(&drafts, "version")));

    let mut at = parked(start(&executor, REWORK).await);
    for round in 2..=3 {
        at = parked(
            relay(
                &executor,
                REWORK,
                &at,
                "Review",
                vars(&[("approved", boolean(false))]),
            )
            .await,
        );
        assert_eq!(at.waiting, ["Review"]);
        assert_eq!(at.fresh, ["Review"]);
        assert_eq!(drafts.get(), round, "a rejected review re-runs the draft");
        assert_eq!(at.variables.get("version"), Some(&num(round)));
    }
    let done = relay(
        &executor,
        REWORK,
        &at,
        "Review",
        vars(&[("approved", boolean(true))]),
    )
    .await;
    assert!(done.is_completed(), "an approval leaves the loop");
    assert_eq!(drafts.get(), 3);
}

// ---- a timer catch in a loop -----------------------------------------------------------------

/// S → Tick → T (timer catch, PT10S) → G: `ticks < 3` loops back to Tick, else E.
const POLL: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Tick" implementation="${tick}"/>
    <bpmn:intermediateCatchEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT10S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="done"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Tick"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Tick" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Tick">
      <bpmn:conditionExpression>ticks &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="done" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn a_timer_catch_in_a_loop_arms_a_fresh_timer_on_every_iteration() {
    let ticks = Rc::new(Cell::new(0));
    let executor =
        feel_executor(TaskRegistry::new().register("tick", counter_task(&ticks, "ticks")));

    let mut at = parked(start(&executor, POLL).await);
    assert_eq!(at.timers, ["T"]);
    for tick in 2..=3 {
        at = parked(fire(&executor, POLL, &at, "T").await);
        assert_eq!(at.waiting, ["T"]);
        assert_eq!(at.fresh, ["T"]);
        assert_eq!(
            at.timers,
            ["T"],
            "tick {tick}: the spent timer is re-armed, never reused"
        );
        assert_eq!(ticks.get(), tick);
    }
    assert!(fire(&executor, POLL, &at, "T").await.is_completed());
    assert_eq!(ticks.get(), 3);
}

// ---- a channel-call in a loop ----------------------------------------------------------------

/// S → Call (channel-call, 30s timeout) → G: `done = true` ends, else Call again.
const CALL_LOOP: &str = r#"<?xml version="1.0"?>
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
    <bpmn:exclusiveGateway id="G" default="poll"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Call"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Call" targetRef="G"/>
    <bpmn:sequenceFlow id="finish" sourceRef="G" targetRef="E">
      <bpmn:conditionExpression>done = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="poll" sourceRef="G" targetRef="Call"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn a_channel_call_in_a_loop_sends_a_new_request_and_timeout_each_iteration() {
    let sink = Rc::new(CollectingSink::new());
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
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
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_outbound_channels(outbound)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build();
    let process = proc(CALL_LOOP, "p1");

    let first = executor
        .execute_stateful_from(
            &process,
            vars(&[("e2eId", string("A-1"))]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let mut at = parked(first);
    assert_eq!(sink.len(), 1);
    assert_eq!(at.timers, ["Call#timeout"]);

    for iteration in 2..=3 {
        at = parked(
            relay(
                &executor,
                CALL_LOOP,
                &at,
                "Call",
                vars(&[("done", boolean(false))]),
            )
            .await,
        );
        assert_eq!(at.waiting, ["Call"]);
        assert_eq!(at.fresh, ["Call"], "iteration {iteration} is a new call");
        assert_eq!(
            sink.len(),
            iteration as usize,
            "each iteration sends its own request"
        );
        assert_eq!(at.timers, ["Call#timeout"], "and arms its own timeout");
    }
    let done = relay(
        &executor,
        CALL_LOOP,
        &at,
        "Call",
        vars(&[("done", boolean(true))]),
    )
    .await;
    assert!(done.is_completed());
    assert_eq!(sink.len(), 3, "the final answer sends nothing more");
}

// ---- an event-based gateway in a loop --------------------------------------------------------

/// S → EBG; arm M (message) → Count → back to EBG; arm T (timer, PT1H) → E.
const RACE_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:eventBasedGateway id="EBG"/>
    <bpmn:intermediateCatchEvent id="M">
      <bpmn:extensionElements><q:source channel="ping-in" name="ping"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:serviceTask id="Count" implementation="${count}"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="EBG"/>
    <bpmn:sequenceFlow id="toM" sourceRef="EBG" targetRef="M"/>
    <bpmn:sequenceFlow id="toT" sourceRef="EBG" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="M" targetRef="Count"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Count" targetRef="EBG"/>
    <bpmn:sequenceFlow id="f4" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn an_event_based_gateway_in_a_loop_races_again_after_each_message() {
    let pings = Rc::new(Cell::new(0));
    let executor =
        feel_executor(TaskRegistry::new().register("count", counter_task(&pings, "pings")));

    let mut at = parked(start(&executor, RACE_LOOP).await);
    assert_eq!(at.waiting, ["M", "T"]);
    assert_eq!(at.timers, ["T"]);
    for ping in 1..=2 {
        at = parked(relay(&executor, RACE_LOOP, &at, "M", vars(&[])).await);
        assert_eq!(pings.get(), ping);
        let mut waiting = at.waiting.clone();
        waiting.sort();
        assert_eq!(waiting, ["M", "T"], "the race is armed again");
        let mut fresh = at.fresh.clone();
        fresh.sort();
        assert_eq!(fresh, ["M", "T"], "both arms are new waits");
        assert_eq!(at.timers, ["T"], "the timer arm restarts its clock");
    }
    let done = fire(&executor, RACE_LOOP, &at, "T").await;
    let StatefulExecResult::Completed { outputs, .. } = done else {
        panic!("the timer arm wins and ends the instance");
    };
    assert_eq!(outputs.get("pings"), Some(&num(2)));
}

/// The message arm winning an UN-looped race retires the timer arm — the race is decided, the
/// token leaves along the winner, and nothing re-arms the loser.
#[tokio::test]
async fn a_won_race_drops_the_losing_arm_from_the_frontier() {
    const RACE_ONCE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:eventBasedGateway id="EBG"/>
    <bpmn:intermediateCatchEvent id="M">
      <bpmn:extensionElements><q:source channel="ping-in" name="ping"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:userTask id="Next">
      <bpmn:extensionElements><q:source channel="next-in" name="next"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="EBG"/>
    <bpmn:sequenceFlow id="toM" sourceRef="EBG" targetRef="M"/>
    <bpmn:sequenceFlow id="toT" sourceRef="EBG" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="M" targetRef="Next"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Next" targetRef="E"/>
    <bpmn:sequenceFlow id="f4" sourceRef="T" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let executor = feel_executor(TaskRegistry::new());
    let at = parked(start(&executor, RACE_ONCE).await);
    let at = parked(relay(&executor, RACE_ONCE, &at, "M", vars(&[])).await);
    assert_eq!(at.waiting, ["Next"], "the timer arm lost the race");
    assert!(at.timers.is_empty());
    let done = relay(&executor, RACE_ONCE, &at, "Next", vars(&[])).await;
    let StatefulExecResult::Completed { visited_nodes, .. } = done else {
        panic!("the winner's path completes");
    };
    assert!(visited_nodes.contains("E") && !visited_nodes.contains("E2"));
}

// ---- a parallel join inside a loop -----------------------------------------------------------

/// S → M (merge) → F (fork) → {A → J, W (message) → J}; J → G: `rounds < 2` back to M, else E.
const JOIN_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:exclusiveGateway id="M"/>
    <bpmn:parallelGateway id="F"/>
    <bpmn:serviceTask id="A" implementation="${work}"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements><q:source channel="w-in" name="w"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:parallelGateway id="J"/>
    <bpmn:exclusiveGateway id="G" default="out"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="M"/>
    <bpmn:sequenceFlow id="fm" sourceRef="M" targetRef="F"/>
    <bpmn:sequenceFlow id="fa" sourceRef="F" targetRef="A"/>
    <bpmn:sequenceFlow id="fw" sourceRef="F" targetRef="W"/>
    <bpmn:sequenceFlow id="ja" sourceRef="A" targetRef="J"/>
    <bpmn:sequenceFlow id="jw" sourceRef="W" targetRef="J"/>
    <bpmn:sequenceFlow id="fg" sourceRef="J" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="M">
      <bpmn:conditionExpression>rounds &lt; 2</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="out" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[tokio::test]
async fn a_parallel_join_inside_a_loop_waits_for_a_full_set_each_round() {
    let rounds = Rc::new(Cell::new(0));
    let executor =
        feel_executor(TaskRegistry::new().register("work", counter_task(&rounds, "rounds")));

    let at = parked(start(&executor, JOIN_LOOP).await);
    assert_eq!(at.waiting, ["W"]);
    // The relay completes the join (A's arrival is rebuilt from the completed set), the loop
    // forks again: A runs a second time and W parks a new wait.
    let at = parked(relay(&executor, JOIN_LOOP, &at, "W", vars(&[])).await);
    assert_eq!(rounds.get(), 2);
    assert_eq!(at.waiting, ["W"]);
    assert_eq!(at.fresh, ["W"]);
    let done = relay(&executor, JOIN_LOOP, &at, "W", vars(&[])).await;
    assert!(
        done.is_completed(),
        "the second round's join fires and the loop exits"
    );
    assert_eq!(rounds.get(), 2, "A ran once per round, never replayed");
}

/// A parked instance whose OTHER branch is still waiting keeps that wait exactly as it was when a
/// relay resumes this one: carried over, not re-parked fresh, its pending timer not re-armed.
#[tokio::test]
async fn a_relay_carries_the_other_branches_waits_over_untouched() {
    let sink = Rc::new(CollectingSink::new());
    let mut outbound = OutboundChannelRegistry::new();
    outbound.register(
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
    const SPLIT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:parallelGateway id="F"/>
    <bpmn:userTask id="U">
      <bpmn:extensionElements><q:source channel="u-in" name="u"/></bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="Call" implementation="channel:out">
      <bpmn:extensionElements>
        <q:timeout duration="PT30S"/>
        <q:alias name="callKey" expression="e2eId"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:parallelGateway id="J"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="F"/>
    <bpmn:sequenceFlow id="fu" sourceRef="F" targetRef="U"/>
    <bpmn:sequenceFlow id="fc" sourceRef="F" targetRef="Call"/>
    <bpmn:sequenceFlow id="ju" sourceRef="U" targetRef="J"/>
    <bpmn:sequenceFlow id="jc" sourceRef="Call" targetRef="J"/>
    <bpmn:sequenceFlow id="fe" sourceRef="J" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_outbound_channels(outbound)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
        .build();
    let first = executor
        .execute_stateful_from(
            &proc(SPLIT, "p1"),
            vars(&[("e2eId", string("A-1"))]),
            dep(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let at = parked(first);
    assert_eq!(sink.len(), 1);

    let at = parked(relay(&executor, SPLIT, &at, "U", vars(&[])).await);
    assert_eq!(at.waiting, ["Call"], "the call is still the frontier");
    assert!(at.fresh.is_empty(), "…carried over, not a new wait");
    assert!(
        at.timers.is_empty(),
        "its timeout keeps its original due-at"
    );
    assert_eq!(sink.len(), 1, "no request re-sent");

    let done = relay(&executor, SPLIT, &at, "Call", vars(&[])).await;
    assert!(
        done.is_completed(),
        "the join fires on the call's answer (U's arrival rebuilt)"
    );
}

// ---- a `<q:retry>` task inside a loop ----------------------------------------------------------

/// S → Work (`<q:retry>`) → W (message catch) → G: `n < 2` back to Work, else E.
const RETRY_LOOP: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Work" implementation="work">
      <bpmn:extensionElements><q:retry maxAttempts="3"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements><q:source channel="w-in" name="w"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="out"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Work"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Work" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Work">
      <bpmn:conditionExpression>n &lt; 2</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="out" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A retry park in a LATER iteration of a loop re-drives the task, even though the task is in
/// the completed set from an earlier iteration — and the attempt budget is the iteration's own.
#[tokio::test]
async fn a_retry_backoff_in_a_later_iteration_re_drives_the_task() {
    let calls = Rc::new(Cell::new(0));
    let counter = Rc::clone(&calls);
    // Succeeds on call 1 (iteration 1), fails on call 2 (iteration 2), succeeds on call 3.
    let registry = TaskRegistry::new().register("work", move |_, _| {
        counter.set(counter.get() + 1);
        if counter.get() == 2 {
            Err(sutra_executor::TaskError::Failed(
                "upstream unavailable".to_string(),
            ))
        } else {
            ok_map(&[("n", num(counter.get()))])
        }
    });
    let executor = feel_executor(registry);
    let process = proc(RETRY_LOOP, "p1");

    let at = parked(start(&executor, RETRY_LOOP).await);
    let looped = relay(&executor, RETRY_LOOP, &at, "W", vars(&[])).await;
    let StatefulExecResult::Suspended {
        waiting_nodes,
        completed_nodes,
        variables,
        retry_attempts,
        ..
    } = looped
    else {
        panic!("iteration 2's failure parks on a backoff");
    };
    assert_eq!(waiting_nodes, ["Work"]);
    assert!(
        completed_nodes.contains(&"Work".to_string()),
        "iteration 1 completed it"
    );
    assert_eq!(
        retry_attempts.get("Work"),
        Some(&1),
        "iteration 2's own first failure"
    );

    // The backoff timer comes due: the poller resumes the task node itself.
    let redriven = executor
        .resume(
            &process,
            INSTANCE,
            &completed_nodes,
            variables,
            "Work",
            &Variables::new(),
            dep(),
            BTreeMap::new(),
            None,
            &waiting_nodes,
            &BTreeMap::new(),
            &retry_attempts,
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        calls.get(),
        3,
        "the re-drive ran the task again — it did not skip it"
    );
    let StatefulExecResult::Suspended {
        waiting_nodes,
        retry_attempts,
        ..
    } = redriven
    else {
        panic!("the re-driven task succeeds and the loop parks at W");
    };
    assert_eq!(waiting_nodes, ["W"]);
    assert!(
        retry_attempts.is_empty(),
        "the success cleared the iteration's budget"
    );
}

// ---- what a resume reports to the listeners (the audit journal's source) ---------------------

/// Records the token events, in order.
#[derive(Default)]
struct TokenRecorder {
    events: std::cell::RefCell<Vec<String>>,
}

impl sutra_executor::listener::ExecutionListener for TokenRecorder {
    fn on_token_entered(&self, e: &sutra_executor::listener::TokenEvent) {
        self.events
            .borrow_mut()
            .push(format!("entered:{}", e.node_id));
    }
    fn on_token_left(&self, e: &sutra_executor::listener::TokenEvent) {
        self.events.borrow_mut().push(format!("left:{}", e.node_id));
    }
}

/// S → Count → G: `turn < 3` → W (message catch) → T (timer catch) → back to Count, else E.
/// The gateway sits in FRONT of the waits, as in a review loop, so a resume's reconstruction
/// walk passes it on the way to the satisfied wait.
const GATE_BEFORE_WAIT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Count" implementation="${count}"/>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements><q:source channel="turn-in" name="revision"/></bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="T">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT2S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Count"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Count" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="W">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f4" sourceRef="W" targetRef="T"/>
    <bpmn:sequenceFlow id="f5" sourceRef="T" targetRef="Count"/>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A resume reports the wait it came through — `left:<wait>` — and never the gateway the
/// reconstruction walk re-evaluated on the way there.
#[tokio::test]
async fn a_resume_reports_the_satisfied_wait_not_the_gateway_in_front_of_it() {
    let counts = Rc::new(Cell::new(0));
    let recorder = Rc::new(TokenRecorder::default());
    let executor = TokenExecutor::builder(
        TaskRegistry::new().register("count", counter_task(&counts, "turn")),
    )
    .with_feel()
    .with_now_supplier(|| "2026-01-01T00:00:00Z".to_string())
    .with_listener(Rc::clone(&recorder) as Rc<dyn sutra_executor::listener::ExecutionListener>)
    .build();

    let at = parked(start(&executor, GATE_BEFORE_WAIT).await);
    assert_eq!(at.waiting, ["W"]);
    recorder.events.borrow_mut().clear();

    let at = parked(relay(&executor, GATE_BEFORE_WAIT, &at, "W", vars(&[])).await);
    assert_eq!(at.waiting, ["T"]);
    assert_eq!(
        *recorder.events.borrow(),
        ["left:W", "entered:T"],
        "the relay resume leaves W and parks at T"
    );
    recorder.events.borrow_mut().clear();

    let at = parked(fire(&executor, GATE_BEFORE_WAIT, &at, "T").await);
    assert_eq!(at.waiting, ["W"]);
    assert_eq!(
        *recorder.events.borrow(),
        [
            "left:T",
            "entered:Count",
            "left:Count",
            "entered:G",
            "left:G",
            "entered:W"
        ],
        "the timer resume leaves T and runs the loop body once"
    );
    assert_eq!(counts.get(), 2);
}
