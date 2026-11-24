//! The audit-point rule: one classification per `Node` variant, exactly as the `events="points"`
//! scope documents it (`xsd/q.xsd#AuditEventScope`).

use std::collections::{HashMap, HashSet};

use sutra_bpmn::model::{BoundaryKind, DataMapping, ThrowKind};
use sutra_bpmn::qbindings::NodeBindings;
use sutra_bpmn::{audit_point_rule, is_audit_point, Node, ProcessDefinition, TimerDefinition};

fn id(s: &str) -> String {
    s.to_string()
}

fn empty_process() -> Box<ProcessDefinition> {
    Box::new(
        ProcessDefinition::of(
            "inner",
            None,
            true,
            "1.0",
            vec![],
            vec![],
            HashMap::new(),
            vec![],
        )
        .expect("an empty inner process builds"),
    )
}

fn service(implementation: &str) -> Node {
    Node::ServiceTask {
        id: id("n"),
        name: None,
        implementation: id(implementation),
        data_mapping: DataMapping::default(),
        params: vec![],
        stream: false,
    }
}

fn timer() -> TimerDefinition {
    TimerDefinition::from_persisted("DURATION", "PT1S").expect("PT1S is a valid duration")
}

/// One node per `Node` variant, with whether it is a point when unbound.
fn every_variant() -> Vec<(Node, bool)> {
    let n = || id("n");
    vec![
        (
            Node::StartEvent {
                id: n(),
                name: None,
                channels: vec![],
                timer: None,
            },
            true,
        ),
        (
            Node::EndEvent {
                id: n(),
                name: None,
            },
            true,
        ),
        (
            Node::TerminateEndEvent {
                id: n(),
                name: None,
            },
            true,
        ),
        (
            Node::ErrorEvent {
                id: n(),
                name: None,
                error_code: None,
            },
            true,
        ),
        (
            Node::CancelEndEvent {
                id: n(),
                name: None,
            },
            true,
        ),
        (
            Node::IntermediateThrowEvent {
                id: n(),
                name: None,
                kind: ThrowKind::Escalation,
                activity_ref: None,
                reference: None,
            },
            true,
        ),
        (
            Node::LinkCatchEvent {
                id: n(),
                name: None,
                link_name: id("l"),
            },
            true,
        ),
        (
            Node::MessageCatchEvent {
                id: n(),
                name: None,
                channels: vec![],
                message_ref: None,
            },
            true,
        ),
        (
            Node::TimerCatchEvent {
                id: n(),
                name: None,
                timer: timer(),
            },
            true,
        ),
        (
            Node::BoundaryEvent {
                id: n(),
                name: None,
                attached_to_ref: id("host"),
                kind: BoundaryKind::Error,
                error_code: None,
                escalation_code: None,
                interrupting: true,
                timer: None,
            },
            true,
        ),
        (
            Node::ReceiveTask {
                id: n(),
                name: None,
                channels: vec![],
                message_ref: None,
            },
            true,
        ),
        (
            Node::SendTask {
                id: n(),
                name: None,
            },
            true,
        ),
        (
            Node::UserTask {
                id: n(),
                name: None,
                channels: vec![],
            },
            true,
        ),
        (
            Node::EventBasedGateway {
                id: n(),
                name: None,
            },
            true,
        ),
        (
            Node::CallActivity {
                id: n(),
                name: None,
                called_element: id("other"),
                called_namespace: None,
            },
            true,
        ),
        (
            Node::SubProcess {
                id: n(),
                name: None,
                inner: empty_process(),
            },
            true,
        ),
        (
            Node::TransactionSubProcess {
                id: n(),
                name: None,
                inner: empty_process(),
            },
            true,
        ),
        (
            Node::AdHocSubProcess {
                id: n(),
                name: None,
                inner: empty_process(),
                completion_condition: None,
                parallel: false,
            },
            true,
        ),
        (
            Node::EventSubProcess {
                id: n(),
                name: None,
                inner: empty_process(),
                error_code: None,
                interrupting: true,
            },
            true,
        ),
        (service("some-task"), false),
        (
            Node::DataTask {
                id: n(),
                name: None,
                data_mapping: DataMapping::default(),
            },
            false,
        ),
        (
            Node::ScriptTask {
                id: n(),
                name: None,
                script_file: id("s.srl"),
            },
            false,
        ),
        (
            Node::ManualTask {
                id: n(),
                name: None,
                untyped: true,
            },
            false,
        ),
        (
            Node::BusinessRuleTask {
                id: n(),
                name: None,
                decision_file: id("d.dmn"),
            },
            false,
        ),
        (
            Node::ExclusiveGateway {
                id: n(),
                name: None,
                default_flow_id: None,
            },
            false,
        ),
        (
            Node::InclusiveGateway {
                id: n(),
                name: None,
                default_flow_id: None,
            },
            false,
        ),
        (
            Node::ParallelGateway {
                id: n(),
                name: None,
            },
            false,
        ),
        (
            Node::ComplexGateway {
                id: n(),
                name: None,
                default_flow_id: None,
                activation_condition: None,
            },
            false,
        ),
        (
            Node::MultiInstance {
                id: n(),
                name: None,
                inner: Box::new(Node::UserTask {
                    id: n(),
                    name: None,
                    channels: vec![],
                }),
                sequential: true,
                loop_cardinality: None,
                loop_data_input_ref: None,
                input_data_item: None,
                completion_condition: None,
            },
            true,
        ),
        (
            Node::StandardLoop {
                id: n(),
                name: None,
                inner: Box::new(service("some-task")),
                loop_condition: None,
                test_before: false,
                loop_maximum: None,
            },
            false,
        ),
    ]
}

#[test]
fn every_variant_classified() {
    let unbound = NodeBindings::default();
    let cases = every_variant();
    let variants: HashSet<_> = cases
        .iter()
        .map(|(n, _)| std::mem::discriminant(n))
        .collect();
    assert_eq!(variants.len(), cases.len(), "one fixture per variant");
    assert_eq!(
        cases.len(),
        30,
        "a Node variant was added: give it a fixture here"
    );
    for (node, expected) in &cases {
        let rule = audit_point_rule(node, &unbound);
        assert_eq!(rule.is_point, *expected, "{node:?}: {}", rule.rule);
        assert_eq!(is_audit_point(node, &unbound), *expected);
        assert!(!rule.rule.is_empty());
        if !expected {
            assert!(rule.rule.contains("not a point"), "{}", rule.rule);
        }
    }
    // Rules read as the documentation does.
    assert_eq!(
        audit_point_rule(&cases[10].0, &unbound).rule,
        "receive task"
    );
    assert_eq!(
        audit_point_rule(&cases[24].0, &unbound).rule,
        "exclusive gateway (not a point)"
    );
}

/// A channel-call service task sends a request and waits for the answer: it is bound, so a point.
#[test]
fn bound_service_task_is_a_point() {
    let unbound = NodeBindings::default();
    assert!(!is_audit_point(&service("some-task"), &unbound));
    assert!(is_audit_point(&service("channel:acquirer"), &unbound));
}
