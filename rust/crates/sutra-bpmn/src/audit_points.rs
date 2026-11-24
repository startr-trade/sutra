//! Which nodes are AUDIT POINTS — the nodes a process audited with `events="points"` records.
//!
//! A point is a node where something observable happens: an event, a wait, a message exchange or
//! a call. Routing (gateways that only choose a path) and local computation (scripts, plain
//! tasks, rule evaluation) are not points. The classification is one exhaustive `match` on
//! [`Node`] with no catch-all arm, so a new node kind does not build until it is classified here.
//! [`audit_point_rules_json`] renders the same table into `xsd/audit-point-rules.json`, which a
//! test keeps fresh, so the published rule cannot drift from the code.

use crate::model::{Node, CHANNEL_CALL_PREFIX};
use crate::qbindings::NodeBindings;

/// Whether a node is an audit point, and the human-readable rule that decided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditPointRule {
    pub is_point: bool,
    /// The rule in words, e.g. `"receive task"` or `"exclusive gateway (not a point)"`.
    pub rule: &'static str,
}

/// The field-less kind of a [`Node`], one per variant, for the rule table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PointKind {
    StartEvent,
    EndEvent,
    TerminateEndEvent,
    ErrorEvent,
    CancelEndEvent,
    IntermediateThrowEvent,
    LinkCatchEvent,
    MessageCatchEvent,
    TimerCatchEvent,
    BoundaryEvent,
    ReceiveTask,
    SendTask,
    UserTask,
    EventBasedGateway,
    CallActivity,
    SubProcess,
    TransactionSubProcess,
    AdHocSubProcess,
    EventSubProcess,
    ServiceTask,
    DataTask,
    ScriptTask,
    ManualTask,
    BusinessRuleTask,
    ExclusiveGateway,
    InclusiveGateway,
    ParallelGateway,
    ComplexGateway,
    MultiInstance,
    StandardLoop,
}

/// Every kind, for the rendered table. [`kind_name`] is exhaustive, and the
/// `every_variant_classified` test builds one node per [`Node`] variant, so this list cannot
/// silently miss one.
const ALL_KINDS: [PointKind; 30] = [
    PointKind::StartEvent,
    PointKind::EndEvent,
    PointKind::TerminateEndEvent,
    PointKind::ErrorEvent,
    PointKind::CancelEndEvent,
    PointKind::IntermediateThrowEvent,
    PointKind::LinkCatchEvent,
    PointKind::MessageCatchEvent,
    PointKind::TimerCatchEvent,
    PointKind::BoundaryEvent,
    PointKind::ReceiveTask,
    PointKind::SendTask,
    PointKind::UserTask,
    PointKind::EventBasedGateway,
    PointKind::CallActivity,
    PointKind::SubProcess,
    PointKind::TransactionSubProcess,
    PointKind::AdHocSubProcess,
    PointKind::EventSubProcess,
    PointKind::ServiceTask,
    PointKind::DataTask,
    PointKind::ScriptTask,
    PointKind::ManualTask,
    PointKind::BusinessRuleTask,
    PointKind::ExclusiveGateway,
    PointKind::InclusiveGateway,
    PointKind::ParallelGateway,
    PointKind::ComplexGateway,
    PointKind::MultiInstance,
    PointKind::StandardLoop,
];

fn kind_of(node: &Node) -> PointKind {
    match node {
        Node::StartEvent { .. } => PointKind::StartEvent,
        Node::EndEvent { .. } => PointKind::EndEvent,
        Node::TerminateEndEvent { .. } => PointKind::TerminateEndEvent,
        Node::ErrorEvent { .. } => PointKind::ErrorEvent,
        Node::CancelEndEvent { .. } => PointKind::CancelEndEvent,
        Node::IntermediateThrowEvent { .. } => PointKind::IntermediateThrowEvent,
        Node::LinkCatchEvent { .. } => PointKind::LinkCatchEvent,
        Node::MessageCatchEvent { .. } => PointKind::MessageCatchEvent,
        Node::TimerCatchEvent { .. } => PointKind::TimerCatchEvent,
        Node::BoundaryEvent { .. } => PointKind::BoundaryEvent,
        Node::ReceiveTask { .. } => PointKind::ReceiveTask,
        Node::SendTask { .. } => PointKind::SendTask,
        Node::UserTask { .. } => PointKind::UserTask,
        Node::EventBasedGateway { .. } => PointKind::EventBasedGateway,
        Node::CallActivity { .. } => PointKind::CallActivity,
        Node::SubProcess { .. } => PointKind::SubProcess,
        Node::TransactionSubProcess { .. } => PointKind::TransactionSubProcess,
        Node::AdHocSubProcess { .. } => PointKind::AdHocSubProcess,
        Node::EventSubProcess { .. } => PointKind::EventSubProcess,
        Node::ServiceTask { .. } => PointKind::ServiceTask,
        Node::DataTask { .. } => PointKind::DataTask,
        Node::ScriptTask { .. } => PointKind::ScriptTask,
        Node::ManualTask { .. } => PointKind::ManualTask,
        Node::BusinessRuleTask { .. } => PointKind::BusinessRuleTask,
        Node::ExclusiveGateway { .. } => PointKind::ExclusiveGateway,
        Node::InclusiveGateway { .. } => PointKind::InclusiveGateway,
        Node::ParallelGateway { .. } => PointKind::ParallelGateway,
        Node::ComplexGateway { .. } => PointKind::ComplexGateway,
        Node::MultiInstance { .. } => PointKind::MultiInstance,
        Node::StandardLoop { .. } => PointKind::StandardLoop,
    }
}

fn kind_name(kind: PointKind) -> &'static str {
    match kind {
        PointKind::StartEvent => "StartEvent",
        PointKind::EndEvent => "EndEvent",
        PointKind::TerminateEndEvent => "TerminateEndEvent",
        PointKind::ErrorEvent => "ErrorEvent",
        PointKind::CancelEndEvent => "CancelEndEvent",
        PointKind::IntermediateThrowEvent => "IntermediateThrowEvent",
        PointKind::LinkCatchEvent => "LinkCatchEvent",
        PointKind::MessageCatchEvent => "MessageCatchEvent",
        PointKind::TimerCatchEvent => "TimerCatchEvent",
        PointKind::BoundaryEvent => "BoundaryEvent",
        PointKind::ReceiveTask => "ReceiveTask",
        PointKind::SendTask => "SendTask",
        PointKind::UserTask => "UserTask",
        PointKind::EventBasedGateway => "EventBasedGateway",
        PointKind::CallActivity => "CallActivity",
        PointKind::SubProcess => "SubProcess",
        PointKind::TransactionSubProcess => "TransactionSubProcess",
        PointKind::AdHocSubProcess => "AdHocSubProcess",
        PointKind::EventSubProcess => "EventSubProcess",
        PointKind::ServiceTask => "ServiceTask",
        PointKind::DataTask => "DataTask",
        PointKind::ScriptTask => "ScriptTask",
        PointKind::ManualTask => "ManualTask",
        PointKind::BusinessRuleTask => "BusinessRuleTask",
        PointKind::ExclusiveGateway => "ExclusiveGateway",
        PointKind::InclusiveGateway => "InclusiveGateway",
        PointKind::ParallelGateway => "ParallelGateway",
        PointKind::ComplexGateway => "ComplexGateway",
        PointKind::MultiInstance => "MultiInstance",
        PointKind::StandardLoop => "StandardLoop",
    }
}

const fn point(rule: &'static str) -> AuditPointRule {
    AuditPointRule {
        is_point: true,
        rule,
    }
}

const fn not_point(rule: &'static str) -> AuditPointRule {
    AuditPointRule {
        is_point: false,
        rule,
    }
}

/// The rule for a kind. `bound` = the node is bound to a message exchange: a `q:send`, `q:reply`
/// or `q:source`, or (for a service task) a `channel:` call, which sends a request and waits for
/// its answer. A bound node is always a point.
fn kind_rule(kind: PointKind, bound: bool) -> AuditPointRule {
    let unbound = match kind {
        PointKind::StartEvent => point("start event"),
        PointKind::EndEvent => point("end event"),
        PointKind::TerminateEndEvent => point("terminate end event"),
        PointKind::ErrorEvent => point("error end event"),
        PointKind::CancelEndEvent => point("cancel end event"),
        PointKind::IntermediateThrowEvent => point("intermediate throw event"),
        PointKind::LinkCatchEvent => point("link catch event"),
        PointKind::MessageCatchEvent => point("message catch event"),
        PointKind::TimerCatchEvent => point("timer catch event"),
        PointKind::BoundaryEvent => point("boundary event"),
        PointKind::ReceiveTask => point("receive task"),
        PointKind::SendTask => point("send task"),
        PointKind::UserTask => point("user task"),
        PointKind::EventBasedGateway => point("event-based gateway"),
        PointKind::CallActivity => point("call activity"),
        PointKind::SubProcess => point("sub-process boundary"),
        PointKind::TransactionSubProcess => point("transaction sub-process boundary"),
        PointKind::AdHocSubProcess => point("ad-hoc sub-process boundary"),
        PointKind::EventSubProcess => point("event sub-process boundary"),
        PointKind::ServiceTask => not_point("unbound service task (not a point)"),
        PointKind::DataTask => not_point("data task (not a point)"),
        PointKind::ScriptTask => not_point("script task (not a point)"),
        PointKind::ManualTask => not_point("plain task (not a point)"),
        PointKind::BusinessRuleTask => not_point("business rule task (not a point)"),
        PointKind::ExclusiveGateway => not_point("exclusive gateway (not a point)"),
        PointKind::InclusiveGateway => not_point("inclusive gateway (not a point)"),
        PointKind::ParallelGateway => not_point("parallel gateway (not a point)"),
        PointKind::ComplexGateway => not_point("complex gateway (not a point)"),
        // A loop wrapper is classified by the activity it wraps; this entry is the table's.
        PointKind::MultiInstance => not_point("multi-instance: as the activity it wraps"),
        PointKind::StandardLoop => not_point("standard loop: as the activity it wraps"),
    };
    if bound && !unbound.is_point {
        return match kind {
            PointKind::ServiceTask => point("service task bound to a message exchange"),
            _ => point("node bound to q:send, q:reply or q:source"),
        };
    }
    unbound
}

fn is_bound(node: &Node, bindings: &NodeBindings) -> bool {
    let channel_call = matches!(
        node,
        Node::ServiceTask { implementation, .. } if implementation.starts_with(CHANNEL_CALL_PREFIX)
    );
    channel_call
        || bindings.send.is_some()
        || bindings.reply.is_some()
        || !bindings.sources.is_empty()
}

/// Whether `node` (with its `bindings`) is an audit point, and why.
pub fn audit_point_rule(node: &Node, bindings: &NodeBindings) -> AuditPointRule {
    match node {
        Node::MultiInstance { inner, .. } | Node::StandardLoop { inner, .. } => {
            let inner_rule = audit_point_rule(inner, bindings);
            if inner_rule.is_point || is_bound(node, bindings) {
                AuditPointRule {
                    is_point: true,
                    rule: inner_rule.rule,
                }
            } else {
                inner_rule
            }
        }
        _ => kind_rule(kind_of(node), is_bound(node, bindings)),
    }
}

/// `audit_point_rule(node, bindings).is_point`.
pub fn is_audit_point(node: &Node, bindings: &NodeBindings) -> bool {
    audit_point_rule(node, bindings).is_point
}

/// The rule table as JSON: one entry per [`Node`] variant, sorted by variant name, each with the
/// rule for the unbound node and for one bound to a message exchange. Stable bytes (the file
/// `xsd/audit-point-rules.json` is compared against it).
pub fn audit_point_rules_json() -> String {
    // Written by hand, keys in a fixed (alphabetical) order: `serde_json::Value`'s key order
    // depends on whether some other crate in the build turns on serde_json's `preserve_order`,
    // and the published file must not change with the feature set it was built under.
    let text = |s: &str| serde_json::Value::String(s.to_string()).to_string();
    let mut kinds: Vec<PointKind> = ALL_KINDS.to_vec();
    kinds.sort_by_key(|k| kind_name(*k));
    let rules: Vec<String> = kinds
        .into_iter()
        .map(|kind| {
            let unbound = kind_rule(kind, false);
            let bound = kind_rule(kind, true);
            format!(
                "    {{\n      \"isPoint\": {},\n      \"rule\": {},\n      \"variant\": {},\n      \
                 \"whenBound\": {{\n        \"isPoint\": {},\n        \"rule\": {}\n      }}\n    }}",
                unbound.is_point,
                text(unbound.rule),
                text(kind_name(kind)),
                bound.is_point,
                text(bound.rule),
            )
        })
        .collect();
    let description = "Audit points: the nodes a process audited with events=\"points\" records. \
        A point records NODE_LEFT; a wait node also records NODE_ENTERED. A node is bound when it \
        carries q:send, q:reply or q:source, or is a channel: service task. A node-level q:audit \
        capture=\"none\" opts any node out.";
    format!(
        "{{\n  \"description\": {},\n  \"rules\": [\n{}\n  ]\n}}\n",
        text(description),
        rules.join(",\n")
    )
}
