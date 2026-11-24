//! The audit plan of a process: for every element, whether it is audited and why. Computed once
//! when the process is built, from its `<q:audit>` declaration, its node-level opt-outs and the
//! audit-point rule ([`crate::audit_points`]). The executor reads it to decide which events a node
//! emits, and `sutra lint --audit` prints it, so what the tooling shows is what runs.

use std::collections::BTreeMap;

use crate::audit_points::audit_point_rule;
use crate::model::{Node, ProcessDefinition};
use crate::qbindings::{AuditCapture, AuditEventScope};

/// What one element of a process records.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ElementAuditDisposition {
    /// Audited by the process's declaration: the element records `NODE_LEFT`, and also
    /// `NODE_ENTERED` when `entered` (every node under `events="all"`, a wait node under
    /// `events="points"`). `rule` says which rule made it audited.
    Desugared { rule: &'static str, entered: bool },
    /// The node's own `<q:audit capture="none">` opts it out.
    OptedOut,
    /// Not audited, and why.
    NotAudited { reason: &'static str },
}

/// The audit plan of one process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPlan {
    pub process_id: String,
    /// The process-level declaration (`events`, `capture`); `None` = no `<q:audit>`.
    pub declared: Option<(AuditEventScope, AuditCapture)>,
    /// Every element of the process, embedded sub-processes' included, by id.
    pub elements: BTreeMap<String, ElementAuditDisposition>,
}

const NOT_DECLARED: &str = "process declares no q:audit";
const CAPTURE_NONE: &str = "process q:audit capture=none";
const SCOPE_INSTANCE: &str = "events=instance";
const SCOPE_ALL: &str = "events=all";

impl AuditPlan {
    /// The plan of `process`, from its declaration and its nodes.
    pub fn build(process: &ProcessDefinition) -> AuditPlan {
        let declared = process.audit.as_ref().map(|a| (a.events, a.capture));
        let mut elements = BTreeMap::new();
        collect(process, declared, &mut elements);
        AuditPlan {
            process_id: process.id.clone(),
            declared,
            elements,
        }
    }

    /// True when the process records its instance lifecycle: it declares `<q:audit>` and its
    /// capture is not `none`.
    pub fn audits_instance(&self) -> bool {
        matches!(self.declared, Some((_, capture)) if capture != AuditCapture::None)
    }

    /// The disposition of one element, if the process has it.
    pub fn disposition(&self, node_id: &str) -> Option<&ElementAuditDisposition> {
        self.elements.get(node_id)
    }

    /// The effective values in one line, as lint and docgen print them: `points · metadata`, or
    /// `not audited`.
    pub fn pool_line(&self) -> String {
        match self.declared {
            Some((events, capture)) => format!("{} · {}", events.as_str(), capture.as_str()),
            None => "not audited".to_string(),
        }
    }

    /// The plan as JSON — the `sutra lint --audit --format json` shape.
    pub fn to_json(&self) -> serde_json::Value {
        let elements: Vec<serde_json::Value> = self
            .elements
            .iter()
            .map(|(id, disposition)| match disposition {
                ElementAuditDisposition::Desugared { rule, entered } => serde_json::json!({
                    "id": id,
                    "disposition": "desugared",
                    "rule": rule,
                    "entered": entered,
                }),
                ElementAuditDisposition::OptedOut => serde_json::json!({
                    "id": id,
                    "disposition": "optedOut",
                    "rule": "node q:audit capture=none",
                }),
                ElementAuditDisposition::NotAudited { reason } => serde_json::json!({
                    "id": id,
                    "disposition": "notAudited",
                    "rule": reason,
                }),
            })
            .collect();
        let declared = match self.declared {
            Some((events, capture)) => serde_json::json!({
                "events": events.as_str(),
                "capture": capture.as_str(),
            }),
            None => serde_json::Value::Null,
        };
        serde_json::json!({
            "processId": self.process_id,
            "declared": declared,
            "pool": self.pool_line(),
            "elements": elements,
        })
    }
}

fn collect(
    process: &ProcessDefinition,
    declared: Option<(AuditEventScope, AuditCapture)>,
    out: &mut BTreeMap<String, ElementAuditDisposition>,
) {
    for node in process.nodes() {
        out.insert(
            node.id().to_string(),
            disposition_of(process, node, declared),
        );
        if let Some(inner) = inner_definition(node) {
            collect(inner, declared, out);
        }
    }
}

fn disposition_of(
    process: &ProcessDefinition,
    node: &Node,
    declared: Option<(AuditEventScope, AuditCapture)>,
) -> ElementAuditDisposition {
    let Some((events, capture)) = declared else {
        return ElementAuditDisposition::NotAudited {
            reason: NOT_DECLARED,
        };
    };
    if capture == AuditCapture::None {
        return ElementAuditDisposition::NotAudited {
            reason: CAPTURE_NONE,
        };
    }
    let bindings = process.bindings_for(node.id());
    if bindings.audit.as_ref().map(|a| a.capture) == Some(AuditCapture::None) {
        return ElementAuditDisposition::OptedOut;
    }
    match events {
        AuditEventScope::All => ElementAuditDisposition::Desugared {
            rule: SCOPE_ALL,
            entered: true,
        },
        AuditEventScope::Instance => ElementAuditDisposition::NotAudited {
            reason: SCOPE_INSTANCE,
        },
        AuditEventScope::Points => {
            let rule = audit_point_rule(node, bindings);
            if rule.is_point {
                ElementAuditDisposition::Desugared {
                    rule: rule.rule,
                    entered: node.is_wait_state(),
                }
            } else {
                ElementAuditDisposition::NotAudited { reason: rule.rule }
            }
        }
    }
}

/// The process a node contains, for the node kinds that embed one (a loop wrapper is looked
/// through to the activity it wraps).
pub(crate) fn inner_definition(node: &Node) -> Option<&ProcessDefinition> {
    match node {
        Node::SubProcess { inner, .. }
        | Node::TransactionSubProcess { inner, .. }
        | Node::AdHocSubProcess { inner, .. }
        | Node::EventSubProcess { inner, .. } => Some(inner),
        Node::MultiInstance { inner, .. } | Node::StandardLoop { inner, .. } => {
            inner_definition(inner)
        }
        _ => None,
    }
}
