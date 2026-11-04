//! The engine's loaded processes as the diagram crate's neutral model. Docs reflect what the engine
//! loads, so the diagram draws from [`sutra_bpmn`]'s model rather than from the crate's own parser.

use bpmn_svg::{
    EventGlyph, FlowNode, GatewayType, LoopMarker, Model, NodeKind, Process, SequenceFlow,
    SubProcessKind, TaskType,
};
use sutra_bpmn::{Node, ProcessDefinition};

/// Map a module's processes, in order.
pub fn to_diagram_model(processes: &[ProcessDefinition]) -> Model {
    Model {
        processes: processes.iter().map(map_process).collect(),
    }
}

fn map_process(p: &ProcessDefinition) -> Process {
    Process {
        id: p.id.clone(),
        nodes: p.nodes().iter().map(map_node).collect(),
        flows: p
            .flows()
            .iter()
            .map(|f| SequenceFlow {
                id: f.id.clone(),
                source_ref: f.source_ref.clone(),
                target_ref: f.target_ref.clone(),
            })
            .collect(),
    }
}

fn node(id: &str, name: &Option<String>, kind: NodeKind) -> FlowNode {
    FlowNode {
        id: id.to_string(),
        label: name.clone(),
        kind,
        markers: Vec::new(),
    }
}

fn sub_process(kind: SubProcessKind, inner: &ProcessDefinition) -> NodeKind {
    NodeKind::SubProcess {
        kind,
        inner: map_process(inner),
    }
}

fn map_node(n: &Node) -> FlowNode {
    let id = n.id();
    match n {
        Node::StartEvent { name, timer, .. } => {
            let glyph = if timer.is_some() {
                EventGlyph::Timer
            } else {
                EventGlyph::Message
            };
            node(id, name, NodeKind::StartEvent(glyph))
        }
        Node::EndEvent { name, .. } => node(id, name, NodeKind::EndEvent(None)),
        Node::TerminateEndEvent { name, .. } => {
            node(id, name, NodeKind::EndEvent(Some(EventGlyph::Terminate)))
        }
        Node::ErrorEvent { name, .. } => {
            node(id, name, NodeKind::EndEvent(Some(EventGlyph::Error)))
        }
        Node::CancelEndEvent { name, .. } => {
            node(id, name, NodeKind::EndEvent(Some(EventGlyph::Cancel)))
        }
        Node::IntermediateThrowEvent { name, kind, .. } => {
            // The kind enums are not exported by the loader, so they are read by name.
            let glyph = match format!("{kind:?}").as_str() {
                k if k.starts_with("Signal") => EventGlyph::Signal,
                k if k.starts_with("Escalation") => EventGlyph::Escalation,
                k if k.starts_with("Link") => EventGlyph::Link,
                k if k.starts_with("Compensat") => EventGlyph::Compensate,
                _ => EventGlyph::Message,
            };
            node(id, name, NodeKind::IntermediateThrowEvent(glyph))
        }
        Node::LinkCatchEvent { name, .. } => {
            node(id, name, NodeKind::IntermediateCatchEvent(EventGlyph::Link))
        }
        Node::MessageCatchEvent { name, .. } => node(
            id,
            name,
            NodeKind::IntermediateCatchEvent(EventGlyph::Message),
        ),
        Node::TimerCatchEvent { name, .. } => node(
            id,
            name,
            NodeKind::IntermediateCatchEvent(EventGlyph::Timer),
        ),
        Node::BoundaryEvent {
            name,
            kind,
            attached_to_ref,
            ..
        } => {
            let glyph = match format!("{kind:?}").as_str() {
                k if k.starts_with("Timer") => EventGlyph::Timer,
                k if k.starts_with("Escalation") => EventGlyph::Escalation,
                k if k.starts_with("Cancel") => EventGlyph::Cancel,
                k if k.starts_with("Compensat") => EventGlyph::Compensate,
                _ => EventGlyph::Error,
            };
            node(
                id,
                name,
                NodeKind::BoundaryEvent {
                    attached_to_ref: attached_to_ref.clone(),
                    glyph,
                },
            )
        }
        Node::ServiceTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Service)),
        // A data task is an engine extension of a service task; BPMN has no glyph for it.
        Node::DataTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Plain)),
        Node::ScriptTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Script)),
        Node::ManualTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Manual)),
        Node::SendTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Send)),
        Node::BusinessRuleTask { name, .. } => {
            node(id, name, NodeKind::Task(TaskType::BusinessRule))
        }
        Node::UserTask { name, .. } => node(id, name, NodeKind::Task(TaskType::User)),
        Node::ReceiveTask { name, .. } => node(id, name, NodeKind::Task(TaskType::Receive)),
        Node::CallActivity { name, .. } => node(id, name, NodeKind::CallActivity),
        Node::SubProcess { name, inner, .. } => {
            node(id, name, sub_process(SubProcessKind::Embedded, inner))
        }
        Node::TransactionSubProcess { name, inner, .. } => {
            node(id, name, sub_process(SubProcessKind::Transaction, inner))
        }
        Node::AdHocSubProcess { name, inner, .. } => {
            node(id, name, sub_process(SubProcessKind::AdHoc, inner))
        }
        Node::EventSubProcess { name, inner, .. } => {
            node(id, name, sub_process(SubProcessKind::Event, inner))
        }
        Node::ExclusiveGateway { name, .. } => {
            node(id, name, NodeKind::Gateway(GatewayType::Exclusive))
        }
        Node::EventBasedGateway { name, .. } => {
            node(id, name, NodeKind::Gateway(GatewayType::EventBased))
        }
        Node::InclusiveGateway { name, .. } => {
            node(id, name, NodeKind::Gateway(GatewayType::Inclusive))
        }
        Node::ParallelGateway { name, .. } => {
            node(id, name, NodeKind::Gateway(GatewayType::Parallel))
        }
        Node::ComplexGateway { name, .. } => {
            node(id, name, NodeKind::Gateway(GatewayType::Complex))
        }
        // A loop wrapper decorates the activity it wraps: map the inner node, add the marker.
        Node::MultiInstance {
            inner, sequential, ..
        } => {
            let mut mapped = map_node(inner);
            mapped.markers.push(if *sequential {
                LoopMarker::SequentialMultiInstance
            } else {
                LoopMarker::ParallelMultiInstance
            });
            mapped
        }
        Node::StandardLoop { inner, .. } => {
            let mut mapped = map_node(inner);
            mapped.markers.push(LoopMarker::Loop);
            mapped
        }
    }
}
