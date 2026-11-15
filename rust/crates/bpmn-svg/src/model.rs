//! The neutral model: only what a diagram uses. Conditions, timers and expressions stay out; where
//! one affects how a node draws, the model carries the resulting glyph or label.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// A BPMN file reduced to what its diagram needs.
///
/// Built with [`Model::new`] and the builder methods, or by [`crate::parse`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Model {
    /// Top-level processes in document order, drawn one above the other.
    pub processes: Vec<Process>,
    /// Pools in document order. With none, the processes stack with no pool drawn.
    pub participants: Vec<Participant>,
    /// Message flows in document order.
    pub message_flows: Vec<MessageFlow>,
}

impl Model {
    /// An empty model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a top-level process.
    #[must_use]
    pub fn process(mut self, p: Process) -> Self {
        self.processes.push(p);
        self
    }

    /// Add a participant (a pool).
    #[must_use]
    pub fn participant(mut self, p: Participant) -> Self {
        self.participants.push(p);
        self
    }

    /// Add a message flow.
    #[must_use]
    pub fn message_flow(mut self, f: MessageFlow) -> Self {
        self.message_flows.push(f);
        self
    }

    /// The element id [`crate::render`] gives each node, flow, participant and message flow when
    /// [`crate::Options::id_prefix`] is `prefix`, keyed by the BPMN id: [`crate::element_id`], with
    /// a `-2`, `-3`, … suffix on an id that collides with an earlier one after sanitising.
    /// Document order is each process's nodes, a sub-process's content right after it, then the
    /// scope's sequence flows; then participants; then message flows. Elements with an empty id
    /// get no element id.
    pub fn element_ids(&self, prefix: &str) -> BTreeMap<String, String> {
        crate::attrs::assign_element_ids(self, prefix)
    }
    /// What in the model does not draw: each dangling sequence flow (one whose source or target
    /// is not a node of its own scope), each dangling message flow (an end that is neither a flow
    /// node at any depth nor a participant), and each participant whose process reference names no
    /// process. Each is named once, with the first missing reference, source before target.
    ///
    /// Order: each process's flows followed by its sub-processes' depth-first, processes in
    /// document order; then message flows; then participants. Rendering still draws what it can.
    pub fn validate(&self) -> Vec<Warning> {
        fn scope(p: &Process, out: &mut Vec<Warning>) {
            let ids: BTreeSet<&str> = p.nodes.iter().map(|n| n.id.as_str()).collect();
            for f in &p.flows {
                if let Some(missing) = [&f.source_ref, &f.target_ref]
                    .into_iter()
                    .find(|r| !ids.contains(r.as_str()))
                {
                    out.push(Warning::DanglingFlow {
                        flow_id: f.id.clone(),
                        missing_ref: missing.clone(),
                    });
                }
            }
            for n in &p.nodes {
                if let NodeKind::SubProcess { inner, .. } = &n.kind {
                    scope(inner, out);
                }
            }
        }
        let mut out = Vec::new();
        for p in &self.processes {
            scope(p, &mut out);
        }
        let nodes = crate::attrs::node_ids(self);
        let pools: BTreeSet<&str> = self.participants.iter().map(|p| p.id.as_str()).collect();
        for f in &self.message_flows {
            if let Some(missing) = [&f.source_ref, &f.target_ref]
                .into_iter()
                .find(|r| !nodes.contains(r.as_str()) && !pools.contains(r.as_str()))
            {
                out.push(Warning::DanglingMessageFlow {
                    flow_id: f.id.clone(),
                    missing_ref: missing.clone(),
                });
            }
        }
        for part in &self.participants {
            if let Some(r) = &part.process_ref {
                if !self.processes.iter().any(|p| &p.id == r) {
                    out.push(Warning::UnknownProcessRef {
                        participant_id: part.id.clone(),
                        process_ref: r.clone(),
                    });
                }
            }
        }
        out
    }
}

/// A pool: a participant of a collaboration, drawn around the process it names or, with none, as
/// an empty black-box band.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Participant {
    /// The element's `id`.
    pub id: String,
    /// The name shown in the pool's header strip.
    pub name: Option<String>,
    /// The id of the process the pool holds. `None` draws a black-box pool.
    pub process_ref: Option<String>,
}

impl Participant {
    /// A black-box participant with no name.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            process_ref: None,
        }
    }

    /// Set the name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Name the process the pool holds.
    #[must_use]
    pub fn process_ref(mut self, id: impl Into<String>) -> Self {
        self.process_ref = Some(id.into());
        self
    }
}

/// A message flow between two pools. Each end is a flow node id, in any process, or a participant
/// id, which attaches the flow to that pool's border. A flow whose end resolves to neither is not
/// drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MessageFlow {
    /// The element's `id`.
    pub id: String,
    /// The source's id.
    pub source_ref: String,
    /// The target's id.
    pub target_ref: String,
    /// The caption, drawn instead of the id when non-blank.
    pub label: Option<String>,
}

impl MessageFlow {
    /// A message flow with no label.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            source_ref: source.into(),
            target_ref: target.into(),
            label: None,
        }
    }

    /// Set the caption.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

/// A process, or the content of a sub-process.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Process {
    /// The element's `id`. Empty when the caller has none.
    pub id: String,
    /// Flow nodes in document order, which drives node order and every layout tie-break.
    pub nodes: Vec<FlowNode>,
    /// Sequence flows in document order.
    pub flows: Vec<SequenceFlow>,
}

impl Process {
    /// An empty process.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: Vec::new(),
            flows: Vec::new(),
        }
    }

    /// Add a flow node.
    #[must_use]
    pub fn node(mut self, n: FlowNode) -> Self {
        self.nodes.push(n);
        self
    }

    /// Add a sequence flow.
    #[must_use]
    pub fn flow(mut self, f: SequenceFlow) -> Self {
        self.flows.push(f);
        self
    }
}

/// One flow node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlowNode {
    /// The element's `id`, drawn in parentheses under the label.
    pub id: String,
    /// The label text. A blank label draws like none.
    pub label: Option<String>,
    /// What shape the node draws as.
    pub kind: NodeKind,
    /// Loop markers along the bottom edge, in drawing order.
    pub markers: Vec<LoopMarker>,
}

impl FlowNode {
    /// A node with no label and no markers.
    #[must_use]
    pub fn new(id: impl Into<String>, kind: NodeKind) -> Self {
        Self {
            id: id.into(),
            label: None,
            kind,
            markers: Vec::new(),
        }
    }

    /// Set the label.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the loop markers, in drawing order.
    #[must_use]
    pub fn markers(mut self, markers: impl IntoIterator<Item = LoopMarker>) -> Self {
        self.markers = markers.into_iter().collect();
        self
    }
}

/// The visual type of a flow node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeKind {
    /// A thin circle, with a hollow glyph when it has one. `None` is a none start event.
    StartEvent(Option<EventGlyph>),
    /// A thick circle, with a filled glyph when it has one.
    EndEvent(Option<EventGlyph>),
    /// A double ring, with a filled glyph when it has one.
    IntermediateThrowEvent(Option<EventGlyph>),
    /// A double ring, with a hollow glyph when it has one.
    IntermediateCatchEvent(Option<EventGlyph>),
    /// A double ring pinned to the bottom edge of the activity it attaches to.
    BoundaryEvent {
        /// The id of the host activity. A boundary event whose host is not in the same scope is
        /// not drawn.
        attached_to_ref: String,
        /// The glyph inside the ring.
        glyph: EventGlyph,
    },
    /// A rounded box with a type glyph in its corner.
    Task(TaskType),
    /// A rounded box with a thick border.
    CallActivity,
    /// An expanded container enclosing its content.
    SubProcess {
        /// Which kind of container.
        kind: SubProcessKind,
        /// What it encloses. An empty one draws as a plain box.
        inner: Process,
    },
    /// A diamond with the gateway's marker.
    Gateway(GatewayType),
}

/// The glyph inside an event's circle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventGlyph {
    Message,
    Timer,
    Error,
    Escalation,
    Signal,
    Terminate,
    Link,
    Cancel,
    Compensate,
    /// A page with ruled lines.
    Conditional,
    /// A pentagon: any one of several triggers.
    Multiple,
    /// An open plus: all of several triggers.
    ParallelMultiple,
}

impl EventGlyph {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            EventGlyph::Message => "message",
            EventGlyph::Timer => "timer",
            EventGlyph::Error => "error",
            EventGlyph::Escalation => "escalation",
            EventGlyph::Signal => "signal",
            EventGlyph::Terminate => "terminate",
            EventGlyph::Link => "link",
            EventGlyph::Cancel => "cancel",
            EventGlyph::Compensate => "compensate",
            EventGlyph::Conditional => "conditional",
            EventGlyph::Multiple => "multiple",
            EventGlyph::ParallelMultiple => "parallel-multiple",
        }
    }
}

/// The glyph in a task's corner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskType {
    /// No glyph: an untyped `<task>`.
    Plain,
    Service,
    Script,
    Send,
    User,
    BusinessRule,
    Manual,
    Receive,
}

impl TaskType {
    pub(crate) fn glyph(self) -> &'static str {
        match self {
            TaskType::Plain => "",
            TaskType::Service => "service",
            TaskType::Script => "script",
            TaskType::Send => "send",
            TaskType::User => "user",
            TaskType::BusinessRule => "rule",
            TaskType::Manual => "manual",
            TaskType::Receive => "message",
        }
    }
}

/// Which kind of expanded container a sub-process is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubProcessKind {
    Embedded,
    /// Drawn with the doubled inline boundary.
    Transaction,
    /// Drawn with the ad-hoc marker.
    AdHoc,
    /// Triggered by an event. Drawn with a dotted border.
    Event,
}

/// The marker inside a gateway's diamond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GatewayType {
    Exclusive,
    /// Drawn with a double ring around an open pentagon.
    EventBased,
    Inclusive,
    Parallel,
    Complex,
}

/// An activity's loop marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoopMarker {
    Loop,
    ParallelMultiInstance,
    SequentialMultiInstance,
}

impl LoopMarker {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LoopMarker::Loop => "loop",
            LoopMarker::ParallelMultiInstance => "parallel-mi",
            LoopMarker::SequentialMultiInstance => "sequential-mi",
        }
    }
}

/// One sequence flow. A flow naming a node that is not in the same scope is not drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SequenceFlow {
    /// The element's `id`, drawn as the edge's caption when there is no label.
    pub id: String,
    /// The source node's id.
    pub source_ref: String,
    /// The target node's id.
    pub target_ref: String,
    /// The caption, drawn instead of the id when non-blank.
    pub label: Option<String>,
    /// The source's default flow: a slash marker at the source end.
    pub is_default: bool,
    /// Guarded by a condition: a diamond marker at the source end when the source is an activity.
    /// A flow that is also the default draws the default marker only.
    pub conditional: bool,
}

impl SequenceFlow {
    /// A plain flow with no label.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            source_ref: source.into(),
            target_ref: target.into(),
            label: None,
            is_default: false,
            conditional: false,
        }
    }

    /// Set the caption.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Mark the flow as its source's default flow.
    #[must_use]
    pub fn default_flow(mut self) -> Self {
        self.is_default = true;
        self
    }

    /// Mark the flow as guarded by a condition.
    #[must_use]
    pub fn conditional(mut self) -> Self {
        self.conditional = true;
        self
    }
}

/// Something in a model or its render options that does not draw as the caller may expect. Found
/// by [`Model::validate`] and [`crate::Options::validate`]; rendering never fails on one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Warning {
    /// A sequence flow names a node that is not in its own scope, so it is not drawn.
    DanglingFlow {
        /// The flow's id.
        flow_id: String,
        /// The first missing reference, source before target.
        missing_ref: String,
    },
    /// A message flow names neither a flow node nor a participant, so it is not drawn.
    DanglingMessageFlow {
        /// The flow's id.
        flow_id: String,
        /// The first missing reference, source before target.
        missing_ref: String,
    },
    /// A participant's process reference names no process, so it draws as a black-box pool.
    UnknownProcessRef {
        /// The participant's id.
        participant_id: String,
        /// The reference that names nothing.
        process_ref: String,
    },
    /// A `data-*` key outside `[a-z][a-z0-9-]*`, which is not emitted.
    InvalidDataKey {
        /// The node or flow id the attributes are for.
        element_id: String,
        /// The key that was dropped.
        key: String,
    },
    /// A class with a character outside `[A-Za-z0-9_-]`, or an empty one, which is not emitted.
    InvalidClass {
        /// The node or flow id the attributes are for.
        element_id: String,
        /// The class that was dropped.
        class: String,
    },
    /// An `href` holding `"` or `<`. It is still emitted, escaped.
    UnsafeHref {
        /// The node or flow id the attributes are for.
        element_id: String,
        /// The link as given.
        href: String,
    },
    /// Attributes keyed by an id that names no node (for node attributes) or no sequence or
    /// message flow (for flow attributes), so they are not used.
    UnknownAttrsTarget {
        /// The key that names nothing.
        id: String,
    },
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Warning::DanglingFlow {
                flow_id,
                missing_ref,
            } => write!(
                f,
                "The sequence flow '{flow_id}' names '{missing_ref}', which is not a node in its scope, so it is not drawn."
            ),
            Warning::DanglingMessageFlow {
                flow_id,
                missing_ref,
            } => write!(
                f,
                "The message flow '{flow_id}' names '{missing_ref}', which is neither a flow node nor a participant, so it is not drawn."
            ),
            Warning::UnknownProcessRef {
                participant_id,
                process_ref,
            } => write!(
                f,
                "The participant '{participant_id}' refers to the process '{process_ref}', which does not exist, so it draws as a black-box pool."
            ),
            Warning::InvalidDataKey { element_id, key } => write!(
                f,
                "The data key '{key}' on '{element_id}' is not of the form [a-z][a-z0-9-]*, so it is not emitted."
            ),
            Warning::InvalidClass { element_id, class } => write!(
                f,
                "The class '{class}' on '{element_id}' is empty or has a character outside [A-Za-z0-9_-], so it is not emitted."
            ),
            Warning::UnsafeHref { element_id, href } => write!(
                f,
                "The link '{href}' on '{element_id}' holds a quote or a '<'; it is emitted escaped."
            ),
            Warning::UnknownAttrsTarget { id } => write!(
                f,
                "Attributes are given for '{id}', which is not in the model, so they are not used."
            ),
        }
    }
}
