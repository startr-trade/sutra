//! The neutral model: only what a diagram uses. Conditions, timers and expressions stay out; where
//! one affects how a node draws, the model carries the resulting glyph or label.

/// A BPMN file reduced to what its diagram needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    /// Top-level processes in document order, drawn one above the other.
    pub processes: Vec<Process>,
}

/// A process, or the content of a sub-process.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Process {
    /// The element's `id`. Empty when the caller has none.
    pub id: String,
    /// Flow nodes in document order, which drives node order and every layout tie-break.
    pub nodes: Vec<FlowNode>,
    /// Sequence flows in document order.
    pub flows: Vec<SequenceFlow>,
}

/// One flow node.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// The visual type of a flow node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    /// A thin circle.
    StartEvent(EventGlyph),
    /// A thick circle, with a filled glyph when it has one.
    EndEvent(Option<EventGlyph>),
    /// A double ring with a filled glyph.
    IntermediateThrowEvent(EventGlyph),
    /// A double ring with a hollow glyph.
    IntermediateCatchEvent(EventGlyph),
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
        }
    }
}

/// The glyph in a task's corner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    /// No glyph. [`crate::parse`] never produces it, because an untyped `<task>` draws as manual;
    /// it is for a caller building a model with a task that has no BPMN glyph.
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
pub enum SubProcessKind {
    Embedded,
    /// Drawn with the doubled inline boundary.
    Transaction,
    /// Drawn with the ad-hoc marker.
    AdHoc,
    /// Triggered by an event. Currently drawn like [`SubProcessKind::Embedded`].
    Event,
}

/// The marker inside a gateway's diamond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayType {
    Exclusive,
    /// Currently drawn like [`GatewayType::Exclusive`].
    EventBased,
    Inclusive,
    Parallel,
    Complex,
}

/// An activity's loop marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub struct SequenceFlow {
    /// The element's `id`, drawn as the edge's caption.
    pub id: String,
    /// The source node's id.
    pub source_ref: String,
    /// The target node's id.
    pub target_ref: String,
}
