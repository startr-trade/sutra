//! Engine-internal process model — the `Node` variant hierarchy, `ProcessDefinition`,
//! `SequenceFlow`, `DataMapping`, `CoveragePath`, `ProcessModule`, and `BpmnImport`.

use std::collections::HashMap;

use crate::codes;
use crate::error::SutraError;
use crate::qbindings::{AuditCapture, NodeBindings};

/// Process-level `<q:audit>` policy (B1): the SINGLE sink a flow's audit events route to plus the
/// capture level. Resolved as process-level `<q:audit>` → deployment-manifest default; `None` on a
/// [`ProcessDefinition`] means the process is not audited (no process-level `<q:audit>` and no
/// manifest default). Node-level `<q:audit>` may ONLY suppress (`capture="none"`) — it never
/// overrides the sink or the capture level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessAudit {
    /// The single sink name events route to (`"sql"` | `"jsonl"` | `"otel"`) — one source of truth.
    pub sink: String,
    /// Capture level for the whole process: `Metadata` (default) or `Payload` (redacted variable
    /// snapshot on `NODE_ENTERED`). `None` is not a process-level value — it only suppresses per node.
    pub capture: AuditCapture,
}

/// The throw flavour of an intermediate throw event. `None` = a bare throw with no
/// event definition (pass-through).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrowKind {
    None,
    Compensate,
    Message,
    Signal,
    Escalation,
    Link,
}

/// Boundary-event flavour (Timer is Rust-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryKind {
    Error,
    Compensation,
    Escalation,
    Cancel,
    /// An interrupting timer boundary on a wait-capable host (channel-call serviceTask /
    /// userTask). Fires when the host's park outlives the ISO-8601 duration.
    Timer,
}

/// The channel-call task prefix on `serviceTask@implementation`.
pub const CHANNEL_CALL_PREFIX: &str = "channel:";

/// The variable a `<bpmn:multiInstanceLoopCharacteristics>` / `<bpmn:standardLoopCharacteristics>`
/// binds to the 0-based iteration index on every pass. Engine-supplied: no data association
/// writes it, so both the executor (which sets it) and the deploy-time linter (which must not
/// call a declaration of it "never initialised") read the name from here.
pub const LOOP_COUNTER_VARIABLE: &str = "loopCounter";

/// The variable a COLLECTION-driven multi-instance binds each item to when the activity omits
/// `<bpmn:inputDataItem name>`. A cardinality-only loop iterates no collection and therefore
/// binds no item variable at all.
pub const DEFAULT_LOOP_ITEM_VARIABLE: &str = "item";

/// Scalar type of a declared `<q:variable>`. Unknown types map to [`FieldType::Any`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Any,
}

/// A process variable declared by `<q:variable>`, in declaration order.
///
/// Carries the scalar `@type` (or [`FieldType::Any`] when only `@schema`/nothing is given), the
/// optional `@schema` reference (a `schemas/` file the variable's value conforms to, used
/// for static navigation-to-schema field checks), and the two persistence flags:
/// `@transient` (held in memory only, never persisted to the instance store) and `@sensitive`
/// (persisted — resume needs the value — but its value must be redacted in logs/audit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredVariable {
    /// `@name` — the variable name.
    pub name: String,
    /// `@type` — the scalar type; [`FieldType::Any`] when unspecified or `@schema`-typed.
    pub ty: FieldType,
    /// `@schema` — a `schemas/` file the value conforms to. `None` = scalar/untyped.
    pub schema: Option<String>,
    /// `@source` — a `<q:variable source="channel">` feed-off. The variable's
    /// navigation shape is derived from that intake channel's codec when `@schema` is absent.
    /// `None` = in-instance state (no intake link).
    pub source: Option<String>,
    /// `@transient` — never persisted to the instance store.
    pub transient: bool,
    /// `@sensitive` — persisted but redacted in logs/audit.
    pub sensitive: bool,
    /// `@subjectKey` — the variable is a GDPR data-subject key (e.g. `customerId`). On persist its
    /// value is stored as an HMAC blind index so instances can be enumerated for disclosure/erasure
    /// with no cleartext PII. The subject NAME is the variable name.
    pub subject_key: bool,
}

impl DeclaredVariable {
    /// A plain scalar-typed declaration (no schema, not transient, not sensitive, not a subject key).
    pub fn scalar(name: impl Into<String>, ty: FieldType) -> DeclaredVariable {
        DeclaredVariable {
            name: name.into(),
            ty,
            schema: None,
            source: None,
            transient: false,
            sensitive: false,
            subject_key: false,
        }
    }
}

/// Which BPMN element spelled a declared variable that a `<q:variable>` of the same name
/// overrode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BpmnDataSpelling {
    /// `<bpmn:property name="x">` on the process.
    Property,
    /// `<bpmn:dataObjectReference>` (or the `<bpmn:dataObject>` it points at) named by a data
    /// association.
    DataObject,
}

/// A BPMN-spelled data declaration that a `<q:variable>` of the same name won over.
///
/// Accepting two spellings for one name buys a round trip from a modelling tool at the cost of
/// ambiguity, and the author is the only one who can say which they meant. The loader cannot ask —
/// it fails closed or accepts — so it records the collision here and `sutra lint` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowedBpmnVariable {
    /// The variable name declared twice.
    pub name: String,
    /// The id of the BPMN element that lost: the process for a `<property>`, the data element
    /// itself otherwise.
    pub element_id: String,
    /// The losing spelling, so the advisory can name the element the author would recognise.
    pub spelling: BpmnDataSpelling,
}

/// A scoped parameter declared on a service task via `<q:param name= expression=/>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamBinding {
    pub name: String,
    pub expression: String,
}

/// Read a key from a data store into a process variable — or, with [`Self::query`], read every
/// row whose key matches a prefix or falls in a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRead {
    pub store: String,
    /// The row key; for a query, the key prefix (`query="prefix"`) or the inclusive lower bound
    /// of the range (`query="range"`).
    pub key_expression: String,
    pub for_update: bool,
    pub target_var: String,
    /// `Some` makes this read a key query (`<q:store query=…>`).
    pub query: Option<StoreQuery>,
}

/// The rows a query returns when `@limit` is not given.
pub const STORE_QUERY_DEFAULT_LIMIT: u32 = 1000;
/// The most rows one query page may return.
pub const STORE_QUERY_MAX_LIMIT: u32 = 10_000;

/// A key query on a key/value data store (`<q:store query="prefix|range">`): the rows whose key
/// starts with the prefix, or lies in `[key, to)`, keys compared bytewise. The target variable
/// receives `{rows, aggregates, truncated, next}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreQuery {
    pub mode: StoreQueryMode,
    /// Whether the result carries the matching rows (`@rows`, default true).
    pub rows: bool,
    /// The most rows one page returns (`@limit`, default [`STORE_QUERY_DEFAULT_LIMIT`]).
    pub limit: u32,
    /// FEEL for the key to continue after (`@after`): only keys greater than it are returned.
    pub after_expression: Option<String>,
    /// The aggregates computed over every matching row, in declaration order.
    pub aggregates: Vec<StoreQueryAggregate>,
}

/// How a [`StoreQuery`] selects keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreQueryMode {
    /// Keys starting with the read's key expression.
    Prefix,
    /// Keys from the read's key expression (inclusive) to `to_expression` (exclusive).
    Range { to_expression: String },
}

/// What an aggregate computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreAggregateFunction {
    /// The number of matching rows.
    Count,
    /// The number of matching rows and the exact decimal sum of one value field.
    Sum,
}

/// One `<q:aggregate>` of a key query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreQueryAggregate {
    pub function: StoreAggregateFunction,
    /// The dotted value path summed (`@field`; required for `sum`).
    pub field: Option<String>,
    /// The dotted value path rows are grouped by (`@groupBy`).
    pub group_by: Option<String>,
}

/// Compute a process variable from a FEEL expression (a visible data-assignment node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub expression: String,
    pub target_var: String,
}

/// Write a process variable back to a data store. When [`Self::field`] is present, only that
/// field of the stored (map) value is replaced. [`Self::expect_unchanged`] makes the write a
/// compare-and-set (data-store optimistic concurrency, `<q:store expect="unchanged">`);
/// [`Self::expect_absent`] makes it an insert that applies only when the key does not exist yet
/// (`<q:store expect="absent">`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreWrite {
    pub store: String,
    pub key_expression: String,
    pub field: Option<String>,
    pub value_var: String,
    pub expect_unchanged: bool,
    pub expect_absent: bool,
}

/// Explicit BPMN data-flow mapping for an activity: variable scoping
/// ([`Self::inputs`]/[`Self::outputs`]) plus the declarative data-task store operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DataMapping {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub store_reads: Vec<StoreRead>,
    pub assignments: Vec<Assignment>,
    pub store_writes: Vec<StoreWrite>,
}

impl DataMapping {
    /// True when the activity declares no data flow at all — it runs against the shared scope.
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
            && self.outputs.is_empty()
            && self.store_reads.is_empty()
            && self.assignments.is_empty()
            && self.store_writes.is_empty()
    }

    /// True when the mapping carries data-store work — i.e. it describes a declarative data task.
    pub fn has_store_ops(&self) -> bool {
        !self.store_reads.is_empty()
            || !self.assignments.is_empty()
            || !self.store_writes.is_empty()
    }
}

/// A tracked compliance/coverage path through a process: a stable id plus the ordered list of
/// sequence-flow ids that define the route. Declared in-BPMN via `<q:coverage path flows>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoveragePath {
    pub id: String,
    pub flows: Vec<String>,
}

/// Directed edge from one [`Node`] to another, optionally guarded by a FEEL condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceFlow {
    pub id: String,
    pub source_ref: String,
    pub target_ref: String,
    pub condition: Option<String>,
}

/// The coverage-path **contiguity relation**: sequence flow `a` is immediately followed by `b`
/// exactly when `a`'s target node is `b`'s source node (`a.target_ref == b.source_ref`). The
/// single source of truth for "a coverage route is contiguous", shared by the intra-process
/// `<q:coverage>` validator ([`crate::loader`]) and the cross-process coverage linter
/// (`FLOW_UNKNOWN` in `sutra-loader`) so the two never drift.
pub fn flows_contiguous(a: &SequenceFlow, b: &SequenceFlow) -> bool {
    a.target_ref == b.source_ref
}

/// One `<bpmn:import>` declaration on a `<bpmn:definitions>` element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BpmnImport {
    pub import_type: String,
    pub namespace: String,
    pub location: String,
}

/// BPMN node types the engine supports — the variants of the `Node` enum.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Start event — the process's entry point, triggered EITHER by an inbound message on one of
    /// its `<q:source channel>` channels OR by a `<bpmn:timerEventDefinition>` schedule, never
    /// both (the loader rejects a start event declaring both trigger contracts).
    StartEvent {
        id: String,
        name: Option<String>,
        /// Channels this start accepts inbound on (via `<q:source channel>`); empty for a
        /// timer-triggered or manually-driven start.
        channels: Vec<String>,
        /// The schedule that fires this start, when it is timer-triggered. The deployment's
        /// activation arms a durable schedule row per timer start; the poller fires it with
        /// EMPTY variables (a schedule carries no inbound payload).
        timer: Option<crate::timer::TimerDefinition>,
    },
    EndEvent {
        id: String,
        name: Option<String>,
    },
    /// End event carrying a `<bpmn:terminateEventDefinition>` — ends the whole instance.
    TerminateEndEvent {
        id: String,
        name: Option<String>,
    },
    /// End event with an `<bpmn:errorEventDefinition>` — throws a BPMN error (empty code =
    /// "any error").
    ErrorEvent {
        id: String,
        name: Option<String>,
        error_code: Option<String>,
    },
    /// Intermediate throw event — emit-and-continue; [`ThrowKind`] selects the flavour.
    IntermediateThrowEvent {
        id: String,
        name: Option<String>,
        kind: ThrowKind,
        activity_ref: Option<String>,
        /// Signal name / escalation code / link name, per [`ThrowKind`].
        reference: Option<String>,
    },
    /// Link catch event — the synchronous goto target of a link throw.
    LinkCatchEvent {
        id: String,
        name: Option<String>,
        link_name: String,
    },
    /// Intermediate Message Catch Event — a wait state on the stateful surface.
    MessageCatchEvent {
        id: String,
        name: Option<String>,
        channels: Vec<String>,
        message_ref: Option<String>,
    },
    /// Event-based gateway — races its outgoing catch events; the FIRST to fire wins and the
    /// others are retired.
    ///
    /// The gateway is not itself a wait state: it is the fork that parks its TARGETS, and those
    /// targets are already wait states, so a process containing one is correctly stateful without
    /// this node claiming to park. One token enters and one token leaves — which is the whole
    /// difficulty, because N parked targets would otherwise be N independent tokens (what a
    /// parallel gateway means). First-wins is enforced at the resume boundary by retiring the
    /// losing siblings in the SAME transaction that advances the winner.
    ///
    /// `Exclusive`, non-instantiating only. BPMN's `Parallel` flavour is meaningful only with
    /// `instantiate="true"`, and both are rejected at load.
    EventBasedGateway {
        id: String,
        name: Option<String>,
    },
    /// Receive task — parks until a correlated inbound arrives, exactly as
    /// [`Node::MessageCatchEvent`] does.
    ///
    /// A separate variant rather than an alias of the catch event, because the difference is not
    /// cosmetic: a receive task is an ACTIVITY, so a boundary event can attach to it. That is
    /// BPMN's way to say "wait for this message, or time out" — and without it the only route to
    /// that shape here is a channel-call service task, which requires a request leg the model may
    /// not have. An event cannot host a boundary, so the catch event could never express it.
    ReceiveTask {
        id: String,
        name: Option<String>,
        channels: Vec<String>,
        message_ref: Option<String>,
    },
    /// Intermediate Timer Catch Event (Rust-only): a wait state that parks a durable TIMER
    /// `waiting_event` row due at the instant `timer` resolves to.
    TimerCatchEvent {
        id: String,
        name: Option<String>,
        /// The scheduling contract — `<timeDuration>` (park + duration) or `<timeDate>` (the
        /// absolute instant). Validated parseable at load time; `<timeCycle>` is rejected here
        /// (a token cannot park at a node that fires repeatedly).
        timer: crate::timer::TimerDefinition,
    },
    /// Boundary event attached to an activity (error / compensation / escalation / cancel /
    /// timer).
    BoundaryEvent {
        id: String,
        name: Option<String>,
        attached_to_ref: String,
        kind: BoundaryKind,
        error_code: Option<String>,
        escalation_code: Option<String>,
        interrupting: bool,
        /// The scheduling contract of a [`BoundaryKind::Timer`] boundary (`None` otherwise) —
        /// `<timeDuration>` (armed when the host parks) or `<timeDate>` (an absolute deadline).
        timer: Option<crate::timer::TimerDefinition>,
    },
    /// Service task routed on its `implementation` attribute (task-kind precedence).
    ServiceTask {
        id: String,
        name: Option<String>,
        implementation: String,
        data_mapping: DataMapping,
        params: Vec<ParamBinding>,
        /// `q:stream="true"` — marks a channel-call task whose synchronous model answer is
        /// streamed to the original caller. Defaults to `false`; only ever `true` on a
        /// `channel:`-prefixed implementation (the loader rejects it elsewhere).
        stream: bool,
    },
    /// Declarative data task: a serviceTask with data associations and NO
    /// `@implementation` — its store reads / FEEL assignments / store writes ARE its behaviour.
    DataTask {
        id: String,
        name: Option<String>,
        data_mapping: DataMapping,
    },
    /// Script task: `<bpmn:script>` names a file in the module's `scripts/` folder.
    ScriptTask {
        id: String,
        name: Option<String>,
        script_file: String,
    },
    /// Manual task — a pure no-op pass-through.
    ManualTask {
        id: String,
        name: Option<String>,
        /// True when the author wrote BPMN's UNTYPED `<task>` rather than `<manualTask>`.
        ///
        /// Both execute identically — with no type there is no behaviour to run — but they do
        /// not MEAN the same thing: a manual task is work done outside the system on purpose,
        /// while an untyped task is unspecified, and an author who typed `<task>` meaning a
        /// service task has written a step that silently does nothing. Carrying the spelling is
        /// what lets `sutra lint` say so at package time; the loader has no warning channel of
        /// its own, so the model is the only route from the parse to the advice.
        untyped: bool,
    },
    /// Send task — emit-and-continue; its `<q:send>` is required (enforced at load).
    SendTask {
        id: String,
        name: Option<String>,
    },
    /// Business rule task — evaluates a decision file and merges the result.
    BusinessRuleTask {
        id: String,
        name: Option<String>,
        decision_file: String,
    },
    /// Wait state: the token parks until an external relay resumes the instance.
    UserTask {
        id: String,
        name: Option<String>,
        channels: Vec<String>,
    },
    /// Call activity invoking a sub-process by id.
    CallActivity {
        id: String,
        name: Option<String>,
        called_element: String,
        called_namespace: Option<String>,
    },
    /// Embedded sub-process — expanded inline and synchronously.
    SubProcess {
        id: String,
        name: Option<String>,
        inner: Box<ProcessDefinition>,
    },
    /// A `<bpmn:transaction>` sub-process: one store transaction around the inner flow.
    TransactionSubProcess {
        id: String,
        name: Option<String>,
        inner: Box<ProcessDefinition>,
    },
    /// A `<bpmn:adHocSubProcess>` (Track H): activities with no enforced sequence flow, run in
    /// document order to a FEEL completion condition.
    AdHocSubProcess {
        id: String,
        name: Option<String>,
        inner: Box<ProcessDefinition>,
        completion_condition: Option<String>,
        parallel: bool,
    },
    /// An error-triggered event sub-process (Track H) — no incoming flow; triggered when an
    /// error escapes an activity in its enclosing scope and no boundary event catches it.
    EventSubProcess {
        id: String,
        name: Option<String>,
        inner: Box<ProcessDefinition>,
        error_code: Option<String>,
        interrupting: bool,
    },
    /// End event with a `<bpmn:cancelEventDefinition>` — valid only inside a transaction.
    CancelEndEvent {
        id: String,
        name: Option<String>,
    },
    /// Diverging XOR gate — at most one outgoing flow fires, chosen by condition order.
    ExclusiveGateway {
        id: String,
        name: Option<String>,
        default_flow_id: Option<String>,
    },
    /// OR-fork / OR-join (E5b).
    InclusiveGateway {
        id: String,
        name: Option<String>,
        default_flow_id: Option<String>,
    },
    /// AND-fork / AND-join.
    ParallelGateway {
        id: String,
        name: Option<String>,
    },
    /// Complex gateway: inclusive-style fork + FEEL-driven N-of-M join.
    ComplexGateway {
        id: String,
        name: Option<String>,
        default_flow_id: Option<String>,
        activation_condition: Option<String>,
    },
    /// Wraps an inner activity with BPMN multi-instance loop characteristics (E5c).
    MultiInstance {
        id: String,
        name: Option<String>,
        inner: Box<Node>,
        sequential: bool,
        loop_cardinality: Option<String>,
        loop_data_input_ref: Option<String>,
        input_data_item: Option<String>,
        completion_condition: Option<String>,
    },
    /// Standard loop marker — `<bpmn:standardLoopCharacteristics>` on an activity.
    StandardLoop {
        id: String,
        name: Option<String>,
        inner: Box<Node>,
        loop_condition: Option<String>,
        test_before: bool,
        loop_maximum: Option<i64>,
    },
}

impl Node {
    /// BPMN element id — unique within a process.
    pub fn id(&self) -> &str {
        match self {
            Node::StartEvent { id, .. }
            | Node::EndEvent { id, .. }
            | Node::TerminateEndEvent { id, .. }
            | Node::ErrorEvent { id, .. }
            | Node::IntermediateThrowEvent { id, .. }
            | Node::LinkCatchEvent { id, .. }
            | Node::MessageCatchEvent { id, .. }
            | Node::ReceiveTask { id, .. }
            | Node::EventBasedGateway { id, .. }
            | Node::TimerCatchEvent { id, .. }
            | Node::BoundaryEvent { id, .. }
            | Node::ServiceTask { id, .. }
            | Node::DataTask { id, .. }
            | Node::ScriptTask { id, .. }
            | Node::ManualTask { id, .. }
            | Node::SendTask { id, .. }
            | Node::BusinessRuleTask { id, .. }
            | Node::UserTask { id, .. }
            | Node::CallActivity { id, .. }
            | Node::SubProcess { id, .. }
            | Node::TransactionSubProcess { id, .. }
            | Node::AdHocSubProcess { id, .. }
            | Node::EventSubProcess { id, .. }
            | Node::CancelEndEvent { id, .. }
            | Node::ExclusiveGateway { id, .. }
            | Node::InclusiveGateway { id, .. }
            | Node::ParallelGateway { id, .. }
            | Node::ComplexGateway { id, .. }
            | Node::MultiInstance { id, .. }
            | Node::StandardLoop { id, .. } => id,
        }
    }

    /// True if this node parks the token and waits for an external relay (or a due timer)
    /// before the process can advance — the stateful surface that makes a process
    /// ineligible for `execute_sync`.
    pub fn is_wait_state(&self) -> bool {
        match self {
            Node::UserTask { .. } | Node::MessageCatchEvent { .. } | Node::ReceiveTask { .. } => {
                true
            }
            // A timer catch parks until the due-at; a channel-call
            // service task parks until the correlated response (or its timer boundary).
            Node::TimerCatchEvent { .. } => true,
            Node::ServiceTask { implementation, .. } => {
                implementation.starts_with(CHANNEL_CALL_PREFIX)
            }
            Node::SubProcess { inner, .. } | Node::TransactionSubProcess { inner, .. } => {
                !inner.is_sync_eligible()
            }
            _ => false,
        }
    }
}

/// One FEEL timer expression in a process ([`ProcessDefinition::node_timer_expressions`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerExpressionSite {
    /// The node that declares it (a synthesized `<taskId>#timeout` boundary for `<q:timeout>`).
    pub node_id: String,
    /// Where it sits: `timeDuration`, `timeDate`, `timeCycle`, `q:retry@initialDelay`,
    /// `q:retry@maxDelay`.
    pub site: &'static str,
    /// The FEEL source.
    pub expression: String,
    /// True on a timer start event, where only `config` is in scope.
    pub on_start_event: bool,
}

/// Engine-internal representation of a single `<bpmn:process>` — indexed by node id, with
/// sequence flows pre-grouped by source/target. Built once at module load and immutable.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessDefinition {
    pub id: String,
    pub name: Option<String>,
    pub is_executable: bool,
    /// Per-process version pin: `versionTag` → `<q:audit version>` → `"1.0"`.
    pub module_version: String,
    nodes: Vec<Node>,
    flows: Vec<SequenceFlow>,
    outgoing_by_source: HashMap<String, Vec<usize>>,
    incoming_by_target: HashMap<String, Vec<usize>>,
    bindings: HashMap<String, NodeBindings>,
    /// Process variables declared by `<q:variables>`, in declaration order.
    pub declared_variables: Vec<DeclaredVariable>,
    /// Names declared BOTH by a BPMN spelling and by a `<q:variable>`, where the `<q:variable>`
    /// won. Carried for `sutra lint` only — the runtime sees just the merged
    /// [`Self::declared_variables`].
    pub shadowed_bpmn_variables: Vec<ShadowedBpmnVariable>,
    /// Path-coverage metric — the tracked routes declared by `<q:coverage>` on this process.
    pub coverage_paths: Vec<CoveragePath>,
    /// Process-level `<q:audit>` policy (single sink + capture level). `None` = the process
    /// declares no `<q:audit>`; the deployment-manifest default (if any) is applied at resolution.
    pub audit: Option<ProcessAudit>,
    /// The process-level idempotency ASSERTION (`<q:process idempotent="true|false">`).
    /// `true` means the author asserts re-executing this process on the same input converges to a
    /// single end state, so the inbound is **safe to retry any number of times**; `false` (the
    /// fail-closed default — an undeclared process is treated as non-idempotent) means blind
    /// redelivery-and-reprocess is unsafe. Distinct from a `<q:source dedupKey>` (a
    /// duplicate-detection value): a dedup key never, on its own, asserts retry-safety. Gates the
    /// inbound ack decision on an execution failure (`SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE`).
    pub idempotent: bool,
}

/// Decides one start event's `<q:source filterExpression>` predicate for the message being
/// routed.
///
/// This crate stores the predicate as TEXT and owns the precedence rule; the expression language
/// belongs to `sutra-channels`, which compiles each predicate once per activation and implements
/// this trait over that table. The seam is what keeps an expression evaluator out of the BPMN
/// model, exactly as `<q:case when>` and `<q:alias expression>` are stored unparsed here.
///
/// **Fail-closed is the implementor's duty.** An evaluation error, a non-boolean result, or a
/// missing compiled predicate must answer `false` and raise the diagnostic at that layer. The
/// model never evaluates FEEL and never learns that a predicate failed — it only learns that the
/// start event is not eligible.
pub trait SourcePredicate {
    /// Does the predicate on start event `node_id` hold? Called ONLY for a source that declares
    /// one, so an implementor may treat an unknown node id as a bug in its own table.
    fn holds(&self, node_id: &str) -> bool;
}

/// Answers `false` for every predicate: the fail-closed stand-in for a caller with no evaluator.
/// A predicated start event is then simply not selected, rather than selected without its filter
/// having been checked.
pub struct PredicateUnavailable;

impl SourcePredicate for PredicateUnavailable {
    fn holds(&self, _node_id: &str) -> bool {
        false
    }
}

/// Answers a constant. `PredicateAlways(true)` ignores predicates altogether — the behaviour
/// before this feature existed, and what a test that is not about filtering wants.
pub struct PredicateAlways(pub bool);

impl SourcePredicate for PredicateAlways {
    fn holds(&self, _node_id: &str) -> bool {
        self.0
    }
}

impl ProcessDefinition {
    /// Build a `ProcessDefinition` from node + flow lists, including the duplicate-node-id
    /// check.
    #[allow(clippy::too_many_arguments)]
    pub fn of(
        id: impl Into<String>,
        name: Option<String>,
        is_executable: bool,
        module_version: impl Into<String>,
        nodes: Vec<Node>,
        flows: Vec<SequenceFlow>,
        bindings: HashMap<String, NodeBindings>,
        declared_variables: Vec<DeclaredVariable>,
    ) -> Result<ProcessDefinition, SutraError> {
        let mut seen = std::collections::HashSet::new();
        for n in &nodes {
            if !seen.insert(n.id().to_string()) {
                return Err(SutraError::new(
                    codes::RESOLVE_TASK_NAME_COLLISION,
                    format!("Duplicate node id in process: {}", n.id()),
                ));
            }
        }
        let mut outgoing_by_source: HashMap<String, Vec<usize>> = HashMap::new();
        let mut incoming_by_target: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, f) in flows.iter().enumerate() {
            outgoing_by_source
                .entry(f.source_ref.clone())
                .or_default()
                .push(i);
            incoming_by_target
                .entry(f.target_ref.clone())
                .or_default()
                .push(i);
        }
        let module_version = {
            let v = module_version.into();
            if v.trim().is_empty() {
                "1.0".to_string()
            } else {
                v
            }
        };
        Ok(ProcessDefinition {
            id: id.into(),
            name,
            is_executable,
            module_version,
            nodes,
            flows,
            outgoing_by_source,
            incoming_by_target,
            bindings,
            declared_variables,
            shadowed_bpmn_variables: Vec::new(),
            coverage_paths: Vec::new(),
            audit: None,
            idempotent: false,
        })
    }

    /// A copy of this process with its BPMN/`q:` declaration collisions recorded (applied by the
    /// parser after assembly, like coverage — the merge is a document-wide scan result, not a
    /// per-node one).
    pub fn with_shadowed_bpmn_variables(
        mut self,
        shadowed: Vec<ShadowedBpmnVariable>,
    ) -> ProcessDefinition {
        self.shadowed_bpmn_variables = shadowed;
        self
    }

    /// A copy of this process with its `<q:coverage>` paths set (applied by the parser after
    /// assembly, since coverage is a top-level-process property).
    pub fn with_coverage_paths(mut self, paths: Vec<CoveragePath>) -> ProcessDefinition {
        self.coverage_paths = paths;
        self
    }

    /// A copy of this process with its process-level `<q:audit>` policy set (B1) — applied by the
    /// parser after assembly, like coverage. `None` leaves the process unaudited pending a
    /// deployment-manifest default.
    pub fn with_audit(mut self, audit: Option<ProcessAudit>) -> ProcessDefinition {
        self.audit = audit;
        self
    }

    /// A copy of this process with its `<q:process idempotent>` assertion set (applied by
    /// the parser after assembly, like coverage/audit). Default is `false` (fail-closed).
    pub fn with_idempotent(mut self, idempotent: bool) -> ProcessDefinition {
        self.idempotent = idempotent;
        self
    }

    /// The node with the given id — `SUTRA.RESOLVE.MODULE.NOT_FOUND` when absent.
    pub fn node(&self, node_id: &str) -> Result<&Node, SutraError> {
        self.nodes
            .iter()
            .find(|n| n.id() == node_id)
            .ok_or_else(|| {
                SutraError::new(
                    codes::RESOLVE_MODULE_NOT_FOUND,
                    format!("Node not found in process {}: {}", self.id, node_id),
                )
            })
    }

    /// All nodes in document order (boundary events appended last, as the loader builds them).
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn flows(&self) -> &[SequenceFlow] {
        &self.flows
    }

    /// Outgoing flows of a node, in declaration order.
    pub fn outgoing(&self, node_id: &str) -> Vec<&SequenceFlow> {
        self.outgoing_by_source
            .get(node_id)
            .map(|idx| idx.iter().map(|&i| &self.flows[i]).collect())
            .unwrap_or_default()
    }

    /// Incoming flows of a node, in declaration order.
    pub fn incoming(&self, node_id: &str) -> Vec<&SequenceFlow> {
        self.incoming_by_target
            .get(node_id)
            .map(|idx| idx.iter().map(|&i| &self.flows[i]).collect())
            .unwrap_or_default()
    }

    /// The parsed `q:` bindings attached to the given node id (never fails — empty aggregate
    /// when no `<q:*>` extensions were present).
    pub fn bindings_for(&self, node_id: &str) -> &NodeBindings {
        static EMPTY: NodeBindings = NodeBindings {
            sources: Vec::new(),
            on_validation: None,
            dispatch: None,
            reply: None,
            send: None,
            aliases: Vec::new(),
            audit: None,
            timeout: None,
            output: None,
            retry: None,
        };
        self.bindings.get(node_id).unwrap_or(&EMPTY)
    }

    /// The (single) start event — errors when the process declares several (multi-start
    /// processes resolve a specific one via [`Self::select_start_event`]).
    pub fn start_event(&self) -> Result<&Node, SutraError> {
        let starts = self.start_events();
        if starts.len() != 1 {
            return Err(SutraError::new(
                codes::PARSE_BPMN_MISSING_PROCESS,
                format!(
                    "Process {} must have exactly one start event, found {}",
                    self.id,
                    starts.len()
                ),
            ));
        }
        Ok(starts[0])
    }

    /// All start events of this process, in id order (deterministic).
    pub fn start_events(&self) -> Vec<&Node> {
        let mut starts: Vec<&Node> = self
            .nodes
            .iter()
            .filter(|n| matches!(n, Node::StartEvent { .. }))
            .collect();
        starts.sort_by_key(|n| n.id().to_string());
        starts
    }

    /// The SCHEDULE-triggered start events of this process: `(start event id, its timer)`, in id
    /// order. Deployment activation arms one durable schedule row per pair; nothing else in the
    /// engine needs to know how a timer start is spelled in the XML.
    pub fn timer_start_events(&self) -> Vec<(&str, &crate::timer::TimerDefinition)> {
        self.start_events()
            .into_iter()
            .filter_map(|n| match n {
                Node::StartEvent {
                    id,
                    timer: Some(timer),
                    ..
                } => Some((id.as_str(), timer)),
                _ => None,
            })
            .collect()
    }

    /// Every FEEL timer expression declared on `node` (its timer definition and its `<q:retry>`
    /// delays). A `<q:timeout>` expression shows up on its synthesized `<taskId>#timeout`
    /// boundary, which is a node of its own. Lint uses this for the `config.<key>` check.
    pub fn node_timer_expressions(&self, node: &Node) -> Vec<TimerExpressionSite> {
        let mut out = Vec::new();
        let (timer, on_start) = match node {
            Node::TimerCatchEvent { timer, .. } => (Some(timer), false),
            Node::BoundaryEvent { timer, .. } => (timer.as_ref(), false),
            Node::StartEvent { timer, .. } => (timer.as_ref(), true),
            _ => (None, false),
        };
        if let Some(crate::timer::TimerDefinition::Expression { form, expression }) = timer {
            out.push(TimerExpressionSite {
                node_id: node.id().to_string(),
                site: form.element_name(),
                expression: expression.clone(),
                on_start_event: on_start,
            });
        }
        if let Some(retry) = self.retry_policy(node.id()) {
            for (is_expression, text, site) in [
                (
                    retry.initial_delay_expression,
                    &retry.initial_delay,
                    "q:retry@initialDelay",
                ),
                (
                    retry.max_delay_expression,
                    &retry.max_delay,
                    "q:retry@maxDelay",
                ),
            ] {
                if is_expression {
                    out.push(TimerExpressionSite {
                        node_id: node.id().to_string(),
                        site,
                        expression: text.clone(),
                        on_start_event: false,
                    });
                }
            }
        }
        out
    }

    /// EVERY start event that handles an inbound arriving on `channel` carrying codec
    /// `message_type` — all the matches at the winning tier, in document order.
    ///
    /// Tiers, highest first: exact `messageTypeValue` → matching `messageTypePattern` (full regex
    /// match) → the unfiltered catch-all. A `<q:source filterExpression>` predicate is a
    /// CONJUNCT on its own tier, never a tier of its own: a start event whose predicate is false
    /// does not occupy its tier, so selection falls through to the next one. That is what keeps
    /// "more specific wins" true while letting two start events pin the same message type and
    /// differ only by attribute.
    ///
    /// Returning every match rather than the first is deliberate. The caller owns the ambiguity
    /// policy, and a single process declaring two start events that both match is a configuration
    /// the predicate feature makes ordinary — it used to resolve silently by document order.
    pub fn select_start_events(
        &self,
        channel: &str,
        message_type: Option<&str>,
        predicate: &dyn SourcePredicate,
    ) -> Vec<&Node> {
        let candidates: Vec<&Node> = self
            .start_events()
            .into_iter()
            .filter(|s| match s {
                Node::StartEvent { channels, .. } => channels.iter().any(|c| c == channel),
                _ => false,
            })
            .collect();
        if candidates.is_empty() {
            return Vec::new();
        }
        // A source with no predicate passes trivially; one with a predicate asks the caller.
        let holds = |s: &&Node| -> bool {
            match self
                .bindings_for(s.id())
                .source()
                .and_then(|b| b.filter_expression.as_deref())
            {
                Some(_) => predicate.holds(s.id()),
                None => true,
            }
        };

        if let Some(mt) = message_type {
            let exact: Vec<&Node> = candidates
                .iter()
                .copied()
                .filter(|s| {
                    self.bindings_for(s.id())
                        .source()
                        .and_then(|b| b.message_type_value.as_deref())
                        == Some(mt)
                })
                .filter(holds)
                .collect();
            if !exact.is_empty() {
                return exact;
            }

            let matched: Vec<&Node> = candidates
                .iter()
                .copied()
                .filter(|s| {
                    self.bindings_for(s.id())
                        .source()
                        .and_then(|b| b.message_type_pattern.as_deref())
                        .is_some_and(|p| {
                            // Full-match semantics: anchor the pattern end to end. A malformed
                            // pattern matches nothing here; it cannot reach a live engine at all,
                            // because activation refuses a deployment whose patterns do not
                            // compile. Skipping rather than panicking keeps the model total.
                            regex::Regex::new(&format!("^(?:{p})$"))
                                .map(|re| re.is_match(mt))
                                .unwrap_or(false)
                        })
                })
                .filter(holds)
                .collect();
            if !matched.is_empty() {
                return matched;
            }
        }

        candidates
            .iter()
            .copied()
            .filter(|s| {
                !self
                    .bindings_for(s.id())
                    .source()
                    .map(|b| b.message_type_value.is_some() || b.message_type_pattern.is_some())
                    .unwrap_or(false)
            })
            .filter(holds)
            .collect()
    }

    /// The first of [`Self::select_start_events`] — for callers that want one start event and
    /// have no ambiguity policy of their own.
    pub fn select_start_event(
        &self,
        channel: &str,
        message_type: Option<&str>,
        predicate: &dyn SourcePredicate,
    ) -> Option<&Node> {
        self.select_start_events(channel, message_type, predicate)
            .into_iter()
            .next()
    }

    /// True if the process runs to completion with no quiescent point — eligible for the
    /// synchronous executor. A wait state OR a respond-and-continue reply (`<q:reply continue>`,
    /// which flushes early then parks + self-resumes) OR a `<q:retry>` task makes it stateful.
    ///
    /// The retry clause is a POTENTIAL-wait rule, not an actual one: a retried task that succeeds
    /// first time never parks. But the retry wait is a durable TIMER park, and the synchronous
    /// executor has no snapshot, no timer rows and no resume — so a process carrying one must be
    /// classified stateful up front or its first failed attempt would have nowhere to land. The
    /// cost of the conservative call is a snapshot for a process that may never park; the cost of
    /// the other call is a retry policy that silently never fires.
    pub fn is_sync_eligible(&self) -> bool {
        self.nodes.iter().all(|n| {
            !n.is_wait_state() && !self.has_continue_reply(n.id()) && !self.has_retry_policy(n.id())
        })
    }

    /// True when this node carries a `<q:retry>` per-task retry policy (see
    /// [`crate::qbindings::RetryBinding`]). The loader has already refused the policy on any node
    /// that could not honour it, so a `true` here means "this node can park a retry timer".
    pub fn has_retry_policy(&self, node_id: &str) -> bool {
        self.bindings_for(node_id).retry.is_some()
    }

    /// This node's `<q:retry>` policy, when it declares one.
    pub fn retry_policy(&self, node_id: &str) -> Option<&crate::qbindings::RetryBinding> {
        self.bindings_for(node_id).retry.as_ref()
    }

    /// True when this node carries a `<q:reply continue="true">` (respond-and-continue) — the
    /// point at which the reply is flushed to the caller and the instance parks to self-resume the
    /// remaining nodes asynchronously.
    pub fn has_continue_reply(&self, node_id: &str) -> bool {
        self.bindings_for(node_id)
            .reply
            .as_ref()
            .map(|r| r.continue_after)
            .unwrap_or(false)
    }
}

/// Engine-internal representation of a BPMN module — one `<bpmn:definitions>`' worth of
/// processes sharing a target namespace.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessModule {
    pub target_namespace: String,
    pub version: Option<String>,
    pub imports: Vec<BpmnImport>,
    processes: Vec<ProcessDefinition>,
}

impl ProcessModule {
    pub fn of(
        target_namespace: impl Into<String>,
        imports: Vec<BpmnImport>,
        processes: Vec<ProcessDefinition>,
    ) -> Result<ProcessModule, SutraError> {
        let mut seen = std::collections::HashSet::new();
        for p in &processes {
            if !seen.insert(p.id.clone()) {
                return Err(SutraError::new(
                    codes::RESOLVE_TASK_NAME_COLLISION,
                    format!("Duplicate process id: {}", p.id),
                ));
            }
        }
        Ok(ProcessModule {
            target_namespace: target_namespace.into(),
            version: None,
            imports,
            processes,
        })
    }

    pub fn process(&self, id: &str) -> Result<&ProcessDefinition, SutraError> {
        self.processes.iter().find(|p| p.id == id).ok_or_else(|| {
            SutraError::new(
                codes::RESOLVE_MODULE_NOT_FOUND,
                format!(
                    "Process not found in module {}: {}",
                    self.target_namespace, id
                ),
            )
        })
    }

    pub fn processes(&self) -> &[ProcessDefinition] {
        &self.processes
    }

    pub fn process_ids(&self) -> Vec<&str> {
        self.processes.iter().map(|p| p.id.as_str()).collect()
    }

    /// B1 — desugar a deployment-level audit default (from `manifest.yaml`) onto this module's
    /// processes: every process that declares NO process-level `<q:audit>` inherits `default` (as
    /// if it had declared it). A process with its own `<q:audit>` keeps it (author override). `None`
    /// leaves the module untouched. This is the whole runtime meaning of the manifest default —
    /// after desugaring, each process carries its effective [`ProcessAudit`] and the executor reads
    /// it directly (no per-deployment resolution needed).
    #[must_use]
    pub fn with_audit_default(mut self, default: &Option<ProcessAudit>) -> ProcessModule {
        if let Some(default) = default {
            for process in &mut self.processes {
                if process.audit.is_none() {
                    process.audit = Some(default.clone());
                }
            }
        }
        self
    }
}
