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

/// A scoped parameter declared on a service task via `<q:param name= expression=/>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamBinding {
    pub name: String,
    pub expression: String,
}

/// Read a key from a data store into a process variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRead {
    pub store: String,
    pub key_expression: String,
    pub for_update: bool,
    pub target_var: String,
}

/// Compute a process variable from a FEEL expression (a visible data-assignment node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub expression: String,
    pub target_var: String,
}

/// Write a process variable back to a data store. When [`Self::field`] is present, only that
/// field of the stored (map) value is replaced. [`Self::expect_unchanged`] makes the write a
/// compare-and-set (data-store optimistic concurrency, `<q:store expect="unchanged">`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreWrite {
    pub store: String,
    pub key_expression: String,
    pub field: Option<String>,
    pub value_var: String,
    pub expect_unchanged: bool,
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
            Node::UserTask { .. } | Node::MessageCatchEvent { .. } => true,
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
            coverage_paths: Vec::new(),
            audit: None,
            idempotent: false,
        })
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

    /// Select the start event that handles an inbound arriving on `channel` carrying codec
    /// `message_type`. Preference (highest first): exact `messageTypeValue` → matching
    /// `messageTypePattern` (full regex match) → the unfiltered catch-all.
    pub fn select_start_event(&self, channel: &str, message_type: Option<&str>) -> Option<&Node> {
        let candidates: Vec<&Node> = self
            .start_events()
            .into_iter()
            .filter(|s| match s {
                Node::StartEvent { channels, .. } => channels.iter().any(|c| c == channel),
                _ => false,
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        if let Some(mt) = message_type {
            for s in &candidates {
                if let Some(v) = self
                    .bindings_for(s.id())
                    .source()
                    .and_then(|b| b.message_type_value.as_deref())
                {
                    if v == mt {
                        return Some(s);
                    }
                }
            }
            for s in &candidates {
                if let Some(p) = self
                    .bindings_for(s.id())
                    .source()
                    .and_then(|b| b.message_type_pattern.as_deref())
                {
                    // Full-match semantics: anchor the pattern end to end.
                    if let Ok(re) = regex::Regex::new(&format!("^(?:{p})$")) {
                        if re.is_match(mt) {
                            return Some(s);
                        }
                    }
                }
            }
        }
        candidates.into_iter().find(|s| {
            let src = self.bindings_for(s.id()).source();
            let filtered = src
                .map(|b| b.message_type_value.is_some() || b.message_type_pattern.is_some())
                .unwrap_or(false);
            !filtered
        })
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
