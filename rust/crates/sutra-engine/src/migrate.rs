//! Instance migration — moving a LIVE instance from the deployment it is pinned to onto another.
//!
//! An instance is pinned to its deployment by content-hash `deploymentId` the moment it first
//! parks, and every resume path resolves that pin fail-closed: a hot-deploy leaves the old graph
//! DRAINING precisely so pinned instances keep resuming on the definition they started under. That
//! is the right default and it is why the engine has no accidental version skew — but it also means
//! an instance parked on a broken model stays parked on the broken model forever.
//!
//! This module is the sanctioned way out: an EXPLICIT, VALIDATED, AUDITED admin operation that
//! re-pins one instance onto another ACTIVE deployment and rewrites every node id its durable state
//! names. It is deliberately not a versioning *mechanism* — there is no in-process branch to write
//! and no worker fleet to label. Migration is an operator action with a machine-readable report,
//! which means it can be dry-run before it is done, refused with every violation listed rather than
//! the first, and read back out of the audit journal afterwards.
//!
//! ## The three questions the validator answers
//!
//! 1. **Is the target legitimate?** It must be an ACTIVE deployment. DRAINING is refused: a
//!    draining deployment is on its way out and retires the moment it is quiescent, so migrating
//!    ONTO one would strand the instance again — with the added insult of having consumed a
//!    migration to get there.
//! 2. **Is the instance migratable?** COMPLETED / TERMINATED are refused (history, not live state).
//!    FAILED is ALLOWED and is in fact the prime use case: repair the model, migrate the corpse
//!    onto it, then decide what to resume. Migration never auto-resumes anything.
//! 3. **Does every LIVE LOCUS land on a compatible construct?** This is the substance. See
//!    [`LocusKind`] for the compatibility matrix and where each rule comes from.
//!
//! ## Live loci
//!
//! A "locus" is any node id the instance's durable state pins. They are read out of the durable
//! state, never guessed:
//!
//! | Locus | Read from |
//! |---|---|
//! | wait frontier | `sutra.waitingNodes` in the snapshot |
//! | parked message wait | a `waiting_event` row, `kind = MESSAGE`, `status = WAITING` |
//! | armed timer | a `waiting_event` row, `kind = TIMER`, `status = WAITING` |
//! | retry park | a TIMER row whose node also carries a `sutra.retry.<nodeId>` counter |
//! | continue-reply park | a TIMER row whose node carries `<q:reply continue>` in the SOURCE graph |
//! | routed start | `sutra.startNode` in the snapshot |
//! | retry budget | every `sutra.retry.<nodeId>` key in the snapshot |
//!
//! ## What v2 (F2) added
//!
//! * **Batch migration** — `POST /admin/instances/migrate` over a filtered population. Every
//!   instance validates, claims, moves and reports INDEPENDENTLY: one per-instance transaction, one
//!   per-instance claim, one per-instance entry in the report. See [`InstanceAttempt`] /
//!   [`batch_report`] for the partial-failure contract and [`BatchFilter`] for the selector.
//! * **Cross-PROCESS migration** — the target process id may differ, but ONLY under an explicit
//!   `nodeMapping` that names every live locus ([`MIGRATE_CROSS_PROCESS_UNMAPPED`]). Identity is
//!   never implicit across a process boundary.
//! * **Migrate-then-resume** — `resume: true` clears a FAILED instance's failure state and re-arms
//!   its parks IN THE MIGRATION'S OWN TRANSACTION, so it comes back through the ordinary
//!   claim-guarded resume paths. There is no new resume entry point, and no non-FAILED instance is
//!   ever "resumed" ([`MIGRATE_RESUME_NOT_FAILED`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sutra_bpmn::model::{Node, ProcessDefinition, ProcessModule};

// ---- structured codes (the `SUTRA.ADMIN.MIGRATE.*` family) ----------------------------------

/// The named target is not an ACTIVE deployment (unknown, DRAINING, or failed to register).
pub const MIGRATE_TARGET_NOT_ACTIVE: &str = "SUTRA.ADMIN.MIGRATE.TARGET_NOT_ACTIVE";
/// The named target is the deployment the instance is ALREADY pinned to — a no-op is a mistake,
/// not a migration, and is refused rather than silently rewriting the row for no reason.
pub const MIGRATE_TARGET_SAME_AS_SOURCE: &str = "SUTRA.ADMIN.MIGRATE.TARGET_SAME_AS_SOURCE";
/// The instance's own pin names a deployment whose graph the engine cannot load, so there is no
/// source model to validate the mapping against.
pub const MIGRATE_SOURCE_UNRESOLVABLE: &str = "SUTRA.ADMIN.MIGRATE.SOURCE_UNRESOLVABLE";
/// The instance's `processId` has no counterpart in the target deployment.
pub const MIGRATE_PROCESS_ABSENT: &str = "SUTRA.ADMIN.MIGRATE.PROCESS_ABSENT";
/// A live locus maps to a node id that does not exist in the target process.
pub const MIGRATE_NODE_UNMAPPED: &str = "SUTRA.ADMIN.MIGRATE.NODE_UNMAPPED";
/// A live locus maps to a node that EXISTS but is the wrong construct for what resume does there.
pub const MIGRATE_NODE_INCOMPATIBLE: &str = "SUTRA.ADMIN.MIGRATE.NODE_INCOMPATIBLE";
/// The supplied `nodeMapping` is itself unusable: it names a source node the instance does not pin,
/// or it folds two live loci onto one target node.
pub const MIGRATE_MAPPING_INVALID: &str = "SUTRA.ADMIN.MIGRATE.MAPPING_INVALID";
/// The instance already reached COMPLETED / TERMINATED — history, not live state.
pub const MIGRATE_INSTANCE_TERMINAL: &str = "SUTRA.ADMIN.MIGRATE.INSTANCE_TERMINAL";
/// Something else holds the instance's ownership claim (a resume in flight on this or another
/// replica). RETRY-SAFE and stamped as such: nothing was read, rewritten or committed. The admin
/// twin of `SUTRA.RUNTIME.RESUME.CLAIM_HELD`, which the relay/timer paths raise for the same
/// condition; the claim is bounded by `sutra.instance.claim-timeout` and cleared by the
/// stuck-instance sweeper, so a crashed owner never holds it forever.
pub const MIGRATE_CLAIM_HELD: &str = "SUTRA.ADMIN.MIGRATE.CLAIM_HELD";
/// A unique-live `<q:alias>` of this instance is already bound to a DIFFERENT live instance under
/// the target deployment, so carrying the alias over would break the unique-live guarantee.
pub const MIGRATE_ALIAS_CONFLICT: &str = "SUTRA.ADMIN.MIGRATE.ALIAS_CONFLICT";
/// The migration was validated but the durable move failed (a race, or a database fault).
pub const MIGRATE_COMMIT_FAILED: &str = "SUTRA.ADMIN.MIGRATE.COMMIT_FAILED";

/// A live locus has no `nodeMapping` entry on a CROSS-PROCESS migration. Distinct from
/// [`MIGRATE_NODE_UNMAPPED`], which means "the mapped id does not exist in the target": here the id
/// might well exist, and that is exactly the danger. Two processes that both declare `Approve` are
/// not thereby the same `Approve`, so identity is never implicit across a process boundary — every
/// live locus must be named by hand.
pub const MIGRATE_CROSS_PROCESS_UNMAPPED: &str = "SUTRA.ADMIN.MIGRATE.CROSS_PROCESS_UNMAPPED";
/// `resume: true` was asked for on an instance that is not FAILED. A SUSPENDED instance is not
/// stuck — it is parked, and it resumes when its correlation arrives or its timer fires. Clearing
/// "failure state" it does not have would be a no-op at best and a fabricated re-drive at worst.
pub const MIGRATE_RESUME_NOT_FAILED: &str = "SUTRA.ADMIN.MIGRATE.RESUME_NOT_FAILED";

/// WARNING — a node in the instance's replay-as-done set is absent from the target graph and was
/// not mapped. Not fatal (the completed set is history, not a live locus), but loud: replay-as-done
/// works by MATCHING ids, so an unmatched completed node will be executed again on the next resume.
pub const MIGRATE_COMPLETED_NODE_ABSENT: &str = "SUTRA.ADMIN.MIGRATE.COMPLETED_NODE_ABSENT";
/// WARNING — the snapshot's wait frontier names a node with no `waiting_event` row behind it. The
/// frontier is still migrated; the missing row means the locus could not be classified from durable
/// state and was validated as a message wait.
pub const MIGRATE_FRONTIER_WITHOUT_WAIT_ROW: &str = "SUTRA.ADMIN.MIGRATE.FRONTIER_WITHOUT_WAIT_ROW";
/// WARNING — the instance has pending outbox rows. They stay under the SOURCE pin by design (they
/// were minted by the source deployment's channel bindings and are dispatched against them; the
/// dispatcher covers draining deployments). Reported so the operator knows why.
pub const MIGRATE_OUTBOX_PENDING_ON_SOURCE: &str = "SUTRA.ADMIN.MIGRATE.OUTBOX_PENDING_ON_SOURCE";

// ---- the node index — the graph facts migration validates against ---------------------------

/// What one target node can be used for, distilled from the constructs the resume paths actually
/// accept. Deliberately a set of CAPABILITY flags rather than the BPMN element enum: the question
/// migration asks is never "is this a userTask" but "can a parked message wait resume here", and
/// several distinct constructs answer yes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeShape {
    /// The BPMN construct, for the human half of the report (`userTask`, `timerCatchEvent`, …).
    pub(crate) construct: &'static str,
    /// A parked MESSAGE wait can be resumed here by a relay: `<bpmn:userTask>`, an intermediate
    /// message catch, or a channel-call `<bpmn:serviceTask>` awaiting its correlated response.
    pub(crate) relay_resumable: bool,
    /// An armed TIMER can fire here: a timer catch event, a timer boundary event, or the
    /// `<taskId>#timeout` boundary a `<q:timeout>` synthesizes.
    pub(crate) timer_capable: bool,
    /// The node carries a `<q:retry>` policy, so a retry park can re-drive its task here.
    pub(crate) retryable_task: bool,
    /// The node carries `<q:reply continue="true">`, so a continue-reply park can self-resume here.
    pub(crate) continue_reply: bool,
    /// The node is a start event, so it can be a snapshot's routed `sutra.startNode`.
    pub(crate) start_event: bool,
}

/// One process's node shapes, keyed by node id.
pub(crate) type ProcessNodeIndex = BTreeMap<String, NodeShape>;

/// A whole deployment's graph facts: process id → its node shapes. Built once per plan at
/// activation and published for the admin surface, so validating a migration costs a map lookup
/// rather than a re-parse of two sealed archives.
#[derive(Debug, Clone, Default)]
pub struct DeploymentNodeIndex {
    processes: BTreeMap<String, ProcessNodeIndex>,
}

impl DeploymentNodeIndex {
    /// Build the index from a deployment's parsed BPMN modules.
    pub(crate) fn of_modules(modules: &[Arc<ProcessModule>]) -> DeploymentNodeIndex {
        let mut processes = BTreeMap::new();
        for module in modules {
            for process in module.processes() {
                let mut nodes = ProcessNodeIndex::new();
                index_process(process, &mut nodes);
                processes.insert(process.id.clone(), nodes);
            }
        }
        DeploymentNodeIndex { processes }
    }

    /// One process's node shapes, or `None` when the deployment does not declare that process.
    pub(crate) fn process(&self, process_id: &str) -> Option<&ProcessNodeIndex> {
        self.processes.get(process_id)
    }

    /// The declared process ids (diagnostics — a `PROCESS_ABSENT` refusal names what IS there).
    pub(crate) fn process_ids(&self) -> Vec<&str> {
        self.processes.keys().map(String::as_str).collect()
    }
}

/// The shared, activation-published index: deploymentId → its graph facts. Republished on every
/// flip alongside the OpenAPI specs, and covering the DRAINING set too — a migration's SOURCE is by
/// definition a deployment that is being flipped away from.
pub type SharedNodeIndex =
    Arc<std::sync::RwLock<std::collections::HashMap<String, Arc<DeploymentNodeIndex>>>>;

/// Index one process's nodes, flattening sub-process bodies into the same map. Flattening is
/// deliberate: a wait frontier can name a node inside an embedded sub-process, and the durable
/// state records the node id alone with no scope path, so the lookup has to be flat to match.
fn index_process(process: &ProcessDefinition, out: &mut ProcessNodeIndex) {
    for node in process.nodes() {
        out.insert(node.id().to_owned(), shape_of(process, node));
        // Loop wrappers carry the real activity inside them; the wrapper's own id is what the
        // frontier names, but the inner node's id can appear in the completed set.
        match node {
            Node::SubProcess { inner, .. }
            | Node::TransactionSubProcess { inner, .. }
            | Node::AdHocSubProcess { inner, .. }
            | Node::EventSubProcess { inner, .. } => index_process(inner, out),
            Node::MultiInstance { inner, .. } | Node::StandardLoop { inner, .. } => {
                out.insert(inner.id().to_owned(), shape_of(process, inner));
            }
            _ => {}
        }
    }
}

/// Classify one node into its [`NodeShape`]. The capability flags mirror what the resume paths do,
/// not what BPMN calls the element — see [`LocusKind`] for the resume behaviour each one gates.
fn shape_of(process: &ProcessDefinition, node: &Node) -> NodeShape {
    let id = node.id();
    let construct = construct_name(node);
    // `is_wait_state()` is the executor's own predicate for "the token parks here". Timer catches
    // are wait states too, so the relay-resumable set is the wait states MINUS the timer catch:
    // a relay arriving at a timer catch has nothing to correlate against.
    let relay_resumable = node.is_wait_state() && !matches!(node, Node::TimerCatchEvent { .. });
    let timer_capable = matches!(node, Node::TimerCatchEvent { .. })
        || matches!(
            node,
            Node::BoundaryEvent {
                kind: sutra_bpmn::model::BoundaryKind::Timer,
                ..
            }
        );
    NodeShape {
        construct,
        relay_resumable,
        timer_capable,
        retryable_task: process.has_retry_policy(id),
        continue_reply: process.has_continue_reply(id),
        start_event: matches!(node, Node::StartEvent { .. }),
    }
}

/// The BPMN construct name reported to the operator.
fn construct_name(node: &Node) -> &'static str {
    match node {
        Node::StartEvent { .. } => "startEvent",
        Node::EndEvent { .. } => "endEvent",
        Node::TerminateEndEvent { .. } => "terminateEndEvent",
        Node::ErrorEvent { .. } => "errorEndEvent",
        Node::IntermediateThrowEvent { .. } => "intermediateThrowEvent",
        Node::LinkCatchEvent { .. } => "linkCatchEvent",
        Node::MessageCatchEvent { .. } => "messageCatchEvent",
        Node::TimerCatchEvent { .. } => "timerCatchEvent",
        Node::BoundaryEvent { .. } => "boundaryEvent",
        Node::ServiceTask { .. } => "serviceTask",
        Node::DataTask { .. } => "dataTask",
        Node::ScriptTask { .. } => "scriptTask",
        Node::ManualTask { .. } => "manualTask",
        Node::SendTask { .. } => "sendTask",
        Node::BusinessRuleTask { .. } => "businessRuleTask",
        Node::UserTask { .. } => "userTask",
        Node::CallActivity { .. } => "callActivity",
        Node::SubProcess { .. } => "subProcess",
        Node::TransactionSubProcess { .. } => "transaction",
        Node::AdHocSubProcess { .. } => "adHocSubProcess",
        Node::EventSubProcess { .. } => "eventSubProcess",
        Node::CancelEndEvent { .. } => "cancelEndEvent",
        Node::ExclusiveGateway { .. } => "exclusiveGateway",
        Node::InclusiveGateway { .. } => "inclusiveGateway",
        Node::ParallelGateway { .. } => "parallelGateway",
        Node::ComplexGateway { .. } => "complexGateway",
        Node::MultiInstance { .. } => "multiInstance",
        Node::StandardLoop { .. } => "standardLoop",
    }
}

// ---- loci + the compatibility matrix --------------------------------------------------------

/// What kind of live locus a pinned node id is — and therefore what the target node must be able
/// to do. Every rule below is derived from what a resume ACTUALLY does with that locus, not from
/// BPMN taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LocusKind {
    /// A token parked awaiting a relay. Resume correlates an inbound to the instance and calls
    /// `resume` naming this node as the satisfied wait, which then joins the replay-as-done set and
    /// the flow continues from its outgoing edges. The target node must therefore be somewhere a
    /// token can legitimately be parked awaiting a message: `relay_resumable`.
    MessageWait,
    /// A token parked on an armed timer. The poller claims the due `waiting_event` row and calls
    /// `resume_timer`, which routes the fire through the timer node's own semantics (a catch
    /// continues the flow; a boundary interrupts its host). The target node must be a construct the
    /// timer path recognises: `timer_capable`.
    TimerWait,
    /// A `<q:retry>` backoff. The due timer's node is the FAILED SERVICE TASK itself: resume
    /// deliberately does NOT fold it into the replay-as-done set and re-runs the task. The target
    /// node must therefore still be a task carrying a retry policy — `retryable_task` — or the
    /// re-drive would either re-run a task with no budget or land on something that is not a task
    /// at all.
    RetryPark,
    /// A `<q:reply continue="true">` self-resume marker. The due timer's node is the reply task,
    /// which IS in the replay-as-done set; resume re-drives the parked tail after it. The target
    /// node must carry the same continue-reply binding — `continue_reply` — or the tail would be
    /// re-driven from a node that never parks.
    ContinueReplyPark,
    /// The snapshot's routed start event (multi-start replay). Resume passes it back as
    /// `start_node`, so the target node must be a `start_event`.
    RoutedStart,
    /// A `sutra.retry.<nodeId>` budget counter with no live park behind it (a task that failed,
    /// retried and succeeded leaves none; a task on another parallel branch can). The counter is
    /// meaningless on a node with no policy, and carrying it onto one would silently apply a burned
    /// budget to a task that never declared retries — `retryable_task`.
    RetryBudget,
}

impl LocusKind {
    /// The stable label used in the JSON report.
    pub(crate) fn label(self) -> &'static str {
        match self {
            LocusKind::MessageWait => "MESSAGE_WAIT",
            LocusKind::TimerWait => "TIMER_WAIT",
            LocusKind::RetryPark => "RETRY_PARK",
            LocusKind::ContinueReplyPark => "CONTINUE_REPLY_PARK",
            LocusKind::RoutedStart => "ROUTED_START",
            LocusKind::RetryBudget => "RETRY_BUDGET",
        }
    }

    /// Whether `shape` can host this locus — the compatibility matrix, in one place.
    pub(crate) fn accepts(self, shape: &NodeShape) -> bool {
        match self {
            LocusKind::MessageWait => shape.relay_resumable,
            LocusKind::TimerWait => shape.timer_capable,
            // A retry park is BOTH a frontier entry and a timer row on the task itself, so the
            // target must be the retryable task — never merely timer-capable.
            LocusKind::RetryPark | LocusKind::RetryBudget => shape.retryable_task,
            LocusKind::ContinueReplyPark => shape.continue_reply,
            LocusKind::RoutedStart => shape.start_event,
        }
    }

    /// What the target node would have to be, in words, for the refusal message.
    pub(crate) fn requirement(self) -> &'static str {
        match self {
            LocusKind::MessageWait => {
                "a relay-resumable wait construct (userTask, intermediate message catch event, or \
                 a channel-call serviceTask)"
            }
            LocusKind::TimerWait => {
                "a timer-capable construct (timer catch event, timer boundary event, or a \
                 <q:timeout>-synthesized #timeout boundary)"
            }
            LocusKind::RetryPark => {
                "a serviceTask carrying a <q:retry> policy (the park re-drives it)"
            }
            LocusKind::ContinueReplyPark => {
                "a node carrying <q:reply continue=\"true\"> (the park self-resumes its tail)"
            }
            LocusKind::RoutedStart => "a start event",
            LocusKind::RetryBudget => {
                "a node carrying a <q:retry> policy (it owns a burned attempt budget)"
            }
        }
    }
}

/// One live locus of the instance under validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Locus {
    pub(crate) kind: LocusKind,
    /// The node id as the SOURCE graph names it.
    pub(crate) source_node_id: String,
    /// The node id it maps to in the target (identity when the mapping does not name it).
    pub(crate) target_node_id: String,
}

/// The durable facts a validation runs over — assembled by the caller from the snapshot and the
/// wait rows so that [`validate`] itself is a pure function over data and needs no database.
#[derive(Debug, Clone, Default)]
pub(crate) struct InstanceFacts {
    pub(crate) process_id: String,
    pub(crate) status: String,
    /// `sutra.waitingNodes`.
    pub(crate) waiting_nodes: Vec<String>,
    /// `sutra.completedNodes`.
    pub(crate) completed_nodes: Vec<String>,
    /// `sutra.startNode` (empty when unset).
    pub(crate) start_node: String,
    /// `sutra.retry.<nodeId>` keys.
    pub(crate) retry_nodes: BTreeSet<String>,
    /// Live `waiting_event` rows: `(node_id, is_timer)`.
    pub(crate) wait_rows: Vec<(String, bool)>,
    /// Pending (undispatched) outbox rows — reported as a warning, never migrated.
    pub(crate) pending_outbox: u64,
}

/// One migration REQUEST, normalised once from the body and then applied verbatim to every
/// instance it names — one instance for `POST /admin/instances/{id}/migrate`, the whole selected
/// population for the batch. Holding it in one place is what makes "each instance validates
/// INDEPENDENTLY, against the same plan" a property of the code rather than a promise.
#[derive(Debug, Clone, Default)]
pub(crate) struct MigrationPlan {
    /// The caller's explicit `nodeMapping` (source node id → target node id).
    pub(crate) mapping: BTreeMap<String, String>,
    /// The process the instance is being re-homed INTO, when the caller named one. `None` — and a
    /// value equal to the instance's own process id — is an ordinary same-process migration.
    pub(crate) target_process_id: Option<String>,
    /// Validate only: take no claim, write nothing.
    pub(crate) dry_run: bool,
    /// After a successful move of a FAILED instance, clear its failure state and re-arm its parks
    /// so it comes back through the ordinary claim-guarded resume paths.
    pub(crate) resume: bool,
}

impl MigrationPlan {
    /// The process id an instance currently in `process_id` will be validated (and re-homed)
    /// against.
    pub(crate) fn target_process_for<'a>(&'a self, process_id: &'a str) -> &'a str {
        self.target_process_id
            .as_deref()
            .filter(|p| !p.is_empty())
            .unwrap_or(process_id)
    }

    /// Whether this plan genuinely re-homes an instance of `process_id` into a DIFFERENT process.
    /// Naming the instance's own process explicitly is not a cross-process migration.
    pub(crate) fn is_cross_process(&self, process_id: &str) -> bool {
        self.target_process_for(process_id) != process_id
    }

    /// The reported `mappingSource`.
    pub(crate) fn mapping_source(&self) -> &'static str {
        if self.mapping.is_empty() {
            "identity"
        } else {
            "explicit"
        }
    }
}

/// Which of an instance's `waiting_event` rows describe where it is PARKED — the input
/// [`InstanceFacts::wait_rows`] wants.
///
/// For a live instance that is simply the WAITING rows. A **FAILED** instance has none: the failure
/// commit resolves every live park in ONE statement, precisely so no timer refires and no relay
/// finds a live wait. Reading only WAITING rows would therefore leave a dead instance with no
/// classifiable park at all — every frontier entry would fall through to the "no row behind it"
/// branch and be validated as a MESSAGE wait, which is the wrong rule for a timer park, a retry
/// backoff or a continue-reply park. A FAILED instance is the migration's PRIME use case, so
/// getting that wrong is not a corner.
///
/// The rows the failure tore down are recoverable exactly: they share one `resolved_at` (one
/// statement stamped it) and it is the instance's latest, because nothing touches a FAILED
/// instance's rows afterwards. That is the same set `resume` re-arms — deliberately, so what
/// validation checked is exactly what a resume brings back.
///
/// Gated on a non-empty frontier: an instance with no durable park has nothing parked, and an
/// older SATISFIED wait is spent history, not a park to classify.
pub(crate) fn live_park_rows(
    status: &str,
    frontier: &[String],
    rows: &[sutra_persistence::stores::InstanceWait],
) -> Vec<(String, bool)> {
    use sutra_persistence::stores::{KIND_TIMER, STATUS_WAITING};

    let torn_down = (status == sutra_persistence::snapshot::STATUS_FAILED && !frontier.is_empty())
        .then(|| rows.iter().filter_map(|w| w.resolved_at).max())
        .flatten();
    rows.iter()
        .filter(|row| {
            row.status == STATUS_WAITING
                || (torn_down.is_some()
                    && row.resolved_at.is_some()
                    && row.resolved_at == torn_down)
        })
        .map(|row| (row.node_id.clone(), row.kind == KIND_TIMER))
        .collect()
}

/// One refusal or warning, machine-readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Finding {
    pub(crate) code: &'static str,
    pub(crate) source_node_id: Option<String>,
    pub(crate) target_node_id: Option<String>,
    pub(crate) detail: String,
}

impl Finding {
    fn of(code: &'static str, detail: impl Into<String>) -> Finding {
        Finding {
            code,
            source_node_id: None,
            target_node_id: None,
            detail: detail.into(),
        }
    }

    fn at(code: &'static str, source: &str, target: &str, detail: impl Into<String>) -> Finding {
        Finding {
            code,
            source_node_id: Some(source.to_owned()),
            target_node_id: Some(target.to_owned()),
            detail: detail.into(),
        }
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "code": self.code,
            "sourceNodeId": self.source_node_id,
            "targetNodeId": self.target_node_id,
            "detail": self.detail,
        })
    }
}

/// The full validation report — returned verbatim on a dry run, on a refusal, and on a success.
#[derive(Debug, Clone, Default)]
pub(crate) struct MigrationReport {
    pub(crate) loci: Vec<(Locus, Option<&'static str>)>,
    pub(crate) violations: Vec<Finding>,
    pub(crate) warnings: Vec<Finding>,
    /// The effective mapping — the caller's explicit entries plus identity for everything else.
    pub(crate) effective_mapping: BTreeMap<String, String>,
}

impl MigrationReport {
    pub(crate) fn valid(&self) -> bool {
        self.violations.is_empty()
    }

    pub(crate) fn loci_json(&self) -> Vec<serde_json::Value> {
        self.loci
            .iter()
            .map(|(locus, construct)| {
                serde_json::json!({
                    "kind": locus.kind.label(),
                    "sourceNodeId": locus.source_node_id,
                    "targetNodeId": locus.target_node_id,
                    "targetConstruct": construct,
                    "status": if construct.is_some() { "OK" } else { "VIOLATION" },
                })
            })
            .collect()
    }
}

/// Derive every live locus from the instance's durable facts, in a stable order.
///
/// Classification is by durable evidence, in precedence order, because one node id can be more than
/// one thing on paper and is exactly one thing in reality:
///
/// 1. a TIMER row whose node carries a retry counter and is NOT in the completed set ⇒ retry park
///    (the executor's own test — a retry park never records its task as done);
/// 2. a TIMER row whose node carries `<q:reply continue>` in the SOURCE graph ⇒ continue-reply park
///    (the timer path routes it to `resume` rather than `resume_timer` for the same reason);
/// 3. any other TIMER row ⇒ timer wait;
/// 4. any MESSAGE row ⇒ message wait.
///
/// A frontier entry with no row behind it is validated as a message wait and warned about; the
/// frontier is the resume-side truth, so it is never silently dropped.
pub(crate) fn derive_loci(
    facts: &InstanceFacts,
    source: Option<&ProcessNodeIndex>,
    mapping: &BTreeMap<String, String>,
    warnings: &mut Vec<Finding>,
) -> Vec<Locus> {
    let map_one =
        |id: &str| -> String { mapping.get(id).cloned().unwrap_or_else(|| id.to_owned()) };
    let completed: BTreeSet<&str> = facts.completed_nodes.iter().map(String::as_str).collect();
    let mut kinds: BTreeMap<String, LocusKind> = BTreeMap::new();

    for (node_id, is_timer) in &facts.wait_rows {
        let kind = if !is_timer {
            LocusKind::MessageWait
        } else if facts.retry_nodes.contains(node_id) && !completed.contains(node_id.as_str()) {
            LocusKind::RetryPark
        } else if source
            .and_then(|p| p.get(node_id))
            .is_some_and(|shape| shape.continue_reply)
        {
            LocusKind::ContinueReplyPark
        } else {
            LocusKind::TimerWait
        };
        kinds.insert(node_id.clone(), kind);
    }
    for node_id in &facts.waiting_nodes {
        if kinds.contains_key(node_id) {
            continue;
        }
        warnings.push(Finding::at(
            MIGRATE_FRONTIER_WITHOUT_WAIT_ROW,
            node_id,
            &map_one(node_id),
            format!(
                "the snapshot's wait frontier names '{node_id}' but no waiting_event row backs it; \
                 the locus is migrated and validated as a message wait"
            ),
        ));
        kinds.insert(node_id.clone(), LocusKind::MessageWait);
    }
    // Retry BUDGETS with no park behind them: the task already succeeded or is on another branch,
    // but the counter still travels and still has to land on a node that declares a policy.
    for node_id in &facts.retry_nodes {
        kinds
            .entry(node_id.clone())
            .or_insert(LocusKind::RetryBudget);
    }

    let mut loci: Vec<Locus> = kinds
        .into_iter()
        .map(|(source_node_id, kind)| Locus {
            kind,
            target_node_id: map_one(&source_node_id),
            source_node_id,
        })
        .collect();
    if !facts.start_node.is_empty() {
        loci.push(Locus {
            kind: LocusKind::RoutedStart,
            target_node_id: map_one(&facts.start_node),
            source_node_id: facts.start_node.clone(),
        });
    }
    loci.sort_by(|a, b| (a.kind, &a.source_node_id).cmp(&(b.kind, &b.source_node_id)));
    loci
}

/// Validate one migration: EVERY violation, never just the first.
///
/// `target` is the target PROCESS's node index — the instance's own process id under a
/// same-process migration, and `plan.target_process_id` under a cross-process one. `None` means the
/// target deployment does not declare that process at all, which is a single decisive refusal
/// (validating loci against a graph that is not there would produce a wall of noise).
pub(crate) fn validate(
    facts: &InstanceFacts,
    source: Option<&ProcessNodeIndex>,
    target: Option<&ProcessNodeIndex>,
    target_process_ids: &[&str],
    plan: &MigrationPlan,
) -> MigrationReport {
    let explicit_mapping = &plan.mapping;
    let mut report = MigrationReport::default();

    if facts.status == sutra_persistence::snapshot::STATUS_COMPLETED
        || facts.status == sutra_persistence::snapshot::STATUS_TERMINATED
    {
        report.violations.push(Finding::of(
            MIGRATE_INSTANCE_TERMINAL,
            format!(
                "instance already reached {} — a finished instance is retained history, not live \
                 state, and migrating it would rewrite the record of where it ran",
                facts.status
            ),
        ));
    }

    // `resume` is a convenience over the FAILED repair loop, not a general re-drive. A SUSPENDED
    // instance is not stuck: it is parked, and it resumes when its correlation arrives or its timer
    // fires. Refusing here (rather than quietly reporting `resumed: false`) keeps a caller who
    // batches `resume: true` over a mixed population from believing it woke anything.
    if plan.resume && facts.status != sutra_persistence::snapshot::STATUS_FAILED {
        report.violations.push(Finding::of(
            MIGRATE_RESUME_NOT_FAILED,
            format!(
                "'resume' was requested but the instance is {} — only a FAILED instance has \
                 failure state to clear and parks to re-arm; a suspended instance resumes by \
                 correlation or by its timers, with no operator action at all",
                facts.status
            ),
        ));
    }

    let loci = derive_loci(facts, source, explicit_mapping, &mut report.warnings);

    // CROSS-PROCESS: identity is never implicit across a process boundary. Two processes that both
    // declare `Approve` are not thereby the same `Approve`, so a re-home must name EVERY live locus
    // by hand — an accidental id collision between two unrelated graphs must never read as a
    // deliberate mapping.
    if plan.is_cross_process(&facts.process_id) {
        let to = plan.target_process_for(&facts.process_id);
        for locus in &loci {
            if !explicit_mapping.contains_key(&locus.source_node_id) {
                report.violations.push(Finding::at(
                    MIGRATE_CROSS_PROCESS_UNMAPPED,
                    &locus.source_node_id,
                    &locus.target_node_id,
                    format!(
                        "this is a CROSS-PROCESS migration ('{}' → '{to}'), so the {} at '{}' \
                         needs an explicit nodeMapping entry — identity is never implicit across \
                         process ids, even when the target happens to declare the same id",
                        facts.process_id,
                        locus.kind.label(),
                        locus.source_node_id
                    ),
                ));
            }
        }
    }

    // The mapping must name loci, not arbitrary nodes. A typo'd source id would otherwise be a
    // silent no-op — the operator believes they remapped a wait point and the identity mapping runs
    // instead, which is exactly the class of mistake this operation exists to make impossible.
    let locus_ids: BTreeSet<&str> = loci.iter().map(|l| l.source_node_id.as_str()).collect();
    let completed: BTreeSet<&str> = facts.completed_nodes.iter().map(String::as_str).collect();
    for (from, to) in explicit_mapping {
        if !locus_ids.contains(from.as_str()) && !completed.contains(from.as_str()) {
            report.violations.push(Finding::at(
                MIGRATE_MAPPING_INVALID,
                from,
                to,
                format!(
                    "nodeMapping names source node '{from}', which this instance neither parks at \
                     nor has completed — the entry would do nothing"
                ),
            ));
        }
    }
    // Two loci onto one target node would collapse two parked tokens into one row.
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for locus in &loci {
        if locus.kind == LocusKind::RoutedStart || locus.kind == LocusKind::RetryBudget {
            continue; // not a token position — a start node may legitimately equal a wait node
        }
        if let Some(previous) = seen.insert(&locus.target_node_id, &locus.source_node_id) {
            report.violations.push(Finding::at(
                MIGRATE_MAPPING_INVALID,
                &locus.source_node_id,
                &locus.target_node_id,
                format!(
                    "nodeMapping folds both '{previous}' and '{}' onto target node '{}' — two \
                     parked wait points cannot share one node",
                    locus.source_node_id, locus.target_node_id
                ),
            ));
        }
    }

    let Some(target) = target else {
        report.violations.push(Finding::of(
            MIGRATE_PROCESS_ABSENT,
            format!(
                "the target deployment declares no process '{}' (it declares: {})",
                plan.target_process_for(&facts.process_id),
                if target_process_ids.is_empty() {
                    "none".to_owned()
                } else {
                    target_process_ids.join(", ")
                }
            ),
        ));
        report.loci = loci.into_iter().map(|l| (l, None)).collect();
        report.effective_mapping = effective_mapping(&report.loci, facts, explicit_mapping);
        return report;
    };

    for locus in loci {
        match target.get(&locus.target_node_id) {
            None => {
                report.violations.push(Finding::at(
                    MIGRATE_NODE_UNMAPPED,
                    &locus.source_node_id,
                    &locus.target_node_id,
                    format!(
                        "the instance's {} at '{}' maps to '{}', which does not exist in the \
                         target process — supply a nodeMapping entry for it",
                        locus.kind.label(),
                        locus.source_node_id,
                        locus.target_node_id
                    ),
                ));
                report.loci.push((locus, None));
            }
            Some(shape) if !locus.kind.accepts(shape) => {
                report.violations.push(Finding::at(
                    MIGRATE_NODE_INCOMPATIBLE,
                    &locus.source_node_id,
                    &locus.target_node_id,
                    format!(
                        "the instance's {} at '{}' maps to '{}', which is a <{}> — this locus \
                         needs {}",
                        locus.kind.label(),
                        locus.source_node_id,
                        locus.target_node_id,
                        shape.construct,
                        locus.kind.requirement()
                    ),
                ));
                report.loci.push((locus, None));
            }
            Some(shape) => report.loci.push((locus, Some(shape.construct))),
        }
    }

    // Completed nodes are history, not loci — but replay-as-done matches them BY ID, so one that
    // is absent from the target will be executed again. A warning, loudly, rather than a refusal:
    // a target that legitimately dropped a node is a normal model evolution.
    for node in &facts.completed_nodes {
        let mapped = explicit_mapping
            .get(node)
            .cloned()
            .unwrap_or_else(|| node.clone());
        if !target.contains_key(&mapped) {
            report.warnings.push(Finding::at(
                MIGRATE_COMPLETED_NODE_ABSENT,
                node,
                &mapped,
                format!(
                    "completed node '{node}' maps to '{mapped}', which the target process does \
                     not declare; replay-as-done matches by id, so if the target still reaches \
                     that node it will be EXECUTED again on the next resume"
                ),
            ));
        }
    }

    if facts.pending_outbox > 0 {
        report.warnings.push(Finding::of(
            MIGRATE_OUTBOX_PENDING_ON_SOURCE,
            format!(
                "{} pending outbox row(s) stay pinned to the SOURCE deployment — they were minted \
                 by its channel bindings and are dispatched against them (the dispatcher covers \
                 draining deployments), so they drain where they were made",
                facts.pending_outbox
            ),
        ));
    }

    report.effective_mapping = effective_mapping(&report.loci, facts, explicit_mapping);
    report
}

/// The mapping that will actually be applied: every explicit entry, plus identity for every locus
/// and completed node the caller did not name. Materialised in full so the report and the audit
/// event record exactly what was rewritten rather than "identity, mostly".
fn effective_mapping(
    loci: &[(Locus, Option<&'static str>)],
    facts: &InstanceFacts,
    explicit: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for (locus, _) in loci {
        out.insert(locus.source_node_id.clone(), locus.target_node_id.clone());
    }
    for node in &facts.completed_nodes {
        out.entry(node.clone()).or_insert_with(|| node.clone());
    }
    for (from, to) in explicit {
        out.insert(from.clone(), to.clone());
    }
    out
}

// ---- batch migration (v2): the selector and the partial-failure contract ---------------------

/// The default number of instances one batch call will touch, and the ceiling an explicit `limit`
/// is clamped to.
///
/// A batch is bounded on purpose. It takes one claim and one transaction PER INSTANCE and it
/// retries nothing, so its worst-case runtime is a caller-visible multiple of `limit` — which is
/// only a usable promise if `limit` cannot be "all of them". An operator with more work than this
/// re-runs the call; the selector is deterministic, so the next run picks up where this one left
/// off (whatever moved is no longer under the source pin).
pub(crate) const BATCH_LIMIT_DEFAULT: i64 = 100;
/// See [`BATCH_LIMIT_DEFAULT`].
pub(crate) const BATCH_LIMIT_MAX: i64 = 1000;

/// Which instances a batch migration acts on.
///
/// `source_deployment_id` is REQUIRED and there is no "every deployment" mode: a migration names
/// one source graph and one target graph, and the node mapping that is correct for one source is
/// meaningless for another. Everything else narrows.
#[derive(Debug, Clone, Default)]
pub(crate) struct BatchFilter {
    /// The pin to migrate OFF. Required.
    pub(crate) source_deployment_id: String,
    /// Narrow to one archive-local process id.
    pub(crate) process_id: Option<String>,
    /// Narrow to one snapshot status — `SUSPENDED` or `FAILED`, the only two an instance can be
    /// migrated in.
    pub(crate) status: Option<String>,
    /// Consider retained TERMINAL rows too. They can never migrate; the flag exists so a caller
    /// gets an explicit per-instance `INSTANCE_TERMINAL` refusal instead of a silent omission.
    pub(crate) include_terminal: bool,
    /// Clamped to [`BATCH_LIMIT_MAX`], defaulting to [`BATCH_LIMIT_DEFAULT`].
    pub(crate) limit: i64,
}

impl BatchFilter {
    /// Echo the filter back in the report, so a saved response says exactly what it acted on.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "sourceDeploymentId": self.source_deployment_id,
            "processId": self.process_id,
            "status": self.status,
            "includeTerminal": self.include_terminal,
            "limit": self.limit,
        })
    }
}

/// What happened to ONE instance in a batch — the vocabulary the partial-failure contract is
/// written in. Every entry of a batch report carries exactly one of these, and the totals are a
/// count per variant, so a caller never has to infer an outcome from a combination of booleans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MigrationOutcome {
    /// The move committed (and, when `resume` was asked for, the instance came back parked).
    Migrated,
    /// A dry run that validated. Nothing was claimed, written or committed.
    DryRunValid,
    /// Validation refused it. The instance is untouched and re-running the call changes nothing
    /// until the model or the mapping does.
    Refused,
    /// Something else holds the instance's claim, or the validated move lost a race at commit
    /// time. **Retry-safe**: nothing was read, rewritten or committed, and re-running the call is
    /// the sanctioned fix.
    Bounced,
    /// The id resolved to no instance under the source pin.
    NotFound,
    /// The engine failed while handling this one instance (a database fault, an undecodable
    /// snapshot). Reported per instance; the batch carries on with the next.
    Error,
}

impl MigrationOutcome {
    /// The stable label in the report.
    pub(crate) fn label(self) -> &'static str {
        match self {
            MigrationOutcome::Migrated => "MIGRATED",
            MigrationOutcome::DryRunValid => "VALID",
            MigrationOutcome::Refused => "REFUSED",
            MigrationOutcome::Bounced => "BOUNCED",
            MigrationOutcome::NotFound => "NOT_FOUND",
            MigrationOutcome::Error => "ERROR",
        }
    }

    /// Whether re-running the same call is the sanctioned response. Only contention is retry-safe:
    /// a refusal needs a different request, and an error needs a look at the log.
    pub(crate) fn retry_safe(self) -> bool {
        matches!(self, MigrationOutcome::Bounced)
    }

    /// The HTTP status the SINGLE-instance endpoint answers with. The batch endpoint does not use
    /// it: a batch's status describes the batch, never its instances.
    pub(crate) fn http_status(self) -> u16 {
        match self {
            MigrationOutcome::Migrated | MigrationOutcome::DryRunValid => 200,
            MigrationOutcome::Refused => 422,
            MigrationOutcome::Bounced => 409,
            MigrationOutcome::NotFound => 404,
            MigrationOutcome::Error => 500,
        }
    }
}

/// One instance's attempt: the outcome, and the full per-instance report body.
///
/// The single-instance endpoint returns `report` with `outcome.http_status()`; the batch endpoint
/// collects one of these per instance and never lets one instance's fate become the call's. That
/// is the whole partial-failure contract, and it is a property of this type: an attempt cannot
/// abort the loop, because it IS the loop's return value.
#[derive(Debug, Clone)]
pub(crate) struct InstanceAttempt {
    pub(crate) instance_id: String,
    pub(crate) outcome: MigrationOutcome,
    /// Whether this instance was brought back live (`resume: true` on a FAILED instance that moved).
    pub(crate) resumed: bool,
    /// The report body — the same document the single-instance endpoint returns.
    pub(crate) report: serde_json::Value,
}

impl InstanceAttempt {
    /// The report body plus the batch-entry fields (`outcome`, `retrySafe`).
    pub(crate) fn to_entry_json(&self) -> serde_json::Value {
        let mut entry = self.report.clone();
        if let Some(map) = entry.as_object_mut() {
            map.insert(
                "instanceId".to_owned(),
                serde_json::Value::String(self.instance_id.clone()),
            );
            map.insert(
                "outcome".to_owned(),
                serde_json::Value::String(self.outcome.label().to_owned()),
            );
            map.insert(
                "retrySafe".to_owned(),
                serde_json::Value::Bool(self.outcome.retry_safe()),
            );
        }
        entry
    }
}

/// Shape the batch response: the plan that was applied, the per-instance entries, and the totals.
///
/// **The batch's HTTP status describes the BATCH, not its instances.** A run in which every single
/// instance refused still answers `200`, with each refusal in its own entry — because the batch
/// itself was accepted, executed to completion and reported in full, which is exactly what the
/// caller asked for. Scripts key on `totals`, never on the status line.
pub(crate) fn batch_report(
    filter: &BatchFilter,
    target_deployment_id: &str,
    plan: &MigrationPlan,
    attempts: &[InstanceAttempt],
) -> serde_json::Value {
    let count =
        |want: MigrationOutcome| attempts.iter().filter(|a| a.outcome == want).count() as u64;
    let resumed = attempts.iter().filter(|a| a.resumed).count() as u64;
    serde_json::json!({
        "fromDeploymentId": filter.source_deployment_id,
        "toDeploymentId": target_deployment_id,
        "toProcessId": plan.target_process_id,
        "dryRun": plan.dry_run,
        "resume": plan.resume,
        "mappingSource": plan.mapping_source(),
        "filter": filter.to_json(),
        "selected": attempts.len() as u64,
        "totals": {
            "migrated": count(MigrationOutcome::Migrated),
            "valid": count(MigrationOutcome::DryRunValid),
            "refused": count(MigrationOutcome::Refused),
            "bounced": count(MigrationOutcome::Bounced),
            "notFound": count(MigrationOutcome::NotFound),
            "errors": count(MigrationOutcome::Error),
            "resumed": resumed,
        },
        "instances": attempts
            .iter()
            .map(InstanceAttempt::to_entry_json)
            .collect::<Vec<_>>(),
        "note": batch_note(attempts),
    })
}

/// The one-line reading of the totals — present so an operator who scrolls past a hundred entries
/// still learns whether anything needs doing.
fn batch_note(attempts: &[InstanceAttempt]) -> String {
    let bounced = attempts
        .iter()
        .filter(|a| a.outcome == MigrationOutcome::Bounced)
        .count();
    let mut note = String::from(
        "each instance was validated, claimed and committed on its own — a refusal or a bounce \
         moves nothing and leaves that instance exactly as it was.",
    );
    if bounced > 0 {
        note.push_str(&format!(
            " {bounced} instance(s) BOUNCED — a live ownership claim, or a validated move that \
             lost a race at commit time; nothing moved for them either way. They are not retried \
             inside this call: a hidden retry loop on an admin surface would make the runtime \
             unbounded and the report a lie about when it was taken. Re-run the same request once \
             the claims clear (sutra.instance.claim-timeout bounds them)."
        ));
    }
    note
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(construct: &'static str) -> NodeShape {
        NodeShape {
            construct,
            relay_resumable: false,
            timer_capable: false,
            retryable_task: false,
            continue_reply: false,
            start_event: false,
        }
    }

    fn user_task() -> NodeShape {
        NodeShape {
            relay_resumable: true,
            ..shape("userTask")
        }
    }

    fn timer_catch() -> NodeShape {
        NodeShape {
            timer_capable: true,
            ..shape("timerCatchEvent")
        }
    }

    fn retry_task() -> NodeShape {
        NodeShape {
            retryable_task: true,
            ..shape("serviceTask")
        }
    }

    fn continue_reply_task() -> NodeShape {
        NodeShape {
            continue_reply: true,
            ..shape("serviceTask")
        }
    }

    fn start() -> NodeShape {
        NodeShape {
            start_event: true,
            ..shape("startEvent")
        }
    }

    fn index(entries: &[(&str, NodeShape)]) -> ProcessNodeIndex {
        entries
            .iter()
            .map(|(id, shape)| ((*id).to_owned(), shape.clone()))
            .collect()
    }

    fn mapping(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    }

    /// A same-process plan carrying just an explicit node mapping — what most cases need.
    fn plan(pairs: &[(&str, &str)]) -> MigrationPlan {
        MigrationPlan {
            mapping: mapping(pairs),
            ..MigrationPlan::default()
        }
    }

    fn parked_at_user_task() -> InstanceFacts {
        InstanceFacts {
            process_id: "p1".to_owned(),
            status: sutra_persistence::snapshot::STATUS_SUSPENDED.to_owned(),
            waiting_nodes: vec!["U".to_owned()],
            completed_nodes: vec!["S".to_owned()],
            start_node: "S".to_owned(),
            retry_nodes: BTreeSet::new(),
            wait_rows: vec![("U".to_owned(), false)],
            pending_outbox: 0,
        }
    }

    #[test]
    fn an_identity_migration_onto_an_identical_graph_validates() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&graph),
            Some(&graph),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report.valid(), "{:?}", report.violations);
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);
        assert_eq!(report.loci.len(), 2, "the wait and the routed start");
        assert_eq!(report.effective_mapping.get("U").unwrap(), "U");
    }

    #[test]
    fn a_renamed_wait_node_validates_only_with_a_mapping_entry() {
        let source = index(&[("S", start()), ("U", user_task())]);
        let target = index(&[("S", start()), ("U2", user_task())]);

        let unmapped = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&target),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!unmapped.valid());
        assert_eq!(unmapped.violations[0].code, MIGRATE_NODE_UNMAPPED);
        assert_eq!(unmapped.violations[0].source_node_id.as_deref(), Some("U"));

        let mapped = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&target),
            &["p1"],
            &plan(&[("U", "U2")]),
        );
        assert!(mapped.valid(), "{:?}", mapped.violations);
        assert_eq!(mapped.effective_mapping.get("U").unwrap(), "U2");
    }

    #[test]
    fn a_message_wait_may_not_land_on_a_timer_or_a_gateway() {
        let source = index(&[("S", start()), ("U", user_task())]);
        for wrong in [timer_catch(), shape("exclusiveGateway"), shape("endEvent")] {
            let target = index(&[("S", start()), ("U", wrong.clone())]);
            let report = validate(
                &parked_at_user_task(),
                Some(&source),
                Some(&target),
                &["p1"],
                &MigrationPlan::default(),
            );
            assert!(!report.valid(), "<{}> must be refused", wrong.construct);
            assert_eq!(report.violations[0].code, MIGRATE_NODE_INCOMPATIBLE);
        }
    }

    #[test]
    fn a_timer_park_needs_a_timer_capable_target() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["T".to_owned()],
            wait_rows: vec![("T".to_owned(), true)],
            start_node: String::new(),
            ..parked_at_user_task()
        };
        let source = index(&[("T", timer_catch())]);
        let ok = validate(
            &facts,
            Some(&source),
            Some(&index(&[("T", timer_catch())])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(ok.valid(), "{:?}", ok.violations);
        assert_eq!(ok.loci[0].0.kind, LocusKind::TimerWait);

        let bad = validate(
            &facts,
            Some(&source),
            Some(&index(&[("T", user_task())])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!bad.valid());
        assert_eq!(bad.violations[0].code, MIGRATE_NODE_INCOMPATIBLE);
    }

    #[test]
    fn a_retry_park_is_classified_from_the_counter_and_needs_a_retry_policy_target() {
        // A TIMER row on a node that carries an attempt counter and is NOT completed is the
        // executor's own definition of a retry backoff.
        let facts = InstanceFacts {
            waiting_nodes: vec!["charge".to_owned()],
            completed_nodes: vec!["S".to_owned()],
            retry_nodes: ["charge".to_owned()].into_iter().collect(),
            wait_rows: vec![("charge".to_owned(), true)],
            start_node: String::new(),
            ..parked_at_user_task()
        };
        let source = index(&[("charge", retry_task())]);
        let mut warnings = Vec::new();
        let loci = derive_loci(&facts, Some(&source), &BTreeMap::new(), &mut warnings);
        assert_eq!(loci[0].kind, LocusKind::RetryPark);

        // A timer-capable node is NOT enough — the park re-drives the task.
        let bad = validate(
            &facts,
            Some(&source),
            Some(&index(&[("charge", timer_catch())])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!bad.valid());
        assert_eq!(bad.violations[0].code, MIGRATE_NODE_INCOMPATIBLE);

        let ok = validate(
            &facts,
            Some(&source),
            Some(&index(&[("charge", retry_task())])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(ok.valid(), "{:?}", ok.violations);
    }

    #[test]
    fn a_retry_budget_with_no_park_still_has_to_land_on_a_retry_policy() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["U".to_owned()],
            completed_nodes: vec!["S".to_owned(), "charge".to_owned()],
            retry_nodes: ["charge".to_owned()].into_iter().collect(),
            wait_rows: vec![("U".to_owned(), false)],
            start_node: String::new(),
            ..parked_at_user_task()
        };
        let source = index(&[("U", user_task()), ("charge", retry_task())]);
        let report = validate(
            &facts,
            Some(&source),
            Some(&index(&[
                ("U", user_task()),
                ("charge", shape("serviceTask")),
            ])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!report.valid());
        assert_eq!(report.violations[0].code, MIGRATE_NODE_INCOMPATIBLE);
        assert_eq!(
            report.violations[0].source_node_id.as_deref(),
            Some("charge")
        );
    }

    #[test]
    fn a_continue_reply_park_is_classified_from_the_source_graph() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["reply".to_owned()],
            completed_nodes: vec!["S".to_owned(), "reply".to_owned()],
            wait_rows: vec![("reply".to_owned(), true)],
            start_node: String::new(),
            ..parked_at_user_task()
        };
        let source = index(&[("reply", continue_reply_task())]);
        let mut warnings = Vec::new();
        let loci = derive_loci(&facts, Some(&source), &BTreeMap::new(), &mut warnings);
        assert_eq!(loci[0].kind, LocusKind::ContinueReplyPark);

        let bad = validate(
            &facts,
            Some(&source),
            Some(&index(&[("reply", shape("serviceTask"))])),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!bad.valid());
        assert_eq!(bad.violations[0].code, MIGRATE_NODE_INCOMPATIBLE);
    }

    #[test]
    fn a_routed_start_must_map_onto_a_start_event() {
        let source = index(&[("S", start()), ("U", user_task())]);
        let target = index(&[("S", user_task()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&target),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(!report.valid());
        assert!(report.violations.iter().any(
            |v| v.code == MIGRATE_NODE_INCOMPATIBLE && v.source_node_id.as_deref() == Some("S")
        ));
    }

    #[test]
    fn every_violation_is_reported_not_just_the_first() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["U".to_owned(), "T".to_owned()],
            completed_nodes: vec!["S".to_owned()],
            start_node: "S".to_owned(),
            wait_rows: vec![("U".to_owned(), false), ("T".to_owned(), true)],
            ..parked_at_user_task()
        };
        let source = index(&[("S", start()), ("U", user_task()), ("T", timer_catch())]);
        // Target: U is gone, T is the wrong construct, S is not a start event. Three violations.
        let target = index(&[("S", user_task()), ("T", user_task())]);
        let report = validate(
            &facts,
            Some(&source),
            Some(&target),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert_eq!(report.violations.len(), 3, "{:?}", report.violations);
        let codes: BTreeSet<&str> = report.violations.iter().map(|v| v.code).collect();
        assert!(codes.contains(MIGRATE_NODE_UNMAPPED));
        assert!(codes.contains(MIGRATE_NODE_INCOMPATIBLE));
    }

    #[test]
    fn a_terminal_instance_is_a_validation_error_and_a_failed_one_is_not() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        for status in ["COMPLETED", "TERMINATED"] {
            let facts = InstanceFacts {
                status: status.to_owned(),
                ..parked_at_user_task()
            };
            let report = validate(
                &facts,
                Some(&graph),
                Some(&graph),
                &["p1"],
                &MigrationPlan::default(),
            );
            assert!(!report.valid());
            assert_eq!(report.violations[0].code, MIGRATE_INSTANCE_TERMINAL);
        }
        let failed = InstanceFacts {
            status: "FAILED".to_owned(),
            ..parked_at_user_task()
        };
        let report = validate(
            &failed,
            Some(&graph),
            Some(&graph),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report.valid(), "{:?}", report.violations);
    }

    #[test]
    fn an_absent_target_process_is_one_decisive_refusal_not_a_wall_of_noise() {
        let source = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&source),
            None,
            &["other"],
            &MigrationPlan::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].code, MIGRATE_PROCESS_ABSENT);
        assert!(report.violations[0].detail.contains("other"));
    }

    #[test]
    fn a_mapping_entry_naming_no_locus_is_refused_rather_than_silently_ignored() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&graph),
            Some(&graph),
            &["p1"],
            &plan(&[("typo", "U")]),
        );
        assert!(!report.valid());
        assert_eq!(report.violations[0].code, MIGRATE_MAPPING_INVALID);
    }

    #[test]
    fn a_mapping_that_folds_two_parked_waits_onto_one_node_is_refused() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["A".to_owned(), "B".to_owned()],
            wait_rows: vec![("A".to_owned(), false), ("B".to_owned(), false)],
            start_node: String::new(),
            ..parked_at_user_task()
        };
        let source = index(&[("A", user_task()), ("B", user_task())]);
        let target = index(&[("Z", user_task())]);
        let report = validate(
            &facts,
            Some(&source),
            Some(&target),
            &["p1"],
            &plan(&[("A", "Z"), ("B", "Z")]),
        );
        assert!(!report.valid());
        assert!(report
            .violations
            .iter()
            .any(|v| v.code == MIGRATE_MAPPING_INVALID && v.detail.contains("folds")));
    }

    #[test]
    fn a_completed_node_absent_from_the_target_warns_but_does_not_refuse() {
        let facts = InstanceFacts {
            completed_nodes: vec!["S".to_owned(), "gone".to_owned()],
            ..parked_at_user_task()
        };
        let source = index(&[
            ("S", start()),
            ("U", user_task()),
            ("gone", shape("serviceTask")),
        ]);
        let target = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &facts,
            Some(&source),
            Some(&target),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report.valid(), "{:?}", report.violations);
        assert_eq!(report.warnings.len(), 1);
        assert_eq!(report.warnings[0].code, MIGRATE_COMPLETED_NODE_ABSENT);
    }

    #[test]
    fn a_frontier_entry_with_no_wait_row_warns_and_is_still_validated() {
        let facts = InstanceFacts {
            waiting_nodes: vec!["U".to_owned(), "orphan".to_owned()],
            wait_rows: vec![("U".to_owned(), false)],
            ..parked_at_user_task()
        };
        let source = index(&[("S", start()), ("U", user_task()), ("orphan", user_task())]);
        let target = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &facts,
            Some(&source),
            Some(&target),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report
            .warnings
            .iter()
            .any(|w| w.code == MIGRATE_FRONTIER_WITHOUT_WAIT_ROW));
        // And it was still validated — the target has no `orphan`, so it is an unmapped locus.
        assert!(report
            .violations
            .iter()
            .any(|v| v.code == MIGRATE_NODE_UNMAPPED
                && v.source_node_id.as_deref() == Some("orphan")));
    }

    #[test]
    fn pending_outbox_rows_are_reported_as_staying_on_the_source() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        let facts = InstanceFacts {
            pending_outbox: 3,
            ..parked_at_user_task()
        };
        let report = validate(
            &facts,
            Some(&graph),
            Some(&graph),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report.valid());
        assert!(report
            .warnings
            .iter()
            .any(|w| w.code == MIGRATE_OUTBOX_PENDING_ON_SOURCE && w.detail.contains('3')));
    }

    // ---- v2: cross-process re-homing --------------------------------------------------------

    fn cross_process_plan(to: &str, pairs: &[(&str, &str)]) -> MigrationPlan {
        MigrationPlan {
            mapping: mapping(pairs),
            target_process_id: Some(to.to_owned()),
            ..MigrationPlan::default()
        }
    }

    #[test]
    fn naming_the_instances_own_process_is_not_a_cross_process_migration() {
        // Explicitness must not change semantics: `targetProcessId: "p1"` for an instance already in
        // `p1` is the same identity-mapped migration as omitting it, NOT a re-home that suddenly
        // demands an explicit entry for every locus.
        let plan = cross_process_plan("p1", &[]);
        assert!(!plan.is_cross_process("p1"));
        let graph = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&graph),
            Some(&graph),
            &["p1"],
            &plan,
        );
        assert!(report.valid(), "{:?}", report.violations);
    }

    #[test]
    fn a_cross_process_migration_refuses_every_locus_the_mapping_does_not_name() {
        // The target process declares the SAME ids — and that is precisely why identity must not be
        // implicit: `U` in `p2` is a different node that happens to share a name.
        let source = index(&[("S", start()), ("U", user_task())]);
        let target = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&target),
            &["p2"],
            &cross_process_plan("p2", &[]),
        );
        assert!(
            !report.valid(),
            "identity across process ids is never implicit"
        );
        let unmapped: Vec<&str> = report
            .violations
            .iter()
            .filter(|v| v.code == MIGRATE_CROSS_PROCESS_UNMAPPED)
            .filter_map(|v| v.source_node_id.as_deref())
            .collect();
        // In locus order (kind, then node id): the parked message wait, then the routed start.
        assert_eq!(unmapped, ["U", "S"], "EVERY live locus, not just the first");
        assert!(
            report.violations[0].detail.contains("p1"),
            "{:?}",
            report.violations[0]
        );
    }

    #[test]
    fn a_fully_mapped_cross_process_migration_validates_against_the_target_process() {
        // The mapping names every locus, and each target node is checked by the SAME matrix — the
        // process boundary changes who must be named, never what a locus is allowed to land on.
        let source = index(&[("S", start()), ("U", user_task())]);
        let target = index(&[("Begin", start()), ("Review", user_task())]);
        let plan = cross_process_plan("p2", &[("U", "Review"), ("S", "Begin")]);
        let ok = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&target),
            &["p2"],
            &plan,
        );
        assert!(ok.valid(), "{:?}", ok.violations);
        assert_eq!(ok.effective_mapping.get("U").unwrap(), "Review");

        // …and an incompatible target is still refused, mapped or not.
        let wrong = index(&[("Begin", start()), ("Review", timer_catch())]);
        let bad = validate(
            &parked_at_user_task(),
            Some(&source),
            Some(&wrong),
            &["p2"],
            &plan,
        );
        assert!(!bad.valid());
        assert!(bad
            .violations
            .iter()
            .any(|v| v.code == MIGRATE_NODE_INCOMPATIBLE));
    }

    #[test]
    fn a_cross_process_migration_onto_an_undeclared_process_names_the_process_it_asked_for() {
        let source = index(&[("S", start()), ("U", user_task())]);
        let report = validate(
            &parked_at_user_task(),
            Some(&source),
            None,
            &["p1", "other"],
            &cross_process_plan("nope", &[("U", "X"), ("S", "Y")]),
        );
        let absent = report
            .violations
            .iter()
            .find(|v| v.code == MIGRATE_PROCESS_ABSENT)
            .expect("PROCESS_ABSENT");
        assert!(absent.detail.contains("'nope'"), "{}", absent.detail);
        assert!(
            absent.detail.contains("other"),
            "and it names what IS there"
        );
    }

    // ---- v2: the resume flag ----------------------------------------------------------------

    #[test]
    fn resume_is_a_validation_error_on_anything_that_is_not_failed() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        let resume = MigrationPlan {
            resume: true,
            ..MigrationPlan::default()
        };
        for status in ["SUSPENDED", "RUNNING"] {
            let facts = InstanceFacts {
                status: status.to_owned(),
                ..parked_at_user_task()
            };
            let report = validate(&facts, Some(&graph), Some(&graph), &["p1"], &resume);
            assert!(!report.valid(), "{status} has no failure state to clear");
            assert!(report
                .violations
                .iter()
                .any(|v| v.code == MIGRATE_RESUME_NOT_FAILED));
        }
        // FAILED is the one status it is FOR.
        let failed = InstanceFacts {
            status: "FAILED".to_owned(),
            ..parked_at_user_task()
        };
        let report = validate(&failed, Some(&graph), Some(&graph), &["p1"], &resume);
        assert!(report.valid(), "{:?}", report.violations);
    }

    #[test]
    fn resume_on_a_terminal_instance_reports_both_reasons_not_the_first() {
        let graph = index(&[("S", start()), ("U", user_task())]);
        let facts = InstanceFacts {
            status: "COMPLETED".to_owned(),
            ..parked_at_user_task()
        };
        let report = validate(
            &facts,
            Some(&graph),
            Some(&graph),
            &["p1"],
            &MigrationPlan {
                resume: true,
                ..MigrationPlan::default()
            },
        );
        let codes: BTreeSet<&str> = report.violations.iter().map(|v| v.code).collect();
        assert!(codes.contains(MIGRATE_INSTANCE_TERMINAL));
        assert!(codes.contains(MIGRATE_RESUME_NOT_FAILED));
    }

    // ---- v2: reading a FAILED instance's parks out of the rows the failure tore down ---------

    fn wait_row(
        node_id: &str,
        kind: &str,
        status: &str,
        resolved_at: Option<time::OffsetDateTime>,
    ) -> sutra_persistence::stores::InstanceWait {
        sutra_persistence::stores::InstanceWait {
            node_id: node_id.to_owned(),
            process_id: "p1".to_owned(),
            kind: kind.to_owned(),
            status: status.to_owned(),
            timer_due_at: None,
            resolved_at,
        }
    }

    #[test]
    fn a_live_instances_parks_are_exactly_its_waiting_rows() {
        let rows = [
            wait_row("U", "MESSAGE", "WAITING", None),
            wait_row(
                "done",
                "MESSAGE",
                "RESOLVED",
                Some(time::OffsetDateTime::now_utc()),
            ),
        ];
        assert_eq!(
            live_park_rows("SUSPENDED", &["U".to_owned()], &rows),
            [("U".to_owned(), false)]
        );
    }

    #[test]
    fn a_failed_instances_parks_are_the_rows_its_failure_resolved_in_one_statement() {
        // The bug this closes: a FAILED instance has NO waiting rows, so a frontier timer park used
        // to be validated against the MESSAGE-wait rule and refused on a perfectly good timer node.
        let failure = time::OffsetDateTime::now_utc();
        let earlier = failure - time::Duration::minutes(5);
        let rows = [
            wait_row("collectDocs", "MESSAGE", "RESOLVED", Some(earlier)),
            wait_row("Wait", "TIMER", "RESOLVED", Some(failure)),
            wait_row("Wait#alt", "MESSAGE", "RESOLVED", Some(failure)),
        ];
        let parks = live_park_rows("FAILED", &["Wait".to_owned()], &rows);
        assert_eq!(
            parks,
            [("Wait".to_owned(), true), ("Wait#alt".to_owned(), false)],
            "the torn-down parks, classified by KIND — and not the wait satisfied five minutes \
             earlier"
        );
    }

    #[test]
    fn a_failed_instance_with_no_frontier_has_no_parks_to_recover() {
        // Nothing was parked, so an older satisfied wait must not be mistaken for a park.
        let rows = [wait_row(
            "collectDocs",
            "MESSAGE",
            "RESOLVED",
            Some(time::OffsetDateTime::now_utc()),
        )];
        assert!(live_park_rows("FAILED", &[], &rows).is_empty());
    }

    #[test]
    fn a_failed_timer_park_now_validates_against_the_timer_rule() {
        // End to end through the validator: the same instance, before and after the fix's input.
        let failure = time::OffsetDateTime::now_utc();
        let rows = [wait_row("Wait", "TIMER", "RESOLVED", Some(failure))];
        let facts = InstanceFacts {
            process_id: "p1".to_owned(),
            status: "FAILED".to_owned(),
            waiting_nodes: vec!["Wait".to_owned()],
            completed_nodes: vec!["S".to_owned()],
            start_node: String::new(),
            retry_nodes: BTreeSet::new(),
            wait_rows: live_park_rows("FAILED", &["Wait".to_owned()], &rows),
            pending_outbox: 0,
        };
        let graph = index(&[("S", start()), ("Wait", timer_catch())]);
        let report = validate(
            &facts,
            Some(&graph),
            Some(&graph),
            &["p1"],
            &MigrationPlan::default(),
        );
        assert!(report.valid(), "{:?}", report.violations);
        assert_eq!(report.loci[0].0.kind, LocusKind::TimerWait);
        assert!(
            report.warnings.is_empty(),
            "and no bogus 'frontier without a wait row' warning: {:?}",
            report.warnings
        );
    }

    // ---- v2: the batch partial-failure contract ---------------------------------------------

    fn attempt(id: &str, outcome: MigrationOutcome, resumed: bool) -> InstanceAttempt {
        InstanceAttempt {
            instance_id: id.to_owned(),
            outcome,
            resumed,
            report: serde_json::json!({ "instanceId": id, "migrated": outcome == MigrationOutcome::Migrated }),
        }
    }

    fn a_filter() -> BatchFilter {
        BatchFilter {
            source_deployment_id: "dep-0123456789abcdef01234567".to_owned(),
            process_id: Some("p1".to_owned()),
            status: None,
            include_terminal: false,
            limit: BATCH_LIMIT_DEFAULT,
        }
    }

    #[test]
    fn a_batch_report_counts_every_outcome_and_keeps_each_instances_own_verdict() {
        // The heart of the contract: one bounced and one refused instance neither hide the two that
        // moved nor infect them — the report is per instance, and the totals are a count, not a
        // verdict on the call.
        let attempts = [
            attempt("a", MigrationOutcome::Migrated, true),
            attempt("b", MigrationOutcome::Migrated, false),
            attempt("c", MigrationOutcome::Refused, false),
            attempt("d", MigrationOutcome::Bounced, false),
        ];
        let report = batch_report(
            &a_filter(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            &MigrationPlan::default(),
            &attempts,
        );

        assert_eq!(report["selected"], 4);
        assert_eq!(report["totals"]["migrated"], 2);
        assert_eq!(report["totals"]["refused"], 1);
        assert_eq!(report["totals"]["bounced"], 1);
        assert_eq!(report["totals"]["resumed"], 1);
        assert_eq!(report["totals"]["errors"], 0);
        assert_eq!(
            report["filter"]["sourceDeploymentId"],
            a_filter().source_deployment_id
        );
        assert_eq!(report["filter"]["processId"], "p1");

        let entries = report["instances"].as_array().expect("entries");
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0]["outcome"], "MIGRATED");
        assert_eq!(
            entries[0]["migrated"], true,
            "the per-instance body rides along whole"
        );
        assert_eq!(entries[2]["outcome"], "REFUSED");
        assert_eq!(entries[2]["retrySafe"], false);
        // Only contention is retry-safe: a refusal needs a different request, not the same one again.
        assert_eq!(entries[3]["outcome"], "BOUNCED");
        assert_eq!(entries[3]["retrySafe"], true);
    }

    #[test]
    fn a_bounced_instance_makes_the_note_say_re_run_rather_than_retrying_internally() {
        let quiet = batch_report(
            &a_filter(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            &MigrationPlan::default(),
            &[attempt("a", MigrationOutcome::Migrated, false)],
        );
        assert!(!quiet["note"].as_str().unwrap().contains("BOUNCED"));

        let contended = batch_report(
            &a_filter(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            &MigrationPlan::default(),
            &[attempt("a", MigrationOutcome::Bounced, false)],
        );
        let note = contended["note"].as_str().unwrap();
        assert!(note.contains("BOUNCED"), "{note}");
        assert!(note.contains("not retried"), "{note}");
    }

    #[test]
    fn an_empty_selection_is_an_honest_empty_report_not_an_error() {
        let report = batch_report(
            &a_filter(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            &MigrationPlan::default(),
            &[],
        );
        assert_eq!(report["selected"], 0);
        assert_eq!(report["totals"]["migrated"], 0);
        assert_eq!(report["instances"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn only_contention_is_retry_safe_and_only_the_single_endpoint_reads_a_status() {
        assert_eq!(MigrationOutcome::Migrated.http_status(), 200);
        assert_eq!(MigrationOutcome::DryRunValid.http_status(), 200);
        assert_eq!(MigrationOutcome::Refused.http_status(), 422);
        assert_eq!(MigrationOutcome::Bounced.http_status(), 409);
        assert_eq!(MigrationOutcome::NotFound.http_status(), 404);
        assert_eq!(MigrationOutcome::Error.http_status(), 500);
        for outcome in [
            MigrationOutcome::Migrated,
            MigrationOutcome::DryRunValid,
            MigrationOutcome::Refused,
            MigrationOutcome::NotFound,
            MigrationOutcome::Error,
        ] {
            assert!(!outcome.retry_safe(), "{}", outcome.label());
        }
        assert!(MigrationOutcome::Bounced.retry_safe());
    }
}
