//! The BPMN element-support matrix — and the golden it renders to
//! `rust/crates/sutra-bpmn/bpmn-support.md`.
//!
//! Why this lives in a test rather than a generator binary: the interesting property is not
//! "regenerate a page", it is "the page cannot silently fall behind the engine". Three
//! exhaustive `match`es give that at COMPILE time —
//!
//! * [`kind_of`] must name every [`Node`] variant, so a new node kind will not build until it
//!   is classified here;
//! * [`NodeKind::next`] chains the kinds into [`all_kinds`], so a new [`NodeKind`] will not
//!   build until it takes a place in the rendered order;
//! * [`row`] must describe every [`NodeKind`], so it cannot be listed without a description.
//!
//! — and the golden comparison below turns "someone changed a description" into a test
//! failure rather than a stale document. Re-bless after an intentional change with:
//!
//! ```text
//! SUTRA_BLESS=1 cargo test -p sutra-bpmn --test all bpmn_support_doc
//! ```
//!
//! The wait-state column is not merely asserted, it is CHECKED: [`wait_state_column_matches_engine`]
//! constructs the cheap-to-build variants and compares the column against the real
//! [`Node::is_wait_state`].

use std::path::PathBuf;

use sutra_bpmn::codes;
use sutra_bpmn::model::{BoundaryKind, DataMapping, ThrowKind};
use sutra_bpmn::{Node, TimerDefinition};

// ---------------------------------------------------------------------------
// The kind taxonomy
// ---------------------------------------------------------------------------

/// One entry per [`Node`] variant. Field-less, so it can be enumerated and rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeKind {
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
    ServiceTask,
    DataTask,
    ScriptTask,
    ManualTask,
    SendTask,
    BusinessRuleTask,
    UserTask,
    CallActivity,
    SubProcess,
    TransactionSubProcess,
    AdHocSubProcess,
    EventSubProcess,
    ExclusiveGateway,
    InclusiveGateway,
    ParallelGateway,
    ComplexGateway,
    MultiInstance,
    StandardLoop,
}

/// Classify a live node. Exhaustive on purpose: a new [`Node`] variant breaks this build.
fn kind_of(node: &Node) -> NodeKind {
    match node {
        Node::StartEvent { .. } => NodeKind::StartEvent,
        Node::EndEvent { .. } => NodeKind::EndEvent,
        Node::TerminateEndEvent { .. } => NodeKind::TerminateEndEvent,
        Node::ErrorEvent { .. } => NodeKind::ErrorEvent,
        Node::CancelEndEvent { .. } => NodeKind::CancelEndEvent,
        Node::IntermediateThrowEvent { .. } => NodeKind::IntermediateThrowEvent,
        Node::LinkCatchEvent { .. } => NodeKind::LinkCatchEvent,
        Node::MessageCatchEvent { .. } => NodeKind::MessageCatchEvent,
        Node::TimerCatchEvent { .. } => NodeKind::TimerCatchEvent,
        Node::BoundaryEvent { .. } => NodeKind::BoundaryEvent,
        Node::ServiceTask { .. } => NodeKind::ServiceTask,
        Node::DataTask { .. } => NodeKind::DataTask,
        Node::ScriptTask { .. } => NodeKind::ScriptTask,
        Node::ManualTask { .. } => NodeKind::ManualTask,
        Node::SendTask { .. } => NodeKind::SendTask,
        Node::BusinessRuleTask { .. } => NodeKind::BusinessRuleTask,
        Node::UserTask { .. } => NodeKind::UserTask,
        Node::CallActivity { .. } => NodeKind::CallActivity,
        Node::SubProcess { .. } => NodeKind::SubProcess,
        Node::TransactionSubProcess { .. } => NodeKind::TransactionSubProcess,
        Node::AdHocSubProcess { .. } => NodeKind::AdHocSubProcess,
        Node::EventSubProcess { .. } => NodeKind::EventSubProcess,
        Node::ExclusiveGateway { .. } => NodeKind::ExclusiveGateway,
        Node::InclusiveGateway { .. } => NodeKind::InclusiveGateway,
        Node::ParallelGateway { .. } => NodeKind::ParallelGateway,
        Node::ComplexGateway { .. } => NodeKind::ComplexGateway,
        Node::MultiInstance { .. } => NodeKind::MultiInstance,
        Node::StandardLoop { .. } => NodeKind::StandardLoop,
    }
}

impl NodeKind {
    /// Render order, as a chain. This is what makes [`all_kinds`] provably complete: the match
    /// is exhaustive, so a new variant cannot be added without being linked into the chain.
    /// (A `const ALL: &[NodeKind]` array would compile happily while missing an entry.)
    fn next(self) -> Option<NodeKind> {
        use NodeKind::*;
        Some(match self {
            StartEvent => EndEvent,
            EndEvent => TerminateEndEvent,
            TerminateEndEvent => ErrorEvent,
            ErrorEvent => CancelEndEvent,
            CancelEndEvent => IntermediateThrowEvent,
            IntermediateThrowEvent => LinkCatchEvent,
            LinkCatchEvent => MessageCatchEvent,
            MessageCatchEvent => TimerCatchEvent,
            TimerCatchEvent => BoundaryEvent,
            BoundaryEvent => ServiceTask,
            ServiceTask => DataTask,
            DataTask => ScriptTask,
            ScriptTask => ManualTask,
            ManualTask => SendTask,
            SendTask => BusinessRuleTask,
            BusinessRuleTask => UserTask,
            UserTask => CallActivity,
            CallActivity => SubProcess,
            SubProcess => TransactionSubProcess,
            TransactionSubProcess => AdHocSubProcess,
            AdHocSubProcess => EventSubProcess,
            EventSubProcess => ExclusiveGateway,
            ExclusiveGateway => InclusiveGateway,
            InclusiveGateway => ParallelGateway,
            ParallelGateway => ComplexGateway,
            ComplexGateway => MultiInstance,
            MultiInstance => StandardLoop,
            StandardLoop => return None,
        })
    }
}

fn all_kinds() -> Vec<NodeKind> {
    let mut out = vec![NodeKind::StartEvent];
    while let Some(next) = out.last().expect("seeded above").next() {
        out.push(next);
    }
    out
}

// ---------------------------------------------------------------------------
// The rows
// ---------------------------------------------------------------------------

/// Whether a token parks at this node — the stateful surface that makes a process ineligible
/// for `execute_sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// Never parks.
    No,
    /// Always parks.
    Yes,
    /// Parks only in the stated case.
    Sometimes(&'static str),
    /// Parks iff the wrapped/inner flow does.
    Inherited,
}

impl Wait {
    fn cell(self) -> String {
        match self {
            Wait::No => "no".to_string(),
            Wait::Yes => "**yes**".to_string(),
            Wait::Sometimes(when) => format!("**when** {when}"),
            Wait::Inherited => "inherited".to_string(),
        }
    }
}

struct Row {
    /// The BPMN 2.0 XML that produces this node.
    xml: &'static str,
    /// The `Node` variant it loads into.
    variant: &'static str,
    wait: Wait,
    notes: &'static str,
}

/// Describe every kind. Exhaustive: a new [`NodeKind`] breaks this build.
fn row(kind: NodeKind) -> Row {
    use NodeKind::*;
    match kind {
        StartEvent => Row {
            xml: "`<startEvent>` (± `<timerEventDefinition>`)",
            variant: "StartEvent",
            wait: Wait::No,
            notes: "The process entry point. Triggered EITHER by an inbound message on one of its \
                    `<q:source channel>` channels OR by a timer schedule — never both; a start \
                    declaring both trigger contracts is rejected at load. A timer start arms a \
                    durable schedule row at activation and fires with EMPTY variables.",
        },
        EndEvent => Row {
            xml: "`<endEvent>`",
            variant: "EndEvent",
            wait: Wait::No,
            notes: "Ends this token's path; other live tokens keep running.",
        },
        TerminateEndEvent => Row {
            xml: "`<endEvent>` + `<terminateEventDefinition>`",
            variant: "TerminateEndEvent",
            wait: Wait::No,
            notes: "Ends the whole instance, not just this token.",
        },
        ErrorEvent => Row {
            xml: "`<endEvent>` + `<errorEventDefinition>`",
            variant: "ErrorEvent",
            wait: Wait::No,
            notes: "Throws a BPMN error for a boundary / event sub-process to catch. An empty \
                    code means \"any error\".",
        },
        CancelEndEvent => Row {
            xml: "`<endEvent>` + `<cancelEventDefinition>`",
            variant: "CancelEndEvent",
            wait: Wait::No,
            notes: "Valid only inside a `<transaction>` — rolls its store transaction back.",
        },
        IntermediateThrowEvent => Row {
            xml: "`<intermediateThrowEvent>`",
            variant: "IntermediateThrowEvent",
            wait: Wait::No,
            notes: "Emit-and-continue. `ThrowKind` selects the flavour: none, compensate, \
                    message, signal, escalation, link. A message throw requires `<q:send>`.",
        },
        LinkCatchEvent => Row {
            xml: "`<intermediateCatchEvent>` + `<linkEventDefinition>`",
            variant: "LinkCatchEvent",
            wait: Wait::No,
            notes: "The synchronous goto target of a link throw — a jump, not a park. Each link \
                    name must have exactly one catch.",
        },
        MessageCatchEvent => Row {
            xml: "`<intermediateCatchEvent>` + `<messageEventDefinition>`",
            variant: "MessageCatchEvent",
            wait: Wait::Yes,
            notes: "Parks until a correlated inbound arrives on one of its channels. \
                    Correlation is by `<q:alias>`.",
        },
        TimerCatchEvent => Row {
            xml: "`<intermediateCatchEvent>` + `<timerEventDefinition>`",
            variant: "TimerCatchEvent",
            wait: Wait::Yes,
            notes: "Parks a durable TIMER `waiting_event` row due at the resolved instant. \
                    `<timeDuration>` and `<timeDate>` only — `<timeCycle>` is rejected here (a \
                    token cannot park at a node that fires repeatedly).",
        },
        BoundaryEvent => Row {
            xml: "`<boundaryEvent>`",
            variant: "BoundaryEvent",
            wait: Wait::No,
            notes: "Attached to an activity. `BoundaryKind`: Error, Compensation, Escalation, \
                    Cancel, Timer. A timer boundary needs a wait-capable host (channel-call \
                    serviceTask / userTask) — it fires when the host's park outlives the \
                    duration. `@cancelActivity=\"false\"` makes it non-interrupting.",
        },
        ServiceTask => Row {
            xml: "`<serviceTask implementation=\"…\">`",
            variant: "ServiceTask",
            wait: Wait::Sometimes("`@implementation` starts with `channel:`"),
            notes: "Routed on `@implementation` by task-kind precedence. A `channel:`-prefixed \
                    implementation is a channel call: it parks until the correlated response \
                    (or its `<q:timeout>` boundary). Anything else is a registered task or a \
                    template, and runs inline.",
        },
        DataTask => Row {
            xml: "`<serviceTask>` with data associations, no `@implementation`",
            variant: "DataTask",
            wait: Wait::No,
            notes: "Declarative: its store reads, FEEL assignments and store writes ARE its \
                    behaviour. No registered task is resolved.",
        },
        ScriptTask => Row {
            xml: "`<scriptTask>` + `<script>`",
            variant: "ScriptTask",
            wait: Wait::No,
            notes: "`<bpmn:script>` names a file in the module's `scripts/` folder (SRL).",
        },
        ManualTask => Row {
            xml: "`<manualTask>`",
            variant: "ManualTask",
            wait: Wait::No,
            notes: "A pure no-op pass-through — a modelling marker, not a wait state.",
        },
        SendTask => Row {
            xml: "`<sendTask>`",
            variant: "SendTask",
            wait: Wait::No,
            notes: "Emit-and-continue; its `<q:send>` is required (enforced at load).",
        },
        BusinessRuleTask => Row {
            xml: "`<businessRuleTask>`",
            variant: "BusinessRuleTask",
            wait: Wait::No,
            notes: "Evaluates a DMN decision file and merges the result into the variables.",
        },
        UserTask => Row {
            xml: "`<userTask>`",
            variant: "UserTask",
            wait: Wait::Yes,
            notes: "Parks until an external relay resumes the instance on one of its channels.",
        },
        CallActivity => Row {
            xml: "`<callActivity>`",
            variant: "CallActivity",
            wait: Wait::No,
            notes: "Invokes another process by `@calledElement` (optionally cross-module via a \
                    namespace). Data associations on the call itself are rejected — pass data \
                    through the called process's declared variables.",
        },
        SubProcess => Row {
            xml: "`<subProcess>`",
            variant: "SubProcess",
            wait: Wait::Inherited,
            notes: "Embedded: expanded inline and run synchronously by the inline runner. See \
                    \"Scope constraints\" — the inline runners cannot park mid-scope.",
        },
        TransactionSubProcess => Row {
            xml: "`<transaction>`",
            variant: "TransactionSubProcess",
            wait: Wait::Inherited,
            notes: "One store transaction around the inner flow; a `<cancelEventDefinition>` end \
                    rolls it back.",
        },
        AdHocSubProcess => Row {
            xml: "`<adHocSubProcess>`",
            variant: "AdHocSubProcess",
            wait: Wait::No,
            notes: "Activities with no enforced sequence flow, run in document order until a FEEL \
                    `<completionCondition>` holds.",
        },
        EventSubProcess => Row {
            xml: "`<subProcess triggeredByEvent=\"true\">`",
            variant: "EventSubProcess",
            wait: Wait::No,
            notes: "ERROR-TRIGGERED ONLY. Fires when an error escapes an activity in its \
                    enclosing scope and no boundary event catches it. Message/timer/signal \
                    triggers are rejected — those are wait states, so model them on the \
                    stateful surface instead.",
        },
        ExclusiveGateway => Row {
            xml: "`<exclusiveGateway>`",
            variant: "ExclusiveGateway",
            wait: Wait::No,
            notes: "XOR. At most one outgoing flow fires, chosen by FEEL condition in document \
                    order, with `@default` as the fallback.",
        },
        InclusiveGateway => Row {
            xml: "`<inclusiveGateway>`",
            variant: "InclusiveGateway",
            wait: Wait::No,
            notes: "OR-fork / OR-join.",
        },
        ParallelGateway => Row {
            xml: "`<parallelGateway>`",
            variant: "ParallelGateway",
            wait: Wait::No,
            notes: "AND-fork / AND-join.",
        },
        ComplexGateway => Row {
            xml: "`<complexGateway>`",
            variant: "ComplexGateway",
            wait: Wait::No,
            notes: "Inclusive-style fork plus a FEEL `<activationCondition>` N-of-M join.",
        },
        MultiInstance => Row {
            xml: "`<multiInstanceLoopCharacteristics>` (marker on an activity)",
            variant: "MultiInstance",
            wait: Wait::Inherited,
            notes: "Wraps the inner activity. Sequential or parallel, driven by \
                    `<loopCardinality>` or a `loopDataInputRef` collection, with an optional \
                    FEEL `<completionCondition>`.",
        },
        StandardLoop => Row {
            xml: "`<standardLoopCharacteristics>` (marker on an activity)",
            variant: "StandardLoop",
            wait: Wait::Inherited,
            notes: "Wraps the inner activity. Test-before (while) or test-after (repeat) on a \
                    FEEL `<loopCondition>`, bounded by `@loopMaximum`.",
        },
    }
}

// ---------------------------------------------------------------------------
// The non-node surface: rejected, ignored, and scope-constrained
// ---------------------------------------------------------------------------

/// Flow nodes the loader FAILS CLOSED on, verbatim from the loader's reject arm.
const REJECTED: &[(&str, &str)] = &[
    (
        "`<task>`",
        "An abstract task has no behaviour to execute — pick a concrete task type.",
    ),
    (
        "`<receiveTask>`",
        "Use `<intermediateCatchEvent>` + `<messageEventDefinition>`, or a `<userTask>`.",
    ),
    (
        "`<eventBasedGateway>`",
        "Racing wait states are not modelled; park on one catch event.",
    ),
    ("`<implicitThrowEvent>`", "Use `<intermediateThrowEvent>`."),
    (
        "`<event>`",
        "The abstract base element is not a concrete flow node.",
    ),
];

/// Elements the loader accepts and IGNORES — they carry no token flow.
const IGNORED: &[(&str, &str)] = &[
    ("`<dataObject>` / `<dataObjectReference>`", "Modelling-only; process data lives in `<q:variables>`."),
    ("`<dataStore>` / `<dataStoreReference>`", "Not a flow node, but `<dataStoreReference>` DOES carry `<q:store>`, which binds it to a durable key in a data store."),
    ("`<laneSet>` / `<lane>`, artifacts, `<documentation>`", "Inert process children."),
];

fn render() -> String {
    let mut s = String::new();
    s.push_str(
        "<!-- GENERATED by rust/crates/sutra-bpmn/tests/all/bpmn_support_doc.rs. Do not edit by \
         hand: re-bless with `SUTRA_BLESS=1 cargo test -p sutra-bpmn --test all bpmn_support_doc`. -->\n\n",
    );
    s.push_str("# BPMN 2.0 element support\n\n");
    s.push_str(
        "What the Sutra engine actually executes, generated from the `Node` enum in\n\
         [`src/model.rs`](src/model.rs) and the loader's reject arm in [`src/loader.rs`](src/loader.rs).\n\
         Three exhaustive `match`es make this page un-driftable: a new node kind does not compile\n\
         until it is described here.\n\n",
    );
    s.push_str(
        "This is the ONLY BPMN implementation. Nothing downstream forks it, extends it or\n\
         overrides it: a distribution adds codecs, transports, redactors and validators through\n\
         the SPI crates, never a node type, a gateway, or an execution semantic. So \"which BPMN\n\
         features does <that repo> have?\" needs no investigation — read this page. (Whether a\n\
         given checkout is at the same REVISION of the crate is a separate question, and a real\n\
         one: the trees are synced by copying, not by git.)\n\n",
    );
    s.push_str(
        "The **wait state** column is the load-bearing one: a process containing any node that\n\
         parks is stateful, and is ineligible for `execute_sync`. `inherited` means the node\n\
         parks iff its inner/wrapped flow does.\n\n",
    );

    s.push_str("## Supported flow nodes\n\n");
    s.push_str("| BPMN 2.0 XML | `Node` variant | Wait state | Notes |\n|---|---|---|---|\n");
    for kind in all_kinds() {
        let r = row(kind);
        s.push_str(&format!(
            "| {} | `{}` | {} | {} |\n",
            r.xml,
            r.variant,
            r.wait.cell(),
            r.notes.split_whitespace().collect::<Vec<_>>().join(" ")
        ));
    }

    s.push_str("\n## Rejected flow nodes\n\n");
    s.push_str(&format!(
        "The loader fails closed on these rather than ignoring them — diagnostic `{}`.\n\n",
        codes::CONFIG_BPMN_UNSUPPORTED_ELEMENT
    ));
    s.push_str("| BPMN 2.0 XML | Model it instead as |\n|---|---|\n");
    for (xml, why) in REJECTED {
        s.push_str(&format!("| {xml} | {why} |\n"));
    }

    s.push_str("\n## Accepted but inert\n\n");
    s.push_str("| BPMN 2.0 XML | Why it carries no token |\n|---|---|\n");
    for (xml, why) in IGNORED {
        s.push_str(&format!("| {xml} | {why} |\n"));
    }

    s.push_str("\n## Scope constraints\n\n");
    s.push_str(
        "The inline sub-process runners cannot park a durable wait state mid-scope, so the loader\n\
         rejects a durable park nested inside one:\n\n",
    );
    s.push_str("| Rejected combination | Diagnostic |\n|---|---|\n");
    s.push_str(&format!(
        "| A channel-call `<serviceTask>` or an `<intermediateCatchEvent>` timer inside \
         `<subProcess>`, `<transaction>`, `<adHocSubProcess>` or an event sub-process | `{}` |\n",
        codes::DISPATCH_TIMER_UNSUPPORTED
    ));
    s.push_str(&format!(
        "| `<q:retry>` on any node inside one of those sub-processes (a retry park is a durable \
         timer) | `{}` |\n",
        codes::CONFIG_BPMN_RETRY_NOT_APPLICABLE
    ));
    s.push_str(&format!(
        "| An event sub-process whose start event is not error-triggered | `{}` |\n",
        codes::PARSE_SUBPROCESS_UNSUPPORTED
    ));
    s.push_str(&format!(
        "| An `<intermediateCatchEvent>` with an event definition other than message / timer / \
         link | `{}` |\n",
        codes::PARSE_BPMN_UNSUPPORTED_CATCH_EVENT
    ));
    s.push_str("\nModel the parking node at the top level of the process instead.\n");

    s.push_str(
        "\n## The `q:` extension surface\n\n\
         Node types are only half the model; the `q:` namespace (`urn:sutra:q:1.0`) carries the\n\
         message binding, correlation, retry, store and audit contracts. `xsd/q.xsd` is\n\
         authoritative for its exact shape — read it, not a design doc, when you need the\n\
         attributes an element supports.\n",
    );
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bpmn-support.md")
}

#[test]
fn bpmn_support_doc_is_in_sync() {
    let rendered = render();
    let path = golden_path();

    if std::env::var_os("SUTRA_BLESS").is_some() {
        std::fs::write(&path, &rendered).expect("write bpmn-support.md");
        return;
    }

    let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        on_disk,
        rendered,
        "\n{} is out of sync with the engine.\nRe-bless it with:\n  \
         SUTRA_BLESS=1 cargo test -p sutra-bpmn --test all bpmn_support_doc\n",
        path.display()
    );
}

#[test]
fn every_kind_is_rendered_exactly_once() {
    let kinds = all_kinds();
    for (i, a) in kinds.iter().enumerate() {
        assert!(
            !kinds[i + 1..].contains(a),
            "{a:?} appears twice in the NodeKind::next chain"
        );
    }
    // A cheap floor so a truncated chain is loud rather than quietly short.
    assert!(
        kinds.len() >= 28,
        "the kind chain lost entries: {}",
        kinds.len()
    );
}

/// The `Wait` column is a claim about `Node::is_wait_state`. Check it against the real thing
/// for every variant cheap enough to construct — a doc that merely asserts itself is worthless.
#[test]
fn wait_state_column_matches_engine() {
    let id = |s: &str| s.to_string();
    let samples: Vec<Node> = vec![
        Node::EndEvent {
            id: id("n"),
            name: None,
        },
        Node::TerminateEndEvent {
            id: id("n"),
            name: None,
        },
        Node::ManualTask {
            id: id("n"),
            name: None,
        },
        Node::SendTask {
            id: id("n"),
            name: None,
        },
        Node::ScriptTask {
            id: id("n"),
            name: None,
            script_file: id("s.srl"),
        },
        Node::UserTask {
            id: id("n"),
            name: None,
            channels: vec![],
        },
        Node::MessageCatchEvent {
            id: id("n"),
            name: None,
            channels: vec![id("c")],
            message_ref: None,
        },
        Node::TimerCatchEvent {
            id: id("n"),
            name: None,
            timer: TimerDefinition::from_persisted("DURATION", "PT1S")
                .expect("PT1S is a valid duration"),
        },
        Node::ServiceTask {
            id: id("n"),
            name: None,
            implementation: id("channel:acquirer"),
            data_mapping: DataMapping::default(),
            params: vec![],
        },
        Node::ServiceTask {
            id: id("n"),
            name: None,
            implementation: id("some-registered-task"),
            data_mapping: DataMapping::default(),
            params: vec![],
        },
        Node::ExclusiveGateway {
            id: id("n"),
            name: None,
            default_flow_id: None,
        },
        Node::ParallelGateway {
            id: id("n"),
            name: None,
        },
        Node::LinkCatchEvent {
            id: id("n"),
            name: None,
            link_name: id("l"),
        },
        Node::IntermediateThrowEvent {
            id: id("n"),
            name: None,
            kind: ThrowKind::Signal,
            activity_ref: None,
            reference: Some(id("sig")),
        },
        Node::BoundaryEvent {
            id: id("n"),
            name: None,
            attached_to_ref: id("host"),
            kind: BoundaryKind::Error,
            error_code: None,
            escalation_code: None,
            interrupting: true,
            timer: None,
        },
    ];

    for node in &samples {
        let kind = kind_of(node);
        let claimed = row(kind).wait;
        let actual = node.is_wait_state();
        let expected = match (kind, claimed) {
            // The only Sometimes in the table: a channel-call serviceTask parks, others do not.
            (NodeKind::ServiceTask, Wait::Sometimes(_)) => {
                matches!(node, Node::ServiceTask { implementation, .. }
                    if implementation.starts_with("channel:"))
            }
            (_, Wait::Yes) => true,
            (_, Wait::No) => false,
            (_, Wait::Inherited) => continue,
            (_, Wait::Sometimes(_)) => continue,
        };
        assert_eq!(
            actual, expected,
            "bpmn-support.md claims {claimed:?} for {kind:?}, but Node::is_wait_state() says \
             {actual} for {node:?}"
        );
    }
}
