//! The synchronous token-graph executor (SYNC subset). Single
//! -threaded; each `execute_sync` call constructs its own internal state.
//!
//! serviceTask task-kind routing follows the contract precedence: reserved ops (`coverage:`) →
//! template suffix → decision suffix → `channel:` (a channel-call task PARKS; Rust-only) →
//! the registered task-function fallback (there is no serviceTask bean SPI).
//!
//! Behaviours worth calling out:
//! - Wait states (userTask / message catch) inside a driven graph raise
//!   `SUTRA.RUNTIME.UNEXPECTED` — the stateful path is `executeStateful`/`resume`.
//! - Template renders bind `responseBody` as a UTF-8 string (`FeelValue` has no bytes
//!   variant), which is byte-identical to the rendered payload on the reply path.
//! - Listener panics are swallowed via `catch_unwind` (listener failures never propagate).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use async_recursion::async_recursion;

use sutra_bpmn::model::{
    BoundaryKind, DataMapping, Node, ParamBinding, ProcessDefinition, SequenceFlow, StoreWrite,
    ThrowKind,
};
use sutra_bpmn::qbindings::{NodeBindings, OnNoMatch, ReplyBinding, ReplyMode, SendBinding};
use sutra_bpmn::SutraError;
use sutra_feel::FeelValue;

use crate::codes;
use crate::coverage::{CoverageCorrelation, CoverageFragment, CoverageMetricStore};
use crate::datastore::{DataStore, DataStoreRegistry, DataStoreTx, StoreError};
use crate::deployment::{ArtifactType, DeploymentId};
use crate::emission::{CloudEventLite, Emission, EmissionKind, EmissionSink};
use crate::error::ExecError;
use crate::listener::{
    DispatchEvent, ExecutionListener, InstanceEvent, ReplyEvent, TaskEvent, TimerEvent, TokenEvent,
};
use crate::registry::{
    AuthRef, AuthRefResolverRegistry, DecisionEngineRegistry, DecisionRegistry, ScriptRegistry,
    TaskContextView, TaskError, TaskRegistry, TemplateEngineRegistry, TemplateRegistry,
};
use crate::variables::{feel_to_json, json_to_feel, Variables};

/// Path coverage — the conventional store name a coverage-tracking module declares.
const COVERAGE_OP_PREFIX: &str = "coverage:";
/// The channel-call task prefix.
const CHANNEL_CALL_PREFIX: &str = "channel:";

type CondEval = dyn Fn(&str, &Variables) -> Result<bool, ExecError>;
type ValEval = dyn Fn(&str, &Variables) -> Result<FeelValue, ExecError>;
type ProcessResolver = dyn Fn(&str) -> Result<Option<Arc<ProcessDefinition>>, ExecError>;
type ModuleResolver = dyn Fn(&DeploymentId, &str) -> Option<Arc<ProcessDefinition>>;

/// A FEEL condition evaluator backed by `sutra-feel`'s boolean evaluation.
pub fn feel_condition_evaluator() -> impl Fn(&str, &Variables) -> Result<bool, ExecError> {
    |expr: &str, vars: &Variables| {
        sutra_feel::expressions::eval_boolean(expr, &vars.to_feel_context())
            .map_err(|e| ExecError::Diagnostic(SutraError::new(&e.code, e.message)))
    }
}

/// A FEEL value evaluator backed by `sutra-feel`'s value evaluation.
pub fn feel_value_evaluator() -> impl Fn(&str, &Variables) -> Result<FeelValue, ExecError> {
    |expr: &str, vars: &Variables| {
        sutra_feel::expressions::eval(expr, &vars.to_feel_context())
            .map_err(|e| ExecError::Diagnostic(SutraError::new(&e.code, e.message)))
    }
}

/// Outcome of a synchronous execution.
#[derive(Debug, Clone)]
pub struct ExecResult {
    /// UUID assigned by the executor.
    pub instance_id: String,
    /// Variable map captured at the end event.
    pub outputs: Variables,
    /// Node ids the token traversed.
    pub visited_nodes: HashSet<String>,
}

impl ExecResult {
    pub fn output(&self, name: &str) -> Option<&FeelValue> {
        self.outputs.get(name)
    }
}

/// A timer wait state scheduled by THIS execution pass (Rust-only):
/// an intermediate timer catch the token parked at, or a timer boundary armed on a parked
/// host task. The park step records it as a TIMER `waiting_event` row due at `due_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerWait {
    /// The timer node — a `TimerCatchEvent`, a BPMN timer boundary, or a synthesized
    /// `<taskId>#timeout` boundary.
    pub node_id: String,
    /// RFC 3339 due timestamp (computed from the ISO-8601 duration at park time).
    pub due_at: String,
}

/// Outcome of a stateful execution pass ([`TokenExecutor::execute_stateful_from`] /
/// [`TokenExecutor::resume`]). `Completed`
/// carries the end-event outputs exactly like [`ExecResult`]; `Suspended` carries the
/// snapshot the persistence layer encodes so a later relay can resume.
#[derive(Debug, Clone)]
pub enum StatefulExecResult {
    Completed {
        instance_id: String,
        outputs: Variables,
        visited_nodes: HashSet<String>,
    },
    Suspended {
        instance_id: String,
        /// Wait-state nodes the token parked at — the suspend frontier. Timer BOUNDARY
        /// nodes are NOT part of this frontier (they are not token positions); they ride
        /// `timer_waits` only. An intermediate timer catch appears in BOTH (it is the
        /// token position AND a timer row).
        waiting_nodes: Vec<String>,
        /// Union of nodes done before this pass and during it (insertion-ordered,
        /// deduplicated) — the snapshot's replay-as-done set.
        completed_nodes: Vec<String>,
        /// The full variable context at the quiescent point.
        variables: Variables,
        /// The routed start event of the ORIGINAL pass (multi-start replay).
        start_node: Option<String>,
        /// Timer wait rows scheduled FRESH by this pass — the park step commits
        /// them atomically with the snapshot. Timers armed in an earlier pass and still
        /// pending are NOT re-listed (their due-at must not reset on a re-park).
        timer_waits: Vec<TimerWait>,
        /// Respond-and-continue: true when this park is a `<q:reply continue>` detach —
        /// the dispatcher builds + flushes the produced reply (`responseBody`) to the caller now,
        /// and a due-now timer wait self-resumes the remaining nodes.
        detached_reply: bool,
        /// Per declared `<q:coverage>` path, the contiguous-prefix cursor at this quiescent
        /// point. The park step persists it under `sutra.coverage.<pathId>`; a later resume seeds
        /// its cursors from it so coverage marking survives suspend/resume (BTreeMap<pathId,count>).
        coverage_progress: BTreeMap<String, u64>,
        /// Per `<q:retry>` node id, the FAILED-attempt count at this quiescent point
        /// (BTreeMap<nodeId,attempts>). The park step persists it under `sutra.retry.<nodeId>` and
        /// a later resume seeds the executor back from it, so the attempt budget is durable rather
        /// than restarting on every re-drive. AUTHORITATIVE, not additive: a node absent here has
        /// no outstanding failed attempts (its task finally succeeded), so the persisted key is
        /// dropped rather than left to accumulate.
        retry_attempts: BTreeMap<String, u32>,
        /// Per CHANNEL-CALL `<q:retry>` node id sitting in a backoff window at this quiescent
        /// point, the classification code of the failure that parked it
        /// (BTreeMap<nodeId,code>). Persisted under `sutra.retryWait.<nodeId>` — the durable
        /// fact that distinguishes "this call's attempt is DEAD, a backoff timer will re-drive
        /// it" from "this call's attempt is IN FLIGHT, waiting on its response" (both park the
        /// node in `waiting_nodes` with an outstanding `retry_attempts` count, so without this
        /// marker the two states are snapshot-indistinguishable). The dispatcher keys on it: a
        /// relay for a marked node is refused (the response belongs to a dead attempt), and a
        /// due timer on a marked node is the backoff re-drive. AUTHORITATIVE like
        /// `retry_attempts`: a node absent here is not in a backoff window. Registered-task
        /// retries never set it (no in-flight wait exists to disambiguate from), so snapshots
        /// of processes without channel-call retries are byte-identical to pre-F1 ones.
        retry_backoff: BTreeMap<String, String>,
    },
}

impl StatefulExecResult {
    pub fn instance_id(&self) -> &str {
        match self {
            StatefulExecResult::Completed { instance_id, .. }
            | StatefulExecResult::Suspended { instance_id, .. } => instance_id,
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self, StatefulExecResult::Completed { .. })
    }
}

/// Builder-assembled synchronous executor.
pub struct TokenExecutor {
    tasks: TaskRegistry,
    condition_evaluator: Box<CondEval>,
    value_evaluator: Box<ValEval>,
    data_stores: Option<Box<DataStoreRegistry>>,
    /// The TYPED coverage-metric + reconstruction-fragment store — the ONLY coverage surface
    /// since the KV covered-set was retired. `mark_coverage`
    /// flips intra-process metric flags here and writes cross-process fragments here; the
    /// `coverage:report` / `coverage:reset` ops read and clear the same flags. `None` (no engine
    /// database) means the deployment has NO coverage — the ops then fail loudly rather than
    /// report a fictitious 0%.
    coverage_metric_store: Option<Rc<dyn CoverageMetricStore>>,
    listeners: Vec<Rc<dyn ExecutionListener>>,
    process_resolver: Option<Box<ProcessResolver>>,
    module_resolver: Option<Box<ModuleResolver>>,
    emissions: Option<Rc<dyn EmissionSink>>,
    auth_resolvers: Option<Box<AuthRefResolverRegistry>>,
    template_engines: TemplateEngineRegistry,
    /// The deployed artifact bytes / resolved outbound channels. `Arc` because they are
    /// immutable once an activation has built them (execution scale-out §2 row 10): one set is
    /// built per activation and every actor lane's executor points at it, instead of each lane
    /// re-copying the archives' template/script/decision bytes. (The template/decision ENGINES
    /// above stay per-lane — they carry a compiled-template cache, i.e. mutable state.)
    templates: Arc<TemplateRegistry>,
    scripts: Arc<ScriptRegistry>,
    decision_engines: DecisionEngineRegistry,
    decisions: Arc<DecisionRegistry>,
    outbound_channels: Arc<crate::registry::OutboundChannelRegistry>,
    uuid_supplier: Box<dyn Fn() -> String>,
    now_supplier: Box<dyn Fn() -> String>,
    /// B1 — the engine-default audit sink applied to a process that declares no `<q:audit sink>`
    /// (and has no manifest default). `None` = no default; such a process is not audited. Resolves
    /// the "single source of truth" sink for every lifecycle event the audit listener emits.
    default_audit_sink: Option<String>,
}

/// `TokenExecutor` builder — the executor's wiring surface.
pub struct Builder {
    executor: TokenExecutor,
}

impl TokenExecutor {
    pub fn builder(tasks: TaskRegistry) -> Builder {
        Builder {
            executor: TokenExecutor {
                tasks,
                // Walking-skeleton default: a present condition is treated as "always true".
                condition_evaluator: Box::new(|_, _| Ok(true)),
                value_evaluator: Box::new(|_, _| {
                    Err(ExecError::diag(
                        codes::RUNTIME_UNEXPECTED,
                        "No FEEL value evaluator is wired; a data-store key/assignment \
                         expression cannot be evaluated (call \
                         TokenExecutor.Builder#withValueEvaluator).",
                    ))
                }),
                data_stores: None,
                coverage_metric_store: None,
                listeners: Vec::new(),
                default_audit_sink: None,
                process_resolver: None,
                module_resolver: None,
                emissions: None,
                auth_resolvers: None,
                template_engines: TemplateEngineRegistry::new(),
                templates: Arc::new(TemplateRegistry::new()),
                scripts: Arc::new(ScriptRegistry::new()),
                decision_engines: DecisionEngineRegistry::new(),
                decisions: Arc::new(DecisionRegistry::new()),
                outbound_channels: Arc::new(crate::registry::OutboundChannelRegistry::new()),
                uuid_supplier: Box::new(new_uuid),
                now_supplier: Box::new(now_utc),
            },
        }
    }

    /// Execute a process to completion (the SYNC subset) with default deployment/labels. The
    /// method is `async` — its store ops await the async SPI; the caller drives it on
    /// the channel-engine actor's current-thread runtime.
    pub async fn execute_sync(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
    ) -> Result<ExecResult, ExecError> {
        self.execute_sync_from(
            process,
            initial_variables,
            DeploymentId::unresolved(),
            BTreeMap::new(),
            None,
        )
        .await
    }

    /// Execute with an explicit deployment identity + labels.
    pub async fn execute_sync_with(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
    ) -> Result<ExecResult, ExecError> {
        self.execute_sync_from(process, initial_variables, deployment, labels, None)
            .await
    }

    /// Execute starting from an explicit Start Event (multi-start routing); `None` uses the
    /// sole start event.
    pub async fn execute_sync_from(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
    ) -> Result<ExecResult, ExecError> {
        self.execute_sync_from_correlated(
            process,
            initial_variables,
            deployment,
            labels,
            start_node_id,
            CoverageCorrelation::default(),
        )
        .await
    }

    /// Like [`Self::execute_sync_from`] but threads the spawn's cross-process
    /// [`CoverageCorrelation`] (business key + trace id) onto the instance, so a
    /// desugar-injected cross-process coverage segment completed on the SYNC path writes a
    /// `coverage_fragment` the union-find can join — parity with
    /// [`Self::execute_stateful_from_correlated`] (without which a sync-eligible participant's
    /// segment fragment carries a NULL business key and never joins the cascade component).
    pub async fn execute_sync_from_correlated(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        correlation: CoverageCorrelation,
    ) -> Result<ExecResult, ExecError> {
        if !process.is_sync_eligible() {
            return Err(ExecError::diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Process {} has wait states and cannot run executeSync()",
                    process.id
                ),
            ));
        }

        let instance_id = (self.uuid_supplier)();
        // The `sutra.execute` waterfall span (an OTLP exporter subscribes to these spans
        // without touching this call site).
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_EXECUTE,
            deployment.id = %deployment.value(),
            process.id = %process.id,
            instance.id = %instance_id,
        )
        .entered();
        let ctx = Rc::new(RefCell::new(InnerCtx {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.clone(),
            module_id: process.id.clone(),
            module_version: String::new(),
            simulation: false,
            variables: initial_variables,
        }));

        let instance_event = InstanceEvent {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.clone(),
            process_id: process.id.clone(),
            module_version: process.module_version.clone(),
            audit_sink: self.effective_audit_sink(process),
        };
        self.notify(|l| l.on_instance_started(&instance_event));

        let mut state = ExecutionState::new(process, deployment, labels, instance_id, ctx);
        state.correlation = correlation;
        let start = match start_node_id {
            Some(s) if !s.trim().is_empty() => s.to_string(),
            _ => process.start_event()?.id().to_string(),
        };
        state.work.push_back(start);

        let outcome = self.run_to_quiescence(&mut state).await.and_then(|()| {
            if !state.reached_end {
                Err(Signal::Fatal(ExecError::diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!("Process {} did not reach an end event", process.id),
                )))
            } else {
                Ok(())
            }
        });
        if let Err(signal) = outcome {
            let e = signal_to_error(signal, &state);
            let d = e.to_diagnostic();
            self.notify(|l| l.on_instance_failed(&instance_event, &d));
            return Err(e);
        }

        self.mark_coverage(process, &state, &instance_event).await;
        self.notify(|l| l.on_instance_completed(&instance_event));
        Ok(ExecResult {
            instance_id: state.instance_id.clone(),
            outputs: state.end_outputs.clone(),
            visited_nodes: state.visited.clone(),
        })
    }

    // ---- stateful execution (S-X1 wait states) ----------------------------------------

    /// Run a STATEFUL process — one that may park at a wait state — to its next quiescent
    /// point: either `Completed` (an end event with no branch waiting) or `Suspended` (one
    /// or more branches parked; the result carries the snapshot needed to [`Self::resume`]).
    /// Unlike [`Self::execute_sync_from`] this does not require `is_sync_eligible()`.
    pub async fn execute_stateful_from(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
    ) -> Result<StatefulExecResult, ExecError> {
        self.execute_stateful_from_correlated(
            process,
            initial_variables,
            deployment,
            labels,
            start_node_id,
            CoverageCorrelation::default(),
        )
        .await
    }

    /// [`Self::execute_stateful_from`] carrying the inbound pass's coverage correlation dims
    /// — the spawn's trace-id + `<q:alias>` value, stamped onto any cross-process
    /// reconstruction fragment `mark_coverage` writes for this instance.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_stateful_from_correlated(
        &self,
        process: &ProcessDefinition,
        initial_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        correlation: CoverageCorrelation,
    ) -> Result<StatefulExecResult, ExecError> {
        let instance_id = (self.uuid_supplier)();
        // `sutra.execute` (stateful activation pass).
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_EXECUTE,
            deployment.id = %deployment.value(),
            process.id = %process.id,
            instance.id = %instance_id,
        )
        .entered();
        let ctx = Rc::new(RefCell::new(InnerCtx {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.clone(),
            module_id: process.id.clone(),
            module_version: String::new(),
            simulation: false,
            variables: initial_variables,
        }));

        let instance_event = InstanceEvent {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.clone(),
            process_id: process.id.clone(),
            module_version: process.module_version.clone(),
            audit_sink: self.effective_audit_sink(process),
        };
        self.notify(|l| l.on_instance_started(&instance_event));

        let mut state = ExecutionState::new(process, deployment, labels, instance_id, ctx);
        state.correlation = correlation;
        let start = match start_node_id {
            Some(s) if !s.trim().is_empty() => s.to_string(),
            _ => process.start_event()?.id().to_string(),
        };
        state.work.push_back(start);
        self.drive_stateful(state, &instance_event, start_node_id)
            .await
    }

    /// Re-enter a suspended instance once a relay satisfied one of its parked wait nodes:
    /// replay from the start event skipping the side-effect of every node in
    /// `completed_nodes` (and of `satisfied_wait_node`, now done), restoring
    /// `prior_variables` merged with `relay_variables` (relay wins on collision). Gateways
    /// re-evaluate deterministically over the restored variables; the instance then either
    /// COMPLETES or SUSPENDS again — the "replay skipping completed" resume contract.
    ///
    /// `prior_waiting` is the snapshot's wait frontier (satisfied node included is fine —
    /// its `prior_completed` fast-path wins): a still-waiting node reached again on this
    /// replay RE-PARKS without re-firing its park side-effects (a channel-call must not
    /// re-send its request; a pending timer must not reset its due-at).
    #[allow(clippy::too_many_arguments)]
    pub async fn resume(
        &self,
        process: &ProcessDefinition,
        instance_id: &str,
        completed_nodes: &[String],
        prior_variables: Variables,
        satisfied_wait_node: &str,
        relay_variables: &Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
    ) -> Result<StatefulExecResult, ExecError> {
        self.resume_inner(
            process,
            instance_id,
            completed_nodes,
            prior_variables,
            satisfied_wait_node,
            relay_variables,
            deployment,
            labels,
            start_node_id,
            prior_waiting,
            prior_coverage,
            prior_retry_attempts,
            prior_retry_backoff,
            None,
            None,
            false,
            CoverageCorrelation::default(),
        )
        .await
    }

    /// Re-drive a channel-call `<q:retry>` node whose BACKOFF TIMER came due: the node re-runs
    /// its park side-effects — a FRESH request emission, fresh timer boundaries, a fresh
    /// response wait — instead of the silent re-park an ordinary resume replay performs on a
    /// still-waiting node.
    ///
    /// A dedicated entry point rather than an inference inside [`Self::resume`], deliberately:
    /// for a channel-call node the snapshot facts alone cannot distinguish "the backoff came
    /// due" from "a relay arrived for the node mid-retry" (both see the node waiting with an
    /// outstanding attempt count), so the CALLER must derive the verdict from the durable
    /// `retry_backoff` marker it loaded under the instance claim, and say so explicitly here.
    /// (Registered-task re-drives keep riding [`Self::resume`]'s inference — no relay can ever
    /// target a registered task, so the inference is sound there and its contract is pinned.)
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_retry_redrive(
        &self,
        process: &ProcessDefinition,
        instance_id: &str,
        completed_nodes: &[String],
        prior_variables: Variables,
        redrive_node: &str,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
    ) -> Result<StatefulExecResult, ExecError> {
        self.resume_inner(
            process,
            instance_id,
            completed_nodes,
            prior_variables,
            redrive_node,
            &Variables::new(),
            deployment,
            labels,
            start_node_id,
            prior_waiting,
            prior_coverage,
            prior_retry_attempts,
            prior_retry_backoff,
            None,
            None,
            true,
            CoverageCorrelation::default(),
        )
        .await
    }

    /// Deliver a TASK FAILURE to a parked channel-call node from OUTSIDE the graph — today's
    /// one caller: the outbox terminally POISONED the node's request delivery
    /// (`sutra.outbox.retry.max-attempts` exhausted), so the in-flight attempt can never be
    /// answered. The replay reaches the parked node and offers the failure to its `<q:retry>`
    /// policy: budget left ⇒ a backoff park (the re-drive later RE-EMITS the request); budget
    /// spent or `code` non-retryable ⇒ the pass fails fatally and the dispatcher stamps the
    /// durable FAILED snapshot. The caller gates on the policy existing and on the durable
    /// poison evidence — a policy-less node is never routed here (its posture is unchanged:
    /// parked until its timeout boundary fires).
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_channel_call_failure(
        &self,
        process: &ProcessDefinition,
        instance_id: &str,
        failed_node: &str,
        failure_code: &str,
        failure_message: &str,
        completed_nodes: &[String],
        prior_variables: Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
    ) -> Result<StatefulExecResult, ExecError> {
        self.resume_inner(
            process,
            instance_id,
            completed_nodes,
            prior_variables,
            failed_node,
            &Variables::new(),
            deployment,
            labels,
            start_node_id,
            prior_waiting,
            prior_coverage,
            prior_retry_attempts,
            prior_retry_backoff,
            None,
            Some(ChannelCallFailure {
                node_id: failed_node.to_string(),
                code: failure_code.to_string(),
                message: failure_message.to_string(),
            }),
            false,
            CoverageCorrelation::default(),
        )
        .await
    }

    /// [`Self::resume`] carrying the relay pass's coverage correlation dims — the
    /// inbound message's trace-id + the wait node's `<q:alias>` correlation value, stamped onto any
    /// cross-process reconstruction fragment `mark_coverage` writes when this resume completes an
    /// injected segment.
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_correlated(
        &self,
        process: &ProcessDefinition,
        instance_id: &str,
        completed_nodes: &[String],
        prior_variables: Variables,
        satisfied_wait_node: &str,
        relay_variables: &Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
        correlation: CoverageCorrelation,
    ) -> Result<StatefulExecResult, ExecError> {
        self.resume_inner(
            process,
            instance_id,
            completed_nodes,
            prior_variables,
            satisfied_wait_node,
            relay_variables,
            deployment,
            labels,
            start_node_id,
            prior_waiting,
            prior_coverage,
            prior_retry_attempts,
            prior_retry_backoff,
            None,
            None,
            false,
            correlation,
        )
        .await
    }

    /// Re-enter a suspended instance because a TIMER
    /// fired. `fire.node_id` is the due TIMER `waiting_event` row's node:
    /// - an intermediate timer catch event ⇒ the wait is satisfied and the token follows
    ///   its outgoing flow (plain replay-skipping-completed, no relay payload);
    /// - an INTERRUPTING timer boundary ⇒ the host task's wait is cancelled and the token
    ///   leaves through the boundary's outgoing flows; a route-less boundary (the
    ///   `<q:timeout>` form) raises the `SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT` BPMN error
    ///   at the host (catchable; uncaught fails the instance closed).
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_timer(
        &self,
        process: &ProcessDefinition,
        fire: &crate::listener::TimerFire,
        completed_nodes: &[String],
        prior_variables: Variables,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
    ) -> Result<StatefulExecResult, ExecError> {
        let fired = match process.node(&fire.node_id) {
            Ok(Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Timer,
                attached_to_ref,
                ..
            }) => Some(FiredTimer {
                boundary_id: id.clone(),
                host_id: attached_to_ref.clone(),
            }),
            Ok(Node::TimerCatchEvent { .. }) => None,
            Ok(other) => {
                return Err(ExecError::diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!(
                        "resume_timer() fired node '{}' is a {} — not a timer boundary or \
                         an intermediate timer catch event",
                        fire.node_id,
                        node_type(other)
                    ),
                ));
            }
            Err(e) => return Err(ExecError::Diagnostic(e)),
        };
        let fired_event = TimerEvent {
            deployment: fire.deployment.clone(),
            labels: labels.clone(),
            instance_id: fire.instance_id.clone(),
            node_id: fire.node_id.clone(),
            due_at: fire.due_at.clone(),
        };
        self.notify(|l| l.on_timer_fired(&fired_event));
        self.resume_inner(
            process,
            &fire.instance_id,
            completed_nodes,
            prior_variables,
            &fire.node_id,
            &Variables::new(),
            fire.deployment.clone(),
            labels,
            start_node_id,
            prior_waiting,
            prior_coverage,
            prior_retry_attempts,
            prior_retry_backoff,
            fired,
            None,
            false,
            // A timer fire has no inbound message → no trace-id / correlation value.
            CoverageCorrelation::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resume_inner(
        &self,
        process: &ProcessDefinition,
        instance_id: &str,
        completed_nodes: &[String],
        prior_variables: Variables,
        satisfied_wait_node: &str,
        relay_variables: &Variables,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        start_node_id: Option<&str>,
        prior_waiting: &[String],
        prior_coverage: &BTreeMap<String, u64>,
        prior_retry_attempts: &BTreeMap<String, u32>,
        prior_retry_backoff: &BTreeMap<String, String>,
        fired_timer: Option<FiredTimer>,
        channel_call_failure: Option<ChannelCallFailure>,
        explicit_redrive: bool,
        correlation: CoverageCorrelation,
    ) -> Result<StatefulExecResult, ExecError> {
        if satisfied_wait_node.trim().is_empty() {
            return Err(ExecError::diag(
                codes::RUNTIME_UNEXPECTED,
                "resume() requires the id of the wait node being satisfied",
            ));
        }
        // `sutra.execute` (resume segment of the original instance).
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_EXECUTE,
            deployment.id = %deployment.value(),
            process.id = %process.id,
            instance.id = %instance_id,
            node.id = %satisfied_wait_node,
        )
        .entered();

        // Restored snapshot variables merged with the relay's (relay wins on key collision
        // — it is the fresher decision/payload).
        let mut merged = prior_variables;
        merged.merge(relay_variables);

        let ctx = Rc::new(RefCell::new(InnerCtx {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.to_string(),
            module_id: process.id.clone(),
            module_version: String::new(),
            simulation: false,
            variables: merged,
        }));

        let instance_event = InstanceEvent {
            deployment: deployment.clone(),
            labels: labels.clone(),
            instance_id: instance_id.to_string(),
            process_id: process.id.clone(),
            module_version: process.module_version.clone(),
            audit_sink: self.effective_audit_sink(process),
        };
        // No on_instance_started — this is a continuation of the original pass.
        self.notify(|l| l.on_instance_resumed(&instance_event));

        let mut state =
            ExecutionState::new(process, deployment, labels, instance_id.to_string(), ctx);
        state.correlation = correlation;
        // Is the satisfied node a channel-call task? Its retry re-drive discrimination is
        // EXPLICIT (the caller's verdict, from the durable backoff marker) — the inference
        // below is unsound for it, because a relay CAN name a channel-call node mid-retry.
        let satisfied_is_channel_call = matches!(
            process.node(satisfied_wait_node),
            Ok(Node::ServiceTask { implementation, .. })
                if implementation.starts_with(CHANNEL_CALL_PREFIX)
        );
        // A RETRY RE-DRIVE: the timer that came due is a `<q:retry>` task's backoff, not a
        // satisfied wait. It is the one resume whose "satisfied" node must NOT join the
        // replay-as-done set — the whole point is to run that task's side-effect AGAIN.
        //
        // For a REGISTERED task the condition is fully derivable from the snapshot the caller
        // already loaded, so no separate entry point is needed: the node carries a durable
        // failed-attempt count, and it is not in `completed_nodes` (a task that ever succeeded
        // is, and a retry park never records one). The inference is sound there because no
        // relay can ever target a registered task — the only resume naming it is the backoff
        // poller.
        //
        // For a CHANNEL-CALL task that inference is UNSOUND: a mid-retry relay (the response
        // to a live later attempt) also sees an outstanding attempt count on an uncompleted
        // node. There the verdict is `explicit_redrive` — the caller derived it from the
        // durable `sutra.retryWait.<nodeId>` marker under the instance claim
        // ([`Self::resume_retry_redrive`]) — and a relay resume walks past the node normally.
        let retry_redrive = if satisfied_is_channel_call {
            explicit_redrive
        } else {
            fired_timer.is_none()
                && prior_retry_attempts.contains_key(satisfied_wait_node)
                && !completed_nodes.iter().any(|n| n == satisfied_wait_node)
        };
        // A channel-call failure delivery is not a satisfied wait either: the node must be
        // REACHED (not replayed past) so its retry policy can rule on the failure.
        let failure_delivery = channel_call_failure.is_some();
        // Seed the prior-completed set: everything done before, plus the satisfied node.
        // A FIRED TIMER BOUNDARY is not a token position — the interception at the host
        // task routes it; only an in-flow satisfied node joins the replay-as-done set.
        for node in completed_nodes {
            state.push_prior_completed(node);
        }
        if fired_timer.is_none() && !retry_redrive && !failure_delivery {
            state.push_prior_completed(satisfied_wait_node);
        }
        // Carry the durable attempt budget forward. Every resume seeds it, not just a retry
        // re-drive: a parallel branch's relay can re-park an instance whose OTHER branch sits on a
        // retry timer, and the re-park's snapshot must not silently reset that branch's count to
        // zero (which would hand it a fresh budget on every unrelated resume).
        state.retry_attempts = prior_retry_attempts.clone();
        // Same carry-forward rule for the channel-call backoff markers, minus the one this
        // pass CONSUMES: the explicit re-drive ends its node's backoff window (the fresh
        // attempt it emits is in flight, no longer dead), and the marker is re-set only if a
        // later failure parks the node again.
        state.retry_backoff = prior_retry_backoff.clone();
        if retry_redrive {
            state.retry_backoff.remove(satisfied_wait_node);
        }
        // A correlated RELAY satisfying a channel-call node is that attempt SUCCEEDING: drop
        // its outstanding attempt count (exactly as a registered task's success does inside
        // `run_service_task`) so the `sutra.retry.<nodeId>` key does not ride every later
        // snapshot as dead weight — and, defensively, any stale backoff marker with it (the
        // dispatcher refuses relays on marked nodes, so the marker should already be absent).
        if satisfied_is_channel_call && !retry_redrive && !failure_delivery && fired_timer.is_none()
        {
            state.retry_attempts.remove(satisfied_wait_node);
            state.retry_backoff.remove(satisfied_wait_node);
        }
        state.channel_call_failure = channel_call_failure;
        if retry_redrive && satisfied_is_channel_call {
            state.channel_call_redrive = Some(satisfied_wait_node.to_string());
        }
        // Pending timer boundaries this resume retires BEFORE they fire: on a relay
        // resume, every timer boundary armed on the satisfied host; on a timer fire, its
        // sibling boundaries. The persistence layer resolves the rows in the same step;
        // this is the listener-side signal (due_at is not re-read here — empty).
        let cancelled_host = fired_timer
            .as_ref()
            .map(|f| f.host_id.clone())
            .unwrap_or_else(|| satisfied_wait_node.to_string());
        for n in process.nodes() {
            if let Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Timer,
                attached_to_ref,
                ..
            } = n
            {
                if attached_to_ref == &cancelled_host && id != satisfied_wait_node {
                    let event = TimerEvent {
                        deployment: state.deployment.clone(),
                        labels: state.labels.clone(),
                        instance_id: instance_id.to_string(),
                        node_id: id.clone(),
                        due_at: String::new(),
                    };
                    self.notify(|l| l.on_timer_cancelled(&event));
                }
            }
        }
        state.fired_timer = fired_timer;
        // A node still waiting from the prior pass re-parks WITHOUT re-firing its park
        // side-effects when the replay reaches it again — EXCEPT the node an explicit
        // channel-call re-drive targets: its prior park is the DEAD attempt's, and the whole
        // point of the re-drive is to re-fire the park side-effects (a fresh request emission,
        // fresh timer boundaries). Leaving it out of `prior_waiting` is what routes
        // `run_channel_call` onto its fresh-park path.
        for node in prior_waiting {
            if state.channel_call_redrive.as_deref() == Some(node.as_str()) {
                continue;
            }
            state.prior_waiting.insert(node.clone());
        }
        // Seed each coverage path's cursor from the prior snapshot's persisted counters so
        // the contiguous prefix walked in earlier passes is not lost when this pass's replay
        // diverges from the historically-taken route.
        state.seed_coverage_progress(prior_coverage);
        let start = match start_node_id {
            Some(s) if !s.trim().is_empty() => s.to_string(),
            _ => process.start_event()?.id().to_string(),
        };
        state.work.push_back(start);
        self.drive_stateful(state, &instance_event, start_node_id)
            .await
    }

    /// Shared worklist driver for the stateful path: run to quiescence, then classify the
    /// terminal state — suspend takes precedence over a sibling branch's end event.
    async fn drive_stateful(
        &self,
        mut state: ExecutionState<'_>,
        instance_event: &InstanceEvent,
        start_node_id: Option<&str>,
    ) -> Result<StatefulExecResult, ExecError> {
        if let Err(signal) = self.run_to_quiescence(&mut state).await {
            let e = signal_to_error(signal, &state);
            let d = e.to_diagnostic();
            self.notify(|l| l.on_instance_failed(instance_event, &d));
            return Err(e);
        }

        if !state.waiting.is_empty() {
            self.notify(|l| l.on_instance_suspended(instance_event));
            // Union of nodes done before this pass and during it — insertion-ordered,
            // deduplicated (an insertion-ordered set).
            let mut completed = state.prior_completed_ordered.clone();
            let mut seen: HashSet<String> = completed.iter().cloned().collect();
            for id in &state.completed_activities {
                if seen.insert(id.clone()) {
                    completed.push(id.clone());
                }
            }
            return Ok(StatefulExecResult::Suspended {
                instance_id: state.instance_id.clone(),
                waiting_nodes: state.waiting.clone(),
                completed_nodes: completed,
                variables: state.ctx.borrow().variables.clone(),
                start_node: start_node_id
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.to_string()),
                timer_waits: state.timer_waits.clone(),
                detached_reply: state.detached_reply,
                coverage_progress: state.coverage_progress(),
                retry_attempts: state.retry_attempts.clone(),
                retry_backoff: state.retry_backoff.clone(),
            });
        }
        if !state.reached_end {
            let e = ExecError::diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Process {} neither reached an end event nor suspended at a wait state",
                    state.process.id
                ),
            );
            let d = e.to_diagnostic();
            self.notify(|l| l.on_instance_failed(instance_event, &d));
            return Err(e);
        }
        self.mark_coverage(state.process, &state, instance_event)
            .await;
        self.notify(|l| l.on_instance_completed(instance_event));
        Ok(StatefulExecResult::Completed {
            instance_id: state.instance_id.clone(),
            outputs: state.end_outputs.clone(),
            visited_nodes: state.visited.clone(),
        })
    }

    // ---- worklist driver ------------------------------------------------------

    /// Drain the worklist, routing BPMN error signals to their boundary handlers.
    async fn run_to_quiescence(&self, state: &mut ExecutionState<'_>) -> Result<(), Signal> {
        while let Some(node_id) = state.work.pop_front() {
            match self.run_node(&node_id, state).await {
                Ok(()) => {}
                Err(Signal::BpmnError { source, code }) => {
                    self.route_error(&source, &code, state).await?;
                }
                Err(other) => return Err(other),
            }
        }
        Ok(())
    }

    // ---- node dispatch ---------------------------------------------------------

    #[async_recursion(?Send)]
    async fn run_node(&self, node_id: &str, state: &mut ExecutionState<'_>) -> Result<(), Signal> {
        let process = state.process;
        let node = process.node(node_id).map_err(fatal)?;

        // Replay fast-path: on a resume pass, a node completed in a prior pass (a
        // side-effecting activity, or the wait node the relay just satisfied) is walked
        // past without re-firing its side-effect or its token events — only its outgoing
        // flow is followed. Gateways and start/end events are never in prior_completed, so
        // they always re-evaluate deterministically over the restored variables. This is
        // what makes resume "replay skipping completed".
        if state.prior_completed.contains(node_id) {
            self.enqueue_outgoing(node_id, state);
            state.visited.insert(node_id.to_string());
            return Ok(());
        }

        // B1 — resolve this node's audit routing: the process's single sink (None = node
        // suppressed via `capture="none"`, or the process is not audited) + the redacted payload
        // when the process captures at payload level.
        let (audit_sink, payload_json) = self.resolve_token_audit(node_id, state);
        let token_event = TokenEvent {
            deployment: state.deployment.clone(),
            labels: state.labels.clone(),
            instance_id: state.instance_id.clone(),
            node_id: node_id.to_string(),
            node_type: node_type(node).to_string(),
            audit_sink,
            payload_json,
        };
        self.notify(|l| l.on_token_entered(&token_event));

        let mut left_handled = false;

        match node {
            Node::StartEvent { .. } => self.enqueue_outgoing(node_id, state),
            // Channel-call task: input mapping → collect the outbound
            // request → PARK keyed by the declared alias. The token does not leave.
            Node::ServiceTask {
                id,
                implementation,
                data_mapping,
                params,
                ..
            } if implementation.starts_with(CHANNEL_CALL_PREFIX) => {
                let parked =
                    self.run_channel_call(id, implementation, data_mapping, params, state)?;
                // Parked ⇒ the token did not leave (on_token_left suppressed, not
                // visited). A fired timer boundary routes the token OUT instead.
                left_handled = parked;
            }
            Node::ServiceTask {
                id,
                implementation,
                data_mapping,
                params,
                ..
            } => {
                let retry_parked = self
                    .run_service_task(
                        id,
                        implementation,
                        data_mapping,
                        params,
                        state.retry_parkable,
                        state,
                    )
                    .await?;
                if retry_parked {
                    // A `<q:retry>` attempt FAILED with retries remaining: the token is parked on
                    // a backoff timer, so the node is neither completed nor visited and nothing
                    // downstream of it may fire. It is deliberately kept OUT of
                    // `completed_activities` — that omission is what makes the timer re-drive
                    // re-execute it instead of replaying past it. No reply/send is emitted
                    // either: the task produced no output to emit. Returning here (rather than
                    // setting `left_handled`) is the same "token did not leave" exit the
                    // channel-call park takes, minus the `on_token_left` notify and the `visited`
                    // mark that only a departing token earns.
                    return Ok(());
                }
                state.completed_activities.push(id.clone());
                self.emit_reply_if_bound(id, state)?;
                // A service task may also carry a <q:send channel="…"> — optional here.
                self.emit_send_if_bound(id, state, false)?;
                if state.process.has_continue_reply(id) {
                    // Respond-and-continue: the reply (`responseBody`) is produced and any
                    // out-of-band reply emitted; the dispatcher flushes it to the caller NOW. Park
                    // here — the token stops, and a due-now timer wait self-resumes the remaining
                    // nodes at once (the tail runs on the resume replay, skipping this completed
                    // node). `is_sync_eligible` already forced this process onto the stateful path.
                    state.waiting.push(id.clone());
                    // Due-now: the poller claims it on its next tick and self-resumes at once.
                    self.record_timer_wait(id, (self.now_supplier)(), state);
                    state.detached_reply = true;
                    left_handled = true;
                } else {
                    self.enqueue_outgoing(node_id, state);
                }
            }
            Node::DataTask {
                id, data_mapping, ..
            } => {
                self.run_data_task(id, data_mapping, state).await?;
                state.completed_activities.push(id.clone());
                self.emit_reply_if_bound(id, state)?;
                self.enqueue_outgoing(node_id, state);
            }
            Node::ScriptTask {
                id, script_file, ..
            } => {
                self.run_script_task(id, script_file, state)?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::BusinessRuleTask {
                id, decision_file, ..
            } => {
                self.run_business_rule_task(id, decision_file, state)?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::ManualTask { id, .. } => {
                // A manual task is work performed outside the engine: a no-op pass-through.
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::SendTask { id, .. } => {
                state.completed_activities.push(id.clone());
                self.emit_send_if_bound(id, state, true)?;
                self.enqueue_outgoing(node_id, state);
            }
            Node::UserTask { id, .. } | Node::MessageCatchEvent { id, .. } => {
                // Wait state (S-X1/S-X2): the token PARKS. A satisfied wait node is
                // short-circuited by the prior_completed fast-path above; reaching this arm
                // means the branch suspends here until a relay resumes the instance. The
                // outgoing flow is NOT enqueued; on_token_left is suppressed (the token did
                // not leave) and the node is not marked visited (waiting, not done).
                // `execute_sync` rejects wait-state processes up-front, so on the sync path
                // this arm is unreachable.
                // An INTERRUPTING timer boundary that fired on this host cancels the
                // wait and routes the token out through the boundary instead.
                if let Some(fired) = state.take_fired_timer_for(id) {
                    self.route_timer_boundary(&fired.boundary_id, id, state)?;
                } else {
                    state.waiting.push(id.clone());
                    self.schedule_timer_boundaries(id, state)?;
                    left_handled = true;
                }
            }
            // Intermediate timer catch: the token parks on a durable TIMER
            // wait row; the poller resumes it at due-at (satisfied node = this node).
            Node::TimerCatchEvent { id, timer, .. } => {
                state.waiting.push(id.clone());
                if !state.prior_waiting.contains(id) {
                    let due_at = self.compute_due_at(timer, id)?;
                    self.record_timer_wait(id, due_at, state);
                }
                left_handled = true;
            }
            Node::ExclusiveGateway {
                id,
                default_flow_id,
                ..
            } => self.handle_exclusive(id, default_flow_id.as_deref(), state)?,
            Node::InclusiveGateway {
                id,
                default_flow_id,
                ..
            } => {
                if self.handle_inclusive(id, default_flow_id.as_deref(), state, &token_event)? {
                    left_handled = true;
                }
            }
            Node::ParallelGateway { id, .. } => {
                if self.handle_parallel(id, state, &token_event)? {
                    left_handled = true;
                }
            }
            Node::ComplexGateway {
                id,
                default_flow_id,
                activation_condition,
                ..
            } => {
                if self.handle_complex(
                    id,
                    default_flow_id.as_deref(),
                    activation_condition.as_deref(),
                    state,
                    &token_event,
                )? {
                    left_handled = true;
                }
            }
            Node::CallActivity {
                id, called_element, ..
            } => {
                self.run_call_activity(id, called_element, state).await?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::SubProcess { id, inner, .. } => {
                self.run_embedded_sub_process(id, inner, state).await?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::TransactionSubProcess { id, inner, .. } => {
                let cancelled = self.run_transaction_sub_process(id, inner, state).await?;
                state.completed_activities.push(id.clone());
                if cancelled {
                    self.route_cancel_boundary(id, state)?;
                } else {
                    self.emit_reply_if_bound(id, state)?;
                    self.enqueue_outgoing(node_id, state);
                }
            }
            Node::AdHocSubProcess {
                id,
                inner,
                completion_condition,
                ..
            } => {
                self.run_ad_hoc_sub_process(inner, completion_condition.as_deref(), state)
                    .await?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::EventSubProcess { .. } => {
                // Reached only via route_error; a token landing here directly is a no-op.
                left_handled = true;
            }
            Node::MultiInstance {
                id,
                inner,
                loop_cardinality,
                loop_data_input_ref,
                input_data_item,
                completion_condition,
                ..
            } => {
                self.run_multi_instance(
                    id,
                    inner,
                    loop_cardinality.as_deref(),
                    loop_data_input_ref.as_deref(),
                    input_data_item.as_deref(),
                    completion_condition.as_deref(),
                    state,
                )
                .await?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::StandardLoop {
                id,
                inner,
                loop_condition,
                test_before,
                loop_maximum,
                ..
            } => {
                self.run_standard_loop(
                    id,
                    inner,
                    loop_condition.as_deref(),
                    *test_before,
                    *loop_maximum,
                    state,
                )
                .await?;
                state.completed_activities.push(id.clone());
                self.enqueue_outgoing(node_id, state);
            }
            Node::IntermediateThrowEvent {
                id,
                kind,
                activity_ref,
                reference,
                ..
            } => match kind {
                ThrowKind::Compensate => {
                    self.fire_compensation(state, activity_ref.as_deref())
                        .await?;
                    self.enqueue_outgoing(node_id, state);
                }
                ThrowKind::Message => {
                    self.emit_send_if_bound(id, state, true)?;
                    self.enqueue_outgoing(node_id, state);
                }
                ThrowKind::Signal => {
                    self.emit_send_if_bound(id, state, false)?;
                    self.enqueue_outgoing(node_id, state);
                }
                ThrowKind::Escalation => {
                    let interrupting_caught = self.route_escalation(reference.as_deref(), state);
                    if !interrupting_caught {
                        self.enqueue_outgoing(node_id, state);
                    }
                }
                ThrowKind::Link => self.jump_to_link_catch(id, reference.as_deref(), state)?,
                ThrowKind::None => self.enqueue_outgoing(node_id, state),
            },
            Node::LinkCatchEvent { .. } => self.enqueue_outgoing(node_id, state),
            Node::ErrorEvent { id, error_code, .. } => {
                // Throwing an error end-event — propagate up to a matching boundary event.
                return Err(Signal::BpmnError {
                    source: id.clone(),
                    code: error_code.clone().unwrap_or_default(),
                });
            }
            Node::BoundaryEvent { .. } => self.enqueue_outgoing(node_id, state),
            Node::EndEvent { id, .. } => {
                self.emit_reply_if_bound(id, state)?;
                state.reached_end = true;
                state.end_outputs = state.ctx.borrow().variables.clone();
            }
            Node::TerminateEndEvent { id, .. } => {
                // A terminate end event ends the whole instance immediately: emit any bound
                // reply, then drop
                // every other live token so no other branch advances.
                self.emit_reply_if_bound(id, state)?;
                state.work.clear();
                state.reached_end = true;
                state.end_outputs = state.ctx.borrow().variables.clone();
            }
            Node::CancelEndEvent { .. } => return Err(Signal::TxCancel),
        }

        if !left_handled {
            self.notify(|l| l.on_token_left(&token_event));
            state.visited.insert(node_id.to_string());
        }
        Ok(())
    }

    // ---- service task (task-kind routing) ----------------------------------------

    /// Run a `<serviceTask>` through the task-kind routing (template → decision → registered
    /// task function). Returns `true` when a `<q:retry>` policy PARKED the token on a backoff
    /// timer instead of completing or failing — the caller must then suppress the node's
    /// completion bookkeeping exactly as it does for a channel-call park.
    ///
    /// `parkable` is the caller's assertion that THIS state owns a wait frontier the dispatcher
    /// will persist. Only the top-level stateful walk passes `true`; the inline runners
    /// (multi-instance, ad-hoc, compensation) pass `false` and a retry there fails closed with
    /// [`codes::DISPATCH_RETRY_UNSUPPORTED_CONTEXT`].
    async fn run_service_task(
        &self,
        id: &str,
        implementation: &str,
        data_mapping: &DataMapping,
        params: &[ParamBinding],
        parkable: bool,
        state: &mut ExecutionState<'_>,
    ) -> Result<bool, Signal> {
        let (event_deployment, event_labels, event_instance) = (
            state.deployment.clone(),
            state.labels.clone(),
            state.instance_id.clone(),
        );
        let task_event = move |duration: u128| TaskEvent {
            deployment: event_deployment.clone(),
            labels: event_labels.clone(),
            instance_id: event_instance.clone(),
            task_name: implementation.to_string(),
            duration_nanos: duration,
        };
        let invoked = task_event(0);
        self.notify(|l| l.on_task_invoked(&invoked));

        // Reserved ops: the coverage: family. Engine operation, handled before
        // template/decision/task resolution; failures propagate without on_task_failed
        // (it runs before the failure-notify guard is armed).
        if implementation.starts_with(COVERAGE_OP_PREFIX) {
            self.run_coverage_op(id, implementation, state)
                .await
                .map_err(Signal::Fatal)?;
            state.completed_activities.push(id.to_string());
            let done = task_event(0);
            self.notify(|l| l.on_task_completed(&done));
            return Ok(false);
        }

        let template_engine = self.template_engines.for_implementation(implementation);
        // Scoped <q:param> inputs: evaluated once per invocation, overlaid on the render
        // model / task view for that call only, never persisted.
        let param_values = self.evaluate_params(params, state).map_err(Signal::Fatal)?;

        let started = Instant::now();
        let outcome: Result<(), TaskFailure> = (|| {
            if let Some(engine) = template_engine {
                // Universal task I/O scoping: declared inputs are the
                // COMPLETE context the render sees (plus `<q:param>` values); declared
                // outputs are the only variables written back. Store-op associations stay
                // data-task-only.
                if data_mapping.has_store_ops() {
                    return Err(TaskFailure::Diag(SutraError::new(
                        sutra_bpmn::codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED,
                        format!(
                            "<serviceTask> '{id}' is a template service task with data-store \
                             associations; store reads/writes belong on a declarative data \
                             task (no @implementation)."
                        ),
                    )));
                }
                let render_vars = self.task_input_view(data_mapping, state);
                let rendered = self.render_template(
                    implementation,
                    &engine,
                    &param_values,
                    &render_vars,
                    state,
                )?;
                let mut produced = Variables::new();
                // `FeelValue` has no bytes variant — a UTF-8 String is byte-identical to the
                // rendered payload on the reply path.
                produced.insert("responseBody", FeelValue::String(rendered.clone()));
                // <q:output variable>: the render is ADDITIONALLY bound to
                // the named variable, independent of any <q:reply>/<q:send> of the bytes.
                if let Some(output) = &state.process.bindings_for(id).output {
                    produced.insert(output.variable.clone(), FeelValue::String(rendered));
                }
                apply_task_outputs(produced, &data_mapping.outputs, state);
                return Ok(());
            }
            // Decision tasks: a decision-file suffix on a serviceTask routes to
            // the decision-engine registry (`businessRuleTask` is the primary carrier;
            // the suffix route is an addition). Task I/O scoping applies.
            if let Some(engine) = self.decision_engines.for_implementation(implementation) {
                if data_mapping.has_store_ops() {
                    return Err(TaskFailure::Diag(SutraError::new(
                        sutra_bpmn::codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED,
                        format!(
                            "<serviceTask> '{id}' is a decision service task with data-store \
                             associations; store reads/writes belong on a declarative data \
                             task (no @implementation)."
                        ),
                    )));
                }
                let mut input = self.task_input_view(data_mapping, state);
                input.merge(&param_values);
                let out = self.evaluate_decision(id, implementation, &engine, &input, state)?;
                apply_task_outputs(out, &data_mapping.outputs, state);
                return Ok(());
            }
            // A channel-call task reached OUTSIDE the top-level stateful walk
            // (multi-instance / ad-hoc / compensation inline runners): these contexts
            // cannot park a durable wait state — fail closed.
            if implementation.starts_with(CHANNEL_CALL_PREFIX) {
                return Err(TaskFailure::Diag(SutraError::new(
                    codes::DISPATCH_CHANNEL_CALL_UNSUPPORTED_CONTEXT,
                    format!(
                        "<serviceTask> '{id}' (implementation=\"{implementation}\") is a \
                         channel-call task running inside a multi-instance / ad-hoc / \
                         compensation scope, which cannot park a durable wait state."
                    ),
                )));
            }
            // Fallback — the registered task function (there is no serviceTask bean SPI).
            let task = self.tasks.resolve(implementation).map_err(|e| match e {
                ExecError::Diagnostic(d) => TaskFailure::Diag(d),
                other => TaskFailure::Diag(other.to_diagnostic()),
            })?;
            // A task with data associations runs under SCOPED I/O.
            let scoped = !data_mapping.is_empty();
            let view = if scoped {
                self.scoped_view(data_mapping, &param_values, state)
            } else {
                self.shared_view(&param_values, state)
            };
            let input = view
                .variable("payload")
                .cloned()
                .unwrap_or_else(|| FeelValue::Map(view.variables().to_feel_context()));
            let output = task(&input, &view).map_err(|e| match e {
                TaskError::BpmnError(code) => TaskFailure::Bpmn(code),
                TaskError::Failed(msg) => TaskFailure::Uncaught(msg),
            })?;
            if scoped {
                // Merge the task output into an isolated map, then propagate ONLY the
                // mapped-out variables back to the shared process scope.
                let mut task_vars = view.variables().clone();
                merge_task_output(output, &mut task_vars);
                let mut ctx = state.ctx.borrow_mut();
                for v in &data_mapping.outputs {
                    if let Some(value) = task_vars.get(v) {
                        ctx.variables.insert(v.clone(), value.clone());
                    }
                }
            } else {
                let mut ctx = state.ctx.borrow_mut();
                merge_task_output(output, &mut ctx.variables);
            }
            Ok(())
        })();

        let duration = started.elapsed().as_nanos().max(1);
        match outcome {
            Ok(()) => {
                // The task SUCCEEDED. If it had been retrying, drop its durable attempt counter:
                // the map the park persists is authoritative, so clearing it here is what stops a
                // `sutra.retry.<nodeId>` key surviving as dead weight on every later snapshot.
                state.retry_attempts.remove(id);
                let done = task_event(duration);
                self.notify(|l| l.on_task_completed(&done));
                Ok(false)
            }
            Err(TaskFailure::Bpmn(code)) => {
                // A BPMN error is a MODELLED outcome, never a retryable fault: it routes to its
                // boundary event / error event sub-process. `<q:retry>` deliberately never sees
                // it — retrying a modelled branch would re-run the flow the author drew.
                let failed = task_event(duration);
                let d = SutraError::new(
                    codes::RUNTIME_ERROR_UNCAUGHT,
                    format!("Service task @Task(\"{implementation}\") threw BPMN error {code}"),
                );
                self.notify(|l| l.on_task_failed(&failed, &d));
                Err(Signal::BpmnError {
                    source: id.to_string(),
                    code,
                })
            }
            Err(TaskFailure::Uncaught(msg)) => {
                let failed = task_event(duration);
                let d = SutraError::new(
                    codes::RUNTIME_TASK_UNCAUGHT,
                    format!("Task @Task(\"{implementation}\") threw {msg}"),
                );
                self.notify(|l| l.on_task_failed(&failed, &d));
                // The ONLY retryable failure: a registered task function that threw. Template
                // renders and decision evaluations arrive as `Diag` below and are deterministic
                // configuration faults — repeating them would only delay the same error behind N
                // backoffs while hiding the config bug that caused it.
                match self.plan_retry(id, parkable, &msg, state)? {
                    RetryDisposition::Park { due_at, attempt } => {
                        state.retry_attempts.insert(id.to_string(), attempt);
                        state.waiting.push(id.to_string());
                        self.record_timer_wait(id, due_at, state);
                        Ok(true)
                    }
                    RetryDisposition::Fail(fatal) => Err(Signal::Fatal(fatal)),
                    RetryDisposition::NoPolicy => Err(Signal::Fatal(ExecError::Diagnostic(d))),
                }
            }
            Err(TaskFailure::Diag(d)) => {
                let failed = task_event(duration);
                self.notify(|l| l.on_task_failed(&failed, &d));
                Err(Signal::Fatal(ExecError::Diagnostic(d)))
            }
        }
    }

    /// Decide what a failed `<q:retry>` attempt does next: park on a backoff timer, fail
    /// terminally, or (no policy declared) fall through to the engine's ordinary fatal path.
    ///
    /// The terminal outcomes are deliberately distinguishable in the persisted `FAILED` snapshot:
    /// a policy-less task keeps its historical `SUTRA.RUNTIME.TASK.UNCAUGHT`, while an exhausted
    /// budget and a non-retryable classification both surface [`codes::RUNTIME_RETRY_EXHAUSTED`]
    /// with the reason spelled out — an operator reading the failure needs to know whether adding
    /// attempts would have helped.
    fn plan_retry(
        &self,
        id: &str,
        parkable: bool,
        failure_message: &str,
        state: &ExecutionState<'_>,
    ) -> Result<RetryDisposition, Signal> {
        let Some(policy) = state.process.retry_policy(id) else {
            return Ok(RetryDisposition::NoPolicy);
        };
        if !parkable {
            return Ok(RetryDisposition::Fail(ExecError::Diagnostic(
                SutraError::new(
                    codes::DISPATCH_RETRY_UNSUPPORTED_CONTEXT,
                    format!(
                        "<serviceTask> '{id}' declares <q:retry> but failed inside a \
                         multi-instance / ad-hoc / compensation / embedded sub-process scope, \
                         which cannot park the durable backoff timer a retry needs. Model the \
                         retried task at the top level of the process. Underlying failure: \
                         {failure_message}"
                    ),
                ),
            )));
        }
        // Attempts that had already failed, plus this one.
        let attempt = state.retry_attempts.get(id).copied().unwrap_or(0) + 1;
        let classification = failure_classification_code(failure_message);
        if policy
            .non_retryable_codes
            .iter()
            .any(|c| c == classification)
        {
            return Ok(RetryDisposition::Fail(ExecError::Diagnostic(
                SutraError::new(
                    codes::RUNTIME_RETRY_EXHAUSTED,
                    format!(
                        "<serviceTask> '{id}' failed on attempt {attempt} of {} with \
                         classification '{classification}', which its <q:retry \
                         nonRetryableCodes> declares NON-RETRYABLE; the remaining attempts are \
                         skipped and the instance fails now. Underlying failure: \
                         {failure_message}",
                        policy.max_attempts
                    ),
                ),
            )));
        }
        if attempt >= policy.max_attempts {
            return Ok(RetryDisposition::Fail(ExecError::Diagnostic(
                SutraError::new(
                    codes::RUNTIME_RETRY_EXHAUSTED,
                    format!(
                        "<serviceTask> '{id}' exhausted its <q:retry maxAttempts=\"{}\"> budget \
                         (attempt {attempt} failed). Underlying failure: {failure_message}",
                        policy.max_attempts
                    ),
                ),
            )));
        }
        let due_at = self.compute_retry_due_at(id, policy, attempt)?;
        Ok(RetryDisposition::Park { due_at, attempt })
    }

    /// The RFC 3339 instant the next attempt becomes due:
    /// `now + min(initialDelay x backoffCoefficient^(attempt-1), maxDelay)`.
    ///
    /// `attempt` is the count of attempts that have now failed, so the FIRST failure (attempt 1)
    /// waits exactly `initialDelay`. Arithmetic runs in f64 milliseconds and saturates at the
    /// declared ceiling before it can overflow, so a large coefficient or a long-lived instance
    /// can never produce a negative or absurd due-at. Deliberately unjittered: unlike the outbox
    /// curve — where every replica re-attempts the same shared rows and would synchronise — each
    /// instance owns its own timer row, so there is no wave to spread.
    fn compute_retry_due_at(
        &self,
        id: &str,
        policy: &sutra_bpmn::qbindings::RetryBinding,
        attempt: u32,
    ) -> Result<String, Signal> {
        let parse = |raw: &str, which: &str| -> Result<f64, Signal> {
            sutra_bpmn::duration::parse_iso8601_duration(raw)
                .map(|d| d.as_millis() as f64)
                .map_err(|reason| {
                    fatal_diag(
                        codes::RUNTIME_UNEXPECTED,
                        format!(
                            "<q:retry> on node '{id}' has an unparseable @{which} '{raw}' at \
                             runtime ({reason}); the loader validates it, so this is an \
                             engine-internal inconsistency."
                        ),
                    )
                })
        };
        let initial_ms = parse(&policy.initial_delay, "initialDelay")?;
        let ceiling_ms = parse(&policy.max_delay, "maxDelay")?;
        let grown = initial_ms
            * policy
                .backoff_coefficient
                .powi(attempt.saturating_sub(1) as i32);
        let delay_ms = if grown.is_finite() && grown > 0.0 {
            grown.min(ceiling_ms)
        } else {
            ceiling_ms
        };
        let now = time::OffsetDateTime::parse(
            &(self.now_supplier)(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| time::OffsetDateTime::now_utc());
        (now + time::Duration::milliseconds(delay_ms as i64))
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| {
                fatal_diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!("retry due-at for node '{id}' could not be formatted: {e}"),
                )
            })
    }

    // ---- channel-call task (Rust-only) --------------------------------------------------

    /// Run a `implementation="channel:<name>"` service task: evaluate the input mapping
    /// → COLLECT the outbound request emission (destination = the named outbound
    /// channel binding; the emission commits atomically with the park step —
    /// delivery is the outbox dispatcher's job) → PARK keyed by the task's declared
    /// `<q:alias>` (the dispatcher evaluates + records the alias rows in the same step) →
    /// arm every attached timer boundary. Returns `true` when the token parked; `false`
    /// when a FIRED timer boundary routed the token out instead.
    fn run_channel_call(
        &self,
        id: &str,
        implementation: &str,
        data_mapping: &DataMapping,
        params: &[ParamBinding],
        state: &mut ExecutionState<'_>,
    ) -> Result<bool, Signal> {
        // A TASK FAILURE delivered from outside the graph (the outbox terminally poisoned
        // this node's request delivery): offer it to the `<q:retry>` policy — park a backoff
        // (budget left) or fail the pass (exhausted / non-retryable). Checked FIRST: a
        // failure delivery must neither re-park silently nor re-send.
        if let Some(failure) = state.take_channel_call_failure_for(id) {
            return self.park_or_fail_channel_call_retry(
                id,
                implementation,
                &failure.code,
                &failure.message,
                state,
            );
        }
        // A fired INTERRUPTING timer boundary on this host cancels the wait: the token
        // leaves through the boundary route (or the timeout error) — nothing re-sends.
        //
        // With a `<q:retry>` policy and the ROUTE-LESS (`<q:timeout>`) boundary form, the
        // timeout is a RETRYABLE TASK FAILURE first: the policy rules on it exactly like a
        // registered task's uncaught error. A boundary WITH outgoing flows is a MODELLED
        // outcome and always wins — but the loader refuses that combination with a retry
        // policy, so reaching `route_timer_boundary` below with a policy present implies the
        // route-less form only when no policy exists (policy-less behaviour is byte-for-byte
        // the pre-F1 one: the catchable `SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT` BPMN error).
        if let Some(fired) = state.take_fired_timer_for(id) {
            let route_less = state.process.outgoing(&fired.boundary_id).is_empty();
            if route_less && state.process.has_retry_policy(id) {
                return self.park_or_fail_channel_call_retry(
                    id,
                    implementation,
                    codes::DISPATCH_CHANNEL_CALL_TIMEOUT,
                    &format!(
                        "channel-call task '{id}' timed out waiting for its correlated \
                         response (timer boundary '{}' fired)",
                        fired.boundary_id
                    ),
                    state,
                );
            }
            self.route_timer_boundary(&fired.boundary_id, id, state)?;
            return Ok(false);
        }
        // Still waiting from a prior pass (a resume replay reached the parked call):
        // RE-PARK only — the request was already sent and the pending timer rows keep
        // their original due-at. An explicit retry RE-DRIVE never takes this exit: the
        // resume seeding left the re-driven node OUT of `prior_waiting`, so it falls
        // through to the fresh-park path below and RE-EMITS its request.
        if state.prior_waiting.contains(id) {
            state.waiting.push(id.to_string());
            return Ok(true);
        }

        let channel_name = implementation
            .strip_prefix(CHANNEL_CALL_PREFIX)
            .expect("caller matched the channel: prefix")
            .trim();
        let resolved = self
            .outbound_channels
            .find(&state.deployment, channel_name)
            .cloned()
            .ok_or_else(|| {
                fatal_diag(
                    codes::CONFIG_CHANNEL_OUTBOUND_UNKNOWN,
                    format!(
                        "Channel-call task '{id}' on instance '{}' names outbound channel \
                         '{channel_name}' but no channel with that name is registered with \
                         direction: outbound for deployment {}.",
                        state.instance_id,
                        state.deployment.value()
                    ),
                )
            })?;

        // The request-build context: declared inputs (or the full shared scope) plus
        // the scoped <q:param> overlay.
        let param_values = self.evaluate_params(params, state).map_err(Signal::Fatal)?;
        let mut view = self.task_input_view(data_mapping, state);
        view.merge(&param_values);
        let body = channel_call_body(&view);

        // The request emission — COLLECTED (EmissionSink); the dispatcher hands it to the
        // park step (enqueue atomic with the snapshot, or nothing).
        let adapter = ReplyBinding {
            mode: resolved.mode,
            destination: Some(resolved.destination.clone()),
            content_type: None,
            required: true,
            ce_type: None,
            ce_source: None,
            ce_subject: None,
            ce_data_content_type: None,
            auth: None,
            auth_secret_ref: None,
            auth_header: None,
            message_type: None,
            continue_after: false,
            headers: Vec::new(),
        };
        let cloud_event = if resolved.mode == ReplyMode::Native {
            None
        } else {
            Some(self.build_cloud_event(&adapter, state))
        };
        let emission = Emission {
            kind: EmissionKind::Send,
            node_id: id.to_string(),
            instance_id: state.instance_id.clone(),
            mode: resolved.mode,
            destination: resolved.destination.clone(),
            content_type: None,
            required: true,
            body: body.into(),
            cloud_event,
            auth_ref: resolved.auth_ref.clone(),
            headers: BTreeMap::new(),
        };
        if let Some(sink) = self.emissions.as_ref() {
            sink.emit(emission);
        }
        let reply_event = ReplyEvent {
            deployment: state.deployment.clone(),
            labels: state.labels.clone(),
            instance_id: state.instance_id.clone(),
            node_id: id.to_string(),
            mode: resolved.mode,
            destination: resolved.destination.clone(),
        };
        self.notify(|l| l.on_reply_emitted(&reply_event));

        // PARK: the host task is the wait frontier; its timer boundaries become TIMER rows.
        state.waiting.push(id.to_string());
        self.schedule_timer_boundaries(id, state)?;
        Ok(true)
    }

    /// Rule on a channel-call TASK FAILURE (`code`: the timeout, or a poisoned delivery)
    /// through the node's `<q:retry>` policy — the channel-call counterpart of the
    /// registered-task `TaskFailure::Uncaught` arm in [`Self::run_service_task`].
    ///
    /// Budget left ⇒ BACKOFF PARK: the node stays the wait frontier with the backoff timer as
    /// its only fresh row, the durable attempt count advances, and the `retry_backoff` marker
    /// records the dead attempt (the dispatcher resolves the attempt's response wait /
    /// timeout rows and refuses its late relays on that marker; the re-drive at due-time
    /// RE-EMITS the request). Returns `Ok(true)` — the token did not leave. Budget spent or
    /// `code` declared non-retryable ⇒ the pass fails fatally
    /// (`SUTRA.RUNTIME.RETRY.EXHAUSTED`), and the dispatcher stamps the durable FAILED
    /// snapshot exactly as it does for an exhausted registered-task retry.
    ///
    /// `NoPolicy` is unreachable by construction — both callers gate on the policy — and
    /// fails loudly rather than silently re-parking if that invariant ever breaks.
    fn park_or_fail_channel_call_retry(
        &self,
        id: &str,
        implementation: &str,
        code: &str,
        detail: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<bool, Signal> {
        // The classification convention `plan_retry` already speaks: a leading "CODE:" token.
        // Both failure codes here are stable SUTRA.* codes, so `<q:retry nonRetryableCodes>`
        // matches them directly.
        let failure_message = format!("{code}: {detail}");
        let failed = TaskEvent {
            deployment: state.deployment.clone(),
            labels: state.labels.clone(),
            instance_id: state.instance_id.clone(),
            task_name: implementation.to_string(),
            // No in-process invocation ran — the attempt failed out in the world.
            duration_nanos: 0,
        };
        let d = SutraError::new(code, failure_message.clone());
        self.notify(|l| l.on_task_failed(&failed, &d));
        match self.plan_retry(id, state.retry_parkable, &failure_message, state)? {
            RetryDisposition::Park { due_at, attempt } => {
                state.retry_attempts.insert(id.to_string(), attempt);
                state.retry_backoff.insert(id.to_string(), code.to_string());
                state.waiting.push(id.to_string());
                self.record_timer_wait(id, due_at, state);
                Ok(true)
            }
            RetryDisposition::Fail(fatal) => Err(Signal::Fatal(fatal)),
            RetryDisposition::NoPolicy => Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "channel-call task '{id}' was offered failure '{code}' for its <q:retry> \
                     policy, but it declares none; the caller must gate on the policy — this \
                     is an engine-internal inconsistency."
                ),
            )),
        }
    }

    /// Arm every (interrupting) timer boundary attached to `host_id` that is not already
    /// pending from a prior pass: compute due-at from the ISO-8601 duration and record the
    /// fresh timer wait (`on_timer_scheduled` fires).
    fn schedule_timer_boundaries(
        &self,
        host_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let mut fresh: Vec<(String, String)> = Vec::new();
        for n in state.process.nodes() {
            if let Node::BoundaryEvent {
                id: boundary_id,
                kind: BoundaryKind::Timer,
                attached_to_ref,
                timer: Some(timer),
                ..
            } = n
            {
                if attached_to_ref != host_id
                    || state.prior_waiting.contains(host_id)
                    || state.timer_waits.iter().any(|t| &t.node_id == boundary_id)
                {
                    continue;
                }
                let due_at = self.compute_due_at(timer, boundary_id)?;
                fresh.push((boundary_id.clone(), due_at));
            }
        }
        for (node_id, due_at) in fresh {
            self.record_timer_wait(&node_id, due_at, state);
        }
        Ok(())
    }

    /// Record one fresh timer wait + emit `on_timer_scheduled`.
    fn record_timer_wait(&self, node_id: &str, due_at: String, state: &mut ExecutionState<'_>) {
        state.timer_waits.push(TimerWait {
            node_id: node_id.to_string(),
            due_at: due_at.clone(),
        });
        let event = TimerEvent {
            deployment: state.deployment.clone(),
            labels: state.labels.clone(),
            instance_id: state.instance_id.clone(),
            node_id: node_id.to_string(),
            due_at,
        };
        self.notify(|l| l.on_timer_scheduled(&event));
    }

    /// Route a FIRED interrupting timer boundary: outgoing flows when it has any (the
    /// modeled timeout path), else the `SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT` BPMN error at
    /// the host (the `<q:timeout>` form — catchable by an error boundary / event
    /// sub-process; uncaught fails the instance closed).
    fn route_timer_boundary(
        &self,
        boundary_id: &str,
        host_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let outgoing = state.process.outgoing(boundary_id);
        if outgoing.is_empty() {
            return Err(Signal::BpmnError {
                source: host_id.to_string(),
                code: codes::DISPATCH_CHANNEL_CALL_TIMEOUT.to_string(),
            });
        }
        for flow in outgoing {
            state.fire(flow);
        }
        state.visited.insert(boundary_id.to_string());
        Ok(())
    }

    /// Compute the RFC 3339 due-at of a timer being ARMED right now.
    ///
    /// `now` is the injected supplier (deterministic under test), and the arithmetic itself lives
    /// in [`sutra_bpmn::timer`] so the executor's park path and the deployment-activation
    /// scheduler compute due-ats the SAME way: a `<timeDuration>` lands `now + duration` out, a
    /// `<timeDate>` lands on its absolute instant — which may already be in the PAST, in which
    /// case the row is written already-due and the poller fires it on its next tick.
    ///
    /// A repeating `<timeCycle>` never reaches here: the loader admits it on START events only,
    /// where deployment activation (not a parked token) owns the schedule.
    fn compute_due_at(
        &self,
        timer: &sutra_bpmn::timer::TimerDefinition,
        node_id: &str,
    ) -> Result<String, Signal> {
        let now = time::OffsetDateTime::parse(
            &(self.now_supplier)(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| time::OffsetDateTime::now_utc());
        let due = timer.first_due_at(now).map_err(|rejection| {
            fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "timer node '{node_id}' spec '{}' failed to schedule: {rejection}",
                    timer.spec_text()
                ),
            )
        })?;
        // UTC-normalised: an offset-bearing `<timeDate>` renders as the instant it names, so
        // every due-at string in the system is directly comparable.
        Ok(sutra_bpmn::timer::format_instant(due))
    }

    /// The input context a task sees: declared inputs only (present ones),
    /// or the full shared scope when none are declared (backward-compatible).
    fn task_input_view(&self, dm: &DataMapping, state: &ExecutionState<'_>) -> Variables {
        let ctx = state.ctx.borrow();
        if dm.inputs.is_empty() {
            return ctx.variables.clone();
        }
        let mut vars = Variables::new();
        for name in &dm.inputs {
            if let Some(v) = ctx.variables.get(name) {
                vars.insert(name.clone(), v.clone());
            }
        }
        vars
    }

    /// Evaluate a service task's scoped `<q:param>` inputs (FEEL, in declaration order).
    fn evaluate_params(
        &self,
        params: &[ParamBinding],
        state: &ExecutionState<'_>,
    ) -> Result<Variables, ExecError> {
        let mut out = Variables::new();
        if params.is_empty() {
            return Ok(out);
        }
        let vars = state.ctx.borrow().variables.clone();
        for p in params {
            out.insert(
                p.name.clone(),
                (self.value_evaluator)(&p.expression, &vars)?,
            );
        }
        Ok(out)
    }

    /// The invocation view for an association-free task: shared scope + param overlay.
    fn shared_view(&self, param_values: &Variables, state: &ExecutionState<'_>) -> TaskContextView {
        let ctx = state.ctx.borrow();
        let mut variables = ctx.variables.clone();
        variables.merge(param_values);
        TaskContextView {
            deployment: ctx.deployment.clone(),
            labels: ctx.labels.clone(),
            instance_id: ctx.instance_id.clone(),
            module_id: ctx.module_id.clone(),
            module_version: ctx.module_version.clone(),
            simulation: ctx.simulation,
            variables,
        }
    }

    /// The scoped view a data-mapped task runs against: ONLY the mapped-in variables
    /// (present ones), plus the param overlay.
    fn scoped_view(
        &self,
        dm: &DataMapping,
        param_values: &Variables,
        state: &ExecutionState<'_>,
    ) -> TaskContextView {
        let ctx = state.ctx.borrow();
        let mut variables = Variables::new();
        for v in &dm.inputs {
            if let Some(value) = ctx.variables.get(v) {
                variables.insert(v.clone(), value.clone());
            }
        }
        variables.merge(param_values);
        TaskContextView {
            deployment: ctx.deployment.clone(),
            labels: ctx.labels.clone(),
            instance_id: ctx.instance_id.clone(),
            module_id: ctx.module_id.clone(),
            module_version: ctx.module_version.clone(),
            simulation: ctx.simulation,
            variables,
        }
    }

    /// Render a template-backed service task against `vars` (the scoped input view) and
    /// return the render string (the caller binds `responseBody` / `<q:output variable>`).
    fn render_template(
        &self,
        file_name: &str,
        engine: &Rc<dyn crate::registry::TemplateEngine>,
        param_values: &Variables,
        vars: &Variables,
        state: &ExecutionState<'_>,
    ) -> Result<String, TaskFailure> {
        let key = self.artifact_key(state, ArtifactType::Template, file_name);
        let bytes = self.templates.find(&key).ok_or_else(|| {
            TaskFailure::Diag(SutraError::new(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "No template '{file_name}' is registered for deployment '{}'. A \
                     <bpmn:serviceTask implementation=\"{file_name}\"> requires a matching \
                     file under the module version's templates/ folder.",
                    state.deployment.value()
                ),
            ))
        })?;
        // The `sutra.template` waterfall span (the OTLP layer subscribes without touching
        // this call site).
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_TEMPLATE,
            deployment.id = %state.deployment.value(),
            instance.id = %state.instance_id,
            task.name = %file_name,
        )
        .entered();
        let model = self.render_model(vars, param_values);
        engine
            .render(&key, bytes, &model)
            .map_err(TaskFailure::Uncaught)
    }

    fn evaluate_decision(
        &self,
        node_id: &str,
        file_name: &str,
        engine: &Rc<dyn crate::registry::DecisionEngine>,
        input: &Variables,
        state: &ExecutionState<'_>,
    ) -> Result<Variables, TaskFailure> {
        let key = self.artifact_key(state, ArtifactType::Decision, file_name);
        let bytes = self.decisions.find(&key).ok_or_else(|| {
            TaskFailure::Diag(SutraError::new(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "<businessRuleTask> {node_id} names decision '{file_name}' but no such \
                     file is registered for deployment '{}'. It requires a matching file \
                     under the module version's decisions/ folder.",
                    state.deployment.value()
                ),
            ))
        })?;
        // The `sutra.decision` waterfall span.
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_DECISION,
            deployment.id = %state.deployment.value(),
            instance.id = %state.instance_id,
            task.name = %file_name,
        )
        .entered();
        engine
            .evaluate(&key, bytes, input)
            .map_err(TaskFailure::Uncaught)
    }

    /// The data model a template / script renders against: `vars` (the shared scope, or a
    /// scoped input view), plus a `vars` handle for dotted-key bracket access, the
    /// caller-injected `uuid` / `now` suppliers, and `sourceXml` (raw inbound body) for
    /// XML-transform engines.
    fn render_model(&self, vars: &Variables, param_values: &Variables) -> serde_json::Value {
        let mut model = match vars.to_json() {
            serde_json::Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        for (k, v) in param_values.iter() {
            model.insert(k.to_string(), feel_to_json(v));
        }
        model.insert("vars".to_string(), vars.to_json());
        model.insert(
            "uuid".to_string(),
            serde_json::Value::String((self.uuid_supplier)()),
        );
        model.insert(
            "now".to_string(),
            serde_json::Value::String((self.now_supplier)()),
        );
        if let Some(FeelValue::Map(ev)) = vars.get("event") {
            if let Some(FeelValue::String(body)) = ev.get("body") {
                model.insert(
                    "sourceXml".to_string(),
                    serde_json::Value::String(body.clone()),
                );
            }
        }
        serde_json::Value::Object(model)
    }

    // ---- script / business-rule tasks -----------------------------------------------

    fn run_script_task(
        &self,
        id: &str,
        file: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let key = self.artifact_key(state, ArtifactType::Script, file);
        let bytes = self.scripts.find(&key).ok_or_else(|| {
            fatal_diag(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "<scriptTask> {id} names script '{file}' but no such file is registered \
                     for deployment '{}'. A <bpmn:scriptTask><bpmn:script>{file}</bpmn:script> \
                     requires a matching file under the module version's scripts/ folder.",
                    state.deployment.value()
                ),
            )
        })?;
        if self.template_engines.is_empty() {
            return Err(fatal_diag(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "<scriptTask> {id} needs a template engine to render '{file}' but no \
                     TemplateEngineRegistry is configured."
                ),
            ));
        }
        let engine = self
            .template_engines
            .for_implementation(file)
            .ok_or_else(|| {
                fatal_diag(
                    codes::RESOLVE_TEMPLATE_UNKNOWN,
                    format!(
                        "<scriptTask> {id} script '{file}' has no template engine for its \
                     extension. Registered engines: {:?}.",
                        self.template_engines.names()
                    ),
                )
            })?;
        // The `sutra.script` waterfall span (a script task IS a template render).
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_SCRIPT,
            deployment.id = %state.deployment.value(),
            instance.id = %state.instance_id,
            task.name = %file,
        )
        .entered();
        let vars = state.ctx.borrow().variables.clone();
        let model = self.render_model(&vars, &Variables::new());
        let rendered = engine
            .render(&key, bytes, &model)
            .map_err(|e| fatal_diag(codes::RUNTIME_UNEXPECTED, e))?;
        let merged = parse_script_output(id, &rendered)?;
        state.ctx.borrow_mut().variables.merge(&merged);
        Ok(())
    }

    fn run_business_rule_task(
        &self,
        id: &str,
        file: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let key = self.artifact_key(state, ArtifactType::Decision, file);
        let bytes = self.decisions.find(&key).ok_or_else(|| {
            fatal_diag(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "<businessRuleTask> {id} names decision '{file}' but no such file is \
                     registered for deployment '{}'. It requires a matching file under the \
                     module version's decisions/ folder.",
                    state.deployment.value()
                ),
            )
        })?;
        if self.decision_engines.is_empty() {
            return Err(fatal_diag(
                codes::RESOLVE_TEMPLATE_UNKNOWN,
                format!(
                    "<businessRuleTask> {id} needs a decision engine to evaluate '{file}' but \
                     no DecisionEngineRegistry is configured."
                ),
            ));
        }
        let engine = self
            .decision_engines
            .for_implementation(file)
            .ok_or_else(|| {
                fatal_diag(
                    codes::RESOLVE_TEMPLATE_UNKNOWN,
                    format!(
                        "<businessRuleTask> {id} decision '{file}' has no decision engine for its \
                     extension. Registered engines: {:?}.",
                        self.decision_engines.names()
                    ),
                )
            })?;
        // The `sutra.decision` waterfall span.
        let _span = tracing::info_span!(
            crate::telemetry::SPAN_DECISION,
            deployment.id = %state.deployment.value(),
            instance.id = %state.instance_id,
            task.name = %file,
        )
        .entered();
        let vars = state.ctx.borrow().variables.clone();
        let result = engine
            .evaluate(&key, bytes, &vars)
            .map_err(|e| fatal_diag(codes::RUNTIME_UNEXPECTED, e))?;
        if !result.is_empty() {
            state.ctx.borrow_mut().variables.merge(&result);
        }
        Ok(())
    }

    fn artifact_key(
        &self,
        state: &ExecutionState<'_>,
        artifact_type: ArtifactType,
        file_name: &str,
    ) -> String {
        if state.deployment.is_resolved() {
            state.deployment.artifact(artifact_type, file_name)
        } else {
            file_name.to_string()
        }
    }

    // ---- declarative data task (the BPMN data store) ------------------------------

    async fn run_data_task(
        &self,
        id: &str,
        dm: &DataMapping,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        for read in &dm.store_reads {
            let vars = state.ctx.borrow().variables.clone();
            let key_value =
                (self.value_evaluator)(&read.key_expression, &vars).map_err(Signal::Fatal)?;
            let key = as_store_key(&key_value, &read.key_expression, id)?;
            // Store ops are async + typed. A backend `Err` fails the instance CLOSED at
            // this step boundary (the old `last_error()` poison check, now `?` on Result).
            let (value, rev) =
                if state.store_tx.is_some() {
                    let tx = self.tx_for(&read.store, id, state).await?;
                    let value = if read.for_update {
                        tx.get_for_update(&key).await
                    } else {
                        tx.get(&key).await
                    }
                    .map_err(|e| store_op_failed(&read.store, &key, id, "read", &e.to_string()))?;
                    let rev = tx.revision(&key).await.map_err(|e| {
                        store_op_failed(&read.store, &key, id, "read", &e.to_string())
                    })?;
                    (value, rev)
                } else {
                    let store = self.resolve_store(&read.store, id, state)?;
                    let value = store.get(&key).await.map_err(|e| {
                        store_op_failed(&read.store, &key, id, "read", &e.to_string())
                    })?;
                    let rev = store.revision(&key).await.map_err(|e| {
                        store_op_failed(&read.store, &key, id, "read", &e.to_string())
                    })?;
                    (value, rev)
                };
            state
                .store_revs
                .insert(format!("{}#{key}", read.store), rev);
            state
                .ctx
                .borrow_mut()
                .variables
                .insert(read.target_var.clone(), value.unwrap_or(FeelValue::Null));
        }
        for assign in &dm.assignments {
            let vars = state.ctx.borrow().variables.clone();
            let value = (self.value_evaluator)(&assign.expression, &vars).map_err(Signal::Fatal)?;
            state
                .ctx
                .borrow_mut()
                .variables
                .insert(assign.target_var.clone(), value);
        }
        for write in &dm.store_writes {
            let vars = state.ctx.borrow().variables.clone();
            let key_value =
                (self.value_evaluator)(&write.key_expression, &vars).map_err(Signal::Fatal)?;
            let key = as_store_key(&key_value, &write.key_expression, id)?;
            let value = vars
                .get(&write.value_var)
                .cloned()
                .unwrap_or(FeelValue::Null);
            if state.store_tx.is_some() {
                let tx = self.tx_for(&write.store, id, state).await?;
                let current = tx.get(&key).await.map_err(|e| {
                    store_op_failed(&write.store, &key, id, "write", &e.to_string())
                })?;
                let to_store = merge_field(write, value, current);
                if write.expect_unchanged {
                    let expected = *state
                        .store_revs
                        .get(&format!("{}#{key}", write.store))
                        .unwrap_or(&0);
                    let applied =
                        tx.put_if_revision(&key, to_store, expected)
                            .await
                            .map_err(|e| {
                                store_op_failed(&write.store, &key, id, "write", &e.to_string())
                            })?;
                    if !applied {
                        return Err(store_conflict(write, &key, expected, id));
                    }
                } else {
                    tx.put(&key, to_store).await.map_err(|e| {
                        store_op_failed(&write.store, &key, id, "write", &e.to_string())
                    })?;
                }
            } else {
                let store = self.resolve_store(&write.store, id, state)?;
                let current = if write.field.is_some() {
                    store.get(&key).await.map_err(|e| {
                        store_op_failed(&write.store, &key, id, "write", &e.to_string())
                    })?
                } else {
                    None
                };
                let to_store = merge_field(write, value, current);
                if write.expect_unchanged {
                    let expected = *state
                        .store_revs
                        .get(&format!("{}#{key}", write.store))
                        .unwrap_or(&0);
                    let applied = store
                        .put_if_revision(&key, to_store, expected)
                        .await
                        .map_err(|e| {
                            store_op_failed(&write.store, &key, id, "write", &e.to_string())
                        })?;
                    if !applied {
                        return Err(store_conflict(write, &key, expected, id));
                    }
                } else {
                    store.put(&key, to_store).await.map_err(|e| {
                        store_op_failed(&write.store, &key, id, "write", &e.to_string())
                    })?;
                }
            }
        }
        Ok(())
    }

    fn resolve_store(
        &self,
        store_name: &str,
        node_id: &str,
        state: &ExecutionState<'_>,
    ) -> Result<Rc<dyn DataStore>, Signal> {
        let Some(registry) = self.data_stores.as_ref() else {
            return Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Data task '{node_id}' references data store '{store_name}' but no \
                     DataStoreRegistry is wired (TokenExecutor.Builder#withDataStores)."
                ),
            ));
        };
        registry(&state.deployment, store_name).ok_or_else(|| {
            fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Data task '{node_id}' references data store '{store_name}' which is not \
                     declared in deployment {} (datastores.yaml).",
                    state.deployment.value()
                ),
            )
        })
    }

    /// The open transaction for `store_name` in the active scope, opened lazily on first use.
    async fn tx_for(
        &self,
        store_name: &str,
        node_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<Rc<dyn DataStoreTx>, Signal> {
        if let Some(tx) = state
            .store_tx
            .as_ref()
            .and_then(|m| m.get(store_name))
            .cloned()
        {
            return Ok(tx);
        }
        let store = self.resolve_store(store_name, node_id, state)?;
        let tx = store
            .begin()
            .await
            .map_err(|e| store_op_failed(store_name, "", node_id, "begin", &e.to_string()))?
            .ok_or_else(|| {
                fatal_diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!(
                        "Data store '{store_name}' does not support transactions (data task \
                         '{node_id}' runs inside a <bpmn:transaction> scope)."
                    ),
                )
            })?;
        state
            .store_tx
            .as_mut()
            .expect("caller checked store_tx presence")
            .insert(store_name.to_string(), Rc::clone(&tx));
        Ok(tx)
    }

    // ---- gateways -------------------------------------------------------------------

    fn handle_exclusive(
        &self,
        id: &str,
        default_flow_id: Option<&str>,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let process = state.process;
        let outgoing = process.outgoing(id);
        let vars = state.ctx.borrow().variables.clone();
        let mut chosen: Option<&SequenceFlow> = None;
        for flow in &outgoing {
            if default_flow_id == Some(flow.id.as_str()) {
                continue;
            }
            let satisfied = match &flow.condition {
                None => true,
                Some(cond) => (self.condition_evaluator)(cond, &vars).map_err(Signal::Fatal)?,
            };
            if satisfied {
                chosen = Some(flow);
                break;
            }
        }
        if chosen.is_none() {
            if let Some(default_id) = default_flow_id {
                chosen = outgoing.iter().find(|f| f.id == default_id).copied();
            }
        }
        let Some(flow) = chosen else {
            return Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!("ExclusiveGateway {id} has no satisfied condition and no default flow"),
            ));
        };
        state.fire(flow);
        Ok(())
    }

    /// Returns true if the token was deferred (on_token_left already emitted).
    fn handle_inclusive(
        &self,
        id: &str,
        default_flow_id: Option<&str>,
        state: &mut ExecutionState<'_>,
        token_event: &TokenEvent,
    ) -> Result<bool, Signal> {
        let process = state.process;
        let incoming = process.incoming(id);
        if incoming.len() > 1 {
            // Joining side: track arrivals, fire when the expected count is reached.
            let arrivals = {
                let e = state.inclusive_arrivals.entry(id.to_string()).or_insert(0);
                *e += 1;
                *e
            };
            let expected = *state.inclusive_expected.get(id).unwrap_or(&0);
            if expected > 0 && arrivals < expected {
                self.notify(|l| l.on_token_left(token_event));
                return Ok(true);
            }
            state.inclusive_arrivals.remove(id);
            state.inclusive_expected.remove(id);
            for flow in process.outgoing(id) {
                state.fire(flow);
            }
            return Ok(false);
        }
        // Forking side: take every flow whose condition holds; default if none.
        let outgoing = process.outgoing(id);
        let vars = state.ctx.borrow().variables.clone();
        let mut taken = 0;
        for flow in &outgoing {
            if default_flow_id == Some(flow.id.as_str()) {
                continue;
            }
            let satisfied = match &flow.condition {
                None => true,
                Some(cond) => (self.condition_evaluator)(cond, &vars).map_err(Signal::Fatal)?,
            };
            if satisfied {
                self.route_inclusive_fork(flow, state);
                taken += 1;
            }
        }
        if taken == 0 {
            if let Some(default_id) = default_flow_id {
                if let Some(flow) = outgoing.iter().find(|f| f.id == default_id) {
                    self.route_inclusive_fork(flow, state);
                    taken += 1;
                }
            }
        }
        if taken == 0 {
            return Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!("InclusiveGateway {id} has no satisfied condition and no default flow"),
            ));
        }
        Ok(false)
    }

    fn route_inclusive_fork(&self, flow: &SequenceFlow, state: &mut ExecutionState<'_>) {
        state.fire(flow);
        for join_id in reachable_inclusive_joins(state.process, &flow.target_ref) {
            *state.inclusive_expected.entry(join_id).or_insert(0) += 1;
        }
    }

    fn handle_parallel(
        &self,
        id: &str,
        state: &mut ExecutionState<'_>,
        token_event: &TokenEvent,
    ) -> Result<bool, Signal> {
        let process = state.process;
        let incoming = process.incoming(id);
        if incoming.len() > 1 {
            let arrived = {
                let e = state.parallel_arrivals.entry(id.to_string()).or_insert(0);
                *e += 1;
                *e
            };
            if arrived < incoming.len() {
                self.notify(|l| l.on_token_left(token_event));
                return Ok(true);
            }
        }
        for flow in process.outgoing(id) {
            state.fire(flow);
        }
        Ok(false)
    }

    fn handle_complex(
        &self,
        id: &str,
        default_flow_id: Option<&str>,
        activation_condition: Option<&str>,
        state: &mut ExecutionState<'_>,
        token_event: &TokenEvent,
    ) -> Result<bool, Signal> {
        let process = state.process;
        let incoming = process.incoming(id);
        if incoming.len() > 1 {
            // Converging N-of-M join; a late token after the firing is absorbed.
            if state.complex_fired.contains(id) {
                self.notify(|l| l.on_token_left(token_event));
                return Ok(true);
            }
            let arrived = {
                let e = state.complex_arrivals.entry(id.to_string()).or_insert(0);
                *e += 1;
                *e
            };
            let expected = incoming.len();
            let fire = match activation_condition {
                None => arrived >= expected, // no condition ⇒ wait for all (AND-join)
                Some(cond) => {
                    let mut vars = state.ctx.borrow().variables.clone();
                    vars.insert("arrivedCount", FeelValue::from(arrived as i64));
                    vars.insert("expectedCount", FeelValue::from(expected as i64));
                    (self.condition_evaluator)(cond, &vars).map_err(Signal::Fatal)?
                }
            };
            if !fire {
                self.notify(|l| l.on_token_left(token_event));
                return Ok(true);
            }
            state.complex_fired.insert(id.to_string());
            state.complex_arrivals.remove(id);
            for flow in process.outgoing(id) {
                state.fire(flow);
            }
            return Ok(false);
        }
        // Diverging fork — inclusive semantics.
        let outgoing = process.outgoing(id);
        let vars = state.ctx.borrow().variables.clone();
        let mut taken = 0;
        for flow in &outgoing {
            if default_flow_id == Some(flow.id.as_str()) {
                continue;
            }
            let satisfied = match &flow.condition {
                None => true,
                Some(cond) => (self.condition_evaluator)(cond, &vars).map_err(Signal::Fatal)?,
            };
            if satisfied {
                self.route_inclusive_fork(flow, state);
                taken += 1;
            }
        }
        if taken == 0 {
            if let Some(default_id) = default_flow_id {
                if let Some(flow) = outgoing.iter().find(|f| f.id == default_id) {
                    self.route_inclusive_fork(flow, state);
                    taken += 1;
                }
            }
        }
        if taken == 0 {
            return Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "ComplexGateway {id} (diverging) has no satisfied condition and no \
                     default flow"
                ),
            ));
        }
        Ok(false)
    }

    // ---- call activity (q:dispatch + q:case) ------------------------------------------

    #[async_recursion(?Send)]
    async fn run_call_activity(
        &self,
        id: &str,
        called_element: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let bindings = state.process.bindings_for(id);
        let outcome = self.choose_called_element(id, called_element, bindings, state)?;
        let Some(selected) = outcome else {
            // onNoMatch=skip — audit the skip and let the parent token advance.
            let skip = DispatchEvent {
                deployment: state.deployment.clone(),
                labels: state.labels.clone(),
                instance_id: state.instance_id.clone(),
                node_id: id.to_string(),
                default_called_element: bindings
                    .dispatch
                    .as_ref()
                    .and_then(|d| d.default_called_element.clone()),
            };
            self.notify(|l| l.on_dispatch_skipped(&skip));
            return Ok(());
        };
        let (sub, sub_deployment) = self.resolve_sub_process(id, &selected, state)?;
        self.run_sub_process_inline(&sub, sub_deployment, state)
            .await
    }

    /// The dispatch decision — `Ok(None)` = skipped (onNoMatch=skip, no default).
    fn choose_called_element(
        &self,
        ca_id: &str,
        static_called_element: &str,
        bindings: &NodeBindings,
        state: &ExecutionState<'_>,
    ) -> Result<Option<String>, Signal> {
        let Some(dispatch) = bindings.dispatch.as_ref() else {
            return Ok(Some(static_called_element.to_string()));
        };
        let vars = state.ctx.borrow().variables.clone();
        for case in &dispatch.cases {
            let matched = (self.condition_evaluator)(&case.when, &vars).map_err(|e| {
                fatal_diag(
                    codes::DISPATCH_FEEL_EVAL_FAILED,
                    format!(
                        "Evaluating <q:case when=\"{}\"> on call activity {ca_id} failed: {}",
                        case.when,
                        e.message()
                    ),
                )
            })?;
            if matched {
                return Ok(Some(case.called_element.clone()));
            }
        }
        if let Some(default) = &dispatch.default_called_element {
            return Ok(Some(default.clone()));
        }
        if dispatch.on_no_match == OnNoMatch::Skip {
            return Ok(None);
        }
        Err(fatal_diag(
            codes::DISPATCH_NO_MATCH,
            format!(
                "Call activity {ca_id} in process {}: no <q:case> @when matched and no \
                 <q:dispatch default=...> was set (onNoMatch=error)",
                state.process.id
            ),
        ))
    }

    /// Resolve a call activity's selected `calledElement`, version-aware (VM-7b): a bare id
    /// with a known caller deployment resolves STRICTLY within that deployment.
    fn resolve_sub_process(
        &self,
        ca_id: &str,
        called_element: &str,
        state: &ExecutionState<'_>,
    ) -> Result<(Arc<ProcessDefinition>, DeploymentId), Signal> {
        if let Some(module_resolver) = self.module_resolver.as_ref() {
            if state.deployment.is_resolved() {
                if let Some(hit) = module_resolver(&state.deployment, called_element) {
                    return Ok((hit, state.deployment.clone()));
                }
                return Err(fatal_diag(
                    codes::DISPATCH_SUB_PROCESS_NOT_FOUND,
                    format!(
                        "Call activity {ca_id} calledElement '{called_element}' resolves to \
                         no process in the caller's deployment '{}'. A bare calledElement \
                         must name a sibling process in the caller's own deployment; to call \
                         across modules, import the target and qualify it with the module's \
                         namespace.",
                        state.deployment.value()
                    ),
                ));
            }
        }
        if let Some(process_resolver) = self.process_resolver.as_ref() {
            if let Some(resolved) = process_resolver(called_element).map_err(Signal::Fatal)? {
                return Ok((resolved, DeploymentId::unresolved()));
            }
        }
        Err(fatal_diag(
            codes::DISPATCH_SUB_PROCESS_NOT_FOUND,
            format!(
                "Call activity {ca_id} selected calledElement={called_element} but no \
                 process with that id is registered"
            ),
        ))
    }

    /// Runs the sub-process inline against the same variable map as the parent.
    #[async_recursion(?Send)]
    async fn run_sub_process_inline(
        &self,
        sub: &ProcessDefinition,
        sub_deployment: DeploymentId,
        parent: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let mut sub_state = ExecutionState::new(
            sub,
            sub_deployment,
            parent.labels.clone(),
            parent.instance_id.clone(),
            Rc::clone(&parent.ctx),
        );
        sub_state
            .work
            .push_back(sub.start_event().map_err(fatal)?.id().to_string());
        while let Some(node_id) = sub_state.work.pop_front() {
            match self.run_node(&node_id, &mut sub_state).await {
                Ok(()) => {}
                Err(Signal::BpmnError { source, code }) => {
                    self.route_error(&source, &code, &mut sub_state).await?;
                }
                Err(other) => return Err(other),
            }
        }
        Ok(())
    }

    /// Run an embedded sub-process inline, sharing the parent's variable scope. An
    /// unhandled inner error re-raises against the sub-process node in the parent scope.
    #[async_recursion(?Send)]
    async fn run_embedded_sub_process(
        &self,
        sp_id: &str,
        inner: &ProcessDefinition,
        parent: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let mut sub_state = ExecutionState::new(
            inner,
            parent.deployment.clone(),
            parent.labels.clone(),
            parent.instance_id.clone(),
            Rc::clone(&parent.ctx),
        );
        // The sub-state's wait frontier is DISCARDED when this runner returns (only `visited`
        // flows back to the parent), so nothing here can park: a `<q:retry>` backoff timer
        // recorded on it would be silently dropped. The loader refuses the placement; this is the
        // runtime floor that makes the refusal unnecessary to trust.
        sub_state.retry_parkable = false;
        let result: Result<(), Signal> = async {
            sub_state
                .work
                .push_back(inner.start_event().map_err(fatal)?.id().to_string());
            while let Some(node_id) = sub_state.work.pop_front() {
                match self.run_node(&node_id, &mut sub_state).await {
                    Ok(()) => {}
                    Err(Signal::BpmnError { source, code }) => {
                        if find_error_boundary(inner, &source, &code).is_some()
                            || find_error_event_sub_process(inner, &code).is_some()
                        {
                            self.route_error(&source, &code, &mut sub_state).await?;
                        } else {
                            // Propagate to a boundary attached to the sub-process node.
                            return Err(Signal::BpmnError {
                                source: sp_id.to_string(),
                                code,
                            });
                        }
                    }
                    Err(other) => return Err(other),
                }
            }
            Ok(())
        }
        .await;
        parent.visited.extend(sub_state.visited.drain());
        result
    }

    /// Run a `<bpmn:transaction>` inline inside ONE store transaction; returns `true`
    /// when the scope was cancelled (roll back → route the cancel boundary).
    #[async_recursion(?Send)]
    async fn run_transaction_sub_process(
        &self,
        tsp_id: &str,
        inner: &ProcessDefinition,
        parent: &mut ExecutionState<'_>,
    ) -> Result<bool, Signal> {
        let mut sub_state = ExecutionState::new(
            inner,
            parent.deployment.clone(),
            parent.labels.clone(),
            parent.instance_id.clone(),
            Rc::clone(&parent.ctx),
        );
        sub_state.store_tx = Some(HashMap::new()); // active transaction scope
        let result: Result<bool, Signal> = async {
            sub_state
                .work
                .push_back(inner.start_event().map_err(fatal)?.id().to_string());
            let mut cancelled = false;
            while let Some(node_id) = sub_state.work.pop_front() {
                match self.run_node(&node_id, &mut sub_state).await {
                    Ok(()) => {}
                    Err(Signal::TxCancel) => {
                        cancelled = true;
                        sub_state.work.clear();
                        break;
                    }
                    Err(Signal::BpmnError { source, code }) => {
                        if find_error_boundary(inner, &source, &code).is_some()
                            || find_error_event_sub_process(inner, &code).is_some()
                        {
                            self.route_error(&source, &code, &mut sub_state).await?;
                        } else {
                            // Error escaping the transaction → roll back + interrupt the
                            // outer flow at the transaction node.
                            return Err(Signal::BpmnError {
                                source: tsp_id.to_string(),
                                code,
                            });
                        }
                    }
                    Err(other) => return Err(other),
                }
            }
            if cancelled {
                // Compensate the completed inner activities BEFORE the rollback, so a
                // compensation handler still sees the transaction's pre-rollback data.
                self.fire_compensation(&mut sub_state, None).await?;
            } else if let Some(err) = commit_store_tx(&mut sub_state).await {
                return Err(fatal_diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!("<bpmn:transaction> '{tsp_id}' data-store {err}"),
                ));
            }
            Ok(cancelled)
        }
        .await;
        rollback_store_tx(&mut sub_state).await; // no-op after commit; rolls back on cancel/escape
        parent.visited.extend(sub_state.visited.drain());
        result
    }

    /// Track H — run an ad-hoc sub-process: its task activities in document order, stopping
    /// early when the FEEL completion condition holds.
    #[async_recursion(?Send)]
    async fn run_ad_hoc_sub_process(
        &self,
        inner: &ProcessDefinition,
        completion_condition: Option<&str>,
        parent: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let mut sub_state = ExecutionState::new(
            inner,
            parent.deployment.clone(),
            parent.labels.clone(),
            parent.instance_id.clone(),
            Rc::clone(&parent.ctx),
        );
        sub_state.store_tx = parent.store_tx.take(); // join the enclosing tx scope, if any
        let result: Result<(), Signal> = async {
            for activity in inner.nodes() {
                if !is_ad_hoc_activity(activity) {
                    continue;
                }
                self.run_ad_hoc_activity(activity, &mut sub_state).await?;
                if let Some(expr) = completion_condition {
                    let vars = sub_state.ctx.borrow().variables.clone();
                    let done = (self.condition_evaluator)(expr, &vars).map_err(|e| {
                        fatal_diag(
                            codes::RUNTIME_ADHOC_COMPLETION_FAILED,
                            format!(
                                "Ad-hoc sub-process completionCondition '{expr}' failed: {}",
                                e.message()
                            ),
                        )
                    })?;
                    if done {
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;
        parent.store_tx = sub_state.store_tx.take();
        parent.visited.extend(sub_state.visited.drain());
        result
    }

    #[async_recursion(?Send)]
    async fn run_ad_hoc_activity(
        &self,
        activity: &Node,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        match activity {
            Node::ServiceTask {
                id,
                implementation,
                data_mapping,
                params,
                ..
            } => {
                // `false`: an ad-hoc iteration is an INLINE runner with no wait frontier of its
                // own, so a `<q:retry>` here fails closed rather than parking a timer nothing
                // would persist. The return can only be `false`.
                self.run_service_task(id, implementation, data_mapping, params, false, state)
                    .await?;
                state.completed_activities.push(id.clone());
                self.emit_reply_if_bound(id, state)?;
                self.emit_send_if_bound(id, state, false)?;
                Ok(())
            }
            Node::DataTask {
                id, data_mapping, ..
            } => {
                self.run_data_task(id, data_mapping, state).await?;
                state.completed_activities.push(id.clone());
                self.emit_reply_if_bound(id, state)
            }
            Node::ScriptTask {
                id, script_file, ..
            } => {
                self.run_script_task(id, script_file, state)?;
                state.completed_activities.push(id.clone());
                Ok(())
            }
            Node::BusinessRuleTask {
                id, decision_file, ..
            } => {
                self.run_business_rule_task(id, decision_file, state)?;
                state.completed_activities.push(id.clone());
                Ok(())
            }
            Node::SendTask { id, .. } => {
                state.completed_activities.push(id.clone());
                self.emit_send_if_bound(id, state, true)
            }
            Node::ManualTask { id, .. } => {
                state.completed_activities.push(id.clone());
                Ok(())
            }
            Node::CallActivity {
                id, called_element, ..
            } => {
                self.run_call_activity(id, called_element, state).await?;
                state.completed_activities.push(id.clone());
                Ok(())
            }
            other => Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Ad-hoc sub-process activity '{}' of type {} is not a runnable task \
                     activity.",
                    other.id(),
                    node_type(other)
                ),
            )),
        }
    }

    /// Track H — run an error-triggered event sub-process inline; because it is
    /// interrupting, its end IS the enclosing scope's end.
    #[async_recursion(?Send)]
    async fn run_event_sub_process(
        &self,
        esp_id: &str,
        inner: &ProcessDefinition,
        parent: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let mut sub_state = ExecutionState::new(
            inner,
            parent.deployment.clone(),
            parent.labels.clone(),
            parent.instance_id.clone(),
            Rc::clone(&parent.ctx),
        );
        sub_state.store_tx = parent.store_tx.take();
        let result: Result<(), Signal> = async {
            sub_state
                .work
                .push_back(inner.start_event().map_err(fatal)?.id().to_string());
            while let Some(node_id) = sub_state.work.pop_front() {
                match self.run_node(&node_id, &mut sub_state).await {
                    Ok(()) => {}
                    Err(Signal::BpmnError { source, code }) => {
                        if find_error_boundary(inner, &source, &code).is_some()
                            || find_error_event_sub_process(inner, &code).is_some()
                        {
                            self.route_error(&source, &code, &mut sub_state).await?;
                        } else {
                            return Err(Signal::BpmnError {
                                source: esp_id.to_string(),
                                code,
                            });
                        }
                    }
                    Err(other) => return Err(other),
                }
            }
            Ok(())
        }
        .await;
        parent.store_tx = sub_state.store_tx.take();
        parent.visited.extend(sub_state.visited.drain());
        result?;
        if sub_state.reached_end {
            parent.reached_end = true;
            parent.end_outputs = sub_state.end_outputs.clone();
        }
        Ok(())
    }

    // ---- multi-instance / standard loop -------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    #[async_recursion(?Send)]
    async fn run_multi_instance(
        &self,
        id: &str,
        inner: &Node,
        loop_cardinality: Option<&str>,
        loop_data_input_ref: Option<&str>,
        input_data_item: Option<&str>,
        completion_condition: Option<&str>,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let cardinality = self.resolve_cardinality(id, loop_cardinality, state)?;
        let collection = self.resolve_collection(id, loop_data_input_ref, state)?;
        let iterations = collection.as_ref().map(|c| c.len()).unwrap_or(cardinality);
        let item_var = input_data_item.unwrap_or(sutra_bpmn::DEFAULT_LOOP_ITEM_VARIABLE);

        for i in 0..iterations {
            {
                let mut ctx = state.ctx.borrow_mut();
                if let Some(items) = &collection {
                    let item = &items[i];
                    if !item.is_null() {
                        ctx.variables.insert(item_var, item.clone());
                    }
                }
                ctx.variables
                    .insert(sutra_bpmn::LOOP_COUNTER_VARIABLE, FeelValue::from(i as i64));
            }
            self.execute_inner(inner, state).await?;
            if let Some(expr) = completion_condition {
                let vars = state.ctx.borrow().variables.clone();
                let done = (self.condition_evaluator)(expr, &vars).map_err(|e| {
                    fatal_diag(
                        codes::RUNTIME_MULTI_INSTANCE_COMPLETION_FAILED,
                        format!(
                            "Multi-instance {id} completionCondition '{expr}' failed: {}",
                            e.message()
                        ),
                    )
                })?;
                if done {
                    break;
                }
            }
        }
        Ok(())
    }

    fn resolve_cardinality(
        &self,
        id: &str,
        loop_cardinality: Option<&str>,
        state: &ExecutionState<'_>,
    ) -> Result<usize, Signal> {
        let Some(expr) = loop_cardinality else {
            return Ok(0);
        };
        if let Ok(n) = expr.trim().parse::<usize>() {
            return Ok(n);
        }
        // Treat as a variable reference holding a number.
        if let Some(FeelValue::Number(n)) = state.ctx.borrow().variables.get(expr.trim()) {
            use bigdecimal::ToPrimitive;
            if let Some(v) = n.to_i64() {
                return Ok(v.max(0) as usize);
            }
        }
        Err(fatal_diag(
            codes::RUNTIME_UNEXPECTED,
            format!("Multi-instance {id} loopCardinality '{expr}' is not an integer"),
        ))
    }

    /// The collection a multi-instance loop iterates.
    ///
    /// `<bpmn:loopDataInputRef>` is evaluated as a FEEL EXPRESSION, not looked up as a bare
    /// variable name. A bare name is itself a valid FEEL expression, so every existing loop is
    /// unaffected — but a PATH now works too (`payload.value`), and that is what lets a batch
    /// flow iterate the decoded payload directly instead of copying the whole collection into a
    /// second variable first. That copy was not free: process variables are persisted in the
    /// instance snapshot, so a copied batch was a second CLOB of the decoded one on every park.
    ///
    /// An expression that resolves to nothing FAILS CLOSED. It used to yield an empty collection,
    /// which meant a loop over a variable that had been dropped (or misspelled) iterated zero
    /// times, the instance completed normally, and the batch vanished with no error anywhere —
    /// silent data loss wearing the shape of success. An explicitly EMPTY list is still a
    /// legitimate zero-iteration loop; it is absence that is now refused.
    fn resolve_collection(
        &self,
        id: &str,
        loop_data_input_ref: Option<&str>,
        state: &ExecutionState<'_>,
    ) -> Result<Option<Vec<FeelValue>>, Signal> {
        let Some(expression) = loop_data_input_ref else {
            return Ok(None);
        };
        let vars = state.ctx.borrow().variables.clone();
        // A BARE name resolves by direct lookup, exactly as before — no FEEL evaluator required.
        // That keeps every existing loop working in an executor that never wired one (the
        // evaluator is optional; only data-store keys and assignments needed it until now), and
        // limits the new dependency to the case that actually needs it: a path.
        let value = match vars
            .get(expression)
            .filter(|_| is_bare_identifier(expression))
        {
            Some(value) => value.clone(),
            None => (self.value_evaluator)(expression, &vars).map_err(|e| {
                fatal_diag(
                    codes::RUNTIME_UNEXPECTED,
                    format!(
                        "Multi-instance {id} loopDataInputRef '{expression}' could not be \
                         evaluated: {}",
                        e.message()
                    ),
                )
            })?,
        };
        match value {
            FeelValue::List(items) => Ok(Some(items)),
            FeelValue::Null => Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Multi-instance {id} loopDataInputRef '{expression}' resolved to nothing. A \
                     loop over an absent collection would silently iterate zero times and report \
                     success, so it is refused: check the name, and that nothing dropped the \
                     value (a @transient variable does not survive a park)."
                ),
            )),
            _ => Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!("Multi-instance {id} loopDataInputRef '{expression}' is not a collection"),
            )),
        }
    }

    #[async_recursion(?Send)]
    async fn run_standard_loop(
        &self,
        id: &str,
        inner: &Node,
        loop_condition: Option<&str>,
        test_before: bool,
        loop_maximum: Option<i64>,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let max = loop_maximum.unwrap_or(i64::MAX);
        let mut i: i64 = 0;
        if test_before {
            while i < max && self.loop_continues(id, loop_condition, state)? {
                state
                    .ctx
                    .borrow_mut()
                    .variables
                    .insert(sutra_bpmn::LOOP_COUNTER_VARIABLE, FeelValue::from(i));
                self.execute_inner(inner, state).await?;
                i += 1;
            }
        } else {
            loop {
                state
                    .ctx
                    .borrow_mut()
                    .variables
                    .insert(sutra_bpmn::LOOP_COUNTER_VARIABLE, FeelValue::from(i));
                self.execute_inner(inner, state).await?;
                i += 1;
                if !(i < max && self.loop_continues(id, loop_condition, state)?) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn loop_continues(
        &self,
        id: &str,
        loop_condition: Option<&str>,
        state: &ExecutionState<'_>,
    ) -> Result<bool, Signal> {
        let Some(expr) = loop_condition else {
            return Ok(false);
        };
        let vars = state.ctx.borrow().variables.clone();
        (self.condition_evaluator)(expr, &vars).map_err(|e| {
            fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "StandardLoop {id} loopCondition '{expr}' failed: {}",
                    e.message()
                ),
            )
        })
    }

    #[async_recursion(?Send)]
    async fn execute_inner(
        &self,
        inner: &Node,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        match inner {
            Node::ServiceTask {
                id,
                implementation,
                data_mapping,
                params,
                ..
            } => {
                // `false`: a loop iteration is not a token position the engine can re-enter, so
                // a `<q:retry>` here fails closed (the loader already refuses the placement).
                self.run_service_task(id, implementation, data_mapping, params, false, state)
                    .await
                    .map(|_parked| ())
            }
            Node::ScriptTask {
                id, script_file, ..
            } => self.run_script_task(id, script_file, state),
            Node::ManualTask { .. } => Ok(()),
            Node::CallActivity {
                id, called_element, ..
            } => self.run_call_activity(id, called_element, state).await,
            Node::SubProcess { id, inner, .. } => {
                self.run_embedded_sub_process(id, inner, state).await
            }
            other => Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "Multi-instance inner node {} of type {} is not supported",
                    other.id(),
                    node_type(other)
                ),
            )),
        }
    }

    // ---- error / escalation / compensation ---------------------------------------------

    /// Routes a BPMN-error signal to a matching boundary event in scope, then to an
    /// error-triggered event sub-process; unhandled → fail closed.
    #[async_recursion(?Send)]
    async fn route_error(
        &self,
        source: &str,
        code: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let process = state.process;
        if let Some(boundary_id) = find_error_boundary(process, source, code) {
            for flow in process.outgoing(&boundary_id) {
                state.fire(flow);
            }
            state.visited.insert(boundary_id);
            return Ok(());
        }
        if let Some(esp_id) = find_error_event_sub_process(process, code) {
            state.work.clear();
            let Node::EventSubProcess { inner, .. } = process.node(&esp_id).map_err(fatal)? else {
                unreachable!("find_error_event_sub_process returns EventSubProcess ids");
            };
            return self.run_event_sub_process(&esp_id, inner, state).await;
        }
        state.work.clear();
        Err(fatal_diag(
            codes::RUNTIME_ERROR_UNCAUGHT,
            format!(
                "Activity {source} threw BPMN error {code} but no boundary event or event \
                 sub-process in scope caught it"
            ),
        ))
    }

    /// Route an escalation throw to matching in-process escalation boundaries by
    /// code; returns true when an interrupting boundary caught it.
    fn route_escalation(&self, code: Option<&str>, state: &mut ExecutionState<'_>) -> bool {
        let process = state.process;
        let code = code.unwrap_or("");
        let mut interrupting_caught = false;
        let mut fired: Vec<String> = Vec::new();
        for n in process.nodes() {
            if let Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Escalation,
                escalation_code,
                interrupting,
                ..
            } = n
            {
                let matches = escalation_code
                    .as_deref()
                    .map(|bec| bec == code)
                    .unwrap_or(true);
                if matches {
                    fired.push(id.clone());
                    if *interrupting {
                        interrupting_caught = true;
                    }
                }
            }
        }
        for boundary_id in fired {
            for flow in process.outgoing(&boundary_id) {
                state.fire(flow);
            }
            state.visited.insert(boundary_id);
        }
        interrupting_caught
    }

    /// The intra-process goto of a link throw.
    fn jump_to_link_catch(
        &self,
        ite_id: &str,
        link_name: Option<&str>,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let link_name = link_name.unwrap_or("");
        for n in state.process.nodes() {
            if let Node::LinkCatchEvent {
                id, link_name: ln, ..
            } = n
            {
                if ln == link_name {
                    state.work.push_back(id.clone());
                    return Ok(());
                }
            }
        }
        Err(fatal_diag(
            codes::RUNTIME_UNEXPECTED,
            format!(
                "Link throw {ite_id} on instance {} found no catch for link '{link_name}' at \
                 runtime (should have been caught at load)",
                state.instance_id
            ),
        ))
    }

    /// Route control out of a cancelled transaction's attached cancel boundary event.
    fn route_cancel_boundary(
        &self,
        node_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let process = state.process;
        for n in process.nodes() {
            if let Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Cancel,
                attached_to_ref,
                ..
            } = n
            {
                if attached_to_ref == node_id {
                    let boundary_id = id.clone();
                    for flow in process.outgoing(&boundary_id) {
                        state.fire(flow);
                    }
                    state.visited.insert(boundary_id);
                    return Ok(());
                }
            }
        }
        Err(fatal_diag(
            codes::RUNTIME_UNEXPECTED,
            format!(
                "Transaction '{node_id}' was cancelled but has no attached cancel boundary \
                 event to route to. Attach a <bpmn:boundaryEvent><bpmn:cancelEventDefinition/> \
                 to the transaction."
            ),
        ))
    }

    /// Fire the compensation boundary handlers for completed activities in LIFO order,
    /// optionally narrowed to a single activity (the throw's activityRef).
    async fn fire_compensation(
        &self,
        state: &mut ExecutionState<'_>,
        only_activity: Option<&str>,
    ) -> Result<(), Signal> {
        let process = state.process;
        let mut comp_by_activity: HashMap<String, String> = HashMap::new();
        for n in process.nodes() {
            if let Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Compensation,
                attached_to_ref,
                ..
            } = n
            {
                comp_by_activity.insert(attached_to_ref.clone(), id.clone());
            }
        }
        let completed: Vec<String> = state.completed_activities.iter().rev().cloned().collect();
        for activity_id in completed {
            if let Some(only) = only_activity {
                if only != activity_id {
                    continue;
                }
            }
            let Some(handler) = comp_by_activity.get(&activity_id).cloned() else {
                continue;
            };
            self.run_compensation_chain(&handler, state)
                .await
                .map_err(|signal| match signal {
                    Signal::Fatal(e) => fatal_diag(
                        codes::RUNTIME_COMPENSATION_FAILED,
                        format!(
                            "Compensation for activity {activity_id} failed: {}",
                            e.message()
                        ),
                    ),
                    other => other,
                })?;
        }
        Ok(())
    }

    /// Walks the compensation handler's outgoing chain until it dead-ends.
    async fn run_compensation_chain(
        &self,
        handler_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let process = state.process;
        let mut chain: VecDeque<String> = process
            .outgoing(handler_id)
            .iter()
            .map(|f| f.target_ref.clone())
            .collect();
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(id) = chain.pop_front() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let node = process.node(&id).map_err(fatal)?;
            match node {
                Node::ServiceTask {
                    id: st_id,
                    implementation,
                    data_mapping,
                    params,
                    ..
                } => {
                    // `false`: a compensation handler runs inside the unwinding chain, which
                    // has no quiescent point to park at.
                    self.run_service_task(
                        st_id,
                        implementation,
                        data_mapping,
                        params,
                        false,
                        state,
                    )
                    .await?;
                    for flow in process.outgoing(&id) {
                        chain.push_back(flow.target_ref.clone());
                    }
                }
                Node::EndEvent { .. } => break,
                _ => {
                    for flow in process.outgoing(&id) {
                        chain.push_back(flow.target_ref.clone());
                    }
                }
            }
        }
        Ok(())
    }

    // ---- emissions (q:reply / q:send → EmissionSink) -------------------------------------

    fn emit_reply_if_bound(
        &self,
        node_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let Some(rb) = state.process.bindings_for(node_id).reply.clone() else {
            return Ok(());
        };
        let destination = self.resolve_destination(&rb, state);
        let Some(destination) = destination else {
            if rb.required {
                return Err(fatal_diag(
                    codes::OUTBOUND_REPLY_DEST_REQUIRED_NOT_SET,
                    format!(
                        "Reply on instance {} node {node_id} is required=true but no \
                         destination resolved (no <q:reply destination=...> and no inbound \
                         reply-to override)",
                        state.instance_id
                    ),
                ));
            }
            // A <q:reply> with no resolved destination is a purely SYNCHRONOUS reply: its
            // body rides the inbound connection — nothing to deliver out-of-band.
            return Ok(());
        };
        let auth_ref = self.build_auth_ref(&rb, node_id)?;
        self.enqueue_outbound(
            EmissionKind::Reply,
            &rb,
            &destination,
            auth_ref,
            node_id,
            state,
        )?;
        Ok(())
    }

    fn emit_send_if_bound(
        &self,
        node_id: &str,
        state: &mut ExecutionState<'_>,
        required: bool,
    ) -> Result<(), Signal> {
        let Some(sb) = state.process.bindings_for(node_id).send.clone() else {
            if required {
                // Defensive — the parser (validate_throw_targets) already fails closed.
                return Err(fatal_diag(
                    sutra_bpmn::codes::PARSE_THROW_SEND_REQUIRED,
                    format!(
                        "Message throw {node_id} on instance {} has no <q:send> to emit",
                        state.instance_id
                    ),
                ));
            }
            return Ok(());
        };
        if let Some(channel_name) = sb.channel.as_deref() {
            return self.emit_send_via_channel(&sb, channel_name, node_id, state);
        }
        // <q:send destination="…"> — a send is a reply with a required, always-declared
        // destination and no reply-to override.
        let adapter = send_as_reply(&sb, sb.destination.clone());
        let destination = sb.destination.clone().expect("parser enforced destination");
        let auth_ref = self.build_auth_ref(&adapter, node_id)?;
        self.enqueue_outbound(
            EmissionKind::Send,
            &adapter,
            &destination,
            auth_ref,
            node_id,
            state,
        )?;
        Ok(())
    }

    fn emit_send_via_channel(
        &self,
        sb: &SendBinding,
        channel_name: &str,
        node_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let resolved = self
            .outbound_channels
            .find(&state.deployment, channel_name)
            .cloned()
            .ok_or_else(|| {
                fatal_diag(
                    codes::CONFIG_CHANNEL_OUTBOUND_UNKNOWN,
                    format!(
                        "Node '{node_id}' on instance '{}' does a <q:send channel='{channel_name}'> \
                         but no channel named '{channel_name}' is registered with direction: \
                         outbound.",
                        state.instance_id
                    ),
                )
            })?;
        // Channel mode is the default; a non-native <q:send mode> overrides it.
        let mode = if sb.mode == ReplyMode::Native {
            resolved.mode
        } else {
            sb.mode
        };
        let mut adapter = send_as_reply(sb, Some(resolved.destination.clone()));
        adapter.mode = mode;
        // Auth lives on the channel (never inline on a <q:send channel>).
        self.enqueue_outbound(
            EmissionKind::Send,
            &adapter,
            &resolved.destination,
            resolved.auth_ref.clone(),
            node_id,
            state,
        )?;
        Ok(())
    }

    fn enqueue_outbound(
        &self,
        kind: EmissionKind,
        rb: &ReplyBinding,
        destination: &str,
        auth_ref: Option<AuthRef>,
        node_id: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), Signal> {
        let body = extract_reply_body(state);
        let cloud_event = if rb.mode == ReplyMode::Native {
            None
        } else {
            Some(self.build_cloud_event(rb, state))
        };
        let headers = self.evaluate_outbound_headers(rb, node_id, state)?;
        let emission = Emission {
            kind,
            node_id: node_id.to_string(),
            instance_id: state.instance_id.clone(),
            mode: rb.mode,
            destination: destination.to_string(),
            content_type: rb.content_type.clone(),
            required: rb.required,
            body: body.into(),
            cloud_event,
            auth_ref,
            headers,
        };
        if let Some(sink) = self.emissions.as_ref() {
            sink.emit(emission);
        }
        let reply_event = ReplyEvent {
            deployment: state.deployment.clone(),
            labels: state.labels.clone(),
            instance_id: state.instance_id.clone(),
            node_id: node_id.to_string(),
            mode: rb.mode,
            destination: destination.to_string(),
        };
        self.notify(|l| l.on_reply_emitted(&reply_event));
        Ok(())
    }

    /// Evaluate each author-declared `<q:header value="<FEEL>">` against the
    /// sending process context (the same variables destinations/content resolve against). A header
    /// whose value resolves null is omitted (producers may legitimately not set it); a FEEL failure
    /// is fatal to the emission. The resolved `name → string` pairs ride the emission onto the wire
    /// as transport headers. Domain-neutral: the name is an opaque author string.
    fn evaluate_outbound_headers(
        &self,
        rb: &ReplyBinding,
        node_id: &str,
        state: &ExecutionState<'_>,
    ) -> Result<BTreeMap<String, String>, Signal> {
        let mut out = BTreeMap::new();
        if rb.headers.is_empty() {
            return Ok(out);
        }
        let vars = state.ctx.borrow().variables.clone();
        for header in &rb.headers {
            let value = (self.value_evaluator)(&header.value, &vars).map_err(|e| {
                fatal_diag(
                    codes::OUTBOUND_HEADER_FEEL_EVAL_FAILED,
                    format!(
                        "<q:header {}> value expression '{}' on node {node_id} threw at \
                         evaluation: {}",
                        header.name,
                        header.value,
                        e.message()
                    ),
                )
            })?;
            if value.is_null() {
                continue;
            }
            out.insert(
                header.name.clone(),
                sutra_feel::value::canonical_string_of(&value),
            );
        }
        Ok(out)
    }

    fn resolve_destination(&self, rb: &ReplyBinding, state: &ExecutionState<'_>) -> Option<String> {
        if let Some(d) = &rb.destination {
            return Some(d.clone());
        }
        // Inbound override capture — the dispatcher places the reply-to URI under "replyTo".
        match state.ctx.borrow().variables.get("replyTo") {
            Some(FeelValue::String(s)) if !s.trim().is_empty() => Some(s.clone()),
            _ => None,
        }
    }

    fn build_auth_ref(&self, rb: &ReplyBinding, node_id: &str) -> Result<Option<AuthRef>, Signal> {
        let Some(scheme) = rb.auth else {
            return Ok(None);
        };
        let Some(secret_ref) = rb.auth_secret_ref.clone() else {
            return Err(fatal_diag(
                codes::OUTBOUND_REPLY_AUTH_RESOLVER_NOT_FOUND,
                format!(
                    "Reply on node {node_id} declares auth={scheme:?} but no authSecretRef was set"
                ),
            ));
        };
        let uri_scheme = secret_ref.split_once(':').map(|(s, _)| s).unwrap_or("");
        if uri_scheme.trim().is_empty() {
            return Err(fatal_diag(
                codes::OUTBOUND_REPLY_AUTH_RESOLVER_NOT_FOUND,
                format!(
                    "Reply on node {node_id} authSecretRef '{secret_ref}' has no URI scheme — \
                     cannot dispatch to an AuthRefResolver"
                ),
            ));
        }
        let Some(resolvers) = self.auth_resolvers.as_ref() else {
            return Err(fatal_diag(
                codes::OUTBOUND_REPLY_AUTH_RESOLVER_NOT_FOUND,
                format!(
                    "Reply on node {node_id} declared auth={scheme:?} secretRef={secret_ref} \
                     but no AuthRefResolverRegistry is wired"
                ),
            ));
        };
        let auth_ref = AuthRef {
            scheme: auth_scheme_name(scheme).to_string(),
            secret_ref: secret_ref.clone(),
            header: rb.auth_header.clone(),
        };
        if resolvers(&auth_ref).is_none() {
            return Err(fatal_diag(
                codes::OUTBOUND_REPLY_AUTH_RESOLVER_NOT_FOUND,
                format!(
                    "Reply on node {node_id} declared auth={scheme:?} secretRef={secret_ref} \
                     but no AuthRefResolver claims URI scheme '{uri_scheme}'"
                ),
            ));
        }
        Ok(Some(auth_ref))
    }

    fn build_cloud_event(&self, rb: &ReplyBinding, state: &ExecutionState<'_>) -> CloudEventLite {
        CloudEventLite {
            id: state.instance_id.clone(),
            source: rb
                .ce_source
                .clone()
                .unwrap_or_else(|| format!("/sutra/instance/{}", state.instance_id)),
            spec_version: "1.0".to_string(),
            ce_type: rb
                .ce_type
                .clone()
                .unwrap_or_else(|| "io.sutra.reply.v1".to_string()),
            subject: rb.ce_subject.clone(),
            time: Some((self.now_supplier)()),
            data_content_type: rb
                .ce_data_content_type
                .clone()
                .or_else(|| rb.content_type.clone()),
        }
    }

    // ---- path coverage ---------------------------------------------------------------------

    /// At instance completion, mark every declared `<q:coverage>` path whose
    /// contiguous cursor reached its full length. The cursor is seeded from the prior snapshot on
    /// resume and advanced as flows fire, so a route spanning multiple wait states is marked even
    /// when the final replay diverges from the historically-taken route (the per-pass `flow_trace`
    /// alone would miss it). Best-effort throughout: a missing/failing store skips the metric,
    /// never crashing the instance.
    ///
    /// ONE surface per completed path — the typed metric store (`coverage_metric_store`). The
    /// dual write to the module KV `coverage` store was retired (RULED 2026-08-04):
    /// one number, one place. Pre-existing KV rows are
    /// deliberately left where they are, so a rollback to a prior engine build still finds them.
    ///
    /// The path id discriminates the two marking kinds (`split_injected_sub_path`):
    /// - **intra-process** path (author mnemonic) → flip its seeded metric flag `covered=true`.
    ///   The flip's "newly covered" answer is the durable first-covers-wins signal
    ///   `on_path_covered` (the `sutra.coverage.path_covered` counter + the
    ///   `sutra.coverage.percent` gauge) fires on — exactly what the KV `get`-before-`put` used
    ///   to provide.
    /// - **cross-process** desugar-injected sub-path (`…:<route>#<process>`) → write a
    ///   reconstruction fragment (with this pass's `instance_id` + correlation dims). We do NOT
    ///   flip the ROUTE flag here — `sutra coverage check` runs the union-find over the
    ///   fragments and flips the route once its cascade is complete — and therefore do NOT fire
    ///   `on_path_covered`: a segment completion is evidence, not coverage, and notifying per
    ///   completion would turn a coverage counter into a throughput counter.
    async fn mark_coverage(
        &self,
        process: &ProcessDefinition,
        state: &ExecutionState<'_>,
        instance_event: &InstanceEvent,
    ) {
        if process.coverage_paths.is_empty() {
            return;
        }
        // Nothing to do without the metric store (a host wired without one — such a deployment
        // has no coverage at all; `run_coverage_op` says so out loud).
        let Some(metric) = self.coverage_metric_store.as_ref() else {
            return;
        };
        let deployment_id = state.deployment.value();
        for path in &process.coverage_paths {
            let progress = state.path_progress.get(&path.id).copied().unwrap_or(0);
            if progress != path.flows.len() {
                continue;
            }
            match crate::coverage::split_injected_sub_path(&path.id) {
                Some((route_urn, segment_process)) => {
                    // Cross-process injected segment completed → reconstruction fragment. The
                    // ROUTE flag is left for `sutra coverage check`'s union-find.
                    let fragment = CoverageFragment {
                        route_urn,
                        segment_process,
                        instance_id: state.instance_id.clone(),
                        business_key: state.correlation.business_key.clone(),
                        trace_id: state.correlation.trace_id.clone(),
                    };
                    // Best-effort: coverage is a metric side-effect, never a reason to fail the
                    // instance (the report op is where a broken store surfaces loudly).
                    let _ = metric.write_fragment(deployment_id, &fragment).await;
                }
                None => {
                    // Intra-process path → flip its seeded metric flag, notifying only on a
                    // genuinely new cover.
                    if let Ok(true) = metric.mark_path_covered(deployment_id, &path.id).await {
                        self.notify(|l| l.on_path_covered(instance_event, &path.id));
                    }
                }
            }
        }
    }

    /// The reserved coverage admin ops: `coverage:report:<process>` / `coverage:reset:<process>`.
    async fn run_coverage_op(
        &self,
        node_id: &str,
        implementation: &str,
        state: &mut ExecutionState<'_>,
    ) -> Result<(), ExecError> {
        let mut parts = implementation.splitn(3, ':');
        let _prefix = parts.next();
        let op = parts.next().unwrap_or("").trim().to_string();
        let target_process_id = parts.next().unwrap_or("").trim().to_string();
        if target_process_id.is_empty() {
            return Err(ExecError::diag(
                sutra_bpmn::codes::RESOLVE_TASK_UNKNOWN,
                format!(
                    "coverage op '{implementation}' on '{node_id}' needs a target process: \
                     implementation=\"coverage:report:<process>\" or \
                     \"coverage:reset:<process>\"."
                ),
            ));
        }
        let target = self.resolve_coverage_target(&target_process_id, node_id, state)?;
        // No coverage store ⇒ no coverage, said out loud. Coverage marks are persisted in the
        // deployment's OWN declared `coverage` data store (—
        // the author picks the database via that store's data source, the engine owns the schema),
        // so a host wired without one recorded nothing. Failing the op is deliberate: a report of
        // 0% here would be indistinguishable from a real measurement of nothing covered.
        let Some(store) = self.coverage_metric_store.as_ref() else {
            return Err(ExecError::diag(
                codes::CONFIG_COVERAGE_STORE_MISSING,
                format!(
                    "coverage op on '{node_id}' has no coverage metric store: deployment {} is \
                     running with no coverage store wired. Coverage marks are persisted in the \
                     'coverage' data store the deployment declares in datastores.yaml — the \
                     engine owns that schema and applies it on first use, so no coverage SQL is \
                     yours to write — and nothing was recorded, so no percentage can be reported.",
                    state.deployment.value()
                ),
            ));
        };
        let deployment_id = state.deployment.value().to_string();
        // Every declared path of the target, mapped onto the metric-flag key space (an injected
        // `…#<process>` sub-path collapses to its ROUTE urn — the flag that actually exists).
        let urns: Vec<String> = target
            .coverage_paths
            .iter()
            .map(|p| crate::coverage::metric_flag_urn(&p.id))
            .collect();
        match op.as_str() {
            "report" => {
                let report = coverage_report(&target, &deployment_id, store.as_ref(), &urns)
                    .await
                    .map_err(|e| coverage_store_failure(node_id, "report", &deployment_id, &e))?;
                state
                    .ctx
                    .borrow_mut()
                    .variables
                    .insert("coverageReport", report);
                Ok(())
            }
            "reset" => {
                // ONE statement: clear exactly this process's declared flags and return how many
                // were actually flipped (the frozen `cleared` count). The old loop issued a
                // `get` + `delete` per declared path. `cleared` counts metric FLAGS, so several
                // injected sub-paths of one cross-process route (which share a route flag) count
                // once — `total` stays the declared-path count either way.
                let cleared = store
                    .clear_paths(&deployment_id, &urns)
                    .await
                    .map_err(|e| coverage_store_failure(node_id, "reset", &deployment_id, &e))?;
                let mut result = BTreeMap::new();
                result.insert("process".to_string(), FeelValue::String(target.id.clone()));
                result.insert("cleared".to_string(), FeelValue::from(cleared as i64));
                result.insert(
                    "total".to_string(),
                    FeelValue::from(target.coverage_paths.len() as i64),
                );
                state
                    .ctx
                    .borrow_mut()
                    .variables
                    .insert("coverageReset", FeelValue::Map(result));
                Ok(())
            }
            other => Err(ExecError::diag(
                sutra_bpmn::codes::RESOLVE_TASK_UNKNOWN,
                format!(
                    "unknown coverage op '{other}' on '{node_id}' (expected 'report' or 'reset')."
                ),
            )),
        }
    }

    /// Resolve a coverage op's target — the caller's own deployment first (VM-7b), else the
    /// bare-id search.
    fn resolve_coverage_target(
        &self,
        process_id: &str,
        node_id: &str,
        state: &ExecutionState<'_>,
    ) -> Result<Arc<ProcessDefinition>, ExecError> {
        if state.deployment.is_resolved() {
            if let Some(resolver) = self.module_resolver.as_ref() {
                if let Some(hit) = resolver(&state.deployment, process_id) {
                    return Ok(hit);
                }
            }
        }
        if let Some(resolver) = self.process_resolver.as_ref() {
            if let Some(hit) = resolver(process_id)? {
                return Ok(hit);
            }
        }
        Err(ExecError::diag(
            sutra_bpmn::codes::RESOLVE_MODULE_NOT_FOUND,
            format!(
                "coverage op on '{node_id}' targets process '{process_id}', which is not \
                 deployed in the caller's module."
            ),
        ))
    }

    // ---- plumbing --------------------------------------------------------------------------

    fn enqueue_outgoing(&self, node_id: &str, state: &mut ExecutionState<'_>) {
        for flow in state.process.outgoing(node_id) {
            state.fire(flow);
        }
    }

    /// Broadcast a callback to listeners; panics are swallowed per the SPI contract
    /// (listener side-effects must never break execution).
    fn notify(&self, callback: impl Fn(&dyn ExecutionListener)) {
        for listener in &self.listeners {
            let _ = catch_unwind(AssertUnwindSafe(|| callback(listener.as_ref())));
        }
    }

    /// B1 — the single audit sink a process's events route to: its process-level `<q:audit sink>`
    /// (author control) → the engine default (`with_default_audit_sink`, which the manifest default
    /// feeds). `None` = the process is not audited.
    fn effective_audit_sink(&self, process: &ProcessDefinition) -> Option<String> {
        process
            .audit
            .as_ref()
            .map(|a| a.sink.clone())
            .or_else(|| self.default_audit_sink.clone())
    }

    /// B1 — resolve a node's audit routing directive: `(audit_sink, payload_json)`.
    /// `audit_sink == None` means the node emits NO audit event — either a node-level
    /// `<q:audit capture="none">` suppression (the only per-node override) or the process is not
    /// audited. `payload_json` is the redacted variable snapshot when the PROCESS captures at
    /// payload level (node entry), else `None`.
    fn resolve_token_audit(
        &self,
        node_id: &str,
        state: &ExecutionState<'_>,
    ) -> (Option<String>, Option<String>) {
        use sutra_bpmn::qbindings::AuditCapture;
        // Node-level override — ONLY `capture="none"` (conscious suppression) is honored.
        if let Some(node_audit) = state.process.bindings_for(node_id).audit.as_ref() {
            if node_audit.capture == AuditCapture::None {
                return (None, None);
            }
        }
        let Some(sink) = self.effective_audit_sink(state.process) else {
            return (None, None);
        };
        let payload = match state.process.audit.as_ref().map(|a| a.capture) {
            Some(AuditCapture::Payload) => Some(redacted_variable_snapshot(state)),
            _ => None,
        };
        (Some(sink), payload)
    }
}

impl Builder {
    pub fn with_condition_evaluator(
        mut self,
        evaluator: impl Fn(&str, &Variables) -> Result<bool, ExecError> + 'static,
    ) -> Builder {
        self.executor.condition_evaluator = Box::new(evaluator);
        self
    }

    pub fn with_value_evaluator(
        mut self,
        evaluator: impl Fn(&str, &Variables) -> Result<FeelValue, ExecError> + 'static,
    ) -> Builder {
        self.executor.value_evaluator = Box::new(evaluator);
        self
    }

    /// Wire BOTH evaluators to the sutra-feel engine (the production wiring).
    pub fn with_feel(self) -> Builder {
        self.with_condition_evaluator(feel_condition_evaluator())
            .with_value_evaluator(feel_value_evaluator())
    }

    pub fn with_data_stores(
        mut self,
        registry: impl Fn(&DeploymentId, &str) -> Option<Rc<dyn DataStore>> + 'static,
    ) -> Builder {
        self.executor.data_stores = Some(Box::new(registry));
        self
    }

    /// Wire the typed coverage-metric store — the single coverage surface: `mark_coverage` flips
    /// intra-process metric flags and writes cross-process reconstruction fragments through it,
    /// and the reserved `coverage:report` / `coverage:reset` ops read and clear the same flags.
    /// Unwired — the deployment declared no `coverage` data store, or its connection could not
    /// be opened — it records no coverage and those ops fail with
    /// `SUTRA.CONFIG.COVERAGE.STORE_MISSING` naming that cause.
    pub fn with_coverage_metric_store(mut self, store: Rc<dyn CoverageMetricStore>) -> Builder {
        self.executor.coverage_metric_store = Some(store);
        self
    }

    pub fn with_listener(mut self, listener: Rc<dyn ExecutionListener>) -> Builder {
        self.executor.listeners.push(listener);
        self
    }

    /// B1 — the engine-default audit sink for processes that declare no `<q:audit sink>` (and have
    /// no deployment-manifest default). The lifecycle events carry the resolved sink so audit
    /// routes to exactly one sink.
    pub fn with_default_audit_sink(mut self, sink: Option<String>) -> Builder {
        self.executor.default_audit_sink = sink;
        self
    }

    pub fn with_process_resolver(
        mut self,
        resolver: impl Fn(&str) -> Result<Option<Arc<ProcessDefinition>>, ExecError> + 'static,
    ) -> Builder {
        self.executor.process_resolver = Some(Box::new(resolver));
        self
    }

    pub fn with_module_resolver(
        mut self,
        resolver: impl Fn(&DeploymentId, &str) -> Option<Arc<ProcessDefinition>> + 'static,
    ) -> Builder {
        self.executor.module_resolver = Some(Box::new(resolver));
        self
    }

    pub fn with_emission_sink(mut self, sink: Rc<dyn EmissionSink>) -> Builder {
        self.executor.emissions = Some(sink);
        self
    }

    pub fn with_auth_ref_resolver(
        mut self,
        resolver: impl Fn(&AuthRef) -> Option<crate::registry::ResolvedSecret> + 'static,
    ) -> Builder {
        self.executor.auth_resolvers = Some(Box::new(resolver));
        self
    }

    /// Each artifact registry is taken as anything convertible into its `Arc` — a plain owned
    /// registry (tests) or the activation-built one every lane shares (the engine assembly).
    pub fn with_templates(
        mut self,
        engines: TemplateEngineRegistry,
        templates: impl Into<Arc<TemplateRegistry>>,
    ) -> Builder {
        self.executor.template_engines = engines;
        self.executor.templates = templates.into();
        self
    }

    pub fn with_scripts(mut self, scripts: impl Into<Arc<ScriptRegistry>>) -> Builder {
        self.executor.scripts = scripts.into();
        self
    }

    pub fn with_decisions(
        mut self,
        engines: DecisionEngineRegistry,
        decisions: impl Into<Arc<DecisionRegistry>>,
    ) -> Builder {
        self.executor.decision_engines = engines;
        self.executor.decisions = decisions.into();
        self
    }

    pub fn with_outbound_channels(
        mut self,
        registry: impl Into<Arc<crate::registry::OutboundChannelRegistry>>,
    ) -> Builder {
        self.executor.outbound_channels = registry.into();
        self
    }

    /// Inject the `uuid` render-context supplier (deterministic tests).
    pub fn with_uuid_supplier(mut self, supplier: impl Fn() -> String + 'static) -> Builder {
        self.executor.uuid_supplier = Box::new(supplier);
        self
    }

    /// Inject the `now` render-context supplier (deterministic tests — never wall-clock in
    /// the template engine itself).
    pub fn with_now_supplier(mut self, supplier: impl Fn() -> String + 'static) -> Builder {
        self.executor.now_supplier = Box::new(supplier);
        self
    }

    pub fn build(self) -> TokenExecutor {
        self.executor
    }
}

// ---- per-execution state -----------------------------------------------------------------

struct InnerCtx {
    deployment: DeploymentId,
    labels: BTreeMap<String, String>,
    instance_id: String,
    module_id: String,
    module_version: String,
    simulation: bool,
    variables: Variables,
}

struct ExecutionState<'p> {
    process: &'p ProcessDefinition,
    deployment: DeploymentId,
    labels: BTreeMap<String, String>,
    instance_id: String,
    ctx: Rc<RefCell<InnerCtx>>,
    work: VecDeque<String>,
    /// Ordered fired-flow ids, matched against `<q:coverage>` paths at completion.
    flow_trace: Vec<String>,
    /// Per declared `<q:coverage>` path, how many of its ordered flows have been
    /// fired so far AS A CONTIGUOUS PREFIX. Unlike `flow_trace` (reset each pass), this cursor
    /// is SEEDED on resume from the prior snapshot's `sutra.coverage.<pathId>` counters and
    /// advanced in [`ExecutionState::fire`], so a route whose flows span multiple wait states
    /// is marked once its cursor reaches the route length — even when the final replay diverges
    /// from the historically-taken route (keyed by `CoveragePath::id`).
    path_progress: HashMap<String, usize>,
    visited: HashSet<String>,
    parallel_arrivals: HashMap<String, usize>,
    inclusive_arrivals: HashMap<String, usize>,
    inclusive_expected: HashMap<String, usize>,
    complex_arrivals: HashMap<String, usize>,
    complex_fired: HashSet<String>,
    completed_activities: Vec<String>,
    end_outputs: Variables,
    reached_end: bool,
    trace_coverage: bool,
    /// The open `<bpmn:transaction>` store transaction(s), keyed by store name.
    store_tx: Option<HashMap<String, Rc<dyn DataStoreTx>>>,
    /// Data-store optimistic concurrency — revision captured per `"<store>#<key>"` at read time.
    store_revs: HashMap<String, i64>,
    /// Nodes completed in PRIOR passes of this instance (from a suspend snapshot; empty on
    /// a fresh start). On a resume pass the executor replays from the start event but skips
    /// the side-effect of every node here — "replay skipping completed".
    prior_completed: HashSet<String>,
    /// The same set, insertion-ordered (the snapshot's `completedNodes` order).
    prior_completed_ordered: Vec<String>,
    /// Wait-state nodes the token parked at during THIS pass — the suspend frontier.
    waiting: Vec<String>,
    /// Wait nodes still parked from the PRIOR pass (the snapshot's frontier): reached
    /// again on a resume replay they RE-PARK without re-firing park side-effects.
    prior_waiting: HashSet<String>,
    /// Timer waits scheduled FRESH by this pass.
    timer_waits: Vec<TimerWait>,
    /// The interrupting timer boundary this resume pass is firing, consumed when
    /// the replay reaches its host activity.
    fired_timer: Option<FiredTimer>,
    /// Respond-and-continue: set when this pass parked at a `<q:reply continue>` service
    /// task — the dispatcher flushes the produced reply (`responseBody`) to the caller now, then the
    /// due-now timer wait self-resumes the remaining nodes.
    detached_reply: bool,
    /// Per `<q:retry>` node id, how many attempts of that task have FAILED so far — seeded from
    /// the resumed snapshot (`sutra.retry.<nodeId>`) and advanced by this pass. Authoritative at
    /// the quiescent point: it is what the park persists, so a node whose retried task finally
    /// SUCCEEDED has its entry removed here and therefore its key dropped from the next snapshot.
    retry_attempts: BTreeMap<String, u32>,
    /// Whether a `<q:retry>` failure in THIS state may park a durable timer. True on the
    /// top-level stateful walk (which owns the wait frontier the dispatcher persists); false
    /// inside an embedded/transaction/ad-hoc/event sub-process, whose runner discards its
    /// sub-state's frontier. The loader already refuses `<q:retry>` in those scopes, so this is
    /// the runtime belt to that braces — and the seam the compensation/multi-instance inline
    /// runners fail closed through.
    retry_parkable: bool,
    /// Per channel-call `<q:retry>` node id in a backoff window, the parking failure's
    /// classification code — seeded from the resumed snapshot (`sutra.retryWait.<nodeId>`),
    /// set by a retry park in THIS pass, cleared by the re-drive that consumes it (and by a
    /// successful relay resume of the node). AUTHORITATIVE at the quiescent point, exactly
    /// like `retry_attempts`.
    retry_backoff: BTreeMap<String, String>,
    /// The channel-call TASK FAILURE this resume pass is delivering (a terminally-poisoned
    /// request delivery), consumed when the replay reaches the named node — the poison
    /// counterpart of `fired_timer`. Set only by
    /// [`TokenExecutor::resume_channel_call_failure`].
    channel_call_failure: Option<ChannelCallFailure>,
    /// The channel-call node this resume pass EXPLICITLY re-drives (its backoff came due):
    /// the node re-runs its park side-effects — a FRESH request emission, fresh timer
    /// boundaries — instead of the silent re-park an ordinary resume replay performs on a
    /// still-waiting node. Explicit rather than inferred, because for a channel-call node the
    /// durable facts alone cannot distinguish a backoff re-drive from a relay that resumed
    /// the node mid-retry (both carry an outstanding attempt count); the DISPATCHER derives
    /// the verdict from the durable `retry_backoff` marker under the instance claim and
    /// passes it down. Set only by [`TokenExecutor::resume_retry_redrive`].
    channel_call_redrive: Option<String>,
    /// The correlation dims the inbound message driving THIS pass carried (trace-id +
    /// the leg's `<q:alias>` value), stamped onto any cross-process reconstruction fragment
    /// `mark_coverage` writes at completion. Defaults empty (both `None`) for internal passes; the
    /// spawn/relay drive sites populate it. Best-effort — the union-find is edge-tolerant.
    correlation: CoverageCorrelation,
}

/// The fired-timer-boundary marker a [`TokenExecutor::resume_timer`] pass carries.
#[derive(Debug, Clone)]
struct FiredTimer {
    boundary_id: String,
    host_id: String,
}

/// The channel-call task-failure marker a [`TokenExecutor::resume_channel_call_failure`] pass
/// carries: `node_id`'s in-flight request failed terminally OUTSIDE the graph (today: the
/// outbox poisoned its delivery), and the replay must offer that failure to the node's
/// `<q:retry>` policy when it reaches it.
#[derive(Debug, Clone)]
struct ChannelCallFailure {
    node_id: String,
    /// The stable classification code (`<q:retry nonRetryableCodes>` matches against it).
    code: String,
    /// Human detail quoted into the park/exhaustion diagnostics.
    message: String,
}

impl<'p> ExecutionState<'p> {
    fn new(
        process: &'p ProcessDefinition,
        deployment: DeploymentId,
        labels: BTreeMap<String, String>,
        instance_id: String,
        ctx: Rc<RefCell<InnerCtx>>,
    ) -> ExecutionState<'p> {
        let trace_coverage = !process.coverage_paths.is_empty();
        ExecutionState {
            process,
            deployment,
            labels,
            instance_id,
            ctx,
            work: VecDeque::new(),
            flow_trace: Vec::new(),
            path_progress: HashMap::new(),
            visited: HashSet::new(),
            parallel_arrivals: HashMap::new(),
            inclusive_arrivals: HashMap::new(),
            inclusive_expected: HashMap::new(),
            complex_arrivals: HashMap::new(),
            complex_fired: HashSet::new(),
            completed_activities: Vec::new(),
            end_outputs: Variables::new(),
            reached_end: false,
            trace_coverage,
            store_tx: None,
            store_revs: HashMap::new(),
            prior_completed: HashSet::new(),
            prior_completed_ordered: Vec::new(),
            waiting: Vec::new(),
            prior_waiting: HashSet::new(),
            timer_waits: Vec::new(),
            fired_timer: None,
            detached_reply: false,
            retry_attempts: BTreeMap::new(),
            retry_parkable: true,
            retry_backoff: BTreeMap::new(),
            channel_call_failure: None,
            channel_call_redrive: None,
            correlation: CoverageCorrelation::default(),
        }
    }

    /// Take the fired-timer marker IF it targets `host_id` (consumes it — a fire routes
    /// exactly once).
    fn take_fired_timer_for(&mut self, host_id: &str) -> Option<FiredTimer> {
        if self
            .fired_timer
            .as_ref()
            .is_some_and(|f| f.host_id == host_id)
        {
            return self.fired_timer.take();
        }
        None
    }

    /// Take the channel-call task-failure marker IF it targets `node_id` (consumes it — a
    /// failure is offered to the retry policy exactly once).
    fn take_channel_call_failure_for(&mut self, node_id: &str) -> Option<ChannelCallFailure> {
        if self
            .channel_call_failure
            .as_ref()
            .is_some_and(|f| f.node_id == node_id)
        {
            return self.channel_call_failure.take();
        }
        None
    }

    /// Seed one prior-completed node (resume pass), keeping insertion order deduplicated.
    fn push_prior_completed(&mut self, node_id: &str) {
        if self.prior_completed.insert(node_id.to_string()) {
            self.prior_completed_ordered.push(node_id.to_string());
        }
    }

    /// Fire a sequence flow: enqueue its target and (when coverage is tracked) record it in the
    /// per-pass trace AND advance every declared coverage path's contiguous-prefix cursor when
    /// this flow is the path's next expected flow (the cross-pass persisted cursor).
    fn fire(&mut self, flow: &SequenceFlow) {
        self.work.push_back(flow.target_ref.clone());
        if self.trace_coverage {
            self.flow_trace.push(flow.id.clone());
            // `self.process` is a `&'p` reference (Copy) — bind it out so the loop borrows the
            // process, leaving `self.path_progress` free to mutate.
            let process = self.process;
            for path in &process.coverage_paths {
                let cursor = self.path_progress.entry(path.id.clone()).or_insert(0);
                if *cursor < path.flows.len() && path.flows[*cursor] == flow.id {
                    *cursor += 1;
                }
            }
        }
    }

    /// The per-path contiguous-prefix cursors as bounded `u64` counters, the wire form the
    /// suspend snapshot persists under `sutra.coverage.<pathId>` and a later resume seeds back.
    fn coverage_progress(&self) -> BTreeMap<String, u64> {
        self.path_progress
            .iter()
            .map(|(id, count)| (id.clone(), *count as u64))
            .collect()
    }

    /// Seed the coverage cursors from a prior snapshot's persisted counters (resume).
    fn seed_coverage_progress(&mut self, prior: &BTreeMap<String, u64>) {
        for (id, count) in prior {
            self.path_progress.insert(id.clone(), *count as usize);
        }
    }
}

// ---- internal signals & helpers -------------------------------------------------------------

/// Control-flow signal raised by a node — the typed carrier for error/cancel/terminate control flow.
enum Signal {
    /// A BPMN error travelling up to a boundary/event-sub-process handler.
    BpmnError { source: String, code: String },
    /// A `<bpmn:cancelEventDefinition>` end event unwinding to the transaction runner.
    TxCancel,
    /// A terminal failure.
    Fatal(ExecError),
}

enum TaskFailure {
    Bpmn(String),
    Uncaught(String),
    Diag(SutraError),
}

/// What a failed attempt of a `<q:retry>` service task does next — the outcome of
/// [`TokenExecutor::plan_retry`].
enum RetryDisposition {
    /// Retries remain: park the token on a backoff timer due at `due_at`, having now recorded
    /// `attempt` failed attempts for this node.
    Park { due_at: String, attempt: u32 },
    /// Terminal: the budget is spent, the failure classified non-retryable, or the scope cannot
    /// park. Carries the diagnostic the fatal path (and the durable `FAILED` snapshot) reports.
    Fail(ExecError),
    /// The node declares no `<q:retry>` — the caller keeps the engine's pre-P1-1 behaviour
    /// verbatim (one attempt, then fatal under `SUTRA.RUNTIME.TASK.UNCAUGHT`).
    NoPolicy,
}

/// The classification code a task failure matches `<q:retry nonRetryableCodes>` against.
///
/// A registered task signals a PERMANENT failure by prefixing its message with a code and a
/// colon — `TaskError::Failed("ACCOUNT_CLOSED: account 42 is closed")` classifies as
/// `ACCOUNT_CLOSED`. That is the whole convention, and it is deliberately narrow: the token must
/// be a leading run of `A-Z`, `0-9`, `_` or `.` (the shape every `SUTRA.*` code and every
/// screaming-snake application code already has) followed immediately by `:`, so ordinary prose
/// — "connection refused: no route to host" — cannot be mistaken for a classification.
///
/// A message with no such prefix classifies as [`codes::RUNTIME_TASK_UNCAUGHT`], the stable code
/// the engine wraps it in. Listing that in `nonRetryableCodes` therefore reads as "never retry an
/// UNCLASSIFIED failure", which is a useful posture in its own right.
fn failure_classification_code(message: &str) -> &str {
    let trimmed = message.trim_start();
    let Some(colon) = trimmed.find(':') else {
        return codes::RUNTIME_TASK_UNCAUGHT;
    };
    let candidate = &trimmed[..colon];
    let classifiable = !candidate.is_empty()
        && candidate
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_' || c == '.');
    if classifiable {
        candidate
    } else {
        codes::RUNTIME_TASK_UNCAUGHT
    }
}

fn fatal(e: SutraError) -> Signal {
    Signal::Fatal(ExecError::Diagnostic(e))
}

fn fatal_diag(code: &str, message: impl Into<String>) -> Signal {
    Signal::Fatal(ExecError::diag(code, message))
}

fn signal_to_error(signal: Signal, state: &ExecutionState<'_>) -> ExecError {
    match signal {
        Signal::Fatal(e) => e,
        Signal::BpmnError { source, code } => ExecError::diag(
            codes::RUNTIME_ERROR_UNCAUGHT,
            format!(
                "Activity {source} threw BPMN error {code} but no boundary event or event \
                 sub-process in scope caught it"
            ),
        ),
        Signal::TxCancel => ExecError::diag(
            codes::RUNTIME_UNEXPECTED,
            format!(
                "Process {} reached a cancel end event outside a <bpmn:transaction> scope",
                state.process.id
            ),
        ),
    }
}

/// Merge a task's return value — a Map bulk-merges, any other non-null lands as `result`.
fn merge_task_output(output: FeelValue, variables: &mut Variables) {
    match output {
        FeelValue::Map(m) => {
            for (k, v) in m {
                variables.insert(k, v);
            }
        }
        FeelValue::Null => {}
        other => variables.insert("result".to_string(), other),
    }
}

/// Parse a script render into the variables to merge — it must be a JSON object.
/// A plain FEEL identifier — no path segments, indexing or operators — which can therefore be
/// resolved by a direct variable lookup instead of an evaluation.
fn is_bare_identifier(expression: &str) -> bool {
    let expression = expression.trim();
    !expression.is_empty()
        && expression
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

fn parse_script_output(node_id: &str, rendered: &str) -> Result<Variables, Signal> {
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(rendered);
    match parsed {
        Ok(serde_json::Value::Object(map)) => {
            let mut out = Variables::new();
            for (k, v) in &map {
                out.insert(k.clone(), json_to_feel(v));
            }
            Ok(out)
        }
        Ok(_) => {
            let mut preview = rendered.trim().to_string();
            if preview.len() > 200 {
                preview.truncate(200);
                preview.push('…');
            }
            Err(fatal_diag(
                codes::RUNTIME_UNEXPECTED,
                format!(
                    "<scriptTask> {node_id} must render a JSON object whose entries merge into \
                     the process variables; rendered output was: {preview}"
                ),
            ))
        }
        Err(e) => Err(fatal_diag(
            codes::RUNTIME_UNEXPECTED,
            format!(
                "<scriptTask> {node_id} must render a JSON object, but its output did not \
                 parse as JSON: {e}"
            ),
        )),
    }
}

/// Write a task's produced variables back to the shared scope: declared
/// outputs are the ONLY variables written back; no declared outputs ⇒ full merge (today's
/// behavior, backward-compatible). Un-mapped task-local writes drop.
fn apply_task_outputs(
    produced: Variables,
    declared_outputs: &[String],
    state: &ExecutionState<'_>,
) {
    let mut ctx = state.ctx.borrow_mut();
    if declared_outputs.is_empty() {
        ctx.variables.merge(&produced);
        return;
    }
    for name in declared_outputs {
        if let Some(v) = produced.get(name) {
            ctx.variables.insert(name.clone(), v.clone());
        }
    }
}

/// The channel-call request body, extracted from the task's (scoped) input view:
/// `requestBody` → `responseBody` → `responseObject` → `payload.body`. Mirrors the reply
/// extraction precedence with `requestBody` in front (the natural channel-call name; a
/// template task upstream typically produced it via `<q:output variable>`).
fn channel_call_body(vars: &Variables) -> Vec<u8> {
    let body = vars
        .get("requestBody")
        .or_else(|| vars.get("responseBody"))
        .or_else(|| vars.get("responseObject"))
        .or_else(|| vars.get("payload.body"))
        .cloned()
        .or_else(|| match vars.get("payload") {
            Some(FeelValue::Map(m)) => m.get("body").cloned(),
            _ => None,
        });
    match body {
        None | Some(FeelValue::Null) => Vec::new(),
        Some(FeelValue::String(s)) => s.into_bytes(),
        Some(other) => sutra_feel::value::canonical_string_of(&other).into_bytes(),
    }
}

/// The reply body: prefer the flow's PRODUCED reply (`responseBody`/`responseObject`) over
/// the INBOUND `payload.body`.
fn extract_reply_body(state: &ExecutionState<'_>) -> Vec<u8> {
    let ctx = state.ctx.borrow();
    let vars = &ctx.variables;
    let body = vars
        .get("responseBody")
        .or_else(|| vars.get("responseObject"))
        .or_else(|| vars.get("payload.body"))
        .cloned()
        .or_else(|| match vars.get("payload") {
            Some(FeelValue::Map(m)) => m.get("body").cloned(),
            _ => None,
        });
    match body {
        None | Some(FeelValue::Null) => Vec::new(),
        Some(FeelValue::String(s)) => s.into_bytes(),
        Some(other) => sutra_feel::value::canonical_string_of(&other).into_bytes(),
    }
}

/// The value to store: for a field-scoped write, the current (map) value with one field
/// replaced; otherwise the whole value.
fn merge_field(write: &StoreWrite, value: FeelValue, current: Option<FeelValue>) -> FeelValue {
    let Some(field) = &write.field else {
        return value;
    };
    let mut map = match current {
        Some(FeelValue::Map(m)) => m,
        _ => BTreeMap::new(),
    };
    map.insert(field.clone(), value);
    FeelValue::Map(map)
}

fn store_conflict(write: &StoreWrite, key: &str, expected_rev: i64, node_id: &str) -> Signal {
    fatal_diag(
        codes::RUNTIME_DATASTORE_CONFLICT,
        format!(
            "Optimistic-concurrency conflict writing data store '{}' key '{key}' at data task \
             '{node_id}': the value changed since it was read (expected revision \
             {expected_rev}). Retry the operation.",
            write.store
        ),
    )
}

/// Coerce a FEEL-evaluated key value to the store key string.
fn as_store_key(value: &FeelValue, expr: &str, node_id: &str) -> Result<String, Signal> {
    match value {
        FeelValue::Null => Err(fatal_diag(
            codes::RUNTIME_UNEXPECTED,
            format!("Data task '{node_id}' store key expression '{expr}' evaluated to null."),
        )),
        FeelValue::String(s) => Ok(s.clone()),
        other => Ok(sutra_feel::value::canonical_string_of(other)),
    }
}

/// A coverage store failure at a reserved op — reported, never swallowed. The old KV path
/// counted a failed read as "uncovered", which quietly turned an unreachable store into a 0%
/// measurement; the ruling that a missing database must be legible applies just as much to a
/// broken one.
fn coverage_store_failure(
    node_id: &str,
    op: &str,
    deployment_id: &str,
    error: &StoreError,
) -> ExecError {
    ExecError::diag(
        codes::CONFIG_COVERAGE_STORE_MISSING,
        format!(
            "coverage {op} on '{node_id}' could not read the coverage metric store for \
             deployment {deployment_id}: {}",
            error.message()
        ),
    )
}

/// True when `path` is an ordered subsequence of `trace`.
/// Build the coverage report of `target`'s declared paths against the metric flags.
///
/// ONE round trip: `covered_among` asks which of this process's declared flags are set
/// (`… WHERE deployment_id = $1 AND covered AND path_urn = ANY($2)`), replacing the previous
/// one-`get`-per-declared-path loop over the KV covered-set. `urns[i]` is `coverage_paths[i]`'s
/// metric-flag urn (positional, built by the caller); a flag that does not exist reads as
/// UNCOVERED, matching the old absent-key behaviour.
///
/// The emitted shape is frozen: `{process, percentage, covered, total, coveredPaths,
/// uncoveredPaths}`, listing the process's own declared path IDs, with the percentage rounded
/// `round(covered × 10000 / total) / 100` and `0.0` when nothing is declared.
async fn coverage_report(
    target: &ProcessDefinition,
    deployment_id: &str,
    store: &dyn CoverageMetricStore,
    urns: &[String],
) -> Result<FeelValue, StoreError> {
    let covered_set = store.covered_among(deployment_id, urns).await?;
    let mut covered_paths = Vec::new();
    let mut uncovered_paths = Vec::new();
    for (p, urn) in target.coverage_paths.iter().zip(urns) {
        if covered_set.contains(urn) {
            covered_paths.push(FeelValue::String(p.id.clone()));
        } else {
            uncovered_paths.push(FeelValue::String(p.id.clone()));
        }
    }
    let total = target.coverage_paths.len() as i64;
    let covered = covered_paths.len() as i64;
    let percentage = if total == 0 {
        0.0
    } else {
        (covered as f64 * 10000.0 / total as f64).round() / 100.0
    };
    let mut report = BTreeMap::new();
    report.insert("process".to_string(), FeelValue::String(target.id.clone()));
    report.insert("percentage".to_string(), FeelValue::from(percentage));
    report.insert("covered".to_string(), FeelValue::from(covered));
    report.insert("total".to_string(), FeelValue::from(total));
    report.insert("coveredPaths".to_string(), FeelValue::List(covered_paths));
    report.insert(
        "uncoveredPaths".to_string(),
        FeelValue::List(uncovered_paths),
    );
    Ok(FeelValue::Map(report))
}

/// BFS from `start_id` to each inclusive join (multiple incoming) — the expected targets.
fn reachable_inclusive_joins(process: &ProcessDefinition, start_id: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(start_id.to_string());
    while let Some(cur) = queue.pop_front() {
        if !seen.insert(cur.clone()) {
            continue;
        }
        let Ok(node) = process.node(&cur) else {
            continue;
        };
        if matches!(node, Node::InclusiveGateway { .. }) && process.incoming(&cur).len() > 1 {
            hits.push(cur);
            continue; // don't traverse past the join
        }
        for flow in process.outgoing(&cur) {
            queue.push_back(flow.target_ref.clone());
        }
    }
    hits
}

/// The interrupting error boundary in `process` that catches the signal, if any.
fn find_error_boundary(process: &ProcessDefinition, source: &str, code: &str) -> Option<String> {
    for n in process.nodes() {
        if let Node::BoundaryEvent {
            id,
            kind: BoundaryKind::Error,
            attached_to_ref,
            error_code,
            ..
        } = n
        {
            if attached_to_ref == source {
                let matches = error_code.as_deref().map(|c| c == code).unwrap_or(true);
                if matches {
                    return Some(id.clone());
                }
            }
        }
    }
    None
}

/// The error-triggered event sub-process in `process` that catches the code, if any.
fn find_error_event_sub_process(process: &ProcessDefinition, code: &str) -> Option<String> {
    for n in process.nodes() {
        if let Node::EventSubProcess { id, error_code, .. } = n {
            let matches = error_code.as_deref().map(|c| c == code).unwrap_or(true);
            if matches {
                return Some(id.clone());
            }
        }
    }
    None
}

/// True for the task activities an ad-hoc sub-process runs.
fn is_ad_hoc_activity(n: &Node) -> bool {
    matches!(
        n,
        Node::ServiceTask { .. }
            | Node::DataTask { .. }
            | Node::ScriptTask { .. }
            | Node::BusinessRuleTask { .. }
            | Node::SendTask { .. }
            | Node::ManualTask { .. }
            | Node::CallActivity { .. }
    )
}

fn send_as_reply(sb: &SendBinding, destination: Option<String>) -> ReplyBinding {
    ReplyBinding {
        mode: sb.mode,
        destination,
        content_type: sb.content_type.clone(),
        required: true,
        ce_type: sb.ce_type.clone(),
        ce_source: sb.ce_source.clone(),
        ce_subject: sb.ce_subject.clone(),
        ce_data_content_type: sb.ce_data_content_type.clone(),
        auth: sb.auth,
        auth_secret_ref: sb.auth_secret_ref.clone(),
        auth_header: sb.auth_header.clone(),
        message_type: sb.message_type.clone(),
        continue_after: false,
        headers: sb.headers.clone(),
    }
}

fn auth_scheme_name(scheme: sutra_bpmn::qbindings::OutboundAuthScheme) -> &'static str {
    use sutra_bpmn::qbindings::OutboundAuthScheme;
    match scheme {
        OutboundAuthScheme::Mtls => "mtls",
        OutboundAuthScheme::Bearer => "bearer",
        OutboundAuthScheme::Apikey => "apikey",
    }
}

/// The BPMN node-type string used in [`TokenEvent`]s (the canonical node-type mapping).
fn node_type(node: &Node) -> &'static str {
    match node {
        Node::StartEvent { .. } => "startEvent",
        Node::EndEvent { .. } => "endEvent",
        Node::TerminateEndEvent { .. } => "terminateEndEvent",
        Node::ErrorEvent { .. } => "errorEndEvent",
        Node::IntermediateThrowEvent { .. } => "intermediateThrowEvent",
        Node::MessageCatchEvent { .. } => "intermediateCatchEvent",
        Node::TimerCatchEvent { .. } => "intermediateCatchEvent",
        Node::LinkCatchEvent { .. } => "intermediateCatchEvent",
        Node::BoundaryEvent { .. } => "boundaryEvent",
        Node::ServiceTask { .. } => "serviceTask",
        Node::DataTask { .. } => "serviceTask",
        Node::ScriptTask { .. } => "scriptTask",
        Node::BusinessRuleTask { .. } => "businessRuleTask",
        Node::ManualTask { .. } => "manualTask",
        Node::SendTask { .. } => "sendTask",
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

/// B1 — render the current variable context as a JSON object with every `@sensitive`-declared
/// variable's value masked by [`sutra_bpmn::REDACTED_PLACEHOLDER`]. Whether to capture a payload at
/// all is the PROCESS's `<q:audit capture>` level, decided by [`TokenExecutor::resolve_token_audit`];
/// this only renders. Captured at node ENTRY (input context); leave-time capture is a future
/// refinement.
fn redacted_variable_snapshot(state: &ExecutionState<'_>) -> String {
    let sensitive: HashSet<&str> = state
        .process
        .declared_variables
        .iter()
        .filter(|d| d.sensitive)
        .map(|d| d.name.as_str())
        .collect();
    let ctx = state.ctx.borrow();
    let mut obj = serde_json::Map::new();
    for (name, value) in ctx.variables.iter() {
        // `<v>.redacted` companions are the DLP projection intake stored — surfaced IN PLACE OF
        // the raw `<v>` below, never on their own line (the raw payload must not leak).
        if name.ends_with(sutra_bpmn::REDACTION_COMPANION_SUFFIX) {
            continue;
        }
        let companion = format!("{name}{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX);
        let rendered = if sensitive.contains(name) {
            serde_json::Value::String(sutra_bpmn::REDACTED_PLACEHOLDER.to_string())
        } else if let Some(masked) = ctx.variables.get(&companion) {
            // Redaction-controlled: show the masked projection, not the raw payload.
            feel_to_json(masked)
        } else {
            feel_to_json(value)
        };
        obj.insert(name.to_string(), rendered);
    }
    serde_json::Value::Object(obj).to_string()
}

/// Commit and release the open store transaction(s), if any. Returns the first backend
/// failure any transaction reported (a durable provider records into `last_error`) — the
/// caller fails the instance closed rather than replying success on an uncommitted write.
async fn commit_store_tx(state: &mut ExecutionState<'_>) -> Option<String> {
    let mut first_error = None;
    if let Some(txs) = state.store_tx.take() {
        for (store, tx) in &txs {
            // Commit is async + typed; a backend failure is the first Err (fail-closed).
            if let Err(err) = tx.commit().await {
                if first_error.is_none() {
                    first_error = Some(format!("commit of store '{store}' failed: {err}"));
                }
            }
        }
    }
    first_error
}

/// A durable-store backend failure surfaced as a fatal diagnostic (fail-closed — never
/// reply success on a lost read/write).
fn store_op_failed(store: &str, key: &str, node_id: &str, op: &str, err: &str) -> Signal {
    fatal_diag(
        codes::RUNTIME_UNEXPECTED,
        format!("Data task '{node_id}' {op} on store '{store}'[{key}] failed: {err}"),
    )
}

/// Roll back and release the open store transaction(s), if any (no-op after commit).
async fn rollback_store_tx(state: &mut ExecutionState<'_>) {
    if let Some(txs) = state.store_tx.take() {
        for tx in txs.values() {
            let _ = tx.rollback().await;
        }
    }
}

/// Random UUID v4 (`getrandom`-seeded), formatted 8-4-4-4-12 lowercase hex.
fn new_uuid() -> String {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).expect("OS entropy source");
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Default `now` render-context supplier — ISO-8601 UTC offset date-time.
fn now_utc() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}
