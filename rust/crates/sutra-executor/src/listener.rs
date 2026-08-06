//! Execution lifecycle listeners — the `ProcessExecutionListener` contract (the
//! sync-relevant callbacks). Listeners MUST NOT mutate execution state; the executor
//! protects itself by catching any panic a listener raises and dropping it (listener
//! panics are swallowed, never propagated).

use std::collections::BTreeMap;

use sutra_bpmn::qbindings::ReplyMode;
use sutra_bpmn::SutraError;

use crate::deployment::DeploymentId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub process_id: String,
    pub module_version: String,
    /// B1 — the SINGLE audit sink this process's events route to (process `<q:audit sink>` →
    /// deployment-manifest default → engine default). `None` = the process is not audited; the
    /// audit listener then emits nothing. Audit goes to exactly one sink (one source of truth).
    pub audit_sink: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub node_id: String,
    pub node_type: String,
    /// B1 — the SINGLE audit sink this node's events route to (the process's sink). `None` = this
    /// node is SUPPRESSED (node-level `<q:audit capture="none">`, the only per-node override) OR the
    /// process is not audited; the listener emits nothing for it.
    pub audit_sink: Option<String>,
    /// B1 — when the PROCESS captures at payload level, the redacted variable-context JSON at node
    /// entry (`@sensitive` values masked); `None` at metadata level. Ridden into `NODE_ENTERED`.
    pub payload_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub task_name: String,
    pub duration_nanos: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub node_id: String,
    pub default_called_element: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub node_id: String,
    pub mode: ReplyMode,
    pub destination: String,
}

/// Timer seam: one timer wait state,
/// as observed by the lifecycle bus. `node_id` is the waiting timer node — a BPMN timer
/// boundary / intermediate timer catch, or the synthetic boundary a `<q:timeout>` binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerEvent {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub node_id: String,
    /// RFC 3339 due timestamp — the value the TIMER `waiting_event` row carries.
    pub due_at: String,
}

/// The resume input a DUE timer produces — the timer poller claims a due TIMER
/// `waiting_event` row, maps it to this, and drives the executor's resume path
/// with `satisfied_wait_node = node_id` (the timer route/timeout path is the resumed
/// frontier; no relay payload rides a timer fire). Plain data so the poller, the
/// dispatcher and tests all speak the same shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerFire {
    pub deployment: DeploymentId,
    pub instance_id: String,
    /// The waiting timer node being satisfied.
    pub node_id: String,
    /// RFC 3339 due timestamp the claimed row carried.
    pub due_at: String,
    /// RFC 3339 observation stamp of the actual fire (poller clock).
    pub fired_at: String,
}

/// Per-instance / per-token / per-task lifecycle callbacks (all default no-ops).
pub trait ExecutionListener {
    fn on_instance_started(&self, _event: &InstanceEvent) {}
    fn on_instance_completed(&self, _event: &InstanceEvent) {}
    /// S-X1 — the instance parked at a wait state (dehydrating; a resume opens fresh state).
    fn on_instance_suspended(&self, _event: &InstanceEvent) {}
    /// S-X1 — a relay re-entered a suspended instance (the resume segment's lifecycle marker).
    fn on_instance_resumed(&self, _event: &InstanceEvent) {}
    fn on_instance_failed(&self, _event: &InstanceEvent, _diagnostic: &SutraError) {}
    fn on_token_entered(&self, _event: &TokenEvent) {}
    fn on_token_left(&self, _event: &TokenEvent) {}
    fn on_task_invoked(&self, _event: &TaskEvent) {}
    fn on_task_completed(&self, _event: &TaskEvent) {}
    fn on_task_failed(&self, _event: &TaskEvent, _diagnostic: &SutraError) {}
    fn on_dispatch_skipped(&self, _event: &DispatchEvent) {}
    fn on_reply_emitted(&self, _event: &ReplyEvent) {}
    /// Fired once per NEW `<q:coverage>` path mark.
    fn on_path_covered(&self, _event: &InstanceEvent, _path_id: &str) {}
    // ---- timer seam — default no-ops the timer poller drives --------------------------
    /// A timer wait state was scheduled: the park step recorded a TIMER `waiting_event`
    /// row due at `event.due_at`.
    fn on_timer_scheduled(&self, _event: &TimerEvent) {}
    /// A due timer fired — the poller claimed the row and is driving the resume path.
    fn on_timer_fired(&self, _event: &TimerEvent) {}
    /// The timer's wait node was satisfied or retired BEFORE firing (e.g. the channel-call
    /// response arrived first) — the pending TIMER row was cancelled.
    fn on_timer_cancelled(&self, _event: &TimerEvent) {}
}
