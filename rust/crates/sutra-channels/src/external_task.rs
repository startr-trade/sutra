//! The PULL delivery surface — long-poll fetch-and-lock for polyglot workers.
//!
//! The engine is otherwise push-only: a `<q:send>` emission lands in the outbox and the relay
//! dials a transport. A channel declaring `transport: pull` with a `pull://<channel>` bind
//! inverts that last hop. [`PullDeliverySink`] claims the `pull` URI scheme like any other
//! [`MessageSink`], but instead of dialing anything it PARKS the delivery as a fetchable task
//! ([`ExternalTaskRows::park`]) and answers [`SendOutcome::Delivered`] — ownership transfers
//! from the outbox row to the task row, and the relay deletes the outbox row exactly as a
//! delivered push would.
//!
//! A worker then drives [`ExternalTaskService`]: `fetch_and_lock` hands out locked tasks (or
//! long-polls, bounded, until one arrives), `complete` feeds the worker's result back, and
//! `fail` returns it to the pool or exhausts it into a terminal incident.
//!
//! **The completion is not a new resume path.** It rebuilds the [`InboundMessage`] the
//! destination encodes — exactly as [`crate::sink::LocalDeliverySink`] does for an in-process
//! hop — and re-enters the engine through the SAME [`EngineHandle::dispatch`] seam every
//! transport uses. That keeps the single-actor invariant intact: the completion is one more
//! serialized turn on the engine mpsc, never a nested call and never a second entry point. It
//! also means correlation is unchanged — the parked headers ride through, so the author's
//! `<q:alias>` resolves the waiting instance the way it always has.
//!
//! Delivery stays AT-LEAST-ONCE. The task row is deleted only after the engine accepted the
//! completion, so a crash in between re-offers the task; the `outbox_key` travels as the
//! completion's idempotency key, and inbox dedup absorbs the duplicate.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;

use crate::codes;
use crate::diag::Diagnostic;
use crate::dispatch::InboundMessage;
use crate::http::EngineHandle;
use crate::outbox_dispatch::LiveDeploymentSet;
use crate::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_executor::DeploymentId;

/// The URI scheme a pull-parked delivery is addressed with, and the channel `transport:` value
/// that declares one. Like `local`, it is engine-internal — no wire protocol, no listener.
/// Defined in [`crate::sink`] so the transport-free deploy-time lint can name it too.
pub use crate::sink::PULL_SCHEME;

/// Header stamped on a completion carrying the worker that produced it (observability +
/// correlation; never identity).
pub const WORKER_HEADER: &str = "sutra-external-task-worker";
/// Header stamped on a completion carrying the task row's id.
pub const TASK_ID_HEADER: &str = "sutra-external-task-id";

/// Diagnostic attribute carrying the ENGINE's own code when a completion was refused on the
/// inbound path — the thing that tells a worker whether re-fetching later can ever help.
pub const COMPLETION_CAUSE_ATTR: &str = "causeCode";

// ---- the persistence-free row seam --------------------------------------------------------

/// One task as the pull surface sees it — the transport-neutral shape, mirroring
/// [`crate::outbox_dispatch::ClaimedOutboxRow`]'s role on the push side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTask {
    /// Opaque row id — the worker-facing `{id}` path segment.
    pub task_id: String,
    pub deployment: DeploymentId,
    pub instance_id: String,
    /// The fetch topic AND the inbound channel the completion is delivered to.
    pub channel: String,
    pub tenant: String,
    pub module_key: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    /// The originating outbox row's key — the worker-visible correlation key and the inbox
    /// dedup key the completion re-enters under.
    pub outbox_key: String,
    pub traceparent: Option<String>,
    pub created_at: OffsetDateTime,
    pub lock_expires_at: Option<OffsetDateTime>,
    /// Fetches handed out so far.
    pub attempt_count: i32,
    /// Remaining failure budget — the `retries` a worker sees.
    pub retries_left: i32,
}

/// Row access as the pull surface needs it — the persistence-free seam (`sutra-engine` bridges
/// it onto `sutra-persistence`'s `PgExternalTaskStore`, whose claim is one `UPDATE` over a
/// `FOR UPDATE SKIP LOCKED` selection, so concurrent workers never receive the same task).
pub trait ExternalTaskRows: Send + Sync {
    /// Parks a delivery. `false` means an identical `(deployment, outbox_key)` was already
    /// parked — a re-delivered outbox row, which must NOT produce a second task.
    fn park<'a>(&'a self, task: &'a ParkRequest) -> BoxFuture<'a, Result<bool, Diagnostic>>;

    /// Claims up to `max_tasks` fetchable tasks on `channels` for `worker`, locked until
    /// `lock_expires_at`.
    #[allow(clippy::too_many_arguments)]
    fn fetch_and_lock<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        channels: &'a [String],
        worker: &'a str,
        now: OffsetDateTime,
        lock_expires_at: OffsetDateTime,
        max_tasks: i64,
    ) -> BoxFuture<'a, Result<Vec<ExternalTask>, Diagnostic>>;

    /// Re-reads a task ignoring lock state — how a caller separates "no such task" from "you no
    /// longer hold the lock" after a guarded statement matched nothing.
    fn peek<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<TaskLockView>, Diagnostic>>;

    /// Ownership-guarded lock extension held across the completion's dispatch. `None` = the
    /// guard did not match.
    fn hold<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        task_id: &'a str,
        worker: &'a str,
        now: OffsetDateTime,
        new_expiry: OffsetDateTime,
    ) -> BoxFuture<'a, Result<Option<ExternalTask>, Diagnostic>>;

    /// Deletes a completed task (after the engine accepted the completion).
    fn delete<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;

    /// Ownership-guarded failure: releases the lock with `retries_left_after` remaining and the
    /// next fetch deferred to `fetchable_at`; a zero budget marks the row TERMINAL instead.
    #[allow(clippy::too_many_arguments)]
    fn fail<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        task_id: &'a str,
        worker: &'a str,
        now: OffsetDateTime,
        retries_left_after: i32,
        fetchable_at: OffsetDateTime,
        error: &'a str,
    ) -> BoxFuture<'a, Result<bool, Diagnostic>>;
}

/// What [`ExternalTaskRows::park`] persists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkRequest {
    pub deployment: DeploymentId,
    pub instance_id: String,
    pub channel: String,
    pub tenant: String,
    pub module_key: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub outbox_key: String,
    pub traceparent: Option<String>,
    pub retries: i32,
}

/// The lock state of one task, as [`ExternalTaskRows::peek`] reports it — enough to name WHY a
/// guarded statement failed, and nothing else (the payload never rides a failure path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLockView {
    pub lock_owner: Option<String>,
    pub lock_expires_at: Option<OffsetDateTime>,
    pub failed: bool,
}

// ---- the sink -----------------------------------------------------------------------------

/// The pull-parking sink — the `pull` scheme among the transports. Registered directly in the
/// engine's runtime assembly (it captures the store and the notifier, so it cannot ride the
/// bare `TransportFactory::register_sink` fn-ptr), exactly like
/// [`crate::sink::LocalDeliverySink`].
///
/// Actor-safe: like every sink it runs on the tokio side (the outbox tick loop), off the
/// `Rc`-based actor thread, and it touches the engine actor not at all — parking is a store
/// write plus a wake-up.
pub struct PullDeliverySink {
    rows: Arc<dyn ExternalTaskRows>,
    notifier: ExternalTaskNotifier,
    /// The failure budget a freshly parked task starts with.
    retries: i32,
}

impl PullDeliverySink {
    pub fn new(
        rows: Arc<dyn ExternalTaskRows>,
        notifier: ExternalTaskNotifier,
        retries: i32,
    ) -> PullDeliverySink {
        PullDeliverySink {
            rows,
            notifier,
            retries,
        }
    }

    /// Reconstruct the park request a `pull://<module_key>/<channel>` delivery encodes. Mirrors
    /// [`crate::sink::LocalDeliverySink`]'s reconstruction — same destination grammar, same
    /// header/traceparent propagation — because the completion ends up on the same inbound
    /// channel an in-process hop would have delivered to.
    fn reconstruct(
        message: &OutboundMessage,
        deployment: &DeploymentId,
        instance_id: &str,
        retries: i32,
    ) -> Result<ParkRequest, Diagnostic> {
        let (tenant, module_key, channel) = parse_pull_destination(&message.destination)
            .ok_or_else(|| {
                Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!(
                        "pull destination '{}' is not a 'pull://<module_key>/<channel>' URI",
                        message.destination
                    ),
                )
            })?;
        Ok(ParkRequest {
            deployment: deployment.clone(),
            instance_id: instance_id.to_string(),
            channel,
            tenant,
            module_key,
            headers: message.headers.clone(),
            body: message.body.clone(),
            content_type: message.content_type.clone(),
            outbox_key: message.outbox_key.clone(),
            traceparent: message.traceparent.clone(),
            retries,
        })
    }
}

/// Split a `pull://<module_key>/<channel>` destination into `(tenant, module_key, channel)`.
/// `module_key` is the version-bearing `"<tenant>/<module>/<version>"` triple, so the channel is
/// the LAST path segment and the tenant is the FIRST.
pub fn parse_pull_destination(destination: &str) -> Option<(String, String, String)> {
    let rest = destination.strip_prefix("pull://")?;
    let (module_key, channel) = rest.rsplit_once('/')?;
    if module_key.is_empty() || channel.is_empty() {
        return None;
    }
    let tenant = module_key.split('/').next().filter(|t| !t.is_empty())?;
    Some((
        tenant.to_string(),
        module_key.to_string(),
        channel.to_string(),
    ))
}

/// The sink needs the owning deployment and instance, which the transport-neutral
/// [`OutboundMessage`] does not carry. The dispatcher stamps both onto the message headers
/// under these reserved keys before resolving the sink; they are stripped again at park time so
/// they never reach a worker.
pub const PARK_DEPLOYMENT_HEADER: &str = "sutra-park-deployment";
pub const PARK_INSTANCE_HEADER: &str = "sutra-park-instance";

impl MessageSink for PullDeliverySink {
    fn schemes(&self) -> Vec<String> {
        vec![PULL_SCHEME.to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            let deployment = match message
                .headers
                .get(PARK_DEPLOYMENT_HEADER)
                .map(|raw| DeploymentId::of(raw))
            {
                Some(Ok(deployment)) => deployment,
                stamped => {
                    let detail = match stamped {
                        Some(Err(reason)) => reason,
                        _ => "no owning deployment was stamped".to_string(),
                    };
                    return SendOutcome::PermanentFailure(Diagnostic::error(
                        codes::OUTBOUND_SEND_FAILED,
                        format!(
                            "a pull:// delivery must carry the owning deployment the dispatcher \
                             stamps before resolving the sink: {detail}"
                        ),
                    ));
                }
            };
            let instance_id = message
                .headers
                .get(PARK_INSTANCE_HEADER)
                .cloned()
                .unwrap_or_default();
            let mut request = match PullDeliverySink::reconstruct(
                message,
                &deployment,
                &instance_id,
                self.retries,
            ) {
                Ok(request) => request,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            request.headers.remove(PARK_DEPLOYMENT_HEADER);
            request.headers.remove(PARK_INSTANCE_HEADER);
            if let Some(traceparent) = request
                .traceparent
                .as_deref()
                .map(str::trim)
                .filter(|t| !t.is_empty())
            {
                request.headers.insert(
                    crate::telemetry::TRACEPARENT_HEADER.to_string(),
                    traceparent.to_string(),
                );
            }
            let channel = request.channel.clone();
            match self.rows.park(&request).await {
                // Parked (or already parked — a re-delivered row is the same task). Either way
                // the outbox row's work is done, so the dispatcher deletes it.
                Ok(_) => {
                    self.notifier.notify(&channel);
                    SendOutcome::Delivered
                }
                // A store failure is transient by construction here: nothing about the delivery
                // is malformed, so a later tick retries and the unique key keeps it idempotent.
                Err(diagnostic) => SendOutcome::RetryableFailure(diagnostic),
            }
        })
    }
}

// ---- the long-poll wake-up ----------------------------------------------------------------

/// Wakes long-polling fetches when a task arrives, keyed by channel.
///
/// A `broadcast` rather than a bare `Notify` so a fetch filters to the topics it asked for
/// instead of re-querying on every unrelated park. Lagging is harmless and deliberately not an
/// error: a woken fetch always re-runs the claim query, so a missed message costs at most one
/// extra round of the loop it was already in. Subscribing BEFORE the first query closes the
/// lost-wakeup race (a task parked between the query and the wait still wakes the waiter).
#[derive(Clone)]
pub struct ExternalTaskNotifier {
    tx: tokio::sync::broadcast::Sender<String>,
}

impl ExternalTaskNotifier {
    /// Capacity of the wake-up ring. Small on purpose — the payload is a wake-up, not a queue.
    const CAPACITY: usize = 256;

    pub fn new() -> ExternalTaskNotifier {
        let (tx, _) = tokio::sync::broadcast::channel(ExternalTaskNotifier::CAPACITY);
        ExternalTaskNotifier { tx }
    }

    /// Announce that `channel` has a new fetchable task. No subscribers is not an error.
    pub fn notify(&self, channel: &str) {
        let _ = self.tx.send(channel.to_string());
    }

    /// Subscribe before querying — see the lost-wakeup note on the type.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

impl Default for ExternalTaskNotifier {
    fn default() -> ExternalTaskNotifier {
        ExternalTaskNotifier::new()
    }
}

// ---- the service ---------------------------------------------------------------------------

/// The bounds the worker-facing surface enforces on every request. Every one of them is a
/// CEILING an operator sets — a worker can ask for less, never more, and asking for more is a
/// loud reject rather than a silent clamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTaskLimits {
    /// Lock duration used when the request omits one.
    pub default_lock_duration: Duration,
    /// Ceiling on a requested lock duration.
    pub max_lock_duration: Duration,
    /// Ceiling on a requested long-poll wait (and the default).
    pub max_async_response_timeout: Duration,
    /// Ceiling on `maxTasks` (and the default).
    pub max_tasks: u32,
    /// The failure budget a freshly parked task starts with.
    pub retries: i32,
    /// How long a failed-with-retries-left task waits before it is fetchable again.
    pub retry_timeout: Duration,
}

impl Default for ExternalTaskLimits {
    fn default() -> ExternalTaskLimits {
        ExternalTaskLimits {
            default_lock_duration: Duration::from_secs(30),
            max_lock_duration: Duration::from_secs(3600),
            max_async_response_timeout: Duration::from_secs(30),
            max_tasks: 100,
            retries: 3,
            retry_timeout: Duration::from_secs(10),
        }
    }
}

/// One fetch-and-lock request, already parsed and validated by the caller's transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub worker_id: String,
    pub channels: Vec<String>,
    pub lock_duration: Duration,
    pub max_tasks: u32,
    pub async_response_timeout: Duration,
}

/// The worker-facing pull operations. Holds the row seam, the engine handle the completion
/// re-enters through, and the live deployment set a fetch walks (a worker names topics, never
/// deployment ids — the same posture the outbox dispatcher and the operate instance list take).
pub struct ExternalTaskService {
    rows: Arc<dyn ExternalTaskRows>,
    engine: EngineHandle,
    deployments: LiveDeploymentSet,
    notifier: ExternalTaskNotifier,
    limits: ExternalTaskLimits,
}

impl ExternalTaskService {
    pub fn new(
        rows: Arc<dyn ExternalTaskRows>,
        engine: EngineHandle,
        deployments: LiveDeploymentSet,
        notifier: ExternalTaskNotifier,
        limits: ExternalTaskLimits,
    ) -> ExternalTaskService {
        ExternalTaskService {
            rows,
            engine,
            deployments,
            notifier,
            limits,
        }
    }

    pub fn limits(&self) -> &ExternalTaskLimits {
        &self.limits
    }

    /// Validate a raw request against [`ExternalTaskLimits`], failing CLOSED on anything out of
    /// bounds. `lock_duration` / `async_response_timeout` arrive as ISO-8601 durations;
    /// omitting them takes the default rather than the ceiling.
    pub fn validate(
        &self,
        worker_id: &str,
        channels: Vec<String>,
        lock_duration: Option<&str>,
        max_tasks: Option<u32>,
        async_response_timeout: Option<&str>,
    ) -> Result<FetchRequest, Diagnostic> {
        let worker_id = worker_id.trim();
        if worker_id.is_empty() {
            return Err(invalid("workerId is required"));
        }
        let channels: Vec<String> = channels
            .into_iter()
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if channels.is_empty() {
            return Err(invalid("at least one channel is required"));
        }
        let lock_duration = match lock_duration {
            None => self.limits.default_lock_duration,
            Some(raw) => parse_duration(raw, "lockDuration")?,
        };
        if lock_duration.is_zero() || lock_duration > self.limits.max_lock_duration {
            return Err(invalid(format!(
                "lockDuration must be positive and at most {} seconds \
                 (sutra.external-task.max-lock-duration)",
                self.limits.max_lock_duration.as_secs()
            )));
        }
        let async_response_timeout = match async_response_timeout {
            None => self.limits.max_async_response_timeout,
            Some(raw) => parse_duration(raw, "asyncResponseTimeout")?,
        };
        if async_response_timeout > self.limits.max_async_response_timeout {
            return Err(invalid(format!(
                "asyncResponseTimeout must be at most {} seconds \
                 (sutra.external-task.max-async-response-timeout)",
                self.limits.max_async_response_timeout.as_secs()
            )));
        }
        let max_tasks = max_tasks.unwrap_or(self.limits.max_tasks);
        if max_tasks == 0 || max_tasks > self.limits.max_tasks {
            return Err(invalid(format!(
                "maxTasks must be between 1 and {} (sutra.external-task.max-tasks)",
                self.limits.max_tasks
            )));
        }
        Ok(FetchRequest {
            worker_id: worker_id.to_string(),
            channels,
            lock_duration,
            max_tasks,
            async_response_timeout,
        })
    }

    /// Fetch-and-lock with a BOUNDED long poll: claim once, and if nothing is available wait for
    /// a matching park until `async_response_timeout` elapses, then answer with whatever the
    /// last claim found (an empty list on timeout — never a hang).
    pub async fn fetch_and_lock(
        &self,
        request: &FetchRequest,
    ) -> Result<Vec<ExternalTask>, Diagnostic> {
        // Subscribe FIRST: a task parked between the claim below and the wait must still wake us.
        let mut wakeups = self.notifier.subscribe();
        let deadline = tokio::time::Instant::now() + request.async_response_timeout;
        loop {
            let claimed = self.claim_once(request).await?;
            if !claimed.is_empty() {
                return Ok(claimed);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(Vec::new());
            }
            match tokio::time::timeout(remaining, wakeups.recv()).await {
                // A park on a channel we asked for — re-claim.
                Ok(Ok(channel)) if request.channels.contains(&channel) => continue,
                // A park elsewhere — keep waiting without re-querying.
                Ok(Ok(_)) => continue,
                // Lagged: we missed wake-ups, so re-claim rather than guess which.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                // The notifier is gone (shutdown) — answer with what we have.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return Ok(Vec::new()),
                Err(_elapsed) => return Ok(Vec::new()),
            }
        }
    }

    /// One claim pass across the live deployment set, stopping as soon as `max_tasks` are held.
    async fn claim_once(&self, request: &FetchRequest) -> Result<Vec<ExternalTask>, Diagnostic> {
        let now = OffsetDateTime::now_utc();
        let lock_expires_at = now + request.lock_duration;
        let mut out: Vec<ExternalTask> = Vec::new();
        for deployment in self.deployments.snapshot() {
            let budget = i64::from(request.max_tasks) - out.len() as i64;
            if budget <= 0 {
                break;
            }
            let claimed = self
                .rows
                .fetch_and_lock(
                    &deployment,
                    &request.channels,
                    &request.worker_id,
                    now,
                    lock_expires_at,
                    budget,
                )
                .await?;
            out.extend(claimed);
        }
        Ok(out)
    }

    /// Complete a locked task: verify + extend the lock, feed the worker's result through the
    /// ordinary inbound path, then delete the row.
    ///
    /// Ordering is deliberate. Dispatching BEFORE the delete makes the surface at-least-once (a
    /// crash in between re-offers the task, and the `outbox_key` idempotency key lets inbox
    /// dedup absorb the second attempt); deleting first would make it at-most-once and lose the
    /// work outright.
    pub async fn complete(
        &self,
        task_id: &str,
        worker_id: &str,
        body: Option<Vec<u8>>,
        content_type: Option<String>,
    ) -> Result<CompletedTask, Diagnostic> {
        let worker_id = worker_id.trim();
        if worker_id.is_empty() {
            return Err(invalid("workerId is required"));
        }
        let now = OffsetDateTime::now_utc();
        // Hold the lock across the dispatch so it cannot expire mid-flight and let a second
        // worker pick the task up while the first one's result is still in the engine.
        let grace = now + self.limits.default_lock_duration;
        let (deployment, task) = self
            .claim_for_worker(task_id, worker_id, now, grace)
            .await?;

        let message = completion_message(&task, worker_id, body, content_type);
        match self.engine.dispatch(message).await {
            Ok(_) => {
                self.rows.delete(&deployment, task_id).await?;
                Ok(CompletedTask {
                    task_id: task.task_id,
                    channel: task.channel,
                    instance_id: task.instance_id,
                })
            }
            // The engine rejected (or could not take) the completion. The row stays, still
            // locked until the grace window elapses, and becomes fetchable again after it —
            // the failure is loud and the work is not lost.
            Err(diagnostic) => Err(Diagnostic::error(
                codes::EXTERNAL_TASK_COMPLETION_REJECTED,
                format!(
                    "the engine rejected the completion of external task '{task_id}' \
                     ({}): {}",
                    diagnostic.code, diagnostic.message
                ),
                // The underlying code rides as an attribute, not just prose: it is what tells a
                // worker whether to re-fetch later (a transiently unavailable actor) or to stop
                // (a validation reject that will never pass).
            )
            .with_attribute(COMPLETION_CAUSE_ATTR, diagnostic.code)),
        }
    }

    /// Fail a locked task. `retries` is the worker's hint at the budget to leave behind; omitted
    /// means "spend one". A zero budget makes the task TERMINAL — never fetched again, retained
    /// with its last error, which is the pull-side twin of the outbox's poison horizon.
    pub async fn fail(
        &self,
        task_id: &str,
        worker_id: &str,
        error_message: &str,
        retries: Option<i32>,
        retry_timeout: Option<Duration>,
    ) -> Result<FailedTask, Diagnostic> {
        let worker_id = worker_id.trim();
        if worker_id.is_empty() {
            return Err(invalid("workerId is required"));
        }
        if let Some(hint) = retries {
            if hint < 0 {
                return Err(invalid("retries must not be negative"));
            }
        }
        let now = OffsetDateTime::now_utc();
        // Re-lock briefly: the same ownership guard the completion uses, so a stale worker fails
        // closed here too, and the row's current budget is read under that guard.
        let (deployment, task) = self
            .claim_for_worker(
                task_id,
                worker_id,
                now,
                now + self.limits.default_lock_duration,
            )
            .await?;
        let retries_left = retries
            .unwrap_or_else(|| task.retries_left.saturating_sub(1))
            .max(0);
        let backoff = retry_timeout.unwrap_or(self.limits.retry_timeout);
        let fetchable_at = now + backoff;
        let released = self
            .rows
            .fail(
                &deployment,
                task_id,
                worker_id,
                now,
                retries_left,
                fetchable_at,
                error_message,
            )
            .await?;
        if !released {
            return Err(self
                .explain_lost_lock(&deployment, task_id, worker_id)
                .await);
        }
        Ok(FailedTask {
            task_id: task.task_id,
            channel: task.channel,
            instance_id: task.instance_id,
            retries_left,
            terminal: retries_left <= 0,
        })
    }

    /// Locate `task_id` across the live deployment set and take it under `worker_id`'s lock.
    /// Fails CLOSED with a structured code naming exactly why when the guard does not match.
    async fn claim_for_worker(
        &self,
        task_id: &str,
        worker_id: &str,
        now: OffsetDateTime,
        new_expiry: OffsetDateTime,
    ) -> Result<(DeploymentId, ExternalTask), Diagnostic> {
        let mut seen: Option<DeploymentId> = None;
        for deployment in self.deployments.snapshot() {
            if let Some(task) = self
                .rows
                .hold(&deployment, task_id, worker_id, now, new_expiry)
                .await?
            {
                return Ok((deployment, task));
            }
            // The guard did not match here — but does the row exist at all under this
            // deployment? That is what separates NOT_FOUND from a lock problem.
            if self.rows.peek(&deployment, task_id).await?.is_some() {
                seen = Some(deployment);
            }
        }
        match seen {
            Some(deployment) => Err(self
                .explain_lost_lock(&deployment, task_id, worker_id)
                .await),
            None => Err(Diagnostic::error(
                codes::EXTERNAL_TASK_NOT_FOUND,
                format!("no external task '{task_id}' is parked on any live deployment"),
            )),
        }
    }

    /// Name the reason a guarded statement matched nothing for a task that DOES exist.
    async fn explain_lost_lock(
        &self,
        deployment: &DeploymentId,
        task_id: &str,
        worker_id: &str,
    ) -> Diagnostic {
        let view = match self.rows.peek(deployment, task_id).await {
            Ok(Some(view)) => view,
            Ok(None) => {
                return Diagnostic::error(
                    codes::EXTERNAL_TASK_NOT_FOUND,
                    format!("external task '{task_id}' no longer exists"),
                )
            }
            Err(diagnostic) => return diagnostic,
        };
        if view.failed {
            return Diagnostic::error(
                codes::EXTERNAL_TASK_TERMINAL,
                format!(
                    "external task '{task_id}' exhausted its retries and is terminal — it \
                     cannot be completed or failed again"
                ),
            );
        }
        let now = OffsetDateTime::now_utc();
        let held_by_other = view
            .lock_owner
            .as_deref()
            .is_some_and(|owner| owner != worker_id)
            && view.lock_expires_at.is_some_and(|expiry| expiry > now);
        if held_by_other {
            Diagnostic::error(
                codes::EXTERNAL_TASK_LOCK_HELD,
                format!(
                    "external task '{task_id}' is locked by another worker — worker \
                     '{worker_id}' does not own it"
                ),
            )
        } else {
            Diagnostic::error(
                codes::EXTERNAL_TASK_LOCK_LOST,
                format!(
                    "worker '{worker_id}' no longer holds the lock on external task \
                     '{task_id}' (it expired or was released); the task is fetchable again"
                ),
            )
        }
    }
}

/// What a successful completion reports back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTask {
    pub task_id: String,
    pub channel: String,
    pub instance_id: String,
}

/// What a recorded failure reports back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedTask {
    pub task_id: String,
    pub channel: String,
    pub instance_id: String,
    pub retries_left: i32,
    /// The budget is spent — the task is terminal and will never be fetched again.
    pub terminal: bool,
}

/// Build the completion's [`InboundMessage`]: the parked headers (so `<q:alias>` correlation is
/// unchanged) plus the worker/task stamps, the worker's result as the body, and the task's
/// `outbox_key` as the explicit idempotency key that drives inbox dedup.
fn completion_message(
    task: &ExternalTask,
    worker_id: &str,
    body: Option<Vec<u8>>,
    content_type: Option<String>,
) -> InboundMessage {
    let mut headers = task.headers.clone();
    headers.insert(WORKER_HEADER.to_string(), worker_id.to_string());
    headers.insert(TASK_ID_HEADER.to_string(), task.task_id.clone());
    if let Some(traceparent) = task
        .traceparent
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        headers.insert(
            crate::telemetry::TRACEPARENT_HEADER.to_string(),
            traceparent.to_string(),
        );
    }
    InboundMessage {
        tenant: task.tenant.clone(),
        module_key: task.module_key.clone(),
        channel: task.channel.clone(),
        headers,
        // A worker that completes with no result re-delivers the request payload — the
        // fire-and-forget shape, where the work happened outside and the flow only waits on the
        // fact of it.
        body: body.unwrap_or_else(|| task.body.clone()).into(),
        content_type: content_type.or_else(|| task.content_type.clone()),
        idempotency_key: task.outbox_key.clone(),
        explicit_event_id: true,
        received_at: now_rfc3339(),
        cloud_event: None,
    }
}

fn invalid(message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(codes::EXTERNAL_TASK_REQUEST_INVALID, message)
}

/// ISO-8601 durations only on this surface — the same grammar the engine's own cadence keys and
/// `<q:retry>` use, so an operator and a worker author read one format.
fn parse_duration(raw: &str, field: &str) -> Result<Duration, Diagnostic> {
    sutra_bpmn::duration::parse_iso8601_duration(raw.trim()).map_err(|reason| {
        invalid(format!(
            "{field}: '{raw}' is not an ISO-8601 duration — {reason}"
        ))
    })
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task() -> ExternalTask {
        let mut headers = BTreeMap::new();
        headers.insert("x-uetr".to_string(), "UETR-9".to_string());
        ExternalTask {
            task_id: "11111111-1111-4111-8111-111111111111".to_string(),
            deployment: DeploymentId::unresolved(),
            instance_id: "22222222-2222-4222-8222-222222222222".to_string(),
            channel: "score-in".to_string(),
            tenant: "acme".to_string(),
            module_key: "acme/demoflow/1.0.0".to_string(),
            headers,
            body: b"{\"ask\":1}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "ob-7".to_string(),
            traceparent: Some("00-abc-def-01".to_string()),
            created_at: OffsetDateTime::UNIX_EPOCH,
            lock_expires_at: None,
            attempt_count: 1,
            retries_left: 3,
        }
    }

    #[test]
    fn parse_pull_destination_splits_module_key_and_channel() {
        assert_eq!(
            parse_pull_destination("pull://acme/demoflow/1.0.0/score-in"),
            Some((
                "acme".to_string(),
                "acme/demoflow/1.0.0".to_string(),
                "score-in".to_string()
            ))
        );
        assert_eq!(parse_pull_destination("https://host/cb"), None);
        assert_eq!(parse_pull_destination("pull://score-in"), None);
        assert_eq!(parse_pull_destination("pull:///score-in"), None);
    }

    #[test]
    fn completion_preserves_correlation_headers_and_dedup_key() {
        let message = completion_message(
            &task(),
            "worker-1",
            Some(b"{\"score\":700}".to_vec()),
            Some("application/json".to_string()),
        );
        assert_eq!(message.channel, "score-in");
        assert_eq!(message.tenant, "acme");
        assert_eq!(message.module_key, "acme/demoflow/1.0.0");
        // The author's alias source rides through untouched — completion is not a new path.
        assert_eq!(
            message.headers.get("x-uetr").map(String::as_str),
            Some("UETR-9")
        );
        assert_eq!(
            message.headers.get(WORKER_HEADER).map(String::as_str),
            Some("worker-1")
        );
        assert_eq!(
            message
                .headers
                .get(crate::telemetry::TRACEPARENT_HEADER)
                .map(String::as_str),
            Some("00-abc-def-01")
        );
        // The outbox key is the explicit dedup key — this is what makes at-least-once safe.
        assert_eq!(message.idempotency_key, "ob-7");
        assert!(message.explicit_event_id);
        assert_eq!(message.body.into_inner(), b"{\"score\":700}");
    }

    #[test]
    fn a_result_less_completion_redelivers_the_request_payload() {
        let message = completion_message(&task(), "worker-1", None, None);
        assert_eq!(message.body.into_inner(), b"{\"ask\":1}");
        assert_eq!(message.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn durations_parse_as_iso8601_and_reject_anything_else() {
        assert_eq!(
            parse_duration("PT30S", "lockDuration").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration("PT5M", "lockDuration").unwrap(),
            Duration::from_secs(300)
        );
        let err = parse_duration("30", "lockDuration").unwrap_err();
        assert_eq!(err.code, codes::EXTERNAL_TASK_REQUEST_INVALID);
    }
}
