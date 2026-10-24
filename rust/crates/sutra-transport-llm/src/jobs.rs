//! Completing `llm.mode: job` calls: the poller claims due job rows and asks the provider for
//! their state; a callback claims its one row. Either way a finished job is delivered to the
//! reply channel exactly as a held call is ([`build_reply`]), deduplicated on the original
//! `outbox_key`, and its row deleted.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sutra_channels::OutboundMessage;
use sutra_llm_spi::{codes, LlmFailure, LlmJobRef, LlmJobState, RetryableClass};

use crate::config::{ChannelMode, ChannelTable, LlmCandidate, LlmChannel};
use crate::job_store::{JobClaim, LlmJobRow, LlmJobStore};
use crate::sink::{build_reply, is_engine_unavailable, ReplyPort};

/// How often the poller looks for due rows.
pub const DEFAULT_POLLER_TICK: Duration = Duration::from_secs(5);
/// Rows claimed per deployment per tick.
pub const POLL_BATCH: i64 = 32;

/// What happened to one claimed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    Delivered,
    Pending,
    Failed,
    /// The reply could not be delivered now; the row is retried shortly.
    Released,
}

/// Shared by the poller and the callback endpoint.
pub struct JobRunner {
    table: Arc<ChannelTable>,
    store: LlmJobStore,
    port: Arc<dyn ReplyPort>,
}

async fn poll_once(
    channel: &LlmChannel,
    candidate: &LlmCandidate,
    provider_job_id: &str,
) -> Result<LlmJobState, LlmFailure> {
    let job = LlmJobRef {
        id: provider_job_id.to_string(),
    };
    match tokio::time::timeout(channel.timeout, candidate.client.poll(&job)).await {
        Ok(state) => state,
        Err(_) => Err(LlmFailure::Retryable {
            class: RetryableClass::Timeout,
            retry_after: None,
            detail: "the provider did not answer the poll before the deadline".into(),
        }),
    }
}

impl JobRunner {
    pub fn new(table: Arc<ChannelTable>, store: LlmJobStore, port: Arc<dyn ReplyPort>) -> Self {
        JobRunner { table, store, port }
    }

    pub fn store(&self) -> &LlmJobStore {
        &self.store
    }

    pub fn table(&self) -> &Arc<ChannelTable> {
        &self.table
    }

    /// One poller pass: claim the due rows of every deployment with a live job channel and
    /// settle each. Returns how many rows were handled.
    pub async fn tick(&self) -> usize {
        let deployments: BTreeSet<String> = self
            .table
            .all()
            .iter()
            .filter(|c| c.mode == ChannelMode::Job)
            .map(|c| c.deployment.clone())
            .collect();
        let mut handled = 0;
        for deployment in deployments {
            let rows = match self.store.claim_due(&deployment, POLL_BATCH).await {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(%deployment, %error, "llm job claim failed");
                    continue;
                }
            };
            for row in rows {
                self.poll_and_settle(row).await;
                handled += 1;
            }
        }
        handled
    }

    /// The live channel and candidate index a row was submitted through.
    fn resolve(&self, row: &LlmJobRow) -> Option<(Arc<LlmChannel>, usize)> {
        let channel = self.table.get(&row.job.module_key, &row.job.channel)?;
        let candidate = channel.candidates.get(row.job.candidate_index)?;
        (channel.mode == ChannelMode::Job && candidate.provider == row.job.provider)
            .then_some((channel.clone(), row.job.candidate_index))
    }

    /// Poll a claimed row once and settle it.
    pub async fn poll_and_settle(&self, row: LlmJobRow) -> JobOutcome {
        let Some((channel, index)) = self.resolve(&row) else {
            return self.unresolvable(&row).await;
        };
        let candidate = &channel.candidates[index];
        let state = poll_once(&channel, candidate, &row.job.provider_job_id).await;
        self.settle(&row, &channel, candidate, state).await
    }

    /// Handle a verified, parsed callback for `provider_job_id` in `deployment`: claim its row
    /// and settle it with `state`, polling once when the callback carried none. `None` when
    /// there was nothing to claim (delivered already, or another claim holds it).
    pub async fn on_callback(
        &self,
        deployment: &str,
        provider: &str,
        provider_job_id: &str,
        state: Option<LlmJobState>,
    ) -> Result<Option<JobOutcome>, sqlx::Error> {
        let row = match self
            .store
            .claim(deployment, provider, provider_job_id)
            .await?
        {
            JobClaim::Claimed(row) => *row,
            JobClaim::Busy | JobClaim::Absent => return Ok(None),
        };
        let Some((channel, index)) = self.resolve(&row) else {
            return Ok(Some(self.unresolvable(&row).await));
        };
        let candidate = &channel.candidates[index];
        let state = match state {
            Some(state) => Ok(state),
            None => poll_once(&channel, candidate, provider_job_id).await,
        };
        Ok(Some(self.settle(&row, &channel, candidate, state).await))
    }

    async fn unresolvable(&self, row: &LlmJobRow) -> JobOutcome {
        let detail = "the channel candidate that accepted the job is no longer configured";
        tracing::warn!(
            code = codes::CALL_FAILED,
            channel = %row.job.channel,
            provider = %row.job.provider,
            "llm job failed: {detail}"
        );
        self.mark_failed(row, detail).await
    }

    async fn mark_failed(&self, row: &LlmJobRow, detail: &str) -> JobOutcome {
        if let Err(error) = self.store.fail(&row.job, detail).await {
            tracing::warn!(channel = %row.job.channel, %error, "llm job row update failed");
        }
        JobOutcome::Failed
    }

    async fn settle(
        &self,
        row: &LlmJobRow,
        channel: &LlmChannel,
        candidate: &LlmCandidate,
        state: Result<LlmJobState, LlmFailure>,
    ) -> JobOutcome {
        let failure = match state {
            Ok(LlmJobState::Done(response)) => {
                return self.deliver(row, channel, candidate, &response).await
            }
            Ok(LlmJobState::Pending) | Err(LlmFailure::Retryable { .. }) if !row.expired => {
                if let Err(error) = self.store.reschedule(&row.job).await {
                    tracing::warn!(channel = %channel.name, %error, "llm job row update failed");
                }
                return JobOutcome::Pending;
            }
            Ok(LlmJobState::Pending) | Err(LlmFailure::Retryable { .. }) => LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the job did not finish before its job-timeout".into(),
            },
            Ok(LlmJobState::Failed(failure)) | Err(failure) => failure,
        };
        // The flow's own timeout on the wait handles a job that never answers.
        tracing::warn!(
            code = codes::CALL_FAILED,
            channel = %channel.name,
            provider = %candidate.provider,
            model = %candidate.model(),
            "llm job failed: {}",
            failure.detail()
        );
        self.mark_failed(row, failure.detail()).await
    }

    async fn deliver(
        &self,
        row: &LlmJobRow,
        channel: &LlmChannel,
        candidate: &LlmCandidate,
        response: &sutra_llm_spi::LlmResponse,
    ) -> JobOutcome {
        let job = &row.job;
        let request: Value = serde_json::from_slice(&job.request_body).unwrap_or(Value::Null);
        let original = OutboundMessage {
            destination: String::new(),
            headers: job.headers.clone(),
            body: job.request_body.clone(),
            content_type: Some("application/json".to_string()),
            outbox_key: job.outbox_key.clone(),
            traceparent: job.traceparent.clone(),
        };
        let mut reply = build_reply(
            &original,
            &job.tenant,
            channel,
            candidate,
            response,
            &request,
        );
        // The row is authoritative: the channel may have been re-pointed since the submit.
        reply.module_key = job.module_key.clone();
        reply.channel = job.reply_channel.clone();
        match self.port.deliver(reply).await {
            Ok(()) => {
                if let Err(error) = self.store.complete(job).await {
                    // Re-claimed after the lease; the inbox absorbs the second delivery.
                    tracing::warn!(channel = %channel.name, %error, "llm job row delete failed");
                }
                JobOutcome::Delivered
            }
            Err(diagnostic) if is_engine_unavailable(&diagnostic) => {
                if let Err(error) = self.store.release(job).await {
                    tracing::warn!(channel = %channel.name, %error, "llm job row update failed");
                }
                JobOutcome::Released
            }
            Err(diagnostic) => {
                tracing::warn!(
                    code = %diagnostic.code,
                    channel = %channel.name,
                    "llm job reply rejected: {}",
                    diagnostic.message
                );
                self.mark_failed(row, &diagnostic.message).await
            }
        }
    }
}

/// The running poller task; stopped on drain or the sync stop path.
pub struct PollerHandle {
    stop: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl PollerHandle {
    /// Spawn the poller on `runtime`, ticking every `tick`.
    pub fn spawn(
        runner: Arc<JobRunner>,
        runtime: &tokio::runtime::Handle,
        tick: Duration,
    ) -> PollerHandle {
        let stop = Arc::new(tokio::sync::Notify::new());
        let stopped = stop.clone();
        let task = runtime.spawn(async move {
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = stopped.notified() => break,
                    _ = interval.tick() => {
                        runner.tick().await;
                    }
                }
            }
        });
        PollerHandle { stop, task }
    }

    /// Stop after the current pass and wait for it.
    pub async fn stop(self) {
        self.stop.notify_one();
        let _ = self.task.await;
    }

    /// Stop without waiting.
    pub fn stop_detached(self) {
        self.stop.notify_one();
        self.task.abort();
    }
}
