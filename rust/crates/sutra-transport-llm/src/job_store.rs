//! The durable `llm_job` rows behind `llm.mode: job`: insert at submit, claim for a poll or a
//! callback, then delete (delivered), re-arm (pending) or mark failed.
//!
//! Every statement binds `deployment_id` explicitly and runs in a transaction that sets the
//! `sutra.deployment_id` GUC, so the table's row-level-security policy engages for an
//! unprivileged engine role. A claim is a lease (`claim_owner` + `claim_expires_at`): an
//! expired claim is claimable again by the predicate itself, so a crashed replica's rows come
//! back without a sweeper.

use std::collections::BTreeMap;
use std::time::Duration;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};

/// How long a claim holds a row before another replica may take it.
pub const DEFAULT_CLAIM_LEASE: Duration = Duration::from_secs(300);
/// How soon a row whose reply could not be delivered is tried again.
pub const REDELIVERY_DELAY: Duration = Duration::from_secs(5);

/// A job to record after a successful submit.
#[derive(Debug, Clone, PartialEq)]
pub struct NewLlmJob {
    pub deployment: String,
    pub provider: String,
    pub provider_job_id: String,
    pub channel: String,
    pub candidate_index: usize,
    pub tenant: String,
    pub module_key: String,
    pub reply_channel: String,
    pub request_body: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub outbox_key: String,
    pub traceparent: Option<String>,
    pub poll_interval: Duration,
    pub job_timeout: Duration,
}

/// A claimed job row.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmJobRow {
    pub job: NewLlmJob,
    /// The deadline has passed.
    pub expired: bool,
}

/// What a by-id claim found.
#[derive(Debug, Clone, PartialEq)]
pub enum JobClaim {
    Claimed(Box<LlmJobRow>),
    /// Another claim holds the row; it was nudged to re-poll on release.
    Busy,
    /// No live row: delivered already, failed, or never recorded.
    Absent,
}

const COLUMNS: &str = "deployment_id, provider, provider_job_id, channel, candidate_index, \
     tenant, module_key, reply_channel, request_body, headers_json, outbox_key, traceparent, \
     poll_interval_seconds, EXTRACT(EPOCH FROM (deadline_at - created_at))::BIGINT AS timeout_s, \
     (deadline_at <= now()) AS expired";

const SQL_INSERT: &str = "INSERT INTO llm_job (deployment_id, provider, provider_job_id, \
     channel, candidate_index, tenant, module_key, reply_channel, request_body, headers_json, \
     outbox_key, traceparent, poll_interval_seconds, next_poll_at, deadline_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
     now() + make_interval(secs => $13), now() + make_interval(secs => $14)) \
     ON CONFLICT DO NOTHING";

/// The `llm_job` table on one pool, claiming as `owner`.
#[derive(Clone)]
pub struct LlmJobStore {
    pool: PgPool,
    owner: String,
    lease: Duration,
}

fn seconds(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn int(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn decode(row: &PgRow) -> Result<LlmJobRow, sqlx::Error> {
    let headers_json: String = row.try_get("headers_json")?;
    let candidate_index: i32 = row.try_get("candidate_index")?;
    let poll_interval: i32 = row.try_get("poll_interval_seconds")?;
    let timeout: i64 = row.try_get("timeout_s")?;
    Ok(LlmJobRow {
        job: NewLlmJob {
            deployment: row.try_get("deployment_id")?,
            provider: row.try_get("provider")?,
            provider_job_id: row.try_get("provider_job_id")?,
            channel: row.try_get("channel")?,
            candidate_index: usize::try_from(candidate_index).unwrap_or(usize::MAX),
            tenant: row.try_get("tenant")?,
            module_key: row.try_get("module_key")?,
            reply_channel: row.try_get("reply_channel")?,
            request_body: row.try_get("request_body")?,
            headers: serde_json::from_str(&headers_json).unwrap_or_default(),
            outbox_key: row.try_get("outbox_key")?,
            traceparent: row.try_get("traceparent")?,
            poll_interval: Duration::from_secs(u64::try_from(poll_interval).unwrap_or(0)),
            job_timeout: Duration::from_secs(u64::try_from(timeout).unwrap_or(0)),
        },
        expired: row.try_get("expired")?,
    })
}

impl LlmJobStore {
    /// A store claiming as `owner` (unique per replica) with the default lease.
    pub fn new(pool: PgPool, owner: impl Into<String>) -> LlmJobStore {
        LlmJobStore {
            pool,
            owner: owner.into(),
            lease: DEFAULT_CLAIM_LEASE,
        }
    }

    /// The claim lease.
    pub fn with_lease(mut self, lease: Duration) -> LlmJobStore {
        self.lease = lease;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn begin(&self, deployment: &str) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('sutra.deployment_id', $1, true)")
            .bind(deployment)
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    /// Record a submitted job. Idempotent on `(deployment, outbox_key)`: a second insert for
    /// the same outbox row is dropped. Returns whether a row was written.
    pub async fn insert(&self, job: &NewLlmJob) -> Result<bool, sqlx::Error> {
        let headers = serde_json::to_string(&job.headers).unwrap_or_else(|_| "{}".into());
        let mut tx = self.begin(&job.deployment).await?;
        let done = sqlx::query(SQL_INSERT)
            .bind(&job.deployment)
            .bind(&job.provider)
            .bind(&job.provider_job_id)
            .bind(&job.channel)
            .bind(int(job.candidate_index as u64))
            .bind(&job.tenant)
            .bind(&job.module_key)
            .bind(&job.reply_channel)
            .bind(&job.request_body)
            .bind(headers)
            .bind(&job.outbox_key)
            .bind(&job.traceparent)
            .bind(int(job.poll_interval.as_secs()))
            .bind(seconds(job.job_timeout))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(done == 1)
    }

    /// Claim up to `limit` due, unclaimed live rows of `deployment` (`FOR UPDATE SKIP LOCKED`,
    /// so concurrent replicas split the work).
    pub async fn claim_due(
        &self,
        deployment: &str,
        limit: i64,
    ) -> Result<Vec<LlmJobRow>, sqlx::Error> {
        let sql = format!(
            "UPDATE llm_job SET claim_owner = $2, \
             claim_expires_at = now() + make_interval(secs => $3) \
             WHERE (deployment_id, provider, provider_job_id) IN ( \
               SELECT deployment_id, provider, provider_job_id FROM llm_job \
               WHERE deployment_id = $1 AND NOT failed AND next_poll_at <= now() \
                 AND (claim_expires_at IS NULL OR claim_expires_at <= now()) \
               ORDER BY next_poll_at LIMIT $4 FOR UPDATE SKIP LOCKED) \
             RETURNING {COLUMNS}"
        );
        let mut tx = self.begin(deployment).await?;
        let rows = sqlx::query(&sql)
            .bind(deployment)
            .bind(&self.owner)
            .bind(seconds(self.lease))
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        rows.iter().map(decode).collect()
    }

    /// Claim one row by provider job id (a callback). A row another claim holds is nudged so
    /// its holder re-polls as soon as it releases it.
    pub async fn claim(
        &self,
        deployment: &str,
        provider: &str,
        provider_job_id: &str,
    ) -> Result<JobClaim, sqlx::Error> {
        let sql = format!(
            "UPDATE llm_job SET claim_owner = $4, \
             claim_expires_at = now() + make_interval(secs => $5) \
             WHERE deployment_id = $1 AND provider = $2 AND provider_job_id = $3 AND NOT failed \
               AND (claim_expires_at IS NULL OR claim_expires_at <= now()) \
             RETURNING {COLUMNS}"
        );
        let mut tx = self.begin(deployment).await?;
        let row = sqlx::query(&sql)
            .bind(deployment)
            .bind(provider)
            .bind(provider_job_id)
            .bind(&self.owner)
            .bind(seconds(self.lease))
            .fetch_optional(&mut *tx)
            .await?;
        let claim = match row {
            Some(row) => JobClaim::Claimed(Box::new(decode(&row)?)),
            None => {
                let nudged = sqlx::query(
                    "UPDATE llm_job SET nudged = TRUE \
                     WHERE deployment_id = $1 AND provider = $2 AND provider_job_id = $3 \
                       AND NOT failed",
                )
                .bind(deployment)
                .bind(provider)
                .bind(provider_job_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if nudged > 0 {
                    JobClaim::Busy
                } else {
                    JobClaim::Absent
                }
            }
        };
        tx.commit().await?;
        Ok(claim)
    }

    async fn update_claimed(
        &self,
        job: &NewLlmJob,
        set: &str,
        extra: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let sql = format!(
            "{set} WHERE deployment_id = $1 AND provider = $2 AND provider_job_id = $3 \
             AND claim_owner = $4"
        );
        let mut tx = self.begin(&job.deployment).await?;
        let mut query = sqlx::query(&sql)
            .bind(&job.deployment)
            .bind(&job.provider)
            .bind(&job.provider_job_id)
            .bind(&self.owner);
        if let Some(extra) = extra {
            query = query.bind(extra);
        }
        let done = query.execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(done == 1)
    }

    /// Delete a delivered row this store claimed.
    pub async fn complete(&self, job: &NewLlmJob) -> Result<bool, sqlx::Error> {
        self.update_claimed(job, "DELETE FROM llm_job", None).await
    }

    /// Release a still-pending row and re-arm it a poll interval out (at once if a callback
    /// nudged it meanwhile).
    pub async fn reschedule(&self, job: &NewLlmJob) -> Result<bool, sqlx::Error> {
        self.update_claimed(
            job,
            "UPDATE llm_job SET claim_owner = NULL, claim_expires_at = NULL, \
             next_poll_at = CASE WHEN nudged THEN now() \
               ELSE now() + make_interval(secs => poll_interval_seconds) END, \
             nudged = FALSE",
            None,
        )
        .await
    }

    /// Release a row whose result could not be delivered, to be tried again shortly.
    pub async fn release(&self, job: &NewLlmJob) -> Result<bool, sqlx::Error> {
        let set = format!(
            "UPDATE llm_job SET claim_owner = NULL, claim_expires_at = NULL, nudged = FALSE, \
             next_poll_at = now() + make_interval(secs => {})",
            REDELIVERY_DELAY.as_secs()
        );
        self.update_claimed(job, &set, None).await
    }

    /// Mark a claimed row failed: kept for inspection, never claimed again.
    pub async fn fail(&self, job: &NewLlmJob, error: &str) -> Result<bool, sqlx::Error> {
        self.update_claimed(
            job,
            "UPDATE llm_job SET failed = TRUE, last_error = $5, claim_owner = NULL, \
             claim_expires_at = NULL",
            Some(error),
        )
        .await
    }
}

/// The process-wide store the sink writes through, set when the transport is spawned with a
/// pool. Empty means no persistence: `mode: job` is refused at load.
#[derive(Default)]
pub struct JobStoreSlot(std::sync::RwLock<Option<LlmJobStore>>);

impl JobStoreSlot {
    pub fn new() -> JobStoreSlot {
        JobStoreSlot::default()
    }

    pub fn set(&self, store: Option<LlmJobStore>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = store;
    }

    pub fn get(&self) -> Option<LlmJobStore> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
