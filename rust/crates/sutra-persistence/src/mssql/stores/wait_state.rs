//! Wait-state store, SQL Server dialect (`waiting_event`, plus the timer addendum).
//!
//! Same surface as the reference implementation: a queryable projection BESIDE
//! `instance_state`, MERGE-on-record with reset-to-WAITING semantics, RESOLVED rows
//! retained for audit, and the TIMER rows claimed via `SELECT TOP (n) ... WITH
//! (UPDLOCK, ROWLOCK, READPAST)` — the SQL Server equivalent of `FOR UPDATE SKIP LOCKED`.

use time::{OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::mssql::{from_db, opt, req, to_db, MssqlClient, MssqlPool, MssqlTx};
use crate::stores::{DueTimer, WaitStateStore, WaitingEvent, WaitingFilter, STATUS_WAITING};
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`WaitStateStore`].
#[derive(Clone)]
pub struct MssqlWaitStateStore {
    pool: MssqlPool,
}

/// Upsert = UPDATE, INSERT on zero rows, retry-as-UPDATE on a duplicate-key race (the
/// same non-deadlocking shape the instance store uses).
const SQL_RECORD_UPDATE: &str = "UPDATE waiting_event SET \
       process_id = @P4, correlation_key = @P5, status = 'WAITING', resolved_at = NULL \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3";

/// [`SQL_RECORD_UPDATE`]'s fresh-incarnation twin — the duplicate-key race fallback for
/// [`SQL_RECORD_FRESH`].
const SQL_RECORD_FRESH_UPDATE: &str = "UPDATE waiting_event SET \
       process_id = @P4, correlation_key = @P5, status = 'WAITING', resolved_at = NULL, \
       kind = 'MESSAGE', timer_due_at = NULL \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3";

const SQL_RECORD: &str = "UPDATE waiting_event SET \
       process_id = @P4, correlation_key = @P5, status = 'WAITING', resolved_at = NULL \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3; \
     IF @@ROWCOUNT = 0 \
       INSERT INTO waiting_event \
         (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
       VALUES (@P1, @P2, @P3, @P4, @P5, 'WAITING', SYSUTCDATETIME());";

/// Fresh-incarnation upsert (PostgreSQL reference: `SQL_RECORD_FRESH`) — additionally resets
/// the V803 timer columns, for a node the same step both resolved and re-parks (a channel-call
/// `<q:retry>` re-drive must not carry the dead backoff's TIMER kind/due-at onto the fresh
/// MESSAGE wait).
const SQL_RECORD_FRESH: &str = "UPDATE waiting_event SET \
       process_id = @P4, correlation_key = @P5, status = 'WAITING', resolved_at = NULL, \
       kind = 'MESSAGE', timer_due_at = NULL \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3; \
     IF @@ROWCOUNT = 0 \
       INSERT INTO waiting_event \
         (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
       VALUES (@P1, @P2, @P3, @P4, @P5, 'WAITING', SYSUTCDATETIME());";

const SQL_RESOLVE_NODE: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3 AND status = 'WAITING'";

const SQL_RESOLVE_ALL: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND status = 'WAITING'";

const SQL_RECORD_TIMER_UPDATE: &str = "UPDATE waiting_event SET \
       process_id = @P4, status = 'WAITING', resolved_at = NULL, kind = 'TIMER', \
       timer_due_at = @P5 \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3";

const SQL_RECORD_TIMER: &str = "UPDATE waiting_event SET \
       process_id = @P4, status = 'WAITING', resolved_at = NULL, kind = 'TIMER', \
       timer_due_at = @P5 \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND node_id = @P3; \
     IF @@ROWCOUNT = 0 \
       INSERT INTO waiting_event \
         (deployment_id, instance_id, node_id, process_id, correlation_key, status, \
          created_at, kind, timer_due_at) \
       VALUES (@P1, @P2, @P3, @P4, NULL, 'WAITING', SYSUTCDATETIME(), 'TIMER', @P5);";

const SQL_CLAIM_DUE_TIMERS: &str = "SELECT TOP (@P3) instance_id, process_id, node_id, \
      timer_due_at \
     FROM waiting_event WITH (UPDLOCK, ROWLOCK, READPAST) \
     WHERE deployment_id = @P1 AND kind = 'TIMER' AND status = 'WAITING' \
       AND timer_due_at <= @P2 \
     ORDER BY timer_due_at";

const SQL_DEFER_TIMER: &str = "UPDATE waiting_event SET timer_due_at = @P1 \
     WHERE deployment_id = @P2 AND instance_id = @P3 AND node_id = @P4 \
       AND kind = 'TIMER' AND status = 'WAITING'";

impl MssqlWaitStateStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// Upsert on a caller-supplied connection (the transactional-step building block).
    pub async fn record_waiting_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        let dep = deployment.as_str();
        let outcome = client
            .execute(
                SQL_RECORD,
                &[&dep, &instance_id, &node_id, &process_id, &correlation_key],
            )
            .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(e) if crate::mssql::is_duplicate_key(&e) => {
                client
                    .execute(
                        SQL_RECORD_UPDATE,
                        &[&dep, &instance_id, &node_id, &process_id, &correlation_key],
                    )
                    .await
                    .map_err(PersistenceError::mssql("waiting_event recordWaiting retry"))?;
                Ok(())
            }
            Err(e) => Err(PersistenceError::mssql("waiting_event recordWaiting")(e)),
        }
    }

    /// [`Self::record_waiting_in`] for a node's FRESH incarnation ([`SQL_RECORD_FRESH`]).
    pub async fn record_waiting_fresh_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        let dep = deployment.as_str();
        let outcome = client
            .execute(
                SQL_RECORD_FRESH,
                &[&dep, &instance_id, &node_id, &process_id, &correlation_key],
            )
            .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(e) if crate::mssql::is_duplicate_key(&e) => {
                client
                    .execute(
                        SQL_RECORD_FRESH_UPDATE,
                        &[&dep, &instance_id, &node_id, &process_id, &correlation_key],
                    )
                    .await
                    .map_err(PersistenceError::mssql(
                        "waiting_event recordWaitingFresh retry",
                    ))?;
                Ok(())
            }
            Err(e) => Err(PersistenceError::mssql("waiting_event recordWaitingFresh")(
                e,
            )),
        }
    }

    // ---- timer wait rows -------------------------------------------------------------------

    /// Upsert a TIMER wait row due at `due_at` on a caller-supplied connection (the
    /// park-step building block). Re-recording resets it to WAITING with the new due-at.
    pub async fn record_timer_waiting_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        due_at: OffsetDateTime,
    ) -> Result<()> {
        let dep = deployment.as_str();
        let due_at = to_db(due_at);
        let outcome = client
            .execute(
                SQL_RECORD_TIMER,
                &[&dep, &instance_id, &node_id, &process_id, &due_at],
            )
            .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(e) if crate::mssql::is_duplicate_key(&e) => {
                client
                    .execute(
                        SQL_RECORD_TIMER_UPDATE,
                        &[&dep, &instance_id, &node_id, &process_id, &due_at],
                    )
                    .await
                    .map_err(PersistenceError::mssql(
                        "waiting_event recordTimerWaiting retry",
                    ))?;
                Ok(())
            }
            Err(e) => Err(PersistenceError::mssql("waiting_event recordTimerWaiting")(
                e,
            )),
        }
    }

    /// Claim up to `max_entries` DUE timer rows via UPDLOCK/READPAST and commit (the
    /// outbox `next_attempt_at` pattern — concurrent pollers never compete for the same
    /// rows). Firing is at-least-once: the resume step resolves the row.
    pub async fn claim_due_timers(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        let mut tx = MssqlTx::begin(&self.pool).await?;
        let claimed = Self::claim_due_timers_in(tx.client(), deployment, now, max_entries).await?;
        tx.commit().await?;
        Ok(claimed)
    }

    /// UPDLOCK/READPAST claim on a caller-supplied transaction connection (rows stay
    /// locked until the caller's transaction ends).
    pub async fn claim_due_timers_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let now = to_db(now);
        let rows = client
            .query(
                SQL_CLAIM_DUE_TIMERS,
                &[&deployment.as_str(), &now, &max_entries],
            )
            .await
            .map_err(PersistenceError::mssql("waiting_event claimDueTimers"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql("waiting_event claimDueTimers rows"))?;
        rows.iter()
            .map(|row| {
                let due_at: PrimitiveDateTime = req(row, "timer_due_at")?;
                Ok(DueTimer {
                    instance_id: req(row, "instance_id")?,
                    process_id: req::<&str>(row, "process_id")?.to_owned(),
                    node_id: req::<&str>(row, "node_id")?.to_owned(),
                    due_at: from_db(due_at),
                })
            })
            .collect()
    }

    /// Push a TIMER row's due-at forward (the poller's failure-path backoff). A
    /// resolved/missing row is a silent no-op.
    pub async fn defer_timer(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
        new_due_at: OffsetDateTime,
    ) -> Result<()> {
        let due = to_db(new_due_at);
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(
                SQL_DEFER_TIMER,
                &[&due, &deployment.as_str(), &instance_id, &node_id],
            )
            .await
            .map_err(PersistenceError::mssql("waiting_event deferTimer"))?;
        Ok(())
    }
}

impl WaitStateStore for MssqlWaitStateStore {
    async fn record_waiting(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        Self::record_waiting_in(
            conn.client(),
            deployment,
            instance_id,
            process_id,
            node_id,
            correlation_key,
        )
        .await
    }

    async fn resolve(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(
                SQL_RESOLVE_NODE,
                &[&deployment.as_str(), &instance_id, &node_id],
            )
            .await
            .map_err(PersistenceError::mssql("waiting_event resolve"))?;
        Ok(())
    }

    async fn resolve_all(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_RESOLVE_ALL, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("waiting_event resolveAll"))?;
        Ok(())
    }

    async fn list_waiting(
        &self,
        deployment: &DeploymentId,
        filter: &WaitingFilter,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WaitingEvent>> {
        // FETCH NEXT rejects a zero row count on this dialect; the reference returns
        // empty for a zero limit, so short-circuit to the same answer.
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let status = filter.status.as_deref().unwrap_or(STATUS_WAITING);
        let dep = deployment.as_str();
        let offset = offset.max(0);
        // Positional parameters assembled per filter shape (no dynamic builder on this
        // client stack).
        let mut sql = String::from(
            "SELECT instance_id, process_id, node_id, correlation_key, status, created_at, \
             resolved_at FROM waiting_event WHERE deployment_id = @P1 AND status = @P2",
        );
        let mut params: Vec<&dyn tiberius::ToSql> = vec![&dep, &status];
        if let Some(process_id) = &filter.process_id {
            sql.push_str(&format!(" AND process_id = @P{}", params.len() + 1));
            params.push(process_id);
        }
        if let Some(correlation_key) = &filter.correlation_key {
            sql.push_str(&format!(" AND correlation_key = @P{}", params.len() + 1));
            params.push(correlation_key);
        }
        sql.push_str(&format!(
            " ORDER BY created_at DESC, node_id ASC \
             OFFSET @P{} ROWS FETCH NEXT @P{} ROWS ONLY",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(&offset);
        params.push(&limit);

        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(sql.as_str(), &params)
            .await
            .map_err(PersistenceError::mssql("waiting_event listWaiting"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql("waiting_event listWaiting rows"))?;
        rows.iter()
            .map(|row| {
                let created_at: PrimitiveDateTime = req(row, "created_at")?;
                let resolved_at: Option<PrimitiveDateTime> = opt(row, "resolved_at")?;
                Ok(WaitingEvent {
                    instance_id: req(row, "instance_id")?,
                    process_id: req::<&str>(row, "process_id")?.to_owned(),
                    node_id: req::<&str>(row, "node_id")?.to_owned(),
                    correlation_key: opt::<&str>(row, "correlation_key")?.map(str::to_owned),
                    status: req::<&str>(row, "status")?.to_owned(),
                    created_at: from_db(created_at),
                    resolved_at: resolved_at.map(from_db),
                })
            })
            .collect()
    }
}
