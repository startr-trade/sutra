//! Outbox store, SQL Server dialect (`outbox_entry`).
//!
//! Same surface as the reference implementation: row-exists = pending (no status column);
//! `claim_due` locks due rows with `SELECT TOP (n) ... WITH (UPDLOCK, ROWLOCK, READPAST)`
//! inside a transaction — the SQL Server equivalent of `FOR UPDATE SKIP LOCKED`, so
//! concurrent replicas never compete for the same rows; `delete` on success, `defer`
//! (backoff + diagnostic) on failure.

use std::collections::BTreeMap;

use time::{OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::mssql::{from_db, opt, req, to_db, MssqlClient, MssqlPool, MssqlTx};
use crate::stores::{OutboxEntry, OutboxStore, ReplyMode};
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`OutboxStore`].
#[derive(Clone)]
pub struct MssqlOutboxStore {
    pool: MssqlPool,
}

const SQL_INSERT: &str = "INSERT INTO outbox_entry \
     (entry_id, deployment_id, instance_id, body, content_type, destination, \
      headers_json, required, mode, outbox_key, cloud_event_json, auth_ref_json, \
      created_at, next_attempt_at, attempt_count, last_diagnostic_json, traceparent, \
      labels_json, node_id) \
     VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13, @P14, \
             @P15, @P16, @P17, @P18, @P19)";

const SQL_CLAIM_DUE: &str = "SELECT TOP (@P3) entry_id, deployment_id, instance_id, body, \
      content_type, destination, headers_json, required, mode, outbox_key, cloud_event_json, \
      auth_ref_json, created_at, next_attempt_at, attempt_count, last_diagnostic_json, \
      traceparent, labels_json, node_id \
     FROM outbox_entry WITH (UPDLOCK, ROWLOCK, READPAST) \
     WHERE deployment_id = @P1 AND poisoned = 0 AND next_attempt_at <= @P2 \
     ORDER BY next_attempt_at";

const SQL_DELETE: &str = "DELETE FROM outbox_entry WHERE deployment_id = @P1 AND entry_id = @P2";

const SQL_DEFER: &str = "UPDATE outbox_entry SET next_attempt_at = @P1, \
      attempt_count = attempt_count + 1, last_diagnostic_json = @P2 \
     WHERE deployment_id = @P3 AND entry_id = @P4";

const SQL_POISON: &str = "UPDATE outbox_entry SET poisoned = 1, last_diagnostic_json = @P1 \
     WHERE deployment_id = @P2 AND entry_id = @P3";

const SQL_COUNT_PENDING: &str =
    "SELECT COUNT_BIG(*) AS n FROM outbox_entry WHERE deployment_id = @P1 AND poisoned = 0";

fn headers_json(map: &BTreeMap<String, String>) -> String {
    serde_json::to_string(map).unwrap_or_else(|_| "{}".to_owned())
}

fn parse_headers(json: &str) -> BTreeMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

fn read_row(row: &tiberius::Row) -> Result<OutboxEntry> {
    let deployment_raw: &str = req(row, "deployment_id")?;
    let mode_raw: &str = req(row, "mode")?;
    let headers_raw: &str = req(row, "headers_json")?;
    let labels_raw: &str = req(row, "labels_json")?;
    let created_at: PrimitiveDateTime = req(row, "created_at")?;
    let next_attempt_at: PrimitiveDateTime = req(row, "next_attempt_at")?;
    Ok(OutboxEntry {
        deployment: DeploymentId::new(deployment_raw)?,
        entry_id: req(row, "entry_id")?,
        instance_id: req(row, "instance_id")?,
        node_id: opt::<&str>(row, "node_id")?.map(str::to_owned),
        body: req::<&[u8]>(row, "body")?.to_vec().into(),
        content_type: opt::<&str>(row, "content_type")?.map(str::to_owned),
        destination: req::<&str>(row, "destination")?.to_owned(),
        headers: parse_headers(headers_raw),
        required: req(row, "required")?,
        mode: ReplyMode::parse(mode_raw)?,
        outbox_key: req::<&str>(row, "outbox_key")?.to_owned(),
        cloud_event_json: opt::<&str>(row, "cloud_event_json")?.map(str::to_owned),
        auth_ref_json: opt::<&str>(row, "auth_ref_json")?.map(str::to_owned),
        labels: parse_headers(labels_raw),
        created_at: from_db(created_at),
        next_attempt_at: from_db(next_attempt_at),
        attempt_count: req(row, "attempt_count")?,
        last_diagnostic_json: opt::<&str>(row, "last_diagnostic_json")?.map(str::to_owned),
        traceparent: opt::<&str>(row, "traceparent")?.map(str::to_owned),
    })
}

impl MssqlOutboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// INSERT on a caller-supplied connection (the transactional-step building block).
    pub async fn enqueue_in(client: &mut MssqlClient, entry: &OutboxEntry) -> Result<()> {
        let created_at = to_db(entry.created_at);
        let next_attempt_at = to_db(entry.next_attempt_at);
        let headers = headers_json(&entry.headers);
        let labels = headers_json(&entry.labels);
        client
            .execute(
                SQL_INSERT,
                &[
                    &entry.entry_id,
                    &entry.deployment.as_str(),
                    &entry.instance_id,
                    &entry.body.as_slice(),
                    &entry.content_type.as_deref(),
                    &entry.destination.as_str(),
                    &headers.as_str(),
                    &entry.required,
                    &entry.mode.as_str(),
                    &entry.outbox_key.as_str(),
                    &entry.cloud_event_json.as_deref(),
                    &entry.auth_ref_json.as_deref(),
                    &created_at,
                    &next_attempt_at,
                    &entry.attempt_count,
                    &entry.last_diagnostic_json.as_deref(),
                    &entry.traceparent.as_deref(),
                    &labels.as_str(),
                    &entry.node_id.as_deref(),
                ],
            )
            .await
            .map_err(PersistenceError::mssql("outbox enqueue"))?;
        Ok(())
    }

    /// UPDLOCK/READPAST claim on a caller-supplied transaction connection. Rows stay
    /// locked until the caller's transaction ends — what the concurrent-claim tests hold
    /// open.
    pub async fn claim_due_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let now = to_db(now);
        let rows = client
            .query(SQL_CLAIM_DUE, &[&deployment.as_str(), &now, &max_entries])
            .await
            .map_err(PersistenceError::mssql("outbox claimDue"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql("outbox claimDue rows"))?;
        rows.iter().map(read_row).collect()
    }
}

impl OutboxStore for MssqlOutboxStore {
    async fn enqueue(&self, entry: &OutboxEntry) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        Self::enqueue_in(conn.client(), entry).await
    }

    async fn claim_due(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        let mut tx = MssqlTx::begin(&self.pool).await?;
        let claimed = Self::claim_due_in(tx.client(), deployment, now, max_entries).await?;
        tx.commit().await?;
        Ok(claimed)
    }

    async fn delete(&self, deployment: &DeploymentId, entry_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_DELETE, &[&deployment.as_str(), &entry_id])
            .await
            .map_err(PersistenceError::mssql("outbox delete"))?;
        Ok(())
    }

    async fn defer(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_due_at: OffsetDateTime,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        let due = to_db(new_due_at);
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(
                SQL_DEFER,
                &[&due, &new_diagnostic_json, &deployment.as_str(), &entry_id],
            )
            .await
            .map_err(PersistenceError::mssql("outbox defer"))?;
        Ok(())
    }

    async fn mark_poisoned(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(
                SQL_POISON,
                &[&new_diagnostic_json, &deployment.as_str(), &entry_id],
            )
            .await
            .map_err(PersistenceError::mssql("outbox markPoisoned"))?;
        Ok(())
    }

    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(SQL_COUNT_PENDING, &[&deployment.as_str()])
            .await
            .map_err(PersistenceError::mssql("outbox countPending"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("outbox countPending row"))?
            .ok_or_else(|| {
                PersistenceError::InvalidArgument("count query returned no row".to_owned())
            })?;
        req::<i64>(&row, "n")
    }
}
