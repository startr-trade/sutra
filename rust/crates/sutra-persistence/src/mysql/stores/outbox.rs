//! Outbox store, MySQL/MariaDB dialect (`outbox_entry`).
//!
//! Same surface as the reference implementation: row-exists = pending (no status column);
//! `claim_due` locks due rows with `SELECT ... FOR UPDATE SKIP LOCKED` (MySQL 8 /
//! MariaDB 10.6+) so concurrent replicas never compete for the same rows; `delete` on
//! success, `defer` (backoff + diagnostic) on failure.

use std::collections::BTreeMap;

use sqlx::{MySqlConnection, MySqlPool, Row};
use time::{OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::mysql::scope::begin_tx;
use crate::mysql::{from_db, to_db};
use crate::stores::{OutboxEntry, OutboxStore, ReplyMode};
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`OutboxStore`].
#[derive(Debug, Clone)]
pub struct MySqlOutboxStore {
    pool: MySqlPool,
}

const SQL_INSERT: &str = "INSERT INTO outbox_entry \
     (entry_id, deployment_id, instance_id, body, content_type, destination, \
      headers_json, required, mode, outbox_key, cloud_event_json, auth_ref_json, \
      created_at, next_attempt_at, attempt_count, last_diagnostic_json, traceparent, \
      labels_json, node_id) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const SQL_CLAIM_DUE: &str = "SELECT entry_id, deployment_id, instance_id, body, content_type, \
      destination, headers_json, required, mode, outbox_key, cloud_event_json, auth_ref_json, \
      created_at, next_attempt_at, attempt_count, last_diagnostic_json, traceparent, labels_json, \
      node_id \
     FROM outbox_entry \
     WHERE deployment_id = ? AND NOT poisoned AND next_attempt_at <= ? \
     ORDER BY next_attempt_at \
     LIMIT ? \
     FOR UPDATE SKIP LOCKED";

const SQL_DELETE: &str = "DELETE FROM outbox_entry WHERE deployment_id = ? AND entry_id = ?";

const SQL_DEFER: &str = "UPDATE outbox_entry SET next_attempt_at = ?, \
      attempt_count = attempt_count + 1, last_diagnostic_json = ? \
     WHERE deployment_id = ? AND entry_id = ?";

const SQL_POISON: &str = "UPDATE outbox_entry SET poisoned = TRUE, last_diagnostic_json = ? \
     WHERE deployment_id = ? AND entry_id = ?";

const SQL_COUNT_PENDING: &str =
    "SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = ? AND NOT poisoned";

fn headers_json(map: &BTreeMap<String, String>) -> String {
    serde_json::to_string(map).unwrap_or_else(|_| "{}".to_owned())
}

fn parse_headers(json: &str) -> BTreeMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

impl MySqlOutboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// INSERT on a caller-supplied connection (the transactional-step building block).
    pub async fn enqueue_in(conn: &mut MySqlConnection, entry: &OutboxEntry) -> Result<()> {
        sqlx::query(SQL_INSERT)
            .bind(entry.entry_id)
            .bind(entry.deployment.as_str())
            .bind(entry.instance_id)
            .bind(entry.body.get())
            .bind(entry.content_type.as_deref())
            .bind(&entry.destination)
            .bind(headers_json(&entry.headers))
            .bind(entry.required)
            .bind(entry.mode.as_str())
            .bind(&entry.outbox_key)
            .bind(entry.cloud_event_json.as_deref())
            .bind(entry.auth_ref_json.as_deref())
            .bind(to_db(entry.created_at))
            .bind(to_db(entry.next_attempt_at))
            .bind(entry.attempt_count)
            .bind(entry.last_diagnostic_json.as_deref())
            .bind(entry.traceparent.as_deref())
            .bind(headers_json(&entry.labels))
            .bind(entry.node_id.as_deref())
            .execute(conn)
            .await
            .map_err(PersistenceError::db("outbox enqueue"))?;
        Ok(())
    }

    /// SKIP LOCKED claim on a caller-supplied transaction connection. Rows stay locked
    /// until the caller's transaction ends — what the concurrent-claim tests hold open.
    pub async fn claim_due_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_CLAIM_DUE)
            .bind(deployment.as_str())
            .bind(to_db(now))
            .bind(max_entries)
            .fetch_all(conn)
            .await
            .map_err(PersistenceError::db("outbox claimDue"))?;
        rows.iter().map(read_row).collect()
    }
}

fn read_row(row: &sqlx::mysql::MySqlRow) -> Result<OutboxEntry> {
    use crate::mysql::{opt_str_col, str_col};
    fn e(source: sqlx::Error) -> PersistenceError {
        PersistenceError::Database {
            operation: "outbox read row",
            source,
        }
    }
    let created_at: PrimitiveDateTime = row.try_get("created_at").map_err(e)?;
    let next_attempt_at: PrimitiveDateTime = row.try_get("next_attempt_at").map_err(e)?;
    Ok(OutboxEntry {
        deployment: DeploymentId::new(str_col(row, "deployment_id")?)?,
        entry_id: row.try_get("entry_id").map_err(e)?,
        instance_id: row.try_get("instance_id").map_err(e)?,
        node_id: opt_str_col(row, "node_id")?,
        body: row.try_get::<Vec<u8>, _>("body").map_err(e)?.into(),
        content_type: opt_str_col(row, "content_type")?,
        destination: str_col(row, "destination")?,
        headers: parse_headers(&str_col(row, "headers_json")?),
        required: row.try_get("required").map_err(e)?,
        mode: ReplyMode::parse(&str_col(row, "mode")?)?,
        outbox_key: str_col(row, "outbox_key")?,
        cloud_event_json: opt_str_col(row, "cloud_event_json")?,
        auth_ref_json: opt_str_col(row, "auth_ref_json")?,
        labels: parse_headers(&str_col(row, "labels_json")?),
        created_at: from_db(created_at),
        next_attempt_at: from_db(next_attempt_at),
        attempt_count: row.try_get("attempt_count").map_err(e)?,
        last_diagnostic_json: opt_str_col(row, "last_diagnostic_json")?,
        traceparent: opt_str_col(row, "traceparent")?,
    })
}

impl OutboxStore for MySqlOutboxStore {
    async fn enqueue(&self, entry: &OutboxEntry) -> Result<()> {
        let mut tx = begin_tx(&self.pool).await?;
        Self::enqueue_in(&mut tx, entry).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox enqueue commit"))
    }

    async fn claim_due(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        let mut tx = begin_tx(&self.pool).await?;
        let claimed = Self::claim_due_in(&mut tx, deployment, now, max_entries).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox claimDue commit"))?;
        Ok(claimed)
    }

    async fn delete(&self, deployment: &DeploymentId, entry_id: Uuid) -> Result<()> {
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("outbox delete"))?;
        Ok(())
    }

    async fn defer(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_due_at: OffsetDateTime,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        sqlx::query(SQL_DEFER)
            .bind(to_db(new_due_at))
            .bind(new_diagnostic_json)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("outbox defer"))?;
        Ok(())
    }

    async fn mark_poisoned(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        sqlx::query(SQL_POISON)
            .bind(new_diagnostic_json)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("outbox markPoisoned"))?;
        Ok(())
    }

    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64> {
        sqlx::query_scalar(SQL_COUNT_PENDING)
            .bind(deployment.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(PersistenceError::db("outbox countPending"))
    }
}
