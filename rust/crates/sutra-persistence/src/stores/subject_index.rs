//! GDPR subject blind index (`subject_index`, V1101) — the discoverability/erasure mechanism.
//!
//! Semantics: for each `subjectKey` variable, the caller computes a one-way
//! `blind_value = HMAC-SHA256(indexKey, normalize(value))` (crypto lives in `sutra-crypto`,
//! not here) and records it here with `INSERT ... ON CONFLICT DO NOTHING` — idempotent, no
//! uniqueness enforcement (unlike `alias_index`, a subject value is expected to recur across
//! many instances). `find_instances` is the disclosure query: every instance that ever
//! recorded a given `(subject_name, blind_value)`, live AND retired, so an erasure request can
//! find instances to purge even after they've gone terminal. `retire` flips an instance's rows
//! to `live = FALSE` on terminal transition, mirroring `alias_index`, but — unlike alias's
//! `find_live` — retirement never removes a row from disclosure.

use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Store trait for GDPR subject discoverability/erasure.
pub trait SubjectIndexStore {
    /// Records a subject blind-index row (own transaction). Idempotent — a repeat of the
    /// same `(deployment, instance, subject_name, blind_value)` tuple is a no-op.
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        subject_name: &str,
        blind_value: &str,
    ) -> Result<()>;
    /// The disclosure query: every instance that ever recorded
    /// `(subject_name, blind_value)` for `deployment`, spanning BOTH live and retired rows —
    /// an erasure request must find a subject's instances even after they've retired.
    async fn find_instances(
        &self,
        deployment: &DeploymentId,
        subject_name: &str,
        blind_value: &str,
    ) -> Result<Vec<Uuid>>;
    /// Retires every subject-index row of the instance (terminal transition). Retirement is
    /// bookkeeping only — it never narrows `find_instances`.
    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Hard-deletes every subject-index row of the instance (GDPR erasure) — in
    /// contrast to [`SubjectIndexStore::retire`], the rows are removed entirely rather than
    /// flagged, so the instance stops being disclosable.
    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgSubjectIndexStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO subject_index \
     (deployment_id, instance_id, subject_name, blind_value, live) \
     VALUES ($1, $2, $3, $4, TRUE) \
     ON CONFLICT DO NOTHING";

const SQL_FIND_INSTANCES: &str = "SELECT instance_id FROM subject_index \
     WHERE deployment_id = $1 AND subject_name = $2 AND blind_value = $3";

const SQL_RETIRE: &str = "UPDATE subject_index SET live = FALSE \
     WHERE deployment_id = $1 AND instance_id = $2 AND live = TRUE";

const SQL_DELETE: &str = "DELETE FROM subject_index WHERE deployment_id = $1 AND instance_id = $2";

impl PgSubjectIndexStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record on a caller-supplied connection (the transactional-step building block; the
    /// caller is responsible for having already set the `sutra.deployment_id` GUC on `conn`,
    /// e.g. via [`begin_deployment_tx`]).
    pub async fn record_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        subject_name: &str,
        blind_value: &str,
    ) -> Result<()> {
        sqlx::query(SQL_INSERT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(subject_name)
            .bind(blind_value)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("subject index record"))?;
        Ok(())
    }
}

impl SubjectIndexStore for PgSubjectIndexStore {
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        subject_name: &str,
        blind_value: &str,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        Self::record_in(&mut tx, deployment, instance_id, subject_name, blind_value).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("subject index record commit"))
    }

    async fn find_instances(
        &self,
        deployment: &DeploymentId,
        subject_name: &str,
        blind_value: &str,
    ) -> Result<Vec<Uuid>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let instances: Vec<Uuid> = sqlx::query_scalar(SQL_FIND_INSTANCES)
            .bind(deployment.as_str())
            .bind(subject_name)
            .bind(blind_value)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("subject index findInstances"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("subject index findInstances commit"))?;
        Ok(instances)
    }

    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RETIRE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("subject index retire"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("subject index retire commit"))
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("subject index delete"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("subject index delete commit"))
    }
}
