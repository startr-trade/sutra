//! Deployment-scoped transactions.
//!
//! Two-layer isolation enforcement:
//!
//! 1. **Application layer** — every SQL statement in [`crate::stores`] carries an explicit
//!    `deployment_id` bind.
//! 2. **Database layer** — PostgreSQL Row-Level Security policies key off the
//!    `sutra.deployment_id` GUC, set per transaction via
//!    `SELECT set_config('sutra.deployment_id', $1, true)`. The `true` (is_local) argument
//!    scopes the setting to the current transaction — it resets automatically at
//!    commit/rollback, matching `SET LOCAL` semantics, so no explicit `RESET` is needed.
//!
//! The value is bound as a statement parameter (never inlined), so there is no injection
//! surface; the [`DeploymentId`] form validation (`dep-<24 hex>`) is retained as defence in
//! depth.

use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use crate::{DeploymentId, PersistenceError, Result};

/// Sets the `sutra.deployment_id` GUC on an already-open transaction so RLS policies engage
/// for the statements that follow. Transaction-local: dies with the commit/rollback.
pub async fn set_deployment_guc(conn: &mut PgConnection, deployment: &DeploymentId) -> Result<()> {
    sqlx::query("SELECT set_config('sutra.deployment_id', $1, true)")
        .bind(deployment.as_str())
        .execute(conn)
        .await
        .map_err(PersistenceError::db("set_config(sutra.deployment_id)"))?;
    Ok(())
}

/// Opens a transaction with the `sutra.deployment_id` GUC already set — the standard entry
/// point for every deployment-scoped store operation. The caller commits or rolls back.
pub async fn begin_deployment_tx(
    pool: &PgPool,
    deployment: &DeploymentId,
) -> Result<Transaction<'static, Postgres>> {
    let mut tx = pool
        .begin()
        .await
        .map_err(PersistenceError::db("begin transaction"))?;
    set_deployment_guc(&mut tx, deployment).await?;
    Ok(tx)
}
