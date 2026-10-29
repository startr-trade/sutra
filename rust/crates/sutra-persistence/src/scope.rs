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
//! [`set_deployment_guc`] binds the value as a statement parameter. The two `begin_*` entry
//! points send `BEGIN` and the `set_config` together, as one simple-protocol query — one round
//! trip instead of two on every transaction — so there the value is written into the SQL text.
//! That is only done for a value that passes the [`DeploymentId`] form check (`dep-` and 24
//! lowercase hex digits: no quote, backslash or whitespace can occur), re-checked right before
//! the text is built; anything else falls back to the bound-parameter path.

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
    if let Some(literal) = guc_literal(deployment) {
        return pool
            .begin_with(format!(
                "BEGIN; SELECT set_config('sutra.deployment_id', {literal}, true)"
            ))
            .await
            .map_err(PersistenceError::db(
                "begin transaction + set_config(sutra.deployment_id)",
            ));
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(PersistenceError::db("begin transaction"))?;
    set_deployment_guc(&mut tx, deployment).await?;
    Ok(tx)
}

/// The deployment id as a SQL string literal, for the one-round-trip `BEGIN; SELECT set_config`
/// — only when it has the validated `dep-<24 lowercase hex>` form (re-checked here, so the text
/// can hold nothing but `[a-z0-9-]`). `None` sends the caller down the bound-parameter path.
fn guc_literal(deployment: &DeploymentId) -> Option<String> {
    DeploymentId::is_valid(deployment.as_str()).then(|| format!("'{}'", deployment.as_str()))
}

/// [`begin_deployment_tx`] for a transaction whose commit need not wait for its own WAL flush
/// (`synchronous_commit = off` for THIS transaction only; the server setting is untouched).
///
/// Only for writes that are safe to lose in a crash: bookkeeping a crash voids anyway, so losing
/// it can neither lose an acknowledged state nor create one. The complete list of callers: the
/// instance ownership claim (`PgInstanceStore::claim_flush_deferred` — a crashed process's claim
/// is void), the outbox claim (`PgOutboxStore::claim_due_excluding` — row locks only) and the
/// delete of delivered outbox rows (`PgOutboxStore::delete_many` — a lost delete redelivers,
/// which the outbox's at-least-once contract already allows). Add to this list only with the
/// same argument, and record it in the design notes and the public tuning guide.
///
/// The row is visible to other transactions as soon as this commit returns, exactly as with a
/// synchronous commit; only its durability is deferred, by at most `wal_writer_delay`.
pub async fn begin_deployment_tx_flush_deferred(
    pool: &PgPool,
    deployment: &DeploymentId,
) -> Result<Transaction<'static, Postgres>> {
    if let Some(literal) = guc_literal(deployment) {
        return pool
            .begin_with(format!(
                "BEGIN; SELECT set_config('synchronous_commit', 'off', true), \
                 set_config('sutra.deployment_id', {literal}, true)"
            ))
            .await
            .map_err(PersistenceError::db(
                "begin transaction + set_config(synchronous_commit, sutra.deployment_id)",
            ));
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(PersistenceError::db("begin transaction"))?;
    sqlx::query(
        "SELECT set_config('synchronous_commit', 'off', true), \
                set_config('sutra.deployment_id', $1, true)",
    )
    .bind(deployment.as_str())
    .execute(&mut *tx)
    .await
    .map_err(PersistenceError::db(
        "set_config(synchronous_commit, sutra.deployment_id)",
    ))?;
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_one_round_trip_begin_inlines_only_the_validated_form() {
        let id = DeploymentId::new("dep-0123456789abcdef01234567").unwrap();
        assert_eq!(
            guc_literal(&id).as_deref(),
            Some("'dep-0123456789abcdef01234567'")
        );
        assert_eq!(
            guc_literal(&DeploymentId::system()).as_deref(),
            Some("'dep-000000000000000000000000'")
        );
    }
}
