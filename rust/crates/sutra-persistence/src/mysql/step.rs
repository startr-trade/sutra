//! The strict transactional step primitive on the MySQL/MariaDB dialect.
//!
//! Identical orchestration to the reference implementation: one transaction commits the
//! instance snapshot, the wait-state rows, the alias rows, and the step's outbox enqueues
//! — commit or nothing. Reuses the shared [`crate::step::StepWrite`] /
//! [`crate::step::StepTimerWait`] shapes so callers are dialect-independent.
//!
//! No XA, ever: external effects stay outside the transaction by construction; delivery
//! is at-least-once via the outbox drain + consumer idempotency (`outbox_key`).

use crate::mysql::scope::begin_tx;
use crate::mysql::stores::{
    MySqlAliasStore, MySqlInstanceStore, MySqlOutboxStore, MySqlWaitStateStore,
};
use crate::step::{StepTimerWait, StepWrite};
use crate::{PersistenceError, Result};

fn check_outbox_ownership(step: &StepWrite) -> Result<()> {
    for entry in &step.outbox {
        if entry.deployment != step.deployment || entry.instance_id != step.instance_id {
            return Err(PersistenceError::InvalidArgument(format!(
                "outbox entry {} does not belong to step instance {} / {}",
                entry.entry_id, step.instance_id, step.deployment
            )));
        }
    }
    Ok(())
}

/// Commits one logical step in a single transaction — or nothing.
///
/// Write order inside the transaction: resolved waits → snapshot upsert → new wait rows →
/// alias rows → outbox enqueues. Any failure (including a unique-live alias collision,
/// surfaced as [`PersistenceError::AliasCollision`]) rolls the whole transaction back.
pub async fn commit_step(pool: &sqlx::MySqlPool, step: &StepWrite) -> Result<()> {
    check_outbox_ownership(step)?;
    let mut tx = begin_tx(pool).await?;
    write_step_in(&mut tx, step).await?;
    tx.commit()
        .await
        .map_err(PersistenceError::db("step commit"))
}

/// The step's writes on an already-open transaction. Exposed so tests can prove
/// atomicity by dropping the transaction before commit; production callers use
/// [`commit_step`].
pub async fn write_step_in(conn: &mut sqlx::MySqlConnection, step: &StepWrite) -> Result<()> {
    for node_id in &step.resolved_waits {
        sqlx::query(
            "UPDATE waiting_event SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP(6) \
             WHERE deployment_id = ? AND instance_id = ? AND node_id = ? AND status = 'WAITING'",
        )
        .bind(step.deployment.as_str())
        .bind(step.instance_id)
        .bind(node_id)
        .execute(&mut *conn)
        .await
        .map_err(PersistenceError::db("step resolve wait"))?;
    }

    // The channel-call retry WITHDRAWAL (see the reference step.rs): the backoff-parked
    // node's dead attempt loses its outstanding request rows in the same commit, BEFORE this
    // step's own enqueues.
    for node_id in &step.withdrawn_call_nodes {
        sqlx::query(
            "DELETE FROM outbox_entry \
             WHERE deployment_id = ? AND instance_id = ? AND node_id = ?",
        )
        .bind(step.deployment.as_str())
        .bind(step.instance_id)
        .bind(node_id)
        .execute(&mut *conn)
        .await
        .map_err(PersistenceError::db("step withdraw call outbox"))?;
    }

    MySqlInstanceStore::persist_in(
        conn,
        &step.deployment,
        &crate::stores::InstanceState {
            instance_id: step.instance_id,
            serialised: step.snapshot.clone(),
        },
    )
    .await?;

    for wait in &step.waits {
        // A node this same step resolved and re-parks is a NEW incarnation — reset its timer
        // columns (see the reference step.rs for the full rationale).
        if step.resolved_waits.iter().any(|n| n == &wait.node_id) {
            MySqlWaitStateStore::record_waiting_fresh_in(
                conn,
                &step.deployment,
                step.instance_id,
                &wait.process_id,
                &wait.node_id,
                wait.correlation_key.as_deref(),
            )
            .await?;
        } else {
            MySqlWaitStateStore::record_waiting_in(
                conn,
                &step.deployment,
                step.instance_id,
                &wait.process_id,
                &wait.node_id,
                wait.correlation_key.as_deref(),
            )
            .await?;
        }
    }

    for alias in &step.aliases {
        let recorded = MySqlAliasStore::record_in(
            conn,
            &step.deployment,
            step.instance_id,
            &alias.alias_name,
            &alias.alias_value,
            alias.unique,
        )
        .await?;
        if !recorded {
            // A DIFFERENT live instance owns this unique alias — the step must not commit.
            return Err(PersistenceError::AliasCollision {
                deployment: step.deployment.clone(),
                alias_name: alias.alias_name.clone(),
                alias_value: alias.alias_value.clone(),
            });
        }
    }

    for entry in &step.outbox {
        MySqlOutboxStore::enqueue_in(conn, entry).await?;
    }
    Ok(())
}

/// [`commit_step`] plus TIMER wait rows — one transaction, commit or nothing.
/// The timer rows are written AFTER the base step's wait rows, so a node that is both a
/// token position and a timer ends up marked TIMER with its due-at.
pub async fn commit_step_with_timers(
    pool: &sqlx::MySqlPool,
    step: &StepWrite,
    timer_waits: &[StepTimerWait],
) -> Result<()> {
    check_outbox_ownership(step)?;
    let mut tx = begin_tx(pool).await?;
    write_step_with_timers_in(&mut tx, step, timer_waits).await?;
    tx.commit()
        .await
        .map_err(PersistenceError::db("step commit"))
}

/// The timer-carrying step's writes on an already-open transaction — the crash-injection
/// building block, mirroring [`write_step_in`].
pub async fn write_step_with_timers_in(
    conn: &mut sqlx::MySqlConnection,
    step: &StepWrite,
    timer_waits: &[StepTimerWait],
) -> Result<()> {
    write_step_in(conn, step).await?;
    for timer in timer_waits {
        MySqlWaitStateStore::record_timer_waiting_in(
            conn,
            &step.deployment,
            step.instance_id,
            &timer.process_id,
            &timer.node_id,
            timer.due_at,
        )
        .await?;
    }
    Ok(())
}
