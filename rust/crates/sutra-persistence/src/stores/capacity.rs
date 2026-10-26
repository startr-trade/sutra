//! Capacity pools (`capacity_pool` + `capacity_slot`, V703): the cluster-wide slot count a pooled
//! channel draws from, and the recovery sweep that returns the slots of finished or vanished
//! holders.
//!
//! A pool spans deployments, so its count cannot live in a deployment-scoped table: the
//! `capacity_pool` row holds the per-direction counters and is the lock every acquisition and
//! release takes (`FOR UPDATE`), which is what keeps two replicas from both taking the last slot.
//! The slot rows themselves are deployment-scoped like every instance table (explicit
//! `deployment_id` bind + row-level security), so a deployment sees only its own holders.

use sqlx::{PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// The persisted direction spellings.
pub const SLOT_INBOUND: &str = "INBOUND";
pub const SLOT_OUTBOUND: &str = "OUTBOUND";

/// Which of an instance's slots a [`PgCapacitySlotStore::release_calls`] releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSelection<'a> {
    /// The slots held for this call key.
    Call(&'a str),
    /// The slots taken in this direction ([`SLOT_INBOUND`] or [`SLOT_OUTBOUND`]).
    Direction(&'a str),
}

/// The outcome of [`PgCapacitySlotStore::acquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotGrant {
    Acquired,
    /// The holder already holds a slot of this pool for this call.
    AlreadyHeld,
    /// The pool's rule refused the direction; the counts it saw.
    Exhausted {
        inbound_used: i32,
        outbound_used: i32,
    },
    /// `require_live` and the holder instance has finished (`terminal_at` stamped).
    InstanceEnded,
    /// An OUTBOUND claim whose session (or call) a `session-end:` already ended (V704): the
    /// request goes without a slot.
    SessionEnded,
}

/// One acquisition, as the store executes it.
pub struct SlotClaim<'a> {
    pub deployment: &'a DeploymentId,
    pub pool: &'a str,
    pub tenant: &'a str,
    pub instance: Uuid,
    /// The call the slot is held for — one slot per `(instance, call key, pool)`.
    pub call_key: &'a str,
    /// [`SLOT_INBOUND`] or [`SLOT_OUTBOUND`].
    pub direction: &'a str,
    /// The engine replica taking the slot.
    pub holder: &'a str,
    pub require_live: bool,
}

const SQL_ENSURE_POOL: &str =
    "INSERT INTO capacity_pool (pool) VALUES ($1) ON CONFLICT (pool) DO NOTHING";
const SQL_LOCK_POOL: &str =
    "SELECT inbound_used, outbound_used FROM capacity_pool WHERE pool = $1 FOR UPDATE";
const SQL_USAGE: &str = "SELECT inbound_used, outbound_used FROM capacity_pool WHERE pool = $1";
const SQL_INSTANCE_ENDED: &str = "SELECT terminal_at IS NOT NULL AS ended FROM instance_state \
     WHERE deployment_id = $1 AND instance_id = $2";
const SQL_HELD: &str = "SELECT 1 FROM capacity_slot \
     WHERE deployment_id = $1 AND instance_id = $2 AND pool = $3 AND call_key = $4";
const SQL_INSERT_SLOT: &str = "INSERT INTO capacity_slot \
     (deployment_id, instance_id, pool, call_key, tenant, direction, holder, acquired_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING";
const SQL_INCREMENT_INBOUND: &str = "UPDATE capacity_pool \
     SET inbound_used = inbound_used + 1, updated_at = now() WHERE pool = $1";
const SQL_INCREMENT_OUTBOUND: &str = "UPDATE capacity_pool \
     SET outbound_used = outbound_used + 1, updated_at = now() WHERE pool = $1";
const SQL_DECREMENT_INBOUND: &str = "UPDATE capacity_pool \
     SET inbound_used = GREATEST(inbound_used - 1, 0), updated_at = now() WHERE pool = $1";
const SQL_DECREMENT_OUTBOUND: &str = "UPDATE capacity_pool \
     SET outbound_used = GREATEST(outbound_used - 1, 0), updated_at = now() WHERE pool = $1";
const SQL_REBIND: &str = "UPDATE capacity_slot SET instance_id = $3 \
     WHERE deployment_id = $1 AND instance_id = $2";
const SQL_RELEASE: &str = "DELETE FROM capacity_slot \
     WHERE deployment_id = $1 AND instance_id = $2 RETURNING pool, direction";
const SQL_RELEASE_CALL: &str = "DELETE FROM capacity_slot \
     WHERE deployment_id = $1 AND instance_id = $2 AND call_key = $3 RETURNING pool, direction";
const SQL_RELEASE_DIRECTION: &str = "DELETE FROM capacity_slot \
     WHERE deployment_id = $1 AND instance_id = $2 AND direction = $3 RETURNING pool, direction";
/// Whether an instance is over: its row is stamped terminal, or it has no row (purged at
/// completion, or never stored) and no in-flight chain marker.
const SQL_INSTANCE_OVER: &str = "SELECT COALESCE( \
       (SELECT terminal_at IS NOT NULL FROM instance_state \
         WHERE deployment_id = $1 AND instance_id = $2), \
       NOT EXISTS (SELECT 1 FROM inflight_sync_chain \
                    WHERE deployment_id = $1 AND instance_id = $2)) AS over";

/// Whether a session end (V704) covers this call: the whole session (`''`) or the call itself.
const SQL_SESSION_ENDED: &str = "SELECT EXISTS (SELECT 1 FROM capacity_session_end \
     WHERE deployment_id = $1 AND instance_id = $2 AND (call_key = '' OR call_key = $3))";
const SQL_RECORD_SESSION_END: &str = "INSERT INTO capacity_session_end \
     (deployment_id, instance_id, call_key) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING";
/// The session ends of instances that are over (see `SQL_INSTANCE_OVER`), once older than the
/// sweep's bound — nothing is left for them to cover.
const SQL_FORGET_SESSION_ENDS: &str = "DELETE FROM capacity_session_end m \
     WHERE m.deployment_id = $1 AND m.ended_at < $2 \
       AND NOT EXISTS (SELECT 1 FROM instance_state i \
                        WHERE i.deployment_id = m.deployment_id \
                          AND i.instance_id = m.instance_id AND i.terminal_at IS NULL) \
       AND NOT EXISTS (SELECT 1 FROM inflight_sync_chain c \
                        WHERE c.deployment_id = m.deployment_id \
                          AND c.instance_id = m.instance_id)";

/// The holders the sweep releases in one deployment: an instance that finished, or one that was
/// never stored, has no in-flight chain marker, and whose slot was taken by a previous process
/// on this host (`holder LIKE '<host>-%'` but not this process) or before the bound.
const SQL_STRANDED: &str = "SELECT DISTINCT s.instance_id FROM capacity_slot s \
     LEFT JOIN instance_state i \
       ON i.deployment_id = s.deployment_id AND i.instance_id = s.instance_id \
     WHERE s.deployment_id = $1 AND ( \
       i.terminal_at IS NOT NULL \
       OR (i.instance_id IS NULL \
           AND NOT EXISTS (SELECT 1 FROM inflight_sync_chain c \
                           WHERE c.deployment_id = s.deployment_id \
                             AND c.instance_id = s.instance_id) \
           AND ((s.holder LIKE $2 AND s.holder <> $3) OR s.acquired_at < $4)))";

/// PostgreSQL store of the capacity pools.
#[derive(Debug, Clone)]
pub struct PgCapacitySlotStore {
    pool: PgPool,
}

impl PgCapacitySlotStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Take a slot of `claim.pool` for `claim.instance`, under the pool row's lock. `admits` is
    /// the pool's rule over the `(inbound, outbound)` counts it sees; the store only counts.
    pub async fn acquire(
        &self,
        claim: &SlotClaim<'_>,
        admits: &(dyn Fn(i32, i32) -> bool + Send + Sync),
    ) -> Result<SlotGrant> {
        let increment = match claim.direction {
            SLOT_INBOUND => SQL_INCREMENT_INBOUND,
            SLOT_OUTBOUND => SQL_INCREMENT_OUTBOUND,
            other => {
                return Err(PersistenceError::InvalidArgument(format!(
                    "capacity slot direction '{other}' is not {SLOT_INBOUND} or {SLOT_OUTBOUND}"
                )))
            }
        };
        let mut tx = begin_deployment_tx(&self.pool, claim.deployment).await?;
        if claim.direction == SLOT_OUTBOUND
            && Self::session_ended_in(&mut tx, claim.deployment, claim.instance, claim.call_key)
                .await?
        {
            return finish(tx, SlotGrant::SessionEnded).await;
        }
        if claim.require_live {
            let ended: Option<bool> = sqlx::query_scalar(SQL_INSTANCE_ENDED)
                .bind(claim.deployment.as_str())
                .bind(claim.instance)
                .fetch_optional(&mut *tx)
                .await
                .map_err(PersistenceError::db("capacity instance-ended read"))?;
            if ended == Some(true) {
                return finish(tx, SlotGrant::InstanceEnded).await;
            }
        }
        sqlx::query(SQL_ENSURE_POOL)
            .bind(claim.pool)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity ensure pool row"))?;
        let row = sqlx::query(SQL_LOCK_POOL)
            .bind(claim.pool)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity lock pool row"))?;
        let inbound_used: i32 = row.get("inbound_used");
        let outbound_used: i32 = row.get("outbound_used");
        // Checked under the lock, so a concurrent acquisition for the same holder cannot slip in.
        let held = sqlx::query(SQL_HELD)
            .bind(claim.deployment.as_str())
            .bind(claim.instance)
            .bind(claim.pool)
            .bind(claim.call_key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity held read"))?;
        if held.is_some() {
            return finish(tx, SlotGrant::AlreadyHeld).await;
        }
        if !admits(inbound_used, outbound_used) {
            return finish(
                tx,
                SlotGrant::Exhausted {
                    inbound_used,
                    outbound_used,
                },
            )
            .await;
        }
        sqlx::query(SQL_INSERT_SLOT)
            .bind(claim.deployment.as_str())
            .bind(claim.instance)
            .bind(claim.pool)
            .bind(claim.call_key)
            .bind(claim.tenant)
            .bind(claim.direction)
            .bind(claim.holder)
            .bind(OffsetDateTime::now_utc())
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity insert slot"))?;
        sqlx::query(increment)
            .bind(claim.pool)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity increment"))?;
        finish(tx, SlotGrant::Acquired).await
    }

    /// Re-key every slot `from` holds in `deployment` to `to`. Counts are unchanged.
    pub async fn rebind(&self, deployment: &DeploymentId, from: Uuid, to: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_REBIND)
            .bind(deployment.as_str())
            .bind(from)
            .bind(to)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity rebind"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity rebind commit"))
    }

    /// Release every slot `instance` holds in `deployment`, decrementing each pool's counter in
    /// the same transaction. Idempotent; returns how many slots were released.
    pub async fn release(&self, deployment: &DeploymentId, instance: Uuid) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let released = Self::release_in(&mut tx, deployment, instance).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity release commit"))?;
        Ok(released)
    }

    /// End `instance`'s whole session (`call_key = None`) or one call, in one transaction:
    /// record it (V704), so a later OUTBOUND claim it covers answers [`SlotGrant::SessionEnded`],
    /// and release the slots it covers. Idempotent; returns the directions released.
    pub async fn end_session(
        &self,
        deployment: &DeploymentId,
        instance: Uuid,
        call_key: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RECORD_SESSION_END)
            .bind(deployment.as_str())
            .bind(instance)
            .bind(call_key.unwrap_or_default())
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity session-end record"))?;
        let selection = call_key.map(SlotSelection::Call);
        let freed = Self::release_selected(&mut tx, deployment, instance, selection).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity session-end commit"))?;
        Ok(freed)
    }

    /// Whether a session end covers `instance`'s `call_key` (its whole session, or that call).
    pub async fn session_ended(
        &self,
        deployment: &DeploymentId,
        instance: Uuid,
        call_key: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let ended = Self::session_ended_in(&mut tx, deployment, instance, call_key).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity session-ended commit"))?;
        Ok(ended)
    }

    async fn session_ended_in(
        tx: &mut Transaction<'static, Postgres>,
        deployment: &DeploymentId,
        instance: Uuid,
        call_key: &str,
    ) -> Result<bool> {
        sqlx::query_scalar(SQL_SESSION_ENDED)
            .bind(deployment.as_str())
            .bind(instance)
            .bind(call_key)
            .fetch_one(&mut **tx)
            .await
            .map_err(PersistenceError::db("capacity session-ended read"))
    }

    /// Release the slots of `instance` that `selection` names, decrementing each pool's counter
    /// in the same transaction. Idempotent; returns the directions released.
    pub async fn release_calls(
        &self,
        deployment: &DeploymentId,
        instance: Uuid,
        selection: SlotSelection<'_>,
    ) -> Result<Vec<String>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let freed = Self::release_selected(&mut tx, deployment, instance, Some(selection)).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity release commit"))?;
        Ok(freed)
    }

    /// Release `instance`'s slots for `call_key` when the instance is over (see
    /// `SQL_INSTANCE_OVER`); a no-op while it runs. Returns how many were released.
    pub async fn release_call_if_over(
        &self,
        deployment: &DeploymentId,
        instance: Uuid,
        call_key: &str,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let over: bool = sqlx::query_scalar(SQL_INSTANCE_OVER)
            .bind(deployment.as_str())
            .bind(instance)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity instance-over read"))?;
        let freed = if over {
            Self::release_selected(
                &mut tx,
                deployment,
                instance,
                Some(SlotSelection::Call(call_key)),
            )
            .await?
            .len() as u64
        } else {
            0
        };
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity release commit"))?;
        Ok(freed)
    }

    async fn release_in(
        tx: &mut Transaction<'static, Postgres>,
        deployment: &DeploymentId,
        instance: Uuid,
    ) -> Result<u64> {
        Ok(Self::release_selected(tx, deployment, instance, None)
            .await?
            .len() as u64)
    }

    /// DELETE the selected slot rows (all of the instance's when `selection` is `None`) and
    /// decrement their pools' counters. The DELETE … RETURNING makes a concurrent double release
    /// count once: the second deleter finds no row.
    async fn release_selected(
        tx: &mut Transaction<'static, Postgres>,
        deployment: &DeploymentId,
        instance: Uuid,
        selection: Option<SlotSelection<'_>>,
    ) -> Result<Vec<String>> {
        let query = match selection {
            None => sqlx::query(SQL_RELEASE),
            Some(SlotSelection::Call(_)) => sqlx::query(SQL_RELEASE_CALL),
            Some(SlotSelection::Direction(_)) => sqlx::query(SQL_RELEASE_DIRECTION),
        };
        let query = query.bind(deployment.as_str()).bind(instance);
        let query = match selection {
            None => query,
            Some(SlotSelection::Call(value) | SlotSelection::Direction(value)) => query.bind(value),
        };
        let rows = query
            .fetch_all(&mut **tx)
            .await
            .map_err(PersistenceError::db("capacity release"))?;
        let mut freed: Vec<(String, String)> = rows
            .iter()
            .map(|r| (r.get::<String, _>("pool"), r.get::<String, _>("direction")))
            .collect();
        // Pool rows are locked in name order, so two releases spanning the same pools cannot
        // deadlock each other.
        freed.sort();
        for (pool, direction) in &freed {
            let decrement = if direction == SLOT_OUTBOUND {
                SQL_DECREMENT_OUTBOUND
            } else {
                SQL_DECREMENT_INBOUND
            };
            sqlx::query(decrement)
                .bind(pool)
                .execute(&mut **tx)
                .await
                .map_err(PersistenceError::db("capacity decrement"))?;
        }
        Ok(freed.into_iter().map(|(_, direction)| direction).collect())
    }

    /// `(inbound, outbound)` slots held in `pool` across every deployment.
    pub async fn usage(&self, pool: &str) -> Result<(i32, i32)> {
        let row = sqlx::query(SQL_USAGE)
            .bind(pool)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("capacity usage"))?;
        Ok(row
            .map(|r| (r.get("inbound_used"), r.get("outbound_used")))
            .unwrap_or((0, 0)))
    }

    /// Release `deployment`'s stranded slots: those of a finished
    /// instance, and those of an instance that was never stored and has no in-flight chain
    /// marker once their holder is a previous process on `host` (any holder starting `<host>-`
    /// other than `current_holder`) or they are older than `max_age`. Returns how many slots
    /// were released.
    pub async fn sweep(
        &self,
        deployment: &DeploymentId,
        host: &str,
        current_holder: &str,
        max_age: std::time::Duration,
    ) -> Result<u64> {
        let cutoff = OffsetDateTime::now_utc() - max_age;
        let host_pattern = format!("{}-%", escape_like(host));
        let instances: Vec<Uuid> = {
            let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
            let rows = sqlx::query(SQL_STRANDED)
                .bind(deployment.as_str())
                .bind(host_pattern)
                .bind(current_holder)
                .bind(cutoff)
                .fetch_all(&mut *tx)
                .await
                .map_err(PersistenceError::db("capacity sweep scan"))?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("capacity sweep scan commit"))?;
            rows.iter().map(|r| r.get("instance_id")).collect()
        };
        let mut released = 0;
        for instance in instances {
            released += self.release(deployment, instance).await?;
        }
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_FORGET_SESSION_ENDS)
            .bind(deployment.as_str())
            .bind(cutoff)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("capacity sweep session ends"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("capacity sweep session ends commit"))?;
        Ok(released)
    }
}

async fn finish(tx: Transaction<'static, Postgres>, grant: SlotGrant) -> Result<SlotGrant> {
    tx.commit()
        .await
        .map_err(PersistenceError::db("capacity acquire commit"))?;
    Ok(grant)
}

/// Escape `LIKE` metacharacters in a host name (`\` is PostgreSQL's default escape).
fn escape_like(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
