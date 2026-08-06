//! Instance store — the engine's recovery substrate (`instance_state`, V401–V404).
//!
//! Semantics: UPSERT persist keyed by
//! `(deployment_id, instance_id)`, row-locking `load_for_update` for replica safety, the
//! claim/heartbeat/release/sweep quartet (claim = CAS on "unowned OR already mine";
//! heartbeat returning 0 rows means "swept — abandon"; release is the owner-scoped
//! hand-back at the quiescent point; sweep clears claims whose heartbeat lapsed).
//!
//! **Terminal retention (V404).** A finished instance no longer deletes its row: the terminal
//! transaction re-stamps the snapshot ([`InstanceSnapshot::mark_terminal`]) and stamps
//! `terminal_at`, so `GET /sutra/instances/{id}` keeps answering for the configured retention
//! window instead of 404-ing the instant the process ends. `terminal_at IS NULL` is therefore the
//! definition of LIVE across this store: [`InstanceStore::count_active`] and the default
//! [`InstanceStore::list`] both key off it, and [`PgInstanceStore::purge_terminal`] is what
//! eventually drops the retained rows. FAILED instances are NOT terminal in this sense — they stay
//! `terminal_at IS NULL`, keep counting as active, and are released only by an operator cancel.
//!
//! **Ownership model.** A resume path (relay correlation or a fired timer) claims the
//! instance BEFORE it rehydrates and hands it back when the step commits, so two replicas
//! cannot rehydrate and advance the same instance concurrently. The CAS is
//! *re-entrant for the same owner* — one process re-claiming an instance it already owns
//! refreshes `claimed_at`/`last_heartbeat_at` rather than bouncing itself — because an
//! owner id names ONE serial lane: the engine's owner is the per-process replica id
//! suffixed with the actor-lane index (`sutra_channels::bridge::replica_id` +
//! `-s<shard>`, `PersistenceBridge::with_shard_owner`), and one lane advances its
//! instances on a single actor thread: same owner ⇒ same lane ⇒ already serialised.
//! The owner string is OPAQUE to this store — it only ever compares it for equality.
//! Every claim-clearing write is owner-scoped (`claim_owner = <me>`), so a late/duplicate
//! release can never steal a claim a successor legitimately re-took.

use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::snapshot::InstanceSnapshot;
use crate::{DeploymentId, PersistenceError, Result};

/// One persisted instance row: the id + the opaque snapshot bytes
/// ([`crate::snapshot::InstanceSnapshot`] v2 format).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceState {
    /// Instance id (row key together with the deployment pin).
    pub instance_id: Uuid,
    /// Opaque snapshot v2 bytes.
    pub serialised: Vec<u8>,
}

/// One persisted instance row read under its row lock, with the two facts a mutating admin
/// operation must re-assert before it writes: who owns the ownership claim, and whether the row has
/// already become terminal. See [`PgInstanceStore::load_owned_for_update`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedInstanceState {
    /// The row itself (id + snapshot bytes).
    pub state: InstanceState,
    /// The replica identity currently holding the claim, or `None` when the row is unowned.
    pub claim_owner: Option<String>,
    /// `true` once `terminal_at` is stamped — the row is retained history, not live state.
    pub terminal: bool,
}

/// Minimal filter for the operate-time instance list. `None`/`false` fields don't constrain.
#[derive(Debug, Clone, Default)]
pub struct InstanceFilter {
    /// Narrow to a single snapshot status (e.g. [`crate::snapshot::STATUS_SUSPENDED`]). The
    /// status lives INSIDE the snapshot bytes, not a column, so it is matched after decode
    /// rather than pushed into SQL.
    pub status: Option<String>,
    /// Include instances that have already finished — the rows terminal retention now keeps
    /// (`terminal_at IS NOT NULL`; see `V404__instance_terminal_at.sql`). **Default `false`**: the
    /// list surface has no paging, and a busy deployment accumulates a whole retention window of
    /// finished instances, so the unfiltered list must keep meaning "what is still in flight".
    /// Pushed into SQL on the PostgreSQL store (the column is indexed); the mysql/mssql stores
    /// have no terminal writer and therefore no terminal rows, so they ignore it.
    pub include_terminal: bool,
}

/// One lightweight instance summary for the operate-time list surface: the row key
/// (instance id + its pinned deployment) and the decoded status — enough to render a list
/// and drill into the [`InstanceStore::load`] inspect view. Variables are deliberately
/// absent here (the inspect view carries them, with `@sensitive` values redacted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceSummary {
    /// Instance id (row key together with the deployment pin).
    pub instance_id: Uuid,
    /// The instance's pinned deployment (the row's single isolation column, R1).
    pub deployment_id: String,
    /// Decoded snapshot status (`sutra.status`; e.g. `SUSPENDED` / `RUNNING`).
    pub status: String,
}

/// Decode `(instance_id, serialised)` rows into summaries pinned to `deployment`, keeping
/// only rows whose decoded status matches `filter.status` when set. Shared by every
/// dialect's `list` — the status is a snapshot field, never a column, so the decode + status
/// filter live here rather than in per-dialect SQL.
pub(crate) fn summarise_instances(
    deployment: &DeploymentId,
    rows: Vec<(Uuid, Vec<u8>)>,
    filter: &InstanceFilter,
) -> Result<Vec<InstanceSummary>> {
    let mut summaries = Vec::with_capacity(rows.len());
    for (instance_id, serialised) in rows {
        // `peek` reads only the routing keys (status included) — no full variable decode.
        let keys = InstanceSnapshot::peek(&serialised).map_err(|e| {
            PersistenceError::InvalidArgument(format!(
                "instance {instance_id} has an undecodable snapshot: {e}"
            ))
        })?;
        if let Some(want) = &filter.status {
            if keys.status != *want {
                continue;
            }
        }
        summaries.push(InstanceSummary {
            instance_id,
            deployment_id: deployment.as_str().to_owned(),
            status: keys.status,
        });
    }
    Ok(summaries)
}

/// Store trait for instance recovery state, including the stuck-instance scan.
pub trait InstanceStore {
    /// UPSERT the serialised state; refreshes `updated_at`.
    async fn persist(&self, deployment: &DeploymentId, state: &InstanceState) -> Result<()>;
    /// Loads a row, or `None` when absent (or invisible to this deployment).
    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>>;
    /// Deployment-scoped in-flight count (quota gate + the RETIRED-deployment check).
    ///
    /// **ACTIVE excludes terminal.** Since terminal retention (V404) a finished instance keeps its
    /// row for the retention window, and this count is what the deploy quiescence gate waits on —
    /// counting retained corpses would pin a DRAINING deployment open for the whole retention
    /// window. A FAILED instance DOES still count: it is not finished, it is waiting for a human,
    /// and its deployment must not retire out from under it.
    async fn count_active(&self, deployment: &DeploymentId) -> Result<i64>;
    /// Lists a deployment's instances as lightweight [`InstanceSummary`] rows (the operate-time
    /// inspection surface), optionally narrowed to one status. Status is decoded from each snapshot
    /// (it is not a column), so `filter.status` is applied after decode.
    async fn list(
        &self,
        deployment: &DeploymentId,
        filter: &InstanceFilter,
    ) -> Result<Vec<InstanceSummary>>;
    /// Deletes a row; 0 rows affected is a silent no-op.
    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Atomic claim: CAS on "unowned OR already mine" (`claim_owner IS NULL OR
    /// claim_owner = <owner>`), stamping `claimed_at` + `last_heartbeat_at`. `true` when
    /// this owner holds the instance afterwards.
    ///
    /// `false` is deliberately ambiguous at this layer: either another replica's claim
    /// stands, or the row no longer exists (the instance completed). Callers that need to
    /// tell those apart re-read the row — the engine's resume paths do exactly that, so a
    /// vanished instance keeps its permanent not-found posture instead of bouncing as
    /// contention.
    async fn claim(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<bool>;
    /// Refreshes `last_heartbeat_at` while still owned; 0 rows means "swept — abandon".
    async fn heartbeat(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64>;
    /// Hands the claim back: clears `claim_owner`/`claimed_at`/`last_heartbeat_at` for
    /// rows THIS owner holds; returns rows released (0 = we did not hold it — a no-op, not
    /// an error). Owner-scoped by construction, so a redundant release (the resume path's
    /// belt-and-braces drop guard firing after the step already released in-transaction)
    /// can never clear a successor's claim.
    async fn release(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64>;
    /// Clears claims whose `last_heartbeat_at` lapsed past `claim_timeout`; returns rows swept.
    async fn sweep_stuck(
        &self,
        deployment: &DeploymentId,
        claim_timeout: std::time::Duration,
    ) -> Result<u64>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgInstanceStore {
    pool: PgPool,
}

const SQL_UPSERT: &str = "INSERT INTO instance_state \
     (deployment_id, instance_id, serialised, updated_at) \
     VALUES ($1, $2, $3, CURRENT_TIMESTAMP) \
     ON CONFLICT (deployment_id, instance_id) DO UPDATE \
     SET serialised = EXCLUDED.serialised, updated_at = CURRENT_TIMESTAMP";

const SQL_SELECT: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = $1 AND instance_id = $2";

const SQL_SELECT_FOR_UPDATE: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = $1 AND instance_id = $2 FOR UPDATE";

/// [`SQL_SELECT_FOR_UPDATE`] plus the ownership columns — the admin instance-migration read.
/// A migration must re-verify UNDER THE ROW LOCK that the claim it took a moment ago is still
/// its own, so it cannot move a row that a resume path re-claimed in between.
const SQL_SELECT_OWNED_FOR_UPDATE: &str =
    "SELECT instance_id, serialised, claim_owner, terminal_at FROM instance_state \
     WHERE deployment_id = $1 AND instance_id = $2 FOR UPDATE";

const SQL_DELETE: &str = "DELETE FROM instance_state WHERE deployment_id = $1 AND instance_id = $2";

/// ACTIVE = not terminal (V404). See [`InstanceStore::count_active`] for why the retained rows
/// must not be counted — the deploy quiescence gate is the caller that would deadlock on them.
const SQL_COUNT_ACTIVE: &str =
    "SELECT COUNT(*) FROM instance_state WHERE deployment_id = $1 AND terminal_at IS NULL";

const SQL_LIST_ACTIVE: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = $1 AND terminal_at IS NULL ORDER BY updated_at DESC";

/// The history form of the list — retained terminal rows included
/// ([`InstanceFilter::include_terminal`]).
const SQL_LIST_ALL: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = $1 ORDER BY updated_at DESC";

/// The retain-at-terminal write: swap in the re-stamped snapshot, stamp `terminal_at`, and clear
/// the ownership claim — one statement inside the caller's terminal transaction.
///
/// The claim clear is deliberately NOT owner-scoped (unlike [`SQL_RELEASE`]). A terminal row is
/// owned by nobody by definition, and this statement only ever runs as the final act of the
/// replica that WAS the owner (the resume path's terminal commit) or of an admin cancel that never
/// claimed at all — so scoping it to an owner could only leave a claim standing on a corpse, which
/// the stuck-instance sweeper would then have to clean up for no reason.
const SQL_MARK_TERMINAL: &str = "UPDATE instance_state SET \
       serialised = $3, terminal_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP, \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = $1 AND instance_id = $2";

/// The retention purge: drop terminal rows whose retention window has fully elapsed. `<=` so a row
/// sitting exactly ON the boundary is purged (the window is "kept FOR retention", not "kept for
/// retention plus one sweep tick").
const SQL_PURGE_TERMINAL: &str = "DELETE FROM instance_state \
     WHERE deployment_id = $1 AND terminal_at IS NOT NULL \
       AND terminal_at <= now() - make_interval(secs => $2)";

const SQL_HEARTBEAT: &str = "UPDATE instance_state SET last_heartbeat_at = now() \
     WHERE deployment_id = $1 AND instance_id = $2 AND claim_owner = $3";

/// The claim CAS: unowned OR already ours (re-entrant refresh — see the module header).
const SQL_CLAIM: &str = "UPDATE instance_state SET \
       claim_owner = $1, claimed_at = now(), last_heartbeat_at = now() \
     WHERE deployment_id = $2 AND instance_id = $3 \
       AND (claim_owner IS NULL OR claim_owner = $1)";

/// The owner-scoped hand-back — the `claim_owner = $3` predicate is what makes a
/// duplicate release harmless.
const SQL_RELEASE: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = $1 AND instance_id = $2 AND claim_owner = $3";

const SQL_SWEEP: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = $1 \
       AND last_heartbeat_at IS NOT NULL \
       AND last_heartbeat_at < now() - make_interval(secs => $2)";

impl PgInstanceStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// UPSERT on a caller-supplied connection (the transactional-step building block).
    pub async fn persist_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        state: &InstanceState,
    ) -> Result<()> {
        sqlx::query(SQL_UPSERT)
            .bind(deployment.as_str())
            .bind(state.instance_id)
            .bind(&state.serialised)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("instance persist"))?;
        Ok(())
    }

    /// Owner-scoped claim release on a caller-supplied connection — the building block that
    /// lets a step hand ownership back **inside its own transaction**, so the instance
    /// becomes claimable at the exact instant its new quiescent point becomes visible
    /// (no window where the frontier has moved but the claim still stands, and no crash
    /// window that strands a claim the sweeper then has to reclaim).
    ///
    /// Returns rows released: 0 when this owner did not hold the row (a park of a NEW
    /// instance, or a claim already handed back) — a no-op, never an error.
    pub async fn release_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        Ok(sqlx::query(SQL_RELEASE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(claim_owner)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("instance release"))?
            .rows_affected())
    }

    /// **Retain-at-terminal on a caller-supplied connection** — the building block that turns the
    /// terminal step from a DELETE into a durable history record, inside the SAME transaction that
    /// resolves the waits, retires the aliases and enqueues the outbox.
    ///
    /// `serialised` is the re-stamped snapshot ([`InstanceSnapshot::mark_terminal`]) — a key-patch
    /// of the bytes already in the row, so at-rest encryption rides through untouched and no key
    /// material is needed to finish an instance. `terminal_at` is stamped from DATABASE time (the
    /// retention clock must not depend on a replica's wall clock), and the ownership claim is
    /// cleared in the same write.
    ///
    /// Returns rows updated: `0` means the row vanished under us (an admin cancel or a GDPR erasure
    /// landed first). That is a race, not an error — the caller logs it and lets the rest of the
    /// terminal transaction commit, exactly as `commit_failed` does.
    pub async fn mark_terminal_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        serialised: &[u8],
    ) -> Result<u64> {
        Ok(sqlx::query(SQL_MARK_TERMINAL)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(serialised)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("instance markTerminal"))?
            .rows_affected())
    }

    /// **Retention purge** — delete every terminal row of `deployment` whose `terminal_at` is at
    /// or past `retention`; returns the number purged. The [`crate::snapshot`] bytes go with the
    /// row, so this is the point at which a finished instance's variables genuinely leave the
    /// database.
    ///
    /// What it does NOT touch: `audit_event`. The per-token-move journal has its own lifecycle and
    /// its own GDPR story (erasure REDACTS captured payloads and keeps the metadata trail, so that
    /// the erasure itself stays auditable) — sweeping it on the instance's retention clock would
    /// silently delete the compliance record that an instance ran at all. Retention here is about
    /// the resumable-state table, nothing else.
    ///
    /// A `retention` of zero purges every terminal row immediately, which is the correct cleanup
    /// for an operator who has just switched `sutra.instance.retention` to `PT0S`: new terminals
    /// stop being retained at the source, and this sweeps the ones written before the flip.
    ///
    /// pg-only, like [`Self::mark_terminal_in`]: `terminal_at` ships in the pg-only V404, and the
    /// engine's only terminal writer is the `PgPool` persistence bridge.
    pub async fn purge_terminal(
        &self,
        deployment: &DeploymentId,
        retention: std::time::Duration,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let purged = sqlx::query(SQL_PURGE_TERMINAL)
            .bind(deployment.as_str())
            .bind(retention.as_secs_f64())
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance purgeTerminal"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance purgeTerminal commit"))?;
        Ok(purged)
    }

    /// Retain one instance as terminal in its OWN deployment-scoped transaction — the admin-cancel
    /// counterpart of [`Self::mark_terminal_in`] (cancel resolves waits and retires aliases through
    /// their own stores, mirroring the shape it has always had). Returns rows updated.
    pub async fn mark_terminal(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        serialised: &[u8],
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let marked = Self::mark_terminal_in(&mut tx, deployment, instance_id, serialised).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance markTerminal commit"))?;
        Ok(marked)
    }

    /// Row-locking load that ALSO reports the row's ownership + terminal marker — the admin
    /// instance-migration read ([`crate::step::commit_instance_migration`]).
    ///
    /// Migration takes the ownership claim before it validates anything, then moves the row in a
    /// separate transaction. This is the read that closes the gap between those two: under the row
    /// lock it re-asserts that the claim is still the migration's own and that the row has not
    /// become terminal, so a resume that raced in between is refused rather than half-migrated.
    pub async fn load_owned_for_update(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<OwnedInstanceState>> {
        /// The column tuple [`SQL_SELECT_OWNED_FOR_UPDATE`] selects, IN SELECT ORDER.
        type OwnedColumns = (Uuid, Vec<u8>, Option<String>, Option<time::OffsetDateTime>);
        let row: Option<OwnedColumns> = sqlx::query_as(SQL_SELECT_OWNED_FOR_UPDATE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_optional(conn)
            .await
            .map_err(PersistenceError::db("instance loadOwnedForUpdate"))?;
        Ok(row.map(
            |(instance_id, serialised, claim_owner, terminal_at)| OwnedInstanceState {
                state: InstanceState {
                    instance_id,
                    serialised,
                },
                claim_owner,
                terminal: terminal_at.is_some(),
            },
        ))
    }

    /// Row-locking load on a caller-supplied transaction connection (`SELECT ... FOR UPDATE`).
    ///
    /// The lock is held until the caller's transaction commits/rolls back — the
    /// concurrent-replica serialisation point (two replicas racing to advance the same
    /// instance queue on this row lock).
    pub async fn load_for_update(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let row: Option<(Uuid, Vec<u8>)> = sqlx::query_as(SQL_SELECT_FOR_UPDATE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_optional(conn)
            .await
            .map_err(PersistenceError::db("instance loadForUpdate"))?;
        Ok(row.map(|(instance_id, serialised)| InstanceState {
            instance_id,
            serialised,
        }))
    }
}

impl InstanceStore for PgInstanceStore {
    async fn persist(&self, deployment: &DeploymentId, state: &InstanceState) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        Self::persist_in(&mut tx, deployment, state).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance persist commit"))
    }

    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let row: Option<(Uuid, Vec<u8>)> = sqlx::query_as(SQL_SELECT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance load"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance load commit"))?;
        Ok(row.map(|(instance_id, serialised)| InstanceState {
            instance_id,
            serialised,
        }))
    }

    async fn count_active(&self, deployment: &DeploymentId) -> Result<i64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(SQL_COUNT_ACTIVE)
            .bind(deployment.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance countActive"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance countActive commit"))?;
        Ok(count)
    }

    async fn list(
        &self,
        deployment: &DeploymentId,
        filter: &InstanceFilter,
    ) -> Result<Vec<InstanceSummary>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let sql = if filter.include_terminal {
            SQL_LIST_ALL
        } else {
            SQL_LIST_ACTIVE
        };
        let rows: Vec<(Uuid, Vec<u8>)> = sqlx::query_as(sql)
            .bind(deployment.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance list"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance list commit"))?;
        summarise_instances(deployment, rows, filter)
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance delete"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance delete commit"))
    }

    async fn claim(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let updated = sqlx::query(SQL_CLAIM)
            .bind(claim_owner)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance claim"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance claim commit"))?;
        Ok(updated == 1)
    }

    async fn heartbeat(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let updated = sqlx::query(SQL_HEARTBEAT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(claim_owner)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance heartbeat"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance heartbeat commit"))?;
        Ok(updated)
    }

    async fn release(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let released = Self::release_in(&mut tx, deployment, instance_id, claim_owner).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance release commit"))?;
        Ok(released)
    }

    async fn sweep_stuck(
        &self,
        deployment: &DeploymentId,
        claim_timeout: std::time::Duration,
    ) -> Result<u64> {
        // Fractional-seconds bind so sub-second timeouts keep precision (make_interval
        // accepts numeric secs).
        let seconds = claim_timeout.as_secs_f64();
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let swept = sqlx::query(SQL_SWEEP)
            .bind(deployment.as_str())
            .bind(seconds)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("instance sweep"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance sweep commit"))?;
        Ok(swept)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::snapshot::{STATUS_RUNNING, STATUS_SUSPENDED};

    fn dep() -> DeploymentId {
        DeploymentId::new("dep-0123456789abcdef01234567").unwrap()
    }

    fn snap_bytes(status: &str) -> Vec<u8> {
        InstanceSnapshot::of("p1", dep().as_str(), status, vec![], BTreeMap::new()).write()
    }

    // ---- summarise_instances (the tier-1 list projection: decode + status filter) ----------

    #[test]
    fn summarise_decodes_status_and_pins_deployment() {
        let id = Uuid::new_v4();
        let rows = vec![(id, snap_bytes(STATUS_SUSPENDED))];
        let out = summarise_instances(&dep(), rows, &InstanceFilter::default()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].instance_id, id);
        assert_eq!(out[0].deployment_id, dep().as_str());
        assert_eq!(out[0].status, STATUS_SUSPENDED);
    }

    #[test]
    fn summarise_status_filter_keeps_only_matches() {
        let rows = vec![
            (Uuid::new_v4(), snap_bytes(STATUS_SUSPENDED)),
            (Uuid::new_v4(), snap_bytes(STATUS_RUNNING)),
            (Uuid::new_v4(), snap_bytes(STATUS_SUSPENDED)),
        ];
        let filter = InstanceFilter {
            status: Some(STATUS_SUSPENDED.to_owned()),
            ..InstanceFilter::default()
        };
        let out = summarise_instances(&dep(), rows, &filter).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|s| s.status == STATUS_SUSPENDED));
    }

    #[test]
    fn summarise_no_filter_returns_every_status() {
        let rows = vec![
            (Uuid::new_v4(), snap_bytes(STATUS_SUSPENDED)),
            (Uuid::new_v4(), snap_bytes(STATUS_RUNNING)),
        ];
        let out = summarise_instances(&dep(), rows, &InstanceFilter::default()).unwrap();
        assert_eq!(out.len(), 2);
    }
}
