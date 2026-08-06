//! The transactional step primitive — **strict**, and normative for this engine.
//!
//! Execution between two quiescent points (start→first wait, wait→wait, wait→end) is one
//! logical step. At the quiescent point ONE local transaction commits the instance snapshot,
//! the wait-state rows, the alias rows, and the step's outbox enqueues — **commit or
//! nothing**. This is the strict transactional outbox; the reference baseline's enqueue is
//! not strictly atomic with the snapshot (an accepted divergence), so this behaviour is
//! proven by the crate's own tests rather than by conformance against it.
//!
//! No XA, ever: external effects stay outside the transaction by construction; delivery is
//! at-least-once via the outbox drain + consumer idempotency (`outbox_key`).
//!
//! One thing rides the same transaction without being part of the snapshot: the
//! per-instance ownership CLAIM the resume paths take before they rehydrate is handed back
//! by [`commit_step_with_timers_releasing`], so ownership ends exactly when the new
//! quiescent point becomes visible — commit-or-nothing covers the hand-back too.

use std::collections::BTreeMap;

use uuid::Uuid;

use crate::scope::{begin_deployment_tx, set_deployment_guc};
use crate::stores::{
    AuditEventRow, OutboxEntry, PgAliasStore, PgAuditEventStore, PgInstanceStore, PgOutboxStore,
    PgSubjectIndexStore, PgWaitStateStore, STATUS_WAITING,
};
use crate::{DeploymentId, PersistenceError, Result};

/// One wait-state row to record at the quiescent point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepWait {
    /// The wait node the token parks at.
    pub node_id: String,
    /// Archive-local process id (denormalised for the admin listing).
    pub process_id: String,
    /// Reserved-but-nullable correlation key.
    pub correlation_key: Option<String>,
}

/// One alias row to record at the quiescent point (instance-start aliases).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepAlias {
    /// Alias name from the `<q:alias>` binding.
    pub alias_name: String,
    /// Alias value.
    pub alias_value: String,
    /// Whether the unique-live guarantee applies (collision fails the whole step).
    pub unique: bool,
}

/// One GDPR blind-index row a step records atomically with the snapshot: the HMAC blind of a
/// `@subjectKey` variable's value, so the instance is enumerable for disclosure/erasure with no
/// cleartext PII. Written in the SAME transaction as the snapshot, so a subject is discoverable iff
/// its instance actually persisted.
#[derive(Debug, Clone)]
pub struct StepSubject {
    /// The subject key name (the `@subjectKey` variable's name, e.g. `customerId`).
    pub subject_name: String,
    /// The blind index — lowercase hex of `HMAC-SHA256(indexKey, normalize(value))`.
    pub blind_value: String,
}

/// Everything one logical step persists atomically at its quiescent point.
#[derive(Debug, Clone)]
pub struct StepWrite {
    /// The instance's pinned deployment (isolation column + GUC value for the transaction).
    pub deployment: DeploymentId,
    /// The instance the step advanced.
    pub instance_id: Uuid,
    /// Snapshot v2 bytes ([`crate::snapshot::InstanceSnapshot::write`]) to UPSERT.
    pub snapshot: Vec<u8>,
    /// Wait frontier reached by this step (empty when the step ran to a terminal point).
    pub waits: Vec<StepWait>,
    /// Wait nodes this step satisfied (resolved on the way through, e.g. a relay resume).
    pub resolved_waits: Vec<String>,
    /// Aliases recorded by this step.
    pub aliases: Vec<StepAlias>,
    /// GDPR blind-index rows for this step's `@subjectKey` variables, written atomically with
    /// the snapshot. Empty when encryption/blind-indexing is off or the instance has no subject keys.
    pub subjects: Vec<StepSubject>,
    /// Outbox enqueues emitted by this step (`<q:send>` / async `<q:reply>`). Each entry's
    /// `deployment` and `instance_id` must match the step's.
    pub outbox: Vec<OutboxEntry>,
    /// Channel-call nodes whose OUTSTANDING outbox rows this step WITHDRAWS (deletes by
    /// `(instance, node)`, pending and poisoned alike) — the nodes sitting in a `<q:retry>`
    /// BACKOFF window at this quiescent point. A backoff park kills its attempt's request in
    /// the same commit that parks the backoff timer: delivered late it would double-submit
    /// against the re-drive's fresh emission, and poisoned later it would mis-fire a failure
    /// against the live attempt. Idempotent by construction (a marked node has no rows after
    /// its first park; the re-drive clears the marker BEFORE its fresh emission enqueues), so
    /// callers pass the full marked set, not a delta. Empty for every step outside the
    /// channel-call retry feature — the pre-F1 behaviour.
    pub withdrawn_call_nodes: Vec<String>,
}

/// Commits one logical step in a single deployment-scoped transaction — or nothing.
///
/// Write order inside the transaction: resolved waits → snapshot UPSERT → new wait rows →
/// alias rows → outbox enqueues. Any failure (including a unique-live alias collision,
/// surfaced as [`PersistenceError::AliasCollision`]) rolls the whole transaction back: zero
/// rows land in ANY table.
pub async fn commit_step(pool: &sqlx::PgPool, step: &StepWrite) -> Result<()> {
    for entry in &step.outbox {
        if entry.deployment != step.deployment || entry.instance_id != step.instance_id {
            return Err(PersistenceError::InvalidArgument(format!(
                "outbox entry {} does not belong to step instance {} / {}",
                entry.entry_id, step.instance_id, step.deployment
            )));
        }
    }
    let mut tx = begin_deployment_tx(pool, &step.deployment).await?;
    write_step_in(&mut tx, step).await?;
    tx.commit()
        .await
        .map_err(PersistenceError::db("step commit"))
}

/// The step's writes on an already-open (deployment-scoped) transaction. Exposed so tests
/// can prove atomicity by dropping the transaction before commit; production callers use
/// [`commit_step`].
pub async fn write_step_in(conn: &mut sqlx::PgConnection, step: &StepWrite) -> Result<()> {
    for node_id in &step.resolved_waits {
        sqlx::query(
            "UPDATE waiting_event SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
             WHERE deployment_id = $1 AND instance_id = $2 AND node_id = $3 AND status = 'WAITING'",
        )
        .bind(step.deployment.as_str())
        .bind(step.instance_id)
        .bind(node_id)
        .execute(&mut *conn)
        .await
        .map_err(PersistenceError::db("step resolve wait"))?;
    }

    // The channel-call retry WITHDRAWAL: a backoff-parked node's dead attempt loses its
    // outstanding request rows in the same commit. Runs BEFORE the step's own enqueues, so a
    // re-drive that (illegally) marked and emitted for one node could never delete its own
    // fresh row.
    for node_id in &step.withdrawn_call_nodes {
        PgOutboxStore::withdraw_for_node_in(conn, &step.deployment, step.instance_id, node_id)
            .await?;
    }

    PgInstanceStore::persist_in(
        conn,
        &step.deployment,
        &crate::stores::InstanceState {
            instance_id: step.instance_id,
            serialised: step.snapshot.clone(),
        },
    )
    .await?;

    for wait in &step.waits {
        // A node this same step RESOLVED and re-parks is a NEW incarnation of the wait (the
        // channel-call `<q:retry>` re-drive resolving its backoff TIMER row into a fresh
        // response MESSAGE wait): reset the timer columns, or the dead backoff's
        // already-elapsed due-at would ride the new row as a phantom fire. Every other wait
        // keeps the plain upsert, which deliberately preserves a pending timer's due-at.
        let fresh = step.resolved_waits.iter().any(|n| n == &wait.node_id);
        if fresh {
            PgWaitStateStore::record_waiting_fresh_in(
                conn,
                &step.deployment,
                step.instance_id,
                &wait.process_id,
                &wait.node_id,
                wait.correlation_key.as_deref(),
            )
            .await?;
        } else {
            PgWaitStateStore::record_waiting_in(
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
        let recorded = PgAliasStore::record_in(
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

    for subject in &step.subjects {
        // Idempotent (ON CONFLICT DO NOTHING) — re-parking the same instance re-writes the same
        // (subject_name, blind_value) harmlessly. RLS GUC is already SET LOCAL by the caller's tx.
        PgSubjectIndexStore::record_in(
            conn,
            &step.deployment,
            step.instance_id,
            &subject.subject_name,
            &subject.blind_value,
        )
        .await?;
    }

    for entry in &step.outbox {
        PgOutboxStore::enqueue_in(conn, entry).await?;
    }
    Ok(())
}

// ---- timer addendum (ADDITIVE; the base StepWrite shape is untouched) -----------------------

/// One TIMER wait row a park step records atomically with the snapshot (the
/// `waiting_event` TIMER marker, V803).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepTimerWait {
    /// The timer node (timer catch event / timer boundary / `#timeout` synthetic).
    pub node_id: String,
    /// Archive-local process id (denormalised, same as the MESSAGE rows).
    pub process_id: String,
    /// When the timer becomes claimable.
    pub due_at: time::OffsetDateTime,
}

/// [`commit_step`] plus TIMER wait rows — one deployment-scoped transaction, commit or
/// nothing. The timer rows are written AFTER the base step's wait rows, so a node that is
/// both a token position and a timer (an intermediate timer catch) ends up marked TIMER
/// with its due-at.
pub async fn commit_step_with_timers(
    pool: &sqlx::PgPool,
    step: &StepWrite,
    timer_waits: &[StepTimerWait],
) -> Result<()> {
    commit_step_with_timers_releasing(pool, step, timer_waits, None).await
}

/// [`commit_step_with_timers`] that additionally hands the instance's ownership CLAIM back
/// **inside the step's own transaction** (ADDITIVE; the [`StepWrite`] shape is untouched —
/// same posture as the timer addendum above).
///
/// `claim_owner` is the replica identity the resume path claimed under
/// (`sutra_channels::bridge::replica_id`); `None` skips the release entirely — what a PARK of
/// a brand-new instance passes, since an instance no other replica can name yet is never
/// claimed. The release is owner-scoped, so even a mismatched owner is a no-op (0 rows, no
/// error) rather than a way to clear someone else's claim.
///
/// Why in-transaction: the new quiescent point and the hand-back become visible in the same
/// commit, so there is no window in which the frontier has moved on while the claim still
/// stands, and no crash window between "step committed" and "claim released" for the
/// engine's `StuckInstanceScanner` to have to clean up.
pub async fn commit_step_with_timers_releasing(
    pool: &sqlx::PgPool,
    step: &StepWrite,
    timer_waits: &[StepTimerWait],
    claim_owner: Option<&str>,
) -> Result<()> {
    for entry in &step.outbox {
        if entry.deployment != step.deployment || entry.instance_id != step.instance_id {
            return Err(PersistenceError::InvalidArgument(format!(
                "outbox entry {} does not belong to step instance {} / {}",
                entry.entry_id, step.instance_id, step.deployment
            )));
        }
    }
    let mut tx = begin_deployment_tx(pool, &step.deployment).await?;
    write_step_with_timers_in(&mut tx, step, timer_waits).await?;
    if let Some(owner) = claim_owner {
        PgInstanceStore::release_in(&mut tx, &step.deployment, step.instance_id, owner).await?;
    }
    tx.commit()
        .await
        .map_err(PersistenceError::db("step commit"))
}

/// The timer-carrying step's writes on an already-open (deployment-scoped) transaction —
/// the crash-injection building block, mirroring [`write_step_in`].
pub async fn write_step_with_timers_in(
    conn: &mut sqlx::PgConnection,
    step: &StepWrite,
    timer_waits: &[StepTimerWait],
) -> Result<()> {
    write_step_in(conn, step).await?;
    for timer in timer_waits {
        PgWaitStateStore::record_timer_waiting_in(
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

// ---- instance migration (P1-8): the one commit shape that spans TWO deployment scopes -------

/// Everything the admin migrate operation moves from one deployment pin to another.
///
/// Deliberately a data record rather than a set of store calls: the move must be
/// commit-or-nothing across five tables AND two RLS scopes, and the only way to say that is one
/// transaction that owns the whole set. See [`commit_instance_migration`].
#[derive(Debug, Clone)]
pub struct InstanceMigration {
    /// The pin the instance currently lives under (the READ scope).
    pub from: DeploymentId,
    /// The pin it is moving to (the WRITE scope) — an ACTIVE deployment, checked by the caller.
    pub to: DeploymentId,
    /// The instance being moved.
    pub instance_id: Uuid,
    /// The re-pinned + node-rewritten snapshot bytes
    /// ([`crate::snapshot::InstanceSnapshot::migrate_pinned`]).
    pub snapshot: Vec<u8>,
    /// Source node id → target node id. Entries absent from the map map to themselves, so an
    /// identity migration passes an empty map.
    pub node_mapping: BTreeMap<String, String>,
    /// The process id the instance is being re-homed INTO (v2 cross-process migration), or `None`
    /// for the overwhelmingly common same-process move.
    ///
    /// `Some` rewrites the `process_id` column of every wait row the move carries, matching the
    /// `sutra.processId` the caller already patched into the snapshot
    /// ([`crate::snapshot::InstanceSnapshot::migrate_pinned`]). The two must not disagree: the
    /// timer poller reads the row's process id when it reports a due park, and the admin listing
    /// renders it, so a row still naming the source process would describe the instance as living
    /// somewhere it no longer does.
    pub process_id: Option<String>,
    /// Re-arm the instance's parks as part of THIS transaction — the durable half of the
    /// migrate-then-resume convenience (v2). `None` (the default, and every v1 caller) moves the
    /// rows exactly as they were.
    ///
    /// `Some(frontier)` carries the TARGET-side node ids of the snapshot's wait frontier. Rows are
    /// re-armed (`status = WAITING`, `resolved_at = NULL`) when they are named by that frontier OR
    /// when they share the instance's LATEST `resolved_at` — which is precisely the set the failure
    /// commit tore down, because it resolves every live park in one statement and nothing touches a
    /// FAILED instance's rows afterwards. That set is the park step's own arming: the frontier's
    /// rows plus the boundary / `<q:timeout>` rows attached to them, whose ids the frontier does not
    /// name. An EMPTY frontier re-arms nothing — an instance with no durable park has nothing to
    /// restore, and an older satisfied wait is spent history, not a park.
    pub rearm_parks: Option<std::collections::BTreeSet<String>>,
    /// The ownership claim the caller holds on the source row, re-asserted under the row lock.
    /// The move DROPS the claim with the source row, so the migrated row lands unowned and
    /// immediately resumable.
    pub claim_owner: String,
    /// Carry the instance's `audit_event` journal across the pin. `true` unless an operator
    /// deliberately wants the trail left behind — see [`commit_instance_migration`].
    pub carry_journal: bool,
    /// The migration's own audit row, written LAST under the target scope so the migrated
    /// instance's journal opens with the record of how it got there. `None` when the caller has no
    /// journal to write to.
    pub audit: Option<AuditEventRow>,
}

const SQL_MIGRATE_DELETE_WAITS: &str =
    "DELETE FROM waiting_event WHERE deployment_id = $1 AND instance_id = $2";
const SQL_MIGRATE_DELETE_ALIASES: &str =
    "DELETE FROM alias_index WHERE deployment_id = $1 AND instance_id = $2";
const SQL_MIGRATE_DELETE_SUBJECTS: &str =
    "DELETE FROM subject_index WHERE deployment_id = $1 AND instance_id = $2";
const SQL_MIGRATE_DELETE_INSTANCE: &str =
    "DELETE FROM instance_state WHERE deployment_id = $1 AND instance_id = $2";
const SQL_MIGRATE_DELETE_JOURNAL: &str =
    "DELETE FROM audit_event WHERE deployment_id = $1 AND instance_id = $2";

/// What one migration actually moved — the machine-readable half of the admin response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InstanceMigrationOutcome {
    /// `waiting_event` rows re-written under the target pin (message + timer parks alike).
    pub wait_rows: u64,
    /// `alias_index` rows carried over (relay correlation must keep resolving the instance).
    pub alias_rows: u64,
    /// `subject_index` blind-index rows carried over (GDPR erasure must keep finding it).
    pub subject_rows: u64,
    /// `audit_event` rows carried over (0 when `carry_journal` is false or the journal is empty).
    pub audit_rows: u64,
    /// Parks re-armed by the migrate-then-resume convenience (`rearm_parks`) — rows that landed
    /// under the target pin as WAITING although the failure commit had resolved them. 0 for every
    /// ordinary migration.
    pub rearmed_rows: u64,
}

/// Move one instance — its snapshot, its wait rows, its aliases, its blind-index rows and (by
/// default) its audit journal — from one deployment pin to another, in ONE transaction.
///
/// # The two-scope problem, and why the GUC is set twice
///
/// Every other commit shape in this module runs inside a SINGLE deployment scope: open the
/// transaction, `set_config('sutra.deployment_id', …, true)`, write, commit. Migration is the one
/// operation that cannot, because it reads rows pinned to `from` and writes rows pinned to `to`.
///
/// A plain `UPDATE … SET deployment_id = to` does not work under Row-Level Security. The shipped
/// policies (V403 / V802 / V101 / V1101 / V201) are `USING (deployment_id =
/// current_setting('sutra.deployment_id', true))` with no explicit `WITH CHECK`, and PostgreSQL
/// then uses the `USING` expression as the `WITH CHECK` expression for `UPDATE`. There is no GUC
/// value that satisfies both ends of a scope-CHANGING update, and the two failures differ — which
/// is what makes the trap worth naming (both halves are pinned by
/// `the_two_scope_move_commits_under_an_enforcing_rls_role_where_a_plain_update_cannot`):
///
/// * **GUC = `from`** — the old row passes `USING`, then the re-scoped NEW row is rejected by the
///   implied `WITH CHECK`: an outright `row-level security policy` error.
/// * **GUC = `to`** — the old row is not even visible to `USING`, so the statement SUCCEEDS and
///   matches nothing. Silently. That is the dangerous half.
///
/// What DOES work is the shape below: the GUC is transaction-LOCAL (`set_config(…, true)`), not
/// transaction-IMMUTABLE, so one transaction may re-scope itself between statements. The move runs
/// as two phases inside one commit envelope:
///
/// 1. **GUC = `from`** — lock the instance row, re-assert the claim, read every row that belongs to
///    the instance, then DELETE them. Every statement here sees only source-scoped rows, which is
///    exactly what the policy is for.
/// 2. **GUC = `to`** — INSERT the rewritten rows. Every statement here writes only target-scoped
///    rows, which the policy also permits.
///
/// The commit is still atomic: a failure in phase 2 rolls phase 1 back, and no other session ever
/// observes the instance existing under both pins or under neither. The isolation guarantee is not
/// weakened either — the transaction is never scoped to two deployments AT ONCE; it is scoped to
/// one, finishes with it, and then scopes to the other.
///
/// # What is rewritten, and what is deliberately not
///
/// * The **snapshot** arrives already re-pinned and node-rewritten (a byte-level key patch, so
///   at-rest encryption rides through untouched).
/// * **Wait rows** carry their node id through `node_mapping`, keeping their kind, due-at, status
///   and timestamps — a timer parked for another hour stays parked for another hour. Two v2
///   opt-ins alter that: `process_id` re-homes the rows onto a different process (cross-process
///   migration), and `rearm_parks` brings a FAILED instance's torn-down parks back to WAITING in
///   this same transaction, so migrate-then-resume is one commit rather than two.
/// * **Alias rows** and **blind-index rows** move verbatim: they name business keys, not nodes.
///   Aliases MUST move or relay correlation would stop resolving the instance; blind-index rows
///   MUST move or a GDPR erasure would stop finding it.
/// * **Audit rows** move verbatim, `node_id` INCLUDED. The trail records what happened at the node
///   it happened at, under the graph that was live then; rewriting those ids would falsify it. Only
///   the scope column changes, because the instance-history endpoint resolves scope from the row's
///   owning deployment and a journal left behind would be silently unreachable.
/// * **Outbox rows are NOT moved.** A pending emission was minted by the SOURCE deployment's
///   channel bindings and is dispatched against them; the dispatcher covers draining deployments,
///   so those rows drain where they were made. Moving them would re-target a message at bindings
///   that never produced it.
///
/// Fails closed on: a vanished row, a terminal row, a claim held by anyone else, a mapping that
/// collapses two wait nodes onto one target id, and a unique-live alias already owned by a
/// different live instance under the target (surfaced as [`PersistenceError::AliasCollision`]).
pub async fn commit_instance_migration(
    pool: &sqlx::PgPool,
    migration: &InstanceMigration,
) -> Result<InstanceMigrationOutcome> {
    if migration.from == migration.to {
        return Err(PersistenceError::InvalidArgument(format!(
            "instance {} is already pinned to {} — a migration must name a different target",
            migration.instance_id, migration.to
        )));
    }
    let map_node = |id: &str| -> String {
        migration
            .node_mapping
            .get(id)
            .cloned()
            .unwrap_or_else(|| id.to_owned())
    };

    let mut tx = pool
        .begin()
        .await
        .map_err(PersistenceError::db("begin migration transaction"))?;

    // ---- phase 1: the SOURCE scope — lock, verify, read, delete -----------------------------
    set_deployment_guc(&mut tx, &migration.from).await?;

    let owned =
        PgInstanceStore::load_owned_for_update(&mut tx, &migration.from, migration.instance_id)
            .await?
            .ok_or_else(|| {
                PersistenceError::InvalidArgument(format!(
                    "instance {} vanished from {} before the migration could move it",
                    migration.instance_id, migration.from
                ))
            })?;
    if owned.terminal {
        return Err(PersistenceError::InvalidArgument(format!(
            "instance {} became terminal before the migration could move it — a finished \
             instance is history, not live state",
            migration.instance_id
        )));
    }
    if owned.claim_owner.as_deref() != Some(migration.claim_owner.as_str()) {
        return Err(PersistenceError::InvalidArgument(format!(
            "instance {}'s ownership claim is no longer held by this migration (now: {}) — \
             refusing to move a row something else is advancing",
            migration.instance_id,
            owned.claim_owner.as_deref().unwrap_or("<unowned>")
        )));
    }

    type WaitRowColumns = (
        String,
        String,
        Option<String>,
        String,
        time::OffsetDateTime,
        Option<time::OffsetDateTime>,
        String,
        Option<time::OffsetDateTime>,
    );
    let waits: Vec<WaitRowColumns> = sqlx::query_as(
        "SELECT node_id, process_id, correlation_key, status, created_at, resolved_at, kind, \
         timer_due_at FROM waiting_event WHERE deployment_id = $1 AND instance_id = $2 FOR UPDATE",
    )
    .bind(migration.from.as_str())
    .bind(migration.instance_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(PersistenceError::db("migration read wait rows"))?;
    // The wait table is keyed (deployment, instance, node): a mapping that folds two parked nodes
    // onto one target id would silently lose a park. Caught here rather than as a raw key
    // violation so the refusal names the collision.
    let mut mapped_wait_nodes: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    for w in &waits {
        let mapped = map_node(&w.0);
        if !mapped_wait_nodes.insert(mapped.clone()) {
            return Err(PersistenceError::InvalidArgument(format!(
                "the node mapping folds more than one parked wait node onto target node \
                 '{mapped}' — a wait point would be lost"
            )));
        }
    }

    type AliasRowColumns = (String, String, bool, bool, time::OffsetDateTime);
    let aliases: Vec<AliasRowColumns> = sqlx::query_as(
        "SELECT alias_name, alias_value, unique_alias, live, created_at FROM alias_index \
         WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(migration.from.as_str())
    .bind(migration.instance_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(PersistenceError::db("migration read alias rows"))?;

    type SubjectRowColumns = (String, String, bool, time::OffsetDateTime);
    let subjects: Vec<SubjectRowColumns> = sqlx::query_as(
        "SELECT subject_name, blind_value, live, created_at FROM subject_index \
         WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(migration.from.as_str())
    .bind(migration.instance_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(PersistenceError::db("migration read subject rows"))?;

    type JournalRowColumns = (
        i32,
        time::OffsetDateTime,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    );
    let journal: Vec<JournalRowColumns> = if migration.carry_journal {
        sqlx::query_as(
            "SELECT seq, at, event_type, node_id, diagnostic_code, diagnostic_json, payload_json \
             FROM audit_event WHERE deployment_id = $1 AND instance_id = $2 ORDER BY seq",
        )
        .bind(migration.from.as_str())
        .bind(migration.instance_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(PersistenceError::db("migration read journal rows"))?
    } else {
        Vec::new()
    };

    for (sql, operation) in [
        (SQL_MIGRATE_DELETE_WAITS, "migration delete source waits"),
        (
            SQL_MIGRATE_DELETE_ALIASES,
            "migration delete source aliases",
        ),
        (
            SQL_MIGRATE_DELETE_SUBJECTS,
            "migration delete source subjects",
        ),
        (
            SQL_MIGRATE_DELETE_INSTANCE,
            "migration delete source instance",
        ),
    ] {
        sqlx::query(sql)
            .bind(migration.from.as_str())
            .bind(migration.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db(operation))?;
    }
    if !journal.is_empty() {
        sqlx::query(SQL_MIGRATE_DELETE_JOURNAL)
            .bind(migration.from.as_str())
            .bind(migration.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("migration delete source journal"))?;
    }

    // ---- phase 2: the TARGET scope — write the rewritten rows -------------------------------
    set_deployment_guc(&mut tx, &migration.to).await?;

    PgInstanceStore::persist_in(
        &mut tx,
        &migration.to,
        &crate::stores::InstanceState {
            instance_id: migration.instance_id,
            serialised: migration.snapshot.clone(),
        },
    )
    .await?;

    let mut outcome = InstanceMigrationOutcome::default();
    // The migrate-then-resume re-arm set, decided BEFORE any row is written (see `rearm_parks`):
    // the frontier's own rows, plus every row the failure commit tore down — identified by the
    // single `resolved_at` that one statement stamped on all of them.
    let newest_resolution = migration
        .rearm_parks
        .as_ref()
        .filter(|frontier| !frontier.is_empty())
        .and_then(|_| waits.iter().filter_map(|w| w.5).max());
    for (node_id, process_id, correlation_key, status, created_at, resolved_at, kind, due_at) in
        &waits
    {
        let mapped_node = map_node(node_id);
        let rearm = migration
            .rearm_parks
            .as_ref()
            .filter(|frontier| !frontier.is_empty())
            .is_some_and(|frontier| {
                frontier.contains(&mapped_node)
                    || (resolved_at.is_some() && *resolved_at == newest_resolution)
            });
        let (status, resolved_at) = if rearm {
            (STATUS_WAITING, None)
        } else {
            (status.as_str(), *resolved_at)
        };
        sqlx::query(
            "INSERT INTO waiting_event (deployment_id, instance_id, node_id, process_id, \
             correlation_key, status, created_at, resolved_at, kind, timer_due_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(migration.to.as_str())
        .bind(migration.instance_id)
        .bind(&mapped_node)
        .bind(migration.process_id.as_deref().unwrap_or(process_id))
        .bind(correlation_key)
        .bind(status)
        .bind(created_at)
        .bind(resolved_at)
        .bind(kind)
        .bind(due_at)
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("migration write wait rows"))?;
        outcome.wait_rows += 1;
        if rearm {
            outcome.rearmed_rows += 1;
        }
    }

    for (alias_name, alias_value, unique_alias, live, created_at) in &aliases {
        let inserted = sqlx::query(
            "INSERT INTO alias_index (deployment_id, instance_id, alias_name, alias_value, \
             unique_alias, live, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT DO NOTHING",
        )
        .bind(migration.to.as_str())
        .bind(migration.instance_id)
        .bind(alias_name)
        .bind(alias_value)
        .bind(unique_alias)
        .bind(live)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("migration write alias rows"))?;
        // The unique-live partial index is per-DEPLOYMENT, so an alias that was unambiguous under
        // the source pin can be already taken under the target. `ON CONFLICT DO NOTHING` covers
        // that index too, so the collision arrives as "0 rows written" rather than as an error —
        // and a silently-dropped alias would mean a migrated instance no relay can correlate to.
        // The instance had no rows under the target before this transaction, so 0 rows on a
        // unique live alias can only be somebody else's.
        if inserted.rows_affected() == 0 && *unique_alias && *live {
            return Err(PersistenceError::AliasCollision {
                deployment: migration.to.clone(),
                alias_name: alias_name.clone(),
                alias_value: alias_value.clone(),
            });
        }
        outcome.alias_rows += inserted.rows_affected();
    }

    for (subject_name, blind_value, live, created_at) in &subjects {
        sqlx::query(
            "INSERT INTO subject_index (deployment_id, instance_id, subject_name, blind_value, \
             live, created_at) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(migration.to.as_str())
        .bind(migration.instance_id)
        .bind(subject_name)
        .bind(blind_value)
        .bind(live)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("migration write subject rows"))?;
        outcome.subject_rows += 1;
    }

    for (seq, at, event_type, node_id, diagnostic_code, diagnostic_json, payload_json) in &journal {
        let row = AuditEventRow {
            deployment: migration.to.clone(),
            instance_id: Some(migration.instance_id),
            seq: *seq,
            at: *at,
            event_type: event_type.clone(),
            // Verbatim: an audit row names the node the move HAPPENED at, under the graph that was
            // live then. Remapping it would rewrite history rather than relocate it.
            node_id: node_id.clone(),
            diagnostic_code: diagnostic_code.clone(),
            diagnostic_json: diagnostic_json.clone(),
            payload_json: payload_json.clone(),
        };
        PgAuditEventStore::insert_in(&mut tx, &row).await?;
        outcome.audit_rows += 1;
    }

    if let Some(event) = &migration.audit {
        PgAuditEventStore::insert_in(&mut tx, event).await?;
    }

    tx.commit()
        .await
        .map_err(PersistenceError::db("instance migration commit"))?;
    Ok(outcome)
}
