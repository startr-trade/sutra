//! The durable suspend→resume bridge — `sutra_channels::InstanceBridge` implemented over
//! `sutra-persistence` (snapshot v2 codec + the strict transactional step primitive), plus the
//! PG-backed inbox-dedup hook. Awaited on the channel-engine lane's single actor task
//! (execution scale-out §3(a), Phase 3): the store calls are async end to end — no
//! captured runtime handle, no `block_on`, and therefore no
//! `Handle::block_on`-inside-runtime panic class. The lane awaits each step to completion
//! before its next dequeue, so the quiescent-point ordering (commit happens-before reply
//! happens-before next request) is exactly what the synchronous form provided.
//!
//! Two operations:
//! - park  = the dispatcher's stateful branch (encode the suspended snapshot
//!   + persist the instance + record the wait state) — here ONE transaction.
//! - relay = correlate by key → resume the instance
//!   (claim → load → peek → resume → delete-or-repersist) — terminal/re-park each ONE
//!   transaction.
//!
//! Instance OWNERSHIP lives here too: this bridge claims an instance under the process's
//! [`replica_id`] before either resume path rehydrates it, and hands the claim back inside
//! the step transaction (`commit_repark`) or as part of the terminal write
//! (`commit_complete`). A fresh `commit_park` neither claims nor releases — an instance
//! nothing can name yet needs no owner. Two replicas can therefore no longer rehydrate and
//! advance the same instance; a claim stranded by a crash is cleared by
//! [`crate::sweeper::StuckInstanceScanner`].
//!
//! **Terminal retention (P1-2).** `commit_complete` no longer deletes the instance row: it
//! re-stamps the STORED snapshot bytes to `COMPLETED` and stamps `terminal_at`, in the same single
//! transaction that resolves the waits, retires the aliases and enqueues the outbox. A finished
//! instance therefore stays queryable (`GET /sutra/instances/{id}`) for
//! `sutra.instance.retention` (default `P7D`) instead of 404-ing the instant it ends, and
//! [`crate::sweeper::TerminalRetentionSweeper`] purges it once the window elapses. The explicit
//! value `PT0S` restores the delete.
//!
//! **What is still NOT recorded: the sync path.** An instance that runs start→end without ever
//! parking (`commit_emissions`, below) has no row to retain — it never had one. Persisting one at
//! completion would mean inventing a durable record for an instance that was, by construction,
//! never durable: no park, no claim, no wait rows, nothing to re-stamp, and a write on the hot
//! non-persistent path that today costs zero. Recording sync-path executions is a genuinely
//! different feature (an execution LOG, keyed off the audit journal rather than the recovery
//! substrate) and is deliberately out of P1-2's scope.

use std::sync::Arc;

use sqlx::PgPool;
use sutra_crypto::{Aes256GcmCipher, CipherError, KeyProvider};
use time::OffsetDateTime;
use tracing::{info, warn};
use uuid::Uuid;

use sutra_channels::bridge::{
    replica_id, AliasRecord, InstanceBridge, InstanceClaimOutcome, OutboxEmission,
    SuspendedInstance, TimerWaitRecord,
};
use sutra_channels::codes as channel_codes;
use sutra_channels::diag::Diagnostic;
use sutra_channels::stores::InboxStore as ChannelInboxStore;
use sutra_channels::stores::{InboundIncident, IncidentSink};
use sutra_persistence::scope::begin_deployment_tx;
use sutra_persistence::snapshot::{InstanceSnapshot, RawSnapshot, SnapshotCrypto};

use crate::snapshot_values;
use sutra_persistence::step::{
    commit_step_with_timers_releasing_retiring_gated, CommitGate as _, StepAlias, StepSubject,
    StepTimerWait, StepWait, StepWrite,
};
use sutra_persistence::stores::{
    DeadLetterRow, InboxSeenState, InboxStore, InstanceStore, OutboxEntry, PgDeadLetterStore,
    PgInboxStore, PgInflightChainStore, PgInstanceStore, PgOutboxStore,
    ReplyMode as PersistReplyMode,
};
use sutra_persistence::{DeploymentId as PersistDeploymentId, PersistenceError};

/// The default terminal-instance retention window (`sutra.instance.retention`) — `P7D`.
///
/// Seven days is the "an operator investigating Monday's incident on Friday still finds the
/// instance" window: long enough to answer the question the feature exists for, short enough that
/// the recovery table does not grow without bound on a busy deployment. Operators who need a
/// longer legal-hold horizon keep it in the audit journal (which this window never purges), not in
/// the instance table.
pub const DEFAULT_INSTANCE_RETENTION: std::time::Duration =
    std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// How long the inbox dedup record of a delivery that has already run is retried while the
/// database is unreachable, before the failure is logged and the delivery acknowledged without it.
/// A restart breaks every pooled connection at once, and a server coming back takes seconds.
const SETTLE_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Run a step's commit, and run it again while it fails only because the database is unreachable,
/// for at most [`SETTLE_RETRY_BUDGET`]. Every caller's attempt is one transaction built from the
/// same inputs, so a retry writes exactly what the failed attempt would have.
///
/// The step has already run, and a data task's autocommitted writes cannot be undone, so giving up
/// at once would make the delivery come again and run those writes twice. A retry after a commit
/// whose answer was lost finds the step's inbox dedup record already there
/// (`SUTRA.INBOUND.DUPLICATE_CONCURRENT`), which a redelivering sender treats as done.
async fn commit_riding_out_outage<T, F, Fut>(what: &str, attempt: F) -> Result<T, PersistenceError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, PersistenceError>>,
{
    let deadline = std::time::Instant::now() + SETTLE_RETRY_BUDGET;
    let mut pause = std::time::Duration::from_millis(50);
    loop {
        match attempt().await {
            Err(e) if e.is_transient() && std::time::Instant::now() + pause < deadline => {
                warn!(error = %e, "{what} could not reach the database; retrying");
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(std::time::Duration::from_secs(2));
            }
            other => return other,
        }
    }
}

/// Persistence-backed [`InstanceBridge`] + channel [`InboxStore`].
///
/// `Sync` deliberately (no interior single-thread state): the [`IncidentSink`] seam's
/// futures must be `Send` (the outbox dispatcher records through the same trait from a
/// spawned runtime task), which requires `&self` to cross threads — hence the
/// `Send + Sync` bound on the key provider below.
pub struct PersistenceBridge {
    pool: PgPool,
    instances: PgInstanceStore,
    inbox: PgInboxStore,
    /// The tenant-DEK provider for encryption at rest. `None` ⇒ encryption disabled
    /// (`sutra.crypto.master-key` unset): snapshots persist plaintext v2 exactly as before.
    key_provider: Option<Arc<dyn KeyProvider + Send + Sync>>,
    /// This lane's instance-ownership identity: `sutra_channels::bridge::replica_id`
    /// (`<host>-<pid>-<8 hex>`, resolved once per process), shard-suffixed to
    /// `…-s<shard>` by [`Self::with_shard_owner`] on the engine path. Every claim is
    /// taken under it and every release is scoped to it, so this bridge can only ever
    /// hand back claims it holds.
    claim_owner: String,
    /// How long a FINISHED instance's row is kept (`sutra.instance.retention`, default `P7D`).
    /// Read by [`InstanceBridge::commit_complete`] as a single boolean decision: non-zero ⇒
    /// retain-and-re-stamp; `PT0S` ⇒ the pre-P1-2 immediate delete. The window itself is enforced
    /// by [`crate::sweeper::TerminalRetentionSweeper`], not here — this bridge only decides whether
    /// a terminal row is born at all.
    retention: std::time::Duration,
    /// The enqueue → dispatcher link (0.6.5): after a commit that enqueued outbox rows the
    /// dispatcher is woken, and rows addressed `local://` are handed to it directly. `None`
    /// (tests, tools) leaves delivery to the dispatcher's own claim.
    outbox_signal: Option<sutra_channels::OutboxSignal>,
    /// Inbox dedup keys recorded by `record_seen` and not yet written: the next step commit for
    /// the same deployment writes them in its own transaction (so the record exists iff the
    /// step's effects do); `settle` writes or drops what no step consumed.
    pending_seen: std::sync::Mutex<Vec<sutra_channels::PendingDedup>>,
    /// The durable audit spool. When set, every step commit first waits for the spool's group
    /// sync ([`Self::audit_barrier`]), so a committed step's audit events are on disk; a spool
    /// that cannot sync fails the commit, and the delivery is retried, instead of losing audit.
    audit: Option<sutra_channels::AuditSpoolSender>,
    /// Fault injection (tests and the `test-hooks` feature only, never a shipped build): when
    /// set, the next step commit stops right after its audit sync, as a process killed between
    /// the spool sync and the database commit would.
    #[cfg(any(test, feature = "test-hooks"))]
    crash_after_audit_sync: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Fault injection: when set, the next successful commit writes no outcome marker, as a
    /// process killed right after its `COMMIT` would.
    #[cfg(any(test, feature = "test-hooks"))]
    crash_after_commit: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The lane's audit attempt tracker (shared with its audit listener): every step commit
    /// prepares and settles the attempts open on the lane.
    attempts: Option<sutra_channels::AuditAttemptTracker>,
    /// Where attempts go whose commit returned an error: the resolver decides them from their
    /// transaction's status.
    attempt_resolver: Option<crate::audit_recovery::ResolverTx>,
}

/// The audit side of one step commit, run inside the step transaction right before `COMMIT`:
/// record the transaction id for the lane's open attempts (`PREPARED` markers), then wait for
/// the spool's sync. A failure is kept so the caller reports it with its own code.
struct AuditGate<'a> {
    bridge: &'a PersistenceBridge,
    failure: std::sync::Mutex<Option<Diagnostic>>,
}

impl sutra_persistence::step::CommitGate for AuditGate<'_> {
    fn before_commit<'a>(
        &'a self,
        conn: &'a mut sqlx::PgConnection,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = sutra_persistence::Result<()>> + Send + 'a>,
    > {
        Box::pin(async move {
            if let (Some(spool), Some(tracker)) = (&self.bridge.audit, &self.bridge.attempts) {
                if tracker.has_open() {
                    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
                        .fetch_one(&mut *conn)
                        .await
                        .map_err(PersistenceError::db("audit attempt transaction id"))?;
                    tracker.prepare(spool, &xid);
                }
            }
            match self.bridge.audit_barrier().await {
                Ok(()) => Ok(()),
                Err(d) => {
                    let message = d.message.clone();
                    *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(d);
                    Err(PersistenceError::InvalidArgument(message))
                }
            }
        })
    }
}

/// The diagnostic a step commit stopped by the `test-hooks` kill point returns.
#[cfg(any(test, feature = "test-hooks"))]
pub const TEST_CRASH_AFTER_AUDIT_SYNC: &str = "SUTRA.TEST.CRASH_AFTER_AUDIT_SYNC";

/// The step commit could not be made because the audit spool did not sync in time.
pub const AUDIT_SPOOL_SYNC_FAILED: &str = sutra_channels::audit_spool::AUDIT_SPOOL_SYNC_FAILED;

/// Wait for `spool`'s group sync of everything appended before this call (nothing to wait for
/// without a spool). A spool that cannot sync within its `sync-timeout` is
/// [`AUDIT_SPOOL_SYNC_FAILED`].
pub(crate) async fn audit_barrier(
    spool: Option<&sutra_channels::AuditSpoolSender>,
) -> Result<(), Diagnostic> {
    let Some(spool) = spool else {
        return Ok(());
    };
    spool.barrier().await.map_err(|e| {
        Diagnostic::error(
            AUDIT_SPOOL_SYNC_FAILED,
            format!(
                "the step's audit events could not be made durable, so it does not commit: {e}"
            ),
        )
    })
}

/// Rows a step commits that the in-process dispatcher can take straight from the commit:
/// `local://` deliveries never leave the engine, so this process is where they go.
fn is_local_destination(destination: &str) -> bool {
    destination.starts_with("local://")
}

impl PersistenceBridge {
    pub fn new(pool: PgPool) -> PersistenceBridge {
        Self::with_key_provider(pool, None)
    }

    /// As [`new`](Self::new) but with an encryption-at-rest key provider (built from
    /// `sutra.crypto.master-key` at assembly). `None` keeps the plaintext-v2 behaviour.
    pub fn with_key_provider(
        pool: PgPool,
        key_provider: Option<Arc<dyn KeyProvider + Send + Sync>>,
    ) -> PersistenceBridge {
        PersistenceBridge {
            instances: PgInstanceStore::new(pool.clone()),
            inbox: PgInboxStore::new(pool.clone()),
            pool,
            key_provider,
            claim_owner: replica_id().to_string(),
            retention: DEFAULT_INSTANCE_RETENTION,
            outbox_signal: None,
            pending_seen: std::sync::Mutex::new(Vec::new()),
            audit: None,
            #[cfg(any(test, feature = "test-hooks"))]
            crash_after_audit_sync: None,
            #[cfg(any(test, feature = "test-hooks"))]
            crash_after_commit: None,
            attempts: None,
            attempt_resolver: None,
        }
    }

    /// Take the recorded-but-unwritten dedup keys of `dep`, for the step commit about to run.
    fn pending_for(&self, dep: &PersistDeploymentId) -> Vec<(String, String)> {
        let mut pending = self.pending_seen.lock().unwrap_or_else(|e| e.into_inner());
        let (mine, rest): (Vec<_>, Vec<_>) = pending
            .drain(..)
            .partition(|p| p.deployment.value() == dep.as_str());
        *pending = rest;
        mine.into_iter().map(|p| (p.channel, p.event_id)).collect()
    }

    /// Wire the durable audit spool: every step commit waits for its sync first.
    #[must_use]
    pub fn with_audit_spool(
        mut self,
        spool: sutra_channels::AuditSpoolSender,
    ) -> PersistenceBridge {
        self.audit = Some(spool);
        self
    }

    /// The audit barrier every step commit passes first (see [`audit_barrier`]).
    async fn audit_barrier(&self) -> Result<(), Diagnostic> {
        audit_barrier(self.audit.as_ref()).await?;
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(crash) = &self.crash_after_audit_sync {
            if crash.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(Diagnostic::error(
                    TEST_CRASH_AFTER_AUDIT_SYNC,
                    "test hook: stopped between the audit sync and the step commit".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Wire the lane's audit attempt tracker and the resolver for unknown commit outcomes.
    #[must_use]
    pub(crate) fn with_audit_attempts(
        mut self,
        tracker: sutra_channels::AuditAttemptTracker,
        resolver: Option<crate::audit_recovery::ResolverTx>,
    ) -> PersistenceBridge {
        self.attempts = Some(tracker);
        self.attempt_resolver = resolver;
        self
    }

    fn audit_gate(&self) -> AuditGate<'_> {
        AuditGate {
            bridge: self,
            failure: std::sync::Mutex::new(None),
        }
    }

    /// After a step commit: settle the lane's open attempts. A commit that succeeded marks
    /// them `COMMITTED`; one whose audit sync failed (so `COMMIT` was never sent) `ABORTED`;
    /// any other error leaves the outcome unknown, for the resolver. Returns the gate's own
    /// failure, which the caller reports instead of the generic persistence error.
    fn settle_attempts<T>(
        &self,
        gate: AuditGate<'_>,
        outcome: &Result<T, PersistenceError>,
    ) -> Option<Diagnostic> {
        let failure = gate.failure.into_inner().unwrap_or_else(|e| e.into_inner());
        #[cfg(any(test, feature = "test-hooks"))]
        {
            if failure
                .as_ref()
                .is_some_and(|d| d.code == TEST_CRASH_AFTER_AUDIT_SYNC)
            {
                return failure;
            }
            if outcome.is_ok() {
                if let Some(crash) = &self.crash_after_commit {
                    if crash.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        return failure;
                    }
                }
            }
        }
        if let (Some(spool), Some(tracker)) = (&self.audit, &self.attempts) {
            match (outcome, &failure) {
                (Ok(_), _) => {
                    tracker.settle(spool, true);
                }
                (Err(_), Some(_)) => {
                    tracker.settle(spool, false);
                }
                (Err(_), None) => {
                    let unknown = tracker.take_unknown();
                    if !unknown.is_empty() {
                        match &self.attempt_resolver {
                            Some(resolver) => {
                                let _ = resolver.send(unknown);
                            }
                            // No resolver (tests): audit is never dropped on doubt.
                            None => {
                                for attempt in unknown {
                                    spool.append(attempt.marker(true));
                                }
                            }
                        }
                    }
                }
            }
        }
        failure
    }

    /// Arm the `test-hooks` kill point right after a commit: while `crash` is true, the next
    /// successful commit writes no outcome marker (and clears it).
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn with_crash_after_commit(
        mut self,
        crash: Arc<std::sync::atomic::AtomicBool>,
    ) -> PersistenceBridge {
        self.crash_after_commit = Some(crash);
        self
    }

    /// Arm the `test-hooks` kill point: while `crash` is true, the next step commit stops right
    /// after its audit sync (and clears it).
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn with_crash_after_audit_sync(
        mut self,
        crash: Arc<std::sync::atomic::AtomicBool>,
    ) -> PersistenceBridge {
        self.crash_after_audit_sync = Some(crash);
        self
    }

    /// Wire the runtime's outbox signal (wake on enqueue + the local hand-off).
    #[must_use]
    pub fn with_outbox_signal(mut self, signal: sutra_channels::OutboxSignal) -> PersistenceBridge {
        self.outbox_signal = Some(signal);
        self
    }

    /// Before a commit that inserts `entries`: reserve the local ones, so no claim can hand them
    /// to a dispatcher worker between the commit and the hand-off, and make them due only after
    /// the hand-off grace ([`sutra_channels::outbox_dispatch::HANDOFF_GRACE`]): the hand-off
    /// delivers them now, and the due time matters only if this process dies first. Returns the
    /// reserved ids.
    fn reserve_local(&self, entries: &mut [OutboxEntry]) -> Vec<String> {
        let Some(signal) = &self.outbox_signal else {
            return Vec::new();
        };
        if !signal.accepts_hand_off() {
            return Vec::new();
        }
        let grace = time::Duration::try_from(sutra_channels::outbox_dispatch::HANDOFF_GRACE)
            .unwrap_or(time::Duration::ZERO);
        let mut ids = Vec::new();
        for entry in entries.iter_mut() {
            if is_local_destination(&entry.destination) {
                entry.next_attempt_at += grace;
                ids.push(entry.entry_id.to_string());
            }
        }
        signal.reserve(&ids);
        ids
    }

    /// After the commit: on success hand the reserved local rows to the dispatcher and wake it
    /// for the rest; on failure release the reservation (nothing was inserted). Reserved rows the
    /// hand-off can no longer take are made due now (see [`crate::outbox::hand_off_or_make_due`]).
    async fn after_enqueue(
        &self,
        deployment: &sutra_executor::DeploymentId,
        entries: &[OutboxEntry],
        reserved: Vec<String>,
        committed: bool,
    ) {
        let Some(signal) = &self.outbox_signal else {
            return;
        };
        if !committed {
            signal.release(&reserved);
            return;
        }
        if entries.is_empty() {
            return;
        }
        let handed: Vec<_> = entries
            .iter()
            .filter(|e| reserved.contains(&e.entry_id.to_string()))
            .cloned()
            .collect();
        let rest = entries.len() - handed.len();
        if rest > 0 {
            signal.wake();
        }
        if !handed.is_empty() {
            crate::outbox::hand_off_or_make_due(signal, &self.pool, deployment, handed).await;
        }
    }

    /// Set the terminal-instance retention window (`sutra.instance.retention`). `PT0S` restores
    /// the pre-P1-2 behaviour: a finished instance's row is DELETED in the terminal transaction
    /// and there is no history to query.
    #[must_use]
    pub fn with_retention(mut self, retention: std::time::Duration) -> PersistenceBridge {
        self.retention = retention;
        self
    }

    /// Shard-scope the claim owner: `<replica_id>-s<shard>` (execution scale-out §4).
    ///
    /// The claim CAS deliberately grants a re-claim by the SAME owner, on the ground that
    /// one owner is one serial lane. With N in-process shards under one per-process owner
    /// id, a re-claim from a sibling shard would succeed re-entrantly — a silent
    /// double-resume. Suffixing the shard index restores the invariant in its honest
    /// form: *same owner ⇒ same shard ⇒ already serialised*. The store and the sweeper
    /// treat the owner as an opaque string (VARCHAR(128); the truncated-host replica id
    /// leaves room for the suffix), and a mis-routed resume degrades to the visible
    /// `CLAIM_HELD` bounce cross-replica contention already takes. The engine assembly
    /// applies this unconditionally — at `shard-count = 1` the owner is `…-s0`, changed
    /// in form only (per-process-unique either way).
    #[must_use]
    pub fn with_shard_owner(mut self, shard_index: u32) -> PersistenceBridge {
        self.claim_owner = format!("{}-s{shard_index}", replica_id());
        self
    }

    /// The replica identity this bridge claims instances under (diagnostics / tests).
    pub fn claim_owner(&self) -> &str {
        &self.claim_owner
    }

    /// The configured terminal-instance retention window (diagnostics / tests).
    pub fn retention(&self) -> std::time::Duration {
        self.retention
    }

    /// Encode a parked snapshot, encrypting its `encrypt_names` values at rest when a key provider
    /// is configured and the instance carries a keyId + a non-empty encrypt-set.
    /// Otherwise a plaintext snapshot. Fails closed: a cipher/key error aborts the park.
    fn encode_snapshot(
        &self,
        snapshot: &SuspendedInstance,
        instance_id: &str,
    ) -> Result<Vec<u8>, Diagnostic> {
        let snap = InstanceSnapshot::of_suspended(
            snapshot.process_id.clone(),
            snapshot.deployment_id.clone(),
            snapshot.completed_nodes.clone(),
            std::collections::BTreeMap::new(),
            snapshot.waiting_nodes.clone(),
            snapshot.start_node.clone(),
            snapshot.audit_seq,
        )
        .with_variables(snapshot_values::to_snapshot_map(
            snapshot.variables.iter().map(|(name, value)| (name, value)),
        ))
        .with_sensitive(snapshot.sensitive.clone())
        .with_coverage(snapshot.coverage.clone())
        .with_retry_attempts(snapshot.retry_attempts.clone())
        .with_retry_backoff(snapshot.retry_backoff.clone());

        match &self.key_provider {
            Some(provider) if !snapshot.encrypt_names.is_empty() && !snapshot.key_id.is_empty() => {
                let key = provider.data_key(&snapshot.key_id).map_err(crypto_diag)?;
                let cipher = Aes256GcmCipher::new(&key);
                let ctx = SnapshotCrypto::new(&cipher, &snapshot.key_id, instance_id);
                let names = snapshot.encrypt_names.iter().cloned().collect();
                snap.write_encrypted(Some(&ctx), &names)
                    .map_err(crypto_diag)
            }
            _ => Ok(snap.write()),
        }
    }

    /// Blind-index this instance's `@subjectKey` values: HMAC each raw value under the tenant's
    /// migration-stable index key, producing the `subject_index` rows the step writes
    /// atomically with the snapshot. Empty when no key provider is configured, the instance carries
    /// no keyId, or it has no subject keys — so GDPR indexing is on exactly when encryption is.
    fn blind_subjects(&self, snapshot: &SuspendedInstance) -> Result<Vec<StepSubject>, Diagnostic> {
        match &self.key_provider {
            Some(provider) if !snapshot.subjects.is_empty() && !snapshot.key_id.is_empty() => {
                let indexer = provider
                    .blind_index_key(&snapshot.key_id)
                    .map_err(crypto_diag)?;
                Ok(snapshot
                    .subjects
                    .iter()
                    .map(|(name, value)| StepSubject {
                        subject_name: name.clone(),
                        blind_value: indexer.blind(value),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }
}

/// A cipher/key failure surfaced as a fatal diagnostic (fail-closed — never park/resume in the
/// clear on a crypto error).
/// Decode a persisted snapshot, decrypting its `sutra.enc.` values when the snapshot carries a
/// keyId (read from the snapshot itself — no external tenant lookup). Fails closed on an
/// encrypted snapshot with no key provider configured. Shared by resume (the bridge) and by the
/// read-only inspection endpoints, so an encrypted instance reads the same way on both paths.
pub(crate) fn decode_persisted_snapshot(
    key_provider: Option<&(dyn KeyProvider + Send + Sync)>,
    bytes: &[u8],
    instance_id: &str,
) -> Result<InstanceSnapshot, Diagnostic> {
    // Parsed once: the key id picks the cipher, and the same parse is then decoded.
    let raw = RawSnapshot::parse(bytes).map_err(decode_diag)?;
    decode_raw_snapshot(key_provider, &raw, instance_id)
}

/// [`decode_persisted_snapshot`] over an already-parsed snapshot.
pub(crate) fn decode_raw_snapshot(
    key_provider: Option<&(dyn KeyProvider + Send + Sync)>,
    raw: &RawSnapshot,
    instance_id: &str,
) -> Result<InstanceSnapshot, Diagnostic> {
    match (raw.key_id(), key_provider) {
        (Some(key_id), Some(provider)) => {
            let key = provider.data_key(key_id).map_err(crypto_diag)?;
            let cipher = Aes256GcmCipher::new(&key);
            let ctx = SnapshotCrypto::new(&cipher, key_id, instance_id);
            InstanceSnapshot::from_raw(raw, Some(&ctx)).map_err(decode_diag)
        }
        (Some(_), None) => Err(Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!(
                "instance {instance_id} was persisted encrypted (sutra.keyId present) but no \
                 encryption key provider is configured (sutra.crypto.master-key) — cannot \
                 decrypt; refusing to resume in the clear"
            ),
        )),
        (None, _) => InstanceSnapshot::from_raw(raw, None).map_err(decode_diag),
    }
}

fn crypto_diag(e: CipherError) -> Diagnostic {
    Diagnostic::error(
        channel_codes::RUNTIME_UNEXPECTED,
        format!("snapshot encryption/decryption failed (fail-closed): {e}"),
    )
}

fn decode_diag(e: String) -> Diagnostic {
    Diagnostic::error(
        channel_codes::RUNTIME_UNEXPECTED,
        format!("failed to decode persisted instance snapshot: {e}"),
    )
}

fn persist_dep(
    deployment: &sutra_executor::DeploymentId,
) -> Result<PersistDeploymentId, Diagnostic> {
    PersistDeploymentId::new(deployment.value()).map_err(|e| {
        Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!("deployment id failed persistence-form validation: {e}"),
        )
    })
}

fn parse_instance(instance_id: &str) -> Result<Uuid, Diagnostic> {
    Uuid::parse_str(instance_id).map_err(|e| {
        Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!("instance id '{instance_id}' is not a UUID: {e}"),
        )
    })
}

fn persistence_diag(context: &str, e: &PersistenceError) -> Diagnostic {
    // The one persistence error with channel-level semantics: a unique-live alias
    // collision inside the park step ⇒ the whole step rolled back ⇒ the canonical
    // ALIAS_CONFLICT_REJECT the transport maps to a client error.
    if let PersistenceError::AliasCollision {
        alias_name,
        alias_value,
        ..
    } = e
    {
        return Diagnostic::error(
            channel_codes::INBOUND_ALIAS_CONFLICT_REJECT,
            format!(
                "<q:alias {alias_name}> = '{alias_value}' already bound to a live instance \
                 (unique-live index); the step was rolled back and the arrival rejected"
            ),
        );
    }
    if let PersistenceError::InboxDuplicate {
        channel, event_id, ..
    } = e
    {
        return Diagnostic::error(
            channel_codes::INBOUND_DUPLICATE_CONCURRENT,
            format!(
                "event '{event_id}' on channel '{channel}' was processed by a concurrent delivery \
                 of the same event; this delivery's step was rolled back"
            ),
        );
    }
    let diagnostic =
        Diagnostic::error(channel_codes::RUNTIME_UNEXPECTED, format!("{context}: {e}"));
    if e.is_transient() {
        // The database was unreachable: nothing about this delivery is wrong, so a sender that
        // can retry (the in-process local sink) offers it again instead of poisoning it.
        diagnostic.with_attribute(sutra_channels::TRANSIENT_ATTR, "true")
    } else {
        diagnostic
    }
}

/// Convert a step's collected emissions to outbox rows: row identity is
/// minted here; `created_at`/`next_attempt_at` = now (immediately due); the emission's
/// `outbox_key` (minted at collection) is carried through unchanged.
fn outbox_entries(
    dep: &PersistDeploymentId,
    emissions: &[OutboxEmission],
) -> Result<Vec<OutboxEntry>, Diagnostic> {
    let now = time::OffsetDateTime::now_utc();
    emissions
        .iter()
        .map(|emission| {
            let instance_id = parse_instance(&emission.instance_id)?;
            Ok(OutboxEntry {
                deployment: dep.clone(),
                entry_id: Uuid::new_v4(),
                instance_id,
                // The mask rides straight into the persisted row: both `Emission.body` and
                // `OutboxEntry.body` are `Sensitive`, so this is a plain carry — the payload stays
                // wrapped through persistence, unwrapped only at the wire boundary (the dispatcher's
                // `to_claimed` hand-off, encrypted separately at rest for a sensitive channel).
                body: emission.body.clone(),
                content_type: emission.content_type.clone(),
                destination: emission.destination.clone(),
                // Author-declared `<q:header>` attributes ride the persisted
                // `headers_json` column → `ClaimedOutboxRow.headers` → `OutboundMessage.headers`,
                // the same seam every sink already puts on the wire.
                headers: emission.headers.clone(),
                required: emission.required,
                mode: match emission.mode {
                    sutra_bpmn::qbindings::ReplyMode::Native => PersistReplyMode::Native,
                    sutra_bpmn::qbindings::ReplyMode::CloudeventBinary => {
                        PersistReplyMode::CloudEventBinary
                    }
                    sutra_bpmn::qbindings::ReplyMode::CloudeventStructured => {
                        PersistReplyMode::CloudEventStructured
                    }
                    sutra_bpmn::qbindings::ReplyMode::MatchInbound => {
                        PersistReplyMode::MatchInbound
                    }
                },
                outbox_key: emission.outbox_key.clone(),
                cloud_event_json: emission.cloud_event_json.clone(),
                auth_ref_json: emission.auth_ref_json.clone(),
                labels: emission.labels.clone(),
                created_at: now,
                next_attempt_at: now,
                attempt_count: 0,
                last_diagnostic_json: None,
                traceparent: emission.traceparent.clone(),
                // The emitting node (V606) — what lets a channel-call retry withdraw the dead
                // attempt's rows and verify a poison wake by (instance, node).
                node_id: Some(emission.node_id.clone()),
            })
        })
        .collect()
}

/// [`TimerWaitRecord`]s (RFC 3339 due-at) → step timer rows.
fn step_timer_waits(
    timer_waits: &[TimerWaitRecord],
    process_id: &str,
) -> Result<Vec<StepTimerWait>, Diagnostic> {
    timer_waits
        .iter()
        .map(|t| {
            let due_at =
                OffsetDateTime::parse(&t.due_at, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| {
                        Diagnostic::error(
                            channel_codes::RUNTIME_UNEXPECTED,
                            format!(
                                "timer wait '{}' carries unparseable due-at '{}': {e}",
                                t.node_id, t.due_at
                            ),
                        )
                    })?;
            Ok(StepTimerWait {
                node_id: t.node_id.clone(),
                process_id: process_id.to_string(),
                due_at,
            })
        })
        .collect()
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for PersistenceBridge {
    /// `commit_complete` deletes the instance's `channel_instance` row in its own transaction.
    fn terminal_commit_releases_admission(&self) -> bool {
        true
    }

    async fn commit_park(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        let mut step = StepWrite {
            deployment: dep.clone(),
            instance_id: instance,
            snapshot: self.encode_snapshot(snapshot, instance_id)?,
            waits: snapshot
                .waiting_nodes
                .iter()
                .map(|node| StepWait {
                    node_id: node.clone(),
                    process_id: snapshot.process_id.clone(),
                    correlation_key: None,
                })
                .collect(),
            resolved_waits: Vec::new(),
            aliases: aliases
                .iter()
                .map(|a| StepAlias {
                    alias_name: a.name.clone(),
                    alias_value: a.value.clone(),
                    unique: a.unique,
                })
                .collect(),
            subjects: self.blind_subjects(snapshot)?,
            outbox: outbox_entries(&dep, emissions)?,
            // Structurally empty on an initial park (a call's first failure needs a later
            // timeout/poison), but derived uniformly: the marked set IS the withdrawal set.
            withdrawn_call_nodes: snapshot.retry_backoff.keys().cloned().collect(),
            inbox_seen: self.pending_for(&dep),
        };
        let timers = step_timer_waits(timer_waits, &snapshot.process_id)?;
        let reserved = self.reserve_local(&mut step.outbox);
        // NO claim, and nothing to release: a park is the FIRST persist of a NEW instance
        // (the dispatcher's park arm is the sole caller, with an id minted microseconds ago
        // on this actor thread). Until this transaction commits there is no row, no alias
        // and no timer pointing at the instance, so no other replica can name it — a fresh
        // instance is unreachable, which is a stronger guarantee than a claim. Ownership
        // only becomes meaningful from the first RESUME onwards, which is where the claim
        // is taken and `commit_repark` hands it back.
        let gate = self.audit_gate();
        let committed = commit_riding_out_outage("park step commit", || {
            commit_step_with_timers_releasing_retiring_gated(
                &self.pool,
                &step,
                &timers,
                None,
                &[],
                &gate,
            )
        })
        .await;
        let gate_failure = self.settle_attempts(gate, &committed);
        self.after_enqueue(deployment, &step.outbox, reserved, committed.is_ok())
            .await;
        if let (Err(_), Some(d)) = (&committed, gate_failure) {
            return Err(d);
        }
        committed.map_err(|e| persistence_diag("park step commit failed", &e))?;
        info!(
            instance_id,
            deployment = deployment.value(),
            waiting = ?snapshot.waiting_nodes,
            timers = timer_waits.len(),
            emissions = emissions.len(),
            "instance parked (snapshot + waits + timers + aliases + outbox in one step)"
        );
        Ok(())
    }

    async fn load(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        let row = self
            .instances
            .load(&dep, instance)
            .await
            .map_err(|e| persistence_diag("instance load failed", &e))?;
        let Some(row) = row else {
            return Ok(None);
        };
        // Decrypt at-rest values when the snapshot self-describes a keyId (fail-closed on a missing
        // provider). The keyId itself is carried forward so a re-park re-encrypts under the same
        // migration-stable anchor even though the resume path has no channel binding.
        let raw = RawSnapshot::parse(&row.serialised).map_err(decode_diag)?;
        let key_id = raw.key_id().unwrap_or_default().to_owned();
        let decoded = decode_raw_snapshot(self.key_provider.as_deref(), &raw, instance_id)?;
        Ok(Some(SuspendedInstance {
            process_id: decoded.process_id().to_string(),
            deployment_id: decoded.deployment_id().to_string(),
            status: decoded.status().to_string(),
            suspended: decoded.is_suspended(),
            completed_nodes: decoded.completed_nodes().to_vec(),
            variables: decoded
                .variables()
                .iter()
                .map(|(k, v)| (k.clone(), snapshot_values::to_feel(v)))
                .collect(),
            sensitive: decoded.sensitive().to_vec(),
            waiting_nodes: decoded.waiting_nodes().to_vec(),
            start_node: decoded.start_node().to_string(),
            coverage: decoded.coverage().clone(),
            retry_attempts: decoded.retry_attempts().clone(),
            retry_backoff: decoded.retry_backoff().clone(),
            audit_seq: decoded.audit_seq(),
            key_id,
            // Recomputed fresh at each re-park (persisted_variables) — not needed on load.
            encrypt_names: Vec::new(),
            subjects: Vec::new(),
        }))
    }

    async fn find_live_alias(
        &self,
        deployment: &sutra_executor::DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let store = sutra_persistence::stores::PgAliasStore::new(self.pool.clone());
        let owner = sutra_persistence::stores::AliasStore::find_live(&store, &dep, name, value)
            .await
            .map_err(|e| persistence_diag("alias findLive failed", &e))?;
        Ok(owner.map(|u| u.to_string()))
    }

    async fn alias_ended(
        &self,
        deployment: &sutra_executor::DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<bool, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let store = sutra_persistence::stores::PgAliasStore::new(self.pool.clone());
        store
            .owned_only_by_ended(&dep, name, value)
            .await
            .map_err(|e| persistence_diag("alias ended lookup failed", &e))
    }

    async fn commit_complete(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        let mut entries = outbox_entries(&dep, emissions)?;
        let reserved = self.reserve_local(&mut entries);
        let retain = !self.retention.is_zero();
        // Terminal step (wait→end): retain-or-delete the row + resolve every wait point +
        // retire every live alias + enqueue the step's emissions — ONE deployment-scoped
        // transaction, exactly as before. The ONLY thing P1-2 changes is what happens to the
        // instance row; the wait resolution, the alias retirement and the outbox enqueues stay
        // in the same commit-or-nothing envelope, so a completed instance still never leaves a
        // live wait behind and still never loses a send.
        // Taken once: every attempt below writes the same dedup keys.
        let pending = self.pending_for(&dep);
        let gate = self.audit_gate();
        let attempt = || async {
            let mut tx = begin_deployment_tx(&self.pool, &dep).await?;
            sutra_persistence::step::record_inbox_seen_in(&mut tx, &dep, &pending, Some(instance))
                .await?;
            let retained = if retain {
                // Row-locking read, then the SAME key-patch shape `commit_failed` uses: the
                // stored bytes are re-stamped COMPLETED and written straight back. No decode,
                // no re-encode — `sutra.enc.*` and `sutra.keyId` ride through verbatim, so
                // finishing an instance can neither need the tenant DEK nor downgrade an
                // at-rest value to plaintext. The lock is what stops a concurrent replica's
                // re-park from overwriting the terminal verdict with a fresh SUSPENDED
                // snapshot.
                match PgInstanceStore::load_for_update(&mut tx, &dep, instance).await? {
                    Some(row) => {
                        let serialised = InstanceSnapshot::mark_terminal(
                            &row.serialised,
                            sutra_persistence::snapshot::STATUS_COMPLETED,
                        )
                        .map_err(PersistenceError::InvalidArgument)?;
                        PgInstanceStore::mark_terminal_in(&mut tx, &dep, instance, &serialised)
                            .await?
                            == 1
                    }
                    // Raced with an admin cancel / GDPR erasure — there is no row left to
                    // retain. The rest of the terminal step still commits.
                    None => false,
                }
            } else {
                // `sutra.instance.retention=PT0S` — the pre-P1-2 behaviour, kept as an
                // explicit operator choice and implemented as the IMMEDIATE delete it used to
                // be, not as a retention of zero swept up later. An operator who asks for no
                // history must get no history, not a window in which it exists.
                sqlx::query(
                    "DELETE FROM instance_state WHERE deployment_id = $1 AND instance_id = $2",
                )
                .bind(dep.as_str())
                .bind(instance)
                .execute(&mut *tx)
                .await
                .map_err(PersistenceError::db("terminal instance delete"))?;
                false
            };
            sqlx::query(
                "UPDATE waiting_event SET status = 'RESOLVED', resolved_at = \
                     CURRENT_TIMESTAMP WHERE deployment_id = $1 AND instance_id = $2 AND \
                     status = 'WAITING'",
            )
            .bind(dep.as_str())
            .bind(instance)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("terminal wait resolveAll"))?;
            sqlx::query(
                "UPDATE alias_index SET live = FALSE WHERE deployment_id = $1 AND \
                     instance_id = $2 AND live = TRUE",
            )
            .bind(dep.as_str())
            .bind(instance)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("terminal alias retire"))?;
            // The per-channel admission row (`channel_instance`) goes with the terminal step, in
            // this transaction, for every instance — whether or not its channel is capped, and
            // whether or not a row exists — so the freed slot can never lag the completion.
            sutra_persistence::stores::PgChannelConcurrencyStore::record_terminal_in(
                &mut tx, &dep, instance,
            )
            .await?;
            for entry in &entries {
                PgOutboxStore::enqueue_in(&mut tx, entry).await?;
            }
            PgInflightChainStore::clear_in(&mut tx, &dep, instance).await?;
            gate.before_commit(&mut tx).await?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("terminal step commit"))?;
            Ok::<bool, PersistenceError>(retained)
        };
        let retained = commit_riding_out_outage("terminal step commit", attempt).await;
        let gate_failure = self.settle_attempts(gate, &retained);
        self.after_enqueue(deployment, &entries, reserved, retained.is_ok())
            .await;
        if let (Err(_), Some(d)) = (&retained, gate_failure) {
            return Err(d);
        }
        let retained = retained.map_err(|e| persistence_diag("terminal step failed", &e))?;
        info!(
            instance_id,
            deployment = deployment.value(),
            emissions = emissions.len(),
            retained,
            retention_s = self.retention.as_secs(),
            "resumed instance completed (row retained as COMPLETED or deleted + waits resolved + \
             aliases retired + outbox enqueued)"
        );
        Ok(())
    }

    async fn commit_repark(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        self.commit_repark_retiring(
            deployment,
            instance_id,
            snapshot,
            satisfied_wait_nodes,
            aliases,
            timer_waits,
            emissions,
            &[],
        )
        .await
    }

    async fn commit_repark_retiring(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
        retired_aliases: &[AliasRecord],
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        let mut step = StepWrite {
            deployment: dep.clone(),
            instance_id: instance,
            snapshot: self.encode_snapshot(snapshot, instance_id)?,
            waits: snapshot
                .waiting_nodes
                .iter()
                .map(|node| StepWait {
                    node_id: node.clone(),
                    process_id: snapshot.process_id.clone(),
                    correlation_key: None,
                })
                .collect(),
            resolved_waits: satisfied_wait_nodes.to_vec(),
            aliases: aliases
                .iter()
                .map(|a| StepAlias {
                    alias_name: a.name.clone(),
                    alias_value: a.value.clone(),
                    unique: a.unique,
                })
                .collect(),
            subjects: self.blind_subjects(snapshot)?,
            outbox: outbox_entries(&dep, emissions)?,
            // The channel-call retry WITHDRAWAL set: every node in a backoff window at this
            // quiescent point loses its outstanding request rows in the same commit (a
            // superseded request must neither deliver late nor poison later). Idempotent —
            // the marked set is authoritative per snapshot, and a re-drive clears its node's
            // marker BEFORE the fresh emission enqueues.
            withdrawn_call_nodes: snapshot.retry_backoff.keys().cloned().collect(),
            inbox_seen: self.pending_for(&dep),
        };
        let timers = step_timer_waits(timer_waits, &snapshot.process_id)?;
        // Re-park = the wait→wait shape of a RESUMED step: the claim taken before rehydrate
        // is handed back inside this same transaction, so the new frontier and the
        // hand-back become visible together (and a crash before the commit leaves the claim
        // for the StuckInstanceScanner rather than a half-advanced instance).
        // The waits this step resolved lose their correlation keys in the same transaction.
        let retired: Vec<(String, String)> = retired_aliases
            .iter()
            .map(|a| (a.name.clone(), a.value.clone()))
            .collect();
        let reserved = self.reserve_local(&mut step.outbox);
        let gate = self.audit_gate();
        let committed = commit_riding_out_outage("re-park step commit", || {
            commit_step_with_timers_releasing_retiring_gated(
                &self.pool,
                &step,
                &timers,
                Some(&self.claim_owner),
                &retired,
                &gate,
            )
        })
        .await;
        let gate_failure = self.settle_attempts(gate, &committed);
        self.after_enqueue(deployment, &step.outbox, reserved, committed.is_ok())
            .await;
        if let (Err(_), Some(d)) = (&committed, gate_failure) {
            return Err(d);
        }
        committed.map_err(|e| persistence_diag("re-park step commit failed", &e))?;
        info!(
            instance_id,
            deployment = deployment.value(),
            satisfied = ?satisfied_wait_nodes,
            retired_aliases = retired.len(),
            waiting = ?snapshot.waiting_nodes,
            timers = timer_waits.len(),
            emissions = emissions.len(),
            "instance re-parked at a new frontier"
        );
        Ok(())
    }

    async fn claim_instance(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        // The ownership claim commits without waiting for its WAL flush: a claim lost in a
        // crash is the crashed lane's own, which a restart voids anyway, and the step that
        // follows a granted claim commits synchronously, claim included.
        let won = self
            .instances
            .claim_flush_deferred(&dep, instance, &self.claim_owner)
            .await
            .map_err(|e| persistence_diag("instance claim failed", &e))?;
        Ok(if won {
            InstanceClaimOutcome::Granted
        } else {
            // Either a live claim stands or the row is gone — the caller re-reads to tell
            // them apart (the store's CAS cannot distinguish "no match" from "no row").
            InstanceClaimOutcome::HeldByOther
        })
    }

    async fn release_instance(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        // Owner-scoped: 0 rows means the step already released in-transaction (or the
        // terminal step deleted the row) — a no-op, never an error.
        self.instances
            .release(&dep, instance, &self.claim_owner)
            .await
            .map_err(|e| persistence_diag("instance claim release failed", &e))?;
        Ok(())
    }

    async fn commit_failed(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        // ONE deployment-scoped transaction, commit or nothing: re-read the stored snapshot,
        // key-patch it to FAILED + the cause (no decode/re-encode — `mark_failed` carries
        // `sutra.enc.*` through verbatim, so a dead instance never loses its at-rest protection),
        // UPSERT it back, and resolve every outstanding wait row so no timer refires and no relay
        // finds a live wait. Aliases stay LIVE on purpose (see the trait doc): the failed instance
        // keeps its business key so a relay correlates to it and gets INSTANCE_FAILED.
        let gate = self.audit_gate();
        let marked = async {
            let mut tx = begin_deployment_tx(&self.pool, &dep).await?;
            // Row-locking read: the marker must not race a concurrent replica re-parking the
            // same instance (which would overwrite FAILED with a fresh SUSPENDED snapshot).
            let Some(row) = PgInstanceStore::load_for_update(&mut tx, &dep, instance).await? else {
                // Raced with a delete/cancel — there is no instance left to mark.
                return Ok(false);
            };
            let serialised = InstanceSnapshot::mark_failed(&row.serialised, failure_code, detail)
                .map_err(PersistenceError::InvalidArgument)?;
            PgInstanceStore::persist_in(
                &mut tx,
                &dep,
                &sutra_persistence::stores::InstanceState {
                    instance_id: instance,
                    serialised,
                },
            )
            .await?;
            sqlx::query(
                "UPDATE waiting_event SET status = 'RESOLVED', resolved_at = \
                     CURRENT_TIMESTAMP WHERE deployment_id = $1 AND instance_id = $2 AND \
                     status = 'WAITING'",
            )
            .bind(dep.as_str())
            .bind(instance)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("failed-step wait resolveAll"))?;
            PgInflightChainStore::clear_in(&mut tx, &dep, instance).await?;
            gate.before_commit(&mut tx).await?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("failed step commit"))?;
            Ok::<bool, PersistenceError>(true)
        }
        .await;
        let gate_failure = self.settle_attempts(gate, &marked);
        if let (Err(_), Some(d)) = (&marked, gate_failure) {
            return Err(d);
        }
        let marked = marked.map_err(|e| persistence_diag("failed-state commit failed", &e))?;
        if marked {
            warn!(
                instance_id,
                deployment = deployment.value(),
                failure_code,
                "instance marked FAILED (snapshot re-stamped + waits resolved in one step); it is \
                 no longer resumable — aliases stay bound until an operator cancels it"
            );
        } else {
            warn!(
                instance_id,
                deployment = deployment.value(),
                failure_code,
                "instance vanished before its FAILED state could be recorded (cancelled or \
                 completed concurrently) — nothing marked"
            );
        }
        Ok(())
    }

    async fn begin_inline_chain(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        _dedup_keys: &[(String, String)],
    ) -> Result<Vec<(String, String)>, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        // The chain has made no call yet, so this is the point to catch a delivery that was already
        // recorded. Record and marker are one transaction: the marker names exactly what this
        // chain wrote, and only that can ever be released.
        let keys = self.pending_for(&dep);
        PgInflightChainStore::new(self.pool.clone())
            .begin_recording(&dep, instance, &keys, sutra_channels::replica_id())
            .await
            .map_err(|e| persistence_diag("in-flight chain marker write failed", &e))?;
        Ok(keys)
    }

    async fn end_inline_chain(&self, deployment: &sutra_executor::DeploymentId, instance_id: &str) {
        let (Ok(dep), Ok(instance)) = (persist_dep(deployment), parse_instance(instance_id)) else {
            return;
        };
        if let Err(e) = PgInflightChainStore::new(self.pool.clone())
            .clear(&dep, instance)
            .await
        {
            // The sweep releases it once its holder is gone or it ages out.
            warn!(instance_id, error = %e, "in-flight chain marker delete failed");
        }
    }

    async fn poisoned_call_emission_exists(
        &self,
        deployment: &sutra_executor::DeploymentId,
        instance_id: &str,
        node_id: &str,
    ) -> Result<bool, Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        let store = sutra_persistence::stores::PgOutboxStore::new(self.pool.clone());
        store
            .poisoned_exists_for_node(&dep, instance, node_id)
            .await
            .map_err(|e| persistence_diag("outbox poisonedExistsForNode failed", &e))
    }

    async fn commit_emissions(
        &self,
        deployment: &sutra_executor::DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        if emissions.is_empty() {
            // Nothing to write: the step is done once its audit is durable (no transaction,
            // so no transaction id; audit is never dropped on doubt).
            let gate = self.audit_gate();
            if let (Some(spool), Some(tracker)) = (&self.audit, &self.attempts) {
                if tracker.has_open() {
                    tracker.prepare(spool, crate::audit_recovery::NO_TRANSACTION);
                }
            }
            let synced: Result<(), PersistenceError> = match self.audit_barrier().await {
                Ok(()) => Ok(()),
                Err(d) => {
                    let message = d.message.clone();
                    *gate.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(d);
                    Err(PersistenceError::InvalidArgument(message))
                }
            };
            return match (self.settle_attempts(gate, &synced), synced) {
                (_, Ok(())) => Ok(()),
                (Some(d), Err(_)) => Err(d),
                (None, Err(e)) => Err(persistence_diag("audit sync failed", &e)),
            };
        }
        let dep = persist_dep(deployment)?;
        let mut entries = outbox_entries(&dep, emissions)?;
        let reserved = self.reserve_local(&mut entries);
        // Sync-path terminal step: the completed activation's emissions land in
        // ONE deployment-scoped transaction — all rows, or none.
        // Taken once: every attempt below writes the same dedup keys.
        let pending = self.pending_for(&dep);
        let gate = self.audit_gate();
        let attempt = || async {
            let mut tx = begin_deployment_tx(&self.pool, &dep).await?;
            sutra_persistence::step::record_inbox_seen_in(
                &mut tx,
                &dep,
                &pending,
                entries.first().map(|entry| entry.instance_id),
            )
            .await?;
            let mut instances = Vec::new();
            for entry in &entries {
                PgOutboxStore::enqueue_in(&mut tx, entry).await?;
                if !instances.contains(&entry.instance_id) {
                    instances.push(entry.instance_id);
                }
            }
            // A synchronous-call chain that completes ends its in-flight marker here.
            for instance in instances {
                PgInflightChainStore::clear_in(&mut tx, &dep, instance).await?;
            }
            gate.before_commit(&mut tx).await?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("sync-path outbox commit"))
        };
        let committed = commit_riding_out_outage("sync-path outbox commit", attempt).await;
        let gate_failure = self.settle_attempts(gate, &committed);
        self.after_enqueue(deployment, &entries, reserved, committed.is_ok())
            .await;
        if let (Err(_), Some(d)) = (&committed, gate_failure) {
            return Err(d);
        }
        committed.map_err(|e| persistence_diag("sync-path outbox commit failed", &e))?;
        info!(
            deployment = deployment.value(),
            emissions = emissions.len(),
            "sync-path emissions committed to the outbox in one step"
        );
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl ChannelInboxStore for PersistenceBridge {
    async fn record_seen(
        &self,
        deployment: &sutra_executor::DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> sutra_channels::DedupCheckOutcome {
        use sutra_channels::DedupCheckOutcome as Check;
        let Ok(dep) = PersistDeploymentId::new(deployment.value()) else {
            // A deployment id the store cannot hold has no record to check or write. Refusing would
            // be permanent, not retryable, and any step of this delivery fails on the same id.
            warn!(
                channel,
                event_id,
                deployment = deployment.value(),
                "inbox dedup skipped: the deployment id is malformed; proceeding with no record"
            );
            return Check::First;
        };
        // Read, don't write: the record is written by the step that processes this delivery, in
        // that step's own transaction, so it exists iff the step's effects do. A crash before the
        // commit leaves no record and the redelivery runs; a concurrent delivery of the same
        // event that commits first makes this delivery's step roll back
        // (SUTRA.INBOUND.DUPLICATE_CONCURRENT).
        match self.inbox.check(&dep, channel, event_id).await {
            Ok(InboxSeenState::Recorded) => Check::Duplicate,
            Ok(InboxSeenState::InDoubt) => Check::InDoubt,
            Ok(InboxSeenState::Absent) => {
                self.pending_seen
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(sutra_channels::PendingDedup {
                        deployment: deployment.clone(),
                        channel: channel.to_string(),
                        event_id: event_id.to_string(),
                    });
                Check::First
            }
            Err(e) => {
                warn!(channel, event_id, error = %e, "inbox dedup check failed; refusing the delivery");
                Check::Unavailable
            }
        }
    }

    async fn settle(&self, processed: bool, instance_id: Option<&str>) {
        let pending: Vec<sutra_channels::PendingDedup> = self
            .pending_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        if !processed || pending.is_empty() {
            return; // a failed dispatch leaves no record, so its redelivery runs
        }
        // The dispatch completed without a step consuming these keys (it persisted nothing, or
        // ended on another path): record them now, synchronously, so a redelivery is a
        // duplicate.
        let instance = instance_id.and_then(|id| uuid::Uuid::parse_str(id).ok());
        for p in pending {
            let Ok(dep) = PersistDeploymentId::new(p.deployment.value()) else {
                continue;
            };
            // The delivery has already run, and its effects outside the engine's own tables (a
            // data task's autocommitted write) cannot be undone: the record is what keeps a
            // redelivery of the same event from running it again. A database that is briefly
            // unreachable (a restart breaks every pooled connection) is waited out, bounded,
            // instead of acknowledging the delivery with no record behind it.
            let recorded = commit_riding_out_outage("inbox dedup write", || {
                self.inbox
                    .record_seen_for(&dep, &p.channel, &p.event_id, instance)
            })
            .await;
            match recorded {
                Ok(true) => {}
                // Only a delivery that raced another gets here: no step wrote its key, so what it
                // did was not durable, and it has already run.
                Ok(false) => {
                    warn!(channel = %p.channel, event_id = %p.event_id, "a delivery that was already recorded ran with no step to catch it");
                }
                Err(e) => {
                    warn!(channel = %p.channel, event_id = %p.event_id, error = %e, "inbox dedup write failed");
                }
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self
            .pending_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    async fn original_instance(
        &self,
        deployment: &sutra_executor::DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Option<String> {
        let dep = PersistDeploymentId::new(deployment.value()).ok()?;
        match self.inbox.original_instance(&dep, channel, event_id).await {
            Ok(found) => found.map(|id| id.to_string()),
            Err(e) => {
                warn!(channel, event_id, error = %e, "inbox original-instance read failed");
                None
            }
        }
    }

    fn take_pending(&self) -> Vec<sutra_channels::PendingDedup> {
        self.pending_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }

    fn restore_pending(&self, pending: Vec<sutra_channels::PendingDedup>) {
        self.pending_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(pending);
    }

    async fn forget(
        &self,
        deployment: &sutra_executor::DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> bool {
        let Ok(dep) = PersistDeploymentId::new(deployment.value()) else {
            return true; // no record can exist under an id the store cannot hold
        };
        match self.inbox.forget(&dep, channel, event_id).await {
            Ok(_) => true,
            Err(e) => {
                // The caller keeps the chain's marker, so a redelivery stays in doubt rather than
                // swallowed, until the sweep releases the record.
                warn!(channel, event_id, error = %e, "inbox dedup release failed");
                false
            }
        }
    }
}

#[async_trait::async_trait]
impl IncidentSink for PersistenceBridge {
    /// Durably record a dead-lettered inbound failure in `dead_letter`.
    /// Best-effort — the dispatcher's `tracing::error!` floor has already fired unconditionally, so
    /// a write failure here is logged and swallowed; this never panics and never changes the ack.
    async fn record(&self, incident: InboundIncident) {
        let dep = match PersistDeploymentId::new(&incident.deployment) {
            Ok(dep) => dep,
            Err(_) => {
                warn!(
                    deployment = %incident.deployment,
                    "dead-letter deployment id failed persistence-form validation — not durably \
                     recorded (the tracing::error! floor already fired)"
                );
                return;
            }
        };
        // Parse the RFC 3339 receive time (same helper the audit sink + timer bridge use); fall
        // back to now on a malformed stamp rather than dropping the incident.
        let received_at = OffsetDateTime::parse(
            &incident.received_at,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| OffsetDateTime::now_utc());
        let row = DeadLetterRow {
            deployment: dep,
            channel: incident.channel,
            process_id: incident.process_id,
            dedup_key: incident.dedup_key,
            failure_code: incident.failure_code,
            detail: incident.detail,
            received_at,
            // The replay capture rides straight through (already cap-truncated by the dispatcher);
            // an incident with no captured message — the outbound `required` path — writes NULLs
            // and the replay endpoint answers "no payload captured" for it.
            payload: incident.payload,
            headers: incident.headers,
            content_type: incident.content_type,
            tenant: incident.tenant,
            module_key: incident.module_key,
        };
        let store = PgDeadLetterStore::new(self.pool.clone());
        if let Err(e) = store.insert(&row).await {
            warn!(error = %e, "durable dead-letter write failed (best-effort; the tracing::error! floor already recorded it)");
        }
    }
}

#[cfg(test)]
mod dedup_tests {
    use std::time::Duration;

    use sutra_channels::DedupCheckOutcome;

    use super::*;

    /// A bridge whose database never answers: nothing listens on this port.
    fn bridge_with_no_database() -> PersistenceBridge {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://sutra@127.0.0.1:1/none")
            .expect("a lazy pool never connects here");
        PersistenceBridge::new(pool)
    }

    fn deployment() -> sutra_executor::DeploymentId {
        sutra_executor::DeploymentId::of("dep-0000000000000000000000d1").expect("valid")
    }

    #[tokio::test]
    async fn a_failed_check_reports_it_and_leaves_no_key_behind() {
        let bridge = bridge_with_no_database();
        assert_eq!(
            bridge.record_seen(&deployment(), "in", "evt-1").await,
            DedupCheckOutcome::Unavailable
        );
        assert!(
            !bridge.has_pending(),
            "the delivery is refused, so no step will write it"
        );
    }

    #[tokio::test]
    async fn a_forget_the_store_cannot_answer_says_so() {
        let bridge = bridge_with_no_database();
        assert!(!bridge.forget(&deployment(), "in", "evt-1").await);
    }
}

#[cfg(test)]
mod audit_durability_tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use sutra_channels::audit_spool::SpoolFaultInjector;
    use sutra_channels::{AuditEvent, AuditSpool, AuditSpoolConfig};

    use super::*;

    fn bridge_with_no_database(spool: &AuditSpool) -> PersistenceBridge {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://sutra@127.0.0.1:1/none")
            .expect("a lazy pool never connects here");
        PersistenceBridge::new(pool).with_audit_spool(spool.sender())
    }

    fn spool_config(name: &str) -> AuditSpoolConfig {
        AuditSpoolConfig {
            dir: std::env::temp_dir().join(format!("sutra-{name}-{}", uuid::Uuid::new_v4())),
            sync_linger: Duration::ZERO,
            sinks: ["jsonl".to_string()].into_iter().collect(),
            ..AuditSpoolConfig::default()
        }
    }

    fn event() -> AuditEvent {
        AuditEvent {
            sink: "jsonl".into(),
            deployment_id: "dep-0000000000000000000000d1".into(),
            tenant: "t".into(),
            instance_id: "11111111-1111-1111-1111-111111111111".into(),
            seq: 1,
            at: "2026-11-24T00:00:00Z".into(),
            event_type: "INSTANCE_COMPLETED".into(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{}".into(),
            correlation: Default::default(),
            attempt: None,
        }
    }

    fn deployment() -> sutra_executor::DeploymentId {
        sutra_executor::DeploymentId::of("dep-0000000000000000000000d1").expect("valid")
    }

    /// The commit does not go ahead until the step's audit events are synced: with a 300 ms
    /// linger and nothing else waiting, the event is not synced until the commit's barrier
    /// asks for it (the linger would hold it), and it is durable when the commit returns.
    #[tokio::test]
    async fn step_commit_waits_for_spool() {
        let cfg = AuditSpoolConfig {
            sync_linger: Duration::from_millis(300),
            ..spool_config("bridge-wait")
        };
        let dir = cfg.dir.clone();
        let spool = AuditSpool::open(cfg).unwrap();
        let bridge = bridge_with_no_database(&spool);
        spool.sender().append(event());
        assert_eq!(spool.durable_through(), 0);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            spool.durable_through(),
            0,
            "held by the linger until a barrier"
        );
        let started = Instant::now();
        // An empty emissions commit writes nothing to the database: only the barrier runs.
        bridge.commit_emissions(&deployment(), &[]).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "a lone step does not wait out the linger"
        );
        assert_eq!(
            spool.durable_through(),
            1,
            "the step's event was synced first"
        );
        drop(bridge);
        spool.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A spool that cannot sync fails the commit (the delivery is then retried); the event
    /// stays queued and is written once the spool can sync again.
    #[tokio::test]
    async fn spool_failure_fails_commit_not_lost() {
        let cfg = AuditSpoolConfig {
            sync_timeout: Duration::from_millis(300),
            ..spool_config("bridge-fail")
        };
        let dir = cfg.dir.clone();
        let faults = Arc::new(SpoolFaultInjector::default());
        let spool = AuditSpool::open_with_faults(cfg, Some(Arc::clone(&faults))).unwrap();
        let bridge = bridge_with_no_database(&spool);
        faults.fail_next_syncs(u64::MAX);
        spool.sender().append(event());
        let refused = bridge
            .commit_emissions(&deployment(), &[])
            .await
            .unwrap_err();
        assert_eq!(refused.code, AUDIT_SPOOL_SYNC_FAILED);
        assert!(
            !spool.healthy(),
            "readiness goes DOWN while the spool cannot sync"
        );
        // The disk is writable again: the spool recovers by itself and the commit goes ahead.
        faults.fail_next_syncs(0);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !spool.healthy() {
            assert!(Instant::now() < deadline, "the spool recovers");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        bridge.commit_emissions(&deployment(), &[]).await.unwrap();
        let held = spool
            .reader(0)
            .next_batch(10, spool.durable_through())
            .unwrap();
        assert_eq!(held.len(), 1, "the audit event was never lost");
        drop(bridge);
        spool.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Tier 2: the spool against real databases, an engine database and a separate audit database
/// on one PostgreSQL container.
#[cfg(test)]
mod audit_durability_docker {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use sutra_channels::{AuditEvent, AuditSpool, AuditSpoolConfig, ShipperConfig};
    use sutra_persistence::migrate::MigrationFamily;

    use super::*;

    const DEP: &str = "dep-0000000000000000000000c7";
    const STEPS: u32 = 40;

    static CONTAINER: OnceLock<(
        testcontainers::Container<testcontainers_modules::postgres::Postgres>,
        u16,
    )> = OnceLock::new();
    static DB_SEQ: AtomicU32 = AtomicU32::new(0);

    fn container_port() -> u16 {
        let (_, port) = CONTAINER.get_or_init(|| {
            std::thread::spawn(|| {
                use testcontainers::runners::SyncRunner;
                use testcontainers::ImageExt;
                let container = testcontainers_modules::postgres::Postgres::default()
                    .with_tag("16-alpine")
                    .start()
                    .expect("start postgres:16-alpine (docker required)");
                sutra_testkit::reap_on_exit(container.id());
                let port = container.get_host_port_ipv4(5432).expect("mapped 5432");
                (container, port)
            })
            .join()
            .expect("container bootstrap thread")
        });
        *port
    }

    /// A fresh database migrated with `family` only, as `serve` migrates each database. Returns
    /// its URL.
    async fn fresh_database(family: MigrationFamily) -> String {
        let port = container_port();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
            ))
            .await
            .expect("admin pool");
        let db = format!("durability_it_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .expect("create database");
        drop(admin);
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root")
            .to_path_buf();
        let roots = [
            repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
            repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
        ];
        let root_refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
        let scripts =
            sutra_persistence::migrate::collect_family(&root_refs, family).expect("collect");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/{db}");
        let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .expect("migration connection");
        sutra_persistence::migrate::apply_migrations(&mut conn, &scripts)
            .await
            .expect("apply migrations");
        url
    }

    async fn pool(url: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .expect("pool")
    }

    fn instance(step: u32) -> String {
        format!("00000000-0000-0000-0000-{step:012}")
    }

    /// One pass of step `step` by process `tag` (its own attempt, base 0): STARTED and
    /// COMPLETED, plus, on the `lost` path, a node the re-run never reaches.
    fn step_events(step: u32, tag: &str, lost: bool) -> Vec<AuditEvent> {
        let attempt = sutra_channels::AuditAttempt {
            id: format!("{tag}-{step}"),
            base: 0,
            at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .expect("rfc3339"),
        };
        let mut types = vec!["INSTANCE_STARTED", "INSTANCE_COMPLETED"];
        if lost {
            types.push("NODE_LEFT");
        }
        types
            .into_iter()
            .zip(1u32..)
            .map(|(event_type, seq)| AuditEvent {
                sink: "sql".into(),
                deployment_id: DEP.into(),
                tenant: "t".into(),
                instance_id: instance(step),
                seq,
                at: "2026-11-24T00:00:00Z".into(),
                event_type: event_type.into(),
                node_id: Some(tag.to_string()),
                diagnostic_code: None,
                diagnostic_json: None,
                payload_json: "{}".into(),
                correlation: Default::default(),
                attempt: Some(attempt.clone()),
            })
            .collect()
    }

    fn step_emission(instance_id: &str, key: &str) -> OutboxEmission {
        OutboxEmission {
            instance_id: instance_id.to_string(),
            node_id: "Reply".to_string(),
            destination: "http://example.invalid/ack".to_string(),
            body: sutra_executor::Sensitive::new(b"{}".to_vec()),
            content_type: Some("application/json".to_string()),
            required: false,
            mode: sutra_bpmn::qbindings::ReplyMode::Native,
            outbox_key: format!("k-{key}"),
            cloud_event_json: None,
            auth_ref_json: None,
            labels: Default::default(),
            traceparent: None,
            headers: Default::default(),
        }
    }

    fn spool_config(dir: &std::path::Path) -> AuditSpoolConfig {
        AuditSpoolConfig {
            dir: dir.to_path_buf(),
            sync_linger: Duration::from_millis(2),
            sinks: ["sql".to_string()].into_iter().collect(),
            ..AuditSpoolConfig::default()
        }
    }

    /// Run `steps` through a bridge whose step commits pass the spool barrier; the step whose
    /// number is `crash_at` stops between its audit sync and its commit, and the "process" dies
    /// there: the spool is dropped unsynced and the runtime (with its shipper) is torn down.
    /// With `crash_after_commit`, the process dies right after that step's commit, before the
    /// shipper is waited for. `tag` names the process's attempts and marks its events' path.
    /// Returns the steps that committed.
    #[allow(clippy::too_many_arguments)]
    fn run_process(
        engine_url: &str,
        audit_url: &str,
        spool_dir: &std::path::Path,
        steps: std::ops::Range<u32>,
        crash_at: Option<u32>,
        crash_after_commit: Option<u32>,
        tag: &str,
        instance_offset: u32,
    ) -> Vec<u32> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let spool = AuditSpool::open(spool_config(spool_dir)).expect("spool opens");
        let (committed, crashed) = runtime.block_on(async {
            let audit_pool = pool(audit_url).await;
            let shipper = sutra_channels::spawn_audit_shipper(
                &tokio::runtime::Handle::current(),
                &spool,
                Arc::new(crate::audit_sinks::SqlAuditStore::new(audit_pool)),
                ShipperConfig::default(),
            );
            let engine_pool = pool(engine_url).await;
            // Recovery of what a previous run left unmarked, and the resolver, as at start.
            let resolver = crate::audit_recovery::spawn(
                &tokio::runtime::Handle::current(),
                &spool,
                Some(engine_pool.clone()),
            )
            .expect("audit recovery");
            let crash = Arc::new(AtomicBool::new(false));
            let crash_committed = Arc::new(AtomicBool::new(false));
            let tracker = sutra_channels::AuditAttemptTracker::new();
            let bridge = PersistenceBridge::new(engine_pool)
                .with_audit_spool(spool.sender())
                .with_audit_attempts(tracker.clone(), Some(resolver))
                .with_crash_after_audit_sync(Arc::clone(&crash))
                .with_crash_after_commit(Arc::clone(&crash_committed));
            let dep = sutra_executor::DeploymentId::of(DEP).expect("deployment id");
            let mut committed = Vec::new();
            for step in steps {
                let instance_id = instance(step + instance_offset);
                for mut event in step_events(step, tag, crash_at == Some(step)) {
                    event.instance_id = instance_id.clone();
                    let attempt = event.attempt.clone().expect("a pass event");
                    if !tracker.is_open(&attempt.id) {
                        tracker.register(DEP, &instance_id, &attempt);
                    }
                    tracker.observe(&attempt.id, event.seq, &event.event_type);
                    spool.sender().append(event);
                }
                // A real transaction (one outbox row), so the attempt is prepared under its id.
                let emission = step_emission(&instance_id, &format!("{tag}-{step}"));
                if crash_at == Some(step) {
                    crash.store(true, Ordering::SeqCst);
                    let stopped = bridge
                        .commit_emissions(&dep, std::slice::from_ref(&emission))
                        .await
                        .unwrap_err();
                    assert_eq!(stopped.code, TEST_CRASH_AFTER_AUDIT_SYNC);
                    // Killed here: no checkpoint, no clean shutdown.
                    drop(shipper);
                    return (committed, true);
                }
                if crash_after_commit == Some(step) {
                    crash_committed.store(true, Ordering::SeqCst);
                }
                bridge
                    .commit_emissions(&dep, std::slice::from_ref(&emission))
                    .await
                    .expect("step commits");
                committed.push(step);
                if crash_after_commit == Some(step) {
                    drop(shipper);
                    return (committed, true);
                }
            }
            shipper
                .ship_through(spool.durable_through(), Duration::from_secs(30))
                .await
                .expect("the SQL store catches up");
            (committed, false)
        });
        drop(runtime);
        if crashed {
            spool.crash();
        } else {
            spool.shutdown().expect("spool closes");
        }
        committed
    }

    /// Kill the engine between the spool sync and the step commit, restart it,
    /// re-run the step as a redelivery would, and every committed step's events are in the
    /// audit database exactly once.
    #[ignore = "docker"]
    #[test]
    fn audit_rows_survive_crash() {
        let setup = tokio::runtime::Runtime::new().expect("runtime");
        let (engine_url, audit_url) = setup.block_on(async {
            (
                fresh_database(MigrationFamily::Engine).await,
                fresh_database(MigrationFamily::Audit).await,
            )
        });
        let dir = std::env::temp_dir().join(format!("sutra-crash-it-{}", uuid::Uuid::new_v4()));
        let crash_at = STEPS / 2;
        let before = run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..STEPS,
            Some(crash_at),
            None,
            "first",
            0,
        );
        assert_eq!(before, (0..crash_at).collect::<Vec<_>>());
        // Restart: the crashed step is redelivered and runs again (another path: no NODE_LEFT),
        // then the rest.
        let after = run_process(
            &engine_url,
            &audit_url,
            &dir,
            crash_at..STEPS,
            None,
            None,
            "restarted",
            0,
        );
        assert_eq!(after, (crash_at..STEPS).collect::<Vec<_>>());

        let rows: Vec<(String, i32, i64)> = setup.block_on(async {
            sqlx::query_as(
                "SELECT instance_id::text, seq, count(*) FROM audit_event \
                 GROUP BY instance_id, seq ORDER BY instance_id, seq",
            )
            .fetch_all(&pool(&audit_url).await)
            .await
            .expect("audit rows")
        });
        let expected: Vec<(String, i32, i64)> = (0..STEPS)
            .flat_map(|step| [(instance(step), 1, 1), (instance(step), 2, 1)])
            .collect();
        assert_eq!(
            rows, expected,
            "every committed step's events, exactly once"
        );
        // The crashed step's trail is the re-run's, the lost NODE_LEFT is gone.
        let crashed: Vec<String> = setup.block_on(async {
            sqlx::query_scalar(
                "SELECT node_id FROM audit_event WHERE instance_id = $1::uuid ORDER BY seq",
            )
            .bind(instance(crash_at))
            .fetch_all(&pool(&audit_url).await)
            .await
            .expect("crashed step rows")
        });
        assert_eq!(crashed, vec!["restarted", "restarted"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The process dies right after a step's commit, before its events were shipped. The
    /// redelivery is deduplicated, so the step does not run again: its committed attempt ships
    /// at the restart, once.
    #[ignore = "docker"]
    #[test]
    fn crash_after_commit_before_marker_sync_recovery_confirms() {
        let setup = tokio::runtime::Runtime::new().expect("runtime");
        let (engine_url, audit_url) = setup.block_on(async {
            (
                fresh_database(MigrationFamily::Engine).await,
                fresh_database(MigrationFamily::Audit).await,
            )
        });
        let dir =
            std::env::temp_dir().join(format!("sutra-crash-after-it-{}", uuid::Uuid::new_v4()));
        let crash_after = STEPS / 2;
        let before = run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..STEPS,
            None,
            Some(crash_after),
            "first",
            0,
        );
        assert_eq!(before, (0..=crash_after).collect::<Vec<_>>());
        let after = run_process(
            &engine_url,
            &audit_url,
            &dir,
            crash_after + 1..STEPS,
            None,
            None,
            "restarted",
            0,
        );
        assert_eq!(after, (crash_after + 1..STEPS).collect::<Vec<_>>());
        let rows: Vec<(String, i32, String)> = setup.block_on(async {
            sqlx::query_as(
                "SELECT instance_id::text, seq, node_id FROM audit_event \
                 ORDER BY instance_id, seq",
            )
            .fetch_all(&pool(&audit_url).await)
            .await
            .expect("audit rows")
        });
        let expected: Vec<(String, i32, String)> = (0..STEPS)
            .flat_map(|step| {
                let tag = if step <= crash_after {
                    "first"
                } else {
                    "restarted"
                };
                [
                    (instance(step), 1, tag.to_string()),
                    (instance(step), 2, tag.to_string()),
                ]
            })
            .collect();
        assert_eq!(rows, expected, "the committed attempts, exactly once");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The ids of the attempts the spool marks `ABORTED`.
    fn aborted_in_spool(dir: &std::path::Path) -> Vec<String> {
        let spool = AuditSpool::open(spool_config(dir)).expect("spool reopens");
        let records = spool
            .reader(0)
            .next_batch(100_000, spool.durable_through())
            .expect("spool reads");
        spool.shutdown().expect("spool closes");
        records
            .into_iter()
            .filter(|r| r.event.event_type == sutra_channels::audit_attempts::MARKER_ABORTED)
            .filter_map(|r| r.event.attempt.map(|a| a.id))
            .collect()
    }

    /// The process dies after the step's spool sync, before its `COMMIT`. Recovery finds
    /// the attempt prepared under a transaction that aborted and marks it `ABORTED`.
    #[ignore = "docker"]
    #[test]
    fn crash_after_spool_sync_before_commit_recovery_aborts() {
        let setup = tokio::runtime::Runtime::new().expect("runtime");
        let (engine_url, audit_url) = setup.block_on(async {
            (
                fresh_database(MigrationFamily::Engine).await,
                fresh_database(MigrationFamily::Audit).await,
            )
        });
        let dir = std::env::temp_dir().join(format!("sutra-abort-it-{}", uuid::Uuid::new_v4()));
        run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..3,
            Some(1),
            None,
            "first",
            0,
        );
        run_process(
            &engine_url,
            &audit_url,
            &dir,
            1..3,
            None,
            None,
            "restarted",
            0,
        );
        assert_eq!(aborted_in_spool(&dir), vec!["first-1".to_string()]);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A start whose commit never happened is retried under a NEW instance id (the
    /// re-run mints one). The orphan instance leaves no trail in the audit database.
    #[ignore = "docker"]
    #[test]
    fn orphan_start_retried_under_a_new_instance_id_leaves_no_trail() {
        let setup = tokio::runtime::Runtime::new().expect("runtime");
        let (engine_url, audit_url) = setup.block_on(async {
            (
                fresh_database(MigrationFamily::Engine).await,
                fresh_database(MigrationFamily::Audit).await,
            )
        });
        let dir = std::env::temp_dir().join(format!("sutra-orphan-it-{}", uuid::Uuid::new_v4()));
        run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..1,
            Some(0),
            None,
            "first",
            0,
        );
        run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..1,
            None,
            None,
            "restarted",
            500,
        );
        let instances: Vec<(String, i64)> = setup.block_on(async {
            sqlx::query_as(
                "SELECT instance_id::text, count(*) FROM audit_event GROUP BY instance_id",
            )
            .fetch_all(&pool(&audit_url).await)
            .await
            .expect("audit rows")
        });
        assert_eq!(
            instances,
            vec![(instance(500), 2)],
            "only the committed re-run"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// With a SQL audit store, step commits write no audit to the engine's own
    /// database; the audit database gets every row.
    #[ignore = "docker"]
    #[test]
    fn engine_db_has_no_audit_writes() {
        let setup = tokio::runtime::Runtime::new().expect("runtime");
        let (engine_url, audit_url) = setup.block_on(async {
            (
                fresh_database(MigrationFamily::Engine).await,
                fresh_database(MigrationFamily::Audit).await,
            )
        });
        let dir =
            std::env::temp_dir().join(format!("sutra-no-engine-audit-{}", uuid::Uuid::new_v4()));
        run_process(
            &engine_url,
            &audit_url,
            &dir,
            0..STEPS,
            None,
            None,
            "only",
            0,
        );
        setup.block_on(async {
            let engine = pool(&engine_url).await;
            let engine_audit: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_user_tables WHERE relname LIKE 'audit%'",
            )
            .fetch_one(&engine)
            .await
            .expect("engine stats");
            assert_eq!(
                engine_audit, 0,
                "the engine database has no audit table to write"
            );
            let audit = pool(&audit_url).await;
            // Table statistics are flushed by each backend within about a second.
            let mut inserted = 0i64;
            for _ in 0..50 {
                inserted = sqlx::query_scalar(
                    "SELECT coalesce(sum(n_tup_ins), 0)::bigint FROM pg_stat_user_tables \
                     WHERE relname = 'audit_event'",
                )
                .fetch_one(&audit)
                .await
                .expect("audit stats");
                if inserted >= i64::from(STEPS * 2) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            assert_eq!(
                inserted,
                i64::from(STEPS * 2),
                "every row went to the audit database"
            );
        });
        let _ = std::fs::remove_dir_all(dir);
    }
}
