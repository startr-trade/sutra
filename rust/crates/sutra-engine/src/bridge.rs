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
use sutra_persistence::snapshot::{InstanceSnapshot, SnapshotCrypto};

use crate::snapshot_values;
use sutra_persistence::step::{
    commit_step_with_timers, commit_step_with_timers_releasing_retiring, StepAlias, StepSubject,
    StepTimerWait, StepWait, StepWrite,
};
use sutra_persistence::stores::{
    DeadLetterRow, InboxStore, InstanceStore, OutboxEntry, PgDeadLetterStore, PgInboxStore,
    PgInflightChainStore, PgInstanceStore, PgOutboxStore, ReplyMode as PersistReplyMode,
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
    /// for the rest; on failure release the reservation (nothing was inserted).
    fn after_enqueue(
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
            .map(crate::outbox::to_claimed)
            .collect();
        let rest = entries.len() - handed.len();
        if !handed.is_empty() {
            signal.hand_off(deployment, handed);
        }
        if rest > 0 {
            signal.wake();
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

    /// Decode a persisted snapshot, decrypting its `sutra.enc.` values when the snapshot carries a
    /// keyId (read from the snapshot itself — no external tenant lookup). Fails closed on an
    /// encrypted snapshot with no key provider configured.
    fn decode_snapshot(
        &self,
        bytes: &[u8],
        instance_id: &str,
    ) -> Result<InstanceSnapshot, Diagnostic> {
        decode_persisted_snapshot(self.key_provider.as_deref(), bytes, instance_id)
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
    let key_id = InstanceSnapshot::peek_key_id(bytes).map_err(decode_diag)?;
    match (key_id, key_provider) {
        (Some(key_id), Some(provider)) => {
            let key = provider.data_key(&key_id).map_err(crypto_diag)?;
            let cipher = Aes256GcmCipher::new(&key);
            let ctx = SnapshotCrypto::new(&cipher, &key_id, instance_id);
            InstanceSnapshot::read_encrypted(bytes, Some(&ctx)).map_err(decode_diag)
        }
        (Some(_), None) => Err(Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!(
                "instance {instance_id} was persisted encrypted (sutra.keyId present) but no \
                 encryption key provider is configured (sutra.crypto.master-key) — cannot \
                 decrypt; refusing to resume in the clear"
            ),
        )),
        (None, _) => InstanceSnapshot::read(bytes).map_err(decode_diag),
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
    Diagnostic::error(channel_codes::RUNTIME_UNEXPECTED, format!("{context}: {e}"))
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
        let committed = commit_step_with_timers(&self.pool, &step, &timers).await;
        self.after_enqueue(deployment, &step.outbox, reserved, committed.is_ok());
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
        let key_id = InstanceSnapshot::peek_key_id(&row.serialised)
            .map_err(decode_diag)?
            .unwrap_or_default();
        let decoded = self.decode_snapshot(&row.serialised, instance_id)?;
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
        let retained = async {
            let mut tx = begin_deployment_tx(&self.pool, &dep).await?;
            sutra_persistence::step::record_inbox_seen_in(&mut tx, &dep, &self.pending_for(&dep))
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
            for entry in &entries {
                PgOutboxStore::enqueue_in(&mut tx, entry).await?;
            }
            PgInflightChainStore::clear_in(&mut tx, &dep, instance).await?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("terminal step commit"))?;
            Ok::<bool, PersistenceError>(retained)
        }
        .await;
        self.after_enqueue(deployment, &entries, reserved, retained.is_ok());
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
        let committed = commit_step_with_timers_releasing_retiring(
            &self.pool,
            &step,
            &timers,
            Some(&self.claim_owner),
            &retired,
        )
        .await;
        self.after_enqueue(deployment, &step.outbox, reserved, committed.is_ok());
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
            tx.commit()
                .await
                .map_err(PersistenceError::db("failed step commit"))?;
            Ok::<bool, PersistenceError>(true)
        }
        .await
        .map_err(|e| persistence_diag("failed-state commit failed", &e))?;
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
        dedup_keys: &[(String, String)],
    ) -> Result<(), Diagnostic> {
        let dep = persist_dep(deployment)?;
        let instance = parse_instance(instance_id)?;
        PgInflightChainStore::new(self.pool.clone())
            .begin(&dep, instance, dedup_keys, sutra_channels::replica_id())
            .await
            .map_err(|e| persistence_diag("in-flight chain marker write failed", &e))
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
            return Ok(());
        }
        let dep = persist_dep(deployment)?;
        let mut entries = outbox_entries(&dep, emissions)?;
        let reserved = self.reserve_local(&mut entries);
        // Sync-path terminal step: the completed activation's emissions land in
        // ONE deployment-scoped transaction — all rows, or none.
        let committed = async {
            let mut tx = begin_deployment_tx(&self.pool, &dep).await?;
            sutra_persistence::step::record_inbox_seen_in(&mut tx, &dep, &self.pending_for(&dep))
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
            tx.commit()
                .await
                .map_err(PersistenceError::db("sync-path outbox commit"))
        }
        .await;
        self.after_enqueue(deployment, &entries, reserved, committed.is_ok());
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
    ) -> bool {
        let Ok(dep) = PersistDeploymentId::new(deployment.value()) else {
            return true; // malformed id — proceed (fail-open on dedup, logged below)
        };
        // Read, don't write: the record is written by the step that processes this delivery, in
        // that step's own transaction, so it exists iff the step's effects do. A crash before the
        // commit leaves no record and the redelivery runs; a concurrent delivery of the same
        // event that commits first makes this delivery's step roll back
        // (SUTRA.INBOUND.DUPLICATE_CONCURRENT).
        match self.inbox.seen(&dep, channel, event_id).await {
            Ok(true) => false,
            Ok(false) => {
                self.pending_seen
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(sutra_channels::PendingDedup {
                        deployment: deployment.clone(),
                        channel: channel.to_string(),
                        event_id: event_id.to_string(),
                    });
                true
            }
            Err(e) => {
                warn!(channel, event_id, error = %e, "inbox dedup failed — proceeding (fail-open)");
                true
            }
        }
    }

    async fn settle(&self, processed: bool) {
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
        for p in pending {
            let Ok(dep) = PersistDeploymentId::new(p.deployment.value()) else {
                continue;
            };
            if let Err(e) = self.inbox.record_seen(&dep, &p.channel, &p.event_id).await {
                warn!(channel = %p.channel, event_id = %p.event_id, error = %e, "inbox dedup write failed");
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
    ) {
        let Ok(dep) = PersistDeploymentId::new(deployment.value()) else {
            return;
        };
        if let Err(e) = self.inbox.forget(&dep, channel, event_id).await {
            // The row then keeps suppressing a redelivery until it is pruned — the same
            // outcome as a crash mid-chain; logged so the stuck key is findable.
            warn!(channel, event_id, error = %e, "inbox dedup release failed");
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
