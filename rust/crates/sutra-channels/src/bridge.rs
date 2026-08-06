//! The suspend→resume persistence bridge — the trait seam between the channel dispatcher
//! and the durable instance store (`sutra-persistence` supplies the implementation; this
//! crate stays persistence-format-agnostic).
//!
//! Semantics: the instance-resume, suspended-instance-codec, and relay-correlator
//! seams together, with the STRICT step primitive underneath: each quiescent point —
//! start→park, wait→wait re-park, wait→end — commits
//! its snapshot + wait rows + alias rows in ONE local transaction, or nothing.

use std::collections::BTreeMap;

use sutra_bpmn::qbindings::ReplyMode;
use sutra_executor::emission::CloudEventLite;
use sutra_executor::{AuthRef, DeploymentId};
use sutra_feel::FeelValue;

use crate::diag::Diagnostic;

/// The persistence-shape of a parked instance — what the instance-snapshot codec encodes.
/// Variables are TYPED: a value survives a wait state as the value it was, so the gateway or
/// FEEL expression that re-evaluates after the resume sees a number as a number. (The persisted
/// encoding of that is the store's business — this crate stays persistence-format-agnostic.)
#[derive(Debug, Clone, Default)]
pub struct SuspendedInstance {
    /// Archive-local process id (`sutra.processId`).
    pub process_id: String,
    /// The instance's pinned deployment (`sutra.deploymentId`).
    pub deployment_id: String,
    /// Persisted status string (`sutra.status`) — diagnostics when not suspended.
    pub status: String,
    /// True when the snapshot is parked (resume is only valid then).
    pub suspended: bool,
    /// Replay-as-done node ids (`sutra.completedNodes`).
    pub completed_nodes: Vec<String>,
    /// The instance's variables (`sutra.var.<name>`), typed, with `@transient` variables already
    /// dropped (never persisted). A store that cannot carry a given type is free to degrade it —
    /// that is a persistence-format concern, and the load side reports whatever it recovered.
    pub variables: Vec<(String, FeelValue)>,
    /// `@sensitive` variable names present in `variables`: their values persist
    /// (resume needs them) but audit/log/diagnostics layers must redact them. Encoded as the
    /// snapshot's `sutra.sensitive` marking.
    pub sensitive: Vec<String>,
    /// The parked wait frontier (`sutra.waitingNodes`).
    pub waiting_nodes: Vec<String>,
    /// Routed start event of the original pass (`sutra.startNode`), empty when the sole
    /// start event was used.
    pub start_node: String,
    /// Per declared `<q:coverage>` path, the contiguous-prefix cursor persisted with the
    /// snapshot (`sutra.coverage.<pathId>`). Seeds the executor's coverage cursors on resume so a
    /// route spanning multiple wait states is still marked at completion (BTreeMap<pathId,count>).
    pub coverage: BTreeMap<String, u64>,
    /// Per `<q:retry>` node id, how many attempts of that task have already FAILED
    /// (`sutra.retry.<nodeId>`). The durable half of the per-task retry policy: a failed attempt
    /// with retries remaining parks the instance with the task still pending and a backoff timer
    /// row due, and this counter is what stops the budget restarting on every re-drive. Empty for
    /// an instance with no retry policy, or one whose retried tasks all succeeded.
    pub retry_attempts: BTreeMap<String, u32>,
    /// Per CHANNEL-CALL `<q:retry>` node id currently sitting in a BACKOFF window, the
    /// classification code of the failure that parked it (`sutra.retryWait.<nodeId>`). The
    /// durable discriminator between a call whose attempt is DEAD (backoff pending — its due
    /// timer is the re-drive; its late relays are refused) and one whose attempt is IN FLIGHT
    /// (waiting on its response — a relay resumes it normally): both look identical through
    /// `waiting_nodes` + `retry_attempts` alone. Registered-task retries never write it.
    pub retry_backoff: BTreeMap<String, String>,
    /// The per-instance monotonic audit-seq high-water at suspend (`sutra.auditSeq`).
    /// Persisted so a later resume seeds the audit listener's counter, keeping the DB audit
    /// sink's `(deployment_id, instance_id, seq)` uniqueness intact across suspend/resume and
    /// engine restart. `0` when no audit listener is wired (nothing emits).
    pub audit_seq: u32,
    /// The migration-stable crypto anchor (`sutra.keyId`): the tenant label whose
    /// DEK the snapshot's `sutra.enc.` values are encrypted under. Empty ⇒ no encryption for this
    /// instance (no cipher configured, or nothing in `encrypt_names`). Consumed at PARK time only;
    /// the load path re-reads it from the snapshot, and a re-park recomputes it.
    pub key_id: String,
    /// The at-rest encrypt set: variable names whose persisted value must be ciphertext
    /// (`@sensitive` ∪ redactor-controlled — those with a `<name>.redacted` companion). Consumed by
    /// the snapshot codec's `write_encrypted`; empty ⇒ every variable persists plaintext.
    pub encrypt_names: Vec<String>,
    /// The GDPR subject keys present in this instance: `(subject_name, raw_value)` for each
    /// `@subjectKey` variable. The bridge HMAC-blind-indexes each value (under the instance's keyId)
    /// and records it in `subject_index` atomically with the snapshot, so the instance is enumerable
    /// for disclosure/erasure with no cleartext PII. Empty ⇒ no subject keys / blind-indexing off.
    pub subjects: Vec<(String, String)>,
}

/// The persisted `sutra.status` value of an instance a fatal step killed. This crate stays
/// persistence-format-agnostic, so the string — not `sutra_persistence::snapshot::STATUS_FAILED`,
/// which it must equal byte for byte — is the seam: the dispatcher reads
/// [`SuspendedInstance::status`] and compares against it to fail every resume path closed
/// (`SUTRA.DISPATCH.INSTANCE_FAILED`). Frozen: it is part of the persisted snapshot format.
pub const INSTANCE_STATUS_FAILED: &str = "FAILED";

/// The persisted `sutra.status` of an instance that ran to an end event. Same seam and the same
/// frozen-string rule as [`INSTANCE_STATUS_FAILED`] (it must equal
/// `sutra_persistence::snapshot::STATUS_COMPLETED` byte for byte). It became load-bearing for THIS
/// crate with terminal retention (P1-2): a finished instance keeps its row for the retention
/// window, so a resume path can load one and must recognise it as over rather than as
/// mid-flight.
pub const INSTANCE_STATUS_COMPLETED: &str = "COMPLETED";

/// The persisted `sutra.status` of an instance an operator cancelled. See
/// [`INSTANCE_STATUS_COMPLETED`] — same rule, same reason.
pub const INSTANCE_STATUS_TERMINATED: &str = "TERMINATED";

/// One alias row a park step records atomically with the snapshot (name, value, unique).
#[derive(Debug, Clone)]
pub struct AliasRecord {
    pub name: String,
    pub value: String,
    pub unique: bool,
}

/// One TIMER wait row a park step records atomically with the snapshot (the
/// `waiting_event` TIMER marker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerWaitRecord {
    /// The timer node (timer catch / timer boundary / `#timeout` synthetic).
    pub node_id: String,
    /// RFC 3339 due timestamp (executor-computed).
    pub due_at: String,
}

/// One destination-bearing emission (`<q:send>` / `<q:reply destination=…>`) a step
/// commits atomically at its quiescent point (the strict transactional outbox). The
/// transport-neutral outbox-row payload: the implementation assigns row identity and
/// timestamps; `outbox_key` is minted at collection time and frozen through delivery
/// (the wire-level `Idempotency-Key`).
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEmission {
    /// Originating process instance (UUID string).
    pub instance_id: String,
    /// The emitting node (diagnostics).
    pub node_id: String,
    /// Scheme-bearing destination URI (already channel-resolved by the executor).
    pub destination: String,
    /// The outbound payload — wrapped [`sutra_executor::Sensitive`] (see
    /// [`crate::dispatch::InboundMessage::body`]).
    pub body: sutra_executor::Sensitive<Vec<u8>>,
    pub content_type: Option<String>,
    /// `<q:send required>` — delivery failure surfaces as an incident vs best-effort.
    pub required: bool,
    /// Wire-rendering mode.
    pub mode: ReplyMode,
    /// Consumer idempotency key — carried through the outbox onto the wire.
    pub outbox_key: String,
    /// Opaque CloudEvents JSON ([`cloud_event_to_json`]), when the emission is a CE.
    pub cloud_event_json: Option<String>,
    /// Opaque auth-reference JSON ([`auth_ref_to_json`]) the dispatcher resolves.
    pub auth_ref_json: Option<String>,
    /// The emitting deployment's authoring labels (payload data, never isolation).
    pub labels: BTreeMap<String, String>,
    /// W3C traceparent of the enqueuing request (trace-context bridge), when it carried one.
    pub traceparent: Option<String>,
    /// Author-declared `<q:header>` attributes (FEEL-resolved at emission),
    /// carried onto the wire as transport headers / broker application-properties via the existing
    /// outbound-header seam (`ClaimedOutboxRow.headers` → `OutboundMessage.headers`).
    pub headers: BTreeMap<String, String>,
}

/// The opaque `cloud_event_json` column encoding — CE structured-mode attribute names,
/// symmetric with the dispatcher's delivery-side parse.
pub fn cloud_event_to_json(ce: &CloudEventLite) -> String {
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::Value::String(ce.id.clone()));
    map.insert(
        "source".to_string(),
        serde_json::Value::String(ce.source.clone()),
    );
    map.insert(
        "specversion".to_string(),
        serde_json::Value::String(ce.spec_version.clone()),
    );
    map.insert(
        "type".to_string(),
        serde_json::Value::String(ce.ce_type.clone()),
    );
    if let Some(subject) = &ce.subject {
        map.insert(
            "subject".to_string(),
            serde_json::Value::String(subject.clone()),
        );
    }
    if let Some(time) = &ce.time {
        map.insert("time".to_string(), serde_json::Value::String(time.clone()));
    }
    if let Some(ct) = &ce.data_content_type {
        map.insert(
            "datacontenttype".to_string(),
            serde_json::Value::String(ct.clone()),
        );
    }
    serde_json::Value::Object(map).to_string()
}

/// The opaque `auth_ref_json` column encoding (`{"scheme","secretRef","header"?}`).
pub fn auth_ref_to_json(auth_ref: &AuthRef) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "scheme".to_string(),
        serde_json::Value::String(auth_ref.scheme.clone()),
    );
    map.insert(
        "secretRef".to_string(),
        serde_json::Value::String(auth_ref.secret_ref.clone()),
    );
    if let Some(header) = &auth_ref.header {
        map.insert(
            "header".to_string(),
            serde_json::Value::String(header.clone()),
        );
    }
    serde_json::Value::Object(map).to_string()
}

/// Random UUID v4 formatted 8-4-4-4-12 lowercase hex — outbox-key minting. Transport-only (the
/// outbox dispatcher is the sole caller); gating it also keeps `getrandom` off the model/wasm build.
#[cfg(feature = "transport")]
pub(crate) fn new_uuid() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("OS entropy source");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The stable per-PROCESS replica identity every instance claim is taken under —
/// `<host>-<pid>-<8 hex>`, computed once and memoised for the life of the process.
///
/// Three properties matter, and each one is load-bearing:
/// * **Per-process unique.** Two engine processes on one host (a dev box, a multi-replica
///   in-process test) MUST NOT share an owner id, or the claim CAS would hand both of them
///   the same instance. That rules out the bare `HOSTNAME` the lease election resolves its
///   holder id from (`sutra_transport_spi::leadership`) — a lease is one-per-role and
///   tolerates a host-scoped name; instance ownership is not.
/// * **Stable within the process.** The claim, its re-entrant refresh, and the release at
///   the quiescent point must all name the same owner; a per-call id would strand claims.
/// * **Bounded.** `instance_state.claim_owner` is `VARCHAR(128)` (V402) — the host part is
///   truncated so a long pod name can never overflow the column.
///
/// It deliberately does NOT survive a restart: a crashed replica's claims are stranded
/// until the `StuckInstanceScanner` sweeps them (`sutra.instance.claim-timeout`). That is
/// the sweeper's whole reason to exist — the alternative (a restart-stable id that silently
/// re-adopts claims) would also let two same-named processes collide.
#[cfg(feature = "transport")]
pub fn replica_id() -> &'static str {
    static REPLICA_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    REPLICA_ID.get_or_init(|| {
        let host = std::env::var("HOSTNAME")
            .ok()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        // 96 chars of host leaves room for `-<pid>-<8 hex>` inside VARCHAR(128).
        let host: String = host.chars().take(96).collect();
        let suffix = &new_uuid()[..8];
        format!("{host}-{}-{suffix}", std::process::id())
    })
}

/// What an instance-ownership claim answered — the resume paths' fork between "advance it"
/// and "bounce, someone else owns it".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceClaimOutcome {
    /// This replica owns the instance until the step commits (or the claim guard releases).
    Granted,
    /// Another replica's unexpired claim stands — this resume must not rehydrate.
    HeldByOther,
}

/// Durable instance persistence, as the dispatcher consumes it. Every mutation is ONE
/// step (one transaction — commit or nothing on the implementation side); the
/// step's `emissions` — destination-bearing `<q:send>`/`<q:reply>` collected up to the
/// quiescent point — enqueue on the outbox atomically WITH the step (nothing
/// commits unless the step commits, and what commits includes the collected emissions).
///
/// ASYNC seam (execution scale-out §3(a), Phase 3): every method is awaited on the shard
/// lane's single actor task, which drives one request to completion before the next
/// dequeue — so the §0 ordering properties (reply-implies-committed; commit
/// happens-before the next dequeue) are exactly what they were under the synchronous
/// `block_on` form. `?Send` deliberately: the consumer is the `Rc`-based, single-threaded
/// dispatcher, and its futures never cross threads.
#[async_trait::async_trait(?Send)]
pub trait InstanceBridge {
    /// Park a freshly-suspended instance: snapshot UPSERT + wait rows + TIMER wait rows +
    /// alias rows + outbox enqueues in ONE step. A unique-live alias collision MUST roll
    /// the whole step back and surface as an `Err` (the dispatcher maps it to
    /// `SUTRA.INBOUND.ALIAS_CONFLICT_REJECT`).
    async fn commit_park(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic>;

    /// Load + decode a persisted instance (`None` when absent).
    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic>;

    /// The live instance carrying alias `(name, value)` — relay correlation.
    async fn find_live_alias(
        &self,
        deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic>;

    /// **Terminal step (wait→end).** Resolve every wait row, retire every live alias, enqueue the
    /// step's emissions, and record the instance as finished — ONE transaction, commit or nothing.
    ///
    /// "Record as finished" is deliberately not "delete". Deleting was the original shape, and it
    /// made a completed instance unanswerable the instant it completed: `GET /sutra/instances/{id}`
    /// returned 404 for the successful case and 200 for the failed one, which is exactly backwards
    /// from what an operator expects to be able to look up. The durable implementation therefore
    /// RETAINS the row with its stored snapshot re-stamped `COMPLETED` (a key-patch of the persisted
    /// bytes, so at-rest encryption rides through and no key material is needed to finish an
    /// instance) plus a `terminal_at` marker, and a retention sweeper purges it once
    /// `sutra.instance.retention` (default `P7D`) elapses. Setting that key to `PT0S` restores the
    /// delete for operators who want it.
    ///
    /// Retention is an implementation concern of the durable bridge, not of this trait: a bridge
    /// with no history to keep (the in-memory doubles the channel tests drive) may simply drop the
    /// instance. What the trait DOES require either way is that the instance stops being resumable
    /// — both resume paths fail closed on a non-SUSPENDED status, so a retained row can never be
    /// re-driven.
    async fn commit_complete(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic>;

    /// Re-park step (wait→wait): overwrite the snapshot with the new frontier, resolve
    /// every satisfied wait node (the relayed/fired node plus any timer rows it cancels),
    /// record the new wait/timer/alias rows, enqueue the step's emissions — one
    /// transaction.
    #[allow(clippy::too_many_arguments)]
    async fn commit_repark(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        satisfied_wait_nodes: &[String],
        aliases: &[AliasRecord],
        timer_waits: &[TimerWaitRecord],
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic>;

    /// Sync-path terminal step (start→end, no park): the completed activation's collected
    /// emissions enqueue in ONE transaction. No snapshot row lands — the instance never
    /// parked — but the atomicity rule is the same: all emissions commit, or none.
    async fn commit_emissions(
        &self,
        deployment: &DeploymentId,
        emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic>;

    /// Take per-instance OWNERSHIP before rehydrating it — the guard that stops two
    /// replicas resuming the same parked instance from a relay and a timer at once.
    ///
    /// Called by both resume paths BEFORE the snapshot is loaded, so a winner reads the
    /// frontier the loser can no longer be moving. [`InstanceClaimOutcome::HeldByOther`] is
    /// reported when the underlying CAS matched no row, which also covers "the row is
    /// gone"; the caller disambiguates by re-reading ([`Self::load`]) so a completed
    /// instance keeps its permanent not-found posture instead of bouncing as contention.
    ///
    /// The default is the CLAIM-LESS posture: a bridge with no ownership column (the
    /// in-memory doubles the channel tests drive, and any single-replica embedding) grants
    /// every claim, which is exactly the pre-ownership behaviour. The durable
    /// `sutra-persistence` bridge overrides it.
    async fn claim_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        let _ = (deployment, instance_id);
        Ok(InstanceClaimOutcome::Granted)
    }

    /// Hand a claim back outside the step transaction — the resume paths' drop-guard belt
    /// for every exit that does NOT commit a step (a correlation/rehydrate rejection, an
    /// executor failure, a commit that rolled back). Owner-scoped in the implementation, so
    /// firing redundantly after a step already released in-transaction is a harmless no-op
    /// and can never clear a successor's claim. Default: a no-op (claim-less bridges).
    async fn release_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<(), Diagnostic> {
        let _ = (deployment, instance_id);
        Ok(())
    }

    /// **Failure step (wait→dead).** A resumed instance's step failed FATALLY — an executor
    /// `Uncaught`/`Diag` signal, never a BPMN error (a BPMN error routes to its boundary/event
    /// sub-process and only reaches here once genuinely uncaught). Persist the instance's LAST
    /// durable state re-stamped `sutra.status=FAILED` with the causing diagnostic, and resolve
    /// every one of its outstanding wait rows — one transaction, commit or nothing.
    ///
    /// Shape, and why each half matters:
    /// - **The FAILED snapshot** replaces the silent hole this used to leave. Before it, a fatal
    ///   resume persisted *nothing*: the instance sat at its previous wait frontier looking healthy,
    ///   and only a log line recorded that it had died. Now `GET /sutra/instances/{id}` reports
    ///   `FAILED` and names the code. The implementation re-stamps the STORED bytes (status +
    ///   failure keys) rather than re-encoding a snapshot it holds in memory — so the record is
    ///   exactly the state the engine last durably knew, and at-rest encryption is carried through
    ///   untouched. Hence no snapshot argument: the durable row IS the input.
    /// - **Resolving the waits** is what stops the corpse from twitching: an unresolved TIMER row
    ///   would be re-claimed by the poller forever, and the instance would look resumable. Both
    ///   resume paths additionally fail closed on the status itself
    ///   ([`INSTANCE_STATUS_FAILED`] → `SUTRA.DISPATCH.INSTANCE_FAILED`).
    /// - **Aliases are deliberately NOT retired.** The business key stays bound to the failed
    ///   instance so a later relay still correlates to it and gets the honest
    ///   `INSTANCE_FAILED` answer instead of a "no live instance" miss — and so a unique key is not
    ///   silently handed to a fresh instance while the failed one awaits a human. Admin
    ///   cancel is the release valve (admin retry/undo of a FAILED instance is not in this surface).
    /// - **The step's emissions are dropped.** The failing step never reached a quiescent point, so
    ///   nothing it collected may be delivered — the strict transactional outbox, unchanged.
    async fn commit_failed(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        failure_code: &str,
        detail: &str,
    ) -> Result<(), Diagnostic>;

    /// Does a TERMINALLY POISONED outbox row exist for this exact `(instance, node)`? The
    /// durable evidence gate of the channel-call `<q:retry>` poison wake: the engine never
    /// fails a parked call on the in-process notification alone — a wake without a poisoned
    /// row (a stale or misrouted prompt) must be a no-op. Default `false`: a bridge with no
    /// outbox knowledge can never validate a poison, so the wake safely does nothing.
    async fn poisoned_call_emission_exists(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        node_id: &str,
    ) -> Result<bool, Diagnostic> {
        let _ = (deployment, instance_id, node_id);
        Ok(false)
    }
}
