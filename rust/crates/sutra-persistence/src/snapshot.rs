//! Instance-snapshot codec — the byte-deterministic encoding of one in-flight instance.
//!
//! Container: Properties-line text ([`crate::props`]) — `key=value` lines with the
//! Properties-line escaping rules (part of the persisted format), sorted byte-wise
//! ascending, no comment lines, trailing `\n` per line. Determinism is normative: identical
//! logical state produces identical bytes; the reference baseline writes this format
//! natively and the round-trip tests must be byte-identical.
//!
//! Keys (v2) — the key names are frozen; changing one is a persisted-format break:
//!
//! | key | presence |
//! |---|---|
//! | `sutra.snapshot` | always (`2`) |
//! | `sutra.deploymentId` | always |
//! | `sutra.processId` | always |
//! | `sutra.status` | always |
//! | `sutra.completedNodes` | always (comma-joined, may be empty) |
//! | `sutra.waitingNodes` | when suspended (non-empty frontier) |
//! | `sutra.startNode` | when set |
//! | `sutra.auditSeq` | when > 0 |
//! | `sutra.failureCode` | when the instance is FAILED (the fatal step's stable `SUTRA.*` code) |
//! | `sutra.failureDetail` | when the instance is FAILED and a message was captured |
//! | `sutra.sensitive` | when any sensitive names are marked (comma-joined, sorted) |
//! | `sutra.var.<name>` | per variable (string form at v2/v3; TYPED from v4 — see below) |
//! | `sutra.coverage.<pathId>` | per declared path with counter > 0 |
//! | `sutra.retry.<nodeId>` | per `<q:retry>` task with failed attempts > 0 |
//!
//! Transient variables are NEVER serialized — the caller simply omits them from
//! [`InstanceSnapshot::variables`]; there is no key to suppress here.
//!
//! # `sutra.snapshot` — the version integer, and what each generation means
//!
//! Version detection at DECODE is the whole compatibility story; nothing is ever migrated in
//! place, and every generation below stays readable for as long as a row carrying it can exist.
//!
//! | value | meaning |
//! |---|---|
//! | `2` | plaintext container; every `sutra.var.<name>` value is an untyped display string |
//! | `3` | same value model, and at least one variable is `sutra.enc.<name>` ciphertext |
//! | `4` | TYPED values: every variable value — plaintext, and the plaintext INSIDE each `sutra.enc.` envelope — carries the [`crate::value`] tag form |
//!
//! The `2` → `3` step folded "is anything encrypted" into the integer. That conflation stops at
//! `4`: encryption has always been detected STRUCTURALLY (the `sutra.enc.` key prefix — `read`,
//! `peek` and `peek_key_id` never consult the version at all), so `4` means one thing only, the
//! value encoding, and an encrypted snapshot with typed values is simply `4`. The engine-internal
//! key families (`sutra.status`, `sutra.startNode`, `sutra.retry.*`, `sutra.coverage.*`,
//! `sutra.enc.*`, …) are unaffected by typing at every generation — they were never user data.
//!
//! The writer bumps the version only when it must: `4` when at least one variable needs the typed
//! form, else `3` when something was encrypted, else `2`. So an instance whose variables are all
//! strings still writes the exact bytes it wrote before typing existed — the golden-bytes corpus
//! below is untouched, and no already-parked row changes shape when it re-parks.

use std::collections::{BTreeMap, BTreeSet};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use sutra_crypto::{CipherError, PayloadCipher};

use crate::props;
use crate::value::SnapshotValue;

/// Instance status strings — part of the persisted format. Kept as strings — the engine round-trips
/// unrecognised values verbatim, and byte-determinism must not depend on an enum whitelist.
pub const STATUS_RUNNING: &str = "RUNNING";
/// See [`STATUS_RUNNING`].
pub const STATUS_SUSPENDED: &str = "SUSPENDED";
/// See [`STATUS_RUNNING`].
pub const STATUS_COMPLETED: &str = "COMPLETED";
/// See [`STATUS_RUNNING`].
pub const STATUS_TERMINATED: &str = "TERMINATED";
/// The terminal-failure status: a step of this instance failed FATALLY (an executor
/// `Uncaught`/`Diag` signal — a BPMN error that routes to a boundary never gets here) after the
/// instance had already been durably parked. The row survives at its last quiescent frontier with
/// this marker so the failure is *visible* rather than silent, and every resume path (relay + timer)
/// fails closed against it (`SUTRA.DISPATCH.INSTANCE_FAILED`). Written by the failure commit shape
/// beside the re-park step; see [`InstanceSnapshot::with_failure`].
pub const STATUS_FAILED: &str = "FAILED";

/// The v2 format version integer written under `sutra.snapshot` — the baseline: plaintext values,
/// untyped.
pub const FORMAT_VERSION: u32 = 2;

/// The v3 format version — written under `sutra.snapshot` when the snapshot carries at least one
/// `sutra.enc.<name>` (AES-256-GCM at-rest) value and nothing needs the typed form. A snapshot with
/// no encrypted variables stays v2 and byte-identical to the pre-encryption form, so the
/// byte-determinism contract (and the golden-bytes corpus) is untouched for the plaintext case.
/// Encrypted values are inherently non-deterministic (random GCM nonce), so byte-determinism is
/// exempt for them — the encrypted round-trip contract is DECRYPT-equality, not byte-equality.
pub const FORMAT_VERSION_ENCRYPTED: u32 = 3;

/// The v4 format version — TYPED variable values ([`crate::value::SnapshotValue`]). Written when at
/// least one variable needs a form a display string cannot carry; see the module docs for why this
/// generation does not also encode "is anything encrypted" (it never had to — that has always been
/// read off the `sutra.enc.` key prefix).
pub const FORMAT_VERSION_TYPED: u32 = 4;

const K_SNAPSHOT_VERSION: &str = "sutra.snapshot";
const K_DEPLOYMENT_ID: &str = "sutra.deploymentId";
const K_PROCESS_ID: &str = "sutra.processId";
const K_STATUS: &str = "sutra.status";
const K_COMPLETED: &str = "sutra.completedNodes";
const K_WAITING: &str = "sutra.waitingNodes";
const K_START_NODE: &str = "sutra.startNode";
const K_AUDIT_SEQ: &str = "sutra.auditSeq";
const K_SENSITIVE: &str = "sutra.sensitive";
/// The stable `SUTRA.*` diagnostic code of the fatal step that FAILED this instance. Emitted only
/// for a [`STATUS_FAILED`] snapshot, so every pre-existing (RUNNING/SUSPENDED/COMPLETED) snapshot
/// stays byte-identical to the form it had before failure state existed.
const K_FAILURE_CODE: &str = "sutra.failureCode";
/// The fatal step's human message. Same emit-only-when-present rule as [`K_FAILURE_CODE`]. NOTE the
/// value can quote business data lifted from the failing expression/task, so it is treated as
/// admin-grade: the unauthenticated operate projection surfaces the CODE only.
const K_FAILURE_DETAIL: &str = "sutra.failureDetail";
const K_VAR_PREFIX: &str = "sutra.var.";
/// A variable persisted as AES-256-GCM ciphertext (base64) instead of plaintext `sutra.var.<name>`
/// — the snapshot v3 at-rest form for a sensitive/redactor-controlled variable.
const K_ENC_PREFIX: &str = "sutra.enc.";
const K_COVERAGE_PREFIX: &str = "sutra.coverage.";
/// Per-node FAILED-ATTEMPT counter for a `<q:retry>` service task — the durable half of the
/// per-task retry policy (P1-1).
///
/// Why a snapshot key and not a `waiting_event` column: the counter is INSTANCE state, not wait
/// state. It must survive the wait row being resolved and re-created on every re-park (the retry
/// park resolves its own timer row and writes a fresh one each attempt, so a column on that row
/// would be erased by the very step that increments it), it must ride the FAILED re-stamp
/// untouched like every other frontier fact (`with_failure` carries the whole snapshot through),
/// and it must reach the executor through the one channel that already carries instance state —
/// the decoded snapshot. It also costs no migration and no dialect work: the Properties container
/// is open-keyed, and an absent key reads as zero, so every pre-P1-1 snapshot decodes unchanged.
/// The `sutra.coverage.<pathId>` cursor family is the precedent this mirrors exactly.
const K_RETRY_PREFIX: &str = "sutra.retry.";
/// Per-node BACKOFF-WINDOW marker for a CHANNEL-CALL `<q:retry>` task (F1 — retry
/// reachability): the value is the classification code of the failure that parked the node
/// (`SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT`, `SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED`).
///
/// Why it exists at all: a channel-call node in a backoff window and one whose attempt is IN
/// FLIGHT both sit in `sutra.waitingNodes` with a non-zero `sutra.retry.<nodeId>` count — the
/// two states are indistinguishable through the pre-existing keys, yet they demand opposite
/// treatment (a due timer on the node is the RE-DRIVE vs. stale; a correlated relay is REFUSED
/// vs. resumes). Same open-keyed Properties rationale as [`K_RETRY_PREFIX`]: no migration, no
/// dialect work, absent = not in backoff, and a process that never backoff-parks a channel
/// call writes byte-identical snapshots to the pre-F1 form. NOTE the prefix shares no
/// namespace with `sutra.retry.` (`retryWait` ≠ `retry` + `.`), so neither decoder can claim
/// the other's keys.
const K_RETRY_WAIT_PREFIX: &str = "sutra.retryWait.";
/// The migration-stable crypto anchor — the `keyId` (tenant label) used to derive the
/// DEK and build the AAD for this snapshot's `sutra.enc.` values. Written ONLY when at least one
/// value is encrypted; stored plaintext (it is not secret) so `read`/`load` rebuilds the cipher
/// without an external tenant lookup, and it survives a version migration (which changes only
/// `deployment_id`).
const K_KEY_ID: &str = "sutra.keyId";

/// The AAD field separator (ASCII Unit Separator) binding a ciphertext to `keyId ⋮ instanceId ⋮
/// varName`. A byte that cannot occur in a key_id / instance-uuid / variable-name keeps the three
/// AAD fields unambiguous.
const AAD_SEP: u8 = 0x1f;

/// Decoded snapshot state, one in-flight instance.
///
/// `Eq` is deliberately absent: a variable can be a FEEL number, whose equality is DECIMAL VALUE
/// equality (`1.0` equals `1.00`) and therefore not reflexive-by-representation the way `Eq`
/// promises. Snapshots are compared in tests and never keyed by, so `PartialEq` is the whole need.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceSnapshot {
    process_id: String,
    deployment_id: String,
    status: String,
    completed_nodes: Vec<String>,
    /// Typed from v4 on; a v2/v3 snapshot decodes every entry as [`SnapshotValue::String`], which
    /// is exactly the state it always had.
    variables: BTreeMap<String, SnapshotValue>,
    waiting_nodes: Vec<String>,
    start_node: String,
    audit_seq: u32,
    sensitive: Vec<String>,
    coverage: BTreeMap<String, u64>,
    /// Per `<q:retry>` node id, how many attempts of that task have already FAILED — the durable
    /// attempt count the retry park persists (`sutra.retry.<nodeId>`). Absent/zero entries are not
    /// written, so a process with no retry policy produces byte-identical snapshots to the
    /// pre-P1-1 form.
    retry_attempts: BTreeMap<String, u32>,
    /// Per CHANNEL-CALL `<q:retry>` node id currently in a BACKOFF window, the classification
    /// code of the failure that parked it (`sutra.retryWait.<nodeId>`). Empty entries are not
    /// written — see [`K_RETRY_WAIT_PREFIX`].
    retry_backoff: BTreeMap<String, String>,
    /// The fatal step's diagnostic code + message, present only on a [`STATUS_FAILED`] snapshot.
    /// Round-tripped verbatim so a FAILED instance keeps naming its cause across reads.
    failure_code: String,
    failure_detail: String,
}

/// Routing keys peeked from persisted bytes without resuming (mirrors
/// `SuspendedInstanceCodec.peek` / `ResumeKeys`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeKeys {
    /// Archive-local process id.
    pub process_id: String,
    /// The instance's pinned deployment id (the rehydration resolution key).
    pub deployment_id: String,
    /// Persisted status string (for diagnostics when not suspended).
    pub status: String,
    /// Whether the snapshot is SUSPENDED (resume requires this).
    pub suspended: bool,
    /// Per-instance monotonic audit seq at suspend; 0 when none was captured.
    pub audit_seq: u32,
}

/// Every NODE ID a persisted snapshot pins, read WITHOUT decrypting anything — the durable half of
/// the admin instance-migration validator's input.
///
/// Deliberately separate from [`ResumeKeys`]: that shape is the ROUTING peek (which deployment,
/// which process, is it resumable) and is frozen. This one is the LOCUS peek, and it must work on
/// an encrypted v3 snapshot with no cipher in sight — a migration is a structural operation on node
/// ids and has no business decrypting an instance's variables to validate one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotLoci {
    /// Archive-local process id (`sutra.processId`).
    pub process_id: String,
    /// The instance's current pin (`sutra.deploymentId`).
    pub deployment_id: String,
    /// Persisted status string (`sutra.status`).
    pub status: String,
    /// The wait frontier (`sutra.waitingNodes`).
    pub waiting_nodes: Vec<String>,
    /// The replay-as-done set (`sutra.completedNodes`).
    pub completed_nodes: Vec<String>,
    /// The routed start event (`sutra.startNode`; empty when unset).
    pub start_node: String,
    /// Node ids carrying a `<q:retry>` failed-attempt counter (`sutra.retry.<nodeId>` with a
    /// counter above zero — a zero/malformed counter reads as "no attempts yet", exactly as the
    /// decode contract says, so it pins nothing).
    pub retry_nodes: BTreeSet<String>,
    /// Per-instance monotonic audit seq (`sutra.auditSeq`); 0 when none was captured.
    pub audit_seq: u32,
}

/// The at-rest cipher plus the AAD context for a snapshot's encrypted variables.
/// The AAD binds each ciphertext to `key_id ⋮ instance_id ⋮ variable-name` — none of which is
/// `deployment_id`, so a version migration (which changes only `deployment_id`) still decrypts,
/// while a cross-tenant/instance/field swap fails closed. The SAME context (key_id + instance_id)
/// must be supplied to [`InstanceSnapshot::read_encrypted`] as was used to write it.
pub struct SnapshotCrypto<'a> {
    cipher: &'a dyn PayloadCipher,
    key_id: &'a str,
    instance_id: &'a str,
}

impl<'a> SnapshotCrypto<'a> {
    pub fn new(cipher: &'a dyn PayloadCipher, key_id: &'a str, instance_id: &'a str) -> Self {
        SnapshotCrypto {
            cipher,
            key_id,
            instance_id,
        }
    }

    /// The AAD for `var_name`: `key_id ⋮ instance_id ⋮ var_name` (⋮ = [`AAD_SEP`]).
    fn aad(&self, var_name: &str) -> Vec<u8> {
        let mut aad =
            Vec::with_capacity(self.key_id.len() + self.instance_id.len() + var_name.len() + 2);
        aad.extend_from_slice(self.key_id.as_bytes());
        aad.push(AAD_SEP);
        aad.extend_from_slice(self.instance_id.as_bytes());
        aad.push(AAD_SEP);
        aad.extend_from_slice(var_name.as_bytes());
        aad
    }
}

impl InstanceSnapshot {
    /// Factory for a non-suspended snapshot.
    ///
    /// `variables` is the STRING-valued shape — the v2 value model, and still the whole truth for
    /// callers that only ever hold display strings. Typed variables are supplied by
    /// [`with_variables`](Self::with_variables); keeping this signature string-valued is what lets
    /// every existing caller (and the three dialect suites) stay exactly as they were.
    pub fn of(
        process_id: impl Into<String>,
        deployment_id: impl Into<String>,
        status: impl Into<String>,
        completed_nodes: Vec<String>,
        variables: BTreeMap<String, String>,
    ) -> Self {
        Self {
            process_id: process_id.into(),
            deployment_id: deployment_id.into(),
            status: status.into(),
            completed_nodes,
            variables: string_variables(variables),
            waiting_nodes: Vec::new(),
            start_node: String::new(),
            audit_seq: 0,
            sensitive: Vec::new(),
            coverage: BTreeMap::new(),
            retry_attempts: BTreeMap::new(),
            retry_backoff: BTreeMap::new(),
            failure_code: String::new(),
            failure_detail: String::new(),
        }
    }

    /// S-X1b factory for a **suspended** instance — wait frontier + routed start node +
    /// audit seq. `variables` is string-valued for the reason [`of`](Self::of) documents.
    pub fn of_suspended(
        process_id: impl Into<String>,
        deployment_id: impl Into<String>,
        completed_nodes: Vec<String>,
        variables: BTreeMap<String, String>,
        waiting_nodes: Vec<String>,
        start_node: impl Into<String>,
        audit_seq: u32,
    ) -> Self {
        Self {
            process_id: process_id.into(),
            deployment_id: deployment_id.into(),
            status: STATUS_SUSPENDED.to_owned(),
            completed_nodes,
            variables: string_variables(variables),
            waiting_nodes,
            start_node: start_node.into(),
            audit_seq,
            sensitive: Vec::new(),
            coverage: BTreeMap::new(),
            retry_attempts: BTreeMap::new(),
            retry_backoff: BTreeMap::new(),
            failure_code: String::new(),
            failure_detail: String::new(),
        }
    }

    /// Re-stamp this snapshot as the FAILED terminal-failure record of a fatal step: status
    /// [`STATUS_FAILED`] plus the causing diagnostic. Everything else — the frontier, the
    /// completed set, the variables, the coverage counters — is carried through UNCHANGED: the
    /// point of the record is that an operator sees exactly where the instance died, on the last
    /// state the engine durably knew.
    ///
    /// Used by the failure commit shape beside the re-park step (`commit_failed`), never on the
    /// happy path. `detail` is stored verbatim; callers that surface it must treat it as
    /// admin-grade (it can quote business data — see [`K_FAILURE_DETAIL`]).
    #[must_use]
    pub fn with_failure(mut self, code: impl Into<String>, detail: impl Into<String>) -> Self {
        self.status = STATUS_FAILED.to_owned();
        self.failure_code = code.into();
        self.failure_detail = detail.into();
        self
    }

    /// Replaces the variables with their TYPED forms — the park path's entry point into snapshot
    /// v4. A map whose values are all [`SnapshotValue::String`] is indistinguishable from the
    /// string-valued factories, and writes the identical v2/v3 bytes.
    #[must_use]
    pub fn with_variables(mut self, variables: BTreeMap<String, SnapshotValue>) -> Self {
        self.variables = variables;
        self
    }

    /// Marks variable names as sensitive: values persist (resume needs them) but
    /// audit/log layers must redact them. Names are stored sorted + deduplicated so the
    /// emitted `sutra.sensitive` list is canonical.
    #[must_use]
    pub fn with_sensitive(mut self, mut names: Vec<String>) -> Self {
        names.sort_unstable();
        names.dedup();
        names.retain(|n| !n.is_empty());
        self.sensitive = names;
        self
    }

    /// Sets the bounded coverage counters: matched-prefix counter per declared
    /// path id. Zero counters are dropped — an untouched path emits no key, keeping the
    /// byte form canonical.
    #[must_use]
    pub fn with_coverage(mut self, coverage: BTreeMap<String, u64>) -> Self {
        self.coverage = coverage
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .collect();
        self
    }

    /// Sets the per-node failed-attempt counters of the `<q:retry>` tasks this instance has
    /// already re-driven (`sutra.retry.<nodeId>`). Zero counters are dropped — a task that has
    /// not failed yet emits no key, so a process whose retried tasks all succeeded first time is
    /// byte-identical to one with no retry policy at all.
    #[must_use]
    pub fn with_retry_attempts(mut self, retry_attempts: BTreeMap<String, u32>) -> Self {
        self.retry_attempts = retry_attempts
            .into_iter()
            .filter(|(_, attempts)| *attempts > 0)
            .collect();
        self
    }

    /// Sets the channel-call BACKOFF-WINDOW markers (`sutra.retryWait.<nodeId>` — the parking
    /// failure's classification code per node). Empty codes are dropped — a node not in a
    /// backoff window emits no key, so a process that never backoff-parks a channel call is
    /// byte-identical to the pre-F1 form.
    #[must_use]
    pub fn with_retry_backoff(mut self, retry_backoff: BTreeMap<String, String>) -> Self {
        self.retry_backoff = retry_backoff
            .into_iter()
            .filter(|(_, code)| !code.trim().is_empty())
            .collect();
        self
    }

    /// Decodes snapshot bytes with no cipher — every generation, as long as nothing is encrypted.
    /// Absent keys default per the decode contract. A snapshot carrying `sutra.enc.` (encrypted)
    /// values requires [`read_encrypted`](Self::read_encrypted) with a cipher — `read` fails closed
    /// on one.
    pub fn read(bytes: &[u8]) -> Result<Self, String> {
        Self::read_encrypted(bytes, None)
    }

    /// Decodes snapshot bytes, decrypting each `sutra.enc.<name>` value with the supplied `crypto`
    /// context back into its plaintext variable. Fails closed: an encrypted value
    /// with no cipher, malformed ciphertext, a decrypt/authentication failure, or non-UTF-8
    /// plaintext is a hard error — the raw ciphertext is never surfaced and the variable is never
    /// silently dropped. Plaintext `sutra.var.` values decode as before, so a v2 snapshot reads
    /// with `crypto = None`. Absent keys default per the decode contract; malformed integers
    /// read as 0.
    ///
    /// Values are TAG-DECODED ([`crate::value`]) only when `sutra.snapshot` says v4 or later — the
    /// version gate is what keeps every v2/v3 row loading with its values byte-for-byte as strings.
    /// A decrypted `sutra.enc.` plaintext goes through the same gate as a plaintext value: at v4 the
    /// ciphertext envelope carries the TAGGED form, so an encrypted variable restores typed too.
    pub fn read_encrypted(bytes: &[u8], crypto: Option<&SnapshotCrypto>) -> Result<Self, String> {
        let map = Self::raw_map(bytes)?;
        let typed = map
            .get(K_SNAPSHOT_VERSION)
            .map(|v| v.trim().parse::<u32>().unwrap_or(FORMAT_VERSION))
            .is_some_and(|v| v >= FORMAT_VERSION_TYPED);
        let decode_value = |raw: &str| -> SnapshotValue {
            if typed {
                SnapshotValue::decode(raw)
            } else {
                SnapshotValue::String(raw.to_owned())
            }
        };

        let get = |key: &str| map.get(key).cloned().unwrap_or_default();
        let split_list = |raw: &str| -> Vec<String> {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        };

        let mut variables = BTreeMap::new();
        let mut coverage = BTreeMap::new();
        let mut retry_attempts = BTreeMap::new();
        let mut retry_backoff = BTreeMap::new();
        for (key, value) in &map {
            if let Some(name) = key.strip_prefix(K_VAR_PREFIX) {
                variables.insert(name.to_owned(), decode_value(value));
            } else if let Some(name) = key.strip_prefix(K_ENC_PREFIX) {
                // Encrypted at rest — decrypt with the supplied cipher; fail closed on a missing
                // cipher or ANY decode/decrypt failure (never surface ciphertext, never drop).
                let c = crypto.ok_or_else(|| {
                    format!(
                        "snapshot variable '{name}' is encrypted (sutra.enc.) but no cipher was \
                         supplied to decrypt it"
                    )
                })?;
                let ciphertext = B64.decode(value).map_err(|e| {
                    format!("snapshot variable '{name}' has malformed base64 ciphertext: {e}")
                })?;
                let plaintext = c.cipher.decrypt(&ciphertext, &c.aad(name)).map_err(|e| {
                    format!("snapshot variable '{name}' failed to decrypt (fail-closed): {e}")
                })?;
                let text = String::from_utf8(plaintext).map_err(|e| {
                    format!("snapshot variable '{name}' decrypted to non-UTF-8 bytes: {e}")
                })?;
                variables.insert(name.to_owned(), decode_value(&text));
            } else if let Some(path_id) = key.strip_prefix(K_COVERAGE_PREFIX) {
                let count = value.trim().parse::<u64>().unwrap_or(0);
                if count > 0 {
                    coverage.insert(path_id.to_owned(), count);
                }
            } else if let Some(node_id) = key.strip_prefix(K_RETRY_WAIT_PREFIX) {
                // Checked BEFORE the `sutra.retry.` arm purely for reading clarity — the
                // prefixes cannot claim each other's keys (`retryWait` ≠ `retry` + `.`).
                // Decode contract: a blank marker reads as "not in a backoff window" (the
                // safe direction — the node then behaves as attempt-in-flight, and its due
                // backoff row resolves as stale rather than double-driving anything).
                if !value.trim().is_empty() {
                    retry_backoff.insert(node_id.to_owned(), value.trim().to_owned());
                }
            } else if let Some(node_id) = key.strip_prefix(K_RETRY_PREFIX) {
                // Same decode contract as the coverage cursors: a malformed or zero counter reads
                // as "no attempts yet", which restarts the curve rather than failing a resume.
                let attempts = value.trim().parse::<u32>().unwrap_or(0);
                if attempts > 0 {
                    retry_attempts.insert(node_id.to_owned(), attempts);
                }
            }
        }

        let status = map
            .get(K_STATUS)
            .cloned()
            .unwrap_or_else(|| STATUS_RUNNING.to_owned());
        let audit_seq = get(K_AUDIT_SEQ).trim().parse::<u32>().unwrap_or(0);
        let mut sensitive = split_list(&get(K_SENSITIVE));
        sensitive.sort_unstable();
        sensitive.dedup();

        Ok(Self {
            process_id: get(K_PROCESS_ID),
            deployment_id: get(K_DEPLOYMENT_ID),
            status,
            completed_nodes: split_list(&get(K_COMPLETED)),
            variables,
            waiting_nodes: split_list(&get(K_WAITING)),
            start_node: get(K_START_NODE),
            audit_seq,
            sensitive,
            coverage,
            retry_attempts,
            retry_backoff,
            failure_code: get(K_FAILURE_CODE),
            failure_detail: get(K_FAILURE_DETAIL),
        })
    }

    /// Encodes to the canonical plaintext byte form. Byte-deterministic: identical logical
    /// state produces identical bytes (contract-normative; rollback/compat tests rely on it).
    /// For at-rest encryption use [`write_encrypted`](Self::write_encrypted).
    pub fn write(&self) -> Vec<u8> {
        self.write_encrypted(None, &BTreeSet::new())
            .expect("a plaintext snapshot write (no cipher) is infallible")
    }

    /// Encodes to the canonical byte form, encrypting each variable named in `encrypt_names` as
    /// `sutra.enc.<name>=<base64 AES-256-GCM>` when a `crypto` context is supplied.
    /// Variables not in `encrypt_names` — and every variable when `crypto` is `None` — stay
    /// plaintext `sutra.var.<name>`. The version is the lowest generation that can carry this
    /// state: [`FORMAT_VERSION_TYPED`] when any variable needs the typed form, else
    /// [`FORMAT_VERSION_ENCRYPTED`] when any value was encrypted, else [`FORMAT_VERSION`] — so a
    /// plaintext all-string snapshot is byte-identical to the form it had before either feature
    /// existed. The typed form is applied UNIFORMLY once the generation is v4, encrypted values
    /// included (their tag rides INSIDE the ciphertext), because a decoder cannot tell a tagged
    /// value from an untagged one per-key — only per-snapshot, off the version.
    /// **Fails closed:** a cipher error aborts the write rather than persist the value in the clear.
    pub fn write_encrypted(
        &self,
        crypto: Option<&SnapshotCrypto>,
        encrypt_names: &BTreeSet<String>,
    ) -> Result<Vec<u8>, CipherError> {
        let completed = self.completed_nodes.join(",");
        let waiting = self.waiting_nodes.join(",");
        let audit_seq = self.audit_seq.to_string();
        let sensitive = self.sensitive.join(",");
        let coverage: Vec<(String, String)> = self
            .coverage
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(path, count)| (format!("{K_COVERAGE_PREFIX}{path}"), count.to_string()))
            .collect();
        let retries: Vec<(String, String)> = self
            .retry_attempts
            .iter()
            .filter(|(_, attempts)| **attempts > 0)
            .map(|(node, attempts)| (format!("{K_RETRY_PREFIX}{node}"), attempts.to_string()))
            .collect();
        let retry_waits: Vec<(String, String)> = self
            .retry_backoff
            .iter()
            .filter(|(_, code)| !code.trim().is_empty())
            .map(|(node, code)| (format!("{K_RETRY_WAIT_PREFIX}{node}"), code.clone()))
            .collect();

        // The generation is decided BEFORE any value is rendered: it selects the value encoding,
        // and the encrypt path has to bake the same choice into the ciphertext.
        let typed = self
            .variables
            .values()
            .any(SnapshotValue::needs_typed_encoding);
        let render = |value: &SnapshotValue| -> String {
            if typed {
                value.encode()
            } else {
                // Only reachable for a String at this point — every other kind forces `typed`.
                value.to_canonical_string()
            }
        };

        // Partition variables: ciphertext (`sutra.enc.`) for the encrypt-set when a cipher is
        // present, plaintext (`sutra.var.`) otherwise. Owned strings so the borrows below outlive
        // `entries`.
        let mut var_pairs: Vec<(String, String)> = Vec::new();
        let mut enc_pairs: Vec<(String, String)> = Vec::new();
        for (name, value) in &self.variables {
            let rendered = render(value);
            match crypto {
                Some(c) if encrypt_names.contains(name) => {
                    let ciphertext = c.cipher.encrypt(rendered.as_bytes(), &c.aad(name))?;
                    enc_pairs.push((format!("{K_ENC_PREFIX}{name}"), B64.encode(ciphertext)));
                }
                _ => var_pairs.push((format!("{K_VAR_PREFIX}{name}"), rendered)),
            }
        }
        let version = if typed {
            FORMAT_VERSION_TYPED
        } else if enc_pairs.is_empty() {
            FORMAT_VERSION
        } else {
            FORMAT_VERSION_ENCRYPTED
        }
        .to_string();

        let mut entries: Vec<(&str, &str)> = vec![
            (K_SNAPSHOT_VERSION, version.as_str()),
            (K_DEPLOYMENT_ID, self.deployment_id.as_str()),
            (K_PROCESS_ID, self.process_id.as_str()),
            (K_STATUS, self.status.as_str()),
            (K_COMPLETED, completed.as_str()),
        ];
        // Self-describing crypto anchor: emit the keyId ONLY when something was encrypted, so
        // `read`/`load` can rebuild the DEK + AAD without an external tenant lookup, and a plaintext
        // snapshot stays byte-identical to the v2 form (no vestigial key).
        if !enc_pairs.is_empty() {
            if let Some(c) = crypto {
                entries.push((K_KEY_ID, c.key_id));
            }
        }
        // Suspended-only keys are emitted only when present, so a non-suspended snapshot
        // writes no vestigial empty keys (byte-determinism).
        if !self.waiting_nodes.is_empty() {
            entries.push((K_WAITING, waiting.as_str()));
        }
        if !self.start_node.is_empty() {
            entries.push((K_START_NODE, self.start_node.as_str()));
        }
        if self.audit_seq > 0 {
            entries.push((K_AUDIT_SEQ, audit_seq.as_str()));
        }
        if !self.sensitive.is_empty() {
            entries.push((K_SENSITIVE, sensitive.as_str()));
        }
        // Failure keys follow the same emit-only-when-present rule, so a snapshot that never
        // failed is byte-identical to the pre-failure-state form (the golden-bytes corpus).
        if !self.failure_code.is_empty() {
            entries.push((K_FAILURE_CODE, self.failure_code.as_str()));
        }
        if !self.failure_detail.is_empty() {
            entries.push((K_FAILURE_DETAIL, self.failure_detail.as_str()));
        }
        for (k, v) in &coverage {
            entries.push((k.as_str(), v.as_str()));
        }
        for (k, v) in &retries {
            entries.push((k.as_str(), v.as_str()));
        }
        for (k, v) in &retry_waits {
            entries.push((k.as_str(), v.as_str()));
        }
        for (k, v) in &var_pairs {
            entries.push((k.as_str(), v.as_str()));
        }
        for (k, v) in &enc_pairs {
            entries.push((k.as_str(), v.as_str()));
        }
        Ok(props::write_lines(entries))
    }

    /// Peek at a persisted instance's routing keys without resuming it (mirrors
    /// `SuspendedInstanceCodec.peek`). Reads ONLY the non-encrypted routing metadata directly from
    /// the raw key/value map — never touches `sutra.enc.`/`sutra.var.`, so it works (fail-open) on
    /// an encrypted v3 snapshot WITHOUT a cipher, which routing legitimately needs.
    pub fn peek(bytes: &[u8]) -> Result<ResumeKeys, String> {
        let map = Self::raw_map(bytes)?;
        let status = map
            .get(K_STATUS)
            .cloned()
            .unwrap_or_else(|| STATUS_RUNNING.to_owned());
        let get = |key: &str| map.get(key).cloned().unwrap_or_default();
        Ok(ResumeKeys {
            suspended: status == STATUS_SUSPENDED,
            process_id: get(K_PROCESS_ID),
            deployment_id: get(K_DEPLOYMENT_ID),
            status,
            audit_seq: get(K_AUDIT_SEQ).trim().parse::<u32>().unwrap_or(0),
        })
    }

    /// Re-stamp PERSISTED BYTES as [`STATUS_FAILED`] with the causing diagnostic, touching nothing
    /// else — a key-level patch of the raw Properties map, not a decode/re-encode.
    ///
    /// That distinction is load-bearing. A decode/re-encode would have to DECRYPT every
    /// `sutra.enc.<name>` value and re-encrypt it, which means the failure path would need the
    /// tenant DEK, would re-derive the encrypt-set from a resume-time snapshot that no longer
    /// carries it, and would persist a previously-encrypted value in the clear the moment either
    /// went wrong. Patching the map instead carries `sutra.enc.*` and `sutra.keyId` through byte
    /// for byte: marking an instance dead can never downgrade its at-rest protection, and needs no
    /// key material at all. Output stays canonical (the writer re-sorts), so byte-determinism holds.
    pub fn mark_failed(bytes: &[u8], failure_code: &str, detail: &str) -> Result<Vec<u8>, String> {
        let mut map = Self::raw_map(bytes)?;
        map.insert(K_STATUS.to_owned(), STATUS_FAILED.to_owned());
        if failure_code.is_empty() {
            map.remove(K_FAILURE_CODE);
        } else {
            map.insert(K_FAILURE_CODE.to_owned(), failure_code.to_owned());
        }
        if detail.is_empty() {
            map.remove(K_FAILURE_DETAIL);
        } else {
            map.insert(K_FAILURE_DETAIL.to_owned(), detail.to_owned());
        }
        Ok(props::write_lines(
            map.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        ))
    }

    /// Re-stamp PERSISTED BYTES with a TERMINAL status — [`STATUS_COMPLETED`] (the process reached
    /// an end event) or [`STATUS_TERMINATED`] (an operator cancelled it) — by the same key-level
    /// patch of the raw Properties map that [`mark_failed`](Self::mark_failed) uses, and for the
    /// same reason: a terminal instance must never have to be DECRYPTED to be recorded as finished.
    ///
    /// This is what turns the terminal transaction from a DELETE into a retention: the row survives
    /// carrying the last state the engine durably knew, re-labelled with the verdict. Everything
    /// else rides through byte for byte — `sutra.enc.*`, `sutra.keyId`, the variables, the
    /// completed set, the coverage counters, and the wait frontier the instance was parked at when
    /// the terminal step ran. The frontier is kept deliberately (mirroring `mark_failed`): it is the
    /// only durable record of WHERE the instance was when it finished, and it can mislead nobody —
    /// the same transaction resolves every one of those wait rows, and every resume path fails
    /// closed on a non-SUSPENDED status.
    ///
    /// Honest limitation of the key-patch: the retained snapshot is the last PARKED state, not the
    /// terminal step's final variable values — the terminal step's own writes were never persisted
    /// (there was no quiescent point after them to persist at). Re-encoding them here would mean
    /// decrypting and re-encrypting every at-rest value on the completion path, which is exactly the
    /// trade `mark_failed` documents as unacceptable. Per-step fidelity lives in the audit journal
    /// (`GET /admin/instances/{id}/history`), not here.
    ///
    /// Fails closed on any status that is not one of the two terminal labels, so this can never be
    /// used to rewrite an instance into RUNNING/SUSPENDED behind the engine's back.
    pub fn mark_terminal(bytes: &[u8], status: &str) -> Result<Vec<u8>, String> {
        if status != STATUS_COMPLETED && status != STATUS_TERMINATED {
            return Err(format!(
                "mark_terminal accepts only {STATUS_COMPLETED} or {STATUS_TERMINATED}, not \
                 '{status}' (FAILED has its own commit shape — see mark_failed)"
            ));
        }
        let mut map = Self::raw_map(bytes)?;
        map.insert(K_STATUS.to_owned(), status.to_owned());
        Ok(props::write_lines(
            map.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        ))
    }

    /// Re-stamp a durably FAILED snapshot back to [`STATUS_SUSPENDED`] and DROP its failure keys —
    /// the snapshot half of the admin migrate-then-resume convenience (v2), by the same key-level
    /// patch of the raw Properties map that [`mark_failed`](Self::mark_failed) uses, in the reverse
    /// direction and for the same reason: bringing a dead instance back must never need the tenant
    /// DEK, and must never downgrade its at-rest protection.
    ///
    /// This is deliberately the exact INVERSE of `mark_failed` and nothing more:
    ///
    /// * `sutra.status` FAILED → SUSPENDED (the one status every resume path accepts).
    /// * `sutra.failureCode` / `sutra.failureDetail` removed, so the snapshot is byte-identical to
    ///   the form it had before the fatal step (the failure keys are emit-only-when-present).
    ///
    /// Everything else rides through byte for byte — in particular the wait FRONTIER (which is what
    /// the instance goes back to being parked at), the completed set, the `sutra.retry.<node>`
    /// budgets (a burned budget stays burned; resume is not a retry reset) and every variable.
    ///
    /// It does NOT re-arm the instance's `waiting_event` rows: the failure commit resolved them, and
    /// re-arming them is a ROW operation that belongs in the same transaction as the move
    /// ([`crate::step::commit_instance_migration`]'s `rearm_parks`). Snapshot and rows are re-armed
    /// together or not at all.
    ///
    /// Fails closed on any status but FAILED, so this can never be used to rewrite a COMPLETED,
    /// TERMINATED or already-live instance into a parked one behind the engine's back.
    pub fn resume_from_failed(bytes: &[u8]) -> Result<Vec<u8>, String> {
        let mut map = Self::raw_map(bytes)?;
        let status = map
            .get(K_STATUS)
            .map(String::as_str)
            .unwrap_or(STATUS_RUNNING);
        if status != STATUS_FAILED {
            return Err(format!(
                "resume_from_failed accepts only a {STATUS_FAILED} snapshot, not '{status}' — a \
                 non-failed instance has no failure state to clear and resumes by correlation or \
                 by its timers"
            ));
        }
        map.insert(K_STATUS.to_owned(), STATUS_SUSPENDED.to_owned());
        map.remove(K_FAILURE_CODE);
        map.remove(K_FAILURE_DETAIL);
        Ok(props::write_lines(
            map.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        ))
    }

    /// Peek every node id a persisted instance pins, WITHOUT decrypting a single variable — the
    /// locus half of [`peek`](Self::peek). See [`SnapshotLoci`] for why this is its own shape.
    pub fn peek_loci(bytes: &[u8]) -> Result<SnapshotLoci, String> {
        let map = Self::raw_map(bytes)?;
        let get = |key: &str| map.get(key).cloned().unwrap_or_default();
        let split_list = |raw: &str| -> Vec<String> {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        };
        let retry_nodes = map
            .iter()
            .filter_map(|(key, value)| {
                let node = key.strip_prefix(K_RETRY_PREFIX)?;
                (value.trim().parse::<u32>().unwrap_or(0) > 0).then(|| node.to_owned())
            })
            .collect();
        Ok(SnapshotLoci {
            process_id: get(K_PROCESS_ID),
            deployment_id: get(K_DEPLOYMENT_ID),
            status: map
                .get(K_STATUS)
                .cloned()
                .unwrap_or_else(|| STATUS_RUNNING.to_owned()),
            waiting_nodes: split_list(&get(K_WAITING)),
            completed_nodes: split_list(&get(K_COMPLETED)),
            start_node: get(K_START_NODE),
            retry_nodes,
            audit_seq: get(K_AUDIT_SEQ).trim().parse::<u32>().unwrap_or(0),
        })
    }

    /// Re-pin PERSISTED BYTES to a different deployment and rewrite every node id the snapshot
    /// names, by the same key-level patch of the raw Properties map that
    /// [`mark_failed`](Self::mark_failed) and [`mark_terminal`](Self::mark_terminal) use — the
    /// durable half of the admin instance-migration operation.
    ///
    /// The key-patch shape is load-bearing for exactly the reason it is on the other two paths, and
    /// one more that is specific to migration. A decode/re-encode would need the tenant DEK to
    /// decrypt every `sutra.enc.<name>`, would have to re-derive an encrypt-set the resume-time
    /// snapshot no longer carries, and would persist a previously-encrypted value in the clear the
    /// moment either went wrong. Patching the map carries `sutra.enc.*` and `sutra.keyId` through
    /// byte for byte. That the ciphertext still DECRYPTS afterwards is not luck: the AAD binds
    /// `keyId ⋮ instanceId ⋮ varName` and deliberately excludes the deployment id (see
    /// [`SnapshotCrypto`]), so re-pinning is invisible to the cipher.
    ///
    /// What is rewritten, and nothing else:
    ///
    /// * `sutra.deploymentId` → `new_deployment_id` (the pin the resume paths resolve against).
    /// * `sutra.processId` → `new_process_id` when `Some` — the CROSS-PROCESS half of the
    ///   operation (v2). `None` leaves the key alone, which is every same-process migration. The
    ///   process id is what every resume path resolves the graph through, so re-homing an instance
    ///   into a different process is exactly this one key plus a node mapping that covers every
    ///   live locus; the caller (never this function) enforces that the mapping is explicit.
    /// * `sutra.waitingNodes`, `sutra.completedNodes`, `sutra.startNode` → each entry mapped
    ///   through `node_mapping` (an id absent from the map is kept as-is — identity mapping).
    /// * `sutra.retry.<nodeId>` → the KEY is renamed to `sutra.retry.<mappedNodeId>`, its counter
    ///   value untouched; a task's burned-attempt budget must follow the task across the migration
    ///   rather than silently resetting to zero.
    /// * `sutra.retryWait.<nodeId>` → same key-rename, value (the parking failure code)
    ///   untouched; a channel-call node's backoff window must follow the node, or the migrated
    ///   instance's due backoff timer would read as stale and never re-drive.
    /// * `sutra.auditSeq` → `audit_seq` when `Some` (the migration itself takes a journal seq, so
    ///   the instance's next event must not collide with it). `None` leaves the key alone.
    ///
    /// Untouched: the variables (plain and encrypted), `sutra.keyId`, `sutra.sensitive`,
    /// `sutra.status`, the failure keys, and `sutra.coverage.<pathId>` — coverage cursors are keyed
    /// by DECLARED PATH id, not node id, so a node mapping must not be applied to them. (A
    /// cross-process move therefore carries a cursor keyed by the SOURCE process's declared path
    /// ids; they are inert on a target that declares no such path, and remapping them would need a
    /// path mapping nobody supplied.)
    ///
    /// Fails closed on a terminal snapshot: a COMPLETED/TERMINATED instance is history, not live
    /// state, and re-pinning history would rewrite the record of where it ran. FAILED is
    /// deliberately allowed — repairing the model and moving a dead instance onto it is the
    /// operation's prime use case.
    pub fn migrate_pinned(
        bytes: &[u8],
        new_deployment_id: &str,
        new_process_id: Option<&str>,
        node_mapping: &BTreeMap<String, String>,
        audit_seq: Option<u32>,
    ) -> Result<Vec<u8>, String> {
        let mut map = Self::raw_map(bytes)?;
        let status = map
            .get(K_STATUS)
            .map(String::as_str)
            .unwrap_or(STATUS_RUNNING);
        if status == STATUS_COMPLETED || status == STATUS_TERMINATED {
            return Err(format!(
                "instance is {status} — a terminal instance is history, not live state, and \
                 cannot be migrated (FAILED instances can)"
            ));
        }
        let map_one = |id: &str| -> String {
            node_mapping
                .get(id)
                .cloned()
                .unwrap_or_else(|| id.to_owned())
        };
        let map_list = |raw: &str| -> String {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(map_one)
                .collect::<Vec<_>>()
                .join(",")
        };
        map.insert(K_DEPLOYMENT_ID.to_owned(), new_deployment_id.to_owned());
        if let Some(process_id) = new_process_id.filter(|p| !p.is_empty()) {
            map.insert(K_PROCESS_ID.to_owned(), process_id.to_owned());
        }
        for key in [K_WAITING, K_COMPLETED] {
            if let Some(raw) = map.get(key).cloned() {
                map.insert(key.to_owned(), map_list(&raw));
            }
        }
        if let Some(start) = map.get(K_START_NODE).cloned() {
            if !start.is_empty() {
                map.insert(K_START_NODE.to_owned(), map_one(&start));
            }
        }
        // Retry counters and backoff markers are keyed BY node id, so the mapping renames keys
        // rather than values. Collected first: mutating the map while iterating it is not
        // possible, and a mapping that renames A→B while another entry already occupies B must
        // not lose one of them.
        for prefix in [K_RETRY_PREFIX, K_RETRY_WAIT_PREFIX] {
            let keyed: Vec<(String, String)> = map
                .keys()
                .filter_map(|k| {
                    k.strip_prefix(prefix)
                        .map(|node| (k.clone(), map_one(node)))
                })
                .collect();
            for (old_key, mapped_node) in keyed {
                let new_key = format!("{prefix}{mapped_node}");
                if new_key == old_key {
                    continue;
                }
                if let Some(value) = map.remove(&old_key) {
                    map.insert(new_key, value);
                }
            }
        }
        if let Some(seq) = audit_seq {
            if seq > 0 {
                map.insert(K_AUDIT_SEQ.to_owned(), seq.to_string());
            }
        }
        Ok(props::write_lines(
            map.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        ))
    }

    /// The migration-stable `keyId` a snapshot was encrypted under (`sutra.keyId`), or `None` for a
    /// plaintext snapshot. Read WITHOUT decrypting so the load path can rebuild the cipher from its
    /// [`KeyProvider`](sutra_crypto::KeyProvider) before calling [`read_encrypted`](Self::read_encrypted).
    pub fn peek_key_id(bytes: &[u8]) -> Result<Option<String>, String> {
        Ok(Self::raw_map(bytes)?.get(K_KEY_ID).cloned())
    }

    /// Parse the raw properties key/value map (last-write-wins), without any decode/decrypt — the
    /// shared front half of [`read_encrypted`](Self::read_encrypted) / [`peek`](Self::peek).
    fn raw_map(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
        let mut map = BTreeMap::new();
        for (k, v) in props::read_lines(bytes)? {
            map.insert(k, v);
        }
        Ok(map)
    }

    /// Re-pin the snapshot to a different deployment — the admin migrate primitive, and the
    /// ONLY sanctioned rewrite of an instance's deployment identity.
    #[must_use]
    pub fn with_deployment_id(mut self, new_deployment_id: impl Into<String>) -> Self {
        self.deployment_id = new_deployment_id.into();
        self
    }

    /// Archive-local process id.
    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    /// The pinned deployment id (R1) — mirrors the row's isolation column.
    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    /// Status string (see the `STATUS_*` constants).
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Completed non-IO node ids (replay-as-done).
    pub fn completed_nodes(&self) -> &[String] {
        &self.completed_nodes
    }

    /// The instance's variables, typed. A snapshot decoded from v2/v3 bytes yields
    /// [`SnapshotValue::String`] throughout — the value model those generations froze.
    pub fn variables(&self) -> &BTreeMap<String, SnapshotValue> {
        &self.variables
    }

    /// Wait-node frontier — empty for a non-suspended snapshot.
    pub fn waiting_nodes(&self) -> &[String] {
        &self.waiting_nodes
    }

    /// Routed start-event id (multi-start replay); empty when unset.
    pub fn start_node(&self) -> &str {
        &self.start_node
    }

    /// Per-instance monotonic audit seq at suspend; 0 when none was captured.
    pub fn audit_seq(&self) -> u32 {
        self.audit_seq
    }

    /// Sensitive variable names (sorted); audit/log layers must redact their values.
    pub fn sensitive(&self) -> &[String] {
        &self.sensitive
    }

    /// Bounded matched-prefix coverage counters per declared path id.
    pub fn coverage(&self) -> &BTreeMap<String, u64> {
        &self.coverage
    }

    /// Per `<q:retry>` node id, the number of attempts of that task that have already FAILED.
    /// An absent entry means "no attempt has failed yet" — the retry curve starts at attempt 1.
    pub fn retry_attempts(&self) -> &BTreeMap<String, u32> {
        &self.retry_attempts
    }

    /// The channel-call backoff-window markers (`sutra.retryWait.<nodeId>` → parking failure
    /// code). See [`with_retry_backoff`](Self::with_retry_backoff).
    pub fn retry_backoff(&self) -> &BTreeMap<String, String> {
        &self.retry_backoff
    }

    /// The fatal step's diagnostic code on a FAILED snapshot; empty otherwise.
    pub fn failure_code(&self) -> &str {
        &self.failure_code
    }

    /// The fatal step's message on a FAILED snapshot; empty otherwise. Admin-grade — it can quote
    /// business data, so it must not reach an unauthenticated surface.
    pub fn failure_detail(&self) -> &str {
        &self.failure_detail
    }

    /// True when parked at a wait state (resumable by a relay).
    pub fn is_suspended(&self) -> bool {
        self.status == STATUS_SUSPENDED
    }

    /// True when a fatal step marked this instance FAILED — durably not resumable (every resume
    /// path fails closed on it) and awaiting an operator.
    pub fn is_failed(&self) -> bool {
        self.status == STATUS_FAILED
    }

    /// True for COMPLETED / TERMINATED.
    pub fn is_terminal(&self) -> bool {
        self.status == STATUS_COMPLETED || self.status == STATUS_TERMINATED
    }
}

/// The string-valued variable map as typed values — the v2 model, lifted unchanged.
fn string_variables(variables: BTreeMap<String, String>) -> BTreeMap<String, SnapshotValue> {
    variables
        .into_iter()
        .map(|(name, value)| (name, SnapshotValue::String(value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn strs(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    // ---- durable FAILED state (P0-4) ------------------------------------------------------

    fn parked() -> InstanceSnapshot {
        InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S", "A"]),
            vars(&[("inboundId", "INB-7")]),
            strs(&["U"]),
            "S",
            7,
        )
    }

    #[test]
    fn a_failed_snapshot_round_trips_its_status_and_cause() {
        let failed = parked().with_failure("SUTRA.RUNTIME.TASK.UNCAUGHT", "the task threw");

        let read = InstanceSnapshot::read(&failed.write()).unwrap();

        assert_eq!(read.status(), STATUS_FAILED);
        assert!(read.is_failed());
        assert!(!read.is_suspended(), "FAILED is not resumable");
        assert_eq!(read.failure_code(), "SUTRA.RUNTIME.TASK.UNCAUGHT");
        assert_eq!(read.failure_detail(), "the task threw");
        // The frontier it died at is preserved — that is the whole value of the record.
        assert_eq!(read.waiting_nodes(), ["U"]);
        assert_eq!(read.completed_nodes(), ["S", "A"]);
        assert_eq!(read.variables(), parked().variables());
    }

    #[test]
    fn a_snapshot_that_never_failed_emits_no_failure_keys() {
        // Byte-determinism guard: the new keys must not appear on the pre-existing corpus.
        let bytes = parked().write();
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("sutra.failureCode"));
        assert!(!text.contains("sutra.failureDetail"));
    }

    #[test]
    fn mark_failed_patches_stored_bytes_without_touching_anything_else() {
        let original = parked().write();

        let marked = InstanceSnapshot::mark_failed(
            &original,
            "SUTRA.RUNTIME.TASK.UNCAUGHT",
            "the task threw",
        )
        .unwrap();

        let read = InstanceSnapshot::read(&marked).unwrap();
        assert_eq!(read.status(), STATUS_FAILED);
        assert_eq!(read.failure_code(), "SUTRA.RUNTIME.TASK.UNCAUGHT");
        assert_eq!(read.failure_detail(), "the task threw");
        let before = InstanceSnapshot::read(&original).unwrap();
        assert_eq!(read.variables(), before.variables());
        assert_eq!(read.waiting_nodes(), before.waiting_nodes());
        assert_eq!(read.completed_nodes(), before.completed_nodes());
        assert_eq!(read.audit_seq(), before.audit_seq());
        assert_eq!(read.start_node(), before.start_node());
        // Patching the map is equivalent to building the FAILED snapshot in memory — and
        // byte-identical, since the writer re-sorts either way.
        assert_eq!(
            marked,
            parked()
                .with_failure("SUTRA.RUNTIME.TASK.UNCAUGHT", "the task threw")
                .write()
        );
    }

    #[test]
    fn mark_failed_carries_encrypted_values_through_untouched() {
        // The reason it patches bytes instead of decode/re-encoding: marking an instance dead must
        // never need the tenant key, and must never be able to downgrade an encrypted value to
        // plaintext. Synthesised v3-shaped bytes (an `sutra.enc.` value + its keyId) prove the
        // carry-through without a cipher anywhere in sight.
        let mut raw = String::new();
        raw.push_str("sutra.snapshot=3\n");
        raw.push_str("sutra.deploymentId=dep-0123456789abcdef01234567\n");
        raw.push_str("sutra.processId=p1\n");
        raw.push_str("sutra.status=SUSPENDED\n");
        raw.push_str("sutra.completedNodes=S\n");
        raw.push_str("sutra.waitingNodes=U\n");
        raw.push_str("sutra.keyId=acme\n");
        raw.push_str("sutra.enc.iban=Y2lwaGVydGV4dA==\n");

        let marked =
            InstanceSnapshot::mark_failed(raw.as_bytes(), "SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
                .unwrap();
        let text = String::from_utf8(marked).unwrap();

        assert!(text.contains("sutra.status=FAILED"));
        assert!(
            text.contains("sutra.enc.iban=Y2lwaGVydGV4dA\\=\\=")
                || text.contains("sutra.enc.iban=Y2lwaGVydGV4dA=="),
            "the ciphertext is carried verbatim: {text}"
        );
        assert!(text.contains("sutra.keyId=acme"), "and so is the anchor");
        assert!(
            !text.contains("sutra.var.iban"),
            "an encrypted value is never rewritten as plaintext"
        );
    }

    // ---- terminal retention (P1-2) --------------------------------------------------------

    #[test]
    fn mark_terminal_restamps_the_status_and_carries_everything_else_through() {
        let original = parked().write();

        let completed = InstanceSnapshot::mark_terminal(&original, STATUS_COMPLETED).unwrap();

        let read = InstanceSnapshot::read(&completed).unwrap();
        assert_eq!(read.status(), STATUS_COMPLETED);
        assert!(read.is_terminal());
        assert!(
            !read.is_suspended(),
            "a completed instance is not resumable"
        );
        let before = InstanceSnapshot::read(&original).unwrap();
        assert_eq!(read.variables(), before.variables());
        assert_eq!(read.completed_nodes(), before.completed_nodes());
        assert_eq!(read.audit_seq(), before.audit_seq());
        assert_eq!(read.start_node(), before.start_node());
        // The frontier it was parked at when it finished is kept — the only durable record of
        // WHERE the instance ended (its wait rows are resolved in the same transaction).
        assert_eq!(read.waiting_nodes(), before.waiting_nodes());
    }

    #[test]
    fn mark_terminal_records_an_operator_cancel_as_terminated() {
        let terminated =
            InstanceSnapshot::mark_terminal(&parked().write(), STATUS_TERMINATED).unwrap();
        let read = InstanceSnapshot::read(&terminated).unwrap();
        assert_eq!(read.status(), STATUS_TERMINATED);
        assert!(read.is_terminal());
    }

    #[test]
    fn mark_terminal_carries_encrypted_values_through_untouched() {
        // The same fail-closed property `mark_failed` has: finishing an instance must never need
        // the tenant key, and must never downgrade an at-rest value to plaintext.
        let mut raw = String::new();
        raw.push_str("sutra.snapshot=3\n");
        raw.push_str("sutra.deploymentId=dep-0123456789abcdef01234567\n");
        raw.push_str("sutra.processId=p1\n");
        raw.push_str("sutra.status=SUSPENDED\n");
        raw.push_str("sutra.completedNodes=S\n");
        raw.push_str("sutra.waitingNodes=U\n");
        raw.push_str("sutra.keyId=acme\n");
        raw.push_str("sutra.enc.iban=Y2lwaGVydGV4dA==\n");

        let marked = InstanceSnapshot::mark_terminal(raw.as_bytes(), STATUS_COMPLETED).unwrap();
        let text = String::from_utf8(marked).unwrap();

        assert!(text.contains("sutra.status=COMPLETED"));
        assert!(
            text.contains("sutra.enc.iban=Y2lwaGVydGV4dA\\=\\=")
                || text.contains("sutra.enc.iban=Y2lwaGVydGV4dA=="),
            "the ciphertext is carried verbatim: {text}"
        );
        assert!(text.contains("sutra.keyId=acme"), "and so is the anchor");
        assert!(
            !text.contains("sutra.var.iban"),
            "an encrypted value is never rewritten as plaintext"
        );
    }

    #[test]
    fn mark_terminal_refuses_a_non_terminal_status() {
        let original = parked().write();
        for status in [STATUS_RUNNING, STATUS_SUSPENDED, STATUS_FAILED, "NONSENSE"] {
            assert!(
                InstanceSnapshot::mark_terminal(&original, status).is_err(),
                "mark_terminal must fail closed on '{status}'"
            );
        }
    }

    #[test]
    fn mark_terminal_preserves_a_failure_marker_it_never_sees() {
        // Belt-and-braces on ordering: a FAILED snapshot is never handed to mark_terminal by the
        // engine (cancel refuses a terminal row, and commit_complete only ever follows a live
        // step), but if it were, the cause keys still ride through rather than being silently lost.
        let failed = parked()
            .with_failure("SUTRA.RUNTIME.TASK.UNCAUGHT", "the task threw")
            .write();
        let marked = InstanceSnapshot::mark_terminal(&failed, STATUS_TERMINATED).unwrap();
        let read = InstanceSnapshot::read(&marked).unwrap();
        assert_eq!(read.status(), STATUS_TERMINATED);
        assert_eq!(read.failure_code(), "SUTRA.RUNTIME.TASK.UNCAUGHT");
    }

    // ---- round-trip corpus ---------------------------------------------------------------

    #[test]
    fn suspended_snapshot_round_trips_waiting_frontier_and_start_node() {
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S", "A"]),
            vars(&[("inboundId", "INB-7"), ("approvedBy", "A")]),
            strs(&["U"]),
            "S",
            7,
        );

        let read = InstanceSnapshot::read(&snap.write()).unwrap();

        assert_eq!(read.status(), STATUS_SUSPENDED);
        assert!(read.is_suspended());
        assert!(!read.is_terminal());
        assert_eq!(read.completed_nodes(), strs(&["S", "A"]));
        assert_eq!(read.waiting_nodes(), strs(&["U"]));
        assert_eq!(read.start_node(), "S");
        assert_eq!(read.audit_seq(), 7);
        assert_eq!(read.variables()["inboundId"], SnapshotValue::from("INB-7"));
        assert_eq!(read.variables()["approvedBy"], SnapshotValue::from("A"));
        assert_eq!(read.process_id(), "p1");
        assert_eq!(read.deployment_id(), "dep-0123456789abcdef01234567");
    }

    #[test]
    fn audit_seq_is_omitted_when_zero_and_survives_when_set() {
        let no_seq = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            BTreeMap::new(),
            strs(&["U"]),
            "S",
            0,
        );
        let text = String::from_utf8(no_seq.write()).unwrap();
        assert!(!text.contains("sutra.auditSeq"));
        assert_eq!(
            InstanceSnapshot::peek(&no_seq.write()).unwrap().audit_seq,
            0
        );

        let with_seq = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            BTreeMap::new(),
            strs(&["U"]),
            "S",
            42,
        );
        assert_eq!(
            InstanceSnapshot::peek(&with_seq.write()).unwrap().audit_seq,
            42
        );
    }

    #[test]
    fn peek_reads_routing_keys_and_suspended_flag_for_both_states() {
        let suspended = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S", "A"]),
            BTreeMap::new(),
            strs(&["U"]),
            "S",
            0,
        );
        let sk = InstanceSnapshot::peek(&suspended.write()).unwrap();
        assert!(sk.suspended);
        assert_eq!(sk.process_id, "p1");
        assert_eq!(sk.deployment_id, "dep-0123456789abcdef01234567");
        assert_eq!(sk.status, STATUS_SUSPENDED);

        let running = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S"]),
            BTreeMap::new(),
        );
        let rk = InstanceSnapshot::peek(&running.write()).unwrap();
        assert!(!rk.suspended);
        assert_eq!(rk.status, STATUS_RUNNING);
        assert_eq!(rk.process_id, "p1");
    }

    #[test]
    fn non_suspended_snapshot_omits_suspended_keys_and_stays_byte_stable() {
        let snap = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S", "T"]),
            vars(&[("x", "1")]),
        );

        let first = snap.write();
        let second = InstanceSnapshot::read(&first).unwrap().write();

        assert_eq!(
            second, first,
            "byte-stable round-trip (migration relies on this)"
        );
        let text = String::from_utf8(first.clone()).unwrap();
        assert!(!text.contains("sutra.waitingNodes"));
        assert!(!text.contains("sutra.startNode"));
        assert!(text.contains("sutra.snapshot=2"));
        let read = InstanceSnapshot::read(&first).unwrap();
        assert!(read.waiting_nodes().is_empty());
        assert!(read.start_node().is_empty());
    }

    // ---- encryption at rest (snapshot v3) --------------------------------------------

    fn test_cipher(key_id: &str) -> sutra_crypto::Aes256GcmCipher {
        use sutra_crypto::{Aes256GcmCipher, HkdfKeyProvider, KeyProvider};
        let provider = HkdfKeyProvider::new(b"unit-test-master-secret");
        Aes256GcmCipher::new(&provider.data_key(key_id).unwrap())
    }

    fn enc_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn v3_round_trips_encrypted_variables_by_decrypt_equality() {
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            vars(&[("pan", "4111111111111111"), ("amount", "42")]),
            strs(&["U"]),
            "S",
            0,
        );
        let cipher = test_cipher("tenant-1");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-1", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&ctx), &enc_set(&["pan"]))
            .unwrap();

        // Ciphertext form: pan is `sutra.enc.`, the version bumped to 3, the raw PAN is nowhere.
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains("sutra.snapshot=3"));
        assert!(text.contains("sutra.enc.pan="));
        assert!(!text.contains("sutra.var.pan="));
        assert!(
            !text.contains("4111111111111111"),
            "raw sensitive value present on disk"
        );
        assert!(text.contains("sutra.var.amount=42")); // a non-encrypted var stays plaintext

        // Decrypt-equality round trip (NOT byte-equality — the GCM nonce is random).
        let read = InstanceSnapshot::read_encrypted(&bytes, Some(&ctx)).unwrap();
        assert_eq!(
            read.variables()["pan"],
            SnapshotValue::from("4111111111111111")
        );
        assert_eq!(read.variables()["amount"], SnapshotValue::from("42"));
    }

    #[test]
    fn plaintext_snapshot_stays_v2_and_byte_identical() {
        let snap = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S"]),
            vars(&[("x", "1")]),
        );
        // No cipher, or a cipher with an EMPTY encrypt-set → identical bytes + the v2 marker.
        let plain = snap.write();
        let cipher = test_cipher("tenant-1");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-1", "inst-1");
        let none_encrypted = snap.write_encrypted(Some(&ctx), &enc_set(&[])).unwrap();
        assert_eq!(
            plain, none_encrypted,
            "an empty encrypt-set must not change the bytes"
        );
        assert!(String::from_utf8(plain)
            .unwrap()
            .contains("sutra.snapshot=2"));
    }

    #[test]
    fn encrypted_snapshot_fails_closed_without_a_cipher() {
        let snap = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S"]),
            vars(&[("pan", "secret")]),
        );
        let cipher = test_cipher("tenant-1");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-1", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&ctx), &enc_set(&["pan"]))
            .unwrap();
        // Both the plain `read` and `read_encrypted(None)` must refuse an encrypted snapshot.
        assert!(InstanceSnapshot::read(&bytes).is_err());
        assert!(InstanceSnapshot::read_encrypted(&bytes, None).is_err());
    }

    #[test]
    fn decrypt_fails_closed_under_a_different_key_or_context() {
        let snap = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S"]),
            vars(&[("pan", "secret")]),
        );
        let cipher_a = test_cipher("tenant-1");
        let write_ctx = SnapshotCrypto::new(&cipher_a, "tenant-1", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&write_ctx), &enc_set(&["pan"]))
            .unwrap();

        // Wrong DEK (a different key_id ⇒ a different HKDF-derived key).
        let cipher_b = test_cipher("tenant-2");
        let wrong_key = SnapshotCrypto::new(&cipher_b, "tenant-2", "inst-1");
        assert!(InstanceSnapshot::read_encrypted(&bytes, Some(&wrong_key)).is_err());

        // Right DEK, wrong AAD context (a different instance_id) ⇒ authentication failure.
        let wrong_instance = SnapshotCrypto::new(&cipher_a, "tenant-1", "inst-OTHER");
        assert!(InstanceSnapshot::read_encrypted(&bytes, Some(&wrong_instance)).is_err());

        // Right DEK + right context ⇒ succeeds (control).
        assert!(InstanceSnapshot::read_encrypted(&bytes, Some(&write_ctx)).is_ok());
    }

    #[test]
    fn peek_and_key_id_work_on_an_encrypted_snapshot_without_a_cipher() {
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            vars(&[("pan", "secret")]),
            strs(&["U"]),
            "S",
            5,
        );
        let cipher = test_cipher("tenant-9");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-9", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&ctx), &enc_set(&["pan"]))
            .unwrap();

        // Routing peek works WITHOUT a cipher on an encrypted snapshot (fail-open metadata).
        let keys = InstanceSnapshot::peek(&bytes).unwrap();
        assert!(keys.suspended);
        assert_eq!(keys.process_id, "p1");
        assert_eq!(keys.deployment_id, "dep-0123456789abcdef01234567");
        assert_eq!(keys.audit_seq, 5);

        // The keyId is recoverable without decrypting — the load path rebuilds the cipher from it.
        assert_eq!(
            InstanceSnapshot::peek_key_id(&bytes).unwrap().as_deref(),
            Some("tenant-9")
        );
        // A plaintext snapshot carries no keyId.
        assert!(InstanceSnapshot::peek_key_id(&snap.write())
            .unwrap()
            .is_none());
    }

    // ---- golden bytes ---------------------------------------------------------------------
    // Pinned expected output of the writer (Properties-line store, no comment line, lines
    // sorted byte-wise ascending); captured 2026-07-12. Any diff here is a persisted-format
    // break, not a test to update.

    #[test]
    fn golden_suspended_bytes_are_pinned() {
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S", "A"]),
            vars(&[("inboundId", "INB-7"), ("approvedBy", "A")]),
            strs(&["U"]),
            "S",
            7,
        );
        let expected = "sutra.auditSeq=7\nsutra.completedNodes=S,A\nsutra.deploymentId=dep-0123456789abcdef01234567\nsutra.processId=p1\nsutra.snapshot=2\nsutra.startNode=S\nsutra.status=SUSPENDED\nsutra.var.approvedBy=A\nsutra.var.inboundId=INB-7\nsutra.waitingNodes=U\n";
        assert_eq!(String::from_utf8(snap.write()).unwrap(), expected);
    }

    #[test]
    fn golden_running_empty_bytes_are_pinned() {
        let snap = InstanceSnapshot::of("loan", "", STATUS_RUNNING, vec![], BTreeMap::new());
        let expected = "sutra.completedNodes=\nsutra.deploymentId=\nsutra.processId=loan\nsutra.snapshot=2\nsutra.status=RUNNING\n";
        assert_eq!(String::from_utf8(snap.write()).unwrap(), expected);
    }

    #[test]
    fn golden_escaping_bytes_are_pinned() {
        let snap = InstanceSnapshot::of(
            "p x",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["n1", "n2"]),
            vars(&[
                ("note", " leading and = : # ! chars"),
                ("multiline", "line1\nline2\ttabbed\rcr\u{c}ff"),
                ("path", "C:\\temp\\x"),
                ("unicode", "caf\u{e9} \u{4e16}\u{754c} \u{1F600}"),
                ("name with space", "v"),
                ("empty", ""),
            ]),
        );
        let expected = concat!(
            "sutra.completedNodes=n1,n2\n",
            "sutra.deploymentId=dep-0123456789abcdef01234567\n",
            "sutra.processId=p x\n",
            "sutra.snapshot=2\n",
            "sutra.status=RUNNING\n",
            "sutra.var.empty=\n",
            "sutra.var.multiline=line1\\nline2\\ttabbed\\rcr\\fff\n",
            "sutra.var.name\\ with\\ space=v\n",
            "sutra.var.note=\\ leading and \\= \\: \\# \\! chars\n",
            "sutra.var.path=C\\:\\\\temp\\\\x\n",
            "sutra.var.unicode=caf\\u00E9 \\u4E16\\u754C \\uD83D\\uDE00\n",
        );
        assert_eq!(String::from_utf8(snap.write()).unwrap(), expected);
        // And the escaped form reads back to the identical logical state + identical bytes.
        let read = InstanceSnapshot::read(&snap.write()).unwrap();
        assert_eq!(read, snap);
        assert_eq!(read.write(), snap.write());
    }

    #[test]
    fn golden_coverage_and_sensitive_match_contract_layout() {
        let snap = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            vars(&[("cardNumber", "4111-xxxx"), ("ssn", "000-00-0000")]),
            strs(&["W"]),
            "",
            0,
        )
        .with_sensitive(strs(&["ssn", "cardNumber"]))
        .with_coverage(
            [
                ("happy_path".to_owned(), 3u64),
                ("reject.path".to_owned(), 1u64),
            ]
            .into(),
        );
        let expected = concat!(
            "sutra.completedNodes=S\n",
            "sutra.coverage.happy_path=3\n",
            "sutra.coverage.reject.path=1\n",
            "sutra.deploymentId=dep-0123456789abcdef01234567\n",
            "sutra.processId=pay\n",
            "sutra.sensitive=cardNumber,ssn\n",
            "sutra.snapshot=2\n",
            "sutra.status=SUSPENDED\n",
            "sutra.var.cardNumber=4111-xxxx\n",
            "sutra.var.ssn=000-00-0000\n",
            "sutra.waitingNodes=W\n",
        );
        assert_eq!(String::from_utf8(snap.write()).unwrap(), expected);
        let read = InstanceSnapshot::read(&snap.write()).unwrap();
        assert_eq!(read.coverage().get("happy_path"), Some(&3));
        assert_eq!(read.coverage().get("reject.path"), Some(&1));
        assert_eq!(read.sensitive(), strs(&["cardNumber", "ssn"]));
        assert_eq!(read.write(), snap.write());
    }

    #[test]
    fn zero_coverage_counters_emit_no_keys() {
        let snap = InstanceSnapshot::of("p", "", STATUS_RUNNING, vec![], BTreeMap::new())
            .with_coverage([("touched".to_owned(), 1u64), ("untouched".to_owned(), 0u64)].into());
        let text = String::from_utf8(snap.write()).unwrap();
        assert!(text.contains("sutra.coverage.touched=1"));
        assert!(!text.contains("untouched"));
    }

    #[test]
    fn repin_rewrites_only_the_deployment_key() {
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            vars(&[("x", "1")]),
            strs(&["U"]),
            "S",
            3,
        );
        let repinned = snap
            .clone()
            .with_deployment_id("dep-aaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(repinned.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
        let old = String::from_utf8(snap.write()).unwrap();
        let new = String::from_utf8(repinned.write()).unwrap();
        assert_eq!(
            old.replace(
                "dep-0123456789abcdef01234567",
                "dep-aaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            new
        );
    }

    #[test]
    fn read_defaults_match_java_reader() {
        // Absent keys: empty ids, RUNNING status, zero audit seq; malformed seq reads 0;
        // list entries are trimmed and empties dropped.
        let bytes = b"sutra.completedNodes= a , ,b\nsutra.auditSeq=oops\n";
        let read = InstanceSnapshot::read(bytes).unwrap();
        assert_eq!(read.process_id(), "");
        assert_eq!(read.deployment_id(), "");
        assert_eq!(read.status(), STATUS_RUNNING);
        assert_eq!(read.completed_nodes(), strs(&["a", "b"]));
        assert_eq!(read.audit_seq(), 0);
        assert!(InstanceSnapshot::read(&[])
            .unwrap()
            .completed_nodes()
            .is_empty());
    }

    // ---- `sutra.retry.<nodeId>` — the durable <q:retry> attempt counters (P1-1) -------------

    #[test]
    fn retry_attempt_counters_round_trip_and_sort_canonically() {
        let snap = parked().with_retry_attempts(BTreeMap::from([
            ("chargeCard".to_string(), 2u32),
            ("callRisk".to_string(), 1),
        ]));
        let bytes = snap.write();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains("sutra.retry.callRisk=1"), "{text}");
        assert!(text.contains("sutra.retry.chargeCard=2"), "{text}");

        let read = InstanceSnapshot::read(&bytes).unwrap();
        assert_eq!(read.retry_attempts().get("chargeCard"), Some(&2));
        assert_eq!(read.retry_attempts().get("callRisk"), Some(&1));
        // Byte-determinism: an identical logical state re-encodes identically.
        assert_eq!(bytes, read.write());
    }

    #[test]
    fn a_snapshot_with_no_retry_attempts_is_byte_identical_to_the_pre_p1_1_form() {
        // The whole reason the counters are emit-only-when-present: every existing snapshot — and
        // the golden-bytes corpus — must be untouched by this feature.
        let baseline = parked().write();
        assert_eq!(
            parked().with_retry_attempts(BTreeMap::new()).write(),
            baseline
        );
        // A zero counter is dropped, not written as `=0`.
        assert_eq!(
            parked()
                .with_retry_attempts(BTreeMap::from([("neverFailed".to_string(), 0u32)]))
                .write(),
            baseline
        );
        assert!(!String::from_utf8(baseline)
            .unwrap()
            .contains("sutra.retry."));
    }

    #[test]
    fn an_absent_or_malformed_retry_counter_reads_as_no_attempts() {
        // Same decode contract as the coverage cursors: never fail a resume over a counter — the
        // worst case is that the retry curve restarts, which is safe.
        let read = InstanceSnapshot::read(b"sutra.retry.T=oops\nsutra.retry.U=0\n").unwrap();
        assert!(read.retry_attempts().is_empty());
    }

    // ---- the channel-call backoff markers (F1) ---------------------------------------------

    #[test]
    fn retry_backoff_markers_round_trip_under_their_own_key_family() {
        let snap = parked()
            .with_retry_attempts(BTreeMap::from([("Call".to_string(), 1u32)]))
            .with_retry_backoff(BTreeMap::from([(
                "Call".to_string(),
                "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT".to_string(),
            )]));
        let bytes = snap.write();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(
            text.contains("sutra.retryWait.Call=SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT"),
            "{text}"
        );
        let read = InstanceSnapshot::read(&bytes).unwrap();
        assert_eq!(
            read.retry_backoff().get("Call").map(String::as_str),
            Some("SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT")
        );
        // The two key families never claim each other's keys.
        assert_eq!(read.retry_attempts().get("Call"), Some(&1));
        assert!(read.retry_attempts().get("Wait.Call").is_none());
    }

    #[test]
    fn a_snapshot_with_no_backoff_markers_is_byte_identical_to_the_pre_f1_form() {
        let baseline = parked().write();
        assert_eq!(
            parked().with_retry_backoff(BTreeMap::new()).write(),
            baseline
        );
        // A blank marker is dropped, not written.
        assert_eq!(
            parked()
                .with_retry_backoff(BTreeMap::from([("Call".to_string(), "  ".to_string())]))
                .write(),
            baseline
        );
        assert!(!String::from_utf8(baseline)
            .unwrap()
            .contains("sutra.retryWait."));
    }

    #[test]
    fn a_blank_backoff_marker_reads_as_not_in_backoff() {
        // The safe direction: the node then behaves as attempt-in-flight, and its due backoff
        // row resolves as stale rather than double-driving anything.
        let read = InstanceSnapshot::read(b"sutra.retryWait.Call= \n").unwrap();
        assert!(read.retry_backoff().is_empty());
    }

    #[test]
    fn migrate_renames_backoff_marker_keys_with_the_node_mapping() {
        let original = parked()
            .with_retry_attempts(BTreeMap::from([("Call".to_string(), 1u32)]))
            .with_retry_backoff(BTreeMap::from([(
                "Call".to_string(),
                "SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED".to_string(),
            )]))
            .write();
        let migrated = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::from([("Call".to_string(), "Call2".to_string())]),
            None,
        )
        .unwrap();
        let read = InstanceSnapshot::read(&migrated).unwrap();
        // The backoff window follows the node — a stale key would leave the migrated
        // instance's due backoff timer reading as stale, never re-driving.
        assert_eq!(
            read.retry_backoff().get("Call2").map(String::as_str),
            Some("SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED")
        );
        assert!(read.retry_backoff().get("Call").is_none());
        assert_eq!(read.retry_attempts().get("Call2"), Some(&1));
    }

    // ---- the migrate key-patch (P1-8) -------------------------------------------------------

    #[test]
    fn migrate_repins_and_rewrites_every_node_id_the_snapshot_names() {
        let original = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S", "A"]),
            vars(&[("x", "1")]),
            strs(&["U"]),
            "S",
            7,
        )
        .with_retry_attempts(BTreeMap::from([("A".to_string(), 2u32)]))
        .with_coverage(BTreeMap::from([("happy".to_string(), 1u64)]))
        .write();

        let mapping = BTreeMap::from([
            ("U".to_string(), "U2".to_string()),
            ("A".to_string(), "A2".to_string()),
            ("S".to_string(), "S2".to_string()),
        ]);
        let migrated = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &mapping,
            Some(8),
        )
        .unwrap();

        let read = InstanceSnapshot::read(&migrated).unwrap();
        assert_eq!(read.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(read.waiting_nodes(), strs(&["U2"]));
        assert_eq!(read.completed_nodes(), strs(&["S2", "A2"]));
        assert_eq!(read.start_node(), "S2");
        assert_eq!(read.audit_seq(), 8);
        // The burned retry budget follows the task under its NEW id — a reset would hand the
        // migrated instance a fresh budget it never earned.
        assert_eq!(read.retry_attempts().get("A2"), Some(&2));
        assert!(read.retry_attempts().get("A").is_none());
        // Coverage cursors are keyed by DECLARED PATH id, not node id — never remapped.
        assert_eq!(read.coverage().get("happy"), Some(&1));
        assert_eq!(read.variables()["x"], SnapshotValue::from("1"));
        assert_eq!(read.status(), STATUS_SUSPENDED);
    }

    #[test]
    fn migrate_leaves_unmapped_ids_alone_and_keeps_the_audit_seq_when_none() {
        let original = parked().write();
        let migrated = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::new(),
            None,
        )
        .unwrap();
        let read = InstanceSnapshot::read(&migrated).unwrap();
        let before = InstanceSnapshot::read(&original).unwrap();
        assert_eq!(read.waiting_nodes(), before.waiting_nodes());
        assert_eq!(read.completed_nodes(), before.completed_nodes());
        assert_eq!(read.start_node(), before.start_node());
        assert_eq!(read.audit_seq(), before.audit_seq());
        assert_eq!(read.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn migrate_carries_encrypted_values_and_the_key_anchor_through_untouched() {
        // The reason it patches bytes: migrating must never need the tenant key, and must never
        // downgrade an at-rest value to plaintext. The AAD excludes the deployment id, so the
        // carried-through ciphertext still decrypts under the new pin.
        let mut raw = String::new();
        raw.push_str("sutra.snapshot=3\n");
        raw.push_str("sutra.deploymentId=dep-0123456789abcdef01234567\n");
        raw.push_str("sutra.processId=p1\n");
        raw.push_str("sutra.status=SUSPENDED\n");
        raw.push_str("sutra.completedNodes=S\n");
        raw.push_str("sutra.waitingNodes=U\n");
        raw.push_str("sutra.keyId=acme\n");
        raw.push_str("sutra.enc.iban=Y2lwaGVydGV4dA==\n");

        let migrated = InstanceSnapshot::migrate_pinned(
            raw.as_bytes(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::from([("U".to_string(), "U2".to_string())]),
            None,
        )
        .unwrap();
        let text = String::from_utf8(migrated).unwrap();

        assert!(text.contains("sutra.deploymentId=dep-aaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(text.contains("sutra.waitingNodes=U2"));
        assert!(
            text.contains("sutra.enc.iban=Y2lwaGVydGV4dA\\=\\=")
                || text.contains("sutra.enc.iban=Y2lwaGVydGV4dA=="),
            "the ciphertext is carried verbatim: {text}"
        );
        assert!(text.contains("sutra.keyId=acme"), "and so is the anchor");
        assert!(!text.contains("sutra.var.iban"));
    }

    #[test]
    fn migrate_refuses_a_terminal_snapshot_and_allows_a_failed_one() {
        for status in [STATUS_COMPLETED, STATUS_TERMINATED] {
            let terminal = InstanceSnapshot::mark_terminal(&parked().write(), status).unwrap();
            assert!(
                InstanceSnapshot::migrate_pinned(
                    &terminal,
                    "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
                    None,
                    &BTreeMap::new(),
                    None
                )
                .is_err(),
                "a {status} instance is history, not live state"
            );
        }
        // FAILED is the prime use case: fix the model, migrate, then decide what to resume.
        let failed = InstanceSnapshot::mark_failed(&parked().write(), "SUTRA.X", "boom").unwrap();
        let migrated = InstanceSnapshot::migrate_pinned(
            &failed,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::new(),
            None,
        )
        .unwrap();
        let read = InstanceSnapshot::read(&migrated).unwrap();
        assert_eq!(read.status(), STATUS_FAILED);
        assert_eq!(read.failure_code(), "SUTRA.X");
        assert_eq!(read.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn migrate_output_stays_canonical_and_re_encodes_identically() {
        let migrated = InstanceSnapshot::migrate_pinned(
            &parked().write(),
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::from([("U".to_string(), "U2".to_string())]),
            None,
        )
        .unwrap();
        assert_eq!(
            InstanceSnapshot::read(&migrated).unwrap().write(),
            migrated,
            "the patched bytes are already the writer's canonical form"
        );
    }

    // ---- typed values (snapshot v4, P1-3) ---------------------------------------------------

    fn typed(pairs: &[(&str, SnapshotValue)]) -> BTreeMap<String, SnapshotValue> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn parked_typed() -> InstanceSnapshot {
        parked().with_variables(typed(&[
            ("amount", SnapshotValue::Number("100.00".parse().unwrap())),
            ("approved", SnapshotValue::Boolean(true)),
            ("inboundId", SnapshotValue::from("INB-7")),
            ("missing", SnapshotValue::Null),
            (
                "lines",
                SnapshotValue::List(vec![
                    SnapshotValue::Number("1".parse().unwrap()),
                    SnapshotValue::Context(BTreeMap::from([(
                        "sku".to_owned(),
                        SnapshotValue::from("A-1"),
                    )])),
                ]),
            ),
            ("due", SnapshotValue::Date("2026-08-05".to_owned())),
        ]))
    }

    #[test]
    fn typed_variables_round_trip_through_the_codec() {
        let bytes = parked_typed().write();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains("sutra.snapshot=4"), "{text}");

        let read = InstanceSnapshot::read(&bytes).unwrap();
        assert_eq!(read.variables(), parked_typed().variables());
        // The whole point: a number comes back a NUMBER, not the string "100.00".
        assert_eq!(
            read.variables()["amount"],
            SnapshotValue::Number("100.00".parse().unwrap())
        );
        assert_eq!(read.variables()["approved"], SnapshotValue::Boolean(true));
        assert_eq!(read.variables()["missing"], SnapshotValue::Null);
        // …and the rest of the snapshot is untouched by typing.
        assert_eq!(read.waiting_nodes(), ["U"]);
        assert_eq!(read.audit_seq(), 7);
        // Byte-determinism survives the new value encoding.
        assert_eq!(read.write(), bytes);
    }

    #[test]
    fn an_all_string_typed_snapshot_writes_the_v2_bytes_unchanged() {
        // The compatibility hinge: typing must be invisible until a value actually needs it, or
        // every already-parked instance changes shape on its next re-park.
        let via_strings = parked().write();
        let via_typed = parked()
            .with_variables(typed(&[("inboundId", SnapshotValue::from("INB-7"))]))
            .write();
        assert_eq!(via_strings, via_typed);
        assert!(String::from_utf8(via_typed)
            .unwrap()
            .contains("sutra.snapshot=2"));
    }

    #[test]
    fn a_v2_snapshot_still_decodes_every_value_as_a_string() {
        // Backward compatibility, stated as bytes: v2 input, v2 value model out — INCLUDING a
        // value that happens to look like a v4 tag, which must NOT be interpreted.
        let raw = "sutra.snapshot=2\nsutra.processId=p1\nsutra.status=SUSPENDED\n\
                   sutra.completedNodes=S\nsutra.waitingNodes=U\n\
                   sutra.var.amount=42\nsutra.var.looksTagged=n|7\n";
        let read = InstanceSnapshot::read(raw.as_bytes()).unwrap();
        assert_eq!(read.variables()["amount"], SnapshotValue::from("42"));
        assert_eq!(read.variables()["looksTagged"], SnapshotValue::from("n|7"));
        // A snapshot with no version key at all defaults to the v2 model too.
        let unversioned = InstanceSnapshot::read(b"sutra.var.x=n|7\n").unwrap();
        assert_eq!(unversioned.variables()["x"], SnapshotValue::from("n|7"));
    }

    #[test]
    fn a_v3_encrypted_snapshot_keeps_its_string_value_model() {
        let snap = InstanceSnapshot::of(
            "p1",
            "dep-0123456789abcdef01234567",
            STATUS_RUNNING,
            strs(&["S"]),
            vars(&[("pan", "n|4111"), ("amount", "42")]),
        );
        let cipher = test_cipher("tenant-1");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-1", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&ctx), &enc_set(&["pan"]))
            .unwrap();
        assert!(String::from_utf8(bytes.clone())
            .unwrap()
            .contains("sutra.snapshot=3"));

        let read = InstanceSnapshot::read_encrypted(&bytes, Some(&ctx)).unwrap();
        // The decrypted plaintext is NOT tag-decoded at v3 — that would rewrite history.
        assert_eq!(read.variables()["pan"], SnapshotValue::from("n|4111"));
        assert_eq!(read.variables()["amount"], SnapshotValue::from("42"));
    }

    #[test]
    fn an_encrypted_typed_value_carries_its_type_inside_the_ciphertext() {
        // The at-rest decision: the tag rides INSIDE the envelope, so a sensitive number resumes
        // as a number and the ciphertext still leaks nothing about it beyond its length.
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            BTreeMap::new(),
            strs(&["U"]),
            "S",
            0,
        )
        .with_variables(typed(&[
            (
                "salary",
                SnapshotValue::Number("125000.50".parse().unwrap()),
            ),
            ("cleared", SnapshotValue::Boolean(false)),
        ]));
        let cipher = test_cipher("tenant-1");
        let ctx = SnapshotCrypto::new(&cipher, "tenant-1", "inst-1");
        let bytes = snap
            .write_encrypted(Some(&ctx), &enc_set(&["salary"]))
            .unwrap();

        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains("sutra.snapshot=4"), "{text}");
        assert!(text.contains("sutra.enc.salary="));
        assert!(!text.contains("125000"), "the raw value is on disk: {text}");
        assert!(text.contains("sutra.var.cleared=b|false"));

        let read = InstanceSnapshot::read_encrypted(&bytes, Some(&ctx)).unwrap();
        assert_eq!(
            read.variables()["salary"],
            SnapshotValue::Number("125000.50".parse().unwrap())
        );
        assert_eq!(read.variables()["cleared"], SnapshotValue::Boolean(false));
        // Fail-closed is unchanged by typing.
        assert!(InstanceSnapshot::read(&bytes).is_err());
    }

    #[test]
    fn golden_typed_bytes_are_pinned() {
        // The v4 wire form, pinned like every generation before it. A diff here is a
        // persisted-format break, not a test to update.
        let snap = InstanceSnapshot::of_suspended(
            "p1",
            "dep-0123456789abcdef01234567",
            strs(&["S"]),
            BTreeMap::new(),
            strs(&["U"]),
            "S",
            0,
        )
        .with_variables(typed(&[
            ("amount", SnapshotValue::Number("100.00".parse().unwrap())),
            ("approved", SnapshotValue::Boolean(true)),
            ("due", SnapshotValue::Date("2026-08-05".to_owned())),
            ("label", SnapshotValue::from("hello")),
            (
                "lines",
                SnapshotValue::List(vec![SnapshotValue::Number("1".parse().unwrap())]),
            ),
            ("missing", SnapshotValue::Null),
        ]));
        let expected = concat!(
            "sutra.completedNodes=S\n",
            "sutra.deploymentId=dep-0123456789abcdef01234567\n",
            "sutra.processId=p1\n",
            "sutra.snapshot=4\n",
            "sutra.startNode=S\n",
            "sutra.status=SUSPENDED\n",
            "sutra.var.amount=n|100.00\n",
            "sutra.var.approved=b|true\n",
            "sutra.var.due=d|2026-08-05\n",
            "sutra.var.label=s|hello\n",
            "sutra.var.lines=j|[1]\n",
            "sutra.var.missing=z|\n",
            "sutra.waitingNodes=U\n",
        );
        assert_eq!(String::from_utf8(snap.write()).unwrap(), expected);
    }

    #[test]
    fn every_key_patcher_works_on_a_typed_snapshot() {
        // The key-patchers rewrite the RAW properties map and never decode a value. Typing must
        // not change that — a typed value has to ride each patch through byte for byte, exactly
        // as an encrypted one does.
        let original = parked_typed()
            .with_retry_attempts(BTreeMap::from([("chargeCard".to_string(), 2u32)]))
            .write();
        let before = InstanceSnapshot::read(&original).unwrap();

        let failed =
            InstanceSnapshot::mark_failed(&original, "SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
                .unwrap();
        let read = InstanceSnapshot::read(&failed).unwrap();
        assert_eq!(read.status(), STATUS_FAILED);
        assert_eq!(read.failure_code(), "SUTRA.RUNTIME.TASK.UNCAUGHT");
        assert_eq!(read.variables(), before.variables());
        assert!(String::from_utf8(failed)
            .unwrap()
            .contains("sutra.snapshot=4"));

        for status in [STATUS_COMPLETED, STATUS_TERMINATED] {
            let terminal = InstanceSnapshot::mark_terminal(&original, status).unwrap();
            let read = InstanceSnapshot::read(&terminal).unwrap();
            assert_eq!(read.status(), status);
            assert_eq!(read.variables(), before.variables());
        }

        let migrated = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::from([
                ("U".to_string(), "U2".to_string()),
                ("chargeCard".to_string(), "chargeCard2".to_string()),
            ]),
            Some(9),
        )
        .unwrap();
        let read = InstanceSnapshot::read(&migrated).unwrap();
        assert_eq!(read.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(read.waiting_nodes(), ["U2"]);
        assert_eq!(read.audit_seq(), 9);
        assert_eq!(read.retry_attempts().get("chargeCard2"), Some(&2));
        assert_eq!(read.variables(), before.variables());
        // The patched bytes are already canonical — the writer re-sorts and re-renders the same
        // typed values, so a decode/re-encode is a no-op.
        assert_eq!(read.write(), migrated);
    }

    // ---- the cross-process re-home + the resume-from-failed patch (F2) ----------------------

    #[test]
    fn migrate_rewrites_the_process_id_only_when_one_is_supplied() {
        let original = parked().write();
        let same_process = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            None,
            &BTreeMap::new(),
            None,
        )
        .unwrap();
        assert_eq!(
            InstanceSnapshot::read(&same_process).unwrap().process_id(),
            InstanceSnapshot::read(&original).unwrap().process_id(),
            "a same-process migration must not touch sutra.processId"
        );

        // The CROSS-PROCESS half: one key, patched at the byte level like every other re-stamp —
        // the ciphertext and the key anchor never move, because the AAD binds neither the
        // deployment nor the process.
        let rehomed = InstanceSnapshot::migrate_pinned(
            &original,
            "dep-aaaaaaaaaaaaaaaaaaaaaaaa",
            Some("payments-v2"),
            &BTreeMap::from([("U".to_string(), "U2".to_string())]),
            None,
        )
        .unwrap();
        let read = InstanceSnapshot::read(&rehomed).unwrap();
        assert_eq!(read.process_id(), "payments-v2");
        assert_eq!(read.deployment_id(), "dep-aaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(read.waiting_nodes(), ["U2"]);
        assert_eq!(read.write(), rehomed, "the patched bytes stay canonical");
    }

    #[test]
    fn resume_from_failed_is_the_exact_inverse_of_the_failure_re_stamp() {
        let parked_bytes = parked()
            .with_retry_attempts(BTreeMap::from([("chargeCard".to_string(), 2u32)]))
            .write();
        let failed =
            InstanceSnapshot::mark_failed(&parked_bytes, "SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
                .unwrap();
        let revived = InstanceSnapshot::resume_from_failed(&failed).unwrap();

        // Byte-identical to the pre-failure form: the failure keys are emit-only-when-present, so
        // clearing them leaves exactly the snapshot the fatal step found.
        assert_eq!(
            revived, parked_bytes,
            "resume must restore the snapshot the fatal step started from"
        );
        let read = InstanceSnapshot::read(&revived).unwrap();
        assert_eq!(read.status(), STATUS_SUSPENDED);
        assert_eq!(read.failure_code(), "");
        assert_eq!(read.failure_detail(), "");
        // The burned budget stays burned — resume is not a retry reset.
        assert_eq!(read.retry_attempts().get("chargeCard"), Some(&2));
    }

    #[test]
    fn resume_from_failed_refuses_every_status_that_is_not_failed() {
        // Fail closed: this must never be a way to rewrite live or finished state into a park.
        assert!(InstanceSnapshot::resume_from_failed(&parked().write()).is_err());
        for status in [STATUS_COMPLETED, STATUS_TERMINATED] {
            let terminal = InstanceSnapshot::mark_terminal(&parked().write(), status).unwrap();
            let err = InstanceSnapshot::resume_from_failed(&terminal).unwrap_err();
            assert!(err.contains(status), "{err}");
        }
    }

    #[test]
    fn resume_from_failed_carries_encrypted_values_through_untouched() {
        let raw = "sutra.snapshot=3\nsutra.deploymentId=dep-0123456789abcdef01234567\n\
                   sutra.processId=p1\nsutra.status=FAILED\nsutra.failureCode=SUTRA.X\n\
                   sutra.failureDetail=boom\nsutra.completedNodes=S\nsutra.waitingNodes=U\n\
                   sutra.keyId=acme\nsutra.enc.iban=Y2lwaGVydGV4dA==\n";
        let text = String::from_utf8(InstanceSnapshot::resume_from_failed(raw.as_bytes()).unwrap())
            .unwrap();
        assert!(text.contains("sutra.status=SUSPENDED"));
        assert!(!text.contains("sutra.failureCode"));
        assert!(!text.contains("sutra.failureDetail"));
        assert!(text.contains("sutra.keyId=acme"));
        assert!(
            text.contains("sutra.enc.iban=Y2lwaGVydGV4dA\\=\\=")
                || text.contains("sutra.enc.iban=Y2lwaGVydGV4dA=="),
            "reviving an instance must never need the tenant key: {text}"
        );
    }

    #[test]
    fn a_key_patcher_never_decodes_a_typed_value_it_cannot_understand() {
        // Forward compatibility of the patch shape: a v4 snapshot written by a NEWER codec (an
        // unknown tag) must still be markable as failed/terminal without its values being touched.
        let raw = "sutra.snapshot=4\nsutra.deploymentId=dep-0123456789abcdef01234567\n\
                   sutra.processId=p1\nsutra.status=SUSPENDED\nsutra.completedNodes=S\n\
                   sutra.waitingNodes=U\nsutra.var.exotic=Q|whatever\n";
        let marked = InstanceSnapshot::mark_failed(raw.as_bytes(), "SUTRA.X", "boom").unwrap();
        let text = String::from_utf8(marked).unwrap();
        assert!(text.contains("sutra.status=FAILED"));
        assert!(text.contains("sutra.var.exotic=Q|whatever"), "{text}");
    }

    #[test]
    fn the_failed_re_stamp_carries_retry_counters_through_untouched() {
        // An operator inspecting a FAILED instance must see how many attempts it burned.
        let original = parked()
            .with_retry_attempts(BTreeMap::from([("chargeCard".to_string(), 3u32)]))
            .write();
        let marked = InstanceSnapshot::mark_failed(
            &original,
            "SUTRA.RUNTIME.RETRY.EXHAUSTED",
            "gave up after 3",
        )
        .unwrap();

        let read = InstanceSnapshot::read(&marked).unwrap();
        assert_eq!(read.status(), STATUS_FAILED);
        assert_eq!(read.retry_attempts().get("chargeCard"), Some(&3));
    }
}
