//! The durable audit spool: every audit event an engine emits is appended here, and fsync'd in
//! groups, before the step that produced it commits. One shipper per configured audit store reads
//! the spool and loads it into that store (or the JSONL files are the store), so the engine's own
//! database never carries audit and a crash between the sync and the commit loses nothing.
//!
//! # Record format
//!
//! One UTF-8 JSON object per line, each line ending in `\n`:
//!
//! ```text
//! {"v":1,"spool":"<id>","n":<u64>,"sink":"<name>","deploymentId":…,"tenant":…,"instanceId":…,
//!  "seq":…,"at":…,"eventType":…,"nodeId":…|null,"diagnosticCode":…|null,
//!  "diagnosticJson":…|null,"payloadJson":"<the exact payload_json string>","correlation":{…}}
//! ```
//!
//! `n` is strictly increasing within one spool and never reused. A reader skips any record whose
//! `n` is not above the highest it has already seen, which makes a batch rewritten after an I/O
//! error harmless. `payloadJson` is the payload string verbatim, so a store row built from a
//! record is byte-for-byte the row the event would have produced directly.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::io::{BufRead, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::audit::AuditEvent;

/// The version of the spool record format, the `v` key of every line.
pub const SPOOL_RECORD_VERSION: u64 = 1;

/// One spooled audit event: its position `n` in the spool and the event itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolRecord {
    /// The record's position in its spool: strictly increasing, never reused.
    pub n: u64,
    pub event: AuditEvent,
}

/// Read one key of a parsed line as the type the call site needs; a missing key reads as `null`,
/// so a missing required key is serde's own "invalid type: null" error.
macro_rules! field {
    ($obj:expr, $key:expr) => {
        serde_json::from_value($obj.get($key).cloned().unwrap_or(serde_json::Value::Null))
    };
}

impl SpoolRecord {
    /// Parse one spool line (with or without its trailing newline). Unknown keys are ignored;
    /// a missing or mistyped required key is an error.
    pub fn parse(line: &str) -> Result<SpoolRecord, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_str(line.trim_end_matches(['\n', '\r']))?;
        // A line that is not an object reads as an empty one: its first required key fails.
        let obj = match value {
            serde_json::Value::Object(obj) => obj,
            _ => serde_json::Map::new(),
        };
        let event = AuditEvent {
            sink: field!(obj, "sink")?,
            deployment_id: field!(obj, "deploymentId")?,
            tenant: field!(obj, "tenant")?,
            instance_id: field!(obj, "instanceId")?,
            seq: field!(obj, "seq")?,
            at: field!(obj, "at")?,
            event_type: field!(obj, "eventType")?,
            node_id: field!(obj, "nodeId")?,
            diagnostic_code: field!(obj, "diagnosticCode")?,
            diagnostic_json: field!(obj, "diagnosticJson")?,
            payload_json: field!(obj, "payloadJson")?,
            correlation: match obj.get("correlation") {
                None | Some(serde_json::Value::Null) => Default::default(),
                Some(_) => field!(obj, "correlation")?,
            },
            // Absent on pre-0.9.0 lines and on events outside a pass.
            attempt: match obj.get("attemptId") {
                None | Some(serde_json::Value::Null) => None,
                Some(_) => Some(crate::audit::AuditAttempt {
                    id: field!(obj, "attemptId")?,
                    base: field!(obj, "attemptBase")?,
                    at: field!(obj, "attemptAt")?,
                }),
            },
        };
        Ok(SpoolRecord {
            n: field!(obj, "n")?,
            event,
        })
    }

    /// The record as one spool line of `spool_id`, ending in `\n`.
    pub fn to_line(&self, spool_id: &str) -> String {
        fn s(v: &str) -> String {
            serde_json::Value::String(v.to_string()).to_string()
        }
        fn opt(v: &Option<String>) -> String {
            v.as_deref().map(s).unwrap_or_else(|| "null".to_string())
        }
        let e = &self.event;
        let correlation = serde_json::to_string(&e.correlation).unwrap_or_else(|_| "{}".into());
        format!(
            "{{\"v\":{SPOOL_RECORD_VERSION},\"spool\":{},\"n\":{},\"sink\":{},\"deploymentId\":{},\
             \"tenant\":{},\"instanceId\":{},\"seq\":{},\"at\":{},\"eventType\":{},\"nodeId\":{},\
             \"diagnosticCode\":{},\"diagnosticJson\":{},\"payloadJson\":{},\"correlation\":{}{}}}\n",
            s(spool_id),
            self.n,
            s(&e.sink),
            s(&e.deployment_id),
            s(&e.tenant),
            s(&e.instance_id),
            e.seq,
            s(&e.at),
            s(&e.event_type),
            opt(&e.node_id),
            opt(&e.diagnostic_code),
            opt(&e.diagnostic_json),
            s(&e.payload_json),
            correlation,
            match &e.attempt {
                Some(a) => format!(
                    ",\"attemptId\":{},\"attemptBase\":{},\"attemptAt\":{}",
                    s(&a.id),
                    a.base,
                    s(&a.at)
                ),
                None => String::new(),
            },
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Configuration, errors, metrics
// ---------------------------------------------------------------------------------------------

/// The spool could not sync before the step's barrier timed out: the step commit fails.
pub const AUDIT_SPOOL_SYNC_FAILED: &str = "SUTRA.AUDIT.SPOOL.SYNC_FAILED";
/// A second process tried to open a spool directory another process holds.
pub const AUDIT_SPOOL_LOCKED: &str = "SUTRA.AUDIT.SPOOL.LOCKED";
/// A torn final record was cut off the last segment when the spool opened.
pub const AUDIT_SPOOL_TAIL_TRUNCATED: &str = "SUTRA.AUDIT.SPOOL.TAIL_TRUNCATED";
/// A record in the middle of a segment does not parse; the segment is quarantined.
pub const AUDIT_SPOOL_CORRUPT: &str = "SUTRA.AUDIT.SPOOL.CORRUPT";
/// An audit event named a store this engine does not have; it is counted, never written.
pub const AUDIT_UNDELIVERABLE: &str = "SUTRA.AUDIT.UNDELIVERABLE";

/// Metric: audit events whose store is not configured, by `sink` and `deployment.id`.
pub const METRIC_AUDIT_UNDELIVERABLE: &str = "sutra.audit.undeliverable";
/// Metric: 1 while the spool can sync, 0 while it cannot.
pub const METRIC_AUDIT_SPOOL_HEALTHY: &str = "sutra.audit.spool.healthy";
/// Metric: bytes of segments not yet shipped by every store.
pub const METRIC_AUDIT_SPOOL_UNSHIPPED_BYTES: &str = "sutra.audit.spool.unshipped_bytes";
/// Metric: corrupt records met by a shipper.
pub const METRIC_AUDIT_SPOOL_CORRUPT: &str = "sutra.audit.spool.corrupt";
/// Counter: records of aborted attempts the shippers dropped (their step did not commit).
pub const METRIC_AUDIT_ATTEMPTS_ABORTED: &str = "sutra.audit.attempts_aborted";
/// Metric: failed ship attempts, by `sink`.
pub const METRIC_AUDIT_SHIP_FAILURES: &str = "sutra.audit.ship.failures";
/// Metric: records durable in the spool and not yet shipped, by `sink`.
pub const METRIC_AUDIT_SHIP_LAG: &str = "sutra.audit.ship.lag";
/// Metric: replayed events the JSONL file store dropped as duplicates.
pub const METRIC_AUDIT_REPLAY_DUPLICATES_DROPPED: &str = "sutra.audit.replay_duplicates_dropped";

/// The most messages the writer takes into one group sync.
pub const SPOOL_BATCH_MAX_MESSAGES: usize = 4096;
/// The most record bytes the writer takes into one group sync.
pub const SPOOL_BATCH_MAX_BYTES: usize = 1 << 20;

const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60);
const UNDELIVERABLE_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BARRIER_BACKOFF: Duration = Duration::from_millis(100);

/// The spool's settings (`sutra.audit.spool.*`) and the stores this engine has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditSpoolConfig {
    /// `sutra.audit.spool.dir`.
    pub dir: PathBuf,
    /// `sutra.audit.spool.segment-bytes`: a segment rotates after a sync once it is this big.
    pub segment_bytes: u64,
    /// `sutra.audit.spool.sync-linger-ms`: how long the writer gathers a group before it syncs.
    pub sync_linger: Duration,
    /// `sutra.audit.spool.max-bytes`: past this many unshipped bytes, barriers fail.
    pub max_bytes: u64,
    /// `sutra.audit.spool.retain`: how long a shipped segment is kept.
    pub retain: Duration,
    /// `sutra.audit.spool.sync-timeout`: how long a barrier keeps retrying.
    pub sync_timeout: Duration,
    /// The configured stores. An event naming any other store is undeliverable.
    pub sinks: BTreeSet<String>,
}

impl Default for AuditSpoolConfig {
    fn default() -> AuditSpoolConfig {
        AuditSpoolConfig {
            dir: PathBuf::from("/var/lib/sutra/audit-spool"),
            segment_bytes: 64 * 1024 * 1024,
            sync_linger: Duration::from_millis(5),
            max_bytes: 8 * 1024 * 1024 * 1024,
            retain: Duration::from_secs(3600),
            sync_timeout: Duration::from_secs(30),
            sinks: BTreeSet::new(),
        }
    }
}

/// Why the spool could not do what was asked.
#[derive(Debug)]
pub enum SpoolError {
    /// A write, sync or file operation failed.
    Io(std::io::Error),
    /// More than `max-bytes` is waiting to be shipped.
    Full { unshipped_bytes: u64 },
    /// Another process holds the spool directory.
    Locked(PathBuf),
    /// A record in the middle of a segment does not parse.
    Corrupt { segment: PathBuf, line: u64 },
    /// The spool has shut down.
    Closed,
}

impl std::fmt::Display for SpoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpoolError::Io(e) => write!(f, "audit spool I/O error: {e}"),
            SpoolError::Full { unshipped_bytes } => write!(
                f,
                "audit spool is full: {unshipped_bytes} bytes are not yet shipped"
            ),
            SpoolError::Locked(dir) => write!(
                f,
                "{AUDIT_SPOOL_LOCKED}: audit spool {} is held by another process",
                dir.display()
            ),
            SpoolError::Corrupt { segment, line } => write!(
                f,
                "{AUDIT_SPOOL_CORRUPT}: {} line {line} does not parse",
                segment.display()
            ),
            SpoolError::Closed => write!(f, "the audit spool is closed"),
        }
    }
}

impl std::error::Error for SpoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpoolError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SpoolError {
    fn from(e: std::io::Error) -> SpoolError {
        SpoolError::Io(e)
    }
}

impl SpoolError {
    /// A fresh copy for one more waiter (an `io::Error` cannot be cloned; its kind and text are).
    fn duplicate(&self) -> SpoolError {
        match self {
            SpoolError::Io(e) => SpoolError::Io(std::io::Error::new(e.kind(), e.to_string())),
            SpoolError::Full { unshipped_bytes } => SpoolError::Full {
                unshipped_bytes: *unshipped_bytes,
            },
            SpoolError::Locked(p) => SpoolError::Locked(p.clone()),
            SpoolError::Corrupt { segment, line } => SpoolError::Corrupt {
                segment: segment.clone(),
                line: *line,
            },
            SpoolError::Closed => SpoolError::Closed,
        }
    }
}

/// The spool's counters and gauges, read by the engine's metric callbacks.
#[derive(Debug, Default)]
pub struct AuditSpoolMetrics {
    undeliverable: Mutex<BTreeMap<(String, String), u64>>,
    unshipped_bytes: AtomicU64,
    corrupt: AtomicU64,
    ship_failures: Mutex<BTreeMap<String, u64>>,
    ship_lag: Mutex<BTreeMap<String, u64>>,
    attempts_aborted: AtomicU64,
}

impl AuditSpoolMetrics {
    /// `sutra.audit.attempts_aborted`: records of attempts whose step did not commit, dropped
    /// by the shippers (counted per store).
    pub fn attempts_aborted(&self) -> u64 {
        self.attempts_aborted.load(Ordering::Relaxed)
    }
    pub(crate) fn count_aborted(&self, records: u64) {
        self.attempts_aborted.fetch_add(records, Ordering::Relaxed);
    }
    /// `sutra.audit.undeliverable`, by `(sink, deployment id)`.
    pub fn undeliverable(&self) -> BTreeMap<(String, String), u64> {
        lock(&self.undeliverable).clone()
    }
    /// `sutra.audit.spool.unshipped_bytes`.
    pub fn unshipped_bytes(&self) -> u64 {
        self.unshipped_bytes.load(Ordering::Relaxed)
    }
    /// `sutra.audit.spool.corrupt`.
    pub fn corrupt(&self) -> u64 {
        self.corrupt.load(Ordering::Relaxed)
    }
    /// `sutra.audit.ship.failures`, by sink.
    pub fn ship_failures(&self) -> BTreeMap<String, u64> {
        lock(&self.ship_failures).clone()
    }
    /// `sutra.audit.ship.lag`, by sink.
    pub fn ship_lag(&self) -> BTreeMap<String, u64> {
        lock(&self.ship_lag).clone()
    }
    pub(crate) fn count_corrupt(&self) {
        self.corrupt.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn count_ship_failure(&self, sink: &str) {
        *lock(&self.ship_failures)
            .entry(sink.to_string())
            .or_insert(0) += 1;
    }
    pub(crate) fn set_ship_lag(&self, sink: &str, lag: u64) {
        lock(&self.ship_lag).insert(sink.to_string(), lag);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Test hook: make the next `n` group syncs fail as if the disk had refused them.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct SpoolFaultInjector {
    fail_syncs: AtomicU64,
}

impl SpoolFaultInjector {
    #[doc(hidden)]
    pub fn fail_next_syncs(&self, n: u64) {
        self.fail_syncs.store(n, Ordering::SeqCst);
    }
    fn take(&self) -> bool {
        self.fail_syncs
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_ok()
    }
}

// ---------------------------------------------------------------------------------------------
// File layout helpers
// ---------------------------------------------------------------------------------------------

const SPOOL_ID_FILE: &str = "spool.id";
const SPOOL_LOCK_FILE: &str = "spool.lock";
const QUARANTINE_DIR: &str = "quarantine";

/// The file name of the segment whose first record is `first_n`.
pub fn segment_name(first_n: u64) -> String {
    format!("seg-{first_n:020}.jsonl")
}

fn segment_first_n(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?
        .strip_suffix(".jsonl")
        .filter(|d| d.len() == 20)?
        .parse()
        .ok()
}

/// The segments in `dir`, in name (= first record) order.
fn list_segments(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(first) = entry.file_name().to_str().and_then(segment_first_n) {
            segments.push((first, entry.path()));
        }
    }
    segments.sort();
    Ok(segments)
}

fn checkpoint_path(dir: &Path, sink: &str) -> PathBuf {
    dir.join(format!("ship-{sink}.ckpt"))
}

/// Make the directory entry changes in `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Create `dir` (and its parents) with mode `0700` where the platform has modes.
pub(crate) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    let existed = dir.is_dir();
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)) {
            // A directory this process does not own (a mounted volume, owned by root with
            // group write for the engine) cannot be re-moded; it is used as it is. A directory
            // created here is always ours, so failing on it is a real error.
            if !existed {
                return Err(e);
            }
            tracing::warn!(
                dir = %dir.display(),
                error = %e,
                "could not restrict an audit directory to its owner; using it with its own \
                 permissions"
            );
        }
    }
    Ok(())
}

/// Open options for a new private file (mode `0600` where the platform has modes).
pub(crate) fn private_file_options() -> std::fs::OpenOptions {
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut options = std::fs::OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

/// Write `body` to `path` atomically: a `.tmp` file, fsync'd, renamed over, directory fsync'd.
pub(crate) fn write_atomically(path: &Path, body: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = private_file_options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(body)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// The `through` of the shipper checkpoint of `sink` in `dir`, if it belongs to `spool_id`.
/// A checkpoint of another spool reads as `0`, with a WARN.
pub fn read_checkpoint(dir: &Path, sink: &str, spool_id: &str) -> u64 {
    let path = checkpoint_path(dir, sink);
    let Ok(body) = std::fs::read_to_string(&path) else {
        return 0;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) else {
        tracing::warn!(path = %path.display(), "unreadable audit shipper checkpoint; starting at 0");
        return 0;
    };
    if v["spool"].as_str() != Some(spool_id) {
        tracing::warn!(
            path = %path.display(),
            "audit shipper checkpoint belongs to another spool; starting at 0"
        );
        return 0;
    }
    v["through"].as_u64().unwrap_or(0)
}

/// Write the shipper checkpoint of `sink`: `{"v":1,"spool":…,"sink":…,"through":…}`.
pub fn write_checkpoint(
    dir: &Path,
    sink: &str,
    spool_id: &str,
    through: u64,
) -> std::io::Result<()> {
    write_checkpoint_read(dir, sink, spool_id, through, through)
}

/// [`write_checkpoint`] with the shipper's read position as well: every record up to `through`
/// is settled (shipped or dropped); past it, records up to `read` without an attempt were
/// shipped already, while some attempt records there may still be held for their marker.
pub fn write_checkpoint_read(
    dir: &Path,
    sink: &str,
    spool_id: &str,
    through: u64,
    read: u64,
) -> std::io::Result<()> {
    let body = serde_json::json!({
        "v": 1, "spool": spool_id, "sink": sink, "through": through, "read": read.max(through)
    })
    .to_string();
    write_atomically(&checkpoint_path(dir, sink), body.as_bytes())
}

/// The read position of a checkpoint (see [`write_checkpoint_read`]); its `through` when it
/// has none, 0 when there is no checkpoint of this spool.
pub fn read_checkpoint_read(dir: &Path, sink: &str, spool_id: &str) -> u64 {
    let through = read_checkpoint(dir, sink, spool_id);
    let Ok(body) = std::fs::read_to_string(checkpoint_path(dir, sink)) else {
        return through;
    };
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["read"].as_u64())
        .map_or(through, |read| read.max(through))
}

/// Move `segment` into the spool's `quarantine/` directory (under `suffix`, when given).
fn move_to_quarantine(dir: &Path, segment: &Path, suffix: &str) -> std::io::Result<PathBuf> {
    let quarantine = dir.join(QUARANTINE_DIR);
    create_private_dir(&quarantine)?;
    let name = segment
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let target = quarantine.join(format!("{name}{suffix}"));
    std::fs::rename(segment, &target)?;
    sync_dir(dir)?;
    Ok(target)
}

// ---------------------------------------------------------------------------------------------
// The spool
// ---------------------------------------------------------------------------------------------

enum SpoolMsg {
    Event(Box<AuditEvent>),
    Barrier(tokio::sync::oneshot::Sender<Result<(), SpoolError>>),
    Close(std::sync::mpsc::Sender<Result<(), SpoolError>>),
    Crash,
}

/// State shared by the spool handle, its senders, its writer and its shippers.
pub(crate) struct SpoolShared {
    pub(crate) spool_id: String,
    pub(crate) dir: PathBuf,
    durable: tokio::sync::watch::Sender<u64>,
    healthy: AtomicBool,
    /// Over `max-bytes` unshipped: barriers fail with `Full` until the stores catch up.
    full: AtomicBool,
    /// Barriers sent and not yet answered, across every sender: the writer lingers for more
    /// only while another one is on its way (see [`SpoolWriter::gather`]).
    outstanding_barriers: AtomicU64,
    /// Each store's shipped-through, as its shipper last checkpointed it.
    progress: Mutex<BTreeMap<String, u64>>,
    pub(crate) metrics: Arc<AuditSpoolMetrics>,
}

impl SpoolShared {
    pub(crate) fn set_progress(&self, sink: &str, through: u64) {
        lock(&self.progress).insert(sink.to_string(), through);
    }
    pub(crate) fn durable_through(&self) -> u64 {
        *self.durable.borrow()
    }
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.durable.subscribe()
    }
}

/// The local durable audit spool of this process (see the module docs).
pub struct AuditSpool {
    shared: Arc<SpoolShared>,
    /// `spool.lock`, held (`flock`) for the life of this handle.
    _lock: std::fs::File,
    tx: std::sync::mpsc::Sender<SpoolMsg>,
    sync_timeout: Duration,
    writer: Option<std::thread::JoinHandle<()>>,
    /// The last record a previous life of this spool wrote (`0` for a new spool): the attempts
    /// up to it with no marker are recovery's to decide.
    recovered_through: u64,
    /// The store names this spool serves.
    sinks: BTreeSet<String>,
}

impl AuditSpool {
    /// Open (creating when new) the spool in `cfg.dir`: take its lock, read or mint its id,
    /// truncate a torn tail record off the last segment, and start the writer thread.
    pub fn open(cfg: AuditSpoolConfig) -> Result<AuditSpool, SpoolError> {
        Self::open_with_faults(cfg, None)
    }

    /// [`Self::open`] with a fault injector (tests only).
    #[doc(hidden)]
    pub fn open_with_faults(
        cfg: AuditSpoolConfig,
        faults: Option<Arc<SpoolFaultInjector>>,
    ) -> Result<AuditSpool, SpoolError> {
        create_private_dir(&cfg.dir)?;
        let lock_file = private_file_options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cfg.dir.join(SPOOL_LOCK_FILE))?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(SpoolError::Locked(cfg.dir.clone()));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(SpoolError::Io(e)),
        }
        let spool_id = read_or_mint_spool_id(&cfg.dir)?;
        let recovered = recover_segments(&cfg.dir, cfg.segment_bytes)?;
        let mut progress = BTreeMap::new();
        let mut highest_checkpoint = 0;
        for sink in &cfg.sinks {
            let through = read_checkpoint(&cfg.dir, sink, &spool_id);
            highest_checkpoint = highest_checkpoint.max(through);
            progress.insert(sink.clone(), through);
        }
        let next_n = recovered.next_n.max(highest_checkpoint + 1);
        let (durable, _) = tokio::sync::watch::channel(next_n - 1);
        let shared = Arc::new(SpoolShared {
            spool_id,
            dir: cfg.dir.clone(),
            durable,
            healthy: AtomicBool::new(true),
            full: AtomicBool::new(false),
            outstanding_barriers: AtomicU64::new(0),
            progress: Mutex::new(progress),
            metrics: Arc::new(AuditSpoolMetrics::default()),
        });
        let (tx, rx) = std::sync::mpsc::channel();
        let sync_timeout = cfg.sync_timeout;
        let recovered_sinks = cfg.sinks.clone();
        let mut writer = SpoolWriter {
            cfg,
            shared: Arc::clone(&shared),
            rx,
            next_n,
            segments: recovered.segments,
            write_pos: recovered.write_pos,
            allocated: recovered.write_pos,
            file: recovered.open_file,
            pending: None,
            unsynced: None,
            full: false,
            warned_80: false,
            faults,
            last_housekeeping: Instant::now(),
            undeliverable_logged: HashMap::new(),
        };
        writer.update_unshipped();
        let writer = std::thread::Builder::new()
            .name("sutra-audit-spool".into())
            .spawn(move || writer.run())?;
        Ok(AuditSpool {
            shared,
            _lock: lock_file,
            tx,
            sync_timeout,
            writer: Some(writer),
            recovered_through: next_n.saturating_sub(1),
            sinks: recovered_sinks,
        })
    }

    /// The spool's id (a UUID minted when the directory was first used).
    pub fn spool_id(&self) -> &str {
        &self.shared.spool_id
    }

    /// The spool's directory.
    pub fn dir(&self) -> &Path {
        &self.shared.dir
    }

    /// A sender for listeners, journals and bridges.
    pub fn sender(&self) -> AuditSpoolSender {
        AuditSpoolSender {
            tx: self.tx.clone(),
            shared: Arc::clone(&self.shared),
            sync_timeout: self.sync_timeout,
        }
    }

    /// The highest `n` synced to disk.
    pub fn durable_through(&self) -> u64 {
        self.shared.durable_through()
    }

    /// The last record a previous life of this spool wrote (0 for a new spool).
    pub fn recovered_through(&self) -> u64 {
        self.recovered_through
    }

    /// The lowest checkpoint over the stores this spool serves: every record up to it is
    /// settled in every store.
    pub fn settled_through(&self) -> u64 {
        let progress = lock(&self.shared.progress);
        self.sinks
            .iter()
            .map(|sink| progress.get(sink).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    /// Watch [`Self::durable_through`].
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.subscribe()
    }

    /// A reader over the records after `from_exclusive`.
    pub fn reader(&self, from_exclusive: u64) -> SpoolReader {
        SpoolReader::new_at(self.shared.dir.clone(), from_exclusive)
    }

    /// Whether the last sync succeeded (readiness reports DOWN while it is `false`).
    pub fn healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire)
    }

    /// The spool's metrics.
    pub fn metrics(&self) -> Arc<AuditSpoolMetrics> {
        Arc::clone(&self.shared.metrics)
    }

    pub(crate) fn shared(&self) -> Arc<SpoolShared> {
        Arc::clone(&self.shared)
    }

    /// Drain what is queued, sync it, close the segment and release the lock.
    pub fn shutdown(mut self) -> Result<(), SpoolError> {
        let (ack, done) = std::sync::mpsc::channel();
        let result = if self.tx.send(SpoolMsg::Close(ack)).is_ok() {
            done.recv().unwrap_or(Err(SpoolError::Closed))
        } else {
            Err(SpoolError::Closed)
        };
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        result
    }

    /// Test hook: stop the writer at once, as a killed process would: whatever it has not
    /// synced is lost, the open file is dropped unsynced, and the lock is released.
    #[doc(hidden)]
    pub fn crash(mut self) {
        let _ = self.tx.send(SpoolMsg::Crash);
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

impl Drop for AuditSpool {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let (ack, done) = std::sync::mpsc::channel();
            if self.tx.send(SpoolMsg::Close(ack)).is_ok() {
                let _ = done.recv();
            }
            let _ = writer.join();
        }
    }
}

/// The append side of the spool, cloned into every listener, journal and bridge.
#[derive(Clone)]
pub struct AuditSpoolSender {
    tx: std::sync::mpsc::Sender<SpoolMsg>,
    shared: Arc<SpoolShared>,
    sync_timeout: Duration,
}

impl std::fmt::Debug for AuditSpoolSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditSpoolSender")
            .field("spool_id", &self.shared.spool_id)
            .finish()
    }
}

impl AuditSpoolSender {
    /// Queue `event` for the next group sync. Never blocks; after the spool has closed the event
    /// is dropped.
    pub fn append(&self, event: AuditEvent) {
        let _ = self.tx.send(SpoolMsg::Event(Box::new(event)));
    }

    /// Resolve once every event appended before this call, through this or any other sender on
    /// the same thread, is synced to disk. Retries every 100 ms while the spool cannot sync, until
    /// `sync-timeout`; the last error is returned then.
    pub fn barrier(&self) -> impl Future<Output = Result<(), SpoolError>> + Send + 'static {
        let tx = self.tx.clone();
        let timeout = self.sync_timeout;
        let shared = Arc::clone(&self.shared);
        async move {
            shared.outstanding_barriers.fetch_add(1, Ordering::AcqRel);
            let _outstanding = OutstandingBarrier(&shared);
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                let (answer, answered) = tokio::sync::oneshot::channel();
                if tx.send(SpoolMsg::Barrier(answer)).is_err() {
                    return Err(SpoolError::Closed);
                }
                let result = match tokio::time::timeout_at(deadline, answered).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(SpoolError::Closed),
                    Err(_) => {
                        return Err(SpoolError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "the audit spool did not sync within sync-timeout",
                        )));
                    }
                };
                match result {
                    Ok(()) => return Ok(()),
                    Err(SpoolError::Closed) => return Err(SpoolError::Closed),
                    Err(e) => {
                        if tokio::time::Instant::now() + BARRIER_BACKOFF >= deadline {
                            return Err(e);
                        }
                        tokio::time::sleep(BARRIER_BACKOFF).await;
                    }
                }
            }
        }
    }

    /// The highest `n` synced to disk (see [`AuditSpool::durable_through`]).
    pub fn durable_through(&self) -> u64 {
        self.shared.durable_through()
    }

    /// Whether the spool's last sync succeeded.
    pub fn healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire)
    }

    /// Whether step commits can go ahead: the last sync succeeded and no more than `max-bytes`
    /// waits to be shipped. Readiness reports DOWN while this is `false`.
    pub fn accepting(&self) -> bool {
        self.healthy() && !self.shared.full.load(Ordering::Acquire)
    }

    /// The spool's id.
    pub fn spool_id(&self) -> &str {
        &self.shared.spool_id
    }

    /// The spool's metrics.
    pub fn metrics(&self) -> Arc<AuditSpoolMetrics> {
        Arc::clone(&self.shared.metrics)
    }
}

/// Counts a barrier as outstanding for as long as it waits.
struct OutstandingBarrier<'a>(&'a SpoolShared);

impl Drop for OutstandingBarrier<'_> {
    fn drop(&mut self) {
        self.0.outstanding_barriers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_or_mint_spool_id(dir: &Path) -> std::io::Result<String> {
    let path = dir.join(SPOOL_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => Ok(id.trim().to_string()),
        Ok(_) | Err(_) if !path.exists() || std::fs::metadata(&path)?.len() == 0 => {
            let id = uuid::Uuid::new_v4().to_string();
            write_atomically(&path, id.as_bytes())?;
            Ok(id)
        }
        Ok(_) => Err(std::io::Error::other("spool.id is empty")),
        Err(e) => Err(e),
    }
}

/// Write zeros over `from..to` of `file` (the next sync makes them durable with the records).
fn zero_fill(file: &mut std::fs::File, from: u64, to: u64) -> std::io::Result<()> {
    static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];
    file.seek(std::io::SeekFrom::Start(from))?;
    let mut left = to.saturating_sub(from);
    while left > 0 {
        let n = left.min(ZEROS.len() as u64) as usize;
        file.write_all(&ZEROS[..n])?;
        left -= n as u64;
    }
    Ok(())
}

struct Segment {
    path: PathBuf,
    first_n: u64,
    /// The last record's `n`; `None` while the segment holds none.
    last_n: Option<u64>,
    bytes: u64,
}

struct Recovered {
    segments: Vec<Segment>,
    open_file: Option<std::fs::File>,
    /// The end of the open segment's records.
    write_pos: u64,
    next_n: u64,
}

/// The open segment is zero-filled this far ahead of its records, as a database pre-zeroes its
/// log. A record then lands inside the file's existing size, so its sync is a data-only flush:
/// appending past the end makes every sync also commit the file's new size to the filesystem
/// journal, which costs about three times as much, and every step commit waits for that sync.
/// The zeros are never records: readers and recovery stop at the first NUL, and a closed
/// segment is cut back to its records.
const PREALLOC_CHUNK: u64 = 4 * 1024 * 1024;

/// Read the segment list, scan the last segment, and cut a torn tail off it.
fn recover_segments(dir: &Path, segment_bytes: u64) -> Result<Recovered, SpoolError> {
    let listed = list_segments(dir)?;
    let mut segments = Vec::with_capacity(listed.len());
    for (i, (first_n, path)) in listed.iter().enumerate() {
        let bytes = std::fs::metadata(path)?.len();
        let last_n = listed
            .get(i + 1)
            .and_then(|(next, _)| next.checked_sub(1))
            .filter(|last| *last >= *first_n);
        segments.push(Segment {
            path: path.clone(),
            first_n: *first_n,
            last_n,
            bytes,
        });
    }
    let Some(last) = segments.last_mut() else {
        return Ok(Recovered {
            segments,
            open_file: None,
            write_pos: 0,
            next_n: 1,
        });
    };
    let body = std::fs::read(&last.path)?;
    let mut good_end = 0usize;
    let mut last_good_n: Option<u64> = None;
    let mut start = 0usize;
    while start < body.len() {
        if body[start] == 0 {
            break; // the zero-filled space ahead of the records
        }
        let Some(rel) = body[start..].iter().position(|b| *b == b'\n') else {
            break; // a final line with no newline: torn
        };
        let end = start + rel + 1;
        let parsed = std::str::from_utf8(&body[start..end])
            .ok()
            .and_then(|line| SpoolRecord::parse(line).ok());
        if let Some(record) = parsed {
            last_good_n = Some(record.n);
            good_end = end;
        }
        start = end;
    }
    let file = private_file_options()
        .read(true)
        .write(true)
        .open(&last.path)?;
    if good_end < body.len() {
        file.set_len(good_end as u64)?;
        file.sync_all()?;
        // Zeros are the preallocated space, not a torn record.
        let cut = body[good_end..].iter().filter(|b| **b != 0).count();
        if cut > 0 {
            tracing::warn!(
                code = AUDIT_SPOOL_TAIL_TRUNCATED,
                segment = %last.path.display(),
                bytes = cut,
                "audit spool: a torn tail record was truncated ({cut} bytes)"
            );
        }
    }
    last.bytes = good_end as u64;
    last.last_n = last_good_n;
    let next_n = last_good_n.map_or(last.first_n, |n| n + 1);
    // A segment already at its size rotates now: the next batch starts a new one.
    let open_file = (last.bytes < segment_bytes).then_some(file);
    Ok(Recovered {
        segments,
        open_file,
        write_pos: good_end as u64,
        next_n,
    })
}

/// A batch the writer could not sync: kept in memory and retried into a new segment.
struct PendingBatch {
    first_n: u64,
    last_n: u64,
    text: String,
}

struct SpoolWriter {
    cfg: AuditSpoolConfig,
    shared: Arc<SpoolShared>,
    rx: std::sync::mpsc::Receiver<SpoolMsg>,
    next_n: u64,
    segments: Vec<Segment>,
    /// The open (last) segment, when there is one to append to.
    file: Option<std::fs::File>,
    /// Where the next record goes in the open segment: the end of its records.
    write_pos: u64,
    /// How far the open segment is zero-filled ahead of `write_pos` (see [`PREALLOC_CHUNK`]).
    allocated: u64,
    pending: Option<PendingBatch>,
    /// Records written to the open segment and not yet synced, and when the oldest was
    /// written. They are synced when a barrier or a close needs them, or once the oldest is
    /// `sync-linger` old: an event no step waits for is durable within one linger.
    unsynced: Option<(PendingBatch, Instant)>,
    /// Over `max-bytes` after the last batch: later barriers answer `Full`.
    full: bool,
    warned_80: bool,
    faults: Option<Arc<SpoolFaultInjector>>,
    last_housekeeping: Instant,
    undeliverable_logged: HashMap<(String, String), Instant>,
}

impl SpoolWriter {
    fn run(mut self) {
        let mut next_retry = Instant::now() + RETRY_INTERVAL;
        loop {
            let wait = if self.pending.is_some() {
                next_retry.saturating_duration_since(Instant::now())
            } else if let Some((_, since)) = &self.unsynced {
                self.cfg.sync_linger.saturating_sub(since.elapsed())
            } else {
                HOUSEKEEPING_INTERVAL.saturating_sub(self.last_housekeeping.elapsed())
            };
            let first = match self.rx.recv_timeout(wait) {
                Ok(msg) => Some(msg),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if self.pending.is_some() {
                        self.retry_pending();
                    }
                    self.close_segment();
                    return;
                }
            };
            if self.pending.is_some() && Instant::now() >= next_retry {
                self.retry_pending();
                next_retry = Instant::now() + RETRY_INTERVAL;
            }
            if first.is_none()
                && self
                    .unsynced
                    .as_ref()
                    .is_some_and(|(_, since)| since.elapsed() >= self.cfg.sync_linger)
            {
                let _ = self.sync_unsynced();
            }
            if let Some(first) = first {
                let batch = self.gather(first);
                match self.process(batch) {
                    Flow::Continue => {}
                    Flow::Crash => return,
                    Flow::Close(ack) => {
                        if self.pending.is_some() {
                            self.retry_pending();
                        }
                        let result = match &self.pending {
                            Some(_) => Err(SpoolError::Io(std::io::Error::other(
                                "the audit spool could not sync its last batch",
                            ))),
                            None => Ok(()),
                        };
                        self.close_segment();
                        let _ = ack.send(result);
                        return;
                    }
                }
            }
            if self.last_housekeeping.elapsed() >= HOUSEKEEPING_INTERVAL {
                self.housekeeping();
            }
        }
    }

    /// Step 2: gather more messages until the linger passes, or a cap is reached.
    ///
    /// The linger is a group-commit window, as a database's commit delay: it is waited only
    /// while a barrier is in the batch and another barrier is still outstanding, so a lone
    /// step (one lane, light load) never waits for company that is not coming.
    fn gather(&self, first: SpoolMsg) -> Vec<SpoolMsg> {
        let deadline = Instant::now() + self.cfg.sync_linger;
        let mut barriers = usize::from(matches!(first, SpoolMsg::Barrier(_)));
        let mut batch = vec![first];
        let mut bytes = 0usize;
        while batch.len() < SPOOL_BATCH_MAX_MESSAGES && bytes < SPOOL_BATCH_MAX_BYTES {
            let siblings = barriers > 0
                && self.shared.outstanding_barriers.load(Ordering::Acquire) > barriers as u64;
            let next = if self.cfg.sync_linger.is_zero() || !siblings {
                self.rx.try_recv().ok()
            } else {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    self.rx.try_recv().ok()
                } else {
                    self.rx.recv_timeout(left).ok()
                }
            };
            let Some(msg) = next else { break };
            if let SpoolMsg::Event(e) = &msg {
                bytes += e.payload_json.len() + 256;
            }
            if matches!(msg, SpoolMsg::Barrier(_)) {
                barriers += 1;
            }
            let stop = matches!(msg, SpoolMsg::Close(_) | SpoolMsg::Crash);
            batch.push(msg);
            if stop {
                break;
            }
        }
        batch
    }

    /// Steps 3–6 for one gathered batch.
    fn process(&mut self, batch: Vec<SpoolMsg>) -> Flow {
        // A batch that hit a cap closes the group at once, linger or not.
        let capped = batch.len() >= SPOOL_BATCH_MAX_MESSAGES;
        let mut text = String::new();
        let mut first_n = None;
        let mut barriers = Vec::new();
        let mut close = None;
        for msg in batch {
            match msg {
                SpoolMsg::Event(event) => {
                    // A commit marker is for every store (`*`).
                    if !self.cfg.sinks.contains(&event.sink)
                        && !crate::audit_attempts::is_marker(&event)
                    {
                        self.undeliverable(&event);
                        continue;
                    }
                    let n = self.next_n;
                    self.next_n += 1;
                    first_n.get_or_insert(n);
                    let record = SpoolRecord { n, event: *event };
                    text.push_str(&record.to_line(&self.shared.spool_id));
                }
                SpoolMsg::Barrier(answer) => barriers.push(answer),
                SpoolMsg::Close(ack) => close = Some(ack),
                SpoolMsg::Crash => return Flow::Crash,
            }
        }
        let last_n = self.next_n - 1;
        let outcome: Result<(), SpoolError> = if let Some(pending) = &mut self.pending {
            // Unhealthy: hold the new records behind the failed batch; barriers fail at once.
            pending.text.push_str(&text);
            if first_n.is_some() {
                pending.last_n = last_n;
            }
            Err(SpoolError::Io(std::io::Error::other(
                "the audit spool is unhealthy: its last sync failed",
            )))
        } else {
            let written = match first_n {
                Some(first_n) => self.write_unsynced(first_n, last_n, text),
                None => Ok(()),
            };
            match written {
                Err(e) => Err(SpoolError::Io(e)),
                Ok(()) => {
                    let due = !barriers.is_empty()
                        || close.is_some()
                        || capped
                        || self
                            .unsynced
                            .as_ref()
                            .is_some_and(|(held, _)| held.text.len() >= SPOOL_BATCH_MAX_BYTES)
                        || self
                            .unsynced
                            .as_ref()
                            .is_some_and(|(_, since)| since.elapsed() >= self.cfg.sync_linger);
                    if due {
                        self.sync_unsynced().map_err(SpoolError::Io)
                    } else {
                        Ok(())
                    }
                }
            }
        };
        let outcome = match outcome {
            Ok(()) if self.full => Err(SpoolError::Full {
                unshipped_bytes: self.shared.metrics.unshipped_bytes(),
            }),
            other => other,
        };
        for answer in barriers {
            let _ = answer.send(match &outcome {
                Ok(()) => Ok(()),
                Err(e) => Err(e.duplicate()),
            });
        }
        if self.pending.is_none() && self.unsynced.is_none() {
            self.after_sync();
        }
        match close {
            Some(ack) => Flow::Close(ack),
            None => Flow::Continue,
        }
    }

    /// Append `text` (records `first_n..=last_n`) to the open segment without syncing it; it
    /// joins the unsynced records. A failed write fails the spool as a failed sync would.
    fn write_unsynced(&mut self, first_n: u64, last_n: u64, text: String) -> std::io::Result<()> {
        match self.write_records(first_n, last_n, &text) {
            Ok(()) => {
                match &mut self.unsynced {
                    Some((held, _)) => {
                        held.last_n = last_n;
                        held.text.push_str(&text);
                    }
                    None => {
                        self.unsynced = Some((
                            PendingBatch {
                                first_n,
                                last_n,
                                text,
                            },
                            Instant::now(),
                        ))
                    }
                }
                Ok(())
            }
            Err(e) => {
                let mut held = self.unsynced.take().map(|(held, _)| held);
                match &mut held {
                    Some(held) => {
                        held.last_n = last_n;
                        held.text.push_str(&text);
                    }
                    None => {
                        held = Some(PendingBatch {
                            first_n,
                            last_n,
                            text,
                        })
                    }
                }
                self.fail(held.expect("set above"), &e);
                Err(e)
            }
        }
    }

    /// Sync the unsynced records and publish them as durable. A failed sync holds them for a
    /// retry into a new segment, and the spool is unhealthy until it succeeds.
    fn sync_unsynced(&mut self) -> std::io::Result<()> {
        let Some((held, _)) = self.unsynced.take() else {
            return Ok(());
        };
        match self.sync_open() {
            Ok(()) => {
                self.publish(held.last_n);
                self.after_sync();
                Ok(())
            }
            Err(e) => {
                self.fail(held, &e);
                Err(e)
            }
        }
    }

    /// The spool could not write or sync `held`: never touch the open file again, keep the
    /// records in memory and go unhealthy until a retry into a new segment succeeds.
    fn fail(&mut self, held: PendingBatch, e: &std::io::Error) {
        tracing::error!(
            code = AUDIT_SPOOL_SYNC_FAILED,
            error = %e,
            "audit spool write or sync failed; retrying into a new segment"
        );
        self.shared.healthy.store(false, Ordering::Release);
        self.abandon_segment(held.first_n);
        self.pending = Some(held);
    }

    /// Append `text` (records `first_n..=last_n`) to the open segment and sync it.
    fn write_and_sync(&mut self, first_n: u64, last_n: u64, text: &str) -> std::io::Result<()> {
        self.write_records(first_n, last_n, text)?;
        self.sync_open()
    }

    /// Append `text` (records `first_n..=last_n`) to the open segment (its page cache).
    fn write_records(&mut self, first_n: u64, last_n: u64, text: &str) -> std::io::Result<()> {
        if self.file.is_none() {
            self.start_segment(first_n)?;
        }
        let file = self.file.as_mut().expect("a segment is open");
        let need = self.write_pos + text.len() as u64;
        if need > self.allocated {
            let target = need.max((self.allocated + PREALLOC_CHUNK).min(self.cfg.segment_bytes));
            zero_fill(file, self.allocated, target)?;
            self.allocated = target;
        }
        file.seek(std::io::SeekFrom::Start(self.write_pos))?;
        file.write_all(text.as_bytes())?;
        file.flush()?;
        self.write_pos = need;
        let segment = self.segments.last_mut().expect("a segment is open");
        segment.bytes += text.len() as u64;
        segment.last_n = Some(last_n);
        Ok(())
    }

    /// Sync the open segment's data.
    fn sync_open(&mut self) -> std::io::Result<()> {
        if self.faults.as_ref().is_some_and(|f| f.take()) {
            return Err(std::io::Error::other("injected sync failure"));
        }
        match &self.file {
            Some(file) => file.sync_data(),
            None => Ok(()),
        }
    }

    /// Create the segment whose first record is `first_n` and make it the open one. A file of
    /// that name left by a failed attempt holds only records about to be rewritten: it moves to
    /// `quarantine/`.
    fn start_segment(&mut self, first_n: u64) -> std::io::Result<()> {
        let path = self.cfg.dir.join(segment_name(first_n));
        if path.exists() {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            move_to_quarantine(&self.cfg.dir, &path, &format!(".failed-{stamp}"))?;
            self.segments.retain(|s| s.path != path);
        }
        let file = private_file_options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        sync_dir(&self.cfg.dir)?;
        self.write_pos = 0;
        self.allocated = 0;
        self.segments.push(Segment {
            path,
            first_n,
            last_n: None,
            bytes: 0,
        });
        self.file = Some(file);
        Ok(())
    }

    /// After a failed write: never touch that file again (Linux fsync-error semantics). What it
    /// may hold of the failed batch is rewritten into a new segment, so its records end before
    /// `first_n`.
    fn abandon_segment(&mut self, first_n: u64) {
        self.file = None;
        if let Some(segment) = self.segments.last_mut() {
            segment.last_n = first_n
                .checked_sub(1)
                .filter(|last| *last >= segment.first_n);
            // `bytes` keeps the records that synced: the file's own length counts the
            // zero-filled space ahead of them.
        }
    }

    /// Retry the failed batch into a new segment named after its first record.
    fn retry_pending(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let attempt = self
            .start_segment(pending.first_n)
            .and_then(|()| self.write_and_sync(pending.first_n, pending.last_n, &pending.text));
        match attempt {
            Ok(()) => {
                tracing::info!(
                    first = pending.first_n,
                    last = pending.last_n,
                    "audit spool recovered: the held batch is synced"
                );
                self.shared.healthy.store(true, Ordering::Release);
                self.publish(pending.last_n);
                self.after_sync();
            }
            Err(e) => {
                tracing::error!(
                    code = AUDIT_SPOOL_SYNC_FAILED,
                    error = %e,
                    "audit spool retry failed; retrying again in 1 s"
                );
                self.abandon_segment(pending.first_n);
                self.pending = Some(pending);
            }
        }
    }

    fn publish(&self, durable: u64) {
        self.shared.durable.send_replace(durable);
    }

    /// Step 6: rotation, then the `max-bytes` check.
    fn after_sync(&mut self) {
        let rotate = self
            .segments
            .last()
            .is_some_and(|s| self.file.is_some() && s.bytes >= self.cfg.segment_bytes);
        if rotate {
            self.close_segment();
            self.sweep_segments();
        }
        self.update_unshipped();
    }

    fn close_segment(&mut self) {
        if let Some(file) = self.file.take() {
            // Cut the zero-filled space off: a closed segment holds its records only.
            if let Err(e) = file.set_len(self.write_pos) {
                tracing::warn!(error = %e, "audit spool: trimming the closed segment failed");
            }
            if let Err(e) = file.sync_all() {
                tracing::warn!(error = %e, "audit spool: closing segment sync failed");
            }
            let _ = sync_dir(&self.cfg.dir);
        }
    }

    /// The lowest shipped-through over every configured store.
    fn min_shipped(&self) -> u64 {
        let progress = lock(&self.shared.progress);
        self.cfg
            .sinks
            .iter()
            .map(|sink| progress.get(sink).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    fn update_unshipped(&mut self) {
        let shipped = self.min_shipped();
        let unshipped: u64 = self
            .segments
            .iter()
            .filter(|s| s.last_n.is_some_and(|last| last > shipped))
            .map(|s| s.bytes)
            .sum();
        self.shared
            .metrics
            .unshipped_bytes
            .store(unshipped, Ordering::Relaxed);
        let full = unshipped > self.cfg.max_bytes;
        if full && !self.full {
            tracing::error!(
                code = AUDIT_SPOOL_SYNC_FAILED,
                unshipped_bytes = unshipped,
                max_bytes = self.cfg.max_bytes,
                "audit spool is over max-bytes: step commits fail until the audit stores catch up"
            );
        } else if !full && self.full {
            tracing::info!("audit spool is back under max-bytes: step commits go ahead");
        }
        self.full = full;
        self.shared.full.store(full, Ordering::Release);
        let warn_at = self.cfg.max_bytes / 10 * 8;
        if unshipped >= warn_at && !self.warned_80 {
            self.warned_80 = true;
            tracing::warn!(
                unshipped_bytes = unshipped,
                max_bytes = self.cfg.max_bytes,
                "audit spool is over 80% of max-bytes: the audit stores are not keeping up"
            );
        } else if unshipped < warn_at {
            self.warned_80 = false;
        }
    }

    /// Delete each segment every store has shipped, once it is older than `retain`. The
    /// newest segment is never deleted (it is the open one, and it carries the next `n`).
    fn sweep_segments(&mut self) {
        let shipped = self.min_shipped();
        let retain = self.cfg.retain;
        let newest = self.segments.last().map(|s| s.path.clone());
        let mut removed = false;
        self.segments.retain(|s| {
            if Some(&s.path) == newest.as_ref() {
                return true;
            }
            let done = s.last_n.is_none_or(|last| last <= shipped);
            let old = std::fs::metadata(&s.path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age >= retain);
            if done && old {
                match std::fs::remove_file(&s.path) {
                    Ok(()) => {
                        removed = true;
                        return false;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
                    Err(e) => tracing::warn!(
                        segment = %s.path.display(),
                        error = %e,
                        "audit spool: a shipped segment could not be deleted"
                    ),
                }
            }
            true
        });
        if removed {
            let _ = sync_dir(&self.cfg.dir);
        }
    }

    fn housekeeping(&mut self) {
        self.last_housekeeping = Instant::now();
        // A segment a shipper quarantined is gone from the directory.
        self.segments.retain(|s| s.path.exists());
        self.sweep_segments();
        self.update_unshipped();
    }

    /// An event whose store is not configured is counted and logged, never written or re-routed.
    fn undeliverable(&mut self, event: &AuditEvent) {
        let key = (event.sink.clone(), event.deployment_id.clone());
        *lock(&self.shared.metrics.undeliverable)
            .entry(key.clone())
            .or_insert(0) += 1;
        let now = Instant::now();
        let due = self
            .undeliverable_logged
            .get(&key)
            .is_none_or(|at| now.duration_since(*at) >= UNDELIVERABLE_LOG_INTERVAL);
        if due {
            self.undeliverable_logged.insert(key, now);
            let sink = if event.sink.is_empty() {
                "<none>"
            } else {
                event.sink.as_str()
            };
            tracing::error!(
                code = AUDIT_UNDELIVERABLE,
                sink,
                deployment = %event.deployment_id,
                "audit events for store '{sink}' are not recorded: no such audit store is \
                 configured on this engine"
            );
        }
    }
}

enum Flow {
    Continue,
    Close(std::sync::mpsc::Sender<Result<(), SpoolError>>),
    Crash,
}

// ---------------------------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------------------------

/// Reads a spool's records in `n` order, across segments in name order.
pub struct SpoolReader {
    dir: PathBuf,
    /// The highest `n` returned (or the starting point).
    high: u64,
    /// The segment being read, and the byte offset and line number reached in it.
    segment: Option<PathBuf>,
    offset: u64,
    line: u64,
}

impl SpoolReader {
    pub(crate) fn new_at(dir: PathBuf, from_exclusive: u64) -> SpoolReader {
        SpoolReader {
            dir,
            high: from_exclusive,
            segment: None,
            offset: 0,
            line: 0,
        }
    }

    /// The highest `n` this reader has returned (or started after).
    pub fn position(&self) -> u64 {
        self.high
    }

    /// The segment being read, if any.
    pub fn current_segment(&self) -> Option<&Path> {
        self.segment.as_deref()
    }

    /// Up to `max` records with `position < n ≤ upto`, in order. A record at or below the
    /// highest already returned is skipped. A torn last line of an older segment (left by a
    /// failed write) is skipped; one that does not parse in the middle of a segment is
    /// [`SpoolError::Corrupt`].
    pub fn next_batch(&mut self, max: usize, upto: u64) -> Result<Vec<SpoolRecord>, SpoolError> {
        let mut out = Vec::new();
        if max == 0 || upto <= self.high {
            return Ok(out);
        }
        let segments = list_segments(&self.dir)?;
        if segments.is_empty() {
            return Ok(out);
        }
        // Where to read from: the current segment if it is still there, else the segment that
        // can hold `high + 1`.
        let mut index = match &self.segment {
            Some(current) => match segments.iter().position(|(_, p)| p == current) {
                Some(i) => i,
                None => {
                    self.offset = 0;
                    self.line = 0;
                    let name_first = current
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(segment_first_n)
                        .unwrap_or(0);
                    match segments.iter().position(|(first, _)| *first > name_first) {
                        Some(i) => i,
                        None => return Ok(out),
                    }
                }
            },
            None => {
                self.offset = 0;
                self.line = 0;
                segments
                    .iter()
                    .rposition(|(first, _)| *first <= self.high + 1)
                    .unwrap_or(0)
            }
        };
        loop {
            let (_, path) = &segments[index];
            let is_last = index + 1 == segments.len();
            if self.segment.as_ref() != Some(path) {
                self.segment = Some(path.clone());
                self.offset = 0;
                self.line = 0;
            }
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if is_last {
                        return Ok(out);
                    }
                    index += 1;
                    continue;
                }
                Err(e) => return Err(SpoolError::Io(e)),
            };
            let mut reader = std::io::BufReader::new(file);
            reader.seek(std::io::SeekFrom::Start(self.offset))?;
            let mut buf = Vec::new();
            let mut stop = false;
            loop {
                if out.len() >= max {
                    return Ok(out);
                }
                buf.clear();
                // The open segment is zero-filled ahead of its records: a NUL is the end.
                if reader.fill_buf()?.first() == Some(&0) {
                    if is_last {
                        return Ok(out);
                    }
                    break;
                }
                let read = reader.read_until(b'\n', &mut buf)?;
                if read == 0 {
                    break; // end of this segment
                }
                if buf.last() != Some(&b'\n') {
                    // An incomplete line: still being written (last segment), or a failed
                    // write's torn tail (older segment).
                    if is_last {
                        return Ok(out);
                    }
                    break;
                }
                let parsed = std::str::from_utf8(&buf)
                    .ok()
                    .and_then(|line| SpoolRecord::parse(line).ok());
                match parsed {
                    Some(record) => {
                        if record.n > upto {
                            stop = true;
                            break;
                        }
                        self.offset += read as u64;
                        self.line += 1;
                        if record.n > self.high {
                            self.high = record.n;
                            out.push(record);
                        }
                    }
                    None => {
                        // A final line of an older segment that does not parse is a failed
                        // write's tail; anywhere else it is corruption.
                        let mut rest = [0u8; 1];
                        let at_end = std::io::Read::read(&mut reader, &mut rest)? == 0;
                        if at_end && !is_last {
                            break;
                        }
                        if at_end && is_last {
                            // The writer has not finished this line yet (it is past `upto`).
                            return Ok(out);
                        }
                        return Err(SpoolError::Corrupt {
                            segment: path.clone(),
                            line: self.line + 1,
                        });
                    }
                }
            }
            if stop || is_last {
                return Ok(out);
            }
            index += 1;
        }
    }

    /// After [`SpoolError::Corrupt`]: skip the corrupt line and go on in the same segment.
    pub fn skip_line(&mut self) -> Result<(), SpoolError> {
        let Some(path) = &self.segment else {
            return Ok(());
        };
        let mut reader = std::io::BufReader::new(std::fs::File::open(path)?);
        reader.seek(std::io::SeekFrom::Start(self.offset))?;
        let mut buf = Vec::new();
        let read = reader.read_until(b'\n', &mut buf)?;
        self.offset += read as u64;
        self.line += 1;
        Ok(())
    }

    /// After [`SpoolError::Corrupt`]: leave the current segment and go on with the next one.
    pub fn skip_segment(&mut self) {
        self.offset = 0;
        self.line = 0;
        // Keep `segment` naming the gone file: the next read starts after it by name.
    }
}

/// After a shipper met [`SpoolError::Corrupt`] in `segment`: move it to `quarantine/` and log
/// ERROR, unless it is the newest segment (the one the writer appends to), whose corrupt line is
/// skipped instead. Returns whether the segment was moved.
pub fn quarantine_segment(
    dir: &Path,
    segment: &Path,
    line: u64,
    metrics: &AuditSpoolMetrics,
) -> bool {
    metrics.count_corrupt();
    let newest = list_segments(dir)
        .ok()
        .and_then(|s| s.last().map(|(_, p)| p.clone()));
    if newest.as_deref() == Some(segment) {
        tracing::error!(
            code = AUDIT_SPOOL_CORRUPT,
            segment = %segment.display(),
            line,
            "audit spool: a record does not parse; it is skipped (the segment is still open)"
        );
        return false;
    }
    match move_to_quarantine(dir, segment, "") {
        Ok(target) => {
            tracing::error!(
                code = AUDIT_SPOOL_CORRUPT,
                segment = %segment.display(),
                quarantined = %target.display(),
                line,
                "audit spool: a record does not parse; the segment is quarantined"
            );
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => {
            tracing::error!(
                code = AUDIT_SPOOL_CORRUPT,
                segment = %segment.display(),
                error = %e,
                "audit spool: a corrupt segment could not be quarantined"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AuditEvent {
        AuditEvent {
            sink: "jsonl".into(),
            deployment_id: "dep-000000000000000000000001".into(),
            tenant: "acme".into(),
            instance_id: "11111111-1111-1111-1111-111111111111".into(),
            seq: 7,
            at: "2026-11-24T12:00:00Z".into(),
            event_type: "NODE_ENTERED".into(),
            node_id: Some("Start".into()),
            diagnostic_code: None,
            diagnostic_json: Some("{\"error\":\"x\"}".into()),
            payload_json: "{\"b\":1,  \"a\":\"\\u00e9\"}".into(),
            correlation: [("orderRef".to_string(), "R-1".to_string())]
                .into_iter()
                .collect(),
            attempt: Some(crate::audit::AuditAttempt {
                id: "a-1".into(),
                base: 6,
                at: "2026-11-24T12:00:00.000000001Z".into(),
            }),
        }
    }

    #[test]
    fn record_line_round_trips_and_keeps_the_payload_string_verbatim() {
        let record = SpoolRecord {
            n: 42,
            event: sample(),
        };
        let line = record.to_line("spool-a");
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1);
        let back = SpoolRecord::parse(&line).unwrap();
        assert_eq!(back, record);
        assert_eq!(back.event.payload_json, "{\"b\":1,  \"a\":\"\\u00e9\"}");
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["v"], 1);
        assert_eq!(v["spool"], "spool-a");
        assert_eq!(v["diagnosticCode"], serde_json::Value::Null);
    }

    #[test]
    fn a_torn_or_partial_line_does_not_parse() {
        let line = SpoolRecord {
            n: 1,
            event: sample(),
        }
        .to_line("s");
        assert!(SpoolRecord::parse(&line[..line.len() / 2]).is_err());
        assert!(SpoolRecord::parse("[1,2]").is_err());
        assert!(SpoolRecord::parse("{\"v\":1}").is_err());
    }
}
