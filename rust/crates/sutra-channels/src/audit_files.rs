//! The JSONL file audit store (`"jsonl"`): the spool's records as rotated, append-only files.
//!
//! - Files are `audit-<spoolId>-<UTC yyyymmddTHHMMSSZ of open>.jsonl` in `sutra.audit.jsonl.dir`.
//!   The newest file of a spool id is the open one. A file rotates before a batch write once it
//!   is `rotate-bytes` big or `rotate-interval` old.
//! - A line is [`AuditEvent::to_jsonl`] (the `sutra audit-replay --from-jsonl` contract) plus
//!   `"diagnosticJson"` and `"spoolRef":"<spoolId>:<n>"`.
//! - A batch is written, then `sync_data`'d. [`AuditSink::recover`] reads the newest file's last
//!   line, so a crash between a write and the shipper's checkpoint ships nothing twice.
//! - A record shipped twice (a crash between the write and the checkpoint) is dropped within
//!   `dedup-window` (rebuilt from the files at open) by its `(deploymentId, instanceId, seq,
//!   attemptId)`; past it, readers drop duplicates on that key. A step re-run after a failed or
//!   lost commit re-emits the same seqs under a NEW attempt: both attempts' lines are written
//!   (the files are append-only), and readers keep, per `(deploymentId, instanceId,
//!   attemptBase)`, only the latest attempt (`attemptAt`, then `attemptId`), as `sutra
//!   audit-replay` does. A transport event carries `seq` 0 and no attempt and is never deduped.
//! - With `retention` set, a timer deletes closed files older than it every
//!   `retention-sweep-interval`. The open file is never deleted.

use std::collections::{HashSet, VecDeque};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use crate::audit::{AuditEvent, AuditSink, AUDIT_SINK_WRITE_FAILED};
use crate::audit_spool::{create_private_dir, private_file_options, sync_dir, SpoolRecord};
use crate::diag::Diagnostic;
use crate::sink::BoxFuture;

/// The most `(deploymentId, instanceId, seq, attemptId)` keys the replay dedup cache holds.
pub const DEDUP_CACHE_CAP: usize = 1_000_000;
/// How much of a file's end [`JsonlFileStore`] reads to find its last line.
const TAIL_READ_BYTES: u64 = 1 << 20;

/// The file store's settings (`sutra.audit.jsonl.*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditFilesConfig {
    /// `sutra.audit.jsonl.dir`.
    pub dir: PathBuf,
    /// `sutra.audit.jsonl.rotate-bytes`.
    pub rotate_bytes: u64,
    /// `sutra.audit.jsonl.rotate-interval`.
    pub rotate_interval: Duration,
    /// `sutra.audit.jsonl.retention`: `None` keeps every file.
    pub retention: Option<Duration>,
    /// `sutra.audit.jsonl.dedup-window`.
    pub dedup_window: Duration,
    /// `sutra.audit.retention-sweep-interval`: how often the retention timer runs.
    pub retention_sweep_interval: Duration,
}

impl AuditFilesConfig {
    /// The documented defaults for a store in `dir`.
    pub fn new(dir: impl Into<PathBuf>) -> AuditFilesConfig {
        AuditFilesConfig {
            dir: dir.into(),
            rotate_bytes: 256 * 1024 * 1024,
            rotate_interval: Duration::from_secs(3600),
            retention: None,
            dedup_window: Duration::from_secs(15 * 60),
            retention_sweep_interval: Duration::from_secs(600),
        }
    }
}

type DedupKey = (String, String, u32, String);

/// The replay dedup cache: the keys written within the window, oldest first.
struct DedupCache {
    window: Duration,
    keys: HashSet<DedupKey>,
    order: VecDeque<(SystemTime, DedupKey)>,
}

impl DedupCache {
    fn new(window: Duration) -> DedupCache {
        DedupCache {
            window,
            keys: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn expire(&mut self, now: SystemTime) {
        while let Some((at, _)) = self.order.front() {
            let old = now.duration_since(*at).unwrap_or_default() > self.window;
            if !old && self.order.len() <= DEDUP_CACHE_CAP {
                break;
            }
            if let Some((_, key)) = self.order.pop_front() {
                self.keys.remove(&key);
            }
        }
    }

    /// `true` when `key` is new (and now remembered); `false` for a duplicate.
    fn admit(&mut self, key: DedupKey, at: SystemTime) -> bool {
        if self.window.is_zero() {
            return true;
        }
        if !self.keys.insert(key.clone()) {
            return false;
        }
        self.order.push_back((at, key));
        self.expire(SystemTime::now());
        true
    }
}

fn dedup_key(event: &AuditEvent) -> Option<DedupKey> {
    (event.seq != 0).then(|| {
        (
            event.deployment_id.clone(),
            event.instance_id.clone(),
            event.seq,
            event
                .attempt
                .as_ref()
                .map(|a| a.id.clone())
                .unwrap_or_default(),
        )
    })
}

/// One file-store line: the replay contract plus `diagnosticJson` and `spoolRef`.
pub fn file_line(event: &AuditEvent, spool_id: &str, n: u64) -> String {
    let mut line = event.to_jsonl();
    // `to_jsonl` is always one JSON object: extend it before its closing brace.
    line.pop();
    let diagnostic = match &event.diagnostic_json {
        Some(d) => serde_json::Value::String(d.clone()).to_string(),
        None => "null".to_string(),
    };
    let spool_ref = serde_json::Value::String(format!("{spool_id}:{n}")).to_string();
    line.push_str(&format!(
        ",\"diagnosticJson\":{diagnostic},\"spoolRef\":{spool_ref}}}\n"
    ));
    line
}

struct OpenFile {
    path: PathBuf,
    file: std::fs::File,
    opened: SystemTime,
    size: u64,
}

struct FilesState {
    open: Option<OpenFile>,
    dedup: DedupCache,
    /// The open-time stamp of the last file created, so a rotation within the same second still
    /// gets a later name.
    last_stamp: Option<SystemTime>,
}

struct FilesInner {
    cfg: AuditFilesConfig,
    spool_id: String,
    /// `Some(path)` for the deprecated single-file target (no rotation, no retention).
    legacy: Option<PathBuf>,
    state: Mutex<FilesState>,
    duplicates_dropped: Arc<AtomicU64>,
}

/// The JSONL file audit store (see the module docs).
pub struct JsonlFileStore {
    inner: Arc<FilesInner>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn stamp_of(t: SystemTime) -> String {
    let t = time::OffsetDateTime::from(t);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

fn parse_stamp(stamp: &str) -> Option<SystemTime> {
    let b = stamp.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| stamp.get(r)?.parse::<u32>().ok();
    let date = time::Date::from_calendar_date(
        num(0..4)? as i32,
        time::Month::try_from(num(4..6)? as u8).ok()?,
        num(6..8)? as u8,
    )
    .ok()?;
    let clock =
        time::Time::from_hms(num(9..11)? as u8, num(11..13)? as u8, num(13..15)? as u8).ok()?;
    Some(
        time::PrimitiveDateTime::new(date, clock)
            .assume_utc()
            .into(),
    )
}

/// The files of `spool_id` in `dir`, oldest first (`(path, open stamp)`).
fn spool_files(dir: &Path, spool_id: &str) -> std::io::Result<Vec<(PathBuf, SystemTime)>> {
    let prefix = format!("audit-{spool_id}-");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stamp) = name
            .strip_prefix(&prefix)
            .and_then(|s| s.strip_suffix(".jsonl"))
        else {
            continue;
        };
        if let Some(at) = parse_stamp(stamp) {
            files.push((entry.path(), at));
        }
    }
    files.sort();
    Ok(files)
}

/// Cut a torn final line (no newline, or not JSON) off the file at `path`, as often as needed;
/// return the last complete line, if any.
fn truncate_torn_tail(path: &Path) -> std::io::Result<Option<String>> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    loop {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(None);
        }
        let start = len.saturating_sub(TAIL_READ_BYTES);
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail)?;
        let ends_with_newline = tail.last() == Some(&b'\n');
        let search_end = tail.len() - usize::from(ends_with_newline);
        let line_start = match tail[..search_end].iter().rposition(|b| *b == b'\n') {
            Some(i) => i + 1,
            None if start == 0 => 0,
            // One line longer than the read window: leave it alone.
            None => return Ok(None),
        };
        let line = &tail[line_start..];
        if ends_with_newline && serde_json::from_slice::<serde_json::Value>(line).is_ok() {
            return Ok(Some(String::from_utf8_lossy(line).trim_end().to_string()));
        }
        let keep = start + line_start as u64;
        tracing::warn!(
            file = %path.display(),
            bytes = len - keep,
            "audit file store: a torn tail line was truncated"
        );
        file.set_len(keep)?;
        file.sync_all()?;
    }
}

/// The `n` of a file line's `spoolRef`, when it is `spool_id`'s.
fn spool_ref_n(line: &str, spool_id: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let (id, n) = v["spoolRef"].as_str()?.rsplit_once(':')?;
    (id == spool_id).then(|| n.parse().ok()).flatten()
}

impl JsonlFileStore {
    /// Open the store in `cfg.dir` for `spool_id`: continue the newest file of this spool id
    /// (cutting a torn tail off it), rebuild the dedup cache from this spool id's files modified
    /// within the window, and start the retention timer when `retention` is set.
    pub fn open(cfg: AuditFilesConfig, spool_id: &str) -> std::io::Result<JsonlFileStore> {
        create_private_dir(&cfg.dir)?;
        let files = spool_files(&cfg.dir, spool_id)?;
        let mut dedup = DedupCache::new(cfg.dedup_window);
        let now = SystemTime::now();
        for (path, _) in &files {
            let modified = std::fs::metadata(path)?.modified().unwrap_or(now);
            if now.duration_since(modified).unwrap_or_default() > cfg.dedup_window {
                continue;
            }
            let body = std::fs::read_to_string(path).unwrap_or_default();
            for line in body.lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let seq = v["seq"].as_u64().unwrap_or(0) as u32;
                if seq == 0 {
                    continue;
                }
                let key = (
                    v["deploymentId"].as_str().unwrap_or_default().to_string(),
                    v["instanceId"].as_str().unwrap_or_default().to_string(),
                    seq,
                    v["attemptId"].as_str().unwrap_or_default().to_string(),
                );
                dedup.admit(key, modified);
            }
        }
        let open = match files.last() {
            Some((path, opened)) => {
                truncate_torn_tail(path)?;
                let file = private_file_options().append(true).open(path)?;
                let size = file.metadata()?.len();
                Some(OpenFile {
                    path: path.clone(),
                    file,
                    opened: *opened,
                    size,
                })
            }
            None => None,
        };
        let last_stamp = open.as_ref().map(|o| o.opened);
        let inner = Arc::new(FilesInner {
            cfg,
            spool_id: spool_id.to_string(),
            legacy: None,
            state: Mutex::new(FilesState {
                open,
                dedup,
                last_stamp,
            }),
            duplicates_dropped: Arc::new(AtomicU64::new(0)),
        });
        if inner.cfg.retention.is_some() {
            spawn_retention_timer(Arc::downgrade(&inner));
        }
        Ok(JsonlFileStore { inner })
    }

    /// The deprecated `sutra.audit.jsonl.path` target: one file, the same line format, no
    /// rotation and no retention.
    pub fn open_legacy_file(path: PathBuf, spool_id: &str) -> std::io::Result<JsonlFileStore> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let file = private_file_options()
            .create(true)
            .append(true)
            .open(&path)?;
        drop(file);
        truncate_torn_tail(&path)?;
        let file = private_file_options().append(true).open(&path)?;
        let size = file.metadata()?.len();
        let mut cfg = AuditFilesConfig::new(path.parent().unwrap_or(Path::new(".")));
        cfg.retention = None;
        let window = cfg.dedup_window;
        let inner = Arc::new(FilesInner {
            cfg,
            spool_id: spool_id.to_string(),
            legacy: Some(path.clone()),
            state: Mutex::new(FilesState {
                open: Some(OpenFile {
                    path,
                    file,
                    opened: SystemTime::now(),
                    size,
                }),
                dedup: DedupCache::new(window),
                last_stamp: None,
            }),
            duplicates_dropped: Arc::new(AtomicU64::new(0)),
        });
        Ok(JsonlFileStore { inner })
    }

    /// `sutra.audit.replay_duplicates_dropped`: re-emitted events dropped within the window.
    pub fn duplicates_dropped(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.inner.duplicates_dropped)
    }

    /// The file being appended to, if one is open.
    pub fn open_file(&self) -> Option<PathBuf> {
        lock(&self.inner.state)
            .open
            .as_ref()
            .map(|o| o.path.clone())
    }

    /// Delete the closed files older than `retention` (never the open one, nor the newest file
    /// of any other spool id, which may be another replica's open file). Returns how many went.
    pub fn sweep_retention(&self) -> std::io::Result<usize> {
        self.inner.sweep_retention()
    }

    fn write_batch(&self, records: &[SpoolRecord]) -> std::io::Result<()> {
        let inner = &self.inner;
        let mut state = lock(&inner.state);
        let now = SystemTime::now();
        let mut text = String::new();
        let mut dropped = 0u64;
        for record in records {
            if let Some(key) = dedup_key(&record.event) {
                if !state.dedup.admit(key, now) {
                    dropped += 1;
                    continue;
                }
            }
            text.push_str(&file_line(&record.event, &inner.spool_id, record.n));
        }
        if dropped > 0 {
            inner
                .duplicates_dropped
                .fetch_add(dropped, Ordering::Relaxed);
        }
        if text.is_empty() {
            return Ok(());
        }
        if inner.legacy.is_none() {
            let rotate = state.open.as_ref().is_none_or(|o| {
                o.size >= inner.cfg.rotate_bytes
                    || now.duration_since(o.opened).unwrap_or_default() >= inner.cfg.rotate_interval
            });
            if rotate {
                inner.rotate(&mut state, now)?;
            }
        }
        let open = state.open.as_mut().expect("a file is open");
        open.file.write_all(text.as_bytes())?;
        open.file.sync_data()?;
        open.size += text.len() as u64;
        Ok(())
    }

    fn last_shipped(&self, spool_id: &str) -> std::io::Result<Option<u64>> {
        let inner = &self.inner;
        let candidates: Vec<PathBuf> = match &inner.legacy {
            Some(path) => vec![path.clone()],
            None => spool_files(&inner.cfg.dir, spool_id)?
                .into_iter()
                .rev()
                .map(|(p, _)| p)
                .collect(),
        };
        let _state = lock(&inner.state);
        for path in candidates {
            if let Some(line) = truncate_torn_tail(&path)? {
                return Ok(spool_ref_n(&line, spool_id));
            }
        }
        Ok(None)
    }
}

impl FilesInner {
    fn rotate(&self, state: &mut FilesState, now: SystemTime) -> std::io::Result<()> {
        if let Some(old) = state.open.take() {
            old.file.sync_all()?;
        }
        // A later name than the last file's, even within the same second.
        let mut stamp = now;
        if let Some(last) = state.last_stamp {
            if stamp_of(stamp) <= stamp_of(last) {
                stamp = last + Duration::from_secs(1);
            }
        }
        let path = self
            .cfg
            .dir
            .join(format!("audit-{}-{}.jsonl", self.spool_id, stamp_of(stamp)));
        let file = private_file_options()
            .create(true)
            .append(true)
            .open(&path)?;
        sync_dir(&self.cfg.dir)?;
        let size = file.metadata()?.len();
        // The age that rotation measures runs from the real open time; only the name is bumped.
        state.open = Some(OpenFile {
            path,
            file,
            opened: now,
            size,
        });
        state.last_stamp = Some(stamp);
        Ok(())
    }

    fn sweep_retention(&self) -> std::io::Result<usize> {
        let Some(retention) = self.cfg.retention else {
            return Ok(0);
        };
        if self.legacy.is_some() {
            return Ok(0);
        }
        let open = lock(&self.state).open.as_ref().map(|o| o.path.clone());
        // Every audit file, grouped by spool id: the newest of each group is kept.
        let mut by_spool: std::collections::BTreeMap<String, Vec<PathBuf>> = Default::default();
        for entry in std::fs::read_dir(&self.cfg.dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(rest) = name
                .strip_prefix("audit-")
                .and_then(|r| r.strip_suffix(".jsonl"))
            else {
                continue;
            };
            let Some((spool, stamp)) = rest.rsplit_once('-') else {
                continue;
            };
            if parse_stamp(stamp).is_none() {
                continue;
            }
            by_spool.entry(spool.to_string()).or_default().push(path);
        }
        let now = SystemTime::now();
        let mut removed = 0;
        for (_, mut paths) in by_spool {
            paths.sort();
            paths.pop(); // the newest file of each spool id
            for path in paths {
                if Some(&path) == open.as_ref() {
                    continue;
                }
                let old = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .map(|m| now.duration_since(m).unwrap_or_default() > retention)
                    .unwrap_or(false);
                if old && std::fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            sync_dir(&self.cfg.dir)?;
        }
        Ok(removed)
    }
}

/// The retention timer: a thread that sweeps every `retention-sweep-interval` until the store
/// is dropped.
fn spawn_retention_timer(inner: Weak<FilesInner>) {
    let spawned = std::thread::Builder::new()
        .name("sutra-audit-files-retention".into())
        .spawn(move || loop {
            let interval = match inner.upgrade() {
                Some(store) => store.cfg.retention_sweep_interval,
                None => return,
            };
            // Sleep in short steps so a dropped store ends the thread promptly.
            let deadline = std::time::Instant::now() + interval;
            while std::time::Instant::now() < deadline {
                if inner.strong_count() == 0 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(500).min(interval));
            }
            let Some(store) = inner.upgrade() else { return };
            match store.sweep_retention() {
                Ok(0) => {}
                Ok(n) => tracing::info!(files = n, "audit file store: retention deleted files"),
                Err(e) => tracing::warn!(error = %e, "audit file store: retention sweep failed"),
            }
        });
    if let Err(e) = spawned {
        tracing::warn!(error = %e, "audit file store: the retention timer could not start");
    }
}

impl AuditSink for JsonlFileStore {
    fn name(&self) -> &str {
        "jsonl"
    }

    fn ship<'a>(
        &'a self,
        _spool_id: &'a str,
        records: &'a [SpoolRecord],
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            self.write_batch(records).map_err(|e| {
                Diagnostic::error(
                    AUDIT_SINK_WRITE_FAILED,
                    format!("audit file store write failed: {e}"),
                )
            })
        })
    }

    fn recover<'a>(&'a self, spool_id: &'a str) -> BoxFuture<'a, Result<Option<u64>, Diagnostic>> {
        Box::pin(async move {
            self.last_shipped(spool_id).map_err(|e| {
                Diagnostic::error(
                    AUDIT_SINK_WRITE_FAILED,
                    format!("audit file store recovery failed: {e}"),
                )
            })
        })
    }
}
