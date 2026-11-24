//! The audit shipper: one tokio task per configured audit store, loading the durable spool into
//! that store in order.
//!
//! At start it resumes after the larger of its checkpoint (`ship-<sink>.ckpt` in the spool
//! directory) and what the store itself reports already holding ([`AuditSink::recover`]). It then
//! waits for the spool to sync past that point, reads up to `batch-max` records (waiting
//! `batch-linger-ms` after the first for more), keeps the ones addressed to its store, ships them,
//! and checkpoints the last record READ, which includes other stores' records. A failed ship is
//! retried, the same batch, with exponential backoff: a record is never skipped. A store therefore
//! sees every one of its records at least once; a store whose `ship` is idempotent (SQL, JSONL)
//! holds each exactly once.
//!
//! A record of an attempt (every event a pass emits under a persistence bridge) is HELD until
//! that attempt's commit marker (`audit_attempts`): shipped on `COMMITTED`, dropped on
//! `ABORTED`. Holding blocks nothing else. The checkpoint stops before the oldest held record,
//! with the read position alongside it: after a restart the shipper re-reads from the
//! checkpoint, skips the records without an attempt up to the read position (they shipped), and
//! settles the attempts again from their markers; a store drops what it already holds.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::audit::AuditSink;
use crate::audit_spool::{
    quarantine_segment, read_checkpoint, read_checkpoint_read, write_checkpoint_read, AuditSpool,
    SpoolError, SpoolReader, SpoolRecord, SpoolShared,
};
use crate::diag::Diagnostic;

/// A store did not reach the requested record before the timeout.
pub const AUDIT_SHIP_TIMEOUT: &str = "SUTRA.AUDIT.SHIP.TIMEOUT";

const BACKOFF_START: Duration = Duration::from_millis(100);
const EMPTY_READ_PAUSE: Duration = Duration::from_millis(50);

/// The shipper's settings (`sutra.audit.batch-max`, `sutra.audit.batch-linger-ms`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShipperConfig {
    /// The most records read into one batch.
    pub batch_max: usize,
    /// How long to wait after the first record of a batch for more.
    pub batch_linger: Duration,
    /// The longest pause between two attempts at a failing batch.
    pub backoff_max: Duration,
}

impl Default for ShipperConfig {
    fn default() -> ShipperConfig {
        ShipperConfig {
            batch_max: crate::audit::AUDIT_BATCH_MAX,
            batch_linger: Duration::from_millis(5),
            backoff_max: Duration::from_secs(30),
        }
    }
}

struct ShipperState {
    sink: String,
    shipped: tokio::sync::watch::Sender<u64>,
}

/// The handle of one running shipper.
pub struct ShipperHandle {
    state: Arc<ShipperState>,
    spool: Arc<SpoolShared>,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ShipperHandle {
    /// The store this shipper loads.
    pub fn sink(&self) -> &str {
        &self.state.sink
    }

    /// The highest `n` checkpointed as shipped.
    pub fn shipped_through(&self) -> u64 {
        *self.state.shipped.borrow()
    }

    /// A cloneable view of this shipper's progress, for callers that must wait for the store to
    /// hold everything spooled before they read or amend it.
    pub fn progress(&self) -> ShipperProgress {
        ShipperProgress {
            state: Arc::clone(&self.state),
        }
    }

    /// Wait until the shipper has shipped through `n`, for at most `timeout`.
    pub async fn ship_through(&self, n: u64, timeout: Duration) -> Result<(), Diagnostic> {
        self.progress().ship_through(n, timeout).await
    }

    /// Ship what the spool has made durable, for at most `timeout`, then stop.
    pub async fn shutdown(self, timeout: Duration) {
        let durable = self.spool.durable_through();
        if let Err(d) = self.ship_through(durable, timeout).await {
            tracing::warn!(code = %d.code, "{}", d.message);
        }
        let _ = self.stop.send(true);
        let task = self.task;
        if tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .is_err()
        {
            tracing::warn!(sink = %self.state.sink, "audit shipper did not stop in time");
        }
    }
}

/// How far one shipper has shipped (see [`ShipperHandle::progress`]).
#[derive(Clone)]
pub struct ShipperProgress {
    state: Arc<ShipperState>,
}

impl std::fmt::Debug for ShipperProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShipperProgress")
            .field("sink", &self.state.sink)
            .finish()
    }
}

impl ShipperProgress {
    /// The store this shipper loads.
    pub fn sink(&self) -> &str {
        &self.state.sink
    }

    /// The highest `n` checkpointed as shipped.
    pub fn shipped_through(&self) -> u64 {
        *self.state.shipped.borrow()
    }

    /// Wait until the shipper has shipped through `n`, for at most `timeout`.
    pub async fn ship_through(&self, n: u64, timeout: Duration) -> Result<(), Diagnostic> {
        let mut shipped = self.state.shipped.subscribe();
        let reached =
            tokio::time::timeout(timeout, shipped.wait_for(|through| *through >= n)).await;
        match reached {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Diagnostic::error(
                AUDIT_SHIP_TIMEOUT,
                format!(
                    "audit store '{}' shipped through {} of {n} within {timeout:?}; the rest ships \
                     at the next start",
                    self.state.sink,
                    self.shipped_through()
                ),
            )),
        }
    }
}

/// Start the shipper of `sink` over `spool` on `handle`.
pub fn spawn_audit_shipper(
    handle: &tokio::runtime::Handle,
    spool: &AuditSpool,
    sink: Arc<dyn AuditSink>,
    cfg: ShipperConfig,
) -> ShipperHandle {
    let shared = spool.shared();
    let initial = read_checkpoint(&shared.dir, sink.name(), &shared.spool_id);
    let (shipped, _) = tokio::sync::watch::channel(initial);
    let state = Arc::new(ShipperState {
        sink: sink.name().to_string(),
        shipped,
    });
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let task = handle.spawn(run(
        Arc::clone(&shared),
        Arc::clone(&state),
        sink,
        cfg,
        stop_rx,
    ));
    ShipperHandle {
        state,
        spool: shared,
        stop,
        task,
    }
}

/// Read a batch off the blocking pool: `(reader back, records or error)`.
async fn read_batch(
    reader: SpoolReader,
    max: usize,
    upto: u64,
) -> (SpoolReader, Result<Vec<SpoolRecord>, SpoolError>) {
    let mut reader = reader;
    match tokio::task::spawn_blocking(move || {
        let result = reader.next_batch(max, upto);
        (reader, result)
    })
    .await
    {
        Ok(done) => done,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

async fn run(
    spool: Arc<SpoolShared>,
    state: Arc<ShipperState>,
    sink: Arc<dyn AuditSink>,
    cfg: ShipperConfig,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let name = state.sink.clone();
    let checkpoint = *state.shipped.borrow();
    // Step 1: resume after the larger of the checkpoint and what the store reports holding.
    let mut backoff = BACKOFF_START;
    let recovered = loop {
        match sink.recover(&spool.spool_id).await {
            Ok(n) => break n.unwrap_or(0),
            Err(d) => {
                tracing::warn!(code = %d.code, sink = %name, "audit store recovery failed: {}", d.message);
                spool.metrics.count_ship_failure(&name);
                if sleep_or_stop(&mut stop, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(cfg.backoff_max);
            }
        }
    };
    // Held attempts mean the store can hold records past the checkpoint while older ones are
    // still held: re-read from the checkpoint, and skip only the records without an attempt
    // up to what was read (or what the store reports holding).
    let mut through = checkpoint;
    let mut read_pos = read_checkpoint_read(&spool.dir, &name, &spool.spool_id).max(recovered);
    let skip_upto = read_pos;
    state.shipped.send_replace(through);
    spool.set_progress(&name, through);
    let mut reader = SpoolReader::new_at(spool.dir.clone(), through);
    // Attempt id -> this store's records of it, waiting for the attempt's marker.
    let mut held: std::collections::HashMap<String, Vec<SpoolRecord>> = Default::default();
    let mut durable = spool.subscribe();
    loop {
        // Step 2: wait for records past `through`.
        let upto = loop {
            let now = *durable.borrow_and_update();
            spool
                .metrics
                .set_ship_lag(&name, now.saturating_sub(through));
            // Past what was read, not what is settled: held records are already read.
            if now > reader.position() {
                break now;
            }
            tokio::select! {
                changed = durable.changed() => if changed.is_err() { return; },
                _ = stop.changed() => return,
            }
        };
        let (back, first) = read_batch(reader, cfg.batch_max, upto).await;
        reader = back;
        let mut batch = match first {
            Ok(records) => records,
            Err(e) => {
                reader = handle_read_error(&spool, reader, e).await;
                continue;
            }
        };
        if !batch.is_empty() && batch.len() < cfg.batch_max && !cfg.batch_linger.is_zero() {
            if sleep_or_stop(&mut stop, cfg.batch_linger).await {
                return;
            }
            let upto = *durable.borrow();
            let (back, more) = read_batch(reader, cfg.batch_max - batch.len(), upto).await;
            reader = back;
            match more {
                Ok(more) => batch.extend(more),
                Err(e) => reader = handle_read_error(&spool, reader, e).await,
            }
        }
        let Some(last_read) = batch.last().map(|r| r.n) else {
            // Nothing readable below `upto` yet (a quarantined segment, or the directory listing
            // raced the writer): look again shortly.
            if sleep_or_stop(&mut stop, EMPTY_READ_PAUSE).await {
                return;
            }
            continue;
        };
        let mut mine: Vec<SpoolRecord> = Vec::new();
        for record in batch {
            if crate::audit_attempts::is_marker(&record.event) {
                let Some(attempt) = &record.event.attempt else {
                    continue;
                };
                // A prepare settles nothing: its outcome marker (or recovery's) follows.
                if record.event.event_type == crate::audit_attempts::MARKER_PREPARED {
                    continue;
                }
                if let Some(records) = held.remove(&attempt.id) {
                    if record.event.event_type == crate::audit_attempts::MARKER_COMMITTED {
                        mine.extend(records);
                    } else {
                        spool.metrics.count_aborted(records.len() as u64);
                    }
                }
                continue;
            }
            if record.event.sink != name {
                continue;
            }
            match &record.event.attempt {
                Some(attempt) => held.entry(attempt.id.clone()).or_default().push(record),
                None if record.n > skip_upto => mine.push(record),
                None => {}
            }
        }
        mine.sort_by_key(|r| r.n);
        // Steps 3 and 4: ship, retrying the same batch until it is accepted.
        let mut backoff = BACKOFF_START;
        while !mine.is_empty() {
            match sink.ship(&spool.spool_id, &mine).await {
                Ok(()) => break,
                Err(d) => {
                    spool.metrics.count_ship_failure(&name);
                    tracing::warn!(
                        code = %d.code,
                        sink = %name,
                        records = mine.len(),
                        "audit ship failed; retrying the same batch: {}",
                        d.message
                    );
                    if sleep_or_stop(&mut stop, backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(cfg.backoff_max);
                }
            }
        }
        let dir: PathBuf = spool.dir.clone();
        let spool_id = spool.spool_id.clone();
        let sink_name = name.clone();
        read_pos = read_pos.max(last_read);
        // Settled through the record before the oldest one still held.
        let settled = held
            .values()
            .filter_map(|records| records.first().map(|r| r.n))
            .min()
            .map_or(last_read, |oldest| oldest.saturating_sub(1).min(last_read));
        let read = read_pos;
        let written = tokio::task::spawn_blocking(move || {
            write_checkpoint_read(&dir, &sink_name, &spool_id, settled, read)
        })
        .await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(
                sink = %name,
                error = %e,
                "audit shipper checkpoint write failed; those records may ship again"
            ),
            Err(e) => tracing::warn!(sink = %name, "audit shipper checkpoint task failed: {e}"),
        }
        through = through.max(settled);
        spool.set_progress(&name, through);
        state.shipped.send_replace(through);
        let now = *durable.borrow();
        spool
            .metrics
            .set_ship_lag(&name, now.saturating_sub(through));
    }
}

/// Step 6: a corrupt record moves its segment to quarantine (or, in the open segment, is
/// skipped); any other read error is logged and retried after a pause.
async fn handle_read_error(
    spool: &SpoolShared,
    mut reader: SpoolReader,
    e: SpoolError,
) -> SpoolReader {
    match e {
        SpoolError::Corrupt { segment, line } => {
            let dir = spool.dir.clone();
            let metrics = Arc::clone(&spool.metrics);
            let moved = tokio::task::spawn_blocking(move || {
                quarantine_segment(&dir, &segment, line, &metrics)
            })
            .await
            .unwrap_or(false);
            if moved {
                reader.skip_segment();
            } else if let Err(e) = reader.skip_line() {
                tracing::warn!("audit spool: could not skip a corrupt line: {e}");
            }
        }
        other => {
            tracing::warn!("audit spool read failed: {other}");
            tokio::time::sleep(EMPTY_READ_PAUSE).await;
        }
    }
    reader
}

/// Sleep `pause`; `true` when the shipper was told to stop meanwhile.
async fn sleep_or_stop(stop: &mut tokio::sync::watch::Receiver<bool>, pause: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(pause) => *stop.borrow(),
        _ = stop.changed() => true,
    }
}
