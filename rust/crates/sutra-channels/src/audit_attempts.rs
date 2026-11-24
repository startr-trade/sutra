//! Commit markers for audit attempts.
//!
//! Every audit event a pass emits carries its attempt ([`crate::AuditAttempt`]). The shippers
//! hold an attempt's records until the spool says what became of its step:
//!
//! - [`MARKER_COMMITTED`]: the step's database commit succeeded; the records ship.
//! - [`MARKER_ABORTED`]: the step did not commit; the records are dropped, never shipped.
//!
//! A marker is a spool record with sink [`MARKER_SINK`] (every shipper reads it, none ships it),
//! the attempt, its deployment and instance, and the attempt's last seq. Markers ride the next
//! group sync: no step waits for them.
//!
//! One [`AuditAttemptTracker`] serves a lane: its audit listener registers every attempt it
//! opens, its persistence bridge settles them after each commit, and the lane aborts whatever is
//! still open when it finishes a request. An attempt whose commit returned an error is handed to
//! the engine's resolver ([`AuditAttemptTracker::take_unknown`]), which decides it from the
//! instance row, as recovery does at start for the attempts a crash left unmarked.

use std::sync::{Arc, Mutex};

use crate::audit::{AuditAttempt, AuditEvent};
use crate::audit_spool::AuditSpoolSender;

/// The attempt's records belong to step transaction `xid` (written inside it, before the
/// barrier that precedes its `COMMIT`); recovery asks the database how it ended.
pub const MARKER_PREPARED: &str = "SUTRA.AUDIT.ATTEMPT_PREPARED";
/// The step of the attempt committed: ship its records.
pub const MARKER_COMMITTED: &str = "SUTRA.AUDIT.ATTEMPT_COMMITTED";
/// The step of the attempt did not commit: drop its records.
pub const MARKER_ABORTED: &str = "SUTRA.AUDIT.ATTEMPT_ABORTED";
/// The sink a marker is addressed to: every shipper reads it, none ships it.
pub const MARKER_SINK: &str = "*";

/// Whether `event` is a commit marker.
pub fn is_marker(event: &AuditEvent) -> bool {
    event.sink == MARKER_SINK
        && (event.event_type == MARKER_COMMITTED
            || event.event_type == MARKER_ABORTED
            || event.event_type == MARKER_PREPARED)
}

/// An attempt a lane opened and has not settled yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAttempt {
    pub attempt: AuditAttempt,
    pub deployment_id: String,
    pub instance_id: String,
    /// The seq of its last event so far.
    pub last_seq: u32,
    /// Whether it recorded `INSTANCE_COMPLETED` or `INSTANCE_FAILED`.
    pub terminal: bool,
    /// The step transaction it was prepared under, once prepared.
    pub xid: Option<String>,
}

impl OpenAttempt {
    /// The `PREPARED` marker of this attempt under transaction `xid`.
    pub fn prepared_marker(&self, xid: &str) -> AuditEvent {
        AuditEvent {
            event_type: MARKER_PREPARED.to_string(),
            diagnostic_json: Some(serde_json::json!({ "xid": xid }).to_string()),
            ..self.marker(false)
        }
    }

    /// The marker that settles this attempt.
    pub fn marker(&self, committed: bool) -> AuditEvent {
        AuditEvent {
            sink: MARKER_SINK.to_string(),
            deployment_id: self.deployment_id.clone(),
            tenant: String::new(),
            instance_id: self.instance_id.clone(),
            seq: self.last_seq,
            at: self.attempt.at.clone(),
            event_type: if committed {
                MARKER_COMMITTED
            } else {
                MARKER_ABORTED
            }
            .to_string(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{}".to_string(),
            correlation: Default::default(),
            attempt: Some(self.attempt.clone()),
        }
    }
}

/// The attempts one lane has open (see the module docs). Cheap to clone: one shared list.
#[derive(Debug, Clone, Default)]
pub struct AuditAttemptTracker {
    open: Arc<Mutex<Vec<OpenAttempt>>>,
}

impl AuditAttemptTracker {
    pub fn new() -> AuditAttemptTracker {
        AuditAttemptTracker::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<OpenAttempt>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The listener opened `attempt` for an instance.
    pub fn register(&self, deployment_id: &str, instance_id: &str, attempt: &AuditAttempt) {
        self.lock().push(OpenAttempt {
            attempt: attempt.clone(),
            deployment_id: deployment_id.to_string(),
            instance_id: instance_id.to_string(),
            last_seq: attempt.base,
            terminal: false,
            xid: None,
        });
    }

    /// The listener emitted event `seq` of type `event_type` under `attempt_id`.
    pub fn observe(&self, attempt_id: &str, seq: u32, event_type: &str) {
        if let Some(open) = self.lock().iter_mut().find(|o| o.attempt.id == attempt_id) {
            open.last_seq = open.last_seq.max(seq);
            open.terminal |= event_type == "INSTANCE_COMPLETED" || event_type == "INSTANCE_FAILED";
        }
    }

    /// Whether any attempt is open.
    pub fn has_open(&self) -> bool {
        !self.lock().is_empty()
    }

    /// Whether `attempt_id` is still open (not settled).
    pub fn is_open(&self, attempt_id: &str) -> bool {
        self.lock().iter().any(|o| o.attempt.id == attempt_id)
    }

    /// The highest seq an open attempt of `instance_id` has emitted: what the step about to
    /// commit persists as the instance's audit high-water.
    pub fn high_water(&self, deployment_id: &str, instance_id: &str) -> Option<u32> {
        self.lock()
            .iter()
            .filter(|o| o.deployment_id == deployment_id && o.instance_id == instance_id)
            .map(|o| o.last_seq)
            .max()
    }

    /// Settle every open attempt with a marker appended to `spool`: `committed` after a
    /// successful commit, aborted otherwise. Returns how many were settled.
    pub fn settle(&self, spool: &AuditSpoolSender, committed: bool) -> usize {
        let settled: Vec<OpenAttempt> = std::mem::take(&mut *self.lock());
        for open in &settled {
            spool.append(open.marker(committed));
        }
        settled.len()
    }

    /// Inside a step transaction `xid`, before its barrier: append a `PREPARED` marker for every
    /// open attempt and remember the xid. Returns how many were prepared.
    pub fn prepare(&self, spool: &AuditSpoolSender, xid: &str) -> usize {
        let mut open = self.lock();
        for attempt in open.iter_mut() {
            attempt.xid = Some(xid.to_string());
            spool.append(attempt.prepared_marker(xid));
        }
        open.len()
    }

    /// Take every open attempt without a marker: a commit returned an error, so whether it
    /// landed is decided from the instance row (the engine's resolver).
    pub fn take_unknown(&self) -> Vec<OpenAttempt> {
        std::mem::take(&mut *self.lock())
    }
}

/// The attempts a previous life of `spool` left without a marker (a crash between the spool
/// sync and the marker's), oldest first: recovery decides each from its instance row and appends
/// the marker. Read from the lowest store checkpoint up to [`crate::AuditSpool::recovered_through`].
pub fn unsettled_attempts(
    spool: &crate::AuditSpool,
) -> Result<Vec<OpenAttempt>, crate::audit_spool::SpoolError> {
    let upto = spool.recovered_through();
    let mut reader = spool.reader(spool.settled_through());
    let mut open: Vec<OpenAttempt> = Vec::new();
    loop {
        let batch = reader.next_batch(4096, upto)?;
        if batch.is_empty() {
            break;
        }
        for record in batch {
            let event = &record.event;
            let Some(attempt) = &event.attempt else {
                continue;
            };
            if event.event_type == MARKER_PREPARED && event.sink == MARKER_SINK {
                let xid = event
                    .diagnostic_json
                    .as_deref()
                    .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
                    .and_then(|v| v["xid"].as_str().map(str::to_string));
                match open.iter_mut().find(|o| o.attempt.id == attempt.id) {
                    Some(o) => o.xid = xid,
                    None => open.push(OpenAttempt {
                        attempt: attempt.clone(),
                        deployment_id: event.deployment_id.clone(),
                        instance_id: event.instance_id.clone(),
                        last_seq: event.seq,
                        terminal: false,
                        xid,
                    }),
                }
                continue;
            }
            if is_marker(event) {
                open.retain(|o| o.attempt.id != attempt.id);
                continue;
            }
            match open.iter_mut().find(|o| o.attempt.id == attempt.id) {
                Some(o) => {
                    o.last_seq = o.last_seq.max(event.seq);
                    o.terminal |= event.event_type == "INSTANCE_COMPLETED"
                        || event.event_type == "INSTANCE_FAILED";
                }
                None => open.push(OpenAttempt {
                    attempt: attempt.clone(),
                    deployment_id: event.deployment_id.clone(),
                    instance_id: event.instance_id.clone(),
                    last_seq: event.seq,
                    terminal: event.event_type == "INSTANCE_COMPLETED"
                        || event.event_type == "INSTANCE_FAILED",
                    xid: None,
                }),
            }
        }
        if reader.position() >= upto {
            break;
        }
    }
    Ok(open)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(id: &str, base: u32) -> AuditAttempt {
        AuditAttempt {
            id: id.into(),
            base,
            at: "2026-11-24T10:00:00Z".into(),
        }
    }

    #[test]
    fn tracker_follows_seq_and_terminal_events() {
        let tracker = AuditAttemptTracker::new();
        tracker.register("dep-1", "i-1", &attempt("a", 2));
        tracker.observe("a", 3, "NODE_LEFT");
        tracker.observe("a", 4, "INSTANCE_COMPLETED");
        assert!(tracker.is_open("a"));
        assert_eq!(tracker.high_water("dep-1", "i-1"), Some(4));
        let unknown = tracker.take_unknown();
        assert_eq!(unknown.len(), 1);
        assert!(unknown[0].terminal);
        assert_eq!(unknown[0].last_seq, 4);
        assert!(!tracker.is_open("a"));
        let marker = unknown[0].marker(true);
        assert!(is_marker(&marker));
        assert_eq!(marker.event_type, MARKER_COMMITTED);
        assert_eq!(marker.seq, 4);
    }
}
