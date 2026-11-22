//! Doubles for the concurrent outbox dispatcher: an in-memory row table whose claim honours the
//! exclusion list, and a sink that records every delivery and the peak number of sends in flight,
//! overall and per destination authority. Copied from the dispatcher's own unit tests (which keep
//! theirs) so the integration tests can drive the public loop.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sutra_bpmn::qbindings::ReplyMode;
use sutra_channels::{
    destination_authority, BoxFuture, ClaimedOutboxRow, Diagnostic, MessageSink, OutboundMessage,
    OutboxDispatcher, OutboxRowStore, RetryPolicy, SendOutcome, SinkRegistry,
};
use sutra_executor::DeploymentId;
use time::OffsetDateTime;

pub fn dep() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000c7").expect("valid deployment id")
}

/// A row of `instance` bound for `destination`; the body is the entry id.
pub fn row(entry_id: &str, instance: &str, destination: &str) -> ClaimedOutboxRow {
    ClaimedOutboxRow {
        entry_id: entry_id.to_string(),
        attempt_count: 0,
        destination: destination.to_string(),
        headers: BTreeMap::new(),
        body: entry_id.as_bytes().to_vec(),
        content_type: Some("text/plain".to_string()),
        outbox_key: format!("key-{entry_id}"),
        mode: ReplyMode::Native,
        cloud_event_json: None,
        auth_ref_json: None,
        labels: BTreeMap::new(),
        traceparent: None,
        node_id: None,
        instance_id: instance.to_string(),
        required: false,
        last_diagnostic_json: None,
        created_at: OffsetDateTime::now_utc(),
    }
}

/// A table of rows with a claim that honours `exclude` (in insertion order), counting claims and
/// delete transactions and recording defers with their new due time.
#[derive(Default)]
pub struct Table {
    pub rows: Mutex<Vec<(ClaimedOutboxRow, bool)>>, // (row, deferred)
    pub delete_calls: Mutex<Vec<usize>>,
    pub deferred: Mutex<Vec<(String, OffsetDateTime)>>,
    pub claims: AtomicUsize,
    pub renewed: Mutex<Vec<Vec<String>>>,
}

impl Table {
    pub fn insert(&self, row: ClaimedOutboxRow) {
        self.rows.lock().unwrap().push((row, false));
    }
    pub fn remaining(&self) -> usize {
        self.rows.lock().unwrap().len()
    }
    pub fn deferred_ids(&self) -> Vec<String> {
        self.deferred
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }
    /// Make every deferred row due again (the backoff passed).
    pub fn undefer_all(&self) {
        for (_, deferred) in self.rows.lock().unwrap().iter_mut() {
            *deferred = false;
        }
    }
}

impl OutboxRowStore for Table {
    fn renew_claims<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_ids: &'a [String],
        _now: OffsetDateTime,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        self.renewed.lock().unwrap().push(entry_ids.to_vec());
        Box::pin(async { Ok(()) })
    }

    fn claim_due<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
        self.claim_due_excluding(deployment, now, max_entries, &[])
    }

    fn claim_due_excluding<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        _now: OffsetDateTime,
        max_entries: i64,
        exclude: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
        self.claims.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|(r, deferred)| !deferred && !exclude.contains(&r.entry_id))
                .take(max_entries as usize)
                .map(|(r, _)| r.clone())
                .collect())
        })
    }

    fn delete<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        let ids = [entry_id.to_string()];
        Box::pin(async move {
            let ids = ids;
            self.delete_many(deployment, &ids).await
        })
    }

    fn delete_many<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_ids: &'a [String],
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            self.delete_calls.lock().unwrap().push(entry_ids.len());
            self.rows
                .lock()
                .unwrap()
                .retain(|(r, _)| !entry_ids.contains(&r.entry_id));
            Ok(())
        })
    }

    fn defer<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        _diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            self.deferred
                .lock()
                .unwrap()
                .push((entry_id.to_string(), new_due_at));
            for (r, deferred) in self.rows.lock().unwrap().iter_mut() {
                if r.entry_id == entry_id {
                    *deferred = true;
                }
            }
            Ok(())
        })
    }

    fn mark_poisoned<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        self.defer(
            deployment,
            entry_id,
            OffsetDateTime::now_utc(),
            diagnostic_json,
        )
    }

    fn requeue<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        self.defer(deployment, entry_id, new_due_at, diagnostic_json)
    }
}

/// Records every delivery as `(destination, entry id, instant)` and the peak number of sends in
/// flight, overall and per destination authority. A destination containing `slow` takes
/// `slow_delay`, every other one `delay`.
pub struct RecordingSink {
    pub delay: Duration,
    pub slow_delay: Duration,
    pub delivered: Mutex<Vec<(String, String, std::time::Instant)>>,
    pub in_flight: AtomicUsize,
    pub peak: AtomicUsize,
    pub per_authority: Mutex<HashMap<String, (usize, usize)>>, // (now, peak)
    /// A send of this body fails (retryable).
    pub fail: Option<String>,
    /// A send of this body panics, once.
    pub panic_once: Option<String>,
    pub panicked: AtomicBool,
}

impl RecordingSink {
    pub fn new(delay_ms: u64) -> RecordingSink {
        RecordingSink {
            delay: Duration::from_millis(delay_ms),
            slow_delay: Duration::from_millis(delay_ms),
            delivered: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            per_authority: Mutex::new(HashMap::new()),
            fail: None,
            panic_once: None,
            panicked: AtomicBool::new(false),
        }
    }

    pub fn peak_for(&self, authority: &str) -> usize {
        self.per_authority
            .lock()
            .unwrap()
            .get(authority)
            .map_or(0, |(_, peak)| *peak)
    }

    pub fn delivered_ids(&self) -> Vec<String> {
        self.delivered
            .lock()
            .unwrap()
            .iter()
            .map(|(_, id, _)| id.clone())
            .collect()
    }
}

impl MessageSink for RecordingSink {
    fn schemes(&self) -> Vec<String> {
        vec!["test".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            let authority = destination_authority(&message.destination);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            {
                let mut per = self.per_authority.lock().unwrap();
                let entry = per.entry(authority.clone()).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = entry.1.max(entry.0);
            }
            let delay = if message.destination.contains("slow") {
                self.slow_delay
            } else {
                self.delay
            };
            tokio::time::sleep(delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.per_authority
                .lock()
                .unwrap()
                .get_mut(&authority)
                .expect("entered above")
                .0 -= 1;
            let id = String::from_utf8(message.body.clone()).unwrap_or_default();
            if self.panic_once.as_deref() == Some(id.as_str())
                && !self.panicked.swap(true, Ordering::SeqCst)
            {
                panic!("test sink panics on {id}");
            }
            if self.fail.as_deref() == Some(id.as_str()) {
                return SendOutcome::RetryableFailure(Diagnostic::error(
                    "OUTBOUND_SEND_FAILED",
                    "down",
                ));
            }
            self.delivered.lock().unwrap().push((
                message.destination.clone(),
                id,
                std::time::Instant::now(),
            ));
            SendOutcome::Delivered
        })
    }
}

pub fn dispatcher(table: Arc<Table>, sink: Arc<RecordingSink>, batch: i64) -> OutboxDispatcher {
    let mut sinks = SinkRegistry::new();
    sinks.register(sink);
    OutboxDispatcher::new(
        table,
        sinks,
        RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(300), false),
        batch,
    )
}

pub async fn wait_until(mut done: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached in 10 s");
}
