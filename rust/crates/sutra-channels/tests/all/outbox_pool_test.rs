//! The outbox side of capacity pools: a request marked with a pool is sent only once its
//! call holds a slot of it. While the pool is full the entry WAITS — put back without a
//! delivery attempt counted, keeping its place, oldest first — and a full batch of waiting
//! entries does not hold the rest of the deployment back. The pool markers never reach a wire.
//! Each request is its own call (one slot each) unless requests share a declared call key, and a
//! request of an instance that has already finished returns its slot once delivered or poisoned.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use sutra_channels::{
    BoxFuture, CallSelector, CapacityPool, CapacityPoolStore, ClaimedOutboxRow, Diagnostic,
    InMemoryCapacityPools, MessageSink, OutboundMessage, OutboxDispatcher, OutboxRowStore,
    RetryPolicy, SendOutcome, SinkRegistry, SlotAcquisition, SlotDirection, SlotRequest,
    CAPACITY_CALL_HEADER, CAPACITY_CALL_KEY_HEADER, CAPACITY_POOL_HEADER,
};
use sutra_executor::DeploymentId;
use time::OffsetDateTime;

const POOL: &str = "lines";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000d2").expect("valid deployment id")
}

fn instance(n: u32) -> String {
    format!("00000000-0000-4000-8000-{n:012}")
}

fn t0() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap()
}

/// An entry for `instance`, pooled when `pooled`.
fn row(entry: &str, instance: &str, pooled: bool) -> ClaimedOutboxRow {
    let mut headers = BTreeMap::from([("X-Keep".to_string(), "1".to_string())]);
    if pooled {
        headers.insert(CAPACITY_POOL_HEADER.to_string(), POOL.to_string());
    }
    ClaimedOutboxRow {
        entry_id: entry.to_string(),
        attempt_count: 0,
        destination: "fake://dial/out".to_string(),
        headers,
        body: b"{}".to_vec(),
        content_type: Some("application/json".to_string()),
        outbox_key: format!("key-{entry}"),
        mode: sutra_bpmn::qbindings::ReplyMode::Native,
        cloud_event_json: None,
        auth_ref_json: None,
        labels: BTreeMap::from([("tenant".to_string(), "acme".to_string())]),
        traceparent: None,
        instance_id: instance.to_string(),
        node_id: None,
        required: false,
        last_diagnostic_json: None,
    }
}

/// Rows claimed in `(due, enqueue order)` order, like the durable claim.
#[derive(Default)]
struct OrderedRows {
    /// `(row, due, enqueue sequence)`.
    rows: Mutex<Vec<(ClaimedOutboxRow, OffsetDateTime, usize)>>,
    terminal: Mutex<Vec<String>>,
}

impl OrderedRows {
    fn enqueue(&self, row: ClaimedOutboxRow) {
        let mut rows = self.rows.lock().unwrap();
        let seq = rows.len();
        rows.push((row, t0(), seq));
    }

    fn attempts(&self, entry: &str) -> i32 {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .find(|(r, _, _)| r.entry_id == entry)
            .map(|(r, _, _)| r.attempt_count)
            .unwrap_or(-1)
    }

    fn pending(&self) -> Vec<String> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .map(|(r, _, _)| r.entry_id.clone())
            .collect()
    }
}

impl OutboxRowStore for OrderedRows {
    fn claim_due<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
        Box::pin(async move {
            let terminal = self.terminal.lock().unwrap().clone();
            let mut due: Vec<(ClaimedOutboxRow, OffsetDateTime, usize)> = self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|(r, at, _)| *at <= now && !terminal.contains(&r.entry_id))
                .cloned()
                .collect();
            due.sort_by_key(|(_, at, seq)| (*at, *seq));
            Ok(due
                .into_iter()
                .take(max_entries as usize)
                .map(|(r, _, _)| r)
                .collect())
        })
    }

    fn delete<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            self.rows
                .lock()
                .unwrap()
                .retain(|(r, _, _)| r.entry_id != entry_id);
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
            for (r, at, _) in self.rows.lock().unwrap().iter_mut() {
                if r.entry_id == entry_id {
                    r.attempt_count += 1;
                    *at = new_due_at;
                }
            }
            Ok(())
        })
    }

    fn mark_poisoned<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_id: &'a str,
        _diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            self.terminal.lock().unwrap().push(entry_id.to_string());
            Ok(())
        })
    }

    fn requeue<'a>(
        &'a self,
        _deployment: &'a DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            for (r, at, _) in self.rows.lock().unwrap().iter_mut() {
                if r.entry_id == entry_id {
                    *at = new_due_at;
                    r.last_diagnostic_json = Some(diagnostic_json.to_string());
                }
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct RecordingSink {
    sent: Mutex<Vec<OutboundMessage>>,
}

impl RecordingSink {
    fn sent_keys(&self) -> Vec<String> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .map(|m| m.outbox_key.clone())
            .collect()
    }
}

impl MessageSink for RecordingSink {
    fn schemes(&self) -> Vec<String> {
        vec!["fake".to_string(), "refuse".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        let refused = message.destination.starts_with("refuse:");
        if !refused {
            self.sent.lock().unwrap().push(message.clone());
        }
        Box::pin(async move {
            if refused {
                SendOutcome::PermanentFailure(Diagnostic::error("TEST.REFUSED", "refused"))
            } else {
                SendOutcome::Delivered
            }
        })
    }
}

struct Rig {
    rows: Arc<OrderedRows>,
    sink: Arc<RecordingSink>,
    pools: Arc<InMemoryCapacityPools>,
    clock: Arc<Mutex<OffsetDateTime>>,
    dispatcher: OutboxDispatcher,
}

fn rig_with(
    batch: i64,
    store: Arc<dyn CapacityPoolStore>,
    pools: Arc<InMemoryCapacityPools>,
) -> Rig {
    let rows = Arc::new(OrderedRows::default());
    let sink = Arc::new(RecordingSink::default());
    let mut sinks = SinkRegistry::new();
    sinks.register(Arc::clone(&sink) as Arc<dyn MessageSink>);
    let clock = Arc::new(Mutex::new(t0()));
    let now = Arc::clone(&clock);
    let dispatcher = OutboxDispatcher::new(
        Arc::clone(&rows) as Arc<dyn OutboxRowStore>,
        sinks,
        RetryPolicy::default(),
        batch,
    )
    .with_clock(move || *now.lock().unwrap())
    .with_capacity_pools(store);
    Rig {
        rows,
        sink,
        pools,
        clock,
        dispatcher,
    }
}

fn rig(slots: u32, batch: i64) -> Rig {
    let pools = Arc::new(InMemoryCapacityPools::new([CapacityPool::new(POOL, slots)]));
    rig_with(
        batch,
        Arc::clone(&pools) as Arc<dyn CapacityPoolStore>,
        pools,
    )
}

impl Rig {
    fn later(&self, seconds: i64) {
        *self.clock.lock().unwrap() += time::Duration::seconds(seconds);
    }

    async fn hold(&self, holder: &str) {
        let acquired = self
            .pools
            .acquire(&SlotRequest {
                pool: POOL,
                deployment: &dep(),
                tenant: "acme",
                holder,
                call: "inbound-call",
                direction: SlotDirection::Inbound,
                require_live: false,
            })
            .await
            .unwrap();
        assert_eq!(acquired, SlotAcquisition::Acquired);
    }
}

#[tokio::test]
async fn a_pooled_request_waits_without_an_attempt_then_sends_oldest_first_as_slots_free() {
    let rig = rig(1, 10);
    let holder = instance(9);
    rig.hold(&holder).await;
    rig.rows.enqueue(row("older", &instance(1), true));
    rig.rows.enqueue(row("newer", &instance(2), true));

    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(stats.waiting, 2, "{stats:?}");
    assert!(
        rig.sink.sent_keys().is_empty(),
        "nothing dialled while full"
    );
    assert_eq!(rig.rows.attempts("older"), 0, "a wait is not an attempt");
    assert_eq!(rig.rows.attempts("newer"), 0);

    // The holder's session ends: the OLDEST waiting request takes the slot.
    rig.pools.release(&dep(), &holder).await;
    rig.later(5);
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!((stats.succeeded, stats.waiting), (1, 1), "{stats:?}");
    assert_eq!(rig.sink.sent_keys(), vec!["key-older".to_string()]);
    // The slot is now held by the request's instance, until that instance ends.
    assert!(rig.pools.holds(&dep(), &instance(1), POOL));

    rig.pools.release(&dep(), &instance(1)).await;
    rig.later(5);
    rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(
        rig.sink.sent_keys(),
        vec!["key-older".to_string(), "key-newer".to_string()]
    );
    assert!(rig.rows.pending().is_empty());
}

/// A pooled entry carrying a declared call key.
fn keyed(entry: &str, instance: &str, call: &str) -> ClaimedOutboxRow {
    let mut row = row(entry, instance, true);
    row.headers
        .insert(CAPACITY_CALL_KEY_HEADER.to_string(), call.to_string());
    row
}

#[tokio::test]
async fn each_request_is_its_own_call_and_takes_its_own_slot() {
    let rig = rig(3, 10);
    let caller = instance(1);
    rig.hold(&caller).await;
    rig.rows.enqueue(row("first", &caller, true));
    rig.rows.enqueue(row("second", &caller, true));
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!((stats.succeeded, stats.waiting), (2, 0), "{stats:?}");
    assert_eq!(
        rig.pools.usage(POOL).await,
        (1, 2),
        "the inbound call and two outbound calls: three slots"
    );
    assert_eq!(rig.pools.held_calls(&dep(), &caller, POOL), 3);
}

#[tokio::test]
async fn requests_sharing_a_call_key_share_one_slot() {
    let rig = rig(2, 10);
    let caller = instance(1);
    rig.hold(&caller).await;
    rig.rows.enqueue(keyed("dial", &caller, "leg-b"));
    rig.rows.enqueue(keyed("follow-up", &caller, "leg-b"));
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!((stats.succeeded, stats.waiting), (2, 0), "{stats:?}");
    assert_eq!(rig.pools.usage(POOL).await, (1, 1), "one slot for leg-b");
    // Ending leg-b frees its slot alone; the inbound call keeps its own.
    let released = rig
        .pools
        .release_calls(&dep(), &caller, CallSelector::Call("leg-b"))
        .await;
    assert_eq!(released, vec![SlotDirection::Outbound]);
    assert_eq!(rig.pools.usage(POOL).await, (1, 0));
}

#[tokio::test]
async fn a_send_of_a_finished_instance_returns_its_slot_once_delivered() {
    let rig = rig(1, 10);
    let finished = instance(1);
    rig.pools.release_ended(&dep(), &finished).await;
    rig.rows.enqueue(row("goodbye", &finished, true));
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(stats.succeeded, 1, "{stats:?}");
    assert_eq!(
        rig.pools.usage(POOL).await,
        (0, 0),
        "returned at delivery, not at the next sweep"
    );

    // A live instance's send keeps its slot: the call it opened lasts until it ends.
    rig.rows.enqueue(row("dial", &instance(2), true));
    rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(rig.pools.usage(POOL).await, (0, 1));
}

#[tokio::test]
async fn a_send_of_a_finished_instance_returns_its_slot_once_poisoned() {
    let rig = rig(1, 10);
    let finished = instance(1);
    rig.pools.release_ended(&dep(), &finished).await;
    let mut refused = row("goodbye", &finished, true);
    refused.destination = "refuse://out".to_string();
    rig.rows.enqueue(refused);
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(stats.failed, 1, "{stats:?}");
    assert_eq!(rig.pools.usage(POOL).await, (0, 0));
}

#[tokio::test]
async fn the_pool_markers_never_reach_the_wire() {
    let rig = rig(1, 10);
    let mut marked = keyed("call", &instance(1), "leg-a");
    marked
        .headers
        .insert(CAPACITY_CALL_HEADER.to_string(), "true".to_string());
    rig.rows.enqueue(marked);
    rig.dispatcher.dispatch_deployment(&dep()).await;
    let sent = rig.sink.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].headers.contains_key(CAPACITY_POOL_HEADER));
    assert!(!sent[0].headers.contains_key(CAPACITY_CALL_HEADER));
    assert!(!sent[0].headers.contains_key(CAPACITY_CALL_KEY_HEADER));
    assert_eq!(sent[0].headers.get("X-Keep").map(String::as_str), Some("1"));
}

#[tokio::test]
async fn the_media_capture_marker_never_reaches_a_sink_that_does_not_record() {
    let rig = rig(1, 10);
    let mut marked = row("send", &instance(1), false);
    marked.headers.insert(
        sutra_channels::AUDIT_CAPTURE_HEADER.to_string(),
        sutra_channels::AUDIT_CAPTURE_MEDIA.to_string(),
    );
    rig.rows.enqueue(marked);
    rig.dispatcher.dispatch_deployment(&dep()).await;
    let sent = rig.sink.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0]
        .headers
        .contains_key(sutra_channels::AUDIT_CAPTURE_HEADER));
}

#[tokio::test]
async fn a_full_batch_of_waiting_requests_does_not_hold_other_entries_back() {
    let rig = rig(1, 2);
    rig.hold(&instance(9)).await;
    rig.rows.enqueue(row("wait-1", &instance(1), true));
    rig.rows.enqueue(row("wait-2", &instance(2), true));
    rig.rows.enqueue(row("plain", &instance(3), false));
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(
        rig.sink.sent_keys(),
        vec!["key-plain".to_string()],
        "{stats:?}"
    );
    assert_eq!(stats.waiting, 2);
}

/// A store whose holder instance has finished.
struct EndedPools;

#[async_trait::async_trait]
impl CapacityPoolStore for EndedPools {
    fn pool(&self, _name: &str) -> Option<CapacityPool> {
        Some(CapacityPool::new(POOL, 1))
    }
    async fn acquire(&self, request: &SlotRequest<'_>) -> Result<SlotAcquisition, Diagnostic> {
        Ok(if request.require_live {
            SlotAcquisition::InstanceEnded
        } else {
            SlotAcquisition::Acquired
        })
    }
    async fn rebind(&self, _deployment: &DeploymentId, _from: &str, _to: &str) {}
    async fn release(&self, _deployment: &DeploymentId, _holder: &str) -> u32 {
        0
    }
    async fn release_calls(
        &self,
        _deployment: &DeploymentId,
        _holder: &str,
        _selector: CallSelector<'_>,
    ) -> Vec<SlotDirection> {
        Vec::new()
    }
    async fn release_call_if_ended(
        &self,
        _deployment: &DeploymentId,
        _holder: &str,
        _call: &str,
    ) -> u32 {
        0
    }
    async fn usage(&self, _pool: &str) -> (u32, u32) {
        (0, 0)
    }
}

#[tokio::test]
async fn a_call_whose_instance_finished_while_waiting_is_abandoned_not_sent() {
    let pools = Arc::new(InMemoryCapacityPools::new([]));
    let rig = rig_with(10, Arc::new(EndedPools), pools);
    let mut call = row("call", &instance(1), true);
    call.headers
        .insert(CAPACITY_CALL_HEADER.to_string(), "true".to_string());
    rig.rows.enqueue(call);
    // A plain send of a finished instance is still delivered (fire-and-forget).
    rig.rows.enqueue(row("send", &instance(1), true));
    rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(rig.sink.sent_keys(), vec!["key-send".to_string()]);
    assert_eq!(
        rig.rows.terminal.lock().unwrap().clone(),
        vec!["call".to_string()],
        "the abandoned call is kept, terminal"
    );
}

/// A request still queued for a slot when its instance's session ends is SENT, takes no slot
/// and holds none — and does not wait for one, even while the pool is full.
#[tokio::test]
async fn a_request_queued_when_its_session_ends_is_sent_without_a_slot() {
    let rig = rig(1, 10);
    rig.hold(&instance(9)).await; // the pool is full for the whole test
    rig.rows.enqueue(row("live", &instance(3), true));
    rig.rows.enqueue(row("ended", &instance(2), true));

    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(stats.waiting, 2, "{stats:?}");

    // Instance 2's session ends while its request waits.
    rig.pools.end_session(&dep(), &instance(2), None).await;
    rig.later(5);
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!(
        (stats.succeeded, stats.waiting),
        (1, 1),
        "the ended session's request is sent; the live one still waits: {stats:?}"
    );
    assert_eq!(rig.sink.sent_keys(), vec!["key-ended".to_string()]);
    assert!(
        !rig.pools.holds(&dep(), &instance(2), POOL),
        "an ended session takes no slot"
    );
    assert_eq!(
        rig.pools.usage(POOL).await,
        (1, 0),
        "only the holder's slot is in use"
    );
}

/// Ending ONE call (`pool-call-key`) lets that call's queued request go without a slot; the
/// instance's other calls still draw from the pool.
#[tokio::test]
async fn a_request_on_an_ended_call_is_sent_without_a_slot() {
    let rig = rig(1, 10);
    rig.hold(&instance(9)).await;
    rig.rows
        .enqueue(keyed("ended-call", &instance(2), "call-a"));
    rig.rows
        .enqueue(keyed("other-call", &instance(2), "call-b"));

    rig.pools
        .end_session(&dep(), &instance(2), Some("call-a"))
        .await;
    let stats = rig.dispatcher.dispatch_deployment(&dep()).await;
    assert_eq!((stats.succeeded, stats.waiting), (1, 1), "{stats:?}");
    assert_eq!(rig.sink.sent_keys(), vec!["key-ended-call".to_string()]);
}
