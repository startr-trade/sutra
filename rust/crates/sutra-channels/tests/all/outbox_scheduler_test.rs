//! The concurrent outbox dispatcher's delivery scheduler, driven through the public loop: one
//! ordered chain per instance, under a buffer, a per-destination and a global bound. A slow
//! destination delays only its own rows; the claimer waits only for buffer room; pooled rows go
//! out off the claimer in claim order; failures, hand-offs and a panicking send keep the
//! at-least-once, once-per-row behaviour; and every delivered row's lag is observed once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sutra_channels::{
    spawn_concurrent_dispatch_loop, DeliveryObservation, LiveDeploymentSet, OutboxConcurrency,
    OutboxDispatcher, OutboxDispatcherHandle, OutboxSignal, CAPACITY_POOL_HEADER,
};
use time::OffsetDateTime;

use crate::support::outbox_doubles::{dep, dispatcher, row, wait_until, RecordingSink, Table};

fn spawn(
    dispatcher: OutboxDispatcher,
    bounds: (usize, usize, usize),
    signal: &OutboxSignal,
    tick_ms: u64,
) -> OutboxDispatcherHandle {
    let (workers, per_destination, max_buffered) = bounds;
    spawn_concurrent_dispatch_loop(
        &tokio::runtime::Handle::current(),
        Arc::new(dispatcher),
        LiveDeploymentSet::new(vec![dep()]),
        Duration::from_millis(tick_ms),
        OutboxConcurrency {
            workers,
            per_destination,
            max_buffered,
        },
        signal.clone(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_destination_does_not_delay_other_destinations() {
    let table = Arc::new(Table::default());
    // The slow rows come first in claim order: with instance-hashed workers they would sit in
    // front of the fast ones.
    for i in 0..16 {
        let inst = format!("slow-{i}");
        table.insert(row(&format!("s{i:02}"), &inst, "test://slow.example/in"));
    }
    for i in 0..64 {
        let inst = format!("fast-{i}");
        table.insert(row(&format!("f{i:02}"), &inst, "test://fast.example/in"));
    }
    let mut sink = RecordingSink::new(5);
    sink.slow_delay = Duration::from_millis(400);
    let sink = Arc::new(sink);
    let signal = OutboxSignal::new();
    let started = Instant::now();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (8, 4, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    let delivered = sink.delivered.lock().unwrap().clone();
    let last_fast = delivered
        .iter()
        .filter(|(d, _, _)| d.contains("fast"))
        .map(|(_, _, at)| *at)
        .max()
        .expect("fast rows delivered");
    let last_slow = delivered
        .iter()
        .filter(|(d, _, _)| d.contains("slow"))
        .map(|(_, _, at)| *at)
        .max()
        .expect("slow rows delivered");
    assert!(
        last_fast.duration_since(started) < Duration::from_millis(800),
        "the fast destination drained in {:?}, behind the slow one",
        last_fast.duration_since(started)
    );
    assert!(last_fast < last_slow, "the slow destination took longer");
    assert!(sink.peak_for("test://slow.example") <= 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_instances_rows_stay_in_claim_order_under_concurrency() {
    let table = Arc::new(Table::default());
    for i in 0..150 {
        let inst = format!("inst-{}", i % 5);
        table.insert(row(
            &format!("e{i:03}"),
            &inst,
            &format!("test://order.example/{inst}"),
        ));
    }
    let sink = Arc::new(RecordingSink::new(2));
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 7),
        (64, 16, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    let mut per_instance: HashMap<String, Vec<String>> = HashMap::new();
    for (destination, id, _) in sink.delivered.lock().unwrap().iter() {
        per_instance
            .entry(destination.clone())
            .or_default()
            .push(id.clone());
    }
    assert_eq!(per_instance.len(), 5);
    for (instance, ids) in per_instance {
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "{instance} delivered out of claim order");
        assert_eq!(ids.len(), 30);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_claimer_never_waits_on_one_busy_instance() {
    let table = Arc::new(Table::default());
    // One instance with a long chain on a slow destination, first in claim order…
    for i in 0..40 {
        table.insert(row(&format!("b{i:02}"), "busy", "test://slow.example/busy"));
    }
    // …and other instances' rows claimed only in later batches.
    for i in 0..20 {
        let inst = format!("other-{i}");
        table.insert(row(&format!("o{i:02}"), &inst, "test://fast.example/in"));
    }
    let mut sink = RecordingSink::new(2);
    sink.slow_delay = Duration::from_millis(200);
    let sink = Arc::new(sink);
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 10),
        (8, 8, 4096),
        &signal,
        10,
    );
    signal.wake();
    let others_done = || {
        sink.delivered_ids()
            .iter()
            .filter(|id| id.starts_with('o'))
            .count()
            == 20
    };
    wait_until(others_done).await;
    let busy_delivered = sink
        .delivered_ids()
        .iter()
        .filter(|id| id.starts_with('b'))
        .count();
    assert!(
        busy_delivered < 40,
        "the other instances went out while the busy chain was still sending \
         ({busy_delivered} of 40 sent)"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_buffer_bound_applies_backpressure_to_the_claim() {
    let table = Arc::new(Table::default());
    for i in 0..60 {
        let inst = format!("inst-{i}");
        table.insert(row(
            &format!("e{i:02}"),
            &inst,
            &format!("test://h{i}.example/in"),
        ));
    }
    let sink = Arc::new(RecordingSink::new(40));
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (4, 4, 8),
        &signal,
        10,
    );
    signal.wake();
    let mut peak_buffered = 0;
    while table.remaining() > 0 {
        let (in_flight, buffered) = handle.gauges();
        assert!(buffered <= 8, "buffered {buffered} past the bound");
        assert!(
            in_flight <= 4,
            "in flight {in_flight} past the global bound"
        );
        peak_buffered = peak_buffered.max(buffered);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    handle.shutdown().await;
    assert_eq!(peak_buffered, 8, "the claim filled the buffer, then waited");
    assert_eq!(sink.delivered_ids().len(), 60);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_never_exceeds_the_global_bound() {
    let table = Arc::new(Table::default());
    for i in 0..60 {
        let inst = format!("inst-{i}");
        table.insert(row(
            &format!("e{i:02}"),
            &inst,
            &format!("test://h{i}.example/in"),
        ));
    }
    let sink = Arc::new(RecordingSink::new(20));
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (4, 4, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    assert_eq!(sink.peak.load(std::sync::atomic::Ordering::SeqCst), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_per_destination_never_exceeds_its_bound() {
    let table = Arc::new(Table::default());
    for i in 0..40 {
        let inst = format!("one-{i}");
        table.insert(row(
            &format!("a{i:02}"),
            &inst,
            &format!("test://one.example/p{i}"),
        ));
        let inst = format!("two-{i}");
        table.insert(row(
            &format!("b{i:02}"),
            &inst,
            &format!("test://two.example:8443/p{i}"),
        ));
    }
    let sink = Arc::new(RecordingSink::new(15));
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 100),
        (16, 3, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    assert_eq!(sink.peak_for("test://one.example"), 3);
    assert_eq!(sink.peak_for("test://two.example:8443"), 3);
    assert!(
        sink.peak.load(std::sync::atomic::Ordering::SeqCst) > 3,
        "two destinations sent at once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pooled_row_is_sent_off_the_claimer_in_claim_order() {
    let table = Arc::new(Table::default());
    for i in 0..8 {
        let mut r = row(
            &format!("p{i}"),
            &format!("pooled-{i}"),
            "test://slow-pool.example/in",
        );
        r.headers
            .insert(CAPACITY_POOL_HEADER.to_string(), "lines".to_string());
        table.insert(r);
    }
    for i in 0..10 {
        table.insert(row(
            &format!("q{i}"),
            &format!("plain-{i}"),
            "test://fast.example/in",
        ));
    }
    let mut sink = RecordingSink::new(2);
    sink.slow_delay = Duration::from_millis(100);
    let sink = Arc::new(sink);
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (8, 8, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    let delivered = sink.delivered.lock().unwrap().clone();
    let pooled: Vec<&(String, String, Instant)> = delivered
        .iter()
        .filter(|(_, id, _)| id.starts_with('p'))
        .collect();
    let ids: Vec<&str> = pooled.iter().map(|(_, id, _)| id.as_str()).collect();
    assert_eq!(
        ids,
        ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"],
        "claim order"
    );
    assert_eq!(
        sink.peak_for("test://slow-pool.example"),
        1,
        "one pooled send at a time"
    );
    let last_plain = delivered
        .iter()
        .filter(|(_, id, _)| id.starts_with('q'))
        .map(|(_, _, at)| *at)
        .max()
        .expect("plain rows delivered");
    assert!(
        last_plain < pooled[2].2,
        "the plain rows did not wait behind the pooled sends"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_row_is_released_and_retried_with_backoff_as_before() {
    let table = Arc::new(Table::default());
    table.insert(row("ok1", "i1", "test://a.example/in"));
    table.insert(row("bad", "i2", "test://a.example/in"));
    let mut sink = RecordingSink::new(1);
    sink.fail = Some("bad".to_string());
    let sink = Arc::new(sink);
    let signal = OutboxSignal::new();
    let before = OffsetDateTime::now_utc();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (4, 4, 4096),
        &signal,
        10,
    );
    signal.wake();
    wait_until(|| table.deferred_ids().contains(&"bad".to_string())).await;
    wait_until(|| table.remaining() == 1 && signal.inflight_count() == 0).await;
    let (_, due) = table.deferred.lock().unwrap()[0];
    assert!(
        due >= before + time::Duration::milliseconds(900),
        "deferred by the retry curve's first backoff (1 s), got {}",
        due - before
    );
    // The backoff passes: the row is claimed and tried again.
    table.undefer_all();
    signal.wake();
    wait_until(|| table.deferred_ids().len() == 2).await;
    wait_until(|| signal.inflight_count() == 0).await;
    handle.shutdown().await;
    assert_eq!(sink.delivered_ids(), ["ok1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_is_delivered_once_across_the_hand_off_and_the_claim() {
    let table = Arc::new(Table::default());
    for i in 0..100 {
        table.insert(row(
            &format!("c{i:03}"),
            &format!("inst-{}", i % 30),
            "test://a.example/in",
        ));
    }
    let sink = Arc::new(RecordingSink::new(3));
    let signal = OutboxSignal::new();
    // A 1 ms tick: the claim runs constantly while hand-offs arrive.
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 20),
        (64, 16, 4096),
        &signal,
        1,
    );
    wait_until(|| signal.accepts_hand_off()).await;
    for i in 0..50 {
        let r = row(
            &format!("h{i:03}"),
            &format!("inst-{}", i % 30),
            "test://a.example/in",
        );
        signal.reserve(std::slice::from_ref(&r.entry_id));
        table.insert(r.clone()); // the "commit"
        assert!(signal.hand_off(&dep(), vec![r]).is_empty());
    }
    wait_until(|| table.remaining() == 0).await;
    handle.shutdown().await;
    let mut ids = sink.delivered_ids();
    assert_eq!(ids.len(), 150, "no row delivered twice");
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 150, "every row delivered");
    assert_eq!(signal.inflight_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_send_releases_the_rest_of_its_chain() {
    let table = Arc::new(Table::default());
    for i in 0..5 {
        table.insert(row(&format!("p{i}"), "panics", "test://a.example/in"));
    }
    table.insert(row("other", "calm", "test://b.example/in"));
    let mut sink = RecordingSink::new(30);
    sink.panic_once = Some("p0".to_string());
    let sink = Arc::new(sink);
    let signal = OutboxSignal::new();
    let handle = spawn(
        dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
        (8, 8, 4096),
        &signal,
        20,
    );
    signal.wake();
    wait_until(|| table.remaining() == 0).await;
    wait_until(|| signal.inflight_count() == 0).await;
    let (in_flight, buffered) = handle.gauges();
    handle.shutdown().await;
    assert!(sink.panicked.load(std::sync::atomic::Ordering::SeqCst));
    let chain: Vec<String> = sink
        .delivered_ids()
        .into_iter()
        .filter(|id| id.starts_with('p'))
        .collect();
    assert_eq!(
        chain,
        ["p0", "p1", "p2", "p3", "p4"],
        "released, re-claimed and delivered once each, in order"
    );
    assert_eq!(
        (in_flight, buffered),
        (0, 0),
        "the panicked chain left no count behind"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_lag_is_observed_once_per_delivered_row() {
    let table = Arc::new(Table::default());
    let enqueued = OffsetDateTime::now_utc() - time::Duration::seconds(3);
    for i in 0..20 {
        let mut r = row(
            &format!("e{i:02}"),
            &format!("inst-{i}"),
            "test://a.example/in",
        );
        r.created_at = enqueued;
        table.insert(r);
    }
    table.insert(row("bad", "inst-bad", "test://a.example/in"));
    let mut sink = RecordingSink::new(1);
    sink.fail = Some("bad".to_string());
    let sink = Arc::new(sink);
    let observed: Arc<Mutex<Vec<DeliveryObservation>>> = Arc::default();
    let sink_observed = Arc::clone(&observed);
    let dispatcher = dispatcher(Arc::clone(&table), Arc::clone(&sink), 50)
        .with_delivery_observer(Arc::new(move |o| sink_observed.lock().unwrap().push(o)));
    let signal = OutboxSignal::new();
    let handle = spawn(dispatcher, (8, 8, 4096), &signal, 10);
    signal.wake();
    wait_until(|| table.remaining() == 1 && signal.inflight_count() == 0).await;
    handle.shutdown().await;
    let observed = observed.lock().unwrap().clone();
    assert_eq!(
        observed.len(),
        20,
        "once per delivered row, none for the failure"
    );
    for o in &observed {
        assert_eq!(o.scheme, "test");
        assert_eq!(o.attempt, 0);
        assert!(o.lag >= Duration::from_secs(3), "lag {:?}", o.lag);
    }
}
