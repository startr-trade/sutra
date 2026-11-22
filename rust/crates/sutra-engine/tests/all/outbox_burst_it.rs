//! Tier 2: an outbound burst against PostgreSQL and two in-test HTTP destinations, one answering
//! at once and one after 500 ms. The fast destination's rows go out within the lag budget while
//! the slow one drains at its per-destination bound; and rows queued in memory behind a slow send
//! keep their claim lease past `CLAIM_LEASE`, so a second dispatcher on the same database never
//! delivers them too.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::Router;
use sutra_channels::{
    spawn_concurrent_dispatch_loop, HttpSink, LiveDeploymentSet, OutboxConcurrency,
    OutboxDispatcher, OutboxSignal, RetryPolicy, SinkRegistry,
};
use sutra_engine::outbox::PgOutboxRows;
use sutra_persistence::stores::{OutboxStore, PgOutboxStore};
use uuid::Uuid;

use crate::outbox_it::{channel_dep, entry, fresh_pool};

/// `(path, body, received at)` of every request.
type Received = Arc<Mutex<Vec<(String, String, Instant)>>>;

async fn destinations(slow: Duration) -> (SocketAddr, Received) {
    let received: Received = Arc::default();
    let app = Router::new()
        .route(
            "/{which}",
            post(
                move |State(rx): State<Received>, Path(which): Path<String>, body: Bytes| async move {
                    if which == "slow" {
                        tokio::time::sleep(slow).await;
                    }
                    rx.lock().unwrap().push((
                        which,
                        String::from_utf8_lossy(&body).into_owned(),
                        Instant::now(),
                    ));
                    "ok"
                },
            ),
        )
        .with_state(Arc::clone(&received));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, received)
}

fn dispatcher(pool: sqlx::PgPool) -> Arc<OutboxDispatcher> {
    let mut sinks = SinkRegistry::new();
    sinks.register(Arc::new(HttpSink::new()));
    Arc::new(OutboxDispatcher::new(
        Arc::new(PgOutboxRows::new(pool)),
        sinks,
        RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(300), false),
        50,
    ))
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

async fn wait_for(received: &Received, n: usize, bound: Duration) {
    let started = Instant::now();
    while received.lock().unwrap().len() < n {
        assert!(
            started.elapsed() < bound,
            "only {} of {n} delivered in {bound:?}",
            received.lock().unwrap().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn a_burst_on_two_destinations_drains_within_budget() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let slow_ms = 500;
    // Two destination authorities (the bound is per scheme + host + port, not per path).
    let (fast_addr, received) = destinations(Duration::from_millis(slow_ms)).await;
    let (slow_addr, slow_received) = destinations(Duration::from_millis(slow_ms)).await;
    // 300 instances emitting two rows each; 30% of them to the slow destination.
    let mut expected = 0;
    for i in 0..300 {
        let instance = Uuid::new_v4();
        let which = if i % 10 < 3 { "slow" } else { "fast" };
        for seq in 0..2 {
            let addr = if which == "slow" {
                slow_addr
            } else {
                fast_addr
            };
            let mut e = entry(&format!("http://{addr}/{which}"));
            e.instance_id = instance;
            e.outbox_key = format!("key-{instance}-{seq}");
            e.body = format!("{which}-{i}-{seq}").into_bytes().into();
            store.enqueue(&e).await.expect("enqueue");
            expected += 1;
        }
    }
    // The burst is "enqueued" now: lag is measured from here.
    sqlx::query("UPDATE outbox_entry SET created_at = now(), next_attempt_at = now()")
        .execute(&pool)
        .await
        .expect("stamp the burst");
    let started = Instant::now();
    let signal = OutboxSignal::new();
    let handle = spawn_concurrent_dispatch_loop(
        &tokio::runtime::Handle::current(),
        dispatcher(pool.clone()),
        LiveDeploymentSet::new(vec![channel_dep()]),
        Duration::from_millis(200),
        OutboxConcurrency {
            workers: 64,
            per_destination: 16,
            max_buffered: 4096,
        },
        signal.clone(),
    );
    signal.wake();
    let all = Arc::clone(&received);
    let deadline = Instant::now() + Duration::from_secs(60);
    while all.lock().unwrap().len() + slow_received.lock().unwrap().len() < expected {
        assert!(Instant::now() < deadline, "the burst did not drain in 60 s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.shutdown().await;

    let mut rows = received.lock().unwrap().clone();
    rows.extend(slow_received.lock().unwrap().iter().cloned());
    let mut fast: Vec<Duration> = rows
        .iter()
        .filter(|(w, _, _)| w == "fast")
        .map(|(_, _, at)| at.duration_since(started))
        .collect();
    fast.sort();
    let slow: Vec<Instant> = rows
        .iter()
        .filter(|(w, _, _)| w == "slow")
        .map(|(_, _, at)| *at)
        .collect();
    let (p99, max) = (percentile(&fast, 99.0), *fast.last().unwrap());
    assert!(p99 <= Duration::from_secs(2), "fast p99 lag {p99:?}");
    assert!(max <= Duration::from_secs(5), "fast max lag {max:?}");
    let slow_span = slow.iter().max().unwrap().duration_since(started);
    let rate = slow.len() as f64 / slow_span.as_secs_f64();
    let floor = 0.9 * 16.0 / (slow_ms as f64 / 1000.0);
    eprintln!(
        "burst: fast p99 {p99:?} max {max:?}; slow {} rows at {rate:.1} rows/s (floor {floor:.1})",
        slow.len()
    );
    assert!(
        rate >= floor,
        "the slow destination drained at {rate:.1} rows/s, under {floor:.1}"
    );
    let mut bodies: Vec<&str> = rows.iter().map(|(_, b, _)| b.as_str()).collect();
    bodies.sort_unstable();
    bodies.dedup();
    assert_eq!(bodies.len(), expected, "every row once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn queued_rows_keep_their_lease_past_claim_lease() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let (addr, received) = destinations(Duration::from_millis(1000)).await;
    // 36 rows on one slow destination, one at a time: the last waits ~35 s in memory, past the
    // 30 s claim lease, which only the renewer keeps alive.
    let rows = 36;
    for i in 0..rows {
        let mut e = entry(&format!("http://{addr}/slow"));
        e.body = format!("row-{i}").into_bytes().into();
        store.enqueue(&e).await.expect("enqueue");
    }
    let one_at_a_time = OutboxConcurrency {
        workers: 1,
        per_destination: 1,
        max_buffered: 4096,
    };
    let first = OutboxSignal::new();
    let replica_a = spawn_concurrent_dispatch_loop(
        &tokio::runtime::Handle::current(),
        dispatcher(pool.clone()),
        LiveDeploymentSet::new(vec![channel_dep()]),
        Duration::from_millis(500),
        one_at_a_time,
        first.clone(),
    );
    first.wake();
    // Let the first replica claim the whole batch before the second starts claiming.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let second = OutboxSignal::new();
    let replica_b = spawn_concurrent_dispatch_loop(
        &tokio::runtime::Handle::current(),
        dispatcher(pool.clone()),
        LiveDeploymentSet::new(vec![channel_dep()]),
        Duration::from_millis(500),
        one_at_a_time,
        second.clone(),
    );
    wait_for(&received, rows, Duration::from_secs(90)).await;
    // A lapsed lease would let replica B deliver a queued row a second time.
    tokio::time::sleep(Duration::from_secs(3)).await;
    replica_a.shutdown().await;
    replica_b.shutdown().await;
    let got = received.lock().unwrap().clone();
    let mut bodies: Vec<&str> = got.iter().map(|(_, b, _)| b.as_str()).collect();
    bodies.sort_unstable();
    bodies.dedup();
    assert_eq!(got.len(), rows, "no row delivered twice: {}", got.len());
    assert_eq!(bodies.len(), rows);
}
