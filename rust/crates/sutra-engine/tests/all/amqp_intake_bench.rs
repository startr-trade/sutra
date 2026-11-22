//! The AMQP 1.0 intake benchmark: one binding's throughput and latency against `max-in-flight`.
//!
//! For each `max-in-flight` in [`WINDOWS`], an engine boots on a fresh database with a package
//! whose intake parks each instance on a timer (so every delivery is a durable intake, then an
//! accept). Two loads run on their own queues:
//!
//! - **preload**: [`PRELOAD`] messages queued before the engine starts, then consumed; the
//!   throughput is settled messages over the time from the first accept to the last;
//! - **steady**: a producer at each rate in [`RATES`] for [`STEADY_SECS`]; the latency of a
//!   message is from the broker accepting the send to the engine settling the delivery.
//!
//! Settle instants come from the transport's `amqp disposition executed` debug event, captured
//! in-process. On one link the delivery id grows with arrival order, so the `k`th delivery of a
//! queue is the `k`th message sent to it.
//!
//! `#[ignore = "bench"]`, and it does nothing unless `SUTRA_AMQP_INTAKE_BENCH=1` is set, so the
//! `--ignored` sweep of `make test-docker` passes over it. Run it alone:
//! `SUTRA_AMQP_INTAKE_BENCH=1 cargo test -p sutra-engine --test all -- --ignored --nocapture
//! amqp_intake_bench`; `SUTRA_BENCH_WINDOWS=1,16` narrows the matrix. It prints a markdown
//! matrix.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tracing::field::{Field, Visit};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use crate::amqp_parallel_intake_it::{boot, fresh_queue, parked, start_broker, Producer};

const WINDOWS: [u32; 6] = [1, 4, 8, 16, 32, 64];
const RATES: [u32; 4] = [10, 25, 50, 100];
const PRELOAD: usize = 2_000;
const STEADY_SECS: u64 = 20;
/// The longest a run waits for its queue to be consumed before it reports what it settled.
const DRAIN_CAP: Duration = Duration::from_secs(120);

// ---- settle capture ------------------------------------------------------------------------

/// `(channel, delivery id, instant)` of every executed disposition.
type Settles = Arc<Mutex<Vec<(String, u32, Instant)>>>;

fn settles() -> &'static Settles {
    static SETTLES: OnceLock<Settles> = OnceLock::new();
    SETTLES.get_or_init(Default::default)
}

#[derive(Default)]
struct SettleVisit {
    channel: Option<String>,
    delivery_id: Option<u32>,
    settled: bool,
    accepted: bool,
}

impl Visit for SettleVisit {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "delivery_id" {
            self.delivery_id = u32::try_from(value).ok();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "channel" {
            self.channel = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.settled = format!("{value:?}") == "amqp disposition executed",
            // `%channel` arrives here; its Debug text is the Display text.
            "channel" => self.channel = Some(format!("{value:?}")),
            // Only an accept counts: a release would be redelivered and counted twice.
            "decision" => self.accepted = format!("{value:?}") == "Ack",
            _ => {}
        }
    }
}

struct SettleLayer;

impl<S: tracing::Subscriber> Layer<S> for SettleLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visit = SettleVisit::default();
        event.record(&mut visit);
        if let (true, true, Some(channel), Some(id)) = (
            visit.settled,
            visit.accepted,
            visit.channel,
            visit.delivery_id,
        ) {
            settles()
                .lock()
                .unwrap()
                .push((channel, id, Instant::now()));
        }
    }
}

fn install_capture() {
    let filter = tracing_subscriber::filter::filter_fn(|meta| {
        meta.target().starts_with("sutra_transport_amqp") && *meta.level() <= tracing::Level::DEBUG
    });
    let subscriber = tracing_subscriber::registry().with(SettleLayer.with_filter(filter));
    tracing::subscriber::set_global_default(subscriber)
        .expect("the bench runs alone, with its own subscriber");
}

/// The settle instants of one channel, by arrival index (delivery id minus the first one).
fn settled_on(channel: &str) -> BTreeMap<u32, Instant> {
    let all = settles().lock().unwrap();
    let mine: Vec<(u32, Instant)> = all
        .iter()
        .filter(|(c, _, _)| c == channel)
        .map(|(_, id, at)| (*id, *at))
        .collect();
    let first = mine.iter().map(|(id, _)| *id).min().unwrap_or(0);
    mine.into_iter()
        .map(|(id, at)| (id.wrapping_sub(first), at))
        .collect()
}

async fn wait_settled(channel: &str, n: usize, cap: Duration) -> bool {
    let deadline = Instant::now() + cap;
    while Instant::now() < deadline {
        if settled_on(channel).len() >= n {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

// ---- the runs ------------------------------------------------------------------------------

struct Steady {
    rate: u32,
    msg_per_s: f64,
    p50: Duration,
    p99: Duration,
    settled: usize,
    sent: usize,
}

async fn steady(port: u16, queue: &str, channel: &str, rate: u32) -> Steady {
    let mut producer = Producer::open(port, queue).await;
    let total = (rate as u64 * STEADY_SECS) as usize;
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let start = Instant::now();
    let mut sent_at = Vec::with_capacity(total);
    for k in 0..total {
        let due = start + interval * k as u32;
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            tokio::time::sleep(wait).await;
        }
        producer.send(&format!("{channel}-{k:06}")).await;
        sent_at.push(Instant::now());
    }
    producer.close().await;
    wait_settled(channel, total, DRAIN_CAP).await;
    let settled = settled_on(channel);
    let mut latencies: Vec<Duration> = settled
        .iter()
        .filter_map(|(k, at)| {
            sent_at
                .get(*k as usize)
                .map(|sent| at.saturating_duration_since(*sent))
        })
        .collect();
    latencies.sort();
    let last = settled.values().max().copied().unwrap_or(start);
    Steady {
        rate,
        msg_per_s: settled.len() as f64 / last.duration_since(start).as_secs_f64().max(0.001),
        p50: percentile(&latencies, 50.0),
        p99: percentile(&latencies, 99.0),
        settled: settled.len(),
        sent: total,
    }
}

struct Row {
    window: u32,
    preload_msg_per_s: f64,
    preload_settled: usize,
    steady: Vec<Steady>,
}

async fn run_window(port: u16, window: u32) -> Row {
    // Each window reuses the channel names: start its capture empty.
    settles().lock().unwrap().clear();
    // Queue 0 is the preload; queues 1.. are the steady rates.
    let queues: Vec<String> = (0..=RATES.len()).map(|_| fresh_queue()).collect();
    let mut producer = Producer::open(port, &queues[0]).await;
    for k in 0..PRELOAD {
        producer.send(&format!("pre-{k:06}")).await;
    }
    producer.close().await;
    let (engine, pool) = boot(port, &queues, window).await;

    wait_settled("in-0", PRELOAD, DRAIN_CAP * 3).await;
    let pre = settled_on("in-0");
    let first = pre.values().min().copied();
    let last = pre.values().max().copied();
    let preload_msg_per_s = match (first, last) {
        (Some(a), Some(b)) if b > a => pre.len() as f64 / b.duration_since(a).as_secs_f64(),
        _ => 0.0,
    };

    let mut steady_runs = Vec::new();
    for (i, rate) in RATES.iter().enumerate() {
        let channel = format!("in-{}", i + 1);
        steady_runs.push(steady(port, &queues[i + 1], &channel, *rate).await);
    }
    // Every accepted delivery is a parked instance: the accepts are real intakes.
    let instances = parked(&pool).await;
    let accepted = pre.len() + steady_runs.iter().map(|s| s.settled).sum::<usize>();
    assert_eq!(
        instances as usize, accepted,
        "one parked instance per accept"
    );
    engine.shutdown().await;
    Row {
        window,
        preload_msg_per_s,
        preload_settled: pre.len(),
        steady: steady_runs,
    }
}

fn matrix(rows: &[Row]) -> String {
    let mut out = String::from("| max-in-flight | preload msg/s |");
    for rate in RATES {
        out.push_str(&format!(" {rate}/s: msg/s, p50, p99 |"));
    }
    out.push_str("\n|---|---|");
    for _ in RATES {
        out.push_str("---|");
    }
    out.push('\n');
    for row in rows {
        out.push_str(&format!(
            "| {} | {:.1}{} |",
            row.window,
            row.preload_msg_per_s,
            if row.preload_settled < PRELOAD {
                format!(" ({}/{PRELOAD} settled)", row.preload_settled)
            } else {
                String::new()
            }
        ));
        for s in &row.steady {
            debug_assert!(RATES.contains(&s.rate));
            out.push_str(&format!(
                " {:.1}, {} ms, {} ms{} |",
                s.msg_per_s,
                s.p50.as_millis(),
                s.p99.as_millis(),
                if s.settled < s.sent {
                    format!(" ({}/{} settled)", s.settled, s.sent)
                } else {
                    String::new()
                }
            ));
        }
        out.push('\n');
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench"]
async fn amqp_intake_bench() {
    if std::env::var("SUTRA_AMQP_INTAKE_BENCH").as_deref() != Ok("1") {
        eprintln!("amqp_intake_bench: set SUTRA_AMQP_INTAKE_BENCH=1 to run it");
        return;
    }
    install_capture();
    let broker = start_broker().await;
    let windows: Vec<u32> = std::env::var("SUTRA_BENCH_WINDOWS")
        .ok()
        .map(|v| v.split(',').filter_map(|w| w.trim().parse().ok()).collect())
        .unwrap_or_else(|| WINDOWS.to_vec());
    let mut rows = Vec::new();
    for window in windows {
        let row = run_window(broker.port, window).await;
        eprintln!("{}", matrix(std::slice::from_ref(&row)));
        rows.push(row);
    }
    println!("\n{}", matrix(&rows));
}
