//! sutra-load-sink — the callback sink for the `relay` load profile.
//!
//! `POST /callback` records one settle: the JSON body's `ref` (for duplicate detection) and its
//! `sentAt` (epoch milliseconds stamped by k6 when the intake request left), so the settle latency
//! is `receivedAt - sentAt`. `GET /stats` returns the counters and latency percentiles as JSON;
//!
//! The burst bench (`relay-bench.sh BURST=<rows>`) delivers to two more endpoints: `POST /fast`
//! answers at once, `POST /slow` after `SLOW_MS` (default 500) milliseconds. Each records the
//! body's `ref` and the lag `receivedAt - created`, where `created` is the instant the engine
//! built the request (RFC 3339 / ISO-8601 date-time, or epoch milliseconds). `GET /stats`
//! carries them as `fast` and `slow` (count, duplicates, lag p50/p99/max, first/last receipt);
//! `POST /reset` clears them between runs. Bind address: `SINK_ADDR` (default `127.0.0.1:0`; the
//! bound port is printed on stdout as `listening on <addr>`).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

#[derive(Default)]
struct Recorded {
    refs: HashSet<String>,
    duplicates: u64,
    malformed: u64,
    latencies_ms: Vec<f64>,
    first_at_ms: Option<f64>,
    last_at_ms: Option<f64>,
}

#[derive(Default)]
struct Sinks {
    callback: Recorded,
    fast: Recorded,
    slow: Recorded,
}

type Shared = Arc<Mutex<Sinks>>;

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

async fn callback(State(state): State<Shared>, body: Bytes) -> &'static str {
    let at = now_ms();
    let parsed: Option<Value> = serde_json::from_slice(&body).ok();
    let mut sinks = state.lock().expect("sink lock");
    let rec = &mut sinks.callback;
    let Some(value) = parsed else {
        rec.malformed += 1;
        return "ok";
    };
    let reference = value
        .get("ref")
        .map(|r| r.as_str().map(str::to_string).unwrap_or_else(|| r.to_string()));
    let sent_at = value.get("sentAt").and_then(|s| {
        s.as_f64()
            .or_else(|| s.as_str().and_then(|t| t.parse::<f64>().ok()))
    });
    match reference {
        Some(r) if !rec.refs.insert(r.clone()) => {
            rec.duplicates += 1;
            return "ok";
        }
        Some(_) => {}
        None => {
            rec.malformed += 1;
            return "ok";
        }
    }
    if let Some(sent) = sent_at {
        rec.latencies_ms.push((at - sent).max(0.0));
    }
    rec.first_at_ms = Some(rec.first_at_ms.map_or(at, |f| f.min(at)));
    rec.last_at_ms = Some(rec.last_at_ms.map_or(at, |l| l.max(at)));
    "ok"
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Epoch milliseconds of `YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM|@UTC]` (UTC assumed without an
/// offset), or of a bare number.
fn epoch_ms(value: &Value) -> Option<f64> {
    if let Some(n) = value.as_f64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if let Ok(n) = text.parse::<f64>() {
        return Some(n);
    }
    let num = |s: &str| s.parse::<i64>().ok();
    let (date, time) = text.split_once('T')?;
    let mut d = date.splitn(3, '-');
    let (y, mo, da) = (num(d.next()?)?, num(d.next()?)?, num(d.next()?)?);
    let time = time.split('@').next()?;
    let (clock, offset_min) = if let Some(c) = time.strip_suffix('Z') {
        (c, 0)
    } else if let Some(i) = time.rfind(['+', '-']) {
        let (c, off) = time.split_at(i);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let (oh, om) = off[1..].split_once(':').unwrap_or((&off[1..], "0"));
        (c, sign * (num(oh)? * 60 + num(om)?))
    } else {
        (time, 0)
    };
    let mut t = clock.splitn(3, ':');
    let (h, mi) = (num(t.next()?)?, num(t.next()?)?);
    let sec: f64 = t.next().unwrap_or("0").parse().ok()?;
    let days = days_from_civil(y, mo, da);
    let secs = days * 86_400 + h * 3_600 + mi * 60 - offset_min * 60;
    Some(secs as f64 * 1000.0 + sec * 1000.0)
}

/// Record one burst request on `fast` or `slow`.
fn record_burst(rec: &mut Recorded, at: f64, body: &[u8]) {
    let Some(value) = serde_json::from_slice::<Value>(body).ok() else {
        rec.malformed += 1;
        return;
    };
    let Some(reference) = value.get("ref").and_then(Value::as_str).map(str::to_string) else {
        rec.malformed += 1;
        return;
    };
    if !rec.refs.insert(reference) {
        rec.duplicates += 1;
        return;
    }
    match value.get("created").and_then(epoch_ms) {
        Some(created) => rec.latencies_ms.push((at - created).max(0.0)),
        None => rec.malformed += 1,
    }
    rec.first_at_ms = Some(rec.first_at_ms.map_or(at, |f| f.min(at)));
    rec.last_at_ms = Some(rec.last_at_ms.map_or(at, |l| l.max(at)));
}

async fn fast(State(state): State<Shared>, body: Bytes) -> &'static str {
    let at = now_ms();
    record_burst(&mut state.lock().expect("sink lock").fast, at, &body);
    "ok"
}

async fn slow(State(state): State<Shared>, body: Bytes) -> &'static str {
    let at = now_ms();
    let delay: u64 = std::env::var("SLOW_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
    record_burst(&mut state.lock().expect("sink lock").slow, at, &body);
    "ok"
}

fn burst_stats(rec: &Recorded) -> Value {
    let mut sorted = rec.latencies_ms.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "count": rec.refs.len(),
        "duplicates": rec.duplicates,
        "malformed": rec.malformed,
        "firstAtMs": rec.first_at_ms,
        "lastAtMs": rec.last_at_ms,
        "lagP50Ms": percentile(&sorted, 50.0),
        "lagP99Ms": percentile(&sorted, 99.0),
        "lagMaxMs": sorted.last().copied().unwrap_or(0.0),
    })
}

async fn stats(State(state): State<Shared>) -> Json<Value> {
    let sinks = state.lock().expect("sink lock");
    let rec = &sinks.callback;
    let mut sorted = rec.latencies_ms.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Json(json!({
        "fast": burst_stats(&sinks.fast),
        "slow": burst_stats(&sinks.slow),
        "settled": rec.refs.len(),
        "duplicates": rec.duplicates,
        "malformed": rec.malformed,
        "firstAtMs": rec.first_at_ms,
        "lastAtMs": rec.last_at_ms,
        "p50Ms": percentile(&sorted, 50.0),
        "p90Ms": percentile(&sorted, 90.0),
        "p99Ms": percentile(&sorted, 99.0),
        "maxMs": sorted.last().copied().unwrap_or(0.0),
    }))
}

async fn reset(State(state): State<Shared>) -> &'static str {
    *state.lock().expect("sink lock") = Sinks::default();
    "reset"
}

#[tokio::main]
async fn main() {
    let addr = std::env::var("SINK_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/callback", post(callback))
        .route("/fast", post(fast))
        .route("/slow", post(slow))
        .route("/stats", get(stats))
        .route("/reset", post(reset))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind SINK_ADDR");
    println!(
        "listening on {}",
        listener.local_addr().expect("local addr")
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("serve");
}
