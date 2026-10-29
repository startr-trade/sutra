//! sutra-load-sink — the callback sink for the `relay` load profile.
//!
//! `POST /callback` records one settle: the JSON body's `ref` (for duplicate detection) and its
//! `sentAt` (epoch milliseconds stamped by k6 when the intake request left), so the settle latency
//! is `receivedAt - sentAt`. `GET /stats` returns the counters and latency percentiles as JSON;
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

type Shared = Arc<Mutex<Recorded>>;

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

async fn callback(State(state): State<Shared>, body: Bytes) -> &'static str {
    let at = now_ms();
    let parsed: Option<Value> = serde_json::from_slice(&body).ok();
    let mut rec = state.lock().expect("sink lock");
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

async fn stats(State(state): State<Shared>) -> Json<Value> {
    let rec = state.lock().expect("sink lock");
    let mut sorted = rec.latencies_ms.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Json(json!({
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
    *state.lock().expect("sink lock") = Recorded::default();
    "reset"
}

#[tokio::main]
async fn main() {
    let addr = std::env::var("SINK_ADDR").unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/callback", post(callback))
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
