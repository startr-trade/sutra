//! Shared measurement for the load tests: whether the engine KEEPS UP with an offered rate, and
//! the one-line `PERF` record the A/B script (`scripts/perf-ab.sh`) parses.
//!
//! A load test does not assert an absolute latency: an absolute bar passes on a fast workstation
//! and fails on a loaded CI runner for the same code. It asserts instead that the engine delivers
//! at (nearly) the rate it was offered, and that the lag does not grow over the run. A machine
//! that is slower but keeps up passes; an engine that falls behind fails on any machine.

use std::time::{Duration, Instant};

/// One load run: per unit of work, the instant it became due and how late it was delivered,
/// both in send order.
#[derive(Debug, Clone, Default)]
pub struct LoadSample {
    /// When each unit became due: its send instant, plus any delay the flow adds on purpose
    /// (a timer the instance parks on, for one).
    pub sent_at: Vec<Instant>,
    /// How long after its due instant each unit arrived.
    pub lag: Vec<Duration>,
}

impl LoadSample {
    /// A sample from `(due, lag)` pairs in any order; they are put in send order.
    pub fn from_pairs(mut pairs: Vec<(Instant, Duration)>) -> LoadSample {
        pairs.sort_by_key(|(due, _)| *due);
        let (sent_at, lag) = pairs.into_iter().unzip();
        LoadSample { sent_at, lag }
    }
}

/// Every figure [`keeps_up`] decided on, so a failure message shows them all.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KeepsUpVerdict {
    pub n: usize,
    pub offered_per_s: f64,
    pub delivered_per_s: f64,
    /// p90 lag of the first third of the run, by send order.
    pub early_p90: Duration,
    /// p90 lag of the last third of the run, by send order.
    pub late_p90: Duration,
}

/// The share of the offered rate the engine must deliver.
const MIN_DELIVERED_SHARE: f64 = 0.9;
/// How much the late lag may exceed twice the early lag before it counts as growing.
const LAG_GROWTH_ALLOWANCE: Duration = Duration::from_millis(500);

impl KeepsUpVerdict {
    pub fn rate_ok(&self) -> bool {
        self.delivered_per_s >= MIN_DELIVERED_SHARE * self.offered_per_s
    }

    pub fn lag_ok(&self) -> bool {
        self.late_p90 <= self.early_p90 * 2 + LAG_GROWTH_ALLOWANCE
    }

    pub fn passed(&self) -> bool {
        self.n > 0 && self.rate_ok() && self.lag_ok()
    }
}

impl std::fmt::Display for KeepsUpVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} offered={:.1}/s delivered={:.1}/s (needs >= {:.1}/s: {}) early_p90={:?} \
             late_p90={:?} (needs <= {:?}: {})",
            self.n,
            self.offered_per_s,
            self.delivered_per_s,
            MIN_DELIVERED_SHARE * self.offered_per_s,
            if self.rate_ok() { "ok" } else { "FAIL" },
            self.early_p90,
            self.late_p90,
            self.early_p90 * 2 + LAG_GROWTH_ALLOWANCE,
            if self.lag_ok() { "ok" } else { "FAIL" },
        )
    }
}

fn p90(lags: &[Duration]) -> Duration {
    if lags.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = lags.to_vec();
    sorted.sort();
    let idx = ((sorted.len() as f64 - 1.0) * 0.9).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Did the engine keep up with `offered_per_s`? The delivered rate is the arrivals' own rate,
/// `(n − 1) / (last arrival − first arrival)`, the same measure the offered rate is taken in, so
/// a constant lag (a slow machine that keeps pace) does not count against it and a growing one
/// does. It must reach 0.9 × the offered rate, and the late third's p90 lag must stay within
/// twice the early third's plus 500 ms.
pub fn keeps_up(sample: &LoadSample, offered_per_s: f64) -> KeepsUpVerdict {
    let n = sample.lag.len().min(sample.sent_at.len());
    let arrivals: Vec<Instant> = sample
        .sent_at
        .iter()
        .zip(&sample.lag)
        .map(|(due, lag)| *due + *lag)
        .collect();
    let delivered_per_s = match (arrivals.iter().min(), arrivals.iter().max()) {
        (Some(first), Some(last)) if n > 1 => {
            let span = last.saturating_duration_since(*first).as_secs_f64();
            if span > 0.0 {
                (n - 1) as f64 / span
            } else {
                f64::INFINITY
            }
        }
        (Some(_), Some(_)) => f64::INFINITY,
        _ => 0.0,
    };
    let third = (n / 3).max(1).min(n);
    KeepsUpVerdict {
        n,
        offered_per_s,
        delivered_per_s,
        early_p90: p90(&sample.lag[..third]),
        late_p90: p90(&sample.lag[n - third..n]),
    }
}

/// Print one `PERF {"test":…,"metric":…,"value":…}` line on stdout. A metric whose name ends
/// in `_ms` is lower-is-better; every other metric is higher-is-better.
pub fn perf_line(test: &str, metric: &str, value: f64) {
    println!(
        "PERF {}",
        serde_json::json!({ "test": test, "metric": metric, "value": value })
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n` units offered at `rate` per second, each with the lag `lag_of(i)`.
    fn sample(n: usize, rate: f64, lag_of: impl Fn(usize) -> Duration) -> LoadSample {
        let start = Instant::now();
        LoadSample::from_pairs(
            (0..n)
                .map(|i| (start + Duration::from_secs_f64(i as f64 / rate), lag_of(i)))
                .collect(),
        )
    }

    /// A machine slow enough that every unit is 800 ms late, but no later at the end than at
    /// the start: it keeps up.
    #[test]
    fn keeps_up_passes_a_slow_but_steady_run() {
        let verdict = keeps_up(
            &sample(1000, 100.0, |i| {
                Duration::from_millis(750 + (i % 7) as u64 * 10)
            }),
            100.0,
        );
        assert!(verdict.passed(), "{verdict}");
    }

    /// The lag grows for as long as the load lasts: the engine is falling behind.
    #[test]
    fn keeps_up_fails_a_growing_lag() {
        let verdict = keeps_up(
            &sample(1000, 100.0, |i| Duration::from_millis(i as u64 * 5)),
            100.0,
        );
        assert!(!verdict.lag_ok(), "{verdict}");
        assert!(!verdict.rate_ok(), "{verdict}");
        assert!(!verdict.passed());
    }

    #[test]
    fn keeps_up_fails_an_empty_run() {
        assert!(!keeps_up(&LoadSample::default(), 10.0).passed());
    }
}
