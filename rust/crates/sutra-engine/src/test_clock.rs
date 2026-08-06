//! The engine-level half of the time-skipping test seam (P1-7 time-skipping test runtime).
//!
//! [`sutra_executor::TestClock`] (re-exported here as [`TestClock`]) is the virtual-clock handle
//! itself — see its docs for the identity semantics and the guarantee that it is reachable ONLY
//! from Rust code holding one, never from a `sutra.*` config key or `SUTRA_*` env var. Installed
//! on [`crate::config::EngineConfig::now_override`] before [`crate::serve`], it becomes the "now"
//! every temporal read in that boot uses: the executor's `now_supplier` (timer park due-ats,
//! `<q:retry>` backoff), the timer poller's per-tick claim instant
//! ([`crate::timer::TimerPollerConfig::now_override`]), and the timer-`<startEvent>` schedule
//! arming instant.
//!
//! [`fast_forward_until`] is the reusable DX surface: an app author's own Testcontainers IT that
//! boots [`crate::serve`] in-process — every docker-gated suite in this crate's own `tests/all/`
//! already does — can drive a long BPMN timer, a repeating `<timeCycle>` schedule, or a
//! `<q:retry>` backoff to completion in real milliseconds the same way this crate's own
//! `tests/all/time_skipping_it.rs` does.
//!
//! # Usage
//!
//! ```text
//! let clock = sutra_engine::TestClock::starting_now();
//! let engine = sutra_engine::serve(sutra_engine::EngineConfig {
//!     now_override: Some(clock.clone()),
//!     ..config
//! }).await?;
//! // park a PT24H timer, then:
//! let completed = sutra_engine::fast_forward_until(
//!     &pool,
//!     &clock,
//!     std::time::Duration::from_secs(10),
//!     || async { live_instance_count(&pool).await == 0 },
//! )
//! .await;
//! assert!(completed);
//! ```

use std::future::Future;

use sqlx::{PgPool, Row};
use time::OffsetDateTime;

pub use sutra_executor::TestClock;

/// The earliest still-armed due instant across BOTH temporal tables the timer poller claims:
/// `waiting_event` TIMER rows (catch timers, channel-call timeout boundaries, AND `<q:retry>`
/// backoff parks — all three ride the same wait-state row, [`crate::timer`]'s module docs) and
/// `timer_schedule` rows (timer `<startEvent>` schedules, single-shot or cyclic). `None` when
/// nothing is armed.
///
/// Deliberately a raw pool-wide query, not deployment-scoped: a fast-forward drives the whole
/// engine under test, not one deployment — the same posture every docker-gated conformance suite
/// in this crate already takes for its own DB-probe assertions (a BYPASSRLS fixture role; RLS
/// enforcement itself is `rls_bypass_it`'s job, not this helper's).
async fn earliest_due(pool: &PgPool) -> Option<OffsetDateTime> {
    let row = sqlx::query(
        "SELECT MIN(due) AS due FROM ( \
           SELECT timer_due_at AS due FROM waiting_event \
             WHERE kind = 'TIMER' AND status = 'WAITING' \
           UNION ALL \
           SELECT next_due_at AS due FROM timer_schedule WHERE status = 'SCHEDULED' \
         ) armed",
    )
    .fetch_one(pool)
    .await
    .expect("earliest-due probe query");
    row.try_get::<Option<OffsetDateTime>, _>("due")
        .unwrap_or(None)
}

/// Fast-forward: repeat {advance `clock` to the earliest armed due instant (or by a small idle
/// step when nothing is armed yet — covers the window between a park request landing and its row
/// committing), give the timer poller's next REAL tick a moment to claim + fire it, re-check
/// `condition`} until `condition` holds or `timeout` (real wall-clock) elapses. Returns whether
/// `condition` was observed true.
///
/// The poller's tick interval is NOT driven manually here — the caller sets it near-zero for the
/// test boot instead (`SUTRA_TIMER_TICK_MS`, read once at [`crate::serve`]; every suite in
/// `tests/all/` that exercises timers already does this), which is simpler than reaching into
/// the poller task and keeps this helper engine-agnostic: it only ever touches the clock and the
/// database, never the poller's internals.
pub async fn fast_forward_until<F, Fut>(
    pool: &PgPool,
    clock: &TestClock,
    timeout: std::time::Duration,
    mut condition: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let idle_step = time::Duration::milliseconds(200);
    let real_poll = std::time::Duration::from_millis(20);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        match earliest_due(pool).await {
            // Jump straight to the due instant — the poller's next real tick claims it.
            Some(due) if due > clock.now() => clock.set(due),
            // Already due (armed but not yet claimed this tick) — just give the poller a moment.
            Some(_) => {}
            // Nothing armed yet — nudge forward and retry; covers the park-request-in-flight
            // window before its row has committed.
            None => clock.advance(idle_step),
        }
        tokio::time::sleep(real_poll).await;
    }
}
