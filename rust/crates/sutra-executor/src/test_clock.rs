//! `TestClock` — a manually-advanced virtual clock: the seam behind the time-skipping test
//! runtime (P1-7). Install it as [`crate::executor::Builder::with_now_supplier`]'s supplier (via
//! [`TestClock::rfc3339`]) and every due-at computation that reads "now" through that seam — a
//! timer catch/boundary park, a `<q:retry>` backoff park — reads the SAME virtual instant a test
//! advances explicitly. No real sleeping, however long the modelled duration.
//!
//! Cheap to clone: every clone is a handle onto the SAME instant (an `Arc<AtomicI64>` of
//! nanoseconds-since-epoch), so every reader and the test driving the clock forward always
//! agree. TEST-ONLY by convention, not by compiler gate: it is a plain, always-compiled public
//! type — exactly like [`crate::executor::Builder::with_now_supplier`] itself, which has taken
//! an arbitrary closure since before this type existed — reachable only by code that holds one.
//! There is no config key, env var, or CLI flag that installs it, so an operator has no way to
//! reach it in a deployed engine; wiring it into a boot is always an explicit Rust-level choice
//! at the call site (see `sutra_engine::EngineConfig::now_override`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use time::{Duration, OffsetDateTime};

/// A manually-advanced virtual clock. See the module docs.
#[derive(Debug, Clone)]
pub struct TestClock(Arc<AtomicI64>);

impl TestClock {
    /// A clock pinned at `start`.
    pub fn new(start: OffsetDateTime) -> TestClock {
        TestClock(Arc::new(AtomicI64::new(nanos_of(start))))
    }

    /// A clock starting at the REAL current instant — the common case: only the FAST-FORWARD is
    /// virtual, so any absolute-time assertion elsewhere in the test (log timestamps, audit
    /// rows, …) stays sane relative to wall clock.
    pub fn starting_now() -> TestClock {
        TestClock::new(OffsetDateTime::now_utc())
    }

    /// The current virtual instant.
    pub fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(self.0.load(Ordering::SeqCst)))
            .expect("TestClock only ever stores an instant `new`/`set` accepted")
    }

    /// The current virtual instant, RFC 3339 — the exact string shape
    /// [`crate::executor::Builder::with_now_supplier`] expects.
    pub fn rfc3339(&self) -> String {
        self.now()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    }

    /// Move the clock to an explicit instant — forward OR backward (an escape hatch; the real
    /// clock can never rewind, so lean on [`Self::advance`] unless a test specifically needs to
    /// pin an exact due-at).
    pub fn set(&self, at: OffsetDateTime) {
        self.0.store(nanos_of(at), Ordering::SeqCst);
    }

    /// Advance the clock forward by `delta`. Panics on a negative delta —
    /// [`Self::set`] is the explicit way to rewind; `advance` never moves backward by surprise.
    pub fn advance(&self, delta: Duration) {
        assert!(
            !delta.is_negative(),
            "TestClock::advance never moves the clock backward — use TestClock::set"
        );
        self.set(self.now() + delta);
    }
}

impl PartialEq for TestClock {
    /// Handle identity, not instant equality (the instant changes out from under any holder as
    /// soon as another clone advances it) — two clones of the SAME clock are equal; two
    /// independently-[`TestClock::new`]ed clocks are never equal, even at the same instant.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for TestClock {}

fn nanos_of(at: OffsetDateTime) -> i64 {
    i64::try_from(at.unix_timestamp_nanos())
        .expect("TestClock instants stay inside the i64 nanosecond range until the year 2262")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_moves_forward_by_exactly_delta() {
        let clock = TestClock::new(time::macros::datetime!(2026-01-01 00:00:00 UTC));
        clock.advance(Duration::hours(24));
        assert_eq!(
            clock.now(),
            time::macros::datetime!(2026-01-02 00:00:00 UTC)
        );
    }

    #[test]
    #[should_panic(expected = "never moves the clock backward")]
    fn advance_refuses_a_negative_delta() {
        TestClock::starting_now().advance(Duration::seconds(-1));
    }

    #[test]
    fn set_can_rewind() {
        let clock = TestClock::new(time::macros::datetime!(2026-01-02 00:00:00 UTC));
        clock.set(time::macros::datetime!(2020-01-01 00:00:00 UTC));
        assert_eq!(
            clock.now(),
            time::macros::datetime!(2020-01-01 00:00:00 UTC)
        );
    }

    #[test]
    fn clones_share_the_same_instant() {
        let a = TestClock::starting_now();
        let b = a.clone();
        a.advance(Duration::minutes(5));
        assert_eq!(a.now(), b.now());
        assert_eq!(a, b);
        assert_ne!(a, TestClock::starting_now());
    }

    #[test]
    fn rfc3339_round_trips_through_offsetdatetime_parse() {
        let clock = TestClock::new(time::macros::datetime!(2026-03-01 09:00:00 UTC));
        let parsed = OffsetDateTime::parse(
            &clock.rfc3339(),
            &time::format_description::well_known::Rfc3339,
        )
        .expect("valid RFC 3339");
        assert_eq!(parsed, clock.now());
    }
}
