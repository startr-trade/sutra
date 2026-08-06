//! Injectable evaluation clock, threaded through
//! `DmnRulesetValidator::with_clock(decision, clock)`. Tests inject [`FixedClock`] for
//! deterministic temporal rules; `DmnRulesetValidator::new` uses [`SystemClock`].

use time::OffsetDateTime;

pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

/// Wall-clock UTC time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// A frozen instant.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}
