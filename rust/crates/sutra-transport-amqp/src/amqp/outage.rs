//! The outage window: when a broker connection was lost and when it came back, logged with both
//! timestamps and the length so an operator (or a test) can read how long a broker was away.

use std::time::Instant;

/// An outage in progress, started at the first failed broker round trip after the last success.
#[derive(Debug, Clone)]
pub(crate) struct OutageWindow {
    started_at: time::OffsetDateTime,
    started: Instant,
}

/// A finished outage, as logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndedOutage {
    /// RFC 3339 UTC time of the first failure.
    pub started: String,
    /// RFC 3339 UTC time of the first success after it.
    pub ended: String,
    /// The length in milliseconds.
    pub millis: u128,
}

impl OutageWindow {
    /// An outage starting now.
    pub(crate) fn begin() -> OutageWindow {
        OutageWindow {
            started_at: time::OffsetDateTime::now_utc(),
            started: Instant::now(),
        }
    }

    /// When it started, as RFC 3339 UTC.
    pub(crate) fn started(&self) -> String {
        rfc3339(self.started_at)
    }

    /// End it now.
    pub(crate) fn end(&self) -> EndedOutage {
        EndedOutage {
            started: self.started(),
            ended: rfc3339(time::OffsetDateTime::now_utc()),
            millis: self.started.elapsed().as_millis(),
        }
    }
}

fn rfc3339(at: time::OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ended_outage_carries_both_timestamps_in_order() {
        let window = OutageWindow::begin();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let ended = window.end();
        assert_eq!(ended.started, window.started());
        assert!(ended.millis >= 5, "{ended:?}");
        let started = time::OffsetDateTime::parse(
            &ended.started,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let finished = time::OffsetDateTime::parse(
            &ended.ended,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        assert!(finished >= started);
    }
}
