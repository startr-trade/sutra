//! The scheduling contract of a `<bpmn:timerEventDefinition>` — the three ISO-8601 forms this
//! engine schedules, and the arithmetic that turns each into an absolute fire instant.
//!
//! BPMN gives a timer event definition exactly one of three mutually-exclusive children, and
//! this module is the single place that knows what each one MEANS to the scheduler:
//!
//! - `<bpmn:timeDuration>` — RELATIVE. Fire once, `duration` after the timer is ARMED. On an
//!   intermediate catch / boundary the arming moment is the park; on a start event it is the
//!   moment the deployment became ACTIVE.
//! - `<bpmn:timeDate>` — ABSOLUTE. Fire once at the named instant. A date in the PAST is
//!   deliberately LEGAL: it is already due, so it fires on the first poller tick that sees it
//!   (a deploy-time reject would make an archive's validity depend on the wall clock, which
//!   would break re-deploys and rollbacks of a perfectly good archive).
//! - `<bpmn:timeCycle>` — REPEATING, and only on a START event (a mid-flow token cannot park at
//!   a node that fires more than once). ISO-8601 repeating-interval syntax only: `R/PT1H`
//!   (unbounded), `R5/PT1H` (five fires), `R/2026-03-01T00:00:00Z/PT1H` (anchored start).
//!
//! Two forms fail CLOSED and stay that way — [`TimerSpecRejection::UnsupportedForm`]:
//! **cron-syntax** cycles (`0 0 * * *`) are a vendor extension, not BPMN, and are deliberately
//! deferred; **calendar-length** components (`P1Y`, `P1M` before the `T`) have no exact length,
//! so a duration-only scheduler cannot honour them (see [`crate::duration`]).
//!
//! Everything here is pure: the caller supplies `now`, so due-at computation is deterministic
//! under an injected clock and the same arithmetic serves the executor (catch/boundary park) and
//! the deployment-activation reconciler (start schedules).

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::duration::parse_iso8601_duration;

/// Why a timer specification was refused — the distinction the loader maps onto two DIFFERENT
/// stable diagnostics, so an operator can tell "you wrote it wrong" from "we do not do that".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerSpecRejection {
    /// A recognisable scheduling form this engine deliberately does not execute: a cron-syntax
    /// `timeCycle`, or a calendar-length (`Y`/`M`-before-`T`) duration. Fail closed rather than
    /// approximate it — the model would look scheduled and silently drift.
    UnsupportedForm(String),
    /// The right form, written wrong (unparseable instant, bad repeat count, missing interval).
    Malformed(String),
}

impl TimerSpecRejection {
    /// The human-readable reason, whichever kind of rejection this is.
    pub fn reason(&self) -> &str {
        match self {
            TimerSpecRejection::UnsupportedForm(r) | TimerSpecRejection::Malformed(r) => r,
        }
    }

    /// True when the form is deliberately out of contract (cron / calendar) rather than a typo.
    pub fn is_unsupported_form(&self) -> bool {
        matches!(self, TimerSpecRejection::UnsupportedForm(_))
    }
}

impl std::fmt::Display for TimerSpecRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

/// A parsed `<bpmn:timeCycle>` — the ISO-8601 repeating-interval triple.
///
/// `R<n>/<interval>` and `R<n>/<start>/<interval>`; an empty `<n>` (`R/…`) means UNBOUNDED. The
/// interval is kept as its authored ISO-8601 text (re-parsed at each occurrence) so a durable
/// schedule row stores exactly what the model said, never a lossy derived number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerCycleSpec {
    /// How many times the cycle fires in total; `None` = unbounded (`R/…`).
    pub repeats: Option<u32>,
    /// The anchored first fire instant (RFC 3339, UTC-normalised), when the cycle declares one.
    /// `None` ⇒ the first fire is one interval after the schedule is armed.
    pub start_at: Option<String>,
    /// The ISO-8601 duration between occurrences, verbatim as authored.
    pub interval: String,
}

/// The scheduling contract of ONE `<bpmn:timerEventDefinition>`.
///
/// Carried on [`crate::model::Node::TimerCatchEvent`], on a timer
/// [`crate::model::Node::BoundaryEvent`] and on a timer-triggered
/// [`crate::model::Node::StartEvent`]. The loader has already validated the payload, so every
/// variant here is known-parseable — the runtime re-parses only to do arithmetic, never to
/// re-decide legality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerDefinition {
    /// `<bpmn:timeDuration>` — fire once, this long after the timer is armed.
    Duration(String),
    /// `<bpmn:timeDate>` — fire once at this absolute instant (RFC 3339, as authored).
    Date(String),
    /// `<bpmn:timeCycle>` — fire repeatedly on an ISO-8601 repeating interval (START events only).
    Cycle(TimerCycleSpec),
}

/// The `kind` discriminator persisted on a durable schedule row: `DURATION`.
pub const TIMER_KIND_DURATION: &str = "DURATION";
/// The `kind` discriminator persisted on a durable schedule row: `DATE`.
pub const TIMER_KIND_DATE: &str = "DATE";
/// The `kind` discriminator persisted on a durable schedule row: `CYCLE`.
pub const TIMER_KIND_CYCLE: &str = "CYCLE";

impl TimerDefinition {
    /// The persisted `kind` discriminator — the stable string a durable schedule row stores
    /// beside [`Self::spec_text`] so the row round-trips through the database without a
    /// bespoke encoding.
    pub fn kind_str(&self) -> &'static str {
        match self {
            TimerDefinition::Duration(_) => TIMER_KIND_DURATION,
            TimerDefinition::Date(_) => TIMER_KIND_DATE,
            TimerDefinition::Cycle(_) => TIMER_KIND_CYCLE,
        }
    }

    /// The authored specification text — what the BPMN element actually said. Round-trips through
    /// [`Self::from_persisted`].
    pub fn spec_text(&self) -> String {
        match self {
            TimerDefinition::Duration(d) | TimerDefinition::Date(d) => d.clone(),
            TimerDefinition::Cycle(c) => {
                let mut out = String::from("R");
                if let Some(n) = c.repeats {
                    out.push_str(&n.to_string());
                }
                out.push('/');
                if let Some(start) = &c.start_at {
                    out.push_str(start);
                    out.push('/');
                }
                out.push_str(&c.interval);
                out
            }
        }
    }

    /// Rebuild a definition from its persisted `(kind, spec)` pair (the durable schedule row).
    /// An unknown kind is [`TimerSpecRejection::Malformed`] — a row written by a newer engine
    /// must never be silently reinterpreted as something else.
    pub fn from_persisted(kind: &str, spec: &str) -> Result<TimerDefinition, TimerSpecRejection> {
        match kind {
            TIMER_KIND_DURATION => {
                parse_timer_duration(spec)?;
                Ok(TimerDefinition::Duration(spec.to_string()))
            }
            TIMER_KIND_DATE => {
                parse_timer_instant(spec)?;
                Ok(TimerDefinition::Date(spec.to_string()))
            }
            TIMER_KIND_CYCLE => Ok(TimerDefinition::Cycle(parse_timer_cycle(spec)?)),
            other => Err(TimerSpecRejection::Malformed(format!(
                "unknown persisted timer kind '{other}' (expected one of \
                 {TIMER_KIND_DURATION}/{TIMER_KIND_DATE}/{TIMER_KIND_CYCLE})"
            ))),
        }
    }

    /// The number of times this timer fires in total; `None` = unbounded. A duration/date timer
    /// fires exactly ONCE — that is what makes a start schedule resolve after its single fire.
    pub fn total_fires(&self) -> Option<u32> {
        match self {
            TimerDefinition::Duration(_) | TimerDefinition::Date(_) => Some(1),
            TimerDefinition::Cycle(c) => c.repeats,
        }
    }

    /// The FIRST instant this timer becomes due, given the moment it is ARMED (`now`).
    ///
    /// - duration ⇒ `now + duration`;
    /// - date ⇒ the instant itself, which MAY already be in the past (already due — it fires on
    ///   the next tick, by design);
    /// - cycle ⇒ its anchored `start_at` when it declares one (again, possibly already past),
    ///   else `now + interval` (the first occurrence of an unanchored cycle is one full interval
    ///   away, never instantly at arming).
    pub fn first_due_at(&self, now: OffsetDateTime) -> Result<OffsetDateTime, TimerSpecRejection> {
        match self {
            TimerDefinition::Duration(d) => Ok(now + parse_timer_duration(d)?),
            TimerDefinition::Date(d) => parse_timer_instant(d),
            TimerDefinition::Cycle(c) => match &c.start_at {
                Some(start) => parse_timer_instant(start),
                None => Ok(now + parse_timer_duration(&c.interval)?),
            },
        }
    }
}

/// The occurrence a fired CYCLE schedule advances to, and the repeats left after it.
///
/// `Exhausted` is the terminal answer — the schedule row RESOLVES and never fires again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerCycleAdvance {
    /// Fire again at this instant; `remaining` is what is left AFTER that fire (`None` =
    /// unbounded).
    Next {
        due_at: OffsetDateTime,
        remaining: Option<u32>,
    },
    /// The `R<n>` budget is spent — resolve the schedule.
    Exhausted,
}

/// Advance a cycle past the occurrence that just fired.
///
/// `fired_due` is the due-at of the occurrence that fired, `remaining` the repeat budget BEFORE
/// it (`None` = unbounded), and `now` the instant of the fire.
///
/// Catch-up rule (the one an operator has to be able to predict): each claim fires EXACTLY ONCE.
/// Occurrences whose slot elapsed while the engine was down or the leader was away are SKIPPED
/// rather than replayed as a burst — but each skipped slot still spends one repeat, so `R3/PT1H`
/// can never fire more than three times and never outlives its third slot. An unbounded cycle
/// simply walks the grid forward to the first future instant.
pub fn advance_cycle(
    spec: &TimerCycleSpec,
    fired_due: OffsetDateTime,
    remaining: Option<u32>,
    now: OffsetDateTime,
) -> Result<TimerCycleAdvance, TimerSpecRejection> {
    let interval = parse_timer_duration(&spec.interval)?;
    // A zero-length interval would spin the grid walk forever; the cycle degenerates to its one
    // fire (the loader accepts `PT0S` as a duration, so guard rather than trust).
    if interval.is_zero() {
        return Ok(TimerCycleAdvance::Exhausted);
    }
    // The fire that just happened spends one repeat.
    let mut left = match remaining {
        Some(0) => return Ok(TimerCycleAdvance::Exhausted),
        Some(n) => Some(n - 1),
        None => None,
    };
    if left == Some(0) {
        return Ok(TimerCycleAdvance::Exhausted);
    }
    let mut next = fired_due + interval;
    while next <= now {
        match left {
            Some(0) => return Ok(TimerCycleAdvance::Exhausted),
            Some(n) => left = Some(n - 1),
            None => {}
        }
        if left == Some(0) {
            return Ok(TimerCycleAdvance::Exhausted);
        }
        next += interval;
    }
    Ok(TimerCycleAdvance::Next {
        due_at: next,
        remaining: left,
    })
}

/// Parse a `<bpmn:timeDuration>` for timer use: calendar components are reported as a
/// deliberately-[`TimerSpecRejection::UnsupportedForm`] rather than a typo, everything else
/// delegates to [`parse_iso8601_duration`].
pub fn parse_timer_duration(input: &str) -> Result<std::time::Duration, TimerSpecRejection> {
    if let Some(unit) = calendar_component(input) {
        return Err(TimerSpecRejection::UnsupportedForm(format!(
            "'{input}' uses the calendar component '{unit}' — years/months have no exact length, \
             so a duration timer cannot schedule them; use weeks/days/hours/minutes/seconds"
        )));
    }
    parse_iso8601_duration(input).map_err(TimerSpecRejection::Malformed)
}

/// The calendar component (`Y`, or `M` BEFORE the `T` separator) an ISO-8601 duration declares,
/// if any. `PT1M` is minutes and is fine; `P1M` is months and is not.
fn calendar_component(input: &str) -> Option<char> {
    let rest = input
        .trim()
        .strip_prefix('P')
        .or_else(|| input.trim().strip_prefix('p'))?;
    let date_part = match rest.find(['T', 't']) {
        Some(i) => &rest[..i],
        None => rest,
    };
    date_part
        .chars()
        .find(|c| matches!(c.to_ascii_uppercase(), 'Y' | 'M'))
        .map(|c| c.to_ascii_uppercase())
}

/// Parse a `<bpmn:timeDate>` — an ISO-8601 datetime with an explicit zone (`Z` or a `±HH:MM`
/// offset). A zone-less local datetime is REFUSED: "which clock?" is not a question a durable
/// schedule may guess at.
pub fn parse_timer_instant(input: &str) -> Result<OffsetDateTime, TimerSpecRejection> {
    let s = input.trim();
    if s.is_empty() {
        return Err(TimerSpecRejection::Malformed(
            "a <timeDate> timer declares no instant".to_owned(),
        ));
    }
    OffsetDateTime::parse(s, &Rfc3339).map_err(|e| {
        TimerSpecRejection::Malformed(format!(
            "'{input}' is not an ISO-8601 datetime with an explicit zone \
             (e.g. 2026-03-01T09:30:00Z or 2026-03-01T09:30:00+05:30): {e}"
        ))
    })
}

/// Format an instant back to the RFC 3339 text the durable rows and the executor's due-at
/// strings carry, NORMALISED TO UTC.
///
/// The normalisation matters: `2026-03-01T15:00:00+05:30` and `2026-03-01T09:30:00Z` are the same
/// instant, and every due-at that leaves this module renders the second form. Comparisons
/// downstream are then string-safe as well as instant-safe, and two authors writing the same
/// deadline in different zones produce the same row. Infallible in practice (a parsed instant
/// always formats); the fallback keeps the signature total.
pub fn format_instant(at: OffsetDateTime) -> String {
    at.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Parse a `<bpmn:timeCycle>` as an ISO-8601 repeating interval.
///
/// Accepted: `R/<interval>`, `R<n>/<interval>`, `R/<start>/<interval>`, `R<n>/<start>/<interval>`.
/// Anything not starting with `R` is treated as a vendor cron expression and refused as an
/// [`TimerSpecRejection::UnsupportedForm`] — cron-syntax scheduling is deliberately deferred and
/// must never be silently ignored.
pub fn parse_timer_cycle(input: &str) -> Result<TimerCycleSpec, TimerSpecRejection> {
    let s = input.trim();
    if s.is_empty() {
        return Err(TimerSpecRejection::Malformed(
            "a <timeCycle> timer declares no cycle".to_owned(),
        ));
    }
    let Some(rest) = s.strip_prefix('R').or_else(|| s.strip_prefix('r')) else {
        return Err(TimerSpecRejection::UnsupportedForm(format!(
            "'{input}' is not an ISO-8601 repeating interval (it must start with 'R', as in \
             R/PT1H, R5/PT1H or R/2026-03-01T00:00:00Z/PT1H); cron-syntax schedules are not \
             supported by this engine"
        )));
    };
    let mut parts = rest.split('/');
    let count = parts.next().unwrap_or_default();
    let repeats = if count.is_empty() {
        None
    } else {
        let n: u32 = count.parse().map_err(|_| {
            TimerSpecRejection::Malformed(format!(
                "'{input}' has an unparseable repeat count 'R{count}'; use R (unbounded) or \
                 R<positive integer>"
            ))
        })?;
        if n == 0 {
            return Err(TimerSpecRejection::Malformed(format!(
                "'{input}' repeats zero times; use R (unbounded) or R<positive integer>, or drop \
                 the timer"
            )));
        }
        Some(n)
    };
    let remainder: Vec<&str> = parts.collect();
    let (start_at, interval_text) = match remainder.as_slice() {
        [interval] => (None, *interval),
        [start, interval] => (Some(parse_timer_instant(start)?), *interval),
        [] => {
            return Err(TimerSpecRejection::Malformed(format!(
                "'{input}' declares no interval; an ISO-8601 repeating interval is \
                 R[n]/[<start>/]<duration>"
            )))
        }
        _ => {
            return Err(TimerSpecRejection::Malformed(format!(
                "'{input}' has more than three '/'-separated parts; an ISO-8601 repeating \
                 interval is R[n]/[<start>/]<duration>"
            )))
        }
    };
    // Validate the interval eagerly so a bad cycle is a LOAD error, never a runtime surprise.
    let d = parse_timer_duration(interval_text)?;
    if d.is_zero() {
        return Err(TimerSpecRejection::Malformed(format!(
            "'{input}' has a zero-length interval; a repeating timer needs a positive interval"
        )));
    }
    Ok(TimerCycleSpec {
        repeats,
        start_at: start_at.map(format_instant),
        interval: interval_text.trim().to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn at(s: &str) -> OffsetDateTime {
        parse_timer_instant(s).unwrap()
    }

    #[test]
    fn instants_need_an_explicit_zone() {
        assert_eq!(
            at("2026-03-01T09:30:00Z"),
            datetime!(2026-03-01 09:30:00 UTC)
        );
        // An offset form normalises to the same instant.
        assert_eq!(
            at("2026-03-01T15:00:00+05:30"),
            datetime!(2026-03-01 09:30:00 UTC)
        );
        for bad in [
            "",
            "   ",
            "2026-03-01",
            "2026-03-01T09:30:00",
            "tomorrow",
            "PT1H",
        ] {
            assert!(
                parse_timer_instant(bad).is_err(),
                "'{bad}' must be rejected"
            );
        }
    }

    #[test]
    fn a_past_date_is_legal_and_already_due() {
        let timer = TimerDefinition::Date("2020-01-01T00:00:00Z".to_owned());
        let now = datetime!(2026-08-05 12:00:00 UTC);
        let due = timer.first_due_at(now).unwrap();
        assert!(due < now, "a past date is due immediately: {due}");
    }

    #[test]
    fn duration_due_at_is_now_plus_the_duration() {
        let timer = TimerDefinition::Duration("PT90S".to_owned());
        let now = datetime!(2026-08-05 12:00:00 UTC);
        assert_eq!(
            timer.first_due_at(now).unwrap(),
            datetime!(2026-08-05 12:01:30 UTC)
        );
    }

    #[test]
    fn calendar_durations_are_an_unsupported_form_not_a_typo() {
        for calendar in ["P1Y", "P1M", "P1YT1S", "P2M"] {
            let e = parse_timer_duration(calendar).unwrap_err();
            assert!(e.is_unsupported_form(), "{calendar}: {e:?}");
            assert!(e.reason().contains("calendar"), "{calendar}: {e}");
        }
        // Minutes are time-scoped and perfectly fine.
        assert_eq!(
            parse_timer_duration("PT1M").unwrap(),
            std::time::Duration::from_secs(60)
        );
        // A genuine typo is Malformed, not UnsupportedForm.
        assert!(!parse_timer_duration("PT5X")
            .unwrap_err()
            .is_unsupported_form());
    }

    #[test]
    fn cycles_parse_every_iso_repeating_form() {
        let unbounded = parse_timer_cycle("R/PT1H").unwrap();
        assert_eq!(unbounded.repeats, None);
        assert_eq!(unbounded.start_at, None);
        assert_eq!(unbounded.interval, "PT1H");

        let bounded = parse_timer_cycle("R5/PT30S").unwrap();
        assert_eq!(bounded.repeats, Some(5));

        let anchored = parse_timer_cycle("R3/2026-03-01T00:00:00Z/P1D").unwrap();
        assert_eq!(anchored.repeats, Some(3));
        assert_eq!(anchored.start_at.as_deref(), Some("2026-03-01T00:00:00Z"));
        assert_eq!(anchored.interval, "P1D");
    }

    #[test]
    fn cron_syntax_is_refused_as_an_unsupported_form() {
        for cron in ["0 0 * * *", "*/5 * * * *", "0 0 12 ? * MON-FRI"] {
            let e = parse_timer_cycle(cron).unwrap_err();
            assert!(e.is_unsupported_form(), "{cron}: {e:?}");
            assert!(e.reason().contains("cron"), "{cron}: {e}");
        }
    }

    #[test]
    fn malformed_cycles_are_malformed_not_unsupported() {
        for bad in ["R", "Rx/PT1H", "R0/PT1H", "R/", "R/PT0S", "R/a/b/c/d"] {
            let e = parse_timer_cycle(bad).unwrap_err();
            assert!(
                !e.is_unsupported_form(),
                "'{bad}' should be Malformed: {e:?}"
            );
        }
        // A calendar interval inside a cycle keeps its UnsupportedForm classification.
        assert!(parse_timer_cycle("R/P1M")
            .unwrap_err()
            .is_unsupported_form());
    }

    #[test]
    fn an_unanchored_cycle_first_fires_one_interval_out() {
        let timer = TimerDefinition::Cycle(parse_timer_cycle("R/PT1H").unwrap());
        let now = datetime!(2026-08-05 12:00:00 UTC);
        assert_eq!(
            timer.first_due_at(now).unwrap(),
            datetime!(2026-08-05 13:00:00 UTC)
        );
    }

    #[test]
    fn an_anchored_cycle_first_fires_at_its_anchor() {
        let timer =
            TimerDefinition::Cycle(parse_timer_cycle("R2/2026-01-01T00:00:00Z/PT1H").unwrap());
        let now = datetime!(2026-08-05 12:00:00 UTC);
        // Anchored in the past ⇒ already due, exactly like a past timeDate.
        assert_eq!(
            timer.first_due_at(now).unwrap(),
            datetime!(2026-01-01 00:00:00 UTC)
        );
    }

    #[test]
    fn advancing_an_unbounded_cycle_walks_the_grid() {
        let spec = parse_timer_cycle("R/PT1H").unwrap();
        let fired = datetime!(2026-08-05 12:00:00 UTC);
        let now = datetime!(2026-08-05 12:00:01 UTC);
        assert_eq!(
            advance_cycle(&spec, fired, None, now).unwrap(),
            TimerCycleAdvance::Next {
                due_at: datetime!(2026-08-05 13:00:00 UTC),
                remaining: None
            }
        );
    }

    #[test]
    fn a_bounded_cycle_exhausts_after_its_last_fire() {
        let spec = parse_timer_cycle("R2/PT1H").unwrap();
        let fired = datetime!(2026-08-05 12:00:00 UTC);
        let now = datetime!(2026-08-05 12:00:01 UTC);
        // Two fires budgeted; the first has just happened ⇒ one left, due an hour on.
        let after_first = advance_cycle(&spec, fired, Some(2), now).unwrap();
        assert_eq!(
            after_first,
            TimerCycleAdvance::Next {
                due_at: datetime!(2026-08-05 13:00:00 UTC),
                remaining: Some(1)
            }
        );
        // The second fire spends the last repeat.
        assert_eq!(
            advance_cycle(
                &spec,
                datetime!(2026-08-05 13:00:00 UTC),
                Some(1),
                datetime!(2026-08-05 13:00:01 UTC)
            )
            .unwrap(),
            TimerCycleAdvance::Exhausted
        );
    }

    #[test]
    fn a_missed_window_coalesces_instead_of_bursting() {
        // The engine was down for five hours; the schedule fires ONCE and lands on the first
        // future slot rather than replaying five occurrences.
        let spec = parse_timer_cycle("R/PT1H").unwrap();
        let fired = datetime!(2026-08-05 12:00:00 UTC);
        let now = datetime!(2026-08-05 17:30:00 UTC);
        assert_eq!(
            advance_cycle(&spec, fired, None, now).unwrap(),
            TimerCycleAdvance::Next {
                due_at: datetime!(2026-08-05 18:00:00 UTC),
                remaining: None
            }
        );
        // A BOUNDED cycle spends a repeat per skipped slot, so it can never outlive its budget.
        let bounded = parse_timer_cycle("R3/PT1H").unwrap();
        assert_eq!(
            advance_cycle(&bounded, fired, Some(3), now).unwrap(),
            TimerCycleAdvance::Exhausted
        );
    }

    #[test]
    fn definitions_round_trip_through_their_persisted_form() {
        for timer in [
            TimerDefinition::Duration("PT1H".to_owned()),
            TimerDefinition::Date("2026-03-01T00:00:00Z".to_owned()),
            TimerDefinition::Cycle(parse_timer_cycle("R/PT1H").unwrap()),
            TimerDefinition::Cycle(parse_timer_cycle("R4/PT15M").unwrap()),
            TimerDefinition::Cycle(parse_timer_cycle("R4/2026-03-01T00:00:00Z/PT15M").unwrap()),
        ] {
            let back =
                TimerDefinition::from_persisted(timer.kind_str(), &timer.spec_text()).unwrap();
            assert_eq!(back, timer, "round-trip of {}", timer.spec_text());
        }
        assert!(TimerDefinition::from_persisted("SUNRISE", "PT1H").is_err());
    }

    #[test]
    fn total_fires_is_one_for_the_single_shot_forms() {
        assert_eq!(
            TimerDefinition::Duration("PT1H".to_owned()).total_fires(),
            Some(1)
        );
        assert_eq!(
            TimerDefinition::Date("2026-03-01T00:00:00Z".to_owned()).total_fires(),
            Some(1)
        );
        assert_eq!(
            TimerDefinition::Cycle(parse_timer_cycle("R/PT1H").unwrap()).total_fires(),
            None
        );
        assert_eq!(
            TimerDefinition::Cycle(parse_timer_cycle("R7/PT1H").unwrap()).total_fires(),
            Some(7)
        );
    }
}
