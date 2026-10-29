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
    /// A FEEL expression in place of the literal, evaluated when the timer is ARMED and resolved
    /// through [`resolve_timer_expression_value`] into one of the three literal variants. Only
    /// the resolved form is ever persisted, so the expression is never evaluated again for the
    /// same arming (a resume or replay reads the stored due-at).
    Expression {
        /// Which literal form the result must take.
        form: TimerExpressionForm,
        /// The FEEL source, as authored.
        expression: String,
    },
}

/// The literal form a timer text stands for — what a FEEL expression in its place must resolve
/// to (see [`resolve_timer_expression_value`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerExpressionForm {
    /// A duration: `<timeDuration>`, `<q:timeout duration>`, `<q:retry initialDelay|maxDelay>`.
    Duration,
    /// An absolute instant: `<timeDate>`.
    Date,
    /// A repeating interval: `<timeCycle>` (start events only for an expression).
    Cycle,
}

impl TimerExpressionForm {
    /// The element name used in diagnostics.
    pub fn element_name(self) -> &'static str {
        match self {
            TimerExpressionForm::Duration => "timeDuration",
            TimerExpressionForm::Date => "timeDate",
            TimerExpressionForm::Cycle => "timeCycle",
        }
    }
}

/// How the loader classified one timer text (see [`classify_timer_text`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerTextClass {
    /// A valid ISO-8601 literal of the expected form — scheduled exactly as before.
    Literal,
    /// Not a literal, but valid FEEL — evaluated when the timer is armed.
    Expression,
}

/// The name a timer expression reads the deployment configuration through (`config.<key>`).
pub const TIMER_CONFIG_ROOT: &str = "config";

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
            TimerDefinition::Expression { form, .. } => match form {
                TimerExpressionForm::Duration => TIMER_KIND_DURATION,
                TimerExpressionForm::Date => TIMER_KIND_DATE,
                TimerExpressionForm::Cycle => TIMER_KIND_CYCLE,
            },
        }
    }

    /// True when this definition is a FEEL expression still to be resolved at arm time.
    pub fn is_expression(&self) -> bool {
        matches!(self, TimerDefinition::Expression { .. })
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
            TimerDefinition::Expression { expression, .. } => expression.clone(),
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
            // Unknown until resolved; only a resolved definition is ever persisted.
            TimerDefinition::Expression { form, .. } => match form {
                TimerExpressionForm::Cycle => None,
                _ => Some(1),
            },
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
            TimerDefinition::Expression { expression, .. } => {
                Err(TimerSpecRejection::Malformed(format!(
                    "the timer expression '{expression}' must be resolved before it is scheduled"
                )))
            }
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

// ---- expression-valued timers ----------------------------------------------------------------

/// True when `text` LOOKS like an attempted ISO-8601 literal of `form` — the shape that keeps a
/// typo a LOAD error instead of letting it parse as a FEEL name (`PT5X`) or as FEEL arithmetic
/// (`2026-03-01` is `2022` in FEEL).
///
/// - duration: `P`/`p` followed by a digit or `T` (`PT5X`, `P1M`);
/// - date: four digits, `-`, two digits, `-`, two digits (`2026-03-01`, a zone-less instant);
/// - cycle: `R`/`r` followed by digits only, or by anything containing `/` (`R`, `R0/PT1H`,
///   `Rx/PT1H`).
pub fn is_literal_shaped(form: TimerExpressionForm, text: &str) -> bool {
    let t = text.trim();
    let b = t.as_bytes();
    match form {
        TimerExpressionForm::Duration => {
            let rest = t.strip_prefix(['+', '-']).unwrap_or(t);
            let rb = rest.as_bytes();
            rb.len() >= 2
                && matches!(rb[0], b'P' | b'p')
                && (rb[1].is_ascii_digit() || matches!(rb[1], b'T' | b't'))
        }
        TimerExpressionForm::Date => {
            b.len() >= 10
                && b[..4].iter().all(u8::is_ascii_digit)
                && b[4] == b'-'
                && b[5..7].iter().all(u8::is_ascii_digit)
                && b[7] == b'-'
                && b[8..10].iter().all(u8::is_ascii_digit)
        }
        TimerExpressionForm::Cycle => {
            let Some(rest) = t.strip_prefix(['R', 'r']) else {
                return false;
            };
            // `R`, `R5`, or anything `R…` with a `/` in it (`Rx/PT1H`): a cycle written wrong.
            rest.chars().all(|c| c.is_ascii_digit()) || rest.contains('/')
        }
    }
}

/// Parse a timer expression as FEEL, with the spaced-builtin name table so `date and time(…)`
/// parses exactly as it will at evaluation time.
pub fn parse_timer_expression(text: &str) -> Result<sutra_feel::FeelExpr, sutra_feel::FeelError> {
    sutra_feel::expressions::parse_with_context_names(
        text.trim(),
        &std::collections::HashSet::from([TIMER_CONFIG_ROOT.to_string()]),
    )
}

/// Validate a timer text as a literal of `form` — the exact check the loader has always run.
pub fn validate_timer_literal(
    form: TimerExpressionForm,
    text: &str,
) -> Result<(), TimerSpecRejection> {
    match form {
        TimerExpressionForm::Duration => parse_timer_duration(text).map(|_| ()),
        TimerExpressionForm::Date => parse_timer_instant(text).map(|_| ()),
        TimerExpressionForm::Cycle => parse_timer_cycle(text).map(|_| ()),
    }
}

/// Classify one timer text: **literal first, else FEEL**.
///
/// 1. A valid literal of `form` is [`TimerTextClass::Literal`].
/// 2. A literal-shaped text ([`is_literal_shaped`]) that fails keeps the literal's rejection.
/// 3. Anything else that parses as FEEL is [`TimerTextClass::Expression`].
/// 4. Anything that is neither keeps the literal's rejection, with the FEEL parse error added to
///    the reason (the rejection KIND is unchanged, so the loader's diagnostic code is too).
pub fn classify_timer_text(
    form: TimerExpressionForm,
    text: &str,
) -> Result<TimerTextClass, TimerSpecRejection> {
    let literal = match validate_timer_literal(form, text) {
        Ok(()) => return Ok(TimerTextClass::Literal),
        Err(rejection) => rejection,
    };
    if text.trim().is_empty() || is_literal_shaped(form, text) {
        // A mistyped duration literal: name the literal that was most likely meant.
        if form == TimerExpressionForm::Duration {
            if let Some(meant) = suggest_duration_literal(text) {
                let reason = format!("{}; did you mean {meant}?", literal.reason());
                return Err(match literal {
                    TimerSpecRejection::UnsupportedForm(_) => {
                        TimerSpecRejection::UnsupportedForm(reason)
                    }
                    TimerSpecRejection::Malformed(_) => TimerSpecRejection::Malformed(reason),
                });
            }
        }
        return Err(literal);
    }
    match parse_timer_expression(text) {
        Ok(_) => Ok(TimerTextClass::Expression),
        Err(feel) => {
            let reason = format!(
                "{}; it is not a FEEL expression either ({})",
                literal.reason(),
                feel.message
            );
            Err(match literal {
                TimerSpecRejection::UnsupportedForm(_) => {
                    TimerSpecRejection::UnsupportedForm(reason)
                }
                TimerSpecRejection::Malformed(_) => TimerSpecRejection::Malformed(reason),
            })
        }
    }
}

/// The root names a timer expression reads (`payload` for `payload.window`, `config` for
/// `config.retryDelay`), deduplicated in source order. Used for the start-timer scope check.
pub fn timer_expression_roots(text: &str) -> Result<Vec<String>, sutra_feel::FeelError> {
    let expr = parse_timer_expression(text)?;
    let mut roots: Vec<String> = Vec::new();
    for path in sutra_feel::paths::extract(&expr) {
        let root = path.root().to_string();
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    Ok(roots)
}

/// The ISO-8601 literal a mistyped timer text was most likely meant to be (`PT10SEC` → `PT10S`,
/// `pt5m` → `PT5M`, `P1DAY` → `P1D`, `PT1H30MIN` → `PT1H30M`), or `None` when the text does not
/// look like a duration attempt. Only ever used to word a diagnostic.
pub fn suggest_duration_literal(text: &str) -> Option<String> {
    let upper: String = text
        .trim()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '_')
        .collect::<String>()
        .to_ascii_uppercase();
    let rest = upper.strip_prefix('P')?;
    let mut date = String::new();
    let mut time = String::new();
    let mut in_time = false;
    let mut chars = rest.chars().peekable();
    if chars.peek() == Some(&'T') {
        chars.next();
        in_time = true;
    }
    let mut seen_unit = false;
    while chars.peek().is_some() {
        let mut number = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_digit() || *c == '.' {
                number.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        let mut word = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_alphabetic() {
                word.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if number.is_empty() {
            if word == "T" && !in_time {
                in_time = true;
                continue;
            }
            return None;
        }
        // A unit word: the unit letter, optionally spelled out (and a time `T` the author left
        // out, inferred from a time unit after a date part).
        let unit = match word.as_str() {
            "S" | "SEC" | "SECS" | "SECOND" | "SECONDS" => 'S',
            "M" if in_time => 'M',
            "MIN" | "MINS" | "MINUTE" | "MINUTES" => 'M',
            "H" | "HR" | "HRS" | "HOUR" | "HOURS" => 'H',
            "D" | "DAY" | "DAYS" => 'D',
            "W" | "WEEK" | "WEEKS" => 'W',
            _ => return None,
        };
        seen_unit = true;
        match unit {
            'D' | 'W' if !in_time => date.push_str(&format!("{number}{unit}")),
            'S' | 'M' | 'H' => {
                in_time = true;
                time.push_str(&format!("{number}{unit}"));
            }
            _ => return None,
        }
    }
    if !seen_unit {
        return None;
    }
    let candidate = if time.is_empty() {
        format!("P{date}")
    } else {
        format!("P{date}T{time}")
    };
    (candidate != text.trim() && parse_timer_duration(&candidate).is_ok()).then_some(candidate)
}

/// The `config.<key>` keys a timer expression reads, deduplicated in source order.
pub fn timer_expression_config_keys(text: &str) -> Result<Vec<String>, sutra_feel::FeelError> {
    let expr = parse_timer_expression(text)?;
    let mut keys: Vec<String> = Vec::new();
    for path in sutra_feel::paths::extract(&expr) {
        if path.root() == TIMER_CONFIG_ROOT {
            if let Some(key) = path.segments.get(1) {
                if !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
        }
    }
    Ok(keys)
}

/// A short, human-readable rendering of a FEEL result for a diagnostic.
fn describe_feel_value(value: &sutra_feel::FeelValue) -> String {
    use sutra_feel::FeelValue as V;
    match value {
        V::Null => "null".to_owned(),
        V::Boolean(b) => format!("the boolean {b}"),
        V::Number(n) => format!("the number {n}"),
        V::String(s) => format!("the string \"{s}\""),
        V::Instant(..) => "a date and time".to_owned(),
        V::Date(..) => "a date".to_owned(),
        V::Time(..) => "a time".to_owned(),
        V::Duration(sutra_feel::FeelDuration::YearsMonths(_)) => {
            "a years-and-months duration".to_owned()
        }
        V::Duration(sutra_feel::FeelDuration::DaysTime(_)) => "a days-and-time duration".to_owned(),
        V::List(_) => "a list".to_owned(),
        V::Map(_) => "a context".to_owned(),
        V::Function(_) | V::Invocable(_) => "a function".to_owned(),
        V::Range(_) => "a range".to_owned(),
    }
}

/// Turn a FEEL result into a non-negative duration: an ISO-8601 duration string (under the
/// literal rules, so a calendar `P1M` is refused) or a FEEL days-and-time duration ≥ 0.
pub fn duration_from_feel_value(
    value: &sutra_feel::FeelValue,
) -> Result<std::time::Duration, String> {
    use sutra_feel::FeelValue as V;
    match value {
        V::String(s) => parse_timer_duration(s).map_err(|r| {
            format!("it returned the string \"{s}\", which is not a usable duration: {r}")
        }),
        V::Duration(sutra_feel::FeelDuration::DaysTime(d)) => std::time::Duration::try_from(*d)
            .map_err(|_| {
                format!(
                    "it returned the negative duration {d}; a timer cannot wait a negative time"
                )
            }),
        V::Duration(sutra_feel::FeelDuration::YearsMonths(_)) => Err(
            "it returned a years-and-months duration, which has no exact length; return a \
             days-and-time duration or compute a date and time instead"
                .to_owned(),
        ),
        other => Err(format!(
            "it returned {}; a duration timer needs an ISO-8601 duration string or a FEEL \
             days-and-time duration",
            describe_feel_value(other)
        )),
    }
}

/// Turn a FEEL result into an absolute instant: an RFC 3339 string with a zone, or a FEEL date
/// and time that carries an offset or zone. A zone-less value is refused, as the literal is.
pub fn instant_from_feel_value(value: &sutra_feel::FeelValue) -> Result<OffsetDateTime, String> {
    use sutra_feel::FeelValue as V;
    match value {
        V::String(s) => parse_timer_instant(s).map_err(|r| {
            format!("it returned the string \"{s}\", which is not a usable instant: {r}")
        }),
        V::Instant(at, Some(_)) => Ok(*at),
        V::Instant(_, None) => Err(
            "it returned a date and time with no offset or zone; \"which clock?\" is not a \
             question a durable timer may guess at"
                .to_owned(),
        ),
        other => Err(format!(
            "it returned {}; a timeDate timer needs an ISO-8601 instant string or a FEEL date \
             and time with a zone",
            describe_feel_value(other)
        )),
    }
}

/// Resolve a FEEL result into the literal [`TimerDefinition`] it stands for. The returned
/// definition is what gets scheduled and persisted.
pub fn resolve_timer_expression_value(
    form: TimerExpressionForm,
    value: &sutra_feel::FeelValue,
) -> Result<TimerDefinition, String> {
    match form {
        TimerExpressionForm::Duration => {
            let d = duration_from_feel_value(value)?;
            // An ISO string result is kept as written (it is what an operator reads on a
            // schedule row); a FEEL duration is rendered.
            Ok(TimerDefinition::Duration(match value {
                sutra_feel::FeelValue::String(s) => s.trim().to_string(),
                _ => format_duration(d),
            }))
        }
        TimerExpressionForm::Date => {
            let at = instant_from_feel_value(value)?;
            Ok(TimerDefinition::Date(format_instant(at)))
        }
        TimerExpressionForm::Cycle => match value {
            sutra_feel::FeelValue::String(s) => parse_timer_cycle(s)
                .map(TimerDefinition::Cycle)
                .map_err(|r| {
                    format!("it returned the string \"{s}\", which is not a usable cycle: {r}")
                }),
            other => Err(format!(
                "it returned {}; a timeCycle timer needs an ISO-8601 repeating-interval string \
                 (R/PT1H, R5/PT1H, R/<start>/PT1H)",
                describe_feel_value(other)
            )),
        },
    }
}

/// Render a duration as the ISO-8601 text a persisted definition carries (`PT<seconds>S`, with
/// a fraction only when there is one).
pub fn format_duration(d: std::time::Duration) -> String {
    let nanos = d.subsec_nanos();
    if nanos == 0 {
        format!("PT{}S", d.as_secs())
    } else {
        let frac = format!("{nanos:09}");
        format!("PT{}.{}S", d.as_secs(), frac.trim_end_matches('0'))
    }
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

#[cfg(test)]
mod suggest_tests {
    use super::suggest_duration_literal;

    #[test]
    fn a_mistyped_duration_is_mapped_to_the_literal_meant() {
        for (typo, meant) in [
            ("PT10SEC", "PT10S"),
            ("pt5m", "PT5M"),
            ("P1DAY", "P1D"),
            ("PT1H30MIN", "PT1H30M"),
            ("P10S", "PT10S"),
            ("PT 2 SECONDS", "PT2S"),
        ] {
            assert_eq!(
                suggest_duration_literal(typo).as_deref(),
                Some(meant),
                "{typo}"
            );
        }
    }

    #[test]
    fn text_that_is_no_duration_attempt_gets_no_suggestion() {
        for text in [
            "retryWindow",
            "payload.delay",
            "PT10S",
            "P",
            "PTX",
            "Period",
        ] {
            assert_eq!(suggest_duration_literal(text), None, "{text}");
        }
    }
}
