//! FEEL temporal literals and their string forms — shared by the lexer (`@"…"` literals), the
//! temporal builtins (`date`/`time`/`duration`/`date and time`), and the DMN-TCK value codec.
//!
//! Four value shapes are recognised from an ISO-8601 string:
//! - `date` — `2021-01-01` → [`FeelValue::Date`] (also accepts a 5/6-digit unsigned year, or a
//!   negative/BCE-extended year — `large-dates` is enabled, but `time`'s own `[year]` format
//!   component is hard-4-digit regardless, so >4-digit years are located and parsed by hand).
//! - `time` — `10:10:10` → [`FeelValue::Time`] — an optional trailing qualifier (`Z`,
//!   `[+-]HH:MM[:SS]`, or `@Region/City`) is parsed and retained for round-trip/accessor purposes,
//!   but never changes the parsed wall-clock value itself (a `time` literal has no date to combine
//!   an offset against).
//! - `date and time` — `2021-01-01T10:10:10` (local ⇒ no qualifier) or with an offset/`Z`/`@Zone`
//!   suffix → [`FeelValue::Instant`]. An `@Zone` suffix is resolved through the bundled IANA tz
//!   database (`time-tz`'s `db` feature) for a DST-correct numeric offset at the value's own date;
//!   the zone *name* is retained separately (as the qualifier) purely so `string()` can echo it
//!   back verbatim instead of the resolved number.
//! - `duration` — `P1Y2M` (years-and-months) or `P1DT2H` (days-and-time) → [`FeelValue::Duration`]
//!   — classified by which unit *letters* were present in the source text, not by whether their
//!   values happen to be zero (`P0Y`/`P0M` are still years-and-months durations, equal to zero).
//!
//! A numeric offset outside `[-14:00, +14:00]`, an offset and an `@Zone` suffix both present at
//! once, or an unresolvable `@Zone` name are all rejected (`None`) — never silently accepted with
//! the offending part dropped.

use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
use time_tz::PrimitiveDateTimeExt;

use crate::value::{FeelDuration, FeelValue, TimeQualifier};

/// Parse the body of an `@"…"` literal (or a temporal builtin's string argument, or a TCK
/// expected value) into the matching temporal [`FeelValue`]; `None` if unrecognised.
pub fn parse_at_literal(s: &str) -> Option<FeelValue> {
    let t = s.trim();
    if t.starts_with('P') || t.starts_with("-P") {
        return parse_duration(t).map(FeelValue::Duration);
    }
    // Decide date vs time vs date-time on the *core* (zone/offset suffix stripped), so a `T`
    // inside a zone name (`00:01:00@Etc/UTC`) doesn't misroute a time as a date-time.
    let core = strip_zone(t);
    if core.contains('T') {
        parse_date_time(t).map(|(dt, q)| FeelValue::Instant(dt, q))
    } else if core.contains(':') {
        parse_time(t).map(|(tm, q)| FeelValue::Time(tm, q))
    } else {
        parse_date(t).map(FeelValue::Date)
    }
}

/// The value core with any trailing zone/offset (`@Zone`, `+hh:mm`, `Z`) removed — a leading sign
/// (negative year) is preserved, so the scan starts at index 1. Routing-only heuristic (decides
/// date vs time vs date-time); the real qualifier parsing lives in [`parse_time`]/[`parse_date_time`].
fn strip_zone(s: &str) -> &str {
    match s
        .char_indices()
        .skip(1)
        .find(|(_, c)| matches!(c, '+' | 'Z' | '@'))
    {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// ISO-8601 duration `[-]P[nY][nM][nW][nD][T[nH][nM][nS]]`. Classified by which unit *letters*
/// were present in the date part (`Y`/`M` vs `W`/`D`), not by whether the parsed numbers happen to
/// be zero — `P0Y`/`P0M` are still years-and-months durations (DMN-TCK 1121-feel-years-and-
/// months-duration-function `#012`/`#013`, 0100-arithmetic cluster 2).
pub fn parse_duration(s: &str) -> Option<FeelDuration> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let body = body.strip_prefix('P')?;
    let (date_part, time_part) = match body.split_once('T') {
        Some((d, t)) => (d, t),
        None => (body, ""),
    };
    let date_pairs = iso_pairs(date_part)?;
    let time_pairs = iso_pairs(time_part)?;
    // A bare `P` / `PT` with no components is not a valid duration.
    if date_pairs.is_empty() && time_pairs.is_empty() {
        return None;
    }
    let (mut years, mut months, mut days) = (0i64, 0i64, 0i64);
    let (mut has_ym, mut has_day) = (false, false);
    for (num, unit) in date_pairs {
        match unit {
            'Y' => {
                years = num;
                has_ym = true;
            }
            'M' => {
                months = num;
                has_ym = true;
            }
            'W' => {
                days += num * 7;
                has_day = true;
            }
            'D' => {
                days += num;
                has_day = true;
            }
            _ => return None,
        }
    }
    let (mut hours, mut mins, mut secs, mut has_time) = (0i64, 0i64, 0i64, false);
    for (num, unit) in time_pairs {
        has_time = true;
        match unit {
            'H' => hours = num,
            'M' => mins = num,
            'S' => secs = num,
            _ => return None,
        }
    }
    if has_ym && !has_day && !has_time {
        let total = years * 12 + months;
        Some(FeelDuration::YearsMonths(sign(neg, total) as i32))
    } else {
        let total = days * 86_400 + hours * 3_600 + mins * 60 + secs;
        Some(FeelDuration::DaysTime(time::Duration::seconds(sign(
            neg, total,
        ))))
    }
}

fn sign(neg: bool, v: i64) -> i64 {
    if neg {
        -v
    } else {
        v
    }
}

/// Consecutive `<integer><letter>` pairs (`1Y`, `2M`, …). `None` on any stray character or a
/// number with no trailing unit.
fn iso_pairs(s: &str) -> Option<Vec<(i64, char)>> {
    let mut out = Vec::new();
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c); // `.` allows a fractional smallest unit (e.g. `0.000S`)
        } else if c.is_ascii_alphabetic() {
            if num.is_empty() {
                return None;
            }
            // Integer part only — sub-second precision is truncated.
            let int_part = num.split('.').next().unwrap_or("");
            let n: i64 = if int_part.is_empty() {
                0
            } else {
                int_part.parse().ok()?
            };
            out.push((n, c));
            num.clear();
        } else {
            return None;
        }
    }
    if num.is_empty() {
        Some(out)
    } else {
        None // trailing digits with no unit
    }
}

fn parse_date(s: &str) -> Option<Date> {
    if s.starts_with('+') {
        return None; // FEEL never accepts a leading `+` year sign
    }
    // A negative (BCE-extended) year: parse the unsigned form, then negate the year.
    if let Some(rest) = s.strip_prefix('-') {
        let d = parse_naive_date(rest)?;
        return d.replace_year(-d.year()).ok();
    }
    parse_naive_date(s)
}

/// `[year]-[month]-[day]`, `year` unsigned — 4-digit fast path via `time`'s own parser, falling
/// back to a hand-rolled year-segment split for a 5/6-digit year (`time`'s `[year]` format
/// component is hard-4-digit even with `large-dates` — verified directly against the vendored
/// crate; `Date`'s own value range comfortably covers ±999,999, so this is a parsing gap only).
fn parse_naive_date(s: &str) -> Option<Date> {
    if let Ok(d) = Date::parse(s, format_description!("[year]-[month]-[day]")) {
        return Some(d);
    }
    let dash = s.get(4..)?.find('-')? + 4;
    if dash > 4 && s.as_bytes().first() == Some(&b'0') {
        return None; // a 5+ digit year must not have a leading zero (ambiguous/invalid — DMN-TCK
                     // 1115 `#039`: "01211-12-31" is rejected, not read as year 1211)
    }
    let year: i32 = s[..dash].parse().ok()?;
    let (month, day) = s[dash + 1..].split_once('-')?;
    let month = time::Month::try_from(month.parse::<u8>().ok()?).ok()?;
    let day: u8 = day.parse().ok()?;
    Date::from_calendar_date(year, month, day).ok()
}

/// `[hour]:[minute]:[second][.subsecond]`, with an optional trailing qualifier retained (never
/// applied to the parsed wall-clock value — see the module docs).
fn parse_time(s: &str) -> Option<(Time, Option<TimeQualifier>)> {
    // `@Zone` suffix: unambiguous manual split (never appears elsewhere in a time literal).
    if let Some((core, zone)) = s.split_once('@') {
        if zone.is_empty() || time_tz::timezones::get_by_name(zone).is_none() {
            return None; // unknown zone id
        }
        let t = parse_naive_time(core)?;
        return Some((t, Some(TimeQualifier::Zone(zone.to_string()))));
    }
    // `Z` / a numeric offset — a FEEL time always starts with a digit, so the first `+`/`-`/`Z`
    // found (from the very start) is unambiguously the qualifier introducer.
    if let Some(idx) = s.find(['+', '-', 'Z']) {
        let core = &s[..idx];
        let suffix = &s[idx..];
        let t = parse_naive_time(core)?;
        if suffix == "Z" || suffix == "z" {
            return Some((t, Some(TimeQualifier::Zulu)));
        }
        let offset = parse_numeric_offset(suffix)?;
        return Some((t, Some(TimeQualifier::Offset(offset))));
    }
    let t = parse_naive_time(s)?;
    Some((t, None))
}

/// The bare wall-clock component (no qualifier) — fractional seconds optional; `24:00:00` (the
/// ISO-8601 end-of-day spelling) folds to midnight (a standalone `time` has no date to roll over).
fn parse_naive_time(s: &str) -> Option<Time> {
    let s = if s.starts_with("24:00:00") {
        "00:00:00"
    } else {
        s
    };
    Time::parse(
        s,
        format_description!("[hour]:[minute]:[second].[subsecond]"),
    )
    .or_else(|_| Time::parse(s, format_description!("[hour]:[minute]:[second]")))
    .ok()
}

/// `[+-]HH:MM[:SS]`, exactly (no trailing garbage — this is what rejects a malformed offset like
/// `+5` or an offset-and-zone combo like `+02:00@Europe/Paris`, since the leftover `@Europe/Paris`
/// fails to fit any of the three numeric segments), validated to FEEL's `±14:00` range.
fn parse_numeric_offset(s: &str) -> Option<UtcOffset> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+')?),
    };
    let mut parts = rest.split(':');
    let h = two_digit(parts.next()?)?;
    let m = two_digit(parts.next()?)?;
    let sec = match parts.next() {
        Some(s) => two_digit(s)?,
        None => 0,
    };
    if parts.next().is_some() {
        return None; // extra `:`-separated segment / trailing garbage
    }
    let total = h as i32 * 3600 + m as i32 * 60 + sec as i32;
    if total > 14 * 3600 {
        return None; // outside FEEL's ±14:00 offset range
    }
    let sign: i8 = if neg { -1 } else { 1 };
    UtcOffset::from_hms(sign * h as i8, sign * m as i8, sign * sec as i8).ok()
}

/// Exactly two ASCII digits, parsed as `u8` — rejects `+5` (no leading zero / wrong width).
fn two_digit(s: &str) -> Option<u8> {
    if s.len() == 2 && s.bytes().all(|b| b.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

/// `date and time` literal → `(instant, qualifier)`. The returned `OffsetDateTime` always carries
/// the fully-resolved numeric offset (DST-correct for a `@Zone` qualifier, via the bundled IANA
/// database), so arithmetic/ordering/`=`-equality all operate on the true absolute instant
/// regardless of which zone/offset spelling produced the value; `qualifier` mirrors the source
/// spelling verbatim for `string()` round-tripping.
fn parse_date_time(s: &str) -> Option<(OffsetDateTime, Option<TimeQualifier>)> {
    if s.starts_with('+') {
        return None; // FEEL never accepts a leading `+` year sign
    }
    // ISO-8601 `24:00:00` end-of-day spelling: rewrite to the next day's midnight and recurse
    // (preserves any offset/zone suffix, which sits after the rewritten substring untouched).
    const END_OF_DAY: &str = "T24:00:00";
    if let Some(idx) = s.find(END_OF_DAY) {
        let rewritten = format!("{}T00:00:00{}", &s[..idx], &s[idx + END_OF_DAY.len()..]);
        let (dt, q) = parse_date_time(&rewritten)?;
        return Some((dt.checked_add(time::Duration::days(1))?, q));
    }
    // A negative (BCE-extended) year: parse the unsigned form, then negate the year (the sign
    // never participates in offset/zone detection below — it is stripped first).
    if let Some(rest) = s.strip_prefix('-') {
        let (dt, q) = parse_date_time_unsigned(rest)?;
        let negated = dt.replace_year(-dt.year()).ok()?;
        return Some((negated, q));
    }
    parse_date_time_unsigned(s)
}

fn parse_date_time_unsigned(s: &str) -> Option<(OffsetDateTime, Option<TimeQualifier>)> {
    // `@Zone` suffix: unambiguous manual split (the character never appears elsewhere in a
    // date-time literal) — resolved through the bundled tz database for a DST-correct offset.
    // Rejects an offset-and-zone combo for free: `core` would still carry the numeric offset
    // text, which `parse_naive_date_time` (an exact-match parser) fails to consume.
    if let Some((core, zone)) = s.split_once('@') {
        if zone.is_empty() {
            return None;
        }
        let tz = time_tz::timezones::get_by_name(zone)?;
        let naive = parse_naive_date_time(core)?;
        let resolved = naive.assume_timezone(tz).take_first()?;
        return Some((resolved, Some(TimeQualifier::Zone(zone.to_string()))));
    }
    // Explicit numeric offset / `Z`, via a real RFC-3339 parser — it resolves the date-vs-offset
    // `-` ambiguity correctly (a fixed ISO-8601 grammar, not a manual character scan).
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        let offset = dt.offset();
        if offset.whole_seconds().unsigned_abs() > 14 * 3600 {
            return None; // outside FEEL's ±14:00 offset range
        }
        let qualifier = if offset.is_utc() && (s.ends_with('Z') || s.ends_with('z')) {
            TimeQualifier::Zulu
        } else {
            TimeQualifier::Offset(offset)
        };
        return Some((dt, Some(qualifier)));
    }
    // Local (no offset/zone at all) — fractional seconds allowed either way.
    let naive = parse_naive_date_time(s)?;
    Some((naive.assume_utc(), None))
}

/// `[year]-[month]-[day]T[hour]:[minute]:[second][.subsecond]`, `year` unsigned — 4-digit fast
/// path (fractional seconds tried first, mirroring [`parse_naive_time`]'s two-arm pattern), falling
/// back to a hand-rolled year-segment split for a 5/6-digit year (see [`parse_naive_date`]).
fn parse_naive_date_time(s: &str) -> Option<PrimitiveDateTime> {
    if let Ok(pdt) = PrimitiveDateTime::parse(
        s,
        format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]"),
    ) {
        return Some(pdt);
    }
    if let Ok(pdt) = PrimitiveDateTime::parse(
        s,
        format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]"),
    ) {
        return Some(pdt);
    }
    // `PrimitiveDateTime::parse` can't be given a yearless format description (it errors
    // `InsufficientInformation` — a `PrimitiveDateTime` needs a full `Date`, not just a month/
    // day) — build the `Date` from `year`/`month`/`day` directly instead, delegating only the
    // time-of-day portion to `parse_naive_time` (which already handles fractional seconds and
    // the `24:00:00` end-of-day spelling).
    let dash = s.get(4..)?.find('-')? + 4;
    if dash > 4 && s.as_bytes().first() == Some(&b'0') {
        return None; // a 5+ digit year must not have a leading zero (ambiguous/invalid — DMN-TCK
                     // 1117 `#067`: "01211-12-31T11:22:33" is rejected, not read as year 1211)
    }
    let year: i32 = s[..dash].parse().ok()?;
    let rest = &s[dash + 1..]; // "MM-DDT..."
    let (month_day, time_part) = rest.split_once('T')?;
    let (month_s, day_s) = month_day.split_once('-')?;
    let month = time::Month::try_from(month_s.parse::<u8>().ok()?).ok()?;
    let day: u8 = day_s.parse().ok()?;
    let date = Date::from_calendar_date(year, month, day).ok()?;
    let time = parse_naive_time(time_part)?;
    Some(PrimitiveDateTime::new(date, time))
}

/// `HH:MM:SS[.fff…]` rendering (canonical string / `string()` builtin), plus whichever
/// zone/offset qualifier the value carries.
pub fn format_time(t: &Time, qualifier: &Option<TimeQualifier>) -> String {
    let base = t
        .format(format_description!("[hour]:[minute]:[second]"))
        .unwrap_or_else(|_| format!("{t:?}"));
    format!(
        "{base}{}{}",
        crate::value::format_subsecond(t.nanosecond()),
        format_qualifier(qualifier)
    )
}

/// `Z` / `+HH:MM[:SS]` / `@Region/City` — empty when there is no qualifier at all (a local value).
pub(crate) fn format_qualifier(qualifier: &Option<TimeQualifier>) -> String {
    match qualifier {
        None => String::new(),
        Some(TimeQualifier::Zulu) => "Z".to_string(),
        Some(TimeQualifier::Offset(o)) => format_offset(*o),
        Some(TimeQualifier::Zone(name)) => format!("@{name}"),
    }
}

fn format_offset(o: UtcOffset) -> String {
    let total = o.whole_seconds();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.unsigned_abs();
    let (h, m, sec) = (abs / 3_600, (abs % 3_600) / 60, abs % 60);
    if sec > 0 {
        format!("{sign}{h:02}:{m:02}:{sec:02}")
    } else {
        format!("{sign}{h:02}:{m:02}")
    }
}

/// ISO-8601 rendering of a duration (canonical string / `string()` builtin). Equality never
/// relies on this — [`FeelDuration`] compares structurally.
pub fn format_duration(d: &FeelDuration) -> String {
    let mut out = String::new();
    match d {
        FeelDuration::YearsMonths(months) => {
            if *months < 0 {
                out.push('-');
            }
            let m = months.unsigned_abs();
            out.push('P');
            if m / 12 > 0 {
                out.push_str(&format!("{}Y", m / 12));
            }
            if m % 12 > 0 || m == 0 {
                out.push_str(&format!("{}M", m % 12));
            }
        }
        FeelDuration::DaysTime(dur) => {
            if dur.is_negative() {
                out.push('-');
            }
            let secs = dur.whole_seconds().unsigned_abs();
            let (days, h, mi, se) = (
                secs / 86_400,
                (secs % 86_400) / 3_600,
                (secs % 3_600) / 60,
                secs % 60,
            );
            out.push('P');
            if days > 0 {
                out.push_str(&format!("{days}D"));
            }
            if h > 0 || mi > 0 || se > 0 || days == 0 {
                out.push('T');
                if h > 0 {
                    out.push_str(&format!("{h}H"));
                }
                if mi > 0 {
                    out.push_str(&format!("{mi}M"));
                }
                if se > 0 || (h == 0 && mi == 0) {
                    out.push_str(&format!("{se}S"));
                }
            }
        }
    }
    out
}
