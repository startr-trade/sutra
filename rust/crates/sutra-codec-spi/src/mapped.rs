//! Typed nested-map payloads produced by the schema-driven mapper layers of the
//! message-standard codecs.
//!
//! A mapper projects a parsed interchange into a small typed value tree suitable for FEEL
//! addressing in BPMN service tasks (`payload.header.orderNumber`,
//! `payload.lineItems[1].quantity`, …). The closed coercion set is deliberate: the codecs
//! stay domain-neutral and the host-provided schema declares, per field, which coercion the
//! mapper performs.
//!
//! [`MappedValue::Decimal`] carries the validated decimal **literal** (scale-preserving:
//! `100` and `100.00` are different values, exactly as arbitrary-precision decimal equality
//! treats them). Dates/times are calendar-validated components, not strings, so a mapped
//! `orderDate` can never hold a malformed value — coercion failure omits the field and
//! surfaces a WARNING diagnostic instead.

use crate::issue::ValidationIssue;
use crate::result::DecodeOutcome;

/// One typed value in a mapped payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappedValue {
    String(String),
    /// 64-bit integer (`INTEGER` coercion).
    Integer(i64),
    /// Scale-preserving decimal literal (`DECIMAL` coercion), decimal-mark-normalised to `.`.
    Decimal(String),
    /// Calendar date (`DATE_CCYYMMDD` coercion).
    Date {
        year: i32,
        month: u8,
        day: u8,
    },
    /// Calendar date-time, minute precision (`DATETIME_CCYYMMDDHHMM` coercion).
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
    },
    /// Wall-clock time, minute precision (`TIME_HHMM` coercion).
    Time {
        hour: u8,
        minute: u8,
    },
    /// A nested section map (`header`, `trailer`, per-tag sections, loop entries).
    Map(MappedMap),
    /// The `lineItems` list — one map per detail-loop iteration (always present, possibly
    /// empty, so FEEL `count(payload.lineItems)` never short-circuits).
    List(Vec<MappedMap>),
}

impl MappedValue {
    pub fn string(s: impl Into<String>) -> MappedValue {
        MappedValue::String(s.into())
    }

    pub fn decimal(s: impl Into<String>) -> MappedValue {
        MappedValue::Decimal(s.into())
    }

    pub fn date(year: i32, month: u8, day: u8) -> MappedValue {
        MappedValue::Date { year, month, day }
    }

    pub fn datetime(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> MappedValue {
        MappedValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
        }
    }

    pub fn time(hour: u8, minute: u8) -> MappedValue {
        MappedValue::Time { hour, minute }
    }

    /// The nested map when this value is a [`MappedValue::Map`].
    pub fn as_map(&self) -> Option<&MappedMap> {
        match self {
            MappedValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The list entries when this value is a [`MappedValue::List`].
    pub fn as_list(&self) -> Option<&[MappedMap]> {
        match self {
            MappedValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// The string content when this value is a [`MappedValue::String`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MappedValue::String(s) => Some(s),
            _ => None,
        }
    }
}

/// Insertion-ordered string-keyed map — re-inserting an existing key replaces the value in
/// place (latest occurrence wins, original position kept).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MappedMap {
    entries: Vec<(String, MappedValue)>,
}

impl MappedMap {
    pub fn new() -> MappedMap {
        MappedMap::default()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: MappedValue) {
        let key = key.into();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some((_, v)) => *v = value,
            None => self.entries.push((key, value)),
        }
    }

    pub fn get(&self, key: &str) -> Option<&MappedValue> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &MappedValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// The nested map at `key` (convenience for `get(key).and_then(as_map)`).
    pub fn map_at(&self, key: &str) -> Option<&MappedMap> {
        self.get(key).and_then(MappedValue::as_map)
    }

    /// The list at `key` (convenience for `get(key).and_then(as_list)`).
    pub fn list_at(&self, key: &str) -> Option<&[MappedMap]> {
        self.get(key).and_then(MappedValue::as_list)
    }

    /// Mutable access to the nested section map at `key`, created empty when absent (or when
    /// the existing value is not a map).
    pub fn section_mut(&mut self, key: &str) -> &mut MappedMap {
        let present = matches!(
            self.entries.iter().find(|(k, _)| k == key),
            Some((_, MappedValue::Map(_)))
        );
        if !present {
            self.insert(key, MappedValue::Map(MappedMap::new()));
        }
        match self
            .entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
        {
            Some(MappedValue::Map(m)) => m,
            _ => unreachable!("section was just inserted as a map"),
        }
    }
}

/// A schema-aware decode outcome: the payload is the mapper's typed nested map instead of
/// the structural interchange model.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedDecodeResult {
    pub outcome: DecodeOutcome,
    /// Present on `OK` / `SOFT_ERRORS`; absent on `FATAL`.
    pub payload: Option<MappedMap>,
    pub issues: Vec<ValidationIssue>,
    pub content_type: String,
}

// ---- shared coercions ---------------------------------------------------------------------

/// `INTEGER` coercion: optional sign, then ASCII digits only.
pub fn parse_integer(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<i64>().ok()
}

/// `DECIMAL` coercion: validate the plain decimal grammar
/// `[sign] (digits [. digits] | . digits) [eE [sign] digits]` and return the literal
/// unchanged (scale-preserving).
pub fn parse_decimal(raw: &str) -> Option<String> {
    let s = raw.trim();
    let b = s.as_bytes();
    let mut i = 0usize;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let mut int_digits = 0usize;
    while i < b.len() && b[i].is_ascii_digit() {
        int_digits += 1;
        i += 1;
    }
    let mut frac_digits = 0usize;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            frac_digits += 1;
            i += 1;
        }
    }
    if int_digits == 0 && frac_digits == 0 {
        return None;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let mut exp_digits = 0usize;
        while i < b.len() && b[i].is_ascii_digit() {
            exp_digits += 1;
            i += 1;
        }
        if exp_digits == 0 {
            return None;
        }
    }
    if i != b.len() {
        return None;
    }
    Some(s.to_string())
}

/// `DATE_CCYYMMDD` coercion: exactly 8 digits; the month must be 1–12 and the day 1–31, a
/// day past the month's end resolving to the month's last day (lenient day-of-month
/// resolution, matching the smart-resolver behaviour the reference fixtures pin).
pub fn parse_date_ccyymmdd(raw: &str) -> Option<(i32, u8, u8)> {
    let s = raw.trim();
    if s.len() != 8 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i32 = s[0..4].parse().ok()?;
    let month: u8 = s[4..6].parse().ok()?;
    let day: u8 = s[6..8].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some((year, month, day.min(days_in_month(year, month))))
}

/// `DATETIME_CCYYMMDDHHMM` coercion: exactly 12 digits; date rules as
/// [`parse_date_ccyymmdd`], hour 0–23, minute 0–59.
pub fn parse_datetime_ccyymmddhhmm(raw: &str) -> Option<(i32, u8, u8, u8, u8)> {
    let s = raw.trim();
    if s.len() != 12 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (year, month, day) = parse_date_ccyymmdd(&s[0..8])?;
    let (hour, minute) = parse_time_hhmm(&s[8..12])?;
    Some((year, month, day, hour, minute))
}

/// `TIME_HHMM` coercion: exactly 4 digits, hour 0–23, minute 0–59.
pub fn parse_time_hhmm(raw: &str) -> Option<(u8, u8)> {
    let s = raw.trim();
    if s.len() != 4 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hour: u8 = s[0..2].parse().ok()?;
    let minute: u8 = s[2..4].parse().ok()?;
    if hour > 23 || minute > 59 {
        return None;
    }
    Some((hour, minute))
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}
