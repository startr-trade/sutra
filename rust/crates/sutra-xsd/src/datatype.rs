//! Builtin datatype lexical checks — the Tier-1 profile: `string`, `decimal`,
//! `boolean`, `date`, `dateTime`, `time`, `gYear`, `gYearMonth`, `base64Binary`, and
//! the integer family. Lexical/value checks only (calendar validity included; no
//! cross-value datetime arithmetic).
//!
//! Whitespace handling follows the builtin's whiteSpace disposition: `string` preserves,
//! everything else collapses (leading/trailing stripped, internal runs become one
//! space) before the lexical check and before facet checks.

/// A supported builtin datatype (the restriction/extension bases and element types the
/// runtime-validated schema corpus uses, plus the integer family for module authoring).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builtin {
    String,
    Decimal,
    Boolean,
    Date,
    DateTime,
    Time,
    GYear,
    GYearMonth,
    Base64Binary,
    Integer,
    NonNegativeInteger,
    NonPositiveInteger,
    PositiveInteger,
    NegativeInteger,
    Long,
    Int,
    Short,
    Byte,
    UnsignedLong,
    UnsignedInt,
    UnsignedShort,
    UnsignedByte,
}

impl Builtin {
    /// Resolve an XSD-namespace local type name to a supported builtin, or `None` when
    /// the name is not in the supported subset (the compiler turns that into a finding).
    pub fn by_name(local: &str) -> Option<Builtin> {
        Some(match local {
            "string" => Builtin::String,
            "decimal" => Builtin::Decimal,
            "boolean" => Builtin::Boolean,
            "date" => Builtin::Date,
            "dateTime" => Builtin::DateTime,
            "time" => Builtin::Time,
            "gYear" => Builtin::GYear,
            "gYearMonth" => Builtin::GYearMonth,
            "base64Binary" => Builtin::Base64Binary,
            "integer" => Builtin::Integer,
            "nonNegativeInteger" => Builtin::NonNegativeInteger,
            "nonPositiveInteger" => Builtin::NonPositiveInteger,
            "positiveInteger" => Builtin::PositiveInteger,
            "negativeInteger" => Builtin::NegativeInteger,
            "long" => Builtin::Long,
            "int" => Builtin::Int,
            "short" => Builtin::Short,
            "byte" => Builtin::Byte,
            "unsignedLong" => Builtin::UnsignedLong,
            "unsignedInt" => Builtin::UnsignedInt,
            "unsignedShort" => Builtin::UnsignedShort,
            "unsignedByte" => Builtin::UnsignedByte,
            _ => None?,
        })
    }

    /// The XSD local name (for messages).
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::String => "string",
            Builtin::Decimal => "decimal",
            Builtin::Boolean => "boolean",
            Builtin::Date => "date",
            Builtin::DateTime => "dateTime",
            Builtin::Time => "time",
            Builtin::GYear => "gYear",
            Builtin::GYearMonth => "gYearMonth",
            Builtin::Base64Binary => "base64Binary",
            Builtin::Integer => "integer",
            Builtin::NonNegativeInteger => "nonNegativeInteger",
            Builtin::NonPositiveInteger => "nonPositiveInteger",
            Builtin::PositiveInteger => "positiveInteger",
            Builtin::NegativeInteger => "negativeInteger",
            Builtin::Long => "long",
            Builtin::Int => "int",
            Builtin::Short => "short",
            Builtin::Byte => "byte",
            Builtin::UnsignedLong => "unsignedLong",
            Builtin::UnsignedInt => "unsignedInt",
            Builtin::UnsignedShort => "unsignedShort",
            Builtin::UnsignedByte => "unsignedByte",
        }
    }

    /// Whether this builtin is numeric (decimal or the integer family) — drives both
    /// decimal-facet applicability and the navigation-shape Number kind.
    pub fn is_numeric(&self) -> bool {
        !matches!(
            self,
            Builtin::String
                | Builtin::Boolean
                | Builtin::Date
                | Builtin::DateTime
                | Builtin::Time
                | Builtin::GYear
                | Builtin::GYearMonth
                | Builtin::Base64Binary
        )
    }

    /// Whitespace disposition: `string` preserves, everything else collapses.
    pub fn collapses_whitespace(&self) -> bool {
        !matches!(self, Builtin::String)
    }

    /// Apply this builtin's whitespace disposition to a raw value.
    pub fn normalize<'v>(&self, raw: &'v str) -> std::borrow::Cow<'v, str> {
        if !self.collapses_whitespace() {
            return std::borrow::Cow::Borrowed(raw);
        }
        let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        std::borrow::Cow::Owned(collapsed)
    }

    /// Lexical (and range, for bounded integers) check of a whitespace-normalized
    /// value. `Err` carries a short reason for the violation message.
    pub fn check(&self, value: &str) -> Result<(), String> {
        let ok = match self {
            Builtin::String => true,
            Builtin::Decimal => is_decimal(value),
            Builtin::Boolean => matches!(value, "true" | "false" | "1" | "0"),
            Builtin::Date => is_date(value),
            Builtin::DateTime => is_date_time(value),
            Builtin::Time => is_time(value),
            Builtin::GYear => is_g_year(value),
            Builtin::GYearMonth => is_g_year_month(value),
            Builtin::Base64Binary => is_base64(value),
            Builtin::Integer => is_integer(value),
            Builtin::NonNegativeInteger => integer_in(value, |s, _| s >= 0),
            Builtin::NonPositiveInteger => integer_in(value, |s, _| s <= 0),
            Builtin::PositiveInteger => integer_in(value, |s, z| s >= 0 && !z),
            Builtin::NegativeInteger => integer_in(value, |s, z| s <= 0 && !z),
            Builtin::Long => in_i128_range(value, i64::MIN as i128, i64::MAX as i128),
            Builtin::Int => in_i128_range(value, i32::MIN as i128, i32::MAX as i128),
            Builtin::Short => in_i128_range(value, i16::MIN as i128, i16::MAX as i128),
            Builtin::Byte => in_i128_range(value, i8::MIN as i128, i8::MAX as i128),
            Builtin::UnsignedLong => in_i128_range(value, 0, u64::MAX as i128),
            Builtin::UnsignedInt => in_i128_range(value, 0, u32::MAX as i128),
            Builtin::UnsignedShort => in_i128_range(value, 0, u16::MAX as i128),
            Builtin::UnsignedByte => in_i128_range(value, 0, u8::MAX as i128),
        };
        if ok {
            Ok(())
        } else {
            Err(format!(
                "'{value}' is not a valid value of type '{}'",
                self.name()
            ))
        }
    }
}

/// `(\+|-)?([0-9]+(\.[0-9]*)?|\.[0-9]+)` — no exponent, at least one digit.
fn is_decimal(v: &str) -> bool {
    let v = v.strip_prefix(['+', '-']).unwrap_or(v);
    let (int_part, frac_part) = match v.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (v, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    match frac_part {
        Some(f) => {
            (digits(int_part) || int_part.is_empty())
                && (digits(f) || f.is_empty())
                && !(int_part.is_empty() && f.is_empty())
        }
        None => digits(int_part),
    }
}

fn is_integer(v: &str) -> bool {
    let d = v.strip_prefix(['+', '-']).unwrap_or(v);
    !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit())
}

fn integer_in(v: &str, accept: impl Fn(i32, bool) -> bool) -> bool {
    if !is_integer(v) {
        return false;
    }
    let sign = if v.starts_with('-') { -1 } else { 1 };
    let digits = v.strip_prefix(['+', '-']).unwrap_or(v);
    let is_zero = digits.bytes().all(|b| b == b'0');
    // A negative zero is zero; sign checks treat it as such.
    accept(if is_zero { 0 } else { sign }, is_zero)
}

fn in_i128_range(v: &str, min: i128, max: i128) -> bool {
    is_integer(v) && v.parse::<i128>().is_ok_and(|n| n >= min && n <= max)
}

/// Split an optional timezone suffix: `Z` or `±hh:mm` with hh ≤ 13 (any minute) or
/// exactly 14:00.
fn split_timezone(v: &str) -> Option<&str> {
    if let Some(rest) = v.strip_suffix('Z') {
        return Some(rest);
    }
    if v.len() >= 6 {
        let (rest, tz) = v.split_at(v.len() - 6);
        let b = tz.as_bytes();
        if (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
            let hh = tz[1..3].parse::<u32>().ok()?;
            let mm = tz[4..6].parse::<u32>().ok()?;
            if tz[1..3].bytes().all(|c| c.is_ascii_digit())
                && tz[4..6].bytes().all(|c| c.is_ascii_digit())
                && (hh < 14 && mm <= 59 || hh == 14 && mm == 0)
            {
                return Some(rest);
            }
            return None;
        }
    }
    Some(v)
}

/// `(-)?YYYY(Y*)` — four or more digits, no leading zero beyond four, year ≠ 0000.
fn parse_year(v: &str) -> Option<()> {
    let digits = v.strip_prefix('-').unwrap_or(v);
    if digits.len() < 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if digits.len() > 4 && digits.starts_with('0') {
        return None;
    }
    if digits.bytes().all(|b| b == b'0') {
        return None;
    }
    Some(())
}

fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn two_digits(s: &str) -> Option<u32> {
    if s.len() == 2 && s.bytes().all(|b| b.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

/// `(-)?YYYY-MM-DD` (+ optional timezone), calendar-valid.
fn is_date(v: &str) -> bool {
    let Some(body) = split_timezone(v) else {
        return false;
    };
    date_fields(body).is_some()
}

fn date_fields(body: &str) -> Option<i64> {
    // Year may itself contain no '-' beyond a leading sign; split from the right.
    if body.len() < 10 {
        return None;
    }
    let (rest, dd) = body.split_at(body.len() - 3);
    let dd = dd.strip_prefix('-')?;
    let (year_part, mm) = rest.split_at(rest.len() - 3);
    let mm = mm.strip_prefix('-')?;
    parse_year(year_part)?;
    let year: i64 = year_part.parse().ok()?;
    let m = two_digits(mm)?;
    let d = two_digits(dd)?;
    if (1..=12).contains(&m) && d >= 1 && d <= days_in_month(year, m) {
        Some(year)
    } else {
        None
    }
}

/// `hh:mm:ss(.f+)?` — 24:00:00(.0*) allowed as the canonical end-of-day.
fn is_time_body(v: &str) -> bool {
    let b = v.as_bytes();
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return false;
    }
    let (Some(hh), Some(mm), Some(ss)) = (
        two_digits(&v[0..2]),
        two_digits(&v[3..5]),
        two_digits(&v[6..8]),
    ) else {
        return false;
    };
    let frac = &v[8..];
    let frac_ok = frac.is_empty()
        || (frac.starts_with('.')
            && frac.len() > 1
            && frac[1..].bytes().all(|c| c.is_ascii_digit()));
    if !frac_ok {
        return false;
    }
    if hh == 24 {
        return mm == 0 && ss == 0 && (frac.is_empty() || frac[1..].bytes().all(|c| c == b'0'));
    }
    hh <= 23 && mm <= 59 && ss <= 59
}

fn is_time(v: &str) -> bool {
    match split_timezone(v) {
        Some(body) => is_time_body(body),
        None => false,
    }
}

fn is_date_time(v: &str) -> bool {
    let Some(body) = split_timezone(v) else {
        return false;
    };
    match body.split_once('T') {
        Some((date, time)) => date_fields(date).is_some() && is_time_body(time),
        None => false,
    }
}

fn is_g_year(v: &str) -> bool {
    match split_timezone(v) {
        Some(body) => parse_year(body).is_some(),
        None => false,
    }
}

fn is_g_year_month(v: &str) -> bool {
    let Some(body) = split_timezone(v) else {
        return false;
    };
    if body.len() < 7 {
        return false;
    }
    let (year_part, mm) = body.split_at(body.len() - 3);
    let Some(mm) = mm.strip_prefix('-') else {
        return false;
    };
    parse_year(year_part).is_some() && two_digits(mm).is_some_and(|m| (1..=12).contains(&m))
}

/// Base64 with canonical padding. Whitespace was already collapsed to single spaces;
/// the base64 lexical space allows single spaces between quartet characters, so strip
/// all remaining spaces before checking.
fn is_base64(v: &str) -> bool {
    let compact: Vec<u8> = v.bytes().filter(|b| *b != b' ').collect();
    if compact.is_empty() {
        return true;
    }
    if !compact.len().is_multiple_of(4) {
        return false;
    }
    let is_b64 = |b: u8| b.is_ascii_alphanumeric() || b == b'+' || b == b'/';
    let pad = compact.iter().rev().take_while(|b| **b == b'=').count();
    if pad > 2 {
        return false;
    }
    let body = &compact[..compact.len() - pad];
    if !body.iter().all(|b| is_b64(*b)) {
        return false;
    }
    match pad {
        // ....== : the char before '==' carries 2 payload bits.
        2 => matches!(body.last(), Some(b'A' | b'Q' | b'g' | b'w')),
        // .....= : the char before '=' carries 4 payload bits.
        1 => matches!(
            body.last(),
            Some(
                b'A' | b'E'
                    | b'I'
                    | b'M'
                    | b'Q'
                    | b'U'
                    | b'Y'
                    | b'c'
                    | b'g'
                    | b'k'
                    | b'o'
                    | b's'
                    | b'w'
                    | b'0'
                    | b'4'
                    | b'8'
            )
        ),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_lexicals() {
        for ok in ["0", "100.00", "+.5", "1.", "-5.00", "007"] {
            assert!(Builtin::Decimal.check(ok).is_ok(), "{ok}");
        }
        for bad in ["", "12,34", "1e3", ".", "+", "1.2.3", "NaN"] {
            assert!(Builtin::Decimal.check(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn datetime_lexicals() {
        for ok in [
            "2026-05-22T10:00:00",
            "2026-05-22T24:00:00",
            "2026-05-22T10:00:00Z",
            "2026-05-22T10:00:00+05:30",
            "2026-05-22T10:00:00.123",
            "2024-02-29T00:00:00",
        ] {
            assert!(Builtin::DateTime.check(ok).is_ok(), "{ok}");
        }
        for bad in [
            "not-a-date",
            "2026-02-30T10:00:00",
            "2026-05-22",
            "2026-05-22T25:00:00",
            "2026-13-01T00:00:00",
            "2023-02-29T00:00:00",
            "2026-05-22T10:00:00+15:00",
        ] {
            assert!(Builtin::DateTime.check(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn date_time_gyear_lexicals() {
        assert!(Builtin::Date.check("2026-05-22").is_ok());
        assert!(Builtin::Date.check("2026-05-22Z").is_ok());
        assert!(Builtin::Date.check("2026-02-30").is_err());
        assert!(Builtin::Time.check("10:00:00").is_ok());
        assert!(Builtin::Time.check("10:60:00").is_err());
        assert!(Builtin::GYear.check("2026").is_ok());
        assert!(Builtin::GYear.check("-0044").is_ok());
        assert!(Builtin::GYear.check("26").is_err());
        assert!(Builtin::GYear.check("0000").is_err());
        assert!(Builtin::GYearMonth.check("2026-05").is_ok());
        assert!(Builtin::GYearMonth.check("2026-13").is_err());
    }

    #[test]
    fn base64_lexicals() {
        for ok in ["", "TWFu", "TWE=", "TQ==", "TWFu TWFu"] {
            assert!(Builtin::Base64Binary.check(ok).is_ok(), "{ok:?}");
        }
        for bad in ["TWF", "TR==", "T===", "TWFu!"] {
            assert!(Builtin::Base64Binary.check(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn integer_family() {
        assert!(Builtin::Integer.check("-99999999999999999999").is_ok());
        assert!(Builtin::Int.check("2147483647").is_ok());
        assert!(Builtin::Int.check("2147483648").is_err());
        assert!(Builtin::Long.check("1.0").is_err());
        assert!(Builtin::PositiveInteger.check("0").is_err());
        assert!(Builtin::NonNegativeInteger.check("-0").is_ok());
        assert!(Builtin::UnsignedByte.check("255").is_ok());
        assert!(Builtin::UnsignedByte.check("256").is_err());
    }

    #[test]
    fn whitespace_disposition() {
        assert_eq!(Builtin::Decimal.normalize("  100.00\n"), "100.00");
        assert_eq!(Builtin::String.normalize("  a  b "), "  a  b ");
        assert!(Builtin::Boolean
            .check(Builtin::Boolean.normalize(" true ").as_ref())
            .is_ok());
    }
}
