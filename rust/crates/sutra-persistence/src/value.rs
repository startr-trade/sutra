//! The TYPED snapshot value model — what one persisted user variable *is*, and how it rides the
//! Properties-line container (see [`crate::props`]) without losing its type across a wait state.
//!
//! Before this model every variable was flattened to its display string at park and restored as a
//! `String` at resume, so a gateway re-evaluating `amount > 100` after a wait compared a STRING
//! against a number and FEEL (correctly, it never coerces) answered `null` ⇒ `false`. The model
//! here is the durable half of the fix; the executor-side half is the `FeelValue` mapping at the
//! engine bridge.
//!
//! # Wire form (snapshot v4)
//!
//! A v4 variable value is `<tag>|<payload>` — one ASCII tag byte, the [`TAG_SEP`] separator, then
//! the payload. Only the FIRST separator is structural, so a string payload that itself contains
//! `|` (or looks like another tag) round-trips unambiguously.
//!
//! | tag | value | payload |
//! |---|---|---|
//! | `z` | null | empty |
//! | `b` | boolean | `true` / `false` |
//! | `n` | number | canonical decimal text (scale-faithful) |
//! | `s` | string | the raw text |
//! | `d` | date | ISO-8601 date |
//! | `t` | time | FEEL `time` literal body |
//! | `i` | date and time | FEEL `date and time` literal body |
//! | `u` | duration | ISO-8601 duration |
//! | `j` | list / context | JSON (see below) |
//!
//! `|` is deliberate: the Properties-line writer escapes `=`, `:`, `#`, `!`, backslash and every
//! non-printable-ASCII code unit, but NOT `|`, so a tagged scalar adds exactly two bytes to the
//! persisted line and stays readable to an operator eyeballing a row.
//!
//! Nested values (`j`) ride JSON — the container is already `serde_json`-shaped elsewhere in this
//! crate, and JSON gives lists/contexts for free. The four temporal types have no JSON counterpart,
//! so they ride a single-key object: `{"@d":…}`, `{"@t":…}`, `{"@i":…}`, `{"@u":…}`. A user context
//! key that would collide (any key starting `@`) is escaped by DOUBLING its leading `@`, so
//! `{"@d": x}` as real user data encodes as `{"@@d": x}` and decodes back — the two are never
//! confusable.
//!
//! # Decode leniency
//!
//! In a v4 snapshot an unknown tag, or a value with no separator at all, decodes as
//! [`SnapshotValue::String`] of the whole raw value. That mirrors the rest of the codec's decode
//! contract (a malformed counter reads as zero rather than failing a resume): a snapshot must never
//! become unloadable, and the worst case here is the pre-v4 behaviour for that one variable.
//!
//! v2 / v3 snapshots are not tag-decoded AT ALL — every value is a `String`, byte for byte what it
//! was. Version detection at decode is the whole compatibility story; there is no migration.

use std::collections::BTreeMap;

use bigdecimal::BigDecimal;
use serde_json::Value as Json;

/// The tag/payload separator of the v4 value form. Chosen because the Properties-line writer does
/// not escape it (see the module docs).
pub const TAG_SEP: char = '|';

const TAG_NULL: char = 'z';
const TAG_BOOLEAN: char = 'b';
const TAG_NUMBER: char = 'n';
const TAG_STRING: char = 's';
const TAG_DATE: char = 'd';
const TAG_TIME: char = 't';
const TAG_DATE_TIME: char = 'i';
const TAG_DURATION: char = 'u';
const TAG_JSON: char = 'j';

/// The JSON envelope keys of the four temporal types (see the module docs).
const JSON_DATE: &str = "@d";
const JSON_TIME: &str = "@t";
const JSON_DATE_TIME: &str = "@i";
const JSON_DURATION: &str = "@u";

/// One persisted user variable, typed.
///
/// The variants mirror the value kinds FEEL distinguishes and a snapshot can honestly carry.
/// Deliberately NOT a re-export of the FEEL value type: this is the PERSISTED model, it must stay
/// a closed, serialisable set, and this crate is below the expression engine in the graph. The
/// mapping between the two lives at the engine bridge, which owns both.
///
/// The four temporal types carry their canonical FEEL literal text rather than a decomposed
/// calendar struct. The text is the round-trip contract (the FEEL layer parses exactly what it
/// formats), it is what an operator reads out of a row, and it keeps time-zone/qualifier semantics
/// where they belong — in the expression engine, not in the persistence layer.
///
/// Not representable, by design: FEEL functions and ranges. A function value closes over an
/// evaluation context that no longer exists after a wait, and a range is a comparison shape rather
/// than instance state; both degrade to their canonical string at the mapping seam, which is
/// exactly what they did before typing existed.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotValue {
    Null,
    Boolean(bool),
    Number(BigDecimal),
    String(String),
    /// FEEL `date` — ISO-8601 calendar date text.
    Date(String),
    /// FEEL `time` — literal body, offset/zone qualifier included when the value carried one.
    Time(String),
    /// FEEL `date and time` — literal body, qualifier included as above.
    DateTime(String),
    /// FEEL `days and time duration` / `years and months duration` — ISO-8601 duration text.
    Duration(String),
    List(Vec<SnapshotValue>),
    /// A FEEL context (nested object).
    Context(BTreeMap<String, SnapshotValue>),
}

impl SnapshotValue {
    /// True when this value needs the v4 typed wire form. A snapshot whose every variable is a
    /// plain string writes the v2/v3 bytes it always did — that is what keeps the golden-bytes
    /// corpus and every already-persisted row untouched by typing.
    pub fn needs_typed_encoding(&self) -> bool {
        !matches!(self, SnapshotValue::String(_))
    }

    /// The raw text of a [`SnapshotValue::String`]; `None` for every other kind.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SnapshotValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// The display string this value had before typing existed — the FEEL canonical string form.
    ///
    /// Load-bearing beyond nostalgia: the admin inspect projection and the GDPR subject blind
    /// index are both defined over it, so both must keep producing byte-identical output for a
    /// value that used to be persisted as this string. A unit test at the engine bridge pins this
    /// against the FEEL formatter it mirrors.
    ///
    /// A number renders here through the plain decimal `to_string`, NOT the scale-faithful
    /// [`canonical_decimal_string`] the wire form uses: the FEEL display formatter prints a
    /// scale-2 zero as `0`, and this method's whole job is to reproduce the display form exactly.
    /// The wire form keeps the scale because it has to reconstruct the value; the display form
    /// keeps the drift because it has to reproduce a projection.
    pub fn to_canonical_string(&self) -> String {
        match self {
            SnapshotValue::Null => "null".to_owned(),
            SnapshotValue::Boolean(b) => b.to_string(),
            SnapshotValue::Number(n) => n.to_string(),
            SnapshotValue::String(s) => s.clone(),
            SnapshotValue::Date(s)
            | SnapshotValue::Time(s)
            | SnapshotValue::DateTime(s)
            | SnapshotValue::Duration(s) => s.clone(),
            SnapshotValue::List(items) => {
                let inner: Vec<String> = items.iter().map(Self::to_canonical_string).collect();
                format!("[{}]", inner.join(", "))
            }
            SnapshotValue::Context(entries) => {
                let inner: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v.to_canonical_string()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
        }
    }

    /// Encode to the v4 tagged wire form.
    pub fn encode(&self) -> String {
        let (tag, payload) = match self {
            SnapshotValue::Null => (TAG_NULL, String::new()),
            SnapshotValue::Boolean(b) => (TAG_BOOLEAN, b.to_string()),
            SnapshotValue::Number(n) => (TAG_NUMBER, canonical_decimal_string(n)),
            SnapshotValue::String(s) => (TAG_STRING, s.clone()),
            SnapshotValue::Date(s) => (TAG_DATE, s.clone()),
            SnapshotValue::Time(s) => (TAG_TIME, s.clone()),
            SnapshotValue::DateTime(s) => (TAG_DATE_TIME, s.clone()),
            SnapshotValue::Duration(s) => (TAG_DURATION, s.clone()),
            SnapshotValue::List(_) | SnapshotValue::Context(_) => {
                (TAG_JSON, self.to_json().to_string())
            }
        };
        format!("{tag}{TAG_SEP}{payload}")
    }

    /// Decode one v4 tagged value. Never fails — see the module's decode-leniency contract.
    pub fn decode(raw: &str) -> SnapshotValue {
        let mut chars = raw.chars();
        let (Some(tag), Some(TAG_SEP)) = (chars.next(), chars.next()) else {
            return SnapshotValue::String(raw.to_owned());
        };
        // Byte arithmetic is safe: both leading chars matched ASCII literals above.
        let payload = &raw[tag.len_utf8() + TAG_SEP.len_utf8()..];
        match tag {
            TAG_NULL => SnapshotValue::Null,
            TAG_BOOLEAN => match payload {
                "true" => SnapshotValue::Boolean(true),
                "false" => SnapshotValue::Boolean(false),
                _ => SnapshotValue::String(raw.to_owned()),
            },
            TAG_NUMBER => match payload.parse::<BigDecimal>() {
                Ok(n) => SnapshotValue::Number(n),
                Err(_) => SnapshotValue::String(raw.to_owned()),
            },
            TAG_STRING => SnapshotValue::String(payload.to_owned()),
            TAG_DATE => SnapshotValue::Date(payload.to_owned()),
            TAG_TIME => SnapshotValue::Time(payload.to_owned()),
            TAG_DATE_TIME => SnapshotValue::DateTime(payload.to_owned()),
            TAG_DURATION => SnapshotValue::Duration(payload.to_owned()),
            TAG_JSON => match serde_json::from_str::<Json>(payload) {
                Ok(json @ (Json::Array(_) | Json::Object(_))) => Self::from_json(&json),
                _ => SnapshotValue::String(raw.to_owned()),
            },
            _ => SnapshotValue::String(raw.to_owned()),
        }
    }

    /// The JSON element form used inside a `j|` payload (see the module docs).
    fn to_json(&self) -> Json {
        match self {
            SnapshotValue::Null => Json::Null,
            SnapshotValue::Boolean(b) => Json::Bool(*b),
            // `arbitrary_precision` is on workspace-wide, so the exact decimal text (and with it
            // the FEEL-visible scale of `100.00`) survives the JSON hop unrounded.
            SnapshotValue::Number(n) => {
                serde_json::Number::from_string_unchecked(canonical_decimal_string(n)).into()
            }
            SnapshotValue::String(s) => Json::String(s.clone()),
            SnapshotValue::Date(s) => temporal_json(JSON_DATE, s),
            SnapshotValue::Time(s) => temporal_json(JSON_TIME, s),
            SnapshotValue::DateTime(s) => temporal_json(JSON_DATE_TIME, s),
            SnapshotValue::Duration(s) => temporal_json(JSON_DURATION, s),
            SnapshotValue::List(items) => Json::Array(items.iter().map(Self::to_json).collect()),
            SnapshotValue::Context(entries) => Json::Object(
                entries
                    .iter()
                    .map(|(k, v)| (escape_context_key(k), v.to_json()))
                    .collect(),
            ),
        }
    }

    /// Inverse of [`to_json`](Self::to_json).
    fn from_json(json: &Json) -> SnapshotValue {
        match json {
            Json::Null => SnapshotValue::Null,
            Json::Bool(b) => SnapshotValue::Boolean(*b),
            Json::Number(n) => match n.to_string().parse::<BigDecimal>() {
                Ok(d) => SnapshotValue::Number(d),
                Err(_) => SnapshotValue::String(n.to_string()),
            },
            Json::String(s) => SnapshotValue::String(s.clone()),
            Json::Array(items) => SnapshotValue::List(items.iter().map(Self::from_json).collect()),
            Json::Object(map) => {
                if map.len() == 1 {
                    if let Some((key, Json::String(text))) = map.iter().next() {
                        match key.as_str() {
                            JSON_DATE => return SnapshotValue::Date(text.clone()),
                            JSON_TIME => return SnapshotValue::Time(text.clone()),
                            JSON_DATE_TIME => return SnapshotValue::DateTime(text.clone()),
                            JSON_DURATION => return SnapshotValue::Duration(text.clone()),
                            _ => {}
                        }
                    }
                }
                SnapshotValue::Context(
                    map.iter()
                        .map(|(k, v)| (unescape_context_key(k), Self::from_json(v)))
                        .collect(),
                )
            }
        }
    }
}

impl From<String> for SnapshotValue {
    fn from(s: String) -> Self {
        SnapshotValue::String(s)
    }
}

impl From<&str> for SnapshotValue {
    fn from(s: &str) -> Self {
        SnapshotValue::String(s.to_owned())
    }
}

fn temporal_json(key: &str, text: &str) -> Json {
    let mut map = serde_json::Map::new();
    map.insert(key.to_owned(), Json::String(text.to_owned()));
    Json::Object(map)
}

/// A user context key is escaped by doubling a leading `@`, so it can never be mistaken for one of
/// the temporal envelope keys.
fn escape_context_key(key: &str) -> String {
    match key.strip_prefix('@') {
        Some(rest) => format!("@@{rest}"),
        None => key.to_owned(),
    }
}

fn unescape_context_key(key: &str) -> String {
    match key.strip_prefix("@@") {
        Some(rest) => format!("@{rest}"),
        None => key.to_owned(),
    }
}

/// Scale-faithful decimal text. The `bigdecimal` crate normalises ZERO to `"0"` whatever its
/// scale, which would silently re-scale a computed `0.00` across a wait; every other value already
/// prints scale-faithfully. Mirrors the executor's identical fix-up on the JSON path — the two must
/// agree, or a value's text would change depending on which layer rendered it.
fn canonical_decimal_string(n: &BigDecimal) -> String {
    let s = n.to_string();
    if s == "0" {
        let (_, exponent) = n.as_bigint_and_exponent();
        if exponent > 0 {
            return format!("0.{}", "0".repeat(exponent as usize));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(v: SnapshotValue) {
        let encoded = v.encode();
        let decoded = SnapshotValue::decode(&encoded);
        assert_eq!(decoded, v, "round trip of {encoded}");
        assert_eq!(decoded.encode(), encoded, "re-encode is byte-identical");
    }

    #[test]
    fn every_scalar_kind_round_trips() {
        round_trip(SnapshotValue::Null);
        round_trip(SnapshotValue::Boolean(true));
        round_trip(SnapshotValue::Boolean(false));
        round_trip(SnapshotValue::Number("42".parse().unwrap()));
        round_trip(SnapshotValue::Number("-17.500".parse().unwrap()));
        round_trip(SnapshotValue::String("INB-7".to_owned()));
        round_trip(SnapshotValue::String(String::new()));
        round_trip(SnapshotValue::Date("2026-08-05".to_owned()));
        round_trip(SnapshotValue::Time("13:45:00@Europe/Paris".to_owned()));
        round_trip(SnapshotValue::DateTime("2026-08-05T13:45:00Z".to_owned()));
        round_trip(SnapshotValue::Duration("P1DT2H".to_owned()));
    }

    #[test]
    fn a_number_keeps_its_scale_across_the_wire() {
        // FEEL prints `100.00` as `100.00`; a re-scaled value would change a rendered total after
        // a wait state.
        let v = SnapshotValue::Number("100.00".parse().unwrap());
        assert_eq!(v.encode(), "n|100.00");
        assert_eq!(
            SnapshotValue::decode("n|100.00").to_canonical_string(),
            "100.00"
        );
        // The zero edge case the bigdecimal crate normalises away.
        assert_eq!(
            SnapshotValue::Number("0.00".parse().unwrap()).encode(),
            "n|0.00"
        );
    }

    #[test]
    fn a_string_that_looks_like_another_tag_survives() {
        // Only the FIRST separator is structural.
        round_trip(SnapshotValue::String("n|42".to_owned()));
        round_trip(SnapshotValue::String("|".to_owned()));
        round_trip(SnapshotValue::String("a|b|c".to_owned()));
        assert_eq!(
            SnapshotValue::decode("s|n|42"),
            SnapshotValue::String("n|42".to_owned())
        );
    }

    #[test]
    fn lists_and_contexts_round_trip_with_their_element_types() {
        round_trip(SnapshotValue::List(vec![
            SnapshotValue::Number("1".parse().unwrap()),
            SnapshotValue::String("two".to_owned()),
            SnapshotValue::Boolean(false),
            SnapshotValue::Null,
            SnapshotValue::Date("2026-01-01".to_owned()),
        ]));
        round_trip(SnapshotValue::Context(BTreeMap::from([
            (
                "amount".to_owned(),
                SnapshotValue::Number("9.99".parse().unwrap()),
            ),
            (
                "currency".to_owned(),
                SnapshotValue::String("EUR".to_owned()),
            ),
            (
                "nested".to_owned(),
                SnapshotValue::List(vec![SnapshotValue::Context(BTreeMap::from([(
                    "deep".to_owned(),
                    SnapshotValue::Boolean(true),
                )]))]),
            ),
        ])));
        round_trip(SnapshotValue::List(Vec::new()));
        round_trip(SnapshotValue::Context(BTreeMap::new()));
    }

    #[test]
    fn a_context_key_that_collides_with_a_temporal_envelope_is_escaped() {
        let user = SnapshotValue::Context(BTreeMap::from([(
            "@d".to_owned(),
            SnapshotValue::String("not a date".to_owned()),
        )]));
        assert_eq!(user.encode(), "j|{\"@@d\":\"not a date\"}");
        round_trip(user);
        // …and the real envelope still decodes as the temporal value it is.
        assert_eq!(
            SnapshotValue::decode("j|[{\"@d\":\"2026-01-01\"}]"),
            SnapshotValue::List(vec![SnapshotValue::Date("2026-01-01".to_owned())])
        );
    }

    #[test]
    fn context_encoding_is_key_sorted_and_deterministic() {
        let a = SnapshotValue::Context(BTreeMap::from([
            ("b".to_owned(), SnapshotValue::Boolean(true)),
            ("a".to_owned(), SnapshotValue::Boolean(false)),
        ]));
        assert_eq!(a.encode(), "j|{\"a\":false,\"b\":true}");
    }

    #[test]
    fn a_malformed_or_untagged_value_reads_as_the_raw_string() {
        // The decode-leniency contract: a snapshot never becomes unloadable over one variable.
        assert_eq!(
            SnapshotValue::decode("plain text"),
            SnapshotValue::String("plain text".to_owned())
        );
        assert_eq!(
            SnapshotValue::decode("q|unknown tag"),
            SnapshotValue::String("q|unknown tag".to_owned())
        );
        assert_eq!(
            SnapshotValue::decode("n|not-a-number"),
            SnapshotValue::String("n|not-a-number".to_owned())
        );
        assert_eq!(
            SnapshotValue::decode("b|yes"),
            SnapshotValue::String("b|yes".to_owned())
        );
        assert_eq!(
            SnapshotValue::decode("j|{"),
            SnapshotValue::String("j|{".to_owned())
        );
        // A `j|` payload that parses but is not a structure is not a list/context either.
        assert_eq!(
            SnapshotValue::decode("j|42"),
            SnapshotValue::String("j|42".to_owned())
        );
        assert_eq!(
            SnapshotValue::decode(""),
            SnapshotValue::String(String::new())
        );
    }

    #[test]
    fn only_a_non_string_needs_the_typed_encoding() {
        assert!(!SnapshotValue::String("x".to_owned()).needs_typed_encoding());
        assert!(SnapshotValue::Null.needs_typed_encoding());
        assert!(SnapshotValue::Number("1".parse().unwrap()).needs_typed_encoding());
        assert!(SnapshotValue::List(Vec::new()).needs_typed_encoding());
    }

    #[test]
    fn the_canonical_string_matches_the_pre_typing_display_form() {
        assert_eq!(SnapshotValue::Null.to_canonical_string(), "null");
        assert_eq!(SnapshotValue::Boolean(true).to_canonical_string(), "true");
        assert_eq!(
            SnapshotValue::Number("42".parse().unwrap()).to_canonical_string(),
            "42"
        );
        // The display form deliberately keeps the FEEL formatter's zero normalisation, while the
        // WIRE form keeps the scale — the two answer different questions.
        let scaled_zero = SnapshotValue::Number("0.00".parse().unwrap());
        assert_eq!(scaled_zero.to_canonical_string(), "0");
        assert_eq!(scaled_zero.encode(), "n|0.00");
        assert_eq!(
            SnapshotValue::List(vec![
                SnapshotValue::Number("1".parse().unwrap()),
                SnapshotValue::String("a".to_owned())
            ])
            .to_canonical_string(),
            "[1, a]"
        );
        assert_eq!(
            SnapshotValue::Context(BTreeMap::from([(
                "k".to_owned(),
                SnapshotValue::Boolean(false)
            )]))
            .to_canonical_string(),
            "{k=false}"
        );
    }
}
