//! The mapping seam between a live FEEL value and its persisted form.
//!
//! `sutra-feel` owns the runtime value model and `sutra-persistence` owns the durable one; neither
//! depends on the other (the persistence layer sits below the expression engine, and the expression
//! engine has no business knowing what a snapshot is). This crate is where both are in scope, so
//! this is where they meet — one function each way, and the round-trip contract between them.
//!
//! **Round-trip contract.** For every value a snapshot can carry,
//! `to_feel(&to_snapshot(v)) == v`. The temporal family satisfies it through TEXT: the persisted
//! form holds the canonical FEEL literal, and the FEEL layer parses exactly what it formats.
//!
//! **What degrades, and why.** A FEEL *function* closes over an evaluation context that ceases to
//! exist the moment the instance parks, and a *range* is a comparison shape rather than instance
//! state. Both persist as their canonical string — which is precisely what they did when every
//! variable was a string, so nothing regressed; they simply did not become typed.

use std::collections::BTreeMap;

use sutra_feel::value::canonical_string_of;
use sutra_feel::FeelValue;
use sutra_persistence::value::SnapshotValue;

/// A live FEEL value as the snapshot model records it.
pub fn to_snapshot(value: &FeelValue) -> SnapshotValue {
    match value {
        FeelValue::Null => SnapshotValue::Null,
        FeelValue::Boolean(b) => SnapshotValue::Boolean(*b),
        FeelValue::Number(n) => SnapshotValue::Number(n.clone()),
        FeelValue::String(s) => SnapshotValue::String(s.clone()),
        FeelValue::Date(_) => SnapshotValue::Date(canonical_string_of(value)),
        FeelValue::Time(..) => SnapshotValue::Time(canonical_string_of(value)),
        FeelValue::Instant(..) => SnapshotValue::DateTime(canonical_string_of(value)),
        FeelValue::Duration(_) => SnapshotValue::Duration(canonical_string_of(value)),
        FeelValue::List(items) => SnapshotValue::List(items.iter().map(to_snapshot).collect()),
        FeelValue::Map(entries) => SnapshotValue::Context(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), to_snapshot(v)))
                .collect(),
        ),
        FeelValue::Function(_) | FeelValue::Invocable(_) | FeelValue::Range(_) => {
            SnapshotValue::String(canonical_string_of(value))
        }
    }
}

/// A persisted value as the executor and FEEL see it again after the resume.
///
/// Never fails: a temporal literal this build cannot parse degrades to its text rather than
/// aborting a resume, matching the codec's decode contract one layer down.
pub fn to_feel(value: &SnapshotValue) -> FeelValue {
    match value {
        SnapshotValue::Null => FeelValue::Null,
        SnapshotValue::Boolean(b) => FeelValue::Boolean(*b),
        SnapshotValue::Number(n) => FeelValue::Number(n.clone()),
        SnapshotValue::String(s) => FeelValue::String(s.clone()),
        SnapshotValue::Date(text) => temporal(text, |v| matches!(v, FeelValue::Date(_))),
        SnapshotValue::Time(text) => temporal(text, |v| matches!(v, FeelValue::Time(..))),
        SnapshotValue::DateTime(text) => temporal(text, |v| matches!(v, FeelValue::Instant(..))),
        SnapshotValue::Duration(text) => temporal(text, |v| matches!(v, FeelValue::Duration(_))),
        SnapshotValue::List(items) => FeelValue::List(items.iter().map(to_feel).collect()),
        SnapshotValue::Context(entries) => FeelValue::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), to_feel(v)))
                .collect(),
        ),
    }
}

/// Whole-map convenience for the park/resume paths.
pub fn to_snapshot_map<'a>(
    values: impl IntoIterator<Item = (&'a String, &'a FeelValue)>,
) -> BTreeMap<String, SnapshotValue> {
    values
        .into_iter()
        .map(|(name, value)| (name.clone(), to_snapshot(value)))
        .collect()
}

/// Parse a persisted temporal literal back to its FEEL value, insisting the KIND matches the tag it
/// was stored under — a `date` literal that somehow parses as a time is corrupt, not a time.
fn temporal(text: &str, is_expected_kind: fn(&FeelValue) -> bool) -> FeelValue {
    match sutra_feel::temporal::parse_at_literal(text) {
        Some(value) if is_expected_kind(&value) => value,
        _ => FeelValue::String(text.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_feel::value::TimeQualifier;
    use time::macros::{date, datetime, time};

    fn round_trip(value: FeelValue) {
        let persisted = to_snapshot(&value);
        assert_eq!(to_feel(&persisted), value, "round trip of {value:?}");
    }

    #[test]
    fn every_persistable_feel_value_round_trips() {
        round_trip(FeelValue::Null);
        round_trip(FeelValue::Boolean(true));
        round_trip(FeelValue::num("100.00"));
        round_trip(FeelValue::String("INB-7".to_owned()));
        round_trip(FeelValue::Date(date!(2026 - 08 - 05)));
        round_trip(FeelValue::Time(time!(13:45:00), None));
        round_trip(FeelValue::Instant(datetime!(2026-08-05 13:45:00 UTC), None));
        round_trip(FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::String("two".to_owned()),
            FeelValue::Null,
        ]));
        round_trip(FeelValue::Map(BTreeMap::from([
            ("amount".to_owned(), FeelValue::num("9.99")),
            ("when".to_owned(), FeelValue::Date(date!(2026 - 01 - 01))),
        ])));
    }

    #[test]
    fn a_zoned_instant_keeps_its_qualifier_across_the_persisted_text() {
        // The qualifier is what makes the value DST-correct; losing it would silently re-express
        // the instant in UTC on the far side of a wait.
        let value =
            sutra_feel::temporal::parse_at_literal("2026-08-05T13:45:00@Europe/Paris").unwrap();
        assert!(matches!(
            &value,
            FeelValue::Instant(_, Some(TimeQualifier::Zone(z))) if z == "Europe/Paris"
        ));
        let persisted = to_snapshot(&value);
        assert!(
            matches!(&persisted, SnapshotValue::DateTime(t) if t.contains("Europe/Paris")),
            "{persisted:?}"
        );
        assert_eq!(to_feel(&persisted), value);
    }

    #[test]
    fn an_offset_instant_round_trips_its_own_offset_not_utc() {
        let value = sutra_feel::temporal::parse_at_literal("2026-08-05T13:45:00+05:30").unwrap();
        let persisted = to_snapshot(&value);
        assert_eq!(
            persisted,
            SnapshotValue::DateTime("2026-08-05T13:45:00+05:30".to_owned())
        );
        assert_eq!(to_feel(&persisted), value);
    }

    #[test]
    fn a_duration_round_trips_both_families() {
        for literal in ["P1Y2M", "P3DT4H5M", "-P1D"] {
            let value = sutra_feel::temporal::parse_at_literal(literal).unwrap();
            round_trip(value);
        }
    }

    #[test]
    fn a_function_or_range_degrades_to_the_string_it_always_was() {
        let range = sutra_feel::expressions::eval("[1..5]", &BTreeMap::new()).unwrap();
        assert!(matches!(range, FeelValue::Range(_)));
        assert_eq!(
            to_snapshot(&range),
            SnapshotValue::String(canonical_string_of(&range))
        );
    }

    #[test]
    fn a_corrupt_temporal_literal_degrades_instead_of_failing_a_resume() {
        assert_eq!(
            to_feel(&SnapshotValue::Date("not-a-date".to_owned())),
            FeelValue::String("not-a-date".to_owned())
        );
        // A literal that parses, but not as the kind it was stored under, is corrupt too.
        assert_eq!(
            to_feel(&SnapshotValue::Date("13:45:00".to_owned())),
            FeelValue::String("13:45:00".to_owned())
        );
    }

    #[test]
    fn the_persisted_canonical_string_matches_the_feel_formatter() {
        // The admin inspect projection and the GDPR blind index are both defined over the
        // pre-typing display string. The persistence layer reimplements that formatter (it cannot
        // depend on FEEL), so the two are pinned against each other here.
        for value in [
            FeelValue::Null,
            FeelValue::Boolean(false),
            FeelValue::num("42"),
            FeelValue::num("100.00"),
            FeelValue::String("plain".to_owned()),
            FeelValue::Date(date!(2026 - 08 - 05)),
            FeelValue::Instant(datetime!(2026-08-05 13:45:00 UTC), None),
            FeelValue::List(vec![FeelValue::num("1"), FeelValue::String("a".to_owned())]),
            FeelValue::Map(BTreeMap::from([("k".to_owned(), FeelValue::Boolean(true))])),
        ] {
            assert_eq!(
                to_snapshot(&value).to_canonical_string(),
                canonical_string_of(&value),
                "display form drifted for {value:?}"
            );
        }
    }
}
