//! DMN 1.5 implicit type conversions as applied by `coerce_to_shape` — the function every
//! typeRef-bearing slot (decision result, context entry, function parameter and result,
//! decision-service output) coerces its value through: "to singleton list", "from singleton
//! list" and "date to date and time".

use sutra_feel::{coerce_to_shape, expressions, FeelContext, FeelTypeShape, FeelValue};

fn eval(text: &str) -> FeelValue {
    expressions::eval(text, &FeelContext::new()).expect("expression should evaluate")
}

fn base(name: &str) -> FeelTypeShape {
    FeelTypeShape::Base(name.to_string())
}

fn list_of(elem: FeelTypeShape) -> FeelTypeShape {
    FeelTypeShape::Collection(Box::new(elem))
}

#[test]
fn a_date_meeting_a_date_and_time_type_becomes_utc_midnight() {
    let expected = eval(r#"date and time("2000-01-02T00:00:00Z")"#);
    for spelling in ["date and time", "dateTime"] {
        let converted = coerce_to_shape(&eval("date(2000, 1, 2)"), &base(spelling))
            .unwrap_or_else(|| panic!("a date should conform to '{spelling}'"));
        assert_eq!(converted, expected, "target spelled '{spelling}'");
        // Explicit UTC, not a floating local value: the offset is part of the result.
        let FeelValue::Instant(dt, Some(_)) = &converted else {
            panic!("expected a UTC-qualified date and time, got {converted:?}");
        };
        assert!(dt.offset().is_utc());
    }
}

#[test]
fn a_date_and_time_value_is_left_unchanged() {
    let value = eval(r#"date and time("2000-01-02T10:30:00@Europe/Paris")"#);
    assert_eq!(coerce_to_shape(&value, &base("date and time")), Some(value));
}

#[test]
fn a_date_meeting_a_date_type_or_an_unrelated_type_is_not_converted() {
    let date = eval("date(2000, 1, 2)");
    assert_eq!(coerce_to_shape(&date, &base("date")), Some(date.clone()));
    assert_eq!(coerce_to_shape(&date, &base("string")), None);
    // A date and time is not implicitly narrowed to a date.
    let dt = eval(r#"date and time("2000-01-02T00:00:00Z")"#);
    assert_eq!(coerce_to_shape(&dt, &base("date")), None);
}

#[test]
fn date_to_date_and_time_applies_to_the_whole_value_only() {
    // Not chained with "from singleton list" ...
    let singleton = eval("[date(2000, 1, 2)]");
    assert_eq!(coerce_to_shape(&singleton, &base("date and time")), None);
    // ... nor with "to singleton list" ...
    assert_eq!(
        coerce_to_shape(&eval("date(2000, 1, 2)"), &list_of(base("date and time"))),
        None
    );
    // ... and a list<date> does not conform to list<date and time>.
    assert_eq!(
        coerce_to_shape(
            &eval("[date(2000, 1, 2), date(2000, 1, 3)]"),
            &list_of(base("date and time"))
        ),
        None
    );
}

#[test]
fn singleton_list_conversions_still_apply() {
    assert_eq!(
        coerce_to_shape(&eval(r#""abc""#), &list_of(base("string"))),
        Some(eval(r#"["abc"]"#))
    );
    assert_eq!(
        coerce_to_shape(&eval(r#"["abc"]"#), &base("string")),
        Some(eval(r#""abc""#))
    );
    assert_eq!(
        coerce_to_shape(&eval(r#"["abc", "bcd"]"#), &base("string")),
        None
    );
    // null conforms as-is, never converted.
    assert_eq!(
        coerce_to_shape(&FeelValue::Null, &base("date and time")),
        Some(FeelValue::Null)
    );
}
