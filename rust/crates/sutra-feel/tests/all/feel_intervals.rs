//! The 14 interval/point relation builtins (DMN 1.4 §10.3.4.6, Table 78): `before`, `after`,
//! `meets`, `met by`, `overlaps`, `overlaps before`, `overlaps after`, `finishes`,
//! `finished by`, `includes`, `during`, `starts`, `started by`, `coincides` — every function's
//! open/closed endpoint boundary combinations, the non-number ordered point types, and the
//! null/type-mismatch → null contract. Expectations follow the spec table's relation
//! conditions and DMN-TCK 1130-feel-interval's full expected-value set.

use sutra_feel::expressions;
use sutra_feel::{FeelContext, FeelValue};

fn eval_bool(expr: &str) -> FeelValue {
    expressions::eval(expr, &FeelContext::new()).unwrap()
}

fn assert_all(cases: &[(&str, bool)]) {
    for (expr, expected) in cases {
        assert_eq!(
            eval_bool(expr),
            FeelValue::Boolean(*expected),
            "expression: {expr}"
        );
    }
}

#[test]
fn interval_before() {
    assert_all(&[
        // point-point
        ("before(1, 10)", true),
        ("before(10, 1)", false),
        ("before(1, 1)", false),
        // point-range: equality at the start only counts when the start is open
        ("before(1, [1..10])", false),
        ("before(1, (1..10])", true),
        ("before(1, [5..10])", true),
        ("before(10, [1..10])", false),
        // range-point: equality at the end only counts when the end is open
        ("before([1..10], 10)", false),
        ("before([1..10), 10)", true),
        ("before([1..10], 15)", true),
        ("before([1..10], 1)", false),
        // range-range: touching ends must not BOTH be closed
        ("before([1..10], [15..20])", true),
        ("before([1..10], [10..20])", false),
        ("before([1..10), [10..20])", true),
        ("before([1..10], (10..20])", true),
        ("before([1..10), (10..20])", true),
        ("before([15..20], [1..10])", false),
    ]);
}

#[test]
fn interval_after() {
    assert_all(&[
        // point-point
        ("after(10, 5)", true),
        ("after(5, 10)", false),
        ("after(5, 5)", false),
        // point-range
        ("after(12, [1..10])", true),
        ("after(10, [1..10])", false),
        ("after(10, [1..10))", true),
        // range-point
        ("after([11..20], 10)", true),
        ("after([11..20], 11)", false),
        ("after((11..20], 11)", true),
        ("after([11..20], 12)", false),
        // range-range
        ("after([11..20], [1..10])", true),
        ("after([1..10], [11..20])", false),
        ("after([11..20], [1..11))", true),
        ("after((11..20], [1..11])", true),
        ("after([11..20], [1..11])", false),
    ]);
}

#[test]
fn interval_meets_and_met_by() {
    assert_all(&[
        // meets: range1.end and range2.start both closed and equal
        ("meets([1..5], [5..10])", true),
        ("meets([1..5), [5..10])", false),
        ("meets([1..5], (5..10])", false),
        ("meets([1..5], [6..10])", false),
        // met by: the converse
        ("met by([5..10], [1..5])", true),
        ("met by([5..10], [1..5))", false),
        ("met by((5..10], [1..5])", false),
        ("met by([6..10], [1..5])", false),
    ]);
}

#[test]
fn interval_overlaps() {
    assert_all(&[
        ("overlaps([1..5], [3..8])", true),
        ("overlaps([3..8], [1..5])", true),
        ("overlaps([1..8], [3..5])", true),
        ("overlaps([3..5], [1..8])", true),
        ("overlaps([1..5], [6..8])", false),
        ("overlaps([6..8], [1..5])", false),
        // single shared point: both touching endpoints must be closed
        ("overlaps([1..5], [5..8])", true),
        ("overlaps([1..5], (5..8])", false),
        ("overlaps([1..5), [5..8])", false),
        ("overlaps([1..5), (5..8])", false),
        ("overlaps([5..8], [1..5])", true),
        ("overlaps((5..8], [1..5])", false),
        ("overlaps([5..8], [1..5))", false),
        ("overlaps((5..8], [1..5))", false),
    ]);
}

#[test]
fn interval_overlaps_before() {
    assert_all(&[
        ("overlaps before([1..5], [3..8])", true),
        ("overlaps before([1..5], [6..8])", false),
        ("overlaps before([1..5], [5..8])", true),
        ("overlaps before([1..5], (5..8])", false),
        ("overlaps before([1..5), [5..8])", false),
        // equal-start ranges: range1 must genuinely start first (closed vs open start)
        ("overlaps before([1..5), (1..5])", true),
        ("overlaps before([1..5], (1..5])", true),
        ("overlaps before([1..5), [1..5])", false),
        ("overlaps before([1..5], [1..5])", false),
    ]);
}

#[test]
fn interval_overlaps_after() {
    assert_all(&[
        ("overlaps after([3..8], [1..5])", true),
        ("overlaps after([6..8], [1..5])", false),
        ("overlaps after([5..8], [1..5])", true),
        ("overlaps after((5..8], [1..5])", false),
        ("overlaps after([5..8], [1..5))", false),
        // equal-end ranges: range1 must genuinely end last (closed vs open end)
        ("overlaps after((1..5], [1..5))", true),
        ("overlaps after((1..5], [1..5])", true),
        ("overlaps after([1..5], [1..5))", false),
        ("overlaps after([1..5], [1..5])", false),
    ]);
}

#[test]
fn interval_finishes_and_finished_by() {
    assert_all(&[
        // finishes point-range: the range end must be closed and equal to the point
        ("finishes(10, [1..10])", true),
        ("finishes(10, [1..10))", false),
        ("finishes(5, [1..10])", false),
        // finishes range-range: same end (same closedness), starting within
        ("finishes([5..10], [1..10])", true),
        ("finishes([5..10), [1..10])", false),
        ("finishes([5..10), [1..10))", true),
        ("finishes([1..10], [1..10])", true),
        ("finishes((1..10], [1..10])", true),
        ("finishes([0..10], [1..10])", false),
        // finished by: the converse
        ("finished by([1..10], 10)", true),
        ("finished by([1..10), 10)", false),
        ("finished by([1..10], [5..10])", true),
        ("finished by([1..10], [5..10))", false),
        ("finished by([1..10), [5..10))", true),
        ("finished by([1..10], (1..10])", true),
        ("finished by([1..10], [1..10])", true),
        ("finished by([1..10], [0..10])", false),
    ]);
}

#[test]
fn interval_includes_and_during() {
    assert_all(&[
        // includes range-point: endpoint membership honours closedness
        ("includes([1..10], 5)", true),
        ("includes([1..10], 12)", false),
        ("includes([1..10], 1)", true),
        ("includes((1..10], 1)", false),
        ("includes([1..10], 10)", true),
        ("includes([1..10), 10)", false),
        // includes range-range
        ("includes([1..10], [4..6])", true),
        ("includes([1..10], [1..5])", true),
        ("includes((1..10], (1..5])", true),
        ("includes((1..10], [1..5])", false),
        ("includes([1..10], (1..10))", true),
        ("includes([1..10), [5..10))", true),
        ("includes([1..10], [1..10])", true),
        ("includes([1..10], [0..10])", false),
        // during: the converse argument order
        ("during(5, [1..10])", true),
        ("during(12, [1..10])", false),
        ("during(1, [1..10])", true),
        ("during(1, (1..10])", false),
        ("during(10, [1..10])", true),
        ("during(10, [1..10))", false),
        ("during([4..6], [1..10])", true),
        ("during([1..5], [1..10])", true),
        ("during((1..5], (1..10])", true),
        ("during([1..5], (1..10])", false),
        ("during((1..10), [1..10])", true),
        ("during([5..10), [1..10))", true),
        ("during([1..10], [1..10])", true),
        ("during([0..10], [1..10])", false),
    ]);
}

#[test]
fn interval_starts_and_started_by() {
    assert_all(&[
        // starts point-range: the range start must be closed and equal to the point
        ("starts(1, [1..10])", true),
        ("starts(1, (1..10])", false),
        ("starts(2, [1..10])", false),
        // starts range-range: same start (same closedness), ending within
        ("starts([1..5], [1..10])", true),
        ("starts((1..5], (1..10])", true),
        ("starts((1..5], [1..10])", false),
        ("starts([1..5], (1..10])", false),
        ("starts([1..10], [1..10])", true),
        ("starts([1..10), [1..10])", true),
        ("starts([1..10], [1..10))", false),
        ("starts((1..10), (1..10))", true),
        // started by: the converse
        ("started by([1..10], 1)", true),
        ("started by((1..10], 1)", false),
        ("started by([1..10], 2)", false),
        ("started by([1..10], [1..5])", true),
        ("started by((1..10], (1..5])", true),
        ("started by([1..10], (1..5])", false),
        ("started by((1..10], [1..5])", false),
        ("started by([1..10], [1..10])", true),
        ("started by([1..10], [1..10))", true),
        ("started by([1..10), [1..10])", false),
        ("started by((1..10), (1..10))", true),
    ]);
}

#[test]
fn interval_coincides() {
    assert_all(&[
        ("coincides(5, 5)", true),
        ("coincides(3, 4)", false),
        ("coincides([1..5], [1..5])", true),
        ("coincides((1..5), [1..5])", false),
        ("coincides([1..5), [1..5])", false),
        ("coincides([1..5], (1..5])", false),
        ("coincides([1..5], [2..6])", false),
    ]);
}

#[test]
fn interval_non_number_ordered_types() {
    // Any ordered FEEL type works as points/endpoints — dates, times, date-times, durations.
    assert_all(&[
        (r#"before(date("2026-01-01"), date("2026-07-01"))"#, true),
        (
            r#"after([date("2026-07-01")..date("2026-12-31")], date("2026-01-01"))"#,
            true,
        ),
        (
            r#"meets([time("08:00:00")..time("12:00:00")], [time("12:00:00")..time("17:00:00")])"#,
            true,
        ),
        (
            r#"coincides(date and time("2026-01-01T10:00:00"), date and time("2026-01-01T10:00:00"))"#,
            true,
        ),
        (
            r#"includes([duration("P1D")..duration("P10D")], duration("P5D"))"#,
            true,
        ),
    ]);
}

#[test]
fn interval_null_and_type_mismatch_yield_null() {
    for expr in [
        // null argument
        "before(null, 10)",
        "after([1..10], null)",
        "coincides(null, null)",
        // non-orderable point type
        "before(true, false)",
        "includes([1..10], true)",
        // cross-type endpoints
        r#"before(1, date("2026-01-01"))"#,
        r#"during(1, [date("2026-01-01")..date("2026-12-31")])"#,
        // shapes the spec does not define for the function
        "meets(1, [1..10])",
        "meets([1..10], 5)",
        "overlaps(1, 2)",
        "starts([1..10], 5)",
        "finishes([1..10], 10)",
        "coincides(3, [1..10])",
        // a comparison-operator range has no bounded side to relate against
        "before(1, (> 10))",
    ] {
        assert_eq!(
            expressions::eval(expr, &FeelContext::new()).unwrap(),
            FeelValue::Null,
            "expression: {expr}"
        );
    }
}
