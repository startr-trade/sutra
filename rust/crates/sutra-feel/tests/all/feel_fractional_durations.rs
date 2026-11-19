//! Fractional seconds in days-and-time durations. A duration such as `PT0.001S` holds its
//! fraction to the nanosecond through parsing, `string()`, comparison, arithmetic with dates and
//! times, scaling, and the `seconds` accessor. Cases are written in the style of the DMN TCK's
//! `1120-feel-duration-function` (expression, then expected string).

use sutra_feel::expressions;
use sutra_feel::{FeelContext, FeelDuration, FeelValue};

fn eval(expr: &str) -> FeelValue {
    expressions::eval(expr, &FeelContext::new()).unwrap_or_else(|e| panic!("{expr}: {e:?}"))
}

fn string_of(expr: &str) -> String {
    match eval(&format!("string({expr})")) {
        FeelValue::String(s) => s,
        other => panic!("{expr}: expected a string, got {other:?}"),
    }
}

fn days_time(expr: &str) -> time::Duration {
    match eval(expr) {
        FeelValue::Duration(FeelDuration::DaysTime(d)) => d,
        other => panic!("{expr}: expected a days-and-time duration, got {other:?}"),
    }
}

#[test]
fn a_millisecond_duration_is_not_zero() {
    assert_eq!(
        days_time(r#"duration("PT0.001S")"#),
        time::Duration::milliseconds(1)
    );
    assert_eq!(
        eval(r#"duration("PT0.001S") > duration("PT0S")"#),
        FeelValue::Boolean(true)
    );
}

#[test]
fn the_fraction_is_kept_to_the_nanosecond() {
    assert_eq!(
        days_time(r#"duration("P1DT2H3M4.123456789S")"#),
        time::Duration::new(93_784, 123_456_789)
    );
    assert_eq!(
        days_time(r#"@"PT0.000000001S""#),
        time::Duration::nanoseconds(1)
    );
    // Past the ninth digit the fraction is truncated, never rounded up.
    assert_eq!(
        days_time(r#"duration("PT0.0000000019S")"#),
        time::Duration::nanoseconds(1)
    );
    assert_eq!(
        days_time(r#"duration("-PT1.5S")"#),
        -time::Duration::milliseconds(1_500)
    );
}

#[test]
fn string_round_trips_the_fraction() {
    // TCK 1120 `007`/`008` expectations.
    assert_eq!(string_of(r#"duration("PT0.999S")"#), "PT0.999S");
    assert_eq!(
        string_of(r#"duration("P1DT2H3M4.123456789S")"#),
        "P1DT2H3M4.123456789S"
    );
    assert_eq!(string_of(r#"duration("PT0.001S")"#), "PT0.001S");
    assert_eq!(string_of(r#"duration("PT1.500S")"#), "PT1.5S");
    assert_eq!(string_of(r#"duration("-PT0.25S")"#), "-PT0.25S");
    assert_eq!(string_of(r#"duration("PT2H0.5S")"#), "PT2H0.5S");
    assert_eq!(string_of(r#"duration("PT1.0S")"#), "PT1S");
    // XML Schema decimal numerals: a bare trailing point (TCK 1120 `011`) or a bare leading one.
    assert_eq!(string_of(r#"duration("PT0.S")"#), "PT0S");
    assert_eq!(string_of(r#"duration("PT.5S")"#), "PT0.5S");
}

#[test]
fn comparison_and_equality_see_the_fraction() {
    assert_eq!(
        eval(r#"duration("PT0.001S") = duration("PT0.002S")"#),
        FeelValue::Boolean(false)
    );
    assert_eq!(
        eval(r#"duration("PT1.5S") = duration("PT1.500S")"#),
        FeelValue::Boolean(true)
    );
    assert_eq!(
        eval(r#"duration("PT0.001S") < duration("PT0.002S")"#),
        FeelValue::Boolean(true)
    );
}

#[test]
fn date_time_arithmetic_keeps_milliseconds() {
    assert_eq!(
        string_of(r#"date and time("2026-11-19T10:00:00Z") + duration("PT0.001S")"#),
        "2026-11-19T10:00:00.001Z"
    );
    assert_eq!(
        string_of(
            r#"date and time("2026-11-19T10:01:00.420Z") - date and time("2026-11-19T10:00:00.120Z")"#
        ),
        "PT1M0.3S"
    );
    assert_eq!(
        string_of(r#"time("10:00:00") - duration("PT0.5S")"#),
        "09:59:59.5"
    );
}

#[test]
fn duration_arithmetic_and_scaling_keep_the_fraction() {
    assert_eq!(
        string_of(r#"duration("PT0.001S") + duration("PT0.002S")"#),
        "PT0.003S"
    );
    assert_eq!(string_of(r#"duration("PT0.001S") * 3"#), "PT0.003S");
    assert_eq!(string_of(r#"duration("PT1S") / 4"#), "PT0.25S");
    assert_eq!(string_of(r#"duration("PT1S") * 0.0015"#), "PT0.0015S");
    assert_eq!(string_of(r#"abs(duration("-PT0.75S"))"#), "PT0.75S");
    assert_eq!(string_of(r#"-duration("PT0.75S")"#), "-PT0.75S");
    match eval(r#"duration("PT1.5S") / duration("PT0.5S")"#) {
        FeelValue::Number(n) => assert_eq!(n.normalized().to_string(), "3"),
        other => panic!("expected a number, got {other:?}"),
    }
}

#[test]
fn the_seconds_accessor_keeps_the_fraction() {
    let n = |expr: &str| match eval(expr) {
        FeelValue::Number(n) => n.normalized().to_string(),
        other => panic!("{expr}: expected a number, got {other:?}"),
    };
    assert_eq!(n(r#"duration("PT4.5S").seconds"#), "4.5");
    assert_eq!(n(r#"duration("-PT0.25S").seconds"#), "-0.25");
    assert_eq!(n(r#"duration("PT2S").seconds"#), "2");
    assert_eq!(n(r#"duration("P1DT2H3M4.001S").minutes"#), "3");
}

#[test]
fn a_fraction_on_any_unit_but_seconds_is_refused() {
    for bad in ["PT1.5H", "PT1.5M", "P1.5D", "P1.5Y", "PT.S", "PT1.2.3S"] {
        let expr = format!(r#"duration("{bad}")"#);
        match expressions::eval(&expr, &FeelContext::new()) {
            Err(_) | Ok(FeelValue::Null) => {}
            Ok(other) => panic!("{bad} should not parse, got {other:?}"),
        }
    }
}
