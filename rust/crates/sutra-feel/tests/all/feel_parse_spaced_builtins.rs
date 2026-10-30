//! The context-free `parse` entry point must accept every expression `eval` accepts: a FEEL
//! builtin whose name contains a space (`context put`, `list contains`, `string length`, …) is
//! one name (DMN 1.5 §10.3.4), whether or not a runtime context is at hand. Build-time callers
//! (the package lint, the loader) parse with no context, so a gap here rejected valid models.

use sutra_feel::expressions::{eval, parse};
use sutra_feel::{FeelContext, FeelValue};

#[test]
fn context_put_parses_without_a_context() {
    parse(r#"context put({a: 1}, "b", 2)"#).expect("context put parses");
    parse(r#"context put(x, ["a", "b"], 3)"#).expect("nested-key form parses");
}

#[test]
fn not_of_list_contains_parses_inside_a_condition() {
    parse(r#"x != null and not(list contains(["A", "B"], x))"#).expect("parses");
    parse(r#"not(list contains(codes, code)) or string length(code) > 3"#).expect("parses");
}

#[test]
fn every_spaced_builtin_parses_as_a_call() {
    for text in [
        r#"string length("abc")"#,
        r#"starts with("abc", "a")"#,
        r#"ends with("abc", "c")"#,
        r#"upper case("a")"#,
        r#"lower case("A")"#,
        r#"substring before("a-b", "-")"#,
        r#"substring after("a-b", "-")"#,
        r#"list contains([1], 1)"#,
        r#"insert before([1], 1, 0)"#,
        r#"index of([1], 1)"#,
        r#"distinct values([1, 1])"#,
        r#"get value({a: 1}, "a")"#,
        r#"get entries({a: 1})"#,
        r#"context merge([{a: 1}])"#,
        r#"date and time("2026-10-30T00:00:00")"#,
        r#"years and months duration(date("2026-01-01"), date("2026-10-01"))"#,
    ] {
        parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    }
}

#[test]
fn parse_and_eval_agree_on_the_parsed_shape() {
    let mut ctx = FeelContext::new();
    ctx.insert("x".into(), FeelValue::from("C"));
    let text = r#"x != null and not(list contains(["A", "B"], x))"#;
    parse(text).expect("parses");
    assert_eq!(eval(text, &ctx).unwrap(), FeelValue::Boolean(true));
    let put = eval(r#"context put({a: 1}, "b", 2).b"#, &ctx).unwrap();
    assert_eq!(put, FeelValue::num("2"));
}

#[test]
fn a_hyphenated_name_still_parses_as_one_name_without_a_context() {
    // The context-free parse keeps the lexer's hyphen fold (no scope = no evidence it is
    // subtraction); only the spaced-builtin merge was missing.
    parse("Pre-bureauRiskCategory = \"HIGH\"").expect("parses");
}
