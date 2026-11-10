//! Dotted-path lookup: the walk borrows through nested maps and clones only the value it ends on.
//! These pin the results the lookup must keep: nested entries, a missing or null segment, a
//! segment past a non-map value, list projection, temporal components and range bounds, and the
//! same results through a parenthesised base (the field-access form of the same path).

use std::collections::BTreeMap;

use bigdecimal::BigDecimal;
use sutra_feel::{expressions, FeelContext, FeelValue};

fn num(n: i64) -> FeelValue {
    FeelValue::Number(BigDecimal::from(n))
}

fn map(entries: &[(&str, FeelValue)]) -> FeelValue {
    FeelValue::Map(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn context() -> FeelContext {
    let body = map(&[
        ("amount", num(42)),
        ("note", FeelValue::Null),
        ("tags", FeelValue::List(vec![FeelValue::String("a".into())])),
        (
            "items",
            FeelValue::List(vec![map(&[("id", num(1))]), map(&[("id", num(2))]), num(3)]),
        ),
    ]);
    let mut ctx = FeelContext::new();
    ctx.insert("payload".into(), map(&[("body", body)]));
    ctx.insert("flag".into(), FeelValue::Boolean(true));
    ctx.insert("nothing".into(), FeelValue::Null);
    ctx
}

fn eval(expression: &str) -> FeelValue {
    expressions::eval(expression, &context()).unwrap()
}

#[test]
fn nested_map_entries_resolve_to_the_final_value() {
    assert_eq!(eval("payload.body.amount"), num(42));
    assert_eq!(eval("(payload).body.amount"), num(42));
    let body = eval("payload.body");
    let FeelValue::Map(m) = body else {
        panic!("expected the body map, got {body:?}")
    };
    assert_eq!(m.len(), 4);
}

#[test]
fn a_missing_or_null_segment_is_null_all_the_way_down() {
    assert_eq!(eval("payload.missing"), FeelValue::Null);
    assert_eq!(eval("payload.missing.deeper.still"), FeelValue::Null);
    assert_eq!(eval("payload.body.note"), FeelValue::Null);
    assert_eq!(eval("payload.body.note.x"), FeelValue::Null);
    assert_eq!(eval("nothing.x"), FeelValue::Null);
    assert_eq!(eval("undeclared.x"), FeelValue::Null);
    assert_eq!(eval("(payload).missing.deeper"), FeelValue::Null);
}

#[test]
fn a_segment_past_a_non_map_value_is_null() {
    assert_eq!(eval("flag.x"), FeelValue::Null);
    assert_eq!(eval("payload.body.amount.x"), FeelValue::Null);
    assert_eq!(eval("payload.body.amount.x.y"), FeelValue::Null);
}

#[test]
fn a_list_segment_projects_the_rest_of_the_path() {
    assert_eq!(
        eval("payload.body.items.id"),
        FeelValue::List(vec![num(1), num(2), FeelValue::Null])
    );
    assert_eq!(
        eval("payload.body.tags.x"),
        FeelValue::List(vec![FeelValue::Null])
    );
    assert_eq!(
        eval("(payload).body.items.id"),
        FeelValue::List(vec![num(1), num(2), FeelValue::Null])
    );
}

#[test]
fn temporal_and_range_properties_still_resolve_after_a_map_walk() {
    let mut ctx = context();
    ctx.insert(
        "when".into(),
        map(&[(
            "d",
            expressions::eval("date(\"2024-03-15\")", &FeelContext::new()).unwrap(),
        )]),
    );
    ctx.insert(
        "r".into(),
        map(&[(
            "span",
            expressions::eval("[1..5)", &FeelContext::new()).unwrap(),
        )]),
    );
    assert_eq!(expressions::eval("when.d.day", &ctx).unwrap(), num(15));
    assert_eq!(expressions::eval("when.d.month", &ctx).unwrap(), num(3));
    assert_eq!(expressions::eval("r.span.start", &ctx).unwrap(), num(1));
    assert_eq!(expressions::eval("r.span.end", &ctx).unwrap(), num(5));
}
