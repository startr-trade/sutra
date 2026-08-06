//! 1:1 port of `FeelPathsTest` — T3a: path extraction with numeric-usage tagging, driven
//! through the `expressions::paths` facade.

use sutra_feel::expressions;
use sutra_feel::paths::Usage;

fn dotted_usages(expression: &str) -> Vec<(String, Usage)> {
    expressions::paths(expression)
        .unwrap()
        .into_iter()
        .map(|p| (p.dotted(), p.usage))
        .collect()
}

#[test]
fn extracts_a_dotted_path_with_its_numeric_usage() {
    let paths = expressions::paths("payload.amount > 1000").unwrap();

    assert_eq!(
        dotted_usages("payload.amount > 1000"),
        vec![("payload.amount".to_string(), Usage::Numeric)]
    );
    assert_eq!(paths[0].root(), "payload");
    assert_eq!(paths[0].segments, vec!["payload", "amount"]);
}

#[test]
fn equality_is_general_usage_but_ordering_is_numeric() {
    // name = "USD" → EQ → GENERAL (equality is type-agnostic); amount > 100 → GT → NUMERIC.
    assert_eq!(
        dotted_usages("name = \"USD\" and amount > 100"),
        vec![
            ("name".to_string(), Usage::General),
            ("amount".to_string(), Usage::Numeric)
        ]
    );
}

#[test]
fn arithmetic_operands_are_numeric() {
    assert_eq!(
        dotted_usages("payload.a + payload.b > payload.c"),
        vec![
            ("payload.a".to_string(), Usage::Numeric),
            ("payload.b".to_string(), Usage::Numeric),
            ("payload.c".to_string(), Usage::Numeric)
        ]
    );
}

#[test]
fn deeply_nested_path_keeps_all_segments() {
    let paths = expressions::paths("payload.body.GrpHdr.MsgId = \"X\"").unwrap();

    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].dotted(), "payload.body.GrpHdr.MsgId");
    assert_eq!(paths[0].usage, Usage::General);
}

#[test]
fn literals_and_calls_without_paths_yield_none() {
    assert!(expressions::paths("42").unwrap().is_empty());
    assert!(expressions::paths("now()").unwrap().is_empty());
}

#[test]
fn function_argument_paths_are_general_usage() {
    // matches(payload.uetr, "…") — a path inside a call arg is general (not numeric).
    assert_eq!(
        dotted_usages("matches(payload.uetr, \"^[A-Z]+$\")"),
        vec![("payload.uetr".to_string(), Usage::General)]
    );
}

#[test]
fn if_branches_inherit_the_outer_usage_and_condition_is_general() {
    // cond payload.flag → GENERAL; the if-expression is itself an operand of > 0, so the
    // value-position branches (payload.hi / payload.lo) read numeric.
    assert_eq!(
        dotted_usages("(if payload.flag then payload.hi else payload.lo) > 0"),
        vec![
            ("payload.flag".to_string(), Usage::General),
            ("payload.hi".to_string(), Usage::Numeric),
            ("payload.lo".to_string(), Usage::Numeric)
        ]
    );
}

#[test]
fn path_offsets_are_preserved_for_diagnostics() {
    // "amount > 100": the path 'amount' sits at offsets [0,6).
    let paths = expressions::paths("amount > 100").unwrap();
    assert_eq!(paths[0].start, 0);
    assert_eq!(paths[0].end, 6);
}

#[test]
fn invoke_callee_and_args_paths_are_general_usage() {
    // `f(2)(payload.x)` — a postfix invocation of the `f(2)` call result; paths in either the
    // callee or the invocation's own arguments are collected.
    assert_eq!(
        dotted_usages("f(2)(payload.x)"),
        vec![("payload.x".to_string(), Usage::General)]
    );
}

#[test]
fn open_range_bound_path_is_numeric_usage() {
    assert_eq!(
        dotted_usages("(< payload.limit)"),
        vec![("payload.limit".to_string(), Usage::Numeric)]
    );
}
