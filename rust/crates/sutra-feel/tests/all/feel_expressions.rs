//! End-to-end FEEL expression evaluation: names with spaces, list/context literals,
//! filters and projection, function literals and named arguments, temporal literals and
//! arithmetic, quantifiers and ranges, and the builtin-function library.
//!
//! Numeric assertions compare the Display form of the resulting decimal, so they are
//! scale-sensitive (`4` and `4.0` are distinct expectations).

use sutra_feel::expressions;
use sutra_feel::{FeelContext, FeelTypeShape, FeelValue, Invocable, TimeQualifier};
use time::macros::datetime;

fn ctx(pairs: Vec<(&str, FeelValue)>) -> FeelContext {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Build a native `Invocable` value for the tests below — a stand-in for what `sutra-dmn`'s DRG
/// builds for indirect decision-service invocation (the closure here is an arbitrary Rust
/// function; this crate has no idea, and doesn't need to, what a caller's closure actually does).
fn make_invocable(
    id: &str,
    params: Vec<&str>,
    param_shapes: Vec<FeelTypeShape>,
    f: impl Fn(&[FeelValue]) -> FeelValue + Send + Sync + 'static,
) -> FeelValue {
    FeelValue::Invocable(Invocable {
        id: id.to_string(),
        params: params.into_iter().map(String::from).collect(),
        param_shapes,
        return_shape: FeelTypeShape::Any,
        call: std::sync::Arc::new(f),
    })
}

fn map(pairs: Vec<(&str, FeelValue)>) -> FeelValue {
    FeelValue::Map(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn empty() -> FeelContext {
    FeelContext::new()
}

/// Scale-sensitive numeric assertion — compares the exact decimal string (scale included).
fn assert_num(v: FeelValue, expected: &str) {
    match v {
        FeelValue::Number(n) => assert_eq!(n.to_string(), expected),
        other => panic!("expected Number({expected}), got {other:?}"),
    }
}

// ----- Names with spaces (FEEL §10.3.1.2; DMN-TCK level 3) -----

#[test]
fn spaced_name_resolves_from_context() {
    let c = ctx(vec![(
        "Base Vacation Days",
        FeelValue::Number("22".parse().unwrap()),
    )]);
    assert_num(
        expressions::eval("Base Vacation Days + 1", &c).unwrap(),
        "23",
    );
}

#[test]
fn spaced_name_inside_function_call() {
    let c = ctx(vec![(
        "Extra days case 1",
        FeelValue::Number("2".parse().unwrap()),
    )]);
    // `string(...)` is an existing 1-arg builtin — asserts the spaced name resolves as a call
    // argument (the merge fires inside parentheses), independent of any new builtin.
    assert_eq!(
        expressions::eval("string(Extra days case 1)", &c).unwrap(),
        FeelValue::String("2".into())
    );
}

#[test]
fn spaced_name_with_trailing_number_part() {
    // "decision C 2" — the final token is a NUMBER; the run must still join into the name.
    let c = ctx(vec![(
        "decision C 2",
        FeelValue::Number("7".parse().unwrap()),
    )]);
    assert_num(expressions::eval("decision C 2 + 0", &c).unwrap(), "7");
}

#[test]
fn spaced_name_longest_match_wins() {
    // Both "Full Name" and "Full Name Prefix" known — the longer must win.
    let c = ctx(vec![
        ("Full Name", FeelValue::String("short".into())),
        ("Full Name Prefix", FeelValue::String("long".into())),
    ]);
    assert_eq!(
        expressions::eval("Full Name Prefix", &c).unwrap(),
        FeelValue::String("long".into())
    );
}

#[test]
fn spaced_field_name_via_path() {
    // A spaced field referenced through a path resolves via the nested-map key collector.
    let inner = map(vec![(
        "Existing Loans",
        FeelValue::Number("3".parse().unwrap()),
    )]);
    let c = ctx(vec![("Applicant", inner)]);
    assert_num(
        expressions::eval("Applicant.Existing Loans", &c).unwrap(),
        "3",
    );
}

#[test]
fn spaced_name_containing_the_in_keyword() {
    // "values in a list" (DMN-TCK 0016-some-every) — the lexer emits a standalone `in` as its own
    // TokenKind, never Ident, so the names-with-spaces merge must special-case it as a mid-run
    // continuation (never a run START — a name can't begin with the word "in").
    let c = ctx(vec![(
        "values in a list",
        FeelValue::List(vec![
            FeelValue::num("0"),
            FeelValue::num("1"),
            FeelValue::num("2"),
        ]),
    )]);
    assert!(expressions::eval_boolean("every i in values in a list satisfies i >= 0", &c).unwrap());
    assert!(
        !expressions::eval_boolean("some i in values in a list satisfies i > 100", &c).unwrap()
    );
}

#[test]
fn spaced_name_containing_in_keyword_with_no_other_adjacent_idents() {
    // "cash in hand" has no adjacent Ident/Ident pair elsewhere in the expression (only
    // Ident/In and In/Ident pairs) — this exercises the `has_adjacent_name_run` pre-check
    // widening, not just the `merge_named_tokens` run-extension loop.
    let c = ctx(vec![("cash in hand", FeelValue::num("42"))]);
    assert_num(expressions::eval("cash in hand", &c).unwrap(), "42");
}

#[test]
fn spaced_name_containing_the_and_keyword() {
    // "Another Date and Time" (DMN-TCK 0036-dt-variable-input's own input names — also "Another
    // Days and Time Duration" / "Another Years and Months Duration") — the lexer emits a
    // standalone `and` as its own TokenKind (a boolean operator), never Ident, so without
    // widening the run-continuation to also cross `and`/`or`/`not`, the merge either failed to
    // fuse the name at all or — worse — matched a SHORTER, unrelated known name as a false-
    // positive prefix (here, "Another Date" is itself a genuine name elsewhere in the same
    // model), corrupting the rest of the parse into `(Another Date) and (Time)`.
    let c = ctx(vec![("Another Date and Time", FeelValue::num("5"))]);
    assert_num(expressions::eval("Another Date and Time", &c).unwrap(), "5");
}

#[test]
fn spaced_name_containing_the_and_keyword_is_not_confused_by_a_shorter_prefix_name() {
    // Both "Another Date" and "Another Date and Time" are genuine known names at once (the exact
    // shape that broke before this fix): the longer, exact match must win over the shorter
    // 2-token prefix that also happens to be a real name.
    let c = ctx(vec![
        ("Another Date", FeelValue::num("1")),
        ("Another Date and Time", FeelValue::num("2")),
    ]);
    assert_num(expressions::eval("Another Date and Time", &c).unwrap(), "2");
    assert_num(expressions::eval("Another Date", &c).unwrap(), "1");
}

#[test]
fn spaced_name_containing_the_or_and_not_keywords() {
    let c = ctx(vec![
        ("Approved or Denied", FeelValue::from("Approved")),
        ("Not Applicable", FeelValue::Boolean(true)),
    ]);
    assert_eq!(
        expressions::eval("Approved or Denied", &c).unwrap(),
        FeelValue::from("Approved")
    );
    assert!(expressions::eval_boolean("Not Applicable", &c).unwrap());
}

#[test]
fn possessive_apostrophe_is_part_of_the_identifier_not_a_string_quote() {
    // "Student's name" (DMN-TCK 0088-no-decision-logic) — a non-standard leniency also treats
    // `'…'` as a string literal, so the possessive apostrophe must be claimed by the identifier
    // lexer first (an apostrophe immediately followed by another letter never starts a quote).
    let c = ctx(vec![("Student's name", FeelValue::from("Ann"))]);
    assert_eq!(
        expressions::eval("Student's name", &c).unwrap(),
        FeelValue::from("Ann")
    );
}

// ----- List & context literals, power operator (DMN-TCK level 3) -----

#[test]
fn list_literal_builds_a_list() {
    assert_eq!(
        expressions::eval("[1, 2, 3]", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("3"),
        ])
    );
}

#[test]
fn empty_list_literal() {
    assert_eq!(
        expressions::eval("[]", &empty()).unwrap(),
        FeelValue::List(vec![])
    );
}

#[test]
fn list_literal_equality() {
    assert!(expressions::eval_boolean("[1, 2] = [1, 2]", &empty()).unwrap());
}

#[test]
fn context_literal_builds_a_map() {
    assert_eq!(
        expressions::eval("{a: 1, b: 2}", &empty()).unwrap(),
        map(vec![("a", FeelValue::num("1")), ("b", FeelValue::num("2"))])
    );
}

#[test]
fn context_literal_string_key_and_later_entry_sees_earlier() {
    // "b" references earlier entry "a" (FEEL context semantics); a string-literal key is allowed.
    assert_eq!(
        expressions::eval("{\"a\": 10, b: a + 5}", &empty()).unwrap(),
        map(vec![
            ("a", FeelValue::num("10")),
            ("b", FeelValue::num("15"))
        ])
    );
}

#[test]
fn context_literal_key_is_a_raw_name_run_not_an_expression() {
    // DMN-TCK 0057-feel-context#004/#005: a context-entry key that isn't a quoted string is
    // FEEL's own permissive "Name" grammar — a raw run of source text up to the `:`, not a
    // general expression. `{foo bar: ...}`'s key is the "names with spaces" run `"foo bar"`.
    assert_eq!(
        expressions::eval(r#"{foo bar: "foo"}"#, &empty()).unwrap(),
        map(vec![("foo bar", FeelValue::from("foo"))])
    );
    // `{foo+bar: ...}`'s key is literally `"foo+bar"` — NOT the arithmetic expression `foo +
    // bar` (which would need `foo`/`bar` bound as variables, and isn't what a context key means).
    assert_eq!(
        expressions::eval(r#"{foo+bar: "foo"}"#, &empty()).unwrap(),
        map(vec![("foo+bar", FeelValue::from("foo"))])
    );
}

#[test]
fn a_bare_supplementary_unicode_character_is_a_valid_name() {
    // DMN-TCK 0083-feel-unicode#decision_006/#decision_007: `{🐎: "bar"}` — a context key that is
    // a single astral/supplementary-plane emoji, never `is_alphanumeric()` (Unicode category
    // "So", Symbol-other) but still a valid bare FEEL "Name".
    assert_eq!(
        expressions::eval("{🐎: \"bar\"}", &empty()).unwrap(),
        map(vec![("🐎", FeelValue::from("bar"))])
    );
    assert_eq!(
        expressions::eval("{🐎: \"😀\"}", &empty()).unwrap(),
        map(vec![("🐎", FeelValue::from("😀"))])
    );
}

#[test]
fn empty_context_literal() {
    assert_eq!(
        expressions::eval("{}", &empty()).unwrap(),
        FeelValue::Map(std::collections::BTreeMap::new())
    );
}

#[test]
fn power_integer_exponent_is_exact() {
    assert_num(expressions::eval("2 ** 10", &empty()).unwrap(), "1024");
}

#[test]
fn power_binds_tighter_than_multiplication() {
    // 3 * 2 ** 3 = 3 * 8 = 24 (not (3*2)**3 = 216)
    assert_num(expressions::eval("3 * 2 ** 3", &empty()).unwrap(), "24");
}

#[test]
fn power_negative_exponent_is_reciprocal() {
    assert_num(expressions::eval("2 ** -2", &empty()).unwrap(), "0.25");
}

// ----- `in` membership, `between`, rounding builtins (DMN-TCK level 3) -----

#[test]
fn in_membership() {
    assert!(expressions::eval_boolean("5 in [1, 5, 9]", &empty()).unwrap());
    assert!(!expressions::eval_boolean("3 in [1, 5, 9]", &empty()).unwrap());
    assert!(expressions::eval_boolean("5 in [1..10]", &empty()).unwrap());
    assert!(expressions::eval_boolean("\"b\" in [\"a\", \"b\"]", &empty()).unwrap());
}

#[test]
fn in_positive_unary_tests() {
    // Bare comparison unary tests.
    assert!(expressions::eval_boolean("5 in < 10", &empty()).unwrap());
    assert!(expressions::eval_boolean("5 in <= 5", &empty()).unwrap());
    assert!(!expressions::eval_boolean("5 in > 10", &empty()).unwrap());
    // Parenthesized test lists mixing values and comparisons.
    assert!(expressions::eval_boolean("5 in (1, 2, >= 5)", &empty()).unwrap());
    assert!(!expressions::eval_boolean("5 in (< 3, > 8)", &empty()).unwrap());
    // A list of intervals.
    assert!(expressions::eval_boolean("7 in [[1..3], [4..7]]", &empty()).unwrap());
    assert!(!expressions::eval_boolean("9 in [[1..3], [4..7]]", &empty()).unwrap());
    // Half-open interval.
    assert!(!expressions::eval_boolean("10 in [1..10)", &empty()).unwrap());
}

#[test]
fn in_bracketed_list_of_lists_is_one_membership_test_not_ord_tests() {
    // DMN-TCK 0072-feel-in list_001/list_011_a: a `[`-bracketed group whose elements are
    // themselves plain LIST values (no range/comparison element among them) collapses to ONE
    // list literal, tested via ordinary FEEL list membership (does the LHS equal any element) —
    // not split into per-element OR'd tests (which would ask "is `[1,2,3]` a MEMBER of
    // `[1,2,3,4]`'s own elements", never true for a list-valued LHS).
    assert!(expressions::eval_boolean("[1,2,3] in [[1,2,3,4], [1,2,3]]", &empty()).unwrap());
    assert!(expressions::eval_boolean("[1,2,3] in [[1,2,3], [1,2,3,4]]", &empty()).unwrap());
    assert!(!expressions::eval_boolean("[1,2,3,5] in [[1,2,3,4], [1,2,3]]", &empty()).unwrap());
    // A range/comparison element anywhere in the group keeps the OR'd-disjunction reading
    // (unaffected — same assertions as `in_positive_unary_tests` above, re-checked alongside the
    // new list-collapse rule to pin the disambiguation boundary).
    assert!(expressions::eval_boolean("7 in [[1..3], [4..7]]", &empty()).unwrap());
}

#[test]
fn between_desugars_to_range_check() {
    assert!(expressions::eval_boolean("5 between 1 and 10", &empty()).unwrap());
    assert!(!expressions::eval_boolean("15 between 1 and 10", &empty()).unwrap());
    assert!(expressions::eval_boolean("1 between 1 and 10", &empty()).unwrap());
    // inclusive
}

#[test]
fn rounding_builtins() {
    assert_num(
        expressions::eval("round up(1.11, 1)", &empty()).unwrap(),
        "1.2",
    );
    assert_num(
        expressions::eval("round down(1.19, 1)", &empty()).unwrap(),
        "1.1",
    );
    assert_num(
        expressions::eval("round half up(2.5, 0)", &empty()).unwrap(),
        "3",
    );
    assert_num(
        expressions::eval("round half down(2.5, 0)", &empty()).unwrap(),
        "2",
    );
}

#[test]
fn and_containing_builtin_names_parse_as_calls() {
    // The `and` keyword splits these names in the lexer; the parser recognizes the token run.
    assert!(matches!(
        expressions::eval("date and time(\"2018-12-08T10:30:00\")", &empty()).unwrap(),
        FeelValue::Instant(..)
    ));
    assert_eq!(
        expressions::eval(
            "years and months duration(date(\"2019-01-01\"), date(\"2020-03-01\"))",
            &empty()
        )
        .unwrap(),
        expressions::eval("@\"P1Y2M\"", &empty()).unwrap()
    );
    assert_eq!(
        expressions::eval(
            "days and time duration(date(\"2019-01-01\"), date(\"2019-01-11\"))",
            &empty()
        )
        .unwrap(),
        expressions::eval("@\"P10D\"", &empty()).unwrap()
    );
}

#[test]
fn date_ordinal_and_week_accessors() {
    assert_num(
        expressions::eval("day of year(@\"2021-01-01\")", &empty()).unwrap(),
        "1",
    );
    assert_num(
        expressions::eval("week of year(@\"2021-01-04\")", &empty()).unwrap(),
        "1",
    );
}

// ----- Function literals + named arguments (DMN-TCK level 3) -----

#[test]
fn function_literal_positional_invocation() {
    let f = expressions::eval("function(a, b) a + b", &empty()).unwrap();
    let c = ctx(vec![("f", f)]);
    assert_num(expressions::eval("f(2, 3)", &c).unwrap(), "5");
}

#[test]
fn function_named_argument_binding_is_order_independent() {
    let f = expressions::eval("function(a, b) a - b", &empty()).unwrap();
    let c = ctx(vec![("f", f)]);
    assert_num(expressions::eval("f(b: 3, a: 10)", &c).unwrap(), "7");
}

#[test]
fn function_body_can_call_builtins() {
    let f = expressions::eval("function(xs) sum(xs)", &empty()).unwrap();
    let c = ctx(vec![("total", f)]);
    assert_num(expressions::eval("total([1, 2, 3, 4])", &c).unwrap(), "10");
}

#[test]
fn function_param_type_annotation_is_ignored() {
    let f = expressions::eval("function(a: number, b: number) a * b", &empty()).unwrap();
    let c = ctx(vec![("mul", f)]);
    assert_num(expressions::eval("mul(6, 7)", &c).unwrap(), "42");
}

// ----- Native `Invocable` values (indirect decision-service invocation, DMN-TCK
// 0085-decision-services / 0092-feel-lambda#013) -----

#[test]
fn invocable_positional_and_named_invocation() {
    let add = || {
        make_invocable(
            "add",
            vec!["a", "b"],
            vec![FeelTypeShape::Any, FeelTypeShape::Any],
            |args| match (&args[0], &args[1]) {
                (FeelValue::Number(a), FeelValue::Number(b)) => FeelValue::Number(a + b),
                _ => FeelValue::Null,
            },
        )
    };
    assert_num(
        expressions::eval("add(2, 3)", &ctx(vec![("add", add())])).unwrap(),
        "5",
    );
    // Named arguments bind by declared parameter name, order-independent — same call-site
    // contract as an ordinary `FeelFunction` (`function_named_argument_binding_is_order_independent`
    // above).
    assert_num(
        expressions::eval("add(b: 3, a: 10)", &ctx(vec![("add", add())])).unwrap(),
        "13",
    );
}

#[test]
fn invocable_strict_arity_gating_never_invokes_on_a_bad_call() {
    // Unlike an ordinary `FeelFunction` (which tolerates a missing/extra argument — the body
    // just sees an unbound name), an `Invocable` has no FEEL-AST body to fall through to, so
    // arity is checked STRICTLY: this is what keeps DMN-TCK 0085-decision-services#005/#007/#008
    // (wrong-arity/wrong-type "the service is never invoked" cases) from silently succeeding.
    let add = || {
        make_invocable(
            "add",
            vec!["a", "b"],
            vec![FeelTypeShape::Any, FeelTypeShape::Any],
            |args| match (&args[0], &args[1]) {
                (FeelValue::Number(a), FeelValue::Number(b)) => FeelValue::Number(a + b),
                _ => FeelValue::Null,
            },
        )
    };
    assert_eq!(
        expressions::eval("add(1)", &ctx(vec![("add", add())])).unwrap(),
        FeelValue::Null,
        "too few positional args"
    );
    assert_eq!(
        expressions::eval("add(1, 2, 3)", &ctx(vec![("add", add())])).unwrap(),
        FeelValue::Null,
        "too many positional args"
    );
    assert_eq!(
        expressions::eval("add(a: 1, c: 2)", &ctx(vec![("add", add())])).unwrap(),
        FeelValue::Null,
        "an unrecognized named argument is not a partial call"
    );
}

#[test]
fn invocable_typeref_gating_never_invokes_on_a_type_mismatch() {
    let greet = || {
        make_invocable(
            "greet",
            vec!["name"],
            vec![FeelTypeShape::Base("string".to_string())],
            |args| match &args[0] {
                FeelValue::String(s) => FeelValue::String(format!("hi {s}")),
                _ => FeelValue::Null,
            },
        )
    };
    assert_eq!(
        expressions::eval(r#"greet("Sam")"#, &ctx(vec![("greet", greet())])).unwrap(),
        FeelValue::from("hi Sam")
    );
    // A number doesn't conform to the declared `string` parameter — the WHOLE call is `null`,
    // never partially invoked (DMN's "the service is never invoked" semantics).
    assert_eq!(
        expressions::eval("greet(5)", &ctx(vec![("greet", greet())])).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn invocable_passed_bare_as_a_value_and_invoked_via_a_function_parameter() {
    // Mirrors DMN-TCK 0092-feel-lambda#013: a decision-service-shaped `Invocable` is passed BARE
    // (as a value, not immediately called) into an ordinary FEEL function, which then invokes it
    // under its own local parameter name — exercising the `Call` dispatch's `Invocable` arm from
    // INSIDE a `FeelFunction` body, not just at the top level.
    let double = make_invocable(
        "double",
        vec!["n"],
        vec![FeelTypeShape::Any],
        |args| match &args[0] {
            FeelValue::Number(n) if n.to_string() == "21" => FeelValue::from(42i64),
            _ => FeelValue::Null,
        },
    );
    let caller = expressions::eval("function(fn) fn(21)", &empty()).unwrap();
    let c = ctx(vec![("caller", caller), ("svc", double)]);
    assert_num(expressions::eval("caller(svc)", &c).unwrap(), "42");
}

#[test]
fn builtin_named_arguments_use_positional_order() {
    assert_num(expressions::eval("abs(n: -5)", &empty()).unwrap(), "5");
    assert!(expressions::eval_boolean("all(list: [true, true])", &empty()).unwrap());
}

#[test]
fn spaced_named_argument_key_parses_as_one_name() {
    // The formal parameter name itself may be multi-word (`start position`, DMN 1.4 §10.3.5's
    // `substring` signature) — a different mechanism from the names-with-spaces context merge:
    // a callee's own parameter names are static metadata, never context keys.
    assert_eq!(
        expressions::eval("substring(string: \"foobar\", start position: 3)", &empty()).unwrap(),
        FeelValue::String("obar".into())
    );
}

#[test]
fn spaced_named_argument_key_tolerates_space_before_colon() {
    // `start position :3` — whitespace between the multi-word key and `:` is inconsequential.
    assert_eq!(
        expressions::eval("substring(string: \"foobar\", start position :3)", &empty()).unwrap(),
        FeelValue::String("obar".into())
    );
}

#[test]
fn instance_of_function() {
    let f = expressions::eval("function(a) a", &empty()).unwrap();
    let c = ctx(vec![("f", f)]);
    assert!(expressions::eval_boolean("f instance of function", &c).unwrap());
    assert!(!expressions::eval_boolean("1 instance of function", &empty()).unwrap());
}

// ----- Postfix invocation of an arbitrary expression (DMN-TCK level 3) -----

#[test]
fn invoke_a_parenthesised_function_literal_immediately() {
    assert_num(
        expressions::eval("(function(a) a * 2)(5)", &empty()).unwrap(),
        "10",
    );
}

#[test]
fn invoke_chained_on_a_call_result() {
    // `f` returns a function (independent of its own argument — this engine's function values
    // capture no outer scope, params + builtins only); invoking the result is a second postfix
    // `(...)` on a `Call` node.
    let f = expressions::eval("function(a) function(b) b * 2", &empty()).unwrap();
    let c = ctx(vec![("f", f)]);
    assert_num(expressions::eval("f(999)(3)", &c).unwrap(), "6");
}

#[test]
fn invoking_a_non_function_value_is_a_type_error_not_a_parse_error() {
    // Parses fine (any expression may be postfix-invoked); a non-function callee is a runtime
    // type error (DMN-TCK 1131-feel-function-invocation's `null()`/`123()` — both errorResult).
    assert!(expressions::parse("null()").is_ok());
    assert!(expressions::eval("null()", &empty()).is_err());
    assert!(expressions::eval("123()", &empty()).is_err());
}

// ----- Filter expressions, projection, instance-of (DMN-TCK level 3) -----

#[test]
fn filter_by_index_one_based_and_negative() {
    assert_num(
        expressions::eval("[10, 20, 30][1]", &empty()).unwrap(),
        "10",
    );
    assert_num(
        expressions::eval("[10, 20, 30][-1]", &empty()).unwrap(),
        "30",
    );
    assert_eq!(
        expressions::eval("[10, 20, 30][9]", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn filter_by_predicate_with_element_scope() {
    // Elements are contexts; the predicate references the element's own field.
    let employees = FeelValue::List(vec![
        map(vec![
            ("dept", FeelValue::num("20")),
            ("name", FeelValue::String("Ann".into())),
        ]),
        map(vec![
            ("dept", FeelValue::num("10")),
            ("name", FeelValue::String("Bo".into())),
        ]),
        map(vec![
            ("dept", FeelValue::num("20")),
            ("name", FeelValue::String("Cy".into())),
        ]),
    ]);
    let c = ctx(vec![("Employees", employees)]);
    // Filter then project .name → ["Ann", "Cy"].
    assert_eq!(
        expressions::eval("Employees[dept = 20].name", &c).unwrap(),
        FeelValue::List(vec![
            FeelValue::String("Ann".into()),
            FeelValue::String("Cy".into()),
        ])
    );
}

#[test]
fn filter_by_predicate_item_variable() {
    assert_eq!(
        expressions::eval("[1, 2, 3, 4][item > 2]", &empty()).unwrap(),
        FeelValue::List(vec![FeelValue::num("3"), FeelValue::num("4")])
    );
}

#[test]
fn filter_element_field_named_item_shadows_the_synthetic_item_variable() {
    // DMN-TCK 0069-feel-list#decision026: `[{item: 1}, {item: 2}, {item: 3}][item >= 2]` — the
    // element's OWN `item` field must shadow the synthetic whole-element `item` convenience
    // binding, not the other way around (which would compare the whole context to a number and
    // error, not filter by the field).
    assert_eq!(
        expressions::eval("[{item: 1}, {item: 2}, {item: 3}][item >= 2]", &empty()).unwrap(),
        FeelValue::List(vec![
            map(vec![("item", FeelValue::num("2"))]),
            map(vec![("item", FeelValue::num("3"))]),
        ])
    );
}

#[test]
fn instance_of_scalar_and_temporal_types() {
    assert!(expressions::eval_boolean("1 instance of number", &empty()).unwrap());
    assert!(expressions::eval_boolean("\"x\" instance of string", &empty()).unwrap());
    assert!(!expressions::eval_boolean("1 instance of string", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"2019-03-31\" instance of date", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"10:30:11\" instance of time", &empty()).unwrap());
    assert!(expressions::eval_boolean(
        "@\"2018-12-08T10:30:11\" instance of date and time",
        &empty()
    )
    .unwrap());
    assert!(
        expressions::eval_boolean("@\"P10D\" instance of days and time duration", &empty())
            .unwrap()
    );
    assert!(
        expressions::eval_boolean("@\"P10Y\" instance of years and months duration", &empty())
            .unwrap()
    );
    assert!(expressions::eval_boolean("[1, 2] instance of list", &empty()).unwrap());
}

// ----- Temporal: @-literals, types, builtins, arithmetic (DMN-TCK level 3) -----

#[test]
fn temporal_literals_and_equality() {
    assert!(expressions::eval_boolean("@\"2021-01-01\" = @\"2021-01-01\"", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"10:10:10\" = @\"10:10:10\"", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"P1D\" = @\"P1D\"", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"P1Y\" = @\"P1Y\"", &empty()).unwrap());
    assert!(!expressions::eval_boolean("@\"P1D\" = @\"P2D\"", &empty()).unwrap());
}

#[test]
fn unrecognised_temporal_literal_is_a_semantic_rejection_not_a_syntax_error() {
    // DMN-TCK 0093-feel-at-literals#test_001: `@"foo"` is a well-formed TOKEN (an `@` followed by
    // a quoted string) whose CONTENT just isn't a valid temporal value — the same
    // `SUTRA.FEEL.COMPILE.TYPE_MISMATCH` rejection an invalid `date("foo")` argument gets, not a
    // `SUTRA.FEEL.SYNTAX.*` code (which would mean the engine couldn't parse the construct at
    // all, not true here).
    let err = expressions::eval(r#"@"foo""#, &empty()).unwrap_err();
    assert!(!err.code.contains("SYNTAX"), "got code: {}", err.code);
    // A genuinely malformed token shape (no quote at all after `@`) is still a real syntax error.
    let err2 = expressions::eval("@123", &empty()).unwrap_err();
    assert!(err2.code.contains("SYNTAX"), "got code: {}", err2.code);
}

#[test]
fn temporal_ordering() {
    assert!(expressions::eval_boolean("@\"2021-01-01\" < @\"2021-02-01\"", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"09:00:00\" < @\"10:00:00\"", &empty()).unwrap());
    assert!(expressions::eval_boolean("@\"P1D\" < @\"P2D\"", &empty()).unwrap());
}

#[test]
fn temporal_constructors_and_accessors() {
    assert!(expressions::eval_boolean("date(2021, 1, 15) = @\"2021-01-15\"", &empty()).unwrap());
    assert_num(
        expressions::eval("year(@\"2021-06-15\")", &empty()).unwrap(),
        "2021",
    );
    assert_num(
        expressions::eval("month(@\"2021-06-15\")", &empty()).unwrap(),
        "6",
    );
    assert_num(
        expressions::eval("day(@\"2021-06-15\")", &empty()).unwrap(),
        "15",
    );
    assert_num(
        expressions::eval("hour(@\"13:20:30\")", &empty()).unwrap(),
        "13",
    );
    assert_eq!(
        expressions::eval("duration(\"P2D\")", &empty()).unwrap(),
        expressions::eval("@\"P2D\"", &empty()).unwrap()
    );
}

#[test]
fn temporal_arithmetic() {
    // date + days-time duration
    assert!(
        expressions::eval_boolean("@\"2021-01-01\" + @\"P1D\" = @\"2021-01-02\"", &empty())
            .unwrap()
    );
    // date + years-months duration (clamps into February)
    assert!(
        expressions::eval_boolean("@\"2021-01-31\" + @\"P1M\" = @\"2021-02-28\"", &empty())
            .unwrap()
    );
    // date − date → days-time duration
    assert!(
        expressions::eval_boolean("@\"2021-01-03\" - @\"2021-01-01\" = @\"P2D\"", &empty())
            .unwrap()
    );
    // duration + duration
    assert!(expressions::eval_boolean("@\"P1D\" + @\"P1D\" = @\"P2D\"", &empty()).unwrap());
    // duration × number
    assert!(expressions::eval_boolean("@\"P1Y\" * 2 = @\"P2Y\"", &empty()).unwrap());
}

// ----- Cycle 4: TimeQualifier reshape (Zulu/Offset/Zone round-trip) -----

#[test]
fn time_qualifier_round_trips_local_zulu_offset_and_zone() {
    // Bare local (no offset/zone at all) round-trips with no suffix.
    assert_eq!(
        expressions::eval(r#"string(date and time("2011-12-31T10:15:30"))"#, &empty()).unwrap(),
        FeelValue::String("2011-12-31T10:15:30".into())
    );
    // A literal `Z` stays `Z`, never resolved to `+00:00`.
    assert_eq!(
        expressions::eval(r#"string(date and time("2011-12-31T10:15:30Z"))"#, &empty()).unwrap(),
        FeelValue::String("2011-12-31T10:15:30Z".into())
    );
    // An explicit numeric offset round-trips exactly (not normalized to UTC).
    assert_eq!(
        expressions::eval(
            r#"string(date and time("2011-12-31T10:15:30+11:00"))"#,
            &empty()
        )
        .unwrap(),
        FeelValue::String("2011-12-31T10:15:30+11:00".into())
    );
    // An `@Zone` suffix round-trips as the zone NAME, not a resolved numeric offset.
    assert_eq!(
        expressions::eval(
            r#"string(date and time("2011-12-31T10:15:30@Australia/Melbourne"))"#,
            &empty()
        )
        .unwrap(),
        FeelValue::String("2011-12-31T10:15:30@Australia/Melbourne".into())
    );
    // Bare local `time()` also renders with no suffix.
    assert_eq!(
        expressions::eval(r#"string(time("11:30:00"))"#, &empty()).unwrap(),
        FeelValue::String("11:30:00".into())
    );
}

#[test]
fn date_and_time_timezone_property_is_the_zone_name_only_when_zone_qualified() {
    // DMN-TCK 0074-feel-properties#dateTime_009: `.timezone` is the IANA zone NAME, distinct
    // from `.time offset`'s numeric duration.
    assert_eq!(
        expressions::eval(
            r#"date and time("2018-12-10T10:30:00@Etc/UTC").timezone"#,
            &empty()
        )
        .unwrap(),
        FeelValue::from("Etc/UTC")
    );
    // A "local" (no offset/zone in the source) value has no zone name to report — DMN-TCK
    // #dateTime_009_a.
    assert_eq!(
        expressions::eval(r#"date and time("2018-12-10T10:30:00").timezone"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn time_qualifier_rejects_invalid_offset_and_zone_combinations() {
    assert!(expressions::eval(r#"time("13:20:00+19:00")"#, &empty()).is_err()); // outside ±14:00
    assert!(expressions::eval(r#"time("13:20:00+5")"#, &empty()).is_err()); // malformed (no minutes)
    assert!(expressions::eval(r#"time("13:20:00+02:00@Europe/Paris")"#, &empty()).is_err()); // offset AND zone
    assert!(
        expressions::eval(r#"date and time("2017-12-31T13:20:00@xyz/abc")"#, &empty()).is_err()
    ); // unknown zone
}

#[test]
fn date_and_time_accepts_a_date_and_time_first_argument() {
    // The first argument accepts either a bare `date` or a `date and time` value (only its date
    // portion is used) — DMN-TCK 1117's biggest ERR bucket in the temporal slice.
    assert_eq!(
        expressions::eval(
            r#"string(date and time(date and time("2017-08-10T10:20:00"), time("23:59:01")))"#,
            &empty()
        )
        .unwrap(),
        FeelValue::String("2017-08-10T23:59:01".into())
    );
    // The second argument's offset carries through onto the combined result.
    assert_eq!(
        expressions::eval(
            r#"string(date and time(date and time("2017-09-05T10:20:00"), time("09:15:30+02:00")))"#,
            &empty()
        )
        .unwrap(),
        FeelValue::String("2017-09-05T09:15:30+02:00".into())
    );
}

#[test]
fn time_builtin_accepts_a_bare_date_and_a_fractional_offset_seconds() {
    // `time(date)` promotes to midnight, explicit UTC (DMN-TCK 1116 `#053`).
    assert_eq!(
        expressions::eval(r#"string(time(date("2017-08-10")))"#, &empty()).unwrap(),
        FeelValue::String("00:00:00Z".into())
    );
    // `second` may carry a fractional part; the 4th argument (a `dayTimeDuration`) becomes the
    // offset (DMN-TCK 0007 `Time3`).
    assert_eq!(
        expressions::eval(r#"string(time(12, 59, 1.3, duration("-PT1H")))"#, &empty()).unwrap(),
        FeelValue::String("12:59:01.3-01:00".into())
    );
    // `hour`/`minute` still reject a fractional value.
    assert!(expressions::eval("time(12.5, 59, 1)", &empty()).is_err());
}

#[test]
fn date_and_time_parses_local_fractional_seconds() {
    assert!(expressions::eval_boolean(
        r#"date and time("2017-12-31T11:22:33.345") instance of date and time"#,
        &empty()
    )
    .unwrap());
    assert_num(
        expressions::eval(
            r#"second(date and time("2015-12-31T23:59:59.9999999"))"#,
            &empty(),
        )
        .unwrap(),
        "59",
    );
}

#[test]
fn date_and_time_parses_5_and_6_digit_years() {
    assert_num(
        expressions::eval(r#"year(date and time("99999-12-31T11:22:33"))"#, &empty()).unwrap(),
        "99999",
    );
    assert_num(
        expressions::eval(r#"year(date and time("-99999-12-31T11:22:33"))"#, &empty()).unwrap(),
        "-99999",
    );
    assert_num(
        expressions::eval(r#"year(date("99999-12-31"))"#, &empty()).unwrap(),
        "99999",
    );
}

#[test]
fn zero_value_years_months_duration_is_not_a_days_time_duration() {
    assert!(
        expressions::eval_boolean(r#"@"P0Y" instance of years and months duration"#, &empty())
            .unwrap()
    );
    assert!(
        expressions::eval_boolean(r#"@"P0M" instance of years and months duration"#, &empty())
            .unwrap()
    );
    // The two flavours never inter-compare, even both-zero (DMN-TCK 0103 `is(@"P0Y", @"P0D")`).
    assert!(!expressions::eval_boolean(r#"is(@"P0Y", @"P0D")"#, &empty()).unwrap());
    assert!(expressions::eval_boolean(r#"@"P1Y" + @"P0M" = @"P1Y""#, &empty()).unwrap());
}

#[test]
fn hyphenated_names_tokenize_as_a_single_identifier() {
    let c = ctx(vec![("Date-Time2", FeelValue::from("hi"))]);
    assert_eq!(
        expressions::eval("Date-Time2", &c).unwrap(),
        FeelValue::String("hi".into())
    );
    // A hyphen followed by a digit is still subtraction, not a name continuation.
    assert_num(expressions::eval("5-1", &empty()).unwrap(), "4");
}

#[test]
fn now_and_today_reject_extra_arguments() {
    assert!(expressions::eval_boolean("now() instance of date and time", &empty()).unwrap());
    assert!(expressions::eval_boolean("today() instance of date", &empty()).unwrap());
    assert!(expressions::eval("now(123)", &empty()).is_err());
    assert!(expressions::eval("today(123)", &empty()).is_err());
}

#[test]
fn day_and_week_ordinal_builtins_reject_bad_arity_and_names() {
    // A wrong-named argument is caught by the named-arg table (not a positional fallback).
    assert_eq!(
        expressions::eval(r#"day of week(value: @"1970-01-01")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    // Too many arguments is a strict arity error.
    assert!(expressions::eval(r#"day of week(@"1970-01-01", @"1970-01-01")"#, &empty()).is_err());
    assert_eq!(
        expressions::eval(r#"day of week(@"1970-01-01")"#, &empty()).unwrap(),
        FeelValue::String("Thursday".into())
    );
}

#[test]
fn duration_scaled_by_a_fractional_number_truncates_the_product_not_the_multiplier() {
    // -2.5 * 23 months = -57.5 -> truncates toward zero to -57 months, not -46 (which is what
    // truncating the multiplier to -2 first would give).
    assert!(expressions::eval_boolean(r#"-2.5 * @"P1Y11M" = @"-P4Y9M""#, &empty()).unwrap());
    // 131 months / 2.5 = 52.4 -> 52 months (4y4m), not 65 (131 / 2).
    assert!(expressions::eval_boolean(r#"@"P10Y11M" / 2.5 = @"P4Y4M""#, &empty()).unwrap());
}

#[test]
fn duration_divided_by_same_flavour_duration_is_a_number() {
    assert_num(
        expressions::eval(r#"@"P10Y" / @"P5Y""#, &empty()).unwrap(),
        "2",
    );
    assert_num(
        expressions::eval(r#"@"P10D" / @"P5D""#, &empty()).unwrap(),
        "2",
    );
    assert!(expressions::eval(r#"@"P10Y" / @"P5D""#, &empty()).is_err()); // cross-flavour
}

#[test]
fn negative_and_extended_years_render_without_the_debug_fallback() {
    assert_eq!(
        expressions::eval(
            r#"string(@"-2021-01-01T10:10:10+11:00" + @"P1M")"#,
            &empty()
        )
        .unwrap(),
        FeelValue::String("-2021-02-01T10:10:10+11:00".into())
    );
}

#[test]
fn instant_and_date_mixed_arithmetic() {
    // The `date` operand is implicitly midnight UTC.
    assert!(expressions::eval_boolean(
        r#"@"2021-01-02" - @"2021-01-01T10:10:10+11:00" = @"P1DT49M50S""#,
        &empty()
    )
    .unwrap());
}

#[test]
fn end_of_day_literal_rolls_over_to_midnight_next_day() {
    assert!(expressions::eval_boolean(
        r#"@"2021-01-01T24:00:00" = @"2021-01-02T00:00:00""#,
        &empty()
    )
    .unwrap());
}

#[test]
fn date_minus_days_time_duration_uses_full_sub_day_precision() {
    assert!(
        expressions::eval_boolean(r#"@"2021-01-02" - @"PT1H" = @"2021-01-01""#, &empty()).unwrap()
    );
    assert!(
        expressions::eval_boolean(r#"@"2021-01-02" - @"PT25H" = @"2020-12-31""#, &empty()).unwrap()
    );
}

#[test]
fn date_and_time_minus_date_and_time_requires_matching_zone_presence() {
    // Zoned − local (and vice versa) is undefined — DMN's "both or neither operand may carry
    // zone info" rule.
    assert!(expressions::eval(
        r#"@"2021-01-02T10:10:10@Europe/Paris" - @"2021-01-01T10:10:10""#,
        &empty()
    )
    .is_err());
    assert!(expressions::eval(
        r#"@"2021-01-02T10:10:10" - @"2021-01-01T10:10:10+02:00""#,
        &empty()
    )
    .is_err());
    // Zoned − zoned, through two different (DST-correctly resolved) zones, is fine.
    assert!(expressions::eval_boolean(
        r#"@"2021-01-02T10:10:10@Europe/Paris" - @"2021-01-01T10:10:10@Asia/Dhaka" = @"P1DT5H""#,
        &empty()
    )
    .unwrap());
}

#[test]
fn equality_resolves_absolute_instant_at_whole_second_precision_ignoring_qualifier() {
    // Sub-second precision is invisible to `=` — DMN-TCK 0068's own description: "resolution is
    // to the second".
    assert!(expressions::eval_boolean(
        r#"@"2018-12-08T00:00:00.0001" = @"2018-12-08T00:00:00.0000""#,
        &empty()
    )
    .unwrap());
    // An explicit offset and a DST-correctly-equal-resolving `@Zone` compare equal (Europe/Paris
    // is +02:00 in October, +01:00 in February).
    assert!(expressions::eval_boolean(
        r#"@"2018-10-08T00:00:00+02:00" = @"2018-10-08T00:00:00@Europe/Paris""#,
        &empty()
    )
    .unwrap());
    assert!(expressions::eval_boolean(
        r#"@"2018-02-08T00:00:00+01:00" = @"2018-02-08T00:00:00@Europe/Paris""#,
        &empty()
    )
    .unwrap());
}

#[test]
fn is_distinguishes_offset_and_zone_identity_unlike_equality() {
    // `Z` and an explicit `+00:00` are the only two spellings `is()` treats as identical.
    assert!(expressions::eval_boolean(r#"is(@"23:00:50Z", @"23:00:50+00:00")"#, &empty()).unwrap());
    // No offset at all vs. an explicit UTC offset are NOT identical (unlike `=`).
    assert!(!expressions::eval_boolean(r#"is(@"23:00:50", @"23:00:50Z")"#, &empty()).unwrap());
    // A `@Zone` name is never identical to a numeric offset, even a matching one.
    assert!(!expressions::eval_boolean(
        r#"is(@"23:00:50@Australia/Melbourne", @"23:00:50+10:00")"#,
        &empty()
    )
    .unwrap());
    // Two different offsets resolving to the same absolute instant are still not `is`-identical.
    assert!(!expressions::eval_boolean(
        r#"is(@"2002-04-02T12:00:00-01:00", @"2002-04-02T17:00:00+04:00")"#,
        &empty()
    )
    .unwrap());
}

// ----- Quantifiers, comprehensions, ranges (DMN-TCK level 3) -----

#[test]
fn for_comprehension_maps_over_a_list() {
    assert_eq!(
        expressions::eval("for h in [1, 2, 3] return h + 1", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("2"),
            FeelValue::num("3"),
            FeelValue::num("4"),
        ])
    );
}

#[test]
fn for_over_a_numeric_range() {
    assert_eq!(
        expressions::eval("for i in 0..3 return i", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("0"),
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("3"),
        ])
    );
}

#[test]
fn for_cartesian_over_two_sources() {
    // for h in [1,2], w in [10,20] return h*w  → [10,20,20,40]
    assert_eq!(
        expressions::eval("for h in [1, 2], w in [10, 20] return h * w", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("10"),
            FeelValue::num("20"),
            FeelValue::num("20"),
            FeelValue::num("40"),
        ])
    );
}

#[test]
fn for_partial_accumulator_supports_a_running_factorial() {
    // DMN-TCK 0084-feel-for-loops#decision_013: `partial` is FEEL's own implicit for-loop
    // accumulator — the list of results from every PRIOR iteration, visible to the current one.
    assert_eq!(
        expressions::eval(
            "for i in 0..4 return if i = 0 then 1 else i * partial[-1]",
            &empty()
        )
        .unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("6"),
            FeelValue::num("24"),
        ])
    );
}

#[test]
fn for_over_a_date_range_steps_one_day_at_a_time() {
    // DMN-TCK 0084-feel-for-loops#decision_017/#decision_018: a `date` range iterates
    // day-by-day, in whichever direction the bare `a..b` domain's own endpoints imply.
    assert_eq!(
        expressions::eval(
            r#"for i in @"1980-01-01"..@"1980-01-03" return i"#,
            &empty()
        )
        .unwrap(),
        FeelValue::List(vec![
            expressions::eval(r#"@"1980-01-01""#, &empty()).unwrap(),
            expressions::eval(r#"@"1980-01-02""#, &empty()).unwrap(),
            expressions::eval(r#"@"1980-01-03""#, &empty()).unwrap(),
        ])
    );
    assert_eq!(
        expressions::eval(
            r#"for i in @"1980-01-03"..@"1980-01-01" return i"#,
            &empty()
        )
        .unwrap(),
        FeelValue::List(vec![
            expressions::eval(r#"@"1980-01-03""#, &empty()).unwrap(),
            expressions::eval(r#"@"1980-01-02""#, &empty()).unwrap(),
            expressions::eval(r#"@"1980-01-01""#, &empty()).unwrap(),
        ])
    );
}

#[test]
fn some_and_every_quantifiers() {
    assert!(expressions::eval_boolean("some i in [1, 2, 3] satisfies i > 2", &empty()).unwrap());
    assert!(!expressions::eval_boolean("some i in [1, 2, 3] satisfies i > 5", &empty()).unwrap());
    assert!(expressions::eval_boolean("every i in [2, 4, 6] satisfies even(i)", &empty()).unwrap());
    assert!(
        !expressions::eval_boolean("every i in [2, 3, 4] satisfies even(i)", &empty()).unwrap()
    );
}

#[test]
fn range_iterates_descending_and_is_a_range_value() {
    // The bracket-less `for i in a..b` iteration-domain form counts down when `a > b` — this is
    // FEEL's dedicated bidirectional for-loop/quantifier domain syntax (DMN-TCK
    // 0084-feel-for-loops#decision_007/008/009), not a range VALUE.
    assert_eq!(
        expressions::eval("for i in 3..1 return i", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("3"),
            FeelValue::num("2"),
            FeelValue::num("1"),
        ])
    );
    // Corrected semantics (cycle 6, maintainer-approved — supersedes this test's earlier
    // assumption that a BRACKETED interval literal used directly as a for-loop source also
    // expands bidirectionally): `[a..b]` is a genuine range VALUE (DMN §10.3.2.11); iterating a
    // descending one directly is invalid, not a silent countdown (DMN-TCK
    // 0084-feel-for-loops#decision_025: "invalid range gives null (ranges may be descending)").
    assert!(expressions::eval("for i in [3..1] return i", &empty()).is_err());
    // It is still a well-formed range VALUE otherwise (containment/`instance of` unaffected).
    assert!(expressions::eval_boolean("[3..1] instance of range", &empty()).unwrap());
}

#[test]
fn range_containment_open_and_closed_bounds() {
    assert!(expressions::eval_boolean("5 in [1..10]", &empty()).unwrap());
    assert!(!expressions::eval_boolean("10 in [1..10)", &empty()).unwrap()); // upper exclusive
    assert!(!expressions::eval_boolean("1 in (1..10]", &empty()).unwrap()); // lower exclusive
    assert!(expressions::eval_boolean("2.5 in [1..10]", &empty()).unwrap()); // non-integer containment
                                                                             // temporal range containment
    assert!(expressions::eval_boolean(
        "@\"2021-06-15\" in [@\"2021-01-01\"..@\"2021-12-31\"]",
        &empty()
    )
    .unwrap());
}

#[test]
fn range_builtin_from_string() {
    assert!(expressions::eval_boolean("2 in range(\"[1..3]\")", &empty()).unwrap());
    assert!(expressions::eval_boolean("range(\"[1..3]\") instance of range", &empty()).unwrap());
}

// ----- Comparison-operator range value (DMN-TCK level 3, 0068-feel-equality) -----

#[test]
fn open_range_equals_itself_but_not_a_literal_range_with_the_same_bound() {
    assert!(expressions::eval_boolean("(< 10) = (< 10)", &empty()).unwrap());
    assert!(expressions::eval_boolean("(=10) = (=10)", &empty()).unwrap());
    assert!(expressions::eval_boolean("(!=10) = (!=10)", &empty()).unwrap());
    // A comparison-form range must NOT structurally equal an ordinary interval literal sharing
    // the same numeric bound, even though both denote "everything below 10" / "exactly 10".
    assert!(!expressions::eval_boolean("(< 10) = (null..10)", &empty()).unwrap());
    assert!(!expressions::eval_boolean("(=10) = [10..10]", &empty()).unwrap());
}

#[test]
fn range_builtin_rejects_a_comparison_operator_string() {
    // DMN-TCK 1156-range-function#017: "a unary range is not a valid literal range string" —
    // `range(">=10")` must error, even though `>=10` parses fine as a bare expression elsewhere.
    assert!(expressions::eval("range(\">=10\")", &empty()).is_err());
}

#[test]
fn open_range_membership_reduces_to_its_operator() {
    assert!(expressions::eval_boolean("5 in (< 10)", &empty()).unwrap());
    assert!(!expressions::eval_boolean("15 in (< 10)", &empty()).unwrap());
    assert!(expressions::eval_boolean("15 in (>= 10)", &empty()).unwrap());
    assert!(expressions::eval_boolean("10 in (=10)", &empty()).unwrap());
    assert!(!expressions::eval_boolean("10 in (!=10)", &empty()).unwrap());
}

// ----- Alternate ISO interval bracket spellings (DMN-TCK level 3, 0068-feel-equality) -----

#[test]
fn leading_rbracket_is_an_alternate_exclusive_lower_bound() {
    // `]` opens an interval the same way `(` does (DMN 1.4 §10.3.1.2's "French" spelling).
    assert!(expressions::eval_boolean("(1..10] = ]1..10]", &empty()).unwrap());
}

#[test]
fn trailing_lbracket_is_an_alternate_exclusive_upper_bound() {
    // `[` closes an interval the same way `)` does — disambiguated from a postfix filter start.
    assert!(expressions::eval_boolean("[1..10) = [1..10[", &empty()).unwrap());
}

#[test]
fn range_builtin_accepts_alternate_bracket_spelling() {
    assert!(expressions::eval_boolean("range(\"]18..21]\") = (18..21]", &empty()).unwrap());
    assert!(expressions::eval_boolean("range(\"[18..21[\") = [18..21)", &empty()).unwrap());
}

// ----- Builtin functions batch (DMN-TCK level 3) -----

#[test]
fn numeric_unary_builtins() {
    assert_num(expressions::eval("abs(-7)", &empty()).unwrap(), "7");
    assert_num(expressions::eval("ceiling(1.01)", &empty()).unwrap(), "2");
    assert_num(expressions::eval("floor(1.99)", &empty()).unwrap(), "1");
    assert_num(expressions::eval("sqrt(16)", &empty()).unwrap(), "4");
    assert_num(expressions::eval("modulo(12, 5)", &empty()).unwrap(), "2");
    assert_num(expressions::eval("decimal(2.5, 0)", &empty()).unwrap(), "2"); // half-even
    assert!(expressions::eval_boolean("even(4)", &empty()).unwrap());
    assert!(expressions::eval_boolean("odd(3)", &empty()).unwrap());
}

#[test]
fn modulo_takes_sign_of_divisor() {
    assert_num(expressions::eval("modulo(-12, 5)", &empty()).unwrap(), "3");
    assert_num(expressions::eval("modulo(12, -5)", &empty()).unwrap(), "-3");
}

#[test]
fn modulo_never_coerces_a_non_number_argument() {
    // DMN-TCK 0056-feel-modulo-function#decision008_b/#decision009: unlike `to_big_decimal`'s
    // general leniency (null → 0, numeric strings parsed), `modulo` itself must reject a
    // non-number dividend/divisor as `null`.
    assert_eq!(
        expressions::eval("modulo(null, 4)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"modulo("10", "4")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    // An unrecognized named argument is invalid — `modulo`'s own declared names are `dividend`/
    // `divisor`, not `foo` (DMN-TCK #decision007).
    assert_eq!(
        expressions::eval("modulo(dividend: 10, foo: 4)", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn log_rejects_an_unrecognized_named_argument() {
    // DMN-TCK 0053-feel-log-function#decision007: `log`'s own declared parameter name is
    // `number`, not `n`.
    assert_eq!(
        expressions::eval("log(n: 4)", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn aggregation_over_list_and_varargs() {
    assert_num(
        expressions::eval("sum([1, 2, 3, 4])", &empty()).unwrap(),
        "10",
    );
    assert_num(
        expressions::eval("count([1, 2, 3])", &empty()).unwrap(),
        "3",
    );
    assert_num(expressions::eval("min(3, 1, 2)", &empty()).unwrap(), "1");
    assert_num(expressions::eval("max([3, 1, 2])", &empty()).unwrap(), "3");
    assert_num(expressions::eval("mean([2, 4, 6])", &empty()).unwrap(), "4");
    assert_num(
        expressions::eval("median([1, 2, 3, 4])", &empty()).unwrap(),
        "2.5",
    );
    assert_num(
        expressions::eval("product([2, 3, 4])", &empty()).unwrap(),
        "24",
    );
}

#[test]
fn product_of_no_elements_is_null_not_the_multiplicative_identity() {
    // DMN-TCK 0094-feel-product-function#decision002/#decision003: both a truly zero-argument
    // call and a single-empty-list call are `null` — unlike `all`/`any` (whose empty-list case is
    // a distinct, valid vacuous truth), `product([])`/`product()` are simply undefined, not `1`.
    assert_eq!(
        expressions::eval("product([])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("product()", &empty()).unwrap(),
        FeelValue::Null
    );
    // An unrecognized named argument is invalid, not silently accepted positionally — DMN-TCK
    // #decision013: `product`'s own declared parameter name is `list`, not `l`.
    assert_eq!(
        expressions::eval("product(l: [2, 4, 7, 5])", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn boolean_aggregation_three_valued() {
    assert!(expressions::eval_boolean("all([true, true])", &empty()).unwrap());
    assert!(!expressions::eval_boolean("all([true, false])", &empty()).unwrap());
    assert!(expressions::eval_boolean("any([false, true])", &empty()).unwrap());
    assert!(!expressions::eval_boolean("any([false, false])", &empty()).unwrap());
}

#[test]
fn string_builtins() {
    assert_eq!(
        expressions::eval("substring(\"foobar\", 4)", &empty()).unwrap(),
        FeelValue::String("bar".into())
    );
    assert_eq!(
        expressions::eval("substring(\"foobar\", 1, 3)", &empty()).unwrap(),
        FeelValue::String("foo".into())
    );
    assert_num(
        expressions::eval("string length(\"héllo\")", &empty()).unwrap(),
        "5",
    );
    assert_eq!(
        expressions::eval("substring before(\"foobar\", \"bar\")", &empty()).unwrap(),
        FeelValue::String("foo".into())
    );
    assert_eq!(
        expressions::eval("upper case(\"abc\")", &empty()).unwrap(),
        FeelValue::String("ABC".into())
    );
}

#[test]
fn string_length_and_camelcase_builtins_alias() {
    // TCK uses the spaced form `string length`; the merge collapses it to `stringLength` only if
    // that is a context name — it is not, so assert the camelCase call form works directly.
    assert_num(
        expressions::eval("stringLength(\"abcd\")", &empty()).unwrap(),
        "4",
    );
}

#[test]
fn replace_and_split() {
    assert_eq!(
        expressions::eval("replace(\"abcabc\", \"a\", \"X\")", &empty()).unwrap(),
        FeelValue::String("XbcXbc".into())
    );
    assert_eq!(
        expressions::eval("split(\"a,b,c\", \",\")", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::String("a".into()),
            FeelValue::String("b".into()),
            FeelValue::String("c".into()),
        ])
    );
}

#[test]
fn replace_backreference_immediately_followed_by_a_literal_character() {
    // DMN-TCK 1109-feel-replace-function#015: `$1c$2` must be group-1, literal "c", group-2 —
    // NOT the `regex` crate's own greedy `$name` syntax mistaking "1c" for a group NAMED "1c"
    // (which doesn't exist, silently dropping both the reference and the literal that follows).
    assert_eq!(
        expressions::eval(r#"replace("darted","^(.*?)d(.*)$","$1c$2")"#, &empty()).unwrap(),
        FeelValue::String("carted".into())
    );
    // A literal `$$` (escaped dollar) still round-trips.
    assert_eq!(
        expressions::eval(r#"replace("5", "5", "$$5")"#, &empty()).unwrap(),
        FeelValue::String("$5".into())
    );
}

#[test]
fn list_builtins() {
    assert_eq!(
        expressions::eval("reverse([1, 2, 3])", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("3"),
            FeelValue::num("2"),
            FeelValue::num("1"),
        ])
    );
    assert_eq!(
        expressions::eval("distinct values([1, 2, 2, 3, 3, 3])", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("3"),
        ])
    );
    assert_eq!(
        expressions::eval("flatten([[1, 2], [3]])", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("3"),
        ])
    );
    assert_eq!(
        expressions::eval("sublist([1, 2, 3, 4], 2, 2)", &empty()).unwrap(),
        FeelValue::List(vec![FeelValue::num("2"), FeelValue::num("3")])
    );
    assert_num(
        expressions::eval("sum(concatenate([1, 2], [3, 4]))", &empty()).unwrap(),
        "10",
    );
}

#[test]
fn number_conversion() {
    assert_num(
        expressions::eval("number(\"1000.5\")", &empty()).unwrap(),
        "1000.5",
    );
    // grouping "." stripped, decimal "," normalized
    assert_num(
        expressions::eval("number(\"1.000.000,5\", \".\", \",\")", &empty()).unwrap(),
        "1000000.5",
    );
}

// ----- Literals -----

#[test]
fn number_literal() {
    assert_num(expressions::eval("42", &empty()).unwrap(), "42");
}

#[test]
fn decimal_literal() {
    assert_num(expressions::eval("3.14", &empty()).unwrap(), "3.14");
}

#[test]
fn leading_dot_number_literal() {
    // `.872` — a digit run with no integer part (DMN-TCK 0101-feel-constants, level 2).
    assert_num(expressions::eval(".5", &empty()).unwrap(), "0.5");
}

#[test]
fn negated_leading_dot_number_literal() {
    assert_num(expressions::eval("-.5", &empty()).unwrap(), "-0.5");
}

#[test]
fn unary_negation_is_not_desugared_to_zero_minus_arg() {
    // DMN-TCK 0099-arithmetic-negation#003/#003_a/#004/#004_a: unary `-` on a duration must
    // negate it directly. Desugaring to `0 - duration` would route it through binary
    // number-minus-duration arithmetic, which FEEL doesn't define at all (an unrelated,
    // pre-existing gap this must NOT reach).
    assert_eq!(
        expressions::eval(r#"-@"P1D""#, &empty()).unwrap(),
        expressions::eval(r#"@"-P1D""#, &empty()).unwrap()
    );
    assert_eq!(
        expressions::eval(r#"-@"-P1D""#, &empty()).unwrap(),
        expressions::eval(r#"@"P1D""#, &empty()).unwrap()
    );
    assert_eq!(
        expressions::eval(r#"-@"P1Y""#, &empty()).unwrap(),
        expressions::eval(r#"@"-P1Y""#, &empty()).unwrap()
    );
    // Plain number negation (the overwhelmingly common case) is unaffected.
    assert_num(expressions::eval("-10", &empty()).unwrap(), "-10");
    assert_num(expressions::eval("--10", &empty()).unwrap(), "10");
    // Negating anything else (date/time/context/string/list/range) is `null`, not an error —
    // there's no "zero" for those types to subtract from in the first place.
    assert_eq!(
        expressions::eval(r#"-@"2021-01-01""#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"-{a: 1}"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"-"10""#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn scientific_notation_number_literal() {
    // DMN-TCK 0068-feel-equality number_008/009/010: `1.23e4`, `1.23e+4`, `1.23e-4`.
    assert!(expressions::eval_boolean("12300 = 1.23e4", &empty()).unwrap());
    assert!(expressions::eval_boolean("12300 = 1.23e+4", &empty()).unwrap());
    assert!(expressions::eval_boolean("0.000123 = 1.23e-4", &empty()).unwrap());
    // A bare trailing `e`/`E` with no exponent digits is never part of the numeric literal —
    // parses as `1` followed by the identifier `e`, an ordinary (unresolved) name reference.
    assert!(expressions::eval("1e", &empty()).is_err());
}

#[test]
fn string_literal_double_quoted() {
    assert_eq!(
        expressions::eval("\"hello\"", &empty()).unwrap(),
        FeelValue::from("hello")
    );
}

#[test]
fn string_literal_single_quoted() {
    assert_eq!(
        expressions::eval("'hello'", &empty()).unwrap(),
        FeelValue::from("hello")
    );
}

#[test]
fn boolean_literals() {
    assert!(expressions::eval_boolean("true", &empty()).unwrap());
    assert!(!expressions::eval_boolean("false", &empty()).unwrap());
}

#[test]
fn null_literal() {
    assert_eq!(
        expressions::eval("null", &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Comments (DMN-TCK level 3, 0073-feel-comments) -----

#[test]
fn line_comment_is_skipped_to_end_of_line() {
    assert_num(
        expressions::eval("1 + 1 // trailing comment", &empty()).unwrap(),
        "2",
    );
}

#[test]
fn block_comment_is_skipped_mid_expression() {
    assert_num(expressions::eval("1 + /* 1 + */ 1", &empty()).unwrap(), "2");
}

#[test]
fn unterminated_block_comment_is_a_lexer_error() {
    assert!(expressions::parse("1 + /* oops").is_err());
}

// ----- Path access -----

#[test]
fn single_segment_path() {
    let c = ctx(vec![("foo", FeelValue::from("bar"))]);
    assert_eq!(
        expressions::eval("foo", &c).unwrap(),
        FeelValue::from("bar")
    );
}

#[test]
fn nested_path() {
    let c = ctx(vec![(
        "payload",
        map(vec![
            ("orderId", FeelValue::from("ORD-123")),
            ("amount", FeelValue::from(42)),
        ]),
    )]);
    assert_eq!(
        expressions::eval("payload.orderId", &c).unwrap(),
        FeelValue::from("ORD-123")
    );
}

#[test]
fn deeply_nested_path() {
    let c = ctx(vec![(
        "a",
        map(vec![("b", map(vec![("c", FeelValue::from("deep"))]))]),
    )]);
    assert_eq!(
        expressions::eval("a.b.c", &c).unwrap(),
        FeelValue::from("deep")
    );
}

#[test]
fn dotted_path_continuation_reaches_temporal_component_properties() {
    // DMN-TCK 0007-date-time's `Date.fromString.day`: the first hop (`Date.fromString`) lands on
    // a `Date` VALUE (not a `Map`), so the second hop (`.day`) needs a temporal component
    // property, not a context-key lookup — a multi-segment dotted path must resolve every
    // continuation segment through the same logic a `FieldAccess` postfix would (`field_access`),
    // not just a `Map` lookup that silently goes `null` the moment an intermediate value isn't a
    // `Map`.
    let c = ctx(vec![(
        "Date",
        map(vec![(
            "fromString",
            FeelValue::Date(datetime!(2015-12-24 0:00 UTC).date()),
        )]),
    )]);
    assert_num(expressions::eval("Date.fromString.day", &c).unwrap(), "24");
    assert_num(
        expressions::eval("Date.fromString.year", &c).unwrap(),
        "2015",
    );
}

#[test]
fn missing_path_yields_null() {
    let c = ctx(vec![(
        "payload",
        map(vec![("orderId", FeelValue::from("ORD-123"))]),
    )]);
    assert_eq!(
        expressions::eval("payload.missing", &c).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("nothing.at.all", &c).unwrap(),
        FeelValue::Null
    );
}

// ----- Comparison -----

#[test]
fn equality_on_strings() {
    let c = ctx(vec![("name", FeelValue::from("Acme"))]);
    assert!(expressions::eval_boolean("name = \"Acme\"", &c).unwrap());
    assert!(!expressions::eval_boolean("name != \"Acme\"", &c).unwrap());
}

#[test]
fn numeric_comparison() {
    let c = ctx(vec![("amount", FeelValue::from(150))]);
    assert!(expressions::eval_boolean("amount > 100", &c).unwrap());
    assert!(expressions::eval_boolean("amount >= 150", &c).unwrap());
    assert!(!expressions::eval_boolean("amount < 100", &c).unwrap());
    assert!(!expressions::eval_boolean("amount <= 149", &c).unwrap());
}

// ----- Boolean -----

#[test]
fn and_or_not() {
    let c = ctx(vec![("x", FeelValue::from(5)), ("y", FeelValue::from(10))]);
    assert!(expressions::eval_boolean("x > 0 and y > x", &c).unwrap());
    assert!(!expressions::eval_boolean("x > 0 and y < x", &c).unwrap());
    assert!(expressions::eval_boolean("x > 100 or y > x", &c).unwrap());
    assert!(expressions::eval_boolean("not(x > 100)", &c).unwrap());
}

#[test]
fn operator_precedence_and_over_or() {
    // a or b and c = a or (b and c)
    assert!(expressions::eval_boolean("true or false and false", &empty()).unwrap());
}

// ----- Arithmetic -----

#[test]
fn add_subtract_multiply_divide() {
    let c = ctx(vec![("amount", FeelValue::from(100))]);
    assert_num(expressions::eval("amount + 50", &c).unwrap(), "150");
    assert_num(expressions::eval("amount - 20", &c).unwrap(), "80");
    assert_num(expressions::eval("amount * 2", &c).unwrap(), "200");
    assert_num(expressions::eval("amount / 4", &c).unwrap(), "25");
}

#[test]
fn division_by_zero_is_null() {
    assert_eq!(
        expressions::eval("10 / 0", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn string_concatenation_with_plus() {
    let c = ctx(vec![
        ("a", FeelValue::from("ORD-")),
        ("b", FeelValue::from("123")),
    ]);
    assert_eq!(
        expressions::eval("a + b", &c).unwrap(),
        FeelValue::from("ORD-123")
    );
}

// ----- Conditional -----

#[test]
fn if_then_else() {
    let c = ctx(vec![("amount", FeelValue::from(150))]);
    assert_eq!(
        expressions::eval("if amount > 100 then \"high\" else \"low\"", &c).unwrap(),
        FeelValue::from("high")
    );
    assert_eq!(
        expressions::eval("if amount > 1000 then \"high\" else \"low\"", &c).unwrap(),
        FeelValue::from("low")
    );
}

// ----- Builtin functions -----

#[test]
fn matches_uses_regex() {
    let c = ctx(vec![(
        "messageType",
        FeelValue::from("order.created.001.08"),
    )]);
    assert!(expressions::eval_boolean("matches(messageType, \"order.created.*\")", &c).unwrap());
}

#[test]
fn contains_and_starts_with_ends_with() {
    let c = ctx(vec![("traceRef", FeelValue::from("ABC-123-XYZ"))]);
    assert!(expressions::eval_boolean("contains(traceRef, \"123\")", &c).unwrap());
    assert!(expressions::eval_boolean("startsWith(traceRef, \"ABC\")", &c).unwrap());
    assert!(expressions::eval_boolean("endsWith(traceRef, \"XYZ\")", &c).unwrap());
}

#[test]
fn contains_never_treats_null_as_the_empty_string() {
    // DMN-TCK 1110-feel-contains-function ErrorCase_001/002/003: a `null` operand must make the
    // WHOLE call `null` — not vacuously `true` via `str_of`'s OWN "null → empty string" leniency
    // (every string "contains" the empty string).
    assert_eq!(
        expressions::eval("contains(null, null)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"contains(null, "bar")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"contains("bar", null)"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn case_conversion() {
    let c = ctx(vec![("v", FeelValue::from("Hello"))]);
    assert_eq!(
        expressions::eval("upperCase(v)", &c).unwrap(),
        FeelValue::from("HELLO")
    );
    assert_eq!(
        expressions::eval("lowerCase(v)", &c).unwrap(),
        FeelValue::from("hello")
    );
}

#[test]
fn exists_checks_path_presence() {
    let c = ctx(vec![(
        "payload",
        map(vec![("traceRef", FeelValue::from("U-1"))]),
    )]);
    assert!(expressions::eval_boolean("exists(payload.traceRef)", &c).unwrap());
    assert!(!expressions::eval_boolean("exists(payload.endToEndId)", &c).unwrap());
}

#[test]
fn is_blank_detects_missing_empty_and_whitespace_only() {
    let c = ctx(vec![
        ("empty", FeelValue::from("")),
        ("spaces", FeelValue::from("   ")),
        ("id", FeelValue::from("E2E-1")),
        ("count", FeelValue::from(0)),
    ]);
    assert!(expressions::eval_boolean("isBlank(missing.path)", &c).unwrap());
    assert!(expressions::eval_boolean("isBlank(empty)", &c).unwrap());
    assert!(expressions::eval_boolean("isBlank(spaces)", &c).unwrap());
    assert!(!expressions::eval_boolean("isBlank(id)", &c).unwrap());
    // A non-string scalar is a present value, not blank.
    assert!(!expressions::eval_boolean("isBlank(count)", &c).unwrap());
}

#[test]
fn seconds_between_computes_signed_seconds() {
    let c = ctx(vec![
        ("createdAt", FeelValue::from("2026-07-11T10:00:00Z")),
        (
            "now",
            FeelValue::Instant(
                datetime!(2026-07-11 10:06:00 UTC),
                Some(TimeQualifier::Zulu),
            ),
        ),
    ]);
    assert_num(
        expressions::eval("secondsBetween(createdAt, now)", &c).unwrap(),
        "360.000",
    );
    // The two temporal shapes: staleness and (signed) future clock-skew.
    assert!(expressions::eval_boolean("secondsBetween(createdAt, now) > 300", &c).unwrap());
    assert!(!expressions::eval_boolean("secondsBetween(now, createdAt) > 30", &c).unwrap());
}

#[test]
fn seconds_between_accepts_offset_date_time_strings() {
    // 15:30+05:30 == 10:00Z — timestamp values commonly carry zone offsets.
    let c = ctx(vec![
        ("createdAt", FeelValue::from("2026-07-11T15:30:00+05:30")),
        ("now", FeelValue::from("2026-07-11T10:00:30Z")),
    ]);
    assert_num(
        expressions::eval("secondsBetween(createdAt, now)", &c).unwrap(),
        "30.000",
    );
}

#[test]
fn seconds_between_rejects_unparseable_temporal() {
    let c = ctx(vec![
        ("bad", FeelValue::from("not-a-timestamp")),
        (
            "now",
            FeelValue::Instant(
                datetime!(2026-07-11 10:00:00 UTC),
                Some(TimeQualifier::Zulu),
            ),
        ),
    ]);
    let err = expressions::eval("secondsBetween(bad, now)", &c).unwrap_err();
    assert!(err.message.contains("secondsBetween"), "{}", err.message);
}

#[test]
fn seconds_between_rejects_null_temporal() {
    let c = ctx(vec![(
        "now",
        FeelValue::Instant(
            datetime!(2026-07-11 10:00:00 UTC),
            Some(TimeQualifier::Zulu),
        ),
    )]);
    let err = expressions::eval("secondsBetween(missing.path, now)", &c).unwrap_err();
    assert!(err.message.contains("null"), "{}", err.message);
}

// ----- Truthiness -----

#[test]
fn null_is_falsy() {
    assert!(!expressions::eval_boolean("null", &empty()).unwrap());
    assert!(!expressions::eval_boolean("missing.path", &empty()).unwrap());
}

// ----- Parser errors -----

#[test]
fn unexpected_token_rejected() {
    assert!(expressions::parse("a + +").is_err());
}

#[test]
fn unexpected_token_carries_source_location() {
    // "a + +" — the unexpected '+' is at column 5 on line 1.
    let err = expressions::parse("a + +").unwrap_err();
    let loc = err.location.expect("location present");
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 5);
}

#[test]
fn unclosed_string_rejected() {
    assert!(expressions::parse("\"unclosed").is_err());
}

#[test]
fn unclosed_string_range_spans_from_opening_quote_to_end() {
    let err = expressions::parse("\"unclosed").unwrap_err();
    let loc = err.location.expect("location present");
    // Opening quote at column 1; end-of-input at column 10 (after 'd').
    assert_eq!(loc.line, 1);
    assert_eq!(loc.column, 1);
    assert_eq!(loc.end_column, Some(10));
}

#[test]
fn multi_line_expression_tracks_line_number() {
    // Trigger an unexpected-token error on line 2.
    let err = expressions::parse("a +\n  + b").unwrap_err();
    let loc = err.location.expect("location present");
    assert_eq!(loc.line, 2);
    // Column 3: two leading spaces on line 2, then the offending '+'.
    assert_eq!(loc.column, 3);
}

#[test]
fn unclosed_paren_rejected() {
    assert!(expressions::parse("(1 + 2").is_err());
}

#[test]
fn unknown_function_rejected() {
    assert!(expressions::eval("nonExistentFunction(1)", &empty()).is_err());
}

// ----- Cycle 3: named-argument -> declared-parameter binding for builtins -----

#[test]
fn named_arg_wrong_name_nulls_out_unary_numeric_builtins() {
    assert_eq!(
        expressions::eval("abs(number:-1)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("sqrt(n:4)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("even(n:4)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("odd(n:4)", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn named_arg_correct_name_binds_unary_numeric_builtins() {
    assert_num(expressions::eval("abs(n:-1)", &empty()).unwrap(), "1");
    assert_num(expressions::eval("sqrt(number:4)", &empty()).unwrap(), "2");
}

#[test]
fn abs_rejects_an_extra_positional_argument() {
    assert!(expressions::eval("abs(1,1)", &empty()).is_err());
}

#[test]
fn named_arg_wrong_name_nulls_out_aggregate_and_range_builtins() {
    assert_eq!(
        expressions::eval("median(l:[2,4,7,5])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("stddev(l:[2,4,7,5])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("all(l:[true])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("any(l:[true])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("mode(l:[2,4,7,5])", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"range(fron: "[1..3]")"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn named_args_reorder_by_declared_parameter_name() {
    assert_num(
        expressions::eval(
            r#"number(from: "1.000.000,01", decimal separator:",", grouping separator:".")"#,
            &empty(),
        )
        .unwrap(),
        "1000000.01",
    );
    assert_eq!(
        expressions::eval(
            r#"number(from: "1.000.000,01", decimal sep:",", grouping sep:".")"#,
            &empty(),
        )
        .unwrap(),
        FeelValue::Null
    );
}

#[test]
fn is_missing_named_argument_defaults_to_null_not_an_arity_error() {
    assert_eq!(
        expressions::eval("is(value1: 1)", &empty()).unwrap(),
        FeelValue::Boolean(false)
    );
}

// ----- Cycle 3: three-valued ordering / range containment null propagation -----

#[test]
fn ordering_comparison_with_a_null_operand_is_null_not_an_error() {
    assert_eq!(
        expressions::eval("1 < null", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("null > 1", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("1 <= null", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("null >= 1", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn range_containment_with_a_null_value_or_endpoint_is_null() {
    assert_eq!(
        expressions::eval("null in [1..10]", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("5 in [1..null]", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("5 in (null..10]", &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 3: context literal duplicate key -----

#[test]
fn context_literal_with_duplicate_key_is_null() {
    assert_eq!(
        expressions::eval(r#"{foo: "bar", foo: "baz"}"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 3: function literals capture their definition-site scope -----

#[test]
fn function_literal_closes_over_an_outer_variable() {
    let c = ctx(vec![("outer", FeelValue::from(10i64))]);
    assert_num(
        expressions::eval("(function(a) a * outer)(5)", &c).unwrap(),
        "50",
    );
}

#[test]
fn function_parameter_shadows_a_captured_variable_of_the_same_name() {
    let c = ctx(vec![("a", FeelValue::from(999i64))]);
    assert_num(expressions::eval("(function(a) a)(5)", &c).unwrap(), "5");
}

// ----- Cycle 3: 1140 string join -----

#[test]
fn string_join_binds_named_args_by_declared_name_and_reorders() {
    assert_eq!(
        expressions::eval(r#"string join(delimiter: "X", list: ["a","c"])"#, &empty()).unwrap(),
        FeelValue::from("aXc")
    );
}

#[test]
fn string_join_rejects_non_string_elements_and_non_list_non_string_scalars() {
    assert_eq!(
        expressions::eval(r#"string join([1,2,3], "X")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"string join(123, "X")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("string join(null)", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn string_join_coerces_a_bare_string_to_a_one_element_list() {
    assert_eq!(
        expressions::eval(r#"string join("a", "X")"#, &empty()).unwrap(),
        FeelValue::from("a")
    );
}

// ----- Cycle 3: 0067 split() -----

#[test]
fn split_rejects_null_string_or_delimiter() {
    assert_eq!(
        expressions::eval("split(null, null)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"split("foo", null)"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"split(null, ",")"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn split_rejects_an_unrecognized_named_argument() {
    assert_eq!(
        expressions::eval(r#"split(delimiter: ",", str:"foo,bar")"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 3: 0058 number() -----

#[test]
fn number_rejects_non_string_from_and_invalid_or_ambiguous_separators() {
    assert_eq!(
        expressions::eval(r#"number(123, ".", ".")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"number("1,000,000.01", ",", ":")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"number("1,000,000.01", ",", 123)"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"number("1,000,000.00", ",", ",")"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn number_accepts_a_null_separator_as_a_no_op() {
    assert_num(
        expressions::eval(r#"number("1000000.01", null, ".")"#, &empty()).unwrap(),
        "1000000.01",
    );
}

// ----- Cycle 3: 1111 matches() -----

#[test]
fn matches_rejects_null_or_non_string_input_pattern_or_flags() {
    assert_eq!(
        expressions::eval(r#"matches(null, "pattern")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"matches("input", null)"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"matches("input","pattern", [])"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn matches_rejects_invalid_flag_characters() {
    assert!(expressions::eval(r#"matches("abracadabra", "bra", "p")"#, &empty()).is_err());
    assert!(expressions::eval(r#"matches("input","pattern", " ")"#, &empty()).is_err());
    assert!(expressions::eval(r#"matches("input","pattern", "X")"#, &empty()).is_err());
}

#[test]
fn matches_dot_excludes_cr_in_default_mode() {
    // A literal CR between "Mary" and "Jones" (mirrors DMN-TCK's \u000D string escape) -- "."
    // must NOT match it in the default (non-DOTALL) mode, unlike the regex crate's own default
    // which only excludes LF.
    let expr = format!("matches(\"Mary{}Jones\", \"Mary.Jones\")", '\r');
    assert!(!expressions::eval_boolean(&expr, &empty()).unwrap());
}

#[test]
fn matches_extended_flag_collapses_whitespace_outside_character_classes() {
    assert!(
        expressions::eval_boolean(r#"matches("hello world", "hello\ sworld", "x")"#, &empty())
            .unwrap()
    );
    assert!(expressions::eval_boolean(
        r#"matches("hello world", " hello[ ]world", "x")"#,
        &empty()
    )
    .unwrap());
    assert!(expressions::eval_boolean(
        r#"matches("hello world", "\p{ IsBasicLatin}+", "x")"#,
        &empty()
    )
    .unwrap());
}

// ----- Cycle 3: 1156 range() dedicated literal-range-string grammar -----

#[test]
fn range_builtin_rejects_mismatched_endpoint_types_descending_and_null_endpoints() {
    assert!(expressions::eval(r#"range("[1..\"b\"]")"#, &empty()).is_err());
    assert!(expressions::eval(r#"range("[3..1]")"#, &empty()).is_err());
    assert!(expressions::eval(r#"range("[null..null]")"#, &empty()).is_err());
}

#[test]
fn range_builtin_rejects_a_non_string_argument_and_a_nested_non_literal_endpoint() {
    assert!(expressions::eval("range([1..3])", &empty()).is_err());
    assert!(expressions::eval(
        r#"range("[date(string(\"1970-01-01\"))..date(\"1970-01-02\")]")"#,
        &empty()
    )
    .is_err());
}

#[test]
fn range_builtin_accepts_a_literal_date_call_endpoint() {
    assert!(expressions::eval_boolean(
        r#"range("[date(\"1970-01-01\")..date(\"1970-01-02\")]") = [date("1970-01-01")..date("1970-01-02")]"#,
        &empty()
    )
    .unwrap());
}

// ----- Cycle 3: 0080/0081 getValue() / getEntries() -----

#[test]
fn get_value_looks_up_a_context_key_and_nulls_on_absence_or_type_mismatch() {
    assert_eq!(
        expressions::eval(r#"get value({a: "foo"}, "a")"#, &empty()).unwrap(),
        FeelValue::from("foo")
    );
    assert_eq!(
        expressions::eval(r#"get value({a: "foo"}, 123)"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"get value("foo", "foo")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"get value({a: null}, "a")"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn get_value_binds_named_arguments_regardless_of_order() {
    assert_eq!(
        expressions::eval(r#"get value(key:"a", m:{a: "foo"})"#, &empty()).unwrap(),
        FeelValue::from("foo")
    );
    assert_eq!(
        expressions::eval(r#"get value(k:"a", m:{a: "foo"})"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn get_entries_lists_key_value_maps_in_key_order() {
    assert_eq!(
        expressions::eval(r#"get entries({a: "foo", b: "bar"})"#, &empty()).unwrap(),
        FeelValue::List(vec![
            map(vec![
                ("key", FeelValue::from("a")),
                ("value", FeelValue::from("foo"))
            ]),
            map(vec![
                ("key", FeelValue::from("b")),
                ("value", FeelValue::from("bar"))
            ]),
        ])
    );
    assert_eq!(
        expressions::eval("get entries({})", &empty()).unwrap(),
        FeelValue::List(vec![])
    );
}

// ----- Cycle 3: 0062 mode() -----

#[test]
fn mode_returns_all_tied_values_sorted_ascending_and_empty_list_for_empty_input() {
    assert_eq!(
        expressions::eval("mode([6, 3, 9, 6, 6])", &empty()).unwrap(),
        FeelValue::List(vec![FeelValue::num("6")])
    );
    assert_eq!(
        expressions::eval("mode([3, 6, 1, 9, 6, 1, 3])", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("3"),
            FeelValue::num("6")
        ])
    );
    assert_eq!(
        expressions::eval("mode([])", &empty()).unwrap(),
        FeelValue::List(vec![])
    );
}

// ----- Cycle 3: 1145/1146/1147 context() / context put() / context merge() -----

#[test]
fn context_builds_a_map_from_key_value_entries_or_a_single_entry() {
    assert_eq!(
        expressions::eval(
            r#"context([{key:"a", value:1}, {key:"b", value:2}])"#,
            &empty()
        )
        .unwrap(),
        map(vec![("a", FeelValue::num("1")), ("b", FeelValue::num("2"))])
    );
    assert_eq!(
        expressions::eval(r#"context({key:"a", value:1})"#, &empty()).unwrap(),
        map(vec![("a", FeelValue::num("1"))])
    );
}

#[test]
fn context_rejects_duplicate_keys_and_malformed_entries() {
    assert_eq!(
        expressions::eval(
            r#"context([{key:"a", value:1},{key:"a", value:2}])"#,
            &empty()
        )
        .unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"context({value:1})"#, &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn context_put_sets_a_top_level_entry_positionally_and_by_name() {
    assert_eq!(
        expressions::eval(r#"context put({}, "a", 1)"#, &empty()).unwrap(),
        map(vec![("a", FeelValue::num("1"))])
    );
    assert_eq!(
        expressions::eval(r#"context put(context: {}, key: "a", value: 1)"#, &empty()).unwrap(),
        map(vec![("a", FeelValue::num("1"))])
    );
}

#[test]
fn context_put_nested_path_recurses_and_rejects_a_list_bound_via_the_singular_key_name() {
    assert_eq!(
        expressions::eval(r#"context put({x:1, y: {a: 0} }, ["y", "a"], 2)"#, &empty()).unwrap(),
        map(vec![
            ("x", FeelValue::num("1")),
            ("y", map(vec![("a", FeelValue::num("2"))]))
        ])
    );
    assert_eq!(
        expressions::eval(
            r#"context put(context: {x:1, y: {a: 0} }, key: ["y", "a"], value: 2)"#,
            &empty()
        )
        .unwrap(),
        FeelValue::Null
    );
}

#[test]
fn context_merge_overwrites_left_to_right_without_deep_merging() {
    assert_eq!(
        expressions::eval(r#"context merge([{"a": 1}, {"a": 2}])"#, &empty()).unwrap(),
        map(vec![("a", FeelValue::num("2"))])
    );
    assert_eq!(
        expressions::eval(
            r#"context merge([{"a": {"aa": 1}}, {"a": {"bb": 2}}])"#,
            &empty()
        )
        .unwrap(),
        map(vec![("a", map(vec![("bb", FeelValue::num("2"))]))])
    );
}

// ----- Cycle 6: 1155-list-replace-function -----

#[test]
fn list_replace_position_form_1_based_and_negative_from_end() {
    assert_eq!(
        expressions::eval("list replace([1,2,3], 2, 4)", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("4"),
            FeelValue::num("3")
        ])
    );
    assert_eq!(
        expressions::eval("list replace([1,2,3], -1, 4)", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("4")
        ])
    );
    // Zero, and any out-of-bounds position (positive or negative), is invalid — null, not a
    // clamp or a silent no-op.
    assert_eq!(
        expressions::eval("list replace([1,2,3], 0, 4)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("list replace([1,2,3], 4, 4)", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("list replace([1,2,3], -4, 4)", &empty()).unwrap(),
        FeelValue::Null
    );
    // A bare (non-list) `list` argument coerces to a singleton list first.
    assert_eq!(
        expressions::eval("list replace(1, 1, 5)", &empty()).unwrap(),
        FeelValue::List(vec![FeelValue::num("5")])
    );
}

#[test]
fn list_replace_match_function_form_replaces_every_satisfying_element() {
    assert_eq!(
        expressions::eval(
            "list replace([2, 4, 7, 8], function(item, newItem) item < newItem, 5)",
            &empty()
        )
        .unwrap(),
        FeelValue::List(vec![
            FeelValue::num("5"),
            FeelValue::num("5"),
            FeelValue::num("7"),
            FeelValue::num("8")
        ])
    );
    // Named arguments, either form, order-independent.
    assert_eq!(
        expressions::eval(
            "list replace(position: 2, newItem: 4, list: [1,2,3])",
            &empty()
        )
        .unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("4"),
            FeelValue::num("3")
        ])
    );
}

#[test]
fn list_replace_never_partially_invokes_on_a_bad_call() {
    // Too few / too many arguments.
    assert_eq!(
        expressions::eval(r#"list replace([1,2,3], "2")"#, &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval(r#"list replace([1,2,3], "2", 4, 4)"#, &empty()).unwrap(),
        FeelValue::Null
    );
    // An unrecognized named argument.
    assert_eq!(
        expressions::eval(
            "list replace(position: 2, newItem: 4, list: [1,2,3], foo: 1)",
            &empty()
        )
        .unwrap(),
        FeelValue::Null
    );
    // A match function whose OWN declared arity isn't exactly 2.
    assert_eq!(
        expressions::eval(
            "list replace([2, 4], function(item, newItem, extraParam) item = 2, 5)",
            &empty()
        )
        .unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("list replace([2, 4], function(item) item = 2, 5)", &empty()).unwrap(),
        FeelValue::Null
    );
    // A match function returning a non-boolean invalidates the whole call.
    assert_eq!(
        expressions::eval(
            "list replace([2, 4], function(item, newItem) item, 5)",
            &empty()
        )
        .unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 3: 0012/0013 list builtins (remove / union / sort) -----

#[test]
fn remove_deletes_the_element_at_a_1_indexed_position() {
    assert_eq!(
        expressions::eval(r#"remove(["a","b","c"], 2)"#, &empty()).unwrap(),
        FeelValue::List(vec![FeelValue::from("a"), FeelValue::from("c")])
    );
}

#[test]
fn union_deduplicates_elements_across_lists() {
    assert_eq!(
        expressions::eval("union([1,2],[2,3])", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("1"),
            FeelValue::num("2"),
            FeelValue::num("3")
        ])
    );
}

#[test]
fn sort_orders_a_list_by_a_comparator_function() {
    assert_eq!(
        expressions::eval("sort([3,1,2], function(x,y) x>y)", &empty()).unwrap(),
        FeelValue::List(vec![
            FeelValue::num("3"),
            FeelValue::num("2"),
            FeelValue::num("1")
        ])
    );
}

// ----- Cycle 3: 1101/1102 floor()/ceiling() 2-arg scale form -----

#[test]
fn floor_and_ceiling_round_to_a_given_decimal_scale() {
    assert_num(
        expressions::eval("floor(1.56, 1)", &empty()).unwrap(),
        "1.5",
    );
    assert_num(
        expressions::eval("floor(-1.56, 1)", &empty()).unwrap(),
        "-1.6",
    );
    assert_num(
        expressions::eval("ceiling(1.56, 1)", &empty()).unwrap(),
        "1.6",
    );
    assert_eq!(
        expressions::eval("floor(1.56, null)", &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 3: 0052 exp() scale-8 rounding -----

#[test]
fn exp_rounds_to_a_fixed_scale_of_eight() {
    assert_num(
        expressions::eval("exp(4)", &empty()).unwrap(),
        "54.59815003",
    );
}

// ----- Cycle 3: 0059/0060 all()/any() zero-argument call -----

#[test]
fn all_and_any_reject_a_truly_zero_argument_call_but_not_an_empty_list() {
    assert_eq!(
        expressions::eval("all()", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("any()", &empty()).unwrap(),
        FeelValue::Null
    );
    assert_eq!(
        expressions::eval("all([])", &empty()).unwrap(),
        FeelValue::Boolean(true)
    );
    assert_eq!(
        expressions::eval("any([])", &empty()).unwrap(),
        FeelValue::Boolean(false)
    );
}

// ----- Cycle 7: scope-gated hyphenated-name split (DMN-TCK 0035/0031) -----

#[test]
fn hyphenated_ident_splits_into_subtraction_when_no_such_name_is_in_scope() {
    let c = ctx(vec![
        ("Rn", FeelValue::num("0.25")),
        ("Kn", FeelValue::num("0.5")),
    ]);
    assert_num(expressions::eval("1-Rn-Kn", &c).unwrap(), "0.25");
    assert_num(expressions::eval("Rn-Kn", &c).unwrap(), "-0.25");
}

#[test]
fn hyphenated_ident_stays_one_name_when_it_is_in_scope() {
    let c = ctx(vec![
        ("Pre-bureauRiskCategory", FeelValue::String("LOW".into())),
        ("Pre", FeelValue::num("1")),
        ("bureauRiskCategory", FeelValue::num("2")),
    ]);
    // Longest-name-in-scope wins over the subtraction reading even when the pieces also resolve.
    assert_eq!(
        expressions::eval("Pre-bureauRiskCategory", &c).unwrap(),
        FeelValue::String("LOW".into())
    );
}

#[test]
fn hyphenated_split_applies_inside_a_lambda_body() {
    // DMN-TCK 0031-user-defined-functions #002: `function(a,b) a-b` must subtract, not
    // dereference a phantom name `a-b`.
    let c = ctx(vec![("x", FeelValue::num("7"))]);
    assert_num(
        expressions::eval("(function(a,b) a-b)(10, x)", &c).unwrap(),
        "3",
    );
}

// ----- Cycle 7: builtins as first-class function values (DMN-TCK 0092 #014/#016) -----

#[test]
fn builtin_referenced_bare_is_a_function_value_invocable_through_a_parameter() {
    assert_num(
        expressions::eval("(function(f) f(-3))(abs)", &empty()).unwrap(),
        "3",
    );
    assert_num(
        expressions::eval("(function(f) f(25))(sqrt)", &empty()).unwrap(),
        "5",
    );
}

#[test]
fn builtin_passed_as_value_rejects_a_wrong_arity_call() {
    // sqrt takes exactly one argument — invoking the passed value with two is "never invoked".
    assert_eq!(
        expressions::eval("(function(f) f(10, 2))(sqrt)", &empty()).unwrap(),
        FeelValue::Null
    );
}

#[test]
fn nondeterministic_builtins_are_not_wrappable_as_values() {
    // `now` must not smuggle past the determinism denylist as a bare value.
    assert_eq!(expressions::eval("now", &empty()).unwrap(), FeelValue::Null);
}

// ----- Cycle 7: inline lambda parameter typeRef gating (DMN-TCK 0082 fd_002) -----

#[test]
fn inline_lambda_declared_param_type_gates_the_call() {
    assert_num(
        expressions::eval("(function(arg: number) arg)(10)", &empty()).unwrap(),
        "10",
    );
    assert_eq!(
        expressions::eval("(function(arg: number) arg)(\"foo\")", &empty()).unwrap(),
        FeelValue::Null
    );
}

// ----- Cycle 7: matches() XSD class subtraction + backreference fallback (DMN-TCK 1111) -----

#[test]
fn matches_translates_xsd_character_class_subtraction() {
    // O and I are subtracted from A-Z — no match, even caselessly.
    assert_eq!(
        expressions::eval("matches(\"O\", \"[A-Z-[OI]]\", \"i\")", &empty()).unwrap(),
        FeelValue::Boolean(false)
    );
    assert_eq!(
        expressions::eval("matches(\"i\", \"[A-Z-[OI]]\", \"i\")", &empty()).unwrap(),
        FeelValue::Boolean(false)
    );
    assert_eq!(
        expressions::eval("matches(\"A\", \"[A-Z-[OI]]\", \"i\")", &empty()).unwrap(),
        FeelValue::Boolean(true)
    );
}

#[test]
fn matches_supports_backreferences_via_the_backtracking_fallback() {
    assert_eq!(
        expressions::eval(r#"matches("aA", "(a)\1", "i")"#, &empty()).unwrap(),
        FeelValue::Boolean(true)
    );
    assert_eq!(
        expressions::eval(r#"matches("ab", "(a)\1")"#, &empty()).unwrap(),
        FeelValue::Boolean(false)
    );
}

#[test]
fn matches_still_rejects_malformed_backreference_patterns() {
    // A reference to a nonexistent group, or a backreference inside a character class, stays an
    // error under the fallback engine too (DMN-TCK K2-MatchesFunc-8..14).
    for expr in [
        r#"matches("h", "(.)\2")"#,
        r#"matches("input", "\3")"#,
        r#"matches("abcd", "(asd)[\1]")"#,
    ] {
        assert!(
            expressions::eval(expr, &empty()).is_err(),
            "expression: {expr}"
        );
    }
}
