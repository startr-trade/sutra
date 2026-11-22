//! Name scoping of the constructs that bind a name: filter, quantifier, `for`, context literal
//! and function call. Each binds over the caller's scope without copying it, so these pin down
//! that lookups still resolve exactly as a copied-and-extended scope would.

use std::collections::BTreeMap;

use sutra_feel::{expressions, FeelContext, FeelValue};

fn eval(expression: &str, ctx: &FeelContext) -> FeelValue {
    expressions::eval(expression, ctx).expect("evaluates")
}

fn n(v: i64) -> FeelValue {
    FeelValue::from(v)
}

fn list(xs: &[i64]) -> FeelValue {
    FeelValue::List(xs.iter().map(|x| n(*x)).collect())
}

fn ctx(pairs: &[(&str, FeelValue)]) -> FeelContext {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn a_filter_elements_own_item_field_shadows_the_item_binding() {
    let rows = FeelValue::List(
        [1, 2, 3]
            .iter()
            .map(|i| FeelValue::Map(BTreeMap::from([("item".to_string(), n(*i))])))
            .collect(),
    );
    let c = ctx(&[("rows", rows), ("item", n(100))]);
    let out = eval("rows[item >= 2]", &c);
    let FeelValue::List(kept) = out else {
        panic!("expected a list, got {out:?}");
    };
    assert_eq!(kept.len(), 2);
}

#[test]
fn a_quantifier_variable_shadows_a_process_variable() {
    let c = ctx(&[("x", n(100)), ("xs", list(&[1, 2, 3]))]);
    assert_eq!(
        eval("every x in xs satisfies x < 10", &c),
        FeelValue::from(true)
    );
    assert_eq!(eval("x", &c), n(100));
}

#[test]
fn a_for_variable_shadows_a_process_variable() {
    let c = ctx(&[("x", n(100)), ("xs", list(&[1, 2, 3]))]);
    assert_eq!(eval("for x in xs return x * 2", &c), list(&[2, 4, 6]));
}

#[test]
fn a_filter_binding_is_not_visible_after_the_filter() {
    let rows = FeelValue::List(vec![FeelValue::Map(BTreeMap::from([(
        "field".to_string(),
        n(7),
    )]))]);
    let c = ctx(&[("rows", rows)]);
    // The element's `field` and the `item` binding exist only inside the predicate.
    assert_eq!(
        eval("{kept: rows[field = 7], after: field, it: item}.after", &c),
        FeelValue::Null
    );
    assert_eq!(
        eval("{kept: rows[field = 7], it: item}.it", &c),
        FeelValue::Null
    );
}

#[test]
fn a_context_entry_sees_earlier_entries_and_outer_variables() {
    let c = ctx(&[("outer", n(10)), ("b", n(1000))]);
    assert_eq!(eval("{a: outer + 1, b: a + 1}.b", &c), n(12));
    // A later entry is not visible to an earlier one: `a` reads the outer `b`.
    assert_eq!(eval("{a: b, b: 1}.a", &c), n(1000));
}

#[test]
fn a_lambda_reads_an_outer_variable_at_call_time() {
    let c = ctx(&[("offset", n(5)), ("xs", list(&[1, 2]))]);
    assert_eq!(
        eval(
            "{f: function(v) v + offset, r: for x in xs return f(x)}.r",
            &c
        ),
        list(&[6, 7])
    );
    // A lambda defined inside a construct closes over that construct's bindings too.
    assert_eq!(
        eval("for k in [10, 20] return {g: function(v) v + k}.g(1)", &c),
        list(&[11, 21])
    );
}

#[test]
fn a_lambda_parameter_shadows_a_captured_variable() {
    let c = ctx(&[("v", n(100))]);
    assert_eq!(eval("{f: function(v) v * 2}.f(3)", &c), n(6));
    assert_eq!(eval("{f: function(w) v + w}.f(3)", &c), n(103));
}

#[test]
fn for_partial_sees_the_results_so_far() {
    let c = FeelContext::new();
    assert_eq!(
        eval(
            "for i in 0..4 return if i = 0 then 1 else i * partial[-1]",
            &c
        ),
        list(&[1, 1, 2, 6, 24])
    );
    assert_eq!(
        eval("for i in 1..3 return count(partial)", &c),
        list(&[0, 1, 2])
    );
    // A body that never reads `partial` is unaffected by it.
    assert_eq!(eval("for i in 1..3 return i", &c), list(&[1, 2, 3]));
}

#[test]
fn a_name_bound_to_null_in_a_frame_is_not_a_builtin_fallback() {
    // Unbound, a bare builtin name is a function value; bound to null in a frame, it is null.
    let c = ctx(&[("xs", FeelValue::List(vec![FeelValue::Null]))]);
    assert_eq!(
        eval("for abs in xs return abs", &c),
        FeelValue::List(vec![FeelValue::Null])
    );
    assert_eq!(
        eval("some abs in xs satisfies abs = null", &c),
        FeelValue::from(true)
    );
    assert_eq!(eval("{abs: null, r: abs}.r", &c), FeelValue::Null);
    assert!(!eval("abs", &c).is_null());
}
