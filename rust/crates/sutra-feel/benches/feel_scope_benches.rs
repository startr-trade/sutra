//! FEEL scope benches: the cost of the constructs that bind a name.
//!
//! A filter, a quantifier, a `for`, a context literal and a user function call each bind a
//! name over the caller's scope. The context here is shaped like a real process-variable map:
//! a `payload` holding a list of records (10, 100 or 500 of them, 12 fields each) next to 20
//! other top-level variables. If any of these constructs copied the whole scope per binding,
//! its cost would grow with the size of the payload rather than with the work it does, and the
//! 500-record row would show it.
//!
//! Expressions are parsed once, outside the timed loop, so only evaluation is measured.
//!
//! Run: `cargo bench -p sutra-feel --bench feel_scope_benches`.

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sutra_feel::{evaluator, expressions, FeelContext, FeelValue};

const SIZES: &[usize] = &[10, 100, 500];

/// Record `i` of `n` in `payload.items`: 12 fields of mixed kinds. Only the last record has
/// `flag` set, so `some i in payload.items satisfies i.flag` visits every record before it
/// answers, rather than stopping at the first.
fn record(i: usize, n: usize) -> FeelValue {
    FeelValue::Map(BTreeMap::from([
        (
            "id".to_string(),
            FeelValue::from(format!("ITEM-{i:06}").as_str()),
        ),
        (
            "amount".to_string(),
            FeelValue::num(&format!("{}.25", i % 40)),
        ),
        ("flag".to_string(), FeelValue::from(i + 1 == n)),
        (
            "name".to_string(),
            FeelValue::from(format!("record number {i}").as_str()),
        ),
        (
            "category".to_string(),
            FeelValue::from(["alpha", "beta", "gamma"][i % 3]),
        ),
        (
            "quantity".to_string(),
            FeelValue::num(&(i % 13).to_string()),
        ),
        ("price".to_string(), FeelValue::num("19.99")),
        ("region".to_string(), FeelValue::from("north")),
        ("code".to_string(), FeelValue::from("C-0042-ROW-07")),
        ("active".to_string(), FeelValue::from(true)),
        (
            "note".to_string(),
            FeelValue::from("a short free-text note"),
        ),
        ("rank".to_string(), FeelValue::num(&(i % 100).to_string())),
    ]))
}

/// `payload.items` of `n` records plus 20 top-level variables.
fn scope_context(n: usize) -> FeelContext {
    let items = FeelValue::List((0..n).map(|i| record(i, n)).collect());
    let payload = FeelValue::Map(BTreeMap::from([("items".to_string(), items)]));
    let mut ctx = FeelContext::from([("payload".to_string(), payload)]);
    for v in 0..20 {
        ctx.insert(
            format!("var{v:02}"),
            FeelValue::from(format!("value of variable {v}").as_str()),
        );
    }
    ctx
}

const EXPRS: &[(&str, &str)] = &[
    ("filter_items", "payload.items[amount > 10]"),
    ("some_items", "some i in payload.items satisfies i.flag"),
    ("for_items", "for i in payload.items return i.amount"),
    ("context_literal", "{a: 1, b: a + 1}"),
    (
        "sort_lambda",
        "sort(payload.items, function(x, y) x.amount < y.amount)",
    ),
];

fn bench_scope(c: &mut Criterion) {
    for (name, text) in EXPRS {
        let expr = expressions::parse(text).expect("parses");
        let mut group = c.benchmark_group(format!("feel_scope/{name}"));
        group.sample_size(10);
        for &n in SIZES {
            let ctx = scope_context(n);
            group.bench_with_input(BenchmarkId::from_parameter(n), &ctx, |b, ctx| {
                b.iter(|| evaluator::eval(black_box(&expr), black_box(ctx)).expect("evaluates"))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_scope);
criterion_main!(benches);
