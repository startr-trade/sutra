//! FEEL micro-benches (the GA-readiness bench harness).
//!
//! Measures the FEEL facade hot path — parse, evaluate, evaluate-as-boolean (the BPMN
//! exclusive-gateway condition path), and path extraction (the deploy-time navigation analysis)
//! — over expressions representative of the payment-processing workload the engine runs.
//!
//! These are the one part of the bench matrix that is measurable without a running engine
//! container: parse/eval are pure CPU. Cold-start, peak RSS, and sustained RPS need the
//! engine image under load and are captured by the shell harness in `rust/bench/`.
//!
//! Run: `cargo bench -p sutra-feel` (results under `rust/target/criterion/`).

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sutra_feel::{expressions, FeelContext, FeelValue};

/// A payment-shaped evaluation context: `payload` maps to a nested message with amount /
/// fee / currency / hold flag and debtor+creditor account IBANs — the fields real gateway
/// conditions and alias expressions dereference.
fn payment_context() -> FeelContext {
    fn account(iban: &str) -> FeelValue {
        FeelValue::Map(BTreeMap::from([(
            "account".to_string(),
            FeelValue::Map(BTreeMap::from([(
                "iban".to_string(),
                FeelValue::from(iban),
            )])),
        )]))
    }

    let payload = FeelValue::Map(BTreeMap::from([
        ("amount".to_string(), FeelValue::num("2500.00")),
        ("fee".to_string(), FeelValue::num("12.50")),
        ("currency".to_string(), FeelValue::from("USD")),
        ("onHold".to_string(), FeelValue::from(false)),
        ("debtor".to_string(), account("DE89370400440532013000")),
        ("creditor".to_string(), account("GB29NWBK60161331926819")),
    ]));

    FeelContext::from([("payload".to_string(), payload)])
}

/// Expressions exercised for parse cost (parsing needs no builtins to exist).
const PARSE_EXPRS: &[(&str, &str)] = &[
    ("literal", "1000"),
    ("path", "payload.debtor.account.iban"),
    ("arith", "payload.amount * 1.15 + payload.fee - 0.5"),
    (
        "compound_bool",
        "payload.currency = \"USD\" and payload.amount >= 100.00 and payload.amount < 1000000",
    ),
];

/// Expressions evaluated end-to-end (parse + eval) against the payment context — arithmetic,
/// path navigation, comparison. Restricted to operators/paths that are always resolvable.
const EVAL_EXPRS: &[(&str, &str)] = &[
    ("arith_decimal", "payload.amount * 1.15 + payload.fee"),
    ("path_nav", "payload.debtor.account.iban"),
    ("comparison", "payload.amount > 1000"),
];

/// Boolean-gate expressions — the exclusive-gateway condition path (`eval_boolean`).
const BOOL_EXPRS: &[(&str, &str)] = &[
    ("gate_simple", "payload.amount > 1000"),
    (
        "gate_compound",
        "payload.currency = \"USD\" and payload.amount >= 100.00",
    ),
];

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("feel_parse");
    for (name, expr) in PARSE_EXPRS {
        group.bench_function(*name, |b| {
            b.iter(|| expressions::parse(black_box(expr)).expect("parses"))
        });
    }
    group.finish();
}

fn bench_eval(c: &mut Criterion) {
    let ctx = payment_context();
    let mut group = c.benchmark_group("feel_eval");
    for (name, expr) in EVAL_EXPRS {
        group.bench_function(*name, |b| {
            b.iter(|| expressions::eval(black_box(expr), black_box(&ctx)).expect("evaluates"))
        });
    }
    group.finish();
}

fn bench_eval_boolean(c: &mut Criterion) {
    let ctx = payment_context();
    let mut group = c.benchmark_group("feel_eval_boolean");
    for (name, expr) in BOOL_EXPRS {
        group.bench_function(*name, |b| {
            b.iter(|| {
                expressions::eval_boolean(black_box(expr), black_box(&ctx)).expect("evaluates")
            })
        });
    }
    group.finish();
}

fn bench_paths(c: &mut Criterion) {
    let expr = "payload.debtor.account.iban + payload.creditor.account.iban";
    let mut group = c.benchmark_group("feel_paths");
    group.bench_function("dual_path", |b| {
        b.iter(|| expressions::paths(black_box(expr)).expect("extracts"))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_eval,
    bench_eval_boolean,
    bench_paths
);
criterion_main!(benches);
