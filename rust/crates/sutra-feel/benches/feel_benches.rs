//! FEEL micro-benches (the GA-readiness bench harness).
//!
//! Measures the FEEL facade hot path — parse, evaluate, evaluate-as-boolean (the BPMN
//! exclusive-gateway condition path), and path extraction (the deploy-time navigation analysis)
//! — over expressions representative of the message-processing workload the engine runs.
//!
//! These are the one part of the bench matrix that is measurable without a running engine
//! container: parse/eval are pure CPU. Cold-start, peak RSS, and sustained RPS need the
//! engine image under load and are captured by the shell harness in `rust/bench/`.
//!
//! `password_verify_cost` measures one argon2id verify (report only).
//!
//! Run: `cargo bench -p sutra-feel` (results under `rust/target/criterion/`).

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sutra_feel::{expressions, FeelContext, FeelValue};

/// An order-shaped evaluation context: `payload` maps to a nested message with total /
/// shipping / region / hold flag and sender+recipient address codes — the kinds of field real
/// gateway conditions and alias expressions dereference.
fn order_context() -> FeelContext {
    fn party(code: &str) -> FeelValue {
        FeelValue::Map(BTreeMap::from([(
            "address".to_string(),
            FeelValue::Map(BTreeMap::from([(
                "code".to_string(),
                FeelValue::from(code),
            )])),
        )]))
    }

    let payload = FeelValue::Map(BTreeMap::from([
        ("total".to_string(), FeelValue::num("2500.00")),
        ("shipping".to_string(), FeelValue::num("12.50")),
        ("region".to_string(), FeelValue::from("EU")),
        ("onHold".to_string(), FeelValue::from(false)),
        ("sender".to_string(), party("SITE-0042-NORTH-ROW-07")),
        ("recipient".to_string(), party("SITE-1331-SOUTH-BAY-19")),
    ]));

    FeelContext::from([("payload".to_string(), payload)])
}

/// Expressions exercised for parse cost (parsing needs no builtins to exist).
const PARSE_EXPRS: &[(&str, &str)] = &[
    ("literal", "1000"),
    ("path", "payload.sender.address.code"),
    ("arith", "payload.total * 1.15 + payload.shipping - 0.5"),
    (
        "compound_bool",
        "payload.region = \"EU\" and payload.total >= 100.00 and payload.total < 1000000",
    ),
];

/// Expressions evaluated end-to-end (parse + eval) against the order context — arithmetic,
/// path navigation, comparison. Restricted to operators/paths that are always resolvable.
const EVAL_EXPRS: &[(&str, &str)] = &[
    ("arith_decimal", "payload.total * 1.15 + payload.shipping"),
    ("path_nav", "payload.sender.address.code"),
    ("comparison", "payload.total > 1000"),
];

/// Boolean-gate expressions — the exclusive-gateway condition path (`eval_boolean`).
const BOOL_EXPRS: &[(&str, &str)] = &[
    ("gate_simple", "payload.total > 1000"),
    (
        "gate_compound",
        "payload.region = \"EU\" and payload.total >= 100.00",
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
    let ctx = order_context();
    let mut group = c.benchmark_group("feel_eval");
    for (name, expr) in EVAL_EXPRS {
        group.bench_function(*name, |b| {
            b.iter(|| expressions::eval(black_box(expr), black_box(&ctx)).expect("evaluates"))
        });
    }
    group.finish();
}

fn bench_eval_boolean(c: &mut Criterion) {
    let ctx = order_context();
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
    let expr = "payload.sender.address.code + payload.recipient.address.code";
    let mut group = c.benchmark_group("feel_paths");
    group.bench_function("dual_path", |b| {
        b.iter(|| expressions::paths(black_box(expr)).expect("extracts"))
    });
    group.finish();
}

/// One `password verify` through the FEEL facade, against a hash made once up front. Report
/// only: the budget is 50 ms or less per verify with the fixed argon2id parameters.
fn bench_password_verify_cost(c: &mut Criterion) {
    let mut ctx = FeelContext::from([("pw".to_string(), FeelValue::from("bench-pass"))]);
    let stored = expressions::eval("password hash(pw)", &ctx).expect("hashes");
    ctx.insert("stored".to_string(), stored);
    let mut group = c.benchmark_group("password_verify_cost");
    group.sample_size(20);
    group.bench_function("verify", |b| {
        b.iter(|| {
            expressions::eval(black_box("password verify(pw, stored)"), black_box(&ctx))
                .expect("verifies")
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_eval,
    bench_eval_boolean,
    bench_paths,
    bench_password_verify_cost
);
criterion_main!(benches);
