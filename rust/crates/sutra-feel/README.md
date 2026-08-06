# sutra-feel

A FEEL (Friendly Enough Expression Language) expression engine: lexer, parser, AST and
evaluator, implemented in pure Rust with no unsafe code (`#![forbid(unsafe_code)]`).

FEEL is the expression language defined by the OMG DMN specification and reused by other
OMG/BPMN-family tooling wherever a small, side-effect-free expression needs to be embedded in a
larger model (a decision rule, a gateway condition, a data-mapping assignment). This crate is a
standalone implementation of that language — it has no dependency on DMN or BPMN machinery and
can be embedded in any Rust project that needs to parse and evaluate FEEL expressions against a
context.

Highlights:

- Full lexer/parser/evaluator pipeline with source-position-aware diagnostics (errors carry
  line/column, pinned to a caller-supplied source URI).
- DECIMAL64 numeric semantics (16 significant digits, `HALF_EVEN` rounding) built on `bigdecimal`,
  so arithmetic matches the FEEL specification rather than native floating point.
- Dates, times, date-and-time, and duration values, including IANA-timezone-aware
  `@Region/City` zone-qualified temporal literals (via `time-tz`, with the timezone database
  bundled at compile time — no host `tzdata` dependency).
- Ranges/intervals, contexts (maps), lists, and function values, with a documented determinism
  denylist for expressions that must be side-effect-free.
- Path extraction (`expressions::paths`) for callers that need to know which context paths an
  expression reads without evaluating it.

## Usage

```rust
use sutra_feel::{expressions, FeelContext, FeelValue};

fn main() {
    let mut context = FeelContext::new();
    context.insert("age".to_string(), FeelValue::Number("42".parse().unwrap()));

    let result = expressions::eval("age >= 18", &context).unwrap();
    assert_eq!(result, FeelValue::Boolean(true));
}
```

`expressions::eval` is the main entry point; `expressions::parse` parses without evaluating,
and `expressions::eval_boolean` is a convenience wrapper for expressions that must produce a
FEEL boolean (e.g. gateway conditions). See the crate's rustdoc for the full facade
(`sutra_feel::expressions`).

This crate powers [`sutra-dmn`](https://crates.io/crates/sutra-dmn)'s decision-table evaluator,
but has no dependency on it — it can be used on its own wherever FEEL expression evaluation is
needed.

## License

Licensed under either of

- MIT license ([LICENSE-MIT](https://github.com/startr-trade/sutra/blob/main/LICENSE-MIT))
- Apache License, Version 2.0
  ([LICENSE-APACHE](https://github.com/startr-trade/sutra/blob/main/LICENSE-APACHE))

at your option.
