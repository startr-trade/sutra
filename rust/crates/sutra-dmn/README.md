# sutra-dmn

A DMN 1.5 decision engine, implemented in pure Rust with no unsafe code
(`#![forbid(unsafe_code)]`).

DMN (Decision Model and Notation) is the OMG standard for modeling and executing business
decision logic — decision tables, decision requirement graphs (DRGs), business knowledge models
(BKMs), and the FEEL expression language that ties them together. This crate loads `.dmn` XML
files, validates them, and evaluates them: single decision tables (all seven OMG DMN 1.5 §8.2.10
hit policies — UNIQUE, ANY, PRIORITY, FIRST, OUTPUT ORDER, RULE ORDER, and COLLECT with SUM/
COUNT/MIN/MAX aggregation) as well as full DRGs, where a decision can depend on other decisions,
invoke BKMs, and call decision services.

FEEL expression parsing and evaluation is delegated to
[`sutra-feel`](https://crates.io/crates/sutra-feel), which this crate depends on directly; the
two are companion crates from the same project.

## Usage

```rust
use sutra_dmn::DmnDecisionEngine;
use sutra_feel::{FeelContext, FeelValue};

const TIER_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:example:tier">
  <decision id="tier">
    <decisionTable hitPolicy="FIRST">
      <input id="i1"><inputExpression typeRef="number"><text>amount</text></inputExpression></input>
      <output id="o1" name="tier" typeRef="string"/>
      <rule id="r1">
        <inputEntry><text>&gt;= 100</text></inputEntry>
        <outputEntry><text>"GOLD"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&lt; 100</text></inputEntry>
        <outputEntry><text>"STANDARD"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

fn main() {
    let mut input = FeelContext::new();
    input.insert("amount".to_string(), FeelValue::from(250_i64));

    let engine = DmnDecisionEngine::new();
    let result = engine.evaluate("tier.dmn", TIER_DMN.as_bytes(), &input).unwrap();

    assert_eq!(result.get("tier"), Some(&FeelValue::from("GOLD")));
}
```

`DmnDecisionEngine::evaluate` is the single-file, single-or-multi-decision entry point and
returns a map of output-clause name to `FeelValue`. For DRG evaluation across a graph of
decisions/BKMs/decision services with imports, see [`sutra_dmn::drg`]; for structural validation
without evaluation, see [`DmnRulesetValidator`].

## Conformance

Evaluated against the OMG [DMN Technology Compatibility Kit](https://github.com/dmn-tck/tck):
**Level 2 126/126 assertions (100%)**; **Level 3 3349/3369 assertions absolute (99.4%)** — 100%
of the assertions this engine attempts (0 FAIL among attempted assertions; the remaining gap is
unsupported constructs, not incorrect results). The TCK corpus itself is not vendored in this
crate (it is separately OMG-licensed) — the conformance harness runs against an external
checkout.

## License

Licensed under either of

- MIT license ([LICENSE-MIT](https://github.com/startr-trade/sutra/blob/main/LICENSE-MIT))
- Apache License, Version 2.0
  ([LICENSE-APACHE](https://github.com/startr-trade/sutra/blob/main/LICENSE-APACHE))

at your option.
