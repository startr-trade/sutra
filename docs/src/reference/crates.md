# Crates: `sutra-feel` and `sutra-dmn`

Every crate in the workspace is unpublished by default (`publish = false` at the workspace level)
except two, which are deliberately carved out as standalone, embeddable libraries:
[`sutra-feel`](https://crates.io/crates/sutra-feel) and
[`sutra-dmn`](https://crates.io/crates/sutra-dmn). Both are pure Rust, `#![forbid(unsafe_code)]`,
and have no dependency on BPMN, channels, or persistence — you can use either in a project that has
nothing to do with workflow engines at all.

```toml
[dependencies]
sutra-feel = "0.2.0-rc.1"
sutra-dmn  = "0.2.0-rc.1"   # depends on sutra-feel directly; use it alone if you only need DMN
```

## `sutra-feel` — the FEEL expression language

FEEL (Friendly Enough Expression Language) is the expression language the OMG DMN specification
defines, reused throughout the BPMN/DMN tooling ecosystem wherever a small, side-effect-free
expression needs to be embedded in a larger model — a decision rule, a gateway condition, a
data-mapping assignment. `sutra-feel` is a complete, standalone implementation: lexer, parser,
AST, and evaluator, with no dependency on DMN or BPMN machinery.

```rust
use sutra_feel::{expressions, FeelContext, FeelValue};

let mut context = FeelContext::new();
context.insert("age".to_string(), FeelValue::Number("42".parse().unwrap()));

let result = expressions::eval("age >= 18", &context).unwrap();
assert_eq!(result, FeelValue::Boolean(true));
```

`expressions::eval` is the main entry point; `expressions::parse` parses without evaluating, and
`expressions::eval_boolean` is a convenience wrapper for expressions that must produce a boolean
(gateway-condition shaped callers). `expressions::paths` extracts which context paths an
expression reads without evaluating it — useful for static analysis over a set of expressions.

Highlights:

- Source-position-aware diagnostics (errors carry line/column, pinned to a caller-supplied source
  URI) — the errors this crate raises are the same ones you'd see from `sutra explain` (see the
  [CLI reference](cli.md)) or from a deploy-time validation failure.
- DECIMAL64 numeric semantics (16 significant digits, `HALF_EVEN` rounding, via `bigdecimal`) —
  arithmetic matches the FEEL specification, not native floating point.
- Full temporal support: dates, times, date-and-time, and durations, including IANA-timezone-aware
  `@Region/City` zone-qualified literals, with the timezone database bundled at compile time (no
  host `tzdata` dependency).
- Ranges/intervals, contexts (maps), lists, and function values, plus a documented determinism
  denylist for expressions that must stay side-effect-free (the engine uses this to keep replay
  deterministic across a wait-state resume — see
  [Wait states and human tasks](../building/wait-states.md)).

## `sutra-dmn` — the DMN 1.5 decision engine

DMN (Decision Model and Notation) is the OMG standard for decision tables, decision requirement
graphs (DRGs), and business knowledge models, tied together by FEEL. `sutra-dmn` loads `.dmn` XML
files, validates them, and evaluates them — single decision tables (all seven OMG hit policies:
UNIQUE, ANY, PRIORITY, FIRST, OUTPUT ORDER, RULE ORDER, and COLLECT with SUM/COUNT/MIN/MAX
aggregation) as well as full DRGs, where a decision depends on other decisions, invokes business
knowledge models, or calls decision services. FEEL parsing and evaluation is delegated directly to
`sutra-feel`.

```rust
use sutra_dmn::DmnDecisionEngine;
use sutra_feel::{FeelContext, FeelValue};

let engine = DmnDecisionEngine::new();
let result = engine.evaluate("tier.dmn", TIER_DMN_XML.as_bytes(), &input).unwrap();
assert_eq!(result.get("tier"), Some(&FeelValue::from("GOLD")));
```

`DmnDecisionEngine::evaluate` is the single-file entry point, returning a map of output-clause
name to `FeelValue` — this is exactly what backs a `businessRuleTask` in the engine (see
[Rules: DMN, FEEL, and .srl](../building/rules.md)). For DRG evaluation across a graph of
decisions/BKMs/decision services with imports, see the crate's `drg` module; for structural
validation without evaluation, see `DmnRulesetValidator`.

**Conformance is measured, not asserted** — see [DMN-TCK conformance](dmn-tck.md) for the current
standing and what it does and doesn't cover.

## Why these two and not the rest of the workspace

Everything else in the workspace — the BPMN model and executor, the channel/transport layer, the
persistence layer, the CLI — is Sutra-engine-internal: coupled to the engine's own conventions
(diagnostics, deployment packages, the `q:` namespace) in a way that wouldn't make sense as a
general-purpose crate. FEEL and DMN evaluation, by contrast, are useful on their own to anyone
building rules or decision logic in Rust, independent of whether BPMN or Sutra is anywhere in the
picture — so those two are published, versioned, and supported as standalone libraries.

## Next

- **[DMN-TCK conformance](dmn-tck.md)** — the numbers behind "measured, not asserted."
- **[Rules: DMN, FEEL, and .srl](../building/rules.md)** — how these two crates show up inside a
  Sutra process.
