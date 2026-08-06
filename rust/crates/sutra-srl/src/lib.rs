//! # Sutra Rule Language (`.srl`)
//!
//! A small, DRL-inspired business-rules DSL that **compiles onto the existing FEEL evaluator**
//! ([`sutra_feel`]): every condition and every action argument is an embedded FEEL expression, so
//! `.srl` adds only the *rule framing* — declaration, salience, activation groups, and a closed
//! set of side-effecting actions — on top of FEEL's expression semantics. `.srl` is stateless;
//! a stateful rules engine (working-memory, `insert`/`retract`, re-activation) is not built.
//!
//! ## Grammar
//!
//! ```text
//! ruleset := rule*
//! rule    := "rule" STRING attr* "when" <condition> "then" action* "end"
//! attr    := "salience" INTEGER
//!          | "activation-group" STRING
//! action  := verb ";"
//! verb    := "report" "(" <feel_expr> "," <feel_expr> "," <feel_expr> ")"
//!          | "set"    "(" IDENT "," <feel_expr> ")"
//! ```
//!
//! - `STRING` is double-quoted with `\"` / `\\` escapes; `INTEGER` is an optional-sign decimal;
//!   `IDENT` is `[A-Za-z_][A-Za-z0-9_]*`. Line comments run `//` to end-of-line; the DSL is
//!   otherwise whitespace-insensitive. `activation-group` is a single hyphenated keyword token.
//!
//! ## The closed action-verb set
//!
//! `.srl` has exactly two verbs:
//! - `set(target, expr)` — evaluate `expr` and bind `target` in the working context (so later
//!   rules observe it) and in the output map.
//! - `report(code, path, message)` — append an issue map to the accumulated `issues` list.
//!
//! `insert` and `retract` are **reserved for a stateful engine** and are a clean parse error,
//! not silently accepted. Any other verb is a parse error.
//!
//! ## Parenthesise FEEL `if / then / else` in a condition
//!
//! FEEL itself has `if / then / else`, and the section keyword `then` that ends a `when` condition
//! is the *first `then` at paren/bracket depth 0 outside any string literal*. A FEEL
//! `if a then b else c` used **inside a condition must therefore be parenthesised**
//! (`when (if a then b else c) …`) so its inner `then` sits at depth ≥ 1 and is not mistaken for
//! the section keyword.
//!
//! ## Sequential-agenda semantics (not rete)
//!
//! [`SrlRuleEngine::evaluate`] runs a single deterministic forward pass: it builds a stable-sorted
//! agenda by `(-salience, decl_index)`, seeds a working context from the input, and fires each
//! rule at most once — evaluating the condition via `eval_boolean`, then running its actions in
//! order (`set` forward-updates the working context; `report` accumulates issues). At most one
//! rule per `activation-group` fires (first-match-wins, ≈ DMN `FIRST`). It is fail-closed: a parse
//! error, a condition that errors, or an action expression that errors is a hard [`SrlError`].
//! The result is a `BTreeMap<String, FeelValue>`: each `set` target → its value, plus an
//! `"issues"` list (present only when at least one `report` fired).
//!
//! The `issues` list uses the engine's frozen issue shape — each entry a `FeelValue::Map` with
//! keys `code` / `severity` / `path` / `message` / `value` (severity is always `"ERROR"` and
//! `value` always `Null`).
#![forbid(unsafe_code)]

pub mod ast;
pub mod codes;
pub mod engine;
pub mod error;
mod lexer;
mod parser;

pub use ast::{Action, Rule, Ruleset};
pub use engine::SrlRuleEngine;
pub use error::SrlError;
pub use parser::parse;

// Re-export the FEEL types that appear in this crate's public API, so integrators need not depend
// on `sutra_feel` directly for the common call shapes.
pub use sutra_feel::{FeelContext, FeelExpr, FeelValue};
