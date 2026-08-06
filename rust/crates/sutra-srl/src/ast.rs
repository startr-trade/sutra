//! The `.srl` abstract syntax tree.
//!
//! Every embedded FEEL expression is stored already compiled (a [`FeelExpr`]) together with the
//! **raw `.srl` character span** it was lifted from. The span's `.0` (start offset) is the
//! *origin* used to compose FEEL diagnostics back onto `.srl` line/column: a FEEL error carries a
//! character offset relative to its own sub-expression, so `origin + feel_offset` is the absolute
//! `.srl` offset. Offsets are **character** offsets (0-based, end-exclusive), matching FEEL's own
//! AST convention.

use sutra_feel::FeelExpr;

/// A parsed ruleset — a sequence of rules in declaration order.
#[derive(Debug, Clone, PartialEq)]
pub struct Ruleset {
    pub rules: Vec<Rule>,
}

/// One `rule "name" … when … then … end` unit.
#[derive(Debug, Clone, PartialEq)]
pub struct Rule {
    /// The rule name (the unescaped string-literal value after `rule`).
    pub name: String,
    /// `salience` attribute — higher fires first; defaults to `0`.
    pub salience: i64,
    /// `activation-group` attribute — at most one rule per group fires (first-match-wins).
    pub activation_group: Option<String>,
    /// The compiled `when` condition (a boolean FEEL expression).
    pub condition: FeelExpr,
    /// Raw `(start, end)` character span of the condition text in the `.srl` source. `start` is
    /// the origin for composing FEEL condition diagnostics.
    pub condition_span: (usize, usize),
    /// The `then` block actions, in declaration order.
    pub actions: Vec<Action>,
    /// 0-based declaration index — the stable tiebreaker for equal-salience agenda ordering.
    pub decl_index: usize,
}

/// A single `then`-block action. The action verb set is **closed**: only `report` and
/// `set` exist (`insert` / `retract` are reserved for a stateful engine and rejected at
/// parse time).
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// `report(code, path, message);` — appends a `{code, severity, path, message, value}` issue
    /// map to the accumulated `issues` list. `arg_spans` holds each argument's raw `(start, end)`
    /// character span (origins for composing FEEL diagnostics).
    Report {
        // Boxed: three inline `FeelExpr`s made this variant dwarf `Set` (clippy
        // large_enum_variant) once the FEEL AST grew its DMN-conformance nodes.
        code: Box<FeelExpr>,
        path: Box<FeelExpr>,
        message: Box<FeelExpr>,
        arg_spans: [(usize, usize); 3],
    },
    /// `set(target, expr);` — evaluates `expr` and binds `target` in both the working context
    /// (so later rules see it) and the output map. `expr_span` is the expression's raw
    /// `(start, end)` character span (origin for composing FEEL diagnostics).
    Set {
        target: String,
        expr: FeelExpr,
        expr_span: (usize, usize),
    },
}
