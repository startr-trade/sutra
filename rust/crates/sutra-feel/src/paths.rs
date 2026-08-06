//! Extracts the data paths a FEEL expression dereferences — port of `FeelPaths` (the T3
//! `navigation ⇒ schema` static-analysis input).
//!
//! Walks the AST collecting every path node with its source offsets and a coarse [`Usage`]
//! (whether the path is used in a numeric position), so a navigation validator can resolve
//! each path against the intake message schema and flag a numeric operator applied to a
//! declared-string field. Path roots are not interpreted here.

use crate::ast::{CompareOp, FeelExpr};

/// How a path is used, so the validator can apply type-compatibility checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    /// Equality, boolean/truthiness, alias projection, function argument — any non-numeric
    /// position.
    General,
    /// An operand of a relational (`< <= > >=`) or arithmetic operator — must be numeric.
    Numeric,
}

/// One dereferenced path with its source offsets and usage context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathRef {
    pub segments: Vec<String>,
    pub start: usize,
    pub end: usize,
    pub usage: Usage,
}

impl PathRef {
    /// The dotted path, e.g. `"payload.amount"`.
    pub fn dotted(&self) -> String {
        self.segments.join(".")
    }

    /// The first segment, e.g. `"payload"` / `"body"` / `"header"` / a variable name.
    pub fn root(&self) -> &str {
        &self.segments[0]
    }
}

/// Every path the expression dereferences, in source order.
pub fn extract(expr: &FeelExpr) -> Vec<PathRef> {
    let mut out = Vec::new();
    walk(expr, Usage::General, &mut out);
    out
}

fn walk(expr: &FeelExpr, usage: Usage, out: &mut Vec<PathRef>) {
    match expr {
        FeelExpr::Literal { .. } => {}
        FeelExpr::Path {
            segments,
            start,
            end,
        } => out.push(PathRef {
            segments: segments.clone(),
            start: *start,
            end: *end,
            usage,
        }),
        FeelExpr::Compare {
            left, op, right, ..
        } => {
            let operand = if is_numeric_compare(*op) {
                Usage::Numeric
            } else {
                Usage::General
            };
            walk(left, operand, out);
            walk(right, operand, out);
        }
        FeelExpr::Arith { left, right, .. } => {
            walk(left, Usage::Numeric, out);
            walk(right, Usage::Numeric, out);
        }
        FeelExpr::BoolOp { left, right, .. } => {
            walk(left, Usage::General, out);
            walk(right, Usage::General, out);
        }
        FeelExpr::Not { arg, .. } => walk(arg, Usage::General, out),
        FeelExpr::Negate { arg, .. } => walk(arg, Usage::Numeric, out),
        FeelExpr::IfThenElse {
            cond,
            then,
            otherwise,
            ..
        } => {
            walk(cond, Usage::General, out);
            // then/otherwise occupy the if-expression's own value position, so they inherit
            // its usage.
            walk(then, usage, out);
            walk(otherwise, usage, out);
        }
        FeelExpr::Call { args, .. } => {
            // A builtin's argument typing is function-specific; treat as general (don't
            // over-claim numeric).
            for arg in args {
                walk(arg, Usage::General, out);
            }
        }
        FeelExpr::ListLit { items, .. } => {
            for item in items {
                walk(item, Usage::General, out);
            }
        }
        FeelExpr::ContextLit { entries, .. } => {
            for (_, value) in entries {
                walk(value, Usage::General, out);
            }
        }
        FeelExpr::Range { from, to, .. } => {
            walk(from, Usage::Numeric, out);
            walk(to, Usage::Numeric, out);
        }
        FeelExpr::Quantifier {
            source, condition, ..
        } => {
            walk(source, Usage::General, out);
            walk(condition, Usage::General, out);
        }
        FeelExpr::For { bindings, body, .. } => {
            for (_, source) in bindings {
                walk(source, Usage::General, out);
            }
            walk(body, Usage::General, out);
        }
        FeelExpr::Filter {
            source, predicate, ..
        } => {
            walk(source, Usage::General, out);
            walk(predicate, Usage::General, out);
        }
        FeelExpr::FieldAccess { base, .. } => walk(base, Usage::General, out),
        FeelExpr::InstanceOf { expr, .. } => walk(expr, Usage::General, out),
        FeelExpr::FunctionDef { body, .. } => walk(body, Usage::General, out),
        FeelExpr::In { value, test, .. } => {
            walk(value, Usage::General, out);
            walk(test, Usage::General, out);
        }
        FeelExpr::Invoke { callee, args, .. } => {
            walk(callee, Usage::General, out);
            for arg in args {
                walk(arg, Usage::General, out);
            }
        }
        FeelExpr::OpenRange { bound, .. } => walk(bound, Usage::Numeric, out),
    }
}

fn is_numeric_compare(op: CompareOp) -> bool {
    // Equality (EQ/NEQ) is type-agnostic — only ordering operators force a numeric reading.
    matches!(
        op,
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
    )
}
