//! The single authoritative denylist of non-deterministic FEEL builtins — port of
//! `FeelDeterminism`.
//!
//! Every FEEL expression evaluated at a replay-bound site (aliases, `<q:onValidation>`
//! predicates, content-validator `when`, dispatch-case `when`, bulk-selector `when` + path
//! templates) must be deterministic. Non-deterministic builtins are denied here.
//!
//! The denylist is the audit checkpoint when FEEL is upgraded — the change that adds a new
//! FEEL version is the natural prompt to re-examine this set.

use crate::ast::FeelExpr;
use crate::codes;
use crate::error::FeelError;

/// Functions that read ambient state (wall clock, OS random) and therefore return different
/// values on re-evaluation. Frozen at the engine level — extensions cannot bypass this list.
///
/// When upgrading FEEL: any new builtin that reads ambient state must be added here.
pub const NON_DETERMINISTIC_BUILTINS: [&str; 7] = [
    "now",
    "today",
    "currentDate", // alternate spelling some FEEL impls use
    "currentTime",
    "uuid",
    "random",
    "randomNumber",
];

/// One denied builtin invocation with its source offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeniedCall {
    pub builtin: String,
    pub offset: usize,
}

/// Returns the list of denied builtin invocations found in the AST, with their offsets.
pub fn find_denied_calls(expr: &FeelExpr) -> Vec<DeniedCall> {
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// True if the expression has no denied builtin invocations.
pub fn is_pure(expr: &FeelExpr) -> bool {
    find_denied_calls(expr).is_empty()
}

/// Errors with `SUTRA.FEEL.DETERMINISM.UNSAFE_BUILTIN` for the first denied call. `site`
/// appears in the message — pass e.g. `"alias 'batch-stamp'"` so operators can locate the
/// problem.
pub fn require_pure(expr: &FeelExpr, site: &str) -> Result<(), FeelError> {
    let denied = find_denied_calls(expr);
    let Some(first) = denied.first() else {
        return Ok(());
    };
    Err(FeelError {
        code: codes::FEEL_DETERMINISM_UNSAFE_BUILTIN.to_string(),
        message: format!(
            "The FEEL expression on {site} uses `{}`, which is denied at replay-bound sites.",
            first.builtin
        ),
        offset: Some(first.offset),
        location: None,
        hint: Some(
            "Use a payload-derived value, the engine-injected `now` variable (available at \
             DMN evaluation sites, paired with secondsBetween()), or compute the value in a \
             service task that runs once on start (its value is then frozen in process \
             variables)."
                .to_string(),
        ),
    })
}

fn walk(expr: &FeelExpr, out: &mut Vec<DeniedCall>) {
    match expr {
        FeelExpr::Literal { .. } | FeelExpr::Path { .. } => {}
        FeelExpr::Compare { left, right, .. } | FeelExpr::BoolOp { left, right, .. } => {
            walk(left, out);
            walk(right, out);
        }
        FeelExpr::Not { arg, .. } => walk(arg, out),
        FeelExpr::Negate { arg, .. } => walk(arg, out),
        FeelExpr::Arith { left, right, .. } => {
            walk(left, out);
            walk(right, out);
        }
        FeelExpr::IfThenElse {
            cond,
            then,
            otherwise,
            ..
        } => {
            walk(cond, out);
            walk(then, out);
            walk(otherwise, out);
        }
        FeelExpr::Call {
            name, args, start, ..
        } => {
            if NON_DETERMINISTIC_BUILTINS.contains(&name.as_str()) {
                out.push(DeniedCall {
                    builtin: name.clone(),
                    offset: *start,
                });
            }
            for arg in args {
                walk(arg, out);
            }
        }
        FeelExpr::ListLit { items, .. } => {
            for item in items {
                walk(item, out);
            }
        }
        FeelExpr::ContextLit { entries, .. } => {
            for (_, value) in entries {
                walk(value, out);
            }
        }
        FeelExpr::Range { from, to, .. } => {
            walk(from, out);
            walk(to, out);
        }
        FeelExpr::Quantifier {
            source, condition, ..
        } => {
            walk(source, out);
            walk(condition, out);
        }
        FeelExpr::For { bindings, body, .. } => {
            for (_, source) in bindings {
                walk(source, out);
            }
            walk(body, out);
        }
        FeelExpr::Filter {
            source, predicate, ..
        } => {
            walk(source, out);
            walk(predicate, out);
        }
        FeelExpr::FieldAccess { base, .. } => walk(base, out),
        FeelExpr::InstanceOf { expr, .. } => walk(expr, out),
        FeelExpr::FunctionDef { body, .. } => walk(body, out),
        FeelExpr::In { value, test, .. } => {
            walk(value, out);
            walk(test, out);
        }
        FeelExpr::Invoke { callee, args, .. } => {
            walk(callee, out);
            for arg in args {
                walk(arg, out);
            }
        }
        FeelExpr::OpenRange { bound, .. } => walk(bound, out),
    }
}
