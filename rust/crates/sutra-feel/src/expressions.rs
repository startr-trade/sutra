//! Public facade for the FEEL subsystem. Engine code and
//! extensions interact with FEEL only through this module.

use crate::ast::FeelExpr;
use crate::determinism;
use crate::error::FeelError;
use crate::evaluator;
use crate::lexer::FeelLexer;
use crate::parser::FeelParser;
use crate::paths::{self, PathRef};
use crate::positions::{FeelSourcePositions, INLINE_FEEL_URI};
use crate::value::{FeelContext, FeelValue};

/// Parse a FEEL expression string into an AST — errors pinned to the inline source URI.
pub fn parse(expression: &str) -> Result<FeelExpr, FeelError> {
    parse_with_uri(expression, INLINE_FEEL_URI)
}

/// Parse with a caller-supplied source URI so any `SUTRA.FEEL.*` diagnostics carry a
/// meaningful pointer (e.g. the path of the BPMN file the expression was lifted from).
pub fn parse_with_uri(expression: &str, source_uri: &str) -> Result<FeelExpr, FeelError> {
    let positions = FeelSourcePositions::new(expression, source_uri);
    let tokens = FeelLexer::new(expression, &positions).tokenize()?;
    FeelParser::new(tokens, &positions).parse()
}

/// Evaluate an expression against a context map. Any runtime error carries the offending
/// sub-expression's line + column.
pub fn eval(expression: &str, context: &FeelContext) -> Result<FeelValue, FeelError> {
    eval_with_uri(expression, context, INLINE_FEEL_URI)
}

/// Parse an expression with the "names with spaces" merge (§10.3.1.2) applied against a
/// caller-supplied known-name set — for a caller that needs this pass WITHOUT a runtime context
/// to derive the set from (`eval`/`eval_with_uri` derive `known` from the context's own keys;
/// this is for build-time parsing where no such context exists yet). `sutra-dmn`'s DRG uses this
/// to build a BKM's callable-prelude function VALUE from its own body text: a bare call to
/// ANOTHER multi-word-named BKM (`BKM II(param)`) must merge into one call-name at parse time,
/// same as it would at eval time against a runtime context that happens to bind that name — DMN-TCK
/// 0034-drg-scopes's "BKM I" → "BKM II" → "BKM III"/"BKM IV" chain (each link's body calls the
/// next by its own multi-word BKM name).
pub fn parse_with_known_names(
    expression: &str,
    known: &std::collections::HashSet<String>,
) -> Result<FeelExpr, FeelError> {
    let positions = FeelSourcePositions::new(expression, INLINE_FEEL_URI);
    let tokens = FeelLexer::new(expression, &positions).tokenize()?;
    let tokens = if crate::lexer::needs_name_resolution(&tokens) {
        crate::lexer::resolve_names(tokens, known)
    } else {
        tokens
    };
    FeelParser::new(tokens, &positions).parse()
}

/// Evaluate with a caller-supplied source URI so diagnostics pin to a real file.
pub fn eval_with_uri(
    expression: &str,
    context: &FeelContext,
    source_uri: &str,
) -> Result<FeelValue, FeelError> {
    let positions = FeelSourcePositions::new(expression, source_uri);
    let tokens = FeelLexer::new(expression, &positions).tokenize()?;
    let tokens = if crate::lexer::needs_name_resolution(&tokens) {
        crate::lexer::resolve_names(tokens, &known_names(context))
    } else {
        tokens
    };
    let expr = FeelParser::new(tokens, &positions).parse()?;
    evaluator::eval_with_positions(&expr, context, &positions)
}

/// Same as [`eval`] but with a custom-type resolver in scope — used by `sutra-dmn`'s DRG so
/// `expr instance of <ItemDefinitionName>` can resolve a DMN `<itemDefinition>` name that isn't
/// one of FEEL's own fixed base types (DMN-TCK 0070-feel-instance-of `number_013`/`string_013`/
/// `list_013`/`list_014`/`list_014_a`/`context_013`/`context_014`). `resolve_named` maps a type
/// name to its structural [`FeelTypeShape`]; return `None` for any name it doesn't recognize as
/// one of its own custom types (never called for FEEL's fixed base names, which `instance of`
/// checks directly).
pub fn eval_with_type_resolver(
    expression: &str,
    context: &FeelContext,
    resolve_named: &crate::value::TypeResolver<'_>,
) -> Result<FeelValue, FeelError> {
    let positions = FeelSourcePositions::new(expression, INLINE_FEEL_URI);
    let tokens = FeelLexer::new(expression, &positions).tokenize()?;
    let tokens = if crate::lexer::needs_name_resolution(&tokens) {
        crate::lexer::resolve_names(tokens, &known_names(context))
    } else {
        tokens
    };
    let expr = FeelParser::new(tokens, &positions).parse()?;
    evaluator::eval_with_positions_and_types(&expr, context, &positions, Some(resolve_named))
}

/// The set of names visible for FEEL "names with spaces" resolution: every context key plus,
/// recursively, the keys of any nested context/map values (so a spaced field referenced via a
/// path — `Applicant . Existing Loans` — resolves too). Cheap: contexts are small and the
/// [`crate::lexer::merge_named_tokens`] early-out skips the pass entirely when none contain a
/// space.
fn known_names(context: &FeelContext) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for (k, v) in context {
        out.insert(k.clone());
        collect_value_names(v, &mut out);
    }
    // FEEL builtins whose canonical name has spaces (`starts with`, `string length`, …) must also
    // merge into a single call-name token, independent of the caller's context. Likewise the
    // spaced temporal/range property names (`time offset`, `start included`, `end included`).
    for (spaced, _) in evaluator::SPACED_BUILTIN_ALIASES {
        out.insert((*spaced).to_string());
    }
    for name in evaluator::SPACED_PROPERTY_NAMES {
        out.insert((*name).to_string());
    }
    out
}

fn collect_value_names(v: &FeelValue, out: &mut std::collections::HashSet<String>) {
    match v {
        FeelValue::Map(m) => {
            for (k, vv) in m {
                out.insert(k.clone());
                collect_value_names(vv, out);
            }
        }
        FeelValue::List(items) => {
            for it in items {
                collect_value_names(it, out);
            }
        }
        _ => {}
    }
}

/// Evaluate as boolean (FEEL truthiness: null is false; a non-boolean non-null result is a
/// `SUTRA.FEEL.COMPILE.TYPE_MISMATCH` error).
pub fn eval_boolean(expression: &str, context: &FeelContext) -> Result<bool, FeelError> {
    let positions = FeelSourcePositions::new(expression, INLINE_FEEL_URI);
    let tokens = FeelLexer::new(expression, &positions).tokenize()?;
    let tokens = if crate::lexer::needs_name_resolution(&tokens) {
        crate::lexer::resolve_names(tokens, &known_names(context))
    } else {
        tokens
    };
    let expr = FeelParser::new(tokens, &positions).parse()?;
    let v = evaluator::eval_with_positions(&expr, context, &positions)?;
    // The type-mismatch error path re-routes through the evaluator's position-less boolean
    // check so the error shape is single-source.
    evaluator::boolean_result(v, &expr, None)
}

/// The data paths the expression dereferences (T3 `navigation ⇒ schema` analysis), each with
/// its source offsets and a [`crate::paths::Usage`].
pub fn paths(expression: &str) -> Result<Vec<PathRef>, FeelError> {
    Ok(paths::extract(&parse(expression)?))
}

/// Path extraction with a caller-supplied source URI (so diagnostics pin to a real file).
pub fn paths_with_uri(expression: &str, source_uri: &str) -> Result<Vec<PathRef>, FeelError> {
    Ok(paths::extract(&parse_with_uri(expression, source_uri)?))
}

/// Enforce determinism on an already-parsed AST: errors with
/// `SUTRA.FEEL.DETERMINISM.UNSAFE_BUILTIN` if the expression uses any non-deterministic
/// builtin. Replay-bound sites call this at build/parse time.
pub fn require_pure_expr(expr: &FeelExpr, site: &str) -> Result<(), FeelError> {
    determinism::require_pure(expr, site)
}

/// Parse-then-enforce determinism (string convenience overload).
pub fn require_pure(expression: &str, site: &str) -> Result<(), FeelError> {
    require_pure_expr(&parse(expression)?, site)
}
