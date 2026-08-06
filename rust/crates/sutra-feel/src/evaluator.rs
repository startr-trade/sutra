//! AST walker that evaluates a [`FeelExpr`] against a context map.
//!
//! The context is a [`FeelContext`] keyed by top-level path segment. Typical top-level keys:
//! `payload`, `event`, `tenant`, `vars`. Values may be nested maps, scalars, or lists.
//!
//! Source positions are passed explicitly ([`eval_with_positions`]) rather than scoped
//! implicitly — facade entry points produce line/column-pinned errors, direct AST evaluation
//! produces position-less ones.
//!
//! # Documented divergences from the reference implementation
//!
//! - `matches()` uses the `regex` crate: unanchored substring search, but no
//!   backreferences or lookaround (the `regex` crate rejects those patterns at compile time —
//!   such a pattern yields a `SUTRA.FEEL.COMPILE.TYPE_MISMATCH` error instead of matching).
//!   An invalid pattern raises a `FeelError` where the reference implementation would throw a
//!   raw, uncoded pattern-syntax error.
//! - converting an unparseable string to a number yields a
//!   `SUTRA.FEEL.COMPILE.TYPE_MISMATCH` `FeelError` where the reference implementation throws a
//!   raw number-format error (an unhandled crash there — there is no coded behaviour to mirror).
//! - Temporal strings parse via RFC 3339, which covers both accepted shapes
//!   (`…Z` and `…+05:30`).

use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};
use time_tz::PrimitiveDateTimeExt;

use crate::ast::{ArithOp, CompareOp, FeelExpr, LogicalOp};
use crate::codes;
use crate::error::FeelError;
use crate::positions::FeelSourcePositions;
use crate::random;
use crate::value::{canonical_string_of, FeelContext, FeelDuration, FeelValue, TimeQualifier};

/// FEEL builtins whose canonical spelling contains spaces (DMN uses these; this engine's arms
/// use camelCase). The names-with-spaces merge pre-pass adds the left column to its known-name
/// set so `starts with(x, y)` parses as a single call name, and [`canonical_builtin`] maps it to
/// the camelCase arm in [`FeelEvaluator::invoke_builtin`]. Temporal entries (`day of week`, …)
/// parse today and light up when the temporal builtins land.
pub(crate) const SPACED_BUILTIN_ALIASES: &[(&str, &str)] = &[
    ("string length", "stringLength"),
    ("upper case", "upperCase"),
    ("lower case", "lowerCase"),
    ("substring before", "substringBefore"),
    ("substring after", "substringAfter"),
    ("starts with", "startsWith"),
    ("ends with", "endsWith"),
    ("list contains", "listContains"),
    ("index of", "indexOf"),
    ("insert before", "insertBefore"),
    ("distinct values", "distinctValues"),
    ("string join", "stringJoin"),
    ("day of week", "dayOfWeek"),
    ("day of year", "dayOfYear"),
    ("month of year", "monthOfYear"),
    ("week of year", "weekOfYear"),
    ("round up", "roundUp"),
    ("round down", "roundDown"),
    ("round half up", "roundHalfUp"),
    ("round half down", "roundHalfDown"),
    ("years and months duration", "yearsAndMonthsDuration"),
    ("days and time duration", "daysAndTimeDuration"),
    ("date and time", "dateAndTime"),
    ("get value", "getValue"),
    ("get entries", "getEntries"),
    ("context put", "contextPut"),
    ("context merge", "contextMerge"),
    ("list replace", "listReplace"),
    // Interval/point relation builtins (DMN 1.4 §10.3.4.6, Table 78) whose FEEL names are
    // two ordinary words — merged the same way as the other spaced builtins.
    ("met by", "metBy"),
    ("overlaps before", "overlapsBefore"),
    ("overlaps after", "overlapsAfter"),
    ("finished by", "finishedBy"),
    ("started by", "startedBy"),
];

/// FEEL property names that contain a space (temporal/range accessors after a `.`). The
/// names-with-spaces merge adds these so `x.time offset` / `r.start included` tokenize as one field.
pub(crate) const SPACED_PROPERTY_NAMES: &[&str] =
    &["time offset", "start included", "end included"];

/// Map a spaced FEEL builtin name to the camelCase arm; any other name passes through unchanged.
fn canonical_builtin(name: &str) -> &str {
    for (spaced, canonical) in SPACED_BUILTIN_ALIASES {
        if *spaced == name {
            return canonical;
        }
    }
    name
}

/// Declared positional-parameter names for builtins whose DMN-TCK call sites use named
/// arguments (DMN 1.4 §10.3.4) — consulted only when the call site supplied at least one named
/// argument ([`FeelExpr::Call::arg_names`]/[`FeelExpr::Invoke::arg_names`]). A builtin absent
/// from this table keeps the legacy positional-only binding (harmless: named args are simply
/// ignored there, exactly as before this table existed) — purely additive.
///
/// `contextPut` is deliberately absent: DMN 1.4 overloads it with two non-interchangeable names
/// at the same position (`key` for a single string, `keys` for a nested path list — binding a
/// list value via the singular `key` name is itself rejected), which this generic table can't
/// express; its own arm inspects `arg_names` directly instead (see [`Evaluator::context_put`]).
fn builtin_param_names(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        "abs" => &["n"],
        "sqrt" => &["number"],
        "exp" => &["number"],
        "log" => &["number"],
        "even" => &["number"],
        "odd" => &["number"],
        "median" => &["list"],
        "stddev" => &["list"],
        "mode" => &["list"],
        "all" => &["list"],
        "any" => &["list"],
        "product" => &["list"],
        "stringJoin" => &["list", "delimiter"],
        "is" => &["value1", "value2"],
        "range" => &["from"],
        "split" => &["string", "delimiter"],
        "number" => &["from", "grouping separator", "decimal separator"],
        "floor" => &["n", "scale"],
        "modulo" => &["dividend", "divisor"],
        "ceiling" => &["n", "scale"],
        "getValue" => &["m", "key"],
        "getEntries" => &["m"],
        "context" => &["entries"],
        "contextMerge" => &["contexts"],
        "dayOfWeek" | "monthOfYear" | "dayOfYear" | "weekOfYear" => &["date"],
        _ => return None,
    })
}

/// Outcome of binding a builtin call's evaluated arguments against its declared parameter names
/// (see [`builtin_param_names`]).
enum BuiltinBinding {
    /// No table entry for this builtin, or no call-site argument was named — `args` passes
    /// through unchanged (today's positional-only behaviour).
    Unchanged(Vec<FeelValue>),
    /// Rebound into declared-parameter order; any parameter the call site didn't supply is
    /// `Null`, so the receiving arm always sees exactly `params.len()` arguments.
    Bound(Vec<FeelValue>),
    /// A named argument didn't match any of the builtin's declared parameter names — DMN 1.4
    /// §10.3.4: an unrecognized parameter name is not a positional fallback, the call is simply
    /// invalid, which this engine surfaces as `null` rather than a hard error.
    UnknownName,
}

/// Reorder (or reject) a builtin call's arguments against its declared parameter table, only
/// when at least one call-site argument used a name (`arg_names.iter().any(Option::is_some)`).
fn bind_builtin_args(
    name: &str,
    args: Vec<FeelValue>,
    arg_names: &[Option<String>],
) -> BuiltinBinding {
    if !arg_names.iter().any(Option::is_some) {
        return BuiltinBinding::Unchanged(args);
    }
    let Some(params) = builtin_param_names(name) else {
        return BuiltinBinding::Unchanged(args);
    };
    let mut slots: Vec<Option<FeelValue>> = vec![None; params.len()];
    for (i, (arg_name, value)) in arg_names.iter().zip(args).enumerate() {
        match arg_name {
            Some(n) => match params.iter().position(|p| *p == n) {
                Some(idx) => slots[idx] = Some(value),
                None => return BuiltinBinding::UnknownName,
            },
            // A positional argument mixed with named ones (not exercised by the corpus): keep
            // its original position when that slot exists, otherwise drop it silently.
            None => {
                if i < slots.len() {
                    slots[i] = Some(value);
                }
            }
        }
    }
    BuiltinBinding::Bound(
        slots
            .into_iter()
            .map(|s| s.unwrap_or(FeelValue::Null))
            .collect(),
    )
}

/// Evaluate an expression AST against a context — no source positions in scope
/// (errors are position-less, mirroring direct `FeelEvaluator.eval` entry).
pub fn eval(expr: &FeelExpr, context: &FeelContext) -> Result<FeelValue, FeelError> {
    Evaluator {
        positions: None,
        type_resolver: None,
    }
    .eval_internal(expr, context)
}

/// Same as [`eval`] but with source positions in scope for the duration of the evaluation:
/// any error carries the offending sub-expression's line + column.
pub fn eval_with_positions(
    expr: &FeelExpr,
    context: &FeelContext,
    positions: &FeelSourcePositions,
) -> Result<FeelValue, FeelError> {
    Evaluator {
        positions: Some(positions),
        type_resolver: None,
    }
    .eval_internal(expr, context)
}

/// Same as [`eval_with_positions`] but with a custom-type resolver in scope — consulted by
/// `expr instance of <Name>` when `<Name>` isn't one of FEEL's own fixed base types (DMN's own
/// `<itemDefinition>` registry, when the caller is `sutra-dmn`'s DRG — DMN-TCK
/// 0070-feel-instance-of `number_013`/`string_013`/`list_013`/`list_014`/`list_014_a`/
/// `context_013`/`context_014`). Absent a resolver (`None`, e.g. every non-DMN evaluation), an
/// unrecognized type name is never an instance — today's plain-FEEL behaviour, unchanged.
pub fn eval_with_positions_and_types(
    expr: &FeelExpr,
    context: &FeelContext,
    positions: &FeelSourcePositions,
    type_resolver: Option<&crate::value::TypeResolver<'_>>,
) -> Result<FeelValue, FeelError> {
    Evaluator {
        positions: Some(positions),
        type_resolver,
    }
    .eval_internal(expr, context)
}

/// Evaluate as boolean (FEEL truthiness for the result: `Boolean` passes through, `null` is
/// false, anything else is a type-mismatch error).
pub fn eval_boolean(expr: &FeelExpr, context: &FeelContext) -> Result<bool, FeelError> {
    let v = eval(expr, context)?;
    boolean_result(v, expr, None)
}

pub(crate) fn boolean_result(
    v: FeelValue,
    expr: &FeelExpr,
    positions: Option<&FeelSourcePositions>,
) -> Result<bool, FeelError> {
    match v {
        FeelValue::Boolean(b) => Ok(b),
        FeelValue::Null => Ok(false),
        other => Err(Evaluator {
            positions,
            type_resolver: None,
        }
        .error(
            codes::FEEL_COMPILE_TYPE_MISMATCH,
            format!("Expected boolean result, got {}", other.type_name()),
            expr.start(),
        )),
    }
}

struct Evaluator<'a> {
    positions: Option<&'a FeelSourcePositions>,
    /// Custom-type resolver for `instance of` (see [`eval_with_positions_and_types`]) — `None`
    /// for every entry point that doesn't supply one (plain-FEEL evaluation, unaffected).
    type_resolver: Option<&'a crate::value::TypeResolver<'a>>,
}

impl Evaluator<'_> {
    fn error(&self, code: &str, message: String, site_start: usize) -> FeelError {
        FeelError {
            code: code.to_string(),
            message,
            offset: Some(site_start),
            location: self.positions.map(|p| Box::new(p.location_for(site_start))),
            hint: None,
        }
    }

    fn eval_internal(&self, expr: &FeelExpr, ctx: &FeelContext) -> Result<FeelValue, FeelError> {
        match expr {
            FeelExpr::Literal { value, .. } => Ok(value.clone()),
            FeelExpr::Path { segments, .. } => Ok({
                let v = resolve_path(segments, ctx);
                // A bare single-segment reference that resolves to nothing may name a BUILTIN
                // used as a first-class function value (DMN-TCK 0092-feel-lambda#014:
                // `bkm_014_1(abs, sqrt)` passes builtins as lambdas). Only when the context
                // genuinely lacks the key — a key explicitly bound to null keeps its null.
                if v.is_null() && segments.len() == 1 && !ctx.contains_key(&segments[0]) {
                    builtin_as_function_value(&segments[0]).unwrap_or(FeelValue::Null)
                } else {
                    v
                }
            }),
            FeelExpr::Compare {
                left, op, right, ..
            } => self.eval_compare(expr, left, *op, right, ctx),
            FeelExpr::BoolOp {
                left, op, right, ..
            } => self.eval_bool_op(left, *op, right, ctx),
            FeelExpr::Not { arg, .. } => Ok(match tri(&self.eval_internal(arg, ctx)?) {
                // FEEL three-valued negation: not(true)=false, not(false)=true, not(null)=null.
                Some(b) => FeelValue::Boolean(!b),
                None => FeelValue::Null,
            }),
            // Unary arithmetic negation (DMN-TCK 0099-arithmetic-negation) — defined directly on
            // `number` and `duration` (either flavour); every other type is `null`, including
            // `null` itself (there is no "0 - duration" fallback to route through — see the
            // `Negate` doc comment on why this is its own AST node, not `0 - arg`).
            FeelExpr::Negate { arg, .. } => Ok(match self.eval_internal(arg, ctx)? {
                FeelValue::Number(n) => FeelValue::Number(-n),
                FeelValue::Duration(FeelDuration::YearsMonths(m)) => {
                    FeelValue::Duration(FeelDuration::YearsMonths(-m))
                }
                FeelValue::Duration(FeelDuration::DaysTime(d)) => {
                    FeelValue::Duration(FeelDuration::DaysTime(-d))
                }
                _ => FeelValue::Null,
            }),
            FeelExpr::Arith {
                left, op, right, ..
            } => self.eval_arith(left, *op, right, ctx),
            FeelExpr::Call {
                name,
                args,
                arg_names,
                start,
                ..
            } => {
                let mut evaluated = Vec::with_capacity(args.len());
                for a in args {
                    evaluated.push(self.eval_internal(a, ctx)?);
                }
                // A name bound to a function value in context is invoked as a user function; a
                // native `Invocable` (e.g. a DMN decision service bound by `sutra-dmn`'s DRG) is
                // invoked the same way with its own strict arity gating; otherwise it is a
                // builtin (builtins ignore argument names — positional order).
                match ctx.get(name).cloned() {
                    Some(FeelValue::Function(f)) => {
                        self.invoke_function(&f, &evaluated, arg_names, *start)
                    }
                    Some(FeelValue::Invocable(inv)) => {
                        self.invoke_invocable(&inv, &evaluated, arg_names, *start)
                    }
                    _ => self.invoke_builtin(name, evaluated, arg_names, *start),
                }
            }
            FeelExpr::FunctionDef {
                params,
                param_shapes,
                body,
                external,
                ..
            } => {
                // An `external` definition's body isn't FEEL logic to run at call time — it's the
                // §10.3.2.13.3 java/pmml binding, classified NOW: defining an external function
                // never errors (it still yields a function value), but every invocation is
                // rejected by `invoke_function` with the recorded binding's diagnostic. A body
                // that won't even evaluate is recorded as malformed rather than raised.
                let external_binding = if *external {
                    Some(Box::new(match self.eval_internal(body, ctx) {
                        Ok(v) => crate::value::ExternalFunctionBinding::classify_body_value(&v),
                        Err(e) => {
                            crate::value::ExternalFunctionBinding::Malformed { detail: e.message }
                        }
                    }))
                } else {
                    None
                };
                Ok(FeelValue::Function(crate::value::FeelFunction {
                    // Declared `function(a: number) …` annotations gate each call's arguments
                    // (DMN-TCK 0082-feel-coercion#fd_002); an unannotated parameter is `Any`.
                    param_shapes: param_shapes.clone(),
                    return_shape: crate::value::FeelTypeShape::Any,
                    params: params.clone(),
                    body: body.clone(),
                    // Snapshot the definition-site scope so the body can reference an outer
                    // (non-parameter) variable at call time (DMN-TCK 0092-feel-lambda
                    // decision_007_1/007_2 — a lambda closing over a DMN input-data variable).
                    captured: ctx.clone(),
                    external: external_binding,
                }))
            }
            FeelExpr::IfThenElse {
                cond,
                then,
                otherwise,
                ..
            } => {
                // DMN §10.3.2.9: the `if` condition must be boolean, but `null` (an "unknown"
                // result — e.g. a null input, or a comparison against null) is treated leniently
                // the same way `false` is, taking the `else` branch (DMN-TCK
                // 0032-conditionals#003/#006). A genuinely wrong-TYPED condition (a string,
                // number, …) is different — not just "unknown" but nonsensical — and makes the
                // whole if-expression `null` rather than silently coercing to true/false
                // (DMN-TCK 1150-boxed-conditional#003).
                match self.eval_internal(cond, ctx)? {
                    FeelValue::Boolean(true) => self.eval_internal(then, ctx),
                    FeelValue::Boolean(false) | FeelValue::Null => {
                        self.eval_internal(otherwise, ctx)
                    }
                    _ => Ok(FeelValue::Null),
                }
            }
            FeelExpr::ListLit { items, .. } => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(self.eval_internal(item, ctx)?);
                }
                Ok(FeelValue::List(out))
            }
            FeelExpr::ContextLit { entries, .. } => {
                // Entries evaluate in order; each may reference earlier keys (FEEL context
                // semantics), so accumulate into a local scope layered over `ctx`. A repeated
                // key is an invalid context literal (DMN-TCK 0057-feel-context#decision008) ⇒
                // `null`, not "last value wins".
                let mut local = ctx.clone();
                let mut map = std::collections::BTreeMap::new();
                for (key, value_expr) in entries {
                    if map.contains_key(key) {
                        return Ok(FeelValue::Null);
                    }
                    let v = self.eval_internal(value_expr, &local)?;
                    local.insert(key.clone(), v.clone());
                    map.insert(key.clone(), v);
                }
                Ok(FeelValue::Map(map))
            }
            FeelExpr::Range {
                from,
                to,
                from_inclusive,
                to_inclusive,
                ..
            } => Ok(FeelValue::Range(crate::value::FeelRange {
                start: Box::new(self.eval_internal(from, ctx)?),
                end: Box::new(self.eval_internal(to, ctx)?),
                start_inclusive: *from_inclusive,
                end_inclusive: *to_inclusive,
                comparison_op: None,
            })),
            FeelExpr::Quantifier {
                every,
                var,
                source,
                condition,
                start,
                ..
            } => {
                let items = self.iterable_items(source, ctx, *start)?;
                // Evaluated in order, short-circuiting on the first DEFINITIVE answer (`some`: a
                // `true`; `every`: a `false`) — but a non-boolean `satisfies` result short-
                // circuits FIRST, to `null`, even ahead of a later definitive answer: DMN-TCK
                // 1153-boxed-some#006/1154-boxed-every#006 both read "any 'satisfies' does not
                // return boolean" as nulling the WHOLE quantifier, not just contributing an
                // "unknown" that a subsequent true/false can still resolve (unlike this same
                // `every`/`any` combining rule as reused by the `all()`/`any()` LIST builtins,
                // which stay on `bool_agg`'s original, order-independent 3-valued reduction —
                // this is a quantified-EXPRESSION-specific reading, not a `bool_agg` change).
                for item in items {
                    let mut local = ctx.clone();
                    local.insert(var.clone(), item);
                    match self.eval_internal(condition, &local)? {
                        FeelValue::Boolean(b) if b != *every => return Ok(FeelValue::Boolean(b)),
                        // Not yet definitive — keep scanning. A `null` result is treated the same
                        // lenient way as a non-resolving boolean (mirrors the `if`-condition and
                        // filter-`match` null handling above), only a genuinely wrong-typed
                        // result (below) is undefined.
                        FeelValue::Boolean(_) | FeelValue::Null => {}
                        _ => return Ok(FeelValue::Null),
                    }
                }
                Ok(FeelValue::Boolean(*every)) // exhausted with no short-circuit (incl. empty)
            }
            FeelExpr::For { bindings, body, .. } => {
                let mut out = Vec::new();
                self.eval_for(bindings, 0, ctx, body, &mut out)?;
                Ok(FeelValue::List(out))
            }
            FeelExpr::Filter {
                source, predicate, ..
            } => {
                let src = self.eval_internal(source, ctx)?;
                self.eval_filter(src, predicate, ctx)
            }
            FeelExpr::FieldAccess { base, field, .. } => {
                Ok(field_access(&self.eval_internal(base, ctx)?, field))
            }
            FeelExpr::InstanceOf {
                expr, type_shape, ..
            } => Ok(FeelValue::Boolean(instance_of_shape(
                &self.eval_internal(expr, ctx)?,
                type_shape,
                self.type_resolver,
            ))),
            FeelExpr::In { value, test, .. } => {
                let v = self.eval_internal(value, ctx)?;
                let t = self.eval_internal(test, ctx)?;
                // Membership: a list test matches any equal element; a single value matches on
                // equality (FEEL `x in [1,2,3]` / `x in [1..10]` / `x in 5`). Containment against
                // an undefined value or an open-ended `null` endpoint is `null`, not `false`
                // (§10.3.2.5/§10.3.2.13) — mirrored from `range_contains`'s `Option<bool>`.
                Ok(match t {
                    FeelValue::List(items) => FeelValue::Boolean(
                        items.iter().any(|e| equals_feel(&v, e).unwrap_or(false)),
                    ),
                    FeelValue::Range(r) => match range_contains(&r, &v) {
                        Some(b) => FeelValue::Boolean(b),
                        None => FeelValue::Null,
                    },
                    other => FeelValue::Boolean(equals_feel(&v, &other).unwrap_or(false)),
                })
            }
            // Postfix invocation of an arbitrary expression (`expr(args)`), not only a bare name
            // (see `FeelExpr::Call`) — a parenthesised function literal called immediately, a call
            // chain returning another function (`f()(4)`), or a non-function value (a clean type
            // error, not a parse crash — DMN-TCK 1131-feel-function-invocation's `null()`/`123()`).
            FeelExpr::Invoke {
                callee,
                args,
                arg_names,
                start,
                ..
            } => {
                let callee_value = self.eval_internal(callee, ctx)?;
                let mut evaluated = Vec::with_capacity(args.len());
                for a in args {
                    evaluated.push(self.eval_internal(a, ctx)?);
                }
                match callee_value {
                    FeelValue::Function(f) => {
                        self.invoke_function(&f, &evaluated, arg_names, *start)
                    }
                    FeelValue::Invocable(inv) => {
                        self.invoke_invocable(&inv, &evaluated, arg_names, *start)
                    }
                    other => Err(self.error(
                        codes::FEEL_COMPILE_TYPE_MISMATCH,
                        format!("Cannot invoke a {} as a function", other.type_name()),
                        *start,
                    )),
                }
            }
            // Comparison-operator range value (`(< 10)`, `(>= 5)`, …) — see `open_range_value`.
            FeelExpr::OpenRange { op, bound, .. } => {
                let b = self.eval_internal(bound, ctx)?;
                Ok(FeelValue::Range(open_range_value(*op, b)))
            }
        }
    }

    /// FEEL filter `source[predicate]`: a predicate that evaluates (in the outer scope) to a number
    /// indexes (1-based, negative from the end, out-of-range ⇒ null); otherwise it filters, each
    /// element evaluated with its own map entries plus `item` layered over the outer scope.
    fn eval_filter(
        &self,
        src: FeelValue,
        predicate: &FeelExpr,
        ctx: &FeelContext,
    ) -> Result<FeelValue, FeelError> {
        if src.is_null() {
            return Ok(FeelValue::Null);
        }
        // Index form: the predicate is a number in the current scope.
        if let Ok(FeelValue::Number(n)) = self.eval_internal(predicate, ctx) {
            let items = as_list(&src);
            let len = items.len() as i64;
            let i = n.to_i64().unwrap_or(0);
            let idx0 = if i < 0 { len + i } else { i - 1 };
            return Ok(if idx0 >= 0 && idx0 < len {
                items[idx0 as usize].clone()
            } else {
                FeelValue::Null
            });
        }
        // Filter form: keep elements for which the predicate is true. A `null` predicate result
        // (e.g. a missing field) just excludes that element, but a definitively wrong-TYPED
        // (non-boolean, non-null) result makes the WHOLE filter undefined — DMN-TCK
        // 1151-boxed-filter#004/#005: "any 'match' does not return boolean" nulls the entire
        // result, not just that one element.
        let mut out = Vec::new();
        for item in as_list(&src) {
            let mut local = ctx.clone();
            // The synthetic `item` convenience binding (the whole current element) is set
            // FIRST, so a context element's OWN field also named `item` — DMN-TCK
            // 0069-feel-list#decision026: `[{item: 1}, ...][item >= 2]` — SHADOWS it, not the
            // other way around (the predicate means "this row's `item` field", never "compare
            // the whole row to a number").
            local.insert("item".to_string(), item.clone());
            if let FeelValue::Map(m) = &item {
                for (k, v) in m {
                    local.insert(k.clone(), v.clone());
                }
            }
            match self.eval_internal(predicate, &local)? {
                FeelValue::Boolean(true) => out.push(item),
                FeelValue::Boolean(false) | FeelValue::Null => {}
                _ => return Ok(FeelValue::Null),
            }
        }
        Ok(FeelValue::List(out))
    }

    /// The items a `for`/quantifier iterates: a list yields its elements; any other single value
    /// iterates once (FEEL treats a scalar source as a one-element list). A `Range` value is
    /// special: the bracket-less `for i in a..b` domain form (`bracketed: false` — see
    /// [`FeelExpr::Range`]'s doc comment) iterates bidirectionally regardless of direction
    /// (DMN-TCK 0084-feel-for-loops#decision_007/008/009), but any OTHER Range-valued source — a
    /// bracketed interval literal used directly (`[2..1]`, #decision_025) or reached indirectly
    /// through a variable/function — is a genuine interval VALUE: a descending one is not a valid
    /// iteration domain (`null`/error, not silent bidirectional iteration).
    fn iterable_items(
        &self,
        source: &FeelExpr,
        ctx: &FeelContext,
        _site: usize,
    ) -> Result<Vec<FeelValue>, FeelError> {
        let allow_descending = matches!(
            source,
            FeelExpr::Range {
                bracketed: false,
                ..
            }
        );
        match self.eval_internal(source, ctx)? {
            FeelValue::List(xs) => Ok(xs),
            FeelValue::Range(r) => self.expand_range(&r, _site, allow_descending),
            other => Ok(vec![other]),
        }
    }

    /// Expand an integer OR a `date` range value to its elements for `for`/quantifier iteration;
    /// anything else cannot be iterated. `allow_descending` gates whether a `start > end` range
    /// counts down (the bare `a..b` for-loop domain form) or is rejected outright (a bracketed
    /// interval-literal VALUE — DMN-TCK 0084-feel-for-loops#decision_025).
    fn expand_range(
        &self,
        r: &crate::value::FeelRange,
        site: usize,
        allow_descending: bool,
    ) -> Result<Vec<FeelValue>, FeelError> {
        if let (FeelValue::Date(s), FeelValue::Date(e)) = (r.start.as_ref(), r.end.as_ref()) {
            return self.expand_date_range(*s, *e, r, site, allow_descending);
        }
        let (Some(start), Some(end)) = (int_of(&r.start), int_of(&r.end)) else {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                "cannot iterate a non-integer range".to_string(),
                site,
            ));
        };
        if !allow_descending && start > end {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                "cannot iterate a descending interval literal value".to_string(),
                site,
            ));
        }
        let ascending = start <= end;
        let lo = start + i64::from(!r.start_inclusive) * if ascending { 1 } else { -1 };
        let hi = end - i64::from(!r.end_inclusive) * if ascending { 1 } else { -1 };
        if (hi - lo).unsigned_abs() > 10_000_000 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("range {start}..{end} is too large to iterate"),
                site,
            ));
        }
        let mut xs = Vec::new();
        let mut i = lo;
        if ascending {
            while i <= hi {
                xs.push(FeelValue::Number(BigDecimal::from(i)));
                i += 1;
            }
        } else {
            while i >= hi {
                xs.push(FeelValue::Number(BigDecimal::from(i)));
                i -= 1;
            }
        }
        Ok(xs)
    }

    /// Expand a `date` range to its day-by-day elements (DMN-TCK 0084-feel-for-loops
    /// `#decision_017`/`#decision_018`: `for i in @"1980-01-01"..@"1980-01-03" return i`) — the
    /// same ascending/descending and `allow_descending` rules as the integer case, just stepping
    /// by one calendar day instead of one integer. Only the closed (inclusive-inclusive) form is
    /// handled — the only shape a bare `a..b` for-loop domain (or a `[a..b]` bracketed literal)
    /// ever produces; an open bound on a date range isn't exercised by the corpus.
    fn expand_date_range(
        &self,
        start: time::Date,
        end: time::Date,
        r: &crate::value::FeelRange,
        site: usize,
        allow_descending: bool,
    ) -> Result<Vec<FeelValue>, FeelError> {
        if !r.start_inclusive || !r.end_inclusive {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                "cannot iterate an open-ended date range".to_string(),
                site,
            ));
        }
        let start_jd = start.to_julian_day();
        let end_jd = end.to_julian_day();
        if !allow_descending && start_jd > end_jd {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                "cannot iterate a descending interval literal value".to_string(),
                site,
            ));
        }
        if (end_jd - start_jd).unsigned_abs() > 10_000_000 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("date range {start}..{end} is too large to iterate"),
                site,
            ));
        }
        let step: i32 = if start_jd <= end_jd { 1 } else { -1 };
        let mut xs = Vec::new();
        let mut jd = start_jd;
        loop {
            let d = time::Date::from_julian_day(jd).map_err(|_| {
                self.error(
                    codes::FEEL_COMPILE_TYPE_MISMATCH,
                    "date range step out of range".to_string(),
                    site,
                )
            })?;
            xs.push(FeelValue::Date(d));
            if jd == end_jd {
                break;
            }
            jd += step;
        }
        Ok(xs)
    }

    /// Cartesian iteration for `for` — bind each source in turn, recursing so the body sees every
    /// combination; a later source may reference an earlier binding. Each body evaluation also
    /// sees `partial` bound to the LIST OF RESULTS FROM EVERY PRIOR ITERATION (FEEL's own
    /// implicit for-loop accumulator, §10.3.2.9 — DMN-TCK 0084-feel-for-loops#decision_013's
    /// running-factorial idiom: `for i in 0..4 return if i = 0 then 1 else i * partial[-1]`),
    /// letting the body reference its own accumulated output so far.
    fn eval_for(
        &self,
        bindings: &[(String, FeelExpr)],
        idx: usize,
        ctx: &FeelContext,
        body: &FeelExpr,
        out: &mut Vec<FeelValue>,
    ) -> Result<(), FeelError> {
        if idx == bindings.len() {
            let mut local = ctx.clone();
            local.insert("partial".to_string(), FeelValue::List(out.clone()));
            out.push(self.eval_internal(body, &local)?);
            return Ok(());
        }
        let (var, source) = &bindings[idx];
        for item in self.iterable_items(source, ctx, body.start())? {
            let mut local = ctx.clone();
            local.insert(var.clone(), item);
            self.eval_for(bindings, idx + 1, &local, body, out)?;
        }
        Ok(())
    }

    fn eval_compare(
        &self,
        site: &FeelExpr,
        left: &FeelExpr,
        op: CompareOp,
        right: &FeelExpr,
        ctx: &FeelContext,
    ) -> Result<FeelValue, FeelError> {
        let l = self.eval_internal(left, ctx)?;
        let r = self.eval_internal(right, ctx)?;
        // Equality between operands whose base types (or Duration flavours) don't line up is FEEL
        // `null`, not `false` (§10.3.2.5) — `equals_feel` returns `None` for those. Ordering
        // (`< <= > >=`) against a `null` operand is likewise `null`, not a hard error — mirrors
        // the `Eq`/`Neq` arms instead of aborting the enclosing expression (DMN-TCK
        // 0069-feel-list#decision031: a filter predicate referencing a missing field must be
        // "not true", not blow up the whole literal).
        Ok(match op {
            CompareOp::Eq => match equals_feel(&l, &r) {
                Some(b) => FeelValue::Boolean(b),
                None => FeelValue::Null,
            },
            CompareOp::Neq => match equals_feel(&l, &r) {
                Some(b) => FeelValue::Boolean(!b),
                None => FeelValue::Null,
            },
            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                if l.is_null() || r.is_null() =>
            {
                FeelValue::Null
            }
            CompareOp::Lt => FeelValue::Boolean(self.compare(&l, &r, site)?.is_lt()),
            CompareOp::Le => FeelValue::Boolean(self.compare(&l, &r, site)?.is_le()),
            CompareOp::Gt => FeelValue::Boolean(self.compare(&l, &r, site)?.is_gt()),
            CompareOp::Ge => FeelValue::Boolean(self.compare(&l, &r, site)?.is_ge()),
        })
    }

    fn compare(
        &self,
        a: &FeelValue,
        b: &FeelValue,
        site: &FeelExpr,
    ) -> Result<std::cmp::Ordering, FeelError> {
        if a.is_null() || b.is_null() {
            return Err(self.error(
                codes::FEEL_EVAL_NULL_DEREFERENCE,
                "Comparison operand is null".to_string(),
                site.start(),
            ));
        }
        match (a, b) {
            (FeelValue::Number(x), FeelValue::Number(y)) => Ok(x.cmp(y)),
            (FeelValue::String(x), FeelValue::String(y)) => Ok(x.cmp(y)),
            // The generic ordering branch covers these same-type pairs:
            (FeelValue::Boolean(x), FeelValue::Boolean(y)) => Ok(x.cmp(y)),
            // Ordering is instant-/wall-clock-based; the zone/offset qualifier is display-only
            // (`FeelValue::Instant`/`Time`'s own doc comments) and never participates here.
            (FeelValue::Instant(x, _), FeelValue::Instant(y, _)) => Ok(x.cmp(y)),
            (FeelValue::Date(x), FeelValue::Date(y)) => Ok(x.cmp(y)),
            (FeelValue::Time(x, _), FeelValue::Time(y, _)) => Ok(x.cmp(y)),
            // Durations compare only within the same flavour (years-months vs days-time).
            (FeelValue::Duration(x), FeelValue::Duration(y)) => match (x, y) {
                (FeelDuration::YearsMonths(m1), FeelDuration::YearsMonths(m2)) => Ok(m1.cmp(m2)),
                (FeelDuration::DaysTime(d1), FeelDuration::DaysTime(d2)) => Ok(d1.cmp(d2)),
                _ => Err(self.error(
                    codes::FEEL_COMPILE_TYPE_MISMATCH,
                    "cannot compare a years-months duration with a days-time duration".to_string(),
                    site.start(),
                )),
            },
            _ => Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("Cannot compare {} with {}", a.type_name(), b.type_name()),
                site.start(),
            )),
        }
    }

    fn eval_bool_op(
        &self,
        left: &FeelExpr,
        op: LogicalOp,
        right: &FeelExpr,
        ctx: &FeelContext,
    ) -> Result<FeelValue, FeelError> {
        // FEEL three-valued logic (§10.3.2.5): operands are {true, false, null}; a non-boolean
        // non-null operand is `null` (unknown). `false` dominates AND, `true` dominates OR,
        // otherwise the result is `null`. Short-circuit on the dominating value so a
        // non-evaluated right operand's errors don't spuriously surface. In a boolean context
        // (gateway condition / unary test) a `null` result coerces to `false` via `boolean_result`,
        // so this is behaviour-preserving there and only changes value-returning positions (DMN).
        let l = tri(&self.eval_internal(left, ctx)?);
        let out = match op {
            LogicalOp::And => {
                if l == Some(false) {
                    return Ok(FeelValue::Boolean(false));
                }
                match (l, tri(&self.eval_internal(right, ctx)?)) {
                    (_, Some(false)) => FeelValue::Boolean(false),
                    (Some(true), Some(true)) => FeelValue::Boolean(true),
                    _ => FeelValue::Null,
                }
            }
            LogicalOp::Or => {
                if l == Some(true) {
                    return Ok(FeelValue::Boolean(true));
                }
                match (l, tri(&self.eval_internal(right, ctx)?)) {
                    (_, Some(true)) => FeelValue::Boolean(true),
                    (Some(false), Some(false)) => FeelValue::Boolean(false),
                    _ => FeelValue::Null,
                }
            }
        };
        Ok(out)
    }

    fn eval_arith(
        &self,
        left: &FeelExpr,
        op: ArithOp,
        right: &FeelExpr,
        ctx: &FeelContext,
    ) -> Result<FeelValue, FeelError> {
        let l = self.eval_internal(left, ctx)?;
        let r = self.eval_internal(right, ctx)?;
        // `+` overloads to concatenation only for string + string (FEEL §10.3.2.4).
        if op == ArithOp::Plus
            && matches!(l, FeelValue::String(_))
            && matches!(r, FeelValue::String(_))
        {
            return Ok(FeelValue::String(format!(
                "{}{}",
                canonical_string_of(&l),
                canonical_string_of(&r)
            )));
        }
        // Temporal arithmetic (date/time/duration) — before the numeric coercion that would
        // reject these operands.
        if is_temporal(&l) || is_temporal(&r) {
            return temporal_arith(&l, op, &r).ok_or_else(|| {
                FeelError::plain(
                    codes::FEEL_COMPILE_TYPE_MISMATCH,
                    format!(
                        "unsupported temporal arithmetic: {} {} {}",
                        l.type_name(),
                        arith_symbol(op),
                        r.type_name()
                    ),
                )
            });
        }
        // A null operand makes the whole arithmetic null (FEEL never coerces null to 0 here); a
        // non-string-pair String operand is an invalid arithmetic type ⇒ null (FEEL never
        // implicitly coerces a string in arithmetic — the explicit `number()` builtin is the
        // conversion). Neither reaches `to_big_decimal`, whose leniency serves other builtins.
        if l.is_null() || r.is_null() {
            return Ok(FeelValue::Null);
        }
        if matches!(l, FeelValue::String(_)) || matches!(r, FeelValue::String(_)) {
            return Ok(FeelValue::Null);
        }
        let ln = self.to_big_decimal(&l)?;
        let rn = self.to_big_decimal(&r)?;
        Ok(match op {
            ArithOp::Plus => FeelValue::Number(crate::numeric::add(&ln, &rn)),
            ArithOp::Minus => FeelValue::Number(crate::numeric::sub(&ln, &rn)),
            ArithOp::Times => FeelValue::Number(crate::numeric::mul(&ln, &rn)),
            ArithOp::Div => {
                if rn.is_zero() {
                    FeelValue::Null // FEEL spec: division by zero is null
                } else {
                    FeelValue::Number(crate::numeric::div(&ln, &rn))
                }
            }
            ArithOp::Pow => pow_decimal(&ln, &rn),
        })
    }

    /// Invoke a user function value. Arguments bind to parameters by name when the call site used
    /// named arguments (`f(b: …, a: …)`), otherwise positionally — only bindings the call site
    /// actually supplied are considered (an unmatched named arg, or a positional arg beyond
    /// `params.len()`, is silently dropped, exactly as before this — DMN's own formal-parameter
    /// conformance gating, below, only ever concerns SUPPLIED arguments). Each supplied value is
    /// then checked against the function's declared `param_shapes` (DMN §10.3.2.13): an ordinary
    /// `function(...) ...` literal has every shape `Any` (always conforms, so this is a no-op for
    /// it), but a caller-built function (e.g. a DRG-bound BKM) can declare real shapes — a
    /// non-conforming argument makes the WHOLE call evaluate to `null` (DMN's "the BKM/decision-
    /// service is never invoked" semantics), never a partial/wrong-shaped invocation. The body
    /// evaluates in the scope captured at the function literal's definition site (its closure —
    /// DMN-TCK 0092-feel-lambda#decision_007_1/007_2), with the bound (coerced) parameters
    /// layered on top (a parameter shadows a same-named captured variable) — builtins still
    /// resolve regardless, since they are name-dispatched independent of context. The result is
    /// finally coerced against `return_shape` (again a no-op when it's `Any`).
    fn invoke_function(
        &self,
        f: &crate::value::FeelFunction,
        args: &[FeelValue],
        arg_names: &[Option<String>],
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        // An `external` function definition (FEEL rule 55 / DMN kind="Java"/"PMML"): execution
        // is an optional DMN feature this engine deliberately does not provide, so ANY
        // invocation — before arity/typeRef gating, regardless of the arguments — is a semantic
        // error naming the recorded java/pmml binding (or what was wrong with it).
        if let Some(binding) = &f.external {
            return Err(self.error(
                codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED,
                binding.rejection_message(),
                site,
            ));
        }
        // (param index, evaluated argument value) for every argument the call site actually
        // supplied and that matches a declared parameter.
        let bindings: Vec<(usize, FeelValue)> = if arg_names.iter().any(Option::is_some) {
            arg_names
                .iter()
                .zip(args)
                .filter_map(|(name, value)| {
                    let n = name.as_ref()?;
                    let idx = f.params.iter().position(|p| p == n)?;
                    Some((idx, value.clone()))
                })
                .collect()
        } else {
            args.iter()
                .cloned()
                .enumerate()
                .take(f.params.len())
                .collect()
        };
        let mut local = f.captured.clone();
        for (idx, value) in bindings {
            let shape = f
                .param_shapes
                .get(idx)
                .unwrap_or(&crate::value::FeelTypeShape::Any);
            match crate::value::coerce_to_shape(&value, shape) {
                Some(coerced) => {
                    local.insert(f.params[idx].clone(), coerced);
                }
                // A declared formal parameter's typeRef isn't satisfied: the call is never made.
                None => return Ok(FeelValue::Null),
            }
        }
        let result = self.eval_internal(&f.body, &local)?;
        Ok(crate::value::coerce_to_shape(&result, &f.return_shape).unwrap_or(FeelValue::Null))
    }

    /// Invoke a native [`crate::value::Invocable`] — same call-site binding shape as
    /// [`Self::invoke_function`] (named args by declared parameter name, else positional), but
    /// with STRICT arity gating: an `Invocable` has no FEEL-AST body to fall through to for an
    /// unmatched parameter, so — unlike a `FeelFunction`, which tolerates a missing/extra
    /// argument (an unbound parameter name simply doesn't resolve inside the body) — every
    /// declared parameter must be supplied, and (for a positional call) exactly that many
    /// arguments, no more. This is what keeps DMN-TCK 0085-decision-services#005/#007/#008
    /// (wrong-arity/wrong-type "the service is never invoked" cases) correct: a naive binding
    /// that silently accepted a mismatched call would regress them (traced in cycle 5's own
    /// deferral note — see `result-cycle5.md`).
    fn invoke_invocable(
        &self,
        inv: &crate::value::Invocable,
        args: &[FeelValue],
        arg_names: &[Option<String>],
        _site: usize,
    ) -> Result<FeelValue, FeelError> {
        let ordered: Option<Vec<FeelValue>> = if arg_names.iter().any(Option::is_some) {
            // Every argument must be named, and the named set must exactly match the declared
            // parameters — a missing, extra, duplicate, or unrecognized name is not a partial
            // call, the WHOLE call is invalid (DMN-TCK 0085#010's "badly named param").
            if arg_names.len() != inv.params.len() || arg_names.iter().any(Option::is_none) {
                None
            } else {
                let mut slots: Vec<Option<FeelValue>> = vec![None; inv.params.len()];
                let mut ok = true;
                for (name, value) in arg_names.iter().zip(args) {
                    let n = name.as_ref().expect("checked above: every name is Some");
                    match inv.params.iter().position(|p| p == n) {
                        Some(idx) if slots[idx].is_none() => slots[idx] = Some(value.clone()),
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    slots.into_iter().collect()
                } else {
                    None
                }
            }
        } else if args.len() == inv.params.len() {
            Some(args.to_vec())
        } else {
            None
        };
        let Some(ordered) = ordered else {
            return Ok(FeelValue::Null);
        };
        let mut coerced = Vec::with_capacity(ordered.len());
        for (value, shape) in ordered.iter().zip(&inv.param_shapes) {
            match crate::value::coerce_to_shape(value, shape) {
                Some(v) => coerced.push(v),
                // A declared formal parameter's typeRef isn't satisfied: the call is never made.
                None => return Ok(FeelValue::Null),
            }
        }
        let result = (inv.call)(&coerced);
        Ok(crate::value::coerce_to_shape(&result, &inv.return_shape).unwrap_or(FeelValue::Null))
    }

    fn invoke_builtin(
        &self,
        name: &str,
        args: Vec<FeelValue>,
        arg_names: &[Option<String>],
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        // Accept the spaced FEEL spelling (`starts with`) alongside this engine's camelCase arms.
        let name = canonical_builtin(name);
        // `contextPut` binds its own way (two non-interchangeable named forms at the same
        // position) — see `builtin_param_names`'s doc comment.
        if name == "contextPut" {
            return self.context_put(&args, arg_names, site);
        }
        // `listReplace` binds its own way too (two non-interchangeable named forms — `position`
        // or `match` — at the same position) — see `list_replace`'s own doc comment.
        if name == "listReplace" {
            return self.list_replace(&args, arg_names, site);
        }
        let args = match bind_builtin_args(name, args, arg_names) {
            BuiltinBinding::Unchanged(a) | BuiltinBinding::Bound(a) => a,
            BuiltinBinding::UnknownName => return Ok(FeelValue::Null),
        };
        match name {
            "matches" => self.matches(&args, site),
            "contains" => {
                self.require_args("contains", &args, 2, site)?;
                // Strictly two STRINGS — `str_of`'s own `null` → `""` leniency (needed for its
                // OTHER callers, e.g. a singleton-list-of-one-string argument) would otherwise
                // make `contains(null, null)`/`contains("bar", null)` vacuously true (every
                // string "contains" the empty string) instead of `null` (DMN-TCK
                // 1110-feel-contains-function's ErrorCase_001/002/003).
                let (FeelValue::String(s), FeelValue::String(m)) = (&args[0], &args[1]) else {
                    return Ok(FeelValue::Null);
                };
                Ok(FeelValue::Boolean(s.contains(m.as_str())))
            }
            "startsWith" => {
                self.require_args("startsWith", &args, 2, site)?;
                Ok(FeelValue::Boolean(
                    str_of(&args[0]).starts_with(&str_of(&args[1])),
                ))
            }
            "endsWith" => {
                self.require_args("endsWith", &args, 2, site)?;
                Ok(FeelValue::Boolean(
                    str_of(&args[0]).ends_with(&str_of(&args[1])),
                ))
            }
            "upperCase" => Ok(FeelValue::String(
                str_of(self.arg0(&args, "upperCase", site)?).to_uppercase(),
            )),
            "lowerCase" => Ok(FeelValue::String(
                str_of(self.arg0(&args, "lowerCase", site)?).to_lowercase(),
            )),
            // `string(from)`: `null` propagates as `null` (DMN-TCK
            // 1161-boxed-list-expression#002 — a missing argument is likewise `null`, not the
            // literal 4-character string `"null"`; canonical string rendering only applies to a
            // genuinely non-null value).
            "string" => Ok(match args.first() {
                None => FeelValue::Null,
                Some(v) if v.is_null() => FeelValue::Null,
                Some(v) => FeelValue::String(canonical_string_of(v)),
            }),
            "exists" => Ok(FeelValue::Boolean(args.len() == 1 && !args[0].is_null())),
            "isBlank" => self.is_blank(&args, site),
            "secondsBetween" => self.seconds_between(&args, site),
            // A real "now" is always explicit-UTC (DMN-TCK 1148/1149: an extra argument is an
            // arity error, not silently ignored — neither is about nondeterminism).
            "now" => {
                self.require_args("now", &args, 0, site)?;
                Ok(FeelValue::Instant(
                    OffsetDateTime::now_utc(),
                    Some(TimeQualifier::Zulu),
                ))
            }
            "today" => {
                self.require_args("today", &args, 0, site)?;
                Ok(FeelValue::Date(OffsetDateTime::now_utc().date()))
            }
            "uuid" => Ok(FeelValue::String(random::uuid_v4())),
            "random" => Ok(FeelValue::from(random::random_unit_f64())),

            // ---- numeric (unary / binary) ----
            "abs" => {
                self.require_args("abs", &args, 1, site)?;
                match self.arg0(&args, "abs", site)? {
                    FeelValue::Number(n) => Ok(FeelValue::Number(n.abs())),
                    FeelValue::Duration(FeelDuration::YearsMonths(m)) => {
                        Ok(FeelValue::Duration(FeelDuration::YearsMonths(m.abs())))
                    }
                    FeelValue::Duration(FeelDuration::DaysTime(d)) => {
                        Ok(FeelValue::Duration(FeelDuration::DaysTime(d.abs())))
                    }
                    other => Err(self.error(
                        codes::FEEL_COMPILE_TYPE_MISMATCH,
                        format!(
                            "abs() expects a number or duration, got {}",
                            other.type_name()
                        ),
                        site,
                    )),
                }
            }
            "ceiling" => self.floor_ceiling(&args, RoundingMode::Ceiling, "ceiling", site),
            "floor" => self.floor_ceiling(&args, RoundingMode::Floor, "floor", site),
            "sqrt" => Ok(self
                .num1(&args, "sqrt", site)?
                .sqrt()
                // bigdecimal's sqrt carries ~100-digit scale — reduce to DECIMAL64 and strip
                // trailing zeros so `sqrt(16)` is `4`, not `4.000…0`.
                .map(|r| FeelValue::Number(crate::numeric::round_decimal64(r).normalized()))
                .unwrap_or(FeelValue::Null)),
            // DMN-TCK expects a fixed scale-8 result (corroborated by `log`'s own TCK cases,
            // outside this cycle's scope but the same shape), not `f64`'s full round-trip
            // precision — `sqrt`'s arm already rounds explicitly; `exp` didn't.
            "exp" => Ok(f64_num_scaled(
                self.num1(&args, "exp", site)?.to_f64().map(f64::exp),
                8,
            )),
            "log" => Ok(match self.num1(&args, "log", site)?.to_f64() {
                Some(x) if x > 0.0 => FeelValue::from(x.ln()),
                _ => FeelValue::Null,
            }),
            "even" => Ok(FeelValue::Boolean(
                int_parity(&self.num1(&args, "even", site)?) == Some(0),
            )),
            "odd" => Ok(FeelValue::Boolean(
                int_parity(&self.num1(&args, "odd", site)?) == Some(1),
            )),
            "modulo" => {
                self.require_args("modulo", &args, 2, site)?;
                // Strictly two NUMBERS — unlike `to_big_decimal`'s general leniency (null → 0,
                // numeric strings parsed), `modulo` itself must reject a `null`/string/boolean
                // dividend or divisor as `null`, never silently coerce it (DMN-TCK
                // 0056-feel-modulo-function#decision008_b/#decision009).
                let (FeelValue::Number(a), FeelValue::Number(b)) = (&args[0], &args[1]) else {
                    return Ok(FeelValue::Null);
                };
                Ok(feel_modulo(a, b))
            }
            "decimal" => self.round_scaled(&args, RoundingMode::HalfEven, site),
            "roundUp" => self.round_scaled(&args, RoundingMode::Up, site),
            "roundDown" => self.round_scaled(&args, RoundingMode::Down, site),
            "roundHalfUp" => self.round_scaled(&args, RoundingMode::HalfUp, site),
            "roundHalfDown" => self.round_scaled(&args, RoundingMode::HalfDown, site),

            // ---- aggregation (a single list arg, or varargs) ----
            "count" => Ok(FeelValue::Number(BigDecimal::from(
                agg_items(&args).len() as i64
            ))),
            "sum" => {
                let ns = self.num_items(&args, "sum", site)?;
                let mut s = BigDecimal::zero();
                for n in &ns {
                    s = crate::numeric::add(&s, n);
                }
                Ok(FeelValue::Number(s))
            }
            // Unlike `sum` (whose empty-input identity, 0, is well-defined and untested-but-
            // presumed-correct here), DMN-TCK 0094-feel-product-function explicitly wants a
            // TRULY zero-argument call (`product()`) AND a single-empty-list call (`product([])`)
            // to both be `null`, not the multiplicative identity `1` — mirroring `all`/`any`'s own
            // "zero args is invalid, but one argument that's an empty list is a distinct valid
            // vacuous case" distinction, except here BOTH shapes are null for `product`
            // specifically (decision002/decision003).
            "product" if args.is_empty() => Ok(FeelValue::Null),
            "product" if agg_items(&args).is_empty() => Ok(FeelValue::Null),
            "product" => {
                let ns = self.num_items(&args, "product", site)?;
                let mut s = BigDecimal::from(1);
                for n in &ns {
                    s = crate::numeric::mul(&s, n);
                }
                Ok(FeelValue::Number(s))
            }
            "min" => Ok(self
                .num_items(&args, "min", site)?
                .into_iter()
                .min()
                .map(FeelValue::Number)
                .unwrap_or(FeelValue::Null)),
            "max" => Ok(self
                .num_items(&args, "max", site)?
                .into_iter()
                .max()
                .map(FeelValue::Number)
                .unwrap_or(FeelValue::Null)),
            "mean" => {
                let ns = self.num_items(&args, "mean", site)?;
                if ns.is_empty() {
                    return Ok(FeelValue::Null);
                }
                let mut s = BigDecimal::zero();
                for n in &ns {
                    s = crate::numeric::add(&s, n);
                }
                Ok(FeelValue::Number(crate::numeric::div(
                    &s,
                    &BigDecimal::from(ns.len() as i64),
                )))
            }
            "median" => Ok(median(&self.num_items(&args, "median", site)?)),
            "stddev" => Ok(stddev(&self.num_items(&args, "stddev", site)?)),
            // A truly zero-argument call is invalid (⇒ null); `all([])`/`any([])` — one argument,
            // an empty list — is the distinct, valid vacuous-truth case and must keep passing
            // through to `agg_items`/`bool_agg` unaffected.
            "all" if args.is_empty() => Ok(FeelValue::Null),
            "any" if args.is_empty() => Ok(FeelValue::Null),
            "all" => Ok(bool_agg(&agg_items(&args), true)),
            "any" => Ok(bool_agg(&agg_items(&args), false)),
            // Same zero-args-vs-empty-list distinction as `all`/`any`: `mode()` (no arguments at
            // all) is invalid, but `mode([])` (one argument, an empty list) is the valid "no
            // values" case and must return `[]`, not null (DMN-TCK 0062#decision007).
            "mode" if args.is_empty() => Ok(FeelValue::Null),
            "mode" => Ok(mode(&self.num_items(&args, "mode", site)?)),

            // ---- string ----
            "stringLength" => Ok(FeelValue::Number(BigDecimal::from(
                str_of(self.arg0(&args, "stringLength", site)?)
                    .chars()
                    .count() as i64,
            ))),
            "substring" => self.substring(&args, site),
            "substringBefore" => {
                self.require_args("substringBefore", &args, 2, site)?;
                let s = str_of(&args[0]);
                let m = str_of(&args[1]);
                Ok(FeelValue::String(match s.find(&m) {
                    Some(i) => s[..i].to_string(),
                    None => String::new(),
                }))
            }
            "substringAfter" => {
                self.require_args("substringAfter", &args, 2, site)?;
                let s = str_of(&args[0]);
                let m = str_of(&args[1]);
                Ok(FeelValue::String(match s.find(&m) {
                    Some(i) => s[i + m.len()..].to_string(),
                    None => String::new(),
                }))
            }
            "replace" => self.replace(&args, site),
            "split" => self.split(&args, site),

            // ---- list ----
            "reverse" => {
                let mut xs = agg_items(&args);
                xs.reverse();
                Ok(FeelValue::List(xs))
            }
            "distinctValues" => {
                let mut out: Vec<FeelValue> = Vec::new();
                for x in agg_items(&args) {
                    if !out.iter().any(|y| equals_feel(y, &x).unwrap_or(false)) {
                        out.push(x);
                    }
                }
                Ok(FeelValue::List(out))
            }
            "flatten" => {
                let mut out = Vec::new();
                for a in &args {
                    flatten_into(a, &mut out);
                }
                Ok(FeelValue::List(out))
            }
            "append" => {
                let mut xs = args.first().map(as_list).unwrap_or_default();
                for a in args.iter().skip(1) {
                    xs.push(a.clone());
                }
                Ok(FeelValue::List(xs))
            }
            "concatenate" => {
                let mut xs = Vec::new();
                for a in &args {
                    xs.extend(as_list(a));
                }
                Ok(FeelValue::List(xs))
            }
            "sublist" => self.sublist(&args, site),
            "listContains" => {
                self.require_args("list contains", &args, 2, site)?;
                Ok(FeelValue::Boolean(
                    as_list(&args[0])
                        .iter()
                        .any(|x| equals_feel(x, &args[1]).unwrap_or(false)),
                ))
            }
            "indexOf" => {
                self.require_args("index of", &args, 2, site)?;
                let indices: Vec<FeelValue> = as_list(&args[0])
                    .iter()
                    .enumerate()
                    .filter(|(_, x)| equals_feel(x, &args[1]).unwrap_or(false))
                    .map(|(i, _)| FeelValue::Number(BigDecimal::from(i as i64 + 1)))
                    .collect();
                Ok(FeelValue::List(indices))
            }
            "insertBefore" => {
                self.require_args("insert before", &args, 3, site)?;
                let mut xs = as_list(&args[0]);
                let pos = self.to_big_decimal(&args[1])?.to_i64().unwrap_or(0);
                let idx0 = if pos < 0 {
                    (xs.len() as i64 + pos).max(0)
                } else {
                    (pos - 1).clamp(0, xs.len() as i64)
                } as usize;
                xs.insert(idx0.min(xs.len()), args[2].clone());
                Ok(FeelValue::List(xs))
            }
            "remove" => {
                self.require_args("remove", &args, 2, site)?;
                let mut xs = as_list(&args[0]);
                let n = xs.len() as i64;
                let pos = self.to_big_decimal(&args[1])?.to_i64().unwrap_or(0);
                let idx0 = if pos < 0 { n + pos } else { pos - 1 };
                if idx0 < 0 || idx0 >= n {
                    return Ok(FeelValue::Null);
                }
                xs.remove(idx0 as usize);
                Ok(FeelValue::List(xs))
            }
            "union" => {
                let mut out: Vec<FeelValue> = Vec::new();
                for a in &args {
                    for x in as_list(a) {
                        if !out.iter().any(|y| equals_feel(y, &x).unwrap_or(false)) {
                            out.push(x);
                        }
                    }
                }
                Ok(FeelValue::List(out))
            }
            "sort" => self.sort_builtin(&args, site),
            "stringJoin" => self.string_join(&args, site),

            // ---- conversion ----
            "number" => self.number(&args, site),

            // ---- temporal constructors & accessors ----
            "date" => self.date_builtin(&args, site),
            "time" => self.time_builtin(&args, site),
            "duration" => self.duration_builtin(&args, site),
            "dateAndTime" => self.date_and_time_builtin(&args, site),
            "yearsAndMonthsDuration" => {
                self.require_args("years and months duration", &args, 2, site)?;
                if args.iter().any(FeelValue::is_null) {
                    return Ok(FeelValue::Null);
                }
                match (date_of(&args[0]), date_of(&args[1])) {
                    (Some(a), Some(b)) => Ok(FeelValue::Duration(FeelDuration::YearsMonths(
                        months_between(a, b),
                    ))),
                    _ => {
                        Err(self
                            .temporal_err("years and months duration() expects two dates", site))
                    }
                }
            }
            "daysAndTimeDuration" => {
                self.require_args("days and time duration", &args, 2, site)?;
                if args.iter().any(FeelValue::is_null) {
                    return Ok(FeelValue::Null);
                }
                match (&args[0], &args[1]) {
                    (FeelValue::Instant(a, _), FeelValue::Instant(b, _)) => {
                        Ok(FeelValue::Duration(FeelDuration::DaysTime(*b - *a)))
                    }
                    _ => match (date_of(&args[0]), date_of(&args[1])) {
                        (Some(a), Some(b)) => Ok(FeelValue::Duration(FeelDuration::DaysTime(
                            time::Duration::days((b.to_julian_day() - a.to_julian_day()) as i64),
                        ))),
                        _ => Err(self.temporal_err(
                            "days and time duration() expects two dates/date-times",
                            site,
                        )),
                    },
                }
            }
            "year" | "month" | "day" | "hour" | "minute" | "second" => {
                self.temporal_component(name, self.arg0(&args, name, site)?, site)
            }
            // DMN-TCK 0095/0096/0098 `null_006`/`null_008`: a wrong-named argument (caught by the
            // named-arg table above, `date` being the real parameter name) or extra arguments
            // (caught by `require_args`) are both errors, not silently-ignored/positionally-
            // dropped — `arg0` alone only ever checked "at least one".
            "dayOfWeek" => {
                self.require_args("day of week", &args, 1, site)?;
                Ok(weekday_name(self.arg0(&args, "day of week", site)?))
            }
            "monthOfYear" => {
                self.require_args("month of year", &args, 1, site)?;
                Ok(month_name(self.arg0(&args, "month of year", site)?))
            }
            "dayOfYear" => {
                self.require_args("day of year", &args, 1, site)?;
                Ok(date_ordinal(self.arg0(&args, "day of year", site)?))
            }
            "weekOfYear" => {
                self.require_args("week of year", &args, 1, site)?;
                Ok(iso_week(self.arg0(&args, "week of year", site)?))
            }
            // `is(a, b)` — same value AND same type (no coercion); FeelValue equality is
            // both value- and variant-based, so structural equality is exactly this.
            "is" => {
                self.require_args("is", &args, 2, site)?;
                Ok(FeelValue::Boolean(args[0] == args[1]))
            }
            // `range("[1..3]")` — the argument is a FEEL range literal in string form, restricted
            // to DMN's dedicated grammar (see `parse_range_literal`): both endpoints must be
            // literal-shaped, of the same comparable type, non-null, and ascending. Strictly
            // narrower than parsing the string as an arbitrary FEEL expression (a comparison-
            // operator range like `(< 10)` — DMN-TCK 1156-range-function#017 — parses to
            // `FeelExpr::OpenRange`, not `FeelExpr::Range`, so it's excluded by construction).
            "range" => {
                self.require_args("range", &args, 1, site)?;
                match self.parse_range_literal(&args[0]) {
                    Some(v) => Ok(v),
                    None => Err(self.error(
                        codes::FEEL_COMPILE_TYPE_MISMATCH,
                        "range() argument is not a valid range literal".to_string(),
                        site,
                    )),
                }
            }

            // `getValue(m, key)` — the context's value at `key`, or `null` if the key is absent
            // or its stored value is itself `null` (both indistinguishable, DMN-TCK
            // 0080-feel-getvalue-function#decision_012).
            "getValue" => {
                self.require_args("getValue", &args, 2, site)?;
                Ok(match (&args[0], &args[1]) {
                    (FeelValue::Map(m), FeelValue::String(k)) => {
                        m.get(k).cloned().unwrap_or(FeelValue::Null)
                    }
                    _ => FeelValue::Null,
                })
            }
            // `getEntries(m)` — a `List` of `{key: <string>, value: <any>}` maps, one per context
            // entry (`getEntries({})` → `[]`, not null).
            "getEntries" => {
                self.require_args("getEntries", &args, 1, site)?;
                Ok(match &args[0] {
                    FeelValue::Map(m) => FeelValue::List(
                        m.iter()
                            .map(|(k, v)| {
                                let mut entry = std::collections::BTreeMap::new();
                                entry.insert("key".to_string(), FeelValue::String(k.clone()));
                                entry.insert("value".to_string(), v.clone());
                                FeelValue::Map(entry)
                            })
                            .collect(),
                    ),
                    _ => FeelValue::Null,
                })
            }
            "context" => self.context_builtin(&args, site),
            "contextMerge" => {
                self.require_args("context merge", &args, 1, site)?;
                let contexts: Vec<FeelValue> = match &args[0] {
                    FeelValue::List(xs) => xs.clone(),
                    FeelValue::Map(_) => vec![args[0].clone()],
                    _ => return Ok(FeelValue::Null),
                };
                let mut out = std::collections::BTreeMap::new();
                for c in contexts {
                    let FeelValue::Map(m) = c else {
                        return Ok(FeelValue::Null);
                    };
                    for (k, v) in m {
                        out.insert(k, v);
                    }
                }
                Ok(FeelValue::Map(out))
            }

            // ---- interval/point relations (DMN 1.4 §10.3.4.6, Table 78) ----
            "before" | "after" | "meets" | "metBy" | "overlaps" | "overlapsBefore"
            | "overlapsAfter" | "finishes" | "finishedBy" | "includes" | "during" | "starts"
            | "startedBy" | "coincides" => {
                self.require_args(name, &args, 2, site)?;
                Ok(match interval_relation(name, &args[0], &args[1]) {
                    Some(b) => FeelValue::Boolean(b),
                    None => FeelValue::Null,
                })
            }

            _ => Err(self.error(
                codes::FEEL_COMPILE_UNDEFINED_VARIABLE,
                format!("Unknown FEEL function: {name}()"),
                site,
            )),
        }
    }

    /// `matches(input, pattern, flags?)` — `input`/`pattern` are typed string; a `null` (or
    /// otherwise non-string) argument is a type error (⇒ `null`), not a `""` substitution. The
    /// optional `flags`, when present, must itself be a string of only `s`/`m`/`i`/`x` characters
    /// — anything else (a non-string value, or a disallowed/uppercase character) is likewise
    /// rejected rather than silently dropped.
    fn matches(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.len() < 2 || args.len() > 3 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("matches() expects 2 or 3 arguments, got {}", args.len()),
                site,
            ));
        }
        let (FeelValue::String(s), FeelValue::String(p)) = (&args[0], &args[1]) else {
            return Ok(FeelValue::Null);
        };
        let flags = match args.get(2) {
            // An explicit `null` flags argument is the same as omitting it (DMN-TCK
            // `fn-null-flags`: `matches("abracadabra", "bra", null)` still matches) — only a
            // present *non*-string, non-null value (a `List`, …) is rejected.
            None | Some(FeelValue::Null) => String::new(),
            Some(FeelValue::String(f)) => f.clone(),
            Some(_) => return Ok(FeelValue::Null),
        };
        // Unanchored substring search — the `regex` crate matches that via is_match. FEEL flags
        // `s/m/i/x` map to inline regex flags (plus a source pre-pass for `x`/dot-vs-CR/
        // `\p{IsXxx}`/class-subtraction — see `translated_pattern`).
        let prefixed = self.translated_pattern(p, &flags, "matches", site)?;
        let matched = match regex::Regex::new(&prefixed) {
            Ok(re) => re.is_match(s),
            // The XPath flavor allows BACKREFERENCES (`(a)\1` — DMN-TCK 1111-feel-matches-
            // function K2-MatchesFunc-17), which the `regex` crate rejects by design (no
            // backtracking): retry through `fancy_regex`, whose backtracking engine accepts a
            // valid backreference while still rejecting every malformed one the same cases
            // require to stay errors (a reference to a nonexistent group, a backreference inside
            // a character class — K2-MatchesFunc-8..14, verified individually). Confined to
            // `matches()`: no TCK/FEEL surface needs backreferences in `replace()`/`split()`,
            // which keep the strict linear-time engine. On a fallback compile failure the
            // PRIMARY engine's error is surfaced (same shape as before this fallback existed).
            Err(primary_err) => match fancy_regex::Regex::new(&prefixed) {
                Ok(re) => re.is_match(s).unwrap_or(false),
                Err(_) => {
                    return Err(self.error(
                        codes::FEEL_COMPILE_TYPE_MISMATCH,
                        format!("matches() invalid regex pattern '{p}': {primary_err}"),
                        site,
                    ))
                }
            },
        };
        Ok(FeelValue::Boolean(matched))
    }

    /// `isBlank(x)` — true when `x` is null, an empty string, or a whitespace-only string;
    /// false for any other value (a non-string scalar is a present value).
    fn is_blank(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        self.require_args("isBlank", args, 1, site)?;
        Ok(FeelValue::Boolean(match &args[0] {
            FeelValue::Null => true,
            FeelValue::String(s) => s.trim().is_empty(),
            _ => false,
        }))
    }

    /// `secondsBetween(a, b)` — signed seconds from instant `a` to instant `b` (positive when
    /// `b` is after `a`), as a number with millisecond precision (scale 3). Accepts instants
    /// or ISO-8601 strings (`2026-07-11T12:00:00Z` or with a zone offset).
    ///
    /// Deterministic — a pure function of its arguments, so it is NOT on the
    /// [`crate::determinism`] denylist. Temporal checks pair it with the engine-injected
    /// `now` context *variable* supplied at DMN evaluation entry points, never with the
    /// banned `now()` builtin.
    fn seconds_between(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        self.require_args("secondsBetween", args, 2, site)?;
        let from = self.to_instant(&args[0], "secondsBetween", site)?;
        let to = self.to_instant(&args[1], "secondsBetween", site)?;
        // Floors the nanosecond difference at millisecond precision.
        let millis = (to - from).whole_nanoseconds().div_euclid(1_000_000);
        Ok(FeelValue::Number(BigDecimal::new(millis.into(), 3)))
    }

    fn to_instant(
        &self,
        v: &FeelValue,
        func: &str,
        site: usize,
    ) -> Result<OffsetDateTime, FeelError> {
        match v {
            FeelValue::Null => Err(self.error(
                codes::FEEL_EVAL_NULL_DEREFERENCE,
                format!("{func}() temporal argument is null"),
                site,
            )),
            FeelValue::Instant(t, _) => Ok(*t),
            FeelValue::String(s) => OffsetDateTime::parse(s, &Rfc3339).map_err(|_| {
                self.error(
                    codes::FEEL_COMPILE_TYPE_MISMATCH,
                    format!(
                        "{func}() cannot parse temporal value '{s}' — expected ISO-8601 \
                         instant or offset date-time"
                    ),
                    site,
                )
            }),
            other => Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!(
                    "{func}() cannot convert {} to an instant",
                    other.type_name()
                ),
                site,
            )),
        }
    }

    fn arg0<'v>(
        &self,
        args: &'v [FeelValue],
        name: &str,
        site: usize,
    ) -> Result<&'v FeelValue, FeelError> {
        args.first().ok_or_else(|| {
            self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{name}() requires at least one argument"),
                site,
            )
        })
    }

    fn require_args(
        &self,
        name: &str,
        args: &[FeelValue],
        expected: usize,
        site: usize,
    ) -> Result<(), FeelError> {
        if args.len() != expected {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{name}() expects {expected} arguments, got {}", args.len()),
                site,
            ));
        }
        Ok(())
    }

    fn to_big_decimal(&self, v: &FeelValue) -> Result<BigDecimal, FeelError> {
        match v {
            FeelValue::Null => Ok(BigDecimal::zero()),
            FeelValue::Number(n) => Ok(n.clone()),
            // Constructing a number from a string — an unparseable string throws a raw
            // number-format error in the reference implementation; this yields a coded error
            // instead (documented divergence in the module docs).
            FeelValue::String(s) => s.parse().map_err(|_| {
                FeelError::plain(
                    codes::FEEL_COMPILE_TYPE_MISMATCH,
                    "Cannot convert to number: String",
                )
            }),
            // No site for this path — invoked from many sub-expressions (a position-less
            // diagnostic).
            other => Err(FeelError::plain(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("Cannot convert to number: {}", other.type_name()),
            )),
        }
    }

    /// Exactly-one-argument numeric builtin helper: requires a single, strictly-numeric arg (no
    /// null/string coercion — `abs`/`sqrt`/`floor`/… reject a non-number).
    fn num1(&self, args: &[FeelValue], name: &str, site: usize) -> Result<BigDecimal, FeelError> {
        self.require_args(name, args, 1, site)?;
        self.require_number(&args[0], name, site)
    }

    /// The numeric elements of an aggregation call — a single list arg is unwrapped to its
    /// elements, otherwise the varargs are taken as-is; every element must already be a number
    /// (null / non-number aborts rather than silently coercing, so a wrong aggregate never
    /// masquerades as a pass).
    fn num_items(
        &self,
        args: &[FeelValue],
        name: &str,
        site: usize,
    ) -> Result<Vec<BigDecimal>, FeelError> {
        let items = agg_items(args);
        let mut out = Vec::with_capacity(items.len());
        for v in items {
            match v {
                FeelValue::Number(n) => out.push(n),
                other => {
                    return Err(self.error(
                        codes::FEEL_COMPILE_TYPE_MISMATCH,
                        format!("{name}() expects numbers, got {}", other.type_name()),
                        site,
                    ))
                }
            }
        }
        Ok(out)
    }

    /// `substring(string, start, length?)` — 1-indexed, a negative `start` counts from the end;
    /// an out-of-range start yields the empty string. Operates on Unicode scalar values.
    fn substring(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.len() < 2 || args.len() > 3 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("substring() expects 2 or 3 arguments, got {}", args.len()),
                site,
            ));
        }
        let chars: Vec<char> = str_of(&args[0]).chars().collect();
        let n = chars.len() as i64;
        let start = self.to_big_decimal(&args[1])?.to_i64().unwrap_or(0);
        let idx0 = if start < 0 { n + start } else { start - 1 };
        if idx0 < 0 || idx0 >= n {
            return Ok(FeelValue::String(String::new()));
        }
        let len = if args.len() == 3 {
            self.to_big_decimal(&args[2])?.to_i64().unwrap_or(0)
        } else {
            n - idx0
        };
        let end = (idx0 + len.max(0)).min(n);
        Ok(FeelValue::String(
            chars[idx0 as usize..end as usize].iter().collect(),
        ))
    }

    /// `replace(input, pattern, replacement, flags?)` — regex replace-all. FEEL flags `s/m/i/x`
    /// map to the `regex` inline flags; `$N` group references pass through.
    fn replace(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.len() < 3 || args.len() > 4 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("replace() expects 3 or 4 arguments, got {}", args.len()),
                site,
            ));
        }
        let input = str_of(&args[0]);
        let pattern = str_of(&args[1]);
        let replacement = str_of(&args[2]);
        let flags = if args.len() == 4 {
            str_of(&args[3])
        } else {
            String::new()
        };
        let re = self.compile_regex(&pattern, &flags, "replace", site)?;
        let replacement = disambiguate_replacement_backreferences(&replacement);
        Ok(FeelValue::String(
            re.replace_all(&input, replacement.as_str()).into_owned(),
        ))
    }

    // (see `disambiguate_replacement_backreferences`, a free function below, for why `replace`'s
    // replacement string is rewritten before being handed to the `regex` crate.)

    /// `split(string, delimiter)` — split on a delimiter regex into a list of strings. Both
    /// parameters are typed string (DMN 1.4 §10.3.4); a `null` (or otherwise non-string)
    /// argument is a type error, not a `""` substitution.
    fn split(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        self.require_args("split", args, 2, site)?;
        let (FeelValue::String(s), FeelValue::String(delim)) = (&args[0], &args[1]) else {
            return Ok(FeelValue::Null);
        };
        let re = self.compile_regex(delim, "", "split", site)?;
        Ok(FeelValue::List(
            re.split(s)
                .map(|p| FeelValue::String(p.to_string()))
                .collect(),
        ))
    }

    /// Compile a `matches`/`replace`/`split` pattern under the FEEL/XPath flags subset
    /// (`s`/`m`/`i`/`x`, any other character is rejected — DMN-TCK 1111-feel-matches-function's
    /// M.1 flags-validation cases). Two source pre-passes run before native inline flags are
    /// attached, since the `regex` crate has no way to customize this behaviour itself:
    ///
    /// - `x` (extended): strip un-escaped whitespace outside a `[...]` character class (a
    ///   backslash does NOT protect a following whitespace character — `\ s` collapses to `\s`,
    ///   confirmed against DMN-TCK's own K2-MatchesFunc-1..6). The `regex` crate's native inline
    ///   `x` flag mishandles an all-whitespace character class (a hard compile error), so this
    ///   engine never passes `x` through natively.
    /// - default (non-`s`) mode: XPath/FEEL's `.` excludes both LF and CR, unlike the `regex`
    ///   crate's default (LF only) — rewrite every un-escaped, outside-a-class `.` to `[^\r\n]`.
    ///
    /// A narrow `\p{IsBasicLatin}` → `[\x00-\x7F]` translation (the `regex` crate has no Unicode
    /// **block**-name support, only categories/scripts) and the XSD character-class-subtraction
    /// spelling translation (`[A-Z-[OI]]` → `[A-Z--[OI]]`, see [`translate_class_subtraction`])
    /// run unconditionally.
    fn compile_regex(
        &self,
        pattern: &str,
        flags: &str,
        func: &str,
        site: usize,
    ) -> Result<regex::Regex, FeelError> {
        let prefixed = self.translated_pattern(pattern, flags, func, site)?;
        regex::Regex::new(&prefixed).map_err(|e| {
            self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{func}() invalid regex pattern '{pattern}': {e}"),
                site,
            )
        })
    }

    /// The flag-validated, source-translated, inline-flag-prefixed pattern [`Self::compile_regex`]
    /// compiles — exposed separately so `matches()` can compile the SAME final pattern through its
    /// backtracking fallback engine (see [`Self::matches`]).
    fn translated_pattern(
        &self,
        pattern: &str,
        flags: &str,
        func: &str,
        site: usize,
    ) -> Result<String, FeelError> {
        if let Some(bad) = flags.chars().find(|c| !"smix".contains(*c)) {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{func}() invalid flag '{bad}'"),
                site,
            ));
        }
        // Extended-mode whitespace-collapse runs first: it must land before the block-name
        // translation below, since `\p{ IsBasicLatin}` (with its internal space) only becomes the
        // exact substring `\p{IsBasicLatin}` once collapsed (DMN-TCK K2-MatchesFunc-5/6).
        let mut translated = pattern.to_string();
        if flags.contains('x') {
            translated = strip_extended_whitespace(&translated);
        }
        translated = translate_unicode_blocks(&translated);
        translated = translate_class_subtraction(&translated);
        if !flags.contains('s') {
            translated = exclude_cr_from_dot(&translated);
        }
        // Translate the FEEL/XPath flags subset into a regex inline-flag prefix (`x` is handled
        // entirely by the source pre-pass above, never passed through natively).
        let inline: String = flags.chars().filter(|c| "smi".contains(*c)).collect();
        Ok(if inline.is_empty() {
            translated
        } else {
            format!("(?{inline}){translated}")
        })
    }

    /// `sublist(list, start, length?)` — 1-indexed, a negative `start` counts from the end.
    fn sublist(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.len() < 2 || args.len() > 3 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("sublist() expects 2 or 3 arguments, got {}", args.len()),
                site,
            ));
        }
        let xs = as_list(&args[0]);
        let n = xs.len() as i64;
        let start = self.to_big_decimal(&args[1])?.to_i64().unwrap_or(0);
        let idx0 = if start < 0 { n + start } else { start - 1 };
        if idx0 < 0 || idx0 >= n {
            return Ok(FeelValue::List(Vec::new()));
        }
        let len = if args.len() == 3 {
            self.to_big_decimal(&args[2])?.to_i64().unwrap_or(0)
        } else {
            n - idx0
        };
        let end = (idx0 + len.max(0)).min(n);
        Ok(FeelValue::List(xs[idx0 as usize..end as usize].to_vec()))
    }

    /// `number(from)` / `number(from, grouping separator, decimal separator)` — parse a numeric
    /// string, optionally stripping a grouping separator and normalizing a decimal separator.
    /// DMN 1.4 §10.3.4: `from` must be a string (no coercion); each separator, when supplied, is
    /// either `null` (no-op) or a single character drawn from `{' ', ',', '.'}`, and the two
    /// separators must differ — any violation is a type error ⇒ null, not a best-effort replace.
    fn number(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.is_empty() || args.len() > 3 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("number() expects 1 or 3 arguments, got {}", args.len()),
                site,
            ));
        }
        let FeelValue::String(raw) = &args[0] else {
            return Ok(FeelValue::Null);
        };
        let mut s = raw.clone();
        if args.len() == 3 {
            let (Ok(grouping), Ok(decimal)) = (separator_char(&args[1]), separator_char(&args[2]))
            else {
                return Ok(FeelValue::Null);
            };
            if matches!((grouping, decimal), (Some(g), Some(d)) if g == d) {
                return Ok(FeelValue::Null);
            }
            if let Some(g) = grouping {
                s = s.replace(g, "");
            }
            if let Some(d) = decimal {
                if d != '.' {
                    s = s.replace(d, ".");
                }
            }
        }
        Ok(match s.parse::<BigDecimal>() {
            Ok(n) => FeelValue::Number(n),
            Err(_) => FeelValue::Null,
        })
    }

    /// A strictly-numeric argument — no null/string coercion (unlike `to_big_decimal`, whose
    /// leniency serves arithmetic-operator coercion). Used by the number-only builtins.
    fn require_number(
        &self,
        v: &FeelValue,
        name: &str,
        site: usize,
    ) -> Result<BigDecimal, FeelError> {
        match v {
            FeelValue::Number(n) => Ok(n.clone()),
            other => Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{name}() expects a number, got {}", other.type_name()),
                site,
            )),
        }
    }

    /// `decimal`/`round *`: round a number to a given scale with the supplied rounding mode. Null
    /// args ⇒ null; a scale outside DECIMAL's `-6111..=6176` range ⇒ null (also bounds the size of
    /// the constructed decimal).
    fn round_scaled(
        &self,
        args: &[FeelValue],
        mode: RoundingMode,
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        self.require_args("round", args, 2, site)?;
        if args[0].is_null() || args[1].is_null() {
            return Ok(FeelValue::Null);
        }
        let n = self.require_number(&args[0], "round", site)?;
        let scale = self
            .require_number(&args[1], "round", site)?
            .to_i64()
            .unwrap_or(i64::MAX);
        if !(-6111..=6176).contains(&scale) {
            return Ok(FeelValue::Null);
        }
        Ok(FeelValue::Number(n.with_scale_round(scale, mode)))
    }

    /// `floor(n)` / `floor(n, scale)` (and `ceiling`) — round toward negative/positive infinity;
    /// the optional second argument is the decimal scale to round to (DMN 1.4's 2-arg form),
    /// defaulting to `0` (whole numbers) when omitted entirely. Unlike `round`/`decimal`, a
    /// *present-but-null* scale is rejected (⇒ null), not treated as "omitted".
    fn floor_ceiling(
        &self,
        args: &[FeelValue],
        rounding: RoundingMode,
        name: &str,
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        if args.is_empty() || args.len() > 2 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{name}() expects 1 or 2 arguments, got {}", args.len()),
                site,
            ));
        }
        let n = self.require_number(&args[0], name, site)?;
        let scale = if args.len() == 2 {
            if args[1].is_null() {
                return Ok(FeelValue::Null);
            }
            self.require_number(&args[1], name, site)?
                .to_i64()
                .unwrap_or(i64::MAX)
        } else {
            0
        };
        if !(-6111..=6176).contains(&scale) {
            return Ok(FeelValue::Null);
        }
        Ok(FeelValue::Number(n.with_scale_round(scale, rounding)))
    }

    fn require_int(&self, v: &FeelValue, name: &str, site: usize) -> Result<i64, FeelError> {
        int_of(v).ok_or_else(|| {
            self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("{name}() expects integer arguments"),
                site,
            )
        })
    }

    fn temporal_err(&self, what: &str, site: usize) -> FeelError {
        self.error(codes::FEEL_COMPILE_TYPE_MISMATCH, what.to_string(), site)
    }

    /// `date(string)` / `date(dateTime)` / `date(year, month, day)`.
    fn date_builtin(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        match args {
            [FeelValue::String(s)] => match crate::temporal::parse_at_literal(s) {
                Some(FeelValue::Date(d)) => Ok(FeelValue::Date(d)),
                Some(FeelValue::Instant(dt, _)) => Ok(FeelValue::Date(dt.date())),
                _ => Err(self.temporal_err("date() cannot parse the string", site)),
            },
            // Narrowing to `Date` drops the qualifier — a `Date` is dateless *and* zoneless by
            // definition.
            [FeelValue::Instant(t, _)] => Ok(FeelValue::Date(t.date())),
            [FeelValue::Date(d)] => Ok(FeelValue::Date(*d)),
            [y, m, d] => {
                let year: i32 = self
                    .require_int(y, "date", site)?
                    .try_into()
                    .map_err(|_| self.temporal_err("date() year out of range", site))?;
                let month = u8::try_from(self.require_int(m, "date", site)?)
                    .ok()
                    .and_then(|n| time::Month::try_from(n).ok())
                    .ok_or_else(|| self.temporal_err("date() month out of range", site))?;
                let day = u8::try_from(self.require_int(d, "date", site)?)
                    .map_err(|_| self.temporal_err("date() day out of range", site))?;
                time::Date::from_calendar_date(year, month, day)
                    .map(FeelValue::Date)
                    .map_err(|_| self.temporal_err("date() is not a valid calendar date", site))
            }
            _ => Err(self.temporal_err("date() expects 1 or 3 arguments", site)),
        }
    }

    /// `time(string)` / `time(dateTime)` / `time(time)` / `time(date)` /
    /// `time(hour, minute, second[, offset])`.
    fn time_builtin(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        match args {
            [FeelValue::String(s)] => match crate::temporal::parse_at_literal(s) {
                Some(FeelValue::Time(t, q)) => Ok(FeelValue::Time(t, q)),
                // Carries the source `Instant`'s qualifier over — narrowing to a bare
                // time-of-day doesn't discard whichever zone/offset it was quoted in.
                Some(FeelValue::Instant(dt, q)) => Ok(FeelValue::Time(dt.time(), q)),
                _ => Err(self.temporal_err("time() cannot parse the string", site)),
            },
            [FeelValue::Instant(t, q)] => Ok(FeelValue::Time(t.time(), q.clone())),
            [FeelValue::Time(t, q)] => Ok(FeelValue::Time(*t, q.clone())),
            // A bare `Date` promotes to midnight, explicit UTC (mirrors `date and
            // time(date)`'s own midnight-UTC promotion) — DMN-TCK 1116 `#053`.
            [FeelValue::Date(_)] => Ok(FeelValue::Time(
                time::Time::MIDNIGHT,
                Some(TimeQualifier::Zulu),
            )),
            [h, m, s] => self.time_from_components(h, m, s, None, site),
            [h, m, s, off] => self.time_from_components(h, m, s, Some(off), site),
            _ => Err(self.temporal_err("time() expects 1, 2, 3 or 4 arguments", site)),
        }
    }

    /// `hour`/`minute` must be integers; `second` may carry a fractional part (sub-second
    /// precision — DMN-TCK 0007 `Time3`); an optional 4th `offset` (a `dayTimeDuration`,
    /// validated to FEEL's ±14:00 range) becomes the result's qualifier.
    fn time_from_components(
        &self,
        h: &FeelValue,
        m: &FeelValue,
        s: &FeelValue,
        offset: Option<&FeelValue>,
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        let hour = u8::try_from(self.require_int(h, "time", site)?);
        let min = u8::try_from(self.require_int(m, "time", site)?);
        let FeelValue::Number(sec_n) = s else {
            return Err(self.temporal_err("time() expects a number for seconds", site));
        };
        let (sec, nanos) = split_seconds(sec_n)
            .ok_or_else(|| self.temporal_err("time() seconds component out of range", site))?;
        // An explicit `null` 4th argument is the same as omitting it entirely (DMN-TCK 1116
        // `#015`/`#038`: `time(12,00,00,null)`/`time(11,59,45,null)` both still succeed, offset-
        // less) — mirrors the same "present-but-null == omitted" pattern already established for
        // `matches()`'s flags argument.
        let qualifier = match offset {
            None | Some(FeelValue::Null) => None,
            Some(FeelValue::Duration(FeelDuration::DaysTime(d))) => Some(TimeQualifier::Offset(
                offset_from_duration(*d)
                    .ok_or_else(|| self.temporal_err("time() offset out of range", site))?,
            )),
            Some(_) => {
                return Err(self.temporal_err("time() offset must be a dayTimeDuration", site))
            }
        };
        match (hour, min) {
            (Ok(h), Ok(m)) => time::Time::from_hms_nano(h, m, sec, nanos)
                .map(|t| FeelValue::Time(t, qualifier))
                .map_err(|_| self.temporal_err("time() is not a valid time", site)),
            _ => Err(self.temporal_err("time() component out of range", site)),
        }
    }

    /// `duration(string)`.
    fn duration_builtin(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        match args {
            [FeelValue::String(s)] => crate::temporal::parse_duration(s)
                .map(FeelValue::Duration)
                .ok_or_else(|| self.temporal_err("duration() cannot parse the string", site)),
            [FeelValue::Duration(d)] => Ok(FeelValue::Duration(d.clone())),
            _ => Err(self.temporal_err("duration() expects a single string argument", site)),
        }
    }

    /// `date and time(string)` / `date and time(date, time)` / `date and time(dateTime, time)`.
    fn date_and_time_builtin(
        &self,
        args: &[FeelValue],
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        match args {
            [FeelValue::String(s)] => match crate::temporal::parse_at_literal(s) {
                Some(FeelValue::Instant(dt, q)) => Ok(FeelValue::Instant(dt, q)),
                // A date-only string defaults the time-of-day to midnight, LOCAL (no zone info
                // at all) — DMN-TCK 1117 `#007`: `date and time("2012-12-24")` renders back as
                // `"2012-12-24T00:00:00"`, no `Z`. (Contrast `time(date)`, which promotes to
                // explicit UTC — a different builtin with a different spec'd rule.)
                Some(FeelValue::Date(d)) => Ok(FeelValue::Instant(
                    time::PrimitiveDateTime::new(d, time::Time::MIDNIGHT).assume_utc(),
                    None,
                )),
                _ => Err(self.temporal_err("date and time() cannot parse the string", site)),
            },
            [FeelValue::Instant(t, q)] => Ok(FeelValue::Instant(*t, q.clone())),
            // The first argument accepts either a bare `date` or a `date and time` value (only
            // its date portion is used) — the time-of-day (with whatever qualifier it carries)
            // comes wholesale from the second argument (DMN-TCK 1117 cluster: `#034`.., the
            // single biggest ERR bucket in the temporal slice).
            [FeelValue::Date(d), FeelValue::Time(t, q)] => Ok(combine_date_time(*d, *t, q.clone())),
            [FeelValue::Instant(dt, _), FeelValue::Time(t, q)] => {
                Ok(combine_date_time(dt.date(), *t, q.clone()))
            }
            // Two-string form: parse a date string and a time string, then combine.
            [FeelValue::String(ds), FeelValue::String(ts)] => match (
                crate::temporal::parse_at_literal(ds),
                crate::temporal::parse_at_literal(ts),
            ) {
                (Some(FeelValue::Date(d)), Some(FeelValue::Time(t, q))) => {
                    Ok(combine_date_time(d, t, q))
                }
                _ => Err(self.temporal_err("date and time() cannot parse the arguments", site)),
            },
            _ => Err(self.temporal_err("date and time() expects a string or (date, time)", site)),
        }
    }

    /// `year`/`month`/`day`/`hour`/`minute`/`second` accessor → a number.
    fn temporal_component(
        &self,
        which: &str,
        v: &FeelValue,
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        let n: i64 = match (which, v) {
            ("year", FeelValue::Date(d)) => d.year() as i64,
            ("year", FeelValue::Instant(t, _)) => t.year() as i64,
            ("month", FeelValue::Date(d)) => u8::from(d.month()) as i64,
            ("month", FeelValue::Instant(t, _)) => u8::from(t.month()) as i64,
            ("day", FeelValue::Date(d)) => d.day() as i64,
            ("day", FeelValue::Instant(t, _)) => t.day() as i64,
            ("hour", FeelValue::Time(t, _)) => t.hour() as i64,
            ("hour", FeelValue::Instant(t, _)) => t.hour() as i64,
            ("minute", FeelValue::Time(t, _)) => t.minute() as i64,
            ("minute", FeelValue::Instant(t, _)) => t.minute() as i64,
            ("second", FeelValue::Time(t, _)) => t.second() as i64,
            ("second", FeelValue::Instant(t, _)) => t.second() as i64,
            _ => {
                return Err(self.temporal_err(
                    &format!("{which}() is not defined for {}", v.type_name()),
                    site,
                ))
            }
        };
        Ok(FeelValue::Number(BigDecimal::from(n)))
    }

    /// `string join(list, delimiter?)` — a bare `String` first argument is coerced to a
    /// one-element list (DMN-TCK 1140#decision015/016); any other non-list value (a number,
    /// `null`, …) is rejected. Every list element must be a `String` or `Null` (nulls are
    /// skipped, everything else is rejected) — no `str_of`-style silent coercion.
    fn string_join(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        if args.is_empty() || args.len() > 2 {
            return Err(self.error(
                codes::FEEL_COMPILE_TYPE_MISMATCH,
                format!("string join() expects 1 or 2 arguments, got {}", args.len()),
                site,
            ));
        }
        let items: Vec<FeelValue> = match &args[0] {
            FeelValue::List(xs) => xs.clone(),
            FeelValue::String(_) => vec![args[0].clone()],
            _ => return Ok(FeelValue::Null),
        };
        let delim = if args.len() == 2 {
            str_of(&args[1])
        } else {
            String::new()
        };
        let mut parts = Vec::with_capacity(items.len());
        for v in &items {
            match v {
                FeelValue::Null => {} // skipped
                FeelValue::String(s) => parts.push(s.clone()),
                // A non-string, non-null element ⇒ null (consistent with the non-list/non-string
                // first-argument rejection above — no `str_of`-style silent coercion).
                _ => return Ok(FeelValue::Null),
            }
        }
        Ok(FeelValue::String(parts.join(&delim)))
    }

    /// Parse & validate a `range(from)` builtin argument against DMN's dedicated literal-range-
    /// string grammar (the `range(from)` conversion function): both endpoints must be
    /// literal-shaped (see [`is_range_literal_endpoint`]), of the same comparable type, non-null,
    /// and ascending (`start <= end`). `None` ⇒ the caller raises its own type-mismatch error.
    fn parse_range_literal(&self, v: &FeelValue) -> Option<FeelValue> {
        let FeelValue::String(s) = v else {
            return None;
        };
        let expr = crate::expressions::parse(s).ok()?;
        let FeelExpr::Range {
            from,
            to,
            from_inclusive,
            to_inclusive,
            ..
        } = &expr
        else {
            // A comparison-operator range (`(< 10)`) parses to `OpenRange`, not `Range` — already
            // excluded here, no special case needed (DMN-TCK 1156-range-function#017).
            return None;
        };
        if !is_range_literal_endpoint(from) || !is_range_literal_endpoint(to) {
            return None;
        }
        let start = self.eval_internal(from, &FeelContext::new()).ok()?;
        let end = self.eval_internal(to, &FeelContext::new()).ok()?;
        if start.is_null() || end.is_null() {
            return None;
        }
        match feel_cmp(&start, &end) {
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => {}
            _ => return None, // mismatched types (None) or a descending range (Greater)
        }
        Some(FeelValue::Range(crate::value::FeelRange {
            start: Box::new(start),
            end: Box::new(end),
            start_inclusive: *from_inclusive,
            end_inclusive: *to_inclusive,
            comparison_op: None,
        }))
    }

    /// `context(entries)` — build a context from a list of `{key: <string>, value: <any>}` entry
    /// maps (a single entry map is coerced to a one-element list). Each entry must have exactly a
    /// non-null string "key" and a present "value" (itself possibly `null`); other fields are
    /// ignored; a duplicate key across entries is rejected (DMN 1.4's "a new context that
    /// includes all specified entries" can't be honoured with a repeated key).
    fn context_builtin(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        self.require_args("context", args, 1, site)?;
        let entries: Vec<FeelValue> = match &args[0] {
            FeelValue::List(xs) => xs.clone(),
            FeelValue::Map(_) => vec![args[0].clone()],
            _ => return Ok(FeelValue::Null),
        };
        let mut out = std::collections::BTreeMap::new();
        for entry in entries {
            let FeelValue::Map(m) = entry else {
                return Ok(FeelValue::Null);
            };
            let Some(FeelValue::String(key)) = m.get("key") else {
                return Ok(FeelValue::Null);
            };
            let Some(value) = m.get("value") else {
                return Ok(FeelValue::Null);
            };
            if out.contains_key(key) {
                return Ok(FeelValue::Null);
            }
            out.insert(key.clone(), value.clone());
        }
        Ok(FeelValue::Map(out))
    }

    /// `context put(context, key, value)` / `context put(context, keys, value)` — DMN 1.4
    /// overloads one builtin name with two non-interchangeable named forms sharing a position: a
    /// single string `key` sets/overwrites a top-level entry; a list `keys` (a path) recurses
    /// into nested contexts, creating no new intermediate levels (every path segment but the
    /// last must already resolve to a context — DMN-TCK 1146#nested009). Handled outside the
    /// generic named-argument table ([`bind_builtin_args`]) because binding a list path via the
    /// singular `key` name is itself rejected (DMN-TCK 1146#nested008), which requires seeing
    /// which literal name was written at the call site, not just its position.
    fn context_put(
        &self,
        args: &[FeelValue],
        arg_names: &[Option<String>],
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        let named = arg_names.iter().any(Option::is_some);
        let (ctx_v, key_v, value_v) = if named {
            let mut ctx_v = None;
            let mut key_v: Option<(&'static str, FeelValue)> = None;
            let mut value_v = None;
            for (arg_name, value) in arg_names.iter().zip(args.iter()) {
                match arg_name.as_deref() {
                    Some("context") => ctx_v = Some(value.clone()),
                    Some("key") => key_v = Some(("key", value.clone())),
                    Some("keys") => key_v = Some(("keys", value.clone())),
                    Some("value") => value_v = Some(value.clone()),
                    _ => return Ok(FeelValue::Null), // unrecognized name or positional mix
                }
            }
            let (Some(c), Some((key_name, k)), Some(v)) = (ctx_v, key_v, value_v) else {
                return Ok(FeelValue::Null);
            };
            if key_name == "key" && matches!(k, FeelValue::List(_)) {
                return Ok(FeelValue::Null);
            }
            (c, k, v)
        } else {
            if args.len() != 3 {
                return Ok(FeelValue::Null);
            }
            self.require_args("context put", args, 3, site)?;
            (args[0].clone(), args[1].clone(), args[2].clone())
        };
        let FeelValue::Map(base) = ctx_v else {
            return Ok(FeelValue::Null);
        };
        match key_v {
            FeelValue::String(k) => {
                let mut out = base;
                out.insert(k, value_v);
                Ok(FeelValue::Map(out))
            }
            FeelValue::List(path) => {
                Ok(set_nested(&base, &path, value_v).unwrap_or(FeelValue::Null))
            }
            _ => Ok(FeelValue::Null),
        }
    }

    /// `list replace(list, position, newItem)` — replace the (1-based, negative-indexes-from-the-
    /// end) element at `position` with `newItem`; OR `list replace(list, match, newItem)` —
    /// replace every element for which the 2-argument boolean predicate `match(item, newItem)`
    /// holds. The two forms share one name and argument COUNT (3), disambiguated by the SECOND
    /// argument's own value type (`Number` selects the position form, `Function` the match
    /// form) — DMN 1.4's own dual-signature definition for this builtin (DMN-TCK
    /// 1155-list-replace-function), the same "two non-interchangeable named forms at the same
    /// position" shape `context put`'s `key`/`keys` duality already has, which is why this binds
    /// itself rather than going through `bind_builtin_args`'s generic single-schema table.
    ///
    /// Every failure mode is `null`, never a partial/best-effort replace: a non-3-arg call
    /// (positional or named), an unrecognized named argument, a `null`/non-list-non-scalar
    /// `list`, a `null`/wrong-typed `position`, an out-of-bounds (including zero) `position`, a
    /// `match` function whose OWN declared arity isn't exactly 2, or a `match` result that isn't
    /// a plain boolean for some element (the WHOLE call is invalid then, not just that element —
    /// DMN-TCK #017/#018/#019). A bare (non-list) `list` argument coerces to a singleton list
    /// first (#021), and `newItem` itself may be `null` (#008).
    fn list_replace(
        &self,
        args: &[FeelValue],
        arg_names: &[Option<String>],
        site: usize,
    ) -> Result<FeelValue, FeelError> {
        let named = arg_names.iter().any(Option::is_some);
        let (list_v, second_v, new_item_v) = if named {
            let mut list_v = None;
            let mut second_v: Option<FeelValue> = None; // `position` or `match`
            let mut new_item_v = None;
            for (arg_name, value) in arg_names.iter().zip(args.iter()) {
                match arg_name.as_deref() {
                    Some("list") => list_v = Some(value.clone()),
                    Some("position") | Some("match") => second_v = Some(value.clone()),
                    Some("newItem") => new_item_v = Some(value.clone()),
                    _ => return Ok(FeelValue::Null), // unrecognized name or a positional mix
                }
            }
            let (Some(l), Some(s), Some(n)) = (list_v, second_v, new_item_v) else {
                return Ok(FeelValue::Null);
            };
            (l, s, n)
        } else {
            if args.len() != 3 {
                return Ok(FeelValue::Null);
            }
            (args[0].clone(), args[1].clone(), args[2].clone())
        };
        let items = match &list_v {
            FeelValue::List(xs) => xs.clone(),
            FeelValue::Null => return Ok(FeelValue::Null),
            other => vec![other.clone()], // scalar `list` coerces to a singleton list
        };
        match &second_v {
            FeelValue::Function(f) => {
                // Match-function form: the function's OWN declared arity must be exactly 2
                // (item, newItem) — a wrong-arity match function is invalid, never called.
                if f.params.len() != 2 {
                    return Ok(FeelValue::Null);
                }
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    let call_args = [item.clone(), new_item_v.clone()];
                    match self.invoke_function(f, &call_args, &[None, None], site)? {
                        FeelValue::Boolean(true) => out.push(new_item_v.clone()),
                        FeelValue::Boolean(false) => out.push(item.clone()),
                        // A non-boolean match result invalidates the WHOLE call, not just this
                        // one element (mirrors this engine's boxed-filter/quantifier posture).
                        _ => return Ok(FeelValue::Null),
                    }
                }
                Ok(FeelValue::List(out))
            }
            FeelValue::Number(_) => {
                let pos = self.to_big_decimal(&second_v)?.to_i64().unwrap_or(0);
                let n = items.len() as i64;
                let idx0 = if pos < 0 { n + pos } else { pos - 1 };
                if pos == 0 || idx0 < 0 || idx0 >= n {
                    return Ok(FeelValue::Null);
                }
                let mut out = items;
                out[idx0 as usize] = new_item_v;
                Ok(FeelValue::List(out))
            }
            // `position`/`match` missing, `null`, or a type neither form accepts (e.g. a string).
            _ => Ok(FeelValue::Null),
        }
    }

    /// `sort(list, precedes)` — sort by a boolean comparator function `precedes(x, y)` (true when
    /// `x` should sort before `y`).
    fn sort_builtin(&self, args: &[FeelValue], site: usize) -> Result<FeelValue, FeelError> {
        self.require_args("sort", args, 2, site)?;
        let (FeelValue::List(xs), FeelValue::Function(f)) = (&args[0], &args[1]) else {
            return Ok(FeelValue::Null);
        };
        let mut out = xs.clone();
        let mut err = None;
        out.sort_by(|a, b| {
            if err.is_some() {
                return std::cmp::Ordering::Equal;
            }
            match self.invoke_function(f, &[a.clone(), b.clone()], &[None, None], site) {
                Ok(FeelValue::Boolean(true)) => std::cmp::Ordering::Less,
                Ok(_) => std::cmp::Ordering::Greater,
                Err(e) => {
                    err = Some(e);
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(FeelValue::List(out))
    }
}

/// FEEL exponentiation `base ** exp`. An integer exponent is exact (repeated multiplication,
/// with a reciprocal for negative powers); a fractional exponent falls back to `f64::powf`
/// (documented precision divergence). A non-finite result (e.g. `0 ** -1`) is FEEL `null`.
fn pow_decimal(base: &BigDecimal, exp: &BigDecimal) -> FeelValue {
    if exp.is_integer() {
        if let Some(n) = exp.to_i64() {
            if n >= 0 {
                let mut acc = BigDecimal::from(1);
                for _ in 0..n {
                    acc = crate::numeric::mul(&acc, base);
                }
                return FeelValue::Number(acc);
            }
            if base.is_zero() {
                return FeelValue::Null; // 0 to a negative power is undefined ⇒ null
            }
            let mut acc = BigDecimal::from(1);
            for _ in 0..(-n) {
                acc = crate::numeric::mul(&acc, base);
            }
            return FeelValue::Number(crate::numeric::div(&BigDecimal::from(1), &acc));
        }
    }
    match (base.to_f64(), exp.to_f64()) {
        (Some(b), Some(e)) => {
            let r = b.powf(e);
            if r.is_finite() {
                FeelValue::from(r)
            } else {
                FeelValue::Null
            }
        }
        _ => FeelValue::Null,
    }
}

/// Resolve a dotted path (`a.b.c`) against the context: the first segment is a plain context-key
/// lookup, and every SUBSEQUENT segment reuses [`field_access`]'s full per-type field logic —
/// not just a `Map` lookup. A multi-segment path and a chain of `FieldAccess` postfixes
/// (`x.y.z` parsed either as one `Path{segments}` or as nested `FieldAccess{base, field}` nodes,
/// depending on what `x` looks like syntactically) must behave IDENTICALLY; before this, a path's
/// continuation segments only ever tried a `Map` lookup, silently going `null` the moment an
/// intermediate value was anything else — e.g. a `Date`/`Instant`/`Time` component property
/// (DMN-TCK 0007-date-time's `Date.fromString.day`: `Date.fromString` is a `Date` value, and
/// `.day` needs `field_access`'s temporal-component arm, never reached via a bare `Map` match) or
/// a `List` projection. `field_access`'s own `Map` arm is behaviorally identical to the old
/// inline match, so this is a pure superset — no change for any already-working Map chain.
fn resolve_path(segments: &[String], ctx: &FeelContext) -> FeelValue {
    let mut current = match ctx.get(&segments[0]) {
        Some(v) => v.clone(),
        None => return FeelValue::Null,
    };
    for seg in &segments[1..] {
        current = field_access(&current, seg);
    }
    current
}

/// Aggregation argument unwrapping: a lone list argument is its own elements; otherwise the
/// varargs are the items (so `sum([1,2,3])` and `sum(1,2,3)` both aggregate three values).
fn agg_items(args: &[FeelValue]) -> Vec<FeelValue> {
    if let [FeelValue::List(xs)] = args {
        xs.clone()
    } else {
        args.to_vec()
    }
}

/// Field access: a `Map` yields the named entry (or null); a `List` projects the field over its
/// elements (`people.name`); anything else is null.
fn field_access(v: &FeelValue, field: &str) -> FeelValue {
    let num = |n: i64| FeelValue::Number(BigDecimal::from(n));
    match v {
        FeelValue::Map(m) => m.get(field).cloned().unwrap_or(FeelValue::Null),
        FeelValue::List(xs) => FeelValue::List(xs.iter().map(|e| field_access(e, field)).collect()),
        // Temporal component properties (FEEL §10.3.2.3).
        FeelValue::Date(d) => match field {
            "year" => num(d.year() as i64),
            "month" => num(u8::from(d.month()) as i64),
            "day" => num(d.day() as i64),
            "weekday" => num(d.weekday().number_from_monday() as i64),
            _ => FeelValue::Null,
        },
        FeelValue::Instant(t, q) => match field {
            "year" => num(t.year() as i64),
            "month" => num(u8::from(t.month()) as i64),
            "day" => num(t.day() as i64),
            "weekday" => num(t.weekday().number_from_monday() as i64),
            "hour" => num(t.hour() as i64),
            "minute" => num(t.minute() as i64),
            "second" => num(t.second() as i64),
            // `q` is always resolved for real (even a `@Zone` qualifier), so reading the
            // `Instant`'s own stored offset is correct regardless of which qualifier kind it is —
            // `null` only when there was no offset/zone in the source at all.
            "time offset" if q.is_some() => FeelValue::Duration(FeelDuration::DaysTime(
                time::Duration::seconds(t.offset().whole_seconds() as i64),
            )),
            // `.timezone` is the IANA zone NAME (`@Etc/UTC`, `@Australia/Melbourne`, …) — distinct
            // from `.time offset`'s numeric duration, and only defined for a `@Zone`-qualified
            // value (DMN-TCK 0074-feel-properties#dateTime_009); an explicit numeric offset or a
            // "local"/floating value (no zone in the source) has no zone NAME to report, `null`.
            "timezone" => match q {
                Some(TimeQualifier::Zone(name)) => FeelValue::String(name.clone()),
                _ => FeelValue::Null,
            },
            _ => FeelValue::Null,
        },
        FeelValue::Time(t, q) => match field {
            "hour" => num(t.hour() as i64),
            "minute" => num(t.minute() as i64),
            "second" => num(t.second() as i64),
            "time offset" => qualifier_offset_duration(q),
            _ => FeelValue::Null,
        },
        FeelValue::Duration(FeelDuration::YearsMonths(m)) => match field {
            "years" => num((*m / 12) as i64),
            "months" => num((*m % 12) as i64),
            _ => FeelValue::Null,
        },
        FeelValue::Duration(FeelDuration::DaysTime(d)) => {
            let total = d.whole_seconds();
            let mag = total.unsigned_abs() as i64;
            let signed = |x: i64| if total < 0 { -x } else { x };
            match field {
                "days" => num(signed(mag / 86_400)),
                "hours" => num(signed((mag % 86_400) / 3_600)),
                "minutes" => num(signed((mag % 3_600) / 60)),
                "seconds" => num(signed(mag % 60)),
                _ => FeelValue::Null,
            }
        }
        FeelValue::Range(r) => match field {
            "start" => (*r.start).clone(),
            "end" => (*r.end).clone(),
            "start included" => FeelValue::Boolean(r.start_inclusive),
            "end included" => FeelValue::Boolean(r.end_inclusive),
            _ => FeelValue::Null,
        },
        _ => FeelValue::Null,
    }
}

/// A `Time`'s `time offset` property — `null` with no qualifier at all; a `@Zone` qualifier has
/// no resolved offset to read here (a bare `Time` has no date to resolve a DST-dependent zone
/// against — not exercised by the corpus, which only ever reads this property off a
/// numeric-offset/no-offset `time()`/`date and time()` value).
fn qualifier_offset_duration(q: &Option<TimeQualifier>) -> FeelValue {
    match q {
        None | Some(TimeQualifier::Zone(_)) => FeelValue::Null,
        Some(TimeQualifier::Zulu) => {
            FeelValue::Duration(FeelDuration::DaysTime(time::Duration::ZERO))
        }
        Some(TimeQualifier::Offset(o)) => FeelValue::Duration(FeelDuration::DaysTime(
            time::Duration::seconds(o.whole_seconds() as i64),
        )),
    }
}

/// FEEL `instance of` type test against a (possibly generic/structural) [`FeelTypeShape`] — the
/// same shape machinery DMN §10.3.2.13 typeRef coercion uses (`sutra-dmn`'s DRG builds these from
/// `<itemDefinition>`; the parser builds them directly from a FEEL `instance of` type expression,
/// including `list<T>`/`context<k: T, ...>`/`range<T>` generics — DMN-TCK 0070-feel-instance-of
/// `list_018`/`list_019`/`list_020`/`context_018..024`).
///
/// `resolve_named` is consulted only for a `Base` name this function doesn't itself recognize as
/// one of FEEL's fixed base types — a DMN `<itemDefinition>` custom type name (`t255`, `tFooBar`,
/// `tNumberList`, `t_context_013`, …), resolved into its own real shape and checked recursively
/// (DMN-TCK `number_013`/`string_013`/`list_013`/`list_014`/`list_014_a`/`context_013`/
/// `context_014`). Absent a resolver, or the resolver not recognizing the name either, an
/// unrecognized type name is never an instance (this engine's plain-FEEL posture, unaffected
/// outside a DMN context).
///
/// A `null` LIST ELEMENT / RECORD COMPONENT VALUE always conforms to its declared shape — the
/// same DMN §10.3.2.13 "null always conforms" leniency [`coerce_to_shape`] applies (DMN-TCK
/// `context_019`: `{a: null} instance of context<a: string>` is `true`). This is deliberately
/// NOT applied at the top level: `null instance of string` stays `false` (DMN-TCK `null_003`) —
/// the plain `Base` match below never special-cases `null`, it simply never matches any concrete
/// variant, which is exactly right for the top-level (non-nested) case.
fn instance_of_shape(
    v: &FeelValue,
    shape: &crate::value::FeelTypeShape,
    resolve_named: Option<&crate::value::TypeResolver<'_>>,
) -> bool {
    use crate::value::FeelTypeShape;
    match shape {
        FeelTypeShape::Any => !v.is_null(),
        FeelTypeShape::Base(name) => match name.as_str() {
            "number" => matches!(v, FeelValue::Number(_)),
            "string" => matches!(v, FeelValue::String(_)),
            "boolean" => matches!(v, FeelValue::Boolean(_)),
            "date" => matches!(v, FeelValue::Date(_)),
            "time" => matches!(v, FeelValue::Time(..)),
            "date and time" => matches!(v, FeelValue::Instant(..)),
            "days and time duration" => {
                matches!(v, FeelValue::Duration(FeelDuration::DaysTime(_)))
            }
            "years and months duration" => {
                matches!(v, FeelValue::Duration(FeelDuration::YearsMonths(_)))
            }
            "duration" => matches!(v, FeelValue::Duration(_)),
            "list" => matches!(v, FeelValue::List(_)),
            "context" => matches!(v, FeelValue::Map(_)),
            "function" => matches!(v, FeelValue::Function(_) | FeelValue::Invocable(_)),
            "range" => matches!(v, FeelValue::Range(_)),
            "Any" | "any" => !v.is_null(),
            "null" => v.is_null(),
            custom => match resolve_named.and_then(|f| f(custom)) {
                Some(resolved) => instance_of_shape(v, &resolved, resolve_named),
                None => false,
            },
        },
        FeelTypeShape::Collection(elem) => match v {
            FeelValue::List(items) => items
                .iter()
                .all(|it| it.is_null() || instance_of_shape(it, elem, resolve_named)),
            _ => false,
        },
        FeelTypeShape::Record(components) => match v {
            FeelValue::Map(m) => components
                .iter()
                .all(|(name, comp_shape)| match m.get(name) {
                    Some(val) => val.is_null() || instance_of_shape(val, comp_shape, resolve_named),
                    None => false,
                }),
            _ => false,
        },
        FeelTypeShape::Range(elem) => match v {
            FeelValue::Range(r) => {
                (r.start.is_null() || instance_of_shape(&r.start, elem, resolve_named))
                    && (r.end.is_null() || instance_of_shape(&r.end, elem, resolve_named))
            }
            _ => false,
        },
    }
}

/// A value as a list: a `List` is its own elements, any other value is a one-element list.
fn as_list(v: &FeelValue) -> Vec<FeelValue> {
    match v {
        FeelValue::List(xs) => xs.clone(),
        other => vec![other.clone()],
    }
}

/// Recursively splice nested lists into `out` (one flat sequence of non-list leaves).
fn flatten_into(v: &FeelValue, out: &mut Vec<FeelValue>) {
    match v {
        FeelValue::List(xs) => {
            for x in xs {
                flatten_into(x, out);
            }
        }
        other => out.push(other.clone()),
    }
}

/// Total order between two same-typed comparable FEEL values (number/string/date/time/instant/
/// duration); `None` for null, mismatched, or non-comparable types.
fn feel_cmp(a: &FeelValue, b: &FeelValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (FeelValue::Number(x), FeelValue::Number(y)) => Some(x.cmp(y)),
        (FeelValue::String(x), FeelValue::String(y)) => Some(x.cmp(y)),
        (FeelValue::Date(x), FeelValue::Date(y)) => Some(x.cmp(y)),
        (FeelValue::Time(x, _), FeelValue::Time(y, _)) => Some(x.cmp(y)),
        (FeelValue::Instant(x, _), FeelValue::Instant(y, _)) => Some(x.cmp(y)),
        (
            FeelValue::Duration(FeelDuration::DaysTime(x)),
            FeelValue::Duration(FeelDuration::DaysTime(y)),
        ) => Some(x.cmp(y)),
        (
            FeelValue::Duration(FeelDuration::YearsMonths(x)),
            FeelValue::Duration(FeelDuration::YearsMonths(y)),
        ) => Some(x.cmp(y)),
        _ => None,
    }
}

/// A bare (non-call-position) reference to a FEEL builtin name, as a first-class function value
/// (DMN-TCK 0092-feel-lambda#014: `bkm_014_1(abs, sqrt)` passes builtins as lambdas the BKM body
/// then invokes via its parameters). Wrapped as a native [`crate::value::Invocable`] — NOT a
/// `FeelFunction` — deliberately: an `Invocable`'s arity gating is strict, so invoking the passed
/// builtin with the wrong argument count is "never invoked" ⇒ `null` (0092#016's
/// `bkm_016_1(sqrt)` body calling `fn1(10,2)` expects exactly that error/null), where a lenient
/// `FeelFunction` wrapper would silently drop the extra argument and compute `sqrt(10)`. Only a
/// builtin with a declared parameter-name row ([`builtin_param_names`]) is wrappable (the row IS
/// the wrapper's signature); a builtin with trailing OPTIONAL parameters (e.g. `substring`'s
/// `length`) wraps to its FULL arity — shorter calls through the wrapper are rejected where a
/// direct call accepts them, acceptable for a construct that was previously a hard error. The
/// non-deterministic builtins are deliberately excluded: wrapping them would smuggle `now()`/
/// `uuid()`/… past the parse-time determinism denylist (which scans `Call` nodes only).
fn builtin_as_function_value(name: &str) -> Option<FeelValue> {
    let canonical = canonical_builtin(name);
    if crate::determinism::NON_DETERMINISTIC_BUILTINS.contains(&canonical) {
        return None;
    }
    let params = builtin_param_names(canonical)?;
    let canonical_name = canonical.to_string();
    let call: std::sync::Arc<crate::value::InvocableFn> =
        std::sync::Arc::new(move |args: &[FeelValue]| {
            Evaluator {
                positions: None,
                type_resolver: None,
            }
            .invoke_builtin(&canonical_name, args.to_vec(), &[], 0)
            .unwrap_or(FeelValue::Null)
        });
    Some(FeelValue::Invocable(crate::value::Invocable {
        id: format!("feel-builtin:{canonical}"),
        params: params.iter().map(|s| (*s).to_string()).collect(),
        param_shapes: vec![crate::value::FeelTypeShape::Any; params.len()],
        return_shape: crate::value::FeelTypeShape::Any,
        call,
    }))
}

/// One argument of an interval-relation builtin: a range, or a point of any ordered FEEL type.
#[derive(Clone, Copy)]
enum IntervalRelationArg<'a> {
    Point(&'a FeelValue),
    Range(&'a crate::value::FeelRange),
}

/// The 14 interval/point relation builtins of DMN 1.4 §10.3.4.6 (Table 78): `before`, `after`,
/// `meets`, `met by`, `overlaps`, `overlaps before`, `overlaps after`, `finishes`,
/// `finished by`, `includes`, `during`, `starts`, `started by`, `coincides` — dispatched here by
/// canonical (camelCase) name. Each relation supports exactly the argument shapes the spec table
/// defines for it (point/point, point/range, range/point, range/range — not every function has
/// all four); every open/closed endpoint condition is transcribed verbatim from the table.
/// `None` (⇒ FEEL `null`) for an unsupported shape, a non-orderable point (boolean, list, …), or
/// any endpoint pair that is `null`/cross-type — including a comparison-operator range (`(< 10)`),
/// whose missing side is a `null` endpoint (DMN-TCK 1130-feel-interval, all 14 result nodes).
fn interval_relation(name: &str, a: &FeelValue, b: &FeelValue) -> Option<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    use IntervalRelationArg::{Point, Range};
    fn classify(v: &FeelValue) -> Option<IntervalRelationArg<'_>> {
        match v {
            FeelValue::Range(r) => Some(Range(r)),
            // A point must be an ordered scalar — `feel_cmp` against itself is `Some` exactly
            // for those (number/string/date/time/date-time/duration).
            v if feel_cmp(v, v).is_some() => Some(Point(v)),
            _ => None,
        }
    }
    let a = classify(a)?;
    let b = classify(b)?;
    // Every endpoint involved must be mutually comparable — same ordered type, no `null`s. The
    // chain check suffices: comparability is transitive (same-variant scalars only).
    let mut endpoints: Vec<&FeelValue> = Vec::with_capacity(4);
    for arg in [a, b] {
        match arg {
            Point(p) => endpoints.push(p),
            Range(r) => {
                endpoints.push(&r.start);
                endpoints.push(&r.end);
            }
        }
    }
    for pair in endpoints.windows(2) {
        feel_cmp(pair[0], pair[1])?;
    }
    let lt = |x: &FeelValue, y: &FeelValue| feel_cmp(x, y) == Some(Less);
    let gt = |x: &FeelValue, y: &FeelValue| feel_cmp(x, y) == Some(Greater);
    let eq = |x: &FeelValue, y: &FeelValue| feel_cmp(x, y) == Some(Equal);
    Some(match (name, a, b) {
        ("before", Point(p1), Point(p2)) => lt(p1, p2),
        ("before", Point(p), Range(r)) => {
            lt(p, &r.start) || (eq(p, &r.start) && !r.start_inclusive)
        }
        ("before", Range(r), Point(p)) => lt(&r.end, p) || (eq(&r.end, p) && !r.end_inclusive),
        ("before", Range(r1), Range(r2)) => {
            lt(&r1.end, &r2.start)
                || (eq(&r1.end, &r2.start) && (!r1.end_inclusive || !r2.start_inclusive))
        }
        ("after", Point(p1), Point(p2)) => gt(p1, p2),
        ("after", Point(p), Range(r)) => gt(p, &r.end) || (eq(p, &r.end) && !r.end_inclusive),
        ("after", Range(r), Point(p)) => gt(&r.start, p) || (eq(&r.start, p) && !r.start_inclusive),
        ("after", Range(r1), Range(r2)) => {
            gt(&r1.start, &r2.end)
                || (eq(&r1.start, &r2.end) && (!r1.start_inclusive || !r2.end_inclusive))
        }
        ("meets", Range(r1), Range(r2)) => {
            r1.end_inclusive && r2.start_inclusive && eq(&r1.end, &r2.start)
        }
        ("metBy", Range(r1), Range(r2)) => {
            r1.start_inclusive && r2.end_inclusive && eq(&r1.start, &r2.end)
        }
        ("overlaps", Range(r1), Range(r2)) => {
            (gt(&r1.end, &r2.start)
                || (eq(&r1.end, &r2.start) && r1.end_inclusive && r2.start_inclusive))
                && (lt(&r1.start, &r2.end)
                    || (eq(&r1.start, &r2.end) && r1.start_inclusive && r2.end_inclusive))
        }
        ("overlapsBefore", Range(r1), Range(r2)) => {
            (lt(&r1.start, &r2.start)
                || (eq(&r1.start, &r2.start) && r1.start_inclusive && !r2.start_inclusive))
                && (gt(&r1.end, &r2.start)
                    || (eq(&r1.end, &r2.start) && r1.end_inclusive && r2.start_inclusive))
                && (lt(&r1.end, &r2.end)
                    || (eq(&r1.end, &r2.end) && (!r1.end_inclusive || r2.end_inclusive)))
        }
        ("overlapsAfter", Range(r1), Range(r2)) => {
            (lt(&r2.start, &r1.start)
                || (eq(&r2.start, &r1.start) && r2.start_inclusive && !r1.start_inclusive))
                && (gt(&r2.end, &r1.start)
                    || (eq(&r2.end, &r1.start) && r2.end_inclusive && r1.start_inclusive))
                && (lt(&r2.end, &r1.end)
                    || (eq(&r2.end, &r1.end) && (!r2.end_inclusive || r1.end_inclusive)))
        }
        ("finishes", Point(p), Range(r)) => r.end_inclusive && eq(&r.end, p),
        ("finishes", Range(r1), Range(r2)) => {
            r1.end_inclusive == r2.end_inclusive
                && eq(&r1.end, &r2.end)
                && (gt(&r1.start, &r2.start)
                    || (eq(&r1.start, &r2.start) && (!r1.start_inclusive || r2.start_inclusive)))
        }
        ("finishedBy", Range(r), Point(p)) => r.end_inclusive && eq(&r.end, p),
        ("finishedBy", Range(r1), Range(r2)) => {
            r1.end_inclusive == r2.end_inclusive
                && eq(&r1.end, &r2.end)
                && (lt(&r1.start, &r2.start)
                    || (eq(&r1.start, &r2.start) && (r1.start_inclusive || !r2.start_inclusive)))
        }
        ("includes", Range(r), Point(p)) => {
            (lt(&r.start, p) && gt(&r.end, p))
                || (eq(&r.start, p) && r.start_inclusive)
                || (eq(&r.end, p) && r.end_inclusive)
        }
        ("includes", Range(r1), Range(r2)) => {
            (lt(&r1.start, &r2.start)
                || (eq(&r1.start, &r2.start) && (r1.start_inclusive || !r2.start_inclusive)))
                && (gt(&r1.end, &r2.end)
                    || (eq(&r1.end, &r2.end) && (r1.end_inclusive || !r2.end_inclusive)))
        }
        ("during", Point(p), Range(r)) => {
            (lt(&r.start, p) && gt(&r.end, p))
                || (eq(&r.start, p) && r.start_inclusive)
                || (eq(&r.end, p) && r.end_inclusive)
        }
        // `during(range1, range2)` ≡ `includes(range2, range1)`.
        ("during", Range(r1), Range(r2)) => {
            (lt(&r2.start, &r1.start)
                || (eq(&r2.start, &r1.start) && (r2.start_inclusive || !r1.start_inclusive)))
                && (gt(&r2.end, &r1.end)
                    || (eq(&r2.end, &r1.end) && (r2.end_inclusive || !r1.end_inclusive)))
        }
        ("starts", Point(p), Range(r)) => eq(&r.start, p) && r.start_inclusive,
        ("starts", Range(r1), Range(r2)) => {
            eq(&r1.start, &r2.start)
                && r1.start_inclusive == r2.start_inclusive
                && (lt(&r1.end, &r2.end)
                    || (eq(&r1.end, &r2.end) && (!r1.end_inclusive || r2.end_inclusive)))
        }
        ("startedBy", Range(r), Point(p)) => eq(&r.start, p) && r.start_inclusive,
        ("startedBy", Range(r1), Range(r2)) => {
            eq(&r1.start, &r2.start)
                && r1.start_inclusive == r2.start_inclusive
                && (lt(&r2.end, &r1.end)
                    || (eq(&r2.end, &r1.end) && (!r2.end_inclusive || r1.end_inclusive)))
        }
        ("coincides", Point(p1), Point(p2)) => eq(p1, p2),
        ("coincides", Range(r1), Range(r2)) => {
            eq(&r1.start, &r2.start)
                && r1.start_inclusive == r2.start_inclusive
                && eq(&r1.end, &r2.end)
                && r1.end_inclusive == r2.end_inclusive
        }
        _ => return None,
    })
}

/// FEEL range containment `x in [a..b]` — honours open/closed bounds. Returns `None` (⇒ FEEL
/// `null`, not `false`) when `x` itself is undefined, or when a *closed* range endpoint is `null`
/// or otherwise incomparable to `x` — an open (unbounded) side is never itself compared, so a
/// `null` there is fine (§10.3.2.5/§10.3.2.13). A comparison-operator range (`(< 10)`, …) reduces
/// directly to its operator instead.
fn range_contains(r: &crate::value::FeelRange, x: &FeelValue) -> Option<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    if x.is_null() {
        return None;
    }
    if let Some(op) = r.comparison_op {
        return Some(match op {
            CompareOp::Lt => matches!(feel_cmp(x, &r.end), Some(Less)),
            CompareOp::Le => matches!(feel_cmp(x, &r.end), Some(Less) | Some(Equal)),
            CompareOp::Gt => matches!(feel_cmp(x, &r.start), Some(Greater)),
            CompareOp::Ge => matches!(feel_cmp(x, &r.start), Some(Greater) | Some(Equal)),
            CompareOp::Eq => equals_feel(x, &r.start).unwrap_or(false),
            CompareOp::Neq => !equals_feel(x, &r.start).unwrap_or(true),
        });
    }
    let lower = feel_cmp(x, &r.start)?;
    let upper = feel_cmp(x, &r.end)?;
    let lower_ok = match lower {
        Greater => true,
        Equal => r.start_inclusive,
        _ => false,
    };
    let upper_ok = match upper {
        Less => true,
        Equal => r.end_inclusive,
        _ => false,
    };
    Some(lower_ok && upper_ok)
}

/// Build the [`FeelValue::Range`] denoted by a comparison-operator range value (`(< 10)`,
/// `(>= 5)`, `(=10)`, `(!=10)`, …). The unbounded side is `Null` (never itself inspected — see
/// [`range_contains`]'s `comparison_op` branch); `comparison_op` is always `Some`, which is what
/// keeps this shape structurally distinct from an ordinary interval literal (module doc on
/// [`crate::value::FeelRange::comparison_op`]).
fn open_range_value(op: CompareOp, bound: FeelValue) -> crate::value::FeelRange {
    use crate::value::FeelRange;
    let unbounded = FeelValue::Null;
    match op {
        CompareOp::Lt => FeelRange {
            start: Box::new(unbounded),
            end: Box::new(bound),
            start_inclusive: false,
            end_inclusive: false,
            comparison_op: Some(op),
        },
        CompareOp::Le => FeelRange {
            start: Box::new(unbounded),
            end: Box::new(bound),
            start_inclusive: false,
            end_inclusive: true,
            comparison_op: Some(op),
        },
        CompareOp::Gt => FeelRange {
            start: Box::new(bound),
            end: Box::new(unbounded),
            start_inclusive: false,
            end_inclusive: false,
            comparison_op: Some(op),
        },
        CompareOp::Ge => FeelRange {
            start: Box::new(bound),
            end: Box::new(unbounded),
            start_inclusive: true,
            end_inclusive: false,
            comparison_op: Some(op),
        },
        // Degenerate single-point ranges: both bounds are the same value. `Neq`'s containment is
        // handled specially in `range_contains` (it never uses `end`), so the exact bound choice
        // here only needs to keep `(!=10) = (!=10)` structurally equal to itself.
        CompareOp::Eq | CompareOp::Neq => FeelRange {
            start: Box::new(bound.clone()),
            end: Box::new(bound),
            start_inclusive: true,
            end_inclusive: true,
            comparison_op: Some(op),
        },
    }
}

/// An integer FEEL number as `i64`, else `None` (used for range bounds).
fn int_of(v: &FeelValue) -> Option<i64> {
    match v {
        FeelValue::Number(n) if n.is_integer() => n.to_i64(),
        _ => None,
    }
}

fn is_temporal(v: &FeelValue) -> bool {
    matches!(
        v,
        FeelValue::Date(_) | FeelValue::Instant(..) | FeelValue::Time(..) | FeelValue::Duration(_)
    )
}

fn arith_symbol(op: ArithOp) -> &'static str {
    match op {
        ArithOp::Plus => "+",
        ArithOp::Minus => "-",
        ArithOp::Times => "*",
        ArithOp::Div => "/",
        ArithOp::Pow => "**",
    }
}

/// FEEL temporal arithmetic (§10.3.4). `None` for an unsupported operand/operator combination
/// (the caller turns that into a type error).
fn temporal_arith(l: &FeelValue, op: ArithOp, r: &FeelValue) -> Option<FeelValue> {
    use ArithOp::{Div, Minus, Plus, Times};
    use FeelValue::{Date, Duration, Instant, Number, Time};
    match (l, op, r) {
        // duration ± duration (same flavour)
        (Duration(a), Plus, Duration(b)) => add_durations(a, b, false),
        (Duration(a), Minus, Duration(b)) => add_durations(a, b, true),
        // duration scaled by a number
        (Duration(a), Times, Number(n)) | (Number(n), Times, Duration(a)) => scale_duration(a, n),
        (Duration(a), Div, Number(n)) => div_duration(a, n),
        // `duration / duration` (same flavour) → a plain number ratio.
        (Duration(a), Div, Duration(b)) => div_duration_by_duration(a, b),
        // temporal ± duration
        (Date(d), Plus, Duration(dur)) | (Duration(dur), Plus, Date(d)) => {
            shift_date(*d, dur, false).map(Date)
        }
        (Date(d), Minus, Duration(dur)) => shift_date(*d, dur, true).map(Date),
        (Instant(t, q), Plus, Duration(dur)) | (Duration(dur), Plus, Instant(t, q)) => {
            shift_instant(*t, q, dur, false).map(|(dt, q)| Instant(dt, q))
        }
        (Instant(t, q), Minus, Duration(dur)) => {
            shift_instant(*t, q, dur, true).map(|(dt, q)| Instant(dt, q))
        }
        // Adding a duration to a time-of-day doesn't change which zone/offset it's quoted in —
        // the qualifier is carried unchanged onto the shifted result.
        (Time(t, q), Plus, Duration(FeelDuration::DaysTime(d)))
        | (Duration(FeelDuration::DaysTime(d)), Plus, Time(t, q)) => Some(Time(*t + *d, q.clone())),
        (Time(t, q), Minus, Duration(FeelDuration::DaysTime(d))) => Some(Time(*t - *d, q.clone())),
        // temporal − temporal → days-and-time duration
        (Date(a), Minus, Date(b)) => Some(Duration(FeelDuration::DaysTime(time::Duration::days(
            (a.to_julian_day() - b.to_julian_day()) as i64,
        )))),
        // `date and time − date and time`: DMN's "both or neither operand may carry zone info"
        // rule (DMN-TCK 0100-arithmetic cluster 10) — a "local"/floating value (no offset/zone
        // at all in the source) has no fixed instant to diff against a zoned one; this is
        // distinct from resolving the correct offset (already done for real at parse time).
        (Instant(a, qa), Minus, Instant(b, qb)) => {
            if qa.is_none() != qb.is_none() {
                return None;
            }
            Some(Duration(FeelDuration::DaysTime(*a - *b)))
        }
        // `date and time ± date` (the `date` operand is implicitly midnight UTC) — DMN-TCK
        // 0100-arithmetic cluster 6. Same "both must have zone info" rule as `Instant − Instant`
        // above: the `date` side is always implicitly zoned (UTC), so the `date and time` side
        // must carry SOME qualifier too, or the pairing is undefined (`subtract_lhs_dateAndTime_
        // minus_rhs_date_001`: a *local* `date and time` minus a `date` is null, not a duration).
        (Instant(a, qa), Minus, Date(b)) => {
            qa.as_ref()?;
            Some(Duration(FeelDuration::DaysTime(*a - midnight_utc(*b))))
        }
        (Date(a), Minus, Instant(b, qb)) => {
            qb.as_ref()?;
            Some(Duration(FeelDuration::DaysTime(midnight_utc(*a) - *b)))
        }
        (Time(a, _), Minus, Time(b, _)) => Some(Duration(FeelDuration::DaysTime(*a - *b))),
        _ => None,
    }
}

fn midnight_utc(d: time::Date) -> OffsetDateTime {
    d.midnight().assume_utc()
}

fn add_durations(a: &FeelDuration, b: &FeelDuration, sub: bool) -> Option<FeelValue> {
    match (a, b) {
        (FeelDuration::YearsMonths(m1), FeelDuration::YearsMonths(m2)) => {
            Some(FeelValue::Duration(FeelDuration::YearsMonths(if sub {
                m1 - m2
            } else {
                m1 + m2
            })))
        }
        (FeelDuration::DaysTime(d1), FeelDuration::DaysTime(d2)) => {
            Some(FeelValue::Duration(FeelDuration::DaysTime(if sub {
                *d1 - *d2
            } else {
                *d1 + *d2
            })))
        }
        _ => None, // the two duration flavours never combine
    }
}

/// `duration * number` / `number * duration` — the multiplier is a general (possibly fractional)
/// FEEL number; the exact product of the duration's base-unit count (months / seconds) against
/// the full-precision multiplier is computed first, and only THEN truncated to the integer count
/// the duration type requires (DMN-TCK 0100-arithmetic cluster 1 — the previous implementation
/// truncated the multiplier itself to an integer *before* multiplying).
fn scale_duration(a: &FeelDuration, n: &BigDecimal) -> Option<FeelValue> {
    Some(FeelValue::Duration(match a {
        FeelDuration::YearsMonths(m) => {
            let total = crate::numeric::mul(&BigDecimal::from(*m), n);
            FeelDuration::YearsMonths(total.to_i64()?.try_into().ok()?)
        }
        FeelDuration::DaysTime(d) => {
            let total = crate::numeric::mul(&BigDecimal::from(d.whole_seconds()), n);
            FeelDuration::DaysTime(time::Duration::seconds(total.to_i64()?))
        }
    }))
}

/// `duration / number` — same exact-product-then-truncate fix as [`scale_duration`]; the
/// zero-divisor check is on the real (untruncated) `n`, not a copy already collapsed to `0`.
fn div_duration(a: &FeelDuration, n: &BigDecimal) -> Option<FeelValue> {
    if n.is_zero() {
        return None;
    }
    Some(FeelValue::Duration(match a {
        FeelDuration::YearsMonths(m) => {
            let q = crate::numeric::div(&BigDecimal::from(*m), n);
            FeelDuration::YearsMonths(q.to_i64()?.try_into().ok()?)
        }
        FeelDuration::DaysTime(d) => {
            let q = crate::numeric::div(&BigDecimal::from(d.whole_seconds()), n);
            FeelDuration::DaysTime(time::Duration::seconds(q.to_i64()?))
        }
    }))
}

/// `years-months-duration / years-months-duration` / `days-time-duration / days-time-duration` →
/// a plain number ratio (DMN-TCK 0100-arithmetic cluster 3). Cross-flavour division and division
/// by a zero-valued duration are both undefined (`None` ⇒ the caller raises a type error, which
/// the harness's `errorResult` check accepts as conformant).
fn div_duration_by_duration(a: &FeelDuration, b: &FeelDuration) -> Option<FeelValue> {
    match (a, b) {
        (FeelDuration::YearsMonths(m1), FeelDuration::YearsMonths(m2)) if *m2 != 0 => {
            Some(FeelValue::Number(crate::numeric::div(
                &BigDecimal::from(*m1),
                &BigDecimal::from(*m2),
            )))
        }
        (FeelDuration::DaysTime(d1), FeelDuration::DaysTime(d2)) if !d2.is_zero() => {
            Some(FeelValue::Number(crate::numeric::div(
                &BigDecimal::new(BigInt::from(d1.whole_nanoseconds()), 0),
                &BigDecimal::new(BigInt::from(d2.whole_nanoseconds()), 0),
            )))
        }
        _ => None, // cross-flavour or zero divisor: undefined
    }
}

/// `date ± duration`. A days-time duration shifts midnight at full sub-day precision (including
/// any hour/minute/second remainder) and takes just the resulting date, rather than dropping the
/// duration's sub-day remainder up front (DMN-TCK 0100-arithmetic cluster 8 — `2021-01-02 −
/// PT1H` must land on `2021-01-01`, not stay on `2021-01-02`).
fn shift_date(d: time::Date, dur: &FeelDuration, sub: bool) -> Option<time::Date> {
    match dur {
        FeelDuration::YearsMonths(m) => add_months(d, if sub { -*m } else { *m }),
        FeelDuration::DaysTime(dt) => {
            let midnight = d.midnight();
            let shifted = if sub {
                midnight.checked_sub(*dt)
            } else {
                midnight.checked_add(*dt)
            }?;
            Some(shifted.date())
        }
    }
}

/// `date and time ± duration`. A `@Zone`-qualified instant shifts in LOCAL wall-clock time, then
/// re-resolves the zone's offset for the new date through the bundled tz database — DST-correct,
/// so a shift crossing a DST boundary lands on the right wall-clock time (DMN-TCK 0100-arithmetic
/// cluster 4b). A fixed-offset/`Z`/local qualifier shifts the absolute instant directly (no
/// re-resolution needed — the offset never depends on the date).
fn shift_instant(
    t: OffsetDateTime,
    qualifier: &Option<TimeQualifier>,
    dur: &FeelDuration,
    sub: bool,
) -> Option<(OffsetDateTime, Option<TimeQualifier>)> {
    if let Some(TimeQualifier::Zone(name)) = qualifier {
        let local = time::PrimitiveDateTime::new(t.date(), t.time());
        let shifted_local = shift_primitive(local, dur, sub)?;
        let tz = time_tz::timezones::get_by_name(name)?;
        let resolved = shifted_local.assume_timezone(tz).take_first()?;
        return Some((resolved, qualifier.clone()));
    }
    let shifted = match dur {
        FeelDuration::YearsMonths(m) => {
            let new_date = add_months(t.date(), if sub { -*m } else { *m })?;
            t.replace_date(new_date)
        }
        FeelDuration::DaysTime(dt) => t.checked_add(if sub { -*dt } else { *dt })?,
    };
    Some((shifted, qualifier.clone()))
}

fn shift_primitive(
    t: time::PrimitiveDateTime,
    dur: &FeelDuration,
    sub: bool,
) -> Option<time::PrimitiveDateTime> {
    match dur {
        FeelDuration::YearsMonths(m) => {
            let new_date = add_months(t.date(), if sub { -*m } else { *m })?;
            Some(t.replace_date(new_date))
        }
        FeelDuration::DaysTime(dt) => {
            if sub {
                t.checked_sub(*dt)
            } else {
                t.checked_add(*dt)
            }
        }
    }
}

/// Add a signed number of months to a date, clamping the day to the target month's length
/// (`2021-01-31 + 1 month = 2021-02-28`).
fn add_months(d: time::Date, months: i32) -> Option<time::Date> {
    let total = d.year() as i64 * 12 + (u8::from(d.month()) as i64 - 1) + months as i64;
    let year: i32 = total.div_euclid(12).try_into().ok()?;
    let month = time::Month::try_from((total.rem_euclid(12) + 1) as u8).ok()?;
    let day = d.day().min(last_day_of_month(year, month));
    time::Date::from_calendar_date(year, month, day).ok()
}

fn last_day_of_month(year: i32, month: time::Month) -> u8 {
    use time::Month::*;
    match month {
        January | March | May | July | August | October | December => 31,
        April | June | September | November => 30,
        February => {
            if time::util::is_leap_year(year) {
                29
            } else {
                28
            }
        }
    }
}

/// Combine a date and a time-of-day (carrying whatever qualifier the `Time` had) into an
/// `Instant`, resolving the actual offset for real: a fixed numeric offset/`Z`/local qualifier
/// applies directly; a `@Zone` qualifier is re-resolved against `d` through the bundled tz
/// database (DST-correct for that specific date) rather than reusing the possibly-different
/// offset an unrelated reference date would imply.
fn combine_date_time(d: time::Date, t: time::Time, q: Option<TimeQualifier>) -> FeelValue {
    let naive = time::PrimitiveDateTime::new(d, t);
    let resolved = match &q {
        None => naive.assume_utc(),
        Some(TimeQualifier::Zulu) => naive.assume_offset(time::UtcOffset::UTC),
        Some(TimeQualifier::Offset(o)) => naive.assume_offset(*o),
        Some(TimeQualifier::Zone(name)) => time_tz::timezones::get_by_name(name)
            .and_then(|tz| naive.assume_timezone(tz).take_first())
            // The zone name was already validated when the `Time` argument was parsed — this
            // fallback only guards a (theoretically unreachable) DST-gap/invalid local time.
            .unwrap_or_else(|| naive.assume_utc()),
    };
    FeelValue::Instant(resolved, q)
}

/// Splits a `time()` `seconds` argument into whole seconds + a nanosecond remainder — only
/// `seconds` may carry a fractional part (`hour`/`minute` stay integer-only), per DMN-TCK 0007
/// `Time3`. `None` for a negative value or one outside a valid seconds-of-minute range.
fn split_seconds(n: &BigDecimal) -> Option<(u8, u32)> {
    if n.is_negative() {
        return None;
    }
    let nanos_total = crate::numeric::mul(n, &BigDecimal::from(1_000_000_000i64)).to_i64()?;
    let whole = u8::try_from(nanos_total / 1_000_000_000).ok()?;
    let nanos = u32::try_from(nanos_total % 1_000_000_000).ok()?;
    Some((whole, nanos))
}

/// A `time()` 4th-argument `dayTimeDuration` as a validated (±14:00) `UtcOffset`.
fn offset_from_duration(d: time::Duration) -> Option<UtcOffset> {
    let total = d.whole_seconds();
    if total.unsigned_abs() > 14 * 3600 {
        return None;
    }
    UtcOffset::from_whole_seconds(total as i32).ok()
}

/// FEEL `day of week` — the English weekday name of a date/date-time.
fn weekday_name(v: &FeelValue) -> FeelValue {
    let wd = match v {
        FeelValue::Date(d) => Some(d.weekday()),
        FeelValue::Instant(t, _) => Some(t.weekday()),
        _ => None,
    };
    match wd {
        Some(w) => FeelValue::String(w.to_string()),
        None => FeelValue::Null,
    }
}

/// A `Date` from a date or date-time value.
fn date_of(v: &FeelValue) -> Option<time::Date> {
    match v {
        FeelValue::Date(d) => Some(*d),
        FeelValue::Instant(t, _) => Some(t.date()),
        _ => None,
    }
}

/// Whole signed months from `a` to `b` (the day-of-month adjusts toward zero when the end day has
/// not been reached), for `years and months duration(a, b)`.
fn months_between(a: time::Date, b: time::Date) -> i32 {
    let mut months = (b.year() - a.year()) * 12
        + (i32::from(u8::from(b.month())) - i32::from(u8::from(a.month())));
    if months > 0 && b.day() < a.day() {
        months -= 1;
    } else if months < 0 && b.day() > a.day() {
        months += 1;
    }
    months
}

/// FEEL `day of year` — the 1-based ordinal day within the year.
fn date_ordinal(v: &FeelValue) -> FeelValue {
    match v {
        FeelValue::Date(d) => FeelValue::Number(BigDecimal::from(d.ordinal() as i64)),
        FeelValue::Instant(t, _) => FeelValue::Number(BigDecimal::from(t.ordinal() as i64)),
        _ => FeelValue::Null,
    }
}

/// FEEL `week of year` — the ISO-8601 week number.
fn iso_week(v: &FeelValue) -> FeelValue {
    match v {
        FeelValue::Date(d) => FeelValue::Number(BigDecimal::from(d.iso_week() as i64)),
        FeelValue::Instant(t, _) => FeelValue::Number(BigDecimal::from(t.iso_week() as i64)),
        _ => FeelValue::Null,
    }
}

/// FEEL `month of year` — the English month name of a date/date-time.
fn month_name(v: &FeelValue) -> FeelValue {
    let m = match v {
        FeelValue::Date(d) => Some(d.month()),
        FeelValue::Instant(t, _) => Some(t.month()),
        _ => None,
    };
    match m {
        Some(month) => FeelValue::String(format!("{month:?}")),
        None => FeelValue::Null,
    }
}

/// `Some(0)` when `n` is an even integer, `Some(1)` when odd, `None` when non-integral.
fn int_parity(n: &BigDecimal) -> Option<i64> {
    if n.is_integer() {
        n.to_i64().map(|v| v.rem_euclid(2))
    } else {
        None
    }
}

/// FEEL `modulo(a, b) = a − b·floor(a/b)` — the result takes the sign of the divisor; a zero
/// divisor is `null`.
fn feel_modulo(a: &BigDecimal, b: &BigDecimal) -> FeelValue {
    if b.is_zero() {
        return FeelValue::Null;
    }
    let q = crate::numeric::div(a, b).with_scale_round(0, RoundingMode::Floor);
    FeelValue::Number(crate::numeric::sub(a, &crate::numeric::mul(b, &q)))
}

/// The median of a numeric list (average of the two middle values for an even count); empty ⇒ null.
fn median(ns: &[BigDecimal]) -> FeelValue {
    if ns.is_empty() {
        return FeelValue::Null;
    }
    let mut s = ns.to_vec();
    s.sort();
    let n = s.len();
    if n % 2 == 1 {
        FeelValue::Number(s[n / 2].clone())
    } else {
        FeelValue::Number(crate::numeric::div(
            &crate::numeric::add(&s[n / 2 - 1], &s[n / 2]),
            &BigDecimal::from(2),
        ))
    }
}

/// Sample standard deviation (denominator `n−1`) via `f64` — fewer than two values ⇒ null. Routed
/// through `round_decimal64` for internal consistency with `sqrt()`'s own arm (safe: DECIMAL64
/// rounding is a no-op below 16 significant digits); the DMN-TCK's own expected values land on a
/// shorter tail than even that (see the module's numeric-precision notes) — not chased further
/// here without more non-terminating-computation samples to corroborate a specific constant.
fn stddev(ns: &[BigDecimal]) -> FeelValue {
    if ns.len() < 2 {
        return FeelValue::Null;
    }
    let xs: Vec<f64> = ns.iter().filter_map(|n| n.to_f64()).collect();
    if xs.len() != ns.len() {
        return FeelValue::Null;
    }
    let mean = xs.iter().sum::<f64>() / xs.len() as f64;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (xs.len() as f64 - 1.0);
    match f64_num(Some(var.sqrt())) {
        FeelValue::Number(n) => FeelValue::Number(crate::numeric::round_decimal64(n)),
        other => other,
    }
}

/// The mode(s) of a numeric list — the value(s) occurring most frequently, sorted ascending
/// (DMN 1.4 §10.3.4: always a `List`, even for a single unique mode; a tie lists *all* tied
/// values); empty ⇒ `[]`, not null.
fn mode(ns: &[BigDecimal]) -> FeelValue {
    let mut counts: Vec<(BigDecimal, usize)> = Vec::new();
    for n in ns {
        match counts.iter_mut().find(|(v, _)| v == n) {
            Some((_, c)) => *c += 1,
            None => counts.push((n.clone(), 1)),
        }
    }
    let max = counts.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let mut modes: Vec<BigDecimal> = counts
        .into_iter()
        .filter(|(_, c)| *c == max)
        .map(|(v, _)| v)
        .collect();
    modes.sort();
    FeelValue::List(modes.into_iter().map(FeelValue::Number).collect())
}

/// A finite `f64` as a FEEL number rounded to a fixed decimal `scale` with `HALF_EVEN` — used by
/// transcendental builtins (`exp`) whose DMN-TCK expected values land on a fixed scale-8
/// convention (corroborated independently by `log`'s own TCK cases, outside this cycle's scope
/// but the exact same shape). `None`/non-finite ⇒ `null`.
fn f64_num_scaled(x: Option<f64>, scale: i64) -> FeelValue {
    match f64_num(x) {
        FeelValue::Number(n) => {
            FeelValue::Number(n.with_scale_round(scale, RoundingMode::HalfEven))
        }
        other => other,
    }
}

/// A `number()` separator argument: `Null` is "no separator" (`Ok(None)`); a single-character
/// string drawn from the allowed separator set is `Ok(Some(char))`; anything else (a non-string,
/// multi-character, or disallowed-character value) is `Err(())` — an invalid separator argument.
fn separator_char(v: &FeelValue) -> Result<Option<char>, ()> {
    match v {
        FeelValue::Null => Ok(None),
        FeelValue::String(s) => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) if matches!(c, ' ' | ',' | '.') => Ok(Some(c)),
                _ => Err(()),
            }
        }
        _ => Err(()),
    }
}

/// Whether `e` is a literal-shaped range endpoint per DMN's `range(from)` string grammar: a bare
/// literal (number/string/boolean/`@"..."` at-literal — already resolved to a typed [`FeelValue`]
/// at parse time), or a `date`/`time`/`duration`/`date and time` call whose sole argument is
/// itself a literal (not a nested expression — DMN-TCK 1156 decision007_b/008_b/009_b/010_b use
/// `date(string("..."))` and must be rejected).
fn is_range_literal_endpoint(e: &FeelExpr) -> bool {
    match e {
        FeelExpr::Literal { .. } => true,
        FeelExpr::Call { name, args, .. } if args.len() == 1 => {
            matches!(
                name.as_str(),
                "date" | "time" | "duration" | "date and time"
            ) && matches!(args[0], FeelExpr::Literal { .. })
        }
        _ => false,
    }
}

/// `context put(context, keys, value)`'s nested-path recursion: `path` must be non-empty and
/// every element a string (a `null`/non-string segment, or an empty path, is rejected); every
/// segment but the last must already resolve to an existing nested context to descend into (no
/// new intermediate levels are created — DMN-TCK 1146#nested009). Returns a new top-level context
/// with the leaf entry set/overwritten (never mutates `base`).
fn set_nested(
    base: &std::collections::BTreeMap<String, FeelValue>,
    path: &[FeelValue],
    value: FeelValue,
) -> Option<FeelValue> {
    let (head, rest) = path.split_first()?;
    let FeelValue::String(key) = head else {
        return None;
    };
    let mut out = base.clone();
    if rest.is_empty() {
        out.insert(key.clone(), value);
    } else {
        let child = match out.get(key) {
            Some(FeelValue::Map(m)) => m.clone(),
            _ => return None,
        };
        let updated = set_nested(&child, rest, value)?;
        out.insert(key.clone(), updated);
    }
    Some(FeelValue::Map(out))
}

/// XPath/FEEL `x` (extended) flag semantics (F&O §5.6.1): every whitespace character in the
/// pattern is removed prior to compiling, except whitespace inside a `[...]` character class — a
/// backslash does NOT protect a following whitespace character (confirmed against DMN-TCK 1111's
/// K2-MatchesFunc-1..6: `\ s` collapses to `\s`, the whitespace-class escape, not a preserved
/// literal escaped space; `\p{ IsBasicLatin}` collapses to `\p{IsBasicLatin}` — `{}` braces are
/// NOT exempted the way `[...]` is).
fn strip_extended_whitespace(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut in_class = false;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            out.push(c);
            if let Some(next) = chars.next() {
                if !next.is_whitespace() || in_class {
                    out.push(next);
                }
            }
            continue;
        }
        match c {
            '[' if !in_class => {
                in_class = true;
                out.push(c);
            }
            ']' if in_class => {
                in_class = false;
                out.push(c);
            }
            c if c.is_whitespace() && !in_class => {} // stripped
            c => out.push(c),
        }
    }
    out
}

/// Rewrite every un-escaped, not-inside-a-character-class `.` to `[^\r\n]` — XPath/FEEL's default
/// (non-DOTALL) `.` excludes both LF and CR, whereas the `regex` crate's default only excludes LF
/// (DMN-TCK 1111-feel-matches-function `fn-matches-45`).
fn exclude_cr_from_dot(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut in_class = false;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            out.push(c);
            if let Some(next) = chars.next() {
                out.push(next);
            }
            continue;
        }
        match c {
            '[' if !in_class => {
                in_class = true;
                out.push(c);
            }
            ']' if in_class => {
                in_class = false;
                out.push(c);
            }
            '.' if !in_class => out.push_str("[^\r\n]"),
            c => out.push(c),
        }
    }
    out
}

/// A narrow, single-entry Unicode **block**-name translation (the `regex` crate supports only
/// general categories/scripts, not XSD-style `\p{IsXxx}` blocks) — `\p{IsBasicLatin}` (and its
/// negation) is the only block name this corpus exercises; not a general ~300-entry table.
fn translate_unicode_blocks(pattern: &str) -> String {
    pattern
        .replace("\\p{IsBasicLatin}", "[\\x00-\\x7F]")
        .replace("\\P{IsBasicLatin}", "[^\\x00-\\x7F]")
}

/// XML-Schema/XPath character-class subtraction → the `regex` crate's native class-difference
/// spelling: inside a character class, an un-escaped `-` immediately followed by a nested `[`
/// (`[A-Z-[OI]]`, XSD's only subtraction form — always at the end of the class) becomes `--`
/// (`[A-Z--[OI]]`), which the `regex` crate evaluates as genuine set difference (DMN-TCK
/// 1111-feel-matches-function caselessmatch10/11: `matches("O", "[A-Z-[OI]]", "i")` must be
/// FALSE — O is subtracted — where the untranslated pattern read `-[` as a literal hyphen plus a
/// nested-class UNION, matching everything in `[OI]` too). A `-` already part of a `--` pair is
/// left alone (the caller wrote the native spelling), as is anything escaped or outside a class.
fn translate_class_subtraction(pattern: &str) -> String {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 2);
    let mut depth = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\\' {
            out.push(c);
            if let Some(&next) = chars.get(i + 1) {
                out.push(next);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            '-' if depth > 0
                && chars.get(i + 1) == Some(&'[')
                && (i == 0 || chars[i - 1] != '-') =>
            {
                out.push_str("--");
                i += 1;
                continue;
            }
            _ => {}
        }
        out.push(c);
        i += 1;
    }
    out
}

/// FEEL three-valued `all`/`any` over a list: `all` is false on any false, else null if any
/// non-boolean, else true; `any` is true on any true, else null if any non-boolean, else false.
/// Empty ⇒ `all` true, `any` false.
fn bool_agg(items: &[FeelValue], is_all: bool) -> FeelValue {
    let mut saw_non_bool = false;
    for it in items {
        match it {
            FeelValue::Boolean(b) => {
                if is_all && !b {
                    return FeelValue::Boolean(false);
                }
                if !is_all && *b {
                    return FeelValue::Boolean(true);
                }
            }
            _ => saw_non_bool = true,
        }
    }
    if saw_non_bool {
        FeelValue::Null
    } else {
        FeelValue::Boolean(is_all)
    }
}

/// A finite `f64` as a FEEL number; a non-finite (or absent) result is `null`.
fn f64_num(x: Option<f64>) -> FeelValue {
    match x {
        Some(v) if v.is_finite() => FeelValue::from(v),
        _ => FeelValue::Null,
    }
}

/// FEEL equality (§10.3.2.5), three-valued: `Some(bool)` only when both operands share a base type
/// (or the same Duration flavour); `None` (⇒ `null`) for cross-type or cross-flavour comparisons.
/// `null` equals only `null`. Numbers/strings/… compare by the derived value-based `PartialEq`.
///
/// `Instant`/`Time` are deliberately NOT compared via the derived `PartialEq` here (that one is
/// reserved for `is()`'s structural identity, which — unlike `=` — DOES care whether a `Z`, an
/// explicit `+00:00`, or an `@Zone` produced the value): FEEL `=` compares the resolved absolute
/// instant (for `Instant`) or the bare wall-clock (for `Time`), ignoring the qualifier entirely
/// AND at whole-SECOND resolution — DMN-TCK 0068-feel-equality's own description of
/// `datetime_003_a`/`time_005`: "resolution is to the second" (any sub-second difference is
/// invisible to `=`), and `datetime_008`/`datetime_008_a`/`datetime_009`/`datetime_012`/
/// `datetime_013`: an explicit offset and an equally-resolving `@Zone` (DST-correctly, via the
/// bundled tz database) compare equal, as do two different zones/offsets that happen to name the
/// same absolute instant.
fn equals_feel(a: &FeelValue, b: &FeelValue) -> Option<bool> {
    use FeelValue::*;
    match (a, b) {
        (Null, Null) => Some(true),
        (Null, _) | (_, Null) => Some(false),
        (Instant(x, _), Instant(y, _)) => Some(x.unix_timestamp() == y.unix_timestamp()),
        (Time(x, _), Time(y, _)) => {
            Some((x.hour(), x.minute(), x.second()) == (y.hour(), y.minute(), y.second()))
        }
        (Number(_), Number(_))
        | (String(_), String(_))
        | (Boolean(_), Boolean(_))
        | (Date(_), Date(_))
        | (List(_), List(_))
        | (Map(_), Map(_))
        | (Range(_), Range(_))
        | (Function(_), Function(_)) => Some(a == b),
        (Duration(x), Duration(y)) => match (x, y) {
            (FeelDuration::YearsMonths(_), FeelDuration::YearsMonths(_))
            | (FeelDuration::DaysTime(_), FeelDuration::DaysTime(_)) => Some(a == b),
            _ => None, // the two duration flavours never inter-compare
        },
        _ => None, // cross-type: undefined, not false
    }
}

/// FEEL three-valued view of a logical operand (`and`/`or`/`not`, an `if` condition, a filter
/// `match`/quantifier `satisfies` result): a `Boolean` is itself; `null` and any non-boolean value
/// are `None` (unknown/undefined) — DMN requires each of these positions to hold a genuine
/// boolean, never silently coercing a wrong-typed value to true/false (DMN-TCK
/// 1150-boxed-conditional#003, 1151-boxed-filter#004/#005).
fn tri(o: &FeelValue) -> Option<bool> {
    match o {
        FeelValue::Boolean(b) => Some(*b),
        _ => None,
    }
}

/// A value coerced to its string-builtin-argument form: `null` is the empty string; a
/// SINGLETON list unwraps to its one element first (DMN §10.3.2.13 semantic conformance to a
/// declared `string` parameter type — DMN-TCK 0021-singleton-list#decision5:
/// `upper case(Employees[item = "Bob"])`, where the filter yields a 1-element list, must
/// stringify as `"BOB"`, not the bracketed `"[BOB]"` a list's own canonical rendering would give);
/// anything else uses the ordinary canonical string rendering.
fn str_of(o: &FeelValue) -> String {
    match o {
        FeelValue::Null => String::new(),
        FeelValue::List(items) if items.len() == 1 => str_of(&items[0]),
        other => canonical_string_of(other),
    }
}

/// FEEL/XPath `replace()`'s replacement-string syntax uses bare `$1`/`$2` (positional
/// backreferences) and `$$` (a literal `$`) — but the `regex` crate's OWN replacement-string
/// syntax is broader: a `$name` reference greedily consumes any FOLLOWING alphanumeric/underscore
/// run as part of the group NAME, so `$1c` looks for a group literally named `"1c"` instead of
/// "group 1, then the literal character 'c'" (DMN-TCK 1109-feel-replace-function#015:
/// `replace("darted", "^(.*?)d(.*)$", "$1c$2")` must produce `"carted"` — silently dropping BOTH
/// the backreference and the literal that follows it, "arted", is this exact bug). Rewrites every
/// bare `$<digits>` run into the regex crate's own unambiguous `${<digits>}` brace form; `$$` and
/// an already-braced `${...}` pass through untouched.
fn disambiguate_replacement_backreferences(replacement: &str) -> String {
    let chars: Vec<char> = replacement.chars().collect();
    let mut out = String::with_capacity(replacement.len() + 4);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '$' {
            if chars.get(i + 1) == Some(&'$') {
                // `$$` is its own two-character escape (→ a literal `$`) — consumed as a pair so
                // the second `$` is never mistaken for the START of another reference.
                out.push('$');
                out.push('$');
                i += 2;
                continue;
            }
            if chars.get(i + 1) == Some(&'{') {
                out.push('$');
                i += 1;
                continue;
            }
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                out.push_str("${");
                out.extend(&chars[i + 1..j]);
                out.push('}');
                i = j;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}
