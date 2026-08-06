//! The FEEL value model — the closed set of untyped runtime values the evaluator passes around.
//!
//! The evaluator works with `null` / boolean / string / number (normalised to `BigDecimal`) /
//! instant / date / map / list; this enum makes that closed set explicit.

use std::collections::BTreeMap;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use time::{Date, OffsetDateTime, Time, UtcOffset};

use crate::ast::{CompareOp, FeelExpr};

/// A FEEL function value — an anonymous `function(params) body` literal. The body is the parsed
/// expression; parameters bind at invocation, layered over the scope captured at the literal's
/// definition site (its closure — DMN-TCK 0092-feel-lambda#decision_007_1/007_2: a lambda body
/// may reference an outer, non-parameter variable, e.g. a DMN input-data value in scope when the
/// function literal itself was evaluated).
///
/// `param_shapes`/`return_shape` are DMN §10.3.2.13 "semantic conformance to typeRef" shapes —
/// `FeelTypeShape::Any` (the default an ordinary `function(...) ...` literal gets, see
/// `evaluator.rs`'s `FunctionDef` arm) never gates or transforms anything, so this field is a
/// no-op for every function value that isn't purpose-built with real shapes. A caller that DOES
/// build one with real shapes (e.g. `sutra-dmn`'s DRG, wiring a BKM's formal-parameter typeRefs)
/// gets the DMN invocation contract for free from [`crate::evaluator`]'s call machinery: an
/// argument that can't be coerced to its declared shape makes the WHOLE call evaluate to `null`
/// (DMN's "the BKM is never invoked" semantics — DMN-TCK 0082-feel-coercion#decision_bkm_002),
/// and the body's result is coerced against `return_shape` before it's handed back.
#[derive(Debug, Clone, PartialEq)]
pub struct FeelFunction {
    pub params: Vec<String>,
    pub body: Box<FeelExpr>,
    pub captured: FeelContext,
    pub param_shapes: Vec<FeelTypeShape>,
    pub return_shape: FeelTypeShape,
    /// `Some` iff this value came from an `external` function definition (FEEL rule 55 /
    /// DMN `kind="Java"/"PMML"`) — the java/pmml binding classified at DEFINITION time (defining
    /// one never errors). The evaluator's call machinery rejects any invocation of such a value
    /// with [`crate::codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED`], before arity/typeRef gating (the
    /// rejection fires regardless of the arguments). Boxed so the ordinary (non-external)
    /// function value doesn't grow.
    pub external: Option<Box<ExternalFunctionBinding>>,
}

/// The DMN §10.3.2.13.3 binding an `external` function definition's body reduces to — recorded so
/// the invocation-time rejection can name exactly what the model asked this engine to execute.
/// External-function EXECUTION is an OPTIONAL DMN feature: a conformant engine that doesn't
/// support it (this one — no host-language runtime, no reflective/PMML dispatch) must reject
/// invocation with an
/// error, which is what [`crate::evaluator`]'s call machinery does for any function value
/// carrying one of these.
#[derive(Debug, Clone, PartialEq)]
pub enum ExternalFunctionBinding {
    /// `{java: {class: <string>, "method signature": <string>}}`.
    Java {
        class: String,
        method_signature: String,
    },
    /// `{pmml: {document: <string>[, model: <string>]}}`.
    Pmml {
        document: String,
        model: Option<String>,
    },
    /// The body didn't reduce to either shape. Recorded, not raised — the definition still
    /// evaluates to a function value; the invocation-time rejection reports this instead.
    Malformed { detail: String },
}

impl ExternalFunctionBinding {
    /// Classify an `external` function definition's evaluated body value against the two
    /// §10.3.2.13.3 shapes. Never fails — an off-shape body is [`Self::Malformed`], carrying
    /// enough detail for the eventual invocation-time diagnostic.
    pub fn classify_body_value(value: &FeelValue) -> ExternalFunctionBinding {
        let FeelValue::Map(entries) = value else {
            return ExternalFunctionBinding::Malformed {
                detail: format!(
                    "body evaluates to {}, expected a context of the form \
                     {{java: {{class, method signature}}}} or {{pmml: {{document[, model]}}}}",
                    value.type_name()
                ),
            };
        };
        if let Some(java) = entries.get("java") {
            let FeelValue::Map(java) = java else {
                return ExternalFunctionBinding::Malformed {
                    detail: "'java' entry is not a context".to_string(),
                };
            };
            match (java.get("class"), java.get("method signature")) {
                (Some(FeelValue::String(class)), Some(FeelValue::String(sig))) => {
                    ExternalFunctionBinding::Java {
                        class: class.clone(),
                        method_signature: sig.clone(),
                    }
                }
                _ => ExternalFunctionBinding::Malformed {
                    detail: "java binding requires string-valued 'class' and 'method signature' \
                             entries"
                        .to_string(),
                },
            }
        } else if let Some(pmml) = entries.get("pmml") {
            let FeelValue::Map(pmml) = pmml else {
                return ExternalFunctionBinding::Malformed {
                    detail: "'pmml' entry is not a context".to_string(),
                };
            };
            match pmml.get("document") {
                Some(FeelValue::String(document)) => ExternalFunctionBinding::Pmml {
                    document: document.clone(),
                    model: match pmml.get("model") {
                        Some(FeelValue::String(m)) => Some(m.clone()),
                        _ => None,
                    },
                },
                _ => ExternalFunctionBinding::Malformed {
                    detail: "pmml binding requires a string-valued 'document' entry".to_string(),
                },
            }
        } else {
            ExternalFunctionBinding::Malformed {
                detail: "body context has neither a 'java' nor a 'pmml' entry".to_string(),
            }
        }
    }

    /// The invocation-time rejection message. Never contains the substring "SYNTAX" (see
    /// [`crate::codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED`]'s doc comment for why that matters).
    pub fn rejection_message(&self) -> String {
        match self {
            ExternalFunctionBinding::Java {
                class,
                method_signature,
            } => format!(
                "external function execution is not supported by this engine \
                 (java class '{class}', method '{method_signature}')"
            ),
            ExternalFunctionBinding::Pmml {
                document,
                model: Some(model),
            } => format!(
                "external function execution is not supported by this engine \
                 (pmml document '{document}', model '{model}')"
            ),
            ExternalFunctionBinding::Pmml {
                document,
                model: None,
            } => format!(
                "external function execution is not supported by this engine \
                 (pmml document '{document}')"
            ),
            ExternalFunctionBinding::Malformed { detail } => format!(
                "external function execution is not supported by this engine, and its \
                 definition is not a valid java/pmml binding: {detail}"
            ),
        }
    }
}

/// A native ("invocable") FEEL callable value — an escape hatch for a caller (e.g. `sutra-dmn`'s
/// DRG) to bind an ordinary Rust closure as a FEEL-callable value, for the case where the
/// callable's behavior can't be reduced to a plain FEEL AST body (DMN's decision-service
/// semantics: invoking one re-runs part of the model's Decision Requirements Graph with
/// overridden bindings, which isn't expressible as a FEEL expression — DMN-TCK 0085's own
/// indirect `decisionService_004()`/`decisionService_006("bar")`/… calls, and 0092-feel-lambda's
/// `#013`, which passes a decision service BARE (as a value, not immediately called) into a BKM
/// that invokes it under a local parameter name).
///
/// This crate has no idea what the closure DOES — it only knows how to CALL it, with the exact
/// same formal-parameter arity/typeRef/named-argument gating an ordinary [`FeelFunction`] gets:
/// [`crate::evaluator`]'s call machinery gates each argument against `param_shapes` and coerces
/// the result against `return_shape`, so a caller-built `Invocable` gets DMN's "never invoked on
/// a bad call" semantics for free, exactly like a purpose-built `FeelFunction`. UNLIKE a
/// `FeelFunction` (whose FEEL-AST body tolerates a missing/extra binding — an unbound parameter
/// name simply doesn't resolve, usually degrading to `null` inside the body), an `Invocable` has
/// no body to fall through to, so the ARITY itself is checked strictly: a positional call must
/// supply exactly `params.len()` arguments, and a named call's argument names must exactly match
/// the declared parameter set (no missing, no extra, no unrecognized name) — see
/// `crate::evaluator`'s `invoke_invocable`.
#[derive(Clone)]
pub struct Invocable {
    /// A stable identity for `Debug`/`PartialEq`/[`FeelContext`]-map-equality purposes — two
    /// `Invocable`s are equal iff their `id` matches (the closure itself carries no meaningful
    /// equality). Set by the caller to something meaningful in its own domain (e.g. `sutra-dmn`
    /// uses the decision service's own name).
    pub id: String,
    /// Declared formal-parameter names, in POSITIONAL binding order (a caller-domain concern —
    /// `sutra-dmn`'s decision services order `inputData` before `inputDecision`, each in their
    /// own declared order, per DMN-TCK 0085-decision-services#011's own worked example).
    pub params: Vec<String>,
    pub param_shapes: Vec<FeelTypeShape>,
    pub return_shape: FeelTypeShape,
    /// The native call, given the already-arity-checked/shape-coerced, POSITIONALLY-ordered
    /// argument values (one per `params`/`param_shapes` entry). `Send + Sync` so a `FeelValue`
    /// carrying one stays freely shareable across threads (mirrors every other `Arc`-free variant
    /// here, which are all naturally `Send + Sync` already).
    pub call: Arc<InvocableFn>,
}

/// The `Invocable::call` closure shape, named so the `Arc<dyn Fn(...) -> ... + Send + Sync>`
/// spelling only has to be written out once.
pub type InvocableFn = dyn Fn(&[FeelValue]) -> FeelValue + Send + Sync;

impl std::fmt::Debug for Invocable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invocable").field("id", &self.id).finish()
    }
}

impl PartialEq for Invocable {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// A structural FEEL value shape — DMN §10.3.2.13 "semantic conformance to typeRef" reduced to
/// FEEL's own value model, carrying no DMN itemDefinition specifics (`sutra-dmn`'s DRG builds
/// these from its own `<itemDefinition>` table; this crate only knows how to coerce a
/// [`FeelValue`] against one).
#[derive(Debug, Clone, PartialEq)]
pub enum FeelTypeShape {
    /// No declared type (or one this engine doesn't model) — always conforms, unchanged.
    Any,
    /// A named base FEEL type: `"string"`, `"number"`, `"boolean"`, `"date"`, `"time"`,
    /// `"dateTime"`/`"date and time"`, `"duration"`, `"context"`. An unrecognized name conforms
    /// unconditionally (treated as `Any`) — forward-compatible with typeRefs this engine doesn't
    /// specifically model (e.g. a DMN `functionItem` type used for a lambda-valued parameter).
    Base(String),
    /// A homogeneous list of the given element shape.
    Collection(Box<FeelTypeShape>),
    /// A structural record: every named component must be present in the value and itself
    /// conform; components in the value beyond the declared set are ignored, not stripped
    /// (DMN-TCK 0082-feel-coercion#decision_context_02 — a "super type" subset still returns the
    /// full original value).
    Record(Vec<(String, FeelTypeShape)>),
    /// A FEEL `range<T>` generic type expression — a range/interval value whose endpoints
    /// conform to the given element shape. Only consulted by `instance of` (parsed from a FEEL
    /// type expression, e.g. `x instance of range<number>`); no DMN `<itemDefinition>` kind ever
    /// produces this directly, so [`coerce_to_shape`] treats it structurally the same way (a
    /// non-`Range` value never conforms, coercion is a no-op otherwise) purely for exhaustiveness.
    Range(Box<FeelTypeShape>),
}

/// A custom-type-name resolver for `instance of` (DMN's own `<itemDefinition>` registry, fed in
/// by `sutra-dmn`'s DRG — see `sutra-feel::expressions::eval_with_type_resolver`). Named so the
/// `Option<&dyn Fn(...) -> ...>` shape only has to be spelled out once.
pub type TypeResolver<'a> = dyn Fn(&str) -> Option<FeelTypeShape> + 'a;

/// DMN §10.3.2.13 "semantic conformance to typeRef": `None` when `value` cannot be made to
/// conform to `shape`; `Some` carries the value, singleton-list-coerced in whichever direction
/// (scalar⇄1-element-list) `shape` requires. `null` always conforms as-is — it is never wrapped
/// into, or unwrapped out of, a list (DMN-TCK 0082-feel-coercion#decision_008/decision_bkm_004_b).
///
/// The scalar-target branches (`Base`/`Record`) unwrap a singleton list AT MOST ONCE — `[[10]]`
/// against `number` is `null`, not `10` (DMN-TCK 0082-feel-coercion#invoke_005's own worked
/// example: substituting `[10]` into a BKM returning `[arg]` gives `[[10]]`, and THAT does not
/// conform to a scalar `number`, full stop — it is not further unwrapped to find a match several
/// list-levels down).
pub fn coerce_to_shape(value: &FeelValue, shape: &FeelTypeShape) -> Option<FeelValue> {
    if value.is_null() || matches!(shape, FeelTypeShape::Any) {
        return Some(value.clone());
    }
    match shape {
        FeelTypeShape::Any => unreachable!("handled above"),
        FeelTypeShape::Base(name) => {
            if base_shape_matches(value, name) {
                return Some(value.clone());
            }
            if let FeelValue::List(items) = value {
                if items.len() == 1 && base_shape_matches(&items[0], name) {
                    return Some(items[0].clone());
                }
            }
            None
        }
        FeelTypeShape::Collection(elem) => match value {
            FeelValue::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    out.push(coerce_to_shape(it, elem)?);
                }
                Some(FeelValue::List(out))
            }
            other => coerce_to_shape(other, elem).map(|v| FeelValue::List(vec![v])),
        },
        FeelTypeShape::Record(components) => {
            let target = match value {
                FeelValue::List(items) if items.len() == 1 => &items[0],
                other => other,
            };
            let FeelValue::Map(m) = target else {
                return None;
            };
            for (name, comp_shape) in components {
                coerce_to_shape(m.get(name)?, comp_shape)?;
            }
            Some(target.clone())
        }
        // No `<itemDefinition>` kind ever resolves to this shape (see its own doc comment) — a
        // `Range` value passes through unchanged when it's already one, exactly like `Base`'s own
        // no-op-on-match posture; anything else doesn't conform.
        FeelTypeShape::Range(_) => matches!(value, FeelValue::Range(_)).then(|| value.clone()),
    }
}

fn base_shape_matches(value: &FeelValue, name: &str) -> bool {
    match name {
        "string" => matches!(value, FeelValue::String(_)),
        "number" => matches!(value, FeelValue::Number(_)),
        "boolean" => matches!(value, FeelValue::Boolean(_)),
        "date" => matches!(value, FeelValue::Date(_)),
        "time" => matches!(value, FeelValue::Time(..)),
        "dateTime" | "date and time" => matches!(value, FeelValue::Instant(..)),
        "duration" | "dayTimeDuration" | "yearMonthDuration" => {
            matches!(value, FeelValue::Duration(_))
        }
        "context" => matches!(value, FeelValue::Map(_)),
        // An unrecognized/unmodeled base type name (e.g. a DMN `functionItem` typeRef used on a
        // lambda-valued parameter) never gates — same posture as `FeelTypeShape::Any`.
        _ => true,
    }
}

/// A FEEL range/interval value `[a..b]` — endpoints of any comparable type (number, string, date,
/// time, duration) with independent open/closed bounds (`[a..b)`, `(a..b]`, …).
///
/// `comparison_op` is `None` for an ordinary interval literal; it is `Some(op)` when this range
/// was built from the comparison-operator-as-value syntax (`(< 10)`, `(>= 5)`, …) — a distinct
/// FEEL construct (DMN 1.4 §10.3.2.11) that must NOT structurally equal a literal interval with
/// the same numeric bound (`(< 10)` != `(null..10)`, DMN-TCK 0068-feel-equality `range_006`).
/// Because this field participates in the derived `PartialEq`, two ranges only compare equal
/// when they agree on it too — exactly the discrimination the TCK requires.
#[derive(Debug, Clone, PartialEq)]
pub struct FeelRange {
    pub start: Box<FeelValue>,
    pub end: Box<FeelValue>,
    pub start_inclusive: bool,
    pub end_inclusive: bool,
    pub comparison_op: Option<CompareOp>,
}

/// A FEEL duration. The two flavours are distinct types that never inter-compare or inter-convert
/// (§10.3.2.3): a years-and-months duration is an integral, signed number of months; a
/// days-and-time duration is a signed span with sub-day precision.
#[derive(Debug, Clone, PartialEq)]
pub enum FeelDuration {
    YearsMonths(i32),
    DaysTime(time::Duration),
}

/// What (if anything) qualified a parsed `time`/`date and time` literal: no qualifier at all (a
/// "local"/floating value with no zone information), an explicit numeric UTC offset (including
/// the literal `Z` spelling of `+00:00`), or an IANA zone id (`@Region/City`). A `Zone` name is
/// resolved through the bundled tz database (the `time-tz` crate's `db` feature — real IANA data,
/// DST-correct) at parse time to build the value's actual offset/instant; the name itself is kept
/// verbatim here purely for `string()` round-tripping (never re-derived from the resolved offset).
///
/// Participates in FEEL structural identity (the `is()` builtin) exactly as written: a bare `Z`
/// and an explicit `+00:00` are the ONLY two spellings ever treated as identical (both are, by
/// definition, explicit UTC — DMN-TCK 0103-feel-is-function `time_004`/`datetime_015`); a `@Zone`
/// name is never identical to an `Offset`/`Zulu` even when its resolved numeric offset happens to
/// coincide (`time_006`/`time_007`, `datetime_016`/`datetime_017`). FEEL value-equality (`=`/`!=`)
/// does NOT consult this type at all — see [`crate::evaluator`]'s dedicated instant-/wall-clock-
/// based comparison (DMN-TCK 0068-feel-equality `datetime_008`/`datetime_009`/`datetime_012`).
#[derive(Debug, Clone)]
pub enum TimeQualifier {
    Zulu,
    Offset(UtcOffset),
    Zone(String),
}

impl PartialEq for TimeQualifier {
    fn eq(&self, other: &Self) -> bool {
        use TimeQualifier::{Offset, Zone, Zulu};
        match (self, other) {
            (Zulu, Zulu) => true,
            (Offset(a), Offset(b)) => a == b,
            (Zulu, Offset(o)) | (Offset(o), Zulu) => o.is_utc(),
            (Zone(a), Zone(b)) => a == b,
            _ => false,
        }
    }
}

/// Evaluation context — a string-keyed map of values, keyed by top-level path segment.
///
/// `BTreeMap` (not a linked/insertion-ordered map): no FEEL semantics depend on context key
/// order, and `BTreeMap` gives deterministic iteration plus order-independent equality —
/// the same observable contract as order-independent map equality.
pub type FeelContext = BTreeMap<String, FeelValue>;

/// A FEEL runtime value.
///
/// Equality follows FEEL value-equality: numbers compare by value (the
/// `bigdecimal` crate's `PartialEq` is value-based, so `1.0 == 1.00`), instants compare as
/// absolute points in time, maps/lists compare structurally.
#[derive(Debug, Clone, PartialEq)]
pub enum FeelValue {
    Null,
    Boolean(bool),
    Number(BigDecimal),
    String(String),
    /// An absolute instant (FEEL `date and time`) — the `OffsetDateTime` always carries the
    /// fully-resolved numeric offset (DST-correct for a `@Zone` qualifier), so ordering and
    /// FEEL `=`/`!=` equality (which the evaluator computes on the absolute instant, ignoring
    /// the qualifier — see [`crate::evaluator`]) are both correct regardless of which zone/offset
    /// spelling produced the value. The qualifier is `None` for a "local"/floating value (no
    /// offset/zone in the source at all — round-trips with no suffix at all).
    Instant(OffsetDateTime, Option<TimeQualifier>),
    /// A calendar date (`today()` builtin).
    Date(Date),
    /// A FEEL `time` value (time of day). Unlike `Instant`, the wall-clock `Time` is never
    /// adjusted by the qualifier — a `time` literal has no date to combine an offset against, so
    /// FEEL keeps the written hour/minute/second exactly as given and carries the qualifier
    /// purely as display/accessor metadata (`string()`, the `time offset` property).
    Time(Time, Option<TimeQualifier>),
    /// A FEEL `duration` (years-and-months or days-and-time).
    Duration(FeelDuration),
    List(Vec<FeelValue>),
    Map(BTreeMap<String, FeelValue>),
    /// A FEEL function value (an anonymous `function(…) …` literal).
    Function(FeelFunction),
    /// A FEEL range/interval value (`[a..b]`, `(a..b)`, …).
    Range(FeelRange),
    /// A native-callable FEEL value bound by the caller (e.g. `sutra-dmn`'s DRG, for indirect
    /// decision-service invocation) — see [`Invocable`]'s own doc comment.
    Invocable(Invocable),
}

impl FeelValue {
    pub fn is_null(&self) -> bool {
        matches!(self, FeelValue::Null)
    }

    /// Simple type-name string used in diagnostic messages — the type names the evaluator
    /// actually surfaces.
    pub fn type_name(&self) -> &'static str {
        match self {
            FeelValue::Null => "null",
            FeelValue::Boolean(_) => "Boolean",
            FeelValue::Number(_) => "BigDecimal",
            FeelValue::String(_) => "String",
            FeelValue::Instant(..) => "Instant",
            FeelValue::Date(_) => "LocalDate",
            FeelValue::Time(..) => "Time",
            FeelValue::Duration(_) => "Duration",
            FeelValue::Function(_) => "function",
            FeelValue::Range(_) => "range",
            FeelValue::Invocable(_) => "function",
            // The reference implementation would print the concrete collection type (e.g.
            // `HashMap`); this uses the interface-level name — message-text divergence only.
            FeelValue::List(_) => "List",
            FeelValue::Map(_) => "Map",
        }
    }

    /// Parse a numeric literal — panics on invalid input (test/fixture convenience).
    pub fn num(s: &str) -> FeelValue {
        FeelValue::Number(s.parse().expect("invalid decimal literal"))
    }
}

impl From<bool> for FeelValue {
    fn from(v: bool) -> Self {
        FeelValue::Boolean(v)
    }
}

impl From<&str> for FeelValue {
    fn from(v: &str) -> Self {
        FeelValue::String(v.to_string())
    }
}

impl From<String> for FeelValue {
    fn from(v: String) -> Self {
        FeelValue::String(v)
    }
}

impl From<BigDecimal> for FeelValue {
    fn from(v: BigDecimal) -> Self {
        FeelValue::Number(v)
    }
}

/// Integer-like inputs keep scale 0 — integral types build a scale-0 `BigDecimal` directly, so
/// integer-equality assertions hold ("100", not "100.0").
impl From<i64> for FeelValue {
    fn from(v: i64) -> Self {
        FeelValue::Number(BigDecimal::from(v))
    }
}

impl From<i32> for FeelValue {
    fn from(v: i32) -> Self {
        FeelValue::Number(BigDecimal::from(v))
    }
}

/// Builds the `BigDecimal` from the shortest decimal string that round-trips the double, with
/// integral doubles keeping one fractional digit (`100.0` → scale 1). Panics on non-finite
/// input.
impl From<f64> for FeelValue {
    fn from(v: f64) -> Self {
        assert!(
            v.is_finite(),
            "cannot convert non-finite double to a number"
        );
        let mut s = format!("{v}");
        // Rust prints integral floats without a fraction ("100"); the canonical decimal form
        // keeps "100.0" — append it so the resulting scale matches.
        if !s.contains('.') && !s.contains('e') && !s.contains('E') {
            s.push_str(".0");
        }
        FeelValue::Number(s.parse().expect("finite double always parses"))
    }
}

/// Canonical string rendering of a FEEL value (`null` → `"null"`).
///
/// Documented divergences (message-text only, no test depends on them):
/// - `Number`: the `bigdecimal` crate always prints plain notation, while the reference
///   implementation's canonical decimal string may switch to scientific notation for extreme
///   exponents.
/// - `Instant`: formatted as RFC 3339 (the reference implementation renders ISO-8601 UTC —
///   same shape).
/// - `List`/`Map`: `[a, b]` / `{k=v}` shapes mirroring the reference collection/map forms.
pub fn canonical_string_of(v: &FeelValue) -> String {
    match v {
        FeelValue::Null => "null".to_string(),
        FeelValue::Boolean(b) => b.to_string(),
        FeelValue::Number(n) => n.to_string(),
        FeelValue::String(s) => s.clone(),
        FeelValue::Instant(t, q) => format_instant(t, q),
        FeelValue::Date(d) => d.to_string(),
        FeelValue::Time(t, q) => crate::temporal::format_time(t, q),
        FeelValue::Duration(d) => crate::temporal::format_duration(d),
        FeelValue::Function(f) => format!("function({})", f.params.join(", ")),
        FeelValue::Invocable(inv) => format!("function({})", inv.params.join(", ")),
        FeelValue::Range(r) => format!(
            "{}{}..{}{}",
            if r.start_inclusive { '[' } else { '(' },
            canonical_string_of(&r.start),
            canonical_string_of(&r.end),
            if r.end_inclusive { ']' } else { ')' },
        ),
        FeelValue::List(xs) => {
            let inner: Vec<String> = xs.iter().map(canonical_string_of).collect();
            format!("[{}]", inner.join(", "))
        }
        FeelValue::Map(m) => {
            let inner: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{k}={}", canonical_string_of(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
    }
}

/// ISO-8601 rendering of an instant, hand-formatted rather than delegating to `time`'s RFC-3339
/// formatter: RFC 3339 mandates a non-negative 4-digit year, so it hard-rejects any BCE-extended
/// (negative) or 5/6-digit year outright — this engine's `large-dates` feature accepts both as
/// values, so `string()` must be able to render them too. Renders the value's OWN offset (never
/// forced to UTC — a `date and time` value with an explicit non-zero offset/zone must round-trip
/// that offset, not get silently re-expressed in UTC).
pub(crate) fn format_instant(t: &OffsetDateTime, qualifier: &Option<TimeQualifier>) -> String {
    // `{:04}` zero-pads to a *minimum* width of 4 — a 5/6-digit year still prints in full.
    let year_str = if t.year() < 0 {
        format!("-{:04}", -t.year())
    } else {
        format!("{:04}", t.year())
    };
    format!(
        "{year_str}-{:02}-{:02}T{:02}:{:02}:{:02}{}{}",
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        format_subsecond(t.nanosecond()),
        crate::temporal::format_qualifier(qualifier),
    )
}

/// Trailing `.fff…` fractional-second suffix (trailing zeros trimmed, mirroring how the source
/// literal's own precision round-trips) — empty when there is no sub-second component at all.
pub(crate) fn format_subsecond(nanos: u32) -> String {
    if nanos == 0 {
        return String::new();
    }
    let digits = format!("{nanos:09}");
    format!(".{}", digits.trim_end_matches('0'))
}
