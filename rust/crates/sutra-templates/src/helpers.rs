//! The Sutra helper set (R6 — complete, no growth beyond it): the conditional
//! helpers (`eq neq gt gte lt lte and or not`), `let`, `substring`,
//! `replace`, `coalesce`. All helper parameters resolve **null-tolerantly** (a missing path
//! is a null argument — params are never routed through a missing-helper hook).

use handlebars::{
    BlockContext, BlockParams, Context, Handlebars, Helper, HelperDef, HelperResult, Output,
    RenderContext, RenderError, RenderErrorReason, Renderable, ScopedJson,
};
use serde_json::Value as Json;

pub(crate) fn register(hb: &mut Handlebars<'static>) {
    hb.register_helper("eq", Box::new(Compare::Eq));
    hb.register_helper("neq", Box::new(Compare::Neq));
    hb.register_helper("gt", Box::new(Compare::Gt));
    hb.register_helper("gte", Box::new(Compare::Gte));
    hb.register_helper("lt", Box::new(Compare::Lt));
    hb.register_helper("lte", Box::new(Compare::Lte));
    hb.register_helper("and", Box::new(Logic::And));
    hb.register_helper("or", Box::new(Logic::Or));
    hb.register_helper("not", Box::new(Logic::Not));
    hb.register_helper("let", Box::new(LetHelper));
    hb.register_helper("substring", Box::new(Substring));
    hb.register_helper("replace", Box::new(Replace));
    hb.register_helper("coalesce", Box::new(Coalesce));
}

/// Null-tolerant parameter access: a missing path resolves to `Json::Null`, never an error.
fn param<'a>(h: &'a Helper<'_>, idx: usize) -> &'a Json {
    h.param(idx).map(|p| p.value()).unwrap_or(&Json::Null)
}

/// handlebars.js truthiness (what the conditional helpers apply): `false`, null, `""`,
/// `0` and `[]` are falsy; everything else is truthy.
fn is_truthy(v: &Json) -> bool {
    match v {
        Json::Null => false,
        Json::Bool(b) => *b,
        Json::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Json::String(s) => !s.is_empty(),
        Json::Array(a) => !a.is_empty(),
        Json::Object(_) => true,
    }
}

/// Canonical string coercion for the JSON value set the render model carries.
fn canonical_string_of(v: &Json) -> String {
    match v {
        Json::Null => "null".to_string(),
        Json::String(s) => s.clone(),
        Json::Number(n) => n.to_string(),
        Json::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Value equality with numeric awareness (`1` == `1.0`) over the map-resolved values.
fn json_eq(a: &Json, b: &Json) -> bool {
    match (as_number(a), as_number(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

fn as_number(v: &Json) -> Option<f64> {
    match v {
        Json::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn compare_order(a: &Json, b: &Json) -> Option<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (as_number(a), as_number(b)) {
        return x.partial_cmp(&y);
    }
    if let (Json::String(x), Json::String(y)) = (a, b) {
        return Some(x.cmp(y));
    }
    None
}

#[derive(Clone, Copy)]
enum Compare {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl HelperDef for Compare {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _r: &'reg Handlebars<'reg>,
        _ctx: &'rc Context,
        _rc: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = param(h, 0);
        let b = param(h, 1);
        let result = match self {
            Compare::Eq => json_eq(a, b),
            Compare::Neq => !json_eq(a, b),
            Compare::Gt => matches!(compare_order(a, b), Some(std::cmp::Ordering::Greater)),
            Compare::Gte => matches!(
                compare_order(a, b),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            Compare::Lt => matches!(compare_order(a, b), Some(std::cmp::Ordering::Less)),
            Compare::Lte => matches!(
                compare_order(a, b),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
        };
        Ok(ScopedJson::Derived(Json::Bool(result)))
    }
}

#[derive(Clone, Copy)]
enum Logic {
    And,
    Or,
    Not,
}

impl HelperDef for Logic {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _r: &'reg Handlebars<'reg>,
        _ctx: &'rc Context,
        _rc: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let values: Vec<&Json> = h.params().iter().map(|p| p.value()).collect();
        let result = match self {
            Logic::And => !values.is_empty() && values.iter().all(|v| is_truthy(v)),
            Logic::Or => values.iter().any(|v| is_truthy(v)),
            Logic::Not => !is_truthy(param(h, 0)),
        };
        Ok(ScopedJson::Derived(Json::Bool(result)))
    }
}

/// `{{#let <value> as |name|}}…{{/let}}` — strict `{{#let}}` semantics: ALWAYS renders its body,
/// binding a possibly-null value as a block param (`{{#with}}` would silently skip the body,
/// hiding data defects from the strict policy). A later USE of a null binding strict-fails.
struct LetHelper;

impl HelperDef for LetHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        r: &'reg Handlebars<'reg>,
        ctx: &'rc Context,
        rc: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h.param(0).map(|p| p.value().clone()).unwrap_or(Json::Null);
        let mut block = BlockContext::new();
        if let Some(bp_name) = h.block_param() {
            let mut params = BlockParams::new();
            params.add_value(bp_name, value)?;
            block.set_block_params(params);
        }
        rc.push_block(block);
        if let Some(t) = h.template() {
            t.render(r, ctx, rc, out)?;
        }
        rc.pop_block();
        Ok(())
    }
}

/// `{{substring value from to?}}` — substring by character index over the string form of
/// `value` (out-of-range fails the render).
struct Substring;

impl HelperDef for Substring {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _r: &'reg Handlebars<'reg>,
        _ctx: &'rc Context,
        _rc: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let s = canonical_string_of(param(h, 0));
        let chars: Vec<char> = s.chars().collect();
        let from = int_param(h, 1, "substring")?;
        let to = match h.param(2) {
            Some(_) => int_param(h, 2, "substring")?,
            None => chars.len(),
        };
        if from > to || to > chars.len() {
            return Err(RenderErrorReason::Other(format!(
                "substring range {from}..{to} out of bounds for value of length {}",
                chars.len()
            ))
            .into());
        }
        let out: String = chars[from..to].iter().collect();
        Ok(ScopedJson::Derived(Json::String(out)))
    }
}

fn int_param(h: &Helper<'_>, idx: usize, helper: &str) -> Result<usize, RenderError> {
    match param(h, idx) {
        Json::Number(n) => n
            .as_u64()
            .map(|v| v as usize)
            .ok_or_else(|| bad_int(helper, idx)),
        _ => Err(bad_int(helper, idx)),
    }
}

fn bad_int(helper: &str, idx: usize) -> RenderError {
    RenderErrorReason::ParamTypeMismatchForName(
        // Leaked once per distinct helper name — the set is fixed at compile time.
        match helper {
            "substring" => "substring",
            other => Box::leak(other.to_string().into_boxed_str()),
        },
        idx.to_string(),
        "integer".to_string(),
    )
    .into()
}

/// `{{replace value "a" "b"}}` — literal `String.replace`.
struct Replace;

impl HelperDef for Replace {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _r: &'reg Handlebars<'reg>,
        _ctx: &'rc Context,
        _rc: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let s = canonical_string_of(param(h, 0));
        let from = canonical_string_of(param(h, 1));
        let to = canonical_string_of(param(h, 2));
        Ok(ScopedJson::Derived(Json::String(s.replace(&from, &to))))
    }
}

/// `{{coalesce value fallback…}}` — the first non-null argument; the strict-mode escape hatch
/// for genuinely optional values (the `?:` default form).
struct Coalesce;

impl HelperDef for Coalesce {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _r: &'reg Handlebars<'reg>,
        _ctx: &'rc Context,
        _rc: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        for p in h.params() {
            if !p.value().is_null() {
                return Ok(ScopedJson::Derived(p.value().clone()));
            }
        }
        Ok(ScopedJson::Derived(Json::Null))
    }
}
