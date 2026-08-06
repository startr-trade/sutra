//! Handlebars strict template engine — the strict Handlebars renderer and its template
//! analyzer. Templates are Handlebars in strict mode: a missing value fails the render.
//!
//! - **Escaping — NOOP, deliberate.** Module templates produce plain-text / XML / JSON wire
//!   payloads, never HTML; `{{x}}` renders verbatim.
//! - **Strict rendering — missing values fail, engine-enforced.** A variable that
//!   resolves to nothing — a missing map key, a typo'd nested path, a null mid-path — aborts
//!   the render. Helper parameters and `#if`/`#unless` conditions stay null-tolerant, so
//!   `(eq maybe "X")` branches on absence and `{{coalesce maybe "dflt"}}` supplies a default.
//!   To make an explicitly-null value strict-fail exactly like an absent key, null-valued
//!   map entries are stripped from the model before render so present-null ≡ absent.
//! - **Whitespace.** handlebars-rust applies the mustache-spec standalone-line removal by
//!   default.
//! - **Helper set (complete — closed, no growth beyond it):** `eq neq gt gte lt lte and or
//!   not` (null-tolerant conditional helpers), `let` (block-param binding that always renders
//!   its body), `substring`, `replace`, `coalesce`. The `uuid`/`now` generators are render
//!   -context suppliers injected by the caller (the executor puts them in the model) — never
//!   wall-clock in the engine.
#![forbid(unsafe_code)]

mod analyzer;
mod helpers;

use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt;

use handlebars::{Handlebars, RenderErrorReason};
use serde_json::Value as Json;

pub use analyzer::TemplateAnalysis;

/// Engine handle / registry key (the engine's registry name).
pub const NAME: &str = "h";

/// A template compile/render failure — carries the engine's failure messages
/// (`Invalid Handlebars template …`, `Handlebars render failed …`,
/// `unresolved template reference …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateError {
    pub message: String,
}

impl TemplateError {
    fn new(message: impl Into<String>) -> Self {
        TemplateError {
            message: message.into(),
        }
    }
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TemplateError {}

/// The strict Handlebars engine. Compiled templates are cached by their immutable
/// module-scoped id (single-threaded here — the sync executor owns one engine).
pub struct HandlebarsTemplateEngine {
    registry: RefCell<Handlebars<'static>>,
    compiled: RefCell<HashSet<String>>,
}

impl Default for HandlebarsTemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlebarsTemplateEngine {
    pub fn new() -> Self {
        let mut hb = Handlebars::new();
        // NOOP escaping — see the crate docs; do NOT switch to the HTML default.
        hb.register_escape_fn(handlebars::no_escape);
        // Strict mode (R6): an unresolved variable aborts the render.
        hb.set_strict_mode(true);
        helpers::register(&mut hb);
        HandlebarsTemplateEngine {
            registry: RefCell::new(hb),
            compiled: RefCell::new(HashSet::new()),
        }
    }

    /// Engine handle (registry key).
    pub fn name(&self) -> &'static str {
        NAME
    }

    /// File extensions this engine claims.
    pub fn extensions(&self) -> Vec<&'static str> {
        vec![".hbs"]
    }

    /// Static analysis for the deploy-time type-safety check — external roots + literal
    /// `payload.`-rooted field paths.
    pub fn analyze(&self, template: &[u8]) -> TemplateAnalysis {
        analyzer::analyze(template)
    }

    /// Compile-check a template without registering or rendering it — the package-time
    /// fail-closed parse gate used by `sutra package` / `sutra lint` ([`Self::analyze`]
    /// deliberately returns empty on a parse error, so packaging needs this explicit check).
    pub fn check(&self, template: &[u8]) -> Result<(), TemplateError> {
        let src = String::from_utf8_lossy(template);
        handlebars::template::Template::compile(&src)
            .map(|_| ())
            .map_err(|e| TemplateError::new(format!("Invalid Handlebars template: {e}")))
    }

    /// Render `template` (cached under `template_id`) against `model` (a JSON object).
    pub fn render(
        &self,
        template_id: &str,
        template: &[u8],
        model: &Json,
    ) -> Result<String, TemplateError> {
        if template_id.trim().is_empty() {
            return Err(TemplateError::new("templateId is required"));
        }
        if !self.compiled.borrow().contains(template_id) {
            let src = String::from_utf8_lossy(template).into_owned();
            self.registry
                .borrow_mut()
                .register_template_string(template_id, src)
                .map_err(|e| {
                    TemplateError::new(format!("Invalid Handlebars template '{template_id}': {e}"))
                })?;
            self.compiled.borrow_mut().insert(template_id.to_string());
        }
        // Strip null-valued map entries so an explicitly-null value strict-fails exactly like
        // an absent key.
        let model = strip_nulls(model);
        self.registry
            .borrow()
            .render(template_id, &model)
            .map_err(|e| match e.reason() {
                RenderErrorReason::MissingVariable(path) => TemplateError::new(format!(
                    "unresolved template reference {{{{{}}}}} (strict rendering: every \
                     referenced value must be present; use {{{{coalesce x \"default\"}}}} for \
                     optional values)",
                    path.clone().unwrap_or_default()
                )),
                _ => {
                    TemplateError::new(format!("Handlebars render failed for '{template_id}': {e}"))
                }
            })
    }
}

/// Recursively drop null-valued entries from JSON objects (arrays keep their elements so
/// index positions stay stable).
fn strip_nulls(v: &Json) -> Json {
    match v {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(_, val)| !val.is_null())
                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}
