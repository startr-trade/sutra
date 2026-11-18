//! Handlebars strict template engine — the strict Handlebars renderer and its template
//! analyzer. Templates are Handlebars in strict mode: a missing value fails the render.
//!
//! - **Escaping — NOOP, deliberate.** Module templates produce plain-text / XML / JSON wire
//!   payloads, never HTML; `{{x}}` renders verbatim. Relayed free text that lands in XML
//!   opts in to escaping per value with `{{xmlEscape x}}`.
//! - **Strict rendering — missing values fail, engine-enforced.** A variable that
//!   resolves to nothing — a missing map key, a typo'd nested path, a null mid-path — aborts
//!   the render. Helper parameters and `#if`/`#unless` conditions stay null-tolerant, so
//!   `(eq maybe "X")` branches on absence and `{{coalesce maybe "dflt"}}` supplies a default.
//!   To make an explicitly-null value strict-fail exactly like an absent key, null-valued
//!   map entries are stripped from the model before render so present-null ≡ absent.
//! - **Whitespace.** handlebars-rust applies the mustache-spec standalone-line removal by
//!   default.
//! - **Helper set (closed — a helper is added only by a deliberate design decision):** `eq neq gt gte lt lte and or
//!   not` (null-tolerant conditional helpers), `let` (block-param binding that always renders
//!   its body), `substring`, `replace`, `coalesce`, `xmlEscape` (the five XML specials to
//!   their predefined entities; null renders empty). The `uuid`/`now` generators are render
//!   -context suppliers injected by the caller (the executor puts them in the model) — never
//!   wall-clock in the engine.
#![forbid(unsafe_code)]

mod analyzer;
mod helpers;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

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

/// Templates compiled in this process, shared by every engine: `template id → (source, compiled)`.
///
/// Compiling is the expensive half of a template's life: handlebars asks its parser for the line
/// and column of every token, which the parser counts from the start of the text, so the cost grows
/// with the square of the template's size (about 0.8 s for 100 KB and 3.4 s for 200 KB in a release
/// build). Each actor lane owns its own engine, and every activation builds new ones, so without
/// this each lane would compile each template again on its first render after every flip. With it,
/// a template is compiled once per process for as long as its id holds the same source: template
/// ids are deployment-scoped, so in practice once per deployment.
///
/// The source is kept and compared on every hit, so a reused id with new content is compiled
/// afresh, never served stale. The map is cleared when it passes [`COMPILED_CACHE_LIMIT`] entries,
/// which bounds what retired deployments leave behind.
static COMPILED: OnceLock<Mutex<HashMap<String, CompiledEntry>>> = OnceLock::new();

/// How many distinct template ids (and texts, for lint) the process-wide cache holds before it
/// starts again.
const COMPILED_CACHE_LIMIT: usize = 4096;

#[derive(Clone)]
struct CompiledEntry {
    source: Arc<str>,
    template: Arc<handlebars::template::Template>,
}

/// The cache key for a template known only by its text (lint and analysis have no stable id: one
/// package's `templates/x.hbs` is another's): `content:` and a 64-bit hash of the text. A hit still
/// compares the whole source, so a hash collision compiles afresh instead of serving another
/// template.
fn content_key(src: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    src.hash(&mut hasher);
    format!("content:{}:{:016x}", src.len(), hasher.finish())
}

/// How many times this process has compiled each distinct template text, by [`content_key`].
static COMPILES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

/// How many times this process has compiled exactly `template` (any id). For tests and
/// diagnostics: with the cache working, a template is compiled once however many lint checks,
/// analyses, deploys and renders see it.
pub fn compile_count(template: &[u8]) -> u64 {
    let src = String::from_utf8_lossy(template);
    COMPILES
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&content_key(&src))
                .copied()
        })
        .unwrap_or(0)
}

fn cached(key: &str, src: &str) -> Option<Arc<handlebars::template::Template>> {
    COMPILED
        .get()?
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(key)
        .filter(|entry| &*entry.source == src)
        .map(|entry| Arc::clone(&entry.template))
}

fn store(key: &str, src: &str, template: &Arc<handlebars::template::Template>) {
    let cache = COMPILED.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if map.len() >= COMPILED_CACHE_LIMIT && !map.contains_key(key) {
        map.clear();
    }
    map.insert(
        key.to_string(),
        CompiledEntry {
            source: Arc::from(src),
            template: Arc::clone(template),
        },
    );
}

/// The compiled form of `src`, by its text alone (see [`content_key`]): compiled once per process
/// however often lint and analysis ask.
fn compiled_by_content(
    src: &str,
) -> Result<Arc<handlebars::template::Template>, handlebars::TemplateError> {
    let key = content_key(src);
    if let Some(hit) = cached(&key, src) {
        return Ok(hit);
    }
    let template = Arc::new(compile_counted(src, None)?);
    store(&key, src, &template);
    Ok(template)
}

/// Compile, and count it.
fn compile_counted(
    src: &str,
    name: Option<&str>,
) -> Result<handlebars::template::Template, handlebars::TemplateError> {
    // Named, as `register_template_string` names it, so an error names the template. This engine
    // never sets `prevent_indent`, the one other option that call compiles with.
    let template = match name {
        Some(name) => handlebars::template::Template::compile_with_name(src, name.to_string()),
        None => handlebars::template::Template::compile(src),
    }?;
    *COMPILES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(content_key(src))
        .or_insert(0) += 1;
    Ok(template)
}

/// The compiled form of `src` under `template_id`: from the process-wide cache when the id was
/// compiled from the same source, else from the same text compiled under no id (by lint at
/// deploy), named for this id, else compiled now. Either way cached under the id.
fn compiled_template(
    template_id: &str,
    src: &str,
) -> Result<Arc<handlebars::template::Template>, handlebars::TemplateError> {
    if let Some(hit) = cached(template_id, src) {
        return Ok(hit);
    }
    // Compiled outside the lock: another lane compiling another template must not wait on this one.
    let template = match cached(&content_key(src), src) {
        Some(by_content) => {
            let mut named = (*by_content).clone();
            named.name = Some(template_id.to_string());
            Arc::new(named)
        }
        None => Arc::new(compile_counted(src, Some(template_id))?),
    };
    store(template_id, src, &template);
    Ok(template)
}

/// Compile `template` under `template_id` into the process-wide cache now, so its first render
/// does not pay for the compile. `Ok(true)` when it was compiled now (or taken from the compile
/// lint made of the same text), `Ok(false)` when the cache
/// already held this id with this source. A template that does not compile is not cached, and
/// renders fail on it exactly as they would have without this call.
pub fn precompile(template_id: &str, template: &[u8]) -> Result<bool, TemplateError> {
    let src = String::from_utf8_lossy(template);
    if is_compiled(template_id, template) {
        return Ok(false);
    }
    compiled_template(template_id, &src)
        .map(|_| true)
        .map_err(|e| {
            TemplateError::new(format!("Invalid Handlebars template '{template_id}': {e}"))
        })
}

/// Whether the process-wide cache holds `template_id` compiled from exactly `template`.
pub fn is_compiled(template_id: &str, template: &[u8]) -> bool {
    let src = String::from_utf8_lossy(template);
    COMPILED
        .get()
        .map(|cache| {
            cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(template_id)
                .is_some_and(|entry| *entry.source == *src)
        })
        .unwrap_or(false)
}

/// The strict Handlebars engine. Compiled templates are cached by their immutable
/// module-scoped id (single-threaded here — the sync executor owns one engine), and the compile
/// itself is shared process-wide (see [`COMPILED`]).
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
        compiled_by_content(&src)
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
            let src = String::from_utf8_lossy(template);
            let compiled = compiled_template(template_id, &src).map_err(|e| {
                TemplateError::new(format!("Invalid Handlebars template '{template_id}': {e}"))
            })?;
            self.registry
                .borrow_mut()
                .register_template(template_id, (*compiled).clone());
            self.compiled.borrow_mut().insert(template_id.to_string());
        }
        // Strip null-valued map entries so an explicitly-null value strict-fails exactly like
        // an absent key. A model with no null-valued entry anywhere renders as it is: stripping
        // it would rebuild an identical copy.
        let stripped;
        let model = if has_null_entry(model) {
            stripped = strip_nulls(model);
            &stripped
        } else {
            model
        };
        self.registry
            .borrow()
            .render(template_id, model)
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

/// Whether [`strip_nulls`] would change `v`: some object, at any depth (inside arrays too), has a
/// null-valued entry. A walk without allocation, so the common all-present model skips the copy.
fn has_null_entry(v: &Json) -> bool {
    match v {
        Json::Object(map) => map.values().any(|val| val.is_null() || has_null_entry(val)),
        Json::Array(items) => items.iter().any(has_null_entry),
        _ => false,
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

#[cfg(test)]
mod null_strip_tests {
    use super::*;

    /// `has_null_entry` is exactly "stripping would change the model".
    #[test]
    fn has_null_entry_is_true_exactly_when_stripping_changes_the_model() {
        let cases = [
            serde_json::json!({"a": 1, "b": {"c": "x", "d": [1, {"e": true}]}}),
            serde_json::json!({"a": null}),
            serde_json::json!({"a": {"b": [1, {"c": null}]}}),
            serde_json::json!({"a": [null, 2]}),
            serde_json::json!([{"a": 1}, {"b": null}]),
            serde_json::json!(null),
            serde_json::json!("text"),
        ];
        for case in cases {
            assert_eq!(has_null_entry(&case), strip_nulls(&case) != case, "{case}");
        }
    }
}

#[cfg(test)]
mod compiled_cache_tests {
    use super::*;

    #[test]
    fn one_id_and_source_is_compiled_once_per_process() {
        let a = compiled_template("cache-test-once", "<A>{{v}}</A>").unwrap();
        let b = compiled_template("cache-test-once", "<A>{{v}}</A>").unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "the second lookup reuses the first compile"
        );
        let changed = compiled_template("cache-test-once", "<B>{{v}}</B>").unwrap();
        assert!(
            !Arc::ptr_eq(&a, &changed),
            "new source under the id compiles afresh"
        );
    }

    #[test]
    fn a_precompiled_template_is_compiled_before_its_first_render() {
        let src = b"<P>{{v}}</P>";
        assert!(!is_compiled("cache-test-pre", src));
        assert!(precompile("cache-test-pre", src).unwrap(), "compiled now");
        assert!(is_compiled("cache-test-pre", src));
        assert!(
            !precompile("cache-test-pre", src).unwrap(),
            "already cached"
        );
        let engine = HandlebarsTemplateEngine::new();
        let out = engine
            .render("cache-test-pre", src, &serde_json::json!({"v": "1"}))
            .unwrap();
        assert_eq!(out, "<P>1</P>");
        assert!(precompile("cache-test-bad", b"{{#if x}}open").is_err());
        assert!(!is_compiled("cache-test-bad", b"{{#if x}}open"));
    }

    #[test]
    fn lint_checks_analyses_and_renders_compile_a_template_once() {
        let src = b"<Once>{{payload.a}}{{#if payload.b}}{{payload.c}}{{/if}}</Once>";
        let engine = HandlebarsTemplateEngine::new();
        engine.check(src).unwrap();
        engine.check(src).unwrap();
        let _ = engine.analyze(src);
        let _ = HandlebarsTemplateEngine::new().analyze(src);
        precompile("cache-test-once-all", src).unwrap();
        engine
            .render(
                "cache-test-once-all",
                src,
                &serde_json::json!({"payload": {"a": 1, "b": false}}),
            )
            .unwrap();
        assert_eq!(compile_count(src), 1);
    }

    #[test]
    fn a_compile_error_is_not_cached() {
        assert!(compiled_template("cache-test-err", "{{#if x}}open").is_err());
        assert!(compiled_template("cache-test-err", "{{x}}").is_ok());
    }
}
