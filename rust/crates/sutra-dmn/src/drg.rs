//! DMN Decision Requirements Graph (DRG) evaluator — the compliance-level-3 execution path,
//! and the engine behind the DMN-TCK harness (`tests/tck.rs`).
//!
//! Where [`crate::validator::DmnRulesetValidator`] evaluates ONE decision table for the BPMN
//! validator SPI, this evaluates a whole DRG the way DMN semantics require: decisions in
//! dependency order (`informationRequirement`), each decision's result bound into its dependents'
//! context by the required decision's `<variable>` name, and a decision's LOGIC evaluated by kind:
//!
//! - **decision table** — reuses the very same firing/output core as the validator
//!   ([`crate::validator::rule_fires`] / [`evaluate_outputs`](crate::validator::evaluate_outputs)),
//!   then shapes the DMN *decision result*: a single-output table yields the output value; a
//!   multi-output table a context; a COLLECT/RULE ORDER/OUTPUT ORDER table a list; PRIORITY picks
//!   the firing rule ranked highest in the output's `<outputValues>` list (not just the first).
//! - **literal expression** — the FEEL text, evaluated against the context via
//!   [`sutra_feel::expressions::eval`] (the bulk of compliance level 3).
//! - **boxed context** — each `contextEntry` evaluated in order, binding earlier entries; the
//!   result is the final unnamed entry, or the assembled context map.
//! - **invocation** — evaluate each binding, bind to the invoked BKM's parameter names (gated by
//!   the BKM's declared formal-parameter typeRefs — DMN §10.3.2.13), evaluate the BKM body.
//! - **business knowledge model** — a named, parameterized FEEL function invoked from decisions,
//!   either via the structured `<invocation>` element above, or — when its body reduces to a
//!   literal expression — as an ordinary FEEL function value bound into every decision's context
//!   under its own name (so `bkm_001(...)` inside a literal expression resolves the same way an
//!   inline `function(...) ...` literal would).
//! - **boxed functionDefinition** — DMN's XML-structured `function(params) body` is translated,
//!   at parse time, to the equivalent inline FEEL text (`function(p1: t1, ...) body`) and treated
//!   as an ordinary literal expression — this reuses the whole FEEL literal pipeline (closures,
//!   currying, a nested `<functionDefinition>` becoming a nested `function(...) ...`) with no new
//!   evaluator plumbing.
//! - **decision service** — an invocable subgraph: its declared `inputDecision`/`inputData`
//!   parameters override the normal per-decision evaluation of those nodes (their own logic is
//!   never run), and only its `outputDecision`(s) are required; see
//!   [`Drg::evaluate_decision_service`]. Only DIRECT invocation (the TCK's own
//!   `type="decisionService"` test-case shape) is supported — invoking a decision service BY NAME
//!   from an ordinary literal expression (`decisionService_004()`) would need a native callable
//!   that re-runs part of the DRG, which isn't expressible as a plain FEEL function value; that
//!   indirect form is left unsupported (see `tests/tck.rs`'s dispatch and `result-cycle5.md`).
//! - **boxed conditional/filter/for/some/every/list** — same text-translation trick as boxed
//!   functionDefinition: DMN's dedicated XML shape for each of these is exactly the structural
//!   twin of the corresponding inline FEEL construct (`if/then/else`, `list[pred]`, `for x in
//!   ... return ...`, `some/every x in ... satisfies ...`, `[a, b, c]`), so it is translated to
//!   that text and evaluated as an ordinary literal expression.
//!
//! typeRef-driven coercion (DMN §10.3.2.13, "semantic conformance to typeRef") is applied at
//! every place a value is bound to a typed slot: a decision's own result (against its
//! `<variable typeRef>`), a context entry's result (against ITS `<variable typeRef>`), an
//! invoked BKM's formal parameters and return value, an `<invocation>`'s own declared type, and a
//! decision service's declared input/output types. `itemDefinition`s (including nested
//! `isCollection`/component/`functionItem` shapes) are parsed once into a
//! [`sutra_feel::FeelTypeShape`] table and consulted by [`sutra_feel::coerce_to_shape`]; a value
//! that cannot be made to conform becomes `null`, never a wrong-shaped value.
//!
//! The existing decision-table model/loader/validator + SPI are deliberately untouched; this is a
//! self-contained, additive path. Anything the engine cannot yet evaluate becomes
//! [`Logic::Unsupported`] / an `Err`, which the harness records as UNSUPPORTED (allowlist) rather
//! than a conformance failure.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bigdecimal::BigDecimal;
use sutra_feel::expressions;
use sutra_feel::{
    coerce_to_shape, ExternalFunctionBinding, FeelContext, FeelExpr, FeelFunction, FeelTypeShape,
    FeelValue, Invocable,
};

use crate::loader::build_table;
use crate::model::{DmnDecisionTable, HitPolicy};
use crate::validator::{
    evaluate_outputs, first_output_with_priority_list, rank_of_output, rule_fires,
    DmnRulesetValidator, EvaluatedOutput,
};
use crate::xml::{self, XmlElement};

/// A decision's logic — the DMN "expression" kinds a decision (or context entry / BKM body) can
/// carry. `Unsupported` records why so the harness can allowlist rather than fail it.
/// COLLECT aggregator (`<decisionTable aggregation="…">`) — collapses the collected outputs to a
/// scalar per DMN § 8.2.10.
#[derive(Clone, Copy)]
enum Aggregation {
    Sum,
    Count,
    Min,
    Max,
}

fn parse_aggregation(raw: &str) -> Option<Aggregation> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "SUM" => Some(Aggregation::Sum),
        "COUNT" => Some(Aggregation::Count),
        "MIN" => Some(Aggregation::Min),
        "MAX" => Some(Aggregation::Max),
        _ => None,
    }
}

enum Logic {
    Table(DmnDecisionTable, Option<Aggregation>),
    Literal(String),
    Context(Vec<ContextEntry>),
    Invocation {
        bkm: String,
        bindings: Vec<(String, Logic)>,
        /// The `<invocation typeRef="…">` attribute — coerces the invoked BKM's (already
        /// return-coerced) result, a separate/later coercion stage from the BKM's own return type.
        type_ref: Option<String>,
    },
    Unsupported(String),
}

struct ContextEntry {
    /// `None` for the final "result" entry of a boxed context.
    name: Option<String>,
    /// The entry's own `<variable typeRef>` (coerces its evaluated value before it's bound).
    type_ref: Option<String>,
    logic: Logic,
}

struct DrgDecision {
    id: String,
    name: String,
    /// Ids of required decisions (`informationRequirement` → `requiredDecision`). Each required
    /// decision's result is bound into this decision's context by its `<variable>` name (looked up
    /// in [`Drg::id_to_var`]).
    requires: Vec<String>,
    /// The decision's own `<variable typeRef>` — coerces its result before it's bound/returned.
    type_ref: Option<String>,
    logic: Logic,
}

/// A BKM's formal parameter: its binding name plus the (optional) declared typeRef that gates
/// conformance at invocation (DMN §10.3.2.13) — a caller-supplied argument that can't be coerced
/// to it means the BKM is never invoked at all.
struct FormalParam {
    name: String,
    type_ref: Option<String>,
}

struct Bkm {
    params: Vec<FormalParam>,
    /// The BKM's own declared return typeRef (the `typeRef` attribute on whatever logic element
    /// sits directly inside `<encapsulatedLogic>` — a boxed `<functionDefinition>` body carries no
    /// such attribute of its own, so this is `None` for those, which is `Any`/no-op, exactly
    /// right: DMN doesn't attach an overall return type to a boxed function definition).
    return_type_ref: Option<String>,
    body: Logic,
    /// Names of OTHER BKMs this BKM's own body may call by bare name (via `knowledgeRequirement`)
    /// — used to order the callable-prelude build so a BKM's captured scope already contains
    /// every BKM it (transitively) depends on (DMN-TCK 0034-drg-scopes's "BKM I" → "BKM II" →
    /// "BKM III"/"BKM IV" chain).
    requires: Vec<String>,
    /// `Some` iff `<encapsulatedLogic kind="Java"/"PMML">` — the classified §10.3.2.13.3
    /// java/pmml binding. Such a BKM loads fine and IS a callable value ([`build_bkm_function`]
    /// builds the same always-rejecting external `FeelFunction` an inline `function(…) external
    /// {…}` literal evaluates to); only invoking it errors, whether bare-called or via a
    /// structured `<invocation>` ([`eval_invocation`]'s own guard).
    external: Option<Box<ExternalFunctionBinding>>,
}

/// A DMN `<itemDefinition>` — resolved lazily into a [`FeelTypeShape`] by [`resolve_named`]
/// (itself may reference another `itemDefinition` by name, hence the indirection).
struct ItemDef {
    is_collection: bool,
    kind: ItemDefKind,
}

enum ItemDefKind {
    /// `<typeRef>X</typeRef>` — an alias for (or, with `isCollection="true"`, a list of) `X`.
    Alias(String),
    /// `<itemComponent name typeRef>*` — a structural record.
    Record(Vec<(String, String)>),
    /// `<functionItem outputTypeRef="…">` — a function-VALUED type (used to type a BKM's
    /// lambda-valued formal parameter, or a decision service's own declared output type).
    FunctionItem { output_type_ref: Option<String> },
}

/// A `<decisionService>` — an invocable subgraph. `input_decisions`/`input_data` are ids whose
/// value, when the service is invoked, comes from the caller's argument rather than the node's
/// own logic (see [`Drg::evaluate_decision_service`]); `output_decisions` are the id(s) whose
/// value the service returns.
struct DecisionService {
    output_decisions: Vec<String>,
    input_decisions: Vec<String>,
    input_data: Vec<String>,
    /// The service's own `<variable typeRef>` — for a single-output service, DMN13-163 says this
    /// coerces that output decision's OWN value (a separate coercion stage from the decision's
    /// own declared type).
    type_ref: Option<String>,
}

/// A parsed DRG: decisions + business knowledge models + decision services + resolved imports.
pub struct Drg {
    decisions: Vec<DrgDecision>,
    bkms: HashMap<String, Bkm>,
    id_to_var: HashMap<String, String>,
    item_defs: HashMap<String, ItemDef>,
    /// typeRef of every `<inputData>`, by id — consulted when a decision service's declared
    /// `inputData` parameter is overridden at invocation (its own declared type still gates the
    /// caller-supplied value).
    input_type_refs: HashMap<String, Option<String>>,
    decision_services: HashMap<String, DecisionService>,
    /// Every BKM whose body reduces to a literal expression, bound as an ordinary
    /// `FeelValue::Function` under its own name — merged into every evaluation's base context so
    /// `bkm_name(...)` resolves via the FEEL evaluator's existing `Call`→`ctx.get` path. Built
    /// once at load time (topologically, so a BKM's own captured scope already contains the BKMs
    /// it transitively depends on).
    prelude: FeelContext,
    /// `<import>`ed sibling models, alias name → the imported model's own fully-loaded DRG
    /// (recursively resolved — see [`load_drg_with_imports`]). Exposed, at evaluation time, as a
    /// `FeelValue::Map` of the imported model's decision results, bound under the alias name.
    /// `Arc`, not `Box`: [`Drg::evaluate`]/[`Drg::evaluate_decision_service`] need a live `Arc<Drg>`
    /// to close over (an indirectly-invoked decision service's native `Invocable` re-runs the DRG
    /// from a `'static` closure — see [`decision_service_invocables`]), so every `Drg` an
    /// evaluation might reach, imports included, is Arc-held uniformly.
    imports: Vec<(String, Arc<Drg>)>,
}

/// Parse a full `.dmn` file into a DRG (level-3 aware), with no `<import>` resolution (a model
/// that itself has no imports — the overwhelming majority of the corpus — loads identically
/// either way; use [`load_drg_with_imports`] when the model may import sibling files).
pub fn load_drg(bytes: &[u8]) -> Result<Arc<Drg>, String> {
    let root = xml::parse(bytes)?;
    Ok(Arc::new(load_drg_from_root(&root)?))
}

/// Parse a full `.dmn` file, resolving its `<import>`ed sibling models (DMN-TCK
/// 0089-nested-inputdata-imports) via `resolve_import`: given an import's namespace URI, it
/// returns that model's raw bytes, or `None` to skip it (an unresolvable import's decisions are
/// simply not exposed under its alias, rather than failing the whole load). Deliberately
/// filesystem-agnostic — `resolve_import` is where a caller (e.g. the TCK harness) does its own
/// directory scan; this crate has no I/O of its own. Each import is itself loaded through this
/// same function, so a multi-level import chain resolves recursively (bounded to guard against a
/// (spec-disallowed) cyclic import).
pub fn load_drg_with_imports(
    bytes: &[u8],
    resolve_import: &dyn Fn(&str) -> Option<Vec<u8>>,
) -> Result<Arc<Drg>, String> {
    Ok(Arc::new(load_drg_with_imports_at_depth(
        bytes,
        resolve_import,
        0,
    )?))
}

const MAX_IMPORT_DEPTH: u8 = 8;

fn load_drg_with_imports_at_depth(
    bytes: &[u8],
    resolve_import: &dyn Fn(&str) -> Option<Vec<u8>>,
    depth: u8,
) -> Result<Drg, String> {
    let root = xml::parse(bytes)?;
    let mut drg = load_drg_from_root(&root)?;
    if depth < MAX_IMPORT_DEPTH {
        for imp in root.children_named("import") {
            let namespace = imp.attr(None, "namespace");
            let alias = imp.attr(None, "name").filter(|s| !s.trim().is_empty());
            let (Some(namespace), Some(alias)) = (namespace, alias) else {
                continue;
            };
            let Some(imported_bytes) = resolve_import(namespace) else {
                continue;
            };
            if let Ok(imported) =
                load_drg_with_imports_at_depth(&imported_bytes, resolve_import, depth + 1)
            {
                drg.imports.push((alias.to_string(), Arc::new(imported)));
            }
        }
    }
    Ok(drg)
}

fn load_drg_from_root(root: &XmlElement) -> Result<Drg, String> {
    let item_defs = parse_item_definitions(root);
    let decision_services = parse_decision_services(root);

    let mut id_to_var = HashMap::new();
    let mut input_type_refs = HashMap::new();
    for input in root.children_named("inputData") {
        // A missing/blank `id` falls back to the (presumably unique) name — mirroring the
        // decision loop below. Never leave it blank: an empty id is not just "unreferenceable
        // by href", it actively COLLIDES with every other element that also lacks an id, corrupting
        // `id_to_var`/`result_by_id` lookups keyed by it.
        let raw_id = input.attr(None, "id").filter(|s| !s.trim().is_empty());
        let variable = input.child("variable");
        let raw_name = variable
            .and_then(|v| v.attr(None, "name"))
            .filter(|s| !s.trim().is_empty())
            .or_else(|| input.attr(None, "name").filter(|s| !s.trim().is_empty()));
        let name = raw_name.or(raw_id).unwrap_or("").to_string();
        let id = raw_id.unwrap_or(&name).to_string();
        if id.is_empty() {
            continue; // neither an id nor a name to key by at all — nothing usable
        }
        let type_ref = variable
            .and_then(|v| v.attr(None, "typeRef"))
            .map(str::to_string);
        id_to_var.insert(id.clone(), name);
        input_type_refs.insert(id, type_ref);
    }

    let mut decisions = Vec::new();
    for d in root.children_named("decision") {
        // Same "id falls back to name, never blank" rule as `<inputData>` above — DMN-TCK
        // 1111-feel-matches-function's 44 decisions all omit `id` entirely; leaving it `""`
        // made every one of them collide in `result_by_id`, so only the first ever actually
        // evaluated (every later same-"id" decision silently reused ITS value —
        // `evaluate_seeded`'s pre-seeded-id fast path, needed for decision-service parameter
        // overrides, can't distinguish "genuinely already computed" from "coincidentally shares
        // the empty-string id").
        let raw_id = d.attr(None, "id").filter(|s| !s.trim().is_empty());
        let raw_name = d.attr(None, "name").filter(|s| !s.trim().is_empty());
        let name = raw_name.or(raw_id).unwrap_or("").to_string();
        let id = raw_id.unwrap_or(&name).to_string();
        let variable = d.child("variable");
        let var = variable
            .and_then(|v| v.attr(None, "name"))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&name)
            .to_string();
        let type_ref = variable
            .and_then(|v| v.attr(None, "typeRef"))
            .map(str::to_string);
        id_to_var.insert(id.clone(), var.clone());
        let requires: Vec<String> = d
            .children_named("informationRequirement")
            .filter_map(|ir| ir.child("requiredDecision"))
            .filter_map(|rd| rd.attr(None, "href"))
            .map(href_fragment)
            .collect();
        let logic = parse_logic(d, &id);
        decisions.push(DrgDecision {
            id,
            name,
            requires,
            type_ref,
            logic,
        });
    }

    // BKMs need each other's ids resolved to names up front (a `knowledgeRequirement` href
    // points at another BKM's id) before the callable prelude can be ordered by dependency.
    let bkm_elements: Vec<&XmlElement> = root.children_named("businessKnowledgeModel").collect();
    let bkm_id_to_name: HashMap<String, String> = bkm_elements
        .iter()
        .filter_map(|b| {
            let name = b
                .attr(None, "name")
                .filter(|s| !s.trim().is_empty())?
                .to_string();
            // Same "id falls back to name, never blank" rule as decisions/inputData above.
            let id = b
                .attr(None, "id")
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(&name)
                .to_string();
            Some((id, name))
        })
        .collect();

    let mut bkms = HashMap::new();
    for b in &bkm_elements {
        let Some(name) = b.attr(None, "name").filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        let encapsulated = b.child("encapsulatedLogic");
        let params: Vec<FormalParam> = encapsulated
            .map(|e| {
                e.children_named("formalParameter")
                    .filter_map(|p| {
                        let pname = p.attr(None, "name")?.to_string();
                        Some(FormalParam {
                            name: pname,
                            type_ref: p.attr(None, "typeRef").map(str::to_string),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let return_type_ref = encapsulated.and_then(own_type_ref);
        // The BKM body is the logic child of <encapsulatedLogic>.
        let body = match encapsulated {
            Some(e) => parse_logic(e, name),
            None => Logic::Unsupported("BKM without <encapsulatedLogic>".to_string()),
        };
        // `<encapsulatedLogic kind="Java"/"PMML">`: the BKM itself is an EXTERNAL function
        // definition — classify its binding context now (self-contained string literals, so an
        // empty scope suffices; a body that won't render/evaluate is a malformed binding, never
        // a load error).
        let external = encapsulated.and_then(|e| {
            let kind_key = external_kind_key(e)?;
            let body_text =
                external_binding_text(e, kind_key).unwrap_or_else(|| "null".to_string());
            Some(Box::new(
                match expressions::eval(&body_text, &FeelContext::new()) {
                    Ok(v) => ExternalFunctionBinding::classify_body_value(&v),
                    Err(e) => ExternalFunctionBinding::Malformed { detail: e.message },
                },
            ))
        });
        let requires: Vec<String> = b
            .children_named("knowledgeRequirement")
            .filter_map(|kr| kr.child("requiredKnowledge"))
            .filter_map(|rk| rk.attr(None, "href"))
            .map(href_fragment)
            .filter_map(|id| bkm_id_to_name.get(&id).cloned())
            .collect();
        bkms.insert(
            name.to_string(),
            Bkm {
                params,
                return_type_ref,
                body,
                requires,
                external,
            },
        );
    }

    let prelude = build_callable_prelude(&bkms, &item_defs);

    Ok(Drg {
        decisions,
        bkms,
        id_to_var,
        item_defs,
        input_type_refs,
        decision_services,
        prelude,
        imports: Vec::new(),
    })
}

/// Take the fragment after the LAST `#` in an href — a bare local reference (`#_id`, no `#`
/// before it) still yields `_id`; a fully namespace-qualified href
/// (`http://.../model#_id`, DMN-TCK 0091-local-hrefs) correctly yields just `_id` too (unlike
/// `trim_start_matches('#')`, which only strips a LEADING `#` and leaves the whole URI otherwise).
fn href_fragment(href: &str) -> String {
    href.rsplit('#').next().unwrap_or(href).to_string()
}

/// Find and parse the single logic child of a logic-bearing element (a `<decision>`,
/// `<contextEntry>`, `<binding>`, or `<encapsulatedLogic>`).
fn parse_logic(parent: &XmlElement, owner_id: &str) -> Logic {
    if let Some(table) = parent.child("decisionTable") {
        let aggregation = table.attr(None, "aggregation").and_then(parse_aggregation);
        return match build_table(table, owner_id, &[]) {
            Ok(t) => Logic::Table(t, aggregation),
            Err(e) => Logic::Unsupported(format!("decisionTable parse: {e}")),
        };
    }
    if let Some(le) = parent.child("literalExpression") {
        let text = le
            .child("text")
            .map(XmlElement::trimmed_text)
            .unwrap_or_else(|| le.trimmed_text());
        return Logic::Literal(text.to_string());
    }
    if let Some(ctx) = parent.child("context") {
        let mut entries = Vec::new();
        for ce in ctx.children_named("contextEntry") {
            let variable = ce.child("variable");
            let name = variable
                .and_then(|v| v.attr(None, "name"))
                .map(str::to_string);
            let type_ref = variable
                .and_then(|v| v.attr(None, "typeRef"))
                .map(str::to_string);
            entries.push(ContextEntry {
                name,
                type_ref,
                logic: parse_logic(ce, owner_id),
            });
        }
        return Logic::Context(entries);
    }
    if let Some(inv) = parent.child("invocation") {
        // The invoked BKM name is the invocation's own literalExpression text.
        let bkm = inv
            .child("literalExpression")
            .and_then(|le| le.child("text"))
            .map(|t| t.trimmed_text().to_string())
            .unwrap_or_default();
        let type_ref = inv.attr(None, "typeRef").map(str::to_string);
        let mut bindings = Vec::new();
        for b in inv.children_named("binding") {
            let Some(pname) = b
                .child("parameter")
                .and_then(|p| p.attr(None, "name"))
                .map(str::to_string)
            else {
                continue;
            };
            bindings.push((pname, parse_logic(b, owner_id)));
        }
        return Logic::Invocation {
            bkm,
            bindings,
            type_ref,
        };
    }
    // A boxed `function(params) body` — translated to the equivalent inline FEEL text and
    // treated as an ordinary literal expression (see the module doc comment).
    if let Some(fd) = parent.child("functionDefinition") {
        return match function_definition_text(fd) {
            Some(text) => Logic::Literal(text),
            None => Logic::Unsupported(
                "functionDefinition body must reduce to a literal/nested functionDefinition"
                    .to_string(),
            ),
        };
    }
    // Boxed conditional/filter/for/some/every/list — each is exactly the structural twin of an
    // inline FEEL construct; translate to that text and evaluate as an ordinary literal (see the
    // module doc comment). Each sub-element's own logic is expected to be a plain
    // `<literalExpression>` in the corpus this covers; anything else degrades to `None` (kept
    // unsupported rather than guessed at).
    if let Some(cond) = parent.child("conditional") {
        return boxed_conditional_text(cond)
            .map(Logic::Literal)
            .unwrap_or_else(|| {
                Logic::Unsupported("boxed conditional: unsupported shape".to_string())
            });
    }
    if let Some(filter) = parent.child("filter") {
        return boxed_in_and(filter, "match", |in_text, body| {
            format!("({in_text})[{body}]")
        })
        .map(Logic::Literal)
        .unwrap_or_else(|| Logic::Unsupported("boxed filter: unsupported shape".to_string()));
    }
    if let Some(list) = parent.child("list") {
        return boxed_list_text(list)
            .map(Logic::Literal)
            .unwrap_or_else(|| Logic::Unsupported("boxed list: unsupported shape".to_string()));
    }
    if let Some(rel) = parent.child("relation") {
        return boxed_relation_text(rel)
            .map(Logic::Literal)
            .unwrap_or_else(|| {
                Logic::Unsupported("boxed relation: unsupported shape".to_string())
            });
    }
    if let Some(for_el) = parent.child("for") {
        let var = for_el.attr(None, "iteratorVariable").unwrap_or("item");
        return boxed_in_and(for_el, "return", |in_text, body| {
            format!("for {var} in ({in_text}) return ({body})")
        })
        .map(Logic::Literal)
        .unwrap_or_else(|| Logic::Unsupported("boxed for: unsupported shape".to_string()));
    }
    if let Some(some_el) = parent.child("some") {
        let var = some_el.attr(None, "iteratorVariable").unwrap_or("item");
        return boxed_in_and(some_el, "satisfies", |in_text, body| {
            format!("some {var} in ({in_text}) satisfies ({body})")
        })
        .map(Logic::Literal)
        .unwrap_or_else(|| Logic::Unsupported("boxed some: unsupported shape".to_string()));
    }
    if let Some(every_el) = parent.child("every") {
        let var = every_el.attr(None, "iteratorVariable").unwrap_or("item");
        return boxed_in_and(every_el, "satisfies", |in_text, body| {
            format!("every {var} in ({in_text}) satisfies ({body})")
        })
        .map(Logic::Literal)
        .unwrap_or_else(|| Logic::Unsupported("boxed every: unsupported shape".to_string()));
    }
    Logic::Unsupported("no supported decision logic (table/literal/context/invocation)".to_string())
}

/// The trimmed FEEL text of a `<literalExpression>` child of `el` (any wrapper — `<if>`, `<in>`,
/// `<then>`, …), if that's what it holds.
fn literal_text_of(el: &XmlElement) -> Option<String> {
    let le = el.child("literalExpression")?;
    Some(
        le.child("text")
            .map(XmlElement::trimmed_text)
            .unwrap_or_else(|| le.trimmed_text())
            .to_string(),
    )
}

/// `<conditional><if>…</if><then>…</then><else>…</else></conditional>` → `if (…) then (…) else
/// (…)` (DMN-TCK 1150-boxed-conditional).
fn boxed_conditional_text(cond: &XmlElement) -> Option<String> {
    let if_text = literal_text_of(cond.child("if")?)?;
    let then_text = literal_text_of(cond.child("then")?)?;
    let else_text = literal_text_of(cond.child("else")?)?;
    Some(format!(
        "if ({if_text}) then ({then_text}) else ({else_text})"
    ))
}

/// `<list><literalExpression>…</literalExpression>…</list>` → `[a, b, c]` (DMN-TCK
/// 1161-boxed-list-expression) — each item may itself be any logic; the corpus covered here
/// nests only plain literal expressions.
fn boxed_list_text(list: &XmlElement) -> Option<String> {
    let items: Option<Vec<String>> = list
        .children_named("literalExpression")
        .map(|le| {
            Some(
                le.child("text")
                    .map(XmlElement::trimmed_text)
                    .unwrap_or_else(|| le.trimmed_text())
                    .to_string(),
            )
        })
        .collect();
    Some(format!("[{}]", items?.join(", ")))
}

/// `<relation><column name="…"/>*<row><literalExpression>…</literalExpression>*</row>*</relation>`
/// → `[{col1: (expr1), col2: (expr2), …}, …]` — a DMN boxed relation is exactly a LIST OF
/// CONTEXTS, one per row, each keyed by the declared column names in order (DMN-TCK
/// 0016-some-every's `priceTable1`). Each cell is parenthesised so an arbitrary expression
/// (not just a bare literal) round-trips unambiguously inside the synthesized context entry.
/// `None` when a row's cell count doesn't match the declared column count (a malformed model).
fn boxed_relation_text(rel: &XmlElement) -> Option<String> {
    let columns: Vec<String> = rel
        .children_named("column")
        .filter_map(|c| c.attr(None, "name").map(str::to_string))
        .collect();
    if columns.is_empty() {
        return None;
    }
    let mut rows_text = Vec::with_capacity(rel.children.len());
    for row in rel.children_named("row") {
        let cells: Vec<String> = row
            .children_named("literalExpression")
            .map(|le| {
                le.child("text")
                    .map(XmlElement::trimmed_text)
                    .unwrap_or_else(|| le.trimmed_text())
                    .to_string()
            })
            .collect();
        if cells.len() != columns.len() {
            return None;
        }
        let entries: Vec<String> = columns
            .iter()
            .zip(cells.iter())
            .map(|(col, cell)| format!("{col}: ({cell})"))
            .collect();
        rows_text.push(format!("{{{}}}", entries.join(", ")));
    }
    Some(format!("[{}]", rows_text.join(", ")))
}

/// Shared shape for `<filter>`/`<for>`/`<some>`/`<every>`: an `<in>` source plus one named
/// second sub-element (`match`/`return`/`satisfies`), combined by `combine`.
fn boxed_in_and(
    el: &XmlElement,
    second_tag: &str,
    combine: impl FnOnce(&str, &str) -> String,
) -> Option<String> {
    let in_text = literal_text_of(el.child("in")?)?;
    let body_text = literal_text_of(el.child(second_tag)?)?;
    Some(combine(&in_text, &body_text))
}

/// Translate a boxed `<functionDefinition>` element into the equivalent inline FEEL
/// `function(params) body` text — DMN's XML-structured lambda is exactly this textual
/// construct's structural twin, so reusing the existing FEEL literal-expression pipeline
/// (parsing + evaluating text) handles it with no new evaluator plumbing, closures/currying
/// included (a nested `<functionDefinition>` recurses into another `function(...) ...` text,
/// achieving DMN-TCK 0092-feel-lambda's currying cases for free).
fn function_definition_text(fd: &XmlElement) -> Option<String> {
    let params: Vec<String> = fd
        .children_named("formalParameter")
        .filter_map(|p| {
            let name = p.attr(None, "name")?;
            Some(match p.attr(None, "typeRef") {
                Some(t) => format!("{name}: {t}"),
                None => name.to_string(),
            })
        })
        .collect();
    let body_text = if let Some(kind_key) = external_kind_key(fd) {
        // `kind="Java"/"PMML"`: the boxed context body is the §10.3.2.13.3 java/pmml binding —
        // rendered as the inline `external {java|pmml: {…}}` form so the FEEL evaluator's own
        // external handling (definition-time classification, invocation-time rejection) applies.
        // A missing/irreducible binding body degrades to `external null`, which classifies as a
        // malformed binding — still a loadable definition, still the rejection at invocation.
        format!(
            "external {}",
            external_binding_text(fd, kind_key).unwrap_or_else(|| "null".to_string())
        )
    } else if let Some(nested) = fd.child("functionDefinition") {
        function_definition_text(nested)?
    } else if let Some(le) = fd.child("literalExpression") {
        le.child("text")
            .map(XmlElement::trimmed_text)
            .unwrap_or_else(|| le.trimmed_text())
            .to_string()
    } else {
        return None; // a boxed (table/context) function body isn't reducible to text.
    };
    Some(format!("function({}) {body_text}", params.join(", ")))
}

/// The inline-FEEL context key a function definition's `kind` attribute maps to — `Some("java")`/
/// `Some("pmml")` for DMN's two EXTERNAL function kinds (case-insensitive), `None` for `FEEL`
/// (the default) or no attribute. Applies to `<functionDefinition kind="…">` and
/// `<encapsulatedLogic kind="…">` alike (the latter IS a functionDefinition in the DMN metamodel).
fn external_kind_key(el: &XmlElement) -> Option<&'static str> {
    match el.attr(None, "kind") {
        Some(k) if k.eq_ignore_ascii_case("java") => Some("java"),
        Some(k) if k.eq_ignore_ascii_case("pmml") => Some("pmml"),
        _ => None,
    }
}

/// A `kind="Java"/"PMML"` function definition's boxed `<context>` body rendered as the inline
/// external-body context literal, wrapped under the kind's key — e.g. `{java: {"class":
/// ("java.lang.Math"), "method signature": ("max(double, double)")}}`. Entry keys are emitted as
/// string literals (they include the spaced `method signature`) and each value parenthesised, so
/// arbitrary entry expressions round-trip unambiguously.
fn external_binding_text(fd: &XmlElement, kind_key: &str) -> Option<String> {
    let ctx = fd.child("context")?;
    let mut entries = Vec::new();
    for ce in ctx.children_named("contextEntry") {
        let name = ce.child("variable").and_then(|v| v.attr(None, "name"))?;
        let value = literal_text_of(ce)?;
        entries.push(format!("\"{}\": ({value})", name.replace('"', "\\\"")));
    }
    Some(format!("{{{kind_key}: {{{}}}}}", entries.join(", ")))
}

/// The `typeRef` attribute of whichever logic element sits directly inside `parent` (used for a
/// BKM's own declared return type — the typeRef lives on `<encapsulatedLogic>`'s direct
/// `<decisionTable>`/`<literalExpression>`/`<context>`/`<invocation>` child, never on
/// `<encapsulatedLogic>` itself). A boxed `<functionDefinition>` body carries no such attribute.
fn own_type_ref(parent: &XmlElement) -> Option<String> {
    for tag in [
        "decisionTable",
        "literalExpression",
        "context",
        "invocation",
    ] {
        if let Some(el) = parent.child(tag) {
            return el.attr(None, "typeRef").map(str::to_string);
        }
    }
    None
}

fn parse_item_definitions(root: &XmlElement) -> HashMap<String, ItemDef> {
    let mut out = HashMap::new();
    for def in root.children_named("itemDefinition") {
        let Some(name) = def.attr(None, "name").filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        let is_collection = def.attr(None, "isCollection") == Some("true");
        let kind = if let Some(fi) = def.child("functionItem") {
            ItemDefKind::FunctionItem {
                output_type_ref: fi.attr(None, "outputTypeRef").map(str::to_string),
            }
        } else if let Some(tr) = def.child("typeRef") {
            ItemDefKind::Alias(tr.trimmed_text().to_string())
        } else {
            let comps = def
                .children_named("itemComponent")
                .filter_map(|c| {
                    let cname = c.attr(None, "name")?.to_string();
                    let ctype = c
                        .child("typeRef")
                        .map(XmlElement::trimmed_text)
                        .unwrap_or_default()
                        .to_string();
                    Some((cname, ctype))
                })
                .collect();
            ItemDefKind::Record(comps)
        };
        out.insert(
            name.to_string(),
            ItemDef {
                is_collection,
                kind,
            },
        );
    }
    out
}

fn parse_decision_services(root: &XmlElement) -> HashMap<String, DecisionService> {
    let mut out = HashMap::new();
    for ds in root.children_named("decisionService") {
        let Some(name) = ds.attr(None, "name").filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        let type_ref = ds
            .child("variable")
            .and_then(|v| v.attr(None, "typeRef"))
            .map(str::to_string);
        let href_ids = |tag: &str| -> Vec<String> {
            ds.children_named(tag)
                .filter_map(|e| e.attr(None, "href"))
                .map(href_fragment)
                .collect()
        };
        out.insert(
            name.to_string(),
            DecisionService {
                output_decisions: href_ids("outputDecision"),
                input_decisions: href_ids("inputDecision"),
                input_data: href_ids("inputData"),
                type_ref,
            },
        );
    }
    out
}

/// Resolve a typeRef string (an `itemDefinition` name, or a base FEEL type name) into a
/// structural [`FeelTypeShape`]; `None` (no declared type) is `Any`.
fn resolve_type_shape(
    type_ref: Option<&str>,
    item_defs: &HashMap<String, ItemDef>,
) -> FeelTypeShape {
    match type_ref.map(str::trim).filter(|s| !s.is_empty()) {
        Some(t) => resolve_named(t, item_defs, 0),
        None => FeelTypeShape::Any,
    }
}

/// Follows `itemDefinition` aliases/records up to a bounded depth (DMN forbids cyclic type refs;
/// the bound is purely a defensive guard against a malformed model).
fn resolve_named(name: &str, item_defs: &HashMap<String, ItemDef>, depth: u8) -> FeelTypeShape {
    if depth > 16 {
        return FeelTypeShape::Any;
    }
    let Some(def) = item_defs.get(name) else {
        return FeelTypeShape::Base(name.to_string());
    };
    // A `functionItem` typeRef describes a lambda-VALUED slot (a BKM/decision-service formal
    // parameter typed as a function) — its value is a `FeelValue::Function`, never structurally
    // comparable to the function's own `outputTypeRef`, so it never gates (DMN-TCK
    // 0092-feel-lambda's `lambda_number_returns_number`-typed BKM parameters).
    let ItemDefKind::FunctionItem { .. } = &def.kind else {
        let inner = match &def.kind {
            ItemDefKind::Alias(inner_name) => resolve_named(inner_name, item_defs, depth + 1),
            ItemDefKind::Record(comps) => FeelTypeShape::Record(
                comps
                    .iter()
                    .map(|(n, tr)| (n.clone(), resolve_named(tr, item_defs, depth + 1)))
                    .collect(),
            ),
            ItemDefKind::FunctionItem { .. } => unreachable!("matched above"),
        };
        return if def.is_collection {
            FeelTypeShape::Collection(Box::new(inner))
        } else {
            inner
        };
    };
    FeelTypeShape::Any
}

/// A decision service's own declared typeRef, for coercing ITS output (DMN13-163): when that
/// typeRef resolves to a `functionItem`, the service's OUTPUT conforms to the functionItem's
/// `outputTypeRef` (the service, when invoked, produces a value of that type) — a different
/// reading of the same typeRef string than [`resolve_type_shape`]'s (which treats a
/// `functionItem` as a lambda-VALUED slot, appropriate for a formal parameter, not a service's own
/// result).
fn ds_output_shape(type_ref: Option<&str>, item_defs: &HashMap<String, ItemDef>) -> FeelTypeShape {
    if let Some(t) = type_ref.map(str::trim).filter(|s| !s.is_empty()) {
        if let Some(ItemDef {
            kind: ItemDefKind::FunctionItem { output_type_ref },
            ..
        }) = item_defs.get(t)
        {
            return resolve_type_shape(output_type_ref.as_deref(), item_defs);
        }
    }
    resolve_type_shape(type_ref, item_defs)
}

/// Build the "callable prelude": every BKM whose body reduces to a literal expression, as an
/// ordinary `FeelValue::Function`, keyed by name. Built in dependency order (via each BKM's own
/// `knowledgeRequirement`s), and — critically — each BKM's OWN `captured` scope is populated with
/// ONLY the (already-built) BKMs it itself DECLARES a `knowledgeRequirement` to, never every BKM
/// processed so far: DMN doesn't allow a BKM to invoke another BKM without declaring that
/// dependency, so a BKM with no `knowledgeRequirement`s (the overwhelmingly common case — e.g.
/// every BKM in DMN-TCK 0092-feel-lambda) needs, and gets, an EMPTY captured scope.
///
/// This restraint is load-bearing, not cosmetic: an earlier version seeded every BKM's captured
/// scope from the whole prelude accumulated so far, which is correct in spirit (DMN-TCK
/// 0034-drg-scopes's "BKM I" → "BKM II" → "BKM III"/"BKM IV" chain does need multi-hop
/// visibility) but catastrophic in practice — each BKM's function value would then embed a full
/// copy of every earlier one, which ITSELF embedded a full copy of every one before THAT,
/// compounding into an exponential-size structure (`FeelContext::clone()` on it took over a
/// second, and a whole run over 3300+ TCK assertions ballooned from ~5s to 260s+). Scoping to
/// declared dependencies keeps the built structure's size bounded by the (small) DMN
/// `knowledgeRequirement` graph — usually empty, at most a handful of edges — while still giving
/// 0034's chain the multi-hop visibility it needs (each link's captured scope contains exactly
/// its own declared dependents, which themselves carry theirs).
fn build_callable_prelude(
    bkms: &HashMap<String, Bkm>,
    item_defs: &HashMap<String, ItemDef>,
) -> FeelContext {
    // Every BKM name in the model, known up front — needed so `build_bkm_function`'s parse of
    // EACH body can merge a bare call to ANOTHER multi-word-named BKM (`BKM II(param)`) into one
    // call-name token, exactly like `sutra_feel::expressions::eval` already does at runtime
    // against a context's own keys (DMN-TCK 0034-drg-scopes's "BKM I" → "BKM II" → "BKM
    // III"/"BKM IV" chain: without this, `expressions::parse`'s context-free tokenizer never
    // fuses "BKM"+"II", so BKM I's own body fails to even PARSE, silently dropping it — and every
    // BKM transitively depending on it — out of the prelude entirely).
    let all_bkm_names: std::collections::HashSet<String> = bkms.keys().cloned().collect();
    let mut built = FeelContext::new();
    let mut remaining: Vec<&String> = bkms.keys().collect();
    remaining.sort(); // deterministic processing order for ties / a (spec-disallowed) cycle
    let mut done: Vec<&str> = Vec::new();
    while !remaining.is_empty() {
        let ready_idx = remaining
            .iter()
            .position(|name| {
                bkms[*name]
                    .requires
                    .iter()
                    .all(|r| done.contains(&r.as_str()) || !bkms.contains_key(r))
            })
            .unwrap_or(0); // break a cycle (shouldn't happen) by taking the next in order
        let name = remaining.remove(ready_idx);
        let bkm = &bkms[name];
        let mut own_scope = FeelContext::new();
        for dep in &bkm.requires {
            if let Some(v) = built.get(dep) {
                own_scope.insert(dep.clone(), v.clone());
            }
        }
        if let Some(value) = build_bkm_function(bkm, &own_scope, item_defs, &all_bkm_names) {
            built.insert(name.clone(), value);
        }
        done.push(name);
    }
    built
}

/// A BKM whose body is a literal expression, as a `FeelValue::Function` — `None` for a body kind
/// that can't be reduced to one (a boxed context/table encapsulatedLogic, or an unparseable
/// literal), which simply leaves that BKM uncallable-by-name (still usable via a structured
/// `<invocation>`, unaffected). `own_scope` is bounded to `bkm`'s own declared
/// `knowledgeRequirement`s (see [`build_callable_prelude`]'s doc comment) — never the whole
/// prelude. `all_bkm_names` is the merge set for the body's OWN parse (every BKM name in the
/// model, plus this BKM's own formal parameters) — broader than `own_scope`/`requires`
/// deliberately: the MERGE is a purely lexical, parse-time concern (turning "BKM"+"II" into one
/// call-name token) independent of which names the body can actually resolve at invocation time.
fn build_bkm_function(
    bkm: &Bkm,
    own_scope: &FeelContext,
    item_defs: &HashMap<String, ItemDef>,
    all_bkm_names: &std::collections::HashSet<String>,
) -> Option<FeelValue> {
    // An external (kind="Java"/"PMML") BKM is callable-by-name like any other, as the same
    // always-rejecting external function value an inline `function(…) external {…}` literal
    // evaluates to — real params/typeRef shapes (arity info is cheap), a body that never runs
    // (the evaluator rejects the invocation before ever touching it).
    if let Some(binding) = &bkm.external {
        return Some(FeelValue::Function(FeelFunction {
            params: bkm.params.iter().map(|p| p.name.clone()).collect(),
            body: Box::new(FeelExpr::Literal {
                start: 0,
                end: 0,
                value: FeelValue::Null,
            }),
            captured: own_scope.clone(),
            param_shapes: bkm
                .params
                .iter()
                .map(|p| resolve_type_shape(p.type_ref.as_deref(), item_defs))
                .collect(),
            return_shape: resolve_type_shape(bkm.return_type_ref.as_deref(), item_defs),
            external: Some(binding.clone()),
        }));
    }
    let Logic::Literal(text) = &bkm.body else {
        return None;
    };
    let mut known = all_bkm_names.clone();
    known.extend(bkm.params.iter().map(|p| p.name.clone()));
    let parsed = expressions::parse_with_known_names(text, &known).ok()?;
    let param_shapes = bkm
        .params
        .iter()
        .map(|p| resolve_type_shape(p.type_ref.as_deref(), item_defs))
        .collect();
    let return_shape = resolve_type_shape(bkm.return_type_ref.as_deref(), item_defs);
    Some(FeelValue::Function(FeelFunction {
        params: bkm.params.iter().map(|p| p.name.clone()).collect(),
        body: Box::new(parsed),
        captured: own_scope.clone(),
        param_shapes,
        return_shape,
        external: None,
    }))
}

/// Every `<import>`ed model's decisions AND its own callable BKMs/decision services (its own
/// [`globals_overlay`], as a `FeelValue::Map` keyed by name, bound under the import's alias —
/// DMN-TCK 0089-nested-inputdata-imports for decisions; 0086-import for a BKM: `myimport.Say
/// Hello(A Person)` field-accesses the imported model's "Say Hello" BKM value off the alias map,
/// then calls it — needing that BKM exposed there in the first place, not just decision results)
/// merged with [`globals_overlay`], over `inputs` — the common base every per-decision context
/// starts from. A free function (not a `Drg` method) taking `&Arc<Drg>` explicitly: it needs to
/// hand OUT a live `Arc` clone to each service/BKM invocable closure it builds.
fn merge_globals(drg: &Arc<Drg>, inputs: &FeelContext) -> FeelContext {
    let mut base = inputs.clone();
    for (alias, imported) in &drg.imports {
        let imported_out = imported.evaluate(inputs);
        let mut map: BTreeMap<String, FeelValue> = imported_out
            .into_iter()
            .filter_map(|(name, res)| res.ok().map(|v| (name, v)))
            .collect();
        // Decision results win on a (spec-disallowed, so purely defensive) name collision.
        for (name, value) in globals_overlay(imported) {
            map.entry(name).or_insert(value);
        }
        base.insert(alias.clone(), FeelValue::Map(map));
    }
    for (name, value) in globals_overlay(drg) {
        base.insert(name, value);
    }
    base
}

/// Everything a bare-name FEEL `Call` can resolve to that ISN'T a raw input or required-decision
/// binding: the load-time BKM-literal prelude (`Drg::prelude`), every `<decisionService>`'s
/// native indirect-invocation `Invocable` (see [`decision_service_invocables`]), and every OTHER
/// BKM (one whose body ISN'T a plain literal expression — a boxed context/table/invocation body,
/// DMN-TCK 0014-loan-comparison's `FinancialMetrics`, a context-bodied BKM bare-called from an
/// ordinary literal expression) as its OWN native `Invocable` (see [`build_bkm_invocable`]).
///
/// Used both by [`merge_globals`] (the top-level per-evaluation base) AND internally by each
/// service/BKM invocable's own closure, so one bare-called service/BKM can itself bare-call
/// ANOTHER — the mechanism composes uniformly instead of being special-cased to exactly one level
/// of nesting.
fn globals_overlay(drg: &Arc<Drg>) -> FeelContext {
    let mut out = drg.prelude.clone();
    for (name, value) in decision_service_invocables(drg) {
        out.insert(name, value);
    }
    for name in drg.bkms.keys() {
        if !out.contains_key(name) {
            if let Some(v) = build_bkm_invocable(name, drg) {
                out.insert(name.clone(), v);
            }
        }
    }
    // Late-bind each literal-BKM `FeelFunction`'s captured scope with any declared knowledge
    // requirement the LOAD-TIME prelude couldn't satisfy — a dep whose body isn't a plain
    // literal (its callable form is the native `Invocable` built just above, which didn't exist
    // yet when `build_callable_prelude` snapshotted `own_scope`) or a decision service. Without
    // this, a literal BKM bare-calling its context/table-bodied dep dies with "unknown function"
    // at invocation time (DMN-TCK 0035-test-structure-output: BKM `to hex` calls BKM `single
    // encode to hex`, whose `<encapsulatedLogic>` is a boxed context). Shallow by construction:
    // only names from the BKM's own `requires` list are copied, and the copied values are Arc'd
    // invocables/prelude functions — no recursive re-capture, so no repeat of cycle 5's
    // whole-prelude capture blowup.
    let resolved = out.clone();
    for (name, value) in out.iter_mut() {
        let (FeelValue::Function(f), Some(bkm)) = (value, drg.bkms.get(name)) else {
            continue;
        };
        for dep in &bkm.requires {
            if !f.captured.contains_key(dep) {
                if let Some(v) = resolved.get(dep) {
                    f.captured.insert(dep.clone(), v.clone());
                }
            }
        }
    }
    out
}

/// A non-literal-bodied BKM (a boxed context/table/invocation `<encapsulatedLogic>`, which
/// [`build_bkm_function`]'s load-time literal prelude can't represent as a `FeelFunction` — there
/// is no FEEL AST to hand it) as a native [`Invocable`] instead: invoking it re-evaluates the
/// BKM's own [`Logic`] fresh, with the caller's (already arity/typeRef-gated) arguments bound to
/// its formal parameters over the usual globals overlay (so the BKM's own body can, in turn,
/// bare-call another BKM or decision service). `None` for a BKM this crate doesn't know about, or
/// one whose body IS a plain literal (already handled by the load-time prelude — this function is
/// only ever consulted for a name [`globals_overlay`] didn't already find there).
fn build_bkm_invocable(name: &str, drg: &Arc<Drg>) -> Option<FeelValue> {
    let bkm = drg.bkms.get(name)?;
    if matches!(bkm.body, Logic::Literal(_)) {
        return None;
    }
    let params: Vec<String> = bkm.params.iter().map(|p| p.name.clone()).collect();
    let param_shapes: Vec<FeelTypeShape> = bkm
        .params
        .iter()
        .map(|p| resolve_type_shape(p.type_ref.as_deref(), &drg.item_defs))
        .collect();
    let return_shape = resolve_type_shape(bkm.return_type_ref.as_deref(), &drg.item_defs);
    let drg_arc = Arc::clone(drg);
    let bkm_name = name.to_string();
    let call: Arc<sutra_feel::value::InvocableFn> = Arc::new(move |args: &[FeelValue]| {
        let Some(bkm) = drg_arc.bkms.get(&bkm_name) else {
            return FeelValue::Null;
        };
        let mut call_ctx = globals_overlay(&drg_arc);
        for (p, v) in bkm.params.iter().zip(args) {
            call_ctx.insert(p.name.clone(), v.clone());
        }
        eval_logic(&bkm.body, &call_ctx, &drg_arc).unwrap_or(FeelValue::Null)
    });
    Some(FeelValue::Invocable(Invocable {
        id: name.to_string(),
        params,
        param_shapes,
        return_shape,
        call,
    }))
}

/// Build a native [`Invocable`] for every `<decisionService>` in the model, bound under its own
/// name — lets an ordinary FEEL literal expression call one BY NAME
/// (`decisionService_004()`/`decisionService_006("bar")`/…), exactly like a BKM, even though a
/// decision service's "body" isn't a FEEL AST at all: invoking it re-runs
/// [`Drg::evaluate_decision_service`] with the caller's arguments bound as overrides. Unlike the
/// BKM callable prelude (built ONCE, at load time, into `Drg::prelude`), this can't be a `Drg`
/// FIELD populated during construction — each closure needs to close over a live `Arc<Drg>`
/// pointing at the FULLY-BUILT graph (itself), which doesn't exist yet while `Drg` is still being
/// constructed. Built fresh on every [`globals_overlay`] call instead (cheap: an empty
/// `decision_services` map, the overwhelming majority of the corpus, makes this a no-op loop; a
/// nested indirect call — DMN-TCK 0085#013/0092-feel-lambda#013 — rebuilds this again for its OWN
/// sub-evaluation, bounded by the actual call depth present in the model, never exponential the
/// way cycle 5's BKM-prelude bug was — see that cycle's report).
///
/// Positional-parameter order is `inputData` first, then `inputDecision` (each in their own
/// declared order) — confirmed against DMN-TCK 0085-decision-services#011's own worked example
/// (`decisionService_011("A","B","C","D")` binds `inputData_011_1="A"`, `inputData_011_2="B"`,
/// `decision_011_3="C"`, `decision_011_4="D"`), NOT the `<decisionService>` XML's own element
/// order (which lists `inputDecision`s first) — this ordering is purely the FEEL-callable
/// signature's own convention, unrelated to the XML's declaration order.
fn decision_service_invocables(drg: &Arc<Drg>) -> FeelContext {
    let mut out = FeelContext::new();
    for (name, ds) in &drg.decision_services {
        let param_ids: Vec<&String> = ds
            .input_data
            .iter()
            .chain(ds.input_decisions.iter())
            .collect();
        let params: Vec<String> = param_ids
            .iter()
            .filter_map(|id| drg.id_to_var.get(id.as_str()).cloned())
            .collect();
        // Only proceed if every declared parameter actually resolved to a variable name — an
        // unresolvable id would silently misalign `params/param_shapes`'s positions otherwise.
        if params.len() != param_ids.len() {
            continue;
        }
        let param_shapes: Vec<FeelTypeShape> = param_ids
            .iter()
            .map(|id| resolve_type_shape(drg.type_ref_of(id), &drg.item_defs))
            .collect();
        let return_shape = ds_output_shape(ds.type_ref.as_deref(), &drg.item_defs);
        let drg_arc = Arc::clone(drg);
        let service_name = name.clone();
        let call_params = params.clone();
        let call: Arc<sutra_feel::value::InvocableFn> = Arc::new(move |args: &[FeelValue]| {
            let mut ctx = FeelContext::new();
            for (var, value) in call_params.iter().zip(args) {
                ctx.insert(var.clone(), value.clone());
            }
            let result = drg_arc.evaluate_decision_service(&service_name, &ctx);
            decision_service_call_result(&drg_arc, &service_name, &result)
        });
        out.insert(
            name.clone(),
            FeelValue::Invocable(Invocable {
                id: name.clone(),
                params,
                param_shapes,
                return_shape,
                call,
            }),
        );
    }
    out
}

/// The value an indirect decision-service CALL evaluates to: a single-output service yields that
/// decision's own value directly (DMN13-163, the same rule [`Drg::evaluate_decision_service`]'s
/// own direct-invocation path already applies); a multi-output service yields a context keyed by
/// each output decision's name (mirroring `shape_row`'s multi-output shaping for decision tables —
/// DMN-TCK 0085-decision-services#015). A decision missing from `result` (shouldn't happen for a
/// service this function itself just evaluated) is `null`, not a panic.
fn decision_service_call_result(
    drg: &Drg,
    service_name: &str,
    result: &BTreeMap<String, Result<FeelValue, String>>,
) -> FeelValue {
    let Some(ds) = drg.decision_services.get(service_name) else {
        return FeelValue::Null;
    };
    let value_of = |id: &str| -> FeelValue {
        drg.decision_name(id)
            .and_then(|name| result.get(name))
            .and_then(|r| r.as_ref().ok())
            .cloned()
            .unwrap_or(FeelValue::Null)
    };
    if let [only] = ds.output_decisions.as_slice() {
        return value_of(only);
    }
    let mut map = BTreeMap::new();
    for id in &ds.output_decisions {
        if let Some(name) = drg.decision_name(id) {
            map.insert(name.to_string(), value_of(id));
        }
    }
    FeelValue::Map(map)
}

impl Drg {
    /// Evaluate every decision in dependency order against `inputs` (the leaf input-data values).
    /// Returns one entry per decision, keyed by decision NAME (matching a TCK `resultNode`): `Ok`
    /// with the decision result, or `Err` describing why it could not be evaluated (unsupported
    /// construct, unresolved FEEL, cyclic/missing dependency).
    ///
    /// Takes `self: &Arc<Drg>` (not a plain `&self`) because [`merge_globals`] needs a live,
    /// clonable `Arc<Drg>` to build each `<decisionService>`'s native `Invocable` binding — an
    /// indirect decision-service call (`decisionService_004()`, DMN-TCK 0085-decision-services)
    /// re-runs part of THIS SAME graph from a `'static` closure, which can only hold an owned
    /// `Arc`, never a borrow tied to this call's own stack frame.
    pub fn evaluate(
        self: &Arc<Drg>,
        inputs: &FeelContext,
    ) -> BTreeMap<String, Result<FeelValue, String>> {
        let base = merge_globals(self, inputs);
        self.evaluate_seeded_scoped(&base, HashMap::new(), None)
    }

    /// Evaluate a named `<decisionService>` directly (the TCK's own `type="decisionService"`
    /// test-case shape, AND every indirect call routed through [`merge_globals`]'s native
    /// `Invocable` bindings): `args` supplies the service's declared `inputDecision`/`inputData`
    /// parameters BY THEIR OWN VARIABLE NAME (overriding those nodes' own logic entirely — they
    /// are never recomputed), and the returned map is keyed by decision name exactly like
    /// [`Self::evaluate`] (every decision reachable from the service's outputs, in particular).
    /// An unknown service name yields an empty map.
    pub fn evaluate_decision_service(
        self: &Arc<Drg>,
        service_name: &str,
        args: &FeelContext,
    ) -> BTreeMap<String, Result<FeelValue, String>> {
        let Some(ds) = self.decision_services.get(service_name) else {
            return BTreeMap::new();
        };
        // Scope evaluation to ONLY the service's own outputs and their transitive dependencies —
        // critically NOT every decision in the model. `evaluate_seeded` (below) otherwise runs
        // EVERY decision unconditionally (fine for the top-level `Self::evaluate` entry point,
        // where nothing else in the model can call back into it) — but once a decision service is
        // callable BY NAME from an ordinary decision's own literal expression (`merge_globals`'s
        // native `Invocable` binding), evaluating "every decision" here would re-evaluate the very
        // CALLING decision too, which calls the service again, which evaluates every decision
        // again, … — unconditional infinite recursion (confirmed by an actual stack overflow
        // while implementing this: DMN-TCK 0082-feel-coercion's `ds_invoke_002_with_singleton_list`
        // calls `decisionService_002(...)`, and `decisionService_002` is one output; without this
        // scoping, computing that output re-ran `ds_invoke_002_with_singleton_list` ITSELF as one
        // of "every decision", forever). Scoping to the reachable subgraph is also just correct
        // DMN semantics regardless: a decision service's own evaluation should never depend on —
        // or trigger — unrelated decisions elsewhere in the model.
        let scope = self.reachable_decisions(&ds.output_decisions);
        let mut base = merge_globals(self, args);
        let mut seed: HashMap<String, FeelValue> = HashMap::new();
        for id in ds.input_decisions.iter().chain(ds.input_data.iter()) {
            let Some(var) = self.id_to_var.get(id) else {
                continue;
            };
            let raw = args.get(var).cloned().unwrap_or(FeelValue::Null);
            let shape = resolve_type_shape(self.type_ref_of(id), &self.item_defs);
            let coerced = coerce_to_shape(&raw, &shape).unwrap_or(FeelValue::Null);
            // `result_by_id` (via `seed`) is what an `informationRequirement > requiredDecision`
            // reference binds from — but an `<inputData>` parameter is referenced via
            // `requiredInput`, which is never tracked as a `requires` dependency at all (an
            // ordinary top-level input is already a flat `ctx` key, no dependency-binding needed
            // for that case) — so overriding ONLY `result_by_id` would leave a decision-service
            // `inputData` parameter's own conforming/coerced value invisible to a decision that
            // references it directly (DMN-TCK 0082-feel-coercion#decisionService_002/002_b).
            // Overriding the flat base context too covers both reference styles uniformly.
            base.insert(var.clone(), coerced.clone());
            seed.insert(id.clone(), coerced);
        }
        let mut out = self.evaluate_seeded_scoped(&base, seed, Some(&scope));
        // DMN13-163: a single-output service's own declared typeRef coerces that decision's
        // value (a coercion stage distinct from — and applied ON TOP OF — the decision's own).
        if let [only] = ds.output_decisions.as_slice() {
            if let Some(name) = self.decision_name(only) {
                if let Some(Ok(v)) = out.get(name).cloned() {
                    let shape = ds_output_shape(ds.type_ref.as_deref(), &self.item_defs);
                    out.insert(
                        name.to_string(),
                        Ok(coerce_to_shape(&v, &shape).unwrap_or(FeelValue::Null)),
                    );
                }
            }
        }
        out
    }

    /// The reachable set of decision ids from `roots`, following `requires` transitively
    /// (BFS/DFS, order-independent — a plain reachability set, not itself a topological order).
    /// Used to scope a decision service's own evaluation to its outputs' dependency closure —
    /// see [`Self::evaluate_decision_service`]'s own doc comment on why this matters (avoiding
    /// unconditional infinite recursion once a decision service is callable BY NAME from an
    /// ordinary decision elsewhere in the SAME model).
    fn reachable_decisions(&self, roots: &[String]) -> std::collections::HashSet<String> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut stack: Vec<String> = roots.to_vec();
        while let Some(id) = stack.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            if let Some(d) = self.decisions.iter().find(|d| d.id == id) {
                for r in &d.requires {
                    if !seen.contains(r) {
                        stack.push(r.clone());
                    }
                }
            }
        }
        seen
    }

    /// The shared per-decision evaluation loop: `result_by_id` may be pre-seeded (a decision
    /// service's overridden `inputDecision`s) — a pre-seeded id's decision is emitted as-is
    /// without recomputing its own logic. `scope`, when `Some`, restricts evaluation to ONLY
    /// decisions whose id is in the set (see [`Self::reachable_decisions`]) — every other
    /// decision is skipped entirely (not evaluated, not present in the returned map); `None`
    /// evaluates every decision in the model (the top-level [`Self::evaluate`] entry point).
    fn evaluate_seeded_scoped(
        &self,
        inputs: &FeelContext,
        mut result_by_id: HashMap<String, FeelValue>,
        scope: Option<&std::collections::HashSet<String>>,
    ) -> BTreeMap<String, Result<FeelValue, String>> {
        let order = self.topological_order();
        let mut out: BTreeMap<String, Result<FeelValue, String>> = BTreeMap::new();

        for decision in order {
            if let Some(scope) = scope {
                if !scope.contains(&decision.id) {
                    continue;
                }
            }
            if let Some(v) = result_by_id.get(&decision.id) {
                out.insert(decision.name.clone(), Ok(v.clone()));
                continue;
            }
            let mut ctx = inputs.clone();
            // Bind each satisfied dependency's result by the required decision's variable name.
            for req_id in &decision.requires {
                if let (Some(value), Some(var)) =
                    (result_by_id.get(req_id), self.id_to_var.get(req_id))
                {
                    ctx.insert(var.clone(), value.clone());
                }
            }
            match eval_logic(&decision.logic, &ctx, self) {
                Ok(value) => {
                    let shape = resolve_type_shape(decision.type_ref.as_deref(), &self.item_defs);
                    let value = coerce_to_shape(&value, &shape).unwrap_or(FeelValue::Null);
                    result_by_id.insert(decision.id.clone(), value.clone());
                    out.insert(decision.name.clone(), Ok(value));
                }
                Err(e) => {
                    out.insert(decision.name.clone(), Err(e));
                }
            }
        }
        out
    }

    fn decision_name(&self, id: &str) -> Option<&str> {
        self.decisions
            .iter()
            .find(|d| d.id == id)
            .map(|d| d.name.as_str())
    }

    /// A decision's or inputData's own declared typeRef, by id (both share one id-space).
    fn type_ref_of(&self, id: &str) -> Option<&str> {
        if let Some(d) = self.decisions.iter().find(|d| d.id == id) {
            return d.type_ref.as_deref();
        }
        self.input_type_refs.get(id).and_then(|t| t.as_deref())
    }

    /// Decisions in dependency order (required decisions first). Cyclic / dangling requirements
    /// fall through in document order — those decisions will simply see an unbound variable.
    fn topological_order(&self) -> Vec<&DrgDecision> {
        let mut emitted: Vec<&DrgDecision> = Vec::new();
        let mut done: Vec<&str> = Vec::new();
        let mut remaining: Vec<&DrgDecision> = self.decisions.iter().collect();
        while !remaining.is_empty() {
            let ready_idx = remaining.iter().position(|d| {
                d.requires
                    .iter()
                    .all(|r| done.contains(&r.as_str()) || !self.has_decision(r))
            });
            // A cycle → no decision is "ready"; break it by taking the next in document order.
            let idx = ready_idx.unwrap_or_default();
            let d = remaining.remove(idx);
            done.push(&d.id);
            emitted.push(d);
        }
        emitted
    }

    fn has_decision(&self, id: &str) -> bool {
        self.decisions.iter().any(|d| d.id == id)
    }
}

/// A resolver from a DMN `<itemDefinition>` name to its structural [`FeelTypeShape`] — fed to
/// [`expressions::eval_with_type_resolver`] so a literal expression's `instance of
/// <ItemDefinitionName>` can recurse into THIS model's own custom types (DMN-TCK
/// 0070-feel-instance-of `number_013`/`string_013`/`list_013`/`list_014`/`list_014_a`/
/// `context_013`/`context_014`). Only ever resolves a name genuinely declared as an
/// `<itemDefinition>` here — never falls back to treating an unrecognized name as a bare base
/// type, which would recurse forever through `resolve_named`'s own "absent ⇒ `Base(name)`"
/// fallback (that fallback is exactly right for typeRef COERCION — an unmodeled name conforms
/// unconditionally — but would be an infinite loop if fed back into this resolver).
fn type_resolver(drg: &Drg) -> impl Fn(&str) -> Option<FeelTypeShape> + '_ {
    move |name: &str| {
        if drg.item_defs.contains_key(name) {
            Some(resolve_named(name, &drg.item_defs, 0))
        } else {
            None
        }
    }
}

fn eval_logic(logic: &Logic, ctx: &FeelContext, drg: &Drg) -> Result<FeelValue, String> {
    match logic {
        Logic::Literal(text) => {
            let resolver = type_resolver(drg);
            expressions::eval_with_type_resolver(text, ctx, &resolver).map_err(|e| e.to_string())
        }
        Logic::Table(table, aggregation) => eval_table(table, *aggregation, ctx),
        Logic::Context(entries) => eval_context(entries, ctx, drg),
        Logic::Invocation {
            bkm,
            bindings,
            type_ref,
        } => eval_invocation(bkm, bindings, type_ref.as_deref(), ctx, drg),
        Logic::Unsupported(why) => Err(format!("unsupported: {why}")),
    }
}

/// Evaluate a decision table to its DMN *decision result* (not the validator's verdict): reuse the
/// shared firing core, then shape the winning rule(s) per hit policy.
fn eval_table(
    table: &DmnDecisionTable,
    aggregation: Option<Aggregation>,
    ctx: &FeelContext,
) -> Result<FeelValue, String> {
    let mut firings: Vec<Vec<EvaluatedOutput>> = Vec::new();
    for rule in &table.rules {
        if rule.input_entries.len() != table.inputs.len() {
            continue;
        }
        if rule_fires(rule, table, ctx) {
            let outputs = evaluate_outputs(rule, table, ctx);
            let single_hit = !DmnRulesetValidator::returns_list(table.hit_policy);
            // PRIORITY must see every firing rule to find the true output-value winner — unlike
            // UNIQUE/FIRST/ANY, it can't stop at the first document-order match (DMN-TCK
            // 0007-simpletable-P2).
            let stop_early = single_hit && table.hit_policy != HitPolicy::Priority;
            firings.push(outputs);
            if stop_early {
                break;
            }
        }
    }
    match table.hit_policy {
        // COLLECT with an aggregator collapses to a scalar; bare COLLECT yields the list. A
        // bare/SUM/COUNT collapse of zero rows is well-defined ([]/0/0) without consulting any
        // default; only MIN/MAX over zero rows is genuinely undefined, so that's the one case
        // that falls through to the table's own `<defaultOutputEntry>` (DMN-TCK
        // 0020-vacation-days's three COLLECT-MAX tables all declare one).
        HitPolicy::Collect => {
            if firings.is_empty()
                && matches!(aggregation, Some(Aggregation::Min | Aggregation::Max))
            {
                return no_rule_fired(table, ctx);
            }
            let rows: Vec<FeelValue> = firings.iter().map(|o| shape_row(o, table)).collect();
            match aggregation {
                Some(agg) => aggregate(agg, &rows),
                None => Ok(FeelValue::List(rows)),
            }
        }
        HitPolicy::RuleOrder => Ok(FeelValue::List(
            firings.iter().map(|o| shape_row(o, table)).collect(),
        )),
        // OUTPUT_ORDER: unlike RULE_ORDER, the collected rows are sorted by the winning output's
        // position in its `<outputValues>` priority list (DMN-TCK 0110/0113-outputOrder-
        // hitpolicy) — the same ranking [`DmnRulesetValidator::apply_output_order`] already
        // applies for the validator-verdict path; a stable sort preserves rule order among ties.
        // No declared priority list at all ⇒ nothing to rank by, so document order stands
        // (mirrors the validator path's own COLLECT fallback for that case).
        HitPolicy::OutputOrder => {
            if let Some(priority_clause) = first_output_with_priority_list(table) {
                firings.sort_by_key(|o| rank_of_output(o, priority_clause));
            }
            Ok(FeelValue::List(
                firings.iter().map(|o| shape_row(o, table)).collect(),
            ))
        }
        HitPolicy::Priority => {
            let winner = match first_output_with_priority_list(table) {
                Some(priority_clause) => firings
                    .into_iter()
                    .min_by_key(|o| rank_of_output(o, priority_clause)),
                // No declared <outputValues> priority list to rank by: fall back to the first
                // firing rule (document order) — the decision-result equivalent of the validator
                // path's own UNIQUE fallback.
                None => firings.into_iter().next(),
            };
            match winner {
                Some(o) => Ok(shape_row(&o, table)),
                None => no_rule_fired(table, ctx),
            }
        }
        _ => match firings.into_iter().next() {
            Some(o) => Ok(shape_row(&o, table)),
            None => no_rule_fired(table, ctx),
        },
    }
}

/// "No rule fired" fallback: the table's own `<defaultOutputEntry>` (DMN § 8.2.4) if it declares
/// one, else the pre-existing `"no rule fired"` error (unchanged — an UNSUPPORTED allowlist entry
/// in the TCK harness, not a hard failure).
fn no_rule_fired(table: &DmnDecisionTable, ctx: &FeelContext) -> Result<FeelValue, String> {
    default_row(table, ctx).ok_or_else(|| "no rule fired".to_string())
}

/// The table's declared default output value(s) for when no rule fires — `None` when NO output
/// clause declares a `<defaultOutputEntry>` at all (in which case "no rule fired" stays a
/// conformance-visible gap, unchanged). A single-output table yields that bare value; a
/// multi-output table a context keyed by output name (mirroring [`shape_row`]'s own shaping), with
/// any column lacking its own default entry falling back to `null`.
fn default_row(table: &DmnDecisionTable, ctx: &FeelContext) -> Option<FeelValue> {
    if table.outputs.iter().all(|o| o.default_output.is_none()) {
        return None;
    }
    let values: Vec<FeelValue> = table
        .outputs
        .iter()
        .map(|o| {
            o.default_output
                .as_deref()
                .and_then(|text| expressions::eval(text, ctx).ok())
                .unwrap_or(FeelValue::Null)
        })
        .collect();
    if table.outputs.len() <= 1 {
        return Some(values.into_iter().next().unwrap_or(FeelValue::Null));
    }
    let mut map = BTreeMap::new();
    for (i, (o, v)) in table.outputs.iter().zip(values).enumerate() {
        let key = o.name.clone().unwrap_or_else(|| format!("output{}", i + 1));
        map.insert(key, v);
    }
    Some(FeelValue::Map(map))
}

/// Collapse COLLECTed rows to a scalar per the `aggregation` attribute (DMN § 8.2.10).
fn aggregate(agg: Aggregation, rows: &[FeelValue]) -> Result<FeelValue, String> {
    match agg {
        Aggregation::Count => Ok(FeelValue::Number(BigDecimal::from(rows.len() as i64))),
        Aggregation::Sum => {
            let mut sum = BigDecimal::from(0i64);
            for row in rows {
                match row {
                    FeelValue::Number(n) => sum += n.clone(),
                    other => return Err(format!("COLLECT SUM over a non-number: {other:?}")),
                }
            }
            Ok(FeelValue::Number(sum))
        }
        Aggregation::Min | Aggregation::Max => {
            let mut chosen: Option<&BigDecimal> = None;
            for row in rows {
                let FeelValue::Number(n) = row else {
                    return Err(format!("COLLECT MIN/MAX over a non-number: {row:?}"));
                };
                let take = match chosen {
                    None => true,
                    Some(c) => {
                        let ord = n.partial_cmp(c).unwrap_or(std::cmp::Ordering::Equal);
                        matches!(
                            (agg, ord),
                            (Aggregation::Min, std::cmp::Ordering::Less)
                                | (Aggregation::Max, std::cmp::Ordering::Greater)
                        )
                    }
                };
                if take {
                    chosen = Some(n);
                }
            }
            chosen
                .map(|n| FeelValue::Number(n.clone()))
                .ok_or_else(|| "COLLECT MIN/MAX over an empty result".to_string())
        }
    }
}

/// A firing rule's outputs → the decision-result value for one row: a single output clause yields
/// its bare value; multiple clauses a context keyed by output name.
fn shape_row(outputs: &[EvaluatedOutput], table: &DmnDecisionTable) -> FeelValue {
    if table.outputs.len() <= 1 {
        return outputs
            .first()
            .map(|o| o.value.clone())
            .unwrap_or(FeelValue::Null);
    }
    let mut map = BTreeMap::new();
    for (i, o) in outputs.iter().enumerate() {
        let key = o
            .clause
            .name
            .clone()
            .unwrap_or_else(|| format!("output{}", i + 1));
        map.insert(key, o.value.clone());
    }
    FeelValue::Map(map)
}

fn eval_context(
    entries: &[ContextEntry],
    ctx: &FeelContext,
    drg: &Drg,
) -> Result<FeelValue, String> {
    let mut local = ctx.clone();
    let mut map = BTreeMap::new();
    let mut result_entry: Option<FeelValue> = None;
    for entry in entries {
        let value = eval_logic(&entry.logic, &local, drg)?;
        let shape = resolve_type_shape(entry.type_ref.as_deref(), &drg.item_defs);
        let value = coerce_to_shape(&value, &shape).unwrap_or(FeelValue::Null);
        match &entry.name {
            Some(name) => {
                local.insert(name.clone(), value.clone());
                map.insert(name.clone(), value);
            }
            None => result_entry = Some(value), // the boxed context's final "result" entry
        }
    }
    Ok(result_entry.unwrap_or(FeelValue::Map(map)))
}

fn eval_invocation(
    bkm_name: &str,
    bindings: &[(String, Logic)],
    inv_type_ref: Option<&str>,
    ctx: &FeelContext,
    drg: &Drg,
) -> Result<FeelValue, String> {
    let bkm = drg
        .bkms
        .get(bkm_name)
        .ok_or_else(|| format!("invocation of unknown BKM '{bkm_name}'"))?;
    // An external (kind="Java"/"PMML") BKM: same invocation-time rejection the bare-call path
    // gets from the evaluator, in the same "[code] message" shape (`bkm.body` here is the raw
    // binding context — evaluating it would wrongly yield the binding itself as the result).
    if let Some(binding) = &bkm.external {
        return Err(format!(
            "[{}] {}",
            sutra_feel::codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED,
            binding.rejection_message()
        ));
    }
    // Seeded from the callable prelude (not empty) so the BKM's own body can in turn call other
    // BKMs by bare name (DMN-TCK 0034-drg-scopes).
    let mut call_ctx = drg.prelude.clone();
    for (pname, arg_logic) in bindings {
        // A binding name that isn't one of the BKM's declared formal parameters: the BKM is
        // never invoked (DMN-TCK 0082-feel-coercion#invoke_007).
        let Some(param) = bkm.params.iter().find(|p| &p.name == pname) else {
            return Ok(FeelValue::Null);
        };
        let value = eval_logic(arg_logic, ctx, drg)?;
        let shape = resolve_type_shape(param.type_ref.as_deref(), &drg.item_defs);
        let Some(coerced) = coerce_to_shape(&value, &shape) else {
            // The bound argument doesn't conform to the formal parameter's declared typeRef: the
            // BKM is never invoked (DMN-TCK 0082-feel-coercion#decision_bkm_002/invoke_001).
            return Ok(FeelValue::Null);
        };
        call_ctx.insert(param.name.clone(), coerced.clone());
        // DMN-TCK 0037-dt-on-bkm-implicit-params: a dotted formal-parameter name (`Person.Gender`)
        // must also be navigable from INSIDE the BKM's own body via ordinary FEEL member access
        // (its `<inputExpression>` text is, verbatim, `Person.Gender`) — the FEEL lexer always
        // tokenizes a bare `.` as navigation (there is no dot-aware "names with spaces"-style
        // merge pass for it), so that text parses as `Person` (member) `.Gender`, needing an
        // actual nested `Person -> {Gender: ..}` map in `call_ctx`, not just the flat key above
        // (kept too, harmlessly, in case anything else ever looks it up as one literal key).
        // Only a single dot level is folded — the observed pattern (`Person.Gender`/`Person.Name`/
        // `Person.Children`) never nests deeper.
        if let Some((head, rest)) = param.name.split_once('.') {
            let mut nested = match call_ctx.get(head) {
                Some(FeelValue::Map(m)) => m.clone(),
                _ => BTreeMap::new(),
            };
            nested.insert(rest.to_string(), coerced);
            call_ctx.insert(head.to_string(), FeelValue::Map(nested));
        }
    }
    let result = eval_logic(&bkm.body, &call_ctx, drg)?;
    let return_shape = resolve_type_shape(bkm.return_type_ref.as_deref(), &drg.item_defs);
    let result = coerce_to_shape(&result, &return_shape).unwrap_or(FeelValue::Null);
    let inv_shape = resolve_type_shape(inv_type_ref, &drg.item_defs);
    Ok(coerce_to_shape(&result, &inv_shape).unwrap_or(FeelValue::Null))
}
