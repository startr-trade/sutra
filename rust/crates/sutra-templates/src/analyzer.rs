//! Static analysis of a Handlebars (`.hbs`) template for the deploy-time type-safety check.
//! Reports the external root of each reference and, for
//! `payload`-rooted references, the literal dotted field path (bracket segments `.[71A]`
//! normalised to plain `.71A` segments). Helper names, block parameters, the implicit context
//! (`this` / `.`) and `@`-data variables are excluded.

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

use regex::Regex;

/// Roots + payload paths + unresolvable constructs a template references (the
/// `TemplateAnalysis` shape, minus the unused fixed-values slot).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TemplateAnalysis {
    /// External model roots, in first-reference order.
    pub roots: Vec<String>,
    /// Literal dotted field paths under the `payload` root, in first-reference order.
    pub payload_paths: Vec<String>,
    /// Literal dotted field paths grouped by their root (including `payload`), in reference order.
    /// Used by SSI to field-check `<q:variable schema=…>`-typed variable-rooted reads
    /// (`{{var.field}}`) against the variable's bound shape — the general form of `payload_paths`.
    pub root_paths: BTreeMap<String, Vec<String>>,
    /// Constructs the analyzer could not tie to a concrete field — a dynamic / computed key
    /// (e.g. `{{lookup obj key}}` where `key` is not a literal). Raw construct text, in
    /// first-reference order, deduped. The deploy-time type-safety check treats each as a hard
    /// error ("not statically validatable"): ambiguity is surfaced, never silently allowed.
    pub unresolvable: Vec<String>,
}

/// Every helper the engine registers plus the handlebars built-ins — never model roots.
const HELPERS: &[&str] = &[
    "each",
    "with",
    "if",
    "unless",
    "let",
    "lookup",
    "log",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "and",
    "or",
    "not",
    "substring",
    "replace",
    "coalesce",
    "else",
];

fn is_helper(name: &str) -> bool {
    HELPERS.contains(&name)
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static regex compiles"))
}

/// A var tag head: `{{payload.E2EId}}` / `{{{raw}}}` / `{{substring dt 2 4}}` (head only).
fn var_tag() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"\{\{\{?\s*([A-Za-z_@][\w.@\[\]-]*)")
}

/// A block/inverted tag: `{{#head arg}}` — head is a helper or a direct-section data ref.
fn block_tag() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(
        &CELL,
        r"\{\{[#^]\s*([A-Za-z_@][\w.@-]*)(?:\s+([A-Za-z_@][\w.@\[\]-]*))?",
    )
}

/// A value-helper tag: `{{substring dt 2 4}}` — the path arguments are model refs.
fn value_helper() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(
        &CELL,
        r"\{\{\s*(?:substring|replace|coalesce|lookup)\s+([^}]*)\}\}",
    )
}

/// A conditional subexpression: `(eq tx.ChrgBr "DEBT")` — the path arguments are model refs.
fn subexpr() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(
        &CELL,
        r"\(\s*(?:eq|neq|gt|gte|lt|lte|and|or|not)\s+([^)]*)\)",
    )
}

/// A dotted/bracketed path token inside an argument list.
fn path_token() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"[A-Za-z_@][\w@-]*(?:\.(?:[\w@-]+|\[[^\]]*\]))*")
}

fn block_params_re() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"as\s*\|([^|]+)\|")
}

fn quoted_literal() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r#""[^"]*"|'[^']*'"#)
}

/// A `lookup` helper invocation with its argument list — either the inline tag `{{lookup …}}`
/// or a subexpression `(lookup …)`. Group 1 is everything up to the first closing `}`/`)`.
fn lookup_call() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"\blookup\s+([^})]*)")
}

/// A handlebars comment: `{{!-- … --}}` (matched non-greedily, so a comment CONTAINING `}}` —
/// the documented-example case — is removed whole rather than up to its first inner brace).
fn block_comment() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"(?s)\{\{!--.*?--\}\}")
}

/// The short comment form: `{{! … }}`.
fn short_comment() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    re(&CELL, r"\{\{![^}]*\}\}")
}

/// Remove comments before the token scans run. Byte offsets do not need preserving — nothing
/// downstream reports positions from this string.
fn strip_comments(src: &str) -> String {
    let once = block_comment().replace_all(src, "");
    short_comment().replace_all(&once, "").into_owned()
}

pub(crate) fn analyze(template: &[u8]) -> TemplateAnalysis {
    let src = String::from_utf8_lossy(template).into_owned();
    // Parse error — the render path surfaces it; analysis is empty.
    if handlebars::template::Template::compile(&src).is_err() {
        return TemplateAnalysis::default();
    }

    // Comments are not references. `{{!-- … --}}` and `{{! … }}` never render, so anything
    // inside them cannot read data — but the scans below are regex-driven over the raw source
    // and would have counted it. That made PROSE load-bearing: a template documenting its own
    // shape ("navigate its fields, e.g. {{payload.someField}}") produced a field reference, and
    // once the codec exposed a schema the comment became a hard FIELD_UNKNOWN error.
    let src = strip_comments(&src);
    let locals = block_params(&src);
    let mut refs: Vec<String> = Vec::new();

    // Var tags (the `{{var}}` / `{{{triple}}}` tags): head token of every
    // non-block tag; helper heads are excluded (their arguments are read below).
    for cap in var_tag().captures_iter(&src) {
        let name = &cap[1];
        if !is_helper(name) {
            refs.push(name.to_string());
        }
    }
    // Block/inverted tags: the DATA ref is the helper argument, or the head for a direct
    // section ({{#payload.Grp}}).
    for cap in block_tag().captures_iter(&src) {
        let head = &cap[1];
        let arg = cap.get(2).map(|m| m.as_str());
        let reference = if is_helper(head) { arg } else { Some(head) };
        if let Some(r) = reference {
            if !r.is_empty() && !is_helper(r) {
                refs.push(r.to_string());
            }
        }
    }
    collect_arg_refs(value_helper(), &src, &mut refs);
    collect_arg_refs(subexpr(), &src, &mut refs);

    let mut roots: Vec<String> = Vec::new();
    let mut payload_paths: Vec<String> = Vec::new();
    let mut root_paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for r in refs {
        let segments = segments(&r);
        let Some(root_name) = segments.first() else {
            continue;
        };
        if root_name.is_empty()
            || root_name.starts_with('@')
            || root_name == "this"
            || locals.contains(root_name)
        {
            continue; // @-data vars, the implicit context, and block params are not roots
        }
        if !roots.contains(root_name) {
            roots.push(root_name.clone());
        }
        if segments.len() > 1 {
            let path = segments[1..].join(".");
            let bucket = root_paths.entry(root_name.clone()).or_default();
            if !bucket.contains(&path) {
                bucket.push(path.clone());
            }
            // `payload_paths` is kept as the dedicated payload view (byte-identical to before).
            if root_name == "payload" && !payload_paths.contains(&path) {
                payload_paths.push(path);
            }
        }
    }
    TemplateAnalysis {
        roots,
        payload_paths,
        root_paths,
        unresolvable: dynamic_lookups(&src),
    }
}

/// Surface `{{lookup <collection> <key>}}` (or `(lookup …)`) invocations whose key argument is
/// not a literal — a dynamic / computed key whose resulting field cannot be tied to a concrete
/// schema path. A literal key (a quoted string or a number) is statically resolvable and never
/// reported. Returns the raw construct text in first-reference order, deduped.
fn dynamic_lookups(src: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for cap in lookup_call().captures_iter(src) {
        let args = cap[1].trim();
        let tokens = split_args(args);
        // `lookup collection key` — the collection (arg 0) is an ordinary path ref; only the key
        // (arg 1) determines static resolvability. Fewer than two args is an incomplete call the
        // render/compile path handles, not an unresolvable construct.
        if tokens.len() >= 2 && !is_literal_arg(&tokens[1]) {
            let construct = format!("{{{{lookup {args}}}}}");
            if !out.contains(&construct) {
                out.push(construct);
            }
        }
    }
    out
}

/// Split a helper argument list into tokens on whitespace, keeping quoted strings intact.
fn split_args(args: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in args.chars() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                cur.push(c);
            }
            None if c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            None => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// A statically-resolvable lookup key: a quoted string literal or a numeric literal. Anything
/// else (a path, an identifier, an `@`-data var, a subexpression) is a dynamic / computed key.
fn is_literal_arg(token: &str) -> bool {
    let quoted = |q: char| token.len() >= 2 && token.starts_with(q) && token.ends_with(q);
    quoted('"') || quoted('\'') || token.parse::<f64>().is_ok()
}

fn collect_arg_refs(tags: &Regex, src: &str, refs: &mut Vec<String>) {
    for cap in tags.captures_iter(src) {
        // Strip quoted string literals, then collect the remaining path tokens.
        let args = quoted_literal().replace_all(&cap[1], " ");
        for token in path_token().find_iter(&args) {
            let r = token.as_str();
            if !is_helper(r) {
                refs.push(r.to_string());
            }
        }
    }
}

/// Splits a ref into path segments, unwrapping bracket segments:
/// `f.[71A].code` → `f, 71A, code`. A dot inside a bracket segment is part of the key.
fn segments(reference: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_bracket = false;
    for c in reference.chars() {
        match c {
            '[' if !in_bracket => in_bracket = true,
            ']' if in_bracket => in_bracket = false,
            '.' if !in_bracket => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn block_params(src: &str) -> HashSet<String> {
    let mut params = HashSet::new();
    for cap in block_params_re().captures_iter(src) {
        for p in cap[1].split_whitespace() {
            params.insert(p.to_string());
        }
    }
    params
}
