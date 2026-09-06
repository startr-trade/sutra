//! Small pure text/path helpers. Kept dependency-free and deterministic.

use quote::ToTokens;
use syn::Attribute;

/// Collapse all runs of ASCII whitespace to single spaces and trim the ends.
pub fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Escape a value for a Markdown table cell (pipes only; whitespace is already collapsed).
pub fn cell(s: &str) -> String {
    s.replace('|', "\\|")
}

/// First paragraph of a doc comment built from `#[doc = "…"]` attributes.
///
/// Doc comments desugar line-by-line, so we join the attribute values with newlines, then take
/// the run of non-blank lines up to the first blank line, join with spaces, and collapse
/// whitespace. Returns `None` when there is no doc content.
pub fn doc_first_paragraph(attrs: &[Attribute]) -> Option<String> {
    let mut raw = String::new();
    for a in attrs {
        if a.path().is_ident("doc") {
            if let Some(v) = doc_value(a) {
                if !raw.is_empty() {
                    raw.push('\n');
                }
                raw.push_str(&v);
            }
        }
    }
    if raw.is_empty() {
        return None;
    }
    let mut para: Vec<&str> = Vec::new();
    let mut started = false;
    for line in raw.split('\n') {
        let t = line.trim();
        if t.is_empty() {
            if started {
                break;
            }
            continue;
        }
        started = true;
        para.push(t);
    }
    if para.is_empty() {
        return None;
    }
    let joined = collapse_ws(&para.join(" "));
    if joined.is_empty() {
        None
    } else {
        Some(demote_escaping_links(&joined))
    }
}

/// Demote any Markdown link whose target escapes the source tree into `docs/**` (a design doc)
/// to a plain code span, dropping the `[…](…)` wrapper but keeping the link text as-is.
///
/// A doc comment is free to link a design doc relative to its *own* file
/// (e.g. `../../../../docs/design/foo.md` from four levels under the repo root); that link
/// resolves fine in a source viewer, but the catalog copies the doc text verbatim into a page
/// nested under the catalog output root — a different depth under a different
/// root — so the same relative path no longer resolves there, and never can without knowing the
/// page's eventual location. Every other design-doc reference in the workspace already uses a
/// plain code span for exactly this reason (see `rust/crates/sutra-loader/src/coverage.rs`'s
/// sibling references); this brings a stray Markdown link into line with that convention instead
/// of emitting a `mkdocs --strict`-breaking dangling link.
fn demote_escaping_links(doc: &str) -> String {
    let mut out = String::with_capacity(doc.len());
    let mut rest = doc;
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        let Some(close_rel) = after_open.find(']') else {
            out.push_str(&rest[open..]);
            rest = "";
            break;
        };
        let text = &after_open[..close_rel];
        let after_text = &after_open[close_rel + 1..];
        if let Some(paren_rest) = after_text.strip_prefix('(') {
            if let Some(target_end) = paren_rest.find(')') {
                let target = &paren_rest[..target_end];
                if escapes_into_docs_tree(target) {
                    out.push_str(text);
                    rest = &paren_rest[target_end + 1..];
                    continue;
                }
            }
        }
        // Not a demotable link — keep the literal `[` and resume right after it so scanning
        // makes progress and any later `]`/`(`/`)` in `after_open` is still considered fresh.
        out.push('[');
        rest = after_open;
    }
    out.push_str(rest);
    out
}

/// True when a Markdown link target is a relative path that climbs out to the repo root (a run
/// of `../`) and then descends into `docs/` — i.e. a design-doc reference authored relative to
/// the source file's own location, which the catalog cannot keep resolvable once the doc text is
/// relocated into the generated page tree.
fn escapes_into_docs_tree(target: &str) -> bool {
    let t = target.split('#').next().unwrap_or(target);
    let mut stripped = t;
    let mut climbed = false;
    while let Some(rest) = stripped.strip_prefix("../") {
        stripped = rest;
        climbed = true;
    }
    climbed && stripped.starts_with("docs/")
}

/// Extract the string value of a `#[doc = "…"]` attribute.
fn doc_value(attr: &Attribute) -> Option<String> {
    if let syn::Meta::NameValue(nv) = &attr.meta {
        if let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(s),
            ..
        }) = &nv.value
        {
            return Some(s.value());
        }
    }
    None
}

/// Render a `syn` node's tokens to a compact, human-readable one-line string.
///
/// `TokenStream`'s `Display` inserts a space between every token; we squeeze the spacing around
/// the punctuation that shows up in signatures/types so `fn f (a : & str) -> Vec < T >` reads as
/// `fn f(a: &str) -> Vec<T>`.
pub fn tidy_tokens<T: ToTokens>(node: &T) -> String {
    let mut out = collapse_ws(&node.to_token_stream().to_string());
    // Order matters: collapse path separators first so the type-annotation colon rule
    // (` : ` → `: `) never touches a `::`.
    for (from, to) in [
        (" ::", "::"),
        (":: ", "::"),
        (" : ", ": "),
        (" ,", ","),
        (" ;", ";"),
        (" (", "("),
        ("( ", "("),
        (" )", ")"),
        (" [", "["),
        ("[ ", "["),
        (" ]", "]"),
        (" <", "<"),
        ("< ", "<"),
        (" >", ">"),
        ("& ", "&"),
    ] {
        out = out.replace(from, to);
    }
    out
}

/// Render a `syn::Visibility` to its source form; `Inherited` (private) renders as `""`.
pub fn vis_str(vis: &syn::Visibility) -> String {
    match vis {
        syn::Visibility::Public(_) => "pub".to_string(),
        syn::Visibility::Restricted(_) => tidy_tokens(vis),
        syn::Visibility::Inherited => String::new(),
    }
}

/// POSIX relative link from one doc-root-relative page path to another.
///
/// Both inputs are POSIX paths relative to the catalog output root (e.g.
/// `rust/crates/sutra-loader/src/manifest.md`). The result is a link usable from `from`'s
/// directory (e.g. `../error.md`).
pub fn rel_link(from: &str, to: &str) -> String {
    let from_dir: Vec<&str> = {
        let mut v: Vec<&str> = from.split('/').collect();
        v.pop(); // drop the file component
        v
    };
    let to_parts: Vec<&str> = to.split('/').collect();

    // Longest common directory prefix.
    let mut common = 0;
    while common < from_dir.len()
        && common + 1 < to_parts.len()
        && from_dir[common] == to_parts[common]
    {
        common += 1;
    }

    let ups = from_dir.len() - common;
    let mut out = String::new();
    for _ in 0..ups {
        out.push_str("../");
    }
    out.push_str(&to_parts[common..].join("/"));
    if out.is_empty() {
        // Linking a page to itself — degenerate, but keep it valid.
        out.push_str(to_parts.last().copied().unwrap_or(""));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_link_same_dir_sibling_and_cross_tree() {
        assert_eq!(
            rel_link("rust/crates/a/src/x.md", "rust/crates/a/src/y.md"),
            "y.md"
        );
        assert_eq!(
            rel_link("rust/crates/a/src/x.md", "rust/crates/a/Cargo.md"),
            "../Cargo.md"
        );
        assert_eq!(
            rel_link("rust/crates/a/src/x.md", "rust/crates/b/src/y.md"),
            "../../b/src/y.md"
        );
        assert_eq!(
            rel_link("rust/Cargo.md", "rust/crates/a/Cargo.md"),
            "crates/a/Cargo.md"
        );
        assert_eq!(
            rel_link("rust/crates/a/src/deep/x.md", "rust/crates/a/src/lib.md"),
            "../lib.md"
        );
    }

    #[test]
    fn doc_first_paragraph_demotes_a_design_doc_link_to_a_code_span() {
        let file: syn::File = syn::parse_str(
            "//! A design note (design \
             [`a-design-note.md`](../../../../docs/design/a-design-note.md), \
             §4.2).\npub struct S;",
        )
        .unwrap();
        assert_eq!(
            doc_first_paragraph(&file.attrs).as_deref(),
            Some(
                "A design note (design `a-design-note.md`, \
                 §4.2)."
            )
        );
    }

    #[test]
    fn doc_first_paragraph_leaves_an_in_catalog_link_untouched() {
        let file: syn::File = syn::parse_str(
            "/// See [`Other`](crate::other::Other) and [crates.io](https://crates.io/x).\npub struct S;",
        )
        .unwrap();
        let syn::Item::Struct(s) = &file.items[0] else {
            panic!("expected struct");
        };
        assert_eq!(
            doc_first_paragraph(&s.attrs).as_deref(),
            Some("See [`Other`](crate::other::Other) and [crates.io](https://crates.io/x).")
        );
    }

    #[test]
    fn escapes_into_docs_tree_requires_a_climb_then_docs_prefix() {
        assert!(escapes_into_docs_tree(
            "../../../../docs/design/a-design-note.md"
        ));
        assert!(escapes_into_docs_tree("../docs/design/x.md#L1"));
        assert!(!escapes_into_docs_tree("docs/design/x.md")); // no climb — not a source-relative escape
        assert!(!escapes_into_docs_tree("../../rust/crates/a/src/b.rs"));
        assert!(!escapes_into_docs_tree("crate::other::Other"));
    }

    #[test]
    fn doc_first_paragraph_stops_at_blank_line() {
        let file: syn::File = syn::parse_str(
            "/// First line.\n/// Continuation.\n///\n/// Second paragraph.\npub struct S;",
        )
        .unwrap();
        let syn::Item::Struct(s) = &file.items[0] else {
            panic!("expected struct");
        };
        assert_eq!(
            doc_first_paragraph(&s.attrs).as_deref(),
            Some("First line. Continuation.")
        );
    }

    #[test]
    fn tidy_tokens_renders_compact_signatures() {
        let sig: syn::Signature =
            syn::parse_str("fn f(a: &str, b: Vec<u8>) -> std::cmp::Ordering").unwrap();
        assert_eq!(
            tidy_tokens(&sig),
            "fn f(a: &str, b: Vec<u8>) -> std::cmp::Ordering"
        );
    }

    #[test]
    fn cell_escapes_pipes() {
        assert_eq!(cell("a | b"), "a \\| b");
    }
}
