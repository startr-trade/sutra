//! Small pure text/path helpers. Kept dependency-free and deterministic (mirrors the sibling
//! `sutra-catalog-gen` crate's `util.rs` conventions, duplicated rather than shared — the two
//! generators are deliberately independent, see the crate-level docs).

/// Escape a value for a Markdown table cell (pipes + hard line breaks only).
pub fn cell(s: &str) -> String {
    let collapsed = s.replace('\r', "").replace('\n', "<br>");
    collapsed.replace('|', "\\|")
}

/// Render an `Option<&str>` for a table cell, with a fixed placeholder for absence.
pub fn opt_cell(s: Option<&str>) -> String {
    match s {
        Some(v) if !v.trim().is_empty() => cell(v),
        _ => "—".to_string(),
    }
}

/// POSIX relative link from one doc-root-relative page path to another. Both inputs are POSIX
/// paths relative to the catalog output root.
pub fn rel_link(from: &str, to: &str) -> String {
    let from_dir: Vec<&str> = {
        let mut v: Vec<&str> = from.split('/').collect();
        v.pop(); // drop the file component
        v
    };
    let to_parts: Vec<&str> = to.split('/').collect();

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
        out.push_str(to_parts.last().copied().unwrap_or(""));
    }
    out
}

/// Basename (final path segment) of a POSIX-style relative path.
pub fn basename(rel: &str) -> &str {
    rel.rsplit('/').next().unwrap_or(rel)
}

/// Replace a file's extension with `.md` (`bpmn/foo.bpmn` -> `bpmn/foo.md`).
pub fn with_md_ext(rel: &str) -> String {
    with_ext(rel, "md")
}

/// Swap `rel`'s extension for `ext` (no dot). Used for a page's sidecar assets — the BPMN
/// diagram's standalone `.svg` sits beside its `.md`, so the page can link it by bare filename.
pub fn with_ext(rel: &str, ext: &str) -> String {
    match rel.rsplit_once('.') {
        Some((stem, _old)) => format!("{stem}.{ext}"),
        None => format!("{rel}.{ext}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_escapes_pipes_and_newlines() {
        assert_eq!(cell("a | b"), "a \\| b");
        assert_eq!(cell("line1\nline2"), "line1<br>line2");
    }

    #[test]
    fn rel_link_same_dir_and_cross_tree() {
        assert_eq!(rel_link("pkg/index.md", "pkg/bpmn/foo.md"), "bpmn/foo.md");
        assert_eq!(rel_link("pkg/bpmn/foo.md", "pkg/index.md"), "../index.md");
        assert_eq!(
            rel_link("a/pkg/index.md", "b/pkg/index.md"),
            "../../b/pkg/index.md"
        );
    }

    #[test]
    fn with_md_ext_swaps_extension() {
        assert_eq!(with_md_ext("bpmn/foo.bpmn"), "bpmn/foo.md");
        assert_eq!(with_md_ext("channels.yaml"), "channels.md");
    }
}
