//! Caller hooks on rendered elements: attribute escaping, element ids, and the checks
//! [`crate::Options::validate`] reports.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{Model, NodeKind, Process};

/// Escape a value for a double-quoted XML attribute.
///
/// `#` is escaped too: the theming pass rewrites colour literals such as `"#333"` inside tags, and
/// a caller's value must never be mistaken for one.
pub(crate) fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            '#' => out.push_str("&#35;"),
            _ => out.push(c),
        }
    }
    out
}

/// The element id a rendered group gets for the BPMN `id`: `prefix` followed by the id with every
/// character outside `[A-Za-z0-9_-]` replaced by `_`.
///
/// This is the stable rule for a model whose sanitised ids do not collide. When two ids sanitise
/// to the same string (`a.b` and `a_b`), the later one in document order gets a `-2`, `-3`, …
/// suffix; [`Model::element_ids`] gives the exact ids a render uses, collisions included.
pub fn element_id(prefix: &str, id: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + id.len());
    out.push_str(prefix);
    out.extend(id.chars().map(|c| if is_class_char(c) { c } else { '_' }));
    out
}

fn is_class_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// A caller class is kept when it is non-empty and every character is in `[A-Za-z0-9_-]`.
pub(crate) fn valid_class(class: &str) -> bool {
    !class.is_empty() && class.chars().all(is_class_char)
}

/// A `data-*` key is kept when it matches `[a-z][a-z0-9-]*`.
pub(crate) fn valid_data_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// An `href` holding `"` or `<` is reported, though rendering escapes it either way.
pub(crate) fn unsafe_href(href: &str) -> bool {
    href.contains('"') || href.contains('<')
}

/// Every id that gets an element id, in document order: each process's nodes, each sub-process's
/// content right after the sub-process itself, then the scope's sequence flows; then participants,
/// then message flows. Empty ids are skipped.
pub(crate) fn ids_in_document_order(model: &Model) -> Vec<&str> {
    fn scope<'a>(p: &'a Process, out: &mut Vec<&'a str>) {
        for n in &p.nodes {
            out.push(&n.id);
            if let NodeKind::SubProcess { inner, .. } = &n.kind {
                scope(inner, out);
            }
        }
        for f in &p.flows {
            out.push(&f.id);
        }
    }
    let mut out = Vec::new();
    for p in &model.processes {
        scope(p, &mut out);
    }
    out.extend(model.participants.iter().map(|p| p.id.as_str()));
    out.extend(model.message_flows.iter().map(|f| f.id.as_str()));
    out.retain(|id| !id.is_empty());
    out
}

/// Each id mapped to its element id, collisions after sanitising suffixed `-2`, `-3`, … in document
/// order. An id that appears twice keeps its first element id.
pub(crate) fn assign_element_ids(model: &Model, prefix: &str) -> BTreeMap<String, String> {
    let mut assigned = BTreeMap::new();
    let mut used = BTreeSet::new();
    for id in ids_in_document_order(model) {
        if assigned.contains_key(id) {
            continue;
        }
        let base = element_id(prefix, id);
        let mut candidate = base.clone();
        let mut n = 2;
        while used.contains(&candidate) {
            candidate = format!("{base}-{n}");
            n += 1;
        }
        used.insert(candidate.clone());
        assigned.insert(id.to_string(), candidate);
    }
    assigned
}

/// Every flow node id, at any depth.
pub(crate) fn node_ids(model: &Model) -> BTreeSet<&str> {
    fn scope<'a>(p: &'a Process, out: &mut BTreeSet<&'a str>) {
        for n in &p.nodes {
            out.insert(&n.id);
            if let NodeKind::SubProcess { inner, .. } = &n.kind {
                scope(inner, out);
            }
        }
    }
    let mut out = BTreeSet::new();
    for p in &model.processes {
        scope(p, &mut out);
    }
    out
}

/// Every sequence-flow id, at any depth, and every message-flow id.
pub(crate) fn flow_ids(model: &Model) -> BTreeSet<&str> {
    fn scope<'a>(p: &'a Process, out: &mut BTreeSet<&'a str>) {
        for n in &p.nodes {
            if let NodeKind::SubProcess { inner, .. } = &n.kind {
                scope(inner, out);
            }
        }
        out.extend(p.flows.iter().map(|f| f.id.as_str()));
    }
    let mut out = BTreeSet::new();
    for p in &model.processes {
        scope(p, &mut out);
    }
    out.extend(model.message_flows.iter().map(|f| f.id.as_str()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_attr_escapes_markup_quotes_and_hash() {
        assert_eq!(
            escape_attr("a&b<c>\"d\"'e'#333"),
            "a&amp;b&lt;c&gt;&quot;d&quot;&#39;e&#39;&#35;333"
        );
        assert_eq!(escape_attr("plain"), "plain");
    }

    #[test]
    fn element_id_sanitises_to_the_class_alphabet() {
        assert_eq!(element_id("p-", "a.b c/é"), "p-a_b_c__");
        assert_eq!(element_id("", "Task_1-x"), "Task_1-x");
    }

    #[test]
    fn keys_and_classes_are_checked() {
        assert!(valid_data_key("row-1"));
        assert!(!valid_data_key("Row"));
        assert!(!valid_data_key("1row"));
        assert!(!valid_data_key(""));
        assert!(valid_class("is-hot_2"));
        assert!(!valid_class("a b"));
        assert!(!valid_class(""));
        assert!(unsafe_href("x\"y") && unsafe_href("<x") && !unsafe_href("/a?b=c&d#e"));
    }
}
