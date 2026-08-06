//! Resolve path-qualified `use` leaves into a bidirectional reference graph.
//!
//! Every edge originates from a structured `use` path (or a `Cargo.toml` path dependency), so
//! the graph never depends on prose matching. Cross-references that cannot be pinned to a file
//! degrade to a crate-level edge; unresolvable / external paths are dropped.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{Crate, Workspace};

/// The reference graph. All maps are keyed by catalog-output-relative page paths (files) or
/// crate names (crate graph) and, being `BTreeMap`/`BTreeSet`, iterate in sorted order.
#[derive(Default)]
pub struct Graph {
    /// file page → source-file pages it references.
    pub file_refs: BTreeMap<String, BTreeSet<String>>,
    /// file page → source-file pages that reference it (inverse of `file_refs`).
    pub file_refby: BTreeMap<String, BTreeSet<String>>,
    /// file page → workspace crate names it uses (cross-crate).
    pub file_uses_crate: BTreeMap<String, BTreeSet<String>>,
    /// crate name → workspace crates it path-depends on.
    pub crate_dep: BTreeMap<String, BTreeSet<String>>,
    /// crate name → workspace crates that depend on it (inverse of `crate_dep`).
    pub crate_depby: BTreeMap<String, BTreeSet<String>>,
}

/// A resolved reference target.
enum Target {
    /// Pinned to a concrete source-file page (`page path`, `crate name`).
    File(String, String),
    /// Only pinned to a crate (couldn't reach a file).
    Crate(String),
}

/// The catalog-output-relative page path for a source file, e.g.
/// `rust/crates/sutra-loader/src/manifest.md`.
pub fn file_page(krate: &Crate, rel: &str) -> String {
    let stem = rel.strip_suffix(".rs").unwrap_or(rel);
    format!("{}/{}.md", krate.rel_dir, stem)
}

/// The catalog-output-relative crate-index (`Cargo.toml`) page path.
pub fn crate_page(krate: &Crate) -> String {
    format!("{}/Cargo.md", krate.rel_dir)
}

/// The catalog-output-relative workspace-root page path.
pub fn workspace_page() -> String {
    "rust/Cargo.md".to_string()
}

/// Build the whole reference graph for `ws`.
pub fn build(ws: &Workspace) -> Graph {
    let mut g = Graph::default();

    let by_ident: BTreeMap<&str, usize> = ws
        .crates
        .iter()
        .enumerate()
        .map(|(i, c)| (c.ident.as_str(), i))
        .collect();

    // Crate-level dependency graph (from Cargo.toml path deps).
    for c in &ws.crates {
        let set: BTreeSet<String> = c.path_deps.iter().cloned().collect();
        for d in &set {
            g.crate_depby
                .entry(d.clone())
                .or_default()
                .insert(c.name.clone());
        }
        g.crate_dep.entry(c.name.clone()).or_default().extend(set);
    }

    // File-level reference graph (from `use` leaves).
    for c in &ws.crates {
        for f in &c.files {
            let from = file_page(c, &f.rel);
            for u in &f.uses {
                // `self`/`super` resolve against the module the `use` textually sits in: the
                // file's module path extended by any inline-module nesting.
                let mut cur = f.module_path.clone();
                cur.extend(u.in_module.iter().cloned());
                match resolve(&u.path, &cur, c, &by_ident, ws) {
                    Some(Target::File(to, tcrate)) => {
                        if to != from {
                            g.file_refs.entry(from.clone()).or_default().insert(to);
                        }
                        if tcrate != c.name {
                            g.file_uses_crate
                                .entry(from.clone())
                                .or_default()
                                .insert(tcrate);
                        }
                    }
                    Some(Target::Crate(tcrate)) if tcrate != c.name => {
                        g.file_uses_crate
                            .entry(from.clone())
                            .or_default()
                            .insert(tcrate);
                    }
                    _ => {}
                }
            }
        }
    }

    // Invert file_refs → file_refby.
    for (from, tos) in &g.file_refs {
        for to in tos {
            g.file_refby
                .entry(to.clone())
                .or_default()
                .insert(from.clone());
        }
    }

    g
}

/// Resolve one flattened `use` leaf (segments) to a [`Target`], relative to `cur` file's module
/// path inside crate `c`.
fn resolve(
    segments: &[String],
    cur_module: &[String],
    c: &Crate,
    by_ident: &BTreeMap<&str, usize>,
    ws: &Workspace,
) -> Option<Target> {
    if segments.is_empty() {
        return None;
    }
    let head = segments[0].as_str();

    // Establish the base crate, the starting module path within it, and the remaining segments.
    let (base, start_module, rest): (&Crate, Vec<String>, &[String]) = match head {
        "crate" => (c, Vec::new(), &segments[1..]),
        "self" => (c, cur_module.to_vec(), &segments[1..]),
        "super" => {
            let mut mp = cur_module.to_vec();
            let mut i = 0;
            while i < segments.len() && segments[i] == "super" {
                mp.pop();
                i += 1;
            }
            (c, mp, &segments[i..])
        }
        other => match by_ident.get(other) {
            Some(&idx) => (&ws.crates[idx], Vec::new(), &segments[1..]),
            None => return None, // external crate (std, third-party) — not in the catalog.
        },
    };

    // Walk the base crate's module tree, taking the longest module prefix of `rest`.
    let mut cand = start_module;
    let mut file = base.module_tree.get(&cand).cloned();
    for seg in rest {
        if seg == "*" || seg == "self" || seg == "super" {
            break;
        }
        let mut next = cand.clone();
        next.push(seg.clone());
        match base.module_tree.get(&next) {
            Some(f) => {
                file = Some(f.clone());
                cand = next;
            }
            None => break,
        }
    }

    match file {
        Some(f) => Some(Target::File(file_page(base, &f), base.name.clone())),
        None if base.name != c.name => Some(Target::Crate(base.name.clone())),
        None => None,
    }
}
