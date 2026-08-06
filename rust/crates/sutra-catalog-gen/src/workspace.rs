//! Workspace discovery: read `rust/Cargo.toml` members, parse each crate's `Cargo.toml`, walk
//! `src/**`, parse every file, and build the per-crate module tree used to resolve `use` paths
//! to concrete source files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};
use toml_edit::DocumentMut;

use crate::model::{Crate, ModDecl, SourceFile, Workspace};
use crate::parse::{parse_file, Parsed};

/// Discover and parse the whole Rust workspace rooted at `repo_root` (the directory containing
/// `rust/`).
pub fn discover(repo_root: &Path) -> Result<Workspace> {
    let ws_manifest = repo_root.join("rust/Cargo.toml");
    let text = std::fs::read_to_string(&ws_manifest)
        .with_context(|| format!("reading {}", ws_manifest.display()))?;
    let doc: DocumentMut = text
        .parse()
        .with_context(|| format!("parsing {}", ws_manifest.display()))?;

    let mut members: Vec<String> = doc
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    members.sort();

    // Map normalised member dir (relative to rust/) → crate name, for path-dep resolution.
    let mut dir_to_name: BTreeMap<String, String> = BTreeMap::new();
    let mut parsed_manifests: BTreeMap<String, ManifestInfo> = BTreeMap::new();
    for member in &members {
        let info = read_manifest(repo_root, member)
            .with_context(|| format!("reading manifest for member {member}"))?;
        dir_to_name.insert(normalize(member), info.name.clone());
        parsed_manifests.insert(member.clone(), info);
    }

    let mut crates: Vec<Crate> = Vec::new();
    for member in &members {
        let info = &parsed_manifests[member];
        let path_deps = resolve_path_deps(member, &info.raw_path_deps, &dir_to_name);
        let krate = build_crate(repo_root, member, info, path_deps)?;
        crates.push(krate);
    }
    crates.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Workspace {
        repo_root: repo_root.to_path_buf(),
        crates,
    })
}

/// Raw manifest facts we need before we know the whole member set.
struct ManifestInfo {
    name: String,
    description: Option<String>,
    /// Dependency `{ path = … }` values, verbatim (resolved to crate names later).
    raw_path_deps: Vec<String>,
}

fn read_manifest(repo_root: &Path, member: &str) -> Result<ManifestInfo> {
    let manifest = repo_root.join("rust").join(member).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("reading {}", manifest.display()))?;
    let doc: DocumentMut = text
        .parse()
        .with_context(|| format!("parsing {}", manifest.display()))?;

    let name = doc
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .map(String::from)
        .unwrap_or_else(|| member.rsplit('/').next().unwrap_or(member).to_string());
    let description = doc
        .get("package")
        .and_then(|p| p.get("description"))
        .and_then(|d| d.as_str())
        .map(crate::util::collapse_ws);

    let mut raw_path_deps = Vec::new();
    for table in ["dependencies", "dev-dependencies", "build-dependencies"] {
        if let Some(deps) = doc.get(table).and_then(|d| d.as_table_like()) {
            for (_, val) in deps.iter() {
                if let Some(path) = val
                    .as_table_like()
                    .and_then(|t| t.get("path"))
                    .and_then(|p| p.as_str())
                {
                    raw_path_deps.push(path.to_string());
                }
            }
        }
    }
    Ok(ManifestInfo {
        name,
        description,
        raw_path_deps,
    })
}

/// Resolve `{ path = "../foo" }` values (relative to the crate dir) to workspace crate names.
fn resolve_path_deps(
    member: &str,
    raw: &[String],
    dir_to_name: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut out = BTreeSet::new();
    for rel in raw {
        let joined = format!("{member}/{rel}");
        if let Some(name) = dir_to_name.get(&normalize(&joined)) {
            out.insert(name.clone());
        }
    }
    out.into_iter().collect()
}

fn build_crate(
    repo_root: &Path,
    member: &str,
    info: &ManifestInfo,
    path_deps: Vec<String>,
) -> Result<Crate> {
    let rel_dir = format!("rust/{member}");
    let crate_dir = repo_root.join("rust").join(member);
    let src_dir = crate_dir.join("src");

    // Parse every source file first; the module tree needs all of them.
    let mut rels: Vec<String> = Vec::new();
    collect_rs_files(&src_dir, &src_dir, &mut rels)?;
    rels.sort();

    let mut parsed: BTreeMap<String, Parsed> = BTreeMap::new();
    let file_set: BTreeSet<String> = rels.iter().cloned().collect();
    for rel in &rels {
        let text = std::fs::read_to_string(crate_dir.join(rel))
            .with_context(|| format!("reading {rel_dir}/{rel}"))?;
        parsed.insert(rel.clone(), parse_file(&text));
    }

    let child_mods_by_file: BTreeMap<String, Vec<ModDecl>> = parsed
        .iter()
        .map(|(k, v)| (k.clone(), v.child_mods.clone()))
        .collect();
    let module_tree = build_module_tree(&child_mods_by_file, &file_set);

    // Invert the tree: a file's own module path is the shortest key mapping to it.
    let mut own_path: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (path, file) in &module_tree {
        own_path
            .entry(file.clone())
            .and_modify(|cur| {
                if path.len() < cur.len() {
                    *cur = path.clone();
                }
            })
            .or_insert_with(|| path.clone());
    }

    let mut files: Vec<SourceFile> = Vec::new();
    for rel in &rels {
        let p = parsed.remove(rel).unwrap();
        let is_binary = rel == "src/main.rs" || rel.starts_with("src/bin/");
        let module_path = own_path
            .get(rel)
            .cloned()
            .unwrap_or_else(|| heuristic_module_path(rel, is_binary));
        files.push(SourceFile {
            rel: rel.clone(),
            module_path,
            is_binary,
            module_doc: p.module_doc,
            items: p.items,
            methods: p.methods,
            trait_impls: p.trait_impls,
            uses: p.uses,
            child_mods: p.child_mods,
        });
    }

    Ok(Crate {
        name: info.name.clone(),
        ident: info.name.replace('-', "_"),
        rel_dir,
        description: info.description.clone(),
        path_deps,
        files,
        module_tree,
    })
}

/// Recursively collect `*.rs` files under `dir`, as paths relative to `base` (POSIX).
fn collect_rs_files(dir: &Path, base: &Path, out: &mut Vec<String>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs_files(&path, base, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Ok(rel) = path.strip_prefix(base.parent().unwrap_or(base)) {
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    Ok(())
}

/// Build module-path → file map from `mod` declarations + on-disk layout.
fn build_module_tree(
    child_mods: &BTreeMap<String, Vec<ModDecl>>,
    file_set: &BTreeSet<String>,
) -> BTreeMap<Vec<String>, String> {
    let mut tree: BTreeMap<Vec<String>, String> = BTreeMap::new();
    let root = if file_set.contains("src/lib.rs") {
        Some("src/lib.rs".to_string())
    } else if file_set.contains("src/main.rs") {
        Some("src/main.rs".to_string())
    } else {
        None
    };
    if let Some(rf) = root {
        tree.insert(Vec::new(), rf.clone());
        walk_mods(&[], &rf, child_mods, file_set, &mut tree);
    }
    tree
}

fn walk_mods(
    module_path: &[String],
    file: &str,
    child_mods: &BTreeMap<String, Vec<ModDecl>>,
    file_set: &BTreeSet<String>,
    tree: &mut BTreeMap<Vec<String>, String>,
) {
    let base_dir = child_dir_of(file);
    if let Some(decls) = child_mods.get(file) {
        for d in decls {
            recurse_decl(module_path, file, &base_dir, d, child_mods, file_set, tree);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn recurse_decl(
    module_path: &[String],
    file: &str,
    base_dir: &str,
    decl: &ModDecl,
    child_mods: &BTreeMap<String, Vec<ModDecl>>,
    file_set: &BTreeSet<String>,
    tree: &mut BTreeMap<Vec<String>, String>,
) {
    let mut child_path = module_path.to_vec();
    child_path.push(decl.name.clone());
    if decl.inline {
        // Inline module lives in the same file; its own file-backed children resolve against
        // base_dir/<name>.
        tree.entry(child_path.clone())
            .or_insert_with(|| file.to_string());
        let nested_dir = format!("{base_dir}/{}", decl.name);
        for c in &decl.children {
            recurse_decl(
                &child_path,
                file,
                &nested_dir,
                c,
                child_mods,
                file_set,
                tree,
            );
        }
    } else {
        let cand1 = format!("{base_dir}/{}.rs", decl.name);
        let cand2 = format!("{base_dir}/{}/mod.rs", decl.name);
        let child_file = if file_set.contains(&cand1) {
            Some(cand1)
        } else if file_set.contains(&cand2) {
            Some(cand2)
        } else {
            None
        };
        if let Some(cf) = child_file {
            tree.entry(child_path.clone()).or_insert_with(|| cf.clone());
            walk_mods(&child_path, &cf, child_mods, file_set, tree);
        }
    }
}

/// The directory external child modules of `file` resolve against.
fn child_dir_of(file: &str) -> String {
    let (dir, name) = match file.rsplit_once('/') {
        Some((d, n)) => (d, n),
        None => ("", file),
    };
    if name == "lib.rs" || name == "main.rs" || name == "mod.rs" {
        dir.to_string()
    } else {
        let stem = name.strip_suffix(".rs").unwrap_or(name);
        if dir.is_empty() {
            stem.to_string()
        } else {
            format!("{dir}/{stem}")
        }
    }
}

/// Fallback module path for files the tree walk cannot reach (binaries, `#[path]` modules).
fn heuristic_module_path(rel: &str, is_binary: bool) -> Vec<String> {
    if is_binary {
        return Vec::new();
    }
    let stripped = rel
        .strip_prefix("src/")
        .unwrap_or(rel)
        .strip_suffix(".rs")
        .unwrap_or(rel);
    if stripped == "lib" || stripped == "main" {
        return Vec::new();
    }
    stripped
        .split('/')
        .filter(|s| *s != "mod")
        .map(String::from)
        .collect()
}

/// Collapse `.` / `..` in a `/`-joined relative path.
fn normalize(path: &str) -> String {
    let mut stack: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                stack.pop();
            }
            other => stack.push(other),
        }
    }
    stack.join("/")
}
