//! Recursive `--input` walk: find every deployment-package boundary (a directory containing
//! `package.yaml`) and classify every file beneath it. Falls back to treating the whole input
//! root as one package when no `package.yaml` exists anywhere (the "arbitrary folder" case).
//!
//! Deterministic by construction: every directory read is sorted before recursing, and every
//! collected vector is sorted again at the end.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::model::{DocTree, Package};

/// Scan `input_root` and build the full [`DocTree`].
pub fn discover(input_root: &Path) -> Result<DocTree> {
    let input_display = input_root.to_string_lossy().replace('\\', "/");
    let input_root = input_root
        .canonicalize()
        .with_context(|| format!("--input folder not found: {}", input_root.display()))?;
    if !input_root.is_dir() {
        anyhow::bail!("--input must be a directory: {}", input_root.display());
    }

    let mut boundaries = Vec::new();
    find_boundaries(&input_root, &mut boundaries)?;
    boundaries.sort();

    let mut packages = if boundaries.is_empty() {
        vec![build_package(&input_root, &input_root, &boundaries)?]
    } else {
        let mut out = Vec::with_capacity(boundaries.len());
        for b in &boundaries {
            out.push(build_package(&input_root, b, &boundaries)?);
        }
        out
    };
    packages.sort_by(|a, b| a.rel.cmp(&b.rel));

    Ok(DocTree {
        input_root,
        input_display,
        packages,
    })
}

/// Depth-first, sorted search for every directory that directly contains `package.yaml`.
fn find_boundaries(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if dir.join("package.yaml").is_file() {
        out.push(dir.to_path_buf());
    }
    for child in sorted_entries(dir)? {
        if child.is_dir() {
            find_boundaries(&child, out)?;
        }
    }
    Ok(())
}

fn build_package(input_root: &Path, pkg_root: &Path, boundaries: &[PathBuf]) -> Result<Package> {
    let mut pkg = Package {
        rel: posix_rel(input_root, pkg_root),
        ..Package::default()
    };
    walk_artifacts(input_root, pkg_root, pkg_root, boundaries, &mut pkg)?;

    pkg.bpmn.sort();
    pkg.dmn.sort();
    pkg.srl.sort();
    pkg.templates.sort();
    pkg.rules_manifest.sort();
    pkg.template_manifest.sort();
    pkg.coverage.sort();
    pkg.other_yaml.sort();
    pkg.other_files.sort();
    Ok(pkg)
}

fn walk_artifacts(
    input_root: &Path,
    pkg_root: &Path,
    dir: &Path,
    boundaries: &[PathBuf],
    pkg: &mut Package,
) -> Result<()> {
    for child in sorted_entries(dir)? {
        if child.is_dir() {
            // A different package's root nested inside this one is its own boundary — its
            // artifacts belong to it, not this package.
            if child != pkg_root && boundaries.iter().any(|b| b == &child) {
                continue;
            }
            walk_artifacts(input_root, pkg_root, &child, boundaries, pkg)?;
        } else {
            classify_file(input_root, pkg_root, dir, &child, pkg);
        }
    }
    Ok(())
}

fn classify_file(input_root: &Path, pkg_root: &Path, dir: &Path, file: &Path, pkg: &mut Package) {
    let rel = posix_rel(input_root, file);
    let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let at_pkg_root = dir == pkg_root;

    if at_pkg_root && name == "package.yaml" {
        pkg.package_yaml = Some(rel);
        return;
    }
    if at_pkg_root && name == "channels.yaml" {
        pkg.channels_yaml = Some(rel);
        return;
    }
    // Manifests are co-located (ruled 2026-07-14) under rules/ and templates/ at any depth —
    // collect ALL by name (not just the package root); merged at render time.
    if name == "rules-manifest.yaml" {
        pkg.rules_manifest.push(rel);
        return;
    }
    if name == "template-manifest.yaml" {
        pkg.template_manifest.push(rel);
        return;
    }

    match file
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .as_deref()
    {
        Some("bpmn") => pkg.bpmn.push(rel),
        Some("dmn") => pkg.dmn.push(rel),
        Some("srl") => pkg.srl.push(rel),
        Some("hbs") | Some("xsl") | Some("xslt") => pkg.templates.push(rel),
        // C6: any YAML under this package's `coverage/` folder (at any depth) is a first-class
        // cross-process coverage artifact — classify it as such BEFORE it falls through to the
        // generic `other_yaml` bucket.
        Some("yaml") | Some("yml") if under_coverage_dir(pkg_root, file) => pkg.coverage.push(rel),
        Some("yaml") | Some("yml") => pkg.other_yaml.push(rel),
        _ => pkg.other_files.push(rel),
    }
}

/// True when `file` lives under this package's `coverage/` folder (any depth).
fn under_coverage_dir(pkg_root: &Path, file: &Path) -> bool {
    posix_rel(pkg_root, file).starts_with("coverage/")
}

/// Sorted (deterministic) directory listing.
fn sorted_entries(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading directory {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    entries.sort();
    Ok(entries)
}

/// POSIX-separated path of `path` relative to `root`; `"."` when `path == root`.
fn posix_rel(root: &Path, path: &Path) -> String {
    if path == root {
        return ".".to_string();
    }
    let rel = path.strip_prefix(root).unwrap_or(path);
    let s = rel.to_string_lossy();
    if std::path::MAIN_SEPARATOR != '/' {
        s.replace(std::path::MAIN_SEPARATOR, "/")
    } else {
        s.into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &Path, rel: &str, content: &str) {
        let p = dir.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(p, content).unwrap();
    }

    #[test]
    fn finds_package_boundary_and_classifies_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write(root, "pkg/package.yaml", "labels: {}\n");
        write(root, "pkg/channels.yaml", "channels: []\n");
        write(root, "pkg/bpmn/flow.bpmn", "<x/>");
        write(root, "pkg/rules/decide.dmn", "<x/>");
        write(root, "pkg/rules/extra.srl", "rule");
        write(root, "pkg/templates/reply.hbs", "{}");
        write(root, "pkg/coverage/orders/e2e.yaml", "correlations: []\n");
        write(root, "pkg/README.md", "hi");

        let tree = discover(root).unwrap();
        assert_eq!(tree.packages.len(), 1);
        let p = &tree.packages[0];
        assert_eq!(p.rel, "pkg");
        assert_eq!(p.bpmn, ["pkg/bpmn/flow.bpmn"]);
        assert_eq!(p.dmn, ["pkg/rules/decide.dmn"]);
        assert_eq!(p.srl, ["pkg/rules/extra.srl"]);
        assert_eq!(p.templates, ["pkg/templates/reply.hbs"]);
        assert_eq!(p.channels_yaml.as_deref(), Some("pkg/channels.yaml"));
        assert_eq!(p.package_yaml.as_deref(), Some("pkg/package.yaml"));
        // C6: coverage/** YAML is first-class — NOT swept into other_yaml.
        assert_eq!(p.coverage, ["pkg/coverage/orders/e2e.yaml"]);
        assert!(p.other_yaml.is_empty());
        assert_eq!(p.other_files, ["pkg/README.md"]);
    }

    #[test]
    fn falls_back_to_root_package_with_no_package_yaml_anywhere() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write(root, "loose/flow.bpmn", "<x/>");

        let tree = discover(root).unwrap();
        assert_eq!(tree.packages.len(), 1);
        assert_eq!(tree.packages[0].rel, ".");
        assert_eq!(tree.packages[0].bpmn, ["loose/flow.bpmn"]);
    }

    #[test]
    fn nested_package_boundary_owns_its_own_subtree() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write(root, "outer/package.yaml", "labels: {}\n");
        write(root, "outer/bpmn/outer.bpmn", "<x/>");
        write(root, "outer/nested/package.yaml", "labels: {}\n");
        write(root, "outer/nested/bpmn/inner.bpmn", "<x/>");

        let tree = discover(root).unwrap();
        assert_eq!(tree.packages.len(), 2);
        let outer = tree.packages.iter().find(|p| p.rel == "outer").unwrap();
        let inner = tree
            .packages
            .iter()
            .find(|p| p.rel == "outer/nested")
            .unwrap();
        assert_eq!(outer.bpmn, ["outer/bpmn/outer.bpmn"]);
        assert_eq!(inner.bpmn, ["outer/nested/bpmn/inner.bpmn"]);
    }
}
