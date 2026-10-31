//! Deployment-manifest layer stacks — the composition behind `sutra package <manifest.yaml>`.
//!
//! A deployment manifest names an ordered list of layer directories plus the package
//! metadata. This module composes such a stack into ONE standalone package directory and
//! then runs the UNCHANGED scan → validate → seal path over it, exactly as the `--overlay`
//! composition in [`crate::overlay`] does. Nothing in the sealed archive records that it was
//! composed: sealing a stack is byte-identical to sealing a hand-merged directory holding the
//! same files and the same `package.yaml`.
//!
//! # The stack rules
//!
//! - Layers apply in order, lowest first; a later layer's copy of a path wins (whole-file
//!   replacement). Every override is reported as a
//!   [`codes::DEPLOY_PACKAGE_OVERLAY_OVERRIDE`] warning and recorded in the
//!   [`StackResolution`].
//! - A layer may be **mounted** under a sub-directory of the package root (`at:`), so a
//!   folder shared between unrelated packages can land where the package layout wants it.
//! - `package.yaml` is supplied by the caller (generated from the manifest), never by a
//!   layer: a layer carrying one is refused, naming the layer.
//! - A layer may carry a **`.ignore`** file at its root: gitignore syntax, patterns relative
//!   to the PACKAGE ROOT (not to the layer, and not to its mount point). When the layer is
//!   applied, its patterns first remove the matching paths contributed by the layers BELOW
//!   it, then the layer's own files are added — so a layer never ignores its own files.
//!   `!pattern` re-includes; the last matching pattern decides. A pattern that matches a
//!   directory matches every file beneath it, and a negation may re-include one file under
//!   an ignored directory. `package.yaml` is never a candidate. A pattern that removes
//!   nothing is a [`codes::DEPLOY_PACKAGE_IGNORE_UNMATCHED`] warning. `.ignore` files are
//!   never copied into the composed tree, so they are never sealed.
//! - Symlinks are refused and a path that is a file in one place and a directory in another
//!   cannot be composed — the same rules as [`crate::overlay`].
//! - Only files are composed: a directory that ends up empty (or was empty in its layer) is
//!   not materialised, so the composed tree is exactly the set of surviving files.

use std::collections::BTreeMap;
use std::path::PathBuf;

use regex::Regex;

use crate::error::{codes, LoaderError};
use crate::lint::{LintDiagnostic, LintReport};
use crate::overlay::{
    copy_file, create_dir, fresh_temp_root, prepend, scan_layer, ComposedPackage, LayerRole,
};
use crate::package::{
    assemble_dir, lint_dir, PackageError, PackageOptions, PackageOutcome, PACKAGE_FILE_NAME,
};

/// The per-layer ignore file's name, at the layer's root.
pub const LAYER_IGNORE_FILE_NAME: &str = ".ignore";

/// One layer of a [`LayerStack`].
#[derive(Debug, Clone)]
pub struct StackLayer {
    /// The layer directory.
    pub root: PathBuf,
    /// Mount point under the package root (`'/'`-separated, relative, no `..`); `None` = the
    /// package root itself. Validated by [`compose_stack`].
    pub mount: Option<String>,
    /// How reports name this layer (e.g. `layer 3 ../app/shared`).
    pub label: String,
}

/// A whole stack: what the composed directory is called, the generated `package.yaml`, and
/// the layers, lowest first.
#[derive(Debug, Clone)]
pub struct LayerStack {
    /// The composed directory's name — it names the archive (`<name>.sutra`).
    pub name: String,
    /// The exact `package.yaml` text to place at the package root.
    pub package_yaml: String,
    /// The layers, lowest first.
    pub layers: Vec<StackLayer>,
}

/// A path an upper layer supplied over a lower layer's copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackOverride {
    pub path: String,
    pub winner: String,
    pub overrode: String,
}

/// A path a layer's `.ignore` removed from what the layers below it contributed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackRemoval {
    pub path: String,
    /// The label of the layer whose `.ignore` removed it.
    pub layer: String,
    /// The pattern that decided the removal (the last matching one).
    pub pattern: String,
    /// The label of the layer that had contributed the removed copy.
    pub from: String,
}

/// What the composition did: the layers in order, every override and every removal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StackResolution {
    pub layers: Vec<String>,
    pub overrides: Vec<StackOverride>,
    pub removals: Vec<StackRemoval>,
}

/// Compose `stack` into a fresh temp package directory named `stack.name`.
///
/// Returns the composed directory (with the override + unmatched-pattern warnings as its
/// [`ComposedPackage::overrides`]) and the resolution report.
pub fn compose_stack(
    stack: &LayerStack,
) -> Result<(ComposedPackage, StackResolution), PackageError> {
    let mut rejections: Vec<LintDiagnostic> = Vec::new();
    let mut scanned = Vec::new();
    for layer in &stack.layers {
        if let Some(mount) = &layer.mount {
            if let Err(message) = check_mount(mount) {
                rejections.push(LintDiagnostic::error(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!("{}: {message}", layer.label),
                ));
            }
        }
        let mut layer_scan = scan_layer(&layer.root, LayerRole::Stack, &mut rejections)?;
        let ignore_text = match layer_scan.files.remove(LAYER_IGNORE_FILE_NAME) {
            Some(path) => Some(std::fs::read_to_string(&path).map_err(|e| {
                PackageError::Io(LoaderError::new(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!("failed to read {}: {e}", path.display()),
                ))
            })?),
            None => None,
        };
        let rules = match ignore_text.as_deref().map(IgnoreRules::parse) {
            Some(Ok(rules)) => rules,
            Some(Err(message)) => {
                rejections.push(LintDiagnostic::error(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!("{} {LAYER_IGNORE_FILE_NAME}: {message}", layer.label),
                ));
                IgnoreRules::default()
            }
            None => IgnoreRules::default(),
        };
        let prefix = layer
            .mount
            .as_deref()
            .map(|m| m.trim_matches('/'))
            .filter(|m| !m.is_empty());
        let files: BTreeMap<String, PathBuf> = layer_scan
            .files
            .into_iter()
            .map(|(relative, source)| match prefix {
                Some(p) => (format!("{p}/{relative}"), source),
                None => (relative, source),
            })
            .collect();
        scanned.push((files, rules));
    }
    if !rejections.is_empty() {
        return Err(PackageError::Validation(LintReport {
            diagnostics: rejections,
        }));
    }

    let mut resolution = StackResolution {
        layers: stack.layers.iter().map(|l| l.label.clone()).collect(),
        ..Default::default()
    };
    let mut warnings: Vec<LintDiagnostic> = Vec::new();
    // Path → (source file, contributing layer index).
    let mut tree: BTreeMap<String, (PathBuf, usize)> = BTreeMap::new();
    for (index, (files, rules)) in scanned.iter().enumerate() {
        let label = &stack.layers[index].label;
        // 1. The layer's `.ignore` removes what the layers BELOW contributed.
        if !rules.patterns.is_empty() {
            let mut matched = vec![false; rules.patterns.len()];
            let mut removed = Vec::new();
            for path in tree.keys() {
                if let Some((pattern_index, ignored)) = rules.decide(path, &mut matched) {
                    if ignored {
                        removed.push((path.clone(), pattern_index));
                    }
                }
            }
            for (path, pattern_index) in removed {
                let (_, from) = tree.remove(&path).expect("present");
                resolution.removals.push(StackRemoval {
                    path,
                    layer: label.clone(),
                    pattern: rules.patterns[pattern_index].text.clone(),
                    from: stack.layers[from].label.clone(),
                });
            }
            for (pattern, hit) in rules.patterns.iter().zip(&matched) {
                if !hit && !pattern.negated {
                    let protected = if pattern.matches(PACKAGE_FILE_NAME) {
                        format!(" ({PACKAGE_FILE_NAME} can never be ignored)")
                    } else {
                        String::new()
                    };
                    warnings.push(LintDiagnostic::warning(
                        codes::DEPLOY_PACKAGE_IGNORE_UNMATCHED,
                        format!(
                            "{label} {LAYER_IGNORE_FILE_NAME}: pattern '{}' matches no path \
                             contributed by the layers below it{protected}",
                            pattern.text
                        ),
                    ));
                }
            }
        }
        // 2. Then the layer's own files are added (and win).
        for (relative, source) in files {
            if let Some(clash) = kind_clash(&tree, relative) {
                rejections.push(LintDiagnostic::error(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!(
                        "'{relative}' from {label} clashes with '{clash}' from a lower layer — \
                         a path cannot be a file in one layer and a directory in another"
                    ),
                ));
                continue;
            }
            if let Some((_, previous)) = tree.insert(relative.clone(), (source.clone(), index)) {
                let overrode = stack.layers[previous].label.clone();
                warnings.push(LintDiagnostic::warning(
                    codes::DEPLOY_PACKAGE_OVERLAY_OVERRIDE,
                    format!(
                        "'{relative}' is provided by more than one layer: {label} overrides \
                         the copy from {overrode}"
                    ),
                ));
                resolution.overrides.push(StackOverride {
                    path: relative.clone(),
                    winner: label.clone(),
                    overrode,
                });
            }
        }
    }
    if !rejections.is_empty() {
        return Err(PackageError::Validation(LintReport {
            diagnostics: rejections,
        }));
    }

    let temp_root = fresh_temp_root().map_err(PackageError::Io)?;
    let root = temp_root.join(&stack.name);
    let composed = ComposedPackage {
        root,
        temp_root: Some(temp_root),
        overrides: warnings,
    };
    create_dir(&composed.root).map_err(PackageError::Io)?;
    std::fs::write(composed.root.join(PACKAGE_FILE_NAME), &stack.package_yaml).map_err(|e| {
        PackageError::Io(LoaderError::new(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            format!("failed to write the generated {PACKAGE_FILE_NAME}: {e}"),
        ))
    })?;
    for (relative, (source, _)) in &tree {
        copy_file(source, &composed.root.join(relative)).map_err(PackageError::Io)?;
    }
    Ok((composed, resolution))
}

/// Compose the stack, then validate and seal it into ONE `<name>.sutra` — byte-identical to
/// sealing a hand-merged directory of the same content.
pub fn assemble_stack(
    stack: &LayerStack,
    out_dir: &std::path::Path,
    options: &PackageOptions,
) -> Result<(PackageOutcome, StackResolution), PackageError> {
    let (composed, resolution) = compose_stack(stack)?;
    match assemble_dir(composed.path(), out_dir, options) {
        Ok(mut outcome) => {
            prepend(&mut outcome.report.diagnostics, composed.overrides());
            Ok((outcome, resolution))
        }
        Err(PackageError::Validation(mut report)) => {
            prepend(&mut report.diagnostics, composed.overrides());
            Err(PackageError::Validation(report))
        }
        Err(other) => Err(other),
    }
}

/// Compose the stack, then run the full fail-closed lint over the composed tree. A
/// composition failure folds into the report; the resolution is empty then.
pub fn lint_stack(stack: &LayerStack) -> (LintReport, StackResolution) {
    match compose_stack(stack) {
        Err(PackageError::Validation(report)) => (report, StackResolution::default()),
        Err(PackageError::Io(e)) => (
            LintReport {
                diagnostics: vec![LintDiagnostic::error(e.code, e.message)],
            },
            StackResolution::default(),
        ),
        Ok((composed, resolution)) => {
            let mut report = lint_dir(composed.path());
            prepend(&mut report.diagnostics, composed.overrides());
            (report, resolution)
        }
    }
}

/// A mount point must stay inside the package root.
fn check_mount(mount: &str) -> Result<(), String> {
    let trimmed = mount.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("`at:` is empty".to_string());
    }
    if mount.starts_with('/') || mount.starts_with('\\') || mount.contains(':') {
        return Err(format!(
            "`at: {mount}` must be a relative path inside the package root"
        ));
    }
    if trimmed
        .split(['/', '\\'])
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!(
            "`at: {mount}` must stay inside the package root (no '..', '.' or empty segments)"
        ));
    }
    if trimmed == PACKAGE_FILE_NAME {
        return Err(format!("`at: {mount}` would shadow {PACKAGE_FILE_NAME}"));
    }
    Ok(())
}

/// A path already in the tree that `relative` cannot coexist with: one of its ancestors is a
/// file, or it is itself a directory of existing files.
fn kind_clash(tree: &BTreeMap<String, (PathBuf, usize)>, relative: &str) -> Option<String> {
    let mut end = 0;
    while let Some(pos) = relative[end..].find('/') {
        let ancestor = &relative[..end + pos];
        if tree.contains_key(ancestor) {
            return Some(ancestor.to_string());
        }
        end += pos + 1;
    }
    let dir_prefix = format!("{relative}/");
    tree.range(dir_prefix.clone()..)
        .next()
        .filter(|(k, _)| k.starts_with(&dir_prefix))
        .map(|(k, _)| k.clone())
}

// ---------------------------------------------------------------------------------------
// `.ignore` — gitignore-syntax patterns over package-root-relative paths
// ---------------------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct IgnoreRules {
    patterns: Vec<IgnorePattern>,
}

#[derive(Debug)]
struct IgnorePattern {
    /// The line as written (for reports).
    text: String,
    negated: bool,
    /// A trailing `/`: matches directories only (so: files beneath a matching directory).
    dir_only: bool,
    regex: Regex,
}

impl IgnorePattern {
    /// Does the pattern match `path` itself (a file) or any ancestor directory of it?
    fn matches(&self, path: &str) -> bool {
        if !self.dir_only && self.regex.is_match(path) {
            return true;
        }
        let mut end = 0;
        while let Some(pos) = path[end..].find('/') {
            if self.regex.is_match(&path[..end + pos]) {
                return true;
            }
            end += pos + 1;
        }
        false
    }
}

impl IgnoreRules {
    pub(crate) fn parse(text: &str) -> Result<IgnoreRules, String> {
        let mut patterns = Vec::new();
        for (number, raw) in text.lines().enumerate() {
            let line = raw.trim_end();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (negated, body) = match line.strip_prefix('!') {
                Some(rest) => (true, rest),
                None => (false, line.strip_prefix('\\').unwrap_or(line)),
            };
            let (dir_only, body) = match body.strip_suffix('/') {
                Some(rest) => (true, rest),
                None => (false, body),
            };
            let anchored = body.contains('/');
            let body = body.trim_start_matches('/');
            if body.is_empty() {
                return Err(format!("line {}: empty pattern '{line}'", number + 1));
            }
            let mut source = String::from("^");
            if !anchored {
                source.push_str("(?:.*/)?");
            }
            source.push_str(&glob_to_regex(body));
            source.push('$');
            let regex = Regex::new(&source)
                .map_err(|e| format!("line {}: invalid pattern '{line}': {e}", number + 1))?;
            patterns.push(IgnorePattern {
                text: line.to_string(),
                negated,
                dir_only,
                regex,
            });
        }
        Ok(IgnoreRules { patterns })
    }

    /// The last pattern matching `path` and whether it ignores it; marks every positive
    /// pattern that matched in `matched`. `package.yaml` is never decided.
    fn decide(&self, path: &str, matched: &mut [bool]) -> Option<(usize, bool)> {
        if path == PACKAGE_FILE_NAME {
            return None;
        }
        let mut decision = None;
        for (index, pattern) in self.patterns.iter().enumerate() {
            if pattern.matches(path) {
                matched[index] = true;
                decision = Some((index, !pattern.negated));
            }
        }
        decision
    }
}

/// Translate one gitignore glob body into a regex fragment: `**` spans directories, `*` and
/// `?` stay within one segment, `[...]` is a character class (`[!...]` negated).
fn glob_to_regex(glob: &str) -> String {
    let chars: Vec<char> = glob.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '*' if chars.get(i + 1) == Some(&'*') => {
                let at_start = i == 0 || chars[i - 1] == '/';
                let followed_by_slash = chars.get(i + 2) == Some(&'/');
                if at_start && followed_by_slash {
                    out.push_str("(?:.*/)?");
                    i += 3;
                } else {
                    out.push_str(".*");
                    i += 2;
                }
                continue;
            }
            '*' => out.push_str("[^/]*"),
            '?' => out.push_str("[^/]"),
            '[' => {
                if let Some(close) = chars[i + 1..].iter().position(|&ch| ch == ']') {
                    let class: String = chars[i + 1..i + 1 + close].iter().collect();
                    let class = match class.strip_prefix('!') {
                        Some(rest) => format!("^{rest}"),
                        None => class,
                    };
                    out.push('[');
                    out.push_str(&class.replace('\\', "\\\\"));
                    out.push(']');
                    i += close + 2;
                    continue;
                }
                out.push_str("\\[");
            }
            '\\' if i + 1 < chars.len() => {
                out.push_str(&regex::escape(&chars[i + 1].to_string()));
                i += 2;
                continue;
            }
            other => out.push_str(&regex::escape(&other.to_string())),
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ignored(rules: &str, path: &str) -> bool {
        let rules = IgnoreRules::parse(rules).expect("parses");
        let mut matched = vec![false; rules.patterns.len()];
        matches!(rules.decide(path, &mut matched), Some((_, true)))
    }

    #[test]
    fn unanchored_patterns_match_at_any_depth() {
        assert!(ignored("*.md", "README.md"));
        assert!(ignored("*.md", "docs/a/b.md"));
        assert!(!ignored("*.md", "docs/a/b.mdx"));
        assert!(ignored("legacy", "bpmn/legacy/x.bpmn"));
    }

    #[test]
    fn anchored_patterns_match_from_the_package_root_only() {
        assert!(ignored("/channels.yaml", "channels.yaml"));
        assert!(!ignored("/channels.yaml", "sub/channels.yaml"));
        assert!(ignored("bpmn/*.bpmn", "bpmn/a.bpmn"));
        assert!(!ignored("bpmn/*.bpmn", "bpmn/x/a.bpmn"));
        assert!(ignored("bpmn/**/*.bpmn", "bpmn/x/y/a.bpmn"));
        assert!(ignored("**/tmp", "a/b/tmp/f"));
    }

    #[test]
    fn directory_patterns_and_negation() {
        assert!(ignored("templates/", "templates/a.txt"));
        assert!(!ignored("templates/", "templates"));
        assert!(!ignored(
            "templates/\n!templates/keep.txt",
            "templates/keep.txt"
        ));
        assert!(ignored(
            "templates/\n!templates/keep.txt",
            "templates/other.txt"
        ));
        assert!(ignored("rules/[ab].dmn", "rules/a.dmn"));
        assert!(!ignored("rules/[!ab].dmn", "rules/a.dmn"));
        assert!(ignored("# comment\n\nfile?.txt", "file1.txt"));
    }

    #[test]
    fn package_yaml_is_never_ignored() {
        assert!(!ignored("*.yaml", "package.yaml"));
        assert!(!ignored("package.yaml", "package.yaml"));
        assert!(ignored("*.yaml", "channels.yaml"));
    }

    #[test]
    fn mounts_must_stay_inside_the_root() {
        assert!(check_mount("schemas").is_ok());
        assert!(check_mount("schemas/shared/").is_ok());
        assert!(check_mount("../x").is_err());
        assert!(check_mount("a/../../x").is_err());
        assert!(check_mount("/abs").is_err());
        assert!(check_mount("C:/x").is_err());
        assert!(check_mount("").is_err());
    }
}
