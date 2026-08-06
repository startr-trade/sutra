//! The discovery model: a [`DocTree`] is one `--input` scan — a sorted list of [`Package`]s,
//! each the recursively-collected artifact inventory of one deployment-package directory (a
//! directory containing `package.yaml`), keyed by file extension/name.
//!
//! When the input folder contains **no** `package.yaml` anywhere (the "arbitrary folder of
//! artifacts" acceptance criterion — no deployment-package structure at all), the whole input
//! root becomes one implicit package (`rel == "."`) so the tool still produces a full catalog.

use std::path::PathBuf;

/// One discovered deployment-package directory (or the whole input root, as a fallback).
#[derive(Debug, Clone, Default)]
pub struct Package {
    /// POSIX path of the package root, relative to the scan's `--input` root. `"."` for the
    /// input root itself (the no-`package.yaml`-anywhere fallback).
    pub rel: String,
    /// `.bpmn` files, POSIX paths relative to `--input`, sorted.
    pub bpmn: Vec<String>,
    /// `.dmn` files, relative to `--input`, sorted.
    pub dmn: Vec<String>,
    /// `.srl` files (the rules DSL, parsed via `sutra_srl::parse`), relative to `--input`,
    /// sorted.
    pub srl: Vec<String>,
    /// `.hbs` / `.xsl` / `.xslt` template/script files, relative to `--input`, sorted.
    pub templates: Vec<String>,
    /// `channels.yaml` at the package root, if present (relative to `--input`).
    pub channels_yaml: Option<String>,
    /// `package.yaml` at the package root, if present (relative to `--input`).
    pub package_yaml: Option<String>,
    /// `rules-manifest.yaml` files, CO-LOCATED under `rules/` (any depth), relative to
    /// `--input`, sorted — merged into one applicability set at render time.
    pub rules_manifest: Vec<String>,
    /// `template-manifest.yaml` files, CO-LOCATED under `templates/` (any depth), relative to
    /// `--input`, sorted — merged into one transform-contract set at render time.
    pub template_manifest: Vec<String>,
    /// C6 cross-process coverage files under `coverage/` (any depth), `*.yaml`/`*.yml`, relative
    /// to `--input`, sorted — a first-class artifact type (URN
    /// `urn:sutra:coverage:<folder…>:<file>`) declaring correlations + coverage routes; rendered
    /// with its own section (declared correlation/route ids) rather than as generic config YAML.
    pub coverage: Vec<String>,
    /// Other `*.yaml` / `*.yml` files found under the package (excluding the four above),
    /// relative to `--input`, sorted — rendered generically as tables.
    pub other_yaml: Vec<String>,
    /// Every other file under the package, relative to `--input`, sorted — listed but not
    /// parsed.
    pub other_files: Vec<String>,
}

impl Package {
    /// The versioned-module triple parsed from the package dir's basename
    /// (`<tenant>--<module>--<version>`), when the name matches that shape.
    pub fn tenant_module_version(&self) -> Option<(String, String, String)> {
        let base = self.rel.rsplit('/').next().unwrap_or(&self.rel);
        let parts: Vec<&str> = base.split("--").collect();
        if parts.len() == 3 {
            Some((
                parts[0].to_string(),
                parts[1].to_string(),
                parts[2].to_string(),
            ))
        } else {
            None
        }
    }

    /// True when this package declares no artifacts at all (an empty/irrelevant directory).
    pub fn is_empty(&self) -> bool {
        self.bpmn.is_empty()
            && self.dmn.is_empty()
            && self.srl.is_empty()
            && self.templates.is_empty()
            && self.channels_yaml.is_none()
            && self.package_yaml.is_none()
            && self.coverage.is_empty()
            && self.other_yaml.is_empty()
            && self.other_files.is_empty()
    }
}

/// One `--input` scan.
#[derive(Debug, Clone)]
pub struct DocTree {
    /// Canonicalized `--input` directory — used for filesystem joins only. NOT rendered into
    /// any page (it's an absolute local-machine path — not portable across checkouts, which
    /// would break `--check` for a committed catalog regenerated on a different machine).
    pub input_root: PathBuf,
    /// The `--input` argument exactly as given (before canonicalization) — safe to render.
    pub input_display: String,
    /// Discovered packages, sorted by [`Package::rel`].
    pub packages: Vec<Package>,
}
