//! The shared loaded-deployment model ([`LoadedDeployment`] / [`LoadedArtifact`] /
//! [`LoadedProcessFile`]) plus the filesystem helpers (`walk_*`, `read_dir_sorted`,
//! `read_codec_schemas`, …) that the archive reader and the package-directory scanner
//! both build on. `q:scope` does not exist — there is no scope validation.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sutra_bpmn::model::ProcessModule;
use sutra_executor::deployment::DeploymentId;

use crate::error::{codes, LoaderError};

/// Artifact-file extensions per folder (shared with the package-dir scan).
/// `rules/` is the single rule slot (the former `decisions/` folder merged in): it admits
/// `.dmn` (DMN decisions/tables) and `.srl` (the rule DSL — reserved; the engine carries
/// it but has no rete runtime yet). The engine dispatches by extension.
pub(crate) const RULE_SUFFIXES: &[&str] = &[".dmn", ".srl"];
pub(crate) const TEMPLATE_SUFFIXES: &[&str] = &[".hbs", ".xsl", ".xslt"];
/// `redactors/` admits Handlebars only: a redactor is a single-engine artifact type,
/// unlike template/script which admit HBS + XSLT.
pub(crate) const REDACTOR_SUFFIXES: &[&str] = &[".hbs"];

/// One loaded artifact file, keyed (in the owning map) by its archive-local id — the
/// `'/'`-separated subpath under its folder (subfolders are organisational only).
#[derive(Debug, Clone)]
pub struct LoadedArtifact {
    /// Source file on disk (diagnostics; the content below is already loaded).
    pub path: PathBuf,
    /// The artifact bytes as UTF-8 text (BPMN/DMN/hbs/xsl/sql artifacts are all text).
    pub content: String,
}

/// One effective `.bpmn` SOURCE FILE of a deployment: the raw text the packager
/// materialises into the archive's `bpmn/**`, paired with its parsed module so the
/// process-id → file backing stays checkable (partial-shadow detection).
#[derive(Debug, Clone)]
pub struct LoadedProcessFile {
    /// Source file on disk (diagnostics).
    pub path: PathBuf,
    /// The raw BPMN XML text.
    pub content: String,
    /// The parsed module — shared (`Arc::ptr_eq`) with the entries of
    /// [`LoadedDeployment::processes`] this file backs.
    pub module: Arc<ProcessModule>,
}

/// The effective, fully-resolved content of ONE deployment (= one tenant binding): what a
/// `.sutra` archive would contain, pre-materialised from library + tenant shadow.
#[derive(Debug, Clone)]
pub struct LoadedDeployment {
    /// The opaque runtime identity — on this authoring-tree path derived as
    /// `dep-<first 24 hex of sha256("<tenant>/<module>/<version>")>`.
    pub id: DeploymentId,
    /// Authoring labels (opaque to the runtime — observability/CLI only).
    pub tenant: String,
    pub module: String,
    pub version: String,
    /// The module's version-bearing URN namespace (every BPMN's `targetNamespace`).
    pub namespace: String,
    /// Effective processes by process id: (library filtered by `inherits.bpmn`) ∪ tenant-own
    /// (own shadows the library by process id). A multi-process file shares one parsed
    /// [`ProcessModule`] `Arc` across its process ids.
    pub processes: BTreeMap<String, Arc<ProcessModule>>,
    /// Effective `.bpmn` SOURCE FILES by archive-local id (subpath under `bpmn/`): every
    /// file backing at least one effective process (what `sutra package` writes to
    /// the archive's `bpmn/**`). Tenant-own shadows the library by subpath.
    pub process_files: BTreeMap<String, LoadedProcessFile>,
    /// Effective rules by archive-local id (`.dmn`/`.srl` under `rules/`): whole library set
    /// ∪ tenant-own; own shadows by filename. This is the SINGLE rule slot — the former
    /// `decisions/` folder merged in; DMN decisions (businessRuleTask) and rule-DSL
    /// validators (complexValidator) both live here, routed by extension.
    pub rules: BTreeMap<String, LoadedArtifact>,
    /// Effective templates by archive-local id (`.hbs`/`.xsl`/`.xslt` under `templates/`).
    pub templates: BTreeMap<String, LoadedArtifact>,
    /// Effective scripts by archive-local id (same extensions as templates, under `scripts/`).
    pub scripts: BTreeMap<String, LoadedArtifact>,
    /// Effective content redactors by archive-local id (`.hbs` under `redactors/`): a
    /// user-authored Handlebars template compiled into an `HbsContentRedactor` and registered
    /// under an archive-scoped URN — the
    /// deployment-scoped counterpart to the built-in redactor crates. Unlike [`Self::templates`]/[`Self::scripts`], this is single-engine (HBS only).
    pub redactors: BTreeMap<String, LoadedArtifact>,
    /// Codec schemas (`schemas/<name>/*.xsd`, library-side): codec base name → the XSD
    /// files composing it. The registry URN is `<namespace>:<name>` (the structural-codec
    /// naming — folder base name; a flat `schemas/<name>.xsd` keys by
    /// file stem). The engine assembly compiles these via `sutra-codec-spi`.
    pub codecs: BTreeMap<String, Vec<LoadedArtifact>>,
    /// Every effective file under `schemas/`, keyed by its `'/'`-separated subpath relative
    /// to `schemas/`: the XSDs of [`Self::codecs`] PLUS non-XSD companions
    /// (`codec-manifest.yaml`) — what `sutra package` writes to the archive's `schemas/**`.
    /// Tenant-own shadows the library per codec folder (whole-folder replacement, mirroring
    /// [`Self::codecs`]).
    pub schema_files: BTreeMap<String, LoadedArtifact>,
    /// Module-resident data-store migrations, keyed by subpath relative to `migrations/`
    /// (`<store>/V001__init.sql`) — what `sutra package` writes to the archive's
    /// `migrations/**`. Datastore-scoped: every `migrations/<dir>` names a store declared in
    /// `datastores.yaml`, and the store runner applies them against that store's own
    /// connection; the engine ledger never sees app SQL.
    pub migrations: BTreeMap<String, LoadedArtifact>,
    /// Raw cross-process coverage files (`.yaml`/`.yml` under `coverage/`), keyed by
    /// archive-local subpath — what `sutra package` writes to the archive's `coverage/**`
    /// The parsed form is [`Self::coverages`]; this raw map is the packaging
    /// source of truth (mirrors [`Self::schema_files`] ↔ [`Self::codecs`]).
    pub coverage_files: BTreeMap<String, LoadedArtifact>,
    /// Parsed cross-process coverage declarations (the deployment table): each a
    /// URN-identified [`crate::coverage::CoverageFile`] derived from [`Self::coverage_files`].
    /// Desugar-inject slices each route's per-process `segments` into the referenced
    /// `ProcessDefinition`s' `coverage_paths` at load.
    pub coverages: Vec<crate::coverage::CoverageFile>,
    /// Raw `channels.yaml` (binding-side, beside `module.yaml`) — parsed by sutra-channels.
    pub channels_yaml: Option<String>,
    /// Raw `datastores.yaml` (binding-side) — parsed by sutra-datastore.
    pub datastores_yaml: Option<String>,
    /// The binding's version directory (`tenants/<t>/modules/<m>/<v>/`) — the base against
    /// which relative declarations (e.g. a store's `sql.migrations` folder) resolve.
    pub binding_dir: PathBuf,
}

/// Codec-schema discovery under a module version's `schemas/` folder: each SUBFOLDER is
/// one codec (all `*.xsd` beneath it compose it, keyed by the folder base name — the
/// structural-codec convention); a flat `schemas/<name>.xsd` keys by file stem. Also
/// returns the full FILE set by subpath (XSDs plus `codec-manifest.yaml` companions) —
/// what `sutra package` materialises into the archive's `schemas/**`.
#[allow(clippy::type_complexity)]
pub(crate) fn read_codec_schemas(
    schemas_dir: &Path,
) -> Result<
    (
        BTreeMap<String, Vec<LoadedArtifact>>,
        BTreeMap<String, LoadedArtifact>,
    ),
    LoaderError,
> {
    let mut out: BTreeMap<String, Vec<LoadedArtifact>> = BTreeMap::new();
    let mut files: BTreeMap<String, LoadedArtifact> = BTreeMap::new();
    if !schemas_dir.is_dir() {
        return Ok((out, files));
    }
    for entry in read_dir_sorted(schemas_dir)? {
        if entry.is_dir() {
            let name = dir_name(&entry);
            let xsds = walk_artifact_files(&entry, &[".xsd"])?;
            if !xsds.is_empty() {
                out.insert(name.clone(), xsds.into_values().collect());
            }
            // The whole folder travels (schema files + codec-manifest.yaml companions).
            for (sub, artifact) in walk_artifact_files(&entry, &[".xsd", ".yaml", ".yml"])? {
                files.insert(format!("{name}/{sub}"), artifact);
            }
        } else if entry.is_file() {
            let file_name = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if let Some(stem) = file_name.strip_suffix(".xsd") {
                let content = std::fs::read_to_string(&entry).map_err(|e| {
                    LoaderError::new(
                        codes::CONFIG_MODULE_LAYOUT_INVALID,
                        format!("Failed to read {}: {e}", entry.display()),
                    )
                })?;
                let artifact = LoadedArtifact {
                    path: entry.clone(),
                    content,
                };
                files.insert(file_name.to_string(), artifact.clone());
                out.insert(stem.to_string(), vec![artifact]);
            }
        }
    }
    Ok((out, files))
}

/// Recursively map artifact files under `dir` to `'/'`-separated archive-local id → loaded
/// content, for the given extensions. Deterministic (BTreeMap, sorted by subpath id).
pub(crate) fn walk_artifact_files(
    dir: &Path,
    suffixes: &[&str],
) -> Result<BTreeMap<String, LoadedArtifact>, LoaderError> {
    let mut out = BTreeMap::new();
    for (id, path) in walk_files(dir, suffixes)? {
        let content = std::fs::read_to_string(&path).map_err(|e| {
            LoaderError::new(
                codes::CONFIG_MODULE_LAYOUT_INVALID,
                format!("Failed to read {}: {e}", path.display()),
            )
        })?;
        out.insert(id, LoadedArtifact { path, content });
    }
    Ok(out)
}

/// Recursively list regular files under `dir` with one of the given suffixes, keyed by the
/// `'/'`-separated subpath relative to `dir`. Empty when the directory is absent.
fn walk_files(dir: &Path, suffixes: &[&str]) -> Result<BTreeMap<String, PathBuf>, LoaderError> {
    let mut out = BTreeMap::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in read_dir_sorted(&current)? {
            if entry.is_dir() {
                stack.push(entry);
            } else if entry.is_file() {
                let name = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if suffixes.iter().any(|s| name.ends_with(s)) {
                    let key = entry
                        .strip_prefix(dir)
                        .map_err(|e| {
                            LoaderError::new(
                                codes::CONFIG_MODULE_LAYOUT_INVALID,
                                format!("Failed to relativize {}: {e}", entry.display()),
                            )
                        })?
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    out.insert(key, entry);
                }
            }
        }
    }
    Ok(out)
}

pub(crate) fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>, LoaderError> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        LoaderError::new(
            codes::CONFIG_MODULE_LAYOUT_INVALID,
            format!("Failed to list {}: {e}", dir.display()),
        )
    })?;
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            LoaderError::new(
                codes::CONFIG_MODULE_LAYOUT_INVALID,
                format!("Failed to list {}: {e}", dir.display()),
            )
        })?;
        out.push(entry.path());
    }
    out.sort();
    Ok(out)
}

pub(crate) fn dir_name(dir: &Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

pub(crate) fn read_optional(file: &Path) -> Result<Option<String>, LoaderError> {
    if !file.is_file() {
        return Ok(None);
    }
    std::fs::read_to_string(file).map(Some).map_err(|e| {
        LoaderError::new(
            codes::CONFIG_MODULE_LAYOUT_INVALID,
            format!("Failed to read {}: {e}", file.display()),
        )
    })
}
