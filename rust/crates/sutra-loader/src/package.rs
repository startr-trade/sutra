//! `sutra package` / `sutra lint` as a LIBRARY — the archive assembler.
//!
//! # The authoring unit: one standalone deployment-package directory
//!
//! The authoring input is a SELF-CONTAINED package directory that mirrors the archive
//! interior layout, plus one metadata file:
//!
//! ```text
//! <package-dir>/
//!   package.yaml          REQUIRED — deployment metadata (schema below)
//!   bpmn/**.bpmn          the processes (subfolders are organisational only); every
//!                         file must carry ONE common targetNamespace, and every
//!                         process id must be defined exactly once
//!   rules/**              .dmn decisions/tables + .srl rule-DSL, routed by
//!                         extension — the SINGLE rule slot (former `decisions/` merged in)
//!   templates/**          .hbs / .xsl(t) transforms
//!   scripts/**            .hbs derivation scripts
//!   redactors/**          .hbs ContentRedactor templates, URN-identified
//!                         `urn:sutra:redactor:<folder…>:<file>:<deploymentId>` (extension
//!                         omitted — a redactor is a single-engine artifact type)
//!   schemas/**            codec schemas: one subfolder per codec (its *.xsd files +
//!                         codec-manifest.yaml companions), or a flat <name>.xsd
//!   migrations/<store>/   V<n>__<desc>.sql data-store migrations; each <store> dir must
//!                         name a store declared in datastores.yaml
//!   coverage/**           .yaml cross-process coverage files, URN-identified
//!                         `urn:sutra:coverage:<folder…>:<file>` (extension omitted)
//!   channels.yaml         channel bindings (optional)
//!   datastores.yaml       data-store declarations (optional)
//!   rules-manifest.yaml / template-manifest.yaml
//!                         optional validation manifests — authoring-side inputs that
//!                         inform `sutra lint` and are NOT packaged
//! ```
//!
//! Unknown top-level FILES (e.g. `README.md`, docs) are ignored; unknown top-level
//! DIRECTORIES draw a lint WARNING (they look like mistyped content categories and will
//! not be packaged). There is no overlay tree, no inheritance, no sharing mechanism —
//! duplication across tenant variants is accepted, and tenant is just a label.
//!
//! # `package.yaml` (minimal schema — closed: unknown keys reject)
//!
//! ```yaml
//! labels:                 # optional mapping, string → string; OPAQUE selectors that
//!   tenant: "default"     #   land in the archive manifest verbatim. `tenant`, `module`
//!   module: "transfers"   #   and `version` are conventional (observability + archive
//!   version: "1.0.0"      #   naming downstream) but carry no engine semantics.
//! engine:                 # optional
//!   minContract: 1        #   minimum engine-contract level (default 1)
//! entryProcesses:         # optional override of the derived entry-process index —
//!   - transfer            #   every named id must be a process the package defines;
//! ```                     #   omitted = derived (processes with channel-subscribed
//!                         #   start events), sorted by process id
//!
//! `package.yaml` itself does not travel: its content is absorbed into the archive's
//! `manifest.yaml` (labels, `engine.minContract`, `entryProcesses`).
//!
//! # Library surface
//!
//! - [`lint_dir`] = the full fail-closed validation suite over a package directory,
//!   without emitting anything.
//! - [`assemble_dir`] = the SAME suite, then exactly ONE fully-deterministic `.sutra`
//!   archive (`<package-dir-name>.sutra` — the file name is human-oriented; identity
//!   stays content-addressed inside). Byte-identical over identical inputs.
//! - [`lint`] / [`assemble`] = the RETIRED tenant-overlay tree input. They survive only
//!   as the engine of the one-shot legacy migration ([`crate::legacy_tree`]) and its
//!   equivalence proof; new callers use the `_dir` forms.
//!
//! The CLI wires `sutra package` / `sutra lint` over exactly this surface; the engine
//! consumes the archives through [`crate::archive::read_archive`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sutra_bpmn::loader::BpmnModelLoader;
use sutra_bpmn::model::{Node, ProcessAudit};
use sutra_bpmn::qbindings::AuditCapture;
use sutra_executor::deployment::DeploymentId;

use crate::archive::{
    deployment_id_of_manifest, sha256_hex, write_archive, ArchiveManifest, ArtifactEntry,
    ARCHIVE_EXTENSION, ENGINE_MIN_CONTRACT, MANIFEST_FILE_NAME, MANIFEST_VERSION,
};
use crate::error::{codes, LoaderError};
use crate::lint::{
    validate_deployment_with_manifests, LintDiagnostic, LintReport, ValidationManifests,
};
use crate::scanner::{
    self, LoadedDeployment, LoadedProcessFile, REDACTOR_SUFFIXES, RULE_SUFFIXES, TEMPLATE_SUFFIXES,
};

/// The package-directory metadata file name.
pub const PACKAGE_FILE_NAME: &str = "package.yaml";

/// The label placeholder used when a package declares no `tenant`/`module`/`version`
/// label (labels are opaque — these three are conventions, not requirements).
const UNLABELED: &str = "unlabeled";

/// The compiled-schema seam: compiled schema artifacts (SchemaShape et al.) emitted into the
/// archive's `schemas/**` once the `sutra-xsd` crate lands. `sutra package` works
/// without an emitter — emission is an increment, not a gate.
pub trait CompiledSchemaEmitter {
    /// Additional archive entries for this deployment. Every path must live under
    /// `schemas/` and must not collide with a source entry.
    fn emit(&self, deployment: &LoadedDeployment) -> Vec<(String, Vec<u8>)>;
}

/// Options for [`assemble`] / [`assemble_dir`].
#[derive(Default)]
pub struct PackageOptions {
    /// Overrides `engine.minContract` (default: the package's own `package.yaml`
    /// declaration, else [`ENGINE_MIN_CONTRACT`]).
    pub engine_min_contract: Option<u64>,
    /// The compiled-schema emission seam (default: none — no compiled artifacts).
    pub schema_emitter: Option<Box<dyn CompiledSchemaEmitter>>,
}

/// One emitted archive.
#[derive(Debug, Clone)]
pub struct PackagedArchive {
    /// The content-addressed identity (`dep-` + 24 hex of sha256(manifest bytes)).
    pub id: DeploymentId,
    /// Authoring labels (opaque to the runtime); absent conventional labels read as
    /// `"unlabeled"`.
    pub tenant: String,
    pub module: String,
    pub version: String,
    /// Where the `.sutra` file was written.
    pub file_path: PathBuf,
    /// The manifest as packaged.
    pub manifest: ArchiveManifest,
}

/// The outcome of a successful [`assemble`] / [`assemble_dir`]: the archives plus the
/// (error-free, possibly warning-bearing) validation report.
#[derive(Debug)]
pub struct PackageOutcome {
    pub archives: Vec<PackagedArchive>,
    pub report: LintReport,
}

/// Why [`assemble`] / [`assemble_dir`] refused.
#[derive(Debug)]
pub enum PackageError {
    /// The validation suite found ERROR diagnostics — nothing was emitted (fail-closed).
    Validation(LintReport),
    /// A container/filesystem failure while emitting.
    Io(LoaderError),
}

impl std::fmt::Display for PackageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageError::Validation(report) => {
                write!(
                    f,
                    "package-time validation failed with {} error(s)",
                    report.errors().count()
                )
            }
            PackageError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PackageError {}

// ---------------------------------------------------------------------------------------
// package.yaml
// ---------------------------------------------------------------------------------------

/// The parsed `package.yaml` — the authoring metadata of one standalone
/// deployment-package directory (schema in the module docs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageConfig {
    /// Opaque archive-manifest labels (`tenant` is a label, never runtime identity).
    pub labels: BTreeMap<String, String>,
    /// Declared `engine.minContract` (default [`ENGINE_MIN_CONTRACT`] when absent).
    pub engine_min_contract: Option<u64>,
    /// Optional override of the derived entry-process index; every id must name a
    /// process the package defines.
    pub entry_processes: Option<Vec<String>>,
    /// B1 — the deployment-wide audit default (`audit.{sink,capture}`), emitted into the sealed
    /// `manifest.yaml`. Identity-bearing: changing it remints the deploymentId (consistent with a
    /// process/node `<q:audit>` change, which already does via the `.bpmn` artifact hash).
    pub audit: Option<ProcessAudit>,
}

impl PackageConfig {
    /// Parse + schema-verify a `package.yaml`. Strict: unknown keys reject (the schema
    /// is closed, like the archive manifest's). An empty document is the all-defaults
    /// package.
    pub fn parse(text: &str) -> Result<PackageConfig, LoaderError> {
        let invalid = |msg: String| LoaderError::new(codes::DEPLOY_PACKAGE_CONFIG_INVALID, msg);
        let parsed: serde_yaml::Value = serde_yaml::from_str(text)
            .map_err(|e| invalid(format!("package.yaml does not parse: {e}")))?;
        if parsed.is_null() {
            return Ok(PackageConfig::default());
        }
        let root = parsed
            .as_mapping()
            .ok_or_else(|| invalid("package.yaml must be a YAML mapping".to_string()))?;

        const KNOWN_KEYS: [&str; 4] = ["labels", "engine", "entryProcesses", "audit"];
        for key in root.keys() {
            let name = key.as_str().unwrap_or("<non-string>");
            if !KNOWN_KEYS.contains(&name) {
                return Err(invalid(format!(
                    "package.yaml declares unknown key '{name}' (the schema is \
                     closed: labels, engine, entryProcesses)"
                )));
            }
        }

        let mut labels = BTreeMap::new();
        if let Some(value) = root.get(serde_yaml::Value::from("labels")) {
            if !value.is_null() {
                let map = value
                    .as_mapping()
                    .ok_or_else(|| invalid("labels must be a mapping".to_string()))?;
                for (k, v) in map {
                    let key = k
                        .as_str()
                        .ok_or_else(|| invalid("labels keys must be strings".to_string()))?;
                    let val = v
                        .as_str()
                        .ok_or_else(|| invalid(format!("label '{key}' must be a string")))?;
                    labels.insert(key.to_string(), val.to_string());
                }
            }
        }

        let mut engine_min_contract = None;
        if let Some(value) = root.get(serde_yaml::Value::from("engine")) {
            if !value.is_null() {
                let engine = value
                    .as_mapping()
                    .ok_or_else(|| invalid("engine must be a mapping".to_string()))?;
                for key in engine.keys() {
                    let name = key.as_str().unwrap_or("<non-string>");
                    if name != "minContract" {
                        return Err(invalid(format!(
                            "package.yaml engine block declares unknown key '{name}' \
                             (only minContract exists)"
                        )));
                    }
                }
                if let Some(v) = engine.get(serde_yaml::Value::from("minContract")) {
                    engine_min_contract = Some(v.as_u64().ok_or_else(|| {
                        invalid("engine.minContract must be a non-negative integer".to_string())
                    })?);
                }
            }
        }

        let mut entry_processes = None;
        if let Some(value) = root.get(serde_yaml::Value::from("entryProcesses")) {
            if !value.is_null() {
                let list = value
                    .as_sequence()
                    .ok_or_else(|| invalid("entryProcesses must be a list".to_string()))?;
                let mut out = Vec::new();
                for item in list {
                    let s = item.as_str().ok_or_else(|| {
                        invalid("entryProcesses entries must be strings".to_string())
                    })?;
                    out.push(s.to_string());
                }
                entry_processes = Some(out);
            }
        }

        // B1 — the deployment audit default (sink + capture); absent ⇒ None.
        let audit = if let Some(value) = root.get(serde_yaml::Value::from("audit")) {
            let map = value
                .as_mapping()
                .ok_or_else(|| invalid("audit must be a mapping".to_string()))?;
            let sink = map
                .get(serde_yaml::Value::from("sink"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("sql")
                .to_string();
            let capture = match map
                .get(serde_yaml::Value::from("capture"))
                .and_then(|v| v.as_str())
            {
                None | Some("") | Some("metadata") => AuditCapture::Metadata,
                Some("payload") => AuditCapture::Payload,
                Some("none") => AuditCapture::None,
                Some(other) => {
                    return Err(invalid(format!(
                        "audit.capture '{other}' is not one of metadata|payload|none"
                    )))
                }
            };
            Some(ProcessAudit { sink, capture })
        } else {
            None
        };

        Ok(PackageConfig {
            labels,
            engine_min_contract,
            entry_processes,
            audit,
        })
    }

    /// Deterministic serialisation (sorted labels, uniform quoting) — what the legacy
    /// materialiser and the `sutra create` scaffolder write. `parse` ∘ `to_yaml` is the identity.
    pub fn to_yaml(&self) -> String {
        let mut out = String::new();
        if self.labels.is_empty() {
            out.push_str("labels: {}\n");
        } else {
            out.push_str("labels:\n");
            for (key, value) in &self.labels {
                out.push_str(&format!("  {}: {}\n", quote(key), quote(value)));
            }
        }
        if let Some(min_contract) = self.engine_min_contract {
            out.push_str("engine:\n");
            out.push_str(&format!("  minContract: {min_contract}\n"));
        }
        if let Some(entries) = &self.entry_processes {
            if entries.is_empty() {
                out.push_str("entryProcesses: []\n");
            } else {
                out.push_str("entryProcesses:\n");
                for entry in entries {
                    out.push_str(&format!("  - {}\n", quote(entry)));
                }
            }
        }
        if let Some(audit) = &self.audit {
            let capture = match audit.capture {
                AuditCapture::Metadata => "metadata",
                AuditCapture::Payload => "payload",
                AuditCapture::None => "none",
            };
            out.push_str("audit:\n");
            out.push_str(&format!("  sink: {}\n", quote(&audit.sink)));
            out.push_str(&format!("  capture: {}\n", quote(capture)));
        }
        out
    }

    fn label_or_unlabeled(&self, key: &str) -> String {
        self.labels
            .get(key)
            .cloned()
            .unwrap_or_else(|| UNLABELED.to_string())
    }
}

/// YAML double-quoted scalar (uniform quoting keeps emitted files trivially
/// deterministic; local twin of the archive-manifest helper).
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------------------
// Package-directory scan (the authoring input model)
// ---------------------------------------------------------------------------------------

/// A scanned package directory: its metadata, its content resolved into the
/// [`LoadedDeployment`] shape the validation suite and the assembler consume, and any
/// scan-level warnings.
struct ScannedPackage {
    config: PackageConfig,
    deployment: LoadedDeployment,
    warnings: Vec<LintDiagnostic>,
}

/// Top-level directories the package layout admits.
const CONTENT_DIRS: [&str; 8] = [
    "bpmn",
    "rules",
    "templates",
    "scripts",
    "redactors",
    "schemas",
    "migrations",
    "coverage",
];

/// Resolve one standalone package directory. Fail-fast on structural errors (missing or
/// invalid `package.yaml`, unparseable BPMN, namespace or process-id conflicts) — those
/// fold into the lint report as its first ERROR, mirroring the tree scan's posture.
fn scan_package_dir(package_dir: &Path) -> Result<ScannedPackage, LoaderError> {
    let invalid = |msg: String| LoaderError::new(codes::DEPLOY_PACKAGE_CONFIG_INVALID, msg);
    if !package_dir.is_dir() {
        return Err(invalid(format!(
            "package directory does not exist: {}",
            package_dir.display()
        )));
    }
    let package_yaml = package_dir.join(PACKAGE_FILE_NAME);
    if !package_yaml.is_file() {
        return Err(invalid(format!(
            "{} has no {PACKAGE_FILE_NAME} — a deployment package declares its metadata \
             there (schema: labels, engine.minContract, entryProcesses)",
            package_dir.display()
        )));
    }
    let text = std::fs::read_to_string(&package_yaml)
        .map_err(|e| invalid(format!("failed to read {}: {e}", package_yaml.display())))?;
    let config = PackageConfig::parse(&text)?;

    // Typo guard: warn on top-level directories outside the package layout.
    let mut warnings = Vec::new();
    for entry in scanner::read_dir_sorted(package_dir)? {
        if entry.is_dir() {
            let name = scanner::dir_name(&entry);
            if !CONTENT_DIRS.contains(&name.as_str()) {
                warnings.push(LintDiagnostic::warning(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!(
                        "directory '{name}/' is not part of the package layout \
                         ({}) and will not be packaged",
                        CONTENT_DIRS.join("/, ")
                    ),
                ));
            }
        }
    }

    // ---- bpmn/** — parse, enforce one namespace and unique process ids --------------
    let loader = BpmnModelLoader;
    let mut processes = BTreeMap::new();
    let mut process_files = BTreeMap::new();
    let mut namespace: Option<String> = None;
    for (sub, artifact) in scanner::walk_artifact_files(&package_dir.join("bpmn"), &[".bpmn"])? {
        let module = loader.load(artifact.content.as_bytes()).map_err(|e| {
            invalid(format!(
                "BPMN bpmn/{sub} fails to parse: {e} ({})",
                artifact.path.display()
            ))
        })?;
        match &namespace {
            None => namespace = Some(module.target_namespace.clone()),
            Some(ns) if *ns != module.target_namespace => {
                return Err(invalid(format!(
                    "BPMN bpmn/{sub} declares targetNamespace '{}' but the package's other \
                     processes declare '{ns}' — one package is one namespace",
                    module.target_namespace
                )));
            }
            Some(_) => {}
        }
        let module = Arc::new(module);
        for pid in module.process_ids() {
            if processes.contains_key(pid) {
                return Err(invalid(format!(
                    "process id '{pid}' is defined by more than one bpmn/** file — a \
                     standalone package must define each process exactly once"
                )));
            }
            processes.insert(pid.to_string(), Arc::clone(&module));
        }
        process_files.insert(
            sub,
            LoadedProcessFile {
                path: artifact.path,
                content: artifact.content,
                module,
            },
        );
    }

    let (codecs, schema_files) = scanner::read_codec_schemas(&package_dir.join("schemas"))?;

    let tenant = config.label_or_unlabeled("tenant");
    let module = config.label_or_unlabeled("module");
    let version = config.label_or_unlabeled("version");
    let mut deployment = LoadedDeployment {
        // Pre-seal: there is no manifest yet, so no identity to assign. The packaged identity
        // is the manifest hash, stamped when the archive is sealed/read (`deployment_id_of_manifest`).
        id: DeploymentId::unresolved(),
        tenant,
        module,
        version,
        namespace: namespace.unwrap_or_default(),
        processes,
        process_files,
        rules: scanner::walk_artifact_files(&package_dir.join("rules"), RULE_SUFFIXES)?,
        templates: scanner::walk_artifact_files(&package_dir.join("templates"), TEMPLATE_SUFFIXES)?,
        scripts: scanner::walk_artifact_files(&package_dir.join("scripts"), TEMPLATE_SUFFIXES)?,
        redactors: scanner::walk_artifact_files(&package_dir.join("redactors"), REDACTOR_SUFFIXES)?,
        codecs,
        schema_files,
        migrations: scanner::walk_artifact_files(&package_dir.join("migrations"), &[".sql"])?,
        coverage_files: scanner::walk_artifact_files(
            &package_dir.join("coverage"),
            crate::coverage::COVERAGE_SUFFIXES,
        )?,
        coverages: Vec::new(),
        channels_yaml: scanner::read_optional(&package_dir.join("channels.yaml"))?,
        datastores_yaml: scanner::read_optional(&package_dir.join("datastores.yaml"))?,
        binding_dir: package_dir.to_path_buf(),
    };

    // Parse `coverage/**` into the deployment table and desugar-inject each route's
    // per-process sub-paths onto the referenced `ProcessDefinition`s (resilient: an absent
    // processId is skipped, deferred to validation).
    deployment.resolve_coverage(codes::CONFIG_MODULE_LAYOUT_INVALID)?;

    Ok(ScannedPackage {
        config,
        deployment,
        warnings,
    })
}

/// The full package-directory report: scan warnings, `entryProcesses` override
/// verification, and the same fail-closed suite `lint`/`assemble`/the archive reader run.
fn validate_package(package_dir: &Path, scanned: &ScannedPackage) -> LintReport {
    let mut out = scanned.warnings.clone();

    if let Some(entries) = &scanned.config.entry_processes {
        let mut seen = std::collections::BTreeSet::new();
        for pid in entries {
            if !scanned.deployment.processes.contains_key(pid) {
                out.push(LintDiagnostic::error(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!(
                        "package.yaml entryProcesses names '{pid}', but the package \
                         defines no such process"
                    ),
                ));
            }
            if !seen.insert(pid) {
                out.push(LintDiagnostic::error(
                    codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                    format!("package.yaml entryProcesses lists '{pid}' more than once"),
                ));
            }
        }
    }

    let mut manifests = ValidationManifests::default();
    manifests.absorb_dir(package_dir, &mut out);
    validate_deployment_with_manifests(&scanned.deployment, &manifests, &mut out);
    LintReport { diagnostics: out }
}

/// `sutra lint <dir>` — validate one standalone package directory without emitting
/// anything (the SAME single code path as [`assemble_dir`]). A scan-level failure folds
/// into the report as its first ERROR.
pub fn lint_dir(package_dir: &Path) -> LintReport {
    match scan_package_dir(package_dir) {
        Err(e) => LintReport {
            diagnostics: vec![LintDiagnostic::error(e.code, e.message)],
        },
        Ok(scanned) => validate_package(package_dir, &scanned),
    }
}

/// `sutra package <dir>` — validate one standalone package directory and emit exactly
/// ONE deterministic `.sutra` into `out_dir`, named `<package-dir-name>.sutra`.
/// Byte-identical over identical inputs; fail-closed (an ERROR emits nothing).
pub fn assemble_dir(
    package_dir: &Path,
    out_dir: &Path,
    options: &PackageOptions,
) -> Result<PackageOutcome, PackageError> {
    let scanned = match scan_package_dir(package_dir) {
        Ok(scanned) => scanned,
        Err(e) => {
            return Err(PackageError::Validation(LintReport {
                diagnostics: vec![LintDiagnostic::error(e.code, e.message)],
            }))
        }
    };
    let report = validate_package(package_dir, &scanned);
    if report.has_errors() {
        return Err(PackageError::Validation(report));
    }

    let (manifest, bytes) = package_dir_deployment(&scanned, options).map_err(PackageError::Io)?;
    let file_name = format!("{}.{ARCHIVE_EXTENSION}", package_dir_name(package_dir));
    let file_path = write_archive_file(out_dir, &file_name, &bytes)?;
    let archive = PackagedArchive {
        id: deployment_id_of_manifest(manifest.to_yaml().as_bytes()),
        tenant: scanned.config.label_or_unlabeled("tenant"),
        module: scanned.config.label_or_unlabeled("module"),
        version: scanned.config.label_or_unlabeled("version"),
        file_path,
        manifest,
    };
    Ok(PackageOutcome {
        archives: vec![archive],
        report,
    })
}

/// Seal one scanned package directory into (manifest, archive bytes) — pure,
/// deterministic. `package.yaml` supplies labels and defaults; [`PackageOptions`] may
/// still override `engine.minContract` (CLI/tooling precedence).
fn package_dir_deployment(
    scanned: &ScannedPackage,
    options: &PackageOptions,
) -> Result<(ArchiveManifest, Vec<u8>), LoaderError> {
    let entry_processes = scanned
        .config
        .entry_processes
        .clone()
        .unwrap_or_else(|| entry_processes(&scanned.deployment));
    seal_deployment(
        &scanned.deployment,
        scanned.config.labels.clone(),
        options
            .engine_min_contract
            .or(scanned.config.engine_min_contract)
            .unwrap_or(ENGINE_MIN_CONTRACT),
        entry_processes,
        scanned.config.audit.clone(),
        options.schema_emitter.as_deref(),
    )
}

/// The archive file name is human-oriented only — identity stays content-addressed.
fn package_dir_name(package_dir: &Path) -> String {
    package_dir
        .canonicalize()
        .ok()
        .as_deref()
        .unwrap_or(package_dir)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "package".to_string())
}

fn write_archive_file(
    out_dir: &Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<PathBuf, PackageError> {
    std::fs::create_dir_all(out_dir).map_err(|e| {
        PackageError::Io(LoaderError::new(
            codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
            format!("failed to create output dir {}: {e}", out_dir.display()),
        ))
    })?;
    let file_path = out_dir.join(file_name);
    std::fs::write(&file_path, bytes).map_err(|e| {
        PackageError::Io(LoaderError::new(
            codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
            format!("failed to write {}: {e}", file_path.display()),
        ))
    })?;
    Ok(file_path)
}

// ---------------------------------------------------------------------------------------
// The sealing path
// ---------------------------------------------------------------------------------------

/// Seal a resolved deployment into (manifest, archive bytes). Deterministic: identical
/// content + identical manifest inputs ⇒ identical bytes ⇒ identical deploymentId.
#[allow(clippy::too_many_arguments)]
fn seal_deployment(
    deployment: &LoadedDeployment,
    labels: BTreeMap<String, String>,
    engine_min_contract: u64,
    entry_processes: Vec<String>,
    audit: Option<ProcessAudit>,
    schema_emitter: Option<&dyn CompiledSchemaEmitter>,
) -> Result<(ArchiveManifest, Vec<u8>), LoaderError> {
    let mut entries: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (sub, file) in &deployment.process_files {
        insert_entry(
            &mut entries,
            format!("bpmn/{sub}"),
            file.content.clone().into_bytes(),
        )?;
    }
    for (folder, artifacts) in [
        ("rules", &deployment.rules),
        ("templates", &deployment.templates),
        ("scripts", &deployment.scripts),
        ("redactors", &deployment.redactors),
    ] {
        for (id, artifact) in artifacts {
            insert_entry(
                &mut entries,
                format!("{folder}/{id}"),
                artifact.content.clone().into_bytes(),
            )?;
        }
    }
    for (sub, artifact) in &deployment.schema_files {
        insert_entry(
            &mut entries,
            format!("schemas/{sub}"),
            artifact.content.clone().into_bytes(),
        )?;
    }
    for (sub, artifact) in &deployment.migrations {
        insert_entry(
            &mut entries,
            format!("migrations/{sub}"),
            artifact.content.clone().into_bytes(),
        )?;
    }
    // Coverage files travel in the sealed archive like bpmn/** / templates/**.
    for (sub, artifact) in &deployment.coverage_files {
        insert_entry(
            &mut entries,
            format!("coverage/{sub}"),
            artifact.content.clone().into_bytes(),
        )?;
    }
    if let Some(yaml) = &deployment.channels_yaml {
        insert_entry(
            &mut entries,
            "channels.yaml".to_string(),
            yaml.clone().into_bytes(),
        )?;
    }
    if let Some(yaml) = &deployment.datastores_yaml {
        insert_entry(
            &mut entries,
            "datastores.yaml".to_string(),
            yaml.clone().into_bytes(),
        )?;
    }
    if let Some(emitter) = schema_emitter {
        for (path, bytes) in emitter.emit(deployment) {
            if !path.starts_with("schemas/") {
                return Err(LoaderError::new(
                    codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
                    format!(
                        "compiled schema emitter produced '{path}' — emitted artifacts \
                         must live under schemas/"
                    ),
                ));
            }
            insert_entry(&mut entries, path, bytes)?;
        }
    }

    let manifest = ArchiveManifest {
        manifest_version: MANIFEST_VERSION,
        engine_min_contract,
        labels,
        // Lineage is a deploy-workflow input (admin migrate) — the packager
        // emits none in v1; the CLI gains a --supersedes wiring when that lands.
        supersedes: Vec::new(),
        entry_processes,
        audit,
        artifacts: entries
            .iter()
            .map(|(path, bytes)| ArtifactEntry {
                path: path.clone(),
                sha256: sha256_hex(bytes),
            })
            .collect(),
    };

    let mut all = entries;
    all.insert(
        MANIFEST_FILE_NAME.to_string(),
        manifest.to_yaml().into_bytes(),
    );
    let bytes = write_archive(&all)?;
    Ok((manifest, bytes))
}

fn insert_entry(
    entries: &mut BTreeMap<String, Vec<u8>>,
    path: String,
    bytes: Vec<u8>,
) -> Result<(), LoaderError> {
    if entries.insert(path.clone(), bytes).is_some() {
        return Err(LoaderError::new(
            codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
            format!("duplicate archive entry '{path}' while packaging"),
        ));
    }
    Ok(())
}

/// The manifest's informational index: processes whose start events subscribe to a channel
/// (`<q:source>`) — the externally-reachable entry points. Sorted (deterministic).
fn entry_processes(deployment: &LoadedDeployment) -> Vec<String> {
    let mut out = Vec::new();
    for (pid, module) in &deployment.processes {
        let Ok(process) = module.process(pid) else {
            continue;
        };
        let reachable = process
            .start_events()
            .iter()
            .any(|node| matches!(node, Node::StartEvent { channels, .. } if !channels.is_empty()));
        if reachable && !out.contains(pid) {
            out.push(pid.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAIN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:solo:1.0.0">
  <bpmn:process id="solo" name="Solo" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

    fn minimal_package(dir: &Path) {
        std::fs::create_dir_all(dir.join("bpmn")).unwrap();
        std::fs::write(dir.join("bpmn/solo.bpmn"), PLAIN_BPMN).unwrap();
        std::fs::write(
            dir.join(PACKAGE_FILE_NAME),
            "labels:\n  module: \"solo\"\n  version: \"1.0.0\"\n",
        )
        .unwrap();
    }

    #[test]
    fn package_config_round_trips_and_rejects_unknown_keys() {
        let config = PackageConfig {
            labels: BTreeMap::from([
                ("tenant".to_string(), "default".to_string()),
                ("module".to_string(), "m".to_string()),
            ]),
            engine_min_contract: Some(1),
            entry_processes: Some(vec!["p".to_string()]),
            audit: Some(ProcessAudit {
                sink: "jsonl".to_string(),
                capture: AuditCapture::Payload,
            }),
        };
        let yaml = config.to_yaml();
        assert_eq!(PackageConfig::parse(&yaml).unwrap(), config);
        // parse ∘ to_yaml is a fixed point (deterministic emission).
        assert_eq!(PackageConfig::parse(&yaml).unwrap().to_yaml(), yaml);

        assert!(PackageConfig::parse("labels: {}\nextra: 1\n").is_err());
        assert!(PackageConfig::parse("engine:\n  maxContract: 2\n").is_err());
        assert!(PackageConfig::parse("labels:\n  tenant: [a]\n").is_err());
        assert!(PackageConfig::parse("entryProcesses: nope\n").is_err());
        // Audit capture is validated against the closed enum.
        assert!(PackageConfig::parse("audit:\n  sink: sql\n  capture: bogus\n").is_err());
        // A bare audit block defaults sink=sql, capture=metadata.
        let bare = PackageConfig::parse("audit:\n  sink: sql\n").unwrap();
        assert_eq!(bare.audit.unwrap().capture, AuditCapture::Metadata);
        // Empty document = all defaults.
        assert_eq!(PackageConfig::parse("").unwrap(), PackageConfig::default());
    }

    #[test]
    fn missing_package_yaml_is_the_first_error() {
        let dir = tempfile::tempdir().unwrap();
        let report = lint_dir(dir.path());
        assert!(report.has_errors());
        let first = report.errors().next().unwrap();
        assert_eq!(first.code, codes::DEPLOY_PACKAGE_CONFIG_INVALID);
        assert!(first.message.contains("package.yaml"), "{}", first.message);
    }

    #[test]
    fn minimal_package_lints_clean_and_seals_one_archive() {
        let dir = tempfile::tempdir().unwrap();
        minimal_package(dir.path());
        let report = lint_dir(dir.path());
        assert!(
            !report.has_errors(),
            "{:?}",
            report.errors().collect::<Vec<_>>()
        );

        let out = tempfile::tempdir().unwrap();
        let outcome =
            assemble_dir(dir.path(), out.path(), &PackageOptions::default()).expect("seals");
        assert_eq!(outcome.archives.len(), 1, "one package dir = one archive");
        let archive = &outcome.archives[0];
        assert_eq!(archive.manifest.labels["module"], "solo");
        assert_eq!(archive.tenant, "unlabeled", "tenant is an optional label");
        assert_eq!(archive.manifest.engine_min_contract, ENGINE_MIN_CONTRACT);
        assert!(archive
            .file_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".sutra"));
        // No channel-subscribed start events ⇒ derived entryProcesses is empty.
        assert!(archive.manifest.entry_processes.is_empty());
        // package.yaml is absorbed into manifest.yaml — it must not travel.
        assert!(archive
            .manifest
            .artifacts
            .iter()
            .all(|a| a.path != PACKAGE_FILE_NAME));

        // Determinism: sealing again is byte-identical.
        let out_two = tempfile::tempdir().unwrap();
        let two =
            assemble_dir(dir.path(), out_two.path(), &PackageOptions::default()).expect("seals");
        assert_eq!(
            std::fs::read(&archive.file_path).unwrap(),
            std::fs::read(&two.archives[0].file_path).unwrap()
        );
    }

    #[test]
    fn redactors_folder_is_admitted_sealed_and_reread_round_trip() {
        // Mirrors the rules/*.dmn archive path: a `redactors/**` folder is an admitted content
        // dir (no lint warning), travels through sealing into the `.sutra` archive, and reads
        // back byte-identical via `read_archive` (the same round trip `archive_activation_*`
        // exercises for bpmn/rules/templates).
        let dir = tempfile::tempdir().unwrap();
        minimal_package(dir.path());
        std::fs::create_dir_all(dir.path().join("redactors/myschema")).unwrap();
        std::fs::write(
            dir.path().join("redactors/myschema/accounts.hbs"),
            "/card\n",
        )
        .unwrap();

        let report = lint_dir(dir.path());
        assert!(
            !report.has_errors(),
            "{:?}",
            report.errors().collect::<Vec<_>>()
        );
        assert!(
            report
                .diagnostics
                .iter()
                .all(|d| !d.message.contains("redactors/")),
            "redactors/ must be an admitted content dir, not a typo-guard warning: {:?}",
            report.diagnostics
        );

        let out = tempfile::tempdir().unwrap();
        let outcome =
            assemble_dir(dir.path(), out.path(), &PackageOptions::default()).expect("seals");
        let archive = &outcome.archives[0];

        let bytes = std::fs::read(&archive.file_path).unwrap();
        let loaded = crate::archive::read_archive(&bytes).expect("reads back");
        assert_eq!(loaded.deployment.redactors.len(), 1);
        assert_eq!(
            loaded.deployment.redactors["myschema/accounts.hbs"].content,
            "/card\n"
        );
    }

    #[test]
    fn entry_process_override_is_verified_and_applied() {
        let dir = tempfile::tempdir().unwrap();
        minimal_package(dir.path());
        std::fs::write(
            dir.path().join(PACKAGE_FILE_NAME),
            "labels:\n  module: \"solo\"\nentryProcesses:\n  - \"solo\"\n",
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();
        let outcome =
            assemble_dir(dir.path(), out.path(), &PackageOptions::default()).expect("seals");
        assert_eq!(outcome.archives[0].manifest.entry_processes, vec!["solo"]);

        // Naming a process the package does not define is a closed failure.
        std::fs::write(
            dir.path().join(PACKAGE_FILE_NAME),
            "entryProcesses:\n  - \"ghost\"\n",
        )
        .unwrap();
        let out_two = tempfile::tempdir().unwrap();
        let err = assemble_dir(dir.path(), out_two.path(), &PackageOptions::default())
            .expect_err("refuses");
        let PackageError::Validation(report) = err else {
            panic!("expected validation refusal");
        };
        assert!(
            report
                .errors()
                .any(|d| d.code == codes::DEPLOY_PACKAGE_CONFIG_INVALID
                    && d.message.contains("ghost"))
        );
        // Fail-closed: nothing was emitted.
        assert_eq!(std::fs::read_dir(out_two.path()).unwrap().count(), 0);
    }

    #[test]
    fn one_package_is_one_namespace_and_unique_process_ids() {
        let dir = tempfile::tempdir().unwrap();
        minimal_package(dir.path());
        std::fs::write(
            dir.path().join("bpmn/other.bpmn"),
            PLAIN_BPMN.replace(
                "urn:sutra:module:solo:1.0.0",
                "urn:sutra:module:other:1.0.0",
            ),
        )
        .unwrap();
        let report = lint_dir(dir.path());
        assert!(report
            .errors()
            .any(|d| d.message.contains("one package is one namespace")));

        // Same namespace but a duplicate process id also rejects.
        std::fs::write(dir.path().join("bpmn/other.bpmn"), PLAIN_BPMN).unwrap();
        let report = lint_dir(dir.path());
        assert!(report
            .errors()
            .any(|d| d.message.contains("more than one bpmn/** file")));
    }

    #[test]
    fn unknown_top_level_directory_warns_but_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        minimal_package(dir.path());
        std::fs::create_dir_all(dir.path().join("template")).unwrap(); // typo of templates/
        std::fs::write(dir.path().join("README.md"), "notes\n").unwrap(); // files are fine
        let report = lint_dir(dir.path());
        assert!(!report.has_errors());
        assert!(report.warnings().any(|d| d.message.contains("'template/'")));
    }
}
