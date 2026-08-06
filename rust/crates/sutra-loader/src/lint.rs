//! The package-time validation suite — ONE fail-closed code path shared by
//! `sutra lint` (validate without emitting), `sutra package` (validate then emit), and the
//! archive reader (the engine re-validates on load).
//!
//! Coverage = everything the Rust loaders already enforce (BPMN/DMN/channels/datastores/
//! templates parse fail-closed, `SUTRA.CONFIG.COVERAGE.{UNKNOWN_FLOW,INVALID_ROUTE,
//! DUPLICATE_PATH}`) PLUS the deploy-pass validators this module reproduces:
//! channel-uniqueness (`SUTRA.CHANNEL.*`), the coverage STORE_MISSING static check,
//! message-type pin declaration, template-input root availability + payload-field checks
//! (to the extent compiled shapes exist), output conformance (`template-manifest.yaml`),
//! rules applicability (`rules-manifest.yaml`), FEEL determinism at replay-bound sites,
//! tenant-configuration checks, and `migrations/<store>/` validation (every `migrations/<dir>`
//! must name a store declared in `datastores.yaml`).
//!
//! Posture note: the reference CLI ran extension codecs "advise-don't-gatekeep" because
//! they were not loadable; every codec is a built-in crate here, so unknown codec
//! references are deploy-blocking ERRORs — a deliberate posture upgrade.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use regex::Regex;
use serde::{Deserialize, Serialize};
use sutra_bpmn::model::{DeclaredVariable, FieldType, Node, ProcessDefinition, SequenceFlow};
use sutra_bpmn::qbindings::SourceBinding;
use sutra_channels::{load_channel_definitions, ChannelDefinition, CODEC_BUILTIN_URN_PREFIX};
use sutra_codec_spi::shape::{PathResolution, SchemaShape};
use sutra_codec_spi::PayloadCodec;
use sutra_datastore::projection::Projection;
use sutra_datastore::{parse_datastores, StoreDefinition, StructureRef};
use sutra_feel::paths::Usage;
use sutra_templates::HandlebarsTemplateEngine;

use crate::error::codes;
use crate::scanner::LoadedDeployment;

/// Diagnostic severity — ERROR is deploy-blocking (fail-closed); WARNING is advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LintSeverity {
    Warning,
    Error,
}

/// The in-file element a diagnostic anchors to — a *logical* locator the editor resolves to a
/// text Range. The lint parsers are streaming and track no byte offsets, so the lint never
/// carries a line:col; instead a diagnostic names the artifact + the element it concerns, and a
/// range-aware consumer (the VS Code LSP, whose XML scanner already captures element/attribute
/// ranges) maps that to a Range by locating the element in the live document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DiagnosticAnchor {
    /// A BPMN flow node or sequence flow — the editor resolves the element whose `id` is `node`
    /// inside `<bpmn:process id="{process}">` (the process disambiguates a multi-process file).
    BpmnNode { process: String, node: String },
    /// A whole BPMN `<bpmn:process id="{process}">` — no single offending node.
    BpmnProcess { process: String },
    /// A named entry in a config YAML (a `channels.yaml` channel / `datastores.yaml` store),
    /// keyed by `name` — the editor resolves the mapping key.
    NamedEntry { name: String },
}

/// Where a diagnostic applies. `file` is the archive-relative artifact path (`bpmn/order.bpmn`,
/// `channels.yaml`, `rules/forex.dmn`); `anchor` names the in-file element. Both are best-effort —
/// an empty `file` means the diagnostic is deployment-level (no single file), a `None` anchor means
/// whole-file. Consumed by the CLI (rendered as a human location string) and the LSP (mapped to an
/// editor Range). For BPMN anchors the emit site sets only the anchor; [`attach_bpmn_files`] fills
/// `file` in a single post-pass from the deployment's process→source-file index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticSite {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<DiagnosticAnchor>,
}

impl DiagnosticSite {
    /// A compact human-readable location (`bpmn/order.bpmn › process 'order' › node 'task1'`) for
    /// the CLI's `[SEVERITY] CODE — message (location)` rendering. Empty parts are elided.
    pub fn human(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if !self.file.is_empty() {
            parts.push(self.file.clone());
        }
        match &self.anchor {
            Some(DiagnosticAnchor::BpmnNode { process, node }) => {
                parts.push(format!("process '{process}'"));
                parts.push(format!("node '{node}'"));
            }
            Some(DiagnosticAnchor::BpmnProcess { process }) => {
                parts.push(format!("process '{process}'"));
            }
            Some(DiagnosticAnchor::NamedEntry { name }) => {
                parts.push(format!("'{name}'"));
            }
            None => {}
        }
        parts.join(" › ")
    }
}

/// One package-time diagnostic: `[severity] code — message`, optionally located at a [`DiagnosticSite`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LintDiagnostic {
    pub severity: LintSeverity,
    /// Stable `SUTRA.*` code string (from the diagnostics registry).
    pub code: String,
    pub message: String,
    /// Where the diagnostic applies (`None` when not attributable). Set via the `at_*` builders at
    /// the emit site; BPMN sites have their `file` filled by the [`attach_bpmn_files`] post-pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<DiagnosticSite>,
}

impl LintDiagnostic {
    pub fn error(code: &str, message: impl Into<String>) -> LintDiagnostic {
        LintDiagnostic {
            severity: LintSeverity::Error,
            code: code.to_string(),
            message: message.into(),
            site: None,
        }
    }

    pub fn warning(code: &str, message: impl Into<String>) -> LintDiagnostic {
        LintDiagnostic {
            severity: LintSeverity::Warning,
            code: code.to_string(),
            message: message.into(),
            site: None,
        }
    }

    /// Anchor at a BPMN flow node / sequence flow. `file` is left empty here and filled by the
    /// [`attach_bpmn_files`] post-pass from the process→source-file index.
    pub fn at_node(mut self, process: &str, node: &str) -> LintDiagnostic {
        self.site = Some(DiagnosticSite {
            file: String::new(),
            anchor: Some(DiagnosticAnchor::BpmnNode {
                process: process.to_string(),
                node: node.to_string(),
            }),
        });
        self
    }

    /// Anchor at a whole BPMN process (no single node). `file` filled by [`attach_bpmn_files`].
    pub fn at_process(mut self, process: &str) -> LintDiagnostic {
        self.site = Some(DiagnosticSite {
            file: String::new(),
            anchor: Some(DiagnosticAnchor::BpmnProcess {
                process: process.to_string(),
            }),
        });
        self
    }

    /// Anchor at a whole artifact file (a rules/template/migration parse error) — the file is known
    /// at the emit site and there is no finer element.
    pub fn at_file(mut self, file: impl Into<String>) -> LintDiagnostic {
        self.site = Some(DiagnosticSite {
            file: file.into(),
            anchor: None,
        });
        self
    }

    /// Anchor at a named entry in a config YAML (`channels.yaml` channel / `datastores.yaml` store).
    pub fn at_named(mut self, file: impl Into<String>, name: &str) -> LintDiagnostic {
        self.site = Some(DiagnosticSite {
            file: file.into(),
            anchor: Some(DiagnosticAnchor::NamedEntry {
                name: name.to_string(),
            }),
        });
        self
    }

    /// Anchor at a pre-built [`DiagnosticAnchor`] with an empty `file` — the threading form the
    /// deeply-nested BPMN checks (navigation / template-field) use, where a `BpmnNode` or
    /// `BpmnProcess` anchor is assembled at the call site and [`attach_bpmn_files`] fills the file.
    pub fn at_anchor(mut self, anchor: DiagnosticAnchor) -> LintDiagnostic {
        self.site = Some(DiagnosticSite {
            file: String::new(),
            anchor: Some(anchor),
        });
        self
    }
}

impl std::fmt::Display for LintDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let severity = match self.severity {
            LintSeverity::Error => "ERROR",
            LintSeverity::Warning => "WARN",
        };
        write!(f, "[{severity}] {} — {}", self.code, self.message)
    }
}

/// Fill in the `file` of every BPMN-anchored diagnostic from the deployment's effective
/// process→source-file index (mirrors [`check_partial_shadow`]'s effective-file test). BPMN emit
/// sites set only the process/node anchor (the source subpath is not in scope there); this single
/// post-pass resolves each `BpmnNode`/`BpmnProcess` process id to its `bpmn/<subpath>` file. Sites
/// that already carry a `file` (YAML / artifact anchors) or have no BPMN anchor are left untouched.
fn attach_bpmn_files(deployment: &LoadedDeployment, out: &mut [LintDiagnostic]) {
    let mut file_of: BTreeMap<String, String> = BTreeMap::new();
    for (subpath, file) in &deployment.process_files {
        for pid in file.module.process_ids() {
            let effective = deployment
                .processes
                .get(pid)
                .is_some_and(|m| std::sync::Arc::ptr_eq(m, &file.module));
            if effective {
                file_of.insert(pid.to_string(), format!("bpmn/{subpath}"));
            }
        }
    }
    for diagnostic in out.iter_mut() {
        let Some(site) = &mut diagnostic.site else {
            continue;
        };
        if !site.file.is_empty() {
            continue; // YAML / artifact anchors already know their file
        }
        let pid = match &site.anchor {
            Some(DiagnosticAnchor::BpmnNode { process, .. })
            | Some(DiagnosticAnchor::BpmnProcess { process }) => process.as_str(),
            _ => continue,
        };
        if let Some(file) = file_of.get(pid) {
            site.file = file.clone();
        }
    }
}

/// The outcome of a lint / package-validation pass. "Lint-clean" = no ERROR diagnostics
/// (warnings are advisory and do not block packaging).
#[derive(Debug, Clone, Default)]
pub struct LintReport {
    pub diagnostics: Vec<LintDiagnostic>,
}

impl LintReport {
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == LintSeverity::Error)
    }

    pub fn errors(&self) -> impl Iterator<Item = &LintDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == LintSeverity::Error)
    }

    pub fn warnings(&self) -> impl Iterator<Item = &LintDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == LintSeverity::Warning)
    }
}

/// Authoring-side validation manifests (they inform validation and are NOT packaged —
/// the archive interior layout does not carry them): `rules-manifest.yaml` applicability and
/// `template-manifest.yaml` transform contracts.
#[derive(Debug, Clone, Default)]
pub struct ValidationManifests {
    /// rule file (archive-local id under `rules/`) → declared message types
    /// (`"*"` = any; entries may be exact types or anchored-regex patterns).
    pub rule_applicability: BTreeMap<String, Vec<String>>,
    /// template file (archive-local id under `templates/`) → declared outputMessageType.
    pub template_outputs: BTreeMap<String, String>,
}

impl ValidationManifests {
    /// Absorb the CO-LOCATED validation manifests under `dir` (ruled 2026-07-14): every
    /// `rules/**/rules-manifest.yaml` and `templates/**/template-manifest.yaml`, recursively.
    /// Each manifest's `file:` refs are relative to ITS OWN folder; they rebase to the
    /// artifact-folder-relative id (under `rules/`/`templates/`) the validators key on — so a
    /// manifest at `rules/finance/rules-manifest.yaml` with `file: forex.dmn` declares the id
    /// `finance/forex.dmn`. A single manifest at the `rules/` (or `templates/`) root is the
    /// zero-prefix case and behaves exactly as the pre-co-location root manifest did. Later
    /// reads shadow earlier ones per id (deterministic sorted path order).
    pub fn absorb_dir(&mut self, dir: &Path, out: &mut Vec<LintDiagnostic>) {
        self.absorb_manifests(&dir.join("rules"), "rules-manifest.yaml", true, out);
        self.absorb_manifests(&dir.join("templates"), "template-manifest.yaml", false, out);
    }

    /// Recursively collect + absorb every `manifest_name` under `artifact_dir`, rebasing each
    /// entry's `file:` by the manifest's subpath (relative to `artifact_dir`).
    fn absorb_manifests(
        &mut self,
        artifact_dir: &Path,
        manifest_name: &str,
        is_rules: bool,
        out: &mut Vec<LintDiagnostic>,
    ) {
        if !artifact_dir.is_dir() {
            return;
        }
        let mut found: Vec<PathBuf> = Vec::new();
        collect_named_files(artifact_dir, manifest_name, &mut found);
        found.sort();
        for manifest in found {
            let prefix = manifest
                .parent()
                .and_then(|p| p.strip_prefix(artifact_dir).ok())
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();
            let source = manifest.display().to_string();
            match std::fs::read_to_string(&manifest) {
                Ok(text) if is_rules => self.absorb_rules_yaml(&text, &source, &prefix, out),
                Ok(text) => self.absorb_templates_yaml(&text, &source, &prefix, out),
                Err(e) => out.push(LintDiagnostic::error(
                    codes::CONFIG_MODULE_MANIFEST_INVALID,
                    format!("failed to read {}: {e}", manifest.display()),
                )),
            }
        }
    }

    fn absorb_rules_yaml(
        &mut self,
        text: &str,
        source: &str,
        prefix: &str,
        out: &mut Vec<LintDiagnostic>,
    ) {
        let Some(entries) = parse_manifest_entries(text, "rules", source, out) else {
            return;
        };
        for entry in entries {
            let Some(file) = string_of(&entry, "file") else {
                out.push(LintDiagnostic::error(
                    codes::CONFIG_MODULE_MANIFEST_INVALID,
                    format!("{source}: every rules[] entry needs a 'file'"),
                ));
                continue;
            };
            let types = entry
                .get(serde_yaml::Value::from("messageTypes"))
                .and_then(|v| v.as_sequence())
                .map(|seq| {
                    seq.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            self.rule_applicability
                .insert(join_prefix(prefix, &file), types);
        }
    }

    fn absorb_templates_yaml(
        &mut self,
        text: &str,
        source: &str,
        prefix: &str,
        out: &mut Vec<LintDiagnostic>,
    ) {
        let Some(entries) = parse_manifest_entries(text, "templates", source, out) else {
            return;
        };
        for entry in entries {
            let Some(file) = string_of(&entry, "file") else {
                out.push(LintDiagnostic::error(
                    codes::CONFIG_MODULE_MANIFEST_INVALID,
                    format!("{source}: every templates[] entry needs a 'file'"),
                ));
                continue;
            };
            if let Some(output) = string_of(&entry, "outputMessageType") {
                self.template_outputs
                    .insert(join_prefix(prefix, &file), output);
            }
        }
    }
}

/// Recursively collect every file named `name` under `dir` (unsorted; caller sorts).
fn collect_named_files(dir: &Path, name: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_named_files(&path, name, out);
        } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
            out.push(path);
        }
    }
}

/// Rebase a manifest `file:` ref (relative to the manifest's folder) to the artifact-folder id
/// (relative to `rules/` or `templates/`) the validators key on.
fn join_prefix(prefix: &str, file: &str) -> String {
    if prefix.is_empty() {
        file.to_string()
    } else {
        format!("{prefix}/{file}")
    }
}

fn parse_manifest_entries(
    text: &str,
    key: &str,
    source: &str,
    out: &mut Vec<LintDiagnostic>,
) -> Option<Vec<serde_yaml::Mapping>> {
    let parsed: serde_yaml::Value = match serde_yaml::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            out.push(LintDiagnostic::error(
                codes::CONFIG_MODULE_MANIFEST_INVALID,
                format!("{source} does not parse: {e}"),
            ));
            return None;
        }
    };
    let list = parsed
        .as_mapping()
        .and_then(|m| m.get(serde_yaml::Value::from(key)))
        .and_then(|v| v.as_sequence())?;
    Some(
        list.iter()
            .filter_map(|v| v.as_mapping().cloned())
            .collect(),
    )
}

fn string_of(entry: &serde_yaml::Mapping, key: &str) -> Option<String> {
    entry
        .get(serde_yaml::Value::from(key))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

// ==================================================================================
// Per-deployment validation (the fail-closed suite shared by `sutra lint`, `sutra
// package`, and the archive reader)
// ==================================================================================

/// The per-deployment suite without authoring-side manifests — what the archive reader
/// runs (manifests are authoring inputs and do not travel; their absence downgrades the
/// applicability/output checks to their "undeclared" behaviours).
pub fn validate_deployment(deployment: &LoadedDeployment, out: &mut Vec<LintDiagnostic>) {
    validate_deployment_with_manifests(deployment, &ValidationManifests::default(), out);
}

/// The per-deployment fail-closed suite.
pub fn validate_deployment_with_manifests(
    deployment: &LoadedDeployment,
    manifests: &ValidationManifests,
    out: &mut Vec<LintDiagnostic>,
) {
    let dep_label = format!(
        "{}/{}/{} ({})",
        deployment.tenant,
        deployment.module,
        deployment.version,
        deployment.id.value()
    );

    // ---- channels.yaml + datastores.yaml parse fail-closed ---------------------------
    let definitions = parse_channels(deployment, &dep_label, out);
    let stores = parse_stores(deployment, &dep_label, out);

    check_partial_shadow(deployment, &dep_label, out);
    check_reserved_first_level_folders(deployment, &dep_label, out);
    check_reserved_codec_names(deployment, &dep_label, out);
    check_rule_artifacts(deployment, &dep_label, out);
    check_templates_compile(deployment, &dep_label, out);
    check_channel_declarations(deployment, &definitions, &dep_label, out);
    check_channel_uniqueness(deployment, &definitions, &dep_label, out);
    check_coverage_store(deployment, &stores, &dep_label, out);
    check_coverage_correlations(deployment, &definitions, &dep_label, out);
    check_store_references(deployment, &stores, &dep_label, out);
    check_literal_credentials(&stores, &definitions, &dep_label, out);
    check_migrations(deployment, &stores, &dep_label, out);
    check_store_structures(deployment, &stores, &dep_label, out);
    check_node_references(deployment, &definitions, &dep_label, out);
    check_feel_sites(deployment, &dep_label, out);
    check_template_inputs(deployment, &definitions, &dep_label, out);
    check_transient_reads(deployment, &dep_label, out);
    check_never_initialized(deployment, &dep_label, out);
    check_navigation_paths(deployment, &definitions, &dep_label, out);
    check_output_conformance(deployment, &definitions, manifests, &dep_label, out);
    check_rules_applicability(deployment, &definitions, manifests, &dep_label, out);

    // Resolve every BPMN-anchored diagnostic's `file` from the process→source-file index in one
    // pass (the emit sites carry only the process/node anchor).
    attach_bpmn_files(deployment, out);
}

fn parse_channels(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) -> Vec<ChannelDefinition> {
    let Some(yaml) = &deployment.channels_yaml else {
        return Vec::new();
    };
    // The reader path may carry no authoring labels; the channel Namespace shim needs
    // non-empty strings (runtime keying only — never archive identity).
    match load_channel_definitions(
        yaml.as_bytes(),
        &deployment.tenant,
        &deployment.module,
        &deployment.version,
        "channels.yaml",
    ) {
        Ok(defs) => defs,
        Err(diag) => {
            out.push(
                LintDiagnostic::error(
                    &diag.code.clone(),
                    format!("deployment {dep_label}: {}", diag.message),
                )
                .at_file("channels.yaml"),
            );
            Vec::new()
        }
    }
}

fn parse_stores(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) -> Vec<StoreDefinition> {
    let Some(yaml) = &deployment.datastores_yaml else {
        return Vec::new();
    };
    match parse_datastores(yaml) {
        Ok(stores) => stores,
        Err(e) => {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_DATASTORE_INVALID,
                    format!("deployment {dep_label}: datastores.yaml failed to load: {e}"),
                )
                .at_file("datastores.yaml"),
            );
            Vec::new()
        }
    }
}

/// A multi-process file must be fully effective: a partially shadowed/inherited file
/// cannot be materialised into archive `bpmn/**` without duplicating a process id.
fn check_partial_shadow(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for (subpath, file) in &deployment.process_files {
        for pid in file.module.process_ids() {
            let effective = deployment
                .processes
                .get(pid)
                .is_some_and(|m| std::sync::Arc::ptr_eq(m, &file.module));
            if !effective {
                out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_BPMN_PARTIAL_SHADOW,
                        format!(
                            "deployment {dep_label}: bpmn/{subpath} defines process '{pid}', \
                             which is shadowed or not inherited while other processes of the \
                             same file are effective — a sealed archive cannot materialise a \
                             partially-effective file. Split the file or shadow/inherit all of \
                             its processes together."
                        ),
                    )
                    .at_process(pid),
                );
            }
        }
    }
}

/// The single rule slot `rules/**` (the former `decisions/` folder merged in) admits
/// `.dmn` (DMN decisions/tables) and `.srl` (the Sutra Rule Language DSL). Both
/// parse fail-closed: a `.dmn` that fails `DmnFileLoader::load` or a `.srl` that fails
/// `sutra_srl::parse` is a deploy ERROR (the runtime `SrlEngine` executes valid `.srl`).
fn check_rule_artifacts(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let dmn_loader = sutra_dmn::DmnFileLoader::new();
    for (id, artifact) in &deployment.rules {
        if id.ends_with(".dmn") {
            if let Err(e) = dmn_loader.load(artifact.content.as_bytes()) {
                out.push(
                    LintDiagnostic::error(
                        &e.code.clone(),
                        format!(
                            "deployment {dep_label}: rules/{id} fails to parse: {}",
                            e.message
                        ),
                    )
                    .at_file(format!("rules/{id}")),
                );
            }
        } else if id.ends_with(".srl") {
            if let Err(e) = sutra_srl::parse(&artifact.content) {
                out.push(
                    LintDiagnostic::error(
                        &e.code.clone(),
                        format!(
                            "deployment {dep_label}: rules/{id} fails to parse: {}",
                            e.message
                        ),
                    )
                    .at_file(format!("rules/{id}")),
                );
            }
        } else {
            out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_ARTIFACT_UNSUPPORTED,
                    format!(
                        "deployment {dep_label}: rules/{id} is neither a .dmn ruleset nor a .srl \
                         rule-DSL — rules/ admits .dmn and .srl only"
                    ),
                )
                .at_file(format!("rules/{id}")),
            );
        }
    }
}

/// `sutra` is the engine's reserved URN keyword. The FIRST-level subfolder under an
/// artifact folder becomes the first segment of a user codec URN (`schemas/<first>/…` →
/// `urn:<first>:…`), so a first-level `sutra` would produce a `urn:sutra:…` reference that
/// collides with the engine namespace. Reject it (case-insensitive) under every artifact
/// folder. A DEEPER `sutra` (`schemas/hr/sutra/…`) is fine and is NOT flagged.
fn check_reserved_first_level_folders(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let folders: [(&str, Vec<&String>); 6] = [
        ("bpmn", deployment.process_files.keys().collect()),
        ("rules", deployment.rules.keys().collect()),
        ("templates", deployment.templates.keys().collect()),
        ("scripts", deployment.scripts.keys().collect()),
        ("redactors", deployment.redactors.keys().collect()),
        ("schemas", deployment.schema_files.keys().collect()),
    ];
    for (folder, subpaths) in folders {
        // A first-level FOLDER (not a bare top-level file) named `sutra` is the hazard.
        let hit = subpaths.iter().any(|sub| {
            sub.contains('/')
                && sub
                    .split('/')
                    .next()
                    .is_some_and(|first| first.eq_ignore_ascii_case("sutra"))
        });
        if hit {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_RESERVED_FIRST_LEVEL_FOLDER,
                    format!(
                        "deployment {dep_label}: '{folder}/sutra/' — 'sutra' is a reserved \
                         namespace keyword and may not be a first-level subfolder under \
                         {folder}/ (it would collide with the engine's urn:sutra: namespace). \
                         Rename the folder; a deeper 'sutra' segment is allowed."
                    ),
                )
                .at_file(format!("{folder}/sutra")),
            );
        }
    }
}

/// A user codec (a `schemas/` codec-manifest) may not shadow an engine-provided (built-in)
/// codec name: the reference forms differ (`urn:<name>` vs `urn:sutra:codec:<name>`), but a
/// shared short name is a resolution hazard. Deploy-blocking.
///
/// EXCEPTION: a schema BUNDLE whose registered `schemaKind` equals the folder name shadows its
/// like-named built-in BY DESIGN — deployment-scoped override of that codec's schemas is the
/// bundle mechanism: the archive's mapping wins for its own deployment while
/// `channels.yaml` keeps binding the same codec URN. Shadowing an UNRELATED built-in
/// name remains the footgun this check exists for.
fn check_reserved_codec_names(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for name in deployment.codecs.keys() {
        if builtin_codec_names().contains(name) {
            let is_like_named_bundle = deployment
                .schema_files
                .get(&format!("{name}/codec-manifest.yaml"))
                .and_then(|manifest| {
                    serde_yaml::from_str::<serde_yaml::Value>(&manifest.content)
                        .ok()?
                        .get("schemaKind")?
                        .as_str()
                        .map(str::to_owned)
                })
                .is_some_and(|kind| {
                    kind == *name && sutra_codec_schema::bundle::bundle_kind(&kind).is_some()
                });
            if is_like_named_bundle {
                continue;
            }
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_CODEC_RESERVED_NAME,
                    format!(
                        "deployment {dep_label}: user codec '{name}' (schemas/{}/) shadows the \
                         engine-provided codec '{name}' — built-in codec names are reserved. \
                         Rename the codec folder.",
                        name.replace(':', "/")
                    ),
                )
                .at_file(format!("schemas/{}", name.replace(':', "/"))),
            );
        }
    }
}

/// Every `.hbs` template/script/redactor must compile under the strict engine. `redactors/`
/// is HBS-only (a redactor is a single-engine artifact type, with no XSLT counterpart), so every
/// entry there is checked (the `.hbs`-only guard below is a no-op for it, kept for the
/// templates/scripts arms which also admit `.xsl`/`.xslt`).
fn check_templates_compile(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let engine = HandlebarsTemplateEngine::new();
    for (folder, artifacts) in [
        ("templates", &deployment.templates),
        ("scripts", &deployment.scripts),
        ("redactors", &deployment.redactors),
    ] {
        for (id, artifact) in artifacts {
            if !id.ends_with(".hbs") {
                continue; // .xsl compile validation is engine-side (no Rust analyzer yet)
            }
            if let Err(e) = engine.check(artifact.content.as_bytes()) {
                out.push(
                    LintDiagnostic::error(
                        codes::CONFIG_MODULE_LAYOUT_INVALID,
                        format!("deployment {dep_label}: {folder}/{id} does not compile: {e}"),
                    )
                    .at_file(format!("{folder}/{id}")),
                );
            }
        }
    }
}

// ---- codec model ---------------------------------------------------------------------

/// The engine-provided (global) built-in codec + format names — the canonical sources are
/// [`sutra_codec_spi::builtin_codecs`] (schema-backed codecs) and [`sutra_codec_spi::builtin_formats`]
/// (the schema-less formats json/xml/yaml/raw-*/csv); this lint never hardcodes the set. All are
/// RESERVED (a user `schemas/` codec may not shadow one) and referenced as `urn:sutra:codec:<name>`
/// with an OPEN (non-enumerable) declared type set. Computed once.
fn builtin_codec_names() -> &'static BTreeSet<String> {
    static NAMES: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
    NAMES.get_or_init(|| {
        sutra_codec_spi::builtin_codecs()
            .iter()
            .map(|c| c.name().to_string())
            .chain(
                sutra_codec_spi::builtin_formats()
                    .iter()
                    .map(|f| f.name.to_string()),
            )
            .collect()
    })
}

/// The path-derived name a user codec reference resolves to (the key of
/// `deployment.codecs`). User codecs are referenced canonically as `urn:<path-derived>`
/// (e.g. `urn:transfer`, `urn:hr:employee`); a bare name (no `urn:` prefix) passes through.
fn user_codec_name(codec_ref: &str) -> &str {
    let r = codec_ref.trim();
    r.strip_prefix("urn:").unwrap_or(r)
}

/// What a channel's codec declares, for the uniqueness/conformance checks.
enum DeclaredTypes {
    /// No codec bound — schema-less ingress.
    NoCodec,
    /// A known codec with an open (non-enumerable) type set — message-standard codecs, media
    /// codecs.
    Open,
    /// A module structural codec with an enumerable root set.
    Closed(Vec<String>),
    /// The codec name resolves to nothing — deploy-blocking (posture upgrade).
    Unknown,
}

impl DeclaredTypes {
    fn as_list(&self) -> &[String] {
        match self {
            DeclaredTypes::Closed(types) => types,
            _ => &[],
        }
    }
}

fn declared_types(deployment: &LoadedDeployment, codec_name: &str) -> DeclaredTypes {
    let name = codec_name.trim();
    if name.is_empty() {
        return DeclaredTypes::NoCodec;
    }
    // Engine-provided / global codecs — the reserved `urn:sutra:codec:<name>` reference or a
    // bare short name (`raw-text`, a message-standard codec's name), mirroring
    // `CodecRegistry::find`. A built-in whose `declared_message_types()` is non-empty (a
    // rail-variant envelope codec: its message types are the closed wrapper-element set) is
    // ENUMERABLE and verified against; an empty declaration (a codec whose types are
    // namespace slugs, the schema-less formats) stays Open.
    let builtin_short = name.strip_prefix(CODEC_BUILTIN_URN_PREFIX).unwrap_or(name);
    if builtin_codec_names().contains(builtin_short) {
        let mut declared: Vec<String> = sutra_codec_spi::builtin_codecs()
            .iter()
            .find(|c| c.name() == builtin_short)
            .map(|c| c.declared_message_types())
            .unwrap_or_default();
        if declared.is_empty() {
            return DeclaredTypes::Open;
        }
        declared.sort();
        return DeclaredTypes::Closed(declared);
    }
    // User codecs use `urn:<path-derived>` (or the bare path-derived name) and register under
    // the path-derived name (the key of `deployment.codecs`).
    let local = user_codec_name(name);
    if let Some(xsds) = deployment.codecs.get(local) {
        let refs: Vec<&[u8]> = xsds.iter().map(|a| a.content.as_bytes()).collect();
        let compiled = sutra_codec_schema::StructuralCodec::compile(name, &refs);
        let mut roots: Vec<String> = compiled.roots().iter().cloned().collect();
        roots.sort();
        return DeclaredTypes::Closed(roots);
    }
    DeclaredTypes::Unknown
}

/// The navigation shape of one pinned message type against the channel's codec — obtained by
/// asking the CODEC itself ([`sutra_codec_spi::PayloadCodec::shape_of`]), never per-standard
/// hardcodes. Opaque / format-only codecs (xml/json/yaml/csv/raw-*) answer `None`; the
/// schema-aware message-standard codecs (supplied by proprietary extension crates) and a user
/// schema-backed codec answer their shape — every one of them automatically.
fn resolve_shape(
    deployment: &LoadedDeployment,
    codec_name: &str,
    message_type: &str,
) -> Option<SchemaShape> {
    resolve_codec_for_shape(deployment, codec_name)?.shape_of(Some(message_type))
}

/// Resolve a channel's codec reference to a codec INSTANCE for shape introspection: a global
/// built-in (the reserved `urn:sutra:codec:<name>` or a bare short name) from the canonical
/// [`sutra_codec_spi::builtin_codecs`] set, or a user schema-backed
/// [`sutra_codec_schema::StructuralCodec`] compiled from the deployment's `schemas/<name>/` XSDs (the
/// validating build, so its `shape_of` resolves). Conservative fail-closed: an XSD outside the
/// `sutra_xsd` subset yields `None` (an Unverifiable WARNING upstream, never a false ERROR).
fn resolve_codec_for_shape(
    deployment: &LoadedDeployment,
    codec_name: &str,
) -> Option<Arc<dyn PayloadCodec>> {
    let name = codec_name.trim();
    let short = name.strip_prefix(CODEC_BUILTIN_URN_PREFIX).unwrap_or(name);
    if let Some(codec) = sutra_codec_spi::builtin_codecs()
        .into_iter()
        .find(|c| c.name() == short)
    {
        return Some(codec);
    }
    let local = user_codec_name(name);
    let xsds = deployment.codecs.get(local)?;
    let refs: Vec<&[u8]> = xsds.iter().map(|a| a.content.as_bytes()).collect();
    // The validating build retains the compiled schema set that `shape_of` navigates; `formats`
    // affects only decode/content-types (not the shape), so a minimal `xml` is passed.
    sutra_codec_schema::StructuralCodec::compile_with_formats(name, &refs, &["xml"])
        .ok()
        .map(|codec| Arc::new(codec) as Arc<dyn PayloadCodec>)
}

// ---- channel declaration checks --------------------------------------------------------

fn check_channel_declarations(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let mut seen = BTreeSet::new();
    for def in definitions {
        let name = &def.binding.channel_name;
        if !seen.insert(name.clone()) {
            out.push(
                LintDiagnostic::error(
                    sutra_channels::codes::CHANNEL_NAME_COLLISION,
                    format!("deployment {dep_label}: channel '{name}' is declared more than once"),
                )
                .at_named("channels.yaml", name),
            );
        }
        // Codec must resolve — every codec is a built-in crate now (fail-closed upgrade
        // over the reference CLI's advise-don't-gatekeep posture for unresolvable codecs).
        if matches!(
            declared_types(deployment, &def.binding.codec),
            DeclaredTypes::Unknown
        ) {
            out.push(
                LintDiagnostic::error(
                    codes::INBOUND_CODEC_NOT_FOUND,
                    format!(
                        "deployment {dep_label}: channel '{name}' binds codec '{}', which is \
                         neither a bundled codec nor a module schema codec of namespace '{}'",
                        def.binding.codec, deployment.namespace
                    ),
                )
                .at_named("channels.yaml", name),
            );
        }
        if def.is_outbound() {
            // Outbound targets resolve fail-closed: transport + scheme-bearing bind.
            if !def.has_transport() {
                out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_CHANNEL_OUTBOUND_INVALID,
                        format!(
                            "deployment {dep_label}: outbound channel '{name}' declares no \
                             transport"
                        ),
                    )
                    .at_named("channels.yaml", name),
                );
            }
            match def
                .bind_spec
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                None => out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_CHANNEL_OUTBOUND_INVALID,
                        format!(
                            "deployment {dep_label}: outbound channel '{name}' declares no \
                             'bind' destination URI"
                        ),
                    )
                    .at_named("channels.yaml", name),
                ),
                Some(bind) => {
                    // `${ENV}` indirection resolves at engine startup; only a literal
                    // bind can be scheme-checked here.
                    if !bind.contains("${") && sutra_channels::sink::scheme_of(bind).is_none() {
                        out.push(
                            LintDiagnostic::error(
                                codes::DEPLOY_CHANNEL_OUTBOUND_INVALID,
                                format!(
                                    "deployment {dep_label}: outbound channel '{name}' bind \
                                     '{bind}' has no URI scheme — a MessageSink resolves \
                                     destinations by scheme"
                                ),
                            )
                            .at_named("channels.yaml", name),
                        );
                    }
                    // A `local://<target>` bind (in-process routing) or a `pull://<target>` bind
                    // (park for a worker, then deliver its RESULT) must name a real
                    // `transport: local` inbound channel of the SAME deployment — both hops
                    // dispatch to it in-process, so an unknown target would poison every send
                    // (`local`) or every completion (`pull`). Co-deployed collaboration = one
                    // deployment (`definitions`).
                    if matches!(
                        sutra_channels::sink::scheme_of(bind),
                        Some("local") | Some(sutra_channels::PULL_SCHEME)
                    ) {
                        let target = local_bind_target(bind);
                        let target_declared = definitions.iter().any(|d| {
                            !d.is_outbound()
                                && d.transport.as_deref() == Some("local")
                                && d.binding.channel_name == target
                        });
                        if !target_declared {
                            out.push(
                                LintDiagnostic::error(
                                    codes::CONFIG_CHANNEL_LOCAL_TARGET_UNKNOWN,
                                    format!(
                                        "deployment {dep_label}: outbound channel '{name}' binds \
                                         '{bind}', but no 'transport: local' inbound channel \
                                         named '{target}' is declared in this deployment"
                                    ),
                                )
                                .at_named("channels.yaml", name),
                            );
                        }
                    }
                }
            }
        } else if !def.has_transport() && def.transport.is_none() {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_CHANNEL_INERT,
                    format!(
                        "deployment {dep_label}: channel '{name}' declares no transport — \
                         nothing can deliver into it"
                    ),
                )
                .at_named("channels.yaml", name),
            );
        }
    }
}

/// The target inbound-channel name a `local://<target>` or `pull://<target>` bind names — the
/// last path segment (a bare `local://orders-in` and a qualified
/// `local://<module_key>/orders-in` both yield `orders-in`). Both schemes share the grammar
/// because they name the same thing: the inbound channel the hop lands on.
fn local_bind_target(bind: &str) -> &str {
    bind.strip_prefix("local://")
        .or_else(|| bind.strip_prefix("pull://"))
        .map(|rest| rest.rsplit('/').next().unwrap_or(rest))
        .unwrap_or(bind)
}

/// Resolve a `<q:send channel=X>` to the inbound channel the consumer listens on. When X names a
/// declared `direction: outbound` channel binding `local://<target>` (in-process routing) or
/// `pull://<target>` (parked for a worker, whose RESULT lands there), the hop delivers to
/// <target>, so a correlation link resolves against <target>; any other X (a direct channel, or
/// an outbound bind on another scheme) resolves to itself.
fn resolve_local_send_target<'a>(
    definitions: &'a [ChannelDefinition],
    channel: &'a str,
) -> &'a str {
    for def in definitions {
        if def.is_outbound() && def.binding.channel_name == channel {
            if let Some(bind) = def.bind_spec.as_deref() {
                if matches!(
                    sutra_channels::sink::scheme_of(bind),
                    Some("local") | Some(sutra_channels::PULL_SCHEME)
                ) {
                    return local_bind_target(bind);
                }
            }
            break;
        }
    }
    channel
}

// ---- channel uniqueness suite --------------------------------------------------------------

struct Subscriber<'a> {
    process_id: &'a str,
    source: &'a SourceBinding,
}

impl Subscriber<'_> {
    fn value(&self) -> Option<&str> {
        self.source.message_type_value.as_deref()
    }
    fn pattern(&self) -> Option<&str> {
        self.source.message_type_pattern.as_deref()
    }
}

fn subscribers_of<'a>(deployment: &'a LoadedDeployment, channel: &str) -> Vec<Subscriber<'a>> {
    let mut subs = Vec::new();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue; // multi-process Arc shared across pid keys
            }
            for node in process.nodes() {
                if let Node::StartEvent { id, channels, .. } = node {
                    if !channels.iter().any(|c| c == channel) {
                        continue;
                    }
                    if let Some(source) = process.bindings_for(id).source() {
                        if source.channel == channel {
                            subs.push(Subscriber {
                                process_id: &process.id,
                                source,
                            });
                        }
                    }
                }
            }
        }
    }
    subs
}

fn full_match(message_type: &str, pattern: &str) -> bool {
    Regex::new(&format!("^(?:{pattern})$"))
        .map(|re| re.is_match(message_type))
        .unwrap_or(false) // a malformed pattern matches nothing here
}

fn check_channel_uniqueness(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for def in definitions {
        if def.is_outbound() {
            continue;
        }
        let channel = &def.binding.channel_name;
        let subscribers = subscribers_of(deployment, channel);
        if subscribers.is_empty() {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_CHANNEL_INERT,
                    format!(
                        "deployment {dep_label}: channel '{channel}' has no <q:source> \
                         subscriber — nothing handles its messages"
                    ),
                )
                .at_named("channels.yaml", channel),
            );
            continue;
        }
        let declared = declared_types(deployment, &def.binding.codec);
        let declared_list = declared.as_list().to_vec();
        let codec_label = if def.binding.codec.trim().is_empty() {
            "none".to_string()
        } else {
            def.binding.codec.clone()
        };

        // WARN — schema-less channel: no message type can be derived.
        if declared_list.is_empty() && !matches!(declared, DeclaredTypes::Unknown) {
            out.push(
                LintDiagnostic::warning(
                    codes::CHANNEL_NO_SCHEMA,
                    format!(
                        "deployment {dep_label}: channel '{channel}' has {} subscriber(s) but \
                         its codec ({codec_label}) declares no message types — no type can be \
                         derived, so only a catch-all <q:source> can match. Schema-less \
                         ingress is legitimate; this warns so it is explicit.",
                        subscribers.len()
                    ),
                )
                .at_named("channels.yaml", channel),
            );
        }

        // WARN — a messageTypePattern matching more than one declared type.
        for sub in &subscribers {
            let Some(pattern) = sub.pattern() else {
                continue;
            };
            let matched: Vec<&String> = declared_list
                .iter()
                .filter(|t| full_match(t, pattern))
                .collect();
            if matched.len() > 1 {
                out.push(
                    LintDiagnostic::warning(
                        codes::CHANNEL_AMBIGUOUS_PATTERN,
                        format!(
                            "deployment {dep_label}: channel '{channel}': process '{}' declares \
                             messageTypePattern '{pattern}', which matches {} of the codec's \
                             message types {matched:?} — the one-process-per-type intent is \
                             blurred",
                            sub.process_id,
                            matched.len()
                        ),
                    )
                    .at_process(sub.process_id),
                );
            }
        }

        // Message-type pin must be declarable on a closed codec.
        for sub in &subscribers {
            if let Some(value) = sub.value() {
                if !declared_list.is_empty() && !declared_list.iter().any(|t| t == value) {
                    out.push(
                        LintDiagnostic::error(
                            codes::CONFIG_BPMN_MESSAGE_TYPE_UNKNOWN,
                            format!(
                                "deployment {dep_label}: process '{}' pins messageTypeValue \
                                 '{value}' on channel '{channel}', but the codec's declared \
                                 types are {declared_list:?} — the pin can never match",
                                sub.process_id
                            ),
                        )
                        .at_process(sub.process_id),
                    );
                }
            }
        }

        // ERROR — non-broadcast uniqueness (broadcast channels are exempt: pub/sub).
        if !def.binding.broadcast {
            check_non_broadcast_uniqueness(channel, &subscribers, &declared_list, dep_label, out);
        }
    }

    check_replies(deployment, definitions, dep_label, out);
}

fn check_non_broadcast_uniqueness(
    channel: &str,
    subscribers: &[Subscriber<'_>],
    declared_list: &[String],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let mut catch_all = Vec::new();
    let mut exact = Vec::new();
    let mut patterns = Vec::new();
    for sub in subscribers {
        if sub.value().is_some() {
            exact.push(sub);
        } else if sub.pattern().is_some() {
            patterns.push(sub);
        } else {
            catch_all.push(sub);
        }
    }

    // WARN — a typed subscriber without an explicit dedup key: redeliveries are NOT deduped.
    // The transport's sha256(body) fallback is a variables-level key only — it is
    // deliberately EXCLUDED from inbox dedup (dispatch.rs), so it provides no duplicate
    // suppression. Only an explicit `<q:source dedupKey>` (header/ce.id/body.<path>) or a
    // transport-side explicit signal deduplicates.
    for sub in subscribers {
        let typed = sub.value().is_some() || sub.pattern().is_some();
        if typed && sub.source.dedup_key.is_none() {
            out.push(
                LintDiagnostic::warning(
                    codes::CHANNEL_NO_IDEMPOTENCY_KEY,
                    format!(
                        "deployment {dep_label}: process '{}' subscribes to non-broadcast \
                         channel '{channel}' (message type '{}') without a <q:source \
                         dedupKey>; redeliveries are NOT deduplicated (the sha256(body) \
                         fallback does not drive inbox dedup)",
                        sub.process_id,
                        sub.value().or(sub.pattern()).unwrap_or_default()
                    ),
                )
                .at_process(sub.process_id),
            );
        }
    }

    let ambiguous = |message_type: &str, processes: Vec<&str>| {
        LintDiagnostic::error(
            codes::CHANNEL_AMBIGUOUS_HANDLER,
            format!(
                "deployment {dep_label}: non-broadcast channel '{channel}': message type \
                 {message_type} is claimed by {processes:?}. A non-broadcast channel must \
                 resolve to exactly one process — this is an ambiguous routing target, so \
                 the deployment must not package."
            ),
        )
        .at_named("channels.yaml", channel)
    };

    // (A) Catch-all overlap: >1 catch-all, or a catch-all coexisting with typed claims.
    let has_typed = !exact.is_empty() || !patterns.is_empty();
    if catch_all.len() > 1 || (!catch_all.is_empty() && has_typed) {
        let mut ids: Vec<&str> = catch_all.iter().map(|s| s.process_id).collect();
        if catch_all.len() == 1 {
            ids.extend(exact.iter().map(|s| s.process_id));
            ids.extend(patterns.iter().map(|s| s.process_id));
        }
        let mut distinct = Vec::new();
        for id in ids {
            if !distinct.contains(&id) {
                distinct.push(id);
            }
        }
        out.push(ambiguous("(catch-all)", distinct));
    }

    // (B) Concrete-type overlap: candidates = exact values ∪ the codec's declared set.
    let mut candidates: Vec<String> = Vec::new();
    for sub in &exact {
        let value = sub.value().expect("exact");
        if !candidates.iter().any(|c| c == value) {
            candidates.push(value.to_string());
        }
    }
    for t in declared_list {
        if !candidates.iter().any(|c| c == t) {
            candidates.push(t.clone());
        }
    }
    for message_type in &candidates {
        let mut matchers: Vec<&str> = Vec::new();
        for sub in &exact {
            if sub.value() == Some(message_type) && !matchers.contains(&sub.process_id) {
                matchers.push(sub.process_id);
            }
        }
        for sub in &patterns {
            if full_match(message_type, sub.pattern().expect("pattern"))
                && !matchers.contains(&sub.process_id)
            {
                matchers.push(sub.process_id);
            }
        }
        if matchers.len() > 1 {
            out.push(ambiguous(message_type, matchers));
        }
    }
}

/// Outbound reply conformance: every `<q:reply messageType>` must be emittable on the
/// process's reply (= inbound) channel codec.
fn check_replies(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            let mut reply_types = Vec::new();
            visit_nodes(process, &mut |owner, node| {
                if let Some(reply) = &owner.bindings_for(node.id()).reply {
                    if let Some(message_type) = &reply.message_type {
                        reply_types.push(message_type.clone());
                    }
                }
            });
            if reply_types.is_empty() {
                continue;
            }
            let mut inbound = Vec::new();
            for node in process.start_events() {
                if let Node::StartEvent { channels, .. } = node {
                    for channel in channels {
                        if !inbound.contains(channel) {
                            inbound.push(channel.clone());
                        }
                    }
                }
            }
            for channel in &inbound {
                let Some(def) = definitions
                    .iter()
                    .find(|d| &d.binding.channel_name == channel)
                else {
                    continue;
                };
                let declared = declared_types(deployment, &def.binding.codec);
                let declared_list = declared.as_list();
                for message_type in &reply_types {
                    if declared_list.is_empty() {
                        out.push(
                            LintDiagnostic::warning(
                                codes::CHANNEL_REPLY_SCHEMALESS,
                                format!(
                                    "deployment {dep_label}: process '{}' declares reply \
                                     messageType '{message_type}' on channel '{channel}', but \
                                     that codec declares no message types — the reply type is \
                                     unvalidated",
                                    process.id
                                ),
                            )
                            .at_process(&process.id),
                        );
                    } else if !declared_list.iter().any(|t| t == message_type) {
                        out.push(
                            LintDiagnostic::error(
                                codes::CHANNEL_REPLY_NOT_EMITTABLE,
                                format!(
                                    "deployment {dep_label}: process '{}' replies messageType \
                                     '{message_type}' on channel '{channel}', but that codec \
                                     can only emit {declared_list:?} — the reply is not \
                                     emittable",
                                    process.id
                                ),
                            )
                            .at_process(&process.id),
                        );
                    }
                }
            }
        }
    }
}

// ---- coverage / stores / migrations -----------------------------------------------------

/// The static STORE_MISSING check: `<q:coverage>` declared ⇒ a `coverage` store
/// must be present in `datastores.yaml`.
///
/// KEPT by the 2026-08-04 superseding ruling (`datastore-schema-projection.md` §7), and now for
/// the TRUE reason: that store is *where the coverage marks are persisted*. The author chooses
/// their database by pointing it at a data source; the engine owns the coverage schema and applies
/// it to that connection on first use, so no coverage SQL — and no `migrations:` key — is the
/// author's to supply. Without the declaration a mark has nowhere to go.
fn check_coverage_store(
    deployment: &LoadedDeployment,
    stores: &[StoreDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    if let Some(store) = stores.iter().find(|s| s.name == "coverage") {
        // Declared — but a `migrations:` key on it is now a fault, not a leftover: the engine
        // applies its OWN coverage DDL to that connection, so an author's script would sit there
        // never being applied. Said loudly rather than silently ignored (it is the same
        // layout-invalid family as a migrations folder no store applies).
        if store
            .properties
            .get("sql.migrations")
            .is_some_and(|v| !v.trim().is_empty())
        {
            out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_MIGRATIONS_LAYOUT_INVALID,
                    format!(
                        "deployment {dep_label}: store 'coverage' declares sql.migrations, but \
                         the engine owns the coverage schema and applies it to this store's \
                         connection on first use — those scripts would never be applied. Remove \
                         the migrations key (and the folder it names)."
                    ),
                )
                .at_file("datastores.yaml"),
            );
        }
        return;
    }
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            if !process.coverage_paths.is_empty() {
                out.push(
                    LintDiagnostic::error(
                        codes::CONFIG_COVERAGE_STORE_MISSING,
                        format!(
                            "deployment {dep_label}: process '{}' declares <q:coverage> paths \
                             but datastores.yaml declares no 'coverage' store — that store is \
                             where coverage marks are persisted, and its data source is how you \
                             choose their database. Declare it (it needs no migrations: key — the \
                             engine owns the coverage schema and applies it on first use), or \
                             remove the coverage paths.",
                            process.id
                        ),
                    )
                    .at_process(&process.id),
                );
            }
        }
    }
}

// ==================================================================================
// Cross-process (collaboration) coverage: the deployment-level checks over
// `coverage/**`. A coverage file spans several
// processes, so — unlike the intra-process `<q:coverage>` checks in sutra-bpmn — these
// belong in the DEPLOYMENT linter. `COVERAGE_STORE_REQUIRED` is NOT a distinct code: each
// route desugar-injects into the referenced processes' `coverage_paths` at load, so the
// existing `check_coverage_store` above already fires `COVERAGE.STORE_MISSING` for them.
// ==================================================================================

/// The effective [`ProcessDefinition`] for `pid` (looked up in its owning module — the
/// desugar-inject keeps `processes` coherent). `None` when the deployment declares no such
/// process (⇒ `PROCESS_UNKNOWN`).
fn effective_def<'a>(deployment: &'a LoadedDeployment, pid: &str) -> Option<&'a ProcessDefinition> {
    deployment
        .processes
        .get(pid)
        .and_then(|module| module.process(pid).ok())
}

/// The channels a node CONSUMES inbound on: a start-event spawn or an `imec` relay-wait
/// (`MessageCatchEvent` / `UserTask`) node's own channels, plus any `<q:source channel>`
/// binding on it. Empty ⇒ the node is not a channel consumer.
fn consumed_channels(def: &ProcessDefinition, node_id: &str) -> Vec<String> {
    let mut channels: Vec<String> = Vec::new();
    if let Ok(
        Node::StartEvent { channels: ch, .. }
        | Node::MessageCatchEvent { channels: ch, .. }
        | Node::UserTask { channels: ch, .. },
    ) = def.node(node_id)
    {
        channels.extend(ch.iter().cloned());
    }
    for src in &def.bindings_for(node_id).sources {
        channels.push(src.channel.clone());
    }
    channels.sort();
    channels.dedup();
    channels.retain(|c| !c.trim().is_empty());
    channels
}

/// The cross-process coverage checks over `deployment.coverages`:
/// `PATH_ID_DUPLICATE`, `PROCESS_UNKNOWN`, `FLOW_UNKNOWN`, `LINK_UNRESOLVED`, `KEY_RESOLVABLE`.
fn check_coverage_correlations(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for file in &deployment.coverages {
        let file_ref = file.urn.as_str();

        // ---- PATH_ID_DUPLICATE — route path mnemonics are unique under the file URN ------
        let mut seen_paths: BTreeSet<&str> = BTreeSet::new();
        let mut dup_reported: BTreeSet<&str> = BTreeSet::new();
        for corr in &file.correlations {
            for route in &corr.coverages {
                if !seen_paths.insert(route.path.as_str())
                    && dup_reported.insert(route.path.as_str())
                {
                    out.push(
                        LintDiagnostic::error(
                            codes::CONFIG_CORRELATION_PATH_ID_DUPLICATE,
                            format!(
                                "deployment {dep_label}: coverage file '{file_ref}' declares \
                                 coverage route path '{}' more than once — path mnemonics are \
                                 unique under the file URN.",
                                route.path
                            ),
                        )
                        .at_named(file_ref, &route.path),
                    );
                }
            }
        }

        for corr in &file.correlations {
            // ---- PROCESS_UNKNOWN + FLOW_UNKNOWN over each route's per-process segments ----
            for route in &corr.coverages {
                for (pid, flows) in &route.segments {
                    let Some(def) = effective_def(deployment, pid) else {
                        out.push(
                            LintDiagnostic::error(
                                codes::CONFIG_CORRELATION_PROCESS_UNKNOWN,
                                format!(
                                    "deployment {dep_label}: coverage file '{file_ref}' \
                                     correlation '{}' route '{}' references process '{pid}', which \
                                     is not a process in this deployment.",
                                    corr.id, route.path
                                ),
                            )
                            .at_named(file_ref, &corr.id),
                        );
                        continue;
                    };
                    check_segment_flows(def, pid, file_ref, &route.path, flows, dep_label, out);
                }
            }

            // ---- the hops: PROCESS_UNKNOWN (hop nodes), LINK_UNRESOLVED, KEY_RESOLVABLE ----
            for hop in &corr.links {
                check_hop(deployment, definitions, file_ref, corr, hop, dep_label, out);
            }
        }
    }
}

/// `FLOW_UNKNOWN` — every flow id in a route's `segments[p]` must be one of process `p`'s own
/// sequence flows, and the sub-path must be contiguous within `p` (reusing the intra-process
/// contiguity relation [`sutra_bpmn::model::flows_contiguous`], so the two never drift).
fn check_segment_flows(
    def: &ProcessDefinition,
    pid: &str,
    file_ref: &str,
    path: &str,
    flows: &[String],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let by_id: BTreeMap<&str, &SequenceFlow> =
        def.flows().iter().map(|f| (f.id.as_str(), f)).collect();
    for fid in flows {
        if !by_id.contains_key(fid.as_str()) {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_CORRELATION_FLOW_UNKNOWN,
                    format!(
                        "deployment {dep_label}: coverage file '{file_ref}' route '{path}' segment \
                         for process '{pid}' references flow '{fid}', which is not a \
                         <bpmn:sequenceFlow> of that process."
                    ),
                )
                .at_process(pid),
            );
        }
    }
    // Contiguity over consecutive pairs that are BOTH present (unknown flows already reported).
    for w in flows.windows(2) {
        let (Some(a), Some(b)) = (by_id.get(w[0].as_str()), by_id.get(w[1].as_str())) else {
            continue;
        };
        if !sutra_bpmn::model::flows_contiguous(a, b) {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_CORRELATION_FLOW_UNKNOWN,
                    format!(
                        "deployment {dep_label}: coverage file '{file_ref}' route '{path}' segment \
                         for process '{pid}' is not contiguous: flow '{}' ends at '{}' but the next \
                         flow '{}' starts at '{}'.",
                        a.id, a.target_ref, b.id, b.source_ref
                    ),
                )
                .at_process(pid),
            );
        }
    }
}

/// One correlation hop — `PROCESS_UNKNOWN` (either `<process>:<node>` endpoint), then
/// `LINK_UNRESOLVED` (channel link) and `KEY_RESOLVABLE` (effective-key resolution). The two
/// endpoint refs are `<processId>:<nodeId>` (split on the first colon).
fn check_hop(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    file_ref: &str,
    corr: &crate::coverage::BusinessCorrelation,
    hop: &crate::coverage::Hop,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let link_err = |detail: String| -> LintDiagnostic {
        LintDiagnostic::error(
            codes::CONFIG_CORRELATION_LINK_UNRESOLVED,
            format!(
                "deployment {dep_label}: coverage file '{file_ref}' correlation '{}' hop '{}' → \
                 '{}' does not resolve: {detail}.",
                corr.id, hop.from_node, hop.to_node
            ),
        )
    };

    // Parse + PROCESS_UNKNOWN for each endpoint.
    let endpoints = [
        ("from", hop.from_node.as_str()),
        ("to", hop.to_node.as_str()),
    ];
    let mut parsed: Vec<Option<(&str, &str)>> = Vec::with_capacity(2);
    let mut ok = true;
    for (_which, node_ref) in endpoints {
        match node_ref.split_once(':') {
            Some((p, n)) if effective_def(deployment, p).is_some() => parsed.push(Some((p, n))),
            Some((p, _)) => {
                ok = false;
                parsed.push(None);
                out.push(
                    LintDiagnostic::error(
                        codes::CONFIG_CORRELATION_PROCESS_UNKNOWN,
                        format!(
                            "deployment {dep_label}: coverage file '{file_ref}' correlation '{}' \
                             hop endpoint '{node_ref}' references process '{p}', which is not a \
                             process in this deployment.",
                            corr.id
                        ),
                    )
                    .at_named(file_ref, &corr.id),
                );
            }
            None => {
                ok = false;
                parsed.push(None);
                out.push(
                    link_err(format!(
                        "endpoint '{node_ref}' is not a '<process>:<node>' reference"
                    ))
                    .at_named(file_ref, &corr.id),
                );
            }
        }
    }
    if !ok {
        return;
    }
    let (from_pid, from_node) = parsed[0].unwrap();
    let (to_pid, to_node) = parsed[1].unwrap();
    let from_def = effective_def(deployment, from_pid).unwrap();
    let to_def = effective_def(deployment, to_pid).unwrap();

    // Node existence — a missing endpoint node is an unresolved link.
    if from_def.node(from_node).is_err() {
        out.push(
            link_err(format!(
                "from-node '{from_node}' is not a node of process '{from_pid}'"
            ))
            .at_process(from_pid),
        );
        return;
    }
    if to_def.node(to_node).is_err() {
        out.push(
            link_err(format!(
                "to-node '{to_node}' is not a node of process '{to_pid}'"
            ))
            .at_process(to_pid),
        );
        return;
    }

    // ---- LINK_UNRESOLVED — the from-node emits on a channel the to-node consumes ----------
    let from_b = from_def.bindings_for(from_node);
    let emits = from_b.send.is_some() || from_b.reply.is_some();
    // An explicit `<q:send channel=X>` pins the channel; a `<q:reply>` (or a `destination`-send)
    // routes dynamically — the channel is not statically pinned, so it is accepted if the
    // consumer listens at all.
    let emit_channel: Option<&str> = from_b.send.as_ref().and_then(|s| s.channel.as_deref());
    let consumed = consumed_channels(to_def, to_node);
    if !emits {
        out.push(
            link_err(format!(
                "from-node '{from_node}' has no <q:send>/<q:reply> — it emits nothing on a channel"
            ))
            .at_node(from_pid, from_node),
        );
    } else if consumed.is_empty() {
        out.push(
            link_err(format!(
                "to-node '{to_node}' consumes no channel — it is neither a start-event \
                 <q:source> spawn nor an imec relay-wait (<q:messageCatchEvent>/<q:userTask>)"
            ))
            .at_node(to_pid, to_node),
        );
    } else if let Some(ch) = emit_channel {
        // Resolve an outbound `local://` indirection: a `<q:send channel="to-X">` naming a
        // `direction: outbound` channel that binds `local://<target>` delivers IN-PROCESS to
        // <target> (co-deployed collaboration), so the correlation link resolves
        // against <target> — the channel the consumer actually listens on — not the outbound
        // channel's own name. Any other channel (a direct inbound, or a non-local outbound bind)
        // resolves to itself.
        let effective = resolve_local_send_target(definitions, ch);
        if !consumed.iter().any(|c| c == effective) {
            out.push(
                link_err(format!(
                    "from-node <q:send channel=\"{ch}\"> but to-node '{to_node}' consumes only [{}]",
                    consumed.join(", ")
                ))
                .at_node(from_pid, from_node),
            );
        }
    }

    // ---- KEY_RESOLVABLE — the effective key resolves at BOTH endpoints --------------------
    check_hop_key(
        from_def, from_pid, from_node, to_def, to_pid, to_node, file_ref, corr, hop, dep_label, out,
    );
}

/// `KEY_RESOLVABLE` — the hop's EFFECTIVE key (`hop.key`, else the correlation's default `key`)
/// must resolve at both endpoints: the consumer reads it via a `<q:alias>` (whose `name` or
/// `expression` matches the key), and the emitter carries the value — a payload field (accepted:
/// carried in the body) or, for a `header.<field>` key, a `<q:header>` the `<q:send>`/`<q:reply>`
/// sets. A correlation value, NOT the `idempotencyKey`.
#[allow(clippy::too_many_arguments)]
fn check_hop_key(
    from_def: &ProcessDefinition,
    from_pid: &str,
    from_node: &str,
    to_def: &ProcessDefinition,
    to_pid: &str,
    to_node: &str,
    file_ref: &str,
    corr: &crate::coverage::BusinessCorrelation,
    hop: &crate::coverage::Hop,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let key = hop.key.as_deref().unwrap_or(corr.key.as_str());
    let key_err = |detail: String| -> LintDiagnostic {
        LintDiagnostic::error(
            codes::CONFIG_CORRELATION_KEY_RESOLVABLE,
            format!(
                "deployment {dep_label}: coverage file '{file_ref}' correlation '{}' hop '{}' → \
                 '{}' key '{key}' does not resolve: {detail}.",
                corr.id, hop.from_node, hop.to_node
            ),
        )
    };

    // Consumer must be able to READ the key — a `<q:alias>` whose name or expression matches it.
    let matched = to_def
        .bindings_for(to_node)
        .aliases
        .iter()
        .find(|a| a.name == key || a.expression == key);
    let Some(alias) = matched else {
        out.push(
            key_err(format!(
                "the consumer to-node '{to_node}' declares no <q:alias> resolving it — add a \
                 <q:alias name=\"{key}\" expression=\"…\"/> (payload or header.<field>)"
            ))
            .at_node(to_pid, to_node),
        );
        return;
    };

    // The FEEL expression that carries the value: the matched alias's expression (matched by
    // name), or the key itself (the key already IS the expression).
    let carrier = if alias.name == key {
        alias.expression.as_str()
    } else {
        key
    };

    // Emitter must CARRY the value. A `header.<field>` key requires the emitter's
    // `<q:send>`/`<q:reply>` to SET that header; a payload-sourced key rides the body
    // and is accepted (no static field check).
    if let Some(field) = carrier.strip_prefix("header.") {
        let from_b = from_def.bindings_for(from_node);
        let sets_header = from_b
            .send
            .as_ref()
            .is_some_and(|s| s.headers.iter().any(|h| h.name == field))
            || from_b
                .reply
                .as_ref()
                .is_some_and(|r| r.headers.iter().any(|h| h.name == field));
        if !sets_header {
            out.push(
                key_err(format!(
                    "the emitter from-node '{from_node}' sets no <q:header name=\"{field}\"> \
                     carrying it — add the header to its <q:send>/<q:reply>"
                ))
                .at_node(from_pid, from_node),
            );
        }
    }
}

/// Every `<q:store>`-style read/write must reference a declared store.
fn check_store_references(
    deployment: &LoadedDeployment,
    stores: &[StoreDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let declared: BTreeSet<&str> = stores.iter().map(|s| s.name.as_str()).collect();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            visit_nodes(process, &mut |_, node| {
                let mapping = match node {
                    Node::ServiceTask { data_mapping, .. } => data_mapping,
                    Node::DataTask { data_mapping, .. } => data_mapping,
                    _ => return,
                };
                for store in mapping
                    .store_reads
                    .iter()
                    .map(|r| r.store.as_str())
                    .chain(mapping.store_writes.iter().map(|w| w.store.as_str()))
                {
                    if !declared.contains(store) {
                        out.push(
                            LintDiagnostic::error(
                                codes::CONFIG_DATASTORE_INVALID,
                                format!(
                                    "deployment {dep_label}: process '{}' node '{}' references \
                                     data store '{store}', which datastores.yaml does not \
                                     declare",
                                    process.id,
                                    node.id()
                                ),
                            )
                            .at_node(&process.id, node.id()),
                        );
                    }
                }
            });
        }
    }
}

/// R14 security posture: a `.sutra` must reference datastore/broker credentials indirectly
/// (`env:` / `secret:` / `${…}`), never inline. A LITERAL username/password, or a URL/URI that
/// carries an embedded literal password, is deploy-blocking — the estate credentials live in
/// the mounted Secret, not the deployments ConfigMap (k8s has no field-level sensitivity in
/// ConfigMaps, so a literal there is exposed at ConfigMap RBAC/etcd treatment).
///
/// Detection is vocabulary-agnostic: it keys on the property's LAST dotted segment
/// (`.url` / `.username` / `.password`, plus their `-ref` misuse forms), so every connection
/// key family flags equally — `sql.password` and `broker.password` are both caught, and no key
/// prefix has to be enumerated for a store to be covered.
fn check_literal_credentials(
    stores: &[StoreDefinition],
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    // ---- datastores.yaml: the store's own connection wiring (url / username / password) ----
    for store in stores {
        for (key, value) in &store.properties {
            report_literal_credential(
                &format!("datastores.yaml store '{}' key '{key}'", store.name),
                key,
                value,
                dep_label,
                ("datastores.yaml", store.name.as_str()),
                out,
            );
        }
    }
    // ---- channels.yaml: broker user-info in the bind URI + username/password properties ----
    for def in definitions {
        let channel = def.binding.channel_name.as_str();
        // `bind:` is a reserved key held in bind_spec (an HTTP "POST /path" or an outbound
        // transport URI); an outbound broker bind may embed user-info.
        if let Some(bind) = &def.bind_spec {
            if url_has_literal_password(bind) {
                out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_CREDENTIALS_LITERAL,
                        format!(
                            "deployment {dep_label}: channel '{channel}' bind embeds a literal \
                             password in its URI — reference credentials with env:/secret:/${{…}} \
                             so they stay in the mounted estate Secret, never the deployments \
                             ConfigMap (R14)"
                        ),
                    )
                    .at_named("channels.yaml", channel),
                );
            }
        }
        for (key, value) in &def.properties {
            report_literal_credential(
                &format!("channel '{channel}' key '{key}'"),
                key,
                value,
                dep_label,
                ("channels.yaml", channel),
                out,
            );
        }
    }
}

/// Flag one connection property whose last dotted segment is a credential field carrying a
/// literal (a bare username/password, or a URL with an embedded literal password). The
/// diagnostic never echoes the value — it names the offending key so the secret is not leaked
/// into logs.
fn report_literal_credential(
    location: &str,
    key: &str,
    value: &str,
    dep_label: &str,
    (named_file, named): (&str, &str),
    out: &mut Vec<LintDiagnostic>,
) {
    if value.trim().is_empty() {
        return;
    }
    let segment = credential_key_segment(key);
    let is_secret_field = matches!(
        segment,
        "username" | "username-ref" | "password" | "password-ref"
    );
    let is_url_field = matches!(segment, "url" | "url-ref" | "uri" | "uri-ref");

    if is_secret_field && !is_credential_ref(value) {
        out.push(
            LintDiagnostic::error(
                codes::DEPLOY_CREDENTIALS_LITERAL,
                format!(
                    "deployment {dep_label}: {location} is a literal credential — reference it with \
                     env:/secret:/${{…}} so credentials stay in the mounted estate Secret, never \
                     the deployments ConfigMap (R14)"
                ),
            )
            .at_named(named_file, named),
        );
        return;
    }
    if is_url_field && url_has_literal_password(value) {
        out.push(
            LintDiagnostic::error(
                codes::DEPLOY_CREDENTIALS_LITERAL,
                format!(
                    "deployment {dep_label}: {location} embeds a literal password in its URL — \
                     reference credentials with env:/secret:/${{…}} (R14)"
                ),
            )
            .at_named(named_file, named),
        );
    }
}

/// The last dotted segment of a flattened property key (`sql.password` → `password`,
/// `sql.username-ref` → `username-ref`). Detection keys on this, so the key's PREFIX is
/// irrelevant — every connection key family is covered without enumerating it.
fn credential_key_segment(key: &str) -> &str {
    key.rsplit('.').next().unwrap_or(key)
}

/// A credential value that is an INDIRECTION reference, not a literal: `env:NAME`,
/// `secret:KEY` (R14 file-backed scheme), or any value carrying a `${…}` placeholder.
fn is_credential_ref(value: &str) -> bool {
    let v = value.trim();
    v.starts_with("env:") || v.starts_with("secret:") || v.contains("${")
}

/// A URL/URI value carrying a LITERAL embedded password: `scheme://user:pass@host…` whose
/// user-info password is not a `${…}`/`env:`/`secret:` reference, or a
/// `;password=<literal>` / `pwd=<literal>` connection parameter (the mssql
/// `sqlserver://host;password=…` form). Vocabulary-agnostic.
fn url_has_literal_password(value: &str) -> bool {
    // user-info form: scheme://[user[:pass]@]authority/…
    if let Some((_, after_scheme)) = value.split_once("://") {
        let authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
        let authority = &after_scheme[..authority_end];
        if let Some(at) = authority.find('@') {
            let userinfo = &authority[..at];
            // A ':' in the user-info means a password component; it is literal unless the whole
            // user-info uses a `${…}`/`env:`/`secret:` reference.
            if userinfo.contains(':') && !is_credential_ref(userinfo) {
                return true;
            }
        }
    }
    // Connection-string / query-parameter form: `;password=…`, `&password=…`, `?pwd=…`.
    let lower = value.to_ascii_lowercase();
    for key in ["password=", "pwd="] {
        let mut from = 0;
        while let Some(rel) = lower[from..].find(key) {
            let start = from + rel + key.len();
            let raw = &value[start..];
            let val = raw.split([';', '&', ' ']).next().unwrap_or(raw).trim();
            if !val.is_empty() && !is_credential_ref(val) {
                return true;
            }
            from = start;
        }
    }
    false
}

/// Archive `migrations/<store>/*.sql` are datastore-scoped. Every dir names a
/// declared store whose `sql.migrations` points at exactly that dir; scripts are
/// V-numbered well-formed SQL (`V<n>__<desc>.sql`, unique version per store, non-empty).
fn check_migrations(
    deployment: &LoadedDeployment,
    stores: &[StoreDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    // Store-declared side: sql.migrations must be the archive-normative layout and the
    // folder must contain at least one script.
    for store in stores {
        let Some(declared) = store
            .properties
            .get("sql.migrations")
            .map(String::as_str)
            .filter(|s| !s.trim().is_empty())
        else {
            continue;
        };
        let canonical = format!("migrations/{}", store.name);
        if declared != canonical {
            out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_MIGRATIONS_LAYOUT_INVALID,
                    format!(
                        "deployment {dep_label}: store '{}' declares sql.migrations \
                         '{declared}' — the sealed-archive layout requires '{canonical}' \
                         (migrations are datastore-scoped: migrations/<store>/)",
                        store.name
                    ),
                )
                .at_named("datastores.yaml", &store.name),
            );
            continue;
        }
        let prefix = format!("{}/", store.name);
        if !deployment.migrations.keys().any(|k| k.starts_with(&prefix)) {
            out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_MIGRATIONS_LAYOUT_INVALID,
                    format!(
                        "deployment {dep_label}: store '{}' declares sql.migrations \
                         '{canonical}' but the folder contains no .sql scripts",
                        store.name
                    ),
                )
                .at_named("datastores.yaml", &store.name),
            );
        }
    }

    // File side: every migration belongs to a declared store and is V-numbered.
    let declares_migrations: BTreeMap<&str, bool> = stores
        .iter()
        .map(|s| {
            (
                s.name.as_str(),
                s.properties
                    .get("sql.migrations")
                    .is_some_and(|v| !v.trim().is_empty()),
            )
        })
        .collect();
    let mut versions_by_store: BTreeMap<&str, BTreeMap<u64, &str>> = BTreeMap::new();
    for (key, artifact) in &deployment.migrations {
        let Some((store_dir, file_name)) = key.split_once('/') else {
            out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_MIGRATIONS_STORE_UNDECLARED,
                    format!(
                        "deployment {dep_label}: migrations/{key} is not inside a \
                         migrations/<store>/ folder (app SQL is datastore-scoped)"
                    ),
                )
                .at_file(format!("migrations/{key}")),
            );
            continue;
        };
        match declares_migrations.get(store_dir) {
            None => {
                out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_MIGRATIONS_STORE_UNDECLARED,
                        format!(
                            "deployment {dep_label}: migrations/{store_dir}/ names no store \
                             declared in datastores.yaml"
                        ),
                    )
                    .at_file(format!("migrations/{key}")),
                );
                continue;
            }
            Some(false) => {
                out.push(
                    LintDiagnostic::error(
                        codes::DEPLOY_MIGRATIONS_LAYOUT_INVALID,
                        format!(
                            "deployment {dep_label}: migrations/{store_dir}/ exists but store \
                             '{store_dir}' declares no sql.migrations — the scripts would \
                             never be applied"
                        ),
                    )
                    .at_file(format!("migrations/{key}")),
                );
                continue;
            }
            Some(true) => {}
        }
        match parse_migration_version(file_name) {
            None => out.push(
                LintDiagnostic::error(
                    codes::DEPLOY_MIGRATIONS_SCRIPT_INVALID,
                    format!(
                        "deployment {dep_label}: migrations/{key} is not V-numbered — scripts \
                         must be named V<n>__<description>.sql"
                    ),
                )
                .at_file(format!("migrations/{key}")),
            ),
            Some(version) => {
                if artifact.content.trim().is_empty() {
                    out.push(
                        LintDiagnostic::error(
                            codes::DEPLOY_MIGRATIONS_SCRIPT_INVALID,
                            format!("deployment {dep_label}: migrations/{key} is empty"),
                        )
                        .at_file(format!("migrations/{key}")),
                    );
                }
                if let Some(previous) = versions_by_store
                    .entry(store_dir)
                    .or_default()
                    .insert(version, file_name)
                {
                    out.push(
                        LintDiagnostic::error(
                            codes::DEPLOY_MIGRATIONS_SCRIPT_INVALID,
                            format!(
                                "deployment {dep_label}: migrations/{store_dir}/ declares \
                                 version V{version} twice ('{previous}' and '{file_name}')"
                            ),
                        )
                        .at_file(format!("migrations/{key}")),
                    );
                }
            }
        }
    }
}

// ---- projected data stores (design `datastore-schema-projection.md` §4.7) ----------------

/// Verify every store that declares a `structure:` block against the row shape its own
/// migrations build — statically, with no database and no credentials.
///
/// The pass is skipped entirely for a store with no `structure:`: that store is the opaque
/// key→JSON store it always was, and nothing here applies to it.
///
/// Three states, deliberately (the `PATH_UNVERIFIABLE` house style):
///
/// - **ERROR** — a definite fault: the type is not flat, a column is missing, a column cannot
///   hold the declared value space, the key is not a key over the projected columns, a column
///   name is unusable.
/// - **WARNING** — unprovable: the migrations are outside [`crate::ddl`]'s subset, the table is
///   created elsewhere, the declared schema is not an enumerable XSD, a column type is not
///   comparable, or the table carries columns the projection never writes.
/// - **silence** — the projection matches.
///
/// The table the projection lives in is the one named by the store's optional `sql.table`
/// property; failing that, the table named after the store; failing that, the single table the
/// migrations create. When none of those resolves, the store is unverifiable rather than wrong.
fn check_store_structures(
    deployment: &LoadedDeployment,
    stores: &[StoreDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for store in stores {
        let Some(structure) = &store.structure else {
            continue; // no `structure:` — the opaque store, unchanged
        };
        check_one_store_structure(deployment, store, structure, dep_label, out);
    }
}

fn check_one_store_structure(
    deployment: &LoadedDeployment,
    store: &StoreDefinition,
    structure: &StructureRef,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let name = store.name.as_str();
    let subject = format!("deployment {dep_label}: data store '{name}'");
    let at = |diagnostic: LintDiagnostic| diagnostic.at_named("datastores.yaml", name);

    // ---- 1. the declared type's fields ------------------------------------------------
    let fields = match structure_fields(deployment, structure) {
        StructureFields::Fields(fields) => fields,
        StructureFields::Unknown(reason) => {
            out.push(at(LintDiagnostic::error(
                codes::CONFIG_DATASTORE_INVALID,
                format!(
                    "{subject}: {reason}. Point 'structure.schema'/'structure.type' at a \
                     declared type, or remove the 'structure' block and keep the opaque store"
                ),
            )));
            return;
        }
        StructureFields::Unverifiable(reason) => {
            out.push(at(LintDiagnostic::warning(
                codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
                format!(
                    "{subject}: {reason}, so its declared structure could not be verified \
                     against migrations/{name}/ — it may be valid; it is simply not provable here"
                ),
            )));
            return;
        }
    };

    // ---- 2. flatness + column naming (design §4.2/§4.4) -------------------------------
    let projection = match structure.project(&fields) {
        Ok(projection) => projection,
        Err(fault) => {
            out.push(at(LintDiagnostic::error(
                fault.code(),
                format!("{subject}: {fault}"),
            )));
            return;
        }
    };
    // A declared field may not claim one of the engine's control columns (design §4.3): the
    // runtime maintains those itself, so the projection would fight it for the column.
    let claimed: Vec<String> = projection
        .fields
        .iter()
        .filter(|field| is_control_column(&field.column))
        .map(|field| format!("'{}' → '{}'", field.field, field.column))
        .collect();
    if !claimed.is_empty() {
        out.push(at(LintDiagnostic::error(
            codes::CONFIG_DATASTORE_COLUMN_NAME_INVALID,
            format!(
                "{subject}: structure type '{}' declares field(s) {} that resolve to a control \
                 column the engine owns ({}) — the runtime maintains those itself. Map them to \
                 different column names under 'columns:'",
                projection.type_name,
                claimed.join(", "),
                CONTROL_COLUMNS.join(", ")
            ),
        )));
        return;
    }

    // ---- 3. the effective table shape, from the package's own migrations --------------
    let scripts = store_migration_scripts(deployment, name);
    if scripts.is_empty() {
        out.push(at(LintDiagnostic::warning(
            codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
            format!(
                "{subject} declares a structure but ships no migrations under \
                 migrations/{name}/ — its table is created elsewhere, so the projection could \
                 not be verified (it may be valid; it is simply not provable here)"
            ),
        )));
        return;
    }
    let tables = match crate::ddl::parse_migrations(&scripts) {
        crate::ddl::DdlShape::Parsed(tables) => tables,
        crate::ddl::DdlShape::Unverifiable { file, reason } => {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
                    format!(
                        "{subject}: {file} could not be fully parsed — {reason} — so the \
                         effective table shape was not derived and the declared structure was \
                         not verified; no column diagnostic is raised for this store (it may be \
                         valid; it is simply not provable here)"
                    ),
                )
                .at_file(file.clone()),
            );
            return;
        }
    };
    let table = match select_projected_table(&tables, store) {
        Ok(table) => table,
        Err(reason) => {
            out.push(at(LintDiagnostic::warning(
                codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
                format!(
                    "{subject}: {reason}, so the declared structure was not verified (it may be \
                     valid; it is simply not provable here)"
                ),
            )));
            return;
        }
    };

    compare_projection_to_table(&projection, table, &subject, name, out);
}

/// The declared type's fields, or why they could not be enumerated.
enum StructureFields {
    /// The declared children, in declared order.
    Fields(Vec<sutra_xsd::FieldDecl>),
    /// A definite fault — the schema or the type does not exist (an ERROR).
    Unknown(String),
    /// The schema exists but its fields are not enumerable here (a WARNING).
    Unverifiable(String),
}

/// Resolve `structure.schema` + `structure.type` against the deployment's own compiled schemas.
///
/// Only a package `schemas/<folder>` XSD codec carries an enumerable, facet-bearing type table.
/// An engine-provided codec, a JSON-Schema / bundle codec folder, or an XSD set outside the
/// `sutra_xsd` subset is *unverifiable*, not wrong — the honest warning, never a false error.
fn structure_fields(deployment: &LoadedDeployment, structure: &StructureRef) -> StructureFields {
    let reference = structure.schema.trim();
    let builtin = reference
        .strip_prefix(CODEC_BUILTIN_URN_PREFIX)
        .unwrap_or(reference);
    if builtin_codec_names().contains(builtin) {
        return StructureFields::Unverifiable(format!(
            "'structure.schema' names the engine-provided codec '{builtin}', whose type set is \
             open (no declared field list to project)"
        ));
    }
    let local = user_codec_name(reference);
    let Some(xsds) = deployment.codecs.get(local) else {
        // A codec folder with no XSDs is a JSON-Schema or schema-bundle codec: it exists, but
        // this pass cannot enumerate its fields (its facets are not carried, see the design's
        // §4.5 note), so it is unprovable rather than unknown.
        let folder = format!("{local}/");
        if deployment
            .schema_files
            .keys()
            .any(|key| key.starts_with(&folder))
        {
            return StructureFields::Unverifiable(format!(
                "'structure.schema' names schema '{local}', which is not an XSD codec (only XSD \
                 schemas carry the declared facets a column comparison needs)"
            ));
        }
        return StructureFields::Unknown(format!(
            "'structure.schema' names '{}', which this package's schemas/ declares no codec for",
            structure.schema
        ));
    };
    let refs: Vec<&[u8]> = xsds.iter().map(|a| a.content.as_bytes()).collect();
    let set = match sutra_xsd::SchemaSet::compile(&refs) {
        Ok(set) => set,
        Err(e) => {
            return StructureFields::Unverifiable(format!(
                "schema '{local}' is outside the supported XSD subset ({e})"
            ))
        }
    };
    for schema in set.schemas() {
        if let Some(fields) = schema.fields_of(&structure.type_name) {
            return StructureFields::Fields(fields);
        }
    }
    StructureFields::Unknown(format!(
        "'structure.type' names '{}', which schema '{local}' declares neither as a type nor as a \
         root element",
        structure.type_name
    ))
}

/// This store's migration scripts, in migration-version order — `(archive path, SQL)` pairs.
/// Scripts whose name is not `V<n>__<desc>.sql` are skipped here; `check_migrations` already
/// reports them, and a second diagnostic would add nothing.
fn store_migration_scripts(deployment: &LoadedDeployment, store: &str) -> Vec<(String, String)> {
    let prefix = format!("{store}/");
    let mut scripts: Vec<(u64, String, String)> = deployment
        .migrations
        .iter()
        .filter_map(|(key, artifact)| {
            let file_name = key.strip_prefix(&prefix)?;
            let version = parse_migration_version(file_name)?;
            Some((
                version,
                format!("migrations/{key}"),
                artifact.content.clone(),
            ))
        })
        .collect();
    scripts.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    scripts
        .into_iter()
        .map(|(_, file, sql)| (file, sql))
        .collect()
}

/// Which of the migrations' tables the projection lives in: the declared `sql.table`, else the
/// table named after the store, else the single table the migrations create.
fn select_projected_table<'a>(
    tables: &'a BTreeMap<String, crate::ddl::TableShape>,
    store: &StoreDefinition,
) -> Result<&'a crate::ddl::TableShape, String> {
    let complete =
        |table: &'a crate::ddl::TableShape| -> Result<&'a crate::ddl::TableShape, String> {
            if table.complete {
                Ok(table)
            } else {
                Err(format!(
                    "table '{}' is altered by these migrations but created elsewhere, so its full \
                 column set is unknown",
                    table.name
                ))
            }
        };
    if let Some(declared) = store
        .properties
        .get("sql.table")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        return match tables.get(&declared.to_ascii_lowercase()) {
            Some(table) => complete(table),
            None => Err(format!(
                "its declared sql.table '{declared}' is created by no migration under \
                 migrations/{}/",
                store.name
            )),
        };
    }
    for candidate in [
        store.name.to_ascii_lowercase(),
        sutra_datastore::projection::default_column_name(&store.name),
    ] {
        if let Some(table) = tables.get(&candidate) {
            return complete(table);
        }
    }
    let created: Vec<&crate::ddl::TableShape> = tables.values().filter(|t| t.complete).collect();
    match created.as_slice() {
        [only] => Ok(only),
        [] => Err(format!(
            "its migrations create no table named '{}' (or any other)",
            store.name
        )),
        many => Err(format!(
            "its migrations create {} tables ({}) and none is named after the store — name the \
             projected one with 'sql.table: <table>' in datastores.yaml",
            many.len(),
            many.iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// The engine's control columns on a projected table (design §4.3): `store_key` (the PRIMARY
/// KEY), `rev` (the CAS revision) and `updated_at` (the write timestamp). They are the fixed part
/// of every projected table, maintained by the RUNTIME rather than derived from the declared
/// structure — so lint requires them, keys on `store_key`, and never reports them as unmapped.
/// Sourced from the runtime's own constants so the two sides cannot drift apart.
const CONTROL_COLUMNS: [&str; 3] = sutra_datastore::projected::CONTROL_COLUMNS;

/// The store-key column — the projected table's PRIMARY KEY.
const KEY_COLUMN: &str = sutra_datastore::projected::KEY_COLUMN;

fn is_control_column(name: &str) -> bool {
    CONTROL_COLUMNS
        .iter()
        .any(|control| control.eq_ignore_ascii_case(name))
}

/// The five table-comparison diagnostics: `COLUMN_MISSING`, `COLUMN_TYPE_MISMATCH`,
/// `KEY_MISMATCH` (errors) and `DDL_UNVERIFIABLE` / `COLUMN_UNMAPPED` (warnings).
///
/// The table is compared as the design's §4.3 shape — **control columns + declared field
/// columns**: the three control columns must exist (their absence is `COLUMN_MISSING`; the
/// runtime genuinely cannot serve the store without them), `store_key` must be the key, and none
/// of them is ever reported as an unmapped column.
///
/// Everything the author has to fix is reported in one pass — the checks never short-circuit
/// each other.
fn compare_projection_to_table(
    projection: &Projection,
    table: &crate::ddl::TableShape,
    subject: &str,
    store: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let at = |diagnostic: LintDiagnostic| diagnostic.at_named("datastores.yaml", store);
    let mut unprovable: Vec<String> = Vec::new();

    // ---- the control columns (design §4.3) -------------------------------------------
    let missing_control: Vec<&str> = CONTROL_COLUMNS
        .iter()
        .copied()
        .filter(|control| table.column(control).is_none())
        .collect();
    if !missing_control.is_empty() {
        out.push(at(LintDiagnostic::error(
            codes::CONFIG_DATASTORE_COLUMN_MISSING,
            format!(
                "{subject}: table '{}' is missing the control column(s) {} a projected store \
                 needs. A projected table is control columns ({}) plus one column per declared \
                 field — 'store_key' is its PRIMARY KEY, 'rev' backs compare-and-set and \
                 'updated_at' the write timestamp. Add them in a migration under \
                 migrations/{store}/",
                table.name,
                missing_control
                    .iter()
                    .map(|c| format!("'{c}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                CONTROL_COLUMNS.join(", ")
            ),
        )));
    }

    // ---- per declared field ----------------------------------------------------------
    for field in &projection.fields {
        let Some(column) = table.column(&field.column) else {
            out.push(at(LintDiagnostic::error(
                codes::CONFIG_DATASTORE_COLUMN_MISSING,
                format!(
                    "{subject}: declared field '{}' projects to column '{}', which table '{}' \
                     does not have. Add it in a new V-numbered migration under \
                     migrations/{store}/, or map the field to an existing column under \
                     'columns:'",
                    field.field, field.column, table.name
                ),
            )));
            continue;
        };
        match crate::ddl::column_fit(field.builtin, &field.facets, column) {
            crate::ddl::Fit::Fits => {}
            crate::ddl::Fit::Mismatch(reason) => out.push(at(LintDiagnostic::error(
                codes::CONFIG_DATASTORE_COLUMN_TYPE_MISMATCH,
                format!(
                    "{subject}: declared field '{}' → column '{}' of table '{}': {reason}. \
                     Widen the column in a new migration, or narrow the declared type",
                    field.field, column.name, table.name
                ),
            ))),
            crate::ddl::Fit::Unprovable(reason) => {
                unprovable.push(format!("'{}' → '{}': {reason}", field.field, column.name))
            }
        }
        if field.nullable && !column.nullable && !column.has_default {
            out.push(at(LintDiagnostic::error(
                codes::CONFIG_DATASTORE_COLUMN_TYPE_MISMATCH,
                format!(
                    "{subject}: declared field '{}' is optional, but column '{}' of table '{}' \
                     is NOT NULL with no DEFAULT — an absent value could not be written. Make \
                     the column nullable (or give it a DEFAULT), or make the field required",
                    field.field, column.name, table.name
                ),
            )));
        } else if !column.nullable && !column.has_default && column.from_alter {
            out.push(at(LintDiagnostic::error(
                codes::CONFIG_DATASTORE_COLUMN_TYPE_MISMATCH,
                format!(
                    "{subject}: column '{}' of table '{}' is added by an ALTER as NOT NULL with \
                     no DEFAULT, so rows written before that migration cannot satisfy it \
                     (declared field '{}'). Give the column a DEFAULT, or add it nullable and \
                     declare the field optional",
                    column.name, table.name, field.field
                ),
            )));
        }
    }

    // ---- the key (design §4.3: it is `store_key`, never a declared field) -------------
    //
    // A projected store reads, upserts, CAS-updates and deletes ONE row by its store key, so
    // that key — not any business field — is the row's identity. A table keyed on a declared
    // field would let two store keys collide on one row.
    let key = table.key();
    let key_is_store_key = matches!(key, [only] if only.eq_ignore_ascii_case(KEY_COLUMN));
    if key.is_empty() {
        out.push(at(LintDiagnostic::error(
            codes::CONFIG_DATASTORE_KEY_MISMATCH,
            format!(
                "{subject}: table '{}' declares no PRIMARY KEY (nor a unique constraint) — a \
                 projected store reads, upserts and CAS-updates one row by its store key, so \
                 '{KEY_COLUMN}' must be the table's PRIMARY KEY. Add it in a migration, or \
                 remove the 'structure' block and keep the opaque store",
                table.name
            ),
        )));
    } else if !key_is_store_key {
        out.push(at(LintDiagnostic::error(
            codes::CONFIG_DATASTORE_KEY_MISMATCH,
            format!(
                "{subject}: table '{}' is keyed on {} — a projected store addresses one row by \
                 its store key, so '{KEY_COLUMN}' must be the PRIMARY KEY (a declared field is \
                 never the key; two store keys would collide on one row). Re-key the table in a \
                 migration",
                table.name,
                key.iter()
                    .map(|c| format!("'{c}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )));
    }

    // ---- columns the projection never writes -----------------------------------------
    // The control columns are written by the RUNTIME, not by the projection, so they are never
    // "unmapped" — reporting them would make every correct projected table warn.
    let unmapped: Vec<&crate::ddl::ColumnDef> = table
        .columns
        .iter()
        .filter(|column| {
            !is_control_column(&column.name) && projection.by_column(&column.name).is_none()
        })
        .collect();
    if !unmapped.is_empty() {
        let described: Vec<String> = unmapped
            .iter()
            .map(|column| {
                if column.nullable || column.has_default {
                    format!("'{}'", column.name)
                } else {
                    format!("'{}' (NOT NULL with no DEFAULT)", column.name)
                }
            })
            .collect();
        let blocking = unmapped
            .iter()
            .any(|column| !column.nullable && !column.has_default);
        out.push(at(LintDiagnostic::warning(
            codes::CONFIG_DATASTORE_COLUMN_UNMAPPED,
            format!(
                "{subject}: table '{}' has {} column(s) the projection never writes: {}. {}",
                table.name,
                unmapped.len(),
                described.join(", "),
                if blocking {
                    "A NOT NULL column with no DEFAULT would make every insert fail — give it a \
                     DEFAULT, declare it in the structure type, or drop it"
                } else {
                    "Usually fine (a legacy or operator column); declare it in the structure \
                     type if the projection should own it"
                }
            ),
        )));
    }

    // ---- what could not be proven ----------------------------------------------------
    if !unprovable.is_empty() {
        out.push(at(LintDiagnostic::warning(
            codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
            format!(
                "{subject}: {} declared field(s) against table '{}' could be neither confirmed \
                 nor refuted — {}. They may be valid; they are simply not provable here",
                unprovable.len(),
                table.name,
                unprovable.join("; ")
            ),
        )));
    }
}

/// `V<n>__<desc>.sql` → n.
fn parse_migration_version(file_name: &str) -> Option<u64> {
    let rest = file_name.strip_prefix('V')?;
    let (digits, tail) = rest.split_once("__")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let desc = tail.strip_suffix(".sql")?;
    if desc.is_empty() {
        return None;
    }
    digits.parse().ok()
}

// ---- node reference resolution ---------------------------------------------------------

/// Template / script / decision / channel references on flow nodes must resolve inside
/// the deployment (cross-deployment references do not exist).
fn check_node_references(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let channel_names: BTreeSet<&str> = definitions
        .iter()
        .map(|d| d.binding.channel_name.as_str())
        .collect();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            visit_nodes(process, &mut |owner, node| {
                match node {
                    Node::ServiceTask {
                        id, implementation, ..
                    } => {
                        let implementation = implementation.trim();
                        if is_template_implementation(implementation)
                            && !deployment.templates.contains_key(implementation)
                        {
                            out.push(
                                LintDiagnostic::error(
                                    codes::RESOLVE_TEMPLATE_UNKNOWN,
                                    format!(
                                        "deployment {dep_label}: process '{}' serviceTask \
                                         '{id}' references template '{implementation}', which \
                                         templates/ does not provide",
                                        process.id
                                    ),
                                )
                                .at_node(&process.id, id),
                            );
                        }
                        if let Some(channel) =
                            implementation.strip_prefix(sutra_bpmn::model::CHANNEL_CALL_PREFIX)
                        {
                            if !channel_names.contains(channel) {
                                out.push(
                                    LintDiagnostic::error(
                                        codes::CONFIG_CHANNEL_OUTBOUND_UNKNOWN,
                                        format!(
                                            "deployment {dep_label}: process '{}' channel-call \
                                             task '{id}' targets channel '{channel}', which \
                                             channels.yaml does not declare",
                                            process.id
                                        ),
                                    )
                                    .at_node(&process.id, id),
                                );
                            }
                        }
                    }
                    Node::ScriptTask {
                        id, script_file, ..
                    } if !deployment.scripts.contains_key(script_file.trim()) => {
                        out.push(
                            LintDiagnostic::error(
                                codes::DEPLOY_ARTIFACT_REF_UNRESOLVED,
                                format!(
                                    "deployment {dep_label}: process '{}' scriptTask '{id}' \
                                     references script '{script_file}', which scripts/ does \
                                     not provide",
                                    process.id
                                ),
                            )
                            .at_node(&process.id, id),
                        );
                    }
                    Node::BusinessRuleTask {
                        id, decision_file, ..
                    } if !deployment.rules.contains_key(decision_file.trim()) => {
                        out.push(
                            LintDiagnostic::error(
                                codes::DEPLOY_ARTIFACT_REF_UNRESOLVED,
                                format!(
                                    "deployment {dep_label}: process '{}' businessRuleTask \
                                     '{id}' references decision '{decision_file}', which \
                                     rules/ does not provide",
                                    process.id
                                ),
                            )
                            .at_node(&process.id, id),
                        );
                    }
                    _ => {}
                }
                // `<q:send channel>` targets a declared channel.
                if let Some(send) = &owner.bindings_for(node.id()).send {
                    if let Some(channel) = &send.channel {
                        if !channel_names.contains(channel.as_str()) {
                            out.push(
                                LintDiagnostic::error(
                                    codes::CONFIG_CHANNEL_OUTBOUND_UNKNOWN,
                                    format!(
                                        "deployment {dep_label}: process '{}' node '{}' \
                                         <q:send channel=\"{channel}\"> targets a channel \
                                         channels.yaml does not declare",
                                        process.id,
                                        node.id()
                                    ),
                                )
                                .at_node(&process.id, node.id()),
                            );
                        }
                    }
                }
                // `<q:source complexValidator>` refs must exist under rules/.
                for source in &owner.bindings_for(node.id()).sources {
                    for validator in &source.complex_validators {
                        if !deployment.rules.contains_key(validator.trim()) {
                            out.push(
                                LintDiagnostic::error(
                                    codes::DEPLOY_ARTIFACT_REF_UNRESOLVED,
                                    format!(
                                        "deployment {dep_label}: process '{}' <q:source \
                                         channel='{}'> references complexValidator \
                                         '{validator}', which rules/ does not provide",
                                        process.id, source.channel
                                    ),
                                )
                                .at_node(&process.id, node.id()),
                            );
                        }
                    }
                }
            });
        }
    }
}

fn is_template_implementation(implementation: &str) -> bool {
    implementation.ends_with(".hbs")
        || implementation.ends_with(".xsl")
        || implementation.ends_with(".xslt")
}

// ---- FEEL checks -------------------------------------------------------------------------

/// Parse + determinism-check the replay-bound FEEL sites: `<q:alias>` expressions and
/// `<q:dispatch>` case predicates (`require_pure`); sequence-flow conditions parse-check
/// only (evaluated per token pass — not replay-bound).
fn check_feel_sites(deployment: &LoadedDeployment, dep_label: &str, out: &mut Vec<LintDiagnostic>) {
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            visit_nodes(process, &mut |owner, node| {
                let bindings = owner.bindings_for(node.id());
                let node_anchor = || DiagnosticAnchor::BpmnNode {
                    process: process.id.clone(),
                    node: node.id().to_string(),
                };
                for alias in &bindings.aliases {
                    let site = format!(
                        "process '{}' node '{}' alias '{}'",
                        process.id,
                        node.id(),
                        alias.name
                    );
                    check_feel_pure(&alias.expression, &site, node_anchor(), dep_label, out);
                }
                if let Some(dispatch) = &bindings.dispatch {
                    for case in &dispatch.cases {
                        let site = format!(
                            "process '{}' node '{}' <q:case when=\"{}\">",
                            process.id,
                            node.id(),
                            case.when
                        );
                        check_feel_pure(&case.when, &site, node_anchor(), dep_label, out);
                    }
                }
            });
            for flow in process.flows() {
                if let Some(condition) = &flow.condition {
                    if let Err(e) = sutra_feel::expressions::parse(condition) {
                        out.push(
                            LintDiagnostic::error(
                                &e.code.clone(),
                                format!(
                                    "deployment {dep_label}: process '{}' flow '{}' condition \
                                     does not parse: {}",
                                    process.id, flow.id, e.message
                                ),
                            )
                            .at_node(&process.id, &flow.id),
                        );
                    }
                }
            }
        }
    }
}

fn check_feel_pure(
    expression: &str,
    site: &str,
    anchor: DiagnosticAnchor,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    match sutra_feel::expressions::parse(expression) {
        Err(e) => out.push(
            LintDiagnostic::error(
                &e.code.clone(),
                format!(
                    "deployment {dep_label}: {site} does not parse: {}",
                    e.message
                ),
            )
            .at_anchor(anchor),
        ),
        Ok(expr) => {
            if let Err(e) = sutra_feel::expressions::require_pure_expr(&expr, site) {
                out.push(
                    LintDiagnostic::error(
                        &e.code.clone(),
                        format!("deployment {dep_label}: {}", e.message),
                    )
                    .at_anchor(anchor),
                );
            }
        }
    }
}

// ---- template input validation (type safety) ------------------------------------------------

/// The fixed render-context roots the executor always supplies.
const FIXED_ROOTS: [&str; 4] = ["uuid", "now", "vars", "event"];

/// A process's resolved intake for the field checks.
struct Intake {
    payload_var: String,
    codec_label: String,
    fields: IntakeFields,
}

/// How a resolved intake's payload paths are field-checked.
enum IntakeFields {
    /// A single pinned message type: resolve each path against `shape` (`None` = an opaque
    /// codec / no schema ⇒ an Unverifiable WARNING).
    Single {
        message_type: String,
        shape: Option<SchemaShape>,
    },
    /// A `messageTypePattern` (or multi-type codec) with no pin and no enumerable
    /// candidates — a blanket FIELD_TYPE_UNPINNED warning.
    Unpinned,
    /// C5 — a `messageTypePattern` over an enumerable codec: the matching message types and
    /// their shapes, for the field-intersection check.
    Candidates {
        types: Vec<String>,
        shapes: Vec<SchemaShape>,
    },
}

fn resolve_intake(
    deployment: &LoadedDeployment,
    process: &ProcessDefinition,
    definitions: &[ChannelDefinition],
) -> Option<Intake> {
    let mut source: Option<&SourceBinding> = None;
    for node in process.start_events() {
        if let Some(s) = process.bindings_for(node.id()).source() {
            if source.is_some() {
                return None; // more than one intake — ambiguous, skip
            }
            source = Some(s);
        }
    }
    let source = source?;
    let payload_var = source.name.clone();
    let def = definitions
        .iter()
        .find(|d| d.binding.channel_name == source.channel);
    let codec_name = def.map(|d| d.binding.codec.clone()).unwrap_or_default();
    let codec_label = if codec_name.trim().is_empty() {
        "none".to_string()
    } else {
        codec_name.clone()
    };
    // No channel / no codec — an intake-less flow whose payload paths cannot be shaped.
    if codec_name.trim().is_empty() || def.is_none() {
        return Some(Intake {
            payload_var,
            codec_label,
            fields: IntakeFields::Single {
                message_type: String::new(),
                shape: None,
            },
        });
    }
    // C5 — a messageTypePattern intake resolves to every matching type's shape.
    if let Some(pattern) = &source.message_type_pattern {
        return Some(pattern_intake(
            deployment,
            payload_var,
            codec_label,
            &codec_name,
            pattern,
        ));
    }
    let declared = declared_types(deployment, &codec_name);
    let pinned_type = match &source.message_type_value {
        Some(value) => Some(value.clone()),
        None => match declared.as_list() {
            [only] => Some(only.clone()),
            [] => None, // open codec, no pin — shape resolves to none (Unverifiable)
            _ => {
                return Some(Intake {
                    payload_var,
                    codec_label,
                    fields: IntakeFields::Unpinned,
                })
            }
        },
    };
    let message_type = pinned_type.unwrap_or_default();
    let shape = resolve_shape(deployment, &codec_name, &message_type);
    Some(Intake {
        payload_var,
        codec_label,
        fields: IntakeFields::Single {
            message_type,
            shape,
        },
    })
}

/// C5 — a `messageTypePattern` source. When the codec has an enumerable type set the intake
/// carries the pattern-matching types + their shapes so the field check can intersect them.
/// An open-typed codec, an uncompilable pattern, or a pattern matching no shapeable type
/// stays type-not-pinned (blanket WARN) — the dead-pattern ERROR is the uniqueness check's.
fn pattern_intake(
    deployment: &LoadedDeployment,
    payload_var: String,
    codec_label: String,
    codec_name: &str,
    pattern: &str,
) -> Intake {
    let Ok(re) = Regex::new(&format!("^(?:{pattern})$")) else {
        return Intake {
            payload_var,
            codec_label,
            fields: IntakeFields::Unpinned,
        };
    };
    let declared = declared_types(deployment, codec_name);
    let types: Vec<String> = declared
        .as_list()
        .iter()
        .filter(|t| re.is_match(t))
        .cloned()
        .collect();
    let shapes: Vec<SchemaShape> = types
        .iter()
        .filter_map(|t| resolve_shape(deployment, codec_name, t))
        .collect();
    let fields = if shapes.is_empty() {
        IntakeFields::Unpinned
    } else {
        IntakeFields::Candidates { types, shapes }
    };
    Intake {
        payload_var,
        codec_label,
        fields,
    }
}

/// A process's own roots: `<q:variables>` declarations, alias names, payload-var names —
/// plus (a deliberate superset over the reference pass, to stay conservative until
/// the analyzers converge) data-mapping targets and `<q:output variable>` bindings.
fn own_roots(process: &ProcessDefinition) -> BTreeSet<String> {
    let mut roots: BTreeSet<String> = process
        .declared_variables
        .iter()
        .map(|d| d.name.clone())
        .collect();
    visit_nodes(process, &mut |owner, node| {
        let bindings = owner.bindings_for(node.id());
        for alias in &bindings.aliases {
            roots.insert(alias.name.clone());
        }
        for source in &bindings.sources {
            roots.insert(source.name.clone());
        }
        if let Some(output) = &bindings.output {
            roots.insert(output.variable.clone());
        }
        let mapping = match node {
            Node::ServiceTask { data_mapping, .. } => Some(data_mapping),
            Node::DataTask { data_mapping, .. } => Some(data_mapping),
            _ => None,
        };
        if let Some(mapping) = mapping {
            for read in &mapping.store_reads {
                roots.insert(read.target_var.clone());
            }
            for assignment in &mapping.assignments {
                roots.insert(assignment.target_var.clone());
            }
        }
    });
    roots
}

/// The sibling process ids a process dispatches into (call activities + `<q:dispatch>`).
fn callees_of(process: &ProcessDefinition, module_pids: &BTreeSet<String>) -> BTreeSet<String> {
    let mut callees = BTreeSet::new();
    let mut add = |called: &str| {
        if called.is_empty() {
            return;
        }
        if module_pids.contains(called) {
            callees.insert(called.to_string());
        } else if let Some((_, local)) = called.rsplit_once(':') {
            if module_pids.contains(local) {
                callees.insert(local.to_string());
            }
        }
    };
    visit_nodes(process, &mut |owner, node| {
        if let Node::CallActivity { called_element, .. } = node {
            add(called_element);
        }
        if let Some(dispatch) = &owner.bindings_for(node.id()).dispatch {
            if let Some(default) = &dispatch.default_called_element {
                add(default);
            }
            for case in &dispatch.cases {
                add(&case.called_element);
            }
        }
    });
    callees
}

/// reachable(P) = own(P) ∪ ⋃ own(Q) over every transitive caller Q (a dispatched
/// sub-process inherits its caller's execution context).
fn reachable_roots(deployment: &LoadedDeployment) -> BTreeMap<String, BTreeSet<String>> {
    let mut own: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut callers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let module_pids: BTreeSet<String> = deployment.processes.keys().cloned().collect();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            own.insert(process.id.clone(), own_roots(process));
            for callee in callees_of(process, &module_pids) {
                callers
                    .entry(callee)
                    .or_default()
                    .insert(process.id.clone());
            }
        }
    }
    let mut reachable = BTreeMap::new();
    for pid in own.keys() {
        let mut owners = BTreeSet::new();
        let mut stack = vec![pid.clone()];
        while let Some(current) = stack.pop() {
            if !owners.insert(current.clone()) {
                continue; // cycle / diamond guard
            }
            if let Some(direct) = callers.get(&current) {
                stack.extend(direct.iter().cloned());
            }
        }
        let mut roots = BTreeSet::new();
        for owner in owners {
            if let Some(r) = own.get(&owner) {
                roots.extend(r.iter().cloned());
            }
        }
        reachable.insert(pid.clone(), roots);
    }
    reachable
}

/// The set of node ids reachable *after* a wait state — every node on a path forward from a wait
/// node's successors. Computed over the top-level process flow graph.
fn nodes_after_wait(process: &ProcessDefinition) -> BTreeSet<String> {
    let mut after: BTreeSet<String> = BTreeSet::new();
    let mut stack: Vec<String> = Vec::new();
    for node in process.nodes() {
        if node.is_wait_state() {
            for flow in process.outgoing(node.id()) {
                stack.push(flow.target_ref.clone());
            }
        }
    }
    while let Some(n) = stack.pop() {
        if !after.insert(n.clone()) {
            continue; // already visited — cycle / diamond guard
        }
        for flow in process.outgoing(&n) {
            stack.push(flow.target_ref.clone());
        }
    }
    after
}

/// Fail-closed authoring gate: a `<q:variable transient="true">` must not be READ by a
/// node reachable *after* a wait state. A transient variable is held in memory only and dropped at
/// the park, so a post-wait read silently yields null. The check walks the process flow graph
/// forward from every wait node and flags a transient-variable read in a reachable service-task
/// template — the primary read surface. (A transient read in a downstream FEEL flow-condition or
/// `<q:param>` expression is not caught here; it degrades safely to null at runtime rather than
/// leaking or corrupting — the persistence layer never carries the value across the wait.)
fn check_transient_reads(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let engine = HandlebarsTemplateEngine::new();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            let transient: BTreeSet<&str> = process
                .declared_variables
                .iter()
                .filter(|d| d.transient)
                .map(|d| d.name.as_str())
                .collect();
            if transient.is_empty() {
                continue; // nothing to guard
            }
            let after_wait = nodes_after_wait(process);
            if after_wait.is_empty() {
                continue; // no wait state — a transient never has to survive a park
            }
            for node in process.nodes() {
                let Node::ServiceTask {
                    id, implementation, ..
                } = node
                else {
                    continue;
                };
                if !after_wait.contains(node.id()) {
                    continue;
                }
                let implementation = implementation.trim();
                if !implementation.ends_with(".hbs") {
                    continue; // only the Handlebars analyzer exists Rust-side
                }
                let Some(template) = deployment.templates.get(implementation) else {
                    continue; // a missing template is already an ERROR elsewhere
                };
                let analysis = engine.analyze(template.content.as_bytes());
                for root in &analysis.roots {
                    if transient.contains(root.as_str()) {
                        out.push(
                            LintDiagnostic::error(
                                codes::CONFIG_TRANSIENT_READ_AFTER_WAIT,
                                format!(
                                    "deployment {dep_label}: process '{}' serviceTask '{id}' template \
                                     '{implementation}' reads transient variable '{root}' after a wait \
                                     state — a @transient variable is never persisted and is gone on \
                                     resume. Drop @transient, or read it only before the wait.",
                                    process.id
                                ),
                            )
                            .at_node(&process.id, id),
                        );
                    }
                }
            }
        }
    }
}

// ---- B2 (deploy-time static lint): read-but-never-initialised variable ----------------

/// B2 slice 1 — the statically-SOUND read-before-init check. A runtime "read before write" can
/// never distinguish a legitimate null-guard from a bug; this instead flags the provable case:
/// a `<q:variables>`-declared variable that is READ somewhere in the process but has NO writer of
/// any kind — no `@source` intake, no data-task output, no `<q:output variable>` capture — so every
/// read of it necessarily yields null.
///
/// Posture is deliberately conservative (correctness over coverage), because an *un-enumerated*
/// writer kind would false-block a deploy: the diagnostic is an advisory **WARNING**, and the
/// whole check is SUPPRESSED for any process that carries an opaque writer (see
/// [`has_opaque_writer`]) — a `scriptTask` / `businessRuleTask` / non-template serviceTask can merge
/// variable names the analyzer cannot see, so nothing in such a process is provably never-written.
fn check_never_initialized(
    deployment: &LoadedDeployment,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let engine = HandlebarsTemplateEngine::new();
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            // A process that can initialise arbitrary (statically-invisible) variable names cannot
            // support a "provably never written" claim about ANY of its variables — bail whole.
            if has_opaque_writer(process) {
                continue;
            }
            let writers = variable_writers(process);
            let readers = variable_readers(process, deployment, &engine);
            for var in &process.declared_variables {
                // `@source` is itself an intake initialiser.
                if var.source.is_some() {
                    continue;
                }
                // Only variables that are actually READ are a concern — a declared-but-unread
                // variable is dead config, a different (and non-fail-closed) matter.
                if !readers.contains(&var.name) {
                    continue;
                }
                if writers.contains(&var.name) {
                    continue; // some enumerated writer initialises it
                }
                out.push(
                    LintDiagnostic::warning(
                        codes::CONFIG_VARIABLE_NEVER_INITIALIZED,
                        format!(
                            "deployment {dep_label}: process '{}' declares variable '{}', which is read \
                             but never initialised — it has no @source, no data-task output \
                             (<dataOutputAssociation>/<assignment>/store read) and no <q:output \
                             variable> writer, so every read yields null. Add an initialiser or remove \
                             the declaration.",
                            process.id, var.name
                        ),
                    )
                    .at_process(&process.id),
                );
            }
        }
    }
}

/// True when the process (including its sub-process tree) carries a node that can write variable
/// names the static analyzer cannot enumerate — the executor merges the node's produced output
/// wholesale into the shared scope:
/// - a `scriptTask` (its parsed script output is merged),
/// - a `businessRuleTask` (its decision result is merged),
/// - a NON-template `serviceTask` with no declared `<dataOutputAssociation>` outputs (a decision-
///   suffix or registered task function whose full output is merged when no outputs pin it).
///
/// A `.hbs` template serviceTask is NOT opaque — it produces only `responseBody` plus its
/// `<q:output variable>` (both enumerated as writers), never arbitrary names.
fn has_opaque_writer(process: &ProcessDefinition) -> bool {
    let mut opaque = false;
    visit_nodes(process, &mut |_, node| match node {
        Node::ScriptTask { .. } | Node::BusinessRuleTask { .. } => opaque = true,
        Node::ServiceTask {
            implementation,
            data_mapping,
            ..
        } => {
            let is_template = implementation.trim().ends_with(".hbs");
            if !is_template && data_mapping.outputs.is_empty() {
                opaque = true;
            }
        }
        _ => {}
    });
    opaque
}

/// Every variable name written by an enumerated (statically-visible) writer anywhere in the
/// process tree: intake payload vars (`<q:source name>`), alias keys (`<q:alias name>`),
/// `<q:output variable>` captures, data-task `<dataOutputAssociation>` outputs, store reads
/// (`target_var`) and FEEL `<assignment>` targets — plus `responseBody`, which every template
/// serviceTask binds.
fn variable_writers(process: &ProcessDefinition) -> BTreeSet<String> {
    let mut writers: BTreeSet<String> = BTreeSet::new();
    visit_nodes(process, &mut |owner, node| {
        let bindings = owner.bindings_for(node.id());
        for source in &bindings.sources {
            writers.insert(source.name.clone());
        }
        for alias in &bindings.aliases {
            writers.insert(alias.name.clone());
        }
        if let Some(output) = &bindings.output {
            writers.insert(output.variable.clone());
        }
        let mapping = match node {
            Node::ServiceTask {
                data_mapping,
                implementation,
                ..
            } => {
                if implementation.trim().ends_with(".hbs") {
                    writers.insert("responseBody".to_string());
                }
                Some(data_mapping)
            }
            Node::DataTask { data_mapping, .. } => Some(data_mapping),
            _ => None,
        };
        if let Some(mapping) = mapping {
            for out_var in &mapping.outputs {
                writers.insert(out_var.clone());
            }
            for read in &mapping.store_reads {
                writers.insert(read.target_var.clone());
            }
            for assignment in &mapping.assignments {
                writers.insert(assignment.target_var.clone());
            }
        }
    });
    writers
}

/// Every variable name READ anywhere in the process tree — the path ROOT of every FEEL read site
/// [`check_navigation_paths`] walks (`<q:alias>` / `<q:dispatch>` case `when` / `<q:source
/// idempotencyKey>` / `<q:simpleValidator path>` / `<q:param>` expressions / sequence-flow
/// conditions), the read side of data-task mappings (`<assignment>` from-expressions, store-read /
/// store-write key expressions, a store-write `value_var`), and every `{{root}}` a serviceTask
/// template dereferences. Roots that fail to parse as FEEL are ignored (a different lint's concern).
fn variable_readers(
    process: &ProcessDefinition,
    deployment: &LoadedDeployment,
    engine: &HandlebarsTemplateEngine,
) -> BTreeSet<String> {
    let mut readers: BTreeSet<String> = BTreeSet::new();
    visit_nodes(process, &mut |owner, node| {
        let bindings = owner.bindings_for(node.id());
        for alias in &bindings.aliases {
            add_feel_roots(&alias.expression, &mut readers);
        }
        if let Some(dispatch) = &bindings.dispatch {
            for case in &dispatch.cases {
                add_feel_roots(&case.when, &mut readers);
            }
        }
        for source in &bindings.sources {
            if let Some(key) = &source.dedup_key {
                add_feel_roots(key, &mut readers);
            }
            for sv in &source.simple_validators {
                add_feel_roots(&sv.path, &mut readers);
            }
        }
        if let Node::ServiceTask { params, .. } = node {
            for param in params {
                add_feel_roots(&param.expression, &mut readers);
            }
        }
        let mapping = match node {
            Node::ServiceTask { data_mapping, .. } | Node::DataTask { data_mapping, .. } => {
                Some(data_mapping)
            }
            _ => None,
        };
        if let Some(mapping) = mapping {
            for assignment in &mapping.assignments {
                add_feel_roots(&assignment.expression, &mut readers);
            }
            for read in &mapping.store_reads {
                add_feel_roots(&read.key_expression, &mut readers);
            }
            for write in &mapping.store_writes {
                add_feel_roots(&write.key_expression, &mut readers);
                readers.insert(write.value_var.clone()); // a bare variable read
            }
        }
        // Template `{{root}}` reads — only the Handlebars analyzer exists Rust-side.
        if let Node::ServiceTask { implementation, .. } = node {
            let implementation = implementation.trim();
            if implementation.ends_with(".hbs") {
                if let Some(template) = deployment.templates.get(implementation) {
                    let analysis = engine.analyze(template.content.as_bytes());
                    for root in &analysis.roots {
                        readers.insert(root.clone());
                    }
                }
            }
        }
    });
    // Sequence-flow conditions across the process AND its sub-process tree.
    for_each_flow_condition(process, &mut |condition| {
        add_feel_roots(condition, &mut readers)
    });
    readers
}

/// Parse `expr` as FEEL and insert the ROOT (first segment) of every dereferenced path into
/// `readers`. A FEEL parse error is ignored — malformed expressions are `check_feel_sites`' concern.
fn add_feel_roots(expr: &str, readers: &mut BTreeSet<String>) {
    if let Ok(paths) = sutra_feel::expressions::paths(expr) {
        for path in &paths {
            readers.insert(path.root().to_string());
        }
    }
}

/// Apply `f` to every sequence-flow `conditionExpression` in the process and, recursively, in its
/// embedded / transaction / event / ad-hoc sub-processes (which share the process variable scope).
fn for_each_flow_condition(process: &ProcessDefinition, f: &mut impl FnMut(&str)) {
    for flow in process.flows() {
        if let Some(condition) = &flow.condition {
            f(condition);
        }
    }
    for node in process.nodes() {
        match node {
            Node::SubProcess { inner, .. }
            | Node::TransactionSubProcess { inner, .. }
            | Node::EventSubProcess { inner, .. }
            | Node::AdHocSubProcess { inner, .. } => for_each_flow_condition(inner, f),
            _ => {}
        }
    }
}

fn check_template_inputs(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let engine = HandlebarsTemplateEngine::new();
    let reachable = reachable_roots(deployment);
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            let intake = resolve_intake(deployment, process, definitions);
            let mut available: BTreeSet<String> =
                FIXED_ROOTS.iter().map(|s| s.to_string()).collect();
            available.insert(
                intake
                    .as_ref()
                    .map(|i| i.payload_var.clone())
                    .unwrap_or_else(|| "payload".to_string()),
            );
            if let Some(roots) = reachable.get(&process.id) {
                available.extend(roots.iter().cloned());
            }
            visit_nodes(process, &mut |_, node| {
                let Node::ServiceTask {
                    id,
                    implementation,
                    params,
                    ..
                } = node
                else {
                    return;
                };
                let implementation = implementation.trim();
                if !implementation.ends_with(".hbs") {
                    return; // only the Handlebars analyzer exists Rust-side
                }
                let Some(template) = deployment.templates.get(implementation) else {
                    return; // missing template is already an ERROR (reference resolution)
                };
                let analysis = engine.analyze(template.content.as_bytes());
                let site = format!("serviceTask '{id}' template '{implementation}'");
                let mut available_here = available.clone();
                for param in params {
                    available_here.insert(param.name.clone());
                }
                for root in &analysis.roots {
                    if !available_here.contains(root) {
                        out.push(
                            LintDiagnostic::error(
                                codes::CONFIG_TEMPLATE_INPUT_UNSATISFIED,
                                format!(
                                    "deployment {dep_label}: process '{}' ({site}) references \
                                     input '{root}', but the deployment provides no such input \
                                     (available: {:?})",
                                    process.id,
                                    available_here.iter().collect::<Vec<_>>()
                                ),
                            )
                            .at_node(&process.id, id),
                        );
                    }
                }
                check_payload_fields(
                    &analysis,
                    intake.as_ref(),
                    process,
                    id,
                    &site,
                    dep_label,
                    out,
                );
                // Field-check variable-rooted reads (`{{myVar.field}}`) against the
                // shape a `<q:variable schema=…>` or `<q:variable source="channel">`
                // binds, with the same closed→ERROR / open→WARN discipline.
                check_variable_fields(
                    &analysis,
                    process,
                    id,
                    deployment,
                    definitions,
                    &site,
                    dep_label,
                    out,
                );
                // A construct the analyzer could not tie to a concrete field (a dynamic /
                // computed lookup key) cannot be statically validated — a hard error, mirroring
                // the deploy-time type-safety contract (precise: ambiguity is reported).
                for construct in &analysis.unresolvable {
                    out.push(
                        LintDiagnostic::error(
                            codes::CONFIG_TEMPLATE_NOT_VALIDATABLE,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) uses a construct that \
                                 cannot be statically validated: {construct}. Declare it or refactor \
                                 to the validatable subset.",
                                process.id
                            ),
                        )
                        .at_node(&process.id, id),
                    );
                }
            });
        }
    }
}

/// The `SchemaShape` a `<q:variable schema="…">` binds. `@schema` is a codec reference (the
/// same form as `channels.yaml codec:` — a builtin `urn:sutra:codec:<name>` or a path-derived
/// `urn:<folder>` user codec). The variable's shape is that codec's shape for its single declared
/// message type. A codec that declares no type (opaque/open) or more than one (ambiguous, no way to
/// pick a root for a bare variable) yields `None` — its variable-rooted reads are Unverifiable
/// (WARN), never a false ERROR.
fn variable_shape(deployment: &LoadedDeployment, schema_ref: &str) -> Option<SchemaShape> {
    match declared_types(deployment, schema_ref).as_list() {
        [only] => resolve_shape(deployment, schema_ref, only),
        _ => None,
    }
}

/// Field-check the variable-rooted template reads (`{{myVar.field}}`) of every typed
/// `<q:variable>` against the shape it binds: a `@schema` codec reference, or — when `@schema`
/// is absent — the intake channel a `<q:variable source="channel">` feeds off (the channel's
/// bound codec). Applies the same closed→ERROR / open→WARN discipline the `payload` root already
/// gets ([`check_payload_fields`]). Reuses the template analyzer's per-root paths and the codec
/// shape machinery — no new subsystem.
#[allow(clippy::too_many_arguments)]
fn check_variable_fields(
    analysis: &sutra_templates::TemplateAnalysis,
    process: &ProcessDefinition,
    node: &str,
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    site: &str,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for var in &process.declared_variables {
        // `@schema` binds the shape; else a `<q:variable source="channel">` derives it from
        // that intake channel's codec. A plain scalar/untyped variable is nothing to check.
        let Some((shape_opt, ref_label)) = declared_variable_shape(deployment, definitions, var)
        else {
            continue;
        };
        let Some(paths) = analysis.root_paths.get(&var.name) else {
            continue; // this template reads no dotted path under the variable
        };
        let Some(shape) = shape_opt else {
            for path in paths {
                out.push(
                    LintDiagnostic::warning(
                        codes::CONFIG_TEMPLATE_FIELD_UNVERIFIABLE,
                        format!(
                            "deployment {dep_label}: process '{}' ({site}) reads {}.{path}, \
                             unverifiable: {ref_label} exposes no single message shape \
                             (opaque, open, or multi-root codec)",
                            process.id, var.name
                        ),
                    )
                    .at_node(&process.id, node),
                );
            }
            continue;
        };
        for path in paths {
            match shape.resolve(path) {
                PathResolution::DeclaredField(_) => {}
                PathResolution::UnknownInClosed { container, .. } => {
                    out.push(
                        LintDiagnostic::error(
                            codes::CONFIG_TEMPLATE_FIELD_UNKNOWN,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) reads {}.{path}, but \
                                 {ref_label} declares no such field under the closed container \
                                 '{container}'",
                                process.id, var.name
                            ),
                        )
                        .at_node(&process.id, node),
                    );
                }
                PathResolution::Unverifiable(detail) => {
                    out.push(
                        LintDiagnostic::warning(
                            codes::CONFIG_TEMPLATE_FIELD_UNVERIFIABLE,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) reads {}.{path}, \
                                 unverifiable: {detail}",
                                process.id, var.name
                            ),
                        )
                        .at_node(&process.id, node),
                    );
                }
            }
        }
    }
}

/// The [`SchemaShape`] a `<q:variable source="channel">` derives from that intake channel's
/// codec: the codec's shape for its single declared message type. An opaque / open / multi-root
/// codec (or an unbound channel) yields `None` ⇒ the reads are Unverifiable (WARN), never a false
/// ERROR — mirroring [`variable_shape`]'s `@schema` resolution but keyed off the channel's codec.
fn source_shape(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    channel: &str,
) -> Option<SchemaShape> {
    let def = definitions
        .iter()
        .find(|d| d.binding.channel_name == channel)?;
    let codec_name = def.binding.codec.clone();
    if codec_name.trim().is_empty() {
        return None;
    }
    match declared_types(deployment, &codec_name).as_list() {
        [only] => resolve_shape(deployment, &codec_name, only),
        _ => None,
    }
}

/// The [`SchemaShape`] a declared `<q:variable>` binds — its `@schema` codec reference or,
/// absent that, the codec of the `@source` intake channel it feeds off. Returns `None` when
/// the variable is a plain scalar / untyped declaration (nothing to field-check); otherwise
/// `Some((shape, ref_label))` where an inner `None` shape means the reference exposes no single
/// message shape (opaque / open / multi-root) ⇒ its field reads are Unverifiable (WARN), never a
/// false ERROR. Shared by the template field-check ([`check_variable_fields`]) and the navigation
/// field-check ([`check_nav_variable_path`]) so both apply an identical schema-resolution policy.
fn declared_variable_shape(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    var: &DeclaredVariable,
) -> Option<(Option<SchemaShape>, String)> {
    if let Some(schema_ref) = var.schema.as_deref() {
        return Some((
            variable_shape(deployment, schema_ref),
            format!("schema '{schema_ref}'"),
        ));
    }
    var.source.as_deref().map(|source| {
        (
            source_shape(deployment, definitions, source),
            format!("source channel '{source}'"),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn check_payload_fields(
    analysis: &sutra_templates::TemplateAnalysis,
    intake: Option<&Intake>,
    process: &ProcessDefinition,
    node: &str,
    site: &str,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    if analysis.payload_paths.is_empty() {
        return;
    }
    let Some(intake) = intake else {
        for path in &analysis.payload_paths {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_TEMPLATE_FIELD_TYPE_UNPINNED,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) reads payload.{path}, \
                         not field-checked: no typed intake could be resolved for this flow",
                        process.id
                    ),
                )
                .at_node(&process.id, node),
            );
        }
        return;
    };
    match &intake.fields {
        IntakeFields::Unpinned => {
            for path in &analysis.payload_paths {
                out.push(
                    LintDiagnostic::warning(
                        codes::CONFIG_TEMPLATE_FIELD_TYPE_UNPINNED,
                        format!(
                            "deployment {dep_label}: process '{}' ({site}) reads payload.{path}, \
                             not field-checked: the flow's <q:source> pins no exact \
                             messageTypeValue (a messageTypePattern / multi-type codec)",
                            process.id
                        ),
                    )
                    .at_node(&process.id, node),
                );
            }
        }
        // C5 — a messageTypePattern over an enumerable codec: intersect the matching types.
        IntakeFields::Candidates { types, shapes } => {
            check_fields_intersection(analysis, types, shapes, process, node, site, dep_label, out);
        }
        IntakeFields::Single {
            message_type,
            shape,
        } => {
            let Some(shape) = shape else {
                for path in &analysis.payload_paths {
                    out.push(
                        LintDiagnostic::warning(
                            codes::CONFIG_TEMPLATE_FIELD_UNVERIFIABLE,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) reads \
                                 payload.{path}, unverifiable: the codec '{}' exposes no schema",
                                process.id, intake.codec_label
                            ),
                        )
                        .at_node(&process.id, node),
                    );
                }
                return;
            };
            for path in &analysis.payload_paths {
                match shape.resolve(path) {
                    PathResolution::DeclaredField(_) => {}
                    PathResolution::UnknownInClosed { container, .. } => {
                        out.push(
                            LintDiagnostic::error(
                                codes::CONFIG_TEMPLATE_FIELD_UNKNOWN,
                                format!(
                                    "deployment {dep_label}: process '{}' ({site}) reads \
                                     payload.{path}, but the schema for message type \
                                     '{message_type}' declares no such field under the closed \
                                     container '{container}'",
                                    process.id
                                ),
                            )
                            .at_node(&process.id, node),
                        );
                    }
                    PathResolution::Unverifiable(detail) => {
                        out.push(
                            LintDiagnostic::warning(
                                codes::CONFIG_TEMPLATE_FIELD_UNVERIFIABLE,
                                format!(
                                    "deployment {dep_label}: process '{}' ({site}) reads \
                                     payload.{path}, unverifiable: {detail}",
                                    process.id
                                ),
                            )
                            .at_node(&process.id, node),
                        );
                    }
                }
            }
        }
    }
}

/// C5 — resolve each payload path against the shape of every type a `messageTypePattern`
/// matches, then combine: unknown-in-a-closed-container across ALL matching types is a
/// provable dead reference (ERROR); unknown in only some is partial/ambiguous (WARN);
/// unverifiable everywhere is a WARN; declared in every matching type is clean.
#[allow(clippy::too_many_arguments)]
fn check_fields_intersection(
    analysis: &sutra_templates::TemplateAnalysis,
    types: &[String],
    shapes: &[SchemaShape],
    process: &ProcessDefinition,
    node: &str,
    site: &str,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    for path in &analysis.payload_paths {
        let mut unknown = 0usize;
        let mut unverifiable = 0usize;
        for shape in shapes {
            match shape.resolve(path) {
                PathResolution::DeclaredField(_) => {}
                PathResolution::UnknownInClosed { .. } => unknown += 1,
                PathResolution::Unverifiable(_) => unverifiable += 1,
            }
        }
        if unknown == shapes.len() {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_TEMPLATE_FIELD_UNKNOWN,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) reads payload.{path}, but \
                         none of the messageTypePattern's matching types {types:?} declare it",
                        process.id
                    ),
                )
                .at_node(&process.id, node),
            );
        } else if unknown > 0 {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_TEMPLATE_FIELD_PARTIAL,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) reads payload.{path}, \
                         declared in only some of the matching types {types:?} (partial — \
                         ambiguous under the messageTypePattern)",
                        process.id
                    ),
                )
                .at_node(&process.id, node),
            );
        } else if unverifiable > 0 {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_TEMPLATE_FIELD_UNVERIFIABLE,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) reads payload.{path}, \
                         unverifiable across the matching types {types:?}",
                        process.id
                    ),
                )
                .at_node(&process.id, node),
            );
        }
    }
}

// ---- navigation ⇒ schema --------------------------------------------------------------------

/// A process's single typed intake for the navigation⇒schema check. `shape` is `None` for an
/// opaque codec; `unpinned_multi_type` flags a multi-type codec whose `<q:source>` pins no
/// type (its paths are unverifiable — a MESSAGE_TYPE_UNPINNED warning, never an error).
struct NavIntake {
    channel: String,
    payload_var: String,
    message_type: String,
    codec_label: String,
    shape: Option<SchemaShape>,
    unpinned_multi_type: bool,
    declared_count: usize,
}

/// For each process resolve the typed intake schema (start event → `<q:source>` channel →
/// codec → message-type shape) and check every FEEL data path it dereferences (`<q:alias>`
/// expressions, `<q:dispatch>` case `when`s, `<q:source idempotencyKey>`, `<q:simpleValidator
/// path>`, sequence-flow conditions). Posture is advise-don't-gatekeep: a path is an ERROR
/// only where provably wrong (absent from a closed container, or a numeric operator on a
/// declared non-numeric field/variable); everything else is a WARNING.
fn check_navigation_paths(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            let Some(intake) = resolve_nav_intake(deployment, process, definitions) else {
                continue; // no single coherent typed intake — nothing to anchor on
            };
            if intake.unpinned_multi_type {
                out.push(
                    LintDiagnostic::warning(
                        codes::CONFIG_BPMN_MESSAGE_TYPE_UNPINNED,
                        format!(
                            "deployment {dep_label}: process '{}' navigates its payload on \
                             channel '{}', but its codec declares {} message types and the \
                             <q:source> pins none — no concrete schema can be selected, so its \
                             paths are unverifiable",
                            process.id, intake.channel, intake.declared_count
                        ),
                    )
                    .at_process(&process.id),
                );
                continue;
            }
            visit_nodes(process, &mut |owner, node| {
                let bindings = owner.bindings_for(node.id());
                let anchor = DiagnosticAnchor::BpmnNode {
                    process: process.id.clone(),
                    node: node.id().to_string(),
                };
                for alias in &bindings.aliases {
                    check_nav_expression(
                        &alias.expression,
                        &format!("alias '{}'", alias.name),
                        process,
                        &anchor,
                        &intake,
                        deployment,
                        definitions,
                        dep_label,
                        out,
                    );
                }
                if let Some(dispatch) = &bindings.dispatch {
                    for case in &dispatch.cases {
                        check_nav_expression(
                            &case.when,
                            &format!("dispatch case → {}", case.called_element),
                            process,
                            &anchor,
                            &intake,
                            deployment,
                            definitions,
                            dep_label,
                            out,
                        );
                    }
                }
                for source in &bindings.sources {
                    if let Some(key) = &source.dedup_key {
                        check_nav_expression(
                            key,
                            "dedupKey",
                            process,
                            &anchor,
                            &intake,
                            deployment,
                            definitions,
                            dep_label,
                            out,
                        );
                    }
                    for sv in &source.simple_validators {
                        check_nav_expression(
                            &sv.path,
                            &format!("simpleValidator '{}'", sv.reference),
                            process,
                            &anchor,
                            &intake,
                            deployment,
                            definitions,
                            dep_label,
                            out,
                        );
                    }
                }
                // `<q:param name= expression=/>` reads on a serviceTask (B2) — field-check the
                // param FEEL's payload-/variable-rooted `x.field` reads via the shared extractor
                // (bare-scalar guard checks do not apply to a value-binding expression).
                if let Node::ServiceTask { params, .. } = node {
                    for param in params {
                        check_nav_param_expression(
                            &param.expression,
                            &format!("param '{}'", param.name),
                            process,
                            &anchor,
                            &intake,
                            deployment,
                            definitions,
                            dep_label,
                            out,
                        );
                    }
                }
            });
            // Sequence-flow conditions — the canonical navigation site (e.g. payload.amount >
            // 1000 on a gateway branch), where a numeric operator on a declared string surfaces.
            for flow in process.flows() {
                if let Some(condition) = &flow.condition {
                    let anchor = DiagnosticAnchor::BpmnNode {
                        process: process.id.clone(),
                        node: flow.id.clone(),
                    };
                    check_nav_expression(
                        condition,
                        &format!("flow '{}' condition", flow.id),
                        process,
                        &anchor,
                        &intake,
                        deployment,
                        definitions,
                        dep_label,
                        out,
                    );
                }
            }
        }
    }
}

/// Extract the FEEL paths of one expression and resolve each payload/variable-rooted path.
#[allow(clippy::too_many_arguments)]
fn check_nav_expression(
    expression: &str,
    site: &str,
    process: &ProcessDefinition,
    anchor: &DiagnosticAnchor,
    intake: &NavIntake,
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let Ok(paths) = sutra_feel::expressions::paths(expression) else {
        return; // a FEEL parse error is a different lint's concern (check_feel_sites)
    };
    for path in &paths {
        if path.root() == intake.payload_var {
            check_nav_payload_path(path, site, process, anchor, intake, dep_label, out);
        } else if declared_variable_type(process, path.root()).is_some() {
            check_nav_variable_path(
                path,
                process,
                anchor,
                deployment,
                definitions,
                site,
                dep_label,
                out,
            );
        }
        // else: header- or undeclared-variable root — not the typed payload, skip
    }
}

/// A payload-rooted path: strip the payload variable root and resolve the remainder against
/// the intake shape (or WARN unverifiable when the codec exposes none).
#[allow(clippy::too_many_arguments)]
fn check_nav_payload_path(
    path: &sutra_feel::paths::PathRef,
    site: &str,
    process: &ProcessDefinition,
    anchor: &DiagnosticAnchor,
    intake: &NavIntake,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    if path.segments.len() == 1 {
        return; // a bare payload reference — whole-message, nothing to resolve
    }
    let relative = path.segments[1..].join(".");
    let Some(shape) = &intake.shape else {
        out.push(
            LintDiagnostic::warning(
                codes::CONFIG_BPMN_PATH_UNVERIFIABLE,
                nav_unverifiable_msg(
                    process,
                    site,
                    path,
                    &format!(
                        "the channel codec '{}' exposes no schema",
                        intake.codec_label
                    ),
                ),
            )
            .at_anchor(anchor.clone()),
        );
        return;
    };
    match shape.resolve(&relative) {
        PathResolution::DeclaredField(field_type) => {
            if path.usage == Usage::Numeric && !field_type.is_numeric_compatible() {
                out.push(
                    LintDiagnostic::error(
                        codes::CONFIG_BPMN_PATH_TYPE_MISMATCH,
                        format!(
                            "deployment {dep_label}: process '{}' ({site}) uses {} in a numeric \
                             operator, but the intake schema for message type '{}' declares it \
                             as {field_type:?}",
                            process.id,
                            path.dotted(),
                            intake.message_type
                        ),
                    )
                    .at_anchor(anchor.clone()),
                );
            }
        }
        PathResolution::UnknownInClosed { container, .. } => {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_BPMN_PATH_UNKNOWN_FIELD,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) navigates {}, but the \
                         intake schema for message type '{}' declares no such field under the \
                         closed container '{container}'",
                        process.id,
                        path.dotted(),
                        intake.message_type
                    ),
                )
                .at_anchor(anchor.clone()),
            );
        }
        PathResolution::Unverifiable(detail) => {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_BPMN_PATH_UNVERIFIABLE,
                    nav_unverifiable_msg(process, site, path, &detail),
                )
                .at_anchor(anchor.clone()),
            );
        }
    }
}

/// A path rooted at a `<q:variables>`-declared variable. A bare scalar variable used in a
/// numeric operator but not declared number is a PATH_TYPE_MISMATCH (the bare-scalar check). A
/// `var.field` descent is field-checked against the variable's bound shape — its `@schema` codec, or the
/// codec of the `@source` channel it feeds off: a field absent from a closed container is
/// PATH_UNKNOWN_FIELD (ERROR), a numeric operator on a declared non-numeric field is a
/// PATH_TYPE_MISMATCH (ERROR), and an open / opaque shape is PATH_UNVERIFIABLE (WARN) — the same
/// closed→ERROR / open→WARN discipline the `payload` root gets ([`check_nav_payload_path`]).
#[allow(clippy::too_many_arguments)]
fn check_nav_variable_path(
    path: &sutra_feel::paths::PathRef,
    process: &ProcessDefinition,
    anchor: &DiagnosticAnchor,
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    site: &str,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let Some(var) = process
        .declared_variables
        .iter()
        .find(|d| d.name == path.root())
    else {
        return; // undeclared root — not checkable (the caller already gates on this)
    };
    if path.segments.len() == 1 {
        // A bare variable reference — the scalar numeric-type check on its declared `@type`.
        if path.usage == Usage::Numeric && !field_type_numeric_compatible(var.ty) {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_BPMN_PATH_TYPE_MISMATCH,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) uses variable '{}' in a \
                         numeric operator, but <q:variables> declares it as {:?}",
                        process.id,
                        path.dotted(),
                        var.ty
                    ),
                )
                .at_anchor(anchor.clone()),
            );
        }
        return;
    }
    check_nav_variable_field(
        path,
        var,
        process,
        anchor,
        deployment,
        definitions,
        site,
        dep_label,
        out,
    );
}

/// The `var.field…` field-descent: resolve the sub-path (everything after the variable
/// root) against the variable's bound schema shape — its `@schema` codec, or the codec of the
/// `@source` channel it feeds off. A field absent from a closed container is
/// PATH_UNKNOWN_FIELD (ERROR); a numeric operator on a declared non-numeric field is a
/// PATH_TYPE_MISMATCH (ERROR); an open / opaque shape is PATH_UNVERIFIABLE (WARN) — the same
/// closed→ERROR / open→WARN discipline the `payload` root gets ([`check_nav_payload_path`]). The
/// caller guarantees `path.segments.len() > 1`.
#[allow(clippy::too_many_arguments)]
fn check_nav_variable_field(
    path: &sutra_feel::paths::PathRef,
    var: &DeclaredVariable,
    process: &ProcessDefinition,
    anchor: &DiagnosticAnchor,
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    site: &str,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let Some((shape_opt, ref_label)) = declared_variable_shape(deployment, definitions, var) else {
        return; // a plain scalar/untyped variable exposes no shape to descend into
    };
    let relative = path.segments[1..].join(".");
    let Some(shape) = shape_opt else {
        out.push(
            LintDiagnostic::warning(
                codes::CONFIG_BPMN_PATH_UNVERIFIABLE,
                nav_unverifiable_msg(
                    process,
                    site,
                    path,
                    &format!("{ref_label} exposes no single message shape (opaque, open, or multi-root codec)"),
                ),
            )
            .at_anchor(anchor.clone()),
        );
        return;
    };
    match shape.resolve(&relative) {
        PathResolution::DeclaredField(field_type) => {
            if path.usage == Usage::Numeric && !field_type.is_numeric_compatible() {
                out.push(
                    LintDiagnostic::error(
                        codes::CONFIG_BPMN_PATH_TYPE_MISMATCH,
                        format!(
                            "deployment {dep_label}: process '{}' ({site}) uses {} in a numeric \
                             operator, but {ref_label} declares it as {field_type:?}",
                            process.id,
                            path.dotted()
                        ),
                    )
                    .at_anchor(anchor.clone()),
                );
            }
        }
        PathResolution::UnknownInClosed { container, .. } => {
            out.push(
                LintDiagnostic::error(
                    codes::CONFIG_BPMN_PATH_UNKNOWN_FIELD,
                    format!(
                        "deployment {dep_label}: process '{}' ({site}) navigates {}, but {ref_label} \
                         declares no such field under the closed container '{container}'",
                        process.id,
                        path.dotted()
                    ),
                )
                .at_anchor(anchor.clone()),
            );
        }
        PathResolution::Unverifiable(detail) => {
            out.push(
                LintDiagnostic::warning(
                    codes::CONFIG_BPMN_PATH_UNVERIFIABLE,
                    nav_unverifiable_msg(process, site, path, &detail),
                )
                .at_anchor(anchor.clone()),
            );
        }
    }
}

/// Field-check a `<q:param name= expression=/>` FEEL read on a serviceTask (B2). A param binds a
/// value expression, not a navigation guard, so it gets only the field-descent existence/type
/// resolution — `payload.field` against the intake shape and `var.field` against the variable's
/// bound shape — and NOT the *bare-scalar* numeric-type check the guard sites apply: binding
/// a whole scalar variable into a value expression (e.g. `riskBand + "-band"`) is legitimate, so a
/// bare root is skipped. Shares the extraction + shape helpers with [`check_nav_expression`].
#[allow(clippy::too_many_arguments)]
fn check_nav_param_expression(
    expression: &str,
    site: &str,
    process: &ProcessDefinition,
    anchor: &DiagnosticAnchor,
    intake: &NavIntake,
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let Ok(paths) = sutra_feel::expressions::paths(expression) else {
        return; // a FEEL parse error is a different lint's concern (check_feel_sites)
    };
    for path in &paths {
        if path.segments.len() == 1 {
            continue; // a bare root (whole-value read) — nothing to field-check on a param
        }
        if path.root() == intake.payload_var {
            check_nav_payload_path(path, site, process, anchor, intake, dep_label, out);
        } else if let Some(var) = process
            .declared_variables
            .iter()
            .find(|d| d.name == path.root())
        {
            check_nav_variable_field(
                path,
                var,
                process,
                anchor,
                deployment,
                definitions,
                site,
                dep_label,
                out,
            );
        }
        // else: header- or undeclared-variable root — not the typed payload, skip
    }
}

fn nav_unverifiable_msg(
    process: &ProcessDefinition,
    site: &str,
    path: &sutra_feel::paths::PathRef,
    detail: &str,
) -> String {
    format!(
        "process '{}' ({site}) navigates {}, which cannot be verified: {detail}. The path \
         may be valid — it is simply not provable",
        process.id,
        path.dotted()
    )
}

/// Whether a `<q:variables>`-declared scalar type may stand in a numeric FEEL position.
fn field_type_numeric_compatible(field_type: FieldType) -> bool {
    matches!(field_type, FieldType::Number | FieldType::Any)
}

fn declared_variable_type(process: &ProcessDefinition, root: &str) -> Option<FieldType> {
    process
        .declared_variables
        .iter()
        .find(|d| d.name == root)
        .map(|d| d.ty)
}

/// Resolve a process's single typed intake for navigation checks. Returns `None` when there
/// is not exactly one coherent source-bound start-event intake (a differing second intake is
/// ambiguous to anchor node paths on) or the channel is unregistered.
fn resolve_nav_intake(
    deployment: &LoadedDeployment,
    process: &ProcessDefinition,
    definitions: &[ChannelDefinition],
) -> Option<NavIntake> {
    let mut source: Option<&SourceBinding> = None;
    for node in process.start_events() {
        if let Some(s) = process.bindings_for(node.id()).source() {
            if let Some(prev) = source {
                if !same_nav_intake(prev, s) {
                    return None; // more than one distinct intake — ambiguous anchor
                }
            }
            source = Some(s);
        }
    }
    let source = source?;
    let payload_var = source.name.clone();
    let channel = source.channel.clone();
    let def = definitions
        .iter()
        .find(|d| d.binding.channel_name == source.channel)?; // unregistered → skip
    let codec_name = def.binding.codec.clone();
    let codec_label = if codec_name.trim().is_empty() {
        "none".to_string()
    } else {
        codec_name.clone()
    };
    if codec_name.trim().is_empty() {
        return Some(NavIntake {
            channel,
            payload_var,
            message_type: String::new(),
            codec_label,
            shape: None,
            unpinned_multi_type: false,
            declared_count: 0,
        });
    }
    let declared = declared_types(deployment, &codec_name);
    let declared_list = declared.as_list();
    let mut pinned = source.message_type_value.clone();
    if pinned.is_none() {
        match declared_list {
            [only] => pinned = Some(only.clone()),
            [] => {} // open codec, no pin — shape resolves to none (opaque)
            _ => {
                return Some(NavIntake {
                    channel,
                    payload_var,
                    message_type: String::new(),
                    codec_label,
                    shape: None,
                    unpinned_multi_type: true,
                    declared_count: declared_list.len(),
                });
            }
        }
    }
    let message_type = pinned.unwrap_or_default();
    let shape = resolve_shape(deployment, &codec_name, &message_type);
    Some(NavIntake {
        channel,
        payload_var,
        message_type,
        codec_label,
        shape,
        unpinned_multi_type: false,
        declared_count: declared_list.len(),
    })
}

/// Two start events share an intake only if same channel, payload variable, and message-type
/// pin (the same-intake rule).
fn same_nav_intake(a: &SourceBinding, b: &SourceBinding) -> bool {
    a.channel == b.channel && a.name == b.name && a.message_type_value == b.message_type_value
}

// ---- output conformance (template-manifest.yaml) ------------------------------------------

fn check_output_conformance(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    manifests: &ValidationManifests,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    if manifests.template_outputs.is_empty() {
        return;
    }
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            // The reply codec = the single-intake channel's codec (a <q:reply> rides the
            // inbound channel).
            let intake = resolve_intake(deployment, process, definitions);
            let reply_codec = intake.as_ref().and_then(|i| {
                if i.codec_label == "none" {
                    None
                } else {
                    Some(i.codec_label.clone())
                }
            });
            visit_nodes(process, &mut |_, node| {
                let Node::ServiceTask {
                    id, implementation, ..
                } = node
                else {
                    return;
                };
                let Some(output_type) = manifests.template_outputs.get(implementation.trim())
                else {
                    return;
                };
                let site = format!("serviceTask '{id}' (template {implementation})");
                let Some(codec_name) = &reply_codec else {
                    out.push(
                        LintDiagnostic::warning(
                            codes::CONFIG_TEMPLATE_OUTPUT_UNVERIFIABLE,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) declares \
                                 outputMessageType '{output_type}', not verifiable: no single \
                                 reply codec resolves for this flow",
                                process.id
                            ),
                        )
                        .at_node(&process.id, id),
                    );
                    return;
                };
                let producible = declared_types(deployment, codec_name);
                let producible_list = producible.as_list();
                if producible_list.is_empty() {
                    out.push(
                        LintDiagnostic::warning(
                            codes::CONFIG_TEMPLATE_OUTPUT_UNVERIFIABLE,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) declares \
                                 outputMessageType '{output_type}', not verifiable: the reply \
                                 codec '{codec_name}' has an open type set",
                                process.id
                            ),
                        )
                        .at_node(&process.id, id),
                    );
                } else if !producible_list.iter().any(|t| t == output_type) {
                    out.push(
                        LintDiagnostic::error(
                            codes::CONFIG_TEMPLATE_OUTPUT_TYPE_UNKNOWN,
                            format!(
                                "deployment {dep_label}: process '{}' ({site}) declares \
                                 outputMessageType '{output_type}', but its reply codec \
                                 '{codec_name}' can only produce {producible_list:?} — a dead \
                                 output binding",
                                process.id
                            ),
                        )
                        .at_node(&process.id, id),
                    );
                }
            });
        }
    }
}

// ---- rules applicability (rules-manifest.yaml) --------------------------------------------

fn check_rules_applicability(
    deployment: &LoadedDeployment,
    definitions: &[ChannelDefinition],
    manifests: &ValidationManifests,
    dep_label: &str,
    out: &mut Vec<LintDiagnostic>,
) {
    let mut seen = BTreeSet::new();
    for module in deployment.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            visit_nodes(process, &mut |owner, node| {
                for source in &owner.bindings_for(node.id()).sources {
                    if source.complex_validators.is_empty() {
                        continue;
                    }
                    let message_type = resolve_source_message_type(deployment, source, definitions);
                    for rule_file in &source.complex_validators {
                        let site = format!(
                            "process '{}' <q:source channel='{}'> complexValidator \
                             '{rule_file}'",
                            process.id, source.channel
                        );
                        let Some(applicability) =
                            manifests.rule_applicability.get(rule_file.trim())
                        else {
                            out.push(
                                LintDiagnostic::warning(
                                    codes::CONFIG_RULES_MESSAGE_TYPE_UNDECLARED,
                                    format!(
                                        "deployment {dep_label}: {site} declares no \
                                         message-type applicability in rules-manifest.yaml — \
                                         its applicability cannot be checked"
                                    ),
                                )
                                .at_node(&process.id, node.id()),
                            );
                            continue;
                        };
                        let Some(message_type) = &message_type else {
                            continue; // node type unresolvable — nothing concrete to check
                        };
                        let applies = applicability.iter().any(|declared| {
                            declared == "*"
                                || declared == message_type
                                || full_match(message_type, declared)
                        });
                        if !applies {
                            out.push(
                                LintDiagnostic::error(
                                    codes::CONFIG_BPMN_RULE_MESSAGE_TYPE_MISMATCH,
                                    format!(
                                        "deployment {dep_label}: {site} reasons over \
                                         {applicability:?}, but the node's intake message type \
                                         is '{message_type}' — a rule may only be attached to \
                                         a node whose intake it supports"
                                    ),
                                )
                                .at_node(&process.id, node.id()),
                            );
                        }
                    }
                }
            });
        }
    }
}

/// The concrete message type a source carries, or `None` when it cannot be pinned to
/// exactly one type (pattern family, unknown channel, schema-less codec, or a multi-type
/// codec with no pin).
fn resolve_source_message_type(
    deployment: &LoadedDeployment,
    source: &SourceBinding,
    definitions: &[ChannelDefinition],
) -> Option<String> {
    if let Some(value) = &source.message_type_value {
        return Some(value.clone());
    }
    if source.message_type_pattern.is_some() {
        return None;
    }
    let def = definitions
        .iter()
        .find(|d| d.binding.channel_name == source.channel)?;
    match declared_types(deployment, &def.binding.codec).as_list() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

// ---- shared node walking -------------------------------------------------------------------

/// Visit every node of a process recursively, descending into embedded sub-process /
/// transaction / ad-hoc / event-sub-process definitions and loop wrappers; the callback
/// receives the DEFINITION owning the node (whose `bindings_for` resolves it).
fn visit_nodes(process: &ProcessDefinition, visit: &mut impl FnMut(&ProcessDefinition, &Node)) {
    fn visit_in(
        owner: &ProcessDefinition,
        node: &Node,
        visit: &mut impl FnMut(&ProcessDefinition, &Node),
    ) {
        visit(owner, node);
        match node {
            Node::SubProcess { inner, .. }
            | Node::TransactionSubProcess { inner, .. }
            | Node::EventSubProcess { inner, .. } => {
                for n in inner.nodes() {
                    visit_in(inner, n, visit);
                }
            }
            Node::AdHocSubProcess { inner, .. } => {
                for n in inner.nodes() {
                    visit_in(inner, n, visit);
                }
            }
            Node::MultiInstance { inner, .. } | Node::StandardLoop { inner, .. } => {
                visit_in(owner, inner, visit);
            }
            _ => {}
        }
    }
    for node in process.nodes() {
        visit_in(process, node, visit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absorb_dir_reads_co_located_manifests_recursively_with_folder_relative_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("rules/finance")).unwrap();
        // A root manifest (prefix "") and a nested one (prefix "finance") — folder-relative refs.
        std::fs::write(
            root.join("rules/rules-manifest.yaml"),
            "rules:\n  - file: top.dmn\n    messageTypes: [a.1]\n",
        )
        .unwrap();
        std::fs::write(
            root.join("rules/finance/rules-manifest.yaml"),
            "rules:\n  - file: forex.dmn\n    messageTypes: [fx.1]\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("templates/mt")).unwrap();
        std::fs::write(
            root.join("templates/mt/template-manifest.yaml"),
            "templates:\n  - file: t.hbs\n    outputMessageType: out.1\n",
        )
        .unwrap();

        let mut m = ValidationManifests::default();
        let mut out = Vec::new();
        m.absorb_dir(root, &mut out);

        assert!(out.is_empty(), "no diagnostics expected: {out:?}");
        // Rebased to rules/-relative ids: root → "top.dmn"; nested → "finance/forex.dmn".
        assert_eq!(
            m.rule_applicability.get("top.dmn"),
            Some(&vec!["a.1".to_string()])
        );
        assert_eq!(
            m.rule_applicability.get("finance/forex.dmn"),
            Some(&vec!["fx.1".to_string()])
        );
        // Template nested manifest → templates/-relative id "mt/t.hbs".
        assert_eq!(
            m.template_outputs.get("mt/t.hbs"),
            Some(&"out.1".to_string())
        );
    }

    #[test]
    fn migration_version_parsing() {
        assert_eq!(parse_migration_version("V001__accounts.sql"), Some(1));
        assert_eq!(parse_migration_version("V803__timers.sql"), Some(803));
        assert_eq!(parse_migration_version("001__x.sql"), None);
        assert_eq!(parse_migration_version("V__x.sql"), None);
        assert_eq!(parse_migration_version("V1__.sql"), None);
        assert_eq!(parse_migration_version("V1__x.txt"), None);
    }

    #[test]
    fn full_match_is_anchored_and_tolerant() {
        assert!(full_match("order.created.001.14", r"order\.created\..*"));
        assert!(!full_match("order.created.001.14", r"order\.cancelled"));
        assert!(!full_match("anything", r"([")); // malformed pattern matches nothing
    }

    // ===== R14 literal-credential lint (SUTRA.DEPLOY.CREDENTIALS.LITERAL) ======================

    fn credential_findings(diags: &[LintDiagnostic]) -> Vec<&LintDiagnostic> {
        diags
            .iter()
            .filter(|d| d.code == codes::DEPLOY_CREDENTIALS_LITERAL)
            .collect()
    }

    fn stores_of(yaml: &str) -> Vec<StoreDefinition> {
        parse_datastores(yaml).expect("datastores.yaml parses")
    }

    fn channels_of(yaml: &str) -> Vec<ChannelDefinition> {
        load_channel_definitions(yaml.as_bytes(), "t", "m", "1.0.0", "channels.yaml")
            .expect("channels.yaml parses")
    }

    fn run_credential_check(
        stores: &[StoreDefinition],
        channels: &[ChannelDefinition],
    ) -> Vec<LintDiagnostic> {
        let mut out = Vec::new();
        check_literal_credentials(stores, channels, "t/m/1.0.0 (id)", &mut out);
        out
    }

    #[test]
    fn is_credential_ref_recognises_the_three_schemes() {
        assert!(is_credential_ref("env:DB_PASSWORD"));
        assert!(is_credential_ref("secret:DB_PASSWORD"));
        assert!(is_credential_ref("${RABBITMQ_PASSWORD}"));
        assert!(is_credential_ref("  ${X}  "));
        assert!(!is_credential_ref("hunter2"));
        assert!(!is_credential_ref("postgres"));
    }

    #[test]
    fn url_literal_password_detection_covers_userinfo_and_query_forms() {
        // user-info embedded literal password.
        assert!(url_has_literal_password(
            "rabbitmq://svc:hunter2@rabbitmq:5672/q"
        ));
        assert!(url_has_literal_password(
            "postgres://acme:s3cr3t@db:5432/app"
        ));
        // mssql connection-string parameter literal password.
        assert!(url_has_literal_password(
            "sqlserver://db;user=sa;password=hunter2;encrypt=false"
        ));
        // Placeholder / ref user-info is clean (the example broker binds).
        assert!(!url_has_literal_password(
            "rabbitmq://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD}@rabbitmq:5672/mx.out.q"
        ));
        assert!(!url_has_literal_password(
            "sqlserver://db;password=${DB_PW}"
        ));
        // No user-info / no password component is clean.
        assert!(!url_has_literal_password("http://${MX_DEST_HOST}/mx-in"));
        assert!(!url_has_literal_password("postgres://db:5432/app"));
        assert!(!url_has_literal_password("postgres://user@db:5432/app"));
    }

    #[test]
    fn detection_keys_on_the_segment_so_any_key_family_flags() {
        // Two different key prefixes over the same last segment flag identically — the store
        // `type` here is `file` so parse_datastores accepts an arbitrary property bag.
        let broker = stores_of(
            "datastores:\n  - name: a\n    type: file\n    broker:\n      password: hunter2\n",
        );
        let sql = stores_of(
            "datastores:\n  - name: a\n    type: file\n    sql:\n      password: hunter2\n",
        );
        assert_eq!(
            credential_findings(&run_credential_check(&broker, &[])).len(),
            1
        );
        assert_eq!(
            credential_findings(&run_credential_check(&sql, &[])).len(),
            1
        );
    }

    #[test]
    fn ref_based_datastore_connection_is_clean() {
        // The money-transfer shape — every connection field is an env: ref.
        let stores = stores_of(
            "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      \
             url-ref: env:ACCOUNTS_DB_URL\n      username-ref: env:ACCOUNTS_DB_USER\n      \
             password-ref: env:ACCOUNTS_DB_PASSWORD\n      migrations: migrations/accounts\n    \
             dataClass: financial\n",
        );
        assert!(credential_findings(&run_credential_check(&stores, &[])).is_empty());
    }

    #[test]
    fn literal_datastore_url_username_and_password_each_flag() {
        let url = stores_of(
            "datastores:\n  - name: a\n    type: sql\n    sql:\n      \
             url: postgres://acme:s3cr3t@db:5432/app\n",
        );
        let user =
            stores_of("datastores:\n  - name: a\n    type: sql\n    sql:\n      username: acme\n");
        let pass = stores_of(
            "datastores:\n  - name: a\n    type: sql\n    sql:\n      password: hunter2\n",
        );
        // A `-ref` key that carries a LITERAL (misuse) is a literal credential too.
        let ref_misused = stores_of(
            "datastores:\n  - name: a\n    type: sql\n    sql:\n      password-ref: hunter2\n",
        );
        assert_eq!(
            credential_findings(&run_credential_check(&url, &[])).len(),
            1
        );
        assert_eq!(
            credential_findings(&run_credential_check(&user, &[])).len(),
            1
        );
        assert_eq!(
            credential_findings(&run_credential_check(&pass, &[])).len(),
            1
        );
        assert_eq!(
            credential_findings(&run_credential_check(&ref_misused, &[])).len(),
            1
        );
    }

    fn local_target_findings(defs: &[ChannelDefinition]) -> Vec<LintDiagnostic> {
        let deployment = deployment_with(&[], &[], &[]);
        let mut out = Vec::new();
        check_channel_declarations(&deployment, defs, "t/m/1.0.0 (id)", &mut out);
        out.into_iter()
            .filter(|d| d.code == codes::CONFIG_CHANNEL_LOCAL_TARGET_UNKNOWN)
            .collect()
    }

    #[test]
    fn local_outbound_bind_requires_a_declared_local_inbound_target() {
        // Positive: `to-orders` binds `local://orders-in`, and `orders-in` IS a real
        // `transport: local` inbound channel of this deployment — no finding.
        let ok = channels_of(
            "channels:\n  - name: orders-in\n    transport: local\n  - name: to-orders\n    \
             direction: outbound\n    transport: local\n    bind: local://orders-in\n",
        );
        assert!(local_target_findings(&ok).is_empty());

        // Negative: the target channel does not exist at all.
        let unknown = channels_of(
            "channels:\n  - name: to-nowhere\n    direction: outbound\n    transport: local\n    \
             bind: local://nope\n",
        );
        assert_eq!(local_target_findings(&unknown).len(), 1);

        // Negative: the target exists but is an HTTP inbound (not `transport: local`).
        let wrong_transport = channels_of(
            "channels:\n  - name: sender-in\n    transport: http\n    auth-scheme: apikey\n  \
             - name: to-sender\n    direction: outbound\n    transport: local\n    \
             bind: local://sender-in\n",
        );
        assert_eq!(local_target_findings(&wrong_transport).len(), 1);
    }

    #[test]
    fn pull_outbound_bind_requires_a_declared_local_inbound_target() {
        // A `pull://` bind names the channel the WORKER'S COMPLETION lands on, so it gets the
        // same target check `local://` gets — a dangling target would poison every completion
        // rather than every send, which is the worse failure of the two.
        let ok = channels_of(
            "channels:\n  - name: score-in\n    transport: local\n  - name: to-score\n    \
             direction: outbound\n    transport: pull\n    bind: pull://score-in\n",
        );
        assert!(local_target_findings(&ok).is_empty());

        let unknown = channels_of(
            "channels:\n  - name: to-nowhere\n    direction: outbound\n    transport: pull\n    \
             bind: pull://nope\n",
        );
        assert_eq!(local_target_findings(&unknown).len(), 1);
    }

    #[test]
    fn a_pull_send_target_resolves_to_the_channel_its_completion_lands_on() {
        // Correlation analysis must follow the pull indirection the same way it follows
        // `local://`: a `<q:send channel="to-score">` ultimately reaches `score-in`.
        let defs = channels_of(
            "channels:\n  - name: score-in\n    transport: local\n  - name: to-score\n    \
             direction: outbound\n    transport: pull\n    bind: pull://score-in\n",
        );
        assert_eq!(resolve_local_send_target(&defs, "to-score"), "score-in");
        // A channel with no outbound pull indirection resolves to itself.
        assert_eq!(resolve_local_send_target(&defs, "score-in"), "score-in");
    }

    #[test]
    fn ref_based_broker_channels_are_clean_but_literals_flag() {
        // The broker-backed request/response shape — ${ENV} placeholders on username/password
        // + an outbound bind whose user-info is placeholder-based. All clean.
        let clean = channels_of(
            "channels:\n  - name: orders-request\n    transport: rabbitmq\n    queue: q\n    \
             host: rabbitmq\n    port: 5672\n    username: ${RABBITMQ_USERNAME}\n    \
             password: ${RABBITMQ_PASSWORD}\n  - name: orders-response\n    direction: outbound\n    \
             transport: rabbitmq\n    bind: \"rabbitmq://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD}@rabbitmq:5672/out\"\n",
        );
        assert!(credential_findings(&run_credential_check(&[], &clean)).is_empty());

        // A literal broker password + a literal outbound bind user-info each flag.
        let literal = channels_of(
            "channels:\n  - name: in\n    transport: rabbitmq\n    queue: q\n    host: h\n    \
             port: 5672\n    username: svc\n    password: hunter2\n  - name: out\n    \
             direction: outbound\n    transport: rabbitmq\n    bind: \"rabbitmq://svc:hunter2@h:5672/out\"\n",
        );
        let findings = run_credential_check(&[], &literal);
        // username(literal) + password(literal) + bind(embedded literal password) = 3.
        assert_eq!(credential_findings(&findings).len(), 3, "{findings:?}");
    }

    // ===== reserved `sutra` first-level folder + reserved built-in codec name =============

    /// A minimal deployment carrying just the artifact subpaths / codec names under test.
    fn deployment_with(rules: &[&str], schemas: &[&str], codecs: &[&str]) -> LoadedDeployment {
        use crate::scanner::LoadedArtifact;
        use std::path::PathBuf;
        use sutra_executor::deployment::DeploymentId;
        let art = |sub: &str| LoadedArtifact {
            path: PathBuf::from(sub),
            content: String::new(),
        };
        LoadedDeployment {
            id: DeploymentId::of("dep-0000000000000000000000d1").expect("valid deployment id"),
            tenant: "t".to_string(),
            module: "m".to_string(),
            version: "1.0.0".to_string(),
            namespace: "urn:sutra:module:m:1.0.0".to_string(),
            processes: BTreeMap::new(),
            process_files: BTreeMap::new(),
            rules: rules.iter().map(|s| (s.to_string(), art(s))).collect(),
            templates: BTreeMap::new(),
            scripts: BTreeMap::new(),
            redactors: BTreeMap::new(),
            codecs: codecs
                .iter()
                .map(|s| (s.to_string(), vec![art(&format!("{s}/x.xsd"))]))
                .collect(),
            schema_files: schemas.iter().map(|s| (s.to_string(), art(s))).collect(),
            migrations: BTreeMap::new(),
            coverage_files: BTreeMap::new(),
            coverages: Vec::new(),
            channels_yaml: None,
            datastores_yaml: None,
            binding_dir: PathBuf::new(),
        }
    }

    #[test]
    fn reserved_first_level_sutra_folder_is_rejected_but_deeper_is_allowed() {
        let flagged = |dep: &LoadedDeployment| {
            let mut out = Vec::new();
            check_reserved_first_level_folders(dep, "t/m/1.0.0 (id)", &mut out);
            out.iter()
                .any(|d| d.code == codes::CONFIG_RESERVED_FIRST_LEVEL_FOLDER)
        };
        // First-level `sutra` under an artifact folder → rejected.
        assert!(flagged(&deployment_with(&["sutra/rule.dmn"], &[], &[])));
        // Case-insensitive, and under schemas/ too.
        assert!(flagged(&deployment_with(&[], &["Sutra/x.xsd"], &[])));
        // A DEEPER `sutra` segment (schemas/hr/sutra/…, rules/hr/sutra/…) → allowed.
        assert!(!flagged(&deployment_with(
            &["hr/sutra/rule.dmn"],
            &["hr/sutra/x.xsd"],
            &[]
        )));
        // A top-level file literally named `sutra.dmn` is not a folder → allowed.
        assert!(!flagged(&deployment_with(&["sutra.dmn"], &[], &[])));
    }

    /// A minimal deployment carrying a single `redactors/<subpath>` entry — mirrors
    /// `deployment_with` but for the redactor folder (kept separate so the existing
    /// `deployment_with` call sites need no 4th argument).
    fn deployment_with_redactor(subpath: &str, hbs_source: &str) -> LoadedDeployment {
        let mut dep = deployment_with(&[], &[], &[]);
        dep.redactors.insert(
            subpath.to_string(),
            crate::scanner::LoadedArtifact {
                path: std::path::PathBuf::from(format!("redactors/{subpath}")),
                content: hbs_source.to_string(),
            },
        );
        dep
    }

    #[test]
    fn reserved_first_level_sutra_folder_under_redactors_is_rejected() {
        let dep = deployment_with_redactor("sutra/x.hbs", "/card\n");
        let mut out = Vec::new();
        check_reserved_first_level_folders(&dep, "t/m/1.0.0 (id)", &mut out);
        assert!(out
            .iter()
            .any(|d| d.code == codes::CONFIG_RESERVED_FIRST_LEVEL_FOLDER));
    }

    #[test]
    fn a_broken_redactor_template_fails_the_compile_check() {
        let dep = deployment_with_redactor("broken.hbs", "<A>{{#if x}}unterminated");
        let mut out = Vec::new();
        check_templates_compile(&dep, "t/m/1.0.0 (id)", &mut out);
        assert!(
            out.iter()
                .any(|d| d.message.contains("redactors/broken.hbs")),
            "{out:?}"
        );
    }

    #[test]
    fn user_codec_shadowing_a_builtin_name_is_rejected() {
        let flagged = |dep: &LoadedDeployment| {
            let mut out = Vec::new();
            check_reserved_codec_names(dep, "t/m/1.0.0 (id)", &mut out);
            out.iter()
                .any(|d| d.code == codes::CONFIG_CODEC_RESERVED_NAME)
        };
        // A user codec `schemas/xml/` shadows a built-in name → rejected. A NEUTRAL format
        // built-in is used here: this test lives in the domain-neutral loader crate, whose test
        // binary does not (and must not) link any business codec crate. The reserved-name check
        // derives the builtin set from `builtin_codecs()`, so any linked builtin proves the rule.
        assert!(flagged(&deployment_with(&[], &[], &["xml"])));
        // A distinct user codec name is fine.
        assert!(!flagged(&deployment_with(&[], &[], &["transfer"])));
    }

    #[test]
    fn apikey_auth_values_and_http_binds_are_not_flagged() {
        // The apikey `value` is the channel's own demo auth token (a literal in all 4 shipped
        // examples) — out of scope; only datastore/broker CONNECTION credentials are ruled.
        let http = channels_of(
            "channels:\n  - name: mt-in\n    transport: http\n    bind: \"POST /channels/mt-in\"\n    \
             auth:\n      scheme: apikey\n      apikey:\n        value: mtmx-demo-key\n        \
             header: X-Api-Key\n  - name: mx-out\n    direction: outbound\n    transport: http\n    \
             bind: \"http://${MX_DEST_HOST}/mx-in\"\n",
        );
        assert!(credential_findings(&run_credential_check(&[], &http)).is_empty());
    }

    // ===== structured DiagnosticSite population ===============================================

    /// The full per-deployment pass must attach a structured `site` to each diagnostic: a
    /// channel-scoped finding anchors to its `channels.yaml` mapping key (`NamedEntry`), while
    /// artifact-file findings carry their archive-relative `file`. (BPMN process/node anchors are
    /// exercised end-to-end by the wasm round-trip test over a real archive.)
    #[test]
    fn diagnostics_carry_structured_sites() {
        // Three independent findings, three anchor forms: an unresolvable channel codec
        // (`NamedEntry` in channels.yaml), a reserved built-in codec-name shadow (a schemas/ file),
        // and an unsupported rules/ artifact (a rules/ file).
        let mut dep = deployment_with(&["bad.txt"], &[], &["xml"]);
        dep.channels_yaml = Some(
            "channels:\n  - name: in\n    transport: http\n    bind: \"POST /channels/in\"\n    \
             codec: urn:doesnotexist\n"
                .to_string(),
        );
        let mut out = Vec::new();
        validate_deployment(&dep, &mut out);

        let site_of = |code: &str| {
            out.iter()
                .find(|d| d.code == code)
                .unwrap_or_else(|| panic!("expected a {code} diagnostic in {out:?}"))
                .site
                .clone()
                .unwrap_or_else(|| panic!("{code} carries no site"))
        };

        // at_named — the channel's channels.yaml mapping key.
        let codec = site_of(codes::INBOUND_CODEC_NOT_FOUND);
        assert_eq!(codec.file, "channels.yaml");
        assert_eq!(
            codec.anchor,
            Some(DiagnosticAnchor::NamedEntry {
                name: "in".to_string()
            })
        );

        // at_file — the reserved codec-name shadow points at the schemas/ folder, no finer anchor.
        let reserved = site_of(codes::CONFIG_CODEC_RESERVED_NAME);
        assert_eq!(reserved.file, "schemas/xml");
        assert_eq!(reserved.anchor, None);

        // at_file — the unsupported artifact points at the rules/ file.
        let unsupported = site_of(codes::DEPLOY_ARTIFACT_UNSUPPORTED);
        assert_eq!(unsupported.file, "rules/bad.txt");
        assert_eq!(unsupported.anchor, None);
    }

    // ===== cross-process (collaboration) coverage checks ====================================

    use crate::coverage::{BusinessCorrelation, CoverageFile, CoverageRoute, Hop};
    use crate::scanner::LoadedProcessFile;
    use std::collections::HashMap;
    use std::sync::Arc;
    use sutra_bpmn::model::ProcessModule;
    use sutra_bpmn::qbindings::{
        AckMode, AliasBinding, DataClass, HeaderAttr, NodeBindings, ReplyMode, SendBinding,
        SourceBinding,
    };
    use sutra_executor::deployment::DeploymentId;

    fn cc_sf(id: &str, s: &str, t: &str) -> SequenceFlow {
        SequenceFlow {
            id: id.into(),
            source_ref: s.into(),
            target_ref: t.into(),
            condition: None,
        }
    }
    fn cc_headers(hs: &[(&str, &str)]) -> Vec<HeaderAttr> {
        hs.iter()
            .map(|(n, v)| HeaderAttr {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }
    /// A `<q:send>` on `channel` (or a `destination` send when `None`), setting `headers`.
    fn cc_send(channel: Option<&str>, headers: &[(&str, &str)]) -> SendBinding {
        SendBinding {
            mode: ReplyMode::Native,
            destination: channel.is_none().then(|| "external".to_string()),
            channel: channel.map(str::to_string),
            content_type: None,
            ce_type: None,
            ce_source: None,
            ce_subject: None,
            ce_data_content_type: None,
            auth: None,
            auth_secret_ref: None,
            auth_header: None,
            message_type: None,
            headers: cc_headers(headers),
        }
    }
    fn cc_alias(name: &str, expr: &str) -> AliasBinding {
        AliasBinding {
            name: name.into(),
            expression: expr.into(),
            unique: false,
            on_conflict: None,
            multi: false,
        }
    }
    fn cc_source(channel: &str) -> SourceBinding {
        SourceBinding {
            channel: channel.into(),
            name: "payload".into(),
            ack: AckMode::OnComplete,
            dedup_key: None,
            message_type: None,
            data_class: DataClass::None,
            complex_validators: vec![],
            simple_validators: vec![],
            redactors: vec![],
            message_type_value: None,
            message_type_pattern: None,
        }
    }

    /// Fixed topology — p1 (sender): `s1 -f1-> send1 -f2-> e1` (send1 carries `p1_send`);
    /// p2 (receiver): `start2 -g1-> e2` (start2 listens on `consume_ch`, carries `p2_aliases`).
    /// Both processes live in ONE module reached by both `processes` keys (the collaboration shape).
    fn corr_dep(
        p1_send: Option<SendBinding>,
        consume_ch: &str,
        p2_aliases: Vec<AliasBinding>,
        coverages: Vec<CoverageFile>,
    ) -> LoadedDeployment {
        let mut p1_bindings: HashMap<String, NodeBindings> = HashMap::new();
        if let Some(send) = p1_send {
            p1_bindings.insert(
                "send1".into(),
                NodeBindings {
                    send: Some(send),
                    ..Default::default()
                },
            );
        }
        let p1 = ProcessDefinition::of(
            "p1",
            None,
            true,
            "1.0",
            vec![
                Node::StartEvent {
                    id: "s1".into(),
                    name: None,
                    channels: vec![],
                    timer: None,
                },
                Node::SendTask {
                    id: "send1".into(),
                    name: None,
                },
                Node::EndEvent {
                    id: "e1".into(),
                    name: None,
                },
            ],
            vec![cc_sf("f1", "s1", "send1"), cc_sf("f2", "send1", "e1")],
            p1_bindings,
            vec![],
        )
        .expect("p1 builds");

        let mut p2_bindings: HashMap<String, NodeBindings> = HashMap::new();
        p2_bindings.insert(
            "start2".into(),
            NodeBindings {
                sources: vec![cc_source(consume_ch)],
                aliases: p2_aliases,
                ..Default::default()
            },
        );
        let p2 = ProcessDefinition::of(
            "p2",
            None,
            true,
            "1.0",
            vec![
                Node::StartEvent {
                    id: "start2".into(),
                    name: None,
                    channels: vec![consume_ch.into()],
                    timer: None,
                },
                Node::EndEvent {
                    id: "e2".into(),
                    name: None,
                },
            ],
            vec![cc_sf("g1", "start2", "e2")],
            p2_bindings,
            vec![],
        )
        .expect("p2 builds");

        let module = Arc::new(
            ProcessModule::of("urn:sutra:module:cc:1.0.0", vec![], vec![p1, p2])
                .expect("module builds"),
        );
        let mut processes = BTreeMap::new();
        processes.insert("p1".to_string(), Arc::clone(&module));
        processes.insert("p2".to_string(), Arc::clone(&module));
        let mut process_files = BTreeMap::new();
        process_files.insert(
            "combined.bpmn".to_string(),
            LoadedProcessFile {
                path: PathBuf::from("bpmn/combined.bpmn"),
                content: String::new(),
                module: Arc::clone(&module),
            },
        );
        LoadedDeployment {
            id: DeploymentId::of("dep-0000000000000000000000d1").expect("valid id"),
            tenant: "t".into(),
            module: "m".into(),
            version: "1.0.0".into(),
            namespace: "urn:sutra:module:cc:1.0.0".into(),
            processes,
            process_files,
            rules: BTreeMap::new(),
            templates: BTreeMap::new(),
            scripts: BTreeMap::new(),
            redactors: BTreeMap::new(),
            codecs: BTreeMap::new(),
            schema_files: BTreeMap::new(),
            migrations: BTreeMap::new(),
            coverage_files: BTreeMap::new(),
            coverages,
            channels_yaml: None,
            datastores_yaml: None,
            binding_dir: PathBuf::new(),
        }
    }

    fn cc_hop(from: &str, to: &str, key: Option<&str>) -> Hop {
        Hop {
            from_node: from.into(),
            to_node: to.into(),
            key: key.map(str::to_string),
        }
    }
    fn cc_route(path: &str, segs: &[(&str, &[&str])]) -> CoverageRoute {
        let mut segments = BTreeMap::new();
        for (p, fs) in segs {
            segments.insert(p.to_string(), fs.iter().map(|s| s.to_string()).collect());
        }
        CoverageRoute {
            path: path.into(),
            segments,
        }
    }
    /// One coverage file `urn:sutra:coverage:cc:e2e`, one correlation `transfer` (default `key`).
    fn cc_file(key: &str, hops: Vec<Hop>, routes: Vec<CoverageRoute>) -> CoverageFile {
        CoverageFile {
            urn: "urn:sutra:coverage:cc:e2e".into(),
            correlations: vec![BusinessCorrelation {
                id: "transfer".into(),
                key: key.into(),
                links: hops,
                coverages: routes,
            }],
        }
    }
    fn cc_diags(dep: &LoadedDeployment) -> Vec<LintDiagnostic> {
        cc_diags_with(dep, &[])
    }
    /// [`cc_diags`] with explicit channel definitions — exercises the outbound `local://`
    /// send-target resolution in the hop's LINK_UNRESOLVED check.
    fn cc_diags_with(dep: &LoadedDeployment, defs: &[ChannelDefinition]) -> Vec<LintDiagnostic> {
        let mut out = Vec::new();
        check_coverage_correlations(dep, defs, "t/m/1.0.0 (id)", &mut out);
        out
    }
    fn cc_has(diags: &[LintDiagnostic], code: &str) -> bool {
        diags.iter().any(|d| d.code == code)
    }

    /// The all-good baseline: one linked hop `p1:send1 → p2:start2` on channel `chA`, key `txnId`
    /// (payload-sourced), one contiguous route over both processes.
    fn positive_dep() -> LoadedDeployment {
        corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        )
    }

    #[test]
    fn cross_process_positive_is_clean() {
        let diags = cc_diags(&positive_dep());
        assert!(
            diags.is_empty(),
            "expected a clean coverage file, got {diags:?}"
        );
    }

    #[test]
    fn hop_resolves_outbound_local_send_target() {
        // Co-deployed collaboration: the emitter sends on the OUTBOUND channel
        // `to-p2` (bind `local://p2-in`); the consumer listens on the internal `p2-in`. The hop
        // link resolves through the in-process indirection — no LINK_UNRESOLVED.
        let dep = corr_dep(
            Some(cc_send(Some("to-p2"), &[])),
            "p2-in",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        let defs = channels_of(
            "channels:\n  - name: p2-in\n    transport: local\n  - name: to-p2\n    \
             direction: outbound\n    transport: local\n    bind: local://p2-in\n",
        );
        assert!(
            cc_diags_with(&dep, &defs).is_empty(),
            "outbound local:// send-target should resolve: {:?}",
            cc_diags_with(&dep, &defs)
        );
        // WITHOUT the channel defs, the bare `to-p2` cannot resolve to `p2-in` → LINK_UNRESOLVED
        // (guards that the clean verdict above comes from the resolution, not a skipped check).
        assert!(cc_has(
            &cc_diags_with(&dep, &[]),
            codes::CONFIG_CORRELATION_LINK_UNRESOLVED
        ));
    }

    #[test]
    fn segment_unknown_process_flags_process_unknown() {
        // A route segment names an undeclared process (p9).
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p9", &["z1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_PROCESS_UNKNOWN
        ));
    }

    #[test]
    fn hop_unknown_process_flags_process_unknown_only() {
        // A hop endpoint names an undeclared process — PROCESS_UNKNOWN, and the hop returns early
        // so it emits no LINK/KEY noise.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p9:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        let diags = cc_diags(&dep);
        assert!(cc_has(&diags, codes::CONFIG_CORRELATION_PROCESS_UNKNOWN));
        assert!(!cc_has(&diags, codes::CONFIG_CORRELATION_LINK_UNRESOLVED));
        assert!(!cc_has(&diags, codes::CONFIG_CORRELATION_KEY_RESOLVABLE));
    }

    #[test]
    fn segment_unknown_flow_flags_flow_unknown() {
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f9"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_FLOW_UNKNOWN
        ));
    }

    #[test]
    fn segment_non_contiguous_flags_flow_unknown() {
        // [f2, f1] — f2 ends at e1 but f1 starts at s1: not contiguous (reused relation).
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f2", "f1"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_FLOW_UNKNOWN
        ));
    }

    #[test]
    fn hop_channel_mismatch_flags_link_unresolved() {
        // Emit on chB, consumer listens on chA — no channel link.
        let dep = corr_dep(
            Some(cc_send(Some("chB"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_LINK_UNRESOLVED
        ));
    }

    #[test]
    fn hop_from_non_emitter_flags_link_unresolved() {
        // The hop's from-node is the start event `s1`, which emits nothing.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:s1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_LINK_UNRESOLVED
        ));
    }

    #[test]
    fn hop_key_missing_consumer_alias_flags_key_resolvable() {
        // The consumer has NO <q:alias>, so it cannot read the correlation key.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_KEY_RESOLVABLE
        ));
    }

    #[test]
    fn hop_header_key_resolves_when_emitter_sets_header() {
        // Consumer alias reads `header.msgId`; the emitter's <q:send> sets that header → clean.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[("msgId", "payload.txnId")])),
            "chA",
            vec![cc_alias("txnId", "header.msgId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        assert!(!cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_KEY_RESOLVABLE
        ));
    }

    #[test]
    fn hop_header_key_flags_when_emitter_omits_header() {
        // Consumer alias reads `header.msgId`, but the emitter sets no header carrying it.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "header.msgId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_KEY_RESOLVABLE
        ));
    }

    #[test]
    fn duplicate_route_path_flags_path_id_duplicate() {
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![
                    cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])]),
                    cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])]),
                ],
            )],
        );
        assert!(cc_has(
            &cc_diags(&dep),
            codes::CONFIG_CORRELATION_PATH_ID_DUPLICATE
        ));
    }

    #[test]
    fn cross_process_routes_reuse_store_missing_check() {
        // COVERAGE_STORE_REQUIRED is deliberately NOT a distinct code — the routes
        // desugar-inject `coverage_paths`, so the existing STORE_MISSING check fires for them.
        let mut dep = positive_dep();
        dep.inject_coverage_paths();
        assert!(
            !dep.processes["p1"]
                .process("p1")
                .unwrap()
                .coverage_paths
                .is_empty(),
            "the route must have injected a coverage_path onto p1"
        );

        let mut out = Vec::new();
        check_coverage_store(&dep, &[], "t/m/1.0.0 (id)", &mut out);
        assert!(
            cc_has(&out, codes::CONFIG_COVERAGE_STORE_MISSING),
            "cross-process routes must trip the reused STORE_MISSING check with no coverage store"
        );

        // With a `coverage` store declared, it does not fire.
        let store = StoreDefinition {
            name: "coverage".into(),
            store_type: "sql".into(),
            properties: BTreeMap::new(),
            structure: None,
        };
        let mut out2 = Vec::new();
        check_coverage_store(
            &dep,
            std::slice::from_ref(&store),
            "t/m/1.0.0 (id)",
            &mut out2,
        );
        assert!(!cc_has(&out2, codes::CONFIG_COVERAGE_STORE_MISSING));
    }

    #[test]
    fn coverage_diagnostics_carry_structured_sites() {
        // FLOW_UNKNOWN anchors at the (known) process; the BPMN post-pass resolves its file.
        let dep = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p2:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f9"]), ("p2", &["g1"])])],
            )],
        );
        let mut out = cc_diags(&dep);
        attach_bpmn_files(&dep, &mut out);
        let flow = out
            .iter()
            .find(|d| d.code == codes::CONFIG_CORRELATION_FLOW_UNKNOWN)
            .expect("a FLOW_UNKNOWN diagnostic")
            .site
            .clone()
            .expect("carries a site");
        assert_eq!(flow.file, "bpmn/combined.bpmn");
        assert_eq!(
            flow.anchor,
            Some(DiagnosticAnchor::BpmnProcess {
                process: "p1".into()
            })
        );

        // PROCESS_UNKNOWN anchors at the coverage file URN + correlation id (at_named).
        let dep2 = corr_dep(
            Some(cc_send(Some("chA"), &[])),
            "chA",
            vec![cc_alias("txnId", "payload.txnId")],
            vec![cc_file(
                "txnId",
                vec![cc_hop("p1:send1", "p9:start2", None)],
                vec![cc_route("r1", &[("p1", &["f1", "f2"]), ("p2", &["g1"])])],
            )],
        );
        let proc = cc_diags(&dep2)
            .iter()
            .find(|d| d.code == codes::CONFIG_CORRELATION_PROCESS_UNKNOWN)
            .expect("a PROCESS_UNKNOWN diagnostic")
            .site
            .clone()
            .expect("carries a site");
        assert_eq!(proc.file, "urn:sutra:coverage:cc:e2e");
        assert_eq!(
            proc.anchor,
            Some(DiagnosticAnchor::NamedEntry {
                name: "transfer".into()
            })
        );
    }
}
