//! The `.sutra` deployment-archive format — ONE module owns it for both the CLI
//! (`sutra package` writes) and the engine (which reads).
//!
//! - **Container**: ZIP, entries byte-sorted by path, all timestamps fixed to the
//!   ZIP epoch (1980-01-01 — DOS time cannot represent 1970), no extra fields, UTF-8
//!   forward-slash names, deflate. Two writes over identical inputs are byte-identical.
//!   Bounds: max 4096 entries, max path depth 8, no nested archives.
//! - **Manifest**: [`ArchiveManifest`] ↔ `manifest.yaml` at the archive root,
//!   serialised canonically (stable key order, uniform quoting) so the manifest bytes —
//!   and therefore the deploymentId — are deterministic.
//! - **Identity**: `deploymentId = "dep-" + first 24 lowercase hex of
//!   sha256(manifest bytes)` — [`deployment_id_of_manifest`]. Content-addressed: the
//!   engine re-derives it on load; nothing in the archive stores it.
//! - **Reader**: open → manifest schema verify → digest-verify every artifact →
//!   stowaway detection → parse/validate content through the same fail-closed suite
//!   `sutra lint` runs → a [`LoadedDeployment`]-shaped result.

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use sutra_bpmn::loader::BpmnModelLoader;
use sutra_bpmn::model::{ProcessAudit, ProcessModule};
use sutra_bpmn::qbindings::AuditCapture;
use sutra_executor::deployment::DeploymentId;

use crate::error::{codes, LoaderError};
use crate::lint::{validate_deployment, LintDiagnostic, LintSeverity};
use crate::scanner::{LoadedArtifact, LoadedDeployment, LoadedProcessFile};

/// Archive root manifest file name.
pub const MANIFEST_FILE_NAME: &str = "manifest.yaml";
/// This contract's manifest major (engines reject unknown majors).
pub const MANIFEST_VERSION: u64 = 1;
/// The single engine-contract level this packager targets (one integer, by contract).
pub const ENGINE_MIN_CONTRACT: u64 = 1;
/// Packaging bound: maximum entry count (diagnostic-bounded, not a design ceiling).
pub const MAX_ENTRIES: usize = 4096;
/// Packaging bound: maximum `'/'`-separated path depth.
pub const MAX_PATH_DEPTH: usize = 8;
/// The archive file extension.
pub const ARCHIVE_EXTENSION: &str = "sutra";

/// One `artifacts[]` row: an archive entry path and the sha256 of its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactEntry {
    /// Forward-slash archive-local path (unique across the manifest).
    pub path: String,
    /// 64 lowercase hex chars — sha256 of the entry bytes.
    pub sha256: String,
}

/// The parsed `manifest.yaml` (the normative manifest schema).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveManifest {
    /// `manifestVersion` — must equal [`MANIFEST_VERSION`].
    pub manifest_version: u64,
    /// `engine.minContract` — minimum engine-contract level required.
    pub engine_min_contract: u64,
    /// `labels` — OPAQUE to the engine (CLI/observability selectors only).
    pub labels: BTreeMap<String, String>,
    /// `supersedes` — deploymentIds this one replaces (lineage).
    pub supersedes: Vec<String>,
    /// `entryProcesses` — informational index of externally-reachable processes.
    pub entry_processes: Vec<String>,
    /// B1 — `audit.{sink,capture}`: the deployment-wide audit default (single sink + capture level)
    /// for every process that declares no process-level `<q:audit>`. Part of the identity-bearing
    /// manifest by design: a node/process `<q:audit>` already remints the deploymentId (it lives in
    /// a `.bpmn` artifact hashed here), so the deployment-level default is identity-bearing too —
    /// every audit routing change is a new, lineage-tracked deployment version. `None` ⇒ no default
    /// (absent from the serialization, so existing deploymentIds are unchanged).
    pub audit: Option<ProcessAudit>,
    /// `artifacts[]` — EVERY entry in the archive except `manifest.yaml` itself.
    pub artifacts: Vec<ArtifactEntry>,
}

impl ArchiveManifest {
    /// Canonical serialisation — deterministic bytes (stable key order, uniform double
    /// quoting), because sha256 of exactly these bytes IS the deploymentId.
    pub fn to_yaml(&self) -> String {
        let mut out = String::with_capacity(256 + self.artifacts.len() * 96);
        out.push_str(&format!("manifestVersion: {}\n", self.manifest_version));
        out.push_str("engine:\n");
        out.push_str(&format!("  minContract: {}\n", self.engine_min_contract));
        // B1 — the deployment audit default. Emitted only when present, so a manifest without an
        // audit default serializes to exactly its pre-B1 bytes (deploymentId unchanged).
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
        if self.labels.is_empty() {
            out.push_str("labels: {}\n");
        } else {
            out.push_str("labels:\n");
            for (key, value) in &self.labels {
                out.push_str(&format!("  {}: {}\n", quote(key), quote(value)));
            }
        }
        push_string_list(&mut out, "supersedes", &self.supersedes);
        push_string_list(&mut out, "entryProcesses", &self.entry_processes);
        if self.artifacts.is_empty() {
            out.push_str("artifacts: []\n");
        } else {
            out.push_str("artifacts:\n");
            for artifact in &self.artifacts {
                out.push_str(&format!("  - path: {}\n", quote(&artifact.path)));
                out.push_str(&format!("    sha256: {}\n", quote(&artifact.sha256)));
            }
        }
        out
    }

    /// Parse + schema-verify a `manifest.yaml`. Strict: unknown top-level keys,
    /// a manifest major other than [`MANIFEST_VERSION`], duplicate artifact paths, or a
    /// malformed sha256 all reject.
    pub fn parse(text: &str) -> Result<ArchiveManifest, LoaderError> {
        let invalid = |msg: String| LoaderError::new(codes::DEPLOY_ARCHIVE_MANIFEST_INVALID, msg);
        let parsed: serde_yaml::Value = serde_yaml::from_str(text)
            .map_err(|e| invalid(format!("manifest.yaml does not parse: {e}")))?;
        let root = parsed
            .as_mapping()
            .ok_or_else(|| invalid("manifest.yaml must be a YAML mapping".to_string()))?;

        const KNOWN_KEYS: [&str; 7] = [
            "manifestVersion",
            "engine",
            "labels",
            "supersedes",
            "entryProcesses",
            "audit",
            "artifacts",
        ];
        for key in root.keys() {
            let name = key.as_str().unwrap_or("<non-string>");
            if !KNOWN_KEYS.contains(&name) {
                return Err(invalid(format!(
                    "manifest.yaml declares unknown key '{name}' (the manifest schema is closed)"
                )));
            }
        }

        let manifest_version = root
            .get(serde_yaml::Value::from("manifestVersion"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| invalid("manifestVersion is required (integer)".to_string()))?;
        if manifest_version != MANIFEST_VERSION {
            return Err(invalid(format!(
                "manifestVersion {manifest_version} is not supported (this engine implements \
                 manifest major {MANIFEST_VERSION}; unknown majors reject)"
            )));
        }

        let engine = root
            .get(serde_yaml::Value::from("engine"))
            .and_then(|v| v.as_mapping())
            .ok_or_else(|| invalid("engine block is required".to_string()))?;
        let engine_min_contract = engine
            .get(serde_yaml::Value::from("minContract"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| invalid("engine.minContract is required (integer)".to_string()))?;

        let mut labels = BTreeMap::new();
        if let Some(value) = root.get(serde_yaml::Value::from("labels")) {
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

        let supersedes = parse_string_list(root, "supersedes", &invalid)?;
        let entry_processes = parse_string_list(root, "entryProcesses", &invalid)?;

        // B1 — the deployment-wide audit default (single sink + capture level). Absent ⇒ None.
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

        let mut artifacts = Vec::new();
        let mut seen_paths = std::collections::BTreeSet::new();
        if let Some(value) = root.get(serde_yaml::Value::from("artifacts")) {
            let list = value
                .as_sequence()
                .ok_or_else(|| invalid("artifacts must be a list".to_string()))?;
            for (i, item) in list.iter().enumerate() {
                let entry = item
                    .as_mapping()
                    .ok_or_else(|| invalid(format!("artifacts[{i}] must be a mapping")))?;
                let path = entry
                    .get(serde_yaml::Value::from("path"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid(format!("artifacts[{i}].path is required")))?;
                let sha256 = entry
                    .get(serde_yaml::Value::from("sha256"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| invalid(format!("artifacts[{i}].sha256 is required")))?;
                if sha256.len() != 64
                    || !sha256
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                {
                    return Err(invalid(format!(
                        "artifacts[{i}].sha256 must be 64 lowercase hex chars"
                    )));
                }
                if path == MANIFEST_FILE_NAME {
                    return Err(invalid(
                        "artifacts[] must not list manifest.yaml itself".to_string(),
                    ));
                }
                if !seen_paths.insert(path.to_string()) {
                    return Err(invalid(format!(
                        "artifacts[] lists duplicate path '{path}'"
                    )));
                }
                artifacts.push(ArtifactEntry {
                    path: path.to_string(),
                    sha256: sha256.to_string(),
                });
            }
        }

        Ok(ArchiveManifest {
            manifest_version,
            engine_min_contract,
            labels,
            supersedes,
            entry_processes,
            audit,
            artifacts,
        })
    }
}

fn parse_string_list(
    root: &serde_yaml::Mapping,
    key: &str,
    invalid: &dyn Fn(String) -> LoaderError,
) -> Result<Vec<String>, LoaderError> {
    let Some(value) = root.get(serde_yaml::Value::from(key)) else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let list = value
        .as_sequence()
        .ok_or_else(|| invalid(format!("{key} must be a list")))?;
    let mut out = Vec::new();
    for item in list {
        let s = item
            .as_str()
            .ok_or_else(|| invalid(format!("{key} entries must be strings")))?;
        out.push(s.to_string());
    }
    Ok(out)
}

/// YAML double-quoted scalar with `\` and `"` escaped — uniform quoting keeps the
/// canonical bytes trivially deterministic.
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

fn push_string_list(out: &mut String, key: &str, values: &[String]) {
    if values.is_empty() {
        out.push_str(&format!("{key}: []\n"));
    } else {
        out.push_str(&format!("{key}:\n"));
        for value in values {
            out.push_str(&format!("  - {}\n", quote(value)));
        }
    }
}

/// Lowercase-hex sha256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for b in digest {
        hex.push_str(&format!("{b:02x}"));
    }
    hex
}

/// The normative identity derivation: `dep-` + first 24 lowercase hex of
/// sha256(manifest bytes). This is the sole deploymentId derivation; the older
/// authoring-triple derivation was removed once the engine went archives-only.
pub fn deployment_id_of_manifest(manifest_bytes: &[u8]) -> DeploymentId {
    let digest = Sha256::digest(manifest_bytes);
    let mut id = String::with_capacity(28);
    id.push_str("dep-");
    for b in digest.iter().take(12) {
        id.push_str(&format!("{b:02x}"));
    }
    DeploymentId::of(&id).expect("derived id is well-formed by construction")
}

/// Validate one archive entry path against the container rules.
fn validate_entry_path(path: &str) -> Result<(), LoaderError> {
    let format_invalid = |msg: String| LoaderError::new(codes::DEPLOY_ARCHIVE_FORMAT_INVALID, msg);
    if path.is_empty() {
        return Err(format_invalid("empty entry path".to_string()));
    }
    if path.contains('\\') {
        return Err(format_invalid(format!(
            "entry '{path}' uses backslashes — archive paths must be forward-slash separated"
        )));
    }
    if path.starts_with('/') {
        return Err(format_invalid(format!("entry '{path}' is absolute")));
    }
    let segments: Vec<&str> = path.split('/').collect();
    if segments
        .iter()
        .any(|s| s.is_empty() || *s == "." || *s == "..")
    {
        return Err(format_invalid(format!(
            "entry '{path}' has an empty or dot path segment"
        )));
    }
    if segments.len() > MAX_PATH_DEPTH {
        return Err(LoaderError::new(
            codes::DEPLOY_PACKAGE_LIMIT_EXCEEDED,
            format!(
                "entry '{path}' exceeds the max path depth of {MAX_PATH_DEPTH} \
                 (depth {})",
                segments.len()
            ),
        ));
    }
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".zip") || lower.ends_with(".sutra") || lower.ends_with(".jar") {
        return Err(format_invalid(format!(
            "entry '{path}' looks like a nested archive — nested archives are forbidden"
        )));
    }
    Ok(())
}

/// Write a deterministic `.sutra` ZIP from `entries` (path → bytes). The `BTreeMap` key
/// order IS the normative byte-wise ascending entry order; timestamps are the fixed ZIP
/// epoch; no extra fields are written. Rejects paths violating the container rules and the
/// 4096-entry bound.
pub fn write_archive(entries: &BTreeMap<String, Vec<u8>>) -> Result<Vec<u8>, LoaderError> {
    if entries.len() > MAX_ENTRIES {
        return Err(LoaderError::new(
            codes::DEPLOY_PACKAGE_LIMIT_EXCEEDED,
            format!(
                "archive would contain {} entries — the bound is {MAX_ENTRIES}",
                entries.len()
            ),
        ));
    }
    for path in entries.keys() {
        if path != MANIFEST_FILE_NAME {
            validate_entry_path(path)?;
        }
    }
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .last_modified_time(zip::DateTime::default())
        .unix_permissions(0o644);
    for (path, bytes) in entries {
        writer.start_file(path, options).map_err(|e| {
            LoaderError::new(
                codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
                format!("failed to start archive entry '{path}': {e}"),
            )
        })?;
        writer.write_all(bytes).map_err(|e| {
            LoaderError::new(
                codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
                format!("failed to write archive entry '{path}': {e}"),
            )
        })?;
    }
    let cursor = writer.finish().map_err(|e| {
        LoaderError::new(
            codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
            format!("failed to finish archive: {e}"),
        )
    })?;
    Ok(cursor.into_inner())
}

/// A fully verified, parsed archive: the derived identity, the manifest, and the
/// [`LoadedDeployment`]-shaped content the engine assembly consumes.
#[derive(Debug, Clone)]
pub struct LoadedArchive {
    /// `dep-<24 hex>` derived from the manifest bytes.
    pub id: DeploymentId,
    pub manifest: ArchiveManifest,
    pub deployment: LoadedDeployment,
}

/// Open and fully verify a `.sutra` archive (the normative load sequence). Fail-closed: any
/// container, manifest, digest, stowaway, or content-validation failure rejects the whole
/// archive and nothing is returned.
pub fn read_archive(bytes: &[u8]) -> Result<LoadedArchive, LoaderError> {
    read_archive_internal(bytes, None)
}

/// [`read_archive`] plus an identity expectation (e.g. the id a deployments-source file
/// name or ledger row carries): a recomputed deploymentId differing from `expected`
/// rejects with [`codes::DEPLOY_ARCHIVE_ID_MISMATCH`].
pub fn read_archive_expecting(
    bytes: &[u8],
    expected: &DeploymentId,
) -> Result<LoadedArchive, LoaderError> {
    read_archive_internal(bytes, Some(expected))
}

/// Read + verify a `.sutra` file from disk.
pub fn read_archive_file(path: &Path) -> Result<LoadedArchive, LoaderError> {
    let bytes = std::fs::read(path).map_err(|e| {
        LoaderError::new(
            codes::DEPLOY_ARCHIVE_FORMAT_INVALID,
            format!("failed to read archive {}: {e}", path.display()),
        )
    })?;
    read_archive(&bytes)
}

/// Reconstruct a [`LoadedDeployment`] from a raw, in-memory interior file set (archive-local
/// path → bytes: `bpmn/order.bpmn`, `channels.yaml`, `schemas/<codec>/x.xsd`, …) WITHOUT sealing
/// or verifying a `.sutra` container — the entry point the WASM lint core (and thus the LSP) uses,
/// holding the loose deployment-package files and wanting to run the FULL advisory lint rather
/// than the fail-closed [`read_archive`] (which rejects on the first content error and so cannot
/// surface the complete diagnostic set an editor needs).
///
/// It shares the **exact** [`parse_deployment`] reconstruction the verifying reader runs, so the
/// in-editor deployment model is byte-identical to the deploy-time one — no parallel path to drift
/// (the whole point of the WASM-shared core). Container-integrity checks (zip decode, manifest
/// schema, digests, stowaways) do not apply here: the caller vouches for the file set, and a
/// synthetic manifest supplies the audit default (none) plus the opaque tenant/module/version
/// `labels`. The one hard prerequisite for building the model still rejects: a `.bpmn` entry that
/// fails to parse returns its parse error (there is no process model to lint without it).
pub fn deployment_from_entries(
    entries: &BTreeMap<String, Vec<u8>>,
    labels: BTreeMap<String, String>,
) -> Result<LoadedDeployment, LoaderError> {
    let manifest = ArchiveManifest {
        labels,
        ..ArchiveManifest::default()
    };
    let id = deployment_id_of_manifest(manifest.to_yaml().as_bytes());
    parse_deployment(&id, &manifest, entries)
}

fn read_archive_internal(
    bytes: &[u8],
    expected: Option<&DeploymentId>,
) -> Result<LoadedArchive, LoaderError> {
    let entries = read_entries(bytes)?;

    // ---- manifest schema verification ------------------------------------------------
    let manifest_bytes = entries.get(MANIFEST_FILE_NAME).ok_or_else(|| {
        LoaderError::new(
            codes::DEPLOY_ARCHIVE_MANIFEST_INVALID,
            "archive contains no manifest.yaml at its root",
        )
    })?;
    let manifest_text = std::str::from_utf8(manifest_bytes).map_err(|_| {
        LoaderError::new(
            codes::DEPLOY_ARCHIVE_MANIFEST_INVALID,
            "manifest.yaml is not valid UTF-8",
        )
    })?;
    let manifest = ArchiveManifest::parse(manifest_text)?;

    // ---- identity: recompute from the manifest bytes ---------------------------------
    let id = deployment_id_of_manifest(manifest_bytes);
    if let Some(expected) = expected {
        if &id != expected {
            return Err(LoaderError::new(
                codes::DEPLOY_ARCHIVE_ID_MISMATCH,
                format!(
                    "archive manifest derives deploymentId {id} but {expected} was expected \
                     — the archive is not the sealed content this id names"
                ),
            ));
        }
    }

    // ---- digest verification + stowaway detection ------------------------------------
    let mut listed = std::collections::BTreeSet::new();
    for artifact in &manifest.artifacts {
        listed.insert(artifact.path.as_str());
        let Some(entry) = entries.get(&artifact.path) else {
            return Err(LoaderError::new(
                codes::DEPLOY_ARCHIVE_MANIFEST_INVALID,
                format!(
                    "manifest lists artifact '{}' but the archive has no such entry",
                    artifact.path
                ),
            ));
        };
        let actual = sha256_hex(entry);
        if actual != artifact.sha256 {
            return Err(LoaderError::new(
                codes::DEPLOY_ARCHIVE_DIGEST_MISMATCH,
                format!(
                    "artifact '{}' hashes to {actual} but the manifest declares {} — the \
                     archive was modified after packaging (sealed archives are verified, \
                     not policed)",
                    artifact.path, artifact.sha256
                ),
            ));
        }
    }
    for path in entries.keys() {
        if path != MANIFEST_FILE_NAME && !listed.contains(path.as_str()) {
            return Err(LoaderError::new(
                codes::DEPLOY_ARCHIVE_STOWAWAY,
                format!(
                    "archive entry '{path}' is not listed in the manifest's artifacts[] — \
                     unlisted entries reject (no stowaways)"
                ),
            ));
        }
    }

    // ---- parse + validate content (the same fail-closed suite `sutra lint` runs) ----
    let deployment = parse_deployment(&id, &manifest, &entries)?;
    let mut diagnostics: Vec<LintDiagnostic> = Vec::new();
    validate_deployment(&deployment, &mut diagnostics);
    if let Some(first) = diagnostics
        .iter()
        .find(|d| d.severity == LintSeverity::Error)
    {
        let error_count = diagnostics
            .iter()
            .filter(|d| d.severity == LintSeverity::Error)
            .count();
        return Err(LoaderError::new(
            codes::DEPLOY_ARCHIVE_CONTENT_INVALID,
            format!(
                "archive content fails package-time validation ({error_count} error(s); \
                 first: [{}] {})",
                first.code, first.message
            ),
        ));
    }

    Ok(LoadedArchive {
        id,
        manifest,
        deployment,
    })
}

/// Decode the raw ZIP into path → bytes, enforcing the container rules.
fn read_entries(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, LoaderError> {
    let format_invalid = |msg: String| LoaderError::new(codes::DEPLOY_ARCHIVE_FORMAT_INVALID, msg);
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| format_invalid(format!("not a readable .sutra ZIP: {e}")))?;
    if archive.len() > MAX_ENTRIES {
        return Err(LoaderError::new(
            codes::DEPLOY_PACKAGE_LIMIT_EXCEEDED,
            format!(
                "archive contains {} entries — the bound is {MAX_ENTRIES}",
                archive.len()
            ),
        ));
    }
    let mut entries = BTreeMap::new();
    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| format_invalid(format!("unreadable archive entry #{i}: {e}")))?;
        let name = std::str::from_utf8(file.name_raw())
            .map_err(|_| format_invalid(format!("entry #{i} has a non-UTF-8 name")))?
            .to_string();
        if name.ends_with('/') {
            return Err(format_invalid(format!(
                "entry '{name}' is a directory marker — the deterministic container has \
                 file entries only"
            )));
        }
        if name != MANIFEST_FILE_NAME {
            validate_entry_path(&name)?;
        }
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)
            .map_err(|e| format_invalid(format!("failed to read entry '{name}': {e}")))?;
        if entries.insert(name.clone(), buf).is_some() {
            return Err(format_invalid(format!("duplicate archive entry '{name}'")));
        }
    }
    Ok(entries)
}

/// Rebuild the [`LoadedDeployment`] shape from verified archive entries. Labels supply
/// the (opaque) tenant/module/version strings; the namespace comes from the parsed BPMN.
fn parse_deployment(
    id: &DeploymentId,
    manifest: &ArchiveManifest,
    entries: &BTreeMap<String, Vec<u8>>,
) -> Result<LoadedDeployment, LoaderError> {
    let content_invalid =
        |msg: String| LoaderError::new(codes::DEPLOY_ARCHIVE_CONTENT_INVALID, msg);
    let text = |path: &str, bytes: &[u8]| -> Result<String, LoaderError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|_| content_invalid(format!("entry '{path}' is not valid UTF-8 text")))
    };

    let loader = BpmnModelLoader;
    let mut processes: BTreeMap<String, Arc<ProcessModule>> = BTreeMap::new();
    let mut process_files: BTreeMap<String, LoadedProcessFile> = BTreeMap::new();
    let mut namespace: Option<String> = None;
    let mut rules = BTreeMap::new();
    let mut templates = BTreeMap::new();
    let mut scripts = BTreeMap::new();
    let mut redactors = BTreeMap::new();
    let mut schema_files = BTreeMap::new();
    let mut migrations = BTreeMap::new();
    let mut coverage_files = BTreeMap::new();
    let mut channels_yaml = None;
    let mut datastores_yaml = None;

    for (path, bytes) in entries {
        if path == MANIFEST_FILE_NAME {
            continue;
        }
        let artifact = |content: String| LoadedArtifact {
            path: PathBuf::from(path),
            content,
        };
        if let Some(sub) = path.strip_prefix("bpmn/") {
            let content = text(path, bytes)?;
            // B1 — desugar the manifest-level audit default onto every process that declares no
            // process-level `<q:audit>`; each process then carries its effective ProcessAudit.
            let module = loader
                .load(content.as_bytes())
                .map_err(|e| content_invalid(format!("BPMN entry '{path}' fails to parse: {e}")))?
                .with_audit_default(&manifest.audit);
            match &namespace {
                None => namespace = Some(module.target_namespace.clone()),
                Some(ns) if *ns != module.target_namespace => {
                    return Err(content_invalid(format!(
                        "BPMN entry '{path}' declares targetNamespace '{}' but the archive's \
                         other processes declare '{ns}' — one archive is one namespace",
                        module.target_namespace
                    )));
                }
                Some(_) => {}
            }
            let module = Arc::new(module);
            for pid in module.process_ids() {
                if processes.contains_key(pid) {
                    return Err(content_invalid(format!(
                        "process id '{pid}' is defined by more than one bpmn/** entry — a \
                         sealed archive must define each process exactly once"
                    )));
                }
                processes.insert(pid.to_string(), Arc::clone(&module));
            }
            process_files.insert(
                sub.to_string(),
                LoadedProcessFile {
                    path: PathBuf::from(path),
                    content,
                    module,
                },
            );
        } else if let Some(sub) = path.strip_prefix("rules/") {
            rules.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("templates/") {
            templates.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("scripts/") {
            scripts.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("redactors/") {
            redactors.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("schemas/") {
            schema_files.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("migrations/") {
            migrations.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if let Some(sub) = path.strip_prefix("coverage/") {
            coverage_files.insert(sub.to_string(), artifact(text(path, bytes)?));
        } else if path == "channels.yaml" {
            channels_yaml = Some(text(path, bytes)?);
        } else if path == "datastores.yaml" {
            datastores_yaml = Some(text(path, bytes)?);
        } else {
            return Err(content_invalid(format!(
                "entry '{path}' is outside the archive interior layout (bpmn/, rules/, \
                 templates/, scripts/, redactors/, schemas/, migrations/, coverage/, \
                 channels.yaml, datastores.yaml)"
            )));
        }
    }

    // Rebuild the codec map from schemas/**: a codec is WHEREVER a `codec-manifest.yaml`
    // sits, named by the relative path from `schemas/` to that folder (`/`→`:`) — the
    // path-derived name that the canonical `urn:<name>` codec reference carries. Each XSD
    // composes the codec of the DEEPEST codec-manifest ancestor folder. Backward-compatible
    // with the flat `schemas/<codec>/` layout (`schemas/transfer/` → `transfer`); a bare
    // `schemas/<name>.xsd` with no manifest falls back to its stem.
    let codecs = build_codec_map(&schema_files);

    // The label triple is OPAQUE (never runtime identity) — it feeds observability and the
    // channel-binding Namespace shim.
    let label = |key: &str| {
        manifest
            .labels
            .get(key)
            .cloned()
            .unwrap_or_else(|| "unlabeled".to_string())
    };

    let mut deployment = LoadedDeployment {
        id: id.clone(),
        tenant: label("tenant"),
        module: label("module"),
        version: label("version"),
        namespace: namespace.unwrap_or_default(),
        processes,
        process_files,
        rules,
        templates,
        scripts,
        redactors,
        codecs,
        schema_files,
        migrations,
        coverage_files,
        coverages: Vec::new(),
        channels_yaml,
        datastores_yaml,
        // No directory backs a sealed archive; migrations travel in-memory above.
        binding_dir: PathBuf::new(),
    };
    // Parse `coverage/**` into the deployment table and desugar-inject each route's
    // per-process sub-paths onto the referenced `ProcessDefinition`s (so the runtime cursor
    // marks them). Resilient: a referenced-but-absent processId is skipped (lint catches it).
    deployment.resolve_coverage(codes::DEPLOY_ARCHIVE_CONTENT_INVALID)?;
    Ok(deployment)
}

/// Build the codec map from the flat `schemas/**` file set (subpath → artifact): a codec
/// is wherever a `codec-manifest.yaml` sits, named by the relative path from `schemas/` to
/// that folder with `/`→`:`. Each `.xsd` composes the codec of the DEEPEST codec-manifest
/// ancestor folder; an `.xsd` with no codec-manifest ancestor falls back to the first-folder
/// (or stem) convention, so the flat `schemas/<codec>/…` layout is unchanged.
fn build_codec_map(
    schema_files: &BTreeMap<String, LoadedArtifact>,
) -> BTreeMap<String, Vec<LoadedArtifact>> {
    // Codec folders = the parent dirs (relative to schemas/) of every codec-manifest.yaml.
    let mut codec_folders: Vec<String> = schema_files
        .keys()
        .filter(|sub| sub.rsplit('/').next() == Some("codec-manifest.yaml"))
        .map(|sub| match sub.rsplit_once('/') {
            Some((dir, _)) => dir.to_string(),
            None => String::new(), // codec-manifest.yaml directly under schemas/
        })
        .collect();
    // Deepest-first so the longest matching ancestor wins.
    codec_folders.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));

    let mut codecs: BTreeMap<String, Vec<LoadedArtifact>> = BTreeMap::new();
    for (sub, artifact) in schema_files {
        if !sub.ends_with(".xsd") {
            continue;
        }
        let ancestor = codec_folders.iter().find(|folder| {
            folder.is_empty()
                || sub
                    .strip_prefix(folder.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        });
        let codec_name = match ancestor {
            Some(folder) => folder.replace('/', ":"),
            // No codec-manifest ancestor: fall back to the first-folder / stem convention.
            None => match sub.split_once('/') {
                Some((first, _)) => first.to_string(),
                None => sub.trim_end_matches(".xsd").to_string(),
            },
        };
        codecs.entry(codec_name).or_default().push(artifact.clone());
    }
    codecs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> ArchiveManifest {
        ArchiveManifest {
            manifest_version: 1,
            engine_min_contract: 1,
            labels: BTreeMap::from([
                ("tenant".to_string(), "default".to_string()),
                ("module".to_string(), "m".to_string()),
                ("version".to_string(), "1.0.0".to_string()),
            ]),
            supersedes: Vec::new(),
            entry_processes: vec!["p".to_string()],
            // Exercise the audit default through the canonical round-trip (deploymentId input).
            audit: Some(ProcessAudit {
                sink: "sql".to_string(),
                capture: AuditCapture::Payload,
            }),
            artifacts: vec![ArtifactEntry {
                path: "bpmn/p.bpmn".to_string(),
                sha256: "a".repeat(64),
            }],
        }
    }

    #[test]
    fn manifest_round_trips_canonically() {
        let manifest = sample_manifest();
        let yaml = manifest.to_yaml();
        let parsed = ArchiveManifest::parse(&yaml).expect("canonical form parses");
        assert_eq!(parsed, manifest);
        // Serialisation is a fixed point — the deploymentId input is stable.
        assert_eq!(parsed.to_yaml(), yaml);
    }

    #[test]
    fn manifest_rejects_unknown_major_and_unknown_keys() {
        let bad_major = sample_manifest()
            .to_yaml()
            .replace("manifestVersion: 1", "manifestVersion: 2");
        assert!(ArchiveManifest::parse(&bad_major).is_err());
        let unknown_key = format!("{}extra: true\n", sample_manifest().to_yaml());
        assert!(ArchiveManifest::parse(&unknown_key).is_err());
    }

    #[test]
    fn deployment_id_is_manifest_hash_prefixed() {
        let bytes = sample_manifest().to_yaml().into_bytes();
        let id = deployment_id_of_manifest(&bytes);
        assert_eq!(id.value().len(), 28);
        assert!(id.value().starts_with("dep-"));
        assert_eq!(id.value()[4..], sha256_hex(&bytes)[..24]);
    }

    #[test]
    fn entry_paths_enforce_container_rules() {
        assert!(validate_entry_path("bpmn/x.bpmn").is_ok());
        assert!(validate_entry_path("a/b/c/d/e/f/g/h.txt").is_ok()); // depth 8
        assert!(validate_entry_path("a/b/c/d/e/f/g/h/i.txt").is_err()); // depth 9
        assert!(validate_entry_path("/abs.txt").is_err());
        assert!(validate_entry_path("win\\path.txt").is_err());
        assert!(validate_entry_path("a/../b.txt").is_err());
        assert!(validate_entry_path("inner.sutra").is_err());
        assert!(validate_entry_path("inner.zip").is_err());
    }

    #[test]
    fn write_is_deterministic_and_epoch_stamped() {
        let entries = BTreeMap::from([
            ("b.txt".to_string(), b"bee".to_vec()),
            ("a.txt".to_string(), b"ay".to_vec()),
        ]);
        let one = write_archive(&entries).expect("writes");
        let two = write_archive(&entries).expect("writes again");
        assert_eq!(one, two, "byte-identical reruns (container determinism)");

        let mut za = zip::ZipArchive::new(Cursor::new(one.as_slice())).expect("opens");
        let names: Vec<String> = (0..za.len())
            .map(|i| za.by_index(i).unwrap().name().to_string())
            .collect();
        assert_eq!(names, vec!["a.txt", "b.txt"], "byte-sorted entry order");
        let f = za.by_index(0).expect("entry");
        let dt = f.last_modified().expect("dos time present");
        assert_eq!(
            (
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second()
            ),
            (1980, 1, 1, 0, 0, 0),
            "fixed ZIP-epoch timestamp"
        );
        assert!(
            f.extra_data().is_none_or(|d| d.is_empty()),
            "no platform extra fields"
        );
    }
}
