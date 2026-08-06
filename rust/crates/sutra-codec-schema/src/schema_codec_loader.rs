//! Loads a module version's `schemas/` folder into URN-keyed typed codecs — the
//! schema-codec loader and its load-error type.
//!
//! The folder holds **one subfolder per codec** (the subfolder name *is* the codec name), each
//! with a required `codec-manifest.yaml` declaring `schemaKind` (`xsd` | `json-schema` | a
//! registered [`crate::bundle`] kind) and, for the two generic kinds, the wire `formats` it
//! accepts; the schema files are discovered by convention (every file with the kind's extension —
//! a bundle instead sees its folder's whole file tree and maps it in its own manifest).
//! Registering by URN (`<module-namespace>:<codec-folder>`) is what lets two variants of the
//! same message definition coexist without collision. Layout / manifest / schema problems are fail-closed deploy errors
//! carrying the exact `SUTRA.CONFIG.CODEC_*` / `SUTRA.CONFIG.SCHEMA.INVALID` code the scanner
//! surfaces.
//!
//! Note (architecture): the folder-scan half of this logic is a loader concern; in
//! the Rust engine the natural long-term home is `sutra-loader` (which owns all filesystem I/O),
//! but the typed-codec composition it produces lives here, so the scan rides along with it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::bundle::{BundleCodecKind, BundleSource};
use crate::json_schema::{JsonSchema, JsonSchemaCodec};
use crate::structural::StructuralCodec;
use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes;
use sutra_codec_spi::schema::SchemaKind;

pub(crate) const CODEC_MANIFEST_FILE: &str = "codec-manifest.yaml";
const XSD_EXT: &str = ".xsd";
const JSON_EXT: &str = ".json";
const JSON_SCHEMA_EXT: &str = ".schema.json";

const XSD_FORMATS: &[&str] = &["xml", "json", "yaml"];
const JSON_FORMATS: &[&str] = &["json"];

/// A schema-codec load failure carrying the stable `SUTRA.CONFIG.*` diagnostic code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecLoadError {
    code: &'static str,
    message: String,
}

impl CodecLoadError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> CodecLoadError {
        CodecLoadError {
            code,
            message: message.into(),
        }
    }

    /// The stable `SUTRA.CONFIG.*` diagnostic code the loader failed with.
    pub fn code(&self) -> &str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for CodecLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CodecLoadError {}

/// The registry key for a module-supplied schema codec: the module's version-bearing namespace
/// plus the codec name (folder name).
pub fn codec_urn(module_namespace: &str, codec_name: &str) -> String {
    format!("{module_namespace}:{codec_name}")
}

/// Compose each codec folder under `schemas_dir` into a URN-keyed typed codec. An absent /
/// non-directory `schemas_dir` yields no codecs. Layout / manifest / schema problems are a
/// [`CodecLoadError`].
pub fn load(
    schemas_dir: &Path,
    module_namespace: &str,
) -> Result<Vec<Arc<dyn PayloadCodec>>, CodecLoadError> {
    if module_namespace.trim().is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            "moduleNamespace is required to build a codec URN",
        ));
    }
    if !schemas_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut codecs: Vec<Arc<dyn PayloadCodec>> = Vec::new();
    for entry in sorted_entries(schemas_dir)? {
        if entry.is_file() {
            let name = entry
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            return Err(CodecLoadError::new(
                codes::CONFIG_CODEC_LAYOUT_INVALID,
                format!(
                    "schemas/ must contain only codec folders, but found a loose file '{name}' — \
                     put each codec's schema(s) under schemas/<codec>/ with a {CODEC_MANIFEST_FILE}"
                ),
            ));
        }
        if entry.is_dir() {
            codecs.push(load_codec_folder(&entry, module_namespace)?);
        }
    }
    Ok(codecs)
}

fn load_codec_folder(
    folder: &Path,
    module_namespace: &str,
) -> Result<Arc<dyn PayloadCodec>, CodecLoadError> {
    let codec_name = folder
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let urn = codec_urn(module_namespace, &codec_name);
    let manifest_path = folder.join(CODEC_MANIFEST_FILE);
    if !manifest_path.is_file() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_MISSING,
            format!("codec folder '{codec_name}' has no {CODEC_MANIFEST_FILE} (required)"),
        ));
    }
    let manifest = parse_manifest(&manifest_path, &codec_name)?;
    // A BUNDLE kind owns its whole folder tree (edition subfolders and all) and maps it in its
    // own manifest; the generic kinds discover schema files by extension in the folder itself.
    if let ManifestKind::Bundle(kind) = manifest.kind {
        return build_bundle_codec(kind, folder, &codec_name);
    }
    let xsd_files = files_with_extension(folder, XSD_EXT)?;
    let json_files = files_with_extension(folder, JSON_EXT)?;
    match manifest.kind {
        ManifestKind::Schema(SchemaKind::Xsd) => {
            build_xsd_codec(&urn, &codec_name, &manifest, &xsd_files, &json_files)
        }
        ManifestKind::Schema(SchemaKind::JsonSchema) => {
            build_json_codec(&urn, &codec_name, &xsd_files, &json_files)
        }
        ManifestKind::Bundle(_) => unreachable!("handled above"),
    }
}

/// Compile a bundle folder through its registered kind. The codec instance is minted from the
/// returned factory right away (the filesystem loader has no deploy-plan stage to carry it
/// through, unlike the archive path — see [`crate::bundle::plan`]).
fn build_bundle_codec(
    kind: &'static BundleCodecKind,
    folder: &Path,
    codec_name: &str,
) -> Result<Arc<dyn PayloadCodec>, CodecLoadError> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    collect_tree(folder, "", &mut files)?;
    let refs: BTreeMap<String, &[u8]> = files
        .iter()
        .map(|(k, v)| (k.clone(), v.as_slice()))
        .collect();
    let source = BundleSource::new(codec_name, refs);
    let make = (kind.build)(&source).map_err(|e| {
        CodecLoadError::new(
            e.code,
            format!(
                "schema bundle '{codec_name}' ({}): {}",
                kind.kind, e.message
            ),
        )
    })?;
    Ok(make())
}

/// Every regular file under `dir`, keyed by its `'/'`-separated path relative to the bundle root.
fn collect_tree(
    dir: &Path,
    prefix: &str,
    out: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), CodecLoadError> {
    for entry in sorted_entries(dir)? {
        let name = entry
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let relative = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if entry.is_dir() {
            collect_tree(&entry, &relative, out)?;
        } else if entry.is_file() {
            out.insert(relative, read_all(&entry)?);
        }
    }
    Ok(())
}

fn build_xsd_codec(
    urn: &str,
    codec_name: &str,
    manifest: &CodecManifest,
    xsd_files: &[std::path::PathBuf],
    json_files: &[std::path::PathBuf],
) -> Result<Arc<dyn PayloadCodec>, CodecLoadError> {
    if !json_files.is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_LAYOUT_INVALID,
            format!(
                "codec '{codec_name}' declares schemaKind: xsd but the folder also contains JSON \
                 schema file(s) — one schema kind per codec"
            ),
        ));
    }
    if xsd_files.is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_LAYOUT_INVALID,
            format!("codec '{codec_name}' declares schemaKind: xsd but has no .xsd files"),
        ));
    }
    let bytes: Vec<Vec<u8>> = xsd_files.iter().map(read_all).collect::<Result<_, _>>()?;
    let refs: Vec<&[u8]> = bytes.iter().map(Vec::as_slice).collect();
    let formats: Vec<&str> = manifest.formats.iter().map(String::as_str).collect();
    let codec = StructuralCodec::compile_with_formats(urn, &refs, &formats).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_SCHEMA_INVALID,
            format!("invalid XSD in codec '{codec_name}': {}", sanitize(&e)),
        )
    })?;
    Ok(Arc::new(codec))
}

fn build_json_codec(
    urn: &str,
    codec_name: &str,
    xsd_files: &[std::path::PathBuf],
    json_files: &[std::path::PathBuf],
) -> Result<Arc<dyn PayloadCodec>, CodecLoadError> {
    if !xsd_files.is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_LAYOUT_INVALID,
            format!(
                "codec '{codec_name}' declares schemaKind: json-schema but the folder also contains \
                 .xsd file(s) — one schema kind per codec"
            ),
        ));
    }
    if json_files.is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_LAYOUT_INVALID,
            format!(
                "codec '{codec_name}' declares schemaKind: json-schema but has no .json schema files"
            ),
        ));
    }
    let mut schemas = Vec::new();
    for f in json_files {
        let type_name = strip_json_schema_extension(&f.file_name().unwrap().to_string_lossy());
        let bytes = read_all(f)?;
        let schema = JsonSchema::compile(&type_name, &bytes, Some(&type_name)).map_err(|e| {
            CodecLoadError::new(
                codes::CONFIG_SCHEMA_INVALID,
                format!(
                    "invalid JSON schema in codec '{codec_name}': {}",
                    sanitize(&e)
                ),
            )
        })?;
        schemas.push(schema);
    }
    let codec = JsonSchemaCodec::of(urn, schemas)
        .map_err(|e| CodecLoadError::new(codes::CONFIG_CODEC_LAYOUT_INVALID, sanitize(&e)))?;
    Ok(Arc::new(codec))
}

// ---- codec-manifest.yaml ---------------------------------------------------------------------

/// What a `codec-manifest.yaml` declares: one of the two generic schema kinds, or a bundle kind
/// a codec crate has registered.
#[derive(Clone, Copy)]
enum ManifestKind {
    Schema(SchemaKind),
    Bundle(&'static BundleCodecKind),
}

struct CodecManifest {
    kind: ManifestKind,
    formats: Vec<String>,
}

fn parse_manifest(manifest_path: &Path, codec_name: &str) -> Result<CodecManifest, CodecLoadError> {
    let text = fs::read_to_string(manifest_path).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': cannot read {CODEC_MANIFEST_FILE}: {e}"),
        )
    })?;
    let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!(
                "codec '{codec_name}': malformed {CODEC_MANIFEST_FILE} — {}",
                sanitize(&e.to_string())
            ),
        )
    })?;
    let serde_yaml_ng::Value::Mapping(map) = &value else {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': {CODEC_MANIFEST_FILE} is not a YAML mapping"),
        ));
    };
    let kind = parse_kind(map.get("schemaKind"), codec_name)?;
    let formats = parse_formats(map.get("formats"), kind, codec_name)?;
    Ok(CodecManifest { kind, formats })
}

fn parse_kind(
    raw: Option<&serde_yaml_ng::Value>,
    codec_name: &str,
) -> Result<ManifestKind, CodecLoadError> {
    let text = raw
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();
    match text.as_str() {
        "xsd" => Ok(ManifestKind::Schema(SchemaKind::Xsd)),
        "json-schema" | "jsonschema" | "json_schema" => {
            Ok(ManifestKind::Schema(SchemaKind::JsonSchema))
        }
        // Anything else is a bundle kind — served if a codec crate registered it, a fail-closed
        // manifest error naming the served set if not.
        other => match crate::bundle::classify(other, codec_name)? {
            Some(kind) => Ok(ManifestKind::Bundle(kind)),
            None => unreachable!("the generic kinds are matched above"),
        },
    }
}

/// The wire formats a GENERIC schema codec accepts. A bundle declares its own content types in
/// code, so its manifest carries no `formats` key.
fn parse_formats(
    raw: Option<&serde_yaml_ng::Value>,
    kind: ManifestKind,
    codec_name: &str,
) -> Result<Vec<String>, CodecLoadError> {
    let kind = match kind {
        ManifestKind::Schema(kind) => kind,
        ManifestKind::Bundle(_) => return Ok(Vec::new()),
    };
    let Some(serde_yaml_ng::Value::Sequence(list)) = raw else {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': formats must be a non-empty list"),
        ));
    };
    if list.is_empty() {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': formats must be a non-empty list"),
        ));
    }
    let allowed = match kind {
        SchemaKind::Xsd => XSD_FORMATS,
        SchemaKind::JsonSchema => JSON_FORMATS,
    };
    let mut formats = Vec::new();
    for item in list {
        let f = item
            .as_str()
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_default();
        if !allowed.contains(&f.as_str()) {
            return Err(CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!(
                    "codec '{codec_name}': format '{f}' is not allowed for this schemaKind (allowed: {allowed:?})"
                ),
            ));
        }
        if !formats.contains(&f) {
            formats.push(f);
        }
    }
    Ok(formats)
}

// ---- helpers ---------------------------------------------------------------------------------

fn sorted_entries(dir: &Path) -> Result<Vec<std::path::PathBuf>, CodecLoadError> {
    let mut entries: Vec<std::path::PathBuf> = fs::read_dir(dir)
        .map_err(|e| {
            CodecLoadError::new(
                codes::CONFIG_CODEC_LAYOUT_INVALID,
                format!("cannot list schemas/ at {}: {e}", dir.display()),
            )
        })?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();
    Ok(entries)
}

/// Regular files directly under `dir` whose lower-cased name ends with `ext`, sorted.
fn files_with_extension(dir: &Path, ext: &str) -> Result<Vec<std::path::PathBuf>, CodecLoadError> {
    let mut files: Vec<std::path::PathBuf> = sorted_entries(dir)?
        .into_iter()
        .filter(|p| p.is_file())
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().to_lowercase().ends_with(ext))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    Ok(files)
}

fn read_all(path: &std::path::PathBuf) -> Result<Vec<u8>, CodecLoadError> {
    fs::read(path).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_SCHEMA_INVALID,
            format!("cannot read schema {}: {e}", path.display()),
        )
    })
}

/// `settlement.schema.json` → `settlement`; `payment.json` → `payment`.
fn strip_json_schema_extension(file_name: &str) -> String {
    let lower = file_name.to_lowercase();
    let ext = if lower.ends_with(JSON_SCHEMA_EXT) {
        JSON_SCHEMA_EXT
    } else {
        JSON_EXT
    };
    file_name[..file_name.len() - ext.len()].to_string()
}

fn sanitize(message: &str) -> String {
    message.replace(['\n', '\r'], " ")
}
