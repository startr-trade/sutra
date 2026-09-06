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

/// The wire formats an XSD codec accepts. Bounded by what `StructuralCodec` can turn into
/// canonical XML: the three syntaxes its `detect` knows, plus `csv` (validated row-wise — see
/// validated row-wise against the declared root).
const XSD_FORMATS: &[&str] = &["xml", "json", "yaml", "csv", "fixed-width"];

/// The wire formats a JSON-schema codec accepts. Per the two-kinds ruling
///, JSON-schema is the contract for every NON-XML tree — json,
/// yaml, and the parsed tree of csv — while xml belongs to XSD, whose type system JSON-schema
/// cannot express. So `xml` is deliberately absent here, not an oversight.
const JSON_FORMATS: &[&str] = &["json", "yaml", "csv", "fixed-width"];

/// The per-format parser config a manifest may carry. Layout is parser config, NOT schema
/// — a schema cannot express byte offsets — so it has no home in the schema file and lives here.
/// Every field is optional: a header-bearing, comma-delimited CSV names its own columns and
/// needs no block at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FormatLayout {
    csv_delimiter: char,
    csv_header: bool,
    /// The `fixed-width:` column layout. Empty means undeclared — and unlike csv's defaults that
    /// is FATAL for a codec that declares the format, because a fixed-width record carries no
    /// structure of its own to fall back on.
    fixed_width_fields: Vec<sutra_formats::FixedWidthField>,
}

impl Default for FormatLayout {
    fn default() -> FormatLayout {
        FormatLayout {
            csv_delimiter: ',',
            csv_header: true,
            fixed_width_fields: Vec::new(),
        }
    }
}

/// Build the `MessageFormat` a codec parses with. `csv` is config-bearing, so it is constructed
/// from the manifest's layout rather than the registry's zero-config default; every other
/// built-in comes from the registry, wrapped by the generic `PayloadCodecFormat` adapter so a
/// format added later is bindable without touching this function.
///
/// An `Opaque`-shaped format (`raw-text` / `raw-bytes`) is refused: there is no map under raw
/// bytes for a schema to type, so binding one would assert nothing.
fn message_format(
    name: &str,
    layout: &FormatLayout,
    codec_name: &str,
) -> Result<Arc<dyn sutra_codec_spi::MessageFormat>, CodecLoadError> {
    if name == sutra_formats::FixedWidthCodec::NAME {
        // No zero-config default: without the widths a line is an undifferentiated string, so an
        // undeclared layout is a manifest error rather than a guess.
        if layout.fixed_width_fields.is_empty() {
            return Err(CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!(
                    "codec '{codec_name}': format 'fixed-width' requires a 'fixed-width:' block \
                     declaring its column layout — a fixed-width record carries no structure of \
                     its own"
                ),
            ));
        }
        let codec = sutra_formats::FixedWidthCodec::new(layout.fixed_width_fields.clone())
            .map_err(|e| {
                CodecLoadError::new(
                    codes::CONFIG_CODEC_MANIFEST_INVALID,
                    format!("codec '{codec_name}': {e}"),
                )
            })?;
        return Ok(Arc::new(sutra_codec_spi::PayloadCodecFormat::new(
            Arc::new(codec),
        )));
    }
    if name == sutra_formats::CsvCodec::NAME {
        return Ok(Arc::new(sutra_codec_spi::PayloadCodecFormat::new(
            Arc::new(sutra_formats::CsvCodec::new(
                layout.csv_delimiter,
                layout.csv_header,
            )),
        )));
    }
    let builtin = sutra_codec_spi::builtin_formats()
        .into_iter()
        .find(|f| f.name == name)
        .ok_or_else(|| {
            CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!("codec '{codec_name}': format '{name}' is not a built-in format"),
            )
        })?;
    if builtin.shape_class == sutra_codec_spi::ShapeClass::Opaque {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!(
                "codec '{codec_name}': format '{name}' is opaque (raw bytes/text) and carries no                  structure for a schema to validate — bind it bare on the channel instead"
            ),
        ));
    }
    Ok(Arc::new(sutra_codec_spi::PayloadCodecFormat::new(
        builtin.codec,
    )))
}

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
            build_json_codec(&urn, &codec_name, &manifest, &xsd_files, &json_files)
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
    let (batch, fixed_columns) = tabular_parser(&formats, &manifest.layout, codec_name)?;
    // A fixed-width layout's columns ARE the record's only field names, so `compile_with_layout`
    // checks them against the bound type; a mismatch would otherwise fail every row at runtime
    // for what is really a configuration mistake.
    let columns: Vec<&str> = fixed_columns.iter().map(String::as_str).collect();
    let codec = StructuralCodec::compile_with_layout(urn, &refs, &formats, batch, &columns)
        .map_err(|e| match e {
            // A layout that disagrees with the schema is a MANIFEST fault, not a schema one —
            // the XSD is fine; the columns declared against it are not.
            crate::structural::LayoutCompileError::Layout(message) => CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!("codec '{codec_name}': {}", sanitize(&message)),
            ),
            crate::structural::LayoutCompileError::Schema(message) => CodecLoadError::new(
                codes::CONFIG_SCHEMA_INVALID,
                format!(
                    "invalid XSD in codec '{codec_name}': {}",
                    sanitize(&message)
                ),
            ),
        })?;
    Ok(Arc::new(codec))
}

/// The tabular parsers a codec reads a BATCH with, and the column names DECLARED for them — a
/// fixed-width layout's fields, or empty for csv (whose columns arrive in the header at runtime).
type TabularParsers = (Vec<Arc<dyn PayloadCodec>>, Vec<String>);

/// The tabular parsers a codec reads a BATCH with, built from the manifest's layout blocks, plus
/// the declared column names when fixed-width is among them (empty otherwise).
///
/// BOTH may be declared. Their content types are disjoint — `text/csv` / `application/csv`
/// against `text/plain` / `application/x-fixed-width` — so an inbound body selects its parser
/// unambiguously, and one schema can serve a CSV feed and a fixed-width feed over the same
/// channel. Declaration order decides only the no-content-type fallback.
fn tabular_parser(
    formats: &[&str],
    layout: &FormatLayout,
    codec_name: &str,
) -> Result<TabularParsers, CodecLoadError> {
    let tabular: Vec<&str> = formats
        .iter()
        .copied()
        .filter(|f| {
            *f == sutra_formats::CsvCodec::NAME || *f == sutra_formats::FixedWidthCodec::NAME
        })
        .collect();
    let mut parsers: Vec<Arc<dyn PayloadCodec>> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    for name in tabular {
        if name == sutra_formats::FixedWidthCodec::NAME {
            if layout.fixed_width_fields.is_empty() {
                return Err(CodecLoadError::new(
                    codes::CONFIG_CODEC_MANIFEST_INVALID,
                    format!(
                        "codec '{codec_name}': format 'fixed-width' requires a 'fixed-width:' \
                         block declaring its column layout — a fixed-width record carries no \
                         structure of its own"
                    ),
                ));
            }
            columns = layout
                .fixed_width_fields
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            let parser = sutra_formats::FixedWidthCodec::new(layout.fixed_width_fields.clone())
                .map_err(|e| {
                    CodecLoadError::new(
                        codes::CONFIG_CODEC_MANIFEST_INVALID,
                        format!("codec '{codec_name}': {e}"),
                    )
                })?;
            parsers.push(Arc::new(parser));
        } else {
            parsers.push(Arc::new(sutra_formats::CsvCodec::new(
                layout.csv_delimiter,
                layout.csv_header,
            )));
        }
    }
    Ok((parsers, columns))
}

fn build_json_codec(
    urn: &str,
    codec_name: &str,
    manifest: &CodecManifest,
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
    // The manifest's declared formats become the parsers this codec negotiates between; the
    // schema validates the tree they produce, whatever syntax it arrived in.
    let mut formats = Vec::new();
    for name in &manifest.formats {
        formats.push(message_format(name, &manifest.layout, codec_name)?);
    }
    let codec = JsonSchemaCodec::with_formats(urn, formats, schemas)
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
    layout: FormatLayout,
}

fn parse_manifest(manifest_path: &Path, codec_name: &str) -> Result<CodecManifest, CodecLoadError> {
    let text = fs::read_to_string(manifest_path).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': cannot read {CODEC_MANIFEST_FILE}: {e}"),
        )
    })?;
    parse_manifest_text(&text, codec_name)
}

/// Compile ONE module XSD codec from bytes — the seam the engine assembly builds through, so a
/// running engine honours the same `formats:` / layout declaration `sutra package` validated.
///
/// Without this the assembly had to assume a format set, and did: it hardcoded
/// `["xml","json","yaml"]`, which made a `formats: [csv]` declaration inert at runtime — the
/// package would seal cleanly and then reject every upload as an unsupported content-type.
///
/// `manifest` absent (or unparseable) falls back to that historical assumption rather than
/// refusing to serve: a deployment that used to load still loads, with the narrower guarantee.
pub fn compile_module_codec(
    urn: &str,
    codec_name: &str,
    manifest: Option<&str>,
    xsds: &[&[u8]],
) -> Result<Arc<dyn PayloadCodec>, CodecLoadError> {
    let Some(text) = manifest else {
        // No manifest at all: the conventional codec, nothing declared to honour.
        let codec = StructuralCodec::compile_with_formats(urn, xsds, &["xml", "json", "yaml"])
            .unwrap_or_else(|_| StructuralCodec::compile(urn, xsds));
        return Ok(Arc::new(codec));
    };
    // A manifest that does not parse is PROPAGATED, not swallowed. It was briefly treated as
    // "absent" and fell back to the historical format set — which meant an unknown format or a
    // malformed block passed `sutra lint` in silence and then decoded nothing at runtime. There
    // is no other owner of this check: lint's codec passes read the XSDs and never open the
    // manifest.
    let manifest = parse_manifest_text(text, codec_name)?;
    if !matches!(manifest.kind, ManifestKind::Schema(SchemaKind::Xsd)) {
        let codec = StructuralCodec::compile_with_formats(urn, xsds, &["xml", "json", "yaml"])
            .unwrap_or_else(|_| StructuralCodec::compile(urn, xsds));
        return Ok(Arc::new(codec));
    }
    let formats: Vec<&str> = manifest.formats.iter().map(String::as_str).collect();
    let (batch, fixed_columns) = tabular_parser(&formats, &manifest.layout, codec_name)?;
    let columns: Vec<&str> = fixed_columns.iter().map(String::as_str).collect();
    match StructuralCodec::compile_with_layout(urn, xsds, &formats, batch, &columns) {
        Ok(codec) => Ok(Arc::new(codec)),
        // A declared layout that disagrees with the schema is a CONFIGURATION fault: refuse it,
        // loudly, here — falling back would hide it and then fail every row of every upload.
        Err(crate::structural::LayoutCompileError::Layout(message)) => Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': {message}"),
        )),
        // The XSD set is outside the supported subset: serve the shape-only build, as before —
        // a deployment that used to load still loads, with a narrower guarantee.
        Err(crate::structural::LayoutCompileError::Schema(_)) => {
            Ok(Arc::new(StructuralCodec::compile(urn, xsds)))
        }
    }
}

fn parse_manifest_text(text: &str, codec_name: &str) -> Result<CodecManifest, CodecLoadError> {
    let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(text).map_err(|e| {
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
    let layout = parse_layout(map, codec_name)?;
    Ok(CodecManifest {
        kind,
        formats,
        layout,
    })
}

/// The per-format parser-config blocks a manifest may carry: `csv:` (`delimiter` / `header`,
/// both defaulting) and `fixed-width:` (`fields`, required when that format is declared).
fn parse_layout(
    map: &serde_yaml_ng::Mapping,
    codec_name: &str,
) -> Result<FormatLayout, CodecLoadError> {
    let mut layout = parse_csv_layout(map.get(serde_yaml_ng::Value::from("csv")), codec_name)?;
    layout.fixed_width_fields = parse_fixed_width_fields(
        map.get(serde_yaml_ng::Value::from("fixed-width")),
        codec_name,
    )?;
    Ok(layout)
}

/// The `fixed-width:` block: `fields:` is a list of `{name, width}` mappings, in wire order.
fn parse_fixed_width_fields(
    raw: Option<&serde_yaml_ng::Value>,
    codec_name: &str,
) -> Result<Vec<sutra_formats::FixedWidthField>, CodecLoadError> {
    let Some(block) = raw else {
        return Ok(Vec::new());
    };
    let invalid = |detail: String| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': {detail}"),
        )
    };
    let serde_yaml_ng::Value::Mapping(block) = block else {
        return Err(invalid(
            "the 'fixed-width' block is not a YAML mapping".to_string(),
        ));
    };
    for key in block.keys() {
        let key = key.as_str().unwrap_or_default();
        if key != "fields" {
            return Err(invalid(format!(
                "unknown key '{key}' in the 'fixed-width' block (expected 'fields')"
            )));
        }
    }
    let Some(serde_yaml_ng::Value::Sequence(entries)) =
        block.get(serde_yaml_ng::Value::from("fields"))
    else {
        return Err(invalid(
            "the 'fixed-width' block needs a non-empty 'fields' list of {name, width}".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let serde_yaml_ng::Value::Mapping(entry) = entry else {
            return Err(invalid(
                "each 'fixed-width.fields' entry must be a {name, width} mapping".to_string(),
            ));
        };
        for key in entry.keys() {
            let key = key.as_str().unwrap_or_default();
            if !matches!(key, "name" | "width") {
                return Err(invalid(format!(
                    "unknown key '{key}' in a 'fixed-width.fields' entry (expected 'name' / 'width')"
                )));
            }
        }
        let name = entry
            .get(serde_yaml_ng::Value::from("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let width = entry
            .get(serde_yaml_ng::Value::from("width"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                invalid(format!(
                    "'fixed-width.fields' entry '{name}' needs a positive integer 'width'"
                ))
            })?;
        out.push(sutra_formats::FixedWidthField::new(name, width as usize).map_err(invalid)?);
    }
    if out.is_empty() {
        return Err(invalid(
            "the 'fixed-width' block needs a non-empty 'fields' list of {name, width}".to_string(),
        ));
    }
    Ok(out)
}

/// The optional `csv:` block — `delimiter` (one character) and `header` (bool). Absent, or with
/// either key absent, the defaults stand (comma, header row).
fn parse_csv_layout(
    raw: Option<&serde_yaml_ng::Value>,
    codec_name: &str,
) -> Result<FormatLayout, CodecLoadError> {
    let mut layout = FormatLayout::default();
    let Some(block) = raw else {
        return Ok(layout);
    };
    let serde_yaml_ng::Value::Mapping(map) = block else {
        return Err(CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{codec_name}': the 'csv' block is not a YAML mapping"),
        ));
    };
    for key in map.keys() {
        let key = key.as_str().unwrap_or_default();
        if !matches!(key, "delimiter" | "header") {
            return Err(CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!(
                    "codec '{codec_name}': unknown key '{key}' in the 'csv' block                      (expected 'delimiter' / 'header')"
                ),
            ));
        }
    }
    if let Some(raw) = map.get(serde_yaml_ng::Value::from("delimiter")) {
        let text = raw.as_str().unwrap_or_default();
        let mut chars = text.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => layout.csv_delimiter = c,
            _ => {
                return Err(CodecLoadError::new(
                    codes::CONFIG_CODEC_MANIFEST_INVALID,
                    format!(
                        "codec '{codec_name}': csv.delimiter must be exactly one character, got                          '{text}'"
                    ),
                ))
            }
        }
    }
    if let Some(raw) = map.get(serde_yaml_ng::Value::from("header")) {
        layout.csv_header = raw.as_bool().ok_or_else(|| {
            CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!("codec '{codec_name}': csv.header must be true or false"),
            )
        })?;
    }
    Ok(layout)
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
