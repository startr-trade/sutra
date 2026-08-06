//! `sutra-schema-gen` — the schema generator: parses a directory of message-definition
//! XSDs written in the Standards-Editor idiom and emits the Rust binding sources for them
//! (model, lenient decoder, canonical map projection, shape metadata, cross-schema registry/lib
//! layout).
//!
//! The tool is DOMAIN-NEUTRAL: it takes arbitrary corpus and output paths and names no message
//! standard. The one thing it does know is the input FORMAT — schemas whose target namespace is
//! `<registry-urn-prefix><message-type>`, from which it derives the module name and the
//! `MESSAGE_TYPE` constant (see [`emit::ISO_NS_PREFIX`]). Whichever distribution owns a corpus
//! owns the generated crate too; this one owns neither.
//!
//! The crate is a **library only** — the shipped entry point is the `sutra` CLI:
//!
//! ```text
//! sutra schemagen generate <schemas_dir> <out_dir> [--full]   # (re)generate sources
//! sutra schemagen check    <schemas_dir> <tree_dir> [--full]  # drift gate: exit 1 on diff
//! ```
//!
//! Default = the **slim** data-driven decode tables (the form a generated binding crate normally
//! commits); `--full` additionally emits the typed model (opt-in). `check` regenerates in
//! memory, formats with `rustfmt`, and diffs against the committed tree ([`check_tree`]);
//! `support.rs`, `Cargo.toml` and the crate's other hand-maintained files are never written or
//! compared.
//!
//! Regeneration is **byte-identical** to the committed sources after `cargo fmt`, with two
//! sanctioned differences from the upstream emitter this backend was ported from: a neutral
//! `@generated` header with generated body comments phrased against the contracts, and the
//! enum accessor named `canonical_name()`.
//!
//! Vocabulary discipline: the crate and its output name the contracts, never foreign class
//! names. The generator carries its own document-order XSD reader
//! ([`xml`]/[`parse`]) rather than sutra-xsd's compiled model, because the validation
//! model is deliberately lossy for codegen — its type table is a `BTreeMap` (alphabetical,
//! discarding XSD document order) and its simple-type chains are flattened. Byte-identical
//! codegen needs document order and the generated surface's canonical scalar mapping, so
//! this focused reader is built over the shared `quick-xml` dependency and the same
//! schema corpus.

use std::path::Path;

pub mod emit;
pub mod model;
pub mod names;
pub mod parse;
pub mod xml;

pub use emit::Mode;

/// One generated source file: its base name (e.g. `order001v01.rs`) and raw content
/// (pre-`rustfmt`).
#[derive(Debug, Clone)]
pub struct GeneratedFile {
    pub name: String,
    pub content: String,
}

/// Generate every source file for the XSD corpus in `schemas_dir`: one module per
/// `<name>.xsd`, plus `registry.rs` and `lib.rs`. Output is raw (unformatted); callers run
/// `rustfmt`/`cargo fmt` to reach the committed canonical form. `support.rs` is
/// hand-maintained and never emitted here.
///
/// Zero configuration by design: the XSD files are the only input. Each module name
/// derives from the schema's own `targetNamespace` message identifier
/// ([`names::module_from_message_type`]), the family feature gates derive from the module
/// names, and nothing is skipped or renamed — so there is no config file to read.
pub fn generate_all(schemas_dir: &Path) -> Result<Vec<GeneratedFile>, String> {
    generate_all_with_mode(schemas_dir, Mode::Slim)
}

/// [`generate_all`] in an explicit [`Mode`]: `Mode::Slim` (the committed default) emits
/// the data-driven decode tables; `Mode::Full` emits the full typed model as well.
pub fn generate_all_with_mode(
    schemas_dir: &Path,
    mode: Mode,
) -> Result<Vec<GeneratedFile>, String> {
    let mut xsd_paths: Vec<_> = std::fs::read_dir(schemas_dir)
        .map_err(|e| format!("cannot read {}: {e}", schemas_dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "xsd"))
        .collect();
    xsd_paths.sort();

    if xsd_paths.is_empty() {
        return Err(format!("no .xsd files under {}", schemas_dir.display()));
    }

    let mut generator = emit::Generator::with_mode(mode);
    let mut files: Vec<GeneratedFile> = Vec::new();

    for xsd_path in &xsd_paths {
        let stem = xsd_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("bad xsd name: {}", xsd_path.display()))?;
        let xsd_bytes = std::fs::read(xsd_path)
            .map_err(|e| format!("cannot read {}: {e}", xsd_path.display()))?;
        let result = parse::parse_xsd(&xsd_bytes).map_err(|e| format!("parse {stem}.xsd: {e}"))?;

        let namespace = result.target_namespace.as_deref().unwrap_or_default();
        let message_type = namespace.strip_prefix(emit::ISO_NS_PREFIX).ok_or_else(|| {
            format!(
                "{stem}.xsd: targetNamespace '{namespace}' is not ISO 20022-shaped; \
                     cannot derive a module name"
            )
        })?;
        let module = names::module_from_message_type(message_type)
            .map_err(|e| format!("{stem}.xsd: {e}"))?;

        let content = generator.generate(&result, &module);
        files.push(GeneratedFile {
            name: format!("{module}.rs"),
            content,
        });
    }

    files.push(GeneratedFile {
        name: "registry.rs".to_string(),
        content: generator.registry(),
    });
    files.push(GeneratedFile {
        name: "lib.rs".to_string(),
        content: generator.lib(),
    });

    Ok(files)
}

/// Format Rust source through `rustfmt` (stdin → stdout) at the workspace edition (2021).
///
/// Using stdin (rather than a path) is deliberate: rustfmt then has no directory context,
/// so it never tries to resolve `mod` children — `lib.rs` formats (and reorders its module
/// declarations, matching `cargo fmt`) without `support.rs` or the per-schema files
/// present. The committed sources are `cargo fmt`-canonical; this reproduces that byte for
/// byte (verified by the `--check` gate and the golden test).
pub fn rustfmt(content: &str) -> Result<String, String> {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let mut child = Command::new("rustfmt")
        .args(["--edition", "2021", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn rustfmt: {e}"))?;
    child
        .stdin
        .take()
        .expect("rustfmt stdin piped")
        .write_all(content.as_bytes())
        .map_err(|e| format!("write rustfmt stdin: {e}"))?;
    let output = child
        .wait_with_output()
        .map_err(|e| format!("wait rustfmt: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "rustfmt failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("rustfmt output not utf-8: {e}"))
}

/// Outcome of [`check_tree`]: how many generated files were compared, and which ones disagree
/// with the committed tree (empty = in sync).
#[derive(Debug, Clone)]
pub struct CheckReport {
    pub checked: usize,
    pub drift: Vec<String>,
}

/// [`generate_all_with_mode`] followed by [`rustfmt`] on every file — `(name, content)` pairs in
/// the committed canonical form, the shape both `generate` and `check` compare/write.
pub fn generate_formatted(schemas_dir: &Path, mode: Mode) -> Result<Vec<(String, String)>, String> {
    generate_all_with_mode(schemas_dir, mode)?
        .iter()
        .map(|f: &GeneratedFile| Ok((f.name.clone(), rustfmt(&f.content)?)))
        .collect()
}

/// Regenerate into `out_dir` (created if absent) and return the written file names.
/// `support.rs`, `Cargo.toml` and the crate's other hand-maintained files are never emitted, so
/// they are never written over.
pub fn generate_into(
    schemas_dir: &Path,
    out_dir: &Path,
    mode: Mode,
) -> Result<Vec<String>, String> {
    let formatted = generate_formatted(schemas_dir, mode)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|e| format!("cannot create {}: {e}", out_dir.display()))?;
    for (name, content) in &formatted {
        let path = out_dir.join(name);
        std::fs::write(&path, content)
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    }
    Ok(formatted.into_iter().map(|(name, _)| name).collect())
}

/// Drift gate: regenerate in memory, format, and diff against the committed tree in `tree_dir`
/// without touching it.
pub fn check_tree(schemas_dir: &Path, tree_dir: &Path, mode: Mode) -> Result<CheckReport, String> {
    let formatted = generate_formatted(schemas_dir, mode)?;
    let mut drift: Vec<String> = Vec::new();
    for (name, content) in &formatted {
        let tree_path = tree_dir.join(name);
        match std::fs::read_to_string(&tree_path) {
            Ok(committed) if &committed == content => {}
            Ok(_) => drift.push(format!("drift: {name}")),
            Err(_) => drift.push(format!("missing in tree: {name}")),
        }
    }
    Ok(CheckReport {
        checked: formatted.len(),
        drift,
    })
}
