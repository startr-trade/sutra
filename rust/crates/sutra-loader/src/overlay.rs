//! Layered package directories — the `--overlay` composition in front of the
//! [`crate::package`] scan.
//!
//! # The layout this serves
//!
//! A multi-variant authoring tree keeps ONE folder of common content plus one small folder
//! per variant carrying only what differs (typically `channels.yaml` + `package.yaml`):
//!
//! ```text
//! shared/              bpmn/ rules/ templates/ …   — the common content
//! variants/alpha/      package.yaml channels.yaml  — what makes alpha alpha
//! variants/bravo/      package.yaml channels.yaml
//! ```
//!
//! [`crate::package::assemble_dir`] takes exactly ONE self-contained directory, so without
//! this module the author has to copy `shared/` and the variant into a merged directory by
//! hand before packaging. The entry points here do that copy, once, in a temp directory, and
//! then run the UNCHANGED scan → validate → seal path over the result — so determinism,
//! fail-closed validation and content addressing behave exactly as they do for a
//! hand-merged directory. That equivalence is the whole contract; nothing in the sealed
//! archive records that it was composed.
//!
//! # The composition rules
//!
//! - Every overlay is a **base** layer, applied in the order given; the package directory is
//!   applied **last** and wins on any conflicting path. A later overlay wins over an earlier
//!   one. (The variant's own files beat the shared ones — which is the point of the layout.)
//! - The composed directory's NAME is exactly the package directory's name, because the
//!   archive is named `<package-dir-name>.sutra`.
//! - **Labels come from the package directory alone**: an overlay carrying `package.yaml` is
//!   refused, naming the file, rather than silently losing to the package directory's copy.
//! - **No deletions.** A layer can add a path or replace one; it can never remove a path a
//!   lower layer provides. A variant that must not carry some shared artifact does not belong
//!   in the shared layer.
//! - **Every override is reported** as a non-fatal
//!   [`codes::DEPLOY_PACKAGE_OVERLAY_OVERRIDE`] warning naming the relative path and the
//!   layer that won — an unintended shadow is otherwise invisible, and the composed tree is
//!   what gets sealed.
//! - **Symlinks are refused** anywhere inside a layer. The package scan itself does not
//!   treat them specially (it stats through them), and a composed archive that depends on
//!   what a link happens to point at on the packaging host is not reproducible, so
//!   composition rejects them instead of following them.
//!
//! Composition is a packaging-time convenience over authoring directories. It is NOT an
//! archive-level mechanism: a sealed archive still has no overlay, no inheritance and no
//! sharing — it is one flat, self-contained deployment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{codes, LoaderError};
use crate::lint::{LintDiagnostic, LintReport};
use crate::package::{
    assemble_dir, lint_dir, package_dir_name, PackageError, PackageOptions, PackageOutcome,
    PACKAGE_FILE_NAME,
};
use crate::scanner;

/// One composed package directory: a standalone, scan-ready mirror of the layers, plus the
/// override diagnostics the composition raised.
///
/// The directory lives in a temp tree that is removed when this value drops, so the usual
/// shape is to keep it alive for the length of the package/lint call. [`Self::into_path`]
/// hands the directory over instead, leaving its removal to the caller.
#[derive(Debug)]
pub struct ComposedPackage {
    pub(crate) root: PathBuf,
    /// The temp parent to remove on drop; `None` once [`Self::into_path`] has disowned it.
    pub(crate) temp_root: Option<PathBuf>,
    pub(crate) overrides: Vec<LintDiagnostic>,
}

impl ComposedPackage {
    /// The composed package directory — named exactly like the package directory it was
    /// composed for, ready for [`assemble_dir`] / [`lint_dir`].
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The non-fatal override warnings, in composition order (layer order, then relative
    /// path). One per shadowing event, so a path three layers supply reports twice.
    pub fn overrides(&self) -> &[LintDiagnostic] {
        &self.overrides
    }

    /// Give up ownership of the composed directory: it is NOT removed when this value
    /// drops, and the caller is responsible for it.
    pub fn into_path(mut self) -> PathBuf {
        self.temp_root = None;
        self.root.clone()
    }
}

impl Drop for ComposedPackage {
    fn drop(&mut self) {
        if let Some(temp_root) = &self.temp_root {
            let _ = std::fs::remove_dir_all(temp_root);
        }
    }
}

/// `sutra lint <dir> --overlay <dir>…` — compose the layers, then run the full fail-closed
/// suite over the composed tree. No overlays = [`lint_dir`] verbatim.
///
/// A composition failure folds into the report as its first ERROR, mirroring how a
/// scan-level failure folds into [`lint_dir`]'s.
pub fn lint_dir_with_overlays(package_dir: &Path, overlays: &[PathBuf]) -> LintReport {
    if overlays.is_empty() {
        return lint_dir(package_dir);
    }
    match compose_overlays(package_dir, overlays) {
        Err(PackageError::Validation(report)) => report,
        Err(PackageError::Io(e)) => LintReport {
            diagnostics: vec![LintDiagnostic::error(e.code, e.message)],
        },
        Ok(composed) => {
            let mut report = lint_dir(composed.path());
            prepend(&mut report.diagnostics, composed.overrides());
            report
        }
    }
}

/// `sutra package <dir> --overlay <dir>…` — compose the layers, then validate and seal the
/// composed tree into exactly ONE `.sutra` named `<package-dir-name>.sutra`. No overlays =
/// [`assemble_dir`] verbatim.
///
/// Byte-identical to sealing a hand-merged directory of the same content, and fail-closed
/// the same way (an ERROR emits nothing). An unreadable layer is an
/// [`PackageError::Io`] — the invocation is wrong, not the content.
pub fn assemble_dir_with_overlays(
    package_dir: &Path,
    overlays: &[PathBuf],
    out_dir: &Path,
    options: &PackageOptions,
) -> Result<PackageOutcome, PackageError> {
    if overlays.is_empty() {
        return assemble_dir(package_dir, out_dir, options);
    }
    let composed = compose_overlays(package_dir, overlays)?;
    match assemble_dir(composed.path(), out_dir, options) {
        Ok(mut outcome) => {
            prepend(&mut outcome.report.diagnostics, composed.overrides());
            Ok(outcome)
        }
        // The overrides belong in the refusal too: which layer supplied the offending file
        // is usually the first thing the author needs to know.
        Err(PackageError::Validation(mut report)) => {
            prepend(&mut report.diagnostics, composed.overrides());
            Err(PackageError::Validation(report))
        }
        Err(other) => Err(other),
    }
}

/// Copy `overlays` (in order, as base layers) and then `package_dir` (last, winning) into a
/// fresh temp directory named exactly like `package_dir`.
///
/// The composition rules — and why each one is fail-closed — are in the module docs.
pub fn compose_overlays(
    package_dir: &Path,
    overlays: &[PathBuf],
) -> Result<ComposedPackage, PackageError> {
    let mut rejections: Vec<LintDiagnostic> = Vec::new();

    // Scan every layer BEFORE writing anything: a rejected layer must leave no temp tree.
    let mut layers: Vec<Layer> = Vec::new();
    for overlay in overlays {
        layers.push(scan_layer(overlay, LayerRole::Overlay, &mut rejections)?);
    }
    layers.push(scan_layer(
        package_dir,
        LayerRole::PackageDir,
        &mut rejections,
    )?);

    check_kind_clashes(&layers, &mut rejections);
    if !rejections.is_empty() {
        return Err(PackageError::Validation(LintReport {
            diagnostics: rejections,
        }));
    }

    let temp_root = fresh_temp_root().map_err(PackageError::Io)?;
    let root = temp_root.join(package_dir_name(package_dir));
    // From here on the guard owns the temp tree, so every early return still cleans up.
    let mut composed = ComposedPackage {
        root,
        temp_root: Some(temp_root),
        overrides: Vec::new(),
    };
    create_dir(&composed.root).map_err(PackageError::Io)?;

    let mut winner: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, layer) in layers.iter().enumerate() {
        for relative in &layer.dirs {
            create_dir(&composed.root.join(relative)).map_err(PackageError::Io)?;
        }
        for (relative, source) in &layer.files {
            if let Some(previous) = winner.insert(relative.as_str(), index) {
                composed.overrides.push(LintDiagnostic::warning(
                    codes::DEPLOY_PACKAGE_OVERLAY_OVERRIDE,
                    format!(
                        "'{relative}' is provided by more than one layer: {} overrides the \
                         copy from {}",
                        layer.label(),
                        layers[previous].label()
                    ),
                ));
            }
            copy_file(source, &composed.root.join(relative)).map_err(PackageError::Io)?;
        }
    }
    Ok(composed)
}

// ---------------------------------------------------------------------------------------
// Layer scanning
// ---------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LayerRole {
    Overlay,
    PackageDir,
    /// A layer of a deployment-manifest stack ([`crate::layer_stack`]): `package.yaml` is
    /// generated from the manifest, so the layer may not carry one.
    Stack,
}

/// One scanned layer: its regular files and its directories, both keyed by the
/// `'/'`-separated path relative to the layer root (deterministic order).
pub(crate) struct Layer {
    pub(crate) root: PathBuf,
    pub(crate) role: LayerRole,
    pub(crate) files: BTreeMap<String, PathBuf>,
    pub(crate) dirs: BTreeSet<String>,
}

impl Layer {
    /// How a diagnostic names this layer — the path, said in terms of its role.
    fn label(&self) -> String {
        match self.role {
            LayerRole::Overlay => format!("overlay {}", self.root.display()),
            LayerRole::PackageDir => format!("package directory {}", self.root.display()),
            LayerRole::Stack => format!("layer {}", self.root.display()),
        }
    }
}

pub(crate) fn scan_layer(
    root: &Path,
    role: LayerRole,
    rejections: &mut Vec<LintDiagnostic>,
) -> Result<Layer, PackageError> {
    if !root.is_dir() {
        let what = match role {
            LayerRole::Overlay => "overlay directory",
            LayerRole::PackageDir => "package directory",
            LayerRole::Stack => "layer directory",
        };
        return Err(PackageError::Io(LoaderError::new(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            format!(
                "{what} does not exist or is not readable: {}",
                root.display()
            ),
        )));
    }

    let mut layer = Layer {
        root: root.to_path_buf(),
        role,
        files: BTreeMap::new(),
        dirs: BTreeSet::new(),
    };
    // Relative path → why it is refused, so the refusals come out in a stable order
    // regardless of the traversal's.
    let mut refused: BTreeMap<String, String> = BTreeMap::new();

    let mut pending = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = pending.pop() {
        let entries = scanner::read_dir_sorted(&dir).map_err(PackageError::Io)?;
        for entry in entries {
            let name = entry
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            // `symlink_metadata`, not `metadata`: the point is to SEE the link rather than
            // resolve it.
            let kind = std::fs::symlink_metadata(&entry)
                .map_err(|e| {
                    PackageError::Io(LoaderError::new(
                        codes::DEPLOY_PACKAGE_CONFIG_INVALID,
                        format!("failed to inspect {}: {e}", entry.display()),
                    ))
                })?
                .file_type();
            if kind.is_symlink() {
                refused.insert(
                    relative.clone(),
                    format!(
                        "'{relative}' in {} is a symbolic link — a composed package layer \
                         carries regular files and directories only, so that the archive \
                         does not depend on what a link resolves to on the packaging host",
                        layer.label()
                    ),
                );
            } else if kind.is_dir() {
                layer.dirs.insert(relative.clone());
                pending.push((entry, relative));
            } else if kind.is_file() {
                layer.files.insert(relative, entry);
            } else {
                refused.insert(
                    relative.clone(),
                    format!(
                        "'{relative}' in {} is neither a regular file nor a directory",
                        layer.label()
                    ),
                );
            }
        }
    }

    if role == LayerRole::Overlay && layer.files.contains_key(PACKAGE_FILE_NAME) {
        refused.insert(
            PACKAGE_FILE_NAME.to_string(),
            format!(
                "overlay {} carries {PACKAGE_FILE_NAME} ({}) — a layered package takes its \
                 metadata from the package directory alone, so an overlay may not declare \
                 labels, engine.minContract or entryProcesses",
                root.display(),
                root.join(PACKAGE_FILE_NAME).display()
            ),
        );
    }

    if role == LayerRole::Stack && layer.files.contains_key(PACKAGE_FILE_NAME) {
        refused.insert(
            PACKAGE_FILE_NAME.to_string(),
            format!(
                "layer {} carries {PACKAGE_FILE_NAME} ({}) — with a deployment manifest the \
                 package metadata is generated from the manifest's `package:` block, so no \
                 layer may carry {PACKAGE_FILE_NAME}",
                root.display(),
                root.join(PACKAGE_FILE_NAME).display()
            ),
        );
    }

    for message in refused.into_values() {
        rejections.push(LintDiagnostic::error(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            message,
        ));
    }
    Ok(layer)
}

/// A path that is a FILE in one layer and a DIRECTORY in another cannot be composed — the
/// winner would have to delete the loser's subtree, which layering does not do.
fn check_kind_clashes(layers: &[Layer], rejections: &mut Vec<LintDiagnostic>) {
    let mut clashes: BTreeMap<&str, (String, String)> = BTreeMap::new();
    for layer in layers {
        for relative in layer.files.keys() {
            for other in layers {
                if other.dirs.contains(relative) {
                    clashes.insert(relative.as_str(), (layer.label(), other.label()));
                }
            }
        }
    }
    for (relative, (file_layer, dir_layer)) in clashes {
        rejections.push(LintDiagnostic::error(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            format!(
                "'{relative}' is a file in {file_layer} but a directory in {dir_layer} — the \
                 layers cannot be composed (a layer never deletes what a lower one provides)"
            ),
        ));
    }
}

// ---------------------------------------------------------------------------------------
// Filesystem plumbing
// ---------------------------------------------------------------------------------------

/// A fresh, empty temp directory to compose inside. The composed package directory is a
/// CHILD of it, because its own name is load-bearing (it names the archive).
pub(crate) fn fresh_temp_root() -> Result<PathBuf, LoaderError> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or_default()
    );
    let root = std::env::temp_dir().join(format!("sutra-compose-{unique}"));
    let _ = std::fs::remove_dir_all(&root);
    create_dir(&root)?;
    Ok(root)
}

pub(crate) fn create_dir(dir: &Path) -> Result<(), LoaderError> {
    std::fs::create_dir_all(dir).map_err(|e| {
        LoaderError::new(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            format!("failed to create {}: {e}", dir.display()),
        )
    })
}

pub(crate) fn copy_file(source: &Path, target: &Path) -> Result<(), LoaderError> {
    if let Some(parent) = target.parent() {
        create_dir(parent)?;
    }
    std::fs::copy(source, target).map(|_| ()).map_err(|e| {
        LoaderError::new(
            codes::DEPLOY_PACKAGE_CONFIG_INVALID,
            format!(
                "failed to copy {} into the composed package: {e}",
                source.display()
            ),
        )
    })
}

pub(crate) fn prepend(diagnostics: &mut Vec<LintDiagnostic>, overrides: &[LintDiagnostic]) {
    if overrides.is_empty() {
        return;
    }
    diagnostics.splice(0..0, overrides.iter().cloned());
}
