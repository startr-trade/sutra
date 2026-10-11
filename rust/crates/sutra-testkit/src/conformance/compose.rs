//! Build-time compose helper for multi-variant examples (the DRY-variants convention).
//! A multi-variant example keeps ONE
//! `shared/` folder (the common `bpmn/ rules/ templates/ …`) plus tiny per-variant
//! overlays under `variants/<full-package-dir-name>/` holding only the two files that
//! differ across variants — `channels.yaml` (transport) and `package.yaml` (labels).
//!
//! [`compose_variant`] materialises one variant into a complete, standalone package dir
//! (indistinguishable from the pre-refactor committed `deployments-src/<variant>/`) by
//! composing `shared/` as a base layer under `variants/<variant>/` into a fresh temp dir
//! named EXACTLY the variant dir name. The temp dir's own name matters: callers
//! (`sutra package`, `assemble_dir`) derive the archive key from the last path
//! component, so the composed dir must carry the same name the committed package dir
//! used to.
//!
//! The composition itself is `sutra_loader::overlay` — the same code path `sutra package
//! --overlay` runs, so the harness proves the shipped composition rather than a second,
//! test-only copy of it. This helper adds only what the harness needs on top: the
//! world-readable perms the unprivileged engine container requires, and a composed dir
//! that OUTLIVES the call (the loader's own guard would remove it on drop).

use std::path::{Path, PathBuf};

use super::util;

/// Compose `<example_dir>/shared/` + `<example_dir>/variants/<variant_dir_name>/` into a
/// fresh temp dir named `<variant_dir_name>`. Returns the composed package dir, ready for
/// `sutra_loader::package::assemble_dir` or the `sutra package` CLI.
///
/// Panics with the loader's own refusal when the layers cannot be composed (an overlay
/// carrying `package.yaml`, a symlink inside a layer, a missing folder) — in a fixture tree
/// that is a build fault, not a condition to recover from.
pub fn compose_variant(example_dir: &Path, variant_dir_name: &str) -> PathBuf {
    let variant = example_dir.join("variants").join(variant_dir_name);
    let layers = [example_dir.join("shared")];
    let composed = sutra_loader::overlay::compose_overlays(&variant, &layers)
        .unwrap_or_else(|e| panic!("compose variant {}: {e}", variant.display()))
        .into_path();
    // The whole temp tree, not just the composed dir: the engine container runs
    // unprivileged and must traverse the parent to reach it.
    util::make_world_readable(composed.parent().unwrap_or(&composed));
    composed
}
