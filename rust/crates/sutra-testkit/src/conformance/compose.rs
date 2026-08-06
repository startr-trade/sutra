//! Build-time compose helper for multi-variant examples (the DRY-variants convention).
//! A multi-variant example keeps ONE
//! `shared/` folder (the common `bpmn/ rules/ templates/ …`) plus tiny per-variant
//! overlays under `variants/<full-package-dir-name>/` holding only the two files that
//! differ across variants — `channels.yaml` (transport) and `package.yaml` (labels).
//!
//! [`compose_variant`] materialises one variant into a complete, standalone package dir
//! (indistinguishable from the pre-refactor committed `deployments-src/<variant>/`) by
//! copying `shared/*` into a fresh temp dir named EXACTLY the variant dir name, then
//! overlaying `variants/<variant>/*` on top. The temp dir's own name matters: callers
//! (`sutra package`, `assemble_dir`) derive the archive key from the last path
//! component, so the composed dir must carry the same name the committed package dir
//! used to.
//!
//! Build/test-time only — this does NOT change `sutra_loader`'s own semantics (no
//! runtime overlay, no new CLI verb); it is purely how the harness and this helper's
//! callers assemble a package dir before handing it to the unmodified loader/CLI.

use std::path::{Path, PathBuf};

use super::util;

/// Compose `<example_dir>/shared/` + `<example_dir>/variants/<variant_dir_name>/` into a
/// fresh temp dir named `<variant_dir_name>`. Returns the composed package dir, ready for
/// `sutra_loader::package::assemble_dir` or the `sutra package` CLI.
pub fn compose_variant(example_dir: &Path, variant_dir_name: &str) -> PathBuf {
    let parent = util::world_readable_temp_dir("sutra-compose");
    let composed = parent.join(variant_dir_name);
    util::copy_tree(&example_dir.join("shared"), &composed);
    util::copy_tree(
        &example_dir.join("variants").join(variant_dir_name),
        &composed,
    );
    composed
}
