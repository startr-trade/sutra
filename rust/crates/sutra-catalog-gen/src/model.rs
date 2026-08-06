//! Data model shared across parse / resolve / render.
//!
//! Everything here is plain data; the parser fills it, the resolver decorates the reference
//! graph, and the renderer walks it. Determinism is a model-level invariant: every collection
//! that reaches a page is sorted before it is written.

use std::collections::BTreeMap;
use std::path::PathBuf;

/// The whole Rust workspace as the catalog sees it.
pub struct Workspace {
    /// Repository root (the directory that contains `rust/`).
    pub repo_root: PathBuf,
    /// Member crates, sorted by crate name.
    pub crates: Vec<Crate>,
}

/// One workspace member crate.
pub struct Crate {
    /// Cargo package name, e.g. `sutra-loader`.
    pub name: String,
    /// Rust identifier form (`-` → `_`), e.g. `sutra_loader`.
    pub ident: String,
    /// Crate directory relative to the repo root, POSIX, e.g. `rust/crates/sutra-loader`.
    pub rel_dir: String,
    /// `[package].description` from `Cargo.toml`, if any.
    pub description: Option<String>,
    /// Workspace-member crate names this crate path-depends on (`[dependencies]` /
    /// `[dev-dependencies]` / `[build-dependencies]` `{ path = … }`), sorted, de-duplicated.
    pub path_deps: Vec<String>,
    /// Source files under `src/**`, sorted by relative path.
    pub files: Vec<SourceFile>,
    /// module-path (from the crate root) → source-file relative path (POSIX, e.g.
    /// `src/manifest.rs`). Built from `mod` declarations + the on-disk layout.
    pub module_tree: BTreeMap<Vec<String>, String>,
}

/// One `*.rs` source file.
pub struct SourceFile {
    /// Path relative to the crate directory, POSIX, e.g. `src/manifest.rs`.
    pub rel: String,
    /// Module path from the crate root, e.g. `["manifest"]`; empty for the crate root file.
    pub module_path: Vec<String>,
    /// True for `src/main.rs` and `src/bin/*.rs`.
    pub is_binary: bool,
    /// First paragraph of the `//!` module doc, whitespace-collapsed.
    pub module_doc: Option<String>,
    /// Public + private top-level items (inline-module items are flattened with a `mod::`
    /// display prefix), sorted by `(section order, name)`.
    pub items: Vec<Item>,
    /// Methods harvested from `impl` blocks, sorted by `(impl type, name)`.
    pub methods: Vec<Method>,
    /// `impl Trait for Type` records for the Relationships "Implements" row.
    pub trait_impls: Vec<TraitImpl>,
    /// Flattened `use` leaves (path-qualified — never prose), used for the reference graph.
    pub uses: Vec<UseLeaf>,
    /// `mod` declarations at the file's top level (name, inline?), for module-tree building.
    pub child_mods: Vec<ModDecl>,
}

/// One flattened `use` leaf, tagged with the inline-module path it appears under so that
/// `self`/`super` resolve against the right module (a `use super::*` inside `mod imp { … }`
/// must not resolve against the file's own module path).
pub struct UseLeaf {
    /// Inline-module path within the file where the `use` appears; empty at file top level.
    pub in_module: Vec<String>,
    /// The imported path's segments (rename ignored, glob → `*`).
    pub path: Vec<String>,
}

/// A `mod x;` / `mod x { … }` declaration.
#[derive(Clone)]
pub struct ModDecl {
    pub name: String,
    pub inline: bool,
    /// For inline modules, the nested `mod` declarations (recursively); empty otherwise.
    pub children: Vec<ModDecl>,
}

/// A top-level item in a source file.
pub struct Item {
    pub kind: ItemKind,
    /// Display name (inline-module members carry a `mod::` prefix).
    pub name: String,
    /// Rendered visibility: `pub`, `pub(crate)`, `pub(super)`, `pub(in …)`, or `` (private).
    pub vis: String,
    /// For functions, the tidied signature `name(args) -> Ret`; `None` otherwise.
    pub signature: Option<String>,
    /// First paragraph of the item doc.
    pub doc: Option<String>,
}

/// A method from an `impl` block.
pub struct Method {
    /// The `impl` target type (tidied), e.g. `ModuleManifest`.
    pub impl_ty: String,
    pub name: String,
    pub vis: String,
    pub signature: String,
    pub doc: Option<String>,
}

/// An `impl Trait for Type` record.
pub struct TraitImpl {
    pub trait_name: String,
    pub type_name: String,
}

/// Item categories, in the fixed order they are rendered.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ItemKind {
    Module,
    Struct,
    Enum,
    Union,
    Trait,
    TypeAlias,
    Function,
    Constant,
    Static,
    Macro,
}

impl ItemKind {
    /// Section heading (plural) under `## Items`.
    pub fn section_title(self) -> &'static str {
        match self {
            ItemKind::Module => "Modules",
            ItemKind::Struct => "Structs",
            ItemKind::Enum => "Enums",
            ItemKind::Union => "Unions",
            ItemKind::Trait => "Traits",
            ItemKind::TypeAlias => "Type aliases",
            ItemKind::Function => "Functions",
            ItemKind::Constant => "Constants",
            ItemKind::Static => "Statics",
            ItemKind::Macro => "Macros",
        }
    }
}
