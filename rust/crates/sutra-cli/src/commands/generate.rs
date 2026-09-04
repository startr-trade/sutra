//! `sutra generate` — the derived-artifact generators, under one verb.
//!
//! These three are siblings in the only way that matters to a caller: each recomputes output
//! that is **derived, not authored**, and each offers the same `--check` drift gate that
//! regenerates without writing. That contract is what groups them.
//!
//! It is also what separates them from [`super::create`], which scaffolds files that become
//! *yours* — every scaffold is headed "edit freely — this file is yours", and `create` needs
//! `--force` precisely because it must never silently overwrite your edits. Generated pages are
//! headed "Do not edit above the MANUAL NOTES sentinel", and `--check` fails the build if you
//! did. Same repository, opposite guarantee: two verbs, never one.
//!
//! This replaces the former top-level `docgen` / `catalog` / `schemagen` commands outright — no
//! aliases. Every invoker in this repository (the Makefile, the pre-commit hook) moved with it;
//! a repository that composes this one must move too.

use std::path::PathBuf;

use crate::commands::{catalog, docs, schemagen};
use crate::output::Io;
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct GenerateArgs {
    #[command(subcommand)]
    pub action: GenerateAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum GenerateAction {
    /// Markdown catalog for a folder of authored deployment artifacts — BPMN (with an
    /// auto-laid-out diagram), DMN/SRL rules, templates, channels.yaml, package.yaml.
    Docs(docs::DocsArgs),
    /// Artifact-documentation catalog for a Rust workspace — one page per source file, one per
    /// crate. Documents the engine's OWN source; a deployment package wants `docs` instead.
    Catalog(catalog::CatalogArgs),
    /// Rust binding sources for an XSD message corpus — the decode tables a rail codec is
    /// built from. Zero configuration: the `.xsd` files are the only input.
    #[command(name = "schema-handler")]
    SchemaHandler(SchemaHandlerArgs),
}

/// Flat form of the schema generator, harmonised with its siblings: one `--check` flag rather
/// than the older `generate` / `check` sub-verbs, which under this parent would have read
/// `sutra generate schema-handler generate …`.
#[derive(Debug, clap::Args)]
pub struct SchemaHandlerArgs {
    /// Directory of `.xsd` schemas — the generator's only input.
    pub schemas_dir: PathBuf,

    /// Directory the generated sources are written into — or, with `--check`, the committed
    /// tree a fresh generation is compared against.
    pub out_dir: PathBuf,

    /// Also emit the typed model (opt-in, not committed); default is the slim decode tables.
    #[arg(long)]
    pub full: bool,

    /// Regenerate in memory and report drift instead of writing (CI / pre-commit gate).
    #[arg(long)]
    pub check: bool,
}

pub fn execute(args: GenerateArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    match args.action {
        GenerateAction::Docs(a) => docs::execute(a, global, io),
        GenerateAction::Catalog(a) => catalog::execute(a, global, io),
        GenerateAction::SchemaHandler(a) => {
            let action = if a.check {
                schemagen::SchemagenAction::Check(schemagen::CheckArgs {
                    schemas_dir: a.schemas_dir,
                    tree_dir: a.out_dir,
                    full: a.full,
                })
            } else {
                schemagen::SchemagenAction::Generate(schemagen::GenerateArgs {
                    schemas_dir: a.schemas_dir,
                    out_dir: a.out_dir,
                    full: a.full,
                })
            };
            schemagen::execute(schemagen::SchemagenArgs { action }, global, io)
        }
    }
}
