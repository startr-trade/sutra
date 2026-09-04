//! Authored-artifact documentation generator (library).
//!
//! The crate is a **library only** — the shipped entry point is the `sutra` CLI:
//!
//! ```text
//! sutra generate docs --input <folder> [--output <dir>] [--check]
//! ```
//!
//! Recurses a folder of user-authored deployment artifacts — BPMN processes, DMN/SRL rules,
//! Handlebars/XSLT templates + their manifests, `channels.yaml`, `package.yaml`, and any other
//! authoring YAML — and emits one deterministic markdown page per artifact, grouped under one
//! index page per deployment-package directory (a directory containing `package.yaml`).
//!
//! Parsing reuses the engine's OWN loaders ([`sutra_bpmn::BpmnModelLoader`],
//! [`sutra_dmn::DmnFileLoader`], [`sutra_srl::parse`]) so the generated docs describe exactly
//! what the engine loads.
//! `channels.yaml` / `package.yaml` / any other loose YAML are rendered generically (see
//! [`render::yaml_table`]) rather than via the engine's typed config structs, which live in
//! crates (`sutra-loader`, `sutra-channels`) that pull in the whole engine dependency graph —
//! architecturally wrong for a CLI whose acceptance bar is "works standalone on an arbitrary
//! folder with no deployment structure at all".
//!
//! This is a SEPARATE crate from `sutra-catalog-gen` (which documents Rust *source*, not
//! authored artifacts) — see that crate's docs for the sibling generator's house style, which
//! this one mirrors (header/manual-notes sentinel, deterministic `--check` diff).

#![forbid(unsafe_code)]

pub mod discover;
pub mod manifest;
pub mod model;
pub mod render;
pub mod util;

use std::path::PathBuf;

use anyhow::{Context, Result};

/// The output path used when `--output` is not given, relative to the process's current
/// directory.
pub const DEFAULT_OUTPUT: &str = "catalog";

/// Where to read authored artifacts from and where to write the catalog.
pub struct Config {
    /// The folder to recurse (required — no default; scanned recursively).
    pub input: PathBuf,
    /// Catalog output root.
    pub output: PathBuf,
}

impl Config {
    pub fn new(input: PathBuf, output: Option<PathBuf>) -> Self {
        let output = output.unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT));
        Config { input, output }
    }
}

/// Outcome of a generation run.
pub struct Report {
    pub pages: usize,
    pub packages: usize,
}

/// Regenerate the catalog in place under `cfg.output`, preserving manual notes.
pub fn run(cfg: &Config) -> Result<Report> {
    let tree = discover::discover(&cfg.input)?;
    let pages = render::generate_pages(&tree);
    std::fs::create_dir_all(&cfg.output)
        .with_context(|| format!("creating output dir {}", cfg.output.display()))?;
    let n = render::write_all(&cfg.output, &pages)?;
    Ok(Report {
        pages: n,
        packages: tree.packages.len(),
    })
}

/// Generate into a temp dir and diff against the committed tree. Returns the drifted page paths
/// (empty = in sync). Mirrors `sutra-catalog-gen`'s `--check` discipline.
pub fn check(cfg: &Config) -> Result<Vec<String>> {
    let tree = discover::discover(&cfg.input)?;
    let pages = render::generate_pages(&tree);

    let tmp = tempfile::Builder::new()
        .prefix("sutra-docgen-check-")
        .tempdir()
        .context("creating temp dir for --check")?;
    render::write_raw(tmp.path(), &pages)?;
    render::diff_against(tmp.path(), &cfg.output, &pages)
}
