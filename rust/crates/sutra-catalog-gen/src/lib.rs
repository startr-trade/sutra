//! Rust catalog generator (library).
//!
//! The crate is a **library only** — the shipped entry point is the `sutra` CLI:
//!
//! ```text
//! sutra catalog [--repo-root <path>] [--output <path>] [--check]
//! ```
//!
//! Emits the artifact-documentation page system for the Rust workspace under `rust/crates/**`:
//! one page per source file, one crate-index page per crate (its `Cargo.toml`), and a
//! workspace-root page. Parsing is `syn` on the **stable** toolchain — rustdoc-JSON is
//! nightly-only and deliberately unused. Output is deterministic (sorted, stable) so the
//! `--check` diff is byte-stable across runs. See the crate README for the full scope
//! (rendering BPMN diagrams is a follow-on, out of scope here).

#![forbid(unsafe_code)]

pub mod model;
pub mod parse;
pub mod render;
pub mod resolve;
pub mod util;
pub mod workspace;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Where to read from and where to write the catalog.
pub struct Config {
    /// Repository root — the directory that contains `rust/` and the docs tree.
    pub repo_root: PathBuf,
    /// Catalog output root, e.g. `<repo>/catalog`.
    pub output: PathBuf,
}

impl Config {
    /// Default output under a repo root.
    pub fn with_defaults(repo_root: PathBuf, output: Option<PathBuf>) -> Self {
        let output = output.unwrap_or_else(|| repo_root.join("catalog"));
        Config { repo_root, output }
    }

    /// Resolve the caller's optional inputs: an absent repo root falls back to the process's
    /// current directory, and the root is canonicalized (best-effort) so a relative `..` renders
    /// stable paths in the generated pages.
    pub fn resolve(repo_root: Option<PathBuf>, output: Option<PathBuf>) -> Result<Self> {
        let repo_root = match repo_root {
            Some(p) => p,
            None => std::env::current_dir().context("resolving the current directory")?,
        };
        let repo_root = repo_root.canonicalize().unwrap_or(repo_root);
        Ok(Config::with_defaults(repo_root, output))
    }
}

/// Outcome of a generation run.
pub struct Report {
    pub pages: usize,
    pub crates: usize,
}

/// Regenerate the Rust catalog in place under `cfg.output`, preserving manual notes.
pub fn run(cfg: &Config) -> Result<Report> {
    let ws = workspace::discover(&cfg.repo_root)?;
    let graph = resolve::build(&ws);
    let pages = render::generate_pages(&ws, &graph);
    std::fs::create_dir_all(&cfg.output)?;
    let n = render::write_all(&cfg.output, &pages)?;
    Ok(Report {
        pages: n,
        crates: ws.crates.len(),
    })
}

/// Generate into a temp dir and diff against the committed tree. Returns the drifted page paths
/// (empty = in sync). Mirrors the depcat-gen `--check` discipline.
pub fn check(cfg: &Config) -> Result<Vec<String>> {
    let ws = workspace::discover(&cfg.repo_root)?;
    let graph = resolve::build(&ws);
    let pages = render::generate_pages(&ws, &graph);

    let tmp = tempfile::Builder::new()
        .prefix("sutra-catalog-check-")
        .tempdir()
        .context("creating temp dir for --check")?;
    write_raw(tmp.path(), &pages)?;
    render::diff_against(tmp.path(), &cfg.output, &pages)
}

/// Write the raw generated content (default manual notes) into `root` — used only by `--check`.
fn write_raw(root: &Path, pages: &[render::Page]) -> Result<()> {
    for p in pages {
        let target = root.join(&p.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target, &p.content)?;
    }
    Ok(())
}
