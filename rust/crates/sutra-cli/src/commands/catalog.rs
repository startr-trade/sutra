//! `sutra generate catalog` — generate (or drift-check) the Rust artifact-documentation catalog: one page
//! per source file, one crate-index page per `Cargo.toml`, and the workspace-root page. Thin
//! binding over `sutra_catalog_gen`, which `syn`-parses the workspace on the stable
//! toolchain and emits deterministic (sorted, byte-stable) pages.
//!
//! Sibling of [`super::docs`]: this one documents Rust *source*, `docs` documents *authored
//! deployment artifacts*. Both preserve hand-written notes below the manual-notes sentinel.

use std::path::PathBuf;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra generate catalog` (the `SUTRA.CATALOG.*` family).
pub mod codes {
    pub const DRIFT: &str = "SUTRA.CATALOG.DRIFT";
    pub const FAILED: &str = "SUTRA.CATALOG.FAILED";
}

#[derive(Debug, clap::Args)]
pub struct CatalogArgs {
    /// Repository root — the directory containing `rust/` and the docs tree (default: the
    /// working directory).
    #[arg(long, value_name = "PATH")]
    pub repo_root: Option<PathBuf>,

    /// Catalog output directory (default: `<repo-root>/catalog`).
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Generate into a temp dir and report drift against the committed catalog; the working tree
    /// is never written (CI / pre-commit gate).
    #[arg(long)]
    pub check: bool,

    /// Also DELETE catalog pages whose source file no longer exists — a page stranded when its
    /// source was renamed or removed. `--check` reports them regardless; this removes them.
    #[arg(long)]
    pub clean: bool,
}

pub fn execute(args: CatalogArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "catalog: {msg}");
            return exit::USAGE;
        }
    };
    let cfg = match sutra_catalog_gen::Config::resolve(args.repo_root, args.output) {
        Ok(cfg) => cfg,
        Err(e) => return fail(&e, io),
    };
    if args.check {
        check(&cfg, format, io)
    } else {
        generate(&cfg, args.clean, format, io)
    }
}

fn generate(
    cfg: &sutra_catalog_gen::Config,
    clean: bool,
    format: ReportFormat,
    io: &mut Io<'_>,
) -> i32 {
    let report = match sutra_catalog_gen::run(cfg, clean) {
        Ok(report) => report,
        Err(e) => return fail(&e, io),
    };
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "generated {} page(s) across {} crate(s) under {}",
                report.pages,
                report.crates,
                cfg.output.display()
            );
            for removed in &report.removed {
                let _ = writeln!(io.out, "removed stranded page {removed}");
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "repoRoot": cfg.repo_root.display().to_string(),
                "output": cfg.output.display().to_string(),
                "pages": report.pages,
                "crates": report.crates,
                "removed": report.removed,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

fn check(cfg: &sutra_catalog_gen::Config, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let drift = match sutra_catalog_gen::check(cfg) {
        Ok(drift) => drift,
        Err(e) => return fail(&e, io),
    };
    match format {
        ReportFormat::Text => {
            // Each entry already carries its kind ("missing: <page>" / "differs: <page>").
            for page in &drift {
                let _ = writeln!(
                    io.out,
                    "{}",
                    Diagnostic::error(codes::DRIFT, page).render_text()
                );
            }
            if drift.is_empty() {
                let _ = writeln!(io.out, "rust catalog in sync with sources");
            } else {
                let _ = writeln!(
                    io.out,
                    "{} page(s) drifted; run `make catalog` and commit the refreshed pages",
                    drift.len()
                );
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "repoRoot": cfg.repo_root.display().to_string(),
                "output": cfg.output.display().to_string(),
                "drifted": drift.len(),
                "pages": drift,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if drift.is_empty() {
        exit::OK
    } else {
        exit::FINDINGS
    }
}

/// A generator failure is an input/environment problem (unreadable workspace, unwritable output),
/// not a finding about the catalog — the CLI's exit-2 bucket.
fn fail(error: &dyn std::fmt::Display, io: &mut Io<'_>) -> i32 {
    let d = Diagnostic::error(codes::FAILED, format!("{error:#}"));
    let _ = writeln!(io.err, "{}", d.render_text());
    exit::USAGE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    /// A one-crate stand-in workspace: the generator walks `rust/Cargo.toml` members, so the
    /// fixture only needs a manifest pair and one source file.
    fn mini_repo(label: &str) -> PathBuf {
        let root = scratch_dir(label);
        let crate_src = root.join("rust/crates/mini/src");
        std::fs::create_dir_all(&crate_src).unwrap();
        std::fs::write(
            root.join("rust/Cargo.toml"),
            "[workspace]\nresolver = \"2\"\nmembers = [\"crates/mini\"]\n",
        )
        .unwrap();
        std::fs::write(
            root.join("rust/crates/mini/Cargo.toml"),
            "[package]\nname = \"mini\"\ndescription = \"fixture crate\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::write(
            crate_src.join("lib.rs"),
            "//! Fixture crate.\n\n/// A greeting.\npub fn hello() -> &'static str {\n    \"hi\"\n}\n",
        )
        .unwrap();
        root
    }

    fn args(root: &std::path::Path, check: bool) -> CatalogArgs {
        CatalogArgs {
            repo_root: Some(root.to_path_buf()),
            output: Some(root.join("catalog")),
            check,
            clean: false,
        }
    }

    #[test]
    fn generates_pages_for_a_workspace() {
        let root = mini_repo("catalog-generate");
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&root, false), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        assert!(stdout.contains("across 1 crate(s)"), "{stdout}");
        assert!(root.join("catalog/rust/crates/mini/src/lib.md").is_file());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn check_reports_drift_before_a_generate_and_is_clean_after_one() {
        let root = mini_repo("catalog-check");
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&root, true), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::FINDINGS);
        assert!(
            stdout.contains("[ERROR] SUTRA.CATALOG.DRIFT — "),
            "{stdout}"
        );

        let (code, _, _) = run_captured("", |io| {
            execute(args(&root, false), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&root, true), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        assert_eq!(stdout, "rust catalog in sync with sources\n");
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn json_format_is_machine_consumable() {
        let root = mini_repo("catalog-json");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let (code, stdout, _) = run_captured("", |io| execute(args(&root, false), &global, io));
        assert_eq!(code, exit::OK);
        let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(v["crates"], 1);
        assert!(v["pages"].as_u64().unwrap() > 0);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_workspace_less_repo_root_is_a_usage_error() {
        let root = scratch_dir("catalog-empty");
        let (code, _, stderr) = run_captured("", |io| {
            execute(args(&root, false), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::USAGE);
        assert!(
            stderr.contains("[ERROR] SUTRA.CATALOG.FAILED — "),
            "{stderr}"
        );
        std::fs::remove_dir_all(root).ok();
    }
}
