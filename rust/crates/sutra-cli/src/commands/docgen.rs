//! `sutra docgen` — generate (or drift-check) the authored-artifact markdown catalog for a
//! folder of deployment artifacts: BPMN processes, DMN/SRL rules, Handlebars/XSLT templates and
//! their manifests, `channels.yaml`, `package.yaml`, coverage files. Thin binding over
//! `sutra_docgen`, which parses through the engine's OWN loaders so the pages describe
//! exactly what the engine loads.

use std::path::PathBuf;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra docgen` (the `SUTRA.DOCGEN.*` family).
pub mod codes {
    pub const DRIFT: &str = "SUTRA.DOCGEN.DRIFT";
    pub const FAILED: &str = "SUTRA.DOCGEN.FAILED";
}

#[derive(Debug, clap::Args)]
pub struct DocgenArgs {
    /// Folder recursed for authored deployment artifacts (BPMN/DMN/SRL/templates/YAML).
    #[arg(long, value_name = "FOLDER")]
    pub input: PathBuf,

    /// Catalog output directory (default: `catalog`, relative to the
    /// working directory).
    #[arg(long, value_name = "DIR")]
    pub output: Option<PathBuf>,

    /// Generate into a temp dir and report drift against the committed catalog; the working tree
    /// is never written (CI / pre-commit gate).
    #[arg(long)]
    pub check: bool,
}

pub fn execute(args: DocgenArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "docgen: {msg}");
            return exit::USAGE;
        }
    };
    if !args.input.is_dir() {
        let _ = writeln!(
            io.err,
            "docgen: input folder not found: {}",
            args.input.display()
        );
        return exit::USAGE;
    }

    let cfg = sutra_docgen::Config::new(args.input, args.output);
    if args.check {
        check(&cfg, format, io)
    } else {
        generate(&cfg, format, io)
    }
}

fn generate(cfg: &sutra_docgen::Config, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let report = match sutra_docgen::run(cfg) {
        Ok(report) => report,
        Err(e) => return fail(&e, io),
    };
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "generated {} page(s) across {} package(s) under {}",
                report.pages,
                report.packages,
                cfg.output.display()
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "input": cfg.input.display().to_string(),
                "output": cfg.output.display().to_string(),
                "pages": report.pages,
                "packages": report.packages,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

fn check(cfg: &sutra_docgen::Config, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let drift = match sutra_docgen::check(cfg) {
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
                let _ = writeln!(io.out, "catalog in sync with {}", cfg.input.display());
            } else {
                let _ = writeln!(
                    io.out,
                    "{} page(s) drifted; re-run `sutra docgen --input {} --output {}` and commit the refreshed pages",
                    drift.len(),
                    cfg.input.display(),
                    cfg.output.display()
                );
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "input": cfg.input.display().to_string(),
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

/// A generator failure is an input/environment problem (unreadable folder, unparseable
/// artifact), not a finding about the catalog — the CLI's exit-2 bucket.
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

    /// The generator's own end-to-end fixture package (one package, every artifact type).
    fn fixture_input() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../sutra-docgen/tests/fixtures/mini-package")
    }

    fn args(output: &std::path::Path, check: bool) -> DocgenArgs {
        DocgenArgs {
            input: fixture_input(),
            output: Some(output.to_path_buf()),
            check,
        }
    }

    #[test]
    fn generates_the_catalog_for_a_folder_of_authored_artifacts() {
        let out = scratch_dir("docgen-generate");
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&out, false), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        assert!(stdout.contains("page(s) across 1 package(s)"), "{stdout}");
        assert!(out.join("acme--mini-package--1.0.0/bpmn/flow.md").is_file());
        std::fs::remove_dir_all(out).ok();
    }

    #[test]
    fn check_is_clean_after_a_generate_and_reports_drift_before_one() {
        let out = scratch_dir("docgen-check");
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&out, true), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::FINDINGS);
        assert!(stdout.contains("[ERROR] SUTRA.DOCGEN.DRIFT — "), "{stdout}");

        let (code, _, _) = run_captured("", |io| {
            execute(args(&out, false), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        let (code, stdout, _) = run_captured("", |io| {
            execute(args(&out, true), &GlobalArgs::default(), io)
        });
        assert_eq!(code, exit::OK);
        assert!(stdout.starts_with("catalog in sync with "), "{stdout}");
        std::fs::remove_dir_all(out).ok();
    }

    #[test]
    fn json_format_is_machine_consumable() {
        let out = scratch_dir("docgen-json");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let (code, stdout, _) = run_captured("", |io| execute(args(&out, false), &global, io));
        assert_eq!(code, exit::OK);
        let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(v["packages"], 1);
        assert!(v["pages"].as_u64().unwrap() > 0);
        std::fs::remove_dir_all(out).ok();
    }

    #[test]
    fn missing_input_folder_is_a_usage_error() {
        let a = DocgenArgs {
            input: "/nonexistent/artifacts".into(),
            output: None,
            check: false,
        };
        let (code, _, stderr) = run_captured("", |io| execute(a, &GlobalArgs::default(), io));
        assert_eq!(code, exit::USAGE);
        assert!(stderr.contains("not found"), "{stderr}");
    }
}
