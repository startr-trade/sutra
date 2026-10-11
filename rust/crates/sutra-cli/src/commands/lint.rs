//! `sutra lint` — run the full fail-closed package-time validation suite over one
//! standalone deployment-package directory (R13 authoring unit) without emitting
//! anything. Thin binding over `sutra_loader::lint_dir` — lint and package share ONE
//! validation code path.
//!
//! `--overlay` lints the COMPOSED tree: the layers (one shared folder plus a small
//! per-variant folder) are merged exactly as `sutra package --overlay` merges them, so a
//! variant that is incomplete on its own is linted as what would actually be sealed.

use std::path::PathBuf;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat, Severity};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct LintArgs {
    /// Deployment-package directory to validate (contains package.yaml).
    pub package_dir: PathBuf,

    /// Directory to compose UNDER the package dir; repeatable, lowest layer first.
    ///
    /// For the "one shared folder plus per-variant overlays" layout: each --overlay is a
    /// BASE layer applied in the order given, and the package dir is applied LAST, so the
    /// package dir's copy of a path wins over every overlay and a later overlay wins over
    /// an earlier one. The composed tree is what gets validated — the same composition
    /// `sutra package --overlay` seals.
    ///
    /// Every overridden path is reported as a warning naming the layer that won. An overlay
    /// may not carry package.yaml (labels come from the package dir alone), may not contain
    /// symlinks, and cannot DELETE a file a lower layer provides — composition only adds and
    /// replaces.
    #[arg(long = "overlay", value_name = "DIR")]
    pub overlay: Vec<PathBuf>,
}

/// Convert a loader diagnostic into the CLI's one printable finding shape.
pub(crate) fn diagnostic_of(d: &sutra_loader::LintDiagnostic) -> Diagnostic {
    Diagnostic {
        severity: match d.severity {
            sutra_loader::LintSeverity::Error => Severity::Error,
            sutra_loader::LintSeverity::Warning => Severity::Warn,
        },
        code: d.code.clone(),
        message: d.message.clone(),
        location: None,
    }
}

pub fn execute(args: LintArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "lint: {msg}");
            return exit::USAGE;
        }
    };
    if !args.package_dir.is_dir() {
        let _ = writeln!(
            io.err,
            "lint: package directory not found: {}",
            args.package_dir.display()
        );
        return exit::USAGE;
    }
    for overlay in &args.overlay {
        if !overlay.is_dir() {
            let _ = writeln!(
                io.err,
                "lint: overlay directory not found: {}",
                overlay.display()
            );
            return exit::USAGE;
        }
    }

    let report = sutra_loader::lint_dir_with_overlays(&args.package_dir, &args.overlay);
    let diagnostics: Vec<Diagnostic> = report.diagnostics.iter().map(diagnostic_of).collect();
    let errors = report.errors().count();
    let warnings = report.warnings().count();

    match format {
        ReportFormat::Text => {
            for diagnostic in &diagnostics {
                let _ = writeln!(io.out, "{}", diagnostic.render_text());
            }
            let _ = writeln!(io.out, "{errors} error(s), {warnings} warning(s)");
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "packageDir": args.package_dir.display().to_string(),
                "errors": errors,
                "warnings": warnings,
                "diagnostics": diagnostics.iter().map(Diagnostic::to_json).collect::<Vec<_>>(),
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if errors > 0 {
        exit::FINDINGS
    } else {
        exit::OK
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    const PLAIN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:solo:1.0.0">
  <bpmn:process id="solo" name="Solo" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

    pub(crate) fn valid_package_dir(label: &str) -> std::path::PathBuf {
        let dir = scratch_dir(label);
        std::fs::create_dir_all(dir.join("bpmn")).unwrap();
        std::fs::write(dir.join("bpmn/solo.bpmn"), PLAIN_BPMN).unwrap();
        std::fs::write(
            dir.join("package.yaml"),
            "labels:\n  module: \"solo\"\n  tenant: \"default\"\n  version: \"1.0.0\"\n",
        )
        .unwrap();
        dir
    }

    #[test]
    fn clean_package_reports_zero_findings() {
        let dir = valid_package_dir("lint-clean");
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::OK);
        assert_eq!(out, "0 error(s), 0 warning(s)\n");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn findings_render_the_shared_diagnostic_shape_and_exit_one() {
        let dir = valid_package_dir("lint-findings");
        // entryProcesses naming an unknown process is a deploy-blocking finding.
        std::fs::write(dir.join("package.yaml"), "entryProcesses:\n  - \"ghost\"\n").unwrap();
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::FINDINGS);
        assert!(
            out.starts_with("[ERROR] SUTRA.DEPLOY.PACKAGE.CONFIG_INVALID — "),
            "{out}"
        );
        assert!(out.contains("ghost"), "{out}");
        assert!(out.ends_with("1 error(s), 0 warning(s)\n"), "{out}");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn json_format_is_machine_consumable() {
        let dir = valid_package_dir("lint-json");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
        };
        let (code, out, _) = run_captured("", |io| execute(args, &global, io));
        assert_eq!(code, exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["errors"], 0);
        assert_eq!(v["warnings"], 0);
        assert!(v["diagnostics"].as_array().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn missing_directory_is_a_usage_error() {
        let args = LintArgs {
            package_dir: "/nonexistent/pkg".into(),
            overlay: Vec::new(),
        };
        let (code, _, err) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("not found"), "{err}");
    }
}
