//! `sutra package` — seal one standalone deployment-package directory (the R13
//! authoring unit: archive-layout mirror + `package.yaml`) into one deterministic,
//! content-addressed `.sutra` archive. Validation is fail-closed and shares the exact
//! code path with `sutra lint`. The `.sutra` archive is the only deployment model.

use std::path::PathBuf;

use crate::commands::lint::diagnostic_of;
use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct PackageArgs {
    /// Deployment-package directory to seal into a `.sutra` archive.
    pub input: PathBuf,

    /// Output directory: receives the `.sutra` archive.
    #[arg(short = 'o', long = "out", value_name = "DIR", default_value = ".")]
    pub out: PathBuf,
}

pub fn execute(args: PackageArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "package: {msg}");
            return exit::USAGE;
        }
    };
    if !args.input.is_dir() {
        let _ = writeln!(
            io.err,
            "package: input directory not found: {}",
            args.input.display()
        );
        return exit::USAGE;
    }
    execute_package(&args, format, io)
}

fn execute_package(args: &PackageArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let outcome = match sutra_loader::assemble_dir(&args.input, &args.out, &Default::default()) {
        Ok(outcome) => outcome,
        Err(e) => return render_refusal("package", e, format, io),
    };
    let diagnostics: Vec<Diagnostic> = outcome
        .report
        .diagnostics
        .iter()
        .map(diagnostic_of)
        .collect();

    match format {
        ReportFormat::Text => {
            for diagnostic in &diagnostics {
                let _ = writeln!(io.out, "{}", diagnostic.render_text());
            }
            for archive in &outcome.archives {
                let _ = writeln!(
                    io.out,
                    "packaged {} (deploymentId {})",
                    archive.file_path.display(),
                    archive.id.value()
                );
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "archives": outcome.archives.iter().map(|a| serde_json::json!({
                    "file": a.file_path.display().to_string(),
                    "deploymentId": a.id.value(),
                    "labels": a.manifest.labels,
                    "entryProcesses": a.manifest.entry_processes,
                })).collect::<Vec<_>>(),
                "diagnostics": diagnostics.iter().map(Diagnostic::to_json).collect::<Vec<_>>(),
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

/// A refusal renders every diagnostic in the shared shape: validation findings exit 1,
/// I/O and container failures exit 2.
fn render_refusal(
    command: &str,
    error: sutra_loader::PackageError,
    format: ReportFormat,
    io: &mut Io<'_>,
) -> i32 {
    match error {
        sutra_loader::PackageError::Validation(report) => {
            let diagnostics: Vec<Diagnostic> =
                report.diagnostics.iter().map(diagnostic_of).collect();
            match format {
                ReportFormat::Text => {
                    for diagnostic in &diagnostics {
                        let _ = writeln!(io.out, "{}", diagnostic.render_text());
                    }
                    let _ = writeln!(
                        io.out,
                        "{} error(s) — nothing was emitted (fail-closed)",
                        report.errors().count()
                    );
                }
                ReportFormat::Json => {
                    let payload = serde_json::json!({
                        "errors": report.errors().count(),
                        "diagnostics": diagnostics.iter().map(Diagnostic::to_json).collect::<Vec<_>>(),
                    });
                    let _ = writeln!(io.out, "{payload}");
                }
            }
            exit::FINDINGS
        }
        sutra_loader::PackageError::Io(e) => {
            let _ = writeln!(io.err, "{command}: {e}");
            exit::USAGE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    fn args(input: PathBuf, out: PathBuf) -> PackageArgs {
        PackageArgs { input, out }
    }

    #[test]
    fn seals_a_package_dir_and_prints_the_archive_line() {
        let dir = crate::commands::lint::tests::valid_package_dir("pkg-seal");
        let out_dir = scratch_dir("pkg-seal-out");
        let (code, out, _) = run_captured("", |io| {
            execute(
                args(dir.clone(), out_dir.clone()),
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, exit::OK);
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let expected_file = out_dir.join(format!("{name}.sutra"));
        assert!(expected_file.is_file(), "archive written");
        assert_eq!(
            out,
            format!(
                "packaged {} (deploymentId {})\n",
                expected_file.display(),
                // Pin the id shape without pinning the hash: recompute from the file.
                sutra_loader::read_archive_file(&expected_file)
                    .expect("round-trips")
                    .id
                    .value()
            )
        );
        std::fs::remove_dir_all(dir).ok();
        std::fs::remove_dir_all(out_dir).ok();
    }

    #[test]
    fn json_format_lists_archives_and_diagnostics() {
        let dir = crate::commands::lint::tests::valid_package_dir("pkg-json");
        let out_dir = scratch_dir("pkg-json-out");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let (code, out, _) = run_captured("", |io| {
            execute(args(dir.clone(), out_dir.clone()), &global, io)
        });
        assert_eq!(code, exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let archives = v["archives"].as_array().unwrap();
        assert_eq!(archives.len(), 1);
        assert!(archives[0]["file"].as_str().unwrap().ends_with(".sutra"));
        assert!(archives[0]["deploymentId"]
            .as_str()
            .unwrap()
            .starts_with("dep-"));
        assert_eq!(archives[0]["labels"]["module"], "solo");
        std::fs::remove_dir_all(dir).ok();
        std::fs::remove_dir_all(out_dir).ok();
    }

    #[test]
    fn validation_findings_exit_one_and_emit_nothing() {
        let dir = crate::commands::lint::tests::valid_package_dir("pkg-refuse");
        std::fs::write(dir.join("package.yaml"), "entryProcesses:\n  - \"ghost\"\n").unwrap();
        let out_dir = scratch_dir("pkg-refuse-out");
        let (code, out, _) = run_captured("", |io| {
            execute(
                args(dir.clone(), out_dir.clone()),
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, exit::FINDINGS);
        assert!(
            out.contains("[ERROR] SUTRA.DEPLOY.PACKAGE.CONFIG_INVALID"),
            "{out}"
        );
        assert!(
            out.ends_with("1 error(s) — nothing was emitted (fail-closed)\n"),
            "{out}"
        );
        assert_eq!(std::fs::read_dir(&out_dir).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).ok();
        std::fs::remove_dir_all(out_dir).ok();
    }

    #[test]
    fn missing_input_is_a_usage_error() {
        let (code, _, err) = run_captured("", |io| {
            execute(
                args("/nonexistent/dir".into(), ".".into()),
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("not found"), "{err}");
    }
}
