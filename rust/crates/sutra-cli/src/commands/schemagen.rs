//! `sutra generate schema-handler` — (re)generate the Rust binding sources for an XSD message corpus,
//! or drift-check the committed tree against a fresh generation. Thin binding over
//! `sutra_schema_gen`: zero configuration, the XSD files are the only input, and the
//! emission is byte-identical to the committed sources after `rustfmt`.
//!
//! `generate` writes only the files the generator emits — `support.rs`, `Cargo.toml` and the
//! crate's other hand-maintained files are never touched. `check` writes nothing at all.

use std::path::{Path, PathBuf};

use sutra_schema_gen::Mode;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra generate schema-handler` (the `SUTRA.SCHEMAGEN.*` family).
pub mod codes {
    pub const DRIFT: &str = "SUTRA.SCHEMAGEN.DRIFT";
    pub const FAILED: &str = "SUTRA.SCHEMAGEN.FAILED";
}

#[derive(Debug, clap::Args)]
pub struct SchemagenArgs {
    #[command(subcommand)]
    pub action: SchemagenAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum SchemagenAction {
    /// Generate the Rust sources for an XSD corpus into an output directory.
    Generate(GenerateArgs),
    /// Drift gate: regenerate in memory and diff against a committed source tree.
    Check(CheckArgs),
}

#[derive(Debug, clap::Args)]
pub struct GenerateArgs {
    /// Directory of `.xsd` schemas — the generator's only input.
    pub schemas_dir: PathBuf,

    /// Directory the generated sources are written into (created if absent).
    pub out_dir: PathBuf,

    /// Also emit the typed model (opt-in, not committed); default is the slim decode tables.
    #[arg(long)]
    pub full: bool,
}

#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    /// Directory of `.xsd` schemas — the generator's only input.
    pub schemas_dir: PathBuf,

    /// The committed source tree the fresh generation is compared against.
    pub tree_dir: PathBuf,

    /// Compare the typed-model emission as well (matches `generate --full`).
    #[arg(long)]
    pub full: bool,
}

pub fn execute(args: SchemagenArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "schemagen: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        SchemagenAction::Generate(args) => generate(args, format, io),
        SchemagenAction::Check(args) => check(args, format, io),
    }
}

fn mode(full: bool) -> Mode {
    if full {
        Mode::Full
    } else {
        Mode::Slim
    }
}

fn generate(args: GenerateArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    if let Some(code) = reject_missing_dir(&args.schemas_dir, io) {
        return code;
    }
    let written =
        match sutra_schema_gen::generate_into(&args.schemas_dir, &args.out_dir, mode(args.full)) {
            Ok(written) => written,
            Err(e) => return fail(&e, io),
        };
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "generated {} files into {}",
                written.len(),
                args.out_dir.display()
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "schemasDir": args.schemas_dir.display().to_string(),
                "outDir": args.out_dir.display().to_string(),
                "files": written,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

fn check(args: CheckArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    if let Some(code) = reject_missing_dir(&args.schemas_dir, io) {
        return code;
    }
    let report =
        match sutra_schema_gen::check_tree(&args.schemas_dir, &args.tree_dir, mode(args.full)) {
            Ok(report) => report,
            Err(e) => return fail(&e, io),
        };
    match format {
        ReportFormat::Text => {
            // Each entry already carries its kind ("drift: <file>" / "missing in tree: <file>").
            for entry in &report.drift {
                let _ = writeln!(
                    io.out,
                    "{}",
                    Diagnostic::error(codes::DRIFT, entry).render_text()
                );
            }
            if report.drift.is_empty() {
                let _ = writeln!(io.out, "check: {} files in sync", report.checked);
            } else {
                let _ = writeln!(
                    io.out,
                    "check failed: {} file(s) drifted; run `sutra generate schema-handler` and commit",
                    report.drift.len()
                );
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "schemasDir": args.schemas_dir.display().to_string(),
                "treeDir": args.tree_dir.display().to_string(),
                "checked": report.checked,
                "drifted": report.drift.len(),
                "drift": report.drift,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if report.drift.is_empty() {
        exit::OK
    } else {
        exit::FINDINGS
    }
}

fn reject_missing_dir(schemas_dir: &Path, io: &mut Io<'_>) -> Option<i32> {
    if schemas_dir.is_dir() {
        return None;
    }
    let _ = writeln!(
        io.err,
        "schemagen: schemas directory not found: {}",
        schemas_dir.display()
    );
    Some(exit::USAGE)
}

/// A generation failure is an input/environment problem (unparseable XSD, no `rustfmt`,
/// unwritable output), not a finding about the tree — the CLI's exit-2 bucket.
fn fail(error: &dyn std::fmt::Display, io: &mut Io<'_>) -> i32 {
    let d = Diagnostic::error(codes::FAILED, format!("{error}"));
    let _ = writeln!(io.err, "{}", d.render_text());
    exit::USAGE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    /// The generator's own mini XSD corpus (one schema → one module + registry + lib).
    fn mini_corpus() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sutra-schema-gen/tests/data/mini")
    }

    fn generate_args(out_dir: &Path) -> GenerateArgs {
        GenerateArgs {
            schemas_dir: mini_corpus(),
            out_dir: out_dir.to_path_buf(),
            full: false,
        }
    }

    fn check_args(tree_dir: &Path) -> CheckArgs {
        CheckArgs {
            schemas_dir: mini_corpus(),
            tree_dir: tree_dir.to_path_buf(),
            full: false,
        }
    }

    #[test]
    fn generate_writes_the_formatted_sources() {
        let out = scratch_dir("schemagen-generate");
        let (code, stdout, _) = run_captured("", |io| {
            generate(generate_args(&out), ReportFormat::Text, io)
        });
        assert_eq!(code, exit::OK);
        assert!(stdout.starts_with("generated 3 files into "), "{stdout}");
        assert!(out.join("lib.rs").is_file());
        assert!(out.join("registry.rs").is_file());
        std::fs::remove_dir_all(out).ok();
    }

    #[test]
    fn check_reports_drift_before_a_generate_and_is_clean_after_one() {
        let tree = scratch_dir("schemagen-check");
        let (code, stdout, _) =
            run_captured("", |io| check(check_args(&tree), ReportFormat::Text, io));
        assert_eq!(code, exit::FINDINGS);
        assert!(
            stdout.contains("[ERROR] SUTRA.SCHEMAGEN.DRIFT — missing in tree: lib.rs"),
            "{stdout}"
        );

        let (code, _, _) = run_captured("", |io| {
            generate(generate_args(&tree), ReportFormat::Text, io)
        });
        assert_eq!(code, exit::OK);
        let (code, stdout, _) =
            run_captured("", |io| check(check_args(&tree), ReportFormat::Text, io));
        assert_eq!(code, exit::OK);
        assert_eq!(stdout, "check: 3 files in sync\n");
        std::fs::remove_dir_all(tree).ok();
    }

    #[test]
    fn json_format_is_machine_consumable() {
        let tree = scratch_dir("schemagen-json");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let args = SchemagenArgs {
            action: SchemagenAction::Check(check_args(&tree)),
        };
        let (code, stdout, _) = run_captured("", |io| execute(args, &global, io));
        assert_eq!(code, exit::FINDINGS);
        let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(v["checked"], 3);
        assert_eq!(v["drifted"], 3);
        std::fs::remove_dir_all(tree).ok();
    }

    #[test]
    fn missing_schemas_directory_is_a_usage_error() {
        let args = GenerateArgs {
            schemas_dir: "/nonexistent/schemas".into(),
            out_dir: "/nonexistent/out".into(),
            full: false,
        };
        let (code, _, stderr) = run_captured("", |io| generate(args, ReportFormat::Text, io));
        assert_eq!(code, exit::USAGE);
        assert!(stderr.contains("not found"), "{stderr}");
    }
}
