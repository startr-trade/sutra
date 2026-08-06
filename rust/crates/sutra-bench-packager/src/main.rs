//! `sutra-bench-packager package <dir> --out <dir>` — a minimal stand-in for `sutra package`,
//! used ONLY by `rust/bench/` (set `SUTRA_CLI` to this binary's path). It calls
//! `sutra_loader::assemble_dir` directly — the exact function `sutra-cli`'s own `package`
//! subcommand calls (see `crates/sutra-cli/src/commands/package.rs::execute_package`) — so the
//! emitted `.sutra` archive is byte-for-byte what the real CLI would produce. The only
//! difference is which binary calls it: this crate depends on nothing but `sutra-loader`, so
//! `cargo build -p sutra-bench-packager --release` never force-links the builtin codec set
//! (`sutra-cli` force-links every built-in codec for codec-URN completeness — irrelevant to
//! packaging a deployment that never references them).
//!
//! Not a general substitute for `sutra-cli`: no `lint`/`deploy`/other subcommands, no format
//! flags. A controlled-host GA bench run should use the real `sutra` CLI
//! (`cargo build --release -p sutra-cli`) for full fidelity; this exists so the harness is still
//! runnable in a worktree where building the full codec set is off-limits.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) != Some("package") {
        eprintln!("usage: sutra-bench-packager package <dir> --out <out-dir>");
        return ExitCode::from(2);
    }

    let mut input: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--out" | "-o" => {
                out = iter.next().map(PathBuf::from);
            }
            other => {
                if input.is_none() {
                    input = Some(PathBuf::from(other));
                }
            }
        }
    }

    let (Some(input), Some(out)) = (input, out) else {
        eprintln!("usage: sutra-bench-packager package <dir> --out <out-dir>");
        return ExitCode::from(2);
    };

    if !input.is_dir() {
        eprintln!(
            "sutra-bench-packager: input directory not found: {}",
            input.display()
        );
        return ExitCode::from(2);
    }

    match sutra_loader::assemble_dir(&input, &out, &Default::default()) {
        Ok(outcome) => {
            for archive in &outcome.archives {
                println!(
                    "packaged {} (deploymentId {})",
                    archive.file_path.display(),
                    archive.id.value()
                );
            }
            ExitCode::SUCCESS
        }
        Err(sutra_loader::PackageError::Validation(report)) => {
            for d in &report.diagnostics {
                eprintln!("[{:?}] {} {}", d.severity, d.code, d.message);
            }
            eprintln!(
                "{} error(s) — nothing was emitted (fail-closed)",
                report.errors().count()
            );
            ExitCode::from(1)
        }
        Err(sutra_loader::PackageError::Io(e)) => {
            eprintln!("sutra-bench-packager: {e}");
            ExitCode::from(2)
        }
    }
}
