//! `sutra compat-baseline` — compare the current tree's BPMN signatures against a
//! baseline (directory or git ref) and report backward-incompatible removals as
//! `SUTRA.COMPAT.*` diagnostics (frozen codes). Removals are breaking (exit 1 findings);
//! additions are informational.

use std::path::{Path, PathBuf};

use crate::compat::{check, BpmnSignature};
use crate::exit;
use crate::gitref;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct CompatBaselineArgs {
    /// Path or git ref (refs/…, origin/…, or a 7–40 hex object id) to compare against.
    #[arg(long, value_name = "PATH_OR_REF")]
    pub baseline: String,

    /// Path to the current tree.
    #[arg(long, default_value = ".", value_name = "PATH")]
    pub current: String,

    /// Glob for BPMN files, matched against paths relative to each root.
    #[arg(long, default_value = "**/*.bpmn", value_name = "GLOB")]
    pub include: String,

    /// If true, exit 1 (findings) when a breaking change is detected.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set, value_name = "BOOL")]
    pub exit_on_break: bool,
}

pub fn execute(args: CompatBaselineArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "compat-baseline: {msg}");
            return exit::USAGE;
        }
    };
    let include = match glob_to_regex(&args.include) {
        Ok(re) => re,
        Err(msg) => {
            let _ = writeln!(io.err, "compat-baseline: invalid --include glob: {msg}");
            return exit::USAGE;
        }
    };

    let current_root = match std::path::absolute(&args.current) {
        Ok(p) => p,
        Err(e) => {
            let _ = writeln!(io.err, "compat-baseline: invalid --current path: {e}");
            return exit::USAGE;
        }
    };

    let mut temp_baseline: Option<PathBuf> = None;
    let baseline_root = if gitref::looks_like_git_ref(&args.baseline) {
        match gitref::extract(&current_root, &args.baseline) {
            Ok(dir) => {
                temp_baseline = Some(dir.clone());
                dir
            }
            Err(msg) => {
                let _ = writeln!(io.err, "compat-baseline: {msg}");
                return exit::USAGE;
            }
        }
    } else {
        let path = PathBuf::from(&args.baseline);
        if !path.is_dir() {
            let _ = writeln!(
                io.err,
                "compat-baseline: baseline path is not a directory: {}",
                path.display()
            );
            return exit::USAGE;
        }
        path
    };

    let result = (|| -> Result<i32, String> {
        let baseline_sigs = scan_signatures(&baseline_root, &include)?;
        let current_sigs = scan_signatures(&current_root, &include)?;
        let report = check(&baseline_sigs, &current_sigs);

        match format {
            ReportFormat::Text => {
                let _ = write!(io.out, "{}", report.render_text());
            }
            ReportFormat::Json => {
                let _ = writeln!(io.out, "{}", report.render_json());
            }
        }
        if report.has_breaking_change() && args.exit_on_break {
            Ok(exit::FINDINGS)
        } else {
            Ok(exit::OK)
        }
    })();

    if let Some(dir) = temp_baseline {
        gitref::cleanup(&dir);
    }

    match result {
        Ok(code) => code,
        Err(msg) => {
            let _ = writeln!(io.err, "compat-baseline: {msg}");
            exit::USAGE
        }
    }
}

/// Walks `root` for files whose root-relative path (forward slashes) matches `include`,
/// extracting a signature keyed by that relative path so both trees pair up.
fn scan_signatures(root: &Path, include: &regex::Regex) -> Result<Vec<BpmnSignature>, String> {
    let mut files = Vec::new();
    collect_files(root, root, include, &mut files)?;
    files.sort();
    let mut signatures = Vec::new();
    for (relative, path) in files {
        let raw = BpmnSignature::extract(&path)?;
        signatures.push(BpmnSignature {
            file_path: relative,
            processes: raw.processes,
        });
    }
    Ok(signatures)
}

fn collect_files(
    root: &Path,
    dir: &Path,
    include: &regex::Regex,
    out: &mut Vec<(String, PathBuf)>,
) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("cannot read entry in {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, include, out)?;
        } else if let Ok(relative) = path.strip_prefix(root) {
            let relative = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if include.is_match(&relative) {
                out.push((relative, path));
            }
        }
    }
    Ok(())
}

/// Minimal glob → anchored regex translation: `**/` spans zero or more directories,
/// `**` spans anything, `*` spans within one segment, `?` one non-separator character.
fn glob_to_regex(pattern: &str) -> Result<regex::Regex, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut re = String::from("^");
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' if i + 1 < chars.len() && chars[i + 1] == '*' => {
                if i + 2 < chars.len() && chars[i + 2] == '/' {
                    re.push_str("(?:[^/]+/)*");
                    i += 3;
                } else {
                    re.push_str(".*");
                    i += 2;
                }
            }
            '*' => {
                re.push_str("[^/]*");
                i += 1;
            }
            '?' => {
                re.push_str("[^/]");
                i += 1;
            }
            c => {
                re.push_str(&regex::escape(&c.to_string()));
                i += 1;
            }
        }
    }
    re.push('$');
    regex::Regex::new(&re).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_translation_covers_the_default_include() {
        let re = glob_to_regex("**/*.bpmn").unwrap();
        assert!(re.is_match("a.bpmn"));
        assert!(re.is_match("modules/pay/1.0.0/bpmn/flow.bpmn"));
        assert!(!re.is_match("flow.bpmn.bak"));
        assert!(!re.is_match("flow.dmn"));

        let single = glob_to_regex("*.bpmn").unwrap();
        assert!(single.is_match("a.bpmn"));
        assert!(!single.is_match("dir/a.bpmn"));

        let q = glob_to_regex("v?.bpmn").unwrap();
        assert!(q.is_match("v1.bpmn"));
        assert!(!q.is_match("v10.bpmn"));
    }
}

#[cfg(test)]
mod command_tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::{scratch_dir, HELLO_BPMN};

    fn run(args: CompatBaselineArgs, format: Option<&str>) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured("", |io| execute(args, &global, io))
    }

    fn write(root: &Path, relative: &str, content: &str) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn args(baseline: &str, current: &str) -> CompatBaselineArgs {
        CompatBaselineArgs {
            baseline: baseline.to_owned(),
            current: current.to_owned(),
            include: "**/*.bpmn".to_owned(),
            exit_on_break: true,
        }
    }

    #[test]
    fn identical_trees_are_compatible() {
        let baseline = scratch_dir("compat-base");
        let current = scratch_dir("compat-cur");
        write(&baseline, "bpmn/hello.bpmn", HELLO_BPMN);
        write(&current, "bpmn/hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(
            args(
                &baseline.display().to_string(),
                &current.display().to_string(),
            ),
            None,
        );
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("No breaking changes."), "{out}");
        assert!(out.contains("Result: COMPATIBLE"), "{out}");
    }

    #[test]
    fn removed_file_is_breaking_with_the_frozen_code() {
        let baseline = scratch_dir("compat-base");
        let current = scratch_dir("compat-cur");
        write(&baseline, "bpmn/hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(
            args(
                &baseline.display().to_string(),
                &current.display().to_string(),
            ),
            None,
        );
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(
            out.contains(
                "[ERROR] SUTRA.COMPAT.PROCESS_REMOVED — process 'hello' removed (bpmn/hello.bpmn)"
            ),
            "{out}"
        );
        assert!(out.contains("Result: BREAKING"), "{out}");
    }

    #[test]
    fn exit_on_break_false_reports_but_exits_zero() {
        let baseline = scratch_dir("compat-base");
        let current = scratch_dir("compat-cur");
        write(&baseline, "bpmn/hello.bpmn", HELLO_BPMN);
        let mut a = args(
            &baseline.display().to_string(),
            &current.display().to_string(),
        );
        a.exit_on_break = false;
        let (code, out, _) = run(a, None);
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("Result: BREAKING"), "{out}");
    }

    #[test]
    fn json_report_carries_the_code() {
        let baseline = scratch_dir("compat-base");
        let current = scratch_dir("compat-cur");
        write(&baseline, "bpmn/hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(
            args(
                &baseline.display().to_string(),
                &current.display().to_string(),
            ),
            Some("json"),
        );
        assert_eq!(code, crate::exit::FINDINGS);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["hasBreakingChange"], true);
        assert_eq!(v["breaking"][0]["code"], "SUTRA.COMPAT.PROCESS_REMOVED");
    }

    #[test]
    fn missing_baseline_directory_is_a_usage_error() {
        let current = scratch_dir("compat-cur");
        let (code, _, err) = run(
            args("/does/not/exist-at-all", &current.display().to_string()),
            None,
        );
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("baseline path is not a directory"), "{err}");
    }

    #[test]
    fn git_ref_baseline_extracts_via_git_archive() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = scratch_dir("compat-git");
        write(&repo, "bpmn/hello.bpmn", HELLO_BPMN);
        git(&repo, &["init", "-q"]);
        git(&repo, &["add", "."]);
        git(
            &repo,
            &[
                "-c",
                "user.email=compat@test",
                "-c",
                "user.name=Compat Test",
                "commit",
                "-q",
                "-m",
                "baseline",
                "--no-gpg-sign",
            ],
        );
        let head = git_out(&repo, &["rev-parse", "HEAD"]);

        // Remove the process in the working tree — HEAD (a hex object id → git-ref path)
        // must flag it as breaking.
        std::fs::remove_file(repo.join("bpmn/hello.bpmn")).unwrap();
        let (code, out, _) = run(args(head.trim(), &repo.display().to_string()), None);
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(out.contains("SUTRA.COMPAT.PROCESS_REMOVED"), "{out}");
    }

    #[test]
    fn unknown_git_ref_is_a_usage_error() {
        if !git_available() {
            eprintln!("skipping: git binary not on PATH");
            return;
        }
        let repo = scratch_dir("compat-git-bad");
        write(&repo, "README", "hi");
        git(&repo, &["init", "-q"]);
        git(&repo, &["add", "."]);
        git(
            &repo,
            &[
                "-c",
                "user.email=compat@test",
                "-c",
                "user.name=Compat Test",
                "commit",
                "-q",
                "-m",
                "init",
                "--no-gpg-sign",
            ],
        );
        // 40-hex id that exists in no repository.
        let (code, _, err) = run(
            args(
                "0123456789abcdef0123456789abcdef01234567",
                &repo.display().to_string(),
            ),
            None,
        );
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("failed"), "{err}");
    }

    fn git_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn git(repo: &Path, cmd_args: &[&str]) {
        let out = std::process::Command::new("git")
            .current_dir(repo)
            .args(cmd_args)
            .output()
            .expect("git");
        assert!(
            out.status.success(),
            "git {cmd_args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn git_out(repo: &Path, cmd_args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .current_dir(repo)
            .args(cmd_args)
            .output()
            .expect("git");
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap()
    }
}
