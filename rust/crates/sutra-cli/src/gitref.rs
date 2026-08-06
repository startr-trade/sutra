//! Materialises a git ref into a temporary directory (`git archive | tar -x`) so
//! `compat-baseline --baseline <ref>` can scan a historical tree without a checkout.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

/// Top-level directories worth extracting for a BPMN scan under common layouts. Entries
/// that do not exist in the target ref are skipped (git rejects unmatched pathspecs).
const DEFAULT_PATHSPECS: [&str; 5] = ["resources", "src", "tools", "bpmn", "tenants"];

static TEMP_SEQ: AtomicU32 = AtomicU32::new(0);

/// Whether `value` should be treated as a git ref rather than a filesystem path:
/// `refs/`- or `origin/`-prefixed names, or a 7–40 lowercase-hex object id.
pub fn looks_like_git_ref(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    if value.starts_with("refs/") || value.starts_with("origin/") {
        return true;
    }
    (7..=40).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Extracts `git_ref` (restricted to the default pathspecs that exist in that ref) into a
/// fresh temporary directory. The caller must [`cleanup`] the returned directory.
pub fn extract(repo_root: &Path, git_ref: &str) -> Result<PathBuf, String> {
    let pathspecs = existing_pathspecs(repo_root, git_ref)?;

    let dir = std::env::temp_dir().join(format!(
        "sutra-compat-baseline-{}-{}",
        std::process::id(),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("cannot create temp dir: {e}"))?;

    let result = (|| {
        let tar_path = dir.join(".baseline.tar");
        let mut archive = Command::new("git");
        archive
            .current_dir(repo_root)
            .args(["archive", "--format=tar", "-o"])
            .arg(&tar_path)
            .arg(git_ref);
        if !pathspecs.is_empty() {
            archive.arg("--");
            archive.args(&pathspecs);
        }
        run_checked(archive, "git archive")?;

        let mut untar = Command::new("tar");
        untar.arg("-x").arg("-f").arg(&tar_path).arg("-C").arg(&dir);
        run_checked(untar, "tar -x")?;
        std::fs::remove_file(&tar_path).ok();
        Ok(())
    })();

    match result {
        Ok(()) => Ok(dir),
        Err(e) => {
            cleanup(&dir);
            Err(e)
        }
    }
}

/// Best-effort recursive removal of an extraction directory.
pub fn cleanup(dir: &Path) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        tracing::debug!("baseline temp cleanup failed for {}: {e}", dir.display());
    }
}

/// The default pathspecs narrowed to the top-level entries actually present in the ref
/// (probed via `git ls-tree`, which also surfaces unknown-ref errors early). Returns an
/// empty list — archive the whole tree — when none of the defaults exist.
fn existing_pathspecs(repo_root: &Path, git_ref: &str) -> Result<Vec<String>, String> {
    let output = Command::new("git")
        .current_dir(repo_root)
        .args(["ls-tree", "--name-only", git_ref])
        .output()
        .map_err(|e| format!("git not found on PATH or could not be executed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "git archive baseline '{git_ref}' failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let top: Vec<&str> = stdout.lines().collect();
    Ok(DEFAULT_PATHSPECS
        .iter()
        .filter(|p| top.contains(p))
        .map(|p| format!("{p}/"))
        .collect())
}

fn run_checked(mut cmd: Command, label: &str) -> Result<(), String> {
    let output = cmd
        .output()
        .map_err(|e| format!("{label} could not be executed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "{label} failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_heuristic_matches_refs_origin_and_hex_ids() {
        assert!(looks_like_git_ref("refs/heads/main"));
        assert!(looks_like_git_ref("origin/main"));
        assert!(looks_like_git_ref("abc1234"));
        assert!(looks_like_git_ref(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!looks_like_git_ref("main"));
        assert!(!looks_like_git_ref("ABC1234"));
        assert!(!looks_like_git_ref("../some/dir"));
        assert!(!looks_like_git_ref(""));
        assert!(!looks_like_git_ref("abc12")); // too short for an object id
    }
}
