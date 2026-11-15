//! Build-time resolution of the commit and build date a binary reports.
//!
//! Shared by the `build.rs` of `sutra-cli` and `sutra-dist` (each `#[path]`-includes this file),
//! and unit-tested from `sutra-cli`'s lib. Every failure degrades to "value omitted": the build
//! never fails because git is missing, and the wall clock is never read, so two builds of one
//! commit produce the same bytes.
//!
//! Order of precedence:
//! - commit: `SUTRA_BUILD_COMMIT`, else `git rev-parse --short=7 HEAD` (`-dirty` appended when
//!   tracked files have uncommitted changes), else omitted.
//! - built (UTC `YYYY-MM-DD`): `SOURCE_DATE_EPOCH`, else the commit's committer timestamp,
//!   else omitted.

/// Resolves `(commit, built)` from the already-gathered inputs. Pure: no environment, no git.
pub fn resolve(
    env_commit: Option<&str>,
    git_commit: Option<&str>,
    git_dirty: bool,
    env_epoch: Option<&str>,
    git_epoch: Option<&str>,
) -> (Option<String>, Option<String>) {
    let commit = match env_commit.and_then(clean_commit) {
        Some(c) => Some(c),
        None => git_commit.and_then(clean_commit).map(|c| {
            if git_dirty && !c.ends_with("-dirty") {
                format!("{c}-dirty")
            } else {
                c
            }
        }),
    };
    let built = env_epoch
        .and_then(date_from_epoch)
        .or_else(|| git_epoch.and_then(date_from_epoch));
    (commit, built)
}

/// Shortens a full hash to 7 characters, keeps a `-dirty` suffix, and rejects anything that is
/// not a plain token (the value ends up in a `cargo:rustc-env` line and in user-facing output).
fn clean_commit(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let (base, suffix) = match raw.strip_suffix("-dirty") {
        Some(b) => (b, "-dirty"),
        None => (raw, ""),
    };
    if base.is_empty()
        || !base
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return None;
    }
    let base = if base.len() > 7 && base.chars().all(|c| c.is_ascii_hexdigit()) {
        &base[..7]
    } else {
        base
    };
    Some(format!("{base}{suffix}"))
}

/// UTC `YYYY-MM-DD` for a Unix timestamp in seconds, or `None` if it does not parse.
pub fn date_from_epoch(raw: &str) -> Option<String> {
    let secs: i64 = raw.trim().parse().ok()?;
    if secs < 0 {
        return None;
    }
    // Howard Hinnant's civil-from-days algorithm.
    let z = secs / 86_400 + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

#[cfg(not(test))]
fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let s = s.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Gathers the inputs, resolves them and prints the `cargo:` directives. Called from `main` of a
/// build script only.
#[cfg(not(test))]
#[allow(dead_code)]
pub fn emit() {
    println!("cargo:rerun-if-env-changed=SUTRA_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=GIT_DIR");
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    let git_commit = git(&["rev-parse", "--short=7", "HEAD"]);
    let dirty =
        git_commit.is_some() && git(&["status", "--porcelain", "--untracked-files=no"]).is_some();
    let git_epoch = git_commit
        .as_ref()
        .and_then(|_| git(&["log", "-1", "--format=%ct"]));
    if git_commit.is_some() {
        // HEAD, and the ref it points at, move when the commit does.
        for target in ["HEAD".to_string()]
            .into_iter()
            .chain(git(&["symbolic-ref", "-q", "HEAD"]))
        {
            if let Some(p) = git(&["rev-parse", "--git-path", &target]) {
                println!("cargo:rerun-if-changed={p}");
            }
        }
    }
    let (commit, built) = resolve(
        env("SUTRA_BUILD_COMMIT").as_deref(),
        git_commit.as_deref(),
        dirty,
        env("SOURCE_DATE_EPOCH").as_deref(),
        git_epoch.as_deref(),
    );
    if let Some(c) = commit {
        println!("cargo:rustc-env=SUTRA_RESOLVED_COMMIT={c}");
    }
    if let Some(b) = built {
        println!("cargo:rustc-env=SUTRA_RESOLVED_BUILT={b}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_env_commit_beats_git_and_is_shortened() {
        let full = "6db6ae0aabbccddeeff00112233445566778899a";
        let (c, _) = resolve(Some(full), Some("1111111"), true, None, None);
        // CI states the commit outright: no dirty marker is added to it.
        assert_eq!(c.as_deref(), Some("6db6ae0"));
    }

    #[test]
    fn resolve_falls_back_to_git_and_marks_a_dirty_tree() {
        let (c, _) = resolve(None, Some("6db6ae0"), false, None, None);
        assert_eq!(c.as_deref(), Some("6db6ae0"));
        let (c, _) = resolve(Some("  "), Some("6db6ae0"), true, None, None);
        assert_eq!(c.as_deref(), Some("6db6ae0-dirty"));
    }

    #[test]
    fn resolve_omits_what_it_cannot_know() {
        assert_eq!(resolve(None, None, false, None, None), (None, None));
        // A value that is not a plain token never reaches a cargo directive.
        let (c, _) = resolve(Some("abc\ncargo:rustc-env=X=1"), None, false, None, None);
        assert_eq!(c, None);
    }

    #[test]
    fn resolve_source_date_epoch_beats_the_commit_time() {
        // 2026-11-15T12:00:00Z and 2026-01-01T00:00:00Z
        let (_, b) = resolve(None, None, false, Some("1794744000"), Some("1767225600"));
        assert_eq!(b.as_deref(), Some("2026-11-15"));
        let (_, b) = resolve(None, None, false, Some("not-a-number"), Some("1767225600"));
        assert_eq!(b.as_deref(), Some("2026-01-01"));
    }

    #[test]
    fn date_from_epoch_handles_known_dates() {
        assert_eq!(date_from_epoch("0").as_deref(), Some("1970-01-01"));
        assert_eq!(date_from_epoch("951782400").as_deref(), Some("2000-02-29"));
        assert_eq!(date_from_epoch("-1"), None);
    }
}
