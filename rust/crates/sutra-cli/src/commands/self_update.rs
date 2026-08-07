//! `self-update` — replace the running `sutra` binary with a published release build.
//!
//! Three deliberate properties, because this command downloads code and then *runs as* that
//! code:
//!
//! * **Never automatic.** Nothing here runs on a timer, at startup, or as a side effect of
//!   another command. The user asks, or nothing happens. `--check` exists so scripts can
//!   report an available update without taking one.
//! * **Always verified.** The downloaded archive's SHA-256 is compared against the release's
//!   own `SHA256SUMS` before a single byte is installed. A mismatch aborts; there is no
//!   `--force` past it (an operator who genuinely wants an unverified binary can download it
//!   by hand and see what they are doing).
//! * **Atomic.** The new binary is staged NEXT TO the current one and moved into place with
//!   a rename, so an interrupted update can never leave a half-written executable on PATH.
//!   Windows cannot rename over a running image, so the old one is moved aside first.
//!
//! The version/asset arithmetic ([`Release`], [`asset_name`], [`target_triple`]) is pure and
//! unit-tested; only [`fetch`] and the install step touch the network or the filesystem.

use std::path::{Path, PathBuf};

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

const REPO: &str = "startr-trade/sutra";

#[derive(Debug, Default, clap::Args)]
pub struct SelfUpdateArgs {
    /// Report whether a newer release exists and exit — download nothing, change nothing.
    #[arg(long)]
    pub check: bool,
    /// Install this exact release tag (e.g. `v0.2.0-rc.1`) instead of the newest one.
    /// Downgrades are allowed: pinning is the point.
    #[arg(long, value_name = "TAG")]
    pub version: Option<String>,
    /// Also pull the ENGINE image of the same release (`docker pull`), so the runtime you
    /// deploy onto matches the CLI that packages for it.
    #[arg(long)]
    pub runtime: bool,
    /// Pull only the engine image; leave this binary alone.
    #[arg(long, conflicts_with = "runtime")]
    pub runtime_only: bool,
}

/// The engine image reference for a release tag. The image is tagged with the version
/// WITHOUT the `v` (see `release.yml`'s metadata step), which is the one place these two
/// naming schemes have to agree.
pub fn engine_image(tag: &str) -> String {
    format!("ghcr.io/{REPO}:{}", normalize(tag))
}

/// The published-asset target triple for the host, or `None` when this platform has no
/// release build (macOS today, and anything exotic) — those install from source, and saying
/// so is better than downloading a binary that cannot run.
pub fn target_triple() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("x86_64-unknown-linux-musl"),
        ("linux", "aarch64") => Some("aarch64-unknown-linux-musl"),
        ("windows", "x86_64") => Some("x86_64-pc-windows-msvc"),
        _ => None,
    }
}

/// The release-asset file name for a tag + target — the exact shape `release.yml` packages
/// (`sutra-<tag>-<target>.tar.gz`, `.zip` on Windows).
pub fn asset_name(tag: &str, target: &str) -> String {
    let ext = if target.contains("windows") {
        "zip"
    } else {
        "tar.gz"
    };
    format!("sutra-{tag}-{target}.{ext}")
}

/// `true` when `candidate` names a different release than `current`. Deliberately a plain
/// inequality rather than a semver comparison: the release tag is the identity, and
/// `--version` is explicitly allowed to move backwards.
pub fn differs(current: &str, candidate: &str) -> bool {
    normalize(current) != normalize(candidate)
}

fn normalize(tag: &str) -> &str {
    tag.trim().trim_start_matches('v')
}

pub fn execute(args: SelfUpdateArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "self-update: {msg}");
            return exit::USAGE;
        }
    };

    // `--runtime-only` never touches this binary, so an unpublished host platform (macOS
    // today) can still pull the engine image.
    let target = match target_triple() {
        Some(target) => target,
        None if args.runtime_only => "",
        None => {
            let _ = writeln!(
                io.err,
                "self-update: no published binary for {}/{} — install from source:\n    \
                 cargo install --path rust/crates/sutra-cli\n\
                 (or `sutra self-update --runtime-only` to update just the engine image)",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
            return exit::USAGE;
        }
    };

    let current = crate::VERSION;
    let wanted = match args.version.clone() {
        Some(tag) => tag,
        None => match latest_tag() {
            Ok(tag) => tag,
            Err(msg) => {
                let _ = writeln!(io.err, "self-update: {msg}");
                return exit::USAGE;
            }
        },
    };

    let available = differs(current, &wanted);
    if args.check {
        match format {
            ReportFormat::Json => {
                let _ = writeln!(
                    io.out,
                    "{}",
                    serde_json::json!({
                        "current": current,
                        "latest": wanted,
                        "updateAvailable": available,
                        "engineImage": engine_image(&wanted),
                    })
                );
            }
            ReportFormat::Text if available => {
                let _ = writeln!(
                    io.out,
                    "an update is available: {current} -> {wanted}\nrun `sutra self-update` to install it"
                );
            }
            ReportFormat::Text => {
                let _ = writeln!(io.out, "up to date ({current})");
            }
        }
        return exit::OK;
    }

    if args.runtime_only {
        return match pull_engine(&wanted, io) {
            Ok(()) => exit::OK,
            Err(msg) => {
                let _ = writeln!(io.err, "self-update: {msg}");
                exit::USAGE
            }
        };
    }

    if !available {
        let _ = writeln!(io.out, "already on {current} — nothing to do");
        // The CLI can be current while the local engine image is not (a fresh machine, or a
        // runtime that was never pulled), so an explicit --runtime still runs.
        if args.runtime {
            if let Err(msg) = pull_engine(&wanted, io) {
                let _ = writeln!(io.err, "self-update: {msg}");
                return exit::USAGE;
            }
        }
        return exit::OK;
    }

    let exe = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            let _ = writeln!(io.err, "self-update: cannot locate the running binary: {e}");
            return exit::USAGE;
        }
    };

    let _ = writeln!(io.out, "updating {current} -> {wanted} ({target})");
    if let Err(msg) = install(&wanted, target, &exe) {
        let _ = writeln!(io.err, "self-update: {msg}");
        return exit::USAGE;
    }
    let _ = writeln!(io.out, "installed {wanted} at {}", exe.display());

    if args.runtime {
        if let Err(msg) = pull_engine(&wanted, io) {
            // The CLI is already updated and working; a failed image pull is reported but
            // must not present the whole command as a failure that needs re-running.
            let _ = writeln!(io.err, "self-update: engine image not updated: {msg}");
            return exit::FINDINGS;
        }
    } else {
        let _ = writeln!(
            io.out,
            "\nthe matching engine image is {}\n  pull it with: sutra self-update --runtime-only",
            engine_image(&wanted)
        );
    }
    exit::OK
}

/// Pull the engine image of `tag` through the local Docker CLI. Docker is shelled out to
/// deliberately: it already holds the user's registry credentials, proxy settings and
/// storage config, none of which an embedded registry client would inherit correctly.
fn pull_engine(tag: &str, io: &mut Io<'_>) -> Result<(), String> {
    let image = engine_image(tag);
    let _ = writeln!(io.out, "pulling {image}…");
    let status = std::process::Command::new("docker")
        .arg("pull")
        .arg(&image)
        .status()
        .map_err(|e| format!("running docker: {e} (is docker installed and on PATH?)"))?;
    if !status.success() {
        return Err(format!("docker pull {image} exited {status}"));
    }
    let _ = writeln!(io.out, "engine image ready: {image}");
    Ok(())
}

// ---- network + install -------------------------------------------------------------------

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent(concat!("sutra-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("http client: {e}"))
}

fn fetch(url: &str) -> Result<Vec<u8>, String> {
    block_on(async {
        let response = client()?
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", response.status()));
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("reading {url}: {e}"))
    })
}

/// The newest release tag, pre-releases included. `/releases/latest` is deliberately NOT
/// used: it skips pre-releases, and every 0.x release so far is one.
fn latest_tag() -> Result<String, String> {
    let url = format!("https://api.github.com/repos/{REPO}/releases?per_page=1");
    let body = fetch(&url)?;
    let json: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| format!("release list: {e}"))?;
    json.get(0)
        .and_then(|r| r.get("tag_name"))
        .and_then(|t| t.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| "could not read a release tag (rate-limited? pass --version)".to_string())
}

fn install(tag: &str, target: &str, exe: &Path) -> Result<(), String> {
    let asset = asset_name(tag, target);
    let base = format!("https://github.com/{REPO}/releases/download/{tag}");

    let archive = fetch(&format!("{base}/{asset}"))?;
    let sums = String::from_utf8(fetch(&format!("{base}/SHA256SUMS"))?)
        .map_err(|_| "SHA256SUMS is not valid UTF-8".to_string())?;
    verify(&archive, &sums, &asset)?;

    // Stage in the TARGET directory, never $TMPDIR: the final step must be a rename, and a
    // rename across filesystems fails (a /tmp tmpfs vs /usr/local/bin is the common case).
    let dir = exe.parent().ok_or("the running binary has no parent dir")?;
    let staged = dir.join(format!(".sutra-update-{tag}"));
    unpack(&archive, target, &staged)
        .map_err(|e| format!("{e}\n(no changes made — {} is untouched)", exe.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("chmod {}: {e}", staged.display()))?;
    }
    // Windows holds a lock on a running image, so the old binary is moved aside rather than
    // overwritten; it is deleted on the next run's staging, or by the user.
    #[cfg(windows)]
    {
        let backup = exe.with_extension("old");
        let _ = std::fs::remove_file(&backup);
        std::fs::rename(exe, &backup).map_err(|e| format!("moving the old binary aside: {e}"))?;
    }
    std::fs::rename(&staged, exe).map_err(|e| {
        let _ = std::fs::remove_file(&staged);
        format!("installing into {}: {e}", exe.display())
    })
}

/// Compare the archive's SHA-256 against its `SHA256SUMS` line. Fail-closed on every
/// ambiguity: a missing entry is as fatal as a mismatch.
fn verify(archive: &[u8], sums: &str, asset: &str) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    let _ = &Sha256::new;
    let want = sums
        .lines()
        .find_map(|line| {
            let (hash, name) = line.split_once(char::is_whitespace)?;
            (name.trim().trim_start_matches('*') == asset).then(|| hash.trim().to_lowercase())
        })
        .ok_or_else(|| format!("SHA256SUMS carries no entry for {asset}"))?;
    let got = format!("{:x}", Sha256::digest(archive));
    if got != want {
        return Err(format!(
            "CHECKSUM MISMATCH for {asset}\n  expected {want}\n  got      {got}\nrefusing to install"
        ));
    }
    Ok(())
}

/// Extract the `sutra` binary out of the release archive into `dest`.
fn unpack(archive: &[u8], target: &str, dest: &PathBuf) -> Result<(), String> {
    if target.contains("windows") {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(archive))
            .map_err(|e| format!("opening the release zip: {e}"))?;
        for i in 0..zip.len() {
            let mut entry = zip.by_index(i).map_err(|e| format!("zip entry {i}: {e}"))?;
            if entry.name().ends_with("sutra.exe") {
                let mut out = std::fs::File::create(dest)
                    .map_err(|e| format!("creating {}: {e}", dest.display()))?;
                std::io::copy(&mut entry, &mut out).map_err(|e| format!("extracting: {e}"))?;
                return Ok(());
            }
        }
        Err("the release zip contained no sutra.exe".to_string())
    } else {
        // The tarball is small (one stripped binary + licenses) and already in memory; shelling
        // out to `tar` keeps the dependency surface of an updater minimal.
        let tmp = dest.with_extension("tar.gz");
        std::fs::write(&tmp, archive).map_err(|e| format!("staging the archive: {e}"))?;
        let dir = dest.parent().ok_or("no staging dir")?;
        let status = std::process::Command::new("tar")
            .arg("-xzf")
            .arg(&tmp)
            .arg("-C")
            .arg(dir)
            .arg("--strip-components=1")
            .arg("--wildcards")
            .arg("*/sutra")
            .status();
        let _ = std::fs::remove_file(&tmp);
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => return Err(format!("tar exited {s}")),
            Err(e) => return Err(format!("running tar: {e} (is tar on PATH?)")),
        }
        let extracted = dir.join("sutra");
        std::fs::rename(&extracted, dest).map_err(|e| format!("staging the new binary: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;

    #[test]
    fn asset_names_match_what_the_release_workflow_packages() {
        assert_eq!(
            asset_name("v0.2.0-rc.1", "x86_64-unknown-linux-musl"),
            "sutra-v0.2.0-rc.1-x86_64-unknown-linux-musl.tar.gz"
        );
        assert_eq!(
            asset_name("v1.0.0", "x86_64-pc-windows-msvc"),
            "sutra-v1.0.0-x86_64-pc-windows-msvc.zip"
        );
    }

    #[test]
    fn a_v_prefix_is_not_a_version_difference() {
        assert!(!differs("0.2.0-rc.1", "v0.2.0-rc.1"));
        assert!(differs("0.2.0-rc.1", "v0.2.0"));
    }

    #[test]
    fn verification_accepts_the_matching_digest_and_refuses_everything_else() {
        let archive = b"release bytes";
        let digest = {
            use sha2::Digest as _;
            format!("{:x}", sha2::Sha256::digest(archive))
        };
        let asset = "sutra-v1.0.0-x86_64-unknown-linux-musl.tar.gz";

        // The real SHA256SUMS shape: `<hash>  <name>`, several entries, one of them ours.
        let sums = format!("{digest}  {asset}\n0000  sutra-v1.0.0-other.tar.gz\n");
        assert!(verify(archive, &sums, asset).is_ok());

        // A wrong digest and a missing entry are equally fatal — no "close enough" path.
        let tampered = format!("{}  {asset}\n", "0".repeat(64));
        let err = verify(archive, &tampered, asset).expect_err("mismatch must fail");
        assert!(err.contains("CHECKSUM MISMATCH"), "{err}");
        let err = verify(archive, "0000  something-else.tar.gz\n", asset)
            .expect_err("a missing entry must fail");
        assert!(err.contains("no entry"), "{err}");
    }

    #[test]
    fn the_engine_image_tag_drops_the_v_the_way_the_release_workflow_does() {
        // release.yml tags the image with `${tag#v}`; if these two ever disagree, `--runtime`
        // pulls a tag that does not exist.
        assert_eq!(
            engine_image("v0.2.0-rc.1"),
            "ghcr.io/startr-trade/sutra:0.2.0-rc.1"
        );
        assert_eq!(engine_image("1.0.0"), "ghcr.io/startr-trade/sutra:1.0.0");
    }

    #[test]
    fn check_json_names_the_matching_engine_image() {
        let args = SelfUpdateArgs {
            check: true,
            version: Some("v9.9.9".to_string()),
            ..SelfUpdateArgs::default()
        };
        let global = GlobalArgs {
            format: Some("json".to_string()),
            ..GlobalArgs::default()
        };
        let (_, out, _) = run_captured("", |io| execute(args, &global, io));
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["engineImage"], "ghcr.io/startr-trade/sutra:9.9.9");
    }

    #[test]
    fn check_against_the_running_version_reports_up_to_date_without_touching_the_network() {
        // `--check --version <own version>` short-circuits before any HTTP call: the tag is
        // supplied, so `latest_tag()` is never reached. This is the property that keeps the
        // command testable (and scriptable) offline.
        let args = SelfUpdateArgs {
            check: true,
            version: Some(crate::VERSION.to_string()),
            ..SelfUpdateArgs::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("up to date"), "{out}");
    }

    #[test]
    fn check_reports_an_available_update_and_json_carries_both_versions() {
        let args = SelfUpdateArgs {
            check: true,
            version: Some("v99.0.0".to_string()),
            ..SelfUpdateArgs::default()
        };
        let global = GlobalArgs {
            format: Some("json".to_string()),
            ..GlobalArgs::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &global, io));
        assert_eq!(code, crate::exit::OK);
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["updateAvailable"], true);
        assert_eq!(payload["latest"], "v99.0.0");
        assert_eq!(payload["current"], crate::VERSION);
    }
}
