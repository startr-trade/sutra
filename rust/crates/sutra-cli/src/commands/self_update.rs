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

/// The distribution that is actually running: its repository, its binary name, and the image
/// its release publishes. `None` means this build never declared one — see
/// [`crate::run_with_update_source`] — and every path below refuses rather than defaulting to
/// some other product's releases.
fn source() -> Option<&'static crate::UpdateSource> {
    crate::update_source()
}

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
pub fn engine_image_of(image: &str, tag: &str) -> String {
    format!("{image}:{}", normalize(tag))
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
pub fn asset_name_of(binary: &str, tag: &str, target: &str) -> String {
    let ext = if target.contains("windows") {
        "zip"
    } else {
        "tar.gz"
    };
    format!("{binary}-{tag}-{target}.{ext}")
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
    execute_with(args, global, io, source())
}

/// [`execute`] with the update source passed in rather than read from the process-global
/// declaration — so both the "declared" and "not declared" behaviours are testable, in
/// parallel, without either test depending on the other's ordering.
pub fn execute_with(
    args: SelfUpdateArgs,
    global: &GlobalArgs,
    io: &mut Io<'_>,
    source: Option<&crate::UpdateSource>,
) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "self-update: {msg}");
            return exit::USAGE;
        }
    };

    // WHICH product is running decides where its releases live. A distribution that never
    // declared one must not fall through to some other project's binaries.
    let Some(src) = source else {
        let _ = writeln!(
            io.err,
            "self-update: {} does not publish through this channel.\n\
             This build embeds the engine CLI library but ships its own releases; updating it \
             from here would install a DIFFERENT product over it.\n\
             Use the install method your distribution documents.",
            crate::program_name()
        );
        return exit::USAGE;
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
                 (or `{} self-update --runtime-only` to update just the engine image)",
                std::env::consts::OS,
                std::env::consts::ARCH,
                crate::program_name()
            );
            return exit::USAGE;
        }
    };

    // One sign-in per invocation, before anything else touches the network. A distribution
    // with no token command gets `None` and the public path.
    let token = match &src.token_command {
        Some(cmd) => match token_from(cmd) {
            Ok(t) => Some(t),
            Err(msg) => {
                let _ = writeln!(io.err, "self-update: {msg}");
                return exit::USAGE;
            }
        },
        None => None,
    };

    // The DISTRIBUTION's own version, not the embedded engine's. `version_string()` may be a
    // multi-line block ("<product> 2.0.0\nsutra 0.2.0-rc.1 (engine)"); line one is the
    // product. Comparing the engine's version against a product's release tags would report
    // an update on every run, forever.
    let current = crate::version_string()
        .lines()
        .next()
        .unwrap_or(crate::VERSION)
        .rsplit(' ')
        .next()
        .unwrap_or(crate::VERSION);
    let wanted = match args.version.clone() {
        Some(tag) => tag,
        None => match latest_tag(&src.channel, &src.binary, token.as_deref()) {
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
                        "engineImage": src.image.as_ref().map(|i| engine_image_of(i, &wanted)),
                    })
                );
            }
            ReportFormat::Text if available => {
                let _ = writeln!(
                    io.out,
                    "an update is available: {current} -> {wanted}\nrun `{} self-update` to install it",
                    crate::program_name()
                );
            }
            ReportFormat::Text => {
                let _ = writeln!(io.out, "up to date ({current})");
            }
        }
        return exit::OK;
    }

    if args.runtime_only {
        return match pull_engine(src, &wanted, io) {
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
            if let Err(msg) = pull_engine(src, &wanted, io) {
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
    if let Err(msg) = install(src, &wanted, target, &exe, token.as_deref()) {
        let _ = writeln!(io.err, "self-update: {msg}");
        return exit::USAGE;
    }
    let _ = writeln!(io.out, "installed {wanted} at {}", exe.display());

    if args.runtime {
        if let Err(msg) = pull_engine(src, &wanted, io) {
            // The CLI is already updated and working; a failed image pull is reported but
            // must not present the whole command as a failure that needs re-running.
            let _ = writeln!(io.err, "self-update: engine image not updated: {msg}");
            return exit::FINDINGS;
        }
    } else if let Some(image) = &src.image {
        let _ = writeln!(
            io.out,
            "\nthe matching engine image is {}\n  pull it with: {} self-update --runtime-only",
            engine_image_of(image, &wanted),
            crate::program_name()
        );
    }
    exit::OK
}

/// Pull the engine image of `tag` through the local Docker CLI. Docker is shelled out to
/// deliberately: it already holds the user's registry credentials, proxy settings and
/// storage config, none of which an embedded registry client would inherit correctly.
fn pull_engine(src: &crate::UpdateSource, tag: &str, io: &mut Io<'_>) -> Result<(), String> {
    let Some(base) = &src.image else {
        return Err("this distribution publishes no engine image".to_string());
    };
    let image = engine_image_of(base, tag);
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

/// Run a distribution's token command and return its stdout as a bearer token. A non-zero
/// exit is reported with the command line, because "not signed in" is the common case and the
/// user needs to know what to run.
fn token_from(command: &[String]) -> Result<String, String> {
    let (program, args) = command.split_first().ok_or("empty token command")?;
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("running `{}`: {e} (is it installed?)", command.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "`{}` failed — you are probably not signed in",
            command.join(" ")
        ));
    }
    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(format!("`{}` produced no token", command.join(" ")));
    }
    Ok(token)
}

fn fetch(url: &str, token: Option<&str>) -> Result<Vec<u8>, String> {
    block_on(async {
        let mut request = client()?.get(url);
        // A private channel needs credentials; a public one must not send any.
        if let Some(token) = token {
            request = request.bearer_auth(token);
        } else if let Some(auth) = channel_auth() {
            let (user, password) = auth.split_once(':').unwrap_or((auth.as_str(), ""));
            request = request.basic_auth(user, Some(password));
        }
        let response = request
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

/// Credentials for a private channel, as `user:app-password`. Read from the
/// program-specific variable first (`FOO_AUTH` for a binary named `foo`), then the generic
/// one — so a machine with several distributions installed keeps their tokens apart.
fn channel_auth() -> Option<String> {
    let specific = format!(
        "{}_AUTH",
        crate::program_name().to_uppercase().replace('-', "_")
    );
    std::env::var(specific)
        .ok()
        .or_else(|| std::env::var("SUTRA_UPDATE_AUTH").ok())
        .filter(|v| !v.trim().is_empty())
}

/// The newest release tag on this distribution's channel, pre-releases included.
fn latest_tag(
    channel: &crate::UpdateChannel,
    binary: &str,
    token: Option<&str>,
) -> Result<String, String> {
    match channel {
        // `/releases/latest` is deliberately NOT used: it skips pre-releases, and every 0.x
        // release so far is one.
        crate::UpdateChannel::GithubReleases { repo } => {
            let body = fetch(
                &format!("https://api.github.com/repos/{repo}/releases?per_page=1"),
                token,
            )?;
            let json: serde_json::Value =
                serde_json::from_slice(&body).map_err(|e| format!("release list: {e}"))?;
            json.get(0)
                .and_then(|r| r.get("tag_name"))
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    "could not read a release tag (rate-limited? pass --version)".to_string()
                })
        }
        // Downloads carry no tag of their own, so the tag is recovered from the asset NAME
        // (`<binary>-<tag>-<target>.tar.gz`) and the newest upload wins.
        // A flat store carries no tag of its own, so the tag is recovered from the asset
        // NAME (`<binary>-<tag>-<target>.<ext>`) and the listing's own order decides "newest".
        crate::UpdateChannel::FileStore {
            index_url,
            index_pointer,
            index_name_field,
            ..
        } => {
            let Some(index_url) = index_url else {
                return Err(
                    "this distribution publishes no release index — pass --version <tag>"
                        .to_string(),
                );
            };
            let body = fetch(index_url, token)?;
            let json: serde_json::Value =
                serde_json::from_slice(&body).map_err(|e| format!("release index: {e}"))?;
            let entries = if index_pointer.is_empty() {
                Some(&json)
            } else {
                json.pointer(index_pointer)
            }
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("release index has no array at '{index_pointer}'"))?;
            entries
                .iter()
                .find_map(|entry| {
                    let name = entry.get(index_name_field)?.as_str()?;
                    let rest = name.strip_prefix(&format!("{binary}-"))?;
                    // Every published target begins with an architecture segment, so that is
                    // where the tag ends.
                    let cut = rest.find("-x86_64").or_else(|| rest.find("-aarch64"))?;
                    Some(rest[..cut].to_string())
                })
                .ok_or_else(|| {
                    format!(
                        "no {binary} release found in the index \
                         (private store? set the auth variable, or pass --version)"
                    )
                })
        }
    }
}

fn install(
    src: &crate::UpdateSource,
    tag: &str,
    target: &str,
    exe: &Path,
    token: Option<&str>,
) -> Result<(), String> {
    let asset = asset_name_of(&src.binary, tag, target);
    let base = match &src.channel {
        crate::UpdateChannel::GithubReleases { repo } => {
            format!("https://github.com/{repo}/releases/download/{tag}")
        }
        // A flat namespace — the tag lives in the file name, not in a path segment.
        crate::UpdateChannel::FileStore { base, .. } => base.clone(),
    };

    let archive = fetch(&format!("{base}/{asset}"), token)?;
    let sums_name = match &src.channel {
        crate::UpdateChannel::GithubReleases { .. } => "SHA256SUMS".to_string(),
        // A flat store holds several artifacts' sums side by side, so the checksum file is
        // named for the artifact it covers rather than being a bare SHA256SUMS.
        crate::UpdateChannel::FileStore { .. } => format!("SHA256SUMS-{}.txt", src.binary),
    };
    let sums = String::from_utf8(fetch(&format!("{base}/{sums_name}"), token)?)
        .map_err(|_| format!("{sums_name} is not valid UTF-8"))?;
    verify(&archive, &sums, &asset)?;

    // Stage in the TARGET directory, never $TMPDIR: the final step must be a rename, and a
    // rename across filesystems fails (a /tmp tmpfs vs /usr/local/bin is the common case).
    let dir = exe.parent().ok_or("the running binary has no parent dir")?;
    let staged = dir.join(format!(".{}-update-{tag}", src.binary));
    unpack(&archive, &src.binary, target, &staged)
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
fn unpack(archive: &[u8], binary: &str, target: &str, dest: &PathBuf) -> Result<(), String> {
    if target.contains("windows") {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(archive))
            .map_err(|e| format!("opening the release zip: {e}"))?;
        for i in 0..zip.len() {
            let mut entry = zip.by_index(i).map_err(|e| format!("zip entry {i}: {e}"))?;
            if entry.name().ends_with(&format!("{binary}.exe")) {
                let mut out = std::fs::File::create(dest)
                    .map_err(|e| format!("creating {}: {e}", dest.display()))?;
                std::io::copy(&mut entry, &mut out).map_err(|e| format!("extracting: {e}"))?;
                return Ok(());
            }
        }
        Err(format!("the release zip contained no {binary}.exe"))
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
            .arg(format!("*/{binary}"))
            .status();
        let _ = std::fs::remove_file(&tmp);
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => return Err(format!("tar exited {s}")),
            Err(e) => return Err(format!("running tar: {e} (is tar on PATH?)")),
        }
        let extracted = dir.join(binary);
        std::fs::rename(&extracted, dest).map_err(|e| format!("staging the new binary: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;

    /// A stand-in distribution, so a rendering test never depends on what this build declared.
    fn test_source() -> crate::UpdateSource {
        crate::UpdateSource {
            channel: crate::UpdateChannel::GithubReleases {
                repo: "startr-trade/sutra".to_string(),
            },
            binary: "sutra".to_string(),
            image: Some("ghcr.io/startr-trade/sutra".to_string()),
            token_command: None,
        }
    }

    #[test]
    fn asset_names_match_what_the_release_workflow_packages() {
        assert_eq!(
            asset_name_of("sutra", "v0.2.0-rc.1", "x86_64-unknown-linux-musl"),
            "sutra-v0.2.0-rc.1-x86_64-unknown-linux-musl.tar.gz"
        );
        assert_eq!(
            asset_name_of("sutra", "v1.0.0", "x86_64-pc-windows-msvc"),
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
            engine_image_of("ghcr.io/startr-trade/sutra", "v0.2.0-rc.1"),
            "ghcr.io/startr-trade/sutra:0.2.0-rc.1"
        );
        assert_eq!(
            engine_image_of("registry.example.com/team/product-engine", "1.0.0"),
            "registry.example.com/team/product-engine:1.0.0"
        );
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
        let src = test_source();
        let (_, out, _) = run_captured("", |io| execute_with(args, &global, io, Some(&src)));
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["engineImage"], "ghcr.io/startr-trade/sutra:9.9.9");
    }

    /// The defect this seam exists to prevent: a distribution that links this library but
    /// publishes elsewhere must NOT be updated from the engine's own releases.
    /// The version compared against a release tag must be the PRODUCT's, not the embedded
    /// engine's — otherwise a distribution at 2.0.0 carrying engine 0.2.0 reports an update
    /// forever, because it is comparing the wrong number.
    #[test]
    fn the_reported_version_is_the_products_first_line_not_the_engines() {
        let block = "product 2.0.0-rc.1\nsutra   0.2.0-rc.1 (engine)";
        let product = block.lines().next().unwrap().rsplit(' ').next().unwrap();
        assert_eq!(product, "2.0.0-rc.1");
        assert!(differs(product, "v2.1.0"));
        assert!(!differs(product, "v2.0.0-rc.1"));
    }

    #[test]
    fn a_distribution_that_declared_no_source_refuses_rather_than_installing_another_product() {
        let args = SelfUpdateArgs {
            check: true,
            version: Some("v9.9.9".to_string()),
            ..SelfUpdateArgs::default()
        };
        let (code, _, err) = run_captured("", |io| {
            execute_with(args, &GlobalArgs::default(), io, None)
        });
        assert_eq!(code, crate::exit::USAGE);
        assert!(
            err.contains("does not publish through this channel"),
            "{err}"
        );
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
        let src = test_source();
        let (code, out, _) = run_captured("", |io| {
            execute_with(args, &GlobalArgs::default(), io, Some(&src))
        });
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
        let src = test_source();
        let (code, out, _) = run_captured("", |io| execute_with(args, &global, io, Some(&src)));
        assert_eq!(code, crate::exit::OK);
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["updateAvailable"], true);
        assert_eq!(payload["latest"], "v99.0.0");
        assert_eq!(payload["current"], crate::VERSION);
    }
}
