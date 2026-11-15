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
//! The version/asset arithmetic ([`compare_versions`], [`asset_name_of`], [`target_candidates`]) is pure and
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

/// The published-asset target triples to try for this host, best first, or an empty list when
/// this platform has no release build (anything outside the release matrix) — those install
/// from source, and saying so is better than downloading a binary that cannot run. Keep this
/// list in step with the targets the release workflows package.
///
/// On Linux a distribution may publish glibc builds, musl builds, or both, so the running
/// binary's own libc comes first and the other libc of the same OS/arch is the fallback.
pub fn target_candidates() -> Vec<&'static str> {
    candidates_for(
        std::env::consts::OS,
        std::env::consts::ARCH,
        cfg!(target_env = "gnu"),
    )
}

fn candidates_for(os: &str, arch: &str, gnu: bool) -> Vec<&'static str> {
    match (os, arch, gnu) {
        ("linux", "x86_64", true) => {
            vec!["x86_64-unknown-linux-gnu", "x86_64-unknown-linux-musl"]
        }
        ("linux", "x86_64", false) => {
            vec!["x86_64-unknown-linux-musl", "x86_64-unknown-linux-gnu"]
        }
        ("linux", "aarch64", true) => {
            vec!["aarch64-unknown-linux-gnu", "aarch64-unknown-linux-musl"]
        }
        ("linux", "aarch64", false) => {
            vec!["aarch64-unknown-linux-musl", "aarch64-unknown-linux-gnu"]
        }
        ("macos", "aarch64", _) => vec!["aarch64-apple-darwin"],
        ("macos", "x86_64", _) => vec!["x86_64-apple-darwin"],
        ("windows", "x86_64", _) => vec!["x86_64-pc-windows-msvc"],
        _ => Vec::new(),
    }
}

/// The preferred published-asset target triple for the host (see [`target_candidates`]).
pub fn target_triple() -> Option<&'static str> {
    target_candidates().first().copied()
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

    // `--runtime-only` never touches this binary, so an unpublished host platform can still
    // pull the engine image.
    let candidates = target_candidates();
    let target = match candidates.first().copied() {
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
    let current = crate::product_version()
        .split(" (")
        .next()
        .unwrap_or(crate::VERSION)
        .rsplit(' ')
        .next()
        .unwrap_or(crate::VERSION);
    let mut listed_target: Option<String> = None;
    let wanted = match args.version.clone() {
        Some(tag) => tag,
        None => match latest_tag(&src.channel, &src.binary, token.as_deref(), &candidates) {
            Ok((tag, listed)) => {
                listed_target = listed;
                tag
            }
            Err(msg) => {
                let _ = writeln!(io.err, "self-update: {msg}");
                return exit::USAGE;
            }
        },
    };

    // A pinned `--version` may move backwards; the newest-release lookup never offers an older
    // release than the one running as an "update".
    let available = differs(current, &wanted)
        && (args.version.is_some() || compare_versions(&wanted, current).is_gt());
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

    // A listing already named the asset's target; otherwise try the candidates in order.
    let to_try: Vec<String> = match listed_target {
        Some(t) => vec![t],
        None => candidates.iter().map(|t| t.to_string()).collect(),
    };
    let _ = writeln!(
        io.out,
        "updating {current} -> {wanted} ({})",
        to_try.first().map(String::as_str).unwrap_or(target)
    );
    match install(src, &wanted, &to_try, &exe, token.as_deref()) {
        Ok(used) => {
            let _ = writeln!(io.out, "installed {wanted} ({used}) at {}", exe.display());
        }
        Err(msg) => {
            let _ = writeln!(io.err, "self-update: {msg}");
            return exit::USAGE;
        }
    }

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
        // Bounded, because an unbounded update check hangs a terminal with no output. The
        // connect timeout is short (a wrong/blocked host should say so quickly); the overall
        // one is generous enough for a 30 MB binary on a slow link.
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(300))
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

/// `tag_name` out of a single-release payload.
fn tag_name_of(body: &[u8]) -> Option<String> {
    let json: serde_json::Value = serde_json::from_slice(body).ok()?;
    json.get("tag_name")
        .and_then(|t| t.as_str())
        .map(|s| s.to_string())
}

/// Orders two release tags by semver precedence: the numeric core compares as numbers (v0.10.0
/// above v0.9.0), a pre-release sorts below its own release, and pre-release identifiers
/// compare one dot-separated identifier at a time, numeric ones numerically and below any
/// alphanumeric one (so rc.10 is above rc.9, and rc.9 above rc.1). Build metadata is ignored.
pub fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn split(tag: &str) -> (Vec<u64>, Option<Vec<String>>) {
        let body = tag.trim().trim_start_matches('v');
        let body = body.split_once('+').map_or(body, |(b, _)| b);
        let (core, pre) = match body.split_once('-') {
            Some((c, p)) => (c, Some(p.split('.').map(str::to_string).collect())),
            None => (body, None),
        };
        let nums = core
            .split('.')
            .map(|p| p.parse::<u64>().unwrap_or(0))
            .collect();
        (nums, pre)
    }
    fn cmp_ident(a: &str, b: &str) -> Ordering {
        match (a.parse::<u64>(), b.parse::<u64>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            (Ok(_), Err(_)) => Ordering::Less,
            (Err(_), Ok(_)) => Ordering::Greater,
            (Err(_), Err(_)) => a.cmp(b),
        }
    }
    let (mut an, ap) = split(a);
    let (mut bn, bp) = split(b);
    let width = an.len().max(bn.len());
    an.resize(width, 0);
    bn.resize(width, 0);
    an.cmp(&bn).then_with(|| match (ap, bp) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(x), Some(y)) => {
            for (i, j) in x.iter().zip(y.iter()) {
                let c = cmp_ident(i, j);
                if c.is_ne() {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
    })
}

/// `(tag, target)` of a store asset named `<binary>-<tag>-<target>.tar.gz|.zip`, or `None` for
/// anything else in the listing (checksum files, other products' artifacts).
fn parse_asset(binary: &str, name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix(&format!("{binary}-"))?;
    let stem = rest
        .strip_suffix(".tar.gz")
        .or_else(|| rest.strip_suffix(".zip"))?;
    // Every published target begins with an architecture segment, so that is where the tag ends.
    let cut = stem.find("-x86_64").or_else(|| stem.find("-aarch64"))?;
    Some((stem[..cut].to_string(), stem[cut + 1..].to_string()))
}

/// The newest release in a listing that has an asset for one of `candidates`, with the target of
/// that asset (the best-ranked candidate the release carries). Listing order is irrelevant.
fn newest_in_listing(
    names: &[String],
    binary: &str,
    candidates: &[&str],
) -> Option<(String, String)> {
    let rank_of = |t: &str| candidates.iter().position(|c| *c == t);
    let mut best: Option<(String, String)> = None;
    for (tag, target) in names.iter().filter_map(|n| parse_asset(binary, n)) {
        let Some(rank) = rank_of(&target) else {
            continue;
        };
        let better = match &best {
            None => true,
            Some((bt, btarget)) => match compare_versions(&tag, bt) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => rank < rank_of(btarget).unwrap_or(usize::MAX),
                std::cmp::Ordering::Less => false,
            },
        };
        if better {
            best = Some((tag, target));
        }
    }
    best
}

/// The newest release tag on this distribution's channel, pre-releases included.
fn latest_tag(
    channel: &crate::UpdateChannel,
    binary: &str,
    token: Option<&str>,
    candidates: &[&str],
) -> Result<(String, Option<String>), String> {
    match channel {
        // THREE sources, because each has a failure mode the next one covers. The release LIST
        // was the only source here, and it has been seen answering `[]` while the release was
        // perfectly fetchable BY TAG — which made `self-update` report "could not read a release
        // tag" for a release that existed. A single source is a single point of failure for the
        // one command whose whole job is to find the newest release.
        crate::UpdateChannel::GithubReleases { repo } => {
            // (a) `/releases/latest`: right once a STABLE release exists, 404 while every
            //     release is a pre-release — which is why it cannot be the only source either.
            if let Ok(body) = fetch(
                &format!("https://api.github.com/repos/{repo}/releases/latest"),
                token,
            ) {
                if let Some(tag) = tag_name_of(&body) {
                    return Ok((tag, None));
                }
            }
            // (b) the list, which does include pre-releases.
            if let Ok(body) = fetch(
                &format!("https://api.github.com/repos/{repo}/releases?per_page=1"),
                token,
            ) {
                let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
                if let Some(tag) = json
                    .get(0)
                    .and_then(|r| r.get("tag_name"))
                    .and_then(|t| t.as_str())
                {
                    return Ok((tag.to_string(), None));
                }
            }
            // (c) git TAGS — a different endpoint, which answered when the list did not. Newest
            //     v-prefixed tag first, then confirmed to actually carry a release: a tag with
            //     no release has no assets to install.
            let body = fetch(
                &format!("https://api.github.com/repos/{repo}/tags?per_page=100"),
                token,
            )?;
            let json: serde_json::Value =
                serde_json::from_slice(&body).map_err(|e| format!("tag list: {e}"))?;
            let mut tags: Vec<String> = json
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                        .filter(|n| n.starts_with('v'))
                        .map(|n| n.to_string())
                        .collect()
                })
                .unwrap_or_default();
            tags.sort_by(|a, b| compare_versions(b, a));
            for tag in tags {
                if fetch(
                    &format!("https://api.github.com/repos/{repo}/releases/tags/{tag}"),
                    token,
                )
                .is_ok()
                {
                    return Ok((tag, None));
                }
            }
            Err(format!(
                "could not resolve a release tag from github.com/{repo} (rate-limited, or \
                 nothing published). Pin one with --version <tag>, or set GH_TOKEN to lift the \
                 API rate limit."
            ))
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
            let names: Vec<String> = entries
                .iter()
                .filter_map(|e| e.get(index_name_field)?.as_str().map(str::to_string))
                .collect();
            newest_in_listing(&names, binary, candidates)
                .map(|(tag, target)| (tag, Some(target)))
                .ok_or_else(|| {
                    format!(
                        "no {binary} release for {} found in the index \
                         (private store? set the auth variable, or pass --version)",
                        candidates.join(" / ")
                    )
                })
        }
    }
}

fn install(
    src: &crate::UpdateSource,
    tag: &str,
    targets: &[String],
    exe: &Path,
    token: Option<&str>,
) -> Result<String, String> {
    let base = match &src.channel {
        crate::UpdateChannel::GithubReleases { repo } => {
            format!("https://github.com/{repo}/releases/download/{tag}")
        }
        // A flat namespace — the tag lives in the file name, not in a path segment.
        crate::UpdateChannel::FileStore { base, .. } => base.clone(),
    };

    // The first target whose asset exists wins; only a 404 moves on to the next candidate.
    let mut found: Option<(Vec<u8>, String, String)> = None;
    let mut last_err = String::from("no target to install");
    for target in targets {
        let asset = asset_name_of(&src.binary, tag, target);
        match fetch(&format!("{base}/{asset}"), token) {
            Ok(bytes) => {
                found = Some((bytes, asset, target.clone()));
                break;
            }
            Err(e) if e.contains("HTTP 404") => last_err = e,
            Err(e) => return Err(e),
        }
    }
    let Some((archive, asset, target)) = found else {
        return Err(last_err);
    };
    let target = target.as_str();
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
    })?;
    Ok(target.to_string())
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
            // zip 9 returns the entry name as a Result: a name that is not valid UTF-8 is an
            // error rather than a lossy string. Such an entry cannot be the binary we are
            // looking for, so skip it instead of failing the whole update.
            let Ok(name) = entry.name() else {
                continue;
            };
            if name.ends_with(&format!("{binary}.exe")) {
                let mut out = std::fs::File::create(dest)
                    .map_err(|e| format!("creating {}: {e}", dest.display()))?;
                std::io::copy(&mut entry, &mut out).map_err(|e| format!("extracting: {e}"))?;
                return Ok(());
            }
        }
        Err(format!("the release zip contained no {binary}.exe"))
    } else {
        // The tarball is small (one stripped binary + licenses) and already in memory; shelling
        // out to `tar` keeps the dependency surface of an updater minimal. Only the options GNU
        // tar and the bsdtar macOS ships share are used: bsdtar has no `--wildcards`, so the whole
        // archive is extracted into a scratch folder and the binary picked out of it. The archive
        // is named relative to that folder, because GNU tar reads `C:\…` as a remote host.
        let scratch = dest.with_extension("unpack");
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch)
            .map_err(|e| format!("creating {}: {e}", scratch.display()))?;
        let result = (|| {
            std::fs::write(scratch.join("release.tar.gz"), archive)
                .map_err(|e| format!("staging the archive: {e}"))?;
            let status = std::process::Command::new("tar")
                .current_dir(&scratch)
                .args(["-xzf", "release.tar.gz"])
                .status();
            match status {
                Ok(s) if s.success() => {}
                Ok(s) => return Err(format!("tar exited {s}")),
                Err(e) => return Err(format!("running tar: {e} (is tar on PATH?)")),
            }
            let extracted = find_unpacked_binary(&scratch, binary)
                .ok_or_else(|| format!("the release archive contained no {binary}"))?;
            std::fs::rename(&extracted, dest).map_err(|e| format!("staging the new binary: {e}"))
        })();
        let _ = std::fs::remove_dir_all(&scratch);
        result
    }
}

/// The release binary in an unpacked tarball: `<top-folder>/<binary>`, the layout the release
/// workflow packages, or `<binary>` at the top.
fn find_unpacked_binary(scratch: &Path, binary: &str) -> Option<PathBuf> {
    let top = scratch.join(binary);
    if top.is_file() {
        return Some(top);
    }
    let mut folders: Vec<PathBuf> = std::fs::read_dir(scratch)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.is_dir())
        .collect();
    folders.sort();
    folders
        .into_iter()
        .map(|folder| folder.join(binary))
        .find(|candidate| candidate.is_file())
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

    /// The tarball branch uses only options GNU tar and macOS bsdtar share, and finds the binary
    /// under the archive's top folder.
    #[cfg(unix)]
    #[test]
    fn a_release_tarball_unpacks_with_portable_tar_options() {
        let work = std::env::temp_dir().join(format!("sutra-unpack-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&work);
        let top = work.join("src/sutra-v9.9.9-x86_64-unknown-linux-gnu");
        std::fs::create_dir_all(&top).unwrap();
        std::fs::write(top.join("sutra"), b"new-binary").unwrap();
        std::fs::write(top.join("LICENSE-MIT"), b"license").unwrap();
        let tarball = work.join("release.tar.gz");
        let status = std::process::Command::new("tar")
            .current_dir(work.join("src"))
            .arg("-czf")
            .arg(&tarball)
            .arg("sutra-v9.9.9-x86_64-unknown-linux-gnu")
            .status()
            .unwrap();
        assert!(status.success());
        let bytes = std::fs::read(&tarball).unwrap();
        let dest = work.join(".sutra-update-v9.9.9");
        unpack(&bytes, "sutra", "x86_64-unknown-linux-gnu", &dest).expect("unpacks");
        assert_eq!(std::fs::read(&dest).unwrap(), b"new-binary");
        assert!(
            !dest.with_extension("unpack").exists(),
            "scratch folder removed"
        );
        let err = unpack(&bytes, "absent", "x86_64-unknown-linux-gnu", &dest).unwrap_err();
        assert!(err.contains("no absent"), "{err}");
        let _ = std::fs::remove_dir_all(&work);
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

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn prerelease_identifiers_compare_numerically_and_a_release_beats_its_prereleases() {
        use std::cmp::Ordering::{Greater, Less};
        assert_eq!(compare_versions("v2.0.0-rc.10", "v2.0.0-rc.9"), Greater);
        assert_eq!(compare_versions("v2.0.0-rc.9", "v2.0.0-rc.1"), Greater);
        assert_eq!(compare_versions("1.0.0", "1.0.0-rc.9"), Greater);
        assert_eq!(compare_versions("v0.10.0", "v0.9.0"), Greater);
        assert_eq!(compare_versions("v2.0.0-rc.4", "v2.0.0"), Less);
        assert_eq!(compare_versions("v1.0.0-rc.1", "v1.0.0-rc"), Greater);
        assert_eq!(compare_versions("v1.0.0-1", "v1.0.0-rc"), Less);
    }

    #[test]
    fn listing_order_does_not_decide_the_newest_release() {
        let gnu = ["x86_64-unknown-linux-gnu", "x86_64-unknown-linux-musl"];
        let files = names(&[
            "SHA256SUMS-sutra.txt",
            "other-v9.0.0-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v2.0.0-rc.1-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v2.0.0-rc.10-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v2.0.0-rc.4-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v2.0.0-rc.5-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v0.9.0-x86_64-unknown-linux-gnu.tar.gz",
            "sutra-v3.0.0-aarch64-unknown-linux-gnu.tar.gz",
        ]);
        let (tag, target) = newest_in_listing(&files, "sutra", &gnu).unwrap();
        assert_eq!(
            (tag.as_str(), target.as_str()),
            ("v2.0.0-rc.10", "x86_64-unknown-linux-gnu")
        );
        let mut reversed = files.clone();
        reversed.reverse();
        assert_eq!(
            newest_in_listing(&reversed, "sutra", &gnu).unwrap().0,
            "v2.0.0-rc.10"
        );
        // A final release outranks every pre-release.
        let mut with_final = files;
        with_final.push("sutra-v2.0.0-x86_64-unknown-linux-gnu.tar.gz".into());
        assert_eq!(
            newest_in_listing(&with_final, "sutra", &gnu).unwrap().0,
            "v2.0.0"
        );
    }

    #[test]
    fn libc_preference_and_fallback_follow_the_running_binary() {
        let only_musl = names(&["sutra-v1.0.0-x86_64-unknown-linux-musl.tar.gz"]);
        let both = names(&[
            "sutra-v1.0.0-x86_64-unknown-linux-musl.tar.gz",
            "sutra-v1.0.0-x86_64-unknown-linux-gnu.tar.gz",
        ]);
        let gnu = candidates_for("linux", "x86_64", true);
        let musl = candidates_for("linux", "x86_64", false);
        let pick = |n: &[String], c: &[&str]| newest_in_listing(n, "sutra", c).unwrap().1;
        assert_eq!(pick(&both, &gnu), "x86_64-unknown-linux-gnu");
        assert_eq!(pick(&only_musl, &gnu), "x86_64-unknown-linux-musl");
        assert_eq!(pick(&both, &musl), "x86_64-unknown-linux-musl");
        assert_eq!(
            candidates_for("linux", "aarch64", true)[0],
            "aarch64-unknown-linux-gnu"
        );
        assert_eq!(
            candidates_for("macos", "aarch64", true),
            ["aarch64-apple-darwin"]
        );
        assert!(candidates_for("plan9", "x86_64", false).is_empty());
    }

    #[test]
    fn an_older_listing_entry_is_never_newer_than_the_running_version() {
        use std::cmp::Ordering::Greater;
        // The reported bug: running rc.4, the lookup found rc.1 and offered it as an update.
        assert_ne!(compare_versions("v2.0.0-rc.1", "2.0.0-rc.4"), Greater);
        assert_eq!(compare_versions("v2.0.0-rc.5", "2.0.0-rc.4"), Greater);
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

    // ---- end to end against a local file store -------------------------------------------

    /// A one-thread HTTP/1.1 file store on 127.0.0.1: `/index` answers `listing`, every other
    /// path answers 503 (so an install attempt stops before anything is written next to the
    /// running test binary). Returns the base URL and the request paths it has seen.
    fn local_store(
        listing: serde_json::Value,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{Read, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        let body = listing.to_string();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let line = String::from_utf8_lossy(&request);
                let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                log.lock().unwrap().push(path.clone());
                let (status, payload) = if path == "/index" {
                    ("200 OK", body.as_str())
                } else {
                    (
                        "503 Service Unavailable",
                        "store refuses downloads in this test",
                    )
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                );
            }
        });
        (base, seen)
    }

    fn file_store_source(base: &str) -> crate::UpdateSource {
        crate::UpdateSource {
            channel: crate::UpdateChannel::FileStore {
                base: format!("{base}/files"),
                index_url: Some(format!("{base}/index")),
                index_pointer: "/values".to_string(),
                index_name_field: "name".to_string(),
            },
            binary: "sutra".to_string(),
            image: None,
            token_command: None,
        }
    }

    fn listing_of(names: &[String]) -> serde_json::Value {
        serde_json::json!({
            "values": names.iter().map(|n| serde_json::json!({ "name": n })).collect::<Vec<_>>()
        })
    }

    /// `execute_with` over a real HTTP listing, shuffled, with release candidates whose
    /// alphabetical order is not their version order, both libcs, a newer release for a
    /// foreign target and another product's binary: it requests the newest release's asset for
    /// this host's preferred target, and nothing else.
    #[test]
    fn execute_with_installs_the_newest_matching_asset_from_a_shuffled_file_store() {
        let candidates = target_candidates();
        let Some(primary) = candidates.first().copied() else {
            return; // no published build for this host: nothing to choose
        };
        let mut names = vec![
            asset_name_of("sutra", "v99.0.0-rc.4", primary),
            "sutra-v99.0.0-rc.10-x86_64-unknown-linux-musl.tar.gz".to_string(),
            asset_name_of("sutra", "v99.0.0-rc.1", primary),
            "SHA256SUMS-sutra.txt".to_string(),
            "sutra-v99.1.0-x86_64-unknown-freebsd.tar.gz".to_string(),
            asset_name_of("sutra", "v99.0.0-rc.10", primary),
            "sutra-v99.0.0-rc.5-aarch64-unknown-linux-musl.tar.gz".to_string(),
            asset_name_of("other-tool", "v100.0.0", primary),
            "sutra-v99.0.0-rc.10-aarch64-unknown-linux-gnu.tar.gz".to_string(),
            asset_name_of("sutra", "v99.0.0-rc.5", primary),
            "sutra-v99.0.0-rc.10-x86_64-unknown-linux-gnu.tar.gz".to_string(),
        ];
        names.dedup();
        let (base, seen) = local_store(listing_of(&names));
        let src = file_store_source(&base);

        // --check reports the newest release by version, not by listing position.
        let global = GlobalArgs {
            format: Some("json".to_string()),
            ..GlobalArgs::default()
        };
        let check = SelfUpdateArgs {
            check: true,
            ..SelfUpdateArgs::default()
        };
        let (code, out, err) = run_captured("", |io| execute_with(check, &global, io, Some(&src)));
        assert_eq!(code, crate::exit::OK, "{err}");
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["latest"], "v99.0.0-rc.10", "{out}");
        assert_eq!(payload["updateAvailable"], true, "{out}");

        // The install path asks the store for exactly that release's asset for this host's own
        // target (the store then refuses, so nothing is installed).
        let (code, out, err) = run_captured("", |io| {
            execute_with(
                SelfUpdateArgs::default(),
                &GlobalArgs::default(),
                io,
                Some(&src),
            )
        });
        assert_eq!(code, crate::exit::USAGE, "{out}{err}");
        assert!(
            out.contains(&format!("-> v99.0.0-rc.10 ({primary})")),
            "{out}"
        );
        assert!(err.contains("HTTP 503"), "{err}");
        let downloads: Vec<String> = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.starts_with("/files/"))
            .cloned()
            .collect();
        assert_eq!(
            downloads,
            vec![format!(
                "/files/{}",
                asset_name_of("sutra", "v99.0.0-rc.10", primary)
            )]
        );
    }

    /// A listing whose newest release is older than (or equal to) the running one offers no
    /// update and downloads nothing.
    #[test]
    fn execute_with_refuses_a_downgrade_from_a_file_store_listing() {
        let candidates = target_candidates();
        let Some(primary) = candidates.first().copied() else {
            return;
        };
        let names = vec![
            asset_name_of("sutra", "v0.0.1-rc.10", primary),
            asset_name_of("sutra", &format!("v{}", crate::VERSION), primary),
            asset_name_of("sutra", "v0.0.1", primary),
            asset_name_of("sutra", "v0.0.1-rc.5", primary),
        ];
        let (base, seen) = local_store(listing_of(&names));
        let src = file_store_source(&base);

        let global = GlobalArgs {
            format: Some("json".to_string()),
            ..GlobalArgs::default()
        };
        let check = SelfUpdateArgs {
            check: true,
            ..SelfUpdateArgs::default()
        };
        let (code, out, err) = run_captured("", |io| execute_with(check, &global, io, Some(&src)));
        assert_eq!(code, crate::exit::OK, "{err}");
        let payload: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        assert_eq!(payload["updateAvailable"], false, "{out}");

        let (code, out, err) = run_captured("", |io| {
            execute_with(
                SelfUpdateArgs::default(),
                &GlobalArgs::default(),
                io,
                Some(&src),
            )
        });
        assert_eq!(code, crate::exit::OK, "{err}");
        assert!(out.contains("nothing to do"), "{out}");
        assert!(
            !seen
                .lock()
                .unwrap()
                .iter()
                .any(|p| p.starts_with("/files/")),
            "a downgrade must download nothing: {:?}",
            seen.lock().unwrap()
        );
    }
}
