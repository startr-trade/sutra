//! Shared primitives for the conformance suites: repo paths, unique names, the shared docker
//! network, filesystem perms for the mounted archive dir, and async polling.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// A thread-safe sink of received message bodies (HTTP callback or broker deliveries).
///
/// Bodies are kept in ONE flat list rather than keyed by correlation id: every test uses a
/// UNIQUE correlation ref, so filtering with `contains(ref)` yields exactly that ref's set.
#[derive(Clone, Default)]
pub struct Recorder {
    entries: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    pub fn record(&self, body: String) {
        self.entries.lock().unwrap().push(body);
    }

    /// Every recorded body containing `needle`.
    pub fn matching(&self, needle: &str) -> Vec<String> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.contains(needle))
            .cloned()
            .collect()
    }

    /// How many recorded bodies contain `needle`.
    pub fn count(&self, needle: &str) -> usize {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.contains(needle))
            .count()
    }
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A process-wide monotonic counter used to make container/db/network names unique.
pub fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// A short unique-enough token (no uuid dependency), unique within and across processes.
pub fn short_id() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        next_seq(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// The repository root — the directory that holds `rust/Cargo.toml`, `examples/` and
/// `deploy/`.
///
/// `SUTRA_REPO_ROOT` wins when set. That override is what makes the harness usable from a
/// suite crate in ANOTHER workspace: the compile-time fallback below resolves the tree this
/// testkit was compiled from, which for a composed extension repo is the engine submodule,
/// not the caller's repo.
///
/// Fallback: `CARGO_MANIFEST_DIR` is `<repo>/rust/crates/sutra-testkit`, so the repo root is
/// three ancestors up (sutra-testkit → crates → rust → repo).
pub fn repo_root() -> PathBuf {
    if let Ok(path) = std::env::var("SUTRA_REPO_ROOT") {
        if !path.trim().is_empty() {
            return PathBuf::from(path.trim());
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root above rust/crates/sutra-testkit")
        .to_path_buf()
}

/// The examples tree the suites assemble packages from — `SUTRA_EXAMPLES_DIR` when set, else
/// `<repo_root>/examples`. An out-of-workspace suite whose examples do NOT sit beside the
/// engine's (an extension repo keeping its own `examples/`) points the harness here without
/// having to move its tree.
pub fn examples_dir() -> PathBuf {
    if let Ok(path) = std::env::var("SUTRA_EXAMPLES_DIR") {
        if !path.trim().is_empty() {
            return PathBuf::from(path.trim());
        }
    }
    repo_root().join("examples")
}

/// The suite a fixture belongs to, derived from its name: everything before the first `-`.
/// Fixture names are already suite-prefixed by convention (`mr`, `mr-accounts`, `mr-0`;
/// `mt`, `mt-accounts`; `mtmx`; `orders`; …), so this needs no call-site changes.
pub fn suite_of(name: &str) -> &str {
    name.split('-').next().unwrap_or(name)
}

/// A docker network scoped to ONE suite. Suites must not share a network: fixtures address each
/// other by container ALIAS (`rabbit`, `rabbitmq`, …), and two suites deploying the same example
/// then resolve the same alias — which is a real bug, not a theoretical one. `tc_money_transfer`
/// and `tc_multi_replica` both deploy the money-transfer package, whose `transfer-queue` channel
/// hardcodes `host: rabbit` / `queue: transfer-queue-q`; on one shared network the money-transfer
/// engine attached to the multi-replica suite's broker, became a SECOND consumer of a channel
/// declared `singleton: true`, and drained the transfers into its own (different) accounts DB —
/// so the multi-replica ledger never moved. It passed in isolation and failed in-suite, at any
/// thread count, which is exactly the signature of cross-fixture interference rather than load.
///
/// Keying the network by suite makes that impossible by construction instead of by convention.
pub fn network_for(suite: &str) -> &'static str {
    static NETS: OnceLock<std::sync::Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let nets = NETS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut nets = nets.lock().expect("network registry");
    if let Some(name) = nets.get(suite) {
        return name;
    }
    let name: &'static str =
        Box::leak(format!("sutra-conf-net-{}-{suite}", std::process::id()).into_boxed_str());
    // A1: the per-process network testcontainers auto-creates leaks unless reaped — the atexit
    // reaper removes it (after its containers) so many runs don't exhaust docker's address pools.
    crate::reap_network_on_exit(name);
    nets.insert(suite.to_string(), name);
    name
}

/// A fresh, world-readable temp directory. The engine container runs unprivileged (uid 10001)
/// and must be able to list a bind-mounted deployments dir, so the dir is 0755.
pub fn world_readable_temp_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}-{}", short_id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    set_mode(&dir, 0o755);
    dir
}

/// Make every entry of an archives directory world-readable (dir 0755, files 0644) so the
/// unprivileged engine container can read the mounted `.sutra` archives.
pub fn make_world_readable(dir: &Path) {
    set_mode(dir, 0o755);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                make_world_readable(&path);
            } else {
                set_mode(&path, 0o644);
            }
        }
    }
}

fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
}

/// Poll `probe` roughly 10×/s until it returns `true` or `timeout` elapses.
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if probe().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// An ISO-8601 UTC timestamp `secs` before now (civil-from-days; no date dependency). A
/// staleness rule rejects an inbound message whose creation timestamp is more than 5 minutes
/// old (rule `r_stale` → E990), so such an intake needs a timestamp computed at call time.
pub fn iso_utc_now_minus_secs(secs: u64) -> String {
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(secs);
    let days = (epoch / 86_400) as i64;
    let rem = epoch % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Recursively copy a directory tree (used by the k8s hot-deploy suite to stage a per-run
/// package copy before editing + re-packaging).
pub fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dest dir");
    for entry in std::fs::read_dir(from).expect("read src dir").flatten() {
        let src = entry.path();
        let dst = to.join(entry.file_name());
        if src.is_dir() {
            copy_tree(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).expect("copy file");
        }
    }
}
