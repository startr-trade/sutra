//! Test-fixture container + network reaping for the Rust integration suites, plus — behind the
//! `conformance` feature — the reusable end-to-end harness ([`conformance`]).
//!
//! # Why this exists
//!
//! testcontainers-rs **0.25** ships **no ryuk reaper** (unlike the reference testcontainers
//! libraries — there is no sidecar that reaps by session label when the client connection
//! drops, so nothing survives a `SIGKILL`). The crate's *only* automatic cleanup is the
//! `Drop` impl on the `Container`/`ContainerAsync` handle, which force-removes the container
//! when the handle is dropped (the default `TESTCONTAINERS_COMMAND=remove`). Its opt-in
//! `watchdog` cargo feature only intercepts `SIGTERM`/`SIGINT`/`SIGQUIT` — never a normal
//! process exit — and it is not enabled here.
//!
//! Our suites deliberately keep **one container per test binary** alive for the whole run,
//! parking the handle in a `static OnceLock<(Container, u16)>`. Rust never drops `static`s at
//! process exit, so the handle's `Drop` never runs — and with no ryuk, the container leaks
//! after the test process is gone (~40-50 stragglers per full `cargo test --workspace`,
//! documented operator pain).
//!
//! # The mechanism
//!
//! Fixtures call [`reap_on_exit`] with each container's id right after `start()`. That records
//! the id and, on the first call in the process, installs a single [`libc::atexit`] hook. When
//! the test process terminates **normally** (libtest returning from `main`, or `exit()`), the
//! hook force-removes every registered container in one `docker rm -f` call — so
//! `cargo test -p <crate>` leaves zero fixtures behind within a couple of seconds of exit.
//!
//! # Residual case (SIGKILL / crash)
//!
//! `atexit(3)` handlers — like every Rust destructor — do **not** run on `SIGKILL`, a hard
//! crash, or `kill -9`. Reaping those is exactly what ryuk would do, and ryuk does not exist
//! in this crate version. For that residual case only, run `scripts/dev-docker-cleanup.sh`,
//! which force-removes leaked test-image containers older than a cutoff.

use std::process::{Command, Stdio};
use std::sync::{Mutex, Once};

/// The end-to-end conformance harness — postgres/broker/engine-container fixtures, the
/// variant composer, the host + broker recorders, and the tier-3 k8s plumbing.
///
/// Feature-gated (`conformance`) because it pulls testcontainers, lapin and kube-rs; the
/// reaper above is what the crate's other consumers dev-depend on, and their builds stay light.
#[cfg(feature = "conformance")]
pub mod conformance;

/// Container ids registered by fixtures in this process, force-removed by the atexit hook.
static REGISTRY: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Docker network names registered for removal by the atexit hook — reaped AFTER the containers
/// (a network cannot be removed while a container is still attached).
static NETWORK_REGISTRY: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Installs the atexit hook exactly once per process.
static INSTALL: Once = Once::new();

/// Install the process-wide atexit reaper exactly once.
fn install_reaper() {
    INSTALL.call_once(|| {
        // SAFETY: `reap` is `extern "C"` and touches only two `Mutex<Vec<String>>` plus a
        // `Command` spawn — all valid during libc's atexit unwind, which runs after main returns
        // and all libtest threads have joined.
        unsafe {
            libc::atexit(reap);
        }
    });
}

/// Register a testcontainers fixture container for force-removal when THIS test process
/// exits normally. Idempotent per id; installs the process-wide reaper on first call.
///
/// Call it immediately after `start()`, while the `Container` handle is still in scope:
///
/// ```text
/// let container = Postgres::default().with_tag("16-alpine").start().expect("start pg");
/// sutra_testkit::reap_on_exit(container.id());
/// ```
///
/// A container held only in a `static` (the shared-fixture pattern) would otherwise never be
/// removed, because its `Drop` never runs. Registering the id here closes that gap for every
/// normal test-process exit.
pub fn reap_on_exit(container_id: impl Into<String>) {
    let id = container_id.into();
    if id.is_empty() {
        return;
    }
    if let Ok(mut ids) = REGISTRY.lock() {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    install_reaper();
}

/// Register a docker NETWORK (e.g. the per-process shared conformance network) for removal when
/// THIS test process exits normally. Idempotent per name; installs the process-wide reaper on
/// first call. The network is removed AFTER the registered containers (docker refuses to remove a
/// network with an attached container), so a per-run `sutra-conf-net-<pid>` no longer leaks and
/// exhausts docker's address pools over many runs. Register it once, when the network is created.
pub fn reap_network_on_exit(network: impl Into<String>) {
    let name = network.into();
    if name.is_empty() {
        return;
    }
    if let Ok(mut nets) = NETWORK_REGISTRY.lock() {
        if !nets.contains(&name) {
            nets.push(name);
        }
    }
    install_reaper();
}

/// atexit hook: force-remove every registered container, then every registered network.
/// Best-effort — the process is on its way out, so already-gone objects or a missing `docker`
/// binary are silently ignored. Containers are reaped FIRST so their networks are then free.
extern "C" fn reap() {
    let lock = |m: &Mutex<Vec<String>>| match m.lock() {
        Ok(v) => v.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let ids = lock(&REGISTRY);
    if !ids.is_empty() {
        let _ = Command::new("docker")
            .arg("rm")
            .arg("-f")
            .args(&ids)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let nets = lock(&NETWORK_REGISTRY);
    if !nets.is_empty() {
        let _ = Command::new("docker")
            .arg("network")
            .arg("rm")
            .args(&nets)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
