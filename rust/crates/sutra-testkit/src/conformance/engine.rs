//! The engine-under-test container — one implementation, every tier-2 suite uses it.
//!
//! Archive mode only.
//! Each example's `deployments-src/<package-dir>` is sealed on the host into one `.sutra`
//! archive via `sutra_loader::package::assemble_dir` (unmodified — the shipped archive is what we
//! certify), the output directory is bind-mounted read-only at `/etc/sutra/deployments` with
//! `SUTRA_DEPLOYMENTS_DIR` set, and the `sutra-rust-engine:dev` image runs with canonical
//! `SUTRA_*` env against a per-test PostgreSQL container. The RLS-bypass boot refusal is
//! relaxed (`SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED=false`) because the harness Postgres
//! role is a BYPASSRLS superuser; the engine's own `rls_bypass_it` suite proves the
//! enforcement itself.
//!
//! N-lane reruns: the container's shard-router lane count is the ONE boot-side knob a suite
//! never has to know about — [`conformance_shards`] reads `SUTRA_CONFORMANCE_SHARDS` once and
//! every [`EngineBuilder`] starts there, so the whole black-box suite re-runs verbatim on a
//! genuinely N-lane engine (`SUTRA_CONFORMANCE_SHARDS=4 cargo test -p sutra-conformance --
//! --ignored --skip k8s_`). Unset injects NOTHING — the default run is byte-identical.
//!
//! Container-to-container reachability: engine + postgres + broker share one docker network
//! ([`util::network_for`]); the engine reaches postgres by the container name
//! this helper set (fed through `SUTRA_DATASOURCE_URL`) and the broker by the alias baked into
//! the example's `channels.yaml` (the broker container is named to match). Host-side clients
//! reach every container by its dynamically mapped host port.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use testcontainers::core::{AccessMode, Host, IntoContainerPort, Mount};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

use super::compose;
use super::util;

/// The default engine image (overridable via `SUTRA_ENGINE_IMAGE`, e.g. for a registry ref).
pub const DEFAULT_IMAGE: &str = "sutra-rust-engine:dev";
/// The canonical in-container deployments directory (the archive source).
pub const CONTAINER_DEPLOYMENTS_DIR: &str = "/etc/sutra/deployments";
/// The engine's in-container HTTP port (the image default).
pub const HTTP_PORT: u16 = 8080;
/// The engine's shard-router lane-count env name (`sutra.engine.shards`).
pub const SHARDS_ENV: &str = "SUTRA_ENGINE_SHARDS";
/// The harness-side N-lane rerun knob: the lane count EVERY engine container this process
/// boots runs at, unless the suite pins its own with [`EngineBuilder::shards`]. Unset (the
/// default) injects NOTHING, so the container boots at its own default of one lane.
pub const CONFORMANCE_SHARDS_ENV: &str = "SUTRA_CONFORMANCE_SHARDS";

/// The effective engine image: `SUTRA_ENGINE_IMAGE` when set, else [`DEFAULT_IMAGE`].
pub fn engine_image_ref() -> String {
    std::env::var("SUTRA_ENGINE_IMAGE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string())
}

/// The run-wide lane count from [`CONFORMANCE_SHARDS_ENV`], or `None` when unset/unusable.
///
/// This is the WHOLE N-lane seam for the black-box suites (execution scale-out §8): the
/// container boot is the only thing that changes, so the entire docker conformance suite
/// re-runs VERBATIM — same tests, same expectations — against a genuinely N-lane engine:
///
/// ```text
/// SUTRA_CONFORMANCE_SHARDS=4 cargo test -p sutra-conformance -- --ignored --skip k8s_
/// ```
///
/// A garbage or sub-1 value is ignored rather than fatal: the knob is a rerun lane, and an
/// unusable value must degrade to the default run, never wedge a suite in a half-configured
/// state.
pub fn conformance_shards() -> Option<u32> {
    parse_shards(std::env::var(CONFORMANCE_SHARDS_ENV).ok().as_deref())
}

/// [`conformance_shards`]'s pure half: `Some(n)` for an integer ≥ 1, `None` otherwise.
fn parse_shards(raw: Option<&str>) -> Option<u32> {
    raw.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n >= 1)
}

/// Split a docker image reference into `(name, tag)` for [`GenericImage::new`]. Handles a
/// registry-qualified reference whose host carries a port colon (`localhost:5000/x:tag`).
fn image_name_tag(reference: &str) -> (String, String) {
    let last_slash = reference.rfind('/').map(|i| i + 1).unwrap_or(0);
    match reference[last_slash..].rfind(':') {
        Some(rel) => {
            let abs = last_slash + rel;
            (
                reference[..abs].to_string(),
                reference[abs + 1..].to_string(),
            )
        }
        None => (reference.to_string(), "latest".to_string()),
    }
}

/// Seal every package dir of an example into `.sutra` archives in a fresh world-readable
/// directory, and return that directory (ready to bind-mount). `example_rel` is relative
/// to `<repo>/examples` (e.g. `"money-transfer"`).
///
/// Single-variant examples still commit `deployments-src/<package-dir>/` directly.
/// Multi-variant examples (the DRY-variants convention) instead commit `shared/` + one
/// `variants/<package-dir>/` overlay per variant; each is composed (see
/// [`compose::compose_variant`]) into a standalone package dir before sealing.
pub fn assemble_example(example_rel: &str) -> PathBuf {
    let example_dir = util::examples_dir().join(example_rel);
    let out = util::world_readable_temp_dir("sutra-archives");
    let legacy_src = example_dir.join("deployments-src");
    let package_dirs: Vec<PathBuf> = if legacy_src.is_dir() {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(&legacy_src)
            .unwrap_or_else(|e| panic!("read {} deployments-src: {e}", legacy_src.display()))
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        dirs
    } else {
        let variants_dir = example_dir.join("variants");
        let mut names: Vec<String> = std::fs::read_dir(&variants_dir)
            .unwrap_or_else(|e| panic!("read {} variants: {e}", variants_dir.display()))
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
            .iter()
            .map(|name| compose::compose_variant(&example_dir, name))
            .collect()
    };
    assert!(
        !package_dirs.is_empty(),
        "no package dirs for example {example_rel} (checked deployments-src/ and variants/ under {})",
        example_dir.display()
    );
    for package_dir in &package_dirs {
        sutra_loader::package::assemble_dir(
            package_dir,
            &out,
            &sutra_loader::PackageOptions::default(),
        )
        .unwrap_or_else(|e| panic!("assemble {}: {e}", package_dir.display()));
    }
    util::make_world_readable(&out);
    out
}

/// A per-test PostgreSQL fixture on the shared network. The engine reaches it at
/// `<container_name>:5432` (db/user/pass = `postgres`); host clients use `host_port`.
///
/// Drop semantics are [`EngineHandle`]'s — park it in the suite's `static` fixture, never in a
/// value a tokio worker can drop.
pub struct PgFixture {
    /// The live testcontainers handle. Held so the container outlives the fixture; see
    /// [`EngineHandle::container`] for why it must not be dropped on a runtime thread.
    pub container: Container<Postgres>,
    pub container_name: String,
    pub host_port: u16,
}

/// Start `postgres:16-alpine` on the shared network with a unique container name.
pub fn start_postgres(name_hint: &str) -> PgFixture {
    let container_name = format!("{name_hint}-pg-{}", util::short_id());
    let container = Postgres::default()
        .with_tag("16-alpine")
        .with_network(util::network_for(util::suite_of(name_hint)))
        .with_container_name(&container_name)
        .start()
        .expect("start postgres:16-alpine (docker required)");
    crate::reap_on_exit(container.id());
    let host_port = container.get_host_port_ipv4(5432).expect("mapped 5432");
    PgFixture {
        container,
        container_name,
        host_port,
    }
}

/// The engine datasource URL for a [`PgFixture`] (inline creds, native `postgres://` scheme —
/// `SUTRA_DATASOURCE_URL` accepts only the native driver schemes and fails closed on any other).
pub fn datasource_url(pg: &PgFixture) -> String {
    format!(
        "postgres://postgres:postgres@{}:5432/postgres",
        pg.container_name
    )
}

/// A running engine container. Kept alive for the fixture's lifetime; the atexit reaper
/// ([`crate::reap_on_exit`]) removes it on process exit.
///
/// # Drop semantics — park it in a `static`, never on a tokio worker
///
/// `container` is a testcontainers **sync** [`Container`], whose `Drop` runs a BLOCKING
/// `docker rm -f` (the crate's only automatic cleanup — 0.25 ships no ryuk). Dropping it while
/// a tokio worker thread is driving the runtime panics with "Cannot drop a runtime in a context
/// where blocking is not allowed" and takes the suite with it. The suites therefore build every
/// fixture on a dedicated `std::thread` and park the handle in a `static OnceLock`, which Rust
/// never drops at process exit — so `Drop` is not what cleans up here. The registration
/// [`start`](EngineBuilder::start) makes with the atexit reaper is, and it is what a suite
/// relies on. Do not move a handle into an async task, a `tokio::spawn`, or any value a
/// runtime worker can drop.
pub struct EngineHandle {
    /// The live testcontainers handle — held ONLY to keep the container running (see the type
    /// docs before moving it anywhere).
    pub container: Container<GenericImage>,
    /// The dynamically mapped host port for the engine's HTTP endpoint.
    pub http_port: u16,
}

/// Builder for one engine container.
pub struct EngineBuilder {
    container_name: String,
    datasource_url: String,
    env: Vec<(String, String)>,
    secrets: Vec<(String, String)>,
    host_gateway: bool,
    expected_deployments: u64,
    shards: Option<u32>,
}

impl EngineBuilder {
    /// Point a new engine at the given internal-database Postgres fixture.
    ///
    /// The lane count defaults to the run-wide [`conformance_shards`] reading, so setting
    /// `SUTRA_CONFORMANCE_SHARDS` re-runs every suite that uses this builder on an N-lane
    /// container without touching a single suite.
    pub fn new(name_hint: &str, pg: &PgFixture) -> Self {
        EngineBuilder {
            container_name: format!("{name_hint}-engine-{}", util::short_id()),
            datasource_url: datasource_url(pg),
            env: Vec::new(),
            secrets: Vec::new(),
            host_gateway: false,
            expected_deployments: 1,
            shards: conformance_shards(),
        }
    }

    /// Add a container env var (module credential refs, callback hosts, …).
    pub fn env(mut self, key: &str, value: impl Into<String>) -> Self {
        self.env.push((key.to_string(), value.into()));
        self
    }

    /// Pin this container's shard-router lane count (`sutra.engine.shards`), overriding the
    /// run-wide [`conformance_shards`] default — for a suite that asserts a specific N
    /// rather than one that merely re-runs at whatever the lane knob says.
    pub fn shards(mut self, count: u32) -> Self {
        self.shards = Some(count);
        self
    }

    /// Mount one estate secret (name → contents) so a `secret:<NAME>` envref resolves
    /// prod-faithfully. Each secret is written to a file `<tmp>/<NAME>` in a fresh
    /// world-readable dir bind-mounted READ-ONLY at [`sutra_envref_spi::DEFAULT_SECRETS_DIR`]
    /// (`/etc/sutra/secrets`) — the same shape a mounted k8s Secret gives, so e.g. a channel's
    /// `auth.apikey.value: secret:SENDER_APIKEY` reads its expected key from the mount rather
    /// than a literal baked into the archive.
    pub fn secret(mut self, name: &str, value: impl Into<String>) -> Self {
        self.secrets.push((name.to_string(), value.into()));
        self
    }

    /// Enable `host.docker.internal` (host-gateway) so the engine can reach a host-side HTTP
    /// recorder — the async out-of-band suites' callback endpoint.
    pub fn host_gateway(mut self) -> Self {
        self.host_gateway = true;
        self
    }

    /// How many deployments the mounted archive dir should activate before the engine is
    /// considered ready (guards the async suites against posting before activation).
    pub fn expected_deployments(mut self, n: u64) -> Self {
        self.expected_deployments = n;
        self
    }

    /// Start the engine against a mounted archives directory and block until it is ready
    /// (HTTP 200 on `/sutra/health/ready`, with at least `expected_deployments` active).
    pub fn start(self, archives_dir: &Path) -> EngineHandle {
        let (name, tag) = image_name_tag(&engine_image_ref());
        let mount = Mount::bind_mount(
            archives_dir.to_string_lossy().to_string(),
            CONTAINER_DEPLOYMENTS_DIR,
        )
        .with_access_mode(AccessMode::ReadOnly);

        // Materialize any mounted secrets into a fresh world-readable dir (file-per-secret) and
        // bind-mount it READ-ONLY at the engine's canonical secrets dir, so `secret:<NAME>` envrefs
        // resolve from files exactly as a mounted estate/k8s Secret would. Kept until process exit
        // (temp dir; the bind mount references the host path).
        let secrets_mount = (!self.secrets.is_empty()).then(|| {
            let dir = util::world_readable_temp_dir("sutra-secrets");
            for (key, value) in &self.secrets {
                std::fs::write(dir.join(key), value)
                    .unwrap_or_else(|e| panic!("write secret {key}: {e}"));
            }
            util::make_world_readable(&dir);
            Mount::bind_mount(
                dir.to_string_lossy().to_string(),
                sutra_envref_spi::DEFAULT_SECRETS_DIR,
            )
            .with_access_mode(AccessMode::ReadOnly)
        });

        // No testcontainers wait condition (the `http_wait` strategy is feature-gated); the
        // container is "started" once running, then `await_ready_deployments` polls the health
        // endpoint until the archives are active.
        let mut image = GenericImage::new(name, tag)
            .with_exposed_port(HTTP_PORT.tcp())
            .with_startup_timeout(Duration::from_secs(180))
            .with_network(util::network_for(util::suite_of(&self.container_name)))
            .with_container_name(&self.container_name)
            .with_mount(mount);
        for (key, value) in container_env(&self.datasource_url, self.shards, &self.env) {
            image = image.with_env_var(key, value);
        }
        if let Some(mount) = secrets_mount {
            image = image.with_mount(mount);
        }
        if self.host_gateway {
            image = image.with_host("host.docker.internal", Host::HostGateway);
        }

        let container = image.start().expect("start sutra engine (docker required)");
        crate::reap_on_exit(container.id());
        let http_port = container
            .get_host_port_ipv4(HTTP_PORT)
            .expect("mapped 8080");
        if !await_ready_deployments(
            http_port,
            self.expected_deployments,
            Duration::from_secs(120),
        ) {
            // Surface the engine's own logs so a boot/deploy failure is diagnosable (otherwise the
            // container is reaped and the timeout is opaque).
            let logs = std::process::Command::new("docker")
                .args(["logs", "--tail", "250", container.id()])
                .output()
                .map(|o| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|e| format!("<docker logs failed: {e}>"));
            panic!(
                "engine on port {http_port} never reached {} active deployment(s). \
                 Engine logs (tail 250):\n{logs}",
                self.expected_deployments
            );
        }
        EngineHandle {
            container,
            http_port,
        }
    }
}

/// The full env set one engine container boots with, in injection order: the canonical
/// `SUTRA_*` four the harness always bakes, then the N-lane knob when this run has one, then
/// the caller's own vars last (so a suite can still override anything above it).
///
/// Pure, and deliberately so — this is where the default run's byte-identity is provable
/// without docker: at `shards = None` the list is EXACTLY the four baked pairs plus `extra`,
/// i.e. an unset [`CONFORMANCE_SHARDS_ENV`] injects nothing whatsoever and the container boots
/// the same as it did before the seam existed.
fn container_env(
    datasource_url: &str,
    shards: Option<u32>,
    extra: &[(String, String)],
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = vec![
        (
            "SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED".to_string(),
            "false".to_string(),
        ),
        (
            "SUTRA_DEPLOYMENTS_DIR".to_string(),
            CONTAINER_DEPLOYMENTS_DIR.to_string(),
        ),
        (
            "SUTRA_DATASOURCE_URL".to_string(),
            datasource_url.to_string(),
        ),
        // A snappy outbox tick so out-of-band sends land well inside the suites' waits.
        ("SUTRA_OUTBOX_TICK_INTERVAL".to_string(), "PT1S".to_string()),
    ];
    if let Some(count) = shards {
        env.push((SHARDS_ENV.to_string(), count.to_string()));
    }
    env.extend(extra.iter().cloned());
    env
}

/// Block until the engine reports `/sutra/health/ready` 200 with at least `expected` deployments.
/// Returns `true` when ready, `false` on timeout (the caller dumps engine logs + panics).
fn await_ready_deployments(port: u16, expected: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(count) = ready_deployments(port) {
            last = Some(count);
            if count >= expected {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    eprintln!(
        "engine on port {port} never reached {expected} active deployment(s) (last={last:?})"
    );
    false
}

/// GET `/sutra/health/ready`; on 200 return the reported active-deployment count.
fn ready_deployments(port: u16) -> Option<u64> {
    let (status, body) = blocking_get(port, "/sutra/health/ready")?;
    if status != 200 {
        return None;
    }
    let json: serde_json::Value = serde_json::from_str(body.trim()).ok()?;
    json["checks"][0]["data"]["deployments"].as_u64()
}

/// GET `/sutra/health/ready`; on 200 return the shard router's LIVE lane count
/// (`checks[0].data.shards`) — the engine reads it off the running router handle, so this is
/// the black-box answer to "how many actor lanes did this container actually spawn?".
/// `None` when the engine is not ready or the payload predates the field.
pub fn ready_shards(port: u16) -> Option<u64> {
    let (status, body) = blocking_get(port, "/sutra/health/ready")?;
    if status != 200 {
        return None;
    }
    let json: serde_json::Value = serde_json::from_str(body.trim()).ok()?;
    json["checks"][0]["data"]["shards"].as_u64()
}

/// A dependency-free blocking HTTP GET (host `127.0.0.1:<port>`) → `(status, body)`.
fn blocking_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let status = response.split_whitespace().nth(1)?.parse().ok()?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Some((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DSN: &str = "postgres://postgres:postgres@pg-1:5432/postgres";

    fn baked(dsn: &str) -> Vec<(String, String)> {
        vec![
            (
                "SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED".to_string(),
                "false".to_string(),
            ),
            (
                "SUTRA_DEPLOYMENTS_DIR".to_string(),
                CONTAINER_DEPLOYMENTS_DIR.to_string(),
            ),
            ("SUTRA_DATASOURCE_URL".to_string(), dsn.to_string()),
            ("SUTRA_OUTBOX_TICK_INTERVAL".to_string(), "PT1S".to_string()),
        ]
    }

    /// The default-identity gate: with no lane count in play the container's env is EXACTLY
    /// what it was before the N-lane seam existed — `SUTRA_ENGINE_SHARDS` is not injected at
    /// all (not even as `1`), so the engine applies its own default.
    #[test]
    fn default_run_injects_no_shards_env() {
        let env = container_env(DSN, None, &[]);
        assert_eq!(env, baked(DSN));
        assert!(
            !env.iter().any(|(key, _)| key == SHARDS_ENV),
            "an unset lane knob must inject nothing: {env:?}"
        );
    }

    /// The caller's own vars still come last (override precedence unchanged), and the lane
    /// knob slots in ahead of them.
    #[test]
    fn extra_env_keeps_its_place_at_the_end() {
        let extra = vec![("SUTRA_MODULE_KEY".to_string(), "k".to_string())];
        let mut expected = baked(DSN);
        expected.push((SHARDS_ENV.to_string(), "4".to_string()));
        expected.extend(extra.iter().cloned());
        assert_eq!(container_env(DSN, Some(4), &extra), expected);
    }

    #[test]
    fn a_lane_count_is_injected_under_the_engine_env_name() {
        let env = container_env(DSN, Some(4), &[]);
        assert_eq!(
            env.iter().find(|(key, _)| key == SHARDS_ENV),
            Some(&(SHARDS_ENV.to_string(), "4".to_string()))
        );
    }

    /// The knob takes an integer ≥ 1 and DEGRADES to the default run on anything else — a
    /// rerun lane must never wedge a suite in a half-configured state.
    #[test]
    fn only_an_integer_of_at_least_one_turns_the_lane_knob() {
        assert_eq!(parse_shards(Some("4")), Some(4));
        assert_eq!(parse_shards(Some("  4 ")), Some(4));
        assert_eq!(parse_shards(Some("1")), Some(1));
        for raw in ["", "   ", "0", "-1", "two", "4.0"] {
            assert_eq!(parse_shards(Some(raw)), None, "rejects {raw:?}");
        }
        assert_eq!(parse_shards(None), None);
    }
}
