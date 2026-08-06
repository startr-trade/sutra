//! Tier-3 (k8s) plumbing: shell tofu against the shared scenario, drive the `sutra` CLI, and
//! read the cluster (Services / Secrets / Nodes) through kube-rs. Teardown is `sutra undeploy`
//! only — the shared instance and the cluster are operator-owned and never touched here.
//!
//! The cluster + observability stages that back these helpers are repo-root tooling
//! (`deploy/k8s-it/{cluster,infra}`, driven by `deploy/k8s-it/Makefile`): ONE kind cluster and
//! ONE EFK/OTel stack serve every tier-3 suite, so they belong to no example. Only the shared
//! ENGINE instance (`deploy/k8s-it/shared-scenario`) is applied from here, idempotently.
//!
//! The coordinator runs the k8s trio serially at wave close (`--test-threads=1`); the suites
//! are `#[ignore = "k8s"]` and never execute in a tier-1/tier-2 run.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Node, Secret, Service};
use kube::api::ListParams;
use kube::{Api, Client};

use super::util;

/// The shared Rust engine image the scenario runs (canonical `SUTRA_*` env; push to the
/// kind-local registry first). Overridable via `SUTRA_ENGINE_IMAGE`.
pub const SHARED_ENGINE_IMAGE: &str = "localhost:5000/sutra-engine:k8s-it";

/// `<repo>/deploy/k8s-it/shared-scenario` — the ONE engine + postgres + rabbitmq + estate
/// Secret + deployments ConfigMap + Ingress (idempotent apply).
pub fn shared_scenario_dir() -> PathBuf {
    util::repo_root().join("deploy/k8s-it/shared-scenario")
}

/// The kind cluster's generated kubeconfig. Overridable via `SUTRA_KUBECONFIG`; defaults to
/// the shared convention `deploy/k8s-it/cluster/sutra-fednow-it-config` — the file the
/// `cluster/` stage's kind provider WRITES. Its filename is historical and pinned by the
/// cluster name — renaming it means recreating the cluster — so only its directory moved.
pub fn kubeconfig_path() -> PathBuf {
    if let Ok(path) = std::env::var("SUTRA_KUBECONFIG") {
        if !path.trim().is_empty() {
            return PathBuf::from(path.trim());
        }
    }
    util::repo_root().join("deploy/k8s-it/cluster/sutra-fednow-it-config")
}

/// The effective engine image threaded through the scenario tofu.
pub fn engine_image() -> String {
    std::env::var("SUTRA_ENGINE_IMAGE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| SHARED_ENGINE_IMAGE.to_string())
}

/// The `sutra` CLI binary: `SUTRA_CLI` when set, else `rust/target/release/sutra` (built once
/// via `cargo build -p sutra-cli --release` when missing).
pub fn sutra_cli() -> PathBuf {
    if let Ok(path) = std::env::var("SUTRA_CLI") {
        if !path.trim().is_empty() {
            return PathBuf::from(path.trim());
        }
    }
    let repo = util::repo_root();
    let cli = repo.join("rust/target/release/sutra");
    if !cli.exists() {
        let status = Command::new("cargo")
            .args(["build", "-p", "sutra-cli", "--release"])
            .current_dir(repo.join("rust"))
            .status()
            .expect("cargo build sutra-cli");
        assert!(status.success(), "cargo build of sutra-cli failed");
    }
    cli
}

/// Run `sutra <args>` with extra environment, panicking on non-zero exit; returns combined
/// stdout+stderr.
pub fn run_cli(extra_env: &[(&str, &str)], args: &[&str]) -> String {
    let output = Command::new(sutra_cli())
        .args(args)
        .envs(
            extra_env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        )
        .output()
        .expect("run sutra cli");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "sutra {args:?} failed:\n{combined}"
    );
    combined
}

/// Deploy a sealed archive via the engine's sync API (`sutra deploy --api`) — the `db` deployment
/// source. Blocks until the deployment is ACTIVE (deterministic: no ConfigMap-propagation window),
/// so a subsequent broker/HTTP probe never races activation. `engine_base` is `http://<ingress-ip>`;
/// `admin_key` is the auth key/secret the `/admin/*` gate expects, passed via `SUTRA_ADMIN_API_KEY`.
pub fn deploy_api(engine_base: &str, archive: &Path, admin_key: &str) {
    let archive_str = archive.to_string_lossy();
    run_cli(
        &[("SUTRA_ADMIN_API_KEY", admin_key)],
        &[
            "deploy",
            archive_str.as_ref(),
            "--api",
            "--engine-url",
            engine_base,
        ],
    );
}

/// Undeploy a slot via the engine's API (`sutra undeploy --api <slot>`) best-effort — the `db`
/// source. `slot` is `tenant--module--version`; the active archive moves to Draining.
pub fn undeploy_api_quiet(engine_base: &str, slot: &str, admin_key: &str) {
    let _ = Command::new(sutra_cli())
        .args(["undeploy", slot, "--api", "--engine-url", engine_base])
        .env("SUTRA_ADMIN_API_KEY", admin_key)
        .output();
}

/// Read the admin auth key/secret the tier-3 deploy API expects — `ADMIN_API_KEY` in the
/// tofu-managed `sutra-admin-auth` Secret (see `shared-scenario`). Passed to [`deploy_api`].
pub async fn admin_api_key(client: &Client, namespace: &str) -> String {
    secret_value(client, namespace, "sutra-admin-auth", "ADMIN_API_KEY").await
}

/// Run `tofu <args>` in `dir` (inherited stdio), panicking on non-zero exit.
pub fn tofu(dir: &Path, args: &[&str]) {
    let status = Command::new("tofu")
        .args(args)
        .current_dir(dir)
        .status()
        .expect("run tofu");
    assert!(status.success(), "tofu {args:?} failed");
}

/// Block until the named Deployment's rollout has fully converged: the controller has observed
/// the current generation AND every replica is updated + available AND no old-generation pod
/// remains. This is the gate the suites were missing: every suite's `tofu apply` passes its own
/// per-run host:port vars (its callback / codec endpoints), so any suite transition legally
/// diffs the engine env and ROLLS the deployment — but `wait_engine_ready` polls through the
/// Service, which the OLD pod answers, so without this gate the pod swap lands minutes later,
/// mid-suite, racing whichever test is then in flight (observed twice as a rail repo's k8s
/// per-deployment hot-deploy suite timing out on its broker verdict while the swap
/// happened inside its poll window). Budget 300 s: a fresh image digest must be pulled +
/// unpacked by the kind node before the new pod can go Ready.
pub async fn await_rollout(client: &Client, namespace: &str, name: &str) {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let d = api.get(name).await.expect("get deployment");
        let generation = d.metadata.generation.unwrap_or(0);
        let want = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let converged = d.status.as_ref().is_some_and(|s| {
            s.observed_generation.unwrap_or(-1) >= generation
                && s.updated_replicas.unwrap_or(0) == want
                && s.available_replicas.unwrap_or(0) == want
                && s.replicas.unwrap_or(0) == want
                && s.unavailable_replicas.unwrap_or(0) == 0
        });
        if converged {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "deployment {namespace}/{name} rollout did not converge within 300s"
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// A kube-rs client pointed at the cluster kubeconfig (via `KUBECONFIG`).
pub async fn kube_client() -> Client {
    std::env::set_var("KUBECONFIG", kubeconfig_path());
    let config = kube::Config::infer().await.expect("infer kube config");
    Client::try_from(config).expect("kube client")
}

/// A Secret's value (k8s-openapi decodes base64 into raw bytes).
pub async fn secret_value(client: &Client, namespace: &str, secret: &str, key: &str) -> String {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let s = api.get(secret).await.expect("get secret");
    let data = s.data.expect("secret data");
    let bytes = data
        .get(key)
        .unwrap_or_else(|| panic!("secret {secret} has no key {key}"));
    String::from_utf8(bytes.0.clone()).expect("utf8 secret value")
}

/// The Service's CURRENT MetalLB LoadBalancer ingress IP, or `None` when none is assigned yet.
///
/// A single, side-effect-free query — safe to call repeatedly to RE-RESOLVE the live LB IP. The
/// poll-until-ready helpers (`ingress_endpoint`, `await_lb_ip`) build on it; the self-healing
/// broker recorder also re-invokes it on a sustained outage so a MetalLB speaker flap that
/// re-announces the LB on a new path is followed without a manual `metallb` bounce.
pub async fn service_lb_ip(client: &Client, namespace: &str, service: &str) -> Option<String> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let svc = api.get(service).await.ok()?;
    svc.status?
        .load_balancer?
        .ingress?
        .into_iter()
        .next()?
        .ip
        .filter(|ip| !ip.is_empty())
}

/// The ingress-nginx HTTP endpoint: MetalLB LB IP when assigned, else the NodePort fallback
/// (node InternalIP + the port-80 nodePort).
///
/// Cold-start budget: 90s, not 30s — the FIRST scenario apply after a cluster reboot
/// needs MetalLB/ingress-nginx to reconverge from cold, which can outrun a tight budget. This
/// only raises the ceiling on an already-fast poll-until-ready loop (checked every 3s), so an
/// already-warm cluster still returns as soon as the IP is assigned — steady-state stays fast.
pub async fn ingress_endpoint(client: &Client, namespace: &str, service: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        if let Some(ip) = service_lb_ip(client, namespace, service).await {
            return ip;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let svc = api.get(service).await.expect("get service");
    let node_port = svc
        .spec
        .and_then(|s| s.ports)
        .and_then(|ports| {
            ports
                .into_iter()
                .find(|p| p.port == 80)
                .and_then(|p| p.node_port)
        })
        .expect("port-80 nodePort");
    let nodes: Api<Node> = Api::all(client.clone());
    let list = nodes
        .list(&ListParams::default())
        .await
        .expect("list nodes");
    let node_ip = list
        .items
        .into_iter()
        .find_map(|n| {
            n.status?
                .addresses?
                .into_iter()
                .find(|a| a.type_ == "InternalIP")
                .map(|a| a.address)
        })
        .expect("node InternalIP");
    format!("{node_ip}:{node_port}")
}

/// Poll for a Service's MetalLB LoadBalancer IP (the broker rail), panicking on timeout.
///
/// Cold-start budget: 240s, not 120s — cold image pulls (postgres/rabbitmq) plus
/// MetalLB reconvergence on the first apply after a cluster reboot can overrun the tighter
/// budget. As with `ingress_endpoint`, this only widens the ceiling on a poll-until-ready loop,
/// so the already-warm steady-state path still returns as soon as the LB IP is assigned.
pub async fn await_lb_ip(client: &Client, namespace: &str, service: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(240);
    while Instant::now() < deadline {
        if let Some(ip) = service_lb_ip(client, namespace, service).await {
            return ip;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    panic!("service {namespace}/{service} never got a LoadBalancer IP (MetalLB)");
}

/// The kind docker-network IPv4 gateway — the pod's route to a host-side recorder.
pub fn kind_gateway_ip() -> String {
    let output = Command::new("docker")
        .args([
            "network",
            "inspect",
            "kind",
            "-f",
            "{{range .IPAM.Config}}{{.Gateway}} {{end}}",
        ])
        .output()
        .expect("docker network inspect kind");
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .find(|gw| gw.contains('.') && !gw.contains(':'))
        .map(|s| s.to_string())
        .expect("IPv4 gateway on the kind network")
}
