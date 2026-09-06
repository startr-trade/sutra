//! `sutra deploy` / `sutra undeploy` — hot deploy of sealed `.sutra` packages onto the
//! ONE shared engine instance, through the Kubernetes API: a native kube-rs client with
//! kubeconfig contexts and rustls, never a kubectl shell-out.
//!
//! The engine stays a passive directory-consumer — no management/upload endpoint, no new
//! network surface: this command patches the `sutra-deployments` ConfigMap the shared
//! tofu mounts at `/etc/sutra/deployments`; kubelet syncs the volume and the engine's
//! deployments-dir watcher runs the two-phase flip. Multi-replica consistency comes from the
//! shared ConfigMap.
//!
//! **Ordering:** `deploy` ensures/patches the estate Secret keys
//! FIRST, the ConfigMap second. An ordering slip self-heals: an unresolvable `secret:` ref
//! aborts the engine's flip with old state intact and the watcher retries next tick, so a
//! Secret key landing late only delays activation by one sync.
//!
//! **Removal:** `undeploy` deletes the archive's ConfigMap key → the engine drains the
//! deployment (no new intake; in-flight/timer/relay continue) and retires it at zero
//! instances + zero pending outbox. Estate Secret keys are garbage-collected ONLY with an
//! explicit `--gc-secrets KEY...` (keys may be shared across deployments — never guessed).
//!
//! **Capacity posture:** the ~1 MiB ConfigMap ceiling is accepted for examples and
//! documented as a posture note for real estates — a deploy that would exceed it fails with
//! a friendly error naming that note instead of an opaque API rejection.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::ByteString;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::Client;

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat};
use crate::GlobalArgs;

/// Diagnostic codes owned by `sutra deploy`/`undeploy` — the reserved `SUTRA.DEPLOY.*`
/// family in the diagnostics registry, beside the loader's archive/package codes.
pub mod codes {
    /// The kubeconfig/context/cluster could not produce a working API connection.
    pub const TARGET_UNREACHABLE: &str = "SUTRA.DEPLOY.TARGET.UNREACHABLE";
    /// The deployments ConfigMap does not exist — the shared instance is not provisioned.
    pub const TARGET_NOT_PROVISIONED: &str = "SUTRA.DEPLOY.TARGET.NOT_PROVISIONED";
    /// The archive would push the deployments ConfigMap past the ~1 MiB ceiling (see the
    /// capacity posture note).
    pub const CONFIGMAP_CAPACITY_EXCEEDED: &str = "SUTRA.DEPLOY.CONFIGMAP.CAPACITY_EXCEEDED";
    /// `undeploy` found no ConfigMap entry for the given deploymentId / archive name.
    pub const DEPLOYMENT_NOT_FOUND: &str = "SUTRA.DEPLOY.DEPLOYMENT.NOT_FOUND";
    /// A `--secret` / `--secret-from` input is not KEY=VALUE shaped.
    pub const SECRET_INPUT_INVALID: &str = "SUTRA.DEPLOY.SECRET.INPUT_INVALID";
    /// The Kubernetes API rejected a read/patch/create.
    pub const API_REJECTED: &str = "SUTRA.DEPLOY.API.REJECTED";
}

/// The total-payload ceiling of one ConfigMap (data + binaryData), as etcd enforces it.
/// Binary entries count at their base64-encoded (wire) size.
const CONFIGMAP_CEILING_BYTES: usize = 1_048_576;

// ==================================================================================
// Arguments
// ==================================================================================

/// Where the shared instance lives — kubeconfig context, namespace, and the two estate
/// objects the shared-scenario tofu provisions.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct TargetArgs {
    /// kubeconfig context to use (default: the kubeconfig's current context).
    #[arg(long, value_name = "CONTEXT")]
    pub context: Option<String>,

    /// Namespace of the deployments ConfigMap + estate Secret (default: the context's).
    #[arg(short = 'n', long, value_name = "NAMESPACE")]
    pub namespace: Option<String>,

    /// Deployments ConfigMap name (the shared-scenario tofu provisions this).
    #[arg(long, value_name = "NAME", default_value = "sutra-deployments")]
    pub configmap: String,

    /// Estate Secret name (the shared-scenario tofu provisions this; mounted at
    /// SUTRA_SECRETS_DIR where `secret:KEY` refs resolve).
    #[arg(
        long = "secret-name",
        value_name = "NAME",
        default_value = "sutra-secrets"
    )]
    pub secret_name: String,
}

#[derive(Debug, clap::Args)]
pub struct DeployArgs {
    /// Sealed `.sutra` archive to deploy (verified through the archive reader before upload).
    /// Required unless `--watch <dir>` is given (the watch loop packages the directory itself).
    pub archive: Option<PathBuf>,

    #[command(flatten)]
    pub target: TargetArgs,

    /// Estate Secret key to ensure/merge BEFORE the ConfigMap patch (repeatable KEY=VALUE —
    /// the values the package's `secret:KEY` refs resolve).
    #[arg(long = "secret", value_name = "KEY=VALUE")]
    pub secret: Vec<String>,

    /// File of KEY=VALUE lines merged into the estate Secret (blank lines and `#` comments
    /// skipped) — the bulk form of --secret.
    #[arg(long = "secret-from", value_name = "FILE")]
    pub secret_from: Option<PathBuf>,

    /// Deploy via the engine's SYNCHRONOUS API (`POST /admin/deployments`) instead of patching
    /// the ConfigMap — the `db` deployment source. Requires
    /// --engine-url; the call returns only once the deployment is ACTIVE (or fails fast on a
    /// rejected archive). This is the deterministic deploy path (no ConfigMap propagation window).
    #[arg(long)]
    pub api: bool,

    /// With --api: submit as an ASYNC long-running deploy (`POST …?mode=async`). The engine
    /// validates + stores synchronously and returns `202 {deploymentId, Pending}` immediately;
    /// the CLI then polls `/sutra/deployments/{id}` until Active. Use for large projects whose
    /// activation flip (registry rebuild + transport rewire) can outlast a k8s ingress
    /// read-timeout on one long request. The poll deadline is --wait-timeout. Ignored without --api.
    #[arg(long = "async")]
    pub async_lro: bool,

    /// Watch a package source directory and re-deploy on change — a dev-mode local
    /// live-reload loop (validate-then-deploy: each save is re-packaged, statically validated, and
    /// deployed only if validation passes). Implies --api; requires --engine-url. The positional
    /// archive arg is ignored (the watch loop packages the dir itself).
    #[arg(long, value_name = "PKG_DIR")]
    pub watch: Option<PathBuf>,

    /// After patching the ConfigMap, poll the engine until THIS deployment is Active
    /// (activation is async — kubelet syncs the volume, then the watcher flips). Requires
    /// --engine-url. Without it, `deploy` returns as soon as the ConfigMap is patched.
    #[arg(long)]
    pub wait: bool,

    /// Engine base URL for --wait (e.g. `http://<ingress-ip>` or `http://localhost:PORT`) —
    /// where `/sutra/deployments/{id}` is polled.
    #[arg(long = "engine-url", value_name = "URL")]
    pub engine_url: Option<String>,

    /// Admin auth key/secret for the `--api` deploy (the `/admin/*` surface's static-secret gate,
    /// `sutra.admin.auth.*`). Sent in the `--api-key-header` header. Reads `SUTRA_ADMIN_API_KEY` when
    /// the flag is omitted. Not needed for a dev-open engine.
    #[arg(long = "api-key", env = "SUTRA_ADMIN_API_KEY", value_name = "KEY")]
    pub api_key: Option<String>,

    /// Header carrying the `--api-key` (default `X-API-Key` for the apikey scheme; use
    /// `authorization` with a `Bearer …` value for the bearer scheme).
    #[arg(
        long = "api-key-header",
        value_name = "HEADER",
        default_value = "X-API-Key"
    )]
    pub api_key_header: String,

    /// --wait deadline in seconds (default 180 — covers the kubelet ConfigMap sync window).
    #[arg(long = "wait-timeout", value_name = "SECS", default_value_t = 180)]
    pub wait_timeout: u64,
}

#[derive(Debug, clap::Args)]
pub struct UndeployArgs {
    /// What to remove. ConfigMap/dir path: a deploymentId (`dep-<24 hex>`) or the archive file
    /// name (`<name>.sutra`, the ConfigMap key). With `--api` (db source): the slot
    /// (`tenant--module--version`) whose active archive to retire.
    pub deployment: String,

    #[command(flatten)]
    pub target: TargetArgs,

    /// Undeploy via the engine's API (`DELETE /admin/deployments/{slot}`) instead of patching the
    /// ConfigMap — the `db` deployment source. Requires --engine-url; the positional argument is the
    /// slot. The slot's active archive moves to Draining and the fleet re-flips synchronously.
    #[arg(long)]
    pub api: bool,

    /// Engine base URL for --api (e.g. `http://<ingress-ip>`), where `/admin/deployments/{slot}` is
    /// issued.
    #[arg(long = "engine-url", value_name = "URL")]
    pub engine_url: Option<String>,

    /// Admin auth key/secret for the `--api` undeploy (see `deploy --api-key`). Reads
    /// `SUTRA_ADMIN_API_KEY` when omitted.
    #[arg(long = "api-key", env = "SUTRA_ADMIN_API_KEY", value_name = "KEY")]
    pub api_key: Option<String>,

    /// Header carrying the `--api-key` (default `X-API-Key`).
    #[arg(
        long = "api-key-header",
        value_name = "HEADER",
        default_value = "X-API-Key"
    )]
    pub api_key_header: String,

    /// Estate Secret keys to delete after the ConfigMap key removal. NEVER implied — keys
    /// may be shared across deployments, so garbage collection is explicit only. (ConfigMap path
    /// only; ignored with --api.)
    #[arg(long = "gc-secrets", value_name = "KEY", num_args = 1..)]
    pub gc_secrets: Vec<String>,
}

// ==================================================================================
// Typed error → exit-code mapping
// ==================================================================================

/// Every failure mode of the two commands, carrying its `SUTRA.DEPLOY.*` diagnostic and
/// the exit bucket the exit-code contract assigns it.
#[derive(Debug)]
pub(crate) enum DeployError {
    /// Bad invocation inputs (exit 2).
    Usage(Diagnostic),
    /// The target has a diagnosable problem — missing entry, capacity (exit 1).
    Findings(Diagnostic),
    /// API/connectivity failure (exit 2).
    Infra(Diagnostic),
}

impl DeployError {
    fn usage(code: &str, message: impl Into<String>) -> DeployError {
        DeployError::Usage(Diagnostic::error(code, message))
    }
    fn findings(code: &str, message: impl Into<String>) -> DeployError {
        DeployError::Findings(Diagnostic::error(code, message))
    }
    fn infra(code: &str, message: impl Into<String>) -> DeployError {
        DeployError::Infra(Diagnostic::error(code, message))
    }

    fn diagnostic(&self) -> &Diagnostic {
        match self {
            DeployError::Usage(d) | DeployError::Findings(d) | DeployError::Infra(d) => d,
        }
    }

    fn exit_code(&self) -> i32 {
        match self {
            DeployError::Usage(_) | DeployError::Infra(_) => exit::USAGE,
            DeployError::Findings(_) => exit::FINDINGS,
        }
    }
}

fn api_error(action: &str, e: kube::Error) -> DeployError {
    DeployError::infra(
        codes::API_REJECTED,
        format!("Kubernetes API rejected {action}: {e}"),
    )
}

/// `true` when the kube error is a 404 for the addressed object.
fn is_not_found(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(resp) if resp.code == 404)
}

// ==================================================================================
// Entry points
// ==================================================================================

pub fn execute_deploy(args: DeployArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "deploy: {msg}");
            return exit::USAGE;
        }
    };

    // ---- --watch: the local live-reload loop (implies --api) -----------------------------
    if let Some(pkg_dir) = args.watch.clone() {
        let Some(engine_url) = args.engine_url.clone() else {
            let _ = writeln!(io.err, "deploy --watch requires --engine-url <URL>");
            return exit::USAGE;
        };
        let admin_key = args
            .api_key
            .clone()
            .map(|k| (args.api_key_header.clone(), k));
        return run_watch_loop(&pkg_dir, &engine_url, admin_key, io);
    }

    // ---- verify the archive through the engine's own fail-closed reader ----------------
    let Some(archive_path) = args.archive.clone() else {
        let _ = writeln!(
            io.err,
            "deploy: an archive path is required (or use --watch <dir>)"
        );
        return exit::USAGE;
    };
    let bytes = match std::fs::read(&archive_path) {
        Ok(b) => b,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "deploy: cannot read archive {}: {e}",
                archive_path.display()
            );
            return exit::USAGE;
        }
    };
    let archive = match sutra_loader::read_archive(&bytes) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(io.err, "deploy: archive rejected: {e}");
            return exit::FINDINGS;
        }
    };
    let Some(file_name) = archive_path.file_name().and_then(|n| n.to_str()) else {
        let _ = writeln!(io.err, "deploy: archive path has no file name");
        return exit::USAGE;
    };

    // ---- --api: the engine deploy API (db source), sync by default or --async LRO --------
    if args.api {
        let Some(engine_url) = args.engine_url.clone() else {
            let _ = writeln!(io.err, "deploy --api requires --engine-url <URL>");
            return exit::USAGE;
        };
        let timeout = std::time::Duration::from_secs(args.wait_timeout);
        let admin_key = args
            .api_key
            .as_deref()
            .map(|k| (args.api_key_header.as_str(), k));
        let result = if args.async_lro {
            block_on(run_deploy_api_async(
                &engine_url,
                bytes,
                format,
                timeout,
                admin_key,
                io,
            ))
        } else {
            // The slot mirrors the engine's `tenant--module--version` archive key; with the
            // locally-known deploymentId it lets a cut/5xx POST fall back to a status poll rather
            // than reporting a false failure on a deploy that actually landed.
            let slot = format!(
                "{}--{}--{}",
                archive.deployment.tenant, archive.deployment.module, archive.deployment.version
            );
            let fallback = Some((archive.id.value().to_string(), slot, timeout));
            block_on(run_deploy_api(
                &engine_url,
                bytes,
                format,
                fallback,
                admin_key,
                io,
            ))
        };
        return match result {
            Ok(()) => exit::OK,
            Err(e) => {
                let _ = writeln!(io.err, "{}", e.diagnostic().render_text());
                e.exit_code()
            }
        };
    }

    // ---- secret inputs -------------------------------------------------------------------
    let secrets = match collect_secret_entries(&args.secret, args.secret_from.as_deref()) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(io.err, "{}", e.diagnostic().render_text());
            return e.exit_code();
        }
    };

    let archive_bytes = bytes;
    let outcome = block_on(async {
        let client = client_for(args.target.context.as_deref()).await?;
        run_deploy(client, &args.target, file_name, archive_bytes, secrets).await
    });

    match outcome {
        Ok(()) => {
            match format {
                ReportFormat::Text => {
                    let _ = writeln!(
                        io.out,
                        "deployed {file_name} ({}) into ConfigMap '{}' — kubelet syncs the \
                         mounted volume and the engine's watcher runs the two-phase flip",
                        archive.id.value(),
                        args.target.configmap
                    );
                }
                ReportFormat::Json => {
                    let _ = writeln!(
                        io.out,
                        "{}",
                        serde_json::json!({
                            "deploymentId": archive.id.value(),
                            "configMap": args.target.configmap,
                            "key": file_name,
                        })
                    );
                }
            }
            if args.wait {
                let Some(engine_url) = args.engine_url.as_deref() else {
                    let _ = writeln!(io.err, "deploy --wait requires --engine-url <URL>");
                    return exit::USAGE;
                };
                let dep_id = archive.id.value().to_string();
                let slot = file_name.to_string();
                match block_on(wait_for_active(
                    engine_url,
                    &dep_id,
                    &slot,
                    std::time::Duration::from_secs(args.wait_timeout),
                )) {
                    Ok(()) => {
                        let _ = writeln!(io.out, "deployment {dep_id} is Active");
                    }
                    Err(e) => {
                        let _ = writeln!(io.err, "deploy --wait: {e}");
                        return exit::FINDINGS;
                    }
                }
            }
            exit::OK
        }
        Err(e) => {
            let _ = writeln!(io.err, "{}", e.diagnostic().render_text());
            e.exit_code()
        }
    }
}

pub fn execute_undeploy(args: UndeployArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "undeploy: {msg}");
            return exit::USAGE;
        }
    };

    // ---- --api: the engine undeploy API (db source) — the positional arg is the slot -----------
    if args.api {
        let Some(engine_url) = args.engine_url.clone() else {
            let _ = writeln!(io.err, "undeploy --api requires --engine-url <URL>");
            return exit::USAGE;
        };
        let admin_key = args
            .api_key
            .as_deref()
            .map(|k| (args.api_key_header.as_str(), k));
        return match block_on(run_undeploy_api(
            &engine_url,
            &args.deployment,
            format,
            admin_key,
            io,
        )) {
            Ok(()) => exit::OK,
            Err(e) => {
                let _ = writeln!(io.err, "{}", e.diagnostic().render_text());
                e.exit_code()
            }
        };
    }

    let outcome = block_on(async {
        let client = client_for(args.target.context.as_deref()).await?;
        run_undeploy(client, &args.target, &args.deployment, &args.gc_secrets).await
    });

    match outcome {
        Ok(removed_key) => {
            match format {
                ReportFormat::Text => {
                    let _ = writeln!(
                        io.out,
                        "undeployed {removed_key} from ConfigMap '{}' — the engine drains \
                         it (no new intake; in-flight work continues) and retires it at \
                         zero instances + zero pending outbox",
                        args.target.configmap
                    );
                }
                ReportFormat::Json => {
                    let _ = writeln!(
                        io.out,
                        "{}",
                        serde_json::json!({
                            "configMap": args.target.configmap,
                            "removedKey": removed_key,
                            "gcSecrets": args.gc_secrets,
                        })
                    );
                }
            }
            exit::OK
        }
        Err(e) => {
            let _ = writeln!(io.err, "{}", e.diagnostic().render_text());
            e.exit_code()
        }
    }
}

// ==================================================================================
// Kubernetes plumbing
// ==================================================================================

/// Build a client from the kubeconfig, honouring `--context`. Falls back to the inferred
/// config (kubeconfig current-context, then in-cluster) when no context is named.
async fn client_for(context: Option<&str>) -> Result<Client, DeployError> {
    let config = match context {
        Some(ctx) => {
            let options = kube::config::KubeConfigOptions {
                context: Some(ctx.to_string()),
                ..Default::default()
            };
            kube::Config::from_kubeconfig(&options).await.map_err(|e| {
                DeployError::infra(
                    codes::TARGET_UNREACHABLE,
                    format!("kubeconfig context '{ctx}' did not load: {e}"),
                )
            })?
        }
        None => kube::Config::infer().await.map_err(|e| {
            DeployError::infra(
                codes::TARGET_UNREACHABLE,
                format!("no usable Kubernetes configuration: {e}"),
            )
        })?,
    };
    Client::try_from(config).map_err(|e| {
        DeployError::infra(
            codes::TARGET_UNREACHABLE,
            format!("Kubernetes client construction failed: {e}"),
        )
    })
}

fn secrets_api(client: &Client, target: &TargetArgs) -> Api<Secret> {
    match &target.namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::default_namespaced(client.clone()),
    }
}

fn configmaps_api(client: &Client, target: &TargetArgs) -> Api<ConfigMap> {
    match &target.namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::default_namespaced(client.clone()),
    }
}

/// Deploy via the engine's SYNCHRONOUS API (`POST /admin/deployments`) — the `db` deployment
/// source. The engine validates + stores + activates and returns the ACTIVE outcome only once the
/// flip is done (or a non-2xx with the reject diagnostic). No ConfigMap, no polling.
/// Attach the admin auth key/secret header to a request when configured (the `/admin/*`
/// static-secret gate). `None` for a dev-open engine.
fn with_admin_key(
    rb: reqwest::RequestBuilder,
    admin_key: Option<(&str, &str)>,
) -> reqwest::RequestBuilder {
    match admin_key {
        Some((header, value)) => rb.header(header, value),
        None => rb,
    }
}

pub(crate) async fn run_deploy_api(
    engine_url: &str,
    archive_bytes: Vec<u8>,
    format: ReportFormat,
    fallback: Option<(String, String, std::time::Duration)>,
    admin_key: Option<(&str, &str)>,
    io: &mut Io<'_>,
) -> Result<(), DeployError> {
    let base = engine_url.trim_end_matches('/');
    let url = format!("{base}/admin/deployments");
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| DeployError::infra(codes::API_REJECTED, format!("http client: {e}")))?;
    let send = with_admin_key(
        client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(archive_bytes),
        admin_key,
    )
    .send()
    .await;
    let resp = match send {
        Ok(r) => r,
        // No HTTP response at all — the connection was cut (e.g. a k8s ingress read-timeout on a
        // long flip). The deploy may have activated server-side; poll the status endpoint (we know
        // the id locally) before declaring failure.
        Err(e) => {
            return sync_poll_fallback(base, fallback, format, io, format!("POST {url}: {e}")).await
        }
    };
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.is_server_error() {
        // 5xx (incl. a 502/504 injected by the ingress on timeout) — treat like a cut connection.
        return sync_poll_fallback(
            base,
            fallback,
            format,
            io,
            format!("engine/ingress returned HTTP {status}: {}", text.trim()),
        )
        .await;
    }
    if !status.is_success() {
        // 4xx — a genuine rejection (bad archive, unauthorized). Fail fast, no poll.
        return Err(DeployError::findings(
            codes::API_REJECTED,
            format!(
                "engine rejected the deployment (HTTP {status}): {}",
                text.trim()
            ),
        ));
    }
    let body: serde_json::Value =
        serde_json::from_str(text.trim()).unwrap_or_else(|_| serde_json::json!({}));
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "deployed {} (slot {}, rev {}) — {}",
                body["deploymentId"].as_str().unwrap_or("?"),
                body["slot"].as_str().unwrap_or("?"),
                body["revision"].as_i64().unwrap_or(-1),
                body["phase"].as_str().unwrap_or("Active"),
            );
        }
        ReportFormat::Json => {
            let _ = writeln!(io.out, "{}", text.trim());
        }
    }
    Ok(())
}

/// Fall-back for the SYNC deploy when the POST does not return a clean 2xx/4xx — a cut connection or
/// a 5xx from the ingress on a long flip. The archive's content-hash deploymentId is known locally,
/// so poll `/sutra/deployments/{id}` to the finite Active/Failed verdict before declaring failure;
/// without a fallback context (e.g. the watch loop over localhost) the original error stands. This
/// is the sync-path counterpart to `--async`: it turns an ingress timeout on a succeeded deploy from
/// a false failure into a confirmed Active.
async fn sync_poll_fallback(
    base: &str,
    fallback: Option<(String, String, std::time::Duration)>,
    format: ReportFormat,
    io: &mut Io<'_>,
    cause: String,
) -> Result<(), DeployError> {
    let Some((dep_id, slot, timeout)) = fallback else {
        return Err(DeployError::infra(codes::TARGET_UNREACHABLE, cause));
    };
    let _ = writeln!(
        io.err,
        "{cause} — the activation flip may still be running; polling \
         /sutra/deployments/{dep_id} (deadline {}s)",
        timeout.as_secs()
    );
    wait_for_active(base, &dep_id, &slot, timeout)
        .await
        .map_err(|pe| {
            DeployError::findings(
                codes::API_REJECTED,
                format!("{cause}; status poll did not reach Active: {pe}"),
            )
        })?;
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "deployed {dep_id} (slot {slot}) — Active (confirmed via status poll)"
            );
        }
        ReportFormat::Json => {
            let _ = writeln!(
                io.out,
                "{}",
                serde_json::json!({"deploymentId": dep_id, "slot": slot, "phase": "Active"})
            );
        }
    }
    Ok(())
}

/// Deploy via the engine's ASYNC long-running API (`POST /admin/deployments?mode=async`) — the
/// engine validates + stores the archive and returns `202 {deploymentId, Pending}` immediately,
/// then runs the (possibly long) activation flip in the background. The CLI polls
/// `/sutra/deployments/{id}` until Active — or fails fast if the slot is Rejected, or times out —
/// so `deploy --api --async` still resolves to a finite Active/Failed verdict without holding one
/// long HTTP request open across the flip (the k8s ingress-read-timeout hazard for big projects).
/// A validation failure still fails synchronously at the POST (`4xx`).
pub(crate) async fn run_deploy_api_async(
    engine_url: &str,
    archive_bytes: Vec<u8>,
    format: ReportFormat,
    wait_timeout: std::time::Duration,
    admin_key: Option<(&str, &str)>,
    io: &mut Io<'_>,
) -> Result<(), DeployError> {
    let base = engine_url.trim_end_matches('/');
    let url = format!("{base}/admin/deployments?mode=async");
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| DeployError::infra(codes::API_REJECTED, format!("http client: {e}")))?;
    let resp = with_admin_key(
        client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(archive_bytes),
        admin_key,
    )
    .send()
    .await
    .map_err(|e| DeployError::infra(codes::TARGET_UNREACHABLE, format!("POST {url}: {e}")))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    // 202 Accepted is the async happy path; a validation failure still fails synchronously (4xx).
    if status.as_u16() != 202 {
        return Err(DeployError::findings(
            codes::API_REJECTED,
            format!(
                "engine did not accept the async deployment (HTTP {status}): {}",
                text.trim()
            ),
        ));
    }
    let body: serde_json::Value =
        serde_json::from_str(text.trim()).unwrap_or_else(|_| serde_json::json!({}));
    let dep_id = body["deploymentId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let slot = body["slot"].as_str().unwrap_or_default().to_string();
    if dep_id.is_empty() {
        return Err(DeployError::infra(
            codes::API_REJECTED,
            format!("async accept returned no deploymentId: {}", text.trim()),
        ));
    }
    let _ = writeln!(
        io.err,
        "accepted {dep_id} (slot {slot}) — Pending; polling activation (deadline {}s)",
        wait_timeout.as_secs()
    );
    // Poll to the finite Active/Failed verdict (reuses the --wait poller).
    wait_for_active(base, &dep_id, &slot, wait_timeout)
        .await
        .map_err(|e| DeployError::findings(codes::API_REJECTED, e))?;
    match format {
        ReportFormat::Text => {
            let _ = writeln!(io.out, "deployed {dep_id} (slot {slot}) — Active");
        }
        ReportFormat::Json => {
            let _ = writeln!(
                io.out,
                "{}",
                serde_json::json!({"deploymentId": dep_id, "slot": slot, "phase": "Active"})
            );
        }
    }
    Ok(())
}

/// Undeploy via the engine's API (`DELETE /admin/deployments/{slot}`) — the `db` deployment source.
/// The slot's active archive moves to Draining and the fleet re-flips synchronously. `404` when the
/// slot has no active archive (surfaced as a finding).
pub(crate) async fn run_undeploy_api(
    engine_url: &str,
    slot: &str,
    format: ReportFormat,
    admin_key: Option<(&str, &str)>,
    io: &mut Io<'_>,
) -> Result<(), DeployError> {
    let base = engine_url.trim_end_matches('/');
    let url = format!("{base}/admin/deployments/{slot}");
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| DeployError::infra(codes::API_REJECTED, format!("http client: {e}")))?;
    let resp = with_admin_key(client.delete(&url), admin_key)
        .send()
        .await
        .map_err(|e| DeployError::infra(codes::TARGET_UNREACHABLE, format!("DELETE {url}: {e}")))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.as_u16() == 404 {
        return Err(DeployError::findings(
            codes::DEPLOYMENT_NOT_FOUND,
            format!("no active deployment for slot '{slot}': {}", text.trim()),
        ));
    }
    if !status.is_success() {
        return Err(DeployError::findings(
            codes::API_REJECTED,
            format!(
                "engine rejected the undeploy (HTTP {status}): {}",
                text.trim()
            ),
        ));
    }
    match format {
        ReportFormat::Text => {
            let _ = writeln!(io.out, "undeployed slot {slot} — Draining");
        }
        ReportFormat::Json => {
            let _ = writeln!(io.out, "{}", text.trim());
        }
    }
    Ok(())
}

/// A cheap change stamp for the package dir — the max mtime (as nanos) over its files. A change to
/// any source file bumps it, which the watch loop keys off (no filesystem-notify dependency).
fn dir_change_stamp(dir: &Path) -> u128 {
    fn walk(dir: &Path, max: &mut u128) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, max);
            } else if let Ok(meta) = entry.metadata() {
                if let Ok(mtime) = meta.modified() {
                    if let Ok(d) = mtime.duration_since(std::time::UNIX_EPOCH) {
                        *max = (*max).max(d.as_nanos());
                    }
                }
            }
        }
    }
    let mut max = 0u128;
    walk(dir, &mut max);
    max
}

/// The dev-mode local live-reload loop: poll `pkg_dir` for changes and, on each change,
/// **re-package + validate then deploy** via the sync API — never a blind deploy (`assemble_dir` is
/// fail-closed, so an invalid package prints its error and skips the deploy). Runs until interrupted.
fn run_watch_loop(
    pkg_dir: &Path,
    engine_url: &str,
    admin_key: Option<(String, String)>,
    io: &mut Io<'_>,
) -> i32 {
    if !pkg_dir.is_dir() {
        let _ = writeln!(
            io.err,
            "deploy --watch: {} is not a directory",
            pkg_dir.display()
        );
        return exit::USAGE;
    }
    let _ = writeln!(
        io.err,
        "watching {} — validate + re-deploy on change via {} (Ctrl-C to stop)",
        pkg_dir.display(),
        engine_url
    );
    let key_ref = admin_key.as_ref().map(|(h, v)| (h.as_str(), v.as_str()));
    let mut last = 0u128;
    loop {
        let stamp = dir_change_stamp(pkg_dir);
        if stamp != last {
            last = stamp;
            match package_and_deploy_once(pkg_dir, engine_url, key_ref, io) {
                Ok(()) => {}
                Err(msg) => {
                    let _ = writeln!(io.err, "watch: skipped deploy — {msg}");
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(700));
    }
}

/// One watch iteration: package `pkg_dir` (fail-closed validation) into a temp `.sutra`, then deploy
/// it via the sync API. Returns the reason on a validation/deploy failure (the loop reports + skips).
fn package_and_deploy_once(
    pkg_dir: &Path,
    engine_url: &str,
    admin_key: Option<(&str, &str)>,
    io: &mut Io<'_>,
) -> Result<(), String> {
    let out = std::env::temp_dir().join(format!("sutra-watch-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&out);
    // assemble_dir is the fail-closed validate+package step (validate-then-deploy).
    sutra_loader::assemble_dir(pkg_dir, &out, &Default::default())
        .map_err(|e| format!("package/validate failed: {e}"))?;
    let sutra = std::fs::read_dir(&out)
        .map_err(|e| format!("read temp out: {e}"))?
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|x| x.to_str()) == Some("sutra"))
        .ok_or_else(|| "packaging produced no .sutra archive".to_string())?;
    let bytes = std::fs::read(&sutra).map_err(|e| format!("read {}: {e}", sutra.display()))?;
    let _ = std::fs::remove_dir_all(&out);
    // No fallback: the watch loop deploys small local packages over localhost (no ingress, fast
    // flip) — a cut connection there is a genuine failure worth surfacing, not a timeout to poll past.
    block_on(run_deploy_api(
        engine_url,
        bytes,
        ReportFormat::Text,
        None,
        admin_key,
        io,
    ))
    .map_err(|e| e.diagnostic().message.clone())
}

/// The deploy sequence — Secret FIRST, ConfigMap second. Separated from
/// argument/IO handling so the mocked-service tests drive it with an injected [`Client`].
pub(crate) async fn run_deploy(
    client: Client,
    target: &TargetArgs,
    archive_key: &str,
    archive_bytes: Vec<u8>,
    secret_entries: BTreeMap<String, String>,
) -> Result<(), DeployError> {
    if !secret_entries.is_empty() {
        ensure_secret_keys(&client, target, secret_entries).await?;
    }
    upsert_configmap_entry(&client, target, archive_key, archive_bytes).await
}

/// Merge the given keys into the estate Secret; create it when absent ("ensures/patches").
/// A merge patch never disturbs sibling keys.
async fn ensure_secret_keys(
    client: &Client,
    target: &TargetArgs,
    entries: BTreeMap<String, String>,
) -> Result<(), DeployError> {
    let api = secrets_api(client, target);
    let data: BTreeMap<String, ByteString> = entries
        .into_iter()
        .map(|(k, v)| (k, ByteString(v.into_bytes())))
        .collect();
    let patch_body = Secret {
        data: Some(data.clone()),
        ..Default::default()
    };
    match api
        .patch(
            &target.secret_name,
            &PatchParams::default(),
            &Patch::Merge(&patch_body),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if is_not_found(&e) => {
            let mut fresh = Secret {
                data: Some(data),
                ..Default::default()
            };
            fresh.metadata.name = Some(target.secret_name.clone());
            api.create(&PostParams::default(), &fresh)
                .await
                .map_err(|e| api_error("estate Secret create", e))?;
            Ok(())
        }
        Err(e) => Err(api_error("estate Secret patch", e)),
    }
}

/// Upsert the archive as one `binaryData` entry (key = archive file name) after checking
/// the capacity posture. The ConfigMap itself is tofu-provisioned — its absence means
/// the shared instance is not there, and creating it here would deploy into a void (no
/// Deployment mounts it), so that is a typed error, not an upsert.
async fn upsert_configmap_entry(
    client: &Client,
    target: &TargetArgs,
    archive_key: &str,
    archive_bytes: Vec<u8>,
) -> Result<(), DeployError> {
    let api = configmaps_api(client, target);
    let existing = match api.get(&target.configmap).await {
        Ok(cm) => cm,
        Err(e) if is_not_found(&e) => {
            return Err(DeployError::findings(
                codes::TARGET_NOT_PROVISIONED,
                format!(
                    "deployments ConfigMap '{}' does not exist — provision the shared \
                     instance first (tofu apply deploy/k8s-it/shared-scenario)",
                    target.configmap
                ),
            ));
        }
        Err(e) => return Err(api_error("deployments ConfigMap read", e)),
    };

    check_capacity(
        &existing,
        archive_key,
        archive_bytes.len(),
        &target.configmap,
    )?;

    let patch_body = ConfigMap {
        binary_data: Some(BTreeMap::from([(
            archive_key.to_string(),
            ByteString(archive_bytes),
        )])),
        ..Default::default()
    };
    api.patch(
        &target.configmap,
        &PatchParams::default(),
        &Patch::Merge(&patch_body),
    )
    .await
    .map_err(|e| api_error("deployments ConfigMap patch", e))?;
    Ok(())
}

/// Capacity posture: the deployments ConfigMap tops out around 1 MiB of payload
/// (binary entries at base64 wire size). Refuse with the posture note instead of letting
/// the API answer with an opaque `Request entity too large`.
fn check_capacity(
    existing: &ConfigMap,
    new_key: &str,
    new_len: usize,
    configmap_name: &str,
) -> Result<(), DeployError> {
    let mut total = base64_len(new_len);
    if let Some(binary) = &existing.binary_data {
        for (key, value) in binary {
            if key != new_key {
                total += base64_len(value.0.len());
            }
        }
    }
    if let Some(data) = &existing.data {
        for value in data.values() {
            total += value.len();
        }
    }
    if total > CONFIGMAP_CEILING_BYTES {
        return Err(DeployError::findings(
            codes::CONFIGMAP_CAPACITY_EXCEEDED,
            format!(
                "adding this archive would put ConfigMap '{configmap_name}' at ~{total} \
                 payload bytes, past the ~1 MiB ConfigMap ceiling. That ceiling is accepted \
                 for the examples and documented as a posture note for real estates — \
                 split the estate across ConfigMaps/instances or slim the archive"
            ),
        ));
    }
    Ok(())
}

/// The base64 wire size of `len` raw bytes (how a binaryData entry counts against the
/// object-size limit).
fn base64_len(len: usize) -> usize {
    len.div_ceil(3) * 4
}

/// The undeploy sequence: resolve the ConfigMap key from a deploymentId or archive name,
/// remove it (JSON merge patch null), then GC the named Secret keys (explicit only).
/// Returns the removed key.
pub(crate) async fn run_undeploy(
    client: Client,
    target: &TargetArgs,
    deployment: &str,
    gc_secrets: &[String],
) -> Result<String, DeployError> {
    let api = configmaps_api(&client, target);
    let existing = match api.get(&target.configmap).await {
        Ok(cm) => cm,
        Err(e) if is_not_found(&e) => {
            return Err(DeployError::findings(
                codes::TARGET_NOT_PROVISIONED,
                format!(
                    "deployments ConfigMap '{}' does not exist — nothing to undeploy",
                    target.configmap
                ),
            ));
        }
        Err(e) => return Err(api_error("deployments ConfigMap read", e)),
    };

    let key = resolve_entry_key(deployment, existing.binary_data.as_ref())?;

    // JSON merge patch: a null value removes the key; sibling entries stay untouched.
    let mut removal = serde_json::Map::new();
    removal.insert(key.clone(), serde_json::Value::Null);
    let patch = serde_json::json!({ "binaryData": removal });
    api.patch(
        &target.configmap,
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .map_err(|e| api_error("deployments ConfigMap patch", e))?;

    if !gc_secrets.is_empty() {
        let secret_api = secrets_api(&client, target);
        let mut removals = serde_json::Map::new();
        for k in gc_secrets {
            removals.insert(k.clone(), serde_json::Value::Null);
        }
        let patch = serde_json::json!({ "data": removals });
        match secret_api
            .patch(
                &target.secret_name,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
        {
            // A missing Secret means there is nothing to collect — not an error.
            Ok(_) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(api_error("estate Secret garbage-collection patch", e)),
        }
    }
    Ok(key)
}

/// Match the operator's argument to a ConfigMap entry key: the exact key, the key with the
/// `.sutra` extension appended, or — for a `dep-…` deploymentId — the entry whose verified
/// archive identity matches (each candidate goes through the archive reader; entries that
/// fail to verify are skipped, they can never match an id).
fn resolve_entry_key(
    deployment: &str,
    entries: Option<&BTreeMap<String, ByteString>>,
) -> Result<String, DeployError> {
    let empty = BTreeMap::new();
    let entries = entries.unwrap_or(&empty);
    if entries.contains_key(deployment) {
        return Ok(deployment.to_string());
    }
    let with_extension = format!("{deployment}.{}", sutra_loader::archive::ARCHIVE_EXTENSION);
    if entries.contains_key(&with_extension) {
        return Ok(with_extension);
    }
    if deployment.starts_with("dep-") {
        for (key, value) in entries {
            if let Ok(archive) = sutra_loader::read_archive(&value.0) {
                if archive.id.value() == deployment {
                    return Ok(key.clone());
                }
            }
        }
    }
    let available = entries.keys().cloned().collect::<Vec<_>>().join(", ");
    Err(DeployError::findings(
        codes::DEPLOYMENT_NOT_FOUND,
        format!(
            "no deployments-ConfigMap entry matches '{deployment}' (available: \
             [{available}])"
        ),
    ))
}

// ==================================================================================
// Secret input parsing
// ==================================================================================

/// Fold `--secret KEY=VALUE` flags and the `--secret-from` file into one key→value map
/// (file first, flags shadow — the explicit flag wins).
fn collect_secret_entries(
    flags: &[String],
    from_file: Option<&std::path::Path>,
) -> Result<BTreeMap<String, String>, DeployError> {
    let mut out = BTreeMap::new();
    if let Some(path) = from_file {
        let text = std::fs::read_to_string(path).map_err(|e| {
            DeployError::usage(
                codes::SECRET_INPUT_INVALID,
                format!("cannot read --secret-from {}: {e}", path.display()),
            )
        })?;
        for (line_no, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = split_secret_entry(line).map_err(|msg| {
                DeployError::usage(
                    codes::SECRET_INPUT_INVALID,
                    format!("{}:{}: {msg}", path.display(), line_no + 1),
                )
            })?;
            out.insert(key, value);
        }
    }
    for flag in flags {
        let (key, value) = split_secret_entry(flag)
            .map_err(|msg| DeployError::usage(codes::SECRET_INPUT_INVALID, msg))?;
        out.insert(key, value);
    }
    Ok(out)
}

/// One `KEY=VALUE` (split at the first `=`; the value may itself contain `=`).
fn split_secret_entry(raw: &str) -> Result<(String, String), String> {
    match raw.split_once('=') {
        Some((key, value)) if !key.trim().is_empty() => {
            Ok((key.trim().to_string(), value.to_string()))
        }
        _ => Err(format!("invalid secret entry '{raw}' (expected KEY=VALUE)")),
    }
}

/// Poll the engine's readiness endpoints until THIS deployment is Active, fail fast if
/// its slot is Rejected, or time out. Activation is async (kubelet syncs the ConfigMap volume,
/// then the engine's deployments watcher flips) so a fresh deploy is `Unknown` (404) for a
/// while — that is not
/// an error, we keep polling. A `Failed` slot (bad bytes) IS terminal → return immediately.
async fn wait_for_active(
    engine_url: &str,
    deployment_id: &str,
    slot: &str,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let base = engine_url.trim_end_matches('/');
    let by_id = format!("{base}/sutra/deployments/{deployment_id}");
    let list = format!("{base}/sutra/deployments");
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        // Active? (the by-id endpoint keys on the content-hash deploymentId)
        if let Ok(resp) = client.get(&by_id).send().await {
            if resp.status().as_u16() == 200 {
                if let Ok(v) = resp.json::<serde_json::Value>().await {
                    if v["phase"] == "Active" {
                        return Ok(());
                    }
                }
            }
        }
        // Rejected? (the slot's bytes failed verification — terminal, fail fast)
        if let Ok(resp) = client.get(&list).send().await {
            if let Ok(v) = resp.json::<serde_json::Value>().await {
                if let Some(f) = v["failed"]
                    .as_array()
                    .and_then(|a| a.iter().find(|f| f["slot"] == slot))
                {
                    return Err(format!(
                        "engine REJECTED the deployment: {}",
                        f["error"].as_str().unwrap_or("(no detail)")
                    ));
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out after {}s waiting for {deployment_id} to become Active",
                timeout.as_secs()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

// ==================================================================================
// Tests — mocked API layer (kube's tower-test mock service; NO live-cluster calls)
// ==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawn a one-shot-per-connection stub HTTP server; `respond(request) -> (status_line, body)`.
    fn stub_engine(respond: impl Fn(&str) -> (&'static str, String) + Send + 'static) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                let mut buf = [0u8; 2048];
                let n = s.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let (status_line, body) = respond(&req);
                let resp = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    #[test]
    fn wait_for_active_returns_ok_when_engine_reports_active() {
        let url = stub_engine(|_req| {
            (
                "200 OK",
                r#"{"deploymentId":"dep-abc","slot":"app.sutra","phase":"Active","ready":true}"#
                    .to_string(),
            )
        });
        let out = block_on(wait_for_active(
            &url,
            "dep-abc",
            "app.sutra",
            std::time::Duration::from_secs(5),
        ));
        assert!(out.is_ok(), "expected Active, got {out:?}");
    }

    #[test]
    fn run_deploy_api_async_accepts_202_then_polls_to_active() {
        // The async path: POST returns 202 {Pending}; the CLI then polls the by-id status
        // endpoint until Active. The stub answers POSTs with 202 and GETs with Active.
        let url = stub_engine(|req| {
            if req.starts_with("POST") {
                (
                    "202 Accepted",
                    r#"{"deploymentId":"dep-async","slot":"t--m--v","revision":1,"phase":"Pending"}"#
                        .to_string(),
                )
            } else {
                (
                    "200 OK",
                    r#"{"deploymentId":"dep-async","slot":"t--m--v","phase":"Active","ready":true}"#
                        .to_string(),
                )
            }
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let res = block_on(run_deploy_api_async(
            &url,
            b"bytes".to_vec(),
            ReportFormat::Text,
            std::time::Duration::from_secs(5),
            None,
            &mut io,
        ));
        assert!(res.is_ok(), "expected Active, got {res:?}");
        assert!(
            String::from_utf8_lossy(&out).contains("Active"),
            "stdout should report Active: {}",
            String::from_utf8_lossy(&out)
        );
    }

    #[test]
    fn run_deploy_api_sends_the_admin_key_header() {
        // The stub answers 200 only when the request carries the admin key header (name is
        // case-insensitive on the wire); otherwise 401 — proving the CLI sends it.
        let url = stub_engine(|req| {
            if req.to_lowercase().contains("x-api-key: sekret") {
                (
                    "200 OK",
                    r#"{"deploymentId":"dep-x","slot":"t--m--v","revision":1,"phase":"Active"}"#
                        .to_string(),
                )
            } else {
                (
                    "401 Unauthorized",
                    r#"{"error":"missing admin key"}"#.to_string(),
                )
            }
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let res = block_on(run_deploy_api(
            &url,
            b"bytes".to_vec(),
            ReportFormat::Text,
            None,
            Some(("X-API-Key", "sekret")),
            &mut io,
        ));
        assert!(
            res.is_ok(),
            "the key header should authorize the deploy: {res:?}"
        );
    }

    #[test]
    fn run_undeploy_api_reports_draining_on_success() {
        let url = stub_engine(|_req| {
            (
                "200 OK",
                r#"{"slot":"t--m--v","phase":"Draining"}"#.to_string(),
            )
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let res = block_on(run_undeploy_api(
            &url,
            "t--m--v",
            ReportFormat::Text,
            None,
            &mut io,
        ));
        assert!(res.is_ok(), "expected success, got {res:?}");
        assert!(String::from_utf8_lossy(&out).contains("Draining"));
    }

    #[test]
    fn run_undeploy_api_404_is_a_not_found_finding() {
        let url = stub_engine(|_req| {
            (
                "404 Not Found",
                r#"{"error":"no active deployment for slot 't--m--v'"}"#.to_string(),
            )
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let res = block_on(run_undeploy_api(
            &url,
            "t--m--v",
            ReportFormat::Text,
            None,
            &mut io,
        ));
        let e = res.expect_err("404 must be a finding");
        assert_eq!(e.diagnostic().code, codes::DEPLOYMENT_NOT_FOUND);
    }

    #[test]
    fn run_deploy_api_sync_falls_back_to_status_poll_on_5xx() {
        // A 5xx (an ingress read-timeout injecting 502/504 on a long flip) is NOT a rejection: with
        // a fallback context the CLI polls the status endpoint, and a succeeded-server-side deploy
        // resolves to Active instead of a false failure.
        let url = stub_engine(|req| {
            if req.starts_with("POST") {
                (
                    "504 Gateway Timeout",
                    r#"{"error":"upstream timed out"}"#.to_string(),
                )
            } else {
                (
                    "200 OK",
                    r#"{"deploymentId":"dep-slow","slot":"t--m--v","phase":"Active","ready":true}"#
                        .to_string(),
                )
            }
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let fallback = Some((
            "dep-slow".to_string(),
            "t--m--v".to_string(),
            std::time::Duration::from_secs(5),
        ));
        let res = block_on(run_deploy_api(
            &url,
            b"bytes".to_vec(),
            ReportFormat::Text,
            fallback,
            None,
            &mut io,
        ));
        assert!(
            res.is_ok(),
            "5xx + succeeded deploy should resolve Active, got {res:?}"
        );
        assert!(String::from_utf8_lossy(&out).contains("Active"));
    }

    #[test]
    fn run_deploy_api_sync_4xx_fails_fast_without_polling() {
        // A 4xx is a genuine rejection — fail fast even with a fallback context (no status poll).
        let url = stub_engine(|_req| {
            (
                "400 Bad Request",
                r#"{"error":"archive rejected: bad digest"}"#.to_string(),
            )
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let fallback = Some((
            "dep-x".to_string(),
            "t--m--v".to_string(),
            std::time::Duration::from_secs(5),
        ));
        let res = block_on(run_deploy_api(
            &url,
            b"bytes".to_vec(),
            ReportFormat::Text,
            fallback,
            None,
            &mut io,
        ));
        let e = res.expect_err("4xx must fail fast");
        assert!(
            e.diagnostic().message.contains("bad digest"),
            "{:?}",
            e.diagnostic()
        );
    }

    #[test]
    fn run_deploy_api_async_fails_when_post_is_rejected() {
        // A validation failure fails synchronously at the POST (4xx) — no poll.
        let url = stub_engine(|_req| {
            (
                "400 Bad Request",
                r#"{"error":"archive rejected: bad manifest"}"#.to_string(),
            )
        });
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        let res = block_on(run_deploy_api_async(
            &url,
            b"bytes".to_vec(),
            ReportFormat::Text,
            std::time::Duration::from_secs(5),
            None,
            &mut io,
        ));
        let e = res.expect_err("a rejected POST must error");
        assert!(
            e.diagnostic().message.contains("bad manifest"),
            "{:?}",
            e.diagnostic()
        );
    }

    #[test]
    fn wait_for_active_fails_fast_when_slot_rejected() {
        // by-id → 404 Unknown; the list reports the slot Failed → terminal, no timeout wait.
        let url = stub_engine(|req| {
            if req.contains("/sutra/deployments/dep-") {
                (
                    "404 Not Found",
                    r#"{"deploymentId":"dep-bad","phase":"Unknown","ready":false}"#.to_string(),
                )
            } else {
                (
                    "200 OK",
                    r#"{"active":[],"draining":[],"failed":[{"slot":"app.sutra","phase":"Failed","error":"bad archive"}]}"#
                        .to_string(),
                )
            }
        });
        let out = block_on(wait_for_active(
            &url,
            "dep-bad",
            "app.sutra",
            std::time::Duration::from_secs(5),
        ));
        let err = out.expect_err("expected fail-fast on a rejected slot");
        assert!(
            err.contains("bad archive"),
            "error carries engine detail: {err}"
        );
    }
    use http::{Request, Response};
    use kube::client::Body;

    /// One scripted API exchange: assert on the request, answer with a canned response.
    struct Exchange {
        method: &'static str,
        path_contains: &'static str,
        /// substrings the request body must contain (empty for GETs).
        body_contains: Vec<String>,
        status: u16,
        response_body: serde_json::Value,
    }

    /// Drive `f(client)` against a mock API scripted with `exchanges`, asserting each
    /// request arrives in order with the expected method/path/body.
    fn with_mock_api<T, Fut>(exchanges: Vec<Exchange>, f: impl FnOnce(Client) -> Fut) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let (mock_service, mut handle) =
                tower_test::mock::pair::<Request<Body>, Response<Body>>();
            let client = Client::new(mock_service, "default");
            let script = tokio::spawn(async move {
                for (i, exchange) in exchanges.into_iter().enumerate() {
                    let (request, send) = handle
                        .next_request()
                        .await
                        .unwrap_or_else(|| panic!("expected API call #{i} was never made"));
                    assert_eq!(
                        request.method().as_str(),
                        exchange.method,
                        "call #{i} method"
                    );
                    let path = request.uri().path().to_string();
                    assert!(
                        path.contains(exchange.path_contains),
                        "call #{i}: path '{path}' should contain '{}'",
                        exchange.path_contains
                    );
                    let body_bytes = request.into_body().collect_bytes().await.expect("body");
                    let body = String::from_utf8_lossy(&body_bytes);
                    for expected in &exchange.body_contains {
                        assert!(
                            body.contains(expected.as_str()),
                            "call #{i}: body '{body}' should contain '{expected}'"
                        );
                    }
                    send.send_response(
                        Response::builder()
                            .status(exchange.status)
                            .header("content-type", "application/json")
                            .body(Body::from(
                                serde_json::to_vec(&exchange.response_body).unwrap(),
                            ))
                            .unwrap(),
                    );
                }
            });
            let outcome = f(client).await;
            script.await.expect("scripted exchanges all satisfied");
            outcome
        })
    }

    fn target() -> TargetArgs {
        TargetArgs {
            context: None,
            namespace: None,
            configmap: "sutra-deployments".to_string(),
            secret_name: "sutra-secrets".to_string(),
        }
    }

    fn secret_json() -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "sutra-secrets", "namespace": "default"}
        })
    }

    fn configmap_json(binary: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "sutra-deployments", "namespace": "default"},
            "binaryData": binary
        })
    }

    fn status_404() -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "Status", "status": "Failure",
            "reason": "NotFound", "code": 404, "message": "not found"
        })
    }

    fn b64(bytes: &[u8]) -> String {
        serde_json::to_value(ByteString(bytes.to_vec()))
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    // ---- deploy ---------------------------------------------------------------------

    #[test]
    fn deploy_patches_the_estate_secret_before_the_configmap() {
        // Ordering: Secret merge FIRST, then ConfigMap read + entry merge. The scripted
        // sequence IS the ordering assertion — a ConfigMap call arriving first fails the
        // method/path expectations of exchange #0.
        let archive = b"fake-archive-bytes".to_vec();
        let secrets = BTreeMap::from([("ORDERS_DB_PASSWORD".to_string(), "pw".to_string())]);
        let exchanges = vec![
            Exchange {
                method: "PATCH",
                path_contains: "/api/v1/namespaces/default/secrets/sutra-secrets",
                body_contains: vec!["ORDERS_DB_PASSWORD".into(), b64(b"pw")],
                status: 200,
                response_body: secret_json(),
            },
            Exchange {
                method: "GET",
                path_contains: "/api/v1/namespaces/default/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/api/v1/namespaces/default/configmaps/sutra-deployments",
                body_contains: vec!["pkg.sutra".into(), b64(b"fake-archive-bytes")],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "pkg.sutra", archive, secrets).await
        })
        .expect("deploy succeeds");
    }

    #[test]
    fn deploy_creates_the_estate_secret_when_it_is_absent() {
        // "ensures/patches": a 404 on the merge patch falls back to create — still
        // strictly BEFORE any ConfigMap call.
        let archive = b"bytes".to_vec();
        let secrets = BTreeMap::from([("K".to_string(), "v".to_string())]);
        let exchanges = vec![
            Exchange {
                method: "PATCH",
                path_contains: "/secrets/sutra-secrets",
                body_contains: vec!["K".into()],
                status: 404,
                response_body: status_404(),
            },
            Exchange {
                method: "POST",
                path_contains: "/api/v1/namespaces/default/secrets",
                body_contains: vec!["sutra-secrets".into(), b64(b"v")],
                status: 201,
                response_body: secret_json(),
            },
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["a.sutra".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "a.sutra", archive, secrets).await
        })
        .expect("deploy with secret-create succeeds");
    }

    #[test]
    fn deploy_without_secret_flags_skips_the_secret_entirely() {
        let archive = b"bytes".to_vec();
        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["a.sutra".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "a.sutra", archive, BTreeMap::new()).await
        })
        .expect("deploy without secrets succeeds");
    }

    #[test]
    fn deploy_refuses_when_the_shared_instance_is_not_provisioned() {
        // ConfigMap 404 = the shared scenario has not been applied — a typed finding naming
        // the operator step, never a client-side create into a void.
        let archive = b"bytes".to_vec();
        let exchanges = vec![Exchange {
            method: "GET",
            path_contains: "/configmaps/sutra-deployments",
            body_contains: vec![],
            status: 404,
            response_body: status_404(),
        }];
        let t = target();
        let err = with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "a.sutra", archive, BTreeMap::new()).await
        })
        .expect_err("must refuse");
        assert_eq!(err.diagnostic().code, codes::TARGET_NOT_PROVISIONED);
        assert!(err
            .diagnostic()
            .message
            .contains("deploy/k8s-it/shared-scenario"));
        assert_eq!(err.exit_code(), exit::FINDINGS);
    }

    #[test]
    fn deploy_over_the_ceiling_names_the_posture_note_and_never_patches() {
        // An existing ~900 KiB entry + a ~200 KiB archive crosses ~1 MiB: the friendly
        // capacity error fires after the GET, and NO patch call follows (the script would
        // fail on an unexpected fourth exchange... there are only two scripted).
        let existing = vec![0u8; 900 * 1024];
        let archive = vec![1u8; 200 * 1024];
        let exchanges = vec![Exchange {
            method: "GET",
            path_contains: "/configmaps/sutra-deployments",
            body_contains: vec![],
            status: 200,
            response_body: configmap_json(serde_json::json!({ "big.sutra": b64(&existing) })),
        }];
        let t = target();
        let err = with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "new.sutra", archive, BTreeMap::new()).await
        })
        .expect_err("capacity refusal");
        assert_eq!(err.diagnostic().code, codes::CONFIGMAP_CAPACITY_EXCEEDED);
        assert!(err.diagnostic().message.contains("1 MiB"));
        assert!(err.diagnostic().message.contains("posture note"));
    }

    #[test]
    fn redeploying_the_same_key_counts_the_replacement_not_the_sum() {
        // Upserting over an existing entry replaces it: a 700 KiB archive (≈933 KiB at
        // base64 wire size) re-deployed at the same size stays under the ceiling because
        // the old entry's size is excluded — counting both would cross it.
        let bytes = vec![0u8; 700 * 1024];
        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({ "big.sutra": b64(&bytes) })),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["big.sutra".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        with_mock_api(exchanges, move |client| async move {
            run_deploy(client, &t, "big.sutra", bytes, BTreeMap::new()).await
        })
        .expect("same-key redeploy fits");
    }

    // ---- undeploy -------------------------------------------------------------------

    #[test]
    fn undeploy_removes_the_configmap_key_with_a_null_merge_patch() {
        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({ "a.sutra": b64(b"x") })),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["\"a.sutra\":null".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        let removed = with_mock_api(exchanges, move |client| async move {
            run_undeploy(client, &t, "a.sutra", &[]).await
        })
        .expect("undeploy succeeds");
        assert_eq!(removed, "a.sutra");
    }

    #[test]
    fn undeploy_accepts_the_archive_name_without_extension() {
        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({ "orders.sutra": b64(b"x") })),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["\"orders.sutra\":null".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        let removed = with_mock_api(exchanges, move |client| async move {
            run_undeploy(client, &t, "orders", &[]).await
        })
        .expect("undeploy by bare name succeeds");
        assert_eq!(removed, "orders.sutra");
    }

    #[test]
    fn undeploy_gc_secrets_null_patches_only_the_named_keys_after_the_configmap() {
        let gc = vec!["ORDERS_DB_PASSWORD".to_string()];
        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({ "a.sutra": b64(b"x") })),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec!["\"a.sutra\":null".into()],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/secrets/sutra-secrets",
                body_contains: vec!["\"ORDERS_DB_PASSWORD\":null".into()],
                status: 200,
                response_body: secret_json(),
            },
        ];
        let t = target();
        with_mock_api(exchanges, move |client| async move {
            run_undeploy(client, &t, "a.sutra", &gc).await
        })
        .expect("undeploy with gc succeeds");
    }

    #[test]
    fn undeploy_unknown_entry_is_a_finding_listing_what_is_there() {
        let exchanges = vec![Exchange {
            method: "GET",
            path_contains: "/configmaps/sutra-deployments",
            body_contains: vec![],
            status: 200,
            response_body: configmap_json(serde_json::json!({ "other.sutra": b64(b"x") })),
        }];
        let t = target();
        let err = with_mock_api(exchanges, move |client| async move {
            run_undeploy(client, &t, "ghost.sutra", &[]).await
        })
        .expect_err("unknown entry refused");
        assert_eq!(err.diagnostic().code, codes::DEPLOYMENT_NOT_FOUND);
        assert!(err.diagnostic().message.contains("other.sutra"));
        assert_eq!(err.exit_code(), exit::FINDINGS);
    }

    #[test]
    fn undeploy_resolves_a_deployment_id_through_the_verifying_reader() {
        // Seal a REAL archive (the reader only accepts verified content), stage it
        // as a ConfigMap entry, and resolve by its dep- id.
        let pkg_dir = crate::commands::lint::tests::valid_package_dir("deploy-id-resolve");
        let out_dir = crate::test_fixtures::scratch_dir("deploy-id-out");
        let outcome = sutra_loader::assemble_dir(
            &pkg_dir,
            &out_dir,
            &sutra_loader::PackageOptions::default(),
        )
        .expect("package seals");
        let sealed = &outcome.archives[0];
        let id = sealed.id.value().to_string();
        let bytes = std::fs::read(&sealed.file_path).expect("archive bytes");
        let key = sealed
            .file_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let exchanges = vec![
            Exchange {
                method: "GET",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![],
                status: 200,
                response_body: configmap_json(serde_json::json!({ key.clone(): b64(&bytes) })),
            },
            Exchange {
                method: "PATCH",
                path_contains: "/configmaps/sutra-deployments",
                body_contains: vec![format!("\"{key}\":null")],
                status: 200,
                response_body: configmap_json(serde_json::json!({})),
            },
        ];
        let t = target();
        let expected_key = key.clone();
        let removed = with_mock_api(exchanges, move |client| async move {
            run_undeploy(client, &t, &id, &[]).await
        })
        .expect("undeploy by deploymentId succeeds");
        assert_eq!(removed, expected_key);
        std::fs::remove_dir_all(pkg_dir).ok();
        std::fs::remove_dir_all(out_dir).ok();
    }

    // ---- pure helpers -----------------------------------------------------------------

    #[test]
    fn secret_entries_fold_file_then_flags_with_flags_shadowing() {
        let file = crate::test_fixtures::scratch_file(
            "deploy-secret-from",
            "secrets.env",
            "# estate credentials\nA=1\nB=from-file\n\n",
        );
        let entries =
            collect_secret_entries(&["B=from-flag".to_string(), "C=3".to_string()], Some(&file))
                .unwrap();
        assert_eq!(entries["A"], "1");
        assert_eq!(entries["B"], "from-flag");
        assert_eq!(entries["C"], "3");
        // The value may contain '='.
        let eq =
            collect_secret_entries(&["URL=postgres://db?sslmode=off".to_string()], None).unwrap();
        assert_eq!(eq["URL"], "postgres://db?sslmode=off");
        // Not KEY=VALUE → usage error.
        assert!(collect_secret_entries(&["broken".to_string()], None).is_err());
        std::fs::remove_dir_all(file.parent().unwrap()).ok();
    }

    #[test]
    fn capacity_math_uses_base64_wire_size() {
        assert_eq!(base64_len(0), 0);
        assert_eq!(base64_len(1), 4);
        assert_eq!(base64_len(3), 4);
        assert_eq!(base64_len(4), 8);
        // 786_432 raw bytes = 1_048_576 base64 bytes — exactly the ceiling: still allowed.
        let cm = ConfigMap::default();
        assert!(check_capacity(&cm, "a.sutra", 786_432, "cm").is_ok());
        assert!(check_capacity(&cm, "a.sutra", 786_433, "cm").is_err());
    }
}
