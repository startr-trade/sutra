//! sutra-engine — the Rust engine binary. main() = telemetry bootstrap
//! (structured-JSON stdout logs always; OTLP traces + metrics + logs when an
//! endpoint is configured) → load config → serve. Telemetry is fail-open: exporter
//! trouble degrades to JSON-logs-only, never refuses boot.

use tracing::{error, info};

// Force-link the built-in payload codec crates so their `inventory::submit!` registrations
// survive linker DCE — the neutral engine collects them generically via
// `sutra_codec_spi::builtin_codecs()`/`builtin_formats()` and never references them by type.
// Bundling lives in the BINARY; the engine library stays domain-neutral.
//
// The public distribution bundles the schema-less FORMATS only. Message-standard codecs are
// proprietary extension crates in a separate repository whose own composition root adds a line
// exactly like this one — no engine change required.
use sutra_formats as _;

// Force-link the vendor envref resolver crates for the same reason: each self-registers a
// `sutra_envref_spi::EnvRefResolverEntry` via `inventory` (the `vault:` + `aws-secrets:` +
// `azure-kv:` + `gcp-secret:` secret-ref schemes), and the neutral engine collects them
// generically through `sutra_envref_spi::ResolverRegistry::with_builtins()` — it never references
// them by type, so without these the linker would drop the crates and their resolver
// registrations. Bundling lives in the BINARY; the engine library names no vendor (no vaultrs /
// AWS SDK dep, and the azure/gcp resolvers talk REST over reqwest/rustls — no vendor SDK at all).
use sutra_envref_aws as _;
use sutra_envref_azure as _;
use sutra_envref_gcp as _;
use sutra_envref_vault as _;

// No redactor crate is force-linked here. A redactor is inherently data-domain-specific — it
// encodes what a sensitive field looks like — so the public distribution bundles none, exactly as
// it bundles no message-standard codec. A concrete redactor is an extension crate that
// `inventory::submit!`s a `sutra_redactor_spi::RedactorEntry` (keyed `urn:sutra:redactor:<name>`)
// and is force-linked by whichever distribution wants it; `RedactorRegistry::with_builtins()`
// collects whatever the binary linked and names none. The deployment-scoped
// `sutra-redactor-template` needs no force-link — the archive carries its template and the
// deploy-time assembly instantiates it directly.

// Force-link the transport crates for the same reason: each self-registers a
// `sutra_transport_spi::TransportFactory` via `inventory`, and the neutral engine composes
// them generically through `transport_factories()` — it never references a transport crate by
// type, so without these the linker would drop the unreferenced crates and their factories.
// Each force-link is behind its cargo feature (see Cargo.toml [features]), so the BINARY
// selects the bundled set — a hardened / air-gapped build links only what it enables and none
// of the other vendor client crates are compiled.
#[cfg(feature = "amqp")]
use sutra_transport_amqp as _;
#[cfg(feature = "dapr")]
use sutra_transport_dapr as _;
#[cfg(feature = "file")]
use sutra_transport_file as _;
#[cfg(feature = "gcp-pubsub")]
use sutra_transport_gcp_pubsub as _;
#[cfg(feature = "http")]
use sutra_transport_http as _;
#[cfg(feature = "kafka")]
use sutra_transport_kafka as _;
#[cfg(feature = "knative")]
use sutra_transport_knative as _;
#[cfg(feature = "rabbitmq")]
use sutra_transport_rabbitmq as _;
#[cfg(feature = "aws-sqs")]
use sutra_transport_sqs as _;

/// `--health-check` — probe this container's own readiness endpoint and exit 0/1.
///
/// The runtime image is distroless: no shell, no curl, no wget. A Docker `HEALTHCHECK`
/// therefore has exactly one executable available to it — this binary — so the probe ships
/// as a mode of the engine itself. It talks to `127.0.0.1` on the port the engine was told
/// to bind, reads nothing but the status code, and never touches the database.
///
/// Kubernetes does not use this (its probes hit `/sutra/health/ready` directly over the
/// network); it exists for `docker run`, Compose, and anything else that gates on Docker's
/// own health state.
async fn health_check() -> ! {
    let port = std::env::var("SUTRA_HTTP_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);
    let url = format!("http://127.0.0.1:{port}/sutra/health/ready");
    let outcome = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build();
    let code = match outcome {
        Ok(client) => match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => 0,
            // Any answer other than 2xx is unhealthy — including the 503 the readiness
            // endpoint returns when a lane has died.
            Ok(response) => {
                eprintln!("health-check: {url} -> HTTP {}", response.status());
                1
            }
            Err(e) => {
                eprintln!("health-check: {url} -> {e}");
                1
            }
        },
        Err(e) => {
            eprintln!("health-check: client build failed: {e}");
            1
        }
    };
    std::process::exit(code);
}

#[tokio::main]
async fn main() {
    // The health probe runs BEFORE any telemetry or config work: it is a short-lived
    // sidecar invocation of the same image, not an engine boot.
    if std::env::args().any(|a| a == "--health-check") {
        health_check().await;
    }

    // Telemetry first so even config errors log through the structured stack.
    // RUST_LOG overrides the default `info` level.
    let telemetry_config = sutra_engine::TelemetryConfig::load();
    let telemetry = sutra_engine::otel::init(&telemetry_config);

    let config = match sutra_engine::EngineConfig::load() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "configuration invalid — refusing to start");
            telemetry.shutdown();
            std::process::exit(1);
        }
    };
    info!(
        deployments_dir = config
            .deployments_dir
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        http_port = config.http_port,
        has_datasource = config.datasource_url.is_some(),
        telemetry_active = config.telemetry.is_active(),
        "sutra-engine starting"
    );

    // join_graceful: the engine is PID 1 in its container — it must handle SIGTERM /
    // SIGINT itself (drain consumers, release leases, flush telemetry) or a platform
    // roll leaves the old replica consuming until the hard kill.
    let exit_code = match sutra_engine::serve(config).await {
        Ok(engine) => match engine.join_graceful().await {
            Ok(()) => 0,
            Err(e) => {
                error!(error = %e, "server terminated");
                1
            }
        },
        Err(e) => {
            error!(error = %e, "startup failed — refusing to serve");
            1
        }
    };

    // Flush + stop the OTLP exporters before the process goes away.
    telemetry.shutdown();
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
