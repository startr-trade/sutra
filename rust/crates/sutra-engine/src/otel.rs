//! OTLP telemetry — exporter bootstrap, the `tracing`→OpenTelemetry layers,
//! the metrics [`ExecutionListener`], the W3C `traceparent` bridge helpers, and
//! the structured-JSON stdout log layer.
//!
//! ## Signals
//! - **Traces** — the EXISTING `tracing` spans (`sutra.dispatch` / `sutra.resolve` /
//!   `sutra.decode` / `sutra.validate` / `sutra.execute` / `sutra.outbox.send`, dotted
//!   span fields = OTel attribute names) export through a [`tracing_opentelemetry`]
//!   layer; no call site changes.
//! - **Metrics** — [`OtelMetricsListener`] maps the executor lifecycle bus onto the
//!   frozen `sutra.*` meter names (`sutra.instance.*`, `sutra.token.*`, `sutra.task.*`,
//!   `sutra.coverage.path_covered`) with `deployment.id` + the label allowlist as
//!   dimensions. Delta temporality is honored from the standard
//!   `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` env (the ES exporter drops
//!   cumulative histograms).
//! - **Logs** — stdout keeps structured JSON in the frozen field shape
//!   (`timestamp`/`level`/`loggerName`/`message`/`service.name` +
//!   `traceId`/`spanId` inside a sampled span) the EFK stack's `sutra_json`
//!   Fluent Bit parser (an infra-side contract name) decodes unchanged; when an OTLP
//!   endpoint is configured, log records ALSO export over OTLP (the `sutra-app-logs`
//!   index path — the k8s-it Fluent Bit excludes engine pods, so OTLP is the only
//!   route into that index).
//!
//! ## Fail-open
//! Telemetry must never affect message processing: a missing endpoint disables export
//! entirely (zero overhead), a bad config value falls back to defaults with a WARN, and
//! exporter build/ship failures only log — they never propagate into intake/dispatch.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TaskEvent, TokenEvent};
use sutra_executor::metric_flag_urn;
use sutra_executor::telemetry as names;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::Layer as _;

use crate::envref;

// =====================================================================================
// Configuration
// =====================================================================================

/// Metrics temporality preference (`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`).
/// The k8s harness sets `delta` — the Elasticsearch exporter drops cumulative
/// histograms, so delta is what actually lands `sutra.task.duration` in `metrics-*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemporalityPreference {
    /// The OTel spec default.
    #[default]
    Cumulative,
    Delta,
    LowMemory,
}

impl TemporalityPreference {
    /// Lenient spec parse (`cumulative` / `delta` / `lowmemory`, case-insensitive);
    /// `None` for anything else (caller falls back to the default with a WARN).
    fn parse(raw: &str) -> Option<TemporalityPreference> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "cumulative" => Some(TemporalityPreference::Cumulative),
            "delta" => Some(TemporalityPreference::Delta),
            "lowmemory" => Some(TemporalityPreference::LowMemory),
            _ => None,
        }
    }

    fn as_sdk(self) -> Temporality {
        match self {
            TemporalityPreference::Cumulative => Temporality::Cumulative,
            TemporalityPreference::Delta => Temporality::Delta,
            TemporalityPreference::LowMemory => Temporality::LowMemory,
        }
    }
}

/// Resolved telemetry configuration — canonical `sutra.*` keys plus the vendor-neutral
/// standard `OTEL_*` env names (there is no framework-alias env layer;
/// precedence: canonical env > standard `OTEL_*` env > config file > default):
///
/// | key (file)                                    | canonical env                                   | standard env                                                          |
/// |-----------------------------------------------|-------------------------------------------------|------------------------------------------------------------------------|
/// | `sutra.telemetry.otlp.endpoint`               | `SUTRA_TELEMETRY_OTLP_ENDPOINT`                 | `OTEL_EXPORTER_OTLP_ENDPOINT`                                          |
/// | `sutra.telemetry.service-name`                | `SUTRA_TELEMETRY_SERVICE_NAME`                  | `OTEL_SERVICE_NAME`                                                    |
/// | `sutra.telemetry.metrics.temporality-preference` | `SUTRA_TELEMETRY_METRICS_TEMPORALITY_PREFERENCE` | `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`                 |
/// | `sutra.telemetry.metrics.export-interval`     | `SUTRA_TELEMETRY_METRICS_EXPORT_INTERVAL`       | `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds)                           |
/// | `sutra.telemetry.metric-labels`               | `SUTRA_TELEMETRY_METRIC_LABELS`                 | — (comma-separated allowlist; default `tenant,module,version`)         |
/// | `sutra.telemetry.enabled`                     | `SUTRA_TELEMETRY_ENABLED`                       | `OTEL_SDK_DISABLED` (inverted)                                         |
///
/// No endpoint configured ⇒ telemetry is OFF: no exporters, no OTel layers, no metrics
/// listener — boot and hot paths are unaffected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    /// OTLP gRPC endpoint (`http://host:4317`). `None` disables all export.
    pub otlp_endpoint: Option<String>,
    /// `service.name` resource attribute (and the JSON log additional-field).
    pub service_name: String,
    /// Metrics temporality preference (harness sets `delta`).
    pub metrics_temporality: TemporalityPreference,
    /// Metric export interval in milliseconds (`None` = SDK default, 60 s).
    pub metrics_export_interval_ms: Option<u64>,
    /// Label-dimension allowlist applied to instance/coverage meters.
    pub metric_labels: Vec<String>,
    /// Kill switch (`false` or `OTEL_SDK_DISABLED=true` forces telemetry off).
    pub enabled: bool,
    /// Config keys that failed to resolve/parse (fail-open — reported at init).
    pub warnings: Vec<String>,
}

impl Default for TelemetryConfig {
    fn default() -> TelemetryConfig {
        TelemetryConfig {
            otlp_endpoint: None,
            service_name: "sutra-engine".to_string(),
            metrics_temporality: TemporalityPreference::default(),
            metrics_export_interval_ms: None,
            metric_labels: default_metric_labels(),
            enabled: true,
            warnings: Vec::new(),
        }
    }
}

/// The default label-dimension allowlist.
fn default_metric_labels() -> Vec<String> {
    vec![
        "tenant".to_string(),
        "module".to_string(),
        "version".to_string(),
    ]
}

impl TelemetryConfig {
    /// Load from the process environment + the optional `SUTRA_CONFIG` properties file.
    /// NEVER fails (fail-open): unreadable files / bad values degrade to defaults with
    /// warnings surfaced by [`init`].
    pub fn load() -> TelemetryConfig {
        let path = std::env::var("SUTRA_CONFIG").unwrap_or_else(|_| "sutra.properties".into());
        let file = read_properties_lenient(std::path::Path::new(&path));
        TelemetryConfig::from_sources(&file, &|name| std::env::var(name).ok())
    }

    /// Pure resolution over explicit sources (unit-testable without process-global env).
    pub fn from_sources(
        file: &BTreeMap<String, String>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> TelemetryConfig {
        let mut warnings = Vec::new();
        // canonical env > aliases (in order) > file; `${ENV}` indirection resolves
        // fail-open (an unresolvable reference drops the value with a warning).
        fn value(
            file: &BTreeMap<String, String>,
            env: &dyn Fn(&str) -> Option<String>,
            warnings: &mut Vec<String>,
            file_key: &str,
            envs: &[&str],
        ) -> Option<String> {
            let raw = envs
                .iter()
                .find_map(|name| env(name))
                .or_else(|| file.get(file_key).cloned())?;
            match envref::resolve_placeholders(&raw) {
                Ok(resolved) => Some(resolved),
                Err(e) => {
                    warnings.push(format!("telemetry key '{file_key}': {e} — value ignored"));
                    None
                }
            }
        }

        let otlp_endpoint = value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.otlp.endpoint",
            &[
                "SUTRA_TELEMETRY_OTLP_ENDPOINT",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
            ],
        )
        .map(|e| e.trim().to_string())
        .filter(|e| !e.is_empty());

        let service_name = value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.service-name",
            &["SUTRA_TELEMETRY_SERVICE_NAME", "OTEL_SERVICE_NAME"],
        )
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "sutra-engine".to_string());

        let metrics_temporality = match value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.metrics.temporality-preference",
            &[
                "SUTRA_TELEMETRY_METRICS_TEMPORALITY_PREFERENCE",
                "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
            ],
        ) {
            None => TemporalityPreference::default(),
            Some(raw) => TemporalityPreference::parse(&raw).unwrap_or_else(|| {
                warnings.push(format!(
                    "telemetry temporality preference '{raw}' is not \
                     cumulative|delta|lowmemory — using cumulative"
                ));
                TemporalityPreference::default()
            }),
        };

        let metrics_export_interval_ms = match value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.metrics.export-interval",
            &[
                "SUTRA_TELEMETRY_METRICS_EXPORT_INTERVAL",
                "OTEL_METRIC_EXPORT_INTERVAL",
            ],
        ) {
            None => None,
            Some(raw) => match raw.trim().parse::<u64>() {
                Ok(ms) if ms > 0 => Some(ms),
                _ => {
                    warnings.push(format!(
                        "telemetry metric export interval '{raw}' is not a positive \
                         millisecond count — using the SDK default"
                    ));
                    None
                }
            },
        };

        let metric_labels = match value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.metric-labels",
            &["SUTRA_TELEMETRY_METRIC_LABELS"],
        ) {
            None => default_metric_labels(),
            Some(raw) => raw
                .split(',')
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect(),
        };

        let mut enabled = match value(
            file,
            env,
            &mut warnings,
            "sutra.telemetry.enabled",
            &["SUTRA_TELEMETRY_ENABLED"],
        ) {
            None => true,
            Some(raw) => !raw.trim().eq_ignore_ascii_case("false"),
        };
        // The standard OTel kill switch also wins (the spec env is `OTEL_SDK_DISABLED`).
        if env("OTEL_SDK_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true")) {
            enabled = false;
        }

        TelemetryConfig {
            otlp_endpoint,
            service_name,
            metrics_temporality,
            metrics_export_interval_ms,
            metric_labels,
            enabled,
            warnings,
        }
    }

    /// Telemetry exports iff enabled AND an endpoint is configured.
    pub fn is_active(&self) -> bool {
        self.enabled && self.otlp_endpoint.is_some()
    }

    /// The metrics-listener wiring for the assembly: `Some(label allowlist)` when
    /// the listener should be registered, `None` for zero-overhead no-telemetry runs.
    pub fn metrics_wiring(&self) -> Option<Vec<String>> {
        self.is_active().then(|| self.metric_labels.clone())
    }
}

/// Properties reader that NEVER fails (telemetry is fail-open; the engine's own config
/// still fail-closes through `config::EngineConfig`).
fn read_properties_lenient(path: &std::path::Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return out;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    out
}

// =====================================================================================
// Exporter bootstrap
// =====================================================================================

/// Handles to the live providers — kept for flush/shutdown; dropping WITHOUT calling
/// [`Telemetry::shutdown`] leaves the batch workers running (fine for tests). Cheap to
/// clone (the SDK providers are shared handles).
#[derive(Clone)]
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

/// The first ACTIVE [`init`]'s provider handles — what [`flush_active`] flushes. Process
/// lifetime, mirroring the process-global subscriber the same `init` installs.
static ACTIVE_TELEMETRY: std::sync::OnceLock<Telemetry> = std::sync::OnceLock::new();

/// Force-flush the process-global telemetry stack, if one is active — the
/// [`crate::server::RunningEngine::shutdown`] hook (drain posture: exported signals
/// leave before the process does). Fail-open no-op when telemetry never initialised.
pub fn flush_active() {
    if let Some(telemetry) = ACTIVE_TELEMETRY.get() {
        telemetry.flush();
    }
}

impl Telemetry {
    /// True when at least one OTLP exporter is live.
    pub fn is_active(&self) -> bool {
        self.tracer_provider.is_some()
            || self.meter_provider.is_some()
            || self.logger_provider.is_some()
    }

    /// Force-flush all live providers (fail-open — failures are logged, never raised).
    pub fn flush(&self) {
        if let Some(p) = &self.tracer_provider {
            if let Err(e) = p.force_flush() {
                warn!(error = %e, "OTLP trace flush failed");
            }
        }
        if let Some(p) = &self.meter_provider {
            if let Err(e) = p.force_flush() {
                warn!(error = %e, "OTLP metric flush failed");
            }
        }
        if let Some(p) = &self.logger_provider {
            if let Err(e) = p.force_flush() {
                warn!(error = %e, "OTLP log flush failed");
            }
        }
    }

    /// Flush + shut the exporters down (process exit path).
    pub fn shutdown(&self) {
        if let Some(p) = &self.tracer_provider {
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "OTLP trace exporter shutdown failed");
            }
        }
        if let Some(p) = &self.meter_provider {
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "OTLP metric exporter shutdown failed");
            }
        }
        if let Some(p) = &self.logger_provider {
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "OTLP log exporter shutdown failed");
            }
        }
    }
}

/// Install the process-wide telemetry stack: the structured-JSON stdout layer (always),
/// plus — when an OTLP endpoint is configured — the OTel trace layer over the existing
/// `sutra.*` spans, the OTLP log bridge, and the global meter provider feeding the
/// metrics listener. Fail-open at every step: exporter build failures degrade to
/// JSON-logs-only and are reported as WARNs once the subscriber is up.
pub fn init(config: &TelemetryConfig) -> Telemetry {
    let mut deferred_warnings: Vec<String> = config.warnings.clone();
    let active = config.is_active();

    // ---- providers (built first — the subscriber needs the tracer) ----------------
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .build();

    let mut tracer_provider = None;
    let mut meter_provider = None;
    let mut logger_provider = None;
    if active {
        let endpoint = config.otlp_endpoint.clone().unwrap_or_default();

        match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .build()
        {
            Ok(exporter) => {
                tracer_provider = Some(
                    SdkTracerProvider::builder()
                        .with_batch_exporter(exporter)
                        .with_resource(resource.clone())
                        .build(),
                );
            }
            Err(e) => {
                deferred_warnings.push(format!("OTLP span exporter unavailable (traces off): {e}"))
            }
        }

        match opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .with_temporality(config.metrics_temporality.as_sdk())
            .build()
        {
            Ok(exporter) => {
                let mut reader = PeriodicReader::builder(exporter);
                if let Some(ms) = config.metrics_export_interval_ms {
                    reader = reader.with_interval(std::time::Duration::from_millis(ms));
                }
                meter_provider = Some(
                    SdkMeterProvider::builder()
                        .with_reader(reader.build())
                        .with_resource(resource.clone())
                        .build(),
                );
            }
            Err(e) => deferred_warnings.push(format!(
                "OTLP metric exporter unavailable (metrics off): {e}"
            )),
        }

        match opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
        {
            Ok(exporter) => {
                logger_provider = Some(
                    SdkLoggerProvider::builder()
                        .with_batch_exporter(exporter)
                        .with_resource(resource)
                        .build(),
                );
            }
            Err(e) => deferred_warnings.push(format!(
                "OTLP log exporter unavailable (OTLP logs off): {e}"
            )),
        }
    }

    // ---- subscriber stack ----------------------------------------------------------
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let json_layer = StructuredJsonLayer {
        service_name: config.service_name.clone(),
        otel_active: tracer_provider.is_some(),
        sequence: AtomicU64::new(0),
        dispatch: std::sync::OnceLock::new(),
        capture: None,
    };

    let otel_trace_layer = tracer_provider.as_ref().map(|provider| {
        tracing_opentelemetry::layer().with_tracer(provider.tracer("sutra-engine"))
    });

    // The OTLP log bridge must not see the exporters' own internal events (the
    // `internal-logs` feature emits them via `tracing`) — that would loop a failing
    // exporter into itself. Everything still reaches stdout via the JSON layer.
    let otel_log_layer = logger_provider.as_ref().map(|provider| {
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(provider)
            .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                let target = meta.target();
                !target.starts_with("opentelemetry")
                    && !target.starts_with("tonic")
                    && !target.starts_with("h2")
                    && !target.starts_with("hyper")
                    && !target.starts_with("tower")
            }))
    });

    if let Err(e) = tracing_subscriber::registry()
        .with(env_filter)
        .with(json_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init()
    {
        deferred_warnings.push(format!(
            "global tracing subscriber already installed — telemetry layers not \
             attached to it ({e})"
        ));
    }

    if let Some(provider) = &meter_provider {
        global::set_meter_provider(provider.clone());
    }

    // The subscriber is up — surface everything that went sideways (fail-open).
    for warning in &deferred_warnings {
        warn!(warning = %warning, "telemetry init warning");
    }
    if active {
        info!(
            endpoint = config.otlp_endpoint.as_deref().unwrap_or_default(),
            service_name = %config.service_name,
            traces = tracer_provider.is_some(),
            metrics = meter_provider.is_some(),
            logs = logger_provider.is_some(),
            temporality = ?config.metrics_temporality,
            "OTLP telemetry export active"
        );
    } else {
        info!("telemetry export off (no OTLP endpoint configured) — JSON stdout logs only");
    }

    let telemetry = Telemetry {
        tracer_provider,
        meter_provider,
        logger_provider,
    };
    if telemetry.is_active() {
        // First active init wins (init is once-per-process in the binary; repeated test
        // inits are inactive/no-op) — the [`flush_active`] shutdown hook flushes it.
        let _ = ACTIVE_TELEMETRY.set(telemetry.clone());
    }
    telemetry
}

// =====================================================================================
// W3C traceparent bridge (outbox delivery joins the enqueueing trace)
// =====================================================================================
//
// The helpers live in `sutra_executor::telemetry`: the delivery-side call site
// is `sutra-channels`' outbox send span and the dependency direction is channels →
// executor. Re-exported here so engine-side callers keep their import path.

pub use sutra_executor::telemetry::{
    current_traceparent, format_traceparent, link_current_span_to_traceparent,
    link_span_to_traceparent, parse_traceparent,
};

// =====================================================================================
// Metrics — the shard router's per-lane observable instruments (scale-out §6.1)
// =====================================================================================

/// Register the `sutra.engine.shard.*` observable instruments over the router's per-lane
/// counter registry: queue-depth gauge, dispatch/park/resume counters, the handoff
/// counter, and the claim-bounce counter split `relay`/`timer` — each dimensioned by
/// `shard` (the lane index). Called ONCE per boot from `serve`, only when telemetry
/// metrics are wired; the registry is router-owned and survives activation flips, so the
/// callbacks always read the live lanes. Fail-open like every meter here: without an
/// installed provider the instruments are no-ops. The instrument handles are dropped
/// deliberately — a registered observable callback is owned by the SDK pipeline.
pub fn register_shard_router_meters(metrics: std::sync::Arc<sutra_channels::ShardRouterMetrics>) {
    use std::sync::Arc;
    let meter: Meter = global::meter("sutra-engine");
    let shard_attr = |index: usize| [KeyValue::new(names::ATTR_SHARD, index as i64)];

    let m = Arc::clone(&metrics);
    let _ = meter
        .i64_observable_gauge(names::METRIC_ENGINE_SHARD_QUEUE_DEPTH)
        .with_description("Per-shard engine mailbox depth (enqueued, not yet dequeued).")
        .with_callback(move |observer| {
            for (index, lane) in m.lanes().iter().enumerate() {
                observer.observe(lane.queue_depth.load(Ordering::Relaxed), &shard_attr(index));
            }
        })
        .build();

    // The monotonic per-lane counters, each reported as its cumulative total.
    struct LaneCounter {
        name: &'static str,
        description: &'static str,
        read: fn(&sutra_channels::ShardLaneMetrics) -> u64,
    }
    let counters = [
        LaneCounter {
            name: names::METRIC_ENGINE_SHARD_DISPATCHES,
            description: "Per-shard work requests drained by the lane's actor.",
            read: |lane| lane.dispatches.load(std::sync::atomic::Ordering::Relaxed),
        },
        LaneCounter {
            name: names::METRIC_ENGINE_SHARD_PARKS,
            description: "Per-shard initial park commits.",
            read: |lane| lane.parks.load(std::sync::atomic::Ordering::Relaxed),
        },
        LaneCounter {
            name: names::METRIC_ENGINE_SHARD_RESUMES,
            description: "Per-shard committed resume passes (relay/timer/handoff).",
            read: |lane| lane.resumes.load(std::sync::atomic::Ordering::Relaxed),
        },
        LaneCounter {
            name: names::METRIC_ENGINE_SHARD_HANDOFFS,
            description: "Per-shard relay handoffs (resolved here, owned by another lane).",
            read: |lane| lane.handoffs.load(std::sync::atomic::Ordering::Relaxed),
        },
    ];
    for counter in counters {
        let m = Arc::clone(&metrics);
        let read = counter.read;
        let _ = meter
            .u64_observable_counter(counter.name)
            .with_description(counter.description)
            .with_callback(move |observer| {
                for (index, lane) in m.lanes().iter().enumerate() {
                    observer.observe(read(lane), &shard_attr(index));
                }
            })
            .build();
    }

    // Claim bounces: ONE meter, split by the `path` dimension — the §4 mis-route alarm.
    let m = Arc::clone(&metrics);
    let _ = meter
        .u64_observable_counter(names::METRIC_ENGINE_SHARD_CLAIM_BOUNCES)
        .with_description(
            "Per-shard CLAIM_HELD bounces, split by path (relay/timer) — near zero at a \
             correct N>1 rollout outside genuine cross-replica contention.",
        )
        .with_callback(move |observer| {
            for (index, lane) in m.lanes().iter().enumerate() {
                observer.observe(
                    lane.claim_bounce_relay.load(Ordering::Relaxed),
                    &[
                        KeyValue::new(names::ATTR_SHARD, index as i64),
                        KeyValue::new(names::ATTR_CLAIM_BOUNCE_PATH, "relay"),
                    ],
                );
                observer.observe(
                    lane.claim_bounce_timer.load(Ordering::Relaxed),
                    &[
                        KeyValue::new(names::ATTR_SHARD, index as i64),
                        KeyValue::new(names::ATTR_CLAIM_BOUNCE_PATH, "timer"),
                    ],
                );
            }
        })
        .build();
}

// =====================================================================================
// Metrics — ExecutionListener → OTel instruments
// =====================================================================================

/// Maps the executor lifecycle bus onto the frozen `sutra.*` meters. Stateless beyond the
/// instrument handles; registered on the executor builder only when telemetry is active
/// (zero overhead otherwise). Dimensions: `deployment.id` on every
/// meter; instance/coverage meters add the label allowlist; token meters add
/// `node.type`/`node.id`; task meters add `task.name`.
pub struct OtelMetricsListener {
    labels: Vec<String>,
    instance_started: Counter<u64>,
    instance_completed: Counter<u64>,
    instance_suspended: Counter<u64>,
    instance_resumed: Counter<u64>,
    token_entered: Counter<u64>,
    token_left: Counter<u64>,
    task_invoked: Counter<u64>,
    task_completed: Counter<u64>,
    task_failed: Counter<u64>,
    task_duration: Histogram<f64>,
    path_covered: Counter<u64>,
    /// The synchronous `sutra.coverage.percent` gauge (`None` until wired via
    /// [`OtelMetricsListener::with_coverage`], which only happens when a coverage store is
    /// present in the assembly). The listener holds NO store reference: the activation
    /// covered-set snapshot is read (async) by the runtime assembly and applied via
    /// [`Self::apply_initial_coverage`]; the event path only grows the in-memory set.
    coverage_gauge: Option<Gauge<f64>>,
    /// Per-`(deployment_id, process_id)` coverage state, keyed so processes sharing
    /// an id across deployments stay distinct. `RefCell`-guarded interior mutability: the
    /// listener runs single-threaded on the engine actor, and `on_path_covered` fires from
    /// within a `&self` callback.
    coverage: HashMap<(String, String), CoverageState>,
}

/// One process's coverage-gauge state. `total = declared_paths.len()`; the covered
/// set is seeded from the store at activation and grown on each `on_path_covered`. `attrs`
/// is fixed at wiring so the initial record and every event record land on ONE time series
/// (the gauge is last-value).
struct CoverageState {
    declared_paths: Vec<String>,
    covered: RefCell<BTreeSet<String>>,
    attrs: Vec<KeyValue>,
}

/// The per-process coverage metadata the assembly hands the listener (built in
/// `build_engine` from the active plans, where the process registry + namespaces are in
/// scope). Plain data — no OTel types, so `sutra-engine::assembly` need not name them.
pub struct CoverageMeta {
    pub deployment_id: String,
    pub process_id: String,
    /// The declared `<q:coverage>` path ids (each maps onto a metric flag via
    /// `sutra_executor::metric_flag_urn`).
    pub declared_paths: Vec<String>,
    /// Authoring namespace of the process's deployment; each attached as a gauge dimension
    /// only when non-empty (a tenant is never fabricated).
    pub tenant: Option<String>,
    pub module: Option<String>,
    pub version: Option<String>,
}

/// The activation-initial covered-set snapshot: `(deployment_id, process_id)` → the
/// declared `<q:coverage>` path ids whose metric flag is already covered. READ once per
/// activation by the runtime assembly (async — `seed_declared_coverage`), then applied
/// per lane via [`OtelMetricsListener::apply_initial_coverage`]. Plain `Send` data so the
/// activation flip can carry one snapshot into every lane's rebuild closure.
pub type InitialCoverage = HashMap<(String, String), BTreeSet<String>>;

/// Map one process's declared `<q:coverage>` path ids and the store's flagged urns back to
/// the covered path-id set. Declared id → metric-flag urn (an injected `…#<process>`
/// sub-path collapses to its ROUTE flag), so the membership test runs on the flag the
/// store actually keeps while the answer stays keyed by the declared id.
pub fn covered_paths_of(declared_paths: &[String], flagged: &BTreeSet<String>) -> BTreeSet<String> {
    declared_paths
        .iter()
        .filter(|path| flagged.contains(&metric_flag_urn(path)))
        .cloned()
        .collect()
}

/// The pure coverage-percent computation (`covered/total × 100`); `0.0` when no
/// paths are declared. Extracted for a dependency-free unit test.
fn coverage_percent(covered: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        covered as f64 / total as f64 * 100.0
    }
}

/// The fixed gauge dimensions for one process: `deployment.id` + `process.id`
/// always, plus `tenant`/`module`/`version` when non-empty. Held constant per process so the
/// activation record and every event record hit the same last-value time series.
fn coverage_attrs(
    deployment_id: &str,
    process_id: &str,
    tenant: Option<String>,
    module: Option<String>,
    version: Option<String>,
) -> Vec<KeyValue> {
    let mut attrs = vec![
        KeyValue::new(names::ATTR_DEPLOYMENT_ID, deployment_id.to_string()),
        KeyValue::new(names::ATTR_PROCESS_ID, process_id.to_string()),
    ];
    if let Some(t) = tenant.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(names::ATTR_TENANT, t));
    }
    if let Some(m) = module.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(names::ATTR_MODULE, m));
    }
    if let Some(v) = version.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(names::ATTR_VERSION, v));
    }
    attrs
}

impl OtelMetricsListener {
    /// Instruments resolve through the GLOBAL meter provider — [`init`] must have run
    /// (otherwise they are no-ops, which is exactly the fail-open posture).
    pub fn new(labels: Vec<String>) -> OtelMetricsListener {
        let meter: Meter = global::meter("sutra-engine");
        OtelMetricsListener {
            labels,
            instance_started: meter.u64_counter(names::METRIC_INSTANCE_STARTED).build(),
            instance_completed: meter.u64_counter(names::METRIC_INSTANCE_COMPLETED).build(),
            instance_suspended: meter.u64_counter(names::METRIC_INSTANCE_SUSPENDED).build(),
            instance_resumed: meter.u64_counter(names::METRIC_INSTANCE_RESUMED).build(),
            token_entered: meter.u64_counter(names::METRIC_TOKEN_ENTERED).build(),
            token_left: meter.u64_counter(names::METRIC_TOKEN_LEFT).build(),
            task_invoked: meter.u64_counter(names::METRIC_TASK_INVOKED).build(),
            task_completed: meter.u64_counter(names::METRIC_TASK_COMPLETED).build(),
            task_failed: meter.u64_counter(names::METRIC_TASK_FAILED).build(),
            task_duration: meter
                .f64_histogram(names::METRIC_TASK_DURATION)
                .with_unit("s")
                .build(),
            path_covered: meter
                .u64_counter(names::METRIC_COVERAGE_PATH_COVERED)
                .build(),
            coverage_gauge: None,
            coverage: HashMap::new(),
        }
    }

    /// Wire the event-driven `sutra.coverage.percent` gauge. The instrument is a
    /// SYNCHRONOUS gauge (`meter.f64_gauge`): the listener `.record()`s it on the actor
    /// thread on each new mark and once at activation — there is no observable callback, so
    /// nothing ever reads the `Rc`-actor / coverage store off the export thread. Called from
    /// `build_engine` (per activation), where the coverage store + process metadata are in
    /// scope; the gauge is idempotent by name, so re-wiring on every flip feeds one series.
    pub fn with_coverage(mut self, meter: &Meter, metas: Vec<CoverageMeta>) -> OtelMetricsListener {
        let gauge = meter
            .f64_gauge(names::METRIC_COVERAGE_PERCENT)
            .with_description(
                "Declared <q:coverage> path coverage per process, percent (covered/total × 100).",
            )
            .with_unit("percent")
            .build();
        let mut coverage = HashMap::new();
        for meta in metas {
            let key = (meta.deployment_id.clone(), meta.process_id.clone());
            let attrs = coverage_attrs(
                &meta.deployment_id,
                &meta.process_id,
                meta.tenant,
                meta.module,
                meta.version,
            );
            coverage.insert(
                key,
                CoverageState {
                    declared_paths: meta.declared_paths,
                    covered: RefCell::new(BTreeSet::new()),
                    attrs,
                },
            );
        }
        self.coverage_gauge = Some(gauge);
        self.coverage = coverage;
        self
    }

    /// The activation-initial record: seed each process's covered-set from the PRE-READ
    /// [`InitialCoverage`] snapshot and record the gauge once, so a replica that boots onto
    /// pre-existing coverage reports the right percent WITHOUT waiting for a fresh mark.
    ///
    /// The store READ is hoisted to the runtime assembly (`seed_declared_coverage` — async,
    /// once per activation, execution scale-out §2 row 11 extended by Phase 3): this method
    /// is therefore synchronous and safe inside `build_engine` wherever it runs — including
    /// an activation flip's rebuild ON a lane's async actor task, where the old
    /// `Handle::block_on` read would panic. A key absent from the snapshot means "nothing
    /// covered yet" (the same best-effort posture a failed store read always had). A no-op
    /// when coverage is unwired.
    pub fn apply_initial_coverage(&self, initial: &InitialCoverage) {
        if self.coverage_gauge.is_none() {
            return;
        }
        for (key, state) in &self.coverage {
            if let Some(covered_paths) = initial.get(key) {
                let mut covered = state.covered.borrow_mut();
                for path in &state.declared_paths {
                    if covered_paths.contains(path) {
                        covered.insert(path.clone());
                    }
                }
            }
            self.record_coverage_for(key);
        }
    }

    /// Record the gauge for one process from its current in-memory covered-set.
    fn record_coverage_for(&self, key: &(String, String)) {
        let Some(gauge) = &self.coverage_gauge else {
            return;
        };
        let Some(state) = self.coverage.get(key) else {
            return;
        };
        let total = state.declared_paths.len();
        if total == 0 {
            return;
        }
        let covered = state.covered.borrow().len();
        gauge.record(coverage_percent(covered, total), &state.attrs);
    }

    /// `deployment.id` + the allowlisted manifest labels.
    fn instance_attrs(&self, event: &InstanceEvent) -> Vec<KeyValue> {
        let mut attrs = Vec::with_capacity(1 + self.labels.len());
        attrs.push(KeyValue::new(
            names::ATTR_DEPLOYMENT_ID,
            event.deployment.value().to_string(),
        ));
        for label in &self.labels {
            if let Some(value) = event.labels.get(label) {
                attrs.push(KeyValue::new(label.clone(), value.clone()));
            }
        }
        attrs
    }

    fn token_attrs(&self, event: &TokenEvent) -> Vec<KeyValue> {
        vec![
            KeyValue::new(
                names::ATTR_DEPLOYMENT_ID,
                event.deployment.value().to_string(),
            ),
            KeyValue::new(names::ATTR_NODE_TYPE, event.node_type.clone()),
            KeyValue::new(names::ATTR_NODE_ID, event.node_id.clone()),
        ]
    }

    fn task_attrs(&self, event: &TaskEvent) -> Vec<KeyValue> {
        vec![
            KeyValue::new(
                names::ATTR_DEPLOYMENT_ID,
                event.deployment.value().to_string(),
            ),
            KeyValue::new(names::ATTR_TASK_NAME, event.task_name.clone()),
        ]
    }
}

impl ExecutionListener for OtelMetricsListener {
    fn on_instance_started(&self, event: &InstanceEvent) {
        self.instance_started.add(1, &self.instance_attrs(event));
    }

    fn on_instance_completed(&self, event: &InstanceEvent) {
        self.instance_completed.add(1, &self.instance_attrs(event));
    }

    fn on_instance_suspended(&self, event: &InstanceEvent) {
        self.instance_suspended.add(1, &self.instance_attrs(event));
    }

    fn on_instance_resumed(&self, event: &InstanceEvent) {
        self.instance_resumed.add(1, &self.instance_attrs(event));
    }

    fn on_token_entered(&self, event: &TokenEvent) {
        self.token_entered.add(1, &self.token_attrs(event));
    }

    fn on_token_left(&self, event: &TokenEvent) {
        self.token_left.add(1, &self.token_attrs(event));
    }

    fn on_task_invoked(&self, event: &TaskEvent) {
        self.task_invoked.add(1, &self.task_attrs(event));
    }

    fn on_task_completed(&self, event: &TaskEvent) {
        let attrs = self.task_attrs(event);
        self.task_completed.add(1, &attrs);
        self.task_duration
            .record(event.duration_nanos as f64 / 1e9, &attrs);
    }

    fn on_task_failed(&self, event: &TaskEvent, _diagnostic: &sutra_bpmn::SutraError) {
        let attrs = self.task_attrs(event);
        self.task_failed.add(1, &attrs);
        self.task_duration
            .record(event.duration_nanos as f64 / 1e9, &attrs);
    }

    fn on_path_covered(&self, event: &InstanceEvent, path_id: &str) {
        let mut attrs = self.instance_attrs(event);
        attrs.push(KeyValue::new(names::ATTR_PATH, path_id.to_string()));
        self.path_covered.add(1, &attrs);
        // Event-driven `sutra.coverage.percent`. This fires from inside the
        // executor's `block_on` (`mark_coverage().await`), so it MUST NOT read the async
        // store here: instead grow the in-memory covered-set (seeded at activation) and
        // re-record the last-value gauge. `mark_coverage` only notifies on a NEW mark, and
        // the set makes a re-notification idempotent regardless.
        if self.coverage_gauge.is_some() {
            let key = (
                event.deployment.value().to_string(),
                event.process_id.clone(),
            );
            if let Some(state) = self.coverage.get(&key) {
                state.covered.borrow_mut().insert(path_id.to_string());
                self.record_coverage_for(&key);
            }
        }
    }
}

// =====================================================================================
// Structured JSON stdout logs (EFK `sutra_json` parser compatibility)
// =====================================================================================

/// One JSON object per line on stdout in the frozen field shape the EFK pipeline expects
/// the EFK pipeline decodes: `timestamp` (`%Y-%m-%dT%H:%M:%S.%L%z`-parsable), `level`,
/// `loggerName`, `message`, `threadName`, `sequence`, the `service.name`
/// additional-field, and — inside a live OTel span — `traceId`/`spanId`/`sampled`
/// (the frozen trace-correlation key names). Event fields are flattened alongside (reserved keys
/// win on collision).
struct StructuredJsonLayer {
    service_name: String,
    otel_active: bool,
    sequence: AtomicU64,
    /// The installed dispatcher (captured in `on_register_dispatch`) — required to read
    /// the OTel span context from inside `on_event`, where `dispatcher::get_default`
    /// would hit tracing-core's re-entrancy guard and yield the none-dispatcher.
    dispatch: std::sync::OnceLock<tracing::dispatcher::WeakDispatch>,
    /// Test-only sink: captured lines instead of stdout (None in production).
    capture: Option<std::sync::Arc<std::sync::Mutex<Vec<String>>>>,
}

impl<S> tracing_subscriber::Layer<S> for StructuredJsonLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        let _ = self.dispatch.set(subscriber.downgrade());
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);

        let mut object = visitor.fields;
        object.insert("timestamp".into(), serde_json::Value::from(timestamp_now()));
        object.insert(
            "sequence".into(),
            serde_json::Value::from(self.sequence.fetch_add(1, Ordering::Relaxed)),
        );
        object.insert(
            "level".into(),
            serde_json::Value::from(event.metadata().level().as_str()),
        );
        object.insert(
            "loggerName".into(),
            serde_json::Value::from(event.metadata().target()),
        );
        object.insert(
            "message".into(),
            serde_json::Value::from(visitor.message.unwrap_or_default()),
        );
        if let Some(name) = std::thread::current().name() {
            object.insert("threadName".into(), serde_json::Value::from(name));
        }
        object.insert(
            "service.name".into(),
            serde_json::Value::from(self.service_name.as_str()),
        );

        // Trace correlation — the frozen JSON-log key names (`traceId`/`spanId`/
        // `sampled`), stamped only when the event fires inside a live exported span.
        // `get_otel_context` needs the REAL dispatcher captured at install time —
        // inside `on_event`, `dispatcher::get_default` yields the none-dispatcher
        // (tracing-core re-entrancy guard), so `Span::current()` would come up empty.
        if self.otel_active {
            let otel_context = ctx.lookup_current().and_then(|span_ref| {
                let dispatch = self.dispatch.get()?.upgrade()?;
                tracing_opentelemetry::get_otel_context(&span_ref.id(), &dispatch)
            });
            if let Some(otel_context) = otel_context {
                let span = otel_context.span();
                let span_context = span.span_context();
                if span_context.is_valid() {
                    object.insert(
                        "traceId".into(),
                        serde_json::Value::from(span_context.trace_id().to_string()),
                    );
                    object.insert(
                        "spanId".into(),
                        serde_json::Value::from(span_context.span_id().to_string()),
                    );
                    object.insert(
                        "sampled".into(),
                        serde_json::Value::from(span_context.is_sampled().to_string()),
                    );
                }
            }
        }

        let line = serde_json::Value::Object(object).to_string();
        if let Some(capture) = &self.capture {
            capture.lock().unwrap().push(line);
            return;
        }
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        let _ = writeln!(lock, "{line}");
    }
}

/// `2026-07-12T09:00:00.123+00:00` — ISO offset date-time with millisecond precision,
/// parsable by the Fluent Bit `sutra_json` parser's `%Y-%m-%dT%H:%M:%S.%L%z`.
fn timestamp_now() -> String {
    let format = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]+00:00"
    );
    time::OffsetDateTime::now_utc()
        .format(&format)
        .unwrap_or_else(|_| String::new())
}

/// Flattens event fields into JSON values; `message` is captured separately.
#[derive(Default)]
struct JsonFieldVisitor {
    message: Option<String>,
    fields: serde_json::Map<String, serde_json::Value>,
}

impl JsonFieldVisitor {
    fn put(&mut self, field: &tracing::field::Field, value: serde_json::Value) {
        if field.name() == "message" {
            if let serde_json::Value::String(s) = value {
                self.message = Some(s);
            } else {
                self.message = Some(value.to_string());
            }
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl tracing::field::Visit for JsonFieldVisitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        let json = serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::from(value.to_string()));
        self.put(field, json);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.put(field, serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.put(field, serde_json::Value::from(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.put(field, serde_json::Value::from(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.put(field, serde_json::Value::from(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.put(field, serde_json::Value::from(format!("{value:?}")));
    }
}

// =====================================================================================
// Tests
// =====================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn defaults_are_off_and_quiet() {
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &no_env);
        assert!(!c.is_active(), "no endpoint ⇒ telemetry off");
        assert!(c.enabled);
        assert_eq!(c.service_name, "sutra-engine");
        assert_eq!(c.metrics_temporality, TemporalityPreference::Cumulative);
        assert_eq!(c.metric_labels, vec!["tenant", "module", "version"]);
        assert_eq!(c.metrics_wiring(), None, "no listener wiring when off");
        assert!(c.warnings.is_empty());
    }

    #[test]
    fn endpoint_precedence_canonical_then_otel_then_file() {
        let mut file = BTreeMap::new();
        file.insert(
            "sutra.telemetry.otlp.endpoint".to_string(),
            "http://file:4317".to_string(),
        );

        let c = TelemetryConfig::from_sources(&file, &no_env);
        assert_eq!(c.otlp_endpoint.as_deref(), Some("http://file:4317"));
        assert!(c.is_active());

        // The standard vendor-neutral env beats the file.
        let c = TelemetryConfig::from_sources(&file, &|name| {
            (name == "OTEL_EXPORTER_OTLP_ENDPOINT")
                .then(|| "http://otel-collector:4317".to_string())
        });
        assert_eq!(
            c.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );

        // The canonical env beats everything.
        let c = TelemetryConfig::from_sources(&file, &|name| match name {
            "SUTRA_TELEMETRY_OTLP_ENDPOINT" => Some("http://canonical:4317".into()),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://standard:4317".into()),
            _ => None,
        });
        assert_eq!(c.otlp_endpoint.as_deref(), Some("http://canonical:4317"));

        // Only the canonical and the standard OTel names are consulted — any other env
        // name is ignored, leaving the endpoint unset.
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "APP_OTLP_ENDPOINT").then(|| "http://ignored:4317".to_string())
        });
        assert_eq!(c.otlp_endpoint, None);
    }

    #[test]
    fn service_name_resolves_from_the_standard_env() {
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "OTEL_SERVICE_NAME").then(|| "demo-svc".to_string())
        });
        assert_eq!(c.service_name, "demo-svc");

        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| match name {
            "SUTRA_TELEMETRY_SERVICE_NAME" => Some("canonical".into()),
            "OTEL_SERVICE_NAME" => Some("standard".into()),
            _ => None,
        });
        assert_eq!(c.service_name, "canonical");

        // Only the canonical and the standard OTel names are consulted — any other env
        // name is ignored, so the service name stays at its default.
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "APP_APPLICATION_NAME").then(|| "demo-svc".to_string())
        });
        assert_eq!(c.service_name, "sutra-engine");
    }

    #[test]
    fn temporality_env_is_honored_and_invalid_values_fall_back() {
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE")
                .then(|| "delta".to_string())
        });
        assert_eq!(c.metrics_temporality, TemporalityPreference::Delta);

        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE")
                .then(|| "DELTA".to_string())
        });
        assert_eq!(
            c.metrics_temporality,
            TemporalityPreference::Delta,
            "case-insensitive"
        );

        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE")
                .then(|| "sideways".to_string())
        });
        assert_eq!(
            c.metrics_temporality,
            TemporalityPreference::Cumulative,
            "fail-open on junk"
        );
        assert_eq!(c.warnings.len(), 1);
    }

    #[test]
    fn kill_switches_disable_even_with_an_endpoint() {
        let mut file = BTreeMap::new();
        file.insert(
            "sutra.telemetry.otlp.endpoint".to_string(),
            "http://collector:4317".to_string(),
        );

        let c = TelemetryConfig::from_sources(&file, &|name| {
            (name == "OTEL_SDK_DISABLED").then(|| "true".to_string())
        });
        assert!(!c.is_active(), "OTEL_SDK_DISABLED=true wins");
        assert_eq!(c.metrics_wiring(), None);

        let c = TelemetryConfig::from_sources(&file, &|name| {
            (name == "SUTRA_TELEMETRY_ENABLED").then(|| "false".to_string())
        });
        assert!(!c.is_active(), "sutra.telemetry.enabled=false wins");
    }

    #[test]
    fn metric_labels_parse_and_export_interval_validates() {
        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| match name {
            "SUTRA_TELEMETRY_METRIC_LABELS" => Some("tenant , custom,".into()),
            "OTEL_METRIC_EXPORT_INTERVAL" => Some("250".into()),
            _ => None,
        });
        assert_eq!(c.metric_labels, vec!["tenant", "custom"]);
        assert_eq!(c.metrics_export_interval_ms, Some(250));

        let c = TelemetryConfig::from_sources(&BTreeMap::new(), &|name| {
            (name == "OTEL_METRIC_EXPORT_INTERVAL").then(|| "soon".to_string())
        });
        assert_eq!(c.metrics_export_interval_ms, None);
        assert_eq!(c.warnings.len(), 1);
    }

    // ---- traceparent bridge --------------------------------------------------------

    const SAMPLE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn traceparent_round_trips() {
        let cx = parse_traceparent(SAMPLE).expect("valid traceparent parses");
        assert_eq!(
            cx.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(cx.span_id().to_string(), "b7ad6b7169203331");
        assert!(cx.is_sampled());
        assert!(cx.is_remote(), "restored contexts are REMOTE parents/links");
        assert_eq!(format_traceparent(&cx).as_deref(), Some(SAMPLE));
    }

    #[test]
    fn traceparent_unsampled_and_whitespace_tolerated() {
        let cx = parse_traceparent(" 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00 ")
            .expect("unsampled parses");
        assert!(!cx.is_sampled());
    }

    #[test]
    fn traceparent_rejects_malformed_values() {
        for bad in [
            "",
            "not-a-traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331", // missing flags
            "00-00000000000000000000000000000000-b7ad6b7169203331-01", // zero trace id
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", // zero span id
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // forbidden version
            "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01", // uppercase hex
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra", // v00 + extra
            "0-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // short version
        ] {
            assert!(
                parse_traceparent(bad).is_none(),
                "must reject malformed traceparent: {bad}"
            );
        }
    }

    #[test]
    fn future_version_with_core_shape_still_parses() {
        // Per W3C: parsers accept versions > 00 when the core fields match version 00.
        let cx = parse_traceparent("01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assert!(cx.is_some());
    }

    #[test]
    fn link_helper_is_fail_open_without_a_subscriber() {
        // No tracing subscriber, no OTel layer — the helper must not panic and must
        // report false only for malformed values (a well-formed value parses; the
        // add_link on a disabled span is a no-op).
        assert!(!link_current_span_to_traceparent("garbage"));
        assert!(link_current_span_to_traceparent(SAMPLE));
        assert_eq!(
            current_traceparent(),
            None,
            "no active span ⇒ nothing to capture"
        );
    }

    // ---- JSON stdout log shape (the EFK `sutra_json` field contract) ----------------

    #[test]
    fn json_log_lines_carry_the_frozen_fields_and_trace_ids() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let json_layer = StructuredJsonLayer {
            service_name: "wsf-shape-test".to_string(),
            otel_active: true,
            sequence: AtomicU64::new(0),
            dispatch: std::sync::OnceLock::new(),
            capture: Some(std::sync::Arc::clone(&captured)),
        };
        // In-memory span exporter keeps trace ids real without any network.
        let tracer_provider = SdkTracerProvider::builder().build();
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("test"));
        let subscriber = tracing_subscriber::registry()
            .with(json_layer)
            .with(otel_layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("sutra.dispatch", channel = "wsf-in");
            let _entered = span.enter();
            tracing::info!(deployment_id = "dep-x", answer = 42, "engine event");
        });

        let lines = captured.lock().unwrap();
        assert_eq!(lines.len(), 1, "one JSON line per event");
        let doc: serde_json::Value = serde_json::from_str(&lines[0]).expect("valid JSON");

        // The fields the Fluent Bit parser + ES mapping rely on:
        // timestamp / level / loggerName / message / service.name.
        let ts = doc["timestamp"].as_str().expect("timestamp present");
        assert!(
            regex_lite_match(ts),
            "timestamp must match %Y-%m-%dT%H:%M:%S.%L%z: {ts}"
        );
        assert_eq!(doc["level"], "INFO");
        assert_eq!(doc["loggerName"], "sutra_engine::otel::tests");
        assert_eq!(doc["message"], "engine event");
        assert_eq!(doc["service.name"], "wsf-shape-test");
        assert_eq!(doc["sequence"], 0);
        // Event fields flatten alongside (additional-field posture).
        assert_eq!(doc["deployment_id"], "dep-x");
        assert_eq!(doc["answer"], 42);
        // OTel trace correlation — the frozen JSON-log key names.
        let trace_id = doc["traceId"].as_str().expect("traceId inside a span");
        assert_eq!(trace_id.len(), 32, "32-hex trace id: {trace_id}");
        assert!(trace_id.bytes().all(|b| b.is_ascii_hexdigit()));
        let span_id = doc["spanId"].as_str().expect("spanId inside a span");
        assert_eq!(span_id.len(), 16, "16-hex span id: {span_id}");
        assert_eq!(doc["sampled"], "true");
    }

    /// `2026-07-12T09:00:00.123+00:00` without pulling a regex dependency.
    fn regex_lite_match(ts: &str) -> bool {
        let bytes = ts.as_bytes();
        bytes.len() == 29
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes[10] == b'T'
            && bytes[13] == b':'
            && bytes[16] == b':'
            && bytes[19] == b'.'
            && bytes[23] == b'+'
            && bytes[26] == b':'
            && ts[20..23].bytes().all(|b| b.is_ascii_digit())
    }

    #[test]
    fn json_log_lines_omit_trace_ids_outside_spans_and_when_otel_is_off() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let json_layer = StructuredJsonLayer {
            service_name: "wsf-shape-test".to_string(),
            otel_active: false,
            sequence: AtomicU64::new(0),
            dispatch: std::sync::OnceLock::new(),
            capture: Some(std::sync::Arc::clone(&captured)),
        };
        let subscriber = tracing_subscriber::registry().with(json_layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!("plain event");
        });

        let lines = captured.lock().unwrap();
        let doc: serde_json::Value = serde_json::from_str(&lines[0]).expect("valid JSON");
        assert_eq!(doc["level"], "WARN");
        assert!(doc.get("traceId").is_none(), "no ids when otel is off");
    }

    // ---- listener smoke (global no-op meter — must not panic) -----------------------

    #[test]
    fn metrics_listener_is_safe_on_the_noop_global_meter() {
        use sutra_executor::DeploymentId;
        let listener = OtelMetricsListener::new(default_metric_labels());
        let event = InstanceEvent {
            deployment: DeploymentId::of("dep-0000000000000000000000c1")
                .expect("valid deployment id"),
            labels: BTreeMap::from([("tenant".to_string(), "default".to_string())]),
            instance_id: "i-1".to_string(),
            process_id: "p".to_string(),
            module_version: "1.0.0".to_string(),
            audit_sink: None,
        };
        listener.on_instance_started(&event);
        listener.on_instance_completed(&event);
        listener.on_path_covered(&event, "happy");
    }

    // ---- coverage-percent gauge (event-driven) --------------------------------------

    fn coverage_meta(
        deployment_id: &str,
        process_id: &str,
        paths: &[&str],
        tenant: Option<&str>,
        module: Option<&str>,
        version: Option<&str>,
    ) -> CoverageMeta {
        CoverageMeta {
            deployment_id: deployment_id.to_string(),
            process_id: process_id.to_string(),
            declared_paths: paths.iter().map(|p| p.to_string()).collect(),
            tenant: tenant.map(str::to_string),
            module: module.map(str::to_string),
            version: version.map(str::to_string),
        }
    }

    fn instance_event(deployment_id: &str, process_id: &str) -> InstanceEvent {
        use sutra_executor::DeploymentId;
        InstanceEvent {
            deployment: DeploymentId::of(deployment_id).expect("valid deployment id"),
            labels: BTreeMap::from([("tenant".to_string(), "acme".to_string())]),
            instance_id: "i-1".to_string(),
            process_id: process_id.to_string(),
            module_version: "1.0.0".to_string(),
            audit_sink: None,
        }
    }

    /// Collect `(percent, sorted (key,value) attrs)` for the `sutra.coverage.percent` gauge.
    fn collect_coverage_points(
        exporter: &opentelemetry_sdk::metrics::InMemoryMetricExporter,
    ) -> Vec<(f64, Vec<(String, String)>)> {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        let mut out = Vec::new();
        for rm in exporter.get_finished_metrics().expect("finished metrics") {
            for scope in rm.scope_metrics() {
                for metric in scope.metrics() {
                    if metric.name() != names::METRIC_COVERAGE_PERCENT {
                        continue;
                    }
                    if let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data() {
                        for dp in gauge.data_points() {
                            let mut attrs: Vec<(String, String)> = dp
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect();
                            attrs.sort();
                            out.push((dp.value(), attrs));
                        }
                    }
                }
            }
        }
        out
    }

    fn has_attr(attrs: &[(String, String)], key: &str, value: &str) -> bool {
        attrs.iter().any(|(k, v)| k == key && v == value)
    }

    /// A wired listener against a collectable SDK meter — the gauge lands on `exporter`.
    fn wired_listener(meter: &Meter, metas: Vec<CoverageMeta>) -> OtelMetricsListener {
        OtelMetricsListener::new(default_metric_labels()).with_coverage(meter, metas)
    }

    /// (b) The pure covered/total computation.
    #[test]
    fn coverage_percent_computes_covered_over_total_times_100() {
        assert_eq!(coverage_percent(0, 4), 0.0);
        assert_eq!(coverage_percent(3, 4), 75.0);
        assert_eq!(coverage_percent(2, 2), 100.0);
        assert_eq!(
            coverage_percent(1, 0),
            0.0,
            "no declared paths ⇒ 0, never NaN"
        );
    }

    /// (a) Driving `on_path_covered` grows the in-memory covered-set and records the gauge
    /// with the right percent + attributes — mirrors the no-op-meter smoke test but collects.
    #[test]
    fn on_path_covered_records_the_coverage_gauge() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("sutra-engine");

        let listener = wired_listener(
            &meter,
            vec![coverage_meta(
                "dep-0000000000000000000000a1",
                "pay",
                &["path-a", "path-b", "path-c", "path-d"],
                Some("acme"),
                Some("billing"),
                Some("1.2.0"),
            )],
        );

        let event = instance_event("dep-0000000000000000000000a1", "pay");
        listener.on_path_covered(&event, "path-a");
        listener.on_path_covered(&event, "path-c");
        listener.on_path_covered(&event, "path-a"); // idempotent — still 2 of 4

        provider.force_flush().expect("flush");
        let points = collect_coverage_points(&exporter);
        assert_eq!(points.len(), 1, "one series for the one process");
        let (pct, attrs) = &points[0];
        assert!((pct - 50.0).abs() < 1e-9, "2/4 = 50%");
        assert!(has_attr(
            attrs,
            "deployment.id",
            "dep-0000000000000000000000a1"
        ));
        assert!(has_attr(attrs, "process.id", "pay"));
        assert!(has_attr(attrs, "tenant", "acme"));
        assert!(has_attr(attrs, "module", "billing"));
        assert!(has_attr(attrs, "version", "1.2.0"));
    }

    /// (c) The activation-initial record reflects PRE-EXISTING store coverage with NO fresh
    /// `on_path_covered` — a replica booting onto an already-covered deployment. Phase 3
    /// mechanism: the store READ (`covered_among` → [`covered_paths_of`]) happens off the
    /// listener (the runtime assembly's hoisted snapshot), and the listener applies it
    /// synchronously via [`OtelMetricsListener::apply_initial_coverage`].
    #[test]
    fn record_initial_coverage_reflects_pre_existing_store_state() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };
        use sutra_executor::{CoverageMetricStore as _, InMemoryCoverageStore};

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("sutra-engine");

        // Two of three declared metric flags already covered for this deployment.
        let seed_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("seed runtime");
        let mem = InMemoryCoverageStore::new();
        let declared = [
            "path-a".to_string(),
            "path-b".to_string(),
            "path-c".to_string(),
        ];
        // Seed + read back the covered flags — the hoisted-snapshot mechanism the runtime
        // assembly runs once per activation (`seed_declared_coverage`).
        let initial: InitialCoverage = seed_rt.block_on(async {
            mem.seed_declared("dep-0000000000000000000000a2", &declared)
                .await
                .unwrap();
            mem.mark_path_covered("dep-0000000000000000000000a2", "path-a")
                .await
                .unwrap();
            mem.mark_path_covered("dep-0000000000000000000000a2", "path-b")
                .await
                .unwrap();
            let urns: Vec<String> = declared.iter().map(|p| metric_flag_urn(p)).collect();
            let flagged = mem
                .covered_among("dep-0000000000000000000000a2", &urns)
                .await
                .unwrap();
            let mut initial = InitialCoverage::new();
            initial.insert(
                (
                    "dep-0000000000000000000000a2".to_string(),
                    "pay".to_string(),
                ),
                covered_paths_of(&declared, &flagged),
            );
            initial
        });

        let listener = wired_listener(
            &meter,
            vec![coverage_meta(
                "dep-0000000000000000000000a2",
                "pay",
                &["path-a", "path-b", "path-c"],
                None, // no namespace resolved ⇒ tenant/module/version omitted
                None,
                None,
            )],
        );

        // No on_path_covered — only the activation-initial apply + record (synchronous:
        // safe wherever the engine build runs, including a lane's async actor task).
        listener.apply_initial_coverage(&initial);

        provider.force_flush().expect("flush");
        let points = collect_coverage_points(&exporter);
        assert_eq!(points.len(), 1);
        let (pct, attrs) = &points[0];
        assert!(
            (pct - (2.0 / 3.0 * 100.0)).abs() < 1e-9,
            "2/3 covered at boot"
        );
        assert!(has_attr(attrs, "process.id", "pay"));
        assert!(
            attrs.iter().all(|(k, _)| k != "tenant"),
            "tenant omitted when no namespace resolved"
        );
    }

    // ---- metric-name / label pins ---------------------------------------------------

    #[test]
    fn metric_names_are_the_frozen_wire_names() {
        // The `sutra.*` meter names are part of the wire contract — pin them so a rename is
        // a conscious break, not a silent one.
        assert_eq!(names::METRIC_INSTANCE_STARTED, "sutra.instance.started");
        assert_eq!(names::METRIC_INSTANCE_COMPLETED, "sutra.instance.completed");
        assert_eq!(names::METRIC_INSTANCE_SUSPENDED, "sutra.instance.suspended");
        assert_eq!(names::METRIC_INSTANCE_RESUMED, "sutra.instance.resumed");
        assert_eq!(names::METRIC_TOKEN_ENTERED, "sutra.token.entered");
        assert_eq!(names::METRIC_TOKEN_LEFT, "sutra.token.left");
        assert_eq!(names::METRIC_TASK_INVOKED, "sutra.task.invoked");
        assert_eq!(names::METRIC_TASK_COMPLETED, "sutra.task.completed");
        assert_eq!(names::METRIC_TASK_FAILED, "sutra.task.failed");
        assert_eq!(names::METRIC_TASK_DURATION, "sutra.task.duration");
        assert_eq!(
            names::METRIC_COVERAGE_PATH_COVERED,
            "sutra.coverage.path_covered"
        );
        // The dimension keys.
        assert_eq!(names::ATTR_DEPLOYMENT_ID, "deployment.id");
        assert_eq!(names::ATTR_NODE_TYPE, "node.type");
        assert_eq!(names::ATTR_NODE_ID, "node.id");
        assert_eq!(names::ATTR_TASK_NAME, "task.name");
    }

    #[test]
    fn instance_id_is_never_emitted_as_a_meter_attribute() {
        // The cardinality-discipline invariant (`instanceIdNeverEmittedAsTag`): no meter
        // dimension is the raw instance id. The Rust listener sources dimensions only from
        // `deployment.id` + the label allowlist + node/task names — never the instance id —
        // so the invariant is structural; this pins it against a regression.
        use sutra_executor::DeploymentId;
        let id = "11111111-2222-3333-4444-555555555555";
        let dep = DeploymentId::of("dep-0000000000000000000000c2").expect("valid deployment id");
        let labels = BTreeMap::from([("tenant".to_string(), "acme".to_string())]);
        let listener = OtelMetricsListener::new(default_metric_labels());

        let instance = InstanceEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: id.to_string(),
            process_id: "payments-v1".to_string(),
            module_version: "1.0.0".to_string(),
            audit_sink: None,
        };
        let token = TokenEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: id.to_string(),
            node_id: "serviceTask_1".to_string(),
            node_type: "serviceTask".to_string(),
            audit_sink: None,
            payload_json: None,
        };
        let task = TaskEvent {
            deployment: dep,
            labels,
            instance_id: id.to_string(),
            task_name: "channel.send".to_string(),
            duration_nanos: 1_000_000,
        };

        let mut all = listener.instance_attrs(&instance);
        all.extend(listener.token_attrs(&token));
        all.extend(listener.task_attrs(&task));

        assert!(!all.is_empty());
        for kv in &all {
            let key = kv.key.as_str();
            assert_ne!(key, "instance_id");
            assert_ne!(key, "instanceId");
            assert_ne!(key, names::ATTR_INSTANCE_ID);
            assert_ne!(
                kv.value.to_string(),
                id,
                "no dimension value is the raw instance id"
            );
        }
        // Positive pins: the identity + node/task dimensions ARE present.
        assert!(all
            .iter()
            .any(|kv| kv.key.as_str() == names::ATTR_DEPLOYMENT_ID));
        assert!(all.iter().any(|kv| kv.key.as_str() == names::ATTR_NODE_ID));
        assert!(all
            .iter()
            .any(|kv| kv.key.as_str() == names::ATTR_TASK_NAME));
    }
}
