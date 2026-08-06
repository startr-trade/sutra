//! Telemetry integration slice: the FULL engine (`serve`) boots a minimal resource
//! tree with telemetry active against an IN-PROCESS OTLP gRPC receiver (the same
//! `grpc-tonic` wire the k8s collector serves on 4317). One sync POST through an HTTP
//! channel must land all three signals at the receiver:
//!
//! - **traces** — `sutra.dispatch` / `sutra.resolve` / `sutra.validate` /
//!   `sutra.execute` spans under ONE trace, `service.name` resource, `deployment.id`
//!   span attribute (the identity dimension);
//! - **metrics** — the frozen `sutra.*` meter names with DELTA temporality
//!   (`sutra.instance.started` / `.completed` increment for the driven flow);
//! - **logs** — OTLP log records carrying the `service.name` resource (the
//!   `sutra-app-logs` path `ObservabilityK8sIT` asserts).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point, AggregationTemporality};

use sutra_engine::otel::TemporalityPreference;
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig, TelemetryConfig};

// ---- in-process OTLP gRPC receiver --------------------------------------------------

#[derive(Clone, Default)]
struct Collector {
    traces: Arc<Mutex<Vec<ExportTraceServiceRequest>>>,
    metrics: Arc<Mutex<Vec<ExportMetricsServiceRequest>>>,
    logs: Arc<Mutex<Vec<ExportLogsServiceRequest>>>,
}

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.traces.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.metrics.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.logs.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}

async fn otlp_receiver() -> (SocketAddr, Collector) {
    let collector = Collector::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = collector.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(svc.clone()))
            .add_service(MetricsServiceServer::new(svc.clone()))
            .add_service(LogsServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("OTLP receiver serves");
    });
    (addr, collector)
}

// ---- the synthesized resource tree (sync flow — start → template task → end) --------

const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:wsf-demo:1.0.0">
  <bpmn:process id="pong">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="wsf-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T1" implementation="pong.hbs"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T1" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const CHANNELS_YAML: &str = r#"channels:
  # Sync intake (http default ack-mode on-complete): the POST returns once the
  # instance ran to completion — instance.started AND instance.completed both fire.
  # The builtin json codec keeps the decode → validate intake legs (and their
  # sutra.decode / sutra.validate spans) in-path, as every gate channel has a codec.
  - name: wsf-in
    transport: http
    bind: "POST /channels/wsf-in"
    codec: json
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: wsf-demo-key
        header: X-Api-Key
"#;

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Build a synthetic standalone deployment-package directory, seal it into one `.sutra`,
/// and return the archives directory the engine watches (`SUTRA_DEPLOYMENTS_DIR`).
fn synthesize_deployments_dir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("wsf-otel-it-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let pkg = base.join("default--wsf-demo--1.0.0");
    write(pkg.join("bpmn/pong.bpmn"), BPMN);
    write(pkg.join("templates/pong.hbs"), "<pong/>");
    write(pkg.join("channels.yaml"), CHANNELS_YAML);
    write(
        pkg.join("package.yaml"),
        "labels:\n  \"tenant\": \"default\"\n  \"module\": \"wsf-demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    let out = base.join("archives");
    std::fs::create_dir_all(&out).expect("archives dir");
    sutra_loader::assemble_dir(&pkg, &out, &sutra_loader::PackageOptions::default())
        .expect("synthetic package seals into one .sutra archive");
    out
}

// ---- minimal blocking HTTP POST ------------------------------------------------------

fn http_post(addr: SocketAddr, path: &str, body: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: wsf-demo-key\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code")
}

// ---- proto digging helpers -----------------------------------------------------------

fn string_attr(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    key: &str,
) -> Option<String> {
    attributes.iter().find(|kv| kv.key == key).and_then(|kv| {
        kv.value.as_ref().and_then(|v| match &v.value {
            Some(any_value::Value::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
    })
}

/// All exported spans as `(resource service.name, trace_id, span name, attributes)`.
type FlatSpan = (
    String,
    Vec<u8>,
    String,
    Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
);

fn flatten_spans(requests: &[ExportTraceServiceRequest]) -> Vec<FlatSpan> {
    let mut out = Vec::new();
    for request in requests {
        for rs in &request.resource_spans {
            let service = rs
                .resource
                .as_ref()
                .and_then(|r| string_attr(&r.attributes, "service.name"))
                .unwrap_or_default();
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    out.push((
                        service.clone(),
                        span.trace_id.clone(),
                        span.name.clone(),
                        span.attributes.clone(),
                    ));
                }
            }
        }
    }
    out
}

/// All exported metrics as `(name, Sum temporality, Sum value, datapoint attrs)`.
type FlatMetric = (
    String,
    Option<i32>,
    Option<i64>,
    Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
);

fn flatten_metrics(requests: &[ExportMetricsServiceRequest]) -> Vec<FlatMetric> {
    let mut out = Vec::new();
    for request in requests {
        for rm in &request.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    match &m.data {
                        Some(metric::Data::Sum(sum)) => {
                            for dp in &sum.data_points {
                                let value = match dp.value {
                                    Some(number_data_point::Value::AsInt(i)) => Some(i),
                                    Some(number_data_point::Value::AsDouble(d)) => Some(d as i64),
                                    None => None,
                                };
                                out.push((
                                    m.name.clone(),
                                    Some(sum.aggregation_temporality),
                                    value,
                                    dp.attributes.clone(),
                                ));
                            }
                        }
                        _ => out.push((m.name.clone(), None, None, Vec::new())),
                    }
                }
            }
        }
    }
    out
}

/// The trace-id of THIS test's own `sutra.dispatch` span — the one bound to the `wsf-in`
/// channel — if it has been exported yet. Any foreign engine test running concurrently in
/// this binary exports its own `sutra.dispatch` spans through this same process-global OTLP
/// subscriber; those carry a different `channel` (and trace-id) and must be ignored.
fn own_dispatch_trace(spans: &[FlatSpan]) -> Option<Vec<u8>> {
    spans.iter().find_map(|(_, tid, name, attrs)| {
        (name == "sutra.dispatch" && string_attr(attrs, "channel").as_deref() == Some("wsf-in"))
            .then(|| tid.clone())
    })
}

// ---- the slice ------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn one_sync_post_exports_traces_metrics_and_logs_over_otlp() {
    let (otlp_addr, collector) = otlp_receiver().await;

    // Telemetry active against the in-process receiver; delta temporality as the k8s
    // harness sets (OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta — the env
    // alias resolution itself is unit-tested in otel.rs/config.rs).
    let telemetry_config = TelemetryConfig {
        otlp_endpoint: Some(format!("http://{otlp_addr}")),
        service_name: "wsf-otel-it".to_string(),
        metrics_temporality: TemporalityPreference::Delta,
        metrics_export_interval_ms: Some(100),
        ..TelemetryConfig::default()
    };
    let telemetry = sutra_engine::otel::init(&telemetry_config);
    assert!(telemetry.is_active(), "all three exporters must come up");

    let engine = serve(EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(synthesize_deployments_dir()),
        deployments_poll_interval: std::time::Duration::from_secs(2),
        http_port: 0,
        datasource_url: None,
        datasource_username: None,
        datasource_password: None,
        outbox_tick_interval: std::time::Duration::from_secs(5),
        outbox_retry: Default::default(),
        deferred_ack: Default::default(),
        external_task: Default::default(),
        instance_sweep: Default::default(),
        engine_shards: crate::shard_support::engine_shards_from_env(),
        instance_retention: Default::default(),
        audit: Default::default(),
        payload_cap_bytes: 10 * 1024 * 1024,
        rls_bypass_check_enabled: true,
        telemetry: telemetry_config,
        admin_auth: Default::default(),
        now_override: None,
    })
    .await
    .expect("engine boots");
    let addr = engine.local_addr;

    // Drive ONE sync flow (http ack-mode default on-complete: 2xx after completion).
    let status =
        tokio::task::spawn_blocking(move || http_post(addr, "/channels/wsf-in", "{\"ping\":1}"))
            .await
            .unwrap();
    assert!(
        (200..300).contains(&status),
        "sync flow must complete, got HTTP {status}"
    );

    // Export is async (batch span processor + periodic metric reader): flush and poll.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let (spans, metrics, logs) = loop {
        telemetry.flush();
        let spans = flatten_spans(&collector.traces.lock().unwrap());
        let metrics = flatten_metrics(&collector.metrics.lock().unwrap());
        let logs: usize = collector
            .logs
            .lock()
            .unwrap()
            .iter()
            .flat_map(|r| &r.resource_logs)
            .map(|rl| {
                let named = rl
                    .resource
                    .as_ref()
                    .and_then(|r| string_attr(&r.attributes, "service.name"))
                    .as_deref()
                    == Some("wsf-otel-it");
                if named {
                    rl.scope_logs.iter().map(|sl| sl.log_records.len()).sum()
                } else {
                    0
                }
            })
            .sum();
        // This test installs the process-global OTLP subscriber (`otel::init`), so the
        // receiver also captures `sutra.*` spans from any OTHER engine test running
        // concurrently in this binary — they boot `serve()` with no telemetry of their own
        // and inherit this global exporter. Anchor on OUR flow: the `sutra.dispatch` span
        // bound to channel `wsf-in`, and wait until every waterfall span sharing ITS
        // trace-id has landed (a foreign flow carries a different channel + trace-id).
        let our_trace = own_dispatch_trace(&spans);
        let have_spans = our_trace.as_ref().is_some_and(|trace| {
            [
                "sutra.dispatch",
                "sutra.resolve",
                "sutra.decode",
                "sutra.validate",
                "sutra.execute",
            ]
            .iter()
            .all(|name| spans.iter().any(|(_, tid, n, _)| n == name && tid == trace))
        });
        let have_metrics = ["sutra.instance.started", "sutra.instance.completed"]
            .iter()
            .all(|name| metrics.iter().any(|(n, _, _, _)| n == name));
        if (have_spans && have_metrics && logs > 0) || std::time::Instant::now() > deadline {
            break (spans, metrics, logs);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };

    // ---- traces -----------------------------------------------------------------
    // Pick OUR dispatch span (channel `wsf-in`) — never a foreign concurrent flow's — and
    // assert the rest of the waterfall shares ITS trace-id (one hierarchical engine trace).
    let dispatch = spans
        .iter()
        .find(|(_, _, name, attrs)| {
            name == "sutra.dispatch" && string_attr(attrs, "channel").as_deref() == Some("wsf-in")
        })
        .expect("sutra.dispatch span for the wsf-in flow exported");
    assert_eq!(dispatch.0, "wsf-otel-it", "service.name resource attribute");
    assert!(
        string_attr(&dispatch.3, "deployment.id")
            .is_some_and(|v| v.starts_with("dep-") && v.len() == 28),
        "deployment.id attribute on sutra.dispatch (identity dimension): {:?}",
        dispatch.3
    );
    for name in [
        "sutra.resolve",
        "sutra.decode",
        "sutra.validate",
        "sutra.execute",
    ] {
        assert!(
            spans
                .iter()
                .any(|(_, tid, n, _)| n == name && *tid == dispatch.1),
            "{name} shares the dispatch trace (one hierarchical engine trace)"
        );
    }

    // ---- metrics ----------------------------------------------------------------
    for name in ["sutra.instance.started", "sutra.instance.completed"] {
        let (_, temporality, value, attributes) = metrics
            .iter()
            .find(|(n, _, _, _)| n == name)
            .unwrap_or_else(|| panic!("{name} metric exported"));
        assert_eq!(
            *temporality,
            Some(AggregationTemporality::Delta as i32),
            "{name} must export DELTA temporality (the ES pipeline drops cumulative)"
        );
        assert!(
            value.unwrap_or(0) >= 1,
            "{name} counts the driven flow: {value:?}"
        );
        assert!(
            string_attr(attributes, "deployment.id").is_some(),
            "deployment.id dimension on {name}: {attributes:?}"
        );
    }

    // ---- logs ---------------------------------------------------------------------
    assert!(
        logs > 0,
        "OTLP log records with the service.name resource must arrive \
         (the sutra-app-logs path of ObservabilityK8sIT)"
    );

    telemetry.shutdown();
}
