//! Conformance: the engine-global audit sinks against the engine image.
//!
//! - **The dedicated OTLP audit pipeline.** With `SUTRA_AUDIT_OTEL_ENDPOINT` pointed at an
//!   OpenTelemetry collector, the engine boots (it used to panic at start: each executor lane built
//!   the gRPC exporter outside the tokio runtime), runs an instance, and the collector receives its
//!   audit events as log records on the `sutra.audit` scope.
//! - **Correlation keys.** Every audit event of an instance carries the instance's `<q:alias>`
//!   keys — in the OTLP record as `correlation.<name>` attributes, in the JSONL line as a
//!   `correlation` object — across a park and resume, with a `sensitive` alias masked.
//! - **Whole JSONL lines.** Every line of the JSONL file is exactly one JSON event.
//!
//! One engine serves both tests: the package's `audited-otel` process routes to the OTLP sink,
//! its `audited-jsonl` process to the JSONL file. The package lives in `fixtures/audit-sinks/`.

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;
use testcontainers::core::{AccessMode, Mount};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "audit-sinks-test-key";
const COLLECTOR_IMAGE: &str = "otel/opentelemetry-collector-contrib";
const COLLECTOR_TAG: &str = "0.114.0";
/// Where the JSONL sink writes inside the engine container.
const JSONL_PATH: &str = "/tmp/sutra-audit/audit.jsonl";
/// The sensitive alias's raw value — must never reach either sink.
const HOLDER: &str = "Jane Example";

/// An OTLP receiver that prints every log record (scope, attributes) to its own stdout.
const COLLECTOR_CONFIG: &str = r#"receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [debug]
"#;

struct Fixture {
    _pg: PgFixture,
    collector: Container<GenericImage>,
    engine: engine::EngineHandle,
}

fn assemble_fixture() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/audit-sinks/default--audited--1.0.0");
    let out = util::world_readable_temp_dir("sutra-archives");
    sutra_loader::package::assemble_dir(&package, &out, &sutra_loader::PackageOptions::default())
        .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
    util::make_world_readable(&out);
    out
}

fn start_collector(name: &str) -> Container<GenericImage> {
    let dir = util::world_readable_temp_dir("sutra-otelcol");
    std::fs::write(dir.join("config.yaml"), COLLECTOR_CONFIG).expect("write collector config");
    util::make_world_readable(&dir);
    let container = GenericImage::new(COLLECTOR_IMAGE, COLLECTOR_TAG)
        .with_network(util::network_for(util::suite_of(name)))
        .with_container_name(name)
        .with_mount(
            Mount::bind_mount(
                dir.join("config.yaml").to_string_lossy().to_string(),
                "/etc/otelcol-contrib/config.yaml",
            )
            .with_access_mode(AccessMode::ReadOnly),
        )
        .start()
        .expect("start the OTel collector (docker required)");
    sutra_testkit::reap_on_exit(container.id());
    container
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        std::thread::spawn(|| {
            let pg = engine::start_postgres("auditsinks");
            let collector_name = format!("auditsinks-otelcol-{}", util::short_id());
            let collector = start_collector(&collector_name);
            let engine = EngineBuilder::new("auditsinks", &pg)
                .env("AUDITED_API_KEY", API_KEY)
                .env(
                    "SUTRA_AUDIT_OTEL_ENDPOINT",
                    format!("http://{collector_name}:4317"),
                )
                .env("SUTRA_AUDIT_JSONL", JSONL_PATH)
                .expected_deployments(1)
                .start(&assemble_fixture());
            Fixture {
                _pg: pg,
                collector,
                engine,
            }
        })
        .join()
        .expect("audit-sinks topology")
    })
}

/// The engine boots with the dedicated OTLP audit endpoint set, runs an instance through a park
/// and resume, and the collector receives its events on the `sutra.audit` scope, each with the
/// instance's correlation keys (the sensitive one masked).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_audit_otel_endpoint_boots_and_the_collector_receives_sutra_audit_records() {
    let client = reqwest::Client::new();
    let reference = "AUD-OTEL-1";
    run_one(&client, "otel", reference).await;

    let deadline = Instant::now() + Duration::from_secs(30);
    let logs = loop {
        let logs = container_logs(fixture().collector.id());
        if logs.contains("event_type: Str(INSTANCE_COMPLETED)") {
            break logs;
        }
        assert!(
            Instant::now() < deadline,
            "the collector never received the instance's INSTANCE_COMPLETED record:\n{}",
            tail(&logs)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        logs.contains("InstrumentationScope sutra.audit"),
        "records arrive on the sutra.audit scope:\n{}",
        tail(&logs)
    );
    for event in [
        "INSTANCE_STARTED",
        "INSTANCE_SUSPENDED",
        "INSTANCE_RESUMED",
        "NODE_ENTERED",
    ] {
        assert!(
            logs.contains(&format!("event_type: Str({event})")),
            "{event} exported:\n{}",
            tail(&logs)
        );
    }
    assert!(
        logs.contains(&format!("correlation.orderRef: Str({reference})")),
        "the alias key rides on the record:\n{}",
        tail(&logs)
    );
    assert!(
        logs.contains("correlation.holder: Str(***REDACTED***)"),
        "the sensitive alias is masked:\n{}",
        tail(&logs)
    );
    assert!(
        !logs.contains(HOLDER),
        "the raw sensitive value never reaches the collector"
    );
}

/// Every JSONL event of an instance carries its correlation keys across the park and resume,
/// the sensitive one masked, and every line of the file is exactly one JSON event.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_audit_jsonl_events_carry_the_instance_correlation_keys() {
    let client = reqwest::Client::new();
    let reference = "AUD-JSONL-1";
    run_one(&client, "jsonl", reference).await;

    let deadline = Instant::now() + Duration::from_secs(30);
    let (body, events) = loop {
        let body = jsonl_file();
        let events: Vec<Value> = body
            .lines()
            .map(|line| {
                serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("not exactly one JSON event ({e}): {line}"))
            })
            .filter(|v: &Value| v["correlation"]["orderRef"] == reference)
            .collect();
        if events
            .iter()
            .any(|v| v["eventType"] == "INSTANCE_COMPLETED")
        {
            break (body, events);
        }
        assert!(
            Instant::now() < deadline,
            "the instance never completed in the JSONL file:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let instance = events[0]["instanceId"]
        .as_str()
        .expect("instanceId")
        .to_string();
    assert_eq!(
        body.lines().filter(|l| l.contains(&instance)).count(),
        events.len(),
        "every event of the instance carries the key"
    );
    for event in ["INSTANCE_STARTED", "INSTANCE_SUSPENDED", "INSTANCE_RESUMED"] {
        assert!(
            events.iter().any(|v| v["eventType"] == event),
            "{event} journaled with the key: {events:?}"
        );
    }
    for v in &events {
        assert_eq!(v["correlation"]["holder"], "***REDACTED***", "{v}");
    }
    assert!(
        !body.contains(HOLDER),
        "the raw sensitive value never reaches the file"
    );
}

/// Start one instance on `<kind>-start`, then answer its receive task so it resumes and completes.
async fn run_one(client: &reqwest::Client, kind: &str, reference: &str) {
    let (status, body) = post(
        client,
        &format!("/channels/audited/{kind}"),
        &format!("{{\"ref\":\"{reference}\",\"holder\":\"{HOLDER}\"}}"),
    )
    .await;
    assert_eq!(status, 200, "the start parks at the receive task: {body}");
    let (status, body) = post(
        client,
        &format!("/channels/audited/{kind}/status"),
        &format!("{{\"ref\":\"{reference}\"}}"),
    )
    .await;
    assert_eq!(status, 200, "the status resumes the instance: {body}");
}

async fn post(client: &reqwest::Client, path: &str, body: &str) -> (u16, String) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}{path}",
            fixture().engine.http_port
        ))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.to_string())
        .send()
        .await
        .expect("engine request");
    let status = resp.status().as_u16();
    (status, resp.text().await.unwrap_or_default())
}

/// The JSONL file as it stands, copied out of the engine container (the image has no shell).
fn jsonl_file() -> String {
    let dir = util::world_readable_temp_dir("sutra-audit-jsonl");
    let target = dir.join("audit.jsonl");
    let out = Command::new("docker")
        .args([
            "cp",
            &format!("{}:{JSONL_PATH}", fixture().engine.container.id()),
            &target.to_string_lossy(),
        ])
        .output()
        .expect("docker cp");
    let body = if out.status.success() {
        std::fs::read_to_string(&target).unwrap_or_default()
    } else {
        String::new()
    };
    let _ = std::fs::remove_dir_all(&dir);
    body
}

fn container_logs(id: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("docker logs");
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn tail(logs: &str) -> String {
    let lines: Vec<&str> = logs.lines().collect();
    lines[lines.len().saturating_sub(120)..].join("\n")
}
