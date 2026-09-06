//! End-to-end: a RUNNING engine, booted on the committed `call-log-load` archive, accepting a
//! CSV upload and a fixed-width upload on the same channel.
//!
//! This is the test that keeps the feature honest. `sutra lint` never decodes anything and the
//! codec unit tests construct their codec directly, so both were green while the engine assembly
//! still hardcoded `["xml","json","yaml"]` and ignored `codec-manifest.yaml` entirely — a
//! `formats: [csv]` package would seal cleanly and then reject every upload as an unsupported
//! content-type. Only booting the real archive catches that.
//!
//! No database is configured, so the detached tail fails at the store write. That is deliberate
//! and irrelevant: everything asserted here happens at INTAKE, before any store is touched.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};

const API_KEY: &str = "call-log-e2e-key";

fn call_log_deployments_dir() -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/call-log-load/deployments-src/default--call-log--1.0.0");
    let dir = std::env::temp_dir().join(format!(
        "call-log-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("deployments dir");
    sutra_loader::assemble_dir(&src, &dir, &sutra_loader::PackageOptions::default())
        .expect("the call-log package seals into one .sutra archive");
    dir
}

fn sample(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/call-log-load/sample")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn config_for(deployments_dir: PathBuf) -> EngineConfig {
    EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
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
        telemetry: sutra_engine::TelemetryConfig::default(),
        admin_auth: Default::default(),
        now_override: None,
    }
}

/// A minimal blocking HTTP/1.1 POST returning `(status, content-type, body)`.
fn http_post(
    addr: SocketAddr,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: {content_type}\r\nX-Api-Key: {API_KEY}\r\n\
         X-Request-Id: e2e-{}\r\nContent-Length: {}\r\n\r\n",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        body.len()
    )
    .expect("request head");
    stream.write_all(body).expect("request body");
    stream.flush().expect("flush");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in: {response}"));
    let ct = response
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-type:"))
        .map(|l| l.split_once(':').unwrap().1.trim().to_string())
        .unwrap_or_default();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, ct, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_engine_accepts_both_wire_forms_and_rejects_a_bad_batch_in_kind() {
    std::env::set_var("CDR_UPLOAD_API_KEY", API_KEY);
    let deployments_dir = call_log_deployments_dir();
    let engine = serve(config_for(deployments_dir.clone()))
        .await
        .expect("engine boots on the call-log archive");
    let addr = engine.local_addr;
    const PATH: &str = "/channels/cdr-upload";

    // ---- CSV: the manifest's `formats: [csv]` must be LIVE, not merely validated -------------
    let csv = sample("call-logs.csv");
    let (_status, ct, body) =
        tokio::task::spawn_blocking(move || http_post(addr, PATH, "text/csv", &csv))
            .await
            .unwrap();
    assert!(
        !body.contains("CAPABILITY_MISMATCH"),
        "a text/csv upload must be DECODED by the schema codec, not refused as an unsupported \
         content-type — that is what a hardcoded xml/json/yaml format set does: {body}"
    );
    assert!(
        body.contains("SUTRA.INBOUND.PERSISTENCE_REQUIRED"),
        "a valid batch gets past decode, validation and routing, and stops only at the \
         no-database persistence check: {body}"
    );
    // Even this failure speaks the caller's format (R4).
    assert!(ct.starts_with("text/csv"), "content-type {ct:?}: {body}");

    // ---- fixed-width: the SAME channel, the same schema, selected by content-type ------------
    let fixed = sample("call-logs.fixed-width.txt");
    let (_status, _ct, body) =
        tokio::task::spawn_blocking(move || http_post(addr, PATH, "text/plain", &fixed))
            .await
            .unwrap();
    assert!(
        !body.contains("CAPABILITY_MISMATCH"),
        "a text/plain upload must select the fixed-width parser: {body}"
    );
    assert!(
        body.contains("SUTRA.INBOUND.PERSISTENCE_REQUIRED"),
        "the fixed-width batch reaches exactly the same point as the CSV one: {body}"
    );

    // ---- a bad batch: refused at intake, BEFORE the persistence check ------------------------
    let bad = sample("call-logs-with-a-bad-row.csv");
    let (status, ct, body) =
        tokio::task::spawn_blocking(move || http_post(addr, PATH, "text/csv", &bad))
            .await
            .unwrap();
    assert_ne!(
        status, 200,
        "a batch with three bad cells must not be accepted: {body}"
    );
    assert!(
        body.contains("SUTRA.INBOUND.VALIDATION_REJECT"),
        "the bad batch is refused by VALIDATION, not by the persistence check the good ones \
         reached — validation runs first, at intake, on the whole file: {body}"
    );
    assert!(
        ct.starts_with("text/csv"),
        "the caller posted text/csv, so the problem document comes back as a table \
         (got content-type {ct:?}): {body}"
    );
    assert!(
        body.starts_with("field,value"),
        "an RFC 7807 problem rendered as a table: {body}"
    );

    engine.shutdown().await;
    let _ = std::fs::remove_dir_all(&deployments_dir);
}
