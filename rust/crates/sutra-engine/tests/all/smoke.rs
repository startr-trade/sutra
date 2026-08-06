//! Smoke test: boot the engine skeleton against the REAL money-transfer deployment sealed
//! into a `.sutra` archive on port 0 (dynamic — never a fixed host port) and GET the health
//! endpoints. `.sutra` archives under `SUTRA_DEPLOYMENTS_DIR` are the only deployment model.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};

/// Seal the committed money-transfer package directory into a fresh temp deployments dir
/// and return it (kept for the test's lifetime — the engine scans + watches it).
fn money_transfer_deployments_dir() -> PathBuf {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/money-transfer/deployments-src/default--money-transfer--1.0.0");
    let dir = std::env::temp_dir().join(format!(
        "smoke-deployments-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("deployments dir");
    sutra_loader::assemble_dir(&src, &dir, &sutra_loader::PackageOptions::default())
        .expect("money-transfer package seals into one .sutra archive");
    dir
}

fn config_for(deployments_dir: PathBuf) -> EngineConfig {
    EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        crypto_master_key: None,
        crypto_envelope: Default::default(),
        incident_sql: false,
        deployments_dir: Some(deployments_dir),
        deployments_poll_interval: std::time::Duration::from_secs(2),
        http_port: 0, // dynamic — the engine reports the bound port
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

/// A minimal blocking HTTP/1.1 GET (keeps the test dependency-free).
fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    (status, response)
}

#[tokio::test(flavor = "multi_thread")]
async fn boots_and_reports_ready_on_money_transfer_archive() {
    let deployments_dir = money_transfer_deployments_dir();
    let engine = serve(config_for(deployments_dir.clone()))
        .await
        .expect("engine boots");
    let addr = engine.local_addr;
    assert_ne!(addr.port(), 0, "port 0 must resolve to a real bound port");

    let (status, body) = tokio::task::spawn_blocking(move || http_get(addr, "/sutra/health/ready"))
        .await
        .unwrap();
    assert_eq!(status, 200, "ready after the loader completed: {body}");
    assert!(
        body.contains("\"status\":\"UP\""),
        "SmallRye-shaped UP body: {body}"
    );
    assert!(
        body.contains("\"deployments\":1"),
        "one loaded deployment: {body}"
    );
    // The readiness payload reports the shard router's LIVE lane count — the black-box
    // evidence a container harness reads back (the `sutra.engine.shard.*` meters need an
    // OTLP collector; thread names are not observable over HTTP). It tracks the boot's
    // configured lane count, so this holds at the default 1 AND under the N-lane rerun
    // (`SUTRA_ENGINE_SHARDS=4`, see `shard_support`).
    let expected_shards = crate::shard_support::engine_shards_from_env().shards;
    assert!(
        body.contains(&format!("\"shards\":{expected_shards}")),
        "the live router lane count is reported ({expected_shards} expected): {body}"
    );

    let (status, body) = tokio::task::spawn_blocking(move || http_get(addr, "/sutra/health/live"))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"UP\""));

    engine.shutdown().await;
    let _ = std::fs::remove_dir_all(&deployments_dir);
}

/// The response body (after the header/body separator) of a raw HTTP/1.1 response.
fn body_of(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("")
}

/// Pull the first `"deploymentId":"dep-…"` value out of a `/sutra/deployments` JSON body.
fn extract_deployment_id(resp: &str) -> Option<String> {
    let body = body_of(resp);
    let key = "\"deploymentId\":\"";
    let start = body.find(key)? + key.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// The per-deployment OpenAPI 3.1 surface is generated at deploy time and served live
/// at `GET /sutra/deployments/{id}/openapi` — YAML by default, JSON via `?format=json`, 404 for
/// an unknown id. Boots the real money-transfer archive and exercises the whole path end to end
/// (generation → cache → serve → content-negotiation).
#[tokio::test(flavor = "multi_thread")]
async fn serves_the_per_deployment_openapi_surface() {
    let deployments_dir = money_transfer_deployments_dir();
    let engine = serve(config_for(deployments_dir.clone()))
        .await
        .expect("engine boots");
    let addr = engine.local_addr;

    // The watcher publishes the active set + specs on its first tick; poll until an id appears.
    let mut dep_id = None;
    for _ in 0..50 {
        let (status, resp) =
            tokio::task::spawn_blocking(move || http_get(addr, "/sutra/deployments"))
                .await
                .unwrap();
        if status == 200 {
            if let Some(id) = extract_deployment_id(&resp) {
                dep_id = Some(id);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let dep_id = dep_id.expect("an active deployment id appears in /sutra/deployments");
    assert!(dep_id.starts_with("dep-"), "content-hash id: {dep_id}");

    // Default: YAML.
    let path = format!("/sutra/deployments/{dep_id}/openapi");
    let (status, resp) = {
        let p = path.clone();
        tokio::task::spawn_blocking(move || http_get(addr, &p))
            .await
            .unwrap()
    };
    assert_eq!(status, 200, "openapi served: {resp}");
    assert!(
        resp.contains("application/yaml"),
        "yaml content-type: {resp}"
    );
    let body = body_of(&resp);
    assert!(body.contains("openapi: 3.1.0"), "openapi 3.1 yaml: {body}");
    assert!(
        body.contains(&format!("x-sutra-deployment-id: {dep_id}")),
        "spec carries its own deployment id: {body}"
    );
    assert!(body.contains("paths:"), "has a paths section: {body}");

    // JSON via ?format=json.
    let (status, resp) = {
        let p = format!("{path}?format=json");
        tokio::task::spawn_blocking(move || http_get(addr, &p))
            .await
            .unwrap()
    };
    assert_eq!(status, 200);
    assert!(
        resp.contains("application/json"),
        "json content-type: {resp}"
    );
    let body = body_of(&resp);
    assert!(body.trim_start().starts_with('{'), "json body: {body}");
    assert!(body.contains("\"openapi\""), "json openapi field: {body}");

    // Unknown id → 404.
    let (status, _resp) = tokio::task::spawn_blocking(move || {
        http_get(
            addr,
            "/sutra/deployments/dep-ffffffffffffffffffffffff/openapi",
        )
    })
    .await
    .unwrap();
    assert_eq!(status, 404, "unknown deployment id 404s");

    engine.shutdown().await;
    let _ = std::fs::remove_dir_all(&deployments_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_to_boot_on_a_missing_deployments_dir() {
    let config = config_for(PathBuf::from("/nonexistent/sutra-deployments"));
    assert!(
        serve(config).await.is_err(),
        "fail-closed on a missing deployments directory"
    );
}
