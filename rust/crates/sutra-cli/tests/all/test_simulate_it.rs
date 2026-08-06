//! `sutra test simulate` proof against a real PostgreSQL (F5, the P1-7 time-skipping CLI
//! wrapper). Mirrors `pg_migrate_it.rs`'s container fixture; the deployment-package fixture
//! (a `PT24H` intermediate catch timer) adapts `sutra-engine/tests/all/time_skipping_it.rs`'s
//! `long_timer_package` — duplicated rather than shared across crate test binaries, matching
//! that file's own stated convention.
//!
//! (a) proves the headline claim end to end THROUGH THE CLI COMMAND rather than the raw
//! `fast_forward_until` seam: a genuinely pre-existing, durably-parked `PT24H` catch-timer
//! instance (started by a throwaway direct `serve()` boot — `sutra test simulate` itself never
//! POSTs to a channel, so the parked instance is seeded before it is invoked, exactly the
//! `--allow-existing-data` scenario) fires and completes under `simulate --until-quiescent` in
//! real wall-clock seconds, and the JSON summary says so with stable field names.
//! (b) proves the safety refusal: a pre-existing `instance_state` row without
//! `--allow-existing-data` refuses before ever booting an engine.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::{Connection, PgConnection};
use sutra_cli::commands::test::{DatasourceArgs, SimulateArgs};
use sutra_cli::output::Io;
use sutra_cli::{exit, GlobalArgs};
use sutra_engine::{serve, DeploymentSourceKind, EngineConfig};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;

static SEQ: AtomicU32 = AtomicU32::new(0);
static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        std::thread::spawn(|| {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .expect("start postgres:16-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5432).expect("mapped 5432");
            (container, port)
        })
        .join()
        .expect("container bootstrap thread")
    });
    *port
}

fn db_url(db: &str) -> String {
    format!(
        "postgres://postgres:postgres@127.0.0.1:{}/{db}",
        container_port()
    )
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn create_database(name: &str) {
    block_on(async {
        let mut admin = PgConnection::connect(&db_url("postgres"))
            .await
            .expect("admin connect");
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .expect("create database");
    });
}

/// The SAME embedded/shipped migration trees `sutra migrate` and `sutra test simulate` both
/// apply — used here only to pre-migrate a fresh database directly (the seed engine and the
/// safety-refusal test both need the schema before touching `instance_state`, and `test
/// simulate` migrates idempotently on its own run anyway).
fn migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    vec![
        repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
        repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
        repo.join("rust/crates/sutra-persistence/migrations/shipped/deploy"),
        repo.join("rust/crates/sutra-persistence/migrations/native"),
    ]
}

fn migrate_db(url: &str) {
    block_on(async {
        let mut conn = PgConnection::connect(url).await.expect("connect");
        let roots = migration_roots();
        let refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
        let scripts = collect_migrations(&refs).expect("collect migrations");
        apply_migrations(&mut conn, &scripts)
            .await
            .expect("apply migrations");
    });
}

// ---- fixture package (adapts time_skipping_it.rs's long_timer_package, minus the outbound
// send task — this suite verifies completion through `test simulate`'s own JSON summary, not
// an independent capture sink) --------------------------------------------------------------

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "cli-tskip-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Start (HTTP, `POST /channels/{marker}-start`) -> intermediate catch timer (`PT24H`) -> End.
fn long_timer_package(marker: &str) -> PathBuf {
    let root = temp_root("pkg-src");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{marker}"
                  targetNamespace="urn:sutra:module:{marker}:1.0.0">
  <bpmn:process id="{marker}" name="Long timer" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="{marker}-start"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Wait"/>
    <bpmn:intermediateCatchEvent id="Wait" name="Hold until due">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT24H</bpmn:timeDuration></bpmn:timerEventDefinition>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        r#"channels:
  - name: {marker}-start
    transport: http
    bind: "POST /channels/{marker}-start"
    auth:
      scheme: apikey
      apikey:
        value: time-skip-key
        header: X-Api-Key
"#
    );
    write(&root, "bpmn/flow.bpmn", &bpmn);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        &format!(
            "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"{marker}\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n"
        ),
    );
    root
}

fn package(package_dir: &Path) -> Vec<u8> {
    let out = temp_root("pkg");
    let outcome =
        sutra_loader::assemble_dir(package_dir, &out, &sutra_loader::PackageOptions::default())
            .expect("fixture package seals");
    assert_eq!(outcome.archives.len(), 1, "one package = one archive");
    std::fs::read(&outcome.archives[0].file_path).expect("archive bytes")
}

fn place_archive(dir: &Path, name: &str, bytes: &[u8]) {
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).expect("write temp archive");
    std::fs::rename(&tmp, dir.join(name)).expect("rename into place");
}

fn empty_deployments_dir() -> PathBuf {
    temp_root("empty-deployments")
}

// ---- tiny blocking HTTP client (mirrors time_skipping_it.rs) -------------------------------

fn http_post(addr: SocketAddr, path: &str, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(addr).expect("connect");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: time-skip-key\r\n\
         Content-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("request head");
    stream.write_all(body).expect("request body");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code")
}

// ---- seed the pre-existing parked instance --------------------------------------------------

/// Boots a THROWAWAY engine directly (no CLI, real wall clock — it never runs long enough to
/// matter), posts to the start channel so the fixture parks at its `PT24H` catch, then shuts
/// down. `sutra test simulate` itself never POSTs to a channel — the scenario this proves
/// (fast-forwarding an ALREADY-DURABLY-PARKED timer) is exactly the `--allow-existing-data`
/// case.
fn seed_one_parked_instance(deployments_dir: &Path, url: &str, marker: &str) {
    block_on(async {
        let engine = serve(EngineConfig {
            deployment_source: DeploymentSourceKind::Dir,
            deployments_dir: Some(deployments_dir.to_path_buf()),
            deployments_poll_interval: std::time::Duration::from_millis(200),
            http_port: 0,
            datasource_url: Some(url.to_owned()),
            rls_bypass_check_enabled: false,
            ..EngineConfig::default()
        })
        .await
        .expect("seed engine boots");
        let status = tokio::task::spawn_blocking({
            let addr = engine.local_addr;
            let path = format!("/channels/{marker}-start");
            move || http_post(addr, &path, b"go")
        })
        .await
        .expect("post task");
        assert_eq!(status, 200, "seed park accepted");
        engine.drain().await;
    });
}

// ---- run the CLI command under test ---------------------------------------------------------

fn run_simulate(args: SimulateArgs) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        sutra_cli::commands::test::execute(
            sutra_cli::commands::test::TestArgs {
                action: sutra_cli::commands::test::TestAction::Simulate(args),
            },
            &GlobalArgs::default(),
            &mut io,
        )
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn datasource(url: &str) -> DatasourceArgs {
    DatasourceArgs {
        url: Some(url.to_owned()),
        username: None,
        password: None,
    }
}

#[ignore = "docker"]
#[test]
fn until_quiescent_fires_a_pt24h_catch_timer_and_completes_the_instance_in_real_seconds() {
    // `SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED=false`: the fixture role (testcontainers
    // postgres superuser) has BYPASSRLS; `test simulate`'s own boot reads this env var the
    // same way `EngineConfig::load` does (see the command's module docs).
    std::env::set_var("SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED", "false");

    let db = "sutra_cli_tskip_main";
    create_database(db);
    let url = db_url(db);
    migrate_db(&url);

    let marker = "long";
    let deployments_dir = empty_deployments_dir();
    let archive = package(&long_timer_package(marker));
    place_archive(&deployments_dir, "long.sutra", &archive);

    seed_one_parked_instance(&deployments_dir, &url, marker);

    let (code, out, err) = run_simulate(SimulateArgs {
        deployments: deployments_dir,
        datasource: datasource(&url),
        advance: None,
        until_quiescent: true,
        timeout: Some("PT20S".to_owned()),
        start: None,
        allow_existing_data: true,
    });
    std::env::remove_var("SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED");

    assert_eq!(code, exit::OK, "stderr:\n{err}\nstdout:\n{out}");
    let summary: serde_json::Value = serde_json::from_str(out.trim())
        .unwrap_or_else(|e| panic!("stdout is not one JSON object: {e}\n{out}"));

    assert_eq!(summary["mode"], "until-quiescent");
    assert_eq!(summary["timedOut"], false);
    assert_eq!(summary["quiescent"], true);
    assert_eq!(summary["preExistingInstances"], 1);
    assert!(
        summary["timersFired"].as_i64().unwrap_or(0) >= 1,
        "the PT24H catch timer must fire: {summary}"
    );
    assert!(
        summary["instancesCompleted"].as_i64().unwrap_or(0) >= 1,
        "the parked instance must complete: {summary}"
    );
    assert_eq!(summary["instancesLive"], 0);
    let virtual_seconds = summary["virtualSecondsAdvanced"].as_f64().unwrap();
    assert!(
        virtual_seconds >= 23.0 * 3600.0,
        "the virtual clock must actually have advanced ~24h: {summary}"
    );
    let wall_seconds = summary["wallSeconds"].as_f64().unwrap();
    assert!(
        wall_seconds < 15.0,
        "a PT24H timer must settle in real wall-clock SECONDS under fast-forward: {summary}"
    );
}

#[ignore = "docker"]
#[test]
fn a_pre_existing_instance_row_refuses_the_run_without_allow_existing_data() {
    let db = "sutra_cli_tskip_refusal";
    create_database(db);
    let url = db_url(db);
    migrate_db(&url);

    block_on(async {
        let mut conn = PgConnection::connect(&url).await.expect("connect");
        sqlx::query(
            "INSERT INTO instance_state (deployment_id, instance_id, serialised) \
             VALUES ('dummy-dep', $1, $2)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(b"dummy".to_vec())
        .execute(&mut conn)
        .await
        .expect("seed a pre-existing instance_state row");
    });

    let (code, _, err) = run_simulate(SimulateArgs {
        deployments: empty_deployments_dir(),
        datasource: datasource(&url),
        advance: Some("PT1H".to_owned()),
        until_quiescent: false,
        timeout: None,
        start: None,
        allow_existing_data: false,
    });

    assert_eq!(code, exit::USAGE, "stderr:\n{err}");
    assert!(err.contains("already has 1 instance_state row"), "{err}");
    assert!(err.contains("--allow-existing-data"), "{err}");
}
