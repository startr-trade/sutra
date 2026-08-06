//! The durable dead-letter sink for failed inbound deliveries, end to end against real Postgres.
//!
//! The channels suite already proves the dispatch path: a non-idempotent execution failure →
//! `DispatchOutcome::DeadLettered` → `IncidentSink::record` fires with the right `InboundIncident`
//! (`sutra-channels/tests/all/intake_test.rs`, with an in-memory sink). The gap this closes is the
//! ENGINE-side durable sink: the `PersistenceBridge`'s `impl IncidentSink` — its `deployment` string
//! → `DeploymentId` parse, the RFC-3339 `received_at` parse, and the `block_on` insert — landing a
//! real row in `dead_letter`. It also pins the best-effort contract: a malformed deployment id is
//! dropped (logged, not persisted) and NEVER panics the caller.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use sutra_channels::stores::{InboundIncident, IncidentSink};
use sutra_engine::bridge::PersistenceBridge;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};

static CONTAINER: OnceLock<(
    testcontainers::Container<testcontainers_modules::postgres::Postgres>,
    u16,
)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        std::thread::spawn(|| {
            use testcontainers::runners::SyncRunner;
            use testcontainers::ImageExt;
            let container = testcontainers_modules::postgres::Postgres::default()
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

fn shipped_migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    // shipped/core recurses into core/incident (V1201 dead_letter), so the migrated DB has it.
    vec![repo.join("rust/crates/sutra-persistence/migrations/shipped/core")]
}

async fn fresh_pool() -> PgPool {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    let db = format!("incident_it_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    drop(admin);

    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/{db}");
    {
        use sqlx::ConnectOptions;
        let roots = shipped_migration_roots();
        let root_refs: Vec<&Path> = roots.iter().map(|p| p.as_path()).collect();
        let scripts = collect_migrations(&root_refs).expect("collect migrations");
        let mut conn = sqlx::postgres::PgConnectOptions::new()
            .host("127.0.0.1")
            .port(port)
            .username("postgres")
            .password("postgres")
            .database(&db)
            .connect()
            .await
            .expect("migration connection");
        apply_migrations(&mut conn, &scripts)
            .await
            .expect("apply migrations");
    }
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("pool")
}

fn incident(deployment: &str, channel: &str, code: &str) -> InboundIncident {
    InboundIncident::of_failure(
        deployment,
        channel,
        "orders",
        "",
        code,
        "synthetic task failure",
        "2026-07-27T10:00:00Z",
    )
}

async fn count_all(pool: &PgPool) -> i64 {
    // The pool connects as the postgres superuser (BYPASSRLS), so this raw count sees every row.
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM dead_letter")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[ignore = "docker"]
#[test]
fn persistence_bridge_durably_records_dead_letters_best_effort() {
    // A plain #[test] with an owned runtime: the sink's `record` is sync and does
    // `handle.block_on(insert)`, which must NOT run inside a runtime worker — so we call it from
    // this (non-worker) test thread, exactly as the ChannelEngine actor thread would.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    let dep = "dep-000000000000000000000001";

    // A well-formed incident lands a durable row with its fields intact.
    rt.block_on(IncidentSink::record(
        &bridge,
        incident(dep, "orders-in", "SUTRA.RUNTIME.TASK.UNCAUGHT"),
    ));
    let row = rt.block_on(async {
        sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT deployment_id, channel, process_id, failure_code FROM dead_letter",
        )
        .fetch_one(&pool)
        .await
        .expect("one dead_letter row")
    });
    assert_eq!(row.0, dep);
    assert_eq!(row.1, "orders-in");
    assert_eq!(row.2, "orders");
    assert_eq!(row.3, "SUTRA.RUNTIME.TASK.UNCAUGHT");

    // Best-effort: a malformed deployment id is dropped (logged), never persisted, never panics.
    rt.block_on(IncidentSink::record(
        &bridge,
        incident("not-a-valid-id", "x", "C"),
    ));
    assert_eq!(
        rt.block_on(count_all(&pool)),
        1,
        "the malformed-deployment incident was dropped best-effort — no row, no panic"
    );

    // Raw append (no idempotency key): a second well-formed failure adds a second row.
    rt.block_on(IncidentSink::record(
        &bridge,
        incident(dep, "orders-in", "SUTRA.RUNTIME.TASK.UNCAUGHT"),
    ));
    assert_eq!(
        rt.block_on(count_all(&pool)),
        2,
        "each failure appends a row"
    );
}

// ---- P0-4: the replay capture, and durable FAILED instance state ---------------------------

#[ignore = "docker"]
#[test]
fn a_captured_incident_is_durably_replayable_end_to_end() {
    // The capture half of P0-4 through the REAL engine sink: the dispatcher's `InboundIncident`
    // (payload + headers + routing keys) → `dead_letter` → the admin read/replay projections.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    let dep = sutra_persistence::DeploymentId::new("dep-000000000000000000000001").unwrap();

    let mut headers = std::collections::BTreeMap::new();
    headers.insert("x-corr".to_string(), "corr-9".to_string());
    rt.block_on(IncidentSink::record(
        &bridge,
        incident(dep.as_str(), "orders-in", "SUTRA.RUNTIME.TASK.UNCAUGHT").with_capture(
            "acme",
            "acme/orders/1.0.0",
            Some("application/json".to_string()),
            br#"{"orderId":"A-1"}"#.to_vec(),
            headers,
        ),
    ));

    let store = sutra_persistence::stores::PgDeadLetterStore::new(pool.clone());
    let listed = rt.block_on(store.list(&dep, 10, 0)).expect("list");
    assert_eq!(listed.len(), 1);
    let record = &listed[0];
    assert_eq!(record.payload_bytes, Some(17));
    assert_eq!(record.tenant, "acme");
    assert_eq!(record.module_key, "acme/orders/1.0.0");
    assert_eq!(record.content_type.as_deref(), Some("application/json"));

    let replay = rt
        .block_on(store.replay_payload(&dep, record.id))
        .expect("replay fetch")
        .expect("the row exists");
    assert_eq!(
        replay.payload.as_deref(),
        Some(&br#"{"orderId":"A-1"}"#[..])
    );
    assert_eq!(
        replay.headers.get("x-corr").map(String::as_str),
        Some("corr-9"),
        "headers replay verbatim"
    );
}

#[ignore = "docker"]
#[test]
fn commit_failed_marks_the_instance_and_resolves_its_waits_in_one_step() {
    // The durable-failure half of P0-4 against real Postgres: park an instance, then fail it.
    // Afterwards the row must READ as FAILED (naming its cause) and hold NO waiting rows — the
    // two properties that make it visible to an operator and invisible to the timer poller.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    let exec_dep = sutra_executor::DeploymentId::of("dep-000000000000000000000001").unwrap();
    let persist_dep = sutra_persistence::DeploymentId::new("dep-000000000000000000000001").unwrap();
    let instance_id = "11111111-1111-4111-8111-111111111111";

    let snapshot = sutra_channels::bridge::SuspendedInstance {
        process_id: "hold".to_string(),
        deployment_id: exec_dep.value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        variables: vec![("orderId".to_string(), sutra_feel::FeelValue::from("A-1"))],
        waiting_nodes: vec!["U".to_string()],
        start_node: "S".to_string(),
        ..Default::default()
    };
    rt.block_on(sutra_channels::bridge::InstanceBridge::commit_park(
        &bridge,
        &exec_dep,
        instance_id,
        &snapshot,
        &[],
        &[],
        &[],
    ))
    .expect("park commits");

    rt.block_on(sutra_channels::bridge::InstanceBridge::commit_failed(
        &bridge,
        &exec_dep,
        instance_id,
        "SUTRA.RUNTIME.TASK.UNCAUGHT",
        "the task threw",
    ))
    .expect("failure state commits");

    // The snapshot reads FAILED, keeps the frontier it died at, and names the cause.
    let loaded = rt
        .block_on(sutra_channels::bridge::InstanceBridge::load(
            &bridge,
            &exec_dep,
            instance_id,
        ))
        .expect("load")
        .expect("the row survives — a failed instance is never deleted");
    assert_eq!(loaded.status, "FAILED");
    assert!(!loaded.suspended, "FAILED is not resumable");
    assert_eq!(loaded.waiting_nodes, ["U"]);
    assert_eq!(
        loaded.variables,
        vec![("orderId".to_string(), sutra_feel::FeelValue::from("A-1"))],
        "the variables it died with are preserved"
    );
    let (code, detail): (String, String) = rt.block_on(async {
        let row: (Vec<u8>,) = sqlx::query_as(
            "SELECT serialised FROM instance_state WHERE deployment_id = $1 AND instance_id = $2",
        )
        .bind(persist_dep.as_str())
        .bind(uuid::Uuid::parse_str(instance_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("instance row");
        let snap = sutra_persistence::snapshot::InstanceSnapshot::read(&row.0).unwrap();
        (
            snap.failure_code().to_string(),
            snap.failure_detail().to_string(),
        )
    });
    assert_eq!(code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
    assert_eq!(detail, "the task threw");

    // No WAITING row survives: the timer poller has nothing left to claim for this instance.
    let waiting: i64 = rt.block_on(async {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM waiting_event WHERE deployment_id = $1 AND status = 'WAITING'",
        )
        .bind(persist_dep.as_str())
        .fetch_one(&pool)
        .await
        .unwrap()
    });
    assert_eq!(waiting, 0, "every wait row was resolved in the same step");
}

#[ignore = "docker"]
#[test]
fn marking_a_vanished_instance_failed_is_a_no_op_not_an_error() {
    // Racing a cancel/complete: there is nothing to mark, and the failure path must never turn
    // that into an error the resume path would surface instead of the real cause.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool);
    let exec_dep = sutra_executor::DeploymentId::of("dep-000000000000000000000001").unwrap();

    rt.block_on(sutra_channels::bridge::InstanceBridge::commit_failed(
        &bridge,
        &exec_dep,
        "22222222-2222-4222-8222-222222222222",
        "SUTRA.RUNTIME.TASK.UNCAUGHT",
        "the task threw",
    ))
    .expect("a vanished instance is a no-op, not an error");
}
