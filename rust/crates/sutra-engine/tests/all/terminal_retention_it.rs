//! Terminal-instance retention (P1-2) at the BRIDGE seam, against real Postgres.
//!
//! What this closes that no other suite does: `InstanceBridge::commit_complete` is the terminal
//! transaction, and its whole contract is that everything in it lands together or not at all. The
//! store-level pieces (the `terminal_at` marker, the purge predicate, the `count_active`
//! exclusion) are proved in `sutra-persistence`'s pg suite; the endpoint projections are proved
//! in-crate. This file proves the COMMIT SHAPE: one call, one transaction, and afterwards the
//! instance row is retained-and-re-stamped, its waits are resolved, its aliases are retired, its
//! emissions are enqueued, and its ownership claim is gone — all four, from the one call.
//!
//! It also pins the two configurations of that shape against each other: the default (retain) and
//! `sutra.instance.retention=PT0S` (delete), which must reproduce the pre-P1-2 behaviour exactly.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use sutra_channels::bridge::{AliasRecord, InstanceBridge, OutboxEmission, SuspendedInstance};
use sutra_channels::bridge::{TimerWaitRecord, INSTANCE_STATUS_COMPLETED};
use sutra_engine::bridge::PersistenceBridge;
use sutra_feel::FeelValue;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::stores::{InstanceStore, PgInstanceStore};
use sutra_persistence::value::SnapshotValue;
use uuid::Uuid;

const DEP: &str = "dep-000000000000000000000077";

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

async fn fresh_pool() -> PgPool {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    let db = format!("retention_bridge_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    drop(admin);

    {
        use sqlx::ConnectOptions;
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root")
            .to_path_buf();
        let roots = [repo.join("rust/crates/sutra-persistence/migrations/shipped/core")];
        let root_refs: Vec<&Path> = roots.iter().map(PathBuf::as_path).collect();
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
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/{db}"
        ))
        .await
        .expect("pool")
}

fn deployment() -> sutra_executor::DeploymentId {
    sutra_executor::DeploymentId::of(DEP).expect("deployment id")
}

fn persist_dep() -> sutra_persistence::DeploymentId {
    sutra_persistence::DeploymentId::new(DEP).expect("persistence deployment id")
}

/// A parked instance the way the dispatcher's park arm would submit it: one wait node, one live
/// unique alias, one variable.
fn suspended() -> SuspendedInstance {
    SuspendedInstance {
        process_id: "pay".to_string(),
        deployment_id: DEP.to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        // A NUMBER, not the text "42" — the retained snapshot must record it as one.
        variables: vec![("amount".to_string(), FeelValue::num("42"))],
        sensitive: Vec::new(),
        waiting_nodes: vec!["W".to_string()],
        start_node: "S".to_string(),
        coverage: Default::default(),
        audit_seq: 3,
        key_id: String::new(),
        encrypt_names: Vec::new(),
        subjects: Vec::new(),
        retry_attempts: Default::default(),
        retry_backoff: Default::default(),
    }
}

fn emission(instance_id: &str) -> OutboxEmission {
    OutboxEmission {
        instance_id: instance_id.to_string(),
        node_id: "Reply".to_string(),
        destination: "http://example.invalid/ack".to_string(),
        body: sutra_executor::Sensitive::new(b"{}".to_vec()),
        content_type: Some("application/json".to_string()),
        required: false,
        mode: sutra_bpmn::qbindings::ReplyMode::Native,
        outbox_key: format!("k-{instance_id}"),
        cloud_event_json: None,
        auth_ref_json: None,
        labels: Default::default(),
        traceparent: None,
        headers: Default::default(),
    }
}

/// Park an instance through the bridge and hand back its id (so the terminal call under test runs
/// against a row the ORDINARY path produced, not one a test hand-wrote).
async fn park(bridge: &PersistenceBridge) -> String {
    let id = Uuid::new_v4().to_string();
    bridge
        .commit_park(
            &deployment(),
            &id,
            &suspended(),
            &[AliasRecord {
                name: "ref".to_string(),
                value: format!("R-{id}"),
                unique: true,
            }],
            &[] as &[TimerWaitRecord],
            &[],
        )
        .await
        .expect("park commits");
    id
}

struct Row {
    status: Option<String>,
    terminal_at: Option<time::OffsetDateTime>,
    claim_owner: Option<String>,
}

async fn read_row(pool: &PgPool, id: Uuid) -> Option<Row> {
    let row: Option<(Vec<u8>, Option<time::OffsetDateTime>, Option<String>)> = sqlx::query_as(
        "SELECT serialised, terminal_at, claim_owner FROM instance_state \
         WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(DEP)
    .bind(id)
    .fetch_optional(pool)
    .await
    .unwrap();
    row.map(|(bytes, terminal_at, claim_owner)| Row {
        status: InstanceSnapshot::peek(&bytes).ok().map(|k| k.status),
        terminal_at,
        claim_owner,
    })
}

async fn count(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .bind(DEP)
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The whole terminal shape, from ONE `commit_complete`: the row is retained + re-stamped
/// COMPLETED + stamped `terminal_at` + un-claimed, the waits are resolved, the aliases are
/// retired, and the emissions are enqueued.
#[ignore = "docker"]
#[test]
fn commit_complete_retains_the_instance_and_finishes_every_other_terminal_effect_at_once() {
    // A plain #[test] with an owned runtime: the bridge is SYNC and `block_on`s internally, so it
    // must be driven from a non-worker thread — exactly as the ChannelEngine actor thread does.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    assert_eq!(
        bridge.retention(),
        sutra_engine::bridge::DEFAULT_INSTANCE_RETENTION,
        "the default posture is retain-for-a-week"
    );

    let id = rt.block_on(park(&bridge));
    let uuid = Uuid::parse_str(&id).unwrap();
    // The resume path holds a claim when the terminal step runs — reproduce that.
    assert!(rt
        .block_on(PgInstanceStore::new(pool.clone()).claim(
            &persist_dep(),
            uuid,
            bridge.claim_owner()
        ))
        .unwrap());

    // Preconditions: parked, waiting, alias live, nothing in the outbox.
    let before = rt.block_on(read_row(&pool, uuid)).expect("parked row");
    assert_eq!(before.status.as_deref(), Some("SUSPENDED"));
    assert!(before.terminal_at.is_none(), "a parked row is not terminal");
    assert_eq!(before.claim_owner.as_deref(), Some(bridge.claim_owner()));

    rt.block_on(bridge.commit_complete(&deployment(), &id, &[emission(&id)]))
        .expect("terminal step commits");

    let after = rt
        .block_on(read_row(&pool, uuid))
        .expect("the row is RETAINED — this is the P1-2 change");
    assert_eq!(
        after.status.as_deref(),
        Some(INSTANCE_STATUS_COMPLETED),
        "the stored snapshot is re-stamped COMPLETED in the terminal transaction"
    );
    assert!(
        after.terminal_at.is_some(),
        "terminal_at is what drives the purge — it must be stamped by the same write"
    );
    assert!(
        after.claim_owner.is_none(),
        "a terminal row is owned by nobody; leaving the claim would give the stuck-instance \
         sweeper work to do on a corpse"
    );

    // The pre-existing terminal effects are unchanged — retention added a write, it did not
    // displace any of them.
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM waiting_event WHERE deployment_id = $1 AND instance_id = $2 \
             AND status = 'WAITING'",
            uuid
        )),
        0,
        "every wait row is resolved (nothing may re-fire against a finished instance)"
    );
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM alias_index WHERE deployment_id = $1 AND instance_id = $2 \
             AND live = TRUE",
            uuid
        )),
        0,
        "live aliases are retired, so the business key is free for a fresh instance"
    );
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = $1 AND instance_id = $2",
            uuid
        )),
        1,
        "the terminal step's emissions are enqueued in the same transaction"
    );

    // And the instance stops being ACTIVE — the deploy quiescence gate must not wait on it.
    assert_eq!(
        rt.block_on(PgInstanceStore::new(pool).count_active(&persist_dep()))
            .unwrap(),
        0
    );
}

/// The variables and the frontier survive the re-stamp: the retained record is the last state the
/// engine durably knew, not an empty husk.
#[ignore = "docker"]
#[test]
fn the_retained_snapshot_keeps_the_state_the_engine_last_knew() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    let id = rt.block_on(park(&bridge));
    rt.block_on(bridge.commit_complete(&deployment(), &id, &[]))
        .expect("terminal step commits");

    let bytes: Vec<u8> = rt
        .block_on(
            sqlx::query_scalar(
                "SELECT serialised FROM instance_state WHERE deployment_id = $1 AND \
                 instance_id = $2",
            )
            .bind(DEP)
            .bind(Uuid::parse_str(&id).unwrap())
            .fetch_one(&pool),
        )
        .unwrap();
    let snapshot = InstanceSnapshot::read(&bytes).unwrap();
    assert_eq!(snapshot.status(), INSTANCE_STATUS_COMPLETED);
    assert_eq!(
        snapshot.variables()["amount"],
        SnapshotValue::Number("42".parse().unwrap()),
        "the terminal re-stamp keeps the variable's TYPE, not just its text"
    );
    assert_eq!(snapshot.completed_nodes(), ["S"]);
    assert_eq!(snapshot.audit_seq(), 3);
    assert_eq!(
        snapshot.waiting_nodes(),
        ["W"],
        "the frontier it was last parked at is the record of WHERE it finished"
    );
}

/// `PT0S` is the explicit opt-out and must reproduce the pre-P1-2 behaviour EXACTLY: the row is
/// deleted in the terminal transaction, not retained-then-swept.
#[ignore = "docker"]
#[test]
fn zero_retention_deletes_the_row_in_the_terminal_transaction() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone()).with_retention(std::time::Duration::ZERO);
    let id = rt.block_on(park(&bridge));
    let uuid = Uuid::parse_str(&id).unwrap();

    rt.block_on(bridge.commit_complete(&deployment(), &id, &[emission(&id)]))
        .expect("terminal step commits");

    assert!(
        rt.block_on(read_row(&pool, uuid)).is_none(),
        "PT0S must DELETE at the terminal step — an operator who wants no history must not get a \
         window in which it exists"
    );
    // The rest of the terminal step is untouched by the retention choice.
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = $1 AND instance_id = $2",
            uuid
        )),
        1
    );
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM waiting_event WHERE deployment_id = $1 AND instance_id = $2 \
             AND status = 'WAITING'",
            uuid
        )),
        0
    );
}

/// Racing an admin cancel: the row is gone before the terminal step reaches it. The step must
/// still commit its other effects rather than fail the whole quiescent point over a missing row.
#[ignore = "docker"]
#[test]
fn commit_complete_tolerates_a_row_that_vanished_under_it() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());
    let id = rt.block_on(park(&bridge));
    let uuid = Uuid::parse_str(&id).unwrap();

    rt.block_on(PgInstanceStore::new(pool.clone()).delete(&persist_dep(), uuid))
        .expect("simulate a concurrent cancel/erasure");

    rt.block_on(bridge.commit_complete(&deployment(), &id, &[emission(&id)]))
        .expect("a vanished row is a race, not a failure of the terminal step");

    assert!(rt.block_on(read_row(&pool, uuid)).is_none());
    assert_eq!(
        rt.block_on(count(
            &pool,
            "SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = $1 AND instance_id = $2",
            uuid
        )),
        1,
        "the emissions still commit — losing the history record must not lose a send"
    );
}
