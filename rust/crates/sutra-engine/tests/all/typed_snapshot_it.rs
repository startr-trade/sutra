//! Typed variable survival across a wait state (P1-3), at the BRIDGE seam against real Postgres.
//!
//! What this closes that no other suite does: the unit tests either side prove the codec encodes
//! types and the mapping seam converts them, but the thing the feature is actually about is a full
//! PARK → durable row → LOAD cycle. That path crosses four layers and a `bytea` column, and it is
//! the one place a type can silently become a string again.
//!
//! Each test therefore ends on the question the feature exists to answer: does the FEEL expression
//! a gateway would re-evaluate after the resume get the RIGHT answer? Before typing, `amount > 100`
//! over a restored instance compared a string to a number and FEEL — which never coerces — returned
//! null, so the gateway took the false branch on a payment that plainly exceeded the limit.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use sutra_channels::bridge::{InstanceBridge, SuspendedInstance, TimerWaitRecord};
use sutra_crypto::{HkdfKeyProvider, KeyProvider};
use sutra_engine::bridge::PersistenceBridge;
use sutra_feel::{FeelContext, FeelValue};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use uuid::Uuid;

const DEP: &str = "dep-000000000000000000000078";

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
    let db = format!("typed_snapshot_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
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

/// A parked instance the way the dispatcher's park arm submits one, carrying a value of every
/// kind a snapshot can hold.
fn parked(variables: Vec<(String, FeelValue)>) -> SuspendedInstance {
    SuspendedInstance {
        process_id: "pay".to_string(),
        deployment_id: DEP.to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        completed_nodes: vec!["S".to_string()],
        variables,
        waiting_nodes: vec!["W".to_string()],
        start_node: "S".to_string(),
        audit_seq: 3,
        ..Default::default()
    }
}

/// Park through the bridge and load back — the whole round trip under test.
async fn park_and_load(
    bridge: &PersistenceBridge,
    snapshot: &SuspendedInstance,
) -> SuspendedInstance {
    let id = Uuid::new_v4().to_string();
    bridge
        .commit_park(
            &deployment(),
            &id,
            snapshot,
            &[],
            &[] as &[TimerWaitRecord],
            &[],
        )
        .await
        .expect("park commits");
    InstanceBridge::load(bridge, &deployment(), &id)
        .await
        .expect("load")
        .expect("the parked row is there")
}

fn context(loaded: &SuspendedInstance) -> FeelContext {
    loaded.variables.iter().cloned().collect()
}

fn eval(expression: &str, ctx: &FeelContext) -> FeelValue {
    sutra_feel::expressions::eval(expression, ctx).expect("expression evaluates")
}

#[ignore = "docker"]
#[test]
fn a_gateway_condition_over_restored_variables_gets_the_right_answer() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool);

    let loaded = rt.block_on(park_and_load(
        &bridge,
        &parked(vec![
            ("amount".to_string(), FeelValue::num("1250.75")),
            ("approved".to_string(), FeelValue::Boolean(false)),
            ("currency".to_string(), FeelValue::from("EUR")),
        ]),
    ));
    let ctx = context(&loaded);

    // The regression this feature exists for: a stringly `amount` made this null ⇒ the gateway
    // took the false branch on a payment that plainly exceeds the limit.
    assert_eq!(eval("amount > 100", &ctx), FeelValue::Boolean(true));
    assert_eq!(eval("amount * 2", &ctx), FeelValue::num("2501.50"));
    // A boolean is a boolean, not the non-empty (hence never-false) string "false".
    assert_eq!(eval("not(approved)", &ctx), FeelValue::Boolean(true));
    // …and a string is still a string.
    assert_eq!(eval("currency = \"EUR\"", &ctx), FeelValue::Boolean(true));
}

#[ignore = "docker"]
#[test]
fn every_value_kind_survives_the_park_load_cycle() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool);

    let due = sutra_feel::temporal::parse_at_literal("2026-08-05").unwrap();
    let deadline = sutra_feel::temporal::parse_at_literal("2026-08-05T13:45:00@Europe/Paris")
        .expect("zoned instant");
    let sla = sutra_feel::temporal::parse_at_literal("P2DT4H").unwrap();
    let lines = FeelValue::List(vec![
        FeelValue::num("1"),
        FeelValue::Map(
            [
                ("sku".to_string(), FeelValue::from("A-1")),
                ("qty".to_string(), FeelValue::num("3")),
            ]
            .into_iter()
            .collect(),
        ),
    ]);
    let original = vec![
        ("amount".to_string(), FeelValue::num("1250.75")),
        ("approved".to_string(), FeelValue::Boolean(false)),
        ("currency".to_string(), FeelValue::from("EUR")),
        ("cancelledAt".to_string(), FeelValue::Null),
        ("due".to_string(), due),
        ("deadline".to_string(), deadline),
        ("sla".to_string(), sla),
        ("lines".to_string(), lines),
    ];

    let loaded = rt.block_on(park_and_load(&bridge, &parked(original.clone())));
    // The store is name-keyed, so a load comes back sorted rather than in submission order.
    let mut expected = original.clone();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(loaded.variables, expected);

    // Structure survives as structure, not as the text `[1, {sku=A-1, qty=3}]`.
    let ctx = context(&loaded);
    assert_eq!(eval("count(lines)", &ctx), FeelValue::num("2"));
    assert_eq!(eval("lines[2].qty", &ctx), FeelValue::num("3"));
    // A null variable is null — before typing it parked as the empty string, which is a value.
    assert_eq!(eval("cancelledAt = null", &ctx), FeelValue::Boolean(true));
    // The temporal family keeps enough of itself to do temporal arithmetic after the wait.
    assert_eq!(
        eval("due + duration(\"P1D\")", &ctx),
        sutra_feel::temporal::parse_at_literal("2026-08-06").unwrap()
    );
}

#[ignore = "docker"]
#[test]
fn an_encrypted_variable_restores_typed_from_inside_its_envelope() {
    // The at-rest decision this pins: the type tag rides INSIDE the ciphertext, so a sensitive
    // number is a number again after the resume, while the raw value never touches the disk.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(fresh_pool());
    let provider: Arc<dyn KeyProvider + Send + Sync> =
        Arc::new(HkdfKeyProvider::new(b"typed-snapshot-it"));
    let bridge = PersistenceBridge::with_key_provider(pool.clone(), Some(provider));

    let mut snapshot = parked(vec![
        ("salary".to_string(), FeelValue::num("125000.50")),
        ("cleared".to_string(), FeelValue::Boolean(false)),
    ]);
    snapshot.key_id = "tenant-a".to_string();
    snapshot.sensitive = vec!["salary".to_string()];
    snapshot.encrypt_names = vec!["salary".to_string()];

    let id = Uuid::new_v4().to_string();
    rt.block_on(bridge.commit_park(
        &deployment(),
        &id,
        &snapshot,
        &[],
        &[] as &[TimerWaitRecord],
        &[],
    ))
    .expect("park commits");

    let stored: Vec<u8> = rt
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
    let text = String::from_utf8_lossy(&stored);
    assert!(text.contains("sutra.snapshot=4"), "{text}");
    assert!(text.contains("sutra.enc.salary="));
    assert!(
        !text.contains("125000"),
        "raw sensitive value at rest: {text}"
    );

    let loaded = rt
        .block_on(InstanceBridge::load(&bridge, &deployment(), &id))
        .expect("load")
        .expect("row");
    assert_eq!(
        loaded.variables,
        vec![
            ("cleared".to_string(), FeelValue::Boolean(false)),
            ("salary".to_string(), FeelValue::num("125000.50")),
        ]
    );
    let ctx = context(&loaded);
    assert_eq!(eval("salary > 100000", &ctx), FeelValue::Boolean(true));
}

#[ignore = "docker"]
#[test]
fn an_all_string_instance_still_parks_as_the_pre_typing_bytes() {
    // Compatibility in the direction that actually ships: an instance whose variables happen to
    // be strings must produce the bytes it always produced, so a fleet mid-upgrade writes rows
    // an older replica can still read.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(fresh_pool());
    let bridge = PersistenceBridge::new(pool.clone());

    let id = Uuid::new_v4().to_string();
    rt.block_on(bridge.commit_park(
        &deployment(),
        &id,
        &parked(vec![("inboundId".to_string(), FeelValue::from("INB-7"))]),
        &[],
        &[] as &[TimerWaitRecord],
        &[],
    ))
    .expect("park commits");

    let stored: Vec<u8> = rt
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
    let text = String::from_utf8(stored).unwrap();
    assert!(text.contains("sutra.snapshot=2"), "{text}");
    assert!(text.contains("sutra.var.inboundId=INB-7"), "{text}");
}
