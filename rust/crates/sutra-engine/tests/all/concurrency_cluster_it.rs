//! Cluster-correctness + crash-safety of the PERSISTED concurrency gauges.
//! Both gauges the dispatcher/quota-enforcer consult are the persisted,
//! replica-coherent source of truth — never an in-memory counter (which would reset on a pod
//! crash and multiply a cluster-wide cap by the replica count):
//!
//!  - the per-channel cap reads `channel_instance` COUNT(*) (V701/V702) via
//!    [`PersistedChannelConcurrency`];
//!  - the tenant concurrent quota reads `instance_state` COUNT(*) per deployment via
//!    [`PersistedActiveInstanceCount`], behind the real `DefaultTenantQuotaEnforcer`.
//!
//! Two engine instances are modelled as two gauges over ONE shared PostgreSQL; a crash/rebuild
//! is modelled as a fresh pool/connection to the same database. The gauges drive their
//! `block_on` from the (non-async) test thread through a runtime handle, exactly as the engine
//! actor thread does.
//!
//! Hermetic postgres:16-alpine via testcontainers, migrated with the shipped SQL.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::runtime::Runtime;
use uuid::Uuid;

use sutra_channels::{
    ConcurrencyStore, DefaultTenantQuotaEnforcer, QuotaCheckResult, StaticTenantConfigSource,
    TenantConfig, TenantQuotaEnforcer, TenantQuotas,
};
use sutra_engine::concurrency::{PersistedActiveInstanceCount, PersistedChannelConcurrency};
use sutra_executor::DeploymentId;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::stores::{
    InstanceState, InstanceStore, PgChannelConcurrencyStore, PgInstanceStore,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;

// ---- fixture (the sutra-persistence pg-suite pattern) --------------------------------------

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

fn migration_roots() -> Vec<PathBuf> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    vec![repo.join("rust/crates/sutra-persistence/migrations/shipped/core")]
}

/// Create ONE fresh database (shared by the two "engine" pools + the crash-rebuild pool) and
/// apply the shipped schema. Returns the database name.
async fn migrated_db() -> String {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    let db = format!(
        "concurrency_cluster_{}",
        DB_SEQ.fetch_add(1, Ordering::SeqCst)
    );
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    drop(admin);

    use sqlx::ConnectOptions;
    let options = sqlx::postgres::PgConnectOptions::new()
        .host("127.0.0.1")
        .port(port)
        .username("postgres")
        .password("postgres")
        .database(&db);
    let roots = migration_roots();
    let root_refs: Vec<&std::path::Path> = roots.iter().map(|p| p.as_path()).collect();
    let scripts = collect_migrations(&root_refs).expect("collect migrations");
    let mut conn = options.connect().await.expect("migration connection");
    apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    db
}

async fn pool_to(db: &str) -> PgPool {
    let port = container_port();
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/{db}"
        ))
        .await
        .expect("pool")
}

fn dep() -> DeploymentId {
    // `dep-<24 hex>` — the persistence-valid form the adapters convert to.
    DeploymentId::of("dep-000000000000000000000051").expect("valid deployment id")
}

// ---- the per-channel concurrency cap: replica-coherent + crash-safe ------------------------

#[test]
#[ignore = "docker"]
fn channel_concurrency_cap_is_replica_coherent_and_crash_safe() {
    let rt = Runtime::new().expect("runtime");
    let db = rt.block_on(migrated_db());

    let d = dep();
    let channel = "voip-in";
    let instance = Uuid::new_v4().to_string();

    // Engine A's gauge parks an instance on the channel (RUNNING → WAITING) — exactly the two
    // calls dispatch.rs makes at commit_park.
    let pool_a = rt.block_on(pool_to(&db));
    let gauge_a = PersistedChannelConcurrency::new(PgChannelConcurrencyStore::new(pool_a));
    rt.block_on(gauge_a.record_started(&d, &instance, channel));
    rt.block_on(gauge_a.record_suspended(&d, &instance));

    // Engine B — a SEPARATE gauge over the SAME PostgreSQL — sees the parked instance in its
    // admission count. `useOnlyInFlightForConcurrencyCap = false` (a held call keeps its line)
    // counts the WAITING instance, so at cap=1 engine B's next inbound is rejected.
    let pool_b = rt.block_on(pool_to(&db));
    let gauge_b = PersistedChannelConcurrency::new(PgChannelConcurrencyStore::new(pool_b));
    assert_eq!(
        rt.block_on(gauge_b.count_active(&d, channel, true)),
        1,
        "engine B must see engine A's parked instance (replica-coherent)"
    );
    // The default (`useOnlyInFlight = true`, RUNNING only) does NOT count a parked instance.
    assert_eq!(rt.block_on(gauge_b.count_active(&d, channel, false)), 0);
    // The admission decision the dispatcher makes at cap=1:
    let cap: u64 = 1;
    assert!(
        rt.block_on(gauge_b.count_active(&d, channel, true)) >= cap,
        "cap=1 with one parked instance ⇒ engine B rejects the next submission"
    );

    // Crash-safety: engines A and B "die" (pools/gauges dropped); a fresh engine C reconnects.
    drop(gauge_a);
    drop(gauge_b);
    let pool_c = rt.block_on(pool_to(&db));
    let gauge_c = PersistedChannelConcurrency::new(PgChannelConcurrencyStore::new(pool_c));
    assert_eq!(
        rt.block_on(gauge_c.count_active(&d, channel, true)),
        1,
        "the parked row survives a crash/rebuild — the count is not in process memory"
    );

    // Terminal frees the slot.
    rt.block_on(gauge_c.record_terminal(&d, &instance));
    assert_eq!(rt.block_on(gauge_c.count_active(&d, channel, true)), 0);
}

// ---- the tenant concurrent quota: replica-coherent + crash-safe ----------------------------

fn quota_enforcer(count: PersistedActiveInstanceCount) -> DefaultTenantQuotaEnforcer {
    DefaultTenantQuotaEnforcer::new(
        Box::new(StaticTenantConfigSource::new(vec![TenantConfig {
            tenant: "acme".to_string(),
            quotas: Some(TenantQuotas {
                max_concurrent_instances: Some(1),
                max_inbound_rate_per_minute: None,
            }),
        }])),
        std::rc::Rc::new(count),
    )
}

#[test]
#[ignore = "docker"]
fn tenant_concurrent_quota_is_replica_coherent_and_crash_safe() {
    let rt = Runtime::new().expect("runtime");
    let db = rt.block_on(migrated_db());

    let d = dep();
    let pdep = PersistDeploymentId::new(d.value()).expect("persistence dep");

    // Engine A parks an instance → a live instance_state row for the deployment.
    let pool_a = rt.block_on(pool_to(&db));
    let instance_store_a = PgInstanceStore::new(pool_a);
    rt.block_on(instance_store_a.persist(
        &pdep,
        &InstanceState {
            instance_id: Uuid::new_v4(),
            serialised: vec![1, 2, 3],
        },
    ))
    .expect("persist a parked instance");

    // Engine B's quota enforcer (concurrent quota = 1) over the persisted per-deployment count.
    let pool_b = rt.block_on(pool_to(&db));
    let enforcer_b = quota_enforcer(PersistedActiveInstanceCount::new(PgInstanceStore::new(
        pool_b,
    )));
    match rt.block_on(enforcer_b.check_inbound("acme", &d, "voip-in")) {
        QuotaCheckResult::Denied { reason, .. } => {
            assert_eq!(reason, "SUTRA.INBOUND.QUOTA_EXCEEDED_CONCURRENT")
        }
        QuotaCheckResult::Allowed => {
            panic!("engine B must quota-reject: 1 in-flight instance on engine A ⇒ at quota=1")
        }
    }

    // Crash-safety: a fresh engine C reconnects to the same DB — the count still reflects the
    // durable instance row, so the quota still denies.
    let pool_c = rt.block_on(pool_to(&db));
    let enforcer_c = quota_enforcer(PersistedActiveInstanceCount::new(PgInstanceStore::new(
        pool_c,
    )));
    assert!(
        matches!(
            rt.block_on(enforcer_c.check_inbound("acme", &d, "voip-in")),
            QuotaCheckResult::Denied { .. }
        ),
        "the per-deployment instance count survives a crash/rebuild"
    );
}
