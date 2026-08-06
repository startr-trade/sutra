//! Broker leadership integration suite — `DbLeaderElection` semantics against the REAL
//! PostgreSQL lease store (durable leases over the shipped migration SQL), plus the
//! gate-load-bearing SINGLETON proof: two `RabbitMqTriggerSource` replicas sharing one
//! lease role against one queue ⇒ exactly one active consumer, with leadership handover
//! when the leader releases.
//!
//! Requires a Docker daemon (postgres:16-alpine + rabbitmq:3.13-management-alpine).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use sutra_channels::source::{AckDecision, InboundIntake, LeaderGate, TriggerSource};
use sutra_channels::{BoxFuture, InboundMessage};
use sutra_engine::leadership::{channel_role, DbLeaderElection, LeaseHandle, PgLeaseHandle};
use sutra_persistence::stores::PgLeaseStore;
use sutra_transport_rabbitmq::{
    AckMode, RabbitMqChannelProperties, RabbitMqSourceConfig, RabbitMqTriggerSource,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// ---- postgres fixture (mirrors sutra-persistence tests/pg/fixture.rs) -----------------------

static PG: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn pg_port() -> u16 {
    let (_, port) = PG.get_or_init(|| {
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
        .expect("postgres bootstrap thread")
    });
    *port
}

/// Fresh database migrated with THE SAME shipped migration SQL trees (includes the
/// `lease` table, V501).
async fn fresh_pool() -> PgPool {
    let port = pg_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin connect");
    let db = format!(
        "leadership_lease_{}",
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create test database");
    admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/{db}"
        ))
        .await
        .expect("test db connect");

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
    let roots = [
        repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
        repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
    ];
    let refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts =
        sutra_persistence::migrate::collect_migrations(&refs).expect("collect migrations");
    let mut conn = pool.acquire().await.expect("acquire for migration");
    sutra_persistence::migrate::apply_migrations(&mut conn, &scripts)
        .await
        .expect("apply migrations");
    drop(conn);
    pool
}

fn lease_handle(pool: &PgPool) -> Arc<dyn LeaseHandle> {
    Arc::new(PgLeaseHandle(PgLeaseStore::new(pool.clone())))
}

/// Slow-poll elector — tests drive timing via `poll_now`.
fn manual_elector(pool: &PgPool, identity: &str) -> DbLeaderElection {
    DbLeaderElection::new(
        lease_handle(pool),
        Some(identity.to_string()),
        Duration::from_secs(600),
        Duration::from_secs(300),
        tokio::runtime::Handle::current(),
    )
    .expect("valid timings")
}

async fn expire_lease(pool: &PgPool, role: &str) {
    sqlx::query("UPDATE lease SET expires_at = now() - interval '1 second' WHERE name = $1")
        .bind(role)
        .execute(pool)
        .await
        .expect("expire lease");
}

async fn lease_row_count(pool: &PgPool, role: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM lease WHERE name = $1")
        .bind(role)
        .fetch_one(pool)
        .await
        .expect("count lease rows")
}

// ---- DbLeaderElectionTest semantics against the durable store -------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn acquires_leadership_on_first_poll_against_postgres() {
    let pool = fresh_pool().await;
    let election = manual_elector(&pool, "replica-A");
    let gate = election.gate("timer-leader");

    election.poll_now("timer-leader").await;

    assert!(election.is_leader("timer-leader"));
    assert!(gate.is_leading());
    assert_eq!(
        election.current_holder("timer-leader").as_deref(),
        Some("replica-A")
    );
    assert_eq!(lease_row_count(&pool, "timer-leader").await, 1);
    election.release_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn contended_lease_stays_with_the_current_holder() {
    let pool = fresh_pool().await;
    let holder = manual_elector(&pool, "replica-B");
    holder.gate("timer-leader");
    holder.poll_now("timer-leader").await;
    assert!(holder.is_leader("timer-leader"));

    let contender = manual_elector(&pool, "replica-A");
    contender.gate("timer-leader");
    contender.poll_now("timer-leader").await;

    assert!(!contender.is_leader("timer-leader"));
    assert_eq!(
        contender.current_holder("timer-leader").as_deref(),
        Some("replica-B"),
        "the contender observes the real holder"
    );
    holder.release_all().await;
    contender.release_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn renewal_by_acquire_keeps_leadership_and_extends_expiry() {
    let pool = fresh_pool().await;
    let election = manual_elector(&pool, "replica-A");
    election.gate("timer-leader");

    let expiry_epoch = |pool: PgPool| async move {
        let epoch: f64 = sqlx::query_scalar(
            "SELECT extract(epoch FROM expires_at)::float8 FROM lease WHERE name = 'timer-leader'",
        )
        .fetch_one(&pool)
        .await
        .expect("expiry");
        epoch
    };
    election.poll_now("timer-leader").await;
    let first_expiry = expiry_epoch(pool.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    election.poll_now("timer-leader").await;
    let second_expiry = expiry_epoch(pool.clone()).await;

    assert!(election.is_leader("timer-leader"));
    assert!(
        second_expiry > first_expiry,
        "renewal-by-acquire extends expiry"
    );
    election.release_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn expired_lease_is_taken_over_and_the_old_leader_demotes() {
    let pool = fresh_pool().await;
    let a = manual_elector(&pool, "replica-A");
    let b = manual_elector(&pool, "replica-B");
    let gate_a = a.gate("timer-leader");
    let gate_b = b.gate("timer-leader");

    a.poll_now("timer-leader").await;
    assert!(gate_a.is_leading());

    // Force expiry (the crashed-leader scenario) — B's next poll takes over.
    expire_lease(&pool, "timer-leader").await;
    b.poll_now("timer-leader").await;
    assert!(gate_b.is_leading());

    // A's next poll observes the loss and demotes (the gate flips — the consumer
    // cancellation signal).
    a.poll_now("timer-leader").await;
    assert!(!gate_a.is_leading());
    assert!(!a.is_leader("timer-leader"));
    assert_eq!(
        a.current_holder("timer-leader").as_deref(),
        Some("replica-B")
    );
    a.release_all().await;
    b.release_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn release_all_deletes_the_lease_row_and_reports_follower() {
    let pool = fresh_pool().await;
    let election = manual_elector(&pool, "replica-A");
    let gate = election.gate("timer-leader");
    election.poll_now("timer-leader").await;
    assert!(gate.is_leading());
    assert_eq!(lease_row_count(&pool, "timer-leader").await, 1);

    election.release_all().await;

    assert!(!election.is_leader("timer-leader"));
    assert!(!gate.is_leading());
    assert_eq!(lease_row_count(&pool, "timer-leader").await, 0);
    // Idempotent second release.
    election.release_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "docker"]
async fn dynamic_channel_role_registers_and_contends_durably() {
    let pool = fresh_pool().await;
    let election = manual_elector(&pool, "replica-A");

    let role = channel_role("acme", "transfer-queue");
    assert_eq!(role, "sutra-channel:acme:transfer-queue");
    let gate = election.gate(&role); // dynamic registration — never pre-listed
    assert!(!gate.is_leading());

    election.poll_now(&role).await;
    assert!(gate.is_leading());
    assert_eq!(lease_row_count(&pool, &role).await, 1);
    election.release_all().await;
    assert_eq!(lease_row_count(&pool, &role).await, 0);
}

// ---- the singleton proof (IT #2 semantics: consumerCount == 1 across replicas) ---------------

static BROKER: OnceLock<(Container<GenericImage>, u16)> = OnceLock::new();

fn rabbit_port() -> u16 {
    let (_, port) = BROKER.get_or_init(|| {
        std::thread::spawn(|| {
            let container = GenericImage::new("rabbitmq", "3.13-management-alpine")
                .with_exposed_port(5672.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
                .start()
                .expect("start rabbitmq:3.13-management-alpine (docker required)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(5672).expect("mapped 5672");
            (container, port)
        })
        .join()
        .expect("broker bootstrap thread")
    });
    *port
}

async fn raw_connection(port: u16) -> Connection {
    Connection::connect(
        &format!("amqp://127.0.0.1:{port}"),
        ConnectionProperties::default(),
    )
    .await
    .expect("raw AMQP connection")
}

async fn declare_queue(port: u16, name: &str) {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .queue_declare(
            name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
    connection.close(200, "declared").await.ok();
}

async fn consumer_count(port: u16, queue: &str) -> u32 {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let q = channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    connection.close(200, "counted").await.ok();
    q.consumer_count()
}

async fn publish(port: u16, queue: &str, message_id: &str, body: &[u8]) {
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default().with_message_id(message_id.into()),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    connection.close(200, "published").await.ok();
}

struct CountingIntake {
    delivered: Mutex<VecDeque<InboundMessage>>,
}

impl CountingIntake {
    fn new() -> Arc<CountingIntake> {
        Arc::new(CountingIntake {
            delivered: Mutex::new(VecDeque::new()),
        })
    }

    fn count(&self) -> usize {
        self.delivered.lock().unwrap().len()
    }
}

impl InboundIntake for CountingIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            self.delivered.lock().unwrap().push_back(message);
            AckDecision::Ack
        })
    }
}

fn singleton_source(port: u16, queue: &str) -> Arc<RabbitMqTriggerSource> {
    let properties = RabbitMqChannelProperties {
        host: "127.0.0.1".to_string(),
        port,
        virtual_host: "/".to_string(),
        username: None,
        password: None,
        queue: queue.to_string(),
        exchange: String::new(),
        prefetch_count: 10,
        ack_mode: AckMode::OnPersist,
        singleton: true,
    };
    let mut config = RabbitMqSourceConfig::new(
        "acme",
        "acme/money-transfer/1.0.0",
        "transfer-queue",
        properties,
    );
    config.gate_poll = Duration::from_millis(150);
    config.reconnect_min = Duration::from_millis(100);
    config.reconnect_max = Duration::from_millis(500);
    Arc::new(RabbitMqTriggerSource::new(config).expect("source"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn singleton_channel_consumes_on_exactly_one_replica_and_hands_over() {
    let pool = fresh_pool().await;
    let port = rabbit_port();
    let queue = format!("transfer-queue-q-{}", std::process::id());
    declare_queue(port, &queue).await;

    // Two replicas contending for the SAME dynamic role through the durable store —
    // fast real-scheduler timings (ttl 2s / poll 300ms).
    let role = channel_role("acme", "transfer-queue");
    let handle = tokio::runtime::Handle::current();
    let election_a = Arc::new(
        DbLeaderElection::new(
            lease_handle(&pool),
            Some("replica-A".to_string()),
            Duration::from_secs(2),
            Duration::from_millis(300),
            handle.clone(),
        )
        .expect("valid timings"),
    );
    let election_b = Arc::new(
        DbLeaderElection::new(
            lease_handle(&pool),
            Some("replica-B".to_string()),
            Duration::from_secs(2),
            Duration::from_millis(300),
            handle,
        )
        .expect("valid timings"),
    );
    let gate_a = election_a.gate(&role);
    let gate_b = election_b.gate(&role);

    let intake_a = CountingIntake::new();
    let intake_b = CountingIntake::new();
    let source_a = singleton_source(port, &queue);
    let source_b = singleton_source(port, &queue);
    source_a
        .start(intake_a.clone(), gate_a)
        .await
        .expect("start A");
    source_b
        .start(intake_b.clone(), gate_b)
        .await
        .expect("start B");

    // Exactly one replica wins the lease…
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leaders = [election_a.is_leader(&role), election_b.is_leader(&role)];
        if leaders.iter().filter(|l| **l).count() == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "no single leader emerged");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // …and exactly ONE consumer subscribes across both replicas — the M4 invariant.
    let deadline = Instant::now() + Duration::from_secs(10);
    while consumer_count(port, &queue).await != 1 {
        assert!(Instant::now() < deadline, "consumer count never reached 1");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        consumer_count(port, &queue).await,
        1,
        "consumerCount stays exactly 1 across 2 replicas"
    );

    // The leader consumes; the follower sees nothing.
    let a_leads = election_a.is_leader(&role);
    publish(port, &queue, "probe-1", b"transfer").await;
    let (leader_intake, follower_intake) = if a_leads {
        (&intake_a, &intake_b)
    } else {
        (&intake_b, &intake_a)
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while leader_intake.count() == 0 {
        assert!(Instant::now() < deadline, "leader never consumed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(follower_intake.count(), 0, "the follower consumed nothing");

    // Leadership handover: the leader's election releases (gate revokes) — its consumer
    // cancels, the other replica acquires and takes the queue over.
    let (leader_election, follower_election) = if a_leads {
        (&election_a, &election_b)
    } else {
        (&election_b, &election_a)
    };
    leader_election.release_all().await;

    let deadline = Instant::now() + Duration::from_secs(15);
    while !follower_election.is_leader(&role) {
        assert!(Instant::now() < deadline, "handover never happened");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The new leader consumes (probe until its consumer is up); the old leader stays
    // silent after its cancellation settles.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut probe = 1;
    while follower_intake.count() == 0 {
        assert!(
            Instant::now() < deadline,
            "the new leader never consumed after handover"
        );
        probe += 1;
        publish(port, &queue, &format!("probe-{probe}"), b"transfer").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let old_leader_count = leader_intake.count();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        consumer_count(port, &queue).await,
        1,
        "consumerCount returns to exactly 1 after handover"
    );
    assert_eq!(
        leader_intake.count(),
        old_leader_count,
        "the demoted replica no longer consumes"
    );

    source_a.stop().await.expect("stop A");
    source_b.stop().await.expect("stop B");
    follower_election.release_all().await;
}
