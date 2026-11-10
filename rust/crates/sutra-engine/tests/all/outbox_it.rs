//! Outbox delivery PG integration — the dispatcher over the REAL `outbox_entry`
//! store (SKIP-LOCKED claim) and the REAL HTTP sink against a local axum listener:
//! enqueue → claim → deliver → delete, and enqueue → fail → defer → redeliver.
//! Hermetic postgres:16-alpine via testcontainers, migrated with THE SAME shipped
//! migration SQL files.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use sutra_channels::{HttpSink, OutboxDispatcher, RetryPolicy, SinkRegistry};
use sutra_engine::outbox::PgOutboxRows;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::stores::{OutboxEntry, OutboxStore, PgOutboxStore, ReplyMode};
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

fn shipped_migration_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf();
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
    let db = format!("outbox_it_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    drop(admin);

    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/{db}");
    {
        use sqlx::ConnectOptions;
        let options = sqlx::postgres::PgConnectOptions::new()
            .host("127.0.0.1")
            .port(port)
            .username("postgres")
            .password("postgres")
            .database(&db);
        let roots = shipped_migration_roots();
        let root_refs: Vec<&std::path::Path> = roots.iter().map(|p| p.as_path()).collect();
        let scripts = collect_migrations(&root_refs).expect("collect migrations");
        let mut migration_conn = options.connect().await.expect("migration connection");
        apply_migrations(&mut migration_conn, &scripts)
            .await
            .expect("apply migrations");
    }
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("pool")
}

// ---- capture server --------------------------------------------------------------------------

type CapturedRequest = (BTreeMap<String, String>, Vec<u8>);

#[derive(Clone)]
struct Capture {
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    /// Statuses served in order; the last one repeats.
    statuses: Arc<Mutex<Vec<u16>>>,
}

async fn capture_handler(
    State(state): State<Capture>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let mut map = BTreeMap::new();
    for (name, value) in &headers {
        if let Ok(v) = value.to_str() {
            map.insert(name.as_str().to_string(), v.to_string());
        }
    }
    state.requests.lock().unwrap().push((map, body.to_vec()));
    let mut statuses = state.statuses.lock().unwrap();
    let status = if statuses.len() > 1 {
        statuses.remove(0)
    } else {
        statuses[0]
    };
    StatusCode::from_u16(status).unwrap()
}

async fn capture_server(statuses: Vec<u16>) -> (SocketAddr, Capture) {
    let capture = Capture {
        requests: Arc::new(Mutex::new(Vec::new())),
        statuses: Arc::new(Mutex::new(statuses)),
    };
    let app = Router::new()
        .route("/cb", post(capture_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, capture)
}

// ---- helpers ----------------------------------------------------------------------------------

fn dep() -> PersistDeploymentId {
    PersistDeploymentId::new("dep-000000000000000000000061").expect("valid deployment id")
}

fn channel_dep() -> sutra_executor::DeploymentId {
    sutra_executor::DeploymentId::of("dep-000000000000000000000061").expect("valid deployment id")
}

fn entry(destination: &str) -> OutboxEntry {
    let now = OffsetDateTime::now_utc();
    let instance = Uuid::new_v4();
    OutboxEntry {
        deployment: dep(),
        entry_id: Uuid::new_v4(),
        instance_id: instance,
        body: b"<invoice-settled/>".to_vec().into(),
        content_type: Some("application/xml".to_owned()),
        destination: destination.to_owned(),
        headers: BTreeMap::new(),
        required: true,
        mode: ReplyMode::Native,
        outbox_key: format!("key-{instance}"),
        cloud_event_json: None,
        auth_ref_json: None,
        labels: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
        created_at: now,
        next_attempt_at: now,
        attempt_count: 0,
        last_diagnostic_json: None,
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned()),
        node_id: None,
    }
}

fn dispatcher(pool: PgPool) -> OutboxDispatcher {
    let mut sinks = SinkRegistry::new();
    sinks.register(Arc::new(HttpSink::new()));
    OutboxDispatcher::new(
        Arc::new(PgOutboxRows::new(pool)),
        sinks,
        RetryPolicy::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(300),
            false, // deterministic backoff for the redelivery clock below
        ),
        50,
    )
}

// ---- the ITs ----------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn enqueue_claim_deliver_delete() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let (addr, capture) = capture_server(vec![202]).await;

    let e = entry(&format!("http://{addr}/cb"));
    store.enqueue(&e).await.expect("enqueue");

    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.attempted, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);

    // Delivered: body bytes verbatim + outbox_key as Idempotency-Key + traceparent.
    {
        let requests = capture.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (headers, body) = &requests[0];
        assert_eq!(body, b"<invoice-settled/>");
        assert_eq!(headers.get("idempotency-key").unwrap(), &e.outbox_key);
        assert_eq!(headers.get("content-type").unwrap(), "application/xml");
        assert_eq!(
            headers.get("traceparent").unwrap(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
    }

    // Deleted: nothing left to claim.
    let remaining = store
        .claim_due(&dep(), OffsetDateTime::now_utc(), 10)
        .await
        .expect("claim");
    assert!(remaining.is_empty(), "delivered row must be deleted");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn enqueue_fail_defer_redeliver() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    // First attempt answers 503 (retryable), the second 202.
    let (addr, capture) = capture_server(vec![503, 202]).await;

    let e = entry(&format!("http://{addr}/cb"));
    store.enqueue(&e).await.expect("enqueue");

    // Tick 1 — send fails, the row defers with backoff + diagnostic.
    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.failed, 1);

    // Tick 2 (same clock) — the deferred row is NOT yet due.
    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(
        stats.attempted, 0,
        "deferred row must not be claimable before its due time"
    );

    // The deferral is observable: attempt_count = 1, diagnostic recorded, future due.
    let deferred = store
        .claim_due(
            &dep(),
            OffsetDateTime::now_utc() + time::Duration::hours(1),
            10,
        )
        .await
        .expect("claim(+1h)");
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].attempt_count, 1);
    assert!(deferred[0].next_attempt_at > OffsetDateTime::now_utc());
    let diagnostic = deferred[0].last_diagnostic_json.as_deref().unwrap();
    assert!(
        diagnostic.contains("SUTRA.OUTBOUND.SEND.FAILED"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("503"), "{diagnostic}");

    // Tick 3 — a dispatcher whose clock passed the backoff redelivers; 202 → deleted.
    let redeliver_at = OffsetDateTime::now_utc() + time::Duration::minutes(10);
    let stats = dispatcher(pool.clone())
        .with_clock(move || redeliver_at)
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.succeeded, 1, "deferred row redelivers once due");

    assert_eq!(
        capture.requests.lock().unwrap().len(),
        2,
        "two wire attempts"
    );
    let remaining = store
        .claim_due(&dep(), redeliver_at + time::Duration::hours(1), 10)
        .await
        .expect("claim final");
    assert!(remaining.is_empty(), "redelivered row must be deleted");
}

/// A row committed for an in-process hand-off is inserted due only after the hand-off grace.
/// When the hand-off has closed by the time the commit lands, the fallback makes the row due at
/// once: the very next poll tick delivers it, instead of a tick 30 s later. A row that was
/// attempted in the meantime is left as it is.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_closed_hand_off_delivers_on_the_next_poll_tick() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let (addr, capture) = capture_server(vec![202]).await;
    let grace = time::Duration::try_from(sutra_channels::outbox_dispatch::HANDOFF_GRACE)
        .expect("grace fits");

    // As the bridge commits a row it means to hand off: reserved, due after the grace.
    let mut e = entry(&format!("http://{addr}/cb"));
    e.next_attempt_at += grace;
    let signal = sutra_channels::OutboxSignal::new();
    signal.reserve(&[e.entry_id.to_string()]);
    store.enqueue(&e).await.expect("enqueue");

    // Without the fallback a poll tick now finds nothing: the row waits out the grace.
    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.attempted, 0, "the row is not due before the grace");

    // No dispatcher takes the hand-off (it has closed), so the row is made due now.
    assert!(!signal.accepts_hand_off());
    sutra_engine::outbox::hand_off_or_make_due(&signal, &pool, &channel_dep(), vec![e.clone()])
        .await;
    assert_eq!(signal.inflight_count(), 0, "the reservation is released");

    // The next poll tick, at the real clock, delivers it once.
    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.succeeded, 1, "the next tick delivers the row");
    assert_eq!(capture.requests.lock().unwrap().len(), 1);

    // A row attempted since its insert keeps the due time that attempt gave it.
    let mut attempted = entry(&format!("http://{addr}/cb"));
    attempted.next_attempt_at += grace;
    store.enqueue(&attempted).await.expect("enqueue");
    let deferred_to = OffsetDateTime::now_utc() + time::Duration::minutes(5);
    store
        .defer(&dep(), attempted.entry_id, deferred_to, None)
        .await
        .expect("defer");
    let made_due = store
        .make_due_if_unclaimed(
            &dep(),
            &[(attempted.entry_id, attempted.next_attempt_at)],
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("make due");
    assert_eq!(made_due, 0, "an attempted row is not made due");
    let stats = dispatcher(pool.clone())
        .dispatch_deployment(&channel_dep())
        .await;
    assert_eq!(stats.attempted, 0, "the attempted row keeps its deferral");
}
