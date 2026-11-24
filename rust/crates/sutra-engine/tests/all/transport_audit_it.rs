//! Transport audit events against real Postgres: the engine's journal
//! ([`sutra_engine::transport_audit::EngineTransportAuditJournal`]) over the SQL audit sink lands
//! a transport's event in the `audit_event` table — in the instance's journal (numbered with the
//! instance's high-water seq, never blocking the engine's next event) and in the deployment-level
//! journal — and writes nothing when audit is off for the process or the deployment.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use sutra_bpmn::qbindings::AuditCapture;
use sutra_channels::{
    TransportAuditContext, TransportAuditEvent, TransportAuditOutcome, TransportAuditRegistry,
    TransportAuditScope,
};
use sutra_engine::audit_sinks::SqlAuditStore;
use sutra_engine::transport_audit::{DeploymentAuditRoute, EngineTransportAuditJournal};
use sutra_persistence::migrate::{apply_migrations, collect_migrations};
use sutra_persistence::stores::{AuditEventRow, PgAuditEventStore};
use uuid::Uuid;

const DEP: &str = "dep-000000000000000000000088";
const MODULE: &str = "acme/calls/1.0.0";

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

/// A fresh database with the core and audit migrations applied.
async fn fresh_pool() -> PgPool {
    let port = container_port();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("admin pool");
    let db = format!("transport_audit_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
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
        let shipped = repo.join("rust/crates/sutra-persistence/migrations/shipped");
        let roots = [shipped.join("core"), shipped.join("audit")];
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

/// A registry holding the engine's journal, appending to a spool (in a temporary directory) that
/// a shipper loads into the SQL store. `route` is the deployment's.
fn journal(
    pool: &PgPool,
    route_sink: Option<&str>,
    route_capture: AuditCapture,
) -> TransportAuditRegistry {
    let spool = sutra_channels::AuditSpool::open(sutra_channels::AuditSpoolConfig {
        dir: std::env::temp_dir().join(format!("sutra-transport-audit-it-{}", Uuid::new_v4())),
        sync_linger: Duration::ZERO,
        sinks: ["sql".to_string()].into_iter().collect(),
        ..Default::default()
    })
    .expect("spool");
    let shipper = sutra_channels::spawn_audit_shipper(
        &tokio::runtime::Handle::current(),
        &spool,
        Arc::new(SqlAuditStore::new(pool.clone())),
        sutra_channels::ShipperConfig::default(),
    );
    let journal = EngineTransportAuditJournal::new(
        spool.sender(),
        vec![(
            MODULE.to_string(),
            DeploymentAuditRoute {
                deployment_id: DEP.into(),
                tenant: "acme".into(),
                sink: route_sink.map(str::to_string),
                capture: route_capture,
            },
        )],
    );
    // The spool and its shipper live for the rest of the test process.
    Box::leak(Box::new((spool, shipper)));
    let registry = TransportAuditRegistry::new();
    registry.install(journal);
    registry
}

fn instance_scope(
    instance: Uuid,
    capture: AuditCapture,
    sink: Option<&str>,
) -> TransportAuditScope {
    TransportAuditScope::Instance(TransportAuditContext {
        deployment_id: DEP.into(),
        instance_id: instance.to_string(),
        capture,
        sink: sink.map(str::to_string),
    })
}

fn refused() -> TransportAuditEvent {
    TransportAuditEvent::new("media", "REQUEST_REFUSED")
        .with_field("rule", "rate-limit")
        .with_sensitive_field("destination", "+15550100")
        .with_diagnostic_code("MEDIA.REQUEST_REFUSED")
}

type JournalRow = (Option<Uuid>, i32, String, Option<String>, String);

/// Every row of the deployment, in journal order.
async fn rows(pool: &PgPool) -> Vec<JournalRow> {
    sqlx::query_as(
        "SELECT instance_id, seq, event_type, diagnostic_code, payload_json FROM audit_event \
         WHERE deployment_id = $1 ORDER BY instance_id NULLS FIRST, seq, id",
    )
    .bind(DEP)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// The rows once `count` have landed (the journal writes in the background).
async fn rows_when(pool: &PgPool, count: usize) -> Vec<JournalRow> {
    for _ in 0..200 {
        let found = rows(pool).await;
        if found.len() >= count {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    rows(pool).await
}

fn engine_row(instance: Uuid, seq: i32, event_type: &str) -> AuditEventRow {
    AuditEventRow {
        deployment: sutra_persistence::DeploymentId::new(DEP).unwrap(),
        instance_id: Some(instance),
        seq,
        at: time::OffsetDateTime::now_utc(),
        event_type: event_type.into(),
        node_id: None,
        diagnostic_code: None,
        diagnostic_json: None,
        payload_json: "{}".into(),
        spool_ref: None,
        attempt: None,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn a_transport_event_lands_in_the_instance_and_deployment_journals() {
    let pool = fresh_pool().await;
    let engine = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();
    // The engine has written the instance's first two events.
    engine
        .insert(&engine_row(instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap();
    engine
        .insert(&engine_row(instance, 2, "NODE_ENTERED"))
        .await
        .unwrap();

    let registry = journal(&pool, Some("sql"), AuditCapture::Metadata);
    // A payload-level process: the fields are kept, the sensitive one redacted.
    assert_eq!(
        registry.append(
            instance_scope(instance, AuditCapture::Payload, Some("sql")),
            refused()
        ),
        TransportAuditOutcome::Queued
    );
    // A deployment-level event: metadata level, so no fields.
    assert_eq!(
        registry.append(
            TransportAuditScope::Deployment {
                module_key: MODULE.into()
            },
            TransportAuditEvent::new("media", "SESSION_RECOVERED")
        ),
        TransportAuditOutcome::Queued
    );
    let landed = rows_when(&pool, 4).await;
    assert_eq!(landed.len(), 4, "{landed:?}");

    let (none, seq, event_type, _, payload) = &landed[0];
    assert_eq!(
        (*none, *seq, event_type.as_str(), payload.as_str()),
        (None, 0, "media.SESSION_RECOVERED", "{}")
    );
    let (id, seq, event_type, code, payload) = &landed[3];
    assert_eq!(*id, Some(instance));
    assert_eq!(*seq, 2, "the instance's high-water seq");
    assert_eq!(event_type, "media.REQUEST_REFUSED");
    assert_eq!(code.as_deref(), Some("MEDIA.REQUEST_REFUSED"));
    let fields: serde_json::Value = serde_json::from_str(payload).unwrap();
    assert_eq!(fields["rule"], "rate-limit");
    assert_eq!(fields["destination"], sutra_bpmn::REDACTED_PLACEHOLDER);

    // The engine's next event is not blocked by the transport row, and its replay is still a
    // no-op.
    assert!(engine
        .insert(&engine_row(instance, 3, "NODE_LEFT"))
        .await
        .unwrap());
    assert!(!engine
        .insert(&engine_row(instance, 3, "NODE_LEFT"))
        .await
        .unwrap());
    // The instance history shows the transport event between the events it came between.
    let history: Vec<String> = engine
        .list_for_instance(
            &sutra_persistence::DeploymentId::new(DEP).unwrap(),
            instance,
            0,
            100,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.event_type)
        .collect();
    assert_eq!(
        history,
        vec![
            "INSTANCE_STARTED",
            "NODE_ENTERED",
            "media.REQUEST_REFUSED",
            "NODE_LEFT"
        ]
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn nothing_is_written_when_audit_is_off() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();

    // The process declares no <q:audit> (the engine stamps it capture=none); the deployment has
    // no audited process.
    let registry = journal(&pool, None, AuditCapture::Metadata);
    assert_eq!(
        registry.append(
            instance_scope(instance, AuditCapture::None, None),
            refused()
        ),
        TransportAuditOutcome::Suppressed
    );
    assert_eq!(
        registry.append(
            TransportAuditScope::Deployment {
                module_key: MODULE.into()
            },
            refused()
        ),
        TransportAuditOutcome::Suppressed
    );
    // A process that captures nothing.
    let audited = journal(&pool, Some("sql"), AuditCapture::Metadata);
    assert_eq!(
        audited.append(
            instance_scope(instance, AuditCapture::None, Some("sql")),
            refused()
        ),
        TransportAuditOutcome::Suppressed
    );
    // No journal installed at all (no audit sink in the engine).
    assert_eq!(
        TransportAuditRegistry::new().append(
            instance_scope(instance, AuditCapture::Metadata, Some("sql")),
            refused()
        ),
        TransportAuditOutcome::NotInstalled
    );

    // Give a background write the time it would take, then check nothing landed.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(rows(&pool).await.is_empty());
}

/// Records the sink each instance and node event of a pass is routed to.
#[derive(Default)]
struct RoutedSinks(std::cell::RefCell<Vec<(String, Option<String>)>>);

impl sutra_executor::listener::ExecutionListener for RoutedSinks {
    fn on_instance_started(&self, e: &sutra_executor::listener::InstanceEvent) {
        self.0
            .borrow_mut()
            .push(("INSTANCE_STARTED".into(), e.audit_sink.clone()));
    }
    fn on_token_entered(&self, e: &sutra_executor::listener::TokenEvent) {
        self.0
            .borrow_mut()
            .push((format!("NODE_ENTERED {}", e.node_id), e.audit_sink.clone()));
    }
}

/// `<q:audit capture="none"/>` means the same to both audit paths: the executor routes none of
/// the process's `INSTANCE_*`/`NODE_*` events, and the
/// transport journal suppresses the instance's transport events; nothing lands in the table.
#[ignore = "docker"]
#[tokio::test]
async fn capture_none_consistent_with_execution_audit() {
    use std::rc::Rc;
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();

    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="quiet">
            <bpmn:extensionElements><q:audit capture="none"/></bpmn:extensionElements>
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let process = module.process("quiet").expect("present").clone();
    let capture = process.audit.as_ref().expect("declares <q:audit>").capture;
    assert_eq!(capture, AuditCapture::None);

    let routed = Rc::new(RoutedSinks::default());
    let executor = sutra_executor::TokenExecutor::builder(sutra_executor::TaskRegistry::new())
        .with_listener(Rc::clone(&routed) as Rc<dyn sutra_executor::listener::ExecutionListener>)
        .build();
    executor
        .execute_sync(&process, sutra_executor::Variables::default())
        .await
        .expect("runs");
    let routed = routed.0.borrow().clone();
    assert!(!routed.is_empty(), "the pass still reports");
    assert!(
        routed.iter().all(|(_, sink)| sink.is_none()),
        "execution audit routes nothing: {routed:?}"
    );

    let registry = journal(&pool, Some("sql"), AuditCapture::Metadata);
    assert_eq!(
        registry.append(instance_scope(instance, capture, Some("sql")), refused()),
        TransportAuditOutcome::Suppressed,
        "the transport journal agrees"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(rows(&pool).await.is_empty());
}
