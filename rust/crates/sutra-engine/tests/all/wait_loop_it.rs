//! A loop through a wait state against real PostgreSQL, through the durable bridge: each turn
//! parks a NEW wait at the same node, keyed by its own correlation value; the step that answers
//! a turn resolves its wait row and retires its key in the same transaction; a late relay for a
//! past turn finds nothing; and the instance completes once the loop exits.
//!
//! The in-memory suites prove the dispatcher asks for the right rows. This one proves the rows
//! the database ends up with — the `waiting_event` slot reused per iteration and the
//! `alias_index` live/retired flips — because that is where a stale key would silently keep
//! matching.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::bridge::InstanceBridge;
use sutra_channels::{
    ChannelBinding, ChannelEngine, CodecRegistry, DrainingSink, InboundChain, InboundMessage,
    Namespace, ValidatorRegistry,
};
use sutra_engine::bridge::PersistenceBridge;
use sutra_executor::{TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;
use sutra_persistence::migrate::{apply_migrations, collect_migrations};

const DEP: &str = "dep-0000000000000000000000e1";
const TENANT: &str = "acme";

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
    let db = format!("wait_loop_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
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
        // The core schema plus the engine-native additions (the wait-row timer columns).
        let roots = [
            repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
            repo.join("rust/crates/sutra-persistence/migrations/native"),
        ];
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

fn namespace() -> Namespace {
    Namespace::new(TENANT, "loops", "1.0.0")
}

/// S → Prompt (sets `turn` and the next turn's key) → W (message catch keyed by that turn) →
/// G: `turn < 3` back to Prompt, else E.
const TURNS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="turns">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="start-in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Prompt" implementation="${prompt}"/>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements>
        <q:source channel="turn-in" name="answer"/>
        <q:alias name="turnKey" expression="if turnKey != null then turnKey else event.body"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="G" default="toEnd"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prompt"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prompt" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="W" targetRef="G"/>
    <bpmn:sequenceFlow id="again" sourceRef="G" targetRef="Prompt">
      <bpmn:conditionExpression>turn &lt; 3</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toEnd" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn engine(pool: PgPool, prompts: Rc<Cell<i64>>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(TURNS.as_bytes())
        .expect("BPMN loads");
    let tasks = TaskRegistry::new().register("prompt", move |_, _| {
        prompts.set(prompts.get() + 1);
        let turn = prompts.get();
        Ok(FeelValue::Map(BTreeMap::from([
            ("turn".to_string(), FeelValue::from(turn)),
            (
                "turnKey".to_string(),
                FeelValue::String(format!("T-{turn}")),
            ),
        ])))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in ["start-in", "turn-in"] {
        builder = builder.with_binding(ChannelBinding::new(
            channel,
            namespace(),
            deployment(),
            "raw-text",
        ));
    }
    builder
        .with_module(&deployment(), &module)
        .with_instance_bridge(Rc::new(PersistenceBridge::new(pool)) as Rc<dyn InstanceBridge>)
        .build()
}

fn inbound(channel: &str, body: &str, key: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: key.to_string(),
        explicit_event_id: true,
        received_at: "2026-10-25T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

/// `(alias_value, live)` of every `turnKey` row, by value.
async fn keys(pool: &PgPool) -> Vec<(String, bool)> {
    sqlx::query_as(
        "SELECT alias_value, live FROM alias_index WHERE alias_name = 'turnKey' \
         ORDER BY alias_value",
    )
    .fetch_all(pool)
    .await
    .expect("alias rows")
}

/// `(node_id, status)` of every wait row.
async fn waits(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as("SELECT node_id, status FROM waiting_event ORDER BY node_id")
        .fetch_all(pool)
        .await
        .expect("wait rows")
}

#[ignore = "docker"]
#[test]
fn a_message_catch_loop_parks_each_turn_fresh_and_retires_the_answered_key() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = rt.block_on(fresh_pool());
    let prompts = Rc::new(Cell::new(0));
    let engine = engine(pool.clone(), Rc::clone(&prompts));
    let send = |channel: &str, body: &str, key: &str| {
        rt.block_on(engine.dispatch(&inbound(channel, body, key)))
    };
    let live = |value: &str| (value.to_string(), true);
    let dead = |value: &str| (value.to_string(), false);
    let waiting = vec![("W".to_string(), "WAITING".to_string())];

    send("start-in", "hello", "s-1").expect("the instance parks at turn 1");
    assert_eq!(rt.block_on(keys(&pool)), [live("T-1")]);
    assert_eq!(rt.block_on(waits(&pool)), waiting);

    send("turn-in", "T-1", "r-1").expect("turn 1 resumes");
    assert_eq!(prompts.get(), 2);
    assert_eq!(
        rt.block_on(keys(&pool)),
        [dead("T-1"), live("T-2")],
        "turn 1's key retired in the step that answered it; turn 2 parks on its own"
    );
    assert_eq!(
        rt.block_on(waits(&pool)),
        waiting,
        "the wait slot is the new turn's, waiting"
    );

    let late = send("turn-in", "T-1", "r-1-late").expect_err("a past turn is refused");
    assert_eq!(
        late.code, "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND",
        "{late:?}"
    );
    assert_eq!(prompts.get(), 2, "the late relay ran nothing");

    send("turn-in", "T-2", "r-2").expect("turn 2 resumes");
    assert_eq!(
        rt.block_on(keys(&pool)),
        [dead("T-1"), dead("T-2"), live("T-3")]
    );
    send("turn-in", "T-3", "r-3").expect("turn 3 leaves the loop");
    assert_eq!(prompts.get(), 3);
    assert_eq!(
        rt.block_on(keys(&pool)),
        [dead("T-1"), dead("T-2"), dead("T-3")],
        "a finished instance holds no live key"
    );
    assert_eq!(
        rt.block_on(waits(&pool)),
        [("W".to_string(), "RESOLVED".to_string())]
    );
}
