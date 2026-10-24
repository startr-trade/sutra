//! `llm.mode: job` against a real PostgreSQL: the submit row, the poller, callbacks and their
//! races. Every test is `#[ignore = "docker"]` (tier-2) and owns a fresh migrated database.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use sutra_channels::{MessageSink, SendOutcome};
use sutra_transport_llm::{
    callback_router, parse_all, CallbackTable, ChannelTable, JobOutcome, JobRunner, JobStoreSlot,
    LlmJobStore, LlmSink,
};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

use crate::jobs::{hooks_channel, job_yaml, CALLBACK_CANDIDATE};
use crate::support::{
    definitions, done, outbound, polls, resolver, set_job_state, CapturePort, ANSWER_TEXT,
    SIGNATURE_HEADER,
};

static CONTAINER: OnceLock<(Container<Postgres>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn port() -> u16 {
    CONTAINER
        .get_or_init(|| {
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
        })
        .1
}

async fn fresh_pool() -> PgPool {
    let url = |db: &str| format!("postgres://postgres:postgres@127.0.0.1:{}/{db}", port());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url("postgres"))
        .await
        .expect("admin connect");
    let db = format!("llm_job_{}", DB_SEQ.fetch_add(1, Ordering::Relaxed));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create database");
    admin.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url(&db))
        .await
        .expect("connect");
    let migrations =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sutra-persistence/migrations");
    let roots = [
        migrations.join("shipped/core"),
        migrations.join("shipped/audit"),
        migrations.join("shipped/deploy"),
        migrations.join("native"),
    ];
    let refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = sutra_persistence::migrate::collect_migrations(&refs).expect("collect");
    let mut conn = pool.acquire().await.expect("acquire");
    sutra_persistence::migrate::apply_migrations(&mut conn, &scripts)
        .await
        .expect("migrate");
    drop(conn);
    pool
}

struct Rig {
    pool: PgPool,
    port: Arc<CapturePort>,
    sink: LlmSink,
    runner: Arc<JobRunner>,
    router: axum::Router,
}

async fn rig(yaml: &str) -> Rig {
    let pool = fresh_pool().await;
    let parsed = parse_all(&definitions(yaml), resolver);
    assert!(parsed.failures.is_empty(), "{:?}", parsed.failures);
    let table = Arc::new(ChannelTable::from_map(parsed.channels));
    let callbacks = Arc::new(CallbackTable::new());
    callbacks.swap(parsed.callbacks);
    let port = Arc::new(CapturePort::default());
    let store = LlmJobStore::new(pool.clone(), "replica-a");
    let slot = Arc::new(JobStoreSlot::new());
    slot.set(Some(store.clone()));
    let sink = LlmSink::new(table.clone(), port.clone()).with_jobs(slot);
    let runner = Arc::new(JobRunner::new(table, store, port.clone()));
    let router = callback_router(callbacks, Some(runner.clone()));
    Rig {
        pool,
        port,
        sink,
        runner,
        router,
    }
}

fn callback_yaml() -> String {
    job_yaml(
        &[("job", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel("      callback-secret: env:hook\n"),
    )
}

impl Rig {
    async fn submit(&self, outbox_key: &str) -> String {
        let mut message = outbound(json!({"prompt": "hi", "orderId": "42"}));
        message.outbox_key = outbox_key.to_string();
        assert_eq!(self.sink.send(&message).await, SendOutcome::Delivered);
        sqlx::query_scalar("SELECT provider_job_id FROM llm_job WHERE outbox_key = $1")
            .bind(outbox_key)
            .fetch_one(&self.pool)
            .await
            .expect("the job row")
    }

    async fn make_due(&self) {
        sqlx::query("UPDATE llm_job SET next_poll_at = now() - interval '1 second'")
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn rows(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM llm_job")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    fn delivered(&self) -> usize {
        self.port.delivered.lock().unwrap().len()
    }

    async fn callback(&self, body: Value) -> StatusCode {
        let request = Request::post("/llm/hooks/fake")
            .header(SIGNATURE_HEADER, "resolved:hook")
            .body(Body::from(body.to_string()))
            .unwrap();
        self.router.clone().oneshot(request).await.unwrap().status()
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn submit_records_the_job_row_and_delivers_the_outbox_row() {
    let rig = rig(&job_yaml(
        &[("job-poll", "poll-interval-seconds: 45")],
        "",
        "",
    ))
    .await;
    let id = rig.submit("ob-1").await;
    let row = sqlx::query(
        "SELECT provider, channel, candidate_index, tenant, module_key, reply_channel, \
         request_body, headers_json, traceparent, claim_owner, failed, \
         EXTRACT(EPOCH FROM (next_poll_at - created_at))::BIGINT AS wait \
         FROM llm_job WHERE provider_job_id = $1",
    )
    .bind(&id)
    .fetch_one(&rig.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("provider"), crate::support::FAKE);
    assert_eq!(row.get::<String, _>("channel"), "ask");
    assert_eq!(row.get::<i32, _>("candidate_index"), 0);
    assert_eq!(row.get::<String, _>("tenant"), crate::support::TENANT);
    assert_eq!(
        row.get::<String, _>("module_key"),
        crate::support::MODULE_KEY
    );
    assert_eq!(row.get::<String, _>("reply_channel"), "answers");
    let body: Value = serde_json::from_slice(&row.get::<Vec<u8>, _>("request_body")).unwrap();
    assert_eq!(body["orderId"], "42");
    assert!(row.get::<String, _>("headers_json").contains("x-order-id"));
    assert!(row.get::<Option<String>, _>("traceparent").is_some());
    assert!(row.get::<Option<String>, _>("claim_owner").is_none());
    assert!(!row.get::<bool, _>("failed"));
    assert_eq!(row.get::<i64, _>("wait"), 45);
    assert_eq!(rig.delivered(), 0, "nothing is delivered at submit");

    // A re-delivered outbox row submits again; the second row is dropped.
    let mut again = outbound(json!({"prompt": "hi"}));
    again.outbox_key = "ob-1".into();
    assert_eq!(rig.sink.send(&again).await, SendOutcome::Delivered);
    assert_eq!(rig.rows().await, 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn poll_done_delivers_once_and_deletes_the_row() {
    let rig = rig(&job_yaml(&[("job-poll", "")], "", "")).await;
    let id = rig.submit("ob-done").await;
    set_job_state(&id, done());
    rig.make_due().await;
    assert_eq!(rig.runner.tick().await, 1);
    assert_eq!(rig.rows().await, 0);
    assert_eq!(rig.runner.tick().await, 0);
    let delivered = rig.port.delivered.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    let reply = &delivered[0];
    assert_eq!(reply.channel, "answers");
    assert_eq!(reply.idempotency_key, "ob-done");
    assert_eq!(reply.headers["x-order-id"], "42");
    assert!(reply.headers.contains_key("traceparent"));
    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(body["text"], ANSWER_TEXT);
    assert_eq!(body["request"]["orderId"], "42");
}

#[ignore = "docker"]
#[tokio::test]
async fn pending_reschedules_and_releases_the_claim() {
    let rig = rig(&job_yaml(&[("job-poll", "")], "", "")).await;
    let id = rig.submit("ob-pending").await;
    rig.make_due().await;
    assert_eq!(rig.runner.tick().await, 1);
    assert_eq!(polls(&id), 1);
    let (owner, ahead): (Option<String>, bool) = sqlx::query_as(
        "SELECT claim_owner, next_poll_at > now() + interval '20 seconds' FROM llm_job",
    )
    .fetch_one(&rig.pool)
    .await
    .unwrap();
    assert!(owner.is_none());
    assert!(ahead, "re-armed a poll interval out");
    assert_eq!(rig.runner.tick().await, 0, "not due again yet");
    assert_eq!(rig.delivered(), 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn a_job_past_its_deadline_fails_without_a_reply() {
    let rig = rig(&job_yaml(&[("job-poll", "")], "", "")).await;
    rig.submit("ob-late").await;
    sqlx::query("UPDATE llm_job SET deadline_at = now() - interval '1 second'")
        .execute(&rig.pool)
        .await
        .unwrap();
    rig.make_due().await;
    assert_eq!(rig.runner.tick().await, 1);
    let (failed, error): (bool, Option<String>) =
        sqlx::query_as("SELECT failed, last_error FROM llm_job")
            .fetch_one(&rig.pool)
            .await
            .unwrap();
    assert!(failed);
    assert!(error.unwrap().contains("job-timeout"));
    assert_eq!(rig.delivered(), 0);
    assert_eq!(
        rig.runner.tick().await,
        0,
        "a failed row is never claimed again"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn a_callback_delivers_once_and_duplicates_are_absorbed() {
    let rig = rig(&callback_yaml()).await;
    let id = rig.submit("ob-cb").await;
    assert_eq!(
        rig.callback(json!({"job": id, "done": true})).await,
        StatusCode::OK
    );
    assert_eq!(
        rig.callback(json!({"job": id, "done": true})).await,
        StatusCode::OK
    );
    assert_eq!(rig.delivered(), 1);
    assert_eq!(rig.rows().await, 0);
    assert_eq!(polls(&id), 0, "the callback carried the result");
}

#[ignore = "docker"]
#[tokio::test]
async fn a_callback_without_a_result_polls_once() {
    let rig = rig(&callback_yaml()).await;
    let id = rig.submit("ob-fetch").await;
    set_job_state(&id, done());
    assert_eq!(rig.callback(json!({"job": id})).await, StatusCode::OK);
    assert_eq!(polls(&id), 1);
    assert_eq!(rig.delivered(), 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn a_callback_while_the_poller_holds_the_row_nudges_and_delivers_once() {
    let rig = rig(&callback_yaml()).await;
    let id = rig.submit("ob-race").await;
    rig.make_due().await;
    // The poller has claimed the row and is mid-poll (still pending at the provider).
    let mut claimed = rig
        .runner
        .store()
        .claim_due(&deployment(), 8)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        rig.callback(json!({"job": id, "done": true})).await,
        StatusCode::OK
    );
    assert_eq!(
        rig.delivered(),
        0,
        "the claim holder delivers, not the callback"
    );
    assert_eq!(
        rig.runner.poll_and_settle(claimed.remove(0)).await,
        JobOutcome::Pending
    );
    // The nudge re-armed the row at once; the next pass finds it finished.
    set_job_state(&id, done());
    assert_eq!(rig.runner.tick().await, 1);
    assert_eq!(rig.delivered(), 1);
    assert_eq!(
        rig.callback(json!({"job": id, "done": true})).await,
        StatusCode::OK
    );
    assert_eq!(rig.delivered(), 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn a_callback_racing_the_poller_delivers_once() {
    for round in 0..5 {
        let rig = rig(&callback_yaml()).await;
        let id = rig.submit(&format!("ob-race-{round}")).await;
        set_job_state(&id, done());
        rig.make_due().await;
        let (_, status) = tokio::join!(
            rig.runner.tick(),
            rig.callback(json!({"job": id, "done": true}))
        );
        assert_eq!(status, StatusCode::OK);
        assert_eq!(rig.delivered(), 1, "round {round}");
        assert_eq!(rig.rows().await, 0);
    }
}

fn deployment() -> String {
    definitions(&callback_yaml())[0]
        .binding
        .deployment_id()
        .value()
        .to_string()
}
