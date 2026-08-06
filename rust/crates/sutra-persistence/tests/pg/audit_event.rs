//! Audit-event store (`audit_event`, V201) — idempotent per-instance persistence + RLS
//! deployment isolation. The engine guarantees a monotonic per-instance seq (persisted in the
//! snapshot, seeded on resume); this suite pins that a re-emit of an already-persisted
//! `(deployment_id, instance_id, seq)` is a NO-OP, and that the same `(instance, seq)` is FRESH
//! under a different deployment.

use sutra_persistence::stores::{AuditEventRow, PgAuditEventStore};
use sutra_persistence::DeploymentId;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool, now_micros};

fn row(dep: &DeploymentId, instance: Uuid, seq: i32, event_type: &str) -> AuditEventRow {
    AuditEventRow {
        deployment: dep.clone(),
        instance_id: Some(instance),
        seq,
        at: now_micros(),
        event_type: event_type.to_string(),
        node_id: Some("Start".to_string()),
        diagnostic_code: None,
        diagnostic_json: None,
        payload_json: "{}".to_string(),
    }
}

async fn count(pool: &sqlx::PgPool, dep: &DeploymentId, instance: Uuid) -> i64 {
    // Cross-deployment read (no GUC) — the test asserts raw row counts; requires the owner role
    // the test pool connects as (RLS bypass for the owner), exactly like the maintenance path.
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM audit_event WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(dep.as_str())
    .bind(instance)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[ignore = "docker"]
#[tokio::test]
async fn first_write_vs_idempotent_replay() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();

    // First observer inserts; the identical (deployment, instance, seq) is a no-op replay.
    assert!(store
        .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());
    assert!(!store
        .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());
    // A DIFFERENT seq for the same instance is a fresh write (the post-resume continuation case).
    assert!(store
        .insert(&row(&dep_a(), instance, 2, "NODE_ENTERED"))
        .await
        .unwrap());

    assert_eq!(
        count(&pool, &dep_a(), instance).await,
        2,
        "the duplicate seq did not add a row"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn same_instance_seq_is_fresh_under_a_different_deployment() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();

    assert!(store
        .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());
    // The uniqueness key is (deployment_id, instance_id, seq): deployment B is a distinct scope.
    assert!(store
        .insert(&row(&dep_b(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());

    assert_eq!(count(&pool, &dep_a(), instance).await, 1);
    assert_eq!(count(&pool, &dep_b(), instance).await, 1);
}

async fn payload(pool: &sqlx::PgPool, dep: &DeploymentId, instance: Uuid, seq: i32) -> String {
    sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM audit_event \
         WHERE deployment_id = $1 AND instance_id = $2 AND seq = $3",
    )
    .bind(dep.as_str())
    .bind(instance)
    .bind(seq)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[ignore = "docker"]
#[tokio::test]
async fn redact_instance_payloads_nulls_payload_retains_row() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();

    let mut captured = row(&dep_a(), instance, 1, "NODE_ENTERED");
    captured.payload_json = "{\"ssn\":\"123-45-6789\"}".to_string();
    assert!(store.insert(&captured).await.unwrap());

    let redacted = store
        .redact_instance_payloads(&dep_a(), instance)
        .await
        .unwrap();
    assert_eq!(redacted, 1);

    // The row still exists (metadata retained — the erasure itself stays auditable); only the
    // captured PII payload is nulled.
    assert_eq!(
        count(&pool, &dep_a(), instance).await,
        1,
        "the row is retained, not deleted"
    );
    assert_eq!(payload(&pool, &dep_a(), instance, 1).await, "{}");

    // Idempotent: nothing left to redact.
    let redacted_again = store
        .redact_instance_payloads(&dep_a(), instance)
        .await
        .unwrap();
    assert_eq!(redacted_again, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn redact_instance_payloads_leaves_other_instances_untouched() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let erased = Uuid::new_v4();
    let other = Uuid::new_v4();

    let mut erased_row = row(&dep_a(), erased, 1, "NODE_ENTERED");
    erased_row.payload_json = "{\"ssn\":\"111-11-1111\"}".to_string();
    assert!(store.insert(&erased_row).await.unwrap());

    let mut other_row = row(&dep_a(), other, 1, "NODE_ENTERED");
    other_row.payload_json = "{\"ssn\":\"222-22-2222\"}".to_string();
    assert!(store.insert(&other_row).await.unwrap());

    let redacted = store
        .redact_instance_payloads(&dep_a(), erased)
        .await
        .unwrap();
    assert_eq!(redacted, 1);

    assert_eq!(payload(&pool, &dep_a(), erased, 1).await, "{}");
    assert_eq!(
        payload(&pool, &dep_a(), other, 1).await,
        "{\"ssn\":\"222-22-2222\"}",
        "a different instance's payload must be untouched"
    );
}

/// The metadata-only `SUTRA.SUBJECT_ERASED` marker uses `instance_id = NULL` + `seq = 0`.
/// Because NULLs are DISTINCT in the `(deployment_id, instance_id, seq)` unique index, two markers
/// coexist (each erasure records its own) — they never collide via the `ON CONFLICT DO NOTHING`
/// dedup that per-instance events rely on. This pins that assumption behind the erasure endpoint.
#[ignore = "docker"]
#[tokio::test]
async fn null_instance_subject_erased_markers_do_not_collide() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let marker = |dep: &DeploymentId| AuditEventRow {
        deployment: dep.clone(),
        instance_id: None,
        seq: 0,
        at: now_micros(),
        event_type: "SUTRA.SUBJECT_ERASED".to_string(),
        node_id: None,
        diagnostic_code: None,
        diagnostic_json: Some("{\"subjectName\":\"customerId\",\"erasedCount\":2}".to_string()),
        payload_json: "{}".to_string(),
    };

    // Two erasure markers for the same deployment, both `(dep, NULL, 0)` — both must INSERT.
    assert!(store.insert(&marker(&dep_a())).await.unwrap());
    assert!(store.insert(&marker(&dep_a())).await.unwrap());

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_event WHERE deployment_id = $1 AND instance_id IS NULL \
         AND event_type = 'SUTRA.SUBJECT_ERASED'",
    )
    .bind(dep_a().as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        n, 2,
        "both NULL-instance erasure markers must persist (NULLs are distinct in the unique)"
    );
}

// ---- the READ side: instance execution history (P1-2) -----------------------------------------

/// The journal reads back in seq order and pages by CURSOR, not offset — the property that makes
/// a page stable while a still-running instance keeps appending events.
#[ignore = "docker"]
#[tokio::test]
async fn list_for_instance_pages_by_seq_cursor_in_order() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    // Insert OUT of seq order to prove the ordering comes from the query, not from insertion.
    for seq in [3, 1, 5, 2, 4] {
        store
            .insert(&row(&dep_a(), instance, seq, "NODE_ENTERED"))
            .await
            .unwrap();
    }

    let first = store
        .list_for_instance(&dep_a(), instance, 0, 2)
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2],
        "seq-ascending from the start of the journal"
    );

    let second = store
        .list_for_instance(&dep_a(), instance, first.last().unwrap().seq, 2)
        .await
        .unwrap();
    assert_eq!(second.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 4]);

    // A short page is the end of the journal.
    let last = store
        .list_for_instance(&dep_a(), instance, 4, 2)
        .await
        .unwrap();
    assert_eq!(last.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![5]);
    assert!(store
        .list_for_instance(&dep_a(), instance, 5, 2)
        .await
        .unwrap()
        .is_empty());
}

/// Every column an operator needs comes back, the captured payload included — which is exactly why
/// the endpoint serving this is admin-only.
#[ignore = "docker"]
#[tokio::test]
async fn list_for_instance_returns_the_full_row_including_the_captured_payload() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    let mut event = row(&dep_a(), instance, 1, "NODE_LEFT");
    event.node_id = Some("Approve".to_string());
    event.diagnostic_code = Some("SUTRA.RUNTIME.TASK.UNCAUGHT".to_string());
    event.diagnostic_json = Some("{\"message\":\"boom\"}".to_string());
    event.payload_json = "{\"amount\":42}".to_string();
    store.insert(&event).await.unwrap();

    let rows = store
        .list_for_instance(&dep_a(), instance, 0, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let read = &rows[0];
    assert_eq!(read.seq, 1);
    assert_eq!(read.event_type, "NODE_LEFT");
    assert_eq!(read.node_id.as_deref(), Some("Approve"));
    assert_eq!(
        read.diagnostic_code.as_deref(),
        Some("SUTRA.RUNTIME.TASK.UNCAUGHT")
    );
    assert_eq!(
        read.diagnostic_json.as_deref(),
        Some("{\"message\":\"boom\"}")
    );
    assert_eq!(read.payload_json, "{\"amount\":42}");
    assert!(
        read.id > 0,
        "the surrogate key comes back for stable ordering"
    );
}

/// Deployment isolation holds on the read path too: dep-B never sees dep-A's journal.
#[ignore = "docker"]
#[tokio::test]
async fn list_for_instance_is_deployment_scoped() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    store
        .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap();

    assert_eq!(
        store
            .list_for_instance(&dep_a(), instance, 0, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .list_for_instance(&dep_b(), instance, 0, 10)
        .await
        .unwrap()
        .is_empty());
}

/// The journal SURVIVES a GDPR erasure as a redacted trail — the fact an instance ran stays
/// auditable while the captured PII does not. This is the same reason the retention purge leaves
/// `audit_event` alone.
#[ignore = "docker"]
#[tokio::test]
async fn list_for_instance_shows_redacted_payloads_after_an_erasure() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    let mut event = row(&dep_a(), instance, 1, "NODE_ENTERED");
    event.payload_json = "{\"ssn\":\"000-00-0000\"}".to_string();
    store.insert(&event).await.unwrap();

    store
        .redact_instance_payloads(&dep_a(), instance)
        .await
        .unwrap();

    let rows = store
        .list_for_instance(&dep_a(), instance, 0, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the trail row itself is retained");
    assert_eq!(rows[0].event_type, "NODE_ENTERED");
    assert_eq!(rows[0].payload_json, "{}", "the captured PII is gone");
}
