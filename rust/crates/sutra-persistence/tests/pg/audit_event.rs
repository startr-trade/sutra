//! Audit-event store (`audit_event`, V201) — idempotent per-instance persistence + RLS
//! deployment isolation. The engine guarantees a monotonic per-instance seq (persisted in the
//! snapshot, seeded on resume); this suite pins that a re-emit of an already-persisted
//! `(deployment_id, instance_id, seq)` is a NO-OP, and that the same `(instance, seq)` is FRESH
//! under a different deployment.

use sutra_persistence::stores::{AuditEventRow, PgAuditEventStore};
use sutra_persistence::DeploymentId;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_audit_pool, fresh_pool, now_micros};

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
        spool_ref: None,
        attempt: None,
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
        spool_ref: None,
        attempt: None,
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

/// A transport event (`<transport>.<EVENT>`) in an instance's journal takes the instance's
/// current high-water seq over the engine's events and sits outside the replay-dedup index: it
/// never blocks the engine event that comes next under that seq, the engine's replay dedup still
/// holds, and a deployment-level transport event (no instance) lands with seq 0.
#[ignore = "docker"]
#[tokio::test]
async fn a_transport_event_never_takes_a_number_the_engine_writes() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();

    // Before any engine event the transport event is numbered 1 (seqs start there).
    assert!(store
        .insert(&row(&dep_a(), instance, 99, "media.SESSION_RECOVERED"))
        .await
        .unwrap());
    assert!(store
        .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());
    // Two transport events after seq 1 both land, numbered 1 (the caller's seq is ignored).
    for _ in 0..2 {
        assert!(store
            .insert(&row(&dep_a(), instance, 1, "media.REQUEST_REFUSED"))
            .await
            .unwrap());
    }
    // The engine's next event, seq 2, is not blocked; its replay is still a no-op.
    assert!(store
        .insert(&row(&dep_a(), instance, 2, "NODE_ENTERED"))
        .await
        .unwrap());
    assert!(!store
        .insert(&row(&dep_a(), instance, 2, "NODE_ENTERED"))
        .await
        .unwrap());

    let journal = store
        .list_for_instance(&dep_a(), instance, 0, 100)
        .await
        .unwrap();
    let order: Vec<(i32, &str)> = journal
        .iter()
        .map(|r| (r.seq, r.event_type.as_str()))
        .collect();
    assert_eq!(
        order,
        vec![
            (1, "media.SESSION_RECOVERED"),
            (1, "INSTANCE_STARTED"),
            (1, "media.REQUEST_REFUSED"),
            (1, "media.REQUEST_REFUSED"),
            (2, "NODE_ENTERED"),
        ]
    );

    // The deployment-level journal: no instance, seq 0, any number of rows.
    let mut deployment_row = row(&dep_a(), instance, 7, "media.REQUEST_REFUSED");
    deployment_row.instance_id = None;
    assert!(store.insert(&deployment_row).await.unwrap());
    assert!(store.insert(&deployment_row).await.unwrap());
    let deployment_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_event WHERE deployment_id = $1 AND instance_id IS NULL \
         AND seq = 0 AND event_type = 'media.REQUEST_REFUSED'",
    )
    .bind(dep_a().as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(deployment_rows, 2);
}

/// A transport event written AFTER a reader passed the seq it lands on is missed by a `seq`
/// cursor but returned by the ordinal cursor: the ordinal is assigned in commit order, so paging
/// by it is lossless while the instance is still writing.
#[ignore = "docker"]
#[tokio::test]
async fn ordinal_paging_returns_a_transport_event_written_behind_the_seq_cursor() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    store
        .insert(&row(&dep_a(), instance, 1, "NODE_ENTERED"))
        .await
        .unwrap();

    let first = store
        .list_for_instance_after_ordinal(&dep_a(), instance, 0, 10)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].ordinal, Some(1));
    let (seq_cursor, ordinal_cursor) = (first[0].seq, first[0].ordinal.unwrap());

    // The transport writes now, at the engine's high-water seq — the one the reader already passed.
    store
        .insert(&row(&dep_a(), instance, 0, "http.REQUEST_SENT"))
        .await
        .unwrap();
    store
        .insert(&row(&dep_a(), instance, 2, "NODE_LEFT"))
        .await
        .unwrap();

    let by_seq = store
        .list_for_instance(&dep_a(), instance, seq_cursor, 10)
        .await
        .unwrap();
    assert_eq!(
        by_seq
            .iter()
            .map(|r| r.event_type.as_str())
            .collect::<Vec<_>>(),
        ["NODE_LEFT"],
        "the legacy seq cursor skips the transport row (the defect the ordinal fixes)"
    );
    let by_ordinal = store
        .list_for_instance_after_ordinal(&dep_a(), instance, ordinal_cursor, 10)
        .await
        .unwrap();
    assert_eq!(
        by_ordinal
            .iter()
            .map(|r| (r.event_type.as_str(), r.ordinal))
            .collect::<Vec<_>>(),
        [("http.REQUEST_SENT", Some(2)), ("NODE_LEFT", Some(3))]
    );

    // A replayed engine event is still dropped by the seq dedup, and takes no ordinal.
    assert!(!store
        .insert(&row(&dep_a(), instance, 2, "NODE_LEFT"))
        .await
        .unwrap());
    let all = store
        .list_for_instance_after_ordinal(&dep_a(), instance, 0, 10)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|r| r.ordinal.unwrap()).collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

/// Concurrent writers of one instance never leave a gap a reader can pass: the ordinals form
/// exactly 1..=N, whatever order the transactions commit in.
#[ignore = "docker"]
#[tokio::test]
async fn concurrent_writers_of_one_instance_get_dense_ordinals() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instance = Uuid::new_v4();
    let writers: Vec<_> = (1..=20)
        .map(|n| {
            let store = store.clone();
            tokio::spawn(async move {
                let event = if n % 2 == 0 {
                    "NODE_ENTERED"
                } else {
                    "http.SENT"
                };
                store
                    .insert(&row(&dep_a(), instance, n, event))
                    .await
                    .unwrap();
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    let all = store
        .list_for_instance_after_ordinal(&dep_a(), instance, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|r| r.ordinal.unwrap()).collect::<Vec<_>>(),
        (1..=20).collect::<Vec<i64>>()
    );
}

/// A batch of several instances' events, interleaved as the engine emits them, lands in ONE
/// transaction with each instance's journal in emission order: dense ordinals 1..=N per instance
/// in the order given, a transport event numbered from the engine events before it, a replayed
/// row skipped (also inside the same batch), and the whole batch counted as first-writes only.
#[ignore = "docker"]
#[tokio::test]
async fn insert_batch_keeps_each_instance_journal_in_order_with_dense_ordinals() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    // Already written one by one: a's seq 1. The batch's copy of it must be a no-op.
    assert!(store
        .insert(&row(&dep_a(), a, 1, "INSTANCE_STARTED"))
        .await
        .unwrap());

    let batch = vec![
        row(&dep_a(), a, 1, "INSTANCE_STARTED"), // replay
        row(&dep_a(), b, 1, "INSTANCE_STARTED"),
        row(&dep_a(), a, 2, "NODE_ENTERED"),
        row(&dep_a(), b, 2, "NODE_ENTERED"),
        row(&dep_a(), a, 0, "http.REQUEST_SENT"), // transport: numbered from a's seq 2
        row(&dep_a(), a, 3, "NODE_LEFT"),
        row(&dep_a(), b, 3, "NODE_LEFT"),
        row(&dep_a(), b, 3, "NODE_LEFT"), // the same key twice in one batch
        row(&dep_a(), a, 4, "INSTANCE_COMPLETED"),
    ];
    assert_eq!(store.insert_batch(&batch).await.unwrap(), 7);

    let journal = |instance| {
        let store = store.clone();
        async move {
            store
                .list_for_instance_after_ordinal(&dep_a(), instance, 0, 100)
                .await
                .unwrap()
                .into_iter()
                .map(|r| (r.ordinal.unwrap(), r.seq, r.event_type))
                .collect::<Vec<_>>()
        }
    };
    let s = |v: &str| v.to_string();
    assert_eq!(
        journal(a).await,
        vec![
            (1, 1, s("INSTANCE_STARTED")),
            (2, 2, s("NODE_ENTERED")),
            (3, 2, s("http.REQUEST_SENT")),
            (4, 3, s("NODE_LEFT")),
            (5, 4, s("INSTANCE_COMPLETED")),
        ]
    );
    assert_eq!(
        journal(b).await,
        vec![
            (1, 1, s("INSTANCE_STARTED")),
            (2, 2, s("NODE_ENTERED")),
            (3, 3, s("NODE_LEFT")),
        ]
    );
    // Replaying the whole batch writes nothing but the transport row (never deduplicated).
    assert_eq!(store.insert_batch(&batch).await.unwrap(), 1);
    assert_eq!(store.insert_batch(&[]).await.unwrap(), 0);
}

/// A batch is one deployment's: a row of another deployment is refused before anything is
/// written, and nothing of the batch is committed.
#[ignore = "docker"]
#[tokio::test]
async fn insert_batch_refuses_mixed_deployments_and_writes_nothing() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let instance = Uuid::new_v4();
    let batch = vec![
        row(&dep_a(), instance, 1, "INSTANCE_STARTED"),
        row(&dep_b(), instance, 2, "NODE_ENTERED"),
    ];
    assert!(store.insert_batch(&batch).await.is_err());
    assert_eq!(count(&pool, &dep_a(), instance).await, 0);
    assert_eq!(count(&pool, &dep_b(), instance).await, 0);
}

/// Two batches of the same instances, written concurrently with their instances in opposite
/// orders, never deadlock (the batch takes the per-instance ordinal locks in one global order)
/// and leave each journal dense.
#[ignore = "docker"]
#[tokio::test]
async fn concurrent_batches_over_the_same_instances_neither_deadlock_nor_gap() {
    let pool = fresh_pool().await;
    let store = PgAuditEventStore::new(pool);
    let instances: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let store = store.clone();
            let mut order = instances.clone();
            if w % 2 == 1 {
                order.reverse();
            }
            tokio::spawn(async move {
                let batch: Vec<AuditEventRow> = order
                    .iter()
                    .map(|i| row(&dep_a(), *i, w + 1, "NODE_ENTERED"))
                    .collect();
                store.insert_batch(&batch).await.unwrap()
            })
        })
        .collect();
    for writer in writers {
        assert_eq!(writer.await.unwrap(), 8);
    }
    for instance in instances {
        let all = store
            .list_for_instance_after_ordinal(&dep_a(), instance, 0, 100)
            .await
            .unwrap();
        assert_eq!(
            all.iter().map(|r| r.ordinal.unwrap()).collect::<Vec<_>>(),
            (1..=8).collect::<Vec<i64>>()
        );
    }
}

/// A row with a spool reference, as the shipper writes it.
fn shipped(row: AuditEventRow, spool_ref: &str) -> AuditEventRow {
    AuditEventRow {
        spool_ref: Some(spool_ref.to_string()),
        attempt: None,
        ..row
    }
}

/// A re-shipped spool record is dropped by its `spool_ref`, including the rows the
/// `(deployment, instance, seq)` index cannot dedup: a deployment-level row and a transport row.
#[ignore = "docker"]
#[tokio::test]
async fn spool_ref_dedups_reshipped_rows() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let spool = Uuid::new_v4().to_string();
    let instance = Uuid::new_v4();
    let deployment_level = AuditEventRow {
        instance_id: None,
        seq: 0,
        ..row(&dep_a(), instance, 0, "http.RECEIVED")
    };
    let transport = row(&dep_a(), instance, 0, "http.SENT");
    let batch = vec![
        shipped(
            row(&dep_a(), instance, 1, "INSTANCE_STARTED"),
            &format!("{spool}:1"),
        ),
        shipped(transport, &format!("{spool}:2")),
        shipped(deployment_level, &format!("{spool}:3")),
    ];
    assert_eq!(store.insert_batch(&batch).await.unwrap(), 3);
    // The shipper crashed before its checkpoint: the same records arrive again.
    assert_eq!(
        store.insert_batch(&batch).await.unwrap(),
        0,
        "a re-ship writes nothing"
    );
    for record in &batch {
        assert!(
            !store.insert(record).await.unwrap(),
            "nor does a single re-ship"
        );
    }
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(total, 3);
}

/// The 0.8 replay dedup on `(deployment, instance, seq)` is unchanged, whether or not the
/// re-emit carries a (new) spool reference: the step re-run after a crash re-emits the same
/// numbers under the next spool records.
#[ignore = "docker"]
#[tokio::test]
async fn instance_row_dedup_unchanged() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let spool = Uuid::new_v4().to_string();
    let instance = Uuid::new_v4();
    assert!(store
        .insert(&shipped(
            row(&dep_a(), instance, 1, "INSTANCE_STARTED"),
            &format!("{spool}:7")
        ))
        .await
        .unwrap());
    assert!(
        !store
            .insert(&shipped(
                row(&dep_a(), instance, 1, "INSTANCE_STARTED"),
                &format!("{spool}:9")
            ))
            .await
            .unwrap(),
        "a re-emit under a new spool record is still the same (instance, seq)"
    );
    assert!(
        !store
            .insert(&row(&dep_a(), instance, 1, "INSTANCE_STARTED"))
            .await
            .unwrap(),
        "and so is one without a reference"
    );
    assert_eq!(count(&pool, &dep_a(), instance).await, 1);
}

/// A fresh audit database gets the audit family and nothing else, with its own
/// ledger.
#[ignore = "docker"]
#[tokio::test]
async fn audit_family_applies_to_fresh_db() {
    let pool = fresh_audit_pool().await;
    let history: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM sutra_schema_history ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(history, vec![201, 202, 203, 204, 205, 206]);
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables \
         WHERE table_schema = 'public' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(tables, vec!["audit_event", "sutra_schema_history"]);
    let spool_ref: Option<String> = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'audit_event_spool_ref'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(
        spool_ref.is_some_and(|def| def.contains("UNIQUE")),
        "the V205 unique index exists"
    );
}

/// A shipped batch spans deployments in one transaction, under each row's own
/// RLS scope, and a re-ship of it writes nothing.
#[ignore = "docker"]
#[tokio::test]
async fn shipped_batch_spans_deployments_once() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let spool = Uuid::new_v4().to_string();
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let batch = vec![
        shipped(
            row(&dep_a(), a, 1, "INSTANCE_STARTED"),
            &format!("{spool}:1"),
        ),
        shipped(
            row(&dep_b(), b, 1, "INSTANCE_STARTED"),
            &format!("{spool}:2"),
        ),
        shipped(row(&dep_a(), a, 2, "NODE_ENTERED"), &format!("{spool}:3")),
    ];
    assert_eq!(store.insert_shipped(&batch).await.unwrap(), 3);
    assert_eq!(
        store.insert_shipped(&batch).await.unwrap(),
        0,
        "a re-ship writes nothing"
    );
    let journal = store.list_for_instance(&dep_a(), a, 0, 10).await.unwrap();
    assert_eq!(
        journal.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2],
        "each instance keeps its order"
    );
    assert_eq!(count(&pool, &dep_b(), b).await, 1);
}

/// The retention sweep's media lookup reads the V204 partial index, never the whole journal: on a
/// journal dominated by non-media events it is one probe of a small index, where a sequential scan
/// would read every row on every tick.
#[ignore = "docker"]
#[tokio::test]
async fn the_expired_media_lookup_uses_the_media_index_not_a_journal_scan() {
    let pool = fresh_audit_pool().await;
    // 200 000 ordinary events over 200 instances (the journal-ordinal trigger takes one advisory
    // lock per instance, so a statement must not touch too many), plus a handful of media events
    // (one already erased).
    sqlx::query(
        "INSERT INTO audit_event (deployment_id, instance_id, seq, at, event_type, payload_json) \
         SELECT $1, md5((g / 1000)::text)::uuid, g, now(), 'NODE_COMPLETED', '{\"x\":1}' \
         FROM generate_series(1, 200000) g",
    )
    .bind(dep_a().as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO audit_event (deployment_id, instance_id, seq, at, event_type, payload_json) \
         SELECT $1, gen_random_uuid(), 2, now(), 'NODE_COMPLETED', \
                CASE WHEN g = 1 THEN '{\"media\":[],\"mediaErasure\":{}}' ELSE '{\"media\":[]}' END \
         FROM generate_series(1, 5) g",
    )
    .bind(dep_a().as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE audit_event")
        .execute(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    sqlx::query(&format!(
        "PREPARE expired_media(text, bigint, float8, bigint) AS {}",
        sutra_persistence::stores::list_media_page_sql()
    ))
    .execute(&mut *conn)
    .await
    .unwrap();
    // A generic plan, as the engine's prepared statement gets once it is reused.
    sqlx::query("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *conn)
        .await
        .unwrap();
    let plan: Vec<String> = sqlx::query_scalar(&format!(
        "EXPLAIN EXECUTE expired_media('{}', 0, 604800, 1000)",
        dep_a().as_str()
    ))
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let plan = plan.join("\n");
    assert!(plan.contains("audit_event_media_pending"), "{plan}");
    assert!(!plan.contains("Seq Scan on audit_event"), "{plan}");
}

/// A row of attempt `id` from `base`, begun `secs` seconds into the test.
fn attempt_row(
    instance: Uuid,
    seq: i32,
    node: &str,
    id: &str,
    base: i32,
    secs: i64,
    spool_ref: &str,
) -> AuditEventRow {
    AuditEventRow {
        node_id: Some(node.to_string()),
        spool_ref: Some(spool_ref.to_string()),
        attempt: Some(sutra_persistence::stores::AuditRowAttempt {
            id: id.to_string(),
            base,
            at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1_800_000_000 + secs),
        }),
        ..row(&dep_a(), instance, seq, "NODE_LEFT")
    }
}

async fn journal(pool: &sqlx::PgPool, instance: Uuid) -> Vec<(i32, String)> {
    sqlx::query_as(
        "SELECT seq, node_id FROM audit_event WHERE deployment_id = $1 AND instance_id = $2 \
         ORDER BY seq",
    )
    .bind(dep_a().as_str())
    .bind(instance)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// A step re-run from the same base after a lost commit took another path. The
/// re-run supersedes the lost attempt entirely (its extra seq too), a re-ship changes nothing,
/// and the next pass numbers on without colliding with the lost attempt's seqs.
#[ignore = "docker"]
#[tokio::test]
async fn replayed_step_with_different_path_keeps_committed_audit() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let i = Uuid::new_v4();
    store
        .insert_shipped(&[
            attempt_row(i, 1, "Start", "a0", 0, 0, "s:1"),
            attempt_row(i, 2, "Park", "a0", 0, 0, "s:2"),
            attempt_row(i, 3, "TimerPath", "a1", 2, 10, "s:3"),
            attempt_row(i, 4, "TimerPath2", "a1", 2, 10, "s:4"),
        ])
        .await
        .unwrap();
    let rerun = [attempt_row(i, 3, "ReplyPath", "a2", 2, 20, "s:5")];
    store.insert_shipped(&rerun).await.unwrap();
    store.insert_shipped(&rerun).await.unwrap();
    assert_eq!(
        journal(&pool, i).await,
        vec![
            (1, "Start".into()),
            (2, "Park".into()),
            (3, "ReplyPath".into())
        ]
    );
    // The next pass, from the committed high-water 3: seq 4 is free again.
    store
        .insert_shipped(&[attempt_row(i, 4, "End", "a3", 3, 30, "s:6")])
        .await
        .unwrap();
    assert_eq!(journal(&pool, i).await.last(), Some(&(4, "End".into())));
}

/// Both attempts in one shipped batch: only the later one is written.
#[ignore = "docker"]
#[tokio::test]
async fn crash_before_commit_keeps_only_the_rerun() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let i = Uuid::new_v4();
    store
        .insert_shipped(&[
            attempt_row(i, 1, "Lost", "a1", 0, 0, "s:1"),
            attempt_row(i, 2, "Lost2", "a1", 0, 0, "s:2"),
            attempt_row(i, 1, "Kept", "a2", 0, 5, "s:3"),
        ])
        .await
        .unwrap();
    assert_eq!(journal(&pool, i).await, vec![(1, "Kept".into())]);
}

/// A dead replica's spool, shipped after the re-run on another replica committed,
/// brings the earlier attempt late: it is dropped.
#[ignore = "docker"]
#[tokio::test]
async fn late_superseded_attempt_from_another_spool_is_dropped() {
    let pool = fresh_audit_pool().await;
    let store = PgAuditEventStore::new(pool.clone());
    let i = Uuid::new_v4();
    store
        .insert_shipped(&[attempt_row(i, 1, "Kept", "b-later", 0, 50, "spool-b:1")])
        .await
        .unwrap();
    store
        .insert_shipped(&[
            attempt_row(i, 1, "Lost", "a-earlier", 0, 10, "spool-a:1"),
            attempt_row(i, 2, "Lost2", "a-earlier", 0, 10, "spool-a:2"),
        ])
        .await
        .unwrap();
    assert_eq!(journal(&pool, i).await, vec![(1, "Kept".into())]);
}
