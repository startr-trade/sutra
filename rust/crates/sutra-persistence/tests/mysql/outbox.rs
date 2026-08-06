//! Mirrors `tests/pg/outbox.rs` (outbox claim/defer/delete + the SKIP LOCKED
//! concurrent-claim proof) on the MySQL dialect.

use std::collections::BTreeMap;

use sutra_persistence::mysql::stores::MySqlOutboxStore;
use sutra_persistence::stores::{OutboxEntry, OutboxStore, ReplyMode};
use time::Duration;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool, now_micros};

fn entry(instance_id: Uuid) -> OutboxEntry {
    let now = now_micros();
    OutboxEntry {
        deployment: dep_a(),
        entry_id: Uuid::new_v4(),
        instance_id,
        body: b"{\"answer\":42}".to_vec().into(),
        content_type: Some("application/json".to_owned()),
        destination: "https://consumer.example/callback".to_owned(),
        headers: BTreeMap::from([
            ("X-One".to_owned(), "1".to_owned()),
            ("X-Two".to_owned(), "2".to_owned()),
        ]),
        required: true,
        mode: ReplyMode::Native,
        outbox_key: format!("key-{instance_id}"),
        cloud_event_json: Some("{\"id\":\"ce-1\",\"source\":\"/sutra\"}".to_owned()),
        auth_ref_json: None,
        labels: BTreeMap::from([
            ("tenant".to_owned(), "acme".to_owned()),
            ("module".to_owned(), "payments".to_owned()),
        ]),
        created_at: now,
        next_attempt_at: now,
        attempt_count: 0,
        last_diagnostic_json: None,
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned()),
        node_id: None,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn enqueue_and_claim() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    let e = entry(Uuid::new_v4());

    store.enqueue(&e).await.unwrap();
    let claimed = store.claim_due(&dep_a(), now_micros(), 10).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0], e,
        "every column round-trips (traceparent + labels included)"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_respects_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    store.enqueue(&entry(Uuid::new_v4())).await.unwrap();

    assert!(store
        .claim_due(&dep_b(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_respects_due_time() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    let mut future = entry(Uuid::new_v4());
    future.next_attempt_at = now_micros() + Duration::minutes(5);
    store.enqueue(&future).await.unwrap();

    assert!(store
        .claim_due(&dep_a(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
    let later = now_micros() + Duration::minutes(6);
    assert_eq!(store.claim_due(&dep_a(), later, 10).await.unwrap().len(), 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_respects_max_entries_limit() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    for _ in 0..5 {
        store.enqueue(&entry(Uuid::new_v4())).await.unwrap();
    }

    assert_eq!(
        store
            .claim_due(&dep_a(), now_micros(), 3)
            .await
            .unwrap()
            .len(),
        3
    );
    assert!(store
        .claim_due(&dep_a(), now_micros(), 0)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_removes_row() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    let e = entry(Uuid::new_v4());
    store.enqueue(&e).await.unwrap();

    store.delete(&dep_a(), e.entry_id).await.unwrap();
    assert!(store
        .claim_due(&dep_a(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn defer_updates_next_attempt_and_increments_attempt_count() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    let e = entry(Uuid::new_v4());
    store.enqueue(&e).await.unwrap();

    let retry_at = now_micros() + Duration::seconds(30);
    store
        .defer(
            &dep_a(),
            e.entry_id,
            retry_at,
            Some("{\"code\":\"SUTRA.OUTBOUND.HTTP_502\"}"),
        )
        .await
        .unwrap();

    assert!(
        store
            .claim_due(&dep_a(), now_micros(), 10)
            .await
            .unwrap()
            .is_empty(),
        "deferred"
    );
    let claimed = store
        .claim_due(&dep_a(), retry_at + Duration::seconds(1), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempt_count, 1);
    assert_eq!(claimed[0].next_attempt_at, retry_at);
    assert_eq!(
        claimed[0].last_diagnostic_json.as_deref(),
        Some("{\"code\":\"SUTRA.OUTBOUND.HTTP_502\"}")
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_non_existent_is_noop() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    store.delete(&dep_a(), Uuid::new_v4()).await.unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_due_orders_by_next_attempt_at_ascending() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);
    let base = now_micros();
    let mut ids = Vec::new();
    for offset in [3i64, 1, 2] {
        let mut e = entry(Uuid::new_v4());
        e.next_attempt_at = base - Duration::seconds(offset);
        ids.push((offset, e.entry_id));
        store.enqueue(&e).await.unwrap();
    }

    let claimed = store.claim_due(&dep_a(), base, 10).await.unwrap();
    let claimed_ids: Vec<Uuid> = claimed.iter().map(|e| e.entry_id).collect();
    ids.sort_by_key(|(offset, _)| -offset); // oldest due first
    let expected: Vec<Uuid> = ids.iter().map(|(_, id)| *id).collect();
    assert_eq!(claimed_ids, expected);
}

#[ignore = "docker"]
#[tokio::test]
async fn count_pending_for_deployment_counts_every_undelivered_row() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool);

    let due = entry(Uuid::new_v4());
    let mut backed_off = entry(Uuid::new_v4());
    backed_off.next_attempt_at = now_micros() + Duration::hours(1);
    let mut other_deployment = entry(Uuid::new_v4());
    other_deployment.deployment = dep_b();
    for e in [&due, &backed_off, &other_deployment] {
        store.enqueue(e).await.unwrap();
    }

    // Row-exists = pending. The backed-off row is invisible to `claim_due` yet is still
    // undelivered work, so it must hold the DRAINING-retirement gate shut.
    let claimed = store.claim_due(&dep_a(), now_micros(), 10).await.unwrap();
    assert_eq!(claimed.len(), 1, "only the due row is claimable");
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        2
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1
    );

    store.delete(&dep_a(), due.entry_id).await.unwrap();
    store.delete(&dep_a(), backed_off.entry_id).await.unwrap();
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        0
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1,
        "the other deployment's row is untouched"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_skip_locked_rows() {
    let pool = fresh_pool().await;
    let store = MySqlOutboxStore::new(pool.clone());
    for _ in 0..4 {
        store.enqueue(&entry(Uuid::new_v4())).await.unwrap();
    }

    // Replica 1 claims two rows and HOLDS its transaction open.
    let mut tx1 = sutra_persistence::mysql::scope::begin_tx(&pool)
        .await
        .unwrap();
    let first = MySqlOutboxStore::claim_due_in(&mut tx1, &dep_a(), now_micros(), 2)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);

    // Replica 2, concurrently, must skip the locked rows and claim the OTHER two — never
    // blocking, never double-claiming.
    let mut tx2 = sutra_persistence::mysql::scope::begin_tx(&pool)
        .await
        .unwrap();
    let second = MySqlOutboxStore::claim_due_in(&mut tx2, &dep_a(), now_micros(), 10)
        .await
        .unwrap();
    assert_eq!(second.len(), 2);

    let ids1: Vec<Uuid> = first.iter().map(|e| e.entry_id).collect();
    for e in &second {
        assert!(
            !ids1.contains(&e.entry_id),
            "SKIP LOCKED must yield disjoint claims"
        );
    }

    tx1.commit().await.unwrap();
    tx2.commit().await.unwrap();
}
