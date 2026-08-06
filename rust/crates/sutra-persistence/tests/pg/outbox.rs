//! `PgOutboxStore` against a real PostgreSQL: enqueue/claim, claim honouring deployment
//! isolation, due time, the max-entries limit and ascending `next_attempt_at` order, delete
//! and defer (which bumps the attempt count), concurrent claims skipping locked rows, and
//! V604's terminal `poisoned` flag (the `sutra.outbox.retry.max-attempts` ceiling).

use std::collections::BTreeMap;

use sutra_persistence::stores::{OutboxEntry, OutboxStore, PgOutboxStore, ReplyMode};
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);
    store.delete(&dep_a(), Uuid::new_v4()).await.unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_due_orders_by_next_attempt_at_ascending() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool);
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
    let store = PgOutboxStore::new(pool);

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
    let store = PgOutboxStore::new(pool.clone());
    for _ in 0..4 {
        store.enqueue(&entry(Uuid::new_v4())).await.unwrap();
    }

    // Replica 1 claims two rows and HOLDS its transaction open.
    let mut tx1 = sutra_persistence::scope::begin_deployment_tx(&pool, &dep_a())
        .await
        .unwrap();
    let first = PgOutboxStore::claim_due_in(&mut tx1, &dep_a(), now_micros(), 2)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);

    // Replica 2, concurrently, must skip the locked rows and claim the OTHER two — never
    // blocking, never double-claiming.
    let mut tx2 = sutra_persistence::scope::begin_deployment_tx(&pool, &dep_a())
        .await
        .unwrap();
    let second = PgOutboxStore::claim_due_in(&mut tx2, &dep_a(), now_micros(), 10)
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

// ---- V604: the terminal `poisoned` flag ------------------------------------------------------

#[ignore = "docker"]
#[tokio::test]
async fn mark_poisoned_makes_a_row_unclaimable_without_deleting_it() {
    // The contract that keeps "we gave up" from becoming "it silently vanished": the row stops
    // being claimed, but it survives with its payload and its final diagnostic intact, so an
    // operator can inspect it and clearing the flag re-arms delivery.
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let e = entry(Uuid::new_v4());
    store.enqueue(&e).await.unwrap();

    assert_eq!(
        store
            .claim_due(&dep_a(), now_micros(), 10)
            .await
            .unwrap()
            .len(),
        1,
        "claimable before the mark"
    );

    store
        .mark_poisoned(
            &dep_a(),
            e.entry_id,
            Some("{\"code\":\"SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED\"}"),
        )
        .await
        .unwrap();

    assert!(
        store
            .claim_due(&dep_a(), now_micros(), 10)
            .await
            .unwrap()
            .is_empty(),
        "a poisoned row is never claimed again"
    );
    let (poisoned, diagnostic): (bool, Option<String>) = sqlx::query_as(
        "SELECT poisoned, last_diagnostic_json FROM outbox_entry WHERE entry_id = $1",
    )
    .bind(e.entry_id)
    .fetch_one(&pool)
    .await
    .expect("the row still exists — terminal is not deleted");
    assert!(poisoned);
    assert!(diagnostic
        .unwrap()
        .contains("SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED"));
}

#[ignore = "docker"]
#[tokio::test]
async fn mark_poisoned_neither_defers_nor_bumps_the_attempt_count() {
    // The final attempt count is the honest record of how many deliveries were tried; the mark
    // must not inflate it, and must not move a due time that no longer means anything.
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool.clone());
    let e = entry(Uuid::new_v4());
    store.enqueue(&e).await.unwrap();
    store
        .defer(&dep_a(), e.entry_id, now_micros(), None)
        .await
        .unwrap();

    let before: (i32, time::OffsetDateTime) = sqlx::query_as(
        "SELECT attempt_count, next_attempt_at FROM outbox_entry WHERE entry_id = $1",
    )
    .bind(e.entry_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    store
        .mark_poisoned(&dep_a(), e.entry_id, None)
        .await
        .unwrap();

    let after: (i32, time::OffsetDateTime) = sqlx::query_as(
        "SELECT attempt_count, next_attempt_at FROM outbox_entry WHERE entry_id = $1",
    )
    .bind(e.entry_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after.0, before.0, "attempt count unchanged");
    assert_eq!(after.1, before.1, "due time unchanged");
}

#[ignore = "docker"]
#[tokio::test]
async fn a_poisoned_row_stops_counting_as_pending_work() {
    // `count_pending_for_deployment` is the quiescence half of the DRAINING retirement gate. A
    // terminal row is undelivered but will never progress, so counting it would pin its
    // deployment out of retirement forever — the gate asks "is work still moving", not "is the
    // table empty".
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool);
    let live = entry(Uuid::new_v4());
    let doomed = entry(Uuid::new_v4());
    store.enqueue(&live).await.unwrap();
    store.enqueue(&doomed).await.unwrap();
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        2
    );

    store
        .mark_poisoned(&dep_a(), doomed.entry_id, None)
        .await
        .unwrap();

    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        1,
        "only the row that can still make progress counts"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn mark_poisoned_is_deployment_scoped_and_a_missing_row_is_a_no_op() {
    let pool = fresh_pool().await;
    let store = PgOutboxStore::new(pool);
    let e = entry(Uuid::new_v4());
    store.enqueue(&e).await.unwrap();

    // Wrong deployment: the isolation column keeps the UPDATE from reaching the row.
    store
        .mark_poisoned(&dep_b(), e.entry_id, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_due(&dep_a(), now_micros(), 10)
            .await
            .unwrap()
            .len(),
        1,
        "a cross-deployment mark must not touch the row"
    );

    // Unknown id: no row, no error (the delivery raced a redrive/delete).
    store
        .mark_poisoned(&dep_a(), Uuid::new_v4(), None)
        .await
        .unwrap();
}
