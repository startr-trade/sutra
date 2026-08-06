//! `PgInboxStore` against a real PostgreSQL: `record_seen` returning true only on first sight,
//! per-deployment and per-channel isolation of the dedupe key, age-based pruning across
//! deployments, and exactly one winner under concurrent first sight of the same event id.
//! A negative prune age is unrepresentable — `std::time::Duration` is unsigned.

use std::time::Duration;

use sutra_persistence::stores::{InboxStore, PgInboxStore};
use sutra_persistence::DeploymentId;

use crate::fixture::{dep_a, dep_b, fresh_pool};

async fn insert_with_seen_at(
    pool: &sqlx::PgPool,
    dep: &DeploymentId,
    channel: &str,
    event_id: &str,
    age_secs: f64,
) {
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
         VALUES ($1, $2, $3, now() - make_interval(secs => $4))",
    )
    .bind(dep.as_str())
    .bind(channel)
    .bind(event_id)
    .bind(age_secs)
    .execute(pool)
    .await
    .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn first_sight_vs_duplicate() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert!(!store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn cross_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    // The same (channel, event) is FRESH for deployment B.
    assert!(store
        .record_seen(&dep_b(), "orders", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn different_channel_is_fresh() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert!(store
        .record_seen(&dep_a(), "payments", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn prune_removes_old_rows_across_deployments() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool.clone());

    insert_with_seen_at(&pool, &dep_a(), "orders", "old-a", 7200.0).await;
    insert_with_seen_at(&pool, &dep_b(), "orders", "old-b", 7200.0).await;
    insert_with_seen_at(&pool, &dep_a(), "orders", "new-a", 10.0).await;

    // The cross-deployment maintenance op prunes BOTH deployments' stale rows.
    let pruned = store
        .prune_older_than(Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(pruned, 2);

    // The fresh row survives — and stays deduplicated.
    assert!(!store
        .record_seen(&dep_a(), "orders", "new-a")
        .await
        .unwrap());
    // The pruned ones are forgotten (fresh again) — the dedup window moved on.
    assert!(store
        .record_seen(&dep_a(), "orders", "old-a")
        .await
        .unwrap());
    assert!(store
        .record_seen(&dep_b(), "orders", "old-b")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_sight_exactly_one_winner() {
    let pool = fresh_pool().await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = PgInboxStore::new(pool.clone());
        handles.push(tokio::spawn(async move {
            store
                .record_seen(&dep_a(), "orders", "contended-evt")
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "first-observer-wins admits exactly one");
}
