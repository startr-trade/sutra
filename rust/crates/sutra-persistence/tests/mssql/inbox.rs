//! Mirrors `tests/pg/inbox.rs` (first-observer inbox dedup) on the SQL Server
//! dialect.

use std::time::Duration;

use sutra_persistence::mssql::stores::MssqlInboxStore;
use sutra_persistence::mssql::MssqlPool;
use sutra_persistence::stores::InboxStore;
use sutra_persistence::DeploymentId;

use crate::fixture::{dep_a, dep_b, fresh_pool};

async fn insert_with_seen_at(
    pool: &MssqlPool,
    dep: &DeploymentId,
    channel: &str,
    event_id: &str,
    age_secs: f64,
) {
    let millis = (age_secs * 1000.0) as i32;
    let mut conn = pool.acquire().await.unwrap();
    conn.client()
        .execute(
            "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
             VALUES (@P1, @P2, @P3, DATEADD(MILLISECOND, -@P4, SYSUTCDATETIME()))",
            &[&dep.as_str(), &channel, &event_id, &millis],
        )
        .await
        .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn first_sight_vs_duplicate() {
    let pool = fresh_pool().await;
    let store = MssqlInboxStore::new(pool);

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
    let store = MssqlInboxStore::new(pool);

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
    let store = MssqlInboxStore::new(pool);

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
    let store = MssqlInboxStore::new(pool.clone());

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
        let store = MssqlInboxStore::new(pool.clone());
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
