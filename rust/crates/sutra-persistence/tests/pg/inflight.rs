//! `PgInflightChainStore` against a real PostgreSQL: the marker a synchronous-call chain writes
//! when it begins, its deletion inside the instance's final step, and the sweep that releases a
//! vanished holder's dedup rows and admission row — and leaves a live holder's chain alone.

use std::time::Duration;

use sutra_persistence::stores::{InboxStore, PgInboxStore, PgInflightChainStore};
use uuid::Uuid;

use crate::fixture::{dep_a, fresh_pool};

async fn markers(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM inflight_sync_chain")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[ignore = "docker"]
#[tokio::test]
async fn the_sweep_releases_a_dead_holders_dedup_rows_and_keeps_a_live_one() {
    let pool = fresh_pool().await;
    let inbox = PgInboxStore::new(pool.clone());
    let store = PgInflightChainStore::new(pool.clone());
    let dead = Uuid::new_v4();
    let live = Uuid::new_v4();

    assert!(inbox.record_seen(&dep_a(), "in", "evt-dead").await.unwrap());
    assert!(inbox.record_seen(&dep_a(), "in", "evt-live").await.unwrap());
    let dead_keys = [("in".to_string(), "evt-dead".to_string())];
    let live_keys = [("in".to_string(), "evt-live".to_string())];
    // A previous process on this host, and the current one.
    store
        .begin(&dep_a(), dead, &dead_keys, "host-1-111-aaaa")
        .await
        .unwrap();
    store
        .begin(&dep_a(), live, &live_keys, "host-1-222-bbbb")
        .await
        .unwrap();

    let released = store
        .sweep(
            &dep_a(),
            "host-1",
            "host-1-222-bbbb",
            Duration::from_secs(900),
        )
        .await
        .unwrap();
    assert_eq!(released, 1);
    // The dead chain's delivery is a first observation again; the live one is untouched.
    assert!(inbox.record_seen(&dep_a(), "in", "evt-dead").await.unwrap());
    assert!(!inbox.record_seen(&dep_a(), "in", "evt-live").await.unwrap());
    assert_eq!(markers(&pool).await, 1);

    // Past the age bound, any holder's marker is released.
    let released = store
        .sweep(
            &dep_a(),
            "other-host",
            "other-host-1-cccc",
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    assert_eq!(released, 1);
    assert_eq!(markers(&pool).await, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn clear_deletes_the_marker_and_is_idempotent() {
    let pool = fresh_pool().await;
    let store = PgInflightChainStore::new(pool.clone());
    let instance = Uuid::new_v4();
    store.begin(&dep_a(), instance, &[], "h-1-x").await.unwrap();
    assert_eq!(markers(&pool).await, 1);
    store.clear(&dep_a(), instance).await.unwrap();
    store.clear(&dep_a(), instance).await.unwrap();
    assert_eq!(markers(&pool).await, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn forget_releases_one_dedup_row() {
    let pool = fresh_pool().await;
    let inbox = PgInboxStore::new(pool);
    assert!(inbox.record_seen(&dep_a(), "in", "evt-1").await.unwrap());
    assert!(inbox.forget(&dep_a(), "in", "evt-1").await.unwrap());
    assert!(!inbox.forget(&dep_a(), "in", "evt-1").await.unwrap());
    assert!(inbox.record_seen(&dep_a(), "in", "evt-1").await.unwrap());
}
