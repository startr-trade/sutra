//! Mirrors `tests/pg/channel.rs` (per-channel concurrency substrate) on the
//! SQL Server dialect.

use sutra_persistence::mssql::stores::MssqlChannelConcurrencyStore;
use sutra_persistence::stores::ChannelConcurrencyStore;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

#[ignore = "docker"]
#[tokio::test]
async fn started_then_terminal() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_started(&dep_a(), instance, "voice")
        .await
        .unwrap();
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", false)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", true)
            .await
            .unwrap(),
        1
    );

    store.record_terminal(&dep_a(), instance).await.unwrap();
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", false)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", true)
            .await
            .unwrap(),
        0
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn waiting_counted_only_when_include_waiting() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_started(&dep_a(), instance, "voice")
        .await
        .unwrap();
    store.record_suspended(&dep_a(), instance).await.unwrap();

    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", false)
            .await
            .unwrap(),
        0,
        "in-flight-only cap ignores parked instances"
    );
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", true)
            .await
            .unwrap(),
        1,
        "VoIP-style cap counts a held call's line"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn resumed_returns_to_in_flight() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_started(&dep_a(), instance, "voice")
        .await
        .unwrap();
    store.record_suspended(&dep_a(), instance).await.unwrap();
    store.record_resumed(&dep_a(), instance).await.unwrap();

    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", false)
            .await
            .unwrap(),
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn per_channel_isolation() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);

    store
        .record_started(&dep_a(), Uuid::new_v4(), "voice")
        .await
        .unwrap();
    store
        .record_started(&dep_a(), Uuid::new_v4(), "chat")
        .await
        .unwrap();

    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", false)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "chat", false)
            .await
            .unwrap(),
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn per_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);

    store
        .record_started(&dep_a(), Uuid::new_v4(), "voice")
        .await
        .unwrap();

    assert_eq!(
        store
            .count_active_by_channel(&dep_b(), "voice", false)
            .await
            .unwrap(),
        0
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn record_started_is_idempotent() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_started(&dep_a(), instance, "voice")
        .await
        .unwrap();
    // Redelivery re-dispatches the same instance — resets, does not duplicate; a channel
    // change follows the redelivery.
    store
        .record_started(&dep_a(), instance, "chat")
        .await
        .unwrap();

    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", true)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "chat", true)
            .await
            .unwrap(),
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn transitions_on_unknown_are_noops() {
    let pool = fresh_pool().await;
    let store = MssqlChannelConcurrencyStore::new(pool);
    let unknown = Uuid::new_v4();

    store.record_suspended(&dep_a(), unknown).await.unwrap();
    store.record_resumed(&dep_a(), unknown).await.unwrap();
    store.record_terminal(&dep_a(), unknown).await.unwrap();
    assert_eq!(
        store
            .count_active_by_channel(&dep_a(), "voice", true)
            .await
            .unwrap(),
        0
    );
}
