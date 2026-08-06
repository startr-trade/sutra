//! `PgWaitStateStore` against a real PostgreSQL: record/resolve of a waiting node, resolved
//! rows being retained (not deleted) for audit, resolve-all on a terminal instance, idempotent
//! records, and `list_waiting` filtering by process and by deployment.

use sutra_persistence::stores::{
    PgWaitStateStore, WaitStateStore, WaitingFilter, STATUS_RESOLVED, STATUS_WAITING,
};
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

fn waiting_filter() -> WaitingFilter {
    WaitingFilter::default()
}

fn resolved_filter() -> WaitingFilter {
    WaitingFilter {
        status: Some(STATUS_RESOLVED.to_owned()),
        ..Default::default()
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn record_then_resolve() {
    let pool = fresh_pool().await;
    let store = PgWaitStateStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_waiting(&dep_a(), instance, "loan", "waitApproval", None)
        .await
        .unwrap();

    let waiting = store
        .list_waiting(&dep_a(), &waiting_filter(), 10, 0)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].instance_id, instance);
    assert_eq!(waiting[0].node_id, "waitApproval");
    assert_eq!(waiting[0].process_id, "loan");
    assert_eq!(waiting[0].status, STATUS_WAITING);
    assert!(waiting[0].resolved_at.is_none());

    store
        .resolve(&dep_a(), instance, "waitApproval")
        .await
        .unwrap();

    assert!(store
        .list_waiting(&dep_a(), &waiting_filter(), 10, 0)
        .await
        .unwrap()
        .is_empty());
    let resolved = store
        .list_waiting(&dep_a(), &resolved_filter(), 10, 0)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1, "RESOLVED rows are retained for audit");
    assert!(resolved[0].resolved_at.is_some());
}

#[ignore = "docker"]
#[tokio::test]
async fn resolve_all_on_terminal() {
    let pool = fresh_pool().await;
    let store = PgWaitStateStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_waiting(&dep_a(), instance, "loan", "waitA", None)
        .await
        .unwrap();
    store
        .record_waiting(&dep_a(), instance, "loan", "waitB", None)
        .await
        .unwrap();

    store.resolve_all(&dep_a(), instance).await.unwrap();

    assert!(store
        .list_waiting(&dep_a(), &waiting_filter(), 10, 0)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_waiting(&dep_a(), &resolved_filter(), 10, 0)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn record_is_idempotent() {
    let pool = fresh_pool().await;
    let store = PgWaitStateStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record_waiting(&dep_a(), instance, "loan", "waitA", None)
        .await
        .unwrap();
    store.resolve(&dep_a(), instance, "waitA").await.unwrap();
    // A redelivery re-parks the same token: back to WAITING, no duplicate, resolved_at cleared.
    store
        .record_waiting(&dep_a(), instance, "loan", "waitA", None)
        .await
        .unwrap();

    let waiting = store
        .list_waiting(&dep_a(), &waiting_filter(), 10, 0)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert!(waiting[0].resolved_at.is_none());
    assert!(store
        .list_waiting(&dep_a(), &resolved_filter(), 10, 0)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn filters_by_process() {
    let pool = fresh_pool().await;
    let store = PgWaitStateStore::new(pool);

    store
        .record_waiting(&dep_a(), Uuid::new_v4(), "loan", "w1", None)
        .await
        .unwrap();
    store
        .record_waiting(&dep_a(), Uuid::new_v4(), "onboarding", "w2", None)
        .await
        .unwrap();

    let filter = WaitingFilter {
        process_id: Some("loan".to_owned()),
        ..Default::default()
    };
    let events = store.list_waiting(&dep_a(), &filter, 10, 0).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].process_id, "loan");
}

#[ignore = "docker"]
#[tokio::test]
async fn deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgWaitStateStore::new(pool);

    store
        .record_waiting(&dep_a(), Uuid::new_v4(), "loan", "w1", None)
        .await
        .unwrap();

    assert!(store
        .list_waiting(&dep_b(), &waiting_filter(), 10, 0)
        .await
        .unwrap()
        .is_empty());
}
