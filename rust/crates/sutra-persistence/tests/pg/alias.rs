//! `PgAliasStore` against a real PostgreSQL: non-unique write/read-back, unique-alias collision
//! detection and same-owner idempotent retry, per-deployment isolation, multiple aliases per
//! instance, `find_live` misses, and retire vs. hard delete.

use sutra_persistence::stores::{AliasStore, PgAliasStore};
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

#[ignore = "docker"]
#[tokio::test]
async fn non_unique_write_read_back() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "orderId", "ORD-1", false)
        .await
        .unwrap());
    assert_eq!(
        store.find_live(&dep_a(), "orderId", "ORD-1").await.unwrap(),
        Some(instance)
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn unique_collision_detected() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), first, "loanId", "L-9", true)
        .await
        .unwrap());
    assert!(
        !store
            .record(&dep_a(), second, "loanId", "L-9", true)
            .await
            .unwrap(),
        "a DIFFERENT live instance owns the unique alias"
    );
    // The original owner still resolves.
    assert_eq!(
        store.find_live(&dep_a(), "loanId", "L-9").await.unwrap(),
        Some(first)
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn unique_idempotent_retry() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "loanId", "L-1", true)
        .await
        .unwrap());
    assert!(
        store
            .record(&dep_a(), instance, "loanId", "L-1", true)
            .await
            .unwrap(),
        "same instance re-attempting is idempotent success"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn cross_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "orderId", "ORD-7", true)
        .await
        .unwrap());

    assert_eq!(
        store.find_live(&dep_b(), "orderId", "ORD-7").await.unwrap(),
        None
    );
    // The same (name, value) is FREE in deployment B — isolation includes uniqueness scope.
    let other = Uuid::new_v4();
    assert!(store
        .record(&dep_b(), other, "orderId", "ORD-7", true)
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn retire_makes_alias_unusable() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "caseId", "C-1", true)
        .await
        .unwrap());
    store.retire(&dep_a(), instance).await.unwrap();

    assert_eq!(
        store.find_live(&dep_a(), "caseId", "C-1").await.unwrap(),
        None
    );
    // The unique-live partial index no longer covers the retired row: a NEW instance can
    // claim the same alias.
    let successor = Uuid::new_v4();
    assert!(store
        .record(&dep_a(), successor, "caseId", "C-1", true)
        .await
        .unwrap());
    assert_eq!(
        store.find_live(&dep_a(), "caseId", "C-1").await.unwrap(),
        Some(successor)
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn multi_row_instance() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "orderId", "ORD-1", true)
        .await
        .unwrap());
    assert!(store
        .record(&dep_a(), instance, "customerId", "CUST-1", false)
        .await
        .unwrap());

    let rows = store.list_for(&dep_a(), instance).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.live));

    store.retire(&dep_a(), instance).await.unwrap();
    let rows = store.list_for(&dep_a(), instance).await.unwrap();
    assert!(
        rows.iter().all(|r| !r.live),
        "retire flips every row of the instance"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn find_live_miss() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    assert_eq!(
        store.find_live(&dep_a(), "nope", "missing").await.unwrap(),
        None
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_hard_removes_the_row_unlike_retire() {
    let pool = fresh_pool().await;
    let store = PgAliasStore::new(pool);
    let instance = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), instance, "orderId", "ORD-ERASE", true)
        .await
        .unwrap());

    store.delete(&dep_a(), instance).await.unwrap();

    assert_eq!(
        store
            .find_live(&dep_a(), "orderId", "ORD-ERASE")
            .await
            .unwrap(),
        None
    );
    // Hard delete removes the row entirely — contrast `retire`, which keeps the row (with
    // `live = FALSE`) so `list_for` still surfaces it.
    let rows = store.list_for(&dep_a(), instance).await.unwrap();
    assert!(
        rows.is_empty(),
        "delete must remove the row entirely, not just flag it live=false"
    );
}
