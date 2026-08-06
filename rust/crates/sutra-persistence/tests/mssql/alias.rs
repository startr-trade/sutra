//! Mirrors `tests/pg/alias.rs` (unique-LIVE alias guarantee) on the SQL Server
//! dialect — here the guarantee rides the FILTERED unique index of the dialect's V101
//! (the 1:1 mapping of the reference's partial index).

use sutra_persistence::mssql::stores::MssqlAliasStore;
use sutra_persistence::stores::AliasStore;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

#[ignore = "docker"]
#[tokio::test]
async fn non_unique_write_read_back() {
    let pool = fresh_pool().await;
    let store = MssqlAliasStore::new(pool);
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
    let store = MssqlAliasStore::new(pool);
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
    let store = MssqlAliasStore::new(pool);
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
    let store = MssqlAliasStore::new(pool);
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
    let store = MssqlAliasStore::new(pool);
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
    // The filtered unique index no longer covers the retired row: a NEW instance can
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
    let store = MssqlAliasStore::new(pool);
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
    let store = MssqlAliasStore::new(pool);
    assert_eq!(
        store.find_live(&dep_a(), "nope", "missing").await.unwrap(),
        None
    );
}
