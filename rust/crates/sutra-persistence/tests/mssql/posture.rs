//! Replaces `tests/pg/rls.rs` for this dialect: no security policy ships for
//! SQL Server (none exists in the shipped migration trees for any non-PG dialect), so
//! the posture is **documented enforced-bind-only** — the explicit `deployment_id`
//! bind on every store statement is the entire isolation. These tests pin the posture
//! from both directions, plus the BIN2 collation choices that keep comparison semantics
//! identical to the reference dialect (the server default collation is
//! case-insensitive).

use sutra_persistence::mssql::stores::{MssqlAliasStore, MssqlInboxStore, MssqlInstanceStore};
use sutra_persistence::stores::{AliasStore, InboxStore, InstanceState, InstanceStore};
use uuid::Uuid;

use crate::fixture::{count_all, dep_a, dep_b, fresh_pool};

#[ignore = "docker"]
#[tokio::test]
async fn store_surface_reads_are_deployment_scoped() {
    // The dialect's counterpart of the reference suite's store-level isolation proof:
    // every read path of the store surface carries the deployment bind.
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: b"scoped".to_vec(),
    };
    store.persist(&dep_a(), &s).await.unwrap();

    assert!(store.load(&dep_b(), s.instance_id).await.unwrap().is_none());
    assert_eq!(store.count_active(&dep_b()).await.unwrap(), 0);
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 1);
    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_some());
}

#[ignore = "docker"]
#[tokio::test]
async fn no_database_layer_filters_unscoped_reads() {
    // Documents the single-layer posture: unlike the reference dialect (whose
    // row-security policies filter even bind-less queries), a raw query WITHOUT the
    // deployment bind sees every deployment's rows here. The belt does not exist on
    // this dialect — only the store layer's braces (the enforced-bind-only posture).
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    for dep in [dep_a(), dep_b()] {
        store
            .persist(
                &dep,
                &InstanceState {
                    instance_id: Uuid::new_v4(),
                    serialised: b"row".to_vec(),
                },
            )
            .await
            .unwrap();
    }

    assert_eq!(
        count_all(&pool, "instance_state").await,
        2,
        "no database-layer isolation on this dialect — the explicit bind is load-bearing"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn alias_comparisons_are_byte_wise_case_sensitive() {
    // The reference dialect compares alias values byte-wise. The dialect schema pins
    // Latin1_General_100_BIN2 explicitly — under the server default (case-insensitive)
    // collation, 'ABC' and 'abc' would collide and unique-alias semantics would change.
    let pool = fresh_pool().await;
    let store = MssqlAliasStore::new(pool);
    let upper = Uuid::new_v4();
    let lower = Uuid::new_v4();

    assert!(store
        .record(&dep_a(), upper, "caseId", "ABC-1", true)
        .await
        .unwrap());
    assert!(
        store
            .record(&dep_a(), lower, "caseId", "abc-1", true)
            .await
            .unwrap(),
        "differently-cased values are DISTINCT unique aliases"
    );
    assert_eq!(
        store.find_live(&dep_a(), "caseId", "ABC-1").await.unwrap(),
        Some(upper)
    );
    assert_eq!(
        store.find_live(&dep_a(), "caseId", "abc-1").await.unwrap(),
        Some(lower)
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn inbox_event_ids_are_byte_wise_case_sensitive() {
    let pool = fresh_pool().await;
    let store = MssqlInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "EVT-1")
        .await
        .unwrap());
    assert!(
        store
            .record_seen(&dep_a(), "orders", "evt-1")
            .await
            .unwrap(),
        "differently-cased event ids are DIFFERENT events (BIN2 collation)"
    );
}
