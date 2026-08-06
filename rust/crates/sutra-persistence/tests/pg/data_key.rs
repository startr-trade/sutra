//! Wrapped-DEK store (`data_key`, V1301) — boot-load list + provisioning/rotation upsert. Not
//! deployment-scoped (infra store, like `deployment_archive`); keyed by the crypto `key_id`.

use sutra_crypto::WrappedDataKey;
use sutra_persistence::stores::PgDataKeyStore;

use crate::fixture::fresh_pool;

#[ignore = "docker"]
#[tokio::test]
async fn upsert_then_list_all_roundtrips_and_rotates_in_place() {
    let pool = fresh_pool().await;
    let store = PgDataKeyStore::new(pool.clone());

    assert!(
        store.list_all().await.unwrap().is_empty(),
        "fresh store is empty"
    );

    store
        .upsert(&WrappedDataKey::from_parts("tenant-a", vec![1, 2, 3]))
        .await
        .unwrap();
    store
        .upsert(&WrappedDataKey::from_parts("tenant-b", vec![4, 5, 6]))
        .await
        .unwrap();

    let all = store.list_all().await.unwrap();
    assert_eq!(all.len(), 2);
    // Deterministic order (ORDER BY key_id) — the boot-load map is stable.
    assert_eq!(all[0].key_id(), "tenant-a");
    assert_eq!(all[0].as_bytes(), &[1, 2, 3]);
    assert_eq!(all[1].key_id(), "tenant-b");
    assert_eq!(all[1].as_bytes(), &[4, 5, 6]);

    // Rotation: same key_id, new wrapped bytes — replaces in place, does not append.
    store
        .upsert(&WrappedDataKey::from_parts("tenant-a", vec![7, 8, 9]))
        .await
        .unwrap();
    let all = store.list_all().await.unwrap();
    assert_eq!(all.len(), 2, "rotation replaces, not appends");
    assert_eq!(all[0].key_id(), "tenant-a");
    assert_eq!(all[0].as_bytes(), &[7, 8, 9]);
}
