//! Deployment-archive store semantics (V1001) on the SQL Server dialect — mirrors the
//! reference `PgDeploymentArchiveStore` behaviour: a first deploy stores an active row, a
//! hot-deploy replaces the slot in place (prior active → draining, bumped per-slot revision,
//! exactly one active row per slot via the filtered unique index), a same-id rollback
//! re-activates in place, distinct slots coexist active, and get_bytes/retire_slot behave.

use sutra_persistence::mssql::stores::MssqlDeploymentArchiveStore;
use sutra_persistence::stores::{ArchiveStatus, NewArchive};

use crate::fixture::fresh_pool;

const ID1: &str = "dep-000000000000000000000001";
const ID2: &str = "dep-000000000000000000000002";
const SLOT: &str = "acme/payments.sutra";

fn archive(id: &str, slot: &str, body: &[u8]) -> NewArchive {
    NewArchive {
        deployment_id: id.to_owned(),
        slot: slot.to_owned(),
        tenant: "acme".to_owned(),
        module: "payments".to_owned(),
        version: "1.0.0".to_owned(),
        bytes: body.to_vec(),
        checksum: format!("sha256:{id}"),
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn first_deploy_stores_active_row() {
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    let rev = store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    assert_eq!(rev, 1);

    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].deployment_id, ID1);
    assert_eq!(active[0].slot, SLOT);
    assert_eq!(active[0].tenant, "acme");
    assert_eq!(active[0].module, "payments");
    assert_eq!(active[0].version, "1.0.0");
    assert_eq!(active[0].revision, 1);
    assert_eq!(active[0].bytes, b"one".to_vec());
}

#[ignore = "docker"]
#[tokio::test]
async fn hot_deploy_replaces_slot_in_place() {
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    let rev2 = store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();
    assert_eq!(rev2, 2, "revision bumps per slot");

    // Exactly one active row, carrying the new content.
    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].deployment_id, ID2);
    assert_eq!(active[0].revision, 2);
    assert_eq!(active[0].bytes, b"two".to_vec());

    // The prior row is retained as draining (audit history).
    let status = store.list_status().await.unwrap();
    assert_eq!(status.len(), 2);
    let draining: Vec<_> = status
        .iter()
        .filter(|s| s.status == ArchiveStatus::Draining)
        .collect();
    assert_eq!(draining.len(), 1);
    assert_eq!(draining[0].deployment_id, ID1);
}

#[ignore = "docker"]
#[tokio::test]
async fn redeploy_same_id_reactivates_in_place() {
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();
    // A rollback re-POSTs the original id — the same PK row re-activates (not a duplicate).
    let rev = store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    assert_eq!(rev, 3);

    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 1, "still exactly one active row per slot");
    assert_eq!(active[0].deployment_id, ID1);
    assert_eq!(active[0].revision, 3);

    let status = store.list_status().await.unwrap();
    assert_eq!(
        status.len(),
        2,
        "the re-deployed id row is reused, not duplicated"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn distinct_slots_coexist_active() {
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, "acme/payments.sutra", b"p"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, "acme/billing.sutra", b"b"))
        .await
        .unwrap();

    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 2, "distinct slots each keep an active row");
    assert!(
        active.iter().all(|a| a.revision == 1),
        "revision is per-slot, so both first deploys are revision 1"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn get_bytes_and_retire() {
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"payload"))
        .await
        .unwrap();

    assert_eq!(
        store.get_bytes(ID1).await.unwrap(),
        Some(b"payload".to_vec())
    );
    assert!(store
        .get_bytes("dep-does-not-exist")
        .await
        .unwrap()
        .is_none());

    assert!(
        store.retire_slot(SLOT).await.unwrap(),
        "active slot retires"
    );
    assert!(
        store.list_active().await.unwrap().is_empty(),
        "no active rows after retire"
    );
    assert!(
        !store.retire_slot(SLOT).await.unwrap(),
        "no active row to retire now"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn served_listing_carries_the_draining_tail_and_retires_it() {
    // Dialect parity for the served set: after a hot-deploy the demoted revision is still
    // listed (with bytes) so pinned instances keep resuming, and `retire_deployment` is the
    // terminal flip that drops it — guarded on `draining`, idempotent.
    let store = MssqlDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();

    let served = store.list_active_and_draining().await.unwrap();
    let ids: Vec<&str> = served
        .iter()
        .map(|r| r.archive.deployment_id.as_str())
        .collect();
    assert_eq!(ids, vec![ID2, ID1], "newest revision first within a slot");
    assert_eq!(served[0].status, ArchiveStatus::Active);
    assert_eq!(served[1].status, ArchiveStatus::Draining);
    assert_eq!(served[1].archive.bytes, b"one".to_vec());

    assert!(
        !store.retire_deployment(ID2).await.unwrap(),
        "the active revision is not eligible"
    );
    assert!(store.retire_deployment(ID1).await.unwrap());
    assert_eq!(store.list_active_and_draining().await.unwrap().len(), 1);
    assert!(
        !store.retire_deployment(ID1).await.unwrap(),
        "idempotent — a second sweep pass flips nothing"
    );
}
