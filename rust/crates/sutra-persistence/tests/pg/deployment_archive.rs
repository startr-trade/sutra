//! Deployment-archive store (V1001) on the reference PostgreSQL dialect, focused on the
//! part a hot-deploy depends on: the **DRAINING tail**.
//!
//! A hot-deploy demotes the slot's prior active row to `draining` in the same transaction
//! that activates the new content. Instances already in flight are PINNED to that demoted
//! deployment id, so the engine must keep serving its definition — which means the store has
//! to hand the drained row (and its sealed bytes) back on re-activation, and a fresh pod that
//! never saw the deploy has to be able to do the same. `list_active` deliberately cannot: it
//! is the live intake set. `list_active_and_draining` is the served set, and `get_bytes` is
//! the by-id re-hydration of a single archive. `retire_deployment` is the terminal flip the
//! retire-when-quiescent sweep performs once nothing is pinned any more.

use sutra_persistence::stores::{ArchiveStatus, NewArchive, PgDeploymentArchiveStore};

use crate::fixture::fresh_pool;

const ID1: &str = "dep-000000000000000000000001";
const ID2: &str = "dep-000000000000000000000002";
const SLOT: &str = "acme--payments--1.0.0";
const OTHER_SLOT: &str = "acme--billing--1.0.0";

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
async fn the_served_listing_carries_the_draining_tail_that_list_active_hides() {
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();

    // The live intake set is unchanged by this feature — one active row per slot.
    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].deployment_id, ID2);

    // The served set adds the demoted prior revision, WITH its bytes, so the engine can
    // re-plan it and keep pinned instances resumable.
    let served = store.list_active_and_draining().await.unwrap();
    let ids: Vec<&str> = served
        .iter()
        .map(|r| r.archive.deployment_id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec![ID2, ID1],
        "newest revision first within a slot — the order the relay's draining scope walk wants"
    );
    assert_eq!(served[0].status, ArchiveStatus::Active);
    assert_eq!(served[1].status, ArchiveStatus::Draining);
    assert_eq!(served[1].archive.bytes, b"one".to_vec());
    assert_eq!(served[1].archive.revision, 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn get_bytes_rehydrates_a_drained_archive_by_id() {
    // The by-id re-hydration path: after a hot-deploy the drained revision's sealed bytes are
    // still readable, byte-for-byte, which is what makes re-planning a pinned definition
    // possible on a replica that only knows the deployment id.
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"sealed-archive-bytes"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();

    assert_eq!(
        store.get_bytes(ID1).await.unwrap(),
        Some(b"sealed-archive-bytes".to_vec()),
        "the drained revision's bytes survive the flip"
    );
    assert!(store
        .get_bytes("dep-does-not-exist")
        .await
        .unwrap()
        .is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn retire_deployment_is_the_terminal_flip_of_a_drained_row() {
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();

    // An ACTIVE row is never retired out from under intake — the guard is `status='draining'`.
    assert!(
        !store.retire_deployment(ID2).await.unwrap(),
        "the active revision is not eligible"
    );

    assert!(
        store.retire_deployment(ID1).await.unwrap(),
        "the drained revision retires once quiescent"
    );
    let served = store.list_active_and_draining().await.unwrap();
    assert_eq!(served.len(), 1, "a retired row leaves the served set");
    assert_eq!(served[0].archive.deployment_id, ID2);

    // Idempotent: a second sweep pass over the same id flips nothing.
    assert!(!store.retire_deployment(ID1).await.unwrap());

    // ...and the row (with its bytes) is still there for audit / forensics.
    let status = store.list_status().await.unwrap();
    let retired: Vec<_> = status
        .iter()
        .filter(|s| s.status == ArchiveStatus::Retired)
        .collect();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].deployment_id, ID1);
    assert_eq!(store.get_bytes(ID1).await.unwrap(), Some(b"one".to_vec()));
}

#[ignore = "docker"]
#[tokio::test]
async fn an_undeployed_slot_still_serves_its_pinned_instances() {
    // Undeploy (`retire_slot`) demotes active → draining rather than deleting: the slot stops
    // taking new intake, but instances pinned to it stay resumable until the quiescence sweep
    // retires the row. The served listing is what carries that guarantee.
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, OTHER_SLOT, b"other"))
        .await
        .unwrap();
    assert!(store.retire_slot(SLOT).await.unwrap());

    let served = store.list_active_and_draining().await.unwrap();
    assert_eq!(served.len(), 2);
    let drained = served
        .iter()
        .find(|r| r.status == ArchiveStatus::Draining)
        .expect("the undeployed slot is draining, not gone");
    assert_eq!(drained.archive.deployment_id, ID1);
    assert_eq!(drained.archive.bytes, b"one".to_vec());
}

#[ignore = "docker"]
#[tokio::test]
async fn a_superseded_row_retains_its_channels_and_an_undeployed_one_does_not() {
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active_superseding(&archive(ID2, OTHER_SLOT, b"two"), &[SLOT.to_owned()])
        .await
        .unwrap();
    let served = store.list_active_and_draining().await.unwrap();
    let row = |id: &str| {
        served
            .iter()
            .find(|r| r.archive.deployment_id == id)
            .unwrap()
    };
    assert!(row(ID1).retains_channels, "superseded by another slot");
    assert!(!row(ID2).retains_channels, "the active row");

    // A replace in place supersedes too.
    let store2 = PgDeploymentArchiveStore::new(fresh_pool().await);
    store2
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store2
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();
    let served = store2.list_active_and_draining().await.unwrap();
    assert!(
        served[1].retains_channels,
        "{:?}",
        served[1].archive.deployment_id
    );

    // An undeploy does not, and neither does a row an older engine drained.
    let store3 = PgDeploymentArchiveStore::new(fresh_pool().await);
    store3
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    assert!(store3.retire_slot(SLOT).await.unwrap());
    let served = store3.list_active_and_draining().await.unwrap();
    assert!(!served[0].retains_channels);

    // A rollback re-activates the row and clears it.
    store2
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    let served = store2.list_active_and_draining().await.unwrap();
    let active = served
        .iter()
        .find(|r| r.status == ArchiveStatus::Active)
        .unwrap();
    assert_eq!(active.archive.deployment_id, ID1);
    assert!(!active.retains_channels);
}

#[ignore = "docker"]
#[tokio::test]
async fn reopen_retired_turns_a_retired_row_back_to_draining() {
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();
    assert!(store.retire_deployment(ID1).await.unwrap());
    assert!(store.reopen_retired(ID1).await.unwrap());
    let served = store.list_active_and_draining().await.unwrap();
    let reopened = served
        .iter()
        .find(|r| r.archive.deployment_id == ID1)
        .expect("served again");
    assert_eq!(reopened.status, ArchiveStatus::Draining);
    assert!(reopened.retains_channels, "it keeps what it had");
}

#[ignore = "docker"]
#[tokio::test]
async fn reopen_retired_touches_only_retired_rows() {
    let store = PgDeploymentArchiveStore::new(fresh_pool().await);
    store
        .upsert_active(&archive(ID1, SLOT, b"one"))
        .await
        .unwrap();
    store
        .upsert_active(&archive(ID2, SLOT, b"two"))
        .await
        .unwrap();
    assert!(!store.reopen_retired(ID2).await.unwrap(), "active");
    assert!(!store.reopen_retired(ID1).await.unwrap(), "draining");
    assert!(!store.reopen_retired("dep-does-not-exist").await.unwrap());
    let served = store.list_active_and_draining().await.unwrap();
    assert_eq!(served[0].status, ArchiveStatus::Active);
}
