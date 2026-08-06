//! GDPR subject blind index (`subject_index`, V1101).
//! The key property under test is that disclosure (`find_instances`) spans BOTH
//! live and retired rows: an erasure request must still find an instance's subject-value
//! history after that instance has gone terminal.

use sutra_persistence::stores::{PgSubjectIndexStore, SubjectIndexStore};
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

/// The erasure semantics end to end via the real HMAC blind: index a subject value the way the
/// persist path does, DISCLOSE it (`find_instances`), hard-DELETE, then confirm disclosure is empty.
#[ignore = "docker"]
#[tokio::test]
async fn blind_disclose_then_erase_leaves_disclosure_empty() {
    use sutra_crypto::{HkdfKeyProvider, KeyProvider};

    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let instance_id = Uuid::new_v4();

    // Index the subject value under its blind (exactly as the engine bridge does at persist).
    let indexer = HkdfKeyProvider::new(b"it-master")
        .blind_index_key("tenant-a")
        .unwrap();
    let blind = indexer.blind("cust-42");
    store
        .record(&dep_a(), instance_id, "customerId", &blind)
        .await
        .unwrap();

    // Disclosure (GDPR requirement 6) finds it by the SAME blind an admin recomputes.
    assert_eq!(
        store
            .find_instances(&dep_a(), "customerId", &blind)
            .await
            .unwrap(),
        vec![instance_id]
    );

    // Erasure hard-deletes the subject rows; disclosure is then empty (the data is gone, not hidden).
    store.delete(&dep_a(), instance_id).await.unwrap();
    assert!(store
        .find_instances(&dep_a(), "customerId", &blind)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn record_then_find_instances() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record(&dep_a(), instance, "customerId", "deadbeef")
        .await
        .unwrap();

    assert_eq!(
        store
            .find_instances(&dep_a(), "customerId", "deadbeef")
            .await
            .unwrap(),
        vec![instance]
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn disclosure_spans_retired_rows() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record(&dep_a(), instance, "customerId", "cafef00d")
        .await
        .unwrap();
    store.retire(&dep_a(), instance).await.unwrap();

    // The whole point of the subject index: an erasure request must still find a RETIRED
    // instance's subject-value history. Unlike alias_index's live-only correlation lookup,
    // there is no `live` filter on the disclosure path.
    assert_eq!(
        store
            .find_instances(&dep_a(), "customerId", "cafef00d")
            .await
            .unwrap(),
        vec![instance],
        "disclosure must still find a retired instance"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn cross_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let instance = Uuid::new_v4();

    store
        .record(&dep_a(), instance, "customerId", "1234abcd")
        .await
        .unwrap();

    assert!(store
        .find_instances(&dep_b(), "customerId", "1234abcd")
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn find_instances_miss_is_empty() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);

    assert!(store
        .find_instances(&dep_a(), "nope", "0000000000000000")
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn two_instances_share_a_subject_value() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();

    // Unlike alias_index's unique-live guarantee, a subject value is expected to recur across
    // many instances (the same customer opening multiple cases) — both must be disclosed.
    store
        .record(&dep_a(), first, "customerId", "shared-value")
        .await
        .unwrap();
    store
        .record(&dep_a(), second, "customerId", "shared-value")
        .await
        .unwrap();

    let mut found = store
        .find_instances(&dep_a(), "customerId", "shared-value")
        .await
        .unwrap();
    found.sort();
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(found, expected);
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_removes_only_the_targeted_instance() {
    let pool = fresh_pool().await;
    let store = PgSubjectIndexStore::new(pool);
    let erased = Uuid::new_v4();
    let other = Uuid::new_v4();

    // Two subject rows for the instance being erased.
    store
        .record(&dep_a(), erased, "customerId", "erase-me")
        .await
        .unwrap();
    store
        .record(&dep_a(), erased, "loanId", "erase-me-too")
        .await
        .unwrap();
    // A DIFFERENT instance's row must survive the erasure untouched.
    store
        .record(&dep_a(), other, "customerId", "erase-me")
        .await
        .unwrap();

    store.delete(&dep_a(), erased).await.unwrap();

    assert_eq!(
        store
            .find_instances(&dep_a(), "customerId", "erase-me")
            .await
            .unwrap(),
        vec![other],
        "the other instance's row must still be discoverable"
    );
    assert!(
        store
            .find_instances(&dep_a(), "loanId", "erase-me-too")
            .await
            .unwrap()
            .is_empty(),
        "the erased instance's row is gone entirely, unlike retire's soft flip"
    );
}
