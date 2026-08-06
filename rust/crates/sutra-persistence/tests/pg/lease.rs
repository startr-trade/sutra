//! `PgLeaseStore` against a real PostgreSQL: acquire/renew/release semantics — a second holder
//! is denied while the lease is live, the same holder re-acquires by renewing, renew and release
//! by a non-holder fail, an expired lease is re-acquirable, a zero TTL is rejected — and
//! exactly one winner under concurrent acquisition.

use std::time::Duration;

use sutra_persistence::stores::{LeaseStore, PgLeaseStore};

use crate::fixture::fresh_pool;

const TTL: Duration = Duration::from_secs(30);

async fn backdate_expiry(pool: &sqlx::PgPool, name: &str, secs: f64) {
    sqlx::query("UPDATE lease SET expires_at = now() - make_interval(secs => $1) WHERE name = $2")
        .bind(secs)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn acquire_fresh() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    let lease = store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.name, "timer-leader");
    assert_eq!(lease.holder, "replica-1");
    assert!(lease.expires_at > lease.acquired_at);
}

#[ignore = "docker"]
#[tokio::test]
async fn second_holder_denied_while_active() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    assert!(store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .is_some());
    assert!(store
        .try_acquire("timer-leader", "replica-2", TTL)
        .await
        .unwrap()
        .is_none());
    // The loser's attempt must not have overwritten the holder.
    assert_eq!(
        store.current("timer-leader").await.unwrap().unwrap().holder,
        "replica-1"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn same_holder_reacquire_renews_expiry() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    let first = store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .unwrap();
    assert!(
        second.expires_at > first.expires_at,
        "renewal-by-acquire extends expiry"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn renew_refreshes_expiry() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    let lease = store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(store.renew("timer-leader", "replica-1", TTL).await.unwrap());
    let renewed = store.current("timer-leader").await.unwrap().unwrap();
    assert!(renewed.expires_at > lease.expires_at);
}

#[ignore = "docker"]
#[tokio::test]
async fn renew_by_non_holder_fails() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    assert!(store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .is_some());
    assert!(!store.renew("timer-leader", "replica-2", TTL).await.unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn acquire_after_expiry() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool.clone());

    assert!(store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .is_some());
    backdate_expiry(&pool, "timer-leader", 5.0).await;

    let taken = store
        .try_acquire("timer-leader", "replica-2", TTL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        taken.holder, "replica-2",
        "expired lease is up for takeover"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn release_frees_the_lease() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    assert!(store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .is_some());
    store.release("timer-leader", "replica-1").await.unwrap();

    assert!(store.current("timer-leader").await.unwrap().is_none());
    assert!(store
        .try_acquire("timer-leader", "replica-2", TTL)
        .await
        .unwrap()
        .is_some());
}

#[ignore = "docker"]
#[tokio::test]
async fn release_by_non_holder_is_noop() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);

    assert!(store
        .try_acquire("timer-leader", "replica-1", TTL)
        .await
        .unwrap()
        .is_some());
    store.release("timer-leader", "replica-2").await.unwrap();

    assert_eq!(
        store.current("timer-leader").await.unwrap().unwrap().holder,
        "replica-1",
        "a non-holder release must not free the lease"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn current_on_unknown_is_empty() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);
    assert!(store.current("never-acquired").await.unwrap().is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn zero_ttl_rejected() {
    let pool = fresh_pool().await;
    let store = PgLeaseStore::new(pool);
    assert!(store
        .try_acquire("timer-leader", "replica-1", Duration::ZERO)
        .await
        .is_err());
    assert!(store
        .renew("timer-leader", "replica-1", Duration::ZERO)
        .await
        .is_err());
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_acquire_single_winner() {
    let pool = fresh_pool().await;

    let mut handles = Vec::new();
    for i in 0..8u32 {
        let store = PgLeaseStore::new(pool.clone());
        handles.push(tokio::spawn(async move {
            store
                .try_acquire("contended-leader", &format!("replica-{i}"), TTL)
                .await
                .unwrap()
                .is_some()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(
        winners, 1,
        "the single-round-trip acquire admits exactly one holder"
    );
}
