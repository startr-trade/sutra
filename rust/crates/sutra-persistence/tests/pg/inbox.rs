//! `PgInboxStore` against a real PostgreSQL: `record_seen` returning true only on first sight,
//! per-deployment and per-channel isolation of the dedupe key, age-based pruning across
//! deployments, and exactly one winner under concurrent first sight of the same event id.
//! A negative prune age is unrepresentable — `std::time::Duration` is unsigned.

use std::time::Duration;

use sutra_persistence::stores::{
    InboxSeenState, InboxStore, PgDeploymentOwnerStore, PgInboxStore, PgInflightChainStore,
};
use sutra_persistence::DeploymentId;

use crate::fixture::{create_app_role, dep_a, dep_b, fresh_pool, fresh_pool_named, role_pool};

async fn insert_with_seen_at(
    pool: &sqlx::PgPool,
    dep: &DeploymentId,
    channel: &str,
    event_id: &str,
    age_secs: f64,
) {
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
         VALUES ($1, $2, $3, now() - make_interval(secs => $4))",
    )
    .bind(dep.as_str())
    .bind(channel)
    .bind(event_id)
    .bind(age_secs)
    .execute(pool)
    .await
    .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn first_sight_vs_duplicate() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert!(!store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn cross_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    // The same (channel, event) is FRESH for deployment B.
    assert!(store
        .record_seen(&dep_b(), "orders", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn different_channel_is_fresh() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert!(store
        .record_seen(&dep_a(), "payments", "evt-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn prune_removes_old_rows_across_deployments() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool.clone());

    insert_with_seen_at(&pool, &dep_a(), "orders", "old-a", 7200.0).await;
    insert_with_seen_at(&pool, &dep_b(), "orders", "old-b", 7200.0).await;
    insert_with_seen_at(&pool, &dep_a(), "orders", "new-a", 10.0).await;

    // The cross-deployment maintenance op prunes BOTH deployments' stale rows.
    let pruned = store
        .prune_older_than(Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(pruned, 2);

    // The fresh row survives — and stays deduplicated.
    assert!(!store
        .record_seen(&dep_a(), "orders", "new-a")
        .await
        .unwrap());
    // The pruned ones are forgotten (fresh again) — the dedup window moved on.
    assert!(store
        .record_seen(&dep_a(), "orders", "old-a")
        .await
        .unwrap());
    assert!(store
        .record_seen(&dep_b(), "orders", "old-b")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_sight_exactly_one_winner() {
    let pool = fresh_pool().await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = PgInboxStore::new(pool.clone());
        handles.push(tokio::spawn(async move {
            store
                .record_seen(&dep_a(), "orders", "contended-evt")
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "first-observer-wins admits exactly one");
}

// ---- dedup per owner (V305/V306) ------------------------------------------------------------

async fn map_owner(pool: &sqlx::PgPool, rows: &[(&DeploymentId, &str)]) {
    let rows: Vec<(String, String)> = rows
        .iter()
        .map(|(dep, owner)| (dep.as_str().to_string(), owner.to_string()))
        .collect();
    PgDeploymentOwnerStore::new(pool.clone())
        .upsert_many(&rows)
        .await
        .unwrap();
}

/// An unprivileged session, so the row-security policy is what decides visibility.
async fn app_role_pool(pool: &sqlx::PgPool, db: &str) -> sqlx::PgPool {
    let role = create_app_role(
        pool,
        &["inbox_seen", "deployment_owner", "inflight_sync_chain"],
    )
    .await;
    role_pool(db, &role).await
}

/// Two versions of one owner: a key the old version recorded is a duplicate on the new one, read
/// through the policy as an unprivileged role, and the instance it names is the old version's.
#[ignore = "docker"]
#[tokio::test]
async fn owner_rows_visible_across_versions_under_rls() {
    let (pool, db) = fresh_pool_named().await;
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-x")]).await;
    let store = PgInboxStore::new(app_role_pool(&pool, &db).await);
    let instance = uuid::Uuid::new_v4();

    assert!(store
        .record_seen_for(&dep_a(), "orders", "evt-1", Some(instance))
        .await
        .unwrap());
    assert_eq!(
        store.check(&dep_b(), "orders", "evt-1").await.unwrap(),
        InboxSeenState::Recorded
    );
    assert_eq!(
        store
            .original_instance(&dep_b(), "orders", "evt-1")
            .await
            .unwrap(),
        Some(instance)
    );
    // The partial unique index refuses the second version's write of the same key.
    assert!(!store
        .record_seen(&dep_b(), "orders", "evt-1")
        .await
        .unwrap());
    // Another event, or another channel, is fresh.
    assert_eq!(
        store.check(&dep_b(), "orders", "evt-2").await.unwrap(),
        InboxSeenState::Absent
    );
    assert_eq!(
        store.check(&dep_b(), "other-in", "evt-1").await.unwrap(),
        InboxSeenState::Absent
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn another_owners_row_is_invisible() {
    let (pool, db) = fresh_pool_named().await;
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-y")]).await;
    let store = PgInboxStore::new(app_role_pool(&pool, &db).await);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert_eq!(
        store.check(&dep_b(), "orders", "evt-1").await.unwrap(),
        InboxSeenState::Absent
    );
    assert!(store
        .record_seen(&dep_b(), "orders", "evt-1")
        .await
        .unwrap());
}

/// A deployment the engine has not mapped writes a NULL owner and dedupes within itself only: the
/// behaviour before V305.
#[ignore = "docker"]
#[tokio::test]
async fn no_mapping_means_per_deployment() {
    let (pool, db) = fresh_pool_named().await;
    let store = PgInboxStore::new(app_role_pool(&pool, &db).await);

    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert!(!store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    assert_eq!(
        store.check(&dep_b(), "orders", "evt-1").await.unwrap(),
        InboxSeenState::Absent
    );
    assert!(store
        .record_seen(&dep_b(), "orders", "evt-1")
        .await
        .unwrap());
    let owner: Option<String> =
        sqlx::query_scalar("SELECT owner_id FROM inbox_seen WHERE deployment_id = $1")
            .bind(dep_a().as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(owner, None);
}

/// A row written before the owner was mapped (a NULL owner) still dedupes in its own deployment
/// after the mapping arrives, and only there.
#[ignore = "docker"]
#[tokio::test]
async fn an_old_row_still_dedupes_in_its_deployment() {
    let pool = fresh_pool().await;
    let store = PgInboxStore::new(pool.clone());
    assert!(store
        .record_seen(&dep_a(), "orders", "evt-1")
        .await
        .unwrap());
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-x")]).await;

    assert_eq!(
        store.check(&dep_a(), "orders", "evt-1").await.unwrap(),
        InboxSeenState::Recorded
    );
    assert_eq!(
        store.check(&dep_b(), "orders", "evt-1").await.unwrap(),
        InboxSeenState::Absent
    );
}

/// A synchronous chain still holding the key on the old version makes the new version's check read
/// in doubt, not recorded: the marker is read under the deployment that wrote the row.
#[ignore = "docker"]
#[tokio::test]
async fn a_chain_in_flight_on_v1_reads_in_doubt_on_v2() {
    let pool = fresh_pool().await;
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-x")]).await;
    let store = PgInboxStore::new(pool.clone());
    assert!(store.record_seen(&dep_a(), "in", "evt-1").await.unwrap());
    PgInflightChainStore::new(pool.clone())
        .begin(
            &dep_a(),
            uuid::Uuid::new_v4(),
            &[("in".to_string(), "evt-1".to_string())],
            "host-1-111-aaaa",
        )
        .await
        .unwrap();
    assert_eq!(
        store.check(&dep_b(), "in", "evt-1").await.unwrap(),
        InboxSeenState::InDoubt
    );
}

/// The check's lookup reaches both arms through an index: the primary key for this deployment's
/// rows and the partial owner index for the owner's, never a sequential scan of a large table.
#[ignore = "docker"]
#[tokio::test]
async fn an_inbox_check_uses_both_indexes() {
    let pool = fresh_pool().await;
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-x")]).await;
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id, owner_id) \
         SELECT $1, 'orders', 'evt-' || g, CASE WHEN g % 2 = 0 THEN 'own-x' END \
         FROM generate_series(1, 50000) g",
    )
    .bind(dep_a().as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE inbox_seen")
        .execute(&pool)
        .await
        .unwrap();
    // The statement `PgInboxStore::check` runs, with its parameters inlined for EXPLAIN.
    let plan: Vec<String> = sqlx::query_scalar(&format!(
        "EXPLAIN SELECT deployment_id FROM inbox_seen \
         WHERE channel = 'orders' AND event_id = 'evt-42' \
           AND (deployment_id = '{dep}' \
                OR owner_id = (SELECT owner_id FROM deployment_owner WHERE deployment_id = '{dep}')) \
         ORDER BY (deployment_id = '{dep}') DESC LIMIT 1",
        dep = dep_b().as_str()
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    let plan = plan.join("\n");
    assert!(!plan.contains("Seq Scan on inbox_seen"), "{plan}");
    assert!(plan.contains("inbox_seen_pkey"), "{plan}");
    assert!(plan.contains("inbox_seen_owner_key"), "{plan}");
}

/// Two versions of one owner (on two replicas, say) both see a key for the first time: the owner
/// index admits exactly one write, so the delivery commits once.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_versions_racing_one_key_commit_once() {
    let pool = fresh_pool().await;
    map_owner(&pool, &[(&dep_a(), "own-x"), (&dep_b(), "own-x")]).await;
    let mut handles = Vec::new();
    for i in 0..8 {
        let store = PgInboxStore::new(pool.clone());
        let dep = if i % 2 == 0 { dep_a() } else { dep_b() };
        handles.push(tokio::spawn(async move {
            store
                .record_seen(&dep, "orders", "raced-evt")
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "one write across both versions");
}
