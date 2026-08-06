//! Mirrors `tests/pg/instance.rs` (instance store + claim/heartbeat/sweep semantics)
//! on the SQL Server dialect, plus the snapshot v2 byte-fidelity round-trip
//! through the dialect's blob column.

use std::collections::BTreeMap;
use std::time::Duration;

use sutra_persistence::mssql::stores::MssqlInstanceStore;
use sutra_persistence::mssql::{MssqlPool, MssqlTx};
use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::stores::{InstanceState, InstanceStore};
use sutra_persistence::DeploymentId;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

fn state(bytes: &[u8]) -> InstanceState {
    InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: bytes.to_vec(),
    }
}

async fn backdate_heartbeat(pool: &MssqlPool, dep: &DeploymentId, id: Uuid, secs: f64) {
    let millis = (secs * 1000.0) as i32;
    let mut conn = pool.acquire().await.unwrap();
    conn.client()
        .execute(
            "UPDATE instance_state \
             SET last_heartbeat_at = DATEADD(MILLISECOND, -@P1, SYSUTCDATETIME()) \
             WHERE deployment_id = @P2 AND instance_id = @P3",
            &[&millis, &dep.as_str(), &id],
        )
        .await
        .unwrap();
}

async fn read_heartbeat(pool: &MssqlPool, dep: &DeploymentId, id: Uuid) -> PrimitiveDateTime {
    let mut conn = pool.acquire().await.unwrap();
    let row = conn
        .client()
        .query(
            "SELECT last_heartbeat_at FROM instance_state \
             WHERE deployment_id = @P1 AND instance_id = @P2",
            &[&dep.as_str(), &id],
        )
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .expect("row");
    row.get::<PrimitiveDateTime, _>("last_heartbeat_at")
        .expect("heartbeat set")
}

#[ignore = "docker"]
#[tokio::test]
async fn persist_and_load_round_trip() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"snapshot-bytes");

    store.persist(&dep_a(), &s).await.unwrap();
    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();

    assert_eq!(loaded, s);
}

#[ignore = "docker"]
#[tokio::test]
async fn snapshot_v2_bytes_round_trip_byte_identical() {
    // The snapshot is byte-deterministic; the dialect's blob column must hand
    // back the exact bytes (no charset, no conversion, no padding).
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let snapshot = InstanceSnapshot::of_suspended(
        "loan",
        dep_a().as_str(),
        vec!["start".to_owned(), "score".to_owned()],
        BTreeMap::from([
            ("applicant".to_owned(), "alice".to_owned()),
            ("amount".to_owned(), "10000.50".to_owned()),
            ("note".to_owned(), "line1\nline2 = tricky:chars".to_owned()),
        ]),
        vec!["waitApproval".to_owned()],
        "start",
        7,
    )
    .with_sensitive(vec!["amount".to_owned()])
    .with_coverage(BTreeMap::from([("happy-path".to_owned(), 3u64)]));
    let bytes = snapshot.write();

    let s = InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: bytes.clone(),
    };
    store.persist(&dep_a(), &s).await.unwrap();
    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();

    assert_eq!(loaded.serialised, bytes, "byte-identical round trip");
    let reread = InstanceSnapshot::read(&loaded.serialised).unwrap();
    assert_eq!(reread.write(), bytes, "re-encode reproduces the same bytes");
}

#[ignore = "docker"]
#[tokio::test]
async fn persist_twice_updates() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let mut s = state(b"v1");

    store.persist(&dep_a(), &s).await.unwrap();
    s.serialised = b"v2-updated".to_vec();
    store.persist(&dep_a(), &s).await.unwrap();

    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();
    assert_eq!(loaded.serialised, b"v2-updated");
    assert_eq!(
        store.count_active(&dep_a()).await.unwrap(),
        1,
        "upsert, not duplicate"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn load_unknown_returns_empty() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    assert!(store
        .load(&dep_a(), Uuid::new_v4())
        .await
        .unwrap()
        .is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_removes_row() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"gone");

    store.persist(&dep_a(), &s).await.unwrap();
    store.delete(&dep_a(), s.instance_id).await.unwrap();

    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_unknown_is_noop() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    store.delete(&dep_a(), Uuid::new_v4()).await.unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn count_active_reflects_live_rows() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);

    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 0);
    let s1 = state(b"one");
    let s2 = state(b"two");
    store.persist(&dep_a(), &s1).await.unwrap();
    store.persist(&dep_a(), &s2).await.unwrap();
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 2);
    store.delete(&dep_a(), s1.instance_id).await.unwrap();
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn deployment_isolation() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"a-only");

    store.persist(&dep_a(), &s).await.unwrap();

    assert!(store.load(&dep_b(), s.instance_id).await.unwrap().is_none());
    assert_eq!(store.count_active(&dep_b()).await.unwrap(), 0);
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 1);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_persists_disjoint_keys() {
    let pool = fresh_pool().await;
    let mut handles = Vec::new();
    for i in 0..8u8 {
        let store = MssqlInstanceStore::new(pool.clone());
        handles.push(tokio::spawn(async move {
            store.persist(&dep_a(), &state(&[i])).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let store = MssqlInstanceStore::new(pool);
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 8);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_for_update_holds_row_lock() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"initial");
    store.persist(&dep_a(), &s).await.unwrap();

    // Transaction 1 takes the row lock (UPDLOCK).
    let mut tx1 = MssqlTx::begin(&pool).await.unwrap();
    let locked = MssqlInstanceStore::load_for_update(tx1.client(), &dep_a(), s.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locked.serialised, b"initial");

    // Transaction 2 must block on the same UPDLOCK until tx1 commits, then observe tx1's
    // write — the replica-serialisation contract.
    let pool2 = pool.clone();
    let id = s.instance_id;
    let waiter = tokio::spawn(async move {
        let mut tx2 = MssqlTx::begin(&pool2).await.unwrap();
        let seen = MssqlInstanceStore::load_for_update(tx2.client(), &dep_a(), id)
            .await
            .unwrap()
            .unwrap();
        tx2.commit().await.unwrap();
        seen.serialised
    });

    // Give tx2 time to reach the lock wait, then write + commit under tx1.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiter.is_finished(), "tx2 must be blocked on the row lock");
    MssqlInstanceStore::persist_in(
        tx1.client(),
        &dep_a(),
        &InstanceState {
            instance_id: s.instance_id,
            serialised: b"advanced-by-tx1".to_vec(),
        },
    )
    .await
    .unwrap();
    tx1.commit().await.unwrap();

    let seen = waiter.await.unwrap();
    assert_eq!(
        seen, b"advanced-by-tx1",
        "tx2 sees tx1's committed bytes after the lock"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn large_payload_round_trip() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let big: Vec<u8> = (0..2_000_000u32).map(|i| (i % 251) as u8).collect();
    let s = InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: big.clone(),
    };

    store.persist(&dep_a(), &s).await.unwrap();
    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();
    assert_eq!(loaded.serialised, big);
}

#[ignore = "docker"]
#[tokio::test]
async fn updated_at_auto_maintained() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"t0");
    store.persist(&dep_a(), &s).await.unwrap();

    let read_updated_at = |pool: MssqlPool, id: Uuid| async move {
        let mut conn = pool.acquire().await.unwrap();
        let row = conn
            .client()
            .query(
                "SELECT updated_at FROM instance_state \
                 WHERE deployment_id = @P1 AND instance_id = @P2",
                &[&dep_a().as_str(), &id],
            )
            .await
            .unwrap()
            .into_row()
            .await
            .unwrap()
            .expect("row");
        row.get::<PrimitiveDateTime, _>("updated_at").expect("set")
    };

    let first = read_updated_at(pool.clone(), s.instance_id).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.persist(&dep_a(), &s).await.unwrap();
    let second = read_updated_at(pool, s.instance_id).await;
    assert!(
        second > first,
        "upsert refreshes updated_at ({second} > {first})"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn load_for_update_missing_row_returns_empty() {
    let pool = fresh_pool().await;
    let mut tx = MssqlTx::begin(&pool).await.unwrap();
    let missing = MssqlInstanceStore::load_for_update(tx.client(), &dep_a(), Uuid::new_v4())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(missing.is_none());
}

// ---- claim / heartbeat -----------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test]
async fn claim_succeeds_when_unclaimed() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();

    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn claim_fails_when_already_owned() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();

    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());
    assert!(!store
        .claim(&dep_a(), s.instance_id, "replica-2")
        .await
        .unwrap());
    // Re-claim by the SAME owner is GRANTED — the CAS is deliberately re-entrant
    // (unowned OR already ours), so a replica that crashed between claim and release
    // re-acquires its own instance instead of locking itself out until claim-timeout.
    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claim_exactly_one_winner() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"contended");
    store.persist(&dep_a(), &s).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..8u32 {
        let store = MssqlInstanceStore::new(pool.clone());
        let id = s.instance_id;
        handles.push(tokio::spawn(async move {
            store
                .claim(&dep_a(), id, &format!("replica-{i}"))
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
    assert_eq!(winners, 1, "the claim CAS admits exactly one replica");
}

#[ignore = "docker"]
#[tokio::test]
async fn heartbeat_by_owner_advances_timestamp() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());

    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 60.0).await;
    let before = read_heartbeat(&pool, &dep_a(), s.instance_id).await;

    assert_eq!(
        store
            .heartbeat(&dep_a(), s.instance_id, "replica-1")
            .await
            .unwrap(),
        1
    );

    let after = read_heartbeat(&pool, &dep_a(), s.instance_id).await;
    assert!(after > before);
}

#[ignore = "docker"]
#[tokio::test]
async fn heartbeat_by_non_owner_returns_zero() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());

    assert_eq!(
        store
            .heartbeat(&dep_a(), s.instance_id, "replica-2")
            .await
            .unwrap(),
        0
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn heartbeat_on_unclaimed_returns_zero() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();

    assert_eq!(
        store
            .heartbeat(&dep_a(), s.instance_id, "replica-1")
            .await
            .unwrap(),
        0
    );
}

// ---- stuck sweep ----------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test]
async fn sweep_clears_stale_claim() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store
        .claim(&dep_a(), s.instance_id, "dead-replica")
        .await
        .unwrap());
    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 120.0).await;

    let swept = store
        .sweep_stuck(&dep_a(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(swept, 1);

    // The sweep re-opens the claim: another replica can now claim.
    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-2")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn sweep_ignores_fresh_heartbeat() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store
        .claim(&dep_a(), s.instance_id, "live-replica")
        .await
        .unwrap());

    let swept = store
        .sweep_stuck(&dep_a(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(swept, 0);
    assert!(!store
        .claim(&dep_a(), s.instance_id, "replica-2")
        .await
        .unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn sweep_empty_table() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool);
    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_secs(30))
            .await
            .unwrap(),
        0
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn sweep_respects_deployment() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let sa = state(b"a");
    let sb = state(b"b");
    store.persist(&dep_a(), &sa).await.unwrap();
    store.persist(&dep_b(), &sb).await.unwrap();
    assert!(store.claim(&dep_a(), sa.instance_id, "r1").await.unwrap());
    assert!(store.claim(&dep_b(), sb.instance_id, "r1").await.unwrap());
    backdate_heartbeat(&pool, &dep_a(), sa.instance_id, 120.0).await;
    backdate_heartbeat(&pool, &dep_b(), sb.instance_id, 120.0).await;

    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_secs(30))
            .await
            .unwrap(),
        1
    );
    // dep-B's stale claim is untouched by dep-A's sweep.
    assert!(!store.claim(&dep_b(), sb.instance_id, "r2").await.unwrap());
    assert_eq!(
        store
            .sweep_stuck(&dep_b(), Duration::from_secs(30))
            .await
            .unwrap(),
        1
    );
    assert!(store.claim(&dep_b(), sb.instance_id, "r2").await.unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn sweep_sub_second_timeout() {
    let pool = fresh_pool().await;
    let store = MssqlInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store.claim(&dep_a(), s.instance_id, "r1").await.unwrap());
    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 0.5).await;

    // Sub-second precision: 100ms timeout sweeps a 500ms-old heartbeat.
    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_millis(100))
            .await
            .unwrap(),
        1
    );
}
