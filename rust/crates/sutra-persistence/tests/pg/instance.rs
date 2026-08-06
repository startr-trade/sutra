//! `PgInstanceStore` against a real PostgreSQL: snapshot persist/load round-trips (including
//! encrypted and large payloads), per-deployment isolation, `SELECT … FOR UPDATE` row locking,
//! exactly-one-winner claim/heartbeat, same-owner claim re-entrancy, the owner-scoped release,
//! the stuck-instance sweep, and the list/cancel surface.

use std::collections::BTreeMap;
use std::time::Duration;

use sqlx::Row;
use sutra_persistence::snapshot::{
    InstanceSnapshot, SnapshotCrypto, STATUS_RUNNING, STATUS_SUSPENDED,
};
use sutra_persistence::stores::{
    AliasStore, InstanceFilter, InstanceState, InstanceStore, PgAliasStore, PgInstanceStore,
    PgWaitStateStore, WaitStateStore,
};
use sutra_persistence::value::SnapshotValue;
use sutra_persistence::DeploymentId;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

fn state(bytes: &[u8]) -> InstanceState {
    InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: bytes.to_vec(),
    }
}

async fn backdate_heartbeat(pool: &sqlx::PgPool, dep: &DeploymentId, id: Uuid, secs: f64) {
    sqlx::query(
        "UPDATE instance_state SET last_heartbeat_at = now() - make_interval(secs => $1) \
         WHERE deployment_id = $2 AND instance_id = $3",
    )
    .bind(secs)
    .bind(dep.as_str())
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn persist_and_load_round_trip() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let s = state(b"snapshot-bytes");

    store.persist(&dep_a(), &s).await.unwrap();
    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();

    assert_eq!(loaded, s);
}

/// An AES-256-GCM snapshot (v3) round-trips through real Postgres: the ciphertext bytes
/// survive the persist/load, the raw sensitive value is NEVER present in the stored bytes, and the
/// self-described keyId lets it decrypt back to plaintext on load.
#[ignore = "docker"]
#[tokio::test]
async fn encrypted_snapshot_survives_postgres_round_trip_and_decrypts() {
    use std::collections::BTreeSet;

    use sutra_crypto::{Aes256GcmCipher, HkdfKeyProvider, KeyProvider};

    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);

    let instance_id = Uuid::new_v4();
    let snap = InstanceSnapshot::of_suspended(
        "p1",
        dep_a().as_str(),
        vec!["S".to_string()],
        [
            ("pan".to_string(), "4111111111111111".to_string()),
            ("amount".to_string(), "42".to_string()),
        ]
        .into_iter()
        .collect(),
        vec!["U".to_string()],
        "S",
        0,
    );
    let provider = HkdfKeyProvider::new(b"it-master-secret");
    let cipher = Aes256GcmCipher::new(&provider.data_key("tenant-a").unwrap());
    let iid = instance_id.to_string();
    let ctx = SnapshotCrypto::new(&cipher, "tenant-a", &iid);
    let encrypt: BTreeSet<String> = ["pan".to_string()].into_iter().collect();
    let bytes = snap.write_encrypted(Some(&ctx), &encrypt).unwrap();

    // The raw PAN must NOT be present in the bytes that hit the database.
    assert!(
        !String::from_utf8_lossy(&bytes).contains("4111111111111111"),
        "raw sensitive value present in the at-rest bytes"
    );

    let s = InstanceState {
        instance_id,
        serialised: bytes,
    };
    store.persist(&dep_a(), &s).await.unwrap();
    let loaded = store.load(&dep_a(), instance_id).await.unwrap().unwrap();
    assert_eq!(
        loaded.serialised, s.serialised,
        "ciphertext bytes must survive the Postgres round trip byte-for-byte"
    );

    // The keyId is recoverable from the loaded bytes, and decryption restores the plaintext.
    assert_eq!(
        InstanceSnapshot::peek_key_id(&loaded.serialised)
            .unwrap()
            .as_deref(),
        Some("tenant-a")
    );
    let decoded = InstanceSnapshot::read_encrypted(&loaded.serialised, Some(&ctx)).unwrap();
    assert_eq!(
        decoded.variables()["pan"],
        SnapshotValue::from("4111111111111111")
    );
    assert_eq!(decoded.variables()["amount"], SnapshotValue::from("42"));
}

/// A TYPED snapshot (v4) survives a real Postgres round trip with every variable's type intact —
/// the durable half of "variables survive waits typed". Postgres stores the snapshot as opaque
/// bytes, so what this actually proves is that the v4 wire form is byte-clean through a `bytea`
/// column and decodes to the same value model it was written from.
#[ignore = "docker"]
#[tokio::test]
async fn typed_snapshot_round_trips_through_postgres() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let instance_id = Uuid::new_v4();

    let typed = BTreeMap::from([
        (
            "amount".to_string(),
            SnapshotValue::Number("1250.75".parse().unwrap()),
        ),
        ("approved".to_string(), SnapshotValue::Boolean(false)),
        ("inboundId".to_string(), SnapshotValue::from("INB-7")),
        ("cancelledAt".to_string(), SnapshotValue::Null),
        (
            "dueDate".to_string(),
            SnapshotValue::Date("2026-08-05".to_string()),
        ),
        (
            "sla".to_string(),
            SnapshotValue::Duration("P2DT4H".to_string()),
        ),
        (
            "lines".to_string(),
            SnapshotValue::List(vec![
                SnapshotValue::Number("1".parse().unwrap()),
                SnapshotValue::Context(BTreeMap::from([
                    ("sku".to_string(), SnapshotValue::from("A-1")),
                    (
                        "qty".to_string(),
                        SnapshotValue::Number("3".parse().unwrap()),
                    ),
                ])),
            ]),
        ),
    ]);
    let snap = InstanceSnapshot::of_suspended(
        "pay",
        dep_a().as_str(),
        vec!["S".to_string()],
        BTreeMap::new(),
        vec!["U".to_string()],
        "S",
        11,
    )
    .with_variables(typed.clone());
    let bytes = snap.write();
    assert!(String::from_utf8_lossy(&bytes).contains("sutra.snapshot=4"));

    store
        .persist(
            &dep_a(),
            &InstanceState {
                instance_id,
                serialised: bytes.clone(),
            },
        )
        .await
        .unwrap();
    let loaded = store.load(&dep_a(), instance_id).await.unwrap().unwrap();
    assert_eq!(loaded.serialised, bytes, "bytea must be byte-transparent");

    let decoded = InstanceSnapshot::read(&loaded.serialised).unwrap();
    assert_eq!(decoded.variables(), &typed);
    assert_eq!(decoded.audit_seq(), 11);
    assert_eq!(decoded.waiting_nodes(), ["U"]);
}

/// The three byte-level key patchers run against a TYPED snapshot that has been through the
/// database, not just an in-memory one. They rewrite the raw properties map without decoding a
/// single value, and that has to keep holding now that a value can be a tagged number, a JSON
/// structure, or a null — none of which they know how to read, and none of which they need to.
#[ignore = "docker"]
#[tokio::test]
async fn key_patchers_operate_on_a_stored_typed_snapshot() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let instance_id = Uuid::new_v4();

    let typed = BTreeMap::from([
        (
            "amount".to_string(),
            SnapshotValue::Number("1250.75".parse().unwrap()),
        ),
        (
            "lines".to_string(),
            SnapshotValue::List(vec![SnapshotValue::from("A-1")]),
        ),
        ("cancelledAt".to_string(), SnapshotValue::Null),
    ]);
    let stored = InstanceSnapshot::of_suspended(
        "pay",
        dep_a().as_str(),
        vec!["S".to_string()],
        BTreeMap::new(),
        vec!["U".to_string()],
        "S",
        4,
    )
    .with_variables(typed.clone())
    .with_retry_attempts(BTreeMap::from([("chargeCard".to_string(), 2u32)]))
    .write();
    store
        .persist(
            &dep_a(),
            &InstanceState {
                instance_id,
                serialised: stored.clone(),
            },
        )
        .await
        .unwrap();
    let from_db = store
        .load(&dep_a(), instance_id)
        .await
        .unwrap()
        .unwrap()
        .serialised;

    let failed =
        InstanceSnapshot::mark_failed(&from_db, "SUTRA.RUNTIME.TASK.UNCAUGHT", "boom").unwrap();
    let read = InstanceSnapshot::read(&failed).unwrap();
    assert_eq!(read.status(), sutra_persistence::snapshot::STATUS_FAILED);
    assert_eq!(read.variables(), &typed);
    assert_eq!(read.retry_attempts().get("chargeCard"), Some(&2));

    let completed =
        InstanceSnapshot::mark_terminal(&from_db, sutra_persistence::snapshot::STATUS_COMPLETED)
            .unwrap();
    let read = InstanceSnapshot::read(&completed).unwrap();
    assert_eq!(read.status(), sutra_persistence::snapshot::STATUS_COMPLETED);
    assert_eq!(read.variables(), &typed);

    let migrated = InstanceSnapshot::migrate_pinned(
        &from_db,
        dep_b().as_str(),
        None,
        &BTreeMap::from([("U".to_string(), "U2".to_string())]),
        Some(5),
    )
    .unwrap();
    let read = InstanceSnapshot::read(&migrated).unwrap();
    assert_eq!(read.deployment_id(), dep_b().as_str());
    assert_eq!(read.waiting_nodes(), ["U2"]);
    assert_eq!(read.audit_seq(), 5);
    assert_eq!(read.variables(), &typed);
    // Each patch output is already the writer's canonical form — typing did not make the raw
    // map rewrite lossy.
    assert_eq!(read.write(), migrated);
}

/// A snapshot written before typing existed must keep loading, unchanged, forever. This is that
/// promise stated against a real database rather than a byte literal in a unit test: v2 bytes go
/// in, v2 semantics come out, including a value that happens to look like a v4 type tag.
#[ignore = "docker"]
#[tokio::test]
async fn a_stored_v2_snapshot_still_loads_with_string_values() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let instance_id = Uuid::new_v4();

    // Written by hand in the v2 shape — exactly what a pre-typing engine persisted.
    let legacy = format!(
        "sutra.auditSeq=3\nsutra.completedNodes=S\nsutra.deploymentId={}\n\
         sutra.processId=pay\nsutra.snapshot=2\nsutra.startNode=S\nsutra.status=SUSPENDED\n\
         sutra.var.amount=1250.75\nsutra.var.approved=false\nsutra.var.looksTagged=n|7\n\
         sutra.waitingNodes=U\n",
        dep_a().as_str()
    );
    store
        .persist(
            &dep_a(),
            &InstanceState {
                instance_id,
                serialised: legacy.clone().into_bytes(),
            },
        )
        .await
        .unwrap();

    let loaded = store.load(&dep_a(), instance_id).await.unwrap().unwrap();
    let decoded = InstanceSnapshot::read(&loaded.serialised).unwrap();
    assert_eq!(decoded.status(), STATUS_SUSPENDED);
    assert_eq!(decoded.audit_seq(), 3);
    for (name, expected) in [
        ("amount", "1250.75"),
        ("approved", "false"),
        ("looksTagged", "n|7"),
    ] {
        assert_eq!(
            decoded.variables()[name],
            SnapshotValue::from(expected),
            "a v2 value must decode as the STRING it was, never as a type"
        );
    }
    // Re-encoding an untouched legacy snapshot reproduces the legacy bytes: no silent upgrade.
    assert_eq!(String::from_utf8(decoded.write()).unwrap(), legacy);
}

#[ignore = "docker"]
#[tokio::test]
async fn persist_twice_updates() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let mut s = state(b"v1");

    store.persist(&dep_a(), &s).await.unwrap();
    s.serialised = b"v2-updated".to_vec();
    store.persist(&dep_a(), &s).await.unwrap();

    let loaded = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();
    assert_eq!(loaded.serialised, b"v2-updated");
    assert_eq!(
        store.count_active(&dep_a()).await.unwrap(),
        1,
        "UPSERT, not duplicate"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn load_unknown_returns_empty() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool);
    let s = state(b"gone");

    store.persist(&dep_a(), &s).await.unwrap();
    store.delete(&dep_a(), s.instance_id).await.unwrap();

    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_unknown_is_noop() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    store.delete(&dep_a(), Uuid::new_v4()).await.unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn count_active_reflects_live_rows() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);

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
    let store = PgInstanceStore::new(pool);
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
        let store = PgInstanceStore::new(pool.clone());
        handles.push(tokio::spawn(async move {
            store.persist(&dep_a(), &state(&[i])).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let store = PgInstanceStore::new(pool);
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 8);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_for_update_holds_row_lock() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"initial");
    store.persist(&dep_a(), &s).await.unwrap();

    // Transaction 1 takes the row lock.
    let mut tx1 = sutra_persistence::scope::begin_deployment_tx(&pool, &dep_a())
        .await
        .unwrap();
    let locked = PgInstanceStore::load_for_update(&mut tx1, &dep_a(), s.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locked.serialised, b"initial");

    // Transaction 2 must block on the same FOR UPDATE until tx1 commits, then observe
    // tx1's write — the replica-serialisation contract.
    let pool2 = pool.clone();
    let id = s.instance_id;
    let waiter = tokio::spawn(async move {
        let mut tx2 = sutra_persistence::scope::begin_deployment_tx(&pool2, &dep_a())
            .await
            .unwrap();
        let seen = PgInstanceStore::load_for_update(&mut tx2, &dep_a(), id)
            .await
            .unwrap()
            .unwrap();
        tx2.commit().await.unwrap();
        seen.serialised
    });

    // Give tx2 time to reach the lock wait, then write + commit under tx1.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiter.is_finished(), "tx2 must be blocked on the row lock");
    PgInstanceStore::persist_in(
        &mut tx1,
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
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"t0");
    store.persist(&dep_a(), &s).await.unwrap();

    let read_updated_at = |pool: sqlx::PgPool, id: Uuid| async move {
        let row = sqlx::query(
            "SELECT updated_at FROM instance_state WHERE deployment_id = $1 AND instance_id = $2",
        )
        .bind(dep_a().as_str())
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        row.get::<OffsetDateTime, _>("updated_at")
    };

    let first = read_updated_at(pool.clone(), s.instance_id).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.persist(&dep_a(), &s).await.unwrap();
    let second = read_updated_at(pool, s.instance_id).await;
    assert!(
        second > first,
        "UPSERT refreshes updated_at ({second} > {first})"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn load_for_update_missing_row_returns_empty() {
    let pool = fresh_pool().await;
    let mut tx = sutra_persistence::scope::begin_deployment_tx(&pool, &dep_a())
        .await
        .unwrap();
    let missing = PgInstanceStore::load_for_update(&mut tx, &dep_a(), Uuid::new_v4())
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
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"contended");
    store.persist(&dep_a(), &s).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..8u32 {
        let store = PgInstanceStore::new(pool.clone());
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
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store
        .claim(&dep_a(), s.instance_id, "replica-1")
        .await
        .unwrap());

    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 60.0).await;
    let before: OffsetDateTime = sqlx::query_scalar(
        "SELECT last_heartbeat_at FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        store
            .heartbeat(&dep_a(), s.instance_id, "replica-1")
            .await
            .unwrap(),
        1
    );

    let after: OffsetDateTime = sqlx::query_scalar(
        "SELECT last_heartbeat_at FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(after > before);
}

#[ignore = "docker"]
#[tokio::test]
async fn heartbeat_by_non_owner_returns_zero() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool);
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

// ---- stuck sweep -------------------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test]
async fn sweep_clears_stale_claim() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
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
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool);
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
    let store = PgInstanceStore::new(pool.clone());
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
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store.claim(&dep_a(), s.instance_id, "r1").await.unwrap());
    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 0.5).await;

    // Fractional-seconds bind: 100ms timeout sweeps a 500ms-old heartbeat.
    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_millis(100))
            .await
            .unwrap(),
        1
    );
}

// ---- ownership: re-entrancy + the owner-scoped release --------------------------------------

/// The engine's resume paths claim before they rehydrate and release when the step commits.
/// A SECOND claim by the SAME owner (one process, one actor thread — already serialised) must
/// succeed and refresh the heartbeat rather than bouncing the owner off its own instance.
#[ignore = "docker"]
#[tokio::test]
async fn claim_is_reentrant_for_the_same_owner_and_refreshes_the_heartbeat() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store.claim(&dep_a(), s.instance_id, "r1").await.unwrap());

    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 60.0).await;
    let before: OffsetDateTime = sqlx::query_scalar(
        "SELECT last_heartbeat_at FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(
        store.claim(&dep_a(), s.instance_id, "r1").await.unwrap(),
        "the same owner re-claims its own instance"
    );

    let after: OffsetDateTime = sqlx::query_scalar(
        "SELECT last_heartbeat_at FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(after > before, "the re-claim stamps a fresh heartbeat");
    // …and it is still exclusive against everyone else.
    assert!(!store.claim(&dep_a(), s.instance_id, "r2").await.unwrap());
}

#[ignore = "docker"]
#[tokio::test]
async fn release_by_the_owner_reopens_the_claim() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store.claim(&dep_a(), s.instance_id, "r1").await.unwrap());
    assert!(!store.claim(&dep_a(), s.instance_id, "r2").await.unwrap());

    assert_eq!(
        store.release(&dep_a(), s.instance_id, "r1").await.unwrap(),
        1
    );

    // Released ⇒ the next replica takes it immediately (no sweep wait).
    assert!(store.claim(&dep_a(), s.instance_id, "r2").await.unwrap());
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT claim_owner FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner.as_deref(), Some("r2"));
}

/// The release is owner-scoped, which is what makes the resume path's drop-guard safe to fire
/// redundantly: a late release from a replica that has already handed the instance on cannot
/// clear the successor's claim.
#[ignore = "docker"]
#[tokio::test]
async fn release_by_a_non_owner_is_a_no_op() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    assert!(store.claim(&dep_a(), s.instance_id, "r2").await.unwrap());

    assert_eq!(
        store.release(&dep_a(), s.instance_id, "r1").await.unwrap(),
        0,
        "r1 never held this claim"
    );
    assert!(
        !store.claim(&dep_a(), s.instance_id, "r3").await.unwrap(),
        "r2's claim still stands"
    );
    // A double release by the true owner is equally harmless.
    assert_eq!(
        store.release(&dep_a(), s.instance_id, "r2").await.unwrap(),
        1
    );
    assert_eq!(
        store.release(&dep_a(), s.instance_id, "r2").await.unwrap(),
        0
    );
}

/// A claim stranded by a crash (no release ever runs) blocks resumes until the
/// `StuckInstanceScanner`'s sweep clears it — the full crash→sweep→re-claim cycle the
/// ownership protocol is built around.
#[ignore = "docker"]
#[tokio::test]
async fn a_stranded_claim_blocks_resume_until_the_sweep_reclaims_it() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = state(b"x");
    store.persist(&dep_a(), &s).await.unwrap();
    // Replica A claims and "dies" — its per-process id never returns.
    assert!(store
        .claim(&dep_a(), s.instance_id, "host-a-101-deadbeef")
        .await
        .unwrap());

    // Replica B cannot resume it, and the sweep is a no-op while the heartbeat is fresh.
    assert!(!store
        .claim(&dep_a(), s.instance_id, "host-b-202-cafebabe")
        .await
        .unwrap());
    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_secs(300))
            .await
            .unwrap(),
        0
    );

    // The claim timeout lapses; the scanner's sweep clears it and B takes over.
    backdate_heartbeat(&pool, &dep_a(), s.instance_id, 600.0).await;
    assert_eq!(
        store
            .sweep_stuck(&dep_a(), Duration::from_secs(300))
            .await
            .unwrap(),
        1
    );
    assert!(store
        .claim(&dep_a(), s.instance_id, "host-b-202-cafebabe")
        .await
        .unwrap());
}

// ---- list + cancel (operate-time inspection surface) ---------------------------------------

/// A persisted snapshot row carrying a real (decodable) status, keyed under `dep`.
fn snapshot_state(status: &str, dep: &DeploymentId) -> InstanceState {
    let bytes = InstanceSnapshot::of("p1", dep.as_str(), status, vec![], BTreeMap::new()).write();
    InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: bytes,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn list_returns_summaries_with_decoded_status() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let s = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &s).await.unwrap();

    let out = store
        .list(&dep_a(), &InstanceFilter::default())
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].instance_id, s.instance_id);
    assert_eq!(out[0].deployment_id, dep_a().as_str());
    assert_eq!(out[0].status, STATUS_SUSPENDED);
}

#[ignore = "docker"]
#[tokio::test]
async fn list_status_filter_and_deployment_isolation() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let parked = snapshot_state(STATUS_SUSPENDED, &dep_a());
    let running = snapshot_state(STATUS_RUNNING, &dep_a());
    store.persist(&dep_a(), &parked).await.unwrap();
    store.persist(&dep_a(), &running).await.unwrap();

    // Status filter keeps only the suspended row.
    let suspended = store
        .list(
            &dep_a(),
            &InstanceFilter {
                status: Some(STATUS_SUSPENDED.to_owned()),
                ..InstanceFilter::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(suspended.len(), 1);
    assert_eq!(suspended[0].instance_id, parked.instance_id);

    // No filter returns both.
    assert_eq!(
        store
            .list(&dep_a(), &InstanceFilter::default())
            .await
            .unwrap()
            .len(),
        2
    );

    // dep-B sees none of dep-A's instances (explicit bind + RLS).
    assert!(store
        .list(&dep_b(), &InstanceFilter::default())
        .await
        .unwrap()
        .is_empty());
}

// ---- terminal retention (P1-2) ---------------------------------------------------------------

/// Backdate a terminal row's `terminal_at` so a retention window can be crossed without waiting.
async fn backdate_terminal(pool: &sqlx::PgPool, dep: &DeploymentId, id: Uuid, secs: f64) {
    sqlx::query(
        "UPDATE instance_state SET terminal_at = now() - make_interval(secs => $1) \
         WHERE deployment_id = $2 AND instance_id = $3",
    )
    .bind(secs)
    .bind(dep.as_str())
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

async fn terminal_at_of(
    pool: &sqlx::PgPool,
    dep: &DeploymentId,
    id: Uuid,
) -> Option<OffsetDateTime> {
    sqlx::query_scalar(
        "SELECT terminal_at FROM instance_state WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(dep.as_str())
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The retain-at-terminal write: the row survives with a re-stamped snapshot, a `terminal_at`
/// marker and NO ownership claim — and it stops counting as active.
#[ignore = "docker"]
#[tokio::test]
async fn mark_terminal_retains_the_row_restamped_and_unclaimed() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let s = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &s).await.unwrap();
    // A resume path would be holding the claim at this point.
    assert!(store
        .claim(&dep_a(), s.instance_id, "owner-1")
        .await
        .unwrap());
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 1);

    let terminal = InstanceSnapshot::mark_terminal(
        &store
            .load(&dep_a(), s.instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
        sutra_persistence::snapshot::STATUS_COMPLETED,
    )
    .unwrap();
    assert_eq!(
        store
            .mark_terminal(&dep_a(), s.instance_id, &terminal)
            .await
            .unwrap(),
        1
    );

    // The row is still there and answers as COMPLETED — this is the whole point of P1-2.
    let row = store.load(&dep_a(), s.instance_id).await.unwrap().unwrap();
    let read = InstanceSnapshot::read(&row.serialised).unwrap();
    assert_eq!(read.status(), sutra_persistence::snapshot::STATUS_COMPLETED);
    assert!(terminal_at_of(&pool, &dep_a(), s.instance_id)
        .await
        .is_some());

    // …but it is no longer ACTIVE (the deploy quiescence gate must not wait on it).
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 0);

    // …and it holds no claim: a terminal row is owned by nobody.
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT claim_owner FROM instance_state WHERE deployment_id = $1 AND instance_id = $2",
    )
    .bind(dep_a().as_str())
    .bind(s.instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(owner.is_none(), "a terminal row must not stay claimed");
}

/// `count_active` counts live and FAILED instances, never terminal ones. The FAILED half is the
/// load-bearing part: a fatal instance still needs a human, so its deployment must not retire.
#[ignore = "docker"]
#[tokio::test]
async fn count_active_excludes_terminal_rows_but_still_counts_failed_ones() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());

    let parked = snapshot_state(STATUS_SUSPENDED, &dep_a());
    let failed = snapshot_state(STATUS_SUSPENDED, &dep_a());
    let done = snapshot_state(STATUS_SUSPENDED, &dep_a());
    for s in [&parked, &failed, &done] {
        store.persist(&dep_a(), s).await.unwrap();
    }
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 3);

    // FAILED is a re-stamp WITHOUT a terminal_at marker (the commit_failed shape).
    let failed_bytes = InstanceSnapshot::mark_failed(
        &store
            .load(&dep_a(), failed.instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
        "SUTRA.RUNTIME.TASK.UNCAUGHT",
        "boom",
    )
    .unwrap();
    store
        .persist(
            &dep_a(),
            &InstanceState {
                instance_id: failed.instance_id,
                serialised: failed_bytes,
            },
        )
        .await
        .unwrap();

    let done_bytes = InstanceSnapshot::mark_terminal(
        &store
            .load(&dep_a(), done.instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
        sutra_persistence::snapshot::STATUS_COMPLETED,
    )
    .unwrap();
    store
        .mark_terminal(&dep_a(), done.instance_id, &done_bytes)
        .await
        .unwrap();

    assert_eq!(
        store.count_active(&dep_a()).await.unwrap(),
        2,
        "the completed instance stops counting; the FAILED one keeps counting (it awaits a human)"
    );
}

/// The operate list hides retained terminal rows by default and shows them on request.
#[ignore = "docker"]
#[tokio::test]
async fn list_hides_terminal_rows_unless_asked() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let parked = snapshot_state(STATUS_SUSPENDED, &dep_a());
    let done = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &parked).await.unwrap();
    store.persist(&dep_a(), &done).await.unwrap();
    let bytes = InstanceSnapshot::mark_terminal(
        &store
            .load(&dep_a(), done.instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
        sutra_persistence::snapshot::STATUS_COMPLETED,
    )
    .unwrap();
    store
        .mark_terminal(&dep_a(), done.instance_id, &bytes)
        .await
        .unwrap();

    let live = store
        .list(&dep_a(), &InstanceFilter::default())
        .await
        .unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].instance_id, parked.instance_id);

    let all = store
        .list(
            &dep_a(),
            &InstanceFilter {
                include_terminal: true,
                ..InstanceFilter::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert!(all
        .iter()
        .any(|s| s.status == sutra_persistence::snapshot::STATUS_COMPLETED));
}

/// The purge sweeper's predicate: past the window purges, ON the window purges, inside it keeps —
/// and a LIVE row is never touched however old it is.
#[ignore = "docker"]
#[tokio::test]
async fn purge_terminal_drops_rows_at_or_past_the_retention_window_only() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let retention = Duration::from_secs(600);

    let mut ids = Vec::new();
    for _ in 0..3 {
        let s = snapshot_state(STATUS_SUSPENDED, &dep_a());
        store.persist(&dep_a(), &s).await.unwrap();
        let bytes = InstanceSnapshot::mark_terminal(
            &store
                .load(&dep_a(), s.instance_id)
                .await
                .unwrap()
                .unwrap()
                .serialised,
            sutra_persistence::snapshot::STATUS_COMPLETED,
        )
        .unwrap();
        store
            .mark_terminal(&dep_a(), s.instance_id, &bytes)
            .await
            .unwrap();
        ids.push(s.instance_id);
    }
    let (past, at, inside) = (ids[0], ids[1], ids[2]);
    // A live (never-terminal) row, deliberately older than everything else.
    let live = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &live).await.unwrap();

    backdate_terminal(&pool, &dep_a(), past, 3600.0).await;
    backdate_terminal(&pool, &dep_a(), at, 600.0).await;
    backdate_terminal(&pool, &dep_a(), inside, 60.0).await;

    let purged = store.purge_terminal(&dep_a(), retention).await.unwrap();
    assert_eq!(purged, 2, "past and at-the-boundary purge; inside does not");
    assert!(store.load(&dep_a(), past).await.unwrap().is_none());
    assert!(store.load(&dep_a(), at).await.unwrap().is_none());
    assert!(store.load(&dep_a(), inside).await.unwrap().is_some());
    assert!(
        store
            .load(&dep_a(), live.instance_id)
            .await
            .unwrap()
            .is_some(),
        "a live instance is NEVER purged by retention, whatever its age"
    );

    // Idempotent: a second sweep over the same window purges nothing more.
    assert_eq!(store.purge_terminal(&dep_a(), retention).await.unwrap(), 0);
}

/// A zero retention sweeps every terminal row immediately — the cleanup an operator gets after
/// switching `sutra.instance.retention` to `PT0S` (new terminals are deleted at the source).
#[ignore = "docker"]
#[tokio::test]
async fn purge_terminal_with_zero_retention_clears_rows_written_before_the_flip() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool);
    let s = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &s).await.unwrap();
    let bytes = InstanceSnapshot::mark_terminal(
        &store
            .load(&dep_a(), s.instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
        sutra_persistence::snapshot::STATUS_COMPLETED,
    )
    .unwrap();
    store
        .mark_terminal(&dep_a(), s.instance_id, &bytes)
        .await
        .unwrap();

    assert_eq!(
        store
            .purge_terminal(&dep_a(), Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_none());
}

/// The purge is deployment-scoped: one deployment's sweep never reaches another's rows.
#[ignore = "docker"]
#[tokio::test]
async fn purge_terminal_is_deployment_scoped() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let mut kept = None;
    for dep in [dep_a(), dep_b()] {
        let s = snapshot_state(STATUS_SUSPENDED, &dep);
        store.persist(&dep, &s).await.unwrap();
        let bytes = InstanceSnapshot::mark_terminal(
            &store
                .load(&dep, s.instance_id)
                .await
                .unwrap()
                .unwrap()
                .serialised,
            sutra_persistence::snapshot::STATUS_COMPLETED,
        )
        .unwrap();
        store
            .mark_terminal(&dep, s.instance_id, &bytes)
            .await
            .unwrap();
        backdate_terminal(&pool, &dep, s.instance_id, 3600.0).await;
        if dep == dep_b() {
            kept = Some(s.instance_id);
        }
    }
    assert_eq!(
        store
            .purge_terminal(&dep_a(), Duration::from_secs(60))
            .await
            .unwrap(),
        1
    );
    assert!(store.load(&dep_b(), kept.unwrap()).await.unwrap().is_some());
}

#[ignore = "docker"]
#[tokio::test]
async fn cancel_primitives_retire_a_parked_instance() {
    let pool = fresh_pool().await;
    let store = PgInstanceStore::new(pool.clone());
    let waits = PgWaitStateStore::new(pool.clone());
    let aliases = PgAliasStore::new(pool);

    let s = snapshot_state(STATUS_SUSPENDED, &dep_a());
    store.persist(&dep_a(), &s).await.unwrap();
    waits
        .record_waiting(&dep_a(), s.instance_id, "p1", "W", None)
        .await
        .unwrap();
    assert!(aliases
        .record(&dep_a(), s.instance_id, "ref", "R-1", true)
        .await
        .unwrap());

    // Precondition: the parked instance shows up in the operate list.
    assert_eq!(
        store
            .list(&dep_a(), &InstanceFilter::default())
            .await
            .unwrap()
            .len(),
        1
    );

    // The cancel primitive sequence (mirrors the /sutra/instances/{id}/cancel handler):
    // resolve waits, retire aliases, delete the row.
    waits.resolve_all(&dep_a(), s.instance_id).await.unwrap();
    aliases.retire(&dep_a(), s.instance_id).await.unwrap();
    store.delete(&dep_a(), s.instance_id).await.unwrap();

    // Gone: no instance row, empty list, and the unique alias is freed.
    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_none());
    assert!(store
        .list(&dep_a(), &InstanceFilter::default())
        .await
        .unwrap()
        .is_empty());
    assert!(aliases
        .find_live(&dep_a(), "ref", "R-1")
        .await
        .unwrap()
        .is_none());
}
