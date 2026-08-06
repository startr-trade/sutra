//! Mirrors `tests/pg/timer_wait.rs` — TIMER wait rows: the V803 addendum,
//! the SKIP-LOCKED due-claim, defer backoff, and the park-with-timer step primitive's
//! commit-or-nothing behaviour on the MySQL dialect.

use std::collections::BTreeMap;

use sqlx::Connection;
use sutra_persistence::mysql::step::{commit_step_with_timers, write_step_with_timers_in};
use sutra_persistence::mysql::stores::MySqlWaitStateStore;
use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::step::{StepTimerWait, StepWait, StepWrite};
use sutra_persistence::stores::{WaitStateStore, WaitingFilter, STATUS_RESOLVED};
use time::Duration;
use uuid::Uuid;

use crate::fixture::{db_url, dep_a, dep_b, fresh_pool, fresh_pool_named, now_micros};

fn park_step(instance_id: Uuid) -> StepWrite {
    StepWrite {
        deployment: dep_a(),
        instance_id,
        snapshot: InstanceSnapshot::of_suspended(
            "call-flow",
            dep_a().as_str(),
            vec!["start".to_owned()],
            BTreeMap::new(),
            vec!["CallOut".to_owned()],
            "start",
            1,
        )
        .write(),
        waits: vec![StepWait {
            node_id: "CallOut".to_owned(),
            process_id: "call-flow".to_owned(),
            correlation_key: None,
        }],
        resolved_waits: vec![],
        aliases: vec![],
        subjects: vec![],
        outbox: vec![],
        withdrawn_call_nodes: Vec::new(),
    }
}

fn timer(node_id: &str, due_at: time::OffsetDateTime) -> StepTimerWait {
    StepTimerWait {
        node_id: node_id.to_owned(),
        process_id: "call-flow".to_owned(),
        due_at,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn park_with_timer_commits_snapshot_wait_and_timer_row_atomically() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    let due = now_micros() + Duration::seconds(30);

    commit_step_with_timers(
        &pool,
        &park_step(instance),
        &[timer("CallOut#timeout", due)],
    )
    .await
    .unwrap();

    let store = MySqlWaitStateStore::new(pool.clone());
    // Not yet due — nothing claimable.
    assert!(store
        .claim_due_timers(&dep_a(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
    // Due — exactly the timer row, with its due-at.
    let claimed = store.claim_due_timers(&dep_a(), due, 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].instance_id, instance);
    assert_eq!(claimed[0].node_id, "CallOut#timeout");
    assert_eq!(claimed[0].process_id, "call-flow");
    assert_eq!(claimed[0].due_at, due);
    // The MESSAGE wait row for the host is NOT claimable as a timer.
    assert!(!claimed.iter().any(|t| t.node_id == "CallOut"));
}

#[ignore = "docker"]
#[tokio::test]
async fn crash_between_write_and_commit_persists_nothing() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    let due = now_micros() + Duration::seconds(1);

    {
        let mut tx = sutra_persistence::mysql::scope::begin_tx(&pool)
            .await
            .unwrap();
        write_step_with_timers_in(&mut tx, &park_step(instance), &[timer("T1", due)])
            .await
            .unwrap();
        // tx dropped WITHOUT commit — the process dying mid-step.
    }

    let waits: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM waiting_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    let instances: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instance_state")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((instances, waits), (0, 0), "commit-or-nothing");
}

#[ignore = "docker"]
#[tokio::test]
async fn concurrent_claims_never_hand_out_the_same_timer() {
    let (pool, db) = fresh_pool_named().await;
    let instance = Uuid::new_v4();
    let due = now_micros() - Duration::seconds(1); // already due

    commit_step_with_timers(&pool, &park_step(instance), &[timer("T1", due)])
        .await
        .unwrap();

    // First claimer holds its transaction open (row locked, not yet committed).
    let mut held = sqlx::MySqlConnection::connect(&db_url(&db)).await.unwrap();
    let mut tx = held.begin().await.unwrap();
    let first = MySqlWaitStateStore::claim_due_timers_in(&mut tx, &dep_a(), now_micros(), 10)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);

    // A concurrent claimer SKIPs the locked row instead of blocking or double-claiming.
    let store = MySqlWaitStateStore::new(pool.clone());
    let second = store
        .claim_due_timers(&dep_a(), now_micros(), 10)
        .await
        .unwrap();
    assert!(second.is_empty(), "SKIP LOCKED: no double-claim");
    tx.rollback().await.unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn defer_pushes_due_at_forward_and_resolve_stops_firing() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    let due = now_micros() - Duration::seconds(1);

    commit_step_with_timers(&pool, &park_step(instance), &[timer("T1", due)])
        .await
        .unwrap();
    let store = MySqlWaitStateStore::new(pool.clone());

    // Defer: no longer claimable now, claimable at the new due-at.
    let later = now_micros() + Duration::seconds(60);
    store
        .defer_timer(&dep_a(), instance, "T1", later)
        .await
        .unwrap();
    assert!(store
        .claim_due_timers(&dep_a(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .claim_due_timers(&dep_a(), later, 10)
            .await
            .unwrap()
            .len(),
        1
    );

    // Resolve (what the resume step does): never claimable again, kept for audit.
    store.resolve(&dep_a(), instance, "T1").await.unwrap();
    assert!(store
        .claim_due_timers(&dep_a(), later, 10)
        .await
        .unwrap()
        .is_empty());
    let resolved = store
        .list_waiting(
            &dep_a(),
            &WaitingFilter {
                status: Some(STATUS_RESOLVED.to_owned()),
                ..Default::default()
            },
            10,
            0,
        )
        .await
        .unwrap();
    assert!(resolved.iter().any(|w| w.node_id == "T1"));
}

#[ignore = "docker"]
#[tokio::test]
async fn timer_claims_are_deployment_isolated() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    let due = now_micros() - Duration::seconds(1);

    commit_step_with_timers(&pool, &park_step(instance), &[timer("T1", due)])
        .await
        .unwrap();

    let store = MySqlWaitStateStore::new(pool);
    assert!(store
        .claim_due_timers(&dep_b(), now_micros(), 10)
        .await
        .unwrap()
        .is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn re_recording_a_timer_resets_status_and_due_at() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    let due1 = now_micros() + Duration::seconds(5);

    commit_step_with_timers(&pool, &park_step(instance), &[timer("T1", due1)])
        .await
        .unwrap();
    let store = MySqlWaitStateStore::new(pool.clone());
    store.resolve(&dep_a(), instance, "T1").await.unwrap();

    // A fresh park of the same node re-arms it with a NEW due-at.
    let due2 = now_micros() + Duration::seconds(10);
    commit_step_with_timers(&pool, &park_step(instance), &[timer("T1", due2)])
        .await
        .unwrap();
    let claimed = store.claim_due_timers(&dep_a(), due2, 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].due_at, due2);
}
