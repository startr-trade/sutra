//! The strict transactional step — commit-or-nothing proofs. The reference baseline is NOT
//! atomic here (an accepted divergence), so these tests are this engine's own conformance
//! rather than a comparison against it.

use std::collections::BTreeMap;

use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::step::{
    commit_step, commit_step_with_timers_releasing, write_step_in, StepAlias, StepSubject,
    StepWait, StepWrite,
};
use sutra_persistence::stores::{
    AliasStore, InstanceStore, OutboxEntry, OutboxStore, PgAliasStore, PgInstanceStore,
    PgOutboxStore, PgSubjectIndexStore, PgWaitStateStore, ReplyMode, SubjectIndexStore,
    WaitStateStore, WaitingFilter,
};
use sutra_persistence::PersistenceError;
use uuid::Uuid;

use crate::fixture::{dep_a, fresh_pool, now_micros};

fn snapshot_bytes(instance_suffix: &str) -> Vec<u8> {
    InstanceSnapshot::of_suspended(
        "loan",
        dep_a().as_str(),
        vec!["start".to_owned(), "score".to_owned()],
        BTreeMap::from([("applicant".to_owned(), instance_suffix.to_owned())]),
        vec!["waitApproval".to_owned()],
        "start",
        1,
    )
    .write()
}

fn outbox_entry(instance_id: Uuid, key: &str) -> OutboxEntry {
    let now = now_micros();
    OutboxEntry {
        deployment: dep_a(),
        entry_id: Uuid::new_v4(),
        instance_id,
        body: b"notify".to_vec().into(),
        content_type: Some("text/plain".to_owned()),
        destination: "https://sink.example/notify".to_owned(),
        headers: BTreeMap::new(),
        required: true,
        mode: ReplyMode::Native,
        outbox_key: key.to_owned(),
        cloud_event_json: None,
        auth_ref_json: None,
        labels: BTreeMap::new(),
        created_at: now,
        next_attempt_at: now,
        attempt_count: 0,
        last_diagnostic_json: None,
        traceparent: None,
        node_id: None,
    }
}

fn step(instance_id: Uuid) -> StepWrite {
    StepWrite {
        deployment: dep_a(),
        instance_id,
        snapshot: snapshot_bytes("alice"),
        waits: vec![StepWait {
            node_id: "waitApproval".to_owned(),
            process_id: "loan".to_owned(),
            correlation_key: None,
        }],
        resolved_waits: vec![],
        withdrawn_call_nodes: Vec::new(),
        aliases: vec![StepAlias {
            alias_name: "loanId".to_owned(),
            alias_value: "L-1".to_owned(),
            unique: true,
        }],
        subjects: vec![],
        outbox: vec![
            outbox_entry(instance_id, "k-1"),
            outbox_entry(instance_id, "k-2"),
        ],
    }
}

async fn table_counts(pool: &sqlx::PgPool) -> (i64, i64, i64, i64) {
    let instance: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instance_state")
        .fetch_one(pool)
        .await
        .unwrap();
    let waits: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM waiting_event")
        .fetch_one(pool)
        .await
        .unwrap();
    let aliases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM alias_index")
        .fetch_one(pool)
        .await
        .unwrap();
    let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_entry")
        .fetch_one(pool)
        .await
        .unwrap();
    (instance, waits, aliases, outbox)
}

#[ignore = "docker"]
#[tokio::test]
async fn step_commits_snapshot_waits_aliases_and_outbox_atomically() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    commit_step(&pool, &step(instance_id)).await.unwrap();

    let instance_store = PgInstanceStore::new(pool.clone());
    let loaded = instance_store
        .load(&dep_a(), instance_id)
        .await
        .unwrap()
        .unwrap();
    let snap = InstanceSnapshot::read(&loaded.serialised).unwrap();
    assert!(snap.is_suspended());
    assert_eq!(snap.waiting_nodes(), ["waitApproval".to_owned()]);

    let wait_store = PgWaitStateStore::new(pool.clone());
    let waiting = wait_store
        .list_waiting(&dep_a(), &WaitingFilter::default(), 10, 0)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].instance_id, instance_id);

    let alias_store = PgAliasStore::new(pool.clone());
    assert_eq!(
        alias_store
            .find_live(&dep_a(), "loanId", "L-1")
            .await
            .unwrap(),
        Some(instance_id)
    );

    let outbox_store = PgOutboxStore::new(pool.clone());
    let claimed = outbox_store
        .claim_due(&dep_a(), now_micros(), 10)
        .await
        .unwrap();
    assert_eq!(
        claimed.len(),
        2,
        "both enqueues committed with the snapshot"
    );
}

/// A RESUMED step hands the instance's ownership claim back inside its own transaction: the
/// new frontier and the released claim become visible together, so the next replica can pick
/// the instance up the instant the step is durable — no sweep wait, no window where the
/// frontier has moved while the claim still stands.
#[ignore = "docker"]
#[tokio::test]
async fn a_committed_step_releases_the_instance_claim_atomically() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();
    let instances = PgInstanceStore::new(pool.clone());

    // Park, then claim as the resuming replica would.
    commit_step(&pool, &step(instance_id)).await.unwrap();
    assert!(instances
        .claim(&dep_a(), instance_id, "host-a-101-deadbeef")
        .await
        .unwrap());

    // The re-park step commits with the owner attached.
    commit_step_with_timers_releasing(&pool, &step(instance_id), &[], Some("host-a-101-deadbeef"))
        .await
        .unwrap();

    let owner: Option<String> = sqlx::query_scalar(
        "SELECT claim_owner FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner, None, "the claim was released with the step");
    assert!(
        instances
            .claim(&dep_a(), instance_id, "host-b-202-cafebabe")
            .await
            .unwrap(),
        "another replica can resume it immediately"
    );
}

/// The in-step release is owner-scoped like every other claim write: a step committed by a
/// replica that does NOT hold the claim (a park of a fresh instance, or a stale owner) leaves
/// the standing claim untouched.
#[ignore = "docker"]
#[tokio::test]
async fn a_step_release_never_clears_another_replicas_claim() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();
    let instances = PgInstanceStore::new(pool.clone());

    commit_step(&pool, &step(instance_id)).await.unwrap();
    assert!(instances
        .claim(&dep_a(), instance_id, "host-b-202-cafebabe")
        .await
        .unwrap());

    commit_step_with_timers_releasing(&pool, &step(instance_id), &[], Some("host-a-101-deadbeef"))
        .await
        .unwrap();

    let owner: Option<String> = sqlx::query_scalar(
        "SELECT claim_owner FROM instance_state WHERE deployment_id=$1 AND instance_id=$2",
    )
    .bind(dep_a().as_str())
    .bind(instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner.as_deref(),
        Some("host-b-202-cafebabe"),
        "only the holder's own release clears a claim"
    );
}

/// A step's `@subjectKey` blind-index rows are written in the SAME transaction as the snapshot,
/// so a subject is discoverable via `subject_index` iff its instance actually persisted.
#[ignore = "docker"]
#[tokio::test]
async fn step_writes_subject_index_rows_atomically_with_the_snapshot() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    let mut s = step(instance_id);
    s.subjects = vec![StepSubject {
        subject_name: "customerId".to_owned(),
        blind_value: "deadbeefcafe".to_owned(),
    }];
    commit_step(&pool, &s).await.unwrap();

    // The disclosure query finds the instance by its subject blind — committed atomically.
    let subjects = PgSubjectIndexStore::new(pool);
    let found = subjects
        .find_instances(&dep_a(), "customerId", "deadbeefcafe")
        .await
        .unwrap();
    assert_eq!(found, vec![instance_id]);
}

#[ignore = "docker"]
#[tokio::test]
async fn induced_failure_before_commit_leaves_zero_rows_in_all_tables() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    // Crash injection: perform every step write, then abandon the transaction (rollback on
    // drop) — the process dying between write and commit.
    {
        let mut tx = sutra_persistence::scope::begin_deployment_tx(&pool, &dep_a())
            .await
            .unwrap();
        write_step_in(&mut tx, &step(instance_id)).await.unwrap();
        // tx dropped here WITHOUT commit.
    }

    assert_eq!(
        table_counts(&pool).await,
        (0, 0, 0, 0),
        "commit-or-nothing: nothing"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn alias_collision_rolls_back_the_whole_step() {
    let pool = fresh_pool().await;

    // A different live instance already owns the unique alias.
    let owner = Uuid::new_v4();
    let alias_store = PgAliasStore::new(pool.clone());
    assert!(alias_store
        .record(&dep_a(), owner, "loanId", "L-1", true)
        .await
        .unwrap());

    let instance_id = Uuid::new_v4();
    let err = commit_step(&pool, &step(instance_id)).await.unwrap_err();
    assert!(
        matches!(err, PersistenceError::AliasCollision { .. }),
        "got: {err}"
    );

    // NOTHING from the failed step landed: no snapshot, no wait rows, no outbox rows, and
    // the alias table still holds only the original owner's row.
    let (instances, waits, aliases, outbox) = table_counts(&pool).await;
    assert_eq!((instances, waits, outbox), (0, 0, 0));
    assert_eq!(aliases, 1);
    assert_eq!(
        alias_store
            .find_live(&dep_a(), "loanId", "L-1")
            .await
            .unwrap(),
        Some(owner)
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn wait_to_wait_step_resolves_prior_frontier_and_writes_next() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    // Quiescent point 1: parked at waitApproval.
    commit_step(&pool, &step(instance_id)).await.unwrap();

    // Quiescent point 2 (a relay satisfied waitApproval; execution ran to waitDisbursement):
    // one transaction resolves the old wait, rewrites the snapshot, parks the new wait.
    let next = StepWrite {
        deployment: dep_a(),
        instance_id,
        snapshot: InstanceSnapshot::of_suspended(
            "loan",
            dep_a().as_str(),
            vec![
                "start".to_owned(),
                "score".to_owned(),
                "waitApproval".to_owned(),
            ],
            BTreeMap::from([("approved".to_owned(), "true".to_owned())]),
            vec!["waitDisbursement".to_owned()],
            "start",
            2,
        )
        .write(),
        waits: vec![StepWait {
            node_id: "waitDisbursement".to_owned(),
            process_id: "loan".to_owned(),
            correlation_key: None,
        }],
        resolved_waits: vec!["waitApproval".to_owned()],
        withdrawn_call_nodes: Vec::new(),
        aliases: vec![],
        subjects: vec![],
        outbox: vec![],
    };
    commit_step(&pool, &next).await.unwrap();

    let wait_store = PgWaitStateStore::new(pool.clone());
    let waiting = wait_store
        .list_waiting(&dep_a(), &WaitingFilter::default(), 10, 0)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].node_id, "waitDisbursement");

    let resolved_filter = WaitingFilter {
        status: Some(sutra_persistence::stores::STATUS_RESOLVED.to_owned()),
        ..Default::default()
    };
    let resolved = wait_store
        .list_waiting(&dep_a(), &resolved_filter, 10, 0)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].node_id, "waitApproval");

    let instance_store = PgInstanceStore::new(pool);
    let snap = InstanceSnapshot::read(
        &instance_store
            .load(&dep_a(), instance_id)
            .await
            .unwrap()
            .unwrap()
            .serialised,
    )
    .unwrap();
    assert_eq!(snap.waiting_nodes(), ["waitDisbursement".to_owned()]);
    assert_eq!(snap.audit_seq(), 2);
}

#[ignore = "docker"]
#[tokio::test]
async fn step_rejects_foreign_outbox_entries() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();
    let mut bad = step(instance_id);
    bad.outbox[0].instance_id = Uuid::new_v4(); // some other instance's emission

    let err = commit_step(&pool, &bad).await.unwrap_err();
    assert!(matches!(err, PersistenceError::InvalidArgument(_)));
    assert_eq!(table_counts(&pool).await, (0, 0, 0, 0));
}

// ============================ channel-call retry: withdrawal + fresh wait (F1) ============

/// The backoff-park step's two F1-specific writes, proven against the real schema:
///
/// 1. `withdrawn_call_nodes` DELETES the dead attempt's outbox rows — pending and poisoned
///    alike — by `(instance, node)`, in the same transaction, leaving other nodes' rows
///    untouched. A superseded request delivered late would double-submit against the
///    re-drive's fresh emission; poisoned later it would mis-fire a failure at the live
///    attempt.
/// 2. A node the SAME step resolved and re-parks gets the FRESH wait upsert: the dead
///    incarnation's TIMER kind / already-elapsed due-at must not ride the new MESSAGE wait
///    (the poller would claim it forever as a phantom fire), while a node the step did NOT
///    resolve keeps the plain upsert that preserves a pending timer's due-at.
#[ignore = "docker"]
#[tokio::test]
async fn a_backoff_park_withdraws_the_dead_attempts_rows_and_the_redrive_resets_the_wait() {
    use sutra_persistence::step::{commit_step_with_timers, StepTimerWait};

    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();
    let now = now_micros();

    // ---- the original park: Call waits (MESSAGE), its request emission enqueued ----------
    let mut call_request = outbox_entry(instance_id, "attempt-1");
    call_request.node_id = Some("Call".to_owned());
    let mut other_send = outbox_entry(instance_id, "unrelated-send");
    other_send.node_id = Some("Notify".to_owned());
    let park = StepWrite {
        deployment: dep_a(),
        instance_id,
        snapshot: snapshot_bytes("alice"),
        waits: vec![StepWait {
            node_id: "Call".to_owned(),
            process_id: "loan".to_owned(),
            correlation_key: None,
        }],
        resolved_waits: vec![],
        withdrawn_call_nodes: Vec::new(),
        aliases: vec![],
        subjects: vec![],
        outbox: vec![call_request.clone(), other_send],
    };
    commit_step_with_timers(
        &pool,
        &park,
        &[StepTimerWait {
            node_id: "Call#timeout".to_owned(),
            process_id: "loan".to_owned(),
            due_at: now,
        }],
    )
    .await
    .unwrap();
    // Poison the attempt's request row (the operator-configured ceiling exhausted).
    let outbox_store = PgOutboxStore::new(pool.clone());
    outbox_store
        .mark_poisoned(&dep_a(), call_request.entry_id, Some("{}"))
        .await
        .unwrap();
    assert!(outbox_store
        .poisoned_exists_for_node(&dep_a(), instance_id, "Call")
        .await
        .unwrap());
    assert!(!outbox_store
        .poisoned_exists_for_node(&dep_a(), instance_id, "Notify")
        .await
        .unwrap());

    // ---- the BACKOFF PARK: resolve the attempt's rows, withdraw its outbox rows ----------
    let backoff = StepWrite {
        resolved_waits: vec!["Call".to_owned(), "Call#timeout".to_owned()],
        withdrawn_call_nodes: vec!["Call".to_owned()],
        aliases: vec![],
        outbox: vec![],
        ..park.clone()
    };
    commit_step_with_timers(
        &pool,
        &backoff,
        &[StepTimerWait {
            node_id: "Call".to_owned(),
            process_id: "loan".to_owned(),
            due_at: now,
        }],
    )
    .await
    .unwrap();
    // The dead attempt's rows (the POISONED one included) are gone; the unrelated node's row
    // survives untouched.
    let by_node: Vec<(Option<String>,)> =
        sqlx::query_as("SELECT node_id FROM outbox_entry WHERE instance_id = $1")
            .bind(instance_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(by_node.len(), 1, "{by_node:?}");
    assert_eq!(by_node[0].0.as_deref(), Some("Notify"));
    assert!(!outbox_store
        .poisoned_exists_for_node(&dep_a(), instance_id, "Call")
        .await
        .unwrap());
    // The node's single row is now the backoff TIMER (same PK, new incarnation).
    let (kind, status): (String, String) = sqlx::query_as(
        "SELECT kind, status FROM waiting_event WHERE instance_id = $1 AND node_id = 'Call'",
    )
    .bind(instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((kind.as_str(), status.as_str()), ("TIMER", "WAITING"));

    // ---- the RE-DRIVE: the backoff row resolves into a FRESH MESSAGE wait ----------------
    let mut fresh_request = outbox_entry(instance_id, "attempt-2");
    fresh_request.node_id = Some("Call".to_owned());
    let redrive = StepWrite {
        resolved_waits: vec!["Call".to_owned()],
        withdrawn_call_nodes: Vec::new(),
        aliases: vec![],
        outbox: vec![fresh_request],
        ..park.clone()
    };
    commit_step_with_timers(
        &pool,
        &redrive,
        &[StepTimerWait {
            node_id: "Call#timeout".to_owned(),
            process_id: "loan".to_owned(),
            due_at: now,
        }],
    )
    .await
    .unwrap();
    // FRESH incarnation: kind reset to MESSAGE, no leftover due-at — the dead backoff's
    // elapsed timer must not ride the response wait as a phantom fire.
    let (kind, status, due): (String, String, Option<time::OffsetDateTime>) = sqlx::query_as(
        "SELECT kind, status, timer_due_at FROM waiting_event \
         WHERE instance_id = $1 AND node_id = 'Call'",
    )
    .bind(instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((kind.as_str(), status.as_str()), ("MESSAGE", "WAITING"));
    assert!(due.is_none(), "the dead backoff's due-at must not survive");
    // The fresh attempt's request rode the same commit.
    let fresh: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_entry WHERE instance_id = $1 AND node_id = 'Call'",
    )
    .bind(instance_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fresh, 1);
}
