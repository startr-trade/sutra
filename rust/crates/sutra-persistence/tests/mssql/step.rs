//! Mirrors `tests/pg/step.rs` — the strict transactional step,
//! commit-or-nothing proofs on the SQL Server dialect. Crash injection here is
//! connection teardown: an abandoned transaction's connection is discarded and the
//! server rolls back.

use std::collections::BTreeMap;

use sutra_persistence::mssql::step::{commit_step, write_step_in};
use sutra_persistence::mssql::stores::{
    MssqlAliasStore, MssqlInstanceStore, MssqlOutboxStore, MssqlWaitStateStore,
};
use sutra_persistence::mssql::{MssqlPool, MssqlTx};
use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::step::{StepAlias, StepWait, StepWrite};
use sutra_persistence::stores::{
    AliasStore, InstanceStore, OutboxEntry, OutboxStore, ReplyMode, WaitStateStore, WaitingFilter,
};
use sutra_persistence::PersistenceError;
use uuid::Uuid;

use crate::fixture::{count_all, dep_a, fresh_pool, now_micros};

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

async fn table_counts(pool: &MssqlPool) -> (i64, i64, i64, i64) {
    (
        count_all(pool, "instance_state").await,
        count_all(pool, "waiting_event").await,
        count_all(pool, "alias_index").await,
        count_all(pool, "outbox_entry").await,
    )
}

#[ignore = "docker"]
#[tokio::test]
async fn step_commits_snapshot_waits_aliases_and_outbox_atomically() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    commit_step(&pool, &step(instance_id)).await.unwrap();

    let instance_store = MssqlInstanceStore::new(pool.clone());
    let loaded = instance_store
        .load(&dep_a(), instance_id)
        .await
        .unwrap()
        .unwrap();
    let snap = InstanceSnapshot::read(&loaded.serialised).unwrap();
    assert!(snap.is_suspended());
    assert_eq!(snap.waiting_nodes(), ["waitApproval".to_owned()]);

    let wait_store = MssqlWaitStateStore::new(pool.clone());
    let waiting = wait_store
        .list_waiting(&dep_a(), &WaitingFilter::default(), 10, 0)
        .await
        .unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].instance_id, instance_id);

    let alias_store = MssqlAliasStore::new(pool.clone());
    assert_eq!(
        alias_store
            .find_live(&dep_a(), "loanId", "L-1")
            .await
            .unwrap(),
        Some(instance_id)
    );

    let outbox_store = MssqlOutboxStore::new(pool.clone());
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

#[ignore = "docker"]
#[tokio::test]
async fn induced_failure_before_commit_leaves_zero_rows_in_all_tables() {
    let pool = fresh_pool().await;
    let instance_id = Uuid::new_v4();

    // Crash injection: perform every step write, then abandon the transaction — the
    // connection is discarded (never re-pooled) and the server rolls back, exactly a
    // process dying between write and commit.
    {
        let mut tx = MssqlTx::begin(&pool).await.unwrap();
        write_step_in(tx.client(), &step(instance_id))
            .await
            .unwrap();
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
    let alias_store = MssqlAliasStore::new(pool.clone());
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

    // Quiescent point 2 (a relay satisfied waitApproval; execution ran to
    // waitDisbursement): one transaction resolves the old wait, rewrites the snapshot,
    // parks the new wait.
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

    let wait_store = MssqlWaitStateStore::new(pool.clone());
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

    let instance_store = MssqlInstanceStore::new(pool);
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
