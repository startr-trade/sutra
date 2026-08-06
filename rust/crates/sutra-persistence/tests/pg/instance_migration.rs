//! The two-scope instance-migration commit shape (P1-8) — `commit_instance_migration`.
//!
//! What only a real database can settle here is the RLS question. Every other commit shape in
//! `sutra_persistence::step` runs inside ONE deployment scope; this one reads rows pinned to the
//! source and writes rows pinned to the target. The last test in this file is the reason the shape
//! is what it is: under a genuine `NOBYPASSRLS` role, the "obvious" implementation — a single
//! `UPDATE … SET deployment_id = target` — writes ZERO rows no matter which value the GUC holds,
//! because the shipped policies have no explicit `WITH CHECK` and PostgreSQL then reuses `USING` for
//! it. The two-phase GUC flip inside one transaction is what actually works.

use std::collections::{BTreeMap, BTreeSet};

use sutra_persistence::snapshot::InstanceSnapshot;
use sutra_persistence::step::{commit_instance_migration, InstanceMigration};
use sutra_persistence::stores::{
    AliasStore, AuditEventRow, InstanceState, InstanceStore, PgAliasStore, PgAuditEventStore,
    PgInstanceStore, PgSubjectIndexStore, PgWaitStateStore, SubjectIndexStore, WaitStateStore,
};
use sutra_persistence::{DeploymentId, PersistenceError};
use uuid::Uuid;

use crate::fixture::{
    create_app_role, dep_a, dep_b, fresh_pool, fresh_pool_named, now_micros, role_pool,
};

const OWNER: &str = "replica-1::migrate";

fn snapshot(dep: &DeploymentId, waiting: &str, start: &str) -> Vec<u8> {
    InstanceSnapshot::of_suspended(
        "loan",
        dep.as_str(),
        vec![start.to_owned()],
        BTreeMap::from([("applicant".to_owned(), "A".to_owned())]),
        vec![waiting.to_owned()],
        start,
        3,
    )
    .write()
}

/// Park one instance under `dep_a`: snapshot + a message wait + a timer wait + a unique-live alias
/// + a blind-index row + three journal rows — one of every row class the move has to carry.
async fn parked(pool: &sqlx::PgPool, instance: Uuid) {
    let instances = PgInstanceStore::new(pool.clone());
    instances
        .persist(
            &dep_a(),
            &InstanceState {
                instance_id: instance,
                serialised: snapshot(&dep_a(), "waitApproval", "start"),
            },
        )
        .await
        .expect("persist");
    let waits = PgWaitStateStore::new(pool.clone());
    waits
        .record_waiting(&dep_a(), instance, "loan", "waitApproval", None)
        .await
        .expect("message wait");
    let mut tx = sutra_persistence::scope::begin_deployment_tx(pool, &dep_a())
        .await
        .expect("tx");
    PgWaitStateStore::record_timer_waiting_in(
        &mut tx,
        &dep_a(),
        instance,
        "loan",
        "waitApproval#timeout",
        now_micros() + time::Duration::hours(1),
    )
    .await
    .expect("timer wait");
    tx.commit().await.expect("commit");
    PgAliasStore::new(pool.clone())
        .record(&dep_a(), instance, "caseRef", "C-1", true)
        .await
        .expect("alias");
    PgSubjectIndexStore::new(pool.clone())
        .record(&dep_a(), instance, "customerId", "blind-abc")
        .await
        .expect("subject");
    let audit = PgAuditEventStore::new(pool.clone());
    for seq in 1..=3 {
        audit
            .insert(&AuditEventRow {
                deployment: dep_a(),
                instance_id: Some(instance),
                seq,
                at: now_micros(),
                event_type: "NODE_ENTERED".to_owned(),
                node_id: Some("waitApproval".to_owned()),
                diagnostic_code: None,
                diagnostic_json: None,
                payload_json: "{}".to_owned(),
            })
            .await
            .expect("journal row");
    }
    instances
        .claim(&dep_a(), instance, OWNER)
        .await
        .expect("claim");
}

fn migration(instance: Uuid, mapping: &[(&str, &str)]) -> InstanceMigration {
    let node_mapping: BTreeMap<String, String> = mapping
        .iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect();
    InstanceMigration {
        from: dep_a(),
        to: dep_b(),
        instance_id: instance,
        snapshot: InstanceSnapshot::migrate_pinned(
            &snapshot(&dep_a(), "waitApproval", "start"),
            dep_b().as_str(),
            None,
            &node_mapping,
            Some(4),
        )
        .expect("re-pin"),
        node_mapping,
        process_id: None,
        rearm_parks: None,
        claim_owner: OWNER.to_owned(),
        carry_journal: true,
        audit: Some(AuditEventRow {
            deployment: dep_b(),
            instance_id: Some(instance),
            seq: 4,
            at: now_micros(),
            event_type: "SUTRA.INSTANCE_MIGRATED".to_owned(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: Some("{}".to_owned()),
            payload_json: "{}".to_owned(),
        }),
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn the_move_carries_every_row_class_across_the_pin_in_one_transaction() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;

    let outcome = commit_instance_migration(
        &pool,
        &migration(
            instance,
            &[
                ("waitApproval", "approve"),
                ("waitApproval#timeout", "approve#timeout"),
            ],
        ),
    )
    .await
    .expect("migration commits");

    assert_eq!(outcome.wait_rows, 2, "message + timer park");
    assert_eq!(outcome.alias_rows, 1);
    assert_eq!(outcome.subject_rows, 1);
    assert_eq!(
        outcome.audit_rows, 3,
        "the journal travels with the instance"
    );

    // The source pin is EMPTY — no orphan rows left behind.
    let instances = PgInstanceStore::new(pool.clone());
    assert!(instances.load(&dep_a(), instance).await.unwrap().is_none());
    assert!(PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_a(), instance)
        .await
        .unwrap()
        .is_empty());
    assert!(PgAliasStore::new(pool.clone())
        .find_live(&dep_a(), "caseRef", "C-1")
        .await
        .unwrap()
        .is_none());

    // The target pin has all of it, with the node ids rewritten and the timer's due-at intact.
    let moved = instances
        .load(&dep_b(), instance)
        .await
        .unwrap()
        .expect("the row is under the target pin");
    let read = InstanceSnapshot::read(&moved.serialised).unwrap();
    assert_eq!(read.deployment_id(), dep_b().as_str());
    assert_eq!(read.waiting_nodes(), ["approve"]);
    assert_eq!(read.audit_seq(), 4, "bumped past the migration event");
    let waits = PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance)
        .await
        .unwrap();
    let nodes: BTreeSet<&str> = waits.iter().map(|w| w.node_id.as_str()).collect();
    assert_eq!(
        nodes,
        BTreeSet::from(["approve", "approve#timeout"]),
        "both parks landed on their mapped ids"
    );
    let timer = waits
        .iter()
        .find(|w| w.node_id == "approve#timeout")
        .expect("timer row");
    assert!(
        timer.timer_due_at.is_some(),
        "a park with an hour left still has an hour left"
    );
    assert_eq!(
        PgAliasStore::new(pool.clone())
            .find_live(&dep_b(), "caseRef", "C-1")
            .await
            .unwrap(),
        Some(instance),
        "relay correlation must keep resolving the instance"
    );
    assert_eq!(
        PgSubjectIndexStore::new(pool.clone())
            .find_instances(&dep_b(), "customerId", "blind-abc")
            .await
            .unwrap(),
        vec![instance],
        "GDPR erasure must keep finding it"
    );
    // Journal: the three carried rows PLUS the migration event, and the carried rows keep the node
    // ids they were WRITTEN with — a trail names where a move happened, not where it now maps.
    let journal = PgAuditEventStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance, 0, 100)
        .await
        .unwrap();
    assert_eq!(journal.len(), 4);
    assert!(journal
        .iter()
        .filter(|r| r.event_type == "NODE_ENTERED")
        .all(|r| r.node_id.as_deref() == Some("waitApproval")));
    assert_eq!(journal[3].event_type, "SUTRA.INSTANCE_MIGRATED");

    // The migrated row is UNOWNED — the claim died with the source row, so it is immediately
    // resumable without waiting out a sweep.
    assert!(
        instances
            .claim(&dep_b(), instance, "another-replica")
            .await
            .unwrap(),
        "a fresh owner can claim the migrated row"
    );
}

// ---- v2: the cross-process re-home and the migrate-then-resume re-arm -------------------------

#[ignore = "docker"]
#[tokio::test]
async fn a_cross_process_move_re_homes_the_wait_rows_process_id_with_the_snapshot() {
    // The snapshot's `sutra.processId` and the rows' `process_id` column must move TOGETHER: the
    // timer poller reports the row's process id and the admin listing renders it, so a row still
    // naming the source process would describe the instance as living somewhere it no longer does.
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;

    let mut rehome = migration(
        instance,
        &[
            ("waitApproval", "approve"),
            ("waitApproval#timeout", "approve#timeout"),
        ],
    );
    rehome.process_id = Some("loan-v2".to_owned());
    rehome.snapshot = InstanceSnapshot::migrate_pinned(
        &snapshot(&dep_a(), "waitApproval", "start"),
        dep_b().as_str(),
        Some("loan-v2"),
        &rehome.node_mapping,
        Some(4),
    )
    .expect("re-pin");
    commit_instance_migration(&pool, &rehome)
        .await
        .expect("the cross-process move commits");

    let moved = PgInstanceStore::new(pool.clone())
        .load(&dep_b(), instance)
        .await
        .unwrap()
        .expect("row under the target pin");
    assert_eq!(
        InstanceSnapshot::read(&moved.serialised)
            .unwrap()
            .process_id(),
        "loan-v2"
    );
    let waits = PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance)
        .await
        .unwrap();
    assert_eq!(waits.len(), 2);
    assert!(
        waits.iter().all(|w| w.process_id == "loan-v2"),
        "every carried park names the process the instance now lives in: {waits:?}"
    );
}

/// Tear the instance's live parks down exactly as the failure commit does: ONE statement, so every
/// row it touches carries the same `resolved_at` — the fact the re-arm rule keys on.
async fn fail_the_parks(pool: &sqlx::PgPool, instance: Uuid) {
    let mut tx = sutra_persistence::scope::begin_deployment_tx(pool, &dep_a())
        .await
        .expect("tx");
    sqlx::query(
        "UPDATE waiting_event SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
         WHERE deployment_id = $1 AND instance_id = $2 AND status = 'WAITING'",
    )
    .bind(dep_a().as_str())
    .bind(instance)
    .execute(&mut *tx)
    .await
    .expect("resolve the live parks");
    tx.commit().await.expect("commit");
}

#[ignore = "docker"]
#[tokio::test]
async fn resume_re_arms_the_parks_the_failure_tore_down_including_the_ones_the_frontier_never_names(
) {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    fail_the_parks(&pool, instance).await;

    // The frontier names ONLY `waitApproval` — the `#timeout` boundary armed beside it has an id
    // the snapshot never records. Re-arming by frontier alone would silently drop the timeout, so
    // the rule is "the frontier's rows PLUS everything the failure resolved in that same statement".
    let mut resume = migration(instance, &[]);
    resume.rearm_parks = Some(BTreeSet::from(["waitApproval".to_owned()]));
    let outcome = commit_instance_migration(&pool, &resume)
        .await
        .expect("the migrate-then-resume move commits");
    assert_eq!(outcome.wait_rows, 2);
    assert_eq!(
        outcome.rearmed_rows, 2,
        "the park AND its timeout came back"
    );

    let waits = PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance)
        .await
        .unwrap();
    assert!(
        waits
            .iter()
            .all(|w| w.status == "WAITING" && w.resolved_at.is_none()),
        "the instance is parked again, through the ordinary rows: {waits:?}"
    );
    assert!(
        waits
            .iter()
            .find(|w| w.node_id == "waitApproval#timeout")
            .and_then(|w| w.timer_due_at)
            .is_some(),
        "and the re-armed timer kept its due-at, so the poller re-drives it on schedule"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn resume_without_a_durable_frontier_re_arms_nothing() {
    // An instance with no frontier had nothing parked, so re-arming an older satisfied wait would
    // be INVENTING a park rather than restoring one.
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    fail_the_parks(&pool, instance).await;

    let mut nothing = migration(instance, &[]);
    nothing.rearm_parks = Some(BTreeSet::new());
    let outcome = commit_instance_migration(&pool, &nothing)
        .await
        .expect("commits");
    assert_eq!(outcome.rearmed_rows, 0);
    assert!(PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance)
        .await
        .unwrap()
        .iter()
        .all(|w| w.status == "RESOLVED"));
}

#[ignore = "docker"]
#[tokio::test]
async fn resume_leaves_genuinely_spent_parks_resolved() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    // An EARLIER park, satisfied long before the failure: a different node, a strictly earlier
    // resolution. It is history, not a park, and resume must not resurrect it.
    let waits = PgWaitStateStore::new(pool.clone());
    waits
        .record_waiting(&dep_a(), instance, "loan", "collectDocs", None)
        .await
        .expect("earlier park");
    waits
        .resolve(&dep_a(), instance, "collectDocs")
        .await
        .expect("…satisfied");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    fail_the_parks(&pool, instance).await;

    let mut resume = migration(instance, &[]);
    resume.rearm_parks = Some(BTreeSet::from(["waitApproval".to_owned()]));
    let outcome = commit_instance_migration(&pool, &resume)
        .await
        .expect("commits");
    assert_eq!(outcome.wait_rows, 3);
    assert_eq!(outcome.rearmed_rows, 2, "only what the failure tore down");
    let moved = PgWaitStateStore::new(pool.clone())
        .list_for_instance(&dep_b(), instance)
        .await
        .unwrap();
    let spent = moved
        .iter()
        .find(|w| w.node_id == "collectDocs")
        .expect("the earlier park travelled");
    assert_eq!(
        spent.status, "RESOLVED",
        "a wait that was genuinely satisfied stays satisfied"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn a_claim_held_by_someone_else_refuses_and_moves_nothing() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    // Steal the claim: release ours, then let a resume path take it.
    let instances = PgInstanceStore::new(pool.clone());
    instances.release(&dep_a(), instance, OWNER).await.unwrap();
    assert!(instances
        .claim(&dep_a(), instance, "replica-2")
        .await
        .unwrap());

    let err = commit_instance_migration(&pool, &migration(instance, &[]))
        .await
        .expect_err("a migration must not move a row something else is advancing");
    assert!(
        format!("{err}").contains("replica-2"),
        "the refusal names the holder: {err}"
    );
    assert!(
        instances.load(&dep_a(), instance).await.unwrap().is_some(),
        "nothing moved"
    );
    assert!(instances.load(&dep_b(), instance).await.unwrap().is_none());
}

#[ignore = "docker"]
#[tokio::test]
async fn a_unique_live_alias_already_taken_under_the_target_rolls_the_whole_move_back() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    // A DIFFERENT live instance already owns `caseRef = C-1` under the target pin.
    let squatter = Uuid::new_v4();
    PgAliasStore::new(pool.clone())
        .record(&dep_b(), squatter, "caseRef", "C-1", true)
        .await
        .expect("squatter alias");

    let err = commit_instance_migration(&pool, &migration(instance, &[]))
        .await
        .expect_err("the unique-live guarantee is per-deployment and must hold under the target");
    assert!(
        matches!(err, PersistenceError::AliasCollision { .. }),
        "{err}"
    );

    let instances = PgInstanceStore::new(pool.clone());
    assert!(
        instances.load(&dep_a(), instance).await.unwrap().is_some(),
        "commit-or-nothing: the instance is still under its original pin"
    );
    assert!(instances.load(&dep_b(), instance).await.unwrap().is_none());
    assert_eq!(
        PgWaitStateStore::new(pool.clone())
            .list_for_instance(&dep_a(), instance)
            .await
            .unwrap()
            .len(),
        2,
        "and its wait rows came back with it"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn a_mapping_that_folds_two_parked_nodes_onto_one_is_refused_before_anything_is_written() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;

    let err = commit_instance_migration(
        &pool,
        &migration(
            instance,
            &[
                ("waitApproval", "approve"),
                ("waitApproval#timeout", "approve"),
            ],
        ),
    )
    .await
    .expect_err("two parks cannot share one node");
    assert!(format!("{err}").contains("approve"), "{err}");
    assert!(PgInstanceStore::new(pool.clone())
        .load(&dep_a(), instance)
        .await
        .unwrap()
        .is_some());
}

#[ignore = "docker"]
#[tokio::test]
async fn migrating_onto_the_same_pin_is_refused() {
    let pool = fresh_pool().await;
    let instance = Uuid::new_v4();
    parked(&pool, instance).await;
    let mut same = migration(instance, &[]);
    same.to = dep_a();
    assert!(commit_instance_migration(&pool, &same).await.is_err());
}

// ---- the RLS proof: why the GUC is flipped mid-transaction -------------------------------------

/// Under a genuine `NOBYPASSRLS` role the two-scope move commits — and the "obvious"
/// single-`UPDATE` implementation does not, at either GUC value.
///
/// This is the test that justifies the shape. The shipped policies are
/// `USING (deployment_id = current_setting('sutra.deployment_id', true))` with NO explicit
/// `WITH CHECK`, and PostgreSQL reuses `USING` as the `WITH CHECK` expression for `UPDATE`. So a
/// scope-CHANGING update has no GUC value that satisfies both ends: at the source the new row fails
/// the check, at the target the old row fails the scan.
#[ignore = "docker"]
#[tokio::test]
async fn the_two_scope_move_commits_under_an_enforcing_rls_role_where_a_plain_update_cannot() {
    let (admin, db) = fresh_pool_named().await;
    let role = create_app_role(
        &admin,
        &[
            "instance_state",
            "waiting_event",
            "alias_index",
            "subject_index",
            "audit_event",
        ],
    )
    .await;
    let app = role_pool(&db, &role).await;

    // (1) The naive scope-changing UPDATE fails at BOTH GUC values, and it fails DIFFERENTLY at
    //     each — which is precisely the shape of the trap.
    let instance = Uuid::new_v4();
    parked(&app, instance).await;
    let naive_update = |guc: DeploymentId| {
        let app = app.clone();
        async move {
            let mut tx = sutra_persistence::scope::begin_deployment_tx(&app, &guc)
                .await
                .expect("tx");
            let result = sqlx::query(
                "UPDATE instance_state SET deployment_id = $1 WHERE deployment_id = $2 AND \
                 instance_id = $3",
            )
            .bind(dep_b().as_str())
            .bind(dep_a().as_str())
            .bind(instance)
            .execute(&mut *tx)
            .await
            .map(|done| done.rows_affected());
            let _ = tx.rollback().await;
            result
        }
    };
    // GUC = SOURCE: the old row passes `USING`, and then the NEW row is rejected by the implied
    // `WITH CHECK` — an outright RLS violation, not a silent miss.
    let at_source = naive_update(dep_a()).await;
    let err = at_source.expect_err("the new row must violate the policy");
    assert!(
        format!("{err}").contains("row-level security"),
        "the implied WITH CHECK rejects the re-scoped row: {err}"
    );
    // GUC = TARGET: the old row is invisible to `USING`, so the statement succeeds and matches
    // NOTHING — the silent-corruption half of the trap.
    assert_eq!(
        naive_update(dep_b()).await.expect("no rows, no error"),
        0,
        "at the target scope the source row is not even visible to the update"
    );
    assert!(
        PgInstanceStore::new(app.clone())
            .load(&dep_a(), instance)
            .await
            .unwrap()
            .is_some(),
        "so the instance never moved"
    );

    // (2) The two-phase GUC flip inside ONE transaction does move it.
    commit_instance_migration(&app, &migration(instance, &[]))
        .await
        .expect("the two-scope move commits under enforcing RLS");
    let instances = PgInstanceStore::new(app.clone());
    assert!(instances.load(&dep_a(), instance).await.unwrap().is_none());
    assert!(instances.load(&dep_b(), instance).await.unwrap().is_some());
}
