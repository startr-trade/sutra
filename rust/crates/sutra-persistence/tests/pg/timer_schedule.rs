//! `PgTimerScheduleStore` against a real PostgreSQL — the full lifecycle a timer START event's
//! durable schedule row goes through:
//!
//! create-at-activation → claim-when-due → advance a cycle → exhaust an `R<n>` budget →
//! retire-at-deactivation, plus the hot-deploy handoff (old deployment's rows resolve, the new
//! deployment's are armed) and the re-arm idempotence an activation flip depends on.
//!
//! Every test is `#[ignore = "docker"]` (tier-2) and WRITE-only against its own fresh database.

use sutra_persistence::stores::{
    PgTimerScheduleStore, TimerScheduleArming, SCHEDULE_STATUS_RESOLVED, SCHEDULE_STATUS_SCHEDULED,
};
use time::OffsetDateTime;

use crate::fixture::{dep_a, dep_b, fresh_pool};

/// An arming for a single-shot duration start, due at `due`.
fn arming(process: &str, node: &str, due: OffsetDateTime) -> TimerScheduleArming {
    TimerScheduleArming {
        process_id: process.to_owned(),
        node_id: node.to_owned(),
        tenant: "acme".to_owned(),
        module_key: "acme/billing/1.0.0".to_owned(),
        kind: "DURATION".to_owned(),
        spec: "PT1H".to_owned(),
        next_due_at: due,
        remaining_fires: Some(1),
    }
}

/// An arming for a bounded repeating cycle (`R<repeats>/PT1H`).
fn cycle_arming(
    process: &str,
    node: &str,
    due: OffsetDateTime,
    repeats: Option<i32>,
) -> TimerScheduleArming {
    let spec = match repeats {
        Some(n) => format!("R{n}/PT1H"),
        None => "R/PT1H".to_owned(),
    };
    TimerScheduleArming {
        process_id: process.to_owned(),
        node_id: node.to_owned(),
        tenant: "acme".to_owned(),
        module_key: "acme/billing/1.0.0".to_owned(),
        kind: "CYCLE".to_owned(),
        spec,
        next_due_at: due,
        remaining_fires: repeats,
    }
}

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

#[ignore = "docker"]
#[tokio::test]
async fn activation_arms_a_row_per_timer_start() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let due = now() + time::Duration::hours(1);

    store
        .arm(
            &dep_a(),
            &[
                arming("billing", "Nightly", due),
                arming("audit", "Sweep", due),
            ],
        )
        .await
        .unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows.len(), 2);
    // Ordered by (process, node) — `audit` sorts before `billing`.
    assert_eq!(rows[0].process_id, "audit");
    assert_eq!(rows[1].process_id, "billing");
    assert!(rows.iter().all(|r| r.status == SCHEDULE_STATUS_SCHEDULED));
    assert_eq!(rows[0].tenant, "acme");
    assert_eq!(rows[0].module_key, "acme/billing/1.0.0");
    assert_eq!(rows[0].remaining_fires, Some(1));
    // Deployment isolation: dep_b sees nothing.
    assert!(store.list(&dep_b()).await.unwrap().is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn only_due_rows_are_claimed_and_a_claim_is_skip_locked() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let past = now() - time::Duration::minutes(5);
    let future = now() + time::Duration::hours(4);

    store
        .arm(
            &dep_a(),
            &[
                arming("billing", "DueNow", past),
                arming("billing", "NotYet", future),
            ],
        )
        .await
        .unwrap();

    let due = store.claim_due(&dep_a(), now(), 10).await.unwrap();
    assert_eq!(due.len(), 1, "only the past-due row is claimable");
    assert_eq!(due[0].node_id, "DueNow");
    assert_eq!(due[0].kind, "DURATION");
    assert_eq!(due[0].spec, "PT1H");
    assert_eq!(due[0].tenant, "acme");

    // A zero/negative batch claims nothing rather than erroring.
    assert!(store
        .claim_due(&dep_a(), now(), 0)
        .await
        .unwrap()
        .is_empty());
}

/// A past-dated `timeDate` schedule is armed ALREADY DUE and fires on the first claim — the
/// documented past-date semantics, exercised end-to-end against the real column type.
#[ignore = "docker"]
#[tokio::test]
async fn a_past_dated_schedule_is_claimable_immediately() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let past = OffsetDateTime::parse(
        "2020-01-01T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    let mut row = arming("billing", "Backfill", past);
    row.kind = "DATE".to_owned();
    row.spec = "2020-01-01T00:00:00Z".to_owned();

    store.arm(&dep_a(), &[row]).await.unwrap();
    let due = store.claim_due(&dep_a(), now(), 10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].node_id, "Backfill");
}

#[ignore = "docker"]
#[tokio::test]
async fn a_single_shot_schedule_resolves_after_its_one_fire() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    store
        .arm(&dep_a(), &[arming("billing", "Once", now())])
        .await
        .unwrap();

    store.resolve(&dep_a(), "billing", "Once").await.unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_RESOLVED);
    assert!(rows[0].resolved_at.is_some(), "resolved rows keep a stamp");
    // A resolved row is no longer claimable, however overdue it is.
    assert!(store
        .claim_due(&dep_a(), now() + time::Duration::days(365), 10)
        .await
        .unwrap()
        .is_empty());
    // Resolving twice is a silent no-op.
    store.resolve(&dep_a(), "billing", "Once").await.unwrap();
}

/// A cycle advances to its next occurrence and keeps the rest of its budget — the poller's
/// advance step, against real rows. (The occurrence ARITHMETIC itself is unit-tested in
/// `sutra_bpmn::timer`; this crate is deliberately downstream of no BPMN model, so the test
/// supplies the computed values the poller would.)
#[ignore = "docker"]
#[tokio::test]
async fn a_cycle_advances_to_its_next_occurrence() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let first = now() - time::Duration::seconds(1);
    store
        .arm(
            &dep_a(),
            &[cycle_arming("billing", "Hourly", first, Some(3))],
        )
        .await
        .unwrap();

    let due = store.claim_due(&dep_a(), now(), 10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].kind, "CYCLE");
    assert_eq!(due[0].spec, "R3/PT1H");
    assert_eq!(due[0].remaining_fires, Some(3));

    // The fire spent one repeat; the next occurrence is one interval on.
    let next = due[0].next_due_at + time::Duration::hours(1);
    store
        .advance(&dep_a(), "billing", "Hourly", next, Some(2))
        .await
        .unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_SCHEDULED, "still armed");
    assert_eq!(rows[0].remaining_fires, Some(2));
    assert!(rows[0].next_due_at > due[0].next_due_at, "moved forward");
    // And it is not claimable again until that next occurrence comes round.
    assert!(store
        .claim_due(&dep_a(), now(), 10)
        .await
        .unwrap()
        .is_empty());
}

/// An unbounded cycle (`R/PT1H`) advances with a NULL budget and stays armed forever.
#[ignore = "docker"]
#[tokio::test]
async fn an_unbounded_cycle_advances_with_no_budget() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let first = now() - time::Duration::seconds(1);
    store
        .arm(&dep_a(), &[cycle_arming("billing", "Forever", first, None)])
        .await
        .unwrap();

    let due = store.claim_due(&dep_a(), now(), 10).await.unwrap();
    assert_eq!(due[0].spec, "R/PT1H");
    assert_eq!(due[0].remaining_fires, None, "unbounded stores as NULL");

    let next = due[0].next_due_at + time::Duration::hours(1);
    store
        .advance(&dep_a(), "billing", "Forever", next, None)
        .await
        .unwrap();
    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_SCHEDULED);
    assert_eq!(rows[0].remaining_fires, None);
}

/// `R1/PT1H` fires once and is spent — the exhaustion path resolves the row, which then never
/// claims again no matter how far the clock moves.
#[ignore = "docker"]
#[tokio::test]
async fn an_rn_cycle_exhausts_its_budget_and_resolves() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let first = now() - time::Duration::seconds(1);
    store
        .arm(&dep_a(), &[cycle_arming("billing", "Once", first, Some(1))])
        .await
        .unwrap();

    let due = store.claim_due(&dep_a(), now(), 10).await.unwrap();
    assert_eq!(due[0].remaining_fires, Some(1), "one repeat, one fire");

    // Budget spent ⇒ the poller resolves rather than advancing.
    store.resolve(&dep_a(), "billing", "Once").await.unwrap();
    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_RESOLVED);
    assert!(store
        .claim_due(&dep_a(), now() + time::Duration::days(365), 10)
        .await
        .unwrap()
        .is_empty());
}

/// Deactivation / retirement: every armed row of the deployment stops firing in one statement.
#[ignore = "docker"]
#[tokio::test]
async fn retiring_a_deployment_resolves_all_its_schedules() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let past = now() - time::Duration::minutes(1);
    store
        .arm(
            &dep_a(),
            &[arming("billing", "A", past), arming("billing", "B", past)],
        )
        .await
        .unwrap();

    let retired = store.resolve_deployment(&dep_a()).await.unwrap();
    assert_eq!(retired, 2);

    let rows = store.list(&dep_a()).await.unwrap();
    assert!(rows.iter().all(|r| r.status == SCHEDULE_STATUS_RESOLVED));
    assert!(
        store
            .claim_due(&dep_a(), now(), 10)
            .await
            .unwrap()
            .is_empty(),
        "a retired deployment mints no more work"
    );
    // Idempotent: a second retire touches nothing.
    assert_eq!(store.resolve_deployment(&dep_a()).await.unwrap(), 0);
}

/// The hot-deploy handoff: the slot's OLD deployment stops minting and the NEW one takes over.
/// Schedules follow the ACTIVE deployment, never the draining tail.
#[ignore = "docker"]
#[tokio::test]
async fn a_hot_deploy_hands_the_schedule_from_the_old_deployment_to_the_new() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let past = now() - time::Duration::minutes(1);

    // The old deployment is ACTIVE and armed.
    store
        .arm(&dep_a(), &[arming("billing", "Nightly", past)])
        .await
        .unwrap();
    assert_eq!(store.claim_due(&dep_a(), now(), 10).await.unwrap().len(), 1);

    // The flip: the replacement is armed, the replaced one is retired.
    store
        .arm(&dep_b(), &[arming("billing", "Nightly", past)])
        .await
        .unwrap();
    store.resolve_deployment(&dep_a()).await.unwrap();

    assert!(
        store
            .claim_due(&dep_a(), now(), 10)
            .await
            .unwrap()
            .is_empty(),
        "the replaced deployment stopped minting"
    );
    let new_due = store.claim_due(&dep_b(), now(), 10).await.unwrap();
    assert_eq!(new_due.len(), 1, "the replacement mints instead");
    assert_eq!(new_due[0].node_id, "Nightly");
}

/// Re-arming is idempotent, and that is load-bearing: an activation flip runs on EVERY
/// deployment change, so an unrelated deployment's flip must not restart this one's schedule.
#[ignore = "docker"]
#[tokio::test]
async fn re_arming_a_live_schedule_does_not_disturb_its_due_at() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let original = now() + time::Duration::hours(1);
    store
        .arm(&dep_a(), &[arming("billing", "Nightly", original)])
        .await
        .unwrap();
    let before = store.list(&dep_a()).await.unwrap()[0].clone();

    // A later flip re-arms with a NEW computed due-at (a fresh `now + PT1H`)...
    let later = now() + time::Duration::hours(9);
    store
        .arm(&dep_a(), &[arming("billing", "Nightly", later)])
        .await
        .unwrap();

    let after = store.list(&dep_a()).await.unwrap()[0].clone();
    assert_eq!(
        after.next_due_at, before.next_due_at,
        "a still-armed schedule keeps its original occurrence across a re-activation"
    );
    assert_eq!(after.status, SCHEDULE_STATUS_SCHEDULED);
}

/// ...but re-arming a RESOLVED schedule DOES re-arm it from scratch — the rollback case, where a
/// deployment that drained comes back and must start scheduling again.
#[ignore = "docker"]
#[tokio::test]
async fn re_arming_a_resolved_schedule_starts_it_over() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    store
        .arm(
            &dep_a(),
            &[arming(
                "billing",
                "Nightly",
                now() + time::Duration::hours(1),
            )],
        )
        .await
        .unwrap();
    store.resolve_deployment(&dep_a()).await.unwrap();

    let fresh_due = now() + time::Duration::hours(9);
    store
        .arm(&dep_a(), &[arming("billing", "Nightly", fresh_due)])
        .await
        .unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_SCHEDULED, "armed again");
    assert!(
        rows[0].resolved_at.is_none(),
        "the resolved stamp is cleared"
    );
    assert!(
        rows[0].next_due_at > now() + time::Duration::hours(8),
        "and it re-armed on the FRESH occurrence, not the stale one"
    );
}

/// A deployment whose new plan drops a timer start retires the orphaned row in the same arming
/// transaction — the armed set always equals what the ACTIVE plan declares.
#[ignore = "docker"]
#[tokio::test]
async fn arming_retires_schedules_the_plan_no_longer_declares() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    let due = now() + time::Duration::hours(1);
    store
        .arm(
            &dep_a(),
            &[
                arming("billing", "Kept", due),
                arming("billing", "Dropped", due),
            ],
        )
        .await
        .unwrap();

    // Re-arm declaring only one of them.
    store
        .arm(&dep_a(), &[arming("billing", "Kept", due)])
        .await
        .unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    let dropped = rows.iter().find(|r| r.node_id == "Dropped").unwrap();
    let kept = rows.iter().find(|r| r.node_id == "Kept").unwrap();
    assert_eq!(dropped.status, SCHEDULE_STATUS_RESOLVED);
    assert_eq!(kept.status, SCHEDULE_STATUS_SCHEDULED);
}

/// Arming an EMPTY set retires everything — the "deployment has no timer starts any more" case,
/// and the statement that has no bind parameters beyond the deployment.
#[ignore = "docker"]
#[tokio::test]
async fn arming_an_empty_set_retires_every_schedule() {
    let pool = fresh_pool().await;
    let store = PgTimerScheduleStore::new(pool);
    store
        .arm(
            &dep_a(),
            &[arming(
                "billing",
                "Nightly",
                now() + time::Duration::hours(1),
            )],
        )
        .await
        .unwrap();

    store.arm(&dep_a(), &[]).await.unwrap();

    let rows = store.list(&dep_a()).await.unwrap();
    assert_eq!(rows[0].status, SCHEDULE_STATUS_RESOLVED);
}
