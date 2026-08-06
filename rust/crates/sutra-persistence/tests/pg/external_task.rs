//! `PgExternalTaskStore` against a real PostgreSQL: idempotent parking, the fetch-and-lock
//! claim (channel filter, deployment isolation, batch cap), the invisibility of a locked task,
//! expiry making it fetchable again with no sweeper, the ownership guards on hold/fail, the
//! terminal `failed` posture, and the GUC-scoped pending count.

use std::collections::BTreeMap;

use sutra_persistence::stores::{ExternalTaskRow, ExternalTaskStore, PgExternalTaskStore};
use time::Duration;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool, now_micros};

fn task(channel: &str, key: &str) -> ExternalTaskRow {
    let now = now_micros();
    ExternalTaskRow {
        deployment: dep_a(),
        task_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        channel: channel.to_owned(),
        tenant: "acme".to_owned(),
        module_key: "acme/demoflow/1.0.0".to_owned(),
        body: b"{\"ask\":1}".to_vec().into(),
        content_type: Some("application/json".to_owned()),
        headers: BTreeMap::from([("x-uetr".to_owned(), "UETR-1".to_owned())]),
        outbox_key: key.to_owned(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned()),
        created_at: now,
        fetchable_at: now,
        lock_owner: None,
        lock_expires_at: None,
        attempt_count: 0,
        retries_left: 3,
        failed: false,
        last_error: None,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn park_then_fetch_and_lock_round_trips_every_column() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    let parked = task("score-in", "ob-1");

    assert!(store.park(&parked).await.unwrap(), "a fresh key parks");

    let now = now_micros();
    let locked = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();

    assert_eq!(locked.len(), 1);
    let got = &locked[0];
    assert_eq!(got.task_id, parked.task_id);
    assert_eq!(got.body.get(), parked.body.get());
    assert_eq!(got.headers, parked.headers, "correlation headers survive");
    assert_eq!(got.traceparent, parked.traceparent);
    assert_eq!(got.outbox_key, "ob-1");
    assert_eq!(got.lock_owner.as_deref(), Some("worker-1"));
    assert_eq!(
        got.attempt_count, 1,
        "the claim counts the hand-out — the honest record of how often this was tried"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn parking_the_same_outbox_key_twice_is_a_no_op() {
    // The outbox delivers at-least-once, so the SAME row can reach the pull sink twice. The
    // second park must not produce a second task for a worker to execute.
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    let first = task("score-in", "ob-dup");
    let mut second = task("score-in", "ob-dup");
    second.task_id = Uuid::new_v4();

    assert!(store.park(&first).await.unwrap());
    assert!(
        !store.park(&second).await.unwrap(),
        "a re-delivered outbox row parks idempotently"
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn a_locked_task_is_invisible_to_other_workers_until_the_lock_expires() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    store.park(&task("score-in", "ob-lock")).await.unwrap();

    let now = now_micros();
    let first = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 1);

    // A second worker sees nothing while the lock holds.
    let contended = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-2",
            now,
            now + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();
    assert!(contended.is_empty(), "a held lock hides the task");

    // Past the expiry the SAME query returns it — expiry is in the claim predicate, so an
    // abandoned task needs no sweeper to come back.
    let later = now + Duration::seconds(31);
    let reclaimed = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-2",
            later,
            later + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].lock_owner.as_deref(), Some("worker-2"));
    assert_eq!(reclaimed[0].attempt_count, 2);
}

#[ignore = "docker"]
#[tokio::test]
async fn the_claim_filters_by_channel_and_deployment_and_honours_the_batch_cap() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    for i in 0..3 {
        store
            .park(&task("score-in", &format!("ob-s{i}")))
            .await
            .unwrap();
    }
    store.park(&task("other-in", "ob-other")).await.unwrap();
    let mut elsewhere = task("score-in", "ob-elsewhere");
    elsewhere.deployment = dep_b();
    store.park(&elsewhere).await.unwrap();

    let now = now_micros();
    let batch = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + Duration::seconds(30),
            2,
        )
        .await
        .unwrap();
    assert_eq!(batch.len(), 2, "maxTasks caps the batch");
    assert!(batch.iter().all(|t| t.channel == "score-in"));
    assert!(batch.iter().all(|t| t.deployment == dep_a()));

    // The other deployment's task is untouched by dep_a's claim.
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn hold_and_fail_are_ownership_guarded() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    let parked = task("score-in", "ob-guard");
    store.park(&parked).await.unwrap();

    let now = now_micros();
    store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();

    // A stale worker cannot take the row.
    assert!(
        store
            .hold(
                &dep_a(),
                parked.task_id,
                "worker-2",
                now,
                now + Duration::seconds(30)
            )
            .await
            .unwrap()
            .is_none(),
        "a foreign worker never wins the guard"
    );
    assert!(
        !store
            .fail(
                &dep_a(),
                parked.task_id,
                "worker-2",
                now,
                2,
                now,
                "not mine"
            )
            .await
            .unwrap(),
        "a foreign worker cannot fail the task either"
    );

    // The owner does.
    let held = store
        .hold(
            &dep_a(),
            parked.task_id,
            "worker-1",
            now,
            now + Duration::seconds(60),
        )
        .await
        .unwrap()
        .expect("the owner holds the lock");
    assert_eq!(held.lock_owner.as_deref(), Some("worker-1"));

    // An EXPIRED lock is not ownership either — the fail-closed case a stale worker hits.
    let after_expiry = now + Duration::seconds(61);
    assert!(
        store
            .hold(
                &dep_a(),
                parked.task_id,
                "worker-1",
                after_expiry,
                after_expiry + Duration::seconds(30)
            )
            .await
            .unwrap()
            .is_none(),
        "the owner's own EXPIRED lock is still a lost lock"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn a_failure_with_budget_left_returns_the_task_and_a_spent_budget_is_terminal() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    let parked = task("score-in", "ob-fail");
    store.park(&parked).await.unwrap();

    let now = now_micros();
    store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();
    assert!(store
        .fail(
            &dep_a(),
            parked.task_id,
            "worker-1",
            now,
            2,
            now + Duration::seconds(10),
            "boom"
        )
        .await
        .unwrap());

    // Deferred: not fetchable until the backoff elapses, then fetchable again.
    let too_soon = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-2",
            now + Duration::seconds(1),
            now + Duration::seconds(31),
            10,
        )
        .await
        .unwrap();
    assert!(too_soon.is_empty(), "the retry backoff holds it back");

    let after = now + Duration::seconds(11);
    let retried = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-2",
            after,
            after + Duration::seconds(30),
            10,
        )
        .await
        .unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].retries_left, 2);
    assert_eq!(retried[0].last_error.as_deref(), Some("boom"));

    // Spending the budget makes it TERMINAL — never fetched again, still inspectable.
    assert!(store
        .fail(
            &dep_a(),
            parked.task_id,
            "worker-2",
            after,
            0,
            after,
            "gave up"
        )
        .await
        .unwrap());
    let terminal = store.peek(&dep_a(), parked.task_id).await.unwrap().unwrap();
    assert!(terminal.failed);
    assert_eq!(terminal.last_error.as_deref(), Some("gave up"));
    let nothing = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-3",
            after + Duration::seconds(60),
            after + Duration::seconds(90),
            10,
        )
        .await
        .unwrap();
    assert!(nothing.is_empty(), "a terminal task is never fetched again");
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        0,
        "a terminal task is not work still moving — the retirement gate must not be pinned by it"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn concurrent_claims_hand_each_task_to_exactly_one_worker() {
    // The SKIP LOCKED proof, from the worker side: two replicas claiming the same channel at the
    // same instant must partition the tasks, never duplicate one.
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    for i in 0..6 {
        store
            .park(&task("score-in", &format!("ob-c{i}")))
            .await
            .unwrap();
    }

    let now = now_micros();
    let one = store.clone();
    let two = store.clone();
    let (a, b) = tokio::join!(
        async move {
            one.fetch_and_lock(
                &dep_a(),
                &["score-in".to_owned()],
                "worker-a",
                now,
                now + Duration::seconds(30),
                6,
            )
            .await
            .unwrap()
        },
        async move {
            two.fetch_and_lock(
                &dep_a(),
                &["score-in".to_owned()],
                "worker-b",
                now,
                now + Duration::seconds(30),
                6,
            )
            .await
            .unwrap()
        }
    );

    let mut ids: Vec<Uuid> = a.iter().chain(b.iter()).map(|t| t.task_id).collect();
    let claimed = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), claimed, "no task was handed to both workers");
    assert_eq!(claimed, 6, "between them the workers drained the channel");
}

#[ignore = "docker"]
#[tokio::test]
async fn delete_removes_the_completed_task() {
    let pool = fresh_pool().await;
    let store = PgExternalTaskStore::new(pool);
    let parked = task("score-in", "ob-done");
    store.park(&parked).await.unwrap();

    store.delete(&dep_a(), parked.task_id).await.unwrap();
    assert!(store
        .peek(&dep_a(), parked.task_id)
        .await
        .unwrap()
        .is_none());
    // A missing row is a no-op, not an error (the completion may race a redrive).
    store.delete(&dep_a(), parked.task_id).await.unwrap();
}
