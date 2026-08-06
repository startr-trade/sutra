//! Row-Level Security proofs: store-level cross-deployment
//! reads under a genuine `NOBYPASSRLS` login role return empty.
//!
//! These tests deliberately bypass the store layer where noted: raw SQL with
//! `SET LOCAL ROLE` + the `sutra.deployment_id` GUC, and queries that OMIT the
//! `deployment_id` WHERE clause — proving the DATABASE enforces isolation even when the
//! application layer misbehaves (belt to the explicit-bind braces).
//!
//! The last test inverts the lens: under that same enforcing posture, a store operation that
//! forgets the GUC reads ZERO rows and silently answers wrong. That is the failure mode the
//! outbox side of the DRAINING-retirement gate used to have.

use std::collections::BTreeMap;

use sutra_persistence::stores::{
    ExternalTaskRow, ExternalTaskStore, InstanceState, InstanceStore, OutboxEntry, OutboxStore,
    PgExternalTaskStore, PgInstanceStore, PgOutboxStore, ReplyMode,
};
use sutra_persistence::DeploymentId;
use uuid::Uuid;

use crate::fixture::{create_app_role, dep_a, dep_b, fresh_pool_named, now_micros, role_pool};

/// Runs one statement batch as `role` with the GUC set (or not) inside a transaction.
async fn count_as(pool: &sqlx::PgPool, role: &str, guc: Option<&DeploymentId>, table: &str) -> i64 {
    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    if let Some(dep) = guc {
        sutra_persistence::scope::set_deployment_guc(&mut tx, dep)
            .await
            .unwrap();
    }
    // Intentionally NO deployment_id WHERE clause: only RLS can be doing the filtering.
    let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    count
}

async fn insert_instance_as(
    pool: &sqlx::PgPool,
    role: &str,
    guc: &DeploymentId,
    row_dep: &DeploymentId,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('sutra.deployment_id', $1, true)")
        .bind(guc.as_str())
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO instance_state (deployment_id, instance_id, serialised) VALUES ($1, $2, $3)",
    )
    .bind(row_dep.as_str())
    .bind(Uuid::new_v4())
    .bind(b"rls-bytes".as_slice())
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

#[ignore = "docker"]
#[tokio::test]
async fn instance_state_cross_deployment_select_blocked() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["instance_state"]).await;

    insert_instance_as(&pool, &role, &dep_a(), &dep_a())
        .await
        .unwrap();

    assert_eq!(
        count_as(&pool, &role, Some(&dep_b()), "instance_state").await,
        0
    );
    assert_eq!(
        count_as(&pool, &role, Some(&dep_a()), "instance_state").await,
        1
    );
}

/// Terminal retention (P1-2) changed WHAT lives in `instance_state`, not who may see it: a
/// retained COMPLETED row is isolated exactly like a parked one. Worth pinning separately because
/// a retained row now lingers for a whole retention window — the exposure surface of a leak here
/// is days long, where before it was the duration of a step.
#[ignore = "docker"]
#[tokio::test]
async fn a_retained_terminal_instance_is_isolated_like_any_other_row() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["instance_state"]).await;

    insert_instance_as(&pool, &role, &dep_a(), &dep_a())
        .await
        .unwrap();
    // Mark it terminal as the owner (the engine's terminal step runs as the app role, but the
    // point under test is the READ, so the setup path does not matter).
    sqlx::query("UPDATE instance_state SET terminal_at = now() WHERE deployment_id = $1")
        .bind(dep_a().as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        count_as(&pool, &role, Some(&dep_b()), "instance_state").await,
        0,
        "a retained terminal row must not be visible to another deployment"
    );
    assert_eq!(
        count_as(&pool, &role, Some(&dep_a()), "instance_state").await,
        1
    );
}

/// The retention PURGE is a DELETE, and a delete that crosses a tenant boundary is a far worse
/// failure than a read that does. Under the enforcing posture, a purge running with ANOTHER
/// deployment's GUC removes nothing — the RLS policy is the backstop behind the store's explicit
/// `deployment_id` bind.
#[ignore = "docker"]
#[tokio::test]
async fn the_retention_purge_cannot_reach_another_deployments_terminal_rows() {
    let (pool, db) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["instance_state"]).await;

    insert_instance_as(&pool, &role, &dep_a(), &dep_a())
        .await
        .unwrap();
    sqlx::query("UPDATE instance_state SET terminal_at = now() - interval '1 day'")
        .execute(&pool)
        .await
        .unwrap();

    // Purge as dep-B, deliberately WITHOUT a deployment_id predicate — only RLS can protect
    // dep-A's row here.
    let app = role_pool(&db, &role).await;
    let mut tx = app.begin().await.unwrap();
    sutra_persistence::scope::set_deployment_guc(&mut tx, &dep_b())
        .await
        .unwrap();
    let purged = sqlx::query(
        "DELETE FROM instance_state WHERE terminal_at IS NOT NULL \
         AND terminal_at <= now() - make_interval(secs => $1)",
    )
    .bind(60.0_f64)
    .execute(&mut *tx)
    .await
    .unwrap()
    .rows_affected();
    tx.commit().await.unwrap();
    assert_eq!(
        purged, 0,
        "the purge must not cross the deployment boundary"
    );

    assert_eq!(
        count_as(&pool, &role, Some(&dep_a()), "instance_state").await,
        1,
        "dep-A's terminal row survives another deployment's sweep"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn inbox_seen_cross_deployment_select_blocked() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["inbox_seen"]).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sutra_persistence::scope::set_deployment_guc(&mut tx, &dep_a())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id) VALUES ($1, 'ch', 'evt-1')",
    )
    .bind(dep_a().as_str())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        count_as(&pool, &role, Some(&dep_b()), "inbox_seen").await,
        0
    );
    assert_eq!(
        count_as(&pool, &role, Some(&dep_a()), "inbox_seen").await,
        1
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn unset_deployment_guc_sees_nothing() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["inbox_seen"]).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sutra_persistence::scope::set_deployment_guc(&mut tx, &dep_a())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id) VALUES ($1, 'ch', 'evt-1')",
    )
    .bind(dep_a().as_str())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // No GUC: current_setting returns NULL, NULL = anything is NULL, RLS yields zero rows.
    assert_eq!(count_as(&pool, &role, None, "inbox_seen").await, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn table_owner_still_sees_everything() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["inbox_seen"]).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::raw_sql(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sutra_persistence::scope::set_deployment_guc(&mut tx, &dep_a())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO inbox_seen (deployment_id, channel, event_id) VALUES ($1, 'ch', 'evt-1')",
    )
    .bind(dep_a().as_str())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Documents the boundary: the migration/owner role bypasses RLS by default (production
    // hardening = FORCE ROW LEVEL SECURITY + dedicated NOBYPASSRLS app role).
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox_seen")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[ignore = "docker"]
#[tokio::test]
async fn cross_deployment_write_attempt_blocked() {
    let (pool, _) = fresh_pool_named().await;
    let role = create_app_role(&pool, &["instance_state"]).await;

    // Session bound to deployment A forging a deployment-B row: PostgreSQL applies the
    // USING expression as the implicit WITH CHECK and rejects the INSERT.
    let err = insert_instance_as(&pool, &role, &dep_a(), &dep_b())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("row-level security"),
        "expected an RLS violation, got: {err}"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn store_reads_under_nobypassrls_role_are_deployment_scoped() {
    // The acceptance check: run the ACTUAL store implementations over a NOBYPASSRLS
    // login connection and prove cross-deployment reads come back empty.
    let (owner_pool, db) = fresh_pool_named().await;
    let role = create_app_role(&owner_pool, &["instance_state"]).await;
    let app_pool = role_pool(&db, &role).await;

    let store = PgInstanceStore::new(app_pool);
    let s = InstanceState {
        instance_id: Uuid::new_v4(),
        serialised: b"scoped".to_vec(),
    };
    store.persist(&dep_a(), &s).await.unwrap();

    // Same store, other deployment: nothing.
    assert!(store.load(&dep_b(), s.instance_id).await.unwrap().is_none());
    assert_eq!(store.count_active(&dep_b()).await.unwrap(), 0);
    // And the right deployment still sees its row through the same unprivileged role.
    assert_eq!(store.count_active(&dep_a()).await.unwrap(), 1);
    assert!(store.load(&dep_a(), s.instance_id).await.unwrap().is_some());
}

/// One outbox row for `deployment`, due `due_in_secs` from now (negative = already due).
fn outbox_entry(deployment: &DeploymentId, due_in_secs: i64) -> OutboxEntry {
    let now = now_micros();
    OutboxEntry {
        deployment: deployment.clone(),
        entry_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        body: b"{\"ok\":true}".to_vec().into(),
        content_type: Some("application/json".to_owned()),
        destination: "https://consumer.example/callback".to_owned(),
        headers: BTreeMap::new(),
        required: true,
        mode: ReplyMode::Native,
        outbox_key: format!("key-{}", Uuid::new_v4()),
        cloud_event_json: None,
        auth_ref_json: None,
        labels: BTreeMap::new(),
        created_at: now,
        next_attempt_at: now + time::Duration::seconds(due_in_secs),
        attempt_count: 0,
        last_diagnostic_json: None,
        traceparent: None,
        node_id: None,
    }
}

#[ignore = "docker"]
#[tokio::test]
async fn outbox_pending_count_is_deployment_scoped_under_enforcing_rls() {
    // The retirement-gate regression. `quiescent_ids` retires a DRAINING deployment only
    // when instances AND outbox rows are both zero. Counting the outbox on the RAW pool
    // leaves `sutra.deployment_id` unset, so under a genuinely enforcing RLS posture the
    // policy compares against NULL and answers 0 — retiring a deployment whose replies are
    // still undelivered. The store's scoped count sets the GUC and answers the truth.
    let (owner_pool, db) = fresh_pool_named().await;
    let role = create_app_role(&owner_pool, &["outbox_entry"]).await;
    let app_pool = role_pool(&db, &role).await;
    let store = PgOutboxStore::new(app_pool.clone());

    store.enqueue(&outbox_entry(&dep_a(), -60)).await.unwrap();
    // Row-exists = pending: a backed-off row that `claim_due` would NOT return is still
    // undelivered work and must hold the gate shut.
    store.enqueue(&outbox_entry(&dep_a(), 3_600)).await.unwrap();
    store.enqueue(&outbox_entry(&dep_b(), -60)).await.unwrap();

    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        2,
        "both the due and the backed-off row count as pending"
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1
    );

    // The bug this replaces: the identical SQL on the raw pool, without the GUC.
    let unscoped: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = $1")
            .bind(dep_a().as_str())
            .fetch_one(&app_pool)
            .await
            .unwrap();
    assert_eq!(
        unscoped, 0,
        "a GUC-less count under enforcing RLS sees nothing — which is exactly how a \
         non-quiescent deployment used to get retired"
    );

    // Draining to zero is what the gate is waiting for.
    for claimed in store
        .claim_due(&dep_a(), now_micros() + time::Duration::hours(2), 10)
        .await
        .unwrap()
    {
        store.delete(&dep_a(), claimed.entry_id).await.unwrap();
    }
    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        0
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1,
        "the other deployment's row is untouched"
    );
}

/// One parked external task for `deployment` on `channel`.
fn external_task(deployment: &DeploymentId, channel: &str) -> ExternalTaskRow {
    let now = now_micros();
    ExternalTaskRow {
        deployment: deployment.clone(),
        task_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        channel: channel.to_owned(),
        tenant: "acme".to_owned(),
        module_key: "acme/demoflow/1.0.0".to_owned(),
        body: b"{\"ask\":1}".to_vec().into(),
        content_type: Some("application/json".to_owned()),
        headers: BTreeMap::new(),
        outbox_key: format!("key-{}", Uuid::new_v4()),
        traceparent: None,
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
async fn external_task_claims_and_counts_are_deployment_scoped_under_enforcing_rls() {
    // The pull surface's version of the outbox proof above. Two things must hold under a
    // genuinely enforcing role: a worker's fetch-and-lock can NEVER reach another deployment's
    // parked task, and the pending count must go through the GUC-scoped path (a raw-pool count
    // answers 0, which is how a deployment whose workers still owe it results would get
    // retired).
    let (owner_pool, db) = fresh_pool_named().await;
    let role = create_app_role(&owner_pool, &["external_task"]).await;
    let app_pool = role_pool(&db, &role).await;
    let store = PgExternalTaskStore::new(app_pool.clone());

    store
        .park(&external_task(&dep_a(), "score-in"))
        .await
        .unwrap();
    store
        .park(&external_task(&dep_a(), "score-in"))
        .await
        .unwrap();
    store
        .park(&external_task(&dep_b(), "score-in"))
        .await
        .unwrap();

    assert_eq!(
        store.count_pending_for_deployment(&dep_a()).await.unwrap(),
        2
    );
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1
    );

    // A GUC-less count under enforcing RLS sees nothing — the failure mode the scoped path
    // exists to prevent.
    let unscoped: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM external_task WHERE deployment_id = $1")
            .bind(dep_a().as_str())
            .fetch_one(&app_pool)
            .await
            .unwrap();
    assert_eq!(unscoped, 0);

    // The claim itself is scoped: dep_a's worker drains dep_a's channel and cannot reach dep_b's
    // identically-named one.
    let now = now_micros();
    let claimed = store
        .fetch_and_lock(
            &dep_a(),
            &["score-in".to_owned()],
            "worker-1",
            now,
            now + time::Duration::seconds(30),
            50,
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|t| t.deployment == dep_a()));
    assert_eq!(
        store.count_pending_for_deployment(&dep_b()).await.unwrap(),
        1,
        "the other deployment's parked task is untouched"
    );
}
