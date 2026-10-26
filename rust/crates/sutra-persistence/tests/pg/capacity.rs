//! `PgCapacitySlotStore` against a real PostgreSQL: concurrent acquirers across deployments never
//! exceed the pool, one slot per active call (two legs of one instance hold two), release of one
//! call, of a direction, or of everything keeps the shared count true, a delivered request of a
//! finished instance returns its slot, `require_live` refuses a finished instance, and the sweep
//! returns stranded slots while leaving live holders alone.

use std::time::Duration;

use sutra_persistence::stores::{
    PgCapacitySlotStore, PgInflightChainStore, SlotClaim, SlotGrant, SlotSelection, SLOT_INBOUND,
    SLOT_OUTBOUND,
};
use sutra_persistence::DeploymentId;
use uuid::Uuid;

use crate::fixture::{dep_a, dep_b, fresh_pool};

/// A plain `slots`-sized rule (no reserves).
fn within(slots: i32) -> impl Fn(i32, i32) -> bool + Send + Sync {
    move |inbound, outbound| inbound + outbound < slots
}

fn claim<'a>(
    deployment: &'a DeploymentId,
    instance: Uuid,
    direction: &'a str,
    holder: &'a str,
) -> SlotClaim<'a> {
    on_call(deployment, instance, "call-1", direction, holder)
}

fn on_call<'a>(
    deployment: &'a DeploymentId,
    instance: Uuid,
    call_key: &'a str,
    direction: &'a str,
    holder: &'a str,
) -> SlotClaim<'a> {
    SlotClaim {
        deployment,
        pool: "lines",
        tenant: "acme",
        instance,
        call_key,
        direction,
        holder,
        require_live: false,
    }
}

async fn slot_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM capacity_slot")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn store_instance(pool: &sqlx::PgPool, dep: &DeploymentId, id: Uuid, terminal: bool) {
    sqlx::query(
        "INSERT INTO instance_state (deployment_id, instance_id, serialised, terminal_at) \
         VALUES ($1, $2, '\\x00'::bytea, CASE WHEN $3 THEN now() ELSE NULL END)",
    )
    .bind(dep.as_str())
    .bind(id)
    .bind(terminal)
    .execute(pool)
    .await
    .unwrap();
}

#[ignore = "docker"]
#[tokio::test]
async fn concurrent_acquirers_across_deployments_never_exceed_the_pool() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let mut tasks = Vec::new();
    for i in 0..12 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let dep = if i % 2 == 0 { dep_a() } else { dep_b() };
            let direction = if i % 3 == 0 {
                SLOT_OUTBOUND
            } else {
                SLOT_INBOUND
            };
            store
                .acquire(
                    &claim(&dep, Uuid::new_v4(), direction, "host-1-1-a"),
                    &within(4),
                )
                .await
                .unwrap()
        }));
    }
    let mut acquired = 0;
    for task in tasks {
        match task.await.unwrap() {
            SlotGrant::Acquired => acquired += 1,
            SlotGrant::Exhausted { .. } => {}
            other => panic!("unexpected grant {other:?}"),
        }
    }
    assert_eq!(acquired, 4, "exactly the pool's 4 slots were granted");
    let (inbound, outbound) = store.usage("lines").await.unwrap();
    assert_eq!(inbound + outbound, 4);
    assert_eq!(slot_rows(&pool).await, 4);
}

#[ignore = "docker"]
#[tokio::test]
async fn one_slot_per_call_and_release_and_rebind_keep_the_count() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let admission = Uuid::new_v4();
    let instance = Uuid::new_v4();
    let dep = dep_a();

    assert_eq!(
        store
            .acquire(
                &claim(&dep, admission, SLOT_INBOUND, "host-1-1-a"),
                &within(2)
            )
            .await
            .unwrap(),
        SlotGrant::Acquired
    );
    store.rebind(&dep, admission, instance).await.unwrap();
    // The same call already holds its slot: a request on it takes none.
    assert_eq!(
        store
            .acquire(
                &claim(&dep, instance, SLOT_OUTBOUND, "host-1-1-a"),
                &within(3)
            )
            .await
            .unwrap(),
        SlotGrant::AlreadyHeld
    );
    // A second leg of the same instance is a second call: a second slot.
    assert_eq!(
        store
            .acquire(
                &on_call(&dep, instance, "call-2", SLOT_OUTBOUND, "host-1-1-a"),
                &within(3)
            )
            .await
            .unwrap(),
        SlotGrant::Acquired
    );
    assert_eq!(store.usage("lines").await.unwrap(), (1, 1));
    // The admission key no longer holds anything.
    assert_eq!(store.release(&dep, admission).await.unwrap(), 0);
    assert_eq!(store.release(&dep, instance).await.unwrap(), 2);
    assert_eq!(
        store.release(&dep, instance).await.unwrap(),
        0,
        "idempotent"
    );
    assert_eq!(store.usage("lines").await.unwrap(), (0, 0));
}

#[ignore = "docker"]
#[tokio::test]
async fn require_live_refuses_a_finished_instance() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let finished = Uuid::new_v4();
    store_instance(&pool, &dep_a(), finished, true).await;
    let dep = dep_a();
    let mut request = claim(&dep, finished, SLOT_OUTBOUND, "host-1-1-a");
    request.require_live = true;
    assert_eq!(
        store.acquire(&request, &within(5)).await.unwrap(),
        SlotGrant::InstanceEnded
    );
    assert_eq!(slot_rows(&pool).await, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn the_sweep_returns_stranded_slots_and_keeps_live_holders() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let markers = PgInflightChainStore::new(pool.clone());
    let dep = dep_a();
    let me = "host-1-222-bbbb";
    let earlier = "host-1-111-aaaa";

    let finished = Uuid::new_v4(); // stored, terminal → released
    let parked = Uuid::new_v4(); // stored, live → kept whoever holds it
    let crashed = Uuid::new_v4(); // unstored, earlier process on this host → released
    let chain = Uuid::new_v4(); // unstored, earlier process, live chain marker → kept
    let mine = Uuid::new_v4(); // unstored, this process, recent → kept
    store_instance(&pool, &dep, finished, true).await;
    store_instance(&pool, &dep, parked, false).await;
    markers.begin(&dep, chain, &[], earlier).await.unwrap();
    for (id, holder) in [
        (finished, me),
        (parked, earlier),
        (crashed, earlier),
        (chain, earlier),
        (mine, me),
    ] {
        assert_eq!(
            store
                .acquire(&claim(&dep, id, SLOT_INBOUND, holder), &within(10))
                .await
                .unwrap(),
            SlotGrant::Acquired
        );
    }

    let released = store
        .sweep(&dep, "host-1", me, Duration::from_secs(900))
        .await
        .unwrap();
    assert_eq!(released, 2, "the finished and the crashed holder's slots");
    assert_eq!(store.usage("lines").await.unwrap(), (3, 0));

    // Past the bound, an unstored slot with no marker is returned whoever holds it.
    let released = store
        .sweep(&dep, "host-1", me, Duration::from_secs(0))
        .await
        .unwrap();
    assert_eq!(released, 1, "this process's own stale unstored slot");
    assert_eq!(store.usage("lines").await.unwrap(), (2, 0));
    // The parked instance and the live chain keep theirs.
    assert_eq!(slot_rows(&pool).await, 2);
}

#[ignore = "docker"]
#[tokio::test]
async fn hanging_up_one_leg_frees_one_slot_and_the_end_frees_the_rest() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let dep = dep_a();
    let bridge = Uuid::new_v4();
    for (call, direction) in [
        ("leg-a", SLOT_INBOUND),
        ("leg-b", SLOT_OUTBOUND),
        ("leg-c", SLOT_OUTBOUND),
    ] {
        assert_eq!(
            store
                .acquire(
                    &on_call(&dep, bridge, call, direction, "host-1-1-a"),
                    &within(5)
                )
                .await
                .unwrap(),
            SlotGrant::Acquired
        );
    }
    assert_eq!(store.usage("lines").await.unwrap(), (1, 2));

    let freed = store
        .release_calls(&dep, bridge, SlotSelection::Call("leg-b"))
        .await
        .unwrap();
    assert_eq!(freed, vec![SLOT_OUTBOUND.to_string()]);
    assert_eq!(store.usage("lines").await.unwrap(), (1, 1));
    assert!(
        store
            .release_calls(&dep, bridge, SlotSelection::Call("leg-b"))
            .await
            .unwrap()
            .is_empty(),
        "idempotent"
    );

    // The inbound call by direction (on-reply), then End takes what is left.
    let freed = store
        .release_calls(&dep, bridge, SlotSelection::Direction(SLOT_INBOUND))
        .await
        .unwrap();
    assert_eq!(freed, vec![SLOT_INBOUND.to_string()]);
    assert_eq!(store.usage("lines").await.unwrap(), (0, 1));
    assert_eq!(store.release(&dep, bridge).await.unwrap(), 1);
    assert_eq!(store.usage("lines").await.unwrap(), (0, 0));
    assert_eq!(slot_rows(&pool).await, 0);
}

#[ignore = "docker"]
#[tokio::test]
async fn a_delivered_request_returns_its_slot_only_once_its_instance_is_over() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let markers = PgInflightChainStore::new(pool.clone());
    let dep = dep_a();
    let live = Uuid::new_v4(); // stored, parked
    let finished = Uuid::new_v4(); // stored, terminal
    let purged = Uuid::new_v4(); // no row, no chain marker: over
    let chain = Uuid::new_v4(); // no row, live chain marker: running
    store_instance(&pool, &dep, live, false).await;
    store_instance(&pool, &dep, finished, true).await;
    markers.begin(&dep, chain, &[], "host-1-1-a").await.unwrap();
    for id in [live, finished, purged, chain] {
        store
            .acquire(
                &on_call(&dep, id, "send-1", SLOT_OUTBOUND, "host-1-1-a"),
                &within(10),
            )
            .await
            .unwrap();
    }
    for (id, expected) in [(live, 0), (finished, 1), (purged, 1), (chain, 0)] {
        assert_eq!(
            store
                .release_call_if_over(&dep, id, "send-1")
                .await
                .unwrap(),
            expected,
            "{id}"
        );
    }
    assert_eq!(store.usage("lines").await.unwrap(), (0, 2));
}

#[ignore = "docker"]
#[tokio::test]
async fn concurrent_releases_of_one_call_count_it_once() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let dep = dep_a();
    let instance = Uuid::new_v4();
    for call in ["leg-a", "leg-b"] {
        store
            .acquire(
                &on_call(&dep, instance, call, SLOT_OUTBOUND, "host-1-1-a"),
                &within(5),
            )
            .await
            .unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let dep = dep.clone();
        tasks.push(tokio::spawn(async move {
            store
                .release_calls(&dep, instance, SlotSelection::Call("leg-a"))
                .await
                .unwrap()
                .len()
        }));
    }
    let mut freed = 0;
    for task in tasks {
        freed += task.await.unwrap();
    }
    assert_eq!(freed, 1);
    assert_eq!(store.usage("lines").await.unwrap(), (0, 1));
}

/// A session end releases the session's slots and is remembered: a later OUTBOUND claim of that
/// session (or of an ended call) answers `SessionEnded` and takes no slot, while another call of
/// a call-scoped end still draws from the pool. The sweep forgets the record once the instance
/// is over.
#[ignore = "docker"]
#[tokio::test]
async fn an_ended_session_takes_no_slot_for_its_outbound_requests() {
    let pool = fresh_pool().await;
    let store = PgCapacitySlotStore::new(pool.clone());
    let dep = dep_a();
    let whole = Uuid::new_v4();
    let partial = Uuid::new_v4();
    store_instance(&pool, &dep, whole, false).await;
    store_instance(&pool, &dep, partial, false).await;

    assert_eq!(
        store
            .acquire(&claim(&dep, whole, SLOT_INBOUND, "host-1-1-a"), &within(5))
            .await
            .unwrap(),
        SlotGrant::Acquired
    );
    assert_eq!(
        store.end_session(&dep, whole, None).await.unwrap(),
        vec![SLOT_INBOUND.to_string()]
    );
    assert_eq!(store.usage("lines").await.unwrap(), (0, 0));
    assert_eq!(
        store
            .acquire(
                &on_call(&dep, whole, "queued-request", SLOT_OUTBOUND, "host-1-1-a"),
                &within(5)
            )
            .await
            .unwrap(),
        SlotGrant::SessionEnded
    );
    assert!(store.session_ended(&dep, whole, "anything").await.unwrap());

    store
        .end_session(&dep, partial, Some("call-a"))
        .await
        .unwrap();
    assert_eq!(
        store
            .acquire(
                &on_call(&dep, partial, "call-a", SLOT_OUTBOUND, "host-1-1-a"),
                &within(5)
            )
            .await
            .unwrap(),
        SlotGrant::SessionEnded
    );
    assert_eq!(
        store
            .acquire(
                &on_call(&dep, partial, "call-b", SLOT_OUTBOUND, "host-1-1-a"),
                &within(5)
            )
            .await
            .unwrap(),
        SlotGrant::Acquired,
        "another call of the instance still draws from the pool"
    );
    assert_eq!(store.usage("lines").await.unwrap(), (0, 1));

    // Once the instance is over, the sweep forgets its session end.
    sqlx::query("UPDATE instance_state SET terminal_at = now() WHERE instance_id = $1")
        .bind(whole)
        .execute(&pool)
        .await
        .unwrap();
    store
        .sweep(&dep, "host-9", "host-9-1-z", Duration::ZERO)
        .await
        .unwrap();
    assert!(!store.session_ended(&dep, whole, "anything").await.unwrap());
    assert!(store.session_ended(&dep, partial, "call-a").await.unwrap());
}
