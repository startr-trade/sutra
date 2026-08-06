//! Dead-letter store (`dead_letter`, V1201 + V1202) — durable incident persistence, deployment
//! isolation, and the READ/REPLAY surface. Each non-idempotent inbound failure appends a row;
//! rows are bound + RLS-scoped to their deployment; V1202's capture columns make one redrivable.

use std::collections::BTreeMap;

use sutra_persistence::stores::{DeadLetterRow, PgDeadLetterStore, DEAD_LETTER_PAGE_MAX};
use sutra_persistence::DeploymentId;

use crate::fixture::{dep_a, dep_b, fresh_pool, now_micros};

fn row(dep: &DeploymentId, channel: &str) -> DeadLetterRow {
    DeadLetterRow {
        deployment: dep.clone(),
        channel: channel.to_string(),
        process_id: "orders".to_string(),
        dedup_key: String::new(),
        failure_code: "SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE".to_string(),
        detail: "handler panicked mid-flight".to_string(),
        received_at: now_micros(),
        payload: None,
        headers: BTreeMap::new(),
        content_type: None,
        tenant: String::new(),
        module_key: String::new(),
    }
}

/// The same row WITH the V1202 replay capture — what the dispatcher writes for a real inbound.
fn captured_row(dep: &DeploymentId, channel: &str, body: &[u8]) -> DeadLetterRow {
    let mut headers = BTreeMap::new();
    headers.insert("x-corr".to_string(), "corr-9".to_string());
    DeadLetterRow {
        dedup_key: "evt-1".to_string(),
        payload: Some(body.to_vec()),
        headers,
        content_type: Some("application/json".to_string()),
        tenant: "acme".to_string(),
        module_key: "acme/orders/1.0.0".to_string(),
        ..row(dep, channel)
    }
}

async fn count(pool: &sqlx::PgPool, dep: &DeploymentId) -> i64 {
    // Owner-role read (RLS bypass, like the maintenance path) asserting raw per-deployment counts.
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dead_letter WHERE deployment_id = $1")
        .bind(dep.as_str())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[ignore = "docker"]
#[tokio::test]
async fn each_failure_appends_a_row() {
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());

    // Unlike the audit trail, a dead-letter is a raw append (no idempotency key): two failures of
    // the same channel record two rows.
    store.insert(&row(&dep_a(), "orders-in")).await.unwrap();
    store.insert(&row(&dep_a(), "orders-in")).await.unwrap();

    assert_eq!(count(&pool, &dep_a()).await, 2);
}

#[ignore = "docker"]
#[tokio::test]
async fn rows_are_deployment_isolated() {
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());

    store.insert(&row(&dep_a(), "a-in")).await.unwrap();
    store.insert(&row(&dep_b(), "b-in")).await.unwrap();
    store.insert(&row(&dep_b(), "b-in")).await.unwrap();

    assert_eq!(count(&pool, &dep_a()).await, 1);
    assert_eq!(count(&pool, &dep_b()).await, 2);
}

// ---- V1202: the replay capture -------------------------------------------------------------

#[ignore = "docker"]
#[tokio::test]
async fn the_capture_columns_round_trip_and_the_read_projection_never_carries_the_bytes() {
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());
    let body = br#"{"orderId":"A-1"}"#;
    store
        .insert(&captured_row(&dep_a(), "orders-in", body))
        .await
        .unwrap();

    let listed = store.list(&dep_a(), 10, 0).await.unwrap();
    assert_eq!(listed.len(), 1);
    let record = &listed[0];
    // The metadata projection reports the payload's SIZE — never its bytes (there is no field
    // that could carry them, by construction).
    assert_eq!(record.payload_bytes, Some(body.len() as i32));
    assert_eq!(record.content_type.as_deref(), Some("application/json"));
    assert_eq!(record.tenant, "acme");
    assert_eq!(record.module_key, "acme/orders/1.0.0");
    assert_eq!(record.channel, "orders-in");
    assert_eq!(record.dedup_key, "evt-1");

    // get(id) answers the same projection.
    let fetched = store.get(&dep_a(), record.id).await.unwrap().unwrap();
    assert_eq!(&fetched, record);

    // The replay read is the ONE path that lifts the bytes out.
    let replay = store
        .replay_payload(&dep_a(), record.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.payload.as_deref(), Some(&body[..]));
    assert_eq!(
        replay.headers.get("x-corr").map(String::as_str),
        Some("corr-9")
    );
    assert_eq!(replay.channel, "orders-in");
    assert_eq!(replay.tenant, "acme");
    assert_eq!(replay.module_key, "acme/orders/1.0.0");
    assert_eq!(replay.content_type.as_deref(), Some("application/json"));
}

#[ignore = "docker"]
#[tokio::test]
async fn a_row_without_capture_reads_back_as_not_replayable_rather_than_empty() {
    // Rows written before V1202 — and outbound required-delivery incidents, which have no inbound
    // message — carry NULLs. The read surface must say "nothing captured", never invent a body.
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());
    store.insert(&row(&dep_a(), "orders-in")).await.unwrap();

    let record = store.list(&dep_a(), 10, 0).await.unwrap().pop().unwrap();
    assert_eq!(record.payload_bytes, None);
    assert!(record.tenant.is_empty() && record.module_key.is_empty());

    let replay = store
        .replay_payload(&dep_a(), record.id)
        .await
        .unwrap()
        .unwrap();
    assert!(replay.payload.is_none());
    assert!(replay.headers.is_empty());
}

#[ignore = "docker"]
#[tokio::test]
async fn listing_is_newest_first_bounded_and_pageable() {
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());
    for i in 0..5 {
        store
            .insert(&captured_row(
                &dep_a(),
                &format!("ch-{i}"),
                format!("body-{i}").as_bytes(),
            ))
            .await
            .unwrap();
    }

    let page = store.list(&dep_a(), 2, 0).await.unwrap();
    assert_eq!(page.len(), 2, "limit is honoured");
    assert!(
        page[0].id > page[1].id,
        "newest first (recorded_at DESC, id DESC as the stable tiebreak)"
    );
    let next = store.list(&dep_a(), 2, 2).await.unwrap();
    assert!(
        next.iter().all(|r| r.id < page[1].id),
        "offset pages strictly older rows"
    );

    // A caller asking for the moon gets the ceiling, not an unbounded scan.
    let clamped = store.list(&dep_a(), i64::MAX, -5).await.unwrap();
    assert!(clamped.len() <= DEAD_LETTER_PAGE_MAX as usize);
    assert_eq!(clamped.len(), 5, "negative offset reads as 0");
}

#[ignore = "docker"]
#[tokio::test]
async fn reads_are_deployment_scoped_so_one_tenant_cannot_fetch_anothers_payload() {
    // The isolation that matters most on THIS table: the payload is raw business data.
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());
    store
        .insert(&captured_row(&dep_a(), "a-in", b"secret-a"))
        .await
        .unwrap();

    let a_row = store.list(&dep_a(), 10, 0).await.unwrap().pop().unwrap();

    assert!(
        store.list(&dep_b(), 10, 0).await.unwrap().is_empty(),
        "another deployment sees nothing"
    );
    assert!(
        store.get(&dep_b(), a_row.id).await.unwrap().is_none(),
        "a forged id from another deployment resolves to nothing"
    );
    assert!(
        store
            .replay_payload(&dep_b(), a_row.id)
            .await
            .unwrap()
            .is_none(),
        "and it certainly cannot fetch the payload"
    );
}

#[ignore = "docker"]
#[tokio::test]
async fn an_unknown_id_is_none_not_an_error() {
    let pool = fresh_pool().await;
    let store = PgDeadLetterStore::new(pool.clone());
    assert!(store.get(&dep_a(), 424_242).await.unwrap().is_none());
    assert!(store
        .replay_payload(&dep_a(), 424_242)
        .await
        .unwrap()
        .is_none());
}
