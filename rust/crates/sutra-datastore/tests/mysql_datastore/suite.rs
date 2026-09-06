//! The dialect-portability semantics proof for the MySQL family — the same CRUD / revision
//! / CAS / transaction-atomicity / `store_name`-partition / pessimistic-lock behaviours
//! the PostgreSQL reference suite (`tests/pg_datastore.rs`) pins, run unchanged against the
//! sqlx MySQL driver. One store SQL shape serves every dialect — the same `data_store` table,
//! the same rev-bumping-`UPDATE` row lock and portable upsert, differing only in placeholder
//! syntax and the advisory-lock primitive; this suite is what pins that for MySQL/MariaDB.
//! The MariaDB binary re-runs these identical sources against a mariadb:11 container.

use serde_json::json;

use crate::fixture;

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn cutover_model_crud_revision_cas_and_transactions() {
    let db = fixture::fresh_db().await;
    let store = fixture::store(&db, "ledger");

    // absent key: get None, revision 0, delete no-op
    assert_eq!(store.get("k1").await.unwrap(), None);
    assert_eq!(store.revision("k1").await.unwrap(), 0);
    store.delete("k1").await.unwrap();

    // insert-or-replace + revision bumps (insert = 1, each update +1)
    let precise = json!({"balance": 100.25_f64, "frozen": false});
    store.put("k1", &precise).await.unwrap();
    assert_eq!(store.revision("k1").await.unwrap(), 1);
    assert_eq!(store.get("k1").await.unwrap().unwrap(), precise);
    store.put("k1", &json!({"balance": 50})).await.unwrap();
    assert_eq!(store.revision("k1").await.unwrap(), 2);

    // arbitrary-precision round-trip: a value beyond f64 precision stays exact as text
    let exact: serde_json::Value =
        serde_json::from_str(r#"{"balance": 0.12345678901234567890123}"#).unwrap();
    store.put("exact", &exact).await.unwrap();
    assert_eq!(store.get("exact").await.unwrap().unwrap(), exact);

    // CAS: expect-absent (rev <= 0) inserts once, conflicts when the key exists
    assert!(store.put_if_revision("cas", &json!(1), 0).await.unwrap());
    assert!(
        !store.put_if_revision("cas", &json!(2), 0).await.unwrap(),
        "expect-absent conflict"
    );
    // expect-specific-rev: correct rev wins, stale rev is a detected conflict
    assert!(store.put_if_revision("cas", &json!(2), 1).await.unwrap());
    assert!(
        !store.put_if_revision("cas", &json!(3), 1).await.unwrap(),
        "stale rev conflict"
    );
    assert_eq!(store.get("cas").await.unwrap().unwrap(), json!(2));

    // store_name partitioning: same key, different store, same connection → independent
    let other = fixture::store(&db, "other");
    assert_eq!(other.get("k1").await.unwrap(), None);
    other.put("k1", &json!("theirs")).await.unwrap();
    assert_eq!(
        store.get("k1").await.unwrap().unwrap(),
        json!({"balance": 50})
    );

    // transaction atomicity: rollback discards, drop-without-commit rolls back, commit publishes
    let mut tx = store.begin().await.unwrap();
    tx.put("t1", &json!("a")).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(store.get("t1").await.unwrap(), None);

    {
        let mut tx = store.begin().await.unwrap();
        tx.put("t1", &json!("b")).await.unwrap();
        // dropped here — implicit rollback (drop is a rollback)
    }
    assert_eq!(store.get("t1").await.unwrap(), None);

    let mut tx = store.begin().await.unwrap();
    tx.put("t1", &json!("c")).await.unwrap();
    tx.delete("k1").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(store.get("t1").await.unwrap().unwrap(), json!("c"));
    assert_eq!(store.get("k1").await.unwrap(), None);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn pessimistic_lock_serialises_concurrent_read_modify_write() {
    let db = fixture::fresh_db().await;
    let store = fixture::store(&db, "accounts");
    store.put("acct", &json!({"balance": 0})).await.unwrap();

    // 4 workers × 5 increments, each a get_for_update + put in its own transaction. The
    // rev-bumping lock UPDATE serialises them; a lost update would end below 20. On
    // MySQL/MariaDB this only holds under READ COMMITTED (set on every pooled connection).
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                let mut tx = store.begin().await.unwrap();
                let current = tx.get_for_update("acct").await.unwrap().unwrap();
                let balance = current["balance"].as_i64().unwrap();
                tx.put("acct", &json!({"balance": balance + 1}))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(
        store.get("acct").await.unwrap().unwrap(),
        json!({"balance": 20}),
        "pessimistic locking must not lose an update"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cas_detects_conflicts_and_retries_converge() {
    let db = fixture::fresh_db().await;
    let store = fixture::store(&db, "counters");
    store.put("n", &json!(0)).await.unwrap();

    // 4 optimistic workers × 5 increments with a CAS-retry loop (the executor's
    // expect="unchanged" + DSO-retry shape): every conflict is detected, never silent.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                loop {
                    let rev = store.revision("n").await.unwrap();
                    let current = store.get("n").await.unwrap().unwrap().as_i64().unwrap();
                    if store
                        .put_if_revision("n", &json!(current + 1), rev)
                        .await
                        .unwrap()
                    {
                        break; // applied
                    }
                    // conflict — a concurrent committed write bumped rev; re-read and retry
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(store.get("n").await.unwrap().unwrap(), json!(20));
}

// ---- the projected (typed-column) shape ------------------------------------

/// The same round-trip / upsert / CAS / lock / delete semantics as the KV suite above, run
/// against the AUTHOR's own typed-column table (design §4.5): one column per declared scalar,
/// no `store_value` blob, no `store_name` column. The MariaDB binary re-runs this unchanged.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn projected_store_round_trips_upserts_cas_locks_and_deletes() {
    let db = fixture::fresh_db().await;
    let store = fixture::projected_store(&db, "accounts", "accounts", fixture::PROJECTED_DDL);

    // absent key: get None, revision 0, delete no-op — identical to the KV path
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    store.delete("a1").await.unwrap();

    // ROUND-TRIP FIDELITY: put/get is byte-equal, including the decimal's written scale and
    // the absent optional field (which stays absent, not an explicit null).
    let written = fixture::account_record();
    store.put("a1", &written).await.unwrap();
    let read = store.get("a1").await.unwrap().expect("row");
    assert_eq!(read, written);
    assert_eq!(read.to_string(), written.to_string(), "byte-equal");
    assert_eq!(read["balance"].to_string(), "100.50", "the written scale");
    assert!(read.get("openedAt").is_none(), "absent stays absent");
    assert_eq!(store.revision("a1").await.unwrap(), 1);

    // every declared class, including the optional one, present
    let full: serde_json::Value = serde_json::from_str(
        r#"{"accountId":"ACC-000999","balance":0.05,"seqNo":-3,"frozen":true,
            "openedAt":"2026-08-04"}"#,
    )
    .unwrap();
    store.put("a2", &full).await.unwrap();
    assert_eq!(
        store.get("a2").await.unwrap().unwrap().to_string(),
        full.to_string(),
        "byte-equal"
    );

    // UPSERT overwrite: the same key replaces, bumping rev (no second row)
    let changed: serde_json::Value = serde_json::from_str(
        r#"{"accountId":"ACC-000123","balance":250.00,"seqNo":8,"frozen":true}"#,
    )
    .unwrap();
    store.put("a1", &changed).await.unwrap();
    assert_eq!(store.revision("a1").await.unwrap(), 2);
    assert_eq!(
        store.get("a1").await.unwrap().unwrap().to_string(),
        changed.to_string()
    );

    // UNDECLARED FIELD: fails closed, names the field, and leaves the row untouched.
    let mut extra = changed.as_object().unwrap().clone();
    extra.insert("nickname".into(), json!("rainy day"));
    let err = store
        .put("a1", &serde_json::Value::Object(extra))
        .await
        .expect_err("an undeclared field must never be silently dropped");
    let message = err.to_string();
    assert!(
        message.contains("SUTRA.RUNTIME.DATASTORE.UNDECLARED_FIELD"),
        "{message}"
    );
    assert!(message.contains("nickname"), "{message}");
    assert_eq!(store.revision("a1").await.unwrap(), 2, "no write happened");

    // a non-record value has nowhere to go in a projected row
    assert!(store.put("a1", &json!("scalar")).await.is_err());

    // CAS: stale rev conflicts, current rev applies, expect-absent inserts exactly once
    assert!(!store.put_if_revision("a1", &written, 1).await.unwrap());
    assert!(store.put_if_revision("a1", &written, 2).await.unwrap());
    assert_eq!(store.revision("a1").await.unwrap(), 3);
    assert!(store.put_if_revision("a3", &written, 0).await.unwrap());
    assert!(
        !store.put_if_revision("a3", &changed, 0).await.unwrap(),
        "expect-absent conflict"
    );

    // get_for_update inside a transaction: the read-modify-write, then rollback discards
    let mut tx = store.begin().await.unwrap();
    let current = tx.get_for_update("a1").await.unwrap().unwrap();
    assert_eq!(current["accountId"], json!("ACC-000123"));
    let mut next = current.as_object().unwrap().clone();
    next.insert("seqNo".into(), json!(99));
    tx.put("a1", &serde_json::Value::Object(next))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(store.get("a1").await.unwrap().unwrap()["seqNo"], json!(99));

    let mut tx = store.begin().await.unwrap();
    tx.put("a4", &written).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(store.get("a4").await.unwrap(), None);

    // delete
    store.delete("a1").await.unwrap();
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    assert!(store.get("a2").await.unwrap().is_some(), "delete is scoped");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn first_use_verification_refuses_a_drifted_table() {
    let db = fixture::fresh_db().await;
    // The table exists but has drifted: `balance` dropped, the optional `opened_at` made
    // mandatory. A silent partial write is far worse than a loud refusal (§4.5).
    let store = fixture::projected_store(&db, "drifted", "drifted", fixture::DRIFTED_DDL);
    let err = store
        .get("a1")
        .await
        .expect_err("a drifted table must fail closed on first use");
    let message = err.to_string();
    assert!(
        message.contains("SUTRA.RUNTIME.DATASTORE.PROJECTION_UNSATISFIABLE"),
        "{message}"
    );
    assert!(message.contains("'balance'"), "{message}");
    assert!(message.contains("'opened_at' is NOT NULL"), "{message}");
    assert!(store.put("a1", &fixture::account_record()).await.is_err());

    // And a projected store whose table is simply not there refuses just as loudly.
    let absent = fixture::projected_store(&db, "missing", "missing", "SELECT 1");
    let err = absent.get("a1").await.expect_err("no table");
    assert!(err.to_string().contains("no columns visible"), "{err}");
}

/// A constraint failure on the author's own table is a FAULT, not a lost race.
///
/// The regression this pins: the duplicate-key predicate used to be the whole SQLSTATE class
/// `23`, and on this driver that class is a single bucket — duplicate key, `NOT NULL`, foreign
/// key and `CHECK` all report `23000`. So a `NOT NULL` violation was swept into the "someone
/// else inserted first" retry path, where the retried UPDATE matches no row; with no
/// aborted-transaction state to trip over, the discarded row count then made it `Ok(())` and the
/// caller was told a write happened that touched nothing. Silent data loss on the one dialect
/// pair where the shortcut looked safest.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn a_constraint_failure_is_reported_not_retried_as_a_duplicate() {
    let db = fixture::fresh_db().await;
    let store = fixture::projected_store(&db, "accounts", "accounts", fixture::PROJECTED_DDL);

    // Writing a partial record to an absent key binds every unmentioned declared column NULL,
    // which the author's `NOT NULL` refuses — the case a `<q:store field="…">` create produces.
    let partial = serde_json::json!({"accountId": "ACC-000123"});
    let err = store
        .put("a1", &partial)
        .await
        .expect_err("a NOT NULL violation is a fault, not a lost race");
    let message = err.to_string();
    assert!(
        message.contains("put failed for 'accounts'[a1]"),
        "the failure must name the store and key: {message}"
    );
    assert!(
        !message.contains("deleted concurrently"),
        "it must not be reported as the vanished-row race: {message}"
    );

    // Nothing was written, and the store still works afterwards.
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    store.put("a1", &fixture::account_record()).await.unwrap();
    assert_eq!(store.revision("a1").await.unwrap(), 1);

    // Same discipline on the CAS create path: `expect="unchanged"` against an absent key does
    // not get to report "conflict" for a write that could never have applied.
    let err = store
        .put_if_revision("a2", &partial, 0)
        .await
        .expect_err("a constraint failure is not a CAS conflict");
    assert!(err.to_string().contains("compare-and-set failed"), "{err}");
    assert_eq!(store.get("a2").await.unwrap(), None);

    // A genuine duplicate still takes the retry path, and it must LAND: this driver does not
    // poison the transaction on a failed statement, so the losing writer's retried UPDATE runs
    // and both writers succeed. (PostgreSQL differs — there the loser's transaction is already
    // aborted and it errors instead. Neither dialect may report a silent no-op.)
    let a = fixture::projected_store(&db, "accounts", "accounts", fixture::PROJECTED_DDL);
    let b = fixture::projected_store(&db, "accounts", "accounts", fixture::PROJECTED_DDL);
    let full = fixture::account_record();
    let (ra, rb) = tokio::join!(a.put("race", &full), b.put("race", &full));
    ra.expect("writer a");
    rb.expect("writer b");
    let row = store.get("race").await.unwrap().expect("the row exists");
    assert_eq!(row.to_string(), full.to_string());
}

/// The coverage contract on this dialect: the ENGINE's DDL
/// applied on first use to a connection the AUTHOR chose, an idempotent seed, durable
/// first-covers-wins carried by the write itself, deployment isolation by the bound predicate,
/// and — the property the portable aggregate had to preserve — counts that agree with a
/// client-side recount over the very same rows.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn coverage_store_marks_and_aggregates_on_the_declared_connection() {
    let db = fixture::fresh_db().await;
    let store = fixture::coverage_store(&db);
    let dep = "dep-coverage-1";
    let other = "dep-coverage-2";
    let urns: Vec<String> = ["a", "b", "c", "d"]
        .iter()
        .map(|p| format!("urn:sutra:coverage:file:{p}"))
        .collect();

    // First use applies the engine-shipped DDL (nothing in the package creates these tables) and
    // seeds every declared path; the seed is idempotent across redeploys / replica boots.
    assert_eq!(store.seed_declared(dep, &urns).await.unwrap(), 4);
    assert_eq!(store.seed_declared(dep, &urns).await.unwrap(), 0);

    // First-covers-wins, from the write's own row count — never a read-then-write.
    assert!(store.mark_path_covered(dep, &urns[0]).await.unwrap());
    assert!(!store.mark_path_covered(dep, &urns[0]).await.unwrap());
    // A path that was never seeded is inserted already-covered — once.
    let unseeded = "urn:sutra:coverage:file:z".to_string();
    assert!(store.mark_path_covered(dep, &unseeded).await.unwrap());
    assert!(!store.mark_path_covered(dep, &unseeded).await.unwrap());
    assert!(store.mark_path_covered(dep, &urns[2]).await.unwrap());

    // A second deployment on the SAME store is invisible to the first: deployment_id is the
    // column AND the predicate (no row-security on a user-owned connection).
    store.seed_declared(other, &urns).await.unwrap();
    store.mark_path_covered(other, &urns[1]).await.unwrap();

    // PARITY: the SQL aggregate against a client-side recount of the same rows.
    let mut all = urns.clone();
    all.push(unseeded.clone());
    let metrics = store.read_metrics(dep).await.unwrap();
    let covered_set = store.covered_among(dep, &all).await.unwrap();
    let recounted_uncovered: Vec<String> = all
        .iter()
        .filter(|u| !covered_set.contains(*u))
        .cloned()
        .collect();
    assert_eq!(metrics.total as usize, all.len());
    assert_eq!(metrics.covered as usize, covered_set.len());
    assert_eq!(metrics.uncovered, recounted_uncovered);
    assert_eq!(metrics.coverage_percentage(), 60.0);
    assert_eq!(store.count_metrics(dep).await.unwrap(), metrics.counts());
    assert_eq!(store.read_metrics(other).await.unwrap().covered, 1);

    // Scoped clear: only the named, actually-covered paths, and the count IS the answer.
    assert_eq!(store.clear_paths(dep, &all).await.unwrap(), 3);
    assert_eq!(store.clear_paths(dep, &all).await.unwrap(), 0);
    assert_eq!(store.read_metrics(dep).await.unwrap().covered, 0);

    // Reconstruction fragments round-trip in insertion order; reset drops them.
    for (i, process) in ["hop-a", "hop-b"].iter().enumerate() {
        store
            .write_fragment(
                dep,
                &sutra_datastore::CoverageFragmentRow {
                    route_urn: urns[0].clone(),
                    segment_process: (*process).to_string(),
                    instance_id: format!("inst-{i}"),
                    business_key: Some(format!("bk-{i}")),
                    trace_id: None,
                },
            )
            .await
            .unwrap();
    }
    let fragments = store.read_fragments(dep).await.unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(fragments[0].segment_process, "hop-a");
    assert_eq!(fragments[1].business_key.as_deref(), Some("bk-1"));
    assert_eq!(fragments[0].trace_id, None);

    store.mark_path_covered(dep, &urns[3]).await.unwrap();
    store.reset(dep).await.unwrap();
    assert_eq!(store.read_metrics(dep).await.unwrap().covered, 0);
    assert!(store.read_fragments(dep).await.unwrap().is_empty());
    assert_eq!(
        store.read_metrics(other).await.unwrap().covered,
        1,
        "reset is deployment-scoped"
    );

    // A second store instance over the same connection re-runs the DDL as a no-op.
    let again = fixture::coverage_store(&db);
    assert_eq!(again.read_metrics(dep).await.unwrap().total, 5);
}
