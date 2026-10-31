//! The dialect-portability semantics proof for SQL Server — the same CRUD / revision /
//! CAS / transaction-atomicity / `store_name`-partition / pessimistic-lock behaviours
//! the PostgreSQL reference suite (`tests/pg_datastore.rs`) pins, run against the tiberius
//! TDS driver. One store SQL shape serves every dialect — the same `data_store` table, the
//! same rev-bumping-`UPDATE` row lock and portable upsert, differing only in placeholder
//! syntax and the advisory-lock primitive; this suite is what pins that for SQL Server.

use std::str::FromStr;

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
        // dropped here — connection discarded, server rolls the open transaction back
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
    // rev-bumping lock UPDATE takes a row-exclusive lock held to commit, serialising the
    // workers; a lost update would end below 20.
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
/// no `store_value` blob, no `store_name` column — here on the tiberius TDS stack, whose
/// `@Pn` placeholders and `CONVERT` renderings are the dialect's only difference.
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

// ---- key queries (`<q:store query=…>`, 0.6.10) ------------------------------

fn agg(sum: Option<&str>, group: Option<&str>) -> sutra_datastore::KeyQueryAggregate {
    sutra_datastore::KeyQueryAggregate {
        sum_field: sum.map(|s| vec![s.to_string()]),
        group_by: group.map(|g| vec![g.to_string()]),
    }
}

fn prefix_query(prefix: &str, row_limit: Option<u32>) -> sutra_datastore::KeyRangeQuery {
    sutra_datastore::KeyRangeQuery {
        lower: prefix.to_string(),
        upper: sutra_datastore::keyquery::prefix_upper_bound(prefix),
        after: None,
        row_limit,
        aggregates: vec![agg(Some("amount"), Some("dir")), agg(Some("amount"), None)],
    }
}

/// The same key-query contract the PostgreSQL suite pins: prefix and range selection in bytewise
/// key order, paging with `after`, grouped and ungrouped exact sums over every matching row,
/// the numeric rules, zeros on no match — and the refusal of a non-bytewise key column.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn key_query_prefix_range_paging_and_aggregates() {
    let db = fixture::fresh_db().await;
    let store = fixture::store_with_ddl(&db, "events", fixture::QUERY_DDL);
    let mut expected_total = 0i64;
    for s in 0..120i64 {
        let cents = (s * 7919) % 10_000 + 1;
        let side = if s % 3 == 0 { "OUT" } else { "IN" };
        // Half the amounts as JSON numbers, half as decimal strings.
        let amount = if s % 2 == 0 {
            serde_json::Value::from_str(&format!("{}.{:02}", cents / 100, cents % 100)).unwrap()
        } else {
            json!(format!("{}.{:02}", cents / 100, cents % 100))
        };
        expected_total += cents;
        store
            .put(
                &format!("src-1|2026-10-07|{s:06}"),
                &json!({"amount": amount, "dir": side}),
            )
            .await
            .unwrap();
    }
    for key in [
        "src-1|2026-10-06|000001",
        "src-1|2026-10-07}",
        "src-2|2026-10-07|000001",
    ] {
        store
            .put(key, &json!({"amount": 1000, "dir": "IN"}))
            .await
            .unwrap();
    }
    for (key, value) in [
        ("edge|1", json!({"amount": 1.25, "dir": "A"})),
        ("edge|2", json!({"amount": "2.50", "dir": "A"})),
        ("edge|3", json!({"dir": "B"})),
        ("edge|4", json!({"amount": null})),
        ("edge|5", json!({"amount": "abc", "dir": "B"})),
        ("edge|6", json!({"amount": true, "dir": "B"})),
    ] {
        store.put(key, &value).await.unwrap();
    }

    // Paging over one day's prefix: every row once, ascending; aggregates over all 120.
    let mut query = prefix_query("src-1|2026-10-07|", Some(50));
    let mut seen = Vec::new();
    loop {
        let page = store.query(&query).await.unwrap();
        let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[1] else {
            panic!("total")
        };
        assert_eq!(total.count, 120);
        let sum = total.sum.as_deref().unwrap();
        let (whole, frac) = sum.split_once('.').unwrap_or((sum, "0"));
        let cents = whole.parse::<i64>().unwrap() * 100
            + format!("{frac:0<2}")[..2].parse::<i64>().unwrap();
        assert_eq!(cents, expected_total, "exact sum: {sum}");
        assert!(
            format!("{frac:0<2}")[2..].chars().all(|c| c == '0'),
            "no float residue: {sum}"
        );
        seen.extend(page.rows.iter().map(|(k, _)| k.clone()));
        match page.next {
            Some(next) => {
                assert!(page.truncated);
                query.after = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(seen.len(), 120);
    assert!(seen.windows(2).all(|w| w[0] < w[1]));
    assert!(seen.iter().all(|k| k.starts_with("src-1|2026-10-07|")));

    // A range: from inclusive, to exclusive.
    let range = sutra_datastore::KeyRangeQuery {
        lower: "src-1|2026-10-06|000001".into(),
        upper: Some("src-1|2026-10-07|000002".into()),
        after: None,
        row_limit: Some(10),
        aggregates: vec![],
    };
    let keys: Vec<String> = store
        .query(&range)
        .await
        .unwrap()
        .rows
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        keys,
        vec![
            "src-1|2026-10-06|000001",
            "src-1|2026-10-07|000000",
            "src-1|2026-10-07|000001"
        ]
    );

    // The numeric rules on this database.
    let page = store.query(&prefix_query("edge|", Some(10))).await.unwrap();
    let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[1] else {
        panic!("total")
    };
    assert_eq!(total.count, 6);
    let sum = total.sum.as_deref().unwrap();
    assert_eq!(
        sum.trim_end_matches('0').trim_end_matches('.'),
        "3.75",
        "{sum}"
    );
    assert_eq!(total.non_numeric, 2, "text and boolean");
    let sutra_datastore::KeyAggregateResult::Grouped(groups) = &page.aggregates[0] else {
        panic!("grouped")
    };
    assert_eq!(groups["B"].non_numeric, 2);
    assert_eq!(groups["null"].count, 1);

    // No match: zeros, not null.
    let page = store
        .query(&prefix_query("nobody|", Some(10)))
        .await
        .unwrap();
    assert!(page.rows.is_empty() && page.next.is_none());
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Grouped(Default::default())
    );
    let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[1] else {
        panic!("total")
    };
    assert_eq!((total.count, total.sum.as_deref()), (0, Some("0")));

    // The shipped cutover DDL keys in a case-insensitive collation: refused, naming the fix.
    let other = fixture::fresh_db().await;
    let linguistic = fixture::store(&other, "events");
    let e = linguistic
        .query(&prefix_query("src|", Some(10)))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_ORDER),
        "{e}"
    );
}

// ---- key queries on a typed (projected) store (0.6.13) -------------------------------------

/// The typed table of a store that runs key queries: `store_key` leads the primary key in a
/// bytewise collation.
const TYPED_QUERY_DDL: &str = "IF OBJECT_ID('ledger_entries', 'U') IS NULL \
  CREATE TABLE ledger_entries ( \
    store_key  NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL, \
    account_id VARCHAR(35)   NOT NULL, \
    balance    DECIMAL(18,2) NOT NULL, \
    seq_no     INT           NOT NULL, \
    frozen     BIT           NOT NULL, \
    opened_at  DATE          NULL, \
    rev        BIGINT        NOT NULL DEFAULT 1, \
    updated_at DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(), \
    CONSTRAINT pk_ledger_entries PRIMARY KEY NONCLUSTERED (store_key) \
  );";

fn typed_query(
    lower: &str,
    upper: Option<String>,
    row_limit: Option<u32>,
    aggregates: Vec<sutra_datastore::KeyQueryAggregate>,
) -> sutra_datastore::KeyRangeQuery {
    sutra_datastore::KeyRangeQuery {
        lower: lower.to_string(),
        upper,
        after: None,
        row_limit,
        aggregates,
    }
}

fn typed_prefix(
    prefix: &str,
    row_limit: Option<u32>,
    aggregates: Vec<sutra_datastore::KeyQueryAggregate>,
) -> sutra_datastore::KeyRangeQuery {
    typed_query(
        prefix,
        sutra_datastore::keyquery::prefix_upper_bound(prefix),
        row_limit,
        aggregates,
    )
}

fn as_cents(text: &str) -> i64 {
    let (whole, frac) = text.split_once('.').unwrap_or((text, "0"));
    assert!(
        format!("{frac:0<2}")[2..].chars().all(|c| c == '0'),
        "no residue: {text}"
    );
    whole.parse::<i64>().unwrap() * 100 + format!("{frac:0<2}")[..2].parse::<i64>().unwrap()
}

/// The typed-store key-query contract the PostgreSQL suite pins: prefix and range over the
/// store's own table in bytewise key order, rows as the typed records a get returns, paging with
/// `after`, count and exact sums on typed columns grouped by a typed column (NULL under "null"),
/// the non-numeric rule, zeros on no match, the path refusal — and the refusal of a key column
/// that does not sort bytewise.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn typed_store_key_query_prefix_range_paging_and_aggregates() {
    let db = fixture::fresh_db().await;
    let store = fixture::projected_store(&db, "ledger", "ledger_entries", TYPED_QUERY_DDL);
    let (mut frozen_c, mut thawed_c, mut frozen_n, mut seq_sum) = (0i64, 0i64, 0i64, 0i64);
    for s in 0..120i64 {
        let cents = (s * 7919) % 10_000 + 1;
        let frozen = s % 3 == 0;
        if frozen {
            frozen_c += cents;
            frozen_n += 1;
        } else {
            thawed_c += cents;
        }
        seq_sum += s % 5;
        let opened = if s % 4 == 0 {
            String::new()
        } else {
            format!(r#","openedAt":"2026-10-0{}""#, s % 7 + 1)
        };
        let record = format!(
            r#"{{"accountId":"ACC-{s}","balance":{}.{:02},"seqNo":{},"frozen":{frozen}{opened}}}"#,
            cents / 100,
            cents % 100,
            s % 5
        );
        store
            .put(
                &format!("acct-1|{s:06}"),
                &serde_json::Value::from_str(&record).unwrap(),
            )
            .await
            .unwrap();
    }
    let decoy = fixture::account_record();
    for key in ["acct-0|000001", "acct-1}", "acct-2|000001"] {
        store.put(key, &decoy).await.unwrap();
    }

    let aggregates = vec![
        agg(Some("balance"), Some("frozen")),
        agg(None, Some("openedAt")),
        agg(Some("balance"), None),
        agg(Some("seqNo"), None),
    ];
    let mut query = typed_prefix("acct-1|", Some(50), aggregates);
    let mut seen = Vec::new();
    loop {
        let page = store.query(&query).await.unwrap();
        for (key, value) in page.rows.iter().take(2) {
            let got = store.get(key).await.unwrap().expect("row exists");
            assert_eq!(value, &got, "a query row is the record a typed get returns");
        }
        let sutra_datastore::KeyAggregateResult::Grouped(by_frozen) = &page.aggregates[0] else {
            panic!("grouped")
        };
        assert_eq!(by_frozen.len(), 2, "{by_frozen:?}");
        assert_eq!(by_frozen["true"].count, frozen_n);
        assert_eq!(
            as_cents(by_frozen["true"].sum.as_deref().unwrap()),
            frozen_c
        );
        assert_eq!(
            as_cents(by_frozen["false"].sum.as_deref().unwrap()),
            thawed_c
        );
        let sutra_datastore::KeyAggregateResult::Grouped(by_day) = &page.aggregates[1] else {
            panic!("grouped")
        };
        assert_eq!(by_day.len(), 8, "{by_day:?}");
        assert_eq!(
            by_day["null"].count, 30,
            "an absent optional field groups under null"
        );
        assert!(by_day.contains_key("2026-10-02"), "{by_day:?}");
        let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[2] else {
            panic!("total")
        };
        assert_eq!(total.count, 120, "aggregates ignore the row limit");
        assert_eq!(as_cents(total.sum.as_deref().unwrap()), frozen_c + thawed_c);
        let sutra_datastore::KeyAggregateResult::Total(seq) = &page.aggregates[3] else {
            panic!("total")
        };
        assert_eq!(seq.sum.as_deref(), Some(seq_sum.to_string().as_str()));
        seen.extend(page.rows.iter().map(|(k, _)| k.clone()));
        match page.next {
            Some(next) => {
                assert!(page.truncated);
                query.after = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(seen.len(), 120);
    assert!(seen.windows(2).all(|w| w[0] < w[1]));
    assert!(seen.iter().all(|k| k.starts_with("acct-1|")));

    // A range: from inclusive, to exclusive.
    let range = typed_query(
        "acct-0|000001",
        Some("acct-1|000002".into()),
        Some(10),
        vec![agg(None, None)],
    );
    let page = store.query(&range).await.unwrap();
    let keys: Vec<&str> = page.rows.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        vec!["acct-0|000001", "acct-1|000000", "acct-1|000001"]
    );
    assert_eq!(page.rows[0].1, decoy);

    // A text column sums nothing here and counts every value as non-numeric.
    let page = store
        .query(&typed_prefix(
            "acct-1|",
            None,
            vec![agg(Some("accountId"), None)],
        ))
        .await
        .unwrap();
    let sutra_datastore::KeyAggregateResult::Total(text) = &page.aggregates[0] else {
        panic!("total")
    };
    assert_eq!(
        (text.count, text.sum.as_deref(), text.non_numeric),
        (120, Some("0"), 120)
    );

    // No match: zeros, not null.
    let page = store
        .query(&typed_prefix(
            "nobody|",
            Some(10),
            vec![
                agg(Some("balance"), Some("frozen")),
                agg(Some("balance"), None),
            ],
        ))
        .await
        .unwrap();
    assert!(page.rows.is_empty() && page.next.is_none() && !page.truncated);
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Grouped(Default::default())
    );
    let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[1] else {
        panic!("total")
    };
    assert_eq!((total.count, total.sum.as_deref()), (0, Some("0")));

    // A nested path or an undeclared field is refused.
    for bad in [
        sutra_datastore::KeyQueryAggregate {
            sum_field: Some(vec!["balance".into(), "value".into()]),
            group_by: None,
        },
        agg(None, Some("missing")),
    ] {
        let e = store
            .query(&typed_prefix("acct-1|", Some(1), vec![bad]))
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            Some(sutra_datastore::keyquery::codes::QUERY_UNSUPPORTED),
            "{e}"
        );
    }

    // The shipped projected DDL keys in a case-insensitive collation: refused, naming the table.
    let other = fixture::fresh_db().await;
    let linguistic = fixture::projected_store(&other, "ledger", "accounts", fixture::PROJECTED_DDL);
    let e = linguistic
        .query(&typed_prefix("acct-1|", Some(10), vec![]))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_ORDER),
        "{e}"
    );
    assert!(e.to_string().contains("ALTER TABLE accounts"), "{e}");
}

// ---- the migration ledger (run once, checksums, adoption, placeholders) ------

/// Deliberately NOT idempotent: a second run violates the primary key.
const LEDGER_SEED: &str = "INSERT INTO data_store (store_name, store_key, store_value) \
     VALUES ('${store.name}', 'seed-${package.tenant}', '{\"module\":\"${package.module}\"}');";
const LEDGER_V3: &str = "INSERT INTO data_store (store_name, store_key, store_value) \
     VALUES ('${store.name}', 'v3', '{\"version\":\"${package.version}\"}');";

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn migration_ledger_runs_each_script_once_and_refuses_an_edited_one() {
    let db = fixture::fresh_db().await;
    let v1v2 = [
        ("V1__table.sql", fixture::CUTOVER_DDL),
        ("V2__seed.sql", LEDGER_SEED),
    ];

    // (1) + (6): first boot applies and records; the seed carries the substituted labels.
    let first = fixture::ledger_store(&db, "ledger", fixture::ledger_scripts("ledger", &v1v2));
    assert_eq!(
        first.get("seed-acme").await.unwrap(),
        Some(json!({"module": "orders"}))
    );
    assert_eq!(fixture::ledger_count(&db, "ledger").await, 2);
    // A re-boot runs nothing — the seed would collide.
    let reboot = fixture::ledger_store(&db, "ledger", fixture::ledger_scripts("ledger", &v1v2));
    assert!(reboot.get("seed-acme").await.unwrap().is_some());
    assert_eq!(fixture::ledger_count(&db, "ledger").await, 2);

    // (2) adding V3 runs exactly V3, once.
    let v1v3 = [v1v2[0], v1v2[1], ("V3__more.sql", LEDGER_V3)];
    let upgraded = fixture::ledger_store(&db, "ledger", fixture::ledger_scripts("ledger", &v1v3));
    assert_eq!(
        upgraded.get("v3").await.unwrap(),
        Some(json!({"version": "1.0.0"}))
    );
    let again = fixture::ledger_store(&db, "ledger", fixture::ledger_scripts("ledger", &v1v3));
    assert!(again.get("v3").await.unwrap().is_some());
    assert_eq!(fixture::ledger_count(&db, "ledger").await, 3);

    // (3) an edited, already-applied script refuses the store.
    let edited = [v1v2[0], ("V2__seed.sql", "SELECT 1;")];
    let refused = fixture::ledger_store(&db, "ledger", fixture::ledger_scripts("ledger", &edited));
    let err = refused.get("seed-acme").await.unwrap_err().to_string();
    assert!(
        err.contains("SUTRA.RUNTIME.DATASTORE.MIGRATION_CHECKSUM") && err.contains("V2__seed.sql"),
        "{err}"
    );

    // (4) two replicas racing first use apply each script exactly once.
    let a = fixture::ledger_store(&db, "race", fixture::ledger_scripts("race", &v1v2));
    let b = fixture::ledger_store(&db, "race", fixture::ledger_scripts("race", &v1v2));
    let (ra, rb) = tokio::join!(a.get("seed-acme"), b.get("seed-acme"));
    assert!(ra.unwrap().is_some() && rb.unwrap().is_some());
    assert_eq!(fixture::ledger_count(&db, "race").await, 2);

    // (5) adoption: a table built before the ledger existed — the idempotent script runs once
    // more and is recorded; existing rows survive.
    let legacy = fixture::store(&db, "adopt");
    legacy.put("kept", &json!({"old": true})).await.unwrap();
    assert_eq!(fixture::ledger_count(&db, "adopt").await, 0);
    let adopted = fixture::ledger_store(
        &db,
        "adopt",
        fixture::ledger_scripts("adopt", &[("V1__table.sql", fixture::CUTOVER_DDL)]),
    );
    assert_eq!(
        adopted.get("kept").await.unwrap(),
        Some(json!({"old": true}))
    );
    assert_eq!(fixture::ledger_count(&db, "adopt").await, 1);

    // A failed script is rolled back and records nothing (SQL Server DDL is transactional), so
    // the fixed package re-creates the same table cleanly.
    let bad = [
        ("V1__table.sql", fixture::CUTOVER_DDL),
        (
            "V2__bad.sql",
            "CREATE TABLE half_done (x INT); INSERT INTO no_such_table VALUES (1);",
        ),
    ];
    let broken = fixture::ledger_store(&db, "broken", fixture::ledger_scripts("broken", &bad));
    assert!(broken.get("k").await.is_err());
    assert_eq!(fixture::ledger_count(&db, "broken").await, 1);
    let good = [bad[0], ("V2__bad.sql", "CREATE TABLE half_done (x INT);")];
    let fixed = fixture::ledger_store(&db, "broken", fixture::ledger_scripts("broken", &good));
    assert_eq!(fixed.get("k").await.unwrap(), None);
    assert_eq!(fixture::ledger_count(&db, "broken").await, 2);
}
