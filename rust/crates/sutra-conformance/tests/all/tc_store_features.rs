//! Conformance (0.6.10), end to end on two engine replicas sharing one PostgreSQL, with the
//! package in `fixtures/store-features/`:
//!
//! - a data-store key query through a BPMN data task: one page of rows, paging with `after`, and
//!   grouped/ungrouped exact sums over every matching row — through a store with the read cache on;
//! - the insert-if-absent claim (`expect="absent"`) raced by both replicas: exactly one claim per
//!   day wins, every loser takes the error-boundary path, nothing fails;
//! - two waits on one channel that differ only by `messageTypeValue`: each is resumed by its own
//!   message type and never by the other's.

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::support::engine::{self, EngineBuilder, EngineHandle, PgFixture};
use crate::support::util;

const API_KEY: &str = "store-features-conformance-key";

struct Topology {
    pg: PgFixture,
    replicas: Vec<EngineHandle>,
}

fn topology() -> &'static Topology {
    static TOPO: OnceLock<Topology> = OnceLock::new();
    TOPO.get_or_init(|| {
        std::thread::spawn(|| {
            let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/store-features/default--storefeat--1.0.0");
            let archives = util::world_readable_temp_dir("sutra-archives");
            sutra_loader::package::assemble_dir(
                &package,
                &archives,
                &sutra_loader::PackageOptions::default(),
            )
            .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
            util::make_world_readable(&archives);
            let pg = engine::start_postgres("storefeat");
            // Replica 0 first (it runs the engine migrations uncontended), then replica 1.
            let replicas = (0..2)
                .map(|i| {
                    EngineBuilder::new(&format!("storefeat-{i}"), &pg)
                        .env("STORE_FEATURES_API_KEY", API_KEY)
                        .env(
                            "STORE_DB_URL",
                            format!("postgresql://{}:5432/postgres", pg.container_name),
                        )
                        .env("STORE_DB_USER", "postgres")
                        .env("STORE_DB_PASSWORD", "postgres")
                        .expected_deployments(1)
                        .start(&archives)
                })
                .collect();
            Topology { pg, replicas }
        })
        .join()
        .expect("store-features topology")
    })
}

fn sql(query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec",
            topology().pg.container.id(),
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAc",
            query,
        ])
        .output()
        .expect("docker exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The stored value of `store`/`key`, parsed, once it exists.
async fn stored(store: &str, key: &str, within: Duration) -> Option<serde_json::Value> {
    let deadline = Instant::now() + within;
    loop {
        let text = sql(&format!(
            "SELECT store_value FROM data_store WHERE store_name = '{store}' AND store_key = '{key}'"
        ));
        if !text.is_empty() {
            return Some(serde_json::from_str(&text).expect("stored JSON"));
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// POST `body` to `channel` on `replica`; the status and the response text (for diagnostics).
async fn send(
    client: &reqwest::Client,
    replica: usize,
    channel: &str,
    content_type: &str,
    body: String,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!(
            "http://127.0.0.1:{}/channels/{channel}",
            topology().replicas[replica].http_port
        ))
        .header("Content-Type", content_type)
        .header("X-Api-Key", API_KEY)
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .unwrap_or_else(|e| panic!("post {channel}: {e}"));
    let status = response.status();
    (status, response.text().await.unwrap_or_default())
}

async fn post(
    client: &reqwest::Client,
    replica: usize,
    channel: &str,
    content_type: &str,
    body: String,
) -> reqwest::StatusCode {
    let (status, text) = send(client, replica, channel, content_type, body).await;
    if status.is_server_error() {
        eprintln!("{channel} answered {status}: {text}");
    }
    status
}

async fn post_json(
    client: &reqwest::Client,
    replica: usize,
    channel: &str,
    body: serde_json::Value,
) {
    let (status, text) = send(
        client,
        replica,
        channel,
        "application/json",
        body.to_string(),
    )
    .await;
    assert!(
        status.is_success(),
        "{channel} answered {status} for {body}: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn tc_store_features_key_query_pages_and_sums_through_a_data_task() {
    let client = reqwest::Client::new();
    // 25 rows for one account-day, a row of another day and another account around them.
    let mut in_cents = 0i64;
    let mut out_cents = 0i64;
    for i in 0..25i64 {
        let cents = (i * 397) % 10_000 + 1;
        let side = if i % 3 == 0 { "OUT" } else { "IN" };
        if side == "OUT" {
            out_cents += cents;
        } else {
            in_cents += cents;
        }
        let amount = format!("{}.{:02}", cents / 100, cents % 100);
        post_json(
            &client,
            0,
            "events-in",
            serde_json::json!({
                "key": format!("src-7|2026-10-30|{i:04}"),
                "entry": {"amount": amount, "dir": side, "tag": if i % 2 == 0 { "OPEN" } else { "CLOSE" }}
            }),
        )
        .await;
    }
    for key in ["src-7|2026-10-29|0001", "src-8|2026-10-30|0001"] {
        post_json(
            &client,
            0,
            "events-in",
            serde_json::json!({"key": key, "entry": {"amount": "1000", "dir": "IN", "tag": "OPEN"}}),
        )
        .await;
    }

    // Page through the day: 10 + 10 + 5, aggregates identical on every page.
    let mut after = serde_json::Value::Null;
    let mut keys: Vec<String> = Vec::new();
    for page in 0.. {
        let id = format!("day-{page}");
        post_json(
            &client,
            0,
            "report-in",
            serde_json::json!({"id": id, "prefix": "src-7|2026-10-30|", "after": after}),
        )
        .await;
        let report = stored("results", &format!("report|{id}"), Duration::from_secs(30))
            .await
            .unwrap_or_else(|| panic!("report {id} never stored"));
        let rows = report["rows"].as_array().expect("rows");
        keys.extend(rows.iter().map(|r| r["key"].as_str().unwrap().to_string()));
        let aggregates = report["aggregates"].as_array().expect("aggregates");
        let side = &aggregates[0]["groups"];
        assert_eq!(side["IN"]["count"], 16, "{report}");
        assert_eq!(side["OUT"]["count"], 9, "{report}");
        let cents = |v: &serde_json::Value| -> i64 {
            let text = v.to_string();
            let (whole, frac) = text.split_once('.').unwrap_or((&text, "0"));
            whole.parse::<i64>().unwrap() * 100 + format!("{frac:0<2}")[..2].parse::<i64>().unwrap()
        };
        assert_eq!(cents(&side["IN"]["sum"]), in_cents, "{report}");
        assert_eq!(cents(&side["OUT"]["sum"]), out_cents, "{report}");
        assert_eq!(aggregates[1]["groups"]["OPEN"]["count"], 13);
        assert!(aggregates[1]["groups"]["OPEN"].get("sum").is_none());
        assert_eq!(aggregates[2]["count"], 25);
        assert_eq!(cents(&aggregates[2]["sum"]), in_cents + out_cents);
        if report["truncated"] == serde_json::Value::Bool(true) {
            assert_eq!(rows.len(), 10);
            after = report["next"].clone();
        } else {
            assert_eq!(report["next"], serde_json::Value::Null);
            assert_eq!(page, 2, "25 rows in pages of 10");
            break;
        }
    }
    assert_eq!(keys.len(), 25);
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "ascending, each once");
    assert!(keys.iter().all(|k| k.starts_with("src-7|2026-10-30|")));

    // A write after a report is seen by the next report: the query bypasses the read cache.
    post_json(
        &client,
        0,
        "events-in",
        serde_json::json!({"key": "src-7|2026-10-30|9999", "entry": {"amount": "0.01", "dir": "IN", "tag": "OPEN"}}),
    )
    .await;
    post_json(
        &client,
        0,
        "report-in",
        serde_json::json!({"id": "after-write", "prefix": "src-7|2026-10-30|", "after": null}),
    )
    .await;
    let report = stored("results", "report|after-write", Duration::from_secs(30))
        .await
        .expect("report stored");
    assert_eq!(report["aggregates"][2]["count"], 26, "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn tc_store_features_a_claim_raced_by_two_replicas_has_exactly_one_winner() {
    let client = reqwest::Client::new();
    const DAYS: usize = 15;
    const CONTENDERS: usize = 4;
    for day in 0..DAYS {
        let day = format!("race-{day:02}");
        // Four contenders per day, alternating replicas, all in flight at once.
        let posts: Vec<_> = (0..CONTENDERS)
            .map(|c| {
                let client = client.clone();
                let day = day.clone();
                tokio::spawn(async move {
                    post_json(
                        &client,
                        c % 2,
                        "claim-in",
                        serde_json::json!({"day": day, "who": format!("c{c}")}),
                    )
                    .await
                })
            })
            .collect();
        for post in posts {
            post.await.expect("claim post");
        }
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let recorded: usize = sql(
            "SELECT count(*) FROM data_store WHERE store_name = 'results' AND store_key LIKE 'claim|race-%'",
        )
        .parse()
        .unwrap_or(0);
        if recorded == DAYS * CONTENDERS {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {recorded} of {} claim outcomes recorded",
            DAYS * CONTENDERS
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let claims: usize = sql(
        "SELECT count(*) FROM data_store WHERE store_name = 'claims' AND store_key LIKE 'race-%'",
    )
    .parse()
    .unwrap();
    assert_eq!(claims, DAYS, "one claim row per day");
    let rows = sql("SELECT store_key || '=' || store_value FROM data_store \
         WHERE store_name = 'results' AND store_key LIKE 'claim|race-%' ORDER BY store_key");
    let mut won: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in rows.lines() {
        let (key, value) = line.split_once('=').unwrap();
        let mut parts = key.split('|');
        let (_, day, who) = (parts.next(), parts.next().unwrap(), parts.next().unwrap());
        let entry = won.entry(day.to_string()).or_default();
        if value.trim_matches('"') == "won" {
            entry.push(who.to_string());
        } else {
            assert_eq!(value.trim_matches('"'), "lost", "{line}");
        }
    }
    for (day, winners) in &won {
        assert_eq!(winners.len(), 1, "{day}: winners {winners:?}");
        // The claim row holds the winner's name.
        let holder = sql(&format!(
            "SELECT store_value FROM data_store WHERE store_name = 'claims' AND store_key = '{day}'"
        ));
        assert_eq!(holder.trim_matches('"'), winners[0], "{day}");
    }
    // Every contender recorded an outcome (checked above), so no instance failed: each lost
    // claim took the error boundary.
    assert_eq!(won.len(), DAYS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker"]
async fn tc_store_features_two_waits_on_one_channel_each_take_only_their_message_type() {
    let client = reqwest::Client::new();
    for reference in ["tw-1", "tw-2"] {
        post_json(
            &client,
            0,
            "waits-in",
            serde_json::json!({"ref": reference}),
        )
        .await;
    }
    // Both instances park at both waits before any relay is sent.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let parked: i64 = sql(
            "SELECT count(*) FROM alias_index WHERE alias_name IN ('answerRef', 'cancelRef') \
             AND alias_value LIKE 'tw-%'",
        )
        .parse()
        .unwrap_or(0);
        if parked == 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {parked} of 4 wait aliases indexed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let relay = |kind: &str, reference: &str| {
        format!(
            "<{kind}><ref>{reference}</ref><kind>{}</kind></{kind}>",
            kind.to_lowercase()
        )
    };

    // tw-1: the Cancel first. WaitAnswer sorts first by id; it must not take it.
    let status = post(
        &client,
        0,
        "answers-in",
        "application/xml",
        relay("Cancel", "tw-1"),
    )
    .await;
    assert!(status.is_success(), "cancel relay answered {status}");
    let cancel = stored("results", "wait|tw-1|cancel", Duration::from_secs(30))
        .await
        .expect("WaitCancel resumed by the Cancel");
    assert_eq!(cancel["kind"], "cancel", "{cancel}");
    assert!(
        stored("results", "wait|tw-1|answer", Duration::from_secs(2))
            .await
            .is_none(),
        "the Cancel did not resume WaitAnswer"
    );
    let status = post(
        &client,
        0,
        "answers-in",
        "application/xml",
        relay("Answer", "tw-1"),
    )
    .await;
    assert!(status.is_success(), "answer relay answered {status}");
    let answer = stored("results", "wait|tw-1|answer", Duration::from_secs(30))
        .await
        .expect("WaitAnswer resumed by the Answer");
    assert_eq!(answer["kind"], "answer", "{answer}");

    // tw-2: the other order.
    let status = post(
        &client,
        0,
        "answers-in",
        "application/xml",
        relay("Answer", "tw-2"),
    )
    .await;
    assert!(status.is_success(), "answer relay answered {status}");
    let answer = stored("results", "wait|tw-2|answer", Duration::from_secs(30))
        .await
        .expect("WaitAnswer resumed");
    assert_eq!(answer["kind"], "answer");
    assert!(
        stored("results", "wait|tw-2|cancel", Duration::from_secs(2))
            .await
            .is_none()
    );

    // A type neither wait declares is not a relay to them; tw-2 keeps waiting for its Cancel.
    let status = post(
        &client,
        0,
        "answers-in",
        "application/xml",
        relay("Other", "tw-2"),
    )
    .await;
    assert!(
        !status.is_success(),
        "an undeclared type is refused, got {status}"
    );
    assert!(
        stored("results", "wait|tw-2|cancel", Duration::from_secs(2))
            .await
            .is_none()
    );

    let status = post(
        &client,
        0,
        "answers-in",
        "application/xml",
        relay("Cancel", "tw-2"),
    )
    .await;
    assert!(status.is_success(), "cancel relay answered {status}");
    let cancel = stored("results", "wait|tw-2|cancel", Duration::from_secs(30))
        .await
        .expect("WaitCancel resumed");
    assert_eq!(cancel["kind"], "cancel");

    // Both instances completed.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let open: i64 = sql(
            "SELECT count(DISTINCT s.instance_id) FROM instance_state s \
             JOIN alias_index a USING (deployment_id, instance_id) \
             WHERE a.alias_name = 'answerRef' AND a.alias_value LIKE 'tw-%' \
             AND s.terminal_at IS NULL",
        )
        .parse()
        .unwrap_or(-1);
        if open == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{open} typed-waits instances still open"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
