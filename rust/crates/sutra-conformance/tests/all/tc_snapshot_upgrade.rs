//! Conformance: instances parked by an earlier release resume under this one, exactly once.
//!
//! Since snapshot format 5 the engine writes instance snapshots in a binary container; every
//! earlier release wrote Properties text. For each earlier release image (the published 0.6.5 and
//! 0.6.6 by default; `SUTRA_UPGRADE_FROM_IMAGES` overrides, comma-separated), on its own fresh
//! database: the old engine parks instances that hold typed state (a decimal, a list, a nested
//! context, text with characters the text container escaped) at a wait; it is stopped; the current
//! image starts on the same database, takes the decisions, and every instance resumes once, with
//! its state intact, and is stored back in the binary format.

use std::collections::BTreeSet;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::support::callback::start_recorder;
use crate::support::engine::{self, EngineBuilder};
use crate::support::util;

const API_KEY: &str = "snapshot-upgrade-key";
const DEFAULT_FROM: &str = "ghcr.io/startr-trade/sutra:0.6.5,ghcr.io/startr-trade/sutra:0.6.6";
const ORDERS: usize = 10;
/// The format-5 marker, as PostgreSQL bytea hex (`\uSNAPSHOT/5`, without its newline).
const MARKER_HEX: &str = "5c75534e415053484f542f35";

fn archives() -> std::path::PathBuf {
    let package = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/snapshot-upgrade/default--upgrade--1.0.0");
    let out = util::world_readable_temp_dir("sutra-archives");
    sutra_loader::package::assemble_dir(&package, &out, &sutra_loader::PackageOptions::default())
        .unwrap_or_else(|e| panic!("assemble {}: {e}", package.display()));
    util::make_world_readable(&out);
    out
}

fn sql(container: &str, query: &str) -> String {
    let out = Command::new("docker")
        .args([
            "exec", container, "psql", "-U", "postgres", "-d", "postgres", "-tAc", query,
        ])
        .output()
        .expect("docker exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn count(container: &str, query: &str) -> i64 {
    sql(container, query).parse().unwrap_or(-1)
}

async fn post(port: u16, channel: &str, body: String) {
    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/channels/{channel}"))
        .header("Content-Type", "application/json")
        .header("X-Api-Key", API_KEY)
        .body(body.clone())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .unwrap_or_else(|e| panic!("post {channel}: {e}"))
        .status();
    assert!(status.is_success(), "{channel} answered {status} to {body}");
}

async fn wait_for(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn tc_snapshot_upgrade_instances_parked_by_earlier_releases_resume_once() {
    let from = std::env::var("SUTRA_UPGRADE_FROM_IMAGES").unwrap_or_else(|_| DEFAULT_FROM.into());
    let archives = archives();
    for (n, old_image) in from
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .enumerate()
    {
        let prefix = format!("up{n}");
        let old_archives = archives.clone();
        let old_image = old_image.to_string();
        // Fixtures are built on a plain thread (see `EngineHandle`'s drop notes) and leaked.
        let (pg, recorder, old) = std::thread::spawn(move || {
            let pg = engine::start_postgres("upgrade");
            let recorder = start_recorder();
            let old = EngineBuilder::new("upgrade-old", &pg)
                .image(old_image)
                .env("UPGRADE_API_KEY", API_KEY)
                .env(
                    "UPGRADE_CALLBACK_URL",
                    format!("http://host.docker.internal:{}/callback", recorder.port),
                )
                .host_gateway()
                .expected_deployments(1)
                .start(&old_archives);
            (pg, recorder, old)
        })
        .join()
        .expect("old engine");
        let (pg, recorder, old) = Box::leak(Box::new((pg, recorder, old)));
        let db = pg.container.id().to_string();

        for i in 0..ORDERS {
            post(
                old.http_port,
                "order-in",
                format!(
                    r#"{{"ref":"{prefix}-{i}","amount":{i}.5,"items":["a","b=c","é"],"note":"line1\nline2 = : # ! é"}}"#
                ),
            )
            .await;
        }
        wait_for(
            "the old release to park every instance",
            Duration::from_secs(60),
            || {
                count(
                    &db,
                    "SELECT count(*) FROM waiting_event WHERE status = 'WAITING'",
                ) == ORDERS as i64
            },
        )
        .await;
        assert_eq!(
            count(
                &db,
                "SELECT count(*) FROM instance_state WHERE position('sutra.'::bytea in serialised) = 1"
            ),
            ORDERS as i64,
            "the earlier release stored text snapshots"
        );

        // `SUTRA_UPGRADE_DUMP_DIR`: keep the old release's stored bytes (how the tier-1 fixture
        // corpus of earlier-release snapshots was captured).
        if let Ok(dir) = std::env::var("SUTRA_UPGRADE_DUMP_DIR") {
            let rows = sql(
                &db,
                "SELECT instance_id || ' ' || encode(serialised, 'hex') FROM instance_state",
            );
            std::fs::create_dir_all(&dir).expect("dump dir");
            for (j, row) in rows.lines().enumerate() {
                let hex = row.split_whitespace().nth(1).unwrap_or_default();
                let bytes: Vec<u8> = (0..hex.len())
                    .step_by(2)
                    .map(|k| u8::from_str_radix(&hex[k..k + 2], 16).expect("hex"))
                    .collect();
                std::fs::write(format!("{dir}/{prefix}-{j}.snapshot"), bytes).expect("dump");
            }
        }

        // Swap engines: the old one stops, the current one starts on the same database.
        let stopped = Command::new("docker")
            .args(["stop", old.container.id()])
            .status()
            .expect("docker stop");
        assert!(stopped.success());
        let callback = format!("http://host.docker.internal:{}/callback", recorder.port);
        let new_archives = archives.clone();
        let pg_ref: &'static engine::PgFixture = pg;
        let new = std::thread::spawn(move || {
            EngineBuilder::new("upgrade-new", pg_ref)
                .env("UPGRADE_API_KEY", API_KEY)
                .env("UPGRADE_CALLBACK_URL", callback)
                .host_gateway()
                .expected_deployments(1)
                .start(&new_archives)
        })
        .join()
        .expect("current engine");
        let new = Box::leak(Box::new(new));

        for i in 0..ORDERS {
            post(
                new.http_port,
                "decision-in",
                format!(r#"{{"ref":"{prefix}-{i}","verdict":"APPROVED"}}"#),
            )
            .await;
        }
        let needle = format!("\"{prefix}-");
        wait_for(
            "every resumed instance to call back",
            Duration::from_secs(60),
            || recorder.recorder.matching(&needle).len() >= ORDERS,
        )
        .await;
        // Exactly once each, with the state the old release parked.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let bodies = recorder.recorder.matching(&needle);
        assert_eq!(
            bodies.len(),
            ORDERS,
            "one callback per instance: {bodies:?}"
        );
        let mut refs = BTreeSet::new();
        for body in &bodies {
            let v: serde_json::Value = serde_json::from_str(body).expect("json callback");
            let r = v["ref"].as_str().expect("ref").to_string();
            let i: f64 = r.rsplit('-').next().unwrap().parse().unwrap();
            assert_eq!(v["verdict"], "APPROVED", "{body}");
            assert_eq!(v["amount"].as_f64(), Some(i + 0.5), "{body}");
            assert_eq!(v["doubled"].as_f64(), Some((i + 0.5) * 2.0), "{body}");
            assert_eq!(v["items"], serde_json::json!(["a", "b=c", "é"]), "{body}");
            assert_eq!(v["count"].as_f64(), Some(3.0), "{body}");
            assert_eq!(v["note"], "line1\nline2 = : # ! é", "{body}");
            assert_eq!(v["flag"], true, "{body}");
            assert_eq!(v["ratio"].as_f64(), Some(0.25), "{body}");
            refs.insert(r);
        }
        assert_eq!(refs.len(), ORDERS, "every instance, none twice");
        assert_eq!(
            count(
                &db,
                "SELECT count(*) FROM instance_state WHERE terminal_at IS NULL"
            ),
            0,
            "every instance completed"
        );
        assert_eq!(
            count(
                &db,
                &format!(
                    "SELECT count(*) FROM instance_state \
                     WHERE position('\\x{MARKER_HEX}'::bytea in serialised) = 1"
                )
            ),
            ORDERS as i64,
            "the current release stored the finished instances in the binary format"
        );
        let _ = Command::new("docker")
            .args(["stop", new.container.id()])
            .status();
    }
}
