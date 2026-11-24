//! The JSONL file audit store (tier 1, temporary directories): rotation, the line format, the
//! replay dedup window, the legacy single file, and retention.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sutra_channels::{AuditFilesConfig, AuditSink, JsonlFileStore, SpoolRecord};

use crate::audit_spool::{event, temp_dir};

const SPOOL: &str = "4f6c7a2e-0000-4000-8000-000000000001";

fn record(n: u64, seq: u32) -> SpoolRecord {
    SpoolRecord {
        n,
        event: event("jsonl", seq),
    }
}

fn audit_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| Some(e.ok()?.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("audit-") && n.ends_with(".jsonl"))
        })
        .collect();
    files.sort();
    files
}

fn lines(dir: &Path) -> Vec<serde_json::Value> {
    audit_files(dir)
        .iter()
        .flat_map(|p| {
            std::fs::read_to_string(p)
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn rotate_by_size() {
    let dir = temp_dir("files-size");
    let store = JsonlFileStore::open(
        AuditFilesConfig {
            rotate_bytes: 1,
            ..AuditFilesConfig::new(&dir)
        },
        SPOOL,
    )
    .unwrap();
    for n in 1..=3 {
        store.ship(SPOOL, &[record(n, n as u32)]).await.unwrap();
    }
    let files = audit_files(&dir);
    assert_eq!(files.len(), 3, "each batch found the file full: {files:?}");
    for file in &files {
        let name = file.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with(&format!("audit-{SPOOL}-")), "{name}");
    }
    assert_eq!(store.open_file().as_ref(), files.last());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn rotate_by_interval() {
    let dir = temp_dir("files-interval");
    let store = JsonlFileStore::open(
        AuditFilesConfig {
            rotate_interval: Duration::from_millis(1),
            ..AuditFilesConfig::new(&dir)
        },
        SPOOL,
    )
    .unwrap();
    store.ship(SPOOL, &[record(1, 1)]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    store.ship(SPOOL, &[record(2, 2)]).await.unwrap();
    assert_eq!(audit_files(&dir).len(), 2);
    let seqs: Vec<u64> = lines(&dir)
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2], "file names sort in write order");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn line_is_replay_contract_plus_spool_ref() {
    let dir = temp_dir("files-line");
    let store = JsonlFileStore::open(AuditFilesConfig::new(&dir), SPOOL).unwrap();
    let mut r = record(7, 3);
    r.event.diagnostic_json = Some("{\"error\":\"x\"}".into());
    store.ship(SPOOL, std::slice::from_ref(&r)).await.unwrap();
    let line = &lines(&dir)[0];
    let contract: serde_json::Value = serde_json::from_str(&r.event.to_jsonl()).unwrap();
    for (key, value) in contract.as_object().unwrap() {
        assert_eq!(&line[key], value, "replay key {key} unchanged");
    }
    assert_eq!(line["diagnosticJson"], "{\"error\":\"x\"}");
    assert_eq!(line["spoolRef"], format!("{SPOOL}:7"));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn replayed_seq_dropped_within_window() {
    let dir = temp_dir("files-dedup");
    let store = JsonlFileStore::open(AuditFilesConfig::new(&dir), SPOOL).unwrap();
    store
        .ship(SPOOL, &[record(1, 1), record(2, 2)])
        .await
        .unwrap();
    // The same record shipped again (no attempt here, so the same key) under a new spool record.
    store
        .ship(SPOOL, &[record(3, 2), record(4, 3)])
        .await
        .unwrap();
    let seqs: Vec<u64> = lines(&dir)
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(
        store
            .duplicates_dropped()
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn dedup_cache_rebuilt_on_open() {
    let dir = temp_dir("files-rebuild");
    {
        let store = JsonlFileStore::open(AuditFilesConfig::new(&dir), SPOOL).unwrap();
        store.ship(SPOOL, &[record(1, 1)]).await.unwrap();
    }
    let store = JsonlFileStore::open(AuditFilesConfig::new(&dir), SPOOL).unwrap();
    store
        .ship(SPOOL, &[record(2, 1), record(3, 2)])
        .await
        .unwrap();
    let seqs: Vec<u64> = lines(&dir)
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2], "the restarted store still knew seq 1");
    assert_eq!(store.recover(SPOOL).await.unwrap(), Some(3));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn legacy_path_still_writes() {
    let dir = temp_dir("files-legacy");
    let path = dir.join("audit.jsonl");
    let store = JsonlFileStore::open_legacy_file(path.clone(), SPOOL).unwrap();
    store
        .ship(SPOOL, &[record(1, 1), record(2, 2)])
        .await
        .unwrap();
    let body = std::fs::read_to_string(&path).unwrap();
    let parsed: Vec<serde_json::Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[1]["spoolRef"], format!("{SPOOL}:2"));
    assert_eq!(store.recover(SPOOL).await.unwrap(), Some(2));
    let _ = std::fs::remove_dir_all(dir);
}

/// Three files, the first two closed (`rotate-interval` 1 ms between batches).
async fn three_files(dir: &Path, retention: Option<Duration>) -> JsonlFileStore {
    let store = JsonlFileStore::open(
        AuditFilesConfig {
            rotate_interval: Duration::from_millis(1),
            retention,
            ..AuditFilesConfig::new(dir)
        },
        SPOOL,
    )
    .unwrap();
    for n in 1..=3 {
        store.ship(SPOOL, &[record(n, n as u32)]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(audit_files(dir).len(), 3);
    store
}

#[tokio::test]
async fn jsonl_files_past_retention_deleted() {
    let dir = temp_dir("files-retention");
    let store = three_files(&dir, Some(Duration::ZERO)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(store.sweep_retention().unwrap(), 2);
    assert_eq!(audit_files(&dir), vec![store.open_file().unwrap()]);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn open_file_never_deleted() {
    let dir = temp_dir("files-open");
    let store = JsonlFileStore::open(
        AuditFilesConfig {
            retention: Some(Duration::ZERO),
            ..AuditFilesConfig::new(&dir)
        },
        SPOOL,
    )
    .unwrap();
    store.ship(SPOOL, &[record(1, 1)]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(store.sweep_retention().unwrap(), 0);
    assert_eq!(audit_files(&dir).len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn no_retention_keeps_all() {
    let dir = temp_dir("files-keep");
    let store = three_files(&dir, None).await;
    assert_eq!(store.sweep_retention().unwrap(), 0);
    assert_eq!(audit_files(&dir).len(), 3);
    let _ = std::fs::remove_dir_all(dir);
}

/// A re-run from the same base writes its lines next to the lost attempt's (the files are
/// append-only; readers keep the latest attempt), while the same attempt's record shipped twice
/// is dropped.
#[tokio::test]
async fn another_attempt_is_written_a_reshipped_record_is_not() {
    let dir = temp_dir("files-attempts");
    let store = JsonlFileStore::open(AuditFilesConfig::new(&dir), SPOOL).unwrap();
    let attempt = |n: u64, seq: u32, id: &str| {
        let mut r = record(n, seq);
        r.event.attempt = Some(sutra_channels::AuditAttempt {
            id: id.into(),
            base: 1,
            at: format!("2026-11-24T10:00:0{}Z", n),
        });
        r
    };
    store.ship(SPOOL, &[attempt(1, 2, "lost")]).await.unwrap();
    store
        .ship(SPOOL, &[attempt(2, 2, "kept"), attempt(2, 2, "kept")])
        .await
        .unwrap();
    let written: Vec<(u64, String)> = lines(&dir)
        .iter()
        .map(|l| {
            (
                l["seq"].as_u64().unwrap(),
                l["attemptId"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(written, vec![(2, "lost".into()), (2, "kept".into())]);
    let _ = std::fs::remove_dir_all(dir);
}
