//! The durable audit spool (tier 1, temporary directories): group sync and its barrier, crash
//! recovery, the lock, I/O-error recovery, back-pressure, undeliverable events, linger and
//! segment retention.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sutra_channels::audit_spool::{segment_name, write_checkpoint, SpoolFaultInjector};
use sutra_channels::{AuditEvent, AuditSpool, AuditSpoolConfig, SpoolError};

pub(crate) fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sutra-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn config(dir: &Path) -> AuditSpoolConfig {
    AuditSpoolConfig {
        dir: dir.to_path_buf(),
        sync_linger: Duration::ZERO,
        sync_timeout: Duration::from_secs(5),
        sinks: ["jsonl", "otel"].map(String::from).into_iter().collect(),
        ..AuditSpoolConfig::default()
    }
}

pub(crate) fn event(sink: &str, seq: u32) -> AuditEvent {
    AuditEvent {
        sink: sink.into(),
        deployment_id: "dep-000000000000000000000001".into(),
        tenant: "acme".into(),
        instance_id: "11111111-1111-1111-1111-111111111111".into(),
        seq,
        at: "2026-11-24T12:00:00Z".into(),
        event_type: "NODE_ENTERED".into(),
        node_id: Some("Start".into()),
        diagnostic_code: None,
        diagnostic_json: None,
        payload_json: "{}".into(),
        correlation: Default::default(),
        attempt: None,
    }
}

fn segments(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    names.sort();
    names
}

fn read_all(spool: &AuditSpool) -> Vec<(u64, u32)> {
    spool
        .reader(0)
        .next_batch(100_000, spool.durable_through())
        .unwrap()
        .into_iter()
        .map(|r| (r.n, r.event.seq))
        .collect()
}

#[tokio::test]
async fn barrier_resolves_after_fsync() {
    let dir = temp_dir("spool-barrier");
    let spool = AuditSpool::open(AuditSpoolConfig {
        sync_linger: Duration::from_millis(200),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    assert_eq!(
        spool.durable_through(),
        0,
        "nothing is durable before the sync"
    );
    sender.barrier().await.unwrap();
    assert!(
        spool.durable_through() >= 1,
        "the barrier waited for the sync"
    );
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn events_before_barrier_are_durable() {
    let dir = temp_dir("spool-durable");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    for seq in 1..=3 {
        sender.append(event("jsonl", seq));
    }
    sender.barrier().await.unwrap();
    // The writer stops as a killed process would: nothing more is synced or closed.
    spool.crash();
    let reopened = AuditSpool::open(config(&dir)).unwrap();
    assert_eq!(read_all(&reopened), vec![(1, 1), (2, 2), (3, 3)]);
    reopened.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn torn_tail_truncated_on_open() {
    let dir = temp_dir("spool-torn");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.append(event("jsonl", 2));
    sender.barrier().await.unwrap();
    spool.shutdown().unwrap();
    let segment = dir.join(&segments(&dir)[0]);
    let good = std::fs::metadata(&segment).unwrap().len();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap();
        file.write_all(b"{\"v\":1,\"spool\":\"x\",\"n\":3,\"si")
            .unwrap();
    }
    let reopened = AuditSpool::open(config(&dir)).unwrap();
    assert_eq!(std::fs::metadata(&segment).unwrap().len(), good);
    assert_eq!(reopened.durable_through(), 2);
    let sender = reopened.sender();
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap();
    assert_eq!(read_all(&reopened), vec![(1, 1), (2, 2), (3, 3)]);
    reopened.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn n_monotonic_across_reopen() {
    let dir = temp_dir("spool-reopen");
    for round in 0..3u32 {
        let spool = AuditSpool::open(config(&dir)).unwrap();
        let sender = spool.sender();
        sender.append(event("jsonl", round * 2 + 1));
        sender.append(event("otel", round * 2 + 2));
        sender.barrier().await.unwrap();
        spool.shutdown().unwrap();
    }
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let ns: Vec<u64> = read_all(&spool).into_iter().map(|(n, _)| n).collect();
    assert_eq!(ns, vec![1, 2, 3, 4, 5, 6]);
    let id = spool.spool_id().to_string();
    spool.shutdown().unwrap();
    // The spool id is minted once and kept.
    let again = AuditSpool::open(config(&dir)).unwrap();
    assert_eq!(again.spool_id(), id);
    again.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn second_open_same_dir_locked() {
    let dir = temp_dir("spool-lock");
    let first = AuditSpool::open(config(&dir)).unwrap();
    match AuditSpool::open(config(&dir)) {
        Err(SpoolError::Locked(locked)) => assert_eq!(locked, dir),
        Err(other) => panic!("expected Locked, got {other}"),
        Ok(_) => panic!("a second open of a held spool must be refused"),
    }
    first.shutdown().unwrap();
    // Released at shutdown: the next process opens it.
    AuditSpool::open(config(&dir)).unwrap().shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn io_error_fails_barrier_and_recovers_into_new_segment() {
    let dir = temp_dir("spool-ioerr");
    let faults = Arc::new(SpoolFaultInjector::default());
    let spool = AuditSpool::open_with_faults(
        AuditSpoolConfig {
            sync_timeout: Duration::from_millis(300),
            ..config(&dir)
        },
        Some(Arc::clone(&faults)),
    )
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.barrier().await.unwrap();
    faults.fail_next_syncs(1);
    sender.append(event("jsonl", 2));
    match sender.barrier().await {
        Err(SpoolError::Io(_)) => {}
        other => panic!("a failed sync fails the barrier, got {other:?}"),
    }
    assert!(!spool.healthy(), "unhealthy while the batch is held");
    // The writer retries every second, into a new segment; work resumes by itself.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !spool.healthy() {
        assert!(Instant::now() < deadline, "the spool recovers");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap();
    let names = segments(&dir);
    assert!(names.contains(&segment_name(1)));
    assert!(
        names.contains(&segment_name(2)),
        "the held batch went to a new segment named after its first record: {names:?}"
    );
    // Every record exactly once, though the failed segment may hold a copy of record 2.
    assert_eq!(read_all(&spool), vec![(1, 1), (2, 2), (3, 3)]);
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn full_spool_fails_barrier_keeps_writing() {
    let dir = temp_dir("spool-full");
    let spool = AuditSpool::open(AuditSpoolConfig {
        max_bytes: 1,
        sync_timeout: Duration::from_millis(250),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    // This batch puts the spool over max-bytes (its own barrier may or may not ride with it).
    let _ = sender.barrier().await;
    sender.append(event("jsonl", 2));
    match sender.barrier().await {
        Err(SpoolError::Full { unshipped_bytes }) => assert!(unshipped_bytes > 1),
        other => panic!("an over-full spool fails the barrier, got {other:?}"),
    }
    assert_eq!(spool.durable_through(), 2, "events are still written");
    assert!(spool.healthy(), "the disk is fine");
    assert!(
        !sender.accepting(),
        "but readiness is DOWN while the spool is full"
    );
    assert!(spool.metrics().unshipped_bytes() > 1);
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn missing_sink_counts_undeliverable_never_reroutes() {
    let dir = temp_dir("spool-undeliverable");
    let spool = AuditSpool::open(AuditSpoolConfig {
        sinks: ["jsonl".to_string()].into_iter().collect(),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("sql", 1));
    sender.append(event("", 2));
    sender.append(event("sql", 3));
    sender.barrier().await.unwrap();
    assert_eq!(
        spool.durable_through(),
        0,
        "nothing is spooled for a missing store"
    );
    assert!(read_all(&spool).is_empty(), "and nothing is re-routed");
    let counted = spool.metrics().undeliverable();
    let dep = "dep-000000000000000000000001".to_string();
    assert_eq!(counted.get(&("sql".to_string(), dep.clone())), Some(&2));
    assert_eq!(counted.get(&(String::new(), dep)), Some(&1));
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn linger_zero_drains_without_wait() {
    let dir = temp_dir("spool-linger0");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    let started = Instant::now();
    for seq in 1..=10 {
        sender.append(event("jsonl", seq));
        sender.barrier().await.unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "ten syncs with no linger: {:?}",
        started.elapsed()
    );
    assert_eq!(spool.durable_through(), 10);
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn linger_groups_until_deadline() {
    let dir = temp_dir("spool-linger");
    let spool = AuditSpool::open(AuditSpoolConfig {
        sync_linger: Duration::from_millis(300),
        ..config(&dir)
    })
    .unwrap();
    let mut durable = spool.subscribe();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    tokio::time::sleep(Duration::from_millis(50)).await;
    sender.append(event("jsonl", 2));
    durable.changed().await.unwrap();
    assert_eq!(
        *durable.borrow(),
        2,
        "both events went out in the one sync that the linger held open"
    );
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cap_flushes_early() {
    let dir = temp_dir("spool-cap");
    let spool = AuditSpool::open(AuditSpoolConfig {
        sync_linger: Duration::from_secs(30),
        ..config(&dir)
    })
    .unwrap();
    let mut durable = spool.subscribe();
    let sender = spool.sender();
    for seq in 0..sutra_channels::audit_spool::SPOOL_BATCH_MAX_MESSAGES as u32 {
        sender.append(event("jsonl", seq + 1));
    }
    // A cap (4096 messages or 1 MiB) closes the group long before the 30 s linger.
    tokio::time::timeout(Duration::from_secs(10), durable.wait_for(|n| *n >= 1000))
        .await
        .expect("the cap syncs long before the 30 s linger")
        .unwrap();
    drop(durable);
    spool.crash();
    let _ = std::fs::remove_dir_all(dir);
}

/// Three synced batches, each in its own segment (`segment-bytes` 1 rotates after every sync).
async fn three_segments(dir: &Path) -> String {
    let spool = AuditSpool::open(AuditSpoolConfig {
        segment_bytes: 1,
        retain: Duration::ZERO,
        sinks: ["jsonl".to_string()].into_iter().collect(),
        ..config(dir)
    })
    .unwrap();
    let sender = spool.sender();
    for seq in 1..=2 {
        sender.append(event("jsonl", seq));
        sender.barrier().await.unwrap();
    }
    let id = spool.spool_id().to_string();
    spool.shutdown().unwrap();
    id
}

#[tokio::test]
async fn shipped_segment_deleted_after_retain() {
    let dir = temp_dir("spool-retain");
    let id = three_segments(&dir).await;
    assert_eq!(segments(&dir), vec![segment_name(1), segment_name(2)]);
    write_checkpoint(&dir, "jsonl", &id, 2).unwrap();
    let spool = AuditSpool::open(AuditSpoolConfig {
        segment_bytes: 1,
        retain: Duration::ZERO,
        sinks: ["jsonl".to_string()].into_iter().collect(),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap();
    // The rotation after this sync sweeps both shipped segments; the newest stays.
    let deadline = Instant::now() + Duration::from_secs(5);
    while segments(&dir) != vec![segment_name(3)] {
        assert!(Instant::now() < deadline, "swept: {:?}", segments(&dir));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn unshipped_segment_kept() {
    let dir = temp_dir("spool-keep");
    three_segments(&dir).await;
    let spool = AuditSpool::open(AuditSpoolConfig {
        segment_bytes: 1,
        retain: Duration::ZERO,
        sinks: ["jsonl".to_string()].into_iter().collect(),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap();
    // Give a sweep the time it would need, then check nothing went.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        segments(&dir),
        vec![segment_name(1), segment_name(2), segment_name(3)],
        "no store has shipped them: nothing is deleted"
    );
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// The open segment is zero-filled ahead of its records, so a sync never extends the file.
/// The zeros are never read as records, a crash leaves them behind harmlessly (the next open
/// continues the numbering and cuts them off without reporting a torn tail), and a cleanly
/// closed segment holds its records only.
#[tokio::test]
async fn open_segment_is_preallocated_and_zeros_are_never_records() {
    let dir = temp_dir("spool-prealloc");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.append(event("jsonl", 2));
    sender.barrier().await.unwrap();
    let segment = dir.join(&segments(&dir)[0]);
    let len = std::fs::metadata(&segment).unwrap().len();
    let body = std::fs::read(&segment).unwrap();
    let records = body.iter().position(|b| *b == 0).expect("zero-filled tail");
    assert!(len > records as u64, "the file is longer than its records");
    assert_eq!(read_all(&spool), vec![(1, 1), (2, 2)]);
    spool.crash();

    let spool = AuditSpool::open(config(&dir)).unwrap();
    assert_eq!(
        read_all(&spool),
        vec![(1, 1), (2, 2)],
        "zeros are not records"
    );
    let sender = spool.sender();
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap();
    assert_eq!(read_all(&spool), vec![(1, 1), (2, 2), (3, 3)]);
    spool.shutdown().unwrap();
    let body = std::fs::read(&segment).unwrap();
    assert!(
        !body.contains(&0),
        "a closed segment holds its records only"
    );
    assert_eq!(body.iter().filter(|b| **b == b'\n').count(), 3);
    let _ = std::fs::remove_dir_all(dir);
}

/// The linger is a group-commit window: a lone barrier (one lane, nobody else waiting) is
/// synced at once instead of waiting out the linger for company that is not coming, and the
/// events before it are durable when it resolves.
#[tokio::test]
async fn lone_barrier_does_not_wait_out_the_linger() {
    let dir = temp_dir("spool-lone");
    let spool = AuditSpool::open(AuditSpoolConfig {
        sync_linger: Duration::from_millis(500),
        ..config(&dir)
    })
    .unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    let started = std::time::Instant::now();
    sender.barrier().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "a lone barrier waited {:?}",
        started.elapsed()
    );
    assert_eq!(spool.durable_through(), 1);
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
