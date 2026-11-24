//! The audit shipper (tier 1, temporary directories): checkpointing, crash idempotence, retry,
//! the durable bound, quarantine, close and linger.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sutra_channels::audit_spool::{read_checkpoint, SpoolFaultInjector};
use sutra_channels::{
    spawn_audit_shipper, AuditFilesConfig, AuditSink, AuditSpool, AuditSpoolConfig, BoxFuture,
    Diagnostic, JsonlFileStore, ShipperConfig, SpoolRecord,
};

use crate::audit_spool::{config, event, temp_dir};

fn shipper_config() -> ShipperConfig {
    ShipperConfig {
        batch_max: 256,
        batch_linger: Duration::ZERO,
        backoff_max: Duration::from_millis(200),
    }
}

/// A store that records the `n`s of each batch it is handed, failing the first `fail` calls.
#[derive(Default)]
struct RecordingStore {
    name: String,
    batches: Mutex<Vec<Vec<u64>>>,
    fail: Mutex<u32>,
}

impl RecordingStore {
    fn named(name: &str) -> Arc<RecordingStore> {
        Arc::new(RecordingStore {
            name: name.into(),
            ..Default::default()
        })
    }
    fn failing(name: &str, fail: u32) -> Arc<RecordingStore> {
        let store = Self::named(name);
        *store.fail.lock().unwrap() = fail;
        store
    }
    fn batches(&self) -> Vec<Vec<u64>> {
        self.batches.lock().unwrap().clone()
    }
    fn shipped(&self) -> Vec<u64> {
        self.batches().into_iter().flatten().collect()
    }
}

impl AuditSink for RecordingStore {
    fn name(&self) -> &str {
        &self.name
    }
    fn ship<'a>(
        &'a self,
        _spool_id: &'a str,
        records: &'a [SpoolRecord],
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        self.batches
            .lock()
            .unwrap()
            .push(records.iter().map(|r| r.n).collect());
        let mut fail = self.fail.lock().unwrap();
        let result = if *fail > 0 {
            *fail -= 1;
            Err(Diagnostic::error("TEST.SHIP", "refused"))
        } else {
            Ok(())
        };
        Box::pin(async move { result })
    }
}

fn file_lines(dir: &std::path::Path) -> Vec<serde_json::Value> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| Some(e.ok()?.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    names.sort();
    names
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

fn handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::current()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ship_then_checkpoint() {
    let dir = temp_dir("ship-ckpt");
    let files = temp_dir("ship-ckpt-files");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store =
        Arc::new(JsonlFileStore::open(AuditFilesConfig::new(&files), spool.spool_id()).unwrap());
    let shipper = spawn_audit_shipper(&handle(), &spool, store, shipper_config());
    let sender = spool.sender();
    for seq in 1..=3 {
        sender.append(event("jsonl", seq));
    }
    sender.barrier().await.unwrap();
    shipper
        .ship_through(3, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(read_checkpoint(&dir, "jsonl", spool.spool_id()), 3);
    let lines = file_lines(&files);
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[2]["spoolRef"], format!("{}:3", spool.spool_id()));
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_ship_before_checkpoint_no_duplicate() {
    let dir = temp_dir("ship-crash-after");
    let files = temp_dir("ship-crash-after-files");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    for seq in 1..=3 {
        sender.append(event("jsonl", seq));
    }
    sender.barrier().await.unwrap();
    let store =
        Arc::new(JsonlFileStore::open(AuditFilesConfig::new(&files), spool.spool_id()).unwrap());
    // A previous run shipped records 1..=3 into the files, then died before its checkpoint.
    let records = spool.reader(0).next_batch(10, 3).unwrap();
    store.ship(spool.spool_id(), &records).await.unwrap();
    assert_eq!(read_checkpoint(&dir, "jsonl", spool.spool_id()), 0);
    let shipper = spawn_audit_shipper(&handle(), &spool, store, shipper_config());
    sender.append(event("jsonl", 4));
    sender.barrier().await.unwrap();
    shipper
        .ship_through(4, Duration::from_secs(5))
        .await
        .unwrap();
    let seqs: Vec<u64> = file_lines(&files)
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4],
        "recover() resumed after the records the files hold"
    );
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(files);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_ship_no_loss() {
    let dir = temp_dir("ship-crash-before");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    for seq in 1..=3 {
        sender.append(event("jsonl", seq));
    }
    sender.barrier().await.unwrap();
    spool.crash();
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    shipper
        .ship_through(3, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.shipped(), vec![1, 2, 3]);
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_ship_retries_same_batch() {
    let dir = temp_dir("ship-retry");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.append(event("jsonl", 2));
    sender.barrier().await.unwrap();
    let store = RecordingStore::failing("jsonl", 2);
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    shipper
        .ship_through(2, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.batches(), vec![vec![1, 2], vec![1, 2], vec![1, 2]]);
    assert_eq!(spool.metrics().ship_failures().get("jsonl"), Some(&2));
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn never_ships_past_durable() {
    let dir = temp_dir("ship-durable");
    let faults = Arc::new(SpoolFaultInjector::default());
    let spool = AuditSpool::open_with_faults(
        AuditSpoolConfig {
            sync_timeout: Duration::from_millis(200),
            ..config(&dir)
        },
        Some(Arc::clone(&faults)),
    )
    .unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.barrier().await.unwrap();
    shipper
        .ship_through(1, Duration::from_secs(5))
        .await
        .unwrap();
    // Record 2 reaches the file, but its sync fails: it is not durable, so it is not shipped.
    faults.fail_next_syncs(1);
    sender.append(event("jsonl", 2));
    assert!(sender.barrier().await.is_err());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(store.shipped(), vec![1]);
    // Once the writer has re-synced it, it ships.
    shipper
        .ship_through(2, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.shipped(), vec![1, 2]);
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_mid_segment_quarantined() {
    let dir = temp_dir("ship-corrupt");
    let cfg = AuditSpoolConfig {
        segment_bytes: 1,
        sync_linger: Duration::from_millis(100),
        ..config(&dir)
    };
    let spool = AuditSpool::open(cfg.clone()).unwrap();
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.append(event("jsonl", 2));
    sender.barrier().await.unwrap(); // records 1 and 2 share the first segment
    sender.append(event("jsonl", 3));
    sender.barrier().await.unwrap(); // record 3 is alone in the second
    spool.shutdown().unwrap();
    let first = dir.join(sutra_channels::audit_spool::segment_name(1));
    let body = std::fs::read(&first).unwrap();
    let newline = body.iter().position(|b| *b == b'\n').unwrap();
    let mut corrupt = vec![b'#'; newline];
    corrupt.extend_from_slice(&body[newline..]);
    std::fs::write(&first, corrupt).unwrap();

    let spool = AuditSpool::open(cfg).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    shipper
        .ship_through(3, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(dir
        .join("quarantine")
        .join(sutra_channels::audit_spool::segment_name(1))
        .exists());
    assert_eq!(store.shipped(), vec![3], "the next segment still ships");
    assert_eq!(spool.metrics().corrupt(), 1);
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_flushes_pending() {
    let dir = temp_dir("ship-close");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(
        &handle(),
        &spool,
        store.clone(),
        ShipperConfig {
            batch_linger: Duration::from_millis(500),
            ..shipper_config()
        },
    );
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.barrier().await.unwrap();
    // Shut down while the batch is still lingering: it ships before the shipper stops.
    shipper.shutdown(Duration::from_secs(5)).await;
    assert_eq!(store.shipped(), vec![1]);
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_sinks_records_advance_checkpoint() {
    let dir = temp_dir("ship-other");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.append(event("otel", 2));
    sender.append(event("jsonl", 3));
    sender.append(event("otel", 4));
    sender.barrier().await.unwrap();
    shipper
        .ship_through(4, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.shipped(), vec![1, 3], "only its own store's records");
    assert_eq!(
        read_checkpoint(&dir, "jsonl", spool.spool_id()),
        4,
        "the checkpoint covers every record read"
    );
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shipper_linger_batches() {
    let dir = temp_dir("ship-linger");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(
        &handle(),
        &spool,
        store.clone(),
        ShipperConfig {
            batch_linger: Duration::from_millis(400),
            ..shipper_config()
        },
    );
    let sender = spool.sender();
    sender.append(event("jsonl", 1));
    sender.barrier().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    sender.append(event("jsonl", 2));
    sender.barrier().await.unwrap();
    shipper
        .ship_through(2, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        store.batches(),
        vec![vec![1, 2]],
        "the linger after the first record gathered the second into the same batch"
    );
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// An event of attempt `id` (base 0) for instance `instance`.
fn attempt_event(instance: &str, seq: u32, id: &str) -> sutra_channels::AuditEvent {
    sutra_channels::AuditEvent {
        instance_id: instance.into(),
        attempt: Some(sutra_channels::AuditAttempt {
            id: id.into(),
            base: 0,
            at: "2026-11-24T10:00:00Z".into(),
        }),
        ..event("jsonl", seq)
    }
}

fn open_attempt(instance: &str, id: &str, last_seq: u32) -> sutra_channels::OpenAttempt {
    sutra_channels::OpenAttempt {
        attempt: sutra_channels::AuditAttempt {
            id: id.into(),
            base: 0,
            at: "2026-11-24T10:00:00Z".into(),
        },
        deployment_id: event("jsonl", 1).deployment_id,
        instance_id: instance.into(),
        last_seq,
        terminal: false,
        xid: None,
    }
}

/// An attempt waiting for its commit marker holds back only its own records; the
/// others (a committed attempt, a record outside any pass) ship, and the checkpoint stops
/// before the held record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_attempt_does_not_block_other_attempts() {
    let dir = temp_dir("ship-held");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    let sender = spool.sender();
    sender.append(attempt_event("i-a", 1, "slow")); // n 1: held
    sender.append(attempt_event("i-b", 1, "fast")); // n 2
    sender.append(open_attempt("i-b", "fast", 1).marker(true)); // n 3
    sender.append(event("jsonl", 0)); // n 4: no attempt
    sender.barrier().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while store.shipped().len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "{:?}",
            store.batches()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        store.shipped(),
        vec![2, 4],
        "the held attempt blocks nothing"
    );
    assert_eq!(read_checkpoint(&dir, "jsonl", spool.spool_id()), 0);
    sender.append(open_attempt("i-a", "slow", 1).marker(true)); // n 5
    sender.barrier().await.unwrap();
    shipper
        .ship_through(5, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.shipped(), vec![2, 4, 1]);
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// An attempt whose step did not commit is never shipped, to any store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_attempt_is_never_shipped() {
    let dir = temp_dir("ship-aborted");
    let spool = AuditSpool::open(config(&dir)).unwrap();
    let store = RecordingStore::named("jsonl");
    let shipper = spawn_audit_shipper(&handle(), &spool, store.clone(), shipper_config());
    let sender = spool.sender();
    sender.append(attempt_event("i-a", 1, "lost"));
    sender.append(attempt_event("i-a", 2, "lost"));
    sender.append(open_attempt("i-a", "lost", 2).marker(false));
    sender.append(attempt_event("i-a", 1, "rerun"));
    sender.append(open_attempt("i-a", "rerun", 1).marker(true));
    sender.barrier().await.unwrap();
    shipper
        .ship_through(5, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(store.shipped(), vec![4], "only the committed re-run");
    assert_eq!(spool.metrics().attempts_aborted(), 2);
    shipper.shutdown(Duration::from_secs(1)).await;
    spool.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
