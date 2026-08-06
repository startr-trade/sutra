//! File-spool inbound round-trip — the pure-`fs` conformance for the file transport (NO docker,
//! NO network): drop a file in a spool directory, wire the source with a capturing intake that
//! acks, and assert the bytes are delivered and the file is moved into `.done/`.
//!
//! Mirrors the broker crates' `broker_rewire_conformance` (a real multi-thread runtime + a
//! capturing [`InboundIntake`]) but needs no container — a filesystem is always present, so this
//! is NOT `#[ignore = "docker"]`. Determinism comes from a short poll interval + a bounded
//! wait-loop rather than fixed sleeps.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sutra_channels::source::{DeferredSettle, DeliveryDisposition};
use sutra_channels::{
    AckDecision, BoxFuture, ChannelBinding, ChannelDefinition, DeferredAckRegistry, DeploymentId,
    InboundIntake, InboundMessage, Namespace,
};
use sutra_transport_file::spawn_file_channels_with_intake;

/// Captures every delivered body and acks (so files land in `.done/`, not requeued).
#[derive(Default)]
struct CapturingIntake {
    delivered: Mutex<Vec<Vec<u8>>>,
}

impl CapturingIntake {
    fn count(&self) -> usize {
        self.delivered.lock().expect("intake").len()
    }
    fn bodies(&self) -> Vec<Vec<u8>> {
        self.delivered.lock().expect("intake").clone()
    }
}

impl InboundIntake for CapturingIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            self.delivered
                .lock()
                .expect("intake")
                .push(message.body.into_inner());
            AckDecision::Ack
        })
    }
}

/// A `transport: file` inbound channel definition pointing at `spool_dir`, polling fast (50ms).
fn file_def(channel: &str, spool_dir: &str) -> ChannelDefinition {
    file_def_with(channel, spool_dir, &[])
}

/// [`file_def`] plus extra channel properties (`ack-mode`, …).
fn file_def_with(channel: &str, spool_dir: &str, extra: &[(&str, &str)]) -> ChannelDefinition {
    let mut properties = BTreeMap::new();
    properties.insert("spool.dir".to_string(), spool_dir.to_string());
    properties.insert("poll.interval.ms".to_string(), "50".to_string());
    for (key, value) in extra {
        properties.insert(key.to_string(), value.to_string());
    }
    ChannelDefinition {
        binding: ChannelBinding::new(
            channel,
            Namespace::new("acme", "orders", "v1"),
            DeploymentId::unresolved(),
            "",
        ),
        transport: Some("file".to_string()),
        bind_spec: None,
        codec: None,
        cloud_events_mode: None,
        auth_scheme: None,
        idempotency_key_header: None,
        payload_cap_bytes: None,
        properties,
    }
}

fn unique_spool() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "sutra-file-spool-it-{}-{nanos}",
        std::process::id()
    ));
    dir
}

/// Poll `cond` until it holds or the timeout elapses.
fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    cond()
}

#[test]
fn file_spool_inbound_round_trip_delivers_bytes_and_moves_file_to_done() {
    let spool = unique_spool();
    std::fs::create_dir_all(&spool).expect("create spool dir");

    // Drop one file into the spool root.
    let body = b"spooled-payload".to_vec();
    std::fs::write(spool.join("evt-1.dat"), &body).expect("drop spool file");

    // A real multi-thread runtime drives the detached poll loop (no docker, no network).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let handle = rt.handle().clone();
    let intake = Arc::new(CapturingIntake::default());
    let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

    let channels = spawn_file_channels_with_intake(
        &[file_def("spool-in", &spool.to_string_lossy())],
        dyn_intake,
        None,                  // no pool ⇒ AlwaysLeading (polls on this replica)
        |r| Ok(r.to_string()), // passthrough resolver (unused by the file transport)
        handle.clone(),
    )
    .expect("wire file channel");
    assert_eq!(channels.consumer_count(), 1);

    // The bytes are delivered to the intake within the timeout.
    assert!(
        wait_until(|| intake.count() >= 1, Duration::from_secs(5)),
        "the intake must receive the spooled file's bytes"
    );
    assert_eq!(
        intake.bodies(),
        vec![body],
        "delivered body is the file bytes"
    );

    // The acked file is moved into `.done/` and no longer sits in the spool root.
    let done = spool.join(".done").join("evt-1.dat");
    assert!(
        wait_until(|| done_exists(&done), Duration::from_secs(5)),
        "the acked file must move into .done/"
    );
    assert!(
        !spool.join("evt-1.dat").exists(),
        "the file is no longer in the spool root"
    );

    rt.block_on(channels.drain());
    let _ = std::fs::remove_dir_all(&spool);
}

fn done_exists(done: &Path) -> bool {
    done.exists()
}

// ---- deferred acking (`ack-mode: on-complete`) -----------------------------------------------
//
// The transport-side half of the on-complete contract: the poll loop hands its per-delivery
// terminal-move callbacks through `InboundIntake::deliver_deferred`, and the claimed file only
// leaves `.processing/` when the instance's terminal event settles the `DeferredAckRegistry`
// entry (the engine-side half — dispatch parks → registry → listener bus — is
// `sutra-channels/tests/all/deferred_ack_test.rs`).

/// The engine-actor stand-in for the on-complete seam: registers each delivery's settle
/// callbacks on a REAL [`DeferredAckRegistry`] under a synthetic instance id (exactly what the
/// dispatcher's park arm does) and answers `Deferred`; the test fires the terminal event by hand.
struct DeferringIntake {
    registry: Arc<DeferredAckRegistry>,
    instances: Mutex<Vec<String>>,
    plain_deliveries: Mutex<usize>,
}

impl DeferringIntake {
    fn new(registry: Arc<DeferredAckRegistry>) -> Arc<DeferringIntake> {
        Arc::new(DeferringIntake {
            registry,
            instances: Mutex::new(Vec::new()),
            plain_deliveries: Mutex::new(0),
        })
    }

    fn instance_count(&self) -> usize {
        self.instances.lock().expect("intake").len()
    }

    fn instance_at(&self, index: usize) -> String {
        self.instances.lock().expect("intake")[index].clone()
    }

    fn plain_count(&self) -> usize {
        *self.plain_deliveries.lock().expect("intake")
    }
}

impl InboundIntake for DeferringIntake {
    fn deliver(&self, _message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            *self.plain_deliveries.lock().expect("intake") += 1;
            AckDecision::Ack
        })
    }

    fn deliver_deferred(
        &self,
        message: InboundMessage,
        settle: DeferredSettle,
    ) -> BoxFuture<'_, DeliveryDisposition> {
        Box::pin(async move {
            let instance_id = format!("inst-{}", message.idempotency_key);
            assert!(self.registry.register(
                &instance_id,
                &message.channel,
                settle.ack,
                settle.nack
            ));
            self.instances.lock().expect("intake").push(instance_id);
            DeliveryDisposition::Deferred
        })
    }
}

/// One on-complete spool wired on a fresh temp dir + a real multi-thread runtime.
struct DeferredFixture {
    spool: PathBuf,
    rt: tokio::runtime::Runtime,
    registry: Arc<DeferredAckRegistry>,
    intake: Arc<DeferringIntake>,
    channels: sutra_transport_file::FileChannels,
}

impl DeferredFixture {
    fn start(file_name: &str, body: &[u8], extra: &[(&str, &str)]) -> DeferredFixture {
        let spool = unique_spool();
        std::fs::create_dir_all(&spool).expect("create spool dir");
        std::fs::write(spool.join(file_name), body).expect("drop spool file");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
        let intake = DeferringIntake::new(Arc::clone(&registry));
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();
        let channels = spawn_file_channels_with_intake(
            &[file_def_with("spool-in", &spool.to_string_lossy(), extra)],
            dyn_intake,
            None,
            |r| Ok(r.to_string()),
            rt.handle().clone(),
        )
        .expect("wire file channel");
        DeferredFixture {
            spool,
            rt,
            registry,
            intake,
            channels,
        }
    }

    fn staged(&self, name: &str) -> PathBuf {
        self.spool.join(".processing").join(name)
    }

    fn done(&self, name: &str) -> PathBuf {
        self.spool.join(".done").join(name)
    }

    fn failed(&self, name: &str) -> PathBuf {
        self.spool.join(".failed").join(name)
    }

    /// Wait for the poll loop to deliver + defer the spooled file.
    fn await_deferred(&self) {
        assert!(
            wait_until(|| self.intake.instance_count() >= 1, Duration::from_secs(5)),
            "the spooled file must be delivered through deliver_deferred"
        );
    }
}

impl Drop for DeferredFixture {
    fn drop(&mut self) {
        self.rt.block_on(self.channels.drain());
        let _ = std::fs::remove_dir_all(&self.spool);
    }
}

#[test]
fn on_complete_holds_the_file_staged_until_the_instance_completes() {
    // file in → terminal move DEFERRED (the file waits in `.processing/`) → instance completes
    // → the deferred ack moves it into `.done/`.
    let fixture = DeferredFixture::start(
        "evt-park.dat",
        b"parked-payload",
        &[("ack-mode", "on-complete")],
    );
    fixture.await_deferred();
    assert_eq!(
        fixture.registry.pending_count(),
        1,
        "the settle is REGISTERED, not fired"
    );
    assert_eq!(
        fixture.intake.plain_count(),
        0,
        "an on-complete channel routes through deliver_deferred, never plain deliver"
    );

    // HELD: the file sits staged, with neither terminal move taken — and several poll ticks
    // later it is still neither moved nor re-delivered (`.processing/` is never re-listed).
    assert!(
        fixture.staged("evt-park.dat").exists(),
        "the file waits in .processing/"
    );
    assert!(!fixture.done("evt-park.dat").exists());
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        fixture.intake.instance_count(),
        1,
        "a staged file is never re-listed"
    );
    assert!(!fixture.done("evt-park.dat").exists());
    assert!(!fixture.failed("evt-park.dat").exists());

    // The instance's terminal event fires the held move (spawned onto the runtime).
    fixture
        .registry
        .on_instance_completed(&fixture.intake.instance_at(0));
    assert_eq!(fixture.registry.pending_count(), 0);
    assert!(
        wait_until(
            || fixture.done("evt-park.dat").exists(),
            Duration::from_secs(5)
        ),
        "the deferred ack must move the staged file into .done/"
    );
    assert!(
        !fixture.staged("evt-park.dat").exists(),
        "it left .processing/"
    );
    assert_eq!(
        std::fs::read(fixture.done("evt-park.dat")).expect("read done file"),
        b"parked-payload",
        "the moved file is the delivered payload, byte for byte"
    );
}

#[test]
fn on_complete_moves_the_file_to_failed_when_the_instance_fails() {
    // failure path: instance FAILS → the deferred nack takes the DROP posture (`.failed/`),
    // never a requeue back to the spool root — a permanent reject can't be retried.
    let fixture = DeferredFixture::start(
        "evt-doomed.dat",
        b"doomed-payload",
        &[("ack-mode", "on-complete")],
    );
    fixture.await_deferred();
    assert_eq!(fixture.registry.pending_count(), 1);

    fixture
        .registry
        .on_instance_failed(&fixture.intake.instance_at(0));
    assert_eq!(fixture.registry.pending_count(), 0);
    assert!(
        wait_until(
            || fixture.failed("evt-doomed.dat").exists(),
            Duration::from_secs(5)
        ),
        "the deferred nack must move the staged file into .failed/"
    );
    assert!(!fixture.staged("evt-doomed.dat").exists());
    assert!(!fixture.done("evt-doomed.dat").exists());
    assert!(
        !fixture.spool.join("evt-doomed.dat").exists(),
        "a drop-posture nack never requeues the file to the spool root"
    );
    // Nothing is re-delivered afterwards either (the file left the scanned root for good).
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(fixture.intake.instance_count(), 1);
}

#[test]
fn on_persist_channel_still_moves_the_file_at_dispatch_return() {
    // Regression pin for the untouched path: without `ack-mode: on-complete` the loop keeps
    // calling plain `deliver` and moves the file at dispatch-return — no registry involvement.
    let fixture = DeferredFixture::start("evt-classic.dat", b"classic", &[]);
    assert!(
        wait_until(|| fixture.intake.plain_count() >= 1, Duration::from_secs(5)),
        "an on-persist channel delivers through plain deliver"
    );
    assert_eq!(
        fixture.intake.instance_count(),
        0,
        "deliver_deferred never called"
    );
    assert_eq!(fixture.registry.pending_count(), 0);
    assert!(
        wait_until(
            || fixture.done("evt-classic.dat").exists(),
            Duration::from_secs(5)
        ),
        "the ack at dispatch-return moved the file into .done/"
    );
}

#[test]
fn a_staged_file_left_by_a_crash_is_not_redelivered_on_restart() {
    // HONEST crash-behaviour pin (documented in `file::source`): a file staged for a deferred
    // settle that never fired stays in `.processing/`. A restart does NOT rescan it — the poll
    // loop lists the spool ROOT and skips dotfiles/dirs — so recovering it is an operator move
    // back to the root (redelivery then rides the file NAME through inbox dedup).
    let fixture = DeferredFixture::start(
        "evt-orphan.dat",
        b"orphaned",
        &[("ack-mode", "on-complete")],
    );
    fixture.await_deferred();
    let staged = fixture.staged("evt-orphan.dat");
    assert!(staged.exists());

    // "Crash": stop the poll loop with the instance still parked, then wire a FRESH channel
    // set (a new process would) over the same spool.
    fixture.rt.block_on(fixture.channels.drain());
    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let restarted_intake = DeferringIntake::new(Arc::clone(&registry));
    let dyn_intake: Arc<dyn InboundIntake> = restarted_intake.clone();
    let restarted = spawn_file_channels_with_intake(
        &[file_def_with(
            "spool-in",
            &fixture.spool.to_string_lossy(),
            &[("ack-mode", "on-complete")],
        )],
        dyn_intake,
        None,
        |r| Ok(r.to_string()),
        fixture.rt.handle().clone(),
    )
    .expect("re-wire file channel");

    std::thread::sleep(Duration::from_millis(400)); // several poll ticks
    assert_eq!(
        restarted_intake.instance_count() + restarted_intake.plain_count(),
        0,
        "a leftover staged file is NOT rescanned or redelivered on restart"
    );
    assert!(
        staged.exists(),
        "it just sits in .processing/ awaiting operator recovery"
    );

    // The operator recovery: move it back to the spool root and the restarted loop picks it up.
    std::fs::rename(&staged, fixture.spool.join("evt-orphan.dat")).expect("operator recovery");
    assert!(
        wait_until(
            || restarted_intake.instance_count() >= 1,
            Duration::from_secs(5)
        ),
        "a file moved back to the spool root is delivered again (same NAME ⇒ inbox dedup)"
    );
    fixture.rt.block_on(restarted.drain());
}

#[test]
fn a_late_settle_with_no_staged_file_left_is_a_tolerated_no_op() {
    // Late settle after the loop stopped AND the staged file vanished (operator swept
    // `.processing/`): the callback must WARN + no-op, never panic and never resurrect a file.
    let fixture = DeferredFixture::start(
        "evt-vanished.dat",
        b"vanished",
        &[("ack-mode", "on-complete")],
    );
    fixture.await_deferred();
    fixture.rt.block_on(fixture.channels.drain());
    std::fs::remove_file(fixture.staged("evt-vanished.dat")).expect("sweep .processing/");

    fixture
        .registry
        .on_instance_completed(&fixture.intake.instance_at(0));
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !fixture.done("evt-vanished.dat").exists(),
        "nothing to move ⇒ nothing appears in .done/"
    );
    assert!(!fixture.failed("evt-vanished.dat").exists());
}
