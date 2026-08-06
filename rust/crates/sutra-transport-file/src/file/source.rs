//! The file inbound trigger — [`FileTriggerSource`] implements [`TriggerSource`]: one
//! background poll loop per channel binding lists the spool directory on a cadence and
//! projects each regular file into an [`InboundMessage`], pushing it through the
//! [`InboundIntake`] seam and executing the returned [`AckDecision`] as a file move.
//!
//! The lifecycle mirrors the broker sources, minus the network:
//!
//! - **Spool-dir absence is NON-FATAL**: `start` spawns the poll loop and
//!   resolves immediately; a missing spool directory WARNs
//!   ([`codes::INBOUND_READ_FAILED`]) and the next poll retries — readiness never blocks on it.
//! - **Ack timing rides the intake seam**: the loop `await`s [`InboundIntake::deliver`]
//!   and maps the decision onto file moves — `Ack` → move into `.done/`, `NackDrop` → move into
//!   `.failed/`, `NackRequeue` → move the file back out to the spool root so a later poll
//!   re-reads it (redelivery; inbox dedup absorbs the duplicate).
//! - **`ack-mode: on-complete` defers the terminal move**: the loop hands the engine
//!   per-delivery settle callbacks through [`InboundIntake::deliver_deferred`]; a PARKED
//!   instance answers `Deferred` and the claimed file simply STAYS in `.processing/` while the
//!   instance runs — the move into `.done/` (COMPLETED) or `.failed/` (FAILED, and registry
//!   timeout/overflow) happens inside the callback at the terminal event.
//!   **Crash behaviour (honest statement)**: a staged file whose terminal move never ran is
//!   NOT re-delivered on restart. The poll loop lists only the spool ROOT and skips dotfiles
//!   and directories, so `.processing/` is never rescanned; the file sits there until an
//!   operator moves it back to the spool root (redelivery then rides the file NAME through
//!   inbox dedup). That is the same window `on-persist` has between deliver and move — just
//!   wider, because it now spans the instance's whole lifetime.
//! - **Claim-before-deliver**: a file is FIRST renamed into `.processing/`, then read + delivered,
//!   then moved to its terminal subdir. So a concurrent poll (or an overlapping tick while a
//!   deliver is in flight) can never double-process the same file.
//! - **Singleton gating**: the loop polls ONLY while `gate.is_leading()`. The engine per-channel
//!   lease — not the filesystem — is what makes a `singleton: true` spool drain on exactly one
//!   replica.
//!
//! Idempotency key: the FILE NAME, marked EXPLICIT (`explicit_event_id = true`) so inbox dedup
//! treats a redelivered file (same name) as the same event.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tracing::{debug, warn};

use sutra_channels::diag::Diagnostic;
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::sink::BoxFuture;
use sutra_channels::source::{
    AckDecision, DeferredSettle, DeliveryDisposition, InboundIntake, LeaderGate, TriggerSource,
};

use super::{
    codes, AckMode, FileChannelProperties, SUBDIR_DONE, SUBDIR_FAILED, SUBDIR_PROCESSING, TRANSPORT,
};

/// Everything one poll loop needs, prepared by the wiring.
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// The serving binding's tenant (rides every [`InboundMessage`]).
    pub tenant: String,
    /// The serving binding's `"<tenant>/<module>/<version>"` namespace key.
    pub module_key: String,
    /// The channel name (lease-role suffix + diagnostics).
    pub channel: String,
    /// Spool-dir / poll-interval / singleton properties.
    pub properties: FileChannelProperties,
    /// The runtime the poll loop detaches onto (the file transport spawns via a stored handle
    /// rather than the ambient `tokio::spawn`, so wiring works whatever context `start` is
    /// called from).
    pub handle: Handle,
}

impl FileSourceConfig {
    /// A config for one channel binding.
    pub fn new(
        tenant: &str,
        module_key: &str,
        channel: &str,
        properties: FileChannelProperties,
        handle: Handle,
    ) -> FileSourceConfig {
        FileSourceConfig {
            tenant: tenant.to_string(),
            module_key: module_key.to_string(),
            channel: channel.to_string(),
            properties,
            handle,
        }
    }
}

/// One spool poll loop serving one channel binding (the singleton unit).
pub struct FileTriggerSource {
    config: FileSourceConfig,
    running: tokio::sync::Mutex<Option<Running>>,
}

struct Running {
    task: tokio::task::JoinHandle<()>,
    stop: Arc<StopFlag>,
}

/// Cooperative stop signal shared with the poll loop — the `closed` flag is the source of
/// truth; the `notify` just wakes an in-progress sleep early so `stop` is prompt.
struct StopFlag {
    closed: AtomicBool,
    notify: tokio::sync::Notify,
}

impl StopFlag {
    fn new() -> StopFlag {
        StopFlag {
            closed: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn request(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_requested(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Sleep that wakes early on stop; returns true when stop was requested.
    async fn sleep(&self, duration: Duration) -> bool {
        if self.is_requested() {
            return true;
        }
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = tokio::time::sleep(duration) => {}
        }
        self.is_requested()
    }
}

impl FileTriggerSource {
    /// A source for one channel binding. The spool dir is validated present at property-parse
    /// time ([`FileChannelProperties::from_definition`]); this only guards the degenerate empty
    /// path so a mis-built config never spins a loop against `""`.
    pub fn new(config: FileSourceConfig) -> Result<FileTriggerSource, Diagnostic> {
        if !config.properties.has_spool_dir() {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "file channel '{}' requires a spool directory",
                    config.channel
                ),
            ));
        }
        Ok(FileTriggerSource {
            config,
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// The configured spool directory (diagnostics / tests).
    pub fn spool_dir(&self) -> &Path {
        &self.config.properties.spool_dir
    }
}

impl TriggerSource for FileTriggerSource {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    fn channel(&self) -> &str {
        &self.config.channel
    }

    fn start(
        &self,
        intake: Arc<dyn InboundIntake>,
        gate: Arc<dyn LeaderGate>,
    ) -> BoxFuture<'_, Result<(), Diagnostic>> {
        Box::pin(async move {
            let mut running = self.running.lock().await;
            if running.is_some() {
                return Ok(()); // idempotent — the poll loop is already up
            }
            let stop = Arc::new(StopFlag::new());
            // Detach onto the stored runtime handle and resolve immediately — a missing spool
            // dir is NON-FATAL (the loop WARNs + retries), never a boot failure.
            let task = self.config.handle.spawn(poll_loop(
                self.config.clone(),
                intake,
                gate,
                Arc::clone(&stop),
            ));
            *running = Some(Running { task, stop });
            Ok(())
        })
    }

    fn stop(&self) -> BoxFuture<'_, Result<(), Diagnostic>> {
        Box::pin(async move {
            let taken = { self.running.lock().await.take() };
            let Some(Running { task, stop }) = taken else {
                return Ok(()); // idempotent
            };
            stop.request();
            if let Err(e) = task.await {
                warn!(
                    channel = %self.config.channel,
                    error = %e,
                    "file source poll loop did not shut down cleanly"
                );
            }
            Ok(())
        })
    }
}

/// The poll loop: leadership-gated `list spool dir → per-file claim → deliver → move` on a
/// cadence. A read failure (spool dir absent, transient IO) never escapes — it WARNs and the
/// next tick retries.
async fn poll_loop(
    config: FileSourceConfig,
    intake: Arc<dyn InboundIntake>,
    gate: Arc<dyn LeaderGate>,
    stop: Arc<StopFlag>,
) {
    loop {
        if stop.is_requested() {
            return;
        }
        if !gate.is_leading() {
            // Not (or no longer) the leader — drain nothing and re-check next tick.
            if stop.sleep(config.properties.poll_interval).await {
                return;
            }
            continue;
        }
        if let Err(diagnostic) = poll_once(&config, &intake, &gate, &stop).await {
            // NON-FATAL: a missing spool dir / transient IO WARNs and the next poll retries.
            warn!(
                channel = %config.channel,
                code = %diagnostic.code,
                "file spool poll skipped: {}",
                diagnostic.message
            );
        }
        if stop.sleep(config.properties.poll_interval).await {
            return;
        }
    }
}

/// One poll turn — list the spool root and process each regular file, re-checking stop +
/// leadership between files (so a leadership loss / stop mid-drain is prompt).
async fn poll_once(
    config: &FileSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    gate: &Arc<dyn LeaderGate>,
    stop: &Arc<StopFlag>,
) -> Result<(), Diagnostic> {
    let spool = &config.properties.spool_dir;
    let mut entries = tokio::fs::read_dir(spool).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_READ_FAILED,
            format!(
                "file channel '{}' cannot read spool dir '{}': {e}",
                config.channel,
                spool.display()
            ),
        )
    })?;
    loop {
        if stop.is_requested() || !gate.is_leading() {
            return Ok(());
        }
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => return Ok(()),
            Err(e) => {
                return Err(Diagnostic::error(
                    codes::INBOUND_READ_FAILED,
                    format!(
                        "file channel '{}' failed to iterate spool dir '{}': {e}",
                        config.channel,
                        spool.display()
                    ),
                ))
            }
        };
        let Some(name) = spoolable_name(&entry).await else {
            continue; // subdir / dotfile (incl. the .processing/.done/.failed control dirs)
        };
        let path = entry.path();
        if let Err(diagnostic) = process_file(config, intake, &path, &name).await {
            // One bad file never kills the loop — WARN and move on; the file stays put and is
            // retried (or, if it never claims, remains for an operator to inspect).
            warn!(
                channel = %config.channel,
                code = %diagnostic.code,
                file = %name,
                "file spool entry skipped: {}",
                diagnostic.message
            );
        }
    }
}

/// The name of `entry` if it is a spoolable regular file — `None` for directories (incl. the
/// `.processing`/`.done`/`.failed` control dirs) and dotfiles (partial-write / hidden markers).
async fn spoolable_name(entry: &tokio::fs::DirEntry) -> Option<String> {
    let name = entry.file_name().to_string_lossy().into_owned();
    if name.starts_with('.') {
        return None;
    }
    let file_type = entry.file_type().await.ok()?;
    if !file_type.is_file() {
        return None;
    }
    Some(name)
}

/// Claim → read → deliver → settle one spool file. The file is renamed into `.processing/`
/// FIRST (the claim); losing that rename means another poll already claimed it (or it vanished),
/// which is a clean no-op. After delivery the [`AckDecision`] decides the terminal move.
async fn process_file(
    config: &FileSourceConfig,
    intake: &Arc<dyn InboundIntake>,
    path: &Path,
    name: &str,
) -> Result<(), Diagnostic> {
    let spool = &config.properties.spool_dir;

    // 1. Claim: stage into `.processing/` so a concurrent poll can never double-deliver while
    //    the deliver is in flight.
    let processing_dir = spool.join(SUBDIR_PROCESSING);
    ensure_dir(&processing_dir).await?;
    let staged = processing_dir.join(name);
    if tokio::fs::rename(path, &staged).await.is_err() {
        // Lost the race (another poll claimed it) or the file vanished — nothing to do.
        return Ok(());
    }

    // 2. Read the claimed bytes.
    let body = tokio::fs::read(&staged).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_READ_FAILED,
            format!(
                "file channel '{}' could not read staged file '{}': {e}",
                config.channel,
                staged.display()
            ),
        )
    })?;

    // 3. Project + deliver; the intake owns ack-mode TIMING. Under `on-complete` the
    //    terminal move rides settle callbacks instead: a PARKED instance answers `Deferred`
    //    and the claimed file stays staged in `.processing/` until its terminal event.
    let message = to_inbound_message(config, name, body);
    let decision = if config.properties.ack_mode == AckMode::OnComplete {
        let settle = deferred_settle(config, &staged, name);
        match intake.deliver_deferred(message, settle).await {
            DeliveryDisposition::Deferred => {
                debug!(
                    channel = %config.channel,
                    file = %name,
                    "spool file deferred — it stays staged in .processing/ until the \
                     instance's terminal event"
                );
                return Ok(());
            }
            DeliveryDisposition::Settle(decision) => decision,
        }
    } else {
        intake.deliver(message).await
    };

    // 4. Settle the file per the decision.
    match decision {
        AckDecision::Ack => move_into(&staged, spool, SUBDIR_DONE, name).await,
        AckDecision::NackDrop => move_into(&staged, spool, SUBDIR_FAILED, name).await,
        AckDecision::NackRequeue => {
            // Leave it for a later poll: move it back out to the spool root (redelivery; inbox
            // dedup absorbs the duplicate on the file NAME).
            let back = spool.join(name);
            tokio::fs::rename(&staged, &back).await.map_err(|e| {
                Diagnostic::error(
                    codes::INBOUND_READ_FAILED,
                    format!(
                        "file channel '{}' could not requeue '{}' to the spool root: {e}",
                        config.channel,
                        staged.display()
                    ),
                )
            })
        }
    }
}

/// Build the per-delivery settle callbacks for `ack-mode: on-complete` — the deferred half of
/// the ack mapping, the SAME terminal moves the inline path performs, just executed later:
/// ack → `.done/` (instance COMPLETED), nack → `.failed/` (instance FAILED is a permanent
/// reject — the drop posture; registry timeout/overflow nacks share it). There is no deferred
/// requeue: the registry only ever fires one of these two.
///
/// The callbacks fire on the engine actor thread (or the registry sweep) — non-async contexts
/// that must never block — so each spawns its move onto the source's stored runtime handle.
/// Everything they capture is owned + `Send` (paths, names, the handle); `armed` makes them
/// IDEMPOTENT, so a repeat call can never move a file twice.
fn deferred_settle(config: &FileSourceConfig, staged: &Path, name: &str) -> DeferredSettle {
    DeferredSettle {
        ack: settle_callback(config, staged, name, SUBDIR_DONE, "ack"),
        nack: settle_callback(config, staged, name, SUBDIR_FAILED, "nack"),
    }
}

/// One deferred settle callback — spawn the terminal move of the staged file into `subdir`.
fn settle_callback(
    config: &FileSourceConfig,
    staged: &Path,
    name: &str,
    subdir: &'static str,
    label: &'static str,
) -> Box<dyn FnMut() + Send> {
    let channel = config.channel.clone();
    let spool = config.properties.spool_dir.clone();
    let handle = config.handle.clone();
    let staged = staged.to_path_buf();
    let name = name.to_string();
    let mut armed = true;
    Box::new(move || {
        if !armed {
            return; // already settled — idempotent no-op
        }
        armed = false;
        let (channel, spool, staged, name) =
            (channel.clone(), spool.clone(), staged.clone(), name.clone());
        handle.spawn(async move {
            settle_staged(&channel, &staged, &spool, subdir, &name, label).await;
        });
    })
}

/// Execute a DEFERRED terminal move. Tolerant of a vanished staged file (`NotFound`): a late
/// settle — the poll loop stopped, the channel was rewired away, or an operator swept
/// `.processing/` — has nothing left to move, which is a WARN no-op, never a failure. A move
/// that fails for any other reason leaves the file staged for operator recovery.
async fn settle_staged(
    channel: &str,
    staged: &Path,
    spool: &Path,
    subdir: &'static str,
    name: &str,
    label: &'static str,
) {
    let dir = spool.join(subdir);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        warn!(
            channel = %channel,
            code = codes::INBOUND_READ_FAILED,
            file = %name,
            error = %e,
            "deferred {label} could not create {subdir}/ — '{}' stays staged in {SUBDIR_PROCESSING}/",
            staged.display()
        );
        return;
    }
    match tokio::fs::rename(staged, dir.join(name)).await {
        Ok(()) => debug!(
            channel = %channel,
            file = %name,
            "deferred {label} moved the staged file into {subdir}/"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => warn!(
            channel = %channel,
            code = codes::INBOUND_READ_FAILED,
            file = %name,
            "deferred {label} found nothing to move — '{}' is already gone (late settle after \
             the poll loop stopped, or {SUBDIR_PROCESSING}/ was swept)",
            staged.display()
        ),
        Err(e) => warn!(
            channel = %channel,
            code = codes::INBOUND_READ_FAILED,
            file = %name,
            error = %e,
            "deferred {label} could not move '{}' into {subdir}/ — it stays staged in \
             {SUBDIR_PROCESSING}/ for operator recovery",
            staged.display()
        ),
    }
}

/// Move `from` into `<spool>/<subdir>/<name>` (creating the subdir if absent).
async fn move_into(from: &Path, spool: &Path, subdir: &str, name: &str) -> Result<(), Diagnostic> {
    let dir = spool.join(subdir);
    ensure_dir(&dir).await?;
    let dest = dir.join(name);
    tokio::fs::rename(from, &dest).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_READ_FAILED,
            format!("could not move '{}' into {subdir}/: {e}", from.display()),
        )
    })
}

/// Create a directory (and parents) if it does not already exist.
async fn ensure_dir(dir: &Path) -> Result<(), Diagnostic> {
    tokio::fs::create_dir_all(dir).await.map_err(|e| {
        Diagnostic::error(
            codes::INBOUND_READ_FAILED,
            format!("could not create '{}': {e}", dir.display()),
        )
    })
}

/// Project one spool file into the engine's [`InboundMessage`]: the FILE NAME is the EXPLICIT
/// idempotency key, the bytes are the body, content type is unknown (`None`), no headers, no CE.
fn to_inbound_message(config: &FileSourceConfig, name: &str, body: Vec<u8>) -> InboundMessage {
    InboundMessage {
        tenant: config.tenant.clone(),
        module_key: config.module_key.clone(),
        channel: config.channel.clone(),
        headers: BTreeMap::new(),
        body: body.into(),
        content_type: None,
        idempotency_key: name.to_string(),
        explicit_event_id: true,
        received_at: now_rfc3339(),
        cloud_event: None,
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn config() -> FileSourceConfig {
        let props = FileChannelProperties {
            spool_dir: PathBuf::from("/var/spool/in"),
            poll_interval: Duration::from_millis(50),
            singleton: true,
            ack_mode: AckMode::OnPersist,
        };
        FileSourceConfig::new(
            "acme",
            "acme/orders/v1",
            "spool-in",
            props,
            Handle::current(),
        )
    }

    #[tokio::test]
    async fn file_name_is_the_explicit_idempotency_key() {
        let m = to_inbound_message(&config(), "evt-42.dat", b"payload".to_vec());
        assert_eq!(m.idempotency_key, "evt-42.dat");
        assert!(m.explicit_event_id, "the file name is an explicit event id");
        assert_eq!(m.body.into_inner(), b"payload");
        assert_eq!(m.content_type, None);
        assert_eq!(m.tenant, "acme");
        assert_eq!(m.module_key, "acme/orders/v1");
        assert_eq!(m.channel, "spool-in");
        assert!(m.headers.is_empty());
        assert!(m.cloud_event.is_none());
        assert!(!m.received_at.is_empty());
    }

    #[test]
    fn source_without_spool_dir_fails_closed() {
        let props = FileChannelProperties {
            spool_dir: PathBuf::new(),
            poll_interval: Duration::from_millis(50),
            singleton: true,
            ack_mode: AckMode::OnPersist,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let cfg = FileSourceConfig::new("acme", "acme/m/1", "ch", props, rt.handle().clone());
        let err = match FileTriggerSource::new(cfg) {
            Err(diagnostic) => diagnostic,
            Ok(_) => panic!("a source without a spool dir must be refused"),
        };
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }
}
