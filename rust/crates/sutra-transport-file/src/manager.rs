//! File channel wiring — the assembly touch-point for `transport: file` spool consumers: for
//! every inbound `transport: file` channel definition this constructs a [`FileTriggerSource`],
//! gates it (`singleton: true` — the file DEFAULT — ⇒ a
//! [`sutra_transport_spi::leadership::DbLeaderElection`] lease role
//! `sutra-channel:<tenant>:<channel>`; otherwise / no datasource ⇒ always-leading), and starts
//! it against an [`InboundIntake`] adapter over the engine actor.
//!
//! Mirrors the vendor broker managers exactly: assembly calls [`spawn_file_channels`] once
//! and moves on; the poll loops detach onto the runtime (spool-dir absence is NON-FATAL), and
//! the shared, rewireable [`FileChannels`] rides the activation flip via
//! [`FileChannels::rewire`] (stop changed/removed, start added, keep unchanged; idempotent;
//! non-fatal on a bad new def; drain on shutdown). There are no channel-YAML credentials (a
//! filesystem authenticates at the OS layer), so the credential-resolution step is absent — the
//! `resolver` rides the uniform factory signature but is unused here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, InboundIntake, SinkRegistry, TriggerSource};
use sutra_persistence::stores::PgLeaseStore;
use sutra_transport_spi::leadership::{
    channel_role, ChannelLeadership, DbLeaderElection, PgLeaseHandle,
};
use sutra_transport_spi::{EngineIntake, EnvRefResolver, TransportChannels, TransportFactory};
use tokio::runtime::Handle;
use tracing::{info, warn};

use crate::file::{
    register_file_sink, FileChannelProperties, FileSourceConfig, FileTriggerSource, TRANSPORT,
};

/// The wired file consumers + everything needed to re-start them. SHARED (`Arc`) between the
/// engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`FileChannels::rewire`] reconciles the running poll loops to a new active definition set,
/// stopping loops whose definition changed (or was removed) and starting the new ones —
/// unchanged loops keep running. `stop` is drain-postured and file at-least-once + inbox dedup
/// absorb any redelivery over the brief handover, so a flip loses no files.
#[derive(Clone, Default)]
pub struct FileChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one poll loop across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<FileTriggerSource>,
    /// The authored-config fingerprint used to detect a changed definition on flip
    /// (`FileChannelProperties: Eq`).
    fingerprint: FileChannelProperties,
}

struct BrokerState {
    running: Mutex<HashMap<ConsumerKey, RunningConsumer>>,
    /// The shared singleton-role election, when one was constructed at boot (datasource present
    /// AND at least one boot `singleton` channel — the file default).
    election: Option<Arc<DbLeaderElection>>,
    ctx: BrokerRespawnContext,
}

/// Everything a flip needs to (re)start a poll loop after boot.
struct BrokerRespawnContext {
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    /// The `env:`/`secret:`/`vault:` reference resolver, injected by the engine to keep the
    /// factory signature uniform across transports. The file transport reads no secrets, so it
    /// is carried but never consulted.
    #[allow(dead_code)]
    resolver: EnvRefResolver,
    handle: Handle,
}

impl FileChannels {
    /// Number of poll loops currently wired.
    pub fn consumer_count(&self) -> usize {
        self.inner
            .as_ref()
            .map(|s| s.running.lock().expect("file registry").len())
            .unwrap_or(0)
    }

    /// Await-drain every poll loop + release the channel-lease election (async shutdown).
    pub async fn drain(&self) {
        let Some(state) = &self.inner else {
            return;
        };
        for source in state.snapshot_sources() {
            let _ = source.stop().await;
        }
        if let Some(election) = &state.election {
            election.release_all().await;
        }
    }

    /// Detached stop of every poll loop + lease release (the sync `shutdown` path).
    pub fn stop_all_detached(&self, runtime: &Handle) {
        let Some(state) = &self.inner else {
            return;
        };
        for source in state.snapshot_sources() {
            runtime.spawn(async move {
                let _ = source.stop().await;
            });
        }
        if let Some(election) = &state.election {
            let election = Arc::clone(election);
            runtime.spawn(async move { election.release_all().await });
        }
    }

    /// Topology rewire on activation flip: reconcile the running poll loops to the new
    /// ACTIVE `transport: file` inbound definitions. Loops whose definition was removed or
    /// CHANGED stop (drain-postured), added/changed ones start on the new definition; unchanged
    /// ones keep running untouched. Idempotent — a no-change flip is a no-op. Non-fatal: a bad
    /// new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active file inbound definitions. An authored-config
        // error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, FileChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let Ok(props) = FileChannelProperties::from_definition(def) else {
                continue;
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<FileTriggerSource>>, Vec<ChannelDefinition>) = {
            let running = state.running.lock().expect("file registry");
            let mut stop = Vec::new();
            for (key, consumer) in running.iter() {
                let unchanged = desired
                    .get(key)
                    .is_some_and(|(_, props)| *props == consumer.fingerprint);
                if !unchanged {
                    stop.push(Arc::clone(&consumer.source)); // removed or changed
                }
            }
            let mut spawn = Vec::new();
            for (key, (def, props)) in desired.iter() {
                let unchanged = running
                    .get(key)
                    .is_some_and(|consumer| consumer.fingerprint == *props);
                if !unchanged {
                    spawn.push(def.clone()); // added or changed
                }
            }
            (stop, spawn)
        };

        if to_stop.is_empty() && to_spawn.is_empty() {
            return;
        }

        // Stop changed/removed loops first (drain posture: an in-flight deliver settles its file
        // move; at-least-once redelivery + inbox dedup absorb the brief handover gap).
        for source in &to_stop {
            if let Err(d) = source.stop().await {
                warn!(code = %d.code, "file poll loop stop during flip: {}", d.message);
            }
        }
        // Drop the stopped (removed/changed) loops from the registry.
        {
            let mut running = state.running.lock().expect("file registry");
            running.retain(|key, consumer| {
                desired
                    .get(key)
                    .is_some_and(|(_, props)| *props == consumer.fingerprint)
            });
        }
        // Start the added/changed loops on their new definitions.
        for def in &to_spawn {
            match build_and_start_consumer(def, &state.election, &state.ctx) {
                Ok(Some((key, consumer))) => {
                    state
                        .running
                        .lock()
                        .expect("file registry")
                        .insert(key, consumer);
                }
                // The file transport has no credential-skip path (no channel-YAML secrets); the
                // Option keeps the uniform signature so a future gated case slots in here.
                Ok(None) => {}
                Err(d) => warn!(
                    code = %d.code,
                    "file poll loop NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "file spool topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<FileTriggerSource>> {
        self.running
            .lock()
            .expect("file registry")
            .values()
            .map(|c| Arc::clone(&c.source))
            .collect()
    }
}

fn consumer_key(def: &ChannelDefinition) -> ConsumerKey {
    (
        def.binding.tenant().to_string(),
        def.binding.namespace.module_key(),
        def.binding.channel_name.clone(),
    )
}

/// The file transport's singleton posture — parsed from the channel's `singleton` property
/// (default TRUE for a spool, unlike the engine-wide `ChannelDefinition::singleton` default of
/// false). A definition whose properties don't parse can't boot a consumer either, so it reports
/// non-singleton here (no election churn).
fn singleton_of(def: &ChannelDefinition) -> bool {
    FileChannelProperties::from_definition(def)
        .map(|p| p.singleton)
        .unwrap_or(false)
}

/// Construct + start ONE poll loop from its definition (singleton gated), detaching `start` onto
/// the runtime (spool-dir absence stays a background WARN + retry, never a boot failure).
/// Shared by boot and the flip rewire.
///
/// - `Err(diagnostic)` — authored config invalid (missing spool dir, bad poll interval): boot
///   fails CLOSED (rewire WARNs and skips — the running engine never crashes on a bad flip).
/// - `Ok(Some(entry))` — the started loop + its registry key.
fn build_and_start_consumer(
    definition: &ChannelDefinition,
    election: &Option<Arc<DbLeaderElection>>,
    ctx: &BrokerRespawnContext,
) -> Result<Option<(ConsumerKey, RunningConsumer)>, Diagnostic> {
    let channel = definition.binding.channel_name.clone();
    let tenant = definition.binding.tenant().to_string();
    let properties = FileChannelProperties::from_definition(definition)?;
    let fingerprint = properties.clone();

    let singleton = properties.singleton;
    if singleton && ctx.pool.is_none() {
        warn!(
            channel = %channel,
            "singleton file channel has no engine datasource — no lease election is possible; \
             polling on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let source_config = FileSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        properties,
        ctx.handle.clone(),
    );
    let source = Arc::new(FileTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "file poll loop failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "file spool consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
}

/// Construct + start one poll loop per inbound `transport: file` definition, fail-closed on
/// authored errors. Returns the shared, rewireable [`FileChannels`].
pub fn spawn_file_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<FileChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_file_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_file_channels`] — the engine wraps its actor as
/// [`EngineIntake`], conformance/round-trip tests inject a capturing intake. Same boot semantics
/// + the same shared, rewireable state.
///
/// PUBLIC (the kafka analogue is `pub(crate)`): the file transport's inbound round-trip is a
/// pure-`fs` integration test in `tests/`, so it needs this seam to inject a capturing intake.
pub fn spawn_file_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<FileChannels, Diagnostic> {
    let inbound: Vec<&ChannelDefinition> = definitions
        .iter()
        .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        .collect();

    // One election for all singleton roles, only when a datasource exists AND a boot channel is
    // singleton (the file default); the AlwaysLeading fallback is the no-election posture. (v1
    // bound: a flip that introduces the FIRST singleton file channel where none existed at boot
    // falls back to AlwaysLeading for it — the election is boot-scoped.)
    let election: Option<Arc<DbLeaderElection>> = match &pool {
        Some(pool) if inbound.iter().any(|d| singleton_of(d)) => {
            Some(Arc::new(DbLeaderElection::with_defaults(
                Arc::new(PgLeaseHandle(PgLeaseStore::new(pool.clone()))),
                None,
                handle.clone(),
            )))
        }
        _ => None,
    };

    let ctx = BrokerRespawnContext {
        intake,
        pool,
        resolver,
        handle,
    };
    let mut running: HashMap<ConsumerKey, RunningConsumer> = HashMap::new();
    for definition in inbound {
        if let Some((key, consumer)) = build_and_start_consumer(definition, &election, &ctx)? {
            running.insert(key, consumer);
        }
    }
    Ok(FileChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------
// The engine drives every transport through `TransportChannels` + composes them by iterating
// `transport_factories()`; this crate self-registers its factory below.

#[async_trait::async_trait]
impl TransportChannels for FileChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        FileChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        FileChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        FileChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        FileChannels::stop_all_detached(self, runtime)
    }
    // `inbound_router` takes the default `None` — the file transport dials the filesystem, it
    // does not receive over the engine's shared HTTP listener.
}

/// Factory `spawn` adapter — widens the concrete [`FileChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_file_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — the file sink needs no engine-wide config.
fn register_sink(registry: &mut SinkRegistry) {
    register_file_sink(registry);
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // `ack-mode: on-complete` is honoured — the poll loop defers the terminal
        // file move (`.done/` vs `.failed/`) through the engine's deferred-ack registry
        // (`InboundIntake::deliver_deferred`; see `file::source`).
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_file_destinations_after_registration() {
        // The gate-defect guard: a registered file sink MUST resolve file:// destinations
        // (an UNREGISTERED sink poisons every outbound row).
        let mut registry = SinkRegistry::new();
        register_file_sink(&mut registry);
        assert!(registry.resolve("file:///var/spool/out/key-1").is_some());
        assert!(registry.resolve("file://relative/dir/").is_some());
        assert!(registry.resolve("https://host/cb").is_none());
        assert!(registry.resolve("kafka://topic").is_none());
    }
}
