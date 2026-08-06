//! Boot sequence + health endpoints — the assembled engine. `serve` is the whole
//! boot: load config → open the deployments dir (sealed `.sutra` archives) →
//! engine-internal persistence (pool + the same shipped migration SQL) → per-deployment
//! assembly (datastores, executor registries, structural codecs, channels) → axum router
//! (health + channel routes).
//!
//! The OIDC-gated Admin REST surface (`/admin/*`) is mounted here too — its handlers +
//! the bearer-JWT `AdminGate` layer live in [`crate::admin`]; telemetry exports separately.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};
use sutra_channels::{LeaderGate, LiveDeploymentSet};
use sutra_persistence::snapshot::{InstanceSnapshot, STATUS_COMPLETED, STATUS_TERMINATED};
use sutra_persistence::stores::{
    AliasStore, AuditEventRecord, InstanceFilter, InstanceStore, InstanceSummary, PgAliasStore,
    PgAuditEventStore, PgDeadLetterStore, PgInstanceStore, PgWaitStateStore, WaitStateStore,
    AUDIT_HISTORY_PAGE_DEFAULT, AUDIT_HISTORY_PAGE_MAX, DEAD_LETTER_PAGE_DEFAULT,
    DEAD_LETTER_PAGE_MAX,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use tokio::net::TcpListener;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::EngineConfig;

/// Shared server state: the readiness gate (set once the assembly completes) and the
/// live deployment-id set (readiness payload — tracks activation flips). `pub(crate)` so the
/// `/admin/*` handlers (in [`crate::admin`]) share it and delegate to the same read/control
/// helpers below — the admin surface is the OIDC-gated twin of the `/sutra/*` operate routes.
#[derive(Clone)]
pub(crate) struct AppState {
    ready: Arc<AtomicBool>,
    deployments: LiveDeploymentSet,
    /// Deploy-readiness — the watcher-published deployment-status snapshot.
    deploy_status: crate::deploy::SharedDeploymentStatus,
    /// The watcher-published per-deployment OpenAPI specs, read by
    /// `GET /sutra/deployments/{id}/openapi`.
    api_specs: crate::deploy::SharedApiSpecs,
    /// The watcher-published per-deployment node indexes (active AND draining), read by
    /// `POST /admin/instances/{id}/migrate` to validate a node mapping against both graphs
    /// without re-planning either sealed archive on the request path.
    node_indexes: crate::migrate::SharedNodeIndex,
    /// Operate surface — the engine-internal pool backing the instance list / inspect /
    /// cancel endpoints. `None` on a persistence-less engine (those endpoints report empty).
    pool: Option<PgPool>,
    /// The sync deploy controller — the `POST`/`DELETE /admin/deployments` handlers call it.
    /// `Some` only for the `db` deployment source (the `dir` source takes deploys via the folder
    /// watch, so this is `None` there).
    deploy: Option<Arc<crate::deploy::DeployController>>,
    /// The tenant-DEK/blind-index provider (from `sutra.crypto.master-key`), used by the GDPR
    /// erasure/disclosure admin surface to recompute a subject's blind index. `None` ⇒ encryption/
    /// blind-indexing is off, so those endpoints report no matches.
    key_provider: Option<Arc<dyn sutra_crypto::KeyProvider + Send + Sync>>,
    /// The channel-engine dispatch handle — the ONE control-plane path that pushes work INTO the
    /// engine: `POST /admin/dead-letters/{id}/replay` redrives a captured payload through it, so a
    /// replay is an ordinary delivery rather than a privileged back door. `None` in the in-crate
    /// test state (no actor is running there).
    engine: Option<sutra_channels::EngineHandle>,
    /// Terminal-instance retention (`sutra.instance.retention`). The cancel handler reads it to
    /// decide whether a cancelled instance is RETAINED as `TERMINATED` (queryable history) or
    /// deleted outright — the same decision the persistence bridge makes for `commit_complete`,
    /// so the two terminal paths never disagree about whether history exists.
    instance_retention: std::time::Duration,
    /// Whether the durable audit journal is switched on (`sutra.audit.sql`). Read ONLY by the
    /// instance-history endpoint, so an empty journal can be reported as "auditing is off" rather
    /// than as "this instance did nothing" — the journal is opt-in and its absence must never be
    /// mistaken for lost history.
    audit_sql_enabled: bool,
}

/// A running engine: the bound address (port `0` resolves here), the serve task, and —
/// when persistence is configured — the background loops: the outbox dispatcher's
/// drain-aware handle, the timer poller (DB-lease leader-gated), and the broker
/// consumers with their channel-lease election.
pub struct RunningEngine {
    pub local_addr: SocketAddr,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    outbox: Option<sutra_channels::OutboxDispatcherHandle>,
    /// The deployments-dir watcher (two-phase activation) — aborted on shutdown.
    deployments_watch: Option<tokio::task::JoinHandle<()>>,
    /// The `db` source's retire-when-quiescent sweep (the dir watcher runs its own inside the
    /// watch loop) — aborted on shutdown.
    deploy_sweep: Option<tokio::task::JoinHandle<()>>,
    timer_poller: Option<tokio::task::JoinHandle<()>>,
    /// The [`crate::sweeper::StuckInstanceScanner`] loop (`sutra.instance.sweep-interval`),
    /// leader-gated under [`crate::sweeper::INSTANCE_SWEEPER_ROLE`] — aborted on shutdown.
    instance_sweeper: Option<tokio::task::JoinHandle<()>>,
    /// The [`crate::sweeper::TerminalRetentionSweeper`] loop
    /// (`sutra.instance.retention-sweep-interval`), leader-gated under
    /// [`crate::sweeper::RETENTION_SWEEPER_ROLE`] — aborted on shutdown.
    retention_sweeper: Option<tokio::task::JoinHandle<()>>,
    /// The deferred-ack `sweep_timeouts()` interval task
    /// (`sutra.ack.deferred.sweep-interval`) — aborted on shutdown.
    ack_sweep: tokio::task::JoinHandle<()>,
    /// The timer poller's DB-lease election — released on shutdown so the NEXT engine
    /// on the same database can lead without waiting out the lease TTL.
    timer_election: Option<Arc<crate::leadership::DbLeaderElection>>,
    /// Every wired vendor transport (+ its singleton-channel lease election), held behind
    /// the neutral [`sutra_transport_spi::TransportChannels`] trait (domain-neutrality
    /// refactor): each is stopped / lease-released on shutdown by iterating, so a successor
    /// replica takes the queues over immediately and the engine names no broker.
    transports: Vec<std::sync::Arc<dyn sutra_transport_spi::TransportChannels>>,
    runtime: tokio::runtime::Handle,
}

impl RunningEngine {
    /// Await the HTTP serve loop (runs until the process is stopped).
    pub async fn join(self) -> std::io::Result<()> {
        self.task.await.map_err(std::io::Error::other)?
    }

    /// [`Self::join`] with termination-signal handling — the container entrypoint path.
    /// The engine runs as PID 1 (exec-form entrypoint), and PID 1 gets NO default
    /// signal dispositions: without an explicit handler a termination signal is IGNORED
    /// and the process lives until the platform's hard kill — during which its broker
    /// consumers keep competing for queue deliveries against the replacement replica
    /// (observed as a stale-rules window in a rolling-update conformance run).
    /// On SIGTERM / SIGINT this drains via [`Self::drain`]: consumers stopped and
    /// leases released BEFORE the process exits, so the successor takes over
    /// immediately.
    pub async fn join_graceful(mut self) -> std::io::Result<()> {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let served = tokio::select! {
            result = &mut self.task => Some(result),
            _ = term.recv() => None,
            _ = int.recv() => None,
        };
        match served {
            Some(result) => result.map_err(std::io::Error::other)?,
            None => {
                info!("termination signal received — draining (consumers, leases, outbox)");
                self.drain().await;
                Ok(())
            }
        }
    }

    /// Async drain for process shutdown — the same steps as [`Self::shutdown`] but
    /// AWAITED, so consumer cancellation and lease releases complete before the caller
    /// lets the process exit.
    pub async fn drain(self) {
        if let Some(watch) = &self.deployments_watch {
            watch.abort();
        }
        if let Some(sweep) = &self.deploy_sweep {
            sweep.abort();
        }
        for transport in &self.transports {
            transport.drain().await;
        }
        if let Some(poller) = self.timer_poller {
            poller.abort();
        }
        if let Some(sweeper) = self.instance_sweeper {
            sweeper.abort();
        }
        if let Some(sweeper) = self.retention_sweeper {
            sweeper.abort();
        }
        self.ack_sweep.abort();
        if let Some(election) = &self.timer_election {
            election.release_all().await;
        }
        if let Some(outbox) = &self.outbox {
            outbox.drain();
        }
        crate::audit_sinks::flush_audit();
        crate::otel::flush_active();
        self.task.abort();
    }

    /// Drain hook: refuse further outbox ticks (the in-flight batch completes). No-op
    /// without persistence. Idempotent.
    pub fn drain_outbox(&self) {
        if let Some(outbox) = &self.outbox {
            outbox.drain();
        }
    }

    /// Stop serving + stop the background loops (the restart-conformance hook: a durable
    /// timer must survive this and fire on the NEXT engine). Drain order: broker
    /// consumers stop detached (idempotent — in-flight deliveries settle their acks),
    /// every held lease is released AND AWAITED (timer leader + singleton channels — a
    /// fire-and-forget release loses the race against process/runtime teardown in a
    /// short-lived caller, stranding the lease for its full TTL so the successor cannot
    /// fire timers; proven by the simulate CLI's IT), the outbox loop refuses further
    /// ticks, and the telemetry exporters flush. [`Self::drain`] additionally awaits the
    /// transports.
    pub async fn shutdown(self) {
        if let Some(watch) = &self.deployments_watch {
            watch.abort();
        }
        if let Some(sweep) = &self.deploy_sweep {
            sweep.abort();
        }
        for transport in &self.transports {
            transport.stop_all_detached(&self.runtime);
        }
        if let Some(poller) = self.timer_poller {
            poller.abort();
        }
        if let Some(sweeper) = self.instance_sweeper {
            sweeper.abort();
        }
        if let Some(sweeper) = self.retention_sweeper {
            sweeper.abort();
        }
        self.ack_sweep.abort();
        if let Some(election) = self.timer_election {
            election.release_all().await;
        }
        if let Some(outbox) = &self.outbox {
            outbox.drain();
        }
        crate::audit_sinks::flush_audit();
        crate::otel::flush_active();
        self.task.abort();
    }
}

/// Boot the engine: deployment source → persistence → assembly → serve. Fail-closed:
/// any assembly error refuses startup (the readiness probe never reports UP on a
/// half-wired engine). Per-archive failures are NOT fatal: a bad `.sutra` is rejected
/// with its `SUTRA.DEPLOY.*` diagnostic and the rest of the source serves.
pub async fn serve(config: EngineConfig) -> Result<RunningEngine, Box<dyn std::error::Error>> {
    let ready = Arc::new(AtomicBool::new(false));

    // ---- persistence: the ENGINE-INTERNAL datasource (the engine tables) ----------
    // Opened FIRST (before the deployment source) because the `db` source boots its active set
    // from the deployment_archive store. Configured ⇒ open the pool and run THE SAME shipped
    // migration SQL (vendored into the image; `sutra_schema_history` interoperates with
    // sutra-migrate). Not configured ⇒ run persistence-less: wait-state inbound fails closed.
    let pool = match &config.datasource_url {
        Some(url) => Some(init_engine_db(url, &config).await?),
        None => {
            warn!(
                "no engine datasource configured — running persistence-less (wait-state \
                 processes will reject with SUTRA.INBOUND.PERSISTENCE_REQUIRED)"
            );
            None
        }
    };

    // ---- RLS-bypass posture check (the two-layer isolation model) -------
    // If the engine's DB role can bypass RLS (superuser / BYPASSRLS), the explicit
    // deployment-bind + RLS defence silently degrades to one layer. Refuse boot loudly
    // (the opt-out downgrades to a warning). A non-PG store / absent role soft-skips.
    if let Some(pool) = &pool {
        crate::rls_check::enforce_rls_bypass_posture(pool, config.rls_bypass_check_enabled).await?;
    }

    // ---- deployment source: `dir` folder-watch OR the DB-backed store -----
    // `dir` opens the archives directory with its first verify-and-prepare scan (blocking: fs +
    // zip + BPMN parse) and is watched for add/remove/change. `db` boots its ACTIVE set from the
    // deployment_archive store (deploy is API-only, no folder watch). Both yield the boot
    // plan set + the
    // per-deployment OpenAPI specs; `directory` is `Some` only for `dir` (it owns the watcher).
    let (directory, boot_plans, boot_draining_plans, boot_api_specs, db_boot) = match config
        .deployment_source
    {
        crate::config::DeploymentSourceKind::Db => {
            let Some(pool) = pool.clone() else {
                return Err(
                    "sutra.deployment.source=db requires a configured datasource \
                            (SUTRA_DATASOURCE_URL)"
                        .into(),
                );
            };
            let store = sutra_persistence::stores::PgDeploymentArchiveStore::new(pool);
            // The SERVED set, not just the active one: a pod that restarts (or joins) after a
            // hot-deploy must re-plan the DRAINING tail from the archive's stored bytes, or the
            // instances pinned to those revisions have no definition to resume against and both
            // resume paths fail closed. The tail carries no channel bindings — it exists to be
            // resumed into, never to accept new intake — and leaves on the quiescence sweep.
            let served = store
                .list_active_and_draining()
                .await
                .map_err(|e| e.to_string())?;
            let (active, draining): (Vec<_>, Vec<_>) = served.into_iter().partition(|row| {
                !matches!(
                    row.status,
                    sutra_persistence::stores::ArchiveStatus::Draining
                )
            });
            // (id, slot) for the boot deploy-status snapshot (the db source has no watcher to
            // publish it, so it is seeded here after assembly).
            let id_slots: Vec<(String, String)> = active
                .iter()
                .map(|row| (row.archive.deployment_id.clone(), row.archive.slot.clone()))
                .collect();
            let archives_of = |rows: Vec<sutra_persistence::stores::ServedArchiveRow>| {
                rows.into_iter()
                    .map(|row| (row.archive.deployment_id, row.archive.bytes))
                    .collect::<Vec<(String, Vec<u8>)>>()
            };
            let plans = crate::deploy::plans_from_store(archives_of(active));
            let draining_plans = crate::deploy::plans_from_store(archives_of(draining));
            let specs = plans
                .iter()
                .chain(draining_plans.iter())
                .map(|p| (p.dep.value().to_string(), p.openapi_spec.clone()))
                .collect();
            info!(
                source = "db",
                deployments = plans.len(),
                draining = draining_plans.len(),
                "booted active set + draining tail from the deployment_archive store"
            );
            (None, plans, draining_plans, specs, Some((store, id_slots)))
        }
        crate::config::DeploymentSourceKind::Dir => {
            let deployments_dir = config.deployments_dir.clone().expect(
                "sutra.deployments.dir is required for the dir source (enforced by EngineConfig::load)",
            );
            let directory = tokio::task::spawn_blocking(
                move || -> Result<crate::deploy::DeploymentDirectory, String> {
                    crate::deploy::DeploymentDirectory::open(deployments_dir)
                        .map_err(|e| e.to_string())
                },
            )
            .await??;
            let plans = directory.active_plans();
            let specs = directory.api_specs();
            // The dir source boots with an EMPTY draining tail: nothing has been flipped away
            // yet in this process, and the directory is the whole truth of what is deployed.
            (Some(directory), plans, Vec::new(), specs, None)
        }
    };

    // The boot node index — the migrate validator's view of both graphs. Projected here, before
    // the plans are moved into assembly, and covering the DRAINING half too: a migration's SOURCE
    // is by definition a deployment that has been flipped away from.
    let boot_node_indexes: std::collections::HashMap<
        String,
        Arc<crate::migrate::DeploymentNodeIndex>,
    > = boot_plans
        .iter()
        .chain(boot_draining_plans.iter())
        .map(|p| (p.dep.value().to_string(), Arc::new(p.node_index())))
        .collect();

    // ---- assembly: datastores + executor + channels on the actor thread ----
    // (plus the outbox dispatcher task when persistence is configured, plus the
    // metrics listener when telemetry is active — the exporter stack itself
    // is process-global, installed once by `otel::init` in main())
    let handle = tokio::runtime::Handle::current();
    let metrics_labels = config.telemetry.metrics_wiring();
    // Boot-active ids (captured before boot_plans is moved into the assembly) — used to seed the
    // db source's deploy-status snapshot below.
    let boot_ids: std::collections::HashSet<String> = boot_plans
        .iter()
        .map(|p| p.dep.value().to_string())
        .collect();
    // Crypto key provider — HkdfKeyProvider from the master key, or the envelope
    // EnvelopeKeyProvider loading sealed DEKs from the `data_key` store (async, so built here in
    // serve() before the runtime-less actor thread). The one provider is shared by the persistence
    // bridge (encryption at rest) and the GDPR erasure surface (blind-index recompute).
    let key_provider = crate::assembly::build_key_provider(pool.as_ref(), &config)
        .await
        .map_err(Box::<dyn std::error::Error>::from)?;
    // ONE deferred-ack registry per engine process (`sutra.ack.deferred.*`):
    // broker `ack-mode: on-complete` channels park their per-delivery settle here until
    // the instance's terminal event. Engine-process-scoped (NOT per activation) so
    // pending acks survive an activation flip; the sweep task below nacks timed-out entries on
    // the configured cadence (the outbox tick-loop convention).
    let deferred_acks = Arc::new(sutra_channels::DeferredAckRegistry::new(
        config.deferred_ack.capacity,
        config.deferred_ack.timeout,
    ));
    let ack_sweep = {
        let registry = Arc::clone(&deferred_acks);
        let sweep_interval = config.deferred_ack.sweep_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                // Per-entry SUTRA.ACK.DEFERRED_TIMEOUT events are emitted inside the
                // registry (channel + instance fields); nothing to add here.
                registry.sweep_timeouts();
            }
        })
    };

    // The boot ACTIVE set's timer-start schedules, captured BEFORE the plans move into the
    // actor. Armed just below, once the pool is known to be live — boot does not run the
    // activation flip, so this is the only thing that arms a statically-deployed schedule.
    // TEST-ONLY (P1-7): `config.now_override` is `None` on every production boot, in which case
    // this is exactly `OffsetDateTime::now_utc()` as before.
    let boot_schedule_now = config
        .now_override
        .as_ref()
        .map(sutra_executor::TestClock::now)
        .unwrap_or_else(time::OffsetDateTime::now_utc);
    let boot_schedules: Vec<(
        sutra_executor::DeploymentId,
        Vec<sutra_persistence::stores::TimerScheduleArming>,
    )> = boot_plans
        .iter()
        .map(|plan| (plan.dep.clone(), plan.timer_schedules(boot_schedule_now)))
        .collect();

    // Coverage seed-at-deploy — once per activation at the assembly level, BEFORE the
    // engine actor spawns (hoisted out of the per-shard build; see `seed_declared_coverage`).
    // Its return value is the activation-initial covered-set snapshot the per-lane builds
    // apply — the read is hoisted here with the seed (Phase 3: no `block_on` in the build).
    let initial_coverage =
        crate::assembly::seed_declared_coverage(&boot_plans, &boot_draining_plans).await;

    let runtime = crate::assembly::build_engine_runtime(
        boot_plans,
        boot_draining_plans,
        pool.clone(),
        handle.clone(),
        config.outbox_tick_interval,
        config.outbox_retry.clone(),
        metrics_labels.clone(),
        config.payload_cap_bytes,
        config.audit.clone(),
        key_provider.clone(),
        config.incident_sql,
        config.instance_retention.retention,
        Arc::clone(&deferred_acks),
        config.external_task.clone(),
        config.now_override.clone(),
        config.engine_shards.clone(),
        initial_coverage,
    )?;
    let crate::assembly::EngineRuntime {
        router: channel_routes,
        engine: engine_handle,
        deployments: live_deployments,
        external_tasks,
        outbox,
        transports,
    } = runtime;

    // The shard router's per-lane meters (`sutra.engine.shard.*`, execution scale-out
    // §6.1) — registered ONCE per boot (the counter registry is router-owned and
    // survives activation flips), and only when telemetry metrics are wired, in parity
    // with the execution-listener wiring above.
    if metrics_labels.is_some() {
        crate::otel::register_shard_router_meters(engine_handle.shard_metrics());
    }

    // Arm the boot ACTIVE set's timer-start schedules (no-op without persistence, and without
    // any timer start). The poller spawned below claims them from here on.
    crate::deploy::arm_boot_schedules(&pool, &boot_schedules).await;

    // Deploy-readiness status — published by the watcher after every tick, read by
    // the /sutra/deployments endpoints. Created unconditionally so the endpoints exist even
    // with no archive source (they report an empty set then).
    let deploy_status: crate::deploy::SharedDeploymentStatus =
        Arc::new(std::sync::RwLock::new(Default::default()));

    // Per-deployment OpenAPI specs — seeded from the boot-activated plans so
    // /sutra/deployments/{id}/openapi answers before the first watch tick.
    let api_specs: crate::deploy::SharedApiSpecs = Arc::new(std::sync::RwLock::new(boot_api_specs));

    // Per-deployment node indexes, seeded the same way — `POST /admin/instances/{id}/migrate`
    // validates against these, and must answer before the first watch tick too.
    let node_indexes: crate::migrate::SharedNodeIndex =
        Arc::new(std::sync::RwLock::new(boot_node_indexes));

    // ---- activation wiring — the dir watcher OR the db sync-deploy controller ----
    // The ActivationHooks (the flip capability) are built once and moved into exactly one of: the
    // `dir` folder-watcher (poll → scan → flip), or the `db` DeployController (the sync deploy
    // API's validate → store → flip). The sources are mutually exclusive.
    let hooks = crate::deploy::ActivationHooks {
        engine: engine_handle.clone(),
        deployments: live_deployments.clone(),
        pool: pool.clone(),
        runtime: handle.clone(),
        metrics_labels,
        payload_cap_bytes: config.payload_cap_bytes,
        transports: transports.clone(),
        status: deploy_status.clone(),
        specs: api_specs.clone(),
        node_indexes: node_indexes.clone(),
        audit: config.audit.clone(),
        key_provider: key_provider.clone(),
        incident_sql: config.incident_sql,
        instance_retention: config.instance_retention.retention,
        deferred_acks,
        now_override: config.now_override.clone(),
    };
    let (deployments_watch, deploy_controller) = match (directory, db_boot) {
        (Some(directory), _) => {
            let watch = crate::deploy::spawn_deployments_watch(
                directory,
                hooks,
                config.deployments_poll_interval,
            );
            (Some(watch), None)
        }
        (None, Some((store, id_slots))) => {
            // Seed the boot deploy-status so /sutra/health/ready reports the active count
            // immediately (the db source runs no watcher to publish it).
            if let Ok(mut s) = deploy_status.write() {
                s.active = id_slots
                    .into_iter()
                    .filter(|(id, _)| boot_ids.contains(id))
                    .collect();
            }
            let controller = Arc::new(crate::deploy::DeployController::new(store, hooks));
            (None, Some(controller))
        }
        (None, None) => (None, None),
    };

    // db source: the multi-replica convergence listener (pg LISTEN/NOTIFY) — re-activate when any
    // replica commits a deploy/undeploy. A no-op for a single replica (it receives its own notify,
    // and the re-activate is idempotent).
    if let (Some(controller), Some(pool)) = (&deploy_controller, &pool) {
        let (controller, pool) = (controller.clone(), pool.clone());
        handle.spawn(async move {
            crate::deploy::spawn_deploy_listen(pool, controller).await;
        });
    }

    // db source: the retire-when-quiescent sweep. The dir watcher runs this inside its poll
    // loop; the db source has no watcher, so the sweep gets its own task — without it the
    // DRAINING tail (which now genuinely serves pinned resumes) would never retire.
    let deploy_sweep = deploy_controller.as_ref().map(|controller| {
        crate::deploy::spawn_deploy_quiescence_sweep(
            controller.clone(),
            config.deployments_poll_interval,
        )
    });

    // Operate surface: keep a pool handle for the instance list / inspect / cancel
    // endpoints before the timer match consumes `pool`.
    let instances_pool = pool.clone();

    // ---- timer poller + the two instance sweepers — persistence-backed engines only ------
    // All three are singleton roles gated on the SAME DB-lease election (roles register
    // dynamically): [`crate::timer::TIMER_LEADER_ROLE`],
    // [`crate::sweeper::INSTANCE_SWEEPER_ROLE`] and [`crate::sweeper::RETENTION_SWEEPER_ROLE`].
    // The `AlwaysLeading` default stays for injected/pool-less use, which never reaches here.
    // One immediate poll per role at boot so a single replica leads without waiting a full poll
    // interval.
    let (timer_poller, timer_election, instance_sweeper, retention_sweeper) = match pool {
        Some(pool) => {
            let poller_config = crate::timer::TimerPollerConfig {
                tick: std::time::Duration::from_millis(
                    std::env::var("SUTRA_TIMER_TICK_MS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(500),
                ),
                // TEST-ONLY (P1-7): `None` on every production boot (no `SUTRA_*` env sets it —
                // see `EngineConfig::now_override`'s doc), in which case the poller reads the
                // real wall clock exactly as before this field existed.
                now_override: config.now_override.clone(),
                ..Default::default()
            };
            let election = Arc::new(crate::leadership::DbLeaderElection::with_defaults(
                Arc::new(crate::leadership::PgLeaseHandle(
                    sutra_persistence::stores::PgLeaseStore::new(pool.clone()),
                )),
                None,
                handle.clone(),
            ));
            let gate = election.gate(crate::timer::TIMER_LEADER_ROLE);
            election.poll_now(crate::timer::TIMER_LEADER_ROLE).await;
            info!(
                tick_ms = poller_config.tick.as_millis() as u64,
                role = crate::timer::TIMER_LEADER_ROLE,
                leading = gate.is_leading(),
                "timer poller starting (DB-lease leader-gated)"
            );

            // The stuck-instance scanner: clears per-instance ownership claims whose owner
            // died mid-step (it never resumes anything — the next relay/timer re-drives).
            let sweep_gate = election.gate(crate::sweeper::INSTANCE_SWEEPER_ROLE);
            election
                .poll_now(crate::sweeper::INSTANCE_SWEEPER_ROLE)
                .await;
            info!(
                interval_s = config.instance_sweep.interval.as_secs(),
                claim_timeout_s = config.instance_sweep.claim_timeout.as_secs(),
                role = crate::sweeper::INSTANCE_SWEEPER_ROLE,
                leading = sweep_gate.is_leading(),
                owner = sutra_channels::replica_id(),
                "stuck-instance scanner starting (DB-lease leader-gated)"
            );
            let sweeper = crate::sweeper::StuckInstanceScanner::new(
                pool.clone(),
                live_deployments.clone(),
                sweep_gate,
                config.instance_sweep.clone(),
            )
            .spawn();

            // The terminal-retention purge: drops finished instances past
            // `sutra.instance.retention`. Its own lease role — a days-scale destructive sweep
            // must be observable (and placeable) independently of the minutes-scale claim GC.
            let retention_gate = election.gate(crate::sweeper::RETENTION_SWEEPER_ROLE);
            election
                .poll_now(crate::sweeper::RETENTION_SWEEPER_ROLE)
                .await;
            info!(
                interval_s = config.instance_retention.interval.as_secs(),
                retention_s = config.instance_retention.retention.as_secs(),
                role = crate::sweeper::RETENTION_SWEEPER_ROLE,
                leading = retention_gate.is_leading(),
                retain_at_terminal = !config.instance_retention.retention.is_zero(),
                "terminal-retention sweeper starting (DB-lease leader-gated); a zero retention \
                 means finished instances are deleted in the terminal step and this sweep only \
                 cleans up rows written before that was configured"
            );
            let retention = crate::sweeper::TerminalRetentionSweeper::new(
                pool.clone(),
                live_deployments.clone(),
                retention_gate,
                config.instance_retention.clone(),
            )
            .spawn();

            let poller = crate::timer::spawn_timer_poller(
                pool,
                live_deployments.clone(),
                engine_handle.clone(),
                gate,
                poller_config,
            );
            (Some(poller), Some(election), Some(sweeper), Some(retention))
        }
        None => (None, None, None, None),
    };

    // The GDPR erasure surface recomputes subject blind indexes with the SAME key provider the
    // persist path used (built once above; None ⇒ blind-indexing off). Move it into the AppState.
    let state = AppState {
        ready: Arc::clone(&ready),
        deployments: live_deployments,
        deploy_status,
        api_specs,
        node_indexes,
        pool: instances_pool,
        deploy: deploy_controller,
        key_provider,
        engine: Some(engine_handle.clone()),
        instance_retention: config.instance_retention.retention,
        audit_sql_enabled: config.audit.sql,
    };

    // The `/admin/*` OIDC gate, resolved from config (fail-closed). A bad JWKS / OIDC
    // param refuses boot here; `DevOpen` (explicit dev flag) and `Unconfigured` (closed, 503) both
    // boot and log their posture. The gate layers ONLY over the admin sub-router below.
    let admin_gate = crate::admin::AdminGate::from_config(&config.admin_auth)
        .map_err(|e| format!("admin OIDC config invalid (sutra.admin.oidc.*): {e}"))?;
    admin_gate.log_posture();

    // NB: this route set is the canonical [`PLATFORM_ROUTES`] table — keep the two in sync (the
    // openapi/platform.yaml drift gate enforces the spec side).
    let router = Router::new()
        .route("/sutra/health/live", get(live))
        .route("/sutra/health/ready", get(readiness))
        .route("/sutra/deployments", get(deployments_list))
        .route("/sutra/deployments/{id}", get(deployment_by_id))
        // The per-deployment OpenAPI 3.1 surface, generated from the archive manifest and
        // served live (YAML by default; Accept: application/json / ?format=json for JSON).
        .route("/sutra/deployments/{id}/openapi", get(deployment_openapi))
        // Operate-time inspection surface (list / inspect / cancel). Unauthenticated
        // internal-ops posture (cluster-internal); the SAME operations behind OIDC gating are the
        // `/admin/*` surface, mounted below.
        .route("/sutra/instances", get(instances_list))
        .route("/sutra/instances/{id}", get(instance_by_id))
        .route("/sutra/instances/{id}/cancel", post(instance_cancel))
        // Structured JSON logs to stdout are the baseline observability surface.
        .with_state(state.clone())
        // The worker-facing PULL surface (fetch-and-lock / complete / failure). Same operate
        // posture as the routes above — workers are engine-adjacent processes, and the gated
        // twins of the operate surface live under `/admin/*`. Its own sub-router because it
        // carries the external-task service as state rather than [`AppState`].
        .merge(crate::external_task::external_task_routes(external_tasks))
        // The OIDC-gated administrative surface (deployments + instances read/control),
        // its own sub-router carrying the bearer-JWT layer so it never loosens the platform routes.
        .merge(crate::admin::admin_router(state, admin_gate))
        .merge(channel_routes);

    let listener = TcpListener::bind(("0.0.0.0", config.http_port)).await?;
    let local_addr = listener.local_addr()?;

    // Loader + assembly completed and the socket is bound — the engine is ready.
    ready.store(true, Ordering::Release);
    info!(
        port = local_addr.port(),
        "sutra-engine (rust) up — health at /sutra/health/*, channels mounted"
    );

    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(RunningEngine {
        local_addr,
        task,
        outbox,
        deployments_watch,
        deploy_sweep,
        timer_poller,
        instance_sweeper,
        retention_sweeper,
        ack_sweep,
        timer_election,
        transports,
        runtime: handle,
    })
}

/// Open the engine-internal pool (`postgres://` / `postgresql://` URL — the canonical
/// native form is the only accepted shape) and apply the vendored engine
/// migrations (root from `SUTRA_DB_MIGRATIONS`, default `/opt/sutra/db/migration` — the
/// image bakes the SQL there). A missing root is only a warning: the tables may have
/// been provisioned externally (`sutra-migrate`; the shared `sutra_schema_history`
/// ledger makes the runners interoperable).
async fn init_engine_db(
    url: &str,
    config: &EngineConfig,
) -> Result<PgPool, Box<dyn std::error::Error>> {
    let mut options = PgConnectOptions::from_str(url)
        .map_err(|e| format!("engine datasource URL '{url}' is invalid: {e}"))?;
    if let Some(user) = &config.datasource_username {
        options = options.username(user);
    }
    if let Some(password) = &config.datasource_password {
        options = options.password(password);
    }

    // `SUTRA_DB_MIGRATIONS` accepts a ':'-separated ROOT LIST: the image bakes one flattened
    // dir, but an in-repo run points at the shipped sibling families (core:audit:deploy) —
    // `collect_migrations` interleaves multiple roots into one global V-number order.
    let migration_roots: Vec<PathBuf> = std::env::var("SUTRA_DB_MIGRATIONS")
        .map(|v| v.split(':').map(PathBuf::from).collect())
        .unwrap_or_else(|_| vec![PathBuf::from("/opt/sutra/db/migration")]);
    let (present, missing): (Vec<PathBuf>, Vec<PathBuf>) =
        migration_roots.into_iter().partition(|r| r.is_dir());
    for root in &missing {
        warn!(
            root = %root.display(),
            "engine migration root missing — assuming that family was provisioned \
             externally (sutra-migrate)"
        );
    }
    if !present.is_empty() {
        // Dedicated connection on a blocking thread (raw_sql futures are not provably
        // Send under the current rustc — the same posture as the datastore migrations).
        let opts = options.clone();
        let roots = present.clone();
        let applied = tokio::task::spawn_blocking(move || run_engine_migrations(opts, &roots))
            .await
            .map_err(|e| format!("engine migration task failed: {e}"))??;
        info!(
            roots = %present
                .iter()
                .map(|r| r.display().to_string())
                .collect::<Vec<_>>()
                .join(":"),
            applied,
            "engine-internal migrations applied (sutra_schema_history ledger)"
        );
    }

    // The engine-internal datasource pool (shared by every system store: coverage, audit,
    // outbox). No hardcoded size — the shared default constant centralises it.
    let pool = PgPoolOptions::new()
        .max_connections(sutra_datastore::DEFAULT_MAX_CONNECTIONS)
        .connect_lazy_with(options);
    Ok(pool)
}

/// Blocking-thread body: dedicated connection + the ordered `V<number>__*.sql` runner.
fn run_engine_migrations(options: PgConnectOptions, roots: &[PathBuf]) -> Result<u32, String> {
    let root_refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
    let scripts = sutra_persistence::migrate::collect_migrations(&root_refs)
        .map_err(|e| format!("engine migration discovery failed: {e}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("engine migration runtime failed: {e}"))?;
    runtime.block_on(async move {
        let mut conn = options
            .connect()
            .await
            .map_err(|e| format!("engine datasource connect failed: {e}"))?;
        let applied = sutra_persistence::migrate::apply_migrations(&mut conn, &scripts)
            .await
            .map_err(|e| format!("engine migrations failed: {e}"))?;
        Ok(applied)
    })
}

/// Liveness — the process is up AND every actor lane is alive (SmallRye-compatible JSON
/// shape). A lane dies only outside the per-dispatch panic containment (a rebuild/boot
/// failure on the lane's own task); after that, work hashed to it answers
/// `SUTRA.RUNTIME.UNEXPECTED — engine actor is not running` forever while the process
/// otherwise looks healthy — the zombie a k8s IT ran against for 14 minutes. A dead lane
/// therefore fails LIVENESS (restart the replica; the lane's key space has no other home
/// in this process), not just readiness.
async fn live(State(state): State<AppState>) -> impl IntoResponse {
    if let Some(engine) = &state.engine {
        let dead = engine.dead_lanes();
        if !dead.is_empty() {
            let body = serde_json::json!({
                "status": "DOWN",
                "checks": [{
                    "name": "sutra-engine",
                    "status": "DOWN",
                    "data": { "deadLanes": dead }
                }]
            });
            return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
        }
    }
    Json(health_body("UP", "sutra-engine", "UP")).into_response()
}

/// Readiness — UP once the loader + assembly completed (deployments resolvable, channels
/// mounted). The count tracks activation flips (active + DRAINING).
///
/// `data.shards` reports the shard router's LIVE lane count — read from the running
/// [`sutra_channels::EngineHandle`] (one entry per spawned actor lane), never echoed from
/// config, so it states what this process actually runs rather than what it was asked to
/// run. It is the only black-box evidence of the lane count: the `sutra.engine.shard.*`
/// meters need an OTLP collector, and thread names are not observable over HTTP. Absent
/// only when no actor is wired (the in-crate test state; every real boot wires one).
async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    // A dead lane fails readiness too (see `live`): stop routing new work at a replica
    // that cannot serve part of its key space, without waiting for the liveness restart.
    if state
        .engine
        .as_ref()
        .is_some_and(|engine| !engine.dead_lanes().is_empty())
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(health_body("DOWN", "sutra-engine", "DOWN")),
        )
            .into_response();
    }
    if state.ready.load(Ordering::Acquire) {
        let mut data = serde_json::json!({ "deployments": state.deployments.snapshot().len() });
        if let Some(engine) = &state.engine {
            data["shards"] = serde_json::json!(engine.shard_count());
        }
        let body = serde_json::json!({
            "status": "UP",
            "checks": [{
                "name": "sutra-loader",
                "status": "UP",
                "data": data
            }]
        });
        (StatusCode::OK, Json(body)).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(health_body("DOWN", "sutra-loader", "DOWN")),
        )
            .into_response()
    }
}

/// The full deployment-status snapshot (active with slot, draining, failed).
async fn deployments_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(deployments_snapshot_json(&state))
}

/// The full deployment-status snapshot as JSON — shared by `/sutra/deployments` and the
/// OIDC-gated `/admin/deployments`. A poisoned lock degrades to an empty snapshot.
pub(crate) fn deployments_snapshot_json(state: &AppState) -> serde_json::Value {
    let snapshot = state
        .deploy_status
        .read()
        .map(|s| s.clone())
        .unwrap_or_default();
    snapshot.to_json()
}

/// One deployment's status by its (content-hash) deploymentId. 404 with phase
/// "Unknown" when the engine has not activated it — the caller (`sutra deploy --wait`) keeps
/// polling (e.g. kubelet has not yet synced the ConfigMap into the pod volume).
async fn deployment_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    deployment_status_response(&state, &id)
}

/// One deployment's status by id — shared by `/sutra/deployments/{id}` and the OIDC-gated
/// `/admin/deployments/{id}`. 404 with phase `Unknown` when never activated.
pub(crate) fn deployment_status_response(
    state: &AppState,
    id: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    let snapshot = state
        .deploy_status
        .read()
        .map(|s| s.clone())
        .unwrap_or_default();
    match snapshot.lookup(id) {
        Some(body) => (StatusCode::OK, Json(body)),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "deploymentId": id, "phase": "Unknown", "ready": false })),
        ),
    }
}

/// `POST /admin/deployments` body — the sync deploy (db source): validate + store + activate via
/// the [`crate::deploy::DeployController`], returning the finite `Active` outcome. `503` when the
/// engine is not on the `db` source (no controller); `400` on a rejected archive.
pub(crate) async fn deploy_response(
    state: &AppState,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let Some(controller) = &state.deploy else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "deploy API unavailable — the engine is not on the db deployment source \
                          (sutra.deployment.source=db)"
            })),
        )
            .into_response();
    };
    match controller.deploy(body.to_vec()).await {
        Ok(out) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "deploymentId": out.deployment_id,
                "slot": out.slot,
                "revision": out.revision,
                "phase": "Active",
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `POST /admin/deployments?mode=async` body (db source) — the async long-running deploy: validate +
/// store ACTIVE synchronously (fail-closed), then defer the activation flip and return `202 Accepted`
/// with `{deploymentId, Pending}`. The caller reaches `Active` by polling `GET /sutra/deployments/{id}`
/// or by awaiting the completion CloudEvent. Suits large archives whose flip can outlast a k8s
/// ingress read-timeout; validation failures still fail the POST synchronously (`400`).
pub(crate) async fn deploy_async_response(
    state: &AppState,
    sinks: Vec<String>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let Some(controller) = &state.deploy else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "deploy API unavailable — the engine is not on the db deployment source \
                          (sutra.deployment.source=db)"
            })),
        )
            .into_response();
    };
    match controller.deploy_async(body.to_vec(), sinks).await {
        Ok(acc) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "deploymentId": acc.deployment_id,
                "slot": acc.slot,
                "revision": acc.revision,
                "phase": "Pending",
                "statusUrl": format!("/sutra/deployments/{}", acc.deployment_id),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `DELETE /admin/deployments/{slot}` body — retire the slot's active archive + re-flip (db source).
pub(crate) async fn undeploy_response(state: &AppState, slot: &str) -> axum::response::Response {
    let Some(controller) = &state.deploy else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "deploy API unavailable — the engine is not on the db deployment source"
            })),
        )
            .into_response();
    };
    match controller.undeploy(slot).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({ "slot": slot, "phase": "Draining" })),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no active deployment for slot '{slot}'") })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// The per-deployment OpenAPI 3.1 surface, generated from the archive manifest and
/// served live. YAML by default (the OpenAPI convention); `Accept: application/json` or
/// `?format=json` returns JSON. 404 when `id` is not a live (active or draining) deployment.
/// Because the spec is projected from the SAME parsed plan that drives routing, it never drifts
/// from the live surface — there is no committed file to gate, only the generator's golden test.
async fn deployment_openapi(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    let spec = state
        .api_specs
        .read()
        .ok()
        .and_then(|m| m.get(&id).cloned());
    let Some(spec) = spec else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!("no live deployment '{id}'"),
                "deploymentId": id,
            })),
        )
            .into_response();
    };
    let want_json = match params.get("format").map(String::as_str) {
        Some("json") => true,
        Some("yaml") => false,
        _ => headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|a| a.contains("application/json") && !a.contains("yaml"))
            .unwrap_or(false),
    };
    if want_json {
        (
            [(header::CONTENT_TYPE, "application/json")],
            sutra_openapi::render_json(&spec),
        )
            .into_response()
    } else {
        (
            [(header::CONTENT_TYPE, "application/yaml")],
            sutra_openapi::render_yaml(&spec),
        )
            .into_response()
    }
}

/// The canonical **platform / system API** route table — `(METHOD, path)`. The router in
/// [`serve`] is built to mirror this, and the committed OpenAPI spec (`openapi/platform.yaml`) is
/// drift-gated against it by `tests::openapi_platform_spec_matches_the_route_table`:
/// adding or removing a platform route means updating THIS table + the router + the spec, or the
/// build fails. (The per-deployment channel routes are generated per archive, not listed here.)
pub const PLATFORM_ROUTES: &[(&str, &str)] = &[
    ("GET", "/sutra/health/live"),
    ("GET", "/sutra/health/ready"),
    ("GET", "/sutra/deployments"),
    ("GET", "/sutra/deployments/{id}"),
    ("GET", "/sutra/deployments/{id}/openapi"),
    ("GET", "/sutra/instances"),
    ("GET", "/sutra/instances/{id}"),
    ("POST", "/sutra/instances/{id}/cancel"),
    // The worker-facing external-task PULL surface: long-poll fetch-and-lock, then complete or
    // fail the locked task. Operate-posture (not admin) — these are the routes polyglot workers
    // drive, and a completion re-enters the engine through the ordinary inbound path, so it is
    // an ordinary delivery rather than a privileged back door. Handlers in
    // [`crate::external_task`].
    ("POST", "/sutra/external-tasks/fetch-and-lock"),
    ("POST", "/sutra/external-tasks/{id}/complete"),
    ("POST", "/sutra/external-tasks/{id}/failure"),
    // The OIDC-gated administrative surface (bearer-JWT: issuer + audience + JWKS + admin
    // scope). The read/control operations are the gated twins of the `/sutra/*` operate routes; the
    // handlers + the `AdminGate` layer live in [`crate::admin`].
    ("GET", "/admin/deployments"),
    ("POST", "/admin/deployments"),
    ("GET", "/admin/deployments/{id}"),
    ("DELETE", "/admin/deployments/{id}"),
    ("GET", "/admin/instances"),
    ("GET", "/admin/instances/{id}"),
    ("GET", "/admin/instances/by-alias/{key}/{value}"),
    // The execution HISTORY of one instance: its `audit_event` journal, seq-ordered and
    // cursor-paged. Admin-only — an audit row carries the captured business payload, the same
    // posture a dead letter's bytes get. The journal itself stays OPT-IN
    // (`sutra.audit.sql` + `<q:audit>`); this route only finally READS it.
    ("GET", "/admin/instances/{id}/history"),
    ("POST", "/admin/instances/{id}/cancel"),
    // Instance MIGRATION: re-pin one live instance onto another ACTIVE deployment, rewriting every
    // node id its durable state names. Admin-only and validated fail-closed — the endpoint returns
    // the full machine-readable report whether it migrated, dry-ran or refused.
    ("POST", "/admin/instances/{id}/migrate"),
    // The BATCH form (F2): the same operation over a filtered population, one claim and one
    // transaction PER INSTANCE, with a per-instance outcome in the report. A static segment beside
    // `/admin/instances/{id}`, which the router resolves to this route because static wins over
    // dynamic — and `migrate` is not a UUID, so the parameterised route could never have served it.
    ("POST", "/admin/instances/migrate"),
    // The GDPR erasure/disclosure surface: resolve a data subject's instances via the blind
    // index, then hard-delete their state + null captured audit payloads (`dryRun` = disclose only).
    ("POST", "/admin/subjects/erase"),
    // The dead-letter (DLQ) surface: read what was consumed-and-dropped, and redrive it through
    // the normal intake path. Admin-only by construction — a dead letter holds the raw payload.
    ("GET", "/admin/dead-letters"),
    ("GET", "/admin/dead-letters/{id}"),
    ("POST", "/admin/dead-letters/{id}/replay"),
];

/// The redaction placeholder substituted for every `@sensitive` variable value in the
/// inspect projection (the value persists for resume but must never leave the box
/// through the operate surface). Sourced from the shared [`sutra_bpmn::REDACTED_PLACEHOLDER`]
/// so the inspect projection and the audit payload capture mask identically.
const REDACTED_PLACEHOLDER: &str = sutra_bpmn::REDACTED_PLACEHOLDER;

/// List parked/running instances as lightweight summaries. Optional `?deployment=`
/// (one `dep-<hex>`) and `?status=` (e.g. `SUSPENDED`) narrow the scan; without
/// `?deployment=` every live deployment is walked (the outbox/timer-poller pattern). A
/// persistence-less engine reports an empty set.
///
/// FINISHED instances — the rows terminal retention now keeps — are excluded by default and
/// included with `?includeTerminal=true`. The default is exclusion because this list has no
/// paging: a busy deployment holds a whole retention window of completed instances, and "what is
/// still in flight" is the question the unqualified list has always answered. Asking for a
/// terminal `?status=` (`COMPLETED` / `TERMINATED`) implies the flag, so the obvious query does
/// the obvious thing instead of silently returning nothing.
async fn instances_list(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    instances_list_response(&state, &params).await
}

/// List parked/running instance summaries — shared by `/sutra/instances` and the OIDC-gated
/// `/admin/instances`. See [`instances_list`] for the filter semantics.
pub(crate) async fn instances_list_response(
    state: &AppState,
    params: &HashMap<String, String>,
) -> axum::response::Response {
    let Some(pool) = state.pool.clone() else {
        return Json(serde_json::json!({ "instances": [] })).into_response();
    };
    let store = PgInstanceStore::new(pool);
    let status = params.get("status").filter(|s| !s.is_empty()).cloned();
    let asked_for_terminal_status = status
        .as_deref()
        .is_some_and(|s| s == STATUS_COMPLETED || s == STATUS_TERMINATED);
    let filter = InstanceFilter {
        include_terminal: asked_for_terminal_status
            || params
                .get("includeTerminal")
                .is_some_and(|v| matches!(v.trim(), "true" | "1" | "yes")),
        status,
    };
    // An explicit ?deployment= wins; otherwise scan the live deployment set.
    let deployments: Vec<PersistDeploymentId> = match params.get("deployment") {
        Some(dep) => match PersistDeploymentId::new(dep.clone()) {
            Ok(d) => vec![d],
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        },
        None => state
            .deployments
            .snapshot()
            .iter()
            .filter_map(|d| PersistDeploymentId::new(d.value()).ok())
            .collect(),
    };
    let mut instances = Vec::new();
    for dep in &deployments {
        match store.list(dep, &filter).await {
            Ok(rows) => instances.extend(rows.iter().map(summary_json)),
            Err(e) => {
                warn!(error = %e, "instance list failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    Json(serde_json::json!({ "instances": instances })).into_response()
}

/// Inspect one instance by id: completed / waiting nodes, `deploymentId`, and its
/// variables with `@sensitive` values redacted (see [`inspect_projection`]). The id alone is
/// the row's UUID; the owning deployment is found by walking the live set (instances are
/// keyed by `(deployment_id, instance_id)`). 404 when no live deployment owns the id.
async fn instance_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    instance_inspect_response(&state, &id).await
}

/// Inspect one instance by id — shared by `/sutra/instances/{id}` and the OIDC-gated
/// `/admin/instances/{id}`. Walks the live deployment set to find the owner; `@sensitive`
/// values are redacted by [`inspect_projection`].
pub(crate) async fn instance_inspect_response(
    state: &AppState,
    id: &str,
) -> axum::response::Response {
    let Some(instance_id) = parse_uuid(id) else {
        return bad_uuid(id).into_response();
    };
    let Some(pool) = state.pool.clone() else {
        return instance_not_found(id).into_response();
    };
    let store = PgInstanceStore::new(pool);
    for dep in state.deployments.snapshot() {
        let Ok(pdep) = PersistDeploymentId::new(dep.value()) else {
            continue;
        };
        match store.load(&pdep, instance_id).await {
            Ok(Some(row)) => {
                return match InstanceSnapshot::read(&row.serialised) {
                    Ok(snapshot) => (
                        StatusCode::OK,
                        Json(inspect_projection(instance_id, &snapshot)),
                    )
                        .into_response(),
                    Err(e) => {
                        warn!(instance_id = %id, error = %e, "instance snapshot decode failed");
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({
                                "instanceId": id, "error": "snapshot decode failed"
                            })),
                        )
                            .into_response()
                    }
                };
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(instance_id = %id, error = %e, "instance load failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "instanceId": id, "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    instance_not_found(id).into_response()
}

/// Terminate a parked instance. Finds the owning deployment, resolves its wait points
/// (so no timer / relay tries to resume a cancelled instance), retires its live aliases (so
/// the unique-live index is freed), then RETAINS the row re-stamped `TERMINATED` — reusing the
/// existing store primitives (the same terminal cleanup a wait→end step performs, minus outbox
/// emissions). 404 when no live deployment owns the id; 409 when it has already finished.
async fn instance_cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    instance_cancel_response(&state, &id).await
}

/// Cancel/terminate one instance — shared by `/sutra/instances/{id}/cancel` and the OIDC-gated
/// `/admin/instances/{id}/cancel`. Resolves wait points, retires live aliases, then re-stamps the
/// snapshot `TERMINATED` and marks the row terminal (the wait→end terminal cleanup, minus outbox
/// emissions). 404 when no live deployment owns it.
///
/// Two P1-2 refinements over the previous delete-the-row shape:
///
/// * **The row is retained**, on the same `sutra.instance.retention` window a completed instance
///   gets — an operator who cancels an instance is exactly the operator who later needs to see
///   that it was cancelled. `PT0S` restores the delete, so the two terminal paths (this and
///   `commit_complete`) never disagree about whether history exists.
/// * **An already-finished instance is a `409`, not a re-cancel.** Before retention this could not
///   arise: a completed instance had no row, so cancelling it was a 404. Now the row lingers, and
///   blindly re-stamping it would overwrite a COMPLETED verdict with TERMINATED — rewriting
///   history through an endpoint whose job is to end things, not to relabel them. A FAILED
///   instance is deliberately still cancellable: cancel is its documented release valve.
pub(crate) async fn instance_cancel_response(
    state: &AppState,
    id: &str,
) -> axum::response::Response {
    let Some(instance_id) = parse_uuid(id) else {
        return bad_uuid(id).into_response();
    };
    let Some(pool) = state.pool.clone() else {
        return instance_not_found(id).into_response();
    };
    let store = PgInstanceStore::new(pool.clone());
    // Locate the owning deployment (instances are keyed by (deployment_id, instance_id)) and keep
    // the bytes: the terminal re-stamp is a key-patch of exactly these, never a re-encode.
    let mut owner = None;
    for dep in state.deployments.snapshot() {
        let Ok(pdep) = PersistDeploymentId::new(dep.value()) else {
            continue;
        };
        match store.load(&pdep, instance_id).await {
            Ok(Some(row)) => {
                owner = Some((pdep, row.serialised));
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(instance_id = %id, error = %e, "instance load failed during cancel");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "instanceId": id, "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    let Some((dep, serialised)) = owner else {
        return instance_not_found(id).into_response();
    };

    // Already finished? Report the verdict it already carries rather than overwriting it.
    // `peek` reads the status without decrypting anything (an encrypted snapshot has no cipher
    // available on this path, and does not need one to answer "is this over?").
    match InstanceSnapshot::peek(&serialised) {
        Ok(keys) if keys.status == STATUS_COMPLETED || keys.status == STATUS_TERMINATED => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "instanceId": id,
                    "deploymentId": dep.as_str(),
                    "status": keys.status,
                    "error": format!(
                        "instance {id} already reached {}; it is retained as history until its \
                         retention window expires, and there is nothing left to cancel",
                        keys.status
                    ),
                })),
            )
                .into_response();
        }
        Ok(_) => {}
        Err(e) => {
            warn!(instance_id = %id, error = %e, "instance snapshot peek failed during cancel");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "instanceId": id, "error": "snapshot decode failed"
                })),
            )
                .into_response();
        }
    }

    let waits = PgWaitStateStore::new(pool.clone());
    let aliases = PgAliasStore::new(pool);
    if let Err(e) = waits.resolve_all(&dep, instance_id).await {
        return cancel_error(id, "wait resolveAll", &e).into_response();
    }
    if let Err(e) = aliases.retire(&dep, instance_id).await {
        return cancel_error(id, "alias retire", &e).into_response();
    }
    if state.instance_retention.is_zero() {
        // `sutra.instance.retention=PT0S` — the operator asked for no history at all.
        if let Err(e) = store.delete(&dep, instance_id).await {
            return cancel_error(id, "instance delete", &e).into_response();
        }
    } else {
        let terminal = match InstanceSnapshot::mark_terminal(&serialised, STATUS_TERMINATED) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(instance_id = %id, error = %e, "terminal re-stamp failed during cancel");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "instanceId": id, "error": "snapshot re-stamp failed"
                    })),
                )
                    .into_response();
            }
        };
        if let Err(e) = store.mark_terminal(&dep, instance_id, &terminal).await {
            return cancel_error(id, "instance markTerminal", &e).into_response();
        }
    }
    info!(
        instance_id = %id,
        deployment = dep.as_str(),
        retained = !state.instance_retention.is_zero(),
        "instance cancelled (waits resolved, aliases retired, row retained as TERMINATED or \
         deleted)"
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "instanceId": id,
            "deploymentId": dep.as_str(),
            // The reported outcome is unchanged (`CANCELLED` is what every caller keys on); the
            // PERSISTED status is `TERMINATED`, which is what a later inspect reports, so both are
            // named rather than leaving a caller to guess how the two relate.
            "status": "CANCELLED",
            "persistedStatus": if state.instance_retention.is_zero() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(STATUS_TERMINATED.to_owned())
            },
            "retained": !state.instance_retention.is_zero(),
        })),
    )
        .into_response()
}

/// Resolve one live instance by an alias/business key — the `/admin/instances/by-alias/{key}/{value}`
/// backing. Scans the live deployment set, uses [`AliasStore::find_live`] to correlate, then
/// returns the same inspect projection as [`instance_inspect_response`]. A persistence-less engine or
/// an unmatched key → 404.
pub(crate) async fn instance_by_alias_response(
    state: &AppState,
    alias_name: &str,
    alias_value: &str,
) -> axum::response::Response {
    let Some(pool) = state.pool.clone() else {
        return alias_not_found(alias_name, alias_value).into_response();
    };
    let aliases = PgAliasStore::new(pool);
    for dep in state.deployments.snapshot() {
        let Ok(pdep) = PersistDeploymentId::new(dep.value()) else {
            continue;
        };
        match aliases.find_live(&pdep, alias_name, alias_value).await {
            Ok(Some(instance_id)) => {
                return instance_inspect_response(state, &instance_id.to_string()).await;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(alias = %alias_name, error = %e, "alias findLive failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    alias_not_found(alias_name, alias_value).into_response()
}

// ---- instance migration (P1-8 single instance; F2 batch / cross-process / resume) -----------
//
// The validation logic + the compatibility matrix live in [`crate::migrate`] as pure functions
// over data; everything below is the HTTP + persistence shell around them. That split is what
// makes the matrix unit-testable without a database, which is the only way a rule set this
// specific stays honest.
//
// v2 added three capabilities and moved NO logic into the shell. A request body parses ONCE into a
// [`crate::migrate::MigrationPlan`]; [`migrate_one`] applies that plan to exactly one instance and
// returns a verdict rather than a response; and both endpoints are thin wrappers over it. "Every
// instance validates, claims and commits INDEPENDENTLY" is therefore structural rather than
// promised: the batch is a loop over the single-instance operation, with the target resolved once.

/// The migrate operation's ownership identity — the process replica id with a `::migrate` suffix.
///
/// The suffix is load-bearing, not decoration. The instance claim CAS is deliberately RE-ENTRANT
/// for the same owner (one process advances instances on a single actor thread, so re-claiming
/// what you already hold is a heartbeat refresh, not contention). If migration claimed under the
/// bare replica id it would therefore SUCCEED against a resume that this very replica has in
/// flight — the one race the claim exists to prevent. Under a distinct owner the CAS fails, the
/// migration refuses with `SUTRA.ADMIN.MIGRATE.CLAIM_HELD`, and a resume that starts afterwards
/// bounces off the migration's claim in turn.
fn migrate_claim_owner() -> String {
    format!("{}::migrate", sutra_channels::bridge::replica_id())
}

/// Stamp one structured refusal into a report body: the code, and `valid`/`migrated` set false.
fn migrate_refusal_body(
    code: &'static str,
    detail: String,
    body: serde_json::Value,
) -> serde_json::Value {
    let mut body = body;
    if let Some(map) = body.as_object_mut() {
        map.insert("valid".to_owned(), serde_json::Value::Bool(false));
        map.insert("migrated".to_owned(), serde_json::Value::Bool(false));
        map.insert(
            "violations".to_owned(),
            serde_json::json!([{ "code": code, "sourceNodeId": null, "targetNodeId": null, "detail": detail }]),
        );
    }
    body
}

/// One structured migrate refusal: the code, the HTTP status, and the report so far.
fn migrate_refusal(
    status: StatusCode,
    code: &'static str,
    detail: String,
    body: serde_json::Value,
) -> axum::response::Response {
    (status, Json(migrate_refusal_body(code, detail, body))).into_response()
}

/// Wrap one instance's verdict as an [`InstanceAttempt`]. `resumed` is read back out of the report
/// so the flag has exactly one source of truth (the body the caller is handed).
fn migrate_attempt(
    instance_id: Uuid,
    outcome: crate::migrate::MigrationOutcome,
    report: serde_json::Value,
) -> crate::migrate::InstanceAttempt {
    crate::migrate::InstanceAttempt {
        instance_id: instance_id.to_string(),
        outcome,
        resumed: report
            .get("resumed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        report,
    }
}

/// Parse the half of a migrate body that is identical on both endpoints into a
/// [`crate::migrate::MigrationPlan`]. `Err` carries the operator-facing 400 message.
fn parse_migration_plan(body: &serde_json::Value) -> Result<crate::migrate::MigrationPlan, String> {
    let mut plan = crate::migrate::MigrationPlan {
        dry_run: body
            .get("dryRun")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        resume: body
            .get("resume")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        ..Default::default()
    };
    if let Some(raw) = body.get("nodeMapping").filter(|v| !v.is_null()) {
        let Some(map) = raw.as_object() else {
            return Err(
                "'nodeMapping' must be an object of {sourceNodeId: targetNodeId}".to_owned(),
            );
        };
        for (from, to) in map {
            let Some(to) = to.as_str() else {
                return Err(format!("nodeMapping['{from}'] must be a string node id"));
            };
            plan.mapping.insert(from.clone(), to.to_owned());
        }
    }
    if let Some(raw) = body.get("targetProcessId").filter(|v| !v.is_null()) {
        let Some(process_id) = raw.as_str().filter(|p| !p.trim().is_empty()) else {
            return Err("'targetProcessId' must be a non-empty string process id".to_owned());
        };
        plan.target_process_id = Some(process_id.trim().to_owned());
    }
    Ok(plan)
}

/// Everything one migrate call resolves ONCE, then applies to each instance it touches.
struct MigrateContext<'a> {
    state: &'a AppState,
    pool: PgPool,
    /// The ACTIVE deployment being migrated onto (already checked).
    target: PersistDeploymentId,
    /// The target deployment's published node index.
    target_index: Arc<crate::migrate::DeploymentNodeIndex>,
    plan: &'a crate::migrate::MigrationPlan,
}

/// Migrate exactly ONE instance and report what happened to it — the whole operation, for both
/// endpoints. Never returns an HTTP response and never propagates an error: the verdict IS the
/// return value, which is what lets a batch keep going past a refusal, a bounce or a fault.
///
/// ## Ordering, and why it is this order
///
/// 1. **Locate the instance's pin.** `source_hint` short-circuits the walk when the caller already
///    knows it (a batch names one source deployment by definition).
/// 2. **Claim it** (real runs only — a dry run mutates nothing, and a claim is a mutation).
///    Contention is retry-safe and reported as such: nothing is read, rewritten or committed.
/// 3. **Validate every locus**, collecting EVERY violation. An invalid migration releases the claim
///    and reports the whole report — fixing a mapping should take one round trip, not one per node.
/// 4. **Commit the move** in the single two-scope transaction
///    ([`sutra_persistence::step::commit_instance_migration`]), which re-asserts the claim under the
///    row lock before it writes anything. With `resume`, the failure-state clear and the park re-arm
///    ride THAT transaction, so migrate-then-resume is one commit and a crash cannot leave a
///    half-revived instance.
async fn migrate_one(
    ctx: &MigrateContext<'_>,
    instance_id: Uuid,
    source_hint: Option<PersistDeploymentId>,
) -> crate::migrate::InstanceAttempt {
    use crate::migrate::MigrationOutcome;
    use sutra_persistence::stores::{AuditEventRow, InstanceStore, PgOutboxStore};

    let id = instance_id.to_string();
    let plan = ctx.plan;
    let dry_run = plan.dry_run;
    let pool = ctx.pool.clone();
    let store = PgInstanceStore::new(pool.clone());
    let not_found = || {
        migrate_attempt(
            instance_id,
            MigrationOutcome::NotFound,
            serde_json::json!({ "instanceId": id, "found": false }),
        )
    };
    let fault = |detail: String| {
        migrate_attempt(
            instance_id,
            MigrationOutcome::Error,
            serde_json::json!({ "instanceId": id, "migrated": false, "error": detail }),
        )
    };

    // 1. The instance's own pin. A batch already knows it; the single endpoint walks the live set,
    //    the same way cancel and history do.
    let scopes: Vec<PersistDeploymentId> = match source_hint {
        Some(dep) => vec![dep],
        None => ctx
            .state
            .deployments
            .snapshot()
            .iter()
            .filter_map(|d| PersistDeploymentId::new(d.value()).ok())
            .collect(),
    };
    let mut owner = None;
    for dep in scopes {
        match store.load(&dep, instance_id).await {
            Ok(Some(row)) => {
                owner = Some((dep, row.serialised));
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(instance_id = %id, error = %e, "instance load failed during migrate");
                return fault(e.to_string());
            }
        }
    }
    let Some((source, serialised)) = owner else {
        return not_found();
    };
    let skeleton = serde_json::json!({
        "instanceId": id,
        "fromDeploymentId": source.as_str(),
        "toDeploymentId": ctx.target.as_str(),
        "dryRun": dry_run,
    });
    if source == ctx.target {
        return migrate_attempt(
            instance_id,
            MigrationOutcome::Refused,
            migrate_refusal_body(
                crate::migrate::MIGRATE_TARGET_SAME_AS_SOURCE,
                format!(
                    "instance {id} is already pinned to {} — a migration must name a different \
                     target",
                    ctx.target.as_str()
                ),
                skeleton,
            ),
        );
    }
    // The SOURCE graph is needed to classify a continue-reply park (a TIMER row whose node carries
    // `<q:reply continue>`); without it the validator would apply the timer rule to a locus that
    // resumes through the relay path. Fail closed rather than validate the wrong rule.
    let source_index = ctx
        .state
        .node_indexes
        .read()
        .ok()
        .and_then(|m| m.get(source.as_str()).cloned());
    let Some(source_index) = source_index else {
        return migrate_attempt(
            instance_id,
            MigrationOutcome::Refused,
            migrate_refusal_body(
                crate::migrate::MIGRATE_SOURCE_UNRESOLVABLE,
                format!(
                    "the instance is pinned to '{}', whose graph is not registered on this engine \
                     — the migration cannot be validated against a source model it cannot see",
                    source.as_str()
                ),
                skeleton,
            ),
        );
    };

    // 2. Claim — real runs only. A dry run mutates NOTHING, and a claim is a mutation of the row.
    let claim_owner = migrate_claim_owner();
    if !dry_run {
        match store.claim(&source, instance_id, &claim_owner).await {
            Ok(true) => {}
            Ok(false) => {
                // The CAS cannot tell "held by another" from "no row"; re-read to say which.
                match store.load(&source, instance_id).await {
                    Ok(None) => return not_found(),
                    _ => {
                        return migrate_attempt(
                            instance_id,
                            MigrationOutcome::Bounced,
                            migrate_refusal_body(
                                crate::migrate::MIGRATE_CLAIM_HELD,
                                format!(
                                    "instance {id} is claimed by another owner (a resume in flight \
                                     on this or another replica) — nothing was read, rewritten or \
                                     committed; retry once the claim clears \
                                     (sutra.instance.claim-timeout bounds it)"
                                ),
                                skeleton,
                            ),
                        );
                    }
                }
            }
            Err(e) => {
                warn!(instance_id = %id, error = %e, "instance claim failed during migrate");
                return fault(e.to_string());
            }
        }
    }
    // From here on every early return on a real run must hand the claim back.
    let release = |pool: PgPool, source: PersistDeploymentId, owner: String| async move {
        if let Err(e) = PgInstanceStore::new(pool)
            .release(&source, instance_id, &owner)
            .await
        {
            warn!(error = %e, "migrate claim release failed (the sweeper will clear it)");
        }
    };

    // 3. Assemble the durable facts. `peek_loci` reads the node ids WITHOUT decrypting a single
    //    variable — a structural operation has no business needing the tenant key.
    let loci_keys = match sutra_persistence::snapshot::InstanceSnapshot::peek_loci(&serialised) {
        Ok(keys) => keys,
        Err(e) => {
            if !dry_run {
                release(pool.clone(), source.clone(), claim_owner.clone()).await;
            }
            warn!(instance_id = %id, error = %e, "snapshot loci peek failed during migrate");
            return fault("snapshot decode failed".to_owned());
        }
    };
    let waits = PgWaitStateStore::new(pool.clone());
    let wait_rows = match waits.list_for_instance(&source, instance_id).await {
        Ok(rows) => rows,
        Err(e) => {
            if !dry_run {
                release(pool.clone(), source.clone(), claim_owner.clone()).await;
            }
            warn!(instance_id = %id, error = %e, "wait-row read failed during migrate");
            return fault(e.to_string());
        }
    };
    let pending_outbox = PgOutboxStore::new(pool.clone())
        .count_pending_for_instance(&source, instance_id)
        .await
        .unwrap_or(0)
        .max(0) as u64;

    let facts = crate::migrate::InstanceFacts {
        process_id: loci_keys.process_id.clone(),
        status: loci_keys.status.clone(),
        waiting_nodes: loci_keys.waiting_nodes.clone(),
        completed_nodes: loci_keys.completed_nodes.clone(),
        start_node: loci_keys.start_node.clone(),
        retry_nodes: loci_keys.retry_nodes.clone(),
        // Not simply "the WAITING rows": a FAILED instance has none (its failure commit resolved
        // them all), and classifying its parks from the frontier alone would apply the message-wait
        // rule to a timer park. See [`crate::migrate::live_park_rows`].
        wait_rows: crate::migrate::live_park_rows(
            &loci_keys.status,
            &loci_keys.waiting_nodes,
            &wait_rows,
        ),
        pending_outbox,
    };
    // Cross-process migration validates against the TARGET process's graph — same matrix, different
    // index. A target that does not declare it at all is one decisive PROCESS_ABSENT refusal.
    let target_process_id = plan.target_process_for(&facts.process_id).to_owned();
    let report = crate::migrate::validate(
        &facts,
        source_index.process(&facts.process_id),
        ctx.target_index.process(&target_process_id),
        &ctx.target_index.process_ids(),
        plan,
    );

    let cross_process = plan.is_cross_process(&facts.process_id);
    let mut response = serde_json::json!({
        "instanceId": id,
        "fromDeploymentId": source.as_str(),
        "toDeploymentId": ctx.target.as_str(),
        "processId": facts.process_id,
        "fromProcessId": facts.process_id,
        "toProcessId": target_process_id,
        "crossProcess": cross_process,
        "status": facts.status,
        "dryRun": dry_run,
        "resumeRequested": plan.resume,
        "valid": report.valid(),
        "migrated": false,
        "resumed": false,
        "mappingSource": plan.mapping_source(),
        "mapping": report.effective_mapping.clone(),
        "loci": report.loci_json(),
        "violations": report.violations.iter().map(crate::migrate::Finding::to_json).collect::<Vec<_>>(),
        "warnings": report.warnings.iter().map(crate::migrate::Finding::to_json).collect::<Vec<_>>(),
    });

    if !report.valid() {
        if !dry_run {
            release(pool.clone(), source.clone(), claim_owner.clone()).await;
        }
        return migrate_attempt(instance_id, MigrationOutcome::Refused, response);
    }
    if dry_run {
        if let Some(map) = response.as_object_mut() {
            map.insert(
                "note".to_owned(),
                serde_json::Value::String(
                    "dry run — nothing was claimed, rewritten or committed. The instance was not \
                     locked while this report was produced, so it is advisory: a concurrent resume \
                     can move the frontier out from under it."
                        .to_owned(),
                ),
            );
        }
        return migrate_attempt(instance_id, MigrationOutcome::DryRunValid, response);
    }

    // 4. Commit. The migration's own audit row takes the next per-instance seq, and the snapshot's
    //    `sutra.auditSeq` is bumped past it so the instance's next event cannot collide with it.
    let migrate_seq = loci_keys.audit_seq.saturating_add(1);
    let repinned = sutra_persistence::snapshot::InstanceSnapshot::migrate_pinned(
        &serialised,
        ctx.target.as_str(),
        cross_process.then_some(target_process_id.as_str()),
        &report.effective_mapping,
        Some(migrate_seq),
    );
    let migrated_snapshot = match repinned {
        Ok(bytes) => bytes,
        Err(e) => {
            release(pool.clone(), source.clone(), claim_owner.clone()).await;
            return migrate_attempt(
                instance_id,
                MigrationOutcome::Refused,
                migrate_refusal_body(crate::migrate::MIGRATE_INSTANCE_TERMINAL, e, response),
            );
        }
    };
    // `resume` (validated above as FAILED-only) clears the failure state in the SAME bytes the move
    // writes, and hands the commit the mapped frontier so the parks the failure tore down are
    // re-armed in the same transaction. The instance therefore reappears as an ordinary parked
    // instance — resumed by the timer poller or by the next correlated inbound, through the
    // ordinary claim-guarded paths. There is no new resume entry point.
    let migrated_snapshot = if plan.resume {
        match sutra_persistence::snapshot::InstanceSnapshot::resume_from_failed(&migrated_snapshot)
        {
            Ok(bytes) => bytes,
            Err(e) => {
                release(pool.clone(), source.clone(), claim_owner.clone()).await;
                return migrate_attempt(
                    instance_id,
                    MigrationOutcome::Refused,
                    migrate_refusal_body(crate::migrate::MIGRATE_RESUME_NOT_FAILED, e, response),
                );
            }
        }
    } else {
        migrated_snapshot
    };
    let rearm_parks = plan.resume.then(|| {
        facts
            .waiting_nodes
            .iter()
            .map(|node| {
                report
                    .effective_mapping
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.clone())
            })
            .collect::<std::collections::BTreeSet<String>>()
    });
    let audit = AuditEventRow {
        deployment: ctx.target.clone(),
        instance_id: Some(instance_id),
        seq: i32::try_from(migrate_seq).unwrap_or(i32::MAX),
        at: time::OffsetDateTime::now_utc(),
        event_type: "SUTRA.INSTANCE_MIGRATED".to_owned(),
        node_id: None,
        diagnostic_code: None,
        // Metadata only — node ids, process ids and deployment ids, never a variable value.
        diagnostic_json: Some(
            serde_json::json!({
                "fromDeploymentId": source.as_str(),
                "toDeploymentId": ctx.target.as_str(),
                "fromProcessId": facts.process_id,
                "toProcessId": target_process_id,
                "processId": facts.process_id,
                "mapping": report.effective_mapping,
                "mappingSource": plan.mapping_source(),
                "resumed": plan.resume,
            })
            .to_string(),
        ),
        payload_json: "{}".to_owned(),
    };
    let migration = sutra_persistence::step::InstanceMigration {
        from: source.clone(),
        to: ctx.target.clone(),
        instance_id,
        snapshot: migrated_snapshot,
        node_mapping: report.effective_mapping.clone(),
        process_id: cross_process.then(|| target_process_id.clone()),
        rearm_parks,
        claim_owner: claim_owner.clone(),
        // Always carry the journal: the history endpoint resolves scope from the row's owning
        // deployment, so a trail left behind would be silently unreachable.
        carry_journal: true,
        audit: Some(audit),
    };
    match sutra_persistence::step::commit_instance_migration(&pool, &migration).await {
        Ok(outcome) => {
            info!(
                instance_id = %id,
                from = source.as_str(),
                to = ctx.target.as_str(),
                from_process = facts.process_id,
                to_process = target_process_id,
                wait_rows = outcome.wait_rows,
                alias_rows = outcome.alias_rows,
                subject_rows = outcome.subject_rows,
                audit_rows = outcome.audit_rows,
                rearmed = outcome.rearmed_rows,
                resumed = plan.resume,
                "instance MIGRATED (snapshot re-pinned + node ids rewritten + waits/aliases/\
                 subjects/journal moved in one two-scope transaction)"
            );
            if let Some(map) = response.as_object_mut() {
                map.insert("migrated".to_owned(), serde_json::Value::Bool(true));
                map.insert(
                    "rewrites".to_owned(),
                    serde_json::json!({
                        "waitRows": outcome.wait_rows,
                        "aliasRows": outcome.alias_rows,
                        "subjectRows": outcome.subject_rows,
                        "auditRows": outcome.audit_rows,
                        "rearmedParks": outcome.rearmed_rows,
                        "outboxRows": 0,
                    }),
                );
                map.insert("auditSeq".to_owned(), serde_json::Value::from(migrate_seq));
                map.insert("resumed".to_owned(), serde_json::Value::Bool(plan.resume));
            }
            migrate_attempt(instance_id, MigrationOutcome::Migrated, response)
        }
        Err(sutra_persistence::PersistenceError::AliasCollision {
            alias_name,
            alias_value,
            ..
        }) => {
            release(pool.clone(), source.clone(), claim_owner.clone()).await;
            migrate_attempt(
                instance_id,
                MigrationOutcome::Bounced,
                migrate_refusal_body(
                    crate::migrate::MIGRATE_ALIAS_CONFLICT,
                    format!(
                        "<q:alias {alias_name}> = '{alias_value}' is already bound to a different \
                         live instance under {} — the whole migration was rolled back",
                        ctx.target.as_str()
                    ),
                    response,
                ),
            )
        }
        Err(e) => {
            release(pool.clone(), source.clone(), claim_owner.clone()).await;
            warn!(instance_id = %id, error = %e, "instance migration commit failed");
            migrate_attempt(
                instance_id,
                MigrationOutcome::Bounced,
                migrate_refusal_body(
                    crate::migrate::MIGRATE_COMMIT_FAILED,
                    format!(
                        "the migration validated but its transaction did not commit — nothing \
                         moved: {e}"
                    ),
                    response,
                ),
            )
        }
    }
}

/// Resolve the migration target ONCE: it must be an ACTIVE, registered deployment with a published
/// node index. `Err` is the refusal both endpoints answer with.
fn resolve_migration_target(
    state: &AppState,
    target: &PersistDeploymentId,
    skeleton: serde_json::Value,
) -> Result<Arc<crate::migrate::DeploymentNodeIndex>, Box<axum::response::Response>> {
    let active = state
        .deploy_status
        .read()
        .map(|s| s.active.iter().any(|(dep, _)| dep == target.as_str()))
        .unwrap_or(false);
    let index = state
        .node_indexes
        .read()
        .ok()
        .and_then(|m| m.get(target.as_str()).cloned());
    match index {
        Some(index) if active => Ok(index),
        // Boxed: an axum `Response` is a large `Err` variant to carry around by value.
        _ => Err(Box::new(migrate_refusal(
            StatusCode::UNPROCESSABLE_ENTITY,
            crate::migrate::MIGRATE_TARGET_NOT_ACTIVE,
            format!(
                "deployment '{}' is not an ACTIVE, registered deployment on this engine — a \
                 migration target must be serving intake (DRAINING deployments retire as soon as \
                 they are quiescent, so migrating onto one would strand the instance again)",
                target.as_str()
            ),
            skeleton,
        ))),
    }
}

/// `POST /admin/instances/{id}/migrate` — re-pin one live instance onto another ACTIVE
/// deployment, rewriting every node id its durable state names.
///
/// Body: `{ targetDeploymentId, targetProcessId?, nodeMapping?, dryRun?, resume? }`. The response
/// is ALWAYS the full validation report — on a dry run, on a refusal and on a success alike — so
/// the same document an operator inspects before migrating is the one they get back afterwards.
///
/// ## Terminal, FAILED, and `resume`
///
/// COMPLETED / TERMINATED are a validation error: they are retained history, and re-pinning history
/// would rewrite the record of where it ran. FAILED is ALLOWED and is the prime use case — repair
/// the model, migrate the dead instance onto it, then decide what to do. Migration on its own still
/// does NOT resume anything: `resume: true` is the explicit opt-in that clears the failure state and
/// re-arms the instance's parks inside the migration's own transaction, and it is a validation error
/// on any instance that is not FAILED (a suspended instance is not stuck — it resumes by
/// correlation or by its timers, with no operator action at all).
pub(crate) async fn instance_migrate_response(
    state: &AppState,
    id: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    let Some(instance_id) = parse_uuid(id) else {
        return bad_uuid(id).into_response();
    };
    let bad_request = |error: String| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "instanceId": id, "error": error })),
        )
            .into_response()
    };
    let Some(target_raw) = body.get("targetDeploymentId").and_then(|v| v.as_str()) else {
        return bad_request("'targetDeploymentId' (string) is required".to_owned());
    };
    let plan = match parse_migration_plan(&body) {
        Ok(plan) => plan,
        Err(e) => return bad_request(e),
    };
    let target = match PersistDeploymentId::new(target_raw.to_owned()) {
        Ok(dep) => dep,
        Err(e) => return bad_request(format!("invalid targetDeploymentId '{target_raw}': {e}")),
    };
    let Some(pool) = state.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "instanceId": id,
                "error": "instance migration requires persistence (no datasource configured)",
            })),
        )
            .into_response();
    };

    let skeleton = serde_json::json!({
        "instanceId": id,
        "toDeploymentId": target.as_str(),
        "dryRun": plan.dry_run,
    });
    let target_index = match resolve_migration_target(state, &target, skeleton) {
        Ok(index) => index,
        Err(refusal) => return *refusal,
    };

    let ctx = MigrateContext {
        state,
        pool,
        target,
        target_index,
        plan: &plan,
    };
    let attempt = migrate_one(&ctx, instance_id, None).await;
    let status = StatusCode::from_u16(attempt.outcome.http_status()).unwrap_or(StatusCode::OK);
    let mut body = attempt.report;
    if attempt.outcome.retry_safe() {
        if let Some(map) = body.as_object_mut() {
            map.insert("retrySafe".to_owned(), serde_json::Value::Bool(true));
        }
    }
    (status, Json(body)).into_response()
}

/// `POST /admin/instances/migrate` — migrate a FILTERED POPULATION off one deployment pin (F2).
///
/// Body: `{ targetDeploymentId, filter: { sourceDeploymentId, processId?, status?, includeTerminal?,
/// limit? }, targetProcessId?, nodeMapping?, dryRun?, resume? }`.
///
/// ## The partial-failure contract
///
/// Every selected instance is validated, claimed, moved and reported ON ITS OWN — its own claim,
/// its own transaction, its own entry in `instances[]`. Nothing about one instance can decide
/// another's fate, and a mid-batch crash therefore leaves every instance either FULLY migrated or
/// completely untouched: there is no batch-wide transaction that could be half-applied, because
/// there is no batch-wide transaction at all.
///
/// The response is `200` whenever the batch itself ran — including a batch in which every instance
/// refused. The HTTP status describes the CALL (accepted, executed, reported in full); the per
/// instance verdicts are data. Callers key on `totals` and on each entry's `outcome`.
///
/// ## Contention is reported, never retried
///
/// An instance whose ownership claim is held bounces (`outcome: BOUNCED`, retry-safe) and the batch
/// moves on. It is deliberately NOT retried inside the call: a hidden retry loop on an admin surface
/// makes the runtime unbounded and turns the report into a claim about a moment that has already
/// passed. The caller re-runs the same request — the selector is deterministic, and whatever moved
/// is no longer under the source pin, so a re-run converges.
///
/// ## Selection
///
/// `filter.sourceDeploymentId` is required: one migration names one source graph and one target
/// graph, and a node mapping that is right for one source is meaningless for another. Order is by
/// instance id (stable across runs — `updated_at` is not, and paging a moving list is how a caller
/// silently skips work), then `limit` is applied. Retained TERMINAL rows are excluded unless
/// `includeTerminal` asks for them, in which case they are reported as explicit `INSTANCE_TERMINAL`
/// refusals rather than silently omitted.
pub(crate) async fn instances_migrate_batch_response(
    state: &AppState,
    body: serde_json::Value,
) -> axum::response::Response {
    use sutra_persistence::stores::{InstanceFilter, InstanceStore};

    let bad_request = |error: String| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response()
    };
    let Some(target_raw) = body.get("targetDeploymentId").and_then(|v| v.as_str()) else {
        return bad_request("'targetDeploymentId' (string) is required".to_owned());
    };
    let target = match PersistDeploymentId::new(target_raw.to_owned()) {
        Ok(dep) => dep,
        Err(e) => return bad_request(format!("invalid targetDeploymentId '{target_raw}': {e}")),
    };
    let plan = match parse_migration_plan(&body) {
        Ok(plan) => plan,
        Err(e) => return bad_request(e),
    };
    let filter = match parse_batch_filter(body.get("filter"), &plan) {
        Ok(filter) => filter,
        Err(e) => return bad_request(e),
    };
    let source = match PersistDeploymentId::new(filter.source_deployment_id.clone()) {
        Ok(dep) => dep,
        Err(e) => {
            return bad_request(format!(
                "invalid filter.sourceDeploymentId '{}': {e}",
                filter.source_deployment_id
            ))
        }
    };
    let Some(pool) = state.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "instance migration requires persistence (no datasource configured)",
            })),
        )
            .into_response();
    };
    let skeleton = serde_json::json!({
        "toDeploymentId": target.as_str(),
        "filter": filter.to_json(),
        "dryRun": plan.dry_run,
    });
    if source == target {
        return migrate_refusal(
            StatusCode::UNPROCESSABLE_ENTITY,
            crate::migrate::MIGRATE_TARGET_SAME_AS_SOURCE,
            format!(
                "the filter's source and the target are both {} — a migration must name a \
                 different target",
                target.as_str()
            ),
            skeleton,
        );
    }
    let target_index = match resolve_migration_target(state, &target, skeleton) {
        Ok(index) => index,
        Err(refusal) => return *refusal,
    };

    // Selection. The status filter is a snapshot field rather than a column, so the store applies
    // it after decode; the process filter costs one read per candidate and is therefore only paid
    // when it was asked for.
    let store = PgInstanceStore::new(pool.clone());
    let listing = InstanceFilter {
        status: filter.status.clone(),
        include_terminal: filter.include_terminal,
    };
    let mut summaries = match store.list(&source, &listing).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "batch migrate selection failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };
    summaries.sort_by_key(|row| row.instance_id);
    let mut selected: Vec<Uuid> = Vec::new();
    for summary in summaries {
        if selected.len() as i64 >= filter.limit {
            break;
        }
        if let Some(want) = &filter.process_id {
            // The process id lives INSIDE the snapshot, so narrowing by it costs a read.
            let matches = match store.load(&source, summary.instance_id).await {
                Ok(Some(row)) => {
                    sutra_persistence::snapshot::InstanceSnapshot::peek(&row.serialised)
                        .map(|keys| keys.process_id == *want)
                        .unwrap_or(false)
                }
                Ok(None) => false,
                Err(e) => {
                    warn!(error = %e, "batch migrate candidate read failed");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            };
            if !matches {
                continue;
            }
        }
        selected.push(summary.instance_id);
    }

    let ctx = MigrateContext {
        state,
        pool,
        target: target.clone(),
        target_index,
        plan: &plan,
    };
    // The loop IS the partial-failure contract: one instance per iteration, one verdict per
    // instance, and no way for one to end the run. Sequential on purpose — a batch that fanned out
    // would multiply its claim contention against the very resume paths it is racing.
    let mut attempts = Vec::with_capacity(selected.len());
    for instance_id in selected {
        attempts.push(migrate_one(&ctx, instance_id, Some(source.clone())).await);
    }
    let report = crate::migrate::batch_report(&filter, target.as_str(), &plan, &attempts);
    info!(
        from = source.as_str(),
        to = target.as_str(),
        selected = attempts.len(),
        dry_run = plan.dry_run,
        resume = plan.resume,
        "batch instance migration completed (per-instance claims + transactions)"
    );
    (StatusCode::OK, Json(report)).into_response()
}

/// Parse + check the batch selector. Every refusal here is decided from the REQUEST alone, so it is
/// a `400` that never depends on a database being reachable.
fn parse_batch_filter(
    raw: Option<&serde_json::Value>,
    plan: &crate::migrate::MigrationPlan,
) -> Result<crate::migrate::BatchFilter, String> {
    use sutra_persistence::snapshot::{STATUS_FAILED, STATUS_SUSPENDED};

    let Some(raw) = raw.filter(|v| !v.is_null()) else {
        return Err(
            "'filter' (object) is required, and must name 'sourceDeploymentId' — a batch migration \
             moves instances off ONE pin, and a node mapping that is right for one source graph is \
             meaningless for another"
                .to_owned(),
        );
    };
    let Some(object) = raw.as_object() else {
        return Err("'filter' must be an object".to_owned());
    };
    let Some(source) = object
        .get("sourceDeploymentId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    else {
        return Err("'filter.sourceDeploymentId' (string) is required".to_owned());
    };
    let status = match object.get("status").filter(|v| !v.is_null()) {
        None => None,
        Some(value) => {
            let Some(status) = value.as_str() else {
                return Err("'filter.status' must be a string".to_owned());
            };
            if status != STATUS_SUSPENDED && status != STATUS_FAILED {
                return Err(format!(
                    "'filter.status' must be {STATUS_SUSPENDED} or {STATUS_FAILED}, not \
                     '{status}' — those are the only states an instance can be migrated in \
                     (a finished instance is retained history)"
                ));
            }
            Some(status.to_owned())
        }
    };
    let process_id = match object.get("processId").filter(|v| !v.is_null()) {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .filter(|p| !p.trim().is_empty())
                .ok_or_else(|| "'filter.processId' must be a non-empty string".to_owned())?
                .trim()
                .to_owned(),
        ),
    };
    let limit = match object.get("limit").filter(|v| !v.is_null()) {
        None => crate::migrate::BATCH_LIMIT_DEFAULT,
        Some(value) => value
            .as_i64()
            .filter(|n| *n > 0)
            .ok_or_else(|| "'filter.limit' must be a positive integer".to_owned())?
            .min(crate::migrate::BATCH_LIMIT_MAX),
    };
    let include_terminal = object
        .get("includeTerminal")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    // Two contradictions worth catching from the request alone rather than N times over in the
    // per-instance reports:
    if plan.resume && status.as_deref() == Some(STATUS_SUSPENDED) {
        return Err(format!(
            "'resume' with filter.status={STATUS_SUSPENDED} selects exactly the instances resume \
             refuses — only a FAILED instance has failure state to clear; a suspended one resumes \
             by correlation or by its timers"
        ));
    }
    if plan.target_process_id.is_some() && process_id.is_none() {
        return Err(
            "'targetProcessId' (a cross-process re-home) requires 'filter.processId' — re-homing a \
             MIXED population into one process is never what a caller means, and the node mapping \
             could only be right for one of them"
                .to_owned(),
        );
    }

    Ok(crate::migrate::BatchFilter {
        source_deployment_id: source.trim().to_owned(),
        process_id,
        status,
        include_terminal,
        limit,
    })
}

// ---- instance execution history (the audit journal, read side) ---------------------------
//
// The engine has written a per-token-move journal to `audit_event` since V201 and has never had a
// way to READ it back; P1-2 closes that. ADMIN-ONLY by construction: an audit row carries the
// captured business payload, exactly the posture a dead letter's bytes get, so there is
// deliberately no `/sutra/*` twin of this route.

/// One journal row as JSON. `payload`/`diagnostic` are rendered as parsed JSON when they hold
/// valid JSON (which is what the sink writes) and as a plain string otherwise, so a malformed or
/// hand-edited row degrades to something readable instead of failing the whole page.
fn audit_event_json(record: &AuditEventRecord) -> serde_json::Value {
    let rfc3339 = &time::format_description::well_known::Rfc3339;
    let as_json = |raw: &str| -> serde_json::Value {
        serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()))
    };
    serde_json::json!({
        "seq": record.seq,
        "at": record.at.format(rfc3339).unwrap_or_default(),
        "eventType": record.event_type,
        "nodeId": record.node_id,
        "diagnosticCode": record.diagnostic_code,
        "diagnostic": record.diagnostic_json.as_deref().map(as_json),
        "payload": as_json(&record.payload_json),
    })
}

/// `?afterSeq=` (exclusive cursor, default 0 — seqs start at 1) and `?limit=` clamped by the
/// store's page ceiling. A cursor rather than an offset because the journal of a still-running
/// instance grows while it is being paged, and an offset page would then skip or repeat rows.
fn history_paging(params: &HashMap<String, String>) -> (i32, i64) {
    let after_seq = params
        .get("afterSeq")
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0)
        .max(0);
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<i64>().ok())
        .unwrap_or(AUDIT_HISTORY_PAGE_DEFAULT)
        .clamp(1, AUDIT_HISTORY_PAGE_MAX);
    (after_seq, limit)
}

/// `GET /admin/instances/{id}/history` — one seq-ordered page of an instance's audit journal.
///
/// This is the surface that makes "what did this instance actually do?" answerable. The retained
/// terminal snapshot (`GET /admin/instances/{id}`) says where an instance ended and with what
/// variables it was last durably parked; the journal says how it got there, one token move at a
/// time, with whatever payload the process chose to capture.
///
/// **The journal stays OPT-IN — this route only reads it.** It is enabled twice over: engine-side
/// by `sutra.audit.sql`, and per-process by `<q:audit>`. An instance with either switch off has no
/// rows, and an empty page therefore means "nothing was recorded", never "the history was lost".
/// Because those are very different facts for an operator staring at an empty list, the response
/// always carries `auditEnabled`, and an empty page carries a `note` naming the reason.
///
/// **Scope resolution outlives the instance.** The journal is NOT purged by
/// `sutra.instance.retention` (see [`crate::sweeper::TerminalRetentionSweeper`]), so an instance
/// whose snapshot has already been purged still has a readable history. The owning deployment is
/// therefore resolved from the instance row when one still exists, and otherwise by scanning the
/// live set for journal rows — a `404` is reserved for an id that neither owns a row nor appears
/// in any journal.
pub(crate) async fn instance_history_response(
    state: &AppState,
    id: &str,
    params: &HashMap<String, String>,
) -> axum::response::Response {
    let Some(instance_id) = parse_uuid(id) else {
        return bad_uuid(id).into_response();
    };
    let (after_seq, limit) = history_paging(params);
    let Some(pool) = state.pool.clone() else {
        return instance_not_found(id).into_response();
    };
    let instances = PgInstanceStore::new(pool.clone());
    let audit = PgAuditEventStore::new(pool);

    // Prefer the deployment that still owns the instance row; fall back to the whole live set for
    // an instance whose snapshot was already purged.
    let mut owner = None;
    for dep in state.deployments.snapshot() {
        let Ok(pdep) = PersistDeploymentId::new(dep.value()) else {
            continue;
        };
        match instances.load(&pdep, instance_id).await {
            Ok(Some(_)) => {
                owner = Some(pdep);
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(instance_id = %id, error = %e, "instance load failed during history read");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "instanceId": id, "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    let scopes: Vec<PersistDeploymentId> = match &owner {
        Some(dep) => vec![dep.clone()],
        None => state
            .deployments
            .snapshot()
            .iter()
            .filter_map(|d| PersistDeploymentId::new(d.value()).ok())
            .collect(),
    };

    let mut found: Option<(PersistDeploymentId, Vec<AuditEventRecord>)> = None;
    for dep in &scopes {
        match audit
            .list_for_instance(dep, instance_id, after_seq, limit)
            .await
        {
            Ok(rows) if !rows.is_empty() => {
                found = Some((dep.clone(), rows));
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                warn!(deployment = dep.as_str(), error = %e, "instance history read failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "instanceId": id, "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }

    let (deployment, records) = match found {
        Some((dep, rows)) => (Some(dep), rows),
        // No journal rows anywhere. If nothing owns the instance either, the id is simply unknown
        // — that is a genuine 404, not an empty history.
        None if owner.is_none() => return instance_not_found(id).into_response(),
        None => (owner, Vec::new()),
    };

    let events: Vec<serde_json::Value> = records.iter().map(audit_event_json).collect();
    // A FULL page means there may be more; a short one is the end of the journal.
    let next_after_seq = (records.len() as i64 == limit)
        .then(|| records.last().map(|r| r.seq))
        .flatten();
    let mut body = serde_json::json!({
        "instanceId": id,
        "deploymentId": deployment.as_ref().map(|d| d.as_str().to_owned()),
        "events": events,
        "afterSeq": after_seq,
        "limit": limit,
        "nextAfterSeq": next_after_seq,
        "auditEnabled": state.audit_sql_enabled,
    });
    if records.is_empty() {
        let note = if state.audit_sql_enabled {
            "no audit events are recorded for this instance — the durable journal is enabled \
             engine-wide (sutra.audit.sql), so either this process declares no <q:audit>, or the \
             requested page starts past the end of its journal"
        } else {
            "the durable audit journal is DISABLED (sutra.audit.sql is not set), so no execution \
             history was ever recorded for this instance — this is an empty journal, not a lost one"
        };
        if let Some(map) = body.as_object_mut() {
            map.insert(
                "note".to_owned(),
                serde_json::Value::String(note.to_owned()),
            );
        }
    }
    (StatusCode::OK, Json(body)).into_response()
}

// ---- dead-letter (DLQ) admin surface -----------------------------------------------------
//
// Read + redrive for the `dead_letter` table. ADMIN-ONLY by construction: these helpers are
// reachable exclusively from the gated `/admin/dead-letters…` routes, never from a `/sutra/*`
// operate route, because a dead letter holds the raw business payload that failed. The projection
// below carries the payload's LENGTH and never its bytes — the bytes leave the store on exactly
// one path, the replay handler, which feeds them straight back into intake.

/// The deployments a dead-letter read applies to: an explicit `?deploymentId=` (the paged,
/// precise form) or, absent one, every live deployment (the fan-out convenience form, mirroring
/// how the instance list walks the live set). An invalid explicit id is a 400.
fn dead_letter_scopes(
    state: &AppState,
    params: &HashMap<String, String>,
) -> Result<Vec<PersistDeploymentId>, Box<axum::response::Response>> {
    match params.get("deploymentId").filter(|d| !d.is_empty()) {
        Some(dep) => match PersistDeploymentId::new(dep.clone()) {
            Ok(dep) => Ok(vec![dep]),
            // Boxed: an axum Response is a large value and this is the cold, error-only arm.
            Err(e) => Err(Box::new(
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response(),
            )),
        },
        None => Ok(state
            .deployments
            .snapshot()
            .iter()
            .filter_map(|d| PersistDeploymentId::new(d.value()).ok())
            .collect()),
    }
}

/// `?limit=` clamped by the store's own page ceiling, and `?offset=`. Offset is only meaningful
/// together with an explicit `?deploymentId=`: the fan-out form applies it per deployment before
/// merging, which is why the paged form is the one an operator should page with.
fn dead_letter_paging(params: &HashMap<String, String>) -> (i64, i64) {
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<i64>().ok())
        .unwrap_or(DEAD_LETTER_PAGE_DEFAULT)
        .clamp(1, DEAD_LETTER_PAGE_MAX);
    let offset = params
        .get("offset")
        .and_then(|o| o.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    (limit, offset)
}

/// One dead letter as JSON. `payloadBytes`/`replayable` are the payload's whole visible surface —
/// the captured business bytes are never rendered here.
fn dead_letter_json(record: &sutra_persistence::stores::DeadLetterRecord) -> serde_json::Value {
    let rfc3339 = &time::format_description::well_known::Rfc3339;
    serde_json::json!({
        "id": record.id,
        "deploymentId": record.deployment.as_str(),
        "channel": record.channel,
        "processId": record.process_id,
        "dedupKey": record.dedup_key,
        "failureCode": record.failure_code,
        "detail": record.detail,
        "receivedAt": record.received_at.format(rfc3339).unwrap_or_default(),
        "recordedAt": record.recorded_at.format(rfc3339).unwrap_or_default(),
        "payloadBytes": record.payload_bytes,
        "contentType": record.content_type,
        "tenant": record.tenant,
        "moduleKey": record.module_key,
        // What an operator actually wants to know before clicking redrive.
        "replayable": record.payload_bytes.is_some()
            && !record.module_key.is_empty()
            && !record.tenant.is_empty(),
    })
}

/// `GET /admin/dead-letters` — one page of dead letters, NEWEST FIRST. See
/// [`dead_letter_scopes`] / [`dead_letter_paging`] for the query semantics. A persistence-less
/// engine reports an empty set (there is no table to read).
pub(crate) async fn dead_letters_list_response(
    state: &AppState,
    params: &HashMap<String, String>,
) -> axum::response::Response {
    // Query validation comes FIRST: a malformed deploymentId is a 400 whatever the engine's
    // persistence posture is (an engine without a pool must not answer it "empty, all good").
    let scopes = match dead_letter_scopes(state, params) {
        Ok(scopes) => scopes,
        Err(response) => return *response,
    };
    let (limit, offset) = dead_letter_paging(params);
    let Some(pool) = state.pool.clone() else {
        return Json(serde_json::json!({ "deadLetters": [] })).into_response();
    };
    let store = PgDeadLetterStore::new(pool);
    let mut records = Vec::new();
    for dep in &scopes {
        match store.list(dep, limit, offset).await {
            Ok(rows) => records.extend(rows),
            Err(e) => {
                warn!(deployment = dep.as_str(), error = %e, "dead-letter list failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    // Merge the fan-out into one newest-first page (a single-scope read is already sorted).
    records.sort_by(|a, b| b.recorded_at.cmp(&a.recorded_at).then(b.id.cmp(&a.id)));
    records.truncate(limit as usize);
    Json(serde_json::json!({
        "deadLetters": records.iter().map(dead_letter_json).collect::<Vec<_>>(),
        "limit": limit,
        "offset": offset,
    }))
    .into_response()
}

/// `GET /admin/dead-letters/{id}` — one dead letter's metadata. Without `?deploymentId=` the live
/// set is walked (ids are per-database, so the first scope that owns the row answers).
pub(crate) async fn dead_letter_get_response(
    state: &AppState,
    id: &str,
    params: &HashMap<String, String>,
) -> axum::response::Response {
    let Some(row_id) = parse_dead_letter_id(id) else {
        return bad_dead_letter_id(id).into_response();
    };
    let scopes = match dead_letter_scopes(state, params) {
        Ok(scopes) => scopes,
        Err(response) => return *response,
    };
    let Some(pool) = state.pool.clone() else {
        return dead_letter_not_found(id).into_response();
    };
    let store = PgDeadLetterStore::new(pool);
    for dep in &scopes {
        match store.get(dep, row_id).await {
            Ok(Some(record)) => {
                return (StatusCode::OK, Json(dead_letter_json(&record))).into_response()
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(deployment = dep.as_str(), error = %e, "dead-letter get failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    dead_letter_not_found(id).into_response()
}

/// `POST /admin/dead-letters/{id}/replay` — redrive one dead letter through the NORMAL intake
/// path.
///
/// The redrive is a genuinely fresh delivery, not a replay hook: the captured payload, headers,
/// content type, tenant and module key are handed to the same `ChannelEngine::dispatch` a
/// transport calls, with a **newly minted event id** so inbox dedup treats it as new work rather
/// than swallowing it as a duplicate of the arrival that died. Everything intake does — auth is
/// already past (this is the admin surface), quotas, the feature gate, payload cap, codec decode,
/// handler resolution — runs again, unchanged and unskipped.
///
/// Fails CLOSED and structurally, never by inventing a delivery: `404` no such row, `422` the row
/// carries no captured payload or no routing keys (pre-capture rows and outbound `required`
/// incidents), `503` no engine/persistence wired, `500` the engine rejected the redrive (its
/// diagnostic code + message are returned verbatim).
pub(crate) async fn dead_letter_replay_response(
    state: &AppState,
    id: &str,
    params: &HashMap<String, String>,
) -> axum::response::Response {
    let Some(row_id) = parse_dead_letter_id(id) else {
        return bad_dead_letter_id(id).into_response();
    };
    let scopes = match dead_letter_scopes(state, params) {
        Ok(scopes) => scopes,
        Err(response) => return *response,
    };
    let (Some(pool), Some(engine)) = (state.pool.clone(), state.engine.clone()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "deadLetterId": id,
                "error": "replay unavailable",
                "reason": "the engine is running without persistence and/or without a dispatch \
                           handle — there is no dead-letter store to redrive from",
            })),
        )
            .into_response();
    };
    let store = PgDeadLetterStore::new(pool);
    let mut found = None;
    for dep in &scopes {
        match store.replay_payload(dep, row_id).await {
            Ok(Some(payload)) => {
                found = Some(payload);
                break;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!(deployment = dep.as_str(), error = %e, "dead-letter replay fetch failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    }
    let Some(replay) = found else {
        return dead_letter_not_found(id).into_response();
    };
    let Some(body) = replay.payload.clone() else {
        return not_replayable(
            id,
            "no payload was captured for this dead letter (it predates payload capture, or it is \
             an outbound required-delivery incident — redrive that from the outbox instead)",
        );
    };
    if replay.module_key.is_empty() || replay.tenant.is_empty() {
        return not_replayable(
            id,
            "no routing keys were captured for this dead letter (tenant/moduleKey are NULL), so \
             the engine cannot resolve the channel binding to deliver it to",
        );
    }
    // A FRESH event id: the original arrival's dedup key is already in `inbox_seen`, so reusing it
    // would make the redrive a silent no-op (Duplicate) — the one failure mode a replay must not
    // have.
    let event_id = Uuid::new_v4().to_string();
    let message = sutra_channels::InboundMessage {
        tenant: replay.tenant.clone(),
        module_key: replay.module_key.clone(),
        channel: replay.channel.clone(),
        headers: replay.headers.clone(),
        body: body.into(),
        content_type: replay.content_type.clone(),
        idempotency_key: event_id.clone(),
        explicit_event_id: true,
        received_at: now_rfc3339(),
        cloud_event: None,
    };
    info!(
        dead_letter_id = row_id,
        deployment = replay.deployment.as_str(),
        channel = %replay.channel,
        event_id = %event_id,
        "replaying a dead letter through the normal intake path (fresh event id)"
    );
    match engine.dispatch(message).await {
        Ok(outcome) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "deadLetterId": row_id,
                "deploymentId": replay.deployment.as_str(),
                "channel": replay.channel,
                "eventId": event_id,
                "replayed": true,
                "outcome": dispatch_outcome_label(&outcome),
            })),
        )
            .into_response(),
        Err(diagnostic) => {
            warn!(
                dead_letter_id = row_id,
                code = %diagnostic.code,
                error = %diagnostic.message,
                "dead-letter replay was rejected by the engine"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "deadLetterId": row_id,
                    "eventId": event_id,
                    "replayed": false,
                    "code": diagnostic.code,
                    "error": diagnostic.message,
                })),
            )
                .into_response()
        }
    }
}

/// The dispatch outcome as a stable label for the replay response (no payload, no reply body —
/// a redrive answers what HAPPENED, never what the flow produced).
fn dispatch_outcome_label(outcome: &sutra_channels::DispatchOutcome) -> serde_json::Value {
    match outcome {
        sutra_channels::DispatchOutcome::Completed { instance_id, .. } => serde_json::json!({
            "kind": "Completed", "instanceId": instance_id,
        }),
        sutra_channels::DispatchOutcome::Duplicate => serde_json::json!({ "kind": "Duplicate" }),
        sutra_channels::DispatchOutcome::DeadLettered {
            code, cause_code, ..
        } => serde_json::json!({
            "kind": "DeadLettered", "code": code, "causeCode": cause_code,
        }),
        // Unreachable: `EngineHandle::dispatch` consumes every shard handoff on the
        // router side before answering.
        sutra_channels::DispatchOutcome::Handoff { .. } => {
            serde_json::json!({ "kind": "Handoff" })
        }
    }
}

fn parse_dead_letter_id(id: &str) -> Option<i64> {
    id.parse::<i64>().ok().filter(|id| *id > 0)
}

fn bad_dead_letter_id(id: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "deadLetterId": id, "error": "dead-letter id is not a positive integer"
        })),
    )
}

fn dead_letter_not_found(id: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "deadLetterId": id, "found": false })),
    )
}

fn not_replayable(id: &str, reason: &str) -> axum::response::Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(serde_json::json!({
            "deadLetterId": id, "replayed": false, "error": "not replayable", "reason": reason,
        })),
    )
        .into_response()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// Project one decoded snapshot into the inspect response, replacing every `@sensitive`
/// variable's value with [`REDACTED_PLACEHOLDER`]. Pure over the decoded snapshot so the
/// security-critical redaction is unit-testable without a database.
fn inspect_projection(instance_id: Uuid, snapshot: &InstanceSnapshot) -> serde_json::Value {
    let sensitive: std::collections::BTreeSet<&str> =
        snapshot.sensitive().iter().map(String::as_str).collect();
    // `<v>.redacted` companions hold the DLP-masked projection intake stored; index them by their
    // base variable so the raw `<v>` is SHOWN AS its masked companion, never in the clear.
    let companions: std::collections::BTreeMap<&str, String> = snapshot
        .variables()
        .iter()
        .filter_map(|(name, value)| {
            name.strip_suffix(sutra_bpmn::REDACTION_COMPANION_SUFFIX)
                .map(|base| (base, value.to_canonical_string()))
        })
        .collect();
    // Variables are typed in the snapshot from v4 on, but this projection stays STRING-VALUED:
    // it is a published admin contract, and every value renders through the same display form it
    // had when the snapshot could hold nothing else. Typing changed what survives a wait, not what
    // an operator's inspect response looks like.
    let variables: serde_json::Map<String, serde_json::Value> = snapshot
        .variables()
        .iter()
        // The companions themselves are internal — surfaced via their base variable, not on their own.
        .filter(|(name, _)| !name.ends_with(sutra_bpmn::REDACTION_COMPANION_SUFFIX))
        .map(|(name, value)| {
            let shown = if sensitive.contains(name.as_str()) {
                REDACTED_PLACEHOLDER.to_owned()
            } else if let Some(masked) = companions.get(name.as_str()) {
                masked.clone()
            } else {
                value.to_canonical_string()
            };
            (name.clone(), serde_json::Value::String(shown))
        })
        .collect();
    let mut projection = serde_json::json!({
        "instanceId": instance_id.to_string(),
        "deploymentId": snapshot.deployment_id(),
        "status": snapshot.status(),
        "completedNodes": snapshot.completed_nodes(),
        "waitingNodes": snapshot.waiting_nodes(),
        "sensitive": snapshot.sensitive(),
        "variables": variables,
    });
    // A FAILED instance names its cause by CODE only. The stable `SUTRA.*` code is safe on this
    // unauthenticated operate surface; the failure DETAIL is not — it can quote business data
    // lifted from the failing expression or task, so it stays in the snapshot for the admin path.
    if !snapshot.failure_code().is_empty() {
        if let Some(map) = projection.as_object_mut() {
            map.insert(
                "failureCode".to_owned(),
                serde_json::Value::String(snapshot.failure_code().to_owned()),
            );
        }
    }
    projection
}

/// One list-summary row as its JSON projection.
fn summary_json(summary: &InstanceSummary) -> serde_json::Value {
    serde_json::json!({
        "instanceId": summary.instance_id.to_string(),
        "deploymentId": summary.deployment_id,
        "status": summary.status,
    })
}

fn parse_uuid(id: &str) -> Option<Uuid> {
    Uuid::parse_str(id).ok()
}

fn bad_uuid(id: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "instanceId": id, "error": "instance id is not a UUID" })),
    )
}

fn instance_not_found(id: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "instanceId": id, "found": false })),
    )
}

/// 404 for the by-alias lookup — no live instance carries `(name, value)` (admin surface).
fn alias_not_found(name: &str, value: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "alias": name, "value": value, "found": false })),
    )
}

fn cancel_error(
    id: &str,
    stage: &str,
    e: &sutra_persistence::PersistenceError,
) -> (StatusCode, Json<serde_json::Value>) {
    warn!(instance_id = %id, stage, error = %e, "instance cancel failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "instanceId": id, "error": e.to_string() })),
    )
}

fn health_body(status: &str, check: &str, check_status: &str) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "checks": [{ "name": check, "status": check_status }]
    })
}

/// Resolve a data subject's instances via the HMAC blind index, then (unless `dryRun`)
/// hard-delete their state across the stores + null captured audit payloads (audit metadata is
/// retained), and emit a metadata-only `SUTRA.SUBJECT_ERASED` audit event. Returns the matched /
/// erased instance ids. The `dryRun` path is GDPR requirement 6 (disclose the instances that WOULD
/// be purged). Body (JSON): `{keyId, subjectName, value, deploymentId?, dryRun?}` — with
/// `deploymentId` OMITTED the operation fans out across ALL of the tenant's active deployments (the
/// blind index is migration-stable, so one `keyId` matches the subject across versions). Fail-closed:
/// an unconfigured master key ⇒ 412 (no subjects are indexed).
///
/// **Reaches RETAINED terminal instances.** Terminal retention (P1-2) means a subject's finished
/// instances keep their `instance_state` row — and their variables — for `sutra.instance.retention`
/// rather than vanishing at completion. Erasure is unaffected by that marker (see the cascade
/// below): it resolves instances through `subject_index`, which the park step wrote regardless of
/// how the instance later ended, and deletes by key. So the population an erasure request most
/// often means — the subject's COMPLETED cases — is exactly the population retention keeps, and
/// exactly the population this reaches.
pub(crate) async fn subject_erase_response(
    state: &AppState,
    body: serde_json::Value,
) -> (StatusCode, Json<serde_json::Value>) {
    use sutra_persistence::stores::{
        AliasStore, AuditEventRow, InstanceStore, PgAliasStore, PgAuditEventStore,
        PgDeploymentArchiveStore, PgInstanceStore, PgSubjectIndexStore, SubjectIndexStore,
    };

    // Required string fields — a missing / non-string field is a 400.
    let field = |key: &str| -> Result<String, (StatusCode, Json<serde_json::Value>)> {
        body.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": format!("'{key}' (string) is required") })),
                )
            })
    };
    let key_id = match field("keyId") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let subject_name = match field("subjectName") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let value = match field("value") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let dry_run = body
        .get("dryRun")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    // deploymentId is OPTIONAL — omitted ⇒ tenant-wide (every active deployment of this tenant).
    let scoped_deployment = body
        .get("deploymentId")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let Some(pool) = state.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "no persistence configured" })),
        );
    };
    let Some(provider) = state.key_provider.clone() else {
        return (
            StatusCode::PRECONDITION_FAILED,
            Json(serde_json::json!({
                "error": "blind-indexing is disabled (sutra.crypto.master-key unset) — no subjects are indexed"
            })),
        );
    };
    // The migration-stable blind is the same across a tenant's deployments (keyId-derived).
    let blind = match provider.blind_index_key(&key_id) {
        Ok(indexer) => indexer.blind(&value),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("index key derivation failed: {e}") })),
            )
        }
    };

    // The deployment set: the one explicit deployment, or every ACTIVE-or-DRAINING deployment of
    // this tenant (keyId == the tenant label). DRAINING must be included: instances stay parked on
    // a flipped-away deployment until it retires, and erasure has to reach them there.
    let deployment_ids: Vec<String> = match &scoped_deployment {
        Some(d) => vec![d.clone()],
        None => {
            let archive = PgDeploymentArchiveStore::new(pool.clone());
            match archive.list_active_and_draining().await {
                Ok(served) => served
                    .into_iter()
                    .filter(|row| row.archive.tenant == key_id)
                    .map(|row| row.archive.deployment_id)
                    .collect(),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(
                            serde_json::json!({ "error": format!("deployment enumeration failed: {e}") }),
                        ),
                    )
                }
            }
        }
    };

    let subjects = PgSubjectIndexStore::new(pool.clone());
    let instances = PgInstanceStore::new(pool.clone());
    let aliases = PgAliasStore::new(pool.clone());
    let audit = PgAuditEventStore::new(pool.clone());

    let mut all_ids: Vec<String> = Vec::new();
    let mut erased_total = 0usize;
    for dep_id in &deployment_ids {
        let deployment = match sutra_persistence::DeploymentId::new(dep_id) {
            Ok(d) => d,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        serde_json::json!({ "error": format!("invalid deploymentId '{dep_id}': {e}") }),
                    ),
                )
            }
        };
        let ids = match subjects
            .find_instances(&deployment, &subject_name, &blind)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("disclosure query failed: {e}") })),
                )
            }
        };
        all_ids.extend(ids.iter().map(Uuid::to_string));
        if dry_run || ids.is_empty() {
            continue;
        }
        // Cascade — idempotent per instance; subject rows LAST so a crash before that leaves the
        // instance still discoverable (a re-run finds + finishes it).
        for id in &ids {
            let cascade = async {
                // `delete` is unconditional on `terminal_at`, which is what makes erasure reach the
                // instances terminal retention now keeps. This matters more than it reads: since
                // P1-2 a finished instance's variables survive in `instance_state` for the whole
                // retention window, so erasure that only reached LIVE rows would leave a subject's
                // data sitting in the recovery table for days after their process completed —
                // precisely the population an erasure request is most likely to name. Retention
                // shortens nothing and hides nothing from this path: the same
                // `(deployment_id, instance_id)` key, the same delete.
                instances.delete(&deployment, *id).await?;
                aliases.delete(&deployment, *id).await?;
                audit.redact_instance_payloads(&deployment, *id).await?;
                subjects.delete(&deployment, *id).await?;
                Ok::<(), sutra_persistence::PersistenceError>(())
            };
            if let Err(e) = cascade.await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(
                        serde_json::json!({ "error": format!("erasure failed on instance {id}: {e}") }),
                    ),
                );
            }
        }
        erased_total += ids.len();
        // Metadata-only SUTRA.SUBJECT_ERASED audit event — NO cleartext PII (the blind is an HMAC).
        // instance_id = None (a deployment-level event); NULLs are distinct in the (deployment,
        // instance, seq) unique index, so seq 0 never collides across erasures.
        let event = AuditEventRow {
            deployment: deployment.clone(),
            instance_id: None,
            seq: 0,
            at: time::OffsetDateTime::now_utc(),
            event_type: "SUTRA.SUBJECT_ERASED".to_string(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: Some(
                serde_json::json!({
                    "subjectName": subject_name,
                    "blind": blind,
                    "erasedCount": ids.len(),
                })
                .to_string(),
            ),
            payload_json: "{}".to_string(),
        };
        if let Err(e) = audit.insert(&event).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("erasure completed but the SUBJECT_ERASED audit event failed: {e}"),
                    "erasedCount": erased_total,
                })),
            );
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "subjectName": subject_name,
            "tenantWide": scoped_deployment.is_none(),
            "deploymentsScanned": deployment_ids.len(),
            "erased": !dry_run,
            "matched": all_ids.len(),
            "erasedCount": if dry_run { 0 } else { erased_total },
            "instanceIds": all_ids,
        })),
    )
}

/// A minimal, persistence-less [`AppState`] for in-crate tests (the admin-gate tests build
/// the admin router over it). Ready, no live deployments, no pool — enough for the activation-status
/// route to answer `200` while the gate decides the request's fate.
#[cfg(test)]
pub(crate) fn app_state_for_test() -> AppState {
    AppState {
        ready: Arc::new(AtomicBool::new(true)),
        deployments: LiveDeploymentSet::new(Vec::new()),
        deploy_status: Arc::new(std::sync::RwLock::new(Default::default())),
        api_specs: Arc::new(std::sync::RwLock::new(Default::default())),
        node_indexes: Arc::new(std::sync::RwLock::new(Default::default())),
        pool: None,
        deploy: None,
        key_provider: None,
        engine: None,
        instance_retention: crate::bridge::DEFAULT_INSTANCE_RETENTION,
        audit_sql_enabled: false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// Drift gate: the committed `openapi/platform.yaml` path set must EXACTLY match
    /// the canonical [`PLATFORM_ROUTES`] table — no missing, no extra. Adding a platform route
    /// without documenting it (or vice-versa) fails here.
    #[test]
    fn openapi_platform_spec_matches_the_route_table() {
        let spec_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3) // crates/sutra-engine -> crates -> rust -> <repo root>
            .expect("repo root")
            .join("openapi/platform.yaml");
        let text = std::fs::read_to_string(&spec_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", spec_path.display()));
        let doc: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&text).expect("parse platform.yaml");

        let methods = ["get", "post", "put", "delete", "patch"];
        let mut spec: Vec<(String, String)> = Vec::new();
        for (path, ops) in doc
            .get("paths")
            .and_then(|p| p.as_mapping())
            .expect("paths mapping")
        {
            let path = path.as_str().expect("path is a string");
            for (method, _op) in ops.as_mapping().expect("operations mapping") {
                let m = method.as_str().unwrap_or_default();
                if methods.contains(&m) {
                    spec.push((m.to_uppercase(), path.to_string()));
                }
            }
        }
        spec.sort();

        let mut table: Vec<(String, String)> = PLATFORM_ROUTES
            .iter()
            .map(|(m, p)| ((*m).to_string(), (*p).to_string()))
            .collect();
        table.sort();

        assert_eq!(
            spec, table,
            "openapi/platform.yaml drifted from PLATFORM_ROUTES (server.rs) — update the spec + the \
             route table together"
        );
    }

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    // ---- redaction projection (tier-1, no DB — the security-critical part) -----------------

    #[test]
    fn inspect_projection_redacts_sensitive_variable_values() {
        let snapshot = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            names(&["S"]),
            vars(&[("ssn", "000-00-0000"), ("amount", "42")]),
            names(&["W"]),
            "",
            0,
        )
        .with_sensitive(names(&["ssn"]));

        let body = inspect_projection(Uuid::nil(), &snapshot);

        // The sensitive value is replaced by the placeholder; the non-sensitive one is shown.
        assert_eq!(
            body["variables"]["ssn"],
            serde_json::json!(REDACTED_PLACEHOLDER)
        );
        assert_eq!(body["variables"]["amount"], serde_json::json!("42"));
        // Belt-and-braces: the raw secret must not appear ANYWHERE in the rendered body.
        assert!(
            !body.to_string().contains("000-00-0000"),
            "sensitive value leaked into the inspect projection"
        );
        // Structural fields survive.
        assert_eq!(body["status"], serde_json::json!("SUSPENDED"));
        assert_eq!(
            body["deploymentId"],
            serde_json::json!("dep-0123456789abcdef01234567")
        );
        assert_eq!(body["waitingNodes"], serde_json::json!(["W"]));
        assert_eq!(body["sensitive"], serde_json::json!(["ssn"]));
    }

    #[test]
    fn inspect_projection_reports_a_failed_instance_by_code_and_never_by_detail() {
        // P0-4 visibility: `GET /sutra/instances/{id}` shows FAILED and names the stable code.
        // The DETAIL is deliberately absent — it can quote business data, and this route is the
        // UNAUTHENTICATED operate surface (the admin path reads the snapshot for the rest).
        let snapshot = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            names(&["S"]),
            vars(&[("amount", "42")]),
            names(&["W"]),
            "",
            0,
        )
        .with_failure(
            "SUTRA.RUNTIME.TASK.UNCAUGHT",
            "debtor account 'GB33BUKB20201555555555' was rejected",
        );

        let body = inspect_projection(Uuid::nil(), &snapshot);

        assert_eq!(body["status"], serde_json::json!("FAILED"));
        assert_eq!(
            body["failureCode"],
            serde_json::json!("SUTRA.RUNTIME.TASK.UNCAUGHT")
        );
        assert!(
            !body.to_string().contains("GB33BUKB"),
            "the failure detail must not reach the unauthenticated operate surface"
        );
        // The frontier it died at is still reported — that is what an operator needs.
        assert_eq!(body["waitingNodes"], serde_json::json!(["W"]));
    }

    #[test]
    fn a_healthy_instance_projects_no_failure_code() {
        let snapshot = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            names(&["S"]),
            vars(&[]),
            names(&["W"]),
            "",
            0,
        );
        assert!(inspect_projection(Uuid::nil(), &snapshot)
            .get("failureCode")
            .is_none());
    }

    #[test]
    fn inspect_projection_shows_all_when_nothing_is_sensitive() {
        let snapshot = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            names(&["S"]),
            vars(&[("amount", "42")]),
            names(&["W"]),
            "",
            0,
        );

        let body = inspect_projection(Uuid::nil(), &snapshot);

        assert_eq!(body["variables"]["amount"], serde_json::json!("42"));
        assert!(body["sensitive"].as_array().unwrap().is_empty());
    }

    #[test]
    fn inspect_projection_shows_the_masked_companion_in_place_of_the_raw_payload() {
        // Intake stored the raw payload AND its DLP-masked projection (`payload.redacted`).
        let snapshot = InstanceSnapshot::of_suspended(
            "pay",
            "dep-0123456789abcdef01234567",
            names(&["S"]),
            vars(&[
                ("payload", "{\"pan\":\"4111111111111111\",\"note\":\"hi\"}"),
                (
                    "payload.redacted",
                    "{\"pan\":\"[REDACTED]\",\"note\":\"hi\"}",
                ),
                ("amount", "42"),
            ]),
            names(&["W"]),
            "",
            0,
        );

        let body = inspect_projection(Uuid::nil(), &snapshot);

        // `payload` is shown AS its masked companion, never the raw value.
        assert_eq!(
            body["variables"]["payload"],
            serde_json::json!("{\"pan\":\"[REDACTED]\",\"note\":\"hi\"}")
        );
        // The companion is internal — not surfaced under its own name.
        assert!(body["variables"].get("payload.redacted").is_none());
        // Unrelated variables are unaffected.
        assert_eq!(body["variables"]["amount"], serde_json::json!("42"));
        // The raw PAN must not leak anywhere in the rendered body.
        assert!(
            !body.to_string().contains("4111111111111111"),
            "raw payload leaked past the masked companion"
        );
    }

    // ---- instance history paging + rendering (tier-1, no DB) -------------------------------

    #[test]
    fn history_paging_defaults_and_clamps() {
        let empty = HashMap::new();
        assert_eq!(history_paging(&empty), (0, AUDIT_HISTORY_PAGE_DEFAULT));

        let params = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect()
        };
        assert_eq!(
            history_paging(&params(&[("afterSeq", "17"), ("limit", "5")])),
            (17, 5)
        );
        // A page bigger than the ceiling is clamped, not honoured — one request must never
        // materialise a long-lived instance's whole journal (each row can carry a payload).
        assert_eq!(
            history_paging(&params(&[("limit", "99999")])).1,
            AUDIT_HISTORY_PAGE_MAX
        );
        assert_eq!(history_paging(&params(&[("limit", "0")])).1, 1);
        // Garbage falls back to the defaults rather than 400-ing a read-only surface.
        assert_eq!(
            history_paging(&params(&[("afterSeq", "abc"), ("limit", "-3")])),
            (0, 1)
        );
    }

    #[test]
    fn audit_event_json_parses_json_columns_and_degrades_readably() {
        let record = AuditEventRecord {
            id: 7,
            seq: 3,
            at: time::OffsetDateTime::UNIX_EPOCH,
            event_type: "NODE_LEFT".to_owned(),
            node_id: Some("Approve".to_owned()),
            diagnostic_code: Some("SUTRA.RUNTIME.TASK.UNCAUGHT".to_owned()),
            diagnostic_json: Some("{\"message\":\"boom\"}".to_owned()),
            payload_json: "{\"amount\":42}".to_owned(),
        };
        let body = audit_event_json(&record);
        assert_eq!(body["seq"], serde_json::json!(3));
        assert_eq!(body["eventType"], serde_json::json!("NODE_LEFT"));
        assert_eq!(body["nodeId"], serde_json::json!("Approve"));
        // JSON columns come back as JSON, not as escaped strings.
        assert_eq!(body["payload"]["amount"], serde_json::json!(42));
        assert_eq!(body["diagnostic"]["message"], serde_json::json!("boom"));

        // A row whose payload is not JSON still renders — as a string, never as a failed page.
        let mut odd = record.clone();
        odd.payload_json = "not json at all".to_owned();
        odd.diagnostic_json = None;
        let body = audit_event_json(&odd);
        assert_eq!(body["payload"], serde_json::json!("not json at all"));
        assert_eq!(body["diagnostic"], serde_json::Value::Null);
    }

    /// A redacted audit row (post-GDPR-erasure) renders as an empty object, and the trail row
    /// itself survives — the property the retention purge deliberately does not disturb.
    #[test]
    fn audit_event_json_renders_a_redacted_payload_as_an_empty_object() {
        let record = AuditEventRecord {
            id: 1,
            seq: 1,
            at: time::OffsetDateTime::UNIX_EPOCH,
            event_type: "NODE_ENTERED".to_owned(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{}".to_owned(),
        };
        let body = audit_event_json(&record);
        assert_eq!(body["payload"], serde_json::json!({}));
        assert_eq!(body["eventType"], serde_json::json!("NODE_ENTERED"));
    }

    // ---- the operate surface over retained terminal instances (docker) ----------------------
    //
    // These drive the `pub(crate)` handlers directly against a real migrated database — the same
    // code path the router calls, minus the socket. They are the endpoint-level half of P1-2; the
    // store-level half lives in `sutra-persistence`'s pg suite.

    #[cfg(test)]
    mod retention_docker {
        use std::sync::atomic::AtomicU32;
        use std::sync::OnceLock;

        use sqlx::postgres::PgPoolOptions;
        use sqlx::PgPool;
        use sutra_persistence::stores::{AuditEventRow, InstanceState};
        use sutra_persistence::DeploymentId as PersistDeploymentId;
        use uuid::Uuid;

        use super::*;

        const DEP: &str = "dep-000000000000000000000042";

        static CONTAINER: OnceLock<(
            testcontainers::Container<testcontainers_modules::postgres::Postgres>,
            u16,
        )> = OnceLock::new();
        static DB_SEQ: AtomicU32 = AtomicU32::new(0);

        fn container_port() -> u16 {
            let (_, port) = CONTAINER.get_or_init(|| {
                // The blocking testcontainers runner drives its own runtime — start it on a
                // dedicated OS thread so we never enter it from inside a tokio worker.
                std::thread::spawn(|| {
                    use testcontainers::runners::SyncRunner;
                    use testcontainers::ImageExt;
                    let container = testcontainers_modules::postgres::Postgres::default()
                        .with_tag("16-alpine")
                        .start()
                        .expect("start postgres:16-alpine (docker required)");
                    sutra_testkit::reap_on_exit(container.id());
                    let port = container.get_host_port_ipv4(5432).expect("mapped 5432");
                    (container, port)
                })
                .join()
                .expect("container bootstrap thread")
            });
            *port
        }

        async fn fresh_pool() -> PgPool {
            let port = container_port();
            let admin = PgPoolOptions::new()
                .max_connections(1)
                .connect(&format!(
                    "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
                ))
                .await
                .expect("admin pool");
            let db = format!(
                "retention_it_{}",
                DB_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            );
            sqlx::query(&format!("CREATE DATABASE {db}"))
                .execute(&admin)
                .await
                .expect("create database");
            drop(admin);

            let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(3)
                .expect("repo root")
                .to_path_buf();
            let roots = [
                repo.join("rust/crates/sutra-persistence/migrations/shipped/core"),
                repo.join("rust/crates/sutra-persistence/migrations/shipped/audit"),
            ];
            let root_refs: Vec<&std::path::Path> = roots.iter().map(PathBuf::as_path).collect();
            let scripts =
                sutra_persistence::migrate::collect_migrations(&root_refs).expect("collect");
            let mut conn = sqlx::postgres::PgConnectOptions::new()
                .host("127.0.0.1")
                .port(port)
                .username("postgres")
                .password("postgres")
                .database(&db)
                .connect()
                .await
                .expect("migration connection");
            sutra_persistence::migrate::apply_migrations(&mut conn, &scripts)
                .await
                .expect("apply migrations");
            drop(conn);

            PgPoolOptions::new()
                .max_connections(4)
                .connect(&format!(
                    "postgres://postgres:postgres@127.0.0.1:{port}/{db}"
                ))
                .await
                .expect("pool")
        }

        fn state_with(pool: PgPool, retention: std::time::Duration, audit_sql: bool) -> AppState {
            AppState {
                ready: Arc::new(AtomicBool::new(true)),
                deployments: LiveDeploymentSet::new(vec![
                    sutra_executor::DeploymentId::of(DEP).expect("deployment id")
                ]),
                deploy_status: Arc::new(std::sync::RwLock::new(Default::default())),
                api_specs: Arc::new(std::sync::RwLock::new(Default::default())),
                node_indexes: Arc::new(std::sync::RwLock::new(Default::default())),
                pool: Some(pool),
                deploy: None,
                key_provider: None,
                engine: None,
                instance_retention: retention,
                audit_sql_enabled: audit_sql,
            }
        }

        fn dep() -> PersistDeploymentId {
            PersistDeploymentId::new(DEP).expect("persistence deployment id")
        }

        /// Persist a parked instance carrying one `@sensitive` variable, and return its id.
        async fn park(pool: &PgPool) -> Uuid {
            let id = Uuid::new_v4();
            let snapshot = InstanceSnapshot::of_suspended(
                "pay",
                DEP,
                vec!["S".to_owned()],
                [
                    ("ssn".to_owned(), "000-00-0000".to_owned()),
                    ("amount".to_owned(), "42".to_owned()),
                ]
                .into_iter()
                .collect(),
                vec!["W".to_owned()],
                "S",
                0,
            )
            .with_sensitive(vec!["ssn".to_owned()]);
            PgInstanceStore::new(pool.clone())
                .persist(
                    &dep(),
                    &InstanceState {
                        instance_id: id,
                        serialised: snapshot.write(),
                    },
                )
                .await
                .expect("persist");
            id
        }

        async fn body_of(response: axum::response::Response) -> serde_json::Value {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            serde_json::from_slice(&bytes).expect("json body")
        }

        /// The headline P1-2 behaviour on the read surface: a finished instance ANSWERS instead of
        /// 404-ing, reports its terminal status, and still redacts its `@sensitive` values.
        #[ignore = "docker"]
        #[tokio::test]
        async fn a_terminal_instance_is_inspectable_with_variables_still_redacted() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::from_secs(3600), false);
            let id = park(&pool).await;
            let store = PgInstanceStore::new(pool.clone());

            // Before: parked, and reported as SUSPENDED.
            let response = instance_inspect_response(&state, &id.to_string()).await;
            assert_eq!(response.status(), StatusCode::OK);

            // Finish it the way the terminal step does: re-stamp + mark terminal.
            let bytes = store.load(&dep(), id).await.unwrap().unwrap().serialised;
            let terminal = InstanceSnapshot::mark_terminal(&bytes, STATUS_COMPLETED).unwrap();
            store.mark_terminal(&dep(), id, &terminal).await.unwrap();

            let response = instance_inspect_response(&state, &id.to_string()).await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "a completed instance used to 404 the instant it finished"
            );
            let body = body_of(response).await;
            assert_eq!(body["status"], serde_json::json!(STATUS_COMPLETED));
            assert_eq!(body["deploymentId"], serde_json::json!(DEP));
            assert_eq!(body["variables"]["amount"], serde_json::json!("42"));
            assert_eq!(
                body["variables"]["ssn"],
                serde_json::json!(REDACTED_PLACEHOLDER),
                "retention must not become a way to read sensitive values back"
            );
            assert!(!body.to_string().contains("000-00-0000"));
        }

        /// The list keeps meaning "in flight" by default and opens up on request.
        #[ignore = "docker"]
        #[tokio::test]
        async fn the_list_hides_terminal_instances_until_asked() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::from_secs(3600), false);
            let live = park(&pool).await;
            let done = park(&pool).await;
            let store = PgInstanceStore::new(pool.clone());
            let bytes = store.load(&dep(), done).await.unwrap().unwrap().serialised;
            let terminal = InstanceSnapshot::mark_terminal(&bytes, STATUS_COMPLETED).unwrap();
            store.mark_terminal(&dep(), done, &terminal).await.unwrap();

            let params = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect()
            };

            let body = body_of(instances_list_response(&state, &params(&[])).await).await;
            let ids: Vec<&str> = body["instances"]
                .as_array()
                .unwrap()
                .iter()
                .map(|i| i["instanceId"].as_str().unwrap())
                .collect();
            assert_eq!(
                ids,
                vec![live.to_string()],
                "default list is in-flight only"
            );

            let body = body_of(
                instances_list_response(&state, &params(&[("includeTerminal", "true")])).await,
            )
            .await;
            assert_eq!(body["instances"].as_array().unwrap().len(), 2);

            // Asking for a terminal status implies the flag — the obvious query works.
            let body =
                body_of(instances_list_response(&state, &params(&[("status", "COMPLETED")])).await)
                    .await;
            let ids: Vec<&str> = body["instances"]
                .as_array()
                .unwrap()
                .iter()
                .map(|i| i["instanceId"].as_str().unwrap())
                .collect();
            assert_eq!(ids, vec![done.to_string()]);
        }

        /// Cancel retains the instance as TERMINATED, and a SECOND cancel is a 409 rather than a
        /// history rewrite.
        #[ignore = "docker"]
        #[tokio::test]
        async fn cancel_retains_as_terminated_and_refuses_to_recancel() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::from_secs(3600), false);
            let id = park(&pool).await;

            let response = instance_cancel_response(&state, &id.to_string()).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = body_of(response).await;
            assert_eq!(body["status"], serde_json::json!("CANCELLED"));
            assert_eq!(
                body["persistedStatus"],
                serde_json::json!(STATUS_TERMINATED)
            );
            assert_eq!(body["retained"], serde_json::json!(true));

            // The row survives and reports TERMINATED.
            let body = body_of(instance_inspect_response(&state, &id.to_string()).await).await;
            assert_eq!(body["status"], serde_json::json!(STATUS_TERMINATED));

            // A repeat cancel must NOT re-stamp it.
            let response = instance_cancel_response(&state, &id.to_string()).await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = body_of(response).await;
            assert_eq!(body["status"], serde_json::json!(STATUS_TERMINATED));
        }

        /// `PT0S` restores the pre-P1-2 posture end to end: cancel deletes, inspect 404s.
        #[ignore = "docker"]
        #[tokio::test]
        async fn zero_retention_cancel_deletes_the_row_outright() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::ZERO, false);
            let id = park(&pool).await;

            let body = body_of(instance_cancel_response(&state, &id.to_string()).await).await;
            assert_eq!(body["retained"], serde_json::json!(false));
            assert_eq!(body["persistedStatus"], serde_json::Value::Null);

            assert_eq!(
                instance_inspect_response(&state, &id.to_string())
                    .await
                    .status(),
                StatusCode::NOT_FOUND,
                "PT0S keeps no history, exactly as before P1-2"
            );
        }

        /// The history endpoint: seq order, cursor paging, and the full row (payload included).
        #[ignore = "docker"]
        #[tokio::test]
        async fn history_pages_the_journal_by_seq_cursor() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::from_secs(3600), true);
            let id = park(&pool).await;
            let audit = PgAuditEventStore::new(pool.clone());
            for seq in 1..=5 {
                audit
                    .insert(&AuditEventRow {
                        deployment: dep(),
                        instance_id: Some(id),
                        seq,
                        at: time::OffsetDateTime::now_utc(),
                        event_type: "NODE_ENTERED".to_owned(),
                        node_id: Some(format!("N{seq}")),
                        diagnostic_code: None,
                        diagnostic_json: None,
                        payload_json: format!("{{\"step\":{seq}}}"),
                    })
                    .await
                    .expect("audit insert");
            }

            let params = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect()
            };

            let body = body_of(
                instance_history_response(&state, &id.to_string(), &params(&[("limit", "2")]))
                    .await,
            )
            .await;
            assert_eq!(body["instanceId"], serde_json::json!(id.to_string()));
            assert_eq!(body["deploymentId"], serde_json::json!(DEP));
            assert_eq!(body["auditEnabled"], serde_json::json!(true));
            assert!(
                body.get("note").is_none(),
                "a non-empty page carries no note"
            );
            let events = body["events"].as_array().unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0]["seq"], serde_json::json!(1));
            assert_eq!(events[0]["nodeId"], serde_json::json!("N1"));
            assert_eq!(events[0]["payload"]["step"], serde_json::json!(1));
            assert_eq!(events[1]["seq"], serde_json::json!(2));
            assert_eq!(body["nextAfterSeq"], serde_json::json!(2));

            // Follow the cursor to the end; the last (short) page reports no successor.
            let body = body_of(
                instance_history_response(
                    &state,
                    &id.to_string(),
                    &params(&[("limit", "2"), ("afterSeq", "2")]),
                )
                .await,
            )
            .await;
            assert_eq!(
                body["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| e["seq"].as_i64().unwrap())
                    .collect::<Vec<_>>(),
                vec![3, 4]
            );
            let body = body_of(
                instance_history_response(
                    &state,
                    &id.to_string(),
                    &params(&[("limit", "2"), ("afterSeq", "4")]),
                )
                .await,
            )
            .await;
            assert_eq!(body["events"].as_array().unwrap().len(), 1);
            assert_eq!(body["nextAfterSeq"], serde_json::Value::Null);
        }

        /// The journal OUTLIVES the instance row: history still answers after the retention purge
        /// has removed the snapshot. This is the property that makes the two lifecycles separate.
        #[ignore = "docker"]
        #[tokio::test]
        async fn history_survives_the_retention_purge_of_the_instance_row() {
            let pool = fresh_pool().await;
            let state = state_with(pool.clone(), std::time::Duration::from_secs(3600), true);
            let id = park(&pool).await;
            PgAuditEventStore::new(pool.clone())
                .insert(&AuditEventRow {
                    deployment: dep(),
                    instance_id: Some(id),
                    seq: 1,
                    at: time::OffsetDateTime::now_utc(),
                    event_type: "INSTANCE_COMPLETED".to_owned(),
                    node_id: None,
                    diagnostic_code: None,
                    diagnostic_json: None,
                    payload_json: "{}".to_owned(),
                })
                .await
                .expect("audit insert");

            let store = PgInstanceStore::new(pool.clone());
            let bytes = store.load(&dep(), id).await.unwrap().unwrap().serialised;
            let terminal = InstanceSnapshot::mark_terminal(&bytes, STATUS_COMPLETED).unwrap();
            store.mark_terminal(&dep(), id, &terminal).await.unwrap();
            // The purge sweeper's own operation, with a zero window.
            assert_eq!(
                store
                    .purge_terminal(&dep(), std::time::Duration::ZERO)
                    .await
                    .unwrap(),
                1
            );

            // The snapshot is gone…
            assert_eq!(
                instance_inspect_response(&state, &id.to_string())
                    .await
                    .status(),
                StatusCode::NOT_FOUND
            );
            // …and the journal is not.
            let body =
                body_of(instance_history_response(&state, &id.to_string(), &HashMap::new()).await)
                    .await;
            assert_eq!(body["events"].as_array().unwrap().len(), 1);
            assert_eq!(
                body["events"][0]["eventType"],
                serde_json::json!("INSTANCE_COMPLETED")
            );
        }

        /// An empty page NAMES the reason. With the journal switched off, "no events" must never
        /// read as "this instance did nothing".
        #[ignore = "docker"]
        #[tokio::test]
        async fn history_of_an_unaudited_instance_explains_why_it_is_empty() {
            let pool = fresh_pool().await;
            let id = park(&pool).await;

            let off = state_with(pool.clone(), std::time::Duration::from_secs(3600), false);
            let body =
                body_of(instance_history_response(&off, &id.to_string(), &HashMap::new()).await)
                    .await;
            assert_eq!(body["auditEnabled"], serde_json::json!(false));
            assert!(body["events"].as_array().unwrap().is_empty());
            assert!(
                body["note"].as_str().unwrap().contains("DISABLED"),
                "an empty page must say the journal is off: {body}"
            );

            // With the journal ON but the process not declaring `<q:audit>`, the note differs.
            let on = state_with(pool, std::time::Duration::from_secs(3600), true);
            let body =
                body_of(instance_history_response(&on, &id.to_string(), &HashMap::new()).await)
                    .await;
            assert_eq!(body["auditEnabled"], serde_json::json!(true));
            assert!(body["note"].as_str().unwrap().contains("<q:audit>"));

            // An id nobody has ever heard of is still a 404, not an empty page.
            assert_eq!(
                instance_history_response(&on, &Uuid::new_v4().to_string(), &HashMap::new())
                    .await
                    .status(),
                StatusCode::NOT_FOUND
            );
        }

        /// REGRESSION (the reason `count_active` had to change): a DRAINING deployment whose only
        /// remaining instances are RETAINED TERMINAL ones must still retire. Before the
        /// `terminal_at IS NULL` predicate, retention would have pinned every draining deployment
        /// open for a full week.
        #[ignore = "docker"]
        #[tokio::test]
        async fn the_deploy_quiescence_gate_retires_with_only_terminal_instances_left() {
            let pool = fresh_pool().await;
            let id = park(&pool).await;
            let store = PgInstanceStore::new(pool.clone());
            let draining = vec![sutra_executor::DeploymentId::of(DEP).expect("deployment id")];
            let some_pool = Some(pool.clone());

            // A live parked instance holds the deployment open — the pre-existing behaviour.
            assert!(
                crate::deploy::quiescent_ids(&draining, &some_pool)
                    .await
                    .is_empty(),
                "a parked instance must keep its deployment DRAINING"
            );

            // Finishing it releases the gate even though the ROW is still there.
            let bytes = store.load(&dep(), id).await.unwrap().unwrap().serialised;
            let terminal = InstanceSnapshot::mark_terminal(&bytes, STATUS_COMPLETED).unwrap();
            store.mark_terminal(&dep(), id, &terminal).await.unwrap();
            assert_eq!(
                crate::deploy::quiescent_ids(&draining, &some_pool).await,
                draining,
                "a retained terminal row is history, not work — it must not pin a DRAINING \
                 deployment open for the retention window"
            );

            // A FAILED instance, by contrast, still counts: it needs a human before its
            // deployment may retire.
            let failed = park(&pool).await;
            let bytes = store
                .load(&dep(), failed)
                .await
                .unwrap()
                .unwrap()
                .serialised;
            let marked =
                InstanceSnapshot::mark_failed(&bytes, "SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
                    .unwrap();
            store
                .persist(
                    &dep(),
                    &InstanceState {
                        instance_id: failed,
                        serialised: marked,
                    },
                )
                .await
                .unwrap();
            assert!(
                crate::deploy::quiescent_ids(&draining, &some_pool)
                    .await
                    .is_empty(),
                "a FAILED instance is not finished — it must keep its deployment DRAINING"
            );
        }

        /// The quiescence gate's third leg: a parked external task is a worker-owed result whose
        /// completion re-enters through this deployment's channels, so it must pin the
        /// deployment DRAINING exactly as a pending outbox row does. A `failed` (terminal) task
        /// releases the gate, mirroring the outbox's poisoned posture.
        #[ignore = "docker"]
        #[tokio::test]
        async fn the_deploy_quiescence_gate_counts_parked_external_tasks() {
            use sutra_persistence::stores::{
                ExternalTaskRow, ExternalTaskStore, PgExternalTaskStore,
            };

            let pool = fresh_pool().await;
            let draining = vec![sutra_executor::DeploymentId::of(DEP).expect("deployment id")];
            let some_pool = Some(pool.clone());

            let store = PgExternalTaskStore::new(pool.clone());
            let now = time::OffsetDateTime::now_utc();
            let task = ExternalTaskRow {
                deployment: dep(),
                task_id: Uuid::new_v4(),
                instance_id: Uuid::new_v4(),
                channel: "score-in".into(),
                tenant: "acme".into(),
                module_key: "acme/scoring/1".into(),
                body: b"{}".to_vec().into(),
                content_type: Some("application/json".into()),
                headers: Default::default(),
                outbox_key: "quiescence-pin-probe".into(),
                traceparent: None,
                created_at: now,
                fetchable_at: now,
                lock_owner: None,
                lock_expires_at: None,
                attempt_count: 0,
                retries_left: 0,
                last_error: None,
                failed: false,
            };
            assert!(store.park(&task).await.expect("park"));
            assert!(
                crate::deploy::quiescent_ids(&draining, &some_pool)
                    .await
                    .is_empty(),
                "a parked external task is work a worker still owes — it must keep its \
                 deployment DRAINING"
            );

            // Spend the budget: the row turns terminal (`failed`) and stops pinning.
            let locked = store
                .fetch_and_lock(
                    &dep(),
                    &["score-in".into()],
                    "w1",
                    now,
                    now + time::Duration::minutes(5),
                    1,
                )
                .await
                .expect("fetch");
            assert_eq!(locked.len(), 1);
            assert!(store
                .fail(&dep(), task.task_id, "w1", now, 0, now, "gave up")
                .await
                .expect("fail"));
            assert_eq!(
                crate::deploy::quiescent_ids(&draining, &some_pool).await,
                draining,
                "a terminal external task is retained history, not pending work"
            );
        }
    }
}
