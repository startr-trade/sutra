//! The assembly — turns loaded deployments (sealed archives and/or the legacy tree scan)
//! into a running channel engine: per-deployment registries (processes, templates,
//! scripts, decisions, rules-as-validators, structural codecs), the module-owned `sql`
//! data stores, the executor, the channel engine on its actor thread, and the axum
//! router serving every HTTP channel.
//!
//! Everything `Rc`-based (executor + channel engine) is constructed INSIDE the actor
//! closure from `Send` raw parts prepared here; async stores are driven through the
//! captured runtime [`Handle`]. The same raw parts ([`DeploymentPlan`], `Clone`)
//! also feed the two-phase activation flip — [`crate::deploy`] prepares a new plan set
//! fully off-line, then replaces the whole engine on the actor thread between dispatches
//! (prepare fully, then swap).

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use axum::Router;
use sqlx::PgPool;
use sutra_bpmn::model::ProcessModule;
use sutra_channels::http::{spawn_engine_sharded, EngineHandle};
use sutra_channels::sink::{scheme_of, LocalDeliverySink, SinkRegistry};
use sutra_channels::{
    load_channel_definitions, spawn_dispatch_loop, ActiveInstanceCount, AllowAllFeatureProvider,
    ChannelDefinition, ChannelEngine, CodecRegistry, CollectingOutbox, ConcurrencyStore,
    DefaultTenantQuotaEnforcer, DeferredAckListener, DeferredAckRegistry, DmnContentValidator,
    DrainingSink, InMemoryActiveInstanceCount, InMemoryAliasStore, InMemoryConcurrencyStore,
    InMemoryInboxStore, InboundChain, LiveDeploymentSet, OutboxDispatcher, OutboxDispatcherHandle,
    PayloadCapPolicy, ProcessModuleRegistry, RedactorRegistry, RetryPolicy, SrlContentValidator,
    StaticTenantConfigSource, ValidatorRegistry,
};
use sutra_persistence::stores::{PgChannelConcurrencyStore, PgInstanceStore};
use sutra_transport_spi::{transport_factories, TransportChannels};

use crate::concurrency::{PersistedActiveInstanceCount, PersistedChannelConcurrency};
use sutra_codec_schema::StructuralCodec;
use sutra_datastore::{
    DataStoreError, MssqlDataStore, MysqlDataStore, PostgresDataStore, ProjectedStore,
    StoreDefinition,
};
use sutra_executor::{
    archive_key, logical_urn, ArtifactType, CoverageMetricStore, DataStore, DecisionEngineRegistry,
    DecisionRegistry, DeploymentId, DmnEngine, HbsTemplateEngine, OutboundChannelRegistry,
    ResolvedOutboundChannel, ScriptRegistry, SrlEngine, TaskRegistry, TemplateEngineRegistry,
    TemplateRegistry, TokenExecutor,
};
use sutra_loader::LoadedDeployment;
use sutra_redactor_template::HbsContentRedactor;
use tokio::runtime::Handle;
use tracing::{error, info, warn};

use crate::bridge::PersistenceBridge;
use crate::outbox::PgOutboxRows;
use crate::stores::{DeclaredCoverageStores, MssqlStore, MysqlStore, PgStore};

/// Send-able per-deployment raw parts, prepared off the actor (all fallible parsing
/// happens HERE, fail-closed — at startup or during a watch-triggered prepare), assembled
/// on the actor thread. `Clone` so the activation flip can rebuild the engine from cached plans
/// without re-parsing untouched deployments.
#[derive(Clone)]
pub(crate) struct DeploymentPlan {
    pub(crate) dep: DeploymentId,
    /// The `(tenant, module, version)` authoring identity this deployment was planned under.
    /// Channel bindings carry it too, but a deployment may declare NO channels at all (a purely
    /// schedule-driven module is exactly that case), so the plan holds it in its own right.
    pub(crate) namespace: sutra_channels::Namespace,
    modules: Vec<Arc<ProcessModule>>,
    templates: Vec<(String, Vec<u8>)>,
    scripts: Vec<(String, Vec<u8>)>,
    decisions: Vec<(String, Vec<u8>)>,
    /// `(archive-scoped rule URN key, decision)` — module rules exposed as tier-2 DMN validators
    /// keyed `archive_key(logical_urn("rule", local_id_with_ext), dep)` —
    /// `urn:sutra:rule:<path>:<name>.<ext>:<deploymentId>`, extension KEPT (rule is
    /// multi-engine, `.dmn`/`.srl`).
    validators: Vec<(String, sutra_dmn::model::DmnDecision)>,
    /// `(archive-scoped rule URN key, `.srl` ruleset source)` — the `.srl` sibling of `validators`
    /// above: module `rules/*.srl` exposed as tier-2 validators so one ruleset can be SPLIT across
    /// both engines on the same `<q:validators>` chain. Keyed the same way
    /// (`urn:sutra:rule:<path>:<name>.srl:<deploymentId>`, extension KEPT); the source is already
    /// deploy-time parse-validated in `plan_deployment`.
    srl_validators: Vec<(String, String)>,
    /// `(archive-scoped redactor URN key, raw `.hbs` source)` — `redactors/*.hbs`, already
    /// deploy-time VALIDATED here (a bad template fails `plan_deployment`, mirroring the `.dmn`
    /// parse above), compiled into a live `HbsContentRedactor` and registered on the actor
    /// (the reference implementation). The key is
    /// `archive_key(logical_urn("redactor", local_id), dep)` —
    /// `urn:sutra:redactor:<local_id>:<deploymentId>` — the extension OMITTED (redactor is
    /// single-engine), unlike `validators` above.
    redactors: Vec<(String, String)>,
    /// `(codec URN, xsd documents)` — compiled to [`StructuralCodec`]s on the actor.
    codecs: Vec<(String, Vec<Vec<u8>>)>,
    /// `(archive-scoped codec URN key, codec factory)` — the schema BUNDLES of `schemas/**`
    /// (a `codec-manifest.yaml` kind an extension codec crate serves), already deploy-time
    /// COMPILED here (a bad manifest / uncompilable schema fails `plan_deployment`, mirroring the
    /// `.dmn` and redactor paths above). The factory is `Send + Sync` so the plan stays
    /// send-able; the actor mints the `Rc` codec from it under
    /// `urn:sutra:codec:<local_id>:<deploymentId>`.
    codec_bundles: Vec<(String, sutra_codec_schema::bundle::BundleFactory)>,
    pub(crate) definitions: Vec<ChannelDefinition>,
    /// `direction: outbound` channels resolved fail-closed at plan time (`${ENV}` in the
    /// bind substituted; scheme validated) — `<q:send channel="…">` targets.
    outbound: Vec<ResolvedOutboundChannel>,
    /// Module-owned durable stores by name (shared pools — every dialect store is
    /// cheap-clone). The backend is picked by the connection URL scheme at plan time.
    stores: Vec<(String, StoreBackend)>,
    /// The deployment's COVERAGE store, built from its own `coverage` declaration in
    /// `datastores.yaml` (`datastore-schema-projection.md` §7): the author picks the database,
    /// the engine owns the schema and applies it on first use. `None` when the deployment
    /// declares no `coverage` store (`sutra lint` errors on that combination when
    /// `<q:coverage>` paths exist) — or when its connection could not be resolved, in which case
    /// `coverage_fault` carries the reason.
    coverage: Option<sutra_datastore::CoverageStore>,
    /// Why this deployment has no coverage store, when the cause was a fault rather than an
    /// absent declaration — surfaced verbatim by the reserved coverage ops instead of a silent
    /// 0%.
    coverage_fault: Option<String>,
    /// The per-deployment OpenAPI 3.1 surface, projected from THIS plan's channels +
    /// modules + declared stores at plan time. Served live at `GET /sutra/deployments/{id}/openapi`
    /// and refreshed on every flip; a spec build is infallible so it never fails a deploy.
    pub(crate) openapi_spec: Arc<serde_json::Value>,
}

impl DeploymentPlan {
    /// This plan's graph facts for the admin instance-migration validator: process id → node id →
    /// what that node can host ([`crate::migrate::DeploymentNodeIndex`]).
    ///
    /// Projected at ACTIVATION and published, rather than derived per request, for two reasons: a
    /// migration validates against BOTH the source and the target graph, and the source is by
    /// definition a deployment that has been flipped away from — so re-deriving it per call would
    /// mean re-verifying and re-planning a sealed archive on an admin request path.
    pub(crate) fn node_index(&self) -> crate::migrate::DeploymentNodeIndex {
        crate::migrate::DeploymentNodeIndex::of_modules(&self.modules)
    }

    /// The durable timer-start schedules this deployment arms when it becomes ACTIVE — one per
    /// `<startEvent>` carrying a `<timerEventDefinition>`, across every module it deploys.
    ///
    /// `now` is the ARMING instant, and it is what a relative `<timeDuration>` start counts from:
    /// "fire once, PT1H after the deployment activates" is exactly `now + PT1H`. An absolute
    /// `<timeDate>` (or a `<timeCycle>` with an anchored start) ignores `now` and may land in the
    /// PAST, in which case the row is written already-due and the poller fires it on its next
    /// tick — the documented past-date semantics.
    ///
    /// A timer whose arithmetic somehow fails is SKIPPED with a warning rather than failing the
    /// activation: the loader already validated every one of these at deploy time, so reaching
    /// here means something stranger than a bad model, and refusing to activate a whole
    /// deployment over one unschedulable timer is the worse trade.
    pub(crate) fn timer_schedules(
        &self,
        now: time::OffsetDateTime,
    ) -> Vec<sutra_persistence::stores::TimerScheduleArming> {
        let module_key = self.namespace.module_key();
        let mut out = Vec::new();
        for module in &self.modules {
            for process in module.processes() {
                for (node_id, timer) in process.timer_start_events() {
                    let due_at = match timer.first_due_at(now) {
                        Ok(due) => due,
                        Err(rejection) => {
                            warn!(
                                deployment = self.dep.value(),
                                process = %process.id,
                                node = node_id,
                                reason = %rejection,
                                "timer start could not be scheduled — skipped"
                            );
                            continue;
                        }
                    };
                    out.push(sutra_persistence::stores::TimerScheduleArming {
                        process_id: process.id.clone(),
                        node_id: node_id.to_string(),
                        tenant: self.namespace.tenant.clone(),
                        module_key: module_key.clone(),
                        kind: timer.kind_str().to_string(),
                        spec: timer.spec_text(),
                        next_due_at: due_at,
                        remaining_fires: timer.total_fires().map(|n| n as i32),
                    });
                }
            }
        }
        // Deterministic order keeps the arming transaction's statement order stable across
        // replicas (and makes the pg tests readable).
        out.sort_by(|a, b| {
            a.process_id
                .cmp(&b.process_id)
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        out.dedup_by(|a, b| a.process_id == b.process_id && a.node_id == b.node_id);
        out
    }
}

/// A module-owned durable store bound to its declared dialect (every supported dialect:
/// the connection URL scheme in `datastores.yaml` selects the driver — `postgres(ql)`,
/// `mysql`/`mariadb`, or `sqlserver`/`mssql`). All three are cheap-clone (shared pools),
/// so a [`DeploymentPlan`] stays `Clone` for the activation flip.
#[derive(Clone)]
pub(crate) enum StoreBackend {
    Pg(PostgresDataStore),
    Mysql(MysqlDataStore),
    Mssql(MssqlDataStore),
}

/// The connection dialect a `sql.url` names, taken from the URL's scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreDialect {
    Postgres,
    Mysql,
    Mssql,
}

/// Classify a store connection URL by its scheme. Accepts the native forms the dialect stores
/// normalise (`postgres://…`, `postgresql://…`, `mysql://…`, `mariadb://…`, `sqlserver://…`,
/// `mssql://…`). An unrecognised scheme is a fail-closed config error.
fn datastore_dialect(url: &str) -> Result<StoreDialect, DataStoreError> {
    let scheme = url
        .split([':', '/'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match scheme.as_str() {
        "postgres" | "postgresql" => Ok(StoreDialect::Postgres),
        "mysql" | "mariadb" => Ok(StoreDialect::Mysql),
        "sqlserver" | "mssql" => Ok(StoreDialect::Mssql),
        other => Err(DataStoreError::new(format!(
            "unsupported data-store connection scheme '{other}' (expected one of \
             postgres/postgresql, mysql/mariadb, sqlserver/mssql)"
        ))),
    }
}

/// Build the dialect-appropriate module store from its `datastores.yaml` definition,
/// dispatching on the resolved connection URL scheme. Fails closed exactly like
/// the single-dialect path did: an unresolvable connection (env-ref unset) or an
/// unsupported scheme is an `Err`, so the caller leaves the store unregistered.
///
/// `projected` is the store's resolved row PROJECTION (`Some` only when it declares a
/// `structure`): the provider then serves it from the author's typed-column table instead of the
/// generic `data_store` blob, and verifies that table on first use. `None` — every store today —
/// is byte-for-byte the historical behaviour, which is the compatibility guarantee.
pub(crate) fn build_store_backend(
    def: &StoreDefinition,
    migrations: Vec<String>,
    legacy_namespace: Option<(String, String, String)>,
    projected: Option<ProjectedStore>,
) -> Result<StoreBackend, DataStoreError> {
    let url = def
        .resolved("sql.url")?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            DataStoreError::new(format!(
                "data store '{}' declares no usable connection in datastores.yaml \
                 (sql.url + credentials, or *-ref pointing at a set env var)",
                def.name
            ))
        })?;
    match datastore_dialect(&url)? {
        StoreDialect::Postgres => Ok(StoreBackend::Pg(
            PostgresDataStore::from_definition_projected(
                def,
                migrations,
                legacy_namespace,
                projected,
            )?,
        )),
        StoreDialect::Mysql => Ok(StoreBackend::Mysql(
            MysqlDataStore::from_definition_projected(
                def,
                migrations,
                legacy_namespace,
                projected,
            )?,
        )),
        StoreDialect::Mssql => Ok(StoreBackend::Mssql(
            MssqlDataStore::from_definition_projected(
                def,
                migrations,
                legacy_namespace,
                projected,
            )?,
        )),
    }
}

/// Resolve a store's declared `structure` into the [`ProjectedStore`] its provider is built with
/// (design `datastore-schema-projection.md` §4.1 → §4.6).
///
/// Everything happens at PLAN time, and every failure refuses the deploy — the deliberate
/// asymmetry with an unresolvable CONNECTION, which only warns (an env-ref unset in this
/// environment is an operational fact; a structure that cannot be projected is a package fault
/// that no environment fixes). Serving it anyway is the outcome §4.2 refuses.
///
/// The type is resolved against the package's OWN `schemas/<folder>` XSD codecs — the same
/// compiled schemas the codecs use, so there is no second schema source of truth. A JSON-Schema
/// or bundle codec folder is refused with that stated plainly rather than projected from a model
/// that carries no facets (§4.5); the deploy-time linter draws the same line, so lint and runtime
/// agree on what is projectable.
fn resolve_projected_store(
    d: &LoadedDeployment,
    def: &StoreDefinition,
    structure: &sutra_datastore::StructureRef,
) -> Result<ProjectedStore, String> {
    let reference = structure.schema.trim();
    let local = reference.strip_prefix("urn:").unwrap_or(reference);
    let sources: Vec<&[u8]> = match d.codecs.get(local) {
        Some(xsds) if !xsds.is_empty() => xsds.iter().map(|a| a.content.as_bytes()).collect(),
        _ => {
            return Err(format!(
                "data store '{}' declares structure schema '{}', which this package provides no \
                 XSD codec for. A projected store's type must be declared by a schemas/<folder> \
                 XSD codec of the same package — only XSD carries the declared facets a typed \
                 column is checked against.",
                def.name, structure.schema
            ))
        }
    };
    let set = sutra_xsd::SchemaSet::compile(&sources).map_err(|e| {
        format!(
            "data store '{}' declares structure schema '{}', which does not compile: {e}",
            def.name, structure.schema
        )
    })?;
    let fields = set
        .schemas()
        .iter()
        .find_map(|schema| schema.fields_of(&structure.type_name))
        .ok_or_else(|| {
            format!(
                "data store '{}' declares structure type '{}', which schema '{local}' declares \
                 neither as a type nor as a root element.",
                def.name, structure.type_name
            )
        })?;
    let projection = structure.project(&fields).map_err(|e| {
        format!(
            "data store '{}' cannot be projected: [{}] {e}",
            def.name,
            e.code()
        )
    })?;
    ProjectedStore::new(&def.name, sutra_datastore::table_for(def), projection)
        .map_err(|e| e.to_string())
}

/// The assembled engine runtime — everything `serve` mounts and the activation
/// watcher drives.
pub(crate) struct EngineRuntime {
    /// The channel router the server mounts under `/sutra/health/*` — the inbound routes the
    /// HTTP transport contributes via [`sutra_transport_spi::TransportChannels::inbound_router`].
    /// The swappable route table itself now lives INSIDE the HTTP transport (in `transports`),
    /// which owns the route-side flip via its `rewire`.
    pub(crate) router: Router,
    pub(crate) engine: EngineHandle,
    /// Active + DRAINING deployment ids, read per tick by the outbox dispatcher and the
    /// timer poller; the watcher replaces it on flip/retire.
    pub(crate) deployments: LiveDeploymentSet,
    pub(crate) outbox: Option<OutboxDispatcherHandle>,
    /// The worker-facing pull surface, wired only with persistence (a parked task IS a database
    /// row). `None` makes the `/sutra/external-tasks/*` routes answer 503 rather than 404 — the
    /// honest distinction between "this needs a datasource" and "no such feature".
    pub(crate) external_tasks: Option<Arc<sutra_channels::ExternalTaskService>>,
    /// Every wired vendor transport, held behind the neutral [`TransportChannels`] trait
    /// (domain-neutrality refactor): the engine drives them by iterating — `rewire` on a
    /// activation flip, `drain`/`stop_all_detached` on shutdown — and names no broker. Order is
    /// the deterministic `transport_factories()` order.
    pub(crate) transports: Vec<Arc<dyn TransportChannels>>,
}

/// Assemble the engine runtime from the prepared plans: the engine actor (built ON its
/// thread from `Send` parts), the dynamic HTTP router, the broker consumers, and — with
/// the ENGINE-INTERNAL `pool` (the engine tables) — the outbox delivery loop (tick cadence
/// `outbox_tick_interval`). `None` pool runs without persistence (wait states fail
/// closed; no outbox dispatcher). `metrics_labels` is the telemetry wiring from
/// [`crate::otel::TelemetryConfig::metrics_wiring`]: `Some(allowlist)` registers the
/// OTel metrics listener on the executor, `None` keeps the lifecycle bus listener-free.
///
/// `draining_plans` is the boot-time DRAINING tail: definitions that serve NO new intake but
/// stay registered under their own deployment ids so instances PINNED to them keep resuming.
/// The `dir` source boots with an empty tail (nothing has been flipped away yet in this
/// process); the `db` source boots it from the deployment archive's `draining` rows, which is
/// what makes a pinned resume survive a pod restart rather than depending on a plan that only
/// ever lived in the memory of the replica that served the hot-deploy.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_engine_runtime(
    plans: Vec<DeploymentPlan>,
    draining_plans: Vec<DeploymentPlan>,
    pool: Option<PgPool>,
    handle: Handle,
    outbox_tick_interval: std::time::Duration,
    outbox_retry: crate::config::RetryConfig,
    metrics_labels: Option<Vec<String>>,
    payload_cap_bytes: u64,
    audit: crate::config::AuditConfig,
    key_provider: Option<Arc<dyn sutra_crypto::KeyProvider + Send + Sync>>,
    incident_sql: bool,
    instance_retention: std::time::Duration,
    deferred_acks: Arc<DeferredAckRegistry>,
    external_task_limits: sutra_channels::ExternalTaskLimits,
    // TEST-ONLY (P1-7): see `build_engine`'s parameter of the same name.
    now_override: Option<sutra_executor::TestClock>,
    // The shard-router knobs (`sutra.engine.shards` / `sutra.engine.shard-queue-capacity`):
    // the actor-lane count (≥ 1, load-validated) and the opt-in per-lane mailbox bound.
    engine_shards: crate::config::EngineShardConfig,
    // The activation-initial coverage snapshot the caller pre-read via
    // `seed_declared_coverage` (async — hence read BEFORE this sync assembly).
    initial_coverage: crate::otel::InitialCoverage,
) -> Result<EngineRuntime, Box<dyn std::error::Error>> {
    // Channel topology comes from the ACTIVE plans only — a draining definition claims no
    // binding and no route (it exists to be resumed into, never to accept new intake).
    let all_definitions: Vec<ChannelDefinition> = plans
        .iter()
        .flat_map(|plan| plan.definitions.iter().cloned())
        .collect();
    // The background loops (outbox dispatcher, timer poller) DO cover the draining tail —
    // pinned instances still fire timers and still drain emissions while their deployment winds
    // down (the same active+draining set `activate_plans` republishes on every flip).
    let deployment_ids: Vec<DeploymentId> = plans
        .iter()
        .chain(draining_plans.iter())
        .map(|plan| plan.dep.clone())
        .collect();

    let actor_handle = handle.clone();
    let actor_pool = pool.clone();
    let actor_labels = metrics_labels.clone();
    let actor_audit = audit.clone();
    let actor_key_provider = key_provider;
    let actor_deferred_acks = Arc::clone(&deferred_acks);
    let actor_now_override = now_override;
    let boot_plans = Arc::new(plans);
    let boot_draining = Arc::new(draining_plans);
    // The activation's read-only registries — built ONCE here (execution scale-out §2 row 10),
    // then handed to every lane. This is also why the plan set itself is an `Arc`: with the
    // registries shared, a per-lane plan clone would be the only remaining O(deployments × lanes)
    // copy, and it carries the archives' raw bytes.
    let shared = build_shared_registries(&boot_plans, &boot_draining);
    // The build closure is CLONED once per lane (`Fn + Clone` — execution scale-out
    // Phase 2): every lane builds its own `Rc`-based execution state, but over POINTERS to
    // the one shared registry set and the one shared plan set.
    let engine_handle: EngineHandle = spawn_engine_sharded(
        engine_shards.shards,
        engine_shards.queue_capacity,
        // The process runtime: lane I/O registers with ITS driver (never a per-lane
        // reactor — see `spawn_engine_sharded`), and it outlives every handle.
        handle.clone(),
        move |shard, lane_metrics| {
            build_engine(
                Arc::clone(&boot_plans),
                Arc::clone(&boot_draining),
                Arc::clone(&shared),
                actor_pool.clone(),
                actor_handle.clone(),
                actor_labels.clone(),
                payload_cap_bytes,
                actor_audit.clone(),
                actor_key_provider.clone(),
                incident_sql,
                instance_retention,
                Arc::clone(&actor_deferred_acks),
                actor_now_override.clone(),
                shard,
                lane_metrics,
                initial_coverage.clone(),
            )
        },
    );

    // Broker consumers for every registered vendor transport, wired GENERICALLY
    // (domain-neutrality refactor). Each `sutra-transport-<vendor>` crate self-registers a
    // `TransportFactory` (inventory); the engine iterates them, and each factory filters the
    // definitions by its own `transport:`, gates singletons (leader election), and detaches
    // its consumers (broker-absence is NON-FATAL). The wired managers ride the shutdown/
    // drain hooks + the activation flip — driven by the neutral `TransportChannels`
    // trait, so this assembly names NO broker. (v1: topology is wired at BOOT; a flip
    // rewires the boot-time managers — see crate::deploy.)
    let mut transports: Vec<Arc<dyn TransportChannels>> = Vec::new();
    // The inbound routes contributed by EVERY transport that serves over the process's shared
    // HTTP listener. More than one can now: http (the arbitrary user binds, dispatched by a
    // catch-all fallback), dapr (`POST /dapr/{topic}`) and knative (`POST /knative/{subscription}`)
    // each own a DISJOINT URL space on the one listener. Collected through the neutral
    // `inbound_router()` capability, NOT an `if transport == "http"` branch — channel
    // bind/activate is protocol-neutral. The routers are MERGED (see the loop) so each URL
    // reaches its owning transport.
    let mut channel_router: Option<Router> = None;
    for factory in transport_factories() {
        let channels = (factory.spawn)(
            &all_definitions,
            engine_handle.clone(),
            pool.clone(),
            envref_resolver,
            handle.clone(),
        )
        .map_err(|d| {
            format!(
                "{} channel wiring failed: [{}] {}",
                factory.transport, d.code, d.message
            )
        })?;
        if let Some(router) = channels.inbound_router() {
            // MERGE, not assign. axum allows merging routers when at most one carries a
            // fallback — only http does; dapr/knative contribute specific `/dapr/*` /
            // `/knative/*` routes — so the union dispatches every URL to its owning transport.
            // (The old last-writer-wins assignment let dapr/knative's empty catch-alls shadow
            // http's fallback → an all-`/channels/*`-404 regression on multi-transport images.)
            channel_router = Some(match channel_router.take() {
                Some(existing) => existing.merge(router),
                None => router,
            });
        }
        if channels.consumer_count() > 0 {
            info!(
                transport = factory.transport,
                consumers = channels.consumer_count(),
                "transport wired"
            );
        }
        transports.push(channels);
    }
    // The channel router the server mounts under its `/sutra/health/*` API. Empty when no
    // transport serves inbound HTTP (HTTP is always force-linked, so in the binary it is
    // always present); an empty router keeps the health API serving on its own.
    let router = channel_router.unwrap_or_default();

    // Startup diagnostic: an inbound definition declaring `ack-mode: on-complete`
    // on a transport that does not REALISE it (no deferred settle path, no
    // connection-hold) degrades LOUDLY to on-persist — never silently. Vendor-neutral:
    // the capability is each factory's self-declared `handles_on_complete`; this scan
    // names no transport.
    let on_complete_capable: std::collections::BTreeMap<&str, bool> = transport_factories()
        .iter()
        .map(|f| (f.transport, f.handles_on_complete))
        .collect();
    for def in all_definitions.iter().filter(|d| !d.is_outbound()) {
        let Some(transport) = def.transport.as_deref() else {
            continue;
        };
        if def.wants_on_complete_ack()
            && !on_complete_capable.get(transport).copied().unwrap_or(false)
        {
            warn!(
                code = sutra_channels::codes::ACK_ON_COMPLETE_UNSUPPORTED,
                channel = %def.binding.channel_name,
                transport,
                "channel declares ack-mode: on-complete but the transport has no deferred \
                 settle path yet — running on-persist (transport-side flow control)"
            );
        }
    }

    let deployments = LiveDeploymentSet::new(deployment_ids.clone());

    // The outbox delivery spine: one tick loop per engine walking the known
    // deployments; retry defaults (base PT1S, max PT5M, jitter, batch 50). Sinks by
    // destination scheme, registered GENERICALLY through the transport factories (each reads
    // its own engine-wide `SUTRA_SINK_*` config): HTTP(S) from the HTTP transport, one broker
    // sink per vendor transport. Every transport sink is registered unconditionally so its
    // scheme always resolves to a sink — an UNREGISTERED sink would poison every outbound row
    // for that scheme (the gate defect this guards against); an unconfigured sink fails a send
    // closed-retryable rather than dropping it.
    // The in-process delivery sink captures the engine handle (so it cannot ride the bare
    // `register_sink` fn-ptr the transport factories use) — cloned here, registered below.
    let local_sink_handle = engine_handle.clone();
    // The PULL surface, wired only with persistence: one notifier shared by the sink that parks
    // tasks and the service that long-polls for them, so a park wakes a waiting worker in the
    // same process without either side polling the database in a spin.
    let pull_notifier = sutra_channels::ExternalTaskNotifier::new();
    let external_task_rows: Option<Arc<dyn sutra_channels::ExternalTaskRows>> = pool
        .as_ref()
        .map(|pool| Arc::new(crate::external_task::PgExternalTaskRows::new(pool.clone())) as _);
    let external_tasks = external_task_rows.as_ref().map(|rows| {
        Arc::new(sutra_channels::ExternalTaskService::new(
            Arc::clone(rows),
            engine_handle.clone(),
            deployments.clone(),
            pull_notifier.clone(),
            external_task_limits.clone(),
        ))
    });
    let outbox_handle = pool.map(|pool| {
        // The delivery-side incident sink, opt-in via the SAME `sutra.incident.sql` gate as the
        // inbound dead-letter sink: a poisoned `<q:send required>` entry records ONE durable
        // incident. Async-safe (it spawns rather than blocks) because the dispatcher is a runtime
        // task — see `PgOutboxIncidentSink`.
        let outbox_incidents: Option<Arc<dyn sutra_channels::stores::IncidentSink + Send + Sync>> =
            incident_sql.then(|| {
                Arc::new(crate::outbox::PgOutboxIncidentSink::new(pool.clone()))
                    as Arc<dyn sutra_channels::stores::IncidentSink + Send + Sync>
            });
        let mut sinks = SinkRegistry::new();
        // Every transport (HTTP included) registers its own outbound sink through the same
        // factory hook — the HTTP(S) sink comes from the HTTP transport, no special case.
        for factory in transport_factories() {
            (factory.register_sink)(&mut sinks);
        }
        // The `local` scheme: in-process routing for co-deployed inter-process hops. One
        // scheme among the transports (uniform `send → channel`), registered DIRECTLY because
        // it holds the `EngineHandle` and re-enters `dispatch` off the actor thread.
        sinks.register(Arc::new(LocalDeliverySink::new(local_sink_handle.clone())));
        // The `pull` scheme: the delivery PARKS as a fetchable external task instead of being
        // dialed anywhere. Registered directly for the same reason `local` is — it holds engine
        // state (the row store + the long-poll notifier) the bare fn-ptr hook cannot carry.
        if let Some(rows) = &external_task_rows {
            sinks.register(Arc::new(sutra_channels::PullDeliverySink::new(
                Arc::clone(rows),
                pull_notifier.clone(),
                external_task_limits.retries,
            )));
        }
        // The retry curve is config-driven (`sutra.outbox.retry.*`); the values were
        // validated at config load (base > 0, max >= base) so `RetryPolicy::new` cannot panic.
        let mut dispatcher_build = OutboxDispatcher::new(
            Arc::new(PgOutboxRows::new(pool)),
            sinks,
            RetryPolicy::new(
                outbox_retry.base_delay,
                outbox_retry.max_delay,
                outbox_retry.jitter,
            )
            // `None` keeps the retry-forever posture the outbox has always had; a configured
            // ceiling makes an exhausted entry terminal + incident-raising instead.
            .with_max_attempts(outbox_retry.max_attempts),
            50,
        );
        if let Some(incidents) = outbox_incidents {
            dispatcher_build = dispatcher_build.with_incident_sink(incidents);
        }
        // The channel-call `<q:retry>` poison wake: a node-bearing row going TERMINAL prompts
        // the engine to offer the failure to the parked task's retry policy NOW instead of at
        // its timeout. Spawned as a fresh serialized actor turn (the timer-poller pattern) —
        // never a nested call, and never blocking the drain loop. Best-effort by contract:
        // the engine validates against durable facts, and a lost prompt is recovered by the
        // call's <q:timeout> boundary.
        let poison_wake_handle = engine_handle.clone();
        dispatcher_build = dispatcher_build.with_poison_notifier(move |poisoned| {
            let engine = poison_wake_handle.clone();
            tokio::spawn(async move {
                let fire = sutra_channels::ChannelCallPoisonFire {
                    deployment: poisoned.deployment,
                    instance_id: poisoned.instance_id,
                    node_id: poisoned.node_id,
                };
                match engine.fail_channel_call(fire.clone()).await {
                    Ok(outcome) => info!(
                        instance = %fire.instance_id,
                        node = %fire.node_id,
                        ?outcome,
                        "channel-call poison wake handled"
                    ),
                    Err(e) => warn!(
                        instance = %fire.instance_id,
                        node = %fire.node_id,
                        code = %e.code,
                        error = %e.message,
                        "channel-call poison wake failed — the task's <q:timeout> boundary \
                         remains the failure detector"
                    ),
                }
            });
        });
        let dispatcher = Arc::new(
            dispatcher_build
                // Outbound HTTP auth-header material resolves through the engine's shared envref
                // registry — the SAME uniform secret path broker transports resolve their connection
                // creds through — so `env:`/`secret:`/`vault:`/`aws-secrets:` refs all resolve at
                // delivery (not env: only). The full secret-ref is passed through, scheme and all.
                .with_secret_env(move |full_ref| crate::envref::resolve_value(full_ref).ok()),
        );
        info!(
            deployments = deployment_ids.len(),
            tick = ?outbox_tick_interval,
            transports = transport_factories().len(),
            "outbox dispatcher spawned (http/https + one broker sink per registered transport)"
        );
        spawn_dispatch_loop(
            &handle,
            dispatcher,
            deployments.clone(),
            outbox_tick_interval,
        )
    });
    Ok(EngineRuntime {
        router,
        engine: engine_handle,
        deployments,
        outbox: outbox_handle,
        external_tasks,
        transports,
    })
}

/// Coverage seed-at-deploy — HOISTED out of the per-shard engine build (execution
/// scale-out §2 row 11): the seed is idempotent (`ON CONFLICT DO NOTHING` — it never
/// clobbers an already-covered flag on a replica boot) but would run S× redundantly once
/// the router spawns N lanes, so it runs ONCE per activation at the runtime-assembly
/// level — `serve` awaits it before the boot engine spawns, `activate_plans` awaits it
/// before the flip's engine rebuild. Both orderings preserve what the old actor-thread
/// placement provided: seeded before any dispatch, and before `record_initial_coverage`
/// reads the rows back.
///
/// The "total to cover" = intra-process path ids ∪ cross-process ROUTE urns — NOT the
/// `#<process>` injected sub-paths (those are marking cursors); `seed_urns` derives that
/// set from each plan's declared `coverage_paths` (the desugared form of
/// `deployment.coverages`) and seeds `covered=false` per deployment. The seed also
/// FIRST-USES each declared coverage store, so the engine's DDL is applied here
/// (idempotent, lock-serialised) rather than at the first mark. Best-effort per
/// deployment — a failed seed under-reports metrics until the first mark / next deploy,
/// it never fails the activation.
///
/// Since Phase 3 it ALSO reads the covered flags back — ONE `covered_among` query per
/// process — and returns the [`crate::otel::InitialCoverage`] snapshot the per-lane engine
/// build seeds its gauge covered-sets from (`apply_initial_coverage`). Hoisted here for
/// the same reason as the seed (§2 row 11: once per activation, not S×) plus a Phase 3
/// necessity: the flip's rebuild runs ON a lane's async actor task, where the old
/// per-build `Handle::block_on` read would panic. Best-effort identically: a failed read
/// counts every path as uncovered and the gauge climbs from the first live mark.
pub(crate) async fn seed_declared_coverage(
    active: &[DeploymentPlan],
    draining: &[DeploymentPlan],
) -> crate::otel::InitialCoverage {
    let mut initial = crate::otel::InitialCoverage::new();
    for plan in active.iter().chain(draining.iter()) {
        let Some(store) = &plan.coverage else {
            continue;
        };
        let declared = plan
            .modules
            .iter()
            .flat_map(|m| m.processes())
            .flat_map(|p| p.coverage_paths.iter().map(|c| c.id.clone()));
        let urns = sutra_executor::coverage::seed_urns(declared);
        if !urns.is_empty() {
            if let Err(e) = store.seed_declared(plan.dep.value(), &urns).await {
                warn!(
                    deployment = %plan.dep.value(),
                    error = %e,
                    "coverage metric seed failed (best-effort) — metrics may under-report until \
                     first mark / next deploy"
                );
            }
        }
        // Read the covered flags back per process — the activation-initial gauge snapshot.
        for module in &plan.modules {
            for process in module.processes() {
                if process.coverage_paths.is_empty() {
                    continue;
                }
                let paths: Vec<String> = process
                    .coverage_paths
                    .iter()
                    .map(|p| p.id.clone())
                    .collect();
                let flag_urns: Vec<String> = paths
                    .iter()
                    .map(|p| sutra_executor::metric_flag_urn(p))
                    .collect();
                let flagged = store
                    .covered_among(plan.dep.value(), &flag_urns)
                    .await
                    .unwrap_or_default();
                initial.insert(
                    (plan.dep.value().to_string(), process.id.clone()),
                    crate::otel::covered_paths_of(&paths, &flagged),
                );
            }
        }
    }
    initial
}

/// The engine's `envref` resolver, handed to every transport factory's `spawn` as a `fn`
/// pointer ([`sutra_transport_spi::EnvRefResolver`]): the engine owns the registry
/// (`env:`/`secret:`/`vault:` + `${…}`), and the extracted transport crates (HTTP's
/// inbound-auth, the brokers' inbound-auth + credentials) resolve through it — so they depend
/// on no engine module and channel binding stays protocol-neutral.
pub(crate) fn envref_resolver(reference: &str) -> Result<String, String> {
    crate::envref::resolve_value(reference).map_err(|e| e.to_string())
}

/// The env var narrowing the permitted transports below what the binary bundles — a
/// comma-separated allow-list (e.g. `file` or `file,http`). Unset ⇒ every bundled transport is
/// permitted. Names not bundled are simply absent from the effective set (fail-closed).
pub(crate) const ALLOWED_TRANSPORTS_ENV: &str = "SUTRA_ALLOWED_TRANSPORTS";

/// The engine-internal `transport: local` discriminator: an INTERNAL channel routed
/// in-process (no external listener, no auth) or a `local://` outbound bind. It is NOT a
/// network protocol — no `TransportFactory` claims it — so it is always permitted, even in a
/// hardened / allow-listed engine (the allow-list governs external protocols only).
pub(crate) const LOCAL_TRANSPORT: &str = "local";

/// The engine-internal `transport: pull` discriminator: an outbound channel whose deliveries
/// PARK as fetchable external tasks instead of being dialed. Like [`LOCAL_TRANSPORT`] it is not
/// a network protocol — no `TransportFactory` claims it, no listener is opened, no credential is
/// presented — so it is always permitted; the allow-list governs external protocols only.
pub(crate) const PULL_TRANSPORT: &str = sutra_channels::PULL_SCHEME;

/// The transports a deployment may bind to: the bundled set
/// ([`sutra_transport_spi::transport_factories`], cargo-feature-selected) intersected with the
/// [`ALLOWED_TRANSPORTS_ENV`] allow-list when set, plus the always-permitted engine-internal
/// [`LOCAL_TRANSPORT`] and [`PULL_TRANSPORT`]. This is the transport GOVERNANCE policy — a
/// hardened engine bundles (and/or allow-lists) only what it permits.
pub(crate) fn allowed_transports() -> std::collections::BTreeSet<String> {
    let bundled: std::collections::BTreeSet<String> = transport_factories()
        .iter()
        .map(|f| f.transport.to_string())
        .collect();
    let mut effective = match std::env::var(ALLOWED_TRANSPORTS_ENV) {
        Ok(list) if !list.trim().is_empty() => {
            let allow: std::collections::BTreeSet<String> = list
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            bundled.intersection(&allow).cloned().collect()
        }
        _ => bundled,
    };
    // In-process routing and pull-parking are engine-internal, never wire protocols — always
    // permitted.
    effective.insert(LOCAL_TRANSPORT.to_string());
    effective.insert(PULL_TRANSPORT.to_string());
    effective
}

/// Expand a `local://<channel>` or `pull://<channel>` outbound bind into the fully-qualified
/// `<scheme>://<module_key>/<channel>` destination the [`LocalDeliverySink`] / the pull sink
/// reconstruct. A bare (single-segment) target is qualified with the deployment's `module_key`
/// (`<tenant>/<module>/<version>`) — co-deployed hops share it; any other destination (another
/// scheme, or a path already carrying a `/`) is returned verbatim.
///
/// Both schemes take the same grammar because they name the same thing: the inbound channel the
/// hop is delivered to. `local://` delivers there immediately; `pull://` parks the delivery for
/// a worker first and delivers its RESULT there instead.
pub(crate) fn expand_local_destination(destination: &str, module_key: &str) -> String {
    for scheme in [LOCAL_TRANSPORT, PULL_TRANSPORT] {
        let prefix = format!("{scheme}://");
        if let Some(target) = destination.strip_prefix(&prefix) {
            return if !target.is_empty() && !target.contains('/') {
                format!("{prefix}{module_key}/{target}")
            } else {
                destination.to_string()
            };
        }
    }
    destination.to_string()
}

/// Fail CLOSED if any channel declares a transport not in `allowed`. Channels with no declared
/// transport are left to the existing per-direction checks (an outbound with no transport is
/// rejected there; an inbound with none simply binds to nothing) — this gate only governs a
/// DECLARED transport against the policy, so it never changes behaviour on a full/default build.
pub(crate) fn reject_disallowed_transports(
    definitions: &[ChannelDefinition],
    allowed: &std::collections::BTreeSet<String>,
    dep: &sutra_executor::DeploymentId,
) -> Result<(), Box<dyn std::error::Error>> {
    for def in definitions {
        let Some(transport) = def
            .transport
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        else {
            continue;
        };
        // In-process routing is engine-internal, never a wire protocol — always permitted,
        // even when the allow-list narrows the external transports.
        if transport == LOCAL_TRANSPORT || transport == PULL_TRANSPORT {
            continue;
        }
        if !allowed.contains(transport) {
            let permitted = allowed
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "channel '{}' of deployment {dep} declares transport '{transport}', which is \
                 not permitted in this engine (permitted transports: [{permitted}]). This engine \
                 bundles/allows only those transports (SUTRA_ALLOWED_TRANSPORTS + the linked \
                 transport crates); a hardened build rejects any other protocol.",
                def.binding.channel_name
            )
            .into());
        }
    }
    Ok(())
}

/// A `redactors/**` archive-local path → its URN local-id: `/` → `:` folder-delimiting, the `.hbs`
/// extension OMITTED (redactor is a single-engine artifact type — unlike template/script/rule,
/// which keep the extension because they admit more than one authoring engine). Mirrors
/// `sutra_loader::coverage::coverage_urn`'s path-building (also single-engine,
/// extension-omitting), but returns just the local-id — the caller supplies the `redactor`
/// type segment to [`sutra_executor::logical_urn`]. e.g. `myschema/accounts.hbs` →
/// `myschema:accounts`.
fn redactor_local_id(subpath: &str) -> String {
    let mut parts: Vec<&str> = subpath.split('/').collect();
    let file = parts.pop().unwrap_or("");
    let stem = file.strip_suffix(".hbs").unwrap_or(file);
    let mut id = String::new();
    for folder in parts {
        id.push_str(folder);
        id.push(':');
    }
    id.push_str(stem);
    id
}

/// A `rules/**` archive-local path → its URN local-id: `/` → `:` folder-delimiting, the
/// file extension KEPT — rule is a multi-engine artifact type (`.dmn` vs `.srl`), so the
/// extension disambiguates the authoring engine and two same-named files. Unlike
/// [`redactor_local_id`] (single-engine, extension-omitting), no suffix is stripped.
/// e.g. `pricing/tiers.dmn` → `pricing:tiers.dmn`.
fn rule_local_id(subpath: &str) -> String {
    subpath.replace('/', ":")
}

/// Prepare one deployment's Send-able parts (all parsing fail-closed). `d.id` is THE
/// runtime identity: the manifest-hash id for archive-sourced deployments, the
/// legacy triple-derived shim id for tree-scanned ones — every binding, registry key and
/// persistence row downstream keys on it opaquely.
pub(crate) fn plan_deployment(
    d: &LoadedDeployment,
) -> Result<DeploymentPlan, Box<dyn std::error::Error>> {
    let dep = d.id.clone();

    // BPMN modules — deduplicate the per-process Arc map back to unique parsed files.
    let mut modules: Vec<Arc<ProcessModule>> = Vec::new();
    for module in d.processes.values() {
        if !modules.iter().any(|m| Arc::ptr_eq(m, module)) {
            modules.push(Arc::clone(module));
        }
    }

    let templates = d
        .templates
        .iter()
        .map(|(id, a)| {
            (
                dep.artifact(ArtifactType::Template, id),
                a.content.clone().into_bytes(),
            )
        })
        .collect();
    let scripts = d
        .scripts
        .iter()
        .map(|(id, a)| {
            (
                dep.artifact(ArtifactType::Script, id),
                a.content.clone().into_bytes(),
            )
        })
        .collect();
    // rules/ is the single rule slot (the older `decisions/` folder merged in). BOTH engines
    // are dispatched to BOTH roles — a businessRuleTask DECISION (raw bytes, DecisionRegistry)
    // and a complexValidator tier-2 VALIDATOR (ValidatorRegistry) — because the same file may be
    // referenced either way. `.dmn` goes through `sutra-dmn`, `.srl` through the rule-DSL
    // engine; a module may therefore SPLIT one tier-2 ruleset across both files and attach both
    // to the same `<q:validators>` chain (a mixed rail module does exactly that: the
    // clock-dependent window rules stay DMN, the stateless field rules are `.srl`).
    let mut decisions: Vec<(String, Vec<u8>)> = Vec::new();
    let mut validators = Vec::new();
    let mut srl_validators: Vec<(String, String)> = Vec::new();
    for (id, artifact) in &d.rules {
        if id.ends_with(".dmn") {
            decisions.push((
                dep.artifact(ArtifactType::Decision, id),
                artifact.content.clone().into_bytes(),
            ));
            let defs = sutra_dmn::DmnFileLoader::new()
                .load(artifact.content.as_bytes())
                .map_err(|e| format!("rule '{id}' of deployment {dep} failed to parse: {e}"))?;
            // Validator keys are archive-scoped URNs:
            // `archive_key(logical_urn("rule", local_id), dep)`. Each decision is
            // reachable by its OWN declared id (no path/extension — a decision name, not a file);
            // a single-decision file is ALSO reachable by its archive path (extension KEPT — rule
            // is multi-engine, `.dmn`/`.srl`) as a convenience alias.
            for decision in defs.decisions() {
                let logical = logical_urn("rule", &decision.id);
                validators.push((archive_key(&logical, &dep), decision.clone()));
            }
            if let [single] = defs.decisions() {
                let local_id = rule_local_id(id);
                let logical = logical_urn("rule", &local_id);
                validators.push((archive_key(&logical, &dep), single.clone()));
            }
        } else if id.ends_with(".srl") {
            // The Sutra Rule Language ruleset. Fail-closed parse at assembly (a broken
            // `.srl` fails the deploy, mirroring the `.dmn` path), then route its bytes into the
            // decision registry so `<businessRuleTask>` evaluates it via the `SrlEngine` AND
            // register it as a tier-2 validator so `<q:complexValidator source="…​.srl">` resolves
            // (the invocation surface is businessRuleTask + the
            // complexValidator tier — a `.srl` ruleset reporting issues is exactly a content
            // validator, and this is what lets one module's ruleset span both engines). A ruleset
            // has no decision id, so its only key is the archive path (extension KEPT).
            sutra_srl::parse(&artifact.content)
                .map_err(|e| format!("rule '{id}' of deployment {dep} failed to parse: {e}"))?;
            decisions.push((
                dep.artifact(ArtifactType::Decision, id),
                artifact.content.clone().into_bytes(),
            ));
            let logical = logical_urn("rule", &rule_local_id(id));
            srl_validators.push((archive_key(&logical, &dep), artifact.content.clone()));
        } else {
            warn!(
                deployment = dep.value(),
                rule = id,
                "unsupported rule artifact extension — rules/ admits .dmn and .srl"
            );
        }
    }

    // redactors/*.hbs — the redactor-URN reference implementation: validated HERE,
    // fail-closed, exactly like an
    // invalid rules/*.dmn fails the plan above — a syntactically broken template must reject the
    // DEPLOY, never surface as a live `ContentRedactor::locate` failure. The archive-local path
    // becomes the URN local-id (folder `/` → `:`, `.hbs` omitted — redactor is single-engine);
    // the registry key is the archive scope (`urn:sutra:redactor:<local_id>:<deploymentId>`) —
    // build_engine folds these into the `RedactorRegistry` alongside the built-ins (Mirror 2).
    let mut redactors: Vec<(String, String)> = Vec::new();
    for (id, artifact) in &d.redactors {
        HbsContentRedactor::new(&artifact.content)
            .map_err(|e| format!("redactor '{id}' of deployment {dep} failed to compile: {e}"))?;
        let local_id = redactor_local_id(id);
        let logical = logical_urn("redactor", &local_id);
        let key = archive_key(&logical, &dep);
        redactors.push((key, artifact.content.clone()));
    }

    // Schema BUNDLES — a `schemas/<name>/` folder whose `codec-manifest.yaml` declares a kind an
    // extension codec crate serves (registered through `sutra_codec_schema::bundle`, the same
    // inventory pull model as the built-in codecs; this assembly names none of them). The whole
    // folder tree is compiled HERE, fail-closed — a bad manifest or an uncompilable schema refuses
    // the deploy, exactly like an invalid rules/*.dmn above — and the resulting factory is carried
    // to the actor, which mints the codec under the ARCHIVE scope
    // (`urn:sutra:codec:<local_id>:<deploymentId>`).
    // A bundle whose folder is named after a built-in codec therefore SHADOWS that built-in for
    // this deployment only, and two archive versions with different mappings coexist.
    let schema_files: std::collections::BTreeMap<String, Vec<u8>> = d
        .schema_files
        .iter()
        .map(|(subpath, artifact)| (subpath.clone(), artifact.content.clone().into_bytes()))
        .collect();
    let planned_bundles = sutra_codec_schema::plan_schema_bundles(&schema_files).map_err(|e| {
        format!(
            "schemas/ of deployment {dep} failed to load: [{}] {}",
            e.code(),
            e.message()
        )
    })?;
    let bundle_folders: std::collections::HashSet<String> = planned_bundles
        .iter()
        .map(|bundle| bundle.local_id.clone())
        .collect();
    let codec_bundles: Vec<(String, sutra_codec_schema::bundle::BundleFactory)> = planned_bundles
        .into_iter()
        .map(|bundle| {
            let logical = logical_urn("codec", &bundle.local_id);
            (archive_key(&logical, &dep), bundle.make)
        })
        .collect();

    // User codec schemas → the canonical `urn:<path-derived name>` reference (the codec map
    // is keyed by the path-derived name; the engine registers each under that URN and
    // `channels.yaml codec:` resolves it). A folder claimed by a bundle above is NOT also a
    // per-folder structural codec — the bundle owns its whole tree.
    let codecs = d
        .codecs
        .iter()
        .filter(|(name, _)| !bundle_folders.contains(*name))
        .map(|(name, xsds)| {
            (
                format!("urn:{name}"),
                xsds.iter()
                    .map(|a| a.content.clone().into_bytes())
                    .collect(),
            )
        })
        .collect();

    // Channels — parsed here so a bad channels.yaml refuses startup (fail-closed).
    // The label triple stays observability-only; the binding pointer is stamped with
    // THE deployment id right below (channels bind to deploymentIds).
    let mut definitions = match &d.channels_yaml {
        Some(yaml) => load_channel_definitions(
            yaml.as_bytes(),
            &d.tenant,
            &d.module,
            &d.version,
            "channels.yaml",
        )
        .map_err(|diag| {
            format!(
                "channels.yaml of deployment {} failed to load: [{}] {}",
                dep, diag.code, diag.message
            )
        })?,
        None => Vec::new(),
    };
    for def in &mut definitions {
        def.binding.deployment = dep.clone();
    }

    // Transport GOVERNANCE (domain-neutrality refactor): fail the deployment CLOSED if any
    // channel declares a transport this engine does not permit — the allowed set is the
    // bundled transports (cargo-feature-selected `transport_factories()`), optionally narrowed
    // by `SUTRA_ALLOWED_TRANSPORTS`. A hardened / air-gapped engine (e.g. file-only) thereby
    // REJECTS a process that would reach data over a forbidden protocol, rather than silently
    // leaving its channels unbound. (`/sutra/health/*` is unaffected — it is the engine's own
    // infra liveness API, not a channel transport.)
    reject_disallowed_transports(&definitions, &allowed_transports(), &dep)?;

    // Outbound channels (`direction: outbound`) — `<q:send channel>` targets, resolved
    // fail-closed at startup (an invalid outbound binding aborts the load): the bind
    // must be a scheme-bearing destination URI; `${ENV}` references (15-factor secret
    // indirection, e.g. a partner callback host) substitute BEFORE parsing. Error
    // messages keep the RAW bind so a resolved secret is never echoed.
    let mut outbound = Vec::new();
    for def in &definitions {
        if !def.is_outbound() {
            continue;
        }
        let name = def.binding.channel_name.clone();
        let transport = def.transport.clone().filter(|t| !t.trim().is_empty()).ok_or_else(|| {
            format!("Outbound channel '{name}' of deployment {dep} is invalid: it declares no transport")
        })?;
        let raw_bind = def
            .bind_spec
            .clone()
            .filter(|b| !b.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "Outbound channel '{name}' of deployment {dep} is invalid: it declares \
                     no 'bind' destination URI"
                )
            })?;
        let destination = crate::envref::resolve_placeholders(raw_bind.trim()).map_err(|e| {
            format!("Outbound channel '{name}' of deployment {dep}: 'bind' has an unresolved reference: {e}")
        })?;
        if scheme_of(&destination).is_none() {
            return Err(format!(
                "Outbound channel '{name}' of deployment {dep} is invalid: 'bind' ({raw_bind}) \
                 has no URI scheme — an outbound destination must be a full URI a MessageSink \
                 resolves by scheme (e.g. https://host/path, rabbitmq://exchange/rk, \
                 local://<internal-channel>)"
            )
            .into());
        }
        // A `local://<channel>` bind (in-process routing) expands to the fully-qualified
        // `local://<module_key>/<channel>` destination the `LocalDeliverySink` reconstructs an
        // InboundMessage from — co-deployed hops share this deployment's module_key. An
        // already-qualified path is left as authored.
        let destination =
            expand_local_destination(&destination, &def.binding.namespace.module_key());
        outbound.push(ResolvedOutboundChannel::resolve(
            &name,
            &transport,
            &destination,
            def.auth_scheme.as_deref(),
            def.properties.get("secret-ref").map(|s| s.as_str()),
            def.properties.get("header").map(|s| s.as_str()),
            def.cloud_events_mode.as_deref().unwrap_or("none"),
        ));
    }

    // Data stores — each store OWNS its connection (datastores.yaml, env-indirected).
    // Pre-cutover compat: INSERTs stamp the legacy namespace triple (see sutra-datastore).
    let mut stores = Vec::new();
    let mut coverage = None;
    let mut coverage_fault = None;
    if let Some(yaml) = &d.datastores_yaml {
        let parsed = sutra_datastore::parse_datastores(yaml)
            .map_err(|e| format!("datastores.yaml of deployment {dep} failed to load: {e}"))?;
        for def in &parsed {
            if def.store_type != "sql" {
                warn!(
                    deployment = dep.value(),
                    store = def.name,
                    store_type = def.store_type,
                    "non-sql data store skipped"
                );
                continue;
            }
            // The reserved COVERAGE store: same declaration, same connection machinery — but the
            // engine owns its schema (§7), so it is built as the deployment's coverage store and
            // NOT as a key→value business store. It carries no package migrations; the engine's
            // own DDL is applied to it on first use.
            if def.name == sutra_datastore::COVERAGE_STORE_NAME {
                match sutra_datastore::CoverageStore::from_definition(def) {
                    Ok(store) => {
                        info!(
                            deployment = dep.value(),
                            "coverage store bound — coverage marks persist in the declared \
                             'coverage' store (engine-owned schema, applied on first use)"
                        );
                        coverage = Some(store);
                    }
                    Err(e) => {
                        // Loud, not silent: a DECLARED coverage store that cannot be built is a
                        // fault, and the reserved coverage ops will name it rather than report a
                        // 0% that reads like a real measurement.
                        error!(
                            deployment = dep.value(),
                            error = %e,
                            "coverage store declared but unusable — no coverage will be recorded \
                             for this deployment; coverage:report / coverage:reset will fail"
                        );
                        coverage_fault = Some(e.to_string());
                    }
                }
                continue;
            }
            // Tree-scanned deployments read migrations off disk (relative to the binding
            // dir); a sealed archive has no backing directory — its `migrations/**`
            // travel in-memory on the LoadedDeployment (archive migrations are datastore-scoped).
            let migrations = if d.binding_dir.as_os_str().is_empty() {
                archive_migrations(d, def)?
            } else {
                sutra_datastore::load_migrations(def, &d.binding_dir).map_err(|e| {
                    format!("migrations of store '{}' failed to load: {e}", def.name)
                })?
            };
            // A declared `structure` is resolved to its projection HERE, at plan time, and a
            // fault refuses the deploy (unlike an unresolvable connection below, which warns).
            let projected = match &def.structure {
                None => None,
                Some(structure) => {
                    Some(resolve_projected_store(d, def, structure).map_err(|e| {
                        format!("datastores.yaml of deployment {dep} failed to load: {e}")
                    })?)
                }
            };
            match build_store_backend(
                def,
                migrations,
                Some((d.tenant.clone(), d.module.clone(), d.version.clone())),
                projected,
            ) {
                Ok(store) => stores.push((def.name.clone(), store)),
                Err(e) => {
                    // The provider builds per-store connection pools lazily — an
                    // unresolvable connection (env ref not set in this environment) must
                    // not refuse boot; the store is simply not usable until configured.
                    warn!(
                        deployment = dep.value(),
                        store = def.name,
                        error = %e,
                        "data store connection unresolvable — store NOT registered \
                         (first use will fail with an unknown-store diagnostic)"
                    );
                }
            }
        }
    }

    info!(
        deployment = dep.value(),
        modules = modules.len(),
        channels = definitions.len(),
        outbound = outbound.len(),
        stores = stores.len(),
        codecs = d.codecs.len(),
        "deployment planned"
    );

    // Project this deployment's API surface once, at plan time — the same parsed
    // channels + modules + declared stores the engine will serve. The declared-store inventory
    // reflects INTENT (every parsed store), independent of runtime connection-bindability, so it
    // is sourced from the parsed definitions, not the `stores` backends built above.
    let store_defs: Vec<sutra_datastore::StoreDefinition> = d
        .datastores_yaml
        .as_deref()
        .map(|y| sutra_datastore::parse_datastores(y).unwrap_or_default())
        .unwrap_or_default();
    let openapi_spec = Arc::new(sutra_openapi::deployment_spec(
        &sutra_openapi::DeploymentApi {
            deployment_id: dep.value(),
            tenant: &d.tenant,
            module: &d.module,
            version: &d.version,
            channels: &definitions,
            modules: &modules,
            stores: &store_defs,
        },
    ));

    Ok(DeploymentPlan {
        dep,
        namespace: sutra_channels::Namespace::new(&d.tenant, &d.module, &d.version),
        modules,
        templates,
        scripts,
        decisions,
        validators,
        srl_validators,
        redactors,
        codecs,
        codec_bundles,
        definitions,
        outbound,
        stores,
        coverage,
        coverage_fault,
        openapi_spec,
    })
}

/// A store's migrations from the archive's in-memory `migrations/**` map. The packager
/// pins `sql.migrations` to the canonical `migrations/<store>` (datastore-scoped), and the map
/// keys are `<store>/<file>.sql` relative to `migrations/` — scripts sort by filename,
/// mirroring `sutra_datastore::load_migrations`.
fn archive_migrations(
    d: &LoadedDeployment,
    def: &sutra_datastore::StoreDefinition,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let Some(rel) = def
        .properties
        .get("sql.migrations")
        .filter(|s| !s.trim().is_empty())
    else {
        return Ok(Vec::new());
    };
    let Some(store_dir) = rel.trim().strip_prefix("migrations/") else {
        return Err(format!(
            "data store '{}' of deployment {} declares migrations at '{rel}'; a sealed \
             archive requires the canonical 'migrations/<store>' form",
            def.name, d.id
        )
        .into());
    };
    let prefix = format!("{}/", store_dir.trim_end_matches('/'));
    let scripts: Vec<String> = d
        .migrations
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix) && key.ends_with(".sql"))
        .map(|(_, artifact)| artifact.content.clone())
        .collect();
    if scripts.is_empty() {
        return Err(format!(
            "data store '{}' of deployment {} declares migrations at '{rel}' but the \
             archive carries no scripts under it",
            def.name, d.id
        )
        .into());
    }
    Ok(scripts)
}

/// Fold `plan`'s archive-scoped `redactors/*.hbs` into `registry`, under the exact key
/// `plan_deployment` minted (`urn:sutra:redactor:<local_id>:<deploymentId>`) — the redactor-URN
/// reference implementation's Mirror-2 fold-in step, called from [`build_engine`] alongside the
/// `validators.register_under(...)` loop above. Rebuilding [`HbsContentRedactor`] here (rather
/// than carrying the compiled object across the plan) is infallible in practice: `plan_deployment`
/// already validated every template compiles (fail-closed at plan time), so a second failure here
/// would mean the plan itself is corrupt — a bug, not a deploy-time condition — hence the
/// `expect`, mirroring how `build_engine`'s other folds (e.g. `DmnRulesetValidator::new`) treat
/// their plan-supplied inputs as already-validated.
fn register_archive_redactors(registry: &mut RedactorRegistry, plan: &DeploymentPlan) {
    for (key, source) in &plan.redactors {
        let redactor = HbsContentRedactor::new(source)
            .expect("redactor template already validated at plan time (plan_deployment)");
        registry.register_under(key, redactor);
    }
}

/// Build the encryption-at-rest / GDPR-blind-index key provider once, at boot, in the async
/// `serve()` context — the envelope path reads the `data_key` store, and the runtime-less actor
/// thread that runs [`build_engine`] cannot `.await`. The single provider is shared by the
/// persistence bridge (encryption at rest) and the erasure surface (blind-index recompute);
/// `None` = plaintext-v2 (no key source configured).
///
/// Envelope mode (`sutra.crypto.envelope.enabled`) resolves the KEK whole-value from the envref/
/// KMS registry, loads the sealed `key_id → WrappedDataKey` map from `data_key`, and builds an
/// [`sutra_crypto::EnvelopeKeyProvider`]. Otherwise the master key (`sutra.crypto.master-key`)
/// builds an [`sutra_crypto::HkdfKeyProvider`]. The two are mutually exclusive (config validates it).
pub(crate) async fn build_key_provider(
    pool: Option<&PgPool>,
    config: &crate::config::EngineConfig,
) -> Result<Option<Arc<dyn sutra_crypto::KeyProvider + Send + Sync>>, String> {
    if config.crypto_envelope.enabled {
        let kek_ref = config
            .crypto_envelope
            .kek_ref
            .as_deref()
            .expect("config load validated: envelope.enabled ⇒ kek set");
        let secret = crate::envref::resolve_value(kek_ref)
            .map_err(|e| format!("sutra.crypto.envelope.kek: {e}"))?;
        let kek = sutra_crypto::Kek::from_secret(secret.as_bytes());
        let pool = pool.ok_or_else(|| {
            "sutra.crypto.envelope.enabled requires a datasource (the data_key store holds the \
             wrapped DEKs)"
                .to_string()
        })?;
        let wrapped = sutra_persistence::stores::PgDataKeyStore::new(pool.clone())
            .list_all()
            .await
            .map_err(|e| format!("loading wrapped DEKs from data_key: {e}"))?;
        let map: std::collections::HashMap<String, sutra_crypto::WrappedDataKey> = wrapped
            .into_iter()
            .map(|w| (w.key_id().to_string(), w))
            .collect();
        info!(
            keys = map.len(),
            "envelope key provider active (KEK-wrap; DEKs loaded from data_key)"
        );
        Ok(Some(
            Arc::new(sutra_crypto::EnvelopeKeyProvider::new(kek, map))
                as Arc<dyn sutra_crypto::KeyProvider + Send + Sync>,
        ))
    } else if let Some(master) = &config.crypto_master_key {
        Ok(Some(
            Arc::new(sutra_crypto::HkdfKeyProvider::new(master.as_bytes()))
                as Arc<dyn sutra_crypto::KeyProvider + Send + Sync>,
        ))
    } else {
        Ok(None)
    }
}

/// The activation's READ-ONLY registries — built ONCE per activation and shared by every
/// engine lane (execution scale-out §2 row 10; the `Arc` refactor that row deferred).
///
/// Everything in here is immutable from the moment the activation finishes building it: the
/// deployed process graphs, the compiled codecs, the compiled tier-2 validators, the archive
/// artifact bytes (templates / scripts / decisions) and the resolved outbound channels. Before
/// this existed, `build_engine` re-derived all of it per lane — compiling every XSD codec, every
/// DMN/`.srl` ruleset and deep-copying every BPMN graph and every archive byte S times, so the
/// engine's read-only working set was O(deployments × lanes).
///
/// What is deliberately NOT here (per-lane by nature, not by omission):
/// - the REDACTOR registry — a template redactor holds a lazily-populated compiled-template
///   cache (interior mutability); sharing it across lanes would be a data race, not a saving;
/// - the template / decision ENGINES, for the same reason;
/// - the module data stores, the concurrency gauges, the coverage router and the whole executor
///   + `ChannelEngine` instance state (`Rc`/`RefCell` by design — the lane's own execution
///     context, its `DrainingSink`, its in-memory alias/inbox stores, its metrics).
pub(crate) struct SharedRegistries {
    processes: Arc<ProcessModuleRegistry>,
    templates: Arc<TemplateRegistry>,
    scripts: Arc<ScriptRegistry>,
    decisions: Arc<DecisionRegistry>,
    validators: Arc<ValidatorRegistry>,
    codecs: Arc<CodecRegistry>,
    formats: Arc<sutra_channels::FormatRegistry>,
    outbound_channels: Arc<OutboundChannelRegistry>,
}

/// Build the activation's shared read-only registries — the once-per-activation half of what
/// [`build_engine`] used to do once per lane. Called at the runtime-assembly level, alongside
/// [`seed_declared_coverage`]: `serve` before the boot lanes spawn, `activate_plans` before the
/// flip fans the rebuild out, so a flip rebuilds this set exactly once no matter how many lanes
/// swap onto it.
///
/// Registration order (active first, then the DRAINING tail) is byte-for-byte the order the
/// per-lane build used, so last-writer-wins on any colliding key resolves identically.
pub(crate) fn build_shared_registries(
    active: &[DeploymentPlan],
    draining: &[DeploymentPlan],
) -> Arc<SharedRegistries> {
    let mut processes = ProcessModuleRegistry::new();
    let mut templates = TemplateRegistry::new();
    let mut scripts = ScriptRegistry::new();
    let mut decisions = DecisionRegistry::new();
    let mut validators = ValidatorRegistry::new();
    let mut outbound_channels = OutboundChannelRegistry::new();
    // Every engine-PROVIDED (global) codec — the schema-less formats AND the zero-config
    // message-standard / delimited parsers (the message-standard codecs supplied by
    // proprietary extension crates, plus csv) — is seeded here from the single canonical
    // `builtin_codecs()` source, keyed `urn:sutra:codec:<name>:internal`. Channels
    // reference them by bare name or the logical URN (`codec: urn:sutra:codec:<name>`, no
    // scope); `CodecRegistry::resolve` appends the scope.
    let mut codecs = CodecRegistry::with_builtins();

    for plan in active.iter().chain(draining.iter()) {
        for module in &plan.modules {
            processes.register(&plan.dep, module);
        }
        for channel in &plan.outbound {
            outbound_channels.register(&plan.dep, channel.clone());
        }
        for (key, bytes) in &plan.templates {
            templates.register(key, bytes.clone());
        }
        for (key, bytes) in &plan.scripts {
            scripts.register(key, bytes.clone());
        }
        for (key, bytes) in &plan.decisions {
            decisions.register(key, bytes.clone());
        }
        for (key, decision) in &plan.validators {
            validators.register_under(
                key,
                DmnContentValidator::new(sutra_dmn::DmnRulesetValidator::new(decision.clone())),
            );
        }
        // The `.srl` siblings — same registry, same keying scheme, so a `<q:validators>` chain
        // may mix engines and their issues accumulate into one `validation.*` summary.
        for (key, source) in &plan.srl_validators {
            validators.register_under(key, SrlContentValidator::new(key, source));
        }
        for (urn, xsds) in &plan.codecs {
            let refs: Vec<&[u8]> = xsds.iter().map(|x| x.as_slice()).collect();
            // THE VALIDATING BUILD. `compile` alone produces a shape-only codec — it projects and
            // coerces leaves but runs no XSD validation, so the structural tier emitted no issues
            // and `validation.outcome` was OK for every payload a module schema codec could
            // decode. A validation gateway could therefore never reject anything, and a schema in
            // a package meant nothing at runtime while package-time lint checked against it.
            //
            // Falls back to the shape-only build when the XSD set is outside the supported subset:
            // that is a deployment that used to load and still should, and the reply to it is a
            // narrower guarantee rather than a refusal to serve.
            let codec = StructuralCodec::compile_with_formats(urn, &refs, &["xml", "json", "yaml"])
                .unwrap_or_else(|_| StructuralCodec::compile(urn, &refs));
            codecs.register(codec);
        }
        // Archive schema bundles under the deployment scope — they win over a built-in of the
        // same logical name for THIS deployment (`CodecRegistry::resolve`'s tier 1), leaving every
        // other deployment on the built-in.
        for (key, make) in &plan.codec_bundles {
            codecs.register_under(key, make());
        }
    }

    Arc::new(SharedRegistries {
        processes: Arc::new(processes),
        templates: Arc::new(templates),
        scripts: Arc::new(scripts),
        decisions: Arc::new(decisions),
        validators: Arc::new(validators),
        codecs: Arc::new(codecs),
        formats: Arc::new(sutra_channels::FormatRegistry::with_builtins()),
        outbound_channels: Arc::new(outbound_channels),
    })
}

/// Runs ON the actor lane: assemble the `Rc`-based executor + channel engine over the
/// activation's shared read-only registries.
///
/// `active` deployments register everything — routes, bindings, processes, artifacts,
/// stores. `draining` deployments (DRAINING: flipped away, not yet quiescent)
/// register their processes/artifacts/stores under their own ids so instances PINNED to
/// them keep resuming (relay + timer), but their channel bindings register only where no
/// active deployment claims the same `(module_key, channel)` key — residual in-flight
/// traffic resolves, new intake flows to the active claimant. (That registration now happens
/// once per activation in [`build_shared_registries`]; what this builds per lane is the lane's
/// own execution state.)
#[allow(clippy::too_many_arguments)] // boot assembly gathers the full engine config (precedent: build_engine_runtime)
pub(crate) fn build_engine(
    active: Arc<Vec<DeploymentPlan>>,
    draining: Arc<Vec<DeploymentPlan>>,
    // The activation's read-only registries, built ONCE by `build_shared_registries` and handed
    // to every lane (execution scale-out §2 row 10).
    shared: Arc<SharedRegistries>,
    pool: Option<PgPool>,
    handle: Handle,
    metrics_labels: Option<Vec<String>>,
    payload_cap_bytes: u64,
    audit: crate::config::AuditConfig,
    key_provider: Option<Arc<dyn sutra_crypto::KeyProvider + Send + Sync>>,
    incident_sql: bool,
    instance_retention: std::time::Duration,
    deferred_acks: Arc<DeferredAckRegistry>,
    // TEST-ONLY (P1-7): `EngineConfig::now_override`, threaded down to the executor's
    // `now_supplier`. `None` on every production boot — see the field doc.
    now_override: Option<sutra_executor::TestClock>,
    // This engine's lane in the shard router (`EngineShard::single()` at the default
    // `sutra.engine.shards = 1`). Salts the per-lane identities (claim owner, intake
    // ids) and decides the relay handoff inside the dispatcher.
    shard: sutra_channels::EngineShard,
    // This lane's observability counters (execution scale-out §6.1) — the router-owned
    // handle, so the exporter reads one registry across activation flips.
    shard_metrics: Arc<sutra_channels::ShardLaneMetrics>,
    // The activation-initial coverage snapshot (read once per activation by
    // `seed_declared_coverage`, applied per lane) — Phase 3 keeps the engine build
    // `block_on`-free so the flip's rebuild may run on a lane's async actor task.
    initial_coverage: crate::otel::InitialCoverage,
) -> ChannelEngine {
    let prior_deployments: Vec<DeploymentId> =
        draining.iter().map(|plan| plan.dep.clone()).collect();
    // The read-only registries (processes, codecs, validators, templates, scripts, decisions,
    // outbound channels) are NOT built here any more: `build_shared_registries` built them once
    // for this activation and `shared` is a pointer to that one set — see [`SharedRegistries`].
    // What remains per lane is what cannot be shared: the redactor registry (compiled-template
    // cache) and the module data-store bindings.
    //
    // Every built-in redactor a `sutra-redactor-<standard>` crate `inventory::submit!`ed and
    // `sutra-dist` force-links — keyed `urn:sutra:redactor:<name>:internal`. Deployment-scoped
    // user `redactors/*.hbs` fold in alongside below (Mirror 2 — the redactor-URN reference
    // implementation).
    let mut redactors = RedactorRegistry::with_builtins();
    let mut store_map: HashMap<(String, String), Rc<dyn DataStore>> = HashMap::new();

    // The binding keys the ACTIVE set claims — a draining deployment's definition on the
    // same key stays unregistered (the pointer has flipped). The set then GROWS as the
    // draining tail registers: several draining revisions of one slot are a legal store
    // state (interrupted drains accumulate; only `status='active'` is unique per slot),
    // and their near-identical bindings would otherwise collide. First-wins over the
    // `revision DESC` listing keeps the most-recently-drained binding — the same order
    // the relay's DRAINING scope walk uses.
    let mut claimed: std::collections::HashSet<(String, String)> = active
        .iter()
        .flat_map(|plan| plan.definitions.iter())
        .map(|def| {
            (
                def.binding.namespace.module_key(),
                def.binding.channel_name.clone(),
            )
        })
        .collect();

    for plan in active.iter().chain(draining.iter()) {
        register_archive_redactors(&mut redactors, plan);
        for (name, store) in &plan.stores {
            let bound: Rc<dyn DataStore> = match store {
                // Bind the async dialect stores DIRECTLY (thin await-adapters — no
                // block_on facade). Awaited on the lane's actor task by the executor.
                StoreBackend::Pg(s) => Rc::new(PgStore::new(s.clone())),
                StoreBackend::Mysql(s) => Rc::new(MysqlStore::new(s.clone())),
                StoreBackend::Mssql(s) => Rc::new(MssqlStore::new(s.clone())),
            };
            store_map.insert((plan.dep.value().to_string(), name.clone()), bound);
        }
    }

    // Concurrency gauges. Production path = the persisted tables (replica-coherent +
    // crash-safe): `channel_instance` for the per-channel cap, `instance_state` COUNT(*) for
    // the tenant-quota concurrent dimension. A persistence-less boot falls back to in-memory
    // gauges (PER-PROCESS only — wait states already fail closed without a pool, so nothing
    // durably parks anyway).
    let (concurrency_store, active_instances): (
        Rc<dyn ConcurrencyStore>,
        Rc<dyn ActiveInstanceCount>,
    ) = match &pool {
        Some(pool) => (
            Rc::new(PersistedChannelConcurrency::new(
                PgChannelConcurrencyStore::new(pool.clone()),
            )),
            Rc::new(PersistedActiveInstanceCount::new(PgInstanceStore::new(
                pool.clone(),
            ))),
        ),
        None => {
            warn!(
                "no engine datasource — per-channel concurrency caps and tenant concurrent \
                 quotas are enforced PER-PROCESS only (in-memory gauges; not replica-coherent \
                 or crash-safe)"
            );
            (
                Rc::new(InMemoryConcurrencyStore::new()),
                Rc::new(InMemoryActiveInstanceCount::new()),
            )
        }
    };

    // The coverage-METRIC store — since 2026-08-04 the ONLY coverage surface
    // (`datastore-schema-projection.md` §7 retired the module KV covered-set; its rows are left
    // in place for rollback but never read or written again). SUPERSEDING RULING, same day: it
    // does NOT ride the engine pool. Each deployment's coverage marks go to the store IT declared
    // (`coverage` in its own datastores.yaml), whose data source picks the database and therefore
    // the dialect; the engine owns only the schema, applied to that connection on first use. So
    // the executor gets a ROUTER over the per-deployment stores rather than one engine-wide store,
    // and "no engine database" is no longer a reason for a deployment to have no coverage.
    let mut coverage_router = DeclaredCoverageStores::new();
    for plan in active.iter().chain(draining.iter()) {
        if let Some(store) = &plan.coverage {
            coverage_router.register(plan.dep.value().to_string(), store.clone());
        } else if let Some(fault) = &plan.coverage_fault {
            coverage_router.register_fault(plan.dep.value().to_string(), fault.clone());
        }
    }
    for plan in active.iter().chain(draining.iter()) {
        let declares_paths = plan
            .modules
            .iter()
            .flat_map(|m| m.processes())
            .any(|p| !p.coverage_paths.is_empty());
        if declares_paths && plan.coverage.is_none() && plan.coverage_fault.is_none() {
            warn!(
                deployment = %plan.dep.value(),
                "<q:coverage> paths are declared but datastores.yaml declares no 'coverage' \
                 store — NO coverage will be recorded (that store is where the marks are \
                 persisted; the engine owns its schema, you supply no SQL). \
                 coverage:report / coverage:reset will fail until it is declared"
            );
        }
    }
    let coverage_metric_store: Option<Rc<dyn CoverageMetricStore>> =
        Some(Rc::new(coverage_router) as Rc<dyn CoverageMetricStore>);
    // The per-process gauge metadata, walked from the active + draining plans: deployment id,
    // declared `<q:coverage>` path ids, and the authoring namespace (tenant/module/version) for
    // the `sutra.coverage.percent` gauge dimensions. Only deployments that actually HAVE a
    // coverage store contribute — a gauge with nowhere to read from would publish a fake 0%.
    let coverage_metas: Vec<crate::otel::CoverageMeta> = {
        let mut metas = Vec::new();
        for plan in active.iter().chain(draining.iter()) {
            if plan.coverage.is_none() {
                continue;
            }
            // All channels of a module version share one namespace (tenant/module/version).
            let namespace = plan.definitions.first().map(|d| &d.binding.namespace);
            for module in &plan.modules {
                for process in module.processes() {
                    if process.coverage_paths.is_empty() {
                        continue;
                    }
                    metas.push(crate::otel::CoverageMeta {
                        deployment_id: plan.dep.value().to_string(),
                        process_id: process.id.clone(),
                        declared_paths: process
                            .coverage_paths
                            .iter()
                            .map(|p| p.id.clone())
                            .collect(),
                        tenant: namespace.map(|n| n.tenant.clone()),
                        module: namespace.map(|n| n.module.clone()),
                        version: namespace.map(|n| n.version.clone()),
                    });
                }
            }
        }
        metas
    };

    // Coverage seed-at-deploy is NOT here any more: it is hoisted to the runtime-assembly
    // level ([`seed_declared_coverage`], awaited once per activation by `serve` and
    // `activate_plans` BEFORE this build runs), so a multi-lane engine build never repeats
    // the S× redundant seed. Since Phase 3 the covered-flag READ is hoisted with it: the
    // `initial_coverage` snapshot passed in is what `apply_initial_coverage` below seeds
    // from, keeping this whole build `block_on`-free (a flip rebuilds ON a lane's async
    // actor task, where `Handle::block_on` panics).

    let sink = Rc::new(DrainingSink::new());
    // The module resolver reads the SHARED process registry (an `Arc` bump per lane, not a copy
    // of every deployed graph).
    let module_resolver_view = Arc::clone(&shared.processes);
    let mut builder = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_templates(
            TemplateEngineRegistry::new()
                .register(HbsTemplateEngine::new())
                .register(crate::xslt::XslTemplateEngine::new()),
            Arc::clone(&shared.templates),
        )
        .with_scripts(Arc::clone(&shared.scripts))
        .with_decisions(
            DecisionEngineRegistry::new()
                .register(DmnEngine::new())
                .register(SrlEngine::new()),
            Arc::clone(&shared.decisions),
        )
        .with_data_stores(move |deployment, name| {
            store_map
                .get(&(deployment.value().to_string(), name.to_string()))
                .cloned()
        })
        .with_module_resolver(move |deployment, id| {
            module_resolver_view.find_in_module(deployment, id)
        })
        .with_outbound_channels(Arc::clone(&shared.outbound_channels))
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>);
    // TEST-ONLY (P1-7 time-skipping test runtime): `now_override` is `None` on every production
    // boot, in which case the executor keeps its own default (`OffsetDateTime::now_utc`) —
    // untouched, so this branch never runs outside a test that explicitly installed a
    // `TestClock` on `EngineConfig`. When it did, every due-at this executor computes (timer
    // park, `<q:retry>` backoff) reads the SAME virtual instant the test advances.
    if let Some(clock) = now_override {
        builder = builder.with_now_supplier(move || clock.rfc3339());
    }
    // The OTel metrics listener — instruments resolve through the global
    // meter provider `otel::init` installed on the main thread before assembly. When
    // a coverage store is present the SAME listener drives the event-driven
    // `sutra.coverage.percent` gauge, and records the activation-initial value here from
    // the PRE-READ `initial_coverage` snapshot (no store I/O in the build — Phase 3) —
    // so a replica booting onto pre-existing coverage reports the right percent immediately.
    if let Some(labels) = metrics_labels {
        let mut listener = crate::otel::OtelMetricsListener::new(labels);
        if coverage_metric_store.is_some() {
            listener = listener.with_coverage(
                &opentelemetry::global::meter("sutra-engine"),
                coverage_metas,
            );
        }
        let listener = Rc::new(listener);
        listener.apply_initial_coverage(&initial_coverage);
        builder = builder.with_listener(listener);
    }
    // The audit trail. Build ONE registry composing every enabled engine-global sink
    // (JSONL + OTel-log — the DB sink is the follow-on), then, when it is non-empty, register the
    // AuditListener on the executor's lifecycle bus (exactly like the OTel metrics listener above),
    // feeding an async dispatcher spawned on the runtime handle. Recreated per activation — the
    // executor is rebuilt on a flip and the previous dispatcher drains + exits when its listener
    // drops. Best-effort: a JSONL open error just drops that sink, it never fails the engine build.
    // The typed listener handle is hoisted so the ChannelEngine can ALSO reach it (below) — it
    // seeds/reads the per-instance audit seq across suspend/resume so the seq stays monotonic.
    let mut audit_listener: Option<Rc<sutra_channels::AuditListener>> = None;
    {
        let mut registry = sutra_channels::AuditSinkRegistry::new();
        if let Some(path) = audit.jsonl_path.as_ref() {
            match sutra_channels::JsonlAuditSink::open(path) {
                Ok(sink) => registry.register(Arc::new(sink)),
                Err(e) => warn!(
                    path = %path.display(),
                    error = %e,
                    "audit JSONL sink could not open — JSONL audit disabled for this activation"
                ),
            }
        }
        // The OTel-log sink exports to a DEDICATED audit OTLP endpoint (`sutra.audit.otel.endpoint`)
        // — its own logs pipeline, independent of the engine telemetry stream. Register it only when
        // that pipeline builds (`init_audit_otel` is idempotent, so a per-activation flip reuses the
        // one provider).
        if let Some(endpoint) = audit.otel_endpoint.as_deref() {
            if crate::audit_sinks::init_audit_otel(endpoint) {
                registry.register(Arc::new(crate::audit_sinks::OtelAuditSink));
            } else {
                warn!(
                    endpoint,
                    "sutra.audit.otel.endpoint is set but the dedicated audit OTLP exporter could \
                     not be built — OTel audit sink not registered"
                );
            }
        }
        // The SQL sink (the `<q:audit>` default target) is the durable audit of record — gate it
        // on both the toggle AND a datasource pool (pg-only). `pool.as_ref()` borrows: the pool is
        // moved into the PersistenceBridge further below, so the sink takes a cheap Arc clone.
        if audit.sql {
            match pool.as_ref() {
                Some(p) => {
                    registry.register(Arc::new(crate::audit_sinks::SqlAuditSink::new(p.clone())))
                }
                None => warn!(
                    "sutra.audit.sql is set but no datasource pool is configured — SQL audit sink \
                     not registered (audit_event needs a database)"
                ),
            }
        }
        if !registry.is_empty() {
            let names = registry.names().join(", ");
            // The engine-default audit sink for a process that declares no `<q:audit sink>` (and
            // has no manifest default): the highest-priority REGISTERED sink — sql (the
            // audit-of-record) > jsonl > otel. Audit routes to exactly one sink; this is the
            // default target. Computed before the registry moves into the dispatcher.
            let default_audit_sink = ["sql", "jsonl", "otel"]
                .into_iter()
                .find(|name| registry.get(name).is_some())
                .map(str::to_string);
            let (tx, rx) = sutra_channels::audit_channel();
            sutra_channels::spawn_audit_dispatcher(&handle, rx, registry);
            let listener = Rc::new(sutra_channels::AuditListener::new(tx, || {
                time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            }));
            // Register the SAME listener on the executor's lifecycle bus AND retain a typed
            // handle for the ChannelEngine (seq seed/read across suspend/resume). The `let`
            // with an explicit trait-object type coerces the clone (`Rc::clone` would pin the
            // concrete type and fail to unsize).
            let as_listener: Rc<dyn sutra_executor::listener::ExecutionListener> = listener.clone();
            builder = builder
                .with_listener(as_listener)
                .with_default_audit_sink(default_audit_sink.clone());
            audit_listener = Some(listener);
            info!(
                sinks = %names,
                default_sink = %default_audit_sink.as_deref().unwrap_or("-"),
                "audit trail active (single-sink routing; default = highest-priority registered)"
            );
        }
    }
    // The deferred-ack registry observes the SAME lifecycle bus (exactly like
    // the OTel metrics + audit listeners above): INSTANCE_COMPLETED fires the held
    // broker ack, INSTANCE_FAILED the nack. The registry itself is engine-PROCESS-scoped
    // (`Arc`, shared across activation flips and with the sweep task) — only this
    // per-activation listener adapter is rebuilt, so pending acks from a pre-flip
    // activation still settle when their instance resumes post-flip.
    builder = builder.with_listener(Rc::new(DeferredAckListener::new(Arc::clone(
        &deferred_acks,
    ))));
    // Thread the same seeded metric store into the executor: `mark_coverage` flips intra flags
    // + writes cross-process fragments through it, and the reserved coverage:report /
    // coverage:reset ops read and clear the same flags.
    if let Some(store) = &coverage_metric_store {
        builder = builder.with_coverage_metric_store(Rc::clone(store));
    }
    let executor = builder.build();

    let mut engine_builder = ChannelEngine::builder(
        executor,
        sink,
        // Codecs / formats / validators come from the activation-shared set (pointer clones);
        // only the redactor registry is this lane's own.
        InboundChain::new(
            Arc::clone(&shared.codecs),
            Arc::clone(&shared.formats),
            Arc::clone(&shared.validators),
        )
        .with_redactors(redactors),
    )
    .with_process_registry(Arc::clone(&shared.processes))
    // Sync-path alias materialisation stays in-memory (retired within the same
    // dispatch); DURABLE alias rows — the relay-correlation index — ride the park step
    // through the PersistenceBridge below. Multi-replica alias visibility for
    // run-to-completion flows is out of the single-container gate's scope.
    .with_alias_store(Rc::new(InMemoryAliasStore::new()))
    // With persistence wired the bridge commits emissions durably at each
    // quiescent point and this hook is never consulted; persistence-less
    // hosts keep the collect-only posture (nothing to deliver from).
    .with_outbox(Rc::new(CollectingOutbox::new()))
    // Channel-policy enforcers (the seams shipped optional; this is the
    // production wiring). Defaults preserve current behavior except where the
    // contract mandates otherwise (the payload-cap ceiling).
    //
    // Payload cap: the global default (10 MiB, operator-overridable
    // via `sutra.codec.max-payload-bytes`; `0` disables). Set BEFORE
    // `with_channel_definitions` so each channel's `payload-cap-bytes` folds on top as
    // a per-channel override.
    .with_payload_cap_policy(
        PayloadCapPolicy::of_global(payload_cap_bytes as i64)
            .unwrap_or_else(|_| PayloadCapPolicy::disabled()),
    )
    // Feature gate: no feature-flag system is configured, so every `${feature.X}`
    // `enabled` expression resolves true (gates are parsed, never denied today). Wiring
    // `AllowAllFeatureProvider` establishes the production seam.
    .with_feature_provider(Rc::new(AllowAllFeatureProvider))
    // Tenant quotas: no tenant-config source is loaded yet (that schema lands with
    // sutra-loader), so every tenant is unlimited — current behavior. The enforcer is
    // wired over an empty source so future per-tenant quotas need only populate it; its
    // concurrent dimension reads the persisted per-deployment instance count above
    // (replica-coherent + crash-safe).
    .with_quota_enforcer(Rc::new(DefaultTenantQuotaEnforcer::new(
        Box::new(StaticTenantConfigSource::new(Vec::new())),
        active_instances,
    )))
    // Concurrency: the per-channel `max-concurrent-instances` admission gauge, backed
    // by the persisted `channel_instance` table (or the in-memory fallback above). The
    // dispatcher maintains the rows at its park/terminal commit points and reads
    // COUNT(*) at admission — replica-coherent and crash-safe.
    .with_concurrency_store(concurrency_store)
    // DRAINING scopes for relay correlation — pinned instances resume.
    .with_prior_deployments(prior_deployments)
    // This engine's lane in the shard router (relay handoff decision + intake-id salt).
    .with_shard(shard)
    // …and its lane's §6.1 counters (park/resume/handoff/claim-bounce sites).
    .with_shard_metrics(shard_metrics)
    // The deferred-ack registry the park arm registers `dispatch_deferred`
    // settle callbacks on (the same Arc the listener above settles through).
    .with_deferred_acks(deferred_acks);
    // Hand the ChannelEngine the same audit listener so it can persist the per-instance
    // audit-seq high-water at suspend and seed it back at resume (seq monotonicity across
    // suspend/resume + restart — the DB sink's uniqueness guard).
    if let Some(listener) = audit_listener {
        engine_builder = engine_builder.with_audit_listener(listener);
    }
    for plan in active.iter() {
        engine_builder = engine_builder.with_channel_definitions(&plan.definitions);
    }
    for plan in draining.iter() {
        let residual: Vec<ChannelDefinition> = plan
            .definitions
            .iter()
            .filter(|def| {
                claimed.insert((
                    def.binding.namespace.module_key(),
                    def.binding.channel_name.clone(),
                ))
            })
            .cloned()
            .collect();
        let skipped = plan.definitions.len() - residual.len();
        if skipped > 0 {
            info!(
                deployment = %plan.dep.value(),
                skipped,
                "draining tail: channel keys already registered (active or a newer \
                 draining revision) — older duplicate bindings stay unregistered"
            );
        }
        if !residual.is_empty() {
            engine_builder = engine_builder.with_channel_definitions(&residual);
        }
    }
    match pool {
        Some(pool) => {
            // The key provider (Hkdf master-key OR the envelope EnvelopeKeyProvider) is built once
            // in serve() — the envelope DEK load is deployment-scoped setup, not per-build work —
            // and threaded in as the `Send + Sync` Arc the bridge holds directly.
            // `instance_retention` decides one thing in the bridge: whether the terminal step
            // RETAINS the instance row (re-stamped COMPLETED, queryable for the window) or deletes
            // it outright the way it always used to (`PT0S`).
            let bridge = Rc::new(
                PersistenceBridge::with_key_provider(pool, key_provider)
                    .with_retention(instance_retention)
                    // Shard-scoped ownership: same owner ⇒ same lane ⇒ already serialised
                    // (`-s0` at the single-lane default; the store treats it as opaque).
                    .with_shard_owner(shard.index),
            );
            // The durable dead-letter sink, opt-in via `sutra.incident.sql`.
            if incident_sql {
                engine_builder = engine_builder.with_incident_sink(
                    Rc::clone(&bridge) as Rc<dyn sutra_channels::stores::IncidentSink>
                );
            }
            engine_builder = engine_builder
                .with_inbox(Rc::clone(&bridge) as Rc<dyn sutra_channels::InboxStore>)
                .with_instance_bridge(bridge as Rc<dyn sutra_channels::InstanceBridge>);
        }
        None => {
            if incident_sql {
                warn!(
                    "sutra.incident.sql is set but no datasource pool is configured — dead-letter \
                     sink not wired (dead_letter needs a database); failures are logged only"
                );
            }
            engine_builder = engine_builder.with_inbox(Rc::new(InMemoryInboxStore::new()));
        }
    }
    engine_builder.build()
}

#[cfg(test)]
mod datastore_dispatch_tests {
    use super::{build_store_backend, datastore_dialect, StoreBackend, StoreDialect};
    use sutra_datastore::StoreDefinition;

    #[test]
    fn dialect_is_selected_by_url_scheme() {
        for url in [
            "postgres://h/db",
            "postgresql://h/db",
            "postgresql://h:5432/db",
        ] {
            assert_eq!(
                datastore_dialect(url).unwrap(),
                StoreDialect::Postgres,
                "{url}"
            );
        }
        for url in ["mysql://h/db", "mysql://h:3306/db", "mariadb://h/db"] {
            assert_eq!(
                datastore_dialect(url).unwrap(),
                StoreDialect::Mysql,
                "{url}"
            );
        }
        for url in ["mssql://h/db", "sqlserver://h;database=db"] {
            assert_eq!(
                datastore_dialect(url).unwrap(),
                StoreDialect::Mssql,
                "{url}"
            );
        }
    }

    #[test]
    fn unsupported_scheme_fails_closed() {
        assert!(datastore_dialect("mongodb://h/db").is_err());
        assert!(datastore_dialect("cassandra:mem:test").is_err());
        // A definition with no resolvable connection is Err, not a silent PG default.
        let def = StoreDefinition {
            name: "no-conn".to_string(),
            store_type: "sql".to_string(),
            properties: std::collections::BTreeMap::new(),
            structure: None,
        };
        assert!(build_store_backend(&def, Vec::new(), None, None).is_err());
    }

    /// The WP-5a proof: a module store whose connection scheme is `mysql://` dispatches to
    /// the MySQL backend and its executor-SPI operations (get/put/CAS/transaction) work on a
    /// real MySQL server — i.e. the engine's store-registry bind point drives the new dialect
    /// end-to-end, not just PostgreSQL.
    #[test]
    #[ignore = "docker"]
    fn mysql_scheme_dispatches_and_store_ops_work_on_real_mysql() {
        use crate::stores::MysqlStore;
        use std::any::Any;
        use sutra_executor::DataStore;
        use sutra_feel::FeelValue;
        use testcontainers::runners::SyncRunner;
        use testcontainers::ImageExt;

        // Cutover DDL a Rust-era module ships on MySQL (rows key on (store_name, store_key)).
        const CUTOVER_DDL: &str = "CREATE TABLE IF NOT EXISTS data_store (\
          store_name  VARCHAR(128) NOT NULL, \
          store_key   VARCHAR(512) NOT NULL, \
          store_value LONGTEXT     NOT NULL, \
          rev         BIGINT       NOT NULL DEFAULT 1, \
          updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP, \
          PRIMARY KEY (store_name, store_key) \
        ) CHARACTER SET ascii;";

        // The blocking testcontainers runner drives its own runtime — start it on a
        // dedicated OS thread so we never enter it from inside a tokio worker.
        let (container, port): (Box<dyn Any + Send + Sync>, u16) = std::thread::spawn(|| {
            let c = testcontainers_modules::mysql::Mysql::default()
                .with_tag("8.0")
                .start()
                .expect("start mysql:8.0 (docker required)");
            sutra_testkit::reap_on_exit(c.id());
            let port = c.get_host_port_ipv4(3306).expect("mapped 3306");
            (Box::new(c) as Box<dyn Any + Send + Sync>, port)
        })
        .join()
        .expect("container bootstrap thread");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();

        // The container ships a default `test` database; the store partitions by store_name.
        let mut properties = std::collections::BTreeMap::new();
        properties.insert(
            "sql.url".to_string(),
            format!("mysql://root@127.0.0.1:{port}/test"),
        );
        let def = StoreDefinition {
            name: "accounts".to_string(),
            store_type: "sql".to_string(),
            properties,
            structure: None,
        };

        // Under test: the URL scheme picks the MySQL backend (not the PG default). The pool
        // is constructed inside the runtime context (as at engine boot); the adapter is then
        // driven from this plain test thread via `handle.block_on` (the lane loop awaits it
        // directly in production — Phase 3).
        let backend = rt
            .block_on(async {
                build_store_backend(&def, vec![CUTOVER_DDL.to_string()], None, None)
            })
            .expect("mysql backend builds");
        assert!(
            matches!(backend, StoreBackend::Mysql(_)),
            "mysql:// scheme must select the MySQL backend"
        );
        let StoreBackend::Mysql(store) = backend else {
            unreachable!()
        };

        // Drive the executor SPI adapter against the real container — the same path the
        // engine's store_map binds (Rc<dyn DataStore>). The adapter is async; drive each
        // op via `handle.block_on` (as the dispatcher does on the actor thread).
        let s = MysqlStore::new(store.clone());
        assert_eq!(handle.block_on(s.get("k")).unwrap(), None);
        handle
            .block_on(s.put("k", FeelValue::String("v1".to_string())))
            .unwrap();
        assert_eq!(
            handle.block_on(s.get("k")).unwrap(),
            Some(FeelValue::String("v1".to_string()))
        );
        let rev = handle.block_on(s.revision("k")).unwrap();
        assert!(rev >= 1);
        // CAS: a stale revision is rejected; the current one applies.
        assert!(!handle
            .block_on(s.put_if_revision("k", FeelValue::String("v2".to_string()), rev + 5))
            .unwrap());
        assert!(handle
            .block_on(s.put_if_revision("k", FeelValue::String("v2".to_string()), rev))
            .unwrap());
        assert_eq!(
            handle.block_on(s.get("k")).unwrap(),
            Some(FeelValue::String("v2".to_string()))
        );
        // Transaction: commit publishes.
        let tx = handle.block_on(s.begin()).unwrap().expect("begin");
        handle
            .block_on(tx.put("t", FeelValue::String("committed".to_string())))
            .unwrap();
        handle.block_on(tx.commit()).unwrap();
        assert_eq!(
            handle.block_on(s.get("t")).unwrap(),
            Some(FeelValue::String("committed".to_string()))
        );
        // Transaction: explicit rollback discards (the path the executor drives when a
        // `<bpmn:transaction>` scope ends without commit — commit/rollback are always explicit).
        let tx = handle.block_on(s.begin()).unwrap().expect("begin");
        handle
            .block_on(tx.put("rb", FeelValue::String("rolled-back".to_string())))
            .unwrap();
        handle.block_on(tx.rollback()).unwrap();
        assert_eq!(
            handle.block_on(s.get("rb")).unwrap(),
            None,
            "a rolled-back transaction must discard its writes"
        );

        // (The KV coverage adapter this test used to exercise here is gone: coverage is typed
        // rows now — in the deployment's OWN declared `coverage` store, with engine-shipped DDL —
        // so there is no KV coverage slot to round-trip. `sutra-datastore`'s per-dialect
        // coverage suites cover that surface; see `datastore-schema-projection.md` §7.)

        drop(container);
    }
}

#[cfg(test)]
mod transport_governance_tests {
    use super::reject_disallowed_transports;
    use std::collections::{BTreeMap, BTreeSet};
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::ChannelDefinition;
    use sutra_executor::DeploymentId;

    fn channel(name: &str, transport: Option<&str>) -> ChannelDefinition {
        ChannelDefinition {
            binding: ChannelBinding::new(
                name,
                Namespace::new("acme", "pay", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: transport.map(str::to_string),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: BTreeMap::new(),
        }
    }

    fn allowed(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_permitted_transport_passes() {
        let defs = vec![channel("in", Some("file")), channel("out", Some("http"))];
        let dep = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        assert!(reject_disallowed_transports(&defs, &allowed(&["file", "http"]), &dep).is_ok());
    }

    #[test]
    fn a_forbidden_transport_is_rejected_closed() {
        // A hardened (file-only) engine must REJECT a kafka channel, not silently drop it.
        let defs = vec![channel("pay", Some("kafka"))];
        let dep = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        let err = reject_disallowed_transports(&defs, &allowed(&["file"]), &dep)
            .expect_err("a non-permitted transport must fail the deployment closed");
        let message = err.to_string();
        assert!(message.contains("kafka"), "{message}");
        assert!(message.contains("not permitted"), "{message}");
    }

    #[test]
    fn a_channel_without_a_declared_transport_is_left_to_the_existing_checks() {
        // No declared transport is not this gate's concern (the per-direction checks handle it),
        // so it passes here even against an empty allowed set — no behaviour change on any build.
        let defs = vec![channel("in", None)];
        let dep = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        assert!(reject_disallowed_transports(&defs, &allowed(&[]), &dep).is_ok());
    }

    #[test]
    fn local_transport_is_always_permitted() {
        // Even a hardened file-only engine must permit in-process routing — internal channels
        // and `local://` binds are engine-internal, not a wire protocol.
        let defs = vec![channel("demoflow-in", Some("local"))];
        let dep = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        assert!(reject_disallowed_transports(&defs, &allowed(&["file"]), &dep).is_ok());
    }
}

#[cfg(test)]
mod local_routing_tests {
    use super::expand_local_destination;
    use sutra_bpmn::qbindings::ReplyMode;
    use sutra_channels::sink::scheme_of;
    use sutra_executor::ResolvedOutboundChannel;

    #[test]
    fn a_bare_local_bind_expands_to_the_qualified_in_process_destination() {
        assert_eq!(
            expand_local_destination("local://demoflow-in", "acme/demoflow/1.0.0"),
            "local://acme/demoflow/1.0.0/demoflow-in"
        );
        // An already-qualified local path is left verbatim.
        assert_eq!(
            expand_local_destination(
                "local://acme/demoflow/1.0.0/demoflow-in",
                "acme/demoflow/1.0.0"
            ),
            "local://acme/demoflow/1.0.0/demoflow-in"
        );
        // A non-local destination is untouched.
        assert_eq!(
            expand_local_destination("https://host/cb", "acme/demoflow/1.0.0"),
            "https://host/cb"
        );
    }

    #[test]
    fn an_outbound_channel_with_a_local_bind_resolves() {
        // `local` is a real scheme (passes the outbound scheme gate), and resolution yields a
        // native-mode, auth-free outbound channel pointing at the in-process destination.
        let raw_bind = "local://demoflow-in";
        assert_eq!(scheme_of(raw_bind), Some("local"));
        let destination = expand_local_destination(raw_bind, "acme/demoflow/1.0.0");
        let resolved = ResolvedOutboundChannel::resolve(
            "to-demoflow",
            "local",
            &destination,
            None,
            None,
            None,
            "none",
        );
        assert_eq!(
            resolved.destination,
            "local://acme/demoflow/1.0.0/demoflow-in"
        );
        assert_eq!(resolved.mode, ReplyMode::Native);
        assert!(resolved.auth_ref.is_none());
    }
}

#[cfg(test)]
mod inbound_router_merge_tests {
    //! Guards the shared-HTTP-listener composition (assemble's `inbound_router()` fold). The
    //! regression this guards: the fold *assigned* last-writer-wins, so an inbound transport
    //! with an empty catch-all (dapr/knative on an all-`transport: http` deployment) shadowed
    //! http's fallback and every `/channels/*` route 404'd. The fix MERGES: http contributes
    //! the sole catch-all fallback (arbitrary user binds), dapr/knative contribute specific
    //! `/dapr/*` /`/knative/*` routes, and axum permits the merge because at most one router
    //! carries a fallback.

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    /// An http-like inbound router: a catch-all fallback (the user's arbitrary channel binds).
    fn http_like() -> Router {
        Router::new().fallback(|| async { "http" })
    }

    /// A dapr/knative-like inbound router: one specific prefixed route, NO fallback.
    fn prefixed(path: &'static str, tag: &'static str) -> Router {
        Router::new().route(path, post(move || async move { tag }))
    }

    /// Fold routers exactly as `assemble` does: merge, never assign.
    fn combine(routers: Vec<Router>) -> Router {
        let mut acc: Option<Router> = None;
        for r in routers {
            acc = Some(match acc.take() {
                Some(existing) => existing.merge(r),
                None => r,
            });
        }
        acc.unwrap_or_default()
    }

    async fn probe(router: Router, method: &str, path: &str) -> (StatusCode, String) {
        let resp = router
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn merge_preserves_every_transport_url_space_in_any_order() {
        // Both orders merge without panic (only http carries a fallback) and dispatch every URL
        // to its owning transport — the empty-catch-all-shadows-http regression cannot recur.
        let orders = [
            vec![
                http_like(),
                prefixed("/dapr/{topic}", "dapr"),
                prefixed("/knative/{sub}", "knative"),
            ],
            vec![
                prefixed("/dapr/{topic}", "dapr"),
                prefixed("/knative/{sub}", "knative"),
                http_like(),
            ],
        ];
        for order in orders {
            let app = combine(order);
            // http's fallback serves an arbitrary channel bind...
            let (status, body) = probe(app.clone(), "POST", "/channels/showcase-request").await;
            assert_eq!(
                status,
                StatusCode::OK,
                "http fallback must serve /channels/*"
            );
            assert_eq!(body, "http");
            // ...and the prefixed transports still own their own URL space.
            let (status, body) = probe(app.clone(), "POST", "/dapr/orders").await;
            assert_eq!(status, StatusCode::OK, "dapr must own /dapr/*");
            assert_eq!(body, "dapr");
            let (status, body) = probe(app, "POST", "/knative/sub-1").await;
            assert_eq!(status, StatusCode::OK, "knative must own /knative/*");
            assert_eq!(body, "knative");
        }
    }
}

/// The deployment-scoped ARCHIVE artifact classes — `redactors/*.hbs` (the redactor-URN
/// reference implementation) and `rules/*.srl`
/// (the ruleset registered as a tier-2 validator). Both mirror the `rules/*.dmn` archive path
/// — `plan_deployment` validates fail-closed at plan time and mints the archive-scoped URN key,
/// `build_engine` folds the artifact into the shared registry alongside the built-ins — proven
/// end-to-end without needing a full `ChannelEngine` / `tests/all` integration harness.
#[cfg(test)]
mod archive_artifact_tests {
    use super::*;
    use std::collections::BTreeMap;
    use sutra_loader::LoadedArtifact;

    /// A minimal `LoadedDeployment` carrying only the `redactors/**` entry under test — no
    /// processes/channels needed since `plan_deployment`'s redactor fan-out only compiles +
    /// keys the archive artifact (mirrors `lint.rs`'s `deployment_with` test helper).
    fn deployment_with_redactor(subpath: &str, hbs_source: &str) -> LoadedDeployment {
        let mut redactors = BTreeMap::new();
        redactors.insert(
            subpath.to_string(),
            LoadedArtifact {
                path: std::path::PathBuf::from(format!("redactors/{subpath}")),
                content: hbs_source.to_string(),
            },
        );
        LoadedDeployment {
            id: DeploymentId::of("dep-0000000000000000000000d1").expect("valid deployment id"),
            tenant: "t".to_string(),
            module: "m".to_string(),
            version: "1.0.0".to_string(),
            namespace: "urn:sutra:module:m:1.0.0".to_string(),
            processes: BTreeMap::new(),
            process_files: BTreeMap::new(),
            rules: BTreeMap::new(),
            templates: BTreeMap::new(),
            scripts: BTreeMap::new(),
            redactors,
            codecs: BTreeMap::new(),
            schema_files: BTreeMap::new(),
            migrations: BTreeMap::new(),
            coverage_files: BTreeMap::new(),
            coverages: Vec::new(),
            channels_yaml: None,
            datastores_yaml: None,
            binding_dir: std::path::PathBuf::new(),
        }
    }

    /// The same minimal shell carrying a single `rules/**` entry instead.
    fn deployment_with_rule(subpath: &str, content: &str) -> LoadedDeployment {
        let mut d = deployment_with_redactor("unused.hbs", "/card\n");
        d.redactors = BTreeMap::new();
        d.rules.insert(
            subpath.to_string(),
            LoadedArtifact {
                path: std::path::PathBuf::from(format!("rules/{subpath}")),
                content: content.to_string(),
            },
        );
        d
    }

    #[test]
    fn archive_redactor_compiles_registers_under_the_archive_urn_and_resolves() {
        let dep = deployment_with_redactor("myschema/accounts.hbs", "/card\n");
        let plan = plan_deployment(&dep).expect("a syntactically valid template plans cleanly");

        // The archive URN key `plan_deployment` mints: `urn:sutra:redactor:<local_id>:<depId>`
        // (folder `/` -> `:`, `.hbs` OMITTED — redactor is single-engine), NOT the older
        // `DeploymentId::artifact()` colon form templates/scripts/rules still use.
        let expected_key = "urn:sutra:redactor:myschema:accounts:dep-0000000000000000000000d1";
        assert_eq!(
            plan.redactors,
            vec![(expected_key.to_string(), "/card\n".to_string())]
        );

        // Mirror `build_engine`'s fold-in step against a fresh registry (the built-ins +
        // deployment-scoped archive redactors coexisting, exactly like `CodecRegistry`).
        let mut registry = RedactorRegistry::with_builtins();
        register_archive_redactors(&mut registry, &plan);

        // Resolves by the author-facing local id, scoped to THIS deployment (the same
        // `RedactorRegistry::resolve(reference, deployment)` call `sutra-channels`'s intake
        // makes for a `<q:redactor ref="myschema:accounts">`).
        let resolved = registry
            .resolve("myschema:accounts", &dep.id)
            .expect("archive redactor must resolve by its local id within this deployment");
        assert_eq!(resolved.name(), "template");

        // Deployment-scoped: a DIFFERENT deployment must NOT see this archive's redactor —
        // proves the archive scope, not just the bare local id, gates the lookup.
        let other = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        assert!(registry.resolve("myschema:accounts", &other).is_none());
    }

    /// A `rules/*.srl` ruleset is registered as a tier-2 VALIDATOR under the same archive-scoped
    /// rule URN a `.dmn` gets (extension KEPT), so `<q:complexValidator source="…​.srl">` resolves
    /// — the wiring that lets one module split a single ruleset across both rule engines.
    #[test]
    fn archive_srl_ruleset_registers_as_a_validator_under_the_archive_rule_urn() {
        const RULESET: &str =
            "rule \"r\"\nwhen\n  x != 1\nthen\n  report(\"C\", \"x\", \"m\");\nend\n";
        let dep = deployment_with_rule("field-rules.srl", RULESET);
        let plan = plan_deployment(&dep).expect("a syntactically valid ruleset plans cleanly");

        let expected_key = "urn:sutra:rule:field-rules.srl:dep-0000000000000000000000d1";
        assert_eq!(
            plan.srl_validators,
            vec![(expected_key.to_string(), RULESET.to_string())]
        );
        // …and it ALSO stays a businessRuleTask decision (both roles, like `.dmn`).
        assert_eq!(plan.decisions.len(), 1);

        // Mirror `build_engine`'s fold-in step: the author-facing reference (the archive-local
        // file name) resolves within THIS deployment only.
        let mut registry = ValidatorRegistry::new();
        for (key, source) in &plan.srl_validators {
            registry.register_under(key, SrlContentValidator::new(key, source));
        }
        assert!(registry.resolve("field-rules.srl", &dep.id).is_some());
        let other = DeploymentId::of("dep-0000000000000000000000a2").expect("valid deployment id");
        assert!(registry.resolve("field-rules.srl", &other).is_none());
    }

    #[test]
    fn a_syntactically_invalid_srl_ruleset_fails_the_plan_closed() {
        let dep = deployment_with_rule("broken.srl", "rule \"r\" when true then insert(1); end\n");
        let err = match plan_deployment(&dep) {
            Ok(_) => panic!("an invalid .srl must fail the PLAN"),
            Err(e) => e,
        };
        let message = err.to_string();
        assert!(message.contains("broken.srl"), "{message}");
        assert!(message.contains("failed to parse"), "{message}");
    }

    #[test]
    fn a_syntactically_invalid_redactor_template_fails_the_plan_closed() {
        // The same unterminated-block shape `sutra-redactor-template`'s own suite uses as its
        // known-invalid case (`syntactically_invalid_template_fails_at_construction`).
        let dep = deployment_with_redactor("broken.hbs", "<A>{{#if x}}unterminated");
        let err = match plan_deployment(&dep) {
            Ok(_) => {
                panic!("an invalid .hbs must fail the PLAN, not surface as a live locate() error")
            }
            Err(e) => e,
        };
        let message = err.to_string();
        assert!(message.contains("broken.hbs"), "{message}");
        assert!(message.contains("failed to compile"), "{message}");
    }
}

/// Plan-time resolution of a data store's declared `structure` (design
/// `datastore-schema-projection.md` §4.1 → §4.6): the store's own `schemas/<folder>` XSD codec is
/// compiled, the declared type's children enumerated, and the projection derived — or the deploy
/// is refused. A store WITHOUT a `structure` resolves to no projection at all, which is the
/// compatibility guarantee (its provider then behaves exactly as before).
#[cfg(test)]
mod projected_store_wiring_tests {
    use super::*;
    use std::collections::BTreeMap;
    use sutra_datastore::StructureRef;
    use sutra_loader::LoadedArtifact;

    const ACCOUNTS_XSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns="urn:sutra:test:accounts"
           targetNamespace="urn:sutra:test:accounts"
           elementFormDefault="qualified">
  <xs:complexType name="AccountRecord">
    <xs:sequence>
      <xs:element name="accountId" type="xs:string"/>
      <xs:element name="balance"   type="xs:decimal"/>
      <xs:element name="openedAt"  type="xs:date" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="NestedRecord">
    <xs:sequence>
      <xs:element name="accountId" type="xs:string"/>
      <xs:element name="address"   type="AccountRecord"/>
      <xs:element name="tag"       type="xs:string" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
"#;

    fn deployment_with_codec(folder: &str, xsd: &str) -> LoadedDeployment {
        let mut codecs = BTreeMap::new();
        codecs.insert(
            folder.to_string(),
            vec![LoadedArtifact {
                path: std::path::PathBuf::from(format!("schemas/{folder}/accounts.xsd")),
                content: xsd.to_string(),
            }],
        );
        LoadedDeployment {
            id: DeploymentId::of("dep-0000000000000000000000d2").expect("valid deployment id"),
            tenant: "t".to_string(),
            module: "m".to_string(),
            version: "1.0.0".to_string(),
            namespace: "urn:sutra:module:m:1.0.0".to_string(),
            processes: BTreeMap::new(),
            process_files: BTreeMap::new(),
            rules: BTreeMap::new(),
            templates: BTreeMap::new(),
            scripts: BTreeMap::new(),
            redactors: BTreeMap::new(),
            codecs,
            schema_files: BTreeMap::new(),
            migrations: BTreeMap::new(),
            coverage_files: BTreeMap::new(),
            coverages: Vec::new(),
            channels_yaml: None,
            datastores_yaml: None,
            binding_dir: std::path::PathBuf::new(),
        }
    }

    fn store_def(name: &str, table: Option<&str>) -> StoreDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("sql.url".to_string(), "postgres://h/db".to_string());
        if let Some(table) = table {
            properties.insert("sql.table".to_string(), table.to_string());
        }
        StoreDefinition {
            name: name.to_string(),
            store_type: "sql".to_string(),
            properties,
            structure: None,
        }
    }

    fn structure(type_name: &str) -> StructureRef {
        StructureRef {
            schema: "urn:accounts".to_string(),
            type_name: type_name.to_string(),
            columns: BTreeMap::new(),
        }
    }

    #[test]
    fn a_flat_structure_resolves_to_its_column_projection() {
        let d = deployment_with_codec("accounts", ACCOUNTS_XSD);
        let projected = resolve_projected_store(
            &d,
            &store_def("accounts", None),
            &structure("AccountRecord"),
        )
        .expect("a flat type projects");
        assert_eq!(projected.table(), "accounts", "table defaults to the store");
        assert_eq!(
            projected.projection().columns().collect::<Vec<_>>(),
            vec!["account_id", "balance", "opened_at"],
            "declared order, lowerCamel folded to snake_case"
        );
        assert!(
            projected.projection().field("openedAt").unwrap().nullable,
            "minOccurs=0 is a nullable column"
        );
    }

    #[test]
    fn sql_table_overrides_the_physical_table() {
        let d = deployment_with_codec("accounts", ACCOUNTS_XSD);
        let projected = resolve_projected_store(
            &d,
            &store_def("accounts", Some("ledger_rows")),
            &structure("AccountRecord"),
        )
        .expect("projects");
        assert_eq!(projected.table(), "ledger_rows");
    }

    #[test]
    fn a_non_flat_structure_is_a_fail_closed_deploy_error() {
        let d = deployment_with_codec("accounts", ACCOUNTS_XSD);
        let err =
            resolve_projected_store(&d, &store_def("accounts", None), &structure("NestedRecord"))
                .expect_err("nested + repeated content cannot be projected");
        assert!(err.contains("STRUCTURE_NOT_FLAT"), "{err}");
        assert!(err.contains("address"), "{err}");
        assert!(err.contains("tag"), "{err}");
    }

    #[test]
    fn an_unknown_schema_or_type_is_a_fail_closed_deploy_error() {
        let d = deployment_with_codec("accounts", ACCOUNTS_XSD);
        let err = resolve_projected_store(
            &d,
            &store_def("accounts", None),
            &StructureRef {
                schema: "urn:nowhere".to_string(),
                type_name: "AccountRecord".to_string(),
                columns: BTreeMap::new(),
            },
        )
        .expect_err("an unresolvable schema refuses the deploy");
        assert!(err.contains("no XSD codec"), "{err}");

        let err = resolve_projected_store(&d, &store_def("accounts", None), &structure("Nope"))
            .expect_err("an unresolvable type refuses the deploy");
        assert!(
            err.contains("neither as a type nor as a root element"),
            "{err}"
        );
    }

    /// The compatibility guarantee: no `structure` block, no projection — the provider is built
    /// exactly as it was before this work. (Async because the lazy pool the provider constructs
    /// needs a tokio context, as it does at engine boot.)
    #[tokio::test]
    async fn a_store_without_a_structure_carries_no_projection() {
        let def = store_def("accounts", None);
        assert!(def.structure.is_none());
        let backend = build_store_backend(&def, Vec::new(), None, None).expect("builds");
        assert!(matches!(backend, StoreBackend::Pg(_)));
    }
}
