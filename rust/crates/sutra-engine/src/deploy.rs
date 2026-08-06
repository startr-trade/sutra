//! The deployments source and two-phase activation.
//!
//! The engine consumes a DIRECTORY of sealed `.sutra` archives (`sutra.deployments.dir`;
//! directory-only in v1 — k8s mounts a volume). Every archive is read through the
//! verifying reader ([`sutra_loader::read_archive_file`]: manifest schema → per-artifact
//! digests → recomputed deploymentId → full parse + fail-closed validation) and prepared
//! into a [`DeploymentPlan`] fully OFF-LINE. A bad archive is rejected with its
//! `SUTRA.DEPLOY.*` diagnostic and changes NOTHING — other deployments are unaffected,
//! and a rewrite of a live archive that fails verification leaves the old deployment
//! serving (prepare fully, then swap).
//!
//! Activation is a pointer flip: the engine actor rebuilds its registries from the new
//! plan set BETWEEN dispatches ([`EngineHandle::update`] — the actor serialises, so
//! in-flight dispatches finish on the old snapshot and new intake sees the new one),
//! then the HTTP route table swaps ([`ChannelRouteTable::swap`] — a request resolves
//! against exactly one snapshot). Flipped-away deployments move to DRAINING: their
//! processes/artifacts stay registered under their own ids so instances PINNED to them
//! keep resuming via relay and timer, and they RETIRE (deregister) once quiescent
//! — zero active instances and zero pending outbox rows. Rollback is the same
//! mechanism: swap the old file back.
//!
//! The `db` source ([`DeployController`]) runs the SAME lifecycle from the
//! `deployment_archive` table rather than from a folder: a hot-deploy demotes the slot's prior
//! row to `draining`, the flip re-plans BOTH sets from their stored bytes (so a fresh pod or a
//! peer replica serves the pinned definitions too — the plan need not be resident anywhere),
//! and [`spawn_deploy_quiescence_sweep`] retires the drained rows on the same
//! zero-instances/zero-outbox gate the watch loop uses.
//!
//! Watching is a boring interval poll (`sutra.deployments.poll-interval`, default PT2S)
//! over `(len, mtime)` stamps — hot-reload degenerates to "archive file added / removed
//! / changed" (archive immutability is verified, not policed).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, LiveDeploymentSet};
use sutra_executor::emission::CloudEventLite;
use sutra_executor::DeploymentId;
use sutra_persistence::stores::{
    ActiveArchive, OutboxEntry, OutboxStore, PgOutboxStore, ReplyMode, TimerScheduleArming,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use sutra_transport_spi::TransportChannels;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::runtime::Handle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::assembly::{build_engine, plan_deployment, DeploymentPlan};

// ---- Activation diagnostics (the SUTRA.DEPLOY.* runtime-activation slice; the
// archive/package codes — SUTRA.DEPLOY.ARCHIVE.* etc. — live in sutra-loader) ---------

/// The configured deployments dir does not exist / is not a directory (boot refusal).
pub const DEPLOY_SOURCE_MISSING: &str = "SUTRA.DEPLOY.SOURCE.MISSING";
/// The configured deployments dir exists but cannot be LISTED by the engine user (boot
/// refusal — a permissions mistake must never boot a silently-empty engine).
pub const DEPLOY_SOURCE_UNREADABLE: &str = "SUTRA.DEPLOY.SOURCE.UNREADABLE";
/// Two archive files carry the same deploymentId. First archive in path order wins
/// (identical content — the later duplicate contributes nothing).
pub const DEPLOY_SOURCE_DUPLICATE_ID: &str = "SUTRA.DEPLOY.SOURCE.DUPLICATE_ID";
/// A flip touched `transport: rabbitmq` definitions — broker consumer topology is wired
/// at boot only in v1; HTTP routes and registries flip live.
pub const DEPLOY_ACTIVATE_BROKER_TOPOLOGY_STATIC: &str =
    "SUTRA.DEPLOY.ACTIVATE.BROKER_TOPOLOGY_STATIC";

/// `(len, mtime)` change stamp of one archive file — the poll comparator.
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    mtime: Option<SystemTime>,
}

/// One tracked `.sutra` path.
struct ArchiveFile {
    stamp: FileStamp,
    /// The deployment this path currently CONTRIBUTES. A rewrite that fails
    /// verification keeps the previous contribution (the old deployment stays live).
    current: Option<DeploymentId>,
    /// The last verification/preparation error for this path's current bytes, if the most
    /// recent (len,mtime) change was REJECTED — surfaced as a `Failed` slot in the
    /// deployment-status endpoint. Cleared when the path next prepares cleanly.
    last_error: Option<String>,
}

/// A point-in-time view of the deployment state machine, published by the watcher after
/// every scan/flip for the `/sutra/deployments` endpoints (deploy readiness).
///
/// The deploymentId IS the content generation (sha256 of the archive manifest), so a slot's
/// `deployment_id` doubles as its observed generation. A caller waits until the id it deployed
/// appears here `Active`; it fails fast when that slot is `Failed`.
#[derive(Clone, Default)]
pub(crate) struct DeploymentStatusSnapshot {
    /// Deployments currently serving intake — `(deploymentId, slot)`; slot is the archive
    /// filename (the ConfigMap key).
    pub(crate) active: Vec<(String, String)>,
    /// ASYNC deploys accepted (`202`) but whose activation flip has not yet completed —
    /// `(deploymentId, slot)`. A caller polls until its id moves from here to `active` (or `failed`).
    pub(crate) pending: Vec<(String, String)>,
    /// Deployments flipped away and finishing in-flight work (deploymentIds).
    pub(crate) draining: Vec<String>,
    /// Slots whose most recent bytes were REJECTED — `(slot, error)`. The slot keeps
    /// serving its previous deployment (or none); this is what lets a caller fail fast.
    pub(crate) failed: Vec<(String, String)>,
}

/// Shared, watcher-published deployment status read by the HTTP endpoints.
pub(crate) type SharedDeploymentStatus =
    std::sync::Arc<std::sync::RwLock<DeploymentStatusSnapshot>>;

/// Shared, watcher-published per-deployment OpenAPI specs — deploymentId → generated spec —
/// read by the `GET /sutra/deployments/{id}/openapi` endpoint. Republished on every flip alongside
/// the status snapshot, so the served surface always matches the live active/draining set.
pub(crate) type SharedApiSpecs = std::sync::Arc<
    std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<serde_json::Value>>>,
>;

impl DeploymentStatusSnapshot {
    /// The whole snapshot as the `/sutra/deployments` JSON body.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "active": self.active.iter()
                .map(|(id, slot)| serde_json::json!({"deploymentId": id, "slot": slot, "phase": "Active", "ready": true}))
                .collect::<Vec<_>>(),
            "pending": self.pending.iter()
                .map(|(id, slot)| serde_json::json!({"deploymentId": id, "slot": slot, "phase": "Pending", "ready": false}))
                .collect::<Vec<_>>(),
            "draining": self.draining,
            "failed": self.failed.iter()
                .map(|(slot, err)| serde_json::json!({"slot": slot, "phase": "Failed", "error": err}))
                .collect::<Vec<_>>(),
        })
    }

    /// One deployment's status by id (the content-hash deploymentId), or `None` if the engine
    /// has never seen it (caller keeps waiting — e.g. kubelet has not synced the ConfigMap yet).
    pub(crate) fn lookup(&self, id: &str) -> Option<serde_json::Value> {
        if let Some((_, slot)) = self.active.iter().find(|(dep, _)| dep == id) {
            return Some(serde_json::json!({
                "deploymentId": id, "slot": slot, "phase": "Active", "ready": true
            }));
        }
        if let Some((_, slot)) = self.pending.iter().find(|(dep, _)| dep == id) {
            return Some(serde_json::json!({
                "deploymentId": id, "slot": slot, "phase": "Pending", "ready": false
            }));
        }
        if self.draining.iter().any(|dep| dep == id) {
            return Some(serde_json::json!({
                "deploymentId": id, "phase": "Draining", "ready": false
            }));
        }
        None
    }
}

/// The engine's deployment state machine: what each archive path contributes, the
/// prepared plans (REGISTERED), the active set, and the DRAINING tail.
pub(crate) struct DeploymentDirectory {
    dir: PathBuf,
    files: HashMap<PathBuf, ArchiveFile>,
    /// Prepared archive plans by id — REGISTERED deployments (active or draining).
    prepared: HashMap<String, DeploymentPlan>,
    /// Archive deployments currently serving intake, in path order.
    active: Vec<DeploymentId>,
    /// Flipped-away deployments, most recently drained first (DRAINING).
    draining: Vec<DeploymentId>,
}

impl DeploymentDirectory {
    /// Boot construction: verify the dir exists (fail-closed, [`DEPLOY_SOURCE_MISSING`])
    /// and run the first scan. Individual bad archives are rejected-and-logged, never
    /// fatal — an empty or partially-bad dir boots an engine that serves the rest.
    pub(crate) fn open(dir: PathBuf) -> Result<DeploymentDirectory, Box<dyn std::error::Error>> {
        if !dir.is_dir() {
            return Err(format!(
                "[{DEPLOY_SOURCE_MISSING}] sutra.deployments.dir '{}' is not a \
                 directory — the deployments source must exist (it may be empty)",
                dir.display()
            )
            .into());
        }
        if let Err(e) = std::fs::read_dir(&dir) {
            return Err(format!(
                "[{DEPLOY_SOURCE_UNREADABLE}] sutra.deployments.dir '{}' cannot be \
                 listed by the engine user: {e} — refusing to boot a silently-empty \
                 deployments source (check mount permissions; the engine container \
                 runs unprivileged)",
                dir.display()
            )
            .into());
        }
        let mut directory = DeploymentDirectory {
            dir,
            files: HashMap::new(),
            prepared: HashMap::new(),
            active: Vec::new(),
            draining: Vec::new(),
        };
        directory.scan();
        directory.activate_desired();
        Ok(directory)
    }

    /// Poll the directory once: re-read added/changed files through the verifying
    /// reader, drop removed paths. Returns `true` when the DESIRED active set differs
    /// from the current one (a flip is needed).
    pub(crate) fn scan(&mut self) -> bool {
        let dir = self.dir.clone();
        let mut seen: HashSet<PathBuf> = HashSet::new();
        for path in list_archives(&dir) {
            seen.insert(path.clone());
            let Some(stamp) = stamp_of(&path) else {
                continue; // raced a writer/remover — next tick settles it
            };
            if self.files.get(&path).is_some_and(|f| f.stamp == stamp) {
                continue;
            }
            // New or changed content — prepare fully off-line; rejection changes NOTHING.
            match prepare_archive(&path) {
                Ok(plan) => {
                    let id = plan.dep.clone();
                    info!(
                        path = %path.display(),
                        deployment = id.value(),
                        "archive verified and prepared (REGISTERED)"
                    );
                    self.prepared.insert(id.value().to_string(), plan);
                    self.files.insert(
                        path,
                        ArchiveFile {
                            stamp,
                            current: Some(id),
                            last_error: None,
                        },
                    );
                }
                Err(e) => {
                    error!(
                        path = %path.display(),
                        error = %e,
                        "archive rejected — the deployments source keeps its previous \
                         state for this path (a rejected archive changes nothing)"
                    );
                    let msg = e.to_string();
                    match self.files.get_mut(&path) {
                        // Known path: keep the previous contribution live, remember the
                        // stamp so the same bad bytes are not re-verified every tick, and
                        // record the error for the status endpoint.
                        Some(file) => {
                            file.stamp = stamp;
                            file.last_error = Some(msg);
                        }
                        None => {
                            self.files.insert(
                                path,
                                ArchiveFile {
                                    stamp,
                                    current: None,
                                    last_error: Some(msg),
                                },
                            );
                        }
                    }
                }
            }
        }
        self.files.retain(|path, file| {
            let kept = seen.contains(path);
            if !kept {
                if let Some(id) = &file.current {
                    info!(
                        path = %path.display(),
                        deployment = id.value(),
                        "archive removed — deployment will drain"
                    );
                }
            }
            kept
        });
        self.desired_active() != self.active
    }

    /// The desired ACTIVE archive set: each path's contribution in path order, first
    /// claimant wins on a duplicated id.
    fn desired_active(&self) -> Vec<DeploymentId> {
        let ordered: BTreeMap<&PathBuf, &ArchiveFile> = self.files.iter().collect();
        let mut out: Vec<DeploymentId> = Vec::new();
        for (path, file) in ordered {
            let Some(id) = &file.current else { continue };
            if out.contains(id) {
                warn!(
                    code = DEPLOY_SOURCE_DUPLICATE_ID,
                    path = %path.display(),
                    deployment = id.value(),
                    "archive carries a deploymentId already contributed by an earlier \
                     path — identical content, first path wins"
                );
                continue;
            }
            out.push(id.clone());
        }
        out
    }

    /// Move the state machine to the desired set: newly-flipped-away ids join the FRONT
    /// of the draining list; resurrected ids (rollback: the old file swapped back) leave
    /// it. Returns the ids that started draining.
    pub(crate) fn activate_desired(&mut self) -> Vec<DeploymentId> {
        let desired = self.desired_active();
        let mut newly_draining: Vec<DeploymentId> = Vec::new();
        for id in &self.active {
            if !desired.contains(id) && !self.draining.contains(id) {
                newly_draining.push(id.clone());
            }
        }
        self.draining.retain(|id| !desired.contains(id));
        let mut draining = newly_draining.clone();
        draining.extend(self.draining.iter().cloned());
        self.draining = draining;
        self.active = desired;
        newly_draining
    }

    /// The plans serving intake: the active archives in path order.
    pub(crate) fn active_plans(&self) -> Vec<DeploymentPlan> {
        let mut out: Vec<DeploymentPlan> = Vec::new();
        for id in &self.active {
            if let Some(plan) = self.prepared.get(id.value()) {
                out.push(plan.clone());
            }
        }
        out
    }

    /// Every live deployment's generated OpenAPI spec (active + draining), keyed by deploymentId —
    /// the cache the `GET /sutra/deployments/{id}/openapi` endpoint reads. Cheap: each spec is
    /// an `Arc` cloned from the already-prepared plan.
    pub(crate) fn api_specs(
        &self,
    ) -> std::collections::HashMap<String, std::sync::Arc<serde_json::Value>> {
        let mut out = std::collections::HashMap::new();
        for plan in self.active_plans().into_iter().chain(self.draining_plans()) {
            out.insert(plan.dep.value().to_string(), plan.openapi_spec.clone());
        }
        out
    }

    /// Every live deployment's node index (active + draining), keyed by deploymentId — what the
    /// admin instance-migration validator resolves both graphs from. DRAINING is included for the
    /// same reason it is in `api_specs`, and more sharply: a migration's SOURCE is by definition a
    /// deployment that has been flipped away from.
    pub(crate) fn node_indexes(
        &self,
    ) -> std::collections::HashMap<String, std::sync::Arc<crate::migrate::DeploymentNodeIndex>>
    {
        let mut out = std::collections::HashMap::new();
        for plan in self.active_plans().into_iter().chain(self.draining_plans()) {
            out.insert(
                plan.dep.value().to_string(),
                std::sync::Arc::new(plan.node_index()),
            );
        }
        out
    }

    /// The DRAINING plans (most recently drained first) — registered for pinned resume,
    /// no routes, no newly-claimed bindings.
    pub(crate) fn draining_plans(&self) -> Vec<DeploymentPlan> {
        self.draining
            .iter()
            .filter_map(|id| self.prepared.get(id.value()).cloned())
            .collect()
    }

    /// The watcher's published view of the state machine: active deployments (with
    /// their slot = archive filename), draining ids, and failed slots. The slot is the
    /// archive path whose `current` contributes the id.
    pub(crate) fn status_snapshot(&self) -> DeploymentStatusSnapshot {
        let slot_of = |id: &DeploymentId| -> String {
            self.files
                .iter()
                .find(|(_, f)| f.current.as_ref() == Some(id))
                .and_then(|(p, _)| p.file_name().map(|n| n.to_string_lossy().into_owned()))
                .unwrap_or_default()
        };
        let active = self
            .active_plans()
            .iter()
            .map(|p| (p.dep.value().to_string(), slot_of(&p.dep)))
            .collect();
        let draining = self
            .draining
            .iter()
            .map(|id| id.value().to_string())
            .collect();
        let failed = self
            .files
            .iter()
            .filter_map(|(p, f)| {
                f.last_error.as_ref().map(|e| {
                    (
                        p.file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        e.clone(),
                    )
                })
            })
            .collect();
        DeploymentStatusSnapshot {
            active,
            pending: Vec::new(),
            draining,
            failed,
        }
    }

    /// Every live id — active archives + draining — for the background loops.
    pub(crate) fn live_ids(&self) -> Vec<DeploymentId> {
        self.active_plans()
            .iter()
            .map(|p| p.dep.clone())
            .chain(self.draining.iter().cloned())
            .collect()
    }

    /// Drop retired ids from the DRAINING tail (and the REGISTERED map when no path
    /// still contributes them). Returns `true` when anything retired.
    fn retire(&mut self, retired: &[DeploymentId]) -> bool {
        if retired.is_empty() {
            return false;
        }
        self.draining.retain(|id| !retired.contains(id));
        for id in retired {
            let still_contributed = self.files.values().any(|f| f.current.as_ref() == Some(id))
                || self.active.contains(id);
            if !still_contributed {
                self.prepared.remove(id.value());
            }
            info!(
                deployment = id.value(),
                "deployment retired (quiescent — zero instances, zero pending outbox)"
            );
        }
        true
    }
}

/// List `*.sutra` files (sorted for deterministic first-claimant-wins ordering).
fn list_archives(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        warn!(dir = %dir.display(), "deployments dir unreadable this tick");
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_file()
                && p.extension().is_some_and(|x| {
                    x.eq_ignore_ascii_case(sutra_loader::archive::ARCHIVE_EXTENSION)
                })
        })
        .collect();
    out.sort();
    out
}

fn stamp_of(path: &Path) -> Option<FileStamp> {
    let meta = std::fs::metadata(path).ok()?;
    Some(FileStamp {
        len: meta.len(),
        mtime: meta.modified().ok(),
    })
}

/// Read + verify one archive and prepare its plan — the whole off-line phase of the
/// two-phase activation. Any failure rejects the archive with its `SUTRA.DEPLOY.*`
/// (or plan-time) diagnostic.
fn prepare_archive(path: &Path) -> Result<DeploymentPlan, String> {
    let archive = sutra_loader::read_archive_file(path).map_err(|e| format!("{e}"))?;
    plan_deployment(&archive.deployment).map_err(|e| format!("{e}"))
}

/// Verify + prepare a [`DeploymentPlan`] from in-memory archive bytes — the DB/API-source
/// analogue of [`prepare_archive`] (which reads a file). Fully off-line: a bad archive is a
/// rejected `Err`, never a panic.
pub(crate) fn prepare_archive_bytes(bytes: &[u8]) -> Result<DeploymentPlan, String> {
    let archive = sutra_loader::read_archive(bytes).map_err(|e| format!("{e}"))?;
    plan_deployment(&archive.deployment).map_err(|e| format!("{e}"))
}

/// Build the boot-active plan set from the DB store's active rows (`(store_id, bytes)`): verify +
/// prepare each, logging REGISTERED / rejected exactly as the dir scan does. A bad row is dropped
/// (logged), never fatal — the engine serves the rest. This is the `db` deployment source's
/// boot-load (the dir source's [`DeploymentDirectory::open`] analogue).
pub(crate) fn plans_from_store(archives: Vec<(String, Vec<u8>)>) -> Vec<DeploymentPlan> {
    let mut plans = Vec::new();
    for (store_id, bytes) in archives {
        match prepare_archive_bytes(&bytes) {
            Ok(plan) => {
                info!(
                    deployment = plan.dep.value(),
                    store_id = %store_id,
                    "archive verified and prepared from store (REGISTERED)"
                );
                plans.push(plan);
            }
            Err(e) => {
                error!(store_id = %store_id, error = %e, "stored archive rejected — skipped");
            }
        }
    }
    plans
}

/// Everything the watcher needs to drive a flip.
pub(crate) struct ActivationHooks {
    pub(crate) engine: EngineHandle,
    pub(crate) deployments: LiveDeploymentSet,
    pub(crate) pool: Option<PgPool>,
    pub(crate) runtime: Handle,
    pub(crate) metrics_labels: Option<Vec<String>>,
    /// Global inbound payload-cap ceiling (`sutra.codec.max-payload-bytes`) rebuilt into
    /// each activated engine so a flip keeps the boot-time policy.
    pub(crate) payload_cap_bytes: u64,
    /// Crypto key provider (encryption at rest + GDPR blind-index recompute) — built once at boot
    /// and cloned into each activated engine so a flip keeps the same key material.
    /// Send+Sync so it can cross into the actor-thread `EngineHandle::update` closure.
    pub(crate) key_provider: Option<Arc<dyn sutra_crypto::KeyProvider + Send + Sync>>,
    /// Whether the durable dead-letter sink is wired on each activated engine
    /// (`sutra.incident.sql`), rebuilt on every flip like the audit config.
    pub(crate) incident_sql: bool,
    /// Terminal-instance retention (`sutra.instance.retention`) rebuilt into each activated
    /// engine, so a hot-deploy cannot silently flip a deployment back to delete-at-terminal
    /// (or on to retention) mid-flight.
    pub(crate) instance_retention: std::time::Duration,
    /// The engine-PROCESS-scoped deferred-ack registry, re-registered on each
    /// activated engine (listener + park-arm hook) so pending broker acks from a
    /// pre-flip activation still settle when their instance resumes post-flip.
    pub(crate) deferred_acks: std::sync::Arc<sutra_channels::DeferredAckRegistry>,
    /// Broker-topology rewire: every wired vendor transport, held behind the neutral
    /// [`sutra_transport_spi::TransportChannels`] trait (domain-neutrality refactor). On a
    /// flip the watcher reconciles each to the new active definitions (stop changed/removed,
    /// start added, keep unchanged) by iterating — naming no broker.
    pub(crate) transports: Vec<Arc<dyn TransportChannels>>,
    /// Deploy-readiness status: the watcher republishes the state-machine snapshot
    /// here after every tick; the `/sutra/deployments` endpoints read it.
    pub(crate) status: SharedDeploymentStatus,
    /// Per-deployment OpenAPI specs — republished after every flip alongside `status`, so
    /// `GET /sutra/deployments/{id}/openapi` tracks the live active/draining set.
    pub(crate) specs: SharedApiSpecs,
    /// Per-deployment node indexes — republished in lockstep with `specs`, so
    /// `POST /admin/instances/{id}/migrate` validates against the graphs that are live RIGHT NOW
    /// rather than against whatever a re-parse of a sealed archive would produce.
    pub(crate) node_indexes: crate::migrate::SharedNodeIndex,
    /// The composable audit-sink config (JSONL + OTel-log), re-applied to the rebuilt
    /// engine on every flip so the audit trail survives activation changes (`Default` = no
    /// engine-global audit sink).
    pub(crate) audit: crate::config::AuditConfig,
    /// TEST-ONLY (P1-7 time-skipping test runtime): `EngineConfig::now_override`, re-applied to
    /// the rebuilt executor on every flip AND used (instead of the real wall clock) as the
    /// arming instant for the flip's timer-START schedules. `None` on every production boot.
    pub(crate) now_override: Option<sutra_executor::TestClock>,
}

/// Apply the directory's current desired state to the running engine — the two-phase
/// swap: (1) build the new route set OFF-LINE (fail-fast: a route conflict aborts
/// the flip with the old state fully intact), (2) rebuild the engine registries on EVERY
/// shard's actor thread between that lane's dispatches, awaited on ALL lanes (the §5.1
/// fan-out barrier), (3) swap the HTTP route table, (4) refresh the
/// background loops' id set. In-flight dispatches finish on the old snapshot; requests
/// routed against the old table during (2)-(3) still resolve — the draining bindings
/// stay registered where unclaimed.
/// Flip the running engine to a new ACTIVE plan set: rebuild the actor-thread registries and
/// rewire every transport's channels. **Source-agnostic** — the dir watcher passes
/// `directory.active_plans()` etc.; the sync deploy API (db source) passes the store's active
/// plans. In-flight dispatches finish on the old snapshot; unchanged channels keep serving.
pub(crate) async fn activate_plans(
    active: Vec<DeploymentPlan>,
    draining: Vec<DeploymentPlan>,
    live_ids: Vec<DeploymentId>,
    hooks: &ActivationHooks,
) -> Result<(), String> {
    let (active_count, draining_count) = (active.len(), draining.len());

    // The new active channel definitions the flip rewires every transport to (collected
    // before `active` is moved into the actor rebuild below). Each transport — HTTP included —
    // filters this set by its own `transport:` in `rewire`.
    let active_definitions: Vec<ChannelDefinition> = active
        .iter()
        .flat_map(|plan| plan.definitions.iter().cloned())
        .collect();

    // The timer-start schedules the ACTIVE set declares, computed BEFORE `active` is moved into
    // the actor rebuild. The arming instant is read once so every relative `<timeDuration>` start
    // in this flip counts from the same moment.
    // TEST-ONLY (P1-7): `hooks.now_override` is `None` on every production boot, in which case
    // this is exactly `OffsetDateTime::now_utc()` as before.
    let arming_now = hooks
        .now_override
        .as_ref()
        .map(sutra_executor::TestClock::now)
        .unwrap_or_else(OffsetDateTime::now_utc);
    let active_schedules: Vec<(DeploymentId, Vec<TimerScheduleArming>)> = active
        .iter()
        .map(|plan| (plan.dep.clone(), plan.timer_schedules(arming_now)))
        .collect();
    // Schedules follow the ACTIVE deployment and NEVER the draining tail, so everything this
    // engine was serving a moment ago and is not activating now must stop minting work: the
    // deployment that just flipped away, the one a hot-deploy replaced in its slot, and the one
    // that finished draining and retired. `hooks.deployments` still holds the PREVIOUS live set
    // (it is replaced below), which is exactly the "was serving" list — union it with the
    // draining plans so a boot-time drained tail is covered too.
    let active_ids: BTreeSet<String> = active.iter().map(|p| p.dep.value().to_string()).collect();
    let obsolete_schedules: Vec<DeploymentId> = hooks
        .deployments
        .snapshot()
        .into_iter()
        .chain(draining.iter().map(|p| p.dep.clone()))
        .filter(|id| !active_ids.contains(id.value()))
        .fold(Vec::new(), |mut acc, id| {
            if !acc
                .iter()
                .any(|seen: &DeploymentId| seen.value() == id.value())
            {
                acc.push(id);
            }
            acc
        });

    // Coverage seed-at-deploy — once per activation. Since Phase 3 the covered-flag READ
    // rides along (`InitialCoverage`): the per-lane rebuild below runs ON each lane's
    // async actor task, so it must stay `block_on`-free — the snapshot is read HERE and
    // applied synchronously inside `build_engine` (`apply_initial_coverage`).
    let initial_coverage = crate::assembly::seed_declared_coverage(&active, &draining).await;

    // The flip's read-only registries — rebuilt ONCE for this activation, not once per lane
    // (execution scale-out §2 row 10). The plan set is shared for the same reason: with the
    // registries shared, a per-lane plan clone would be the last O(deployments × lanes) copy.
    // Both are `Arc`s the per-lane rebuild closures below merely point at, so the flip's cost is
    // one registry build no matter how many lanes swap onto it.
    let active = std::sync::Arc::new(active);
    let draining = std::sync::Arc::new(draining);
    let shared = crate::assembly::build_shared_registries(&active, &draining);

    let pool = hooks.pool.clone();
    let runtime = hooks.runtime.clone();
    let labels = hooks.metrics_labels.clone();
    let payload_cap_bytes = hooks.payload_cap_bytes;
    let audit = hooks.audit.clone();
    let key_provider = hooks.key_provider.clone();
    let incident_sql = hooks.incident_sql;
    let instance_retention = hooks.instance_retention;
    let deferred_acks = std::sync::Arc::clone(&hooks.deferred_acks);
    let now_override = hooks.now_override.clone();
    // Control-plane fan-out (execution scale-out §5.1): the factory below is called once
    // per shard ON THIS task, minting each lane its own rebuild closure over a CLONE of
    // the prepared plan set; `update` enqueues all of them and awaits all N swaps before
    // returning. Only after that barrier do the deployment-set replace, the timer-schedule
    // reconcile and the transport rewire below run — so none of those stages can race a
    // lane still serving the pre-flip engine. At `sutra.engine.shards = 1` this is exactly
    // the historic single swap.
    hooks
        .engine
        .update(|| {
            let active = std::sync::Arc::clone(&active);
            let draining = std::sync::Arc::clone(&draining);
            let shared = std::sync::Arc::clone(&shared);
            let pool = pool.clone();
            let runtime = runtime.clone();
            let labels = labels.clone();
            let audit = audit.clone();
            let key_provider = key_provider.clone();
            let deferred_acks = std::sync::Arc::clone(&deferred_acks);
            let now_override = now_override.clone();
            let initial_coverage = initial_coverage.clone();
            Box::new(move |engine: &mut sutra_channels::ChannelEngine| {
                // The rebuilt engine keeps the lane identity (and the lane's §6.1
                // counter handle) of the engine it replaces — the Update runs ON that
                // lane's actor thread (inside its async actor task since Phase 3, which
                // is why this closure must stay `block_on`-free), so each lane rebuilds
                // as itself.
                let shard = engine.shard();
                let shard_metrics = engine.shard_metrics();
                *engine = build_engine(
                    active,
                    draining,
                    shared,
                    pool,
                    runtime,
                    labels,
                    payload_cap_bytes,
                    audit,
                    key_provider,
                    incident_sql,
                    instance_retention,
                    deferred_acks,
                    now_override,
                    shard,
                    shard_metrics,
                    initial_coverage,
                );
            })
        })
        .await
        .map_err(|d| format!("[{}] {}", d.code, d.message))?;
    hooks.deployments.replace(live_ids);
    // Timer-start schedules ride the same flip: arm the ACTIVE set, retire everything that
    // stopped being active. Best-effort by design — a schedule write failure must not abort an
    // otherwise-good activation (the flip has already happened above), and the next flip
    // re-reconciles idempotently.
    reconcile_timer_schedules(&hooks.pool, &active_schedules, &obsolete_schedules).await;
    // Channel rewire — the SINGLE protocol-neutral activation path: reconcile every
    // transport's channels to the new active definitions by iterating the neutral trait. HTTP
    // (route-table swap) and the brokers (stop-changed/start-added consumer reconcile, with
    // at-least-once + inbox dedup keeping the handover lossless) flip through the SAME loop;
    // this names no protocol. Unchanged channels keep serving.
    for transport in &hooks.transports {
        transport.rewire(&active_definitions).await;
    }
    info!(
        active = active_count,
        draining = draining_count,
        "deployment activation flipped (registry swap + channel rewire across all transports)"
    );
    Ok(())
}

/// Arm the boot-time ACTIVE set's timer schedules.
///
/// Boot does NOT go through [`activate_plans`] — it builds the engine actor directly from the
/// prepared plans — and the dir watcher's first tick is deliberately skipped (boot already
/// activated). Without this call a statically-deployed engine would arm no schedules at all
/// until something in the deployments source happened to change, which is precisely the
/// situation a scheduled deployment is never in. Same idempotent upsert as the flip path, so a
/// restart re-arms without disturbing a schedule that is already counting down.
pub(crate) async fn arm_boot_schedules(
    pool: &Option<PgPool>,
    active: &[(DeploymentId, Vec<TimerScheduleArming>)],
) {
    reconcile_timer_schedules(pool, active, &[]).await;
}

/// Reconcile the durable timer-start schedule table against the flip that just happened.
///
/// Two halves, and the ORDER between them does not matter because they touch disjoint
/// deployment ids: arm every ACTIVE deployment's schedules (idempotent — a re-activation of a
/// still-armed deployment leaves its due-at alone; a re-activation of a RESOLVED one re-arms it
/// from scratch, which is what a rollback needs), and resolve every deployment that stopped
/// being active.
///
/// This is the single seam BOTH deploy sources reach: the dir watcher and the db-source deploy
/// controller each land in `activate_plans`, so hot-deploy slot replacement, undeploy and the
/// DRAINING-tail retirement are all covered without a second hook.
///
/// Failures are logged, never propagated: the registry flip has already committed, and a
/// schedule row that fails to arm now is armed by the next flip. Without a pool (persistence-less
/// host) there are no schedules at all and this is a no-op.
async fn reconcile_timer_schedules(
    pool: &Option<PgPool>,
    active: &[(DeploymentId, Vec<TimerScheduleArming>)],
    obsolete: &[DeploymentId],
) {
    let Some(pool) = pool else {
        return;
    };
    let store = sutra_persistence::stores::PgTimerScheduleStore::new(pool.clone());
    let persist_id = |id: &DeploymentId| match PersistDeploymentId::new(id.value()) {
        Ok(p) => Some(p),
        Err(e) => {
            warn!(deployment = id.value(), error = %e, "timer schedules skip deployment");
            None
        }
    };
    for (id, schedules) in active {
        let Some(persist) = persist_id(id) else {
            continue;
        };
        if let Err(e) = store.arm(&persist, schedules).await {
            warn!(deployment = id.value(), error = %e, "timer schedules could not be armed");
        } else if !schedules.is_empty() {
            info!(
                deployment = id.value(),
                schedules = schedules.len(),
                "timer-start schedules armed"
            );
        }
    }
    for id in obsolete {
        let Some(persist) = persist_id(id) else {
            continue;
        };
        match store.resolve_deployment(&persist).await {
            Ok(0) => {}
            Ok(n) => info!(
                deployment = id.value(),
                schedules = n,
                "timer-start schedules retired (deployment is no longer ACTIVE)"
            ),
            Err(e) => {
                warn!(deployment = id.value(), error = %e, "timer schedules could not be retired")
            }
        }
    }
}

/// The dir watcher's flip: activate the directory's current desired set.
async fn apply_activation(
    directory: &DeploymentDirectory,
    hooks: &ActivationHooks,
) -> Result<(), String> {
    activate_plans(
        directory.active_plans(),
        directory.draining_plans(),
        directory.live_ids(),
        hooks,
    )
    .await
}

/// The result of a successful sync deploy — the caller's finite `Active` signal.
pub(crate) struct DeployOutcome {
    pub(crate) deployment_id: String,
    pub(crate) slot: String,
    pub(crate) revision: i64,
}

/// The result of an ACCEPTED async deploy (`202`) — the row is durably stored ACTIVE and marked
/// PENDING; the activation flip runs in the background. The caller reaches `Active` by polling
/// `GET /sutra/deployments/{id}` or by awaiting the completion CloudEvent.
pub(crate) struct DeployAccepted {
    pub(crate) deployment_id: String,
    pub(crate) slot: String,
    pub(crate) revision: i64,
}

/// The synchronous deploy path for the `db` deployment source: validate the archive bytes →
/// store the row ACTIVE (replace-in-place) → run the activation flip → refresh the
/// deploy-status + OpenAPI-spec projections. Held in the admin `AppState`; the
/// `POST/DELETE /admin/deployments` handlers call it. It drives the SAME [`activate_plans`]
/// flip the dir watcher uses, so activation is confirmed in-process before the API returns —
/// no ConfigMap-propagation window.
pub(crate) struct DeployController {
    store: sutra_persistence::stores::PgDeploymentArchiveStore,
    hooks: ActivationHooks,
}

impl DeployController {
    pub(crate) fn new(
        store: sutra_persistence::stores::PgDeploymentArchiveStore,
        hooks: ActivationHooks,
    ) -> DeployController {
        DeployController { store, hooks }
    }

    /// Deploy (or hot-deploy replace-in-place) a sealed `.sutra` archive: validate the bytes
    /// fail-closed, store the row ACTIVE for its slot, re-activate the whole active set (the
    /// flip), and refresh the projections. Returns id/slot/revision once ACTIVE.
    pub(crate) async fn deploy(&self, bytes: Vec<u8>) -> Result<DeployOutcome, String> {
        let archive = sutra_loader::read_archive(&bytes).map_err(|e| format!("{e}"))?;
        let dep = &archive.deployment;
        let deployment_id = dep.id.value().to_string();
        let (tenant, module, version) =
            (dep.tenant.clone(), dep.module.clone(), dep.version.clone());
        // The slot is the stable archive key — the tenant/module/version identity, which a
        // hot-deploy of new content targets in place (the ConfigMap-key / file-name analogue).
        let slot = format!("{tenant}--{module}--{version}");
        let new = sutra_persistence::stores::NewArchive {
            deployment_id: deployment_id.clone(),
            slot: slot.clone(),
            tenant,
            module,
            version,
            // The content-hash deploymentId doubles as the integrity marker for v1 (a full-bytes
            // digest is a tracked refinement).
            checksum: deployment_id.clone(),
            bytes,
        };
        let revision = self
            .store
            .upsert_active(&new)
            .await
            .map_err(|e| e.to_string())?;
        self.reactivate_from_store().await?;
        info!(deployment = %deployment_id, slot = %slot, revision, "deployment ACTIVE via the sync deploy API");
        Ok(DeployOutcome {
            deployment_id,
            slot,
            revision,
        })
    }

    /// Undeploy a slot (retire its active row) + re-activate the remaining set. `Ok(false)` when
    /// the slot had no active row.
    pub(crate) async fn undeploy(&self, slot: &str) -> Result<bool, String> {
        let retired = self
            .store
            .retire_slot(slot)
            .await
            .map_err(|e| e.to_string())?;
        if retired {
            self.reactivate_from_store().await?;
        }
        Ok(retired)
    }

    /// Accept a deploy as a LONG-RUNNING operation (design: async-LRO section of
    /// `db-backed-deployment-store.md`): validate + store the row ACTIVE synchronously (the fast,
    /// fail-closed, DURABLE part), mark the id PENDING, then defer the potentially-long activation
    /// flip to a background task. Returns `DeployAccepted` for a `202` immediately — the caller
    /// observes completion by polling `GET /sutra/deployments/{id}` until Active, or by awaiting the
    /// completion CloudEvent. Suits large projects where the flip (registry rebuild + transport
    /// rewire) can outlast a k8s ingress `proxy-read-timeout`; the sync [`deploy`] stays the default
    /// for small/local archives.
    pub(crate) async fn deploy_async(
        self: &Arc<Self>,
        bytes: Vec<u8>,
        notify_sinks: Vec<String>,
    ) -> Result<DeployAccepted, String> {
        // Validate fail-closed and store ACTIVE — the same durable write the sync path does, so a
        // pod restart mid-flip boots the row from the store and pg LISTEN/NOTIFY converges the
        // fleet. Only the in-process flip is deferred.
        let archive = sutra_loader::read_archive(&bytes).map_err(|e| format!("{e}"))?;
        let dep = &archive.deployment;
        let deployment_id = dep.id.value().to_string();
        let (tenant, module, version) =
            (dep.tenant.clone(), dep.module.clone(), dep.version.clone());
        let slot = format!("{tenant}--{module}--{version}");
        let new = sutra_persistence::stores::NewArchive {
            deployment_id: deployment_id.clone(),
            slot: slot.clone(),
            tenant,
            module,
            version,
            checksum: deployment_id.clone(),
            bytes,
        };
        let revision = self
            .store
            .upsert_active(&new)
            .await
            .map_err(|e| e.to_string())?;
        // Publish the PENDING marker so a poller sees the accepted id before the flip lands (unless
        // a concurrent flip already made it active).
        if let Ok(mut s) = self.hooks.status.write() {
            let known = s
                .pending
                .iter()
                .chain(s.active.iter())
                .any(|(id, _)| id == &deployment_id);
            if !known {
                s.pending.push((deployment_id.clone(), slot.clone()));
            }
        }
        // Defer the (possibly long) activation flip; the caller already has its 202.
        let this = Arc::clone(self);
        let dep_id = deployment_id.clone();
        let dep_slot = slot.clone();
        tokio::spawn(async move {
            let result = this.reactivate_from_store().await;
            match &result {
                // `reactivate_from_store` carries pending→active (the flipped id drops out of
                // pending because it is now reported active), so nothing else to clear here.
                Ok(()) => info!(
                    deployment = %dep_id, slot = %dep_slot,
                    "async deployment ACTIVE (background flip complete)"
                ),
                Err(e) => {
                    this.fail_pending(&dep_id, &dep_slot, e);
                    error!(
                        deployment = %dep_id, slot = %dep_slot, error = %e,
                        "async deployment activation FAILED"
                    );
                }
            }
            // Completion notification (best-effort — never fails the deploy): emit a CloudEvent to
            // each requested sink through the durable outbox. On success the just-activated id is
            // live so the running dispatcher delivers it; a `.failed` event may be undeliverable
            // when the deployment never went live (the guaranteed failure signal is the status
            // poll, `GET /sutra/deployments/{id}`).
            this.emit_completion(&dep_id, &dep_slot, revision, &result, &notify_sinks)
                .await;
        });
        info!(
            deployment = %deployment_id, slot = %slot, revision,
            "deployment ACCEPTED (async) — stored ACTIVE, activation flip deferred"
        );
        Ok(DeployAccepted {
            deployment_id,
            slot,
            revision,
        })
    }

    /// Move a stuck-PENDING async deploy to the FAILED surface (fail-fast for pollers) when its
    /// background flip errored. The `failed` marker lives until the next successful flip recomputes
    /// the projection, matching the dir-watcher's fail semantics.
    fn fail_pending(&self, deployment_id: &str, slot: &str, err: &str) {
        if let Ok(mut s) = self.hooks.status.write() {
            s.pending.retain(|(id, _)| id != deployment_id);
            if !s.failed.iter().any(|(sl, _)| sl == slot) {
                s.failed.push((slot.to_string(), err.to_string()));
            }
        }
    }

    /// Emit the deploy-completion CloudEvent (`com.sutra.deployment.activated` /
    /// `com.sutra.deployment.failed`) to each requested sink by enqueuing one durable outbox row per
    /// destination — reusing the engine's existing outbound spine (outbox + dispatcher + the
    /// scheme-resolved `MessageSink`s: `http(s)://` webhooks, broker-scheme topics). Best-effort:
    /// any enqueue error is logged, never surfaced (the deploy already resolved). The row binds to
    /// the just-processed deployment id, so a `.activated` event delivers once that id is live; a
    /// `.failed` event for a deployment that never went live is not delivered by the
    /// deployment-scoped dispatcher — the guaranteed failure signal remains the status poll.
    async fn emit_completion(
        &self,
        deployment_id: &str,
        slot: &str,
        revision: i64,
        result: &Result<(), String>,
        sinks: &[String],
    ) {
        if sinks.is_empty() {
            return;
        }
        let Some(pool) = self.hooks.pool.clone() else {
            warn!(
                deployment = %deployment_id,
                "completion event requested but the engine has no db pool — skipping"
            );
            return;
        };
        let dep = match PersistDeploymentId::new(deployment_id) {
            Ok(d) => d,
            Err(e) => {
                warn!(deployment = %deployment_id, error = %e, "completion event: bad deployment id");
                return;
            }
        };
        let now = OffsetDateTime::now_utc();
        let (ce, body, outbox_key) = build_completion_event(
            deployment_id,
            slot,
            revision,
            result,
            now.format(&Rfc3339).ok(),
        );
        let cloud_event_json = sutra_channels::bridge::cloud_event_to_json(&ce);
        let store = PgOutboxStore::new(pool);
        for dest in sinks {
            let entry = OutboxEntry {
                deployment: dep.clone(),
                entry_id: Uuid::new_v4(),
                // Lifecycle event — no originating process instance.
                instance_id: Uuid::nil(),
                body: body.clone().into(),
                content_type: Some("application/json".to_string()),
                destination: dest.clone(),
                headers: BTreeMap::new(),
                // Best-effort: a failed completion notification must not raise an incident.
                required: false,
                mode: ReplyMode::CloudEventStructured,
                outbox_key: outbox_key.clone(),
                cloud_event_json: Some(cloud_event_json.clone()),
                auth_ref_json: None,
                labels: BTreeMap::new(),
                created_at: now,
                next_attempt_at: now,
                attempt_count: 0,
                last_diagnostic_json: None,
                traceparent: None,
                // Lifecycle event — no emitting BPMN node (like `instance_id`, nil above).
                node_id: None,
            };
            match store.enqueue(&entry).await {
                Ok(()) => info!(
                    deployment = %deployment_id, destination = %dest, ce_type = %ce.ce_type,
                    "enqueued deployment completion event"
                ),
                Err(e) => warn!(
                    deployment = %deployment_id, destination = %dest, error = %e,
                    "failed to enqueue deployment completion event"
                ),
            }
        }
    }

    /// Re-read the store's SERVED set, run the flip, and refresh the status + spec projections
    /// (what `/sutra/health/ready`, `/sutra/deployments`, and the CRD mirror read). Also the
    /// convergence entry point — [`spawn_deploy_listen`] calls it on a `LISTEN/NOTIFY` wakeup.
    ///
    /// The served set is ACTIVE **plus the DRAINING tail**, the same two-part shape the dir
    /// watcher flips. The tail is what keeps a hot-deploy honest: `upsert_active` demotes the
    /// slot's prior row to `draining` in the same transaction that activates the new content, so
    /// the flip that follows re-plans that demoted revision from its stored bytes and re-registers
    /// it under its own deployment id. Instances PINNED to it keep resuming (relay and timer both
    /// fail closed otherwise) until the quiescence sweep retires it. Re-planning from the TABLE
    /// rather than from a resident plan is deliberate — it is what lets a fresh pod, or a peer
    /// replica that never saw the deploy, serve the pinned definition too.
    pub(crate) async fn reactivate_from_store(&self) -> Result<(), String> {
        let served = self
            .store
            .list_active_and_draining()
            .await
            .map_err(|e| e.to_string())?;
        let (active, draining) = split_served_archives(served);
        let id_slots: Vec<(String, String)> = active
            .iter()
            .map(|a| (a.deployment_id.clone(), a.slot.clone()))
            .collect();
        let plans = plans_from_store(archive_bytes(active));
        let draining_plans = plans_from_store(archive_bytes(draining));
        let served_ids: std::collections::HashSet<String> =
            plans.iter().map(|p| p.dep.value().to_string()).collect();
        let draining_ids: Vec<String> = draining_plans
            .iter()
            .map(|p| p.dep.value().to_string())
            .collect();
        // The background loops (outbox dispatcher, timer poller) cover both sets: a pinned
        // instance on a draining deployment still fires timers and still drains emissions.
        let live_ids: Vec<DeploymentId> = plans
            .iter()
            .chain(draining_plans.iter())
            .map(|p| p.dep.clone())
            .collect();
        // Specs cover both sets too — `GET /sutra/deployments/{id}/openapi` answers for a
        // draining id while it is still resumable (the dir source's `api_specs` does the same).
        let specs: std::collections::HashMap<String, std::sync::Arc<serde_json::Value>> = plans
            .iter()
            .chain(draining_plans.iter())
            .map(|p| (p.dep.value().to_string(), p.openapi_spec.clone()))
            .collect();
        // Node indexes cover both sets for the same reason, and one sharper one: an instance
        // migration reads its SOURCE graph from a deployment that has been flipped away from.
        let node_indexes: std::collections::HashMap<
            String,
            std::sync::Arc<crate::migrate::DeploymentNodeIndex>,
        > = plans
            .iter()
            .chain(draining_plans.iter())
            .map(|p| {
                (
                    p.dep.value().to_string(),
                    std::sync::Arc::new(p.node_index()),
                )
            })
            .collect();
        activate_plans(plans, draining_plans, live_ids, &self.hooks).await?;
        // Status reflects only the successfully-served ids (a stored row that failed re-prepare is
        // dropped from the projection, not reported Active).
        if let Ok(mut s) = self.hooks.status.write() {
            let active: Vec<(String, String)> = id_slots
                .into_iter()
                .filter(|(id, _)| served_ids.contains(id))
                .collect();
            // Carry forward async-pending entries whose flip has not yet landed them in `active`
            // (a concurrent async deploy still mid-flight); the just-flipped id drops out here
            // because it is now reported active.
            let pending: Vec<(String, String)> = s
                .pending
                .iter()
                .filter(|(id, _)| !active.iter().any(|(a, _)| a == id))
                .cloned()
                .collect();
            *s = DeploymentStatusSnapshot {
                active,
                pending,
                draining: draining_ids,
                failed: Vec::new(),
            };
        }
        if let Ok(mut m) = self.hooks.specs.write() {
            *m = specs;
        }
        if let Ok(mut m) = self.hooks.node_indexes.write() {
            *m = node_indexes;
        }
        Ok(())
    }

    /// The db source's retire-when-quiescent sweep — the counterpart of the dir watcher's
    /// per-tick sweep, which the db source never had (its draining rows would otherwise be
    /// re-registered on every activation, forever).
    ///
    /// Each pass reads the served set, runs the SAME quiescence gate the watcher runs over the
    /// draining ids ([`quiescent_ids`]: zero active instances, zero pending outbox rows AND zero
    /// live external tasks), and
    /// flips each quiescent row `draining` → `retired`. Retired rows drop out of the served
    /// listing, so the re-activation that follows deregisters those definitions — and the store's
    /// notify carries the same conclusion to every peer replica. Returns how many rows retired.
    ///
    /// Fail-soft by construction: a row whose gate query errors is simply not retired this pass
    /// (the warning comes from the gate), and a retire that loses a race with a concurrent
    /// rollback re-deploy flips nothing — the guard is `status = 'draining'`.
    pub(crate) async fn sweep_quiescent_draining(&self) -> Result<usize, String> {
        let served = self
            .store
            .list_active_and_draining()
            .await
            .map_err(|e| e.to_string())?;
        let (_, draining) = split_served_archives(served);
        let candidates: Vec<DeploymentId> = draining
            .iter()
            .filter_map(|a| DeploymentId::of(&a.deployment_id).ok())
            .collect();
        if candidates.is_empty() {
            return Ok(0);
        }
        let quiescent = quiescent_ids(&candidates, &self.hooks.pool).await;
        let mut retired = 0usize;
        for id in &quiescent {
            match self.store.retire_deployment(id.value()).await {
                Ok(true) => {
                    retired += 1;
                    info!(
                        deployment = id.value(),
                        "deployment retired (quiescent — zero instances, zero pending outbox)"
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    warn!(deployment = id.value(), error = %e, "retire of a quiescent deployment failed")
                }
            }
        }
        if retired > 0 {
            self.reactivate_from_store().await?;
        }
        Ok(retired)
    }
}

/// Split a served listing into `(active, draining)`, preserving the store's order (per slot,
/// newest revision first) so the draining tail reaches the relay's scope walk most-recent-first.
fn split_served_archives(
    served: Vec<sutra_persistence::stores::ServedArchiveRow>,
) -> (Vec<ActiveArchive>, Vec<ActiveArchive>) {
    let (mut active, mut draining) = (Vec::new(), Vec::new());
    for row in served {
        match row.status {
            sutra_persistence::stores::ArchiveStatus::Draining => draining.push(row.archive),
            // `validated` / `retired` never reach here (the listing excludes them); anything
            // else is treated as live, which is the conservative reading for a serving row.
            _ => active.push(row.archive),
        }
    }
    (active, draining)
}

/// `(deployment_id, bytes)` pairs for [`plans_from_store`] — the store rows re-planned
/// off-line into deployment plans.
fn archive_bytes(archives: Vec<ActiveArchive>) -> Vec<(String, Vec<u8>)> {
    archives
        .into_iter()
        .map(|a| (a.deployment_id, a.bytes))
        .collect()
}

/// Spawn the db source's quiescence sweep: a boring interval poll that retires DRAINING
/// deployments once nothing is pinned to them any more. The dir source runs the same sweep
/// inside its watch loop; the db source has no watcher, so it gets its own task, riding the
/// SAME `sutra.deployments.poll-interval` cadence (floored at one second — the sweep touches
/// the db, and a quiescence decision is never urgent). Runs until aborted (engine shutdown).
pub(crate) fn spawn_deploy_quiescence_sweep(
    controller: Arc<DeployController>,
    poll_interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(poll_interval.max(std::time::Duration::from_secs(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately — skip it; boot just activated the served set.
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = controller.sweep_quiescent_draining().await {
                // Never fatal: the tail stays registered and the next tick retries.
                error!(error = %e, "draining-quiescence sweep failed — retrying next tick");
            }
        }
    })
}

/// Multi-replica convergence (pg): `LISTEN` on the `sutra_deployments` channel and re-activate the
/// store's active set whenever ANY replica commits a deploy/undeploy (the deploying replica
/// already flipped in-process; this converges the rest of the fleet). The deploying replica
/// also receives its own notification — the re-activate is idempotent. Best-effort: a
/// listener/connection error reconnects after a short backoff. (Non-pg dialects use a poll
/// fallback — a tracked follow-up.)
pub(crate) async fn spawn_deploy_listen(pool: PgPool, controller: Arc<DeployController>) {
    loop {
        match sqlx::postgres::PgListener::connect_with(&pool).await {
            Ok(mut listener) => {
                if let Err(e) = listener.listen("sutra_deployments").await {
                    error!(error = %e, "deploy convergence: LISTEN failed — retrying");
                } else {
                    info!("deploy convergence: LISTEN sutra_deployments (db source)");
                    loop {
                        match listener.recv().await {
                            Ok(notif) => {
                                let slot = notif.payload().to_string();
                                match controller.reactivate_from_store().await {
                                    Ok(()) => info!(
                                        slot = %slot,
                                        "deploy convergence: re-activated on notify"
                                    ),
                                    Err(e) => error!(
                                        slot = %slot, error = %e,
                                        "deploy convergence: re-activation failed"
                                    ),
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "deploy convergence: listener error — reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => error!(error = %e, "deploy convergence: connect failed — retrying"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// The DRAINING ids that are now quiescent: zero active instances AND zero pending
/// outbox rows. Persistence-less engines have nothing pinned — everything drains
/// immediately.
pub(crate) async fn quiescent_ids(
    draining: &[DeploymentId],
    pool: &Option<PgPool>,
) -> Vec<DeploymentId> {
    let Some(pool) = pool else {
        return draining.to_vec();
    };
    let mut out = Vec::new();
    for id in draining {
        let Ok(dep) = sutra_persistence::DeploymentId::new(id.value()) else {
            out.push(id.clone());
            continue;
        };
        let instances = sutra_persistence::stores::InstanceStore::count_active(
            &sutra_persistence::stores::PgInstanceStore::new(pool.clone()),
            &dep,
        )
        .await;
        // All legs of the gate go through the deployment-SCOPED store path: the count runs
        // inside a transaction with the `sutra.deployment_id` GUC set. A raw-pool count leaves
        // the GUC unset, and under an enforcing RLS posture the policy then evaluates
        // `deployment_id = NULL` → 0 rows → the deployment retires with replies still pending.
        let outbox = PgOutboxStore::new(pool.clone())
            .count_pending_for_deployment(&dep)
            .await;
        // Third leg: a parked external task is undelivered work exactly like a pending outbox
        // row — a worker still owes this deployment a result, and its completion re-enters
        // through a channel the retirement would tear down. `failed` (terminal) rows do not
        // pin, mirroring the outbox's poisoned posture.
        let external = sutra_persistence::stores::ExternalTaskStore::count_pending_for_deployment(
            &sutra_persistence::stores::PgExternalTaskStore::new(pool.clone()),
            &dep,
        )
        .await;
        match (instances, outbox, external) {
            (Ok(0), Ok(0), Ok(0)) => out.push(id.clone()),
            (Ok(_), Ok(_), Ok(_)) => {}
            (i, o, x) => {
                if let Err(e) = i {
                    warn!(deployment = id.value(), error = %e, "quiescence instance check failed");
                }
                if let Err(e) = o {
                    warn!(deployment = id.value(), error = %e, "quiescence outbox check failed");
                }
                if let Err(e) = x {
                    warn!(deployment = id.value(), error = %e, "quiescence external-task check failed");
                }
            }
        }
    }
    out
}

/// Spawn the deployments-dir watcher: a boring interval poll. Each tick: rescan; on a
/// desired-set change, run the two-phase flip; then retire any quiescent DRAINING
/// deployments (their registrations drop on the next rebuild). The task runs until
/// aborted (engine shutdown).
pub(crate) fn spawn_deployments_watch(
    mut directory: DeploymentDirectory,
    hooks: ActivationHooks,
    poll_interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(poll_interval.max(std::time::Duration::from_millis(100)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately — skip it; boot already activated the initial set.
        interval.tick().await;
        // Publish the boot-activated state now so /sutra/deployments is populated
        // before the first poll tick.
        if let Ok(mut s) = hooks.status.write() {
            *s = directory.status_snapshot();
        }
        if let Ok(mut m) = hooks.specs.write() {
            *m = directory.api_specs();
        }
        if let Ok(mut m) = hooks.node_indexes.write() {
            *m = directory.node_indexes();
        }
        // A failed apply (e.g. an active-set route conflict) retries every tick until a
        // source change resolves it — the directory state is ahead of the engine then.
        let mut pending_apply = false;
        loop {
            interval.tick().await;
            // Scan on a blocking thread (fs walk + archive verification + BPMN parse).
            let (dir, changed) = match tokio::task::spawn_blocking(move || {
                let changed = directory.scan();
                (directory, changed)
            })
            .await
            {
                Ok(result) => result,
                Err(e) => {
                    error!(error = %e, "deployments watch scan task failed — watcher stopped");
                    return;
                }
            };
            directory = dir;

            let mut needs_apply = changed;
            if changed {
                let newly = directory.activate_desired();
                for id in &newly {
                    info!(
                        deployment = id.value(),
                        "deployment DRAINING (flipped away)"
                    );
                }
            }

            // Retirement sweep: quiescent DRAINING deployments deregister.
            if !directory.draining.is_empty() {
                let retired = quiescent_ids(&directory.draining, &hooks.pool).await;
                if directory.retire(&retired) {
                    needs_apply = true;
                }
            }

            if needs_apply || pending_apply {
                match apply_activation(&directory, &hooks).await {
                    Ok(()) => pending_apply = false,
                    Err(e) => {
                        // The flip aborted — old state stays live; next tick retries.
                        pending_apply = true;
                        error!(
                            error = %e,
                            "deployment activation failed — previous state stays live"
                        );
                    }
                }
            }

            // Republish the state-machine snapshot every tick so /sutra/deployments
            // reflects the current active/draining/failed set — cheap (a small struct clone).
            if let Ok(mut s) = hooks.status.write() {
                *s = directory.status_snapshot();
            }
            // Republish the per-deployment OpenAPI specs + node indexes in lockstep with status.
            if let Ok(mut m) = hooks.specs.write() {
                *m = directory.api_specs();
            }
            if let Ok(mut m) = hooks.node_indexes.write() {
                *m = directory.node_indexes();
            }
        }
    })
}

/// Build the deploy-completion CloudEvent + its `data` payload + the idempotency `outbox_key`,
/// separated from the outbox enqueue so the event contract is unit-testable without a db pool.
/// Success → `com.sutra.deployment.activated`; failure → `com.sutra.deployment.failed` (with the
/// error carried in `data.error`). The CE `id` and `outbox_key` are stable per (deployment,
/// outcome) so the same logical event is deduped across sinks and redelivery.
fn build_completion_event(
    deployment_id: &str,
    slot: &str,
    revision: i64,
    result: &Result<(), String>,
    time_str: Option<String>,
) -> (CloudEventLite, Vec<u8>, String) {
    let (ce_type, status_str, error) = match result {
        Ok(()) => ("com.sutra.deployment.activated", "activated", None),
        Err(e) => ("com.sutra.deployment.failed", "failed", Some(e.as_str())),
    };
    let mut data = serde_json::json!({
        "deploymentId": deployment_id,
        "slot": slot,
        "revision": revision,
        "status": status_str,
    });
    if let Some(err) = error {
        data["error"] = serde_json::json!(err);
    }
    let body = serde_json::to_vec(&data).unwrap_or_default();
    let ce = CloudEventLite {
        id: format!("{deployment_id}:{status_str}:{revision}"),
        source: "urn:sutra:deployment-controller".to_string(),
        spec_version: "1.0".to_string(),
        ce_type: ce_type.to_string(),
        subject: Some(deployment_id.to_string()),
        time: time_str,
        data_content_type: Some("application/json".to_string()),
    };
    let outbox_key = format!("dep-complete:{deployment_id}:{status_str}");
    (ce, body, outbox_key)
}

#[cfg(test)]
mod completion_event_tests {
    use super::build_completion_event;

    #[test]
    fn activated_event_carries_type_subject_and_data() {
        let (ce, body, key) = build_completion_event(
            "dep-0123456789abcdef01234567",
            "acme--pay--1.0.0",
            3,
            &Ok(()),
            Some("2026-07-24T00:00:00Z".to_string()),
        );
        assert_eq!(ce.ce_type, "com.sutra.deployment.activated");
        assert_eq!(ce.subject.as_deref(), Some("dep-0123456789abcdef01234567"));
        assert_eq!(ce.spec_version, "1.0");
        assert_eq!(ce.data_content_type.as_deref(), Some("application/json"));
        assert_eq!(key, "dep-complete:dep-0123456789abcdef01234567:activated");
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["status"], "activated");
        assert_eq!(data["slot"], "acme--pay--1.0.0");
        assert_eq!(data["revision"], 3);
        assert!(data.get("error").is_none(), "no error on success");
    }

    #[test]
    fn failed_event_carries_failed_type_and_error() {
        let (ce, body, key) = build_completion_event(
            "dep-0123456789abcdef01234567",
            "acme--pay--1.0.0",
            4,
            &Err("registry rebuild failed".to_string()),
            None,
        );
        assert_eq!(ce.ce_type, "com.sutra.deployment.failed");
        assert_eq!(key, "dep-complete:dep-0123456789abcdef01234567:failed");
        assert!(ce.time.is_none());
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(data["status"], "failed");
        assert_eq!(data["error"], "registry rebuild failed");
    }
}

#[cfg(test)]
mod status_snapshot_tests {
    use super::DeploymentStatusSnapshot;

    fn snapshot() -> DeploymentStatusSnapshot {
        DeploymentStatusSnapshot {
            active: vec![("id-active".into(), "t--m--v".into())],
            pending: vec![("id-pending".into(), "t--m--v2".into())],
            draining: vec!["id-draining".into()],
            failed: vec![("t--m--v3".into(), "boom".into())],
        }
    }

    #[test]
    fn to_json_reports_pending_array() {
        let json = snapshot().to_json();
        let pending = json["pending"].as_array().expect("pending array present");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0]["deploymentId"], "id-pending");
        assert_eq!(pending[0]["slot"], "t--m--v2");
        assert_eq!(pending[0]["phase"], "Pending");
        assert_eq!(pending[0]["ready"], false);
        // The other buckets are unaffected.
        assert_eq!(json["active"].as_array().unwrap().len(), 1);
        assert_eq!(json["failed"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn lookup_reports_pending_between_active_and_draining() {
        let snap = snapshot();
        // A pending id resolves to phase Pending, not-ready, with its slot.
        let p = snap.lookup("id-pending").expect("pending id found");
        assert_eq!(p["phase"], "Pending");
        assert_eq!(p["ready"], false);
        assert_eq!(p["slot"], "t--m--v2");
        // Active still wins and reports ready; draining still reports Draining.
        assert_eq!(snap.lookup("id-active").unwrap()["phase"], "Active");
        assert_eq!(snap.lookup("id-active").unwrap()["ready"], true);
        assert_eq!(snap.lookup("id-draining").unwrap()["phase"], "Draining");
        // An unknown id is still None (caller keeps polling).
        assert!(snap.lookup("id-nope").is_none());
    }
}
