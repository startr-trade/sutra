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
use sutra_channels::{
    describe_endpoint_refusal, find_endpoint_collisions, ChannelDefinition, DeclaredEndpoint,
    EndpointSharing, LiveDeploymentSet,
};
use sutra_executor::emission::CloudEventLite;
use sutra_executor::DeploymentId;
use sutra_persistence::stores::{
    ActiveArchive, EndpointHolderRow, OutboxEntry, OutboxStore, PgEndpointHolderStore,
    PgOutboxStore, ReplyMode, TimerScheduleArming,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use sutra_transport_spi::TransportChannels;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::runtime::Handle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::assembly::{build_engine, plan_deployment, DeploymentPlan};
use crate::deployment_facts::{
    already_shared_endpoints, deployment_slot, evaluate_candidate, holder_record_refusal,
    settle_claims, ConnectionView, DeployWarning, DeploymentFacts, EndpointHolding, EndpointKey,
    SettledClaim,
};

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

/// The advisory-lock key serialising deploy admission across replicas: the ASCII of `sutradep`.
const DEPLOY_ADMISSION_LOCK: i64 = 0x7375_7472_6164_6570;

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
    /// recent (len,mtime) change was REJECTED, or why its archive was refused an endpoint —
    /// surfaced as a `Failed` slot in the deployment-status endpoint. Cleared when the path next
    /// prepares cleanly and is judged.
    last_error: Option<String>,
    /// The higher versions serving in this archive's place, when it is superseded.
    superseded_by: Vec<String>,
    /// What this file may claim though this replica cannot serve it. `None` once it verifies.
    unjudged: Option<UnjudgedClaim>,
}

/// A file this replica could not verify. Another replica may be able to, so it still stands in the
/// way of anything that would take what it holds.
#[derive(Clone)]
enum UnjudgedClaim {
    /// It reads, and something this replica lacks stops it serving, a secret say. Its claims are known.
    Known(Box<DeploymentFacts>),
    /// It does not read. Nothing can be said about what it claims.
    Unreadable,
}

/// Why an archive failed preparation, and what that leaves of its claims.
struct ArchiveRejection {
    reason: String,
    unjudged: Option<UnjudgedClaim>,
}

/// Who holds which exclusive endpoint, as far as this replica can know. With a database that is
/// the shared record, so restarts and peers read one answer. Without one, what this process has seen
/// is all there is, and it is gone at a restart.
struct HolderLedger {
    held: BTreeMap<EndpointKey, EndpointHolding>,
    /// What this replica itself claimed last. Only a hold this replica watched end may be released:
    /// a replica whose folder is behind must not delete a peer's newer record.
    claimed: BTreeMap<EndpointKey, String>,
    store: Option<(PgEndpointHolderStore, Handle)>,
}

impl HolderLedger {
    /// Read the shared record, if there is one.
    fn refresh(&mut self) -> Result<(), String> {
        let Some((store, runtime)) = &self.store else {
            return Ok(());
        };
        let rows = runtime.block_on(store.load()).map_err(|e| e.to_string())?;
        self.held = rows
            .into_iter()
            .map(|row| {
                (
                    (row.transport, row.identity),
                    EndpointHolding {
                        owner: row.owner,
                        deployment_id: row.holder_deployment_id,
                        label: row.holder_label,
                        also_held_by: row.also_held_by.into_iter().collect(),
                    },
                )
            })
            .collect();
        Ok(())
    }

    /// Record `claims` as held, replacing any record of an owner that no longer claims the
    /// endpoint, and let go of what this replica held and no longer claims. `Ok(false)` when a peer
    /// got an endpoint first: nothing changed, and the caller judges again.
    fn record(&mut self, claims: BTreeMap<EndpointKey, EndpointHolding>) -> Result<bool, String> {
        let changed: Vec<(EndpointHolderRow, Option<String>)> = claims
            .iter()
            .filter(|(key, holding)| self.held.get(*key) != Some(*holding))
            .map(|(key, holding)| {
                let row = EndpointHolderRow {
                    transport: key.0.clone(),
                    identity: key.1.clone(),
                    owner: holding.owner.clone(),
                    holder_deployment_id: holding.deployment_id.clone(),
                    holder_label: holding.label.clone(),
                    also_held_by: holding.also_held_by.iter().cloned().collect(),
                };
                let stale_owner = self.held.get(key).map(|held| held.owner.clone());
                (row, stale_owner)
            })
            .collect();
        let released: Vec<(EndpointKey, String)> = self
            .claimed
            .iter()
            .filter(|(key, owner)| {
                claims
                    .get(*key)
                    .is_none_or(|holding| holding.owner != **owner)
            })
            .map(|(key, owner)| (key.clone(), owner.clone()))
            .collect();
        if let Some((store, runtime)) = &self.store {
            if !runtime
                .block_on(store.claim(&changed))
                .map_err(|e| e.to_string())?
            {
                return Ok(false);
            }
            for ((transport, identity), owner) in &released {
                if claims.contains_key(&(transport.clone(), identity.clone())) {
                    continue;
                }
                runtime
                    .block_on(store.release(transport, identity, owner))
                    .map_err(|e| e.to_string())?;
            }
        }
        for ((key, owner), _) in released.iter().map(|r| (r, ())) {
            if self
                .held
                .get(&key.clone())
                .is_some_and(|h| h.owner == *owner)
            {
                self.held.remove(key);
            }
        }
        self.claimed = claims
            .iter()
            .map(|(key, holding)| (key.clone(), holding.owner.clone()))
            .collect();
        self.held.extend(claims);
        Ok(true)
    }
}

/// One archive as the settling sees it.
struct JudgedArchive {
    path: PathBuf,
    plan: DeploymentPlan,
    /// This plan is the path's latest bytes, not the earlier content the path falls back on.
    latest: bool,
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
    /// What each ACTIVE deployment claims — what `POST /admin/deployments/check` judges a
    /// candidate against. Never serialized.
    pub(crate) facts: Vec<DeploymentFacts>,
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
    /// A flip failed, so the engine still serves the set of the last flip that landed, and what
    /// the directory wants instead is not served. Report that truth: a caller waiting on an
    /// unserved deployment must get a failed verdict, not Active.
    fn report_unapplied(&mut self, last_applied: &DeploymentStatusSnapshot, error: &str) {
        for (id, slot) in &self.active {
            if !last_applied.active.iter().any(|(served, _)| served == id) {
                self.failed.push((slot.clone(), error.to_string()));
            }
        }
        self.active = last_applied.active.clone();
        self.draining = last_applied.draining.clone();
        self.facts = last_applied.facts.clone();
    }

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
    /// Each path's latest verified bytes, whatever became of them: a superseded or refused
    /// archive's bytes will not change when what stood in its way leaves, so the stamp comparison
    /// would never look at it again.
    verified: BTreeMap<PathBuf, DeploymentPlan>,
    /// The folder needs judging: a file changed, or an earlier judgement could not finish.
    unsettled: bool,
    /// Why each path's latest bytes were refused while earlier content of it keeps serving.
    held_back: BTreeMap<PathBuf, String>,
    ledger: HolderLedger,
    /// Something listed could not even be examined this scan, so what it holds is not known.
    listing_incomplete: bool,
    /// The engine this runs in has a database, which some channels need.
    persistence: bool,
    /// The first judgement of this process. What it finds on an endpoint an earlier engine let
    /// deployments share was running before it, so it goes on running. Anything later is new.
    booting: bool,
    /// What was last said about deployments sharing such an endpoint, so it is said once.
    already_shared_reported: Vec<String>,
}

impl DeploymentDirectory {
    /// Boot construction: verify the dir exists (fail-closed, [`DEPLOY_SOURCE_MISSING`])
    /// and run the first scan. Individual bad archives are rejected-and-logged, never
    /// fatal — an empty or partially-bad dir boots an engine that serves the rest. The superseded
    /// versions a boot finds go straight to DRAINING: instances parked on them before the restart
    /// must have their definition to resume into.
    ///
    /// Blocks on `holders` when there is one, so it runs on a blocking thread.
    pub(crate) fn open(
        dir: PathBuf,
        holders: Option<(PgEndpointHolderStore, Handle)>,
        persistence: bool,
    ) -> Result<DeploymentDirectory, Box<dyn std::error::Error>> {
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
            verified: BTreeMap::new(),
            unsettled: true,
            held_back: BTreeMap::new(),
            ledger: HolderLedger {
                held: BTreeMap::new(),
                claimed: BTreeMap::new(),
                store: holders,
            },
            listing_incomplete: false,
            persistence,
            booting: true,
            already_shared_reported: Vec::new(),
        };
        directory.scan();
        if directory.unsettled {
            // Nothing can be judged without the record, and guessing could hand a holder's endpoint
            // to another owner.
            return Err("the endpoint holder record could not be read or written at boot".into());
        }
        directory.booting = false;
        directory.activate_desired();
        directory.drain_inactive_at_boot();
        Ok(directory)
    }

    /// Poll the directory once: re-read added/changed files through the verifying
    /// reader, drop removed paths. Returns `true` when the DESIRED active set differs
    /// from the current one (a flip is needed).
    pub(crate) fn scan(&mut self) -> bool {
        let dir = self.dir.clone();
        // A folder that cannot be listed says nothing about its files. Taking that as all of them
        // gone would drain everything and let go of what they hold.
        let Some(listed) = list_archives(&dir) else {
            return self.desired_active() != self.active;
        };
        let seen: HashSet<PathBuf> = listed.iter().cloned().collect();
        let files_before = self.files.len();
        // Removals first: an archive swapped for a differently named one in the same tick must
        // not collide with the file it replaces.
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
        let known = self.verified.len();
        self.verified.retain(|path, _| seen.contains(path));
        self.held_back.retain(|path, _| seen.contains(path));
        if self.verified.len() != known || self.files.len() != files_before {
            self.unsettled = true;
        }
        self.listing_incomplete = false;
        let host = sutra_transport_spi::PreflightHost {
            persistence: self.persistence,
        };
        for path in listed {
            let Some(stamp) = stamp_of(&path) else {
                // Raced a writer or a remover: the next tick settles it, and this one cannot say.
                self.listing_incomplete = true;
                continue;
            };
            if self.files.get(&path).is_some_and(|f| f.stamp == stamp) {
                continue;
            }
            // New or changed content — prepare fully off-line; rejection changes NOTHING.
            self.unsettled = true;
            match prepare_archive(&path, host) {
                Ok(plan) => {
                    // A new verdict replaces any earlier refusal of this path's old bytes.
                    self.held_back.remove(&path);
                    let (current, superseded_by) = self
                        .files
                        .get(&path)
                        .map(|f| (f.current.clone(), f.superseded_by.clone()))
                        .unwrap_or_default();
                    self.files.insert(
                        path.clone(),
                        ArchiveFile {
                            stamp,
                            current,
                            last_error: None,
                            superseded_by,
                            unjudged: None,
                        },
                    );
                    self.verified.insert(path, plan);
                }
                Err(ArchiveRejection { reason, unjudged }) => {
                    error!(
                        path = %path.display(),
                        error = %reason,
                        "archive rejected — the deployments source keeps its previous \
                         state for this path (a rejected archive changes nothing)"
                    );
                    let msg = reason;
                    self.verified.remove(&path);
                    self.held_back.remove(&path);
                    match self.files.get_mut(&path) {
                        // Known path: keep the previous contribution live, remember the
                        // stamp so the same bad bytes are not re-verified every tick, and
                        // record the error for the status endpoint.
                        Some(file) => {
                            file.stamp = stamp;
                            file.last_error = Some(msg);
                            file.unjudged = unjudged;
                        }
                        None => {
                            self.files.insert(
                                path,
                                ArchiveFile {
                                    stamp,
                                    current: None,
                                    last_error: Some(msg),
                                    superseded_by: Vec::new(),
                                    unjudged,
                                },
                            );
                        }
                    }
                }
            }
        }
        // A refused archive is judged every scan: what stood in its way may have gone.
        if self.unsettled || self.has_refused_archive() {
            self.settle();
        }
        self.desired_active() != self.active
    }

    fn has_refused_archive(&self) -> bool {
        !self.held_back.is_empty()
            || self.verified.keys().any(|path| {
                self.files
                    .get(path)
                    .is_some_and(|f| f.current.is_none() && f.superseded_by.is_empty())
            })
    }

    /// Judge the whole folder and apply the answer. It is a function of the files and the holder
    /// record alone, so a restart, a new pod and a running engine all come to the same answer, and
    /// nothing depends on the order files arrived in or on how they sort.
    ///
    /// If the record cannot be read or a peer keeps getting there first, nothing changes and the
    /// next scan tries again.
    fn settle(&mut self) {
        if let Err(e) = self.ledger.refresh() {
            error!(
                error = %e,
                "endpoint holder record unreadable — the deployments folder keeps its current state"
            );
            self.unsettled = true;
            return;
        }
        for _ in 0..3 {
            let (judged, outcomes) = self.judge();
            let claims = active_claims(&judged, &outcomes);
            match self.ledger.record(claims) {
                Ok(true) => {
                    self.apply_settlement(judged, outcomes);
                    self.unsettled = false;
                    return;
                }
                Ok(false) => {
                    if let Err(e) = self.ledger.refresh() {
                        error!(error = %e, "endpoint holder record unreadable");
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "endpoint holder record unwritable — the deployments folder keeps its \
                         current state"
                    );
                    break;
                }
            }
        }
        self.unsettled = true;
    }

    /// What each path offers, and what becomes of it. A path whose latest bytes are refused, or do
    /// not verify, falls back on the content it already serves, and that content is judged again in
    /// the same light: it may be all that path has left.
    fn judge(&mut self) -> (Vec<JudgedArchive>, Vec<SettledClaim>) {
        let mut fell_back: BTreeMap<PathBuf, String> = BTreeMap::new();
        loop {
            let judged = self.judged_archives(&fell_back);
            let facts: Vec<DeploymentFacts> = judged
                .iter()
                .map(|archive| {
                    let slot = archive
                        .path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    archive.plan.facts.clone().in_slot(&slot)
                })
                .collect();
            // What cannot serve here still claims: it goes after the archives, so the outcomes that
            // get applied are the archives' alone.
            let mut facts = facts;
            facts.extend(self.unjudged_claims(&judged, &facts));
            let outcomes = settle_claims(&facts, &self.holders_for(&facts));
            let mut more = false;
            for (archive, outcome) in judged.iter().zip(&outcomes) {
                let SettledClaim::Refused { reason } = outcome else {
                    continue;
                };
                let serves_other_content = self
                    .files
                    .get(&archive.path)
                    .and_then(|f| f.current.as_ref())
                    .is_some_and(|id| {
                        id != &archive.plan.dep && self.prepared.contains_key(id.value())
                    });
                if archive.latest && serves_other_content {
                    fell_back.insert(archive.path.clone(), reason.clone());
                    more = true;
                }
            }
            if !more {
                self.held_back = fell_back;
                return (judged, outcomes);
            }
        }
    }

    /// The archives to judge, one per path, the first path of any id, in path order.
    fn judged_archives(&self, fell_back: &BTreeMap<PathBuf, String>) -> Vec<JudgedArchive> {
        let ordered: BTreeMap<&PathBuf, &ArchiveFile> = self.files.iter().collect();
        let mut seen: HashSet<String> = HashSet::new();
        let mut out = Vec::new();
        for (path, file) in ordered {
            let latest = self
                .verified
                .get(path)
                .filter(|_| !fell_back.contains_key(path));
            let judged = match latest {
                Some(plan) => Some((plan, true)),
                None => file
                    .current
                    .as_ref()
                    .and_then(|id| self.prepared.get(id.value()))
                    .map(|plan| (plan, false)),
            };
            let Some((plan, latest)) = judged else {
                continue;
            };
            if seen.insert(plan.dep.value().to_string()) {
                out.push(JudgedArchive {
                    path: path.clone(),
                    plan: plan.clone(),
                    latest,
                });
            }
        }
        out
    }

    /// The claims of what this replica cannot judge. Another replica may serve these files, so they
    /// must not lose what they hold because of this one: an endpoint passes on only when its holder's
    /// file is gone, not when it is merely unjudged.
    ///
    /// A file that reads claims what it says, unless a version of its owner is being judged anyway:
    /// what this replica cannot serve must not supersede what it can. A file that does not read could
    /// be anyone, so while there is one, every recorded holder not otherwise accounted for stands.
    fn unjudged_claims(
        &self,
        judged: &[JudgedArchive],
        candidates: &[DeploymentFacts],
    ) -> Vec<DeploymentFacts> {
        let judged_paths: HashSet<&PathBuf> = judged.iter().map(|a| &a.path).collect();
        let mut claims: Vec<DeploymentFacts> = Vec::new();
        let mut unreadable = self.listing_incomplete;
        let ordered: BTreeMap<&PathBuf, &ArchiveFile> = self.files.iter().collect();
        for (path, file) in ordered {
            if judged_paths.contains(path) {
                continue;
            }
            match &file.unjudged {
                Some(UnjudgedClaim::Known(facts)) => {
                    if !candidates.iter().any(|c| c.owner == facts.owner) {
                        let slot = path
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        claims.push(facts.as_ref().clone().in_slot(&slot));
                    }
                }
                Some(UnjudgedClaim::Unreadable) => unreadable = true,
                None => {}
            }
        }
        if !unreadable {
            return claims;
        }
        for (key, holding) in &self.ledger.held {
            let accounted_for = candidates.iter().chain(&claims).any(|c| {
                c.owner == holding.owner
                    && c.endpoints
                        .iter()
                        .any(|e| e.transport == key.0 && e.claim.identity == key.1)
            });
            let taken_by_another = candidates.iter().any(|c| {
                c.owner != holding.owner
                    && c.endpoints.iter().any(|e| {
                        e.transport == key.0
                            && e.claim.identity == key.1
                            && e.claim.sharing != EndpointSharing::Broadcast
                    })
            });
            if accounted_for || !taken_by_another {
                continue;
            }
            claims.push(DeploymentFacts {
                deployment_id: holding.deployment_id.clone(),
                label: holding.label.clone(),
                owner: holding.owner.clone(),
                slot: "a file that cannot be read here".to_string(),
                endpoints: vec![DeclaredEndpoint {
                    deployment_id: holding.deployment_id.clone(),
                    deployment_label: holding.label.clone(),
                    owner: holding.owner.clone(),
                    channel: String::new(),
                    transport: key.0.clone(),
                    claim: sutra_channels::EndpointClaim::exclusive(key.1.clone()),
                }],
                ..DeploymentFacts::default()
            });
        }
        claims
    }

    /// Who holds each endpoint these candidates claim. The record says, when the owner it names
    /// still claims the endpoint here; a record of an owner with no claim here is stale. Beneath the
    /// record is what this engine serves right now, which holds an endpoint until told otherwise.
    /// On an endpoint that can have several, everything serving it holds it.
    ///
    /// At boot, where nothing records an endpoint an earlier engine let deployments share, whoever
    /// is on it now was running before: they all hold it.
    fn holders_for(&self, facts: &[DeploymentFacts]) -> BTreeMap<EndpointKey, EndpointHolding> {
        let mut holders: BTreeMap<EndpointKey, EndpointHolding> = BTreeMap::new();
        for file in self.files.values() {
            let Some(plan) = file
                .current
                .as_ref()
                .and_then(|id| self.prepared.get(id.value()))
            else {
                continue;
            };
            for endpoint in &plan.facts.endpoints {
                let key = (endpoint.transport.clone(), endpoint.claim.identity.clone());
                add_holder(&mut holders, key, endpoint, &plan.facts);
            }
        }
        if self.booting {
            for candidate in facts {
                for endpoint in &candidate.endpoints {
                    if matches!(
                        endpoint.claim.sharing,
                        EndpointSharing::ExclusiveToNewDeployments { .. }
                    ) {
                        let key = (endpoint.transport.clone(), endpoint.claim.identity.clone());
                        add_holder(&mut holders, key, endpoint, candidate);
                    }
                }
            }
        }
        for (key, holding) in &self.ledger.held {
            let claimed_here = facts.iter().any(|candidate| {
                (candidate.owner == holding.owner || holding.names(&candidate.deployment_id))
                    && candidate.endpoints.iter().any(|endpoint| {
                        endpoint.transport == key.0 && endpoint.claim.identity == key.1
                    })
            });
            if claimed_here {
                holders.insert(key.clone(), holding.clone());
            }
        }
        holders
    }

    fn apply_settlement(&mut self, judged: Vec<JudgedArchive>, outcomes: Vec<SettledClaim>) {
        let judged_paths: HashSet<PathBuf> = judged.iter().map(|a| a.path.clone()).collect();
        let mut first_path_of: HashMap<String, Option<DeploymentId>> = HashMap::new();
        for (archive, outcome) in judged.into_iter().zip(outcomes) {
            let JudgedArchive { path, plan, latest } = archive;
            let id = plan.dep.clone();
            let Some(file) = self.files.get_mut(&path) else {
                continue;
            };
            match outcome {
                SettledClaim::Active => {
                    info!(
                        path = %path.display(),
                        deployment = id.value(),
                        "archive verified and prepared (REGISTERED)"
                    );
                    file.current = Some(id.clone());
                    file.superseded_by.clear();
                    if latest {
                        file.last_error = None;
                    }
                    first_path_of.insert(id.value().to_string(), Some(id));
                    self.prepared.insert(plan.dep.value().to_string(), plan);
                }
                SettledClaim::Superseded { by } => {
                    if file.superseded_by != by {
                        warn!(
                            code = sutra_channels::codes::DEPLOY_ENDPOINT_SUPERSEDED,
                            path = %path.display(),
                            deployment = id.value(),
                            "{} is an older version than {}, on an endpoint they share: it takes \
                             no new work. Its running instances stay on it, and its file may stay",
                            plan.facts.label,
                            by.join(", "),
                        );
                    }
                    file.current = None;
                    file.superseded_by = by;
                    if latest {
                        file.last_error = None;
                    }
                    first_path_of.insert(id.value().to_string(), None);
                }
                SettledClaim::Refused { reason } => {
                    if file.last_error.as_ref() != Some(&reason) {
                        error!(
                            path = %path.display(),
                            error = %reason,
                            "archive refused — it is not deployed"
                        );
                    }
                    file.current = None;
                    file.superseded_by.clear();
                    file.last_error = Some(reason);
                    first_path_of.insert(id.value().to_string(), None);
                }
            }
        }
        // Latest bytes refused, earlier content still serving: the path reports the refusal.
        for (path, reason) in &self.held_back {
            if let Some(file) = self.files.get_mut(path) {
                if file.last_error.as_ref() != Some(reason) {
                    error!(
                        path = %path.display(),
                        error = %reason,
                        "archive refused — the path keeps serving its previous content"
                    );
                }
                file.last_error = Some(reason.clone());
            }
        }
        let serving: Vec<DeploymentFacts> = self
            .files
            .values()
            .filter_map(|f| f.current.as_ref())
            .filter_map(|id| self.prepared.get(id.value()))
            .map(|plan| plan.facts.clone())
            .collect();
        let shared = already_shared_endpoints(&serving);
        if shared != self.already_shared_reported {
            for message in &shared {
                error!(
                    code = sutra_channels::codes::DEPLOY_ENDPOINT_ALREADY_SHARED,
                    "{message}"
                );
            }
            self.already_shared_reported = shared;
        }
        // A second path holding a judged id contributes whatever the first came to.
        for (path, file) in self.files.iter_mut() {
            if judged_paths.contains(path) {
                continue;
            }
            let Some(id) = self
                .verified
                .get(path)
                .map(|plan| plan.dep.value().to_string())
            else {
                continue;
            };
            if let Some(contribution) = first_path_of.get(&id) {
                file.current = contribution.clone();
            }
        }
    }

    /// What a boot verified and did not activate goes straight to DRAINING, registered so parked
    /// instances can resume into it: a superseded version, and a refused archive too, which may be
    /// what served before this restart. It carries no routes. Nothing pinned to it means the first
    /// sweep retires it.
    fn drain_inactive_at_boot(&mut self) {
        let inactive: Vec<DeploymentPlan> = self
            .verified
            .values()
            .filter(|plan| !self.active.contains(&plan.dep))
            .cloned()
            .collect();
        for plan in inactive {
            if !self.draining.contains(&plan.dep) {
                self.draining.push(plan.dep.clone());
            }
            self.prepared.insert(plan.dep.value().to_string(), plan);
        }
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
        let facts = self
            .active_plans()
            .iter()
            .map(|p| p.facts.clone().in_slot(&slot_of(&p.dep)))
            .collect();
        DeploymentStatusSnapshot {
            active,
            pending: Vec::new(),
            draining,
            failed,
            facts,
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

/// The exclusive endpoints of the deployments that came out active, as the record should hold them.
fn active_claims(
    judged: &[JudgedArchive],
    outcomes: &[SettledClaim],
) -> BTreeMap<EndpointKey, EndpointHolding> {
    let mut claims = BTreeMap::new();
    for (archive, outcome) in judged.iter().zip(outcomes) {
        if *outcome != SettledClaim::Active {
            continue;
        }
        let facts = &archive.plan.facts;
        for endpoint in &facts.endpoints {
            let key = (endpoint.transport.clone(), endpoint.claim.identity.clone());
            add_holder(&mut claims, key, endpoint, facts);
        }
    }
    claims
}

/// Add `facts` as a holder of `key`. An exclusive endpoint has one holder and the last word wins. One
/// that can have several names them all, the lowest id first, so every replica writes the same row.
fn add_holder(
    holders: &mut BTreeMap<EndpointKey, EndpointHolding>,
    key: EndpointKey,
    endpoint: &DeclaredEndpoint,
    facts: &DeploymentFacts,
) {
    let holding = EndpointHolding::by(facts);
    if endpoint.claim.sharing == EndpointSharing::Exclusive {
        holders.insert(key, holding);
        return;
    }
    let Some(held) = holders.get_mut(&key) else {
        holders.insert(key, holding);
        return;
    };
    if held.names(&facts.deployment_id) {
        return;
    }
    if holding.deployment_id < held.deployment_id {
        let previous = std::mem::replace(held, holding);
        held.also_held_by = previous.also_held_by;
        held.also_held_by.insert(previous.deployment_id);
    } else {
        held.also_held_by.insert(holding.deployment_id);
    }
}

/// List `*.sutra` files (sorted, so what a scan logs and reports comes in a stable order). `None` when
/// the folder cannot be listed this tick.
fn list_archives(dir: &Path) -> Option<Vec<PathBuf>> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        warn!(dir = %dir.display(), "deployments dir unreadable this tick");
        return None;
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
    Some(out)
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
///
/// A folder has no caller to answer, and what lands in it could displace what is serving. So an
/// archive whose routes could never be served is rejected here, before it can supersede anything,
/// and at a restart it gives the same answer it gave at runtime.
///
/// An archive that reads but cannot serve here still says what it claims, for a replica that can
/// serve it may hold those endpoints. One the holder record could not store fails alone, and claims
/// nothing: no replica can serve it.
fn prepare_archive(
    path: &Path,
    host: sutra_transport_spi::PreflightHost,
) -> Result<DeploymentPlan, ArchiveRejection> {
    let archive = sutra_loader::read_archive_file(path).map_err(|e| ArchiveRejection {
        reason: format!("{e}"),
        unjudged: Some(UnjudgedClaim::Unreadable),
    })?;
    // A folder can't say whether a file was ever deployed. Refusing could strand instances parked
    // on one that was, so it loads, and the log says what it dedupes.
    report_unrecognised_dedup_keys(&archive.deployment);
    let known = |reason: String| ArchiveRejection {
        reason,
        unjudged: Some(
            DeploymentFacts::of_loaded(&archive.deployment, ConnectionView::Declared)
                .map(|facts| UnjudgedClaim::Known(Box::new(facts)))
                .unwrap_or(UnjudgedClaim::Unreadable),
        ),
    };
    let plan = plan_deployment(&archive.deployment).map_err(|e| known(format!("{e}")))?;
    if let Some(reason) = holder_record_refusal(&plan.facts) {
        return Err(ArchiveRejection {
            reason,
            unjudged: None,
        });
    }
    sutra_transport_spi::preflight_definitions(
        &plan.definitions,
        crate::assembly::envref_resolver,
        host,
    )
    .map_err(|d| known(format!("[{}] {}", d.code, d.message)))?;
    Ok(plan)
}

/// Verify + prepare a [`DeploymentPlan`] from in-memory archive bytes — the DB/API-source
/// analogue of [`prepare_archive`] (which reads a file). Fully off-line: a bad archive is a
/// rejected `Err`, never a panic.
pub(crate) fn prepare_archive_bytes(bytes: &[u8]) -> Result<DeploymentPlan, String> {
    let archive = sutra_loader::read_archive(bytes).map_err(|e| format!("{e}"))?;
    report_unrecognised_dedup_keys(&archive.deployment);
    plan_deployment(&archive.deployment).map_err(|e| format!("{e}"))
}

/// A package that declares a `dedupKey` nothing reads keeps running, and each load says so.
fn report_unrecognised_dedup_keys(deployment: &sutra_loader::LoadedDeployment) {
    let package = format!(
        "{}--{}--{}",
        deployment.tenant, deployment.module, deployment.version
    );
    for found in sutra_loader::unrecognised_dedup_keys(deployment) {
        error!(
            code = sutra_bpmn::codes::PARSE_Q_SOURCE_DEDUP_KEY_UNRECOGNISED,
            "{}",
            found.describe_running(&package)
        );
    }
}

/// Why a NEW deploy of this package is refused, if it declares a `dedupKey` nothing reads.
pub(crate) fn refuse_unrecognised_dedup_keys(
    deployment: &sutra_loader::LoadedDeployment,
) -> Result<(), String> {
    match sutra_loader::unrecognised_dedup_keys(deployment).first() {
        Some(found) => Err(format!(
            "[{}] {}",
            sutra_bpmn::codes::PARSE_Q_SOURCE_DEDUP_KEY_UNRECOGNISED,
            found.describe_refusal()
        )),
        None => Ok(()),
    }
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

/// The `db` source's served set, settled: what a boot or a re-activation activates.
pub(crate) struct SettledStoreSet {
    pub(crate) active: Vec<DeploymentPlan>,
    pub(crate) draining: Vec<DeploymentPlan>,
    /// `(deployment id, slot)` of the active rows.
    pub(crate) id_slots: Vec<(String, String)>,
    /// `(slot, why)` of rows this settling took out of the active set.
    pub(crate) refused: Vec<(String, String)>,
}

/// Read the store's served set and settle any rows that share an endpoint. Only rows an older engine
/// accepted can: of two owners the earliest activated holds and the later stops being active; of two
/// versions of one owner the highest stays. Every replica reads the same rows and comes to the same
/// answer. A row that loses goes to draining rather than away, because instances may be parked on it.
pub(crate) async fn settled_store_set(
    store: &sutra_persistence::stores::PgDeploymentArchiveStore,
) -> Result<SettledStoreSet, String> {
    let served = store
        .list_active_and_draining()
        .await
        .map_err(|e| e.to_string())?;
    let (mut active_rows, mut draining_rows) = (Vec::new(), Vec::new());
    for row in served {
        match row.status {
            sutra_persistence::stores::ArchiveStatus::Draining => draining_rows.push(row),
            _ => active_rows.push(row),
        }
    }
    let activated: HashMap<String, Option<OffsetDateTime>> = active_rows
        .iter()
        .map(|row| (row.archive.deployment_id.clone(), row.activated_at))
        .collect();
    let mut id_slots: Vec<(String, String)> = active_rows
        .iter()
        .map(|row| (row.archive.deployment_id.clone(), row.archive.slot.clone()))
        .collect();
    let slot_of = |id_slots: &[(String, String)], id: &str| {
        id_slots
            .iter()
            .find(|(row, _)| row == id)
            .map(|(_, slot)| slot.clone())
            .unwrap_or_default()
    };
    let bytes_of = |rows: Vec<sutra_persistence::stores::ServedArchiveRow>| {
        rows.into_iter()
            .map(|row| (row.archive.deployment_id, row.archive.bytes))
            .collect::<Vec<_>>()
    };
    let mut plans = plans_from_store(bytes_of(active_rows));
    let mut draining = plans_from_store(bytes_of(draining_rows));

    // The earliest activated first, so the first claimant of an endpoint is its holder.
    let mut order: Vec<usize> = (0..plans.len()).collect();
    order.sort_by_key(|i| {
        let id = plans[*i].dep.value().to_string();
        let at = activated.get(&id).copied().flatten();
        (at.is_none(), at, id)
    });
    let mut holders: BTreeMap<EndpointKey, EndpointHolding> = BTreeMap::new();
    for i in order {
        let facts = &plans[i].facts;
        for endpoint in &facts.endpoints {
            let key = (endpoint.transport.clone(), endpoint.claim.identity.clone());
            // An active row on an endpoint an earlier engine let several share keeps running. A
            // new one was refused at its deploy, so a row here was admitted before.
            if matches!(
                endpoint.claim.sharing,
                EndpointSharing::ExclusiveToNewDeployments { .. }
            ) {
                add_holder(&mut holders, key, endpoint, facts);
            } else {
                holders
                    .entry(key)
                    .or_insert_with(|| EndpointHolding::by(facts));
            }
        }
    }
    let facts: Vec<DeploymentFacts> = plans
        .iter()
        .map(|p| p.facts.clone().in_slot(&slot_of(&id_slots, p.dep.value())))
        .collect();
    let outcomes = settle_claims(&facts, &holders);
    let mut refused = Vec::new();
    let mut kept = Vec::new();
    for message in already_shared_endpoints(&facts) {
        error!(
            code = sutra_channels::codes::DEPLOY_ENDPOINT_ALREADY_SHARED,
            "{message}"
        );
    }
    for (plan, outcome) in plans.drain(..).zip(outcomes) {
        let reason = match outcome {
            SettledClaim::Active => {
                kept.push(plan);
                continue;
            }
            SettledClaim::Superseded { by } => format!(
                "[{}] {} is an older version than {}, on an endpoint they share",
                sutra_channels::codes::DEPLOY_ENDPOINT_SUPERSEDED,
                plan.facts.label,
                by.join(", ")
            ),
            SettledClaim::Refused { reason } => reason,
        };
        let id = plan.dep.value().to_string();
        store.demote_active(&id).await.map_err(|e| e.to_string())?;
        error!(
            deployment = %id,
            error = %reason,
            "stored deployment shares an endpoint with one that holds it — it is no longer active"
        );
        refused.push((slot_of(&id_slots, &id), reason));
        id_slots.retain(|(row, _)| row != &id);
        draining.push(plan);
    }
    Ok(SettledStoreSet {
        active: kept,
        draining,
        id_slots,
        refused,
    })
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
    /// The runtime's outbox enqueue → dispatcher link, handed to every rebuilt lane's bridge.
    pub(crate) outbox_signal: sutra_channels::OutboxSignal,
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

    // The last gate before anything changes. A deploy path has already judged its candidate; this
    // catches a set that arrived by another route (a peer replica's commit, a row stored by an
    // older engine), and asks every transport whether its rewire can succeed. A refusal here
    // leaves every registry, route and consumer as it was.
    let endpoints: Vec<DeclaredEndpoint> = active
        .iter()
        .flat_map(|plan| plan.facts.endpoints.iter().cloned())
        // Refused to anything new, and allowed to go on for what already runs on it: a deploy path
        // has judged those, and a set can only hold what those let through.
        .filter(|endpoint| {
            !matches!(
                endpoint.claim.sharing,
                EndpointSharing::ExclusiveToNewDeployments { .. }
            )
        })
        .collect();
    let collisions = find_endpoint_collisions(&endpoints);
    if !collisions.is_empty() {
        return Err(describe_endpoint_refusal(&collisions));
    }
    prepare_transports(&hooks.transports, &active_definitions)?;

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
    let outbox_signal = hooks.outbox_signal.clone();
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
            let outbox_signal = outbox_signal.clone();
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
                    outbox_signal,
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

/// Ask every transport whether it can serve `definitions`, changing nothing.
fn prepare_transports(
    transports: &[Arc<dyn TransportChannels>],
    definitions: &[ChannelDefinition],
) -> Result<(), String> {
    for transport in transports {
        transport
            .prepare_rewire(definitions)
            .map_err(|d| format!("[{}] {}", d.code, d.message))?;
    }
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
    pub(crate) warnings: Vec<DeployWarning>,
}

/// The result of an ACCEPTED async deploy (`202`) — the row is durably stored ACTIVE and marked
/// PENDING; the activation flip runs in the background. The caller reaches `Active` by polling
/// `GET /sutra/deployments/{id}` or by awaiting the completion CloudEvent.
pub(crate) struct DeployAccepted {
    pub(crate) deployment_id: String,
    pub(crate) slot: String,
    pub(crate) revision: i64,
    pub(crate) warnings: Vec<DeployWarning>,
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
        let (deployment_id, slot, revision, warnings) = self.admit_and_store(bytes).await?;
        self.reactivate_from_store()
            .await
            .map_err(|e| flip_failed_after_store(&slot, &e))?;
        info!(deployment = %deployment_id, slot = %slot, revision, "deployment ACTIVE via the sync deploy API");
        Ok(DeployOutcome {
            deployment_id,
            slot,
            revision,
            warnings,
        })
    }

    /// The durable half of a deploy: verify and plan the candidate, judge it against the store's
    /// other active rows, and only then store it ACTIVE. A refusal has changed nothing.
    ///
    /// The judgement and the write hold a database-wide advisory lock, so two replicas taking
    /// colliding deploys at once cannot each pass the check against a store without the other. All
    /// of it runs on the lock's own connection: a waiter holds one connection and needs no other, so
    /// waiters can never exhaust the pool out from under the deploy they are waiting for.
    async fn admit_and_store(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(String, String, i64, Vec<DeployWarning>), String> {
        let archive = sutra_loader::read_archive(&bytes).map_err(|e| format!("{e}"))?;
        refuse_unrecognised_dedup_keys(&archive.deployment)?;
        // A bad candidate is refused here rather than stored and then dropped from every flip.
        let plan = prepare_archive_bytes(&bytes)?;
        let dep = &archive.deployment;
        let deployment_id = dep.id.value().to_string();
        let (tenant, module, version) =
            (dep.tenant.clone(), dep.module.clone(), dep.version.clone());
        // The slot is the stable archive key — the tenant/module/version identity, which a
        // hot-deploy of new content targets in place (the ConfigMap-key / file-name analogue).
        let slot = deployment_slot(&tenant, &module, &version, &deployment_id);

        let mut tx = self.store.pool().begin().await.map_err(|e| e.to_string())?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(DEPLOY_ADMISSION_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        let served =
            sutra_persistence::stores::PgDeploymentArchiveStore::list_active_and_draining_in(
                &mut tx,
            )
            .await
            .map_err(|e| e.to_string())?;
        let (warnings, superseded_slots) = self.admit(&plan, &slot, served)?;
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
        let revision =
            sutra_persistence::stores::PgDeploymentArchiveStore::upsert_active_superseding_in(
                &mut tx,
                &new,
                &superseded_slots,
            )
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok((deployment_id, slot, revision, warnings))
    }

    /// Judge a planned candidate against the store's other active rows: refuse an endpoint
    /// collision, and refuse a set some transport could not rewire to. Returns the warnings and
    /// the slots of the same tenant and module this candidate supersedes.
    fn admit(
        &self,
        plan: &DeploymentPlan,
        slot: &str,
        served: Vec<sutra_persistence::stores::ServedArchiveRow>,
    ) -> Result<(Vec<DeployWarning>, Vec<String>), String> {
        let (active, _) = split_served_archives(served);
        let mut others = Vec::new();
        let mut other_definitions: Vec<(String, Vec<ChannelDefinition>)> = Vec::new();
        for row in active.into_iter().filter(|row| row.slot != slot) {
            // Only what the check reads: a full plan of every row belongs to the flip.
            let loaded = match sutra_loader::read_archive(&row.bytes) {
                Ok(archive) => archive.deployment,
                Err(e) => {
                    warn!(slot = %row.slot, error = %e, "stored archive skipped by the endpoint check");
                    continue;
                }
            };
            let facts = match DeploymentFacts::of_loaded(&loaded, ConnectionView::Resolved) {
                Ok(facts) => facts.in_slot(&row.slot),
                Err(e) => {
                    warn!(slot = %row.slot, error = %e, "stored archive skipped by the endpoint check");
                    continue;
                }
            };
            if let Some(yaml) = &loaded.channels_yaml {
                if let Ok(defs) = sutra_channels::load_channel_definitions(
                    yaml.as_bytes(),
                    &loaded.tenant,
                    &loaded.module,
                    &loaded.version,
                    "channels.yaml",
                ) {
                    other_definitions.push((row.slot.clone(), defs));
                }
            }
            others.push(facts);
        }
        let verdict = evaluate_candidate(&plan.facts.clone().in_slot(slot), &others);
        if !verdict.collisions.is_empty() {
            return Err(describe_endpoint_refusal(&verdict.collisions));
        }
        let superseded_slots: Vec<String> =
            verdict.superseded.iter().map(|s| s.slot.clone()).collect();
        // The set the flip will really serve: a superseded deployment is no part of it.
        let mut definitions = plan.definitions.clone();
        for (other_slot, defs) in other_definitions {
            if !superseded_slots.contains(&other_slot) {
                definitions.extend(defs);
            }
        }
        prepare_transports(&self.hooks.transports, &definitions)?;
        for warning in &verdict.warnings {
            warn!(code = %warning.code, slot = %slot, "{}", warning.message);
        }
        Ok((verdict.warnings, superseded_slots))
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
            self.reactivate_from_store()
                .await
                .map_err(|e| flip_failed_after_store(slot, &e))?;
        }
        Ok(retired)
    }

    /// Accept a deploy as a LONG-RUNNING operation: validate + store the row ACTIVE
    /// synchronously (the fast,
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
        let (deployment_id, slot, revision, warnings) = self.admit_and_store(bytes).await?;
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
            warnings,
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
        let SettledStoreSet {
            active: plans,
            draining: draining_plans,
            id_slots,
            refused,
        } = settled_store_set(&self.store).await?;
        let facts: Vec<DeploymentFacts> = plans
            .iter()
            .map(|p| {
                let slot = id_slots
                    .iter()
                    .find(|(id, _)| id == p.dep.value())
                    .map(|(_, slot)| slot.as_str())
                    .unwrap_or_default();
                p.facts.clone().in_slot(slot)
            })
            .collect();
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
                failed: refused,
                facts,
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

/// The durable write landed and the flip after it did not. Not a refusal: the stored state is what a
/// restart serves, so a caller that took this for "nothing happened" would be wrong.
fn flip_failed_after_store(slot: &str, error: &str) -> String {
    format!(
        "[{}] slot {slot} is stored, but this replica could not flip to it and still serves its \
         previous state; a restart serves what is stored: {error}",
        sutra_channels::codes::DEPLOY_ACTIVATE_FLIP_FAILED
    )
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
        // A failed apply (e.g. an auth reference that does not resolve) retries every tick until
        // a source change resolves it — the directory state is ahead of the engine then, and
        // the status reports what the engine really serves, not what the directory wants.
        let mut pending_apply = false;
        let mut apply_error: Option<String> = None;
        let mut last_applied = directory.status_snapshot();
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
                    Ok(()) => {
                        pending_apply = false;
                        apply_error = None;
                        last_applied = directory.status_snapshot();
                    }
                    Err(e) => {
                        // The flip aborted — old state stays live; next tick retries.
                        pending_apply = true;
                        error!(
                            error = %e,
                            "deployment activation failed — previous state stays live"
                        );
                        apply_error = Some(e);
                    }
                }
            }

            // Republish the state-machine snapshot every tick so /sutra/deployments
            // reflects the current active/draining/failed set — cheap (a small struct clone).
            if let Ok(mut s) = hooks.status.write() {
                let mut snapshot = directory.status_snapshot();
                if let Some(error) = &apply_error {
                    snapshot.report_unapplied(&last_applied, error);
                }
                *s = snapshot;
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
            facts: Vec::new(),
        }
    }

    #[test]
    fn a_failed_flip_reports_the_unapplied_deployment_failed_not_active() {
        let mut snap = snapshot();
        let last_applied = DeploymentStatusSnapshot {
            active: vec![("id-served".into(), "t--m--v".into())],
            ..Default::default()
        };
        snap.report_unapplied(&last_applied, "[CODE] no");
        assert_eq!(snap.active, last_applied.active);
        assert!(snap
            .failed
            .contains(&("t--m--v".to_string(), "[CODE] no".to_string())));
        assert!(snap.lookup("id-active").is_none());
        assert_eq!(snap.lookup("id-served").unwrap()["phase"], "Active");
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
