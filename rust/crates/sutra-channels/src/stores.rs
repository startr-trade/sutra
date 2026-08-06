//! Persistence hooks the intake pipeline calls — the persistence trait shapes
//! (`InboxStore`, `AliasStore`, and the outbox enqueue side of `OutboxStore`).
//! `sutra-persistence` provides the durable implementations; the in-memory impls here
//! serve tests and persistence-less hosts.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use sutra_executor::{DeploymentId, Emission};

/// Inbox dedup substrate — pipeline step 1, `(deployment, channel, event_id)`
/// first-observer-wins.
///
/// ASYNC seam (execution scale-out §3(a), Phase 3): awaited on the shard lane's single
/// actor task — see [`crate::bridge::InstanceBridge`] for the ordering argument. `?Send`
/// because the consumer is the `Rc`-based dispatcher.
#[async_trait::async_trait(?Send)]
pub trait InboxStore {
    /// Records the triple; `true` on first sight, `false` on a duplicate.
    async fn record_seen(&self, deployment: &DeploymentId, channel: &str, event_id: &str) -> bool;
}

/// In-memory [`InboxStore`] (unbounded; tests / persistence-less hosts).
#[derive(Debug, Default)]
pub struct InMemoryInboxStore {
    seen: RefCell<HashSet<(String, String, String)>>,
}

impl InMemoryInboxStore {
    pub fn new() -> InMemoryInboxStore {
        InMemoryInboxStore::default()
    }
}

#[async_trait::async_trait(?Send)]
impl InboxStore for InMemoryInboxStore {
    async fn record_seen(&self, deployment: &DeploymentId, channel: &str, event_id: &str) -> bool {
        self.seen.borrow_mut().insert((
            deployment.value().to_string(),
            channel.to_string(),
            event_id.to_string(),
        ))
    }
}

/// Alias-index substrate — the alias-store contract.
///
/// Deliberately SYNCHRONOUS (not part of the Phase 3 async conversion): the only
/// production wiring is the per-lane in-memory store for sync-path materialisation —
/// the DURABLE alias rows ride the park step through [`crate::bridge::InstanceBridge`],
/// and relay correlation reads them via the (async) `find_live_alias` there.
pub trait AliasStore {
    /// Records an alias row; `false` when `unique=true` collides with a LIVE row bound to
    /// a DIFFERENT instance (first inserter wins).
    fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> bool;

    /// The live instance carrying `(alias_name, alias_value)`, if any.
    fn find_live(
        &self,
        deployment: &DeploymentId,
        alias_name: &str,
        alias_value: &str,
    ) -> Option<String>;

    /// Marks every row of `instance_id` no longer live (instance reached terminal state).
    fn retire(&self, deployment: &DeploymentId, instance_id: &str);

    /// Every `(alias_name, alias_value)` currently recorded for the instance (tests/admin).
    fn list_for(&self, deployment: &DeploymentId, instance_id: &str) -> Vec<(String, String)>;
}

#[derive(Debug, Clone)]
struct AliasRow {
    instance_id: String,
    alias_name: String,
    alias_value: String,
    live: bool,
}

/// In-memory [`AliasStore`] mirroring the PostgreSQL unique-live-row semantics.
#[derive(Debug, Default)]
pub struct InMemoryAliasStore {
    rows: RefCell<HashMap<String, Vec<AliasRow>>>,
}

impl InMemoryAliasStore {
    pub fn new() -> InMemoryAliasStore {
        InMemoryAliasStore::default()
    }
}

impl AliasStore for InMemoryAliasStore {
    fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: &str,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> bool {
        let mut rows = self.rows.borrow_mut();
        let entry = rows.entry(deployment.value().to_string()).or_default();
        if unique {
            let collides = entry.iter().any(|r| {
                r.live
                    && r.alias_name == alias_name
                    && r.alias_value == alias_value
                    && r.instance_id != instance_id
            });
            if collides {
                return false;
            }
        }
        entry.push(AliasRow {
            instance_id: instance_id.to_string(),
            alias_name: alias_name.to_string(),
            alias_value: alias_value.to_string(),
            live: true,
        });
        true
    }

    fn find_live(
        &self,
        deployment: &DeploymentId,
        alias_name: &str,
        alias_value: &str,
    ) -> Option<String> {
        self.rows.borrow().get(deployment.value()).and_then(|rows| {
            rows.iter()
                .find(|r| r.live && r.alias_name == alias_name && r.alias_value == alias_value)
                .map(|r| r.instance_id.clone())
        })
    }

    fn retire(&self, deployment: &DeploymentId, instance_id: &str) {
        if let Some(rows) = self.rows.borrow_mut().get_mut(deployment.value()) {
            for r in rows.iter_mut() {
                if r.instance_id == instance_id {
                    r.live = false;
                }
            }
        }
    }

    fn list_for(&self, deployment: &DeploymentId, instance_id: &str) -> Vec<(String, String)> {
        self.rows
            .borrow()
            .get(deployment.value())
            .map(|rows| {
                rows.iter()
                    .filter(|r| r.instance_id == instance_id)
                    .map(|r| (r.alias_name.clone(), r.alias_value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// One dead-lettered inbound: a NON-idempotent process that FAILED during execution.
/// Blind redelivery-and-reprocess would duplicate side effects, so the delivery is consumed
/// (ack, at-most-once) and this durable record is the incident an operator inspects/replays.
///
/// Also carries the outbound `required`-delivery incident the outbox dispatcher records when it
/// poisons an entry: same seam, same durable table, `channel` = the destination URI and
/// `process_id`/payload fields empty (there is no inbound message to capture).
///
/// **The capture half is what makes a dead letter redrivable.** `payload` + `headers` +
/// `content_type` + `tenant` + `module_key` are exactly what the intake path needs to re-dispatch
/// the consumed message as a fresh delivery; the payload arrives here already truncated to the
/// channel's effective cap. All of it is raw business data — see the `dead_letter` store header
/// for the storage posture (deployment-RLS-scoped, admin-only, bytes never rendered).
#[derive(Clone, PartialEq, Eq)]
pub struct InboundIncident {
    /// The pinned deployment (`dep-<hex>`) the failing process belongs to — the isolation key the
    /// durable sink binds and RLS-scopes on, NOT the `module_key` namespace string.
    pub deployment: String,
    /// The channel the inbound arrived on (for an outbound incident: the destination URI).
    pub channel: String,
    /// The (non-idempotent) process whose execution failed.
    pub process_id: String,
    /// The transport-resolved dedup key of the consumed message (empty when none was supplied).
    pub dedup_key: String,
    /// The originating failure's diagnostic code (the CAUSE — e.g. `SUTRA.RUNTIME.TASK.UNCAUGHT`).
    pub failure_code: String,
    /// The originating failure's human message.
    pub detail: String,
    /// RFC 3339 receive stamp of the consumed inbound.
    pub received_at: String,
    /// The delivering tenant — replay re-stamps it (a client never supplies its own tenant).
    pub tenant: String,
    /// The `"<tenant>/<module>/<version>"` namespace key of the serving channel; with `channel`,
    /// the pair a replay resolves its binding by.
    pub module_key: String,
    /// The declared inbound media type, when the delivery carried one.
    pub content_type: Option<String>,
    /// The consumed body, TRUNCATED at the channel's effective payload cap. `None` ⇒ nothing was
    /// captured (an outbound incident, or a sink that predates capture) and replay fails closed.
    pub payload: Option<Vec<u8>>,
    /// The inbound transport headers, replayed verbatim.
    pub headers: BTreeMap<String, String>,
}

/// Masked by hand: an incident is logged and `{:?}`-dumped on the failure path, and the captured
/// payload/headers are raw business data — the one thing that must never ride a log line.
impl std::fmt::Debug for InboundIncident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundIncident")
            .field("deployment", &self.deployment)
            .field("channel", &self.channel)
            .field("process_id", &self.process_id)
            .field("dedup_key", &self.dedup_key)
            .field("failure_code", &self.failure_code)
            .field("detail", &self.detail)
            .field("received_at", &self.received_at)
            .field("tenant", &self.tenant)
            .field("module_key", &self.module_key)
            .field("content_type", &self.content_type)
            .field("payload_bytes", &self.payload.as_ref().map(Vec::len))
            .field("header_count", &self.headers.len())
            .finish()
    }
}

impl InboundIncident {
    /// The metadata-only skeleton every incident starts from: routing keys + cause, no capture.
    /// Callers that HAVE the consumed message add it with [`Self::with_capture`]; the outbound
    /// `required`-delivery path (which has no inbound message) leaves it empty.
    pub fn of_failure(
        deployment: impl Into<String>,
        channel: impl Into<String>,
        process_id: impl Into<String>,
        dedup_key: impl Into<String>,
        failure_code: impl Into<String>,
        detail: impl Into<String>,
        received_at: impl Into<String>,
    ) -> InboundIncident {
        InboundIncident {
            deployment: deployment.into(),
            channel: channel.into(),
            process_id: process_id.into(),
            dedup_key: dedup_key.into(),
            failure_code: failure_code.into(),
            detail: detail.into(),
            received_at: received_at.into(),
            tenant: String::new(),
            module_key: String::new(),
            content_type: None,
            payload: None,
            headers: BTreeMap::new(),
        }
    }

    /// Attach the replay capture: the routing keys plus the (already cap-truncated) body and its
    /// headers. Without it the row is an audit record only — the replay endpoint answers "no
    /// payload captured" rather than inventing an empty delivery.
    #[must_use]
    pub fn with_capture(
        mut self,
        tenant: impl Into<String>,
        module_key: impl Into<String>,
        content_type: Option<String>,
        payload: Vec<u8>,
        headers: BTreeMap<String, String>,
    ) -> InboundIncident {
        self.tenant = tenant.into();
        self.module_key = module_key.into();
        self.content_type = content_type;
        self.payload = Some(payload);
        self.headers = headers;
        self
    }
}

/// The durable dead-letter / incident seam. On a NON-idempotent process's execution
/// failure the dispatcher records the incident here (then acks — at-most-once), so the message
/// is never silently lost yet never blind-redelivered. `sutra-persistence` provides the durable
/// implementation; the in-memory impl here serves tests / persistence-less hosts.
///
/// ASYNC seam (execution scale-out §3(a), Phase 3). Unlike the `?Send` dispatcher-only
/// seams, this trait's futures are `Send`: the OUTBOX dispatcher — a spawned runtime task —
/// records `required`-delivery incidents through the same seam, so the future must be able
/// to live on a `Send` task. Implementations therefore need `&self` access that is `Sync`
/// (the in-memory impl uses a `Mutex`, not a `RefCell`).
#[async_trait::async_trait]
pub trait IncidentSink {
    /// Record one dead-lettered inbound. Best-effort from the dispatcher's view (a failure to
    /// record does not change the ack decision — the dispatcher also logs at error level).
    async fn record(&self, incident: InboundIncident);
}

/// In-memory [`IncidentSink`] for tests / persistence-less hosts — collects, never persists.
#[derive(Debug, Default)]
pub struct InMemoryIncidentSink {
    incidents: std::sync::Mutex<Vec<InboundIncident>>,
}

impl InMemoryIncidentSink {
    pub fn new() -> InMemoryIncidentSink {
        InMemoryIncidentSink::default()
    }

    pub fn incidents(&self) -> Vec<InboundIncident> {
        self.incidents.lock().expect("incident sink lock").clone()
    }

    pub fn len(&self) -> usize {
        self.incidents.lock().expect("incident sink lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.incidents
            .lock()
            .expect("incident sink lock")
            .is_empty()
    }
}

#[async_trait::async_trait]
impl IncidentSink for InMemoryIncidentSink {
    async fn record(&self, incident: InboundIncident) {
        self.incidents
            .lock()
            .expect("incident sink lock")
            .push(incident);
    }
}

/// Where destination-bearing emissions (`<q:send channel=…>`, `<q:reply destination=…>`)
/// land after a dispatch — the outbox ENQUEUE hook. Delivery (drain, retries,
/// `Idempotency-Key`) happens on the outbox side; `sutra-persistence` provides the durable row store.
///
/// Deliberately SYNCHRONOUS (not part of the Phase 3 async conversion): with a bridge
/// wired the durable enqueue rides the step transaction ([`crate::bridge::InstanceBridge`]);
/// this hook only serves the in-memory collect-only posture.
pub trait OutboxSink {
    fn enqueue(&self, emission: Emission);
}

/// In-memory [`OutboxSink`] for tests — collects, never delivers.
#[derive(Debug, Default)]
pub struct CollectingOutbox {
    emissions: RefCell<Vec<Emission>>,
}

impl CollectingOutbox {
    pub fn new() -> CollectingOutbox {
        CollectingOutbox::default()
    }

    pub fn emissions(&self) -> Vec<Emission> {
        self.emissions.borrow().clone()
    }

    pub fn len(&self) -> usize {
        self.emissions.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.emissions.borrow().is_empty()
    }
}

impl OutboxSink for CollectingOutbox {
    fn enqueue(&self, emission: Emission) {
        self.emissions.borrow_mut().push(emission);
    }
}
