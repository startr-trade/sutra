//! The one check that runs before a deployment activates: what the candidate claims from the
//! namespaces every deployment of an engine shares, judged against what the other active
//! deployments claim.
//!
//! An endpoint another deployment holds exclusively refuses the candidate. A data store that would
//! share rows with another deployment only warns: that sharing is a legitimate deployment choice.
//!
//! Public because the CLI runs this same check, client-side, before it touches a target.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use sutra_channels::{
    deployment_owner, find_endpoint_collisions, ChannelDefinition, DeclaredEndpoint,
    EndpointCollision, EndpointSharing, UNLABELED_PACKAGE_LABEL,
};
use sutra_datastore::StoreConnectionUse;
use sutra_persistence::stores::{
    ENDPOINT_HOLDER_IDENTITY_MAX, ENDPOINT_HOLDER_LABEL_MAX, ENDPOINT_HOLDER_OWNER_MAX,
    ENDPOINT_HOLDER_TRANSPORT_MAX,
};
use sutra_transport_spi::declared_endpoints;

/// The "transport" of a channel's name. One engine routes a channel by its name within a package's
/// labels, so two deployments with those labels cannot both have a channel of that name, whatever
/// their routes. That makes the name an endpoint like any other, held by one owner: two packages
/// with no labels are not versions of one another, and neither can take it from the other.
pub const CHANNEL_NAME_ENDPOINT: &str = "channel-name";

/// How a data store's connection is compared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionView {
    /// As this process resolves it, so two reference names leading to one database compare equal.
    /// The engine's view: its environment is the one the deployment will run in.
    Resolved,
    /// As authored. For a client, which does not have the target's environment to resolve in.
    Declared,
}

/// What one deployment claims from the namespaces every deployment of this engine shares: its
/// inbound endpoints and its data-store connections. Cheap to derive from a verified archive,
/// without planning it.
#[derive(Clone, Default)]
pub struct DeploymentFacts {
    pub deployment_id: String,
    /// `tenant--module--version`, how a refusal or a warning names the deployment.
    pub label: String,
    /// Who the deployment belongs to across its versions, compared only ([`deployment_owner`]):
    /// versions of one owner may take endpoints from each other.
    pub owner: String,
    /// The `version` label. Decides between versions of one owner in a folder.
    pub version: String,
    /// `(channel, path, transport)`: channels bound under a path another transport serves.
    pub shadowed: Vec<(String, String, String)>,
    /// The deployment source's slot: replacing a slot's content is not a collision with itself.
    pub slot: String,
    pub endpoints: Vec<DeclaredEndpoint>,
    /// `(channel, transport)` of inbound channels no linked transport declares claims for.
    pub unchecked: Vec<(String, String)>,
    /// Compared in memory only and never reported: the identity of a connection may be its
    /// resolved URL.
    pub stores: Vec<StoreConnectionUse>,
    /// Each inbound channel as a newer version compares it ([`compare_with_older`]).
    pub inbound: Vec<InboundChannelFact>,
    /// Every start event and wait on a channel.
    pub subscriptions: Vec<ChannelSubscriptionFact>,
    /// `process/node` of each timer start event: these stop when the deployment drains.
    pub timer_starts: Vec<String>,
}

/// An inbound channel, as an upgrade compares it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InboundChannelFact {
    pub name: String,
    pub transport: Option<String>,
    /// The sorted endpoint identities, else the bind spec.
    pub address: String,
    pub codec: String,
    /// A digest of the package's schema files under the codec's folder; `None` for a bare format.
    pub schema_digest: Option<String>,
    /// The auth scheme and its properties as authored. Compared only: a message names "inbound
    /// auth", never a value.
    pub auth_view: String,
}

/// Whether a subscription starts an instance or resumes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionKind {
    Start,
    Wait,
}

/// One start event or wait on a channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelSubscriptionFact {
    pub kind: SubscriptionKind,
    pub process: String,
    pub node: String,
    pub channel: String,
    pub message_type: Option<String>,
    pub message_type_pattern: Option<String>,
}

/// The inbound channel facts of `definitions`, given the endpoints declared for them.
fn inbound_facts(
    d: &sutra_loader::LoadedDeployment,
    definitions: &[ChannelDefinition],
    endpoints: &[DeclaredEndpoint],
) -> Vec<InboundChannelFact> {
    definitions
        .iter()
        .filter(|def| !def.is_outbound())
        .map(|def| {
            let name = def.binding.channel_name.clone();
            let mut identities: Vec<&str> = endpoints
                .iter()
                .filter(|e| e.channel == name && e.transport != CHANNEL_NAME_ENDPOINT)
                .map(|e| e.claim.identity.as_str())
                .collect();
            identities.sort_unstable();
            let address = if identities.is_empty() {
                def.bind_spec.clone().unwrap_or_default()
            } else {
                identities.join(", ")
            };
            let codec = def.codec.clone().unwrap_or_default().trim().to_string();
            let scheme = def.auth_scheme.clone().unwrap_or_default();
            let prefix = format!("{scheme}.");
            let auth_view = std::iter::once(scheme.clone())
                .chain(
                    def.properties
                        .iter()
                        .filter(|(key, _)| !scheme.is_empty() && key.starts_with(&prefix))
                        .map(|(key, value)| format!("{key}={value}")),
                )
                .collect::<Vec<_>>()
                .join("\n");
            InboundChannelFact {
                schema_digest: schema_digest(d, &codec),
                name,
                transport: def.transport.clone(),
                address,
                codec,
                auth_view,
            }
        })
        .collect()
}

/// A digest over the package's schema files under the folder `codec` names, or `None` when no file
/// is there: a bare format, or a codec the engine provides.
fn schema_digest(d: &sutra_loader::LoadedDeployment, codec: &str) -> Option<String> {
    if codec.is_empty() {
        return None;
    }
    let local = codec
        .strip_prefix("urn:")
        .map(|rest| rest.strip_prefix("sutra:codec:").unwrap_or(rest))
        .unwrap_or(codec);
    let folder = local.replace(':', "/");
    let last = local.rsplit(':').next().unwrap_or(local);
    let mut bytes: Vec<u8> = Vec::new();
    for (path, artifact) in &d.schema_files {
        let under =
            |dir: &str| path.starts_with(&format!("{dir}/")) || *path == format!("{dir}.xsd");
        if under(&folder) || under(last) {
            bytes.extend_from_slice(path.as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(artifact.content.as_bytes());
            bytes.push(0);
        }
    }
    (!bytes.is_empty()).then(|| sutra_loader::archive::sha256_hex(&bytes))
}

/// Every start event and wait on a channel in the deployment's processes, and its timer starts.
fn subscription_facts(
    d: &sutra_loader::LoadedDeployment,
) -> (Vec<ChannelSubscriptionFact>, Vec<String>) {
    let mut subscriptions = Vec::new();
    let mut timers = Vec::new();
    let mut seen = BTreeSet::new();
    for module in d.processes.values() {
        for process in module.processes() {
            if !seen.insert(process.id.clone()) {
                continue;
            }
            for start in process.start_events() {
                if let sutra_bpmn::model::Node::StartEvent { channels, .. } = start {
                    let source = process.bindings_for(start.id()).source();
                    for channel in channels {
                        subscriptions.push(ChannelSubscriptionFact {
                            kind: SubscriptionKind::Start,
                            process: process.id.clone(),
                            node: start.id().to_string(),
                            channel: channel.clone(),
                            message_type: source.and_then(|s| s.message_type_value.clone()),
                            message_type_pattern: source
                                .and_then(|s| s.message_type_pattern.clone()),
                        });
                    }
                }
            }
            for wait in sutra_channels::wait_subscriptions(process) {
                subscriptions.push(ChannelSubscriptionFact {
                    kind: SubscriptionKind::Wait,
                    process: wait.process_id,
                    node: wait.node_id,
                    channel: wait.channel,
                    message_type: wait.message_type,
                    message_type_pattern: wait.message_type_pattern,
                });
            }
            for (node, _) in process.timer_start_events() {
                timers.push(format!("{}/{}", process.id, node));
            }
        }
    }
    (subscriptions, timers)
}

/// A channel a newer version changes under instances an older one has parked on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedChannel {
    pub channel: String,
    /// Of "inbound auth", "codec", "schema", "address", "transport", "renamed or replaced".
    pub changes: Vec<&'static str>,
    pub older_label: String,
    pub older_deployment_id: String,
    /// The older version's wait nodes on the channel: what its parked instances are counted on.
    pub wait_nodes: Vec<String>,
}

/// A start event a newer version adds on a channel an older one waits on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewStartOnWait {
    pub channel: String,
    pub start_node: String,
    pub older_label: String,
    pub older_deployment_id: String,
    pub wait_nodes: Vec<String>,
}

impl ChangedChannel {
    /// The warning, with the older version's parked instances counted (`None`: not counted here).
    pub fn warning(&self, parked: Option<u64>) -> DeployWarning {
        DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_CHANNEL_CHANGED.to_string(),
            message: format!(
                "channel '{}' changes {} from {}; {}",
                self.channel,
                self.changes.join(", "),
                self.older_label,
                parked_text(parked, &self.older_label)
            ),
        }
    }
}

impl NewStartOnWait {
    pub fn warning(&self, parked: Option<u64>) -> DeployWarning {
        DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_START_ON_WAIT.to_string(),
            message: format!(
                "channel '{}' gets start event '{}', where {} waits on it: start events are tried \
                 first, so a reply for a parked instance starts a new one; {}",
                self.channel,
                self.start_node,
                self.older_label,
                parked_text(parked, &self.older_label)
            ),
        }
    }
}

fn parked_text(parked: Option<u64>, older: &str) -> String {
    match parked {
        Some(1) => format!("1 instance of {older} is parked on it"),
        Some(n) => format!("{n} instances of {older} are parked on it"),
        None => "parked count unknown here: ask an engine (--engine-url)".to_string(),
    }
}

/// What `candidate` changes for an older version of its owner: channels it changes or takes the
/// endpoint of under another name, where the older version waits; and start events it adds where
/// the older version waits, with types that can overlap. An older channel nothing waits on has no
/// parked instance to warn about.
pub fn compare_with_older(
    candidate: &DeploymentFacts,
    older: &DeploymentFacts,
) -> (Vec<ChangedChannel>, Vec<NewStartOnWait>) {
    let waits_on = |channel: &str| -> Vec<String> {
        let mut nodes: Vec<String> = older
            .subscriptions
            .iter()
            .filter(|s| s.kind == SubscriptionKind::Wait && s.channel == channel)
            .map(|s| s.node.clone())
            .collect();
        nodes.sort();
        nodes.dedup();
        nodes
    };
    let mut changed = Vec::new();
    for old in &older.inbound {
        let wait_nodes = waits_on(&old.name);
        if wait_nodes.is_empty() {
            continue;
        }
        let mut changes: Vec<&'static str> = Vec::new();
        match candidate.inbound.iter().find(|new| new.name == old.name) {
            Some(new) => {
                if new.auth_view != old.auth_view {
                    changes.push("inbound auth");
                }
                if new.codec != old.codec {
                    changes.push("codec");
                }
                if new.schema_digest != old.schema_digest {
                    changes.push("schema");
                }
                if new.address != old.address {
                    changes.push("address");
                }
                if new.transport != old.transport {
                    changes.push("transport");
                }
            }
            None => {
                // Its endpoint served under another name: nothing maps the parked instances over.
                let identities: Vec<&str> = older
                    .endpoints
                    .iter()
                    .filter(|e| e.channel == old.name && e.transport != CHANNEL_NAME_ENDPOINT)
                    .map(|e| e.claim.identity.as_str())
                    .collect();
                let taken = candidate.endpoints.iter().any(|e| {
                    e.channel != old.name
                        && e.transport != CHANNEL_NAME_ENDPOINT
                        && identities.contains(&e.claim.identity.as_str())
                });
                if taken {
                    changes.push("renamed or replaced");
                }
            }
        }
        if !changes.is_empty() {
            changed.push(ChangedChannel {
                channel: old.name.clone(),
                changes,
                older_label: older.label.clone(),
                older_deployment_id: older.deployment_id.clone(),
                wait_nodes,
            });
        }
    }
    let mut starts = Vec::new();
    for start in candidate
        .subscriptions
        .iter()
        .filter(|s| s.kind == SubscriptionKind::Start)
    {
        let already = older.subscriptions.iter().any(|s| {
            s.kind == SubscriptionKind::Start
                && s.channel == start.channel
                && s.message_type == start.message_type
                && s.message_type_pattern == start.message_type_pattern
        });
        if already {
            continue;
        }
        let overlapping: Vec<String> = older
            .subscriptions
            .iter()
            .filter(|s| {
                s.kind == SubscriptionKind::Wait
                    && s.channel == start.channel
                    && sutra_channels::message_types_can_overlap(
                        s.message_type.as_deref(),
                        s.message_type_pattern.as_deref(),
                        start.message_type.as_deref(),
                        start.message_type_pattern.as_deref(),
                    )
            })
            .map(|s| s.node.clone())
            .collect();
        if !overlapping.is_empty() {
            starts.push(NewStartOnWait {
                channel: start.channel.clone(),
                start_node: start.node.clone(),
                older_label: older.label.clone(),
                older_deployment_id: older.deployment_id.clone(),
                wait_nodes: overlapping,
            });
        }
    }
    (changed, starts)
}

impl DeploymentFacts {
    /// The facts of a loaded deployment whose channels and stores are already parsed. The default
    /// slot is the `db` source's slot key; other sources set theirs with [`Self::in_slot`].
    pub fn of(
        d: &sutra_loader::LoadedDeployment,
        definitions: &[ChannelDefinition],
        stores: &[sutra_datastore::StoreDefinition],
        view: ConnectionView,
    ) -> DeploymentFacts {
        let label = format!("{}--{}--{}", d.tenant, d.module, d.version);
        let mut declared = declared_endpoints(d.id.value(), &label, definitions);
        let owner = deployment_owner(&d.tenant, &d.module, d.id.value());
        // Outbound channels too: they are registered by name just the same.
        let mut names: BTreeSet<(String, &str)> = BTreeSet::new();
        for def in definitions {
            let name = (
                def.binding.namespace.module_key(),
                def.binding.channel_name.as_str(),
            );
            if names.insert(name) {
                declared.endpoints.push(DeclaredEndpoint {
                    deployment_id: d.id.value().to_string(),
                    deployment_label: label.clone(),
                    owner: owner.clone(),
                    channel: def.binding.channel_name.clone(),
                    transport: CHANNEL_NAME_ENDPOINT.to_string(),
                    claim: sutra_channels::EndpointClaim::exclusive(format!(
                        "{} channel {}",
                        def.binding.namespace.module_key(),
                        def.binding.channel_name
                    )),
                });
            }
        }
        let inbound = inbound_facts(d, definitions, &declared.endpoints);
        let (subscriptions, timer_starts) = subscription_facts(d);
        DeploymentFacts {
            deployment_id: d.id.value().to_string(),
            owner,
            version: d.version.clone(),
            shadowed: declared.shadowed,
            slot: deployment_slot(&d.tenant, &d.module, &d.version, d.id.value()),
            label,
            endpoints: declared.endpoints,
            unchecked: declared.unchecked,
            inbound,
            subscriptions,
            timer_starts,
            stores: match view {
                ConnectionView::Resolved => sutra_datastore::resolved_store_connections(stores),
                ConnectionView::Declared => sutra_datastore::declared_store_connections(stores),
            },
        }
    }

    /// Derive the facts of a verified archive, without planning it.
    pub fn of_loaded(
        d: &sutra_loader::LoadedDeployment,
        view: ConnectionView,
    ) -> Result<DeploymentFacts, String> {
        let definitions = match &d.channels_yaml {
            Some(yaml) => sutra_channels::load_channel_definitions(
                yaml.as_bytes(),
                &d.tenant,
                &d.module,
                &d.version,
                "channels.yaml",
            )
            .map_err(|diag| format!("[{}] {}", diag.code, diag.message))?,
            None => Vec::new(),
        };
        let stores = match &d.datastores_yaml {
            Some(yaml) => sutra_datastore::parse_datastores(yaml).map_err(|e| e.to_string())?,
            None => Vec::new(),
        };
        Ok(DeploymentFacts::of(d, &definitions, &stores, view))
    }

    pub fn in_slot(mut self, slot: &str) -> DeploymentFacts {
        self.slot = slot.to_string();
        self
    }
}

/// The `db` source's slot for a package: `tenant--module--version`, which new content replaces in
/// place. A package with no tenant or module label owns only itself, so it gets a slot of its own,
/// or the next unlabeled package would silently replace it.
pub fn deployment_slot(tenant: &str, module: &str, version: &str, deployment_id: &str) -> String {
    let slot = format!("{tenant}--{module}--{version}");
    if tenant == UNLABELED_PACKAGE_LABEL || module == UNLABELED_PACKAGE_LABEL {
        return format!("{slot}--{deployment_id}");
    }
    slot
}

/// Order two `version` labels: numeric segments as numbers, so `1.10.0` is above `1.9.0`; a missing
/// segment is zero; a number ranks above text. Anything after a hyphen is a pre-release, below its
/// release, compared as semver has it: so `1.0.0-1` is below `1.0.0`, a number ranks below text there,
/// and more identifiers rank above fewer. Build metadata (`+…`) and a leading `v` mean nothing. Labels
/// are free text, so this is total on any input rather than a parse that can fail.
pub fn compare_deployment_versions(a: &str, b: &str) -> Ordering {
    fn split(version: &str) -> (Vec<&str>, Option<Vec<&str>>) {
        let version = version.trim();
        let version = version
            .strip_prefix(['v', 'V'])
            .filter(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
            .unwrap_or(version);
        let version = version.split('+').next().unwrap_or(version);
        match version.split_once('-') {
            Some((core, pre)) => (core.split('.').collect(), Some(pre.split('.').collect())),
            None => (version.split('.').collect(), None),
        }
    }
    fn segment(a: &str, b: &str) -> Ordering {
        let number = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
        match (number(a), number(b)) {
            (true, true) => {
                let (a, b) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
                a.len().cmp(&b.len()).then_with(|| a.cmp(b))
            }
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => a.cmp(b),
        }
    }
    fn core_segments(a: &[&str], b: &[&str]) -> Ordering {
        for i in 0..a.len().max(b.len()) {
            let found = segment(
                a.get(i).copied().unwrap_or("0"),
                b.get(i).copied().unwrap_or("0"),
            );
            if found != Ordering::Equal {
                return found;
            }
        }
        Ordering::Equal
    }
    fn pre_release_segments(a: &[&str], b: &[&str]) -> Ordering {
        for (a, b) in a.iter().zip(b) {
            let found = segment(a, b);
            let number = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
            let found = if number(a) != number(b) {
                found.reverse()
            } else {
                found
            };
            if found != Ordering::Equal {
                return found;
            }
        }
        a.len().cmp(&b.len())
    }
    let ((core_a, pre_a), (core_b, pre_b)) = (split(a), split(b));
    core_segments(&core_a, &core_b).then_with(|| match (pre_a, pre_b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => pre_release_segments(&a, &b),
    })
}

/// One endpoint, as `(transport, identity)`.
pub type EndpointKey = (String, String);

/// Who holds an exclusive endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointHolding {
    pub owner: String,
    pub deployment_id: String,
    pub label: String,
    /// The other deployments on an endpoint that can have several, by id: the subscribers of a
    /// topic, or deployments an earlier engine let share what now belongs to one owner.
    pub also_held_by: BTreeSet<String>,
}

impl EndpointHolding {
    /// Who a deployment's facts say holds an endpoint: that deployment, alone.
    pub fn by(facts: &DeploymentFacts) -> EndpointHolding {
        EndpointHolding {
            owner: facts.owner.clone(),
            deployment_id: facts.deployment_id.clone(),
            label: facts.label.clone(),
            also_held_by: BTreeSet::new(),
        }
    }

    /// Does this name the deployment `id`?
    pub fn names(&self, id: &str) -> bool {
        self.deployment_id == id || self.also_held_by.contains(id)
    }
}

/// Why a folder archive cannot go in the holder record: a value longer than its column, or a NUL,
/// which the database refuses in text. Without this one archive's label would fail the write for the
/// whole folder, and a booting pod with it.
pub fn holder_record_refusal(facts: &DeploymentFacts) -> Option<String> {
    let unfit = |what: &str, value: &str, max: usize| {
        let too_long = value.chars().count() > max;
        (too_long || value.contains('\0')).then(|| {
            let shown: String = value.chars().take(64).filter(|c| *c != '\0').collect();
            format!(
                "[{}] {what} '{shown}' cannot be recorded: {}",
                sutra_channels::codes::DEPLOY_ENDPOINT_UNRECORDABLE,
                if too_long {
                    format!("it is longer than {max} characters")
                } else {
                    "it holds a NUL character".to_string()
                }
            )
        })
    };
    unfit("the label", &facts.label, ENDPOINT_HOLDER_LABEL_MAX)
        .or_else(|| unfit("the owner", &facts.owner, ENDPOINT_HOLDER_OWNER_MAX))
        .or_else(|| {
            facts.endpoints.iter().find_map(|endpoint| {
                unfit(
                    "the transport",
                    &endpoint.transport,
                    ENDPOINT_HOLDER_TRANSPORT_MAX,
                )
                .or_else(|| {
                    unfit(
                        "the endpoint",
                        &endpoint.claim.identity,
                        ENDPOINT_HOLDER_IDENTITY_MAX,
                    )
                })
            })
        })
}

/// What should be said about `active` deployments sharing an endpoint that now belongs to one owner,
/// one message per endpoint, naming who shares it and how to separate them.
pub fn already_shared_endpoints(active: &[DeploymentFacts]) -> Vec<String> {
    let mut shared: BTreeMap<EndpointKey, (&'static str, BTreeSet<&str>, BTreeSet<&str>)> =
        BTreeMap::new();
    for facts in active {
        for endpoint in &facts.endpoints {
            if let EndpointSharing::ExclusiveToNewDeployments { remedy } = endpoint.claim.sharing {
                let (_, owners, labels) = shared
                    .entry((endpoint.transport.clone(), endpoint.claim.identity.clone()))
                    .or_insert((remedy, BTreeSet::new(), BTreeSet::new()));
                owners.insert(&facts.owner);
                labels.insert(&facts.label);
            }
        }
    }
    shared
        .into_iter()
        .filter(|(_, (_, owners, _))| owners.len() > 1)
        .map(|((transport, identity), (remedy, _, labels))| {
            format!(
                "[{}] {transport} endpoint '{identity}' is shared by deployments {}. It belongs to \
                 one owner: these keep running, and a new deployment claiming it is refused. {remedy}",
                sutra_channels::codes::DEPLOY_ENDPOINT_ALREADY_SHARED,
                labels.into_iter().collect::<Vec<_>>().join(", "),
            )
        })
        .collect()
}

/// What became of one deployment when a whole set was settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettledClaim {
    /// Serves new work.
    Active,
    /// A higher version of its owner serves an endpoint of its. Takes no new work; its running
    /// instances stay on it. `by` names the higher versions.
    Superseded { by: Vec<String> },
    /// Another owner holds an endpoint it claims, or nobody can say who does. Never deployed.
    Refused { reason: String },
}

/// Decide a whole set of deployments at once: what a folder's archives come to at any scan or
/// restart, and what rows that already share endpoints come to. The answer depends only on the set
/// and on `holders`, so restarts and replicas that hold the same inputs agree, and the order
/// deployments arrived in does not matter.
///
/// - Of two versions of one owner sharing any endpoint, the lower is superseded. A topic counts, or
///   the old version would go on processing every event beside the new one.
/// - A version that is refused supersedes nothing, so the version below it keeps serving.
/// - On an endpoint several owners want, its holder keeps it and the others are refused. With no
///   holder among them, all of them are refused: no order among them is meaningful, and a guess could
///   differ between replicas.
/// - A refusal is judged whole-deployment and holder refusals are settled first, because a deployment
///   refused for one endpoint does not contest any other.
/// - Before any of that, a deployment of another owner claiming an endpoint a draining version keeps
///   (`holds`) is refused, naming that version, unless the record says its owner holds the endpoint
///   already. A hold never refuses its own owner.
pub fn settle_claims(
    candidates: &[DeploymentFacts],
    holders: &BTreeMap<EndpointKey, EndpointHolding>,
    holds: &[DrainingHold],
) -> Vec<SettledClaim> {
    let shares_endpoint = |a: &DeploymentFacts, b: &DeploymentFacts| {
        a.endpoints.iter().any(|e| {
            b.endpoints
                .iter()
                .any(|o| o.transport == e.transport && o.claim.identity == e.claim.identity)
        })
    };
    let mut refused: BTreeMap<usize, String> = BTreeMap::new();
    for (i, candidate) in candidates.iter().enumerate() {
        let already_held = |key: &EndpointKey| {
            holders
                .get(key)
                .is_some_and(|holding| holding.owner == candidate.owner)
        };
        if let Some(collision) = retained_collisions(candidate, holds)
            .into_iter()
            .find(|c| !already_held(&(c.transport.clone(), c.identity.clone())))
        {
            refused.insert(i, collision.describe());
        }
    }
    loop {
        let superseded_by = |i: usize| -> Vec<String> {
            candidates
                .iter()
                .enumerate()
                .filter(|(j, other)| {
                    *j != i
                        && !refused.contains_key(j)
                        && other.owner == candidates[i].owner
                        && compare_deployment_versions(&other.version, &candidates[i].version)
                            == Ordering::Greater
                        && shares_endpoint(other, &candidates[i])
                })
                .map(|(_, other)| other.label.clone())
                .collect()
        };
        let live: Vec<usize> = (0..candidates.len())
            .filter(|i| !refused.contains_key(i) && superseded_by(*i).is_empty())
            .collect();
        let mut contested: BTreeMap<EndpointKey, Vec<usize>> = BTreeMap::new();
        for i in &live {
            for endpoint in &candidates[*i].endpoints {
                let claimants = contested
                    .entry((endpoint.transport.clone(), endpoint.claim.identity.clone()))
                    .or_default();
                if !claimants.contains(i) {
                    claimants.push(*i);
                }
            }
        }
        for (key, claimants) in contested.iter_mut() {
            narrow_to_contest(key, claimants, candidates);
        }
        contested.retain(|_, claimants| !claimants.is_empty());

        let mut by_holder: BTreeMap<usize, String> = BTreeMap::new();
        let mut unheld: BTreeMap<usize, String> = BTreeMap::new();
        for (key, claimants) in &contested {
            match holders_among(holders.get(key), claimants, candidates) {
                Some(kept) => {
                    let holder = holders.get(key).map(|h| h.label.as_str()).unwrap_or("");
                    for i in claimants.iter().filter(|i| !kept.contains(i)) {
                        by_holder.entry(*i).or_insert_with(|| {
                            format!(
                                "[{}] {} endpoint '{}' is held by deployment {holder}, and \
                                 deployment {} ({}) claims it as well; it can belong to only one \
                                 deployment",
                                sutra_channels::codes::DEPLOY_ENDPOINT_COLLISION,
                                key.0,
                                key.1,
                                candidates[*i].label,
                                candidates[*i].slot,
                            )
                        });
                    }
                }
                None => {
                    let named: Vec<String> = claimants
                        .iter()
                        .map(|i| format!("{} ({})", candidates[*i].label, candidates[*i].slot))
                        .collect();
                    for i in claimants {
                        unheld.entry(*i).or_insert_with(|| {
                            format!(
                                "[{}] {} endpoint '{}' is claimed by {}, and nothing records which \
                                 of them holds it, so none of them deploys. Remove all but one",
                                sutra_channels::codes::DEPLOY_ENDPOINT_COLLISION,
                                key.0,
                                key.1,
                                named.join(" and "),
                            )
                        });
                    }
                }
            }
        }
        if !by_holder.is_empty() {
            refused.extend(by_holder);
            continue;
        }
        if !unheld.is_empty() {
            refused.extend(unheld);
            continue;
        }
        return (0..candidates.len())
            .map(|i| match refused.get(&i) {
                Some(reason) => SettledClaim::Refused {
                    reason: reason.clone(),
                },
                None => match superseded_by(i) {
                    by if by.is_empty() => SettledClaim::Active,
                    by => SettledClaim::Superseded { by },
                },
            })
            .collect();
    }
}

/// Reduce `claimants` to those who cannot all stay, or to nothing. On an endpoint only one owner may
/// have, that is all of them. On a topic other owners are welcome, so it is only two different
/// deployments of one version of one owner: nothing ranks them, and both would process every event.
fn narrow_to_contest(
    key: &EndpointKey,
    claimants: &mut Vec<usize>,
    candidates: &[DeploymentFacts],
) {
    if claimants.len() < 2 {
        claimants.clear();
        return;
    }
    let one_owner_only = claimants.iter().any(|i| {
        candidates[*i].endpoints.iter().any(|e| {
            e.transport == key.0
                && e.claim.identity == key.1
                && e.claim.sharing != EndpointSharing::Broadcast
        })
    });
    if one_owner_only {
        return;
    }
    let of_owner = |owner: &str| {
        claimants
            .iter()
            .filter(|i| candidates[**i].owner == owner)
            .count()
    };
    let contesting: Vec<usize> = claimants
        .iter()
        .copied()
        .filter(|i| of_owner(&candidates[*i].owner) > 1)
        .collect();
    *claimants = contesting;
}

/// The claimants the record lets keep a contested endpoint, or `None` when it cannot say.
fn holders_among(
    holder: Option<&EndpointHolding>,
    claimants: &[usize],
    candidates: &[DeploymentFacts],
) -> Option<Vec<usize>> {
    let holder = holder?;
    let exact: Vec<usize> = claimants
        .iter()
        .copied()
        .filter(|i| holder.names(&candidates[*i].deployment_id))
        .collect();
    if !exact.is_empty() {
        return Some(exact);
    }
    let of_owner: Vec<usize> = claimants
        .iter()
        .copied()
        .filter(|i| candidates[*i].owner == holder.owner)
        .collect();
    (of_owner.len() == 1).then_some(of_owner)
}

/// A finding that never blocks a deploy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeployWarning {
    pub code: String,
    pub message: String,
}

impl DeployWarning {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "code": self.code, "message": self.message })
    }
}

/// What the check found for a candidate against the other active deployments.
pub struct CandidateVerdict {
    /// Non-empty refuses the deploy.
    pub collisions: Vec<EndpointCollision>,
    pub warnings: Vec<DeployWarning>,
    /// Other versions of the candidate's tenant and module it takes an endpoint from. Activating
    /// the candidate drains each of these whole.
    pub superseded: Vec<SupersededDeployment>,
    /// Higher versions of the candidate's owner on an endpoint it claims. An explicit deploy may
    /// still roll back to it; a folder will not serve it while they are there.
    pub outranked_by: Vec<String>,
    /// Endpoints the candidate claims that a draining version of another owner keeps until it
    /// retires. Non-empty refuses the deploy.
    pub retained_collisions: Vec<RetainedCollision>,
}

impl CandidateVerdict {
    /// Nothing refuses the candidate.
    pub fn is_ok(&self) -> bool {
        self.collisions.is_empty() && self.retained_collisions.is_empty()
    }

    /// The refusal, one line per endpoint, or `None` when nothing refuses it.
    pub fn refusal(&self) -> Option<String> {
        if !self.collisions.is_empty() {
            return Some(sutra_channels::describe_endpoint_refusal(&self.collisions));
        }
        if self.retained_collisions.is_empty() {
            return None;
        }
        Some(
            self.retained_collisions
                .iter()
                .map(RetainedCollision::describe)
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

/// A draining version and the endpoints it keeps serving until it retires: the endpoints of the
/// channels no newer version of its owner declares.
#[derive(Clone)]
pub struct DrainingHold {
    /// The draining deployment, its endpoints reduced to the ones it keeps.
    pub facts: DeploymentFacts,
    /// Its running instances, when they could be counted.
    pub instances: Option<u64>,
}

/// An endpoint a candidate claims that a draining version of another owner keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedCollision {
    pub transport: String,
    pub identity: String,
    pub holder_label: String,
    pub holder_deployment_id: String,
    pub instances: Option<u64>,
}

impl RetainedCollision {
    pub fn describe(&self) -> String {
        let count = match self.instances {
            Some(1) => "1 running instance".to_string(),
            Some(n) => format!("{n} running instances"),
            None => "instance count unknown".to_string(),
        };
        format!(
            "[{}] {} endpoint '{}' stays with draining deployment {} ({count}) until it retires",
            sutra_channels::codes::DEPLOY_ENDPOINT_COLLISION,
            self.transport,
            self.identity,
            self.holder_label,
        )
    }
}

/// The endpoints `candidate` claims that a draining version of another owner keeps. On a topic
/// both may be, unless either claims it alone.
fn retained_collisions(
    candidate: &DeploymentFacts,
    holds: &[DrainingHold],
) -> Vec<RetainedCollision> {
    let mut found: Vec<RetainedCollision> = Vec::new();
    for hold in holds.iter().filter(|h| h.facts.owner != candidate.owner) {
        for held in &hold.facts.endpoints {
            let clashes = candidate.endpoints.iter().any(|claim| {
                claim.transport == held.transport
                    && claim.claim.identity == held.claim.identity
                    && (claim.claim.sharing != EndpointSharing::Broadcast
                        || held.claim.sharing != EndpointSharing::Broadcast)
            });
            if clashes
                && !found
                    .iter()
                    .any(|f| f.transport == held.transport && f.identity == held.claim.identity)
            {
                found.push(RetainedCollision {
                    transport: held.transport.clone(),
                    identity: held.claim.identity.clone(),
                    holder_label: hold.facts.label.clone(),
                    holder_deployment_id: hold.facts.deployment_id.clone(),
                    instances: hold.instances,
                });
            }
        }
    }
    found
}

impl CandidateVerdict {
    /// Add the warnings a folder target owes this verdict: a folder serves the highest version, so
    /// the candidate would not serve while a higher one is there.
    pub fn warn_for_folder_target(&mut self, candidate: &DeploymentFacts) {
        if self.outranked_by.is_empty() {
            return;
        }
        self.warnings.push(DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_OUTRANKED.to_string(),
            message: format!(
                "deployment {} is an older version than {}, on an endpoint they share. A folder \
                 serves the highest version, so {} would not serve until that version is removed",
                candidate.label,
                self.outranked_by.join(", "),
                candidate.label
            ),
        });
    }
}

/// A deployment a candidate takes an endpoint from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupersededDeployment {
    pub deployment_id: String,
    pub slot: String,
}

/// The other versions of `candidate`'s owner that hold an endpoint it claims, each with a warning
/// naming the endpoints it goes on serving: those of channels the candidate does not declare stay
/// with it until it retires, so the operator has to be told. A topic counts as well: a version
/// left beside its successor would process every event a second time.
fn find_superseded(
    candidate: &DeploymentFacts,
    others: &[DeploymentFacts],
) -> (Vec<SupersededDeployment>, Vec<DeployWarning>) {
    let claims = |facts: &DeploymentFacts, endpoint: &DeclaredEndpoint| {
        facts.endpoints.iter().any(|e| {
            e.transport == endpoint.transport && e.claim.identity == endpoint.claim.identity
        })
    };
    let mut superseded = Vec::new();
    let mut warnings = Vec::new();
    for other in others.iter().filter(|other| {
        other.owner == candidate.owner && other.deployment_id != candidate.deployment_id
    }) {
        if !other
            .endpoints
            .iter()
            .any(|endpoint| claims(candidate, endpoint))
        {
            continue;
        }
        let lost: Vec<&str> = other
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.transport != CHANNEL_NAME_ENDPOINT)
            .filter(|endpoint| !claims(candidate, endpoint))
            .map(|endpoint| endpoint.claim.identity.as_str())
            .collect();
        let lost = if lost.is_empty() {
            String::new()
        } else {
            format!(
                " Channels only it declares stay served by it until it retires: {}.",
                lost.join(", ")
            )
        };
        let timers = if other.timer_starts.is_empty() {
            String::new()
        } else {
            format!(" Its timer starts stop: {}.", other.timer_starts.join(", "))
        };
        warnings.push(DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_SUPERSEDED.to_string(),
            message: format!(
                "deployment {} takes over from {}, an earlier deployment of the same tenant and \
                 module. {} drains: it takes no new work, and its running instances finish on it.{lost}{timers}",
                candidate.label, other.label, other.label
            ),
        });
        superseded.push(SupersededDeployment {
            deployment_id: other.deployment_id.clone(),
            slot: other.slot.clone(),
        });
    }
    (superseded, warnings)
}

/// Judge a candidate. Every deploy path runs this before anything changes. `others` must not hold
/// the candidate's own slot: replacing a slot is not a collision. The candidate goes last, so a
/// refusal names it as the claimant.
pub fn evaluate_candidate(
    candidate: &DeploymentFacts,
    others: &[DeploymentFacts],
) -> CandidateVerdict {
    evaluate_candidate_with_holds(candidate, others, &[])
}

/// [`evaluate_candidate`], also refusing an endpoint a draining version of another owner keeps
/// (`holds`). A hold never refuses its own owner: a new version takes over from the old one.
pub fn evaluate_candidate_with_holds(
    candidate: &DeploymentFacts,
    others: &[DeploymentFacts],
    holds: &[DrainingHold],
) -> CandidateVerdict {
    let endpoints: Vec<DeclaredEndpoint> = others
        .iter()
        .chain(std::iter::once(candidate))
        .flat_map(|facts| facts.endpoints.iter().cloned())
        .collect();
    let mut warnings: Vec<DeployWarning> = candidate
        .unchecked
        .iter()
        .map(|(channel, transport)| DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_UNCHECKED.to_string(),
            message: format!(
                "channel '{channel}' of deployment {} uses transport '{transport}', which declares \
                 no endpoint claims here, so its endpoint was not checked against other \
                 deployments",
                candidate.label
            ),
        })
        .collect();
    for (channel, path, transport) in &candidate.shadowed {
        warnings.push(DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_SHADOWED.to_string(),
            message: format!(
                "channel '{channel}' of deployment {} binds '{path}', under a path the {transport} \
                 transport serves on the engine's listener, so it would never receive anything",
                candidate.label
            ),
        });
    }
    let (superseded, superseded_warnings) = find_superseded(candidate, others);
    warnings.extend(superseded_warnings);
    let outranked_by: Vec<String> = others
        .iter()
        .filter(|other| {
            other.owner == candidate.owner
                && compare_deployment_versions(&other.version, &candidate.version)
                    == Ordering::Greater
                && other.endpoints.iter().any(|e| {
                    candidate
                        .endpoints
                        .iter()
                        .any(|c| c.transport == e.transport && c.claim.identity == e.claim.identity)
                })
        })
        .map(|other| other.label.clone())
        .collect();
    // Versions of one owner sharing their own stores is what an upgrade looks like, not a
    // forgotten override.
    let other_stores: Vec<(String, Vec<StoreConnectionUse>)> = others
        .iter()
        .filter(|facts| facts.owner != candidate.owner)
        .map(|facts| (facts.label.clone(), facts.stores.clone()))
        .collect();
    for shared in sutra_datastore::shared_store_connections(&candidate.stores, &other_stores) {
        warnings.push(DeployWarning {
            code: sutra_channels::codes::DEPLOY_DATASTORE_SHARED_CONNECTION.to_string(),
            message: shared.describe(&candidate.label),
        });
    }
    // Only the candidate's own collisions: two rows that already collide are not its doing.
    let collisions = find_endpoint_collisions(&endpoints)
        .into_iter()
        .filter(|collision| collision.claimant_label == candidate.label)
        .collect();
    CandidateVerdict {
        collisions,
        warnings,
        superseded,
        outranked_by,
        retained_collisions: retained_collisions(candidate, holds),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::EndpointClaim;

    fn facts(id: &str, identity: &str, stores: &[(&str, &str)]) -> DeploymentFacts {
        DeploymentFacts {
            deployment_id: id.to_string(),
            label: format!("label-{id}"),
            owner: format!("owner-{id}"),
            version: "1.0.0".to_string(),
            slot: format!("slot-{id}"),
            shadowed: Vec::new(),
            endpoints: vec![DeclaredEndpoint {
                deployment_id: id.to_string(),
                deployment_label: format!("label-{id}"),
                owner: format!("owner-{id}"),
                channel: "in".to_string(),
                transport: "t".to_string(),
                claim: EndpointClaim::exclusive(identity),
            }],
            unchecked: Vec::new(),
            stores: stores
                .iter()
                .map(|(store, connection)| StoreConnectionUse {
                    store: store.to_string(),
                    connection: connection.to_string(),
                })
                .collect(),
            ..DeploymentFacts::default()
        }
    }

    /// `facts` for version `id` of one owner, holding each endpoint in `identities`.
    fn version(id: &str, identities: &[&str]) -> DeploymentFacts {
        let mut version = facts(id, "unused", &[]);
        version.owner = "t--m".to_string();
        version.endpoints = identities
            .iter()
            .map(|identity| DeclaredEndpoint {
                deployment_id: id.to_string(),
                deployment_label: format!("label-{id}"),
                owner: "t--m".to_string(),
                channel: "in".to_string(),
                transport: "t".to_string(),
                claim: EndpointClaim::exclusive(*identity),
            })
            .collect();
        version
    }

    #[test]
    fn a_new_version_supersedes_the_old_one_and_names_what_it_stops_serving() {
        let verdict = evaluate_candidate(&version("v2", &["a"]), &[version("v1", &["a", "b"])]);
        assert!(verdict.collisions.is_empty());
        assert_eq!(verdict.superseded[0].deployment_id, "v1");
        assert_eq!(verdict.superseded[0].slot, "slot-v1");
        assert_eq!(verdict.warnings[0].code, "SUTRA.DEPLOY.ENDPOINT.SUPERSEDED");
        assert!(verdict.warnings[0]
            .message
            .contains("Channels only it declares stay served by it until it retires: b"));
    }

    #[test]
    fn a_new_version_on_a_topic_supersedes_the_old_one() {
        let shared = |id: &str| {
            let mut facts = version(id, &[]);
            facts.endpoints = vec![DeclaredEndpoint {
                deployment_id: id.to_string(),
                deployment_label: format!("label-{id}"),
                owner: "t--m".to_string(),
                channel: "in".to_string(),
                transport: "t".to_string(),
                claim: EndpointClaim::broadcast("topic"),
            }];
            facts
        };
        let verdict = evaluate_candidate(&shared("v2"), &[shared("v1")]);
        assert_eq!(verdict.superseded[0].deployment_id, "v1");
        assert!(verdict.collisions.is_empty());
    }

    #[test]
    fn an_older_version_is_flagged_only_for_a_folder_target() {
        let mut older = version("v1", &["a"]);
        older.version = "1.9.0".to_string();
        let mut newer = version("v2", &["a"]);
        newer.version = "1.10.0".to_string();
        let mut verdict = evaluate_candidate(&older, std::slice::from_ref(&newer));
        assert_eq!(verdict.outranked_by, vec!["label-v2".to_string()]);
        assert_eq!(verdict.warnings.len(), 1);
        verdict.warn_for_folder_target(&older);
        assert_eq!(verdict.warnings[1].code, "SUTRA.DEPLOY.ENDPOINT.OUTRANKED");
        assert!(evaluate_candidate(&newer, &[older]).outranked_by.is_empty());
    }

    #[test]
    fn a_collision_elsewhere_is_not_the_candidates() {
        let verdict = evaluate_candidate(
            &facts("unrelated", "free", &[]),
            &[facts("a", "shared", &[]), facts("b", "shared", &[])],
        );
        assert!(verdict.collisions.is_empty());
    }

    #[test]
    fn an_endpoint_already_shared_is_reported_with_who_and_how_to_separate_them() {
        let on_group = |id: &str, owner: &str| {
            let mut found = facts(id, "unused", &[]);
            found.owner = owner.to_string();
            found.endpoints[0].claim = sutra_channels::EndpointClaim::exclusive_to_new_deployments(
                "b1 group sutra-in",
                "Set a group of its own.",
            );
            found
        };
        let messages = already_shared_endpoints(&[on_group("a", "one"), on_group("b", "two")]);
        assert_eq!(messages.len(), 1);
        for part in [
            "SUTRA.DEPLOY.ENDPOINT.ALREADY_SHARED",
            "b1 group sutra-in",
            "label-a, label-b",
            "Set a group of its own.",
        ] {
            assert!(messages[0].contains(part), "{part}: {}", messages[0]);
        }
        assert!(already_shared_endpoints(&[on_group("a", "one"), on_group("b", "one")]).is_empty());
    }

    #[test]
    fn what_the_holder_record_cannot_store_is_named() {
        assert_eq!(holder_record_refusal(&facts("a", "fine", &[])), None);
        let long = holder_record_refusal(&facts("a", &"x".repeat(2049), &[])).unwrap();
        assert!(
            long.contains("SUTRA.DEPLOY.ENDPOINT.UNRECORDABLE"),
            "{long}"
        );
        assert!(long.contains("2048"), "{long}");
        let nul = holder_record_refusal(&facts("a", "a\0b", &[])).unwrap();
        assert!(nul.contains("NUL"), "{nul}");
    }

    #[test]
    fn versions_compare_as_versions() {
        use Ordering::*;
        for (a, b, expected) in [
            ("1.10.0", "1.9.0", Greater),
            ("1.9.0", "1.10.0", Less),
            ("2.0.0", "2", Equal),
            ("v1.2.0", "1.2.0", Equal),
            ("1.0.0", "1.0.0-rc.1", Greater),
            ("1.0.0-rc.2", "1.0.0-rc.10", Less),
            // As semver has it: anything after a hyphen is a pre-release of that version, a numeric
            // identifier ranks below a textual one, and more identifiers rank above fewer.
            ("1.0.0-1", "1.0.0", Less),
            ("1.0.0-1", "1.0.0-rc", Less),
            ("1.0.0-2", "1.0.0-10", Less),
            ("1.0.0-rc", "1.0.0-rc.1", Less),
            ("1.0.0-RC", "1.0.0-rc", Less),
            ("1.0.0-alpha.1", "1.0.0-alpha.beta", Less),
            ("1.0.0+a", "1.0.0+b", Equal),
            ("1.0.0", "unlabeled", Greater),
            ("007", "7", Equal),
            (
                "100000000000000000000000000000000000000000.0",
                "1.0",
                Greater,
            ),
        ] {
            assert_eq!(compare_deployment_versions(a, b), expected, "{a} vs {b}");
        }
    }

    #[test]
    fn an_unlabeled_package_gets_a_slot_of_its_own() {
        assert_eq!(deployment_slot("t", "m", "1", "d"), "t--m--1");
        assert_ne!(
            deployment_slot("unlabeled", "unlabeled", "1", "d1"),
            deployment_slot("unlabeled", "unlabeled", "1", "d2")
        );
    }

    fn holding(owner: &str, id: &str) -> EndpointHolding {
        EndpointHolding {
            owner: owner.to_string(),
            deployment_id: id.to_string(),
            label: format!("label-{id}"),
            also_held_by: BTreeSet::new(),
        }
    }

    fn held(rows: &[(&str, &str, &str)]) -> BTreeMap<EndpointKey, EndpointHolding> {
        rows.iter()
            .map(|(identity, owner, id)| {
                (("t".to_string(), identity.to_string()), holding(owner, id))
            })
            .collect()
    }

    #[test]
    fn the_recorded_holder_keeps_its_endpoint_whatever_the_order() {
        let (a, z) = (facts("a", "e", &[]), facts("z", "e", &[]));
        let holders = held(&[("e", "owner-z", "z")]);
        let found = settle_claims(&[a.clone(), z.clone()], &holders, &[]);
        assert!(
            matches!(found[0], SettledClaim::Refused { .. }),
            "{found:?}"
        );
        assert_eq!(found[1], SettledClaim::Active);
        let found = settle_claims(&[z, a], &holders, &[]);
        assert_eq!(found[0], SettledClaim::Active);
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
    }

    #[test]
    fn with_no_record_neither_claimant_deploys_and_the_rest_do() {
        let found = settle_claims(
            &[
                facts("a", "e", &[]),
                facts("b", "e", &[]),
                facts("c", "free", &[]),
            ],
            &BTreeMap::new(),
            &[],
        );
        for refused in &found[..2] {
            let SettledClaim::Refused { reason } = refused else {
                panic!("{found:?}");
            };
            assert!(
                reason.contains("slot-a") && reason.contains("slot-b"),
                "{reason}"
            );
            assert!(
                reason.starts_with("[SUTRA.DEPLOY.ENDPOINT.COLLISION]"),
                "{reason}"
            );
        }
        assert_eq!(found[2], SettledClaim::Active);
    }

    #[test]
    fn a_stale_record_of_an_owner_with_no_claim_decides_nothing() {
        let found = settle_claims(
            &[facts("a", "e", &[]), facts("b", "e", &[])],
            &held(&[("e", "owner-gone", "gone")]),
            &[],
        );
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        let alone = settle_claims(
            &[facts("a", "e", &[])],
            &held(&[("e", "owner-gone", "gone")]),
            &[],
        );
        assert_eq!(alone[0], SettledClaim::Active);
    }

    #[test]
    fn the_highest_version_wins_whatever_order_they_are_given_in() {
        let mut low = version("v1", &["a"]);
        low.version = "1.9.0".to_string();
        let mut high = version("v2", &["a"]);
        high.version = "1.10.0".to_string();
        for order in [[low.clone(), high.clone()], [high.clone(), low.clone()]] {
            let found = settle_claims(&order, &BTreeMap::new(), &[]);
            for (facts, outcome) in order.iter().zip(&found) {
                if facts.deployment_id == "v2" {
                    assert_eq!(*outcome, SettledClaim::Active);
                } else {
                    assert!(matches!(outcome, SettledClaim::Superseded { .. }));
                }
            }
        }
    }

    #[test]
    fn a_refused_version_supersedes_nothing() {
        let mut low = version("v1", &["a"]);
        low.version = "1.0.0".to_string();
        let mut high = version("v2", &["a", "taken"]);
        high.version = "2.0.0".to_string();
        let other = facts("x", "taken", &[]);
        let found = settle_claims(
            &[low, high, other],
            &held(&[("taken", "owner-x", "x"), ("a", "t--m", "v1")]),
            &[],
        );
        assert_eq!(found[0], SettledClaim::Active, "{found:?}");
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        assert_eq!(found[2], SettledClaim::Active);
    }

    #[test]
    fn a_lower_version_is_superseded_through_a_version_above_that_left_its_endpoint_to_nobody() {
        let mut v1 = version("v1", &["b"]);
        v1.version = "1.0.0".to_string();
        let mut v2 = version("v2", &["a", "b"]);
        v2.version = "2.0.0".to_string();
        let mut v3 = version("v3", &["a"]);
        v3.version = "3.0.0".to_string();
        let found = settle_claims(&[v1, v2, v3], &BTreeMap::new(), &[]);
        assert!(matches!(found[0], SettledClaim::Superseded { .. }));
        assert!(matches!(found[1], SettledClaim::Superseded { .. }));
        assert_eq!(found[2], SettledClaim::Active);
    }

    #[test]
    fn a_refused_claimant_does_not_contest_its_other_endpoints() {
        let a = facts("a", "e1", &[]);
        let mut b = facts("b", "e1", &[]);
        b.endpoints.extend(facts("b", "e2", &[]).endpoints);
        let c = facts("c", "e2", &[]);
        let found = settle_claims(&[a, b, c], &held(&[("e1", "owner-a", "a")]), &[]);
        assert_eq!(found[0], SettledClaim::Active);
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        assert_eq!(found[2], SettledClaim::Active, "{found:?}");
    }

    #[test]
    fn two_deployments_swapping_endpoints_both_land() {
        let (a, b) = (facts("a", "e2", &[]), facts("b", "e1", &[]));
        let found = settle_claims(
            &[a, b],
            &held(&[("e1", "owner-a", "old-a"), ("e2", "owner-b", "old-b")]),
            &[],
        );
        assert_eq!(found, vec![SettledClaim::Active, SettledClaim::Active]);
    }

    #[test]
    fn two_contents_of_one_version_are_ranked_only_by_the_record() {
        let (one, two) = (version("c1", &["a"]), version("c2", &["a"]));
        let found = settle_claims(&[one.clone(), two.clone()], &BTreeMap::new(), &[]);
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        let found = settle_claims(&[one, two], &held(&[("a", "t--m", "c2")]), &[]);
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert_eq!(found[1], SettledClaim::Active);
    }

    #[test]
    fn versions_on_separate_endpoints_both_stay() {
        let verdict = evaluate_candidate(&version("v2", &["a"]), &[version("v1", &["b"])]);
        assert!(verdict.superseded.is_empty());
        assert!(verdict.warnings.is_empty());
    }

    #[test]
    fn versions_of_one_owner_do_not_warn_about_their_own_stores() {
        let mut older = version("v1", &["b"]);
        let mut newer = version("v2", &["a"]);
        let shared = vec![StoreConnectionUse {
            store: "accounts".to_string(),
            connection: "c".to_string(),
        }];
        older.stores = shared.clone();
        newer.stores = shared;
        assert!(evaluate_candidate(&newer, &[older]).warnings.is_empty());
    }

    #[test]
    fn the_candidate_is_named_as_the_claimant() {
        let verdict = evaluate_candidate(&facts("new", "e", &[]), &[facts("old", "e", &[])]);
        assert_eq!(verdict.collisions[0].holder_label, "label-old");
        assert_eq!(verdict.collisions[0].claimant_label, "label-new");
    }

    #[test]
    fn a_shared_store_only_warns() {
        let verdict = evaluate_candidate(
            &facts("new", "e1", &[("accounts", "c")]),
            &[facts("old", "e2", &[("accounts", "c")])],
        );
        assert!(verdict.collisions.is_empty());
        assert_eq!(
            verdict.warnings[0].code,
            "SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION"
        );
        assert!(verdict.warnings[0].message.contains("label-old"));
        assert!(verdict.warnings[0].message.contains("accounts"));
    }

    #[test]
    fn a_channel_no_transport_declares_is_reported_unchecked() {
        let mut candidate = facts("new", "e", &[]);
        candidate.unchecked = vec![("in".to_string(), "other".to_string())];
        let verdict = evaluate_candidate(&candidate, &[]);
        assert_eq!(verdict.warnings[0].code, "SUTRA.DEPLOY.ENDPOINT.UNCHECKED");
    }

    /// A draining version `id` of owner `t--m` keeping `identities`, with `instances`.
    fn hold(id: &str, identities: &[&str], instances: Option<u64>) -> DrainingHold {
        DrainingHold {
            facts: version(id, identities),
            instances,
        }
    }

    fn refusal(outcome: &SettledClaim) -> &str {
        match outcome {
            SettledClaim::Refused { reason } => reason,
            other => panic!("not refused: {other:?}"),
        }
    }

    #[test]
    fn settle_claims_holds_refuse_another_owner_naming_the_version_and_its_count() {
        let found = settle_claims(
            &[facts("x", "kept", &[])],
            &BTreeMap::new(),
            &[hold("v1", &["kept"], Some(3))],
        );
        let reason = refusal(&found[0]);
        for part in [
            "[SUTRA.DEPLOY.ENDPOINT.COLLISION]",
            "'kept'",
            "draining deployment label-v1",
            "3 running instances",
            "until it retires",
        ] {
            assert!(reason.contains(part), "{part}: {reason}");
        }
        let unknown = settle_claims(
            &[facts("x", "kept", &[])],
            &BTreeMap::new(),
            &[hold("v1", &["kept"], None)],
        );
        assert!(refusal(&unknown[0]).contains("instance count unknown"));
        let one = settle_claims(
            &[facts("x", "kept", &[])],
            &BTreeMap::new(),
            &[hold("v1", &["kept"], Some(1))],
        );
        assert!(refusal(&one[0]).contains("(1 running instance)"));
    }

    #[test]
    fn settle_claims_holds_never_refuse_their_own_owner() {
        // A newer version of the same owner takes the endpoint over.
        let found = settle_claims(
            &[version("v3", &["kept"])],
            &BTreeMap::new(),
            &[hold("v1", &["kept"], Some(2))],
        );
        assert_eq!(found[0], SettledClaim::Active);
    }

    #[test]
    fn settle_claims_holds_yield_to_a_record_naming_the_candidates_owner() {
        // Already held at a restart: the record decides, not the hold.
        let found = settle_claims(
            &[facts("x", "kept", &[])],
            &held(&[("kept", "owner-x", "x")]),
            &[hold("v1", &["kept"], Some(2))],
        );
        assert_eq!(found[0], SettledClaim::Active);
        // A record naming the draining version's owner refuses as before.
        let found = settle_claims(
            &[facts("x", "kept", &[])],
            &held(&[("kept", "t--m", "v1")]),
            &[hold("v1", &["kept"], Some(2))],
        );
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
    }

    #[test]
    fn settle_claims_holds_leave_other_endpoints_and_other_candidates_alone() {
        let found = settle_claims(
            &[facts("x", "kept", &[]), facts("y", "free", &[])],
            &BTreeMap::new(),
            &[hold("v1", &["kept"], Some(2))],
        );
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert_eq!(found[1], SettledClaim::Active);
    }

    #[test]
    fn settle_claims_holds_let_a_topic_be_shared_unless_either_claims_it_alone() {
        let broadcast = |mut f: DeploymentFacts| {
            for e in &mut f.endpoints {
                e.claim = EndpointClaim::broadcast(e.claim.identity.clone());
            }
            f
        };
        let topic_hold = DrainingHold {
            facts: broadcast(version("v1", &["topic"])),
            instances: Some(1),
        };
        let found = settle_claims(
            &[broadcast(facts("x", "topic", &[]))],
            &BTreeMap::new(),
            std::slice::from_ref(&topic_hold),
        );
        assert_eq!(found[0], SettledClaim::Active, "both subscribe");
        let found = settle_claims(&[facts("x", "topic", &[])], &BTreeMap::new(), &[topic_hold]);
        assert!(
            matches!(found[0], SettledClaim::Refused { .. }),
            "the candidate wants it alone"
        );
    }

    #[test]
    fn settle_claims_holds_refuse_before_contest_so_the_refused_supersedes_nothing() {
        // Another owner's v2 claims a held endpoint and shares one with its own v1: refused by the
        // hold, it supersedes nothing, so its v1 keeps serving.
        let mut low = facts("x1", "shared", &[]);
        low.owner = "owner-x".to_string();
        let mut high = facts("x2", "shared", &[]);
        high.owner = "owner-x".to_string();
        high.version = "2.0.0".to_string();
        high.endpoints.extend(facts("x2", "kept", &[]).endpoints);
        let found = settle_claims(
            &[low, high],
            &held(&[("shared", "owner-x", "x1")]),
            &[hold("v1", &["kept"], Some(1))],
        );
        assert_eq!(found[0], SettledClaim::Active, "{found:?}");
        assert!(refusal(&found[1]).contains("label-v1"));
    }

    #[test]
    fn evaluate_candidate_with_holds_reports_retained_collisions_and_refuses() {
        let verdict = evaluate_candidate_with_holds(
            &facts("x", "kept", &[]),
            &[],
            &[hold("v1", &["kept", "other"], Some(5))],
        );
        assert!(verdict.collisions.is_empty());
        assert!(!verdict.is_ok());
        assert_eq!(verdict.retained_collisions.len(), 1);
        assert_eq!(verdict.retained_collisions[0].holder_deployment_id, "v1");
        assert!(verdict.refusal().unwrap().contains("5 running instances"));
        // Without holds the same candidate deploys; its own owner's hold never refuses it.
        assert!(evaluate_candidate(&facts("x", "kept", &[]), &[]).is_ok());
        assert!(evaluate_candidate_with_holds(
            &version("v2", &["kept"]),
            &[],
            &[hold("v1", &["kept"], Some(5))]
        )
        .is_ok());
    }

    fn inbound(name: &str, address: &str, codec: &str, auth: &str) -> InboundChannelFact {
        InboundChannelFact {
            name: name.to_string(),
            transport: Some("http".to_string()),
            address: address.to_string(),
            codec: codec.to_string(),
            schema_digest: None,
            auth_view: auth.to_string(),
        }
    }

    fn subscription(
        kind: SubscriptionKind,
        node: &str,
        channel: &str,
        message_type: Option<&str>,
        pattern: Option<&str>,
    ) -> ChannelSubscriptionFact {
        ChannelSubscriptionFact {
            kind,
            process: "p".to_string(),
            node: node.to_string(),
            channel: channel.to_string(),
            message_type: message_type.map(str::to_string),
            message_type_pattern: pattern.map(str::to_string),
        }
    }

    #[test]
    fn compare_with_older_names_each_change() {
        let mut older = version("v1", &[]);
        older.inbound = vec![
            inbound("same", "POST /s", "json", "apikey\napikey.value=k"),
            inbound("moved", "POST /m", "json", "apikey\napikey.value=k"),
            inbound("unwaited", "POST /u", "json", ""),
        ];
        older.subscriptions = vec![
            subscription(SubscriptionKind::Wait, "W1", "same", None, None),
            subscription(SubscriptionKind::Wait, "W2", "moved", None, None),
        ];
        let mut candidate = version("v2", &[]);
        let mut changed_all = inbound("moved", "POST /m2", "xml", "apikey\napikey.value=other");
        changed_all.schema_digest = Some("abc".to_string());
        changed_all.transport = Some("dapr".to_string());
        candidate.inbound = vec![
            inbound("same", "POST /s", "json", "apikey\napikey.value=k"),
            changed_all,
            inbound("unwaited", "POST /u2", "json", ""),
        ];
        let (changed, starts) = compare_with_older(&candidate, &older);
        assert!(starts.is_empty());
        assert_eq!(changed.len(), 1, "{changed:?}");
        assert_eq!(changed[0].channel, "moved");
        assert_eq!(
            changed[0].changes,
            vec!["inbound auth", "codec", "schema", "address", "transport"]
        );
        assert_eq!(changed[0].wait_nodes, vec!["W2".to_string()]);
        let warning = changed[0].warning(Some(3));
        assert_eq!(warning.code, "SUTRA.DEPLOY.ENDPOINT.CHANNEL_CHANGED");
        assert!(
            warning
                .message
                .contains("3 instances of label-v1 are parked on it"),
            "{}",
            warning.message
        );
        assert!(changed[0]
            .warning(None)
            .message
            .contains("parked count unknown here: ask an engine (--engine-url)"));
    }

    #[test]
    fn a_channel_served_under_another_name_is_renamed_or_replaced() {
        let mut older = version("v1", &["POST /a"]);
        older.endpoints[0].channel = "a".to_string();
        older.inbound = vec![inbound("a", "POST /a", "json", "")];
        older.subscriptions = vec![subscription(SubscriptionKind::Wait, "W", "a", None, None)];
        let mut candidate = version("v2", &["POST /a"]);
        candidate.endpoints[0].channel = "b".to_string();
        candidate.inbound = vec![inbound("b", "POST /a", "json", "")];
        let (changed, _) = compare_with_older(&candidate, &older);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].changes, vec!["renamed or replaced"]);
        // Dropped with nothing taking its endpoint: kept by the older version, no warning.
        candidate.endpoints[0].claim = EndpointClaim::exclusive("POST /elsewhere");
        assert!(compare_with_older(&candidate, &older).0.is_empty());
    }

    #[test]
    fn it_never_prints_an_auth_value() {
        let mut older = version("v1", &[]);
        older.inbound = vec![inbound(
            "in",
            "POST /i",
            "json",
            "apikey\napikey.value=SECRET-ONE",
        )];
        older.subscriptions = vec![subscription(SubscriptionKind::Wait, "W", "in", None, None)];
        let mut candidate = version("v2", &[]);
        candidate.inbound = vec![inbound(
            "in",
            "POST /i",
            "json",
            "apikey\napikey.value=SECRET-TWO",
        )];
        let (changed, _) = compare_with_older(&candidate, &older);
        let message = changed[0].warning(Some(1)).message;
        assert!(message.contains("inbound auth"), "{message}");
        assert!(!message.contains("SECRET"), "{message}");
        assert!(!format!("{:?}", changed[0]).contains("SECRET"));
    }

    #[test]
    fn a_new_start_on_an_older_wait_warns() {
        let mut older = version("v1", &[]);
        older.subscriptions = vec![subscription(
            SubscriptionKind::Wait,
            "Await",
            "reply",
            None,
            None,
        )];
        let mut candidate = version("v2", &[]);
        candidate.subscriptions = vec![subscription(
            SubscriptionKind::Start,
            "Fresh",
            "reply",
            None,
            None,
        )];
        let (_, starts) = compare_with_older(&candidate, &older);
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].start_node, "Fresh");
        assert_eq!(starts[0].wait_nodes, vec!["Await".to_string()]);
        let warning = starts[0].warning(Some(1));
        assert_eq!(warning.code, "SUTRA.DEPLOY.ENDPOINT.START_ON_WAIT");
        assert!(warning
            .message
            .contains("1 instance of label-v1 is parked on it"));
        // A start the older version had already is nothing new.
        older.subscriptions.push(subscription(
            SubscriptionKind::Start,
            "Old",
            "reply",
            None,
            None,
        ));
        assert!(compare_with_older(&candidate, &older).1.is_empty());
    }

    #[test]
    fn disjoint_types_do_not() {
        let mut older = version("v1", &[]);
        older.subscriptions = vec![subscription(
            SubscriptionKind::Wait,
            "Await",
            "c",
            Some("reply"),
            None,
        )];
        let mut candidate = version("v2", &[]);
        candidate.subscriptions = vec![subscription(
            SubscriptionKind::Start,
            "S",
            "c",
            Some("request"),
            None,
        )];
        assert!(compare_with_older(&candidate, &older).1.is_empty());
        // A pattern is taken to overlap.
        candidate.subscriptions[0].message_type = None;
        candidate.subscriptions[0].message_type_pattern = Some("req.*".to_string());
        assert_eq!(compare_with_older(&candidate, &older).1.len(), 1);
    }

    #[test]
    fn superseded_names_the_timer_starts_that_stop() {
        let mut older = version("v1", &["a"]);
        older.timer_starts = vec!["nightly/Tick".to_string()];
        let verdict = evaluate_candidate(&version("v2", &["a"]), &[older]);
        assert!(
            verdict.warnings[0]
                .message
                .contains("Its timer starts stop: nightly/Tick."),
            "{}",
            verdict.warnings[0].message
        );
    }
}
