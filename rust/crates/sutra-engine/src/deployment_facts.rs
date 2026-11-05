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
        DeploymentFacts {
            deployment_id: d.id.value().to_string(),
            owner,
            version: d.version.clone(),
            shadowed: declared.shadowed,
            slot: deployment_slot(&d.tenant, &d.module, &d.version, d.id.value()),
            label,
            endpoints: declared.endpoints,
            unchecked: declared.unchecked,
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
pub fn settle_claims(
    candidates: &[DeploymentFacts],
    holders: &BTreeMap<EndpointKey, EndpointHolding>,
) -> Vec<SettledClaim> {
    let shares_endpoint = |a: &DeploymentFacts, b: &DeploymentFacts| {
        a.endpoints.iter().any(|e| {
            b.endpoints
                .iter()
                .any(|o| o.transport == e.transport && o.claim.identity == e.claim.identity)
        })
    };
    let mut refused: BTreeMap<usize, String> = BTreeMap::new();
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
/// naming what it loses. A deployment drains whole, so an endpoint the candidate did not ask for
/// stops being served as well, and the operator has to be told. A topic counts as well: a version
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
            format!(" These endpoints stop being served: {}.", lost.join(", "))
        };
        warnings.push(DeployWarning {
            code: sutra_channels::codes::DEPLOY_ENDPOINT_SUPERSEDED.to_string(),
            message: format!(
                "deployment {} takes over from {}, an earlier deployment of the same tenant and \
                 module. {} drains: it takes no new work, and its running instances finish on it.{lost}",
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
        assert!(verdict.warnings[0].message.contains("stop being served: b"));
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
        let found = settle_claims(&[a.clone(), z.clone()], &holders);
        assert!(
            matches!(found[0], SettledClaim::Refused { .. }),
            "{found:?}"
        );
        assert_eq!(found[1], SettledClaim::Active);
        let found = settle_claims(&[z, a], &holders);
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
        );
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        let alone = settle_claims(
            &[facts("a", "e", &[])],
            &held(&[("e", "owner-gone", "gone")]),
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
            let found = settle_claims(&order, &BTreeMap::new());
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
        let found = settle_claims(&[v1, v2, v3], &BTreeMap::new());
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
        let found = settle_claims(&[a, b, c], &held(&[("e1", "owner-a", "a")]));
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
        );
        assert_eq!(found, vec![SettledClaim::Active, SettledClaim::Active]);
    }

    #[test]
    fn two_contents_of_one_version_are_ranked_only_by_the_record() {
        let (one, two) = (version("c1", &["a"]), version("c2", &["a"]));
        let found = settle_claims(&[one.clone(), two.clone()], &BTreeMap::new());
        assert!(matches!(found[0], SettledClaim::Refused { .. }));
        assert!(matches!(found[1], SettledClaim::Refused { .. }));
        let found = settle_claims(&[one, two], &held(&[("a", "t--m", "c2")]));
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
}
