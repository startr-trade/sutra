//! Inbound endpoint claims: who may receive on what, across every deployment of one engine.
//!
//! A deployment's inbound channels each claim an endpoint on their transport. Most endpoints
//! hand each message to ONE receiver — an HTTP route, a queue, a subscription, a consumer group —
//! so a second deployment claiming one would either never receive or silently compete for
//! messages. Those claims are [`EndpointSharing::Exclusive`]. A topic hands every subscriber its
//! own copy, so several deployments may claim one: [`EndpointSharing::Broadcast`].
//!
//! This module is the neutral half of the check: the claim shape and the collision rule. What an
//! endpoint's identity IS belongs to the transport that owns it, so this code names none.

use std::collections::BTreeMap;

/// How many deployments may hold one endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointSharing {
    /// One deployment only: each message reaches one receiver.
    Exclusive,
    /// Any number of deployments: every subscriber receives its own copy.
    Broadcast,
    /// One owner, for anything new. Deployments already running on it when the engine came up keep
    /// running, because earlier engines let them share it and refusing either would stop work that
    /// is going fine. `remedy` tells an operator how to give each its own.
    ExclusiveToNewDeployments { remedy: &'static str },
}

/// One inbound endpoint a channel claims on its transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointClaim {
    /// The endpoint's identity within its transport. Two claims collide only when their
    /// transports and identities are equal, so it must carry whatever tells two endpoints apart
    /// (a broker address as well as a queue name). It appears in refusal messages, so it must
    /// never carry a credential.
    pub identity: String,
    pub sharing: EndpointSharing,
}

impl EndpointClaim {
    /// A claim only one deployment may hold.
    pub fn exclusive(identity: impl Into<String>) -> EndpointClaim {
        EndpointClaim {
            identity: identity.into(),
            sharing: EndpointSharing::Exclusive,
        }
    }

    /// A claim a new deployment may not share, though those already running on it stay.
    pub fn exclusive_to_new_deployments(
        identity: impl Into<String>,
        remedy: &'static str,
    ) -> EndpointClaim {
        EndpointClaim {
            identity: identity.into(),
            sharing: EndpointSharing::ExclusiveToNewDeployments { remedy },
        }
    }

    /// A claim any number of deployments may hold.
    pub fn broadcast(identity: impl Into<String>) -> EndpointClaim {
        EndpointClaim {
            identity: identity.into(),
            sharing: EndpointSharing::Broadcast,
        }
    }
}

/// The label value a package gets for a `tenant` or `module` it does not declare.
pub const UNLABELED_PACKAGE_LABEL: &str = "unlabeled";

/// Who owns a deployment across its versions. The key is length-prefixed because labels are opaque
/// text: joining them with a separator would merge tenant `a--b` + module `c` with tenant `a` +
/// module `b--c`. A package that does not say who it belongs to cannot be anyone's next version, so
/// it owns only itself.
pub fn deployment_owner(tenant: &str, module: &str, deployment_id: &str) -> String {
    if tenant == UNLABELED_PACKAGE_LABEL || module == UNLABELED_PACKAGE_LABEL {
        return format!("unlabeled:{deployment_id}");
    }
    format!("{}:{tenant}/{module}", tenant.len())
}

/// A claim attributed to the deployment and channel making it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredEndpoint {
    /// Identifies the deployment: claims under one id never collide with each other, because the
    /// same content contributed twice is one deployment.
    pub deployment_id: String,
    /// How a refusal names the deployment (`tenant--module--version`).
    pub deployment_label: String,
    /// Who owns the deployment across its versions ([`deployment_owner`]). Versions of one owner
    /// may take an endpoint from each other, so their claims never collide.
    pub owner: String,
    pub channel: String,
    pub transport: String,
    pub claim: EndpointClaim,
}

/// Two deployments on one exclusive endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointCollision {
    pub transport: String,
    pub identity: String,
    /// The earlier claimant, in the order the claims were given.
    pub holder_label: String,
    pub holder_channel: String,
    /// The later claimant.
    pub claimant_label: String,
    pub claimant_channel: String,
}

impl EndpointCollision {
    /// The one-line account every surface reports.
    pub fn describe(&self) -> String {
        format!(
            "{} endpoint '{}' is claimed by channel '{}' of deployment {} and by channel '{}' of \
             deployment {}; it can belong to only one deployment",
            self.transport,
            self.identity,
            self.holder_channel,
            self.holder_label,
            self.claimant_channel,
            self.claimant_label
        )
    }
}

/// Every exclusive endpoint claimed by more than one owner. Claims within one deployment, or
/// between versions of one owner, are not collisions. Each later deployment is reported once
/// against the first claimant, so the deployment under test goes last when the caller wants it
/// named as the claimant.
pub fn find_endpoint_collisions(endpoints: &[DeclaredEndpoint]) -> Vec<EndpointCollision> {
    let mut by_endpoint: BTreeMap<(&str, &str), Vec<&DeclaredEndpoint>> = BTreeMap::new();
    for endpoint in endpoints {
        by_endpoint
            .entry((&endpoint.transport, &endpoint.claim.identity))
            .or_default()
            .push(endpoint);
    }
    let mut collisions = Vec::new();
    for ((transport, identity), claims) in by_endpoint {
        if claims
            .iter()
            .all(|c| c.claim.sharing == EndpointSharing::Broadcast)
        {
            continue;
        }
        let holder = claims[0];
        let mut reported: Vec<&str> = vec![&holder.deployment_id];
        for claim in &claims[1..] {
            if reported.contains(&claim.deployment_id.as_str()) || claim.owner == holder.owner {
                continue;
            }
            reported.push(&claim.deployment_id);
            collisions.push(EndpointCollision {
                transport: transport.to_string(),
                identity: identity.to_string(),
                holder_label: holder.deployment_label.clone(),
                holder_channel: holder.channel.clone(),
                claimant_label: claim.deployment_label.clone(),
                claimant_channel: claim.channel.clone(),
            });
        }
    }
    collisions
}

/// A refusal message for `collisions` (non-empty), prefixed by the code the way the deployment
/// source reports every activation failure.
pub fn describe_endpoint_refusal(collisions: &[EndpointCollision]) -> String {
    let details: Vec<String> = collisions.iter().map(EndpointCollision::describe).collect();
    format!(
        "[{}] {}",
        crate::codes::DEPLOY_ENDPOINT_COLLISION,
        details.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(
        dep: &str,
        channel: &str,
        transport: &str,
        claim: EndpointClaim,
    ) -> DeclaredEndpoint {
        DeclaredEndpoint {
            deployment_id: dep.to_string(),
            deployment_label: format!("label-{dep}"),
            owner: format!("owner-{dep}"),
            channel: channel.to_string(),
            transport: transport.to_string(),
            claim,
        }
    }

    #[test]
    fn an_exclusive_endpoint_held_by_two_deployments_collides() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "http", EndpointClaim::exclusive("POST /x")),
            endpoint("b", "in2", "http", EndpointClaim::exclusive("POST /x")),
        ]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].holder_label, "label-a");
        assert_eq!(found[0].claimant_label, "label-b");
        assert_eq!(found[0].claimant_channel, "in2");
        assert!(found[0].describe().contains("POST /x"));
    }

    #[test]
    fn a_broadcast_endpoint_may_be_held_by_many_deployments() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "bus", EndpointClaim::broadcast("orders")),
            endpoint("b", "in", "bus", EndpointClaim::broadcast("orders")),
            endpoint("c", "in", "bus", EndpointClaim::broadcast("orders")),
        ]);
        assert!(found.is_empty());
    }

    #[test]
    fn one_deployment_never_collides_with_itself() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "q", EndpointClaim::exclusive("q1")),
            endpoint("a", "in2", "q", EndpointClaim::exclusive("q1")),
        ]);
        assert!(found.is_empty());
    }

    #[test]
    fn versions_of_one_owner_do_not_collide() {
        let mut older = endpoint("v1", "in", "q", EndpointClaim::exclusive("q1"));
        let mut newer = endpoint("v2", "in", "q", EndpointClaim::exclusive("q1"));
        older.owner = "t--m".to_string();
        newer.owner = "t--m".to_string();
        assert!(find_endpoint_collisions(&[older.clone(), newer.clone()]).is_empty());
        // Another owner on that endpoint still collides, with the first claimant.
        let other = endpoint("x", "in", "q", EndpointClaim::exclusive("q1"));
        assert_eq!(find_endpoint_collisions(&[older, newer, other]).len(), 1);
    }

    #[test]
    fn labels_that_join_alike_are_different_owners() {
        assert_ne!(
            deployment_owner("a--b", "c", "d1"),
            deployment_owner("a", "b--c", "d1")
        );
        assert_eq!(
            deployment_owner("a", "b", "d1"),
            deployment_owner("a", "b", "d2")
        );
    }

    #[test]
    fn packages_without_labels_are_never_one_owner() {
        assert_ne!(
            deployment_owner(UNLABELED_PACKAGE_LABEL, UNLABELED_PACKAGE_LABEL, "d1"),
            deployment_owner(UNLABELED_PACKAGE_LABEL, UNLABELED_PACKAGE_LABEL, "d2")
        );
        assert_ne!(
            deployment_owner("t", UNLABELED_PACKAGE_LABEL, "d1"),
            deployment_owner("t", UNLABELED_PACKAGE_LABEL, "d2")
        );
    }

    #[test]
    fn equal_identities_on_different_transports_do_not_collide() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "one", EndpointClaim::exclusive("q1")),
            endpoint("b", "in", "two", EndpointClaim::exclusive("q1")),
        ]);
        assert!(found.is_empty());
    }

    #[test]
    fn each_later_deployment_is_reported_once() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "q", EndpointClaim::exclusive("q1")),
            endpoint("b", "in", "q", EndpointClaim::exclusive("q1")),
            endpoint("b", "in2", "q", EndpointClaim::exclusive("q1")),
            endpoint("c", "in", "q", EndpointClaim::exclusive("q1")),
        ]);
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn the_refusal_message_carries_its_code() {
        let found = find_endpoint_collisions(&[
            endpoint("a", "in", "q", EndpointClaim::exclusive("q1")),
            endpoint("b", "in", "q", EndpointClaim::exclusive("q1")),
        ]);
        assert!(describe_endpoint_refusal(&found).starts_with("[SUTRA.DEPLOY.ENDPOINT.COLLISION] "));
    }
}
