//! The live versions of each owner, as the dispatcher correlates across them.
//!
//! An owner is a package across its versions ([`crate::deployment_owner`]): while a superseded
//! version drains, its parked instances are still the owner's work, and a message on the owner's
//! channel may be the answer one of them waits for. [`OwnerScopes`] answers, for any served
//! deployment, which deployments a correlation should search and in what order, and with which of
//! each version's own channel bindings a hit is then processed.
//!
//! Built once per activation, immutable, shared by every lane. It holds every live revision's
//! inbound bindings, including those of a draining revision the channel registry leaves out because
//! a newer revision serves the same channel name.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::ChannelBinding;
use crate::DeploymentId;

/// One live version of an owner: its deployment and its own inbound bindings by channel name.
#[derive(Clone)]
pub struct OwnerScope {
    pub deployment: DeploymentId,
    bindings: HashMap<String, ChannelBinding>,
}

impl OwnerScope {
    /// This version's own binding of `channel`, if it declares one.
    pub fn binding(&self, channel: &str) -> Option<&ChannelBinding> {
        self.bindings.get(channel)
    }
}

/// Every served deployment's owner scopes, keyed by deployment id.
#[derive(Default)]
pub struct OwnerScopes {
    by_deployment: HashMap<String, Arc<[OwnerScope]>>,
}

impl OwnerScopes {
    /// `versions`: `(owner, deployment, every inbound binding of that deployment)`, active ones
    /// first, then the draining ones most recently drained first. Each deployment's scope list is
    /// itself first, then the owner's other versions in that order.
    pub fn build(versions: Vec<(String, DeploymentId, Vec<ChannelBinding>)>) -> OwnerScopes {
        let mut by_owner: Vec<(String, Vec<OwnerScope>)> = Vec::new();
        for (owner, deployment, bindings) in versions {
            let scope = OwnerScope {
                deployment,
                bindings: bindings
                    .into_iter()
                    .map(|b| (b.channel_name.clone(), b))
                    .collect(),
            };
            match by_owner.iter_mut().find(|(o, _)| *o == owner) {
                Some((_, scopes)) => {
                    if !scopes.iter().any(|s| s.deployment == scope.deployment) {
                        scopes.push(scope);
                    }
                }
                None => by_owner.push((owner, vec![scope])),
            }
        }
        let mut by_deployment = HashMap::new();
        for (_, scopes) in by_owner {
            for (i, own) in scopes.iter().enumerate() {
                let ordered: Vec<OwnerScope> = std::iter::once(own.clone())
                    .chain(
                        scopes
                            .iter()
                            .enumerate()
                            .filter(|(j, _)| *j != i)
                            .map(|(_, s)| s.clone()),
                    )
                    .collect();
                by_deployment.insert(own.deployment.value().to_string(), Arc::from(ordered));
            }
        }
        OwnerScopes { by_deployment }
    }

    /// The owner's live versions, `deployment` first. Empty for an unknown id (a bare builder):
    /// the caller then treats the deployment as alone.
    pub fn live_for(&self, deployment: &DeploymentId) -> &[OwnerScope] {
        self.by_deployment
            .get(deployment.value())
            .map(|scopes| &scopes[..])
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Namespace;

    fn dep(n: u8) -> DeploymentId {
        DeploymentId::of(&format!("dep-0000000000000000000000{n:02x}")).unwrap()
    }

    fn binding(d: &DeploymentId, channel: &str) -> ChannelBinding {
        ChannelBinding::new(
            channel,
            Namespace::new("t", "m", "1.0.0"),
            d.clone(),
            "json",
        )
    }

    #[test]
    fn each_version_sees_itself_first_then_its_owners_others_and_no_other_owner() {
        let (v2, v1, other) = (dep(2), dep(1), dep(9));
        let scopes = OwnerScopes::build(vec![
            ("own".into(), v2.clone(), vec![binding(&v2, "in")]),
            ("else".into(), other.clone(), vec![binding(&other, "in")]),
            ("own".into(), v1.clone(), vec![binding(&v1, "old-in")]),
        ]);
        let ids = |d: &DeploymentId| {
            scopes
                .live_for(d)
                .iter()
                .map(|s| s.deployment.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&v2), vec![v2.clone(), v1.clone()]);
        assert_eq!(ids(&v1), vec![v1.clone(), v2.clone()]);
        assert_eq!(ids(&other), vec![other.clone()]);
        assert!(scopes.live_for(&dep(7)).is_empty());
        assert!(scopes.live_for(&v1)[0].binding("old-in").is_some());
        assert!(scopes.live_for(&v1)[0].binding("in").is_none());
    }
}
