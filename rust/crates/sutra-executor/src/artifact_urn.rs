//! Shared artifact-URN keying/resolution helper. Every pluggable engine artifact (redactor,
//! and in a follow-up sweep codec/validator) is
//! keyed in its registry under a URN whose TRAILING segment is the scope:
//! `urn:sutra:<artifactType>:<localId>:<scope>`, where `<scope>` is either the literal
//! [`BUILTIN_SCOPE`] (`internal`, a built-in) or a real `<deploymentId>` (`dep-<24 hex>`, an
//! archive artifact). Because a real deployment id can never equal `internal`, and both live in
//! the same (last) key position, built-in vs archive vs cross-deployment keys are disjoint by
//! construction — the logical reference URN is a clean prefix the resolver just appends the scope
//! to.
//!
//! This mirrors (and is the extraction target for) `sutra-redactor-spi`'s hand-rolled
//! `REDACTOR_URN_PREFIX` / `BUILTIN_SCOPE` / `RedactorRegistry::resolve`. Each registry keeps its
//! own value type and `HashMap`; only the keying + resolve logic is shared here.

use crate::deployment::DeploymentId;

/// The reserved built-in scope — the trailing URN segment for engine-provided artifacts
/// (`urn:sutra:<type>:<localId>:internal`). A real deployment id (`dep-…`) can never equal it.
pub const BUILTIN_SCOPE: &str = "internal";

/// The deployment-agnostic reference form an author writes: `urn:sutra:<type_segment>:<local_id>`.
/// Scope is appended later, at resolution time, by [`archive_key`] / [`builtin_key`].
pub fn logical_urn(type_segment: &str, local_id: &str) -> String {
    format!("urn:sutra:{type_segment}:{local_id}")
}

/// Normalize a reference to its logical URN: returns it unchanged if it already starts with
/// `urn:sutra:<type_segment>:`, else prefixes it via [`logical_urn`] (a bare name or archive-local
/// path form, e.g. `pci` or `myschema:accounts`).
pub fn logical_of(type_segment: &str, reference: &str) -> String {
    let prefix = format!("urn:sutra:{type_segment}:");
    if reference.starts_with(&prefix) {
        reference.to_string()
    } else {
        logical_urn(type_segment, reference)
    }
}

/// Archive-scope key: this deployment's artifact for `logical` — `<logical>:<deploymentId>`.
pub fn archive_key(logical: &str, deployment: &DeploymentId) -> String {
    format!("{logical}:{}", deployment.value())
}

/// Built-in-scope key: the engine-provided artifact for `logical` — `<logical>:internal`.
pub fn builtin_key(logical: &str) -> String {
    format!("{logical}:{BUILTIN_SCOPE}")
}

/// Generic 3-try resolution, most-specific first,
/// fail-closed (`None`) on total miss:
///
/// 1. this deployment's archive artifact — `find(archive_key(logical, deployment))`;
/// 2. a built-in — `find(builtin_key(logical))`;
/// 3. the reference verbatim — `find(reference)` (an explicit fully-scoped URN, e.g. a
///    cross-deployment reference or a pinned built-in).
///
/// `logical` is [`logical_of`]`(type_segment, reference)`. `find` looks a full registry key up in
/// whatever store the caller owns; the first hit wins.
pub fn resolve_scoped<T>(
    type_segment: &str,
    reference: &str,
    deployment: &DeploymentId,
    find: impl Fn(&str) -> Option<T>,
) -> Option<T> {
    let logical = logical_of(type_segment, reference);
    find(&archive_key(&logical, deployment))
        .or_else(|| find(&builtin_key(&logical)))
        .or_else(|| find(reference))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn logical_urn_builds_the_type_rooted_form() {
        assert_eq!(logical_urn("redactor", "pci"), "urn:sutra:redactor:pci");
    }

    #[test]
    fn logical_of_is_idempotent_on_an_already_qualified_urn() {
        assert_eq!(
            logical_of("redactor", "urn:sutra:redactor:pci"),
            "urn:sutra:redactor:pci"
        );
    }

    #[test]
    fn logical_of_prefixes_a_bare_reference() {
        assert_eq!(logical_of("redactor", "pci"), "urn:sutra:redactor:pci");
    }

    #[test]
    fn builtin_key_appends_the_internal_scope() {
        assert_eq!(
            builtin_key("urn:sutra:redactor:pci"),
            "urn:sutra:redactor:pci:internal"
        );
    }

    #[test]
    fn archive_key_appends_the_deployment_scope() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        assert_eq!(
            archive_key("urn:sutra:redactor:myschema:accounts", &dep),
            "urn:sutra:redactor:myschema:accounts:dep-000000000000000000000001"
        );
    }

    #[test]
    fn resolve_scoped_prefers_archive_over_builtin_of_the_same_logical_name() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let mut store: HashMap<&str, &str> = HashMap::new();
        store.insert(
            "urn:sutra:redactor:pci:dep-000000000000000000000001",
            "archive",
        );
        store.insert("urn:sutra:redactor:pci:internal", "builtin");
        let found = resolve_scoped("redactor", "pci", &dep, |k| store.get(k).copied());
        assert_eq!(found, Some("archive"));
    }

    #[test]
    fn resolve_scoped_falls_back_to_builtin_when_no_archive_entry() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let mut store: HashMap<&str, &str> = HashMap::new();
        store.insert("urn:sutra:redactor:pci:internal", "builtin");
        let found = resolve_scoped("redactor", "pci", &dep, |k| store.get(k).copied());
        assert_eq!(found, Some("builtin"));
    }

    #[test]
    fn resolve_scoped_falls_back_to_the_explicit_full_urn() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let mut store: HashMap<&str, &str> = HashMap::new();
        store.insert(
            "urn:sutra:redactor:pci:dep-000000000000000000000002",
            "cross-deployment",
        );
        let found = resolve_scoped(
            "redactor",
            "urn:sutra:redactor:pci:dep-000000000000000000000002",
            &dep,
            |k| store.get(k).copied(),
        );
        assert_eq!(found, Some("cross-deployment"));
    }

    #[test]
    fn resolve_scoped_returns_none_on_total_miss() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let store: HashMap<&str, &str> = HashMap::new();
        let found: Option<&str> =
            resolve_scoped("redactor", "pci", &dep, |k| store.get(k).copied());
        assert_eq!(found, None);
    }
}
