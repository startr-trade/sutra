//! Opaque runtime identity of one sealed deployment — the `DeploymentId` contract. One
//! opaque id replaces tenant/module/version as runtime identity. String form is
//! `dep-<24 lowercase hex>`; artifact ids are archive-local and scoped only by this id via
//! [`DeploymentId::artifact`].

/// The kind of a deployment-scoped artifact — the `type` segment of its registry key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactType {
    Template,
    Script,
    Decision,
    /// A rule/validator file (`*.dmn` validator rulesets) — the `rule` artifact-type
    /// segment the inbound validator chain resolves module-scoped validators under.
    Rule,
    /// A user-supplied redactor (`redactors/*.hbs`) — the `redactor` artifact-type segment the
    /// inbound redactor chain resolves deployment-scoped redactors under
    /// (`<deploymentId>:redactor:<name>`, alongside the `urn:sutra:redactor:<name>` built-ins).
    Redactor,
}

impl ArtifactType {
    pub fn segment(&self) -> &'static str {
        match self {
            ArtifactType::Template => "template",
            ArtifactType::Script => "script",
            ArtifactType::Decision => "decision",
            ArtifactType::Rule => "rule",
            ArtifactType::Redactor => "redactor",
        }
    }
}

/// `dep-<24 lowercase hex>` deployment identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeploymentId(String);

impl DeploymentId {
    /// Validating constructor — enforces the `dep-<24 lowercase hex>` form check.
    pub fn of(value: &str) -> Result<DeploymentId, String> {
        let ok = value.len() == 28
            && value.starts_with("dep-")
            && value[4..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase());
        if !ok {
            return Err(format!(
                "deployment id must match dep-<24 lowercase hex>, got '{value}'"
            ));
        }
        Ok(DeploymentId(value.to_string()))
    }

    /// Engine-internal default for system-scoped audit streams.
    pub fn system() -> DeploymentId {
        DeploymentId("dep-000000000000000000000000".to_string())
    }

    /// Pre-resolution placeholder — executor-only paths / test fixtures with no deployment.
    pub fn unresolved() -> DeploymentId {
        DeploymentId("dep-ffffffffffffffffffffffff".to_string())
    }

    pub fn value(&self) -> &str {
        &self.0
    }

    /// True when a real deployment identity is in scope (not the executor-only sentinel).
    pub fn is_resolved(&self) -> bool {
        *self != DeploymentId::unresolved()
    }

    /// Registry key of an artifact local to this deployment:
    /// `<deploymentId>:<type>:<localId>` (artifact ids are archive-local).
    pub fn artifact(&self, artifact_type: ArtifactType, local_id: &str) -> String {
        format!("{}:{}:{}", self.0, artifact_type.segment(), local_id)
    }
}

impl std::fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
