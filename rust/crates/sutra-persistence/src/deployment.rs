use crate::PersistenceError;

/// Opaque runtime identity of one sealed deployment. String form is
/// `dep-<24 lowercase hex>`. Every deployment-scoped row keys on this single value — the
/// engine never decomposes it back into human coordinates (tenant / module / version survive
/// only as manifest labels). The identity is the SHA-256 of the deployment manifest bytes —
/// there is no authoring-triple derivation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeploymentId(String);

impl DeploymentId {
    /// Validates the `dep-<24 lowercase hex>` form and wraps the value.
    pub fn new(value: impl Into<String>) -> Result<Self, PersistenceError> {
        let value = value.into();
        if Self::is_valid(&value) {
            Ok(Self(value))
        } else {
            Err(PersistenceError::InvalidArgument(format!(
                "deployment id must match dep-<24 lowercase hex>, got '{value}'"
            )))
        }
    }

    fn is_valid(value: &str) -> bool {
        let Some(hex) = value.strip_prefix("dep-") else {
            return false;
        };
        hex.len() == 24
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    /// Engine-internal default for system-scoped audit streams (startup events, operator
    /// actions).
    pub fn system() -> Self {
        Self("dep-000000000000000000000000".to_owned())
    }

    /// Pre-resolution placeholder — executor-only paths / test fixtures with no deployment.
    pub fn unresolved() -> Self {
        Self("dep-ffffffffffffffffffffffff".to_owned())
    }

    /// The raw string form (`dep-<24 hex>`), as bound into every SQL statement.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_form() {
        let id = DeploymentId::new("dep-0123456789abcdef01234567").unwrap();
        assert_eq!(id.as_str(), "dep-0123456789abcdef01234567");
    }

    #[test]
    fn rejects_bad_forms() {
        for bad in [
            "",
            "dep-",
            "dep-0123456789ABCDEF01234567",  // uppercase hex
            "dep-0123456789abcdef0123456",   // 23 chars
            "dep-0123456789abcdef012345678", // 25 chars
            "dip-0123456789abcdef01234567",  // wrong prefix
            "dep-0123456789abcdef0123456g",  // non-hex
        ] {
            assert!(DeploymentId::new(bad).is_err(), "should reject '{bad}'");
        }
    }

    #[test]
    fn well_known_ids_are_valid() {
        assert!(DeploymentId::is_valid(DeploymentId::system().as_str()));
        assert!(DeploymentId::is_valid(DeploymentId::unresolved().as_str()));
    }
}
