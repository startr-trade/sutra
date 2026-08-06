//! Tier-2 content validators — the content-validator contract, the validator registry,
//! and the DMN-backed adapter with its payload-shape projection.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use sutra_codec_spi::{IssueSeverity, ValidationIssue};
use sutra_dmn::validator::DmnPayload;
use sutra_dmn::DmnRulesetValidator;
use sutra_executor::{resolve_scoped, DeploymentId, Variables};
use sutra_feel::FeelValue;

use crate::codes;

/// Validation tier — drives the frozen `validation.tier` summary. Schema-class
/// validators declare `Structural`; everything else defaults to `Content`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidatorTier {
    Structural,
    Content,
}

/// A tier-2 content validator, run against the projected payload.
///
/// - Complex validators (`<q:complexValidator source=…>`) receive the decoded payload
///   projected to the FEEL context (map → the map; scalar/bytes → `{value: …}`) — handled
///   by the adapter, one projection for aliases, gateways and validators alike.
/// - Simple validators (`<q:simpleValidator ref=… path=…>`) receive the single value the
///   FEEL path resolves (unresolvable ⇒ null ⇒ the validator decides).
///
/// `Err(msg)` (or a panic) is converted by the intake into a synthetic
/// `SUTRA.RUNTIME.VALIDATOR.UNCAUGHT` ERROR issue — never a dropped message, never a
/// crashed intake (fail-closed to business-reject routing).
///
/// **`Send + Sync` is required** for the same reason [`sutra_codec_spi::PayloadCodec`]
/// requires it (execution scale-out §2 row 10): a validator is a pure function over its
/// compiled ruleset, so the engine builds the validator registry once per activation and
/// shares it across every actor lane rather than compiling one copy per lane.
pub trait ContentValidator: Send + Sync {
    fn name(&self) -> &str;

    fn tier(&self) -> ValidatorTier {
        ValidatorTier::Content
    }

    /// Validate the payload; `variables` is the pre-instance context snapshot.
    fn validate(
        &self,
        payload: &FeelValue,
        variables: &Variables,
    ) -> Result<Vec<ValidationIssue>, String>;
}

/// Name → validator, keyed under the `rule` artifact-type URN scheme (the same scheme
/// [`crate::RedactorRegistry`] implements): a user `rules/*.dmn`/`*.srl` validator archive-keyed
/// (`urn:sutra:rule:<path>:<name>.<ext>:<deploymentId>`), an extension-supplied validator
/// builtin-keyed (`urn:sutra:rule:<name>:internal` — no registration SPI is wired yet, a
/// separate follow-up), or an explicit fully-scoped key. See [`ValidatorRegistry::resolve`].
#[derive(Default, Clone)]
pub struct ValidatorRegistry {
    validators: HashMap<String, Arc<dyn ContentValidator>>,
}

impl ValidatorRegistry {
    pub fn new() -> ValidatorRegistry {
        ValidatorRegistry::default()
    }

    /// Register under the validator's own name (a global SPI validator / test convenience).
    pub fn register(&mut self, validator: impl ContentValidator + 'static) {
        let v: Arc<dyn ContentValidator> = Arc::new(validator);
        self.validators.insert(v.name().to_string(), v);
    }

    /// Register under an explicit key (the deployment-scoped rule URN key `assembly.rs`'s
    /// `plan_deployment` mints via `archive_key(logical_urn("rule", local_id_with_ext), dep)`).
    pub fn register_under(&mut self, key: &str, validator: impl ContentValidator + 'static) {
        self.validators.insert(key.to_string(), Arc::new(validator));
    }

    /// Exact registry-key lookup (no scope resolution) — the seam [`ValidatorRegistry::resolve`]
    /// composes over. A `<q:complexValidator>`/`<q:simpleValidator>` reference should go through
    /// [`ValidatorRegistry::resolve`] instead.
    pub fn find(&self, key: &str) -> Option<Arc<dyn ContentValidator>> {
        self.validators.get(key).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.validators.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Resolve a `<q:complexValidator source=…>`/`<q:simpleValidator ref=…>` reference within
    /// `deployment`, in the same tri-tier most-specific-first order the codec registry uses:
    /// this deployment's archive rule first (`<logical>:<deploymentId>`), then a built-in (`<logical>:internal` — presently
    /// unwired, see the struct doc), then the reference verbatim (an explicit fully-scoped URN).
    /// `None` = fail closed (the intake raises `VALIDATE.VALIDATOR_NOT_FOUND`).
    pub fn resolve(
        &self,
        name: &str,
        deployment: &DeploymentId,
    ) -> Option<Arc<dyn ContentValidator>> {
        resolve_scoped("rule", name, deployment, |k| self.find(k))
    }
}

/// Run a validator, converting a crash (`Err` or panic) into the synthetic
/// `RUNTIME.VALIDATOR.UNCAUGHT` ERROR issue — the InboundChain `runValidator` contract.
pub fn run_validator(
    validator: &dyn ContentValidator,
    payload: &FeelValue,
    variables: &Variables,
) -> Vec<ValidationIssue> {
    let outcome = catch_unwind(AssertUnwindSafe(|| validator.validate(payload, variables)));
    match outcome {
        Ok(Ok(issues)) => issues,
        Ok(Err(message)) => vec![uncaught(validator.name(), &message)],
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "(panic)".to_string());
            vec![uncaught(validator.name(), &message)]
        }
    }
}

fn uncaught(name: &str, message: &str) -> ValidationIssue {
    ValidationIssue {
        code: codes::RUNTIME_VALIDATOR_UNCAUGHT.to_string(),
        severity: IssueSeverity::Error,
        path: String::new(),
        message: format!("Validator '{name}' threw: {message}"),
        value: None,
    }
}

// ---- DMN adapter -------------------------------------------------------------------------

/// DMN-decision-backed [`ContentValidator`] — the `sutra-validator-dmn` adapter. Applies
/// the `feelContext` projection via [`DmnPayload`]: a map payload IS the context;
/// a scalar becomes `{value: …}` (the `{body: …}` envelope rule is the codec side,
/// applied before the payload reaches the validator).
pub struct DmnContentValidator {
    inner: DmnRulesetValidator,
}

impl DmnContentValidator {
    pub fn new(inner: DmnRulesetValidator) -> DmnContentValidator {
        DmnContentValidator { inner }
    }
}

impl ContentValidator for DmnContentValidator {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn validate(
        &self,
        payload: &FeelValue,
        _variables: &Variables,
    ) -> Result<Vec<ValidationIssue>, String> {
        let projected = match payload {
            FeelValue::Map(m) => DmnPayload::Map(m.clone()),
            other => DmnPayload::Value(other.clone()),
        };
        let issues = self.inner.validate(&projected).map_err(|e| e.to_string())?;
        Ok(issues.into_iter().map(convert_issue).collect())
    }
}

// ---- SRL adapter -------------------------------------------------------------------------

/// `.srl` rule-DSL-backed [`ContentValidator`] — the sibling of [`DmnContentValidator`],
/// so a module can split ONE tier-2 ruleset across BOTH engines and attach both files to the
/// same `<q:validators>` chain (their issues accumulate into the single `validation.*` summary,
/// in declaration order).
///
/// Payload projection is the same rule as the DMN adapter's [`DmnPayload`]: a map payload IS the
/// evaluation context; anything else becomes `{value: …}`.
///
/// Two deliberate differences from the DMN adapter:
/// - **No clock.** The reserved `now` variable is injected only at the DMN evaluation entry
///   points ([`sutra_dmn::validator::NOW_VARIABLE`]); `.srl` evaluation has no clock injection,
///   so clock-dependent window rules belong in a decision table, not a ruleset.
/// - **Parsed per invocation**, matching `sutra_executor::SrlEngine`'s stateless
///   `DecisionEngine` contract (`SrlRuleEngine::evaluate` owns parsing). The deploy already
///   rejected an unparseable `.srl` fail-closed in `assembly.rs`, so this is a re-parse of
///   known-good source, never the place a syntax error first surfaces.
pub struct SrlContentValidator {
    /// Registry/diagnostic name — the archive-local rule id (e.g. `order-amount-field.srl`).
    name: String,
    /// The ruleset source (already deploy-time validated).
    source: String,
    engine: sutra_srl::SrlRuleEngine,
}

impl SrlContentValidator {
    pub fn new(name: &str, source: &str) -> SrlContentValidator {
        SrlContentValidator {
            name: name.to_string(),
            source: source.to_string(),
            engine: sutra_srl::SrlRuleEngine::new(),
        }
    }
}

impl ContentValidator for SrlContentValidator {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate(
        &self,
        payload: &FeelValue,
        _variables: &Variables,
    ) -> Result<Vec<ValidationIssue>, String> {
        let context = match payload {
            FeelValue::Map(m) => m.clone(),
            other => {
                let mut ctx = sutra_feel::FeelContext::new();
                ctx.insert("value".to_string(), other.clone());
                ctx
            }
        };
        let outputs = self
            .engine
            .evaluate(&self.name, self.source.as_bytes(), &context)
            .map_err(|e| format!("[{}] {}", e.code, e.message))?;
        // The engine emits `issues` only when at least one `report(…)` fired; `set` targets are
        // decision outputs, not validation verdicts, so they are ignored on this path.
        let Some(FeelValue::List(issues)) = outputs.get("issues") else {
            return Ok(Vec::new());
        };
        Ok(issues.iter().map(convert_srl_issue).collect())
    }
}

/// Project one `.srl` issue map (the engine's frozen `code`/`severity`/`path`/`message`/`value`
/// shape) onto the codec-SPI [`ValidationIssue`]. A missing/mistyped key degrades to its neutral
/// form rather than dropping the diagnostic — fail-closed: an unrecognised severity is ERROR.
fn convert_srl_issue(issue: &FeelValue) -> ValidationIssue {
    let field = |key: &str| match issue {
        FeelValue::Map(m) => m.get(key).cloned(),
        _ => None,
    };
    let text = |key: &str| match field(key) {
        Some(FeelValue::String(s)) => s,
        Some(other) => sutra_feel::value::canonical_string_of(&other),
        None => String::new(),
    };
    ValidationIssue {
        code: text("code"),
        severity: match text("severity").as_str() {
            "WARNING" => IssueSeverity::Warning,
            "INFO" => IssueSeverity::Info,
            _ => IssueSeverity::Error,
        },
        path: text("path"),
        message: text("message"),
        // `validation.firstReasonCode` renders the canonical string form of the value slot;
        // The `.srl` engine always reports a null value slot, which stays `None` here (not "null").
        value: match field("value") {
            None | Some(FeelValue::Null) => None,
            Some(other) => Some(sutra_feel::value::canonical_string_of(&other)),
        },
    }
}

fn convert_issue(issue: sutra_dmn::ValidationIssue) -> ValidationIssue {
    ValidationIssue {
        code: issue.code,
        severity: match issue.severity {
            sutra_dmn::Severity::Error => IssueSeverity::Error,
            sutra_dmn::Severity::Warning => IssueSeverity::Warning,
            sutra_dmn::Severity::Info => IssueSeverity::Info,
        },
        path: issue.path,
        message: issue.message,
        // `validation.firstReasonCode` renders the canonical string form of the value slot.
        value: issue
            .value
            .as_ref()
            .map(sutra_feel::value::canonical_string_of),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_executor::{archive_key, logical_urn};

    struct StubValidator(&'static str);
    impl ContentValidator for StubValidator {
        fn name(&self) -> &str {
            self.0
        }
        fn validate(
            &self,
            _payload: &FeelValue,
            _variables: &Variables,
        ) -> Result<Vec<ValidationIssue>, String> {
            Ok(Vec::new())
        }
    }

    /// `resolve()`'s tri-tier order against a rule
    /// archive-keyed exactly as `assembly.rs`'s `plan_deployment` mints it —
    /// `archive_key(logical_urn("rule", local_id_with_ext), dep)`, extension KEPT (a rule may be
    /// authored in either engine, so the extension selects it). The bare
    /// local id resolves within the owning deployment but NOT under a different one (no cross-
    /// deployment leakage), and an unknown reference fails closed.
    #[test]
    fn resolve_finds_an_archive_keyed_rule_within_its_own_deployment_only() {
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let other = DeploymentId::of("dep-000000000000000000000002").unwrap();
        let mut registry = ValidatorRegistry::new();
        let logical = logical_urn("rule", "pricing:tiers.dmn");
        registry.register_under(&archive_key(&logical, &dep), StubValidator("tiers"));

        assert!(registry.resolve("pricing:tiers.dmn", &dep).is_some());
        assert!(registry
            .resolve("urn:sutra:rule:pricing:tiers.dmn", &dep)
            .is_some());
        assert!(registry.resolve("pricing:tiers.dmn", &other).is_none());
        assert!(registry.resolve("nope", &dep).is_none());
    }

    /// The `.srl` adapter's payload projection mirrors the DMN one: a NON-map payload becomes
    /// `{value: …}`, so a ruleset can reason over a scalar-decoding codec's output. The reported
    /// issue keeps the `.srl` engine's frozen shape — ERROR severity, and a null `value` slot stays `None`
    /// (not the string `"null"`), because `validation.firstReasonCode` renders that slot.
    #[test]
    fn srl_validator_projects_a_scalar_payload_under_value_and_converts_the_issue() {
        const RULESET: &str = r#"
rule "must-be-ok"
when
  value != "ok"
then
  report("TEST.NOT_OK", "value", "expected ok, got " + value);
end
"#;
        let validator = SrlContentValidator::new("scalar.srl", RULESET);
        let issues = validator
            .validate(&FeelValue::String("nope".to_string()), &Variables::new())
            .expect("evaluates");
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "TEST.NOT_OK");
        assert_eq!(issues[0].path, "value");
        assert_eq!(issues[0].message, "expected ok, got nope");
        assert_eq!(issues[0].severity, IssueSeverity::Error);
        assert_eq!(issues[0].value, None);

        // A satisfied ruleset reports nothing at all (the engine omits an empty `issues` list).
        assert!(validator
            .validate(&FeelValue::String("ok".to_string()), &Variables::new())
            .expect("evaluates")
            .is_empty());
    }

    /// A bare-name-registered validator (the `register()` convenience — global SPI / test double)
    /// resolves via the explicit tier (tier 3, `find(reference)` verbatim) regardless of
    /// deployment, since the builtin tier (`urn:sutra:rule:<name>:internal`) is presently unwired.
    #[test]
    fn resolve_falls_back_to_the_explicit_tier_for_a_bare_registered_validator() {
        let dep = DeploymentId::unresolved();
        let mut registry = ValidatorRegistry::new();
        registry.register(StubValidator("checker"));
        assert!(registry.resolve("checker", &dep).is_some());
    }
}
