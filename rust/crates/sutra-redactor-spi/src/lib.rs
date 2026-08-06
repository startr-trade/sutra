//! The content-redactor SPI — the vendor-neutral seam the per-standard
//! `sutra-redactor-<standard>` crates self-register through, so the engine runs redactors
//! GENERICALLY and names no standard (mirroring the codec / transport / envref SPIs).
//!
//! A redactor LOCATES the sensitive spans of a DECODED payload (a [`FeelValue`] tree). The engine
//! then (a) masks every located path on observability surfaces (audit, logs, diagnostics, inspect)
//! and (b) marks them for encryption at rest. This crate ships no redactors — each is its own
//! `sutra-redactor-<standard>` crate that `inventory::submit!`s a [`RedactorEntry`], so
//! IMPLEMENTING a redactor IS registering it; `sutra-dist` (the composition root) force-links the
//! bundled ones so their submissions survive linker DCE, and [`RedactorRegistry::with_builtins`]
//! collects them.
//!
//! Fail-closed: unlike a validator crash (which becomes a business-reject issue), a redactor crash
//! (`Err` or panic) tells the engine to OVER-MASK — treat the whole bound payload as sensitive on
//! every observability surface — so a broken redactor can never leak and never crash the intake.
//! See [`run_redactor`] / [`RedactionOutcome`].
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use sutra_executor::{DeploymentId, Variables};
use sutra_feel::FeelValue;

pub mod tree;

/// A single located sensitive span in a decoded payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionLocator {
    /// JSON-Pointer-shaped path into the decoded payload tree — e.g. `/card`, `/tx/0/pan`, or
    /// `""` for the whole payload. The engine masks the value at this path.
    pub path: String,
    /// Stable reason code for the match (the redactor's own `SUTRA.*.REDACT.*` code), surfaced in
    /// the audit `redacted` list and diagnostics.
    pub reason_code: String,
}

impl RedactionLocator {
    pub fn new(path: impl Into<String>, reason_code: impl Into<String>) -> RedactionLocator {
        RedactionLocator {
            path: path.into(),
            reason_code: reason_code.into(),
        }
    }
}

/// A content redactor — run against the decoded payload projection to locate sensitive spans.
///
/// `Err(msg)` (or a panic) is converted by [`run_redactor`] into a fail-closed
/// [`RedactionOutcome::Failed`]: the engine over-masks the whole payload rather than risk a leak.
/// A redactor MUST NOT mutate anything; it only reports where the sensitive data is.
pub trait ContentRedactor {
    fn name(&self) -> &str;

    /// Locate the sensitive spans of `payload`. `variables` is the pre-instance context snapshot
    /// (for redactors whose policy depends on other fields).
    fn locate(
        &self,
        payload: &FeelValue,
        variables: &Variables,
    ) -> Result<Vec<RedactionLocator>, String>;
}

/// The result of running one redactor via [`run_redactor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedactionOutcome {
    /// The redactor located these spans (possibly empty).
    Located(Vec<RedactionLocator>),
    /// The redactor crashed (`Err` or panic). Fail-closed: the engine must over-mask the whole
    /// payload on every observability surface and treat it as sensitive at rest.
    Failed { redactor: String, message: String },
}

/// A redactor's self-registration: its `<q:redactor ref="…">` name + a constructor. Each
/// `sutra-redactor-<standard>` crate `inventory::submit!`s exactly one next to its
/// [`ContentRedactor`] impl — the same inventory pull model the transports / codecs / envref
/// resolvers use. The `make` fn-pointer keeps the submitted static `Sync`; the redactor is built
/// at registry-construction time.
pub struct RedactorEntry {
    /// The `<q:redactor ref="…">` name this redactor claims — matches its [`ContentRedactor::name`].
    pub name: &'static str,
    /// Construct a fresh boxed redactor (fn-pointer, so the inventory static stays `Sync`).
    pub make: fn() -> Box<dyn ContentRedactor>,
}

inventory::collect!(RedactorEntry);

/// The type-rooted URN prefix for redactor keys — `urn:sutra:redactor:`. A key is
/// `<prefix><localId>:<scope>` where the TRAILING `<scope>` is [`BUILTIN_SCOPE`] (`internal`, an
/// engine-provided redactor) or a `<deploymentId>` (`dep-…`, a user archive redactor). Scope-last
/// keeps the logical reference URN a clean prefix the resolver appends to, and makes built-in vs
/// archive vs cross-deployment keys disjoint by construction.
pub const REDACTOR_URN_PREFIX: &str = "urn:sutra:redactor:";

/// The reserved built-in scope — the trailing URN segment for engine-provided redactors
/// (`urn:sutra:redactor:<name>:internal`). A real deployment id (`dep-…`) can never equal it.
pub const BUILTIN_SCOPE: &str = "internal";

/// Name → redactor. Built-ins are keyed under their reserved URN; user archive redactors under a
/// deployment-scoped key. Resolution accepts a bare builtin name or the full URN.
#[derive(Default, Clone)]
pub struct RedactorRegistry {
    redactors: HashMap<String, Rc<dyn ContentRedactor>>,
}

impl RedactorRegistry {
    pub fn new() -> RedactorRegistry {
        RedactorRegistry::default()
    }

    /// Every redactor a `sutra-redactor-<standard>` crate self-registered via [`RedactorEntry`]
    /// and that the final binary force-links. Registered in name order (deterministic); the
    /// neutral engine names none.
    pub fn with_builtins() -> RedactorRegistry {
        let mut registry = RedactorRegistry::new();
        let mut entries: Vec<&RedactorEntry> = inventory::iter::<RedactorEntry>().collect();
        entries.sort_by_key(|e| e.name);
        for entry in entries {
            // Built-ins are keyed `urn:sutra:redactor:<name>:internal` — the `internal` scope is the
            // TRAILING segment (a real `<deploymentId>` can never equal it). See resolve().
            let key = format!("{REDACTOR_URN_PREFIX}{}:{BUILTIN_SCOPE}", entry.name);
            registry.redactors.insert(key, Rc::from((entry.make)()));
        }
        registry
    }

    /// Register under the redactor's own name (the test / extension seam).
    pub fn register(&mut self, redactor: impl ContentRedactor + 'static) {
        let r: Rc<dyn ContentRedactor> = Rc::new(redactor);
        self.redactors.insert(r.name().to_string(), r);
    }

    /// Register under an explicit key.
    pub fn register_under(&mut self, key: &str, redactor: impl ContentRedactor + 'static) {
        self.redactors.insert(key.to_string(), Rc::new(redactor));
    }

    pub fn find(&self, key: &str) -> Option<Rc<dyn ContentRedactor>> {
        self.redactors.get(key).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.redactors.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Resolve a `<q:redactor ref=…>` reference within `deployment`, appending the scope:
    /// this deployment's archive redactor first
    /// (`<logical>:<deploymentId>`), then a built-in (`<logical>:internal`), then the reference
    /// verbatim (an explicit fully-scoped URN). `logical` is the reference itself if it already
    /// starts with `urn:sutra:redactor:`, else that prefix + the reference. `None` = unknown ref
    /// (the intake raises `VALIDATE.REDACTOR_NOT_FOUND`, fail closed).
    pub fn resolve(
        &self,
        reference: &str,
        deployment: &DeploymentId,
    ) -> Option<Rc<dyn ContentRedactor>> {
        let logical = if reference.starts_with(REDACTOR_URN_PREFIX) {
            reference.to_string()
        } else {
            format!("{REDACTOR_URN_PREFIX}{reference}")
        };
        self.find(&format!("{logical}:{}", deployment.value()))
            .or_else(|| self.find(&format!("{logical}:{BUILTIN_SCOPE}")))
            .or_else(|| self.find(reference))
    }
}

/// Run a redactor, converting a crash (`Err` or panic) into a fail-closed
/// [`RedactionOutcome::Failed`] — the over-mask signal. Never panics, never leaks.
pub fn run_redactor(
    redactor: &dyn ContentRedactor,
    payload: &FeelValue,
    variables: &Variables,
) -> RedactionOutcome {
    let outcome = catch_unwind(AssertUnwindSafe(|| redactor.locate(payload, variables)));
    match outcome {
        Ok(Ok(locators)) => RedactionOutcome::Located(locators),
        Ok(Err(message)) => RedactionOutcome::Failed {
            redactor: redactor.name().to_string(),
            message,
        },
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "(panic)".to_string());
            RedactionOutcome::Failed {
                redactor: redactor.name().to_string(),
                message,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubRedactor {
        hits: Vec<RedactionLocator>,
    }
    impl ContentRedactor for StubRedactor {
        fn name(&self) -> &str {
            "stub"
        }
        fn locate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<RedactionLocator>, String> {
            Ok(self.hits.clone())
        }
    }
    struct BoomRedactor;
    impl ContentRedactor for BoomRedactor {
        fn name(&self) -> &str {
            "boom"
        }
        fn locate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<RedactionLocator>, String> {
            Err("kaboom".into())
        }
    }
    struct PanicRedactor;
    impl ContentRedactor for PanicRedactor {
        fn name(&self) -> &str {
            "panic"
        }
        fn locate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<RedactionLocator>, String> {
            panic!("boom")
        }
    }

    #[test]
    fn registry_registers_and_resolves_by_name() {
        let dep = DeploymentId::unresolved();
        let mut reg = RedactorRegistry::new();
        reg.register(StubRedactor { hits: vec![] });
        assert!(reg.resolve("stub", &dep).is_some()); // explicit-key tier (registered bare)
        assert!(reg.resolve("nope", &dep).is_none());
        assert_eq!(reg.names(), vec!["stub".to_string()]);
    }

    #[test]
    fn run_redactor_returns_located_on_ok() {
        let r = StubRedactor {
            hits: vec![RedactionLocator::new("/card", "X")],
        };
        let out = run_redactor(&r, &FeelValue::Null, &Variables::new());
        assert_eq!(
            out,
            RedactionOutcome::Located(vec![RedactionLocator::new("/card", "X")])
        );
    }

    #[test]
    fn run_redactor_fails_closed_on_err() {
        let out = run_redactor(&BoomRedactor, &FeelValue::Null, &Variables::new());
        match out {
            RedactionOutcome::Failed { redactor, .. } => assert_eq!(redactor, "boom"),
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn run_redactor_fails_closed_on_panic() {
        let out = run_redactor(&PanicRedactor, &FeelValue::Null, &Variables::new());
        assert!(matches!(out, RedactionOutcome::Failed { .. }));
    }

    // A crate-local redactor `inventory::submit!`ed to prove `with_builtins()` keys inventory
    // entries under the reserved URN and resolves them by both the bare name and the full URN — the
    // mechanism the real `sutra-redactor-<std>` crates use.
    struct FakeRedactor;
    impl ContentRedactor for FakeRedactor {
        fn name(&self) -> &str {
            "fake"
        }
        fn locate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<RedactionLocator>, String> {
            Ok(vec![])
        }
    }
    inventory::submit! {
        RedactorEntry { name: "fake", make: || Box::new(FakeRedactor) }
    }

    #[test]
    fn with_builtins_keys_under_internal_suffix_and_resolves() {
        let dep = DeploymentId::unresolved();
        let reg = RedactorRegistry::with_builtins();
        // Built-in keyed with the `internal` scope as the TRAILING segment.
        assert!(reg.find("urn:sutra:redactor:fake:internal").is_some());
        assert!(reg.find("fake").is_none());
        // A bare name resolves (scope appended); the full internal URN resolves verbatim.
        assert!(reg.resolve("fake", &dep).is_some());
        assert!(reg
            .resolve("urn:sutra:redactor:fake:internal", &dep)
            .is_some());
        assert!(reg
            .names()
            .contains(&"urn:sutra:redactor:fake:internal".to_string()));
    }
}
