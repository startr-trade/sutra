//! Executor registries as plain structs — `TaskRegistry`,
//! `TemplateEngineRegistry`/`TemplateRegistry`, `DecisionEngineRegistry`/`DecisionRegistry`,
//! `ScriptRegistry`, `OutboundChannelRegistry`, `ProcessRegistry` and the auth-ref resolver
//! shape — everything the sync executor wires explicitly, with no dependency-injection container.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::qbindings::ReplyMode;
use sutra_bpmn::{ProcessDefinition, ProcessModule, SutraError};
use sutra_feel::FeelValue;

use crate::codes;
use crate::deployment::DeploymentId;
use crate::error::ExecError;
use crate::variables::Variables;

// ---- tasks ---------------------------------------------------------------------

/// The read view a task function gets of its invocation context — the `TaskContext`
/// getters (mutation happens through the returned output, not by mutating the context).
#[derive(Debug, Clone)]
pub struct TaskContextView {
    pub deployment: DeploymentId,
    pub labels: BTreeMap<String, String>,
    pub instance_id: String,
    pub module_id: String,
    pub module_version: String,
    pub simulation: bool,
    pub(crate) variables: Variables,
}

impl TaskContextView {
    /// The variables visible to this invocation (scoped `<q:param>` inputs overlaid,
    /// shadowing same-named process variables for this call only).
    pub fn variables(&self) -> &Variables {
        &self.variables
    }

    pub fn variable(&self, name: &str) -> Option<&FeelValue> {
        self.variables.get(name)
    }
}

/// How a task fails — the BPMN-error vs plain-failure split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    /// Raise a BPMN error the engine routes to a matching boundary event.
    BpmnError(String),
    /// An uncaught failure — wrapped as `SUTRA.RUNTIME.TASK.UNCAUGHT`.
    Failed(String),
}

pub type TaskFn = dyn Fn(&FeelValue, &TaskContextView) -> Result<FeelValue, TaskError>;

/// In-memory map of named task functions — the walking-skeleton `TaskRegistry`. There is no
/// serviceTask bean SPI; this registry backs the registered-task-function fallback of the
/// task-kind routing.
#[derive(Default, Clone)]
pub struct TaskRegistry {
    tasks: HashMap<String, Rc<TaskFn>>,
}

impl TaskRegistry {
    pub fn new() -> TaskRegistry {
        TaskRegistry::default()
    }

    /// Register a task under `name` (builder-style). Duplicate registration is a programming
    /// error (the `SUTRA.RESOLVE.TASK.NAME_COLLISION` code) — it panics here.
    pub fn register(
        mut self,
        name: &str,
        task: impl Fn(&FeelValue, &TaskContextView) -> Result<FeelValue, TaskError> + 'static,
    ) -> TaskRegistry {
        assert!(!name.trim().is_empty(), "task name is required");
        let prior = self.tasks.insert(name.to_string(), Rc::new(task));
        assert!(prior.is_none(), "Duplicate @Task(\"{name}\") registration");
        self
    }

    pub fn resolve(&self, name: &str) -> Result<Rc<TaskFn>, ExecError> {
        self.tasks.get(name).cloned().ok_or_else(|| {
            let mut known: Vec<&str> = self.tasks.keys().map(|k| k.as_str()).collect();
            known.sort_unstable();
            ExecError::Diagnostic(SutraError::new(
                sutra_bpmn::codes::RESOLVE_TASK_UNKNOWN,
                format!("No @Task(\"{name}\") registered. Known: {known:?}"),
            ))
        })
    }
}

// ---- template engines ------------------------------------------------------------

/// A template-rendering engine, selected by file extension (the `TemplateEngine` SPI).
pub trait TemplateEngine {
    /// Engine handle / registry key.
    fn name(&self) -> &str;
    /// The file extensions this engine claims (e.g. `".hbs"`).
    fn extensions(&self) -> Vec<String>;
    /// Render `template` (cached under `template_id`) against a JSON-object model.
    fn render(
        &self,
        template_id: &str,
        template: &[u8],
        model: &serde_json::Value,
    ) -> Result<String, String>;
}

/// The strict Handlebars engine adapter (R6 — the `.hbs` normative engine).
#[derive(Default)]
pub struct HbsTemplateEngine {
    inner: sutra_templates::HandlebarsTemplateEngine,
}

impl HbsTemplateEngine {
    pub fn new() -> HbsTemplateEngine {
        HbsTemplateEngine {
            inner: sutra_templates::HandlebarsTemplateEngine::new(),
        }
    }
}

impl TemplateEngine for HbsTemplateEngine {
    fn name(&self) -> &str {
        "h"
    }

    fn extensions(&self) -> Vec<String> {
        vec![".hbs".to_string()]
    }

    fn render(
        &self,
        template_id: &str,
        template: &[u8],
        model: &serde_json::Value,
    ) -> Result<String, String> {
        self.inner
            .render(template_id, template, model)
            .map_err(|e| e.message)
    }
}

/// Name→engine registry; call sites select by file extension via
/// [`TemplateEngineRegistry::for_implementation`].
#[derive(Default, Clone)]
pub struct TemplateEngineRegistry {
    engines: Vec<Rc<dyn TemplateEngine>>,
}

impl TemplateEngineRegistry {
    pub fn new() -> TemplateEngineRegistry {
        TemplateEngineRegistry::default()
    }

    pub fn register(mut self, engine: impl TemplateEngine + 'static) -> TemplateEngineRegistry {
        self.engines.push(Rc::new(engine));
        self
    }

    /// The engine that renders `implementation`, if any — the first registered engine whose
    /// extensions include a suffix of the (lower-cased) name.
    pub fn for_implementation(&self, implementation: &str) -> Option<Rc<dyn TemplateEngine>> {
        if implementation.trim().is_empty() {
            return None;
        }
        let lower = implementation.to_lowercase();
        self.engines
            .iter()
            .find(|e| {
                e.extensions()
                    .iter()
                    .any(|ext| !ext.trim().is_empty() && lower.ends_with(&ext.to_lowercase()))
            })
            .cloned()
    }

    pub fn names(&self) -> Vec<String> {
        self.engines.iter().map(|e| e.name().to_string()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }
}

// ---- decision engines ---------------------------------------------------------------

/// A decision-evaluation engine, selected by file extension (the `DecisionEngine` SPI).
pub trait DecisionEngine {
    fn name(&self) -> &str;
    fn extensions(&self) -> Vec<String>;
    /// Evaluate the decision file against the process variables; the result map merges
    /// (typed) into the variables.
    fn evaluate(
        &self,
        decision_id: &str,
        decision: &[u8],
        input: &Variables,
    ) -> Result<Variables, String>;
}

/// The DMN engine adapter backed by `sutra-dmn` (the `.dmn` normative engine).
#[derive(Default)]
pub struct DmnEngine {
    inner: sutra_dmn::DmnDecisionEngine,
}

impl DmnEngine {
    pub fn new() -> DmnEngine {
        DmnEngine {
            inner: sutra_dmn::DmnDecisionEngine::new(),
        }
    }
}

impl DecisionEngine for DmnEngine {
    fn name(&self) -> &str {
        "dmn"
    }

    fn extensions(&self) -> Vec<String> {
        vec![".dmn".to_string()]
    }

    fn evaluate(
        &self,
        decision_id: &str,
        decision: &[u8],
        input: &Variables,
    ) -> Result<Variables, String> {
        self.inner
            .evaluate(decision_id, decision, &input.to_feel_context())
            .map(|m| m.into_iter().collect())
            .map_err(|e| e.to_string())
    }
}

/// The `.srl` rule-DSL engine adapter backed by `sutra-srl` (the Sutra Rule Language:
/// a DRL-inspired ruleset front-end compiled onto FEEL). `businessRuleTask` binds either a `.dmn`
/// decision or a `.srl` ruleset; the registry selects by extension.
#[derive(Default)]
pub struct SrlEngine {
    inner: sutra_srl::SrlRuleEngine,
}

impl SrlEngine {
    pub fn new() -> SrlEngine {
        SrlEngine {
            inner: sutra_srl::SrlRuleEngine::new(),
        }
    }
}

impl DecisionEngine for SrlEngine {
    fn name(&self) -> &str {
        "srl"
    }

    fn extensions(&self) -> Vec<String> {
        vec![".srl".to_string()]
    }

    fn evaluate(
        &self,
        decision_id: &str,
        decision: &[u8],
        input: &Variables,
    ) -> Result<Variables, String> {
        self.inner
            .evaluate(decision_id, decision, &input.to_feel_context())
            .map(|m| m.into_iter().collect())
            .map_err(|e| e.to_string())
    }
}

/// Name→engine registry for [`DecisionEngine`]s.
#[derive(Default, Clone)]
pub struct DecisionEngineRegistry {
    engines: Vec<Rc<dyn DecisionEngine>>,
}

impl DecisionEngineRegistry {
    pub fn new() -> DecisionEngineRegistry {
        DecisionEngineRegistry::default()
    }

    pub fn register(mut self, engine: impl DecisionEngine + 'static) -> DecisionEngineRegistry {
        self.engines.push(Rc::new(engine));
        self
    }

    pub fn for_implementation(&self, implementation: &str) -> Option<Rc<dyn DecisionEngine>> {
        if implementation.trim().is_empty() {
            return None;
        }
        let lower = implementation.to_lowercase();
        self.engines
            .iter()
            .find(|e| {
                e.extensions()
                    .iter()
                    .any(|ext| !ext.trim().is_empty() && lower.ends_with(&ext.to_lowercase()))
            })
            .cloned()
    }

    pub fn names(&self) -> Vec<String> {
        self.engines.iter().map(|e| e.name().to_string()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }
}

// ---- byte registries ------------------------------------------------------------------

macro_rules! bytes_registry {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Default, Clone)]
        pub struct $name {
            by_key: HashMap<String, Vec<u8>>,
        }

        impl $name {
            pub fn new() -> $name {
                $name::default()
            }

            pub fn register(&mut self, key: &str, bytes: Vec<u8>) {
                assert!(!key.trim().is_empty(), "registry key is required");
                self.by_key.insert(key.to_string(), bytes);
            }

            pub fn find(&self, key: &str) -> Option<&[u8]> {
                self.by_key.get(key).map(|b| b.as_slice())
            }

            pub fn len(&self) -> usize {
                self.by_key.len()
            }

            pub fn is_empty(&self) -> bool {
                self.by_key.is_empty()
            }
        }
    };
}

bytes_registry!(
    /// Raw bytes of every deployed template file, keyed by its deployment-scoped id.
    TemplateRegistry
);
bytes_registry!(
    /// Raw bytes of every deployed `scripts/` file, keyed by its deployment-scoped id.
    ScriptRegistry
);
bytes_registry!(
    /// Raw bytes of every deployed decision file, keyed by its deployment-scoped id.
    DecisionRegistry
);

// ---- outbound channels -------------------------------------------------------------------

/// A `direction: outbound` channel resolved to its destination + wire rendering + auth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOutboundChannel {
    pub name: String,
    pub destination: String,
    pub mode: ReplyMode,
    pub auth_ref: Option<AuthRef>,
}

impl ResolvedOutboundChannel {
    /// Resolves an outbound channel: destination URI + auth scheme/secret-ref/header + the
    /// channel's declared cloudevents mode (`none`/`binary`/`structured`).
    pub fn resolve(
        name: &str,
        _transport: &str,
        destination: &str,
        auth_scheme: Option<&str>,
        auth_secret_ref: Option<&str>,
        auth_header: Option<&str>,
        cloudevents_mode: &str,
    ) -> ResolvedOutboundChannel {
        let mode = match cloudevents_mode {
            "structured" => ReplyMode::CloudeventStructured,
            "binary" => ReplyMode::CloudeventBinary,
            _ => ReplyMode::Native,
        };
        let auth_ref = match (auth_scheme, auth_secret_ref) {
            (Some(scheme), Some(secret_ref)) => Some(AuthRef {
                scheme: scheme.to_string(),
                secret_ref: secret_ref.to_string(),
                header: auth_header.map(|h| h.to_string()),
            }),
            _ => None,
        };
        ResolvedOutboundChannel {
            name: name.to_string(),
            destination: destination.to_string(),
            mode,
            auth_ref,
        }
    }
}

/// Declared outbound channels for `<q:send channel="…">` resolution, keyed by
/// (deployment, channel name).
#[derive(Default, Clone)]
pub struct OutboundChannelRegistry {
    channels: HashMap<(String, String), ResolvedOutboundChannel>,
}

impl OutboundChannelRegistry {
    pub fn new() -> OutboundChannelRegistry {
        OutboundChannelRegistry::default()
    }

    pub fn register(&mut self, deployment: &DeploymentId, channel: ResolvedOutboundChannel) {
        self.channels.insert(
            (deployment.value().to_string(), channel.name.clone()),
            channel,
        );
    }

    pub fn find(&self, deployment: &DeploymentId, name: &str) -> Option<&ResolvedOutboundChannel> {
        self.channels
            .get(&(deployment.value().to_string(), name.to_string()))
    }
}

// ---- outbound auth -----------------------------------------------------------------------

/// A resolvable outbound-auth reference (`auth` + `authSecretRef` + optional `authHeader`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRef {
    pub scheme: String,
    pub secret_ref: String,
    pub header: Option<String>,
}

/// A resolved secret (the `AuthRefResolver.ResolvedSecret` analog).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSecret {
    pub scheme: String,
    pub secret: Vec<u8>,
    pub header: Option<String>,
}

/// Registry closure consulted by `<q:reply auth=…>` emission — `None` when no resolver
/// claims the secret-ref URI scheme.
pub type AuthRefResolverRegistry = dyn Fn(&AuthRef) -> Option<ResolvedSecret>;

// ---- process registry ----------------------------------------------------------------------

/// Deployment-scoped process lookup — the port of `dispatch.ProcessRegistry`'s
/// `findInModule` (strict VM-7b resolution) + `findProcessAnywhere` (bare-id search with the
/// ambiguity guard).
#[derive(Default, Clone)]
pub struct ProcessRegistry {
    modules: Vec<(DeploymentId, Rc<ProcessModule>)>,
}

impl ProcessRegistry {
    pub fn new() -> ProcessRegistry {
        ProcessRegistry::default()
    }

    pub fn register(&mut self, deployment: DeploymentId, module: ProcessModule) {
        self.modules.push((deployment, Rc::new(module)));
    }

    /// The process within the module registered under `deployment`, if any.
    pub fn find_in_module(
        &self,
        deployment: &DeploymentId,
        process_id: &str,
    ) -> Option<Arc<ProcessDefinition>> {
        if process_id.trim().is_empty() {
            return None;
        }
        self.modules
            .iter()
            .filter(|(d, _)| d == deployment)
            .find_map(|(_, m)| m.process(process_id).ok().cloned().map(Arc::new))
    }

    /// Bare-id search across all live modules: single owner → resolve; 2+ live owners →
    /// `SUTRA.RESOLVE.BARE_ID.AMBIGUOUS` (fail closed rather than guess).
    pub fn find_process_anywhere(
        &self,
        process_id: &str,
    ) -> Result<Option<Arc<ProcessDefinition>>, ExecError> {
        if process_id.trim().is_empty() {
            return Ok(None);
        }
        let mut owners: Vec<(&DeploymentId, &ProcessDefinition)> = Vec::new();
        for (d, m) in &self.modules {
            if let Ok(p) = m.process(process_id) {
                if !owners.iter().any(|(od, _)| *od == d) {
                    owners.push((d, p));
                }
            }
        }
        if owners.len() > 1 {
            let keys: Vec<&str> = owners.iter().map(|(d, _)| d.value()).collect();
            return Err(ExecError::diag(
                codes::RESOLVE_BARE_ID_AMBIGUOUS,
                format!(
                    "Bare process id '{process_id}' is ambiguous: it exists in {} live \
                     deployments ({keys:?}). Qualify the reference so exactly one deployment \
                     resolves.",
                    owners.len()
                ),
            ));
        }
        Ok(owners.pop().map(|(_, p)| Arc::new(p.clone())))
    }
}
