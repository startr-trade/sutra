//! Synchronous token-graph executor — the SYNC execution surface. The frozen execution
//! contract is the spec and governs serviceTask task-kind routing.
//!
//! Scope: start events → data tasks (FEEL assignments) → exclusive / inclusive / parallel /
//! complex gateways → serviceTask routing by the task-kind precedence (reserved `coverage:`
//! ops → template suffix → decision suffix → `channel:` calls, which PARK the token as a wait
//! state keyed by the task's declared `<q:alias>`) →
//! embedded/transaction/ad-hoc/event sub-processes → link / escalation / error events → end
//! events. Emissions (`<q:reply>` / `<q:send>` / channel-call requests) land in an
//! [`emission::EmissionSink`] — collected; the dispatcher commits them atomically with the
//! quiescent-point step. Timer wait states (intermediate timer catch + interrupting
//! timer boundaries + `<q:timeout>`) park durable TIMER rows resumed via
//! [`executor::TokenExecutor::resume_timer`].
#![forbid(unsafe_code)]

pub mod artifact_urn;
pub mod codes;
pub mod coverage;
pub mod datastore;
pub mod deployment;
pub mod emission;
pub mod error;
pub mod executor;
pub mod listener;
pub mod registry;
pub mod telemetry;
pub mod test_clock;
pub mod variables;

pub use artifact_urn::{
    archive_key, builtin_key, logical_of, logical_urn, resolve_scoped, BUILTIN_SCOPE,
};
pub use coverage::{
    metric_flag_urn, CoverageCorrelation, CoverageFragment, CoverageMetricStore, CoverageMetrics,
    InMemoryCoverageStore,
};
pub use datastore::{DataStore, DataStoreTx, InMemoryDataStore, StoreError};
pub use deployment::{ArtifactType, DeploymentId};
pub use emission::{CollectingSink, Emission, EmissionKind, EmissionSink};
pub use error::ExecError;
pub use executor::{ExecResult, StatefulExecResult, TimerWait, TokenExecutor};
pub use listener::{TimerEvent, TimerFire};
pub use registry::{
    AuthRef, DecisionEngine, DecisionEngineRegistry, DecisionRegistry, DmnEngine,
    HbsTemplateEngine, OutboundChannelRegistry, ProcessRegistry, ResolvedOutboundChannel,
    ResolvedSecret, ScriptRegistry, SrlEngine, TaskContextView, TaskError, TaskRegistry,
    TemplateEngine, TemplateEngineRegistry, TemplateRegistry,
};
pub use sutra_crypto::Sensitive;
pub use test_clock::TestClock;
pub use variables::Variables;
