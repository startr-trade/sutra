//! BPMN 2.0 model + loader — the engine-internal process model and the loader that
//! parses the BPMN 2.0 subset the engine supports.
//!
//! Scope: the engine-internal process model ([`model`] — the `Node` variants,
//! [`model::ProcessDefinition`], [`model::SequenceFlow`], [`qbindings`], coverage paths) and
//! the [`loader::BpmnModelLoader`] that parses BPMN 2.0 XML plus the `q:` extension namespace
//! (`q:source`, `q:validators`, `q:onValidation`, `q:alias`, `q:dispatch`, `q:case`,
//! `q:reply`, `q:send`, `q:audit`, `q:param`, `q:variable`, `q:store`, `q:coverage`).
//!
//! Security posture matches the hardened XML load: quick-xml never resolves DTDs or external
//! entities, and a `<!DOCTYPE>` declaration is rejected outright (see [`xml`]).
#![forbid(unsafe_code)]

pub mod codes;
pub mod duration;
pub mod error;
pub mod loader;
pub mod model;
pub mod qbindings;
pub mod timer;
mod xml;

/// The canonical mask substituted for an `@sensitive`-declared variable's value wherever a
/// value would otherwise surface (audit payloads, the inspect projection, logs).
/// Single source so every redaction layer masks identically.
pub const REDACTED_PLACEHOLDER: &str = "***REDACTED***";

/// Suffix of the variable the intake redactor chain writes alongside a source's raw payload:
/// `<source.name>.redacted` holds the DLP-masked projection every observability surface shows
/// IN PLACE OF the raw `<source.name>`. Single source so intake (which writes it) and the read
/// sites (audit snapshot, inspect projection — which prefer it) agree on the name.
pub const REDACTION_COMPANION_SUFFIX: &str = ".redacted";

pub use error::SutraError;
pub use loader::BpmnModelLoader;
pub use model::{
    BpmnImport, CoveragePath, DataMapping, DeclaredVariable, FieldType, Node, ProcessAudit,
    ProcessDefinition, ProcessModule, SequenceFlow, DEFAULT_LOOP_ITEM_VARIABLE,
    LOOP_COUNTER_VARIABLE,
};
pub use timer::{TimerCycleSpec, TimerDefinition, TimerSpecRejection};
