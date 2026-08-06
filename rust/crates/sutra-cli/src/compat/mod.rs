//! BPMN backward-compatibility checking: structural signatures, the removal diff, and
//! the report renderers. The five `SUTRA.COMPAT.*` diagnostic codes emitted here are
//! frozen contract strings; the wording rendered around them is this tool's own, pinned
//! by this crate's own tests.

mod checker;
mod report;
mod signature;

pub use checker::check;
pub use report::{diagnostic_code, Addition, CompatReport, Removal};
pub use signature::{BpmnSignature, ProcessSignature};
