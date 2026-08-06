//! Executor error type. Coded diagnostics carry the `SUTRA.*` codes; the
//! `channel:<name>` channel-call task kind belongs to the stateful surface and surfaces as
//! the typed [`ExecError::NotYetImplemented`] variant instead of a diagnostic.

use std::fmt;

use sutra_bpmn::SutraError;

/// Reserved (non-`SUTRA.*`-catalog) code string carried by the diagnostic view of a
/// [`ExecError::NotYetImplemented`] — the stateful executor lands the real channel-call
/// semantics.
pub const NOT_YET_IMPLEMENTED_CODE: &str = "SUTRA.EXEC.NOT_YET_IMPLEMENTED";

/// A synchronous-execution failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// A coded engine diagnostic (the `SutraException` analog).
    Diagnostic(SutraError),
    /// A `channel:<outboundChannel>` channel-call task. The sync executor
    /// recognises the task kind but does not implement it: channel-call = enqueue + park +
    /// correlate, which is the stateful/outbox surface.
    NotYetImplemented {
        node_id: String,
        implementation: String,
    },
}

impl ExecError {
    pub fn diag(code: &str, message: impl Into<String>) -> Self {
        ExecError::Diagnostic(SutraError::new(code, message))
    }

    /// The diagnostic code string (`NotYetImplemented` maps to
    /// [`NOT_YET_IMPLEMENTED_CODE`]).
    pub fn code(&self) -> &str {
        match self {
            ExecError::Diagnostic(d) => &d.code,
            ExecError::NotYetImplemented { .. } => NOT_YET_IMPLEMENTED_CODE,
        }
    }

    pub fn message(&self) -> String {
        match self {
            ExecError::Diagnostic(d) => d.message.clone(),
            ExecError::NotYetImplemented {
                node_id,
                implementation,
            } => format!(
                "Service task '{node_id}' is a channel-call task ({implementation}); \
                 channel-call execution (enqueue + park + correlate) is not implemented \
                 in the synchronous executor."
            ),
        }
    }

    /// A `SutraError` view of this failure — what listeners observe on `on_instance_failed`.
    pub fn to_diagnostic(&self) -> SutraError {
        match self {
            ExecError::Diagnostic(d) => d.clone(),
            other => SutraError::new(NOT_YET_IMPLEMENTED_CODE, other.message()),
        }
    }
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code(), self.message())
    }
}

impl std::error::Error for ExecError {}

impl From<SutraError> for ExecError {
    fn from(e: SutraError) -> Self {
        ExecError::Diagnostic(e)
    }
}
