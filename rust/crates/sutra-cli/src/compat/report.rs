//! Compatibility report model + renderers. Removals are breaking; additions are
//! informational. The `SUTRA.COMPAT.*` codes are frozen contract strings.

use crate::output::Diagnostic;

/// Element kinds reported by the checker — stable wire-format strings.
pub const KIND_PROCESS: &str = "process";
pub const KIND_START_EVENT: &str = "startEvent";
pub const KIND_END_EVENT: &str = "endEvent";
pub const KIND_USER_TASK: &str = "userTask";
pub const KIND_SERVICE_TASK: &str = "serviceTask";
pub const KIND_SCRIPT_TASK: &str = "scriptTask";
pub const KIND_MESSAGE_REF: &str = "messageRef";

/// The frozen diagnostic code for a removal of the given element kind. All task kinds map
/// to `TASK_REMOVED`; unknown kinds fall back to `SUTRA.COMPAT.UNKNOWN`.
pub fn diagnostic_code(element_kind: &str) -> &'static str {
    match element_kind {
        KIND_PROCESS => "SUTRA.COMPAT.PROCESS_REMOVED",
        KIND_START_EVENT => "SUTRA.COMPAT.START_EVENT_REMOVED",
        KIND_END_EVENT => "SUTRA.COMPAT.END_EVENT_REMOVED",
        KIND_USER_TASK | KIND_SERVICE_TASK | KIND_SCRIPT_TASK => "SUTRA.COMPAT.TASK_REMOVED",
        KIND_MESSAGE_REF => "SUTRA.COMPAT.MESSAGE_REF_REMOVED",
        _ => "SUTRA.COMPAT.UNKNOWN",
    }
}

/// A breaking removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removal {
    pub file: String,
    pub process_id: String,
    pub element_kind: String,
    pub element_id: String,
}

impl Removal {
    pub fn message(&self) -> String {
        if self.element_kind == KIND_PROCESS {
            format!("process '{}' removed", self.process_id)
        } else {
            format!(
                "{} '{}' removed from process '{}'",
                self.element_kind, self.element_id, self.process_id
            )
        }
    }

    pub fn diagnostic(&self) -> Diagnostic {
        Diagnostic::error(diagnostic_code(&self.element_kind), self.message()).at(&self.file)
    }
}

/// An informational addition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addition {
    pub file: String,
    pub process_id: String,
    pub element_kind: String,
    pub element_id: String,
}

/// Result of a compatibility check. `has_breaking_change` (any removal) drives the
/// findings exit code.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompatReport {
    pub removed: Vec<Removal>,
    pub added: Vec<Addition>,
}

impl CompatReport {
    pub fn has_breaking_change(&self) -> bool {
        !self.removed.is_empty()
    }

    /// Human-readable rendering (shape pinned by this crate's tests).
    pub fn render_text(&self) -> String {
        let mut s = String::new();
        s.push_str("BPMN compatibility report\n");
        s.push_str("=========================\n");
        if self.removed.is_empty() {
            s.push_str("No breaking changes.\n");
        } else {
            s.push_str(&format!("Breaking changes ({}):\n", self.removed.len()));
            for r in &self.removed {
                s.push_str(&format!("  {}\n", r.diagnostic().render_text()));
            }
        }
        if !self.added.is_empty() {
            s.push_str(&format!(
                "\nInformational additions ({}):\n",
                self.added.len()
            ));
            for a in &self.added {
                s.push_str(&format!(
                    "  + {}: process '{}' added {} '{}'\n",
                    a.file, a.process_id, a.element_kind, a.element_id
                ));
            }
        }
        s.push('\n');
        s.push_str(if self.has_breaking_change() {
            "Result: BREAKING\n"
        } else {
            "Result: COMPATIBLE\n"
        });
        s
    }

    /// Machine-readable rendering.
    pub fn render_json(&self) -> serde_json::Value {
        serde_json::json!({
            "hasBreakingChange": self.has_breaking_change(),
            "breaking": self.removed.iter().map(|r| serde_json::json!({
                "code": diagnostic_code(&r.element_kind),
                "file": r.file,
                "processId": r.process_id,
                "elementKind": r.element_kind,
                "elementId": r.element_id,
                "message": r.message(),
            })).collect::<Vec<_>>(),
            "added": self.added.iter().map(|a| serde_json::json!({
                "file": a.file,
                "processId": a.process_id,
                "elementKind": a.element_kind,
                "elementId": a.element_id,
            })).collect::<Vec<_>>(),
        })
    }
}
