//! Output conventions shared by every command: the captured stream bundle ([`Io`]), the
//! `--format` resolution helpers, and the one diagnostic rendering
//! (`[SEVERITY] CODE — message (location)`) used for every finding the CLI prints.

use std::io::{BufRead, Write};

/// The three process streams, injectable so tests capture output exactly like the
/// production binary produces it.
pub struct Io<'a> {
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
    /// Line source for interactive commands (`explain` REPL).
    pub input: &'a mut dyn BufRead,
}

/// Report output format for text/json commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    Text,
    Json,
}

/// Resolves the global `--format` for a text/json command (`None` → text).
pub fn report_format(format: Option<&str>) -> Result<ReportFormat, String> {
    match format {
        None => Ok(ReportFormat::Text),
        Some(f) if f.eq_ignore_ascii_case("text") => Ok(ReportFormat::Text),
        Some(f) if f.eq_ignore_ascii_case("json") => Ok(ReportFormat::Json),
        Some(other) => Err(format!(
            "unsupported --format: {other} (expected text|json)"
        )),
    }
}

/// Graph renderer selection for `dispatch-graph` (`None` → dot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphFormat {
    Dot,
    Mermaid,
}

/// Resolves the global `--format` for `dispatch-graph`.
pub fn graph_format(format: Option<&str>) -> Result<GraphFormat, String> {
    match format {
        None => Ok(GraphFormat::Dot),
        Some(f) if f.eq_ignore_ascii_case("dot") => Ok(GraphFormat::Dot),
        Some(f) if f.eq_ignore_ascii_case("mermaid") => Ok(GraphFormat::Mermaid),
        Some(other) => Err(format!(
            "unsupported --format: {other} (expected dot|mermaid)"
        )),
    }
}

/// Diagnostic severity — the CLI prints findings only at these three levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warn,
    Info,
}

impl Severity {
    pub fn label(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Warn => "WARN",
            Severity::Info => "INFO",
        }
    }
}

/// One printable finding. Text form: `[SEVERITY] CODE — message (location)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: String,
    pub message: String,
    pub location: Option<String>,
}

impl Diagnostic {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Diagnostic {
            severity: Severity::Error,
            code: code.to_owned(),
            message: message.into(),
            location: None,
        }
    }

    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// The one diagnostic line shape every command prints.
    pub fn render_text(&self) -> String {
        match &self.location {
            Some(loc) => format!(
                "[{}] {} — {} ({loc})",
                self.severity.label(),
                self.code,
                self.message
            ),
            None => format!(
                "[{}] {} — {}",
                self.severity.label(),
                self.code,
                self.message
            ),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "severity": self.severity.label(),
            "code": self.code,
            "message": self.message,
            "location": self.location,
        })
    }
}

/// Test plumbing: runs `f` against captured streams (with `input` as stdin) and returns
/// `(exit_code, stdout, stderr)`.
#[cfg(test)]
pub(crate) fn run_captured(
    input: &str,
    f: impl FnOnce(&mut Io<'_>) -> i32,
) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut cursor = std::io::Cursor::new(input.as_bytes().to_vec());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut cursor,
        };
        f(&mut io)
    };
    (
        code,
        String::from_utf8(out).expect("stdout utf8"),
        String::from_utf8(err).expect("stderr utf8"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_text_shape_is_pinned() {
        let d =
            Diagnostic::error("SUTRA.COMPAT.PROCESS_REMOVED", "process 'p2' removed").at("a.bpmn");
        assert_eq!(
            d.render_text(),
            "[ERROR] SUTRA.COMPAT.PROCESS_REMOVED — process 'p2' removed (a.bpmn)"
        );
        let plain = Diagnostic::error("SUTRA.MIGRATE.LEDGER_EMPTY", "no migrations applied");
        assert_eq!(
            plain.render_text(),
            "[ERROR] SUTRA.MIGRATE.LEDGER_EMPTY — no migrations applied"
        );
    }

    #[test]
    fn format_resolution_defaults_and_rejects() {
        assert_eq!(report_format(None).unwrap(), ReportFormat::Text);
        assert_eq!(report_format(Some("JSON")).unwrap(), ReportFormat::Json);
        assert!(report_format(Some("xml")).is_err());
        assert_eq!(graph_format(None).unwrap(), GraphFormat::Dot);
        assert_eq!(graph_format(Some("mermaid")).unwrap(), GraphFormat::Mermaid);
        assert!(graph_format(Some("svg")).is_err());
    }
}
