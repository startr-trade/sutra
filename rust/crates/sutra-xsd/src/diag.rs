//! Diagnostics: severities, source positions, instance violations, schema-compile
//! findings, and the stable diagnostic-code strings consumers map violations onto.
//!
//! The behavioural contract is **semantics, never
//! message prose**: collect-all violations with line:col locations, ERROR/WARNING
//! severity, soft-error posture. Message wording is free.

use std::fmt;

/// Stable diagnostic-code strings for the surfaces this crate itself serves. The
/// validator emits [`Violation`]s without a code; the consuming codec picks the codes for
/// its surface by handing in a [`DiagnosticProfile`]. An extension codec supplies its own
/// pair — this crate names no message standard.
pub mod codes {
    /// Module `schemaKind: xsd` codec violations (the engine-runtime XSD surface).
    pub const MODULE_SCHEMA_VIOLATION: &str = "SUTRA.PARSE.XSD.SCHEMA_VIOLATION";
    /// Module `schemaKind: xsd` codec: a document namespace with no compiled schema — a
    /// soft "could-not-validate", not a reject.
    pub const MODULE_SCHEMA_NOT_FOUND: &str = "SUTRA.PARSE.XSD.SCHEMA_NOT_FOUND";
}

/// ERROR / WARNING — the two severities instance violations carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

impl Severity {
    /// The wire string (`ERROR` / `WARNING`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Warning => "WARNING",
        }
    }
}

/// A 1-based line:column source position. Columns count Unicode scalar values from the
/// start of the line, and a violation is positioned just *after* the tag it is reported on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePos {
    pub line: u32,
    pub column: u32,
}

impl fmt::Display for SourcePos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

/// One instance-validation violation. Collect-all: validation never stops at the first
/// violation; the full list comes back and the payload stays routable (SOFT_ERRORS —
/// the outcome mapping itself belongs to the consuming codec).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub severity: Severity,
    pub pos: SourcePos,
    pub message: String,
}

impl Violation {
    pub(crate) fn error(pos: SourcePos, message: impl Into<String>) -> Violation {
        Violation {
            severity: Severity::Error,
            pos,
            message: message.into(),
        }
    }
}

/// A ready-to-emit diagnostic in the slot layout this crate renders.
/// See [`Violation::diagnostic`] and [`schema_not_found`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable `SUTRA.*` code string, taken from the caller's [`DiagnosticProfile`].
    pub code: String,
    pub severity: Severity,
    /// Path slot — carries the `line N:M` location for a violation, the document root
    /// path for a [`schema_not_found`].
    pub path: String,
    pub message: String,
    /// Value slot — the offending namespace on a [`schema_not_found`], empty otherwise.
    pub value: Option<String>,
}

/// The code pair one consumer surface emits this crate's findings under. The validator is
/// standard-agnostic, so the codes are the caller's: the engine-runtime module-codec
/// surface uses [`DiagnosticProfile::MODULE_CODEC`], and an extension codec constructs its
/// own profile from its own published code strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticProfile {
    /// Code for an instance violation collected by [`crate::Schema::validate`].
    pub violation: &'static str,
    /// Code for "no compiled schema covers this document" (soft, never a reject).
    pub not_found: &'static str,
}

impl DiagnosticProfile {
    /// The engine-runtime module `schemaKind: xsd` surface (`SUTRA.PARSE.XSD.*`).
    pub const MODULE_CODEC: DiagnosticProfile = DiagnosticProfile {
        violation: codes::MODULE_SCHEMA_VIOLATION,
        not_found: codes::MODULE_SCHEMA_NOT_FOUND,
    };
}

impl Violation {
    /// Render this violation under `profile`'s violation code: location in the path slot
    /// as `line N:M`, message prose verbatim.
    pub fn diagnostic(&self, profile: DiagnosticProfile) -> Diagnostic {
        Diagnostic {
            code: profile.violation.to_string(),
            severity: self.severity,
            path: format!("line {}:{}", self.pos.line, self.pos.column),
            message: self.message.clone(),
            value: None,
        }
    }
}

/// The "could-not-validate" diagnostic: a document namespace no compiled schema covers.
/// Soft (still an ERROR severity, routable outcome) — never a reject.
pub fn schema_not_found(
    profile: DiagnosticProfile,
    namespace: &str,
    root_local_name: &str,
) -> Diagnostic {
    Diagnostic {
        code: profile.not_found.to_string(),
        severity: Severity::Error,
        path: format!("/{root_local_name}"),
        message: format!(
            "No compiled schema matches namespace '{namespace}'; structural validation skipped."
        ),
        value: Some(namespace.to_string()),
    }
}

/// One schema-compile finding: an unsupported construct, an unresolved reference, or a
/// malformed schema document. Compilation collects all findings before failing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileFinding {
    pub pos: SourcePos,
    pub message: String,
}

/// Schema compilation failed. Doubles as the module-codec authoring contract: every
/// finding names the construct and states it is not in the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileError {
    pub findings: Vec<CompileFinding>,
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "schema does not compile ({} finding(s))",
            self.findings.len()
        )?;
        for finding in &self.findings {
            write!(f, "\n  {} {}", finding.pos, finding.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for CompileError {}

/// The instance document is not usable at all (malformed XML, DOCTYPE, no document
/// element). The consumer maps this to its FATAL parse code — well-formedness is the
/// format layer's failure, distinct from soft schema violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentError {
    pub pos: Option<SourcePos>,
    pub message: String,
}

impl fmt::Display for DocumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.pos {
            Some(pos) => write!(f, "{} {}", pos, self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for DocumentError {}

/// Byte-offset → line:column index over one source text. Columns count Unicode scalar
/// values (multi-byte UTF-8 sequences are one column).
pub(crate) struct SourceMap<'a> {
    text: &'a [u8],
    line_starts: Vec<usize>,
}

impl<'a> SourceMap<'a> {
    pub(crate) fn new(text: &'a [u8]) -> SourceMap<'a> {
        let mut line_starts = vec![0usize];
        for (i, b) in text.iter().enumerate() {
            if *b == b'\n' {
                line_starts.push(i + 1);
            }
        }
        SourceMap { text, line_starts }
    }

    pub(crate) fn pos(&self, offset: usize) -> SourcePos {
        let offset = offset.min(self.text.len());
        let idx = match self.line_starts.binary_search(&offset) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        let line_start = self.line_starts[idx];
        let column = String::from_utf8_lossy(&self.text[line_start..offset])
            .chars()
            .count()
            + 1;
        SourcePos {
            line: (idx + 1) as u32,
            column: column as u32,
        }
    }
}
