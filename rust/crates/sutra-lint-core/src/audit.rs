//! The audit report of a package: for each process, its audit plan (which element records
//! what, and why), and the findings about its audit declarations. I/O-free, like the rest of
//! this crate: the caller hands in the BPMN texts and the manifest's `audit` block.

use std::collections::BTreeMap;

use sutra_bpmn::qbindings::AuditCapture;
use sutra_bpmn::{AuditPlan, BpmnModelLoader};
use sutra_loader::ManifestAudit;

/// A process declares no `<q:audit>`, so it is not audited (info).
pub const LINT_AUDIT_NOT_DECLARED: &str = "SUTRA.LINT.AUDIT.NOT_DECLARED";
/// A process's only `<q:audit>` carries just `version`, which now also turns audit on (warning).
pub const LINT_AUDIT_VERSION_PIN_AUDITS: &str = "SUTRA.LINT.AUDIT.VERSION_PIN_AUDITS";
/// A process captures payloads (or media references) into the JSONL file store, whose
/// append-only files cannot be redacted later (warning).
pub const LINT_AUDIT_PAYLOAD_IN_FILE_STORE: &str = "SUTRA.LINT.AUDIT.PAYLOAD_IN_FILE_STORE";
/// A `<q:audit>` carries the removed `sink` / `target` (error; the BPMN does not load).
pub const LINT_AUDIT_SINK_REMOVED: &str = "SUTRA.LINT.AUDIT.SINK_REMOVED";

/// How serious an audit finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditFindingSeverity {
    Info,
    Warning,
    Error,
}

impl AuditFindingSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            AuditFindingSeverity::Info => "info",
            AuditFindingSeverity::Warning => "warning",
            AuditFindingSeverity::Error => "error",
        }
    }
}

/// One finding about a package's audit declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditFinding {
    pub severity: AuditFindingSeverity,
    pub code: &'static str,
    pub message: String,
    /// The BPMN file (package-relative) the finding is about.
    pub file: String,
    pub process_id: Option<String>,
}

impl AuditFinding {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "severity": self.severity.as_str(),
            "code": self.code,
            "message": self.message,
            "file": self.file,
            "processId": self.process_id,
        })
    }
}

/// One process's plan, with the file it comes from and the destination its manifest names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessAuditReport {
    pub file: String,
    pub plan: AuditPlan,
    /// The store the manifest routes it to (`audit.sinks`, then `audit.sink`), if any. The
    /// engine may still default it at deploy.
    pub sink: Option<String>,
}

/// The audit report of a package.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditReport {
    pub processes: Vec<ProcessAuditReport>,
    pub findings: Vec<AuditFinding>,
}

impl AuditReport {
    pub fn has_errors(&self) -> bool {
        self.findings
            .iter()
            .any(|f| f.severity == AuditFindingSeverity::Error)
    }
}

/// Build the audit report of the BPMN files `bpmn` (package-relative path → XML text) under
/// the manifest's `audit` block.
pub fn audit_report(
    bpmn: &BTreeMap<String, String>,
    manifest: Option<&ManifestAudit>,
) -> AuditReport {
    let mut report = AuditReport::default();
    let destinations = manifest
        .map(ManifestAudit::destinations)
        .unwrap_or_default();
    for (file, xml) in bpmn {
        let sources = sutra_bpmn::scan_audit_sources(xml.as_bytes()).unwrap_or_default();
        let mut removed = false;
        for source in &sources {
            for (host, attr, value) in &source.removed_attributes {
                removed = true;
                report.findings.push(AuditFinding {
                    severity: AuditFindingSeverity::Error,
                    code: LINT_AUDIT_SINK_REMOVED,
                    message: format!(
                        "'{host}': q:audit {attr}=\"{value}\" was removed in 0.9; where audit is \
                         stored is set in the package manifest. Run `sutra lint --fix audit`"
                    ),
                    file: file.clone(),
                    process_id: Some(source.process_id.clone()),
                });
            }
            if source.is_version_only() {
                report.findings.push(AuditFinding {
                    severity: AuditFindingSeverity::Warning,
                    code: LINT_AUDIT_VERSION_PIN_AUDITS,
                    message: format!(
                        "process '{}': use `versionTag`; this tag now also turns audit on",
                        source.process_id
                    ),
                    file: file.clone(),
                    process_id: Some(source.process_id.clone()),
                });
            }
        }
        if removed {
            // The loader refuses the document; there is no plan to show until it is fixed.
            continue;
        }
        let Ok(module) = BpmnModelLoader::new().load(xml.as_bytes()) else {
            // A document that does not load is the ordinary lint's to report.
            continue;
        };
        for process in module.processes() {
            let plan = process.audit_plan().clone();
            let sink = destinations
                .per_process
                .get(&process.id)
                .or(destinations.default.as_ref())
                .cloned();
            match plan.declared {
                None => report.findings.push(AuditFinding {
                    severity: AuditFindingSeverity::Info,
                    code: LINT_AUDIT_NOT_DECLARED,
                    message: format!(
                        "process '{}' declares no <q:audit>: not audited (add <q:audit/> or run \
                         `sutra lint --fix audit`)",
                        process.id
                    ),
                    file: file.clone(),
                    process_id: Some(process.id.clone()),
                }),
                Some((_, capture))
                    if matches!(capture, AuditCapture::Payload | AuditCapture::Media)
                        && sink.as_deref() == Some("jsonl") =>
                {
                    report.findings.push(AuditFinding {
                        severity: AuditFindingSeverity::Warning,
                        code: LINT_AUDIT_PAYLOAD_IN_FILE_STORE,
                        message: format!(
                            "process '{}' captures {} into the jsonl file store: payloads in the \
                             file store cannot be redacted later",
                            process.id,
                            capture.as_str()
                        ),
                        file: file.clone(),
                        process_id: Some(process.id.clone()),
                    })
                }
                Some(_) => {}
            }
            report.processes.push(ProcessAuditReport {
                file: file.clone(),
                plan,
                sink,
            });
        }
    }
    report
}
