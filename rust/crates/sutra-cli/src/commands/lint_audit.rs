//! `sutra lint --audit` (print each process's audit plan) and `sutra lint --fix audit` (the
//! codemod that brings a package to the 0.9 audit model: every process declares `<q:audit>`
//! with explicit `events` and `capture`, and where audit is stored moves from the BPMN into the
//! package manifest).
//!
//! The codemod edits text, not a re-serialised tree: everything it does not touch stays byte
//! for byte, so the diff it prints is the whole change. One pass is complete and a second finds
//! nothing to do.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sutra_lint_core::audit::{audit_report, AuditFindingSeverity, AuditReport};
use sutra_loader::package::PackageConfig;
use sutra_loader::ManifestAudit;

use crate::output::{Diagnostic, Severity};

const PACKAGE_FILE: &str = "package.yaml";

/// The package's BPMN files (package-relative path → text), its `package.yaml` text, and the
/// parsed manifest.
pub(crate) struct PackageSources {
    pub(crate) bpmn: BTreeMap<String, String>,
    pub(crate) package_yaml: String,
    pub(crate) config: PackageConfig,
}

pub(crate) fn read_package(dir: &Path) -> Result<PackageSources, String> {
    let package_yaml = std::fs::read_to_string(dir.join(PACKAGE_FILE)).unwrap_or_default();
    let config = PackageConfig::parse(&package_yaml).map_err(|e| format!("{PACKAGE_FILE}: {e}"))?;
    let mut bpmn = BTreeMap::new();
    let root = dir.join("bpmn");
    let mut pending = vec![root.clone()];
    while let Some(folder) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&folder) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == "bpmn") {
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                let relative = path
                    .strip_prefix(dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                bpmn.insert(relative, text);
            }
        }
    }
    Ok(PackageSources {
        bpmn,
        package_yaml,
        config,
    })
}

/// The audit report of the package in `dir`.
pub(crate) fn report_of(dir: &Path) -> Result<AuditReport, String> {
    let sources = read_package(dir)?;
    Ok(audit_report(&sources.bpmn, sources.config.audit.as_ref()))
}

/// The report's findings as CLI diagnostics. `with_info` keeps the info-level ones.
pub(crate) fn diagnostics_of(report: &AuditReport, with_info: bool) -> Vec<Diagnostic> {
    report
        .findings
        .iter()
        .filter(|f| with_info || f.severity != AuditFindingSeverity::Info)
        .map(|f| Diagnostic {
            severity: match f.severity {
                AuditFindingSeverity::Info => Severity::Info,
                AuditFindingSeverity::Warning => Severity::Warn,
                AuditFindingSeverity::Error => Severity::Error,
            },
            code: f.code.to_string(),
            message: f.message.clone(),
            location: Some(f.file.clone()),
        })
        .collect()
}

/// `sutra lint --audit` as text: per process, the pool line and each element's disposition.
pub(crate) fn render_text(report: &AuditReport) -> String {
    let mut out = String::new();
    for process in &report.processes {
        let plan = &process.plan;
        let destination = match &process.sink {
            Some(sink) => format!(" → {sink}"),
            None if plan.declared.is_some() => " → (engine default)".to_string(),
            None => String::new(),
        };
        out.push_str(&format!(
            "{}  process {}: {}{destination}\n",
            process.file,
            plan.process_id,
            plan.pool_line()
        ));
        let width = plan.elements.keys().map(String::len).max().unwrap_or(0);
        for (id, disposition) in &plan.elements {
            let (kind, why) = match disposition {
                sutra_bpmn::ElementAuditDisposition::Desugared { rule, entered } => (
                    "audited",
                    if *entered {
                        format!("{rule} (entered and left)")
                    } else {
                        format!("{rule} (left)")
                    },
                ),
                sutra_bpmn::ElementAuditDisposition::OptedOut => {
                    ("opted out", "node q:audit capture=none".to_string())
                }
                sutra_bpmn::ElementAuditDisposition::NotAudited { reason } => {
                    ("not audited", reason.to_string())
                }
                _ => ("unknown", String::new()),
            };
            out.push_str(&format!("  {id:<width$}  {kind:<11}  {why}\n"));
        }
    }
    out
}

/// `sutra lint --audit --format json`: each process's plan exactly as
/// [`sutra_bpmn::AuditPlan::to_json`] shapes it, the files and destinations beside it.
pub(crate) fn render_json(dir: &Path, report: &AuditReport) -> serde_json::Value {
    let files: BTreeMap<&str, &str> = report
        .processes
        .iter()
        .map(|p| (p.plan.process_id.as_str(), p.file.as_str()))
        .collect();
    let sinks: BTreeMap<&str, Option<&str>> = report
        .processes
        .iter()
        .map(|p| (p.plan.process_id.as_str(), p.sink.as_deref()))
        .collect();
    serde_json::json!({
        "packageDir": dir.display().to_string(),
        "processes": report.processes.iter().map(|p| p.plan.to_json()).collect::<Vec<_>>(),
        "files": files,
        "sinks": sinks,
        "findings": report.findings.iter().map(|f| f.to_json()).collect::<Vec<_>>(),
    })
}

// ---- the codemod -------------------------------------------------------------------------------

/// What `--fix audit` changed (or, with `--dry-run`, would change).
#[derive(Debug, Default)]
pub(crate) struct FixOutcome {
    /// `(file, new text)` of every file whose text changes.
    pub(crate) changed: Vec<(PathBuf, String)>,
    /// The unified diff of every change.
    pub(crate) diff: String,
}

/// Run the codemod over the package in `dir`. `events` is the scope written where a process
/// gets or lacks one (`all` reproduces 0.8's trail).
pub(crate) fn fix_package(dir: &Path, events: &str) -> Result<FixOutcome, String> {
    let sources = read_package(dir)?;
    let manifest = sources.config.audit.clone();
    let manifest_capture = manifest
        .as_ref()
        .and_then(|a| a.capture)
        .map(|c| c.as_str())
        .unwrap_or("metadata");
    let mut outcome = FixOutcome::default();
    let mut named: Vec<(String, String)> = Vec::new();
    for (file, text) in &sources.bpmn {
        let (fixed, sinks) =
            fix_bpmn(text, events, manifest_capture).map_err(|e| format!("{file}: {e}"))?;
        named.extend(sinks);
        if fixed != *text {
            outcome.diff.push_str(&unified_diff(file, text, &fixed));
            outcome.changed.push((dir.join(file), fixed));
        }
    }

    // The manifest: the destinations the BPMN named move here, and `capture` goes.
    let mut audit = manifest.clone().unwrap_or_default();
    let manifest_sink = audit.sink.clone();
    let first = named.first().map(|(_, s)| s.clone());
    if manifest_sink.is_none()
        && !named.is_empty()
        && named.iter().all(|(_, s)| Some(s) == first.as_ref())
    {
        audit.sink = first;
    } else {
        for (pid, sink) in &named {
            if Some(sink) != manifest_sink.as_ref() {
                audit.sinks.insert(pid.clone(), sink.clone());
            }
        }
    }
    audit.capture = None;
    let empty = audit == ManifestAudit::default();
    let new_audit = match (&manifest, empty) {
        (None, true) => None,
        (Some(_), true) => None,
        _ => Some(audit),
    };
    if new_audit != manifest {
        let fixed = rewrite_audit_block(&sources.package_yaml, new_audit.as_ref());
        if fixed != sources.package_yaml {
            outcome
                .diff
                .push_str(&unified_diff(PACKAGE_FILE, &sources.package_yaml, &fixed));
            outcome.changed.push((dir.join(PACKAGE_FILE), fixed));
        }
    }
    Ok(outcome)
}

/// Fix one BPMN text: returns the new text and the `(process id, sink)` each process-level
/// `<q:audit sink>` named.
pub(crate) fn fix_bpmn(
    text: &str,
    events: &str,
    manifest_capture: &str,
) -> Result<(String, Vec<(String, String)>), String> {
    let (processes, q_prefix) = sutra_bpmn::audit_edit_sites(text).map_err(|e| e.to_string())?;
    let (q, xmlns) = match &q_prefix {
        Some(prefix) => (prefix.clone(), String::new()),
        None => ("q".to_string(), r#" xmlns:q="urn:sutra:q:1.0""#.to_string()),
    };
    // (range to replace, replacement), applied from the end so earlier offsets stay valid.
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    let mut named = Vec::new();
    for process in &processes {
        for (range, attr, value, process_level) in &process.removed_attributes {
            if *process_level && attr == "sink" {
                named.push((process.process_id.clone(), value.clone()));
            }
            edits.push((range.clone(), String::new()));
        }
        let b = if process.bpmn_prefix.is_empty() {
            String::new()
        } else {
            format!("{}:", process.bpmn_prefix)
        };
        match &process.process_audit {
            None => {
                let tag = format!(
                    r#"<{q}:audit{xmlns} events="{events}" capture="{manifest_capture}"/>"#
                );
                match &process.extension_elements {
                    Some((range, false)) => edits.push((
                        range.end..range.end,
                        format!("\n{}    {tag}", process.indent),
                    )),
                    Some((range, true)) => edits.push((
                        range.clone(),
                        format!("<{b}extensionElements>{tag}</{b}extensionElements>"),
                    )),
                    None => edits.push((
                        process.insert_child_at..process.insert_child_at,
                        format!(
                            "\n{}  <{b}extensionElements>{tag}</{b}extensionElements>",
                            process.indent
                        ),
                    )),
                }
            }
            Some((range, empty, names)) => {
                let mut added = String::new();
                if !names.iter().any(|n| n == "events") {
                    added.push_str(&format!(r#" events="{events}""#));
                }
                if !names.iter().any(|n| n == "capture") {
                    added.push_str(r#" capture="metadata""#);
                }
                if !added.is_empty() {
                    let at = if *empty { range.end - 2 } else { range.end - 1 };
                    edits.push((at..at, added));
                }
            }
        }
    }
    edits.sort_by(|a, b| b.0.start.cmp(&a.0.start).then(b.0.end.cmp(&a.0.end)));
    let mut out = text.to_string();
    for (range, replacement) in edits {
        out.replace_range(range, &replacement);
    }
    Ok((out, named))
}

/// `package.yaml` with its top-level `audit:` block replaced by `audit` (removed when `None`),
/// every other line kept as it was.
pub(crate) fn rewrite_audit_block(text: &str, audit: Option<&ManifestAudit>) -> String {
    let mut block = String::new();
    if let Some(audit) = audit {
        audit.push_yaml(&mut block);
    }
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let start = lines
        .iter()
        .position(|l| l.trim_end() == "audit:" || l.starts_with("audit:"));
    let Some(start) = start else {
        if block.is_empty() {
            return text.to_string();
        }
        let mut out = text.to_string();
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&block);
        return out;
    };
    let mut end = start + 1;
    let mut last = start + 1;
    while end < lines.len() {
        let line = lines[end];
        if line.trim().is_empty() {
            end += 1;
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            end += 1;
            last = end;
            continue;
        }
        break;
    }
    let mut out: String = lines[..start].concat();
    out.push_str(&block);
    out.push_str(&lines[last..].concat());
    out
}

/// A unified diff of `old` → `new` with three lines of context.
pub(crate) fn unified_diff(path: &str, old: &str, new: &str) -> String {
    let a: Vec<&str> = old.split_inclusive('\n').collect();
    let b: Vec<&str> = new.split_inclusive('\n').collect();
    // LCS table; packages are small enough for the quadratic form.
    let mut lcs = vec![vec![0u32; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    // The edit script: (' ' | '-' | '+', line, old index, new index).
    let mut ops: Vec<(char, &str)> = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() || j < b.len() {
        if i < a.len() && j < b.len() && a[i] == b[j] {
            ops.push((' ', a[i]));
            i += 1;
            j += 1;
        } else if j < b.len() && (i == a.len() || lcs[i][j + 1] >= lcs[i + 1][j]) {
            ops.push(('+', b[j]));
            j += 1;
        } else {
            ops.push(('-', a[i]));
            i += 1;
        }
    }
    const CONTEXT: usize = 3;
    let mut out = format!("--- a/{path}\n+++ b/{path}\n");
    let changed: Vec<usize> = (0..ops.len()).filter(|&k| ops[k].0 != ' ').collect();
    let mut k = 0;
    while k < changed.len() {
        let first = changed[k].saturating_sub(CONTEXT);
        let mut last = changed[k];
        while k + 1 < changed.len() && changed[k + 1] <= last + 2 * CONTEXT + 1 {
            k += 1;
            last = changed[k];
        }
        let last = (last + CONTEXT).min(ops.len() - 1);
        let old_start = ops[..first].iter().filter(|o| o.0 != '+').count();
        let new_start = ops[..first].iter().filter(|o| o.0 != '-').count();
        let hunk = &ops[first..=last];
        let old_len = hunk.iter().filter(|o| o.0 != '+').count();
        let new_len = hunk.iter().filter(|o| o.0 != '-').count();
        out.push_str(&format!(
            "@@ -{},{old_len} +{},{new_len} @@\n",
            old_start + 1,
            new_start + 1
        ));
        for (op, line) in hunk {
            out.push(*op);
            out.push_str(line);
            if !line.ends_with('\n') {
                out.push_str("\n\\ No newline at end of file\n");
            }
        }
        k += 1;
    }
    out
}
