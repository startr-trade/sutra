//! Diffs two signature sets keyed by file path. Removed processes, start/end events,
//! named tasks and start-event message refs are breaking; additions are informational.
//! Iteration follows baseline/current document order, so reports are deterministic.

use super::report::{
    Addition, CompatReport, Removal, KIND_END_EVENT, KIND_MESSAGE_REF, KIND_PROCESS,
    KIND_SCRIPT_TASK, KIND_SERVICE_TASK, KIND_START_EVENT, KIND_USER_TASK,
};
use super::signature::{BpmnSignature, ProcessSignature};

/// Compare `baseline` signatures to `current` signatures. Files pair by their
/// (relative) `file_path`; per-file processes pair by process id.
pub fn check(baseline: &[BpmnSignature], current: &[BpmnSignature]) -> CompatReport {
    let mut report = CompatReport::default();

    for base_sig in baseline {
        let Some(cur_sig) = current.iter().find(|c| c.file_path == base_sig.file_path) else {
            // Entire file missing — every baseline process counts as removed.
            for p in &base_sig.processes {
                report.removed.push(Removal {
                    file: base_sig.file_path.clone(),
                    process_id: p.id.clone(),
                    element_kind: KIND_PROCESS.to_owned(),
                    element_id: p.id.clone(),
                });
            }
            continue;
        };

        for base_proc in &base_sig.processes {
            let Some(cur_proc) = cur_sig.processes.iter().find(|p| p.id == base_proc.id) else {
                report.removed.push(Removal {
                    file: base_sig.file_path.clone(),
                    process_id: base_proc.id.clone(),
                    element_kind: KIND_PROCESS.to_owned(),
                    element_id: base_proc.id.clone(),
                });
                continue;
            };
            diff_kind(&mut report, base_sig, base_proc, cur_proc, KIND_START_EVENT);
            diff_kind(&mut report, base_sig, base_proc, cur_proc, KIND_END_EVENT);
            diff_kind(&mut report, base_sig, base_proc, cur_proc, KIND_USER_TASK);
            diff_kind(
                &mut report,
                base_sig,
                base_proc,
                cur_proc,
                KIND_SERVICE_TASK,
            );
            diff_kind(&mut report, base_sig, base_proc, cur_proc, KIND_SCRIPT_TASK);
            diff_kind(&mut report, base_sig, base_proc, cur_proc, KIND_MESSAGE_REF);
        }

        // New processes in an existing file — informational only.
        for cur_proc in &cur_sig.processes {
            if !base_sig.processes.iter().any(|p| p.id == cur_proc.id) {
                report.added.push(Addition {
                    file: cur_sig.file_path.clone(),
                    process_id: cur_proc.id.clone(),
                    element_kind: KIND_PROCESS.to_owned(),
                    element_id: cur_proc.id.clone(),
                });
            }
        }
    }

    // New files — informational only.
    for cur_sig in current {
        if !baseline.iter().any(|b| b.file_path == cur_sig.file_path) {
            for p in &cur_sig.processes {
                report.added.push(Addition {
                    file: cur_sig.file_path.clone(),
                    process_id: p.id.clone(),
                    element_kind: KIND_PROCESS.to_owned(),
                    element_id: p.id.clone(),
                });
            }
        }
    }

    report
}

fn ids_of<'a>(p: &'a ProcessSignature, kind: &str) -> &'a [String] {
    match kind {
        KIND_START_EVENT => &p.start_event_ids,
        KIND_END_EVENT => &p.end_event_ids,
        KIND_USER_TASK => &p.user_task_ids,
        KIND_SERVICE_TASK => &p.service_task_ids,
        KIND_SCRIPT_TASK => &p.script_task_ids,
        KIND_MESSAGE_REF => &p.referenced_message_ids,
        _ => &[],
    }
}

fn diff_kind(
    report: &mut CompatReport,
    sig: &BpmnSignature,
    base: &ProcessSignature,
    cur: &ProcessSignature,
    kind: &str,
) {
    let base_ids = ids_of(base, kind);
    let cur_ids = ids_of(cur, kind);
    for id in base_ids {
        if !cur_ids.contains(id) {
            report.removed.push(Removal {
                file: sig.file_path.clone(),
                process_id: base.id.clone(),
                element_kind: kind.to_owned(),
                element_id: id.clone(),
            });
        }
    }
    for id in cur_ids {
        if !base_ids.contains(id) {
            report.added.push(Addition {
                file: sig.file_path.clone(),
                process_id: base.id.clone(),
                element_kind: kind.to_owned(),
                element_id: id.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;

    fn sig(path: &str, processes: Vec<ProcessSignature>) -> BpmnSignature {
        BpmnSignature {
            file_path: path.to_owned(),
            processes,
        }
    }

    fn pr(id: &str) -> ProcessSignature {
        ProcessSignature {
            id: id.to_owned(),
            start_event_ids: vec![format!("s-{id}")],
            end_event_ids: vec![format!("e-{id}")],
            ..ProcessSignature::default()
        }
    }

    #[test]
    fn identical_signatures_are_compatible() {
        let a = sig("a.bpmn", vec![pr("p1")]);
        let report = check(std::slice::from_ref(&a), std::slice::from_ref(&a));
        assert!(!report.has_breaking_change());
        assert!(report.removed.is_empty());
        assert!(report.added.is_empty());
    }

    #[test]
    fn removing_a_process_is_breaking() {
        let base = sig("a.bpmn", vec![pr("p1"), pr("p2")]);
        let cur = sig("a.bpmn", vec![pr("p1")]);
        let report = check(&[base], &[cur]);
        assert!(report.has_breaking_change());
        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].element_kind, KIND_PROCESS);
        assert_eq!(report.removed[0].element_id, "p2");
    }

    #[test]
    fn removing_a_start_event_is_breaking() {
        let mut base_p = pr("p1");
        base_p.start_event_ids = vec!["s1".into(), "s2".into()];
        let mut cur_p = pr("p1");
        cur_p.start_event_ids = vec!["s1".into()];
        let report = check(
            &[sig("a.bpmn", vec![base_p])],
            &[sig("a.bpmn", vec![cur_p])],
        );
        assert!(report.has_breaking_change());
        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].element_kind, KIND_START_EVENT);
        assert_eq!(report.removed[0].element_id, "s2");
    }

    #[test]
    fn renaming_a_user_task_counts_as_remove_and_add() {
        let mut base_p = pr("p1");
        base_p.user_task_ids = vec!["oldName".into()];
        let mut cur_p = pr("p1");
        cur_p.user_task_ids = vec!["newName".into()];
        let report = check(
            &[sig("a.bpmn", vec![base_p])],
            &[sig("a.bpmn", vec![cur_p])],
        );
        assert!(report.has_breaking_change());
        assert_eq!(report.removed[0].element_id, "oldName");
        assert_eq!(report.added[0].element_id, "newName");
    }

    #[test]
    fn adding_a_new_process_is_informational_only() {
        let base = sig("a.bpmn", vec![pr("p1")]);
        let cur = sig("a.bpmn", vec![pr("p1"), pr("p2-new")]);
        let report = check(&[base], &[cur]);
        assert!(!report.has_breaking_change());
        assert!(report.removed.is_empty());
        assert_eq!(report.added.len(), 1);
        assert_eq!(report.added[0].element_kind, KIND_PROCESS);
        assert_eq!(report.added[0].element_id, "p2-new");
    }

    #[test]
    fn removing_a_message_ref_is_breaking() {
        let mut base_p = pr("p1");
        base_p.referenced_message_ids = vec!["msgA".into()];
        let cur_p = pr("p1");
        let report = check(
            &[sig("a.bpmn", vec![base_p])],
            &[sig("a.bpmn", vec![cur_p])],
        );
        assert!(report.has_breaking_change());
        assert_eq!(report.removed[0].element_kind, KIND_MESSAGE_REF);
        assert_eq!(report.removed[0].element_id, "msgA");
    }

    #[test]
    fn missing_file_flags_all_baseline_processes_as_removed() {
        let base = sig("removed.bpmn", vec![pr("p1"), pr("p2")]);
        let report = check(&[base], &[]);
        assert!(report.has_breaking_change());
        let ids: Vec<&str> = report
            .removed
            .iter()
            .map(|r| r.element_id.as_str())
            .collect();
        assert_eq!(ids, vec!["p1", "p2"]);
    }

    #[test]
    fn json_rendering_contains_breaking_array_with_codes() {
        let mut base_p = pr("p1");
        base_p.user_task_ids = vec!["t1".into()];
        let cur_p = pr("p1");
        let report = check(
            &[sig("a.bpmn", vec![base_p])],
            &[sig("a.bpmn", vec![cur_p])],
        );
        let json = report.render_json().to_string();
        assert!(json.contains("\"hasBreakingChange\":true"), "{json}");
        assert!(json.contains("SUTRA.COMPAT.TASK_REMOVED"), "{json}");
        assert!(json.contains("\"breaking\":"), "{json}");
    }

    #[test]
    fn all_five_frozen_codes_map() {
        use super::super::report::diagnostic_code;
        assert_eq!(diagnostic_code("process"), "SUTRA.COMPAT.PROCESS_REMOVED");
        assert_eq!(
            diagnostic_code("startEvent"),
            "SUTRA.COMPAT.START_EVENT_REMOVED"
        );
        assert_eq!(
            diagnostic_code("endEvent"),
            "SUTRA.COMPAT.END_EVENT_REMOVED"
        );
        for task_kind in ["userTask", "serviceTask", "scriptTask"] {
            assert_eq!(diagnostic_code(task_kind), "SUTRA.COMPAT.TASK_REMOVED");
        }
        assert_eq!(
            diagnostic_code("messageRef"),
            "SUTRA.COMPAT.MESSAGE_REF_REMOVED"
        );
        assert_eq!(diagnostic_code("other"), "SUTRA.COMPAT.UNKNOWN");
    }
}
