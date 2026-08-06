//! `sutra describe` — print a reviewer-oriented structural summary of a BPMN file:
//! processes, start/end events, user/service tasks (with their extension-attribute refs),
//! gateways, channel sources and reply refs. Read-only streaming walk; never the engine
//! loader.

use std::path::PathBuf;

use crate::bpmn_walk::{attr, local_name, walk_bpmn, WalkEvent};
use crate::compat::BpmnSignature;
use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct DescribeArgs {
    /// BPMN file to describe.
    pub bpmn_file: PathBuf,
}

/// Per-service-task detail captured during the walk (attribute values verbatim).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ServiceTaskInfo {
    id: String,
    name: Option<String>,
    implementation: Option<String>,
    codec: Option<String>,
    validator: Option<String>,
    redactor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayInfo {
    id: String,
    name: Option<String>,
    kind: &'static str,
}

#[derive(Debug, Clone, Default)]
struct ProcessDetail {
    service_tasks: Vec<ServiceTaskInfo>,
    gateways: Vec<GatewayInfo>,
    channels: Vec<String>,
    replies: Vec<String>,
}

pub fn execute(args: DescribeArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "describe: {msg}");
            return exit::USAGE;
        }
    };
    if !args.bpmn_file.is_file() {
        let _ = writeln!(
            io.err,
            "describe: file not found: {}",
            args.bpmn_file.display()
        );
        return exit::USAGE;
    }
    let xml = match std::fs::read_to_string(&args.bpmn_file) {
        Ok(xml) => xml,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "describe: cannot read {}: {e}",
                args.bpmn_file.display()
            );
            return exit::USAGE;
        }
    };
    let file_path = args.bpmn_file.display().to_string();

    // Structural skeleton from the compat signature, per-element detail from a second
    // focused walk — keeps the compat signature lean.
    let signature = match BpmnSignature::extract_from_str(&file_path, &xml) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(io.err, "describe: failed to parse {file_path}: {e}");
            return exit::USAGE;
        }
    };
    let details = match walk_details(&xml) {
        Ok(d) => d,
        Err(e) => {
            let _ = writeln!(io.err, "describe: failed to parse {file_path}: {e}");
            return exit::USAGE;
        }
    };

    match format {
        ReportFormat::Text => {
            let _ = write!(io.out, "{}", render_text(&signature, &details));
        }
        ReportFormat::Json => {
            let _ = writeln!(io.out, "{}", render_json(&signature, &details));
        }
    }
    exit::OK
}

/// Second pass: gateway kinds, channel refs, reply refs and the service-task attribute
/// bundle, keyed by process id.
fn walk_details(xml: &str) -> Result<Vec<(String, ProcessDetail)>, String> {
    let mut by_process: Vec<(String, ProcessDetail)> = Vec::new();
    let mut current: Option<usize> = None;

    walk_bpmn(xml, |event| match event {
        WalkEvent::Start(e) | WalkEvent::Empty(e) => {
            let name = local_name(e);
            match name.as_str() {
                "process" => {
                    if let Some(id) = attr(e, "id") {
                        by_process.push((id, ProcessDetail::default()));
                        current = Some(by_process.len() - 1);
                    }
                }
                "serviceTask" => {
                    if let Some(idx) = current {
                        by_process[idx].1.service_tasks.push(ServiceTaskInfo {
                            id: attr(e, "id").unwrap_or_default(),
                            name: attr(e, "name"),
                            implementation: attr(e, "implementation"),
                            codec: attr(e, "codec"),
                            validator: attr(e, "validator"),
                            redactor: attr(e, "redactor"),
                        });
                    }
                }
                "exclusiveGateway" | "inclusiveGateway" | "parallelGateway" => {
                    if let Some(idx) = current {
                        let kind = match name.as_str() {
                            "exclusiveGateway" => "exclusive",
                            "inclusiveGateway" => "inclusive",
                            _ => "parallel",
                        };
                        by_process[idx].1.gateways.push(GatewayInfo {
                            id: attr(e, "id").unwrap_or_default(),
                            name: attr(e, "name"),
                            kind,
                        });
                    }
                }
                "source" => {
                    // <q:source channel="…"> — only counted when a channel attribute is
                    // present, so unrelated same-named elements stay ignored.
                    if let (Some(idx), Some(channel)) = (current, attr(e, "channel")) {
                        if !by_process[idx].1.channels.contains(&channel) {
                            by_process[idx].1.channels.push(channel);
                        }
                    }
                }
                "reply" => {
                    if let Some(idx) = current {
                        let value = attr(e, "channel")
                            .or_else(|| attr(e, "ref"))
                            .unwrap_or_else(|| "(unnamed)".to_owned());
                        if !by_process[idx].1.replies.contains(&value) {
                            by_process[idx].1.replies.push(value);
                        }
                    }
                }
                _ => {}
            }
        }
        WalkEvent::End(name) => {
            if name == "process" {
                current = None;
            }
        }
    })?;

    Ok(by_process)
}

fn render_text(sig: &BpmnSignature, details: &[(String, ProcessDetail)]) -> String {
    let empty = ProcessDetail::default();
    let mut s = String::new();
    s.push_str(&format!("File: {}\n", sig.file_path));
    s.push_str(&format!("Processes: {}\n", sig.processes.len()));
    for p in &sig.processes {
        let detail = details
            .iter()
            .find(|(id, _)| *id == p.id)
            .map(|(_, d)| d)
            .unwrap_or(&empty);
        s.push('\n');
        s.push_str(&format!("Process: {}", p.id));
        if let Some(name) = &p.name {
            s.push_str(&format!(" ({name})"));
        }
        s.push('\n');
        s.push_str(&format!(
            "  Start events: {}\n",
            format_list(&p.start_event_ids)
        ));
        s.push_str(&format!(
            "  End events:   {}\n",
            format_list(&p.end_event_ids)
        ));
        if !p.user_task_ids.is_empty() {
            s.push_str(&format!(
                "  User tasks:   {}\n",
                format_list(&p.user_task_ids)
            ));
        }
        if !p.service_task_ids.is_empty() {
            s.push_str("  Service tasks:\n");
            for id in &p.service_task_ids {
                let info = detail
                    .service_tasks
                    .iter()
                    .find(|t| t.id == *id)
                    .cloned()
                    .unwrap_or_else(|| ServiceTaskInfo {
                        id: id.clone(),
                        ..ServiceTaskInfo::default()
                    });
                s.push_str(&format!("    - {}", info.id));
                if let Some(name) = &info.name {
                    s.push_str(&format!(" ({name})"));
                }
                if let Some(v) = &info.implementation {
                    s.push_str(&format!(" impl={v}"));
                }
                if let Some(v) = &info.codec {
                    s.push_str(&format!(" codec={v}"));
                }
                if let Some(v) = &info.validator {
                    s.push_str(&format!(" validator={v}"));
                }
                if let Some(v) = &info.redactor {
                    s.push_str(&format!(" redactor={v}"));
                }
                s.push('\n');
            }
        }
        if !detail.gateways.is_empty() {
            s.push_str("  Gateways:\n");
            for g in &detail.gateways {
                s.push_str(&format!("    - {} [{}]", g.id, g.kind));
                if let Some(name) = &g.name {
                    s.push_str(&format!(" ({name})"));
                }
                s.push('\n');
            }
        }
        if !detail.channels.is_empty() {
            s.push_str(&format!("  Channels: {}\n", format_list(&detail.channels)));
        }
        if !detail.replies.is_empty() {
            s.push_str(&format!("  Replies:  {}\n", format_list(&detail.replies)));
        }
    }
    s
}

fn render_json(sig: &BpmnSignature, details: &[(String, ProcessDetail)]) -> serde_json::Value {
    let empty = ProcessDetail::default();
    let processes: Vec<serde_json::Value> = sig
        .processes
        .iter()
        .map(|p| {
            let detail = details
                .iter()
                .find(|(id, _)| *id == p.id)
                .map(|(_, d)| d)
                .unwrap_or(&empty);
            let service_tasks: Vec<serde_json::Value> = p
                .service_task_ids
                .iter()
                .map(|id| {
                    let info = detail.service_tasks.iter().find(|t| t.id == *id);
                    serde_json::json!({
                        "id": id,
                        "name": info.and_then(|t| t.name.clone()),
                        "implementation": info.and_then(|t| t.implementation.clone()),
                        "codec": info.and_then(|t| t.codec.clone()),
                        "validator": info.and_then(|t| t.validator.clone()),
                        "redactor": info.and_then(|t| t.redactor.clone()),
                    })
                })
                .collect();
            serde_json::json!({
                "id": p.id,
                "name": p.name,
                "startEvents": p.start_event_ids,
                "endEvents": p.end_event_ids,
                "userTasks": p.user_task_ids,
                "serviceTasks": service_tasks,
                "gateways": detail.gateways.iter().map(|g| serde_json::json!({
                    "id": g.id,
                    "kind": g.kind,
                    "name": g.name,
                })).collect::<Vec<_>>(),
                "channels": detail.channels,
                "replies": detail.replies,
            })
        })
        .collect();
    serde_json::json!({
        "filePath": sig.file_path,
        "processes": processes,
    })
}

fn format_list(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_owned()
    } else {
        items.join(", ")
    }
}

#[cfg(test)]
mod tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::{scratch_file, BRANCHING_BPMN, HELLO_BPMN};

    fn run(args: DescribeArgs, format: Option<&str>) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured("", |io| execute(args, &global, io))
    }

    #[test]
    fn describes_a_full_bpmn_as_text() {
        let file = scratch_file("describe", "branching.bpmn", BRANCHING_BPMN);
        let (code, out, _) = run(DescribeArgs { bpmn_file: file }, None);
        assert_eq!(code, crate::exit::OK);
        for expected in [
            "Process: branching",
            "BranchingProcess",
            "Start events: S",
            "End events:   End",
            "Service tasks:",
            "Validate",
            "impl=validate",
            "codec=json",
            "validator=schema-v1",
            "redactor=pii",
            "Gateways:",
            "GW [exclusive]",
            "Channels: branch-in",
        ] {
            assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
        }
    }

    #[test]
    fn json_format_emits_structured_output() {
        let file = scratch_file("describe", "branching.bpmn", BRANCHING_BPMN);
        let (code, out, _) = run(DescribeArgs { bpmn_file: file }, Some("json"));
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(v["filePath"].as_str().unwrap().ends_with("branching.bpmn"));
        let p = &v["processes"][0];
        assert_eq!(p["id"], "branching");
        assert_eq!(p["serviceTasks"][0]["implementation"], "validate");
        assert_eq!(p["serviceTasks"][0]["codec"], "json");
        assert_eq!(p["gateways"][0]["kind"], "exclusive");
        assert_eq!(p["channels"], serde_json::json!(["branch-in"]));
    }

    #[test]
    fn missing_file_is_a_usage_error() {
        let (code, _, err) = run(
            DescribeArgs {
                bpmn_file: PathBuf::from("/does/not/exist.bpmn"),
            },
            None,
        );
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("file not found"), "{err}");
    }

    #[test]
    fn malformed_bpmn_is_a_usage_error() {
        let file = scratch_file("describe", "bad.bpmn", "<not><closed-properly>");
        let (code, _, err) = run(DescribeArgs { bpmn_file: file }, None);
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("failed to parse"), "{err}");
    }

    #[test]
    fn unknown_format_is_a_usage_error() {
        let file = scratch_file("describe", "hello.bpmn", HELLO_BPMN);
        let (code, _, err) = run(DescribeArgs { bpmn_file: file }, Some("xml"));
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("unsupported --format"), "{err}");
    }

    #[test]
    fn text_output_snapshot_for_hello() {
        let file = scratch_file("describe", "hello.bpmn", HELLO_BPMN);
        let display = file.display().to_string();
        let (code, out, _) = run(DescribeArgs { bpmn_file: file }, None);
        assert_eq!(code, crate::exit::OK);
        let expected = format!(
            "File: {display}\n\
             Processes: 1\n\
             \n\
             Process: hello (HelloProcess)\n\
            \x20 Start events: Start\n\
            \x20 End events:   End\n\
            \x20 Service tasks:\n\
            \x20   - Greet (Say Hello)\n\
            \x20 Channels: hello-in\n"
        );
        assert_eq!(out, expected);
    }
}
