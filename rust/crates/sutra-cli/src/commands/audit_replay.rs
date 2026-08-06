//! `sutra audit-replay <instance-id>` — walk an instance's audit events from a JSONL
//! stream (one JSON object per line: `instanceId`, `tenant`, `eventType`, `at`, `nodeId`)
//! so an operator can retrace a production run locally. Reads a file or a directory of
//! `.jsonl` files; database-backed sources ride the later admin surface.

use std::path::{Path, PathBuf};

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct AuditReplayArgs {
    /// Process instance id to replay.
    pub instance_id: String,

    /// Path to the audit JSONL file or a directory of .jsonl files.
    #[arg(long = "from-jsonl", value_name = "PATH")]
    pub from_jsonl: Option<PathBuf>,

    /// Optional tenant id filter.
    #[arg(long, value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Stop after this event type (e.g. INSTANCE_COMPLETED).
    #[arg(long, value_name = "EVENT_TYPE")]
    pub until: Option<String>,
}

#[derive(Debug, Clone)]
struct AuditEvent {
    at: String,
    event_type: String,
    node_id: Option<String>,
}

pub fn execute(args: AuditReplayArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "audit-replay: {msg}");
            return exit::USAGE;
        }
    };
    let Some(source) = &args.from_jsonl else {
        let _ = writeln!(io.err, "audit-replay: --from-jsonl <path> is required");
        return exit::USAGE;
    };
    if !source.exists() {
        let _ = writeln!(
            io.err,
            "audit-replay: audit source not found: {}",
            source.display()
        );
        return exit::USAGE;
    }
    let files = match collect_files(source) {
        Ok(f) => f,
        Err(e) => {
            let _ = writeln!(io.err, "audit-replay: failed to read audit source: {e}");
            return exit::USAGE;
        }
    };

    let mut events: Vec<AuditEvent> = Vec::new();
    let mut stopped = false;
    'files: for file in &files {
        let body = match std::fs::read_to_string(file) {
            Ok(b) => b,
            Err(e) => {
                let _ = writeln!(
                    io.err,
                    "audit-replay: failed to read {}: {e}",
                    file.display()
                );
                return exit::USAGE;
            }
        };
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                tracing::debug!("skipping unparseable audit line in {}", file.display());
                continue;
            };
            if value.get("instanceId").and_then(|v| v.as_str()) != Some(&args.instance_id) {
                continue;
            }
            if let Some(tenant) = &args.tenant {
                if value.get("tenant").and_then(|v| v.as_str()) != Some(tenant.as_str()) {
                    continue;
                }
            }
            let event = AuditEvent {
                at: value
                    .get("at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_owned(),
                event_type: value
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_owned(),
                node_id: value
                    .get("nodeId")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            };
            let hit_until = args.until.as_deref() == Some(event.event_type.as_str());
            events.push(event);
            if hit_until {
                stopped = true;
                break 'files;
            }
        }
    }

    if events.is_empty() {
        let _ = writeln!(
            io.err,
            "audit-replay: no audit events found for instance {}{}",
            args.instance_id,
            args.tenant
                .as_deref()
                .map(|t| format!(" in tenant {t}"))
                .unwrap_or_default()
        );
        return exit::FINDINGS;
    }

    match format {
        ReportFormat::Text => {
            for event in &events {
                let _ = writeln!(
                    io.out,
                    "{}  {:<28} node={}",
                    event.at,
                    event.event_type,
                    event.node_id.as_deref().unwrap_or("-")
                );
            }
            if stopped {
                let _ = writeln!(
                    io.out,
                    "(stopped at --until={})",
                    args.until.as_deref().unwrap_or_default()
                );
            } else {
                let _ = writeln!(io.out, "({} events shown)", events.len());
            }
        }
        ReportFormat::Json => {
            let stopped_at = if stopped { args.until.clone() } else { None };
            let payload = serde_json::json!({
                "instanceId": args.instance_id,
                "tenant": args.tenant,
                "stoppedAt": stopped_at,
                "events": events.iter().map(|e| serde_json::json!({
                    "at": e.at,
                    "eventType": e.event_type,
                    "nodeId": e.node_id,
                })).collect::<Vec<_>>(),
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

fn collect_files(source: &Path) -> Result<Vec<PathBuf>, String> {
    if !source.is_dir() {
        return Ok(vec![source.to_owned()]);
    }
    let mut files = Vec::new();
    walk(source, &mut files)?;
    files.sort();
    Ok(files)
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("cannot read entry in {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;
    use crate::output::run_captured;

    fn scratch_file(name: &str, content: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sutra-audit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn run(args: AuditReplayArgs) -> (i32, String, String) {
        run_captured("", |io| execute(args, &GlobalArgs::default(), io))
    }

    #[test]
    fn happy_path_walks_two_events_for_one_instance() {
        let jsonl = scratch_file(
            "happy.jsonl",
            concat!(
                "{\"instanceId\":\"inst-1\",\"tenant\":\"acme\",\"eventType\":\"INSTANCE_STARTED\",\"at\":\"2026-05-20T10:00:00Z\",\"nodeId\":\"start\"}\n",
                "{\"instanceId\":\"inst-1\",\"tenant\":\"acme\",\"eventType\":\"INSTANCE_COMPLETED\",\"at\":\"2026-05-20T10:00:01Z\",\"nodeId\":\"end\"}\n",
            ),
        );
        let (code, out, _) = run(AuditReplayArgs {
            instance_id: "inst-1".into(),
            from_jsonl: Some(jsonl),
            tenant: None,
            until: None,
        });
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("INSTANCE_STARTED"), "{out}");
        assert!(out.contains("INSTANCE_COMPLETED"), "{out}");
        assert!(out.contains("(2 events shown)"), "{out}");
    }

    #[test]
    fn missing_source_is_a_usage_error() {
        let (code, _, err) = run(AuditReplayArgs {
            instance_id: "inst-1".into(),
            from_jsonl: Some(PathBuf::from("/does/not/exist.jsonl")),
            tenant: None,
            until: None,
        });
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("audit source not found"), "{err}");
    }

    #[test]
    fn no_matching_events_is_a_finding() {
        let jsonl = scratch_file(
            "nomatch.jsonl",
            "{\"instanceId\":\"other-inst\",\"tenant\":\"acme\",\"eventType\":\"INSTANCE_STARTED\",\"at\":\"now\"}\n",
        );
        let (code, _, err) = run(AuditReplayArgs {
            instance_id: "inst-1".into(),
            from_jsonl: Some(jsonl),
            tenant: None,
            until: None,
        });
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(
            err.contains("no audit events found for instance inst-1"),
            "{err}"
        );
    }

    #[test]
    fn until_stops_at_the_named_event_type() {
        let jsonl = scratch_file(
            "until.jsonl",
            concat!(
                "{\"instanceId\":\"inst-2\",\"eventType\":\"INSTANCE_STARTED\",\"at\":\"t1\",\"nodeId\":\"start\"}\n",
                "{\"instanceId\":\"inst-2\",\"eventType\":\"TASK_INVOKED\",\"at\":\"t2\",\"nodeId\":\"task-a\"}\n",
                "{\"instanceId\":\"inst-2\",\"eventType\":\"INSTANCE_COMPLETED\",\"at\":\"t3\",\"nodeId\":\"end\"}\n",
            ),
        );
        let (code, out, _) = run(AuditReplayArgs {
            instance_id: "inst-2".into(),
            from_jsonl: Some(jsonl),
            tenant: None,
            until: Some("TASK_INVOKED".into()),
        });
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("INSTANCE_STARTED"), "{out}");
        assert!(out.contains("TASK_INVOKED"), "{out}");
        assert!(!out.contains("INSTANCE_COMPLETED"), "{out}");
        assert!(out.contains("(stopped at --until=TASK_INVOKED)"), "{out}");
    }

    #[test]
    fn tenant_filter_excludes_other_tenants() {
        let jsonl = scratch_file(
            "tenant.jsonl",
            concat!(
                "{\"instanceId\":\"inst-3\",\"tenant\":\"acme\",\"eventType\":\"INSTANCE_STARTED\",\"at\":\"t1\"}\n",
                "{\"instanceId\":\"inst-3\",\"tenant\":\"umbrella\",\"eventType\":\"INSTANCE_STARTED\",\"at\":\"t1\"}\n",
            ),
        );
        let (code, out, _) = run(AuditReplayArgs {
            instance_id: "inst-3".into(),
            from_jsonl: Some(jsonl),
            tenant: Some("acme".into()),
            until: None,
        });
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("(1 events shown)"), "{out}");
    }
}
