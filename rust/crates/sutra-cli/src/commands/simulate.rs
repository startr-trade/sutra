//! `sutra simulate --dry-run` — routing-only dispatch report: which process (and start
//! event) an inbound message on `--channel` would route to, from the BPMN file's own
//! channel-source declarations. No execution, no stubs.
//!
//! Full fixture execution (`sutra test` / non-dry `simulate`) is deliberately not part of
//! this command: a service task is a channel call under the execution contract, so its
//! honest test double stubs *channels*, and that stub model is a design problem in its own
//! right. Invoking this command without `--dry-run` says exactly that.

use std::path::PathBuf;

use crate::bpmn_walk::{attr, local_name, walk_bpmn, WalkEvent};
use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct SimulateArgs {
    /// BPMN file to inspect.
    pub bpmn_file: PathBuf,

    /// Channel name to resolve.
    #[arg(long, value_name = "CHANNEL")]
    pub channel: String,

    /// Routing report only (required — execution is not part of this release).
    #[arg(long)]
    pub dry_run: bool,
}

/// One `<q:source channel="…">` declaration found in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChannelSource {
    channel: String,
    process_id: String,
    start_event_id: Option<String>,
}

pub fn execute(args: SimulateArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "simulate: {msg}");
            return exit::USAGE;
        }
    };
    if !args.dry_run {
        let _ = writeln!(
            io.err,
            "simulate: only --dry-run (routing report) is available in this release; \
             fixture execution with channel stubs is not implemented"
        );
        return exit::USAGE;
    }
    if !args.bpmn_file.is_file() {
        let _ = writeln!(
            io.err,
            "simulate: file not found: {}",
            args.bpmn_file.display()
        );
        return exit::USAGE;
    }
    let xml = match std::fs::read_to_string(&args.bpmn_file) {
        Ok(xml) => xml,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "simulate: cannot read {}: {e}",
                args.bpmn_file.display()
            );
            return exit::USAGE;
        }
    };
    let sources = match collect_channel_sources(&xml) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "simulate: failed to parse {}: {e}",
                args.bpmn_file.display()
            );
            return exit::USAGE;
        }
    };

    let matches: Vec<&ChannelSource> = sources
        .iter()
        .filter(|s| s.channel == args.channel)
        .collect();
    let file = args.bpmn_file.display();

    if matches.is_empty() {
        let mut available: Vec<&str> = sources.iter().map(|s| s.channel.as_str()).collect();
        available.dedup();
        let _ = writeln!(
            io.err,
            "simulate: channel '{}' is not declared in {file}",
            args.channel
        );
        let _ = writeln!(io.err, "  available channels: [{}]", available.join(", "));
        return exit::FINDINGS;
    }

    match format {
        ReportFormat::Text => {
            for m in &matches {
                let start = m
                    .start_event_id
                    .as_deref()
                    .map(|s| format!(" (start event '{s}')"))
                    .unwrap_or_default();
                let _ = writeln!(
                    io.out,
                    "Channel '{}' routes to process '{}'{start} in {file}",
                    m.channel, m.process_id
                );
            }
            if matches.len() > 1 {
                let _ = writeln!(
                    io.out,
                    "[WARN] channel '{}' is declared by {} processes — ambiguous routing",
                    args.channel,
                    matches.len()
                );
            }
            let _ = writeln!(io.out, "--dry-run: no execution");
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "channel": args.channel,
                "file": args.bpmn_file.display().to_string(),
                "dryRun": true,
                "ambiguous": matches.len() > 1,
                "routes": matches.iter().map(|m| serde_json::json!({
                    "processId": m.process_id,
                    "startEventId": m.start_event_id,
                })).collect::<Vec<_>>(),
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if matches.len() > 1 {
        exit::FINDINGS
    } else {
        exit::OK
    }
}

/// Collects `<q:source channel="…">` declarations with their enclosing process and start
/// event (channel sources live under a start event's extension elements).
fn collect_channel_sources(xml: &str) -> Result<Vec<ChannelSource>, String> {
    let mut sources = Vec::new();
    let mut current_process: Option<String> = None;
    let mut current_start_event: Option<String> = None;

    walk_bpmn(xml, |event| {
        let closes_immediately = matches!(event, WalkEvent::Empty(_));
        match event {
            WalkEvent::Start(e) | WalkEvent::Empty(e) => match local_name(e).as_str() {
                "process" => {
                    if !closes_immediately {
                        current_process = attr(e, "id");
                    }
                }
                "startEvent" => {
                    if !closes_immediately {
                        current_start_event = attr(e, "id");
                    }
                }
                "source" => {
                    if let (Some(process_id), Some(channel)) =
                        (current_process.clone(), attr(e, "channel"))
                    {
                        sources.push(ChannelSource {
                            channel,
                            process_id,
                            start_event_id: current_start_event.clone(),
                        });
                    }
                }
                _ => {}
            },
            WalkEvent::End(name) => match name.as_str() {
                "process" => current_process = None,
                "startEvent" => current_start_event = None,
                _ => {}
            },
        }
    })?;
    Ok(sources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::{scratch_file, HELLO_BPMN};

    fn scratch_bpmn(name: &str, content: &str) -> PathBuf {
        scratch_file("sim", name, content)
    }

    fn run(args: SimulateArgs) -> (i32, String, String) {
        run_captured("", |io| execute(args, &GlobalArgs::default(), io))
    }

    #[test]
    fn dry_run_reports_routing_without_executing() {
        let file = scratch_bpmn("hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(SimulateArgs {
            bpmn_file: file,
            channel: "hello-in".into(),
            dry_run: true,
        });
        assert_eq!(code, crate::exit::OK);
        assert!(
            out.contains("Channel 'hello-in' routes to process 'hello' (start event 'Start')"),
            "{out}"
        );
        assert!(out.contains("--dry-run: no execution"), "{out}");
        assert!(!out.contains("Visited:"), "{out}");
    }

    #[test]
    fn unknown_channel_is_a_finding_listing_available_channels() {
        let file = scratch_bpmn("hello2.bpmn", HELLO_BPMN);
        let (code, _, err) = run(SimulateArgs {
            bpmn_file: file,
            channel: "nope".into(),
            dry_run: true,
        });
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(err.contains("channel 'nope' is not declared"), "{err}");
        assert!(err.contains("available channels: [hello-in]"), "{err}");
    }

    #[test]
    fn missing_file_is_a_usage_error() {
        let (code, _, err) = run(SimulateArgs {
            bpmn_file: PathBuf::from("/does/not/exist.bpmn"),
            channel: "hello-in".into(),
            dry_run: true,
        });
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("file not found"), "{err}");
    }

    #[test]
    fn execution_without_dry_run_is_parked_to_r4() {
        let file = scratch_bpmn("hello3.bpmn", HELLO_BPMN);
        let (code, _, err) = run(SimulateArgs {
            bpmn_file: file,
            channel: "hello-in".into(),
            dry_run: false,
        });
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("not implemented"), "{err}");
    }

    #[test]
    fn json_route_report() {
        let file = scratch_bpmn("hello4.bpmn", HELLO_BPMN);
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let (code, out, _) = run_captured("", |io| {
            execute(
                SimulateArgs {
                    bpmn_file: file,
                    channel: "hello-in".into(),
                    dry_run: true,
                },
                &global,
                io,
            )
        });
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["routes"][0]["processId"], "hello");
        assert_eq!(v["routes"][0]["startEventId"], "Start");
        assert_eq!(v["ambiguous"], false);
    }
}
