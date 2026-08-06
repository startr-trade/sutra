//! `sutra deployments list` — enumerate the deployments in a directory of sealed `.sutra`
//! archives — the deployments source the engine loads — printing each deployment's
//! content-addressed id and its manifest labels. Read-only: every archive is opened
//! through the same fail-closed verifying reader the engine uses
//! ([`sutra_loader::read_archive_file`]); a file that fails to verify is warned and skipped
//! so one bad archive never hides the rest of the directory.
//!
//! Labels are OPAQUE observability data by contract — the engine never interprets them; this
//! command surfaces them for operators and offers repeatable `--label KEY=VALUE` selectors
//! (AND across all filters) so a listing can be narrowed to one tenant / environment / etc.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct DeploymentsArgs {
    #[command(subcommand)]
    pub action: DeploymentsAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum DeploymentsAction {
    /// List each `.sutra` archive's deployment id and labels (optionally filtered).
    List(ListArgs),
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// Directory of packaged `.sutra` archives to enumerate (the deployments source).
    pub dir: PathBuf,

    /// Keep only deployments whose labels contain KEY=VALUE (repeatable; AND across all).
    #[arg(long = "label", value_name = "KEY=VALUE")]
    pub label: Vec<String>,
}

/// One archive's listable facts: its content-addressed identity plus the opaque manifest
/// metadata.
struct Listed {
    id: String,
    labels: BTreeMap<String, String>,
    supersedes: Vec<String>,
}

pub fn execute(args: DeploymentsArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "deployments: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        DeploymentsAction::List(a) => list(a, format, io),
    }
}

fn list(args: ListArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let filters = match parse_label_filters(&args.label) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "deployments list: {msg}");
            return exit::USAGE;
        }
    };
    if !args.dir.is_dir() {
        let _ = writeln!(
            io.err,
            "deployments list: directory not found: {}",
            args.dir.display()
        );
        return exit::USAGE;
    }

    // Collect the archive files up front, sorted by file name — a stable, source-order
    // listing independent of the filesystem's directory-iteration order.
    let read_dir = match std::fs::read_dir(&args.dir) {
        Ok(rd) => rd,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "deployments list: cannot read {}: {e}",
                args.dir.display()
            );
            return exit::USAGE;
        }
    };
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in read_dir {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                let _ = writeln!(
                    io.err,
                    "deployments list: cannot read {}: {e}",
                    args.dir.display()
                );
                return exit::USAGE;
            }
        };
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str())
            == Some(sutra_loader::archive::ARCHIVE_EXTENSION)
        {
            files.push(path);
        }
    }
    files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Verify + read each archive through the engine's own reader; a rejected archive
    // is warned and skipped (fail-closed per archive — the directory still lists).
    let mut listed: Vec<Listed> = Vec::new();
    for path in &files {
        match sutra_loader::read_archive_file(path) {
            Ok(archive) => {
                if matches_filters(&archive.manifest.labels, &filters) {
                    listed.push(Listed {
                        id: archive.id.value().to_string(),
                        labels: archive.manifest.labels,
                        supersedes: archive.manifest.supersedes,
                    });
                }
            }
            Err(e) => {
                let _ = writeln!(io.err, "deployments list: skipping {}: {e}", path.display());
            }
        }
    }

    match format {
        ReportFormat::Text => {
            for d in &listed {
                if d.labels.is_empty() {
                    let _ = writeln!(io.out, "{}", d.id);
                } else {
                    let labels = d
                        .labels
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let _ = writeln!(io.out, "{}  {labels}", d.id);
                }
            }
        }
        ReportFormat::Json => {
            let payload: Vec<serde_json::Value> = listed
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "id": d.id,
                        "labels": d.labels,
                        "supersedes": d.supersedes,
                    })
                })
                .collect();
            let _ = writeln!(io.out, "{}", serde_json::Value::Array(payload));
        }
    }
    exit::OK
}

/// Split every `--label KEY=VALUE` at its first `=`. An entry without a non-empty key and a
/// `=` is a usage error (the whole invocation is refused — a filter that never matches is a
/// mistake, not an empty result).
fn parse_label_filters(raw: &[String]) -> Result<Vec<(String, String)>, String> {
    raw.iter()
        .map(|item| match item.split_once('=') {
            Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
            _ => Err(format!("invalid --label '{item}' (expected KEY=VALUE)")),
        })
        .collect()
}

/// AND semantics: every filter's key must be present with exactly its value.
fn matches_filters(labels: &BTreeMap<String, String>, filters: &[(String, String)]) -> bool {
    filters
        .iter()
        .all(|(key, value)| labels.get(key).map(String::as_str) == Some(value.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_filters_parse_key_value_pairs() {
        let parsed = parse_label_filters(&["env=prod".into(), "tenant=acme".into()]).unwrap();
        assert_eq!(
            parsed,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("tenant".to_string(), "acme".to_string()),
            ]
        );
        // A value may itself contain '=' (split at the first one only).
        let parsed = parse_label_filters(&["url=a=b".into()]).unwrap();
        assert_eq!(parsed, vec![("url".to_string(), "a=b".to_string())]);
    }

    #[test]
    fn label_filter_without_equals_is_rejected() {
        assert!(parse_label_filters(&["prod".into()]).is_err());
        assert!(parse_label_filters(&["=prod".into()]).is_err());
    }

    #[test]
    fn and_semantics_across_filters() {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("tenant".to_string(), "acme".to_string());
        assert!(matches_filters(&labels, &[("env".into(), "prod".into())]));
        assert!(matches_filters(
            &labels,
            &[
                ("env".into(), "prod".into()),
                ("tenant".into(), "acme".into())
            ]
        ));
        // One mismatching filter fails the whole conjunction.
        assert!(!matches_filters(
            &labels,
            &[
                ("env".into(), "prod".into()),
                ("tenant".into(), "globex".into())
            ]
        ));
        // A missing key never matches.
        assert!(!matches_filters(&labels, &[("region".into(), "eu".into())]));
    }
}
