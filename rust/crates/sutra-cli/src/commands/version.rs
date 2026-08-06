//! `version` — print the tool version, the same rendering as `--version`.
//!
//! Both halves are derived, not hardcoded: the name is the running binary's
//! ([`crate::program_name`]) and the version is whatever the distribution reported through
//! [`crate::run_with_version`], defaulting to the engine's own [`crate::VERSION`]. A
//! distribution that versions itself independently therefore prints its own product line here
//! and in `--version` identically.

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, Default, clap::Args)]
pub struct VersionArgs {}

pub fn execute(_args: VersionArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "version: {msg}");
            return exit::USAGE;
        }
    };
    match format {
        ReportFormat::Text => {
            // Byte-identical to what clap prints for `--version`, multi-line block included.
            let _ = writeln!(
                io.out,
                "{} {}",
                crate::program_name(),
                crate::version_string()
            );
        }
        ReportFormat::Json => {
            // Structured, so a script never has to split a version block: `version` is the
            // product's own (the first line), `engine` always the embedded engine's.
            let payload = serde_json::json!({
                "name": crate::program_name(),
                "version": crate::version_string().lines().next().unwrap_or_default(),
                "engine": crate::VERSION,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;

    #[test]
    fn prints_the_program_name_and_the_reported_version() {
        let (code, out, _) = run_captured("", |io| {
            execute(VersionArgs::default(), &GlobalArgs::default(), io)
        });
        assert_eq!(code, crate::exit::OK);
        // The name is the running binary's — under `cargo test` that is the test harness, not
        // `sutra`, which is exactly the derivation a distribution relies on.
        assert_eq!(
            out,
            format!("{} {}\n", crate::program_name(), crate::VERSION)
        );
    }

    #[test]
    fn json_format_emits_an_object() {
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let (code, out, _) = run_captured("", |io| execute(VersionArgs::default(), &global, io));
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["name"], crate::program_name());
        assert_eq!(v["version"], crate::VERSION);
        assert_eq!(v["engine"], crate::VERSION);
    }

    #[test]
    fn unknown_format_is_a_usage_error() {
        let global = GlobalArgs {
            format: Some("xml".into()),
            verbose: 0,
        };
        let (code, _, err) = run_captured("", |io| execute(VersionArgs::default(), &global, io));
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("unsupported --format"), "{err}");
    }
}
