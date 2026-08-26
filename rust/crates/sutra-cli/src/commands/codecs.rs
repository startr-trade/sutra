//! `codecs` — list the payload codecs linked into THIS binary, and the message types each
//! declares.
//!
//! # Why this is a command and not a document
//!
//! Codec identity is a property of the executable. A codec registers itself with
//! `inventory::submit!` at link time, so the set a binary can resolve is decided when it is
//! built, not by any configuration a reader could inspect. A distribution that links
//! message-standard codecs answers `codec: urn:sutra:codec:swift-mt` happily; the engine's own
//! CLI, which bundles the schema-less formats and nothing else, answers
//! `SUTRA.INBOUND.CODEC_NOT_FOUND` — and both answers are correct for the binary that gave
//! them. Anything written down instead would be a claim about somebody else's build.
//!
//! So this asks the registry the loader itself resolves through
//! ([`sutra_codec_spi::builtin_codecs`]), which makes the output true by construction and makes
//! a generated reference page possible: `<program> codecs --format json` is the source.
//!
//! # Reading the output
//!
//! `message types` is the codec's DECLARED set — the closed list it can emit. Empty means an
//! OPEN set: the codec derives a type at decode time (the MT application header, the ISO
//! namespace, HL7's MSH-9) and cannot enumerate the possibilities in advance. The distinction
//! is not cosmetic: a closed set is what lets the deploy-time lint check a start event's
//! declared message type, and an open one is why `SUTRA.CHANNEL.NO_SCHEMA` exists.
//!
//! FORMATS are listed separately because they are a different kind: schema-less parsers chosen
//! by content type, with no message types and no shape at all.

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::GlobalArgs;

#[derive(Debug, Default, clap::Args)]
pub struct CodecsArgs {
    /// List only the codec (or format) whose name or URN contains this string.
    #[arg(long, value_name = "TEXT")]
    pub filter: Option<String>,
}

pub fn execute(args: CodecsArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "codecs: {msg}");
            return exit::USAGE;
        }
    };
    let keep = |name: &str| match &args.filter {
        Some(f) => name.contains(f.as_str()),
        None => true,
    };

    let codecs: Vec<_> = sutra_codec_spi::builtin_codecs()
        .into_iter()
        .filter(|c| keep(c.name()))
        .map(|c| {
            let mut types = c.declared_message_types();
            types.sort();
            (c.name().to_string(), c.accepted_content_types(), types)
        })
        .collect();
    let formats: Vec<_> = sutra_codec_spi::builtin_formats()
        .into_iter()
        .filter(|f| keep(f.name))
        .map(|f| (f.name.to_string(), f.codec.accepted_content_types()))
        .collect();

    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "{} {} — {} codec(s), {} format(s)\n",
                crate::program_name(),
                crate::product_version(),
                codecs.len(),
                formats.len()
            );
            for (name, content_types, types) in &codecs {
                let _ = writeln!(io.out, "  urn:sutra:codec:{name}");
                if !content_types.is_empty() {
                    let _ = writeln!(io.out, "    content types  {}", content_types.join(", "));
                }
                match types.len() {
                    // An open set is a REPORTED fact, not an absence — saying "none" would read
                    // as "this codec produces nothing", which is the opposite of the truth.
                    0 => {
                        let _ =
                            writeln!(io.out, "    message types  open — derived at decode time");
                    }
                    n => {
                        let _ = writeln!(io.out, "    message types  {n} declared");
                        for t in types {
                            let _ = writeln!(io.out, "      {t}");
                        }
                    }
                }
                let _ = writeln!(io.out);
            }
            if !formats.is_empty() {
                let _ = writeln!(io.out, "  formats (schema-less, chosen by content type)");
                for (name, content_types) in &formats {
                    let _ = writeln!(
                        io.out,
                        "    urn:sutra:codec:{name}{}{}",
                        if content_types.is_empty() { "" } else { "  " },
                        content_types.join(", ")
                    );
                }
            }
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "binary": crate::program_name(),
                "version": crate::product_version(),
                "codecs": codecs.iter().map(|(name, ct, types)| serde_json::json!({
                    "name": name,
                    "urn": format!("urn:sutra:codec:{name}"),
                    "contentTypes": ct,
                    "declaredMessageTypes": types,
                    "open": types.is_empty(),
                })).collect::<Vec<_>>(),
                "formats": formats.iter().map(|(name, ct)| serde_json::json!({
                    "name": name,
                    "urn": format!("urn:sutra:codec:{name}"),
                    "contentTypes": ct,
                })).collect::<Vec<_>>(),
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

    fn run(args: CodecsArgs, format: Option<&str>) -> (i32, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        let (code, out, _) = run_captured("", |io| execute(args, &global, io));
        (code, out)
    }

    #[test]
    fn lists_the_formats_this_binary_links() {
        // The engine's own test binary links `sutra-formats` and no message standard, which is
        // precisely the distinction the command exists to report.
        let (code, out) = run(CodecsArgs::default(), None);
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("urn:sutra:codec:json"), "{out}");
        assert!(out.contains("formats (schema-less"), "{out}");
    }

    #[test]
    fn the_filter_narrows_to_one_entry() {
        let (code, out) = run(
            CodecsArgs {
                filter: Some("csv".to_string()),
            },
            None,
        );
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("csv"), "{out}");
        assert!(!out.contains("urn:sutra:codec:json"), "{out}");
    }

    #[test]
    fn json_is_machine_readable_and_says_whether_a_set_is_open() {
        let (code, out) = run(CodecsArgs::default(), Some("json"));
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");
        assert!(v["formats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["name"] == "json"));
        // Every entry answers the open/closed question explicitly — a generated page renders
        // that answer rather than inferring it from an empty list.
        for c in v["codecs"].as_array().unwrap() {
            assert!(c["open"].is_boolean(), "{c}");
        }
    }
}
