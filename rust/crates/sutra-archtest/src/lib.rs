//! The Rust **domain-neutrality enforcement gate**. Restores the load-bearing, structurally
//! enforced claim that the engine's execution + model + language CORE knows nothing about any
//! specific business domain (SWIFT, FedNow, ISO 20022, HL7, NACHA, EDIFACT, …). A structural test
//! plus a build-failing canary.
//!
//! The gate walks the gated crates' `src/` ([`NEUTRAL_CORE_CRATES`] + [`GATED_ASSEMBLY_CRATES`]) and
//! fails on any [`DOMAIN_DENYLIST`] literal in **code** — comments are stripped first, so a domain
//! example cited in a doc/comment is allowed; only a domain term baked into an identifier or string
//! literal is a violation.
//!
//! ## What is (and is NOT) gated
//!
//! Enforced (must be domain-literal-free):
//! - the neutral CORE — the token executor, the BPMN model, the FEEL language, the DMN + SRL
//!   decision engines, the Handlebars template engine, durable storage, and the codec / format /
//!   schema SPI (`sutra-codec-spi`: the traits + registries + shape/result/issue types).
//! - the ASSEMBLY / binding layer — `sutra-channels` (protocol-neutral channel binding + the neutral
//!   codec/format registries) and `sutra-engine` (the engine LIBRARY; it collects codecs/formats/
//!   transports generically via the SPIs and names none). Domain-literal-free since the transport +
//!   codec/format extraction and the `sutra-dist` composition-root split.
//!
//! Deliberately EXCLUDED — the legitimate domain edge (names concrete impls BY DESIGN):
//! - `sutra-dist` — the composition root / deployed binary: it force-links the bundled business
//!   codecs + formats + feature-selected transports, so it necessarily NAMES them. That is the SPI
//!   wiring boundary (where wiring concretes to SPIs belongs), not a core leak.
//! - `sutra-formats` (json/xml/yaml/raw/csv) and `sutra-codec-schema` (structural / json-schema)
//!   — the schema-less format impls, and the only codec-side crates left in this workspace.
//! - `sutra-transport-*` — the vendor transports.
//!
//! Out of reach entirely: every `sutra-codec-<standard>` business crate, the generated binding
//! crate their schema corpora produce, the per-data-structure redactors and the financial
//! validators live in a SEPARATE, proprietary repository that composes this one as a submodule.
//! This gate stays here, on the engine, because the boundary it defends is the engine's — the
//! rail repo is the legitimate place to name a standard out loud.
//! - `sutra-loader` — deploy-lint (resolves the reserved codec/format set + compiles user schema
//!   codecs; domain names appear only in comments + test fixtures).
//! - the tooling / test crates (`sutra-cli`, `sutra-conformance`, `sutra-testkit`, the generators).

use std::fs;
use std::path::{Path, PathBuf};

/// The neutral-core crates the gate enforces. A domain literal in any of these fails the build.
pub const NEUTRAL_CORE_CRATES: &[&str] = &[
    "sutra-executor",
    "sutra-bpmn",
    "sutra-feel",
    "sutra-dmn",
    "sutra-srl",
    "sutra-templates",
    "sutra-persistence",
    "sutra-datastore",
    // The codec / format / schema SPI — the PayloadCodec/MessageFormat/MessageSchema traits, the
    // BuiltinCodec + BuiltinFormat registries, and the shape/result/issue/codes/mapped/projection
    // types carry no business standard. The concrete parsers live ABOVE it (sutra-formats,
    // sutra-codec-schema, sutra-codec-<standard>) and are excluded.
    "sutra-codec-spi",
    // The model-provider SPI behind the `llm` transport — neutral types + trait; the provider
    // adapters (`sutra-llm-<provider>`) live above it and are excluded like the transports.
    "sutra-llm-spi",
    // The BPMN diagram crate: published on its own, and knows no engine at all (see
    // `bpmn_svg_stays_standalone`).
    "bpmn-svg",
];

/// The assembly / binding-layer crates — domain-literal-free since the codec/format + transport
/// extraction and the `sutra-dist` composition-root split moved every concrete-impl name out of the
/// engine library. Gated alongside the core; the composition root (`sutra-dist`) that legitimately
/// names the concretes is EXCLUDED.
pub const GATED_ASSEMBLY_CRATES: &[&str] = &["sutra-channels", "sutra-engine"];

/// The forbidden business-domain literals (matched case-insensitively, as substrings of the
/// comment-stripped code). Curated to distinctive names that do not collide with ordinary English
/// or code identifiers (so e.g. `pain` — which lurks in "paint" — is represented by the ISO context
/// terms, not a bare-word match).
pub const DOMAIN_DENYLIST: &[&str] = &[
    "swift", "fednow", "fedwire", "pacs", "camt", "nacha", "edifact", "hl7", "iso20022", "x12",
];

/// One violation: `(file, 1-based line, matched term)`.
pub type Violation = (String, usize, String);

/// Strip Rust line (`//`) and block (`/* */`) comments, keeping string and char literals intact (a
/// domain term inside a string constant IS a code-level violation, so strings are preserved; and a
/// `//` inside a string must not be mistaken for a comment). Newlines are preserved so line numbers
/// stay meaningful.
pub fn strip_comments(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Line comment `// … \n` — drop to end of line (the `\n` is pushed next iteration).
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // Block comment `/* … */` — drop, but keep newlines so line numbers survive.
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                if chars[i] == '\n' {
                    out.push('\n');
                }
                i += 1;
            }
            i += 2; // consume the closing `*/`
            continue;
        }
        // String literal — kept (a domain literal in a string constant IS a code violation); a `//`
        // inside must not be read as a comment. A backslash escapes the next char.
        if c == '"' {
            out.push(c);
            i += 1;
            while i < chars.len() {
                out.push(chars[i]);
                if chars[i] == '\\' && i + 1 < chars.len() {
                    out.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Distinguish a char literal (`'a'`, `'\n'`, `'\''`) from a LIFETIME (`'static`, `'a`). A
        // naive scanner treats `&'static str` as an unterminated char literal and swallows the
        // following code + comments (so domain words in later doc comments leak through unstripped).
        // A char literal closes within one char (or is an escape); anything else is a lifetime whose
        // `'` is an ordinary code char.
        if c == '\'' {
            let is_char_lit = chars.get(i + 1) == Some(&'\\')
                || (chars.get(i + 1).is_some() && chars.get(i + 2) == Some(&'\''));
            if is_char_lit {
                out.push(c);
                i += 1;
                while i < chars.len() {
                    out.push(chars[i]);
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        out.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    if chars[i] == '\'' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            out.push(c);
            i += 1;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Every `(1-based line, matched term)` domain-literal hit in comment-stripped `code`.
pub fn scan_code(code: &str) -> Vec<(usize, String)> {
    let terms: Vec<String> = DOMAIN_DENYLIST.iter().map(|t| t.to_lowercase()).collect();
    let mut hits = Vec::new();
    for (i, line) in code.lines().enumerate() {
        let lower = line.to_lowercase();
        for term in &terms {
            if lower.contains(term.as_str()) {
                hits.push((i + 1, term.clone()));
            }
        }
    }
    hits
}

/// Recursively collect every `.rs` file under `dir`.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(rust_files(&path));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    out
}

/// The `crates/` directory holding every workspace crate (this crate's parent).
fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

/// Scan one crate's `src/` and return every domain-literal violation (comment-stripped code).
pub fn scan_crate_src(crate_name: &str) -> Vec<Violation> {
    let src = crates_dir().join(crate_name).join("src");
    let mut violations = Vec::new();
    for path in rust_files(&src) {
        let content = fs::read_to_string(&path).unwrap_or_default();
        for (line, term) in scan_code(&strip_comments(&content)) {
            violations.push((path.display().to_string(), line, term));
        }
    }
    violations
}

/// The `channel` of a `rust-toolchain.toml`: the toolchain every build of the workspace uses.
pub fn toolchain_channel(toml: &str) -> Option<String> {
    toml.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "channel").then(|| unquote(value.trim()).to_string())
    })
}

fn unquote(value: &str) -> &str {
    let value = value.split(" #").next().unwrap_or(value).trim();
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
}

/// Every place a GitHub Actions workflow disagrees with the pinned toolchain `channel`: a
/// `toolchain:` input naming another toolchain, or a `dtolnay/rust-toolchain` step with no
/// `toolchain:` input at all, which would install whatever the action's default is. Each entry is
/// `<line>: <what>`. A workflow that installs a second toolchain beside the pin compiles nothing
/// with it, because every step runs inside `rust/` where the pin wins, so the only cost of a
/// mismatch is a slower job; but a CI job that says 1.95 while the build uses 1.96 misleads anyone
/// reading it, so the two must agree.
pub fn workflow_toolchain_mismatches(workflow: &str, channel: &str) -> Vec<String> {
    let mut out = Vec::new();
    let lines: Vec<&str> = workflow.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start().trim_start_matches("- ").trim_start();
        if let Some(value) = trimmed.strip_prefix("toolchain:") {
            let value = unquote(value.trim());
            if value != channel {
                out.push(format!(
                    "{}: toolchain '{value}', but rust-toolchain.toml pins '{channel}'",
                    index + 1
                ));
            }
        }
        if let Some(action) = trimmed.strip_prefix("uses:") {
            if !action.trim().starts_with("dtolnay/rust-toolchain@") {
                continue;
            }
            let indent = line.len() - line.trim_start().len();
            let step_has_toolchain = lines[index + 1..]
                .iter()
                .take_while(|next| {
                    let next_indent = next.len() - next.trim_start().len();
                    next.trim().is_empty() || next_indent > indent || {
                        // A key of the same step at the same indent as `uses:`.
                        next_indent == indent && !next.trim_start().starts_with('-')
                    }
                })
                .any(|next| next.trim_start().starts_with("toolchain:"));
            if !step_has_toolchain {
                out.push(format!(
                    "{}: dtolnay/rust-toolchain without a `toolchain:` input; pass \
                     `toolchain: {channel}`",
                    index + 1
                ));
            }
        }
    }
    out
}

/// The repository root: `rust/crates/<this crate>` is three levels down.
#[cfg(test)]
fn repo_root() -> PathBuf {
    crates_dir()
        .parent()
        .and_then(Path::parent)
        .expect("rust/crates sits two levels below the repository root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE GATE — no forbidden domain literal appears in the neutral core OR the assembly layer.
    #[test]
    fn gated_crates_have_no_domain_literals() {
        let mut violations = Vec::new();
        for crate_name in NEUTRAL_CORE_CRATES.iter().chain(GATED_ASSEMBLY_CRATES) {
            violations.extend(scan_crate_src(crate_name));
        }
        assert!(
            violations.is_empty(),
            "domain literal(s) leaked into a domain-neutral crate — move the domain-specific code to \
             the composition root (sutra-dist) / a codec/format/transport crate, or reword the \
             reference:\n{}",
            violations
                .iter()
                .map(|(f, l, t)| format!("  {f}:{l} — '{t}'"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// Guard: the gate is not silently a no-op — each gated crate's `src/` is present + scanned.
    /// `bpmn-svg` is published for other projects to use without the rest of this workspace: it
    /// has one third-party dependency, none from here, and nothing in its folder names the project.
    #[test]
    fn bpmn_svg_stays_standalone() {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for entry in fs::read_dir(dir).expect("read crate folder") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    walk(&path, out);
                } else {
                    out.push(path);
                }
            }
        }
        let root = crates_dir().join("bpmn-svg");
        let mut files = Vec::new();
        walk(&root, &mut files);
        assert!(files.len() > 10, "the crate's folder was walked");
        for file in &files {
            let text = fs::read_to_string(file).expect("every file in the crate is text");
            assert!(
                !text.to_lowercase().contains("sutra"),
                "{} names the project",
                file.display()
            );
        }
        let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("manifest");
        let dependencies: Vec<&str> = manifest
            .lines()
            .skip_while(|line| *line != "[dependencies]")
            .skip(1)
            .take_while(|line| !line.starts_with('['))
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(dependencies, ["quick-xml = { workspace = true }"]);
    }

    #[test]
    fn every_gated_crate_is_scanned() {
        for crate_name in NEUTRAL_CORE_CRATES.iter().chain(GATED_ASSEMBLY_CRATES) {
            let src = crates_dir().join(crate_name).join("src");
            assert!(
                src.is_dir() && !rust_files(&src).is_empty(),
                "gated crate '{crate_name}' has no scanned src/ — renamed or missing?"
            );
        }
    }

    /// THE CANARY — the gate actually fires on an injected domain literal in code. If this ever
    /// passes silently, the gate above is asleep.
    #[test]
    fn the_gate_fires_on_an_injected_code_literal() {
        let injected = r#"
            pub fn route(codec: &str) -> bool {
                let builtin = "swift-mx"; // an injected domain leak
                builtin == codec
            }
        "#;
        let hits = scan_code(&strip_comments(injected));
        assert!(
            hits.iter().any(|(_, t)| t == "swift"),
            "the gate must catch a domain literal baked into code"
        );
    }

    /// A domain example cited only in a COMMENT is documentation, not a violation — the stripper
    /// removes it so the gate stays honest about what it flags.
    #[test]
    fn domain_examples_in_comments_are_not_flagged() {
        let doc = "// routes e.g. the fednow pacs.008 flow to its handler\n\
                   /* nacha, hl7, edifact are all just examples here */\n\
                   let x = 1;\n";
        assert!(
            scan_code(&strip_comments(doc)).is_empty(),
            "domain terms confined to comments must not be flagged"
        );
    }

    /// Regression: a domain word in a comment that FOLLOWS a Rust lifetime (`&'static str`) must
    /// still be stripped. A naive scanner mis-reads `'static` as an unterminated char literal and
    /// swallows the following code + comments, leaking domain words (this bit `sutra-codec-spi`).
    #[test]
    fn lifetimes_do_not_defeat_comment_stripping() {
        let code = "pub const NAME: &'static str = \"x\";\n\
                    /// see urn:sutra:codec:swift-mx and nacha-ach for examples\n\
                    fn f<'a>(s: &'a str) -> usize { s.len() }\n";
        assert!(
            scan_code(&strip_comments(code)).is_empty(),
            "a domain word in a comment after a lifetime must still be stripped"
        );
    }

    /// Every workflow installs the toolchain `rust/rust-toolchain.toml` pins. The workflows exist only
    /// in a checkout that carries `.github/workflows`; anywhere else there is nothing to check.
    #[test]
    fn workflows_use_the_pinned_toolchain() {
        let root = repo_root();
        let workflows = root.join(".github").join("workflows");
        let Ok(entries) = fs::read_dir(&workflows) else {
            eprintln!("no {} here: nothing to check", workflows.display());
            return;
        };
        let pin = fs::read_to_string(root.join("rust").join("rust-toolchain.toml"))
            .expect("rust/rust-toolchain.toml");
        let channel = toolchain_channel(&pin).expect("rust-toolchain.toml names a channel");
        let mut files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "yml" || x == "yaml"))
            .collect();
        files.sort();
        assert!(
            !files.is_empty(),
            "{} has no workflows",
            workflows.display()
        );
        let mismatches: Vec<String> = files
            .iter()
            .flat_map(|file| {
                let text = fs::read_to_string(file).expect("workflow readable");
                let name = file.file_name().unwrap().to_string_lossy().into_owned();
                workflow_toolchain_mismatches(&text, &channel)
                    .into_iter()
                    .map(move |m| format!("{name}:{m}"))
            })
            .collect();
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn the_toolchain_check_reads_the_pin_and_flags_a_drift() {
        assert_eq!(
            toolchain_channel("[toolchain]\nchannel = \"1.96.0\"\ncomponents = [\"clippy\"]\n")
                .as_deref(),
            Some("1.96.0")
        );
        let pinned =
            "steps:\n  - uses: dtolnay/rust-toolchain@0123abcd # master\n    with:\n      \
                      toolchain: 1.96.0\n      components: clippy\n  - run: make lint\n";
        assert!(workflow_toolchain_mismatches(pinned, "1.96.0").is_empty());
        let drifted = pinned.replace("toolchain: 1.96.0", "toolchain: \"1.95.0\"");
        let found = workflow_toolchain_mismatches(&drifted, "1.96.0");
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].starts_with("4: toolchain '1.95.0'"), "{found:?}");
        let missing = "steps:\n  - uses: dtolnay/rust-toolchain@stable\n  - name: next\n    \
                       with:\n      toolchain: 1.96.0\n";
        let found = workflow_toolchain_mismatches(missing, "1.96.0");
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].starts_with("2: dtolnay/rust-toolchain without"),
            "{found:?}"
        );
    }
}
