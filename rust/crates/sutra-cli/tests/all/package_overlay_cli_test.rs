//! `sutra package --overlay` / `sutra lint --overlay` through the CLI entry points: the
//! layered authoring layout (one shared folder plus a small per-variant folder) composed,
//! validated and sealed in one invocation.
//!
//! The composition rules themselves are proven in `sutra-loader`'s own fixtures. What is
//! only provable here is the CLI contract around them: a layer that cannot be read is a
//! USAGE exit (2), not a finding and not a panic, and the override report reaches the
//! printed diagnostics.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use sutra_cli::commands::lint::{self, LintArgs};
use sutra_cli::commands::package::{self, PackageArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;

static SEQ: AtomicU32 = AtomicU32::new(0);

const SOLO_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:solo:1.0.0">
  <bpmn:process id="solo" name="Solo" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const PACKAGE_YAML: &str = "labels:\n  module: \"solo\"\n  version: \"1.0.0\"\n";

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sutra-cli-overlay-{label}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn write(dir: &Path, relative: &str, content: &str) {
    let path = dir.join(relative);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
    std::fs::write(&path, content).expect("write fixture");
}

/// A shared layer (the content) and a variant layer named `alpha` (the metadata, plus its own
/// `channels.yaml`-sized difference — here a second copy of one shared file, so the override
/// report has something to say).
fn layers(label: &str) -> (PathBuf, PathBuf) {
    let root = scratch(label);
    let shared = root.join("shared");
    write(&shared, "bpmn/solo.bpmn", SOLO_BPMN);
    let variant = root.join("alpha");
    write(&variant, "package.yaml", PACKAGE_YAML);
    (shared, variant)
}

fn run<T>(command: impl FnOnce(&mut Io<'_>) -> T) -> (T, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let value = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        command(&mut io)
    };
    (
        value,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

#[test]
fn package_seals_the_composed_tree_under_the_package_dirs_name() {
    let (shared, variant) = layers("seal");
    let out_dir = scratch("seal-out");
    let (code, out, err) = run(|io| {
        package::execute(
            PackageArgs {
                input: variant.clone(),
                out: out_dir.clone(),
                overlay: vec![shared.clone()],
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "err: {err}");
    assert!(out_dir.join("alpha.sutra").is_file(), "{out}");
    assert!(out.contains("packaged"), "{out}");
}

#[test]
fn package_reports_an_override_as_a_warning_and_still_seals() {
    let (shared, variant) = layers("override");
    // The variant's own copy of a shared process file — the override the report must name.
    write(&variant, "bpmn/solo.bpmn", SOLO_BPMN);
    let out_dir = scratch("override-out");
    let (code, out, err) = run(|io| {
        package::execute(
            PackageArgs {
                input: variant.clone(),
                out: out_dir.clone(),
                overlay: vec![shared.clone()],
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "err: {err}");
    assert!(
        out.contains("[WARN] SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE"),
        "{out}"
    );
    assert!(out.contains("bpmn/solo.bpmn"), "{out}");
    assert!(out_dir.join("alpha.sutra").is_file());
}

#[test]
fn lint_validates_the_composed_tree() {
    let (shared, variant) = layers("lint");
    // Incomplete alone (it defines no process), complete once composed.
    write(&variant, "package.yaml", "entryProcesses:\n  - \"solo\"\n");

    let (code, out, err) = run(|io| {
        lint::execute(
            LintArgs {
                package_dir: variant.clone(),
                overlay: vec![shared.clone()],
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "out: {out} err: {err}");
    assert_eq!(out, "0 error(s), 0 warning(s)\n");

    // Without the overlay the same directory is a finding, not a clean run.
    let (code, out, _) = run(|io| {
        lint::execute(
            LintArgs {
                package_dir: variant.clone(),
                overlay: Vec::new(),
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::FINDINGS, "{out}");
}

#[test]
fn a_missing_overlay_directory_is_a_usage_error_in_both_commands() {
    let (_, variant) = layers("missing");
    let absent = variant.parent().unwrap().join("not-here");

    let (code, _, err) = run(|io| {
        package::execute(
            PackageArgs {
                input: variant.clone(),
                out: scratch("missing-out"),
                overlay: vec![absent.clone()],
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("overlay directory not found"), "{err}");
    assert!(err.contains("not-here"), "{err}");

    let (code, _, err) = run(|io| {
        lint::execute(
            LintArgs {
                package_dir: variant.clone(),
                overlay: vec![absent.clone()],
            },
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("overlay directory not found"), "{err}");
}
