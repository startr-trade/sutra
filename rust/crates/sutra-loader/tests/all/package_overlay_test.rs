//! `--overlay` composition (`sutra_loader::overlay`): the layered authoring layout — one
//! shared folder plus a small per-variant folder — composed into the ONE standalone package
//! directory the scan takes.
//!
//! The load-bearing property is EQUIVALENCE: a layered run and a hand-merged run of the same
//! content must produce the same deploymentId, because everything downstream (content
//! addressing, `--check`-style determinism, fail-closed validation) is defined over the
//! composed tree and must not know that it was composed. The rest of these fixtures pin the
//! layering rules: precedence, the refused `package.yaml` in an overlay, the override
//! report, byte-determinism, and `lint --overlay` over the composed tree.
//!
//! Fixtures are built in temp dirs here rather than committed under `examples/`: the layout
//! is a packaging-time convention, not an example deployment.

use std::path::{Path, PathBuf};

use sutra_loader::overlay::{assemble_dir_with_overlays, compose_overlays, lint_dir_with_overlays};
use sutra_loader::{assemble_dir, lint_dir, read_archive_file, LintSeverity, PackageOptions};

/// A process whose `name` carries the LAYER it came from, so "whose copy won" is readable
/// straight out of the sealed archive.
fn bpmn(process_id: &str, layer: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:solo:1.0.0">
  <bpmn:process id="{process_id}" name="{layer}" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

const PACKAGE_YAML: &str = "labels:\n  module: \"solo\"\n  tenant: \"default\"\n  \
                            version: \"1.0.0\"\nentryProcesses:\n  - \"solo\"\n";

fn write(dir: &Path, relative: &str, content: &str) {
    let path = dir.join(relative);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
    std::fs::write(&path, content).expect("write fixture");
}

/// The shared layer: the common content, and NO `package.yaml`.
fn shared_layer(root: &Path) -> PathBuf {
    let dir = root.join("shared");
    write(&dir, "bpmn/solo.bpmn", &bpmn("solo", "shared"));
    write(&dir, "bpmn/aux.bpmn", &bpmn("aux", "shared"));
    dir
}

/// The variant layer: only what differs — here just the metadata the archive is labelled
/// with. Named `alpha`, so the archive is `alpha.sutra`.
fn variant_layer(root: &Path) -> PathBuf {
    let dir = root.join("alpha");
    write(&dir, "package.yaml", PACKAGE_YAML);
    dir
}

/// The same content as `shared_layer` + `variant_layer`, merged by hand into ONE directory
/// carrying the same leaf name — the thing a layered run must be indistinguishable from.
fn hand_merged(root: &Path) -> PathBuf {
    let dir = root.join("merged").join("alpha");
    write(&dir, "bpmn/solo.bpmn", &bpmn("solo", "shared"));
    write(&dir, "bpmn/aux.bpmn", &bpmn("aux", "shared"));
    write(&dir, "package.yaml", PACKAGE_YAML);
    dir
}

fn overridden_paths(diagnostics: &[sutra_loader::LintDiagnostic]) -> Vec<String> {
    diagnostics
        .iter()
        .filter(|d| d.code == "SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE")
        .map(|d| {
            d.message
                .split('\'')
                .nth(1)
                .expect("the message quotes the relative path")
                .to_string()
        })
        .collect()
}

// ---------------------------------------------------------------------------------------
// 1. Equivalence — the contract everything else rests on.
// ---------------------------------------------------------------------------------------

#[test]
fn a_layered_package_seals_to_the_same_deployment_id_as_a_hand_merged_one() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());
    let merged = hand_merged(root.path());

    let layered_out = root.path().join("out-layered");
    let merged_out = root.path().join("out-merged");
    let layered = assemble_dir_with_overlays(
        &variant,
        std::slice::from_ref(&shared),
        &layered_out,
        &PackageOptions::default(),
    )
    .expect("the layered package seals");
    let direct =
        assemble_dir(&merged, &merged_out, &PackageOptions::default()).expect("the merge seals");

    let (layered, direct) = (&layered.archives[0], &direct.archives[0]);
    assert_eq!(
        layered.id, direct.id,
        "composition must be invisible to content addressing"
    );
    // The archive NAME comes from the package dir's leaf, which is why the composed temp dir
    // has to carry it.
    assert_eq!(layered.file_path, layered_out.join("alpha.sutra"));
    assert_eq!(direct.file_path, merged_out.join("alpha.sutra"));
    assert_eq!(
        std::fs::read(&layered.file_path).unwrap(),
        std::fs::read(&direct.file_path).unwrap(),
        "byte-identical, not merely id-identical"
    );
}

// ---------------------------------------------------------------------------------------
// 2. Precedence — the package dir wins over every overlay; a later overlay over an earlier.
// ---------------------------------------------------------------------------------------

#[test]
fn the_package_dir_wins_over_every_overlay_and_a_later_overlay_over_an_earlier() {
    let root = tempfile::tempdir().unwrap();

    let first = root.join_layer("first", &[("notes.txt", "first"), ("shared.txt", "first")]);
    write(&first, "only-first.txt", "first");
    let second = root.join_layer(
        "second",
        &[("notes.txt", "second"), ("shared.txt", "second")],
    );
    let package = root.join_layer("alpha", &[("notes.txt", "package"), ("package.yaml", "")]);

    let composed = compose_overlays(&package, &[first, second]).expect("the layers compose");
    let read = |relative: &str| std::fs::read_to_string(composed.path().join(relative)).unwrap();

    assert_eq!(read("notes.txt"), "package", "the package dir wins");
    assert_eq!(read("shared.txt"), "second", "a later overlay wins");
    assert_eq!(read("only-first.txt"), "first", "unopposed layers survive");
    assert_eq!(
        composed.path().file_name().unwrap(),
        "alpha",
        "the composed dir is named after the package dir"
    );
}

#[test]
fn the_winning_layers_bytes_are_what_gets_sealed() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());
    // The variant overrides one shared process file.
    write(&variant, "bpmn/solo.bpmn", &bpmn("solo", "variant"));

    let out = root.path().join("out");
    let outcome = assemble_dir_with_overlays(
        &variant,
        std::slice::from_ref(&shared),
        &out,
        &PackageOptions::default(),
    )
    .expect("the layered package seals");
    let archive = read_archive_file(&outcome.archives[0].file_path).expect("round-trips");
    let sealed = &archive.deployment.process_files["solo.bpmn"].content;
    assert!(sealed.contains(r#"name="variant""#), "{sealed}");
    assert_eq!(
        overridden_paths(&outcome.report.diagnostics),
        vec!["bpmn/solo.bpmn".to_string()]
    );
}

// ---------------------------------------------------------------------------------------
// 3. An overlay may not carry package.yaml.
// ---------------------------------------------------------------------------------------

#[test]
fn an_overlay_carrying_package_yaml_is_refused_by_name() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());
    write(&shared, "package.yaml", PACKAGE_YAML);

    let report = lint_dir_with_overlays(&variant, std::slice::from_ref(&shared));
    let errors: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("package.yaml"), "{}", errors[0]);
    assert!(
        errors[0].contains(&shared.join("package.yaml").display().to_string()),
        "the refusal names the offending file: {}",
        errors[0]
    );

    // Fail-closed on the packaging side too: nothing is emitted.
    let out = root.path().join("out");
    let error = assemble_dir_with_overlays(
        &variant,
        std::slice::from_ref(&shared),
        &out,
        &PackageOptions::default(),
    )
    .expect_err("packaging refuses the overlay");
    match error {
        sutra_loader::PackageError::Validation(report) => {
            assert!(report
                .errors()
                .any(|d| d.message.contains("package.yaml") && d.message.contains("overlay")));
        }
        other => panic!("expected a validation refusal, got {other:?}"),
    }
    assert!(!out.exists(), "nothing is emitted");
}

// ---------------------------------------------------------------------------------------
// 5. The override report.
// ---------------------------------------------------------------------------------------

#[test]
fn every_override_is_reported_once_naming_the_path_and_the_winning_layer() {
    let root = tempfile::tempdir().unwrap();
    let first = root.join_layer(
        "first",
        &[
            ("notes.txt", "first"),
            ("shared.txt", "first"),
            ("nested/deep.txt", "first"),
            ("only-first.txt", "first"),
        ],
    );
    let second = root.join_layer(
        "second",
        &[("notes.txt", "second"), ("shared.txt", "second")],
    );
    let package = root.join_layer(
        "alpha",
        &[
            ("notes.txt", "package"),
            ("nested/deep.txt", "package"),
            ("package.yaml", ""),
        ],
    );

    let composed =
        compose_overlays(&package, &[first.clone(), second.clone()]).expect("the layers compose");

    // One diagnostic per shadowing EVENT, in layer order then path order: `notes.txt` is
    // supplied by all three layers, so it is reported twice.
    assert_eq!(
        overridden_paths(composed.overrides()),
        vec!["notes.txt", "shared.txt", "nested/deep.txt", "notes.txt"],
    );
    for diagnostic in composed.overrides() {
        assert_eq!(diagnostic.severity, LintSeverity::Warning, "non-fatal");
        assert_eq!(diagnostic.code, "SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE");
    }
    // Nothing that was never shadowed is mentioned.
    assert!(!composed
        .overrides()
        .iter()
        .any(|d| d.message.contains("only-first.txt")));
    // And each one says which layer won.
    let messages: Vec<&str> = composed
        .overrides()
        .iter()
        .map(|d| d.message.as_str())
        .collect();
    assert!(
        messages[0].contains(&format!("overlay {}", second.display())),
        "{}",
        messages[0]
    );
    assert!(
        messages[2].contains(&format!("package directory {}", package.display())),
        "{}",
        messages[2]
    );
}

// ---------------------------------------------------------------------------------------
// 6. Determinism.
// ---------------------------------------------------------------------------------------

#[test]
fn two_layered_runs_produce_byte_identical_archives() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());

    let seal = |out: PathBuf| {
        let outcome = assemble_dir_with_overlays(
            &variant,
            std::slice::from_ref(&shared),
            &out,
            &PackageOptions::default(),
        )
        .expect("the layered package seals");
        let path = outcome.archives[0].file_path.clone();
        (outcome.archives[0].id.clone(), std::fs::read(path).unwrap())
    };
    let (first_id, first) = seal(root.path().join("out-1"));
    let (second_id, second) = seal(root.path().join("out-2"));
    assert_eq!(first_id, second_id);
    assert_eq!(first, second, "composition adds no run-to-run variance");
}

// ---------------------------------------------------------------------------------------
// 7. `lint --overlay` lints the composed tree.
// ---------------------------------------------------------------------------------------

#[test]
fn a_variant_that_is_incomplete_alone_lints_clean_once_composed() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());

    // Alone, the variant declares an entry process it does not define.
    let alone = lint_dir(&variant);
    assert!(
        alone.errors().any(|d| d.message.contains("solo")),
        "{:?}",
        alone.diagnostics
    );

    let composed = lint_dir_with_overlays(&variant, std::slice::from_ref(&shared));
    let errors: Vec<String> = composed.errors().map(|d| d.message.clone()).collect();
    assert!(errors.is_empty(), "{errors:?}");
}

#[test]
fn an_error_in_either_layer_is_reported() {
    let root = tempfile::tempdir().unwrap();

    // (a) The fault is in the OVERLAY: a second namespace in the shared content.
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());
    write(
        &shared,
        "bpmn/other.bpmn",
        &bpmn("other", "shared").replace("urn:sutra:module:solo:1.0.0", "urn:sutra:module:other:1"),
    );
    let report = lint_dir_with_overlays(&variant, std::slice::from_ref(&shared));
    assert!(
        report
            .errors()
            .any(|d| d.message.contains("one package is one namespace")),
        "{:?}",
        report.diagnostics
    );

    // (b) The fault is in the PACKAGE DIR: an entry process nothing defines.
    let shared = shared_layer(&root.path().join("clean"));
    let variant = variant_layer(&root.path().join("clean"));
    write(
        &variant,
        "package.yaml",
        "entryProcesses:\n  - \"ghost\"\n  - \"solo\"\n",
    );
    let report = lint_dir_with_overlays(&variant, std::slice::from_ref(&shared));
    assert!(
        report.errors().any(|d| d.message.contains("ghost")),
        "{:?}",
        report.diagnostics
    );
}

// ---------------------------------------------------------------------------------------
// Layering rules that keep the composition deterministic.
// ---------------------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn a_symlink_inside_a_layer_is_refused_rather_than_followed() {
    let root = tempfile::tempdir().unwrap();
    let shared = shared_layer(root.path());
    let variant = variant_layer(root.path());
    std::os::unix::fs::symlink(shared.join("bpmn/solo.bpmn"), shared.join("bpmn/link.bpmn"))
        .unwrap();

    let report = lint_dir_with_overlays(&variant, std::slice::from_ref(&shared));
    let errors: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("bpmn/link.bpmn"), "{}", errors[0]);
    assert!(errors[0].contains("symbolic link"), "{}", errors[0]);
}

#[test]
fn a_missing_overlay_is_an_io_refusal_not_a_panic() {
    let root = tempfile::tempdir().unwrap();
    let variant = variant_layer(root.path());
    let absent = root.path().join("not-here");

    let error = assemble_dir_with_overlays(
        &variant,
        std::slice::from_ref(&absent),
        &root.path().join("out"),
        &PackageOptions::default(),
    )
    .expect_err("a missing layer is refused");
    match error {
        sutra_loader::PackageError::Io(e) => {
            assert!(e.message.contains("overlay directory"), "{e}");
            assert!(e.message.contains("not-here"), "{e}");
        }
        other => panic!("expected an I/O refusal, got {other:?}"),
    }
    // Lint has only the report to speak through, so the same refusal folds in as its error.
    let report = lint_dir_with_overlays(&variant, std::slice::from_ref(&absent));
    assert!(
        report.errors().any(|d| d.message.contains("not-here")),
        "{:?}",
        report.diagnostics
    );
}

#[test]
fn no_overlays_is_the_unlayered_path_verbatim() {
    let root = tempfile::tempdir().unwrap();
    let merged = hand_merged(root.path());

    let layered = assemble_dir_with_overlays(
        &merged,
        &[],
        &root.path().join("out-a"),
        &PackageOptions::default(),
    )
    .expect("seals");
    let direct = assemble_dir(
        &merged,
        &root.path().join("out-b"),
        &PackageOptions::default(),
    )
    .expect("seals");
    assert_eq!(layered.archives[0].id, direct.archives[0].id);
    assert!(layered.report.diagnostics.is_empty());
    assert_eq!(
        lint_dir_with_overlays(&merged, &[]).diagnostics.len(),
        lint_dir(&merged).diagnostics.len()
    );
}

/// Small fixture sugar: a layer directory with a handful of files.
trait LayerDir {
    fn join_layer(&self, name: &str, files: &[(&str, &str)]) -> PathBuf;
}

impl LayerDir for tempfile::TempDir {
    fn join_layer(&self, name: &str, files: &[(&str, &str)]) -> PathBuf {
        let dir = self.path().join(name);
        std::fs::create_dir_all(&dir).expect("mkdir");
        for (relative, content) in files {
            write(&dir, relative, content);
        }
        dir
    }
}
