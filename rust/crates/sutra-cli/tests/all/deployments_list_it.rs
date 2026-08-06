//! Exit proof for `sutra deployments list` over a directory of sealed `.sutra` archives.
//! Tier-1 (no docker) — archives are synthesised by sealing tiny deployment-package
//! directories through `sutra_loader::assemble_dir` (the same packaging path the CLI's
//! `sutra package` uses), so `deployments list` verifies them through the very same
//! archive reader the engine uses.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use sutra_cli::commands::deployments::{self, DeploymentsAction, DeploymentsArgs, ListArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;

/// Globally-unique suffix so the parallel test threads never share a scratch path.
static SEQ: AtomicU32 = AtomicU32::new(0);

const PLAIN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
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

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sutra-cli-deployments-{label}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// Seal one tiny package (distinct labels → distinct manifest → distinct id) into `out_dir`
/// as `<name>.sutra`; returns the derived deployment id. `assemble_dir` names the archive
/// after the package dir's leaf, so the package lives at `<unique>/<name>`.
fn seal(out_dir: &Path, name: &str, labels: &[(&str, &str)]) -> String {
    let pkg = scratch("pkg").join(name);
    std::fs::create_dir_all(pkg.join("bpmn")).unwrap();
    std::fs::write(pkg.join("bpmn/solo.bpmn"), PLAIN_BPMN).unwrap();
    let mut yaml = String::from("labels:\n");
    for (k, v) in labels {
        yaml.push_str(&format!("  {k}: \"{v}\"\n"));
    }
    std::fs::write(pkg.join("package.yaml"), yaml).unwrap();

    let outcome = sutra_loader::assemble_dir(&pkg, out_dir, &Default::default())
        .expect("package seals into one .sutra");
    assert_eq!(outcome.archives.len(), 1);
    let archive = &outcome.archives[0];
    assert_eq!(archive.file_path, out_dir.join(format!("{name}.sutra")));
    archive.id.value().to_string()
}

fn run_list(dir: &Path, label: &[&str], format: Option<&str>) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        deployments::execute(
            DeploymentsArgs {
                action: DeploymentsAction::List(ListArgs {
                    dir: dir.to_path_buf(),
                    label: label.iter().map(|s| s.to_string()).collect(),
                }),
            },
            &GlobalArgs {
                format: format.map(str::to_owned),
                verbose: 0,
            },
            &mut io,
        )
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

/// Three archives with distinct labels sealed into one directory.
fn three_archive_dir() -> (PathBuf, String, String, String) {
    let dir = scratch("archives");
    let alpha = seal(&dir, "alpha", &[("env", "prod"), ("tenant", "acme")]);
    let bravo = seal(&dir, "bravo", &[("env", "prod"), ("tenant", "globex")]);
    let charlie = seal(&dir, "charlie", &[("env", "staging"), ("tenant", "acme")]);
    (dir, alpha, bravo, charlie)
}

#[test]
fn text_lists_every_deployment_with_its_labels_in_filename_order() {
    let (dir, alpha, bravo, charlie) = three_archive_dir();
    let (code, out, err) = run_list(&dir, &[], None);
    assert_eq!(code, exit::OK, "err: {err}");

    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3, "one line per deployment:\n{out}");
    // Filename order alpha < bravo < charlie is stable, independent of the OS's readdir order.
    assert_eq!(lines[0], format!("{alpha}  env=prod tenant=acme"));
    assert_eq!(lines[1], format!("{bravo}  env=prod tenant=globex"));
    assert_eq!(lines[2], format!("{charlie}  env=staging tenant=acme"));
}

#[test]
fn label_filter_narrows_the_listing_with_and_semantics() {
    let (dir, alpha, bravo, charlie) = three_archive_dir();

    // Single filter: both prod deployments.
    let (code, out, _) = run_list(&dir, &["env=prod"], None);
    assert_eq!(code, exit::OK);
    assert!(out.contains(&alpha), "{out}");
    assert!(out.contains(&bravo), "{out}");
    assert!(!out.contains(&charlie), "{out}");

    // Two filters AND together: only the prod+acme deployment.
    let (code, out, _) = run_list(&dir, &["env=prod", "tenant=acme"], None);
    assert_eq!(code, exit::OK);
    assert_eq!(out.lines().count(), 1, "{out}");
    assert!(out.contains(&alpha), "{out}");

    // A filter that matches nothing yields an empty listing (exit OK).
    let (code, out, _) = run_list(&dir, &["env=nope"], None);
    assert_eq!(code, exit::OK);
    assert!(out.is_empty(), "{out}");
}

#[test]
fn json_output_is_a_valid_array_of_id_labels_supersedes() {
    let (dir, alpha, _bravo, _charlie) = three_archive_dir();
    let (code, out, _) = run_list(&dir, &["tenant=acme"], Some("json"));
    assert_eq!(code, exit::OK);

    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let arr = v.as_array().unwrap();
    // tenant=acme keeps alpha (prod) and charlie (staging), filename-ordered.
    assert_eq!(arr.len(), 2, "{out}");
    assert_eq!(arr[0]["id"], serde_json::json!(alpha));
    assert_eq!(arr[0]["labels"]["env"], "prod");
    assert_eq!(arr[0]["labels"]["tenant"], "acme");
    // supersedes is present as an array (empty for these fresh archives).
    assert_eq!(arr[0]["supersedes"], serde_json::json!([] as [String; 0]));
    // Every element carries the object shape.
    for element in arr {
        assert!(element["id"].as_str().unwrap().starts_with("dep-"));
        assert!(element["labels"].is_object());
        assert!(element["supersedes"].is_array());
    }
}

#[test]
fn a_corrupt_archive_is_skipped_with_a_warning_and_the_rest_still_list() {
    let (dir, alpha, bravo, charlie) = three_archive_dir();
    // A file that carries the .sutra extension but is not a readable archive: it must be
    // warned + skipped (fail-closed per archive), never abort the whole listing.
    std::fs::write(dir.join("zzz-broken.sutra"), b"not a real zip").unwrap();

    let (code, out, err) = run_list(&dir, &[], None);
    assert_eq!(code, exit::OK, "one bad archive never fails the listing");
    assert_eq!(
        out.lines().count(),
        3,
        "the three good archives still list\n{out}"
    );
    for id in [&alpha, &bravo, &charlie] {
        assert!(out.contains(id), "{out}");
    }
    assert!(
        err.contains("skipping") && err.contains("zzz-broken.sutra"),
        "the bad archive is reported on stderr: {err}"
    );
}

#[test]
fn missing_directory_is_a_usage_error() {
    let (code, _, err) = run_list(Path::new("/does/not/exist/deployments"), &[], None);
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("directory not found"), "{err}");
}

#[test]
fn a_malformed_label_filter_is_a_usage_error() {
    let dir = scratch("labels-bad");
    let (code, _, err) = run_list(&dir, &["justakey"], None);
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("expected KEY=VALUE"), "{err}");
}

#[test]
fn an_empty_directory_lists_nothing_cleanly() {
    let dir = scratch("empty");
    let (code, out, err) = run_list(&dir, &[], None);
    assert_eq!(code, exit::OK, "err: {err}");
    assert!(out.is_empty(), "{out}");

    // json over an empty directory is a well-formed empty array.
    let (code, out, _) = run_list(&dir, &[], Some("json"));
    assert_eq!(code, exit::OK);
    assert_eq!(out.trim(), "[]");
}
