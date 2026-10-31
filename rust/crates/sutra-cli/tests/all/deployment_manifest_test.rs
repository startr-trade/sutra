//! Deployment manifests through the CLI entry points: `sutra lint|package|deploy
//! <manifest.yaml>`. A manifest is pure de-sugaring into a generated `package.yaml` plus a
//! layer stack, so the load-bearing property is EQUIVALENCE with a hand-merged directory
//! (same archive bytes, same deploymentId). The rest pins the stack rules: order, `at:`
//! mounts, per-layer `.ignore`, refusals, and flag precedence.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use sutra_cli::commands::deploy::{self, DeployArgs, TargetArgs};
use sutra_cli::commands::lint::{self, LintArgs};
use sutra_cli::commands::package::{self, PackageArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn bpmn(process_id: &str, layer: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:order-hub:1.0.0">
  <bpmn:process id="{process_id}" name="{layer}" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

/// The `package.yaml` the fixture manifest's `package:` block generates.
const GENERATED_PACKAGE_YAML: &str =
    "labels:\n  module: order-hub\n  tenant: customer-a\n  version: 1.1.0\nengine:\n  minContract: 1\n";

const PACKAGE_BLOCK: &str = "package:\n  tenant: customer-a\n  module: order-hub\n  \
                             version: 1.1.0\n  engine: { minContract: 1 }\n";

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sutra-cli-manifest-{label}-{}-{}",
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

/// The worked layout: shared processes mounted under `bpmn/`, an app layer, an environment
/// layer that overrides one process and removes another through `.ignore`. Returns the
/// manifest path (under `deploy/`, so every layer path is `../…`).
fn fixture(root: &Path) -> PathBuf {
    // Mounted with `at: bpmn` — the files sit at the layer root.
    write(
        &root.join("shared/procs"),
        "solo.bpmn",
        &bpmn("solo", "shared"),
    );
    write(
        &root.join("shared/procs"),
        "legacy.bpmn",
        &bpmn("legacy", "shared"),
    );
    write(&root.join("app"), "bpmn/aux.bpmn", &bpmn("aux", "app"));
    // The environment: its own copy of solo wins, and legacy is dropped.
    write(
        &root.join("envs/dev"),
        "bpmn/solo.bpmn",
        &bpmn("solo", "dev"),
    );
    write(
        &root.join("envs/dev"),
        ".ignore",
        "# not needed in dev\nbpmn/legacy.bpmn\n",
    );
    let manifest = root.join("deploy/customer-a.dev.yaml");
    write(
        root,
        "deploy/customer-a.dev.yaml",
        &format!(
            "deployment: customer-a-dev\n{PACKAGE_BLOCK}layers:\n  - path: ../shared/procs\n    \
             at: bpmn\n  - ../app\n  - path: ../envs/dev\n"
        ),
    );
    manifest
}

/// What the fixture composes to, merged by hand under the same leaf name.
fn hand_merged(root: &Path) -> PathBuf {
    let dir = root.join("merged/customer-a.dev");
    write(&dir, "package.yaml", GENERATED_PACKAGE_YAML);
    write(&dir, "bpmn/solo.bpmn", &bpmn("solo", "dev"));
    write(&dir, "bpmn/aux.bpmn", &bpmn("aux", "app"));
    dir
}

fn package_args(input: PathBuf, out: PathBuf, overlay: Vec<PathBuf>) -> PackageArgs {
    PackageArgs {
        input,
        out,
        overlay,
    }
}

fn seal(input: PathBuf, overlay: Vec<PathBuf>) -> (PathBuf, String) {
    let out = scratch("out");
    let (code, stdout, err) = run(|io| {
        package::execute(
            package_args(input, out.clone(), overlay),
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "out: {stdout} err: {err}");
    let archive = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "sutra"))
        .expect("an archive");
    (archive, stdout)
}

fn lint_out(input: PathBuf, overlay: Vec<PathBuf>, format: Option<&str>) -> (i32, String, String) {
    let global = GlobalArgs {
        format: format.map(str::to_string),
        verbose: 0,
    };
    run(|io| {
        lint::execute(
            LintArgs {
                package_dir: input,
                overlay,
            },
            &global,
            io,
        )
    })
}

/// The process `name` (= the layer it came from) of `process_id` in a sealed archive.
fn process_layer(archive: &Path, file: &str) -> Option<String> {
    let bytes = std::fs::read(archive).unwrap();
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let names: Vec<String> = zip.file_names().map(|n| n.unwrap().into_owned()).collect();
    let name = names.iter().find(|n| n.ends_with(file))?.clone();
    let mut text = String::new();
    zip.by_name(&name)
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    let start = text.find("name=\"")? + 6;
    Some(text[start..start + text[start..].find('"')?].to_string())
}

fn archive_names(archive: &Path) -> Vec<String> {
    let bytes = std::fs::read(archive).unwrap();
    let zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    zip.file_names().map(|n| n.unwrap().into_owned()).collect()
}

#[test]
fn manifest_seal_is_byte_identical_to_the_hand_merged_seal() {
    let root = scratch("equiv");
    let manifest = fixture(&root);
    let (from_manifest, out) = seal(manifest, Vec::new());
    let (by_hand, _) = seal(hand_merged(&root), Vec::new());

    assert_eq!(
        from_manifest.file_name().unwrap(),
        "customer-a.dev.sutra",
        "named after the manifest file stem"
    );
    assert_eq!(
        std::fs::read(&from_manifest).unwrap(),
        std::fs::read(&by_hand).unwrap(),
        "same archive bytes"
    );
    let id_a = sutra_loader::read_archive_file(&from_manifest).unwrap().id;
    let id_b = sutra_loader::read_archive_file(&by_hand).unwrap().id;
    assert_eq!(id_a.value(), id_b.value(), "same deploymentId");
    assert!(out.contains(id_a.value()), "{out}");
    // `.ignore` is never sealed, and the removed file is gone.
    let names = archive_names(&from_manifest);
    assert!(!names.iter().any(|n| n.contains(".ignore")), "{names:?}");
    assert!(!names.iter().any(|n| n.contains("legacy")), "{names:?}");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn resolution_order_later_layers_win_and_lint_prints_the_stack() {
    let root = scratch("order");
    let manifest = fixture(&root);
    let (archive, _) = seal(manifest.clone(), Vec::new());
    assert_eq!(process_layer(&archive, "solo.bpmn").as_deref(), Some("dev"));
    assert_eq!(process_layer(&archive, "aux.bpmn").as_deref(), Some("app"));

    let (code, out, err) = lint_out(manifest, Vec::new(), None);
    assert_eq!(code, exit::OK, "out: {out} err: {err}");
    let expected_stack = "  layer 1 ../shared/procs (at bpmn)\n  layer 2 ../app\n  \
                          layer 3 ../envs/dev\n  override bpmn/solo.bpmn: layer 3 ../envs/dev \
                          over layer 1 ../shared/procs (at bpmn)\n  removed bpmn/legacy.bpmn: \
                          by layer 3 ../envs/dev .ignore 'bpmn/legacy.bpmn' (was from layer 1 \
                          ../shared/procs (at bpmn))\n";
    assert!(out.contains("(deployment customer-a-dev)"), "{out}");
    assert!(out.contains(expected_stack), "{out}");
    assert!(
        out.contains("[WARN] SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE"),
        "{out}"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lint_json_carries_the_same_stack() {
    let root = scratch("json");
    let manifest = fixture(&root);
    let (code, out, _) = lint_out(manifest, Vec::new(), Some("json"));
    assert_eq!(code, exit::OK, "{out}");
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    let stack = &v["stack"];
    assert_eq!(stack["deployment"], "customer-a-dev");
    assert_eq!(stack["layers"].as_array().unwrap().len(), 3);
    assert_eq!(stack["overrides"][0]["path"], "bpmn/solo.bpmn");
    assert_eq!(stack["overrides"][0]["winner"], "layer 3 ../envs/dev");
    assert_eq!(stack["removals"][0]["path"], "bpmn/legacy.bpmn");
    assert_eq!(stack["removals"][0]["pattern"], "bpmn/legacy.bpmn");
    std::fs::remove_dir_all(root).ok();
}

/// A two-layer manifest whose upper layer carries `ignore` as its `.ignore` and `upper` as
/// extra files; returns the composed archive's bpmn file names and the lint output.
fn ignore_case(label: &str, ignore: &str, upper: &[&str]) -> (Vec<String>, String) {
    let root = scratch(label);
    for f in ["a", "b", "own"] {
        write(
            &root.join("lower"),
            &format!("bpmn/{f}.bpmn"),
            &bpmn(f, "lower"),
        );
    }
    write(&root.join("upper"), ".ignore", ignore);
    for f in upper {
        write(
            &root.join("upper"),
            &format!("bpmn/{f}.bpmn"),
            &bpmn(f, "upper"),
        );
    }
    write(
        &root,
        "m.yaml",
        &format!("{PACKAGE_BLOCK}layers: [lower, upper]\n"),
    );
    let manifest = root.join("m.yaml");
    let (archive, _) = seal(manifest.clone(), Vec::new());
    let mut names: Vec<String> = archive_names(&archive)
        .into_iter()
        .filter(|n| n.ends_with(".bpmn"))
        .map(|n| n.rsplit('/').next().unwrap().to_string())
        .collect();
    names.sort();
    let (_, out, _) = lint_out(manifest, Vec::new(), None);
    std::fs::remove_dir_all(root).ok();
    (names, out)
}

#[test]
fn ignore_removes_lower_layer_paths_with_negation() {
    let (names, out) = ignore_case("neg", "bpmn/*.bpmn\n!bpmn/b.bpmn\n", &[]);
    assert_eq!(names, vec!["b.bpmn"]);
    assert!(out.contains("removed bpmn/a.bpmn"), "{out}");
    assert!(!out.contains("removed bpmn/b.bpmn"), "{out}");
}

#[test]
fn a_layer_never_ignores_its_own_files() {
    let (names, _) = ignore_case("own", "bpmn/\n", &["own"]);
    // The lower layer's three are removed; the upper layer's own copy survives.
    assert_eq!(names, vec!["own.bpmn"]);
}

#[test]
fn package_yaml_cannot_be_ignored_and_an_unmatched_pattern_warns() {
    let (names, out) = ignore_case("pkg", "package.yaml\nnothing-here/\n", &[]);
    assert_eq!(names, vec!["a.bpmn", "b.bpmn", "own.bpmn"]);
    assert!(
        out.contains(
            "[WARN] SUTRA.DEPLOY.PACKAGE.IGNORE_UNMATCHED — layer 2 upper .ignore: pattern \
             'package.yaml' matches no path contributed by the layers below it (package.yaml \
             can never be ignored)"
        ),
        "{out}"
    );
    assert!(
        out.contains("pattern 'nothing-here/' matches no path"),
        "{out}"
    );
    // The archive still validates: the generated package.yaml is there.
    assert!(out.ends_with("0 error(s), 2 warning(s)\n"), "{out}");
}

#[test]
fn explicit_overlays_stack_above_the_manifest_layers() {
    let root = scratch("overlay");
    let manifest = fixture(&root);
    write(
        &root.join("hotfix"),
        "bpmn/solo.bpmn",
        &bpmn("solo", "hotfix"),
    );
    let (archive, _) = seal(manifest.clone(), vec![root.join("hotfix")]);
    assert_eq!(
        process_layer(&archive, "solo.bpmn").as_deref(),
        Some("hotfix")
    );
    let (_, out, _) = lint_out(manifest, vec![root.join("hotfix")], None);
    assert!(out.contains("override bpmn/solo.bpmn: overlay "), "{out}");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn refusals_name_the_problem() {
    let root = scratch("refuse");
    write(&root.join("lower"), "bpmn/a.bpmn", &bpmn("a", "lower"));
    write(&root.join("withpkg"), "package.yaml", "labels: {}\n");

    let cases: [(&str, String, i32, &str); 4] = [
        (
            "pkg-in-layer.yaml",
            format!("{PACKAGE_BLOCK}layers: [lower, withpkg]\n"),
            exit::FINDINGS,
            "carries package.yaml",
        ),
        (
            "escape.yaml",
            format!("{PACKAGE_BLOCK}layers:\n  - path: lower\n    at: ../outside\n"),
            exit::FINDINGS,
            "must stay inside the package root",
        ),
        (
            "missing.yaml",
            format!("{PACKAGE_BLOCK}layers: [lower, not-here]\n"),
            exit::USAGE,
            "layer 2 (not-here) not found",
        ),
        (
            "unknown.yaml",
            format!("{PACKAGE_BLOCK}layers: [lower]\nlayer: [x]\n"),
            exit::USAGE,
            "unknown key 'layer'",
        ),
    ];
    for (file, body, expected_code, needle) in cases {
        write(&root, file, &body);
        let (code, out, err) = run(|io| {
            package::execute(
                package_args(root.join(file), scratch("refuse-out"), Vec::new()),
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, expected_code, "{file}: out {out} err {err}");
        assert!(
            out.contains(needle) || err.contains(needle),
            "{file}: out {out} err {err}"
        );
        if file == "pkg-in-layer.yaml" {
            assert!(out.contains("withpkg"), "names the layer: {out}");
        }
    }
    std::fs::remove_dir_all(root).ok();
}

/// A one-shot HTTP server standing in for the engine's deploy API: answers the first
/// request with 200 `{}` and hands back the request body.
fn mock_engine() -> (String, std::thread::JoinHandle<Vec<u8>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        let body_start;
        loop {
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                body_start = pos + 4;
                break;
            }
        }
        let head = String::from_utf8_lossy(&buf[..body_start]).to_ascii_lowercase();
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map(|v| v.trim().parse().unwrap())
            .unwrap_or(0);
        while buf.len() < body_start + length {
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .unwrap();
        buf[body_start..body_start + length].to_vec()
    });
    (url, handle)
}

fn deploy_args(archive: PathBuf, engine_url: Option<String>) -> DeployArgs {
    DeployArgs {
        archive: Some(archive),
        target: TargetArgs {
            context: None,
            namespace: None,
            configmap: "sutra-deployments".into(),
            secret_name: "sutra-secrets".into(),
        },
        secret: Vec::new(),
        secret_from: None,
        api: false,
        async_lro: false,
        watch: None,
        wait: false,
        engine_url,
        api_key: None,
        api_key_header: "X-API-Key".into(),
        wait_timeout: 180,
    }
}

#[test]
fn manifest_deploy_sends_the_hand_merged_archive_using_the_target_block() {
    let root = scratch("deploy");
    fixture(&root);
    let (by_hand, _) = seal(hand_merged(&root), Vec::new());
    let (url, server) = mock_engine();
    write(
        &root,
        "deploy/customer-a.api.yaml",
        &format!(
            "{PACKAGE_BLOCK}layers:\n  - path: ../shared/procs\n    at: bpmn\n  - ../app\n  \
             - ../envs/dev\ntarget:\n  api: true\n  engine-url: {url}\n"
        ),
    );
    let (code, out, err) = run(|io| {
        deploy::execute_deploy(
            deploy_args(root.join("deploy/customer-a.api.yaml"), None),
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "out: {out} err: {err}");
    assert_eq!(server.join().unwrap(), std::fs::read(&by_hand).unwrap());
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn command_line_deploy_args_win_over_the_target_block() {
    let root = scratch("deploy-cli");
    fixture(&root);
    let (url, server) = mock_engine();
    // The manifest points at a port nothing listens on; the command line's URL must win.
    write(
        &root,
        "deploy/customer-a.cli.yaml",
        &format!(
            "{PACKAGE_BLOCK}layers: [../app]\ntarget:\n  api: true\n  \
             engine-url: http://127.0.0.1:9\n"
        ),
    );
    let (code, out, err) = run(|io| {
        deploy::execute_deploy(
            deploy_args(root.join("deploy/customer-a.cli.yaml"), Some(url)),
            &GlobalArgs::default(),
            io,
        )
    });
    assert_eq!(code, exit::OK, "out: {out} err: {err}");
    assert!(
        !server.join().unwrap().is_empty(),
        "the CLI URL received the archive"
    );
    std::fs::remove_dir_all(root).ok();
}
