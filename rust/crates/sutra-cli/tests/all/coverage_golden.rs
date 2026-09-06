//! Exit proof: regenerate money-transfer's hand-authored coverage set from its
//! `transfer.bpmn` and compare STRUCTURALLY against the golden files
//! (`examples/money-transfer/deployments-src/default--money-transfer--1.0.0/**`).
//!
//! The golden source is the committed standalone deployment package — the shape
//! `sutra coverage init` targets; the proof copies its content into a scratch package dir,
//! strips the hand-authored coverage artifacts, regenerates them, and asserts:
//!
//! - the seeded `<q:coverage>` declarations carry exactly the golden flow routes (path IDS
//!   differ by design: the generator names `path-N`; `accept`/`reject` are hand renames —
//!   and the rename-preservation test proves they survive regeneration);
//! - the generated admin pair is model-equal (engine loader) AND byte-equal modulo
//!   comments to the golden `coverage-report.bpmn` / `coverage-reset.bpmn`;
//! - the generated channels/store entries parse through the engine's own loaders into
//!   definitions equal to the golden ones;
//! - `coverage check` passes clean on the regenerated package;
//! - re-running init over the PRISTINE golden content is a no-op: up-to-date detection,
//!   and the hand-authored files carry no generated-by marker so they are never touched.

use std::path::{Path, PathBuf};

use sutra_cli::commands::coverage::{self, CoverageAction, CoverageArgs, InitArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;

/// The committed standalone deployment package (archive source) — a flat package dir; the
/// former split module/binding roots both resolve here now.
fn golden_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/money-transfer/deployments-src/default--money-transfer--1.0.0")
}

fn golden_module() -> PathBuf {
    golden_root()
}

fn golden_binding() -> PathBuf {
    golden_root()
}

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sutra-cli-golden-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Assemble the golden content as one standalone deployment package. `pristine` keeps the
/// hand-authored coverage artifacts; otherwise they are stripped (pre-init state).
fn assemble_package(label: &str, pristine: bool) -> PathBuf {
    let pkg = scratch(label);
    std::fs::create_dir_all(pkg.join("bpmn")).unwrap();
    std::fs::create_dir_all(pkg.join("templates")).unwrap();

    let bpmn = read(&golden_module().join("bpmn/transfer.bpmn"));
    let channels = read(&golden_binding().join("channels.yaml"));
    let datastores = read(&golden_binding().join("datastores.yaml"));

    if pristine {
        std::fs::write(pkg.join("bpmn/transfer.bpmn"), &bpmn).unwrap();
        std::fs::write(pkg.join("channels.yaml"), &channels).unwrap();
        std::fs::write(pkg.join("datastores.yaml"), &datastores).unwrap();
        for f in ["coverage-report.bpmn", "coverage-reset.bpmn"] {
            std::fs::copy(
                golden_module().join("bpmn").join(f),
                pkg.join("bpmn").join(f),
            )
            .unwrap();
        }
        for f in ["coverage-report.hbs", "coverage-reset.hbs"] {
            std::fs::copy(
                golden_module().join("templates").join(f),
                pkg.join("templates").join(f),
            )
            .unwrap();
        }
    } else {
        // Strip the hand-authored <q:coverage> declarations (single-line elements).
        let stripped: String = bpmn
            .lines()
            .filter(|l| !l.trim_start().starts_with("<q:coverage "))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(pkg.join("bpmn/transfer.bpmn"), stripped).unwrap();
        // Strip the coverage admin channels / store (the golden files declare them last,
        // under a marker comment).
        let channels = channels
            .split("  # Coverage ADMIN channels")
            .next()
            .unwrap()
            .trim_end()
            .to_string()
            + "\n";
        std::fs::write(pkg.join("channels.yaml"), channels).unwrap();
        let datastores = datastores
            .split("  # The path-coverage covered-set")
            .next()
            .unwrap()
            .trim_end()
            .to_string()
            + "\n";
        std::fs::write(pkg.join("datastores.yaml"), datastores).unwrap();
    }
    pkg
}

fn run_init(bpmn: &Path) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        coverage::execute(
            CoverageArgs {
                action: CoverageAction::Init(InitArgs {
                    file: bpmn.to_path_buf(),
                    process_ids: Vec::new(),
                    single: false,
                    package: None,
                    process: None,
                    max_paths: 256,
                    force: false,
                }),
            },
            &GlobalArgs::default(),
            &mut io,
        )
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn run_check(bpmn: &Path) -> (i32, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        coverage::execute(
            CoverageArgs {
                action: CoverageAction::Check(coverage::CheckArgs {
                    bpmn_file: Some(bpmn.to_path_buf()),
                    process: None,
                    max_paths: 256,
                    archive: None,
                    database_url: None,
                    db_user: None,
                    db_password: None,
                    threshold: 100.0,
                }),
            },
            &GlobalArgs::default(),
            &mut io,
        )
    };
    (code, String::from_utf8(out).unwrap())
}

/// Comment-stripped, trailing-whitespace-trimmed, blank-line-free view of an XML file.
fn normalize_xml(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    // Strip <!-- --> comments (multi-line).
    let mut stripped = String::new();
    while let Some(start) = rest.find("<!--") {
        stripped.push_str(&rest[..start]);
        match rest[start..].find("-->") {
            Some(end) => rest = &rest[start + end + 3..],
            None => {
                rest = "";
                break;
            }
        }
    }
    stripped.push_str(rest);
    for line in stripped.lines() {
        let line = line.trim_end();
        if !line.trim().is_empty() {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn normalize_hbs(text: &str) -> String {
    let mut stripped = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{!--") {
        stripped.push_str(&rest[..start]);
        match rest[start..].find("--}}") {
            Some(end) => rest = &rest[start + end + 4..],
            None => {
                rest = "";
                break;
            }
        }
    }
    stripped.push_str(rest);
    stripped
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn regenerated_coverage_set_is_structurally_equivalent_to_the_golden_files() {
    let pkg = assemble_package("regen", false);
    let bpmn = pkg.join("bpmn/transfer.bpmn");

    let (code, out, err) = run_init(&bpmn);
    assert_eq!(code, exit::OK, "init failed\nout: {out}\nerr: {err}");

    // --- 1. Seeded declarations == golden routes (ids generated, flows identical) -------
    let loader = sutra_bpmn::BpmnModelLoader::new();
    let golden_module_model = loader
        .load(read(&golden_module().join("bpmn/transfer.bpmn")).as_bytes())
        .unwrap();
    let golden_paths = &golden_module_model
        .process("transfer")
        .unwrap()
        .coverage_paths;
    let seeded_model = loader.load(read(&bpmn).as_bytes()).unwrap();
    let seeded_paths = &seeded_model.process("transfer").unwrap().coverage_paths;

    let golden_flows: std::collections::BTreeSet<Vec<String>> =
        golden_paths.iter().map(|p| p.flows.clone()).collect();
    let seeded_flows: std::collections::BTreeSet<Vec<String>> =
        seeded_paths.iter().map(|p| p.flows.clone()).collect();
    assert_eq!(
        golden_flows, seeded_flows,
        "the enumerator must reproduce exactly the hand-authored routes"
    );
    assert_eq!(seeded_paths.len(), 2);
    // Intentional difference: ids are generator-named (path-1/path-2 in discovery order —
    // accept first); `accept`/`reject` are semantic hand renames the generator cannot infer.
    assert_eq!(seeded_paths[0].id, "path-1");
    assert_eq!(seeded_paths[0].flows, vec!["Flow_TxToOk", "Flow_OkToEnd"]);
    assert_eq!(seeded_paths[1].id, "path-2");
    assert_eq!(
        seeded_paths[1].flows,
        vec!["Flow_CancelToReject", "Flow_RejectToEnd"]
    );

    // Everything else in the business process is untouched byte-for-byte: restoring the
    // golden ids yields the golden file's non-comment content for the coverage lines.
    let seeded_text = read(&bpmn);
    assert!(seeded_text.contains(r#"<q:coverage path="path-1" flows="Flow_TxToOk Flow_OkToEnd"/>"#));
    assert!(seeded_text
        .contains(r#"<q:coverage path="path-2" flows="Flow_CancelToReject Flow_RejectToEnd"/>"#));

    // --- 2. Admin pair: engine-model equality + byte equality modulo comments ----------
    for file in ["coverage-report.bpmn", "coverage-reset.bpmn"] {
        let generated = read(&pkg.join("bpmn").join(file));
        let golden = read(&golden_module().join("bpmn").join(file));
        let generated_model = loader.load(generated.as_bytes()).unwrap();
        let golden_model = loader.load(golden.as_bytes()).unwrap();
        assert_eq!(
            generated_model, golden_model,
            "{file}: generated admin flow must be model-identical to the golden one"
        );
        assert_eq!(
            normalize_xml(&generated),
            normalize_xml(&golden),
            "{file}: generated admin flow must be byte-identical modulo comments"
        );
    }

    // --- 3. Templates: byte equality modulo comments ------------------------------------
    for file in ["coverage-report.hbs", "coverage-reset.hbs"] {
        let generated = read(&pkg.join("templates").join(file));
        let golden = read(&golden_module().join("templates").join(file));
        assert_eq!(
            normalize_hbs(&generated),
            normalize_hbs(&golden),
            "{file}: generated template must match the golden one modulo comments"
        );
    }

    // --- 4. Channels: the appended admin pair parses into definitions equal to golden ---
    let namespace = ("default", "money-transfer", "1.0.0");
    let parse = |text: &str| {
        sutra_channels::config::load_channel_definitions(
            text.as_bytes(),
            namespace.0,
            namespace.1,
            namespace.2,
            "channels.yaml",
        )
        .unwrap()
    };
    let generated_channels = parse(&read(&pkg.join("channels.yaml")));
    let golden_channels = parse(&read(&golden_binding().join("channels.yaml")));
    for name in ["coverage-query", "coverage-reset"] {
        let generated = generated_channels
            .iter()
            .find(|c| c.binding.channel_name == name)
            .unwrap_or_else(|| panic!("generated channels.yaml lacks {name}"));
        let golden = golden_channels
            .iter()
            .find(|c| c.binding.channel_name == name)
            .unwrap();
        assert_eq!(generated, golden, "channel {name} must be definition-equal");
    }
    // The pre-existing business channels stayed untouched (append-only wiring).
    assert_eq!(
        generated_channels.len(),
        golden_channels.len(),
        "same channel set as golden after regeneration"
    );

    // --- 5. Store: the coverage store parses into a definition equal to golden ----------
    let generated_stores =
        sutra_datastore::config::parse_datastores(&read(&pkg.join("datastores.yaml"))).unwrap();
    let golden_stores =
        sutra_datastore::config::parse_datastores(&read(&golden_binding().join("datastores.yaml")))
            .unwrap();
    let find = |stores: &[sutra_datastore::config::StoreDefinition]| {
        stores
            .iter()
            .find(|s| s.name == "coverage")
            .cloned()
            .expect("coverage store")
    };
    assert_eq!(
        find(&generated_stores),
        find(&golden_stores),
        "coverage store must be definition-equal (same shared connection, no migrations)"
    );

    // --- 6. No coverage SQL, anywhere -----------------------------------------------------
    // The engine owns the coverage schema and applies it to the declared store on first use
    //, so neither the scaffolder nor the golden package
    // ships coverage DDL.
    assert!(!find(&generated_stores)
        .properties
        .contains_key("sql.migrations"));
    assert!(!pkg.join("migrations/coverage").exists());
    assert!(!golden_binding().join("migrations/coverage").exists());

    // --- 7. The regenerated package lints clean ------------------------------------------
    let (code, out) = run_check(&bpmn);
    assert_eq!(code, exit::OK, "coverage check must pass clean:\n{out}");
    assert!(out.contains("0 error(s)"), "{out}");
}

#[test]
fn init_over_the_pristine_golden_set_is_a_no_op() {
    let pkg = assemble_package("pristine", true);
    let bpmn = pkg.join("bpmn/transfer.bpmn");
    let before_bpmn = read(&bpmn);
    let before_channels = read(&pkg.join("channels.yaml"));
    let before_datastores = read(&pkg.join("datastores.yaml"));
    let before_report = read(&pkg.join("bpmn/coverage-report.bpmn"));

    let (code, out, err) = run_init(&bpmn);
    assert_eq!(code, exit::OK, "out: {out}\nerr: {err}");

    // Up-to-date detection: the hand-authored ids (accept/reject) match the enumerated
    // routes, so the declarations are kept verbatim.
    assert!(out.contains("2 kept, 0 new"), "{out}");
    assert_eq!(read(&bpmn), before_bpmn, "business bpmn untouched");
    assert_eq!(
        read(&pkg.join("channels.yaml")),
        before_channels,
        "channels.yaml untouched (admin pair already declared)"
    );
    assert_eq!(
        read(&pkg.join("datastores.yaml")),
        before_datastores,
        "datastores.yaml untouched (coverage store already declared)"
    );
    // The hand-authored admin files carry no generated-by marker → user-owned → untouched
    // without --force, surfaced as a warning.
    assert_eq!(
        read(&pkg.join("bpmn/coverage-report.bpmn")),
        before_report,
        "hand-authored admin flow untouched"
    );
    assert!(
        err.contains("user edits") || out.contains("skipped (user file"),
        "user-file skip surfaced\nout: {out}\nerr: {err}"
    );

    // And the pristine golden set passes the drift lint.
    let (code, out) = run_check(&bpmn);
    assert_eq!(code, exit::OK, "{out}");
    assert!(out.contains("0 error(s)"), "{out}");
}

// ------------------------------------------- cross-process connectable-graph generation

/// `alpha`: spawns `beta` (`<q:send channel=beta-in>` → beta's start-event `<q:source>`) and
/// parks on an imec relay-wait (`<q:source channel=alpha-reply>`) for the reply.
const ALPHA_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:deployment:recon-alpha">
  <bpmn:process id="alpha" isExecutable="true">
    <bpmn:startEvent id="alphaStart">
      <bpmn:extensionElements>
        <q:source channel="alpha-in" ack="on-complete"/>
        <q:alias name="txnId" expression="payload.txnId"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>aF1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="aF1" sourceRef="alphaStart" targetRef="alphaSend"/>
    <bpmn:sendTask id="alphaSend">
      <bpmn:extensionElements>
        <q:send channel="beta-in"/>
      </bpmn:extensionElements>
      <bpmn:incoming>aF1</bpmn:incoming>
      <bpmn:outgoing>aF2</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="aF2" sourceRef="alphaSend" targetRef="alphaWait"/>
    <bpmn:intermediateCatchEvent id="alphaWait">
      <bpmn:extensionElements>
        <q:source channel="alpha-reply"/>
        <q:alias name="alphaRef" expression="payload.alphaRef"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
      <bpmn:incoming>aF2</bpmn:incoming>
      <bpmn:outgoing>aF3</bpmn:outgoing>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="aF3" sourceRef="alphaWait" targetRef="alphaEnd"/>
    <bpmn:endEvent id="alphaEnd"/>
  </bpmn:process>
</bpmn:definitions>
"#;

/// `beta`: spawned on `beta-in`, forks on a gateway, then replies (`<q:send channel=alpha-reply>`
/// → alpha's imec). The fork gives a non-trivial intra-process adjacency to draw from.
const BETA_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:deployment:recon-beta">
  <bpmn:process id="beta" isExecutable="true">
    <bpmn:startEvent id="betaStart">
      <bpmn:extensionElements>
        <q:source channel="beta-in" ack="on-complete"/>
        <q:alias name="txnId" expression="payload.txnId"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>bF1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="bF1" sourceRef="betaStart" targetRef="betaGw"/>
    <bpmn:exclusiveGateway id="betaGw" default="bOk"/>
    <bpmn:sequenceFlow id="bKo" sourceRef="betaGw" targetRef="betaReply">
      <bpmn:conditionExpression>reject</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="bOk" sourceRef="betaGw" targetRef="betaReply"/>
    <bpmn:sendTask id="betaReply">
      <bpmn:extensionElements>
        <q:send channel="alpha-reply"/>
      </bpmn:extensionElements>
      <bpmn:incoming>bKo</bpmn:incoming>
      <bpmn:incoming>bOk</bpmn:incoming>
      <bpmn:outgoing>bF2</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="bF2" sourceRef="betaReply" targetRef="betaEnd"/>
    <bpmn:endEvent id="betaEnd"/>
  </bpmn:process>
</bpmn:definitions>
"#;

fn run_init_cross(pkg: &Path, file: &str, pids: &[&str], single: bool) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        coverage::execute(
            CoverageArgs {
                action: CoverageAction::Init(InitArgs {
                    file: PathBuf::from(file),
                    process_ids: pids.iter().map(|s| s.to_string()).collect(),
                    single,
                    package: Some(pkg.to_path_buf()),
                    process: None,
                    max_paths: 256,
                    force: false,
                }),
            },
            &GlobalArgs::default(),
            &mut io,
        )
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

#[test]
fn init_cross_process_emits_connectable_graph_scaffold() {
    let pkg = scratch("cov-xproc");
    std::fs::create_dir_all(pkg.join("bpmn")).unwrap();
    std::fs::write(pkg.join("bpmn/alpha.bpmn"), ALPHA_BPMN).unwrap();
    std::fs::write(pkg.join("bpmn/beta.bpmn"), BETA_BPMN).unwrap();

    let (code, out, err) = run_init_cross(&pkg, "recon/e2e", &["alpha", "beta"], false);
    assert_eq!(code, exit::OK, "out={out} err={err}");

    // The scaffold lands under coverage/ carrying the phase-1-derived URN — and does NOT seed
    // <q:coverage> into any BPMN (that is the single-process form's behaviour).
    let path = pkg.join("coverage/recon/e2e.yaml");
    assert!(path.is_file(), "scaffold written");
    let text = read(&path);
    let urn = sutra_loader::coverage::coverage_urn("recon/e2e.yaml");
    assert_eq!(urn, "urn:sutra:coverage:recon:e2e");
    assert!(
        text.contains(&format!("urn: {urn}")),
        "URN in header: {text}"
    );
    assert!(
        !read(&pkg.join("bpmn/alpha.bpmn")).contains("<q:coverage"),
        "no <q:coverage> seeded into the BPMN"
    );

    // Intra-process sequence-flow adjacency (A.targetRef == B.sourceRef), incl. beta's fork.
    assert!(
        text.contains("process alpha:"),
        "alpha adjacency block: {text}"
    );
    assert!(text.contains("aF1 -> aF2"), "alpha contiguity: {text}");
    assert!(
        text.contains("bF1 -> bKo, bOk"),
        "beta fork adjacency: {text}"
    );

    // Inter-process hops: spawn (start-event <q:source>) + relay (imec), each ack-marked.
    assert!(
        text.contains("alpha:alphaSend --beta-in--> beta:betaStart"),
        "spawn hop: {text}"
    );
    assert!(
        text.contains("[spawn request-reply]"),
        "spawn ack-mode: {text}"
    );
    assert!(
        text.contains("beta:betaReply --alpha-reply--> alpha:alphaWait"),
        "relay hop: {text}"
    );
    assert!(
        text.contains("[relay fire-and-forget]"),
        "relay ack-mode: {text}"
    );

    // The emitted file deserializes into the loader's coverage model (a valid coverage file).
    let file: sutra_loader::CoverageFile =
        serde_yaml_ng::from_str(&text).expect("emitted scaffold is a valid coverage file");
    assert_eq!(file.correlations.len(), 1);
    let corr = &file.correlations[0];
    assert_eq!(corr.id, "e2e", "correlation id = file stem");
    assert_eq!(corr.key, "txnId", "inferred default hop key");
    // Two inferred links; the relay leg overrides the key (its consumer alias differs).
    assert_eq!(corr.links.len(), 2, "inferred hops become links");
    assert_eq!(corr.links[0].from_node, "alpha:alphaSend");
    assert_eq!(corr.links[0].to_node, "beta:betaStart");
    assert_eq!(corr.links[0].key, None, "spawn leg uses the default key");
    assert_eq!(corr.links[1].from_node, "beta:betaReply");
    assert_eq!(corr.links[1].to_node, "alpha:alphaWait");
    assert_eq!(corr.links[1].key.as_deref(), Some("alphaRef"));
    // A coverages starter: one path, per-process connectable flow ids (segments) to trim.
    assert_eq!(corr.coverages.len(), 1);
    let route = &corr.coverages[0];
    assert_eq!(route.path, "path-1");
    assert_eq!(route.segments["alpha"], vec!["aF1", "aF2", "aF3"]);
    assert!(
        route.segments["beta"].contains(&"bKo".to_string()),
        "beta segment lists its connectable flow ids"
    );

    // Re-run refuses to overwrite without --force.
    let (code2, _, err2) = run_init_cross(&pkg, "recon/e2e", &["alpha", "beta"], false);
    assert_eq!(code2, exit::FINDINGS, "no clobber without --force");
    assert!(err2.contains("--force"), "{err2}");

    // --single restricts to intra-process adjacency: no inter-process hops / links.
    let (code3, out3, err3) = run_init_cross(&pkg, "recon/single", &["alpha"], true);
    assert_eq!(code3, exit::OK, "out={out3} err={err3}");
    let single_text = read(&pkg.join("coverage/recon/single.yaml"));
    assert!(
        single_text.contains("links: []"),
        "single form emits no hops: {single_text}"
    );
    assert!(
        single_text.contains("--single"),
        "single note: {single_text}"
    );
}

#[test]
fn init_cross_process_unknown_process_is_usage_error() {
    let pkg = scratch("cov-xproc-unknown");
    std::fs::create_dir_all(pkg.join("bpmn")).unwrap();
    std::fs::write(pkg.join("bpmn/alpha.bpmn"), ALPHA_BPMN).unwrap();
    let (code, _out, err) = run_init_cross(&pkg, "recon/e2e", &["alpha", "ghost"], false);
    assert_eq!(code, exit::USAGE);
    assert!(err.contains("'ghost' not found"), "{err}");
    assert!(
        !pkg.join("coverage/recon/e2e.yaml").exists(),
        "nothing written on a bad processId"
    );
}
