//! Negative fixtures: sealed-archive rejection paths (mutated digest, stowaway,
//! id mismatch) and fail-closed lint fixtures (coverage-without-store, ambiguous
//! channels, undeclared migration store, non-V-numbered scripts). Fixture trees are
//! synthesised in tempdirs — minimal, self-contained.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use sutra_loader::{
    assemble_dir, lint_dir, read_archive, read_archive_expecting, write_archive, PackageError,
    PackageOptions,
};

/// The `package.yaml` every synthetic package directory carries (labels are opaque).
const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

// ---- tiny authoring-tree builder -------------------------------------------------------

fn bpmn(process_id: &str, channel: &str, typed: bool, coverage: bool) -> String {
    let message_type = if typed {
        r#" messageTypeValue="Demo""#
    } else {
        ""
    };
    let coverage_el = if coverage {
        r#"<q:coverage path="main" flows="F1"/>"#
    } else {
        ""
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{process_id}"
                  targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="{process_id}" name="Demo" isExecutable="true">
    <bpmn:extensionElements>{coverage_el}</bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="{channel}"{message_type} dedupKey="payload.id"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

const CHANNELS_YAML: &str = r#"channels:
  - name: demo-in
    transport: http
    bind: "POST /channels/demo-in"
"#;

struct TreeBuilder {
    root: std::path::PathBuf,
}

impl TreeBuilder {
    fn new(root: &Path) -> TreeBuilder {
        TreeBuilder {
            root: root.to_path_buf(),
        }
    }

    fn file(&self, rel: &str, content: &str) -> &TreeBuilder {
        let path = self.root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
        self
    }

    /// The minimal valid package directory: one bpmn, one channel, `package.yaml`.
    fn minimal(&self) -> &TreeBuilder {
        self.file("package.yaml", PACKAGE_YAML)
            .file("bpmn/flow.bpmn", &bpmn("demo-flow", "demo-in", true, false))
            .file("channels.yaml", CHANNELS_YAML)
    }
}

fn error_codes(report: &sutra_loader::LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

// ---- lint negatives ---------------------------------------------------------------------

#[test]
fn minimal_tree_lints_clean_and_packages() {
    let dir = tempfile::tempdir().unwrap();
    TreeBuilder::new(dir.path()).minimal();
    let report = lint_dir(dir.path());
    assert!(
        !report.has_errors(),
        "minimal tree must be clean, got: {:?}",
        error_codes(&report)
    );
    let out = tempfile::tempdir().unwrap();
    let outcome =
        assemble_dir(dir.path(), out.path(), &PackageOptions::default()).expect("packages");
    assert_eq!(outcome.archives.len(), 1);
    read_archive(&std::fs::read(&outcome.archives[0].file_path).unwrap())
        .expect("round-trips clean");
}

#[test]
fn coverage_without_store_is_deploy_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal().file(
        "bpmn/flow.bpmn",
        &bpmn("demo-flow", "demo-in", true, true), // <q:coverage>, no datastores.yaml
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CONFIG.COVERAGE.STORE_MISSING".to_string()),
        "got: {:?}",
        report.diagnostics
    );
    // ONE code path: package must refuse with the same report.
    let out = tempfile::tempdir().unwrap();
    match assemble_dir(dir.path(), out.path(), &PackageOptions::default()) {
        Err(PackageError::Validation(r)) => {
            assert!(error_codes(&r).contains(&"SUTRA.CONFIG.COVERAGE.STORE_MISSING".to_string()))
        }
        other => panic!("package must fail closed, got {other:?}"),
    }
    assert_eq!(
        std::fs::read_dir(out.path()).unwrap().count(),
        0,
        "nothing may be emitted on a failed validation"
    );
}

#[test]
fn coverage_with_declared_store_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal()
        .file("bpmn/flow.bpmn", &bpmn("demo-flow", "demo-in", true, true))
        .file(
            "datastores.yaml",
            "datastores:\n  - name: coverage\n    type: sql\n",
        );
    let report = lint_dir(dir.path());
    assert!(!report.has_errors(), "got: {:?}", error_codes(&report));
}

#[test]
fn ambiguous_catch_all_handlers_are_deploy_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal()
        .file(
            "bpmn/flow.bpmn",
            &bpmn("demo-flow", "demo-in", false, false), // catch-all
        )
        .file(
            "bpmn/other.bpmn",
            &bpmn("other-flow", "demo-in", false, false), // second catch-all claimant
        );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.AMBIGUOUS_HANDLER".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn migrations_for_undeclared_store_are_deploy_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal()
        .file(
            "datastores.yaml",
            "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      migrations: migrations/accounts\n",
        )
        .file(
            "migrations/accounts/V001__init.sql",
            "CREATE TABLE IF NOT EXISTS t (k TEXT);\n",
        )
        .file(
            "migrations/orphan/V001__init.sql",
            "CREATE TABLE IF NOT EXISTS o (k TEXT);\n",
        );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.DEPLOY.MIGRATIONS.STORE_UNDECLARED".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn non_v_numbered_migration_script_is_deploy_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal()
        .file(
            "datastores.yaml",
            "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      migrations: migrations/accounts\n",
        )
        .file(
            "migrations/accounts/init.sql",
            "CREATE TABLE t (k TEXT);\n",
        );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.DEPLOY.MIGRATIONS.SCRIPT_INVALID".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn unknown_codec_is_deploy_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal().file(
        "channels.yaml",
        "channels:\n  - name: demo-in\n    transport: http\n    codec: no-such-codec\n",
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.INBOUND.CODEC_NOT_FOUND".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_broken_dmn_rule_is_deploy_blocking() {
    // The fail-closed rule-load guarantee. A `.dmn` under rules/ that fails to parse is a
    // deploy ERROR (`check_rule_artifacts` propagates the DMN loader's DMN.FILE_PARSE_ERROR), and
    // the package refuses to emit. The sealed-archive model catches the broken rule at
    // DEPLOY, so it never reaches startup — the reference baseline's STARTUP.DMN.LOAD_FAILED
    // is superseded.
    let dir = tempfile::tempdir().unwrap();
    let tree = TreeBuilder::new(dir.path());
    tree.minimal()
        .file("rules/broken.dmn", "<definitions><not-valid-dmn")
        .file(
            "rules/rules-manifest.yaml",
            "rules:\n  - file: broken.dmn\n    messageTypes: [Demo]\n",
        );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.VALIDATE.DMN.FILE_PARSE_ERROR".to_string()),
        "a broken .dmn must fail closed at deploy; got: {:?}",
        report.diagnostics
    );
    // ONE code path: the package must refuse to emit on the same failure.
    let out = tempfile::tempdir().unwrap();
    match assemble_dir(dir.path(), out.path(), &PackageOptions::default()) {
        Err(PackageError::Validation(r)) => assert!(
            error_codes(&r).contains(&"SUTRA.VALIDATE.DMN.FILE_PARSE_ERROR".to_string()),
            "got: {:?}",
            r.diagnostics
        ),
        other => panic!("package must fail closed on a broken rule, got {other:?}"),
    }
    assert_eq!(
        std::fs::read_dir(out.path()).unwrap().count(),
        0,
        "nothing may be emitted on a failed validation"
    );
}

// ---- archive rejection paths ------------------------------------------------------------

/// Package the minimal tree and return the archive bytes + its id.
fn packaged_minimal() -> (Vec<u8>, sutra_loader::DeploymentId) {
    let dir = tempfile::tempdir().unwrap();
    TreeBuilder::new(dir.path()).minimal();
    let out = tempfile::tempdir().unwrap();
    let outcome =
        assemble_dir(dir.path(), out.path(), &PackageOptions::default()).expect("packages");
    let archive = &outcome.archives[0];
    (
        std::fs::read(&archive.file_path).unwrap(),
        archive.id.clone(),
    )
}

/// Decode a `.sutra` into its raw entries (test-side tampering helper).
fn entries_of(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let mut out = BTreeMap::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        out.insert(file.name().to_string(), buf);
    }
    out
}

#[test]
fn mutated_artifact_digest_rejects() {
    let (bytes, _) = packaged_minimal();
    let mut entries = entries_of(&bytes);
    let flow = entries.get_mut("bpmn/flow.bpmn").expect("entry");
    let tampered = String::from_utf8(flow.clone())
        .unwrap()
        .replace("Demo", "Tampered");
    *flow = tampered.into_bytes();
    let tampered_archive = write_archive(&entries).unwrap();
    let err = read_archive(&tampered_archive).expect_err("must reject");
    assert_eq!(err.code, "SUTRA.DEPLOY.ARCHIVE.DIGEST_MISMATCH", "{err}");
}

#[test]
fn stowaway_entry_rejects() {
    let (bytes, _) = packaged_minimal();
    let mut entries = entries_of(&bytes);
    entries.insert(
        "templates/stowaway.hbs".to_string(),
        b"not in the manifest".to_vec(),
    );
    let tampered_archive = write_archive(&entries).unwrap();
    let err = read_archive(&tampered_archive).expect_err("must reject");
    assert_eq!(err.code, "SUTRA.DEPLOY.ARCHIVE.STOWAWAY", "{err}");
}

#[test]
fn manifest_edit_changes_identity_and_expected_id_rejects() {
    let (bytes, original_id) = packaged_minimal();
    // Sanity: the untampered archive verifies against its own id.
    read_archive_expecting(&bytes, &original_id).expect("verifies");

    // Any manifest edit (here: a label) derives a DIFFERENT deploymentId — the sealed
    // content the original id names no longer exists (identity is content-addressed).
    let mut entries = entries_of(&bytes);
    let manifest = entries.get_mut("manifest.yaml").expect("manifest");
    let edited = String::from_utf8(manifest.clone())
        .unwrap()
        .replace("\"t1\"", "\"t2\"");
    assert_ne!(String::from_utf8(manifest.clone()).unwrap(), edited);
    *manifest = edited.into_bytes();
    let tampered_archive = write_archive(&entries).unwrap();
    let err = read_archive_expecting(&tampered_archive, &original_id).expect_err("must reject");
    assert_eq!(err.code, "SUTRA.DEPLOY.ARCHIVE.ID_MISMATCH", "{err}");
}

#[test]
fn missing_manifest_rejects() {
    let (bytes, _) = packaged_minimal();
    let mut entries = entries_of(&bytes);
    entries.remove("manifest.yaml");
    let tampered_archive = write_archive(&entries).unwrap();
    let err = read_archive(&tampered_archive).expect_err("must reject");
    assert_eq!(err.code, "SUTRA.DEPLOY.ARCHIVE.MANIFEST_INVALID", "{err}");
}

#[test]
fn garbage_bytes_reject_as_format_invalid() {
    let err = read_archive(b"definitely not a zip").expect_err("must reject");
    assert_eq!(err.code, "SUTRA.DEPLOY.ARCHIVE.FORMAT_INVALID", "{err}");
}
