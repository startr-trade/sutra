//! Golden fixtures: `lint_dir` + `assemble_dir` over the REAL example
//! deployment-package directories (`examples/<ex>/deployments-src/<pkg>/`). Asserts
//! manifest contents, deterministic byte-identical reruns, lint-cleanliness (no ERROR
//! diagnostics), and reader round-trips. `.sutra` is the only deployment model, so the
//! standalone package directory is the sole authoring input here.
//!
//! All three public examples (money-transfer, approval-hold, call-log-load) are single-variant:
//! they commit a
//! `deployments-src/<pkg>/` directly, which is what these fixtures package. The MULTI-variant
//! layout (`shared/` + `variants/<name>/`, composed before sealing) belongs to the proprietary
//! example packages, so its packaging gate lives in the repository that owns them; the loader
//! itself is variant-agnostic — composition happens before it ever sees a directory.
//!
//! Nested-subfolder artifact ids ('/'-subpaths, depth ≤ 8) are covered by the money-transfer
//! fixture below (`migrations/<store>/…`, `schemas/<codec>/…`).

use std::path::{Path, PathBuf};

use sutra_loader::{assemble_dir, lint_dir, read_archive_file, PackageOptions};

/// The committed standalone package directory of a single-variant example (the archive source).
fn package_dir(example: &str, pkg: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples")
        .join(example)
        .join("deployments-src")
        .join(pkg)
}

fn assert_lint_clean(dir: &Path) {
    let report = lint_dir(dir);
    let errors: Vec<String> = report.errors().map(|d| d.to_string()).collect();
    assert!(
        errors.is_empty(),
        "package {} must lint clean, got:\n{}",
        dir.display(),
        errors.join("\n")
    );
}

/// Seal one package directory twice into two temp dirs and assert byte-identical reruns
/// (the container determinism pin); returns the archive bytes.
fn seal_twice(dir: &Path) -> Vec<u8> {
    let out_one = tempfile::tempdir().expect("tempdir");
    let out_two = tempfile::tempdir().expect("tempdir");
    let one = assemble_dir(dir, out_one.path(), &PackageOptions::default())
        .unwrap_or_else(|e| panic!("package {} must seal: {e:?}", dir.display()));
    let two = assemble_dir(dir, out_two.path(), &PackageOptions::default())
        .unwrap_or_else(|e| panic!("package {} must seal: {e:?}", dir.display()));
    assert!(!one.report.has_errors());
    let a = &one.archives[0];
    let b = &two.archives[0];
    assert_eq!(a.id, b.id, "content-addressed id is stable across reruns");
    let bytes_a = std::fs::read(&a.file_path).expect("archive bytes");
    let bytes_b = std::fs::read(&b.file_path).expect("archive bytes");
    assert_eq!(bytes_a, bytes_b, "double-seal must be byte-identical");
    bytes_a
}

#[test]
fn money_transfer_packages_and_round_trips() {
    let dir = package_dir("money-transfer", "default--money-transfer--1.0.0");
    assert_lint_clean(&dir);
    let out = tempfile::tempdir().expect("tempdir");
    let outcome = assemble_dir(&dir, out.path(), &PackageOptions::default()).expect("packages");
    assert_eq!(outcome.archives.len(), 1, "one package = one archive");
    let archive = &outcome.archives[0];
    assert_eq!(
        archive.file_path.file_name().unwrap().to_string_lossy(),
        "default--money-transfer--1.0.0.sutra"
    );

    // ---- manifest content ----
    let manifest = &archive.manifest;
    assert_eq!(manifest.manifest_version, 1);
    assert_eq!(manifest.engine_min_contract, 1);
    assert_eq!(manifest.labels["tenant"], "default");
    assert_eq!(manifest.labels["module"], "money-transfer");
    assert_eq!(manifest.labels["version"], "1.0.0");
    assert!(manifest.supersedes.is_empty());
    assert_eq!(
        manifest.entry_processes,
        vec![
            "balance-query",
            "coverage-report",
            "coverage-reset",
            "transfer"
        ],
        "every process here is channel-reachable"
    );
    let paths: Vec<&str> = manifest.artifacts.iter().map(|a| a.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "bpmn/balance-query.bpmn",
            "bpmn/coverage-report.bpmn",
            "bpmn/coverage-reset.bpmn",
            "bpmn/transfer.bpmn",
            "channels.yaml",
            "datastores.yaml",
            "migrations/accounts/V001__accounts.sql",
            "schemas/transfer/codec-manifest.yaml",
            "schemas/transfer/transfer.xsd",
            "templates/balance.hbs",
            "templates/coverage-report.hbs",
            "templates/coverage-reset.hbs",
            "templates/transfer-rejected.hbs",
            "templates/transfer-result.hbs",
        ],
        "byte-sorted artifact set"
    );
    for artifact in &manifest.artifacts {
        assert_eq!(artifact.sha256.len(), 64);
    }

    // deploymentId = dep- + first 24 hex of sha256(manifest bytes) — the normative derivation.
    assert!(archive.id.value().starts_with("dep-"));

    // ---- reader round-trip ----
    let loaded = read_archive_file(&archive.file_path).expect("verifies and loads");
    assert_eq!(loaded.id, archive.id, "recomputed id matches");
    assert_eq!(loaded.deployment.tenant, "default");
    assert_eq!(loaded.deployment.module, "money-transfer");
    assert_eq!(
        loaded.deployment.namespace,
        "urn:sutra:module:money-transfer:1.0.0"
    );
    let pids: Vec<&str> = loaded
        .deployment
        .processes
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        pids,
        vec![
            "balance-query",
            "coverage-report",
            "coverage-reset",
            "transfer"
        ]
    );
    assert_eq!(loaded.deployment.templates.len(), 5);
    assert_eq!(loaded.deployment.codecs.len(), 1);
    assert!(loaded.deployment.codecs.contains_key("transfer"));
    // One migration only: `migrations/accounts`. The `coverage` store ships none — the engine
    // owns the coverage schema and applies it to the declared store on first use (§7).
    assert_eq!(loaded.deployment.migrations.len(), 1);
    assert!(loaded.deployment.channels_yaml.is_some());
    assert!(loaded.deployment.datastores_yaml.is_some());

    seal_twice(&dir);
}

#[test]
fn approval_hold_packages_one_archive() {
    let dir = package_dir("approval-hold", "default--approval--1.0.0");
    assert_lint_clean(&dir);
    seal_twice(&dir);

    let out = tempfile::tempdir().expect("tempdir");
    let outcome = assemble_dir(&dir, out.path(), &PackageOptions::default()).expect("packages");
    let archive = &outcome.archives[0];
    assert_eq!(
        archive.file_path.file_name().unwrap().to_string_lossy(),
        "default--approval--1.0.0.sutra"
    );
    // rules/ (the merged decision) + scripts/ + the .xsl template travel.
    let paths: Vec<&str> = archive
        .manifest
        .artifacts
        .iter()
        .map(|a| a.path.as_str())
        .collect();
    assert!(paths.contains(&"rules/approval-decide.dmn"));
    assert!(paths.contains(&"scripts/derive-decision.hbs"));
    assert!(paths.contains(&"templates/transform.xsl"));
    assert!(
        !paths.contains(&"datastores.yaml"),
        "approval declares no stores"
    );

    let loaded = read_archive_file(&archive.file_path).expect("round-trips");
    assert_eq!(loaded.deployment.rules.len(), 1);
    assert_eq!(loaded.deployment.scripts.len(), 2);
}

#[test]
fn call_log_load_packages_one_archive() {
    let dir = package_dir("call-log-load", "default--call-log--1.0.0");
    assert_lint_clean(&dir);
    seal_twice(&dir);

    let out = tempfile::tempdir().expect("tempdir");
    let outcome = assemble_dir(&dir, out.path(), &PackageOptions::default()).expect("packages");
    let archive = &outcome.archives[0];
    assert_eq!(
        archive.file_path.file_name().unwrap().to_string_lossy(),
        "default--call-log--1.0.0.sutra"
    );
    let paths: Vec<&str> = archive
        .manifest
        .artifacts
        .iter()
        .map(|a| a.path.as_str())
        .collect();
    // The transform is a SCRIPT (its render merges into variables), the receipt is a TEMPLATE
    // (its render is the reply) — the distinction this example turns on, pinned as artifacts.
    assert!(paths.contains(&"scripts/call-log-entry.hbs"), "{paths:?}");
    assert!(paths.contains(&"templates/batch-accepted.hbs"), "{paths:?}");
    // Two codecs: the csv-bound INBOUND schema and the STORAGE schema the projected store
    // declares as its row type.
    assert!(paths.contains(&"schemas/cdr/cdr.xsd"), "{paths:?}");
    assert!(
        paths.contains(&"schemas/call-log/call-log.xsd"),
        "{paths:?}"
    );
    assert!(
        paths.contains(&"migrations/call_log/V001__call_log.sql"),
        "{paths:?}"
    );
}
