//! End-to-end tests over the fixture package in `tests/fixtures/mini-package/`: discovery,
//! per-artifact-type rendering, determinism, manual-notes preservation, and `--check` drift.

use std::path::{Path, PathBuf};

use sutra_docgen::{check, discover, render, Config};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mini-package")
}

// ---- discovery ----------------------------------------------------------------------------

#[test]
fn discovers_one_package_and_classifies_every_artifact() {
    let tree = discover::discover(&fixture_root()).expect("discover");
    assert_eq!(tree.packages.len(), 1);
    let pkg = &tree.packages[0];
    assert_eq!(pkg.rel, "acme--mini-package--1.0.0");
    assert_eq!(
        pkg.tenant_module_version(),
        Some((
            "acme".to_string(),
            "mini-package".to_string(),
            "1.0.0".to_string()
        ))
    );
    assert_eq!(pkg.bpmn, ["acme--mini-package--1.0.0/bpmn/flow.bpmn"]);
    assert_eq!(pkg.dmn, ["acme--mini-package--1.0.0/rules/decide.dmn"]);
    assert_eq!(pkg.srl, ["acme--mini-package--1.0.0/rules/extra.srl"]);
    assert_eq!(
        pkg.templates,
        ["acme--mini-package--1.0.0/templates/reply.hbs"]
    );
    assert!(pkg.channels_yaml.is_some());
    assert!(pkg.package_yaml.is_some());
    // Manifests are discovered CO-LOCATED inside rules/ and templates/ (not the package root).
    assert_eq!(
        pkg.rules_manifest,
        ["acme--mini-package--1.0.0/rules/rules-manifest.yaml"]
    );
    assert_eq!(
        pkg.template_manifest,
        ["acme--mini-package--1.0.0/templates/template-manifest.yaml"]
    );
    // C6: coverage/** YAML is a first-class artifact type — classified into `coverage`, NOT the
    // generic `other_yaml` bucket.
    assert_eq!(
        pkg.coverage,
        ["acme--mini-package--1.0.0/coverage/orders/e2e.yaml"]
    );
    assert!(
        pkg.other_yaml.is_empty(),
        "coverage/** must not fall through to other_yaml: {:?}",
        pkg.other_yaml
    );
}

// ---- per-artifact-type rendering ------------------------------------------------------------

fn find<'a>(pages: &'a [render::Page], path: &str) -> &'a render::Page {
    pages
        .iter()
        .find(|p| p.path == path)
        .unwrap_or_else(|| panic!("page not found: {path}"))
}

#[test]
fn bpmn_page_summarises_tasks_and_has_no_wait_states() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);
    let page = find(&pages, "acme--mini-package--1.0.0/bpmn/flow.md");

    assert!(page.content.contains("Process `balance-query`"));
    assert!(page.content.contains("Load account (read-only)"));
    assert!(page.content.contains("Data task"));
    assert!(page.content.contains("implementation=reply.hbs"));
    assert!(
        page.content
            .contains("No wait states — this process runs to completion synchronously."),
        "a plain serviceTask reply must not be classified as a wait state:\n{}",
        page.content
    );
    // Never raw XML.
    assert!(!page.content.contains("<bpmn:process"));
}

#[test]
fn dmn_page_renders_decision_table_and_manifest_applicability() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);
    let page = find(&pages, "acme--mini-package--1.0.0/rules/decide.md");

    assert!(page.content.contains("Decision `decide`"));
    assert!(page.content.contains("**Hit policy:** FIRST"));
    assert!(
        page.content.contains("BalanceQuery"),
        "rules-manifest.yaml applicability must show"
    );
    assert!(page.content.contains("\"sufficient\""));
    assert!(page.content.contains("\"insufficient\""));
}

#[test]
fn srl_page_renders_rule_agenda_conditions_and_actions() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);
    let page = find(&pages, "acme--mini-package--1.0.0/rules/extra.md");

    assert!(page.content.contains("Rule `extra check`"));
    assert!(page.content.contains("**Salience:** 5"));
    assert!(page.content.contains("`balance`"));
    assert!(page.content.contains("balance < 0"));
    assert!(page.content.contains("`set`"));
    assert!(page.content.contains("`report`"));
    assert!(
        page.content.contains("BalanceQuery"),
        "rules-manifest.yaml applicability must show for .srl too"
    );
    // Never the old stub note — this is a real parsed page now.
    assert!(!page.content.contains("SUTRA-DOCGEN NOTE"));
    assert!(!page.content.contains("no Rust parser yet"));
}

#[test]
fn template_page_shows_manifest_entry_and_extracted_description() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);
    let page = find(&pages, "acme--mini-package--1.0.0/templates/reply.md");

    assert!(page.content.contains("BalanceQuery"));
    assert!(page.content.contains("BalanceReply"));
    assert!(page.content.contains("application/xml"));
    assert!(page
        .content
        .contains("Renders the BalanceReply payload for the fixture flow."));
}

#[test]
fn channels_and_package_yaml_render_as_tables() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);

    let channels = find(&pages, "acme--mini-package--1.0.0/channels.md");
    assert!(channels.content.contains("`name`"));
    assert!(channels.content.contains("balance-response"));

    let package = find(&pages, "acme--mini-package--1.0.0/package.md");
    assert!(package.content.contains("`tenant`"));
    assert!(package.content.contains("acme"));
    assert!(package.content.contains("`minContract`"));
}

#[test]
fn coverage_file_renders_as_first_class_page_with_correlations_and_routes() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);

    // A dedicated coverage page exists (extension swapped to .md), NOT rendered as generic Config.
    let page = find(&pages, "acme--mini-package--1.0.0/coverage/orders/e2e.md");
    assert!(page.content.contains("Coverage — `e2e.yaml`"));
    assert!(page.content.contains("## Correlations"));
    assert!(page.content.contains("`transfer`"));
    assert!(page.content.contains("`txnId`"));
    assert!(page.content.contains("## Coverage routes"));
    assert!(page.content.contains("`reply1`"));
    assert!(page.content.contains("`reply2`"));

    // The package index links the coverage artifact under its own section.
    let index = find(&pages, "acme--mini-package--1.0.0/index.md");
    assert!(index.content.contains("Coverage routes (cross-process)"));
    assert!(index.content.contains("coverage/orders/e2e.md"));
}

#[test]
fn root_index_lists_the_package() {
    let tree = discover::discover(&fixture_root()).unwrap();
    let pages = render::generate_pages(&tree);
    let root = find(&pages, "index.md");
    assert!(root.content.contains("acme--mini-package--1.0.0"));
}

// ---- end-to-end: run, determinism, manual notes, --check -----------------------------------

#[test]
fn run_is_deterministic_and_check_detects_drift() {
    let out1 = tempfile::tempdir().unwrap();
    let out2 = tempfile::tempdir().unwrap();
    let cfg1 = Config::new(fixture_root(), Some(out1.path().to_path_buf()));
    let cfg2 = Config::new(fixture_root(), Some(out2.path().to_path_buf()));

    let r1 = sutra_docgen::run(&cfg1).expect("run 1");
    let r2 = sutra_docgen::run(&cfg2).expect("run 2");
    assert_eq!(r1.pages, r2.pages);
    assert_eq!(r1.packages, 1);

    let mut paths = Vec::new();
    collect_files(out1.path(), out1.path(), &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    for rel in &paths {
        let a = std::fs::read_to_string(out1.path().join(rel)).unwrap();
        let b = std::fs::read_to_string(out2.path().join(rel)).unwrap();
        assert_eq!(a, b, "page {rel} differs between two runs");
    }

    let drift = check(&cfg1).expect("check");
    assert!(drift.is_empty(), "no drift expected, got: {drift:?}");

    // Manual notes below the sentinel survive a re-run and do not trip --check.
    let bpmn_page = out1.path().join("acme--mini-package--1.0.0/bpmn/flow.md");
    let content = std::fs::read_to_string(&bpmn_page).unwrap();
    let with_notes = content.replace(
        "_Add hand-curated notes for this page here._",
        "Hand-written wisdom.",
    );
    std::fs::write(&bpmn_page, with_notes).unwrap();
    sutra_docgen::run(&cfg1).expect("re-run");
    let after = std::fs::read_to_string(&bpmn_page).unwrap();
    assert!(
        after.contains("Hand-written wisdom."),
        "manual notes must be preserved"
    );
    let drift = check(&cfg1).expect("check with notes");
    assert!(
        drift.is_empty(),
        "manual notes must not count as drift: {drift:?}"
    );

    // Edits ABOVE the sentinel are drift; so is a missing page.
    std::fs::write(&bpmn_page, after.replace("### Tasks", "### Tampered")).unwrap();
    let drift = check(&cfg1).expect("check tampered");
    assert_eq!(drift, ["differs: acme--mini-package--1.0.0/bpmn/flow.md"]);

    std::fs::remove_file(&bpmn_page).unwrap();
    let drift = check(&cfg1).expect("check missing");
    assert_eq!(drift, ["missing: acme--mini-package--1.0.0/bpmn/flow.md"]);
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            collect_files(root, &p, out);
        } else {
            out.push(
                p.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
}
