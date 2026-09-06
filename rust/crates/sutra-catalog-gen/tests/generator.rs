//! End-to-end tests over the fixture workspace in `tests/fixtures/demo-workspace/`:
//! parser extraction, reference-graph bidirectionality, golden page content, determinism,
//! manual-notes preservation, and `--check` drift detection.

use std::path::{Path, PathBuf};

use sutra_catalog_gen::{check, render, resolve, run, workspace, Config};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/demo-workspace")
}

// ---- parser -----------------------------------------------------------------------------------

#[test]
fn parses_crates_files_and_items() {
    let ws = workspace::discover(&fixture_root()).expect("discover");
    let names: Vec<&str> = ws.crates.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["demo-app", "demo-core"], "sorted crate names");

    let core = &ws.crates[1];
    assert_eq!(core.rel_dir, "rust/crates/demo-core");
    assert_eq!(
        core.description.as_deref(),
        Some("Fixture core crate — types the app crate references")
    );

    let lib = core.files.iter().find(|f| f.rel == "src/lib.rs").unwrap();
    // First paragraph only — the second `//!` paragraph must not leak into the summary.
    assert_eq!(lib.module_doc.as_deref(), Some("Fixture core crate root."));
    let item_names: Vec<&str> = lib.items.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(
        item_names,
        ["store", "WidgetId", "Renderer", "PROTOCOL_VERSION"],
        "kind-ordered item inventory (module, struct, trait, const)"
    );
    let widget = lib.items.iter().find(|i| i.name == "WidgetId").unwrap();
    assert_eq!(widget.vis, "pub");
    assert_eq!(widget.doc.as_deref(), Some("A widget identifier."));
}

#[test]
fn cfg_test_modules_are_skipped() {
    let ws = workspace::discover(&fixture_root()).expect("discover");
    let core = &ws.crates[1];
    let store = core.files.iter().find(|f| f.rel == "src/store.rs").unwrap();
    assert!(
        store
            .items
            .iter()
            .all(|i| i.name != "tests" && i.name != "make"),
        "cfg(test) module and its items must not be catalogued"
    );
    // The `use super::*` inside `mod tests` must not create edges either — store.rs's only
    // use is `crate::WidgetId` (the crate root).
    assert_eq!(store.uses.len(), 1);
    assert_eq!(store.uses[0].path, ["crate", "WidgetId"]);
}

#[test]
fn methods_and_trait_impls_are_harvested() {
    let ws = workspace::discover(&fixture_root()).expect("discover");
    let core = &ws.crates[1];
    let store = core.files.iter().find(|f| f.rel == "src/store.rs").unwrap();
    let methods: Vec<&str> = store.methods.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(
        methods,
        ["default", "is_empty", "len", "new"],
        "sorted methods"
    );
    assert_eq!(store.trait_impls.len(), 1);
    assert_eq!(store.trait_impls[0].trait_name, "Default");
    assert_eq!(store.trait_impls[0].type_name, "Store");
}

// ---- reference graph --------------------------------------------------------------------------

#[test]
fn reference_graph_is_bidirectional_and_path_qualified() {
    let ws = workspace::discover(&fixture_root()).expect("discover");
    let g = resolve::build(&ws);

    let app_lib = "rust/crates/demo-app/src/lib.md";
    let core_lib = "rust/crates/demo-core/src/lib.md";
    let core_store = "rust/crates/demo-core/src/store.md";

    let refs = g.file_refs.get(app_lib).expect("app lib refs");
    assert!(
        refs.contains(core_lib),
        "use demo_core::WidgetId → core lib page"
    );
    assert!(
        refs.contains(core_store),
        "use demo_core::store::Store → store page"
    );

    // Bidirectional: both targets know the referrer.
    assert!(g.file_refby.get(core_lib).unwrap().contains(app_lib));
    assert!(g.file_refby.get(core_store).unwrap().contains(app_lib));

    // Crate-level graph from Cargo.toml path deps, both directions.
    assert!(g.crate_dep.get("demo-app").unwrap().contains("demo-core"));
    assert!(g.crate_depby.get("demo-core").unwrap().contains("demo-app"));

    // Intra-crate: store.rs → crate::WidgetId resolves to the crate root page.
    assert!(g.file_refs.get(core_store).unwrap().contains(core_lib));
}

// ---- golden page ------------------------------------------------------------------------------

#[test]
fn golden_page_store() {
    let ws = workspace::discover(&fixture_root()).expect("discover");
    let g = resolve::build(&ws);
    let pages = render::generate_pages(&ws, &g);
    let store = pages
        .iter()
        .find(|p| p.path == "rust/crates/demo-core/src/store.md")
        .expect("store page");
    let golden = include_str!("golden/demo-core-store.md");
    assert_eq!(
        store.content, golden,
        "store.md must match the golden page byte-for-byte"
    );
}

// ---- end-to-end: run, determinism, manual notes, --check ---------------------------------------

#[test]
fn run_is_deterministic_and_check_detects_drift() {
    let out1 = tempfile::tempdir().unwrap();
    let out2 = tempfile::tempdir().unwrap();
    let cfg1 = Config::with_defaults(fixture_root(), Some(out1.path().to_path_buf()));
    let cfg2 = Config::with_defaults(fixture_root(), Some(out2.path().to_path_buf()));

    let r1 = run(&cfg1, false).expect("run 1");
    let r2 = run(&cfg2, false).expect("run 2");
    assert_eq!(r1.pages, r2.pages);
    assert!(
        r1.pages >= 5,
        "workspace page + 2 crate pages + 3 file pages"
    );

    // Byte-stable across runs.
    let mut paths = Vec::new();
    collect_files(out1.path(), out1.path(), &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    for rel in &paths {
        let a = std::fs::read_to_string(out1.path().join(rel)).unwrap();
        let b = std::fs::read_to_string(out2.path().join(rel)).unwrap();
        assert_eq!(a, b, "page {rel} differs between two runs");
    }

    // Freshly generated tree is in sync.
    let drift = check(&cfg1).expect("check");
    assert!(drift.is_empty(), "no drift expected, got: {drift:?}");

    // Manual notes below the sentinel survive a re-run and do not trip --check.
    let store_page = out1.path().join("rust/crates/demo-core/src/store.md");
    let content = std::fs::read_to_string(&store_page).unwrap();
    let with_notes = content.replace(
        "_Add hand-curated notes for this page here._",
        "Hand-written wisdom.",
    );
    std::fs::write(&store_page, with_notes).unwrap();
    run(&cfg1, false).expect("re-run");
    let after = std::fs::read_to_string(&store_page).unwrap();
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
    std::fs::write(&store_page, after.replace("## Items", "## Tampered")).unwrap();
    let drift = check(&cfg1).expect("check tampered");
    assert_eq!(drift, ["differs: rust/crates/demo-core/src/store.md"]);

    std::fs::remove_file(&store_page).unwrap();
    let drift = check(&cfg1).expect("check missing");
    assert_eq!(drift, ["missing: rust/crates/demo-core/src/store.md"]);
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

/// `--clean` removes a page whose SOURCE is gone, and nothing else.
///
/// The predicate that matters is "does a sibling file share this stem", not "did this run produce
/// this page". Two earlier cuts got that wrong and deleted real content: keying on the produced
/// set condemned 106 pages (directory `index.md` stubs the generator never emits, plus pages for
/// crates a run did not discover), and assuming a `.rs` extension condemned 40 more (asset pages
/// for `.bpmn`, `.xsd`, `.hbs` sources). Both are pinned below.
#[test]
fn clean_removes_only_pages_whose_source_is_gone() {
    let out = tempfile::tempdir().expect("tempdir");
    let cfg = Config::with_defaults(fixture_root(), Some(out.path().to_path_buf()));
    run(&cfg, false).expect("initial run");

    let rust_dir = out.path().join("rust");
    // (a) a page with no source at all — the only thing `--clean` should remove.
    let stranded = rust_dir.join("crates/zzz-gone/src/vanished.md");
    std::fs::create_dir_all(stranded.parent().unwrap()).unwrap();
    std::fs::write(&stranded, "# stranded").unwrap();
    // (b) a directory stub — never has a single source, must survive.
    let stub = rust_dir.join("index.md");
    std::fs::write(&stub, "# directory stub").unwrap();

    let report = run(&cfg, true).expect("clean run");

    assert!(!stranded.exists(), "a page with no source must be removed");
    assert!(
        stub.exists(),
        "a directory stub has no source and must survive"
    );
    assert_eq!(
        report.removed,
        vec!["rust/crates/zzz-gone/src/vanished.md".to_string()],
        "exactly one page removed, and it is the stranded one"
    );
}

/// Without `--clean`, a stranded page is REPORTED by `check` but left on disk. The gate stays
/// honest; the deletion stays explicit.
#[test]
fn check_reports_a_stranded_page_that_a_plain_run_leaves_alone() {
    let out = tempfile::tempdir().expect("tempdir");
    let cfg = Config::with_defaults(fixture_root(), Some(out.path().to_path_buf()));
    run(&cfg, false).expect("initial run");

    let stranded = out.path().join("rust/crates/zzz-gone/src/vanished.md");
    std::fs::create_dir_all(stranded.parent().unwrap()).unwrap();
    std::fs::write(&stranded, "# stranded").unwrap();

    let drift = sutra_catalog_gen::check(&cfg).expect("check");
    assert!(
        drift
            .iter()
            .any(|d| d == "orphaned: rust/crates/zzz-gone/src/vanished.md"),
        "check must report it; drift: {drift:?}"
    );

    run(&cfg, false).expect("plain re-run");
    assert!(stranded.exists(), "a plain run must not delete anything");
}
