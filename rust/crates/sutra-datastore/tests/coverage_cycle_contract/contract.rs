//! The compliance-cycle contract, one body for every dialect's suite: the statements and the
//! driver differ per dialect, the promised behaviour does not.

use sutra_datastore::{shipped_ddl, CoverageCycleActivation, CoverageFragmentRow, CoverageStore};

const DEP: &str = "dep-cycle-1";

fn urns() -> Vec<String> {
    ["a", "b", "c"]
        .iter()
        .map(|p| format!("urn:sutra:coverage:file:{p}"))
        .collect()
}

fn fragment(instance: &str) -> CoverageFragmentRow {
    CoverageFragmentRow {
        route_urn: "urn:sutra:coverage:file:a".to_string(),
        segment_process: "p1".to_string(),
        instance_id: instance.to_string(),
        business_key: Some("k".to_string()),
        trace_id: None,
    }
}

async fn counts(store: &CoverageStore, cycle: Option<&str>) -> (u64, u64) {
    let m = store.in_cycle(cycle).read_metrics(DEP).await.unwrap();
    (m.total, m.covered)
}

/// Declaring, activating, the single-active rule, row isolation, deactivating, and continuing an
/// earlier cycle. Boxed step by step: an unoptimised build lays every await of one long async body
/// side by side, and across this many the future outgrows a test thread's stack.
pub async fn run(store: &CoverageStore) {
    Box::pin(declaring_and_the_single_active_rule(store)).await;
    Box::pin(rows_stay_apart(store)).await;
    Box::pin(deactivating_and_continuing(store)).await;
    Box::pin(racing_activations(store)).await;
    Box::pin(scoped_clear_and_reset(store)).await;
    Box::pin(unusable_labels(store)).await;
}

async fn declaring_and_the_single_active_rule(store: &CoverageStore) {
    let urns = urns();
    store.seed_declared(DEP, &urns).await.unwrap();

    // Nothing active, and an undeclared label is not activatable.
    assert_eq!(store.active_cycle(DEP).await.unwrap(), None);
    assert_eq!(
        store.activate_cycle(DEP, "rel-1").await.unwrap(),
        CoverageCycleActivation::Undeclared
    );

    // A repeat declaration is harmless, and a seed covers every path in the cycle.
    assert!(store.declare_cycle(DEP, "rel-1").await.unwrap());
    assert!(!store.declare_cycle(DEP, "rel-1").await.unwrap());
    assert!(store.declare_cycle(DEP, "Rel 2 / QA").await.unwrap());
    assert_eq!(
        store.list_cycles(DEP).await.unwrap().len(),
        2,
        "both declared"
    );
    let rel1 = store.in_cycle(Some("rel-1"));
    assert_eq!(rel1.seed_declared(DEP, &urns).await.unwrap(), 3);
    assert_eq!(rel1.seed_declared(DEP, &urns).await.unwrap(), 0);

    // Single active, per deployment.
    assert_eq!(
        store.activate_cycle(DEP, "rel-1").await.unwrap(),
        CoverageCycleActivation::Activated
    );
    assert_eq!(
        store.activate_cycle(DEP, "rel-1").await.unwrap(),
        CoverageCycleActivation::AlreadyActive
    );
    assert_eq!(
        store.activate_cycle(DEP, "Rel 2 / QA").await.unwrap(),
        CoverageCycleActivation::Refused {
            active_cycle: "rel-1".to_string()
        }
    );
    assert_eq!(
        store.active_cycle(DEP).await.unwrap().as_deref(),
        Some("rel-1"),
        "a refusal leaves the holder active"
    );
    // Another deployment has its own slot.
    assert!(store.declare_cycle("dep-cycle-2", "rel-1").await.unwrap());
    assert_eq!(
        store.activate_cycle("dep-cycle-2", "rel-1").await.unwrap(),
        CoverageCycleActivation::Activated
    );
}

async fn rows_stay_apart(store: &CoverageStore) {
    let urns = urns();
    let rel1 = store.in_cycle(Some("rel-1"));

    // Marks and fragments stay in their own rows, in both directions.
    assert!(rel1.mark_path_covered(DEP, &urns[0]).await.unwrap());
    assert!(!rel1.mark_path_covered(DEP, &urns[0]).await.unwrap());
    assert_eq!(counts(store, Some("rel-1")).await, (3, 1));
    assert_eq!(counts(store, None).await, (3, 0));
    assert_eq!(counts(store, Some("Rel 2 / QA")).await.1, 0);
    assert!(store.mark_path_covered(DEP, &urns[1]).await.unwrap());
    assert_eq!(counts(store, Some("rel-1")).await, (3, 1));
    let covered = rel1.covered_among(DEP, &urns).await.unwrap();
    assert_eq!(covered.len(), 1);
    assert!(covered.contains(&urns[0]));
    assert_eq!(rel1.count_metrics(DEP).await.unwrap().covered, 1);
    assert_eq!(
        rel1.read_metrics(DEP).await.unwrap().uncovered,
        vec![urns[1].clone(), urns[2].clone()]
    );
    // A path never seeded in the cycle still lands in it.
    assert!(rel1.mark_path_covered(DEP, "unseeded").await.unwrap());
    assert_eq!(counts(store, Some("rel-1")).await, (4, 2));

    rel1.write_fragment(DEP, &fragment("in-cycle"))
        .await
        .unwrap();
    store
        .write_fragment(DEP, &fragment("no-cycle"))
        .await
        .unwrap();
    let in_cycle = rel1.read_fragments(DEP).await.unwrap();
    assert_eq!(in_cycle.len(), 1);
    assert_eq!(in_cycle[0].instance_id, "in-cycle");
    assert_eq!(in_cycle[0].business_key.as_deref(), Some("k"));
    let plain = store.read_fragments(DEP).await.unwrap();
    assert_eq!(plain.len(), 1);
    assert_eq!(plain[0].instance_id, "no-cycle");
}

async fn deactivating_and_continuing(store: &CoverageStore) {
    // Deactivating takes only the holder.
    assert!(!store.deactivate_cycle(DEP, "Rel 2 / QA").await.unwrap());
    assert!(store.active_cycle(DEP).await.unwrap().is_some());
    assert!(store.deactivate_cycle(DEP, "rel-1").await.unwrap());
    assert_eq!(store.active_cycle(DEP).await.unwrap(), None);
    assert!(!store.deactivate_cycle(DEP, "rel-1").await.unwrap());

    // A second cycle can start, and the first continues afterwards with its marks intact.
    assert_eq!(
        store.activate_cycle(DEP, "Rel 2 / QA").await.unwrap(),
        CoverageCycleActivation::Activated
    );
    assert!(store.deactivate_cycle(DEP, "Rel 2 / QA").await.unwrap());
    assert_eq!(
        store.activate_cycle(DEP, "rel-1").await.unwrap(),
        CoverageCycleActivation::Activated
    );
    assert_eq!(counts(store, Some("rel-1")).await.1, 2);
    assert!(store.deactivate_cycle(DEP, "rel-1").await.unwrap());
}

async fn racing_activations(store: &CoverageStore) {
    // Two activations racing: the key lets exactly one through.
    let (x, y) = tokio::join!(
        store.activate_cycle(DEP, "rel-1"),
        store.activate_cycle(DEP, "Rel 2 / QA"),
    );
    let outcomes = [x.unwrap(), y.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| **o == CoverageCycleActivation::Activated)
            .count(),
        1,
        "{outcomes:?}"
    );
    let holder = store.active_cycle(DEP).await.unwrap().unwrap();
    assert!(store.deactivate_cycle(DEP, &holder).await.unwrap());
}

async fn scoped_clear_and_reset(store: &CoverageStore) {
    let urns = urns();
    let rel1 = store.in_cycle(Some("rel-1"));

    // A scoped clear and reset touch only their own rows.
    assert_eq!(rel1.clear_paths(DEP, &urns[..1]).await.unwrap(), 1);
    assert_eq!(counts(store, Some("rel-1")).await.1, 1);
    rel1.reset(DEP).await.unwrap();
    assert_eq!(counts(store, Some("rel-1")).await, (4, 0));
    assert!(rel1.read_fragments(DEP).await.unwrap().is_empty());
    assert_eq!(counts(store, None).await, (3, 1), "plain marks survive");
    assert_eq!(store.read_fragments(DEP).await.unwrap().len(), 1);
}

async fn unusable_labels(store: &CoverageStore) {
    // An unusable label is a typed refusal, not a driver surprise.
    assert!(store.declare_cycle(DEP, "  ").await.is_err());
    assert!(store
        .declare_cycle(DEP, &"x".repeat(sutra_datastore::CYCLE_LABEL_MAX_CHARS + 1))
        .await
        .is_err());
}

/// The coverage schema as a database that predates cycles has it, with rows in it.
pub fn pre_cycle_ddl(dialect: sutra_datastore::SqlDialect) -> String {
    let (covered, uncovered) = match dialect {
        sutra_datastore::SqlDialect::Postgres => ("true", "false"),
        _ => ("1", "0"),
    };
    let ddl = shipped_ddl(dialect);
    let scripts: Vec<&String> = ddl
        .iter()
        .filter(|s| !s.contains("coverage_cycle"))
        .collect();
    assert_eq!(scripts.len(), 2, "the two pre-cycle scripts");
    format!(
        "{}\n{}\nINSERT INTO coverage_metric (deployment_id, path_urn, covered) VALUES \
         ('{DEP}', 'old-covered', {covered}), ('{DEP}', 'old-uncovered', {uncovered});\n\
         INSERT INTO coverage_fragment (deployment_id, route_urn, segment_process, instance_id) \
         VALUES ('{DEP}', 'old-route', 'p1', 'old-instance');\n",
        scripts[0], scripts[1]
    )
}

/// `store` opens a database [`pre_cycle_ddl`] built: its rows read as "no cycle", untouched, and
/// cycles work beside them.
pub async fn assert_upgraded(store: &CoverageStore) {
    let plain = store.read_metrics(DEP).await.unwrap();
    assert_eq!((plain.total, plain.covered), (2, 1));
    assert_eq!(plain.uncovered, vec!["old-uncovered".to_string()]);
    assert_eq!(
        store.read_fragments(DEP).await.unwrap()[0].instance_id,
        "old-instance"
    );

    assert_eq!(store.active_cycle(DEP).await.unwrap(), None);
    assert!(store.declare_cycle(DEP, "after-upgrade").await.unwrap());
    assert_eq!(
        store.activate_cycle(DEP, "after-upgrade").await.unwrap(),
        CoverageCycleActivation::Activated
    );
    let cycle = store.in_cycle(Some("after-upgrade"));
    assert!(cycle.mark_path_covered(DEP, "old-uncovered").await.unwrap());
    assert_eq!(counts(store, None).await, (2, 1), "old rows are not moved");
    assert_eq!(counts(store, Some("after-upgrade")).await, (1, 1));

    // The rows are still written the old way once no cycle is active.
    assert!(store.deactivate_cycle(DEP, "after-upgrade").await.unwrap());
    assert!(store.mark_path_covered(DEP, "old-uncovered").await.unwrap());
    assert_eq!(counts(store, None).await, (2, 2));
}
