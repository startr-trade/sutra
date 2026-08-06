//! Path-coverage store — the typed metric-flag table coverage is recorded in.
//!
//! Async + `Result`-returning — matching the [`crate::datastore`] SPI. Runtime coverage
//! marking is a best-effort metric side-effect: the executor logs/skips on `Err` rather than
//! failing the instance, but the typed surface lets a durable provider surface failures. The
//! reserved `coverage:report` / `coverage:reset` ops, by contrast, fail LOUDLY — a report that
//! silently read 0% would look like a real measurement.
//!
//! ## One store (RULED 2026-08-04, `datastore-schema-projection.md` §7)
//!
//! [`CoverageMetricStore`] is the single coverage surface: a per-deployment *metric-flag* table
//! (every declared path URN seeded `covered=false`, flipped `true` on exercise) plus a
//! *reconstruction-fragment* log (cross-process hop evidence). Metrics (`total` / `covered` /
//! `coverage_percentage` + the uncovered set) derive straight off the flags, computed by SQL
//! aggregates rather than client-side folds. Decoupled from audit by design — this SPI never
//! reads or writes audit tables.
//!
//! The **retired** surface: a module KV data store named `coverage`, keyed
//! `"<process>/<pathId>"` with `{coveredBy}` values, dual-written by the runtime marking and read
//! back one `get` per declared path by `coverage:report`. It is no longer written or read;
//! existing rows are deliberately left in place so a rollback to a prior engine build still finds
//! its data.
//!
//! ## Where the rows live (SUPERSEDING RULING, same day)
//!
//! In the **user-declared `coverage` data store**, not the engine's database: the author names
//! that store in `datastores.yaml` and its data source picks the database (and therefore the
//! dialect), while the engine owns the coverage SCHEMA and applies it there on first use — the
//! author writes no coverage SQL. Consequence, stated rather than hidden: **a deployment that
//! declares no `coverage` store has no coverage at all** (which is why `sutra lint` errors on
//! `<q:coverage>` paths without one), and a declared store that cannot be opened fails the ops
//! loudly instead of reporting 0%.
//!
//! The durable implementation lives in `sutra-datastore` (`CoverageStore`, one per dialect, over
//! the declared store's own connection); [`InMemoryCoverageStore`] below implements the same SPI
//! for unit tests.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::datastore::StoreError;

/// One cross-process reconstruction fragment: the completion of an injected
/// coverage segment plus the correlation dimensions the union-find unions on. Written
/// per injected-segment completion by the runtime marking; read back by the
/// correlation-aware `coverage check`.
///
/// Domain-neutral: `route_urn` / `segment_process` are structural refs, `business_key` is an
/// author-declared correlation value (a FEEL-computed string), `trace_id` is the W3C traceparent —
/// no message-standard or per-domain identifier knowledge enters the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageFragment {
    /// Fully-qualified coverage-route URN (`urn:sutra:coverage:<file>:<path>`).
    pub route_urn: String,
    /// The process whose injected segment completed (a deployment-declared `processId`).
    pub segment_process: String,
    /// The completing instance's id (ties an instance's several passes/legs together).
    pub instance_id: String,
    /// Per-hop business key observed at this segment (a correlation value; `None` when absent).
    pub business_key: Option<String>,
    /// W3C trace-id observed at this segment (`None` when absent).
    pub trace_id: Option<String>,
}

/// The reserved URN prefix a coverage-route / injected-sub-path id carries
/// (`urn:sutra:coverage:<file>:<route>[#<process>]`). An intra-process `<q:coverage>`
/// path id is an author mnemonic that never starts with this, so it is the discriminator
/// between the two marking surfaces (intra flag vs cross-process fragment).
pub const COVERAGE_URN_PREFIX: &str = "urn:sutra:coverage:";

/// The correlation dimensions the runtime marking stamps onto a cross-process
/// reconstruction [`CoverageFragment`]. Both are best-effort / `None`-tolerant
/// — the union-find reconstructs the cascade even when one edge is missing:
///
/// - `trace_id` — the W3C traceparent of the inbound message that drove THIS pass (`None` for a
///   timer / purely-internal completion with no inbound trace).
/// - `business_key` — the `<q:alias>` correlation value this instance's leg was correlated on
///   (the spawn/relay alias value); `None` when not cleanly reachable at the drive site.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoverageCorrelation {
    pub trace_id: Option<String>,
    pub business_key: Option<String>,
}

/// Split a coverage `path_id` into `(route_urn, segment_process)` IFF it is a cross-process
/// desugar-injected sub-path — id `urn:sutra:coverage:<file>:<route>#<process>`. The
/// route URN is everything before the LAST `#`, the segment process everything after. Returns
/// `None` for an intra-process `<q:coverage>` path (an author mnemonic that carries neither the
/// reserved prefix nor a `#`), which the marking treats as a metric flag rather than a fragment.
pub fn split_injected_sub_path(path_id: &str) -> Option<(String, String)> {
    if !path_id.starts_with(COVERAGE_URN_PREFIX) {
        return None;
    }
    let (route_urn, segment_process) = path_id.rsplit_once('#')?;
    if route_urn.is_empty() || segment_process.is_empty() {
        return None;
    }
    Some((route_urn.to_string(), segment_process.to_string()))
}

/// The metric-flag URN one declared coverage-path id maps onto: a desugar-injected
/// cross-process sub-path (`…:<route>#<process>`) collapses to its ROUTE urn (the flag
/// `sutra coverage check`'s union-find flips once the whole cascade is complete); an
/// intra-process author mnemonic passes through verbatim. The per-id form of [`seed_urns`], so
/// seeding, marking and reporting all agree on the same key space.
pub fn metric_flag_urn(path_id: &str) -> String {
    match split_injected_sub_path(path_id) {
        Some((route_urn, _)) => route_urn,
        None => path_id.to_string(),
    }
}

/// Derive the seed-at-deploy metric-flag set (the "total to cover") from a deployment's
/// declared coverage-path ids: **intra-process path ids ∪ cross-process ROUTE urns** — NEVER the
/// `#<process>` injected sub-paths (those are per-process marking cursors, not metric
/// flags). An injected sub-path collapses to its route URN (many processes → one route flag);
/// an intra id passes through verbatim. Deterministic + deduplicated (BTreeSet).
pub fn seed_urns(path_ids: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    for id in path_ids {
        set.insert(metric_flag_urn(&id));
    }
    set.into_iter().collect()
}

/// Derived coverage metrics for a deployment — read straight off the seeded metric flags.
/// The fail-closed CI gate and the runtime SLO signal are the same query over this shape.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoverageMetrics {
    /// Total declared paths (the "total to cover" — every seeded URN).
    pub total: u64,
    /// How many are `covered = true`.
    pub covered: u64,
    /// The still-uncovered path URNs (`covered = false`), ascending for determinism.
    pub uncovered: Vec<String>,
}

impl CoverageMetrics {
    /// Coverage percentage (two-decimal, `0.0` for an empty declaration) — matches the existing
    /// `coverage:report` percentage math so the two coverage surfaces read consistently.
    pub fn coverage_percentage(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.covered as f64 * 10000.0 / self.total as f64).round() / 100.0
        }
    }
}

/// The **typed** coverage-metric + reconstruction-fragment store — a per-deployment
/// covered-flag table plus a cross-process fragment log, decoupled from audit. The durable
/// provider (`sutra_persistence::stores::PgCoverageStore`) and [`InMemoryCoverageStore`] implement
/// it; the runtime marking, the reserved `coverage:report` / `coverage:reset` ops, the
/// `sutra.coverage.percent` gauge and `sutra coverage check` ALL read and write it — since the
/// module KV covered-set was retired it is the only place coverage exists.
///
/// Counting methods are deliberately shaped so a durable provider can answer them with a single
/// aggregate/statement: [`Self::covered_among`] for a caller-supplied set, [`Self::clear_paths`]
/// for a scoped reset, [`Self::read_metrics`] for the whole deployment — never a
/// fetch-everything-and-fold-in-Rust round trip.
#[async_trait(?Send)]
pub trait CoverageMetricStore {
    /// Idempotently insert every declared `path_urn` with `covered = false` (the "total to
    /// cover"). Re-callable at deploy / `coverage init` / reset — an already-present flag (including one
    /// already flipped `true`) is left untouched (`INSERT … ON CONFLICT DO NOTHING`).
    async fn seed_declared(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<(), StoreError>;

    /// Flip a declared path to `covered = true` (idempotent; upserts if not yet seeded).
    /// Returns whether THIS call newly flipped it — `false` when the flag was already `true`.
    /// That is the durable "first covers wins" signal the `coverage.path_covered` event and the
    /// `sutra.coverage.percent` gauge fire on (it replaces the retired KV `get`-before-`put`).
    async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, StoreError>;

    /// The subset of `path_urns` currently flagged `covered = true`.
    ///
    /// ONE round trip for a caller-supplied set — the substrate of `coverage:report`, which used
    /// to issue one KV `get` per declared path and fold the answers in Rust.
    async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, StoreError>;

    /// Clear the covered flag on `path_urns` (leaving the rows seeded, `covered = false`), and
    /// return how many were actually flipped `true → false` — the `coverage:reset` `cleared`
    /// count, in ONE statement. Scoped to the given paths, so a per-process reset does not
    /// disturb the rest of the deployment's flags; [`Self::reset`] is the deployment-wide form.
    async fn clear_paths(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, StoreError>;

    /// Append a cross-process reconstruction fragment.
    async fn write_fragment(
        &self,
        deployment_id: &str,
        fragment: &CoverageFragment,
    ) -> Result<(), StoreError>;

    /// Derived metrics off the flags (`total` / `covered` / `coverage_percentage` + uncovered set).
    async fn read_metrics(&self, deployment_id: &str) -> Result<CoverageMetrics, StoreError>;

    /// Re-seed every declared path of the deployment to `covered = false` and clear its
    /// reconstruction fragments — the `coverage:reset` substrate. Clearing the fragments
    /// alongside the flags prevents stale cross-process evidence from immediately re-flipping a
    /// route on the next `coverage check`.
    async fn reset(&self, deployment_id: &str) -> Result<(), StoreError>;

    /// All reconstruction fragments for the deployment (insertion order) — the union-find
    /// input.
    async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragment>, StoreError>;
}

/// In-memory [`CoverageMetricStore`] for tests.
#[derive(Debug, Default)]
pub struct InMemoryCoverageStore {
    /// Typed metric flags, keyed `(deployment_id, path_urn) → covered`.
    metrics: RefCell<BTreeMap<(String, String), bool>>,
    /// Reconstruction fragments in insertion order, tagged by deployment.
    fragments: RefCell<Vec<(String, CoverageFragment)>>,
}

impl InMemoryCoverageStore {
    pub fn new() -> InMemoryCoverageStore {
        InMemoryCoverageStore::default()
    }

    /// Whether a metric flag exists AND is `covered = true` (test assertion helper).
    pub fn is_covered(&self, deployment_id: &str, path_urn: &str) -> bool {
        self.metrics
            .borrow()
            .get(&(deployment_id.to_string(), path_urn.to_string()))
            .copied()
            .unwrap_or(false)
    }
}

#[async_trait(?Send)]
impl CoverageMetricStore for InMemoryCoverageStore {
    async fn seed_declared(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<(), StoreError> {
        let mut metrics = self.metrics.borrow_mut();
        for urn in path_urns {
            // ON CONFLICT DO NOTHING — never clobber an already-seeded (or already-covered) flag.
            metrics
                .entry((deployment_id.to_string(), urn.clone()))
                .or_insert(false);
        }
        Ok(())
    }

    async fn mark_path_covered(
        &self,
        deployment_id: &str,
        path_urn: &str,
    ) -> Result<bool, StoreError> {
        let was = self
            .metrics
            .borrow_mut()
            .insert((deployment_id.to_string(), path_urn.to_string()), true);
        // Newly covered iff there was no row, or the row was still false.
        Ok(!was.unwrap_or(false))
    }

    async fn covered_among(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, StoreError> {
        let metrics = self.metrics.borrow();
        Ok(path_urns
            .iter()
            .filter(|urn| {
                metrics
                    .get(&(deployment_id.to_string(), (*urn).clone()))
                    .copied()
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn clear_paths(
        &self,
        deployment_id: &str,
        path_urns: &[String],
    ) -> Result<u64, StoreError> {
        let wanted: BTreeSet<&String> = path_urns.iter().collect();
        let mut cleared = 0u64;
        for ((dep, urn), is_covered) in self.metrics.borrow_mut().iter_mut() {
            if dep == deployment_id && *is_covered && wanted.contains(urn) {
                *is_covered = false;
                cleared += 1;
            }
        }
        Ok(cleared)
    }

    async fn write_fragment(
        &self,
        deployment_id: &str,
        fragment: &CoverageFragment,
    ) -> Result<(), StoreError> {
        self.fragments
            .borrow_mut()
            .push((deployment_id.to_string(), fragment.clone()));
        Ok(())
    }

    async fn read_metrics(&self, deployment_id: &str) -> Result<CoverageMetrics, StoreError> {
        let metrics = self.metrics.borrow();
        let mut total = 0u64;
        let mut covered = 0u64;
        let mut uncovered = Vec::new();
        for ((dep, urn), is_covered) in metrics.iter() {
            if dep != deployment_id {
                continue;
            }
            total += 1;
            if *is_covered {
                covered += 1;
            } else {
                uncovered.push(urn.clone());
            }
        }
        // BTreeMap iteration is already key-ascending, so `uncovered` is deterministic.
        Ok(CoverageMetrics {
            total,
            covered,
            uncovered,
        })
    }

    async fn reset(&self, deployment_id: &str) -> Result<(), StoreError> {
        for ((dep, _), is_covered) in self.metrics.borrow_mut().iter_mut() {
            if dep == deployment_id {
                *is_covered = false;
            }
        }
        self.fragments
            .borrow_mut()
            .retain(|(dep, _)| dep != deployment_id);
        Ok(())
    }

    async fn read_fragments(
        &self,
        deployment_id: &str,
    ) -> Result<Vec<CoverageFragment>, StoreError> {
        Ok(self
            .fragments
            .borrow()
            .iter()
            .filter(|(dep, _)| dep == deployment_id)
            .map(|(_, f)| f.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEP: &str = "dep-0123456789abcdef01234567";

    fn urns(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn seed_then_mark_then_metrics_percentage() {
        let store = InMemoryCoverageStore::new();
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b", "urn:c", "urn:d"]))
            .await
            .unwrap();

        // Freshly seeded: all uncovered, 0%.
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!(m.total, 4);
        assert_eq!(m.covered, 0);
        assert_eq!(m.uncovered, urns(&["urn:a", "urn:b", "urn:c", "urn:d"]));
        assert_eq!(m.coverage_percentage(), 0.0);

        // Mark two → 50%.
        store.mark_path_covered(DEP, "urn:a").await.unwrap();
        store.mark_path_covered(DEP, "urn:c").await.unwrap();
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!((m.total, m.covered), (4, 2));
        assert_eq!(m.uncovered, urns(&["urn:b", "urn:d"]));
        assert_eq!(m.coverage_percentage(), 50.0);

        // Two-decimal rounding: 1/3 declared covered → 33.33%.
        let store3 = InMemoryCoverageStore::new();
        store3
            .seed_declared(DEP, &urns(&["x", "y", "z"]))
            .await
            .unwrap();
        store3.mark_path_covered(DEP, "x").await.unwrap();
        assert_eq!(
            store3
                .read_metrics(DEP)
                .await
                .unwrap()
                .coverage_percentage(),
            33.33
        );
    }

    #[tokio::test]
    async fn empty_declaration_is_zero_percent() {
        let store = InMemoryCoverageStore::new();
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!(m.total, 0);
        assert_eq!(m.coverage_percentage(), 0.0);
    }

    #[tokio::test]
    async fn seed_is_idempotent_and_never_clobbers_a_covered_flag() {
        let store = InMemoryCoverageStore::new();
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b"]))
            .await
            .unwrap();
        store.mark_path_covered(DEP, "urn:a").await.unwrap();

        // Re-seed (redeploy / coverage init) must NOT reset urn:a back to false, and must not
        // duplicate the total.
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b", "urn:c"]))
            .await
            .unwrap();
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!((m.total, m.covered), (3, 1));
        assert_eq!(m.uncovered, urns(&["urn:b", "urn:c"]));
    }

    #[tokio::test]
    async fn mark_reports_newly_covered_once() {
        // The durable first-covers-wins signal that replaced the KV `get`-before-`put`: the
        // FIRST flip returns true, every re-mark false.
        let store = InMemoryCoverageStore::new();
        store.seed_declared(DEP, &urns(&["urn:a"])).await.unwrap();
        assert!(store.mark_path_covered(DEP, "urn:a").await.unwrap());
        assert!(!store.mark_path_covered(DEP, "urn:a").await.unwrap());
        // An unseeded path upserts, and that upsert is itself a new cover.
        assert!(store.mark_path_covered(DEP, "urn:new").await.unwrap());
        assert!(!store.mark_path_covered(DEP, "urn:new").await.unwrap());
    }

    #[tokio::test]
    async fn covered_among_answers_a_caller_supplied_set_in_one_call() {
        let store = InMemoryCoverageStore::new();
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b", "urn:c"]))
            .await
            .unwrap();
        store.mark_path_covered(DEP, "urn:a").await.unwrap();
        store.mark_path_covered(DEP, "urn:c").await.unwrap();

        let got = store
            .covered_among(DEP, &urns(&["urn:a", "urn:b", "urn:absent"]))
            .await
            .unwrap();
        // Only the asked-for covered ones; an absent (never-seeded) urn reads UNCOVERED, never
        // covered — the fail-closed direction the report needs.
        assert_eq!(got, BTreeSet::from(["urn:a".to_string()]));

        // Another deployment's flags never leak in.
        assert!(store
            .covered_among("dep-ffffffffffffffffffffffff", &urns(&["urn:a"]))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn clear_paths_counts_only_the_previously_covered_in_scope() {
        let store = InMemoryCoverageStore::new();
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b", "urn:c"]))
            .await
            .unwrap();
        store.mark_path_covered(DEP, "urn:a").await.unwrap();
        store.mark_path_covered(DEP, "urn:b").await.unwrap();
        store.mark_path_covered(DEP, "urn:c").await.unwrap();

        // Scoped clear: only a and b, and `cleared` counts the true → false flips.
        let cleared = store
            .clear_paths(DEP, &urns(&["urn:a", "urn:b"]))
            .await
            .unwrap();
        assert_eq!(cleared, 2);
        // Re-clearing the same set clears nothing (already false).
        let again = store
            .clear_paths(DEP, &urns(&["urn:a", "urn:b"]))
            .await
            .unwrap();
        assert_eq!(again, 0);
        // Out-of-scope flags are untouched, and the total never shrinks (rows stay seeded).
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!((m.total, m.covered), (3, 1));
        assert_eq!(m.uncovered, urns(&["urn:a", "urn:b"]));
    }

    #[tokio::test]
    async fn write_and_read_fragments() {
        let store = InMemoryCoverageStore::new();
        let f1 = CoverageFragment {
            route_urn: "urn:sutra:coverage:demoflow:e2e:reply1".to_string(),
            segment_process: "p1".to_string(),
            instance_id: "i1".to_string(),
            business_key: Some("txn-1".to_string()),
            trace_id: Some("tA".to_string()),
        };
        let f2 = CoverageFragment {
            route_urn: "urn:sutra:coverage:demoflow:e2e:reply1".to_string(),
            segment_process: "p2".to_string(),
            instance_id: "i2".to_string(),
            business_key: Some("txn-1".to_string()),
            trace_id: None,
        };
        store.write_fragment(DEP, &f1).await.unwrap();
        store.write_fragment(DEP, &f2).await.unwrap();

        // Fragments are scoped by deployment.
        store
            .write_fragment("dep-ffffffffffffffffffffffff", &f1)
            .await
            .unwrap();

        let got = store.read_fragments(DEP).await.unwrap();
        assert_eq!(got, vec![f1, f2]);
    }

    /// Integration-style: mirror the seed-at-deploy activation walk. The engine
    /// assembly, on activation, walks each process's `coverage_paths` (which ALREADY includes the
    /// load-time desugar-injected cross-process sub-paths, fq ids `…:reply1#p1|#p2|#p3`) and seeds
    /// every URN `covered=false` — the "total to cover". This test reproduces that walk over real
    /// `ProcessDefinition`s and asserts the seeded metric surface, then a reset.
    #[tokio::test]
    async fn seed_at_deploy_walks_declared_coverage_paths() {
        use std::collections::HashMap;

        use sutra_bpmn::model::{CoveragePath, ProcessDefinition};

        let mk = |id: &str, path_ids: &[&str]| {
            ProcessDefinition::of(
                id,
                None,
                true,
                "1.0",
                vec![],
                vec![],
                HashMap::new(),
                vec![],
            )
            .unwrap()
            .with_coverage_paths(
                path_ids
                    .iter()
                    .map(|p| CoveragePath {
                        id: p.to_string(),
                        flows: vec![],
                    })
                    .collect(),
            )
        };
        // The worked example: one intra-process path on p1 plus the injected cross-process
        // sub-paths of route `reply1` on p1/p2/p3.
        let processes = [
            mk(
                "p1",
                &[
                    "urn:sutra:coverage:demoflow:intra:happy#p1",
                    "urn:sutra:coverage:demoflow:e2e:reply1#p1",
                ],
            ),
            mk("p2", &["urn:sutra:coverage:demoflow:e2e:reply1#p2"]),
            mk("p3", &["urn:sutra:coverage:demoflow:e2e:reply1#p3"]),
        ];

        // The assembly's trivial walk: collect every declared path id across the active plans.
        let declared: Vec<String> = processes
            .iter()
            .flat_map(|p| p.coverage_paths.iter().map(|c| c.id.clone()))
            .collect();

        let store = InMemoryCoverageStore::new();
        store.seed_declared(DEP, &declared).await.unwrap();

        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!(
            m.total, 4,
            "total to cover = all declared + injected sub-paths"
        );
        assert_eq!(m.covered, 0);
        assert_eq!(m.coverage_percentage(), 0.0);

        // Exercising one cross-process sub-path flips just that flag.
        store
            .mark_path_covered(DEP, "urn:sutra:coverage:demoflow:e2e:reply1#p2")
            .await
            .unwrap();
        assert_eq!(store.read_metrics(DEP).await.unwrap().covered, 1);

        // Reset returns to the fully-seeded "total to cover".
        store.reset(DEP).await.unwrap();
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!((m.total, m.covered), (4, 0));
    }

    #[tokio::test]
    async fn reset_reseeds_false_and_clears_fragments() {
        let store = InMemoryCoverageStore::new();
        store
            .seed_declared(DEP, &urns(&["urn:a", "urn:b"]))
            .await
            .unwrap();
        store.mark_path_covered(DEP, "urn:a").await.unwrap();
        store
            .write_fragment(
                DEP,
                &CoverageFragment {
                    route_urn: "urn:r".to_string(),
                    segment_process: "p1".to_string(),
                    instance_id: "i1".to_string(),
                    business_key: None,
                    trace_id: None,
                },
            )
            .await
            .unwrap();

        store.reset(DEP).await.unwrap();

        // Flags re-seeded false (total preserved), fragments cleared.
        let m = store.read_metrics(DEP).await.unwrap();
        assert_eq!((m.total, m.covered), (2, 0));
        assert_eq!(m.uncovered, urns(&["urn:a", "urn:b"]));
        assert!(store.read_fragments(DEP).await.unwrap().is_empty());
    }

    // ---- seed-set derivation + injected-sub-path split ------------------------------------

    #[test]
    fn split_injected_sub_path_parses_route_and_process() {
        // A desugar-injected cross-process sub-path → (route_urn, segment_process).
        assert_eq!(
            split_injected_sub_path("urn:sutra:coverage:demoflow:e2e:reply1#p2"),
            Some((
                "urn:sutra:coverage:demoflow:e2e:reply1".to_string(),
                "p2".to_string()
            ))
        );
        // An intra-process author mnemonic is NOT an injected sub-path.
        assert_eq!(split_injected_sub_path("accept"), None);
        // A coverage URN without a `#` process suffix is a ROUTE urn, not a sub-path.
        assert_eq!(
            split_injected_sub_path("urn:sutra:coverage:demoflow:e2e:reply1"),
            None
        );
    }

    #[test]
    fn metric_flag_urn_collapses_injected_subpaths_only() {
        // The per-id mapping seeding, marking and reporting all share.
        assert_eq!(
            metric_flag_urn("urn:sutra:coverage:demoflow:e2e:reply1#p2"),
            "urn:sutra:coverage:demoflow:e2e:reply1"
        );
        assert_eq!(metric_flag_urn("accept"), "accept");
        assert_eq!(
            metric_flag_urn("urn:sutra:coverage:demoflow:e2e:reply1"),
            "urn:sutra:coverage:demoflow:e2e:reply1"
        );
    }

    #[test]
    fn seed_urns_yields_route_urns_plus_intra_ids_never_hash_subpaths() {
        // The worked-example shape: one intra path on p1, plus the reply1 route injected on p1/p2/p3.
        let declared = vec![
            "happy".to_string(), // intra-process author mnemonic
            "urn:sutra:coverage:demoflow:e2e:reply1#p1".to_string(),
            "urn:sutra:coverage:demoflow:e2e:reply1#p2".to_string(),
            "urn:sutra:coverage:demoflow:e2e:reply1#p3".to_string(),
        ];
        let seed = seed_urns(declared);
        // The three `#p` sub-paths collapse to ONE route urn; the intra id passes through.
        // NO `#p` sub-path appears in the seed set (they are cursors, not metric flags).
        assert_eq!(
            seed,
            vec![
                "happy".to_string(),
                "urn:sutra:coverage:demoflow:e2e:reply1".to_string(),
            ]
        );
        assert!(!seed.iter().any(|u| u.contains('#')));
    }
}
