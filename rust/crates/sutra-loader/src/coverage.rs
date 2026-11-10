//! Cross-process (collaboration) coverage — the config/load foundation (see the book's
//! *Coverage: declared routes as the compliance signal* chapter).
//!
//! A `coverage/**` file is a new URN-identified authored artifact (like `templates/**`) that
//! declares **business correlations** (per-hop links) and **coverage routes** (per-process
//! `segments`) spanning several participant processes of one collaboration. This module owns:
//!
//! 1. the parsed model ([`CoverageFile`] / [`BusinessCorrelation`] / [`Hop`] /
//!    [`CoverageRoute`]) and its YAML shape;
//! 2. URN derivation ([`coverage_urn`]) — `urn:sutra:coverage:<folder…>:<file>` (extension
//!    OMITTED: coverage is a single-extension type, unlike templates/scripts);
//! 3. **desugar-inject at load** ([`LoadedDeployment::inject_coverage_paths`]) — each route
//!    `segments[p]` becomes an injected `coverage_path` on process `p`'s `ProcessDefinition`
//!    (fq id `urn:sutra:coverage:<file>:<path>#<p>`), reusing the existing
//!    [`sutra_bpmn::model::ProcessDefinition::with_coverage_paths`] builder so the EXISTING
//!    runtime cursor marks the injected sub-path unchanged.
//!
//! It does NOT parse `<bpmn:collaboration>` / `<bpmn:participant>` / `<bpmn:messageFlow>`
//! — coverage files replace that. Validation, the coverage store, runtime marking and the
//! CLI are later phases.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::Deserialize;
use sutra_bpmn::model::{CoveragePath, ProcessModule};

use crate::error::LoaderError;
use crate::scanner::{LoadedArtifact, LoadedDeployment};

/// Coverage files are single-extension (`.yaml`/`.yml`) — so the URN OMITS the extension.
pub(crate) const COVERAGE_SUFFIXES: &[&str] = &[".yaml", ".yml"];

/// The reserved file at the root of `coverage/` that maps declared paths to what a compliance
/// report states about them. It travels in the sealed archive with the coverage files, but it is
/// not one of them: it declares no routes.
pub const REQUIREMENTS_MAP_FILE: &str = "requirements-map.yaml";

/// The weight of a declared path that states none, or that the map does not mention.
pub const DEFAULT_ROUTE_WEIGHT: f64 = 1.0;

/// The parsed `coverage/requirements-map.yaml`: declared path → what it stands for. A package with
/// no such file has no compliance view, and `coverage check` reports as it always has.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequirementsMap {
    /// Keyed by the path's metric URN, as `coverage check` lists it: a cross-process route's
    /// `urn:sutra:coverage:<file>:<path>`, or an intra-process `<q:coverage>` path id.
    #[serde(default)]
    pub routes: BTreeMap<String, RouteRequirements>,
}

/// What one declared path stands for in a compliance report.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRequirements {
    /// The requirement ids this path evidences.
    #[serde(default)]
    pub requirements: Vec<String>,
    /// This path's share of the weighted percentage, relative to the others.
    #[serde(default = "default_route_weight")]
    pub weight: f64,
    /// The ids of the test cases that walk this path. Opaque here: a case is the test client's
    /// concept, and the report only hands these back.
    #[serde(default)]
    pub cases: Vec<String>,
}

fn default_route_weight() -> f64 {
    DEFAULT_ROUTE_WEIGHT
}

/// Parse and validate a requirements map. Every declared path counts, so a weight of zero is
/// refused along with a negative one: it would be an exclusion by another name. An unknown key,
/// `exclude` among them, is refused by the parser for the same reason.
pub fn parse_requirements_map(content: &str) -> Result<RequirementsMap, String> {
    let map: RequirementsMap = serde_yaml::from_str(content).map_err(|e| e.to_string())?;
    for (route, entry) in &map.routes {
        if !entry.weight.is_finite() || entry.weight <= 0.0 {
            return Err(format!(
                "route '{route}': weight {} must be a number above zero; every declared path \
                 counts",
                entry.weight
            ));
        }
        if let Some(blank) = entry
            .requirements
            .iter()
            .chain(&entry.cases)
            .find(|id| id.trim().is_empty())
        {
            return Err(format!(
                "route '{route}': a requirement or case id is blank ('{blank}')"
            ));
        }
    }
    Ok(map)
}

/// One parsed `coverage/**` file: a set of [`BusinessCorrelation`]s plus its derived URN.
/// `urn` is NOT present in the YAML — it is derived from the archive-local subpath by
/// [`coverage_urn`] after the body is deserialized.
#[derive(Debug, Clone, Deserialize)]
pub struct CoverageFile {
    /// `urn:sutra:coverage:<folder…>:<file>` — derived from the file's subpath, not the YAML.
    #[serde(default)]
    pub urn: String,
    /// The correlations declared by this file (each a key + links + coverage routes).
    #[serde(default)]
    pub correlations: Vec<BusinessCorrelation>,
}

/// A correlated business flow: processes tied by per-hop correlation, plus the coverage
/// routes drawn over them. `id` is a mnemonic, made globally unique by the file URN.
#[derive(Debug, Clone, Deserialize)]
pub struct BusinessCorrelation {
    /// Mnemonic, unique under the file URN.
    pub id: String,
    /// The correlation's DEFAULT hop key (a FEEL expression / `header.<field>`); a hop sets
    /// its own [`Hop::key`] only when its request/reply leg correlates on a different value.
    pub key: String,
    /// Each hop connects two instances by its (effective) key — a request/reply leg.
    #[serde(default)]
    pub links: Vec<Hop>,
    /// The coverage routes over this correlation (mnemonic path ids, unique under the URN).
    #[serde(default)]
    pub coverages: Vec<CoverageRoute>,
}

/// One correlation hop: a directed link from a `<q:send>`-side node to a consuming
/// start-event `<q:source>` / `imec` relay-wait, optionally overriding the correlation's
/// default key for this leg (`None` = use the correlation's `key`).
#[derive(Debug, Clone, Deserialize)]
pub struct Hop {
    /// The emitting node (`from` in the YAML).
    #[serde(rename = "from")]
    pub from_node: String,
    /// The consuming node (`to` in the YAML).
    #[serde(rename = "to")]
    pub to_node: String,
    /// Per-leg key override; `None` = use the correlation's default `key`.
    #[serde(default)]
    pub key: Option<String>,
}

/// One coverage route: a mnemonic `path` id plus the FULL, self-contained per-process
/// `segments` (processId → ordered flow ids). No inheritance between routes.
#[derive(Debug, Clone, Deserialize)]
pub struct CoverageRoute {
    /// Mnemonic path id, unique under the file URN. Fully-qualified as
    /// `urn:sutra:coverage:<file>:<path>`.
    pub path: String,
    /// Per-process sub-paths: processId → ordered sequence-flow ids.
    #[serde(default)]
    pub segments: BTreeMap<String, Vec<String>>,
}

/// The deserialized YAML body — `urn` is derived separately (not in the file), so the raw
/// document maps to just the correlations.
#[derive(Debug, Clone, Deserialize)]
struct CoverageFileBody {
    #[serde(default)]
    correlations: Vec<BusinessCorrelation>,
}

/// Derive a coverage file's URN from its archive-local subpath under `coverage/`.
///
/// `urn:sutra:coverage:<folder-path-colon-delimited>:<filename-without-extension>` — the
/// `coverage/` root is NOT part of the colon path, and the extension is OMITTED (coverage is
/// a single-extension type; contrast templates/scripts which KEEP it because they admit
/// several). Mirrors the `/`→`:` folder-to-URN convention used for codec names
/// (`build_codec_map`). Example: `orders/e2e.yaml` → `urn:sutra:coverage:orders:e2e`.
///
/// Public so the `sutra coverage init` connectable-graph generator derives the same
/// URN for the coverage file it emits under `coverage/`.
pub fn coverage_urn(subpath: &str) -> String {
    let mut parts: Vec<&str> = subpath.split('/').collect();
    // The last component is the file name — strip its (single) extension.
    let file = parts.pop().unwrap_or("");
    let stem = COVERAGE_SUFFIXES
        .iter()
        .find_map(|s| file.strip_suffix(s))
        .unwrap_or(file);
    let mut urn = String::from("urn:sutra:coverage:");
    for folder in parts {
        urn.push_str(folder);
        urn.push(':');
    }
    urn.push_str(stem);
    urn
}

/// Parse a set of raw `coverage/**` artifacts (subpath → content) into [`CoverageFile`]s,
/// deriving each URN from its subpath. Deterministic (input is a sorted `BTreeMap`). A
/// malformed coverage document fails the load with `err_code`.
pub(crate) fn build_coverage_files(
    files: &BTreeMap<String, LoadedArtifact>,
    err_code: &'static str,
) -> Result<Vec<CoverageFile>, LoaderError> {
    let mut out = Vec::with_capacity(files.len());
    for (sub, artifact) in files {
        if sub == REQUIREMENTS_MAP_FILE {
            continue;
        }
        let body: CoverageFileBody = serde_yaml::from_str(&artifact.content).map_err(|e| {
            LoaderError::new(
                err_code,
                format!("coverage/{sub} is not a valid coverage file: {e}"),
            )
        })?;
        out.push(CoverageFile {
            urn: coverage_urn(sub),
            correlations: body.correlations,
        });
    }
    Ok(out)
}

impl LoadedDeployment {
    /// Parse `coverage/**` (raw → [`CoverageFile`]) into [`Self::coverages`], then
    /// desugar-inject the routes onto the referenced processes ([`Self::inject_coverage_paths`]).
    /// `err_code` classifies a malformed coverage document (layout vs archive-content).
    pub(crate) fn resolve_coverage(&mut self, err_code: &'static str) -> Result<(), LoaderError> {
        self.coverages = build_coverage_files(&self.coverage_files, err_code)?;
        self.inject_coverage_paths();
        if let Some(artifact) = self.coverage_files.get(REQUIREMENTS_MAP_FILE) {
            parse_requirements_map(&artifact.content).map_err(|e| {
                LoaderError::new(
                    err_code,
                    format!("coverage/{REQUIREMENTS_MAP_FILE} is not a valid mapping: {e}"),
                )
            })?;
        }
        Ok(())
    }

    /// The package's requirements map, or `None` when it ships none. Loading already refused a
    /// malformed one; the `Err` is the same refusal for a deployment assembled some other way.
    pub fn requirements_map(&self) -> Result<Option<RequirementsMap>, String> {
        self.coverage_files
            .get(REQUIREMENTS_MAP_FILE)
            .map(|artifact| parse_requirements_map(&artifact.content))
            .transpose()
    }

    /// Every declared path by its metric URN, ascending: the "total to cover". The intra-process
    /// paths and injected sub-paths collapse through the same mapping seeding and marking use, and
    /// a route over a process this deployment does not declare still counts, because it is
    /// declared and nothing can ever cover it.
    pub fn declared_coverage_urns(&self) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();
        for (pid, module) in &self.processes {
            if let Ok(process) = module.process(pid) {
                ids.extend(process.coverage_paths.iter().map(|p| p.id.clone()));
            }
        }
        for file in &self.coverages {
            for corr in &file.correlations {
                ids.extend(
                    corr.coverages
                        .iter()
                        .map(|route| format!("{}:{}", file.urn, route.path)),
                );
            }
        }
        sutra_executor::coverage::seed_urns(ids)
    }

    /// Desugar-inject at load: for each coverage route `segments[p]` (processId → flow
    /// ids), inject a `coverage_path` (fq id `urn:sutra:coverage:<file>:<path>#<p>`) onto
    /// process `p`'s `ProcessDefinition`, reusing
    /// [`sutra_bpmn::model::ProcessDefinition::with_coverage_paths`] so the existing runtime
    /// cursor marks it unchanged. Any injected path is APPENDED to the process's own
    /// `<q:coverage>` paths (never replaces them).
    ///
    /// A referenced processId that is NOT in the deployment is silently skipped — that is
    /// phase-2 validation's job (`PROCESS_UNKNOWN`), and the load stays resilient.
    ///
    /// Module `Arc`s shared across process ids (a multi-process `.bpmn` file) are rebuilt
    /// ONCE and swapped consistently into both [`Self::processes`] and
    /// [`Self::process_files`], preserving the `Arc::ptr_eq` backing invariant.
    pub(crate) fn inject_coverage_paths(&mut self) {
        // processId → the coverage paths to inject onto it.
        let mut by_process: BTreeMap<String, Vec<CoveragePath>> = BTreeMap::new();
        for file in &self.coverages {
            for corr in &file.correlations {
                for route in &corr.coverages {
                    for (pid, flows) in &route.segments {
                        by_process
                            .entry(pid.clone())
                            .or_default()
                            .push(CoveragePath {
                                id: format!("{}:{}#{pid}", file.urn, route.path),
                                flows: flows.clone(),
                            });
                    }
                }
            }
        }
        if by_process.is_empty() {
            return;
        }

        // Rebuild each DISTINCT touched module once (keyed by its current Arc address), so
        // shared modules stay shared (and their `process_files` backing stays ptr-equal).
        let mut rebuilt: HashMap<*const ProcessModule, Arc<ProcessModule>> = HashMap::new();
        for module in self.processes.values() {
            let ptr = Arc::as_ptr(module);
            if rebuilt.contains_key(&ptr) {
                continue;
            }
            if let Some(new_module) = module_with_injected(module, &by_process) {
                rebuilt.insert(ptr, new_module);
            }
        }
        if rebuilt.is_empty() {
            return;
        }
        for module in self.processes.values_mut() {
            if let Some(new_module) = rebuilt.get(&Arc::as_ptr(module)) {
                *module = Arc::clone(new_module);
            }
        }
        for file in self.process_files.values_mut() {
            if let Some(new_module) = rebuilt.get(&Arc::as_ptr(&file.module)) {
                file.module = Arc::clone(new_module);
            }
        }
    }
}

/// Rebuild `module` with `by_process` coverage paths appended to the matching processes —
/// `None` when this module contains no injected process (leave its `Arc` untouched). Uses
/// only public `sutra-bpmn` APIs (the private `processes` field is preserved via clone).
fn module_with_injected(
    module: &ProcessModule,
    by_process: &BTreeMap<String, Vec<CoveragePath>>,
) -> Option<Arc<ProcessModule>> {
    let touched = module
        .process_ids()
        .iter()
        .any(|pid| by_process.contains_key(*pid));
    if !touched {
        return None;
    }
    let mut processes = Vec::with_capacity(module.processes().len());
    for def in module.processes() {
        let def = def.clone();
        let def = match by_process.get(&def.id) {
            Some(extra) => {
                let mut combined = def.coverage_paths.clone();
                combined.extend(extra.iter().cloned());
                def.with_coverage_paths(combined)
            }
            None => def,
        };
        processes.push(def);
    }
    // `of` re-runs the duplicate-process-id check the source module already passed, so this
    // cannot fail; preserve the module's version pin (not a `of` argument).
    let mut rebuilt = ProcessModule::of(
        module.target_namespace.clone(),
        module.imports.clone(),
        processes,
    )
    .expect("rebuilt module preserves the source's unique process ids");
    rebuilt.version = module.version.clone();
    Some(Arc::new(rebuilt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::codes;
    use crate::scanner::LoadedProcessFile;
    use std::path::PathBuf;
    use sutra_bpmn::model::ProcessDefinition;
    use sutra_executor::deployment::DeploymentId;

    /// The worked example (`coverage/orders/e2e.yaml`).
    const ORDERS_E2E: &str = include_str!("../tests/fixtures/coverage/orders/e2e.yaml");
    const ORDERS_E2E_SUBPATH: &str = "orders/e2e.yaml";

    fn parse_fixture() -> CoverageFile {
        let files = BTreeMap::from([(
            ORDERS_E2E_SUBPATH.to_string(),
            LoadedArtifact {
                path: PathBuf::from("coverage/orders/e2e.yaml"),
                content: ORDERS_E2E.to_string(),
            },
        )]);
        let mut coverages =
            build_coverage_files(&files, codes::CONFIG_MODULE_LAYOUT_INVALID).expect("parses");
        assert_eq!(coverages.len(), 1);
        coverages.pop().unwrap()
    }

    #[test]
    fn urn_derivation_omits_extension_and_drops_coverage_root() {
        // Coverage is single-extension → OMIT the extension; the `coverage/` root is
        // not part of the colon path.
        assert_eq!(
            coverage_urn("orders/e2e.yaml"),
            "urn:sutra:coverage:orders:e2e"
        );
        // Flat (no subfolder).
        assert_eq!(coverage_urn("e2e.yaml"), "urn:sutra:coverage:e2e");
        // Nested subfolders → colon-delimited.
        assert_eq!(
            coverage_urn("orders/sub/x.yml"),
            "urn:sutra:coverage:orders:sub:x"
        );
    }

    #[test]
    fn parses_the_section_4_2_example() {
        let file = parse_fixture();
        assert_eq!(file.urn, "urn:sutra:coverage:orders:e2e");
        assert_eq!(file.correlations.len(), 1);
        let corr = &file.correlations[0];
        assert_eq!(corr.id, "transfer");
        assert_eq!(corr.key, "txnId");

        // links: from/to rename to from_node/to_node; per-leg key override on the p2↔p3 legs.
        assert_eq!(corr.links.len(), 5);
        assert_eq!(corr.links[0].from_node, "p1:sendMessage");
        assert_eq!(corr.links[0].to_node, "p2:startEvent");
        assert_eq!(corr.links[0].key, None); // uses the correlation default (txnId)
        assert_eq!(corr.links[2].from_node, "p2:sendMessage");
        assert_eq!(corr.links[2].key.as_deref(), Some("p2p3Ref"));
        assert_eq!(corr.links[4].key.as_deref(), Some("p2p3Ref"));

        // coverages: two self-contained routes over p1/p2/p3.
        assert_eq!(corr.coverages.len(), 2);
        let reply1 = &corr.coverages[0];
        assert_eq!(reply1.path, "reply1");
        assert_eq!(
            reply1.segments.keys().collect::<Vec<_>>(),
            vec!["p1", "p2", "p3"]
        );
        assert_eq!(
            reply1.segments["p1"],
            vec!["startSeq", "noErrorsSequence", "sequence2", "endSeq"]
        );
        assert_eq!(reply1.segments["p3"], vec!["startSeq", "seq1", "endSeq1"]);
        assert_eq!(corr.coverages[1].path, "reply2");
        assert_eq!(
            corr.coverages[1].segments["p3"],
            vec!["startSeq", "seq2", "endSeq2"]
        );
    }

    fn empty_process(id: &str) -> ProcessDefinition {
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
        .expect("empty process is valid")
    }

    /// One module holding p1/p2/p3, shared across three `processes` keys AND one
    /// `process_files` entry — the multi-process-file layout the injector must keep coherent.
    fn deployment_with_shared_module(coverages: Vec<CoverageFile>) -> LoadedDeployment {
        let module = Arc::new(
            ProcessModule::of(
                "urn:sutra:module:orders:1.0.0",
                vec![],
                vec![
                    empty_process("p1"),
                    empty_process("p2"),
                    empty_process("p3"),
                ],
            )
            .expect("module builds"),
        );
        let mut processes = BTreeMap::new();
        for pid in ["p1", "p2", "p3"] {
            processes.insert(pid.to_string(), Arc::clone(&module));
        }
        let mut process_files = BTreeMap::new();
        process_files.insert(
            "orders.bpmn".to_string(),
            LoadedProcessFile {
                path: PathBuf::from("bpmn/orders.bpmn"),
                content: String::new(),
                module: Arc::clone(&module),
            },
        );
        LoadedDeployment {
            id: DeploymentId::of("dep-0000000000000000000000d1").expect("valid id"),
            tenant: "t".to_string(),
            module: "m".to_string(),
            version: "1.0.0".to_string(),
            namespace: "urn:sutra:module:orders:1.0.0".to_string(),
            processes,
            process_files,
            rules: BTreeMap::new(),
            templates: BTreeMap::new(),
            scripts: BTreeMap::new(),
            redactors: BTreeMap::new(),
            codecs: BTreeMap::new(),
            schema_files: BTreeMap::new(),
            migrations: BTreeMap::new(),
            coverage_files: BTreeMap::new(),
            coverages,
            channels_yaml: None,
            datastores_yaml: None,
            properties: Default::default(),
            binding_dir: PathBuf::new(),
        }
    }

    #[test]
    fn desugar_inject_adds_fq_coverage_paths_to_each_process() {
        let file = parse_fixture();
        let mut dep = deployment_with_shared_module(vec![file]);
        dep.inject_coverage_paths();

        let p1 = dep.processes["p1"].process("p1").expect("p1");
        let ids: Vec<&str> = p1.coverage_paths.iter().map(|c| c.id.as_str()).collect();
        // Two routes (reply1, reply2) each inject a p1 sub-path with the fq id `…:<path>#p1`.
        assert_eq!(
            ids,
            vec![
                "urn:sutra:coverage:orders:e2e:reply1#p1",
                "urn:sutra:coverage:orders:e2e:reply2#p1"
            ]
        );
        // The injected flows are the route's segment for that process, verbatim.
        assert_eq!(
            p1.coverage_paths[0].flows,
            vec!["startSeq", "noErrorsSequence", "sequence2", "endSeq"]
        );

        // p3's two routes differ (reply1 vs reply2 last flow) — fq id carries the process suffix.
        let p3 = dep.processes["p3"].process("p3").expect("p3");
        assert_eq!(
            p3.coverage_paths[0].id,
            "urn:sutra:coverage:orders:e2e:reply1#p3"
        );
        assert_eq!(
            p3.coverage_paths[0].flows,
            vec!["startSeq", "seq1", "endSeq1"]
        );
        assert_eq!(
            p3.coverage_paths[1].flows,
            vec!["startSeq", "seq2", "endSeq2"]
        );

        // The shared module was rebuilt ONCE: all three keys + the process_files backing
        // still point to the SAME new Arc (ptr_eq invariant preserved).
        assert!(Arc::ptr_eq(&dep.processes["p1"], &dep.processes["p2"]));
        assert!(Arc::ptr_eq(&dep.processes["p1"], &dep.processes["p3"]));
        assert!(Arc::ptr_eq(
            &dep.processes["p1"],
            &dep.process_files["orders.bpmn"].module
        ));
    }

    #[test]
    fn absent_referenced_process_is_skipped_not_fatal() {
        // A route references p3, but the deployment only has p1/p2 — must not panic, and the
        // present processes still get their sub-paths (phase-2 validation flags the absentee).
        let file = parse_fixture();
        let module = Arc::new(
            ProcessModule::of(
                "urn:sutra:module:orders:1.0.0",
                vec![],
                vec![empty_process("p1"), empty_process("p2")],
            )
            .expect("module builds"),
        );
        let mut processes = BTreeMap::new();
        processes.insert("p1".to_string(), Arc::clone(&module));
        processes.insert("p2".to_string(), Arc::clone(&module));
        let mut dep = deployment_with_shared_module(vec![]);
        dep.processes = processes;
        dep.process_files.clear();
        dep.coverages = vec![file];

        dep.inject_coverage_paths(); // must not panic on the absent p3
        assert_eq!(
            dep.processes["p1"]
                .process("p1")
                .unwrap()
                .coverage_paths
                .len(),
            2
        );
        assert!(!dep.processes.contains_key("p3"));
    }

    #[test]
    fn the_requirements_map_parses_with_a_default_weight() {
        let map = parse_requirements_map(
            "routes:\n  accept:\n    requirements: [REQ-1]\n    weight: 2.5\n    cases: [c1, c2]\n  reject: {}\n",
        )
        .unwrap();
        assert_eq!(map.routes["accept"].weight, 2.5);
        assert_eq!(map.routes["accept"].cases, vec!["c1", "c2"]);
        assert_eq!(map.routes["reject"].weight, DEFAULT_ROUTE_WEIGHT);
        assert!(map.routes["reject"].requirements.is_empty());
    }

    #[test]
    fn the_requirements_map_has_no_way_to_exclude_a_path() {
        for bad in [
            "routes:\n  accept: { weight: 0 }\n",
            "routes:\n  accept: { weight: -1 }\n",
            "routes:\n  accept: { weight: .inf }\n",
            "routes:\n  accept: { exclude: true }\n",
            "routes:\n  accept: { requirements: [''] }\n",
            "paths: {}\n",
        ] {
            assert!(parse_requirements_map(bad).is_err(), "accepted: {bad}");
        }
    }

    #[test]
    fn the_requirements_map_is_not_read_as_a_coverage_file() {
        let artifact = |content: &str| LoadedArtifact {
            path: PathBuf::from("coverage/x.yaml"),
            content: content.to_string(),
        };
        let files = BTreeMap::from([
            (ORDERS_E2E_SUBPATH.to_string(), artifact(ORDERS_E2E)),
            (
                REQUIREMENTS_MAP_FILE.to_string(),
                artifact("routes:\n  accept: { weight: 2 }\n"),
            ),
        ]);
        let parsed = build_coverage_files(&files, codes::CONFIG_MODULE_LAYOUT_INVALID).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].urn, "urn:sutra:coverage:orders:e2e");
    }

    #[test]
    fn a_malformed_requirements_map_fails_the_load() {
        let mut dep = deployment_with_shared_module(vec![]);
        dep.coverage_files.insert(
            REQUIREMENTS_MAP_FILE.to_string(),
            LoadedArtifact {
                path: PathBuf::from("coverage/requirements-map.yaml"),
                content: "routes:\n  accept: { weight: 0 }\n".to_string(),
            },
        );
        let err = dep
            .resolve_coverage(codes::CONFIG_MODULE_LAYOUT_INVALID)
            .unwrap_err();
        assert!(err.to_string().contains("requirements-map.yaml"), "{err}");
    }

    #[test]
    fn declared_urns_collapse_sub_paths_and_count_every_route() {
        let mut dep = deployment_with_shared_module(vec![parse_fixture()]);
        dep.inject_coverage_paths();
        assert_eq!(
            dep.declared_coverage_urns(),
            vec![
                "urn:sutra:coverage:orders:e2e:reply1".to_string(),
                "urn:sutra:coverage:orders:e2e:reply2".to_string(),
            ]
        );
    }
}
