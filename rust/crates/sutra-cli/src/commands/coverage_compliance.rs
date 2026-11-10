//! The compliance view of a coverage check: a package's `coverage/requirements-map.yaml` read over
//! the covered flags. Pure, so the arithmetic is provable without a database.
//!
//! Every declared path counts. A path the map does not mention still weighs its default in the
//! weighted percentage and is listed as unmapped, because leaving it out would inflate the number
//! a compliance report exists to state.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};
use sutra_loader::{RequirementsMap, DEFAULT_ROUTE_WEIGHT};

/// One requirement's standing across the routes mapped to it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequirementStanding {
    pub requirement: String,
    pub total: u64,
    pub covered: u64,
    /// Weighted by the routes' weights, two decimals.
    pub percentage: f64,
    pub uncovered: Vec<String>,
}

/// One uncovered declared path and what would cover it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UncoveredRoute {
    pub path: String,
    pub requirements: Vec<String>,
    pub weight: f64,
    pub cases: Vec<String>,
}

/// The compliance figures for one set of covered flags.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComplianceReport {
    pub weighted_total: f64,
    pub weighted_covered: f64,
    /// Two decimals, `0.0` for an empty declaration, as the plain percentage rounds.
    pub weighted_percentage: f64,
    pub requirements: Vec<RequirementStanding>,
    pub uncovered_routes: Vec<UncoveredRoute>,
    /// The union of the uncovered routes' cases, ascending. Running these is what a client that
    /// resumes a cycle needs to do next.
    pub cases_to_run: Vec<String>,
    /// Uncovered routes no case is mapped to, so that no re-run can cover them as things stand.
    pub routes_without_cases: Vec<String>,
    /// Declared paths the map does not mention.
    pub unmapped_routes: Vec<String>,
}

fn percentage(part: f64, whole: f64) -> f64 {
    if whole <= 0.0 {
        0.0
    } else {
        (part * 10000.0 / whole).round() / 100.0
    }
}

/// Read `map` over the declared paths and the subset of them that is covered.
pub(crate) fn compliance_report(
    map: &RequirementsMap,
    declared: &[String],
    covered: &BTreeSet<String>,
) -> ComplianceReport {
    let mut weighted_total = 0.0;
    let mut weighted_covered = 0.0;
    // requirement -> (weight total, weight covered, total, covered, uncovered paths)
    let mut by_requirement: BTreeMap<&str, (f64, f64, u64, u64, Vec<String>)> = BTreeMap::new();
    let mut uncovered_routes = Vec::new();
    let mut cases_to_run = BTreeSet::new();
    let mut routes_without_cases = Vec::new();
    let mut unmapped_routes = Vec::new();

    for path in declared {
        let mapped = map.routes.get(path);
        if mapped.is_none() {
            unmapped_routes.push(path.clone());
        }
        let weight = mapped.map_or(DEFAULT_ROUTE_WEIGHT, |m| m.weight);
        let is_covered = covered.contains(path);
        weighted_total += weight;
        if is_covered {
            weighted_covered += weight;
        }
        let requirements: BTreeSet<&str> = mapped
            .map(|m| m.requirements.iter().map(String::as_str).collect())
            .unwrap_or_default();
        for requirement in &requirements {
            let standing = by_requirement.entry(requirement).or_default();
            standing.0 += weight;
            standing.2 += 1;
            if is_covered {
                standing.1 += weight;
                standing.3 += 1;
            } else {
                standing.4.push(path.clone());
            }
        }
        if !is_covered {
            let cases: Vec<String> = mapped.map(|m| m.cases.clone()).unwrap_or_default();
            if cases.is_empty() {
                routes_without_cases.push(path.clone());
            }
            cases_to_run.extend(cases.iter().cloned());
            uncovered_routes.push(UncoveredRoute {
                path: path.clone(),
                requirements: requirements.iter().map(|r| r.to_string()).collect(),
                weight,
                cases,
            });
        }
    }

    ComplianceReport {
        weighted_total,
        weighted_covered,
        weighted_percentage: percentage(weighted_covered, weighted_total),
        requirements: by_requirement
            .into_iter()
            .map(
                |(requirement, (weight, weight_covered, total, covered, uncovered))| {
                    RequirementStanding {
                        requirement: requirement.to_string(),
                        total,
                        covered,
                        percentage: percentage(weight_covered, weight),
                        uncovered,
                    }
                },
            )
            .collect(),
        uncovered_routes,
        cases_to_run: cases_to_run.into_iter().collect(),
        routes_without_cases,
        unmapped_routes,
    }
}

impl ComplianceReport {
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "weightedPercentage": self.weighted_percentage,
            "weightedTotal": self.weighted_total,
            "weightedCovered": self.weighted_covered,
            "requirements": self.requirements.iter().map(|r| json!({
                "requirement": r.requirement,
                "total": r.total,
                "covered": r.covered,
                "coveragePercentage": r.percentage,
                "uncovered": r.uncovered,
            })).collect::<Vec<_>>(),
            "uncoveredRoutes": self.uncovered_routes.iter().map(|r| json!({
                "path": r.path,
                "requirements": r.requirements,
                "weight": r.weight,
                "cases": r.cases,
            })).collect::<Vec<_>>(),
            "casesToRun": self.cases_to_run,
            "routesWithoutCases": self.routes_without_cases,
            "unmappedRoutes": self.unmapped_routes,
        })
    }

    pub(crate) fn render_text(&self, out: &mut dyn std::io::Write) {
        let _ = writeln!(
            out,
            "  weighted: {:.2}%  ({} of {} by weight)",
            self.weighted_percentage, self.weighted_covered, self.weighted_total
        );
        if !self.requirements.is_empty() {
            let _ = writeln!(out, "  requirements ({}):", self.requirements.len());
            for r in &self.requirements {
                let _ = writeln!(
                    out,
                    "    {}: {:.2}%  ({} of {} routes)",
                    r.requirement, r.percentage, r.covered, r.total
                );
            }
        }
        if !self.uncovered_routes.is_empty() {
            let _ = writeln!(out, "  uncovered routes and the cases that cover them:");
            for r in &self.uncovered_routes {
                let cases = if r.cases.is_empty() {
                    "(no case mapped)".to_string()
                } else {
                    r.cases.join(", ")
                };
                let _ = writeln!(out, "    - {}: {cases}", r.path);
            }
        }
        if !self.cases_to_run.is_empty() {
            let _ = writeln!(out, "  cases to run: {}", self.cases_to_run.join(", "));
        }
        if !self.unmapped_routes.is_empty() {
            let _ = writeln!(
                out,
                "  not in the requirements map (counted at weight {DEFAULT_ROUTE_WEIGHT}): {}",
                self.unmapped_routes.join(", ")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> RequirementsMap {
        sutra_loader::parse_requirements_map(
            r#"
routes:
  accept: { requirements: [REQ-1, REQ-2], weight: 3, cases: [case-accept] }
  reject: { requirements: [REQ-2], cases: [case-reject, case-reject-alt] }
  timeout: { requirements: [REQ-3], weight: 2 }
"#,
        )
        .unwrap()
    }

    fn declared() -> Vec<String> {
        ["accept", "reject", "timeout", "unlisted"]
            .map(String::from)
            .to_vec()
    }

    fn covered(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn weighted_percentage_counts_every_declared_path_by_its_weight() {
        let r = compliance_report(&map(), &declared(), &covered(&["accept"]));
        // 3 of 3 + 1 + 2 + the unlisted path's default 1.
        assert_eq!(r.weighted_total, 7.0);
        assert_eq!(r.weighted_covered, 3.0);
        assert_eq!(r.weighted_percentage, 42.86);
        assert_eq!(r.unmapped_routes, vec!["unlisted".to_string()]);
    }

    #[test]
    fn each_requirement_reads_only_its_own_routes() {
        let r = compliance_report(&map(), &declared(), &covered(&["accept"]));
        let by: BTreeMap<_, _> = r
            .requirements
            .iter()
            .map(|s| (s.requirement.as_str(), s))
            .collect();
        assert_eq!(by["REQ-1"].percentage, 100.0);
        assert_eq!((by["REQ-1"].covered, by["REQ-1"].total), (1, 1));
        // accept weighs 3, reject 1.
        assert_eq!(by["REQ-2"].percentage, 75.0);
        assert_eq!(by["REQ-2"].uncovered, vec!["reject".to_string()]);
        assert_eq!(by["REQ-3"].percentage, 0.0);
    }

    #[test]
    fn an_uncovered_route_lists_the_cases_that_would_cover_it() {
        let r = compliance_report(&map(), &declared(), &covered(&["accept"]));
        let reject = r.uncovered_routes.iter().find(|u| u.path == "reject");
        assert_eq!(
            reject.unwrap().cases,
            vec!["case-reject".to_string(), "case-reject-alt".to_string()]
        );
        assert_eq!(
            r.cases_to_run,
            vec!["case-reject".to_string(), "case-reject-alt".to_string()]
        );
        // Nothing can re-cover these two as the map stands, and the report says so.
        assert_eq!(
            r.routes_without_cases,
            vec!["timeout".to_string(), "unlisted".to_string()]
        );
    }

    #[test]
    fn full_coverage_is_one_hundred_with_nothing_left_to_run() {
        let r = compliance_report(&map(), &declared(), &covered(&declared_refs()));
        assert_eq!(r.weighted_percentage, 100.0);
        assert!(r.uncovered_routes.is_empty());
        assert!(r.cases_to_run.is_empty());
    }

    fn declared_refs() -> [&'static str; 4] {
        ["accept", "reject", "timeout", "unlisted"]
    }

    #[test]
    fn an_empty_declaration_is_zero_percent() {
        let r = compliance_report(&map(), &[], &BTreeSet::new());
        assert_eq!(r.weighted_percentage, 0.0);
        assert!(r.requirements.is_empty());
    }
}
