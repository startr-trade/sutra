//! Route enumeration over `sutra_bpmn::ProcessDefinition`, backing `sutra coverage
//! init|check`. Enumerates the simple start→terminal routes of a process's
//! TOP-LEVEL flow graph — a callActivity / subProcess / transaction is a single step in
//! the enclosing route (composition, not coupling; see the book's *Coverage: declared
//! routes as the compliance signal* chapter).
//!
//! Semantics:
//! - Continuations of a node = its outgoing sequence flows PLUS the outgoing flows of any
//!   boundary event attached to it (an interrupting outcome is an alternative route).
//! - Simple paths: a sequence flow is never revisited within one route, so cyclic graphs
//!   terminate (a route that can only continue by re-walking a flow ends where it stands).
//! - Declared coverage routes are transport-agnostic — the intake flow is irrelevant,
//!   because coverage matches by ordered subsequence — so the leading flows sourced
//!   at start events are trimmed and identical remainders deduplicated. A route that is
//!   ONLY intake flows keeps its full form.
//! - Route-explosion cap: enumeration errors out beyond `max_paths` (default 256,
//!   `--max-paths` overrides) instead of flooding a combinatorial process with
//!   declarations.

use sutra_bpmn::{Node, ProcessDefinition, SequenceFlow};

/// The default route-explosion cap.
pub const DEFAULT_MAX_PATHS: usize = 256;

/// Enumeration refused: the process has more routes than the cap allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteExplosion {
    pub cap: usize,
}

impl std::fmt::Display for RouteExplosion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "route enumeration exceeded the cap of {} paths — declare coverage for a \
             coarser process or re-run with a higher --max-paths",
            self.cap
        )
    }
}

/// A start→terminal route: the ordered sequence-flow ids it fires.
pub type Route = Vec<String>;

/// All simple routes of the process (full form, intake flows included), document order.
pub fn enumerate_full_routes(
    process: &ProcessDefinition,
    max_paths: usize,
) -> Result<Vec<Route>, RouteExplosion> {
    let mut routes: Vec<Route> = Vec::new();
    for node in process.nodes() {
        if matches!(node, Node::StartEvent { .. }) {
            let mut current: Vec<&SequenceFlow> = Vec::new();
            walk(process, node.id(), &mut current, &mut routes, max_paths)?;
        }
    }
    Ok(routes)
}

/// The coverage-declaration form: full routes with leading start-event flows trimmed and
/// identical remainders deduplicated (first-discovery order preserved).
pub fn enumerate_coverage_routes(
    process: &ProcessDefinition,
    max_paths: usize,
) -> Result<Vec<Route>, RouteExplosion> {
    let full = enumerate_full_routes(process, max_paths)?;
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for route in full {
        let trimmed = trim_intake(process, &route);
        if seen.insert(trimmed.clone()) {
            out.push(trimmed);
        }
    }
    Ok(out)
}

/// True when `declared` is an ordered subsequence of `route` — the runtime covering rule.
pub fn is_subsequence(declared: &[String], route: &[String]) -> bool {
    if declared.is_empty() {
        return false;
    }
    let mut it = route.iter();
    declared
        .iter()
        .all(|want| it.by_ref().any(|have| have == want))
}

fn walk<'a>(
    process: &'a ProcessDefinition,
    node_id: &str,
    current: &mut Vec<&'a SequenceFlow>,
    routes: &mut Vec<Route>,
    max_paths: usize,
) -> Result<(), RouteExplosion> {
    let mut advanced = false;
    for flow in continuations(process, node_id) {
        if current.iter().any(|f| f.id == flow.id) {
            continue; // simple path: never re-fire a flow within one route
        }
        advanced = true;
        current.push(flow);
        walk(process, &flow.target_ref, current, routes, max_paths)?;
        current.pop();
    }
    if !advanced && !current.is_empty() {
        // Terminal (no outgoing continuation, or only cycles): the walked flows are a route.
        if routes.len() >= max_paths {
            return Err(RouteExplosion { cap: max_paths });
        }
        routes.push(current.iter().map(|f| f.id.clone()).collect());
    }
    Ok(())
}

/// Outgoing flows of the node plus those of its attached boundary events.
fn continuations<'a>(process: &'a ProcessDefinition, node_id: &str) -> Vec<&'a SequenceFlow> {
    let mut out = process.outgoing(node_id);
    for node in process.nodes() {
        if let Node::BoundaryEvent {
            id,
            attached_to_ref,
            ..
        } = node
        {
            if attached_to_ref == node_id {
                out.extend(process.outgoing(id));
            }
        }
    }
    out
}

/// Drop the leading flows whose source is a start event; keep the full route when
/// nothing would remain.
fn trim_intake(process: &ProcessDefinition, route: &Route) -> Route {
    let is_start = |flow_id: &str| {
        process
            .flows()
            .iter()
            .find(|f| f.id == flow_id)
            .map(|f| {
                process
                    .nodes()
                    .iter()
                    .any(|n| n.id() == f.source_ref && matches!(n, Node::StartEvent { .. }))
            })
            .unwrap_or(false)
    };
    let skip = route.iter().take_while(|id| is_start(id)).count();
    if skip == route.len() {
        route.clone()
    } else {
        route[skip..].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_bpmn::BpmnModelLoader;

    fn load(xml: &str) -> sutra_bpmn::ProcessModule {
        BpmnModelLoader::new().load(xml.as_bytes()).unwrap()
    }

    const GATEWAY_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:routes">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="GW"/>
    <bpmn:exclusiveGateway id="GW" default="fB"/>
    <bpmn:sequenceFlow id="fA" sourceRef="GW" targetRef="A">
      <bpmn:conditionExpression>x &gt; 1</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="fB" sourceRef="GW" targetRef="B"/>
    <bpmn:manualTask id="A"/>
    <bpmn:manualTask id="B"/>
    <bpmn:sequenceFlow id="fAEnd" sourceRef="A" targetRef="End"/>
    <bpmn:sequenceFlow id="fBEnd" sourceRef="B" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn enumerates_gateway_branches_and_trims_intake() {
        let module = load(GATEWAY_BPMN);
        let p = module.process("p").unwrap();
        let full = enumerate_full_routes(p, DEFAULT_MAX_PATHS).unwrap();
        assert_eq!(
            full,
            vec![vec!["f0", "fA", "fAEnd"], vec!["f0", "fB", "fBEnd"]]
                .into_iter()
                .map(|v| v.into_iter().map(str::to_owned).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
        let coverage = enumerate_coverage_routes(p, DEFAULT_MAX_PATHS).unwrap();
        assert_eq!(coverage[0], vec!["fA".to_owned(), "fAEnd".to_owned()]);
        assert_eq!(coverage[1], vec!["fB".to_owned(), "fBEnd".to_owned()]);
    }

    #[test]
    fn multiple_intakes_deduplicate_after_trim() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:routes">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S1"><bpmn:extensionElements><q:source channel="a"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:startEvent id="S2"><bpmn:extensionElements><q:source channel="b"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="i1" sourceRef="S1" targetRef="T"/>
    <bpmn:sequenceFlow id="i2" sourceRef="S2" targetRef="T"/>
    <bpmn:manualTask id="T"/>
    <bpmn:sequenceFlow id="f1" sourceRef="T" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let module = load(xml);
        let p = module.process("p").unwrap();
        assert_eq!(
            enumerate_full_routes(p, DEFAULT_MAX_PATHS).unwrap().len(),
            2
        );
        let coverage = enumerate_coverage_routes(p, DEFAULT_MAX_PATHS).unwrap();
        assert_eq!(coverage, vec![vec!["f1".to_owned()]]);
    }

    #[test]
    fn boundary_event_is_an_alternative_continuation() {
        // Mirrors the money-transfer shape: a transaction with a cancel boundary.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:routes">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="i" sourceRef="S" targetRef="Tx"/>
    <bpmn:transaction id="Tx">
      <bpmn:startEvent id="SubS"/>
      <bpmn:sequenceFlow id="t1" sourceRef="SubS" targetRef="SubEnd"/>
      <bpmn:endEvent id="SubEnd"/>
    </bpmn:transaction>
    <bpmn:boundaryEvent id="Cancelled" attachedToRef="Tx">
      <bpmn:cancelEventDefinition/>
    </bpmn:boundaryEvent>
    <bpmn:sequenceFlow id="ok" sourceRef="Tx" targetRef="End"/>
    <bpmn:sequenceFlow id="ko" sourceRef="Cancelled" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let module = load(xml);
        let p = module.process("p").unwrap();
        let coverage = enumerate_coverage_routes(p, DEFAULT_MAX_PATHS).unwrap();
        assert_eq!(coverage, vec![vec!["ok".to_owned()], vec!["ko".to_owned()]]);
    }

    #[test]
    fn cycles_terminate_and_cap_errors() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:routes">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="i" sourceRef="S" targetRef="GW"/>
    <bpmn:exclusiveGateway id="GW" default="fEnd"/>
    <bpmn:sequenceFlow id="loop" sourceRef="GW" targetRef="T">
      <bpmn:conditionExpression>again</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:manualTask id="T"/>
    <bpmn:sequenceFlow id="back" sourceRef="T" targetRef="GW"/>
    <bpmn:sequenceFlow id="fEnd" sourceRef="GW" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let module = load(xml);
        let p = module.process("p").unwrap();
        let coverage = enumerate_coverage_routes(p, DEFAULT_MAX_PATHS).unwrap();
        // Both the exit route and the loop-then-exit route terminate.
        assert!(coverage.contains(&vec!["fEnd".to_owned()]));
        assert!(coverage.contains(&vec![
            "loop".to_owned(),
            "back".to_owned(),
            "fEnd".to_owned()
        ]));

        let err = enumerate_full_routes(p, 1).unwrap_err();
        assert_eq!(err.cap, 1);
    }

    #[test]
    fn subsequence_matching_is_ordered() {
        let route: Vec<String> = ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();
        let hit: Vec<String> = ["b", "d"].iter().map(|s| s.to_string()).collect();
        let miss: Vec<String> = ["d", "b"].iter().map(|s| s.to_string()).collect();
        assert!(is_subsequence(&hit, &route));
        assert!(!is_subsequence(&miss, &route));
        assert!(!is_subsequence(&[], &route));
    }
}
