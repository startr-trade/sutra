//! `sutra dispatch-graph` — emit a graphviz `.dot` or mermaid diagram of a BPMN file's
//! dispatch tree: nodes are BPMN elements, edges are sequence flows. Read-only streaming
//! walk.

use std::path::PathBuf;

use crate::bpmn_walk::{attr, local_name, walk_bpmn, WalkEvent};
use crate::exit;
use crate::output::{graph_format, GraphFormat, Io};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct DispatchGraphArgs {
    /// BPMN file to render.
    pub bpmn_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node {
    id: String,
    label: String,
    kind: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    source: String,
    target: String,
    label: Option<String>,
}

#[derive(Debug, Default)]
struct Graph {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

pub fn execute(args: DispatchGraphArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match graph_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "dispatch-graph: {msg}");
            return exit::USAGE;
        }
    };
    if !args.bpmn_file.is_file() {
        let _ = writeln!(
            io.err,
            "dispatch-graph: file not found: {}",
            args.bpmn_file.display()
        );
        return exit::USAGE;
    }
    let xml = match std::fs::read_to_string(&args.bpmn_file) {
        Ok(xml) => xml,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "dispatch-graph: cannot read {}: {e}",
                args.bpmn_file.display()
            );
            return exit::USAGE;
        }
    };
    let graph = match parse(&xml) {
        Ok(g) => g,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "dispatch-graph: failed to parse {}: {e}",
                args.bpmn_file.display()
            );
            return exit::USAGE;
        }
    };
    match format {
        GraphFormat::Dot => {
            let _ = writeln!(io.out, "{}", render_dot(&graph));
        }
        GraphFormat::Mermaid => {
            let _ = write!(io.out, "{}", render_mermaid(&graph));
        }
    }
    exit::OK
}

fn parse(xml: &str) -> Result<Graph, String> {
    let mut graph = Graph::default();
    let mut in_process = false;

    walk_bpmn(xml, |event| match event {
        WalkEvent::Start(e) | WalkEvent::Empty(e) => {
            let name = local_name(e);
            if name == "process" {
                in_process = true;
                return;
            }
            if !in_process {
                return;
            }
            let id = attr(e, "id");
            let label = attr(e, "name");
            let kind: Option<&'static str> = match name.as_str() {
                "startEvent" => Some("start"),
                "endEvent" => Some("end"),
                "serviceTask" => Some("service"),
                "userTask" => Some("user"),
                "scriptTask" | "task" => Some("task"),
                "callActivity" => Some("call"),
                "exclusiveGateway" => Some("gateway-exclusive"),
                "inclusiveGateway" => Some("gateway-inclusive"),
                "parallelGateway" => Some("gateway-parallel"),
                "sequenceFlow" => {
                    if let (Some(source), Some(target)) =
                        (attr(e, "sourceRef"), attr(e, "targetRef"))
                    {
                        graph.edges.push(Edge {
                            source,
                            target,
                            label: label.clone().filter(|l| !l.trim().is_empty()),
                        });
                    }
                    None
                }
                _ => None,
            };
            if let (Some(kind), Some(id)) = (kind, id) {
                let label = label
                    .filter(|l| !l.trim().is_empty())
                    .unwrap_or_else(|| id.clone());
                // Last write wins per id, preserving first-seen order (mirrors a keyed map).
                if let Some(existing) = graph.nodes.iter_mut().find(|n| n.id == id) {
                    existing.label = label;
                    existing.kind = kind;
                } else {
                    graph.nodes.push(Node { id, label, kind });
                }
            }
        }
        WalkEvent::End(name) => {
            if name == "process" {
                in_process = false;
            }
        }
    })?;
    Ok(graph)
}

// ----- graphviz -----

fn render_dot(g: &Graph) -> String {
    let mut s = String::new();
    s.push_str("digraph BpmnDispatch {\n");
    s.push_str("  rankdir=LR;\n");
    s.push_str("  node [fontname=Helvetica];\n");
    for n in &g.nodes {
        s.push_str(&format!(
            "  {} [label={}, shape={}",
            quote(&n.id),
            quote(&n.label),
            dot_shape(n.kind)
        ));
        if let Some(fill) = dot_fill(n.kind) {
            s.push_str(&format!(", style=filled, fillcolor=\"{fill}\""));
        }
        s.push_str("];\n");
    }
    for e in &g.edges {
        s.push_str(&format!("  {} -> {}", quote(&e.source), quote(&e.target)));
        if let Some(label) = &e.label {
            s.push_str(&format!(" [label={}]", quote(label)));
        }
        s.push_str(";\n");
    }
    s.push('}');
    s
}

fn dot_shape(kind: &str) -> &'static str {
    match kind {
        "start" | "end" => "circle",
        "service" | "user" | "task" | "call" => "box",
        "gateway-exclusive" | "gateway-inclusive" | "gateway-parallel" => "diamond",
        _ => "ellipse",
    }
}

fn dot_fill(kind: &str) -> Option<&'static str> {
    match kind {
        "start" => Some("#cfe8cf"),
        "end" => Some("#f4c7c7"),
        "service" => Some("#cfd8e8"),
        "user" => Some("#e8e0c0"),
        "gateway-exclusive" | "gateway-inclusive" | "gateway-parallel" => Some("#fff4c0"),
        _ => None,
    }
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

// ----- mermaid -----

fn render_mermaid(g: &Graph) -> String {
    let mut s = String::new();
    s.push_str("flowchart LR\n");
    for n in &g.nodes {
        s.push_str(&format!(
            "  {}{}\n",
            mermaid_id(&n.id),
            mermaid_shape(n.kind, &n.label)
        ));
    }
    for e in &g.edges {
        s.push_str(&format!("  {} --> ", mermaid_id(&e.source)));
        if let Some(label) = &e.label {
            s.push_str(&format!("|{}| ", escape_mermaid(label)));
        }
        s.push_str(&format!("{}\n", mermaid_id(&e.target)));
    }
    s
}

/// Mermaid node ids must be bare tokens; sanitise everything outside `[A-Za-z0-9_]`-ish
/// alphanumerics to underscores so ids with dots/hyphens stay lexer-safe.
fn mermaid_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn mermaid_shape(kind: &str, label: &str) -> String {
    let safe = escape_mermaid(label);
    match kind {
        "start" | "end" => format!("(({safe}))"),
        "gateway-exclusive" | "gateway-inclusive" | "gateway-parallel" => format!("{{{safe}}}"),
        _ => format!("[{safe}]"),
    }
}

fn escape_mermaid(s: &str) -> String {
    s.replace('"', "\\\"").replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    //! Behavior carried over from the reference baseline's test suite.

    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::{scratch_file, BRANCHING_BPMN, HELLO_BPMN};

    fn run(args: DispatchGraphArgs, format: Option<&str>) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured("", |io| execute(args, &global, io))
    }

    #[test]
    fn emits_graphviz_dot_by_default() {
        let file = scratch_file("graph", "branching.bpmn", BRANCHING_BPMN);
        let (code, out, _) = run(DispatchGraphArgs { bpmn_file: file }, None);
        assert_eq!(code, crate::exit::OK);
        for expected in [
            "digraph BpmnDispatch",
            "\"S\"",
            "\"Validate\"",
            "\"GW\"",
            "shape=diamond",
            "shape=box",
            "\"S\" -> \"Validate\"",
            "\"GW\" -> \"Store\"",
            "\"Store\" -> \"End\"",
        ] {
            assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
        }
    }

    #[test]
    fn emits_mermaid_flowchart() {
        let file = scratch_file("graph", "hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(DispatchGraphArgs { bpmn_file: file }, Some("mermaid"));
        assert_eq!(code, crate::exit::OK);
        assert!(out.starts_with("flowchart LR"), "{out}");
        for expected in [
            "Start((",
            "End((",
            "Greet[",
            "Start --> Greet",
            "Greet --> End",
        ] {
            assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
        }
    }

    #[test]
    fn invalid_format_is_a_usage_error() {
        let file = scratch_file("graph", "hello.bpmn", HELLO_BPMN);
        let (code, _, err) = run(DispatchGraphArgs { bpmn_file: file }, Some("svg"));
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("unsupported --format"), "{err}");
    }

    #[test]
    fn missing_file_is_a_usage_error() {
        let (code, _, err) = run(
            DispatchGraphArgs {
                bpmn_file: PathBuf::from("/does/not/exist.bpmn"),
            },
            None,
        );
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("file not found"), "{err}");
    }

    #[test]
    fn dot_output_snapshot_for_hello() {
        let file = scratch_file("graph", "hello.bpmn", HELLO_BPMN);
        let (code, out, _) = run(DispatchGraphArgs { bpmn_file: file }, Some("dot"));
        assert_eq!(code, crate::exit::OK);
        let expected = "digraph BpmnDispatch {\n\
            \x20 rankdir=LR;\n\
            \x20 node [fontname=Helvetica];\n\
            \x20 \"Start\" [label=\"Start\", shape=circle, style=filled, fillcolor=\"#cfe8cf\"];\n\
            \x20 \"Greet\" [label=\"Say Hello\", shape=box, style=filled, fillcolor=\"#cfd8e8\"];\n\
            \x20 \"End\" [label=\"End\", shape=circle, style=filled, fillcolor=\"#f4c7c7\"];\n\
            \x20 \"Start\" -> \"Greet\";\n\
            \x20 \"Greet\" -> \"End\";\n\
            }\n";
        assert_eq!(out, expected);
    }
}
