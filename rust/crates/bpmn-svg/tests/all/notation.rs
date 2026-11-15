//! F6 notation: plain start events and tasks, the event-based gateway, the event sub-process and
//! the conditional, multiple and parallel-multiple glyphs.

use bpmn_svg::{
    parse, render, EventGlyph, FlowNode, GatewayType, Model, NodeKind, Options, Process,
    SequenceFlow, SubProcessKind, TaskType,
};

use super::support::expect_svg;

/// The content of the group whose id is `id`, up to its closing tag.
fn group(svg: &str, id: &str) -> String {
    let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
    let body = &svg[at..];
    body[body.find('>').unwrap() + 1..body.find("</g>").unwrap()].to_string()
}

/// The element names in a fragment, in order.
fn tags(fragment: &str) -> Vec<String> {
    fragment
        .split('<')
        .skip(1)
        .filter(|t| !t.starts_with('/'))
        .map(|t| t.split([' ', '>', '/']).next().unwrap().to_string())
        .collect()
}

fn ided() -> Options {
    Options::new().id_prefix("")
}

#[test]
fn plain_start_and_plain_task_draw_plain() {
    let xml = "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\">\
        <process id=\"p\"><startEvent id=\"s\"/><task id=\"t\" name=\"Do\"/>\
        <sequenceFlow id=\"f\" sourceRef=\"s\" targetRef=\"t\"/></process></definitions>";
    let svg = render(&parse(xml).unwrap(), &ided()).unwrap();
    assert_eq!(
        tags(&group(&svg, "s")),
        ["circle", "text"],
        "a bare circle and its id"
    );
    assert_eq!(
        tags(&group(&svg, "t")),
        ["rect", "text", "text"],
        "no corner glyph"
    );
}

#[test]
fn event_based_gateway_glyph() {
    let model = Model::new().process(Process::new("p").node(FlowNode::new(
        "g",
        NodeKind::Gateway(GatewayType::EventBased),
    )));
    let g = group(&render(&model, &ided()).unwrap(), "g");
    assert_eq!(tags(&g), ["polygon", "circle", "circle", "polygon", "text"]);
    assert!(g.contains("r=\"14\"") && g.contains("r=\"11\""));
    // An open pentagon of radius 7, one vertex straight up from the centre (25, 25).
    assert!(g.contains(
        "<polygon points=\"25,18 31.66,22.84 29.11,30.66 20.89,30.66 18.34,22.84\" fill=\"none\""
    ));
}

#[test]
fn event_subprocess_is_dotted() {
    let sub = |kind| {
        Model::new().process(Process::new("p").node(FlowNode::new(
            "sub",
            NodeKind::SubProcess {
                kind,
                inner: Process::new("in").node(FlowNode::new("x", NodeKind::Task(TaskType::User))),
            },
        )))
    };
    let dotted = group(
        &render(&sub(SubProcessKind::Event), &ided()).unwrap(),
        "sub",
    );
    assert!(dotted.starts_with("<rect ") && dotted.contains("stroke-dasharray=\"2 3\""));
    let solid = group(
        &render(&sub(SubProcessKind::Embedded), &ided()).unwrap(),
        "sub",
    );
    assert!(!solid.contains("stroke-dasharray"));
}

#[test]
fn new_event_glyphs() {
    let nodes = [
        (
            "conditional-start",
            NodeKind::StartEvent(Some(EventGlyph::Conditional)),
        ),
        (
            "multiple-start",
            NodeKind::StartEvent(Some(EventGlyph::Multiple)),
        ),
        (
            "parallel-start",
            NodeKind::StartEvent(Some(EventGlyph::ParallelMultiple)),
        ),
        (
            "conditional-catch",
            NodeKind::IntermediateCatchEvent(Some(EventGlyph::Conditional)),
        ),
        (
            "multiple-catch",
            NodeKind::IntermediateCatchEvent(Some(EventGlyph::Multiple)),
        ),
        (
            "multiple-throw",
            NodeKind::IntermediateThrowEvent(Some(EventGlyph::Multiple)),
        ),
        ("plain-throw", NodeKind::IntermediateThrowEvent(None)),
        ("plain-catch", NodeKind::IntermediateCatchEvent(None)),
        (
            "multiple-end",
            NodeKind::EndEvent(Some(EventGlyph::Multiple)),
        ),
    ];
    let mut p = Process::new("notation");
    for (id, kind) in nodes.iter().cloned() {
        p = p.node(FlowNode::new(id, kind));
    }
    for pair in nodes.windows(2) {
        p = p.flow(SequenceFlow::new(
            format!("{}-{}", pair[0].0, pair[1].0),
            pair[0].0,
            pair[1].0,
        ));
    }
    let model = Model::new().process(p);
    let svg = render(&model, &ided()).unwrap();
    assert_eq!(
        tags(&group(&svg, "conditional-start")),
        ["circle", "rect", "path", "text"]
    );
    assert_eq!(
        group(&svg, "conditional-start").matches(" H").count(),
        4,
        "four rules"
    );
    let multiple_catch = group(&svg, "multiple-catch");
    assert!(
        multiple_catch.contains("<polygon ") && multiple_catch.contains("fill=\"none\" stroke")
    );
    assert!(
        group(&svg, "multiple-throw").contains("fill=\"#333\" stroke"),
        "filled when thrown"
    );
    assert_eq!(
        group(&svg, "parallel-start")
            .split("<polygon points=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .split(' ')
            .count(),
        12,
        "an open plus"
    );
    assert_eq!(
        tags(&group(&svg, "plain-throw")),
        ["circle", "circle", "text"]
    );
    assert_eq!(
        tags(&group(&svg, "plain-catch")),
        ["circle", "circle", "text"]
    );
    expect_svg("notation", &render(&model, &Options::default()).unwrap());
}
