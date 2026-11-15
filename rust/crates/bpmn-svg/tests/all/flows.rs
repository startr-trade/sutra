//! F2 sequence-flow semantics: labels, default and conditional markers, and `show_ids`.

use bpmn_svg::{
    parse, render, FlowNode, GatewayType, Model, NodeKind, Options, Process, SequenceFlow, TaskType,
};

use super::support::{bpmn, expect_svg, group_tag};

const OLD_FIXTURES: [&str; 7] = [
    "tasks",
    "events",
    "gateways",
    "sub-processes",
    "boundary-events",
    "several-processes",
    "messages",
];

fn two_tasks(flow: SequenceFlow) -> Model {
    Model::new().process(
        Process::new("p")
            .node(FlowNode::new("a", NodeKind::Task(TaskType::User)))
            .node(FlowNode::new("b", NodeKind::Task(TaskType::User)))
            .flow(flow),
    )
}

fn from_gateway(flow: SequenceFlow) -> Model {
    Model::new().process(
        Process::new("p")
            .node(FlowNode::new(
                "a",
                NodeKind::Gateway(GatewayType::Exclusive),
            ))
            .node(FlowNode::new("b", NodeKind::Task(TaskType::User)))
            .flow(flow),
    )
}

fn ided() -> Options {
    Options::new().id_prefix("")
}

#[test]
fn a_label_replaces_the_id_caption() {
    let model = two_tasks(SequenceFlow::new("f1", "a", "b").label("Approved"));
    for options in [Options::default(), Options::new().show_ids(false)] {
        let svg = render(&model, &options).unwrap();
        assert!(svg.contains(">Approved</text>"), "{svg}");
        assert!(!svg.contains("(f1)"));
    }
    // A blank label falls back to the id.
    let blank = two_tasks(SequenceFlow::new("f1", "a", "b").label("  "));
    assert!(render(&blank, &Options::default())
        .unwrap()
        .contains(">(f1)</text>"));
}

#[test]
fn default_and_conditional_markers() {
    let model = parse(&bpmn("flow-markers")).unwrap();
    let flows = &model.processes[0].flows;
    let flow = |id: &str| flows.iter().find(|f| f.id == id).unwrap();
    assert!(flow("f-default").is_default && !flow("f-default").conditional);
    assert!(flow("g-default").is_default);
    assert!(flow("f-cond").conditional && !flow("f-cond").is_default);
    assert_eq!(flow("f-cond").label.as_deref(), Some("urgent"));
    assert_eq!(
        flow("g-cond").label.as_deref(),
        Some("Large"),
        "a name wins"
    );
    assert_eq!(
        flow("g-long").label,
        None,
        "a long condition is not a caption"
    );
    assert!(!flow("f1").is_default && !flow("f1").conditional);

    let svg = render(&model, &ided()).unwrap();
    assert_eq!(
        group_tag(&svg, "f-default"),
        "<g class=\"bpmn-flow bpmn-flow-default\" id=\"f-default\">"
    );
    assert_eq!(
        group_tag(&svg, "f-cond"),
        "<g class=\"bpmn-flow bpmn-flow-conditional\" id=\"f-cond\">"
    );
    let group = |id: &str| {
        let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
        &svg[at..at + svg[at..].find("</g>").unwrap()]
    };
    assert!(
        group("f-cond").contains("<polygon "),
        "a diamond leaving a task"
    );
    assert!(
        !group("g-cond").contains("<polygon "),
        "none leaving a gateway"
    );
    assert_eq!(
        group("f-default").matches("<path ").count(),
        2,
        "line and slash"
    );
    assert_eq!(group("g-default").matches("<path ").count(), 2);
    assert_eq!(group("f1").matches("<path ").count(), 1);
    assert!(svg.contains(">urgent</text>") && svg.contains(">Large</text>"));
    assert!(svg.contains(">(g-long)</text>"));
    expect_svg(
        "flow-markers",
        &render(&model, &Options::default()).unwrap(),
    );
}

#[test]
fn default_wins_over_conditional() {
    let model = two_tasks(
        SequenceFlow::new("f1", "a", "b")
            .default_flow()
            .conditional(),
    );
    let svg = render(&model, &ided()).unwrap();
    assert_eq!(
        group_tag(&svg, "f1"),
        "<g class=\"bpmn-flow bpmn-flow-default\" id=\"f1\">"
    );
    assert!(!svg.contains("<polygon "));
    // The slash: 10 units long at 45°, centred 8 units from the source on a rightward line.
    assert!(
        svg.contains("<path d=\"M124.46,35.54 L131.54,28.46\""),
        "{svg}"
    );
}

#[test]
fn no_condition_diamond_leaving_a_gateway() {
    let svg = render(
        &from_gateway(SequenceFlow::new("f1", "a", "b").conditional()),
        &ided(),
    )
    .unwrap();
    assert_eq!(
        group_tag(&svg, "f1"),
        "<g class=\"bpmn-flow bpmn-flow-conditional\" id=\"f1\">"
    );
    assert!(!svg.contains("<polygon points=\"50,"), "{svg}");
    let from_task = render(
        &two_tasks(SequenceFlow::new("f1", "a", "b").conditional()),
        &ided(),
    )
    .unwrap();
    // 12 long and 8 wide, the near tip on the task's right edge.
    assert!(
        from_task.contains("<polygon points=\"120,32 126,36 132,32 126,28\" fill=\"#fff\""),
        "{from_task}"
    );
}

/// Elements in a document: every start tag.
fn elements(svg: &str) -> usize {
    svg.matches('<').count() - svg.matches("</").count()
}

#[test]
fn show_ids_false_removes_every_id_caption_and_nothing_else() {
    for name in OLD_FIXTURES {
        let model = parse(&bpmn(name)).unwrap();
        let shown = render(&model, &Options::default()).unwrap();
        let hidden = render(&model, &Options::new().show_ids(false)).unwrap();
        let ids = model.element_ids("");
        for id in ids.keys() {
            assert!(!hidden.contains(&format!(">({id})<")), "{name}: ({id})");
        }
        let node_id_texts = ids
            .keys()
            .filter(|id| shown.contains(&format!("fill=\"#888\">({id})</text>")))
            .count();
        let edge_captions = shown.matches("<rect ").count() - hidden.matches("<rect ").count();
        // An edge caption is a group, a background rect and a text.
        assert_eq!(
            elements(&hidden),
            elements(&shown) - (node_id_texts - edge_captions) - 3 * edge_captions,
            "{name}"
        );
        assert_eq!(
            hidden.matches("<g class=\"bpmn-node").count(),
            shown.matches("<g class=\"bpmn-node").count()
        );
        assert!(!hidden.contains("fill=\"#888\">("), "{name}");
    }
}
