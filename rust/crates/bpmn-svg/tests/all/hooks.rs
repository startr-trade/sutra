//! F1 element hooks: group classes, element ids, and caller attributes.

use std::collections::BTreeSet;

use bpmn_svg::{
    element_id, parse, render, EventGlyph, FlowNode, GatewayType, Model, NodeAttrs, NodeKind,
    Options, Process, SequenceFlow, SubProcessKind, TaskType, Warning,
};

use super::support::{bpmn, expect_svg, group_ids, group_tag};

fn one_of_each() -> Model {
    let mut p = Process::new("p");
    let kinds = [
        ("start", NodeKind::StartEvent(None)),
        ("end", NodeKind::EndEvent(None)),
        (
            "throw",
            NodeKind::IntermediateThrowEvent(Some(EventGlyph::Signal)),
        ),
        (
            "catch",
            NodeKind::IntermediateCatchEvent(Some(EventGlyph::Timer)),
        ),
        ("t-plain", NodeKind::Task(TaskType::Plain)),
        ("t-service", NodeKind::Task(TaskType::Service)),
        ("t-script", NodeKind::Task(TaskType::Script)),
        ("t-send", NodeKind::Task(TaskType::Send)),
        ("t-user", NodeKind::Task(TaskType::User)),
        ("t-rule", NodeKind::Task(TaskType::BusinessRule)),
        ("t-manual", NodeKind::Task(TaskType::Manual)),
        ("t-receive", NodeKind::Task(TaskType::Receive)),
        ("call", NodeKind::CallActivity),
        ("g-xor", NodeKind::Gateway(GatewayType::Exclusive)),
        ("g-event", NodeKind::Gateway(GatewayType::EventBased)),
        ("g-or", NodeKind::Gateway(GatewayType::Inclusive)),
        ("g-and", NodeKind::Gateway(GatewayType::Parallel)),
        ("g-complex", NodeKind::Gateway(GatewayType::Complex)),
    ];
    for (id, kind) in kinds {
        p = p.node(FlowNode::new(id, kind));
    }
    for (id, kind) in [
        ("s-embedded", SubProcessKind::Embedded),
        ("s-transaction", SubProcessKind::Transaction),
        ("s-ad-hoc", SubProcessKind::AdHoc),
        ("s-event", SubProcessKind::Event),
    ] {
        p = p.node(FlowNode::new(
            id,
            NodeKind::SubProcess {
                kind,
                inner: Process::new(format!("{id}-inner")).node(FlowNode::new(
                    format!("{id}-task"),
                    NodeKind::Task(TaskType::User),
                )),
            },
        ));
    }
    p = p.node(FlowNode::new(
        "boundary",
        NodeKind::BoundaryEvent {
            attached_to_ref: "t-user".into(),
            glyph: EventGlyph::Error,
        },
    ));
    Model::new().process(p)
}

#[test]
fn every_kind_has_its_class() {
    let svg = render(&one_of_each(), &Options::new().id_prefix("")).unwrap();
    for (id, class) in [
        ("start", "bpmn-node bpmn-event bpmn-event-start"),
        ("end", "bpmn-node bpmn-event bpmn-event-end"),
        (
            "throw",
            "bpmn-node bpmn-event bpmn-event-intermediate-throw",
        ),
        (
            "catch",
            "bpmn-node bpmn-event bpmn-event-intermediate-catch",
        ),
        ("boundary", "bpmn-node bpmn-event bpmn-event-boundary"),
        ("t-plain", "bpmn-node bpmn-task bpmn-task-plain"),
        ("t-service", "bpmn-node bpmn-task bpmn-task-service"),
        ("t-script", "bpmn-node bpmn-task bpmn-task-script"),
        ("t-send", "bpmn-node bpmn-task bpmn-task-send"),
        ("t-user", "bpmn-node bpmn-task bpmn-task-user"),
        ("t-rule", "bpmn-node bpmn-task bpmn-task-business-rule"),
        ("t-manual", "bpmn-node bpmn-task bpmn-task-manual"),
        ("t-receive", "bpmn-node bpmn-task bpmn-task-receive"),
        ("call", "bpmn-node bpmn-call-activity"),
        (
            "s-embedded",
            "bpmn-node bpmn-subprocess bpmn-subprocess-embedded",
        ),
        (
            "s-transaction",
            "bpmn-node bpmn-subprocess bpmn-subprocess-transaction",
        ),
        (
            "s-ad-hoc",
            "bpmn-node bpmn-subprocess bpmn-subprocess-ad-hoc",
        ),
        ("s-event", "bpmn-node bpmn-subprocess bpmn-subprocess-event"),
        ("g-xor", "bpmn-node bpmn-gateway bpmn-gateway-exclusive"),
        ("g-event", "bpmn-node bpmn-gateway bpmn-gateway-event-based"),
        ("g-or", "bpmn-node bpmn-gateway bpmn-gateway-inclusive"),
        ("g-and", "bpmn-node bpmn-gateway bpmn-gateway-parallel"),
        ("g-complex", "bpmn-node bpmn-gateway bpmn-gateway-complex"),
    ] {
        assert_eq!(
            group_tag(&svg, id),
            format!("<g class=\"{class}\" id=\"{id}\">"),
            "{id}"
        );
    }
}

fn colliding() -> Model {
    Model::new().process(
        Process::new("p")
            .node(FlowNode::new("a.b", NodeKind::Task(TaskType::User)))
            .node(FlowNode::new("a_b", NodeKind::Task(TaskType::User)))
            .flow(SequenceFlow::new("f 1", "a.b", "a_b")),
    )
}

#[test]
fn ids_are_prefixed_sanitised_and_deduplicated() {
    let svg = render(&colliding(), &Options::new().id_prefix("p-")).unwrap();
    assert_eq!(group_ids(&svg), ["p-f_1", "p-a_b", "p-a_b-2"]);
    assert!(group_tag(&svg, "p-a_b-2").contains("bpmn-task-user"));
}

#[test]
fn no_prefix_means_no_id_attributes() {
    let svg = render(&colliding(), &Options::default()).unwrap();
    assert!(group_ids(&svg).is_empty());
    assert!(svg.contains("<marker id=\"arrow\""));
    assert!(svg.contains("marker-end=\"url(#arrow)\""));
}

#[test]
fn marker_ids_follow_the_prefix() {
    let svg = render(&colliding(), &Options::new().id_prefix("d2-")).unwrap();
    assert!(svg.contains("<marker id=\"d2-arrow\""));
    assert!(svg.contains("marker-end=\"url(#d2-arrow)\""));
    assert!(!svg.contains("url(#arrow)"));
}

fn hooked() -> Options {
    Options::new()
        .id_prefix("bpmn-")
        .node_attrs(
            "review",
            NodeAttrs::new()
                .href("/items/review")
                .title("Review the request")
                .class("is-current")
                .class("hot")
                .data("status", "open")
                .data("count", "3"),
        )
        .flow_attrs(
            "f1",
            NodeAttrs::new().title("First step").data("weight", "1"),
        )
}

fn small() -> Model {
    Model::new().process(
        Process::new("p")
            .node(FlowNode::new("start", NodeKind::StartEvent(None)))
            .node(FlowNode::new("review", NodeKind::Task(TaskType::User)).label("Review"))
            .node(FlowNode::new("end", NodeKind::EndEvent(None)))
            .flow(SequenceFlow::new("f1", "start", "review"))
            .flow(SequenceFlow::new("f2", "review", "end")),
    )
}

#[test]
fn href_title_classes_and_data_are_emitted() {
    let svg = render(&small(), &hooked()).unwrap();
    assert!(svg.contains(
        "<g class=\"bpmn-node bpmn-task bpmn-task-user is-current hot\" id=\"bpmn-review\" \
         data-count=\"3\" data-status=\"open\"><title>Review the request</title>\
         <a href=\"/items/review\"><rect "
    ));
    assert!(svg.contains(
        "<g class=\"bpmn-flow\" id=\"bpmn-f1\" data-weight=\"1\"><title>First step</title><path "
    ));
    // Only the path group carries the id and attributes; the caption group has the classes alone.
    assert_eq!(svg.matches("id=\"bpmn-f1\"").count(), 1);
    assert!(svg.contains("<g class=\"bpmn-flow\"><rect "));
    expect_svg("hooks", &svg);
}

#[test]
fn attribute_values_are_escaped() {
    let nasty = "a<b & \"c\" 'd' #333";
    let model = Model::new().process(
        Process::new("p").node(FlowNode::new("t", NodeKind::Task(TaskType::User)).label(nasty)),
    );
    let options = Options::new().node_attrs(
        "t",
        NodeAttrs::new().href(nasty).title(nasty).data("v", nasty),
    );
    let escaped = "a&lt;b &amp; &quot;c&quot; &#39;d&#39; &#35;333";
    for themed in [false, true] {
        let svg = render(&model, &options.clone().css_variables(themed)).unwrap();
        assert!(svg.contains(&format!("data-v=\"{escaped}\"")), "{svg}");
        assert!(svg.contains(&format!("<a href=\"{escaped}\">")), "{svg}");
        assert!(
            svg.contains("<title>a&lt;b &amp; \"c\" 'd' #333</title>"),
            "{svg}"
        );
        assert!(svg.contains(">a&lt;b &amp; \"c\" 'd' #333</text>"), "{svg}");
        assert!(!svg.contains("var(--bpmn-stroke, #333)333"));
    }
}

#[test]
fn invalid_data_keys_and_classes_are_dropped_and_reported() {
    let options = Options::new().node_attrs(
        "review",
        NodeAttrs::new()
            .class("ok")
            .class("not ok")
            .class("x\"y")
            .data("good", "1")
            .data("Bad", "2")
            .data("9lives", "3"),
    );
    let svg = render(&small(), &options).unwrap();
    assert!(svg.contains("<g class=\"bpmn-node bpmn-task bpmn-task-user ok\" data-good=\"1\">"));
    assert!(!svg.contains("not ok") && !svg.contains("data-Bad") && !svg.contains("data-9lives"));
    assert_eq!(
        options.validate(&small()),
        [
            Warning::InvalidClass {
                element_id: "review".into(),
                class: "not ok".into()
            },
            Warning::InvalidClass {
                element_id: "review".into(),
                class: "x\"y".into()
            },
            Warning::InvalidDataKey {
                element_id: "review".into(),
                key: "9lives".into()
            },
            Warning::InvalidDataKey {
                element_id: "review".into(),
                key: "Bad".into()
            },
        ]
    );
    let unknown = Options::new()
        .node_attrs("nobody", NodeAttrs::new())
        .flow_attrs("review", NodeAttrs::new());
    assert_eq!(
        unknown.validate(&small()),
        [
            Warning::UnknownAttrsTarget {
                id: "nobody".into()
            },
            Warning::UnknownAttrsTarget {
                id: "review".into()
            },
        ],
        "a node id is not a flow id"
    );
}

#[test]
fn validate_reports_unsafe_href_and_bad_data_key() {
    let options = Options::new()
        .node_attrs("review", NodeAttrs::new().href("/x?\"y\"").data("Key", "v"))
        .flow_attrs("f1", NodeAttrs::new().href("<script>"))
        .flow_attrs("f2", NodeAttrs::new().href("/fine?a=1&b=2#c"));
    let warnings = options.validate(&small());
    assert_eq!(
        warnings,
        [
            Warning::UnsafeHref {
                element_id: "review".into(),
                href: "/x?\"y\"".into()
            },
            Warning::InvalidDataKey {
                element_id: "review".into(),
                key: "Key".into()
            },
            Warning::UnsafeHref {
                element_id: "f1".into(),
                href: "<script>".into()
            },
        ]
    );
    for w in &warnings {
        let sentence = w.to_string();
        assert!(
            sentence.ends_with('.') && sentence.contains('\''),
            "{sentence}"
        );
    }
    // Rendering still escapes the value.
    let svg = render(&small(), &options).unwrap();
    assert!(svg.contains("<a href=\"/x?&quot;y&quot;\">"));
    assert!(svg.contains("<a href=\"&lt;script&gt;\">"));
}

#[test]
fn element_id_matches_the_rendered_ids() {
    for name in [
        "tasks",
        "events",
        "gateways",
        "sub-processes",
        "boundary-events",
        "several-processes",
        "messages",
    ] {
        let model = parse(&bpmn(name)).unwrap();
        let svg = render(&model, &Options::new().id_prefix("x-")).unwrap();
        let rendered: BTreeSet<String> = group_ids(&svg).into_iter().collect();
        let expected: BTreeSet<String> = model.element_ids("x-").into_values().collect();
        assert_eq!(rendered, expected, "{name}");
    }
    let ids = colliding().element_ids("p-");
    assert_eq!(ids["a.b"], "p-a_b");
    assert_eq!(ids["a_b"], "p-a_b-2");
    assert_eq!(ids["f 1"], "p-f_1");
}

#[test]
fn element_id_is_stable_for_a_collision_free_model() {
    let model = parse(&bpmn("sub-processes")).unwrap();
    for (id, element) in model.element_ids("bpmn-") {
        assert_eq!(element, element_id("bpmn-", &id));
    }
    assert_eq!(element_id("bpmn-", "Task_1.a"), "bpmn-Task_1_a");
}

#[test]
fn nested_nodes_get_ids_classes_and_attrs() {
    let model = Model::new().process(
        Process::new("p").node(FlowNode::new(
            "outer",
            NodeKind::SubProcess {
                kind: SubProcessKind::Embedded,
                inner: Process::new("outer-inner").node(FlowNode::new(
                    "middle",
                    NodeKind::SubProcess {
                        kind: SubProcessKind::Embedded,
                        inner: Process::new("middle-inner")
                            .node(FlowNode::new("deep-a", NodeKind::Task(TaskType::User)))
                            .node(FlowNode::new("deep-b", NodeKind::Task(TaskType::User)))
                            .flow(SequenceFlow::new("deep-f", "deep-a", "deep-b")),
                    },
                )),
            },
        )),
    );
    let options = Options::new()
        .id_prefix("n-")
        .node_attrs("deep-a", NodeAttrs::new().class("mine").data("k", "v"))
        .flow_attrs("deep-f", NodeAttrs::new().title("inner flow"));
    assert!(options.validate(&model).is_empty());
    let svg = render(&model, &options).unwrap();
    assert_eq!(
        group_tag(&svg, "n-deep-a"),
        "<g class=\"bpmn-node bpmn-task bpmn-task-user mine\" id=\"n-deep-a\" data-k=\"v\">"
    );
    assert_eq!(
        group_tag(&svg, "n-middle"),
        "<g class=\"bpmn-node bpmn-subprocess bpmn-subprocess-embedded\" id=\"n-middle\">"
    );
    assert!(svg.contains("<g class=\"bpmn-flow\" id=\"n-deep-f\"><title>inner flow</title>"));
}
