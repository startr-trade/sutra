//! `parse`: standard BPMN into the neutral model, with everything else ignored.

use bpmn_svg::{
    parse, Error, EventGlyph, GatewayType, LoopMarker, NodeKind, Options, SubProcessKind, TaskType,
};

const BPMN: &str = "http://www.omg.org/spec/BPMN/20100524/MODEL";

fn document(body: &str) -> String {
    format!("<definitions xmlns=\"{BPMN}\">{body}</definitions>")
}

#[test]
fn reads_a_file_with_no_bpmn_di() {
    let model = parse(&document(
        "<process id=\"p\"><startEvent id=\"s\" name=\"Go\"/><endEvent id=\"e\"/>\
         <sequenceFlow id=\"f\" sourceRef=\"s\" targetRef=\"e\"/></process>",
    ))
    .unwrap();
    let process = &model.processes[0];
    assert_eq!(process.id, "p");
    assert_eq!(process.nodes[0].id, "s");
    assert_eq!(process.nodes[0].label.as_deref(), Some("Go"));
    assert_eq!(
        process.nodes[0].kind,
        NodeKind::StartEvent(EventGlyph::Message)
    );
    assert_eq!(process.nodes[1].label, None);
    assert_eq!(process.nodes[1].kind, NodeKind::EndEvent(None));
    assert_eq!(process.flows[0].id, "f");
    assert_eq!(process.flows[0].source_ref, "s");
    assert_eq!(process.flows[0].target_ref, "e");
}

#[test]
fn reads_a_prefixed_bpmn_namespace() {
    let model = parse(&format!(
        "<bpmn:definitions xmlns:bpmn=\"{BPMN}\"><bpmn:process id=\"p\">\
         <bpmn:userTask id=\"t\"/></bpmn:process></bpmn:definitions>"
    ))
    .unwrap();
    assert_eq!(
        model.processes[0].nodes[0].kind,
        NodeKind::Task(TaskType::User)
    );
}

#[test]
fn ignores_vendor_extensions_and_bpmn_di() {
    let plain = document(
        "<process id=\"p\"><startEvent id=\"s\"/><serviceTask id=\"t\" name=\"Work\"/>\
         <sequenceFlow id=\"f\" sourceRef=\"s\" targetRef=\"t\"/></process>",
    );
    let decorated = format!(
        "<definitions xmlns=\"{BPMN}\" xmlns:v=\"urn:example:vendor\" \
           xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" \
           xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" exporter=\"Some Tool\">\
         <v:settings retries=\"3\"/>\
         <process id=\"p\" v:owner=\"someone\" unknownAttribute=\"1\">\
           <documentation>Anything &amp; nothing</documentation>\
           <laneSet id=\"lanes\"><lane id=\"lane\"/></laneSet>\
           <v:startEvent id=\"not-a-bpmn-node\"/>\
           <startEvent id=\"s\"><extensionElements><v:trigger/></extensionElements></startEvent>\
           <serviceTask id=\"t\" name=\"Work\" v:timeout=\"PT1S\" implementation=\"anything\">\
             <extensionElements><v:serviceTask id=\"nested\"/></extensionElements>\
             <ioSpecification/></serviceTask>\
           <sequenceFlow id=\"f\" sourceRef=\"s\" targetRef=\"t\">\
             <conditionExpression>x &gt; 1</conditionExpression></sequenceFlow>\
           <dataObject id=\"data\"/><textAnnotation id=\"note\"/>\
         </process>\
         <bpmndi:BPMNDiagram id=\"d\"><bpmndi:BPMNPlane id=\"plane\" bpmnElement=\"p\">\
           <bpmndi:BPMNShape id=\"shape\" bpmnElement=\"s\"><dc:Bounds x=\"900\" y=\"900\" \
             width=\"1\" height=\"1\"/></bpmndi:BPMNShape>\
         </bpmndi:BPMNPlane></bpmndi:BPMNDiagram></definitions>"
    );
    let plain = parse(&plain).unwrap();
    let decorated = parse(&decorated).unwrap();
    assert_eq!(decorated, plain, "the model holds only what a diagram uses");
    assert_eq!(
        bpmn_svg::render(&decorated, &Options::default()),
        bpmn_svg::render(&plain, &Options::default()),
        "and the coordinates in BPMN-DI do not move anything"
    );
}

#[test]
fn refuses_malformed_xml() {
    for bad in [
        "".to_string(),
        "not xml at all".to_string(),
        format!("<definitions xmlns=\"{BPMN}\"><process id=\"p\">"),
        format!("<definitions xmlns=\"{BPMN}\"><process></definitions>"),
        format!("<definitions xmlns=\"{BPMN}\" a=\"1\" a=\"2\"/>"),
        format!("<definitions xmlns=\"{BPMN}\"><process id=\"p\"/>&bogus;</definitions>"),
        format!("<definitions xmlns=\"{BPMN}\"/><definitions xmlns=\"{BPMN}\"/>"),
        format!("<!DOCTYPE definitions []><definitions xmlns=\"{BPMN}\"/>"),
    ] {
        match parse(&bad) {
            Err(Error::Malformed(detail)) => assert!(!detail.is_empty(), "{bad}"),
            other => panic!("{bad}: expected malformed, got {other:?}"),
        }
    }
    let message = parse("<a><b></a>").unwrap_err().to_string();
    assert!(message.starts_with("malformed XML: "), "{message}");
}

#[test]
fn refuses_a_document_that_is_not_bpmn() {
    assert!(matches!(parse("<html/>"), Err(Error::NotBpmn(_))));
    // The right name in the wrong namespace is not BPMN either.
    assert!(matches!(
        parse("<definitions xmlns=\"urn:other\"/>"),
        Err(Error::NotBpmn(_))
    ));
}

#[test]
fn refuses_what_cannot_be_drawn() {
    for bad in [
        "<process id=\"p\"><userTask name=\"No id\"/></process>",
        "<process id=\"p\"><userTask id=\"t\"/><endEvent id=\"t\"/></process>",
        "<process id=\"p\"><boundaryEvent id=\"b\"/></process>",
    ] {
        assert!(
            matches!(parse(&document(bad)), Err(Error::Invalid(_))),
            "{bad}"
        );
    }
}

#[test]
fn refuses_unbounded_nesting() {
    let depth = 100_000;
    let xml = format!(
        "<definitions xmlns=\"{BPMN}\">{}{}</definitions>",
        "<process id=\"p\">".repeat(depth),
        "</process>".repeat(depth)
    );
    assert!(matches!(parse(&xml), Err(Error::Invalid(_))));
}

#[test]
fn a_blank_label_is_no_label() {
    let model = parse(&document(
        "<process id=\"p\"><userTask id=\"t\" name=\"   \"/></process>",
    ))
    .unwrap();
    assert_eq!(model.processes[0].nodes[0].label, None);
}

#[test]
fn classifies_every_visual_type() {
    let model = parse(&document(
        "<process id=\"p\">\
           <startEvent id=\"a\"><timerEventDefinition/></startEvent>\
           <endEvent id=\"b\"><errorEventDefinition/></endEvent>\
           <endEvent id=\"c\"><terminateEventDefinition/></endEvent>\
           <endEvent id=\"d\"><cancelEventDefinition/></endEvent>\
           <intermediateThrowEvent id=\"e\"><signalEventDefinition/></intermediateThrowEvent>\
           <implicitThrowEvent id=\"f\"><linkEventDefinition/></implicitThrowEvent>\
           <intermediateCatchEvent id=\"g\"><timerEventDefinition/></intermediateCatchEvent>\
           <intermediateCatchEvent id=\"h\"><linkEventDefinition/></intermediateCatchEvent>\
           <intermediateCatchEvent id=\"i\"><messageEventDefinition/></intermediateCatchEvent>\
           <boundaryEvent id=\"j\" attachedToRef=\"v:k\"><timerEventDefinition/></boundaryEvent>\
           <task id=\"k\"/>\
           <callActivity id=\"l\"/>\
           <eventBasedGateway id=\"m\"/>\
           <complexGateway id=\"n\"/>\
           <transaction id=\"o\"/>\
           <adHocSubProcess id=\"q\"/>\
           <subProcess id=\"r\" triggeredByEvent=\"true\"/>\
           <subProcess id=\"s\"><userTask id=\"inner\"/></subProcess>\
           <manualTask id=\"t\"><standardLoopCharacteristics/>\
             <multiInstanceLoopCharacteristics/></manualTask>\
           <manualTask id=\"u\"><multiInstanceLoopCharacteristics isSequential=\"true\"/></manualTask>\
           <manualTask id=\"w\"><multiInstanceLoopCharacteristics/></manualTask>\
           <exclusiveGateway id=\"x\"/>\
         </process>",
    ))
    .unwrap();
    let nodes = &model.processes[0].nodes;
    let kind = |id: &str| &nodes.iter().find(|n| n.id == id).unwrap().kind;
    let markers = |id: &str| &nodes.iter().find(|n| n.id == id).unwrap().markers;
    assert_eq!(kind("a"), &NodeKind::StartEvent(EventGlyph::Timer));
    assert_eq!(kind("b"), &NodeKind::EndEvent(Some(EventGlyph::Error)));
    assert_eq!(kind("c"), &NodeKind::EndEvent(Some(EventGlyph::Terminate)));
    assert_eq!(kind("d"), &NodeKind::EndEvent(Some(EventGlyph::Cancel)));
    assert_eq!(
        kind("e"),
        &NodeKind::IntermediateThrowEvent(EventGlyph::Signal)
    );
    assert_eq!(
        kind("f"),
        &NodeKind::IntermediateThrowEvent(EventGlyph::Link)
    );
    assert_eq!(
        kind("g"),
        &NodeKind::IntermediateCatchEvent(EventGlyph::Timer)
    );
    assert_eq!(
        kind("h"),
        &NodeKind::IntermediateCatchEvent(EventGlyph::Link)
    );
    assert_eq!(
        kind("i"),
        &NodeKind::IntermediateCatchEvent(EventGlyph::Message)
    );
    assert_eq!(
        kind("j"),
        &NodeKind::BoundaryEvent {
            attached_to_ref: "k".to_string(),
            glyph: EventGlyph::Timer
        },
        "an attachedToRef QName resolves to its local part"
    );
    assert_eq!(kind("k"), &NodeKind::Task(TaskType::Manual));
    assert_eq!(kind("l"), &NodeKind::CallActivity);
    assert_eq!(kind("m"), &NodeKind::Gateway(GatewayType::EventBased));
    assert_eq!(kind("n"), &NodeKind::Gateway(GatewayType::Complex));
    assert_eq!(kind("x"), &NodeKind::Gateway(GatewayType::Exclusive));
    let sub = |id: &str| match kind(id) {
        NodeKind::SubProcess { kind, inner } => (*kind, inner.nodes.len()),
        other => panic!("{id}: {other:?}"),
    };
    assert_eq!(sub("o"), (SubProcessKind::Transaction, 0));
    assert_eq!(sub("q"), (SubProcessKind::AdHoc, 0));
    assert_eq!(sub("r"), (SubProcessKind::Event, 0));
    assert_eq!(sub("s"), (SubProcessKind::Embedded, 1));
    assert_eq!(markers("t"), &[LoopMarker::Loop]);
    assert_eq!(markers("u"), &[LoopMarker::SequentialMultiInstance]);
    assert_eq!(markers("w"), &[LoopMarker::ParallelMultiInstance]);
}

#[test]
fn an_empty_process_has_no_diagram() {
    let model = parse(&document("<process id=\"p\"/>")).unwrap();
    assert_eq!(model.processes.len(), 1);
    assert_eq!(bpmn_svg::render(&model, &Options::default()), None);
    let no_process = parse(&document("")).unwrap();
    assert_eq!(bpmn_svg::render(&no_process, &Options::default()), None);
}

#[test]
fn a_dangling_reference_is_not_drawn_and_does_not_panic() {
    let model = parse(&document(
        "<process id=\"p\"><userTask id=\"t\"/>\
         <boundaryEvent id=\"b\" attachedToRef=\"missing\"/>\
         <sequenceFlow id=\"f1\" sourceRef=\"t\" targetRef=\"missing\"/>\
         <sequenceFlow id=\"f2\" sourceRef=\"missing\" targetRef=\"t\"/>\
         <sequenceFlow id=\"f3\" sourceRef=\"b\" targetRef=\"t\"/>\
         <sequenceFlow id=\"f4\" sourceRef=\"t\" targetRef=\"b\"/>\
         <sequenceFlow sourceRef=\"t\" targetRef=\"t\"/>\
         </process>",
    ))
    .unwrap();
    let svg = bpmn_svg::render(&model, &Options::default()).unwrap();
    assert!(svg.contains("(t)"));
    assert!(
        !svg.contains("(b)"),
        "a hostless boundary event is not drawn"
    );
    assert!(!svg.contains("(f1)"));
}
