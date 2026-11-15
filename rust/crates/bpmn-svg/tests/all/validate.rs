//! F5(a) `Model::validate`: what does not draw, each named once.

use bpmn_svg::{parse, render, Options, Warning};

use super::support::bpmn;

#[test]
fn each_dangling_flow_is_named_once() {
    let xml = "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\">\
        <collaboration>\
          <participant id=\"pool\" processRef=\"p\"/>\
          <participant id=\"ghost\" processRef=\"nowhere\"/>\
          <messageFlow id=\"m-ok\" sourceRef=\"inner-t\" targetRef=\"pool\"/>\
          <messageFlow id=\"m-bad\" sourceRef=\"lost\" targetRef=\"also-lost\"/>\
          <messageFlow id=\"m-bad-target\" sourceRef=\"t\" targetRef=\"lost\"/>\
        </collaboration>\
        <process id=\"p\">\
          <userTask id=\"t\"/>\
          <subProcess id=\"sub\">\
            <userTask id=\"inner-t\"/>\
            <sequenceFlow id=\"inner-bad\" sourceRef=\"inner-t\" targetRef=\"t\"/>\
          </subProcess>\
          <sequenceFlow id=\"both-bad\" sourceRef=\"x\" targetRef=\"y\"/>\
          <sequenceFlow id=\"target-bad\" sourceRef=\"t\" targetRef=\"y\"/>\
          <sequenceFlow id=\"ok\" sourceRef=\"t\" targetRef=\"sub\"/>\
        </process>\
        </definitions>";
    let model = parse(xml).unwrap();
    let warnings = model.validate();
    assert_eq!(
        warnings,
        [
            Warning::DanglingFlow {
                flow_id: "both-bad".into(),
                missing_ref: "x".into()
            },
            Warning::DanglingFlow {
                flow_id: "target-bad".into(),
                missing_ref: "y".into()
            },
            // A nested flow is checked against its own scope: `t` is outside it.
            Warning::DanglingFlow {
                flow_id: "inner-bad".into(),
                missing_ref: "t".into()
            },
            Warning::DanglingMessageFlow {
                flow_id: "m-bad".into(),
                missing_ref: "lost".into()
            },
            Warning::DanglingMessageFlow {
                flow_id: "m-bad-target".into(),
                missing_ref: "lost".into()
            },
            Warning::UnknownProcessRef {
                participant_id: "ghost".into(),
                process_ref: "nowhere".into()
            },
        ]
    );
    for w in &warnings {
        let sentence = w.to_string();
        assert!(sentence.ends_with('.'), "{sentence}");
    }
    // Rendering still draws what it can: the black box, and the good message flow.
    let svg = render(&model, &Options::new().id_prefix("")).unwrap();
    assert!(svg.contains("<g class=\"bpmn-pool bpmn-pool-collapsed\" id=\"ghost\">"));
    assert!(svg.contains(" id=\"m-ok\""));
    assert!(!svg.contains(" id=\"m-bad\"") && !svg.contains(" id=\"both-bad\""));
}

#[test]
fn the_fixtures_have_nothing_dangling() {
    for name in [
        "tasks",
        "events",
        "gateways",
        "sub-processes",
        "boundary-events",
        "several-processes",
        "messages",
        "flow-markers",
        "collaboration",
    ] {
        assert_eq!(parse(&bpmn(name)).unwrap().validate(), [], "{name}");
    }
}
