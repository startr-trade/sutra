//! `render`: exact output on the crate's own fixtures, determinism and the theming option.
//!
//! Each `tests/fixtures/<name>.svg` is the exact expected document. The diagrams change only on
//! purpose: after one, re-bless with `BLESS=1 cargo test -p bpmn-svg --test all rendering` and read
//! the diff.

use std::fs;
use std::path::PathBuf;

use bpmn_svg::{parse, render, Options};

const FIXTURES: [&str; 7] = [
    "tasks",
    "events",
    "gateways",
    "sub-processes",
    "boundary-events",
    "several-processes",
    "messages",
];

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn fixture(name: &str) -> String {
    fs::read_to_string(fixtures_dir().join(format!("{name}.bpmn"))).unwrap()
}

fn rendered(name: &str, options: &Options) -> String {
    let model = parse(&fixture(name)).unwrap();
    render(&model, options).unwrap()
}

fn themed() -> Options {
    let mut options = Options::default();
    options.css_variables = true;
    options
}

#[test]
fn every_fixture_renders_exactly_as_expected() {
    for name in FIXTURES {
        let svg = rendered(name, &Options::default());
        let path = fixtures_dir().join(format!("{name}.svg"));
        if std::env::var_os("BLESS").is_some() {
            fs::write(&path, &svg).unwrap();
        }
        let expected = fs::read_to_string(&path).unwrap();
        assert!(svg == expected, "{name}: output differs from {path:?}");
    }
}

#[test]
fn the_fixtures_cover_every_shape() {
    let all: String = FIXTURES.map(fixture).concat();
    for element in [
        "startEvent",
        "endEvent",
        "intermediateThrowEvent",
        "intermediateCatchEvent",
        "boundaryEvent",
        "serviceTask",
        "scriptTask",
        "sendTask",
        "userTask",
        "businessRuleTask",
        "manualTask",
        "<task",
        "receiveTask",
        "callActivity",
        "subProcess",
        "<transaction",
        "adHocSubProcess",
        "triggeredByEvent",
        "exclusiveGateway",
        "eventBasedGateway",
        "inclusiveGateway",
        "parallelGateway",
        "complexGateway",
        "standardLoopCharacteristics",
        "multiInstanceLoopCharacteristics",
        "terminateEventDefinition",
        "errorEventDefinition",
        "cancelEventDefinition",
        "timerEventDefinition",
        "messageEventDefinition",
        "signalEventDefinition",
        "escalationEventDefinition",
        "compensateEventDefinition",
        "linkEventDefinition",
    ] {
        assert!(all.contains(element), "no fixture has {element}");
    }
}

#[test]
fn rendering_twice_gives_identical_bytes() {
    for name in FIXTURES {
        let model = parse(&fixture(name)).unwrap();
        let first = render(&model, &Options::default()).unwrap();
        for _ in 0..20 {
            // A fresh parse too: each `HashMap` gets its own random seed, so an iteration order
            // that reached the output would show here.
            let again = parse(&fixture(name)).unwrap();
            assert!(
                render(&again, &Options::default()).unwrap() == first,
                "{name}"
            );
            assert!(
                render(&model, &Options::default()).unwrap() == first,
                "{name}"
            );
        }
    }
}

#[test]
fn the_default_output_uses_no_css_variables() {
    for name in FIXTURES {
        let svg = rendered(name, &Options::default());
        assert!(!svg.contains("var("), "{name}");
    }
}

#[test]
fn the_option_emits_css_variables_for_every_colour() {
    for name in FIXTURES {
        let plain = rendered(name, &Options::default());
        let svg = rendered(name, &themed());
        assert!(svg.contains("var(--bpmn-stroke, #333)"), "{name}");
        assert!(svg.contains("var(--bpmn-muted, #888)"), "{name}");
        assert!(
            svg.contains("var(--bpmn-label-background, #ffffffe6)"),
            "{name}"
        );
        // Nothing but the colours changed, and no colour literal was missed.
        let mut unthemed = svg.clone();
        for (variable, literal) in [
            ("var(--bpmn-stroke, #333)", "#333"),
            ("var(--bpmn-text, #222)", "#222"),
            ("var(--bpmn-muted, #888)", "#888"),
            ("var(--bpmn-surface, #fff)", "#fff"),
            ("var(--bpmn-label-background, #ffffffe6)", "#ffffffe6"),
        ] {
            unthemed = unthemed.replace(variable, literal);
        }
        assert!(unthemed == plain, "{name}");
        let bare_colours = svg
            .match_indices("=\"#")
            .map(|(at, _)| &svg[at..at + 12])
            .collect::<Vec<_>>();
        assert!(bare_colours.is_empty(), "{name}: {bare_colours:?}");
    }
}

#[test]
fn the_option_leaves_label_text_as_written() {
    let xml = "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\">\
        <process id=\"p\"><userTask id=\"t\" name=\"fill=&quot;#333&quot;\"/></process></definitions>";
    let svg = render(&parse(xml).unwrap(), &themed()).unwrap();
    assert!(svg.contains(">fill=\"#333\"<"), "{svg}");
}
