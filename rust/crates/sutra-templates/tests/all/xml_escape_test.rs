//! The `xmlEscape` helper: the five XML specials become their predefined entities, non-Latin
//! text passes through unchanged, a missing value renders empty, and a template that escapes
//! relayed free text renders well-formed XML.

use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;
use quick_xml::{Reader, XmlVersion};
use serde_json::{json, Value};
use sutra_templates::HandlebarsTemplateEngine;

fn render(id: &str, tpl: &str, model: Value) -> String {
    HandlebarsTemplateEngine::new()
        .render(id, tpl.as_bytes(), &model)
        .expect("render succeeds")
}

#[test]
fn xml_escape_escapes_the_five_specials() {
    let out = render("xe-five", "{{xmlEscape v}}", json!({"v": "a&b<c>d\"e'f"}));
    assert_eq!(out, "a&amp;b&lt;c&gt;d&quot;e&apos;f");
    // Non-strings render via their string form.
    assert_eq!(render("xe-num", "{{xmlEscape n}}", json!({"n": 42})), "42");
    assert_eq!(
        render("xe-bool", "{{xmlEscape b}}", json!({"b": true})),
        "true"
    );
}

#[test]
fn xml_escape_leaves_non_latin_text_unchanged() {
    for text in ["नमस्ते दुनिया", "வணக்கம் உலகம்", "مرحبا بالعالم", "Zoë ✓ 😀"]
    {
        let out = render("xe-utf8", "{{xmlEscape v}}", json!({ "v": text }));
        assert_eq!(out, text);
    }
}

#[test]
fn xml_escape_of_missing_value_is_empty() {
    assert_eq!(
        render("xe-missing", "[{{xmlEscape absent}}]", json!({})),
        "[]"
    );
    assert_eq!(
        render("xe-null", "[{{xmlEscape v}}]", json!({"v": null})),
        "[]"
    );
}

#[test]
fn xml_escape_template_renders_well_formed_xml() {
    let tpl = "<party name=\"{{xmlEscape name}}\"><nm>{{xmlEscape name}}</nm></party>";
    let name = "Smith & Sons <Ltd> \"quoted\" O'Brien";
    let out = render("xe-xml", tpl, json!({ "name": name }));

    let mut reader = Reader::from_str(&out);
    let mut text = String::new();
    let mut attr = String::new();
    loop {
        match reader.read_event().expect("well-formed XML") {
            Event::Start(e) if e.name().as_ref() == b"party" => {
                let a = e
                    .try_get_attribute("name")
                    .expect("attribute parses")
                    .expect("name attribute present");
                attr = a
                    .normalized_value(XmlVersion::Implicit1_0)
                    .expect("attribute unescapes")
                    .into_owned();
            }
            Event::Text(t) => text.push_str(&t.decode().expect("text decodes")),
            Event::GeneralRef(r) => {
                let entity = r.decode().expect("entity name decodes");
                let resolved = resolve_predefined_entity(&entity)
                    .unwrap_or_else(|| panic!("undeclared entity &{entity};"));
                text.push_str(resolved);
            }
            Event::Eof => break,
            _ => {}
        }
    }
    assert_eq!(text, name);
    assert_eq!(attr, name);
}

#[test]
fn xml_escape_is_a_helper_not_a_model_root() {
    let a = HandlebarsTemplateEngine::new().analyze(b"<nm>{{xmlEscape payload.Nm}}</nm>");
    assert!(
        !a.roots.iter().any(|r| r == "xmlEscape"),
        "roots {:?}",
        a.roots
    );
    assert!(
        a.roots.iter().any(|r| r == "payload"),
        "roots {:?}",
        a.roots
    );
    assert!(a.payload_paths.iter().any(|p| p == "Nm"));
}
