//! Long and non-Latin labels. Widths are estimated from character counts with no font metrics, so
//! these pin the estimate, including the case it gets wrong, rather than a measured fit.

use bpmn_svg::{parse, render, Options};

fn task_width(label: &str) -> f64 {
    let xml = format!(
        "<definitions xmlns=\"http://www.omg.org/spec/BPMN/20100524/MODEL\">\
         <process id=\"p\"><userTask id=\"t\" name=\"{label}\"/></process></definitions>"
    );
    let svg = render(&parse(&xml).unwrap(), &Options::default()).unwrap();
    assert!(svg.contains(label), "the label is drawn verbatim");
    let rect = svg.split("<rect ").nth(1).unwrap();
    let width = rect.split("width=\"").nth(1).unwrap();
    width.split('"').next().unwrap().parse().unwrap()
}

#[test]
fn a_task_box_grows_with_the_character_count() {
    assert_eq!(task_width("Short"), 120.0, "the minimum width");
    // 6.2 units a character, plus 12 of padding each side.
    assert_eq!(task_width(&"x".repeat(100)), 644.0);
}

#[test]
fn non_latin_labels_are_sized_by_characters_not_by_glyph_width() {
    // 40 CJK characters get the width of 40 Latin ones, though at the 11px name font each glyph is
    // about 11 units wide against the 6.2 assumed: the text is about 440 wide in a 272 box and
    // overflows. Recorded as known behaviour: changing it would change the layout.
    let cjk = "流程图标签".repeat(8);
    assert_eq!(cjk.chars().count(), 40);
    assert_eq!(task_width(&cjk), task_width(&"x".repeat(40)));
    assert_eq!(task_width(&cjk), 272.0);
    // A combining mark counts as a character of its own, which errs the other way.
    assert_eq!(
        task_width(&"e\u{301}".repeat(30)),
        task_width(&"x".repeat(60))
    );
}
