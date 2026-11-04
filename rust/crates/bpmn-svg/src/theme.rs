//! The theming option: every colour of a rendered SVG rewritten as a CSS variable.
//!
//! Done as a pass over the finished document, so the drawing code keeps one set of literals and the
//! default output cannot change. Only the inside of tags is rewritten, so a label that happens to
//! spell a colour stays as written: label text has `<` and `>` escaped, which makes the tag
//! boundaries unambiguous.

/// The drawing code's colours, each with its variable.
const COLOURS: [(&str, &str); 5] = [
    ("\"#333\"", "\"var(--bpmn-stroke, #333)\""),
    ("\"#222\"", "\"var(--bpmn-text, #222)\""),
    ("\"#888\"", "\"var(--bpmn-muted, #888)\""),
    ("\"#fff\"", "\"var(--bpmn-surface, #fff)\""),
    (
        "\"#ffffffe6\"",
        "\"var(--bpmn-label-background, #ffffffe6)\"",
    ),
];

pub(crate) fn with_css_variables(svg: &str) -> String {
    let mut out = String::with_capacity(svg.len() + svg.len() / 4);
    let mut rest = svg;
    while let Some(open) = rest.find('<') {
        let (text, from_tag) = rest.split_at(open);
        out.push_str(text);
        let close = from_tag.find('>').map_or(from_tag.len(), |i| i + 1);
        let (tag, after) = from_tag.split_at(close);
        let mut themed = tag.to_string();
        for (literal, variable) in COLOURS {
            themed = themed.replace(literal, variable);
        }
        out.push_str(&themed);
        rest = after;
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_attributes_and_leaves_text_alone() {
        let svg = "<rect fill=\"#fff\" stroke=\"#333\"/><text fill=\"#222\">\"#333\"</text>";
        assert_eq!(
            with_css_variables(svg),
            "<rect fill=\"var(--bpmn-surface, #fff)\" stroke=\"var(--bpmn-stroke, #333)\"/>\
             <text fill=\"var(--bpmn-text, #222)\">\"#333\"</text>"
        );
    }
}
