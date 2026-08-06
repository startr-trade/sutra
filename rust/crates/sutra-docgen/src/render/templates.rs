//! Template/script pages (`.hbs`, `.xsl`, `.xslt`) — the file plus its `template-manifest.yaml`
//! entry (target message type, content type) when one exists, plus a best-effort description
//! extracted from the file's own leading comment. The template BODY is never dumped — this is
//! a metadata summary, matching the BPMN pages' "readable summary, not raw source" posture.

use std::fmt::Write as _;

use crate::manifest::TemplateManifest;
use crate::util::cell;

pub fn render_template_page(
    text: &str,
    rel: &str,
    basename: &str,
    manifest: Option<&TemplateManifest>,
) -> String {
    let mut out = String::new();
    let engine = match rel.rsplit('.').next() {
        Some("hbs") => "Handlebars",
        Some("xsl") | Some("xslt") => "XSLT",
        _ => "unknown",
    };
    let _ = writeln!(out, "**Engine:** {engine}\n");

    match manifest.and_then(|m| m.entry_for(basename)) {
        Some(entry) => {
            out.push_str("| Field | Value |\n|---|---|\n");
            let _ = writeln!(
                out,
                "| Input message type | {} |",
                entry.input_message_type.as_deref().unwrap_or("—")
            );
            let _ = writeln!(
                out,
                "| Output message type | {} |",
                entry.output_message_type.as_deref().unwrap_or("—")
            );
            let _ = writeln!(
                out,
                "| Content type | {} |",
                entry.content_type.as_deref().unwrap_or("—")
            );
            out.push('\n');
        }
        None => {
            out.push_str("_No `template-manifest.yaml` entry found for this file._\n\n");
        }
    }

    if let Some(desc) = leading_comment(text, engine) {
        let _ = writeln!(out, "**Description** (from the file's leading comment):\n");
        let _ = writeln!(out, "> {}\n", cell(&desc));
    }

    out
}

/// Best-effort extraction of the file's leading comment as a description:
/// Handlebars `{{! ... }}` / `{{!-- ... --}}`, or an XML/XSLT `<!-- ... -->` (after an optional
/// `<?xml ... ?>` declaration).
fn leading_comment(text: &str, engine: &str) -> Option<String> {
    let trimmed = text.trim_start();
    let body = match engine {
        "Handlebars" => {
            let inner = trimmed
                .strip_prefix("{{!--")
                .or_else(|| trimmed.strip_prefix("{{!"))?;
            let end = inner.find("--}}").or_else(|| inner.find("}}"))?;
            &inner[..end]
        }
        "XSLT" => {
            let after_decl = if let Some(rest) = trimmed.strip_prefix("<?xml") {
                let end = rest.find("?>")? + 2;
                rest[end..].trim_start()
            } else {
                trimmed
            };
            let inner = after_decl.strip_prefix("<!--")?;
            let end = inner.find("-->")?;
            &inner[..end]
        }
        _ => return None,
    };
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        None
    } else {
        Some(collapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_handlebars_comment() {
        let src = "{{! Renders a greeting.\n   Second line. }}\n{{name}}";
        assert_eq!(
            leading_comment(src, "Handlebars").as_deref(),
            Some("Renders a greeting. Second line.")
        );
    }

    #[test]
    fn extracts_xslt_comment_after_xml_decl() {
        let src = "<?xml version=\"1.0\"?>\n<!-- Demonstrates parity. -->\n<xsl:stylesheet/>";
        assert_eq!(
            leading_comment(src, "XSLT").as_deref(),
            Some("Demonstrates parity.")
        );
    }

    #[test]
    fn no_comment_returns_none() {
        assert_eq!(leading_comment("{{name}}", "Handlebars"), None);
    }
}
