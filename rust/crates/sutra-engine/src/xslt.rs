//! A minimal, FAIL-CLOSED XSLT 1.0 subset engine — the OPTIONAL `.xsl` template
//! extension (`.hbs` is the normative template engine). Deliberately small: it
//! implements exactly the constructs the input-parity contract of the XSLT
//! template engine exposes to module authors, and ERRORS on anything else rather
//! than rendering wrong output.
//!
//! Supported:
//! - `<xsl:stylesheet>` with `<xsl:param name=…>` declarations and ONE
//!   `<xsl:template match="/">` whose body is literal result elements + text;
//! - attribute value templates `{expr}` and `<xsl:value-of select="expr"/>`;
//! - expressions: `$var` (a scalar process variable), `$var/Child/...` (navigation into a
//!   map-valued variable, e.g. the decoded `$payload`), and
//!   `//*[local-name()='Name']` (first matching element of the SOURCE document — the raw
//!   inbound message, `event.body`).
//!
//! Everything else (other match patterns, for-each/if/choose, functions, axes) is a
//! render error — fail-closed, never silently wrong.

// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value` (which additionally collapses in-value whitespace); this subset
// engine keeps the exact 0.37 entity-only semantics, so the deprecation is allowed.
#![allow(deprecated)]

use quick_xml::events::Event;
use quick_xml::Reader;
use sutra_executor::TemplateEngine;

/// The `.xsl`/`.xslt` engine (see module docs for the supported subset).
#[derive(Default)]
pub struct XslTemplateEngine;

impl XslTemplateEngine {
    pub fn new() -> XslTemplateEngine {
        XslTemplateEngine
    }
}

impl TemplateEngine for XslTemplateEngine {
    fn name(&self) -> &str {
        "xslt-subset"
    }

    fn extensions(&self) -> Vec<String> {
        vec![".xsl".to_string(), ".xslt".to_string()]
    }

    fn render(
        &self,
        template_id: &str,
        template: &[u8],
        model: &serde_json::Value,
    ) -> Result<String, String> {
        render_stylesheet(template, model)
            .map_err(|e| format!("XSLT subset render of '{template_id}' failed: {e}"))
    }
}

fn render_stylesheet(template: &[u8], model: &serde_json::Value) -> Result<String, String> {
    let mut reader = Reader::from_reader(template);
    let mut out = String::new();
    let mut in_root_template = false;
    let mut open_literals: Vec<String> = Vec::new();
    // Buffer for literal-result-element text. quick-xml 0.41 splits a text run around
    // entity references (`Event::GeneralRef`); the whole run is decoded/reassembled here
    // and trimmed+escaped as one, matching the 0.37 single-`Text`-event behaviour.
    let mut pending = String::new();
    loop {
        let event = reader.read_event().map_err(|e| e.to_string())?;
        // Flush buffered literal text before any non-text event closes the run.
        if in_root_template && !matches!(event, Event::Text(_) | Event::GeneralRef(_)) {
            flush_literal_text(&mut out, &mut pending);
        }
        match event {
            Event::Start(e) | Event::Empty(e)
                if is_xsl(e.name().as_ref(), "template") && !in_root_template =>
            {
                let matches = attr(&e, "match")?.unwrap_or_default();
                if matches.trim() != "/" {
                    return Err(format!(
                        "only <xsl:template match=\"/\"> is supported (got match=\"{matches}\")"
                    ));
                }
                in_root_template = true;
            }
            Event::End(e) if is_xsl(e.name().as_ref(), "template") => {
                in_root_template = false;
            }
            Event::Start(e) if in_root_template => {
                if let Some(local) = xsl_local(e.name().as_ref()) {
                    if local == "value-of" {
                        // handled at Empty; a Start form with children is unsupported
                        return Err("<xsl:value-of> must be empty".to_string());
                    }
                    return Err(format!("<xsl:{local}> is outside the supported subset"));
                }
                out.push_str(&serialize_open(&e, model, false)?);
                open_literals.push(qname(e.name().as_ref()));
            }
            Event::Empty(e) if in_root_template => {
                if let Some(local) = xsl_local(e.name().as_ref()) {
                    if local == "value-of" {
                        let select = attr(&e, "select")?.ok_or("xsl:value-of needs @select")?;
                        out.push_str(&escape_xml(&eval_expr(select.trim(), model)?));
                        continue;
                    }
                    return Err(format!("<xsl:{local}/> is outside the supported subset"));
                }
                out.push_str(&serialize_open(&e, model, true)?);
            }
            Event::End(e) if in_root_template => {
                let name = qname(e.name().as_ref());
                open_literals.pop();
                out.push_str(&format!("</{name}>"));
            }
            Event::Text(t) if in_root_template => {
                pending.push_str(&t.decode().map_err(|e| e.to_string())?);
            }
            Event::GeneralRef(r) if in_root_template => {
                push_reference(&mut pending, &r)?;
            }
            Event::Start(e) | Event::Empty(e)
                if xsl_local(e.name().as_ref()).is_some_and(|l| {
                    !["stylesheet", "output", "param", "transform"].contains(&l)
                }) =>
            {
                return Err(format!(
                    "top-level <xsl:{}> is outside the supported subset",
                    xsl_local(e.name().as_ref()).unwrap_or("?")
                ));
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(out)
}

/// Flush a buffered literal-text run: trim the whole run and, when non-empty, XML-escape
/// and append it (the 0.37 per-`Text`-event behaviour, applied once per full run).
fn flush_literal_text(out: &mut String, pending: &mut String) {
    if !pending.trim().is_empty() {
        out.push_str(&escape_xml(pending.trim()));
    }
    pending.clear();
}

/// Resolve one general reference (`&name;` or `&#nn;`) into `out`, reproducing the
/// quick-xml 0.37 text-unescape behaviour: only the five predefined entities and numeric
/// character references; any other entity is an error.
fn push_reference(out: &mut String, r: &quick_xml::events::BytesRef<'_>) -> Result<(), String> {
    if let Some(ch) = r.resolve_char_ref().map_err(|e| e.to_string())? {
        out.push(ch);
    } else {
        let name = r.decode().map_err(|e| e.to_string())?;
        match quick_xml::escape::resolve_predefined_entity(&name) {
            Some(rep) => out.push_str(rep),
            None => return Err(format!("unknown entity reference '&{name};'")),
        }
    }
    Ok(())
}

/// Serialize a literal result element with AVT-substituted attributes.
fn serialize_open(
    e: &quick_xml::events::BytesStart<'_>,
    model: &serde_json::Value,
    self_closing: bool,
) -> Result<String, String> {
    let mut s = format!("<{}", qname(e.name().as_ref()));
    for a in e.attributes() {
        let a = a.map_err(|e| e.to_string())?;
        let name = String::from_utf8_lossy(a.key.as_ref()).into_owned();
        if name.starts_with("xmlns") {
            continue; // namespace declarations are not re-emitted by the subset
        }
        let raw = a.unescape_value().map_err(|e| e.to_string())?.into_owned();
        let value = substitute_avt(&raw, model)?;
        s.push_str(&format!(" {name}=\"{}\"", escape_xml(&value)));
    }
    s.push_str(if self_closing { "/>" } else { ">" });
    Ok(s)
}

/// Attribute value template: replace every `{expr}` with its evaluated string.
fn substitute_avt(raw: &str, model: &serde_json::Value) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = raw;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| format!("unterminated {{ in attribute value '{raw}'"))?;
        out.push_str(&eval_expr(after[..close].trim(), model)?);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// The supported expression forms (see module docs). Fail-closed on anything else.
fn eval_expr(expr: &str, model: &serde_json::Value) -> Result<String, String> {
    if let Some(var_path) = expr.strip_prefix('$') {
        let mut parts = var_path.split('/');
        let var = parts.next().unwrap_or_default().trim();
        let mut value = model
            .get(var)
            .ok_or_else(|| format!("parameter '${var}' is not bound"))?;
        for part in parts {
            let part = part.trim();
            value = value.get(part).ok_or_else(|| {
                format!("'${var_path}' does not resolve (missing field '{part}')")
            })?;
        }
        return Ok(json_scalar_string(value));
    }
    if let Some(rest) = expr.strip_prefix("//*[local-name()=") {
        let name = rest
            .strip_suffix(']')
            .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
            .ok_or_else(|| format!("unsupported XPath '{expr}'"))?;
        let source = model
            .get("event")
            .and_then(|e| e.get("body"))
            .and_then(|b| b.as_str())
            .ok_or("no source document (event.body) available for '//' XPath")?;
        return first_element_text(source.as_bytes(), name)
            .ok_or_else(|| format!("source document has no element '{name}'"));
    }
    Err(format!(
        "XPath '{expr}' is outside the supported subset ($var, $var/child, \
         //*[local-name()='name'])"
    ))
}

/// Text content of the first element with the given local name in the source document.
fn first_element_text(source: &[u8], local_name: &str) -> Option<String> {
    let mut reader = Reader::from_reader(source);
    let mut capturing = false;
    let mut depth = 0usize;
    let mut text = String::new();
    loop {
        match reader.read_event().ok()? {
            Event::Start(e) => {
                let local = e.local_name();
                let matches = String::from_utf8_lossy(local.as_ref()) == local_name;
                if capturing {
                    depth += 1;
                } else if matches {
                    capturing = true;
                    depth = 0;
                }
            }
            Event::End(_) if capturing => {
                if depth == 0 {
                    return Some(text);
                }
                depth -= 1;
            }
            Event::Text(t) if capturing => {
                text.push_str(&t.decode().ok()?);
            }
            Event::GeneralRef(r) if capturing => {
                // quick-xml 0.41 emits entity/char references as their own event; fold
                // them back into the captured element text (concatenated, no trimming).
                push_reference(&mut text, &r).ok()?;
            }
            Event::Eof => return None,
            _ => {}
        }
    }
}

fn json_scalar_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn is_xsl(name: &[u8], local: &str) -> bool {
    xsl_local(name) == Some(local)
}

/// The local name when the qname carries the conventional `xsl:` prefix.
fn xsl_local(name: &[u8]) -> Option<&str> {
    let q = std::str::from_utf8(name).ok()?;
    q.strip_prefix("xsl:")
}

/// One attribute's unescaped value, by name.
fn attr(e: &quick_xml::events::BytesStart<'_>, name: &str) -> Result<Option<String>, String> {
    for a in e.attributes() {
        let a = a.map_err(|e| e.to_string())?;
        if a.key.as_ref() == name.as_bytes() {
            return Ok(Some(
                a.unescape_value().map_err(|e| e.to_string())?.into_owned(),
            ));
        }
    }
    Ok(None)
}

fn qname(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHOWCASE_XSL: &[u8] =
        br#"<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" omit-xml-declaration="yes"/>
  <xsl:param name="payload"/>
  <xsl:param name="prepped"/>
  <xsl:template match="/">
    <Xslt prepped="{$prepped}" e2e="{//*[local-name()='E2EId']}" amount="{$payload/Amount}"/>
  </xsl:template>
</xsl:stylesheet>"#;

    #[test]
    fn renders_the_shipped_showcase_stylesheet() {
        let model = serde_json::json!({
            "prepped": "ok",
            "payload": { "E2EId": "SHOW-XSLT-1", "Amount": "99.00" },
            "event": { "body":
                "<ApprovalRequest xmlns=\"urn:sutra:approval\"><E2EId>SHOW-XSLT-1</E2EId><Amount>99.00</Amount></ApprovalRequest>" }
        });
        let out = XslTemplateEngine::new()
            .render("t.xsl", SHOWCASE_XSL, &model)
            .expect("renders");
        assert_eq!(
            out,
            r#"<Xslt prepped="ok" e2e="SHOW-XSLT-1" amount="99.00"/>"#
        );
    }

    #[test]
    fn unsupported_constructs_fail_closed() {
        let xsl =
            br#"<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <Out><xsl:for-each select="//x"><i/></xsl:for-each></Out>
  </xsl:template>
</xsl:stylesheet>"#;
        let err = XslTemplateEngine::new()
            .render("t.xsl", xsl, &serde_json::json!({}))
            .expect_err("for-each is unsupported");
        assert!(err.contains("for-each"), "{err}");

        let bad_match =
            br#"<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="//thing"><Out/></xsl:template>
</xsl:stylesheet>"#;
        let err = XslTemplateEngine::new()
            .render("t.xsl", bad_match, &serde_json::json!({}))
            .expect_err("non-root match is unsupported");
        assert!(err.contains("match"), "{err}");
    }
}
