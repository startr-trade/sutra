//! Archive-supplied Handlebars redactor — wraps a user-supplied `.hbs` template, compiled/checked
//! at deploy time, into a [`ContentRedactor`]. The counterpart to a `sutra-redactor-<kind>`
//! extension crate: those `inventory::submit!` a hardcoded, per-data-structure detector and are
//! force-linked by a distribution; this one is NOT a built-in and does NOT self-register — the
//! deploy-time assembly instantiates [`HbsContentRedactor::new`] directly from the archive's
//! `<q:redactor>` template body, exactly like the `<q:redactor engine="hbs">` composition the
//! design describes. Being template-driven, it is the redactor a DEPLOYMENT can bring with it,
//! which is why it is the only one the public distribution needs to ship.
//!
//! The template renders against the decoded payload (bridged to JSON via
//! `sutra_executor::variables::feel_to_json`, the same bridge the script/template render paths
//! use). Its OUTPUT is newline-delimited RFC 6901 JSON-Pointers — one sensitive path per line;
//! blank / whitespace-only lines are skipped so a trailing newline or a blank separator line never
//! produces a stray empty-string locator (which would mean "redact the whole payload").
//!
//! Fail-closed at two different times, deliberately:
//! - a syntactically invalid template fails [`HbsContentRedactor::new`] — a deploy-time reject,
//!   never a live [`ContentRedactor::locate`] failure;
//! - a template that compiles but cannot render against a GIVEN payload (e.g. it strict-mode
//!   references a field the payload does not have) fails `locate`, which `run_redactor` (in
//!   `sutra-redactor-spi`) converts into a fail-closed over-mask outcome.
#![forbid(unsafe_code)]

use sutra_executor::variables::feel_to_json;
use sutra_executor::Variables;
use sutra_feel::FeelValue;
use sutra_redactor_spi::{ContentRedactor, RedactionLocator};
use sutra_templates::HandlebarsTemplateEngine;

/// Reason code recorded for every locator this redactor emits.
pub const REDACT_TEMPLATE: &str = "SUTRA.REDACT.TEMPLATE";

/// The template-id the single compiled template is cached under inside the engine instance.
/// Arbitrary but stable — one `HbsContentRedactor` owns exactly one engine and one template.
const TEMPLATE_ID: &str = "redactor";

/// A [`ContentRedactor`] backed by a user-supplied Handlebars template. The template is
/// compile-checked once at construction (deploy time) so a malformed template fails the deploy,
/// never a live `locate` call.
pub struct HbsContentRedactor {
    engine: HandlebarsTemplateEngine,
    source: Vec<u8>,
}

impl HbsContentRedactor {
    /// Compile-check `template_source` and build the redactor. `Err` on a syntactically invalid
    /// template (fail at deploy time, not at locate time).
    pub fn new(template_source: &str) -> Result<Self, String> {
        let engine = HandlebarsTemplateEngine::new();
        engine
            .check(template_source.as_bytes())
            .map_err(|e| e.to_string())?;
        Ok(HbsContentRedactor {
            engine,
            source: template_source.as_bytes().to_vec(),
        })
    }
}

impl ContentRedactor for HbsContentRedactor {
    fn name(&self) -> &str {
        "template"
    }

    /// Render the template against `feel_to_json(payload)`; split the output on `\n`, trim each
    /// line, and skip empty/whitespace-only lines. Each remaining line becomes a
    /// [`RedactionLocator`] at that JSON-Pointer path with reason code [`REDACT_TEMPLATE`].
    fn locate(
        &self,
        payload: &FeelValue,
        _variables: &Variables,
    ) -> Result<Vec<RedactionLocator>, String> {
        let model = feel_to_json(payload);
        let rendered = self
            .engine
            .render(TEMPLATE_ID, &self.source, &model)
            .map_err(|e| e.to_string())?;
        Ok(rendered
            .split('\n')
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| RedactionLocator::new(line, REDACT_TEMPLATE))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn s(v: &str) -> FeelValue {
        FeelValue::String(v.to_string())
    }
    fn map(pairs: &[(&str, FeelValue)]) -> FeelValue {
        FeelValue::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    #[test]
    fn two_pointers_over_a_map_become_two_locators() {
        // `{{#each this}}` iterates the root object's entries (BTreeMap order == sorted keys).
        let r = HbsContentRedactor::new("{{#each this}}/{{@key}}\n{{/each}}").unwrap();
        let payload = map(&[("card", s("4111")), ("note", s("hi"))]);
        let locs = r.locate(&payload, &Variables::new()).unwrap();
        assert_eq!(
            locs,
            vec![
                RedactionLocator::new("/card", REDACT_TEMPLATE),
                RedactionLocator::new("/note", REDACT_TEMPLATE),
            ]
        );
    }

    #[test]
    fn blank_and_whitespace_lines_are_skipped_and_trimmed() {
        let r = HbsContentRedactor::new("\n   \n  /card  \n\n").unwrap();
        let locs = r.locate(&FeelValue::Null, &Variables::new()).unwrap();
        assert_eq!(locs, vec![RedactionLocator::new("/card", REDACT_TEMPLATE)]);
    }

    #[test]
    fn syntactically_invalid_template_fails_at_construction() {
        // Same unterminated-block shape sutra-templates' own test suite uses as its
        // known-invalid case (handlebars_engine_test.rs::invalid_template_fails_clearly).
        let err = HbsContentRedactor::new("<A>{{#if x}}unterminated")
            .map(|_| ())
            .unwrap_err();
        assert!(err.contains("Invalid Handlebars template"), "{err}");
    }

    #[test]
    fn render_failure_against_a_given_payload_fails_locate() {
        // Compiles fine (no block/syntax error) but strict mode aborts the render when `nope`
        // is absent from the payload — a post-compile, per-payload failure.
        let r = HbsContentRedactor::new("{{nope}}").unwrap();
        let payload = map(&[("card", s("4111"))]);
        let err = r.locate(&payload, &Variables::new()).unwrap_err();
        assert!(err.contains("unresolved template reference"), "{err}");
    }

    #[test]
    fn reason_code_constant_matches_locator() {
        let r = HbsContentRedactor::new("/x\n").unwrap();
        let locs = r.locate(&FeelValue::Null, &Variables::new()).unwrap();
        assert_eq!(locs[0].reason_code, "SUTRA.REDACT.TEMPLATE");
    }
}
