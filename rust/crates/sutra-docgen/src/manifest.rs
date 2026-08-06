//! Typed parsing for the two flat authoring manifests that pair with rule/template files:
//! `rules-manifest.yaml` (message-type applicability for `.dmn`/`.srl` files) and
//! `template-manifest.yaml` (input/output message-type + content-type contract for
//! `.hbs`/`.xsl`/`.xslt` files). Both schemas are simple closed lists — see
//! `sutra-loader/src/lint.rs` for the deploy-time consumer this mirrors read-only.

use serde::Deserialize;

/// `rules-manifest.yaml`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RulesManifest {
    #[serde(default)]
    pub rules: Vec<RuleEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuleEntry {
    pub file: String,
    #[serde(default, rename = "messageTypes")]
    pub message_types: Vec<String>,
}

impl RulesManifest {
    pub fn parse(text: &str) -> Result<RulesManifest, serde_yaml::Error> {
        serde_yaml::from_str(text)
    }

    /// Lookup by file basename (the manifest's `file:` key is a bare basename).
    pub fn entry_for(&self, basename: &str) -> Option<&RuleEntry> {
        self.rules.iter().find(|r| r.file == basename)
    }
}

/// `template-manifest.yaml`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TemplateManifest {
    #[serde(default)]
    pub templates: Vec<TemplateEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TemplateEntry {
    pub file: String,
    #[serde(default, rename = "inputMessageType")]
    pub input_message_type: Option<String>,
    #[serde(default, rename = "outputMessageType")]
    pub output_message_type: Option<String>,
    #[serde(default, rename = "contentType")]
    pub content_type: Option<String>,
}

impl TemplateManifest {
    pub fn parse(text: &str) -> Result<TemplateManifest, serde_yaml::Error> {
        serde_yaml::from_str(text)
    }

    pub fn entry_for(&self, basename: &str) -> Option<&TemplateEntry> {
        self.templates.iter().find(|t| t.file == basename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rules_manifest() {
        let m = RulesManifest::parse(
            "rules:\n  - file: a.dmn\n    messageTypes: [order.created.001.14]\n",
        )
        .unwrap();
        assert_eq!(m.rules.len(), 1);
        assert_eq!(
            m.entry_for("a.dmn").unwrap().message_types,
            ["order.created.001.14"]
        );
        assert!(m.entry_for("missing.dmn").is_none());
    }

    #[test]
    fn parses_template_manifest() {
        let m = TemplateManifest::parse(
            "templates:\n  - file: a.hbs\n    inputMessageType: order.created.001.14\n    \
             outputMessageType: invoice.settled.001.15\n    contentType: application/xml\n",
        )
        .unwrap();
        let e = m.entry_for("a.hbs").unwrap();
        assert_eq!(
            e.input_message_type.as_deref(),
            Some("order.created.001.14")
        );
        assert_eq!(e.content_type.as_deref(), Some("application/xml"));
    }
}
