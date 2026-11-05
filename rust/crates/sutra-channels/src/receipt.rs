//! The acceptance receipt: what the engine accepted when it answers an HTTP request 202.
//!
//! The receipt belongs to the engine, not to the flow. It travels in two response headers, which
//! leaves a flow-authored 202 body byte-for-byte untouched, and it also becomes the JSON body when
//! the flow replied nothing. A duplicate's receipt names the instance the first delivery started.
//!
//! Nothing here reaches flow data by itself: alias values and `<q:receipt>` attributes are
//! already redacted when they arrive (see `sutra_executor::audit_correlation`).

use std::collections::BTreeMap;

/// The response header carrying the instance id, alone, for a caller that wants nothing else.
pub const INSTANCE_ID_HEADER: &str = "Sutra-Instance-Id";
/// The response header carrying the whole receipt as one line of ASCII JSON.
pub const RECEIPT_HEADER: &str = "Sutra-Receipt";
/// The largest [`RECEIPT_HEADER`] value sent. Proxies commonly refuse response headers beyond
/// 8 KiB in total, and the flow chooses how much goes in, so a receipt that would not fit drops
/// its flow-sized fields and says so with `"truncated": true`. The instance id always survives.
pub const RECEIPT_HEADER_MAX_BYTES: usize = 4096;

/// What a dispatch contributes to a receipt, beyond what the channel and the outcome already say.
/// Plain data (`Send`), carried on [`crate::dispatch::DispatchOutcome::Completed`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AcceptanceFacts {
    /// The instance's correlation aliases at the moment of the answer, redacted.
    pub aliases: BTreeMap<String, String>,
    /// The flow's `<q:receipt>` attributes, redacted.
    pub attributes: BTreeMap<String, String>,
    /// The key that makes a retry the same request; absent when the caller supplied none, since
    /// the transport's body hash neither identifies a retry nor suppresses one.
    pub idempotency_key: Option<String>,
}

/// One rendered receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptanceReceipt {
    /// The started or resumed instance; for a duplicate, the original. Absent when the first
    /// delivery's record names no instance: it predates the record keeping one, or that delivery
    /// ended without an instance.
    pub instance_id: Option<String>,
    /// The deployment serving the channel.
    pub deployment_id: String,
    /// The serving deployment's authoring labels (`tenant`, `module`, `version`).
    pub labels: BTreeMap<String, String>,
    /// This delivery was already seen: nothing new was started.
    pub duplicate: bool,
    pub facts: AcceptanceFacts,
}

impl AcceptanceReceipt {
    /// The receipt as the JSON document the body carries.
    pub fn to_json(&self) -> serde_json::Value {
        let mut doc = self.identity();
        doc["aliases"] = serde_json::json!(self.facts.aliases);
        doc["attributes"] = serde_json::json!(self.facts.attributes);
        if let Some(key) = &self.facts.idempotency_key {
            doc["idempotencyKey"] = serde_json::json!(key);
        }
        doc
    }

    /// The fields that identify the acceptance, which every rendering keeps.
    fn identity(&self) -> serde_json::Value {
        let mut doc = serde_json::json!({
            "deploymentId": self.deployment_id,
            "labels": self.labels,
            "duplicate": self.duplicate,
        });
        if let Some(id) = &self.instance_id {
            doc["instanceId"] = serde_json::json!(id);
            if self.duplicate {
                doc["originalInstanceId"] = serde_json::json!(id);
            }
        }
        doc
    }

    /// The value of [`RECEIPT_HEADER`]: ASCII JSON, so it is a legal header value whatever the
    /// flow put in it, cut down to [`RECEIPT_HEADER_MAX_BYTES`] when it would not fit.
    pub fn header_value(&self) -> String {
        let full = ascii_json(&self.to_json());
        if full.len() <= RECEIPT_HEADER_MAX_BYTES {
            return full;
        }
        let mut doc = self.identity();
        doc["truncated"] = serde_json::json!(true);
        ascii_json(&doc)
    }
}

/// Compact JSON with every byte a header value may not carry escaped: non-ASCII as `\uXXXX`
/// (surrogate pairs included), and DEL, which `serde_json` leaves alone. A header stays one line
/// because `serde_json` has already escaped the control characters.
fn ascii_json(value: &serde_json::Value) -> String {
    let text = value.to_string();
    let mut out = String::with_capacity(text.len());
    let mut units = [0u16; 2];
    for c in text.chars() {
        if c.is_ascii() && c != '\u{7f}' {
            out.push(c);
        } else {
            for unit in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(duplicate: bool) -> AcceptanceReceipt {
        AcceptanceReceipt {
            instance_id: Some("i-1".to_string()),
            deployment_id: "dep-1".to_string(),
            labels: BTreeMap::from([("tenant".to_string(), "t".to_string())]),
            duplicate,
            facts: AcceptanceFacts {
                aliases: BTreeMap::from([("order".to_string(), "42".to_string())]),
                attributes: BTreeMap::from([("caseId".to_string(), "c-7".to_string())]),
                idempotency_key: Some("k-1".to_string()),
            },
        }
    }

    #[test]
    fn an_unknown_original_names_no_instance() {
        let mut r = receipt(true);
        r.instance_id = None;
        let doc = r.to_json();
        assert_eq!(doc["duplicate"], true);
        assert!(doc.get("instanceId").is_none());
        assert!(doc.get("originalInstanceId").is_none());
    }

    #[test]
    fn body_carries_every_field() {
        let doc = receipt(false).to_json();
        assert_eq!(doc["instanceId"], "i-1");
        assert_eq!(doc["deploymentId"], "dep-1");
        assert_eq!(doc["labels"]["tenant"], "t");
        assert_eq!(doc["duplicate"], false);
        assert_eq!(doc["aliases"]["order"], "42");
        assert_eq!(doc["attributes"]["caseId"], "c-7");
        assert_eq!(doc["idempotencyKey"], "k-1");
        assert!(doc.get("originalInstanceId").is_none());
    }

    #[test]
    fn a_duplicate_names_its_original() {
        let doc = receipt(true).to_json();
        assert_eq!(doc["duplicate"], true);
        assert_eq!(doc["originalInstanceId"], "i-1");
        assert_eq!(doc["instanceId"], "i-1");
    }

    #[test]
    fn header_value_is_a_legal_header_whatever_the_flow_exposed() {
        let mut r = receipt(false);
        r.facts.attributes.insert(
            "note".to_string(),
            "caf\u{e9} \u{1f600}\n\u{7f}".to_string(),
        );
        let value = r.header_value();
        assert!(axum::http::HeaderValue::from_str(&value).is_ok(), "{value}");
        let parsed: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert_eq!(parsed["attributes"]["note"], "caf\u{e9} \u{1f600}\n\u{7f}");
    }

    #[test]
    fn an_oversized_header_drops_the_flow_sized_fields_and_says_so() {
        let mut r = receipt(false);
        r.facts
            .attributes
            .insert("big".to_string(), "x".repeat(RECEIPT_HEADER_MAX_BYTES));
        let value = r.header_value();
        assert!(value.len() <= RECEIPT_HEADER_MAX_BYTES);
        let parsed: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert_eq!(parsed["truncated"], true);
        assert_eq!(parsed["instanceId"], "i-1");
        assert!(parsed.get("attributes").is_none());
        // The body has no such limit.
        assert_eq!(
            r.to_json()["attributes"]["big"].as_str().unwrap().len(),
            RECEIPT_HEADER_MAX_BYTES
        );
    }
}
