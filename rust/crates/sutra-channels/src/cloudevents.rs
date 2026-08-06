//! Inbound CloudEvents-over-HTTP extraction — the `cloudevents.mode` channel
//! property decides how an inbound HTTP request projects into a [`CloudEvent`] view:
//!
//! - **binary** — `ce-*` request headers become attributes, the body IS the event data;
//! - **structured** — an `application/cloudevents+json` envelope is parsed, `data` /
//!   `data_base64` becomes the event data;
//! - **auto** (default) — content-type sniff: a `application/cloudevents+json` content type
//!   selects structured, all four required `ce-` headers select binary, else raw passthrough;
//! - **wrap** — a non-CE request is wrapped into a synthetic CE view (ids/source/type
//!   synthesised), the body preserved raw;
//! - **none** / **native** — passthrough (no CE view).
//!
//! The extracted `id` feeds the idempotency key (EXPLICIT — it drives inbox dedup);
//! `type` / `source` / `subject` and the rest land as intake variables under
//! `event.cloudEvent.*` (camel-cased attribute names: `specVersion`, `dataContentType`,
//! `dataSchema`, plus an `extensions` sub-map).
//!
//! The wrap-mode synthetic defaults use the `sutra.*` vocabulary: `sutra:channel:<channel>`
//! for `source` and `sutra.channel.inbound` for `type`.

use std::collections::BTreeMap;

use base64::Engine;

use crate::codes;
use crate::diag::Diagnostic;

/// The wrap-mode synthetic `type` default (the `sutra.*` vocabulary).
pub const WRAP_DEFAULT_TYPE: &str = "sutra.channel.inbound";
/// The wrap-mode default CloudEvents `specversion`.
pub const DEFAULT_SPEC_VERSION: &str = "1.0";

/// A parsed CloudEvent view — the intake variables `event.cloudEvent.*` project from this.
/// Field → variable name: `id`, `source`, `specVersion`, `type`, `subject`, `time`,
/// `dataContentType`, `dataSchema`, `extensions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudEvent {
    pub id: Option<String>,
    pub source: Option<String>,
    pub spec_version: Option<String>,
    pub event_type: Option<String>,
    pub subject: Option<String>,
    pub time: Option<String>,
    pub data_content_type: Option<String>,
    pub data_schema: Option<String>,
    pub extensions: BTreeMap<String, String>,
}

/// The declared `cloudevents.mode` (YAML `cloudevents-mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CeMode {
    Binary,
    Structured,
    Auto,
    Wrap,
    /// No CE view — raw passthrough (`none` / `native`).
    None,
}

impl CeMode {
    /// Parse the property value. The default (absent / unknown) is `auto`.
    pub fn parse(raw: Option<&str>) -> CeMode {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("binary") => CeMode::Binary,
            Some("structured") => CeMode::Structured,
            Some("wrap") => CeMode::Wrap,
            Some("none") | Some("native") => CeMode::None,
            _ => CeMode::Auto,
        }
    }
}

/// The result of an extraction: the (optional) CE view plus the effective body/content-type
/// the intake should carry, and the CE `id` when it should drive the idempotency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CeExtraction {
    pub cloud_event: Option<CloudEvent>,
    /// The extracted payload — wrapped [`sutra_executor::Sensitive`] (see
    /// [`crate::dispatch::InboundMessage::body`]).
    pub body: sutra_executor::Sensitive<Vec<u8>>,
    pub content_type: Option<String>,
    /// The CE `id` to use as the EXPLICIT idempotency key (drives inbox dedup); `None` when
    /// there is no stable client-supplied id.
    pub explicit_id: Option<String>,
}

/// Defaults an operator can set for wrap mode via channel properties (`ce.source` / `ce.type`).
#[derive(Debug, Clone, Copy, Default)]
pub struct WrapDefaults<'a> {
    pub source: Option<&'a str>,
    pub event_type: Option<&'a str>,
}

/// Extract a CloudEvent view from an inbound HTTP request per `mode`.
pub fn extract(
    mode: CeMode,
    channel: &str,
    headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
    body: &[u8],
    wrap_defaults: WrapDefaults<'_>,
) -> Result<CeExtraction, Diagnostic> {
    match mode {
        CeMode::None => Ok(passthrough(body, content_type)),
        CeMode::Binary => parse_binary(channel, headers, content_type, body),
        CeMode::Structured => parse_structured(channel, body),
        CeMode::Wrap => Ok(wrap_native(
            channel,
            headers,
            content_type,
            body,
            wrap_defaults,
        )),
        CeMode::Auto => {
            if content_type
                .map(|ct| ct.trim().to_ascii_lowercase().starts_with(CE_STRUCTURED_CT))
                .unwrap_or(false)
            {
                parse_structured(channel, body)
            } else if has_all_binary_headers(headers) {
                parse_binary(channel, headers, content_type, body)
            } else {
                Ok(passthrough(body, content_type))
            }
        }
    }
}

/// The structured CE content type (binding spec).
const CE_STRUCTURED_CT: &str = "application/cloudevents+json";

/// The four `ce-` headers `auto` requires to select the binary binding.
fn has_all_binary_headers(headers: &BTreeMap<String, String>) -> bool {
    ["ce-id", "ce-source", "ce-type", "ce-specversion"]
        .iter()
        .all(|h| lookup_ci(headers, h).is_some())
}

fn passthrough(body: &[u8], content_type: Option<&str>) -> CeExtraction {
    CeExtraction {
        cloud_event: None,
        body: body.to_vec().into(),
        content_type: content_type.map(str::to_string),
        explicit_id: None,
    }
}

/// Binary binding: `ce-*` headers → attributes; the body is the data.
fn parse_binary(
    channel: &str,
    headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
    body: &[u8],
) -> Result<CeExtraction, Diagnostic> {
    let mut ce = CloudEvent::default();
    for (key, value) in headers {
        let Some(attr) = strip_ce_prefix(key) else {
            continue;
        };
        let attr = attr.to_ascii_lowercase();
        match attr.as_str() {
            "id" => ce.id = non_blank(value),
            "source" => ce.source = non_blank(value),
            "type" => ce.event_type = non_blank(value),
            "specversion" => ce.spec_version = non_blank(value),
            "subject" => ce.subject = non_blank(value),
            "time" => ce.time = non_blank(value),
            "dataschema" => ce.data_schema = non_blank(value),
            other => {
                if let Some(v) = non_blank(value) {
                    ce.extensions.insert(other.to_string(), v);
                }
            }
        }
    }
    require_core(channel, &ce, "binary")?;
    if ce.spec_version.is_none() {
        ce.spec_version = Some(DEFAULT_SPEC_VERSION.to_string());
    }
    // datacontenttype rides the transport Content-Type in the binary binding.
    ce.data_content_type = content_type.map(str::to_string);
    validate_time(channel, ce.time.as_deref())?;
    let explicit_id = ce.id.clone();
    Ok(CeExtraction {
        content_type: ce.data_content_type.clone(),
        cloud_event: Some(ce),
        body: body.to_vec().into(),
        explicit_id,
    })
}

/// Structured binding: parse the `application/cloudevents+json` envelope; `data` /
/// `data_base64` becomes the event data.
fn parse_structured(channel: &str, body: &[u8]) -> Result<CeExtraction, Diagnostic> {
    let envelope: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
        reject(
            channel,
            format!("structured CloudEvent envelope is not valid JSON: {e}"),
        )
    })?;
    let serde_json::Value::Object(map) = &envelope else {
        return Err(reject(
            channel,
            "structured CloudEvent envelope must be a JSON object".to_string(),
        ));
    };
    let get_str = |key: &str| map.get(key).and_then(|v| v.as_str()).and_then(non_blank);
    let mut ce = CloudEvent {
        id: get_str("id"),
        source: get_str("source"),
        event_type: get_str("type"),
        spec_version: get_str("specversion"),
        subject: get_str("subject"),
        time: get_str("time"),
        data_content_type: get_str("datacontenttype"),
        data_schema: get_str("dataschema"),
        extensions: BTreeMap::new(),
    };
    require_core(channel, &ce, "structured")?;
    if ce.spec_version.is_none() {
        ce.spec_version = Some(DEFAULT_SPEC_VERSION.to_string());
    }
    validate_time(channel, ce.time.as_deref())?;
    // Non-standard scalar top-level keys are CE extension attributes.
    for (key, value) in map {
        if is_standard_attribute(key) {
            continue;
        }
        if let Some(v) = value.as_str().and_then(non_blank) {
            ce.extensions.insert(key.clone(), v);
        }
    }
    let data = extract_structured_data(channel, map)?;
    let explicit_id = ce.id.clone();
    Ok(CeExtraction {
        content_type: ce.data_content_type.clone(),
        cloud_event: Some(ce),
        body: data.into(),
        explicit_id,
    })
}

/// `data` (JSON string → UTF-8 bytes; object/array → re-serialised bytes; scalar →
/// serialised) → else `data_base64` (standard Base64 decode) → else empty.
fn extract_structured_data(
    channel: &str,
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<u8>, Diagnostic> {
    if let Some(data) = map.get("data") {
        return Ok(match data {
            serde_json::Value::String(s) => s.clone().into_bytes(),
            serde_json::Value::Null => Vec::new(),
            other => serde_json::to_vec(other).unwrap_or_default(),
        });
    }
    if let Some(serde_json::Value::String(b64)) = map.get("data_base64") {
        return base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .map_err(|e| reject(channel, format!("data_base64 is not valid Base64: {e}")));
    }
    Ok(Vec::new())
}

/// Wrap mode: synthesise a CE overlay over a raw request. `id` from `X-Request-Id` → `ce-id`
/// → a generated id (only a client-supplied id is EXPLICIT); source/type from properties or
/// the `sutra.*` defaults; the body is preserved raw.
fn wrap_native(
    channel: &str,
    headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
    body: &[u8],
    defaults: WrapDefaults<'_>,
) -> CeExtraction {
    let header_id = lookup_ci(headers, "x-request-id")
        .and_then(non_blank)
        .or_else(|| lookup_ci(headers, "ce-id").and_then(non_blank));
    let (id, explicit_id) = match header_id {
        Some(id) => (id.clone(), Some(id)),
        None => (generated_id(), None),
    };
    let ce = CloudEvent {
        id: Some(id),
        source: Some(
            defaults
                .source
                .and_then(non_blank)
                .unwrap_or_else(|| format!("sutra:channel:{channel}")),
        ),
        event_type: Some(
            defaults
                .event_type
                .and_then(non_blank)
                .unwrap_or_else(|| WRAP_DEFAULT_TYPE.to_string()),
        ),
        spec_version: Some(DEFAULT_SPEC_VERSION.to_string()),
        subject: None,
        time: Some(now_rfc3339()),
        data_content_type: content_type.map(str::to_string),
        data_schema: None,
        extensions: BTreeMap::new(),
    };
    CeExtraction {
        cloud_event: Some(ce),
        body: body.to_vec().into(),
        content_type: content_type.map(str::to_string),
        explicit_id,
    }
}

/// A standard CloudEvent context attribute (excluded from the extensions map / structured
/// data keys).
fn is_standard_attribute(key: &str) -> bool {
    matches!(
        key,
        "id" | "source"
            | "type"
            | "specversion"
            | "subject"
            | "time"
            | "datacontenttype"
            | "dataschema"
            | "data"
            | "data_base64"
    )
}

/// id / source / type are required (binary + structured).
fn require_core(channel: &str, ce: &CloudEvent, mode: &str) -> Result<(), Diagnostic> {
    for (name, present) in [
        ("id", ce.id.is_some()),
        ("source", ce.source.is_some()),
        ("type", ce.event_type.is_some()),
    ] {
        if !present {
            return Err(reject(
                channel,
                format!("{mode} CloudEvent is missing the required '{name}' attribute"),
            ));
        }
    }
    Ok(())
}

/// A present `time` must be an RFC 3339 timestamp; anything else rejects the delivery.
fn validate_time(channel: &str, time: Option<&str>) -> Result<(), Diagnostic> {
    let Some(time) = time else { return Ok(()) };
    time::OffsetDateTime::parse(time, &time::format_description::well_known::Rfc3339)
        .map(|_| ())
        .map_err(|_| {
            reject(
                channel,
                format!("CloudEvent 'time' attribute '{time}' is not an RFC 3339 timestamp"),
            )
        })
}

fn reject(channel: &str, detail: String) -> Diagnostic {
    Diagnostic::error(
        codes::INBOUND_REJECTED_CLOUDEVENT,
        format!("Channel '{channel}': {detail}"),
    )
}

/// Strip a case-insensitive `ce-` prefix, returning the attribute name.
fn strip_ce_prefix(key: &str) -> Option<&str> {
    if key.len() > 3 && key[..3].eq_ignore_ascii_case("ce-") {
        Some(&key[3..])
    } else {
        None
    }
}

fn lookup_ci(headers: &BTreeMap<String, String>, name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn non_blank(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// A random 32-hex-char id for a wrap event with no client-supplied id (NON-explicit — a
/// fresh id per request never dedups, so it stays out of the idempotency key).
fn generated_id() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::getrandom(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn mode_parse_defaults_to_auto() {
        assert_eq!(CeMode::parse(None), CeMode::Auto);
        assert_eq!(CeMode::parse(Some("AUTO")), CeMode::Auto);
        assert_eq!(CeMode::parse(Some("binary")), CeMode::Binary);
        assert_eq!(CeMode::parse(Some("structured")), CeMode::Structured);
        assert_eq!(CeMode::parse(Some("wrap")), CeMode::Wrap);
        assert_eq!(CeMode::parse(Some("none")), CeMode::None);
        assert_eq!(CeMode::parse(Some("native")), CeMode::None);
        assert_eq!(CeMode::parse(Some("weird")), CeMode::Auto);
    }

    #[test]
    fn binary_mode_extracts_ce_headers_and_keeps_body_as_data() {
        let h = headers(&[
            ("ce-id", "evt-1"),
            ("ce-source", "/pay/gw"),
            ("ce-type", "payment.captured"),
            ("ce-subject", "acct-9"),
            ("ce-extfoo", "bar"),
        ]);
        let x = extract(
            CeMode::Binary,
            "pay",
            &h,
            Some("application/json"),
            b"{\"amt\":10}",
            WrapDefaults::default(),
        )
        .unwrap();
        let ce = x.cloud_event.expect("ce");
        assert_eq!(ce.id.as_deref(), Some("evt-1"));
        assert_eq!(ce.source.as_deref(), Some("/pay/gw"));
        assert_eq!(ce.event_type.as_deref(), Some("payment.captured"));
        assert_eq!(ce.subject.as_deref(), Some("acct-9"));
        assert_eq!(ce.spec_version.as_deref(), Some("1.0"));
        assert_eq!(ce.data_content_type.as_deref(), Some("application/json"));
        assert_eq!(ce.extensions.get("extfoo").map(String::as_str), Some("bar"));
        assert_eq!(x.body.into_inner(), b"{\"amt\":10}");
        assert_eq!(x.explicit_id.as_deref(), Some("evt-1"));
    }

    #[test]
    fn binary_mode_rejects_missing_required_attribute() {
        let h = headers(&[("ce-id", "evt-1"), ("ce-source", "/s")]); // no ce-type
        let e = extract(
            CeMode::Binary,
            "pay",
            &h,
            None,
            b"x",
            WrapDefaults::default(),
        )
        .unwrap_err();
        assert_eq!(e.code, codes::INBOUND_REJECTED_CLOUDEVENT);
    }

    #[test]
    fn structured_mode_extracts_data_string() {
        let body = br#"{"specversion":"1.0","id":"e2","source":"/s","type":"t",
            "datacontenttype":"text/plain","data":"hello","tenantx":"acme"}"#;
        let x = extract(
            CeMode::Structured,
            "pay",
            &headers(&[]),
            Some("application/cloudevents+json"),
            body,
            WrapDefaults::default(),
        )
        .unwrap();
        let ce = x.cloud_event.expect("ce");
        assert_eq!(ce.id.as_deref(), Some("e2"));
        assert_eq!(ce.data_content_type.as_deref(), Some("text/plain"));
        assert_eq!(
            ce.extensions.get("tenantx").map(String::as_str),
            Some("acme")
        );
        assert_eq!(x.body.into_inner(), b"hello");
        assert_eq!(x.content_type.as_deref(), Some("text/plain"));
        assert_eq!(x.explicit_id.as_deref(), Some("e2"));
    }

    #[test]
    fn structured_mode_decodes_data_base64() {
        let body = br#"{"id":"e3","source":"/s","type":"t","data_base64":"aGVsbG8="}"#;
        let x = extract(
            CeMode::Structured,
            "pay",
            &headers(&[]),
            None,
            body,
            WrapDefaults::default(),
        )
        .unwrap();
        assert_eq!(x.body.into_inner(), b"hello");
    }

    #[test]
    fn structured_mode_object_data_reserialises() {
        let body = br#"{"id":"e4","source":"/s","type":"t","data":{"k":1}}"#;
        let x = extract(
            CeMode::Structured,
            "pay",
            &headers(&[]),
            None,
            body,
            WrapDefaults::default(),
        )
        .unwrap();
        assert_eq!(x.body.into_inner(), br#"{"k":1}"#);
    }

    #[test]
    fn auto_mode_sniffs_structured_then_binary_then_passthrough() {
        // structured via content type
        let structured = extract(
            CeMode::Auto,
            "pay",
            &headers(&[]),
            Some("application/cloudevents+json; charset=utf-8"),
            br#"{"id":"a1","source":"/s","type":"t","data":"x"}"#,
            WrapDefaults::default(),
        )
        .unwrap();
        assert_eq!(structured.cloud_event.unwrap().id.as_deref(), Some("a1"));

        // binary via the four required headers
        let binary = extract(
            CeMode::Auto,
            "pay",
            &headers(&[
                ("ce-id", "a2"),
                ("ce-source", "/s"),
                ("ce-type", "t"),
                ("ce-specversion", "1.0"),
            ]),
            Some("application/json"),
            b"body",
            WrapDefaults::default(),
        )
        .unwrap();
        assert_eq!(binary.cloud_event.unwrap().id.as_deref(), Some("a2"));

        // neither → raw passthrough, no CE
        let raw = extract(
            CeMode::Auto,
            "pay",
            &headers(&[("content-type", "application/json")]),
            Some("application/json"),
            b"plain",
            WrapDefaults::default(),
        )
        .unwrap();
        assert!(raw.cloud_event.is_none());
        assert_eq!(raw.body.into_inner(), b"plain");
        assert!(raw.explicit_id.is_none());
    }

    #[test]
    fn wrap_mode_synthesises_a_ce_and_uses_request_id_when_present() {
        let x = extract(
            CeMode::Wrap,
            "orders",
            &headers(&[("X-Request-Id", "req-77")]),
            Some("application/xml"),
            b"<Doc/>",
            WrapDefaults::default(),
        )
        .unwrap();
        let ce = x.cloud_event.expect("ce");
        assert_eq!(ce.id.as_deref(), Some("req-77"));
        assert_eq!(ce.source.as_deref(), Some("sutra:channel:orders"));
        assert_eq!(ce.event_type.as_deref(), Some("sutra.channel.inbound"));
        assert_eq!(ce.spec_version.as_deref(), Some("1.0"));
        assert_eq!(x.body.into_inner(), b"<Doc/>");
        assert_eq!(
            x.explicit_id.as_deref(),
            Some("req-77"),
            "a client id is explicit"
        );
    }

    #[test]
    fn wrap_mode_generates_a_non_explicit_id_without_a_request_id() {
        let x = extract(
            CeMode::Wrap,
            "orders",
            &headers(&[]),
            None,
            b"raw",
            WrapDefaults {
                source: Some("my:src"),
                event_type: Some("my.type"),
            },
        )
        .unwrap();
        let ce = x.cloud_event.expect("ce");
        assert_eq!(ce.source.as_deref(), Some("my:src"));
        assert_eq!(ce.event_type.as_deref(), Some("my.type"));
        assert!(ce.id.is_some());
        assert!(
            x.explicit_id.is_none(),
            "a generated id never drives dedup (non-explicit)"
        );
    }

    #[test]
    fn none_mode_is_passthrough() {
        let x = extract(
            CeMode::None,
            "pay",
            &headers(&[("ce-id", "x")]),
            Some("text/plain"),
            b"hi",
            WrapDefaults::default(),
        )
        .unwrap();
        assert!(x.cloud_event.is_none());
        assert_eq!(x.body.into_inner(), b"hi");
    }
}
