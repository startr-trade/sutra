//! Why a provider call failed, in a form fit for a log line and a stored diagnostic: the HTTP
//! status, the provider's own error code and a truncated message — read out of the failure
//! detail an adapter returns (`HTTP <status>: <error body>`, see `sutra_llm_spi::http`).
//!
//! What is kept never carries a credential: every URL is passed through
//! [`sutra_llm_spi::redact_url`] and the candidate's resolved key, should a provider echo it,
//! is replaced. Prompts and answers never reach a failure detail in the first place (the
//! adapters build their details from status lines and provider error bodies only).

use std::fmt;

use serde_json::Value;
use sutra_llm_spi::{redact_url, LlmFailure};

/// Longest provider message kept, in characters.
pub const REASON_MESSAGE_LIMIT: usize = 300;

/// The parts of a failure detail an operator needs to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmFailureReason {
    /// The provider's HTTP status, when the call got that far.
    pub http_status: Option<u16>,
    /// The provider's error code (`code`, `status` or `type` of its error object).
    pub provider_code: Option<String>,
    /// The provider's error message (or the adapter's detail), on one line, truncated to
    /// [`REASON_MESSAGE_LIMIT`] characters, with URLs redacted.
    pub message: String,
}

impl LlmFailureReason {
    /// Read `detail` (an [`LlmFailure::detail`]). `secret`, the candidate's resolved
    /// credential, is replaced wherever it appears.
    pub fn from_detail(detail: &str, secret: Option<&str>) -> LlmFailureReason {
        let (http_status, body) = split_status(detail);
        let (provider_code, message) = match body {
            Some(body) => provider_error(body),
            None => (None, detail.to_string()),
        };
        LlmFailureReason {
            http_status,
            provider_code: provider_code
                .map(|code| clip(&scrub(&code, secret), 64))
                .filter(|code| !code.is_empty()),
            message: clip(&scrub(&message, secret), REASON_MESSAGE_LIMIT),
        }
    }

    /// [`Self::from_detail`] of a failure's detail.
    pub fn of(failure: &LlmFailure, secret: Option<&str>) -> LlmFailureReason {
        LlmFailureReason::from_detail(failure.detail(), secret)
    }

    /// The HTTP status as a log field value (`-` when there is none).
    pub fn status_field(&self) -> String {
        self.http_status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string())
    }

    /// The provider code as a log field value (`-` when there is none).
    pub fn code_field(&self) -> &str {
        self.provider_code.as_deref().unwrap_or("-")
    }
}

/// `HTTP 400 (invalid_request_error): <message>`; parts that are missing are left out.
impl fmt::Display for LlmFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.http_status, &self.provider_code) {
            (Some(status), Some(code)) => write!(f, "HTTP {status} ({code}): {}", self.message),
            (Some(status), None) => write!(f, "HTTP {status}: {}", self.message),
            (None, Some(code)) => write!(f, "({code}) {}", self.message),
            (None, None) => f.write_str(&self.message),
        }
    }
}

/// `failure` with every occurrence of `secret` in its detail replaced, so a credential a
/// provider echoes travels no further than the adapter.
pub fn scrub_failure(failure: LlmFailure, secret: Option<&str>) -> LlmFailure {
    let Some(secret) = secret.filter(|s| s.len() >= 4) else {
        return failure;
    };
    if !failure.detail().contains(secret) {
        return failure;
    }
    match failure {
        LlmFailure::Retryable {
            class,
            retry_after,
            detail,
        } => LlmFailure::Retryable {
            class,
            retry_after,
            detail: detail.replace(secret, "<redacted>"),
        },
        LlmFailure::Permanent { detail } => LlmFailure::Permanent {
            detail: detail.replace(secret, "<redacted>"),
        },
    }
}

/// The status and error body of an `HTTP <status>: <body>` detail.
fn split_status(detail: &str) -> (Option<u16>, Option<&str>) {
    let Some(at) = detail.find("HTTP ") else {
        return (None, None);
    };
    let rest = &detail[at + "HTTP ".len()..];
    let Some(status) = rest.get(..3).and_then(|s| s.parse::<u16>().ok()) else {
        return (None, None);
    };
    let body = rest[3..].strip_prefix(':').unwrap_or(&rest[3..]).trim();
    (Some(status), Some(body))
}

/// The code and message of a provider error body: OpenAI-style `{"error": {"message", "type",
/// "code"}}`, Gemini `{"error": {"code", "message", "status"}}` (or a one-element array of
/// it), Anthropic `{"type": "error", "error": {"type", "message"}}`, AWS `{"message"}` with an
/// `__type`. A body cut off at the adapter's limit is not JSON any more; its `"message"` and
/// `"code"` strings are then read as far as they go.
fn provider_error(body: &str) -> (Option<String>, String) {
    let parsed = serde_json::from_str::<Value>(body).ok().map(|v| match v {
        Value::Array(items) => items.into_iter().next().unwrap_or(Value::Null),
        other => other,
    });
    let Some(value) = parsed.filter(Value::is_object) else {
        let code = string_field(body, "code").or_else(|| string_field(body, "type"));
        let message = string_field(body, "message").unwrap_or_else(|| body.to_string());
        return (code, message);
    };
    let error = if value["error"].is_object() {
        &value["error"]
    } else {
        &value
    };
    let text = |v: &Value| v.as_str().map(str::to_string).filter(|s| !s.is_empty());
    let code = text(&error["code"])
        .or_else(|| text(&error["status"]))
        .or_else(|| text(&error["type"]))
        .or_else(|| text(&value["__type"]))
        .or_else(|| error["code"].as_i64().map(|c| c.to_string()));
    let message = text(&error["message"])
        .or_else(|| text(&value["message"]))
        .or_else(|| text(&value["Message"]))
        .or_else(|| text(&value["error"]))
        .unwrap_or_else(|| body.to_string());
    (code, message)
}

/// The string value of `"name": "..."` in text that may not be complete JSON, read up to its
/// closing quote or the end of the text.
fn string_field(text: &str, name: &str) -> Option<String> {
    let key = format!("\"{name}\"");
    let after = &text[text.find(&key)? + key.len()..];
    let after = after.trim_start().strip_prefix(':')?.trim_start();
    let mut chars = after.strip_prefix('"')?.chars();
    let mut value = String::new();
    while let Some(c) = chars.next() {
        match c {
            '"' => break,
            '\\' => match chars.next() {
                Some('n') | Some('t') | Some('r') => value.push(' '),
                Some(other) => value.push(other),
                None => break,
            },
            other => value.push(other),
        }
    }
    Some(value).filter(|v| !v.is_empty())
}

/// `text` with `secret` replaced and every `http(s)://` URL redacted.
fn scrub(text: &str, secret: Option<&str>) -> String {
    let text = match secret.filter(|s| s.len() >= 4) {
        Some(secret) => text.replace(secret, "<redacted>"),
        None => text.to_string(),
    };
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    // The earlier of the two schemes, while either is left.
    let next_url = |s: &str| match (s.find("http://"), s.find("https://")) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    while let Some(at) = next_url(rest) {
        out.push_str(&rest[..at]);
        let url_end = rest[at..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | '`'))
            .map(|end| at + end)
            .unwrap_or(rest.len());
        out.push_str(&redact_url(&rest[at..url_end]));
        rest = &rest[url_end..];
    }
    out.push_str(rest);
    out
}

/// `text` on one line, cut to `limit` characters (marked with `…`).
fn clip(text: &str, limit: usize) -> String {
    let one_line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.chars().count() <= limit {
        return one_line;
    }
    let mut cut: String = one_line.chars().take(limit).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_error_body() {
        let detail = r#"HTTP 400: {"error": {"message": "Invalid parameter: 'response_format' of type 'json_object' is not supported with this model.", "type": "invalid_request_error", "param": "response_format", "code": null}}"#;
        let reason = LlmFailureReason::from_detail(detail, None);
        assert_eq!(reason.http_status, Some(400));
        assert_eq!(
            reason.provider_code.as_deref(),
            Some("invalid_request_error")
        );
        assert!(reason
            .message
            .starts_with("Invalid parameter: 'response_format'"));
        assert!(reason
            .to_string()
            .starts_with("HTTP 400 (invalid_request_error): Invalid parameter"));
    }

    #[test]
    fn gemini_and_anthropic_and_aws_bodies() {
        let gemini = r#"HTTP 404: [{"error": {"code": 404, "message": "models/x is not found", "status": "NOT_FOUND"}}]"#;
        let reason = LlmFailureReason::from_detail(gemini, None);
        assert_eq!(reason.provider_code.as_deref(), Some("NOT_FOUND"));
        assert_eq!(reason.message, "models/x is not found");

        let anthropic = r#"HTTP 401: {"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#;
        let reason = LlmFailureReason::from_detail(anthropic, None);
        assert_eq!(
            reason.provider_code.as_deref(),
            Some("authentication_error")
        );
        assert_eq!(reason.message, "invalid x-api-key");

        let aws =
            r#"HTTP 403: {"message":"The security token included in the request is invalid."}"#;
        let reason = LlmFailureReason::from_detail(aws, None);
        assert_eq!(reason.provider_code, None);
        assert_eq!(
            reason.to_string(),
            "HTTP 403: The security token included in the request is invalid."
        );
    }

    #[test]
    fn truncated_body_still_yields_message() {
        let detail = r#"HTTP 400: {"error": {"message": "Unsupported value: 'temperature' does not support 0.2 with this mo"#;
        let reason = LlmFailureReason::from_detail(detail, None);
        assert_eq!(reason.http_status, Some(400));
        assert_eq!(
            reason.message,
            "Unsupported value: 'temperature' does not support 0.2 with this mo"
        );
    }

    #[test]
    fn non_http_detail_is_kept() {
        let reason =
            LlmFailureReason::from_detail("the provider did not answer before the deadline", None);
        assert_eq!(reason.http_status, None);
        assert_eq!(
            reason.to_string(),
            "the provider did not answer before the deadline"
        );
    }

    #[test]
    fn secrets_and_urls_are_redacted_and_message_is_clipped() {
        let detail = "HTTP 401: {\"error\": {\"message\": \"bad key sk-live-123456 for \
                      https://api.example.test/v1/models?key=sk-live-123456 see docs\"}}";
        let reason = LlmFailureReason::from_detail(detail, Some("sk-live-123456"));
        assert!(!reason.message.contains("sk-live-123456"), "{reason}");
        assert!(reason.message.contains("key=<redacted>"), "{reason}");

        let long = format!("HTTP 500: {{\"message\": \"{}\"}}", "x\n".repeat(400));
        let reason = LlmFailureReason::from_detail(&long, None);
        assert!(reason.message.chars().count() <= REASON_MESSAGE_LIMIT + 1);
        assert!(!reason.message.contains('\n'));
    }

    #[test]
    fn scrub_failure_replaces_an_echoed_key() {
        let failure = LlmFailure::Permanent {
            detail: "HTTP 401: key resolved:OPENAI rejected".into(),
        };
        let scrubbed = scrub_failure(failure, Some("resolved:OPENAI"));
        assert_eq!(scrubbed.detail(), "HTTP 401: key <redacted> rejected");
    }
}
