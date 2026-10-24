//! The model-provider SPI behind the `llm` transport.
//!
//! A process calls a language model through an ordinary channel call; the `llm` transport
//! owns everything that is the same for every provider (routing, fallback, concurrency,
//! reply delivery), and a provider crate owns only what differs: the endpoint, the auth
//! header, the request/response JSON and the failure classification. The two meet here.
//!
//! - [`LlmRequest`] / [`LlmResponse`] / [`LlmFailure`] — the provider-neutral vocabulary.
//! - [`LlmProvider`] — one configured provider + model, answering a request.
//! - [`LlmProviderFactory`] — what a provider crate `inventory::submit!`s, keyed by the
//!   channel's `provider:` value; [`provider_factories`] / [`find_provider`] collect them.
//! - [`JsonHttpClient`] — the shared JSON-over-HTTPS client, so no adapter needs a vendor SDK.
//!
//! This crate names no provider and is held to that by the domain-neutrality gate.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
pub use sutra_channels::{BoxFuture, Diagnostic};

mod http;

pub use http::{HttpFailure, JsonHttpClient, SseEvent, SseParser};

/// Stable diagnostic codes of the `llm` transport and its providers.
pub mod codes {
    /// A channel's `llm:` block is malformed (missing candidates, unknown option, bad value).
    pub const CONFIG_INVALID: &str = "SUTRA.LLM.CONFIG.INVALID";
    /// A candidate names a `provider:` no linked provider crate registered.
    pub const PROVIDER_UNKNOWN: &str = "SUTRA.LLM.PROVIDER.UNKNOWN";
    /// The request body sent to an `llm` channel is not the documented JSON shape.
    pub const REQUEST_INVALID: &str = "SUTRA.LLM.REQUEST.INVALID";
    /// Every candidate failed; the last failure is attached.
    pub const CALL_FAILED: &str = "SUTRA.LLM.CALL.FAILED";
    /// A provider answered with a body the adapter could not map.
    pub const RESPONSE_INVALID: &str = "SUTRA.LLM.RESPONSE.INVALID";
}

/// Who said a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmRole {
    User,
    Assistant,
    /// The result of a tool call the assistant requested ([`LlmMessage::tool_call_id`]).
    Tool,
}

impl LlmRole {
    /// Parse the request-body spelling (`user` / `assistant` / `tool`).
    pub fn parse(value: &str) -> Option<LlmRole> {
        match value {
            "user" => Some(LlmRole::User),
            "assistant" => Some(LlmRole::Assistant),
            "tool" => Some(LlmRole::Tool),
            _ => None,
        }
    }
}

/// A tool invocation the model asked for. `arguments` is a JSON object.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// One conversation turn. Text-only content.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
    /// Set on an assistant turn that requested tools.
    pub tool_calls: Vec<LlmToolCall>,
    /// Set on a [`LlmRole::Tool`] turn: the [`LlmToolCall::id`] it answers.
    pub tool_call_id: Option<String>,
}

impl LlmMessage {
    pub fn user(content: impl Into<String>) -> LlmMessage {
        LlmMessage {
            role: LlmRole::User,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }
}

/// A tool the model may call. `parameters` is a JSON Schema object.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// A provider-neutral completion request.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmRequest {
    /// The provider's own model id, as the channel candidate names it.
    pub model: String,
    pub system: Option<String>,
    pub messages: Vec<LlmMessage>,
    pub tools: Vec<LlmToolSpec>,
    /// Ask for a JSON object as the answer (the provider's JSON mode, or the nearest it has).
    pub json_output: bool,
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    /// The whole-call deadline; the adapter passes it to [`JsonHttpClient::post_json`].
    pub deadline: Duration,
}

/// Why the model stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishReason {
    Stop,
    /// Hit the output-token limit.
    Length,
    ToolCalls,
    /// The provider withheld or cut the answer on content grounds. A RESULT, not a failure:
    /// asking again spends money for the same answer.
    Filtered,
    Other(String),
}

impl FinishReason {
    /// The reply-body spelling.
    pub fn as_str(&self) -> &str {
        match self {
            FinishReason::Stop => "stop",
            FinishReason::Length => "length",
            FinishReason::ToolCalls => "tool-calls",
            FinishReason::Filtered => "filtered",
            FinishReason::Other(other) => other,
        }
    }
}

/// Token counts as REPORTED by the provider (never counted locally).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LlmUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// A provider-neutral completion.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmResponse {
    pub text: String,
    pub tool_calls: Vec<LlmToolCall>,
    pub finish: FinishReason,
    pub usage: LlmUsage,
    pub provider_request_id: Option<String>,
}

/// The transient failure classes a channel's `fallback-on:` list selects from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryableClass {
    /// HTTP 429 or the provider's overload signal.
    RateLimited,
    /// 5xx, connection refused/reset.
    Unavailable,
    /// The deadline elapsed.
    Timeout,
}

impl RetryableClass {
    /// Parse the `fallback-on:` spelling (`rate-limited` / `unavailable` / `timeout`).
    pub fn parse(value: &str) -> Option<RetryableClass> {
        match value {
            "rate-limited" => Some(RetryableClass::RateLimited),
            "unavailable" => Some(RetryableClass::Unavailable),
            "timeout" => Some(RetryableClass::Timeout),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RetryableClass::RateLimited => "rate-limited",
            RetryableClass::Unavailable => "unavailable",
            RetryableClass::Timeout => "timeout",
        }
    }
}

/// A failed call. `detail` never carries prompt or answer text.
#[derive(Debug, Clone, PartialEq)]
pub enum LlmFailure {
    /// Transient: another candidate, or a later attempt, may succeed.
    Retryable {
        class: RetryableClass,
        retry_after: Option<Duration>,
        detail: String,
    },
    /// Retrying can never fix it (4xx contract reject, bad key, unknown model, unmappable body).
    Permanent { detail: String },
}

impl LlmFailure {
    pub fn detail(&self) -> &str {
        match self {
            LlmFailure::Retryable { detail, .. } | LlmFailure::Permanent { detail } => detail,
        }
    }
}

/// What a configured provider + model can do; candidates lacking a channel's `require:`
/// capability are skipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LlmCapabilities {
    pub tools: bool,
    pub json_output: bool,
    /// [`LlmProvider::submit`] / [`LlmProvider::poll`] are implemented (a `mode: job` candidate).
    pub async_jobs: bool,
    /// [`LlmProvider::parse_callback`] / [`LlmProvider::verify_callback`] are implemented
    /// (`completion: callback`).
    pub job_callbacks: bool,
    /// [`LlmProvider::complete_streaming`] streams for real rather than yielding one chunk.
    pub streaming: bool,
}

/// A submitted provider job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmJobRef {
    pub id: String,
}

/// A job's state as the provider reports it.
#[derive(Debug, Clone, PartialEq)]
pub enum LlmJobState {
    Pending,
    Done(LlmResponse),
    Failed(LlmFailure),
}

/// What a provider callback says. `state: None` means the notification carries no result and
/// the transport fetches it with [`LlmProvider::poll`].
#[derive(Debug, Clone, PartialEq)]
pub struct LlmCallbackEvent {
    pub job: LlmJobRef,
    pub state: Option<LlmJobState>,
}

/// Receives streamed answer text as it arrives.
pub type ChunkSink<'a> = &'a (dyn Fn(&str) + Send + Sync);

fn unsupported(what: &str) -> LlmFailure {
    LlmFailure::Permanent {
        detail: format!("this provider does not support {what}"),
    }
}

/// One configured provider + model.
pub trait LlmProvider: Send + Sync {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>>;
    fn capabilities(&self) -> LlmCapabilities;

    /// Stream the answer: `on_chunk` receives text deltas as they arrive, and the accumulated
    /// response is returned at the end. The default calls [`Self::complete`] and yields the whole
    /// text as one chunk, so a provider without streaming still works on a streaming task.
    fn complete_streaming<'a>(
        &'a self,
        request: &'a LlmRequest,
        on_chunk: ChunkSink<'a>,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let response = self.complete(request).await?;
            if !response.text.is_empty() {
                on_chunk(&response.text);
            }
            Ok(response)
        })
    }

    /// Submit a job that completes later.
    fn submit<'a>(
        &'a self,
        _request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmJobRef, LlmFailure>> {
        Box::pin(async { Err(unsupported("async jobs")) })
    }

    /// Ask for a submitted job's state (also used to fetch a result a callback only announced).
    fn poll<'a>(&'a self, _job: &'a LlmJobRef) -> BoxFuture<'a, Result<LlmJobState, LlmFailure>> {
        Box::pin(async { Err(unsupported("async jobs")) })
    }

    /// Parse a provider callback (a webhook body, or a broker event body).
    fn parse_callback(
        &self,
        _headers: &BTreeMap<String, String>,
        _body: &[u8],
    ) -> Result<LlmCallbackEvent, LlmFailure> {
        Err(unsupported("job callbacks"))
    }

    /// Check a callback's signature with the candidate's resolved callback secret. Called before
    /// [`Self::parse_callback`]; `false` rejects the callback.
    fn verify_callback(
        &self,
        _headers: &BTreeMap<String, String>,
        _body: &[u8],
        _secret: &str,
    ) -> bool {
        false
    }
}

/// One channel candidate's provider settings, secrets already resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmProviderConfig {
    pub model: String,
    /// Overrides the provider's default endpoint (a compatible server, a proxy, a test stub).
    pub base_url: Option<String>,
    /// The resolved credential; `None` when the candidate declares none.
    pub api_key: Option<String>,
}

/// A provider crate's self-registration.
pub struct LlmProviderFactory {
    /// The channel candidate's `provider:` value.
    pub provider: &'static str,
    pub build: fn(&LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic>,
}

inventory::collect!(LlmProviderFactory);

/// Every registered [`LlmProviderFactory`], sorted by `provider`.
pub fn provider_factories() -> Vec<&'static LlmProviderFactory> {
    let mut factories: Vec<&'static LlmProviderFactory> =
        inventory::iter::<LlmProviderFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.provider);
    factories
}

/// The factory registered for `provider`, if any.
pub fn find_provider(provider: &str) -> Option<&'static LlmProviderFactory> {
    inventory::iter::<LlmProviderFactory>
        .into_iter()
        .find(|f| f.provider == provider)
}

/// Map a non-2xx provider status to a failure. 408 / 504 → timeout, 429 / 529 → rate-limited,
/// other 5xx → unavailable, every other status → permanent.
pub fn classify_status(status: u16, retry_after: Option<Duration>, detail: String) -> LlmFailure {
    let class = match status {
        408 | 504 => RetryableClass::Timeout,
        429 | 529 => RetryableClass::RateLimited,
        500..=599 => RetryableClass::Unavailable,
        _ => return LlmFailure::Permanent { detail },
    };
    LlmFailure::Retryable {
        class,
        retry_after,
        detail,
    }
}

impl From<HttpFailure> for LlmFailure {
    fn from(failure: HttpFailure) -> LlmFailure {
        match failure {
            HttpFailure::Status {
                status,
                retry_after,
                detail,
            } => classify_status(status, retry_after, detail),
            HttpFailure::Timeout => LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the provider did not answer before the deadline".into(),
            },
            HttpFailure::Connect(detail) => LlmFailure::Retryable {
                class: RetryableClass::Unavailable,
                retry_after: None,
                detail,
            },
            HttpFailure::Invalid(detail) => LlmFailure::Permanent { detail },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classification_table() {
        let class = |s| match classify_status(s, None, String::new()) {
            LlmFailure::Retryable { class, .. } => Some(class),
            LlmFailure::Permanent { .. } => None,
        };
        assert_eq!(class(429), Some(RetryableClass::RateLimited));
        assert_eq!(class(529), Some(RetryableClass::RateLimited));
        assert_eq!(class(408), Some(RetryableClass::Timeout));
        assert_eq!(class(504), Some(RetryableClass::Timeout));
        assert_eq!(class(500), Some(RetryableClass::Unavailable));
        assert_eq!(class(503), Some(RetryableClass::Unavailable));
        assert_eq!(class(400), None);
        assert_eq!(class(401), None);
        assert_eq!(class(404), None);
    }

    #[test]
    fn fallback_class_spellings_round_trip() {
        for class in [
            RetryableClass::RateLimited,
            RetryableClass::Unavailable,
            RetryableClass::Timeout,
        ] {
            assert_eq!(RetryableClass::parse(class.as_str()), Some(class));
        }
        assert_eq!(RetryableClass::parse("refused"), None);
    }
}
