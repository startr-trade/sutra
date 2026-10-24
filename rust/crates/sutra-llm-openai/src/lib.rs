//! The OpenAI-compatible Chat Completions provider for the `llm` transport.
//!
//! Registers `provider: openai`, plus one `provider:` id per hosted service that speaks the same
//! wire format at a known endpoint (see [`COMPATIBLE_SERVICES`]) — the id only selects the
//! default `base_url`. Any other server speaking the same wire format works by setting
//! the candidate's `base_url`. The request/response mapping is pure ([`request_body`],
//! [`parse_response`]); only [`OpenAiProvider::complete`] touches the network.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use serde_json::{json, Map, Value};
use sutra_llm_spi::{
    codes, BoxFuture, ChunkSink, Diagnostic, FinishReason, JsonHttpClient, LlmCallbackEvent,
    LlmCapabilities, LlmFailure, LlmJobRef, LlmJobState, LlmProvider, LlmProviderConfig,
    LlmProviderFactory, LlmRequest, LlmResponse, LlmRole, LlmToolCall, LlmUsage,
};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

struct OpenAiProvider {
    url: String,
    /// `<base>/responses` — the background-job endpoint (the `openai` provider only).
    responses_url: String,
    /// True only for the `openai` provider: jobs, callbacks and `stream_options` are OpenAI's,
    /// not part of the Chat Completions wire format the other services share.
    first_party: bool,
    api_key: Option<String>,
    client: JsonHttpClient,
}

/// Hosted services that speak the Chat Completions wire format, by `provider:` id → default
/// endpoint. Each is registered as its own provider so a channel names the service it means;
/// `base-url` still overrides the endpoint (a regional host, a proxy, a test stub).
pub const COMPATIBLE_SERVICES: &[(&str, &str)] = &[
    ("openai", DEFAULT_BASE_URL),
    ("mistral", "https://api.mistral.ai/v1"),
    ("deepseek", "https://api.deepseek.com"),
    ("xai", "https://api.x.ai/v1"),
    ("kimi", "https://api.moonshot.ai/v1"),
    ("meta-llama", "https://api.llama.com/compat/v1"),
    (
        "google-gemini",
        "https://generativelanguage.googleapis.com/v1beta/openai",
    ),
];

fn default_base_url(provider: &str) -> &'static str {
    COMPATIBLE_SERVICES
        .iter()
        .find(|(id, _)| *id == provider)
        .map(|(_, url)| *url)
        .unwrap_or(DEFAULT_BASE_URL)
}

fn build_for(
    provider: &str,
    config: &LlmProviderConfig,
) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    if config.model.trim().is_empty() {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            format!("the {provider} provider requires a non-empty 'model'"),
        ));
    }
    let base = config
        .base_url
        .as_deref()
        .unwrap_or(default_base_url(provider))
        .trim_end_matches('/');
    Ok(Arc::new(OpenAiProvider {
        url: format!("{base}/chat/completions"),
        responses_url: format!("{base}/responses"),
        first_party: provider == "openai",
        api_key: config.api_key.clone(),
        client: JsonHttpClient::new(Duration::from_secs(5)),
    }))
}

fn build(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    build_for("openai", config)
}

/// One `LlmProviderFactory` per compatible service (a factory's `build` is a plain fn pointer,
/// so each id needs its own fn).
macro_rules! compatible_service {
    ($fn_name:ident, $id:literal) => {
        fn $fn_name(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
            build_for($id, config)
        }
        inventory::submit! {
            LlmProviderFactory { provider: $id, build: $fn_name }
        }
    };
}

inventory::submit! {
    LlmProviderFactory { provider: "openai", build }
}
compatible_service!(build_mistral, "mistral");
compatible_service!(build_deepseek, "deepseek");
compatible_service!(build_xai, "xai");
compatible_service!(build_kimi, "kimi");
compatible_service!(build_meta_llama, "meta-llama");
compatible_service!(build_google_gemini, "google-gemini");

impl LlmProvider for OpenAiProvider {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let body = request_body(request);
            let auth = self.api_key.as_ref().map(|key| format!("Bearer {key}"));
            let mut headers: Vec<(&str, &str)> = Vec::new();
            if let Some(auth) = &auth {
                headers.push(("authorization", auth));
            }
            let value = self
                .client
                .post_json(&self.url, &headers, &body, request.deadline)
                .await
                .map_err(LlmFailure::from)?;
            parse_response(&value)
        })
    }

    fn capabilities(&self) -> LlmCapabilities {
        LlmCapabilities {
            tools: true,
            json_output: true,
            streaming: true,
            async_jobs: self.first_party,
            job_callbacks: self.first_party,
        }
    }

    fn complete_streaming<'a>(
        &'a self,
        request: &'a LlmRequest,
        on_chunk: ChunkSink<'a>,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let mut body = request_body(request);
            body["stream"] = json!(true);
            if self.first_party {
                body["stream_options"] = json!({"include_usage": true});
            }
            let auth = self.auth();
            let headers = auth_headers(&auth);
            let mut stream = ChatStream::default();
            self.client
                .post_sse(&self.url, &headers, &body, request.deadline, &mut |event| {
                    stream.feed(&event.data, on_chunk)
                })
                .await
                .map_err(LlmFailure::from)?;
            stream.finish()
        })
    }

    fn submit<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmJobRef, LlmFailure>> {
        Box::pin(async move {
            self.require_jobs()?;
            let auth = self.auth();
            let value = self
                .client
                .post_json(
                    &self.responses_url,
                    &auth_headers(&auth),
                    &responses_body(request),
                    request.deadline,
                )
                .await
                .map_err(LlmFailure::from)?;
            let id = value["id"].as_str().ok_or_else(|| LlmFailure::Permanent {
                detail: format!("{}: the job answer has no 'id'", codes::RESPONSE_INVALID),
            })?;
            Ok(LlmJobRef { id: id.to_string() })
        })
    }

    fn poll<'a>(&'a self, job: &'a LlmJobRef) -> BoxFuture<'a, Result<LlmJobState, LlmFailure>> {
        Box::pin(async move {
            self.require_jobs()?;
            let auth = self.auth();
            let url = format!("{}/{}", self.responses_url, job.id);
            let value = self
                .client
                .get_json(&url, &auth_headers(&auth), POLL_DEADLINE)
                .await
                .map_err(LlmFailure::from)?;
            parse_job_state(&value)
        })
    }

    fn parse_callback(
        &self,
        _headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<LlmCallbackEvent, LlmFailure> {
        self.require_jobs()?;
        parse_webhook(body)
    }

    fn verify_callback(
        &self,
        headers: &BTreeMap<String, String>,
        body: &[u8],
        secret: &str,
    ) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.first_party && verify_webhook(headers, body, secret, now)
    }
}

/// A job poll is a short GET; the job itself has no deadline.
const POLL_DEADLINE: Duration = Duration::from_secs(30);
/// Standard Webhooks replay window.
const WEBHOOK_TOLERANCE_SECS: u64 = 300;

impl OpenAiProvider {
    fn auth(&self) -> Option<String> {
        self.api_key.as_ref().map(|key| format!("Bearer {key}"))
    }

    fn require_jobs(&self) -> Result<(), LlmFailure> {
        if self.first_party {
            Ok(())
        } else {
            Err(LlmFailure::Permanent {
                detail: "this provider does not support async jobs or callbacks".into(),
            })
        }
    }
}

fn auth_headers(auth: &Option<String>) -> Vec<(&str, &str)> {
    auth.as_deref()
        .map(|a| vec![("authorization", a)])
        .unwrap_or_default()
}

fn failure(detail: String) -> LlmFailure {
    LlmFailure::Permanent {
        detail: format!("{}: {detail}", codes::RESPONSE_INVALID),
    }
}

/// Accumulates a Chat Completions SSE stream into a response.
#[derive(Default)]
struct ChatStream {
    text: String,
    /// Tool calls by stream `index`: (id, name, argument text).
    calls: BTreeMap<u64, (String, String, String)>,
    finish: Option<String>,
    usage: LlmUsage,
    id: Option<String>,
    error: Option<String>,
    done: bool,
}

impl ChatStream {
    fn feed(&mut self, data: &str, on_chunk: ChunkSink<'_>) {
        if data.trim() == "[DONE]" {
            self.done = true;
            return;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(data) else {
            return;
        };
        if let Some(error) = chunk.get("error") {
            self.error = Some(
                error["message"]
                    .as_str()
                    .unwrap_or("the provider reported a stream error")
                    .to_string(),
            );
            return;
        }
        if let Some(id) = chunk["id"].as_str() {
            self.id.get_or_insert_with(|| id.to_string());
        }
        if let Some(usage) = chunk.get("usage").filter(|u| u.is_object()) {
            self.usage = LlmUsage {
                input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
                output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
            };
        }
        let Some(choice) = chunk["choices"].as_array().and_then(|c| c.first()) else {
            return;
        };
        let delta = &choice["delta"];
        if let Some(text) = delta["content"].as_str().filter(|t| !t.is_empty()) {
            self.text.push_str(text);
            on_chunk(text);
        }
        for call in delta["tool_calls"].as_array().into_iter().flatten() {
            let entry = self
                .calls
                .entry(call["index"].as_u64().unwrap_or(0))
                .or_default();
            if let Some(id) = call["id"].as_str() {
                entry.0 = id.to_string();
            }
            if let Some(name) = call["function"]["name"].as_str() {
                entry.1.push_str(name);
            }
            if let Some(args) = call["function"]["arguments"].as_str() {
                entry.2.push_str(args);
            }
        }
        if let Some(reason) = choice["finish_reason"].as_str() {
            self.finish = Some(reason.to_string());
        }
    }

    fn finish(self) -> Result<LlmResponse, LlmFailure> {
        if let Some(error) = self.error {
            return Err(failure(format!("stream error: {error}")));
        }
        if !self.done && self.finish.is_none() {
            return Err(failure("the stream ended before a finish reason".into()));
        }
        Ok(LlmResponse {
            text: self.text,
            tool_calls: self
                .calls
                .into_values()
                .map(|(id, name, args)| LlmToolCall {
                    id,
                    name,
                    arguments: serde_json::from_str(&args).unwrap_or(Value::String(args)),
                })
                .collect(),
            finish: map_finish(self.finish.as_deref()),
            usage: self.usage,
            provider_request_id: self.id,
        })
    }
}

fn map_finish(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("tool_calls") => FinishReason::ToolCalls,
        Some("content_filter") => FinishReason::Filtered,
        Some(other) => FinishReason::Other(other.to_string()),
        None => FinishReason::Other("unknown".to_string()),
    }
}

/// Map a neutral request to a Responses API body for a background job.
fn responses_body(request: &LlmRequest) -> Value {
    let mut input = Vec::new();
    for message in &request.messages {
        match message.role {
            LlmRole::User => input.push(json!({"role": "user", "content": message.content})),
            LlmRole::Assistant => {
                if !message.content.is_empty() {
                    input.push(json!({"role": "assistant", "content": message.content}));
                }
                for call in &message.tool_calls {
                    input.push(json!({
                        "type": "function_call",
                        "call_id": call.id,
                        "name": call.name,
                        "arguments": call.arguments.to_string(),
                    }));
                }
            }
            LlmRole::Tool => input.push(json!({
                "type": "function_call_output",
                "call_id": message.tool_call_id,
                "output": message.content,
            })),
        }
    }
    let mut body = Map::new();
    body.insert("model".into(), json!(request.model));
    body.insert("input".into(), Value::Array(input));
    body.insert("background".into(), json!(true));
    // Not required by background mode (store=false keeps the response ~10 minutes for polling),
    // but an explicit true retains it beyond the polling window so a late webhook can still fetch it.
    body.insert("store".into(), json!(true));
    if let Some(system) = &request.system {
        body.insert("instructions".into(), json!(system));
    }
    if let Some(max) = request.max_output_tokens {
        body.insert("max_output_tokens".into(), json!(max));
    }
    if let Some(temperature) = request.temperature {
        body.insert("temperature".into(), json!(temperature));
    }
    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                })
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    if request.json_output {
        body.insert("text".into(), json!({"format": {"type": "json_object"}}));
    }
    Value::Object(body)
}

/// Map a Responses API object (from `GET /responses/{id}`) to a job state.
fn parse_job_state(value: &Value) -> Result<LlmJobState, LlmFailure> {
    let status = value["status"].as_str().unwrap_or("");
    match status {
        "queued" | "in_progress" => Ok(LlmJobState::Pending),
        "completed" | "incomplete" => {
            let mut text = String::new();
            let mut tool_calls = Vec::new();
            let mut refused = false;
            for item in value["output"].as_array().into_iter().flatten() {
                match item["type"].as_str() {
                    Some("message") => {
                        for part in item["content"].as_array().into_iter().flatten() {
                            match part["type"].as_str() {
                                Some("output_text") => {
                                    text.push_str(part["text"].as_str().unwrap_or(""))
                                }
                                Some("refusal") => refused = true,
                                _ => {}
                            }
                        }
                    }
                    Some("function_call") => {
                        let raw = item["arguments"].as_str().unwrap_or("");
                        tool_calls.push(LlmToolCall {
                            id: item["call_id"].as_str().unwrap_or("").to_string(),
                            name: item["name"].as_str().unwrap_or("").to_string(),
                            arguments: serde_json::from_str(raw)
                                .unwrap_or_else(|_| Value::String(raw.to_string())),
                        });
                    }
                    _ => {}
                }
            }
            let finish = match value["incomplete_details"]["reason"].as_str() {
                Some("max_output_tokens") => FinishReason::Length,
                Some("content_filter") => FinishReason::Filtered,
                Some(other) => FinishReason::Other(other.to_string()),
                None if refused => FinishReason::Filtered,
                None if !tool_calls.is_empty() => FinishReason::ToolCalls,
                None => FinishReason::Stop,
            };
            Ok(LlmJobState::Done(LlmResponse {
                text,
                tool_calls,
                finish,
                usage: LlmUsage {
                    input_tokens: value["usage"]["input_tokens"].as_u64().unwrap_or(0),
                    output_tokens: value["usage"]["output_tokens"].as_u64().unwrap_or(0),
                },
                provider_request_id: value["id"].as_str().map(str::to_string),
            }))
        }
        "failed" => Ok(LlmJobState::Failed(LlmFailure::Permanent {
            detail: format!(
                "the job failed: {}",
                value["error"]["message"].as_str().unwrap_or("no detail")
            ),
        })),
        "cancelled" => Ok(LlmJobState::Failed(LlmFailure::Permanent {
            detail: "the job was cancelled".into(),
        })),
        other => Err(failure(format!("unknown job status '{other}'"))),
    }
}

/// Read an OpenAI webhook body: a terminal response event names the job; the result is
/// fetched with `poll`.
fn parse_webhook(body: &[u8]) -> Result<LlmCallbackEvent, LlmFailure> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| failure(format!("the callback body is not JSON: {e}")))?;
    match value["type"].as_str() {
        Some(
            "response.completed" | "response.failed" | "response.cancelled" | "response.incomplete",
        ) => {}
        other => {
            return Err(failure(format!(
                "callback event type {other:?} does not announce a finished response"
            )))
        }
    }
    let id = value["data"]["id"]
        .as_str()
        .ok_or_else(|| failure("the callback has no data.id".into()))?;
    Ok(LlmCallbackEvent {
        job: LlmJobRef { id: id.to_string() },
        state: None,
    })
}

fn header<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Standard Webhooks verification: HMAC-SHA256 over `id.timestamp.body`, keyed with the
/// base64 secret (after an optional `whsec_` prefix); any listed `v1,<sig>` may match.
fn verify_webhook(
    headers: &BTreeMap<String, String>,
    body: &[u8],
    secret: &str,
    now_secs: u64,
) -> bool {
    let (Some(id), Some(timestamp), Some(signatures)) = (
        header(headers, "webhook-id"),
        header(headers, "webhook-timestamp"),
        header(headers, "webhook-signature"),
    ) else {
        return false;
    };
    let Ok(sent) = timestamp.trim().parse::<u64>() else {
        return false;
    };
    if now_secs.abs_diff(sent) > WEBHOOK_TOLERANCE_SECS {
        return false;
    }
    let raw = secret.strip_prefix("whsec_").unwrap_or(secret);
    let key = BASE64
        .decode(raw)
        .unwrap_or_else(|_| raw.as_bytes().to_vec());
    signatures
        .split_whitespace()
        .filter_map(|entry| entry.strip_prefix("v1,"))
        .filter_map(|sig| BASE64.decode(sig).ok())
        .any(|sig| {
            let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&key) else {
                return false;
            };
            mac.update(id.as_bytes());
            mac.update(b".");
            mac.update(timestamp.as_bytes());
            mac.update(b".");
            mac.update(body);
            mac.verify_slice(&sig).is_ok()
        })
}

/// Map a provider-neutral request to a Chat Completions body.
fn request_body(request: &LlmRequest) -> Value {
    let mut messages = Vec::new();
    if let Some(system) = &request.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    for message in &request.messages {
        messages.push(match message.role {
            LlmRole::User => json!({"role": "user", "content": message.content}),
            LlmRole::Assistant => {
                let mut turn = json!({"role": "assistant", "content": message.content});
                if !message.tool_calls.is_empty() {
                    turn["tool_calls"] = message
                        .tool_calls
                        .iter()
                        .map(|call| {
                            json!({
                                "id": call.id,
                                "type": "function",
                                "function": {
                                    "name": call.name,
                                    "arguments": call.arguments.to_string(),
                                },
                            })
                        })
                        .collect();
                }
                turn
            }
            LlmRole::Tool => json!({
                "role": "tool",
                "tool_call_id": message.tool_call_id,
                "content": message.content,
            }),
        });
    }
    let mut body = Map::new();
    body.insert("model".into(), json!(request.model));
    body.insert("messages".into(), Value::Array(messages));
    if let Some(max) = request.max_output_tokens {
        body.insert("max_tokens".into(), json!(max));
    }
    if let Some(temperature) = request.temperature {
        body.insert("temperature".into(), json!(temperature));
    }
    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    },
                })
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    if request.json_output {
        body.insert("response_format".into(), json!({"type": "json_object"}));
    }
    Value::Object(body)
}

/// Map a Chat Completions answer to a provider-neutral response.
fn parse_response(value: &Value) -> Result<LlmResponse, LlmFailure> {
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| LlmFailure::Permanent {
            detail: format!(
                "{}: the answer has no 'choices' entry",
                codes::RESPONSE_INVALID
            ),
        })?;
    let message = &choice["message"];
    let text = message["content"].as_str().unwrap_or("").to_string();
    let tool_calls = message["tool_calls"]
        .as_array()
        .map(|calls| {
            calls
                .iter()
                .map(|call| {
                    let function = &call["function"];
                    let raw = function["arguments"].as_str().unwrap_or("");
                    LlmToolCall {
                        id: call["id"].as_str().unwrap_or("").to_string(),
                        name: function["name"].as_str().unwrap_or("").to_string(),
                        arguments: serde_json::from_str(raw)
                            .unwrap_or_else(|_| Value::String(raw.to_string())),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let finish = map_finish(choice["finish_reason"].as_str());
    let usage = LlmUsage {
        input_tokens: value["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
        output_tokens: value["usage"]["completion_tokens"].as_u64().unwrap_or(0),
    };
    Ok(LlmResponse {
        text,
        tool_calls,
        finish,
        usage,
        provider_request_id: value["id"].as_str().map(str::to_string),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deepseek_default_base_url_has_no_version_suffix() {
        assert_eq!(default_base_url("deepseek"), "https://api.deepseek.com");
    }

    use sutra_llm_spi::{LlmMessage, LlmToolSpec, RetryableClass};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn request() -> LlmRequest {
        LlmRequest {
            model: "test-model".into(),
            system: None,
            messages: vec![LlmMessage::user("hi")],
            tools: Vec::new(),
            json_output: false,
            max_output_tokens: None,
            temperature: None,
            deadline: Duration::from_secs(5),
        }
    }

    #[test]
    fn minimal_request_has_only_model_and_messages() {
        let body = request_body(&request());
        assert_eq!(
            body,
            json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]})
        );
    }

    #[test]
    fn system_limits_tools_and_json_are_mapped() {
        let mut req = request();
        req.system = Some("be brief".into());
        req.max_output_tokens = Some(64);
        req.temperature = Some(0.5);
        req.json_output = true;
        req.tools = vec![LlmToolSpec {
            name: "lookup".into(),
            description: "find a thing".into(),
            parameters: json!({"type": "object"}),
        }];
        let body = request_body(&req);
        assert_eq!(
            body["messages"][0],
            json!({"role": "system", "content": "be brief"})
        );
        assert_eq!(body["messages"][1]["role"], "user");
        assert_eq!(body["max_tokens"], 64);
        assert_eq!(body["temperature"], 0.5);
        assert_eq!(body["response_format"], json!({"type": "json_object"}));
        assert_eq!(
            body["tools"][0],
            json!({"type": "function", "function": {
                "name": "lookup", "description": "find a thing",
                "parameters": {"type": "object"}}})
        );
    }

    #[test]
    fn tool_turns_are_mapped() {
        let mut req = request();
        req.messages.push(LlmMessage {
            role: LlmRole::Assistant,
            content: String::new(),
            tool_calls: vec![LlmToolCall {
                id: "c1".into(),
                name: "lookup".into(),
                arguments: json!({"q": 1}),
            }],
            tool_call_id: None,
        });
        req.messages.push(LlmMessage {
            role: LlmRole::Tool,
            content: "found".into(),
            tool_calls: Vec::new(),
            tool_call_id: Some("c1".into()),
        });
        let body = request_body(&req);
        let call = &body["messages"][1]["tool_calls"][0];
        assert_eq!(call["id"], "c1");
        assert_eq!(call["type"], "function");
        assert_eq!(call["function"]["name"], "lookup");
        assert_eq!(call["function"]["arguments"], r#"{"q":1}"#);
        assert_eq!(
            body["messages"][2],
            json!({"role": "tool", "tool_call_id": "c1", "content": "found"})
        );
    }

    #[test]
    fn text_answer_with_usage_and_id() {
        let response = parse_response(&json!({
            "id": "req-1",
            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 5},
        }))
        .unwrap();
        assert_eq!(response.text, "hello");
        assert_eq!(response.finish, FinishReason::Stop);
        assert_eq!(response.usage.input_tokens, 3);
        assert_eq!(response.usage.output_tokens, 5);
        assert_eq!(response.provider_request_id.as_deref(), Some("req-1"));
    }

    #[test]
    fn tool_calls_are_parsed_and_null_content_is_empty() {
        let response = parse_response(&json!({
            "choices": [{"message": {"content": null, "tool_calls": [
                {"id": "a", "type": "function",
                 "function": {"name": "f", "arguments": "{\"x\":2}"}},
                {"id": "b", "type": "function",
                 "function": {"name": "g", "arguments": "not json"}},
            ]}, "finish_reason": "tool_calls"}],
        }))
        .unwrap();
        assert_eq!(response.text, "");
        assert_eq!(response.finish, FinishReason::ToolCalls);
        assert_eq!(response.tool_calls[0].arguments, json!({"x": 2}));
        assert_eq!(response.tool_calls[1].arguments, json!("not json"));
        assert_eq!(response.tool_calls[1].name, "g");
    }

    #[test]
    fn finish_reasons_map() {
        let finish = |reason: &str| {
            parse_response(&json!({"choices": [{"message": {"content": "x"},
                "finish_reason": reason}]}))
            .unwrap()
            .finish
        };
        assert_eq!(finish("length"), FinishReason::Length);
        assert_eq!(finish("content_filter"), FinishReason::Filtered);
        assert_eq!(finish("weird"), FinishReason::Other("weird".into()));
    }

    #[test]
    fn missing_choices_is_permanent() {
        for body in [json!({}), json!({"choices": []})] {
            match parse_response(&body) {
                Err(LlmFailure::Permanent { detail }) => {
                    assert!(detail.contains(codes::RESPONSE_INVALID), "{detail}")
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn build_requires_a_model() {
        let config = LlmProviderConfig {
            model: " ".into(),
            base_url: None,
            api_key: None,
        };
        let err = build(&config).err().expect("must fail");
        assert_eq!(err.code, codes::CONFIG_INVALID);
    }

    /// One-shot stub: answers `status` + `body` and hands back the raw request it received.
    async fn stub(status: u16, body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16384];
            let n = socket.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        (format!("http://{addr}/v1/"), handle)
    }

    fn provider(base_url: String) -> Arc<dyn LlmProvider> {
        build(&LlmProviderConfig {
            model: "test-model".into(),
            base_url: Some(base_url),
            api_key: Some("test-key".into()),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn round_trip_sends_bearer_and_maps_the_answer() {
        let (url, seen) = stub(
            200,
            r#"{"id":"r1","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"#,
        )
        .await;
        let response = provider(url).complete(&request()).await.unwrap();
        assert_eq!(response.text, "ok");
        let raw = seen.await.unwrap().to_lowercase();
        assert!(raw.starts_with("post /v1/chat/completions"), "{raw}");
        assert!(raw.contains("authorization: bearer test-key"), "{raw}");
    }

    #[tokio::test]
    async fn rate_limit_is_retryable() {
        let (url, _) = stub(429, r#"{"error":"slow"}"#).await;
        match provider(url).complete(&request()).await {
            Err(LlmFailure::Retryable { class, .. }) => {
                assert_eq!(class, RetryableClass::RateLimited)
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn registers_the_openai_factory() {
        assert!(sutra_llm_spi::find_provider("openai").is_some());
    }

    #[test]
    fn registers_every_compatible_service_with_its_endpoint() {
        for (id, url) in COMPATIBLE_SERVICES {
            assert!(
                sutra_llm_spi::find_provider(id).is_some(),
                "{id} not registered"
            );
            assert_eq!(default_base_url(id), *url);
            assert!(url.starts_with("https://"), "{id}");
        }
    }

    const CHAT_SSE: &str = concat!(
        "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"t1\",\"function\":{\"name\":\"look\",\"arguments\":\"{\\\"q\\\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"up\",\"arguments\":\":1}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":6}}\n\n",
        "data: [DONE]\n\n",
    );

    fn run_stream(sse: &str) -> (Result<LlmResponse, LlmFailure>, Vec<String>) {
        let chunks = std::sync::Mutex::new(Vec::new());
        let sink = |t: &str| chunks.lock().unwrap().push(t.to_string());
        let mut parser = sutra_llm_spi::SseParser::new();
        let mut stream = ChatStream::default();
        for event in parser.feed(sse.as_bytes()) {
            stream.feed(&event.data, &sink);
        }
        (stream.finish(), chunks.into_inner().unwrap())
    }

    #[test]
    fn chat_stream_accumulates_text_tools_finish_and_usage() {
        let (response, chunks) = run_stream(CHAT_SSE);
        let response = response.unwrap();
        assert_eq!(chunks, vec!["Hel", "lo"]);
        assert_eq!(response.text, "Hello");
        assert_eq!(response.finish, FinishReason::ToolCalls);
        assert_eq!(response.tool_calls[0].id, "t1");
        assert_eq!(response.tool_calls[0].name, "lookup");
        assert_eq!(response.tool_calls[0].arguments, json!({"q": 1}));
        assert_eq!(response.usage.input_tokens, 4);
        assert_eq!(response.usage.output_tokens, 6);
        assert_eq!(response.provider_request_id.as_deref(), Some("c1"));
    }

    #[test]
    fn truncated_or_errored_stream_is_a_failure() {
        let (response, _) = run_stream("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
        assert!(response.is_err());
        let (response, _) = run_stream("data: {\"error\":{\"message\":\"boom\"}}\n\n");
        assert!(response.unwrap_err().detail().contains("boom"));
    }

    #[test]
    fn responses_body_maps_the_neutral_request() {
        let mut req = request();
        req.system = Some("sys".into());
        req.json_output = true;
        req.max_output_tokens = Some(9);
        req.tools = vec![LlmToolSpec {
            name: "lookup".into(),
            description: "d".into(),
            parameters: json!({"type": "object"}),
        }];
        req.messages.push(LlmMessage {
            role: LlmRole::Assistant,
            content: String::new(),
            tool_calls: vec![LlmToolCall {
                id: "c1".into(),
                name: "lookup".into(),
                arguments: json!({"q": 1}),
            }],
            tool_call_id: None,
        });
        req.messages.push(LlmMessage {
            role: LlmRole::Tool,
            content: "found".into(),
            tool_calls: Vec::new(),
            tool_call_id: Some("c1".into()),
        });
        let body = responses_body(&req);
        assert_eq!(body["background"], true);
        assert_eq!(body["store"], true);
        assert_eq!(body["instructions"], "sys");
        assert_eq!(body["max_output_tokens"], 9);
        assert_eq!(body["text"]["format"]["type"], "json_object");
        assert_eq!(body["tools"][0]["name"], "lookup");
        assert_eq!(body["input"][0], json!({"role": "user", "content": "hi"}));
        assert_eq!(body["input"][1]["type"], "function_call");
        assert_eq!(body["input"][1]["arguments"], r#"{"q":1}"#);
        assert_eq!(
            body["input"][2],
            json!({"type": "function_call_output", "call_id": "c1", "output": "found"})
        );
    }

    #[test]
    fn job_states_map() {
        assert_eq!(
            parse_job_state(&json!({"status": "queued"})).unwrap(),
            LlmJobState::Pending
        );
        assert_eq!(
            parse_job_state(&json!({"status": "in_progress"})).unwrap(),
            LlmJobState::Pending
        );
        let done = parse_job_state(&json!({
            "id": "resp_1", "status": "completed",
            "output": [
                {"type": "reasoning", "summary": []},
                {"type": "message", "content": [{"type": "output_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "c", "name": "f", "arguments": "{\"a\":1}"},
            ],
            "usage": {"input_tokens": 2, "output_tokens": 3},
        }))
        .unwrap();
        match done {
            LlmJobState::Done(r) => {
                assert_eq!(r.text, "hi");
                assert_eq!(r.finish, FinishReason::ToolCalls);
                assert_eq!(r.tool_calls[0].arguments, json!({"a": 1}));
                assert_eq!(r.usage.output_tokens, 3);
                assert_eq!(r.provider_request_id.as_deref(), Some("resp_1"));
            }
            other => panic!("{other:?}"),
        }
        match parse_job_state(&json!({"status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"}, "output": []}))
        .unwrap()
        {
            LlmJobState::Done(r) => assert_eq!(r.finish, FinishReason::Length),
            other => panic!("{other:?}"),
        }
        match parse_job_state(&json!({"status": "failed", "error": {"message": "bad"}})).unwrap() {
            LlmJobState::Failed(f) => assert!(f.detail().contains("bad")),
            other => panic!("{other:?}"),
        }
        assert!(parse_job_state(&json!({"status": "???"})).is_err());
    }

    fn signed(secret_key: &[u8], id: &str, ts: &str, body: &str) -> BTreeMap<String, String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).unwrap();
        mac.update(format!("{id}.{ts}.{body}").as_bytes());
        let sig = BASE64.encode(mac.finalize().into_bytes());
        BTreeMap::from([
            ("Webhook-Id".to_string(), id.to_string()),
            ("webhook-timestamp".to_string(), ts.to_string()),
            (
                "webhook-signature".to_string(),
                format!("v1,bogus v1,{sig}"),
            ),
        ])
    }

    #[test]
    fn webhook_signature_verifies_and_rejects() {
        let key = b"0123456789abcdef0123456789abcdef";
        let secret = format!("whsec_{}", BASE64.encode(key));
        let body =
            r#"{"object":"event","id":"evt_1","type":"response.completed","data":{"id":"resp_9"}}"#;
        let headers = signed(key, "msg_1", "1750287018", body);
        assert!(verify_webhook(
            &headers,
            body.as_bytes(),
            &secret,
            1750287100
        ));
        // tampered body, wrong secret, stale timestamp, missing header
        assert!(!verify_webhook(&headers, b"{}", &secret, 1750287100));
        assert!(!verify_webhook(
            &headers,
            body.as_bytes(),
            "whsec_AAAA",
            1750287100
        ));
        assert!(!verify_webhook(
            &headers,
            body.as_bytes(),
            &secret,
            1750287018 + 3600
        ));
        let mut missing = headers.clone();
        missing.remove("webhook-timestamp");
        assert!(!verify_webhook(
            &missing,
            body.as_bytes(),
            &secret,
            1750287100
        ));
    }

    #[test]
    fn webhook_parse_names_the_job_without_state() {
        let event =
            parse_webhook(br#"{"type":"response.completed","data":{"id":"resp_9"}}"#).unwrap();
        assert_eq!(event.job.id, "resp_9");
        assert!(event.state.is_none());
        assert!(parse_webhook(br#"{"type":"batch.completed","data":{"id":"x"}}"#).is_err());
        assert!(parse_webhook(b"nope").is_err());
    }

    #[test]
    fn only_openai_advertises_jobs_and_callbacks() {
        let config = LlmProviderConfig {
            model: "test-model".into(),
            base_url: None,
            api_key: None,
        };
        for (id, _) in COMPATIBLE_SERVICES {
            let caps = build_for(id, &config).unwrap().capabilities();
            assert!(caps.streaming, "{id}");
            assert_eq!(caps.async_jobs, *id == "openai", "{id}");
            assert_eq!(caps.job_callbacks, *id == "openai", "{id}");
        }
    }

    #[tokio::test]
    async fn streaming_round_trip_sends_stream_flags_and_emits_chunks() {
        let (url, seen) = stub_sse(CHAT_SSE).await;
        let chunks = std::sync::Mutex::new(Vec::new());
        let sink = |t: &str| chunks.lock().unwrap().push(t.to_string());
        let response = provider(url)
            .complete_streaming(&request(), &sink)
            .await
            .unwrap();
        assert_eq!(response.text, "Hello");
        assert_eq!(chunks.into_inner().unwrap(), vec!["Hel", "lo"]);
        let raw = seen.await.unwrap();
        assert!(raw.contains(r#""stream":true"#), "{raw}");
        assert!(raw.contains(r#""include_usage":true"#), "{raw}");
    }

    #[tokio::test]
    async fn job_round_trip_submits_and_polls() {
        let (url, seen) = stub(200, r#"{"id":"resp_7","status":"queued"}"#).await;
        let p = provider(url);
        let job = p.submit(&request()).await.unwrap();
        assert_eq!(job.id, "resp_7");
        let raw = seen.await.unwrap();
        assert!(raw.starts_with("POST /v1/responses"), "{raw}");
        assert!(raw.contains(r#""background":true"#), "{raw}");
        let (url, seen) = stub(200, r#"{"id":"resp_7","status":"in_progress"}"#).await;
        assert_eq!(p_with(url).poll(&job).await.unwrap(), LlmJobState::Pending);
        assert!(seen.await.unwrap().starts_with("GET /v1/responses/resp_7"));
    }

    fn p_with(url: String) -> Arc<dyn LlmProvider> {
        provider(url)
    }

    /// One-shot stub answering `body` as an event stream.
    async fn stub_sse(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16384];
            let n = socket.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        (format!("http://{addr}/v1/"), handle)
    }
}
