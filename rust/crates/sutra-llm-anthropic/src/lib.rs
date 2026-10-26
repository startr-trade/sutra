//! The Anthropic Messages API adapter for the `llm` transport.
//!
//! Registers an [`LlmProviderFactory`] for `provider: anthropic`. Request and response
//! mapping are pure functions ([`request_body`], [`parse_response`]) so they are testable
//! without a network; the HTTP exchange goes through the shared [`JsonHttpClient`].
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use sutra_llm_spi::{
    codes, BoxFuture, ChunkSink, Diagnostic, FinishReason, JsonHttpClient, LlmCapabilities,
    LlmFailure, LlmJobRef, LlmJobState, LlmProvider, LlmProviderConfig, LlmProviderFactory,
    LlmRequest, LlmResponse, LlmRole, LlmToolCall, LlmUsage, RetryableClass,
};

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";
/// `max_tokens` is mandatory on the Messages API; used when the request sets none.
const DEFAULT_MAX_TOKENS: u32 = 1024;
const JSON_INSTRUCTION: &str = "Respond with a single JSON object and nothing else.";

/// The one request in a job's batch.
const BATCH_CUSTOM_ID: &str = "sutra-1";
/// A job poll or result fetch is a short GET; the job itself has no deadline.
const POLL_DEADLINE: Duration = Duration::from_secs(60);

struct AnthropicProvider {
    url: String,
    /// `<base>/v1/messages/batches`.
    batches_url: String,
    api_key: Option<String>,
    http: JsonHttpClient,
}

fn build(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    if config.model.trim().is_empty() {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            "provider 'anthropic' requires a non-empty model",
        ));
    }
    let base = config.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
    Ok(Arc::new(AnthropicProvider {
        url: format!("{}/v1/messages", base.trim_end_matches('/')),
        batches_url: format!("{}/v1/messages/batches", base.trim_end_matches('/')),
        api_key: config.api_key.clone(),
        http: JsonHttpClient::new(Duration::from_secs(5)),
    }))
}

inventory::submit! {
    LlmProviderFactory { provider: "anthropic", build }
}

impl AnthropicProvider {
    async fn complete_once(&self, request: &LlmRequest) -> Result<LlmResponse, LlmFailure> {
        let body = request_body(request);
        let headers = self.headers();
        let value = self
            .http
            .post_json(&self.url, &headers, &body, request.deadline)
            .await
            .map_err(LlmFailure::from)?;
        parse_response(&value)
    }

    async fn stream_once(
        &self,
        request: &LlmRequest,
        on_chunk: ChunkSink<'_>,
    ) -> Result<LlmResponse, LlmFailure> {
        let mut body = request_body(request);
        body["stream"] = json!(true);
        let headers = self.headers();
        let mut stream = MessageStream::default();
        self.http
            .post_sse(&self.url, &headers, &body, request.deadline, &mut |event| {
                stream.feed(&event.data, on_chunk)
            })
            .await
            .map_err(LlmFailure::from)?;
        stream.finish()
    }
}

/// Newer models reject sampling parameters (`temperature`) outright with a 400. A channel's
/// `temperature` is a preference, not a contract, so such a rejection is retried once without it
/// rather than failing the call.
fn retry_without_sampling(request: &LlmRequest, failure: &LlmFailure) -> bool {
    request.temperature.is_some()
        && matches!(failure, LlmFailure::Permanent { detail }
            if detail.starts_with("HTTP 400") && detail.contains("temperature"))
}

fn without_sampling(request: &LlmRequest) -> LlmRequest {
    LlmRequest {
        temperature: None,
        ..request.clone()
    }
}

impl LlmProvider for AnthropicProvider {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            match self.complete_once(request).await {
                Err(failure) if retry_without_sampling(request, &failure) => {
                    self.complete_once(&without_sampling(request)).await
                }
                other => other,
            }
        })
    }

    fn capabilities(&self) -> LlmCapabilities {
        LlmCapabilities {
            tools: true,
            json_output: true,
            streaming: true,
            async_jobs: true,
            job_callbacks: false,
            ..LlmCapabilities::default()
        }
    }

    fn complete_streaming<'a>(
        &'a self,
        request: &'a LlmRequest,
        on_chunk: ChunkSink<'a>,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            // A rejected parameter is answered before any event, so a retry never repeats a chunk.
            match self.stream_once(request, on_chunk).await {
                Err(failure) if retry_without_sampling(request, &failure) => {
                    self.stream_once(&without_sampling(request), on_chunk).await
                }
                other => other,
            }
        })
    }

    fn submit<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmJobRef, LlmFailure>> {
        Box::pin(async move {
            let value = self
                .http
                .post_json(
                    &self.batches_url,
                    &self.headers(),
                    &batch_body(request),
                    request.deadline,
                )
                .await
                .map_err(LlmFailure::from)?;
            let id = value["id"].as_str().ok_or_else(|| LlmFailure::Permanent {
                detail: format!("{}: the batch answer has no 'id'", codes::RESPONSE_INVALID),
            })?;
            Ok(LlmJobRef { id: id.to_string() })
        })
    }

    fn poll<'a>(&'a self, job: &'a LlmJobRef) -> BoxFuture<'a, Result<LlmJobState, LlmFailure>> {
        Box::pin(async move {
            let headers = self.headers();
            let url = format!("{}/{}", self.batches_url, job.id);
            let batch = self
                .http
                .get_json(&url, &headers, POLL_DEADLINE)
                .await
                .map_err(LlmFailure::from)?;
            if batch["processing_status"].as_str() != Some("ended") {
                return Ok(LlmJobState::Pending);
            }
            let results_url = batch["results_url"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| format!("{url}/results"));
            let jsonl = self
                .http
                .get_text(&results_url, &headers, POLL_DEADLINE)
                .await
                .map_err(LlmFailure::from)?;
            parse_batch_results(&jsonl)
        })
    }
}

impl AnthropicProvider {
    fn headers(&self) -> Vec<(&str, &str)> {
        let mut headers: Vec<(&str, &str)> = vec![("anthropic-version", API_VERSION)];
        if let Some(key) = &self.api_key {
            headers.push(("x-api-key", key));
        }
        headers
    }
}

fn map_stop(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("end_turn" | "stop_sequence") => FinishReason::Stop,
        Some("max_tokens" | "model_context_window_exceeded") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCalls,
        Some("refusal") => FinishReason::Filtered,
        Some(other) => FinishReason::Other(other.to_string()),
        None => FinishReason::Other("unknown".to_string()),
    }
}

fn invalid(detail: String) -> LlmFailure {
    LlmFailure::Permanent {
        detail: format!("{}: {detail}", codes::RESPONSE_INVALID),
    }
}

/// Map an API error object (`{"type": ..., "message": ...}`) to a failure.
fn error_failure(error: &Value) -> LlmFailure {
    let kind = error["type"].as_str().unwrap_or("unknown_error");
    let detail = format!(
        "{kind}: {}",
        error["message"].as_str().unwrap_or("no detail")
    );
    let class = match kind {
        "overloaded_error" | "rate_limit_error" => RetryableClass::RateLimited,
        "api_error" => RetryableClass::Unavailable,
        "timeout_error" => RetryableClass::Timeout,
        _ => return LlmFailure::Permanent { detail },
    };
    LlmFailure::Retryable {
        class,
        retry_after: None,
        detail,
    }
}

/// Accumulates a Messages API event stream into a response.
#[derive(Default)]
struct MessageStream {
    text: String,
    /// Content blocks by stream `index`: (id, name, input JSON text) for `tool_use` blocks.
    tools: BTreeMap<u64, (String, String, String)>,
    stop: Option<String>,
    usage: LlmUsage,
    id: Option<String>,
    error: Option<LlmFailure>,
    stopped: bool,
}

impl MessageStream {
    fn feed(&mut self, data: &str, on_chunk: ChunkSink<'_>) {
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            return;
        };
        let index = event["index"].as_u64().unwrap_or(0);
        match event["type"].as_str() {
            Some("message_start") => {
                let message = &event["message"];
                self.id = message["id"].as_str().map(str::to_string);
                self.usage.input_tokens = message["usage"]["input_tokens"].as_u64().unwrap_or(0);
                self.usage.output_tokens = message["usage"]["output_tokens"].as_u64().unwrap_or(0);
            }
            Some("content_block_start") => {
                let block = &event["content_block"];
                if block["type"] == "tool_use" {
                    self.tools.insert(
                        index,
                        (
                            block["id"].as_str().unwrap_or("").to_string(),
                            block["name"].as_str().unwrap_or("").to_string(),
                            String::new(),
                        ),
                    );
                }
            }
            Some("content_block_delta") => {
                let delta = &event["delta"];
                match delta["type"].as_str() {
                    Some("text_delta") => {
                        if let Some(text) = delta["text"].as_str().filter(|t| !t.is_empty()) {
                            self.text.push_str(text);
                            on_chunk(text);
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some(tool) = self.tools.get_mut(&index) {
                            tool.2
                                .push_str(delta["partial_json"].as_str().unwrap_or(""));
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                if let Some(reason) = event["delta"]["stop_reason"].as_str() {
                    self.stop = Some(reason.to_string());
                }
                if let Some(output) = event["usage"]["output_tokens"].as_u64() {
                    self.usage.output_tokens = output;
                }
                if let Some(input) = event["usage"]["input_tokens"].as_u64() {
                    self.usage.input_tokens = input;
                }
            }
            Some("message_stop") => self.stopped = true,
            Some("error") => self.error = Some(error_failure(&event["error"])),
            _ => {}
        }
    }

    fn finish(self) -> Result<LlmResponse, LlmFailure> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if !self.stopped && self.stop.is_none() {
            return Err(invalid("the stream ended before message_stop".into()));
        }
        Ok(LlmResponse {
            text: self.text,
            tool_calls: self
                .tools
                .into_values()
                .map(|(id, name, input)| LlmToolCall {
                    id,
                    name,
                    arguments: if input.trim().is_empty() {
                        json!({})
                    } else {
                        serde_json::from_str(&input).unwrap_or(Value::String(input))
                    },
                })
                .collect(),
            finish: map_stop(self.stop.as_deref()),
            usage: self.usage,
            provider_request_id: self.id,
            audio: None,
            transcript: None,
        })
    }
}

/// A Message Batches create body holding the one request.
fn batch_body(request: &LlmRequest) -> Value {
    json!({"requests": [{"custom_id": BATCH_CUSTOM_ID, "params": request_body(request)}]})
}

/// Read a batch's JSONL results and map our one request's result to a job state.
fn parse_batch_results(jsonl: &str) -> Result<LlmJobState, LlmFailure> {
    for line in jsonl.lines().filter(|l| !l.trim().is_empty()) {
        let entry: Value = serde_json::from_str(line)
            .map_err(|e| invalid(format!("a batch result line is not JSON: {e}")))?;
        if entry["custom_id"].as_str() != Some(BATCH_CUSTOM_ID) {
            continue;
        }
        let result = &entry["result"];
        return match result["type"].as_str() {
            Some("succeeded") => parse_response(&result["message"]).map(LlmJobState::Done),
            Some("errored") => Ok(LlmJobState::Failed(error_failure(
                &result["error"]["error"],
            ))),
            Some("canceled") => Ok(LlmJobState::Failed(LlmFailure::Permanent {
                detail: "the batch was canceled before the request ran".into(),
            })),
            Some("expired") => Ok(LlmJobState::Failed(LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the batch expired before the request ran".into(),
            })),
            other => Err(invalid(format!("unknown batch result type {other:?}"))),
        };
    }
    Err(invalid(format!(
        "the batch results hold no entry for '{BATCH_CUSTOM_ID}'"
    )))
}

/// Map a neutral request to a Messages API body.
fn request_body(request: &LlmRequest) -> Value {
    let mut body = Map::new();
    body.insert("model".into(), json!(request.model));
    body.insert(
        "max_tokens".into(),
        json!(request.max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
    );
    // The API has no JSON-mode flag, so the nearest equivalent is an instruction appended
    // to the system prompt (or the system prompt itself when none was given).
    let system = match (&request.system, request.json_output) {
        (Some(s), true) => Some(format!("{s}\n\n{JSON_INSTRUCTION}")),
        (Some(s), false) => Some(s.clone()),
        (None, true) => Some(JSON_INSTRUCTION.to_string()),
        (None, false) => None,
    };
    if let Some(system) = system {
        body.insert("system".into(), json!(system));
    }
    if let Some(t) = request.temperature {
        body.insert("temperature".into(), json!(t));
    }

    let mut messages: Vec<Value> = Vec::new();
    for m in &request.messages {
        match m.role {
            LlmRole::User => messages.push(json!({"role": "user", "content": m.content})),
            LlmRole::Assistant if m.tool_calls.is_empty() => {
                messages.push(json!({"role": "assistant", "content": m.content}))
            }
            LlmRole::Assistant => {
                let mut blocks = Vec::new();
                if !m.content.is_empty() {
                    blocks.push(json!({"type": "text", "text": m.content}));
                }
                for call in &m.tool_calls {
                    blocks.push(json!({
                        "type": "tool_use", "id": call.id, "name": call.name, "input": call.arguments
                    }));
                }
                messages.push(json!({"role": "assistant", "content": blocks}));
            }
            LlmRole::Tool => {
                let block = json!({
                    "type": "tool_result",
                    "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                    "content": m.content,
                });
                // Consecutive tool results share one user message.
                let merged = messages.last_mut().and_then(|last| {
                    let is_results = last["role"] == "user"
                        && last["content"]
                            .as_array()
                            .is_some_and(|a| a.iter().all(|b| b["type"] == "tool_result"));
                    if is_results {
                        last["content"].as_array_mut()
                    } else {
                        None
                    }
                });
                match merged {
                    Some(blocks) => blocks.push(block),
                    None => messages.push(json!({"role": "user", "content": [block]})),
                }
            }
        }
    }
    body.insert("messages".into(), Value::Array(messages));

    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|t| {
                json!({"name": t.name, "description": t.description, "input_schema": t.parameters})
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    Value::Object(body)
}

/// Map a Messages API answer to a neutral response.
fn parse_response(value: &Value) -> Result<LlmResponse, LlmFailure> {
    let Some(content) = value.get("content").and_then(Value::as_array) else {
        return Err(LlmFailure::Permanent {
            detail: format!(
                "{}: the Anthropic answer has no 'content' array",
                codes::RESPONSE_INVALID
            ),
        });
    };
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in content {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => text.push_str(block["text"].as_str().unwrap_or_default()),
            Some("tool_use") => tool_calls.push(LlmToolCall {
                id: block["id"].as_str().unwrap_or_default().to_string(),
                name: block["name"].as_str().unwrap_or_default().to_string(),
                arguments: block.get("input").cloned().unwrap_or_else(|| json!({})),
            }),
            _ => {}
        }
    }
    let finish = map_stop(value.get("stop_reason").and_then(Value::as_str));
    let usage = LlmUsage {
        input_tokens: value["usage"]["input_tokens"].as_u64().unwrap_or(0),
        output_tokens: value["usage"]["output_tokens"].as_u64().unwrap_or(0),
    };
    Ok(LlmResponse {
        text,
        tool_calls,
        finish,
        usage,
        provider_request_id: value.get("id").and_then(Value::as_str).map(String::from),
        audio: None,
        transcript: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_window_stop_reason_maps_to_length() {
        assert_eq!(
            map_stop(Some("model_context_window_exceeded")),
            FinishReason::Length
        );
    }

    use sutra_llm_spi::{LlmMessage, LlmToolSpec, RetryableClass};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn req(messages: Vec<LlmMessage>) -> LlmRequest {
        LlmRequest {
            model: "test-model".into(),
            system: None,
            messages,
            tools: vec![],
            json_output: false,
            max_output_tokens: None,
            temperature: None,
            deadline: Duration::from_secs(5),
            audio_output: None,
            output_voice: None,
        }
    }

    fn tool_msg(id: &str, content: &str) -> LlmMessage {
        LlmMessage {
            role: LlmRole::Tool,
            content: content.into(),
            tool_calls: vec![],
            tool_call_id: Some(id.into()),
            audio: None,
            video: None,
        }
    }

    #[test]
    fn basic_request_defaults_max_tokens_and_omits_optionals() {
        let b = request_body(&req(vec![LlmMessage::user("hi")]));
        assert_eq!(b["model"], "test-model");
        assert_eq!(b["max_tokens"], 1024);
        assert!(b.get("system").is_none() && b.get("tools").is_none());
        assert!(b.get("temperature").is_none());
        assert_eq!(b["messages"][0], json!({"role": "user", "content": "hi"}));
    }

    #[test]
    fn system_temperature_and_max_tokens_pass_through() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.system = Some("be brief".into());
        r.temperature = Some(0.5);
        r.max_output_tokens = Some(200);
        let b = request_body(&r);
        assert_eq!(b["system"], "be brief");
        assert_eq!(b["max_tokens"], 200);
        assert_eq!(b["temperature"], 0.5);
    }

    #[test]
    fn json_output_appends_instruction_to_system() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.json_output = true;
        assert_eq!(request_body(&r)["system"], JSON_INSTRUCTION);
        r.system = Some("be brief".into());
        assert_eq!(
            request_body(&r)["system"],
            format!("be brief\n\n{JSON_INSTRUCTION}")
        );
    }

    #[test]
    fn tools_map_to_input_schema() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.tools = vec![LlmToolSpec {
            name: "lookup".into(),
            description: "find".into(),
            parameters: json!({"type": "object"}),
        }];
        assert_eq!(
            request_body(&r)["tools"][0],
            json!({"name": "lookup", "description": "find", "input_schema": {"type": "object"}})
        );
    }

    #[test]
    fn tool_turns_use_blocks_and_merge_consecutive_results() {
        let assistant = LlmMessage {
            role: LlmRole::Assistant,
            content: "checking".into(),
            tool_calls: vec![
                LlmToolCall {
                    id: "t1".into(),
                    name: "a".into(),
                    arguments: json!({"x": 1}),
                },
                LlmToolCall {
                    id: "t2".into(),
                    name: "b".into(),
                    arguments: json!({}),
                },
            ],
            tool_call_id: None,
            audio: None,
            video: None,
        };
        let b = request_body(&req(vec![
            LlmMessage::user("go"),
            assistant,
            tool_msg("t1", "r1"),
            tool_msg("t2", "r2"),
        ]));
        let m = b["messages"].as_array().unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(
            m[1]["content"],
            json!([
                {"type": "text", "text": "checking"},
                {"type": "tool_use", "id": "t1", "name": "a", "input": {"x": 1}},
                {"type": "tool_use", "id": "t2", "name": "b", "input": {}},
            ])
        );
        assert_eq!(m[2]["role"], "user");
        assert_eq!(
            m[2]["content"],
            json!([
                {"type": "tool_result", "tool_use_id": "t1", "content": "r1"},
                {"type": "tool_result", "tool_use_id": "t2", "content": "r2"},
            ])
        );
    }

    #[test]
    fn assistant_tool_call_without_text_has_no_text_block() {
        let assistant = LlmMessage {
            role: LlmRole::Assistant,
            content: String::new(),
            tool_calls: vec![LlmToolCall {
                id: "t1".into(),
                name: "a".into(),
                arguments: json!({}),
            }],
            tool_call_id: None,
            audio: None,
            video: None,
        };
        let b = request_body(&req(vec![assistant]));
        assert_eq!(b["messages"][0]["content"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn response_text_tool_use_usage_and_id() {
        let r = parse_response(&json!({
            "id": "msg_1",
            "content": [
                {"type": "text", "text": "a"},
                {"type": "text", "text": "b"},
                {"type": "tool_use", "id": "t1", "name": "lookup", "input": {"q": "x"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 11, "output_tokens": 7}
        }))
        .unwrap();
        assert_eq!(r.text, "ab");
        assert_eq!(r.tool_calls.len(), 1);
        assert_eq!(r.tool_calls[0].name, "lookup");
        assert_eq!(r.tool_calls[0].arguments, json!({"q": "x"}));
        assert_eq!(r.finish, FinishReason::ToolCalls);
        assert_eq!(
            r.usage,
            LlmUsage {
                input_tokens: 11,
                output_tokens: 7
            }
        );
        assert_eq!(r.provider_request_id.as_deref(), Some("msg_1"));
    }

    #[test]
    fn stop_reasons_map() {
        let finish = |s: &str| {
            parse_response(&json!({"content": [], "stop_reason": s}))
                .unwrap()
                .finish
        };
        assert_eq!(finish("end_turn"), FinishReason::Stop);
        assert_eq!(finish("stop_sequence"), FinishReason::Stop);
        assert_eq!(finish("max_tokens"), FinishReason::Length);
        assert_eq!(finish("tool_use"), FinishReason::ToolCalls);
        assert_eq!(finish("refusal"), FinishReason::Filtered);
        assert_eq!(
            finish("pause_turn"),
            FinishReason::Other("pause_turn".into())
        );
    }

    #[test]
    fn missing_content_is_permanent() {
        match parse_response(&json!({"id": "x"})).unwrap_err() {
            LlmFailure::Permanent { detail } => assert!(detail.contains(codes::RESPONSE_INVALID)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn build_requires_a_model_and_trims_base_url() {
        let cfg = |model: &str, base: Option<&str>| LlmProviderConfig {
            model: model.into(),
            base_url: base.map(String::from),
            api_key: None,
        };
        let err = build(&cfg(" ", None)).err().expect("empty model rejected");
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(build(&cfg("test-model", Some("http://h/"))).is_ok());
        assert!(sutra_llm_spi::find_provider("anthropic").is_some());
    }

    /// One-shot stub: captures the request head, answers `status` + `body`.
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
            String::from_utf8_lossy(&buf[..n]).to_lowercase()
        });
        (format!("http://{addr}/"), handle)
    }

    fn provider(base: String) -> Arc<dyn LlmProvider> {
        build(&LlmProviderConfig {
            model: "test-model".into(),
            base_url: Some(base),
            api_key: Some("test-key".into()),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn round_trip_sends_auth_headers() {
        let (base, head) = stub(
            200,
            r#"{"id":"msg_9","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}"#,
        )
        .await;
        let r = provider(base)
            .complete(&req(vec![LlmMessage::user("hi")]))
            .await
            .unwrap();
        assert_eq!(r.text, "ok");
        let head = head.await.unwrap();
        assert!(head.starts_with("post /v1/messages "), "{head}");
        assert!(head.contains("x-api-key: test-key"), "{head}");
        assert!(head.contains("anthropic-version: 2023-06-01"), "{head}");
    }

    #[tokio::test]
    async fn overload_529_is_rate_limited() {
        let (base, _head) = stub(529, r#"{"type":"error"}"#).await;
        let failure = provider(base)
            .complete(&req(vec![LlmMessage::user("hi")]))
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            LlmFailure::Retryable {
                class: RetryableClass::RateLimited,
                ..
            }
        ));
    }

    const MESSAGE_SSE: &str = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":11,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: ping\ndata: {\"type\":\"ping\"}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"lookup\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\": 1}\"}}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":15}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );

    fn run_stream(sse: &str) -> (Result<LlmResponse, LlmFailure>, Vec<String>) {
        let chunks = std::sync::Mutex::new(Vec::new());
        let sink = |t: &str| chunks.lock().unwrap().push(t.to_string());
        let mut parser = sutra_llm_spi::SseParser::new();
        let mut stream = MessageStream::default();
        for event in parser.feed(sse.as_bytes()) {
            stream.feed(&event.data, &sink);
        }
        (stream.finish(), chunks.into_inner().unwrap())
    }

    #[test]
    fn message_stream_accumulates_text_tools_stop_and_usage() {
        let (response, chunks) = run_stream(MESSAGE_SSE);
        let response = response.unwrap();
        assert_eq!(chunks, vec!["Hel", "lo"]);
        assert_eq!(response.text, "Hello");
        assert_eq!(response.finish, FinishReason::ToolCalls);
        assert_eq!(response.tool_calls[0].id, "tu_1");
        assert_eq!(response.tool_calls[0].arguments, json!({"q": 1}));
        assert_eq!(response.usage.input_tokens, 11);
        assert_eq!(response.usage.output_tokens, 15);
        assert_eq!(response.provider_request_id.as_deref(), Some("msg_1"));
    }

    #[test]
    fn stream_error_event_is_classified_and_truncation_fails() {
        let (response, _) = run_stream(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
        );
        match response {
            Err(LlmFailure::Retryable { class, .. }) => {
                assert_eq!(class, RetryableClass::RateLimited)
            }
            other => panic!("{other:?}"),
        }
        let (response, _) = run_stream(
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n",
        );
        assert!(response.is_err());
    }

    #[test]
    fn batch_body_holds_one_request_with_custom_id() {
        let body = batch_body(&req(vec![LlmMessage::user("hi")]));
        assert_eq!(body["requests"].as_array().unwrap().len(), 1);
        assert_eq!(body["requests"][0]["custom_id"], BATCH_CUSTOM_ID);
        assert_eq!(body["requests"][0]["params"]["model"], "test-model");
        assert!(body["requests"][0]["params"].get("stream").is_none());
    }

    #[test]
    fn batch_results_map_each_result_type() {
        let line = |result: &str| format!(r#"{{"custom_id":"sutra-1","result":{result}}}"#);
        let ok = line(
            r#"{"type":"succeeded","message":{"id":"msg_9","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3}}}"#,
        );
        // another request's line is skipped
        let jsonl =
            format!("{{\"custom_id\":\"other\",\"result\":{{\"type\":\"expired\"}}}}\n{ok}\n");
        match parse_batch_results(&jsonl).unwrap() {
            LlmJobState::Done(r) => {
                assert_eq!(r.text, "hi");
                assert_eq!(r.usage.output_tokens, 3);
                assert_eq!(r.provider_request_id.as_deref(), Some("msg_9"));
            }
            other => panic!("{other:?}"),
        }
        let errored = line(
            r#"{"type":"errored","error":{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}}"#,
        );
        match parse_batch_results(&errored).unwrap() {
            LlmJobState::Failed(LlmFailure::Permanent { detail }) => {
                assert!(detail.contains("bad"))
            }
            other => panic!("{other:?}"),
        }
        let overloaded = line(
            r#"{"type":"errored","error":{"type":"error","error":{"type":"overloaded_error","message":"x"}}}"#,
        );
        assert!(matches!(
            parse_batch_results(&overloaded).unwrap(),
            LlmJobState::Failed(LlmFailure::Retryable { .. })
        ));
        assert!(matches!(
            parse_batch_results(&line(r#"{"type":"expired"}"#)).unwrap(),
            LlmJobState::Failed(LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                ..
            })
        ));
        assert!(matches!(
            parse_batch_results(&line(r#"{"type":"canceled"}"#)).unwrap(),
            LlmJobState::Failed(LlmFailure::Permanent { .. })
        ));
        assert!(parse_batch_results("").is_err());
    }

    #[test]
    fn capabilities_advertise_jobs_but_not_callbacks() {
        let p = build(&LlmProviderConfig {
            model: "test-model".into(),
            base_url: None,
            api_key: None,
        })
        .unwrap();
        let caps = p.capabilities();
        assert!(caps.streaming && caps.async_jobs && !caps.job_callbacks);
    }

    /// One-shot stub answering `body` with `content_type`; returns the raw request seen.
    async fn stub_raw(
        content_type: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16384];
            let n = socket.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\nconnection: close\r\n\r\n{body}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        (format!("http://{addr}"), handle)
    }

    fn provider_at(base_url: String) -> Arc<dyn LlmProvider> {
        build(&LlmProviderConfig {
            model: "test-model".into(),
            base_url: Some(base_url),
            api_key: Some("test-key".into()),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn streaming_round_trip_sends_stream_flag_and_emits_chunks() {
        let (url, seen) = stub_raw("text/event-stream", MESSAGE_SSE).await;
        let chunks = std::sync::Mutex::new(Vec::new());
        let sink = |t: &str| chunks.lock().unwrap().push(t.to_string());
        let response = provider_at(url)
            .complete_streaming(&req(vec![LlmMessage::user("hi")]), &sink)
            .await
            .unwrap();
        assert_eq!(response.text, "Hello");
        assert_eq!(chunks.into_inner().unwrap(), vec!["Hel", "lo"]);
        let raw = seen.await.unwrap();
        assert!(raw.starts_with("POST /v1/messages"), "{raw}");
        assert!(raw.contains(r#""stream":true"#), "{raw}");
        assert!(raw.to_lowercase().contains("x-api-key: test-key"), "{raw}");
    }

    /// A two-request stub: the first answer rejects `temperature`, the second succeeds.
    async fn stub_rejecting_temperature() -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let rejected = r#"{"type":"error","error":{"type":"invalid_request_error","message":"`temperature` is deprecated for this model."}}"#;
            let ok = r#"{"id":"msg_1","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}"#;
            let mut seen = Vec::new();
            for (status, body) in [("400 Bad Request", rejected), ("200 OK", ok)] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 16384];
                let n = socket.read(&mut buf).await.unwrap();
                seen.push(String::from_utf8_lossy(&buf[..n]).to_string());
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            seen
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn a_rejected_temperature_is_retried_once_without_it() {
        let (url, seen) = stub_rejecting_temperature().await;
        let mut request = req(vec![LlmMessage::user("hi")]);
        request.temperature = Some(0.0);
        let response = provider_at(url).complete(&request).await.unwrap();
        assert_eq!(response.text, "ok");
        let seen = seen.await.unwrap();
        assert!(seen[0].contains(r#""temperature""#), "{}", seen[0]);
        assert!(!seen[1].contains(r#""temperature""#), "{}", seen[1]);
    }

    #[tokio::test]
    async fn job_round_trip_submits_and_polls_pending() {
        let (url, seen) = stub_raw("application/json", r#"{"id":"msgbatch_1"}"#).await;
        let p = provider_at(url);
        let job = p.submit(&req(vec![LlmMessage::user("hi")])).await.unwrap();
        assert_eq!(job.id, "msgbatch_1");
        assert!(seen.await.unwrap().starts_with("POST /v1/messages/batches"));
        let (url, seen) = stub_raw(
            "application/json",
            r#"{"id":"msgbatch_1","processing_status":"in_progress","results_url":null}"#,
        )
        .await;
        assert_eq!(
            provider_at(url).poll(&job).await.unwrap(),
            LlmJobState::Pending
        );
        assert!(seen
            .await
            .unwrap()
            .starts_with("GET /v1/messages/batches/msgbatch_1"));
    }
}
