//! The Amazon Bedrock Converse API adapter for the `llm` transport.
//!
//! Registers an [`LlmProviderFactory`] for `provider: aws-bedrock`. Authentication is a
//! Bedrock API key sent as a bearer token; IAM SigV4 signing is not supported. Request and
//! response mapping are pure functions ([`request_body`], [`parse_response`]).
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use sutra_llm_spi::{
    codes, BoxFuture, Diagnostic, FinishReason, JsonHttpClient, LlmCapabilities, LlmFailure,
    LlmProvider, LlmProviderConfig, LlmProviderFactory, LlmRequest, LlmResponse, LlmRole,
    LlmToolCall, LlmUsage,
};

const JSON_INSTRUCTION: &str = "Respond with a single JSON object and nothing else.";

struct BedrockProvider {
    /// Base URL without a trailing slash; the model id is encoded per request.
    base: String,
    api_key: String,
    http: JsonHttpClient,
}

fn build(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    if config.model.trim().is_empty() {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            "provider 'aws-bedrock' requires a non-empty model",
        ));
    }
    let Some(api_key) = config.api_key.clone() else {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            "provider 'aws-bedrock' requires an api-key: IAM SigV4 signing is not supported, use a Bedrock API key",
        ));
    };
    let Some(base) = config.base_url.as_deref() else {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            "provider 'aws-bedrock' requires base-url: https://bedrock-runtime.<region>.amazonaws.com",
        ));
    };
    Ok(Arc::new(BedrockProvider {
        base: base.trim_end_matches('/').to_string(),
        api_key,
        http: JsonHttpClient::new(Duration::from_secs(5)),
    }))
}

inventory::submit! {
    LlmProviderFactory { provider: "aws-bedrock", build }
}

/// Percent-encode a path segment: everything except RFC 3986 unreserved characters.
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl LlmProvider for BedrockProvider {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let url = format!(
                "{}/model/{}/converse",
                self.base,
                encode_segment(&request.model)
            );
            let auth = format!("Bearer {}", self.api_key);
            let headers: Vec<(&str, &str)> = vec![("authorization", auth.as_str())];
            let body = request_body(request);
            let value = self
                .http
                .post_json(&url, &headers, &body, request.deadline)
                .await
                .map_err(LlmFailure::from)?;
            parse_response(&value)
        })
    }

    fn capabilities(&self) -> LlmCapabilities {
        LlmCapabilities {
            tools: true,
            json_output: true,
            ..LlmCapabilities::default()
        }
    }
}

/// Map a neutral request to a Converse body (the model id travels in the path).
fn request_body(request: &LlmRequest) -> Value {
    let mut body = Map::new();
    // No JSON-mode flag exists, so the nearest equivalent is a system instruction.
    let system = match (&request.system, request.json_output) {
        (Some(s), true) => Some(format!("{s}\n\n{JSON_INSTRUCTION}")),
        (Some(s), false) => Some(s.clone()),
        (None, true) => Some(JSON_INSTRUCTION.to_string()),
        (None, false) => None,
    };
    if let Some(system) = system {
        body.insert("system".into(), json!([{"text": system}]));
    }

    let mut messages: Vec<Value> = Vec::new();
    for m in &request.messages {
        match m.role {
            LlmRole::User => {
                messages.push(json!({"role": "user", "content": [{"text": m.content}]}))
            }
            LlmRole::Assistant => {
                let mut blocks = Vec::new();
                if !m.content.is_empty() || m.tool_calls.is_empty() {
                    blocks.push(json!({"text": m.content}));
                }
                for call in &m.tool_calls {
                    blocks.push(json!({"toolUse": {
                        "toolUseId": call.id, "name": call.name, "input": call.arguments
                    }}));
                }
                messages.push(json!({"role": "assistant", "content": blocks}));
            }
            LlmRole::Tool => {
                let block = json!({"toolResult": {
                    "toolUseId": m.tool_call_id.clone().unwrap_or_default(),
                    "content": [{"text": m.content}],
                }});
                // Consecutive tool results share one user message.
                let merged = messages.last_mut().and_then(|last| {
                    let is_results = last["role"] == "user"
                        && last["content"]
                            .as_array()
                            .is_some_and(|a| a.iter().all(|b| b.get("toolResult").is_some()));
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

    let mut inference = Map::new();
    if let Some(n) = request.max_output_tokens {
        inference.insert("maxTokens".into(), json!(n));
    }
    if let Some(t) = request.temperature {
        inference.insert("temperature".into(), json!(t));
    }
    if !inference.is_empty() {
        body.insert("inferenceConfig".into(), Value::Object(inference));
    }

    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|t| {
                json!({"toolSpec": {
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": {"json": t.parameters},
                }})
            })
            .collect();
        body.insert("toolConfig".into(), json!({"tools": tools}));
    }
    Value::Object(body)
}

/// Map a Converse answer to a neutral response.
fn parse_response(value: &Value) -> Result<LlmResponse, LlmFailure> {
    let Some(content) = value["output"]["message"]["content"].as_array() else {
        return Err(LlmFailure::Permanent {
            detail: format!(
                "{}: the Bedrock answer has no 'output.message.content' array",
                codes::RESPONSE_INVALID
            ),
        });
    };
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in content {
        if let Some(t) = block.get("text").and_then(Value::as_str) {
            text.push_str(t);
        } else if let Some(u) = block.get("toolUse") {
            tool_calls.push(LlmToolCall {
                id: u["toolUseId"].as_str().unwrap_or_default().to_string(),
                name: u["name"].as_str().unwrap_or_default().to_string(),
                arguments: u.get("input").cloned().unwrap_or_else(|| json!({})),
            });
        }
    }
    let finish = match value.get("stopReason").and_then(Value::as_str) {
        Some("end_turn" | "stop_sequence") => FinishReason::Stop,
        Some("max_tokens" | "model_context_window_exceeded") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCalls,
        Some("guardrail_intervened" | "content_filtered") => FinishReason::Filtered,
        Some(other) => FinishReason::Other(other.to_string()),
        None => FinishReason::Other("unknown".to_string()),
    };
    let usage = LlmUsage {
        input_tokens: value["usage"]["inputTokens"].as_u64().unwrap_or(0),
        output_tokens: value["usage"]["outputTokens"].as_u64().unwrap_or(0),
    };
    Ok(LlmResponse {
        text,
        tool_calls,
        finish,
        usage,
        // The request id is a response header, which the shared client does not expose.
        provider_request_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_window_stop_reason_maps_to_length() {
        let answer = json!({"output": {"message": {"content": [{"text": "x"}]},},
            "stopReason": "model_context_window_exceeded"});
        assert_eq!(
            parse_response(&answer).unwrap().finish,
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
        }
    }

    fn tool_msg(id: &str, content: &str) -> LlmMessage {
        LlmMessage {
            role: LlmRole::Tool,
            content: content.into(),
            tool_calls: vec![],
            tool_call_id: Some(id.into()),
        }
    }

    #[test]
    fn basic_request_omits_optionals() {
        let b = request_body(&req(vec![LlmMessage::user("hi")]));
        assert!(b.get("system").is_none() && b.get("toolConfig").is_none());
        assert!(b.get("inferenceConfig").is_none() && b.get("model").is_none());
        assert_eq!(
            b["messages"][0],
            json!({"role": "user", "content": [{"text": "hi"}]})
        );
    }

    #[test]
    fn system_and_inference_config_pass_through() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.system = Some("be brief".into());
        r.temperature = Some(0.5);
        r.max_output_tokens = Some(200);
        let b = request_body(&r);
        assert_eq!(b["system"], json!([{"text": "be brief"}]));
        assert_eq!(b["inferenceConfig"]["maxTokens"], 200);
        assert_eq!(b["inferenceConfig"]["temperature"], 0.5);
    }

    #[test]
    fn json_output_appends_instruction_to_system() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.json_output = true;
        assert_eq!(request_body(&r)["system"][0]["text"], JSON_INSTRUCTION);
        r.system = Some("be brief".into());
        assert_eq!(
            request_body(&r)["system"][0]["text"],
            format!("be brief\n\n{JSON_INSTRUCTION}")
        );
    }

    #[test]
    fn tools_map_to_tool_spec() {
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.tools = vec![LlmToolSpec {
            name: "lookup".into(),
            description: "find".into(),
            parameters: json!({"type": "object"}),
        }];
        assert_eq!(
            request_body(&r)["toolConfig"]["tools"][0],
            json!({"toolSpec": {
                "name": "lookup", "description": "find",
                "inputSchema": {"json": {"type": "object"}}
            }})
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
                {"text": "checking"},
                {"toolUse": {"toolUseId": "t1", "name": "a", "input": {"x": 1}}},
                {"toolUse": {"toolUseId": "t2", "name": "b", "input": {}}},
            ])
        );
        assert_eq!(m[2]["role"], "user");
        assert_eq!(
            m[2]["content"],
            json!([
                {"toolResult": {"toolUseId": "t1", "content": [{"text": "r1"}]}},
                {"toolResult": {"toolUseId": "t2", "content": [{"text": "r2"}]}},
            ])
        );
    }

    #[test]
    fn response_text_tool_use_and_usage() {
        let r = parse_response(&json!({
            "output": {"message": {"role": "assistant", "content": [
                {"text": "a"}, {"text": "b"},
                {"toolUse": {"toolUseId": "t1", "name": "lookup", "input": {"q": "x"}}}
            ]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 11, "outputTokens": 7}
        }))
        .unwrap();
        assert_eq!(r.text, "ab");
        assert_eq!(r.tool_calls[0].id, "t1");
        assert_eq!(r.tool_calls[0].arguments, json!({"q": "x"}));
        assert_eq!(r.finish, FinishReason::ToolCalls);
        assert_eq!(
            r.usage,
            LlmUsage {
                input_tokens: 11,
                output_tokens: 7
            }
        );
        assert_eq!(r.provider_request_id, None);
    }

    #[test]
    fn stop_reasons_map() {
        let finish = |s: &str| {
            parse_response(&json!({"output": {"message": {"content": []}}, "stopReason": s}))
                .unwrap()
                .finish
        };
        assert_eq!(finish("end_turn"), FinishReason::Stop);
        assert_eq!(finish("stop_sequence"), FinishReason::Stop);
        assert_eq!(finish("max_tokens"), FinishReason::Length);
        assert_eq!(finish("tool_use"), FinishReason::ToolCalls);
        assert_eq!(finish("guardrail_intervened"), FinishReason::Filtered);
        assert_eq!(finish("content_filtered"), FinishReason::Filtered);
        assert_eq!(finish("weird"), FinishReason::Other("weird".into()));
    }

    #[test]
    fn missing_content_is_permanent() {
        match parse_response(&json!({"output": {}})).unwrap_err() {
            LlmFailure::Permanent { detail } => assert!(detail.contains(codes::RESPONSE_INVALID)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn model_id_segment_is_percent_encoded() {
        assert_eq!(
            encode_segment("anthropic.claude-3:0/x"),
            "anthropic.claude-3%3A0%2Fx"
        );
    }

    #[test]
    fn build_requires_key_and_base_url() {
        let cfg = |key: Option<&str>, base: Option<&str>| LlmProviderConfig {
            model: "test-model".into(),
            base_url: base.map(String::from),
            api_key: key.map(String::from),
        };
        let err = build(&cfg(None, Some("http://h"))).err().expect("no key");
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("SigV4"));
        let err = build(&cfg(Some("k"), None)).err().expect("no base url");
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("bedrock-runtime"));
        assert!(build(&cfg(Some("k"), Some("http://h/"))).is_ok());
        assert!(sutra_llm_spi::find_provider("aws-bedrock").is_some());
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
    async fn round_trip_sends_bearer_and_encoded_path() {
        let (base, head) = stub(
            200,
            r#"{"output":{"message":{"role":"assistant","content":[{"text":"ok"}]}},"stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":2}}"#,
        )
        .await;
        let mut r = req(vec![LlmMessage::user("hi")]);
        r.model = "vendor.model-v1:0".into();
        let out = provider(base).complete(&r).await.unwrap();
        assert_eq!(out.text, "ok");
        let head = head.await.unwrap();
        assert!(
            head.starts_with("post /model/vendor.model-v1%3a0/converse "),
            "{head}"
        );
        assert!(head.contains("authorization: bearer test-key"), "{head}");
    }

    #[tokio::test]
    async fn throttling_429_is_rate_limited() {
        let (base, _head) = stub(429, r#"{"message":"Too many requests"}"#).await;
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
}
