//! The native Google Gemini API adapter for the `llm` transport.
//!
//! Registers an [`LlmProviderFactory`] for `provider: gemini`.
//!
//! WHY A SECOND GOOGLE ROUTE. The `google-gemini` provider in `sutra-llm-openai` talks to
//! Google's OpenAI-compatible endpoint, which accepts audio as WAV only and has no video input.
//! This crate speaks the native API, so a message may carry audio in any container the model
//! reads (WAV, MP3, AAC, OGG, FLAC, Opus, WebM, M4A) and video as a clip or as sampled frames,
//! and the Live API is available as an [`AudioSession`](sutra_llm_spi::AudioSession). Choose
//! `google-gemini` for plain text and tools through the compatible route; choose `gemini` for
//! audio or video understanding and live sessions.
//!
//! WIRE SHAPES (Gemini Developer API, `v1beta`):
//! - `POST {base}/v1beta/models/{model}:generateContent` and
//!   `:streamGenerateContent?alt=sse`; the key travels in the `x-goog-api-key` header.
//! - `contents[].parts[]` carry `text`, `inlineData {mimeType, data}` (base64 audio, image
//!   frames, video), `functionCall {id?, name, args}` and `functionResponse {id?, name,
//!   response}`; roles are `user` and `model`. `systemInstruction` is a parts holder,
//!   `generationConfig` carries `maxOutputTokens`, `temperature` and `responseMimeType`.
//! - Inline media shares the request's 20 MB ceiling; larger media needs the Files API, which
//!   this adapter does not use, so an oversized request is refused before it is sent.
//!
//! THOUGHT SIGNATURES. A model that thinks may attach an opaque `thoughtSignature` to a
//! function-call part and expects it echoed on the next turn. The neutral tool call has no such
//! field, so the signature rides inside [`LlmToolCall::id`] (`<call id>#sig=<signature>`) and is
//! split back out when the assistant turn is replayed. Callers treat ids as opaque already.
//!
//! Request and response mapping are pure functions ([`request_body`], [`parse_response`]) so
//! they are testable without a network.
#![forbid(unsafe_code)]

mod live;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::{json, Map, Value};
use sutra_llm_spi::audio::adapt;
use sutra_llm_spi::{
    classify_status, codes, AudioFormat, AudioSession, AudioSessionConfig, BoxFuture, ChunkSink,
    Diagnostic, FinishReason, JsonHttpClient, LlmAudio, LlmCapabilities, LlmFailure, LlmMessage,
    LlmProvider, LlmProviderConfig, LlmProviderFactory, LlmRequest, LlmResponse, LlmRole,
    LlmToolCall, LlmUsage, LlmVideo,
};

pub use live::{
    ephemeral_token, live_setup, live_url, GeminiLiveSession, LIVE_INPUT_RATE, LIVE_OUTPUT_RATE,
};

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com";
const API_KEY_HEADER: &str = "x-goog-api-key";
/// The request-size ceiling for inline media (base64 included).
const INLINE_LIMIT: usize = 20 * 1024 * 1024;
const JSON_INSTRUCTION: &str = "Respond with a single JSON object and nothing else.";
/// Prefix of ids this adapter makes up for a call the API sent without one.
const SYNTHETIC_ID: &str = "gemini-call-";
const SIGNATURE_SEPARATOR: &str = "#sig=";

/// Containers the API lists for audio; headerless PCM and G.711 are wrapped as WAV by `adapt`.
const AUDIO_INPUT_FORMATS: &[AudioFormat] = &[
    AudioFormat::Wav,
    AudioFormat::Mp3,
    AudioFormat::Flac,
    AudioFormat::Aac,
    AudioFormat::Ogg,
    AudioFormat::Opus,
    AudioFormat::M4a,
    AudioFormat::Webm,
];

struct GeminiProvider {
    base: String,
    model: String,
    api_key: Option<String>,
    http: JsonHttpClient,
}

fn build(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    if config.model.trim().is_empty() {
        return Err(Diagnostic::error(
            codes::CONFIG_INVALID,
            "provider 'gemini' requires a non-empty model",
        ));
    }
    Ok(Arc::new(GeminiProvider {
        base: config
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/')
            .to_string(),
        model: config.model.clone(),
        api_key: config.api_key.clone(),
        http: JsonHttpClient::new(Duration::from_secs(5)),
    }))
}

inventory::submit! {
    LlmProviderFactory { provider: "gemini", build }
}

fn permanent(detail: impl Into<String>) -> LlmFailure {
    LlmFailure::Permanent {
        detail: detail.into(),
    }
}

/// The model id as the URL path wants it: without a `models/` prefix, and only characters a
/// path segment can carry.
fn model_path(model: &str) -> Result<&str, LlmFailure> {
    let id = model.strip_prefix("models/").unwrap_or(model);
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        return Err(permanent("the model id has characters a URL cannot carry"));
    }
    Ok(id)
}

impl GeminiProvider {
    fn headers(&self) -> Vec<(&str, &str)> {
        match &self.api_key {
            Some(key) => vec![(API_KEY_HEADER, key.as_str())],
            None => Vec::new(),
        }
    }

    fn url(&self, request: &LlmRequest, method: &str) -> Result<String, LlmFailure> {
        Ok(format!(
            "{}/v1beta/models/{}:{method}",
            self.base,
            model_path(&request.model)?
        ))
    }
}

impl LlmProvider for GeminiProvider {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let url = self.url(request, "generateContent")?;
            let body = request_body(request)?;
            let value = self
                .http
                .post_json(&url, &self.headers(), &body, request.deadline)
                .await
                .map_err(LlmFailure::from)?;
            parse_response(&value)
        })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: &'a LlmRequest,
        on_chunk: ChunkSink<'a>,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            let url = self.url(request, "streamGenerateContent?alt=sse")?;
            let body = request_body(request)?;
            let mut answer = Answer::default();
            self.http
                .post_sse(
                    &url,
                    &self.headers(),
                    &body,
                    request.deadline,
                    &mut |event| {
                        if let Ok(value) = serde_json::from_str::<Value>(&event.data) {
                            answer.feed(&value, on_chunk);
                        }
                    },
                )
                .await
                .map_err(LlmFailure::from)?;
            answer.finish()
        })
    }

    fn capabilities(&self) -> LlmCapabilities {
        LlmCapabilities {
            tools: true,
            json_output: true,
            streaming: true,
            audio_input: true,
            // The model hears the audio itself, and reads video frames and clips.
            audio_understanding: true,
            video_understanding: true,
            realtime: self.model.contains("live"),
            ..LlmCapabilities::default()
        }
    }

    fn audio_input_formats(&self) -> Vec<AudioFormat> {
        AUDIO_INPUT_FORMATS.to_vec()
    }

    fn open_audio_session<'a>(
        &'a self,
        config: &'a AudioSessionConfig,
    ) -> BoxFuture<'a, Result<Box<dyn AudioSession>, LlmFailure>> {
        Box::pin(async move {
            let session =
                GeminiLiveSession::open(&self.http, &self.base, self.api_key.as_deref(), config)
                    .await?;
            Ok(Box::new(session) as Box<dyn AudioSession>)
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Request mapping
// ---------------------------------------------------------------------------------------------

fn inline_part(media_type: &str, data: &[u8]) -> Value {
    json!({"inlineData": {"mimeType": media_type, "data": BASE64.encode(data)}})
}

/// Split a neutral tool-call id into the API's call id and its thought signature.
fn split_id(id: &str) -> (&str, Option<&str>) {
    match id.rsplit_once(SIGNATURE_SEPARATOR) {
        Some((call, signature)) => (call, Some(signature)),
        None => (id, None),
    }
}

fn media_parts(message: &LlmMessage, inline_bytes: &mut usize) -> Result<Vec<Value>, LlmFailure> {
    let mut parts = Vec::new();
    if let Some(audio) = &message.audio {
        let audio: LlmAudio = adapt(audio, AUDIO_INPUT_FORMATS)
            .map_err(|e| permanent(format!("{}: {e}", codes::AUDIO_UNSUPPORTED)))?;
        *inline_bytes += audio.data.len();
        parts.push(inline_part(&audio.format.media_type(), &audio.data));
    }
    match &message.video {
        Some(LlmVideo::Clip { data, media_type }) => {
            *inline_bytes += data.len();
            parts.push(inline_part(media_type, data));
        }
        Some(LlmVideo::Frames(frames)) => {
            for frame in frames {
                if let Some(at) = frame.at_ms {
                    parts.push(json!({"text": format!("[frame at {:.1}s]", at as f64 / 1000.0)}));
                }
                *inline_bytes += frame.data.len();
                parts.push(inline_part(&frame.media_type, &frame.data));
            }
        }
        None => {}
    }
    Ok(parts)
}

/// A tool result as the `response` object `functionResponse` requires.
fn tool_response(content: &str) -> Value {
    match serde_json::from_str::<Value>(content) {
        Ok(Value::Object(object)) => Value::Object(object),
        Ok(other) => json!({"result": other}),
        Err(_) => json!({"result": content}),
    }
}

/// Drop JSON-Schema keywords the function-declaration schema subset rejects.
fn declaration_schema(schema: &Value) -> Value {
    match schema {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| !matches!(key.as_str(), "additionalProperties" | "$schema"))
                .map(|(key, value)| (key.clone(), declaration_schema(value)))
                .collect::<Map<_, _>>(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(declaration_schema).collect()),
        other => other.clone(),
    }
}

/// The `generateContent` request body for `request`.
pub fn request_body(request: &LlmRequest) -> Result<Value, LlmFailure> {
    let names: HashMap<&str, &str> = request
        .messages
        .iter()
        .flat_map(|m| m.tool_calls.iter())
        .map(|call| (call.id.as_str(), call.name.as_str()))
        .collect();
    let mut inline_bytes = 0usize;
    let mut contents: Vec<Value> = Vec::new();
    let mut last_was_tool = false;
    for message in &request.messages {
        match message.role {
            LlmRole::Tool => {
                let id = message.tool_call_id.as_deref().unwrap_or("");
                let (call_id, _) = split_id(id);
                let name = names.get(id).copied().ok_or_else(|| {
                    permanent(format!(
                        "{}: a tool result answers no known tool call",
                        codes::REQUEST_INVALID
                    ))
                })?;
                let mut response =
                    json!({"name": name, "response": tool_response(&message.content)});
                if !call_id.starts_with(SYNTHETIC_ID) {
                    response["id"] = json!(call_id);
                }
                let part = json!({"functionResponse": response});
                // Parallel results travel together in one turn.
                if last_was_tool {
                    if let Some(parts) = contents.last_mut().and_then(|c| c["parts"].as_array_mut())
                    {
                        parts.push(part);
                        continue;
                    }
                }
                contents.push(json!({"role": "user", "parts": [part]}));
                last_was_tool = true;
                continue;
            }
            LlmRole::User | LlmRole::Assistant => last_was_tool = false,
        }
        let mut parts = media_parts(message, &mut inline_bytes)?;
        if !message.content.is_empty() {
            parts.push(json!({"text": message.content}));
        }
        for call in &message.tool_calls {
            let (call_id, signature) = split_id(&call.id);
            let mut function = json!({"name": call.name, "args": call.arguments});
            if !call_id.starts_with(SYNTHETIC_ID) {
                function["id"] = json!(call_id);
            }
            let mut part = json!({"functionCall": function});
            if let Some(signature) = signature {
                part["thoughtSignature"] = json!(signature);
            }
            parts.push(part);
        }
        if parts.is_empty() {
            continue;
        }
        let role = if message.role == LlmRole::Assistant {
            "model"
        } else {
            "user"
        };
        contents.push(json!({"role": role, "parts": parts}));
    }
    if inline_bytes / 3 * 4 > INLINE_LIMIT {
        return Err(permanent(format!(
            "{}: the audio and video in this request exceed the 20 MB inline limit",
            codes::REQUEST_INVALID
        )));
    }

    let mut system = request.system.clone();
    let mut generation = Map::new();
    if let Some(max) = request.max_output_tokens {
        generation.insert("maxOutputTokens".into(), json!(max));
    }
    if let Some(temperature) = request.temperature {
        generation.insert("temperature".into(), json!(temperature));
    }
    if request.json_output {
        if request.tools.is_empty() {
            generation.insert("responseMimeType".into(), json!("application/json"));
        } else {
            // A JSON response type and function calling are not combined; ask in words.
            system = Some(match system {
                Some(text) => format!("{text}\n\n{JSON_INSTRUCTION}"),
                None => JSON_INSTRUCTION.to_string(),
            });
        }
    }

    let mut body = json!({"contents": contents});
    if let Some(system) = system {
        body["systemInstruction"] = json!({"parts": [{"text": system}]});
    }
    if !generation.is_empty() {
        body["generationConfig"] = Value::Object(generation);
    }
    if !request.tools.is_empty() {
        let declarations: Vec<Value> = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": declaration_schema(&tool.parameters),
                })
            })
            .collect();
        body["tools"] = json!([{"functionDeclarations": declarations}]);
    }
    Ok(body)
}

// ---------------------------------------------------------------------------------------------
// Response mapping
// ---------------------------------------------------------------------------------------------

fn map_finish(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("STOP") => FinishReason::Stop,
        Some("MAX_TOKENS") => FinishReason::Length,
        Some(
            "SAFETY"
            | "RECITATION"
            | "LANGUAGE"
            | "BLOCKLIST"
            | "PROHIBITED_CONTENT"
            | "SPII"
            | "IMAGE_SAFETY"
            | "IMAGE_PROHIBITED_CONTENT",
        ) => FinishReason::Filtered,
        Some(other) => FinishReason::Other(other.to_ascii_lowercase()),
        None => FinishReason::Other("unknown".to_string()),
    }
}

fn invalid(detail: impl std::fmt::Display) -> LlmFailure {
    permanent(format!("{}: {detail}", codes::RESPONSE_INVALID))
}

/// Map an API error object (`{"code", "message", "status"}`) to a failure; the numeric code is
/// the HTTP status the same error would have carried.
fn error_failure(error: &Value) -> LlmFailure {
    let status = error["status"].as_str().unwrap_or("UNKNOWN");
    let detail = format!(
        "{status}: {}",
        error["message"].as_str().unwrap_or("no detail")
    );
    let code = error["code"].as_u64().unwrap_or(0) as u16;
    classify_status(code, None, detail)
}

/// Accumulates one or more `GenerateContentResponse` objects (a single reply, or the chunks of
/// a stream) into a response.
#[derive(Default)]
struct Answer {
    text: String,
    tool_calls: Vec<LlmToolCall>,
    finish: Option<String>,
    blocked: bool,
    usage: LlmUsage,
    id: Option<String>,
    error: Option<LlmFailure>,
    seen: bool,
}

impl Answer {
    fn feed(&mut self, value: &Value, on_chunk: ChunkSink<'_>) {
        if value["error"].is_object() {
            self.error = Some(error_failure(&value["error"]));
            return;
        }
        self.seen = true;
        if let Some(id) = value["responseId"].as_str() {
            self.id = Some(id.to_string());
        }
        if value["promptFeedback"]["blockReason"].is_string() {
            self.blocked = true;
        }
        let usage = &value["usageMetadata"];
        if usage.is_object() {
            self.usage.input_tokens = usage["promptTokenCount"].as_u64().unwrap_or(0);
            // Thinking tokens are billed as output.
            self.usage.output_tokens = usage["candidatesTokenCount"].as_u64().unwrap_or(0)
                + usage["thoughtsTokenCount"].as_u64().unwrap_or(0);
        }
        let candidate = &value["candidates"][0];
        if let Some(reason) = candidate["finishReason"].as_str() {
            self.finish = Some(reason.to_string());
        }
        for part in candidate["content"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let Some(call) = part.get("functionCall").filter(|c| c.is_object()) {
                let number = self.tool_calls.len();
                let mut id = call["id"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{SYNTHETIC_ID}{number}"));
                if let Some(signature) = part["thoughtSignature"].as_str() {
                    id = format!("{id}{SIGNATURE_SEPARATOR}{signature}");
                }
                self.tool_calls.push(LlmToolCall {
                    id,
                    name: call["name"].as_str().unwrap_or("").to_string(),
                    arguments: if call["args"].is_null() {
                        json!({})
                    } else {
                        call["args"].clone()
                    },
                });
            } else if part["thought"] != json!(true) {
                if let Some(text) = part["text"].as_str().filter(|t| !t.is_empty()) {
                    self.text.push_str(text);
                    on_chunk(text);
                }
            }
        }
    }

    fn finish(self) -> Result<LlmResponse, LlmFailure> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if !self.seen {
            return Err(invalid("the answer carried no content"));
        }
        let finish = match (&self.finish, self.blocked) {
            (None, true) => FinishReason::Filtered,
            (Some(reason), _) if !self.tool_calls.is_empty() && reason == "STOP" => {
                FinishReason::ToolCalls
            }
            (None, false) if !self.tool_calls.is_empty() => FinishReason::ToolCalls,
            (None, false) if self.text.is_empty() => {
                return Err(invalid("the answer has no candidate"));
            }
            (reason, _) => map_finish(reason.as_deref()),
        };
        Ok(LlmResponse {
            text: self.text,
            tool_calls: self.tool_calls,
            finish,
            usage: self.usage,
            provider_request_id: self.id,
            audio: None,
            transcript: None,
        })
    }
}

/// Map a `generateContent` response body to a response.
pub fn parse_response(value: &Value) -> Result<LlmResponse, LlmFailure> {
    let mut answer = Answer::default();
    answer.feed(value, &|_| {});
    answer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_llm_spi::{LlmToolSpec, LlmVideoFrame, RetryableClass};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn req(messages: Vec<LlmMessage>) -> LlmRequest {
        LlmRequest {
            model: "gemini-test".into(),
            system: None,
            messages,
            tools: Vec::new(),
            json_output: false,
            max_output_tokens: None,
            temperature: None,
            deadline: Duration::from_secs(5),
            audio_output: None,
            output_voice: None,
        }
    }

    fn weather_tool() -> LlmToolSpec {
        LlmToolSpec {
            name: "get_weather".into(),
            description: "Weather".into(),
            parameters: json!({"type": "object", "additionalProperties": false,
                "properties": {"city": {"type": "string"}}, "required": ["city"]}),
        }
    }

    #[test]
    fn text_request_maps_system_and_generation_config() {
        let mut r = req(vec![
            LlmMessage::user("hi"),
            LlmMessage {
                role: LlmRole::Assistant,
                content: "hello".into(),
                ..LlmMessage::user("")
            },
        ]);
        r.system = Some("be brief".into());
        r.max_output_tokens = Some(64);
        r.temperature = Some(0.5);
        let body = request_body(&r).unwrap();
        assert_eq!(
            body,
            json!({
                "contents": [
                    {"role": "user", "parts": [{"text": "hi"}]},
                    {"role": "model", "parts": [{"text": "hello"}]},
                ],
                "systemInstruction": {"parts": [{"text": "be brief"}]},
                "generationConfig": {"maxOutputTokens": 64, "temperature": 0.5},
            })
        );
    }

    #[test]
    fn json_mode_sets_mime_type_or_falls_back_to_words_with_tools() {
        let mut r = req(vec![LlmMessage::user("x")]);
        r.json_output = true;
        let body = request_body(&r).unwrap();
        assert_eq!(
            body["generationConfig"]["responseMimeType"],
            "application/json"
        );
        assert!(body.get("systemInstruction").is_none());
        r.tools = vec![weather_tool()];
        let body = request_body(&r).unwrap();
        assert!(body.get("generationConfig").is_none());
        assert_eq!(
            body["systemInstruction"]["parts"][0]["text"],
            JSON_INSTRUCTION
        );
    }

    #[test]
    fn audio_and_video_parts_are_inline_data_before_the_text() {
        let mut m = LlmMessage::user("what is said and shown?");
        m.audio = Some(LlmAudio {
            data: vec![1, 2, 3],
            format: AudioFormat::Mp3,
        });
        m.video = Some(LlmVideo::Frames(vec![
            LlmVideoFrame {
                data: vec![9],
                media_type: "image/jpeg".into(),
                at_ms: Some(1500),
            },
            LlmVideoFrame {
                data: vec![8],
                media_type: "image/png".into(),
                at_ms: None,
            },
        ]));
        let body = request_body(&req(vec![m])).unwrap();
        let parts = body["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(
            parts[0],
            json!({"inlineData": {"mimeType": "audio/mpeg", "data": BASE64.encode([1, 2, 3])}})
        );
        assert_eq!(parts[1], json!({"text": "[frame at 1.5s]"}));
        assert_eq!(parts[2]["inlineData"]["mimeType"], "image/jpeg");
        assert_eq!(parts[3]["inlineData"]["mimeType"], "image/png");
        assert_eq!(parts[4], json!({"text": "what is said and shown?"}));

        let mut clip = LlmMessage::user("summarise");
        clip.video = Some(LlmVideo::Clip {
            data: vec![7, 7],
            media_type: "video/mp4".into(),
        });
        let body = request_body(&req(vec![clip])).unwrap();
        assert_eq!(
            body["contents"][0]["parts"][0]["inlineData"]["mimeType"],
            "video/mp4"
        );
    }

    #[test]
    fn g711_audio_is_wrapped_as_wav() {
        let mut m = LlmMessage::user("");
        m.audio = Some(LlmAudio {
            data: vec![0xFF; 160],
            format: AudioFormat::Pcmu,
        });
        let body = request_body(&req(vec![m])).unwrap();
        let part = &body["contents"][0]["parts"][0]["inlineData"];
        assert_eq!(part["mimeType"], "audio/wav");
        let wav = BASE64.decode(part["data"].as_str().unwrap()).unwrap();
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(body["contents"][0]["parts"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn oversized_media_is_refused_before_sending() {
        let mut m = LlmMessage::user("x");
        m.video = Some(LlmVideo::Clip {
            data: vec![0; 16 * 1024 * 1024],
            media_type: "video/mp4".into(),
        });
        match request_body(&req(vec![m])).unwrap_err() {
            LlmFailure::Permanent { detail } => assert!(detail.contains("20 MB"), "{detail}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn tools_calls_and_results_round_trip_through_the_wire_shape() {
        let mut r = req(vec![
            LlmMessage::user("weather?"),
            LlmMessage {
                role: LlmRole::Assistant,
                content: String::new(),
                tool_calls: vec![
                    LlmToolCall {
                        id: "c1#sig=SIG".into(),
                        name: "get_weather".into(),
                        arguments: json!({"city": "Oslo"}),
                    },
                    LlmToolCall {
                        id: "gemini-call-1".into(),
                        name: "get_weather".into(),
                        arguments: json!({"city": "Rome"}),
                    },
                ],
                ..LlmMessage::user("")
            },
            LlmMessage {
                role: LlmRole::Tool,
                content: r#"{"temp": 3}"#.into(),
                tool_call_id: Some("c1#sig=SIG".into()),
                ..LlmMessage::user("")
            },
            LlmMessage {
                role: LlmRole::Tool,
                content: "sunny".into(),
                tool_call_id: Some("gemini-call-1".into()),
                ..LlmMessage::user("")
            },
        ]);
        r.tools = vec![weather_tool()];
        let body = request_body(&r).unwrap();
        assert_eq!(
            body["tools"],
            json!([{"functionDeclarations": [{
                "name": "get_weather", "description": "Weather",
                "parameters": {"type": "object",
                    "properties": {"city": {"type": "string"}}, "required": ["city"]},
            }]}])
        );
        let model = &body["contents"][1];
        assert_eq!(model["role"], "model");
        assert_eq!(
            model["parts"][0],
            json!({"functionCall": {"id": "c1", "name": "get_weather", "args": {"city": "Oslo"}},
                   "thoughtSignature": "SIG"})
        );
        assert_eq!(
            model["parts"][1],
            json!({"functionCall": {"name": "get_weather", "args": {"city": "Rome"}}})
        );
        let results = &body["contents"][2];
        assert_eq!(results["role"], "user");
        assert_eq!(
            results["parts"],
            json!([
                {"functionResponse": {"id": "c1", "name": "get_weather", "response": {"temp": 3}}},
                {"functionResponse": {"name": "get_weather", "response": {"result": "sunny"}}},
            ])
        );
        assert_eq!(body["contents"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn a_tool_result_without_a_call_is_a_request_error() {
        let m = LlmMessage {
            role: LlmRole::Tool,
            content: "x".into(),
            tool_call_id: Some("nope".into()),
            ..LlmMessage::user("")
        };
        assert!(request_body(&req(vec![m])).is_err());
    }

    #[test]
    fn text_response_maps_usage_and_finish() {
        let r = parse_response(&json!({
            "candidates": [{"content": {"role": "model", "parts": [
                {"text": "secret thoughts", "thought": true}, {"text": "Hello"}, {"text": " there"}]},
                "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 3,
                              "thoughtsTokenCount": 2, "totalTokenCount": 12},
            "responseId": "resp-1",
        }))
        .unwrap();
        assert_eq!(r.text, "Hello there");
        assert_eq!(r.finish, FinishReason::Stop);
        assert_eq!(
            r.usage,
            LlmUsage {
                input_tokens: 7,
                output_tokens: 5
            }
        );
        assert_eq!(r.provider_request_id.as_deref(), Some("resp-1"));
    }

    #[test]
    fn function_call_response_is_tool_calls_with_signature_in_the_id() {
        let r = parse_response(&json!({
            "candidates": [{"content": {"role": "model", "parts": [
                {"functionCall": {"name": "get_weather", "args": {"city": "Oslo"}},
                 "thoughtSignature": "abc"},
                {"functionCall": {"id": "k9", "name": "other"}}]},
                "finishReason": "STOP"}],
        }))
        .unwrap();
        assert_eq!(r.finish, FinishReason::ToolCalls);
        assert_eq!(r.tool_calls[0].id, "gemini-call-0#sig=abc");
        assert_eq!(r.tool_calls[0].arguments, json!({"city": "Oslo"}));
        assert_eq!(r.tool_calls[1].id, "k9");
        assert_eq!(r.tool_calls[1].arguments, json!({}));
    }

    #[test]
    fn finish_reasons_map() {
        let finish = |reason: &str| {
            parse_response(
                &json!({"candidates": [{"content": {"parts": [{"text": "x"}]},
                "finishReason": reason}]}),
            )
            .unwrap()
            .finish
        };
        assert_eq!(finish("STOP"), FinishReason::Stop);
        assert_eq!(finish("MAX_TOKENS"), FinishReason::Length);
        for filtered in [
            "SAFETY",
            "RECITATION",
            "BLOCKLIST",
            "PROHIBITED_CONTENT",
            "SPII",
        ] {
            assert_eq!(finish(filtered), FinishReason::Filtered, "{filtered}");
        }
        assert_eq!(
            finish("MALFORMED_FUNCTION_CALL"),
            FinishReason::Other("malformed_function_call".into())
        );
    }

    #[test]
    fn a_blocked_prompt_is_a_filtered_result() {
        let r = parse_response(&json!({"promptFeedback": {"blockReason": "SAFETY"},
            "usageMetadata": {"promptTokenCount": 4}}))
        .unwrap();
        assert_eq!(r.finish, FinishReason::Filtered);
        assert_eq!(r.text, "");
        assert_eq!(r.usage.input_tokens, 4);
    }

    #[test]
    fn empty_and_error_bodies_fail() {
        match parse_response(&json!({})).unwrap_err() {
            LlmFailure::Permanent { detail } => assert!(detail.contains(codes::RESPONSE_INVALID)),
            other => panic!("{other:?}"),
        }
        match parse_response(
            &json!({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED",
            "message": "quota"}}),
        )
        .unwrap_err()
        {
            LlmFailure::Retryable { class, detail, .. } => {
                assert_eq!(class, RetryableClass::RateLimited);
                assert!(detail.contains("RESOURCE_EXHAUSTED"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn build_requires_a_model_and_registers() {
        let cfg = |model: &str| LlmProviderConfig {
            model: model.into(),
            base_url: None,
            api_key: None,
        };
        assert_eq!(
            build(&cfg(" ")).err().expect("rejected").code,
            codes::CONFIG_INVALID
        );
        assert!(build(&cfg("gemini-test")).is_ok());
        assert!(sutra_llm_spi::find_provider("gemini").is_some());
    }

    #[test]
    fn capabilities_reflect_the_native_surface() {
        let caps = |model: &str| {
            build(&LlmProviderConfig {
                model: model.into(),
                base_url: None,
                api_key: None,
            })
            .unwrap()
            .capabilities()
        };
        let c = caps("gemini-test");
        assert!(c.tools && c.json_output && c.streaming && c.audio_input);
        assert!(c.audio_understanding && c.video_understanding);
        assert!(!c.async_jobs && !c.audio_output && !c.realtime);
        assert!(caps("gemini-live-test").realtime);
    }

    #[test]
    fn model_ids_are_checked_for_the_url() {
        assert_eq!(
            model_path("models/gemini-2.5-flash").unwrap(),
            "gemini-2.5-flash"
        );
        assert!(model_path("a/b").is_err());
        assert!(model_path("").is_err());
    }

    /// One-shot stub: answers `status` + `body` with `content_type`; returns the raw request.
    async fn stub(
        status: u16,
        content_type: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 65536];
            let n = socket.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        (format!("http://{addr}/"), handle)
    }

    fn provider(base: String) -> Arc<dyn LlmProvider> {
        build(&LlmProviderConfig {
            model: "gemini-test".into(),
            base_url: Some(base),
            api_key: Some("test-key".into()),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn generate_content_round_trip_sends_the_key_header() {
        let (base, seen) = stub(
            200,
            "application/json",
            r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2}}"#,
        )
        .await;
        let r = provider(base)
            .complete(&req(vec![LlmMessage::user("hi")]))
            .await
            .unwrap();
        assert_eq!(r.text, "ok");
        let raw = seen.await.unwrap();
        assert!(
            raw.starts_with("POST /v1beta/models/gemini-test:generateContent "),
            "{raw}"
        );
        assert!(
            raw.to_lowercase().contains("x-goog-api-key: test-key"),
            "{raw}"
        );
        assert!(raw.contains(r#""contents""#), "{raw}");
    }

    #[tokio::test]
    async fn rate_limit_and_overload_are_retryable_and_bad_requests_are_not() {
        let class = |failure: LlmFailure| match failure {
            LlmFailure::Retryable { class, .. } => Some(class),
            LlmFailure::Permanent { .. } => None,
        };
        for (status, expected) in [
            (429, Some(RetryableClass::RateLimited)),
            (503, Some(RetryableClass::Unavailable)),
            (504, Some(RetryableClass::Timeout)),
            (400, None),
            (403, None),
        ] {
            let (base, _seen) = stub(
                status,
                "application/json",
                r#"{"error":{"code":0,"message":"m","status":"S"}}"#,
            )
            .await;
            let err = provider(base)
                .complete(&req(vec![LlmMessage::user("hi")]))
                .await
                .unwrap_err();
            assert_eq!(class(err), expected, "status {status}");
        }
    }

    const SSE: &str = concat!(
        "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hel\"}]}}],\"responseId\":\"r7\"}\r\n\r\n",
        "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"lo\"}]},\"finishReason\":\"STOP\"}],",
        "\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2}}\r\n\r\n",
    );

    #[tokio::test]
    async fn streaming_round_trip_emits_chunks_and_accumulates() {
        let (base, seen) = stub(200, "text/event-stream", SSE).await;
        let chunks = std::sync::Mutex::new(Vec::new());
        let sink = |t: &str| chunks.lock().unwrap().push(t.to_string());
        let r = provider(base)
            .complete_streaming(&req(vec![LlmMessage::user("hi")]), &sink)
            .await
            .unwrap();
        assert_eq!(r.text, "Hello");
        assert_eq!(r.finish, FinishReason::Stop);
        assert_eq!(r.usage.output_tokens, 2);
        assert_eq!(r.provider_request_id.as_deref(), Some("r7"));
        assert_eq!(chunks.into_inner().unwrap(), vec!["Hel", "lo"]);
        let raw = seen.await.unwrap();
        assert!(
            raw.starts_with("POST /v1beta/models/gemini-test:streamGenerateContent?alt=sse "),
            "{raw}"
        );
    }

    #[test]
    fn streamed_function_calls_and_mid_stream_errors() {
        let mut answer = Answer::default();
        answer.feed(
            &json!({"candidates": [{"content": {"parts": [
                {"functionCall": {"name": "f", "args": {"a": 1}}}]}, "finishReason": "STOP"}]}),
            &|_| {},
        );
        let r = answer.finish().unwrap();
        assert_eq!(r.finish, FinishReason::ToolCalls);
        assert_eq!(r.tool_calls.len(), 1);

        let mut answer = Answer::default();
        answer.feed(
            &json!({"candidates": [{"content": {"parts": [{"text": "a"}]}}]}),
            &|_| {},
        );
        answer.feed(
            &json!({"error": {"code": 503, "message": "overloaded"}}),
            &|_| {},
        );
        assert!(matches!(
            answer.finish(),
            Err(LlmFailure::Retryable {
                class: RetryableClass::Unavailable,
                ..
            })
        ));
    }
}
