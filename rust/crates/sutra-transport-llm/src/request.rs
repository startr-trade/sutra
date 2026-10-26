//! The two wire shapes this transport reads: the qualified `llm://<module_key>/<channel>`
//! destination and the JSON request body a channel call carries.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::Value;
use sutra_channels::Diagnostic;
use sutra_llm_spi::{
    codes, AudioFormat, LlmAudio, LlmMessage, LlmRole, LlmToolCall, LlmToolSpec, LlmVideo,
    LlmVideoFrame,
};

/// Split `llm://<tenant>/<module>/<version>/<channel>` into `(tenant, module_key, channel)`:
/// the channel is the LAST segment, the tenant the FIRST.
pub fn parse_destination(destination: &str) -> Option<(String, String, String)> {
    let rest = destination.strip_prefix("llm://")?;
    let (module_key, channel) = rest.rsplit_once('/')?;
    if module_key.is_empty() || channel.is_empty() {
        return None;
    }
    let tenant = module_key.split('/').next().filter(|t| !t.is_empty())?;
    Some((
        tenant.to_string(),
        module_key.to_string(),
        channel.to_string(),
    ))
}

/// A parsed request body. `raw` is the body as sent, echoed on the reply.
#[derive(Debug, Clone, PartialEq)]
pub struct CallRequest {
    pub system: Option<String>,
    pub messages: Vec<LlmMessage>,
    pub tools: Vec<LlmToolSpec>,
    pub json_output: bool,
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    /// `audioOutput`: the format the spoken answer is wanted in.
    pub audio_output: Option<AudioFormat>,
    /// `outputVoice`: the voice of the spoken answer.
    pub output_voice: Option<String>,
    /// Media parts sent as object-store references, whose content is still to be read
    /// ([`crate::media::resolve_media`]); their slots hold empty data until then.
    pub media_refs: Vec<MediaReference>,
    /// The body as sent, with media `data` replaced by its size (see [`echo_without_media`]).
    pub raw: Value,
}

impl CallRequest {
    /// Whether any message carries audio.
    pub fn has_audio_input(&self) -> bool {
        self.messages.iter().any(|m| m.audio.is_some())
    }

    /// Whether any message carries video.
    pub fn has_video_input(&self) -> bool {
        self.messages.iter().any(|m| m.video.is_some())
    }

    /// Whether the call involves audio either way.
    pub fn involves_audio(&self) -> bool {
        self.has_audio_input() || self.audio_output.is_some()
    }
}

/// Where a referenced object's content goes once it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaSlot {
    /// The message's `audio`.
    Audio,
    /// The message's video clip.
    VideoClip,
    /// One frame of the message's video (an `image` part is frame 0 of a one-frame video).
    VideoFrame(usize),
}

/// A media part sent as `{"ref": "<object URI>", "sha256": "<hex>", "format": …}` instead of
/// inline base64.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaReference {
    /// Index into [`CallRequest::messages`].
    pub message: usize,
    pub slot: MediaSlot,
    pub uri: String,
    /// Lower-case hex SHA-256 of the stored object.
    pub sha256: String,
}

/// A media part's content: inline bytes, or a reference to read later.
enum MediaSource {
    Inline(Vec<u8>),
    Ref { uri: String, sha256: String },
}

fn media_source(
    object: &serde_json::Map<String, Value>,
    at: &str,
) -> Result<MediaSource, Diagnostic> {
    match (object.get("data"), object.get("ref")) {
        (Some(_), Some(_)) => Err(invalid(format!("{at} carries both 'data' and 'ref'"))),
        (None, Some(reference)) => {
            let uri = reference
                .as_str()
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .ok_or_else(|| invalid(format!("{at}.ref must be an object URI string")))?;
            let sha256 = opt_str(object, "sha256")
                .map(str::trim)
                .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or_else(|| {
                    invalid(format!(
                        "{at}.sha256 is required with 'ref' (64 hex characters)"
                    ))
                })?;
            Ok(MediaSource::Ref {
                uri: uri.to_string(),
                sha256: sha256.to_ascii_lowercase(),
            })
        }
        _ => {
            let data = opt_str(object, "data").ok_or_else(|| {
                invalid(format!(
                    "{at}.data must be a base64 string (or give 'ref' and 'sha256')"
                ))
            })?;
            BASE64
                .decode(data.trim())
                .map(MediaSource::Inline)
                .map_err(|_| invalid(format!("{at}.data is not valid base64")))
        }
    }
}

/// Inline bytes as they are; a reference recorded in `refs` with an empty placeholder.
fn media_bytes(
    source: MediaSource,
    message: usize,
    slot: MediaSlot,
    refs: &mut Vec<MediaReference>,
) -> Vec<u8> {
    match source {
        MediaSource::Inline(data) => data,
        MediaSource::Ref { uri, sha256 } => {
            refs.push(MediaReference {
                message,
                slot,
                uri,
                sha256,
            });
            Vec::new()
        }
    }
}

fn parse_audio(
    value: &Value,
    at: &str,
    message: usize,
    refs: &mut Vec<MediaReference>,
) -> Result<LlmAudio, Diagnostic> {
    let Some(object) = value.as_object() else {
        return Err(invalid(format!(
            "{at} must be an object with 'data' (or 'ref') and 'format'"
        )));
    };
    let format = opt_str(object, "format")
        .ok_or_else(|| invalid(format!("{at}.format is required (a media type)")))?;
    let format =
        AudioFormat::parse(format).map_err(|reason| invalid(format!("{at}.format: {reason}")))?;
    let source = media_source(object, at)?;
    if matches!(&source, MediaSource::Inline(data) if data.is_empty()) {
        return Err(invalid(format!("{at}.data is empty")));
    }
    Ok(LlmAudio {
        data: media_bytes(source, message, MediaSlot::Audio, refs),
        format,
    })
}

fn media_type(
    object: &serde_json::Map<String, Value>,
    at: &str,
    prefix: &str,
) -> Result<String, Diagnostic> {
    opt_str(object, "format")
        .map(str::trim)
        .filter(|f| f.to_ascii_lowercase().starts_with(prefix))
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("{at}.format must be a '{prefix}…' media type")))
}

fn parse_frame(
    frame: &Value,
    at: &str,
    message: usize,
    index: usize,
    refs: &mut Vec<MediaReference>,
) -> Result<LlmVideoFrame, Diagnostic> {
    let Some(frame) = frame.as_object() else {
        return Err(invalid(format!("{at} must be an object")));
    };
    let media_type = media_type(frame, at, "image/")?;
    let source = media_source(frame, at)?;
    Ok(LlmVideoFrame {
        data: media_bytes(source, message, MediaSlot::VideoFrame(index), refs),
        media_type,
        at_ms: frame.get("atMs").and_then(Value::as_u64),
    })
}

fn parse_video(
    value: &Value,
    at: &str,
    message: usize,
    refs: &mut Vec<MediaReference>,
) -> Result<LlmVideo, Diagnostic> {
    let Some(object) = value.as_object() else {
        return Err(invalid(format!("{at} must be an object")));
    };
    if let Some(frames) = object.get("frames") {
        let Some(items) = frames.as_array().filter(|f| !f.is_empty()) else {
            return Err(invalid(format!("{at}.frames must be a non-empty list")));
        };
        return items
            .iter()
            .enumerate()
            .map(|(i, frame)| parse_frame(frame, &format!("{at}.frames[{i}]"), message, i, refs))
            .collect::<Result<Vec<_>, _>>()
            .map(LlmVideo::Frames);
    }
    let media_type = media_type(object, at, "video/")?;
    let source = media_source(object, at)?;
    Ok(LlmVideo::Clip {
        data: media_bytes(source, message, MediaSlot::VideoClip, refs),
        media_type,
    })
}

/// The request as echoed on the reply: every `audio` / `video` part keeps its format, and its
/// `data` is replaced by `bytes` (the decoded size), so the process does not get its media back.
pub fn echo_without_media(raw: &Value) -> Value {
    let mut echo = raw.clone();
    let strip = |part: &mut Value| {
        if let Some(object) = part.as_object_mut() {
            if let Some(Value::String(data)) = object.remove("data") {
                let bytes = BASE64.decode(data.trim()).map(|d| d.len()).unwrap_or(0);
                object.insert("bytes".into(), Value::from(bytes));
            }
        }
    };
    if let Some(messages) = echo.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if let Some(audio) = message.get_mut("audio") {
                strip(audio);
            }
            if let Some(video) = message.get_mut("video") {
                strip(video);
                if let Some(frames) = video.get_mut("frames").and_then(Value::as_array_mut) {
                    frames.iter_mut().for_each(strip);
                }
            }
        }
    }
    echo
}

fn invalid(message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(codes::REQUEST_INVALID, message)
}

fn opt_str<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

fn parse_tool_calls(value: Option<&Value>, index: usize) -> Result<Vec<LlmToolCall>, Diagnostic> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(invalid(format!(
            "messages[{index}].toolCalls must be a list"
        )));
    };
    items
        .iter()
        .map(|item| {
            let id = item.get("id").and_then(Value::as_str);
            let name = item.get("name").and_then(Value::as_str);
            match (id, name) {
                (Some(id), Some(name)) => Ok(LlmToolCall {
                    id: id.to_string(),
                    name: name.to_string(),
                    arguments: item.get("arguments").cloned().unwrap_or(Value::Null),
                }),
                _ => Err(invalid(format!(
                    "messages[{index}].toolCalls entries need 'id' and 'name'"
                ))),
            }
        })
        .collect()
}

fn parse_messages(
    value: &Value,
    refs: &mut Vec<MediaReference>,
) -> Result<Vec<LlmMessage>, Diagnostic> {
    let Some(items) = value.as_array() else {
        return Err(invalid("'messages' must be a list"));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let Some(object) = item.as_object() else {
                return Err(invalid(format!("messages[{index}] must be an object")));
            };
            let role = opt_str(object, "role")
                .and_then(LlmRole::parse)
                .ok_or_else(|| {
                    invalid(format!(
                        "messages[{index}].role must be user, assistant or tool"
                    ))
                })?;
            let content = match object.get("content") {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.clone(),
                Some(_) => {
                    return Err(invalid(format!(
                        "messages[{index}].content must be a string"
                    )))
                }
            };
            let media = |key: &str| object.get(key).filter(|v| !v.is_null());
            if role != LlmRole::User
                && ["audio", "video", "image"]
                    .iter()
                    .any(|key| media(key).is_some())
            {
                return Err(invalid(format!(
                    "messages[{index}]: audio, video and images are accepted on user turns only"
                )));
            }
            let audio = media("audio")
                .map(|v| parse_audio(v, &format!("messages[{index}].audio"), index, refs))
                .transpose()?;
            let video = match (media("video"), media("image")) {
                (Some(_), Some(_)) => {
                    return Err(invalid(format!(
                        "messages[{index}]: give several images as video.frames, not 'image' \
                         beside 'video'"
                    )))
                }
                (Some(v), None) => Some(parse_video(
                    v,
                    &format!("messages[{index}].video"),
                    index,
                    refs,
                )?),
                // An image is read as a one-frame video.
                (None, Some(image)) => Some(LlmVideo::Frames(vec![parse_frame(
                    image,
                    &format!("messages[{index}].image"),
                    index,
                    0,
                    refs,
                )?])),
                (None, None) => None,
            };
            Ok(LlmMessage {
                role,
                content,
                tool_calls: parse_tool_calls(object.get("toolCalls"), index)?,
                tool_call_id: opt_str(object, "toolCallId").map(str::to_string),
                audio,
                video,
            })
        })
        .collect()
}

fn parse_tools(value: Option<&Value>) -> Result<Vec<LlmToolSpec>, Diagnostic> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(invalid("'tools' must be a list"));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
                .ok_or_else(|| invalid(format!("tools[{index}].name is required")))?;
            Ok(LlmToolSpec {
                name: name.to_string(),
                description: item
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                parameters: item
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"type": "object"})),
            })
        })
        .collect()
}

/// Parse a request body: a JSON object with `prompt` and/or `messages` (a `prompt` after
/// `messages` becomes the final user turn), optional `system`, `json`, `maxOutputTokens`,
/// `temperature` and `tools`. A user message may carry `audio` (`{data, format}`: base64 and a
/// media type), `video` (`{data, format}` for a clip, or `{frames: [{data, format, atMs}]}`) and
/// `image` (`{data, format}`, read as a one-frame video). Any of those parts may give
/// `{ref, sha256}` (an object-store URI and the stored object's SHA-256) instead of `data`; the
/// references are listed in [`CallRequest::media_refs`] and read before the call is routed;
/// `audioOutput` (a media type) asks for a spoken answer, `outputVoice` names its voice.
/// Diagnostics name the offending field, never its content.
pub fn parse_request_body(body: &[u8]) -> Result<CallRequest, Diagnostic> {
    let raw: Value =
        serde_json::from_slice(body).map_err(|_| invalid("the request body is not JSON"))?;
    let Some(object) = raw.as_object() else {
        return Err(invalid("the request body must be a JSON object"));
    };
    let mut media_refs = Vec::new();
    let mut messages = match object.get("messages") {
        None | Some(Value::Null) => Vec::new(),
        Some(value) => parse_messages(value, &mut media_refs)?,
    };
    match object.get("prompt") {
        None | Some(Value::Null) => {}
        Some(Value::String(prompt)) => messages.push(LlmMessage::user(prompt.clone())),
        Some(_) => return Err(invalid("'prompt' must be a string")),
    }
    if messages.is_empty() {
        return Err(invalid(
            "the request body needs a 'prompt' or a non-empty 'messages' list",
        ));
    }
    let system = match object.get("system") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(invalid("'system' must be a string")),
    };
    let json_output = match object.get("json") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(invalid("'json' must be a boolean")),
    };
    let max_output_tokens = match object.get("maxOutputTokens") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| invalid("'maxOutputTokens' must be a positive integer"))?,
        ),
    };
    let temperature = match object.get("temperature") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_f64()
                .ok_or_else(|| invalid("'temperature' must be a number"))? as f32,
        ),
    };
    let tools = parse_tools(object.get("tools"))?;
    let audio_output = match object.get("audioOutput") {
        None | Some(Value::Null) => None,
        Some(Value::String(format)) => Some(
            AudioFormat::parse(format)
                .map_err(|reason| invalid(format!("'audioOutput': {reason}")))?,
        ),
        Some(_) => return Err(invalid("'audioOutput' must be a media type string")),
    };
    let output_voice = match object.get("outputVoice") {
        None | Some(Value::Null) => None,
        Some(Value::String(voice)) if !voice.trim().is_empty() => Some(voice.trim().to_string()),
        Some(_) => return Err(invalid("'outputVoice' must be a non-empty string")),
    };
    let raw = echo_without_media(&raw);
    Ok(CallRequest {
        system,
        messages,
        tools,
        json_output,
        max_output_tokens,
        temperature,
        audio_output,
        output_voice,
        media_refs,
        raw,
    })
}
