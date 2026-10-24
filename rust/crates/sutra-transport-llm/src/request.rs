//! The two wire shapes this transport reads: the qualified `llm://<module_key>/<channel>`
//! destination and the JSON request body a channel call carries.

use serde_json::Value;
use sutra_channels::Diagnostic;
use sutra_llm_spi::{codes, LlmMessage, LlmRole, LlmToolCall, LlmToolSpec};

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
    pub raw: Value,
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

fn parse_messages(value: &Value) -> Result<Vec<LlmMessage>, Diagnostic> {
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
            Ok(LlmMessage {
                role,
                content,
                tool_calls: parse_tool_calls(object.get("toolCalls"), index)?,
                tool_call_id: opt_str(object, "toolCallId").map(str::to_string),
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
/// `temperature` and `tools`. Diagnostics name the offending field, never its content.
pub fn parse_request_body(body: &[u8]) -> Result<CallRequest, Diagnostic> {
    let raw: Value =
        serde_json::from_slice(body).map_err(|_| invalid("the request body is not JSON"))?;
    let Some(object) = raw.as_object() else {
        return Err(invalid("the request body must be a JSON object"));
    };
    let mut messages = match object.get("messages") {
        None | Some(Value::Null) => Vec::new(),
        Some(value) => parse_messages(value)?,
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
    Ok(CallRequest {
        system,
        messages,
        tools,
        json_output,
        max_output_tokens,
        temperature,
        raw,
    })
}
