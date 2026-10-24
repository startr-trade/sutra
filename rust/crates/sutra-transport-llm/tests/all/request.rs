use serde_json::json;
use sutra_llm_spi::{codes, LlmRole};
use sutra_transport_llm::{parse_destination, parse_request_body};

#[test]
fn destination_splits_into_tenant_module_key_and_channel() {
    assert_eq!(
        parse_destination("llm://acme/orders/1.0.0/ask"),
        Some(("acme".into(), "acme/orders/1.0.0".into(), "ask".into()))
    );
    assert_eq!(parse_destination("llm://ask"), None);
    assert_eq!(parse_destination("llm://acme/orders/1.0.0/"), None);
    assert_eq!(parse_destination("local://acme/orders/1.0.0/ask"), None);
}

#[test]
fn prompt_becomes_one_user_message() {
    let call =
        parse_request_body(br#"{"prompt":"hello","json":true,"maxOutputTokens":64}"#).unwrap();
    assert_eq!(call.messages.len(), 1);
    assert_eq!(call.messages[0].role, LlmRole::User);
    assert_eq!(call.messages[0].content, "hello");
    assert!(call.json_output);
    assert_eq!(call.max_output_tokens, Some(64));
    assert_eq!(call.raw["prompt"], "hello");
}

#[test]
fn messages_tools_and_system_parse() {
    let body = json!({
        "system": "you are terse",
        "temperature": 0.5,
        "messages": [
            {"role": "user", "content": "find order 7"},
            {"role": "assistant", "content": "",
             "toolCalls": [{"id": "c1", "name": "lookup", "arguments": {"id": 7}}]},
            {"role": "tool", "content": "{\"status\":\"open\"}", "toolCallId": "c1"}
        ],
        "tools": [{"name": "lookup", "description": "find", "parameters": {"type": "object"}}]
    });
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(call.system.as_deref(), Some("you are terse"));
    assert_eq!(call.temperature, Some(0.5));
    assert_eq!(call.messages.len(), 3);
    assert_eq!(call.messages[1].tool_calls[0].name, "lookup");
    assert_eq!(call.messages[2].role, LlmRole::Tool);
    assert_eq!(call.messages[2].tool_call_id.as_deref(), Some("c1"));
    assert_eq!(call.tools[0].name, "lookup");
}

#[test]
fn invalid_bodies_are_request_invalid() {
    for body in [
        &b"not json"[..],
        br#"[1,2]"#,
        br#"{"system":"only"}"#,
        br#"{"messages":[]}"#,
        br#"{"prompt":7}"#,
        br#"{"messages":[{"role":"robot","content":"x"}]}"#,
        br#"{"prompt":"x","tools":[{"description":"no name"}]}"#,
    ] {
        let err = parse_request_body(body).expect_err("invalid");
        assert_eq!(err.code, codes::REQUEST_INVALID);
    }
}
