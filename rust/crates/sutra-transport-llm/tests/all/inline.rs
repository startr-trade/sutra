use serde_json::json;
use std::sync::{Arc, Mutex};

use sutra_channels::{InlineCaller, InlineChunk, InlineFailure};
use sutra_transport_llm::{parse_channel, LlmInlineCaller, MODEL_HEADER, PROVIDER_HEADER};

use crate::support::{channel_yaml, definitions, outbound, resolver, table, ANSWER_TEXT, FAKE};

fn caller(models: &[&str]) -> LlmInlineCaller {
    LlmInlineCaller::new(table(&channel_yaml(models, "")))
}

#[tokio::test]
async fn inline_success_returns_body_and_headers() {
    let response = caller(&["ok"])
        .call(&outbound(json!({"prompt": "hi"})))
        .await
        .expect("answered");
    assert_eq!(response.content_type.as_deref(), Some("application/json"));
    assert_eq!(response.headers[PROVIDER_HEADER], FAKE);
    assert_eq!(response.headers[MODEL_HEADER], "ok");
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["text"], ANSWER_TEXT);
    assert_eq!(body["model"], "ok");
}

#[tokio::test]
async fn retryable_and_permanent_failures_map() {
    let c = caller(&["unavailable"]);
    let err = c
        .call(&outbound(json!({"prompt": "hi"})))
        .await
        .unwrap_err();
    assert!(matches!(err, InlineFailure::Retryable(_)), "{err:?}");

    let c = caller(&["permanent"]);
    let err = c
        .call(&outbound(json!({"prompt": "hi"})))
        .await
        .unwrap_err();
    assert!(matches!(err, InlineFailure::Permanent(_)), "{err:?}");

    // An invalid request body is permanent.
    let err = caller(&["ok"])
        .call(&outbound(json!("nope")))
        .await
        .unwrap_err();
    assert!(matches!(err, InlineFailure::Permanent(_)), "{err:?}");
}

#[tokio::test]
async fn unknown_channel_is_retryable() {
    let mut message = outbound(json!({"prompt": "hi"}));
    message.destination = message.destination.replace("/ask", "/missing");
    let err = caller(&["ok"]).call(&message).await.unwrap_err();
    assert!(matches!(err, InlineFailure::Retryable(_)), "{err:?}");
}

const NO_REPLY: &str =
    "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                        bind: \"llm://ask\"\n";
const CANDIDATES: &str =
    "    llm:\n      candidates:\n        - provider: test-fake\n          model: ok\n";

#[test]
fn sync_channel_without_reply_channel_parses() {
    let yaml = format!("{NO_REPLY}    ack-mode: on-complete\n{CANDIDATES}");
    let defs = definitions(&yaml);
    let channel = parse_channel(&defs[0], resolver).unwrap().unwrap();
    assert_eq!(channel.reply_channel, "");
}

#[test]
fn async_channel_without_reply_channel_still_errors() {
    let yaml = format!("{NO_REPLY}{CANDIDATES}");
    let defs = definitions(&yaml);
    let err = parse_channel(&defs[0], resolver).err().expect("refused");
    assert!(err.message.contains("reply-channel"), "{}", err.message);
}

#[tokio::test]
async fn a_streamed_fallback_resets_before_the_next_candidates_answer() {
    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&events);
    let response = caller(&["rate-limited-after-part", "ok"])
        .call_streaming(
            &outbound(json!({"prompt": "hi"})),
            Arc::new(move |chunk| {
                seen.lock().unwrap().push(match chunk {
                    InlineChunk::Text(text) => text.to_string(),
                    InlineChunk::Reset => "<reset>".to_string(),
                })
            }),
        )
        .await
        .expect("the second candidate answers");
    assert_eq!(response.headers[MODEL_HEADER], "ok");
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "partial-".to_string(),
            "<reset>".to_string(),
            ANSWER_TEXT.to_string()
        ],
        "the partial answer is voided before the fallback's first piece"
    );
}

#[tokio::test]
async fn a_streamed_call_without_fallback_never_resets() {
    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&events);
    caller(&["ok"])
        .call_streaming(
            &outbound(json!({"prompt": "hi"})),
            Arc::new(move |chunk| {
                if let InlineChunk::Text(text) = chunk {
                    seen.lock().unwrap().push(text.to_string());
                } else {
                    seen.lock().unwrap().push("<reset>".to_string());
                }
            }),
        )
        .await
        .expect("answered");
    assert_eq!(*events.lock().unwrap(), vec![ANSWER_TEXT.to_string()]);
}
