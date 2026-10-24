use std::sync::Arc;

use serde_json::{json, Value};
use sutra_channels::{Diagnostic, MessageSink, SendOutcome};
use sutra_transport_llm::{MODEL_HEADER, PROVIDER_HEADER};

use crate::support::{channel_yaml, outbound, sink, CapturePort, ANSWER_TEXT, MODULE_KEY, TENANT};

#[tokio::test]
async fn reply_message_shape() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["ok"], ""), port.clone());
    let request = json!({"prompt": "hi", "orderId": "42"});
    let message = outbound(request.clone());
    assert_eq!(sink.send(&message).await, SendOutcome::Delivered);

    let delivered = port.delivered.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    let reply = &delivered[0];
    assert_eq!(reply.tenant, TENANT);
    assert_eq!(reply.module_key, MODULE_KEY);
    assert_eq!(reply.channel, "answers");
    assert_eq!(reply.headers["x-order-id"], "42");
    assert_eq!(reply.headers[PROVIDER_HEADER], "test-fake");
    assert_eq!(reply.headers[MODEL_HEADER], "ok");
    assert_eq!(
        reply.headers["traceparent"],
        message.traceparent.clone().unwrap()
    );
    assert_eq!(reply.content_type.as_deref(), Some("application/json"));
    assert_eq!(reply.idempotency_key, "ob-1");
    assert!(reply.explicit_event_id);
    assert!(reply.cloud_event.is_none());
    assert!(!reply.received_at.is_empty());

    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(body["text"], ANSWER_TEXT);
    assert_eq!(body["finish"], "stop");
    assert_eq!(body["toolCalls"][0]["name"], "lookup");
    assert_eq!(body["toolCalls"][0]["arguments"]["id"], 7);
    assert_eq!(body["usage"]["inputTokens"], 11);
    assert_eq!(body["usage"]["outputTokens"], 22);
    assert_eq!(body["provider"], "test-fake");
    assert_eq!(body["model"], "ok");
    assert_eq!(body["providerRequestId"], "req-ok");
    assert_eq!(body["request"], request);
    assert_eq!(body["request"]["orderId"], "42");
}

async fn deliver_failing(diagnostic: Diagnostic) -> SendOutcome {
    let port = Arc::new(CapturePort {
        fail_with: Some(diagnostic),
        ..CapturePort::default()
    });
    let sink = sink(&channel_yaml(&["ok"], ""), port);
    sink.send(&outbound(json!({"prompt": "hi"}))).await
}

#[tokio::test]
async fn engine_unavailable_reply_is_retryable() {
    let outcome = deliver_failing(Diagnostic::error(
        sutra_channels::codes::RUNTIME_UNEXPECTED,
        "engine actor is not running",
    ))
    .await;
    assert!(
        matches!(outcome, SendOutcome::RetryableFailure(_)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn rejected_reply_is_permanent() {
    let outcome = deliver_failing(Diagnostic::error("SUTRA.TEST.REJECT", "no such channel")).await;
    assert!(
        matches!(outcome, SendOutcome::PermanentFailure(_)),
        "{outcome:?}"
    );
}
