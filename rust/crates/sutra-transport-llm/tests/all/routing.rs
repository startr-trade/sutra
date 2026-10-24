use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;
use sutra_channels::{MessageSink, SendOutcome};
use sutra_llm_spi::codes;
use sutra_transport_llm::MODEL_HEADER;

use crate::support::{channel_yaml, outbound, peak, sink, CapturePort};

async fn run(
    models: &[&str],
    extra: &str,
    body: serde_json::Value,
) -> (SendOutcome, Arc<CapturePort>) {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(models, extra), port.clone());
    let outcome = sink.send(&outbound(body)).await;
    (outcome, port)
}

fn answered_by(port: &CapturePort) -> String {
    port.delivered.lock().unwrap()[0].headers[MODEL_HEADER].clone()
}

#[tokio::test]
async fn fallback_skips_to_next_on_rate_limit() {
    let (outcome, port) = run(&["rate-limited", "ok"], "", json!({"prompt": "hi"})).await;
    assert_eq!(outcome, SendOutcome::Delivered);
    assert_eq!(answered_by(&port), "ok");
}

#[tokio::test]
async fn no_fallback_on_permanent() {
    let (outcome, port) = run(&["permanent", "ok"], "", json!({"prompt": "hi"})).await;
    let SendOutcome::PermanentFailure(d) = outcome else {
        panic!("expected permanent, got {outcome:?}");
    };
    assert_eq!(d.code, codes::CALL_FAILED);
    assert_eq!(d.attributes["model"], "permanent");
    assert_eq!(d.attributes["provider"], "test-fake");
    assert_eq!(d.attributes["channel"], "ask");
    assert!(port.delivered.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_fallback_on_a_class_outside_fallback_on() {
    let (outcome, _) = run(
        &["unavailable", "ok"],
        "fallback-on: [rate-limited]",
        json!({"prompt": "hi"}),
    )
    .await;
    let SendOutcome::RetryableFailure(d) = outcome else {
        panic!("expected retryable, got {outcome:?}");
    };
    assert_eq!(d.attributes["model"], "unavailable");
}

#[tokio::test]
async fn require_filters_candidates() {
    let (outcome, port) = run(
        &["no-tools", "ok"],
        "require: [tools]",
        json!({"prompt": "hi"}),
    )
    .await;
    assert_eq!(outcome, SendOutcome::Delivered);
    assert_eq!(answered_by(&port), "ok");
}

#[tokio::test]
async fn request_tools_filter_candidates_too() {
    let body = json!({"prompt": "hi", "tools": [{"name": "lookup"}]});
    let (outcome, port) = run(&["no-tools", "ok-tools"], "", body).await;
    assert_eq!(outcome, SendOutcome::Delivered);
    assert_eq!(answered_by(&port), "ok-tools");

    let body = json!({"prompt": "hi", "tools": [{"name": "lookup"}]});
    let (outcome, _) = run(&["no-tools"], "", body).await;
    assert!(
        matches!(outcome, SendOutcome::PermanentFailure(ref d) if d.code == codes::CALL_FAILED)
    );
}

#[tokio::test]
async fn all_retryable_exhausted_is_retryable_outcome() {
    let (outcome, _) = run(
        &["rate-limited", "unavailable", "timeout"],
        "",
        json!({"prompt": "hi"}),
    )
    .await;
    let SendOutcome::RetryableFailure(d) = outcome else {
        panic!("expected retryable, got {outcome:?}");
    };
    assert_eq!(d.code, codes::CALL_FAILED);
    assert_eq!(d.attributes["model"], "timeout");
    assert_eq!(d.attributes["class"], "timeout");
}

#[tokio::test(start_paused = true)]
async fn a_hung_candidate_is_cut_at_the_deadline_and_falls_back() {
    let (outcome, port) = run(
        &["hang", "ok"],
        "timeout-seconds: 2",
        json!({"prompt": "hi"}),
    )
    .await;
    assert_eq!(outcome, SendOutcome::Delivered);
    assert_eq!(answered_by(&port), "ok");
}

#[tokio::test]
async fn invalid_body_is_permanent_request_invalid() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["ok"], ""), port);
    let mut message = outbound(json!({}));
    message.body = b"not json".to_vec();
    let outcome = sink.send(&message).await;
    assert!(
        matches!(outcome, SendOutcome::PermanentFailure(ref d) if d.code == codes::REQUEST_INVALID)
    );
}

#[tokio::test]
async fn unknown_channel_is_retryable_and_bad_destination_permanent() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["ok"], ""), port);
    let mut message = outbound(json!({"prompt": "hi"}));
    message.destination = "llm://acme/orders/1.0.0/other".into();
    assert!(matches!(
        sink.send(&message).await,
        SendOutcome::RetryableFailure(_)
    ));
    message.destination = "llm://other".into();
    assert!(matches!(
        sink.send(&message).await,
        SendOutcome::PermanentFailure(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_enforced() {
    let port = Arc::new(CapturePort::default());
    let sink = Arc::new(sink(
        &channel_yaml(&["slow-conc"], "concurrency: 2"),
        port.clone(),
    ));
    let mut calls = Vec::new();
    for _ in 0..6 {
        let sink = sink.clone();
        calls.push(tokio::spawn(async move {
            sink.send(&outbound(json!({"prompt": "hi"}))).await
        }));
    }
    for call in calls {
        assert_eq!(call.await.unwrap(), SendOutcome::Delivered);
    }
    assert_eq!(port.delivered.lock().unwrap().len(), 6);
    let peak = peak("conc");
    assert_eq!(peak.max.load(Ordering::SeqCst), 2);
}
