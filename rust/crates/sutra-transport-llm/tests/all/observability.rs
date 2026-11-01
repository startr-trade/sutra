use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::json;
use sutra_channels::{InlineCaller, InlineFailure, MessageSink, SendOutcome};
use sutra_transport_llm::LlmInlineCaller;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::support::{channel_yaml, outbound, sink, table, CapturePort, ANSWER_TEXT};

const PROMPT: &str = "SECRET-PROMPT-TEXT";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The suite's one global log capture. Global, not thread-scoped: a scoped subscriber races
/// the callsite-interest cache with the suite's other threads and can see the span disabled.
fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("one global subscriber");
        capture
    })
}

fn logs() -> String {
    String::from_utf8(capture().0.lock().unwrap().clone()).unwrap()
}

#[tokio::test]
async fn spans_and_diagnostics_carry_no_prompt_text() {
    capture();
    let body = json!({"system": PROMPT, "prompt": PROMPT});
    // One answered call (fallback warn + span close) and one failed call (diagnostic).
    let port = Arc::new(CapturePort::default());
    let answered = sink(&channel_yaml(&["rate-limited", "ok"], ""), port.clone());
    assert_eq!(
        answered.send(&outbound(body.clone())).await,
        SendOutcome::Delivered
    );
    let failing = sink(&channel_yaml(&["permanent"], ""), port);
    let SendOutcome::PermanentFailure(diagnostic) = failing.send(&outbound(body)).await else {
        panic!("expected permanent");
    };

    let logs = logs();
    assert!(logs.contains("sutra.llm.call"), "{logs}");
    assert!(logs.contains("input_tokens=11"), "{logs}");
    assert!(
        logs.contains("finish=\"stop\"") || logs.contains("finish=stop"),
        "{logs}"
    );
    for secret in [PROMPT, ANSWER_TEXT] {
        assert!(!logs.contains(secret), "logs leak content: {logs}");
        assert!(!diagnostic.message.contains(secret));
        assert!(diagnostic.attributes.values().all(|v| !v.contains(secret)));
    }
}

/// The lines of the capture that mention `marker`.
fn lines_with(logs: &str, marker: &str) -> Vec<String> {
    logs.lines()
        .filter(|l| l.contains(marker))
        .map(str::to_string)
        .collect()
}

/// A sync (inline) call that fails logs, at WARN, the provider's reason for each candidate and
/// for the call — status, provider code, message — under the code the engine's instance-failed
/// line names; the stored diagnostic carries the same reason. No prompt and no key appears.
#[tokio::test]
async fn failed_inline_call_logs_its_reason_without_secrets() {
    capture();
    // `http-401-echo` echoes the candidate's key (`env:LLM_KEY` resolves to `resolved:LLM_KEY`)
    // and falls through to nothing: a 401 is permanent. The channel is named for this test.
    let yaml = channel_yaml(&["http-400"], "")
        .replace("name: ask", "name: reason-inline")
        .replace("llm://ask", "llm://reason-inline");
    let caller = LlmInlineCaller::new(table(&yaml));
    let mut message = outbound(json!({"system": PROMPT, "prompt": PROMPT, "json": true}));
    message.destination = message.destination.replace("/ask", "/reason-inline");
    message.headers.insert(
        sutra_channels::AUDIT_JOURNAL_INSTANCE_HEADER.to_string(),
        "inst-reason-77".to_string(),
    );
    let Err(InlineFailure::Permanent(diagnostic)) = caller.call(&message).await else {
        panic!("expected a permanent failure");
    };
    assert_eq!(diagnostic.code, "SUTRA.LLM.CALL.FAILED");
    assert!(
        diagnostic.message.contains(
            "HTTP 400 (invalid_request_error): Invalid parameter: 'response_format' of type \
             'json_object' is not supported with this model."
        ),
        "{}",
        diagnostic.message
    );
    assert_eq!(diagnostic.attributes["http_status"], "400");
    assert_eq!(
        diagnostic.attributes["provider_code"],
        "invalid_request_error"
    );

    let logs = logs();
    let lines = lines_with(&logs, "reason-inline");
    let candidate = lines
        .iter()
        .find(|l| l.contains("llm candidate failed"))
        .unwrap_or_else(|| panic!("no candidate WARN: {logs}"));
    assert!(candidate.contains("WARN"), "{candidate}");
    assert!(candidate.contains("http_status=400"), "{candidate}");
    assert!(
        candidate.contains("provider_code=invalid_request_error"),
        "{candidate}"
    );
    assert!(candidate.contains("model=http-400"), "{candidate}");
    let call = lines
        .iter()
        .find(|l| l.contains("llm call failed"))
        .unwrap_or_else(|| panic!("no call WARN: {logs}"));
    assert!(call.contains("WARN"), "{call}");
    assert!(call.contains("code=SUTRA.LLM.CALL.FAILED"), "{call}");
    assert!(call.contains("instance=inst-reason-77"), "{call}");
    assert!(
        call.contains("trace_id=0af7651916cd43dd8448eb211c80319c"),
        "{call}"
    );
    assert!(call.contains("is not supported with this model"), "{call}");
    for line in &lines {
        assert!(!line.contains(PROMPT), "a prompt reached the log: {line}");
    }
}

/// The async (outbox) path reports the same way, and a key the provider echoes is redacted in
/// the log line and the diagnostic alike.
#[tokio::test]
async fn failed_async_call_logs_its_reason_and_redacts_an_echoed_key() {
    capture();
    let yaml = channel_yaml(&["http-401-echo"], "    api-key: env:LLM_KEY")
        .replace("name: ask", "name: reason-async")
        .replace("llm://ask", "llm://reason-async");
    let failing = sink(&yaml, Arc::new(CapturePort::default()));
    let mut message = outbound(json!({"system": PROMPT, "prompt": PROMPT}));
    message.destination = message.destination.replace("/ask", "/reason-async");
    let SendOutcome::PermanentFailure(diagnostic) = failing.send(&message).await else {
        panic!("expected a permanent failure");
    };
    assert!(
        diagnostic
            .message
            .contains("HTTP 401 (invalid_api_key): Incorrect API key provided: <redacted>"),
        "{}",
        diagnostic.message
    );

    let logs = logs();
    let lines = lines_with(&logs, "reason-async");
    assert!(
        lines
            .iter()
            .any(|l| l.contains("llm call failed") && l.contains("http_status=401")),
        "{logs}"
    );
    for line in &lines {
        assert!(
            !line.contains("resolved:LLM_KEY"),
            "a key reached the log: {line}"
        );
        assert!(!line.contains(PROMPT), "a prompt reached the log: {line}");
    }
    assert!(!diagnostic.message.contains("resolved:LLM_KEY"));
}
