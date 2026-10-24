use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::json;
use sutra_channels::{MessageSink, SendOutcome};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::support::{channel_yaml, outbound, sink, CapturePort, ANSWER_TEXT};

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

#[tokio::test]
async fn spans_and_diagnostics_carry_no_prompt_text() {
    // Global, not thread-scoped: a scoped subscriber races the callsite-interest cache with
    // the suite's other threads and can see the span disabled.
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    let capture = CAPTURE.get_or_init(|| {
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
    });

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

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
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
