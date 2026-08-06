//! The HTTP/HTTPS [`MessageSink`] — the outbound HTTP sink on the transport seam.
//!
//! Wire format (FROZEN — the m9 broker-contract wire invariants):
//! - the row's `outbox_key` goes on the wire as **`Idempotency-Key`** (consumer-side
//!   dedup for the crash-during-send duplicate, per replica-semantics),
//! - `Content-Type` from the message (default `application/octet-stream`),
//! - the **`X-HTTP-Method`** pseudo-header overrides the method (default `POST`) and is
//!   consumed, never forwarded,
//! - the persisted W3C **`traceparent`** rides the wire when present (trace-context bridge).
//!
//! Every send opens the `sutra.outbox.send` span ([`telemetry::SPAN_OUTBOX_SEND`]) with
//! the restored `traceparent` and `otel.kind = "producer"` recorded as span fields — the
//! frozen `SpanKind.PRODUCER` link semantics, modelled on the `tracing` facade for now
//! (the OTLP exporter maps the fields without touching this call site).
//!
//! Outcome mapping (the [`SendOutcome`] tri-state — 4xx contract rejects land in the poison
//! arm rather than being deferred):
//! - 2xx / 3xx → `Delivered` (opaque 3xx counts as success),
//! - 408 / 429 / 5xx / connect / timeout / transport errors → `RetryableFailure`,
//! - other 4xx / malformed destination → `PermanentFailure` (a retry can never fix it).

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tracing::Instrument;

use crate::codes;
use crate::diag::Diagnostic;
use crate::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use crate::telemetry;

const HEADER_METHOD_OVERRIDE: &str = "X-HTTP-Method";
const HEADER_IDEMPOTENCY_KEY: &str = "Idempotency-Key";
const HEADER_CONTENT_TYPE: &str = "Content-Type";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// One shared hyper client per sink instance — connection-pooling, `http` and `https`
/// schemes (rustls, native roots).
pub struct HttpSink {
    client: Client<HttpsConnector, Full<Bytes>>,
    request_timeout: std::time::Duration,
}

impl HttpSink {
    /// Sink-config defaults: connect 5 s, request 30 s.
    pub fn new() -> HttpSink {
        HttpSink::with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
        )
    }

    pub fn with_timeouts(
        connect_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
    ) -> HttpSink {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(connect_timeout));
        http.enforce_http(false); // the rustls wrapper decides per-scheme
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("native TLS root store")
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        HttpSink {
            client: Client::builder(TokioExecutor::new()).build(https),
            request_timeout,
        }
    }

    async fn send_once(&self, message: &OutboundMessage) -> SendOutcome {
        let destination = &message.destination;
        let uri: hyper::Uri = match destination.parse() {
            Ok(uri) => uri,
            Err(e) => {
                return SendOutcome::PermanentFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("destination '{destination}' is not a valid URI: {e}"),
                ));
            }
        };

        let method = method_for(message);
        let method: hyper::Method = match method.parse() {
            Ok(m) => m,
            Err(_) => {
                return SendOutcome::PermanentFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("X-HTTP-Method override '{method}' is not a valid HTTP method"),
                ));
            }
        };

        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(HEADER_CONTENT_TYPE, resolved_content_type(message))
            .header(HEADER_IDEMPOTENCY_KEY, &message.outbox_key);
        if let Some(traceparent) = &message.traceparent {
            if !traceparent.trim().is_empty() {
                builder = builder.header(telemetry::TRACEPARENT_HEADER, traceparent);
            }
        }
        for (name, value) in &message.headers {
            if name.eq_ignore_ascii_case(HEADER_METHOD_OVERRIDE)
                || name.eq_ignore_ascii_case(HEADER_CONTENT_TYPE)
                || name.eq_ignore_ascii_case(HEADER_IDEMPOTENCY_KEY)
                || name.eq_ignore_ascii_case(telemetry::TRACEPARENT_HEADER)
            {
                continue; // computed headers win — frozen wire contract
            }
            builder = builder.header(name, value);
        }
        let request = match builder.body(Full::new(Bytes::from(message.body.clone()))) {
            Ok(request) => request,
            Err(e) => {
                return SendOutcome::PermanentFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("HTTP request to {destination} could not be built: {e}"),
                ));
            }
        };

        let exchange = async {
            let response = self.client.request(request).await?;
            let status = response.status();
            // Drain the body so the pooled connection is reusable.
            let _ = response.into_body().collect().await;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(status)
        };
        let status = match tokio::time::timeout(self.request_timeout, exchange).await {
            Err(_elapsed) => {
                return SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!(
                        "HTTP send to {destination} timed out after {:?}",
                        self.request_timeout
                    ),
                ));
            }
            Ok(Err(e)) => {
                // Connect refused / reset / TLS / DNS — all transient from here.
                return SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("HTTP send to {destination} failed: {e}"),
                ));
            }
            Ok(Ok(status)) => status,
        };

        let code = status.as_u16();
        if (200..400).contains(&code) {
            return SendOutcome::Delivered;
        }
        let diagnostic = Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("HTTP send to {destination} failed with status {code}"),
        );
        if code == 408 || code == 429 || code >= 500 {
            SendOutcome::RetryableFailure(diagnostic)
        } else {
            SendOutcome::PermanentFailure(diagnostic)
        }
    }
}

impl Default for HttpSink {
    fn default() -> HttpSink {
        HttpSink::new()
    }
}

impl MessageSink for HttpSink {
    fn schemes(&self) -> Vec<String> {
        vec!["http".to_string(), "https".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        // The delivery span, linked to the enqueueing step via the persisted
        // traceparent: the value rides as a span field (attribute) AND as a first-class
        // OTel LINK (`link_span_to_traceparent`, PRODUCER link semantics — fail-open on
        // malformed values or when no OTel layer is installed).
        let span = tracing::info_span!(
            telemetry::SPAN_OUTBOX_SEND,
            otel.kind = "producer",
            traceparent = tracing::field::Empty,
        );
        if let Some(traceparent) = &message.traceparent {
            span.record("traceparent", tracing::field::display(traceparent));
            telemetry::link_span_to_traceparent(&span, traceparent);
        }
        Box::pin(self.send_once(message).instrument(span))
    }
}

/// The `X-HTTP-Method` override (case-insensitive header + value uppercased), default POST.
fn method_for(message: &OutboundMessage) -> String {
    for (name, value) in &message.headers {
        if name.eq_ignore_ascii_case(HEADER_METHOD_OVERRIDE) && !value.trim().is_empty() {
            return value.trim().to_ascii_uppercase();
        }
    }
    "POST".to_string()
}

fn resolved_content_type(message: &OutboundMessage) -> String {
    match &message.content_type {
        Some(ct) if !ct.trim().is_empty() => ct.clone(),
        _ => message
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(HEADER_CONTENT_TYPE))
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes as AxumBytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, StatusCode};
    use axum::routing::any;
    use axum::Router;

    use super::*;

    type CapturedRequest = (String, BTreeMap<String, String>, Vec<u8>);

    #[derive(Clone, Default)]
    struct Captured {
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        status: u16,
    }

    async fn capture(
        State(state): State<Captured>,
        method: Method,
        headers: HeaderMap,
        body: AxumBytes,
    ) -> StatusCode {
        let mut map = BTreeMap::new();
        for (name, value) in &headers {
            if let Ok(v) = value.to_str() {
                map.insert(name.as_str().to_string(), v.to_string());
            }
        }
        state
            .requests
            .lock()
            .unwrap()
            .push((method.to_string(), map, body.to_vec()));
        StatusCode::from_u16(state.status).unwrap()
    }

    async fn serve(status: u16) -> (SocketAddr, Captured) {
        let captured = Captured {
            requests: Arc::new(Mutex::new(Vec::new())),
            status,
        };
        let app = Router::new()
            .route("/cb", any(capture))
            .with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, captured)
    }

    fn message(destination: String) -> OutboundMessage {
        OutboundMessage {
            destination,
            headers: BTreeMap::new(),
            body: b"<ack/>".to_vec(),
            content_type: Some("application/xml".to_string()),
            outbox_key: "ob-key-1".to_string(),
            traceparent: None,
        }
    }

    #[tokio::test]
    async fn preserves_payload_bytes_and_propagates_outbox_key() {
        // The m9 HTTP wire contract: body bytes verbatim, outbox_key as Idempotency-Key,
        // Content-Type pinned.
        let (addr, captured) = serve(202).await;
        let sink = HttpSink::new();
        let mut m = message(format!("http://{addr}/cb"));
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());

        assert_eq!(sink.send(&m).await, SendOutcome::Delivered);

        let requests = captured.requests.lock().unwrap();
        let (method, headers, body) = &requests[0];
        assert_eq!(method, "POST");
        assert_eq!(body, b"<ack/>");
        assert_eq!(headers.get("idempotency-key").unwrap(), "ob-key-1");
        assert_eq!(headers.get("content-type").unwrap(), "application/xml");
        assert_eq!(
            headers.get("traceparent").unwrap(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
    }

    #[tokio::test]
    async fn method_override_header_is_consumed_not_forwarded() {
        let (addr, captured) = serve(200).await;
        let sink = HttpSink::new();
        let mut m = message(format!("http://{addr}/cb"));
        m.headers
            .insert("x-http-method".to_string(), "put".to_string());
        m.headers.insert("X-Custom".to_string(), "kept".to_string());

        assert_eq!(sink.send(&m).await, SendOutcome::Delivered);

        let requests = captured.requests.lock().unwrap();
        let (method, headers, _) = &requests[0];
        assert_eq!(method, "PUT");
        assert!(!headers.contains_key("x-http-method"));
        assert_eq!(headers.get("x-custom").unwrap(), "kept");
    }

    #[tokio::test]
    async fn default_content_type_is_octet_stream() {
        let (addr, captured) = serve(200).await;
        let sink = HttpSink::new();
        let mut m = message(format!("http://{addr}/cb"));
        m.content_type = None;

        sink.send(&m).await;

        let requests = captured.requests.lock().unwrap();
        assert_eq!(
            requests[0].1.get("content-type").unwrap(),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn five_hundreds_are_retryable() {
        let (addr, _) = serve(503).await;
        let sink = HttpSink::new();
        let outcome = sink.send(&message(format!("http://{addr}/cb"))).await;
        let SendOutcome::RetryableFailure(d) = outcome else {
            panic!("503 must be retryable, got {outcome:?}");
        };
        assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED);
        assert!(d.message.contains("503"));
    }

    #[tokio::test]
    async fn contract_rejects_are_permanent_and_backpressure_is_retryable() {
        let (addr, _) = serve(400).await;
        let sink = HttpSink::new();
        assert!(matches!(
            sink.send(&message(format!("http://{addr}/cb"))).await,
            SendOutcome::PermanentFailure(_)
        ));

        let (addr, _) = serve(429).await;
        assert!(matches!(
            sink.send(&message(format!("http://{addr}/cb"))).await,
            SendOutcome::RetryableFailure(_)
        ));
    }

    #[tokio::test]
    async fn connect_refused_is_retryable() {
        // Bind then drop a listener so the port is closed.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let sink = HttpSink::new();
        assert!(matches!(
            sink.send(&message(format!("http://{addr}/cb"))).await,
            SendOutcome::RetryableFailure(_)
        ));
    }

    #[tokio::test]
    async fn malformed_destination_is_permanent() {
        let sink = HttpSink::new();
        assert!(matches!(
            sink.send(&message("http://".to_string())).await,
            SendOutcome::PermanentFailure(_)
        ));
    }
}
