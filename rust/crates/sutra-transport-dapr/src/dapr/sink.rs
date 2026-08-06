//! The Dapr outbound sink — [`DaprMessageSink`] implements
//! [`sutra_channels::sink::MessageSink`] for the `dapr://<pubsub>/<topic>` destination
//! scheme.
//!
//! The sink is a thin URL-rewrite in front of [`sutra_channels::HttpSink`]: the
//! CloudEvents-binary `ce-*` header projection / structured-envelope rendering already
//! happens dispatcher-side (`sutra_channels::outbox_dispatch::encode_wire_message`, the SAME
//! machinery every other Rust transport's outbound reply rides — `dapr://` destinations fall
//! into that function's default `CeBinding::Http` arm). This sink only needs to:
//! rewrite `dapr://<pubsub>/<topic>` to `http://localhost:<sidecar-port>/v1.0/publish/<pubsub>/<topic>`
//! and delegate the POST — reusing `HttpSink`'s rustls client, `Idempotency-Key` lift,
//! Content-Type resolution, and retryable/permanent status-code mapping verbatim (so no
//! second hyper/rustls client is built in this crate).

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::HttpSink;

use super::codes;

/// Engine-wide Dapr sidecar HTTP port ([`TransportFactory::register_sink`][spi] reads this
/// once, at registration; a Dapr sidecar is one-per-process, so the port is NOT a per-channel
/// property).
///
/// [spi]: sutra_transport_spi::TransportFactory::register_sink
pub const SINK_SIDECAR_PORT_ENV: &str = "SUTRA_SINK_DAPR_SIDECAR_PORT";

/// The outbound Dapr transport: rewrites a `dapr://<pubsub>/<topic>` destination to the
/// local sidecar's publish URL and delegates delivery to an inner [`HttpSink`].
pub struct DaprMessageSink {
    sidecar_port: u16,
    inner: HttpSink,
}

impl DaprMessageSink {
    /// A sink dialing the sidecar at `http://localhost:<sidecar_port>` (Dapr's conventional
    /// HTTP port is 3500 — see [`super::DaprChannelProperties::DEFAULT_SIDECAR_PORT`]).
    pub fn new(sidecar_port: u16) -> DaprMessageSink {
        DaprMessageSink {
            sidecar_port,
            inner: HttpSink::new(),
        }
    }
}

impl Default for DaprMessageSink {
    fn default() -> DaprMessageSink {
        DaprMessageSink::new(super::DaprChannelProperties::DEFAULT_SIDECAR_PORT)
    }
}

impl MessageSink for DaprMessageSink {
    fn schemes(&self) -> Vec<String> {
        vec!["dapr".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            let (pubsub, topic) = match parse_destination(&message.destination) {
                Ok(v) => v,
                Err(outcome) => return outcome,
            };
            let mut rewritten = message.clone();
            rewritten.destination = format!(
                "http://localhost:{}/v1.0/publish/{}/{}",
                self.sidecar_port, pubsub, topic
            );
            retag(
                self.inner.send(&rewritten).await,
                codes::OUTBOUND_PUBLISH_FAILED,
            )
        })
    }
}

/// Parse `dapr://<pubsub>/<topic>` — host is the pub/sub component name, path (everything
/// after the first `/`, verbatim — a topic MAY itself contain further `/`s) is the topic.
fn parse_destination(destination: &str) -> Result<(String, String), SendOutcome> {
    if sutra_channels::sink::scheme_of(destination)
        .map(str::to_ascii_lowercase)
        .as_deref()
        != Some("dapr")
    {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_INVALID_DESTINATION,
            format!("'{destination}' is not a 'dapr://<pubsub>/<topic>' URI"),
        )));
    }
    let rest = destination.strip_prefix("dapr://").unwrap_or(destination);
    let (pubsub, topic) = rest.split_once('/').unwrap_or((rest, ""));
    if pubsub.trim().is_empty() {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_NO_PUBSUB,
            format!("dapr destination '{destination}' is missing the pubsub name (URI host)"),
        )));
    }
    if topic.trim().is_empty() {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_NO_TOPIC,
            format!("dapr destination '{destination}' is missing the topic (URI path)"),
        )));
    }
    Ok((pubsub.to_string(), topic.to_string()))
}

/// Re-tag an inner [`HttpSink`] outcome's diagnostic code with the Dapr-specific publish-
/// failure code while preserving the tri-state + message.
fn retag(outcome: SendOutcome, code: &'static str) -> SendOutcome {
    match outcome {
        SendOutcome::Delivered => SendOutcome::Delivered,
        SendOutcome::RetryableFailure(d) => SendOutcome::RetryableFailure(Diagnostic {
            code: code.to_string(),
            ..d
        }),
        SendOutcome::PermanentFailure(d) => SendOutcome::PermanentFailure(Diagnostic {
            code: code.to_string(),
            ..d
        }),
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

    /// A local mock Dapr sidecar bound on loopback — the sink's hardcoded `localhost` host
    /// resolves to it because it is bound on `127.0.0.1`.
    async fn serve_sidecar(status: u16) -> (SocketAddr, Captured) {
        let captured = Captured {
            requests: Arc::new(Mutex::new(Vec::new())),
            status,
        };
        let app = Router::new()
            .route("/v1.0/publish/{pubsub}/{topic}", any(capture))
            .with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, captured)
    }

    fn message(destination: &str) -> OutboundMessage {
        let mut headers = BTreeMap::new();
        headers.insert("ce-id".to_string(), "evt-1".to_string());
        headers.insert("ce-source".to_string(), "/orders/gw".to_string());
        headers.insert("ce-type".to_string(), "orders.created".to_string());
        OutboundMessage {
            destination: destination.to_string(),
            headers,
            body: b"{\"amount\":10}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "outbox-key-1".to_string(),
            traceparent: None,
        }
    }

    #[test]
    fn sink_claims_only_the_dapr_scheme() {
        assert_eq!(DaprMessageSink::new(3500).schemes(), vec!["dapr"]);
    }

    #[test]
    fn parses_pubsub_and_topic_from_the_destination() {
        assert_eq!(
            parse_destination("dapr://messagebus/orders.created").unwrap(),
            ("messagebus".to_string(), "orders.created".to_string())
        );
        // A topic MAY itself contain further slashes — the whole remaining path is the
        // topic, not just its first segment.
        assert_eq!(
            parse_destination("dapr://messagebus/orders/created").unwrap(),
            ("messagebus".to_string(), "orders/created".to_string())
        );
    }

    #[tokio::test]
    async fn round_trips_to_the_rewritten_sidecar_publish_url_with_headers_and_idempotency_key() {
        let (addr, captured) = serve_sidecar(200).await;
        let sink = DaprMessageSink::new(addr.port());

        let outcome = sink
            .send(&message("dapr://messagebus/orders.created"))
            .await;
        assert_eq!(outcome, SendOutcome::Delivered);

        let requests = captured.requests.lock().unwrap();
        let (method, headers, body) = &requests[0];
        assert_eq!(method, "POST");
        assert_eq!(body, b"{\"amount\":10}");
        assert_eq!(headers.get("idempotency-key").unwrap(), "outbox-key-1");
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        // The ce-* headers rode verbatim (already projected upstream by
        // `encode_wire_message`) — this sink adds no CE logic of its own.
        assert_eq!(headers.get("ce-id").unwrap(), "evt-1");
        assert_eq!(headers.get("ce-source").unwrap(), "/orders/gw");
    }

    #[tokio::test]
    async fn wrong_scheme_is_a_permanent_invalid_destination_failure() {
        let sink = DaprMessageSink::new(3500);
        for bad in ["http://host/x", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(d) => {
                    assert_eq!(d.code, codes::OUTBOUND_INVALID_DESTINATION, "for '{bad}'")
                }
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn missing_pubsub_is_a_permanent_no_pubsub_failure() {
        let sink = DaprMessageSink::new(3500);
        match sink.send(&message("dapr://")).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_NO_PUBSUB),
            other => panic!("expected PermanentFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_topic_is_a_permanent_no_topic_failure() {
        let sink = DaprMessageSink::new(3500);
        match sink.send(&message("dapr://messagebus")).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_NO_TOPIC),
            other => panic!("expected PermanentFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unreachable_sidecar_is_retryable_and_retagged() {
        // Bind then drop a listener so the port is closed — connect refused.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let sink = DaprMessageSink::new(addr.port());
        match sink
            .send(&message("dapr://messagebus/orders.created"))
            .await
        {
            SendOutcome::RetryableFailure(d) => assert_eq!(d.code, codes::OUTBOUND_PUBLISH_FAILED),
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }
}
