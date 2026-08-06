//! The Knative outbound sink — [`KnativeMessageSink`] implements
//! [`sutra_channels::sink::MessageSink`] for the `knative://<namespace>/<broker>`
//! destination scheme.
//!
//! Like the Dapr sink, this one is a thin URL-rewrite in front of
//! [`sutra_channels::HttpSink`]: the CloudEvents-binary `ce-*` header projection already
//! happens dispatcher-side (`sutra_channels::outbox_dispatch::encode_wire_message`, whose
//! default `CeBinding::Http` arm covers `knative://` destinations) — this sink only
//! rewrites `knative://<namespace>/<broker>` to the Broker ingress URL and delegates the
//! POST, reusing `HttpSink`'s rustls client, `Idempotency-Key` lift, and retryable/permanent
//! status-code mapping verbatim.

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::HttpSink;

use super::codes;

/// Engine-wide Broker ingress base URL override
/// ([`TransportFactory::register_sink`][spi] reads this). Precedence (highest first):
/// this env var → the Knative-idiomatic `K_SINK` env var (the addressable sink URL Knative
/// injects into a Service acting as an event source) → [`super::DEFAULT_BROKER_INGRESS`].
/// Like Dapr's sidecar port, this is engine-wide config resolved once at registration — the
/// per-channel `broker.url` property is never consulted.
///
/// [spi]: sutra_transport_spi::TransportFactory::register_sink
pub const SINK_BROKER_INGRESS_ENV: &str = "SUTRA_SINK_KNATIVE_BROKER_INGRESS";
/// The Knative-standard sink-binding env var (set by Knative on a Service/sink binding).
pub const K_SINK_ENV: &str = "K_SINK";

/// The outbound Knative transport: rewrites a `knative://<namespace>/<broker>` destination
/// to the Broker ingress URL and delegates delivery to an inner [`HttpSink`].
pub struct KnativeMessageSink {
    broker_ingress_base: String,
    inner: HttpSink,
}

impl KnativeMessageSink {
    /// A sink targeting `broker_ingress_base` (trailing slash stripped; a blank base falls
    /// back to [`super::DEFAULT_BROKER_INGRESS`]).
    pub fn new(broker_ingress_base: impl Into<String>) -> KnativeMessageSink {
        let base = broker_ingress_base.into();
        let base = if base.trim().is_empty() {
            super::DEFAULT_BROKER_INGRESS.to_string()
        } else {
            base.trim_end_matches('/').to_string()
        };
        KnativeMessageSink {
            broker_ingress_base: base,
            inner: HttpSink::new(),
        }
    }
}

impl Default for KnativeMessageSink {
    fn default() -> KnativeMessageSink {
        KnativeMessageSink::new(super::DEFAULT_BROKER_INGRESS)
    }
}

impl MessageSink for KnativeMessageSink {
    fn schemes(&self) -> Vec<String> {
        vec!["knative".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            let (namespace, broker) = match parse_destination(&message.destination) {
                Ok(v) => v,
                Err(outcome) => return outcome,
            };
            let mut rewritten = message.clone();
            rewritten.destination =
                format!("{}/{}/{}", self.broker_ingress_base, namespace, broker);
            retag(
                self.inner.send(&rewritten).await,
                codes::OUTBOUND_PUBLISH_FAILED,
            )
        })
    }
}

/// Parse `knative://<namespace>/<broker>` — host is the Kubernetes namespace owning the
/// Broker, path (everything after the first `/`, verbatim) is the Broker name.
fn parse_destination(destination: &str) -> Result<(String, String), SendOutcome> {
    if sutra_channels::sink::scheme_of(destination)
        .map(str::to_ascii_lowercase)
        .as_deref()
        != Some("knative")
    {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_INVALID_DESTINATION,
            format!("'{destination}' is not a 'knative://<namespace>/<broker>' URI"),
        )));
    }
    let rest = destination
        .strip_prefix("knative://")
        .unwrap_or(destination);
    let (namespace, broker) = rest.split_once('/').unwrap_or((rest, ""));
    if namespace.trim().is_empty() {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_NO_NAMESPACE,
            format!("knative destination '{destination}' is missing the namespace (URI host)"),
        )));
    }
    if broker.trim().is_empty() {
        return Err(SendOutcome::PermanentFailure(Diagnostic::error(
            codes::OUTBOUND_NO_BROKER,
            format!("knative destination '{destination}' is missing the broker (URI path)"),
        )));
    }
    Ok((namespace.to_string(), broker.to_string()))
}

/// Re-tag an inner [`HttpSink`] outcome's diagnostic code with the Knative-specific publish-
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

    /// A local mock Broker ingress bound on loopback.
    async fn serve_broker(status: u16) -> (SocketAddr, Captured) {
        let captured = Captured {
            requests: Arc::new(Mutex::new(Vec::new())),
            status,
        };
        let app = Router::new()
            .route("/{namespace}/{broker}", any(capture))
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
        headers.insert("ce-id".to_string(), "evt-2".to_string());
        headers.insert("ce-source".to_string(), "/orders/gw".to_string());
        headers.insert("ce-type".to_string(), "orders.created".to_string());
        OutboundMessage {
            destination: destination.to_string(),
            headers,
            body: b"{\"amount\":42}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "outbox-key-2".to_string(),
            traceparent: None,
        }
    }

    #[test]
    fn sink_claims_only_the_knative_scheme() {
        assert_eq!(
            KnativeMessageSink::new("http://x").schemes(),
            vec!["knative"]
        );
    }

    #[test]
    fn blank_ingress_falls_back_to_the_java_default() {
        let sink = KnativeMessageSink::new("");
        assert_eq!(
            sink.broker_ingress_base,
            super::super::DEFAULT_BROKER_INGRESS
        );
    }

    #[test]
    fn trailing_slash_is_stripped() {
        let sink =
            KnativeMessageSink::new("http://broker-ingress.knative-eventing.svc.cluster.local/");
        assert_eq!(
            sink.broker_ingress_base,
            "http://broker-ingress.knative-eventing.svc.cluster.local"
        );
    }

    #[test]
    fn parses_namespace_and_broker_from_the_destination() {
        assert_eq!(
            parse_destination("knative://acme-ns/default").unwrap(),
            ("acme-ns".to_string(), "default".to_string())
        );
    }

    #[tokio::test]
    async fn round_trips_to_the_rewritten_broker_ingress_url_with_headers_and_idempotency_key() {
        let (addr, captured) = serve_broker(200).await;
        let sink = KnativeMessageSink::new(format!("http://{addr}"));

        let outcome = sink.send(&message("knative://acme-ns/default")).await;
        assert_eq!(outcome, SendOutcome::Delivered);

        let requests = captured.requests.lock().unwrap();
        let (method, headers, body) = &requests[0];
        assert_eq!(method, "POST");
        assert_eq!(body, b"{\"amount\":42}");
        assert_eq!(headers.get("idempotency-key").unwrap(), "outbox-key-2");
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        assert_eq!(headers.get("ce-id").unwrap(), "evt-2");
    }

    #[tokio::test]
    async fn wrong_scheme_is_a_permanent_invalid_destination_failure() {
        let sink = KnativeMessageSink::new("http://broker");
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
    async fn missing_namespace_is_a_permanent_no_namespace_failure() {
        let sink = KnativeMessageSink::new("http://broker");
        match sink.send(&message("knative://")).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_NO_NAMESPACE),
            other => panic!("expected PermanentFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_broker_is_a_permanent_no_broker_failure() {
        let sink = KnativeMessageSink::new("http://broker");
        match sink.send(&message("knative://acme-ns")).await {
            SendOutcome::PermanentFailure(d) => assert_eq!(d.code, codes::OUTBOUND_NO_BROKER),
            other => panic!("expected PermanentFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unreachable_broker_is_retryable_and_retagged() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let sink = KnativeMessageSink::new(format!("http://{addr}"));
        match sink.send(&message("knative://acme-ns/default")).await {
            SendOutcome::RetryableFailure(d) => assert_eq!(d.code, codes::OUTBOUND_PUBLISH_FAILED),
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }
}
