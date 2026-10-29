//! The HTTP/HTTPS [`InlineCaller`]: a real request/response for a synchronous outbound
//! channel. Wire format matches the outbound sink (`Idempotency-Key` = outbox key, the
//! message's `Content-Type`, `traceparent`, `X-HTTP-Method` override consumed); the
//! response body and headers come back to the caller.
//!
//! Outcomes: 2xx is the response; 408 / 429 / 5xx / connect / timeout are retryable;
//! other 4xx / 3xx / an invalid destination are permanent.

use std::collections::BTreeMap;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use sutra_channels::codes::OUTBOUND_SEND_FAILED;
use sutra_channels::telemetry::TRACEPARENT_HEADER;
use sutra_channels::{
    BoxFuture, Diagnostic, InlineCaller, InlineFailure, InlineResponse, OutboundMessage,
};

const HEADER_METHOD_OVERRIDE: &str = "X-HTTP-Method";
const HEADER_IDEMPOTENCY_KEY: &str = "Idempotency-Key";
const HEADER_CONTENT_TYPE: &str = "Content-Type";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
/// Longest slice of a response body echoed into a diagnostic.
const BODY_EXCERPT_MAX: usize = 300;

type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// One pooled hyper client serving `http` and `https`.
pub struct HttpInlineCaller {
    client: Client<HttpsConnector, Full<Bytes>>,
    request_timeout: Duration,
}

impl HttpInlineCaller {
    /// Connect 5 s, request 30 s; the dispatcher applies the task's real deadline.
    pub fn new() -> HttpInlineCaller {
        HttpInlineCaller::with_timeouts(Duration::from_secs(5), Duration::from_secs(30))
    }

    pub fn with_timeouts(connect_timeout: Duration, request_timeout: Duration) -> HttpInlineCaller {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(connect_timeout));
        http.enforce_http(false); // the rustls wrapper decides per scheme
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            // The process-wide configuration: the CA bundle is read once, not per client.
            .with_tls_config(
                sutra_channels::shared_client_tls_config().expect("native TLS root store"),
            )
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        HttpInlineCaller {
            client: Client::builder(TokioExecutor::new()).build(https),
            request_timeout,
        }
    }

    async fn call_once(&self, message: &OutboundMessage) -> Result<InlineResponse, InlineFailure> {
        let destination = &message.destination;
        let permanent =
            |text: String| InlineFailure::Permanent(Diagnostic::error(OUTBOUND_SEND_FAILED, text));
        let retryable =
            |text: String| InlineFailure::Retryable(Diagnostic::error(OUTBOUND_SEND_FAILED, text));

        let uri: hyper::Uri = destination.parse().map_err(|e| {
            permanent(format!(
                "destination '{destination}' is not a valid URI: {e}"
            ))
        })?;
        let method_text = method_for(message);
        let method: hyper::Method = method_text.parse().map_err(|_| {
            permanent(format!(
                "X-HTTP-Method override '{method_text}' is not a valid HTTP method"
            ))
        })?;

        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(HEADER_CONTENT_TYPE, resolved_content_type(message))
            .header(HEADER_IDEMPOTENCY_KEY, &message.outbox_key);
        if let Some(traceparent) = message
            .traceparent
            .as_deref()
            .filter(|t| !t.trim().is_empty())
        {
            builder = builder.header(TRACEPARENT_HEADER, traceparent);
        }
        for (name, value) in &message.headers {
            if [
                HEADER_METHOD_OVERRIDE,
                HEADER_CONTENT_TYPE,
                HEADER_IDEMPOTENCY_KEY,
                TRACEPARENT_HEADER,
            ]
            .iter()
            .any(|computed| name.eq_ignore_ascii_case(computed))
            {
                continue; // computed headers win
            }
            builder = builder.header(name, value);
        }
        let request = builder
            .body(Full::new(Bytes::from(message.body.clone())))
            .map_err(|e| {
                permanent(format!(
                    "HTTP request to {destination} could not be built: {e}"
                ))
            })?;

        let exchange = async {
            let response = self.client.request(request).await?;
            let (parts, body) = response.into_parts();
            let bytes = body.collect().await?.to_bytes();
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((parts, bytes))
        };
        let (parts, bytes) = match tokio::time::timeout(self.request_timeout, exchange).await {
            Err(_) => {
                return Err(retryable(format!(
                    "HTTP call to {destination} timed out after {:?}",
                    self.request_timeout
                )))
            }
            Ok(Err(e)) => return Err(retryable(format!("HTTP call to {destination} failed: {e}"))),
            Ok(Ok(done)) => done,
        };

        let code = parts.status.as_u16();
        if (200..300).contains(&code) {
            let mut headers = BTreeMap::new();
            for (name, value) in &parts.headers {
                if let Ok(value) = value.to_str() {
                    headers.insert(name.as_str().to_ascii_lowercase(), value.to_string());
                }
            }
            let content_type = headers.get("content-type").cloned();
            return Ok(InlineResponse {
                body: bytes.to_vec(),
                content_type,
                headers,
            });
        }
        let excerpt: String = String::from_utf8_lossy(&bytes)
            .chars()
            .take(BODY_EXCERPT_MAX)
            .collect();
        let text = format!("HTTP call to {destination} failed with status {code}: {excerpt}");
        if code == 408 || code == 429 || code >= 500 {
            Err(retryable(text))
        } else {
            Err(permanent(text))
        }
    }
}

impl Default for HttpInlineCaller {
    fn default() -> HttpInlineCaller {
        HttpInlineCaller::new()
    }
}

impl InlineCaller for HttpInlineCaller {
    fn schemes(&self) -> Vec<String> {
        vec!["http".to_string(), "https".to_string()]
    }

    fn call<'a>(
        &'a self,
        request: &'a OutboundMessage,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        Box::pin(self.call_once(request))
    }
}

/// The `X-HTTP-Method` override (case-insensitive name, value uppercased), default POST.
fn method_for(message: &OutboundMessage) -> String {
    message
        .headers
        .iter()
        .find(|(name, value)| {
            name.eq_ignore_ascii_case(HEADER_METHOD_OVERRIDE) && !value.trim().is_empty()
        })
        .map(|(_, value)| value.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "POST".to_string())
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
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Serve one canned response; resolves to the raw request text.
    async fn stub(response: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/call", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                seen.extend_from_slice(&buf[..n]);
                if n == 0 || seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    // Small bodies arrive with the head in practice; read the rest briefly.
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
            String::from_utf8_lossy(&seen).to_string()
        });
        (url, handle)
    }

    fn message(destination: String) -> OutboundMessage {
        OutboundMessage {
            destination,
            headers: BTreeMap::new(),
            body: b"{\"q\":1}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "ob-key-9".to_string(),
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        }
    }

    /// Building HTTP clients after the process has run out of file descriptors: the CA bundle was
    /// read once, by the first client, so every later client (one per execution lane, sink and
    /// inline caller) builds without opening it. Before the shared configuration, each build read
    /// the bundle, and a build under EMFILE panicked ("native TLS root store").
    ///
    /// The fd exhaustion runs in a child process (this same test binary, re-executed for this one
    /// test under a low `ulimit -n`), so no other test in this binary ever sees it.
    #[test]
    fn clients_built_after_the_process_runs_out_of_file_descriptors_reuse_the_loaded_roots() {
        const CHILD: &str = "SUTRA_TEST_FD_EXHAUSTION_CHILD";
        const NAME: &str = "inline::tests::\
            clients_built_after_the_process_runs_out_of_file_descriptors_reuse_the_loaded_roots";
        if std::env::var_os(CHILD).is_none() {
            let exe = std::env::current_exe().expect("test binary path");
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg("ulimit -n 256 && exec \"$0\" \"$@\"")
                .arg(exe)
                .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .output()
                .expect("re-run this test in a child process");
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.status.success(), "child failed:\n{text}");
            assert!(text.contains("1 passed"), "the child ran the test:\n{text}");
            return;
        }

        // The first client loads the roots.
        let _first = HttpInlineCaller::new();
        // Now use up every descriptor the process may open.
        let mut held = Vec::new();
        loop {
            match std::fs::File::open("/dev/null") {
                Ok(file) => held.push(file),
                Err(e) if e.raw_os_error() == Some(24) => break,
                Err(e) => panic!("unexpected open error: {e}"),
            }
        }
        // The condition is real: reading the bundle fresh fails here.
        assert!(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_provider_and_native_roots(Arc::new(rustls::crypto::ring::default_provider()))
                .is_err(),
            "a fresh read of the CA bundle must fail under EMFILE"
        );
        // Every later client still builds.
        for _ in 0..32 {
            let _caller = HttpInlineCaller::new();
            let _sink = sutra_channels::HttpSink::new();
        }
        drop(held);
    }

    #[tokio::test]
    async fn ok_returns_body_content_type_and_lowercased_headers() {
        let (url, seen) = stub(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Trace-Id: abc\r\n\
             Content-Length: 8\r\nConnection: close\r\n\r\n{\"a\":42}",
        )
        .await;
        let response = HttpInlineCaller::new().call(&message(url)).await.unwrap();
        assert_eq!(response.body, b"{\"a\":42}");
        assert_eq!(response.content_type.as_deref(), Some("application/json"));
        assert_eq!(response.headers["x-trace-id"], "abc");
        let request = seen.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("post /call"), "{request}");
        assert!(request.contains("idempotency-key: ob-key-9"));
        assert!(request.contains("content-type: application/json"));
        assert!(request.contains("traceparent: 00-0af7"));
    }

    #[tokio::test]
    async fn method_override_is_applied_and_not_forwarded() {
        let (url, seen) =
            stub("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
        let mut m = message(url);
        m.headers.insert("x-http-method".into(), "put".into());
        HttpInlineCaller::new().call(&m).await.unwrap();
        let request = seen.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("put /call"), "{request}");
        assert!(!request.contains("x-http-method"));
    }

    #[tokio::test]
    async fn service_unavailable_is_retryable() {
        let (url, _) =
            stub("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbusy")
                .await;
        let err = HttpInlineCaller::new()
            .call(&message(url))
            .await
            .unwrap_err();
        let InlineFailure::Retryable(d) = err else {
            panic!("expected retryable, got {err:?}");
        };
        assert_eq!(d.code, OUTBOUND_SEND_FAILED);
        assert!(d.message.contains("503"));
    }

    #[tokio::test]
    async fn not_found_is_permanent() {
        let (url, _) =
            stub("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
        let err = HttpInlineCaller::new()
            .call(&message(url))
            .await
            .unwrap_err();
        assert!(matches!(err, InlineFailure::Permanent(_)), "{err:?}");
    }

    #[tokio::test]
    async fn invalid_uri_is_permanent_and_refused_connection_retryable() {
        let caller = Arc::new(HttpInlineCaller::new());
        let err = caller.call(&message("http://".into())).await.unwrap_err();
        assert!(matches!(err, InlineFailure::Permanent(_)), "{err:?}");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/x", listener.local_addr().unwrap());
        drop(listener);
        let err = caller.call(&message(url)).await.unwrap_err();
        assert!(matches!(err, InlineFailure::Retryable(_)), "{err:?}");
    }
}
