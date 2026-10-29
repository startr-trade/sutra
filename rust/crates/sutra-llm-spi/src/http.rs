//! The shared JSON-over-HTTPS client provider adapters POST through — one pooled hyper
//! client, rustls with native roots, `http` allowed for local servers and test stubs.

use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;

type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// `url` as it may appear in a log line or a failure detail: every query value and any
/// `user:password@` part replaced by `<redacted>`, so a credential a URL carries never leaves
/// in a message. Parameter names are kept (they help diagnose); the path is kept as is.
pub fn redact_url(url: &str) -> String {
    let (head, fragment) = match url.split_once('#') {
        Some((head, _)) => (head, "#<redacted>"),
        None => (url, ""),
    };
    let (base, query) = match head.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (head, None),
    };
    let base = match base.split_once("://") {
        Some((scheme, rest)) => {
            let (authority, path) = match rest.find('/') {
                Some(i) => rest.split_at(i),
                None => (rest, ""),
            };
            match authority.rsplit_once('@') {
                Some((_, host)) => format!("{scheme}://<redacted>@{host}{path}"),
                None => base.to_string(),
            }
        }
        None => base.to_string(),
    };
    let query = query
        .map(|q| {
            let pairs: Vec<String> = q
                .split('&')
                .map(|pair| match pair.split_once('=') {
                    Some((name, _)) => format!("{name}=<redacted>"),
                    None => pair.to_string(),
                })
                .collect();
            format!("?{}", pairs.join("&"))
        })
        .unwrap_or_default();
    format!("{base}{query}{fragment}")
}

/// Longest slice of a provider's error body kept in a failure detail.
const DETAIL_LIMIT: usize = 300;

/// A failed exchange, before provider-neutral classification (see `From<HttpFailure> for
/// LlmFailure`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpFailure {
    /// A non-2xx answer; `detail` is the start of the error body.
    Status {
        status: u16,
        retry_after: Option<Duration>,
        detail: String,
    },
    Timeout,
    /// Connection refused / reset / TLS failure.
    Connect(String),
    /// The request could not be built, or the 2xx body is not JSON.
    Invalid(String),
}

/// A pooled JSON client.
#[derive(Clone)]
pub struct JsonHttpClient {
    client: Client<HttpsConnector, Full<Bytes>>,
}

impl JsonHttpClient {
    pub fn new(connect_timeout: Duration) -> JsonHttpClient {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(connect_timeout));
        http.enforce_http(false);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            // ring, named explicitly: the build links rustls with more than one provider (the
            // SQL Server driver adds aws-lc-rs), and then rustls has no implicit default.
            .with_provider_and_native_roots(std::sync::Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .expect("native TLS root store")
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        JsonHttpClient {
            client: Client::builder(TokioExecutor::new()).build(https),
        }
    }

    /// POST `body` as JSON to `url` with `headers`, within `deadline`; a 2xx answer is parsed
    /// as JSON.
    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        deadline: Duration,
    ) -> Result<Value, HttpFailure> {
        let uri: hyper::Uri = url.parse().map_err(|e| {
            HttpFailure::Invalid(format!("'{}' is not a valid URI: {e}", redact_url(url)))
        })?;
        let mut builder = Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .header("accept", "application/json");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder
            .body(Full::new(Bytes::from(body.to_string())))
            .map_err(|e| HttpFailure::Invalid(format!("request could not be built: {e}")))?;

        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| HttpFailure::Connect(e.to_string()))?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs);
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| HttpFailure::Connect(e.to_string()))?
                .to_bytes();
            Ok::<_, HttpFailure>((status, retry_after, bytes))
        };
        let (status, retry_after, bytes) = tokio::time::timeout(deadline, exchange)
            .await
            .map_err(|_| HttpFailure::Timeout)??;
        if !(200..300).contains(&status) {
            let text = String::from_utf8_lossy(&bytes);
            let detail: String = text.chars().take(DETAIL_LIMIT).collect();
            return Err(HttpFailure::Status {
                status,
                retry_after,
                detail: format!("HTTP {status}: {detail}"),
            });
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| HttpFailure::Invalid(format!("2xx body is not JSON: {e}")))
    }
}

/// One server-sent event: the optional `event:` name and the joined `data:` lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

/// Incremental server-sent-events parser: feed it body bytes as they arrive.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
}

impl SseParser {
    pub fn new() -> SseParser {
        SseParser::default()
    }

    /// Consume `chunk`, returning every event it completed.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(pos) = self.buffer.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.buffer.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                if !self.data.is_empty() || self.event.is_some() {
                    events.push(SseEvent {
                        event: self.event.take(),
                        data: std::mem::take(&mut self.data).join("\n"),
                    });
                }
            } else if line.starts_with(':') {
                // comment / keep-alive
            } else {
                let (field, value) = line.split_once(':').unwrap_or((line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => self.event = Some(value.to_string()),
                    "data" => self.data.push(value.to_string()),
                    _ => {}
                }
            }
        }
        events
    }
}

impl JsonHttpClient {
    async fn send(
        &self,
        method: hyper::Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
        accept: &str,
    ) -> Result<hyper::Response<hyper::body::Incoming>, HttpFailure> {
        let uri: hyper::Uri = url.parse().map_err(|e| {
            HttpFailure::Invalid(format!("'{}' is not a valid URI: {e}", redact_url(url)))
        })?;
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("accept", accept);
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let payload = body.map(|b| b.to_string()).unwrap_or_default();
        let request = builder
            .body(Full::new(Bytes::from(payload)))
            .map_err(|e| HttpFailure::Invalid(format!("request could not be built: {e}")))?;
        self.client
            .request(request)
            .await
            .map_err(|e| HttpFailure::Connect(e.to_string()))
    }

    /// Turn a non-2xx response into its [`HttpFailure::Status`].
    async fn status_failure(response: hyper::Response<hyper::body::Incoming>) -> HttpFailure {
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let bytes = match response.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => Bytes::new(),
        };
        let text = String::from_utf8_lossy(&bytes);
        let detail: String = text.chars().take(DETAIL_LIMIT).collect();
        HttpFailure::Status {
            status,
            retry_after,
            detail: format!("HTTP {status}: {detail}"),
        }
    }

    /// POST a raw `body` with `content_type` (a multipart form, say) and return the 2xx body as
    /// bytes with its `content-type` — for endpoints that answer with media rather than JSON.
    pub async fn post_bytes(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        content_type: &str,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<(Vec<u8>, Option<String>), HttpFailure> {
        let uri: hyper::Uri = url.parse().map_err(|e| {
            HttpFailure::Invalid(format!("'{}' is not a valid URI: {e}", redact_url(url)))
        })?;
        let mut builder = Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("content-type", content_type)
            .header("accept", "*/*");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| HttpFailure::Invalid(format!("request could not be built: {e}")))?;
        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| HttpFailure::Connect(e.to_string()))?;
            if !response.status().is_success() {
                return Err(Self::status_failure(response).await);
            }
            let kind = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| HttpFailure::Connect(e.to_string()))?
                .to_bytes();
            Ok((bytes.to_vec(), kind))
        };
        tokio::time::timeout(deadline, exchange)
            .await
            .map_err(|_| HttpFailure::Timeout)?
    }

    /// GET `url` and return the 2xx body as text (a JSONL results file, for one).
    pub async fn get_text(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        deadline: Duration,
    ) -> Result<String, HttpFailure> {
        let exchange = async {
            let response = self
                .send(hyper::Method::GET, url, headers, None, "*/*")
                .await?;
            if !response.status().is_success() {
                return Err(Self::status_failure(response).await);
            }
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| HttpFailure::Connect(e.to_string()))?
                .to_bytes();
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        };
        tokio::time::timeout(deadline, exchange)
            .await
            .map_err(|_| HttpFailure::Timeout)?
    }

    /// GET `url` and parse the 2xx body as JSON.
    pub async fn get_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        deadline: Duration,
    ) -> Result<Value, HttpFailure> {
        let text = self.get_text(url, headers, deadline).await?;
        serde_json::from_str(&text)
            .map_err(|e| HttpFailure::Invalid(format!("2xx body is not JSON: {e}")))
    }

    /// POST `body` as JSON and read the 2xx answer as a server-sent-event stream, calling
    /// `on_event` for each event as it arrives, all within `deadline`.
    pub async fn post_sse(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        deadline: Duration,
        on_event: &mut (dyn FnMut(&SseEvent) + Send),
    ) -> Result<(), HttpFailure> {
        let exchange = async {
            let mut response = self
                .send(
                    hyper::Method::POST,
                    url,
                    headers,
                    Some(body),
                    "text/event-stream",
                )
                .await?;
            if !response.status().is_success() {
                return Err(Self::status_failure(response).await);
            }
            let mut parser = SseParser::new();
            while let Some(frame) = response.body_mut().frame().await {
                let frame = frame.map_err(|e| HttpFailure::Connect(e.to_string()))?;
                if let Some(data) = frame.data_ref() {
                    for event in parser.feed(data) {
                        on_event(&event);
                    }
                }
            }
            for event in parser.feed(b"\n\n") {
                on_event(&event);
            }
            Ok(())
        };
        tokio::time::timeout(deadline, exchange)
            .await
            .map_err(|_| HttpFailure::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn redacted_urls_keep_no_query_value_or_userinfo() {
        assert_eq!(
            redact_url("wss://h.example/ws/x?key=secret&alt=sse"),
            "wss://h.example/ws/x?key=<redacted>&alt=<redacted>"
        );
        assert_eq!(
            redact_url("https://user:pw@h.example:8443/p?flag"),
            "https://<redacted>@h.example:8443/p?flag"
        );
        assert_eq!(redact_url("http://h/p#frag"), "http://h/p#<redacted>");
        assert_eq!(redact_url("http://h/p"), "http://h/p");
        assert!(!redact_url("x?access_token=t0k").contains("t0k"));
    }

    /// One-shot HTTP/1.1 stub answering `status` + `body`.
    async fn stub(status: u16, extra: &'static str, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{addr}/v1")
    }

    #[tokio::test]
    async fn post_json_parses_a_2xx_body() {
        let url = stub(200, "", r#"{"ok":true}"#).await;
        let client = JsonHttpClient::new(Duration::from_secs(2));
        let value = client
            .post_json(&url, &[], &serde_json::json!({}), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(value["ok"], true);
    }

    #[tokio::test]
    async fn post_json_reports_status_and_retry_after() {
        let url = stub(429, "retry-after: 7\r\n", r#"{"error":"slow down"}"#).await;
        let client = JsonHttpClient::new(Duration::from_secs(2));
        let failure = client
            .post_json(&url, &[], &serde_json::json!({}), Duration::from_secs(5))
            .await
            .unwrap_err();
        match failure {
            HttpFailure::Status {
                status,
                retry_after,
                ..
            } => {
                assert_eq!(status, 429);
                assert_eq!(retry_after, Some(Duration::from_secs(7)));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_json_connect_refused_is_connect_failure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let client = JsonHttpClient::new(Duration::from_secs(1));
        let failure = client
            .post_json(
                &format!("http://{addr}/"),
                &[],
                &serde_json::json!({}),
                Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(matches!(failure, HttpFailure::Connect(_)), "{failure:?}");
    }

    #[test]
    fn sse_parser_handles_split_chunks_comments_and_crlf() {
        let mut parser = SseParser::new();
        let mut events = parser.feed(b": ping\r\nevent: a\r\ndata: {\"x\"");
        assert!(events.is_empty());
        events.extend(parser.feed(b":1}\r\n\r\ndata: one\ndata: two\n\n"));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.as_deref(), Some("a"));
        assert_eq!(events[0].data, "{\"x\":1}");
        assert_eq!(events[1].event, None);
        assert_eq!(events[1].data, "one\ntwo");
    }

    #[tokio::test]
    async fn get_json_and_post_sse_round_trip() {
        let url = stub(200, "", r#"{"ok":1}"#).await;
        let client = JsonHttpClient::new(Duration::from_secs(2));
        let value = client
            .get_json(&url, &[], Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(value["ok"], 1);
        let url = stub(200, "", "event: e\ndata: d\n\n").await;
        let mut seen = Vec::new();
        client
            .post_sse(
                &url,
                &[],
                &serde_json::json!({}),
                Duration::from_secs(5),
                &mut |e| seen.push(e.clone()),
            )
            .await
            .unwrap();
        assert_eq!(seen[0].event.as_deref(), Some("e"));
        assert_eq!(seen[0].data, "d");
    }
}
