//! An S3-compatible object store over HTTP(S), signed with Signature Version 4: put / get /
//! delete / bucket create, a ranged get, a paginated object listing (ListObjectsV2), and
//! multipart uploads (create, upload part, complete, abort, list uploads, list parts).
//!
//! Addressing: with an `endpoint` (a self-hosted store, `http://store:9000`) requests are
//! path-style (`<endpoint>/<bucket>/<key>`); without one, virtual-hosted style against
//! `https://<bucket>.s3.<region>.amazonaws.com`. `path_style` forces path-style either way.

use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::sigv4::{self, Credentials, SigningRequest};
use crate::{
    checked_full_key, checked_list_prefix, relative_key, sha256_hex, BoxFuture, ListedObject,
    ObjectStore, ObjectStoreError, PendingUpload, StoreLocation, StoredObject, UploadedPart,
};

/// The default page size of a listing request (`max-keys` / `max-uploads` / `max-parts`): the
/// S3 maximum, so a listing takes as few round trips as the store allows.
pub const S3_LIST_PAGE_SIZE: u32 = 1000;

/// A bound on the pages one listing follows, so a store that keeps answering "truncated" with
/// a token cannot hold a caller in a loop.
const MAX_LIST_PAGES: usize = 100_000;

type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// How an [`S3ObjectStore`] is reached.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// `s3://<bucket>[/<prefix>]`.
    pub location: StoreLocation,
    /// The store's base URL for a self-hosted or non-AWS store (`http://store:9000`); `None`
    /// for AWS itself.
    pub endpoint: Option<String>,
    pub region: String,
    pub credentials: Credentials,
    /// Address `<endpoint>/<bucket>/<key>` (always on with an endpoint unless turned off).
    pub path_style: bool,
    /// Bound on one request, connect to last byte.
    pub timeout: Duration,
    /// How many entries one listing request asks for ([`S3_LIST_PAGE_SIZE`] by default); a
    /// listing follows the store's continuation markers across pages either way.
    pub list_page_size: u32,
}

impl S3Config {
    pub fn new(location: StoreLocation, region: &str, credentials: Credentials) -> S3Config {
        S3Config {
            location,
            endpoint: None,
            region: region.to_string(),
            credentials,
            path_style: false,
            timeout: Duration::from_secs(60),
            list_page_size: S3_LIST_PAGE_SIZE,
        }
    }
}

/// An S3-compatible store.
#[derive(Clone)]
pub struct S3ObjectStore {
    config: S3Config,
    client: Client<HttpsConnector, Full<Bytes>>,
}

struct Target {
    url: String,
    host: String,
    path: String,
}

impl S3ObjectStore {
    pub fn new(config: S3Config) -> Result<S3ObjectStore, ObjectStoreError> {
        if config.location.scheme != "s3" {
            return Err(ObjectStoreError::Config(format!(
                "an S3 store needs an s3:// location, got {}://",
                config.location.scheme
            )));
        }
        if let Some(endpoint) = &config.endpoint {
            let lowered = endpoint.to_ascii_lowercase();
            if !lowered.starts_with("http://") && !lowered.starts_with("https://") {
                return Err(ObjectStoreError::Config(format!(
                    "endpoint '{endpoint}' is not an http(s) URL"
                )));
            }
        }
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(Duration::from_secs(10)));
        http.enforce_http(false);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            // ring, named explicitly: the build links rustls with more than one provider (the
            // SQL Server driver adds aws-lc-rs), and then rustls has no implicit default.
            .with_provider_and_native_roots(std::sync::Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(|e| ObjectStoreError::Config(format!("no TLS root store: {e}")))?
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        Ok(S3ObjectStore {
            config,
            client: Client::builder(TokioExecutor::new()).build(https),
        })
    }

    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Where a request for `key` (a full key; `None` for the bucket itself) goes.
    fn target(&self, key: Option<&str>) -> Target {
        let bucket = &self.config.location.bucket;
        let path_style = self.config.path_style || self.config.endpoint.is_some();
        let base = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.config.region))
            .trim_end_matches('/')
            .to_string();
        let (scheme, authority) = base.split_once("://").unwrap_or(("https", &base));
        let authority = authority.split('/').next().unwrap_or_default().to_string();
        let key_path = key.map(|k| format!("/{k}")).unwrap_or_default();
        let (host, path) = if path_style {
            (authority, format!("/{bucket}{key_path}"))
        } else {
            (
                format!("{bucket}.{authority}"),
                if key_path.is_empty() {
                    "/".to_string()
                } else {
                    key_path
                },
            )
        };
        Target {
            url: format!("{scheme}://{host}{}", sigv4::uri_encode(&path, true)),
            host,
            path,
        }
    }

    /// Send one signed request with `query` (unencoded pairs) and `extra_headers` (lower-case
    /// names, signed); answers the status, the response headers, and the body.
    async fn send(
        &self,
        method: &str,
        target: &Target,
        query: &[(&str, &str)],
        extra_headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, hyper::HeaderMap, Vec<u8>), ObjectStoreError> {
        let payload_hash = sha256_hex(&body);
        let amz_date = sigv4::amz_date(time::OffsetDateTime::now_utc());
        let mut headers = vec![
            ("host".to_string(), target.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &self.config.credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        headers.extend(extra_headers.iter().cloned());
        let authorization = sigv4::authorization(
            &SigningRequest {
                method,
                path: &target.path,
                query,
                headers: &headers,
                payload_hash: &payload_hash,
                region: &self.config.region,
                amz_date: &amz_date,
            },
            &self.config.credentials,
        );
        let mut url = target.url.clone();
        if !query.is_empty() {
            let encoded: Vec<String> = query
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        sigv4::uri_encode(k, false),
                        sigv4::uri_encode(v, false)
                    )
                })
                .collect();
            url.push('?');
            url.push_str(&encoded.join("&"));
        }
        let method_value: hyper::Method = method
            .parse()
            .map_err(|_| ObjectStoreError::Config(format!("bad method {method}")))?;
        let mut builder = Request::builder()
            .method(method_value)
            .uri(&url)
            .header("authorization", authorization);
        for (name, value) in headers.iter().filter(|(n, _)| n != "host") {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| ObjectStoreError::Config(format!("request could not be built: {e}")))?;
        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| ObjectStoreError::Unavailable(e.to_string()))?;
            let status = response.status().as_u16();
            let response_headers = response.headers().clone();
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| ObjectStoreError::Unavailable(e.to_string()))?
                .to_bytes();
            Ok((status, response_headers, bytes.to_vec()))
        };
        tokio::time::timeout(self.config.timeout, exchange)
            .await
            .map_err(|_| ObjectStoreError::Unavailable("timed out".to_string()))?
    }

    /// The full key of `key` (checked) and where a request for it goes.
    fn key_target(&self, key: &str) -> Result<(String, Target), ObjectStoreError> {
        let full = checked_full_key(&self.config.location, key)?;
        let target = self.target(Some(&full));
        Ok((full, target))
    }

    /// `list_page_size` as a query value (at least 1).
    fn page_size(&self) -> String {
        self.config.list_page_size.max(1).to_string()
    }

    fn failure(operation: &str, status: u16, body: &[u8]) -> ObjectStoreError {
        // The store's error code (`<Code>…</Code>`), never the body as a whole.
        let text = String::from_utf8_lossy(body);
        let code = tag_text(&text, "Code")
            .unwrap_or_default()
            .chars()
            .take(80)
            .collect::<String>();
        let message = format!("{operation}: HTTP {status} {code}")
            .trim()
            .to_string();
        if status >= 500 || status == 429 || status == 408 {
            ObjectStoreError::Unavailable(message)
        } else {
            ObjectStoreError::Rejected(message)
        }
    }
}

/// The text of the first `<name>…</name>` in `xml`, entity-decoded.
fn tag_text(xml: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml_unescape(&xml[start..end]))
}

/// Every `<name>…</name>` block of `xml`, in order.
fn tag_blocks<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(at) = rest.find(&open) {
        let after = &rest[at + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(&after[..end]);
        rest = &after[end + close.len()..];
    }
    out
}

fn xml_unescape(text: &str) -> String {
    text.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#34;", "\"")
        .replace("&amp;", "&")
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Whether a listing answer says more pages follow.
fn truncated(xml: &str) -> bool {
    tag_text(xml, "IsTruncated").as_deref() == Some("true")
}

/// The body a ranged GET answered, cut to the range when the store ignored the `Range` header
/// and sent the whole object (200 without `content-range`).
fn range_of(
    status: u16,
    headers: &hyper::HeaderMap,
    body: Vec<u8>,
    start: u64,
    len: u64,
) -> Vec<u8> {
    if status == 206 || headers.contains_key("content-range") {
        return body;
    }
    let from = usize::try_from(start).unwrap_or(usize::MAX).min(body.len());
    let to = usize::try_from(start.saturating_add(len))
        .unwrap_or(usize::MAX)
        .min(body.len());
    body[from..to].to_vec()
}

impl ObjectStore for S3ObjectStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<StoredObject, ObjectStoreError>> {
        Box::pin(async move {
            let (full, target) = self.key_target(key)?;
            let sha256 = sha256_hex(&body);
            let bytes = body.len() as u64;
            let headers = [("content-type".to_string(), content_type.to_string())];
            let (status, _, answer) = self.send("PUT", &target, &[], &headers, body).await?;
            if !(200..300).contains(&status) {
                return Err(Self::failure("put", status, &answer));
            }
            Ok(StoredObject {
                uri: self.uri_of(key),
                key: full,
                sha256,
                bytes,
            })
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let (status, _, answer) = self.send("GET", &target, &[], &[], Vec::new()).await?;
            match status {
                200..=299 => Ok(Some(answer)),
                404 => Ok(None),
                _ => Err(Self::failure("get", status, &answer)),
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let (status, _, answer) = self.send("DELETE", &target, &[], &[], Vec::new()).await?;
            match status {
                // S3 answers 204 whether or not the object existed; some stores answer 404.
                200..=299 | 404 => Ok(()),
                _ => Err(Self::failure("delete", status, &answer)),
            }
        })
    }

    fn ensure_bucket(&self) -> BoxFuture<'_, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let target = self.target(None);
            let body = if self.config.region == "us-east-1" {
                Vec::new()
            } else {
                format!(
                    "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                    self.config.region
                )
                .into_bytes()
            };
            let (status, _, answer) = self.send("PUT", &target, &[], &[], body).await?;
            let text = String::from_utf8_lossy(&answer);
            if (200..300).contains(&status) || text.contains("BucketAlreadyOwnedByYou") {
                Ok(())
            } else {
                Err(Self::failure("create bucket", status, &answer))
            }
        })
    }

    fn uri_of(&self, key: &str) -> String {
        self.config.location.uri_of(key)
    }

    fn full_key(&self, key: &str) -> String {
        self.config.location.full_key(key)
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            if len == 0 {
                // An empty range cannot be asked for; answer whether the object is there.
                return Ok(self.get(key).await?.map(|_| Vec::new()));
            }
            let last = start.saturating_add(len - 1);
            let range = [("range".to_string(), format!("bytes={start}-{last}"))];
            let (status, headers, answer) =
                self.send("GET", &target, &[], &range, Vec::new()).await?;
            match status {
                200..=299 => Ok(Some(range_of(status, &headers, answer, start, len))),
                404 => Ok(None),
                // The range starts past the end of the object.
                416 => Ok(Some(Vec::new())),
                _ => Err(Self::failure("get range", status, &answer)),
            }
        })
    }

    fn list_objects<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ListedObject>, ObjectStoreError>> {
        Box::pin(async move {
            checked_list_prefix(prefix)?;
            let target = self.target(None);
            let base = self.config.location.full_key("");
            let full_prefix = format!("{base}{prefix}");
            let page_size = self.page_size();
            let mut out = Vec::new();
            let mut token = String::new();
            for _ in 0..MAX_LIST_PAGES {
                let mut query: Vec<(&str, &str)> = vec![
                    ("list-type", "2"),
                    ("max-keys", &page_size),
                    ("prefix", &full_prefix),
                ];
                if !token.is_empty() {
                    query.push(("continuation-token", &token));
                }
                let (status, _, answer) =
                    self.send("GET", &target, &query, &[], Vec::new()).await?;
                if !(200..300).contains(&status) {
                    return Err(Self::failure("list objects", status, &answer));
                }
                let text = String::from_utf8_lossy(&answer).to_string();
                for block in tag_blocks(&text, "Contents") {
                    let Some(key) = tag_text(block, "Key").and_then(|k| relative_key(&base, &k))
                    else {
                        continue;
                    };
                    let bytes = tag_text(block, "Size")
                        .and_then(|n| n.parse().ok())
                        .unwrap_or(0);
                    out.push(ListedObject { key, bytes });
                }
                if !truncated(&text) {
                    break;
                }
                token = tag_text(&text, "NextContinuationToken").unwrap_or_default();
                if token.is_empty() {
                    break;
                }
            }
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
    }

    fn create_upload<'a>(
        &'a self,
        key: &'a str,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let headers = [("content-type".to_string(), content_type.to_string())];
            let (status, _, answer) = self
                .send("POST", &target, &[("uploads", "")], &headers, Vec::new())
                .await?;
            if !(200..300).contains(&status) {
                return Err(Self::failure("create upload", status, &answer));
            }
            tag_text(&String::from_utf8_lossy(&answer), "UploadId")
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    ObjectStoreError::Rejected("create upload: no UploadId in the answer".into())
                })
        })
    }

    fn upload_part<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        number: u32,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        Box::pin(async move {
            if !(1..=10_000).contains(&number) {
                return Err(ObjectStoreError::Rejected(format!(
                    "upload part: part number {number} is outside 1..=10000"
                )));
            }
            let (_, target) = self.key_target(key)?;
            let number_text = number.to_string();
            let query = [
                ("partNumber", number_text.as_str()),
                ("uploadId", upload_id),
            ];
            let (status, headers, answer) = self.send("PUT", &target, &query, &[], body).await?;
            if !(200..300).contains(&status) {
                return Err(Self::failure("upload part", status, &answer));
            }
            headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .filter(|etag| !etag.is_empty())
                .map(str::to_string)
                .ok_or_else(|| ObjectStoreError::Rejected("upload part: no ETag".into()))
        })
    }

    fn complete_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        parts: &'a [UploadedPart],
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let mut xml = String::from("<CompleteMultipartUpload>");
            for part in parts {
                xml.push_str(&format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                    part.number,
                    xml_escape(&part.etag)
                ));
            }
            xml.push_str("</CompleteMultipartUpload>");
            let headers = [("content-type".to_string(), "application/xml".to_string())];
            let (status, _, answer) = self
                .send(
                    "POST",
                    &target,
                    &[("uploadId", upload_id)],
                    &headers,
                    xml.into_bytes(),
                )
                .await?;
            // A completion can fail after a 200 with an <Error> body (the store answers 200 at
            // once and keeps the connection open while it assembles); the code decides whether
            // a retry may succeed.
            let text = String::from_utf8_lossy(&answer);
            if !(200..300).contains(&status) {
                return Err(Self::failure("complete upload", status, &answer));
            }
            if text.contains("<Error>") {
                let code = tag_text(&text, "Code").unwrap_or_default();
                let transient = matches!(
                    code.as_str(),
                    "InternalError" | "SlowDown" | "ServiceUnavailable" | "RequestTimeout"
                );
                return Err(Self::failure(
                    "complete upload",
                    if transient { 503 } else { 400 },
                    &answer,
                ));
            }
            Ok(())
        })
    }

    fn abort_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let (status, _, answer) = self
                .send(
                    "DELETE",
                    &target,
                    &[("uploadId", upload_id)],
                    &[],
                    Vec::new(),
                )
                .await?;
            if (200..300).contains(&status) || status == 404 {
                Ok(())
            } else {
                Err(Self::failure("abort upload", status, &answer))
            }
        })
    }

    fn list_uploads<'a>(
        &'a self,
        key_or_prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PendingUpload>, ObjectStoreError>> {
        Box::pin(async move {
            checked_list_prefix(key_or_prefix)?;
            let target = self.target(None);
            let base = self.config.location.full_key("");
            let full_prefix = format!("{base}{key_or_prefix}");
            let page_size = self.page_size();
            let mut out = Vec::new();
            let (mut key_marker, mut id_marker) = (String::new(), String::new());
            for _ in 0..MAX_LIST_PAGES {
                let mut query: Vec<(&str, &str)> = vec![
                    ("max-uploads", &page_size),
                    ("prefix", &full_prefix),
                    ("uploads", ""),
                ];
                if !key_marker.is_empty() {
                    query.push(("key-marker", &key_marker));
                    if !id_marker.is_empty() {
                        query.push(("upload-id-marker", &id_marker));
                    }
                }
                let (status, _, answer) =
                    self.send("GET", &target, &query, &[], Vec::new()).await?;
                if !(200..300).contains(&status) {
                    return Err(Self::failure("list uploads", status, &answer));
                }
                let text = String::from_utf8_lossy(&answer).to_string();
                // The last key the page listed: some stores answer a truncated page with an
                // empty NextKeyMarker and only a NextUploadIdMarker, which a store honours
                // only next to a key marker.
                let mut last_key = String::new();
                for block in tag_blocks(&text, "Upload") {
                    let (Some(full), Some(upload_id)) =
                        (tag_text(block, "Key"), tag_text(block, "UploadId"))
                    else {
                        continue;
                    };
                    last_key.clone_from(&full);
                    // A store that ignores `prefix` still answers only what was asked for.
                    if !full.starts_with(&full_prefix) {
                        continue;
                    }
                    if let Some(key) = relative_key(&base, &full) {
                        out.push(PendingUpload { key, upload_id });
                    }
                }
                if !truncated(&text) {
                    break;
                }
                let next_key = tag_text(&text, "NextKeyMarker").unwrap_or_default();
                let next_id = tag_text(&text, "NextUploadIdMarker").unwrap_or_default();
                let next_key = if next_key.is_empty() {
                    last_key
                } else {
                    next_key
                };
                // No marker to go on from, or the same one again: stop rather than loop.
                if next_key.is_empty() || (&next_key, &next_id) == (&key_marker, &id_marker) {
                    break;
                }
                (key_marker, id_marker) = (next_key, next_id);
            }
            out.sort_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
            out.dedup();
            Ok(out)
        })
    }

    fn list_parts<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<UploadedPart>, ObjectStoreError>> {
        Box::pin(async move {
            let (_, target) = self.key_target(key)?;
            let page_size = self.page_size();
            let mut out = Vec::new();
            let mut marker = String::new();
            for _ in 0..MAX_LIST_PAGES {
                let mut query: Vec<(&str, &str)> =
                    vec![("max-parts", &page_size), ("uploadId", upload_id)];
                if !marker.is_empty() {
                    query.push(("part-number-marker", &marker));
                }
                let (status, _, answer) =
                    self.send("GET", &target, &query, &[], Vec::new()).await?;
                if !(200..300).contains(&status) {
                    return Err(Self::failure("list parts", status, &answer));
                }
                let text = String::from_utf8_lossy(&answer).to_string();
                for block in tag_blocks(&text, "Part") {
                    let number = tag_text(block, "PartNumber").and_then(|n| n.parse().ok());
                    let size = tag_text(block, "Size").and_then(|n| n.parse().ok());
                    if let (Some(number), Some(etag), Some(size)) =
                        (number, tag_text(block, "ETag"), size)
                    {
                        out.push(UploadedPart { number, etag, size });
                    }
                }
                if !truncated(&text) {
                    break;
                }
                marker = tag_text(&text, "NextPartNumberMarker").unwrap_or_default();
                if marker.is_empty() {
                    break;
                }
            }
            out.sort_by_key(|p| p.number);
            out.dedup_by_key(|p| p.number);
            Ok(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(endpoint: Option<&str>) -> S3ObjectStore {
        let mut config = S3Config::new(
            StoreLocation::parse("s3://recordings/calls").unwrap(),
            "us-east-1",
            Credentials {
                access_key: "a".into(),
                secret_key: "b".into(),
                session_token: None,
            },
        );
        config.endpoint = endpoint.map(str::to_string);
        S3ObjectStore::new(config).unwrap()
    }

    #[test]
    fn addresses_path_style_with_an_endpoint_and_virtual_host_without() {
        let t = store(Some("http://store:9000/")).target(Some("calls/x.wav"));
        assert_eq!(t.url, "http://store:9000/recordings/calls/x.wav");
        assert_eq!(t.host, "store:9000");
        assert_eq!(t.path, "/recordings/calls/x.wav");
        let t = store(None).target(Some("calls/x.wav"));
        assert_eq!(
            t.url,
            "https://recordings.s3.us-east-1.amazonaws.com/calls/x.wav"
        );
        assert_eq!(t.path, "/calls/x.wav");
        assert_eq!(
            store(Some("http://store:9000")).target(None).path,
            "/recordings"
        );
    }

    #[test]
    fn reads_listing_answers_and_cuts_an_ignored_range() {
        let xml = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated>\
            <Upload><Key>calls/a.bin</Key><UploadId>u-1</UploadId></Upload>\
            <Upload><Key>calls/b&amp;c</Key><UploadId>u-2</UploadId></Upload>\
            </ListMultipartUploadsResult>";
        let blocks = tag_blocks(xml, "Upload");
        assert_eq!(blocks.len(), 2);
        assert_eq!(tag_text(blocks[1], "Key").unwrap(), "calls/b&c");
        assert!(truncated(xml));
        assert_eq!(xml_escape("\"e&t<\""), "&quot;e&amp;t&lt;&quot;");
        let whole = b"0123456789".to_vec();
        let headers = hyper::HeaderMap::new();
        assert_eq!(
            range_of(200, &headers, whole.clone(), 2, 3),
            b"234".to_vec()
        );
        assert_eq!(
            range_of(206, &headers, b"234".to_vec(), 2, 3),
            b"234".to_vec()
        );
        assert_eq!(range_of(200, &headers, whole, 20, 3), Vec::<u8>::new());
    }

    #[test]
    fn refuses_a_non_http_endpoint() {
        let mut config = store(None).config().clone();
        config.endpoint = Some("ftp://x".into());
        assert!(S3ObjectStore::new(config).is_err());
    }

    #[test]
    fn classifies_failures_by_status() {
        let body = b"<Error><Code>AccessDenied</Code><Message>no</Message></Error>";
        assert_eq!(
            S3ObjectStore::failure("put", 403, body),
            ObjectStoreError::Rejected("put: HTTP 403 AccessDenied".into())
        );
        assert!(matches!(
            S3ObjectStore::failure("put", 503, b""),
            ObjectStoreError::Unavailable(_)
        ));
    }
}
