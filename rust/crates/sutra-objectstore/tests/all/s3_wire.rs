//! The S3 client against an in-process HTTP endpoint: path-style addressing, a signed request
//! carrying its payload hash, put then get, a missing object, bucket creation and a refusal;
//! then, against a fuller fake that checks every request's signature (query string included),
//! multipart uploads, a ranged get, and paginated object and upload listings.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::collections::BTreeMap;

use sutra_objectstore::sigv4::{self, Credentials, SigningRequest};
use sutra_objectstore::{
    sha256_hex, ListedObject, ObjectStore, ObjectStoreError, PendingUpload, S3Config,
    S3ObjectStore, StoreLocation, UploadedPart,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type Seen = Arc<Mutex<Vec<(String, String, HashMap<String, String>)>>>;

/// A minimal store: PUT keeps the body, GET returns it (404 when absent), a request without an
/// AWS4 authorization or with a payload hash that does not match its body is refused (403).
async fn fake_store() -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let objects: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::default();
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let objects = Arc::clone(&objects);
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head_end, headers, method, path) = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buffer[..end]).to_string();
                        let mut lines = head.lines();
                        let first: Vec<String> = lines
                            .next()
                            .unwrap()
                            .split(' ')
                            .map(str::to_string)
                            .collect();
                        let headers: HashMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        break (end + 4, headers, first[0].clone(), first[1].clone());
                    }
                };
                let length: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while buffer.len() < head_end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    buffer.extend_from_slice(&chunk[..n]);
                }
                let body = buffer[head_end..head_end + length].to_vec();
                log.lock()
                    .unwrap()
                    .push((method.clone(), path.clone(), headers.clone()));
                let signed = headers
                    .get("authorization")
                    .is_some_and(|a| a.starts_with("AWS4-HMAC-SHA256 Credential=ak/"))
                    && headers.get("x-amz-content-sha256") == Some(&sha256_hex(&body));
                let (status, answer) = if !signed || path.contains("forbidden") {
                    (
                        "403 Forbidden",
                        b"<Error><Code>AccessDenied</Code></Error>".to_vec(),
                    )
                } else if method == "PUT" {
                    objects.lock().unwrap().insert(path, body);
                    ("200 OK", Vec::new())
                } else {
                    match objects.lock().unwrap().get(&path) {
                        Some(found) => ("200 OK", found.clone()),
                        None => (
                            "404 Not Found",
                            b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
                        ),
                    }
                };
                let reply = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    answer.len()
                );
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.write_all(&answer).await.unwrap();
            });
        }
    });
    (format!("http://{address}"), seen)
}

fn store(endpoint: &str, location: &str) -> S3ObjectStore {
    let mut config = S3Config::new(
        StoreLocation::parse(location).unwrap(),
        "us-east-1",
        Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
            session_token: None,
        },
    );
    config.endpoint = Some(endpoint.to_string());
    S3ObjectStore::new(config).unwrap()
}

#[tokio::test]
async fn puts_gets_and_creates_a_bucket_path_style() {
    let (endpoint, seen) = fake_store().await;
    let store = store(&endpoint, "s3://recordings/calls");
    store.ensure_bucket().await.unwrap();
    let stored = store
        .put("c1.wav", b"RIFF-audio".to_vec(), "audio/wav")
        .await
        .unwrap();
    assert_eq!(stored.uri, "s3://recordings/calls/c1.wav");
    assert_eq!(stored.key, "calls/c1.wav");
    assert_eq!(stored.sha256, sha256_hex(b"RIFF-audio"));
    assert_eq!(stored.bytes, 10);
    assert_eq!(
        store.get("c1.wav").await.unwrap().as_deref(),
        Some(&b"RIFF-audio"[..])
    );
    assert_eq!(store.get("missing.wav").await.unwrap(), None);
    let seen = seen.lock().unwrap();
    let paths: Vec<(&str, &str)> = seen
        .iter()
        .map(|(m, p, _)| (m.as_str(), p.as_str()))
        .collect();
    assert_eq!(
        paths,
        vec![
            ("PUT", "/recordings"),
            ("PUT", "/recordings/calls/c1.wav"),
            ("GET", "/recordings/calls/c1.wav"),
            ("GET", "/recordings/calls/missing.wav"),
        ]
    );
    let put = &seen[1].2;
    assert_eq!(
        put.get("content-type").map(String::as_str),
        Some("audio/wav")
    );
    assert!(put["authorization"]
        .contains("SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date"));
}

#[tokio::test]
async fn a_refusal_is_rejected_not_retried() {
    let (endpoint, _) = fake_store().await;
    let store = store(&endpoint, "s3://recordings");
    let error = store
        .put("forbidden.wav", b"x".to_vec(), "audio/wav")
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ObjectStoreError::Rejected("put: HTTP 403 AccessDenied".into())
    );
    assert!(matches!(
        store.put("../escape", b"x".to_vec(), "audio/wav").await,
        Err(ObjectStoreError::Config(_))
    ));
}

#[tokio::test]
async fn an_unreachable_store_is_unavailable() {
    let store = store("http://127.0.0.1:9", "s3://recordings");
    assert!(matches!(
        store.put("a.wav", b"x".to_vec(), "audio/wav").await,
        Err(ObjectStoreError::Unavailable(_))
    ));
}

// ---- the fuller fake ---------------------------------------------------------------------------

/// What the fuller fake holds: objects by full key, uploads by id (key, parts by number).
#[derive(Default)]
struct FakeState {
    objects: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, (String, BTreeMap<u32, Vec<u8>>)>,
    next_upload: u32,
    /// Every request's (method, path, query) as received.
    requests: Vec<(String, String, String)>,
}

type Fake = Arc<Mutex<FakeState>>;

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&text[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap()
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Whether `authorization` is the SigV4 signature of this request under the test credentials:
/// the fake recomputes it from the method, path, query, signed headers and payload hash.
fn signature_matches(
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &HashMap<String, String>,
) -> bool {
    let Some(authorization) = headers.get("authorization") else {
        return false;
    };
    let Some(signed) = authorization
        .split("SignedHeaders=")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
    else {
        return false;
    };
    let signed_headers: Vec<(String, String)> = signed
        .split(';')
        .map(|name| {
            (
                name.to_string(),
                headers.get(name).cloned().unwrap_or_default(),
            )
        })
        .collect();
    let query_refs: Vec<(&str, &str)> = query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let expected = sigv4::authorization(
        &SigningRequest {
            method,
            path: &percent_decode(path),
            query: &query_refs,
            headers: &signed_headers,
            payload_hash: headers
                .get("x-amz-content-sha256")
                .map(String::as_str)
                .unwrap_or(""),
            region: "us-east-1",
            amz_date: headers.get("x-amz-date").map(String::as_str).unwrap_or(""),
        },
        &Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
            session_token: None,
        },
    );
    &expected == authorization
}

fn etag_of(body: &[u8]) -> String {
    format!("\"{}\"", &sha256_hex(body)[..32])
}

/// Answer one request: (status line, extra headers, body).
fn answer(
    state: &mut FakeState,
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &HashMap<String, String>,
    body: Vec<u8>,
) -> (&'static str, Vec<(String, String)>, Vec<u8>) {
    let q = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let bucket_path = "/recordings";
    let key = path
        .strip_prefix("/recordings/")
        .map(percent_decode)
        .unwrap_or_default();
    let ok = |body: String| ("200 OK", Vec::new(), body.into_bytes());
    match (method, path == bucket_path) {
        ("GET", true) if q("list-type").as_deref() == Some("2") => {
            let prefix = q("prefix").unwrap_or_default();
            let max: usize = q("max-keys").and_then(|m| m.parse().ok()).unwrap_or(1000);
            let after = q("continuation-token").unwrap_or_default();
            let matching: Vec<(&String, &Vec<u8>)> = state
                .objects
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix) && k.as_str() > after.as_str())
                .collect();
            let page = &matching[..matching.len().min(max)];
            let more = matching.len() > max;
            let mut xml = format!("<ListBucketResult><IsTruncated>{more}</IsTruncated>");
            for (k, v) in page {
                xml.push_str(&format!(
                    "<Contents><Key>{k}</Key><Size>{}</Size></Contents>",
                    v.len()
                ));
            }
            if more {
                xml.push_str(&format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    page.last().unwrap().0
                ));
            }
            xml.push_str("</ListBucketResult>");
            ok(xml)
        }
        ("GET", true) if q("uploads").is_some() => {
            let prefix = q("prefix").unwrap_or_default();
            let max: usize = q("max-uploads")
                .and_then(|m| m.parse().ok())
                .unwrap_or(1000);
            let after = (
                q("key-marker").unwrap_or_default(),
                q("upload-id-marker").unwrap_or_default(),
            );
            let mut matching: Vec<(String, String)> = state
                .uploads
                .iter()
                .map(|(id, (k, _))| (k.clone(), id.clone()))
                .filter(|(k, id)| {
                    k.starts_with(&prefix) && (after.0.is_empty() || (k, id) > (&after.0, &after.1))
                })
                .collect();
            matching.sort();
            let page = &matching[..matching.len().min(max)];
            let more = matching.len() > max;
            let mut xml = format!("<ListMultipartUploadsResult><IsTruncated>{more}</IsTruncated>");
            for (k, id) in page {
                xml.push_str(&format!(
                    "<Upload><Key>{k}</Key><UploadId>{id}</UploadId></Upload>"
                ));
            }
            if let (true, Some((k, id))) = (more, page.last()) {
                xml.push_str(&format!(
                    "<NextKeyMarker>{k}</NextKeyMarker><NextUploadIdMarker>{id}</NextUploadIdMarker>"
                ));
            }
            xml.push_str("</ListMultipartUploadsResult>");
            ok(xml)
        }
        ("POST", false) if q("uploads").is_some() => {
            state.next_upload += 1;
            let id = format!("up-{}", state.next_upload);
            state.uploads.insert(id.clone(), (key, BTreeMap::new()));
            ok(format!(
                "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
            ))
        }
        ("PUT", false) if q("uploadId").is_some() => {
            let id = q("uploadId").unwrap();
            let number: u32 = q("partNumber").unwrap().parse().unwrap();
            match state.uploads.get_mut(&id) {
                Some((upload_key, parts)) if *upload_key == key => {
                    let etag = etag_of(&body);
                    parts.insert(number, body);
                    ("200 OK", vec![("etag".into(), etag)], Vec::new())
                }
                _ => (
                    "404 Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchUpload</Code></Error>".to_vec(),
                ),
            }
        }
        ("POST", false) if q("uploadId").is_some() => {
            let id = q("uploadId").unwrap();
            let Some((upload_key, parts)) = state.uploads.remove(&id) else {
                return (
                    "404 Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchUpload</Code></Error>".to_vec(),
                );
            };
            assert_eq!(upload_key, key);
            assert_eq!(
                headers.get("content-type").map(String::as_str),
                Some("application/xml")
            );
            let xml = String::from_utf8(body).unwrap();
            let mut assembled = Vec::new();
            for block in xml.split("<Part>").skip(1) {
                let number: u32 = block
                    .split("<PartNumber>")
                    .nth(1)
                    .and_then(|r| r.split("</PartNumber>").next())
                    .unwrap()
                    .parse()
                    .unwrap();
                let etag = block
                    .split("<ETag>")
                    .nth(1)
                    .and_then(|r| r.split("</ETag>").next())
                    .unwrap()
                    .replace("&quot;", "\"");
                let part = &parts[&number];
                if etag_of(part) != etag {
                    // A completion that fails after the store answered 200.
                    return ok("<Error><Code>InvalidPart</Code></Error>".into());
                }
                assembled.extend_from_slice(part);
            }
            state.objects.insert(key, assembled);
            ok("<CompleteMultipartUploadResult/>".into())
        }
        ("DELETE", false) if q("uploadId").is_some() => {
            state.uploads.remove(&q("uploadId").unwrap());
            ("204 No Content", Vec::new(), Vec::new())
        }
        ("GET", false) if q("uploadId").is_some() => {
            let id = q("uploadId").unwrap();
            let Some((_, parts)) = state.uploads.get(&id) else {
                return (
                    "404 Not Found",
                    Vec::new(),
                    b"<Error><Code>NoSuchUpload</Code></Error>".to_vec(),
                );
            };
            let mut xml = String::from("<ListPartsResult><IsTruncated>false</IsTruncated>");
            for (n, body) in parts {
                xml.push_str(&format!(
                    "<Part><PartNumber>{n}</PartNumber><ETag>{}</ETag><Size>{}</Size></Part>",
                    etag_of(body).replace('"', "&quot;"),
                    body.len()
                ));
            }
            xml.push_str("</ListPartsResult>");
            ok(xml)
        }
        ("PUT", false) => {
            state.objects.insert(key, body);
            ("200 OK", Vec::new(), Vec::new())
        }
        ("GET", false) => match state.objects.get(&key) {
            Some(found) => match headers.get("range") {
                Some(range) => {
                    let (from, to) = range
                        .trim_start_matches("bytes=")
                        .split_once('-')
                        .map(|(a, b)| (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap()))
                        .unwrap();
                    if from >= found.len() {
                        ("416 Range Not Satisfiable", Vec::new(), Vec::new())
                    } else {
                        let to = to.min(found.len() - 1);
                        (
                            "206 Partial Content",
                            vec![(
                                "content-range".into(),
                                format!("bytes {from}-{to}/{}", found.len()),
                            )],
                            found[from..=to].to_vec(),
                        )
                    }
                }
                None => ("200 OK", Vec::new(), found.clone()),
            },
            None => (
                "404 Not Found",
                Vec::new(),
                b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
            ),
        },
        _ => (
            "400 Bad Request",
            Vec::new(),
            b"<Error><Code>Unexpected</Code></Error>".to_vec(),
        ),
    }
}

/// An in-process S3 endpoint that checks each request's full SigV4 signature (403 otherwise)
/// and implements objects, ranges, ListObjectsV2 and multipart uploads, with pages as small as
/// the client asks for.
async fn fuller_fake() -> (String, Fake) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state: Fake = Arc::default();
    let shared = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let state = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 65536];
                let (head_end, headers, method, target) = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buffer[..end]).to_string();
                        let mut lines = head.lines();
                        let first: Vec<String> = lines
                            .next()
                            .unwrap()
                            .split(' ')
                            .map(str::to_string)
                            .collect();
                        let headers: HashMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        break (end + 4, headers, first[0].clone(), first[1].clone());
                    }
                };
                let length: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while buffer.len() < head_end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    buffer.extend_from_slice(&chunk[..n]);
                }
                let body = buffer[head_end..head_end + length].to_vec();
                let (path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
                let query = parse_query(raw_query);
                let (status, extra, reply_body) = {
                    let mut state = state.lock().unwrap();
                    state
                        .requests
                        .push((method.clone(), path.to_string(), raw_query.to_string()));
                    if !signature_matches(&method, path, &query, &headers)
                        || headers.get("x-amz-content-sha256") != Some(&sha256_hex(&body))
                    {
                        (
                            "403 Forbidden",
                            Vec::new(),
                            b"<Error><Code>SignatureDoesNotMatch</Code></Error>".to_vec(),
                        )
                    } else {
                        answer(&mut state, &method, path, &query, &headers, body)
                    }
                };
                let mut reply = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n",
                    reply_body.len()
                );
                for (name, value) in extra {
                    reply.push_str(&format!("{name}: {value}\r\n"));
                }
                reply.push_str("\r\n");
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.write_all(&reply_body).await.unwrap();
            });
        }
    });
    (format!("http://{address}"), state)
}

fn paged_store(endpoint: &str, page: u32) -> S3ObjectStore {
    let mut config = S3Config::new(
        StoreLocation::parse("s3://recordings/calls").unwrap(),
        "us-east-1",
        Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
            session_token: None,
        },
    );
    config.endpoint = Some(endpoint.to_string());
    config.list_page_size = page;
    S3ObjectStore::new(config).unwrap()
}

#[tokio::test]
async fn a_multipart_upload_completes_in_part_order_over_signed_requests() {
    let (endpoint, fake) = fuller_fake().await;
    let store = paged_store(&endpoint, 1000);
    let id = store
        .create_upload("call-1/rec.wav", "audio/wav")
        .await
        .unwrap();
    let second = store
        .upload_part("call-1/rec.wav", &id, 2, b"-second".to_vec())
        .await
        .unwrap();
    let first = store
        .upload_part("call-1/rec.wav", &id, 1, b"first".to_vec())
        .await
        .unwrap();
    assert_eq!(first, format!("\"{}\"", &sha256_hex(b"first")[..32]));
    let listed = store.list_parts("call-1/rec.wav", &id).await.unwrap();
    assert_eq!(
        listed,
        vec![
            UploadedPart {
                number: 1,
                etag: first.clone(),
                size: 5
            },
            UploadedPart {
                number: 2,
                etag: second.clone(),
                size: 7
            },
        ]
    );
    store
        .complete_upload("call-1/rec.wav", &id, &listed)
        .await
        .unwrap();
    assert_eq!(
        store.get("call-1/rec.wav").await.unwrap(),
        Some(b"first-second".to_vec())
    );
    // The requests a multipart write is made of, path-style, each one signed (the fake answers
    // 403 to a signature it cannot reproduce, query string included).
    let requests = fake.lock().unwrap().requests.clone();
    assert_eq!(requests[0].0, "POST");
    assert_eq!(requests[0].1, "/recordings/calls/call-1/rec.wav");
    assert_eq!(requests[0].2, "uploads=");
    assert!(requests[1].2.contains("partNumber=2") && requests[1].2.contains(&id));
}

#[tokio::test]
async fn a_completion_that_fails_after_a_200_is_refused_and_an_abort_discards() {
    let (endpoint, fake) = fuller_fake().await;
    let store = paged_store(&endpoint, 1000);
    let id = store
        .create_upload("x.bin", "application/octet-stream")
        .await
        .unwrap();
    store
        .upload_part("x.bin", &id, 1, b"x".to_vec())
        .await
        .unwrap();
    let wrong = [UploadedPart {
        number: 1,
        etag: "\"not-the-etag\"".into(),
        size: 1,
    }];
    assert_eq!(
        store.complete_upload("x.bin", &id, &wrong).await,
        Err(ObjectStoreError::Rejected(
            "complete upload: HTTP 400 InvalidPart".into()
        ))
    );
    let id = store
        .create_upload("y.bin", "application/octet-stream")
        .await
        .unwrap();
    store
        .upload_part("y.bin", &id, 1, b"y".to_vec())
        .await
        .unwrap();
    store.abort_upload("y.bin", &id).await.unwrap();
    assert!(fake.lock().unwrap().uploads.is_empty());
    // Aborting again is harmless.
    store.abort_upload("y.bin", &id).await.unwrap();
    assert!(matches!(
        store.upload_part("y.bin", &id, 0, b"y".to_vec()).await,
        Err(ObjectStoreError::Rejected(_))
    ));
}

#[tokio::test]
async fn listings_follow_every_page_and_answer_keys_relative_to_the_prefix() {
    let (endpoint, fake) = fuller_fake().await;
    let store = paged_store(&endpoint, 2);
    for name in [
        "a/1.bin", "a/2.bin", "a/3.bin", "a/4.bin", "a/5.bin", "b/1.bin",
    ] {
        store
            .put(name, name.as_bytes().to_vec(), "application/octet-stream")
            .await
            .unwrap();
    }
    let listed = store.list_objects("a/").await.unwrap();
    assert_eq!(
        listed.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        vec!["a/1.bin", "a/2.bin", "a/3.bin", "a/4.bin", "a/5.bin"]
    );
    assert_eq!(
        listed[0],
        ListedObject {
            key: "a/1.bin".into(),
            bytes: 7
        }
    );
    let pages = fake
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(m, _, q)| m == "GET" && q.contains("list-type=2"))
        .count();
    assert_eq!(pages, 3, "five keys at two per page");
    assert_eq!(store.list_objects("").await.unwrap().len(), 6);

    let mut ids = Vec::new();
    for name in ["u/1.bin", "u/2.bin", "u/3.bin"] {
        ids.push(store.create_upload(name, "x/y").await.unwrap());
    }
    store.create_upload("v/1.bin", "x/y").await.unwrap();
    let uploads = store.list_uploads("u/").await.unwrap();
    assert_eq!(
        uploads,
        vec![
            PendingUpload {
                key: "u/1.bin".into(),
                upload_id: ids[0].clone()
            },
            PendingUpload {
                key: "u/2.bin".into(),
                upload_id: ids[1].clone()
            },
            PendingUpload {
                key: "u/3.bin".into(),
                upload_id: ids[2].clone()
            },
        ]
    );
    assert_eq!(store.list_uploads("u/2.bin").await.unwrap().len(), 1);
    assert!(matches!(
        store.list_objects("../x").await,
        Err(ObjectStoreError::Config(_))
    ));
}

#[tokio::test]
async fn a_ranged_get_answers_the_range_and_nothing_past_the_end() {
    let (endpoint, _) = fuller_fake().await;
    let store = paged_store(&endpoint, 1000);
    store
        .put("r.bin", b"0123456789".to_vec(), "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(
        store.get_range("r.bin", 2, 3).await.unwrap(),
        Some(b"234".to_vec())
    );
    assert_eq!(
        store.get_range("r.bin", 8, 100).await.unwrap(),
        Some(b"89".to_vec())
    );
    assert_eq!(
        store.get_range("r.bin", 10, 4).await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        store.get_range("r.bin", 0, 0).await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(store.get_range("missing.bin", 0, 4).await.unwrap(), None);
}
