//! AWS Signature Version 4 for S3 requests (header-based signing, the `s3` service).
//!
//! Only what an object put / get / bucket create needs: every header passed is signed, the
//! payload hash is the body's SHA-256 (or `UNSIGNED-PAYLOAD` when the caller says so), and the
//! canonical URI keeps `/` and encodes everything outside RFC 3986's unreserved set once.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::hex;

type HmacSha256 = Hmac<Sha256>;

/// The algorithm name in the `Authorization` header.
pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// Credentials for signing.
#[derive(Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    /// A temporary credential's session token (`x-amz-security-token`).
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// One request to sign.
pub struct SigningRequest<'a> {
    pub method: &'a str,
    /// The path, unencoded (`/bucket/key`); it is encoded here.
    pub path: &'a str,
    /// Query parameters, unencoded.
    pub query: &'a [(&'a str, &'a str)],
    /// Every header to sign (name, value) — must include `host`, `x-amz-date` and
    /// `x-amz-content-sha256`.
    pub headers: &'a [(String, String)],
    /// The value of `x-amz-content-sha256`.
    pub payload_hash: &'a str,
    pub region: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
}

/// Percent-encode `value` per SigV4 (unreserved characters pass; `/` passes when `keep_slash`).
pub fn uri_encode(value: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// The canonical request (exposed for tests).
pub fn canonical_request(request: &SigningRequest<'_>) -> (String, String) {
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    headers.sort();
    let signed = headers
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let mut query: Vec<(String, String)> = request
        .query
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_headers: String = headers.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        uri_encode(request.path, true),
        query,
        canonical_headers,
        signed,
        request.payload_hash
    );
    (canonical, signed)
}

/// The `Authorization` header value for `request`.
pub fn authorization(request: &SigningRequest<'_>, credentials: &Credentials) -> String {
    let (canonical, signed) = canonical_request(request);
    let date = &request.amz_date[..8.min(request.amz_date.len())];
    let scope = format!("{date}/{}/s3/aws4_request", request.region);
    let string_to_sign = format!(
        "{ALGORITHM}\n{}\n{scope}\n{}",
        request.amz_date,
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let k_date = hmac(
        format!("AWS4{}", credentials.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(&k_date, request.region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex(&hmac(&k_signing, string_to_sign.as_bytes()));
    format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
        credentials.access_key
    )
}

/// `YYYYMMDDTHHMMSSZ` for `at`.
pub fn amz_date(at: time::OffsetDateTime) -> String {
    let at = at.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        at.year(),
        u8::from(at.month()),
        at.day(),
        at.hour(),
        at.minute(),
        at.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The "GET Object" example of the S3 Signature Version 4 documentation.
    #[test]
    fn matches_the_documented_get_object_example() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = vec![
            (
                "Host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("Range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), empty.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let request = SigningRequest {
            method: "GET",
            path: "/test.txt",
            query: &[],
            headers: &headers,
            payload_hash: empty,
            region: "us-east-1",
            amz_date: "20130524T000000Z",
        };
        assert_eq!(
            authorization(&request, &credentials),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn encodes_paths_once_and_keeps_slashes() {
        assert_eq!(uri_encode("/b/a key+x.wav", true), "/b/a%20key%2Bx.wav");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
    }

    #[test]
    fn formats_the_date() {
        let at = time::macros::datetime!(2026-10-25 09:08:07 UTC);
        assert_eq!(amz_date(at), "20261025T090807Z");
    }
}
