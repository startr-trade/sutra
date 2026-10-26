//! A neutral object-store seam: where a transport puts media it records (a call's audio), and
//! where an operator or a test reads it back.
//!
//! - [`ObjectStore`] — put / get / delete of one object by key, inside one configured location,
//!   plus what a streamed, crash-recoverable write needs: multipart uploads (create, upload a
//!   part, complete, abort, list the uploads in progress and their parts), a ranged read, and a
//!   paginated listing of the objects under a prefix.
//! - [`S3ObjectStore`] — any S3-compatible store (a cloud bucket, or a self-hosted server
//!   addressed path-style), signed with AWS Signature Version 4 ([`sigv4`]).
//! - [`MemoryObjectStore`] — an in-process store for tests.
//! - [`registry`] — the named stores the engine configuration declares, which the `llm`
//!   transport reads media references from and erasure and retention delete them from.
//! - [`sealed`] — the at-rest format: the object encrypted with a fresh data key, the data key
//!   wrapped under a key-encryption key (sutra-crypto envelope encryption), both in one
//!   self-describing object.
//!
//! The object's SHA-256 is computed over the bytes stored, so anyone holding the reference can
//! check the stored object's integrity without the key.
#![forbid(unsafe_code)]

pub mod memory;
pub mod registry;
pub mod s3;
pub mod sealed;
pub mod sigv4;

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use sha2::{Digest, Sha256};

pub use memory::MemoryObjectStore;
pub use registry::{MediaRefError, NamedStore, ObjectRef, StoreSettings};
pub use s3::{S3Config, S3ObjectStore, S3_LIST_PAGE_SIZE};

/// A boxed, sendable future (the seam stays free of an async-trait dependency).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a successful put stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    /// `s3://<bucket>/<key>` (or `memory://<bucket>/<key>`).
    pub uri: String,
    /// The object key, including the store's prefix.
    pub key: String,
    /// Lower-case hex SHA-256 of the bytes stored.
    pub sha256: String,
    pub bytes: u64,
}

/// A failed object-store operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreError {
    /// Worth retrying: the store could not be reached, timed out, or answered 5xx / 429.
    Unavailable(String),
    /// Not worth retrying: refused (credentials, permissions, a bad request).
    Rejected(String),
    /// The configuration is unusable.
    Config(String),
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectStoreError::Unavailable(m) => write!(f, "object store unavailable: {m}"),
            ObjectStoreError::Rejected(m) => write!(f, "object store refused: {m}"),
            ObjectStoreError::Config(m) => write!(f, "object store configuration: {m}"),
        }
    }
}

impl std::error::Error for ObjectStoreError {}

/// One uploaded part of a multipart upload: its number (from 1), the ETag the store answered
/// for it, and its size. [`ObjectStore::complete_upload`] takes these in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    pub number: u32,
    pub etag: String,
    pub size: u64,
}

/// A multipart upload in progress, as [`ObjectStore::list_uploads`] finds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpload {
    /// The object key the upload will complete into, relative to the store's prefix.
    pub key: String,
    pub upload_id: String,
}

/// One stored object, as [`ObjectStore::list_objects`] finds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// The object key, relative to the store's prefix.
    pub key: String,
    pub bytes: u64,
}

/// The answer of an operation a store does not implement: a refusal, so a caller that needs it
/// fails plainly instead of silently doing less.
fn unsupported<'a, T: Send + 'a>(
    operation: &'static str,
) -> BoxFuture<'a, Result<T, ObjectStoreError>> {
    Box::pin(async move {
        Err(ObjectStoreError::Rejected(format!(
            "this object store does not support {operation}"
        )))
    })
}

/// One object-store location (a bucket, and optionally a key prefix inside it).
///
/// Every key a method takes or answers is relative to the store's prefix. The methods past
/// put / get / bucket creation have default implementations that refuse
/// ([`ObjectStoreError::Rejected`]), so an implementor that cannot offer them still compiles and
/// a caller learns at the call that the store lacks them.
///
/// Multipart uploads write one object in parts, so a long recording streams to the store while
/// it is captured instead of being held in memory: [`Self::create_upload`] opens one, each
/// [`Self::upload_part`] answers its part's ETag, and [`Self::complete_upload`] assembles the
/// parts in the order given (or [`Self::abort_upload`] discards them). On an S3-compatible store
/// every part but the last must be at least 5 MiB.
///
/// Finding uploads after a crash: [`Self::list_uploads`] lists the uploads in progress under a
/// key or prefix, but some S3-compatible stores answer it only for an exact object key, not a
/// prefix. A caller that must rediscover its uploads without knowing their keys should keep a
/// small marker object per upload (naming its key and upload id) under a prefix of its own, find
/// the markers with [`Self::list_objects`], which every store answers by prefix, and use
/// [`Self::list_parts`] or [`Self::list_uploads`] with the exact key from there.
pub trait ObjectStore: Send + Sync {
    /// Store `body` under `key` (relative to the store's prefix).
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<StoredObject, ObjectStoreError>>;

    /// Read the object at `key` (relative to the store's prefix); `None` when there is none.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>>;

    /// Delete the object at `key` (relative to the store's prefix). Deleting an object that is
    /// not there succeeds, so a repeated erasure is harmless. A store that cannot delete refuses.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        let _ = key;
        Box::pin(async {
            Err(ObjectStoreError::Rejected(
                "this object store does not support delete".to_string(),
            ))
        })
    }

    /// Create the bucket when it does not exist (a self-hosted store set up from scratch).
    fn ensure_bucket(&self) -> BoxFuture<'_, Result<(), ObjectStoreError>>;

    /// At most `len` bytes of the object at `key`, from byte `start`: `None` when there is no
    /// object, fewer bytes (or none) past its end.
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>> {
        let _ = (key, start, len);
        unsupported("a ranged get")
    }

    /// The objects whose key starts with `prefix`, sorted by key, across every page the store
    /// answers.
    fn list_objects<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ListedObject>, ObjectStoreError>> {
        let _ = prefix;
        unsupported("listing objects")
    }

    /// Start a multipart upload of `key`; answers its upload id.
    fn create_upload<'a>(
        &'a self,
        key: &'a str,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        let _ = (key, content_type);
        unsupported("multipart uploads")
    }

    /// Upload part `number` (from 1) of the upload `upload_id` of `key`; answers the part's
    /// ETag, which [`Self::complete_upload`] needs back. Uploading a number again replaces the
    /// part.
    fn upload_part<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        number: u32,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        let _ = (key, upload_id, number, body);
        unsupported("multipart uploads")
    }

    /// Assemble `parts`, in the order given, into the object at `key`, ending the upload.
    fn complete_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        parts: &'a [UploadedPart],
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        let _ = (key, upload_id, parts);
        unsupported("multipart uploads")
    }

    /// Discard the upload and the parts uploaded so far. Aborting an upload that is not there
    /// succeeds, so a repeated cleanup is harmless.
    fn abort_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        let _ = (key, upload_id);
        unsupported("multipart uploads")
    }

    /// The uploads in progress whose key starts with `key_or_prefix`, sorted by key then upload
    /// id. Some S3-compatible stores answer only for an exact key (see the trait's note on
    /// finding uploads after a crash).
    fn list_uploads<'a>(
        &'a self,
        key_or_prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PendingUpload>, ObjectStoreError>> {
        let _ = key_or_prefix;
        unsupported("multipart uploads")
    }

    /// The parts of the upload `upload_id` of `key` uploaded so far, by number.
    fn list_parts<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<UploadedPart>, ObjectStoreError>> {
        let _ = (key, upload_id);
        unsupported("multipart uploads")
    }

    /// The URI an object at `key` (relative to the prefix) is reported under.
    fn uri_of(&self, key: &str) -> String;

    /// The full object key `key` is stored under (the prefix joined with `key`).
    fn full_key(&self, key: &str) -> String;
}

/// An object-store location parsed from `s3://<bucket>[/<prefix>]` or `memory://<bucket>[/<prefix>]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    pub scheme: String,
    pub bucket: String,
    /// The key prefix, without leading or trailing `/` (empty for the bucket root).
    pub prefix: String,
}

impl StoreLocation {
    pub fn parse(uri: &str) -> Result<StoreLocation, ObjectStoreError> {
        let (scheme, rest) = uri.trim().split_once("://").ok_or_else(|| {
            ObjectStoreError::Config(format!("'{uri}' is not a <scheme>://<bucket> URI"))
        })?;
        let scheme = scheme.to_ascii_lowercase();
        if scheme != "s3" && scheme != "memory" {
            return Err(ObjectStoreError::Config(format!(
                "'{uri}': the scheme must be s3 or memory"
            )));
        }
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let valid_bucket = (3..=63).contains(&bucket.len())
            && bucket
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.');
        if !valid_bucket {
            return Err(ObjectStoreError::Config(format!(
                "'{uri}': '{bucket}' is not a bucket name (3-63 of a-z, 0-9, '-', '.')"
            )));
        }
        Ok(StoreLocation {
            scheme,
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
        })
    }

    /// The prefix joined with `key`.
    pub fn full_key(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    pub fn uri_of(&self, key: &str) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.full_key(key))
    }
}

/// `full` made relative to the store prefix `base` (the location's `full_key("")`, so `""` or
/// `"<prefix>/"`), when it is under it.
pub(crate) fn relative_key(base: &str, full: &str) -> Option<String> {
    full.strip_prefix(base).map(str::to_string)
}

/// A key the caller passed, checked once it is joined with the store's prefix.
pub(crate) fn checked_full_key(
    location: &StoreLocation,
    key: &str,
) -> Result<String, ObjectStoreError> {
    let full = location.full_key(key);
    if valid_key(&full) {
        Ok(full)
    } else {
        Err(ObjectStoreError::Config(format!(
            "'{full}' is not an object key"
        )))
    }
}

/// A listing prefix (relative to the store's prefix), checked: empty, or the characters a key
/// may carry with no `.` / `..` segment. A trailing `/` is allowed.
pub(crate) fn checked_list_prefix(prefix: &str) -> Result<(), ObjectStoreError> {
    let usable = prefix.is_empty()
        || (prefix.len() <= 900
            && !prefix.starts_with('/')
            && prefix.split('/').all(|s| s != "." && s != "..")
            && prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/')));
    if usable {
        Ok(())
    } else {
        Err(ObjectStoreError::Config(format!(
            "'{prefix}' is not a key prefix"
        )))
    }
}

/// Whether `key` is a usable object key: 1-900 bytes of `[A-Za-z0-9._/-]`, no empty, `.` or
/// `..` segment, no leading `/`.
pub fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 900
        && !key.starts_with('/')
        && key
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..")
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
}

/// Lower-case hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0F)] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locations_parse_and_join() {
        let l = StoreLocation::parse("s3://meeting-recordings/calls/").unwrap();
        assert_eq!(l.bucket, "meeting-recordings");
        assert_eq!(l.prefix, "calls");
        assert_eq!(l.full_key("a/b.wav"), "calls/a/b.wav");
        assert_eq!(l.uri_of("a/b.wav"), "s3://meeting-recordings/calls/a/b.wav");
        assert_eq!(StoreLocation::parse("s3://rec").unwrap().full_key("x"), "x");
        assert!(StoreLocation::parse("http://rec").is_err());
        assert!(StoreLocation::parse("s3://Bad_Bucket").is_err());
        assert!(StoreLocation::parse("rec").is_err());
    }

    #[test]
    fn keys_are_checked() {
        assert!(valid_key("meeting/2026/call-1.wav.sealed"));
        assert!(!valid_key(""));
        assert!(!valid_key("/abs"));
        assert!(!valid_key("a/../b"));
        assert!(!valid_key("a//b"));
        assert!(!valid_key("a b"));
    }

    #[test]
    fn sha256_is_lower_hex() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
