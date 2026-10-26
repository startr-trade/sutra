//! Named object stores: the stores the engine configuration declares
//! (`sutra.object-stores.<name>.*`), installed once at boot and looked up by name (an `llm`
//! channel's `llm.media-store`) or by the URI a media reference carries (erasure and retention
//! deleting a recording).
//!
//! A [`NamedStore`] holds the store, its location, and — for sealed objects — the
//! key-encryption key the objects were sealed under, so a reader gets the plaintext back after
//! the stored bytes have been checked against the reference's SHA-256.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use sutra_crypto::Kek;

use crate::sigv4::Credentials;
use crate::{
    sealed, sha256_hex, valid_key, MemoryObjectStore, ObjectStore, ObjectStoreError, S3Config,
    S3ObjectStore, StoreLocation,
};

/// How one named store is reached, with its secrets already resolved.
#[derive(Clone, PartialEq, Eq)]
pub struct StoreSettings {
    pub name: String,
    /// `s3://<bucket>[/<prefix>]` or `memory://<bucket>[/<prefix>]`.
    pub location: StoreLocation,
    /// A self-hosted or non-AWS store's base URL; `None` for AWS itself.
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    /// The key-encryption secret sealed objects were sealed under; `None` = objects are stored
    /// as is, and a sealed object read from this store is refused.
    pub kek_secret: Option<String>,
    /// The key id sealed objects name.
    pub key_id: String,
}

impl fmt::Debug for StoreSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreSettings")
            .field("name", &self.name)
            .field("location", &self.location)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("credentials", &self.access_key.as_ref().map(|_| "<set>"))
            .field("sealed", &self.kek_secret.is_some())
            .field("key_id", &self.key_id)
            .finish()
    }
}

/// An object addressed by URI: `<scheme>://<bucket>/<full key>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub scheme: String,
    pub bucket: String,
    /// The full object key (the store's prefix included).
    pub key: String,
}

impl ObjectRef {
    /// Parse an object URI; the key must be a usable object key (no `..`, no empty segment).
    pub fn parse(uri: &str) -> Result<ObjectRef, String> {
        let location = StoreLocation::parse(uri).map_err(|e| e.to_string())?;
        if location.prefix.is_empty() || !valid_key(&location.prefix) {
            return Err(format!("'{uri}' does not name an object"));
        }
        Ok(ObjectRef {
            scheme: location.scheme,
            bucket: location.bucket,
            key: location.prefix,
        })
    }

    pub fn uri(&self) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.key)
    }
}

/// Why a media reference could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaRefError {
    /// Worth retrying: the store could not be reached.
    Unavailable(String),
    /// Not worth retrying: no such object, outside the store, over the size limit, a digest
    /// that does not match, or a sealed object that does not open.
    Refused(String),
}

impl fmt::Display for MediaRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaRefError::Unavailable(m) | MediaRefError::Refused(m) => f.write_str(m),
        }
    }
}

/// One configured store.
pub struct NamedStore {
    name: String,
    location: StoreLocation,
    store: Arc<dyn ObjectStore>,
    kek: Option<Kek>,
    key_id: String,
}

impl fmt::Debug for NamedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedStore")
            .field("name", &self.name)
            .field("location", &self.location)
            .field("sealed", &self.kek.is_some())
            .finish()
    }
}

impl NamedStore {
    /// Wrap an already-built store.
    pub fn new(
        name: &str,
        store: Arc<dyn ObjectStore>,
        location: StoreLocation,
        kek_secret: Option<&str>,
        key_id: &str,
    ) -> NamedStore {
        NamedStore {
            name: name.to_string(),
            location,
            store,
            kek: kek_secret.map(|s| Kek::from_secret(s.as_bytes())),
            key_id: key_id.to_string(),
        }
    }

    /// Build the store `settings` describe (`memory://` in process, `s3://` over HTTP(S)).
    pub fn open(settings: &StoreSettings) -> Result<NamedStore, ObjectStoreError> {
        let store: Arc<dyn ObjectStore> = match settings.location.scheme.as_str() {
            "memory" => Arc::new(MemoryObjectStore::new(settings.location.clone())),
            _ => {
                let (Some(access_key), Some(secret_key)) =
                    (settings.access_key.clone(), settings.secret_key.clone())
                else {
                    return Err(ObjectStoreError::Config(format!(
                        "object store '{}': an s3:// store needs an access key and a secret key",
                        settings.name
                    )));
                };
                let mut config = S3Config::new(
                    settings.location.clone(),
                    &settings.region,
                    Credentials {
                        access_key,
                        secret_key,
                        session_token: None,
                    },
                );
                config.endpoint = settings.endpoint.clone();
                Arc::new(S3ObjectStore::new(config)?)
            }
        };
        Ok(NamedStore::new(
            &settings.name,
            store,
            settings.location.clone(),
            settings.kek_secret.as_deref(),
            &settings.key_id,
        ))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn location(&self) -> &StoreLocation {
        &self.location
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Whether objects sealed for this store can be opened.
    pub fn sealed(&self) -> bool {
        self.kek.is_some()
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The key relative to the store's prefix, when `reference` lies inside this store (same
    /// scheme, same bucket, under the prefix); `None` otherwise.
    pub fn relative_key(&self, reference: &ObjectRef) -> Option<String> {
        if reference.scheme != self.location.scheme || reference.bucket != self.location.bucket {
            return None;
        }
        if self.location.prefix.is_empty() {
            return Some(reference.key.clone());
        }
        reference
            .key
            .strip_prefix(&self.location.prefix)
            .and_then(|rest| rest.strip_prefix('/'))
            .filter(|rest| !rest.is_empty())
            .map(str::to_string)
    }

    /// Read `reference`, check the stored bytes against `sha256` (lower- or upper-case hex) and
    /// `max_bytes`, and return the content: opened when the object is sealed, as stored
    /// otherwise.
    pub async fn read(
        &self,
        reference: &ObjectRef,
        sha256: &str,
        max_bytes: u64,
    ) -> Result<Vec<u8>, MediaRefError> {
        let uri = reference.uri();
        let Some(key) = self.relative_key(reference) else {
            return Err(MediaRefError::Refused(format!(
                "'{uri}' is not in object store '{}'",
                self.name
            )));
        };
        let stored = match self.store.get(&key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Err(MediaRefError::Refused(format!("'{uri}' does not exist"))),
            Err(ObjectStoreError::Unavailable(m)) => {
                return Err(MediaRefError::Unavailable(format!("reading '{uri}': {m}")))
            }
            Err(e) => return Err(MediaRefError::Refused(format!("reading '{uri}': {e}"))),
        };
        if stored.len() as u64 > max_bytes {
            return Err(MediaRefError::Refused(format!(
                "'{uri}' is {} bytes, over the limit of {max_bytes}",
                stored.len()
            )));
        }
        if !sha256_hex(&stored).eq_ignore_ascii_case(sha256.trim()) {
            return Err(MediaRefError::Refused(format!(
                "'{uri}' does not match its SHA-256"
            )));
        }
        if !stored.starts_with(sealed::MAGIC) {
            return Ok(stored);
        }
        let Some(kek) = &self.kek else {
            return Err(MediaRefError::Refused(format!(
                "'{uri}' is sealed and object store '{}' has no key to open it",
                self.name
            )));
        };
        sealed::open(kek, &reference.key, &stored)
            .map(|(_, plaintext)| plaintext)
            .map_err(|_| MediaRefError::Refused(format!("'{uri}' could not be opened")))
    }

    /// Delete `reference` (absent is fine).
    pub async fn delete(&self, reference: &ObjectRef) -> Result<(), ObjectStoreError> {
        let key = self.relative_key(reference).ok_or_else(|| {
            ObjectStoreError::Config(format!(
                "'{}' is not in object store '{}'",
                reference.uri(),
                self.name
            ))
        })?;
        self.store.delete(&key).await
    }
}

type Stores = BTreeMap<String, Arc<NamedStore>>;

fn installed() -> &'static RwLock<Arc<Stores>> {
    static STORES: std::sync::OnceLock<RwLock<Arc<Stores>>> = std::sync::OnceLock::new();
    STORES.get_or_init(|| RwLock::new(Arc::new(BTreeMap::new())))
}

/// Install the configured stores (boot), replacing any earlier set.
pub fn install(stores: Vec<NamedStore>) {
    let map = stores
        .into_iter()
        .map(|s| (s.name.clone(), Arc::new(s)))
        .collect();
    *installed().write().unwrap_or_else(|e| e.into_inner()) = Arc::new(map);
}

/// Add (or replace) one store, keeping the others.
pub fn register(store: NamedStore) {
    let mut guard = installed().write().unwrap_or_else(|e| e.into_inner());
    let mut map = (**guard).clone();
    map.insert(store.name.clone(), Arc::new(store));
    *guard = Arc::new(map);
}

/// The store configured under `name`.
pub fn named(name: &str) -> Option<Arc<NamedStore>> {
    installed()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(name)
        .cloned()
}

/// The configured store names.
pub fn names() -> Vec<String> {
    installed()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect()
}

/// The configured store `uri` lies in — the one with the longest matching prefix — and the
/// parsed reference.
pub fn for_uri(uri: &str) -> Option<(Arc<NamedStore>, ObjectRef)> {
    let reference = ObjectRef::parse(uri).ok()?;
    let stores = Arc::clone(&installed().read().unwrap_or_else(|e| e.into_inner()));
    stores
        .values()
        .filter(|s| s.relative_key(&reference).is_some())
        .max_by_key(|s| s.location.prefix.len())
        .cloned()
        .map(|s| (s, reference))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory(name: &str, uri: &str, kek: Option<&str>) -> (NamedStore, MemoryObjectStore) {
        let location = StoreLocation::parse(uri).unwrap();
        let raw = MemoryObjectStore::new(location.clone());
        (
            NamedStore::new(name, Arc::new(raw.clone()), location, kek, "media"),
            raw,
        )
    }

    #[test]
    fn references_parse_and_lie_inside_their_store_only() {
        let (store, _) = memory("rec", "memory://registry-rec/calls", None);
        let inside = ObjectRef::parse("memory://registry-rec/calls/a/b.wav").unwrap();
        assert_eq!(store.relative_key(&inside).as_deref(), Some("a/b.wav"));
        for outside in [
            "memory://registry-rec/other/b.wav",
            "memory://another-bucket/calls/b.wav",
            "s3://registry-rec/calls/b.wav",
            "memory://registry-rec/callsx/b.wav",
        ] {
            let r = ObjectRef::parse(outside).unwrap();
            assert_eq!(store.relative_key(&r), None, "{outside}");
        }
        assert!(ObjectRef::parse("memory://registry-rec").is_err());
        assert!(ObjectRef::parse("memory://registry-rec/a/../b").is_err());
    }

    #[tokio::test]
    async fn reads_check_the_digest_the_limit_and_open_sealed_objects() {
        let (store, raw) = memory(
            "sealed",
            "memory://registry-sealed",
            Some("a test key secret"),
        );
        let kek = Kek::from_secret(b"a test key secret");
        let body = sealed::seal(&kek, "media", "c1.wav.sealed", "audio/wav", b"RIFFdata").unwrap();
        let stored = raw.put("c1.wav.sealed", body, "x").await.unwrap();
        let reference = ObjectRef::parse(&stored.uri).unwrap();
        assert_eq!(
            store
                .read(&reference, &stored.sha256, 1 << 20)
                .await
                .unwrap(),
            b"RIFFdata"
        );
        assert!(matches!(
            store.read(&reference, &"0".repeat(64), 1 << 20).await,
            Err(MediaRefError::Refused(m)) if m.contains("SHA-256")
        ));
        assert!(matches!(
            store.read(&reference, &stored.sha256, 8).await,
            Err(MediaRefError::Refused(m)) if m.contains("over the limit")
        ));
        let missing = ObjectRef::parse("memory://registry-sealed/none.wav").unwrap();
        assert!(matches!(
            store.read(&missing, &stored.sha256, 1 << 20).await,
            Err(MediaRefError::Refused(m)) if m.contains("does not exist")
        ));
        // Without the key the sealed object is refused, never returned sealed.
        let (keyless, keyless_raw) = memory("keyless", "memory://registry-keyless", None);
        let body = sealed::seal(&kek, "media", "c2.wav.sealed", "audio/wav", b"x").unwrap();
        let stored = keyless_raw.put("c2.wav.sealed", body, "x").await.unwrap();
        let reference = ObjectRef::parse(&stored.uri).unwrap();
        assert!(keyless
            .read(&reference, &stored.sha256, 1 << 20)
            .await
            .is_err());
        // Delete is idempotent.
        keyless.delete(&reference).await.unwrap();
        keyless.delete(&reference).await.unwrap();
        assert!(keyless_raw.keys().is_empty());
    }

    #[test]
    fn the_registry_resolves_by_name_and_by_uri() {
        let (outer, _) = memory("registry-outer", "memory://registry-uri", None);
        let (inner, _) = memory("registry-inner", "memory://registry-uri/deep", None);
        register(outer);
        register(inner);
        assert!(named("registry-outer").is_some());
        let (store, reference) = for_uri("memory://registry-uri/deep/x.wav").unwrap();
        assert_eq!(store.name(), "registry-inner", "the longest prefix wins");
        assert_eq!(reference.key, "deep/x.wav");
        assert_eq!(
            for_uri("memory://registry-uri/x.wav").unwrap().0.name(),
            "registry-outer"
        );
        assert!(for_uri("memory://registry-unknown/x.wav").is_none());
    }
}
