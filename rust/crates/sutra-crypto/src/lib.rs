//! Domain-neutral encryption-at-rest seam.
//!
//! This crate knows nothing about tenants, deployments, or payload shapes. It exposes two
//! traits:
//!
//! - [`PayloadCipher`] — authenticated encryption (AES-256-GCM) of an opaque byte payload. The
//!   caller composes the Additional Authenticated Data (AAD), which MUST bind
//!   the ciphertext to its context (`keyId` + `instance_id` + variable name) so a ciphertext can
//!   never be swapped between tenants, instances, or fields.
//! - [`KeyProvider`] — resolves the tenant Data Encryption Key (DEK) for a migration-stable
//!   `keyId`. `keyId` is deliberately NOT `deployment_id`: a version migration changes
//!   `deployment_id` but keeps `keyId` stable, so a v1 ciphertext must still decrypt under v2
//!   with no re-encryption.
//!
//! [`Aes256GcmCipher`] and [`HkdfKeyProvider`] are the concrete envelope-encryption
//! implementations. Both fail closed: any decrypt failure (bad tag, wrong AAD, wrong key,
//! truncated input) is a [`CipherError::Decrypt`], never a panic.
//!
//! It also exposes [`Sensitive<T>`] — the compile-time debug-leak backstop — as a shared
//! leaf type so every layer that carries a raw body (executor emissions, channel messages, the
//! persisted outbox row) can mask it without pulling a heavier dependency graph.

#![forbid(unsafe_code)]

pub mod sensitive;
pub use sensitive::Sensitive;

use std::collections::HashMap;
use std::fmt;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

type HmacSha256 = Hmac<Sha256>;

/// The HKDF `info` suffix that derives the blind-index key from the SAME `(master, keyId)` material
/// as the DEK but under a DISTINCT label — so the index key is cryptographically independent of the
/// DEK yet equally migration-stable. The unit separator can't occur in a keyId.
const BLIND_INDEX_LABEL: &[u8] = b"\x1fblind-index";

/// Authenticated encryption of an at-rest value. AAD binds the ciphertext to its context
/// (keyId + instance_id + variable-name) — the CALLER composes the AAD; this trait just uses it.
pub trait PayloadCipher {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, CipherError>;
    fn decrypt(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, CipherError>;
}

/// Supplies the per-tenant key material selected by a migration-stable `key_id`: the DEK for
/// encryption at rest and the blind-index key for GDPR discoverability, both derived from the same
/// material under distinct labels.
pub trait KeyProvider {
    /// The Data Encryption Key for `key_id` (encryption at rest).
    fn data_key(&self, key_id: &str) -> Result<DataKey, CipherError>;
    /// The blind-index key for `key_id` (GDPR discoverability) — independent of the DEK.
    fn blind_index_key(&self, key_id: &str) -> Result<BlindIndexer, CipherError>;
}

/// A 256-bit DEK plus the migration-stable id it was selected by. Zeroizes its material on drop.
pub struct DataKey {
    key_id: String,
    material: Zeroizing<[u8; 32]>,
}

impl DataKey {
    /// Builds a `DataKey` from raw material. The caller's `material` is copied into a zeroizing
    /// buffer; callers holding key bytes in a non-zeroizing buffer are responsible for scrubbing
    /// their own copy once this returns.
    pub fn new(key_id: impl Into<String>, material: [u8; 32]) -> Self {
        Self {
            key_id: key_id.into(),
            material: Zeroizing::new(material),
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn material(&self) -> &[u8; 32] {
        &self.material
    }
}

#[derive(Debug)]
pub enum CipherError {
    Encrypt(String),
    Decrypt(String),
    KeyDerivation(String),
}

impl fmt::Display for CipherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CipherError::Encrypt(msg) => write!(f, "encryption failed: {msg}"),
            CipherError::Decrypt(msg) => write!(f, "decryption failed: {msg}"),
            CipherError::KeyDerivation(msg) => write!(f, "key derivation failed: {msg}"),
        }
    }
}

impl std::error::Error for CipherError {}

/// 96-bit GCM nonce length, per NIST SP 800-38D (the recommended/only size `aes-gcm` optimizes).
const NONCE_LEN: usize = 12;

/// AES-256-GCM [`PayloadCipher`]. Output layout: `nonce (12 bytes) || ciphertext_with_tag`. A
/// fresh random nonce is generated per `encrypt` call via `OsRng` — safe because the nonce is
/// never reused for a given key (each `encrypt` draws a new one), and it travels with the
/// ciphertext so `decrypt` never needs it supplied out-of-band.
pub struct Aes256GcmCipher {
    cipher: Aes256Gcm,
}

impl Aes256GcmCipher {
    pub fn new(key: &DataKey) -> Self {
        // A 32-byte slice is always a valid AES-256 key; this can never fail.
        let cipher = Aes256Gcm::new_from_slice(key.material())
            .expect("DataKey material is always exactly 32 bytes");
        Self { cipher }
    }
}

impl PayloadCipher for Aes256GcmCipher {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, CipherError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let body = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| CipherError::Encrypt(e.to_string()))?;

        let mut out = Vec::with_capacity(NONCE_LEN + body.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&body);
        Ok(out)
    }

    fn decrypt(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, CipherError> {
        if ciphertext.len() < NONCE_LEN {
            return Err(CipherError::Decrypt(
                "ciphertext shorter than the nonce prefix".to_string(),
            ));
        }
        let (nonce_bytes, body) = ciphertext.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, Payload { msg: body, aad })
            .map_err(|e| CipherError::Decrypt(e.to_string()))
    }
}

/// Derives deterministic, migration-stable DEKs from a tenant master secret via HKDF-SHA256
/// (RFC 5869), using `key_id` as the HKDF `info` parameter and an empty salt. Same
/// `(master, key_id)` always yields the same 32-byte key — no key storage is needed, and a v1
/// ciphertext decrypts under v2 as long as both share the tenant's `key_id`.
pub struct HkdfKeyProvider {
    master: Zeroizing<Vec<u8>>,
}

impl HkdfKeyProvider {
    pub fn new(master: &[u8]) -> Self {
        Self {
            master: Zeroizing::new(master.to_vec()),
        }
    }
}

impl KeyProvider for HkdfKeyProvider {
    fn data_key(&self, key_id: &str) -> Result<DataKey, CipherError> {
        let hk = Hkdf::<Sha256>::new(None, &self.master);
        let mut material = [0u8; 32];
        hk.expand(key_id.as_bytes(), &mut material)
            .map_err(|e| CipherError::KeyDerivation(e.to_string()))?;

        let key = DataKey::new(key_id, material);
        // `[u8; 32]` is `Copy`, so the buffer above still exists on the stack after being copied
        // into the DataKey's zeroizing storage; scrub it explicitly rather than rely on drop.
        material.zeroize();
        Ok(key)
    }

    /// Derive the per-`keyId` **blind-index key**: HKDF from the same master as the DEK
    /// but under a distinct label ([`BLIND_INDEX_LABEL`]), so it is independent of the DEK yet equally
    /// migration-stable — the same `keyId` yields the same index across a module's versions, so GDPR
    /// enumeration spans the whole version history.
    fn blind_index_key(&self, key_id: &str) -> Result<BlindIndexer, CipherError> {
        let hk = Hkdf::<Sha256>::new(None, &self.master);
        let mut info = key_id.as_bytes().to_vec();
        info.extend_from_slice(BLIND_INDEX_LABEL);
        let mut key = [0u8; 32];
        hk.expand(&info, &mut key)
            .map_err(|e| CipherError::KeyDerivation(e.to_string()))?;
        let indexer = BlindIndexer {
            key: Zeroizing::new(key),
        };
        key.zeroize();
        info.zeroize();
        Ok(indexer)
    }
}

/// HKDF `info` used by [`Kek::from_secret`] — domain-separates "derive a KEK from this secret"
/// from any other derivation callers might run over the same raw secret elsewhere.
const KEK_FROM_SECRET_LABEL: &[u8] = b"sutra-crypto-kek";

/// A 256-bit Key-Encryption Key (KEK) — wraps/unwraps [`DataKey`]s for envelope
/// encryption. This crate implements only the WRAP/UNWRAP mechanism; sourcing the KEK itself
/// from a KMS or the env-reference machinery (`sutra-envref-*`) and persisting the resulting
/// [`WrappedDataKey`]s are separate wiring steps done by the caller. Zeroizes its material on
/// drop.
pub struct Kek {
    material: Zeroizing<[u8; 32]>,
}

impl Kek {
    /// Builds a `Kek` directly from 32 bytes of key material.
    pub fn new(material: [u8; 32]) -> Self {
        Self {
            material: Zeroizing::new(material),
        }
    }

    /// Derives a `Kek` from an arbitrary-length secret (e.g. a KMS-sourced or env-reference
    /// secret) via HKDF-SHA256, so a secret of any length can serve as a KEK. Deterministic: the
    /// same `secret` always yields the same `Kek`, so a KEK re-derived from the same source
    /// (e.g. across replicas, or a v1/v2 migration) can unwrap the other's wrapped DEKs.
    pub fn from_secret(secret: &[u8]) -> Kek {
        let hk = Hkdf::<Sha256>::new(None, secret);
        let mut material = [0u8; 32];
        hk.expand(KEK_FROM_SECRET_LABEL, &mut material)
            // 32 bytes is far under HKDF-SHA256's ~8KB max output; this can never fail.
            .expect("32-byte HKDF-SHA256 expand always succeeds");
        let kek = Kek::new(material);
        material.zeroize();
        kek
    }

    /// Wraps `dek`'s 32-byte material under this KEK via AES-256-GCM, with AAD = `dek.key_id()`'s
    /// bytes — binding the wrapped blob to its `keyId` so it can never be unwrapped while
    /// claiming a different one. Output layout matches [`Aes256GcmCipher`]:
    /// `nonce (12 bytes) || ciphertext_with_tag`.
    pub fn wrap(&self, dek: &DataKey) -> Result<WrappedDataKey, CipherError> {
        let cipher = Aes256Gcm::new_from_slice(self.material.as_ref())
            .expect("Kek material is always exactly 32 bytes");
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let body = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: dek.material().as_ref(),
                    aad: dek.key_id().as_bytes(),
                },
            )
            .map_err(|e| CipherError::Encrypt(e.to_string()))?;

        let mut wrapped = Vec::with_capacity(NONCE_LEN + body.len());
        wrapped.extend_from_slice(nonce.as_slice());
        wrapped.extend_from_slice(&body);

        Ok(WrappedDataKey {
            key_id: dek.key_id().to_string(),
            wrapped,
        })
    }

    /// Unwraps `wrapped` back into a [`DataKey`]. Fails closed (`CipherError::Decrypt`) on a
    /// wrong KEK, a tampered blob, or a `key_id` that doesn't match the one it was wrapped under —
    /// the AAD binding in [`Kek::wrap`] turns any such mismatch into a GCM authentication failure,
    /// never a silent success under the wrong identity.
    pub fn unwrap(&self, wrapped: &WrappedDataKey) -> Result<DataKey, CipherError> {
        if wrapped.wrapped.len() < NONCE_LEN {
            return Err(CipherError::Decrypt(
                "wrapped key shorter than the nonce prefix".to_string(),
            ));
        }
        let cipher = Aes256Gcm::new_from_slice(self.material.as_ref())
            .expect("Kek material is always exactly 32 bytes");
        let (nonce_bytes, body) = wrapped.wrapped.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);

        let mut material_vec = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: body,
                    aad: wrapped.key_id.as_bytes(),
                },
            )
            .map_err(|e| CipherError::Decrypt(e.to_string()))?;

        if material_vec.len() != 32 {
            material_vec.zeroize();
            return Err(CipherError::Decrypt(
                "unwrapped DEK material is not 32 bytes".to_string(),
            ));
        }
        let mut material = [0u8; 32];
        material.copy_from_slice(&material_vec);
        material_vec.zeroize();

        let dek = DataKey::new(wrapped.key_id.clone(), material);
        material.zeroize();
        Ok(dek)
    }

    /// Mints a fresh wrapped DEK for `key_id` from raw 32-byte material — a convenience over
    /// `wrap(&DataKey::new(key_id, dek_material))` for callers provisioning a brand-new DEK (e.g.
    /// at tenant onboarding) rather than rewrapping one that already exists.
    pub fn wrap_fresh_dek(
        &self,
        key_id: &str,
        dek_material: [u8; 32],
    ) -> Result<WrappedDataKey, CipherError> {
        self.wrap(&DataKey::new(key_id, dek_material))
    }

    /// Mints a fresh wrapped DEK for `key_id` from freshly-generated random 32-byte material
    /// (`OsRng`) — the common provisioning path (tenant onboarding via `sutra crypto provision-dek`),
    /// where the caller wants a NEW random DEK rather than supplying one. The raw material is
    /// zeroized before return; only the wrapped (ciphertext) form leaves this function.
    pub fn wrap_random_dek(&self, key_id: &str) -> Result<WrappedDataKey, CipherError> {
        use aes_gcm::aead::rand_core::RngCore;
        let mut material = [0u8; 32];
        let mut rng = OsRng;
        rng.fill_bytes(&mut material);
        let wrapped = self.wrap_fresh_dek(key_id, material);
        material.zeroize();
        wrapped
    }
}

/// A [`DataKey`] wrapped (encrypted) under a [`Kek`] — safe to store, log, or transmit, since it
/// is ciphertext, never raw key material. Layout: `nonce (12 bytes) || ciphertext_with_tag`
/// (envelope encryption).
#[derive(Debug, Clone)]
pub struct WrappedDataKey {
    key_id: String,
    wrapped: Vec<u8>,
}

impl WrappedDataKey {
    /// Builds a `WrappedDataKey` from its already-wrapped parts, e.g. when reading one back out
    /// of storage. No validation happens here; a bad blob simply fails later at [`Kek::unwrap`].
    pub fn from_parts(key_id: impl Into<String>, wrapped: Vec<u8>) -> Self {
        Self {
            key_id: key_id.into(),
            wrapped,
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.wrapped
    }
}

/// A [`KeyProvider`] that resolves DEKs by unwrapping pre-wrapped [`WrappedDataKey`]s under a
/// [`Kek`] (envelope encryption) — a drop-in alternative to [`HkdfKeyProvider`] for
/// callers whose KEK is sourced from a KMS / env-reference rather than derived directly from a
/// flat master secret. This crate holds only the wrap/unwrap mechanism and an in-memory lookup;
/// sourcing the KEK and persisting the `key_id -> WrappedDataKey` map are separate wiring steps.
pub struct EnvelopeKeyProvider {
    kek: Kek,
    wrapped: HashMap<String, WrappedDataKey>,
}

impl EnvelopeKeyProvider {
    pub fn new(kek: Kek, wrapped: HashMap<String, WrappedDataKey>) -> Self {
        Self { kek, wrapped }
    }
}

impl KeyProvider for EnvelopeKeyProvider {
    /// Unwraps the [`WrappedDataKey`] registered for `key_id`. Errs (`CipherError::KeyDerivation`)
    /// if no wrapped DEK is registered for `key_id`; propagates `Kek::unwrap`'s fail-closed
    /// `CipherError::Decrypt` for a bad KEK or a tampered/mismatched blob.
    fn data_key(&self, key_id: &str) -> Result<DataKey, CipherError> {
        let wrapped = self.wrapped.get(key_id).ok_or_else(|| {
            CipherError::KeyDerivation(format!("no wrapped DEK registered for key_id {key_id}"))
        })?;
        self.kek.unwrap(wrapped)
    }

    /// Derives the blind-index key from the UNWRAPPED DEK material (not the KEK) under the same
    /// distinct label [`HkdfKeyProvider::blind_index_key`] uses ([`BLIND_INDEX_LABEL`]) — mirroring
    /// that derivation, but rooted in the per-`key_id` DEK material rather than a flat master
    /// secret, since an envelope provider has no single flat secret to derive from.
    fn blind_index_key(&self, key_id: &str) -> Result<BlindIndexer, CipherError> {
        let dek = self.data_key(key_id)?;
        let hk = Hkdf::<Sha256>::new(None, dek.material());
        let mut info = key_id.as_bytes().to_vec();
        info.extend_from_slice(BLIND_INDEX_LABEL);
        let mut key = [0u8; 32];
        hk.expand(&info, &mut key)
            .map_err(|e| CipherError::KeyDerivation(e.to_string()))?;
        let indexer = BlindIndexer {
            key: Zeroizing::new(key),
        };
        key.zeroize();
        info.zeroize();
        Ok(indexer)
    }
}

/// A per-tenant keyed hasher producing GDPR **blind indexes** — `HMAC-SHA256(indexKey, value)` — so a
/// `subjectKey` value (e.g. a `customerId`) can be enumerated for disclosure/erasure with NO
/// cleartext PII stored. Built via [`HkdfKeyProvider::blind_index_key`]. Migration-
/// stable: the same `keyId` hashes the same value identically across module versions.
pub struct BlindIndexer {
    key: Zeroizing<[u8; 32]>,
}

impl BlindIndexer {
    /// The blind index of `value`: lowercase hex of `HMAC-SHA256(indexKey, normalize(value))`.
    /// `normalize` trims surrounding whitespace so trivially-different spellings of the same subject
    /// value collide (and thus enumerate together) as intended.
    pub fn blind(&self, value: &str) -> String {
        // Fully-qualified: `KeyInit` (aes-gcm) is also in scope and also defines `new_from_slice`.
        let mut mac = <HmacSha256 as Mac>::new_from_slice(self.key.as_ref())
            .expect("HMAC-SHA256 accepts a key of any length");
        mac.update(value.trim().as_bytes());
        hex_lower(&mac.finalize().into_bytes())
    }
}

/// Lowercase-hex encode — the on-disk form of a blind index (fixed 64 chars for SHA-256).
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher_for(key_id: &str) -> Aes256GcmCipher {
        let provider = HkdfKeyProvider::new(b"unit-test-master-secret");
        let key = provider.data_key(key_id).expect("derivation succeeds");
        Aes256GcmCipher::new(&key)
    }

    #[test]
    fn round_trip_with_same_aad() {
        let cipher = cipher_for("tenant-a");
        let aad = b"tenant-a|instance-1|balance";
        let ciphertext = cipher.encrypt(b"secret payload", aad).expect("encrypt");
        let plaintext = cipher.decrypt(&ciphertext, aad).expect("decrypt");
        assert_eq!(plaintext, b"secret payload");
    }

    #[test]
    fn wrong_aad_fails_closed() {
        let cipher = cipher_for("tenant-a");
        let ciphertext = cipher
            .encrypt(b"secret payload", b"tenant-a|instance-1|balance")
            .expect("encrypt");

        let err = cipher
            .decrypt(&ciphertext, b"tenant-a|instance-2|balance")
            .expect_err("wrong aad must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn tampered_ciphertext_fails_closed() {
        let cipher = cipher_for("tenant-a");
        let aad = b"tenant-a|instance-1|balance";
        let mut ciphertext = cipher.encrypt(b"secret payload", aad).expect("encrypt");

        // Flip a byte in the ciphertext body (past the 12-byte nonce prefix).
        let last = ciphertext.len() - 1;
        ciphertext[last] ^= 0xFF;

        let err = cipher
            .decrypt(&ciphertext, aad)
            .expect_err("tampered ciphertext must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn wrong_key_fails_closed() {
        let provider = HkdfKeyProvider::new(b"unit-test-master-secret");
        let key_a = provider.data_key("tenant-a").expect("derive a");
        let key_b = provider.data_key("tenant-b").expect("derive b");

        let cipher_a = Aes256GcmCipher::new(&key_a);
        let cipher_b = Aes256GcmCipher::new(&key_b);

        let aad = b"shared-aad";
        let ciphertext = cipher_a.encrypt(b"secret payload", aad).expect("encrypt");

        let err = cipher_b
            .decrypt(&ciphertext, aad)
            .expect_err("wrong key must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn derivation_is_deterministic_and_migration_stable() {
        let provider_v1 = HkdfKeyProvider::new(b"tenant-master-secret");
        let provider_v2 = HkdfKeyProvider::new(b"tenant-master-secret");

        // Same (master, key_id) derived independently (simulating a v1 deployment and a v2
        // deployment that share the tenant's keyId) must produce interchangeable keys: a
        // ciphertext produced under one decrypts under the other.
        let key_v1 = provider_v1.data_key("tenant-a").expect("derive v1");
        let key_v2 = provider_v2.data_key("tenant-a").expect("derive v2");

        let cipher_v1 = Aes256GcmCipher::new(&key_v1);
        let cipher_v2 = Aes256GcmCipher::new(&key_v2);

        let aad = b"tenant-a|instance-1|balance";
        let ciphertext = cipher_v1
            .encrypt(b"v1 snapshot payload", aad)
            .expect("encrypt under v1-derived key");
        let plaintext = cipher_v2
            .decrypt(&ciphertext, aad)
            .expect("decrypt under independently re-derived same-keyId key");
        assert_eq!(plaintext, b"v1 snapshot payload");

        // A different key_id must NOT be able to decrypt the other's ciphertext.
        let key_other = provider_v1.data_key("tenant-b").expect("derive other");
        let cipher_other = Aes256GcmCipher::new(&key_other);
        let err = cipher_other
            .decrypt(&ciphertext, aad)
            .expect_err("different keyId must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn nonce_is_unique_per_encrypt_call() {
        let cipher = cipher_for("tenant-a");
        let aad = b"tenant-a|instance-1|balance";
        let first = cipher.encrypt(b"same plaintext", aad).expect("encrypt 1");
        let second = cipher.encrypt(b"same plaintext", aad).expect("encrypt 2");
        assert_ne!(first, second, "random nonce must vary encrypt output");
        // And both must still independently decrypt to the same plaintext.
        assert_eq!(cipher.decrypt(&first, aad).unwrap(), b"same plaintext");
        assert_eq!(cipher.decrypt(&second, aad).unwrap(), b"same plaintext");
    }

    #[test]
    fn blind_index_is_deterministic_normalized_and_migration_stable() {
        // Migration-stable: two independently-built providers (same master) derive the same index
        // key for the same keyId, so the same value hashes identically across module versions.
        let indexer_v1 = HkdfKeyProvider::new(b"tenant-master-secret")
            .blind_index_key("tenant-a")
            .expect("derive v1 index key");
        let indexer_v2 = HkdfKeyProvider::new(b"tenant-master-secret")
            .blind_index_key("tenant-a")
            .expect("derive v2 index key");
        assert_eq!(indexer_v1.blind("cust-42"), indexer_v2.blind("cust-42"));

        // Deterministic + 64-char lowercase hex (SHA-256).
        let b = indexer_v1.blind("cust-42");
        assert_eq!(b.len(), 64);
        assert!(b
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_eq!(b, indexer_v1.blind("cust-42"));

        // normalize() trims surrounding whitespace so spellings collide (enumerate together).
        assert_eq!(indexer_v1.blind("  cust-42 "), indexer_v1.blind("cust-42"));
        // Different values differ.
        assert_ne!(indexer_v1.blind("cust-42"), indexer_v1.blind("cust-43"));
    }

    #[test]
    fn blind_index_is_tenant_isolated_and_independent_of_the_dek() {
        let provider = HkdfKeyProvider::new(b"tenant-master-secret");
        // A different keyId yields a different index (cross-tenant enumeration is impossible).
        let a = provider
            .blind_index_key("tenant-a")
            .unwrap()
            .blind("cust-42");
        let b = provider
            .blind_index_key("tenant-b")
            .unwrap()
            .blind("cust-42");
        assert_ne!(a, b, "index must be tenant-scoped");

        // Independent of the DEK: the blind-index key material is not the DEK material (distinct
        // HKDF label), so hashing the raw DEK bytes as a value does not reproduce anything keyed.
        let dek = provider.data_key("tenant-a").unwrap();
        let dek_hex = hex_lower(dek.material());
        assert_ne!(a, dek_hex);
    }

    // -- KEK-wrap envelope primitive ----------------------------------------------------------

    /// `Result::expect_err`/`unwrap_err` require `T: Debug`; `DataKey` deliberately does NOT
    /// derive `Debug` (that would print its raw key material). This helper extracts the `Err`
    /// side of a `Result<DataKey, _>` without that bound.
    fn expect_err<T>(result: Result<T, CipherError>, msg: &str) -> CipherError {
        match result {
            Ok(_) => panic!("{msg}"),
            Err(e) => e,
        }
    }

    #[test]
    fn wrap_unwrap_round_trips_the_dek() {
        let kek = Kek::new([7u8; 32]);
        let dek = DataKey::new("tenant-a", [42u8; 32]);

        let wrapped = kek.wrap(&dek).expect("wrap");
        let unwrapped = kek.unwrap(&wrapped).expect("unwrap");

        assert_eq!(unwrapped.key_id(), dek.key_id());
        assert_eq!(unwrapped.material(), dek.material());

        // A ciphertext produced under the ORIGINAL DEK must decrypt under the UNWRAPPED one.
        let aad = b"tenant-a|instance-1|balance";
        let original_cipher = Aes256GcmCipher::new(&dek);
        let unwrapped_cipher = Aes256GcmCipher::new(&unwrapped);
        let ciphertext = original_cipher
            .encrypt(b"secret payload", aad)
            .expect("encrypt under original dek");
        let plaintext = unwrapped_cipher
            .decrypt(&ciphertext, aad)
            .expect("decrypt under unwrapped dek");
        assert_eq!(plaintext, b"secret payload");
    }

    #[test]
    fn wrap_random_dek_mints_distinct_unwrappable_keys() {
        let kek = Kek::new([9u8; 32]);

        let a = kek.wrap_random_dek("tenant-a").expect("wrap random a");
        let b = kek.wrap_random_dek("tenant-a").expect("wrap random b");
        assert_eq!(a.key_id(), "tenant-a");
        // Each provisioning mints a fresh random DEK — distinct wrapped bytes.
        assert_ne!(
            a.as_bytes(),
            b.as_bytes(),
            "each provisioning is a fresh random DEK"
        );

        // The wrapped DEK unwraps back under the same KEK to usable, non-zero 32-byte material.
        let dek = kek.unwrap(&a).expect("unwrap");
        assert_eq!(dek.key_id(), "tenant-a");
        assert_eq!(dek.material().len(), 32);
        assert!(
            dek.material().iter().any(|&byte| byte != 0),
            "random DEK material is not all-zero"
        );
    }

    #[test]
    fn unwrap_with_a_different_kek_fails_closed() {
        let kek_a = Kek::new([1u8; 32]);
        let kek_b = Kek::new([2u8; 32]);
        let dek = DataKey::new("tenant-a", [42u8; 32]);

        let wrapped = kek_a.wrap(&dek).expect("wrap under kek_a");
        let err = expect_err(kek_b.unwrap(&wrapped), "wrong kek must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn unwrap_of_a_tampered_blob_fails_closed() {
        let kek = Kek::new([9u8; 32]);
        let dek = DataKey::new("tenant-a", [42u8; 32]);
        let wrapped = kek.wrap(&dek).expect("wrap");

        let mut tampered_bytes = wrapped.as_bytes().to_vec();
        let last = tampered_bytes.len() - 1;
        tampered_bytes[last] ^= 0xFF;
        let tampered = WrappedDataKey::from_parts(wrapped.key_id().to_string(), tampered_bytes);

        let err = expect_err(
            kek.unwrap(&tampered),
            "tampered wrapped blob must fail closed",
        );
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn unwrap_with_a_mismatched_key_id_fails_closed() {
        // AAD binds the wrapped blob to the key_id it was wrapped under; claiming a
        // different key_id at unwrap time must fail the GCM tag check, never silently succeed
        // under the wrong identity.
        let kek = Kek::new([3u8; 32]);
        let dek = DataKey::new("A", [42u8; 32]);
        let wrapped = kek.wrap(&dek).expect("wrap under key_id A");

        let relabeled = WrappedDataKey::from_parts("B", wrapped.as_bytes().to_vec());
        let err = expect_err(kek.unwrap(&relabeled), "mismatched key_id must fail closed");
        assert!(matches!(err, CipherError::Decrypt(_)));
    }

    #[test]
    fn envelope_key_provider_data_key_unwraps_the_registered_dek() {
        let kek = Kek::new([11u8; 32]);
        let wrapped = kek
            .wrap_fresh_dek("tenant-a", [5u8; 32])
            .expect("mint wrapped dek");

        let mut registry = HashMap::new();
        registry.insert("tenant-a".to_string(), wrapped);
        let provider = EnvelopeKeyProvider::new(kek, registry);

        let dek = provider.data_key("tenant-a").expect("resolve dek");
        assert_eq!(dek.key_id(), "tenant-a");
        assert_eq!(dek.material(), &[5u8; 32]);
    }

    #[test]
    fn envelope_key_provider_unknown_key_id_errs() {
        let kek = Kek::new([12u8; 32]);
        let provider = EnvelopeKeyProvider::new(kek, HashMap::new());

        let err = expect_err(
            provider.data_key("no-such-tenant"),
            "unknown key_id must error",
        );
        assert!(matches!(err, CipherError::KeyDerivation(_)));
    }

    #[test]
    fn envelope_key_provider_blind_index_key_is_derived_and_independent_of_the_dek() {
        let kek = Kek::new([13u8; 32]);
        let wrapped = kek
            .wrap_fresh_dek("tenant-a", [6u8; 32])
            .expect("mint wrapped dek");
        let mut registry = HashMap::new();
        registry.insert("tenant-a".to_string(), wrapped);
        let provider = EnvelopeKeyProvider::new(kek, registry);

        let indexer = provider
            .blind_index_key("tenant-a")
            .expect("derive blind index key");
        let blind = indexer.blind("cust-42");
        assert_eq!(blind.len(), 64);

        // Deterministic given the same registered wrapped DEK.
        let blind_again = provider
            .blind_index_key("tenant-a")
            .expect("re-derive blind index key")
            .blind("cust-42");
        assert_eq!(blind, blind_again);

        // Independent of the DEK material itself.
        let dek_hex = hex_lower(&[6u8; 32]);
        assert_ne!(blind, dek_hex);
    }

    #[test]
    fn envelope_key_provider_and_hkdf_key_provider_are_interchangeable_as_dyn_key_provider() {
        // Compile-level check: both concrete providers satisfy the same trait object type.
        let kek = Kek::new([21u8; 32]);
        let wrapped = kek
            .wrap_fresh_dek("tenant-a", [8u8; 32])
            .expect("mint wrapped dek");
        let mut registry = HashMap::new();
        registry.insert("tenant-a".to_string(), wrapped);

        let providers: Vec<Box<dyn KeyProvider>> = vec![
            Box::new(EnvelopeKeyProvider::new(kek, registry)),
            Box::new(HkdfKeyProvider::new(b"unit-test-master-secret")),
        ];

        // Round-trip through the trait object for the envelope provider (index 0): resolve the
        // DEK via `dyn KeyProvider`, then use it like any other `KeyProvider`-sourced DEK.
        let dek = providers[0].data_key("tenant-a").expect("resolve via dyn");
        assert_eq!(dek.material(), &[8u8; 32]);

        let cipher = Aes256GcmCipher::new(&dek);
        let aad = b"tenant-a|instance-1|balance";
        let ciphertext = cipher.encrypt(b"payload", aad).expect("encrypt");
        assert_eq!(
            cipher.decrypt(&ciphertext, aad).expect("decrypt"),
            b"payload"
        );
    }

    #[test]
    fn kek_from_secret_is_deterministic_and_interchangeable() {
        let kek_1 = Kek::from_secret(b"kms-envref-secret-value");
        let kek_2 = Kek::from_secret(b"kms-envref-secret-value");

        let dek = DataKey::new("tenant-a", [17u8; 32]);
        let wrapped = kek_1.wrap(&dek).expect("wrap under kek_1");

        // The independently-derived kek_2 (same secret) must unwrap what kek_1 wrapped.
        let unwrapped = kek_2
            .unwrap(&wrapped)
            .expect("kek derived from the same secret must unwrap");
        assert_eq!(unwrapped.material(), dek.material());

        // A different secret derives a different (non-interchangeable) KEK.
        let kek_other = Kek::from_secret(b"a-different-secret-value");
        let err = expect_err(
            kek_other.unwrap(&wrapped),
            "kek derived from a different secret must fail closed",
        );
        assert!(matches!(err, CipherError::Decrypt(_)));
    }
}
