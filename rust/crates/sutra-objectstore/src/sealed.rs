//! The sealed-object format: an object encrypted at rest with envelope encryption.
//!
//! Each object gets a fresh random data key (AES-256-GCM), wrapped under the deployment's
//! key-encryption key ([`sutra_crypto::Kek`]); the wrapped key travels inside the object, so one
//! KEK opens every recording and rotating it means re-wrapping, not re-encrypting.
//!
//! Layout:
//!
//! ```text
//! "SUTRA-SEALED-1\n"            magic, 15 bytes
//! u32 big-endian                length of the header
//! header                        JSON: {keyId, wrappedKey (base64), contentType, cipher}
//! ciphertext                    nonce (12) || AES-256-GCM(plaintext) || tag (16)
//! ```
//!
//! The AAD binds the ciphertext to its key id and to the object key it was sealed for, so a
//! sealed object copied under another key does not open.

use base64::Engine as _;
use serde_json::{json, Value};
use sutra_crypto::{Aes256GcmCipher, Kek, PayloadCipher, WrappedDataKey};

/// The format's magic prefix.
pub const MAGIC: &[u8] = b"SUTRA-SEALED-1\n";
/// The cipher named in the header.
pub const CIPHER: &str = "aes-256-gcm";
/// The content type a sealed object is stored with.
pub const SEALED_CONTENT_TYPE: &str = "application/vnd.sutra.sealed";
/// The extension appended to a sealed object's key.
pub const SEALED_EXTENSION: &str = ".sealed";

/// The header of a sealed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedHeader {
    pub key_id: String,
    pub content_type: String,
}

fn aad(key_id: &str, object_key: &str) -> Vec<u8> {
    format!("sutra-sealed-object\x1f{key_id}\x1f{object_key}").into_bytes()
}

/// Seal `plaintext` for storage under `object_key`.
pub fn seal(
    kek: &Kek,
    key_id: &str,
    object_key: &str,
    content_type: &str,
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    let wrapped = kek
        .wrap_random_dek(key_id)
        .map_err(|e| format!("wrapping a data key: {e}"))?;
    let data_key = kek
        .unwrap(&wrapped)
        .map_err(|e| format!("unwrapping the data key: {e}"))?;
    let ciphertext = Aes256GcmCipher::new(&data_key)
        .encrypt(plaintext, &aad(key_id, object_key))
        .map_err(|e| e.to_string())?;
    let header = json!({
        "keyId": key_id,
        "wrappedKey": base64::engine::general_purpose::STANDARD.encode(wrapped.as_bytes()),
        "contentType": content_type,
        "cipher": CIPHER,
    })
    .to_string();
    let mut out = Vec::with_capacity(MAGIC.len() + 4 + header.len() + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Read a sealed object's header without opening it.
pub fn header(sealed: &[u8]) -> Result<SealedHeader, String> {
    parts(sealed).map(|(header, _, _)| header)
}

fn parts(sealed: &[u8]) -> Result<(SealedHeader, Vec<u8>, &[u8]), String> {
    let rest = sealed
        .strip_prefix(MAGIC)
        .ok_or("not a sealed object (no magic)")?;
    let len_bytes: [u8; 4] = rest
        .get(..4)
        .and_then(|b| b.try_into().ok())
        .ok_or("truncated sealed object")?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let header_bytes = rest.get(4..4 + len).ok_or("truncated sealed header")?;
    let value: Value =
        serde_json::from_slice(header_bytes).map_err(|e| format!("sealed header: {e}"))?;
    if value["cipher"].as_str() != Some(CIPHER) {
        return Err(format!("unknown cipher {}", value["cipher"]));
    }
    let text = |key: &str| {
        value[key]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("sealed header has no {key}"))
    };
    let wrapped = base64::engine::general_purpose::STANDARD
        .decode(text("wrappedKey")?)
        .map_err(|e| format!("wrapped key: {e}"))?;
    Ok((
        SealedHeader {
            key_id: text("keyId")?,
            content_type: text("contentType")?,
        },
        wrapped,
        &rest[4 + len..],
    ))
}

/// Open a sealed object stored under `object_key`.
pub fn open(kek: &Kek, object_key: &str, sealed: &[u8]) -> Result<(SealedHeader, Vec<u8>), String> {
    let (header, wrapped, ciphertext) = parts(sealed)?;
    let data_key = kek
        .unwrap(&WrappedDataKey::from_parts(header.key_id.clone(), wrapped))
        .map_err(|e| e.to_string())?;
    let plaintext = Aes256GcmCipher::new(&data_key)
        .decrypt(ciphertext, &aad(&header.key_id, object_key))
        .map_err(|e| e.to_string())?;
    Ok((header, plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_binds_the_object_key() {
        let kek = Kek::from_secret(b"a test key-encryption secret");
        let sealed = seal(
            &kek,
            "recordings",
            "calls/c1.wav.sealed",
            "audio/wav",
            b"RIFF....",
        )
        .unwrap();
        assert!(sealed.starts_with(MAGIC));
        assert!(
            !sealed.windows(8).any(|w| w == b"RIFF...."),
            "the plaintext is not visible"
        );
        assert_eq!(header(&sealed).unwrap().content_type, "audio/wav");
        let (h, plain) = open(&kek, "calls/c1.wav.sealed", &sealed).unwrap();
        assert_eq!(
            (h.key_id.as_str(), plain.as_slice()),
            ("recordings", &b"RIFF...."[..])
        );
        assert!(
            open(&kek, "calls/other.wav.sealed", &sealed).is_err(),
            "moved object"
        );
        assert!(open(
            &Kek::from_secret(b"another"),
            "calls/c1.wav.sealed",
            &sealed
        )
        .is_err());
        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(open(&kek, "calls/c1.wav.sealed", &tampered).is_err());
        assert!(open(&kek, "x", b"plain").is_err());
    }
}
