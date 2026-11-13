//! Password hashing for the `password hash` and `password verify` builtins.
//!
//! The hash is argon2id in PHC string form
//! (`$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`), with a fresh 16-byte salt drawn from the
//! OS RNG. The parameters are the OWASP minimum for argon2id and are fixed here, not
//! configured: a package cannot weaken them.
//!
//! `verify` reads its parameters from the stored hash. A planted hash asking for a huge memory
//! or time cost would otherwise pin an executor worker and its memory, so a hash whose
//! parameters exceed [`MAX_M_COST_KIB`], [`MAX_T_COST`] or [`MAX_P_COST`] is refused (`None`)
//! before any hashing work is done.
//!
//! FEEL evaluation is synchronous: one verify blocks its worker for about one hash time. That
//! is fine at sign-in rates; these functions are not meant for per-message use.
//!
//! `hash` reads OS randomness, so it is on the determinism denylist. `verify` is pure.

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};

/// Memory cost of a new hash, in KiB.
const M_COST_KIB: u32 = 19_456;
/// Iterations of a new hash.
const T_COST: u32 = 2;
/// Lanes of a new hash.
const P_COST: u32 = 1;
/// Salt length of a new hash, in bytes.
const SALT_LEN: usize = 16;

/// Highest memory cost `verify` accepts from a stored hash, in KiB.
const MAX_M_COST_KIB: u32 = 65_536;
/// Highest iteration count `verify` accepts from a stored hash.
const MAX_T_COST: u32 = 10;
/// Highest lane count `verify` accepts from a stored hash.
const MAX_P_COST: u32 = 4;

/// Hashes `pw` with argon2id and a fresh OS-random salt. `None` for an empty password.
pub(crate) fn hash(pw: &str) -> Option<String> {
    if pw.is_empty() {
        return None;
    }
    let mut salt_bytes = [0u8; SALT_LEN];
    getrandom::getrandom(&mut salt_bytes).expect("OS RNG unavailable");
    hash_with_salt(pw, &salt_bytes)
}

/// Hashes `pw` with the fixed parameters and the given salt.
fn hash_with_salt(pw: &str, salt_bytes: &[u8]) -> Option<String> {
    let salt = SaltString::encode_b64(salt_bytes).ok()?;
    let params = Params::new(M_COST_KIB, T_COST, P_COST, None).ok()?;
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password(pw.as_bytes(), &salt)
        .ok()
        .map(|h| h.to_string())
}

/// Checks `pw` against the PHC string `phc`, in constant time. `Some(true)` on a match,
/// `Some(false)` on a mismatch, `None` when `phc` is malformed, is not argon2id, or asks for
/// parameters above the cap.
pub(crate) fn verify(pw: &str, phc: &str) -> Option<bool> {
    let parsed = PasswordHash::new(phc).ok()?;
    if parsed.algorithm != argon2::ARGON2ID_IDENT {
        return None;
    }
    // Without a salt and an output there is nothing to compare: malformed, not a mismatch.
    if parsed.salt.is_none() || parsed.hash.is_none() {
        return None;
    }
    let params = Params::try_from(&parsed).ok()?;
    if params.m_cost() > MAX_M_COST_KIB
        || params.t_cost() > MAX_T_COST
        || params.p_cost() > MAX_P_COST
    {
        return None;
    }
    match Argon2::default().verify_password(pw.as_bytes(), &parsed) {
        Ok(()) => Some(true),
        Err(argon2::password_hash::Error::Password) => Some(false),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed argon2id vector for the password `correct horse`, salt bytes `0..16`, with the
    /// fixed parameters. It pins the output format: a later dependency bump that changes the
    /// encoding fails here. The output was cross-checked against OpenSSL 3.5's `ARGON2ID` KDF
    /// (`openssl kdf -keylen 32 ... -kdfopt iter:2 -kdfopt memcost:19456 -kdfopt lanes:1`).
    const REFERENCE_PASSWORD: &str = "correct horse";
    const REFERENCE_PHC: &str = "$argon2id$v=19$m=19456,t=2,p=1$AAECAwQFBgcICQoLDA0ODw$\
                                 0AuQxA+RjPiJeA19Op7DwrFxnfh9xeTU2T6mFj2wThc";

    #[test]
    fn password_hash_then_verify_is_true() {
        let phc = hash("s3cret-pass").expect("hash");
        assert!(phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"), "{phc}");
        assert_eq!(verify("s3cret-pass", &phc), Some(true));
    }

    #[test]
    fn password_verify_wrong_password_is_false() {
        let phc = hash("s3cret-pass").expect("hash");
        assert_eq!(verify("not-the-pass", &phc), Some(false));
    }

    #[test]
    fn password_verify_accepts_reference_phc_vector() {
        let salt: Vec<u8> = (0u8..16).collect();
        assert_eq!(
            hash_with_salt(REFERENCE_PASSWORD, &salt).as_deref(),
            Some(REFERENCE_PHC)
        );
        assert_eq!(verify(REFERENCE_PASSWORD, REFERENCE_PHC), Some(true));
        assert_eq!(verify("correct horsf", REFERENCE_PHC), Some(false));
    }

    #[test]
    fn password_verify_malformed_hash_is_null() {
        assert_eq!(verify("pw", ""), None);
        assert_eq!(verify("pw", "not a phc string"), None);
        // Another algorithm, well-formed.
        let argon2i = REFERENCE_PHC.replacen("argon2id", "argon2i", 1);
        assert_eq!(verify(REFERENCE_PASSWORD, &argon2i), None);
        let bcrypt_like = "$2b$12$abcdefghijklmnopqrstuuABCDEFGHIJKLMNOPQRSTUVWXYZ01234";
        assert_eq!(verify("pw", bcrypt_like), None);
        // No output part.
        assert_eq!(
            verify(
                "pw",
                "$argon2id$v=19$m=19456,t=2,p=1$AAECAwQFBgcICQoLDA0ODw"
            ),
            None
        );
    }

    #[test]
    fn password_verify_oversized_parameters_is_null() {
        let tail = REFERENCE_PHC.rsplit_once("p=1").expect("params").1;
        for params in [
            "m=65537,t=2,p=1",
            "m=19456,t=11,p=1",
            "m=19456,t=2,p=5",
            "m=4194304,t=2,p=1",
        ] {
            let phc = format!("$argon2id$v=19${params}{tail}");
            assert_eq!(verify(REFERENCE_PASSWORD, &phc), None, "{params}");
        }
    }

    #[test]
    fn password_hash_is_salted() {
        let a = hash("same-pass").expect("hash");
        let b = hash("same-pass").expect("hash");
        assert_ne!(a, b);
        assert_eq!(verify("same-pass", &a), Some(true));
        assert_eq!(verify("same-pass", &b), Some(true));
    }
}
