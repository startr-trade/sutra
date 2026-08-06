//! OS-entropy helpers for the two non-deterministic builtins (`uuid()`, `random()`).
//!
//! These builtins draw bytes from the OS RNG via `getrandom`. Both remain on the determinism
//! denylist — present but denied at replay-bound sites.

/// Random RFC 4122 version-4 UUID string.
pub(crate) fn uuid_v4() -> String {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).expect("OS RNG unavailable");
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    let hex = |r: &[u8]| r.iter().map(|x| format!("{x:02x}")).collect::<String>();
    format!(
        "{}-{}-{}-{}-{}",
        hex(&b[0..4]),
        hex(&b[4..6]),
        hex(&b[6..8]),
        hex(&b[8..10]),
        hex(&b[10..16])
    )
}

/// Uniform double in `[0, 1)` with 53 bits of precision.
pub(crate) fn random_unit_f64() -> f64 {
    let mut b = [0u8; 8];
    getrandom::getrandom(&mut b).expect("OS RNG unavailable");
    (u64::from_be_bytes(b) >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}
