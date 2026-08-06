//! DECIMAL64 arithmetic — 16 significant digits, `HALF_EVEN` rounding.
//!
//! The `bigdecimal` crate's own operators compute exact sums/differences/products but its
//! division uses a fixed default precision with different rounding, so every arithmetic site
//! in the evaluator routes through this module instead:
//!
//! - [`add`] / [`sub`] / [`mul`]: exact result, then rounded to 16 significant digits with
//!   `HALF_EVEN` **only when the exact result exceeds 16 digits** — DECIMAL64 add/subtract/
//!   multiply semantics (no trailing-zero stripping, scale preserved when no rounding occurs).
//! - [`div`]: DECIMAL64 division semantics — a terminating
//!   quotient representable in ≤ 16 significant digits is returned exactly with its scale
//!   adjusted toward the preferred scale (`dividend.scale − divisor.scale`) by stripping
//!   trailing zeros; a non-terminating (or longer) quotient is rounded to 16 significant
//!   digits `HALF_EVEN`, with an exact sticky bit taken from the division remainder.
//!
//! Documented divergence: in the inexact-quotient path the reference implementation may in
//! rare shapes strip a trailing zero produced by the final rounding step toward the preferred
//! scale; this keeps the rounded 16-digit mantissa as computed. The conformance corpus polices
//! actual usage.

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use num_traits::{One, Signed, Zero};

/// DECIMAL64 precision: 16 significant digits.
pub const DECIMAL64_DIGITS: u64 = 16;

/// Exact `a + b`, rounded to DECIMAL64 when the exact result exceeds 16 digits.
pub fn add(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    round_decimal64(a + b)
}

/// Exact `a - b`, rounded to DECIMAL64 when the exact result exceeds 16 digits.
pub fn sub(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    round_decimal64(a - b)
}

/// Exact `a * b`, rounded to DECIMAL64 when the exact result exceeds 16 digits.
pub fn mul(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    round_decimal64(a * b)
}

/// `a / b` with DECIMAL64 semantics. The caller must guard `b != 0`
/// (FEEL division by zero is `null` and is handled before this call).
pub fn div(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    let (ia, sa) = a.as_bigint_and_exponent();
    let (ib, sb) = b.as_bigint_and_exponent();
    debug_assert!(!ib.is_zero(), "caller must guard division by zero");
    let preferred_scale = sa - sb;
    if ia.is_zero() {
        return BigDecimal::new(BigInt::zero(), preferred_scale);
    }
    let negative = ia.is_negative() != ib.is_negative();
    let mut na = ia.abs();
    let mut nb = ib.abs();
    let g = gcd(na.clone(), nb.clone());
    if !g.is_one() {
        na /= &g;
        nb /= &g;
    }
    let (p2, p5, rest) = strip_2_5(nb.clone());
    if rest.is_one() {
        // Terminating quotient: na / (2^p2 · 5^p5) = na · 2^(m−p2) · 5^(m−p5) / 10^m.
        let m = p2.max(p5);
        let mut u = na * pow(2u32, m - p2) * pow(5u32, m - p5);
        let mut s = preferred_scale + i64::from(m);
        // Preferred-scale adjustment: strip trailing zeros down toward the preferred scale.
        while s > preferred_scale && divisible_by_10(&u) {
            u /= 10u8;
            s -= 1;
        }
        // Precision fit: an exact quotient is represented in 16 significant digits by
        // pulling further trailing zeros into the exponent (scale below preferred is fine).
        while digits(&u) > DECIMAL64_DIGITS && divisible_by_10(&u) {
            u /= 10u8;
            s -= 1;
        }
        let (m2, s2) = round_mantissa(u, s, false);
        return signed(m2, negative, s2);
    }
    // Non-terminating expansion: long-divide with guard digits (quotient lands on 17 or 18
    // digits by construction), then round to 16 with an exact sticky bit from the remainder.
    let dn = digits(&na) as i64;
    let dd = digits(&nb) as i64;
    let k = 17 - (dn - dd);
    let (num, den) = if k >= 0 {
        (na * pow(10u32, k as u32), nb)
    } else {
        (na, nb * pow(10u32, (-k) as u32))
    };
    let q = &num / &den;
    let r = num - &q * &den;
    let (m2, s2) = round_mantissa(q, preferred_scale + k, !r.is_zero());
    signed(m2, negative, s2)
}

/// Round an exact result to DECIMAL64 iff it exceeds 16 significant digits.
pub fn round_decimal64(x: BigDecimal) -> BigDecimal {
    if x.is_zero() || x.digits() <= DECIMAL64_DIGITS {
        return x;
    }
    let (mantissa, scale) = x.into_bigint_and_exponent();
    let negative = mantissa.is_negative();
    let (m, s) = round_mantissa(mantissa.abs(), scale, false);
    signed(m, negative, s)
}

/// Round a non-negative mantissa to 16 significant digits with `HALF_EVEN`.
///
/// `sticky` marks that non-zero digits exist beyond the supplied mantissa (division
/// remainder), which breaks apparent ties upward.
fn round_mantissa(u: BigInt, scale: i64, sticky: bool) -> (BigInt, i64) {
    let d = digits(&u);
    if d <= DECIMAL64_DIGITS {
        return (u, scale);
    }
    let drop = (d - DECIMAL64_DIGITS) as u32;
    let pow_d = pow(10u32, drop);
    let hi = &u / &pow_d;
    let lo = u - &hi * &pow_d;
    let half = pow(10u32, drop - 1) * BigInt::from(5);
    let round_up = lo > half || (lo == half && (sticky || is_odd(&hi)));
    let mut m = if round_up { hi + BigInt::one() } else { hi };
    let mut s = scale - i64::from(drop);
    if digits(&m) > DECIMAL64_DIGITS {
        // Carry: 999…9 + 1 → 1000…0 (17 digits, trailing zero is exact).
        m /= 10u8;
        s -= 1;
    }
    (m, s)
}

fn signed(magnitude: BigInt, negative: bool, scale: i64) -> BigDecimal {
    let m = if negative { -magnitude } else { magnitude };
    BigDecimal::new(m, scale)
}

/// Decimal digit count of a non-negative integer (`0` counts as 1 digit, matching decimal precision).
fn digits(u: &BigInt) -> u64 {
    if u.is_zero() {
        1
    } else {
        u.to_string().len() as u64
    }
}

fn divisible_by_10(u: &BigInt) -> bool {
    (u % 10u8).is_zero()
}

fn is_odd(u: &BigInt) -> bool {
    !(u % 2u8).is_zero()
}

fn pow(base: u32, exp: u32) -> BigInt {
    let mut out = BigInt::one();
    let b = BigInt::from(base);
    for _ in 0..exp {
        out *= &b;
    }
    out
}

fn gcd(mut a: BigInt, mut b: BigInt) -> BigInt {
    while !b.is_zero() {
        let r = &a % &b;
        a = b;
        b = r;
    }
    a
}

/// Split all factors of 2 and 5 out of `n`: returns `(p2, p5, rest)` with `n = 2^p2·5^p5·rest`.
fn strip_2_5(mut n: BigInt) -> (u32, u32, BigInt) {
    let mut p2 = 0u32;
    while (&n % 2u8).is_zero() {
        n /= 2u8;
        p2 += 1;
    }
    let mut p5 = 0u32;
    while (&n % 5u8).is_zero() {
        n /= 5u8;
        p5 += 1;
    }
    (p2, p5, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bd(s: &str) -> BigDecimal {
        s.parse().expect("valid literal")
    }

    #[test]
    fn exact_division_keeps_preferred_scale() {
        assert_eq!(div(&bd("100"), &bd("4")).to_string(), "25");
        assert_eq!(div(&bd("100.0"), &bd("4")).to_string(), "25.0");
        assert_eq!(div(&bd("1.0"), &bd("0.25")).to_string(), "4");
        assert_eq!(div(&bd("1"), &bd("8")).to_string(), "0.125");
        assert_eq!(div(&bd("0"), &bd("5")).to_string(), "0");
    }

    #[test]
    fn non_terminating_division_rounds_half_even_to_16_digits() {
        // A non-terminating quotient rounds to 16 significant digits, HALF_EVEN.
        assert_eq!(div(&bd("10"), &bd("3")).to_string(), "3.333333333333333");
        assert_eq!(div(&bd("2"), &bd("3")).to_string(), "0.6666666666666667");
        assert_eq!(div(&bd("1"), &bd("7")).to_string(), "0.1428571428571429");
        assert_eq!(div(&bd("-10"), &bd("3")).to_string(), "-3.333333333333333");
    }

    #[test]
    fn multiplication_rounds_only_past_16_digits() {
        assert_eq!(mul(&bd("1.5"), &bd("2")).to_string(), "3.0");
        // 12345678.87654321² exact is 32 digits → rounded to 16 digits HALF_EVEN.
        assert_eq!(
            mul(&bd("12345678.87654321"), &bd("12345678.87654321")).to_string(),
            "152415786922725.2"
        );
    }

    #[test]
    fn addition_keeps_exact_scale() {
        assert_eq!(add(&bd("0.1"), &bd("0.9")).to_string(), "1.0");
        assert_eq!(sub(&bd("100"), &bd("20")).to_string(), "80");
    }
}
