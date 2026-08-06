//! The nine Tier-1 facet kinds: `enumeration`, `pattern` (whole-value anchored),
//! `length`/`minLength`/`maxLength` (Unicode scalar counts; `length` is an exact
//! count), `totalDigits`/`fractionDigits` (value-space digit counts),
//! `minInclusive`/`maxInclusive` (decimal comparison).
//!
//! A restriction chain keeps one [`FacetStep`] per derivation step; a value must
//! satisfy every step (facets accumulate down the chain; multiple patterns *within*
//! one step OR together, patterns *across* steps AND together — the W3C rule).

use bigdecimal::BigDecimal;
use regex::Regex;

use crate::datatype::Builtin;

/// The facets contributed by one `restriction` step.
#[derive(Debug, Clone, Default)]
pub(crate) struct FacetStep {
    pub enumeration: Option<Vec<String>>,
    /// Anchored `^(?:…)$` compiled patterns; several in one step are alternatives.
    pub patterns: Vec<Regex>,
    /// The source pattern texts (for violation messages).
    pub pattern_texts: Vec<String>,
    /// Exact Unicode scalar count (`xs:length`).
    pub length: Option<u64>,
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub total_digits: Option<u64>,
    pub fraction_digits: Option<u64>,
    pub min_inclusive: Option<BigDecimal>,
    pub max_inclusive: Option<BigDecimal>,
}

impl FacetStep {
    pub(crate) fn is_empty(&self) -> bool {
        self.enumeration.is_none()
            && self.patterns.is_empty()
            && self.length.is_none()
            && self.min_length.is_none()
            && self.max_length.is_none()
            && self.total_digits.is_none()
            && self.fraction_digits.is_none()
            && self.min_inclusive.is_none()
            && self.max_inclusive.is_none()
    }

    /// Check a whitespace-normalized, lexically-valid value against this step.
    /// Returns the first violated facet's message (one specific violation per value —
    /// the companion "value not valid" violation is the validator's concern).
    pub(crate) fn check(&self, value: &str, builtin: Builtin) -> Result<(), String> {
        if !self.patterns.is_empty() && !self.patterns.iter().any(|p| p.is_match(value)) {
            return Err(format!(
                "value '{value}' does not match pattern '{}'",
                self.pattern_texts.join("' | '")
            ));
        }
        if let Some(allowed) = &self.enumeration {
            if !allowed.iter().any(|a| a == value) {
                return Err(format!(
                    "value '{value}' is not one of the enumerated values [{}]",
                    allowed.join(", ")
                ));
            }
        }
        if self.length.is_some() || self.min_length.is_some() || self.max_length.is_some() {
            let len = value.chars().count() as u64;
            if let Some(exact) = self.length {
                if len != exact {
                    return Err(format!(
                        "value '{value}' has length {len}, not the required length {exact}"
                    ));
                }
            }
            if let Some(min) = self.min_length {
                if len < min {
                    return Err(format!(
                        "value '{value}' has length {len}, shorter than minLength {min}"
                    ));
                }
            }
            if let Some(max) = self.max_length {
                if len > max {
                    return Err(format!(
                        "value '{value}' has length {len}, longer than maxLength {max}"
                    ));
                }
            }
        }
        let needs_decimal = self.total_digits.is_some()
            || self.fraction_digits.is_some()
            || self.min_inclusive.is_some()
            || self.max_inclusive.is_some();
        if needs_decimal && builtin.is_numeric() {
            // Lexical validity was already checked; a parse failure cannot produce a
            // *second* violation here, so bail out silently if it happens anyway.
            let Ok(number) = value.parse::<BigDecimal>() else {
                return Ok(());
            };
            let (int_digits, frac_digits) = digit_counts(&number);
            if let Some(max) = self.total_digits {
                if int_digits + frac_digits > max {
                    return Err(format!(
                        "value '{value}' has {} total digits, more than totalDigits {max}",
                        int_digits + frac_digits
                    ));
                }
            }
            if let Some(max) = self.fraction_digits {
                if frac_digits > max {
                    return Err(format!(
                        "value '{value}' has {frac_digits} fraction digits, more than fractionDigits {max}"
                    ));
                }
            }
            if let Some(min) = &self.min_inclusive {
                if number < *min {
                    return Err(format!("value '{value}' is less than minInclusive {min}"));
                }
            }
            if let Some(max) = &self.max_inclusive {
                if number > *max {
                    return Err(format!(
                        "value '{value}' is greater than maxInclusive {max}"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Value-space digit counts: integer-part digits (leading zeros dropped, `0` counts as
/// none) and fraction digits (trailing zeros dropped).
fn digit_counts(number: &BigDecimal) -> (u64, u64) {
    let normalized = number.normalized();
    let (mantissa, exponent) = normalized.as_bigint_and_exponent();
    if mantissa.sign() == bigdecimal::num_bigint::Sign::NoSign {
        return (0, 0);
    }
    let mantissa_digits = mantissa.magnitude().to_string().len() as i64;
    if exponent <= 0 {
        // A pure integer scaled up: every mantissa digit plus the trailing zeros.
        ((mantissa_digits - exponent) as u64, 0)
    } else {
        let int_digits = (mantissa_digits - exponent).max(0) as u64;
        (int_digits, exponent as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dc(v: &str) -> (u64, u64) {
        digit_counts(&BigDecimal::from_str(v).unwrap())
    }

    #[test]
    fn value_space_digit_counts() {
        assert_eq!(dc("100"), (3, 0));
        assert_eq!(dc("100.00"), (3, 0));
        assert_eq!(dc("0.05"), (0, 2));
        assert_eq!(dc("100.123456"), (3, 6));
        assert_eq!(dc("0"), (0, 0));
        assert_eq!(dc("-1.10"), (1, 1));
    }

    #[test]
    fn facet_checks() {
        let step = FacetStep {
            patterns: vec![Regex::new("^(?:[A-Z]{3,3})$").unwrap()],
            pattern_texts: vec!["[A-Z]{3,3}".to_string()],
            ..FacetStep::default()
        };
        assert!(step.check("USD", Builtin::String).is_ok());
        assert!(step.check("US1", Builtin::String).is_err());

        let step = FacetStep {
            min_length: Some(1),
            max_length: Some(35),
            ..FacetStep::default()
        };
        assert!(step.check("x", Builtin::String).is_ok());
        assert!(step.check("", Builtin::String).is_err());
        assert!(step.check(&"y".repeat(36), Builtin::String).is_err());

        let step = FacetStep {
            total_digits: Some(18),
            fraction_digits: Some(5),
            min_inclusive: Some(BigDecimal::from(0)),
            ..FacetStep::default()
        };
        assert!(step.check("100.00", Builtin::Decimal).is_ok());
        assert!(step.check("100.123456", Builtin::Decimal).is_err());
        assert!(step.check("-5.00", Builtin::Decimal).is_err());
        assert!(step.check(&("1".repeat(19)), Builtin::Decimal).is_err());
    }
}
