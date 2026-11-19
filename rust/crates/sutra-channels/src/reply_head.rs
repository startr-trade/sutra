//! The head of a synchronous reply: the status `<q:reply status>` sets and the response headers
//! its `<q:header>` children declare.
//!
//! Pure rules, shared by the deploy-time lint (which refuses a bad literal before anything runs)
//! and the HTTP answer (which refuses a bad evaluated value with a 500). Kept free of the transport
//! spine so the lint compiles without it.
//!
//! The rules:
//!
//! - A status is a final answer between 200 and 599. A 1xx is not a final answer (101 would hand
//!   the connection over to another protocol), so it is refused.
//! - A 3xx answer must carry a `Location` header, since a redirect with nowhere to go is a broken
//!   answer rather than a choice.
//! - A header name is an RFC 9110 token. The names that describe the connection or the message
//!   framing (`Connection`, `Transfer-Encoding`, `Content-Length`, …) belong to the HTTP layer,
//!   `Content-Type` is the reply's `contentType`, and the `Sutra-` prefix is the engine's own
//!   (its acceptance receipt), so none of them can be set from a flow.

use sutra_feel::value::FeelValue;

/// The header names a reply may not set, lower-case. Hop-by-hop and framing headers are the HTTP
/// layer's; `content-type` comes from the reply's `contentType` (or `responseContentType`).
pub const REFUSED_REPLY_HEADER_NAMES: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "content-length",
    "content-type",
];

/// The header-name prefix the engine keeps for itself (lower-case): the acceptance receipt's
/// `Sutra-Instance-Id` and `Sutra-Receipt`, and any header the engine adds later.
pub const ENGINE_REPLY_HEADER_PREFIX: &str = "sutra-";

/// The lowest and highest status a reply may answer with.
pub const REPLY_STATUS_MIN: u32 = 200;
/// See [`REPLY_STATUS_MIN`].
pub const REPLY_STATUS_MAX: u32 = 599;

/// How a `<q:reply status>` text is read: literal first, else FEEL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyStatusSpec<'a> {
    /// Digits only: the status itself (saturated at `u32::MAX` when too long to hold).
    Literal(u32),
    /// Any other text: a FEEL expression evaluated when the reply renders.
    Expression(&'a str),
}

/// Read a `<q:reply status>` text. Digits only (surrounding blanks ignored) are a literal; any
/// other text is a FEEL expression.
pub fn parse_reply_status_spec(text: &str) -> ReplyStatusSpec<'_> {
    let trimmed = text.trim();
    if !trimmed.is_empty() && trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return ReplyStatusSpec::Literal(trimmed.parse::<u32>().unwrap_or(u32::MAX));
    }
    ReplyStatusSpec::Expression(trimmed)
}

/// Why `status` cannot answer, or `None` when it can. `has_location` says whether the answer
/// carries a `Location` header, which a 3xx needs.
pub fn reply_status_fault(status: u32, has_location: bool) -> Option<String> {
    if !(REPLY_STATUS_MIN..=REPLY_STATUS_MAX).contains(&status) {
        return Some(format!(
            "status {status} is outside {REPLY_STATUS_MIN}-{REPLY_STATUS_MAX}; a reply is a final \
             answer, so a 1xx is refused too"
        ));
    }
    if (300..400).contains(&status) && !has_location {
        return Some(format!(
            "status {status} is a redirect, which needs a Location header (declare \
             <q:header name=\"Location\">, and make sure it is not null)"
        ));
    }
    None
}

/// The status an evaluated `status` expression gives: a whole number or a string of digits.
/// `Ok(None)` for `null`, which keeps the default status. `Err` names what came back instead
/// (its kind, never its content).
pub fn reply_status_of_value(value: &FeelValue) -> Result<Option<u32>, String> {
    let digits = match value {
        FeelValue::Null => return Ok(None),
        FeelValue::Number(_) => {
            let text = sutra_feel::value::canonical_string_of(value);
            // A whole number may print with a zero fraction (`201.0`); anything else is not one.
            match text.split_once('.') {
                Some((whole, fraction)) if fraction.bytes().all(|b| b == b'0') => whole.to_string(),
                Some(_) => return Err("a number with a fraction".to_string()),
                None => text,
            }
        }
        FeelValue::String(text) => text.trim().to_string(),
        _ => return Err(format!("a {}", feel_kind(value))),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(match value {
            FeelValue::String(_) => "a string that is not a whole number".to_string(),
            _ => "a negative number".to_string(),
        });
    }
    Ok(Some(digits.parse::<u32>().unwrap_or(u32::MAX)))
}

/// Why `name` cannot be a reply header, or `None` when it can.
pub fn reply_header_name_fault(name: &str) -> Option<String> {
    if name.is_empty() || !name.bytes().all(is_token_byte) {
        return Some(format!(
            "header name '{}' is not an HTTP token (letters, digits and !#$%&'*+-.^_`|~ only)",
            name.escape_default()
        ));
    }
    let lower = name.to_ascii_lowercase();
    if REFUSED_REPLY_HEADER_NAMES.contains(&lower.as_str()) {
        return Some(if lower == "content-type" {
            format!(
                "header '{name}' is set from the reply's contentType (or responseContentType), \
                 not from a <q:header>"
            )
        } else {
            format!(
                "header '{name}' describes the connection or the framing; the HTTP layer sets it"
            )
        });
    }
    if lower.starts_with(ENGINE_REPLY_HEADER_PREFIX) {
        return Some(format!(
            "header '{name}' uses the '{ENGINE_REPLY_HEADER_PREFIX}' prefix, which the engine \
             keeps for its own headers"
        ));
    }
    None
}

/// Whether `value` can be written as a header field value: visible ASCII, spaces and tabs, and
/// bytes past ASCII; no CR, LF, NUL or other control character.
pub fn reply_header_value_ok(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b == b'\t' || (0x20..0x7f).contains(&b) || b >= 0x80)
}

/// RFC 9110 `tchar`.
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

fn feel_kind(value: &FeelValue) -> &'static str {
    match value {
        FeelValue::Null => "null",
        FeelValue::Boolean(_) => "boolean",
        FeelValue::Number(_) => "number",
        FeelValue::String(_) => "string",
        FeelValue::Instant(..) => "date and time",
        FeelValue::Date(_) => "date",
        FeelValue::Time(..) => "time",
        FeelValue::Duration(_) => "duration",
        FeelValue::List(_) => "list",
        FeelValue::Map(_) => "context",
        FeelValue::Function(_) => "function",
        _ => "value of another kind",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digits_are_a_literal_and_anything_else_an_expression() {
        assert_eq!(
            parse_reply_status_spec(" 201 "),
            ReplyStatusSpec::Literal(201)
        );
        assert_eq!(
            parse_reply_status_spec("99999999999"),
            ReplyStatusSpec::Literal(u32::MAX)
        );
        assert_eq!(
            parse_reply_status_spec("if ok then 200 else 404"),
            ReplyStatusSpec::Expression("if ok then 200 else 404")
        );
        assert_eq!(
            parse_reply_status_spec("-1"),
            ReplyStatusSpec::Expression("-1")
        );
    }

    #[test]
    fn the_range_is_200_to_599_and_a_redirect_needs_a_location() {
        assert!(reply_status_fault(200, false).is_none());
        assert!(reply_status_fault(599, false).is_none());
        assert!(reply_status_fault(404, false).is_none());
        assert!(reply_status_fault(101, true).is_some());
        assert!(reply_status_fault(199, true).is_some());
        assert!(reply_status_fault(600, true).is_some());
        assert!(reply_status_fault(302, false).is_some());
        assert!(reply_status_fault(302, true).is_none());
    }

    #[test]
    fn an_evaluated_status_is_a_whole_number_or_digits() {
        let n = |s: &str| FeelValue::Number(s.parse().unwrap());
        assert_eq!(reply_status_of_value(&n("201")), Ok(Some(201)));
        assert_eq!(reply_status_of_value(&n("201.0")), Ok(Some(201)));
        assert_eq!(
            reply_status_of_value(&FeelValue::String("404".into())),
            Ok(Some(404))
        );
        assert_eq!(reply_status_of_value(&FeelValue::Null), Ok(None));
        assert!(reply_status_of_value(&n("201.5")).is_err());
        assert!(reply_status_of_value(&n("-1")).is_err());
        assert!(reply_status_of_value(&FeelValue::String("Created".into())).is_err());
        assert!(reply_status_of_value(&FeelValue::Boolean(true)).is_err());
    }

    #[test]
    fn header_names_are_tokens_and_not_the_transports_or_the_engines() {
        assert!(reply_header_name_fault("Location").is_none());
        assert!(reply_header_name_fault("WWW-Authenticate").is_none());
        assert!(reply_header_name_fault("X-User-Id").is_none());
        assert!(reply_header_name_fault("Bad Name").is_some());
        assert!(reply_header_name_fault("X-Bad:").is_some());
        assert!(reply_header_name_fault("").is_some());
        for refused in [
            "Content-Length",
            "transfer-encoding",
            "Connection",
            "Content-Type",
            "Upgrade",
            "Sutra-Receipt",
            "sutra-instance-id",
        ] {
            assert!(reply_header_name_fault(refused).is_some(), "{refused}");
        }
    }

    #[test]
    fn a_header_value_has_no_control_characters() {
        assert!(reply_header_value_ok(
            "Bearer realm=\"api\", error=\"invalid_token\""
        ));
        assert!(reply_header_value_ok("caf\u{e9}"));
        assert!(!reply_header_value_ok("a\r\nSet-Cookie: x=1"));
        assert!(!reply_header_value_ok("a\0b"));
    }
}
