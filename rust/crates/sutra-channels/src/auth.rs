//! Message-level channel authentication — the shared enforcement helper every
//! transport routes through, so a scheme's semantics live in exactly one place.
//!
//! Two directions:
//!
//! - **Inbound** ([`BrokerInboundAuth`]): a broker source parses `inbound-auth.*` channel
//!   properties into this, then calls [`BrokerInboundAuth::verify`] on EACH delivery's
//!   headers. `apikey` / `bearer` do a case-insensitive header lookup + constant-time
//!   compare against a ref-resolved expected secret; a mismatch → [`InboundVerdict::Reject`]
//!   (the source drops the delivery — Kafka commit/advance, RabbitMQ `basicNack(requeue=
//!   false)` — and raises `SUTRA.INBOUND.<BROKER>.AUTH_REJECTED`). `mtls` is per-message
//!   UNSUPPORTED: it allows through (broker-level TLS still applies) and the source logs a
//!   one-time boot WARN `SUTRA.INBOUND.<BROKER>.MTLS_UNSUPPORTED`.
//!
//! - **Outbound** ([`outbound_auth_headers`]): the auth-header injection the outbox
//!   dispatcher applies to EVERY destination (broker-agnostic — the sinks carry the headers
//!   verbatim, so kafka/rabbitmq/http and the three new brokers all inherit it with no
//!   per-broker code). `bearer` → `authorization: Bearer <material>`; `apikey` → the
//!   configured header (default `X-API-Key`); `mtls` / an unresolved ref inject nothing
//!   (the dispatcher WARNs and skips — the upstream answers 401/403 rather than receiving a
//!   malformed header).
//!
//! The shape is identical across brokers — only the `<BROKER>` code segment differs. Wiring a
//! NEW broker's inbound auth is a single [`BrokerInboundAuth::verify`] call site in its source's
//! delivery loop plus a [`BrokerInboundAuth::from_properties`] call in its engine wiring — no
//! change here.

use std::collections::BTreeMap;

use crate::diag::Diagnostic;

/// Property key: the inbound message-auth scheme (`apikey` | `bearer` | `mtls`).
pub const KEY_SCHEME: &str = "inbound-auth.scheme";
/// Property key: the resolver reference (`env:…` / `secret:…` / `vault:…`) whose resolved
/// value is the expected credential (apikey / bearer schemes).
pub const KEY_EXPECTED_REF: &str = "inbound-auth.expected-key-ref";
/// Property key: the header/property carrying the presented credential (defaults per scheme).
pub const KEY_HEADER: &str = "inbound-auth.header";

/// Default header for the `apikey` scheme.
pub const DEFAULT_APIKEY_HEADER: &str = "X-API-Key";
/// Default header for the `bearer` scheme.
pub const DEFAULT_BEARER_HEADER: &str = "authorization";

/// The auth header the `bearer` outbound scheme writes (lowercase — the broker binding form;
/// HTTP treats it case-insensitively).
pub const OUTBOUND_AUTHORIZATION_HEADER: &str = "authorization";

/// Fixed-cost byte comparison — a length mismatch is leaked (unavoidable), bit-pattern
/// mismatches inside a common length are constant-time. The one comparator every inbound
/// auth check (HTTP + brokers) shares.
pub fn constant_time_equals(a: &[u8], b: &[u8]) -> bool {
    let mut diff = 0u8;
    for i in 0..a.len().max(b.len()) {
        let ax = a.get(i).copied().unwrap_or(0);
        let bx = b.get(i).copied().unwrap_or(0);
        diff |= ax ^ bx;
    }
    a.len() == b.len() && diff == 0
}

/// The inbound message-auth scheme declared on a broker channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundScheme {
    /// Shared-secret API key in a configured header.
    ApiKey,
    /// Static bearer token (`Authorization: Bearer <token>` form).
    Bearer,
    /// Per-message mTLS — UNSUPPORTED (allow-through + boot WARN; broker-level TLS applies).
    Mtls,
}

/// The verdict of an inbound per-message auth check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundVerdict {
    /// The delivery is authorised (or the scheme is per-message mTLS, which allows through).
    Allow,
    /// The delivery presented a missing/wrong credential — the source drops it and raises
    /// `SUTRA.INBOUND.<BROKER>.AUTH_REJECTED`.
    Reject,
}

/// Parsed inbound message-auth config on a broker source. The expected secret is resolved
/// ONCE at wiring time (envref registry — `env:`/`secret:`/`vault:`); an unresolvable ref
/// fails the channel closed at boot rather than silently admitting traffic.
#[derive(Debug, Clone)]
pub struct BrokerInboundAuth {
    scheme: InboundScheme,
    header: String,
    /// The resolved expected credential — `None` only for the mTLS scheme.
    expected: Option<String>,
}

impl BrokerInboundAuth {
    /// Parse `inbound-auth.*` from a channel's flattened properties. `None` when no
    /// `inbound-auth.scheme` is declared (the channel has no per-message auth). `resolve_ref`
    /// resolves the expected-key reference to plaintext (the engine passes the envref
    /// registry); `config_invalid_code` labels the broker's config diagnostics.
    pub fn from_properties(
        props: &BTreeMap<String, String>,
        config_invalid_code: &str,
        resolve_ref: impl Fn(&str) -> Result<String, String>,
    ) -> Result<Option<BrokerInboundAuth>, Diagnostic> {
        let Some(raw_scheme) = props
            .get(KEY_SCHEME)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let scheme = match raw_scheme.to_ascii_lowercase().as_str() {
            "apikey" => InboundScheme::ApiKey,
            "bearer" => InboundScheme::Bearer,
            "mtls" => InboundScheme::Mtls,
            other => {
                return Err(Diagnostic::error(
                    config_invalid_code,
                    format!(
                        "channel property '{KEY_SCHEME}' is '{other}'; must be one of apikey, \
                         bearer, mtls"
                    ),
                ));
            }
        };
        let header = props
            .get(KEY_HEADER)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| match scheme {
                InboundScheme::ApiKey => DEFAULT_APIKEY_HEADER.to_string(),
                _ => DEFAULT_BEARER_HEADER.to_string(),
            });
        let expected = match scheme {
            InboundScheme::Mtls => None,
            InboundScheme::ApiKey | InboundScheme::Bearer => {
                let reference = props
                    .get(KEY_EXPECTED_REF)
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        Diagnostic::error(
                            config_invalid_code,
                            format!(
                                "channel property '{KEY_SCHEME}={raw_scheme}' requires \
                                 '{KEY_EXPECTED_REF}'"
                            ),
                        )
                    })?;
                Some(resolve_ref(reference).map_err(|e| {
                    Diagnostic::error(
                        config_invalid_code,
                        format!("channel property '{KEY_EXPECTED_REF}' could not be resolved: {e}"),
                    )
                })?)
            }
        };
        Ok(Some(BrokerInboundAuth {
            scheme,
            header,
            expected,
        }))
    }

    /// The declared scheme (a source logs the boot MTLS WARN when this is `Mtls`).
    pub fn scheme(&self) -> InboundScheme {
        self.scheme
    }

    /// Verify one delivery's headers/properties. mTLS allows through (per-message mTLS is
    /// unsupported; broker-level TLS still applies). apikey/bearer look the header up
    /// case-insensitively, strip a `Bearer ` prefix for the bearer scheme, and
    /// constant-time-compare against the expected secret.
    pub fn verify(&self, headers: &BTreeMap<String, String>) -> InboundVerdict {
        match self.scheme {
            InboundScheme::Mtls => InboundVerdict::Allow,
            InboundScheme::ApiKey | InboundScheme::Bearer => {
                let Some(expected) = &self.expected else {
                    return InboundVerdict::Reject;
                };
                let Some(presented) = lookup_ci(headers, &self.header) else {
                    return InboundVerdict::Reject;
                };
                let presented = if matches!(self.scheme, InboundScheme::Bearer) {
                    strip_bearer(presented.trim())
                } else {
                    presented.trim()
                };
                if constant_time_equals(expected.as_bytes(), presented.as_bytes()) {
                    InboundVerdict::Allow
                } else {
                    InboundVerdict::Reject
                }
            }
        }
    }
}

/// Case-insensitive header lookup (broker property tables and HTTP headers alike).
fn lookup_ci<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Strip a leading case-insensitive `Bearer ` scheme (RFC 6750), returning the token.
fn strip_bearer(value: &str) -> &str {
    let prefix = "Bearer ";
    if value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        value[prefix.len()..].trim()
    } else {
        value
    }
}

/// The outbound auth-header injection (the outbox dispatcher's shared helper). `scheme` is
/// the auth-ref scheme; `material` is the already-resolved secret; `apikey_header` is the
/// header the apikey scheme writes (default [`DEFAULT_APIKEY_HEADER`]). `bearer` →
/// `authorization: Bearer <material>`; `apikey` → `<apikey_header>: <material>`; any other
/// scheme (`mtls`, unknown) injects nothing (the caller WARNs + skips).
pub fn outbound_auth_headers(
    scheme: &str,
    material: &str,
    apikey_header: &str,
) -> Vec<(String, String)> {
    match scheme {
        "bearer" => vec![(
            OUTBOUND_AUTHORIZATION_HEADER.to_string(),
            format!("Bearer {material}"),
        )],
        "apikey" => {
            let header = if apikey_header.trim().is_empty() {
                DEFAULT_APIKEY_HEADER
            } else {
                apikey_header
            };
            vec![(header.to_string(), material.to_string())]
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG_INVALID: &str = "SUTRA.INBOUND.KAFKA.CONFIG_INVALID";

    fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn ok_resolver(reference: &str) -> Result<String, String> {
        // A trivial resolver: `literal:VALUE` yields VALUE; anything else "fails".
        reference
            .strip_prefix("literal:")
            .map(str::to_string)
            .ok_or_else(|| format!("no resolver for '{reference}'"))
    }

    #[test]
    fn constant_time_equals_matches_only_identical_bytes() {
        assert!(constant_time_equals(b"secret", b"secret"));
        assert!(!constant_time_equals(b"secret", b"secreT"));
        assert!(!constant_time_equals(b"secret", b"secret-longer"));
        assert!(constant_time_equals(b"", b""));
    }

    #[test]
    fn absent_when_no_inbound_auth_configured() {
        // Empty channel properties parse to `None` — inbound auth stays off unless declared.
        let parsed =
            BrokerInboundAuth::from_properties(&props(&[]), CONFIG_INVALID, ok_resolver).unwrap();
        assert!(parsed.is_none());
    }

    #[test]
    fn parses_apikey_with_default_header() {
        // scheme + expected-key-ref alone yield the `X-API-Key` default header and a
        // ref-resolved expected credential.
        let parsed = BrokerInboundAuth::from_properties(
            &props(&[
                (KEY_SCHEME, "apikey"),
                (KEY_EXPECTED_REF, "literal:correct-key"),
            ]),
            CONFIG_INVALID,
            ok_resolver,
        )
        .unwrap()
        .expect("configured");
        assert_eq!(parsed.scheme(), InboundScheme::ApiKey);
        assert_eq!(parsed.header, "X-API-Key");
        assert_eq!(parsed.expected.as_deref(), Some("correct-key"));
    }

    #[test]
    fn apikey_accepts_matching_and_rejects_bad_credential() {
        let auth = BrokerInboundAuth::from_properties(
            &props(&[
                (KEY_SCHEME, "apikey"),
                (KEY_EXPECTED_REF, "literal:correct-key"),
            ]),
            CONFIG_INVALID,
            ok_resolver,
        )
        .unwrap()
        .expect("configured");
        assert_eq!(
            auth.verify(&props(&[("X-API-Key", "correct-key")])),
            InboundVerdict::Allow
        );
        assert_eq!(
            auth.verify(&props(&[("x-api-key", "correct-key")])),
            InboundVerdict::Allow,
            "header lookup is case-insensitive"
        );
        assert_eq!(
            auth.verify(&props(&[("X-API-Key", "wrong-key")])),
            InboundVerdict::Reject
        );
        assert_eq!(auth.verify(&props(&[])), InboundVerdict::Reject);
    }

    #[test]
    fn bearer_strips_the_prefix_before_comparing() {
        let auth = BrokerInboundAuth::from_properties(
            &props(&[(KEY_SCHEME, "bearer"), (KEY_EXPECTED_REF, "literal:tok-1")]),
            CONFIG_INVALID,
            ok_resolver,
        )
        .unwrap()
        .expect("configured");
        assert_eq!(auth.header, "authorization");
        assert_eq!(
            auth.verify(&props(&[("Authorization", "Bearer tok-1")])),
            InboundVerdict::Allow
        );
        assert_eq!(
            auth.verify(&props(&[("authorization", "bearer tok-1")])),
            InboundVerdict::Allow,
            "the Bearer scheme is case-insensitive"
        );
        assert_eq!(
            auth.verify(&props(&[("Authorization", "Bearer nope")])),
            InboundVerdict::Reject
        );
    }

    #[test]
    fn mtls_allows_through_without_a_secret() {
        let auth = BrokerInboundAuth::from_properties(
            &props(&[(KEY_SCHEME, "mtls")]),
            CONFIG_INVALID,
            ok_resolver,
        )
        .unwrap()
        .expect("configured");
        assert_eq!(auth.scheme(), InboundScheme::Mtls);
        assert_eq!(auth.verify(&props(&[])), InboundVerdict::Allow);
    }

    #[test]
    fn unknown_scheme_and_missing_ref_fail_closed() {
        assert!(BrokerInboundAuth::from_properties(
            &props(&[(KEY_SCHEME, "oauth2")]),
            CONFIG_INVALID,
            ok_resolver
        )
        .is_err());
        // apikey/bearer require the expected-key-ref.
        assert!(BrokerInboundAuth::from_properties(
            &props(&[(KEY_SCHEME, "apikey")]),
            CONFIG_INVALID,
            ok_resolver
        )
        .is_err());
        // an unresolvable ref fails the channel closed at boot.
        assert!(BrokerInboundAuth::from_properties(
            &props(&[(KEY_SCHEME, "apikey"), (KEY_EXPECTED_REF, "env:MISSING")]),
            CONFIG_INVALID,
            ok_resolver
        )
        .is_err());
    }

    #[test]
    fn outbound_headers_carry_the_scheme_specific_header() {
        // bearer → `authorization: Bearer <material>`; apikey → the configured header, or
        // `X-API-Key` when blank.
        assert_eq!(
            outbound_auth_headers("bearer", "tok-1", "X-API-Key"),
            vec![("authorization".to_string(), "Bearer tok-1".to_string())]
        );
        assert_eq!(
            outbound_auth_headers("apikey", "kv-9", "X-Kafka-Key"),
            vec![("X-Kafka-Key".to_string(), "kv-9".to_string())]
        );
        assert_eq!(
            outbound_auth_headers("apikey", "kv-9", ""),
            vec![("X-API-Key".to_string(), "kv-9".to_string())]
        );
        // mtls / unknown inject nothing (caller WARNs + skips).
        assert!(outbound_auth_headers("mtls", "bundle", "X-API-Key").is_empty());
    }
}
