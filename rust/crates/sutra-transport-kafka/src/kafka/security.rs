//! Native TLS and SASL for the Kafka client, and the context that turns librdkafka's background
//! connection errors into diagnostics an operator can act on.
//!
//! librdkafka's TLS backend is OpenSSL (statically linked here), so this transport cannot share
//! rustls with the rest of the engine. It follows the same trust policy anyway. With no CA setting
//! the broker is verified against the platform bundle, honouring `SSL_CERT_FILE` and
//! `SSL_CERT_DIR`. `tls-ca-file` or `tls-ca-pem` replaces that with the named bundle. Certificate
//! and host-name verification are on unless `tls-insecure-skip-verify: true` says otherwise, which
//! logs a warning each time it takes effect.
//!
//! SASL mechanisms are PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512. GSSAPI is not compiled in, and
//! OAUTHBEARER has no token source here, so both are refused rather than left to fail later.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use tracing::warn;

use sutra_channels::diag::Diagnostic;

/// The SASL mechanisms this build accepts.
pub const SASL_MECHANISMS: [&str; 3] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

/// The engine-wide sink's `security.protocol`.
pub const SINK_SECURITY_PROTOCOL_ENV: &str = "SUTRA_SINK_KAFKA_SECURITY_PROTOCOL";
/// The engine-wide sink's SASL mechanism.
pub const SINK_SASL_MECHANISM_ENV: &str = "SUTRA_SINK_KAFKA_SASL_MECHANISM";
/// The engine-wide sink's SASL username.
pub const SINK_SASL_USERNAME_ENV: &str = "SUTRA_SINK_KAFKA_SASL_USERNAME";
/// The engine-wide sink's SASL password.
pub const SINK_SASL_PASSWORD_ENV: &str = "SUTRA_SINK_KAFKA_SASL_PASSWORD";
/// A PEM bundle that replaces the platform bundle for the engine-wide sink.
pub const SINK_TLS_CA_FILE_ENV: &str = "SUTRA_SINK_KAFKA_TLS_CA_FILE";
/// Set to `true` to disable broker certificate and host-name verification for the sink.
pub const SINK_TLS_INSECURE_SKIP_VERIFY_ENV: &str = "SUTRA_SINK_KAFKA_TLS_INSECURE_SKIP_VERIFY";

/// The Kafka `security.protocol` values librdkafka accepts (the allow-list).
pub(crate) const SECURITY_PROTOCOLS: [&str; 4] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];

/// The client settings behind a `security.protocol`. As authored, `sasl_username`, `sasl_password`
/// and `tls_ca_pem` hold secret references; the wiring swaps in the resolved values before a client
/// is built, and debug output never shows the credentials.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct KafkaBrokerSecurity {
    /// `sasl.mechanism`, upper-cased; one of [`SASL_MECHANISMS`].
    pub sasl_mechanism: Option<String>,
    /// `sasl.username`.
    pub sasl_username: Option<String>,
    /// `sasl.password`.
    pub sasl_password: Option<String>,
    /// `tls-ca-file`: path to a PEM bundle that replaces the platform bundle.
    pub tls_ca_file: Option<String>,
    /// `tls-ca-pem`: PEM text (a secret reference until resolved) that replaces the platform
    /// bundle.
    pub tls_ca_pem: Option<String>,
    /// `tls-insecure-skip-verify`: accept any certificate and host name. Off unless set.
    pub tls_insecure_skip_verify: bool,
}

impl std::fmt::Debug for KafkaBrokerSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBrokerSecurity")
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("tls_insecure_skip_verify", &self.tls_insecure_skip_verify)
            .finish_non_exhaustive()
    }
}

impl KafkaBrokerSecurity {
    /// Check these settings against the protocol: everything required is present, and nothing is
    /// set that the protocol would silently ignore. The error text names the setting and carries
    /// no value.
    pub(crate) fn validate(&self, protocol: &str) -> Result<(), String> {
        let uses_sasl = protocol.starts_with("SASL_");
        let uses_tls = protocol.ends_with("SSL");
        if uses_sasl {
            match self.sasl_mechanism.as_deref() {
                None => return Err(format!("{protocol} requires 'sasl.mechanism'")),
                Some(m) if !SASL_MECHANISMS.contains(&m) => {
                    return Err(format!(
                        "'sasl.mechanism' must be one of {SASL_MECHANISMS:?}, got '{m}'"
                    ))
                }
                Some(_) => {}
            }
            if self.sasl_username.is_none() || self.sasl_password.is_none() {
                return Err(format!(
                    "{protocol} requires both 'sasl.username' and 'sasl.password'"
                ));
            }
        } else if self.sasl_mechanism.is_some()
            || self.sasl_username.is_some()
            || self.sasl_password.is_some()
        {
            return Err(format!(
                "a sasl.* setting is present but 'security.protocol' is {protocol}; the \
                 connection would not authenticate"
            ));
        }
        let sets_tls = self.tls_ca_file.is_some()
            || self.tls_ca_pem.is_some()
            || self.tls_insecure_skip_verify;
        if sets_tls && !uses_tls {
            return Err(format!(
                "a tls-* setting is present but 'security.protocol' is {protocol}; the connection \
                 would not use TLS"
            ));
        }
        if self.tls_ca_file.is_some() && self.tls_ca_pem.is_some() {
            return Err("both 'tls-ca-file' and 'tls-ca-pem' are set".to_string());
        }
        Ok(())
    }

    /// Write these settings onto a librdkafka client configuration.
    pub(crate) fn apply_to(&self, protocol: &str, client: &mut ClientConfig) {
        client.set("security.protocol", protocol);
        if let (Some(mechanism), Some(username), Some(password)) = (
            &self.sasl_mechanism,
            &self.sasl_username,
            &self.sasl_password,
        ) {
            client
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
        if !protocol.ends_with("SSL") {
            return;
        }
        if self.tls_insecure_skip_verify {
            warn!(
                "kafka broker TLS verification is DISABLED (tls-insecure-skip-verify): any \
                 certificate and host name will be accepted"
            );
            client
                .set("enable.ssl.certificate.verification", "false")
                .set("ssl.endpoint.identification.algorithm", "none");
            return;
        }
        client
            .set("enable.ssl.certificate.verification", "true")
            .set("ssl.endpoint.identification.algorithm", "https");
        if let Some(pem) = &self.tls_ca_pem {
            client.set("ssl.ca.pem", pem);
        } else {
            client.set(
                "ssl.ca.location",
                self.tls_ca_file
                    .clone()
                    .unwrap_or_else(platform_ca_location),
            );
        }
    }
}

/// The platform bundle librdkafka should use, matching what the rest of the engine reads:
/// `SSL_CERT_FILE`, then `SSL_CERT_DIR`, then librdkafka's probe of the well-known paths. The probe
/// is needed because the statically linked OpenSSL's compiled-in default directory is not the
/// platform's.
fn platform_ca_location() -> String {
    ["SSL_CERT_FILE", "SSL_CERT_DIR"]
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|v| !v.trim().is_empty()))
        .unwrap_or_else(|| "probe".to_string())
}

/// `security.protocol` and its settings for the engine-wide sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSinkSecurity {
    /// `security.protocol`, upper-cased.
    pub protocol: String,
    /// The settings behind it.
    pub security: KafkaBrokerSecurity,
}

impl Default for KafkaSinkSecurity {
    fn default() -> KafkaSinkSecurity {
        KafkaSinkSecurity {
            protocol: "PLAINTEXT".to_string(),
            security: KafkaBrokerSecurity::default(),
        }
    }
}

impl KafkaSinkSecurity {
    /// Read the sink's settings from the environment. An unset environment yields PLAINTEXT, and an
    /// invalid one is an error the sink reports on every send (never a silent downgrade).
    pub fn from_env() -> Result<KafkaSinkSecurity, String> {
        let names = [
            SINK_SECURITY_PROTOCOL_ENV,
            SINK_SASL_MECHANISM_ENV,
            SINK_SASL_USERNAME_ENV,
            SINK_SASL_PASSWORD_ENV,
            SINK_TLS_CA_FILE_ENV,
            SINK_TLS_INSECURE_SKIP_VERIFY_ENV,
        ];
        let values: BTreeMap<&str, String> = names
            .iter()
            .filter_map(|name| std::env::var(name).ok().map(|value| (*name, value)))
            .collect();
        Self::from_values(&values)
    }

    pub(crate) fn from_values(
        values: &BTreeMap<&str, String>,
    ) -> Result<KafkaSinkSecurity, String> {
        let read = |name: &str| {
            values
                .get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let protocol = read(SINK_SECURITY_PROTOCOL_ENV)
            .unwrap_or_else(|| "PLAINTEXT".to_string())
            .to_ascii_uppercase();
        if !SECURITY_PROTOCOLS.contains(&protocol.as_str()) {
            return Err(format!(
                "{SINK_SECURITY_PROTOCOL_ENV} must be one of {SECURITY_PROTOCOLS:?}, got \
                 '{protocol}'"
            ));
        }
        let insecure = read(SINK_TLS_INSECURE_SKIP_VERIFY_ENV).unwrap_or_default();
        let tls_insecure_skip_verify =
            if insecure.is_empty() || insecure.eq_ignore_ascii_case("false") {
                false
            } else if insecure.eq_ignore_ascii_case("true") {
                true
            } else {
                return Err(format!(
                "{SINK_TLS_INSECURE_SKIP_VERIFY_ENV} must be 'true' or 'false', got '{insecure}'"
            ));
            };
        let security = KafkaBrokerSecurity {
            sasl_mechanism: read(SINK_SASL_MECHANISM_ENV).map(|m| m.to_ascii_uppercase()),
            sasl_username: read(SINK_SASL_USERNAME_ENV),
            sasl_password: read(SINK_SASL_PASSWORD_ENV),
            tls_ca_file: read(SINK_TLS_CA_FILE_ENV),
            tls_ca_pem: None,
            tls_insecure_skip_verify,
        };
        security
            .validate(&protocol)
            .map_err(|e| format!("the SUTRA_SINK_KAFKA_* settings are invalid: {e}"))?;
        Ok(KafkaSinkSecurity { protocol, security })
    }
}

/// Keys librdkafka would accept through the `kafka.*` passthrough but that this module owns: they
/// hold secrets or decide trust, so they must not bypass the reference rule or the explicit
/// opt-out. Returns the offending key, prefix stripped.
pub(crate) fn passthrough_security_key(stripped_key: &str) -> Option<&str> {
    let owned = stripped_key.starts_with("sasl.")
        || stripped_key.starts_with("ssl.")
        || stripped_key.starts_with("ssl_")
        || stripped_key == "enable.ssl.certificate.verification";
    owned.then_some(stripped_key)
}

/// Which side of the transport a client serves, which picks its diagnostic codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientRole {
    Inbound,
    Outbound,
}

/// A librdkafka client context. Connection failures surface only as background errors, and a
/// producer's queued message would otherwise report nothing better than a delivery timeout, so this
/// records the last TLS or SASL failure and logs it under a stable code. librdkafka repeats the
/// error on every reconnect; each distinct failure is logged once. Clones share one record, so the
/// holder of a client can read what the client's own context saw.
#[derive(Clone)]
pub struct BrokerFailureRecorder {
    role: ClientRole,
    label: String,
    last: Arc<Mutex<Option<Diagnostic>>>,
}

impl BrokerFailureRecorder {
    pub(crate) fn new(role: ClientRole, label: impl Into<String>) -> BrokerFailureRecorder {
        BrokerFailureRecorder {
            role,
            label: label.into(),
            last: Arc::new(Mutex::new(None)),
        }
    }

    /// The last TLS or SASL failure recorded since the last [`clear`](Self::clear).
    pub(crate) fn last_failure(&self) -> Option<Diagnostic> {
        self.last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn clear(&self) {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn code_for(&self, failure: BrokerFailureKind) -> &'static str {
        use super::codes;
        match (self.role, failure) {
            (ClientRole::Inbound, BrokerFailureKind::Tls) => codes::INBOUND_BROKER_TLS_FAILED,
            (ClientRole::Inbound, BrokerFailureKind::Auth) => codes::INBOUND_BROKER_AUTH_FAILED,
            (ClientRole::Outbound, BrokerFailureKind::Tls) => codes::OUTBOUND_BROKER_TLS_FAILED,
            (ClientRole::Outbound, BrokerFailureKind::Auth) => codes::OUTBOUND_BROKER_AUTH_FAILED,
        }
    }
}

impl ClientContext for BrokerFailureRecorder {
    fn error(&self, error: KafkaError, reason: &str) {
        let Some(failure) = classify_broker_failure(&error, reason) else {
            warn!(client = %self.label, "librdkafka: {error}: {reason}");
            return;
        };
        let code = self.code_for(failure);
        let diagnostic = Diagnostic::error(code, format!("kafka {}: {reason}", self.label));
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let repeated = last
            .as_ref()
            .is_some_and(|d| d.message == diagnostic.message);
        if !repeated {
            warn!(client = %self.label, code, "{}", diagnostic.message);
        }
        *last = Some(diagnostic);
    }
}

impl ConsumerContext for BrokerFailureRecorder {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerFailureKind {
    Tls,
    Auth,
}

/// Sort a background client error. librdkafka reports some handshake failures under a generic
/// transport code, so the reason text is consulted too.
fn classify_broker_failure(error: &KafkaError, reason: &str) -> Option<BrokerFailureKind> {
    match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::SSL) => return Some(BrokerFailureKind::Tls),
        Some(
            RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::IllegalSASLState
            | RDKafkaErrorCode::UnsupportedSASLMechanism,
        ) => return Some(BrokerFailureKind::Auth),
        _ => {}
    }
    if reason.contains("SSL handshake failed") || reason.contains("certificate verify failed") {
        return Some(BrokerFailureKind::Tls);
    }
    if reason.contains("SASL authentication") {
        return Some(BrokerFailureKind::Auth);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sasl() -> KafkaBrokerSecurity {
        KafkaBrokerSecurity {
            sasl_mechanism: Some("SCRAM-SHA-256".to_string()),
            sasl_username: Some("svc-user".to_string()),
            sasl_password: Some("hunter2".to_string()),
            ..KafkaBrokerSecurity::default()
        }
    }

    fn config_of(protocol: &str, security: &KafkaBrokerSecurity) -> ClientConfig {
        let mut client = ClientConfig::new();
        security.apply_to(protocol, &mut client);
        client
    }

    #[test]
    fn plaintext_writes_nothing_beyond_the_protocol() {
        let client = config_of("PLAINTEXT", &KafkaBrokerSecurity::default());
        assert_eq!(client.get("security.protocol"), Some("PLAINTEXT"));
        assert_eq!(client.get("ssl.ca.location"), None);
        assert_eq!(client.get("enable.ssl.certificate.verification"), None);
        assert_eq!(client.get("sasl.mechanism"), None);
    }

    #[test]
    fn tls_verifies_certificate_and_host_name_by_default() {
        let client = config_of("SSL", &KafkaBrokerSecurity::default());
        assert_eq!(
            client.get("enable.ssl.certificate.verification"),
            Some("true")
        );
        assert_eq!(
            client.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert!(client.get("ssl.ca.location").is_some());
    }

    #[test]
    fn a_named_ca_replaces_the_platform_bundle() {
        let file = KafkaBrokerSecurity {
            tls_ca_file: Some("/etc/ca.pem".to_string()),
            ..KafkaBrokerSecurity::default()
        };
        assert_eq!(
            config_of("SSL", &file).get("ssl.ca.location"),
            Some("/etc/ca.pem")
        );
        let pem = KafkaBrokerSecurity {
            tls_ca_pem: Some("PEM".to_string()),
            ..KafkaBrokerSecurity::default()
        };
        let client = config_of("SSL", &pem);
        assert_eq!(client.get("ssl.ca.pem"), Some("PEM"));
        assert_eq!(client.get("ssl.ca.location"), None);
    }

    #[test]
    fn the_opt_out_turns_off_both_checks() {
        let security = KafkaBrokerSecurity {
            tls_insecure_skip_verify: true,
            ..KafkaBrokerSecurity::default()
        };
        let client = config_of("SSL", &security);
        assert_eq!(
            client.get("enable.ssl.certificate.verification"),
            Some("false")
        );
        assert_eq!(
            client.get("ssl.endpoint.identification.algorithm"),
            Some("none")
        );
    }

    #[test]
    fn sasl_settings_reach_the_client_and_stay_out_of_debug() {
        let security = sasl();
        let client = config_of("SASL_SSL", &security);
        assert_eq!(client.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(client.get("sasl.username"), Some("svc-user"));
        assert_eq!(client.get("sasl.password"), Some("hunter2"));
        let rendered = format!("{security:?}");
        assert!(!rendered.contains("hunter2") && !rendered.contains("svc-user"));
    }

    #[test]
    fn validation_refuses_what_could_not_take_effect() {
        let ok = sasl();
        assert!(ok.validate("SASL_SSL").is_ok());
        assert!(ok.validate("SASL_PLAINTEXT").is_ok());
        assert!(ok.validate("SSL").is_err(), "sasl.* without SASL");
        assert!(ok.validate("PLAINTEXT").is_err(), "sasl.* without SASL");

        let no_mechanism = KafkaBrokerSecurity {
            sasl_mechanism: None,
            ..sasl()
        };
        assert!(no_mechanism.validate("SASL_SSL").is_err());
        let gssapi = KafkaBrokerSecurity {
            sasl_mechanism: Some("GSSAPI".to_string()),
            ..sasl()
        };
        assert!(gssapi.validate("SASL_SSL").is_err());
        let no_password = KafkaBrokerSecurity {
            sasl_password: None,
            ..sasl()
        };
        assert!(no_password.validate("SASL_SSL").is_err());

        let ca = KafkaBrokerSecurity {
            tls_ca_file: Some("/etc/ca.pem".to_string()),
            ..KafkaBrokerSecurity::default()
        };
        assert!(ca.validate("SSL").is_ok());
        assert!(ca.validate("PLAINTEXT").is_err(), "CA without TLS");
        let two_cas = KafkaBrokerSecurity {
            tls_ca_pem: Some("PEM".to_string()),
            ..ca
        };
        assert!(two_cas.validate("SSL").is_err());
    }

    #[test]
    fn the_sink_environment_defaults_to_plaintext_and_refuses_a_typo() {
        assert_eq!(
            KafkaSinkSecurity::from_values(&BTreeMap::new()).expect("defaults"),
            KafkaSinkSecurity::default()
        );
        let mut values = BTreeMap::new();
        values.insert(SINK_SECURITY_PROTOCOL_ENV, "sasl_ssl".to_string());
        values.insert(SINK_SASL_MECHANISM_ENV, "scram-sha-512".to_string());
        values.insert(SINK_SASL_USERNAME_ENV, "u".to_string());
        values.insert(SINK_SASL_PASSWORD_ENV, "p".to_string());
        let read = KafkaSinkSecurity::from_values(&values).expect("sasl");
        assert_eq!(read.protocol, "SASL_SSL");
        assert_eq!(
            read.security.sasl_mechanism.as_deref(),
            Some("SCRAM-SHA-512")
        );

        values.insert(SINK_TLS_INSECURE_SKIP_VERIFY_ENV, "yes".to_string());
        assert!(KafkaSinkSecurity::from_values(&values).is_err());
        values.remove(SINK_TLS_INSECURE_SKIP_VERIFY_ENV);
        values.remove(SINK_SASL_PASSWORD_ENV);
        let err = KafkaSinkSecurity::from_values(&values).unwrap_err();
        assert!(err.contains("sasl.password"), "{err}");
    }

    #[test]
    fn broker_failures_sort_by_code_and_by_reason() {
        let auth = KafkaError::Global(RDKafkaErrorCode::Authentication);
        assert_eq!(
            classify_broker_failure(&auth, "invalid credentials"),
            Some(BrokerFailureKind::Auth)
        );
        let ssl = KafkaError::Global(RDKafkaErrorCode::SSL);
        assert_eq!(
            classify_broker_failure(&ssl, "anything"),
            Some(BrokerFailureKind::Tls)
        );
        let transport = KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure);
        assert_eq!(
            classify_broker_failure(
                &transport,
                "SSL handshake failed: certificate verify failed"
            ),
            Some(BrokerFailureKind::Tls)
        );
        assert_eq!(
            classify_broker_failure(&transport, "connection refused"),
            None
        );
    }

    #[test]
    fn the_recorder_keeps_the_last_security_failure_under_its_role_code() {
        let recorder = BrokerFailureRecorder::new(ClientRole::Outbound, "sink");
        recorder.error(
            KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure),
            "connection refused",
        );
        assert!(recorder.last_failure().is_none());
        recorder.error(
            KafkaError::Global(RDKafkaErrorCode::Authentication),
            "SASL authentication error",
        );
        assert_eq!(
            recorder.last_failure().expect("recorded").code,
            super::super::codes::OUTBOUND_BROKER_AUTH_FAILED
        );
        recorder.clear();
        assert!(recorder.last_failure().is_none());
    }

    #[test]
    fn passthrough_cannot_set_what_this_module_owns() {
        for key in [
            "sasl.password",
            "ssl.ca.location",
            "ssl_key",
            "enable.ssl.certificate.verification",
        ] {
            assert_eq!(passthrough_security_key(key), Some(key));
        }
        assert_eq!(passthrough_security_key("fetch.min.bytes"), None);
    }
}
