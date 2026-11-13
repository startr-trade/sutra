//! Native TLS and the one connection-open path both AMQP 1.0 directions share.
//!
//! The handshake is done here with `tokio-rustls` on rustls' ring provider, the stack every other
//! outbound client in the process uses, and the finished stream is handed to `fe2o3-amqp` as an
//! already-secured transport. The client's own `rustls` feature is deliberately left off: it would
//! pull the aws-lc provider and a compiled-in copy of the webpki roots, and it would make the trust
//! decision somewhere this crate cannot see.
//!
//! Trust follows the rest of the engine. With no CA property the broker is verified against the
//! process-wide native root store, which reads the platform bundle and honours `SSL_CERT_FILE`.
//! `tls-ca-file` or `tls-ca-pem` REPLACES that store with the named bundle, so a private CA does
//! not also trust every public one. Certificate and host-name verification are always on unless
//! `tls-insecure-skip-verify: true` says otherwise, and that setting logs a warning each time it
//! takes effect.

use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp::connection::{ConnectionHandle, OpenError};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::types::definitions::{AmqpError, ErrorCondition};
use fe2o3_amqp::Connection;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::warn;

/// How long a TCP connect plus TLS handshake may take. A peer that accepts the connection and then
/// says nothing (a plaintext port, a stalled proxy) must surface as a failure, not a hung task.
const TLS_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The authored trust settings for a TLS broker connection. Holds no secret: a CA is public
/// material, and `ca_pem` carries a reference until the wiring swaps in the resolved value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AmqpTlsProperties {
    /// `tls-ca-file`: path to a PEM bundle that replaces the native root store.
    pub ca_file: Option<String>,
    /// `tls-ca-pem`: a secret reference to PEM text (resolved before connect) that replaces the
    /// native root store.
    pub ca_pem: Option<String>,
    /// `tls-insecure-skip-verify`: accept any certificate and host name. Off unless set.
    pub insecure_skip_verify: bool,
}

impl AmqpTlsProperties {
    /// True when any trust setting is authored (they are meaningless on a plaintext connection).
    pub fn is_configured(&self) -> bool {
        self.ca_file.is_some() || self.ca_pem.is_some() || self.insecure_skip_verify
    }

    /// Build the rustls client configuration these settings describe. The error text names the
    /// file or setting at fault and carries no certificate material.
    pub(crate) fn client_config(&self) -> Result<rustls::ClientConfig, String> {
        if self.insecure_skip_verify {
            warn!(
                "amqp broker TLS verification is DISABLED (tls-insecure-skip-verify): any \
                 certificate and host name will be accepted"
            );
            return insecure_client_config();
        }
        if let Some(pem) = &self.ca_pem {
            return client_config_with_roots(pem.as_bytes(), "tls-ca-pem");
        }
        if let Some(path) = &self.ca_file {
            let pem = std::fs::read(path)
                .map_err(|e| format!("could not read tls-ca-file '{path}': {e}"))?;
            return client_config_with_roots(&pem, &format!("tls-ca-file '{path}'"));
        }
        sutra_channels::shared_client_tls_config()
            .map_err(|e| format!("could not load the native TLS root store: {e}"))
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    // Named explicitly: the build links rustls with more than one provider, and then rustls has no
    // implicit default.
    Arc::new(rustls::crypto::ring::default_provider())
}

fn client_config_with_roots(pem: &[u8], origin: &str) -> Result<rustls::ClientConfig, String> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        let certificate =
            certificate.map_err(|e| format!("{origin} is not a valid PEM bundle: {e}"))?;
        roots
            .add(certificate)
            .map_err(|e| format!("{origin} holds an unusable certificate: {e}"))?;
    }
    if roots.is_empty() {
        return Err(format!("{origin} holds no certificates"));
    }
    Ok(rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn insecure_client_config() -> Result<rustls::ClientConfig, String> {
    let provider = provider();
    let verifier = Arc::new(UnverifiedServerCertificate {
        provider: Arc::clone(&provider),
    });
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth())
}

/// The explicit opt-out: any certificate chain and name is accepted. The handshake signatures are
/// still checked, so the peer must hold the key of the certificate it presented.
#[derive(Debug)]
struct UnverifiedServerCertificate {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for UnverifiedServerCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Why a broker connection could not be opened, sorted so an operator sees what to fix. Every
/// variant's text is safe to log: it never carries a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AmqpConnectFailure {
    /// The trust settings could not be loaded, or the TLS handshake failed.
    Tls(String),
    /// The broker refused the SASL credentials.
    Auth(String),
    /// Anything else (unreachable, closed, protocol).
    Connection(String),
}

/// Where and how to connect. Credentials are resolved values at this point.
pub(crate) struct AmqpBrokerTarget<'a> {
    pub host: &'a str,
    pub port: u16,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}

/// Open a TLS broker connection: TCP, then the rustls handshake, then the AMQP open on top of the
/// secured stream, with SASL PLAIN when a username and password are both present.
pub(crate) async fn open_tls_connection(
    container_id: &str,
    target: &AmqpBrokerTarget<'_>,
    config: Arc<rustls::ClientConfig>,
) -> Result<ConnectionHandle<()>, AmqpConnectFailure> {
    let server_name = ServerName::try_from(target.host.to_string()).map_err(|_| {
        AmqpConnectFailure::Tls(format!("'{}' is not a valid TLS server name", target.host))
    })?;
    let tcp = tokio::time::timeout(
        TLS_CONNECT_TIMEOUT,
        TcpStream::connect((target.host, target.port)),
    )
    .await
    .map_err(|_| AmqpConnectFailure::Connection("timed out connecting".to_string()))?
    .map_err(|e| AmqpConnectFailure::Connection(e.to_string()))?;
    let stream = tokio::time::timeout(
        TLS_CONNECT_TIMEOUT,
        TlsConnector::from(config).connect(server_name, tcp),
    )
    .await
    .map_err(|_| AmqpConnectFailure::Tls("the TLS handshake timed out".to_string()))?
    .map_err(|e| AmqpConnectFailure::Tls(format!("the TLS handshake failed: {e}")))?;

    let mut builder = Connection::builder()
        .container_id(container_id.to_string())
        .hostname(Some(target.host))
        .sasl_hostname(Some(target.host));
    if let (Some(username), Some(password)) = (target.username, target.password) {
        builder = builder.sasl_profile(SaslProfile::Plain {
            username: username.to_string(),
            password: password.to_string(),
        });
    }
    builder
        .open_with_stream(stream)
        .await
        .map_err(classify_open_error)
}

/// Sort a client open failure. The SASL outcome's `additional_data` is broker-supplied and is left
/// out of the text, so nothing the broker echoes back can reach a log.
pub(crate) fn classify_open_error(error: OpenError) -> AmqpConnectFailure {
    match &error {
        OpenError::SaslError { code, .. } => AmqpConnectFailure::Auth(format!(
            "the broker refused the SASL credentials (outcome {code:?})"
        )),
        OpenError::RemoteClosedWithError(remote)
            if matches!(
                remote.condition,
                ErrorCondition::AmqpError(AmqpError::UnauthorizedAccess)
            ) =>
        {
            AmqpConnectFailure::Auth("the broker closed the connection as unauthorized".to_string())
        }
        _ => AmqpConnectFailure::Connection(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_properties_verify_against_the_native_roots() {
        let tls = AmqpTlsProperties::default();
        assert!(!tls.is_configured());
        assert!(tls.client_config().is_ok());
    }

    #[test]
    fn a_ca_bundle_that_holds_no_certificate_is_refused_not_ignored() {
        let tls = AmqpTlsProperties {
            ca_pem: Some("not a pem".to_string()),
            ..AmqpTlsProperties::default()
        };
        let message = tls.client_config().unwrap_err();
        assert!(message.contains("tls-ca-pem"), "{message}");
    }

    #[test]
    fn a_missing_ca_file_is_refused_and_named() {
        let tls = AmqpTlsProperties {
            ca_file: Some("/nonexistent/ca.pem".to_string()),
            ..AmqpTlsProperties::default()
        };
        assert!(tls
            .client_config()
            .unwrap_err()
            .contains("/nonexistent/ca.pem"));
    }

    #[test]
    fn a_sasl_refusal_is_an_auth_failure_and_omits_the_broker_data() {
        let failure = classify_open_error(OpenError::SaslError {
            code: fe2o3_amqp::types::sasl::SaslCode::Auth,
            additional_data: Some(b"echoed-hunter2".to_vec().into()),
        });
        match failure {
            AmqpConnectFailure::Auth(text) => assert!(!text.contains("hunter2"), "{text}"),
            other => panic!("expected Auth, got {other:?}"),
        }
    }
}
