//! GCP Secret Manager envref resolver — the `gcp-secret:<project>/<secret>[#<version>]` scheme
//! (the version defaults to `latest`); a per-vendor crate that self-registers a
//! [`sutra_envref_spi::EnvRefResolverEntry`] via `inventory`, so `sutra-dist` (the composition
//! root) force-links it to bundle the `gcp-secret:` scheme while the neutral `sutra-engine` library
//! never names a GCP SDK.
//!
//! The GCP Rust SDK (and google-cloud-auth) risk pulling `native-tls`/`openssl`, which is exactly
//! why this resolver was deferred — the tree is rustls/ring ONLY. So there is NO vendor SDK here:
//! the resolver speaks the Secret Manager REST API directly over `reqwest` with `rustls-tls` (the
//! same line `sutra-cli` uses), authenticating with a hand-built service-account OAuth2 flow.
//!
//! Auth (service account): `GOOGLE_APPLICATION_CREDENTIALS` (fail-closed) points at the SA JSON
//! key; the resolver signs an RS256 JWT (`iss`=client_email, `aud`=token_uri, scope=cloud-platform,
//! 1h exp) with the SA private key via `jsonwebtoken` (crypto backend `ring` — already in the tree,
//! rustls-clean), then exchanges it at the token endpoint (`grant_type=jwt-bearer`) for an access
//! token. The secret payload is base64-decoded (the workspace `base64` dep). The read runs on a
//! dedicated thread + current-thread runtime, so it is safe whether or not the caller is already
//! inside a tokio runtime (the deploy/assembly flip path runs on the sync actor thread — the same
//! pattern as the vault resolver).
//!
//! Real-GCP coverage is a follow-on IT (there is no OSS Secret Manager emulator, so no Tier-2
//! container test lives here — unlike the vault crate's `vault_resolver_it.rs`): the resolver is
//! exercised against live Secret Manager in a manual / follow-on integration test with a real
//! service account, not in this crate's unit tests (which cover reference parsing + fail-closed-
//! when-GOOGLE_APPLICATION_CREDENTIALS-unset only, with NO network — the JWT `iat`/`exp`
//! timestamps are non-deterministic, so signing is not unit-tested).
#![forbid(unsafe_code)]

use sutra_envref_spi::{EnvRefError, EnvRefResolverEntry, SchemeResolver};

/// `gcp-secret:<project>/<secret>[#<version>]` → the Secret Manager payload.
pub struct GcpSecretManagerResolver;

impl SchemeResolver for GcpSecretManagerResolver {
    fn scheme(&self) -> &'static str {
        "gcp-secret"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        gcp_secret::resolve(body)
    }
}

// Self-registers as the `gcp-secret:` envref resolver (inventory pull model) — force-linked by
// sutra-dist so this submission survives linker DCE and is collected by
// `ResolverRegistry::with_builtins()`.
inventory::submit! {
    EnvRefResolverEntry { scheme: "gcp-secret", make: || Box::new(GcpSecretManagerResolver) }
}

mod gcp_secret {
    use base64::Engine;
    use sutra_envref_spi::EnvRefError;

    /// Path to the service-account JSON key (the GCP ADC convention).
    pub const CREDENTIALS_ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";

    /// The default secret version when the ref omits `#<version>`.
    const DEFAULT_VERSION: &str = "latest";
    /// The OAuth2 scope the access token requests.
    const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

    /// A parsed `gcp-secret:<project>/<secret>[#<version>]` reference (version → `latest`).
    struct GcpSecretReference {
        project: String,
        secret: String,
        version: String,
    }

    impl GcpSecretReference {
        fn parse(body: &str) -> Result<GcpSecretReference, EnvRefError> {
            let malformed = || {
                EnvRefError(format!(
                    "secret-ref 'gcp-secret:{body}' is malformed — expected \
                     gcp-secret:<project>/<secret>[#<version>]"
                ))
            };
            // The optional `#<version>` suffix (mirrors the vault/aws `#`-split shape); absent → the
            // `latest` alias GCP resolves server-side.
            let (location, version) = match body.split_once('#') {
                Some((loc, ver)) => {
                    if ver.is_empty() {
                        return Err(malformed());
                    }
                    (loc, ver.to_string())
                }
                None => (body, DEFAULT_VERSION.to_string()),
            };
            let (project, secret) = location.split_once('/').ok_or_else(malformed)?;
            if project.is_empty() || secret.is_empty() {
                return Err(malformed());
            }
            Ok(GcpSecretReference {
                project: project.to_string(),
                secret: secret.to_string(),
                version,
            })
        }
    }

    /// The three fields the resolver needs out of the SA JSON key.
    struct ServiceAccount {
        client_email: String,
        private_key: String,
        token_uri: String,
    }

    /// Resolve a `gcp-secret:` reference body to the secret value.
    pub fn resolve(body: &str) -> Result<String, EnvRefError> {
        let reference = GcpSecretReference::parse(body)?;
        // Fail closed on the missing credentials path BEFORE any network call runs — env resolution
        // and JWT signing precede the token exchange (the unit tests rely on this: no network).
        let creds_path = env_required(CREDENTIALS_ENV, body)?;
        let service_account = load_service_account(&creds_path, body)?;
        let assertion = sign_jwt(&service_account, body)?;
        fetch_blocking(&service_account.token_uri, &assertion, &reference)
    }

    /// The credentials-path env var, fail-closed (mirrors vault's `env_required`): an unset / blank
    /// var errors naming itself, and no network call is attempted.
    fn env_required(name: &str, body: &str) -> Result<String, EnvRefError> {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| {
                EnvRefError(format!(
                    "secret-ref 'gcp-secret:{body}' resolves to no value ({name} is not set)"
                ))
            })
    }

    /// Read + parse the SA JSON key at `path`, plucking the three fields the OAuth2 flow needs.
    fn load_service_account(path: &str, body: &str) -> Result<ServiceAccount, EnvRefError> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            EnvRefError(format!(
                "secret-ref 'gcp-secret:{body}' resolves to no value (service-account key file \
                 '{path}' from {CREDENTIALS_ENV} is not readable: {e})"
            ))
        })?;
        let json: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
            EnvRefError(format!(
                "service-account key file '{path}' is not valid JSON: {e}"
            ))
        })?;
        let field = |name: &str| -> Result<String, EnvRefError> {
            json.get(name)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    EnvRefError(format!(
                        "service-account key file '{path}' has no string '{name}' field"
                    ))
                })
        };
        Ok(ServiceAccount {
            client_email: field("client_email")?,
            private_key: field("private_key")?,
            token_uri: field("token_uri")?,
        })
    }

    /// Sign an RS256 assertion JWT for the OAuth2 jwt-bearer flow with the SA private key. No
    /// network; the `iat`/`exp` timestamps are wall-clock (so this step is not unit-tested).
    fn sign_jwt(sa: &ServiceAccount, body: &str) -> Result<String, EnvRefError> {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .map_err(|e| EnvRefError(format!("gcp-secret resolver clock error: {e}")))?;
        // The jwt-bearer assertion claims (Google service-account flow): iss/scope/aud + iat/exp.
        let claims = serde_json::json!({
            "iss": sa.client_email,
            "scope": CLOUD_PLATFORM_SCOPE,
            "aud": sa.token_uri,
            "iat": now,
            "exp": now + 3600,
        });
        let key = EncodingKey::from_rsa_pem(sa.private_key.as_bytes()).map_err(|e| {
            EnvRefError(format!(
                "secret-ref 'gcp-secret:{body}' service-account private_key is not a valid RSA \
                 PEM: {e}"
            ))
        })?;
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| EnvRefError(format!("gcp-secret resolver could not sign the JWT: {e}")))
    }

    /// Drive the async read on a dedicated thread + current-thread runtime (mirrors vault).
    fn fetch_blocking(
        token_uri: &str,
        assertion: &str,
        reference: &GcpSecretReference,
    ) -> Result<String, EnvRefError> {
        let (token_uri, assertion) = (token_uri.to_string(), assertion.to_string());
        let (project, secret, version) = (
            reference.project.clone(),
            reference.secret.clone(),
            reference.version.clone(),
        );
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            EnvRefError(format!(
                                "gcp-secret resolver could not start a runtime: {e}"
                            ))
                        })?;
                    runtime.block_on(fetch(&token_uri, &assertion, &project, &secret, &version))
                })
                .join()
                .map_err(|_| EnvRefError("gcp-secret resolver thread panicked".to_string()))?
        })
    }

    async fn fetch(
        token_uri: &str,
        assertion: &str,
        project: &str,
        secret: &str,
        version: &str,
    ) -> Result<String, EnvRefError> {
        // rustls transport (the `reqwest` workspace line is default-features off + rustls-tls) —
        // the tree is rustls/ring ONLY, so no GCP SDK (which risks native-tls) is linked.
        let client = reqwest::Client::builder().build().map_err(|e| {
            EnvRefError(format!(
                "gcp-secret resolver could not build an http client: {e}"
            ))
        })?;

        let token = exchange_jwt(&client, token_uri, assertion).await?;

        // GET .../v1/projects/<project>/secrets/<secret>/versions/<version>:access
        let secret_url = format!(
            "https://secretmanager.googleapis.com/v1/projects/{project}/secrets/{secret}/versions/{version}:access"
        );
        let resp = client
            .get(&secret_url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                EnvRefError(format!(
                    "gcp-secret read of '{project}/{secret}' failed \
                     (GOOGLE_APPLICATION_CREDENTIALS): {e}"
                ))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(EnvRefError(format!(
                "gcp-secret read of '{project}/{secret}' returned HTTP {status}"
            )));
        }
        let payload: serde_json::Value = resp.json().await.map_err(|e| {
            EnvRefError(format!(
                "gcp-secret secret '{project}/{secret}' response is not JSON: {e}"
            ))
        })?;
        // The SecretPayload.data field is standard-base64-encoded octets.
        let data_b64 = payload
            .get("payload")
            .and_then(|p| p.get("data"))
            .and_then(|d| d.as_str())
            .ok_or_else(|| {
                EnvRefError(format!(
                    "gcp-secret secret '{project}/{secret}' response has no payload.data field"
                ))
            })?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data_b64.trim())
            .map_err(|e| {
                EnvRefError(format!(
                    "gcp-secret secret '{project}/{secret}' payload.data is not valid base64: {e}"
                ))
            })?;
        String::from_utf8(bytes).map_err(|e| {
            EnvRefError(format!(
                "gcp-secret secret '{project}/{secret}' payload is not valid UTF-8: {e}"
            ))
        })
    }

    /// Exchange the signed assertion JWT for an access token (OAuth2 jwt-bearer grant), reading
    /// `access_token` — the standard Google service-account token endpoint (the SA key's
    /// `token_uri`, canonically `https://oauth2.googleapis.com/token`).
    async fn exchange_jwt(
        client: &reqwest::Client,
        token_uri: &str,
        assertion: &str,
    ) -> Result<String, EnvRefError> {
        let form = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion),
        ];
        let resp = client
            .post(token_uri)
            .form(&form)
            .send()
            .await
            .map_err(|e| EnvRefError(format!("gcp-secret token exchange failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(EnvRefError(format!(
                "gcp-secret token exchange returned HTTP {status} \
                 (check GOOGLE_APPLICATION_CREDENTIALS)"
            )));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| EnvRefError(format!("gcp-secret token response is not JSON: {e}")))?;
        match body.get("access_token") {
            Some(serde_json::Value::String(s)) => Ok(s.clone()),
            _ => Err(EnvRefError(
                "gcp-secret token response has no 'access_token' field".to_string(),
            )),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_a_reference_defaulting_the_version_to_latest() {
            let r = GcpSecretReference::parse("my-project/rabbit-password").unwrap();
            assert_eq!(r.project, "my-project");
            assert_eq!(r.secret, "rabbit-password");
            assert_eq!(r.version, "latest");
        }

        #[test]
        fn parses_a_reference_with_an_explicit_version() {
            let r = GcpSecretReference::parse("my-project/rabbit-password#7").unwrap();
            assert_eq!(r.project, "my-project");
            assert_eq!(r.secret, "rabbit-password");
            assert_eq!(r.version, "7");
        }

        #[test]
        fn rejects_malformed_references() {
            assert!(GcpSecretReference::parse("").is_err());
            assert!(GcpSecretReference::parse("no-slash").is_err());
            assert!(GcpSecretReference::parse("/secret").is_err());
            assert!(GcpSecretReference::parse("project/").is_err());
            assert!(GcpSecretReference::parse("project/secret#").is_err());
        }

        #[test]
        fn fails_closed_when_google_credentials_env_is_unset() {
            std::env::remove_var(CREDENTIALS_ENV);
            // Parses fine, then fails closed on the missing credentials path BEFORE any network
            // call is made (no network — env resolution precedes token exchange).
            let e = resolve("my-project/rabbit-password").unwrap_err();
            assert!(e.0.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{e}");
        }
    }
}
