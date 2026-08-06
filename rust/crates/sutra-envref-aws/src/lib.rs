//! AWS Secrets Manager envref resolver — the `aws-secrets:<secret-id>[#<json-key>]` scheme,
//! extracted from the neutral engine into its own vendor crate (domain-neutrality
//! refactor, mirroring the per-vendor transport / codec crates). It self-registers a
//! [`sutra_envref_spi::EnvRefResolverEntry`] via `inventory`, so `sutra-dist` (the composition
//! root) force-links it to bundle the `aws-secrets:` scheme while the neutral `sutra-engine`
//! library never names the AWS SDK.
//!
//! The region comes fail-closed from `AWS_REGION` / `AWS_DEFAULT_REGION`; credentials come from
//! the AWS default chain (env vars / IRSA / pod-identity). Bare `aws-secrets:<secret-id>` returns
//! the whole `SecretString`; a `#<json-key>` suffix parses the SecretString as JSON and plucks
//! that field (error if absent). The read runs on a dedicated current-thread runtime, so it is
//! safe whether or not the caller is already inside a tokio runtime (the deploy/assembly flip
//! path runs on the sync actor thread — the same pattern as the vault resolver). TLS is pinned to
//! the ring rustls smithy HTTP client the SQS transport uses (the tree is rustls/ring ONLY).
//!
//! Real-AWS / LocalStack coverage is a follow-on: a `#[ignore = "docker"]` Tier-2 IT that seeds a
//! secret in a LocalStack `secretsmanager` container, then resolves it through
//! `sutra_envref_spi::resolve_value("aws-secrets:<id>#<key>")` (mirroring the vault crate's IT).
#![forbid(unsafe_code)]

use sutra_envref_spi::{EnvRefError, EnvRefResolverEntry, SchemeResolver};

/// `aws-secrets:<secret-id>[#<json-key>]` → the AWS Secrets Manager secret value.
pub struct AwsSecretsResolver;

impl SchemeResolver for AwsSecretsResolver {
    fn scheme(&self) -> &'static str {
        "aws-secrets"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        aws_secrets::resolve(body)
    }
}

// Self-registers as the `aws-secrets:` envref resolver (inventory pull model) — force-linked by
// sutra-dist so this submission survives linker DCE and is collected by
// `ResolverRegistry::with_builtins()`.
inventory::submit! {
    EnvRefResolverEntry { scheme: "aws-secrets", make: || Box::new(AwsSecretsResolver) }
}

mod aws_secrets {
    use sutra_envref_spi::EnvRefError;

    /// Primary region env var.
    pub const REGION_ENV: &str = "AWS_REGION";
    /// Fallback region env var (the AWS SDK convention: `AWS_REGION` then `AWS_DEFAULT_REGION`).
    pub const REGION_FALLBACK_ENV: &str = "AWS_DEFAULT_REGION";

    /// A parsed `aws-secrets:<secret-id>[#<json-key>]` reference.
    struct AwsSecretReference {
        secret_id: String,
        json_key: Option<String>,
    }

    impl AwsSecretReference {
        fn parse(body: &str) -> Result<AwsSecretReference, EnvRefError> {
            let malformed = || {
                EnvRefError(format!(
                    "secret-ref 'aws-secrets:{body}' is malformed — expected \
                     aws-secrets:<secret-id>[#<json-key>]"
                ))
            };
            // Reuse the vault `#`-suffix split shape, but the JSON key is OPTIONAL for AWS.
            let (secret_id, json_key) = match body.split_once('#') {
                Some((id, key)) => {
                    if key.is_empty() {
                        return Err(malformed());
                    }
                    (id, Some(key.to_string()))
                }
                None => (body, None),
            };
            if secret_id.is_empty() {
                return Err(malformed());
            }
            Ok(AwsSecretReference {
                secret_id: secret_id.to_string(),
                json_key,
            })
        }
    }

    /// Resolve an `aws-secrets:` reference body to the secret value.
    pub fn resolve(body: &str) -> Result<String, EnvRefError> {
        let reference = AwsSecretReference::parse(body)?;
        let region = region_required(body)?;
        fetch_blocking(&region, &reference)
    }

    /// Region, fail-closed (mirrors vault's `env_required`): `AWS_REGION`, else
    /// `AWS_DEFAULT_REGION`, else an error naming the env var.
    fn region_required(body: &str) -> Result<String, EnvRefError> {
        for name in [REGION_ENV, REGION_FALLBACK_ENV] {
            if let Some(v) = std::env::var(name).ok().filter(|v| !v.trim().is_empty()) {
                return Ok(v);
            }
        }
        Err(EnvRefError(format!(
            "secret-ref 'aws-secrets:{body}' resolves to no value \
             ({REGION_ENV} / {REGION_FALLBACK_ENV} is not set)"
        )))
    }

    /// Drive the async read on a dedicated thread + current-thread runtime (mirrors vault).
    fn fetch_blocking(region: &str, reference: &AwsSecretReference) -> Result<String, EnvRefError> {
        let region = region.to_string();
        let (secret_id, json_key) = (reference.secret_id.clone(), reference.json_key.clone());
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            EnvRefError(format!(
                                "aws-secrets resolver could not start a runtime: {e}"
                            ))
                        })?;
                    runtime.block_on(fetch(&region, &secret_id, json_key.as_deref()))
                })
                .join()
                .map_err(|_| EnvRefError("aws-secrets resolver thread panicked".to_string()))?
        })
    }

    async fn fetch(
        region: &str,
        secret_id: &str,
        json_key: Option<&str>,
    ) -> Result<String, EnvRefError> {
        use aws_config::BehaviorVersion;
        use aws_sdk_secretsmanager::config::Region;

        // The SAME ring rustls smithy HTTP client the SQS transport pins — the tree is
        // rustls/ring ONLY (no native-tls / aws-lc-rs). Setting it explicitly also gives the
        // SDK a connector under `default-features = false` (no default-https-client feature).
        let http = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();

        let cfg = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .http_client(http)
            .load()
            .await;
        let client = aws_sdk_secretsmanager::Client::new(&cfg);
        let output = client
            .get_secret_value()
            .secret_id(secret_id)
            .send()
            .await
            .map_err(|e| {
                EnvRefError(format!(
                    "aws-secrets read of '{secret_id}' failed (AWS_REGION/credentials): {e}"
                ))
            })?;
        let secret = output.secret_string().ok_or_else(|| {
            EnvRefError(format!(
                "aws-secrets secret '{secret_id}' has no string value \
                 (binary secrets are unsupported)"
            ))
        })?;
        match json_key {
            None => Ok(secret.to_string()),
            Some(key) => {
                let json: serde_json::Value = serde_json::from_str(secret).map_err(|e| {
                    EnvRefError(format!(
                        "aws-secrets secret '{secret_id}' is not JSON but a '#{key}' key was \
                         requested: {e}"
                    ))
                })?;
                match json.get(key) {
                    Some(serde_json::Value::String(s)) => Ok(s.clone()),
                    Some(other) => Ok(other.to_string()),
                    None => Err(EnvRefError(format!(
                        "aws-secrets secret '{secret_id}' has no key '{key}'"
                    ))),
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_a_secret_id_with_a_json_key() {
            let r = AwsSecretReference::parse("prod/payments/rabbit#password").unwrap();
            assert_eq!(r.secret_id, "prod/payments/rabbit");
            assert_eq!(r.json_key.as_deref(), Some("password"));
        }

        #[test]
        fn parses_a_bare_secret_id() {
            let r = AwsSecretReference::parse("prod/payments/rabbit").unwrap();
            assert_eq!(r.secret_id, "prod/payments/rabbit");
            assert_eq!(r.json_key, None);
        }

        #[test]
        fn rejects_malformed_references() {
            assert!(AwsSecretReference::parse("").is_err());
            assert!(AwsSecretReference::parse("#key").is_err());
            assert!(AwsSecretReference::parse("id#").is_err());
        }

        #[test]
        fn fails_closed_when_aws_region_is_unset() {
            std::env::remove_var(REGION_ENV);
            std::env::remove_var(REGION_FALLBACK_ENV);
            // Parses fine, then fails closed on the missing region BEFORE any AWS call is made
            // (no network — region resolution precedes client construction).
            let e = resolve("prod/some-secret#password").unwrap_err();
            assert!(e.0.contains("AWS_REGION"), "{e}");
        }
    }
}
