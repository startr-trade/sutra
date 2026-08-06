//! Azure Key Vault envref resolver — the `azure-kv:<vault-name>/<secret-name>[#<version>]`
//! scheme, a per-vendor crate that self-registers a
//! [`sutra_envref_spi::EnvRefResolverEntry`] via `inventory`, so `sutra-dist` (the composition
//! root) force-links it to bundle the `azure-kv:` scheme while the neutral `sutra-engine` library
//! never names an Azure SDK.
//!
//! The Azure Rust SDK risks pulling `native-tls`, which is exactly why this resolver was deferred —
//! the tree is rustls/ring ONLY. So there is NO vendor SDK here: the resolver speaks the Key Vault
//! REST API directly over `reqwest` with `rustls-tls` (the same line `sutra-cli` uses), acquiring
//! an AAD client-credentials token by hand.
//!
//! A service-principal's `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` come
//! fail-closed from the environment (mirror the vault resolver's `env_required`). The read runs on
//! a dedicated thread + current-thread runtime, so it is safe whether or not the caller is already
//! inside a tokio runtime (the deploy/assembly flip path runs on the sync actor thread — the same
//! pattern as the vault resolver).
//!
//! Real-Azure coverage is a follow-on IT (there is no OSS Key Vault emulator, so no Tier-2
//! container test lives here — unlike the vault crate's `vault_resolver_it.rs`): the resolver is
//! exercised against a live vault in a manual / follow-on integration test with a real service
//! principal, not in this crate's unit tests (which cover reference parsing + fail-closed-when-env-
//! unset only, with NO network).
#![forbid(unsafe_code)]

use sutra_envref_spi::{EnvRefError, EnvRefResolverEntry, SchemeResolver};

/// `azure-kv:<vault-name>/<secret-name>[#<version>]` → the Key Vault secret value.
pub struct AzureKeyVaultResolver;

impl SchemeResolver for AzureKeyVaultResolver {
    fn scheme(&self) -> &'static str {
        "azure-kv"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        azure_kv::resolve(body)
    }
}

// Self-registers as the `azure-kv:` envref resolver (inventory pull model) — force-linked by
// sutra-dist so this submission survives linker DCE and is collected by
// `ResolverRegistry::with_builtins()`.
inventory::submit! {
    EnvRefResolverEntry { scheme: "azure-kv", make: || Box::new(AzureKeyVaultResolver) }
}

mod azure_kv {
    use sutra_envref_spi::EnvRefError;

    /// Directory (tenant) the service principal lives in.
    pub const TENANT_ENV: &str = "AZURE_TENANT_ID";
    /// The service-principal application (client) id.
    pub const CLIENT_ID_ENV: &str = "AZURE_CLIENT_ID";
    /// The service-principal client secret.
    pub const CLIENT_SECRET_ENV: &str = "AZURE_CLIENT_SECRET";

    /// The Key Vault data-plane REST API version (the current GA line).
    const API_VERSION: &str = "7.4";
    /// The scope an AAD client-credentials token for the Key Vault data plane requests.
    const KEY_VAULT_SCOPE: &str = "https://vault.azure.net/.default";

    /// A parsed `azure-kv:<vault-name>/<secret-name>[#<version>]` reference.
    struct AzureKeyVaultReference {
        vault: String,
        secret: String,
        version: Option<String>,
    }

    impl AzureKeyVaultReference {
        fn parse(body: &str) -> Result<AzureKeyVaultReference, EnvRefError> {
            let malformed = || {
                EnvRefError(format!(
                    "secret-ref 'azure-kv:{body}' is malformed — expected \
                     azure-kv:<vault-name>/<secret-name>[#<version>]"
                ))
            };
            // The optional `#<version>` suffix (mirrors the vault/aws `#`-split shape).
            let (location, version) = match body.split_once('#') {
                Some((loc, ver)) => {
                    if ver.is_empty() {
                        return Err(malformed());
                    }
                    (loc, Some(ver.to_string()))
                }
                None => (body, None),
            };
            let (vault, secret) = location.split_once('/').ok_or_else(malformed)?;
            if vault.is_empty() || secret.is_empty() {
                return Err(malformed());
            }
            Ok(AzureKeyVaultReference {
                vault: vault.to_string(),
                secret: secret.to_string(),
                version,
            })
        }
    }

    /// Resolve an `azure-kv:` reference body to the secret value.
    pub fn resolve(body: &str) -> Result<String, EnvRefError> {
        let reference = AzureKeyVaultReference::parse(body)?;
        // Fail closed on any missing service-principal credential BEFORE any network call runs —
        // env resolution precedes token acquisition (the unit tests rely on this: no network).
        let tenant = env_required(TENANT_ENV, body)?;
        let client_id = env_required(CLIENT_ID_ENV, body)?;
        let client_secret = env_required(CLIENT_SECRET_ENV, body)?;
        fetch_blocking(&tenant, &client_id, &client_secret, &reference)
    }

    /// A required credential env var, fail-closed (mirrors vault's `env_required`): an unset /
    /// blank var errors naming itself, and no network call is attempted.
    fn env_required(name: &str, body: &str) -> Result<String, EnvRefError> {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| {
                EnvRefError(format!(
                    "secret-ref 'azure-kv:{body}' resolves to no value ({name} is not set)"
                ))
            })
    }

    /// Drive the async read on a dedicated thread + current-thread runtime (mirrors vault).
    fn fetch_blocking(
        tenant: &str,
        client_id: &str,
        client_secret: &str,
        reference: &AzureKeyVaultReference,
    ) -> Result<String, EnvRefError> {
        let (tenant, client_id, client_secret) = (
            tenant.to_string(),
            client_id.to_string(),
            client_secret.to_string(),
        );
        let (vault, secret, version) = (
            reference.vault.clone(),
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
                            EnvRefError(format!("azure-kv resolver could not start a runtime: {e}"))
                        })?;
                    runtime.block_on(fetch(
                        &tenant,
                        &client_id,
                        &client_secret,
                        &vault,
                        &secret,
                        version.as_deref(),
                    ))
                })
                .join()
                .map_err(|_| EnvRefError("azure-kv resolver thread panicked".to_string()))?
        })
    }

    async fn fetch(
        tenant: &str,
        client_id: &str,
        client_secret: &str,
        vault: &str,
        secret: &str,
        version: Option<&str>,
    ) -> Result<String, EnvRefError> {
        // rustls transport (the `reqwest` workspace line is default-features off + rustls-tls) —
        // the tree is rustls/ring ONLY, so no Azure SDK (which risks native-tls) is linked.
        let client = reqwest::Client::builder().build().map_err(|e| {
            EnvRefError(format!(
                "azure-kv resolver could not build an http client: {e}"
            ))
        })?;

        let token = acquire_token(&client, tenant, client_id, client_secret).await?;

        // GET https://<vault>.vault.azure.net/secrets/<secret>[/<version>]?api-version=7.4
        let secret_url = match version {
            Some(v) => format!(
                "https://{vault}.vault.azure.net/secrets/{secret}/{v}?api-version={API_VERSION}"
            ),
            None => format!(
                "https://{vault}.vault.azure.net/secrets/{secret}?api-version={API_VERSION}"
            ),
        };
        let resp = client
            .get(&secret_url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                EnvRefError(format!(
                    "azure-kv read of '{vault}/{secret}' failed \
                     (AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET): {e}"
                ))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(EnvRefError(format!(
                "azure-kv read of '{vault}/{secret}' returned HTTP {status}"
            )));
        }
        let body: serde_json::Value = resp.json().await.map_err(|e| {
            EnvRefError(format!(
                "azure-kv secret '{vault}/{secret}' response is not JSON: {e}"
            ))
        })?;
        match body.get("value") {
            Some(serde_json::Value::String(s)) => Ok(s.clone()),
            _ => Err(EnvRefError(format!(
                "azure-kv secret '{vault}/{secret}' response has no string 'value' field"
            ))),
        }
    }

    /// Acquire an AAD client-credentials token for the Key Vault data plane by hand (no SDK):
    /// POST the token endpoint with a `grant_type=client_credentials` form, read `access_token`.
    async fn acquire_token(
        client: &reqwest::Client,
        tenant: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<String, EnvRefError> {
        let token_url = format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token");
        let form = [
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", KEY_VAULT_SCOPE),
        ];
        let resp = client
            .post(&token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| EnvRefError(format!("azure-kv token acquisition failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(EnvRefError(format!(
                "azure-kv token acquisition returned HTTP {status} \
                 (check AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET)"
            )));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| EnvRefError(format!("azure-kv token response is not JSON: {e}")))?;
        match body.get("access_token") {
            Some(serde_json::Value::String(s)) => Ok(s.clone()),
            _ => Err(EnvRefError(
                "azure-kv token response has no 'access_token' field".to_string(),
            )),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_a_reference_without_a_version() {
            let r = AzureKeyVaultReference::parse("prod-vault/rabbit-password").unwrap();
            assert_eq!(r.vault, "prod-vault");
            assert_eq!(r.secret, "rabbit-password");
            assert_eq!(r.version, None);
        }

        #[test]
        fn parses_a_reference_with_a_version() {
            let r = AzureKeyVaultReference::parse("prod-vault/rabbit-password#abc123").unwrap();
            assert_eq!(r.vault, "prod-vault");
            assert_eq!(r.secret, "rabbit-password");
            assert_eq!(r.version.as_deref(), Some("abc123"));
        }

        #[test]
        fn rejects_malformed_references() {
            assert!(AzureKeyVaultReference::parse("").is_err());
            assert!(AzureKeyVaultReference::parse("no-slash").is_err());
            assert!(AzureKeyVaultReference::parse("/secret").is_err());
            assert!(AzureKeyVaultReference::parse("vault/").is_err());
            assert!(AzureKeyVaultReference::parse("vault/secret#").is_err());
        }

        #[test]
        fn fails_closed_when_azure_env_is_unset() {
            std::env::remove_var(TENANT_ENV);
            std::env::remove_var(CLIENT_ID_ENV);
            std::env::remove_var(CLIENT_SECRET_ENV);
            // Parses fine, then fails closed on the missing credential BEFORE any network call is
            // made (no network — env resolution precedes token acquisition).
            let e = resolve("prod-vault/rabbit-password").unwrap_err();
            assert!(e.0.contains("AZURE_TENANT_ID"), "{e}");
        }
    }
}
