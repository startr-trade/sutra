//! HashiCorp Vault KV-v2 envref resolver — the `vault:<mount>/<path>#<key>` scheme,
//! extracted from the neutral engine into its own vendor crate (domain-neutrality refactor,
//! mirroring the per-vendor transport / codec crates). It self-registers a
//! [`sutra_envref_spi::EnvRefResolverEntry`] via `inventory`, so `sutra-dist` (the composition
//! root) force-links it to bundle the `vault:` scheme while the neutral `sutra-engine` library
//! never names `vaultrs`.
//!
//! Address and token come from `SUTRA_VAULT_ADDR` / `SUTRA_VAULT_TOKEN` (fail-closed when unset).
//! The read runs on a dedicated current-thread runtime, so it is safe whether or not the caller
//! is already inside a tokio runtime (the deploy/assembly flip path runs on the sync actor
//! thread). rustls transport (no openssl) to match the ring/rustls stack the rest of the tree
//! uses.
#![forbid(unsafe_code)]

use sutra_envref_spi::{EnvRefError, EnvRefResolverEntry, SchemeResolver};

/// `vault:<mount>/<path>#<key>` → the KV-v2 secret value.
pub struct VaultResolver;

impl SchemeResolver for VaultResolver {
    fn scheme(&self) -> &'static str {
        "vault"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        vault::resolve(body)
    }
}

// Self-registers as the `vault:` envref resolver (inventory pull model) — force-linked by
// sutra-dist so this submission survives linker DCE and is collected by
// `ResolverRegistry::with_builtins()`.
inventory::submit! {
    EnvRefResolverEntry { scheme: "vault", make: || Box::new(VaultResolver) }
}

mod vault {
    use sutra_envref_spi::EnvRefError;

    /// Vault server address (`http(s)://host:port`).
    pub const ADDR_ENV: &str = "SUTRA_VAULT_ADDR";
    /// Vault token authenticating the read.
    pub const TOKEN_ENV: &str = "SUTRA_VAULT_TOKEN";

    /// A parsed `vault:<mount>/<path>#<key>` reference.
    struct VaultReference {
        mount: String,
        path: String,
        key: String,
    }

    impl VaultReference {
        fn parse(body: &str) -> Result<VaultReference, EnvRefError> {
            let malformed = || {
                EnvRefError(format!(
                    "secret-ref 'vault:{body}' is malformed — expected \
                     vault:<mount>/<path>#<key>"
                ))
            };
            let (location, key) = body.split_once('#').ok_or_else(malformed)?;
            let (mount, path) = location.split_once('/').ok_or_else(malformed)?;
            if mount.is_empty() || path.is_empty() || key.is_empty() {
                return Err(malformed());
            }
            Ok(VaultReference {
                mount: mount.to_string(),
                path: path.to_string(),
                key: key.to_string(),
            })
        }
    }

    /// Resolve a `vault:` reference body to the secret value.
    pub fn resolve(body: &str) -> Result<String, EnvRefError> {
        let reference = VaultReference::parse(body)?;
        let addr = env_required(ADDR_ENV, body)?;
        let token = env_required(TOKEN_ENV, body)?;
        fetch_blocking(&addr, &token, &reference)
    }

    fn env_required(name: &str, body: &str) -> Result<String, EnvRefError> {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| {
                EnvRefError(format!(
                    "secret-ref 'vault:{body}' resolves to no value ({name} is not set)"
                ))
            })
    }

    /// Drive the async read on a dedicated thread + current-thread runtime.
    fn fetch_blocking(
        addr: &str,
        token: &str,
        reference: &VaultReference,
    ) -> Result<String, EnvRefError> {
        let (addr, token) = (addr.to_string(), token.to_string());
        let (mount, path, key) = (
            reference.mount.clone(),
            reference.path.clone(),
            reference.key.clone(),
        );
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            EnvRefError(format!("vault resolver could not start a runtime: {e}"))
                        })?;
                    runtime.block_on(fetch(&addr, &token, &mount, &path, &key))
                })
                .join()
                .map_err(|_| EnvRefError("vault resolver thread panicked".to_string()))?
        })
    }

    async fn fetch(
        addr: &str,
        token: &str,
        mount: &str,
        path: &str,
        key: &str,
    ) -> Result<String, EnvRefError> {
        use std::collections::HashMap;

        use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};

        let settings = VaultClientSettingsBuilder::default()
            .address(addr)
            .token(token)
            .build()
            .map_err(|e| EnvRefError(format!("vault client settings invalid: {e}")))?;
        let client = VaultClient::new(settings)
            .map_err(|e| EnvRefError(format!("vault client could not be built: {e}")))?;
        let data: HashMap<String, serde_json::Value> = vaultrs::kv2::read(&client, mount, path)
            .await
            .map_err(|e| {
                EnvRefError(format!(
                    "vault read of '{mount}/{path}' failed (SUTRA_VAULT_ADDR/TOKEN): {e}"
                ))
            })?;
        match data.get(key) {
            Some(serde_json::Value::String(s)) => Ok(s.clone()),
            Some(other) => Ok(other.to_string()),
            None => Err(EnvRefError(format!(
                "vault secret '{mount}/{path}' has no key '{key}'"
            ))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_a_well_formed_reference() {
            let r = VaultReference::parse("secret/payments/rabbit#password").unwrap();
            assert_eq!(r.mount, "secret");
            assert_eq!(r.path, "payments/rabbit");
            assert_eq!(r.key, "password");
        }

        #[test]
        fn rejects_malformed_references() {
            assert!(VaultReference::parse("no-hash").is_err());
            assert!(VaultReference::parse("secret/path").is_err());
            assert!(VaultReference::parse("nomount#key").is_err());
            assert!(VaultReference::parse("secret/path#").is_err());
            assert!(VaultReference::parse("#key").is_err());
        }

        #[test]
        fn fails_closed_when_vault_env_is_unset() {
            std::env::remove_var(ADDR_ENV);
            std::env::remove_var(TOKEN_ENV);
            let e = resolve("secret/x/y#k").unwrap_err();
            assert!(e.0.contains("SUTRA_VAULT_ADDR"), "{e}");
        }
    }
}
