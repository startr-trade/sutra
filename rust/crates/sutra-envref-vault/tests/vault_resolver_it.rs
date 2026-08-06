//! The `vault:` envref resolver against a real HashiCorp Vault (KV-v2, dev
//! mode). Tier-2 (`#[ignore = "docker"]`): a `hashicorp/vault:1.17` dev-mode container,
//! reaper-registered; a secret is seeded via the KV-v2 API, then resolved through
//! `sutra_envref_spi::resolve_value("vault:<mount>/<path>#<key>")` with THIS vendor crate
//! linked (its `inventory::submit!` registers the `vault:` scheme). Moved here from
//! `sutra-engine` with the resolver (domain-neutrality refactor): the vendor IT lives with its
//! vendor crate, so the neutral engine names no vault client.

use std::collections::HashMap;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

use sutra_envref_spi::resolve_value;
// Force-link this crate so its `inventory::submit!` of the `vault:` EnvRefResolverEntry is
// collected by `ResolverRegistry::with_builtins()` (an integration test links the crate-under-
// test, but name it explicitly to make the dependency intent obvious).
use sutra_envref_vault as _;

/// Start a Vault dev-mode server; returns the mapped `http://127.0.0.1:<port>` address.
fn start_vault() -> (testcontainers::Container<GenericImage>, String) {
    // Blocking runner on a dedicated thread — never inside a tokio worker.
    std::thread::spawn(|| {
        let container = GenericImage::new("hashicorp/vault", "1.17")
            .with_exposed_port(8200.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Vault server started"))
            .with_env_var("VAULT_DEV_ROOT_TOKEN_ID", "root")
            .with_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
            .with_cmd(["server", "-dev"])
            .start()
            .expect("start hashicorp/vault:1.17 (docker required)");
        sutra_testkit::reap_on_exit(container.id());
        let port = container.get_host_port_ipv4(8200).expect("mapped 8200");
        (container, format!("http://127.0.0.1:{port}"))
    })
    .join()
    .expect("vault bootstrap thread")
}

/// Seed a KV-v2 secret at `secret/<path>` with the given key/value (dev mode mounts KV-v2
/// at `secret/`). Retries briefly — the API can lag the "server started" line.
fn seed_secret(addr: &str, path: &str, key: &str, value: &str) {
    use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};

    let addr = addr.to_string();
    let (path, key, value) = (path.to_string(), key.to_string(), value.to_string());
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let client = VaultClient::new(
                VaultClientSettingsBuilder::default()
                    .address(&addr)
                    .token("root")
                    .build()
                    .expect("settings"),
            )
            .expect("client");
            let mut data = HashMap::new();
            data.insert(key, value);
            let mut last_err = None;
            for _ in 0..20 {
                match vaultrs::kv2::set(&client, "secret", &path, &data).await {
                    Ok(_) => return,
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                }
            }
            panic!("vault seed failed after retries: {last_err:?}");
        });
    })
    .join()
    .expect("vault seed thread");
}

#[test]
#[ignore = "docker"]
fn vault_resolver_reads_a_kv2_secret() {
    let (_container, addr) = start_vault();
    seed_secret(&addr, "payments/rabbit", "password", "s3cr3t-broker-pw");

    std::env::set_var("SUTRA_VAULT_ADDR", &addr);
    std::env::set_var("SUTRA_VAULT_TOKEN", "root");

    // Whole-value form.
    let resolved =
        resolve_value("vault:secret/payments/rabbit#password").expect("vault resolves the secret");
    assert_eq!(resolved, "s3cr3t-broker-pw");

    // Placeholder form, embeddable mid-string (broker user-info shape).
    let embedded = resolve_value("amqp://svc:${vault:secret/payments/rabbit#password}@host:5672/q")
        .expect("vault resolves inside a placeholder");
    assert_eq!(embedded, "amqp://svc:s3cr3t-broker-pw@host:5672/q");

    // A missing key fails closed.
    assert!(resolve_value("vault:secret/payments/rabbit#nope").is_err());
    // A missing path fails closed.
    assert!(resolve_value("vault:secret/absent/path#password").is_err());
    // A placeholder default backs an unresolvable ref.
    assert_eq!(
        resolve_value("${vault:secret/absent/path#password:fallback}").unwrap(),
        "fallback"
    );

    std::env::remove_var("SUTRA_VAULT_ADDR");
    std::env::remove_var("SUTRA_VAULT_TOKEN");
}
