//! Env-indirection helpers for configuration values — a thin re-export of the vendor-neutral
//! [`sutra_envref_spi`] SPI (domain-neutrality refactor). The concrete secret-ref schemes are:
//!
//! - `env:NAME` / `secret:KEY` — the neutral builtins, ALWAYS present (they live in the SPI);
//! - `vault:<mount>/<path>#<key>` / `aws-secrets:<secret-id>[#<json-key>]` — vendor schemes each
//!   supplied by a `sutra-envref-<vendor>` crate that `inventory::submit!`s an
//!   `EnvRefResolverEntry`. The neutral engine names NONE of them (no `vaultrs` / AWS SDK dep);
//!   `sutra-dist` (the composition root) force-links the bundled resolver crates, so their
//!   inventory registrations ship and `ResolverRegistry::with_builtins()` collects them.
//!
//! This module keeps the historical `crate::envref::…` / `sutra_engine::envref::…` call-site
//! path stable (config.rs / assembly.rs / otel.rs) while the implementation lives in the SPI.

pub use sutra_envref_spi::{
    has_env_token, resolve_placeholders, resolve_placeholders_with, resolve_value,
    resolve_value_with, EnvRefError, EnvRefResolverEntry, ResolverRegistry, SchemeResolver,
    DEFAULT_SECRETS_DIR, SECRETS_DIR_ENV,
};
