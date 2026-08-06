//! The secret-reference (**envref**) SPI + the neutral builtins — the vendor-neutral seam the
//! per-vendor `sutra-envref-<vendor>` resolver crates (vault, aws) self-register through, so
//! `sutra-engine` resolves configuration secret-refs GENERICALLY and names no vendor
//! (domain-neutrality refactor, mirroring the transport / codec SPIs).
//!
//! Env-indirection helpers for configuration values — the forms the resource YAMLs and `sutra.*`
//! config use (15-factor: no literal secrets in mounted files):
//!
//! - **secret-refs**: a whole value of the form `env:NAME` resolves to that environment
//!   variable (secret-ref resolution — fail-closed when unset);
//! - **file-backed secret-refs**: a whole value of the form `secret:KEY` resolves to the
//!   contents of `<secrets-dir>/KEY` (the R14 file-backed scheme — a k8s Secret volume-mounted
//!   at `SUTRA_SECRETS_DIR`, default `/etc/sutra/secrets`; the trailing newline is trimmed);
//! - **vendor secret-refs** (`vault:…`, `aws-secrets:…`): resolved by a [`SchemeResolver`] a
//!   vendor crate `inventory::submit!`s as an [`EnvRefResolverEntry`] — this crate names NONE of
//!   them; `sutra-dist` (the composition root) force-links the bundled ones;
//! - **placeholders**: `${NAME}` / `${NAME:default}` interpolate an environment variable and
//!   `${secret:KEY}` / `${secret:KEY:default}` interpolate a file-backed secret — the same
//!   schemes as the whole-value forms, embeddable mid-string (the interpolation style the
//!   channel YAMLs use for broker user-info: `rabbitmq://${secret:USER}:${secret:PASS}@host`).
//!
//! The file-backed scheme exists per R14 (secrets amendment): per-deployment credentials must
//! not become pod env vars (env is pod-spec-immutable — a first deploy would force a rolling
//! restart and rotation never reaches a running pod), so the estate credentials are delivered
//! as a live-synced Secret volume and referenced as `secret:KEY`. An unresolvable `secret:`
//! ref is byte-for-byte the same failure a `env:` ref gives on an unset variable — the caller
//! (assembly / pool creation) aborts the two-phase flip with old state intact and the
//! deployments watcher retries next tick (the deploy.rs flip-abort semantics), so a
//! deploy-before-secret ordering slip self-heals on the next sync.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

/// The conventional in-container mount point of the estate Secret volume — where `secret:KEY`
/// refs resolve from when `SUTRA_SECRETS_DIR` is not set (R14 shared-instance tofu mounts the
/// Secret here, defaultMode 0400 / runAsNonRoot).
pub const DEFAULT_SECRETS_DIR: &str = "/etc/sutra/secrets";

/// The env var naming the mounted secrets directory (overrides [`DEFAULT_SECRETS_DIR`]).
pub const SECRETS_DIR_ENV: &str = "SUTRA_SECRETS_DIR";

/// A failed env indirection — an unset variable, or an unreadable file-backed secret, with no
/// default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvRefError(pub String);

impl fmt::Display for EnvRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EnvRefError {}

/// Resolve a configuration value: a `<scheme>:<body>` secret-ref (`env:NAME`, `secret:KEY`,
/// `vault:<mount>/<path>#<key>`, `aws-secrets:<secret-id>[#<json-key>]`) resolves whole through
/// the [`ResolverRegistry`]; anything else goes through [`resolve_placeholders`]. The one helper
/// channel/datastore YAML values route through. The fixed `env:`/`secret:` prefixes were
/// generalised into a scheme→resolver registry so a new scheme is a drop-in self-registered
/// [`EnvRefResolverEntry`] and needs no change here.
pub fn resolve_value(value: &str) -> Result<String, EnvRefError> {
    resolve_value_with(value, default_registry())
}

/// [`resolve_value`] against an explicit registry (the test/extension seam).
pub fn resolve_value_with(value: &str, registry: &ResolverRegistry) -> Result<String, EnvRefError> {
    if let Some((scheme, body)) = split_scheme(value) {
        if let Some(resolver) = registry.get(scheme) {
            return resolver.resolve(body);
        }
    }
    resolve_placeholders_with(value, registry)
}

/// `true` when `value` contains at least one `${…}` placeholder token — the
/// `SecretRefs.hasEnvToken` detector (a value with no token needs no substitution).
pub fn has_env_token(value: &str) -> bool {
    value.contains("${")
}

/// Interpolate `${…}` placeholders. Two schemes, mirroring the whole-value forms:
/// `${secret:KEY}` / `${secret:KEY:default}` reads the file-backed secret `KEY`, and a plain
/// `${NAME}` / `${NAME:default}` reads the environment variable `NAME`. An unset variable /
/// unreadable secret with no default fails closed; text without placeholders passes through.
pub fn resolve_placeholders(value: &str) -> Result<String, EnvRefError> {
    resolve_placeholders_with(value, default_registry())
}

/// [`resolve_placeholders`] against an explicit registry.
pub fn resolve_placeholders_with(
    value: &str,
    registry: &ResolverRegistry,
) -> Result<String, EnvRefError> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(EnvRefError(format!(
                "unterminated ${{…}} placeholder in '{value}'"
            )));
        };
        let inner = &after[..end];
        out.push_str(&resolve_placeholder_token(inner, registry)?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Resolve one `${…}` token's inner text. A registered `<scheme>:` prefix (`secret:`,
/// `vault:`, …) selects that resolver; otherwise the token names an environment variable.
/// Either form accepts a `:default` suffix used when the underlying value is absent.
fn resolve_placeholder_token(
    inner: &str,
    registry: &ResolverRegistry,
) -> Result<String, EnvRefError> {
    if let Some((scheme, rest)) = split_scheme(inner) {
        if let Some(resolver) = registry.get(scheme) {
            // `${secret:KEY:default}` / `${vault:m/p#k:default}` — split the default off the
            // resolver body (k8s Secret keys match [-._a-zA-Z0-9]+, so a `:` after the body
            // unambiguously starts the default).
            let (body, default) = split_default(rest);
            return match resolver.resolve(body) {
                Ok(v) => Ok(v),
                Err(e) => match default {
                    Some(d) => Ok(d.to_string()),
                    None => Err(e),
                },
            };
        }
    }
    let (name, default) = split_default(inner);
    match std::env::var(name) {
        Ok(v) => Ok(v),
        Err(_) => match default {
            Some(d) => Ok(d.to_string()),
            None => Err(EnvRefError(format!(
                "placeholder '${{{name}}}' resolves to no value (environment variable \
                 '{name}' is not set and no default is given)"
            ))),
        },
    }
}

/// Split a placeholder body into `(key, default)` at the first `:` (the `${NAME:default}`
/// grammar; `None` default when there is no `:`).
fn split_default(body: &str) -> (&str, Option<&str>) {
    match body.split_once(':') {
        Some((k, d)) => (k, Some(d)),
        None => (body, None),
    }
}

/// Split a value into `(scheme, body)` when it leads with a lowercase-ASCII URI scheme token
/// followed by `:` — the shape a scheme-resolver ref takes (`env:X`, `secret:K`,
/// `vault:m/p#k`, `aws-secrets:id#key`). The scheme token is lowercase-ASCII letters plus
/// internal hyphens (the `-` in `aws-secrets`, RFC-3986-legal in a scheme); a value with no
/// leading scheme (`plain`, `${…}`, `DB_USER`) or an uppercase/non-scheme prefix returns
/// `None`, so a `rabbitmq://…` URI or an env-var name is never mistaken for a resolver ref.
fn split_scheme(value: &str) -> Option<(&str, &str)> {
    let (scheme, body) = value.split_once(':')?;
    if scheme.is_empty() || !scheme.bytes().all(|b| b.is_ascii_lowercase() || b == b'-') {
        return None;
    }
    Some((scheme, body))
}

// ===== scheme→resolver registry =============================================================

/// One secret-reference scheme resolver — resolves the `body` of a `<scheme>:<body>` ref.
/// Implementations MUST fail closed (return `Err`) on any resolution failure so the caller
/// (assembly / pool creation) aborts the two-phase flip with old state intact.
pub trait SchemeResolver: Send + Sync {
    /// The lowercase scheme this resolver claims (e.g. `"vault"`).
    fn scheme(&self) -> &'static str;

    /// Resolve the body — the text after `<scheme>:` (default suffixes are stripped by the
    /// placeholder layer before this is called).
    fn resolve(&self, body: &str) -> Result<String, EnvRefError>;
}

/// A vendor resolver's self-registration: its `<scheme>:` discriminator + a constructor. Each
/// `sutra-envref-<vendor>` crate `inventory::submit!`s exactly one next to its
/// [`SchemeResolver`] impl, so IMPLEMENTING a vendor resolver IS registering it — there is no
/// central push-list to forget (the same inventory pull model the transports/codecs use). The
/// `make` fn-pointer keeps the submitted static `Sync`; the `Box` is built at registration time.
/// The neutral engine names NO vendor crate; `sutra-dist` force-links the bundled ones so their
/// submissions survive linker DCE and are collected by [`ResolverRegistry::with_builtins`].
pub struct EnvRefResolverEntry {
    /// The `<scheme>:` value this resolver claims — matches its [`SchemeResolver::scheme`].
    pub scheme: &'static str,
    /// Construct a fresh boxed resolver (fn-pointer, so the inventory static stays `Sync`).
    pub make: fn() -> Box<dyn SchemeResolver>,
}

inventory::collect!(EnvRefResolverEntry);

/// scheme → [`SchemeResolver`] registry. The neutral built-ins are `env:` + `secret:`, always
/// present ([`ResolverRegistry::with_builtins`]); every vendor scheme (`vault:`, `aws-secrets:`,
/// Azure later — deferred) is a self-registered [`EnvRefResolverEntry`] collected from inventory,
/// so the resolution path never changes and this crate depends on no vendor SDK.
#[derive(Clone, Default)]
pub struct ResolverRegistry {
    by_scheme: BTreeMap<&'static str, Arc<dyn SchemeResolver>>,
}

impl ResolverRegistry {
    pub fn new() -> ResolverRegistry {
        ResolverRegistry::default()
    }

    /// The default set: the neutral `env:` (environment variable) + `secret:` (file-backed R14
    /// mount), always registered, PLUS every vendor resolver a `sutra-envref-<vendor>` crate
    /// self-registered via [`EnvRefResolverEntry`] and that the final binary force-links (`vault:`,
    /// `aws-secrets:`). Vendor entries are registered in scheme order (deterministic).
    pub fn with_builtins() -> ResolverRegistry {
        let mut registry = ResolverRegistry::new();
        registry.register(Arc::new(EnvResolver));
        registry.register(Arc::new(SecretFileResolver));
        let mut entries: Vec<&EnvRefResolverEntry> =
            inventory::iter::<EnvRefResolverEntry>().collect();
        entries.sort_by_key(|e| e.scheme);
        for entry in entries {
            registry.register(Arc::from((entry.make)()));
        }
        registry
    }

    /// Register `resolver` under its claimed scheme (last registration of a scheme wins).
    pub fn register(&mut self, resolver: Arc<dyn SchemeResolver>) -> &mut ResolverRegistry {
        self.by_scheme.insert(resolver.scheme(), resolver);
        self
    }

    /// The resolver for `scheme`, if any.
    pub fn get(&self, scheme: &str) -> Option<&Arc<dyn SchemeResolver>> {
        self.by_scheme.get(scheme)
    }

    /// Every registered scheme, sorted (diagnostics).
    pub fn schemes(&self) -> Vec<&'static str> {
        self.by_scheme.keys().copied().collect()
    }
}

/// The process-global default registry (the neutral `env:`/`secret:` + every force-linked
/// vendor resolver collected from inventory).
fn default_registry() -> &'static ResolverRegistry {
    static REGISTRY: OnceLock<ResolverRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ResolverRegistry::with_builtins)
}

/// `env:NAME` → the environment variable `NAME` (fail-closed when unset).
struct EnvResolver;
impl SchemeResolver for EnvResolver {
    fn scheme(&self) -> &'static str {
        "env"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        std::env::var(body).map_err(|_| {
            EnvRefError(format!(
                "secret-ref 'env:{body}' resolves to no value (environment variable '{body}' \
                 is not set)"
            ))
        })
    }
}

/// `secret:KEY` → the contents of `<secrets-dir>/KEY` (the R14 file-backed scheme).
struct SecretFileResolver;
impl SchemeResolver for SecretFileResolver {
    fn scheme(&self) -> &'static str {
        "secret"
    }
    fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
        read_secret(body)
    }
}

/// The mounted secrets directory — `SUTRA_SECRETS_DIR` or [`DEFAULT_SECRETS_DIR`].
fn secrets_dir() -> PathBuf {
    std::env::var(SECRETS_DIR_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SECRETS_DIR))
}

/// Read `secret:KEY` from `<secrets-dir>/KEY`, trimming a single trailing newline (LF or
/// CRLF — k8s / `echo`-created secret files usually carry one). Unreadable (missing file,
/// unreadable mount, absent directory) fails closed with the SAME shape an unset `env:` ref
/// gives — this is what drives the flip-abort/retry: assembly propagates the `Err`, the flip
/// aborts with old state intact, the watcher retries next tick.
fn read_secret(key: &str) -> Result<String, EnvRefError> {
    // A path-traversing key (`../…`, absolute) must never escape the mount — a mounted Secret's
    // keys are flat file names. Refuse anything that isn't a single path component.
    if key.is_empty() || key.contains('/') || key.contains('\\') {
        return Err(EnvRefError(format!(
            "secret-ref 'secret:{key}' names an invalid secret key (a flat secret file name, \
             no path separators)"
        )));
    }
    let path = secrets_dir().join(key);
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(trim_trailing_newline(&s).to_string()),
        Err(_) => Err(EnvRefError(format!(
            "secret-ref 'secret:{key}' resolves to no value (secret file '{}' is not readable — \
             mount the estate Secret at {SECRETS_DIR_ENV} or provide the key)",
            path.display()
        ))),
    }
}

/// Trim one trailing newline (`\n` or `\r\n`) — the conventional shape of a mounted secret file.
fn trim_trailing_newline(s: &str) -> &str {
    let s = s.strip_suffix('\n').unwrap_or(s);
    s.strip_suffix('\r').unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_ref_and_placeholders() {
        std::env::set_var("SUTRA_ENVREF_TEST_A", "hello");
        assert_eq!(resolve_value("env:SUTRA_ENVREF_TEST_A").unwrap(), "hello");
        assert_eq!(
            resolve_value("x-${SUTRA_ENVREF_TEST_A}-y").unwrap(),
            "x-hello-y"
        );
        assert_eq!(
            resolve_value("${SUTRA_ENVREF_TEST_UNSET:fallback}").unwrap(),
            "fallback"
        );
        assert!(resolve_value("env:SUTRA_ENVREF_TEST_UNSET").is_err());
        assert!(resolve_value("${SUTRA_ENVREF_TEST_UNSET}").is_err());
        assert_eq!(resolve_value("plain").unwrap(), "plain");
    }

    // ===== secret-ref edge cases =====

    #[test]
    fn env_value_beats_the_default() {
        std::env::set_var("SUTRA_ENVREF_TEST_BEATS", "real");
        assert_eq!(
            resolve_value("${SUTRA_ENVREF_TEST_BEATS:def}").unwrap(),
            "real"
        );
    }

    #[test]
    fn an_empty_default_resolves_to_empty_string() {
        assert_eq!(
            resolve_value("${SUTRA_ENVREF_TEST_EMPTY_MISSING:}").unwrap(),
            ""
        );
    }

    #[test]
    fn has_env_token_detects_presence_of_a_token() {
        assert!(has_env_token("${X}"));
        assert!(has_env_token("rabbitmq://${U}:${P}@host"));
        assert!(!has_env_token("plain"));
        assert!(!has_env_token(""));
    }

    #[test]
    fn returns_input_unchanged_when_it_has_no_tokens() {
        assert_eq!(
            resolve_value("rabbitmq://rabbitmq:5672/q").unwrap(),
            "rabbitmq://rabbitmq:5672/q"
        );
    }

    // ===== R14 file-backed secret: scheme =====================================================
    //
    // These serialise on the shared `SUTRA_SECRETS_DIR` env var, so they run inside one test to
    // avoid racing the process-global environment (the same discipline the env-ref tests above
    // use for their variables).

    #[test]
    fn secret_scheme_resolves_from_the_mounted_dir_and_fails_closed_like_an_unset_env_ref() {
        let dir =
            std::env::temp_dir().join(format!("sutra-envref-secret-{}-a", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // A trailing newline (the k8s / echo convention) is trimmed.
        std::fs::write(dir.join("DB_PASSWORD"), "s3cr3t\n").unwrap();
        // A value with no trailing newline is returned verbatim.
        std::fs::write(dir.join("DB_USER"), "svc").unwrap();
        std::env::set_var(SECRETS_DIR_ENV, &dir);

        // Whole-value form.
        assert_eq!(resolve_value("secret:DB_PASSWORD").unwrap(), "s3cr3t");
        assert_eq!(resolve_value("secret:DB_USER").unwrap(), "svc");

        // Placeholder form, embeddable mid-string (broker user-info shape).
        assert_eq!(
            resolve_value("rabbitmq://${secret:DB_USER}:${secret:DB_PASSWORD}@host:5672/q")
                .unwrap(),
            "rabbitmq://svc:s3cr3t@host:5672/q"
        );

        // A default backs an absent secret in the placeholder form.
        assert_eq!(
            resolve_value("${secret:DB_MISSING:fallback}").unwrap(),
            "fallback"
        );

        // ---- flip-abort posture: an unresolvable secret: ref is the SAME failure signal an
        // unset env: ref gives. Assembly/pool-creation propagates this Err, the two-phase flip
        // aborts with old state intact, and the watcher retries next tick (deploy.rs semantics).
        let unset_env = resolve_value("env:SUTRA_ENVREF_DEFINITELY_UNSET");
        let missing_secret = resolve_value("secret:DB_MISSING");
        assert!(unset_env.is_err(), "unset env: ref is the abort signal");
        assert!(
            missing_secret.is_err(),
            "missing secret: ref aborts the flip exactly like an unset env: ref"
        );
        // The placeholder form fails closed too (no default → abort).
        assert!(resolve_value("${secret:DB_MISSING}").is_err());

        std::env::remove_var(SECRETS_DIR_ENV);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn secret_scheme_refuses_path_traversal() {
        // A key with a path separator can never be a mounted-Secret file name — refuse it
        // rather than let it escape the mount.
        assert!(resolve_value("secret:../../etc/passwd").is_err());
        assert!(resolve_value("secret:sub/key").is_err());
        assert!(resolve_value("secret:").is_err());
    }

    #[test]
    fn default_secrets_dir_is_the_conventional_mount_point() {
        // Documented invariant the shared tofu depends on: absent SUTRA_SECRETS_DIR, refs
        // resolve from /etc/sutra/secrets.
        assert_eq!(DEFAULT_SECRETS_DIR, "/etc/sutra/secrets");
    }

    // ===== inventory seam: neutral schemes + self-registered vendor resolvers ================
    //
    // A crate-local fake resolver, `inventory::submit!`ed as an EnvRefResolverEntry, proves that
    // `with_builtins()` collects self-registered vendor resolvers from inventory (the mechanism
    // the real `sutra-envref-vault` / `-aws` crates use) WITHOUT this crate naming any vendor.
    // The hyphen in the scheme name also guards `split_scheme`'s `-` acceptance (needed for
    // `aws-secrets`).

    struct FakeVendorResolver;
    impl SchemeResolver for FakeVendorResolver {
        fn scheme(&self) -> &'static str {
            "fake-vendor"
        }
        fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
            Ok(format!("fake::{body}"))
        }
    }
    inventory::submit! {
        EnvRefResolverEntry { scheme: "fake-vendor", make: || Box::new(FakeVendorResolver) }
    }

    #[test]
    fn with_builtins_always_has_the_neutral_schemes_and_collects_inventory_resolvers() {
        let registry = ResolverRegistry::with_builtins();
        let schemes = registry.schemes();
        // The neutral builtins are ALWAYS present, with no vendor crate linked.
        assert!(schemes.contains(&"env"), "{schemes:?}");
        assert!(schemes.contains(&"secret"), "{schemes:?}");
        // The inventory seam collected the crate-local self-registered vendor resolver.
        assert!(schemes.contains(&"fake-vendor"), "{schemes:?}");
    }

    #[test]
    fn a_self_registered_resolver_dispatches_via_both_forms() {
        // Proves BOTH the whole-value ref (`fake-vendor:x`) and the interpolated placeholder
        // (`${fake-vendor:x}`) route to the inventory-collected resolver through the process
        // registry — the exact dispatch the real vault/aws crates rely on.
        assert_eq!(
            resolve_value("fake-vendor:prod/db#password").unwrap(),
            "fake::prod/db#password"
        );
        assert_eq!(
            resolve_value("amqp://svc:${fake-vendor:prod-db}@host/q").unwrap(),
            "amqp://svc:fake::prod-db@host/q"
        );
    }

    #[test]
    fn explicit_registry_dispatches_a_hyphenated_scheme() {
        // The registry seam directly (no inventory): a stub claiming a hyphenated scheme routes
        // for both the whole-value + placeholder forms. Guards `split_scheme`'s `-` acceptance.
        struct StubAwsSecrets;
        impl SchemeResolver for StubAwsSecrets {
            fn scheme(&self) -> &'static str {
                "aws-secrets"
            }
            fn resolve(&self, body: &str) -> Result<String, EnvRefError> {
                Ok(format!("stub::{body}"))
            }
        }
        let mut registry = ResolverRegistry::new();
        registry.register(Arc::new(EnvResolver));
        registry.register(Arc::new(StubAwsSecrets));

        assert_eq!(
            resolve_value_with("aws-secrets:prod/db#password", &registry).unwrap(),
            "stub::prod/db#password"
        );
        assert_eq!(
            resolve_value_with("amqp://svc:${aws-secrets:prod-db}@host/q", &registry).unwrap(),
            "amqp://svc:stub::prod-db@host/q"
        );
    }
}
