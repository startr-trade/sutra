//! Bundle completeness for the vendor envref resolvers (domain-neutrality refactor). The engine
//! BINARY bundles every vendor secret-ref resolver crate (force-linked in `main.rs`); this test
//! lives in the DISTRIBUTION layer — not in the neutral `sutra-envref-spi` crate, which names no
//! vendor — and asserts the full vendor scheme set self-registers via the inventory pull model. It
//! is the DCE / "missing `inventory::submit!`" guard for the vendor resolvers: if a resolver crate
//! is dropped from the bundle or loses its registration, its scheme stops appearing below.

// Force-link the bundled vendor envref resolver crates into THIS test binary, exactly as the
// engine binary does — otherwise the linker would drop the never-referenced crates and their
// `EnvRefResolverEntry` inventory submissions.
use sutra_envref_aws as _;
use sutra_envref_azure as _;
use sutra_envref_gcp as _;
use sutra_envref_vault as _;

use sutra_envref_spi::ResolverRegistry;

#[test]
fn the_bundle_registers_every_neutral_and_vendor_scheme() {
    let schemes = ResolverRegistry::with_builtins().schemes();
    // The neutral builtins are always present (they live in the SPI, not a vendor crate).
    for neutral in ["env", "secret"] {
        assert!(
            schemes.contains(&neutral),
            "neutral scheme '{neutral}' missing from the registry — {schemes:?}"
        );
    }
    // Every vendor resolver crate the binary force-links self-registers its scheme via inventory.
    // A missing entry here means a dropped force-link or a missing `inventory::submit!` in that
    // vendor crate (is sutra-envref-<vendor> in the binary's dependencies + force-linked?).
    for vendor in ["vault", "aws-secrets", "azure-kv", "gcp-secret"] {
        assert!(
            schemes.contains(&vendor),
            "vendor scheme '{vendor}' does not resolve via the inventory pull model — a missing \
             inventory::submit! or a linker-dropped resolver crate — {schemes:?}"
        );
    }
}
