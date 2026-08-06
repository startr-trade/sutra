//! Bundle completeness (domain-neutrality refactor). The engine BINARY bundles the built-in
//! payload codecs (force-linked in `main.rs`); this test lives in the DISTRIBUTION layer — not in
//! the neutral `sutra-channels` / `sutra-codec-spi` crates — and asserts the zero-config codec set
//! resolves via the inventory pull model. It is the DCE / "missing `inventory::submit!`" guard: if
//! a codec crate is dropped from the bundle or loses its registration, it stops resolving below.
//!
//! # The PUBLIC distribution is formats-only
//!
//! Every message-standard codec is a PROPRIETARY extension crate in a separate repository,
//! force-linked by ITS composition root.
//! So `builtin_codecs()` is EMPTY here and `builtin_formats()` carries the whole public set —
//! and that emptiness is itself the assertion ([`the_public_bundle_is_formats_only`]): a domain
//! codec that ever reappears in the public dependency graph fails the build, which is the
//! machine-checked half of the neutrality boundary `sutra-archtest` enforces on source.
//!
//! # The expectation is DERIVED, not listed
//!
//! What "the bundle" contains is decided in ONE place — the force-links in `main.rs` mirrored
//! here — so this file reads the linked set back out of `builtin_codecs()` and asserts every
//! member resolves through all three lookup forms. The sweep is vacuous while the public set is
//! empty and becomes live the moment a codec is linked; the equivalent live sweep over a
//! non-empty set runs in the rails repo's own `rails_codec_bundle`.

// Force-link the bundled codec crates into THIS test binary, exactly as the engine binary does —
// otherwise the linker would drop the never-referenced crates and their registrations.
use sutra_formats as _;

use sutra_channels::{CodecRegistry, DeploymentId, FormatRegistry};

const URN_PREFIX: &str = "urn:sutra:codec:";

/// The codecs the PUBLIC distribution must always carry, whatever else is linked. Deliberately
/// EMPTY: the public engine bundles the schema-less formats only, and a message-standard codec is
/// an extension crate proved by the derived sweep below while it is linked. Adding or removing one
/// must not require editing this list.
const ALWAYS_BUNDLED: &[&str] = &[];

#[test]
fn the_public_bundle_is_formats_only() {
    // The neutrality boundary, machine-checked at LINK time rather than on source text. The public
    // distribution force-links `sutra-formats` and nothing else that registers a `BuiltinCodec`, so
    // a non-empty set here means a domain codec crate re-entered the public dependency graph.
    let codecs = sutra_codec_spi::builtin_codecs();
    let names: Vec<&str> = codecs.iter().map(|c| c.name()).collect();
    assert!(
        names.is_empty(),
        "the public distribution must bundle NO message-standard codec (found {names:?}) — \
         domain codecs are proprietary extension crates that a downstream composition root \
         force-links into its own binary"
    );
    // The formats, by contrast, must be there — the "everything got dropped" guard the emptiness
    // assertion above can no longer provide on its own.
    assert!(
        !sutra_codec_spi::builtin_formats().is_empty(),
        "no builtin formats registered at all — every force-link was dropped, or inventory \
         collection is broken"
    );
}

#[test]
fn the_bundle_resolves_every_linked_business_codec_by_urn_and_name() {
    let registry = CodecRegistry::with_builtins();
    let dep = DeploymentId::unresolved();
    // The bundle as the LINKER actually assembled it — the same set the engine sees at runtime.
    let codecs = sutra_codec_spi::builtin_codecs();
    let bundled: Vec<&str> = codecs.iter().map(|c| c.name()).collect();
    for expected in ALWAYS_BUNDLED {
        assert!(
            bundled.contains(expected),
            "the public distribution must always bundle codec '{expected}' (linked set: \
             {bundled:?}) — is sutra-codec-{expected} in the binary's dependencies AND \
             force-linked in main.rs?"
        );
    }
    // Built-ins are keyed `urn:sutra:codec:<name>:internal`; a `channels.yaml codec:`
    // reference stays the bare name or the logical URN and resolves
    // via `CodecRegistry::resolve`, which appends the scope. Every bundled codec must answer to all
    // three forms.
    for name in &bundled {
        let urn = format!("{URN_PREFIX}{name}");
        assert!(
            registry.find(&format!("{urn}:internal")).is_some(),
            "bundled codec '{name}' does not resolve via '{urn}:internal' — a registration \
             whose inventory name and URN key disagree"
        );
        assert!(
            registry.resolve(&urn, &dep).is_some(),
            "the logical URN '{urn}' must resolve (scope appended)"
        );
        assert!(
            registry.resolve(name, &dep).is_some(),
            "bare bundled codec '{name}' must also resolve"
        );
    }
    // A user codec URN (`urn:<path>`) is NOT a built-in and must not resolve here.
    assert!(registry.resolve("urn:transfer", &dep).is_none());
}

#[test]
fn the_bundle_registers_every_schema_less_format() {
    // The format DCE guard: every schema-less format crate the binary force-links self-registers as
    // a BuiltinFormat (they are formats, not codecs — absent from builtin_codecs()). The format set
    // is public-tree only (sutra-formats), so it stays an explicit list.
    let names: Vec<&str> = sutra_codec_spi::builtin_formats()
        .iter()
        .map(|f| f.name)
        .collect();
    for expected in ["csv", "json", "raw-bytes", "raw-text", "xml", "yaml"] {
        assert!(
            names.contains(&expected),
            "format '{expected}' missing from the bundle — a dropped force-link or missing \
             inventory::submit! BuiltinFormat"
        );
    }
    // Formats are NOT in the codec set.
    assert!(sutra_codec_spi::builtin_codecs()
        .iter()
        .all(|c| !names.contains(&c.name())));
}

#[test]
fn shape_of_is_codec_driven_through_the_trait_object() {
    // `shape_of` is answered by the CODEC through the trait (no per-standard hardcode in the neutral
    // engine); exercising it through the trait object also proves the default impl does not recurse
    // (a recursion would overflow the stack here). A schema-LESS format has no fixed shape by
    // construction, so `None` for every message-type argument is the whole contract at this layer.
    //
    // The positive half — a schema-aware codec returning Some(shape) via a delegating override —
    // is proved where such a codec actually lives: `sutra-codec-schema`'s own
    // `structural_codec_test` (public, XSD-backed) and each extension codec's shape test in the
    // repository that owns it. The public distribution links no schema-aware built-in to drive it
    // from here, and asserting it against a codec this binary does not bundle would be fiction.
    for entry in sutra_codec_spi::builtin_formats() {
        assert!(
            entry.codec.shape_of(None).is_none(),
            "format '{}' must expose no fixed shape",
            entry.name
        );
        assert!(
            entry.codec.shape_of(Some("any.message.type")).is_none(),
            "format '{}' must expose no shape for a named message type either",
            entry.name
        );
    }
    // Formats live in the FormatRegistry, NOT the CodecRegistry — they declare a shape CLASS
    // (the negotiation contract), never a SchemaShape. Both halves of that split are asserted
    // here so the empty public codec set cannot be mistaken for a broken registry.
    let formats = FormatRegistry::with_builtins();
    assert_eq!(
        formats.contract("json"),
        formats.contract("urn:sutra:codec:json")
    );
    assert!(
        formats.contract("json").is_some(),
        "json declares a shape class"
    );
    assert!(
        CodecRegistry::with_builtins()
            .resolve("json", &DeploymentId::unresolved())
            .is_none(),
        "a format must not resolve through the CODEC registry — the two sets are disjoint"
    );
}
