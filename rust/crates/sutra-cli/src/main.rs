//! Binary entry point — all behavior lives in the library crate so tests can drive
//! commands with captured streams.

// Force-link the builtin payload codec crates so their inventory registrations survive linker DCE
// (collected generically via `sutra_codec_spi::builtin_codecs()`/`builtin_formats()`; bundling is a
// BINARY concern — the CLI library stays domain-neutral). The public CLI links the schema-less
// formats; a distribution that bundles message-standard codecs adds them to its own CLI the same
// way.
use sutra_formats as _;

// Force-link the HTTP transport for the same reason: `sutra test simulate` boots a real
// `sutra_engine::serve`, whose channel intake resolves transports generically through
// `transport_factories()` — without this the linker drops the crate and every HTTP-bound
// channel 404s. HTTP is the one transport this public CLI bundles by default (see the
// dependency comment in Cargo.toml for the rationale and the extension-crate escape hatch).
use sutra_transport_http as _;

fn main() {
    std::process::exit(sutra_cli::run());
}
