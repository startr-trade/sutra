//! The exit-code contract every `sutra` subcommand maps onto.
//!
//! | code | meaning |
//! |------|---------|
//! | 0    | success — clean run, no findings |
//! | 1    | findings — the inputs have a diagnosable problem (breaking compat change, ledger drift, routing miss, failing migration script, no matching events) |
//! | 2    | usage or infrastructure — bad flags, missing files, unparseable inputs, unreachable database |
//!
//! `clap` usage errors also exit 2, so flag misuse and file-not-found land in the same
//! bucket. These three values are frozen; the wording rendered around them is not.

/// Clean run.
pub const OK: i32 = 0;
/// The inputs carry findings (deploy-blocking diagnostics, drift, misses).
pub const FINDINGS: i32 = 1;
/// The invocation or environment is broken (usage, I/O, connectivity, parse).
pub const USAGE: i32 = 2;
