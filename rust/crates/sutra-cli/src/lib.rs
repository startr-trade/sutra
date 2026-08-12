//! The `sutra` command-line tool. One binary covering schema migrations (`migrate`),
//! read-only BPMN/FEEL inspection (`describe`, `dispatch-graph`, `explain`, `simulate`,
//! `compat-baseline`, `audit-replay`), deployment-package authoring and operation
//! (`lint`, `package`, `deployments`, `deploy`, `undeploy`, `openapi`, `crypto`),
//! scaffolding (`create`), path coverage (`coverage init|check`), engine-driving test tooling
//! (`test simulate` — the P1-7 time-skipping CLI wrapper, unrelated to the routing-only
//! `simulate` above), and the documentation and code generators (`docgen`, `schemagen`,
//! `catalog`).
//!
//! Standing rules behind that surface:
//!
//! - **Migrations** are tracked in a checksummed minimal ledger — the normative format.
//!   `migrate verify` checks the expected head, version continuity, and checksum drift.
//! - **Scaffolding is language-free.** `create` emits deployment artifacts and deploy
//!   assets, never build tooling for a host language.
//! - **`simulate` is routing-only** (`--dry-run` is required): it reports which process
//!   and start event an inbound message would reach. Executing fixtures is not part of it.
//! - **Coverage enumeration is capped** — 256 routes by default, `--max-paths` overrides —
//!   so a combinatorial process cannot flood a declaration file; and regeneration never
//!   overwrites a user-edited file implicitly, which takes `--force`.
//! - **Deployment packages are standalone**: a sealed, self-contained archive with no
//!   inherited tenant or library tree.
//!
//! # Conventions
//!
//! - **Exit codes** ([`exit`]): `0` success / no findings, `1` findings (lint-style
//!   errors, breaking compat, verify drift, routing miss, no matching events), `2` usage
//!   or infrastructure failure (bad flags, missing files, unreachable database, parse
//!   failures). Every command maps onto this one contract.
//! - **Output format**: global `--format` flag. Most commands accept `text` (default) or
//!   `json`; `dispatch-graph` interprets the same flag as its renderer (`dot` default,
//!   `mermaid`).
//! - **Diagnostics** ([`output::Diagnostic`]): every finding prints as
//!   `[SEVERITY] CODE — message (location)`, the same shape in every command.
//! - **Logging**: `-v/-vv/-vvv` raise the `tracing` level (warn → info → debug → trace),
//!   always on stderr so stdout stays machine-consumable.
//!
//! Command modules live under [`commands`]; the registry there documents how a new
//! subcommand is added.
//!
//! # Embedding this CLI in a distribution
//!
//! The library half is the whole tool; [`main.rs`](../src/main.rs) is a force-link shim that
//! calls [`run`]. A distribution that bundles extra codecs ships its own binary crate with its
//! own `[[bin]] name`, force-links what it needs and calls [`run`] — or [`run_with_version`]
//! when the distribution versions itself independently of the engine.
//!
//! Two rules keep that neutral:
//!
//! - The **displayed program name is derived from the running binary** ([`program_name`]), not
//!   hardcoded, so whatever a distribution calls its binary is what `--version`, `version` and
//!   the usage line say.
//! - The **engine's own version is [`VERSION`]**, exported so a downstream can render it next
//!   to its own product version.

#![forbid(unsafe_code)]

use clap::{CommandFactory, FromArgMatches, Parser};
use std::sync::OnceLock;

pub(crate) mod bpmn_walk;
pub mod commands;
pub mod compat;
pub mod embedded;
pub mod exit;
pub mod gitref;
pub mod output;
pub mod routes;
pub mod scaffold;
#[cfg(test)]
pub(crate) mod test_fixtures;

/// The engine's own product name — the fallback program name when argv is empty, and the
/// label a distribution uses when it renders the embedded engine next to itself.
pub const NAME: &str = "sutra";

/// The engine's own version. A distribution that versions itself independently renders this
/// alongside its own version (see [`run_with_version`]).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The version string `--version` and `version` print, set once by [`run_with_version`].
/// Unset (direct library use, tests) means the engine's own [`VERSION`].
static VERSION_STRING: OnceLock<String> = OnceLock::new();

/// Where `self-update` fetches this distribution's releases from — set once by
/// [`run_with_update_source`].
///
/// This is a SEAM, not a constant, for a load-bearing reason: this library is the tooling
/// composition root for other distributions too (a rails CLI links it with its own codecs and
/// runs under its own name). A hardcoded repository would make `self-update` download THIS
/// engine's binary and overwrite whatever product is actually running — replacing a
/// distribution's CLI with a different one, along with every codec it linked.
///
/// Unset therefore means "this distribution publishes elsewhere", and `self-update` REFUSES
/// rather than guessing. Silence is the safe default here.
static UPDATE_SOURCE: OnceLock<UpdateSource> = OnceLock::new();

/// How a distribution's releases are discovered and fetched.
///
/// Two shapes, and deliberately no vendor list: this crate is the tooling composition root
/// for distributions that publish to hosts it has never heard of, and enumerating them here
/// would put one product's infrastructure in another product's source tree. A distribution
/// that does not publish to GitHub Releases describes its store with [`Self::FileStore`] and
/// keeps its own URLs in its own repository.
#[derive(Debug, Clone)]
pub enum UpdateChannel {
    /// GitHub Releases on `owner/repo` — this project's own channel. Assets live under
    /// `/releases/download/<tag>/`, and the newest tag comes from the releases API.
    GithubReleases { repo: String },

    /// A flat HTTPS file store: every asset is `<base>/<filename>`, with the tag carried in
    /// the filename rather than a path. Enough to describe a repository download area, an
    /// object-store prefix, or an internal artifact server.
    FileStore {
        /// Base URL assets hang off, without a trailing slash.
        base: String,
        /// Optional JSON listing used to discover the newest tag. Without it, callers must
        /// pass `--version` — which is the honest failure, not a guess.
        index_url: Option<String>,
        /// JSON Pointer to the array of entries within that listing (e.g. `/values`); an
        /// empty pointer means the document is itself the array.
        index_pointer: String,
        /// Field on each entry holding the file name (e.g. `name`).
        index_name_field: String,
    },
}

/// Where a distribution's release binaries live, and what they are called.
#[derive(Debug, Clone)]
pub struct UpdateSource {
    /// The publishing channel.
    pub channel: UpdateChannel,
    /// Asset stem: assets are `<binary>-<tag>-<target>.tar.gz` (`.zip` on Windows).
    pub binary: String,
    /// Container image the same release publishes, for `--runtime`. `None` = no image.
    /// Not necessarily on the same host as the binaries: a distribution may publish its CLI
    /// to its source host and its runtimes to a cloud registry.
    pub image: Option<String>,
}

/// The update source this distribution declared, if any.
pub fn update_source() -> Option<&'static UpdateSource> {
    UPDATE_SOURCE.get()
}

/// Top-level argument surface of the CLI. The `name` here is only a fallback: [`run`] and
/// [`run_with_version`] override it with [`program_name`] before parsing.
#[derive(Debug, Parser)]
#[command(
    name = NAME,
    version,
    about = "Sutra engine tooling: schema migrations, BPMN inspectors, FEEL explain"
)]
pub struct SutraCli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: commands::Command,
}

/// Flags shared by every subcommand.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct GlobalArgs {
    /// Output format: text|json for report commands, dot|mermaid for dispatch-graph.
    #[arg(long, global = true, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Increase log verbosity (-v info, -vv debug, -vvv trace); logs go to stderr.
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

/// The displayed program name: the file stem of `argv[0]`, so a distribution that names its
/// binary something else shows that name without touching this crate. Falls back to [`NAME`]
/// when argv carries no usable program path (an exec with an empty argv, some embeddings).
///
/// This is only the *displayed* name; clap independently derives the usage line's binary name
/// from the same argv entry.
pub fn program_name() -> String {
    let argv0 = std::env::args_os().next();
    program_name_from(argv0.as_deref())
}

fn program_name_from(argv0: Option<&std::ffi::OsStr>) -> String {
    argv0
        .map(std::path::Path::new)
        .and_then(std::path::Path::file_stem)
        .map(|stem| stem.to_string_lossy().into_owned())
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| NAME.to_string())
}

/// The version string this process reports. [`VERSION`] unless a distribution supplied its own
/// through [`run_with_version`].
pub(crate) fn version_string() -> &'static str {
    VERSION_STRING.get().map(String::as_str).unwrap_or(VERSION)
}

/// Parses argv, initialises logging and executes the selected command against the real
/// process streams. Returns the process exit code (see [`exit`]).
///
/// Reports the engine's own [`VERSION`]; see [`run_with_version`] for distributions that
/// version themselves independently.
pub fn run() -> i32 {
    run_with_version(VERSION)
}

/// [`run`], with the version string a distribution reports for itself.
///
/// The string may be **multi-line**: clap prints `"<program-name> <version>"`, so a first line
/// carrying the product version and further lines describing what is embedded render as a
/// version block. A distribution at its own version, embedding this engine, composes:
///
/// ```no_run
/// let version = format!(
///     "{}\n{}  {} (engine)",
///     env!("CARGO_PKG_VERSION"),
///     sutra_cli::NAME,
///     sutra_cli::VERSION,
/// );
/// std::process::exit(sutra_cli::run_with_version(version));
/// ```
///
/// The first line is also what `version --format json` reports as `version`; the engine's own
/// version is always available there as `engine`.
/// [`run_with_version`], plus where `self-update` should fetch this distribution's releases.
///
/// A distribution that publishes to GitHub Releases calls this; one that publishes anywhere
/// else (a private registry, an internal artifact store) calls [`run_with_version`] and its
/// `self-update` refuses with a pointer to its own install docs — which is correct, and far
/// better than updating a user's binary to a different product.
pub fn run_with_update_source(version: impl Into<String>, source: UpdateSource) -> i32 {
    let _ = UPDATE_SOURCE.set(source);
    run_with_version(version)
}

pub fn run_with_version(version: impl Into<String>) -> i32 {
    let version = version.into();
    let command = SutraCli::command()
        .name(program_name())
        .version(version.clone());
    // Set before parsing: `--version` is handled inside `get_matches`.
    let _ = VERSION_STRING.set(version);
    let matches = command.get_matches();
    let cli = match SutraCli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(err) => err.exit(),
    };
    init_tracing(cli.global.verbose);

    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let stdin = std::io::stdin();
    let mut out = stdout.lock();
    let mut err = stderr.lock();
    let mut input = stdin.lock();
    let mut io = output::Io {
        out: &mut out,
        err: &mut err,
        input: &mut input,
    };
    cli.command.execute(&cli.global, &mut io)
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    // Ignore a second-init error: `run` is called once per process; tests may call
    // commands directly without logging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_consistent() {
        SutraCli::command().debug_assert();
    }

    #[test]
    fn the_program_name_is_the_file_stem_of_argv0() {
        use std::ffi::OsStr;
        assert_eq!(
            program_name_from(Some(OsStr::new("/usr/bin/sutra"))),
            "sutra"
        );
        assert_eq!(
            program_name_from(Some(OsStr::new("./target/debug/sutra"))),
            "sutra"
        );
        // Any embedder's binary name, unchanged — nothing here is hardcoded to the engine's.
        assert_eq!(
            program_name_from(Some(OsStr::new("/opt/acme/bin/acme-flow"))),
            "acme-flow"
        );
        assert_eq!(program_name_from(Some(OsStr::new("sutra.exe"))), "sutra");
    }

    #[test]
    fn the_program_name_falls_back_when_argv0_is_unusable() {
        use std::ffi::OsStr;
        assert_eq!(program_name_from(None), NAME);
        assert_eq!(program_name_from(Some(OsStr::new(""))), NAME);
        assert_eq!(program_name_from(Some(OsStr::new("/"))), NAME);
    }

    #[test]
    fn version_output_pairs_the_program_name_with_the_supplied_version() {
        // The engine's own binary: one line, its own version.
        let engine = SutraCli::command()
            .name(NAME)
            .version(VERSION)
            .render_version();
        assert_eq!(engine, format!("sutra {VERSION}\n"));

        // A distribution at its own version, embedding this engine: a version block.
        let embedded = SutraCli::command()
            .name("acme-flow")
            .version(format!("2.0.0\n{NAME}  {VERSION} (engine)"))
            .render_version();
        assert_eq!(
            embedded,
            format!("acme-flow 2.0.0\nsutra  {VERSION} (engine)\n")
        );
    }

    #[test]
    fn the_reported_version_defaults_to_the_engines_own() {
        // `run_with_version` is the only writer, and no test drives a whole process through
        // it, so the reported version here is the unoverridden default.
        assert_eq!(version_string(), VERSION);
    }

    #[test]
    fn global_format_flag_is_accepted_before_and_after_the_subcommand() {
        let before = SutraCli::try_parse_from(["sutra", "--format", "json", "version"]).unwrap();
        assert_eq!(before.global.format.as_deref(), Some("json"));
        let after = SutraCli::try_parse_from(["sutra", "version", "--format", "json"]).unwrap();
        assert_eq!(after.global.format.as_deref(), Some("json"));
    }

    #[test]
    fn verbosity_flag_counts() {
        let cli = SutraCli::try_parse_from(["sutra", "-vv", "version"]).unwrap();
        assert_eq!(cli.global.verbose, 2);
    }

    #[test]
    fn migrate_reads_the_deploy_contract_environment_variables() {
        // Serialised inside one test: environment mutation must not race other tests.
        std::env::set_var("SUTRA_DB_URL", "postgres://db:5432/sutra");
        std::env::set_var("SUTRA_DB_USERNAME", "svc");
        std::env::set_var("SUTRA_DB_PASSWORD", "secret");
        std::env::set_var("SUTRA_DB_SCHEMA", "engine");
        let cli = SutraCli::try_parse_from(["sutra", "migrate"]).unwrap();
        std::env::remove_var("SUTRA_DB_URL");
        std::env::remove_var("SUTRA_DB_USERNAME");
        std::env::remove_var("SUTRA_DB_PASSWORD");
        std::env::remove_var("SUTRA_DB_SCHEMA");
        let commands::Command::Migrate(args) = cli.command else {
            panic!("expected migrate");
        };
        assert_eq!(args.conn.url.as_deref(), Some("postgres://db:5432/sutra"));
        assert_eq!(args.conn.user.as_deref(), Some("svc"));
        assert_eq!(args.conn.password.as_deref(), Some("secret"));
        assert_eq!(args.conn.schema.as_deref(), Some("engine"));
    }

    #[test]
    fn migrate_subcommands_parse_with_their_own_connection_flags() {
        let cli = SutraCli::try_parse_from([
            "sutra",
            "migrate",
            "verify",
            "--url",
            "postgres://db/x",
            "--expected-head",
            "803",
        ])
        .unwrap();
        let commands::Command::Migrate(args) = cli.command else {
            panic!("expected migrate");
        };
        let Some(commands::migrate::MigrateAction::Verify(verify)) = args.action else {
            panic!("expected verify");
        };
        assert_eq!(verify.conn.url.as_deref(), Some("postgres://db/x"));
        assert_eq!(verify.expected_head, Some(803));
    }
}
