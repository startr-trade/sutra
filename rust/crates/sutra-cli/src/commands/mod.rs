//! Command registry.
//!
//! Each subcommand is a self-contained module exposing an `Args` struct (clap derive) and
//! an `execute(args, &GlobalArgs, &mut Io) -> i32` entry point that returns an [`crate::exit`]
//! code. **Adding a subcommand** touches only this file plus the new module:
//! (1) declare the module, (2) add one enum variant,
//! (3) add one dispatch arm. Existing command files never change.

use crate::output::Io;
use crate::GlobalArgs;

pub mod audit_replay;
pub mod catalog;
pub mod compat_baseline;
pub mod coverage;
pub mod create;
pub mod crypto;
pub mod deploy;
pub mod deployments;
pub mod describe;
pub mod dispatch_graph;
pub mod docgen;
pub mod explain;
pub mod lint;
pub mod migrate;
pub mod openapi;
pub mod package;
pub mod schemagen;
pub mod self_update;
pub mod simulate;
pub mod test;
pub mod version;

/// The `sutra` subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Print the tool version.
    Version(version::VersionArgs),
    /// Replace this binary with a published release build (explicit, verified, atomic).
    SelfUpdate(self_update::SelfUpdateArgs),
    /// Apply engine schema migrations (or inspect them: status, verify, --dry-run).
    Migrate(migrate::MigrateArgs),
    /// Print a structural summary of a BPMN file (processes, events, tasks, gateways, channels).
    Describe(describe::DescribeArgs),
    /// Emit a sealed archive's generated OpenAPI 3.1 surface (channels → BPMNs, message types,
    /// endpoint nature, data-stores) — the same document the engine serves per deploymentId.
    Openapi(openapi::OpenapiArgs),
    /// Emit a graphviz dot or mermaid graph of a BPMN file's dispatch tree.
    DispatchGraph(dispatch_graph::DispatchGraphArgs),
    /// Compare current BPMN signatures against a baseline directory or git ref.
    CompatBaseline(compat_baseline::CompatBaselineArgs),
    /// Evaluate a FEEL expression (one-shot or REPL) against an optional context.
    Explain(explain::ExplainArgs),
    /// Walk an instance's audit events from a JSONL stream.
    AuditReplay(audit_replay::AuditReplayArgs),
    /// Report which process an inbound channel message would route to (--dry-run only).
    Simulate(simulate::SimulateArgs),
    /// Engine-driving test tooling — currently `test simulate`, the P1-7 time-skipping CLI
    /// wrapper (boot a real engine with a virtual clock, fast-forward, report). Unrelated to
    /// the routing-only `simulate` above; see `test`'s module docs for why they are separate.
    Test(test::TestArgs),
    /// Scaffold an application workspace, a deployment package, or a process.
    Create(create::CreateArgs),
    /// Seed or lint <q:coverage> path declarations.
    Coverage(coverage::CoverageArgs),
    /// Provision a KEK-wrapped per-tenant DEK into the `data_key` store (envelope encryption).
    Crypto(crypto::CryptoArgs),
    /// Validate a deployment-package directory (full package-time suite, emits nothing).
    Lint(lint::LintArgs),
    /// Seal a deployment-package directory into one .sutra archive.
    Package(package::PackageArgs),
    /// Inspect a directory of sealed .sutra archives — list deployment ids + labels.
    Deployments(deployments::DeploymentsArgs),
    /// Hot-deploy a sealed .sutra archive onto the shared engine instance: patch the
    /// estate Secret first, then the deployments ConfigMap.
    Deploy(deploy::DeployArgs),
    /// Remove a deployment from the shared instance's ConfigMap — the engine drains it
    /// (no new intake) and retires at zero instances + zero pending outbox.
    Undeploy(deploy::UndeployArgs),
    /// Generate (or drift-check) the markdown catalog for a folder of authored deployment
    /// artifacts — BPMN, DMN/SRL rules, templates, channels.yaml, package.yaml.
    Docgen(docgen::DocgenArgs),
    /// Generate (or drift-check) the Rust binding sources from an XSD corpus.
    Schemagen(schemagen::SchemagenArgs),
    /// Generate (or drift-check) the Rust artifact-documentation catalog for the workspace.
    Catalog(catalog::CatalogArgs),
}

impl Command {
    /// Dispatch to the command implementation; returns the process exit code.
    pub fn execute(self, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
        match self {
            Command::Version(args) => version::execute(args, global, io),
            Command::SelfUpdate(args) => self_update::execute(args, global, io),
            Command::Migrate(args) => migrate::execute(args, global, io),
            Command::Describe(args) => describe::execute(args, global, io),
            Command::Openapi(args) => openapi::execute(args, global, io),
            Command::DispatchGraph(args) => dispatch_graph::execute(args, global, io),
            Command::CompatBaseline(args) => compat_baseline::execute(args, global, io),
            Command::Explain(args) => explain::execute(args, global, io),
            Command::AuditReplay(args) => audit_replay::execute(args, global, io),
            Command::Simulate(args) => simulate::execute(args, global, io),
            Command::Test(args) => test::execute(args, global, io),
            Command::Create(args) => create::execute(args, global, io),
            Command::Coverage(args) => coverage::execute(args, global, io),
            Command::Crypto(args) => crypto::execute(args, global, io),
            Command::Lint(args) => lint::execute(args, global, io),
            Command::Package(args) => package::execute(args, global, io),
            Command::Deployments(args) => deployments::execute(args, global, io),
            Command::Deploy(args) => deploy::execute_deploy(args, global, io),
            Command::Undeploy(args) => deploy::execute_undeploy(args, global, io),
            Command::Docgen(args) => docgen::execute(args, global, io),
            Command::Schemagen(args) => schemagen::execute(args, global, io),
            Command::Catalog(args) => catalog::execute(args, global, io),
        }
    }
}
