//! The `.sutra` format owner — three layers, one crate, shared by the CLI and the
//! engine:
//!
//! 1. **Authoring input** ([`package`]): the STANDALONE deployment-package
//!    directory — an archive-layout mirror plus a minimal `package.yaml`
//!    (schema in the [`package`] module docs). [`lint_dir`] validates one,
//!    [`assemble_dir`] seals one into exactly one `.sutra`. No overlay tree, no
//!    inheritance, no sharing mechanism; tenant is just a label. The `.sutra` archive is
//!    the ONE deployment model, in and out — there is no resource-tree source.
//! 2. **Package/lint library** ([`package`], [`lint`]) — the fail-closed package-time
//!    validation suite (ONE code path for `sutra lint`, `sutra package` and the archive
//!    reader) and the deterministic archive assembler.
//! 3. **Archive format** ([`archive`]) — the deterministic `.sutra` container, the
//!    `manifest.yaml` schema, the content-addressed deploymentId
//!    (`dep-<first 24 hex of sha256(manifest bytes)>`), and the verifying reader
//!    whose output keeps the [`LoadedDeployment`] shape the engine assembly consumes.
//!
//! The [`scanner`] module holds the shared loaded-deployment model
//! ([`LoadedDeployment`] / [`LoadedArtifact`] / [`LoadedProcessFile`]) and the filesystem
//! helpers the archive reader and the package-directory scanner both build on.
#![forbid(unsafe_code)]

pub mod archive;
pub mod coverage;
pub mod ddl;
pub mod error;
pub mod lint;
pub mod package;
pub mod scanner;

pub use archive::{
    deployment_from_entries, deployment_id_of_manifest, read_archive, read_archive_expecting,
    read_archive_file, write_archive, ArchiveManifest, ArtifactEntry, LoadedArchive,
};
pub use coverage::{BusinessCorrelation, CoverageFile, CoverageRoute, Hop};
pub use error::LoaderError;
pub use lint::{LintDiagnostic, LintReport, LintSeverity};
pub use package::{
    assemble_dir, lint_dir, CompiledSchemaEmitter, PackageConfig, PackageError, PackageOptions,
    PackageOutcome, PackagedArchive, PACKAGE_FILE_NAME,
};
pub use scanner::{LoadedArtifact, LoadedDeployment, LoadedProcessFile};
pub use sutra_executor::deployment::DeploymentId;
