//! The conformance harness — the reusable half of the end-to-end suites.
//!
//! `engine` owns the engine-under-test container (the one implementation every tier-2 suite
//! uses); `broker` wraps the rabbitmq fixture + lapin helpers; `callback` is the host-side
//! HTTP recorder the async out-of-band suites assert against; `compose` overlays a
//! multi-variant example's `shared/` + `variants/<name>/` into a standalone package dir
//! (the build-time DRY-variants convention); `k8s` shells tofu/CLI + reads the cluster
//! through kube-rs for the tier-3 suites; `util` holds the shared primitives.
//!
//! # Why it lives in the testkit
//!
//! The harness is a LIBRARY, not a suite: it must be drivable from a conformance crate in
//! ANOTHER workspace (an extension repo composing this engine as a path/submodule dependency)
//! exactly as it is from `sutra-conformance` here. Everything a suite needs to stand up —
//! postgres + broker + engine-container fixtures, [`compose::compose_variant`], the recorders,
//! and the k8s helpers ([`k8s::run_cli`] / [`k8s::deploy_api`] / [`k8s::await_rollout`] /
//! [`k8s::engine_image`] / [`k8s::kubeconfig_path`]) — is public here; a suite crate
//! contributes only its own payloads and assertions.
//!
//! An out-of-workspace caller points the harness at ITS tree through [`util::repo_root`]'s
//! `SUTRA_REPO_ROOT` and [`util::examples_dir`]'s `SUTRA_EXAMPLES_DIR` overrides (the
//! compile-time fallback resolves the testkit's own repo, which is only correct in-workspace)
//! and at its own CLI build through `SUTRA_CLI`.
//!
//! Gated behind the `conformance` cargo feature so the testkit's default build — the atexit
//! container reaper every crate's `tests/` tree dev-depends on — stays free of testcontainers
//! / lapin / kube.

pub mod broker;
pub mod callback;
pub mod compose;
pub mod engine;
pub mod k8s;
pub mod util;
