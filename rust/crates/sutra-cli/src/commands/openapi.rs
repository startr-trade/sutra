//! `sutra openapi` — emit a deployment's generated OpenAPI 3.1 surface from a sealed `.sutra`
//! archive, offline.
//!
//! This is the SAME projection the engine serves live at `GET /sutra/deployments/{id}/openapi`
//! (both call the `sutra-openapi` crate). Because a per-deployment surface is derived from the
//! archive manifest — one surface per `deploymentId` — it cannot be a committed static file; a
//! golden captured from this command is the Factor-13 drift gate for the *generator*.
//!
//! Input is a sealed archive (run `sutra package <dir>` first for a package directory). Output
//! goes to stdout — YAML by default, JSON with the global `--format json`.

use std::path::PathBuf;
use std::sync::Arc;

use sutra_bpmn::model::ProcessModule;
use sutra_channels::load_channel_definitions;
use sutra_datastore::parse_datastores;
use sutra_openapi::{deployment_spec, render_json, render_yaml, DeploymentApi};

use crate::exit;
use crate::output::Io;
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct OpenapiArgs {
    /// A sealed `.sutra` deployment archive (run `sutra package <dir>` first for a directory).
    pub archive: PathBuf,
}

pub fn execute(args: OpenapiArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let archive = match sutra_loader::read_archive_file(&args.archive) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "openapi: cannot read archive {}: {e}",
                args.archive.display()
            );
            return exit::USAGE;
        }
    };
    let d = &archive.deployment;

    // Parse the channel bindings (fail-closed, exactly like the engine's deploy path), then stamp
    // each with the archive's deploymentId so outbound/reachability keys resolve.
    let mut channels = match d.channels_yaml.as_deref() {
        Some(yaml) => match load_channel_definitions(
            yaml.as_bytes(),
            &d.tenant,
            &d.module,
            &d.version,
            "channels.yaml",
        ) {
            Ok(c) => c,
            Err(diag) => {
                let _ = writeln!(
                    io.err,
                    "openapi: channels.yaml failed to parse: [{}] {}",
                    diag.code, diag.message
                );
                return exit::USAGE;
            }
        },
        None => Vec::new(),
    };
    for c in &mut channels {
        c.binding.deployment = d.id.clone();
    }

    // Unique BPMN modules — the process map aliases one `Arc` per file.
    let mut modules: Vec<Arc<ProcessModule>> = Vec::new();
    for m in d.processes.values() {
        if !modules.iter().any(|x| Arc::ptr_eq(x, m)) {
            modules.push(Arc::clone(m));
        }
    }

    // Declared data-stores (intent-level inventory; a bad datastores.yaml would already have
    // failed the archive read, so an unexpected parse error degrades to an empty inventory).
    let stores = d
        .datastores_yaml
        .as_deref()
        .map(|y| parse_datastores(y).unwrap_or_default())
        .unwrap_or_default();

    let spec = deployment_spec(&DeploymentApi {
        deployment_id: d.id.value(),
        tenant: &d.tenant,
        module: &d.module,
        version: &d.version,
        channels: &channels,
        modules: &modules,
        stores: &stores,
    });

    let body = if global.format.as_deref() == Some("json") {
        render_json(&spec)
    } else {
        render_yaml(&spec)
    };
    let _ = writeln!(io.out, "{body}");
    exit::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;

    // Force-link the builtin data formats so the packager's codec resolution (json/xml/yaml)
    // sees them via inventory in this test binary (DCE would otherwise drop the unreferenced
    // rlib). Mirrors the binary's main.rs force-links.
    use sutra_formats as _;

    fn run(archive: PathBuf, format: Option<&str>) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured("", |io| execute(OpenapiArgs { archive }, &global, io))
    }

    /// Seal the committed money-transfer package into a temp dir and return the sealed archive.
    fn money_transfer_archive() -> (PathBuf, tempdir_guard::Guard) {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../../examples/money-transfer/deployments-src/default--money-transfer--1.0.0",
        );
        let dir = std::env::temp_dir().join(format!(
            "cli-openapi-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("out dir");
        sutra_loader::assemble_dir(&src, &dir, &sutra_loader::PackageOptions::default())
            .expect("money-transfer seals");
        let archive = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().is_some_and(|x| x == "sutra"))
            .expect("a sealed .sutra archive");
        (archive, tempdir_guard::Guard(dir))
    }

    #[test]
    fn emits_yaml_for_a_sealed_archive() {
        let (archive, _g) = money_transfer_archive();
        let (code, out, err) = run(archive, None);
        assert_eq!(code, crate::exit::OK, "stderr: {err}");
        assert!(out.contains("openapi: 3.1.0"), "openapi 3.1 yaml:\n{out}");
        assert!(
            out.contains("x-sutra-deployment-id: dep-"),
            "carries its id:\n{out}"
        );
        assert!(out.contains("paths:"), "has paths:\n{out}");
    }

    #[test]
    fn emits_json_with_the_global_format_flag() {
        let (archive, _g) = money_transfer_archive();
        let (code, out, _) = run(archive, Some("json"));
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(v["openapi"], "3.1.0");
        assert!(v["info"]["x-sutra-deployment-id"]
            .as_str()
            .unwrap()
            .starts_with("dep-"));
    }

    #[test]
    fn missing_archive_is_a_usage_error() {
        let (code, _, err) = run(PathBuf::from("/does/not/exist.sutra"), None);
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("cannot read archive"), "{err}");
    }

    /// Remove the temp seal dir when the test ends.
    mod tempdir_guard {
        use std::path::PathBuf;
        pub struct Guard(pub PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
}
