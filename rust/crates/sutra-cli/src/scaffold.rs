//! Scaffolding engine shared by `sutra create` and `sutra coverage init`: the
//! embedded template assets, `%%TOKEN%%` rendering, and the write semantics —
//! **regeneration never touches user-edited files without `--force`**, generated files
//! carry a `generated-by:` header, and `create` never clobbers anything.

use std::io;
use std::path::{Path, PathBuf};

use include_dir::{include_dir, Dir};

/// The embedded scaffold templates (`assets/**` inside this crate).
static ASSETS: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/assets");

/// The header marker every generated file carries. A file WITHOUT this marker is
/// user-authored (or user-adopted) and is never overwritten without `--force`.
pub const GENERATED_MARKER: &str = "generated-by: sutra";

/// Fetch an embedded template by its `assets/`-relative path. Panics on a missing asset —
/// assets are compiled in, so absence is a build defect, not a runtime condition.
pub fn asset(path: &str) -> &'static str {
    ASSETS
        .get_file(path)
        .unwrap_or_else(|| panic!("embedded scaffold asset missing: {path}"))
        .contents_utf8()
        .unwrap_or_else(|| panic!("embedded scaffold asset not utf-8: {path}"))
}

/// Replace `%%KEY%%` tokens. Unmatched tokens are a template defect: callers pass every
/// token their template declares (checked by the scaffold tests).
pub fn render(template: &str, vars: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (key, value) in vars {
        out = out.replace(&format!("%%{key}%%"), value);
    }
    out
}

/// What a write attempt did — the per-file outcome the commands report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// File did not exist; written.
    Created,
    /// File existed (generated or `--force`) and was rewritten with new content.
    Updated,
    /// File existed with identical content; untouched.
    Unchanged,
    /// File existed with user content (no `generated-by:` marker) and no `--force`; untouched.
    SkippedUserFile,
    /// File existed and `create` semantics never overwrite; untouched.
    SkippedExisting,
}

impl WriteOutcome {
    pub fn label(self) -> &'static str {
        match self {
            WriteOutcome::Created => "created",
            WriteOutcome::Updated => "updated",
            WriteOutcome::Unchanged => "unchanged",
            WriteOutcome::SkippedUserFile => "skipped (user file — re-run with --force)",
            WriteOutcome::SkippedExisting => "skipped (exists)",
        }
    }
}

/// Regeneration write: create when absent; leave identical content alone; rewrite a
/// previously generated file (it carries [`GENERATED_MARKER`]); refuse a user file
/// unless `force`.
pub fn write_generated(path: &Path, content: &str, force: bool) -> io::Result<WriteOutcome> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        std::fs::write(path, content)?;
        return Ok(WriteOutcome::Created);
    }
    let existing = std::fs::read_to_string(path)?;
    if existing == content {
        return Ok(WriteOutcome::Unchanged);
    }
    if force || existing.contains(GENERATED_MARKER) {
        std::fs::write(path, content)?;
        return Ok(WriteOutcome::Updated);
    }
    Ok(WriteOutcome::SkippedUserFile)
}

/// `create` write: create when absent, otherwise never touch (idempotent-safe).
pub fn write_pristine(path: &Path, content: &str) -> io::Result<WriteOutcome> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let existing = std::fs::read_to_string(path).unwrap_or_default();
        return Ok(if existing == content {
            WriteOutcome::Unchanged
        } else {
            WriteOutcome::SkippedExisting
        });
    }
    std::fs::write(path, content)?;
    Ok(WriteOutcome::Created)
}

/// Accumulates per-file outcomes for the end-of-command report.
#[derive(Debug, Default)]
pub struct WriteReport {
    pub entries: Vec<(PathBuf, WriteOutcome)>,
}

impl WriteReport {
    pub fn record(&mut self, path: &Path, outcome: WriteOutcome) {
        self.entries.push((path.to_path_buf(), outcome));
    }

    pub fn count(&self, outcome: WriteOutcome) -> usize {
        self.entries.iter().filter(|(_, o)| *o == outcome).count()
    }

    pub fn skipped_user_files(&self) -> Vec<&Path> {
        self.entries
            .iter()
            .filter(|(_, o)| *o == WriteOutcome::SkippedUserFile)
            .map(|(p, _)| p.as_path())
            .collect()
    }

    /// Text listing, relative to `base` when possible — one `  <outcome>  <path>` per file.
    pub fn render_text(&self, base: &Path) -> String {
        let mut s = String::new();
        for (path, outcome) in &self.entries {
            let shown = path.strip_prefix(base).unwrap_or(path);
            s.push_str(&format!("  {:<9} {}\n", outcome.label(), shown.display()));
        }
        s
    }

    pub fn to_json(&self, base: &Path) -> serde_json::Value {
        serde_json::Value::Array(
            self.entries
                .iter()
                .map(|(path, outcome)| {
                    let shown = path.strip_prefix(base).unwrap_or(path);
                    serde_json::json!({
                        "path": shown.display().to_string(),
                        "outcome": outcome.label(),
                    })
                })
                .collect(),
        )
    }
}

/// Validate a scaffold name: lowercase kebab-case, starting with a letter — the shape
/// every generated id (package dir, process id, channel name) derives from.
pub fn validate_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && !name.ends_with('-')
        && !name.contains("--")
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "'{name}' is not a valid name (lowercase kebab-case starting with a letter, \
             e.g. payments-intake)"
        ))
    }
}

/// `payments-intake` → `PaymentsIntake` (message-type / display derivations).
pub fn pascal_case(name: &str) -> String {
    name.split(['-', '_'])
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut chars = s.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::scratch_dir;

    #[test]
    fn render_replaces_all_tokens() {
        assert_eq!(
            render("a %%X%% b %%Y%% %%X%%", &[("X", "1"), ("Y", "2")]),
            "a 1 b 2 1"
        );
    }

    #[test]
    fn generated_write_semantics_follow_f5() {
        let dir = scratch_dir("scaffold-f5");
        let path = dir.join("out/file.yaml");
        let generated_v1 = "# generated-by: sutra test\nv: 1\n";
        let generated_v2 = "# generated-by: sutra test\nv: 2\n";

        // Absent → created; identical → unchanged; marker present → regenerated.
        assert_eq!(
            write_generated(&path, generated_v1, false).unwrap(),
            WriteOutcome::Created
        );
        assert_eq!(
            write_generated(&path, generated_v1, false).unwrap(),
            WriteOutcome::Unchanged
        );
        assert_eq!(
            write_generated(&path, generated_v2, false).unwrap(),
            WriteOutcome::Updated
        );

        // User content (marker removed) → never touched without force.
        std::fs::write(&path, "hand-tuned: true\n").unwrap();
        assert_eq!(
            write_generated(&path, generated_v2, false).unwrap(),
            WriteOutcome::SkippedUserFile
        );
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "hand-tuned: true\n"
        );
        assert_eq!(
            write_generated(&path, generated_v2, true).unwrap(),
            WriteOutcome::Updated
        );
    }

    #[test]
    fn pristine_write_never_clobbers() {
        let dir = scratch_dir("scaffold-pristine");
        let path = dir.join("file.txt");
        assert_eq!(write_pristine(&path, "one").unwrap(), WriteOutcome::Created);
        assert_eq!(
            write_pristine(&path, "two").unwrap(),
            WriteOutcome::SkippedExisting
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "one");
        assert_eq!(
            write_pristine(&path, "one").unwrap(),
            WriteOutcome::Unchanged
        );
    }

    #[test]
    fn names_validate_and_pascalize() {
        assert!(validate_name("payments-intake").is_ok());
        assert!(validate_name("a1-b2").is_ok());
        assert!(validate_name("Payments").is_err());
        assert!(validate_name("1abc").is_err());
        assert!(validate_name("a--b").is_err());
        assert!(validate_name("a-").is_err());
        assert!(validate_name("").is_err());
        assert_eq!(pascal_case("payments-intake"), "PaymentsIntake");
        assert_eq!(pascal_case("sample"), "Sample");
    }
}
