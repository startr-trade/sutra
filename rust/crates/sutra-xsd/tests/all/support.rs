//! Path helpers shared by this crate's integration tests.

use std::path::PathBuf;

pub fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn repo_root() -> PathBuf {
    crate_root()
        .ancestors()
        .nth(3)
        .expect("crate lives at rust/crates/sutra-xsd")
        .to_path_buf()
}

/// The AUTHORED schema fixtures this crate's tests compile and shape-check against
/// (`tests/data/schemas/`, see its `provenance.md`). Nothing published is vendored here — the
/// fixtures reproduce the Standards-Editor *idiom* the Tier-1 subset targets, not anyone's
/// message content.
pub fn fixtures_dir() -> PathBuf {
    crate_root().join("tests/data/schemas")
}

/// Every `.xsd` fixture, sorted.
pub fn fixtures() -> Vec<PathBuf> {
    let dir = fixtures_dir();
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("schema fixtures at {}: {e}", dir.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "xsd"))
        .collect();
    entries.sort();
    entries
}

/// Read + compile a fixture by file name.
pub fn fixture(name: &str) -> sutra_xsd::Schema {
    let path = fixtures_dir().join(name);
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    sutra_xsd::Schema::compile(&bytes)
        .unwrap_or_else(|e| panic!("{} must compile:\n{e}", path.display()))
}

/// Read + compile a schema at a REPO-relative path (the example modules' own schemas, which
/// live in the public tree and are referenced in place).
pub fn repo_schema(rel: &str) -> Vec<u8> {
    let path = repo_root().join(rel);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}
