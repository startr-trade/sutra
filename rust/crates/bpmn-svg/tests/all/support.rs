//! Shared helpers: the fixture folder and exact-output checks.
//!
//! An expected document changes only on purpose: after one, re-bless with
//! `BLESS=1 cargo test -p bpmn-svg --test all` and read the diff.

use std::fs;
use std::path::PathBuf;

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A fixture's BPMN source.
pub fn bpmn(name: &str) -> String {
    fs::read_to_string(fixtures_dir().join(format!("{name}.bpmn"))).unwrap()
}

/// Compare `svg` with `tests/fixtures/<name>.svg`, writing it first under `BLESS`.
pub fn expect_svg(name: &str, svg: &str) {
    let path = fixtures_dir().join(format!("{name}.svg"));
    if std::env::var_os("BLESS").is_some() {
        fs::write(&path, svg).unwrap();
    }
    let expected = fs::read_to_string(&path).unwrap();
    assert!(svg == expected, "{name}: output differs from {path:?}");
}

/// Every `id` on a `<g>` group, in output order.
pub fn group_ids(svg: &str) -> Vec<String> {
    svg.split("<g ")
        .skip(1)
        .filter_map(|g| {
            let tag = &g[..g.find('>').unwrap()];
            tag.split(" id=\"")
                .nth(1)
                .map(|rest| rest[..rest.find('"').unwrap()].to_string())
        })
        .collect()
}

/// The opening tag of the group whose `id` is `id`.
pub fn group_tag<'a>(svg: &'a str, id: &str) -> &'a str {
    let at = svg
        .find(&format!(" id=\"{id}\""))
        .unwrap_or_else(|| panic!("no group {id}"));
    let start = svg[..at].rfind("<g ").unwrap();
    let end = at + svg[at..].find('>').unwrap() + 1;
    &svg[start..end]
}
