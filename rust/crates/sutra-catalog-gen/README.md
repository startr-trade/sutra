# sutra-catalog-gen

The Rust catalog generator. It emits the artifact-documentation page system for the
Rust workspace under `rust/crates/**`, in the same page shape the frozen
reference-baseline catalog uses for the non-Rust trees.

## Scope

For every crate in the Rust workspace (`rust/Cargo.toml` members) this tool generates,
under the catalog output root's `rust/` subtree:

- **One page per Rust source file** (`rust/crates/<crate>/src/**/*.rs` → `…/src/**/*.md`) —
  the module's doc summary, an item inventory (structs / enums / traits / functions /
  methods / constants / statics / type-aliases / macros / modules), and a **Relationships**
  table with **bidirectional** links (`References` / `Referenced by`, plus the crates it
  depends on).
- **One crate-index page per crate** — the crate's `Cargo.toml` mirrored as
  `rust/crates/<crate>/Cargo.md`: package summary, the workspace crates it depends on and is
  depended on by (bidirectional), and the module map (links to every source-file page).
- **One workspace-root page** — `rust/Cargo.toml` mirrored as `rust/Cargo.md`: the list of
  member crates. This is the Rust catalog's root index.

The pages mirror the source tree, share the section shape of the frozen non-Rust pages
(heading, `## Members`-style inventory tables,
`## Relationships`, `## Stability`), the `<!-- GENERATED … -->` sentinel, and the
`<!-- MANUAL NOTES BELOW -->` splice that preserves hand-written notes across regeneration.

## Parsing (stable toolchain)

Parsing is done with [`syn`](https://docs.rs/syn) on the **stable** toolchain. rustdoc-JSON is
nightly-only and is deliberately **not** used — the workspace pins stable and the catalog must
build in CI without a nightly toolchain.

References are extracted from **path-qualified `use` trees** (and each `Cargo.toml`'s path
dependencies), never by matching bare capitalised words against prose — this avoids the
name-collision defect that once polluted the earlier catalog. Name resolution is best-effort
(module tree walk per crate); output is **deterministic** — every list is sorted and the
`--check` diff is byte-stable across runs.

## Ownership of the output tree

This is the only live catalog generator. The pages generated for the retired reference tree
are frozen in place as reference documentation and are never regenerated; this tool owns —
and writes only — the `rust/` subtree of the catalog (source-file pages, `Cargo.md`
crate/workspace pages). It never touches the frozen folder `index.md` pages or the
hand-curated root `index.md`.

**Scope-evolution rule:** any NEW kind of Rust artifact (a new item category, a new manifest
convention) requires extending this single generator so the catalog stays complete.

**Out of scope:** rendering BPMN diagrams. Diagram rendering is a possible follow-on, not part
of this generator.

## Usage

The crate is a **library**; it ships through the one tooling binary as `sutra generate catalog`:

```
sutra generate catalog --repo-root=<path> --output=<path>           # regenerate in place
sutra generate catalog --repo-root=<path> --output=<path> --check   # CI: exit 1 on drift
```

Defaults: `--repo-root=.` and `--output=<repo-root>/catalog`.
The repo-root `Makefile` `catalog` / `catalog-check` targets are the canonical entry points.
