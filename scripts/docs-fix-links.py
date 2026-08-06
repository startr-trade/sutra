#!/usr/bin/env python3
"""
Post-process HAND-WRITTEN docs under docs/ so they build clean under MkDocs.

Scope: this script ONLY touches author-written pages. It deliberately skips the
generated catalog tree — its generator owns mkdocs-friendliness for those pages
and is the right place to fix any catalog link issue.

What this does:
  Strip Markdown links whose resolved target is anywhere other than an actual
  file/directory inside docs/. That covers two failure modes the same way:
    (a) `[Foo.rs](../../rust/crates/sutra-bpmn/src/loader.rs)` — link to a source file
        outside docs/. The docs site is self-contained; source files live in
        git, not on the docs site.
    (b) `[extensions/](../extensions)` written from a nested page under `docs/`
        — depth-arithmetic bug; resolves to the phantom `docs/extensions/`
        which doesn't exist on disk.

  The link label is kept verbatim (so `[loader.rs](…)` becomes plain
  `loader.rs`). Backticks / code formatting inside the label survive.

Idempotent — running twice produces no changes.

Run from repo root:  python3 scripts/docs-fix-links.py
"""
from __future__ import annotations
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS = REPO_ROOT / "docs"
CATALOG = DOCS / "design" / "artifact-documentation"

# Matches Markdown link targets — captures the text and the URL.
# Skips reference-style links and images.
LINK_RX = re.compile(r"(?<!\!)\[([^\]]+)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


def is_external(url: str) -> bool:
    return url.startswith(("http://", "https://", "mailto:", "tel:", "#"))


def resolve_relative(doc_path: Path, link: str) -> Path:
    """Resolve `link` relative to the directory containing `doc_path`.
    Strips any URL fragment (#…) and query (?…) before resolving."""
    bare = link.split("#", 1)[0].split("?", 1)[0]
    return (doc_path.parent / bare).resolve()


def link_target_missing(resolved: Path) -> bool:
    """True iff `resolved` is not a real file/directory under docs/.

    Two failure modes — same outcome:
      (a) Path is outside docs/ entirely (source tree, escaped above repo, …).
      (b) Path is nominally inside docs/ but doesn't exist on disk — typically
          a depth-arithmetic bug where the author wrote `..` segments assuming
          the path is relative to the repo root rather than to the page's
          parent directory."""
    try:
        resolved.relative_to(DOCS)
    except ValueError:
        return True  # outside docs/
    return not (resolved.is_file() or resolved.is_dir())


def is_catalog_path(p: Path) -> bool:
    """True iff `p` is inside the depcat-gen-owned catalog tree."""
    try:
        p.relative_to(CATALOG)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Strip pass — hand-written docs only
# ---------------------------------------------------------------------------

def strip_broken_links() -> tuple[int, int]:
    """Returns (files_changed, links_stripped). Skips catalog files (depcat-gen owns those)."""
    files_changed = 0
    links_stripped = 0

    for md in DOCS.rglob("*.md"):
        if is_catalog_path(md):
            continue  # depcat-gen owns catalog mkdocs-friendliness
        text = md.read_text()
        original = text

        def repl(m: re.Match) -> str:
            nonlocal links_stripped
            label, url = m.group(1), m.group(2)
            if is_external(url):
                return m.group(0)
            try:
                resolved = resolve_relative(md, url)
            except (OSError, ValueError):
                return m.group(0)
            if link_target_missing(resolved):
                links_stripped += 1
                return label
            return m.group(0)

        text = LINK_RX.sub(repl, text)

        if text != original:
            md.write_text(text)
            files_changed += 1

    return files_changed, links_stripped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Repo root: {REPO_ROOT}")
    print(f"Docs root: {DOCS}")
    print(f"Catalog:   {CATALOG.relative_to(REPO_ROOT)}  (skipped — its generator owns this tree)")
    print()

    print("Stripping links to source-tree paths and broken intra-docs paths…")
    files_changed, links_stripped = strip_broken_links()
    print(f"  → {links_stripped} links stripped across {files_changed} files")

    return 0


if __name__ == "__main__":
    sys.exit(main())
