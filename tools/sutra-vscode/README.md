# sutra-vscode

VS Code extension + Language Server for **sutra** BPMN files.

Provides editor support for BPMN documents that use the `q:*` extension
namespace defined by sutra: codec/validator/channel id validation,
FEEL `payload.X` go-to-definition into `<q:source>` schemas, and
attribute-value completion driven by a workspace registry.

> Status: **canary**. The codec/validator/channel id pool is a hard-coded
> placeholder for now — a fixed sample list, not the engine's real built-in
> set. Workspace-config loading ships in a later wave.

## Features

| Feature | Trigger | Notes |
|---------|---------|-------|
| Diagnostics | on open / change | parse errors + unknown id refs |
| Completion | typing `:` inside `codec=""` / `validator=""` / `channel=""` | mock pool |
| Go-to-definition | `payload.<field>` in `<q:expression>` / `<q:condition>` / `<q:guard>` | jumps to `<q:source>` |
| Hover | on q:* elements and referencing attrs | markdown summary |

## Layout

```
tools/sutra-vscode/
  package.json                   # extension manifest + npm scripts
  language-configuration.json    # brackets / comments
  syntaxes/sutra.tmLanguage.json   # textmate grammar (XML + FEEL highlight)
  server/                        # LSP server (Node)
    src/
      server.ts                  # entry point (stdio / IPC)
      parser.ts                  # StAX-style walker
      symbols.ts                 # symbol-table builder
      registry.ts                # mock workspace registry
      diagnostics.ts             # diag computation
      completion.ts              # completion provider
      definition.ts              # go-to-def provider
      hover.ts                   # hover provider
      __tests__/                 # vitest suites
  client/                        # VS Code extension shell
    src/extension.ts
```

## Build & test

```bash
cd tools/sutra-vscode
npm install
npm run build   # tsc -b server+client, then copies the WASM glue into server/out
npm test
```

Tests run under [vitest](https://vitest.dev/). All canary suites are
pure unit tests — they do not stand up an LSP, so they're fast and
deterministic.

## Cross-file WASM lint

The heavy schema/codec/FEEL deploy-time checks (the ones that need
`channels.yaml` / `schemas/**` / templates, not just the open document) run
in-editor by compiling the Rust `sutra lint` core (`rust/crates/sutra-lint-core`)
to WASM and calling it from the LSP server — so **in-editor == deploy-time by
construction** (same code, no TypeScript re-implementation to drift). The cheap
single-document checks stay in TypeScript (`staticValidation.ts`); the WASM adds
the cross-file ones alongside them.

- **Target**: `wasm32-unknown-unknown` + `wasm-bindgen --target nodejs` (the LSP
  server is a Node child process). The generated CommonJS glue + `.wasm` live in
  `server/src/generated/` (committed — they are the build product the VSIX ships).
- **Regenerate** whenever the Rust core changes:

  ```bash
  npm run build:wasm   # cargo build (wasm32-unknown-unknown, release) + wasm-bindgen
  ```

  `npm run build` does **not** invoke cargo; it consumes the committed glue and
  copies it to `server/out/generated/` (via `postbuild`) so the compiled server
  can load it at runtime.
- **Flow**: `workspaceConfig.gatherDeploymentFiles` collects the deployment's
  interior files around the open `.bpmn`, `wasmValidation.ts` calls
  `lint(requestJson)` and maps each diagnostic that concerns the open document to
  an editor range (BPMN node/process anchors → the element's tag; unresolvable →
  a file-level 0:0 squiggle). The pass is debounced + async so it never blocks the
  event loop, and degrades to "no cross-file diagnostics" if the WASM is absent.

## Packaging

```bash
npm run package   # vsce package  → sutra-vscode-<version>.vsix
```

`.vscodeignore` keeps the compiled `server/out/**` (including the WASM glue) and
production `node_modules`, and excludes the TypeScript sources + tests.

## Screenshot

> _placeholder — replace with `docs/screenshots/sutra-vscode.png` once the
> extension is wired into a VS Code dev host._

## Deferred

- BPMN preview pane.
- Alias-index browser.
- Workspace-config loader (real codec/validator/channel ids).
- JetBrains packaging via LSP4IJ.
