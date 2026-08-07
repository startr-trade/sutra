# `sutra` CLI reference

The single `sutra` binary (crate `rust/crates/sutra-cli`) carries the whole authoring-to-operations
toolchain: scaffolding, validation, packaging, deployment, and analysis. Build it from source:

```bash
cd rust && cargo build --release -p sutra-cli    # -> rust/target/release/sutra
cargo run -p sutra-cli -- <args>                  # or run ad hoc
```

Commands are grouped below by workflow: author → validate → package → deploy → operate → analyze,
with the generators last.

## Global conventions

| Convention | Behavior |
|---|---|
| `--format <FORMAT>` | `text` (default) or `json` for report commands; `dot` or `mermaid` for `dispatch-graph` |
| `-v` / `-vv` / `-vvv` | Log verbosity (info / debug / trace); logs always go to **stderr**, reports to stdout |
| Exit `0` | Clean run, no findings |
| Exit `1` | Findings — the *input* has a diagnosable problem (breaking compat change, routing miss, coverage below threshold) |
| Exit `2` | Usage or infrastructure — bad flags, missing files, unparseable input, unreachable database |

Database-touching commands (`migrate`, `coverage check/reset`, `crypto`) share one set of
connection options, each with an env fallback: `--url` (`SUTRA_DB_URL`), `--user`
(`SUTRA_DB_USERNAME`), `--password` (`SUTRA_DB_PASSWORD`), and — for `migrate` — `--schema`
(`SUTRA_DB_SCHEMA`). These are the CLI's own database-connection names, distinct from the engine's
`SUTRA_DATASOURCE_*` — see [Configuration reference](../operating/configuration.md).

---

## Author

### `sutra create app`

```
sutra create app <NAME> [--dir <DIR>]
```

Scaffolds an application workspace: a sample standalone deployment package under
`packages/<name>-main/` plus deploy assets (a compose file, a deployments drop-directory, a k8s
manifest, a health-gated smoke script). Idempotent-safe — existing files are never overwritten.
See [Quickstart](../getting-started/quickstart.md) and [Anatomy of an app](../getting-started/first-app.md).

### `sutra create deployment`

```
sutra create deployment <NAME> [--dir <DIR>] [--from <PACKAGE_DIR>]
```

Scaffolds a fresh standalone deployment-package skeleton, or copies an existing package with
`--from` — the explicit variant model: packages never inherit, a variant is a copy. See
[Deployment packages](../building/deployment-packages.md).

### `sutra create bpmn`

```
sutra create bpmn <PROCESS> [--package <PACKAGE_DIR>] [--validation fatal|soft] [flags]
```

Generates a process with the validation-gateway wiring (plus accepted/rejected reply templates)
into a package, verified through the engine's own BPMN loader before being written.

| Flag | Meaning |
|---|---|
| `--validation <MODE>` | `fatal` (default): only FATAL outcomes take the rejected branch; `soft`: FATAL and SOFT_ERRORS both reject |
| `--channel <NAME>` | Inbound channel the start event subscribes to (default `<process>-in`) |
| `--message-type <TYPE>` | Inbound message type (default `<Process>Request`) |
| `--force` | Overwrite an existing user-edited file (never implicit) |

---

## Validate

### `sutra lint`

```
sutra lint <PACKAGE_DIR>
```

Runs the full package-time validation suite (the same fail-closed checks `sutra package` runs)
and emits nothing on success. The fast pre-flight before every `package`.

### `sutra describe`

```
sutra describe <BPMN_FILE> [--format json]
```

Prints a structural summary of a BPMN file: processes, events, tasks, gateways, channels. See
[Troubleshooting BPMN solutions](../operating/troubleshooting.md).

### `sutra dispatch-graph`

```
sutra dispatch-graph <BPMN_FILE> --format dot|mermaid
```

Emits a graphviz or mermaid diagram of a BPMN file's dispatch tree.

### `sutra simulate`

```
sutra simulate <BPMN_FILE> --channel <CHANNEL> --dry-run
```

Reports which process an inbound channel message would route to. `--dry-run` is required —
routing report only, no execution.

### `sutra explain`

```
sutra explain [EXPRESSION] [--context <FILE>]
```

Evaluates a FEEL expression — one-shot, or a REPL on stdin when the expression is omitted. See
[Rules: DMN, FEEL, and .srl](../building/rules.md).

### `sutra compat-baseline`

```
sutra compat-baseline --baseline <PATH_OR_REF> [--current <PATH>] [flags]
```

Compares current BPMN signatures against a baseline directory or git ref and reports breaking
changes — a CI gate on process-contract compatibility between releases.

---

## Package

### `sutra package`

```
sutra package <INPUT> [-o|--out <DIR>]
```

Seals a deployment-package directory into one immutable `.sutra` archive, running the full
validation suite fail-closed first. The archive manifest (per-file digests, the content-addressed
deployment id) is derived, never authored. See [Deployment packages](../building/deployment-packages.md).

### `sutra deployments list`

```
sutra deployments list <DIR> [--label KEY=VALUE]...
```

Inspects a directory of sealed `.sutra` archives — lists each archive's deployment id and labels.

### `sutra openapi`

```
sutra openapi <ARCHIVE>
```

Emits a sealed archive's generated OpenAPI 3.1 surface (channels → BPMNs, message types, endpoint
nature, data stores) — the same document the engine serves live per deployment id.

---

## Deploy

### `sutra migrate`

```
sutra migrate [status|verify] [--url <URL>] [--dry-run] [flags]
```

Applies engine schema migrations to the engine-internal database, or inspects them (`status` —
applied vs. pending; `verify` — ledger integrity, expected head, checksum drift).

### `sutra crypto provision-dek`

```
sutra crypto provision-dek --key-id <KEY_ID> --kek <REF> [--url <URL>] [flags]
```

Provisions a KEK-wrapped per-tenant data-encryption key for envelope encryption of sensitive
instance variables at rest.

### `sutra deploy`

```
sutra deploy [ARCHIVE] [flags]
```

Hot-deploys a sealed `.sutra` archive onto a running engine. See
[Deploy, hot-deploy, and rollback](../operating/deploy-rollback.md) and
[Deployment model](../architecture/deployment-model.md) for the full mechanics.

| Flag | Meaning |
|---|---|
| `--api` | Deploy via the engine's synchronous admin API instead of a ConfigMap patch; requires `--engine-url` |
| `--async` | With `--api`: submit async, then poll until Active |
| `--watch <PKG_DIR>` | Watch a package source directory and re-deploy on change (validate-then-deploy loop); implies `--api` |
| `--wait` / `--wait-timeout <SECS>` | ConfigMap path: poll until Active |
| `--engine-url <URL>` | Engine base URL for `--wait` / `--api` |
| `--api-key <KEY>` / `--api-key-header <HEADER>` | Admin auth for `--api` (default header `X-API-Key`) |
| `--secret <KEY=VALUE>` / `--secret-from <FILE>` | Estate-secret keys ensured/merged **before** the ConfigMap patch |

### `sutra undeploy`

```
sutra undeploy <DEPLOYMENT> [flags]
```

Removes a deployment — the engine drains it (no new intake) and retires it at zero instances and
zero pending outbox.

---

## Operate and analyze

### `sutra coverage` {#sutra-coverage}

```
sutra coverage init  <FILE> [PROCESS_ID]... [flags]   # seed declarations / scaffold admin set
sutra coverage check [BPMN_FILE] [flags]              # drift lint, or the store-backed check
sutra coverage reset --archive <FILE> [flags]         # re-seed the store covered=false
```

Path-coverage tooling for `q:coverage` route declarations (intra-process) and `coverage/*.yaml`
files (cross-process) — see
[Coverage: declared routes as the compliance signal](../building/coverage.md) for the full
walkthrough of both shapes and every flag above, and
[Troubleshooting BPMN solutions](../operating/troubleshooting.md#sutra-coverage-check--is-my-compliance-path-actually-being-exercised)
for reading a report that doesn't match what you expected.

### `sutra test simulate` {#sutra-test-simulate}

```
sutra test simulate --deployments <DIR> --datasource <URL>
                    (--advance <DURATION> | --until-quiescent) [flags]
```

Boots a real engine on a dynamic port against a directory of sealed deployment archives with a
**virtual clock** installed, fast-forwards it, reports, and shuts down — so a `PT24H` timer or an
`R3/PT12H` schedule settles in wall-clock seconds. Unrelated to `sutra simulate` above, which is a
dry-run routing report over one BPMN file and boots nothing.

| Flag | Meaning |
|---|---|
| `--deployments <DIR>` | Directory of sealed `.sutra` archives to serve (required) |
| `--datasource <URL>` / `--datasource-username` / `--datasource-password` | Engine datasource — the engine's own `SUTRA_DATASOURCE_*` env names, not the CLI's `SUTRA_DB_*` set (required) |
| `--advance <DURATION>` | Fast-forward the virtual clock by this ISO-8601 duration, firing everything due along the way, then stop |
| `--until-quiescent` | Fast-forward until nothing is armed and nothing is live, or `--timeout` elapses |
| `--timeout <DURATION>` | **Real** wall-clock budget for the fast-forward loop, either mode (default `PT30S`) |
| `--start <RFC3339>` | Virtual start instant (default: the real current instant) |
| `--allow-existing-data` | Proceed even though the datasource already holds instances |

Exactly one of `--advance` / `--until-quiescent` is required. **Safety:** the target database must
hold no instances or the run refuses with exit `2` — pointing this at a database with real
in-flight instances would durably fire their real timers early; `--allow-existing-data` is the
explicit acknowledgement.

Progress is text on **stderr**; the final summary is one JSON object on **stdout** and nothing
else touches stdout, so `sutra test simulate … | jq .` is always safe. See
[Testing time](../building/testing-time.md) for the summary's fields and the embedded seam behind
this command.

### `sutra audit-replay`

```
sutra audit-replay <INSTANCE_ID> --from-jsonl <PATH> [--tenant <TENANT>] [--until <EVENT_TYPE>]
```

Walks a process instance's audit events from a JSONL stream — reconstructing what a specific
instance did, offline. See [Logging and audit](../operating/logging.md).

### `sutra version`

```
sutra version            # sutra 0.2.0-rc.1
sutra --version          # identical text
sutra version --format json
```

Prints the tool version. The program name is **derived from the running binary**, not
hardcoded: a distribution that embeds this CLI as a library (`sutra_cli::run`) under its own
binary name prints that name, and one that versions itself independently of the engine
(`sutra_cli::run_with_version`) prints its own version with the embedded engine's underneath:

```
<tool> 2.0.0
sutra  0.2.0-rc.1 (engine)
```

`--format json` is the structured form and always separates the two — `version` is the
reporting tool's own, `engine` the embedded engine's (equal for this binary):

```json
{"name":"sutra","version":"0.2.0-rc.1","engine":"0.2.0-rc.1"}
```

---

## Generate

### `sutra docgen`

```
sutra docgen --input <FOLDER> [--output <DIR>] [--check]
```

Recurses a folder of authored deployment artifacts — BPMN processes, DMN/`.srl` rules,
Handlebars/XSLT templates and their manifests, `channels.yaml`, `package.yaml`, coverage files —
and emits a deterministic markdown catalog, one page per artifact. It parses through the engine's
*own* loaders, so each page describes exactly what the engine loads rather than a second parser's
opinion of it. `--check` generates into a temporary directory and reports drift instead of writing
anything, which is the shape a CI or pre-commit gate wants. `sutra catalog` is its sibling for
Rust source — same `--output` / `--check` contract, one page per source file, rooted at
`--repo-root`.

### `sutra schemagen`

```
sutra schemagen generate <SCHEMAS_DIR> <OUT_DIR> [--full]
sutra schemagen check    <SCHEMAS_DIR> <TREE_DIR> [--full]
```

Compiles a directory of XSD schemas into Rust sources: the decode tables, the canonical map
projection, and the shape metadata a schema-bound codec is built on. The schema files are the only
input, and emission is byte-identical run to run after `rustfmt` — which is what makes `check`
(regenerate in memory, diff against a committed tree, exit `1` on any difference) a usable drift
gate. The default emission is the slim, data-driven form; `--full` additionally emits the typed
model. `generate` writes only the files the generator itself produces and never touches
hand-maintained ones alongside them.

Both paths are arguments, not conventions: `schemagen` is a neutral tool over whatever corpus you
point it at, and the crate it emits lives wherever the caller wants it — including in a repository
that composes this one.
