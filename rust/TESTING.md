# Testing the Sutra Rust workspace

The suite is split into **three tiers** by cost and infrastructure. The default
`cargo test` is tier-1 and needs nothing but a Rust toolchain; docker and Kubernetes
are opt-in. Root `Makefile` targets wrap the canonical commands (`make help` lists them).

## Tiers

| Tier | What | How to run | Needs |
|------|------|-----------|-------|
| **1** | Unit tests (`src/`) + no-docker integration tests | `make test` → `cargo test --workspace` | Rust toolchain only |
| **2** | Container (testcontainers) integration tests — PostgreSQL, MySQL/MariaDB, SQL Server, RabbitMQ | `make test-docker` → `cargo test --workspace -- --ignored` | Docker daemon |
| **3** | Kubernetes ITs (kind cluster, tofu-provisioned) | `make test-k8s` → `cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_` | kind cluster up (`make -C deploy/k8s-it init`) |

Every container-spawning test is annotated `#[ignore = "docker"]`, so a bare
`cargo test` (tier-1) **skips them** — it runs fast and needs no docker. The ignored
tests are the tier-2 set; `--ignored` runs *only* them, `--include-ignored` runs both.

```bash
make test                      # tier-1: no docker, the default gate while iterating
make test-docker               # tier-2: every docker suite in the workspace
make test-docker P=sutra-persistence   # tier-2, one crate only
make test-all                  # tier-1 + tier-2 (cargo test --workspace -- --include-ignored)
make test-k8s                  # tier-3: the k8s conformance suites (cluster must be up)
make docker-clean              # reap leaked fixtures (see "Reaper" below)
```

Raw cargo equivalents:

```bash
cargo test --workspace                       # tier-1
cargo test --workspace -- --ignored          # tier-2 (all docker suites)
cargo test -p sutra-persistence -- --ignored # tier-2, one crate
cargo test --workspace -- --include-ignored  # tier-1 + tier-2
```

### N-lane reruns

The engine's shard router (`sutra.engine.shards`, execution scale-out §8) defaults to one
actor lane, and the acceptance bar for turning it up is the EXISTING suites passing verbatim
at N > 1 — same tests, same expectations, more lanes underneath. Two knobs re-run a tier
that way; both are read at boot only, so nothing per-test changes, and both are inert when
unset (no env is injected at all — the default runs are byte-identical):

```bash
# in-process serve() boots — the engine's own ITs
SUTRA_ENGINE_SHARDS=4 cargo test -p sutra-engine --test all -- --ignored --skip k8s_

# black-box: the sutra-rust-engine:dev CONTAINER the conformance suites boot
SUTRA_CONFORMANCE_SHARDS=4 cargo test -p sutra-conformance -- --ignored --skip k8s_
SUTRA_CONFORMANCE_SHARDS=4 make test-docker P=sutra-conformance
```

`SUTRA_CONFORMANCE_SHARDS` is the container-side seam
(`sutra_testkit::conformance::engine`): it sets every engine container's
`SUTRA_ENGINE_SHARDS`, and a suite that needs a specific count regardless pins its own with
`EngineBuilder::shards(n)`. A container's LIVE lane count is readable black-box at
`/sutra/health/ready` → `checks[0].data.shards` (read off the running router, not echoed from
config); `tc_shard_lanes` asserts it. Rebuild the image first (`make image`) — the tag is
mutable, and an N-lane run against yesterday's binary proves nothing.

## Test layout & filters

Each crate's many top-level integration files are consolidated into **one**
`tests/all.rs` per crate that declares the original files as modules under `tests/all/`
(one linked test binary instead of one-per-file — a large link-time saving, since every
integration file is otherwise its own crate that re-links the whole dependency graph).
**Module names preserve the original file stems**, so filters are unchanged:

```bash
cargo test -p sutra-xsd shape_tables::                     # one module (was shape_tables.rs)
cargo test -p sutra-xsd compile_subset::                   # another module (was compile_subset.rs)
cargo test -p sutra-executor gateways_test::               # module filter
```

Unit tests live in `src/` (`#[cfg(test)] mod tests`) and are **not** part of the
consolidation — they always compile into their crate's lib-test binary.

Two crates keep a per-dialect binary layout instead of a single `tests/all.rs`:
`sutra-datastore` and `sutra-persistence` use explicit `[[test]]` targets with
`required-features` (`mysql` / `mssql`) so a `--no-default-features` build can drop a
dialect cleanly — folding them into one binary would defeat that gating. Their suites are
entirely docker, hence entirely `#[ignore = "docker"]`.

## Reaper — why fixtures don't leak (and when they can)

`testcontainers-rs` has **no ryuk sidecar** (unlike other Testcontainers implementations): container
cleanup relies on the `Drop` impl of the container handle. But the shared-fixture pattern
parks the handle in a `static OnceLock<(Container, port)>` so the whole test binary reuses
one container — and **Rust never drops `static`s at process exit**, so that `Drop` never
runs and the container would leak.

`sutra_testkit::reap_on_exit(container.id())` closes the gap: each fixture registers its
container id, and the first call installs a single `libc::atexit` hook per process. When
the test binary exits normally, the hook force-removes every registered container in one
`docker rm -f`. Because a consolidated `tests/all.rs` is **one process**, all its module
fixtures register into the one registry and the one atexit hook reaps them together —
consolidation does not weaken the reaper (it is exercised by any tier-2 run; a `docker ps`
after a docker suite shows zero leaked fixtures).

Caveat: `atexit` handlers — like every Rust destructor — do **not** run on `SIGKILL` or a
hard crash. If a docker run is killed (e.g. `kill -9`, OOM, CI cancel), fixtures can
survive. `make docker-clean` (`scripts/dev-docker-cleanup.sh`) reaps leaked test
containers + dangling volumes/images older than a cutoff (default 30 min, `CUTOFF=<min>`
to change it) without touching the kind cluster, the local registry, or the docs container.

## Conventions

- Add a new integration test as a module of the crate's `tests/all.rs` (a new file under
  `tests/all/` + one `mod <name>;` line), not as a new top-level `tests/*.rs` (which would
  reintroduce a separate link unit).
- A test that spawns a container gets `#[ignore = "docker"]` (per-test, or on the whole
  module if it is entirely docker) and must call `sutra_testkit::reap_on_exit` on its
  container id.
- Keep tier-1 green while iterating; run the touched crate's tier-2 (`make test-docker
  P=<crate>`) before handing off. The full workspace tier-2 + tier-3 sweep is batched at
  milestones, not on every change.

## Generated code

No crate is carved out of the routine tiers: `make test`, `make test-docker`, `make test-all`
and `make lint` cover the whole workspace. This workspace holds no generated binding crate — a
schema corpus large enough to need one belongs to the extension crate that owns that message
standard, along with whatever slow gate it needs.

What stays here is the GENERATOR, `sutra-schema-gen` (shipped as `sutra generate schema-handler`), fast to
build and fully covered by tier-1 — including a byte-equality golden gate that regenerates an
authored fixture schema under `crates/sutra-schema-gen/tests/data/` and compares it to the
committed emission beside it. If you regenerate a downstream tree, the drift gate is
`sutra generate schema-handler <schemas-dir> <tree-dir> --check`: text-level, no rustc, and both paths are
arbitrary.

Still worth avoiding: ad-hoc `--all-features` invocations. Feature-set churn busts the build
cache for everyone on a shared target dir; keep feature sets stable and let the close-out
checkpoints own the wide sweeps.
