# sutra-testkit

Dev-only test-support library for the Rust integration suites. Never linked by production
crates — added under `[dev-dependencies]` only. Two layers:

| Layer | Gate | Contents |
|---|---|---|
| Container reaping | default | `reap_on_exit` / `reap_network_on_exit` — the atexit hook (below) |
| Conformance harness | `features = ["conformance"]` | `sutra_testkit::conformance::{util, engine, broker, callback, compose, k8s}` |

## The conformance harness (`feature = "conformance"`)

The end-to-end harness the `sutra-conformance` suites run on: postgres + rabbitmq + engine
container fixtures, the DRY-variant composer (`compose_variant`), the host-HTTP and broker
recorders, and the tier-3 k8s plumbing (`run_cli`, `deploy_api`, `await_rollout`,
`engine_image`, `kubeconfig_path`, the MetalLB/ingress resolvers).

It is a **library**, not a suite, so a conformance crate in ANOTHER workspace — one composing
this engine as a path or submodule dependency — drives the same fixtures and contributes only
its own payloads and assertions. `sutra-conformance/tests/all/support.rs` is a thin re-export of
exactly these modules.

Out-of-workspace callers redirect the harness at their own tree:

| Variable | Redirects |
|---|---|
| `SUTRA_REPO_ROOT` | the repo root (default: the tree this testkit was compiled from) |
| `SUTRA_EXAMPLES_DIR` | the examples tree (default: `<repo root>/examples`) |
| `SUTRA_CLI` | the `sutra` binary (default: build `-p sutra-cli --release`) |
| `SUTRA_ENGINE_IMAGE` | the engine image, tier-2 and tier-3 |
| `SUTRA_KUBECONFIG` | the kind cluster's generated kubeconfig (default: `deploy/k8s-it/cluster/sutra-fednow-it-config` — the filename is historical and pinned by the cluster name) |

The feature gate exists so the ~10 crates that dev-depend on the reaper alone never build
testcontainers, lapin or kube-rs.

**Fixture handles must not be dropped on a tokio worker** — see the `EngineHandle` type docs:
testcontainers' sync `Container::drop` blocks on `docker rm -f`, which panics inside a runtime.
Suites build fixtures on a dedicated `std::thread` and park them in a `static OnceLock`; the
atexit reaper, not `Drop`, is what cleans up.

## Container reaping (`reap_on_exit`)

### The problem

The DB/broker integration suites keep **one testcontainers container per test binary** alive
for the whole run, parking the handle in a `static OnceLock<(Container, u16)>`. That is the
right performance design (one container per binary, not per test) but it leaks containers:

- **testcontainers-rs 0.25 ships no ryuk reaper.** Unlike other testcontainers
  implementations, there is no sidecar that reaps containers by session label when the client
  connection drops — so nothing survives a `SIGKILL`, and there is no
  `TESTCONTAINERS_RYUK_DISABLED` knob because there is no ryuk. Verified by source
  inspection of `testcontainers-0.25.2` (zero `ryuk`/`reaper` symbols).
- The crate's **only** automatic cleanup is the `Drop` impl on the `Container` handle, which
  force-removes the container (`TESTCONTAINERS_COMMAND` defaults to `remove`). Its opt-in
  `watchdog` cargo feature only traps `SIGTERM`/`SIGINT`/`SIGQUIT`, never a normal exit, and
  is not enabled here.
- Rust **never drops `static`s** at process exit, so the shared handle's `Drop` never runs —
  and with no ryuk, the container leaks after the test process is gone (~40–50 stragglers per
  full `cargo test --workspace`).

### The fix

Fixtures call [`reap_on_exit`](src/lib.rs) with each container's id right after `start()`.
That records the id and, on the first call in the process, installs a single `libc::atexit(3)`
hook. When the test process terminates **normally** (libtest returning from `main`, or
`exit()`), the hook force-removes every registered container in one `docker rm -f` call.

Proven: `cargo test -p sutra-persistence` (all four dialect containers — pg/mysql/mariadb/
mssql) leaves **zero** test containers the instant the process exits (the atexit `docker rm -f`
is synchronous, so well within any grace window).

### Residual case: SIGKILL / crash / `kill -9`

`atexit(3)` handlers — like every Rust destructor — do **not** run on `SIGKILL`, a hard crash,
or `kill -9`. Reaping those is exactly what ryuk would do, and ryuk does not exist in this
crate version. For that residual case only, run:

```sh
scripts/dev-docker-cleanup.sh [cutoff-minutes]   # default 30
```

which force-removes leaked test-image containers (postgres/mysql/mariadb/mssql/rabbitmq,
`sutra-rust-engine:*`) older than the cutoff, leaving the kind cluster, local registry, and
mkdocs container untouched.
