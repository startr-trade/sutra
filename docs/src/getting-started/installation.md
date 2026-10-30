# Installation

Sutra ships as two things, released together and versioned together:

| | What it is | How you get it |
|---|---|---|
| **`sutra`** | the CLI that scaffolds, packages, lints, deploys and inspects | one-line install, below |
| **`sutra-engine`** | the runtime that executes your processes | a container image, `docker pull` |

Neither needs a Rust toolchain. You only need one to build from source, which is the last
section on this page.

## Install the CLI

```bash
curl -fsSL https://raw.githubusercontent.com/startr-trade/sutra/main/scripts/install.sh | sh
```

On Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/startr-trade/sutra/main/scripts/install.ps1 | iex
```

The installer downloads the release build for your platform, **verifies its SHA-256 against
the release's own `SHA256SUMS`**, and installs it — into `/usr/local/bin` when that is
writable, otherwise `~/.local/bin`. It edits no shell profile and starts no daemon.

Useful knobs:

```bash
# Pin a version instead of taking the newest release
curl -fsSL …/install.sh | sh -s -- --version v0.6.12

# Install somewhere specific
curl -fsSL …/install.sh | SUTRA_INSTALL_DIR=~/bin sh

# Lift the API rate limit when retrying (60 requests/hour anonymous -> 5000 with a token)
GH_TOKEN=$(gh auth token) curl -fsSL …/install.sh | sh
```

### If the install fails

`raw.githubusercontent.com` **rate-limits** and answers `429 Too Many Requests` under load; so
does the GitHub API, at 60 requests an hour per IP without a token. Three ways around it, in the
order worth trying:

```bash
# 1. the SAME installer, served from the release instead of the raw CDN (different host, and
#    the script is versioned with the binaries it installs)
curl -fsSL https://github.com/startr-trade/sutra/releases/download/v0.6.12/install.sh \
  | sh -s -- --version v0.6.12

# 2. a token, which moves the API to 5000 requests an hour
GH_TOKEN=$(gh auth token) curl -fsSL …/install.sh | sh

# 3. no installer at all — the assets are plain files
#    (swap in your target: aarch64-unknown-linux-musl, aarch64-apple-darwin, x86_64-apple-darwin)
gh release download v0.6.12 -R startr-trade/sutra \
  -p 'sutra-*-x86_64-unknown-linux-musl.tar.gz' -p SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
tar xzf sutra-*-x86_64-unknown-linux-musl.tar.gz --strip-components=1 -C ~/.local/bin
```

The installer retries transient failures and times out rather than hanging, and it resolves the
release from three different endpoints (`/releases/latest`, the release list, then git tags),
because each of them has been observed failing while a release was perfectly installable. When
all three come up empty it tells you to pin `--version`, which is the one path that needs no
lookup at all.

Prefer to see what you are running before you run it? Download `install.sh`, read it — it is
about a hundred lines of POSIX shell — then execute it. Or skip the script entirely and grab
the archive yourself from the [releases page](https://github.com/startr-trade/sutra/releases).

Verify the install:

```bash
sutra --version
```

### Published targets

**linux x86_64**, **linux aarch64**, **macOS arm64**, **macOS x86_64**, **windows x86_64**.

| | x86_64 | arm64 |
|---|---|---|
| **Linux** | ✅ static musl | ✅ static musl |
| **macOS** | ✅ built natively | ✅ built natively |
| **Windows** | ✅ MSVC | — |

The Linux builds are **static musl**, so there is no glibc version to match — they run on an old
distribution and a new one alike. The macOS builds are compiled on Apple hardware of each
architecture rather than cross-compiled.

**This table is the CLI.** The engine is a container, and its image is **Linux only**
(`linux/amd64` + `linux/arm64`) — which is not a limitation in practice: on macOS or Windows you
author, lint, package and deploy with the native CLI, and the engine runs as a Linux container
through Docker Desktop, exactly as it does in production. There is no native macOS or Windows
engine build, and none is planned.

On **Apple silicon that container runs natively**, which is why the absence costs so little. The
image is a multi-arch manifest, so `docker pull` resolves to `linux/arm64` on its own, and that
arm64 build is compiled on a native arm64 runner rather than cross-built or emulated. Docker on
macOS is always a Linux VM, and on Apple silicon that VM is arm64 — so you get native-speed
execution of the same artifact production runs. (The reverse would be emulated: an `linux/amd64`
image on Apple silicon goes through Rosetta 2 or QEMU, which is slower and occasionally fails
outright. Nothing here asks you to do that.)

On an Apple-silicon Mac running the installer under Rosetta 2, `uname -m` answers `x86_64`, so
you get the Intel build — it runs, translated. Pass `--version` and fetch the
`aarch64-apple-darwin` archive by hand if you want the native one.

The macOS binaries are **unsigned and un-notarized**. That matters only for how you obtained
them: a file downloaded by a *browser* carries `com.apple.quarantine`, and Gatekeeper refuses
it until you clear the attribute —

```bash
xattr -d com.apple.quarantine ./sutra
```

— while an archive fetched by the installer (or any `curl`) has no such attribute and simply
runs. The installer does not strip it for you: quietly removing a security attribute is not an
installer's business.

## Get the engine image

```bash
docker pull ghcr.io/startr-trade/sutra:0.6.12
```

One generic image runs every application: behavior comes entirely from the deployment
packages you mount or deploy into it. The [quickstart](quickstart.md) starts it for you with
Docker Compose, so you can skip this step if you are heading straight there.

## Staying current

```bash
sutra self-update --check      # is there a newer release? (changes nothing)
sutra self-update              # replace this binary with the newest release
sutra self-update --runtime    # …and pull the matching engine image
sutra self-update --runtime-only   # only the engine image
```

Updates are **never automatic** — nothing runs on a timer or as a side effect of another
command. Every download is checksum-verified against the release's `SHA256SUMS` before it is
installed, and the binary is replaced by an atomic rename, so an interrupted update cannot
leave a half-written executable on your PATH. `--version <tag>` pins or rolls back.

Keeping the CLI and the engine on the same release is worth doing deliberately: they are
built, tested and published from one tag, which is what `--runtime` is for.

## Build from source

You need a stable [Rust toolchain](https://rustup.rs). Docker is needed only for the
container test tier.

```bash
git clone https://github.com/startr-trade/sutra.git
cd sutra

make test                                        # the no-docker suite
cargo install --path rust/crates/sutra-cli       # install the CLI you just built
docker build -t sutra-engine:dev -f rust/Dockerfile rust/   # the engine image
```

## Next

**[Quickstart](quickstart.md)** — a running engine and your first message flowing through it,
in about five minutes.
