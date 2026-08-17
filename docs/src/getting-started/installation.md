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
curl -fsSL …/install.sh | sh -s -- --version v0.2.0-rc.1

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
curl -fsSL https://github.com/startr-trade/sutra/releases/download/v0.2.0-rc.1/install.sh \
  | sh -s -- --version v0.2.0-rc.1

# 2. a token, which moves the API to 5000 requests an hour
GH_TOKEN=$(gh auth token) curl -fsSL …/install.sh | sh

# 3. no installer at all — the assets are plain files
gh release download v0.2.0-rc.1 -R startr-trade/sutra \
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

Published targets: **linux x86_64**, **linux aarch64** (both static musl — no glibc version
to match), and **windows x86_64**. macOS builds from source for now.

## Get the engine image

```bash
docker pull ghcr.io/startr-trade/sutra:0.2.0-rc.1
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
