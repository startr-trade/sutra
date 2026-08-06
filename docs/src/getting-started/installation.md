# Installation

Sutra ships as two things: the **`sutra-engine`** runtime (a container image) and the
**`sutra`** CLI (a single binary that packages, deploys, and inspects deployments).

> **Coming with the first tagged release:** a one-line CLI installer
> (`curl … | sh`, like `kubectl` / `kind` / `tofu`) and a published engine image on a container
> registry. Until then, build from source as below.

## Prerequisites

- A stable **Rust toolchain** ([rustup](https://rustup.rs)).
- **Docker** — only for the container / integration test tiers.
- **[OpenTofu](https://opentofu.org)** (`tofu`) + a **kind** cluster — only for the Kubernetes
  tiers.

## Build from source

```bash
git clone https://github.com/startr-trade/sutra.git
cd sutra

# Build + test the workspace (no Docker needed)
make test
make lint

# Build the engine container image
docker build -t sutra-engine:dev -f rust/Dockerfile rust/

# Run the CLI
cd rust && cargo run -p sutra-cli -- --help
```

## Next

Head to **[Your first app](first-app.md)** to scaffold and run a Sutra application.
