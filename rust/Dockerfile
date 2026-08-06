# syntax=docker/dockerfile:1
# sutra-engine (Rust). Multi-stage with cargo-chef dependency caching:
# chef (toolchain) → planner (recipe.json) → builder (cook deps, then build) → Debian slim runtime.
#
# cargo-chef splits the dependency build into its own layer keyed only on recipe.json, so
# source-only changes rebuild just the workspace crates — the dependency layer stays cached.
#
# Build (from the rust/ directory, or `-f rust/Dockerfile rust/` from the repo root):
#   docker build -f rust/Dockerfile -t sutra-rust-engine:dev rust/
#
# Run — the entrypoint is the engine binary itself; it takes the canonical `SUTRA_*`
# env names ONLY (the pre-GA framework aliases were removed; config.rs):
#   SUTRA_DEPLOYMENTS_DIR  → directory of sealed .sutra archives (the canonical deployments
#                            source — verified fail-closed per archive, watched for
#                            add/remove/change; two-phase activation)
#   SUTRA_RESOURCE_ROOT    → legacy pre-archive resource-tree root (transition source;
#                            boot-time scan only; at least one source is required)
#   SUTRA_HTTP_PORT        → listen port (default 8080, exposed below)
#   SUTRA_DATASOURCE_URL / _USERNAME / _PASSWORD → engine-internal database (native
#                            postgres:// form)
# e.g. (archive source)
#   docker run --rm -p 0:8080 \
#     -v $PWD/deployments:/etc/sutra/deployments:ro \
#     -e SUTRA_DEPLOYMENTS_DIR=/etc/sutra/deployments \
#     sutra-rust-engine:dev

# ---- chef (shared toolchain layer) -------------------------------------------
# Pin the bookworm variant so the builder's glibc matches the runtime image below.
FROM rust:1.96-slim-bookworm AS chef
# make + g++: the Kafka transport's rdkafka dependency vendors librdkafka and builds it from source
# via librdkafka's mklove `configure` (the `cmake-build` feature is deliberately OFF — it keeps curl
# coupled and needs libcurl-dev; mklove passes --disable-{curl,ssl,gssapi,zlib,zstd} for a minimal
# PLAINTEXT build). The rust:*-slim image ships gcc + binutils (ring needs them) but neither make
# nor a C++ compiler — mklove's C++ check is a hard FAIL without g++. No libssl-dev/libsasl2-dev/
# libcurl-dev needed (the broker default is PLAINTEXT; mTLS is contract-UNSUPPORTED). Builder-stage
# only; librdkafka links statically into the binary, so the debian:bookworm-slim runtime stays
# unchanged.
RUN apt-get update \
    && apt-get install -y --no-install-recommends make g++ \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /build

# ---- planner: compute the dependency recipe ----------------------------------
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook dependencies (cached layer), then build the workspace -----
FROM chef AS builder
# CARGO_PROFILE: `release` (default — the shipped profile: full thin-LTO + one codegen unit)
# or `release-it` (fast IT/dev image: no LTO, parallel codegen — see rust/Cargo.toml). Both emit
# `target/<profile>/sutra-engine`. The shipped image build is unchanged (default `release`).
ARG CARGO_PROFILE=release
COPY --from=planner /build/recipe.json recipe.json
# cargo-chef keys the dependency build on recipe.json (a source-only change reuses this layer).
# The BuildKit cache mounts (crate registry + git + the target dir) go further: a DEPENDENCY
# change recompiles only the changed crates incrementally across builds, instead of the whole
# tree — the local-iteration win. (CI without a warm cache still gets cargo-chef's layer cache.)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo chef cook --profile ${CARGO_PROFILE} --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# No `COPY schemas`: the public engine ships no schema corpus. The only ISO 20022 XSDs in this
# tree are test fixtures inside `crates/sutra-xsd/tests/data/` and `crates/sutra-schema-gen/
# tests/data/`, which travel with `COPY crates`. A distribution that DOES carry a corpus (for
# `sutra schemagen`, or for a codec that embeds one) keeps it in its own tree and copies it in
# its own image build.
# The target dir is a cache mount (not part of the image), so copy the finished binary OUT of it
# to a normal path for the runtime stage's COPY --from.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --profile ${CARGO_PROFILE} -p sutra-dist \
    && cp target/${CARGO_PROFILE}/sutra-engine /build/sutra-engine

# ---- runtime (distroless) ----------------------------------------------------
# gcr.io/distroless/cc-debian12 — glibc + libgcc + libstdc++ + CA certs, no shell/package
# manager/apt (~20 MB base). glibc is REQUIRED: the binary statically links librdkafka (Kafka
# transport) + ring (rustls), both glibc-built by the bookworm builder above — a musl/`scratch`
# variant is blocked on the rdkafka-sys musl build. CA certs ship in the base
# (TLS to postgres/brokers), so no apt-get. The `[profile.release] strip` (rust/Cargo.toml)
# already stripped the binary. Together with the strip this closes the footprint regression
# (debian-slim runtime + unstripped binary → distroless + stripped binary).
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /build/sutra-engine /usr/local/bin/sutra-engine

# The engine-internal migration SQL — now Rust-owned under
# sutra-persistence/migrations: shipped/{core,audit} are byte-identical copies of the
# frozen reference engine migration trees, and native/ holds the Rust-only V803 timer
# addendum (waiting_event TIMER kind + timer_due_at + claim index). The
# runner shares the `sutra_schema_history` ledger with the reference `sutra-migrate` job
# (deliberate interop) and walks the whole root; version numbers are globally unique across
# subfolders, so shipped + native apply in one ordered pass.
COPY crates/sutra-persistence/migrations /opt/sutra/db/migration
ENV SUTRA_DB_MIGRATIONS=/opt/sutra/db/migration

# Container-internal default port (k8s Services / the IT harness map it dynamically on the
# host side — never bind host 8080 in tests).
ENV SUTRA_HTTP_PORT=8080
EXPOSE 8080

# Non-root (k8s restricted PSS friendly).
USER 10001

ENTRYPOINT ["/usr/local/bin/sutra-engine"]
