# Sutra — repo-root convenience targets.
#
# Thin wrappers over the canonical commands so there's ONE source of truth.
# Nothing here changes generator behaviour or catalog output — it's pure DX.
#
#   make catalog        regenerate the artifact-documentation catalog
#   make catalog-check  verify the catalog is in sync (CI mode; fails on drift)
#   make install-hooks  install the optional catalog-regen pre-commit hook (opt-in)
#
# The catalog has a single LIVE generator: rust/crates/sutra-catalog-gen — it emits the
# rust/crates/** pages (source-file pages, Cargo.md crate/workspace indexes). Like the other
# generators (sutra-docgen, sutra-schema-gen) it is a LIBRARY: every invocation goes through
# the CLI (`sutra generate catalog` / `... docs` / `... schema-handler`), the workspace's one tooling
# binary. It only ever writes the rust/ subtree of the output directory; any other page
# already present there is left untouched.

# Output directory. `catalog/` is the default; a checkout that already carries the catalog
# under docs/ keeps writing there.
CATALOG_OUTPUT := $(if $(wildcard docs/design/artifact-documentation),docs/design/artifact-documentation,catalog)

.PHONY: catalog catalog-rust catalog-check catalog-rust-check install-hooks verify-workflows verify-docs \
	print-catalog-output \
	help test test-docker test-all test-k8s lint docker-clean audit image image-it

## Regenerate the catalog. Only the rust/ pages are written; anything else in the
## output directory is left untouched.
catalog: ## Regenerate the artifact-documentation catalog.
	$(MAKE) --no-print-directory catalog-rust

## The live generator for the rust/crates/** pages, shipped as the `sutra generate catalog`
## subcommand (release-it profile: it syn-parses the whole workspace, and debug is too slow).
catalog-rust: ## Regenerate the Rust catalog pages (sutra generate catalog; sutra-catalog-gen).
	cd rust && cargo run -q --profile release-it -p sutra-cli -- generate catalog \
		--repo-root=.. --output=../$(CATALOG_OUTPUT)

## Check the catalog for drift WITHOUT writing (same as CI). Only the rust/ pages are
## checked.
catalog-check: ## Verify the catalog is in sync (fails on drift, like CI).
	$(MAKE) --no-print-directory catalog-rust-check

## Print the resolved catalog output directory (used by the optional pre-commit hook).
print-catalog-output:
	@echo $(CATALOG_OUTPUT)

catalog-rust-check: ## Verify the Rust catalog is in sync (generator --check).
	cd rust && cargo run -q --profile release-it -p sutra-cli -- generate catalog \
		--repo-root=.. --output=../$(CATALOG_OUTPUT) --check

## Install the OPTIONAL pre-commit hook (scripts/hooks/pre-commit) into the
## repo's hooks dir. Local convenience only — `catalog-check` (above) is the real CI
## backstop, so this is opt-in, not required. The hook fast-exits (no-op) unless
## the commit stages rust/crates/** changes, and only then runs `make catalog`
## (release build) + `git add`s the result. Hooks live outside the tracked tree
## (and outside a linked worktree's own .git file — resolved via `git rev-parse
## --git-path`, which finds the shared hooks dir and honours core.hooksPath), so
## re-run this after every fresh clone/worktree.
install-hooks: ## Install the optional catalog-regen pre-commit hook (opt-in local DX).
	@hooks_dir="$$(git rev-parse --git-path hooks)"; \
	mkdir -p "$$hooks_dir"; \
	cp scripts/hooks/pre-commit "$$hooks_dir/pre-commit"; \
	chmod +x "$$hooks_dir/pre-commit"; \
	echo "Installed $$hooks_dir/pre-commit (regenerates the catalog only when rust/crates/ changes are staged)."; \
	echo "  Bypass a single commit: git commit --no-verify"; \
	echo "  Uninstall: rm $$hooks_dir/pre-commit"

# ---- Rust test tiers (full detail in rust/TESTING.md) ---------------------
# Tier-1 = the default `cargo test` (NO docker; every container-spawning test
# is `#[ignore = "docker"]`). Tier-2 = the docker/testcontainers suites, run
# with `--ignored` (the `sutra-conformance` k8s_* suites are excluded via
# `--skip k8s_`). Tier-3 = the k8s conformance suites (`cargo test -p
# sutra-conformance -- --ignored --test-threads=1 k8s_`, a running kind cluster).
# `docker-clean` reaps any fixtures a killed test run leaked (the testkit
# atexit reaper covers normal exits; testcontainers-rs ships no ryuk sidecar).
#
# No crate is excluded from the routine tiers any more. The generated-code carve-out that used
# to live here (an `--exclude` on every target for a large generated decode-table crate, plus a
# `test-generated` gate for its ~127k generated lines) moved WITH that crate to the repository
# that owns it: `sutra-schema-gen` — the neutral generator — stays public and is gated by its own
# fast suite, including a byte-equality golden over an AUTHORED fixture (no published schema is
# vendored anywhere in this repository — see THIRD-PARTY-NOTICES.md). Whatever a distribution
# generates, and the cost of compiling it, is that distribution's business.

test: ## Tier-1: the no-docker Rust suite.
	cd rust && cargo test --workspace

## Tier-2: the docker (testcontainers) suites. `make test-docker P=sutra-persistence`
## narrows it to one crate; bare `make test-docker` runs the whole workspace's ignored set.
##
## `--test-threads=4 --no-fail-fast` is pinned so the three-replica `tc_multi_replica`
## singleton-saga (sutra-conformance) can co-reside with the rest of the parallel tier-2 suite
## on the shared box without CPU-starving it (the failure was starvation, not a real timeout —
## capping the harness's own concurrency leaves headroom for tc_multi_replica's 3 engine
## replicas + brokers). `--no-fail-fast` keeps that determinism visible: one flaky/starved test
## no longer aborts the run before the rest (incl. tc_multi_replica) get a chance to finish.
# Concurrent container-spawning tests. 4 suits a dev box; a 2-core CI runner oversubscribes at
# that level and starves broker fixtures into timeouts (TEST_THREADS=2 in .github nightly).
TEST_THREADS ?= 4

test-docker: ## Tier-2: docker/testcontainers suites (P=<crate> narrows; TEST_THREADS=<n> caps concurrency).
ifdef P
	cd rust && cargo test -p $(P) --no-fail-fast -- --ignored --skip k8s_ --test-threads=$(TEST_THREADS)
else
	cd rust && cargo test --workspace --no-fail-fast -- --ignored --skip k8s_ --test-threads=$(TEST_THREADS)
endif

## Same `--test-threads=4 --no-fail-fast` pin as test-docker (this target's
## `--include-ignored` sweep runs tier-2, incl. tc_multi_replica, alongside tier-1).
test-all: ## Tier-1 + tier-2 together (k8s tier-3 excluded).
	cd rust && cargo test --workspace --no-fail-fast -- --include-ignored --skip k8s_ --test-threads=4

## Check that every GitHub Actions workflow parses and that every `uses:` reference exists
## upstream. A version nobody published (`cosign-installer@v4` — sigstore ships v4.1.2, not v4)
## fails a workflow at its FIRST step, and a release is an expensive place to find that out.
## Run the `sutra` commands the getting-started chapters tell a reader to run, against a real
## binary, and report which ones work. Prose that names a command is a promise: `sutra create app`
## followed by `sutra package` — the two the scaffolder itself prints as "next" — were both broken
## in 0.2.0-rc.1 while every unit test passed.
verify-docs: ## Run the commands the getting-started docs document (SUTRA_BIN=<path> to pick a binary).
	bash scripts/verify-doc-commands.sh

verify-workflows: ## Verify workflow YAML + that every action reference resolves upstream.
	bash scripts/verify-workflow-actions.sh

lint: ## Routine clippy -D warnings (workspace) + the domain-neutrality gate.
	cd rust && cargo clippy --workspace --all-targets -- -D warnings
	cd rust && cargo test -p sutra-archtest --quiet

## Tier-3: the k8s conformance suites (k8s_money_transfer + k8s_observability in
## sutra-conformance). Serial (`--test-threads=1`) — they share the one cluster instance, and
## both deploy the same money-transfer package, so each suite's `_zz_teardown` must precede the
## next suite's provisioning (name order does that). The rail k8s suites live in the private
## rails repo and drive this same cluster through the shared-scenario stage.
## Bring the kind cluster up ONCE first:
##   make -C deploy/k8s-it init
## and tear it down with `make destroy` there when finished.
test-k8s: ## Tier-3: the k8s conformance suites (needs a running kind cluster).
	@echo "Prerequisite: kind cluster up — make -C deploy/k8s-it init"
	cd rust && cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_

## Build the LOCAL tier-2 image the testcontainers suites boot (`sutra-testkit`'s
## `DEFAULT_IMAGE`). Fast `release-it` profile. Rebuild this from HEAD before trusting any
## tier-2 result — the tag is mutable, so a stale one silently tests yesterday's binary.
## The SHIPPED image uses the full-LTO `release` (the Dockerfile's default CARGO_PROFILE):
## build that with a plain `docker build -f rust/Dockerfile rust/`.
IMAGE ?= sutra-rust-engine:dev
image: ## Build the local tier-2 engine image (IMAGE=, default sutra-rust-engine:dev; fast `release-it`).
	DOCKER_BUILDKIT=1 docker build --build-arg CARGO_PROFILE=release-it -t $(IMAGE) -f rust/Dockerfile rust/

## Build + push the k8s-IT engine image with the FAST `release-it` profile (no LTO, parallel
## codegen — ~2x faster than the shipped `release`; a few MB larger, irrelevant for functional
## ITs) to the kind-local registry, then roll the shared Deployment onto it. IMG overrides the
## tag.
##
## Why all three kubectl calls (mirroring the rails repo's `image-it`, which shares this
## cluster): `set image` switches a Deployment currently running the RAILS tag back to this
## one; `rollout restart` covers the other case, a re-push of the SAME tag (the pod only
## re-pulls on a restart, even with imagePullPolicy=Always — and tofu sees no drift, so an
## apply alone would leave the stale pod serving); `rollout status` blocks until the new pod
## is ready, so a suite never races the rollout. None of it drifts from tofu: every k8s
## suite's own `tofu apply` passes `engine_image=$(IMG)`, the same value set here, so the
## next apply converges instead of fighting.
##
## Tier-3 is MUTUALLY EXCLUSIVE with the rails repo's tier-3 — this takes the shared
## Deployment away from it until that repo runs its own `image-it`.
IMG ?= localhost:5000/sutra-engine:k8s-it
# The `sutra-fednow-it` segment is a historical name (frozen: the kind provider names the
# generated kubeconfig after the cluster, so renaming it means recreating the cluster).
KUBECONFIG_IT ?= deploy/k8s-it/cluster/sutra-fednow-it-config
K8S_NAMESPACE ?= default
image-it: ## Build + push the fast (release-it) engine image for k8s ITs, then roll deployment/sutra-engine (IMG=<tag>).
	DOCKER_BUILDKIT=1 docker build --build-arg CARGO_PROFILE=release-it -t $(IMG) -f rust/Dockerfile rust/
	docker push $(IMG)
	kubectl --kubeconfig $(KUBECONFIG_IT) -n $(K8S_NAMESPACE) set image deployment/sutra-engine engine=$(IMG)
	kubectl --kubeconfig $(KUBECONFIG_IT) -n $(K8S_NAMESPACE) rollout restart deployment/sutra-engine
	kubectl --kubeconfig $(KUBECONFIG_IT) -n $(K8S_NAMESPACE) rollout status deployment/sutra-engine --timeout=300s

## Reap leaked test containers + dangling volumes/images. Optional CUTOFF=<minutes>
## (default 30) keeps fixtures younger than the cutoff — so an in-flight run is untouched.
docker-clean: ## Reap leaked test containers (wraps scripts/dev-docker-cleanup.sh; CUTOFF=<min>).
	bash scripts/dev-docker-cleanup.sh $(CUTOFF)

# ---- Supply-chain audit -----------------------------------------------------
# `cargo audit` scans rust/Cargo.lock against the RustSec advisory DB (vulnerabilities +
# yanked crates). `cargo deny check` enforces the licence allowlist, bans (duplicate
# versions), source allowlist, and the same advisories — configured by rust/deny.toml.
# Both operate on the resolved lockfile only (no rustc). Install once:
#   cargo install cargo-audit cargo-deny --locked
audit: ## Supply-chain gate: cargo-audit + cargo-deny (licenses/bans/sources/advisories).
	cd rust && cargo audit
	cd rust && cargo deny check

help: ## Show this help.
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
