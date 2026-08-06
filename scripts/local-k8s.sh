#!/usr/bin/env bash
#
# Operator-facing lifecycle controller for the local tier-3 K8s IT environment.
#
# One-stop entry point for spinning up / inspecting / tearing down the kind cluster +
# production-realistic dependencies (MetalLB, ingress-nginx, KEDA, EFK + the OTel
# pipeline) that the tier-3 conformance suites run against.
#
# There is exactly ONE such environment and it is shared by every suite, so its OpenTofu
# roots live at the repo root — `deploy/k8s-it/{cluster,infra,shared-scenario}` — not under
# any example. `deploy/k8s-it/Makefile` owns the tofu invocations; this script is the
# stable CLI wrapper around it (plus `status`, which the Makefile has no equivalent for).
#
# Per user direction 2026-05-24: cluster lifecycle is operator-driven and lives OUTSIDE
# the IT — the ITs only apply the shared scenario and hot-deploy/undeploy their packages.
# This script is the operator's primary handle on the cluster lifetime.
#
# Usage:
#   scripts/local-k8s.sh init
#       Bring the cluster + production-realistic deps up (make init). Idempotent —
#       re-running against an already-initialised state is a no-op apply.
#
#   scripts/local-k8s.sh ready
#       Show cluster status — node listing + each prod-dep namespace's pods.
#
#   scripts/local-k8s.sh destroy
#       Tear infra + cluster down (make destroy). The ONLY supported way to delete the
#       cluster — the ITs themselves never destroy it.
#
#   scripts/local-k8s.sh clean
#       Wipe local tofu state + provider plugins under cluster/ and infra/ (make clean).
#       ONLY safe to run AFTER a successful `destroy` — otherwise live cluster
#       resources are orphaned.
#
#   scripts/local-k8s.sh status
#       Print whether the cluster is reachable via `kubectl cluster-info`.
#       Exit code: 0 if reachable, 1 if not. Useful for shell pipelines.
#
# `--app=<name>` is accepted and IGNORED — a legacy flag from when each example carried
# its own cluster config. One cluster now serves every suite.
#
# Tooling requirements: kind, kubectl, tofu, docker, make on PATH.
# Skip cleanly with a clear message when any are missing.

set -euo pipefail

# ---- Locate the repo root + the shared k8s-it harness ----

if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
else
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

K8S_IT_DIR="$REPO_ROOT/deploy/k8s-it"
CLUSTER_DIR="$K8S_IT_DIR/cluster"
COMMAND=""

# ---- Parse args ----

for arg in "$@"; do
    case "$arg" in
        init|ready|destroy|clean|status|help|--help|-h)
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                COMMAND="help"
            else
                COMMAND="$arg"
            fi
            ;;
        --app=*)
            echo "[local-k8s] note: --app is legacy and ignored — one shared cluster at deploy/k8s-it/." >&2
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            COMMAND="help"
            ;;
    esac
done

if [[ -z "$COMMAND" ]]; then
    COMMAND="help"
fi

if [[ ! -d "$K8S_IT_DIR" ]]; then
    echo "ERROR — expected the shared k8s-it harness at $K8S_IT_DIR." >&2
    exit 2
fi

# ---- Tooling probe ----

require_tools() {
    local missing=()
    for cmd in kind kubectl tofu docker make; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR — missing local tooling: ${missing[*]}" >&2
        echo "       Install before running 'scripts/local-k8s.sh $COMMAND'." >&2
        exit 1
    fi
}

# Delegate to the harness Makefile — the single owner of the tofu invocations, so this
# wrapper can never drift from the stage ordering / kubeconfig plumbing it encodes.
harness_make() {
    make -C "$K8S_IT_DIR" "$@"
}

# ---- Commands ----

cmd_init() {
    require_tools
    echo "[local-k8s] Initialising the shared cluster + infra via $K8S_IT_DIR ..."
    harness_make init
    echo "[local-k8s] Cluster ready. Run 'scripts/local-k8s.sh ready' to inspect."
}

cmd_ready() {
    require_tools
    echo "[local-k8s] Nodes:"
    kubectl get nodes -o wide
    echo ""
    for ns in metallb-system ingress-nginx keda elastic-system logging; do
        echo "[local-k8s] Namespace '$ns':"
        kubectl -n "$ns" get pods 2>/dev/null || echo "  (not installed)"
        echo ""
    done
    echo "[local-k8s] Elasticsearch + Kibana CRs (default ns):"
    kubectl get elasticsearch,kibana 2>/dev/null || echo "  (no CRs found)"
}

cmd_destroy() {
    require_tools
    echo "[local-k8s] Destroying infra + cluster via $K8S_IT_DIR ..."
    # The shared engine instance goes first (avoids dangling refs into the infra stage).
    if [[ -f "$K8S_IT_DIR/shared-scenario/terraform.tfstate" ]]; then
        echo "[local-k8s] Tearing down the shared engine instance first ..."
        harness_make shared-destroy || true
    fi
    harness_make destroy
    echo "[local-k8s] Cluster destroyed."
}

cmd_clean() {
    echo "[local-k8s] Wiping local tofu state under $K8S_IT_DIR ..."
    harness_make clean
    echo "[local-k8s] Local state wiped. (If a cluster is still running, recover via 'kind delete cluster --name sutra-fednow-it' [cluster name is historical, frozen — do not rename] + 'docker rm -f kind-registry'.)"
}

cmd_status() {
    require_tools
    if [[ ! -d "$CLUSTER_DIR/.terraform" ]] && [[ ! -f "$CLUSTER_DIR/terraform.tfstate" ]]; then
        echo "[local-k8s] Cluster not initialised (no tofu state under $CLUSTER_DIR)."
        exit 1
    fi
    local kubeconfig
    kubeconfig="$(cd "$CLUSTER_DIR" && tofu output -raw kubeconfig_path 2>/dev/null || true)"
    if [[ -z "$kubeconfig" ]]; then
        echo "[local-k8s] Cluster tofu state exists but kubeconfig output is empty."
        exit 1
    fi
    if KUBECONFIG="$kubeconfig" kubectl cluster-info --request-timeout=5s >/dev/null 2>&1; then
        echo "[local-k8s] Cluster reachable via $kubeconfig."
        exit 0
    else
        echo "[local-k8s] Cluster tofu state exists but 'kubectl cluster-info' failed — cluster may have been deleted externally. Run 'scripts/local-k8s.sh clean' then 'init' to recover."
        exit 1
    fi
}

cmd_help() {
    sed -n '/^# Usage:/,/^# Tooling requirements/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

case "$COMMAND" in
    init)    cmd_init ;;
    ready)   cmd_ready ;;
    destroy) cmd_destroy ;;
    clean)   cmd_clean ;;
    status)  cmd_status ;;
    help)    cmd_help ;;
    *)
        echo "Unknown command: $COMMAND" >&2
        cmd_help
        exit 2
        ;;
esac
