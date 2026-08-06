# Stage 1 of 3 — CLUSTER lifecycle (operator-driven init/destroy).
#
# Three-stage K8s IT deploy (per the user's proven pattern, 2026-06-25):
#
#   cluster/  (this)  kind cluster + local registry. Provider-light — no
#                     kubernetes/helm/kubectl providers, no in-cluster resources —
#                     so a fresh `tofu apply` has nothing that would force a
#                     provider to configure from the not-yet-created cluster. One
#                     clean apply, no targeted two-phase hack.
#   infra/            MetalLB + ingress-nginx + KEDA + EFK + the OTel observability
#                     pipeline. Points its providers at the kubeconfig FILE this
#                     stage wrote, so it configures against a cluster that already
#                     exists.
#   shared-scenario/  the ONE engine instance every tier-3 suite hot-deploys onto
#                     (R14; re-applied idempotently by the suites themselves).
#
# Operator workflow (see Makefile):
#   make init       # cluster apply, then infra apply
#   make ready
#   (cd rust && cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_)
#   make destroy    # infra destroy, then cluster destroy (operator-only; ITs never call it)
#
# Stages 1 and 2 are OPERATOR-owned and shared by every tier-3 suite, which is why
# they live at the repo root rather than under any one example: the suites only ever
# apply ../shared-scenario/, and assume this cluster + infra are already up. The
# cluster keeps the name it was created with (`sutra-fednow-it`) — kind derives the
# kubeconfig filename from it, and that generated path is hardcoded by the harness
# (sutra_testkit::conformance::k8s::kubeconfig_path), so renaming would break a live
# environment for no gain.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.6"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

module "cluster" {
  source = "../../../deploy/modules/local-k8s-cluster"

  cluster_name = "sutra-fednow-it"
  worker_count = 2

  # NodePort 30808 (engine HTTP) → host port 8080. Lets the shared instance's
  # Service + the IT test client reach the engine via http://localhost:8080
  # without a kubectl port-forward.
  host_port_mappings = [
    {
      container_port = 30808
      host_port      = 8080
      protocol       = "TCP"
    }
  ]
}

# ===== Outputs the infra + scenario stages read =====

output "cluster_name" {
  description = "kind cluster name."
  value       = module.cluster.cluster_name
}

output "kubeconfig_path" {
  description = "kubeconfig file path. ../infra/ + ../shared-scenario/ pass this to their providers (config_path) and local-execs (KUBECONFIG); the conformance harness reads the same file."
  value       = module.cluster.kubeconfig_path
}

output "registry_endpoint" {
  description = "Host-visible registry endpoint. The IT tags + pushes the engine image here."
  value       = module.cluster.registry_endpoint
}

output "endpoint" {
  description = "Kubernetes API endpoint."
  value       = module.cluster.endpoint
}
