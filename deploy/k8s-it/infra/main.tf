# Stage 2 of 3 — INFRA lifecycle (operator-driven, after cluster/).
#
# Applies the production-realistic deps + the full OTel observability pipeline
# (MetalLB, ingress-nginx, KEDA, EFK, OTel Collector, LBs, Kibana data views) on
# the kind cluster that ../cluster/ already brought up.
#
# The key to a clean fresh apply: every provider here is configured from the
# kubeconfig FILE the cluster stage wrote — passed in as var.kubeconfig_path, a
# plain string, NOT a live resource output. Because the cluster already exists by
# the time this stage runs, even the eagerly-configuring alekc/kubectl provider
# finds real credentials. That removes the provider cycle that forced the old
# single-root design into a targeted two-phase `tofu apply`.
#
# Operator workflow (see Makefile `make init`):
#   cd cluster && tofu apply ...
#   cd infra   && tofu apply -var "kubeconfig_path=$(cd ../cluster && tofu output -raw kubeconfig_path)"

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.0"
    }
  }
}

variable "kubeconfig_path" {
  description = "Path to the kubeconfig ../cluster/ wrote (its `kubeconfig_path` output). All providers below read it; the module's local-execs use it as KUBECONFIG."
  type        = string
}

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes {
    config_path = var.kubeconfig_path
  }
}

provider "kubectl" {
  config_path      = var.kubeconfig_path
  load_config_file = true
}

module "infra" {
  source = "../../../deploy/modules/local-k8s-infra"

  kubeconfig_path = var.kubeconfig_path

  # Defaults install everything: install_metallb / install_ingress_nginx /
  # install_keda / install_efk all true; metallb pool derived from the live kind
  # subnet. Override here for resource-constrained runners.
}

# ----- Observability access -----
# Provisioned by the module. The browser URLs + elastic password are printed at the
# end of `tofu apply` and written to <kubeconfig-dir>/observability-access.txt (the
# module can't expose them as tofu outputs without reintroducing a provider cycle —
# see deploy/modules/local-k8s-infra/observability.tf).
