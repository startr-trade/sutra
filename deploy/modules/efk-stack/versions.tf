# Per-module provider declarations so `tofu validate` / `tofu plan` work
# in-place from this directory without needing the root deploy/versions.tf
# to be on the same path. The root deploy/versions.tf still pins versions
# for the top-level workspace; this file declares the providers this
# module actually uses.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30, < 3.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.13, < 3.0"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = ">= 2.0, < 3.0"
    }
  }
}
