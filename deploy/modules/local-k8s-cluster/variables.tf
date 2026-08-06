# Inputs for the STAGE 1 (cluster) module — kind cluster + local registry only.
# Infra/observability toggles live in the local-k8s-infra module's variables.

variable "cluster_name" {
  description = "kind cluster name. Consumed by `kind get clusters` + downstream cleanup."
  type        = string
  default     = "sutra-it"
}

variable "worker_count" {
  description = "Number of worker nodes. 2 by default (per user direction 2026-05-24 — exercises multi-node scheduling)."
  type        = number
  default     = 2
}

variable "node_image" {
  description = "kindest/node image tag. Pin to a known-good Kubernetes version."
  type        = string
  default     = "kindest/node:v1.31.0"
}

variable "registry_name" {
  description = "Local Docker registry container name (reachable from nodes via the `kind` Docker network)."
  type        = string
  default     = "kind-registry"
}

variable "registry_port" {
  description = "Local registry host port. Containerd hosts.toml in each node points at <registry_name>:<port>."
  type        = number
  default     = 5000
}

variable "host_port_mappings" {
  description = "extraPortMappings on the control-plane node. List of {container_port, host_port, protocol} maps — each NodePort the IT needs to reach from the host."
  type = list(object({
    container_port = number
    host_port      = number
    protocol       = string
  }))
  default = []
}
