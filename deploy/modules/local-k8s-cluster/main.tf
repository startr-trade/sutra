# Shared "local Kubernetes for IT" — STAGE 1: CLUSTER.
#
# Provisions ONLY the kind cluster + the local Docker registry that feeds it.
# Production-realistic deps (MetalLB, ingress-nginx, KEDA, EFK) and the whole
# observability pipeline live in the SEPARATE `local-k8s-infra` module / `infra/`
# stage, applied afterwards against this already-running cluster.
#
# Why the split (the three-stage pattern: cluster → infra → application):
#   A single root that BOTH creates the cluster AND applies in-cluster resources
#   has to configure its kubernetes/helm/kubectl providers from this cluster's
#   own not-yet-created outputs. On a fresh `tofu apply` those outputs are unknown
#   when the providers configure, and the alekc/kubectl provider (which configures
#   eagerly) aborts with "no configuration has been provided". Keeping THIS stage
#   provider-light — kind + docker + null only, zero in-cluster resources — means
#   there is nothing to trigger that eager configuration, so a clean apply just
#   works. The infra stage then reads the kubeconfig FILE this stage wrote (a
#   static path, not a resource output), so its providers configure against a
#   cluster that already exists.
#
# Consumed by the repo-root deploy/k8s-it/cluster wrapper (one cluster for every tier-3 suite). Per user direction
# 2026-05-24: "kind cluster creation would have to be module or FedNow IT
# independent."

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

# ===== Local Docker registry =====
# Created before the cluster so the per-node containerd hosts.toml in the cluster
# resource can reference it as soon as nodes come up. The container is published
# on 127.0.0.1:<registry_port> for host-side pushes (`docker tag` + `docker push`)
# and rejoins the `kind` network via local-exec once kind allocates it.

resource "docker_image" "registry" {
  name = "registry:2"
}

resource "docker_container" "registry" {
  name  = var.registry_name
  image = docker_image.registry.image_id

  ports {
    internal = 5000
    external = var.registry_port
    ip       = "127.0.0.1"
  }

  restart = "always"
  rm      = false
}

# ===== kind cluster =====
# Multi-node by default (1 control-plane + N workers). The control-plane node
# carries any operator-supplied extraPortMappings so IT scenarios can reach
# NodePort services from the host without `kubectl port-forward`.

locals {
  worker_nodes = [for i in range(var.worker_count) : i]
}

resource "kind_cluster" "this" {
  name           = var.cluster_name
  wait_for_ready = true
  node_image     = var.node_image

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    # Control-plane carries extra port mappings supplied by the consumer.
    node {
      role = "control-plane"
      dynamic "extra_port_mappings" {
        for_each = var.host_port_mappings
        content {
          container_port = extra_port_mappings.value.container_port
          host_port      = extra_port_mappings.value.host_port
          protocol       = extra_port_mappings.value.protocol
        }
      }
    }

    # N worker nodes via dynamic block.
    dynamic "node" {
      for_each = local.worker_nodes
      content {
        role = "worker"
      }
    }

    containerd_config_patches = [
      <<-EOT
        [plugins."io.containerd.grpc.v1.cri".registry]
          config_path = "/etc/containerd/certs.d"
      EOT
    ]
  }
}

# ----- Connect the registry to the kind network -----
# kind creates the `kind` Docker network when the first cluster comes up. Connect
# the registry container so nodes can resolve `<registry_name>:<port>` by hostname.

resource "null_resource" "registry_on_kind_network" {
  triggers = {
    cluster_id  = kind_cluster.this.id
    registry_id = docker_container.registry.id
  }

  provisioner "local-exec" {
    command = "docker network connect kind ${var.registry_name} 2>/dev/null || true"
  }
}

# ----- Per-node containerd hosts.toml -----
# Drop a hosts.toml in /etc/containerd/certs.d/<host>/ on every node, for BOTH the
# registry hostname (`<registry_name>:<port>`) and the canonical kind alias
# (`localhost:<port>`). Each tells containerd: "when a pull asks for an image at
# <host>/..., go through this insecure HTTP endpoint" — the registry container on
# the `kind` Docker network.
#
# Both aliases are wired because the registry is referenced two ways:
#   * `localhost:<port>` — the canonical kind local-registry name. The host pushes
#     to it via the published 127.0.0.1:<port> port (a host CAN'T resolve the
#     `kind-registry` container name, only `localhost`), and pods pull `localhost:
#     <port>/img` which containerd redirects here to `http://<registry_name>:<port>`
#     (resolvable on the kind network). This is the name IT scenarios use so the
#     same image ref works from host push AND in-cluster pull with no /etc/hosts edit.
#   * `<registry_name>:<port>` — kept for in-cluster references that address the
#     registry container by its kind-network hostname directly.
# See https://kind.sigs.k8s.io/docs/user/local-registry/.

locals {
  node_names = concat(
    ["${var.cluster_name}-control-plane"],
    [for i in local.worker_nodes : i == 0 ? "${var.cluster_name}-worker" : "${var.cluster_name}-worker${i + 1}"]
  )

  # Registry aliases that resolve to the same container via these node hosts.toml
  # entries. `localhost:<port>` is the host-pushable canonical name; the container
  # hostname stays available for in-cluster direct references.
  registry_aliases = ["localhost:${var.registry_port}", "${var.registry_name}:${var.registry_port}"]
}

resource "null_resource" "registry_hosts_toml" {
  for_each = toset([
    for pair in setproduct(local.node_names, local.registry_aliases) : "${pair[0]}|${pair[1]}"
  ])

  triggers = {
    cluster_id = kind_cluster.this.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      docker exec ${split("|", each.key)[0]} mkdir -p /etc/containerd/certs.d/${split("|", each.key)[1]} \
        && echo '[host."http://${var.registry_name}:${var.registry_port}"]' | \
            docker exec -i ${split("|", each.key)[0]} tee /etc/containerd/certs.d/${split("|", each.key)[1]}/hosts.toml > /dev/null
    EOT
  }

  depends_on = [null_resource.registry_on_kind_network]
}

# NOTE: the `local-registry-hosting` ConfigMap (a discovery pointer for lens/tilt/
# devspace) lives in the INFRA stage — it is the one in-cluster resource that would
# otherwise force a kubernetes provider into this cluster-creating root. Keeping it
# in infra preserves this stage's provider-light guarantee. See local-k8s-infra.
