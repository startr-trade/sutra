# Outputs let the downstream stages (infra/, scenario/) point their
# kubernetes/helm/kubectl providers at this cluster by reading the kubeconfig
# FILE this stage wrote — a static path, NOT a live resource output — which is
# what keeps those stages free of the fresh-apply provider cycle.

output "cluster_name" {
  description = "kind cluster name."
  value       = kind_cluster.this.name
}

output "cluster_id" {
  description = "kind cluster id — useful in `depends_on` / null_resource triggers to gate downstream resources behind cluster readiness."
  value       = kind_cluster.this.id
}

output "kubeconfig_path" {
  description = "Path to the kubeconfig kind wrote out. infra/ + scenario/ pass this to their providers as config_path; set KUBECONFIG to it for kubectl."
  value       = kind_cluster.this.kubeconfig_path
}

output "kubeconfig" {
  description = "Inline kubeconfig (sensitive). Use kubeconfig_path for kubectl invocations; this output is for in-process k8s clients."
  value       = kind_cluster.this.kubeconfig
  sensitive   = true
}

output "client_certificate" {
  description = "Client certificate from the kubeconfig — used by Kubernetes-provider configurations downstream."
  value       = kind_cluster.this.client_certificate
  sensitive   = true
}

output "client_key" {
  description = "Client key from the kubeconfig (downstream provider config)."
  value       = kind_cluster.this.client_key
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = "Cluster CA cert from the kubeconfig (downstream provider config)."
  value       = kind_cluster.this.cluster_ca_certificate
}

output "endpoint" {
  description = "API server endpoint from the kubeconfig (downstream provider config)."
  value       = kind_cluster.this.endpoint
}

output "registry_endpoint" {
  description = "Host-visible registry endpoint. Tag images here and `docker push` so cluster nodes can pull via containerd's hosts.toml."
  value       = "localhost:${var.registry_port}"
}

output "registry_internal_endpoint" {
  description = "In-cluster registry endpoint (containerd resolves <registry_name>:<port> via the kind Docker network)."
  value       = "${var.registry_name}:${var.registry_port}"
}
