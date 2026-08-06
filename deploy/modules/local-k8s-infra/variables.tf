# Inputs for the STAGE 2 (infra) module. The cluster already exists; this module
# only needs the kubeconfig path to drive kubectl/kubectl-shell local-execs, plus
# the install toggles + pinned versions.

variable "kubeconfig_path" {
  description = "Path to the kubeconfig the cluster stage wrote (cluster module output `kubeconfig_path`). Used by the metallb/observability local-execs as KUBECONFIG. The consuming root also points its kubernetes/helm/kubectl providers at this same file."
  type        = string
}

variable "registry_port" {
  description = "Local registry host port — rendered into the local-registry-hosting ConfigMap. Must match the cluster stage's registry_port."
  type        = number
  default     = 5000
}

# ===== Production-realistic deps =====
# All default to true so the cluster reflects the production posture even when
# the IT scenarios themselves use NodePort and don't depend on these components.
# Per user direction 2026-05-24: "include production realistic dependencies in
# the kind cluster. Yes, the IT scenarios need not use it."

variable "install_metallb" {
  description = "Install MetalLB (LoadBalancer allocator). True by default."
  type        = bool
  default     = true
}

variable "metallb_version" {
  description = "MetalLB Helm chart version."
  type        = string
  default     = "0.14.8"
}

variable "metallb_address_pool" {
  description = "MetalLB IPAddressPool addresses. EMPTY (the default) DERIVES the pool from the live `kind` Docker-network subnet at apply time (.255.200-.255.250 of whatever /16 kind got — 172.18, 172.19, …). Set a non-empty list to override with explicit addresses."
  type        = list(string)
  default     = []
}

variable "install_ingress_nginx" {
  description = "Install ingress-nginx (Ingress controller). True by default."
  type        = bool
  default     = true
}

variable "ingress_nginx_version" {
  description = "ingress-nginx Helm chart version (controller image v1.10.x line)."
  type        = string
  default     = "4.11.2"
}

variable "install_keda" {
  description = "Install KEDA (event-driven autoscaler — required by deploy/modules/sutra ScaledObject)."
  type        = bool
  default     = true
}

variable "keda_version" {
  description = "KEDA Helm chart version."
  type        = string
  default     = "2.15.1"
}

variable "install_efk" {
  description = "Install in-cluster EFK (Elasticsearch + Kibana via ECK + Fluent Bit DaemonSet) + the OTel observability pipeline. Per user direction 2026-05-24 the K8s scenario assumes EFK runs in-cluster; this provisions that single canonical posture. Set false to opt out for resource-constrained CI runners."
  type        = bool
  default     = true
}

variable "eck_operator_version" {
  description = "ECK operator Helm chart version."
  type        = string
  default     = "2.13.0"
}

variable "elasticsearch_version" {
  description = "Elasticsearch version (must be supported by the ECK operator pinned above)."
  type        = string
  default     = "8.15.0"
}

variable "fluent_bit_version" {
  description = "fluent-bit Helm chart version."
  type        = string
  default     = "0.47.10"
}

variable "otel_collector_version" {
  description = "OpenTelemetry Collector (contrib) image tag. 0.114.0 lands OTLP logs in ES reliably (0.110 silently dropped them) and supports ECS mapping mode."
  type        = string
  default     = "0.114.0"
}
