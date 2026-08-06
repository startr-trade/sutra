# R14 shared-instance harness — inputs.

variable "kubeconfig_path" {
  description = "Path to the running kind cluster's kubeconfig (the sibling cluster stage under deploy/k8s-it/cluster wrote it; the ITs pass it via -var)."
  type        = string
}

variable "namespace" {
  description = "Namespace for every shared-instance object (engine, postgres, rabbitmq, the deployments ConfigMap and the estate Secret)."
  type        = string
  default     = "default"
}

variable "engine_image" {
  description = "The Rust engine image, pullable from the cluster (push it to the kind-local registry first: docker build -t localhost:5000/sutra-engine:k8s-it -f rust/Dockerfile rust/ && docker push localhost:5000/sutra-engine:k8s-it). The shared instance speaks canonical SUTRA_* env only."
  type        = string
  default     = "localhost:5000/sutra-engine:k8s-it"
}

# ---- conventional ${ENV} refs the example packages resolve (R14: the shared instance's
# env contract pre-provisions them; pod env is immutable, so these are apply-time values —
# each IT class passes its own recorder endpoints on its one shared-scenario apply).

variable "fednow_callback_host" {
  description = "host:port the fednow-http deployment's fednow-response outbound channel calls back (the FEDNOW_CALLBACK_HOST reference in its channels.yaml). ITs set it to a pod-reachable host — the kind docker-network gateway plus the recorder port."
  type        = string
  default     = "127.0.0.1:18099" # placeholder; the fednow IT overrides on apply
}

variable "mx_dest_host" {
  description = "host:port the mt-mx-http deployment's mx-out channel POSTs the pacs.008 to (the MX_DEST_HOST reference in its channels.yaml)."
  type        = string
  default     = "127.0.0.1:19001" # placeholder; the swift IT overrides on apply
}

variable "mt_client_host" {
  description = "host:port the mt-mx-http deployment's mt-out channel POSTs the MT199 to (the MT_CLIENT_HOST reference in its channels.yaml)."
  type        = string
  default     = "127.0.0.1:19002" # placeholder; the swift IT overrides on apply
}

variable "otlp_endpoint" {
  description = "OTLP gRPC endpoint for engine telemetry (the infra stage's collector). Export to an absent collector is non-fatal, so the default is safe without the observability stack."
  type        = string
  default     = "http://otel-collector:4317"
}
