variable "name" {
  description = "Deployment name"
  type        = string
  default     = "sutra"
}

variable "namespace" {
  description = "Kubernetes namespace to deploy into. Will be created if create_namespace=true."
  type        = string
}

variable "create_namespace" {
  description = "Whether to create the namespace"
  type        = bool
  default     = true
}

variable "image" {
  description = "Container image — production must pin by digest, e.g. ghcr.io/startr-trade/sutra@sha256:..."
  type        = string
}

variable "image_pull_secrets" {
  description = "imagePullSecrets to attach to the pod"
  type        = list(string)
  default     = []
}

variable "replicas" {
  description = "Replica count (ignored when autoscaling.enabled=true)"
  type        = number
  default     = 2
}

variable "resources" {
  description = "Container resource requests + limits"
  type = object({
    limits = object({
      cpu    = string
      memory = string
    })
    requests = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    limits   = { cpu = "1000m", memory = "1Gi" }
    requests = { cpu = "200m", memory = "512Mi" }
  }
}

# ===== Resource tree mount =====
# Per resource-layout.md, the engine reads configuration.yaml + bpmn/ + rules/ + tenants/ from /etc/sutra/resources.
# Source: ConfigMap (typical, <1MB), PVC (large), or CSI volume backed by object storage (very large).

variable "resource_tree" {
  description = "Source of the mounted resource tree"
  type = object({
    type              = string # "configmap" | "pvc" | "csi"
    configmap_name    = optional(string)
    pvc_claim_name    = optional(string)
    csi_driver        = optional(string)
    csi_volume_handle = optional(string)
    csi_attributes    = optional(map(string), {})
  })
  validation {
    condition     = contains(["configmap", "pvc", "csi"], var.resource_tree.type)
    error_message = "resource_tree.type must be one of: configmap, pvc, csi"
  }
}

# ===== Database =====

variable "database" {
  description = "Postgres connection — engine expects an existing database; sutra-migrate Job runs schema migration"
  type = object({
    host                = string
    port                = optional(number, 5432)
    database            = string
    username_secret_ref = string # "secret-name#key"
    password_secret_ref = string # "secret-name#key"
  })
}

# ===== Observability =====
# The EFK reference stack may be deployed in-cluster via the efk-stack module,
# OR the engine may point at an external EFK stack (corporate, shared, etc.).
# Either way, the engine receives the endpoints as inputs.

variable "observability" {
  description = "Observability endpoints — supply from the efk-stack module's outputs OR provide directly for an external EFK"
  type = object({
    # OTLP — required for traces and metrics
    otlp_endpoint = string                   # e.g. otel-collector.observability.svc:4317
    otlp_protocol = optional(string, "grpc") # "grpc" or "http"
    otlp_tls      = optional(bool, false)

    # Fluent Bit forward — optional; when set, engine ships JSON logs via forward protocol instead of stdout
    log_forward_endpoint = optional(string) # e.g. fluent-bit.observability.svc:24224
    log_forward_protocol = optional(string, "forward")

    # Elasticsearch — optional; engine itself does not call ES directly, but the JSONL audit sink
    # may be configured to fan out via an OpenSearch/ES sidecar shipper. Most operators leave this null
    # and let Fluent Bit handle the ES ingest from the audit JSONL directory.
    elasticsearch_endpoint               = optional(string)
    elasticsearch_credentials_secret_ref = optional(string)
  })
}

# ===== Tenancy =====

variable "tenants" {
  description = <<-EOT
    Map of tenant configurations to materialise as kubernetes_config_map_v1 resources mounted into the engine pod at /etc/sutra/resources/tenants/<id>/.
    Each entry's key is the tenant id; the value is the tenant-configuration.yaml body (status, retention, quotas, inherits, redactors per multi-tenancy.md). Channels live in sibling channels/*.yaml entries — see resource-layout.md. The engine's ResourceLayoutObserver picks up atomic ConfigMap updates within ~1s.

    Tenants are tofu-applied; mutations flow through `tofu apply`. No K8s Custom Resources.
  EOT
  type        = map(any)
  default     = {}
}

# ===== Auth =====

variable "admin_oidc" {
  description = "OIDC config for the admin REST API. Per auth.md, no anonymous access; 3-role model enforced."
  type = object({
    issuer   = string
    audience = string
    jwks_url = optional(string)
  })
}

# ===== Networking =====

variable "ingress" {
  description = "Optional ingress for HTTP channels (8080) and admin (9090)"
  type = object({
    enabled         = bool
    class_name      = optional(string)
    annotations     = optional(map(string), {})
    public_host     = optional(string)
    admin_host      = optional(string)
    tls_secret_name = optional(string)
  })
  default = { enabled = false }
}

variable "network_policy" {
  description = "Whether to create a NetworkPolicy restricting egress"
  type        = bool
  default     = true
}

# ===== Autoscaling =====

variable "autoscaling" {
  description = "KEDA-driven autoscaling on inbox+outbox lagging-work depth (per replica-semantics.md)"
  type = object({
    enabled             = bool
    min_replicas        = optional(number, 2)
    max_replicas        = optional(number, 20)
    target_lagging_work = optional(number, 50)
    keda_db_secret_name = optional(string)
  })
  default = {
    enabled = true
  }
}

# ===== Misc =====

variable "extra_env" {
  description = "Additional environment variables (e.g. feature flags)"
  type        = map(string)
  default     = {}
}

variable "labels" {
  description = "Additional labels on every resource"
  type        = map(string)
  default     = {}
}

variable "service_account_name" {
  description = "Service account name (will be created)"
  type        = string
  default     = "sutra"
}

# ===== Schema migration Job =====
# A one-shot Kubernetes Job runs sutra-migrate before the Deployment rolls out so
# replicas never race the schema upgrade. The Deployment depends_on this Job — if disabled, the operator is on the hook for running migrations
# out-of-band before each release.

variable "enable_migrate_job" {
  description = "Run sutra-migrate as a K8s Job before the Deployment rolls out. Set false if migrations are handled out-of-band."
  type        = bool
  default     = true
}

variable "migrate_image" {
  description = "Image for the schema-migration one-shot Job."
  type        = string
  default     = "ghcr.io/startr-trade/sutra-migrate:latest"
}

variable "migrate_image_pull_policy" {
  description = "imagePullPolicy for the migrate Job container."
  type        = string
  default     = "IfNotPresent"
}

variable "migrate_active_deadline_seconds" {
  description = "K8s Job activeDeadlineSeconds — Job is killed (and considered failed) past this. Tune up for very large schemas."
  type        = number
  default     = 300
}

variable "migrate_backoff_limit" {
  description = "K8s Job backoffLimit — how many times the pod may retry before the Job is marked failed."
  type        = number
  default     = 2
}

