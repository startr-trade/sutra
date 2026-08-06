variable "namespace" {
  description = "Namespace for the EFK stack (typically sutra-observability)"
  type        = string
  default     = "sutra-observability"
}

variable "create_namespace" {
  description = "Whether to create the namespace"
  type        = bool
  default     = true
}

variable "storage_class" {
  description = "StorageClass to use for ES PVCs"
  type        = string
  default     = null
}

variable "elasticsearch" {
  description = "Elasticsearch configuration"
  type = object({
    replicas     = optional(number, 3)
    heap_size    = optional(string, "1g")
    storage_size = optional(string, "30Gi")
    version      = optional(string, "8.13.0")
    eck_operator = optional(bool, true) # use ECK; false → use elastic/elasticsearch chart
  })
  default = {}
}

variable "kibana" {
  description = "Kibana configuration"
  type = object({
    replicas = optional(number, 1)
    version  = optional(string, "8.13.0")
  })
  default = {}
}

variable "fluent_bit" {
  description = "Fluent Bit configuration"
  type = object({
    chart_version       = optional(string, "0.46.7")
    tail_paths          = optional(list(string), ["/var/log/containers/*sutra*.log"])
    audit_tail_path     = optional(string, "/var/log/sutra-audit/**/*.jsonl")
    expose_forward_port = optional(bool, true) # expose 24224 for engine log forwarding
    # When true, the audit JSONL stream is fanned out per tenant: a Lua filter
    # extracts the tenant id from the audit-log path layout
    # (/var/log/sutra-audit/<tenantId>/yyyy/mm/dd/...) and the ES output
    # uses Logstash_Prefix_Key tenant_id to produce per-tenant indices like
    # sutra-audit-acme-corp-2026.05.19. When false (default), all tenants share
    # a single sutra-audit index. Per-tenant indices make Kibana role-based
    # access trivial: a tenant viewer role granting read on
    # sutra-audit-<tenantId>-* restricts what that role can see without
    # query-time filtering.
    audit_fanout_per_tenant = optional(bool, false)
  })
  default = {}
}

variable "otel_collector" {
  description = "OpenTelemetry Collector configuration"
  type = object({
    chart_version = optional(string, "0.94.0")
    replicas      = optional(number, 2)
  })
  default = {}
}

variable "ilm" {
  description = "Index Lifecycle Management policy thresholds"
  type = object({
    logs_retention_days    = optional(number, 30)
    metrics_retention_days = optional(number, 90)
    traces_retention_days  = optional(number, 14)
    audit_retention_days   = optional(number, 2555) # 7 years for financial use cases
  })
  default = {}
}

variable "labels" {
  description = "Additional labels on every resource"
  type        = map(string)
  default     = {}
}
