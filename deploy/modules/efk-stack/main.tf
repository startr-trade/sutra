locals {
  labels = merge({
    "app.kubernetes.io/part-of"    = "sutra-observability"
    "app.kubernetes.io/managed-by" = "opentofu"
  }, var.labels)

  # Fluent Bit config fragments — composed via string concatenation rather
  # than heredoc-inside-interpolation so HCL stays happy.

  fluent_bit_inputs = <<-EOT
    [INPUT]
        Name tail
        Tag sutra.logs.*
        Path ${join(",", var.fluent_bit.tail_paths)}
        Parser docker
        DB /var/log/flb_sutra.db
        Mem_Buf_Limit 50MB
        Skip_Long_Lines On

    # Decode the engine's structured-JSON log record sitting inside the Docker `log`
    # field so Elasticsearch sees `level`, `loggerName`, `service.name`, etc. as
    # first-class columns. Without this filter every Sutra application log lands in ES
    # as a single string blob and Kibana's structured queries (level=ERROR, …) silently
    # match nothing. The decoder is a no-op when the inner payload isn't JSON (any
    # pre-subscriber startup line) — those records pass through unchanged.
    [FILTER]
        Name parser
        Match sutra.logs.*
        Key_Name log
        Parser sutra_json
        Reserve_Data On
        Preserve_Key Off

    [INPUT]
        Name tail
        Tag sutra.audit.*
        Path ${var.fluent_bit.audit_tail_path}
        Path_Key file_path
        Parser json
        DB /var/log/flb_sutra_audit.db
        Read_from_Head true
  EOT

  fluent_bit_forward_block = <<-EOT

    [INPUT]
        Name forward
        Listen 0.0.0.0
        Port 24224
        Tag_Prefix sutra.forward.
  EOT

  fluent_bit_forward_input = var.fluent_bit.expose_forward_port ? local.fluent_bit_forward_block : ""

  # Lua filter to extract tenant id from JSONL audit file path. Active only
  # when per-tenant fan-out is requested. The script itself is mounted into
  # the pod via the Helm chart's luaScripts key (see helm_release.fluent_bit).
  fluent_bit_lua_filter_block = <<-EOT
    [FILTER]
        Name lua
        Match sutra.audit.*
        Script /fluent-bit/scripts/extract_tenant.lua
        Call extract_tenant
  EOT

  fluent_bit_filters = var.fluent_bit.audit_fanout_per_tenant ? local.fluent_bit_lua_filter_block : ""

  fluent_bit_logs_output = <<-EOT
    [OUTPUT]
        Name es
        Match sutra.logs.*
        Host sutra-es-es-http.${var.namespace}.svc
        Port 9200
        HTTP_User elastic
        HTTP_Passwd $${ES_PASSWORD}
        tls On
        tls.verify Off
        Index sutra-logs
        Type _doc
        Replace_Dots On
        Suppress_Type_Name On
  EOT

  # When fan-out is on, route audit records to per-tenant indices using
  # Logstash_Prefix_Key against the tenant_id field added by the Lua filter.
  # When off, all audit records land in a single sutra-audit index.
  fluent_bit_audit_output_fanout = <<-EOT

    [OUTPUT]
        Name es
        Match sutra.audit.*
        Host sutra-es-es-http.${var.namespace}.svc
        Port 9200
        HTTP_User elastic
        HTTP_Passwd $${ES_PASSWORD}
        tls On
        tls.verify Off
        Logstash_Format On
        Logstash_Prefix sutra-audit
        Logstash_Prefix_Key tenant_id
        Logstash_DateFormat %Y.%m.%d
        Type _doc
        Replace_Dots On
        Suppress_Type_Name On
  EOT

  fluent_bit_audit_output_single = <<-EOT

    [OUTPUT]
        Name es
        Match sutra.audit.*
        Host sutra-es-es-http.${var.namespace}.svc
        Port 9200
        HTTP_User elastic
        HTTP_Passwd $${ES_PASSWORD}
        tls On
        tls.verify Off
        Index sutra-audit
        Type _doc
        Replace_Dots On
        Suppress_Type_Name On
  EOT

  fluent_bit_audit_output = var.fluent_bit.audit_fanout_per_tenant ? local.fluent_bit_audit_output_fanout : local.fluent_bit_audit_output_single
}

# ===== Namespace =====

resource "kubernetes_namespace_v1" "this" {
  count = var.create_namespace ? 1 : 0
  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

# ===== Elastic Cloud on Kubernetes (ECK) operator =====
# Installs the operator cluster-wide; safe to omit if the operator is already installed elsewhere.

resource "helm_release" "eck_operator" {
  count            = var.elasticsearch.eck_operator ? 1 : 0
  name             = "elastic-operator"
  namespace        = "elastic-system"
  create_namespace = true
  repository       = "https://helm.elastic.co"
  chart            = "eck-operator"
  version          = "2.13.0"
}

# ===== Elasticsearch (via ECK CRD) =====

resource "kubectl_manifest" "elasticsearch" {
  depends_on = [helm_release.eck_operator, kubernetes_namespace_v1.this]
  yaml_body = yamlencode({
    apiVersion = "elasticsearch.k8s.elastic.co/v1"
    kind       = "Elasticsearch"
    metadata = {
      name      = "sutra-es"
      namespace = var.namespace
      labels    = local.labels
    }
    spec = {
      version = var.elasticsearch.version
      nodeSets = [{
        name  = "default"
        count = var.elasticsearch.replicas
        config = {
          "node.store.allow_mmap" = false
        }
        podTemplate = {
          spec = {
            containers = [{
              name = "elasticsearch"
              env = [{
                name  = "ES_JAVA_OPTS"
                value = "-Xms${var.elasticsearch.heap_size} -Xmx${var.elasticsearch.heap_size}"
              }]
              resources = {
                requests = { memory = "2Gi", cpu = "500m" }
                limits   = { memory = "2Gi", cpu = "2000m" }
              }
            }]
          }
        }
        volumeClaimTemplates = [{
          metadata = { name = "elasticsearch-data" }
          spec = {
            accessModes      = ["ReadWriteOnce"]
            storageClassName = var.storage_class
            resources = {
              requests = { storage = var.elasticsearch.storage_size }
            }
          }
        }]
      }]
    }
  })
}

# ===== Kibana =====

resource "kubectl_manifest" "kibana" {
  depends_on = [kubectl_manifest.elasticsearch]
  yaml_body = yamlencode({
    apiVersion = "kibana.k8s.elastic.co/v1"
    kind       = "Kibana"
    metadata = {
      name      = "sutra-kibana"
      namespace = var.namespace
      labels    = local.labels
    }
    spec = {
      version = var.kibana.version
      count   = var.kibana.replicas
      elasticsearchRef = {
        name = "sutra-es"
      }
    }
  })
}

# ===== OpenTelemetry Collector =====

resource "helm_release" "otel_collector" {
  depends_on = [kubectl_manifest.elasticsearch, kubernetes_namespace_v1.this]
  name       = "otel-collector"
  namespace  = var.namespace
  repository = "https://open-telemetry.github.io/opentelemetry-helm-charts"
  chart      = "opentelemetry-collector"
  version    = var.otel_collector.chart_version

  values = [yamlencode({
    mode         = "deployment"
    replicaCount = var.otel_collector.replicas
    image = {
      repository = "otel/opentelemetry-collector-contrib"
    }
    config = {
      receivers = {
        otlp = {
          protocols = {
            grpc = { endpoint = "0.0.0.0:4317" }
            http = { endpoint = "0.0.0.0:4318" }
          }
        }
      }
      exporters = {
        elasticsearch = {
          endpoints     = ["https://sutra-es-es-http.${var.namespace}.svc:9200"]
          traces_index  = "sutra-traces"
          logs_index    = "sutra-logs"
          metrics_index = "sutra-metrics"
          tls = {
            insecure_skip_verify = false
            ca_file              = "/etc/es-certs/ca.crt"
          }
        }
      }
      service = {
        pipelines = {
          traces  = { receivers = ["otlp"], exporters = ["elasticsearch"] }
          metrics = { receivers = ["otlp"], exporters = ["elasticsearch"] }
          logs    = { receivers = ["otlp"], exporters = ["elasticsearch"] }
        }
      }
    }
    extraVolumeMounts = [{
      name      = "es-certs"
      mountPath = "/etc/es-certs"
      readOnly  = true
    }]
    extraVolumes = [{
      name = "es-certs"
      secret = {
        secretName = "sutra-es-es-http-certs-public"
      }
    }]
  })]
}

# ===== Fluent Bit =====

resource "helm_release" "fluent_bit" {
  depends_on = [kubectl_manifest.elasticsearch, kubernetes_namespace_v1.this]
  name       = "fluent-bit"
  namespace  = var.namespace
  repository = "https://fluent.github.io/helm-charts"
  chart      = "fluent-bit"
  version    = var.fluent_bit.chart_version

  values = [yamlencode({
    kind = "DaemonSet"
    config = {
      service = <<-EOT
        [SERVICE]
            Daemon Off
            Flush 1
            Log_Level info
            Parsers_File parsers.conf
            HTTP_Server On
            HTTP_Listen 0.0.0.0
            HTTP_Port 2020
      EOT
      inputs  = "${local.fluent_bit_inputs}${local.fluent_bit_forward_input}"
      filters = local.fluent_bit_filters
      outputs = "${local.fluent_bit_logs_output}${local.fluent_bit_audit_output}"
      customParsers = <<-EOT
        # Parser used by the parser-filter on sutra.logs.* to decode the inner JSON
        # log record sitting inside the Docker `log` field. sutra-engine writes one
        # JSON object per line on stdout with the fields: timestamp, sequence, level,
        # loggerName, message, threadName, service.name, plus traceId/spanId/sampled
        # when the event fires inside a live exported span, plus flattened event fields.
        [PARSER]
            Name sutra_json
            Format json
            Time_Key timestamp
            Time_Format %Y-%m-%dT%H:%M:%S.%L%z
            Time_Keep On
      EOT
    }
    luaScripts = var.fluent_bit.audit_fanout_per_tenant ? {
      "extract_tenant.lua" = file("${path.module}/scripts/extract_tenant.lua")
    } : {}
    env = [{
      name = "ES_PASSWORD"
      valueFrom = {
        secretKeyRef = {
          name = "sutra-es-es-elastic-user"
          key  = "elastic"
        }
      }
    }]
    service = {
      type = "ClusterIP"
      additionalPorts = var.fluent_bit.expose_forward_port ? [{
        name       = "forward"
        port       = 24224
        targetPort = 24224
      }] : []
    }
  })]
}

# ===== Index Lifecycle Management policies =====

resource "kubectl_manifest" "ilm_policies" {
  for_each = {
    logs    = var.ilm.logs_retention_days
    metrics = var.ilm.metrics_retention_days
    traces  = var.ilm.traces_retention_days
    audit   = var.ilm.audit_retention_days
  }
  depends_on = [kubectl_manifest.elasticsearch]
  yaml_body = yamlencode({
    apiVersion = "batch/v1"
    kind       = "Job"
    metadata = {
      name      = "sutra-ilm-${each.key}"
      namespace = var.namespace
      labels    = local.labels
    }
    spec = {
      template = {
        spec = {
          restartPolicy = "OnFailure"
          containers = [{
            name  = "ilm"
            image = "curlimages/curl:8.7.1"
            command = [
              "sh", "-c",
              <<-EOT
                curl -k -u elastic:$ES_PASSWORD -X PUT \
                  https://sutra-es-es-http.${var.namespace}.svc:9200/_ilm/policy/sutra-${each.key}-policy \
                  -H 'Content-Type: application/json' \
                  -d '{"policy":{"phases":{"hot":{"actions":{"rollover":{"max_age":"7d","max_size":"50gb"}}},"delete":{"min_age":"${each.value}d","actions":{"delete":{}}}}}}'
              EOT
            ]
            env = [{
              name = "ES_PASSWORD"
              valueFrom = {
                secretKeyRef = {
                  name = "sutra-es-es-elastic-user"
                  key  = "elastic"
                }
              }
            }]
          }]
        }
      }
    }
  })
}
