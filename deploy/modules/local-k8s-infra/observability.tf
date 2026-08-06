# =============================================================================
# Cluster-level observability — provisioned as part of bring-up, BEFORE any Sutra
# component, so a fresh `tofu apply` (after any teardown) reproduces the entire
# pipeline automatically (per user direction 2026-06-24: "all these integration
# points must be done automatically ... after a teardown and init").
#
# Pipeline (everything over OTLP → Elasticsearch, the single backend):
#   logs    : engine logs               → OTLP → collector → ES  sutra-app-logs
#   traces  : engine OTLP                → OTLP → collector → ES  sutra-traces
#   metrics : engine micrometer→OTel     → OTLP → collector → ES  metrics-* stream
#
# ES + Kibana (ECK) are created in main.tf. This file adds: the OTel Collector, the
# LoadBalancer services (browser/IT access), the auto-created Kibana data views, and
# the exported creds/URLs. All via kubectl_manifest (alekc); this is the INFRA stage,
# so it runs against an already-existing cluster (kubeconfig passed in as
# var.kubeconfig_path) — no fresh-apply provider cycle.
#
# Gated on install_efk (ES/Kibana presence) — no EFK, no collector/views.
# =============================================================================

locals {
  observability_enabled = var.install_efk ? 1 : 0

  # OTel Collector config: one OTLP receiver for all three signals; ECS mapping mode
  # (clean message/log.level/trace.id fields, and the path that actually lands LOG records);
  # NaN datapoints dropped so one bad gauge can't sink a whole metrics batch.
  otel_collector_config = <<-EOT
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
          http:
            endpoint: 0.0.0.0:4318
    processors:
      batch: {}
      filter/drop_nan:
        error_mode: ignore
        metrics:
          datapoint:
            - value_double != value_double
      # Resolve a dotted-vs-scalar attribute collision: a metric carrying DOTTED
      # `pool.name`/`pool.type` attributes collides with any metric carrying a
      # SCALAR `pool`. Under ECS mapping ES turns the dotted keys into an OBJECT
      # field `pool`, then rejects the scalar-`pool` docs ("object mapping for
      # [pool] ... found a concrete value"), silently dropping them. Rename the
      # dotted side to flat keys so `pool` stays a scalar keyword and nothing collides.
      transform/pool_attr:
        error_mode: ignore
        metric_statements:
          - context: datapoint
            statements:
              - set(attributes["pool_name"], attributes["pool.name"]) where attributes["pool.name"] != nil
              - delete_key(attributes, "pool.name")
              - set(attributes["pool_type"], attributes["pool.type"]) where attributes["pool.type"] != nil
              - delete_key(attributes, "pool.type")
    exporters:
      debug:
        verbosity: normal
      elasticsearch:
        endpoints: ["https://sutra-es-es-http:9200"]
        user: elastic
        password: "$${env:ES_PASSWORD}"
        tls:
          insecure_skip_verify: true
        mapping:
          mode: ecs
        traces_index: sutra-traces
        metrics_index: sutra-metrics
        logs_index: sutra-app-logs
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [debug, elasticsearch]
        metrics:
          receivers: [otlp]
          processors: [transform/pool_attr, filter/drop_nan, batch]
          exporters: [debug, elasticsearch]
        logs:
          receivers: [otlp]
          processors: [batch]
          exporters: [debug, elasticsearch]
  EOT
}

# ----- OTel Collector: ConfigMap + Deployment + ClusterIP Service --------------
resource "kubectl_manifest" "otel_collector_config" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.elasticsearch]
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "ConfigMap"
    metadata   = { name = "otel-collector-config", namespace = "default" }
    data       = { "config.yaml" = local.otel_collector_config }
  })
}

resource "kubectl_manifest" "otel_collector" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.otel_collector_config]
  yaml_body = yamlencode({
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata   = { name = "otel-collector", namespace = "default", labels = { app = "otel-collector" } }
    spec = {
      replicas = 1
      selector = { matchLabels = { app = "otel-collector" } }
      template = {
        metadata = { labels = { app = "otel-collector" } }
        spec = {
          containers = [{
            name  = "collector"
            image = "otel/opentelemetry-collector-contrib:${var.otel_collector_version}"
            args  = ["--config=/etc/otel/config.yaml"]
            env = [{
              name      = "ES_PASSWORD"
              valueFrom = { secretKeyRef = { name = "sutra-es-es-elastic-user", key = "elastic" } }
            }]
            ports        = [{ containerPort = 4317 }, { containerPort = 4318 }]
            volumeMounts = [{ name = "config", mountPath = "/etc/otel" }]
          }]
          volumes = [{ name = "config", configMap = { name = "otel-collector-config" } }]
        }
      }
    }
  })
}

resource "kubectl_manifest" "otel_collector_svc" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.otel_collector]
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "Service"
    # ClusterIP named `otel-collector` so engine pods reach it at otel-collector:4317.
    metadata = { name = "otel-collector", namespace = "default", labels = { app = "otel-collector" } }
    spec = {
      selector = { app = "otel-collector" }
      ports = [
        { name = "otlp-grpc", port = 4317, targetPort = 4317 },
        { name = "otlp-http", port = 4318, targetPort = 4318 },
      ]
    }
  })
}

# ----- LoadBalancer services (browser + IT access) ----------------------------
resource "kubectl_manifest" "kibana_lb" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.kibana, null_resource.metallb_pool]
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "Service"
    metadata   = { name = "kibana-lb", namespace = "default", labels = { app = "kibana-lb" } }
    spec = {
      type     = "LoadBalancer"
      selector = { "kibana.k8s.elastic.co/name" = "sutra-kb" }
      ports    = [{ name = "https", port = 5601, targetPort = 5601 }]
    }
  })
}

resource "kubectl_manifest" "es_lb" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.elasticsearch, null_resource.metallb_pool]
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "Service"
    metadata   = { name = "es-lb", namespace = "default", labels = { app = "es-lb" } }
    spec = {
      type     = "LoadBalancer"
      selector = { "elasticsearch.k8s.elastic.co/cluster-name" = "sutra-es" }
      ports    = [{ name = "https", port = 9200, targetPort = 9200 }]
    }
  })
}

resource "kubectl_manifest" "otel_collector_lb" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.otel_collector_svc, null_resource.metallb_pool]
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "Service"
    metadata   = { name = "otel-collector-lb", namespace = "default", labels = { app = "otel-collector" } }
    spec = {
      type     = "LoadBalancer"
      selector = { app = "otel-collector" }
      ports = [
        { name = "otlp-grpc", port = 4317, targetPort = 4317 },
        { name = "otlp-http", port = 4318, targetPort = 4318 },
      ]
    }
  })
}

# ----- Auto-create the Kibana data views (logs / traces / metrics) ------------
# A Job that waits for Kibana then POSTs the three data views with FIXED ids, so it is
# idempotent (re-runs 409 on existing ids, ignored). This is the "integration done before
# sutra components" step — after init, Discover has all three views with no manual curl.
resource "kubectl_manifest" "kibana_data_views" {
  count      = local.observability_enabled
  depends_on = [kubectl_manifest.kibana]
  yaml_body = yamlencode({
    apiVersion = "batch/v1"
    kind       = "Job"
    metadata   = { name = "sutra-kibana-data-views", namespace = "default" }
    spec = {
      backoffLimit = 30
      template = {
        spec = {
          restartPolicy = "OnFailure"
          containers = [{
            name  = "data-views"
            image = "curlimages/curl:8.7.1"
            env = [{
              name      = "ES_PASSWORD"
              valueFrom = { secretKeyRef = { name = "sutra-es-es-elastic-user", key = "elastic" } }
            }]
            command = ["sh", "-c", <<-EOT
              set -e
              KB=https://sutra-kb-kb-http:5601
              echo "waiting for Kibana..."
              until curl -sk -u "elastic:$ES_PASSWORD" "$KB/api/status" | grep -q '"level":"available"'; do sleep 5; done
              # api/status "available" can flip true before the saved-objects /
              # data_views API is actually ready, which returns a transient HTTP 400.
              # allowNoIndex:true lets the view be created even before its backing
              # index exists (verified), so the ONLY failure mode is that readiness
              # race — RETRY each create until a 2xx (409 = already exists = ok).
              # A fire-once `|| true` would false-succeed on the 400 and leave Kibana
              # with no data views (the "add integration" prompt the user hit).
              create() {
                for i in $(seq 1 60); do
                  code=$(curl -sk -u "elastic:$ES_PASSWORD" -X POST "$KB/api/data_views/data_view" \
                    -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d "$1" \
                    -o /dev/null -w "%%{http_code}")
                  case "$code" in
                    200|201|409) echo "  data view -> HTTP $code (ok)"; return 0 ;;
                    *) echo "  data view -> HTTP $code (not ready, retry $i/60)"; sleep 5 ;;
                  esac
                done
                echo "  ERROR: data view create never returned 2xx"; return 1
              }
              create '{"data_view":{"id":"sutra-logs","title":"sutra-app-logs*","name":"Sutra Logs (OTLP)","timeFieldName":"@timestamp","allowNoIndex":true}}'
              create '{"data_view":{"id":"sutra-traces","title":"sutra-traces*","name":"Sutra Traces","timeFieldName":"@timestamp","allowNoIndex":true}}'
              create '{"data_view":{"id":"sutra-metrics","title":"metrics-*,.ds-metrics-*","name":"Sutra Metrics","timeFieldName":"@timestamp","allowNoIndex":true}}'
              echo "data views created."
            EOT
            ]
          }]
        }
      }
    }
  })
}

# ----- Print observability access info at the end of bring-up -----------------
# NOTE: tofu data sources can't read the LB IPs / elastic password here — their provider would
# be configured from this module's own cluster outputs, which forces provider evaluation before
# the cluster exists (breaks the kubectl provider's lazy-load on a fresh apply). Instead a
# local-exec on the kind kubeconfig fetches and prints them once everything is up, satisfying
# "creds + URLs show up after the environment is ready" without the provider cycle.
resource "null_resource" "observability_access" {
  count = local.observability_enabled
  depends_on = [
    kubectl_manifest.kibana_lb,
    kubectl_manifest.es_lb,
    kubectl_manifest.otel_collector_lb,
    kubectl_manifest.kibana_data_views,
  ]

  triggers = {
    kubeconfig = var.kubeconfig_path
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      export KUBECONFIG='${var.kubeconfig_path}'
      echo "  waiting for LoadBalancer IPs..."
      for svc in kibana-lb es-lb otel-collector-lb; do
        for i in $(seq 1 30); do
          ip=$(kubectl get svc -n default "$svc" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
          [ -n "$ip" ] && break; sleep 2
        done
      done
      pw=$(kubectl get secret -n default sutra-es-es-elastic-user -o jsonpath='{.data.elastic}' 2>/dev/null | base64 -d)
      kib=$(kubectl get svc -n default kibana-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
      es=$(kubectl get svc -n default es-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
      col=$(kubectl get svc -n default otel-collector-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
      out="$(dirname '${var.kubeconfig_path}')/observability-access.txt"
      {
        echo "Kibana       : https://$kib:5601   (user: elastic)"
        echo "Elasticsearch: https://$es:9200     (user: elastic)"
        echo "OTel Collector OTLP: $col:4317 (grpc) / $col:4318 (http)"
        echo "elastic password   : $pw"
      } | tee "$out"
      echo "  (also written to $out)"
    EOT
  }
}
