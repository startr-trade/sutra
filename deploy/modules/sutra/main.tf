locals {
  match_labels = {
    "app.kubernetes.io/name"      = var.name
    "app.kubernetes.io/component" = "engine"
  }
  labels = merge(local.match_labels, {
    "app.kubernetes.io/part-of"    = "sutra"
    "app.kubernetes.io/managed-by" = "opentofu"
  }, var.labels)

  # Parse "secret-name#key" references
  db_username_secret = split("#", var.database.username_secret_ref)[0]
  db_username_key    = split("#", var.database.username_secret_ref)[1]
  db_password_secret = split("#", var.database.password_secret_ref)[0]
  db_password_key    = split("#", var.database.password_secret_ref)[1]

  otlp_url = "${var.observability.otlp_tls ? "https" : "http"}://${var.observability.otlp_endpoint}"
}

# ===== Namespace =====

resource "kubernetes_namespace_v1" "this" {
  count = var.create_namespace ? 1 : 0
  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

# ===== Service account + RBAC =====

resource "kubernetes_service_account_v1" "this" {
  metadata {
    name      = var.service_account_name
    namespace = var.namespace
    labels    = local.labels
  }
  depends_on = [kubernetes_namespace_v1.this]
}

# Lease coordination for timer leader election (see the book's "Replica semantics" chapter)
resource "kubernetes_role_v1" "lease" {
  metadata {
    name      = "${var.name}-lease"
    namespace = var.namespace
    labels    = local.labels
  }
  rule {
    api_groups = ["coordination.k8s.io"]
    resources  = ["leases"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["create", "patch"]
  }
}

resource "kubernetes_role_binding_v1" "lease" {
  metadata {
    name      = "${var.name}-lease"
    namespace = var.namespace
    labels    = local.labels
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.lease.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.this.metadata[0].name
    namespace = var.namespace
  }
}

# ===== Tenant config ConfigMaps =====
# Tenants are declared by the customer as tofu variables; each one becomes a
# kubernetes_config_map_v1 mounted into the engine pod at
# /etc/sutra/resources/tenants/<id>/. The engine watches the mounted directory —
# atomic ConfigMap updates are picked up within ~1s, providing the documented
# hot-reload semantics. Tofu-applied ConfigMaps plus the directory watch are the
# sole tenant-config delivery mechanism (no K8s Custom Resources in this project).

resource "kubernetes_config_map_v1" "tenants" {
  for_each = var.tenants
  metadata {
    name      = "${var.name}-tenant-${each.key}"
    namespace = var.namespace
    labels = merge(local.labels, {
      "app.kubernetes.io/component" = "tenant-config"
      "sutra.startr.trade/tenant"       = each.key
    })
  }
  data = {
    "tenant-configuration.yaml" = yamlencode(merge({ tenantId = each.key }, each.value))
  }
}

# ===== Schema migration Job =====
# One-shot migration runner that exits 0 when the schema is current. The Deployment
# depends_on this resource so replicas never see a half-migrated DB.
#
# Name suffix: a short hash of the inputs that actually change the desired Job spec
# (image + db URL). K8s Job .spec.template is immutable; re-running `tofu apply` with
# the SAME inputs is a no-op (same name, same hash, existing Job is fine), while
# bumping the image produces a NEW Job name and a NEW Job rollout.

locals {
  migrate_name_suffix = substr(sha1(jsonencode({
    image = var.migrate_image
    url   = "postgresql://${var.database.host}:${var.database.port}/${var.database.database}"
  })), 0, 8)
  migrate_job_name = "${var.name}-migrate-${local.migrate_name_suffix}"
}

resource "kubernetes_job_v1" "migrate" {
  count = var.enable_migrate_job ? 1 : 0

  metadata {
    name      = local.migrate_job_name
    namespace = var.namespace
    labels = merge(local.labels, {
      "app.kubernetes.io/component" = "migrate"
    })
  }

  spec {
    backoff_limit              = var.migrate_backoff_limit
    active_deadline_seconds    = var.migrate_active_deadline_seconds
    ttl_seconds_after_finished = 600 # Job + pods auto-pruned 10 minutes after completion

    template {
      metadata {
        labels = merge(local.match_labels, {
          "app.kubernetes.io/component" = "migrate"
        })
      }
      spec {
        service_account_name = kubernetes_service_account_v1.this.metadata[0].name
        restart_policy       = "Never"

        dynamic "image_pull_secrets" {
          for_each = var.image_pull_secrets
          content {
            name = image_pull_secrets.value
          }
        }

        security_context {
          run_as_non_root = true
          run_as_user     = 1001
          seccomp_profile {
            type = "RuntimeDefault"
          }
        }

        container {
          name              = "migrate"
          image             = var.migrate_image
          image_pull_policy = var.migrate_image_pull_policy

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true
            run_as_non_root            = true
            run_as_user                = 1001
            capabilities {
              drop = ["ALL"]
            }
          }

          env {
            name  = "SUTRA_DB_URL"
            value = "postgresql://${var.database.host}:${var.database.port}/${var.database.database}"
          }
          env {
            name = "SUTRA_DB_USERNAME"
            value_from {
              secret_key_ref {
                name = local.db_username_secret
                key  = local.db_username_key
              }
            }
          }
          env {
            name = "SUTRA_DB_PASSWORD"
            value_from {
              secret_key_ref {
                name = local.db_password_secret
                key  = local.db_password_key
              }
            }
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }
        }
      }
    }

    # The migration runner is small and idempotent; one shot is the contract. Two
    # replicas would race the ledger lock — possible but pointless.
    parallelism = 1
    completions = 1
  }

  wait_for_completion = true
  timeouts {
    create = "${var.migrate_active_deadline_seconds + 60}s"
    update = "${var.migrate_active_deadline_seconds + 60}s"
  }

  depends_on = [kubernetes_namespace_v1.this, kubernetes_service_account_v1.this]
}

# ===== Deployment =====

resource "kubernetes_deployment_v1" "this" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }

  # When enable_migrate_job=true, kubernetes_job_v1.migrate is a 1-element list and the
  # splat produces [job]; when false, the list is empty and depends_on is a no-op.
  depends_on = [kubernetes_job_v1.migrate]

  spec {
    replicas = var.autoscaling.enabled ? null : var.replicas
    selector {
      match_labels = local.match_labels
    }
    strategy {
      type = "RollingUpdate"
      rolling_update {
        max_surge       = "1"
        max_unavailable = "0"
      }
    }
    template {
      metadata {
        labels = local.labels
      }
      spec {
        service_account_name             = kubernetes_service_account_v1.this.metadata[0].name
        termination_grace_period_seconds = 60

        dynamic "image_pull_secrets" {
          for_each = var.image_pull_secrets
          content {
            name = image_pull_secrets.value
          }
        }

        security_context {
          run_as_non_root = true
          run_as_user     = 65532
          fs_group        = 65532
          seccomp_profile {
            type = "RuntimeDefault"
          }
        }

        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "topology.kubernetes.io/zone"
          when_unsatisfiable = "ScheduleAnyway"
          label_selector {
            match_labels = local.match_labels
          }
        }

        container {
          name              = "engine"
          image             = var.image
          image_pull_policy = "IfNotPresent"

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true
            capabilities {
              drop = ["ALL"]
            }
          }

          port {
            name           = "http"
            container_port = 8080
          }
          port {
            name           = "management"
            container_port = 9090
          }

          env {
            name  = "SUTRA_DEPLOYMENTS_DIR"
            value = "/etc/sutra/resources"
          }
          # Deployment packages are file-mounted from tofu-managed ConfigMaps; the engine
          # watches the directory and hot-reloads what lands there.

          # Database
          env {
            name  = "SUTRA_DATASOURCE_URL"
            value = "postgresql://${var.database.host}:${var.database.port}/${var.database.database}"
          }
          env {
            name = "SUTRA_DATASOURCE_USERNAME"
            value_from {
              secret_key_ref {
                name = local.db_username_secret
                key  = local.db_username_key
              }
            }
          }
          env {
            name = "SUTRA_DATASOURCE_PASSWORD"
            value_from {
              secret_key_ref {
                name = local.db_password_secret
                key  = local.db_password_key
              }
            }
          }

          # OTLP. The engine exports over gRPC only, so there is no protocol selector;
          # var.observability.otlp_protocol is retained for callers but not passed through.
          env {
            name  = "SUTRA_TELEMETRY_OTLP_ENDPOINT"
            value = local.otlp_url
          }

          # Audit fan-out (optional). The engine ships audit records over OTLP; there is no
          # direct Elasticsearch writer and no separate audit credential env, so
          # var.observability.elasticsearch_endpoint is honoured only as an OTLP endpoint.
          dynamic "env" {
            for_each = var.observability.elasticsearch_endpoint != null ? [var.observability.elasticsearch_endpoint] : []
            content {
              name  = "SUTRA_AUDIT_OTEL_ENDPOINT"
              value = env.value
            }
          }

          # OIDC for the admin API
          env {
            name  = "SUTRA_ADMIN_OIDC_ISSUER"
            value = var.admin_oidc.issuer
          }
          env {
            name  = "SUTRA_ADMIN_OIDC_AUDIENCE"
            value = var.admin_oidc.audience
          }
          dynamic "env" {
            for_each = var.admin_oidc.jwks_url != null ? [var.admin_oidc.jwks_url] : []
            content {
              name  = "SUTRA_ADMIN_OIDC_JWKS"
              value = env.value
            }
          }

          # Extra env
          dynamic "env" {
            for_each = var.extra_env
            content {
              name  = env.key
              value = env.value
            }
          }

          volume_mount {
            name       = "resources"
            mount_path = "/etc/sutra/resources"
            read_only  = true
          }
          # One volume_mount per tofu-managed tenant ConfigMap.
          # Mounting under /etc/sutra/resources/tenants/<id>/ overlays the file-source
          # path that the engine's ResourceLayoutObserver scans.
          dynamic "volume_mount" {
            for_each = kubernetes_config_map_v1.tenants
            content {
              name       = "tenant-${volume_mount.key}"
              mount_path = "/etc/sutra/resources/tenants/${volume_mount.key}"
              read_only  = true
            }
          }
          volume_mount {
            name       = "tmp"
            mount_path = "/tmp"
          }

          startup_probe {
            http_get {
              path = "/sutra/health/ready"
              port = "management"
            }
            failure_threshold = 30
            period_seconds    = 5
          }

          liveness_probe {
            http_get {
              path = "/sutra/health/live"
              port = "management"
            }
            failure_threshold     = 3
            period_seconds        = 10
            initial_delay_seconds = 30
          }

          readiness_probe {
            http_get {
              path = "/sutra/health/ready"
              port = "management"
            }
            failure_threshold = 3
            period_seconds    = 5
          }

          # No preStop HTTP drain: the engine drains on SIGTERM (Kubernetes sends it on
          # pod termination; the Rust engine exposes no HTTP drain admin endpoint).

          resources {
            limits   = var.resources.limits
            requests = var.resources.requests
          }
        }

        volume {
          name = "resources"
          dynamic "config_map" {
            for_each = var.resource_tree.type == "configmap" ? [1] : []
            content {
              name = var.resource_tree.configmap_name
            }
          }
          dynamic "persistent_volume_claim" {
            for_each = var.resource_tree.type == "pvc" ? [1] : []
            content {
              claim_name = var.resource_tree.pvc_claim_name
              read_only  = true
            }
          }
          dynamic "csi" {
            for_each = var.resource_tree.type == "csi" ? [1] : []
            content {
              driver            = var.resource_tree.csi_driver
              read_only         = true
              volume_attributes = merge({ volumeHandle = var.resource_tree.csi_volume_handle }, var.resource_tree.csi_attributes)
            }
          }
        }

        volume {
          name = "tmp"
          empty_dir {}
        }

        # One volume per tofu-managed tenant ConfigMap; mounted above.
        dynamic "volume" {
          for_each = kubernetes_config_map_v1.tenants
          content {
            name = "tenant-${volume.key}"
            config_map {
              name = volume.value.metadata[0].name
            }
          }
        }
      }
    }
  }
}

# ===== Service =====

resource "kubernetes_service_v1" "this" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }
  spec {
    selector = local.match_labels
    port {
      name        = "http"
      port        = 8080
      target_port = "http"
    }
    port {
      name        = "management"
      port        = 9090
      target_port = "management"
    }
    type = "ClusterIP"
  }
}

# ===== PodDisruptionBudget =====

resource "kubernetes_pod_disruption_budget_v1" "this" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }
  spec {
    min_available = 2
    selector {
      match_labels = local.match_labels
    }
  }
}

# ===== NetworkPolicy =====

resource "kubernetes_network_policy_v1" "engine" {
  count = var.network_policy ? 1 : 0
  metadata {
    name      = "${var.name}-egress"
    namespace = var.namespace
    labels    = local.labels
  }
  spec {
    pod_selector {
      match_labels = local.match_labels
    }
    policy_types = ["Egress"]

    # DNS
    egress {
      to {
        namespace_selector {
          match_labels = { "kubernetes.io/metadata.name" = "kube-system" }
        }
      }
      ports {
        port     = 53
        protocol = "UDP"
      }
    }

    # Postgres
    egress {
      ports {
        port     = var.database.port
        protocol = "TCP"
      }
    }

    # OTLP
    egress {
      ports {
        port     = tonumber(split(":", var.observability.otlp_endpoint)[1])
        protocol = "TCP"
      }
    }
  }
}

# ===== KEDA autoscaling =====

resource "kubectl_manifest" "scaledobject" {
  count = var.autoscaling.enabled ? 1 : 0
  yaml_body = yamlencode({
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledObject"
    metadata = {
      name      = var.name
      namespace = var.namespace
      labels    = local.labels
    }
    spec = {
      scaleTargetRef = {
        name = kubernetes_deployment_v1.this.metadata[0].name
      }
      minReplicaCount = var.autoscaling.min_replicas
      maxReplicaCount = var.autoscaling.max_replicas
      cooldownPeriod  = 300
      pollingInterval = 30
      triggers = [
        {
          type = "postgresql"
          metadata = merge(
            {
              query            = <<-EOT
                SELECT COALESCE(SUM(depth), 0) FROM (
                  SELECT count(*) AS depth FROM inbox
                    WHERE instance_id IS NULL AND received_at < now() - INTERVAL '5 seconds'
                  UNION ALL
                  SELECT count(*) AS depth FROM outbox
                    WHERE claimed_at IS NULL AND enqueued_at < now() - INTERVAL '5 seconds'
                ) lagging;
              EOT
              targetQueryValue = tostring(var.autoscaling.target_lagging_work)
            },
            var.autoscaling.keda_db_secret_name != null ? { connectionFromEnv = "KEDA_PG_CONN" } : {}
          )
          authenticationRef = var.autoscaling.keda_db_secret_name != null ? {
            name = var.autoscaling.keda_db_secret_name
          } : null
        }
      ]
    }
  })
}

