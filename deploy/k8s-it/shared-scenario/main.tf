# R14 shared-instance k8s harness — ONE engine deployment, hot-deployed packages.
#
# Ruled 2026-07-13 (project_rust_first_rewrite.md R14): one shared engine Deployment is
# provisioned HERE by tofu; example packages deploy to it live via `sutra deploy`, which
# patches the estate Secret first and then the `sutra-deployments` ConfigMap through the
# k8s API — kubelet syncs the mounted volume and the engine's deployments-dir watcher runs
# the two-phase flip. The engine stays a passive directory-consumer: NO management/upload
# endpoint, no new network surface. This module replaces the retired per-example scenario
# configs (fednow pacs08 + swift-mt-mx k8s-it/scenario, approval-hold + money-transfer
# deploy/tofu).
#
# What it provisions:
#   - postgres (engine persistence) + a DB credentials Secret
#   - rabbitmq (one broker; ClusterIP alias Services carry the per-example host names)
#   - the estate Secret `sutra-secrets`, volume-mounted 0400/runAsNonRoot at
#     /etc/sutra/secrets (SUTRA_SECRETS_DIR) — `secret:KEY` refs resolve here; `sutra
#     deploy --secret` merges keys; kubelet live-syncs value rotation (applies on next flip)
#   - the EMPTY `sutra-deployments` ConfigMap (binary_data), mounted at
#     /etc/sutra/deployments (SUTRA_DEPLOYMENTS_DIR) — `sutra deploy`/`undeploy` own its
#     entries (tofu ignores them, see lifecycle below)
#   - the engine Deployment (canonical SUTRA_* env incl. the RLS-bypass harness posture)
#     + a ClusterIP Service + ONE Ingress (the cluster's ingress-nginx replaces the
#     per-example NodePort/LB engine exposure)
#
# Lifecycle: the operator applies this ONCE per environment (after the cluster + infra
# stages); IT classes re-apply idempotently (only their recorder-endpoint env vars roll the
# engine pod) and hot-deploy/undeploy their packages against the running instance. Secrets
# posture per the R14 security block: credentials NEVER enter the ConfigMap; the estate
# store is a real k8s Secret (tmpfs-backed files, no /proc environ leak), byte-compatible
# with Vault-agent/CSI delivery later.

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

# ===== Generated credentials =====
# Lowercase + numeric DB password (URL-safe, no percent-encoding); mixed-case broker
# password (AMQP/URL-safe, no specials). Same conventions as the retired per-example
# scenarios.

resource "random_password" "db_password" {
  length           = 24
  special          = false
  upper            = false
  numeric          = true
  lower            = true
  override_special = ""
}

resource "random_password" "rabbitmq" {
  length  = 24
  special = false
  upper   = true
  lower   = true
  numeric = true
}

# ===== DB credentials Secret (engine persistence + the money-transfer conventional refs) =====

resource "kubernetes_secret_v1" "db_creds" {
  metadata {
    name      = "sutra-shared-db"
    namespace = var.namespace
  }
  type = "Opaque"
  data = {
    POSTGRES_DB       = "sutra"
    POSTGRES_USER     = "sutra"
    POSTGRES_PASSWORD = random_password.db_password.result
    # Native URL form — the canonical-env (Rust) engine takes postgres schemes directly.
    DB_URL = "postgresql://postgres-shared.${var.namespace}.svc:5432/sutra"
  }
}

# ===== The estate Secret (R14 secrets amendment) =====
# Seeded with the shared broker service account; `sutra deploy --secret KEY=VALUE` merges
# further per-deployment keys. Mounted at /etc/sutra/secrets where the engine's
# `secret:KEY` refs resolve; ALSO the source of the conventional ${RABBITMQ_*} env refs the
# example channel YAMLs use today. After creation the DATA is CLI-owned — tofu must never
# reconcile away keys `sutra deploy` merged, hence the lifecycle ignore.

resource "kubernetes_secret_v1" "estate" {
  metadata {
    name      = "sutra-secrets"
    namespace = var.namespace
  }
  type = "Opaque"
  data = {
    RABBITMQ_USERNAME = "sutra"
    RABBITMQ_PASSWORD = random_password.rabbitmq.result
  }
  lifecycle {
    ignore_changes = [data, binary_data]
  }
}

# ===== The admin auth key (the auth-key + secret gate) =====
# The static key/secret the engine's `/admin/*` gate (sutra.admin.auth.scheme=apikey) expects and
# `sutra deploy --api` presents. A tofu-managed Secret (NO ignore_changes, unlike the estate Secret)
# so a re-apply keeps engine + harness in sync; the harness reads ADMIN_API_KEY from it at runtime.
resource "random_password" "admin_api_key" {
  length  = 40
  special = false
}

resource "kubernetes_secret_v1" "admin_auth" {
  metadata {
    name      = "sutra-admin-auth"
    namespace = var.namespace
  }
  type = "Opaque"
  data = {
    ADMIN_API_KEY = random_password.admin_api_key.result
  }
}

# ===== Postgres =====

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres-shared"
    namespace = var.namespace
  }
  spec {
    selector = { app = "postgres-shared" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

resource "kubernetes_deployment_v1" "postgres" {
  metadata {
    name      = "postgres-shared"
    namespace = var.namespace
    labels    = { app = "postgres-shared" }
  }
  spec {
    replicas = 1
    selector { match_labels = { app = "postgres-shared" } }
    template {
      metadata { labels = { app = "postgres-shared" } }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          # Credentials from the Secret — no literals in the manifest.
          env_from {
            secret_ref { name = kubernetes_secret_v1.db_creds.metadata[0].name }
          }
          port { container_port = 5432 }
          readiness_probe {
            exec { command = ["pg_isready", "-U", "sutra", "-d", "sutra"] }
            initial_delay_seconds = 3
            period_seconds        = 2
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }
        }
        volume {
          name = "data"
          empty_dir {}
        }
      }
    }
  }
}

# ===== RabbitMQ — ONE broker for every deployed package =====
# The default user is the real shared service account from the estate Secret (no guest, no
# loopback backdoor). Queues are NOT declared here: on the shared instance queue topology
# is per-deployment, so each IT/operator declares its queues (mgmt API/AMQP) BEFORE
# `sutra deploy` — the engine's broker triggers declare passively on the activation flip.

resource "kubernetes_deployment_v1" "rabbitmq" {
  metadata {
    name      = "rabbitmq"
    namespace = var.namespace
    labels    = { app = "rabbitmq-shared" }
  }
  spec {
    replicas = 1
    selector { match_labels = { app = "rabbitmq-shared" } }
    template {
      metadata { labels = { app = "rabbitmq-shared" } }
      spec {
        container {
          name  = "rabbitmq"
          image = "rabbitmq:3.13-management-alpine"
          env {
            name = "RABBITMQ_DEFAULT_USER"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.estate.metadata[0].name
                key  = "RABBITMQ_USERNAME"
              }
            }
          }
          env {
            name = "RABBITMQ_DEFAULT_PASS"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.estate.metadata[0].name
                key  = "RABBITMQ_PASSWORD"
              }
            }
          }
          port { container_port = 5672 }
          port { container_port = 15672 }
          readiness_probe {
            exec { command = ["rabbitmq-diagnostics", "-q", "ping"] }
            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 10
          }
        }
      }
    }
  }
}

# Host-reachable broker endpoint (MetalLB LB): the ITs publish AMQP (5672) and drive the
# mgmt API (15672) from the host. AMQP is not HTTP — the Ingress below covers only the
# engine; the broker keeps its LB.
resource "kubernetes_service_v1" "rabbitmq" {
  metadata {
    name      = "rabbitmq"
    namespace = var.namespace
  }
  spec {
    type     = "LoadBalancer"
    selector = { app = "rabbitmq-shared" }
    port {
      name        = "amqp"
      port        = 5672
      target_port = 5672
    }
    port {
      name        = "management"
      port        = 15672
      target_port = 15672
    }
  }
}

# In-cluster alias Services: the example channel YAMLs name their broker hosts
# `rabbitmq` (fednow), `rabbitmq-mtmx` (swift-mt-mx) and `rabbit` (money-transfer). On the
# shared instance all three resolve to the ONE broker — aliasing here keeps every shipped
# package deployable without editing its channels.yaml (R14: "examples converge on shared
# conventional names" — the aliases ARE the convergence, zero package edits).
resource "kubernetes_service_v1" "rabbitmq_alias_mtmx" {
  metadata {
    name      = "rabbitmq-mtmx"
    namespace = var.namespace
  }
  spec {
    selector = { app = "rabbitmq-shared" }
    port {
      name        = "amqp"
      port        = 5672
      target_port = 5672
    }
    port {
      name        = "management"
      port        = 15672
      target_port = 15672
    }
  }
}

resource "kubernetes_service_v1" "rabbitmq_alias_rabbit" {
  metadata {
    name      = "rabbit"
    namespace = var.namespace
  }
  spec {
    selector = { app = "rabbitmq-shared" }
    port {
      name        = "amqp"
      port        = 5672
      target_port = 5672
    }
  }
}

# ===== The deployments ConfigMap (the engine's archive source, CLI-owned entries) =====
# Starts EMPTY. `sutra deploy` upserts one binary_data entry per archive (key = archive
# file name); `sutra undeploy` deletes it. Tofu owns the OBJECT, the CLI owns the ENTRIES —
# without the lifecycle ignore every re-apply would wipe the deployed estate.

resource "kubernetes_config_map_v1" "deployments" {
  metadata {
    name      = "sutra-deployments"
    namespace = var.namespace
  }
  binary_data = {}
  lifecycle {
    ignore_changes = [binary_data, data]
  }
}

# ===== The ONE engine Deployment =====

locals {
  # Canonical SUTRA_* env (R12-T2 — the Rust image takes no framework-alias names).
  # value/secret/key rows follow the retired scenarios' dynamic-env convention.
  engine_env = [
    # Engine persistence (its own store — not the per-package datastores).
    { name = "SUTRA_DATASOURCE_URL", value = null, secret = "db", key = "DB_URL" },
    { name = "SUTRA_DATASOURCE_USERNAME", value = null, secret = "db", key = "POSTGRES_USER" },
    { name = "SUTRA_DATASOURCE_PASSWORD", value = null, secret = "db", key = "POSTGRES_PASSWORD" },
    # Deployment source: the DB-backed store (deployment_archive table). Archives are deployed
    # through the sync/async API (`sutra deploy --api`), NOT the ConfigMap/dir — activation is
    # deterministic (no kubelet ConfigMap-propagation window). The `deployment_archive` migration
    # (V1001) is baked in the image and self-applied on boot, so no seeding is needed.
    { name = "SUTRA_DEPLOYMENT_SOURCE", value = "db", secret = null, key = null },
    # The deploy API (`POST/DELETE /admin/deployments`) is gated by the auth-key+secret model the
    # channels use (`sutra.admin.auth.*`) — NOT bypassed. The engine expects a static key/secret in
    # the X-API-Key header; the key lives in the `sutra-admin-auth` Secret, injected here as an env
    # var and referenced via `env:`. `sutra deploy --api --api-key <key>` presents it (the harness
    # reads the same value from the Secret).
    { name = "SUTRA_ADMIN_AUTH_SCHEME", value = "apikey", secret = null, key = null },
    { name = "SUTRA_ADMIN_AUTH_KEY_REF", value = "env:SUTRA_ADMIN_API_KEY", secret = null, key = null },
    { name = "SUTRA_ADMIN_API_KEY", value = null, secret = "admin", key = "ADMIN_API_KEY" },
    # Secrets source (live-synced mount below). SUTRA_DEPLOYMENTS_DIR is inert under the db
    # source — the ConfigMap/volume/mount stay provisioned but unused.
    { name = "SUTRA_SECRETS_DIR", value = "/etc/sutra/secrets", secret = null, key = null },
    # Harness posture: the scenario database role (POSTGRES_USER on the official image) is
    # a superuser with BYPASSRLS; relax the RLS-bypass boot refusal HERE only — the
    # dedicated rls_bypass_it suite proves the enforcement itself.
    { name = "SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED", value = "false", secret = null, key = null },
    # Telemetry — one shared service identity for the shared instance.
    { name = "SUTRA_TELEMETRY_OTLP_ENDPOINT", value = var.otlp_endpoint, secret = null, key = null },
    { name = "SUTRA_TELEMETRY_SERVICE_NAME", value = "sutra-engine", secret = null, key = null },
    { name = "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", value = "delta", secret = null, key = null },
    # ---- the conventional ${ENV} refs the example packages resolve (R14 env contract).
    { name = "RABBITMQ_USERNAME", value = null, secret = "estate", key = "RABBITMQ_USERNAME" },
    { name = "RABBITMQ_PASSWORD", value = null, secret = "estate", key = "RABBITMQ_PASSWORD" },
    { name = "FEDNOW_CALLBACK_HOST", value = var.fednow_callback_host, secret = null, key = null },
    { name = "MX_DEST_HOST", value = var.mx_dest_host, secret = null, key = null },
    { name = "MT_CLIENT_HOST", value = var.mt_client_host, secret = null, key = null },
    # money-transfer's datastore refs (env:ACCOUNTS_DB_*) point at the shared postgres.
    { name = "ACCOUNTS_DB_URL", value = null, secret = "db", key = "DB_URL" },
    { name = "ACCOUNTS_DB_USER", value = null, secret = "db", key = "POSTGRES_USER" },
    { name = "ACCOUNTS_DB_PASSWORD", value = null, secret = "db", key = "POSTGRES_PASSWORD" },
  ]
}

resource "kubernetes_deployment_v1" "sutra_engine" {
  # Block the apply until the rollout converges. Every suite passes its own per-run host:port
  # vars, so suite transitions legally diff the env and roll this deployment — without the wait,
  # apply returns while the old pod still serves the Service and the pod swap lands mid-suite
  # (observed 2026-07-28/29: HashiCorp-manager update at apply, state written 8 s later, pod
  # Ready ~1 min after that — provider did not wait). The conformance fixtures additionally gate
  # on rollout convergence via the k8s API as insurance.
  wait_for_rollout = true

  metadata {
    name      = "sutra-engine"
    namespace = var.namespace
    labels    = { app = "sutra-engine" }
  }
  spec {
    replicas = 1
    selector { match_labels = { app = "sutra-engine" } }
    template {
      metadata { labels = { app = "sutra-engine" } }
      spec {
        # R14 security riders: runAsNonRoot; fsGroup grants the non-root engine group-read
        # on the 0400 secret files (kubelet chowns projected files to the fsGroup).
        security_context {
          run_as_non_root = true
          run_as_user     = 10001
          fs_group        = 10001
        }

        container {
          name              = "engine"
          image             = var.engine_image
          image_pull_policy = "Always"

          dynamic "env" {
            for_each = local.engine_env
            content {
              name  = env.value.name
              value = env.value.value
              dynamic "value_from" {
                for_each = env.value.secret == null ? [] : [env.value]
                content {
                  secret_key_ref {
                    name = value_from.value.secret == "db" ? kubernetes_secret_v1.db_creds.metadata[0].name : (value_from.value.secret == "admin" ? kubernetes_secret_v1.admin_auth.metadata[0].name : kubernetes_secret_v1.estate.metadata[0].name)
                    key  = value_from.value.key
                  }
                }
              }
            }
          }

          port {
            name           = "http"
            container_port = 8080
          }

          startup_probe {
            http_get {
              path = "/sutra/health/ready"
              port = "http"
            }
            failure_threshold = 30
            period_seconds    = 5
          }
          readiness_probe {
            http_get {
              path = "/sutra/health/ready"
              port = "http"
            }
            period_seconds = 5
          }
          liveness_probe {
            http_get {
              path = "/sutra/health/live"
              port = "http"
            }
            period_seconds = 10
          }

          # The hot-deploy surface: kubelet live-syncs both mounts; the engine's watcher
          # flips deployments, and `secret:KEY` refs re-resolve on the next flip.
          volume_mount {
            name       = "deployments"
            mount_path = "/etc/sutra/deployments"
            read_only  = true
          }
          volume_mount {
            name       = "secrets"
            mount_path = "/etc/sutra/secrets"
            read_only  = true
          }
        }

        volume {
          name = "deployments"
          config_map {
            name = kubernetes_config_map_v1.deployments.metadata[0].name
          }
        }
        volume {
          name = "secrets"
          secret {
            secret_name = kubernetes_secret_v1.estate.metadata[0].name
            # R14 rider: 0400 in the spec; kubelet's fsGroup handling adds the group-read
            # bit for uid/gid 10001 (tmpfs-backed, never on node disk).
            default_mode = "0400"
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_deployment_v1.postgres,
    kubernetes_deployment_v1.rabbitmq,
  ]
}

resource "kubernetes_service_v1" "sutra_engine" {
  metadata {
    name      = "sutra-engine"
    namespace = var.namespace
    labels    = { app = "sutra-engine" }
  }
  spec {
    # ClusterIP — host traffic comes through the Ingress below, not a per-example
    # NodePort/LB.
    selector = { app = "sutra-engine" }
    port {
      name        = "http"
      port        = 8080
      target_port = "http"
      protocol    = "TCP"
    }
  }
}

# ===== ONE Ingress (ingress-nginx, provisioned by the infra stage) =====
# Catch-all rule: every path (/channels/**, /sutra/health/**) routes to the shared engine. The
# ITs reach it at the ingress-nginx controller's LoadBalancer IP, port 80.

resource "kubernetes_ingress_v1" "sutra_engine" {
  metadata {
    name      = "sutra-engine"
    namespace = var.namespace
    # A large project's synchronous deploy (`POST /admin/deployments`) holds the request open across
    # the activation flip (registry rebuild + transport rewire). Raise the nginx upstream timeouts
    # so the ingress never cuts a long deploy in half; the async LRO path (`?mode=async`) removes the
    # long request entirely, and the CLI falls back to a status poll on a cut — these are defence in
    # depth for the sync path.
    annotations = {
      "nginx.ingress.kubernetes.io/proxy-read-timeout" = "300"
      "nginx.ingress.kubernetes.io/proxy-send-timeout" = "300"
    }
  }
  spec {
    ingress_class_name = "nginx"
    rule {
      http {
        path {
          path      = "/"
          path_type = "Prefix"
          backend {
            service {
              name = kubernetes_service_v1.sutra_engine.metadata[0].name
              port {
                number = 8080
              }
            }
          }
        }
      }
    }
  }
}
