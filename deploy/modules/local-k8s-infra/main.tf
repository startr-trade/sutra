# Shared "local Kubernetes for IT" — STAGE 2: INFRA.
#
# Provisions the production-realistic deps + the full observability pipeline on a
# kind cluster that ALREADY EXISTS (created by the local-k8s-cluster module /
# `cluster/` stage):
#
#   * local-registry-hosting ConfigMap (discovery pointer; relocated here so the
#     cluster stage stays provider-light)
#   * MetalLB (LoadBalancer allocator) + derived address pool
#   * ingress-nginx (Ingress controller)
#   * KEDA (event-driven autoscaler — required by deploy/modules/sutra ScaledObject)
#   * EFK: Elasticsearch + Kibana (ECK) + Fluent Bit
#   * OTel Collector + LoadBalancers + Kibana data views + creds export (observability.tf)
#
# The consuming root (infra/) configures the kubernetes/helm/kubectl providers from
# the kubeconfig FILE the cluster stage wrote (a static path passed in as
# var.kubeconfig_path) — NOT from a live cluster resource output. Because the
# cluster already exists when this stage applies, every provider (including the
# eagerly-configuring alekc/kubectl) finds real credentials, so there is no
# fresh-apply provider cycle. That cycle is exactly why the old single-root design
# needed a targeted two-phase `tofu apply`; the three-stage split removes it.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    # kubectl provider (alekc fork) applies the K8s CRs (Elasticsearch/Kibana/
    # collector/LBs). Here it points at an already-running cluster's kubeconfig,
    # so its eager configuration succeeds — unlike in a cluster-creating root.
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.0"
    }
  }
}

# ----- local-registry-hosting ConfigMap -----
# Documented at https://kind.sigs.k8s.io/docs/user/local-registry/ — gives kubectl
# tools (lens, tilt, devspace, …) a discoverable pointer at the host-side registry.
# Lives in this infra stage (not the cluster stage) so the cluster-creating root
# can stay free of any kubernetes provider — see local-k8s-cluster/main.tf.

resource "kubernetes_config_map_v1" "local_registry_hosting" {
  metadata {
    name      = "local-registry-hosting"
    namespace = "kube-public"
  }
  data = {
    "localRegistryHosting.v1" = <<-EOT
      host: "localhost:${var.registry_port}"
      help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
    EOT
  }
}

# ===== Production-realistic deps =====

# ----- MetalLB -----
resource "helm_release" "metallb" {
  count            = var.install_metallb ? 1 : 0
  name             = "metallb"
  repository       = "https://metallb.github.io/metallb"
  chart            = "metallb"
  version          = var.metallb_version
  namespace        = "metallb-system"
  create_namespace = true
  wait             = true
  # 600s (not 300): the speaker DaemonSet runs FRR sidecars whose init containers
  # cold-pull quay.io/frrouting/frr on every worker; on a fresh cluster that pull
  # plus the controller-created metallb-memberlist secret can exceed 5 min, which
  # tripped the previous 300s wait. All five helm releases use the same headroom.
  timeout = 600
}

# MetalLB IPAddressPool + L2Advertisement.
#
# Address pool: when var.metallb_address_pool is empty (the default) the pool is
# DERIVED from the actual `kind` Docker network subnet — kind picks 172.18.0.0/16
# on clean hosts but 172.19/172.20/… when earlier Docker networks already hold the
# lower ranges, so a hardcoded default silently mis-allocates LB IPs onto an
# unreachable subnet. The local-exec reads the live subnet at apply time (per user
# direction 2026-06-24: derive the pool, don't hardcode 172.18). An explicit
# non-empty var.metallb_address_pool overrides the derivation.
resource "null_resource" "metallb_pool" {
  count      = var.install_metallb ? 1 : 0
  depends_on = [helm_release.metallb]

  triggers = {
    kubeconfig = var.kubeconfig_path
    override   = join(",", var.metallb_address_pool)
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      export KUBECONFIG='${var.kubeconfig_path}'
      override='${join(",", var.metallb_address_pool)}'
      if [ -n "$override" ]; then
        pool="$override"
      else
        subnet=$(docker network inspect kind -f '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' 2>/dev/null | grep -E '^[0-9]+\.' | head -1)
        prefix=$(echo "$subnet" | cut -d. -f1-2)
        pool="$${prefix}.255.200-$${prefix}.255.250"
        echo "  metallb pool $pool (derived from kind subnet $subnet)"
      fi
      kubectl -n metallb-system rollout status deploy/metallb-controller --timeout=180s || true
      for i in $(seq 1 12); do
        cat <<YAML | kubectl apply -f - && break || sleep 10
      apiVersion: metallb.io/v1beta1
      kind: IPAddressPool
      metadata:
        name: kind-pool
        namespace: metallb-system
      spec:
        addresses: ["$pool"]
      ---
      apiVersion: metallb.io/v1beta1
      kind: L2Advertisement
      metadata:
        name: kind-l2
        namespace: metallb-system
      spec:
        ipAddressPools: [kind-pool]
      YAML
      done
    EOT
  }
}

# ----- ingress-nginx -----
resource "helm_release" "ingress_nginx" {
  count            = var.install_ingress_nginx ? 1 : 0
  name             = "ingress-nginx"
  repository       = "https://kubernetes.github.io/ingress-nginx"
  chart            = "ingress-nginx"
  version          = var.ingress_nginx_version
  namespace        = "ingress-nginx"
  create_namespace = true
  wait             = true
  timeout          = 600

  # Kind-friendly controller config: hostPort + NodePort instead of LoadBalancer.
  set {
    name  = "controller.hostPort.enabled"
    value = "true"
  }
  set {
    name  = "controller.service.type"
    value = "NodePort"
  }
}

# ----- KEDA -----
resource "helm_release" "keda" {
  count            = var.install_keda ? 1 : 0
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = var.keda_version
  namespace        = "keda"
  create_namespace = true
  wait             = true
  timeout          = 600
}

# ----- EFK (Elasticsearch + Kibana + Fluent Bit) =====
# Per user direction 2026-05-24: "in the k8s scenario assume EFK is locally
# installed in the k8s environment. No need for separate EFK and non-EFK
# scenarios." The K8s scenario is canonical-EFK-in-cluster; IT runners that
# can't host EFK (low-RAM CI) opt out via install_efk=false.

resource "helm_release" "eck_operator" {
  count            = var.install_efk ? 1 : 0
  name             = "elastic-operator"
  repository       = "https://helm.elastic.co"
  chart            = "eck-operator"
  version          = var.eck_operator_version
  namespace        = "elastic-system"
  create_namespace = true
  wait             = true
  timeout          = 600
}

resource "kubectl_manifest" "elasticsearch" {
  count      = var.install_efk ? 1 : 0
  depends_on = [helm_release.eck_operator]

  yaml_body = <<-YAML
    apiVersion: elasticsearch.k8s.elastic.co/v1
    kind: Elasticsearch
    metadata:
      name: sutra-es
      namespace: default
    spec:
      version: ${var.elasticsearch_version}
      nodeSets:
        - name: default
          count: 1
          config:
            node.store.allow_mmap: false
  YAML
}

resource "kubectl_manifest" "kibana" {
  count      = var.install_efk ? 1 : 0
  depends_on = [kubectl_manifest.elasticsearch]

  yaml_body = <<-YAML
    apiVersion: kibana.k8s.elastic.co/v1
    kind: Kibana
    metadata:
      name: sutra-kb
      namespace: default
    spec:
      version: ${var.elasticsearch_version}
      count: 1
      elasticsearchRef:
        name: sutra-es
  YAML
}

resource "helm_release" "fluent_bit" {
  count = var.install_efk ? 1 : 0
  name  = "fluent-bit"
  # Deployed into `default` — the SAME namespace as Elasticsearch — because the
  # ES_PASSWORD env reads the ECK-managed `sutra-es-es-elastic-user` Secret via a
  # secretKeyRef, and secretKeyRef CANNOT cross namespaces (a fluent-bit pod in a
  # separate `logging` namespace fails with CreateContainerConfigError: secret not
  # found). Co-locating with ES keeps the credential reference in-namespace.
  repository       = "https://fluent.github.io/helm-charts"
  chart            = "fluent-bit"
  version          = var.fluent_bit_version
  namespace        = "default"
  create_namespace = false
  wait             = true
  timeout          = 600

  # Full logs pipeline: tail every container's stdout, attach Kubernetes
  # metadata (pod / namespace / labels), decode the inner JSON log record
  # so `level` / `loggerName` / `service.name` land as first-class ES fields,
  # then ship to the in-cluster Elasticsearch `sutra-logs` index. This mirrors
  # deploy/modules/efk-stack/ but stays self-contained so the IT cluster's
  # bring-up provisions the complete observability pipeline (logs land in ES
  # without a separate efk-stack apply) — per user direction 2026-06-24:
  # "observability should be part of bringing the infra up and running."
  #
  # Exclude_Path drops Fluent Bit's OWN container logs — otherwise the tail
  # re-ingests its own output and spirals into a self-referential feedback loop.
  values = [yamlencode({
    kind = "DaemonSet"
    env = [{
      name = "ES_PASSWORD"
      valueFrom = {
        secretKeyRef = {
          name = "sutra-es-es-elastic-user"
          key  = "elastic"
        }
      }
    }]
    config = {
      service = <<-EOT
        [SERVICE]
            Daemon Off
            Flush 1
            Log_Level info
            Parsers_File /fluent-bit/etc/parsers.conf
            HTTP_Server On
            HTTP_Listen 0.0.0.0
            HTTP_Port 2020
            Health_Check On
      EOT
      inputs  = <<-EOT
        [INPUT]
            Name tail
            # Infra / system container logs only. The Sutra engine (sutra-fednow*) is EXCLUDED:
            # its application logs flow over OTLP straight to ES sutra-app-logs
            # with trace_id/span_id correlation, so tailing its stdout here would double-write them
            # (and as unstructured plain text). Fluent Bit excludes its own logs to avoid a loop.
            Path /var/log/containers/*.log
            Exclude_Path /var/log/containers/*fluent-bit*.log,/var/log/containers/sutra-fednow*.log
            multiline.parser docker, cri
            Tag kube.*
            Mem_Buf_Limit 50MB
            Skip_Long_Lines On
      EOT
      filters = <<-EOT
        [FILTER]
            Name kubernetes
            Match kube.*
            Merge_Log On
            Keep_Log Off
            K8S-Logging.Parser On
            K8S-Logging.Exclude On
      EOT
      outputs = <<-EOT
        [OUTPUT]
            Name es
            Match kube.*
            Host sutra-es-es-http.default.svc
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
    }
  })]

  depends_on = [kubectl_manifest.elasticsearch]
}
