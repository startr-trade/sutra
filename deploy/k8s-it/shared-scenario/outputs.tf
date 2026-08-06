# R14 shared-instance harness — handles the ITs / operator read back.

output "namespace" {
  description = "Namespace holding every shared-instance object."
  value       = var.namespace
}

output "deployments_configmap" {
  description = "The ConfigMap `sutra deploy` patches (binary_data entry per archive)."
  value       = kubernetes_config_map_v1.deployments.metadata[0].name
}

output "estate_secret" {
  description = "The estate Secret `sutra deploy --secret` merges keys into (mounted at /etc/sutra/secrets)."
  value       = kubernetes_secret_v1.estate.metadata[0].name
}

output "engine_service" {
  description = "The shared engine's ClusterIP Service (fronted by the Ingress)."
  value       = kubernetes_service_v1.sutra_engine.metadata[0].name
}

output "ingress_note" {
  description = "How to reach the engine from the host."
  value       = "HTTP via the ingress-nginx controller LB: kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}' — port 80, paths /channels/** and /sutra/health/**."
}

output "rabbitmq_service" {
  description = "The shared broker's LoadBalancer Service (AMQP 5672 + mgmt 15672 from the host; in-cluster aliases: rabbitmq-mtmx, rabbit)."
  value       = kubernetes_service_v1.rabbitmq.metadata[0].name
}
