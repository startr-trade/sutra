output "service_name" {
  description = "ClusterIP service name (use this for in-cluster routing)"
  value       = kubernetes_service_v1.this.metadata[0].name
}

output "namespace" {
  description = "Namespace the engine is deployed in"
  value       = var.namespace
}

output "http_endpoint" {
  description = "Internal HTTP endpoint for inbound channels"
  value       = "http://${kubernetes_service_v1.this.metadata[0].name}.${var.namespace}.svc:8080"
}

output "admin_endpoint" {
  description = "Internal management endpoint (/sutra/health/*)"
  value       = "http://${kubernetes_service_v1.this.metadata[0].name}.${var.namespace}.svc:9090"
}

output "service_account_name" {
  description = "Service account name (use this for granting cross-resource RBAC)"
  value       = kubernetes_service_account_v1.this.metadata[0].name
}

output "deployment_name" {
  description = "Deployment name (use for kubectl rollout, etc.)"
  value       = kubernetes_deployment_v1.this.metadata[0].name
}

output "migrate_job_name" {
  description = "Schema-migration Job name when enable_migrate_job=true; empty string when the Job is disabled."
  value       = var.enable_migrate_job ? kubernetes_job_v1.migrate[0].metadata[0].name : ""
}

output "tenant_configmap_names" {
  description = "Map of tenant id → ConfigMap name created by this module. Mounted into the engine pod at /etc/sutra/resources/tenants/<id>/."
  value       = { for k, cm in kubernetes_config_map_v1.tenants : k => cm.metadata[0].name }
}
