# Outputs are consumed by the sutra module's `observability` variable.
# Compose like:
#
#   module "efk" { source = "../../modules/efk-stack" }
#   module "engine" {
#     source = "../../modules/sutra"
#     observability = {
#       otlp_endpoint        = module.efk.otlp_endpoint
#       log_forward_endpoint = module.efk.log_forward_endpoint
#       elasticsearch_endpoint = module.efk.elasticsearch_endpoint
#       elasticsearch_credentials_secret_ref = module.efk.elasticsearch_credentials_secret_ref
#     }
#   }

output "otlp_endpoint" {
  description = "OTLP collector endpoint (grpc) for engine traces/metrics"
  value       = "otel-collector-opentelemetry-collector.${var.namespace}.svc:4317"
}

output "log_forward_endpoint" {
  description = "Fluent Bit forward endpoint (engine ships logs via forward protocol when set)"
  value       = var.fluent_bit.expose_forward_port ? "fluent-bit.${var.namespace}.svc:24224" : null
}

output "elasticsearch_endpoint" {
  description = "ES HTTPS endpoint (engine itself does not call ES directly; only used by audit fan-out shippers)"
  value       = "https://sutra-es-es-http.${var.namespace}.svc:9200"
}

output "elasticsearch_credentials_secret_ref" {
  description = "Secret reference for ES credentials (in secret-name#key form)"
  value       = "sutra-es-es-elastic-user#elastic"
}

output "kibana_endpoint" {
  description = "Kibana HTTPS endpoint (port-forward or ingress to access UI)"
  value       = "https://sutra-kibana-kb-http.${var.namespace}.svc:5601"
}

output "namespace" {
  description = "Namespace the EFK stack is deployed in"
  value       = var.namespace
}
