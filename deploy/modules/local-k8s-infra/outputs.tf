# ----- Observability access (printed by null_resource.observability_access) ----
# The LB IPs + elastic password are written to <kubeconfig-dir>/observability-access.txt at the
# end of `tofu apply` (a tofu data source can't read them here — see observability.tf). Operators
# read that file, or: kubectl get svc -n default kibana-lb es-lb otel-collector-lb ; and
# kubectl get secret -n default sutra-es-es-elastic-user -o jsonpath='{.data.elastic}' | base64 -d
#
# This stage intentionally exposes no tofu outputs: the scenario stage reads the
# cluster stage's kubeconfig_path directly, and the observability endpoints are
# dynamic LB IPs surfaced via the access file above.
