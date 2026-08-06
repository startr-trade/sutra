-- Enables PostgreSQL Row-Level Security on channel_instance. The policy keys off the
-- sutra.deployment_id GUC, which the repository layer sets via SET LOCAL inside every transaction
-- (see DeploymentScopedConnection). The `true` flag on current_setting returns NULL when the GUC
-- isn't set so migration runs and bootstrap paths that legitimately operate outside a deployment can
-- still write — production-style hardening (FORCE ROW LEVEL SECURITY, dedicated application-role)
-- is layered by the operator on top. Mirrors V403__instance_rls.sql.

ALTER TABLE channel_instance ENABLE ROW LEVEL SECURITY;
CREATE POLICY channel_instance_deployment_iso ON channel_instance
  USING (deployment_id = current_setting('sutra.deployment_id', true));
