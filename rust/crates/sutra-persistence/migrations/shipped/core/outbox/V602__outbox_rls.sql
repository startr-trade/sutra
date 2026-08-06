-- Enables PostgreSQL Row-Level Security on outbox_entry. Parity with instance_state V3: the
-- policy keys off the sutra.deployment_id GUC which the repository layer (the outbox store via
-- DeploymentScopedConnection) sets per transaction. The `true` flag on current_setting returns
-- NULL when the GUC isn't set so migrations and bootstrap paths still function.

ALTER TABLE outbox_entry ENABLE ROW LEVEL SECURITY;
CREATE POLICY outbox_entry_deployment_iso ON outbox_entry
  USING (deployment_id = current_setting('sutra.deployment_id', true));
