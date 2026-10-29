-- relay-bench reference data: the generic key->value shape the sql store provider reads.
CREATE TABLE IF NOT EXISTS data_store (
  tenant_id       VARCHAR(64)  NOT NULL,
  module_id       VARCHAR(256) NOT NULL,
  module_version  VARCHAR(64)  NOT NULL,
  store_name      VARCHAR(128) NOT NULL,
  store_key       VARCHAR(512) NOT NULL,
  store_value     TEXT         NOT NULL,
  rev             BIGINT       NOT NULL DEFAULT 1,
  updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (tenant_id, module_id, module_version, store_name, store_key)
);
INSERT INTO data_store (tenant_id, module_id, module_version, store_name, store_key, store_value)
VALUES
  ('default', 'relay-bench', '1.0.0', 'refdata', 'default', '{"maxAmount": 1000000, "enabled": true}'),
  ('default', 'relay-bench', '1.0.0', 'refdata', 'party-a', '{"name": "Party A", "active": true}'),
  ('default', 'relay-bench', '1.0.0', 'refdata', 'party-b', '{"name": "Party B", "active": true}')
ON CONFLICT DO NOTHING;
