-- The `accounts` data store's own schema + seed, module-resident (mounted with the resources folder,
-- not baked into the image). The sql provider runs this against the store's OWN DataSource (declared in
-- datastores.yaml) on first use. Idempotent: CREATE TABLE IF NOT EXISTS + INSERT ... ON CONFLICT DO NOTHING.
--
-- The table is the generic key->value shape the sql store provider reads/writes: rows are keyed by
-- the full namespace (tenant, module, version) + store name + key. The engine binds store_value as a
-- plain string (JSON text), so the column type is THIS module's choice — TEXT here (unlimited on
-- Postgres). A module on MySQL/MariaDB would use LONGTEXT, on SQL Server NVARCHAR(MAX), etc. An account
-- item is the map {balance, frozen}, keyed by account id.

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

-- Seed the ledger (IT convenience — in production a store starts empty and seeding is an explicit
-- deployment step). alice/bob/carol = 100, frozen-fred = 100 (frozen), explode-on-credit = 100 (the
-- atomicity fault sentinel below).
INSERT INTO data_store (tenant_id, module_id, module_version, store_name, store_key, store_value, rev, updated_at)
VALUES
  ('default','money-transfer','1.0.0','accounts','alice',            '{"balance":100,"frozen":false}', 1, now()),
  ('default','money-transfer','1.0.0','accounts','bob',              '{"balance":100,"frozen":false}', 1, now()),
  ('default','money-transfer','1.0.0','accounts','carol',            '{"balance":100,"frozen":false}', 1, now()),
  ('default','money-transfer','1.0.0','accounts','frozen-fred',      '{"balance":100,"frozen":true}',  1, now()),
  ('default','money-transfer','1.0.0','accounts','explode-on-credit','{"balance":100,"frozen":false}', 1, now())
ON CONFLICT (tenant_id, module_id, module_version, store_name, store_key) DO NOTHING;

-- Atomicity fault injection (IT-only): a trigger that RAISEs when the sentinel 'explode-on-credit' item's
-- VALUE actually changes, so a transfer alice -> explode-on-credit debits alice (first store write) then
-- fails on the credit (the value-changing write) -> the <bpmn:transaction> rolls back -> alice is
-- unchanged. The `IS DISTINCT FROM OLD.store_value` guard is deliberate: the engine takes its row lock via
-- a rev-only UPDATE (lock-via-UPDATE, portable in place of SELECT ... FOR UPDATE), which must NOT trip the
-- fault — only the real credit (a value change) does. Fires on UPDATE only, so the seed INSERT is safe.
CREATE OR REPLACE FUNCTION reject_credit_to_sentinel() RETURNS trigger AS $$
BEGIN
    IF NEW.store_name = 'accounts' AND NEW.store_key = 'explode-on-credit'
       AND NEW.store_value IS DISTINCT FROM OLD.store_value THEN
        RAISE EXCEPTION 'injected credit failure for atomicity test (store_key=%)', NEW.store_key;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reject_credit_to_sentinel ON data_store;
CREATE TRIGGER trg_reject_credit_to_sentinel
    BEFORE UPDATE ON data_store
    FOR EACH ROW
    EXECUTE FUNCTION reject_credit_to_sentinel();
