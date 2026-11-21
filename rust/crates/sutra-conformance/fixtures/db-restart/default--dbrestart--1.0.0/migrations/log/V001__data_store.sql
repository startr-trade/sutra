-- The key/value table every store of this package shares (rows partition by store_name). The
-- engine stamps the tenant/module/version columns on insert; the row's identity is
-- (store_name, store_key), so that is the primary key, and the key column is in the bytewise "C"
-- collation: together they let the primary key serve a key query's range.
CREATE TABLE IF NOT EXISTS data_store (
  tenant_id       VARCHAR(64)  NOT NULL,
  module_id       VARCHAR(256) NOT NULL,
  module_version  VARCHAR(64)  NOT NULL,
  store_name      VARCHAR(128) NOT NULL,
  store_key       VARCHAR(512) COLLATE "C" NOT NULL,
  store_value     TEXT         NOT NULL,
  rev             BIGINT       NOT NULL DEFAULT 1,
  updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_name, store_key)
);
