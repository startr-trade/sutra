-- The `call_log` store's own table — module-resident (mounted with the resources folder, not
-- baked into the image) and run against the store's own DataSource on first use. Idempotent.
--
-- This is a PROJECTED table, so it is NOT the generic key->blob `data_store` shape. Layout is
-- control columns + one column per scalar declared by schemas/call-log/call-log.xsd's
-- CallLogEntry, in declared order:
--
--   store_key  -- the <q:store key="..."> value; the table's PRIMARY KEY (required)
--   rev        -- optimistic-concurrency revision, bumped on every write (required)
--   updated_at -- write timestamp (required)
--
-- There is no store_name column: this table IS the store. There is no deployment_id and no
-- RLS: a business store's data deliberately carries across a version bump.
--
-- Column types must be able to hold the declared facets; `sutra lint` checks exactly that
-- (VARCHAR(40) for maxLength=40, NUMERIC(12,4) for totalDigits=12/fractionDigits=4, a
-- NULL-admitting column for the minOccurs=0 cellSite).

CREATE TABLE IF NOT EXISTS call_log (
  store_key        VARCHAR(512)  NOT NULL,
  entry_id         VARCHAR(40)   NOT NULL,
  subscriber       VARCHAR(20)   NOT NULL,
  counterparty     VARCHAR(20)   NOT NULL,
  started_at       TIMESTAMP WITH TIME ZONE NOT NULL,
  duration_seconds INTEGER       NOT NULL,
  bearing          VARCHAR(8)    NOT NULL,
  cell_site        VARCHAR(16),
  rated_amount     NUMERIC(12,4) NOT NULL,
  billable         BOOLEAN       NOT NULL,
  rev              BIGINT        NOT NULL DEFAULT 1,
  updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_key)
);

-- Typed columns are the point: these indexes are only possible because the rows are not blobs.
CREATE INDEX IF NOT EXISTS call_log_subscriber_started_idx ON call_log (subscriber, started_at);
CREATE INDEX IF NOT EXISTS call_log_started_idx            ON call_log (started_at);
