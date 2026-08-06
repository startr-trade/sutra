-- MySQL/MariaDB dialect of the alias correlation index (PostgreSQL reference:
-- V101__alias_index.sql). Semantics are normative, syntax is not — this file must reproduce:
--
--   * the unique-LIVE alias guarantee. PostgreSQL uses a partial unique index
--     (WHERE unique_alias AND live). MySQL/MariaDB have no partial indexes, so a
--     functional workaround applies: a STORED generated column `unique_live` that is 1 when
--     the row is a live unique alias and NULL otherwise, plus a unique key over
--     (deployment_id, alias_name, alias_value, unique_live). NULLs never collide in a MySQL
--     unique index, so retired / non-unique rows are exempt — exactly the partial-index
--     behaviour: first inserter wins, retire frees the slot for a successor.
--   * byte-wise comparison semantics (PostgreSQL compares these values byte-wise): the
--     ascii_bin collation. The ascii CHARACTER SET additionally keeps the wide composite
--     PRIMARY KEY inside InnoDB's 3072-byte index limit (the same trade the per-dialect
--     data_store scaffolding makes); consequence, flagged: alias names/values are
--     ASCII-only on this dialect — a non-ASCII value is rejected fail-closed (strict mode).
--
-- Deployment isolation on this dialect is single-layer: the explicit deployment_id bind on
-- every statement (the documented enforced-bind-only posture — MySQL/MariaDB have no
-- row-security policies, so the database belt of the PostgreSQL reference does not exist
-- here; the V403/V602/V702/V802 policy scripts are intentionally absent from this tree).

CREATE TABLE alias_index (
  deployment_id VARCHAR(64)   NOT NULL,
  instance_id   BINARY(16)    NOT NULL,
  alias_name    VARCHAR(256)  NOT NULL,
  alias_value   VARCHAR(1024) NOT NULL,
  unique_alias  BOOLEAN       NOT NULL DEFAULT FALSE,
  live          BOOLEAN       NOT NULL DEFAULT TRUE,
  created_at    DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  unique_live   TINYINT GENERATED ALWAYS AS
                  (CASE WHEN unique_alias AND live THEN 1 ELSE NULL END) STORED,
  PRIMARY KEY (deployment_id, instance_id, alias_name, alias_value)
) CHARACTER SET ascii COLLATE ascii_bin;

-- Lookup path for follow-up signal correlation. No partial indexes on this dialect: the
-- `live` flag joins the key instead of filtering the index.
CREATE INDEX alias_index_live_lookup
  ON alias_index (deployment_id, alias_name, alias_value, live);

-- Atomic unique-alias enforcement via the generated column (see header).
CREATE UNIQUE INDEX alias_index_unique_live
  ON alias_index (deployment_id, alias_name, alias_value, unique_live);
