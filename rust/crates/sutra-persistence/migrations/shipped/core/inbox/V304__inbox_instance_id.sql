-- The instance a recorded delivery started or resumed, so a duplicate's answer can name it.
-- Nullable: rows written before this column, and a delivery that ended without an instance, have
-- none, and the duplicate is then simply answered without one. Read by primary key only, so it
-- needs no index; adding a nullable column rewrites nothing.

ALTER TABLE inbox_seen ADD COLUMN instance_id UUID;
