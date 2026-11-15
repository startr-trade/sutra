-- Inbox dedup per owner. Two live versions of one package (a draining one and its successor) share
-- one owner, so a redelivery that spans a version flip is a duplicate rather than a second run.
--
-- `deployment_owner` maps each deployment to its owner key ('own-' + the first 24 hex digits of
-- sha256 of the owner string). It is engine infrastructure, written by the engine at activation,
-- and carries no row security. A deployment with no row has a NULL owner, which dedupes within
-- that deployment only, exactly as before this script.
--
-- `inbox_seen.owner_id` is nullable, so adding it rewrites nothing; rows written before it stay
-- NULL and keep deduping under their own deployment through the primary key. The policy lets a
-- deployment see its own rows and the rows of every deployment of its owner. The subquery is
-- uncorrelated, so it is evaluated once per statement.
--
-- The unique index that makes the owner key arbitrate concurrent first sightings is V306: it is
-- built concurrently, which a transaction cannot do.

CREATE TABLE deployment_owner (
  deployment_id VARCHAR(64) PRIMARY KEY,
  owner_id      VARCHAR(64) NOT NULL
);

ALTER TABLE inbox_seen ADD COLUMN owner_id VARCHAR(64);

DROP POLICY inbox_seen_deployment_iso ON inbox_seen;
CREATE POLICY inbox_seen_deployment_iso ON inbox_seen USING (
  deployment_id = current_setting('sutra.deployment_id', true)
  OR owner_id = (SELECT o.owner_id FROM deployment_owner o
                 WHERE o.deployment_id = current_setting('sutra.deployment_id', true))
);
