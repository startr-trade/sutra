-- Who holds each exclusive inbound endpoint, for the folder deployment source. A folder only says
-- which archives exist. It cannot say which of two archives on one endpoint got there first, so
-- a restart or a new pod would have to guess. This table is what they read instead, and because it is
-- shared, every replica reads the same answer.
--
-- The `db` deployment source does not use it: `deployment_archive.activated_at` answers the same
-- question there. Like that table this is engine infrastructure, not deployment-scoped data, so it
-- carries no RLS. A row whose owner no longer claims the endpoint means nothing and is replaced by
-- the next claimant.
--
-- `also_held_by` names the other deployments on an endpoint that can have several: the subscribers of
-- a topic, or deployments an earlier engine let share what now belongs to one owner.
CREATE TABLE deployment_endpoint_holder (
  transport            VARCHAR(64)   NOT NULL,
  identity             VARCHAR(2048) NOT NULL,
  owner                VARCHAR(1024) NOT NULL,
  holder_deployment_id VARCHAR(64)   NOT NULL,
  holder_label         VARCHAR(1024) NOT NULL,
  also_held_by         TEXT[]        NOT NULL DEFAULT '{}',
  held_since           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (transport, identity)
);
