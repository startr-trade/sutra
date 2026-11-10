-- Whether a draining row keeps serving the inbound channels no newer version of its owner declares.
-- A row superseded by a newer version of its owner does: instances parked on such a channel can
-- still be reached, and messages on it still start work there, until the row retires (zero
-- instances, zero pending outbox). A row taken out by an undeploy does not. Rows already draining
-- when this column arrives keep FALSE, so they stop serving as before.
ALTER TABLE deployment_archive ADD COLUMN retains_channels BOOLEAN NOT NULL DEFAULT FALSE;
