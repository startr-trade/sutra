-- MySQL/MariaDB dialect of V1003 (PostgreSQL reference: V1003__deployment_archive_retains_channels.sql).
-- Whether a draining row keeps serving the inbound channels no newer version of its owner declares:
-- TRUE for a row a newer version superseded, FALSE for an undeploy and for rows already draining
-- when the column arrives.
ALTER TABLE deployment_archive ADD COLUMN retains_channels BOOLEAN NOT NULL DEFAULT FALSE;
