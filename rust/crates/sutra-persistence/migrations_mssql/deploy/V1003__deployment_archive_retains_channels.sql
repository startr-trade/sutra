-- SQL Server dialect of V1003 (PostgreSQL reference: V1003__deployment_archive_retains_channels.sql).
-- Whether a draining row keeps serving the inbound channels no newer version of its owner declares:
-- 1 for a row a newer version superseded, 0 for an undeploy and for rows already draining when the
-- column arrives.
ALTER TABLE deployment_archive ADD retains_channels BIT NOT NULL
  CONSTRAINT df_deployment_archive_retains_channels DEFAULT 0;
