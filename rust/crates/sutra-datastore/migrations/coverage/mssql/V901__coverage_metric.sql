-- SQL Server dialect of the ENGINE-OWNED coverage metric-flag table (PostgreSQL reference:
-- ../postgres/V901__coverage_metric.sql — read it for the ownership + idempotence contract).
--
-- Shipped inside the engine and applied to the USER-DECLARED `coverage` store's own connection on
-- first use (datastore-schema-projection.md §7), so it must be re-runnable: the whole CREATE is
-- guarded by `IF OBJECT_ID(...) IS NULL`, the T-SQL spelling of `CREATE TABLE IF NOT EXISTS`, and
-- the secondary index is declared INLINE (SQL Server 2014+) so it is created with the table and
-- needs no second existence guard.
--
-- Latin1_General_100_BIN2 collation: byte-wise comparison like the reference dialect. The
-- (deployment_id, path_urn) PRIMARY KEY is declared NONCLUSTERED: a clustered key is capped at 900
-- bytes, but NVARCHAR(64)+NVARCHAR(450) = (64+450)*2 = 1028 bytes exceeds it — the 1700-byte
-- nonclustered limit accommodates it. The key still drives the idempotent seed (insert +
-- duplicate-key rejection, server errors 2627/2601).
--
-- No security policy on this dialect: enforced-bind posture, as everywhere outside the engine's
-- own PostgreSQL database.
IF OBJECT_ID(N'coverage_metric', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_metric (
    deployment_id NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    path_urn      NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NOT NULL,
    covered       BIT NOT NULL CONSTRAINT df_coverage_metric_covered DEFAULT 0,
    CONSTRAINT pk_coverage_metric PRIMARY KEY NONCLUSTERED (deployment_id, path_urn),
    INDEX coverage_metric_deployment_covered NONCLUSTERED (deployment_id, covered)
  );
END
