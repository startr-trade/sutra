-- SQL Server dialect of the ENGINE-OWNED compliance-cycle tables (PostgreSQL reference:
-- ../postgres/V904__coverage_cycle.sql, which states the contract). Each CREATE is existence-guarded
-- and declares its index inline, as the other coverage tables do.
--
-- The metric key is NONCLUSTERED: (64 + 128 + 450) * 2 = 1284 bytes exceeds the 900-byte clustered
-- limit and fits the 1700-byte nonclustered one.
IF OBJECT_ID(N'coverage_cycle', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_cycle (
    deployment_id NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    cycle_label   NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    declared_at   DATETIME2(6) NOT NULL CONSTRAINT df_coverage_cycle_declared_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_coverage_cycle PRIMARY KEY NONCLUSTERED (deployment_id, cycle_label)
  );
END

IF OBJECT_ID(N'coverage_active_cycle', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_active_cycle (
    deployment_id NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    cycle_label   NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CONSTRAINT pk_coverage_active_cycle PRIMARY KEY NONCLUSTERED (deployment_id)
  );
END

IF OBJECT_ID(N'coverage_cycle_metric', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_cycle_metric (
    deployment_id NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    cycle_label   NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    path_urn      NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NOT NULL,
    covered       BIT NOT NULL CONSTRAINT df_coverage_cycle_metric_covered DEFAULT 0,
    CONSTRAINT pk_coverage_cycle_metric PRIMARY KEY NONCLUSTERED (deployment_id, cycle_label, path_urn)
  );
END

IF OBJECT_ID(N'coverage_cycle_fragment', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_cycle_fragment (
    id              BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_coverage_cycle_fragment PRIMARY KEY,
    deployment_id   NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    cycle_label     NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    route_urn       NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
    segment_process NVARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
    instance_id     NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    business_key    NVARCHAR(MAX),
    trace_id        NVARCHAR(MAX),
    at              DATETIME2(6) NOT NULL CONSTRAINT df_coverage_cycle_fragment_at DEFAULT SYSUTCDATETIME(),
    INDEX coverage_cycle_fragment_route NONCLUSTERED (deployment_id, cycle_label, route_urn)
  );
END
