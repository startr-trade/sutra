-- SQL Server dialect of the ENGINE-OWNED coverage reconstruction-fragment table (PostgreSQL
-- reference: ../postgres/V902__coverage_fragment.sql). Existence-guarded + inline index for the
-- same reasons as V901.
--
-- Latin1_General_100_BIN2 collation. business_key / trace_id are NVARCHAR(MAX) (portable,
-- arbitrary UTF-8); `at` defaults to SYSUTCDATETIME() mirroring the reference `DEFAULT now()`.
IF OBJECT_ID(N'coverage_fragment', N'U') IS NULL
BEGIN
  CREATE TABLE coverage_fragment (
    id              BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_coverage_fragment PRIMARY KEY,
    deployment_id   NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
    route_urn       NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
    segment_process NVARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
    instance_id     NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    business_key    NVARCHAR(MAX),
    trace_id        NVARCHAR(MAX),
    at              DATETIME2(6) NOT NULL CONSTRAINT df_coverage_fragment_at DEFAULT SYSUTCDATETIME(),
    INDEX coverage_fragment_route NONCLUSTERED (deployment_id, route_urn)
  );
END
