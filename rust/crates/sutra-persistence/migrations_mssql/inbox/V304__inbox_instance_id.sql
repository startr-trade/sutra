-- SQL Server dialect of V304__inbox_instance_id.sql: the instance a recorded delivery started or
-- resumed, so a duplicate's answer can name it. Nullable; read by primary key only.

ALTER TABLE inbox_seen ADD instance_id UNIQUEIDENTIFIER NULL;
