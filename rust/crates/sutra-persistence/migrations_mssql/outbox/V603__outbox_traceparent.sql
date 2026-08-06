-- SQL Server dialect of the async trace-context bridge column (PostgreSQL reference:
-- V603__outbox_traceparent.sql). Stores the W3C traceparent of the enqueuing request so
-- the dispatcher can continue the same end-to-end trace. Nullable; 64 chars leaves
-- headroom over the fixed 55-char W3C form.

ALTER TABLE outbox_entry ADD traceparent NVARCHAR(64) NULL;
