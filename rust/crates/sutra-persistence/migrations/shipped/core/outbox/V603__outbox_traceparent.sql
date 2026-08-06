-- Async trace-context bridge. Stores the W3C `traceparent` (00-<traceId>-<spanId>-<flags>) of the
-- request that enqueued this reply, so the outbox dispatcher — which runs seconds later on a
-- separate scheduler thread, long after the request's in-memory OTel context is gone — can restore
-- it and emit the outbound-send span into the SAME end-to-end trace (one traceId from inbound HTTP
-- through to the asynchronous delivery), instead of starting a disconnected root span.
--
-- Nullable: untraced enqueues, or hosts running without an OpenTelemetry SDK, simply store NULL and
-- the dispatcher falls back to a fresh span. 55 chars is the fixed W3C traceparent length; 64 leaves
-- headroom.

ALTER TABLE outbox_entry ADD COLUMN traceparent VARCHAR(64);
