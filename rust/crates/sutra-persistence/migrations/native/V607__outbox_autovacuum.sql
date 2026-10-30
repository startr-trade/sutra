-- Per-table autovacuum for the outbox queue, PostgreSQL only.
--
-- `outbox_entry` is a queue: every row is inserted by a step commit and deleted once it is
-- delivered, so at a steady rate the table stays small while it produces one dead tuple per
-- delivery. PostgreSQL's default trigger is 50 dead tuples plus 20% of the table. For a table that
-- holds a few thousand live rows, that fires rarely and then finds a long tail of dead tuples. The
-- claim (`ORDER BY next_attempt_at … FOR UPDATE SKIP LOCKED`) has to step over every dead index
-- entry until a vacuum removes them. At about 1,700 deliveries a second with the defaults, the
-- claim grew to 1–1.6 s every few minutes, throughput dropped below 100/s for some 40 s, and p99
-- latency reached 38 s.
--
-- The fix is to vacuum after a fixed number of dead tuples, whatever the table's size, and not to
-- throttle that vacuum. A queue table is small, so an unthrottled vacuum of it is cheap. At low
-- rates 5,000 deletions take a long time to accumulate, so this vacuums rarely and costs nothing
-- there. Statistics follow the same trigger, so the planner's row estimate tracks the queue depth.
--
-- How often autovacuum WAKES to look (`autovacuum_naptime`, default 1 min) is server-wide and
-- cannot be set per table. For high-rate deployments, lower it on the server (the throughput-tuning
-- guide recommends 1 s to 10 s).
--
-- MySQL/MariaDB and SQL Server have no per-table equivalent: InnoDB purge and SQL Server's ghost
-- cleanup are server-wide background tasks, so those dialects ship no counterpart of this script.
ALTER TABLE outbox_entry SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 5000
);
