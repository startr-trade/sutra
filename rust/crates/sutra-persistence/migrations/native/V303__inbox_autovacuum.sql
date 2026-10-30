-- Per-table autovacuum for `inbox_seen`, PostgreSQL only. See V607 (the outbox queue) for the
-- measurement behind these settings.
--
-- `inbox_seen` churns at the engine's rate: one dedup record per inbound message, deleted by the
-- dedup-retention prune. It can also be large, because it holds every live or retained row.
-- PostgreSQL's default trigger (50 dead tuples plus 20% of the table) therefore lets dead tuples
-- pile up to a fifth of a big table before a vacuum, and the indexes the engine's hot queries walk
-- carry all of them. A 1% trigger keeps the dead share small whatever the size. The fixed floor of
-- 5,000 keeps a small table from being vacuumed on every handful of changes, so at low rates this
-- vacuums rarely. Cost throttling stays at the server's setting: these tables can be big, and an
-- unthrottled vacuum of one would compete with the workload for I/O.
--
-- `autovacuum_naptime` is server-wide (see V607). MySQL/MariaDB and SQL Server have no per-table
-- equivalent, so they ship no counterpart.
ALTER TABLE inbox_seen SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 5000
);
