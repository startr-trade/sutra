-- sutra:no-transaction
-- Lookup path for a RETIRED correlation key, PostgreSQL only: given (deployment, name, value), is
-- there a row an instance that has since ended carried? The relay path asks only when no live
-- instance carries the key a message correlates on, to tell a message for an instance that has
-- finished (nothing will wait for it again) from one that may have beaten its instance to the wait.
-- Without this index that question scans every alias row of the deployment, which a sender of
-- uncorrelated messages could make the engine do at will.
--
-- Partial over retired rows only, so the live lookups keep their own small index. A terminal step
-- already writes a new row version for each alias it retires (the `live` flip is never a HOT update,
-- because `live` is in the predicates of the live indexes), so this adds one index entry per
-- retired alias; the retention purge removes them with their rows.
--
-- Built concurrently because alias_index can be large; the runner applies this script outside a
-- transaction, under its migration lock, one statement at a time. The DROP clears an index a
-- crashed concurrent build left invalid.
DROP INDEX CONCURRENTLY IF EXISTS alias_index_retired_lookup;
CREATE INDEX CONCURRENTLY alias_index_retired_lookup ON alias_index (deployment_id, alias_name, alias_value) WHERE live = FALSE;
