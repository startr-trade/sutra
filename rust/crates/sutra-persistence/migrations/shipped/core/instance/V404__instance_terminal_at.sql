-- Terminal-instance retention (P1-2, queryable execution history).
--
-- WAVE-B FLAG: V404 is the next free number in the `instance` family (V401 table, V402 claim
-- columns, V403 RLS). Sibling wave branches may have claimed it too — the integrator renumbers.
--
-- Until now a finished instance left NO trace: the terminal transaction DELETEd its
-- instance_state row, so `GET /sutra/instances/{id}` answered 404 the moment the process ended.
-- The row is now RETAINED with its snapshot re-stamped COMPLETED/TERMINATED, and this column is
-- the marker that says so.
--
-- Why a COLUMN and not another snapshot key. Everything that consumes it is a SQL predicate that
-- must not decode a single snapshot:
--   * count_active (the deploy quiescence gate + the instance quota) must exclude terminal rows,
--     and it is a COUNT(*) — pushing status into the bytes would force a full decode per row.
--   * the retention purge sweeps `terminal_at <= now() - retention`, which needs an indexable
--     timestamp and DATABASE time, not a clock the engine writes into a byte blob.
--   * the snapshot v2/v3 key set is FROZEN and byte-deterministic (a pinned golden corpus);
--     terminal_at is engine lifecycle metadata — when the row became purge-eligible — not process
--     state, and process state is all the snapshot is allowed to describe.
--
-- NULL = live (parked / running / FAILED). FAILED deliberately stays NULL: a fatal instance is
-- retained until an operator cancels it, still counts against quiescence (it needs a human before
-- its deployment retires), and is never purged on a timer.
--
-- The partial index keys the purge scan on (deployment_id, terminal_at) over ONLY terminal rows,
-- so a table dominated by live instances never pays for the sweep — the same shape V402's
-- heartbeat index uses for claimed rows.
--
-- pg-only, exactly like V403: instance_state's engine writer is the PgPool persistence bridge, and
-- terminal retention is an engine-runtime behaviour. The mysql/mssql instance family exists for
-- dialect completeness and has no terminal writer, so it stays at V402.

ALTER TABLE instance_state
  ADD COLUMN terminal_at TIMESTAMPTZ;

CREATE INDEX instance_state_terminal_at
  ON instance_state (deployment_id, terminal_at)
  WHERE terminal_at IS NOT NULL;
