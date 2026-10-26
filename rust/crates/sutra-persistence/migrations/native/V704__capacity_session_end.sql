-- Schema addendum, Rust engine only: the sessions a `session-end:` ended while their instance runs
-- on (see V703 for the pools themselves).
--
-- A session end releases the instance's slots — the whole session, or one call when the session
-- channel declares `pool-call-key:` — and the instance keeps running. A request the instance put
-- in the outbox for a pooled channel may still be waiting for a slot at that moment, or the
-- instance may send one later. Such a request is still sent, but an ended session holds no slot
-- and takes none: the outbox asks for a slot, and the store answers "session ended" when a row
-- here covers the request's call, so the request goes without one instead of re-taking a slot
-- and holding it until the instance ends.
--
--   call_key ''   the whole session ended
--   call_key <k>  the call <k> ended
--
-- Deployment-scoped like capacity_slot (an explicit deployment_id bind on every statement, plus
-- row-level security). The capacity sweep deletes the rows of instances that are over.

CREATE TABLE capacity_session_end (
  deployment_id  VARCHAR(64)  NOT NULL,
  instance_id    UUID         NOT NULL,
  call_key       VARCHAR(512) NOT NULL DEFAULT '',
  ended_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, instance_id, call_key)
);

ALTER TABLE capacity_session_end ENABLE ROW LEVEL SECURITY;
CREATE POLICY capacity_session_end_deployment_iso ON capacity_session_end
  USING (deployment_id = current_setting('sutra.deployment_id', true));
