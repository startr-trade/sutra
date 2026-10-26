-- Audit events a transport writes (event types `<transport>.<EVENT>`, always carrying a `.`;
-- the engine's own event types never do).
--
-- The (deployment_id, instance_id, seq) uniqueness is the engine's replay dedup: the engine
-- numbers an instance's events itself, monotonically across suspend and resume, and a re-emit of
-- an already-written number is a no-op. A transport cannot take part in that numbering — the
-- counter lives with the replica running the instance — so its events must never occupy a number
-- the engine will write next, or the engine's event would be the one dropped. The uniqueness is
-- therefore narrowed to the engine's events: a transport event takes the instance's current
-- high-water seq (it sorts right after the engine event it followed, by seq then id) and never
-- conflicts with anything.
--
-- PostgreSQL only: the engine's audit writer is PostgreSQL only, and the dialect trees keep
-- V201 as they are.
ALTER TABLE audit_event DROP CONSTRAINT audit_event_seq_unique;

CREATE UNIQUE INDEX audit_event_seq_unique ON audit_event (deployment_id, instance_id, seq)
  WHERE event_type NOT LIKE '%.%';
