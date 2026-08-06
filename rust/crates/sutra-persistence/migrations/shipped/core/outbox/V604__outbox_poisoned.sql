-- P1-1: the outbox delivery-attempt ceiling (`sutra.outbox.retry.max-attempts`).
--
-- Before this column the outbox retried FOREVER: a `PermanentFailure` (or an unresolvable
-- destination scheme) was deferred at the retry curve's max-delay horizon and re-claimed there for
-- the life of the deployment. That is a deliberate at-least-once posture and stays the DEFAULT —
-- with `sutra.outbox.retry.max-attempts` unset, nothing about the old behaviour changes and this
-- column is never set. When the operator DOES configure a ceiling, an entry that exhausts it is
-- marked here instead of being deferred again.
--
-- Why a column rather than a far-future `next_attempt_at` sentinel: "terminal" is a fact about the
-- entry, not about when it is next due. An operator (and `count_pending_for_deployment`, the
-- quiescence half of the DRAINING-deployment retirement gate) must be able to tell "parked at the
-- poison horizon, still trying" from "given up on" — a sentinel timestamp encodes neither, and a
-- redrive would have to guess which rows were deliberately stopped.
--
-- The row is NOT deleted: at-least-once is never traded for silence. It stays visible, still
-- carries its `last_diagnostic_json` (including the once-only incident marker), and clearing the
-- flag re-arms delivery — the redrive path.

ALTER TABLE outbox_entry ADD COLUMN poisoned BOOLEAN NOT NULL DEFAULT FALSE;

-- The claim predicate is `deployment_id = $1 AND NOT poisoned AND next_attempt_at <= $2`; this
-- partial index keeps it on the live rows only, so a growing tail of terminal entries never slows
-- the dispatcher's hot path.
CREATE INDEX idx_outbox_entry_due_live
    ON outbox_entry (deployment_id, next_attempt_at)
    WHERE NOT poisoned;
