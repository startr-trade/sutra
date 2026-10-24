-- Schema addendum, Rust engine only: provider-native async jobs of the `llm` transport
-- (`llm.mode: job`).
--
-- A job-mode outbound call submits the request to the provider and returns at once; the
-- provider finishes it later. This row is the durable record of that hand-off: the transport's
-- poller (or a provider callback) finds the job here by its provider job id, fetches the result
-- and delivers it to the channel's reply channel exactly as a held call would, then deletes the
-- row. The flow never sees the provider job id — the row maps it back to the request.
--
-- Lifecycle (owned by the `llm` transport):
--   outbound send in job mode -> INSERT (idempotent on outbox_key: a re-delivered outbox row
--       re-submits, and the second row is dropped by the unique key)
--   poller / callback         -> claim (claim_owner + claim_expires_at); Pending re-arms
--       next_poll_at and releases the claim; Done delivers and DELETEs; a provider failure or
--       a passed deadline_at marks the row failed (kept for inspection, never claimed again)
--   callback while claimed    -> sets `nudged`, so the claim holder re-polls at once on release
--
-- Deployment scoping mirrors timer_schedule: an explicit deployment_id bind on every statement,
-- with PostgreSQL Row-Level Security layered on below (same policy shape as V804/V605).

CREATE TABLE llm_job (
  deployment_id    VARCHAR(64)  NOT NULL,
  provider         VARCHAR(128) NOT NULL,
  provider_job_id  VARCHAR(512) NOT NULL,
  -- The outbound `llm` channel and the index of the candidate that accepted the job.
  channel          VARCHAR(255) NOT NULL,
  candidate_index  INTEGER      NOT NULL,
  tenant           VARCHAR(255) NOT NULL,
  module_key       VARCHAR(512) NOT NULL,
  reply_channel    VARCHAR(255) NOT NULL,

  -- The request as the flow sent it (echoed on the reply) and its headers.
  request_body     BYTEA        NOT NULL,
  headers_json     TEXT         NOT NULL DEFAULT '{}',
  -- The originating outbox row's key: the reply's inbox dedup key.
  outbox_key       VARCHAR(128) NOT NULL,
  traceparent      VARCHAR(64),

  poll_interval_seconds INTEGER NOT NULL,
  created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  next_poll_at     TIMESTAMP WITH TIME ZONE NOT NULL,
  deadline_at      TIMESTAMP WITH TIME ZONE NOT NULL,

  -- Claim ownership. An EXPIRED claim is claimable again by the claim predicate itself.
  claim_owner      VARCHAR(255),
  claim_expires_at TIMESTAMP WITH TIME ZONE,
  nudged           BOOLEAN      NOT NULL DEFAULT FALSE,

  failed           BOOLEAN      NOT NULL DEFAULT FALSE,
  last_error       TEXT,
  PRIMARY KEY (deployment_id, provider, provider_job_id)
);

-- Idempotent insert: a re-delivered outbox row must not create a second job row.
CREATE UNIQUE INDEX llm_job_key ON llm_job (deployment_id, outbox_key);
-- The poller's due-claim access path (live rows only).
CREATE INDEX llm_job_due ON llm_job (deployment_id, next_poll_at) WHERE NOT failed;

ALTER TABLE llm_job ENABLE ROW LEVEL SECURITY;
CREATE POLICY llm_job_deployment_iso ON llm_job
  USING (deployment_id = current_setting('sutra.deployment_id', true));
